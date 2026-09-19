//! Can anything **other than MeterStore** read what MeterStore wrote?
//!
//! P2 says the durable state is an open format, and that is the project's main
//! procurement argument: twenty years of regulated history that any Iceberg
//! engine can read without this crate. An argument nobody tests is a hope.
//!
//! # What this suite does and does not cover
//!
//! It opens the written Parquet **files directly, by path**, with a bare
//! DataFusion session that has none of this crate's providers registered — the
//! same position an external engine is in once its catalog has told it where the
//! files are. That covers the half of the open-format claim that can be checked
//! hermetically:
//!
//! - the bytes are standard Parquet, readable without `iceberg-rust`;
//! - the values survive — decimals at full precision, quality flags as the
//!   self-describing strings the schema promises rather than opaque integers;
//! - the documented cold-layout tuning is actually in the footer;
//! - and, decisively, **the published resolution SQL turns the raw versioned
//!   rows into the same answer MeterStore gives**.
//!
//! It does *not* start a container. The suites that do are siblings —
//! `interop_duckdb` and `interop_pyiceberg` run the real engines, and
//! `interop_trino` pins the SQL semantics that decided how the balancing day is
//! stored. This one is the hermetic floor beneath them: it needs no Docker, so
//! it still rules out the failure mode that matters most — output only this
//! crate's own reader can make sense of — on a machine that cannot run the rest.
//! Spark remains open.
//!
//! # Why the naive query is asserted to be *wrong*
//!
//! One test below deliberately checks that summing the raw files double-counts.
//! The resolution trap is a claim about what happens to an engine that does not
//! apply the resolution SQL, and a mitigation for a hazard nobody has
//! demonstrated is a mitigation nobody will bother to apply.

#![cfg(feature = "testkit")]

use datafusion::prelude::SessionContext;
use meterstore::testkit::{MeteringWorkload, Oracle, TestHarness};
use rust_decimal::Decimal;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// A store with a workload ingested and everything archived into Iceberg.
///
/// Everything, deliberately: this suite is about what an external engine sees,
/// and an external engine sees only the cold tier.
async fn archived(workload: MeteringWorkload) -> (TestHarness, meterstore::MeterStore, Oracle) {
    let harness = TestHarness::start().await.expect("harness");
    let (from, to) = workload.range();

    harness
        .ensure_partitions(from, to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");

    let store = harness.store().await.expect("store");
    let series = workload.generate().expect("workload");
    let mut oracle = Oracle::new();
    oracle.record(&series).expect("oracle");
    harness.ingest(&store, &series).await.expect("ingest");

    // One day past the end, so every window closes and the hot tier empties.
    store
        .admin()
        .archive(to + Duration::days(2), 64)
        .await
        .expect("archive");
    assert!(
        store.watermark().await.unwrap().get() >= to,
        "the suite is meaningless unless everything reached the cold tier"
    );

    (harness, store, oracle)
}

/// A DataFusion session that knows nothing about MeterStore.
///
/// No `TieredTableProvider`, no `ResolvedTableProvider`, no Iceberg catalog —
/// just Parquet files on disk, registered under the raw table's name so the
/// published resolution SQL runs against them unmodified.
async fn external_reader(harness: &TestHarness) -> SessionContext {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use std::sync::Arc;

    let files = harness.parquet_files();
    assert!(
        !files.is_empty(),
        "archival wrote no Parquet files, so there is nothing to read"
    );

    let ctx = SessionContext::new();
    let options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");

    // Each data file named explicitly, which is what an engine actually has
    // after reading a manifest — the manifest lists paths, and the engine opens
    // them. It also sidesteps a listing quirk that has nothing to do with the
    // format: the test warehouse lives under a `.tmp…` directory, and object
    // stores skip dot-prefixed path segments as hidden.
    let urls: Vec<ListingTableUrl> = files
        .iter()
        .map(|f| ListingTableUrl::parse(format!("file://{}", f.display())).expect("file url"))
        .collect();

    let resolved = options
        .infer_schema(&ctx.state(), &urls[0])
        .await
        .expect("infer schema from the files alone");

    let config = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(options)
        .with_schema(resolved);
    ctx.register_table(
        TestHarness::TABLE,
        Arc::new(ListingTable::try_new(config).expect("listing table")),
    )
    .expect("register");

    ctx
}

/// One `BIGINT` from a bare session.
async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    use datafusion::arrow::array::AsArray;
    let batches = ctx
        .sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("run");
    batches[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0)
}

/// One decimal total from a bare session.
async fn total(ctx: &SessionContext, sql: &str) -> Decimal {
    use datafusion::arrow::array::AsArray;
    let batches = ctx
        .sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("run");
    let array = batches[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Decimal128Type>();
    Decimal::try_from_i128_with_scale(array.value(0), u32::from(array.scale() as u8))
        .expect("decimal in range")
        .normalize()
}

#[tokio::test]
async fn the_files_are_readable_without_meterstore_or_iceberg() {
    // The floor P2 rests on: if this fails, nothing else in the argument stands.
    let workload = MeteringWorkload::new(START)
        .seed(0x09E4)
        .malo_ids(4)
        .days(3);
    let (from, to) = workload.range();
    let (harness, _store, oracle) = archived(workload).await;

    let ctx = external_reader(&harness).await;
    let rows = count(&ctx, "SELECT COUNT(*) FROM readings_versions").await as u64;

    assert_eq!(
        rows,
        oracle.row_count(from, to),
        "a plain Parquet reader must see every row"
    );
}

#[tokio::test]
async fn the_published_resolution_sql_gives_an_external_engine_the_right_answer() {
    // The mitigation, actually exercised. The SQL comes from the store —
    // the same text `system.resolution` serves — and runs unmodified against
    // files opened by a session that has never heard of MeterStore.
    let workload = MeteringWorkload::new(START)
        .seed(0xC0FFEE)
        .malo_ids(4)
        .days(3)
        .with_corrections(0.2);
    let (from, to) = workload.range();
    let (harness, store, oracle) = archived(workload).await;

    let ctx = external_reader(&harness).await;
    let resolution = store.resolution_sql();

    let resolved = total(
        &ctx,
        &format!("SELECT COALESCE(SUM(value), 0) FROM ({resolution}) AS resolved"),
    )
    .await;

    assert_eq!(
        resolved,
        oracle.sum_kwh(from, to).normalize(),
        "the published SQL must reproduce MeterStore's own answer"
    );
}

#[tokio::test]
async fn the_naive_query_really_does_double_count() {
    // The hazard the naming and the published SQL exist to prevent. If this ever
    // stopped being true the mitigation would be theatre — and note that
    // compaction would make it *incidentally* true, which is exactly why the
    // resolution trap
    // calls the trap dangerous rather than merely inconvenient.
    let workload = MeteringWorkload::new(START)
        .seed(0xBAD)
        .malo_ids(3)
        .days(2)
        .with_corrections(0.3);
    let (from, to) = workload.range();
    let (harness, _store, oracle) = archived(workload).await;

    let ctx = external_reader(&harness).await;
    let naive = total(
        &ctx,
        "SELECT COALESCE(SUM(value), 0) FROM readings_versions",
    )
    .await;

    assert!(
        naive > oracle.sum_kwh(from, to).normalize(),
        "a raw sum must overstate; if it does not, the fixture produced no corrections"
    );
}

#[tokio::test]
async fn stored_values_are_self_describing() {
    // An engine reading the Parquet sees `MEASURED`, not an opaque `2`
    // whose meaning lives in this crate's source. That is the difference between
    // an open format and a format with a decoder ring.
    let workload = MeteringWorkload::new(START)
        .seed(0x5E1F)
        .malo_ids(2)
        .days(1);
    let (harness, _store, _oracle) = archived(workload).await;

    let ctx = external_reader(&harness).await;
    let measured = count(
        &ctx,
        "SELECT COUNT(*) FROM readings_versions WHERE quality = 'MEASURED'",
    )
    .await;
    assert!(measured > 0, "quality must be readable as its own name");

    let resolution = count(
        &ctx,
        "SELECT COUNT(*) FROM readings_versions WHERE resolution = 'PT15M'",
    )
    .await;
    assert!(
        resolution > 0,
        "resolution must be an ISO 8601 duration, not a local code"
    );

    // OBIS codes are canonical, so an external engine can join on them.
    let obis = count(
        &ctx,
        "SELECT COUNT(DISTINCT obis_code) FROM readings_versions WHERE obis_code LIKE '1-0:%'",
    )
    .await;
    assert_eq!(obis, 1);
}

#[tokio::test]
async fn decimals_survive_at_full_precision_outside_this_crate() {
    // Settlement is money. A value that round-trips through MeterStore but loses
    // a place when read by anything else is a bug an internal test cannot see.
    let workload = MeteringWorkload::new(START).seed(0xDEC).malo_ids(3).days(2);
    let (from, to) = workload.range();
    let (harness, _store, oracle) = archived(workload).await;

    let ctx = external_reader(&harness).await;
    let sum = total(
        &ctx,
        "SELECT COALESCE(SUM(value), 0) FROM readings_versions",
    )
    .await;

    // No corrections in this workload, so raw and resolved agree.
    assert_eq!(sum, oracle.sum_kwh(from, to).normalize());

    // And the column is still a decimal rather than having been widened to a
    // float somewhere along the way.
    let batches = ctx
        .sql("SELECT value FROM readings_versions LIMIT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(matches!(
        batches[0].schema().field(0).data_type(),
        datafusion::arrow::datatypes::DataType::Decimal128(18, 6)
    ));
}

#[tokio::test]
async fn the_footer_carries_the_tuning_the_design_claims() {
    // The cold layout lists settings the performance targets depend on. A claim
    // about a
    // file's layout is checkable by reading the file, and nothing else here
    // would notice if a writer property silently stopped applying.
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::schema::types::ColumnPath;

    let workload = MeteringWorkload::new(START)
        .seed(0xF007)
        .malo_ids(4)
        .days(2);
    let (harness, _store, _oracle) = archived(workload).await;

    let files = harness.parquet_files();
    let file = std::fs::File::open(&files[0]).expect("open a data file");
    let reader = SerializedFileReader::new(file).expect("parse the footer");
    let metadata = reader.metadata();
    let row_group = metadata.row_group(0);

    // Sort order, declared so a reader can exploit it rather than rediscover it.
    let sorting = row_group
        .sorting_columns()
        .expect("the footer must declare a sort order");
    assert_eq!(sorting.len(), 2, "malo_id then from");
    assert!(sorting.iter().all(|c| !c.descending));

    // Bloom filters on the lookup columns — the highest-leverage
    // setting in the table, and it is what the single-meter latency target rests
    // on.
    for name in ["malo_id", "obis_code"] {
        let column = row_group
            .columns()
            .iter()
            .find(|c| c.column_path() == &ColumnPath::from(name))
            .unwrap_or_else(|| panic!("{name} must be a column"));
        assert!(
            column.bloom_filter_offset().is_some(),
            "{name} must carry a bloom filter"
        );
    }

    // Page-level statistics, without which the page-pruning layer silently does
    // nothing.
    let from_column = row_group
        .columns()
        .iter()
        .find(|c| c.column_path() == &ColumnPath::from("from"))
        .expect("from column");
    assert!(
        from_column.offset_index_offset().is_some(),
        "page index must be written, or layer 4 of the pruning stack is absent"
    );
}

#[tokio::test]
async fn the_files_really_are_sorted_the_way_the_footer_says() {
    // A declaration a reader is entitled to trust. Asserting the footer *says*
    // `(malo_id, from)` is worth little if the rows are in some other order —
    // that would be worse than declaring nothing at all.
    //
    // Checked per file, because that is the scope of the claim: `sorting_columns`
    // describes one file's rows, and concatenating two sorted files does not
    // produce a sorted stream.
    let workload = MeteringWorkload::new(START)
        .seed(0x5017)
        .malo_ids(5)
        .days(2);
    let (harness, _store, _oracle) = archived(workload).await;

    for path in harness.parquet_files() {
        let ctx = SessionContext::new();
        ctx.register_parquet(
            "one_file",
            path.to_str().expect("utf-8 path"),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .expect("register a single file");

        let batches = ctx
            .sql(r#"SELECT malo_id, "from" FROM one_file"#)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");

        let mut previous: Option<(String, i64)> = None;
        for batch in &batches {
            // Cast rather than downcast: a Parquet reader is free to hand back a
            // dictionary or a view array for a dictionary-encoded column, and
            // that is a reader detail rather than anything about the data.
            let malo = datafusion::arrow::compute::cast(
                batch.column(0),
                &datafusion::arrow::datatypes::DataType::Utf8,
            )
            .expect("malo_id is a string column");

            use datafusion::arrow::array::AsArray;
            let malo = malo.as_string::<i32>();
            let from = batch
                .column(1)
                .as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>();

            for i in 0..batch.num_rows() {
                let current = (malo.value(i).to_string(), from.value(i));
                if let Some(prev) = &previous {
                    assert!(
                        prev <= &current,
                        "{} breaks its declared (malo_id, from) order: {prev:?} then {current:?}",
                        path.display()
                    );
                }
                previous = Some(current);
            }
        }
    }
}
