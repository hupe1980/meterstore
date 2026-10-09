//! A filter on the cold tier returns exactly the rows it accepts.
//!
//! Every filter is checked twice: through the cold scan, where a shape the
//! integration converts may prune files, and by DataFusion alone over the
//! unfiltered rows. A pushed form stronger than its filter shows up as rows
//! the first answer lacks.

#![cfg(feature = "testkit")]

use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use meterstore::ReadMode;
use meterstore::testkit::{MeteringWorkload, TestHarness};
use std::sync::Arc;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

#[tokio::test]
async fn every_cold_filter_returns_what_the_engine_alone_returns() {
    let h = TestHarness::start().await.expect("harness");
    h.ensure_partitions(START, START + Duration::days(3))
        .await
        .expect("partitions");
    h.seed_watermark(START).await.expect("watermark");
    let store = h.store().await.expect("store");
    h.ingest(
        &store,
        &MeteringWorkload::new(START)
            .seed(0x23)
            .malo_ids(3)
            .days(2)
            .generate()
            .expect("workload"),
    )
    .await
    .expect("ingest");
    store
        .admin()
        .archive(START + Duration::days(3), 4)
        .await
        .expect("archive");

    let cold = h
        .builder_for(h.config().clone())
        .await
        .unwrap()
        .read_mode(ReadMode::Historical)
        .build()
        .await
        .expect("historical");
    let everything = cold
        .query("SELECT * FROM readings")
        .await
        .expect("unfiltered");
    assert!(everything.num_rows() > 0, "the fixture archived nothing");
    let oracle = SessionContext::new();
    oracle
        .register_table(
            "readings",
            Arc::new(
                MemTable::try_new(everything.schema(), vec![everything.batches().to_vec()])
                    .expect("memtable"),
            ),
        )
        .expect("register");

    let first = everything.batches()[0]
        .column_by_name("malo_id")
        .map(|c| {
            use meterstore::arrow::array::AsArray;
            c.as_string::<i32>().value(0).to_string()
        })
        .expect("malo_id");
    let prefix = &first[..2];
    let filters = [
        "CAST(value AS DOUBLE) > 0.1".to_string(),
        "value > 0.1".to_string(),
        "\"from\" < TIMESTAMP '2026-07-20T12:00:00.0000005Z'".to_string(),
        // Wildcards in every position. An escaped `%` is not among them: no
        // identifier holds a literal one, so the comparison would be zero
        // against zero and prove nothing.
        format!("malo_id LIKE '{prefix}%'"),
        format!("malo_id LIKE '%{}'", &first[first.len() - 1..]),
        format!("malo_id LIKE '{prefix}_%'"),
        format!("NOT (malo_id = '{first}' AND \"from\" >= TIMESTAMP '2026-07-21T00:00:00Z')"),
        format!("malo_id = '{first}'"),
        format!("malo_id IN ('{first}', '00000000000')"),
        "\"from\" >= TIMESTAMP '2026-07-21T00:00:00Z'".to_string(),
        "source_detail IS NULL".to_string(),
    ];
    let count = |b: &[meterstore::arrow::array::RecordBatch]| {
        use meterstore::arrow::array::AsArray;
        b[0].column(0)
            .as_primitive::<meterstore::arrow::datatypes::Int64Type>()
            .value(0)
    };
    // Every filter is checked before any is reported, so one run says which
    // shapes the cold scan gets wrong rather than only the first.
    let mut wrong = Vec::new();
    let mut matched_some = 0;
    for filter in &filters {
        let sql = format!("SELECT COUNT(*) FROM readings WHERE {filter}");
        let alone = count(&oracle.sql(&sql).await.unwrap().collect().await.unwrap());
        matched_some += usize::from(alone > 0);
        match cold.query(&sql).await {
            Ok(r) if count(r.batches()) == alone => {}
            Ok(r) => wrong.push(format!(
                "{filter}: {} rows, expected {alone}",
                count(r.batches())
            )),
            Err(e) => wrong.push(format!("{filter}: {e}")),
        }
    }
    assert!(
        wrong.is_empty(),
        "the cold scan disagrees with the filter: {wrong:#?}"
    );
    assert!(
        matched_some >= filters.len() - 2,
        "most filters must select rows, or agreeing on zero proves nothing"
    );
}

#[tokio::test]
async fn a_pinned_snapshot_is_handed_only_filters_its_scan_can_honour() {
    // The pinned-snapshot provider is a second door into the Iceberg scan, and
    // the same allowlist stands in front of it.
    let h = TestHarness::start().await.expect("harness");
    h.ensure_partitions(START, START + Duration::days(3))
        .await
        .expect("partitions");
    h.seed_watermark(START).await.expect("watermark");
    let store = h.store().await.expect("store");
    h.ingest(
        &store,
        &MeteringWorkload::new(START)
            .seed(0x23)
            .malo_ids(1)
            .days(2)
            .generate()
            .expect("workload"),
    )
    .await
    .expect("ingest");
    store
        .admin()
        .archive(START + Duration::days(3), 4)
        .await
        .expect("archive");
    let pinned = store
        .as_of(
            meterstore::planner::SnapshotSelector::Timestamp(datetime!(2100-01-01 00:00 UTC)),
            None,
        )
        .await
        .expect("pin");
    pinned
        // The raw table: on `readings` a filter on `value` sits above version
        // resolution and never reaches the scan.
        .query("SELECT COUNT(*) FROM readings_versions WHERE CAST(value AS DOUBLE) > 0.1")
        .await
        .expect("a cast the integration cannot translate is applied above the scan");
}

#[tokio::test]
async fn an_escaped_wildcard_matches_the_literal_character_through_the_cold_scan() {
    // `LIKE 'a\%%'` asks for values starting with a literal `a%`. A conversion
    // that ignored the escape would push `starts_with("a\%")` or treat the `%`
    // as a wildcard — the first matching nothing, the second matching `ab` and
    // `axb` — and either answer differs from the filter's. The fixture holds a
    // literal `%`, a backslash and both near misses, so the comparison is not
    // zero against zero.
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::scalar::ScalarValue;
    let config = meterstore::config::TableConfig::new(TestHarness::TABLE)
        .settlement_lag(Duration::DAY)
        .attribute_column(Field::new("note", DataType::Utf8, true))
        .build()
        .expect("config");
    let h = TestHarness::with_config(config).await.expect("harness");
    h.ensure_partitions(START, START + Duration::days(3))
        .await
        .expect("partitions");
    h.seed_watermark(START).await.expect("watermark");
    let store = h.store().await.expect("store");
    let notes = ["a%b", "ab", "a\\b", "axb"];
    let series: Vec<_> = MeteringWorkload::new(START)
        .seed(0x40)
        .malo_ids(notes.len())
        .days(1)
        .generate()
        .expect("workload")
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            s.with_extra(
                "note",
                ScalarValue::Utf8(Some(notes[i % notes.len()].to_string())),
            )
        })
        .collect();
    h.ingest(&store, &series).await.expect("ingest");
    store
        .admin()
        .archive(START + Duration::days(3), 4)
        .await
        .expect("archive");

    let cold = h
        .builder_for(h.config().clone())
        .await
        .unwrap()
        .read_mode(ReadMode::Historical)
        .build()
        .await
        .expect("historical");
    let everything = cold
        .query("SELECT * FROM readings_versions")
        .await
        .expect("unfiltered");
    let oracle = SessionContext::new();
    oracle
        .register_table(
            "readings_versions",
            Arc::new(
                MemTable::try_new(everything.schema(), vec![everything.batches().to_vec()])
                    .expect("memtable"),
            ),
        )
        .expect("register");
    let count = |b: &[meterstore::arrow::array::RecordBatch]| {
        use meterstore::arrow::array::AsArray;
        b[0].column(0)
            .as_primitive::<meterstore::arrow::datatypes::Int64Type>()
            .value(0)
    };

    for (filter, expected) in [
        ("note LIKE 'a\\%%'", 96),
        ("note LIKE 'a\\\\%'", 96),
        ("note LIKE 'a%'", 384),
        ("note LIKE 'a_b'", 288),
    ] {
        let sql = format!("SELECT COUNT(*) FROM readings_versions WHERE {filter}");
        let alone = count(&oracle.sql(&sql).await.unwrap().collect().await.unwrap());
        assert_eq!(
            alone, expected,
            "{filter}: the fixture is not what the case needs"
        );
        let through_scan = cold.query(&sql).await.expect(&sql);
        assert_eq!(
            count(through_scan.batches()),
            alone,
            "{filter}: the cold scan disagrees with the filter"
        );
    }
}
