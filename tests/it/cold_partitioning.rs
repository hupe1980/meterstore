//! The cold layout's partition spec, and what it actually prunes.
//!
//! The cold layout partitions by the declared identity columns, then `month(from)`.
//! Both halves are asserted because a partition spec is the kind of thing that
//! can be documented and absent — the table created unpartitioned, or a
//! `buckets` setting validated by configuration and read by nothing.
//!
//! The assertions are deliberately about *observable layout* rather than about
//! `iceberg-rust`'s planner agreeing with itself. A partition spec that exists
//! in the metadata and does not reach the file paths prunes nothing, and a test
//! that asked the library whether the library had pruned would pass either way.

#![cfg(feature = "testkit")]

use datafusion::common::ScalarValue;
use meterstore::arrow::datatypes::{DataType, Field};
use meterstore::config::TableConfig;
use meterstore::encode::StoredSeries;
use meterstore::testkit::{MeteringWorkload, TestHarness};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// A tenant-scoped table: one identity column, as a multi-operator deployment
/// declares it.
fn tenanted() -> meterstore::ValidatedTableConfig {
    TableConfig::new(TestHarness::TABLE)
        .settlement_lag(Duration::days(1))
        .identity_column(Field::new("tenant", DataType::Utf8, false))
        .build()
        .expect("config")
}

/// `workload`'s series, tagged with a tenant.
fn for_tenant(workload: &MeteringWorkload, tenant: &str) -> Vec<StoredSeries> {
    workload
        .generate()
        .expect("workload")
        .into_iter()
        .map(|s| s.with_extra("tenant", ScalarValue::Utf8(Some(tenant.to_string()))))
        .collect()
}

/// Two operators' readings over the same days, with the first day archived.
async fn two_tenants() -> TestHarness {
    let harness = TestHarness::with_config(tenanted()).await.expect("harness");
    harness
        .ensure_partitions(START, START + Duration::days(4))
        .await
        .expect("partitions");
    harness.seed_watermark(START).await.expect("watermark");
    let store = harness.store().await.expect("store");

    for (i, tenant) in ["9900000000001", "9900000000002"].into_iter().enumerate() {
        let workload = MeteringWorkload::new(START)
            .seed(0x7E1 + i as u64)
            // Distinct measuring points per operator. Two tenants *may* share a
            // MaLo-ID — that is precisely what the identity column exists to keep
            // apart — but a fixture that relied on it would be testing the merge
            // key rather than the layout.
            .malo_offset(i * 100)
            .malo_ids(2)
            .days(2);
        harness
            .ingest(&store, &for_tenant(&workload, tenant))
            .await
            .expect("ingest");
    }

    store
        .admin()
        .archive(START + Duration::days(2), 1)
        .await
        .expect("archive");

    harness
}

#[tokio::test]
async fn the_cold_table_is_partitioned_by_tenant_then_month() {
    let harness = two_tenants().await;
    let table = harness
        .cold()
        .load(TestHarness::TABLE)
        .await
        .expect("load the table");

    let spec = table.metadata().default_partition_spec();
    let names: Vec<&str> = spec.fields().iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        names,
        ["tenant", "from_month"],
        "identity columns lead, because they are what every query filters on"
    );

    // The transform matters as much as the order: `identity` on the tenant and
    // `month` on the interval start. A day transform would produce one partition
    // per archival window, which is how a lakehouse acquires a small-file problem.
    let schema = table.metadata().current_schema();
    let tenant_source = schema.field_by_name("tenant").expect("tenant column").id;
    let from_source = schema.field_by_name("from").expect("from column").id;
    assert_eq!(spec.fields()[0].source_id, tenant_source);
    assert_eq!(spec.fields()[1].source_id, from_source);
    assert_eq!(
        spec.fields()[0].transform,
        iceberg::spec::Transform::Identity
    );
    assert_eq!(spec.fields()[1].transform, iceberg::spec::Transform::Month);
}

#[tokio::test]
async fn each_tenant_lands_in_its_own_files() {
    // The property that makes the spec worth having. If both operators' rows
    // shared a file, no predicate could eliminate either at the manifest level
    // however good the statistics were.
    let harness = two_tenants().await;
    let files = harness.parquet_files();
    assert!(!files.is_empty(), "archival must have written something");

    let paths: Vec<String> = files
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();

    for tenant in ["9900000000001", "9900000000002"] {
        assert!(
            paths
                .iter()
                .any(|p| p.contains(&format!("tenant={tenant}"))),
            "no file is scoped to {tenant}: {paths:#?}"
        );
    }

    // And no file may belong to both. Hidden partitioning writes the value into
    // the path, so a file carrying two tenants would have to omit it.
    for path in &paths {
        let tagged = ["9900000000001", "9900000000002"]
            .iter()
            .filter(|t| path.contains(&format!("tenant={t}")))
            .count();
        assert_eq!(
            tagged, 1,
            "a data file belongs to exactly one tenant: {path}"
        );
    }
}

#[tokio::test]
async fn the_month_partition_does_not_split_a_window_per_day() {
    // `month(from)`, not `day(from)`. Two archived days of one tenant belong to
    // one month partition, so the path must carry the month and nothing finer.
    let harness = two_tenants().await;
    let paths: Vec<String> = harness
        .parquet_files()
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();

    assert!(
        paths.iter().all(|p| p.contains("from_month=")),
        "every data file sits under a month partition: {paths:#?}"
    );
    assert!(
        paths.iter().all(|p| !p.contains("from_day=")),
        "a day transform would produce one partition per archival window"
    );
}

#[tokio::test]
async fn a_tenant_scoped_query_still_returns_only_that_tenant() {
    // Partitioning changes the plan, not the answer. This is the assertion that
    // would catch a splitter routing rows into the wrong partition — the failure
    // mode that is invisible in the layout and fatal in a bill.
    let harness = two_tenants().await;
    let store = harness.store().await.expect("store");

    let result = store
        .query("SELECT tenant, COUNT(*) FROM readings GROUP BY tenant ORDER BY tenant")
        .await
        .expect("query");
    let rendered = meterstore::arrow::util::pretty::pretty_format_batches(result.batches())
        .expect("render")
        .to_string();

    assert!(rendered.contains("9900000000001"), "{rendered}");
    assert!(rendered.contains("9900000000002"), "{rendered}");

    // Both tenants generated the same shape, so their counts must match — a
    // splitter that dropped or duplicated a partition's rows would show up here
    // and nowhere else.
    let counts: Vec<i64> = {
        use meterstore::arrow::array::AsArray;
        let mut out = Vec::new();
        for batch in result.batches() {
            let column = batch
                .column(1)
                .as_primitive::<meterstore::arrow::datatypes::Int64Type>();
            for i in 0..batch.num_rows() {
                out.push(column.value(i));
            }
        }
        out
    };
    assert_eq!(counts.len(), 2, "two tenants reported");
    assert_eq!(
        counts[0], counts[1],
        "identical workloads, identical counts"
    );
}

#[tokio::test]
async fn a_table_with_no_identity_columns_partitions_by_month_alone() {
    // The single-operator deployment, which is most of them. It must not be made
    // to carry an empty partition field, and it must still get time pruning.
    let harness = TestHarness::start().await.expect("harness");
    let table = harness
        .cold()
        .create_table(TestHarness::TABLE)
        .await
        .expect("table");

    let spec = table.metadata().default_partition_spec();
    let names: Vec<&str> = spec.fields().iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["from_month"]);
    assert!(
        !spec.is_unpartitioned(),
        "month pruning applies whether or not a tenant column was declared"
    );
}

#[tokio::test]
async fn a_table_whose_stored_layout_disagrees_with_the_configuration_is_refused() {
    // Iceberg has no `update_spec` here, so a table created without a tenant
    // partition can never acquire one. Loading it anyway leaves a deployment
    // believing its scans prune by tenant while every one reads every operator's
    // manifests — right answers, silently linear in the tenant count.
    let harness = TestHarness::with_config(tenanted()).await.expect("harness");

    // The table exists with [tenant, from_month]. Asking for it with no identity
    // columns is a different layout, and there is no way to reconcile the two.
    let err = harness
        .cold()
        .create_table_with(
            TestHarness::TABLE,
            &[],
            &[],
            &meterstore::tiering::store::MaintenancePolicy::default(),
        )
        .await
        .expect_err("a layout mismatch must halt rather than degrade");

    let msg = err.to_string();
    assert!(msg.contains("tenant"), "{msg}");
    assert!(msg.contains("from_month"), "{msg}");
    assert!(
        msg.contains("no partition-spec evolution"),
        "the message must say why it cannot simply be fixed: {msg}"
    );
}

#[tokio::test]
async fn a_tenant_predicate_eliminates_the_other_tenant_s_files() {
    // The claim the whole spec rests on, asserted where it is actually decided.
    //
    // DataFusion's Iceberg scan exposes no metrics, so "did the engine prune"
    // cannot be observed from a query plan. Iceberg's own planner can be asked
    // directly, and it is the layer the claim is about: `plan_files` is manifest
    // evaluation, before a single Parquet footer is opened.
    use futures::TryStreamExt;
    use iceberg::expr::Reference;
    use iceberg::spec::Datum;

    let harness = two_tenants().await;
    let table = harness
        .cold()
        .load(TestHarness::TABLE)
        .await
        .expect("table");

    let count_files = async |predicate: Option<iceberg::expr::Predicate>| {
        let mut builder = table.scan();
        if let Some(p) = predicate {
            builder = builder.with_filter(p);
        }
        builder
            .build()
            .expect("scan")
            .plan_files()
            .await
            .expect("plan")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect")
            .len()
    };

    let all = count_files(None).await;
    let one = count_files(Some(
        Reference::new("tenant").equal_to(Datum::string("9900000000001")),
    ))
    .await;

    assert!(all >= 2, "the fixture must span more than one file: {all}");
    assert!(
        one < all,
        "a tenant predicate must eliminate files at the manifest: {one} of {all}"
    );

    // And it must eliminate exactly the other tenant's, not merely some.
    let other = count_files(Some(
        Reference::new("tenant").equal_to(Datum::string("9900000000002")),
    ))
    .await;
    assert_eq!(
        one + other,
        all,
        "every file belongs to exactly one tenant, so the two subsets must partition it"
    );

    // A tenant that reported nothing reads nothing at all — the property that
    // makes a scan's cost proportional to the tenant rather than the warehouse.
    let absent = count_files(Some(
        Reference::new("tenant").equal_to(Datum::string("9900000000009")),
    ))
    .await;
    assert_eq!(absent, 0, "an unknown tenant must touch no files");
}
