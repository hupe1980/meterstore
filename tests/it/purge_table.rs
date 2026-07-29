//! Decommissioning a table, which is the only way this crate deletes readings.
//!
//! Everything else is append-only: a correction is a new version, erasure
//! destroys a mapping rather than rows, and a hot partition drop reclaims space
//! for rows already durable in Iceberg. That asymmetry is what makes a settlement
//! reproducible, and it is also why "delete this data" has exactly one answer and
//! that answer is whole-table.
//!
//! The claim under test is the strong one: **the files are gone**. Dropping the
//! Iceberg catalog entry alone would leave every Parquet file in object storage
//! while reporting success, which is the failure mode that looks like a feature.

#![cfg(feature = "testkit")]

use meterstore::HotStore;
use meterstore::config::TableConfig;
use meterstore::testkit::{MeteringWorkload, TestHarness};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// A store with data in both tiers.
async fn populated() -> (TestHarness, meterstore::MeterStore) {
    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(START, START + Duration::days(4))
        .await
        .expect("partitions");
    harness.seed_watermark(START).await.expect("watermark");
    let store = harness.store().await.expect("store");

    let workload = MeteringWorkload::new(START)
        .seed(0x9E12)
        .malo_ids(3)
        .days(3);
    harness
        .ingest(&store, &workload.generate().expect("workload"))
        .await
        .expect("ingest");
    // Archive part of it, so both tiers genuinely hold rows.
    store
        .archive(START + Duration::days(2), 1)
        .await
        .expect("archive");

    (harness, store)
}

#[tokio::test]
async fn a_purge_removes_the_data_files_not_only_the_catalog_entry() {
    // The assertion that separates a real purge from a bookkeeping one. Checked
    // by walking the warehouse rather than by asking Iceberg, because asking
    // Iceberg whether Iceberg forgot the table proves nothing about the bytes.
    let (harness, store) = populated().await;

    let before = harness.parquet_files();
    assert!(
        !before.is_empty(),
        "the fixture must have archived something, or there is nothing to purge"
    );

    store.purge_table(TestHarness::TABLE).await.expect("purge");

    let after = harness.parquet_files();
    assert!(
        after.is_empty(),
        "every data file must be gone from object storage, not merely unreferenced: {after:#?}"
    );
}

#[tokio::test]
async fn a_purge_removes_the_hot_table_and_every_partition() {
    let (harness, store) = populated().await;

    let partitions_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_class WHERE relname LIKE 'readings_versions%'")
            .fetch_one(harness.hot().pool())
            .await
            .expect("count relations");
    assert!(partitions_before > 1, "parent plus at least one partition");

    store.purge_table(TestHarness::TABLE).await.expect("purge");

    let partitions_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_class WHERE relname LIKE 'readings_versions%'")
            .fetch_one(harness.hot().pool())
            .await
            .expect("count relations");
    assert_eq!(
        partitions_after, 0,
        "the parent and every partition must be gone"
    );
}

#[tokio::test]
async fn a_detached_orphan_does_not_survive_the_purge() {
    // A partition detached by an interrupted archival run is no longer a
    // dependent relation, so `DROP TABLE ... CASCADE` does not reach it. Left
    // behind it would hold rows from a table that no longer exists — invisible
    // to every query, and to the orphan check, which needs the parent to find it.
    let (harness, store) = populated().await;

    let partition =
        meterstore::tiering::store::PartitionId::new(TestHarness::TABLE, START + Duration::days(2));
    harness
        .hot()
        .detach_partition(&partition)
        .await
        .expect("simulate an interrupted run");

    let name = partition.relation_name().expect("name");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1)")
            .bind(&name)
            .fetch_one(harness.hot().pool())
            .await
            .expect("lookup");
    assert!(exists, "the detached partition must exist before the purge");

    store.purge_table(TestHarness::TABLE).await.expect("purge");

    let survives: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1)")
            .bind(&name)
            .fetch_one(harness.hot().pool())
            .await
            .expect("lookup");
    assert!(!survives, "a detached orphan must not outlive its table");
}

#[tokio::test]
async fn the_table_name_must_be_repeated() {
    // A handle carries no visual indication of which table it points at, and a
    // purge has no recovery path. The name is stated by the caller and checked
    // by the store rather than trusted.
    let (_h, store) = populated().await;

    let err = store
        .purge_table("some_other_table")
        .await
        .expect_err("a mismatched confirmation must be refused");

    let msg = err.to_string();
    assert!(msg.contains(TestHarness::TABLE), "{msg}");
    assert!(msg.contains("some_other_table"), "{msg}");
    assert!(
        msg.contains("no recovery path"),
        "the message must say why the name is repeated: {msg}"
    );

    // And nothing was destroyed by the refusal.
    assert!(!_h.parquet_files().is_empty());
}

#[tokio::test]
async fn purging_one_table_leaves_another_intact() {
    // The decommissioning case: a table per tenant (§15.2.1) means one tenant
    // leaving must not touch the others. They share a catalog, a warehouse root
    // and a PostgreSQL database, so this is not self-evident.
    let harness = TestHarness::start().await.expect("harness");

    let keep = TableConfig::new("keep_versions")
        .settlement_lag(Duration::days(1))
        .build()
        .expect("config");
    let leaving = TableConfig::new("leaving_versions")
        .settlement_lag(Duration::days(1))
        .build()
        .expect("config");

    let catalog = meterstore::MeterCatalog::builder()
        .table(harness.builder_for(keep).await.expect("keep"))
        .table(harness.builder_for(leaving).await.expect("leaving"))
        .build()
        .await
        .expect("catalog");
    catalog.create_tables().await.expect("create");

    for store in catalog.tables() {
        store
            .hot_store()
            .ensure_partitions(
                store.config().name(),
                START,
                START + Duration::days(3),
                Duration::DAY,
            )
            .await
            .expect("partitions");
        harness
            .seed_watermark_for(store.config().name(), START, Duration::DAY)
            .await
            .expect("watermark");
        store
            .append(
                &MeteringWorkload::new(START)
                    .seed(0x11)
                    .malo_ids(1)
                    .days(1)
                    .generate()
                    .expect("workload"),
            )
            .await
            .expect("append");
    }

    catalog
        .table("leaving_versions")
        .expect("leaving")
        .purge_table("leaving_versions")
        .await
        .expect("purge");

    // The surviving table still answers, which is the whole point.
    let kept = catalog
        .table("keep_versions")
        .expect("keep")
        .query("SELECT COUNT(*) FROM keep")
        .await
        .expect("the surviving table must still be queryable");

    use meterstore::arrow::array::AsArray;
    let n = kept.batches()[0]
        .column(0)
        .as_primitive::<meterstore::arrow::datatypes::Int64Type>()
        .value(0);
    assert!(n > 0, "the table that stayed must still hold its readings");
}
