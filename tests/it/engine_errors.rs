//! An error raised under the query engine reaches the caller as itself.
//!
//! A scan's failure crosses DataFusion boxed as an opaque external error. A
//! caller deciding whether to retry matches on this crate's variants, so the
//! variant — and with it `is_retryable` — must survive the crossing.

#![cfg(feature = "testkit")]

use meterstore::testkit::{MeteringWorkload, TestHarness};
use meterstore::tiering::store::{HotStore, PartitionId};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

#[tokio::test]
async fn a_scan_short_a_reclaimed_window_is_a_retryable_boundary_move_through_sql() {
    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(START, START + Duration::days(3))
        .await
        .expect("partitions");
    harness.seed_watermark(START).await.expect("watermark");
    let store = harness.store().await.expect("store");
    let workload = MeteringWorkload::new(START).seed(0x31).malo_ids(1).days(2);
    harness
        .ingest(&store, &workload.generate().expect("workload"))
        .await
        .expect("ingest");

    // The hot tier gives `[START, START+1d)` away while the published boundary
    // still says the window is hot: what a plan made just before archival sees.
    let hot = store.admin().hot_store();
    let first = PartitionId::new(TestHarness::TABLE, START);
    hot.detach_partition(&first).await.expect("detach");
    hot.drop_partition(&first, START + Duration::DAY)
        .await
        .expect("drop");

    let err = store
        .query(&format!("SELECT count(*) FROM {}", TestHarness::TABLE))
        .await
        .map(|_| ())
        .expect_err("a query short a window must not answer");
    assert!(
        matches!(err, meterstore::Error::BoundaryMoved { .. }) && err.is_retryable(),
        "the store's own refusal must reach the caller as itself: {err:?}"
    );
}

/// A store with two days ingested above a boundary at `START`.
async fn two_days() -> (TestHarness, meterstore::MeterStore) {
    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(START, START + Duration::days(3))
        .await
        .expect("partitions");
    harness.seed_watermark(START).await.expect("watermark");
    let store = harness.store().await.expect("store");
    harness
        .ingest(
            &store,
            &MeteringWorkload::new(START)
                .seed(0x14)
                .malo_ids(1)
                .days(2)
                .generate()
                .expect("workload"),
        )
        .await
        .expect("ingest");
    (harness, store)
}

#[tokio::test]
async fn a_hot_writer_overtaken_by_archival_recovers_on_retry() {
    // The writer holds the boundary it opened at. Archival detaches the window
    // just above it; the writer's next write there is a retryable boundary move,
    // and the retry through the same writer refuses the rows by name instead of
    // failing the same way again.
    let (_h, store) = two_days().await;
    let writer = store.hot_writer().await.expect("writer");
    let late = MeteringWorkload::new(START)
        .seed(0x14)
        .malo_ids(1)
        .days(1)
        .version(20_260_726_000_002)
        .generate()
        .expect("workload");
    writer.append(&late).await.expect("the window is still hot");

    store
        .admin()
        .archive(START + Duration::days(2), 1)
        .await
        .expect("archive the first day");

    let moved = writer
        .append(&late)
        .await
        .expect_err("the window was detached under the writer");
    assert!(
        matches!(moved, meterstore::Error::BoundaryMoved { .. }) && moved.is_retryable(),
        "{moved:?}"
    );
    let refused = writer
        .append(&late)
        .await
        .expect_err("now below the boundary");
    assert!(
        matches!(refused, meterstore::Error::Config(_))
            && refused.to_string().contains("cold tier"),
        "the retry names the tier the rows belong to: {refused:?}"
    );
    assert_eq!(writer.watermark().get(), START + Duration::DAY);
}

#[tokio::test]
async fn a_reclaimed_window_is_never_created_again() {
    // Its rows are in the cold tier; a partition made for it would hold rows no
    // query reads.
    let (h, _store) = two_days().await;
    let first = PartitionId::new(TestHarness::TABLE, START);
    h.hot().detach_partition(&first).await.expect("detach");
    h.hot()
        .drop_partition(&first, START + Duration::DAY)
        .await
        .expect("drop");

    let err = h
        .ensure_partitions(START, START + Duration::days(2))
        .await
        .expect_err("the first window was reclaimed");
    assert!(
        matches!(err, meterstore::Error::BoundaryMoved { .. }) && err.is_retryable(),
        "{err:?}"
    );
}
