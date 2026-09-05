//! Ingest, archival and reads running against one table at the same time.
//!
//! Every other suite drives the store in sequence: write, then archive, then
//! query. That is the right shape for asserting *what* each step does, and it
//! cannot see the class of defect that lives **between** two steps — a decision
//! taken from state read at one moment and served from state read at another.
//!
//! Two such defects were found by reading rather than by testing:
//!
//! * a query planned before an archival commit and executed after it lost the
//!   window archival had just moved, because the partition was dropped in the
//!   same run that committed it;
//! * a scan whose merge-elision decision was judged against a snapshot older
//!   than the one it read, so a correction landing in between could be counted
//!   twice.
//!
//! Neither raises an error, and neither is reachable from a sequential fixture.
//! This suite is the one that runs the three roles at once and asserts the
//! property they must jointly preserve.
//!
//! # The invariants
//!
//! **A count over a fully-written, unchanging range is exact, always.** Archival
//! only ever *moves* a row between tiers, so no interleaving of archival with a
//! query may change what the query returns. A short count means a row is in
//! neither tier; a long one means it is in both.
//!
//! **A correction changes no count.** It is a new row at a higher version, so
//! the raw relation grows and the resolved one does not — including while
//! elision is deciding whether to rank versions at all.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them.
#![cfg(feature = "testkit")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use meterstore::MeterStore;
use meterstore::config::TableConfig;
use meterstore::testkit::{MeteringWorkload, TestHarness};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

/// The instant the workload starts from.
const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
/// Days written up front, and therefore the range every reader counts over.
const DAYS: i64 = 6;
/// Measuring points, one channel each.
const METERS: usize = 8;

/// Rows the workload writes: one quarter-hour value per meter per day.
const EXPECTED_ROWS: u64 = (DAYS as u64) * 96 * (METERS as u64);

/// A store, its harness, and the range it holds.
///
/// # The reader grace is set in the units of the clock the test drives
///
/// Archival takes `now` as a parameter, and this suite advances it a **day per
/// cycle** so that a window closes between reads instead of all at once. The
/// reader grace is measured on that same clock — deliberately, so that nothing
/// depends on a second one — which means the default hour is exhausted by the
/// first jump, and a reclamation would then be free to take a partition a reader
/// planned against.
///
/// That is not an artefact to work around; it is the contract, stated in the one
/// place it can be observed. A deployment that drives `now` faster than wall
/// clock — a catch-up after an outage, a backfill — gets exactly as much grace as
/// the clock it supplies. So the grace here is set wider than the span the test
/// covers, which is what a deployment doing the same thing has to do.
async fn seeded() -> (TestHarness, Arc<MeterStore>) {
    let harness = TestHarness::with_config(
        TableConfig::new(TestHarness::TABLE)
            .settlement_lag(Duration::DAY)
            .reader_grace(Duration::days(365))
            .build()
            .expect("config"),
    )
    .await
    .expect("harness");
    let workload = MeteringWorkload::new(START)
        .seed(0x5EED)
        .malo_ids(METERS)
        .days(DAYS);
    let (from, to) = workload.range();

    harness
        .ensure_partitions(from, to + Duration::days(2))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");

    let store = harness.store().await.expect("store");
    let series = workload.generate().expect("workload");
    harness.ingest(&store, &series).await.expect("ingest");

    (harness, Arc::new(store))
}

/// Count the resolved rows over the whole written range.
async fn resolved_count(store: &MeterStore) -> u64 {
    let sql = format!(r#"SELECT count(*) AS n FROM "{}""#, store.resolved_table());
    let result = store.query(&sql).await.expect("count");
    use datafusion::arrow::array::AsArray;
    result.batches()[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0) as u64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_is_exact_while_archival_moves_the_data_under_it() {
    // The property the tiering design exists to make true, asserted while the
    // boundary is actually moving rather than after it has stopped.
    let (_h, store) = seeded().await;
    assert_eq!(resolved_count(&store).await, EXPECTED_ROWS, "seeded");

    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));

    // Readers: count the same fixed range over and over. Every answer must be
    // the same number, whatever archival is doing to where the rows live.
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            let reads = Arc::clone(&reads);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let n = resolved_count(&store).await;
                    reads.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(
                        n, EXPECTED_ROWS,
                        "a count over an unchanging range changed while archival ran: \
                         got {n}, expected {EXPECTED_ROWS} — a window is in neither tier"
                    );
                    tokio::task::yield_now().await;
                }
            })
        })
        .collect();

    // The archiver, advancing the horizon one day per cycle so that a window
    // closes between reads rather than all at once.
    let archiver = {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            // A one-day settlement lag, so `START + n + 1` closes the nth window.
            for day in 1..=DAYS {
                let now = START + Duration::days(day + 1);
                store.archive(now, 4).await.expect("archive");
                tokio::task::yield_now().await;
            }
            stop.store(true, Ordering::Relaxed);
            store.watermark().await.expect("watermark")
        })
    };

    let watermark = archiver.await.expect("archiver task");
    for reader in readers {
        reader.await.expect("reader task");
    }

    assert!(
        reads.load(Ordering::Relaxed) >= 3,
        "the readers must actually have run"
    );
    assert!(
        watermark.get() > START,
        "archival must actually have moved the boundary: {watermark}"
    );
    assert_eq!(
        resolved_count(&store).await,
        EXPECTED_ROWS,
        "and the count is still exact once everything has settled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingest_archival_and_reads_together_leave_the_written_range_exact() {
    // All three roles at once, which is the only shape in which the write path's
    // DDL meets a scan. Ingest creates partitions ahead of the frontier and
    // attaches them; archival detaches behind it; readers enumerate partitions
    // and scan them by name. A partition attached between a reader's enumeration
    // and its scan is simply not read — correct, because it can only hold rows
    // written after that reader was entitled to see them.
    //
    // The invariant is stated over the range written *up front*, which no writer
    // touches: it must be exact throughout, whatever is being attached, detached
    // or committed alongside.
    let (_h, store) = seeded().await;
    let counted = format!(
        r#"SELECT count(*) AS n FROM "{}" WHERE "from" >= '{}' AND "from" < '{}'"#,
        store.resolved_table(),
        START
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap(),
        (START + Duration::days(DAYS))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap(),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));

    let readers: Vec<_> = (0..2)
        .map(|_| {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            let reads = Arc::clone(&reads);
            let sql = counted.clone();
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let result = store.query(&sql).await.expect("count");
                    use datafusion::arrow::array::AsArray;
                    let n = result.batches()[0]
                        .column(0)
                        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                        .value(0) as u64;
                    reads.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(
                        n, EXPECTED_ROWS,
                        "the range written up front must stay exact while ingest, \
                         archival and reads run together"
                    );
                    tokio::task::yield_now().await;
                }
            })
        })
        .collect();

    // The writer, filling days beyond the seeded range — so it is creating and
    // attaching partitions while the readers are enumerating them.
    let writer = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            for day in 0..3i64 {
                let ahead = MeteringWorkload::new(START + Duration::days(DAYS + day))
                    .seed(0xA11CE)
                    .malo_ids(METERS)
                    .days(1)
                    .generate()
                    .expect("workload");
                for delivery in &ahead {
                    store
                        .append(std::slice::from_ref(delivery))
                        .await
                        .expect("append");
                }
                tokio::task::yield_now().await;
            }
        })
    };

    let archiver = {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            for day in 1..=DAYS {
                store
                    .archive(START + Duration::days(day + 1), 4)
                    .await
                    .expect("archive");
                tokio::task::yield_now().await;
            }
            stop.store(true, Ordering::Relaxed);
        })
    };

    writer.await.expect("writer task");
    archiver.await.expect("archiver task");
    for reader in readers {
        reader.await.expect("reader task");
    }

    assert!(
        reads.load(Ordering::Relaxed) >= 2,
        "the readers must have run"
    );
    assert_eq!(
        resolved_count(&store).await,
        EXPECTED_ROWS + 3 * 96 * (METERS as u64),
        "and everything written is present once it has all settled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrections_landing_during_a_scan_are_never_double_counted() {
    // Merge elision decides whether to rank versions from per-file statistics,
    // and a late correction is exactly what makes ranking necessary. If the
    // decision is judged against a snapshot older than the files the scan reads,
    // elision fires on evidence that does not describe the scan and the
    // corrected interval comes back twice.
    //
    // Corrections here go to the cold tier (they restate already-archived
    // intervals), which is the only tier elision applies to.
    let (_h, store) = seeded().await;

    // Archive the whole range, so every correction below is a cold append and
    // every read is a cold-only scan — the elision path.
    store
        .archive(START + Duration::days(DAYS + 2), 16)
        .await
        .expect("archive");
    assert!(
        store.watermark().await.unwrap().get() >= START + Duration::days(DAYS),
        "the range must be entirely cold for this to exercise elision"
    );
    assert_eq!(resolved_count(&store).await, EXPECTED_ROWS, "archived");

    let stop = Arc::new(AtomicBool::new(false));
    let corrections = Arc::new(AtomicU64::new(0));

    let readers: Vec<_> = (0..3)
        .map(|_| {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let n = resolved_count(&store).await;
                    assert_eq!(
                        n, EXPECTED_ROWS,
                        "a correction is a new version of an existing reading, so the \
                         resolved count cannot change: got {n}, expected {EXPECTED_ROWS}"
                    );
                    tokio::task::yield_now().await;
                }
            })
        })
        .collect();

    // The corrector: restate day one at ascending versions, which is what an
    // MSCONS correction is.
    let corrector = {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let corrections = Arc::clone(&corrections);
        tokio::spawn(async move {
            for round in 1..=4u64 {
                let restated = MeteringWorkload::new(START)
                    .seed(0x5EED)
                    .malo_ids(METERS)
                    .days(1)
                    .version(20_260_720_000_000u128 + u128::from(round))
                    .generate()
                    .expect("restatement");
                for delivery in &restated {
                    store
                        .append(std::slice::from_ref(delivery))
                        .await
                        .expect("correction");
                }
                corrections.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
            stop.store(true, Ordering::Relaxed);
        })
    };

    corrector.await.expect("corrector task");
    for reader in readers {
        reader.await.expect("reader task");
    }

    assert_eq!(corrections.load(Ordering::Relaxed), 4);
    assert_eq!(
        resolved_count(&store).await,
        EXPECTED_ROWS,
        "the resolved view still holds one row per reading"
    );

    // And the raw relation grew, so the corrections really were stored rather
    // than deduplicated away — otherwise the assertion above proves nothing.
    let raw = format!(r#"SELECT count(*) AS n FROM "{}""#, store.raw_table());
    let result = store.query(&raw).await.expect("raw count");
    use datafusion::arrow::array::AsArray;
    let stored = result.batches()[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0) as u64;
    assert!(
        stored > EXPECTED_ROWS,
        "the audit trail must hold every version: {stored} rows for {EXPECTED_ROWS} readings"
    );
}
