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
/// # The grace runs out here on purpose
///
/// Archival takes `now` as a parameter and this suite advances it a **day per
/// cycle**, so the default hour of `reader_grace` is exhausted by the first
/// jump. Reclamation is therefore live throughout: every cycle is free to take
/// the partition it just archived, while three readers plan and execute against
/// the boundary it is moving.
///
/// That is the point. The grace is measured on the clock the caller drives —
/// deliberately, so nothing depends on a second one — and a deployment catching
/// up after an outage drives it just as fast. Widening the grace instead — to a
/// year, say — keeps reclamation out of the way, and then the interleaving this
/// suite exists to test never meets a partition being taken.
///
/// **What this does not prove.** These readers plan and execute in one step, so
/// the window between the two is microseconds and the reclamation floor each
/// plan registers is never what saves them. The floor is proved where it can be:
/// a plan held across an archival run, in `query_end_to_end`.
async fn seeded() -> (TestHarness, Arc<MeterStore>) {
    let harness = TestHarness::with_config(
        TableConfig::new(TestHarness::TABLE)
            .settlement_lag(Duration::DAY)
            .reader_grace(Duration::HOUR)
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
    let (h, store) = seeded().await;
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
                store.admin().archive(now, 4).await.expect("archive");
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

    // And reclamation was **live** for all of it. Without this the suite would
    // keep passing if the grace were widened back out, which is the state it
    // spent its life in: every count exact because no partition was ever taken.
    use meterstore::tiering::store::HotStore;
    assert!(
        h.hot()
            .reclaimed_below(TestHarness::TABLE)
            .await
            .expect("reclaimed_below")
            .is_some(),
        "no partition was reclaimed, so nothing here was tested against reclamation"
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
                    .admin()
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
        .admin()
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

// ── Two replicas ────────────────────────────────────────────────────────────
//
// Everything above runs three roles in **one** process against one pool. The
// archive lease exists for a different shape: every replica of a deployment runs
// the same schedule, and exactly one must win each table. A session-scoped
// advisory lock is what makes that true, and "session-scoped" is the trap — a
// lock taken on a connection that then returns to the pool is released the moment
// another caller checks it out.
//
// A second `PostgresHot` over its **own** `PgPool` is what a second replica
// actually is: the same database, the same warehouse, no shared connection.

/// A second replica over the same database and warehouse as the harness.
///
/// Its own pool, so its advisory lock is taken on a session the first replica
/// cannot reach — which is the whole of what is being tested. Sharing the
/// harness's pool would make the lock re-entrant and the test vacuous.
async fn second_replica(harness: &TestHarness) -> Arc<meterstore::PostgresHot> {
    let pool = sqlx::PgPool::connect(harness.url())
        .await
        .expect("second replica pool");
    Arc::new(meterstore::PostgresHot::new(pool))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leaked_lease_is_cleared_when_its_connection_is_next_used() {
    // A lease is a *session* advisory lock on a connection the lease owns. A
    // future dropped at an await point — a task abort, a `timeout`, shutdown —
    // skips the release, and the connection goes back to the pool still holding
    // it. Every other session is then refused, and it is reported as
    // `lease_contended`: the one outcome operators are told not to alert on.
    //
    // The re-entrancy is what made it permanent. Advisory locks nest per session,
    // so handing the connection back out let `pg_try_advisory_lock` succeed —
    // count two — and the single unlock on release left it at one. The lock was
    // then held forever by a connection nobody was using.
    //
    // Asserted from a **second session**, because that is the only place the
    // difference shows: on the leaking connection itself the re-entrancy makes
    // every attempt succeed whether or not anything was cleared.
    use meterstore::tiering::HotStore;

    let (harness, _store) = seeded().await;
    let table = harness.config().name();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(harness.url())
        .await
        .expect("single-connection pool");
    let leaky = meterstore::PostgresHot::new(pool);
    let observer = second_replica(&harness).await;

    // Leak it: take the lease and drop it without releasing.
    drop(
        leaky
            .try_archive_lease(table)
            .await
            .expect("lease query")
            .expect("an uncontended lease is granted"),
    );
    assert!(
        observer
            .try_archive_lease(table)
            .await
            .expect("lease query")
            .is_none(),
        "the leaked lock really is still held, or this test proves nothing"
    );

    // The leaking pool has one connection, so the next lease gets it back —
    // clears what was leaked, takes the lock cleanly, and releases it.
    leaky
        .try_archive_lease(table)
        .await
        .expect("lease query")
        .expect("the leaking pool must be able to lease its own table")
        .release()
        .await
        .expect("release");

    assert!(
        observer
            .try_archive_lease(table)
            .await
            .expect("lease query")
            .is_some(),
        "the lock is still held after a clean acquire-and-release: the leaked \
         count was decremented rather than cleared, so archival stays wedged"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_one_replica_holds_is_refused_to_another_and_released_to_it() {
    // The mechanism, asserted directly rather than inferred from a race: while A
    // holds the lease B cannot take it, and once A releases it B can. Both halves
    // matter — a lock that is never granted is as broken as one always granted,
    // and it is the second half that a connection returning to the pool breaks.
    use meterstore::tiering::HotStore;

    let (harness, _store) = seeded().await;
    let table = harness.config().name();
    let a = harness.hot().clone();
    let b = second_replica(&harness).await;

    let held = a
        .try_archive_lease(table)
        .await
        .expect("lease query")
        .expect("an uncontended lease is granted");

    assert!(
        b.try_archive_lease(table)
            .await
            .expect("lease query")
            .is_none(),
        "a second replica took a lease the first holds"
    );

    // A different table is a different lock: one busy table must not stop a
    // deployment archiving the others.
    assert!(
        b.try_archive_lease("some_other_table")
            .await
            .expect("lease query")
            .is_some(),
        "the lease is per table, not per deployment"
    );

    held.release().await.expect("release");

    let after = b
        .try_archive_lease(table)
        .await
        .expect("lease query")
        .expect("the lease is available once released");
    after.release().await.expect("release");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_replicas_racing_to_archive_leave_the_range_exact() {
    // The deployment shape the lease exists for, run rather than argued. Two
    // archivers, two pools, one table, the same `now` — repeatedly.
    //
    // What is asserted is safety, not who wins: scheduling decides that, and a
    // test that required a particular winner would be asserting the runtime's
    // behaviour rather than this crate's. A losing replica reports
    // `lease_contended` and changes nothing, so across any interleaving the row
    // count must be untouched, the watermark must only advance, and the invariant
    // must hold at every step.
    use meterstore::{Archiver, tiering::ColdStore};

    let (harness, store) = seeded().await;
    let before = resolved_count(&store).await;
    assert_eq!(before, EXPECTED_ROWS, "the fixture is what it claims");

    let table = harness.config().name();
    let a = Archiver::new(
        harness.hot().clone(),
        harness.cold().clone(),
        harness.config().clone(),
    );
    let b = Archiver::new(
        second_replica(&harness).await,
        harness.cold().clone(),
        harness.config().clone(),
    );

    let mut watermark = harness.cold().watermark(table).await.expect("watermark");
    let mut contended = 0usize;
    let mut archived = 0usize;

    for day in 1..=DAYS + 1 {
        let now = START + Duration::days(day + 1);
        let (ra, rb) = tokio::join!(a.run_once(now), b.run_once(now));
        let (ra, rb) = (ra.expect("replica a"), rb.expect("replica b"));

        for outcome in [&ra, &rb] {
            if outcome.lease_contended {
                contended += 1;
            }
            if outcome.archived_anything() {
                archived += 1;
            }
        }

        // Both replicas doing real work in one cycle is the failure the lease
        // exists to prevent: they would target the window above the same
        // watermark and one would commit rows the other had already taken.
        assert!(
            !(ra.archived_anything() && rb.archived_anything()),
            "both replicas archived in one cycle: a={ra:?} b={rb:?}"
        );

        let now_watermark = harness.cold().watermark(table).await.expect("watermark");
        assert!(
            now_watermark >= watermark,
            "the watermark moved backwards: {watermark} -> {now_watermark}"
        );
        watermark = now_watermark;

        // Checked every cycle rather than at the end: a violation that appears
        // and is then archived over would be invisible to a final assertion.
        a.verify_invariant()
            .await
            .expect("invariant after the race");

        assert_eq!(
            resolved_count(&store).await,
            before,
            "archival moved rows between tiers; it must not change how many there are"
        );
    }

    assert!(archived > 0, "neither replica ever archived anything");
    // Reported rather than asserted: contention is a scheduling outcome, and a
    // run where the two never overlapped is a valid run of a correct system. In
    // practice every cycle contends — 7 archived, 7 contended, repeatably — so
    // the race is real rather than two archivers politely taking turns.
    println!("two-replica race: {archived} archived, {contended} contended");
}
