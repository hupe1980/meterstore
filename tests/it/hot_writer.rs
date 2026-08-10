//! The bulk ingest path, and the boundary it refuses to cross.
//!
//! `MeterStore::append` routes each interval to the tier that owns it, and pays
//! for that with an Iceberg metadata load on every call — correct for a delivery
//! that might contain a late correction, wrong as the steady-state path for a
//! service landing batches continuously.
//!
//! `hot_writer()` reads the boundary once. The whole safety argument is that it
//! **refuses** anything below that boundary rather than routing it: routing on a
//! stale snapshot could place a row below the true watermark, where no query
//! looks, and refusing cannot. These tests are about that refusal.

#![cfg(feature = "testkit")]

use meterstore::testkit::{MeteringWorkload, Oracle, TestHarness};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// A store with the first day archived, so a boundary genuinely exists.
async fn split_store() -> (TestHarness, meterstore::MeterStore) {
    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(START, START + Duration::days(5))
        .await
        .expect("partitions");
    harness.seed_watermark(START).await.expect("watermark");
    let store = harness.store().await.expect("store");

    let seed = MeteringWorkload::new(START)
        .seed(0x5EED)
        .malo_ids(2)
        .days(2);
    harness
        .ingest(&store, &seed.generate().expect("workload"))
        .await
        .expect("ingest");
    store
        .archive(START + Duration::days(2), 1)
        .await
        .expect("archive");

    (harness, store)
}

/// Current data, starting well above the boundary.
fn current() -> Vec<meterstore::encode::StoredSeries> {
    MeteringWorkload::new(START + Duration::days(2))
        .seed(0xC0FFEE)
        .malo_offset(100)
        .malo_ids(2)
        .days(2)
        .generate()
        .expect("workload")
}

#[tokio::test]
async fn current_data_writes_without_a_catalog_round_trip_per_batch() {
    // The point of the type. One boundary read, then any number of batches.
    let (_h, store) = split_store().await;

    let writer = store.hot_writer().await.expect("writer");
    let boundary = writer.watermark();
    assert!(
        boundary.get() > START,
        "the fixture must have archived something, or there is no boundary to reuse"
    );

    let series = current();
    let mut written = 0;
    for delivery in series.chunks(3) {
        written += writer.append(delivery).await.expect("append");
    }

    let expected: usize = series.iter().map(|s| s.series.intervals.len()).sum();
    assert_eq!(written as usize, expected, "every interval must land");

    // The boundary did not move, and nothing was stranded below it.
    assert_eq!(writer.watermark(), boundary);
    store
        .verify_invariant()
        .await
        .expect("no row may sit below the watermark in PostgreSQL");
}

#[tokio::test]
async fn an_interval_below_the_boundary_is_refused_rather_than_routed() {
    // The safety argument. A hot writer that quietly routed this to Iceberg —
    // or worse, wrote it to PostgreSQL — would be trading a round trip for the
    // one failure the tiering invariant exists to prevent.
    let (_h, store) = split_store().await;
    let writer = store.hot_writer().await.expect("writer");

    // START is below the boundary by construction: the first day was archived.
    let archived = MeteringWorkload::new(START)
        .seed(0x1A7E)
        .malo_offset(200)
        .malo_ids(1)
        .days(1)
        .generate()
        .expect("workload");

    let err = writer
        .append(&archived)
        .await
        .expect_err("a below-boundary interval must be refused");

    let msg = err.to_string();
    assert!(msg.contains("below the tier boundary"), "{msg}");
    assert!(
        msg.contains("MeterStore::append"),
        "the message must name the entry point that does route: {msg}"
    );
}

#[tokio::test]
async fn a_mixed_batch_fails_whole_rather_than_landing_its_current_half() {
    // Partial success is the worst outcome: the caller would have to work out
    // which intervals made it, and the answer depends on iteration order.
    let (_h, store) = split_store().await;
    let writer = store.hot_writer().await.expect("writer");

    let before = store
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("count");

    let mut mixed = current();
    mixed.extend(
        MeteringWorkload::new(START)
            .seed(0x81ED)
            .malo_offset(300)
            .malo_ids(1)
            .days(1)
            .generate()
            .expect("workload"),
    );

    writer
        .append(&mixed)
        .await
        .expect_err("a mixed batch must be refused");

    let after = store
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("count");

    use meterstore::arrow::array::AsArray;
    let scalar = |r: &meterstore::QueryResult| {
        r.batches()[0]
            .column(0)
            .as_primitive::<meterstore::arrow::datatypes::Int64Type>()
            .value(0)
    };
    assert_eq!(
        scalar(&before),
        scalar(&after),
        "nothing may be written when the batch is refused"
    );
}

#[tokio::test]
async fn a_redelivery_is_a_no_op_rather_than_an_error() {
    // Every transport worth using delivers at least once, so replay is ordinary
    // traffic. The returned count is what was *inserted*, so a caller can tell
    // a replay from a loss.
    let (_h, store) = split_store().await;
    let writer = store.hot_writer().await.expect("writer");
    let series = current();

    let first = writer.append(&series).await.expect("first delivery");
    let second = writer.append(&series).await.expect("redelivery");

    assert!(first > 0);
    assert_eq!(
        second, 0,
        "a replayed batch inserts nothing and errors on nothing"
    );
}

#[tokio::test]
async fn the_result_matches_what_the_routing_path_would_have_written() {
    // The two entry points must agree about the data, or choosing the fast one
    // would be choosing a different answer. Same workload, same store shape,
    // compared against the oracle rather than against each other.
    let (harness, store) = split_store().await;
    let series = current();

    let writer = store.hot_writer().await.expect("writer");
    writer.append(&series).await.expect("hot writer");

    let mut oracle = Oracle::new();
    oracle.record(&series).expect("oracle");

    let (from, to) = (START + Duration::days(2), START + Duration::days(4));
    let result = store
        .query(&format!(
            "SELECT COUNT(*) FROM readings WHERE \"from\" >= '{from}' AND \"from\" < '{to}'",
            from = from
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
            to = to
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        ))
        .await
        .expect("query");

    use meterstore::arrow::array::AsArray;
    let counted = result.batches()[0]
        .column(0)
        .as_primitive::<meterstore::arrow::datatypes::Int64Type>()
        .value(0);
    assert_eq!(counted as u64, oracle.row_count(from, to));

    drop(harness);
}

#[tokio::test]
async fn concurrent_writers_can_open_the_same_new_partition() {
    // The ordinary ingest topology: N stateless workers landing batches, all of
    // which call `ensure_partitions` for the day they are writing. The first
    // batch of a new day therefore has every worker trying to create the same
    // partition at the same moment.
    let harness = TestHarness::start().await.expect("harness");
    let store = harness.store().await.expect("store");

    // A day no partition covers yet, one worker per measuring point.
    let fresh = START + Duration::days(30);
    let mut tasks = Vec::new();
    for worker in 0..8u32 {
        let store = store.clone();
        let batch = MeteringWorkload::new(fresh)
            .seed(0xA11CE + u64::from(worker))
            .malo_offset(200 + worker as usize)
            .malo_ids(1)
            .days(1)
            .generate()
            .expect("workload");
        tasks.push(tokio::spawn(async move {
            store
                .hot_writer()
                .await
                .expect("writer")
                .append(&batch)
                .await
        }));
    }

    for task in tasks {
        let result = task.await.expect("join");
        assert!(
            result.is_ok(),
            "a concurrent writer must not lose the partition-creation race: {result:?}"
        );
    }
}
