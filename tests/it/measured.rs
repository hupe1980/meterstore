//! The performance figures that need real storage, measured rather than asserted.
//!
//! The whole argument for tiering is that a row store cannot hold this volume
//! economically, and the ">10× vs Postgres" figure supporting it had never been
//! measured. Neither had archival throughput. Both need a real PostgreSQL and a
//! real object store — against fakes they would produce numbers that look like
//! the targets and mean nothing — so they live here rather than in `criterion`.
//!
//! # These are floors, not the published figures
//!
//! A test container on a developer machine is not the reference hardware
//! (8 vCPU / 32 GiB, S3 in-region), and the volumes here are thousands of rows
//! rather than millions. So the assertions are deliberately loose: they catch a
//! **regression or a collapse** — compression falling to nothing because an
//! encoding stopped applying, throughput falling off a cliff — without pretending
//! to be an acceptance measurement.
//!
//! What they print is more useful than what they assert. Run with `--nocapture`
//! to see the actual numbers.
//!
//! # Why the comparison is fair
//!
//! Postgres is measured with `pg_total_relation_size`, which includes indexes and
//! per-row overhead. That is the honest comparison: the alternative being argued
//! against is *keeping the data in PostgreSQL*, and a deployment doing that pays
//! for the index too. Comparing against `pg_relation_size` would flatter the lake
//! by excluding the thing that makes the row store expensive at this volume.

#![cfg(feature = "testkit")]

use meterstore::testkit::{MeteringWorkload, TestHarness};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// Total bytes of every Parquet file in the warehouse.
fn cold_bytes(harness: &TestHarness) -> u64 {
    harness
        .parquet_files()
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum()
}

/// Bytes PostgreSQL uses for the hot table, indexes and row overhead included.
///
/// Summed over the **partition tree**, not the parent. A declaratively
/// partitioned parent holds no rows of its own, so `pg_total_relation_size` on it
/// alone returns zero — which would make the lake look infinitely better than
/// the thing it is being compared against.
async fn hot_bytes(harness: &TestHarness) -> u64 {
    let size: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(SUM(pg_total_relation_size(relid)), 0)::bigint \
         FROM pg_partition_tree('\"{}\"')",
        TestHarness::TABLE
    ))
    .fetch_one(harness.hot().pool())
    .await
    .expect("partition tree size");
    size.max(0) as u64
}

#[tokio::test]
async fn the_cold_tier_compresses_against_postgres_row_storage() {
    // The headline number, and the whole argument for tiering. Measured on a small
    // volume, so the assertion is a floor rather than the published figure —
    // but a ratio near 1 would mean an encoding silently stopped applying, and
    // that is exactly what this is here to catch.
    let workload = MeteringWorkload::new(START)
        .seed(0xC0A1)
        .malo_ids(50)
        .days(4);
    let (from, to) = workload.range();

    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(from, to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");

    let store = harness.store().await.expect("store");
    let series = workload.generate().expect("workload");
    harness.ingest(&store, &series).await.expect("ingest");

    // Give PostgreSQL its best case: statistics current and pages compacted, so
    // the comparison is not flattered by transient bloat from the load.
    sqlx::query(&format!("VACUUM ANALYZE \"{}\"", TestHarness::TABLE))
        .execute(harness.hot().pool())
        .await
        .expect("vacuum");

    let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM \"{}\"", TestHarness::TABLE))
        .fetch_one(harness.hot().pool())
        .await
        .expect("count");
    let postgres = hot_bytes(&harness).await;

    // Everything to the cold tier, then measure what it cost.
    store
        .archive(to + Duration::days(2), 64)
        .await
        .expect("archive");
    let iceberg = cold_bytes(&harness);

    assert!(iceberg > 0, "archival wrote nothing");
    let ratio = postgres as f64 / iceberg as f64;

    println!(
        "compression: {rows} rows — postgres {postgres} B ({:.1} B/row), \
         parquet {iceberg} B ({:.1} B/row), ratio {ratio:.1}×",
        postgres as f64 / rows as f64,
        iceberg as f64 / rows as f64,
    );

    // The target is >10×, and this fixture clears it by an order of
    // magnitude — but read the number with its caveats, which run in *both*
    // directions:
    //
    // Flattering it: the fixture has 50 measuring points, one OBIS code, one
    // source and one version scope, so the dictionary-encoded columns compress
    // to almost nothing. A real deployment has higher `malo_id` cardinality.
    // The `source_detail` and `provenance` JSON also repeat per row in
    // PostgreSQL while Parquet dictionary-encodes them away — a genuine
    // columnar advantage for this schema, but one this schema maximises.
    //
    // Working against it: at this volume the per-file Parquet footer — schema,
    // statistics, bloom filters, page index — is a large fixed cost that a real
    // 512 MiB partition amortises away.
    //
    // The assertion is the target itself, because the fixture clears it
    // comfortably and a drop below would mean an encoding stopped applying.
    assert!(
        ratio > 10.0,
        "columnar storage should be an order of magnitude smaller than row \
         storage; got {ratio:.1}× — check that delta encoding, dictionary \
         encoding and ZSTD are all still applying"
    );
}

#[tokio::test]
async fn archival_throughput_does_not_collapse() {
    // The target is 500 k rows/s on reference hardware. This is a container on a
    // developer machine over a few thousand rows, so it cannot check that — what
    // it catches is a change that makes archival *orders of magnitude* slower,
    // such as a per-row round trip creeping back into the scan.
    let workload = MeteringWorkload::new(START)
        .seed(0x7409)
        .malo_ids(60)
        .days(3);
    let (from, to) = workload.range();

    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(from, to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");

    let store = harness.store().await.expect("store");
    let series = workload.generate().expect("workload");
    harness.ingest(&store, &series).await.expect("ingest");

    let started = std::time::Instant::now();
    let outcomes = store
        .archive(to + Duration::days(2), 64)
        .await
        .expect("archive");
    let elapsed = started.elapsed();

    let rows: u64 = outcomes.iter().map(|o| o.rows).sum();
    assert!(rows > 0, "nothing was archived, so nothing was measured");

    let per_second = rows as f64 / elapsed.as_secs_f64();
    println!(
        "archival: {rows} rows in {:.2?} — {per_second:.0} rows/s \
         (target 500 000 rows/s on reference hardware)",
        elapsed
    );

    // Far below the target, and expected to be: each window here is ~5 700
    // rows, so the fixed per-window cost — catalog compare-and-swap, file
    // creation, footer — dominates entirely. A real 9.6 M-row window amortises
    // all of it. The number above is the useful output; what follows is only a
    // floor against pathology.
    //
    // **The scan's round-trip shape is not guarded here.** A wall clock cannot
    // distinguish a per-row round trip from a loaded machine: on this workload
    // the two differ by a factor of a few, and an assertion tight enough to
    // catch the first fires constantly on the second. That property is exact and
    // belongs in an exact test — `a_range_scan_streams_in_bounded_chunks` pins
    // the round-trip *count* against the chunk size, where 25 rows in chunks of
    // ten is three statements and nothing else passes.
    //
    // So this floor sits where only something catastrophic reaches it: ~2 600
    // rows/s unloaded on a developer machine, ~650 with three suites competing
    // for the same cores and Docker daemon. A hundred means archival took three
    // minutes to move seventeen thousand rows.
    assert!(
        per_second > 100.0,
        "archival managed only {per_second:.0} rows/s over {rows} rows — that is \
         not slow, it is broken"
    );
}

#[tokio::test]
async fn archival_memory_does_not_scale_with_the_window() {
    // The budget is < 512 MiB steady state, and the design's claim is stronger
    // than a budget: *nothing on the path holds a window*. That is a structural
    // property, so it is checked structurally — the scan is asked for a chunk
    // far smaller than the partition, and archival must still complete.
    //
    // A materialising implementation passes this too; what it would fail is the
    // row count, because a chunked scan whose cursor is not unique silently
    // drops rows at every chunk boundary. Both properties are asserted together
    // because a small chunk is what makes the second one bite.
    use meterstore::config::TableConfig;

    let workload = MeteringWorkload::new(START)
        .seed(0xC401)
        .malo_ids(20)
        .days(2);
    let (from, to) = workload.range();

    let harness = TestHarness::with_config(
        TableConfig::new(TestHarness::TABLE)
            .settlement_lag(Duration::DAY)
            // Far below one partition's row count, so a day is paged many times.
            .scan_chunk_rows(64)
            .build()
            .expect("config"),
    )
    .await
    .expect("harness");

    harness
        .ensure_partitions(from, to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");

    let store = harness.store().await.expect("store");
    let series = workload.generate().expect("workload");
    let expected: u64 = series.iter().map(|s| s.series.intervals.len() as u64).sum();
    harness.ingest(&store, &series).await.expect("ingest");

    let outcomes = store
        .archive(to + Duration::days(2), 64)
        .await
        .expect("archive");
    let archived: u64 = outcomes.iter().map(|o| o.rows).sum();

    assert_eq!(
        archived, expected,
        "a 64-row chunk over a {expected}-row window dropped rows: the keyset \
         cursor is not unique per row"
    );
    store.verify_invariant().await.expect("invariant");
}
