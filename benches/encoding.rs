//! Benchmarks for the paths every row travels.
//!
//! §18 states targets and admits none of them are measured. These close the part
//! that can be measured without infrastructure: the per-row cost of encoding,
//! decoding and planning, which is what the archival-throughput and query-latency
//! targets are ultimately made of.
//!
//! # What is here and what is not
//!
//! Here: encode, decode, the tier split, predicate extraction and elision
//! planning. All CPU-bound, all deterministic, all on the hot path for every row
//! or every query.
//!
//! Not here: archival throughput, query latency, and the compression ratio.
//! Those need a real PostgreSQL and a real object store, so they belong to a
//! harness-driven suite rather than to `criterion` — measuring them against
//! in-memory fakes would produce numbers that look like the §18 targets and mean
//! nothing.
//!
//! # Why the fixtures are shaped like real deliveries
//!
//! A benchmark over uniform synthetic rows measures the allocator. These use one
//! measuring point's day at quarter-hour resolution — 96 intervals, the unit an
//! MSCONS delivery actually arrives in — and scale by repeating deliveries rather
//! than by widening one, because that is how volume actually grows.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use metering::interval::{MeterInterval, QualityFlag};
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use meterstore::encode::{StoredSeries, from_record_batch, to_record_batch};
use meterstore::planner::{TimeRange, split};
use meterstore::watermark::TieringWatermark;
use meterstore::{ScopedVersion, Version, VersionScope};
use rust_decimal::Decimal;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

/// The start of the benchmark day.
const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// Intervals in a German quarter-hour day. Not a DST day, deliberately: the
/// irregular ones belong in correctness tests, not in a throughput baseline.
const INTERVALS_PER_DAY: usize = 96;

/// One measuring point's day, as a delivery would carry it.
fn delivery(meter: usize) -> StoredSeries {
    let intervals: Vec<MeterInterval> = (0..INTERVALS_PER_DAY)
        .map(|i| {
            let from = START + Duration::minutes(15 * i as i64);
            MeterInterval {
                from,
                to: from + Duration::minutes(15),
                // Values that vary, so delta encoding and decimal handling do
                // realistic work rather than compressing a constant away.
                value_kwh: Decimal::new(100 + (i as i64 * 7) % 500, 2),
                quality: QualityFlag::Measured,
                obis_code: "1-0:1.8.0".parse().ok(),
            }
        })
        .collect();

    let series = MeasurementSeries::new(
        format!("{:011}", 10_000_000_000u64 + meter as u64),
        "1-0:1.8.0".parse().ok(),
        intervals,
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "9900000000001".to_string(),
        },
        START,
    );

    StoredSeries::new(
        series,
        ScopedVersion::new(
            VersionScope::for_interval("9900000000001", START).expect("scope"),
            Version::new(20_260_701_000_001).expect("version"),
        ),
        START,
    )
}

fn deliveries(n: usize) -> Vec<StoredSeries> {
    (0..n).map(delivery).collect()
}

/// Encoding: `MeasurementSeries` into Arrow. Every ingested row passes here.
fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    for meters in [1usize, 10, 100] {
        let batch = deliveries(meters);
        let rows = (meters * INTERVALS_PER_DAY) as u64;
        group.throughput(Throughput::Elements(rows));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &batch, |b, batch| {
            b.iter(|| to_record_batch(black_box(batch)).expect("encode"));
        });
    }
    group.finish();
}

/// Decoding: Arrow back into the domain type. Every typed read passes here.
fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    for meters in [1usize, 10, 100] {
        let batch = to_record_batch(&deliveries(meters)).expect("encode");
        let rows = (meters * INTERVALS_PER_DAY) as u64;
        group.throughput(Throughput::Elements(rows));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &batch, |b, batch| {
            b.iter(|| from_record_batch(black_box(batch)).expect("decode"));
        });
    }
    group.finish();
}

/// The full round trip, which is what archival does to every row.
fn bench_round_trip(c: &mut Criterion) {
    let batch = deliveries(10);
    let rows = (10 * INTERVALS_PER_DAY) as u64;

    let mut group = c.benchmark_group("round_trip");
    group.throughput(Throughput::Elements(rows));
    group.bench_function("encode_then_decode", |b| {
        b.iter(|| {
            let encoded = to_record_batch(black_box(&batch)).expect("encode");
            from_record_batch(black_box(&encoded)).expect("decode")
        });
    });
    group.finish();
}

/// The tier split. Runs once per scan, so it must not be where time goes.
fn bench_split(c: &mut Criterion) {
    let watermark = TieringWatermark::new(START);
    let spanning = TimeRange::between(START - Duration::days(7), START + Duration::days(7));

    c.bench_function("planner/split_spanning", |b| {
        b.iter(|| split(black_box(spanning), black_box(watermark)));
    });
}

/// Predicate extraction: pulling a range out of DataFusion's filter expressions.
///
/// Conservative by construction, and on the planning path for every query — a
/// query with no recognised bound scans both tiers in full, so this is also
/// where a missed optimisation costs the most.
fn bench_predicate(c: &mut Criterion) {
    use datafusion::logical_expr::{col, lit};
    use datafusion::scalar::ScalarValue;

    let ts = |t: OffsetDateTime| {
        lit(ScalarValue::TimestampMicrosecond(
            Some((t.unix_timestamp_nanos() / 1_000) as i64),
            Some("UTC".into()),
        ))
    };
    let filters = vec![
        col("from").gt_eq(ts(START)),
        col("from").lt(ts(START + Duration::days(30))),
        col("malo_id").eq(lit("12345678901")),
    ];

    c.bench_function("planner/time_range", |b| {
        b.iter(|| meterstore::planner::time_range(black_box(&filters)));
    });
}

/// Merge-elision planning over per-file statistics.
///
/// Scales with the number of data files a range touches, so a month of history
/// at 512 MiB per file is tens to hundreds — the sizes benchmarked here.
fn bench_elision(c: &mut Criterion) {
    use meterstore::planner::{VersionStats, version};

    let mut group = c.benchmark_group("planner/elision");
    for files in [16usize, 256, 4096] {
        // Every file at one version: the common case, and the one that has to
        // scan the whole list before it can prove anything.
        let stats: Vec<Option<VersionStats>> =
            vec![Some(VersionStats::single(20_260_701_000_001)); files];

        group.throughput(Throughput::Elements(files as u64));
        group.bench_with_input(BenchmarkId::from_parameter(files), &stats, |b, stats| {
            b.iter(|| version::plan(black_box(stats)))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_encode,
    bench_decode,
    bench_round_trip,
    bench_split,
    bench_predicate,
    bench_elision,
);
criterion_main!(benches);
