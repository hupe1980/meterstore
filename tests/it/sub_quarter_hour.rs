//! Resolutions finer than 15 minutes.
//!
//! iMSys already delivers 1-minute and 1-second values over the SMGW HAN
//! interface, and §14a steering needs them; the quarter-hour is the *settlement*
//! grain, not the only one a store will be asked to hold. Nothing in MeterStore
//! is written against 96 — the expected interval count is asked of
//! [`metering::calendar::intervals_in_day`] per declared resolution — but until
//! this file existed, every test fixture in the crate declared `PT15M`, so that
//! was a claim about the design rather than a fact about the code.
//!
//! The two things that could break and would break quietly:
//!
//! - **The declared resolution round-trip.** `IntervalResolution::Custom(60)`
//!   renders `PT60S`, and if storage could not read that back, completeness
//!   would silently fall through to "no declared resolution" and report every
//!   series as unmeasurable rather than as short.
//! - **The DST day.** A 25-hour local day holds 1500 one-minute intervals, not
//!   1440. A count derived from a flat 86 400 would call a 60-interval gap
//!   complete — the direction that reaches a bill.

#![cfg(feature = "testkit")]

use metering::resolution::IntervalResolution;
use metering::{Sparte, calendar};
use meterstore::testkit::{MeteringWorkload, Oracle, TestHarness};
use time::macros::{date, datetime};
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// The rows a workload produces, ingested with the first day archived.
async fn split_store(workload: &MeteringWorkload) -> (TestHarness, meterstore::MeterStore, Oracle) {
    let (from, to) = workload.range();

    let harness = TestHarness::start().await.expect("harness");
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

    store
        .archive(from + Duration::days(2), 1)
        .await
        .expect("archive");

    (harness, store, oracle)
}

#[test]
fn the_domain_counts_sub_quarter_hour_intervals_per_calendar_day() {
    // The arithmetic everything below depends on. Asserted directly first, so a
    // failure downstream is attributable to storage rather than to the calendar.
    let minute = IntervalResolution::from_seconds(60).expect("a minute is a resolution");
    assert_eq!(minute, IntervalResolution::Custom(60));

    assert_eq!(
        calendar::intervals_in_day(date!(2026 - 07 - 20), minute),
        Some(1440),
        "an ordinary day"
    );
    assert_eq!(
        calendar::intervals_in_day(date!(2026 - 03 - 29), minute),
        Some(1380),
        "the 23-hour spring day"
    );
    assert_eq!(
        calendar::intervals_in_day(date!(2026 - 10 - 25), minute),
        Some(1500),
        "the 25-hour autumn day — the direction a flat 1440 gets dangerously wrong"
    );
}

#[test]
fn a_sub_quarter_hour_resolution_round_trips_through_storage() {
    // `Custom(n)` renders `PT{n}S`, which is not one of the named spellings. If
    // the column could not read it back, completeness would report every series
    // as unmeasurable instead of as complete or short — a silent downgrade.
    for seconds in [1u32, 30, 60, 300, 600] {
        let declared = IntervalResolution::from_seconds(seconds).expect("positive");
        let rendered = declared.to_iso8601();
        let parsed: IntervalResolution =
            rendered.parse().expect("storage reads back what it wrote");
        assert_eq!(parsed, declared, "{rendered} did not round-trip");
    }
}

#[test]
fn a_resolution_that_does_not_divide_a_day_is_refused_by_the_generator() {
    // Not a storage rule — a fixture rule. A ragged final interval would make
    // these tests fail for a reason that has nothing to do with resolution.
    assert!(
        MeteringWorkload::new(START)
            .resolution(Duration::minutes(7))
            .is_err()
    );
    assert!(
        MeteringWorkload::new(START)
            .resolution(Duration::ZERO)
            .is_err()
    );
    assert!(
        MeteringWorkload::new(START)
            .resolution(Duration::minutes(1))
            .is_ok()
    );
}

#[tokio::test]
async fn one_minute_data_survives_both_tiers_intact() {
    // 1440 intervals per meter per day rather than 96 — fifteen times the row
    // count, through the same archival window, the same keyset cursor and the
    // same merge resolution.
    let workload = MeteringWorkload::new(START)
        .seed(0x60_5EC)
        .resolution(Duration::minutes(1))
        .expect("a minute divides a day")
        .malo_ids(2)
        .days(2);
    let (_h, store, oracle) = split_store(&workload).await;
    let (from, to) = workload.range();

    let result = store
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("query");
    let counted =
        meterstore::arrow::util::display::array_value_to_string(result.batches()[0].column(0), 0)
            .expect("render");

    assert_eq!(
        counted,
        oracle.row_count(from, to).to_string(),
        "no row may be lost to the finer grain"
    );
    // And the fixture must actually be fine-grained, or this proves nothing.
    assert_eq!(
        oracle.row_count(from, to),
        2 * 2 * 1440,
        "two meters, two 24-hour days, one value a minute"
    );
}

#[tokio::test]
async fn completeness_measures_a_one_minute_series_against_the_right_day_length() {
    // The assertion this whole file exists for. A store that assumed 96 — or
    // that derived the expectation from a flat 86 400 — would call the autumn
    // day complete while 60 intervals were missing.
    let workload = MeteringWorkload::new(datetime!(2026-10-25 00:00 UTC))
        .seed(0xD57)
        .resolution(Duration::minutes(1))
        .expect("a minute divides a day")
        .malo_ids(1)
        .days(1);

    let (from, to) = workload.range();
    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(from - Duration::days(1), to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");
    let store = harness.store().await.expect("store");
    harness
        .ingest(&store, &workload.generate().expect("workload"))
        .await
        .expect("ingest");

    // The local day 2026-10-25 runs 22:00Z on the 24th to 23:00Z on the 25th —
    // 25 hours, 1500 minutes. Asked over exactly that span, a complete series
    // reports 1500 expected and no gap.
    let local_start = calendar::day_start_utc(date!(2026 - 10 - 25));
    let local_end = local_start + calendar::day_length(date!(2026 - 10 - 25));
    assert_eq!(local_end - local_start, Duration::hours(25));

    let rows = store
        .completeness(local_start, local_end)
        .await
        .expect("completeness");
    let row = rows.first().expect("one channel reported");

    assert_eq!(row.resolution.as_deref(), Some("PT60S"));
    assert!(
        row.is_measurable(),
        "a declared sub-quarter-hour resolution must yield an expectation"
    );
    assert_eq!(
        row.expected, 1500,
        "25 local hours at one value a minute — not 1440"
    );
}

#[tokio::test]
async fn a_gap_in_one_minute_data_is_reported_rather_than_absorbed() {
    // The complement: with the expectation right, a shortfall has to show up as
    // one. A store that reported `expected == actual` whatever arrived would
    // pass the test above and still be useless.
    let workload = MeteringWorkload::new(START)
        .seed(0x6A9)
        .resolution(Duration::minutes(1))
        .expect("a minute divides a day")
        .malo_ids(1)
        .days(1)
        .with_gaps(0.10);

    let (from, to) = workload.range();
    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(from - Duration::days(1), to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");
    let store = harness.store().await.expect("store");
    harness
        .ingest(&store, &workload.generate().expect("workload"))
        .await
        .expect("ingest");

    let local_start = calendar::day_start_utc(date!(2026 - 07 - 20));
    let local_end = local_start + calendar::day_length(date!(2026 - 07 - 20));
    let rows = store
        .completeness(local_start, local_end)
        .await
        .expect("completeness");
    let row = rows.first().expect("one channel reported");

    assert_eq!(row.expected, 1440, "an ordinary day of minutes");
    assert!(row.missing > 0, "a 10 % gap rate must leave gaps");
    assert!(!row.is_complete());
    assert!(
        row.first_gap.is_some(),
        "and the operator must be told which day to look at"
    );
}

#[tokio::test]
async fn a_sub_quarter_hour_water_series_works_like_any_other() {
    // The two features compose: neither is a special case built beside the
    // other. Sub-minute *and* m³, through both tiers.
    let workload = MeteringWorkload::new(START)
        .seed(0xC0_1D)
        .sparte(Sparte::Wasser)
        .resolution(Duration::minutes(5))
        .expect("five minutes divides a day")
        .malo_ids(2)
        .days(2);
    let (_h, store, oracle) = split_store(&workload).await;
    let (from, to) = workload.range();

    let result = store
        .query("SELECT COUNT(*) FROM readings WHERE unit = 'M3'")
        .await
        .expect("query");
    let counted =
        meterstore::arrow::util::display::array_value_to_string(result.batches()[0].column(0), 0)
            .expect("render");

    assert_eq!(counted, oracle.row_count(from, to).to_string());
    assert_eq!(oracle.row_count(from, to), 2 * 2 * 288);
}
