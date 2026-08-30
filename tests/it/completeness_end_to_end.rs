//! Completeness against a real store, over the shapes the arithmetic can only be
//! trusted about end to end.
//!
//! The unit tests in `session::completeness` drive the roll-up directly with
//! hand-built daily rows. What they cannot show is that the **aggregate** feeding
//! it behaves the way the roll-up assumes: a `GROUP BY` yields no group for a day
//! with nothing in it, and for a long time the roll-up summed its expectation
//! over the groups it got back. A channel that stopped mid-range therefore
//! reported the range complete — the one answer this report must never give.
//!
//! So these run real deliveries with real holes in them through PostgreSQL and
//! Iceberg, and check the report against days that were never written.

#![cfg(feature = "testkit")]

use metering::interval::{MeterInterval, QualityFlag, Sparte};
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use meterstore::encode::StoredSeries;
use meterstore::testkit::TestHarness;
use time::macros::{date, datetime};
use time::{Duration, OffsetDateTime};

const MALO: &str = "12345678905";
const OBIS: &str = "1-0:1.29.0";

/// A quarter-hourly Lastgang covering `[from, to)` for `MALO` on `OBIS`.
fn series(from: OffsetDateTime, to: OffsetDateTime) -> StoredSeries {
    series_of(MALO, OBIS, from, to)
}

/// A quarter-hourly Lastgang for a named measuring point and channel.
fn series_of(malo: &str, obis: &str, from: OffsetDateTime, to: OffsetDateTime) -> StoredSeries {
    let intervals: Vec<MeterInterval> =
        std::iter::successors(Some(from), |t| Some(*t + Duration::minutes(15)))
            .take_while(|t| *t < to)
            .map(|t| MeterInterval {
                from: t,
                to: t + Duration::minutes(15),
                value: rust_decimal::Decimal::new(1, 0),
                quality: QualityFlag::Measured,
                obis_code: obis.parse().ok(),
            })
            .collect();

    let mut series = MeasurementSeries::new(
        malo.parse().expect("a valid MaLo-ID"),
        obis.parse().ok(),
        intervals,
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "9900000000001".parse().expect("Marktpartner-ID"),
        },
        from,
    );
    series.resolution = Some(metering::IntervalResolution::QuarterHour);
    StoredSeries::of(
        Sparte::Strom,
        series,
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("9900000000001", from, Sparte::Strom)
                .expect("scope"),
            meterstore::Version::new(20_260_727_000_001).expect("version"),
        ),
        datetime!(2026-07-27 06:00 UTC),
    )
}

/// A store holding `deliveries`, over `[from, to)` of hot partitions.
async fn store_with(
    from: OffsetDateTime,
    to: OffsetDateTime,
    deliveries: &[StoredSeries],
) -> (TestHarness, meterstore::MeterStore) {
    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(from, to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");
    let store = harness.store().await.expect("store");
    if !deliveries.is_empty() {
        harness.ingest(&store, deliveries).await.expect("ingest");
    }
    (harness, store)
}

/// The Berlin days of July 2026 are all 24 hours, so the arithmetic is plain.
const D01: OffsetDateTime = datetime!(2026-06-30 22:00 UTC); // 00:00 local, 1 July
const D02: OffsetDateTime = datetime!(2026-07-01 22:00 UTC);
const D03: OffsetDateTime = datetime!(2026-07-02 22:00 UTC);
const D04: OffsetDateTime = datetime!(2026-07-03 22:00 UTC);
const D06: OffsetDateTime = datetime!(2026-07-05 22:00 UTC);

#[tokio::test]
async fn a_channel_that_stopped_is_short_by_every_day_after_it_stopped() {
    // Two days delivered out of five. The aggregate returns two groups, and
    // summing the expectation over those two would report the range complete —
    // which is what it did.
    let (_h, store) = store_with(D01, D06, &[series(D01, D03)]).await;

    let report = store.completeness(D01, D06).await.expect("completeness");
    assert_eq!(report.len(), 1, "one channel: {report:?}");
    let row = &report[0];

    assert_eq!(row.actual, 96 * 2, "two days were delivered");
    assert_eq!(row.expected, 96 * 5, "five were asked about");
    assert_eq!(row.missing, 96 * 3);
    assert_eq!(row.surplus, 0, "a day nobody delivered is not a duplicate");
    assert!(!row.is_complete(), "{row:?}");
    assert_eq!(row.first_gap, Some(date!(2026 - 07 - 03)));
    assert!(
        !row.is_silent(),
        "it delivered something, so the roster is not what finds it"
    );
}

#[tokio::test]
async fn a_whole_day_missing_from_the_middle_is_found() {
    // The least visible form: deliveries either side of the hole, so nothing
    // about the channel's totals looks unusual and no roster can help.
    let (_h, store) = store_with(D01, D04, &[series(D01, D02), series(D03, D04)]).await;

    let report = store.completeness(D01, D04).await.expect("completeness");
    let row = &report[0];

    assert_eq!(row.actual, 96 * 2);
    assert_eq!(row.expected, 96 * 3);
    assert_eq!(row.missing, 96, "the 2nd of July, in full");
    assert_eq!(row.first_gap, Some(date!(2026 - 07 - 02)));
}

#[tokio::test]
async fn a_complete_range_is_still_complete() {
    // The other direction, and the one a false alarm would break: walking every
    // day of the range must not invent a gap at either end of a range that is
    // day-aligned and fully delivered.
    let (_h, store) = store_with(D01, D04, &[series(D01, D04)]).await;

    let report = store.completeness(D01, D04).await.expect("completeness");
    let row = &report[0];

    assert_eq!((row.expected, row.actual), (96 * 3, 96 * 3));
    assert_eq!((row.missing, row.surplus), (0, 0));
    assert!(row.is_complete(), "{row:?}");
    assert_eq!(row.first_gap, None);
}

#[tokio::test]
async fn a_range_that_reaches_past_the_last_delivery_says_so() {
    // Stated rather than discovered: the report is defined over its range, so a
    // range extending past what has been delivered reports the remainder as
    // missing. Consulting a clock instead would make one report answer
    // differently on two runs.
    let (_h, store) = store_with(D01, D06, &[series(D01, D02)]).await;

    let over_the_delivered_day = store.completeness(D01, D02).await.expect("completeness");
    assert!(
        over_the_delivered_day[0].is_complete(),
        "the day that was delivered is complete: {over_the_delivered_day:?}"
    );

    let over_the_week = store.completeness(D01, D06).await.expect("completeness");
    assert_eq!(over_the_week[0].missing, 96 * 4);
}

#[tokio::test]
async fn a_report_can_be_narrowed_to_one_meter_and_one_channel() {
    // The narrowing every other read on a store already has. Without it the only
    // way to ask about one meter is to compute the whole portfolio's report and
    // filter the answer — a scan of everything for a question about one thing.
    let other = "10000000009";
    let export = "1-0:2.29.0";
    let (_h, store) = store_with(
        D01,
        D04,
        &[
            series(D01, D04),
            series_of(MALO, export, D01, D02),
            series_of(other, OBIS, D01, D04),
        ],
    )
    .await;

    let everything = store.completeness(D01, D04).await.expect("completeness");
    assert_eq!(everything.len(), 3, "three channels: {everything:?}");

    let one_meter = store
        .completeness(D01, D04)
        .malo(MALO)
        .expect("a valid MaLo-ID")
        .await
        .expect("completeness");
    assert_eq!(one_meter.len(), 2, "two channels of one meter");
    assert!(one_meter.iter().all(|r| r.malo_id == MALO));

    let one_channel = store
        .completeness(D01, D04)
        .malo(MALO)
        .expect("a valid MaLo-ID")
        // The non-canonical spelling, which storage does not hold: it is
        // canonicalised on the way in, so the report is not silently empty.
        .obis("1-0:1.29.0*255")
        .expect("a valid OBIS code")
        .await
        .expect("completeness");
    assert_eq!(one_channel.len(), 1);
    assert_eq!(one_channel[0].obis_code, OBIS);
    assert!(one_channel[0].is_complete(), "{one_channel:?}");

    // And the export channel, which stopped after the first day, is short — so
    // the narrowing did not also narrow the *range*.
    let stopped = store
        .completeness(D01, D04)
        .obis(export)
        .expect("a valid OBIS code")
        .await
        .expect("completeness");
    assert_eq!(stopped.len(), 1);
    assert_eq!(stopped[0].missing, 96 * 2);
}

#[tokio::test]
async fn narrowing_applies_to_the_roster_as_well_as_the_range() {
    // The roster and the report have to see the same population. Drawn wider
    // than the report, every channel outside the narrowing would be absent from
    // it and come back as *silent* — a page of findings about meters nobody
    // asked about.
    let other = "10000000009";
    let (_h, store) = store_with(
        D01,
        D06,
        &[series(D01, D06), series_of(other, OBIS, D01, D02)],
    )
    .await;

    let narrowed = store
        .completeness(D03, D06)
        .malo(MALO)
        .expect("a valid MaLo-ID")
        .seen_since(D01)
        .await
        .expect("completeness");

    assert_eq!(
        narrowed.len(),
        1,
        "only the meter asked about: {narrowed:?}"
    );
    assert!(narrowed[0].is_complete());
    assert!(
        !narrowed.iter().any(|r| r.is_silent()),
        "the other meter is outside the narrowing, not silent"
    );
}

#[tokio::test]
async fn a_column_that_is_not_declared_is_refused_rather_than_interpolated() {
    // A column name reaches SQL as an identifier and no dialect parameterises
    // one, so the check against the store's declared columns is what stands in
    // for a bound parameter. The error names what is available rather than
    // letting a fragment through.
    let (_h, store) = store_with(D01, D02, &[]).await;

    let err = store
        .completeness(D01, D02)
        .column_eq(
            "tenant",
            datafusion::common::ScalarValue::Utf8(Some("a".into())),
        )
        .expect_err("this table declares no tenant column")
        .to_string();
    assert!(err.contains("tenant"), "{err}");
    assert!(
        err.contains("malo_id"),
        "the message names what is accepted: {err}"
    );

    // A mistyped identifier fails at the call rather than returning an empty
    // report, which would read as "this meter is fine".
    assert!(store.completeness(D01, D02).malo("nonsense").is_err());
    assert!(store.completeness(D01, D02).obis("nonsense").is_err());
}
