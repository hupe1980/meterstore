//! The typed read builder's retrieval options, end to end against real
//! PostgreSQL: the newest interval without scanning the history, and a
//! quality filter pushed into the scan rather than applied by the caller.
//!
//! These exist so an application layer stops hand-rolling "latest sample" and
//! "exclude FAULTY/UNKNOWN" over a materialised whole-series read — the two
//! generic retrievals every meter-data consumer needs.

#![cfg(feature = "testkit")]

use metering::QualityFlag;
use metering::interval::MeterInterval;
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use meterstore::testkit::TestHarness;
use rust_decimal::Decimal;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// One channel carrying several 15-minute intervals, each with its own quality.
fn reading(intervals: &[(i64, QualityFlag)], version: u128) -> meterstore::encode::StoredSeries {
    on_channel("1-0:1.8.0", intervals, version)
}

/// [`reading`], on a named channel — a measuring point has several.
fn on_channel(
    obis: &str,
    intervals: &[(i64, QualityFlag)],
    version: u128,
) -> meterstore::encode::StoredSeries {
    let ivs: Vec<MeterInterval> = intervals
        .iter()
        .enumerate()
        .map(|(i, (kwh, quality))| {
            let from = START + Duration::minutes(15 * i as i64);
            MeterInterval {
                from,
                to: from + Duration::minutes(15),
                value: Decimal::new(*kwh, 0),
                quality: *quality,
                obis_code: obis.parse().ok(),
            }
        })
        .collect();
    let series = MeasurementSeries::new(
        "12345678905".parse().unwrap(),
        obis.parse().ok(),
        ivs,
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        },
        datetime!(2026-07-26 06:00 UTC),
    );
    meterstore::encode::StoredSeries::new(
        series,
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("99", START, metering::interval::Sparte::Strom)
                .unwrap(),
            meterstore::Version::new(version).unwrap(),
        ),
        datetime!(2026-07-26 06:00 UTC),
    )
}

async fn store() -> (TestHarness, meterstore::MeterStore) {
    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(START, START + Duration::days(2))
        .await
        .expect("partitions");
    harness.seed_watermark(START).await.expect("watermark");
    let store = harness.store().await.expect("store");
    (harness, store)
}

#[tokio::test]
async fn latest_returns_the_newest_interval() {
    let (_h, store) = store().await;
    store
        .append(&[reading(
            &[
                (10, QualityFlag::Measured),
                (20, QualityFlag::Measured),
                (30, QualityFlag::Measured),
            ],
            20_260_720_000_001,
        )])
        .await
        .expect("append");

    let latest = store
        .series("12345678905")
        .unwrap()
        .latest()
        .await
        .expect("latest")
        .expect("a reading exists");

    assert_eq!(
        latest.from,
        START + Duration::minutes(30),
        "the newest start"
    );
    assert_eq!(latest.value, Decimal::new(30, 0));
}

#[tokio::test]
async fn latest_on_an_empty_range_is_absence() {
    let (_h, store) = store().await;
    let latest = store
        .series("12345678905")
        .unwrap()
        .range(START + Duration::days(1), START + Duration::days(2))
        .latest()
        .await
        .expect("latest");
    assert!(latest.is_none(), "no rows in the range");
}

#[tokio::test]
async fn quality_in_filters_to_the_accepted_set() {
    let (_h, store) = store().await;
    store
        .append(&[reading(
            &[
                (10, QualityFlag::Measured),
                (20, QualityFlag::Faulty),
                (30, QualityFlag::Substituted),
            ],
            20_260_720_000_001,
        )])
        .await
        .expect("append");

    // Billable-only: exclude FAULTY (and UNKNOWN, absent here).
    let billable = store
        .series("12345678905")
        .unwrap()
        .quality_in(&[
            QualityFlag::Measured,
            QualityFlag::Estimated,
            QualityFlag::Substituted,
            QualityFlag::Calculated,
            QualityFlag::Corrected,
            QualityFlag::Preliminary,
        ])
        .collect()
        .await
        .expect("collect")
        .expect("some intervals survive");
    assert_eq!(
        billable.intervals.len(),
        2,
        "the FAULTY interval is dropped"
    );
    assert!(
        billable
            .intervals
            .iter()
            .all(|i| i.quality != QualityFlag::Faulty)
    );

    // Measured only.
    let measured = store
        .series("12345678905")
        .unwrap()
        .quality_in(&[QualityFlag::Measured])
        .intervals()
        .await
        .expect("intervals");
    assert_eq!(measured.len(), 1);
    assert_eq!(measured[0].value, Decimal::new(10, 0));
}

#[tokio::test]
async fn an_empty_quality_filter_is_a_no_op() {
    let (_h, store) = store().await;
    store
        .append(&[reading(
            &[(10, QualityFlag::Measured), (20, QualityFlag::Faulty)],
            20_260_720_000_001,
        )])
        .await
        .expect("append");

    let all = store
        .series("12345678905")
        .unwrap()
        .quality_in(&[])
        .intervals()
        .await
        .expect("intervals");
    assert_eq!(all.len(), 2, "an empty filter keeps every interval");
}

#[tokio::test]
async fn latest_respects_the_quality_filter() {
    // The current MEASURED reading, ignoring a newer FAULTY interval — the
    // combination a "last good reading" display needs.
    let (_h, store) = store().await;
    store
        .append(&[reading(
            &[
                (10, QualityFlag::Measured),
                (20, QualityFlag::Measured),
                (30, QualityFlag::Faulty),
            ],
            20_260_720_000_001,
        )])
        .await
        .expect("append");

    let last_good = store
        .series("12345678905")
        .unwrap()
        .quality_in(&[QualityFlag::Measured])
        .latest()
        .await
        .expect("latest")
        .expect("a measured reading exists");
    assert_eq!(last_good.from, START + Duration::minutes(15));
    assert_eq!(last_good.value, Decimal::new(20, 0));
}

#[tokio::test]
async fn a_measuring_point_reads_back_channel_by_channel() {
    // The gap this closes. `collect` refuses a range spanning two channels — and
    // the refusal is right, folding import and export doubles the month — but a
    // measuring point *is* a set of registers, and a billing period projecting
    // Bezug across HT, NT and total is a question about all of them.
    //
    // Before this, the only way to ask was a hand-written `SELECT DISTINCT
    // obis_code` followed by one typed read per channel: SQL in front of the API
    // whose whole point is that callers do not write SQL, and `1 + N` round trips
    // with version resolution and the tier split held outside the store by
    // convention.
    let (_h, store) = store().await;
    store
        .append(&[
            on_channel(
                "1-0:1.8.0",
                &[(10, QualityFlag::Measured)],
                20_260_720_000_001,
            ),
            on_channel(
                "1-0:1.8.1",
                &[(4, QualityFlag::Measured)],
                20_260_720_000_002,
            ),
            on_channel(
                "1-0:1.8.2",
                &[(6, QualityFlag::Measured)],
                20_260_720_000_003,
            ),
        ])
        .await
        .expect("append");

    // The list, without decoding an interval.
    let channels = store
        .series("12345678905")
        .unwrap()
        .range(START, START + Duration::days(1))
        .channels()
        .await
        .expect("channels");
    assert_eq!(
        channels.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["1-0:1.8.0", "1-0:1.8.1", "1-0:1.8.2"],
        "in OBIS order"
    );

    // The whole point, in one scan.
    let (by_channel, provenance) = store
        .series("12345678905")
        .unwrap()
        .range(START, START + Duration::days(1))
        .collect_by_channel_with_provenance()
        .await
        .expect("collect_by_channel");

    assert_eq!(by_channel.len(), 3);
    let total: Decimal = by_channel
        .values()
        .flat_map(|r| r.series.intervals.iter().map(|i| i.value))
        .sum();
    assert_eq!(total, Decimal::new(20, 0), "10 + 4 + 6, each counted once");

    for (channel, resolved) in &by_channel {
        assert_eq!(
            resolved.series.obis_code.as_ref(),
            Some(channel),
            "each series names its own channel"
        );
        assert!(
            resolved
                .series
                .intervals
                .iter()
                .all(|i| i.obis_code.as_ref() == Some(channel)),
            "and carries no other channel's intervals"
        );
    }

    // One boundary for the whole map, which is what the `1 + N` spelling cannot
    // say: there, each channel is read against a boundary observed at a different
    // moment.
    assert_eq!(provenance.watermarks().len(), 1);

    // And the single-channel read still refuses, so nothing was loosened.
    let err = store
        .series("12345678905")
        .unwrap()
        .range(START, START + Duration::days(1))
        .collect()
        .await
        .expect_err("three channels cannot be one MeasurementSeries");
    assert!(err.to_string().contains("two channels"), "{err}");
}

#[tokio::test]
async fn the_channel_list_is_narrowed_by_everything_the_read_was() {
    // A list that ignored the builder's filters would be worse than no list: an
    // operator would act on channels the read they are about to run cannot see.
    let (_h, store) = store().await;
    store
        .append(&[
            on_channel(
                "1-0:1.8.0",
                &[(10, QualityFlag::Measured)],
                20_260_720_000_001,
            ),
            on_channel(
                "1-0:2.8.0",
                &[(3, QualityFlag::Measured)],
                20_260_720_000_002,
            ),
        ])
        .await
        .expect("append");

    // Narrowed by the range: a window holding nothing lists nothing.
    let none = store
        .series("12345678905")
        .unwrap()
        .range(START + Duration::days(1), START + Duration::days(2))
        .channels()
        .await
        .expect("channels");
    assert!(none.is_empty(), "{none:?}");

    // Narrowed by the channel filter itself, which is the degenerate case.
    let one = store
        .series("12345678905")
        .unwrap()
        .obis("1-0:2.8.0")
        .unwrap()
        .channels()
        .await
        .expect("channels");
    assert_eq!(
        one.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["1-0:2.8.0"]
    );

    // And `channels` takes `&self`, so the builder survives to be collected.
    let query = store
        .series("12345678905")
        .unwrap()
        .obis("1-0:1.8.0")
        .unwrap();
    assert_eq!(query.channels().await.expect("channels").len(), 1);
    let series = query.collect().await.expect("collect").expect("rows");
    assert_eq!(series.intervals.len(), 1);
}
