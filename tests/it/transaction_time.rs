//! Transaction-time reads: the resolved value **as it was known at** an instant,
//! end to end against real PostgreSQL.
//!
//! `store.as_known_at(t)` pins the row-level `recorded_at` axis rather than an
//! Iceberg snapshot, so — unlike `as_of` — it works on the hot tier too. Only
//! versions recorded by `t` enter resolution, so a correction delivered later,
//! and an interval first stored later, are both invisible; a read without the
//! ceiling sees current knowledge.

#![cfg(feature = "testkit")]

use metering::QualityFlag;
use metering::interval::MeterInterval;
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use meterstore::testkit::TestHarness;
use rust_decimal::Decimal;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

// Transaction-time instants: T1 stores the original, T2 the correction.
const T1: OffsetDateTime = datetime!(2026-07-26 06:00 UTC);
const T_MID: OffsetDateTime = datetime!(2026-07-26 12:00 UTC);
const T2: OffsetDateTime = datetime!(2026-07-27 06:00 UTC);

/// One interval at `from`, at a value, version and transaction time.
fn stored(
    from: OffsetDateTime,
    kwh: i64,
    version: u128,
    recorded_at: OffsetDateTime,
) -> meterstore::encode::StoredSeries {
    let series = MeasurementSeries::new(
        "12345678901",
        "1-0:1.8.0".parse().ok(),
        vec![MeterInterval {
            from,
            to: from + Duration::minutes(15),
            value: Decimal::new(kwh, 0),
            quality: QualityFlag::Measured,
            obis_code: "1-0:1.8.0".parse().ok(),
        }],
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        },
        recorded_at,
    );
    meterstore::encode::StoredSeries::new(
        series,
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("99", from).unwrap(),
            meterstore::Version::new(version).unwrap(),
        ),
        recorded_at,
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

/// Read the single interval's value from the resolved series.
async fn value_at(store: &meterstore::MeterStore, from: OffsetDateTime) -> Option<Decimal> {
    let series = store
        .series("12345678901")
        .range(from, from + Duration::minutes(15))
        .collect()
        .await
        .expect("collect");
    series.and_then(|s| s.intervals.first().map(|i| i.value))
}

#[tokio::test]
async fn as_known_at_sees_the_value_in_force_at_that_time() {
    let (_h, store) = store().await;

    // Original recorded at T1, correction (higher version) recorded at T2.
    store
        .append(&[stored(START, 10, 20_260_726_060_000, T1)])
        .await
        .expect("append original");
    store
        .append(&[stored(START, 42, 20_260_727_060_000, T2)])
        .await
        .expect("append correction");

    // Current knowledge: the correction wins.
    assert_eq!(value_at(&store, START).await, Some(Decimal::new(42, 0)));

    // As known at T_MID (after the original, before the correction): the original.
    let mid = store.as_known_at(T_MID).await.expect("as_known_at");
    assert_eq!(
        value_at(&mid, START).await,
        Some(Decimal::new(10, 0)),
        "the correction was not yet known at T_MID"
    );

    // As known at T2: the correction is now visible.
    let after = store.as_known_at(T2).await.expect("as_known_at");
    assert_eq!(value_at(&after, START).await, Some(Decimal::new(42, 0)));
}

#[tokio::test]
async fn as_known_at_excludes_an_interval_first_stored_later() {
    // The set-membership property the hand-rolled correction overlay could not
    // give: an interval that did not exist yet is absent, not merely unchanged.
    let (_h, store) = store().await;

    store
        .append(&[stored(START, 10, 20_260_726_060_000, T1)])
        .await
        .expect("append first interval");
    // A second interval, first stored at T2.
    let later = START + Duration::minutes(15);
    store
        .append(&[stored(later, 20, 20_260_727_060_000, T2)])
        .await
        .expect("append later interval");

    let mid = store.as_known_at(T_MID).await.expect("as_known_at");
    assert_eq!(
        value_at(&mid, START).await,
        Some(Decimal::new(10, 0)),
        "the interval known at T_MID is present"
    );
    assert_eq!(
        value_at(&mid, later).await,
        None,
        "the interval first stored at T2 did not exist at T_MID"
    );

    // Current knowledge holds both.
    assert_eq!(value_at(&store, later).await, Some(Decimal::new(20, 0)));
}

#[tokio::test]
async fn as_known_at_reads_the_same_after_archival_to_cold() {
    // The reproducibility claim: archival only moves a row (with its recorded_at)
    // from hot to cold, so a transaction-time read returns the same value whether
    // the row is served hot or cold.
    let (_h, store) = store().await;
    store
        .append(&[stored(START, 10, 20_260_726_060_000, T1)])
        .await
        .expect("append original");
    store
        .append(&[stored(START, 42, 20_260_727_060_000, T2)])
        .await
        .expect("append correction");

    // Archive START's day into the cold tier.
    store
        .archive(START + Duration::days(2), 8)
        .await
        .expect("archive");
    assert!(
        store.watermark().await.expect("watermark").get() > START,
        "the interval's day must have moved into cold"
    );

    let mid = store.as_known_at(T_MID).await.expect("as_known_at");
    assert_eq!(
        value_at(&mid, START).await,
        Some(Decimal::new(10, 0)),
        "the as-of value is the same read from cold"
    );
    let after = store.as_known_at(T2).await.expect("as_known_at");
    assert_eq!(value_at(&after, START).await, Some(Decimal::new(42, 0)));
}

#[tokio::test]
async fn the_ceiling_reaches_the_raw_versions_relation_too() {
    // The ceiling used to live only inside the resolution plan, so `readings`
    // honoured it and `readings_versions` — the audit relation, and the one a
    // correction history is read from — did not. A session that says it
    // reproduces a past state must not hand back rows it had not yet been told
    // about, whichever of its two relations is queried.
    let (_h, store) = store().await;
    store
        .append(&[stored(START, 10, 20_260_726_060_000, T1)])
        .await
        .expect("append original");
    store
        .append(&[stored(START, 42, 20_260_727_060_000, T2)])
        .await
        .expect("append correction");

    let raw = store.raw_table();
    let count = |s: meterstore::MeterStore, sql: String| async move {
        let result = s.query(&sql).await.expect("query");
        result.batches().iter().map(|b| b.num_rows()).sum::<usize>()
    };

    // Current knowledge holds both versions of the interval.
    assert_eq!(
        count(store.clone(), format!("SELECT version FROM {raw}")).await,
        2
    );

    let mid = store.as_known_at(T_MID).await.expect("as_known_at");
    assert_eq!(
        count(mid, format!("SELECT version FROM {raw}")).await,
        1,
        "only the version recorded by T_MID was known then"
    );
}

#[tokio::test]
async fn a_ceiling_before_any_delivery_is_empty() {
    let (_h, store) = store().await;
    store
        .append(&[stored(START, 10, 20_260_726_060_000, T1)])
        .await
        .expect("append");

    let before = store
        .as_known_at(datetime!(2026-07-01 00:00 UTC))
        .await
        .expect("as_known_at");
    assert_eq!(
        value_at(&before, START).await,
        None,
        "nothing had been recorded yet"
    );
}
