//! What a write reports about the value it displaced.
//!
//! The distinction these exist to pin: **being stored and becoming current are
//! not the same thing.** An append-only store accepts a backfill at an older
//! version and accepts a replay, and neither changes what a query returns. A
//! caller writing a correction-audit row on every accepted write would record
//! changes that did not happen.
//!
//! The reason this belongs in the store rather than in the caller is atomicity:
//! reading the prior state in a separate query races the write, and is wrong
//! exactly when two corrections arrive together — which is when an audit trail
//! is worth having.

#![cfg(feature = "testkit")]

use metering::QualityFlag;
use metering::interval::MeterInterval;
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use meterstore::session::Effect;
use meterstore::testkit::TestHarness;
use rust_decimal::Decimal;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// One reading, at a chosen value, version and quality.
fn reading(kwh: i64, version: u128, quality: QualityFlag) -> meterstore::encode::StoredSeries {
    let mut series = MeasurementSeries::new(
        "12345678901",
        "1-0:1.8.0".parse().ok(),
        vec![MeterInterval {
            from: START,
            to: START + Duration::minutes(15),
            value_kwh: Decimal::new(kwh, 0),
            quality,
            obis_code: "1-0:1.8.0".parse().ok(),
        }],
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        },
        datetime!(2026-07-26 06:00 UTC),
    );
    series.resolution = Some(metering::IntervalResolution::QuarterHour);

    meterstore::encode::StoredSeries::new(
        series,
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("99", START).unwrap(),
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
async fn a_first_value_reports_an_insert_and_supersedes_nothing() {
    let (_h, store) = store().await;

    let outcome = store
        .append(&[reading(10, 20_260_720_000_001, QualityFlag::Measured)])
        .await
        .expect("append");

    assert_eq!(outcome.displacements.len(), 1);
    let d = &outcome.displacements[0];
    assert_eq!(d.effect, Effect::Inserted);
    assert!(d.superseded.is_none());
    assert_eq!(d.written.value, Decimal::new(10, 0));
    assert!(d.effect.changed_current_value());
    // The interval boundaries both come off the written row: a §60 audit row
    // built from a displacement covers `[from, to)`, not a zero-width `[from, from)`.
    assert_eq!(d.from, START);
    assert_eq!(
        d.to,
        START + Duration::minutes(15),
        "the displacement carries the interval end, not a collapse to its start"
    );
}

#[tokio::test]
async fn a_correction_reports_what_it_superseded() {
    // The § 60 Abs. 6 shape: an audit row needs the value that stopped being
    // current, and getting it from a separate query races the write.
    let (_h, store) = store().await;
    store
        .append(&[reading(10, 20_260_720_000_001, QualityFlag::Measured)])
        .await
        .expect("original");

    let outcome = store
        .append(&[reading(42, 20_260_728_000_002, QualityFlag::Corrected)])
        .await
        .expect("correction");

    let d = &outcome.displacements[0];
    assert_eq!(d.effect, Effect::Superseded);

    let prior = d
        .superseded
        .as_ref()
        .expect("the prior value must be reported");
    assert_eq!(prior.value, Decimal::new(10, 0));
    assert_eq!(prior.version.version().get(), 20_260_720_000_001);
    assert_eq!(prior.quality, QualityFlag::Measured);

    assert_eq!(d.written.value, Decimal::new(42, 0));
    assert!(d.value_changed(), "10 -> 42 is a change in the amount");
    assert_eq!(d.current().value, Decimal::new(42, 0));
}

#[tokio::test]
async fn a_backfill_behind_a_newer_version_is_shadowed_not_superseding() {
    // The case a naive design gets wrong. The row is accepted and joins the
    // audit trail, but an existing higher version still wins — so nothing a
    // query returns changed, and no correction row should be written.
    let (_h, store) = store().await;
    store
        .append(&[reading(42, 20_260_728_000_002, QualityFlag::Corrected)])
        .await
        .expect("newer arrives first");

    let outcome = store
        .append(&[reading(10, 20_260_720_000_001, QualityFlag::Measured)])
        .await
        .expect("older backfill is still accepted");

    let d = &outcome.displacements[0];
    assert_eq!(d.effect, Effect::Shadowed);
    assert!(
        !d.effect.changed_current_value(),
        "a shadowed write must not gate an audit row"
    );
    assert_eq!(
        d.current().value,
        Decimal::new(42, 0),
        "and the caller must be told what still holds"
    );
    assert!(!d.value_changed());
}

#[tokio::test]
async fn a_replay_reports_a_duplicate_rather_than_a_second_insert() {
    // Every transport worth using delivers at least once, so replay is ordinary
    // traffic — but it must not look like a new reading.
    let (_h, store) = store().await;
    let delivery = reading(10, 20_260_720_000_001, QualityFlag::Measured);

    store
        .append(std::slice::from_ref(&delivery))
        .await
        .expect("first");
    let outcome = store.append(&[delivery]).await.expect("replay");

    let d = &outcome.displacements[0];
    assert_eq!(d.effect, Effect::Duplicate);
    assert!(!d.effect.changed_current_value());
    assert_eq!(outcome.hot_rows, 0, "a replay writes nothing");
}

#[tokio::test]
async fn a_quality_change_at_an_unchanged_value_is_visible() {
    // The § 60 Abs. 2 shape: a substitute replaced by a measurement discharges
    // the obligation, and the quantity can be identical. A report carrying only
    // the value would show nothing happened.
    let (_h, store) = store().await;
    store
        .append(&[reading(10, 20_260_720_000_001, QualityFlag::Substituted)])
        .await
        .expect("substitute");

    let outcome = store
        .append(&[reading(10, 20_260_728_000_002, QualityFlag::Measured)])
        .await
        .expect("measurement");

    let d = &outcome.displacements[0];
    assert_eq!(d.effect, Effect::Superseded);
    assert!(d.quality_changed(), "SUBSTITUTED -> MEASURED");
    assert!(!d.value_changed(), "and the quantity did not move");
    assert_eq!(
        d.superseded.as_ref().unwrap().quality,
        QualityFlag::Substituted
    );
}

#[tokio::test]
async fn two_versions_in_one_batch_report_against_each_other() {
    // A batch may carry a value and its correction. The second must report the
    // first as superseded, not whatever preceded the batch — otherwise an audit
    // trail built from one delivery is internally inconsistent.
    let (_h, store) = store().await;

    let outcome = store
        .append(&[
            reading(10, 20_260_720_000_001, QualityFlag::Measured),
            reading(42, 20_260_728_000_002, QualityFlag::Corrected),
        ])
        .await
        .expect("append");

    assert_eq!(outcome.displacements.len(), 2);
    let first = &outcome.displacements[0];
    let second = &outcome.displacements[1];

    assert_eq!(first.effect, Effect::Inserted);
    assert_eq!(second.effect, Effect::Superseded);
    assert_eq!(
        second.superseded.as_ref().expect("prior").value,
        Decimal::new(10, 0),
        "the second must supersede the first, not what preceded the batch"
    );
}

#[tokio::test]
async fn the_report_matches_what_a_query_returns() {
    // The report is a convenience; `readings` stays authoritative. If they ever
    // disagreed, the convenience would be worse than no convenience.
    let (_h, store) = store().await;
    store
        .append(&[reading(10, 20_260_720_000_001, QualityFlag::Measured)])
        .await
        .expect("original");
    let outcome = store
        .append(&[reading(42, 20_260_728_000_002, QualityFlag::Corrected)])
        .await
        .expect("correction");

    let result = store
        .query("SELECT CAST(SUM(value) AS BIGINT) FROM readings")
        .await
        .expect("query");
    use meterstore::arrow::array::AsArray;
    let total = result.batches()[0]
        .column(0)
        .as_primitive::<meterstore::arrow::datatypes::Int64Type>()
        .value(0);

    assert_eq!(
        Decimal::new(total, 0),
        outcome.displacements[0].current().value,
        "the reported current value must be the one a query returns"
    );
}
