//! Gas, heat and water — the three Sparten that are not electricity.
//!
//! A store that only ever saw Strom cannot tell whether it is carrying the unit
//! or merely storing it. Water is the case that settles the question: it is
//! metered **and** billed in m³ ([`Sparte::billing_unit`]), so a `value` column
//! that implied kWh would be wrong for every row, and a `SUM` across a mixed
//! portfolio would add volumes to energies with nothing to notice.
//!
//! What is asserted here:
//!
//! 1. A water series round-trips through both tiers with its unit intact.
//! 2. Gas can be stored on either side of the Brennwert conversion — m³ as
//!    registered, kWh once converted — because both are true of real deliveries.
//! 3. A unit the commodity cannot be expressed in is refused at the write, not
//!    discovered in a bill.
//! 4. A mixed-commodity table can be summed *per unit*, which is the query a
//!    multi-Sparte operator actually runs.

#![cfg(feature = "testkit")]

use metering::interval::{MeasurementUnit, Sparte};
use meterstore::encode::StoredSeries;
use meterstore::testkit::{MeteringWorkload, Oracle, TestHarness};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// A store holding `workload`'s output, with the first day archived.
///
/// Archiving is not incidental: the unit has to survive the Arrow → Parquet →
/// Iceberg round trip as well as the PostgreSQL one, and those are different
/// code paths with different type mappings.
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
        .admin()
        .archive(from + Duration::days(2), 1)
        .await
        .expect("archive");

    (harness, store, oracle)
}

/// The single value a one-column, one-row query produces, as a string.
///
/// Rendered rather than cast: a decimal compared through `f64` is a decimal
/// this crate has stopped guaranteeing.
async fn scalar(store: &meterstore::MeterStore, sql: &str) -> String {
    let result = store.query(sql).await.expect("query");
    let batch = result
        .batches()
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a row");
    meterstore::arrow::util::display::array_value_to_string(batch.column(0), 0).expect("render")
}

#[tokio::test]
async fn water_keeps_its_cubic_metres_across_both_tiers() {
    // The reading that a `value_kwh` column would have mislabelled on every row.
    let workload = MeteringWorkload::new(START)
        .seed(0xA7E_0000)
        .sparte(Sparte::Wasser)
        .malo_ids(3)
        .days(3);
    let (_h, store, oracle) = split_store(&workload).await;
    let (from, to) = workload.range();

    assert_eq!(
        scalar(&store, "SELECT COUNT(*) FROM readings").await,
        oracle.row_count(from, to).to_string(),
        "every water reading must survive both tiers"
    );

    let units = store
        .query("SELECT DISTINCT sparte, unit FROM readings")
        .await
        .expect("query");
    let rendered = meterstore::arrow::util::pretty::pretty_format_batches(units.batches())
        .expect("render")
        .to_string();
    assert!(rendered.contains("WASSER"), "{rendered}");
    assert!(rendered.contains("M3"), "{rendered}");
    assert!(
        !rendered.contains("KWH"),
        "water has no calorific value, so no row may claim kWh: {rendered}"
    );
}

#[tokio::test]
async fn gas_may_be_stored_on_either_side_of_the_brennwert_conversion() {
    // `Sparte::requires_conversion` is true for gas alone: the register advances
    // in m³ and settlement is in kWh. Both are legitimate contents for a stored
    // row, and which one it is has to be recorded rather than assumed.
    let converted = MeteringWorkload::new(START)
        .seed(0x6A5_0001)
        .sparte(Sparte::Gas)
        .malo_ids(2)
        .days(2);
    assert_eq!(Sparte::Gas.billing_unit(), MeasurementUnit::KiloWattHour);

    let (_h, store, _oracle) = split_store(&converted).await;
    assert_eq!(
        scalar(&store, "SELECT DISTINCT unit FROM readings").await,
        "KWH",
        "the default for gas is the settled unit"
    );

    // And the unconverted form, which an operator archiving Betriebsvolumen holds.
    let raw = MeteringWorkload::new(START)
        .seed(0x6A5_0002)
        .sparte(Sparte::Gas)
        .in_unit(MeasurementUnit::CubicMetre)
        .malo_ids(2)
        .days(2);
    let (_h2, store2, _o2) = split_store(&raw).await;
    assert_eq!(
        scalar(&store2, "SELECT DISTINCT unit FROM readings").await,
        "M3",
        "unconverted gas is m³ and must say so"
    );
}

#[test]
fn a_unit_the_commodity_cannot_have_is_refused_at_the_write() {
    // The check that makes the column worth reading. Without it a water series
    // labelled KWH would sum straight into an electricity total.
    let workload = MeteringWorkload::new(START)
        .sparte(Sparte::Wasser)
        .in_unit(MeasurementUnit::KiloWattHour)
        .malo_ids(1)
        .days(1);
    let series = workload.generate().expect("generating is not what fails");

    let err =
        meterstore::encode::to_record_batch(&series).expect_err("water cannot be expressed in kWh");
    let msg = err.to_string();
    assert!(msg.contains("WASSER"), "{msg}");
    assert!(msg.contains("KWH"), "{msg}");
    assert!(
        msg.contains("M3"),
        "the message must name the unit that would have been right: {msg}"
    );
}

#[test]
fn every_sparte_admits_its_own_measured_and_billing_units() {
    // The rule is `metering`'s, not ours (P5). This pins that we enforce exactly
    // it — no narrower, which would reject legitimate gas m³, and no wider,
    // which would let anything through.
    for sparte in Sparte::ALL {
        for unit in MeasurementUnit::ALL {
            let admissible = unit == sparte.measured_unit() || unit == sparte.billing_unit();
            let series = MeteringWorkload::new(START)
                .sparte(sparte)
                .in_unit(unit)
                .malo_ids(1)
                .days(1)
                .generate()
                .expect("workload");
            assert_eq!(
                meterstore::encode::to_record_batch(&series).is_ok(),
                admissible,
                "{sparte} in {unit} should be {}",
                if admissible { "accepted" } else { "refused" }
            );
        }
    }
}

#[tokio::test]
async fn a_mixed_portfolio_sums_per_unit_and_never_across_them() {
    // The query a multi-Sparte operator runs, and the one that silently produced
    // a dimensionless number before `unit` existed. Grouping by it is what makes
    // the answer mean anything.
    let harness = TestHarness::start().await.expect("harness");
    let to = START + Duration::days(2);
    harness
        .ensure_partitions(START, to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(START).await.expect("watermark");
    let store = harness.store().await.expect("store");

    let mut all: Vec<StoredSeries> = Vec::new();
    for (i, sparte) in [Sparte::Strom, Sparte::Gas, Sparte::Wasser]
        .into_iter()
        .enumerate()
    {
        all.extend(
            MeteringWorkload::new(START)
                .seed(0x1_0000 + i as u64)
                // A Marktlokation belongs to one commodity, and `sparte` is not
                // in the merge key — so the three populations must be disjoint
                // or they would collide on `(malo_id, obis_code, from)`.
                .malo_offset(i * 100)
                .sparte(sparte)
                .malo_ids(2)
                .days(2)
                .generate()
                .expect("workload"),
        );
    }
    harness.ingest(&store, &all).await.expect("ingest");

    let batches = store
        .query("SELECT unit, COUNT(*) FROM readings GROUP BY unit ORDER BY unit")
        .await
        .expect("query");
    let rendered = meterstore::arrow::util::pretty::pretty_format_batches(batches.batches())
        .expect("render")
        .to_string();

    // Two dimensions, both present: gas and water are m³ and kWh respectively,
    // electricity is kWh. A single row would mean the grouping did nothing.
    assert!(rendered.contains("KWH"), "{rendered}");
    assert!(rendered.contains("M3"), "{rendered}");
}

// ── the Gastag ───────────────────────────────────────────────────────────────
//
// Gas is the one commodity here that is not balanced on the calendar day. The
// German gas market runs 06:00–06:00 local (GaBi Gas), so a gas Lastgang
// grouped by `meter_local_day` books its 00:00–06:00 draw into the neighbouring
// Bilanzierungstag — six hours a day, with totals that still look plausible.
// The tests below are the end-to-end counterpart of the unit tests in
// `planner::calendar`: they check that the day survives storage, the resolved
// view and the completeness report, not merely the arithmetic.

/// One gas quarter-hour, valued so a mis-bucketed interval is arithmetically
/// visible rather than merely miscounted.
fn gas_quarter(from: OffsetDateTime, value: i64) -> metering::interval::MeterInterval {
    metering::interval::MeterInterval {
        from,
        to: from + Duration::minutes(15),
        value: rust_decimal::Decimal::new(value, 0),
        quality: metering::QualityFlag::Measured,
        obis_code: "7-1:99.33.0".parse().ok(),
    }
}

/// A gas delivery covering `[from, to)` at the quarter-hour.
fn gas_series(malo: &str, from: OffsetDateTime, to: OffsetDateTime, value: i64) -> StoredSeries {
    let intervals: Vec<_> = std::iter::successors(Some(from), |t| Some(*t + Duration::minutes(15)))
        .take_while(|t| *t < to)
        .map(|t| gas_quarter(t, value))
        .collect();

    let mut series = metering::measurement_series::MeasurementSeries::new(
        malo.parse().expect("a valid MaLo-ID"),
        "7-1:99.33.0".parse().ok(),
        intervals,
        metering::measurement_series::MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
        },
        to,
    );
    series.resolution = Some(metering::IntervalResolution::QuarterHour);

    StoredSeries::of(
        Sparte::Gas,
        series,
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("9900000000001", from, Sparte::Gas)
                .expect("scope"),
            meterstore::Version::new(20_261_001_000_001).expect("version"),
        ),
        to,
    )
}

#[tokio::test]
async fn a_gas_delivery_scoped_to_its_real_bilanzierungsmonat_is_accepted() {
    // EDI@Energy Allgemeine Festlegungen v6.1c, Kap. 3.1: the gas
    // Bilanzierungsmonat Juni 2021 covers 01.06 06:00 to 01.07 06:00 — the
    // Gastag boundary carries all the way up, so a gas interval in the first six
    // hours of a calendar month belongs to the *previous* month's scope.
    //
    // 01:00 UTC on 1 March is 02:00 local: March by the calendar, still the
    // Gastag of February.
    let straddling = datetime!(2026-03-01 1:00 UTC);
    let series = gas_series(
        "10000000009",
        straddling,
        straddling + Duration::hours(1),
        7,
    );

    assert_eq!(
        series.version.scope().period(),
        "2026-02",
        "the derived scope must be February's, not March's"
    );

    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(
            straddling - Duration::days(1),
            straddling + Duration::days(1),
        )
        .await
        .expect("partitions");
    let store = harness.store().await.expect("store");

    // The whole point: this is the correctly-scoped delivery, and it must land.
    let outcome = store
        .append(&[series])
        .await
        .expect("a correctly scoped gas delivery");
    assert_eq!(outcome.total(), 4, "four quarter-hours");

    // And the calendar month is refused for gas, naming the commodity.
    let mut wrong = gas_series(
        "10000000009",
        straddling,
        straddling + Duration::hours(1),
        7,
    );
    wrong.version = meterstore::ScopedVersion::new(
        meterstore::VersionScope::new("9900000000001", 2026, 3).expect("scope"),
        meterstore::Version::new(20_261_001_000_002).expect("version"),
    );
    let err = store
        .append(&[wrong])
        .await
        .expect_err("the calendar month is not this interval's gas Bilanzierungsmonat")
        .to_string();
    assert!(err.contains("Bilanzierungsmonat"), "{err}");
    assert!(err.contains("GAS"), "{err}");
}

#[tokio::test]
async fn a_gas_lastgang_groups_onto_the_gastag_not_the_calendar_day() {
    // Two full Gastage in July, each 96 quarter-hours from 06:00 local. Grouped
    // by the gas day they are 96 and 96; grouped by the calendar day they are
    // 72, 96 and 24 — three buckets, two of them fractions of a day, and every
    // one of them a number a Bilanzkreis would be settled on.
    let first = datetime!(2026-07-15 4:00 UTC); // 06:00 CEST
    let last = first + Duration::days(2);

    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(first, last + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(first).await.expect("watermark");
    let store = harness.store().await.expect("store");

    harness
        .ingest(&store, &[gas_series("10000000009", first, last, 4)])
        .await
        .expect("ingest");

    let rendered = |batches: &[meterstore::arrow::array::RecordBatch]| {
        meterstore::arrow::util::pretty::pretty_format_batches(batches)
            .expect("render")
            .to_string()
    };

    let gas = store
        .query(
            r#"SELECT meter_gas_day("from") AS d, COUNT(*) AS n
               FROM readings GROUP BY 1 ORDER BY 1"#,
        )
        .await
        .expect("query");
    let gas = rendered(gas.batches());
    assert!(gas.contains("2026-07-15"), "{gas}");
    assert!(gas.contains("2026-07-16"), "{gas}");
    assert!(
        !gas.contains("2026-07-17"),
        "two whole Gastage must be two buckets: {gas}"
    );

    // `meter_balancing_day` must reach the same answer from the stored Sparte,
    // without the statement having to know which commodity it is reading.
    let balanced = store
        .query(
            r#"SELECT meter_balancing_day("from", sparte) AS d, COUNT(*) AS n
               FROM readings GROUP BY 1 ORDER BY 1"#,
        )
        .await
        .expect("query");
    assert_eq!(
        rendered(balanced.batches()),
        gas,
        "dispatch must not differ"
    );

    // And the calendar day is the wrong answer, visibly: three buckets.
    let calendar = store
        .query(
            r#"SELECT meter_local_day("from") AS d, COUNT(*) AS n
               FROM readings GROUP BY 1 ORDER BY 1"#,
        )
        .await
        .expect("query");
    let calendar = rendered(calendar.batches());
    assert!(
        calendar.contains("2026-07-17"),
        "the calendar day spills into a third bucket, which is the bug: {calendar}"
    );
}

#[tokio::test]
async fn gas_completeness_puts_the_long_day_on_the_saturday() {
    // The autumn transition, which is where the two calendars disagree about
    // *which* day is long. The clocks go back at 03:00 local on Sunday 25
    // October — inside the Gastag that began Saturday at 06:00 — so the
    // 100-interval gas day is the 24th while the 100-interval calendar day is
    // the 25th. A report on calendar days would call the Saturday four in
    // surplus and the Sunday four short: two findings, on a channel with
    // nothing wrong with it.
    let first = datetime!(2026-10-24 4:00 UTC); // 06:00 CEST, Saturday
    let last = datetime!(2026-10-26 5:00 UTC); // 06:00 CET, Monday

    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(first, last + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(first).await.expect("watermark");
    let store = harness.store().await.expect("store");

    harness
        .ingest(&store, &[gas_series("10000000009", first, last, 4)])
        .await
        .expect("ingest");

    let report = store.completeness(first, last).await.expect("completeness");
    assert_eq!(report.len(), 1, "one channel: {report:?}");
    let row = &report[0];

    assert_eq!(row.sparte, Sparte::Gas);
    // 100 + 96 = 196 quarter-hours across the two Gastage.
    assert_eq!(row.actual, 196);
    assert_eq!(row.expected, 196);
    assert!(
        row.is_complete(),
        "a complete pair of Gastage must report complete: {row:?}"
    );
    assert_eq!(row.missing, 0);
    assert_eq!(row.surplus, 0, "the Saturday is 25 hours long, not 24");
    assert_eq!(row.first_gap, None);
}

#[tokio::test]
async fn a_gap_in_a_gas_day_is_still_found() {
    // The other direction: the Gastag boundary must not become a place gaps hide.
    let first = datetime!(2026-10-24 4:00 UTC);
    let last = datetime!(2026-10-26 5:00 UTC);

    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(first, last + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(first).await.expect("watermark");
    let store = harness.store().await.expect("store");

    // Everything except the last hour of the long Saturday Gastag — four
    // intervals that end at 06:00 local on the Sunday.
    let cut = datetime!(2026-10-25 4:00 UTC);
    harness
        .ingest(
            &store,
            &[
                gas_series("10000000009", first, cut, 4),
                gas_series("10000000009", datetime!(2026-10-25 5:00 UTC), last, 4),
            ],
        )
        .await
        .expect("ingest");

    let report = store.completeness(first, last).await.expect("completeness");
    let row = &report[0];
    assert_eq!(row.missing, 4, "the missing hour must be reported: {row:?}");
    assert_eq!(row.surplus, 0);
    assert_eq!(
        row.first_gap,
        Some(time::macros::date!(2026 - 10 - 24)),
        "and attributed to the Gastag that began on the Saturday"
    );
}
