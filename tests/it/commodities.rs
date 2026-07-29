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
