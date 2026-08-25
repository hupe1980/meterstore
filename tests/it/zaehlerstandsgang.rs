//! A **Zählerstandsgang** through both tiers.
//!
//! BK6-24-174 (in force 06.06.2025) means a German MSB holds one register-value
//! series per measuring point at the same cadence as the Lastgang it is
//! differenced into — 96 values a day for electricity. So the *primary* record
//! is exactly as voluminous as the derived one, and § 146 Abs. 4 AO means it
//! cannot be discarded after differencing: a stored difference cannot reproduce
//! the register values it came from.
//!
//! Left in plain PostgreSQL that is the one table nobody may delete from and
//! nothing tiers out of. These pin that it tiers like everything else, and that
//! the two shapes can never be confused for one another.

#![cfg(feature = "testkit")]

use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use metering::QualityFlag;
use metering::interval::{MeasurementUnit, Sparte};
use metering::measurement_series::MeasurementSource;
use metering::reading::MeterReading;
use meterstore::cold::IcebergCold;
use meterstore::config::{TableConfig, TimeModel};
use meterstore::encode::StoredReadings;
use meterstore::hot::PostgresHot;
use meterstore::tiering::store::{ColdStore, HotStore};
use rust_decimal::Decimal;
use sqlx::PgPool;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const TABLE: &str = "meter_reads_versions";
const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const MALO: &str = "12345678905";
/// The Messlokation the register belongs to.
///
/// A Zählerstandsgang is a *meter's* history, and a Marktlokation may be
/// measured by more than one — so a point table identifies a reading by this
/// too, and a delivery that names none is refused.
const MELO: &str = "DE0001234567890123456789012345678";
/// A second meter under the same Marktlokation: the Einliegerwohnung.
const MELO_2: &str = "DE0009876543210987654321098765432";

/// A point store, plus the warehouse it must outlive.
async fn point_store() -> (meterstore::MeterStore, tempfile::TempDir) {
    point_store_keyed(true).await
}

/// [`point_store`] with the Messlokation in or out of the merge key.
async fn point_store_keyed(by_melo: bool) -> (meterstore::MeterStore, tempfile::TempDir) {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let pool = PgPool::connect(&url).await.expect("connect");
    let hot = Arc::new(PostgresHot::new(pool));

    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::Fs))
        .load(
            "meterstore",
            std::collections::HashMap::from([
                (SQL_CATALOG_PROP_URI.to_string(), url.clone()),
                (
                    SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                    format!("file://{}", warehouse.path().display()),
                ),
                (
                    SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                    SqlBindStyle::DollarNumeric.to_string(),
                ),
            ]),
        )
        .await
        .expect("catalog");
    let cold = Arc::new(IcebergCold::new(
        Arc::new(catalog) as Arc<dyn Catalog>,
        NamespaceIdent::new("metering".to_string()),
        8 * 1024 * 1024,
    ));

    let config = TableConfig::new(TABLE)
        .time_model(TimeModel::Point)
        .identify_by_melo(by_melo)
        .settlement_lag(Duration::days(1))
        .build()
        .expect("config");

    // The provider is built from the cold table, so it has to exist first — the
    // same order every other suite's harness uses.
    cold.create_tables(TABLE, &[], &config.extra_columns())
        .await
        .expect("cold table");

    let store = meterstore::MeterStore::builder()
        .hot(Arc::clone(&hot) as Arc<dyn HotStore>)
        .cold(
            Arc::clone(&cold) as Arc<dyn ColdStore>,
            cold.table_provider(TABLE).await.expect("provider"),
        )
        .table(config)
        .build()
        .await
        .expect("store");

    store.create_tables().await.expect("tables");
    hot.ensure_partitions(TABLE, START, START + Duration::days(3), Duration::DAY)
        .await
        .expect("partitions");
    (store, warehouse)
}

/// `count` quarter-hourly register readings, climbing by `step` each time.
fn zsg(first: OffsetDateTime, count: i64, start_value: i64, step: i64) -> StoredReadings {
    zsg_at(MELO, first, count, start_value, step)
}

/// [`zsg`] for a named Messlokation.
fn zsg_at(
    melo: &str,
    first: OffsetDateTime,
    count: i64,
    start_value: i64,
    step: i64,
) -> StoredReadings {
    zsg_register(melo, "1-0:1.8.0", first, count, start_value, step)
}

/// [`zsg_at`] on a named register — a meter has several.
fn zsg_register(
    melo: &str,
    obis: &str,
    first: OffsetDateTime,
    count: i64,
    start_value: i64,
    step: i64,
) -> StoredReadings {
    let readings: Vec<MeterReading> = (0..count)
        .map(|i| MeterReading {
            at: first + Duration::minutes(15 * i),
            value: Decimal::new(start_value + step * i, 0),
            quality: QualityFlag::Measured,
            obis_code: None,
        })
        .collect();

    StoredReadings::new(
        MALO.parse().expect("a valid MaLo-ID"),
        // A Zählerstand register, not the Lastgang.
        obis.parse().expect("obis"),
        Sparte::Strom,
        readings,
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        },
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("99", first, Sparte::Strom).expect("scope"),
            meterstore::Version::new(20_260_720_000_001).expect("version"),
        ),
        datetime!(2026-07-27 06:00 UTC),
    )
    .with_melo_id(melo.parse().expect("a valid MeLo-ID"))
    .at_cadence(metering::IntervalResolution::QuarterHour)
    .in_unit(MeasurementUnit::KiloWattHour)
}

#[tokio::test]
async fn a_zaehlerstandsgang_round_trips_as_the_domain_type() {
    let (store, _w) = point_store().await;

    let outcome = store
        .append_readings(&[zsg(START, 96, 100_000, 3)])
        .await
        .expect("a Zählerstandsgang must store");
    assert_eq!(outcome.total(), 96);

    let back = store
        .readings(MALO)
        .expect("a point table")
        .range(START, START + Duration::days(1))
        .deliveries()
        .await
        .expect("typed read");

    assert_eq!(back.len(), 1, "one delivery");
    let d = &back[0];
    assert_eq!(d.readings.len(), 96);
    assert_eq!(d.sparte, Sparte::Strom);
    assert_eq!(d.unit, MeasurementUnit::KiloWattHour);
    assert_eq!(d.cadence, Some(metering::IntervalResolution::QuarterHour));
    assert_eq!(d.obis_code.to_string(), "1-0:1.8.0");

    // Cumulative and ascending — the register, not a difference of it.
    assert_eq!(d.readings[0].value, Decimal::new(100_000, 0));
    assert_eq!(d.readings[95].value, Decimal::new(100_000 + 3 * 95, 0));
    assert!(
        d.readings.windows(2).all(|w| w[1].value > w[0].value),
        "a register climbs"
    );

    // And it is what `metering` computes with directly.
    let consumed = d.readings[95].value - d.readings[0].value;
    assert_eq!(consumed, Decimal::new(285, 0));
}

#[tokio::test]
async fn a_reading_has_no_span_end() {
    // `to IS NULL` is the row-level signal that `value` is a register reading
    // rather than energy over a span — the one an external engine holding only
    // the Parquet can see.
    let (store, _w) = point_store().await;
    store
        .append_readings(&[zsg(START, 4, 100_000, 3)])
        .await
        .expect("append");

    let rows = store
        .query(r#"SELECT COUNT(*) AS n FROM meter_reads_versions WHERE "to" IS NULL"#)
        .await
        .expect("query")
        .to_json()
        .expect("json");
    assert_eq!(rows[0]["n"].as_i64(), Some(4));
}

#[tokio::test]
async fn a_zaehlerstandsgang_tiers_like_everything_else() {
    // The primary record has to tier too: it is as voluminous as the Lastgang
    // derived from it, and § 146 Abs. 4 AO forbids deleting it.
    let (store, _w) = point_store().await;

    store
        .append_readings(&[zsg(START, 96, 100_000, 3)])
        .await
        .expect("day one");
    store
        .append_readings(&[zsg(START + Duration::days(1), 96, 100_285, 3)])
        .await
        .expect("day two");

    // Archive the first day. `settlement_lag` is one day, so a clock two days on
    // closes exactly the first window.
    store
        .archive(START + Duration::days(2), 4)
        .await
        .expect("archive");
    assert_eq!(
        store.watermark().await.expect("watermark").get(),
        START + Duration::days(1),
        "the first day moved to Iceberg"
    );

    // Nothing is stranded below the boundary in PostgreSQL — the partition was
    // dropped once its rows were durable in Iceberg.
    let operational = store
        .query("SELECT COUNT(*) AS n FROM meter_reads_versions")
        .await
        .expect("query");
    assert_eq!(
        operational.to_json().expect("json")[0]["n"].as_i64(),
        Some(192),
        "both days are readable, from whichever tier owns them"
    );

    // ...and the query still returns every reading, across the boundary.
    let all = store
        .readings(MALO)
        .expect("a point table")
        .range(START, START + Duration::days(2))
        .deliveries()
        .await
        .expect("typed read across the boundary");
    let total: usize = all.iter().map(|d| d.readings.len()).sum();
    assert_eq!(total, 192, "both days, one from each tier");

    store.verify_invariant().await.expect("invariant");
}

#[tokio::test]
async fn the_two_shapes_cannot_be_confused() {
    // `value` is interval energy on one table and a cumulative register reading
    // on the other, so a table holding both would carry a column no aggregate
    // could interpret — and summing Zählerstände produces a number with no
    // meaning that looks exactly like a consumption total.
    let (store, _w) = point_store().await;

    // The interval write path is refused on a point table, and says why.
    let err = store
        .append(&[interval_series()])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("POINT"), "{err}");
    assert!(err.contains("INTERVAL"), "{err}");
    assert!(
        err.contains("time_model"),
        "the message names the fix: {err}"
    );

    assert!(store.hot_writer().await.is_err());

    // And the typed interval read is refused too, rather than decoding a
    // register reading as an interval ending at the Unix epoch.
    store
        .append_readings(&[zsg(START, 4, 100_000, 3)])
        .await
        .expect("readings");
    assert!(
        store
            .series(MALO)
            .expect("parses")
            .range(START, START + Duration::days(1))
            .collect()
            .await
            .is_err(),
        "a point row is not an interval"
    );
}

#[tokio::test]
async fn an_interval_table_still_refuses_readings() {
    // The mirror. An INTERVAL table is the default, and it must not silently
    // accept a Zählerstandsgang with a null end.
    let harness = meterstore::testkit::TestHarness::start()
        .await
        .expect("harness");
    let store = harness.store().await.expect("store");

    let err = store
        .append_readings(&[zsg(START, 4, 100_000, 3)])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("INTERVAL"), "{err}");
    assert!(err.contains("append_readings"), "{err}");

    // And the read path refuses before it builds a query at all: `readings`
    // returns the builder only for a table that has registers.
    assert!(store.readings(MALO).is_err());
}

/// A Lastgang delivery, for the refusal tests.
fn interval_series() -> meterstore::encode::StoredSeries {
    let mut series = metering::measurement_series::MeasurementSeries::new(
        MALO.parse().expect("a valid MaLo-ID"),
        "1-0:1.29.0".parse().ok(),
        vec![metering::interval::MeterInterval {
            from: START,
            to: START + Duration::minutes(15),
            value: Decimal::new(3, 0),
            quality: QualityFlag::Measured,
            obis_code: "1-0:1.29.0".parse().ok(),
        }],
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        },
        datetime!(2026-07-27 06:00 UTC),
    );
    series.resolution = Some(metering::IntervalResolution::QuarterHour);

    meterstore::encode::StoredSeries::new(
        series,
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("99", START, Sparte::Strom).expect("scope"),
            meterstore::Version::new(20_260_720_000_001).expect("version"),
        ),
        datetime!(2026-07-27 06:00 UTC),
    )
}

#[tokio::test]
async fn two_messlokationen_under_one_marktlokation_are_two_registers() {
    // The case a Zählerstandsgang keyed on the Marktlokation alone cannot hold.
    //
    // A Marktlokation may be measured by several Messlokationen — a
    // Mehrfamilienhaus split into sub-measurements, a house whose
    // Einliegerwohnung has its own meter — and the network operator decides the
    // assignment. Each meter carries the *same* OBIS register, `1-0:1.8.0`, at
    // the same instants, so on a key of `(malo_id, obis_code, from)` the second
    // meter's readings are a restatement of the first's. Where the two agree on
    // a number — which two freshly installed meters do — one of them is dropped
    // with nothing to notice it.
    let (store, _w) = point_store().await;

    let house = store
        .append_readings(&[zsg_at(MELO, START, 96, 100_000, 3)])
        .await
        .expect("the first meter");
    let annex = store
        .append_readings(&[zsg_at(MELO_2, START, 96, 100_000, 3)])
        .await
        .expect("the second meter must be stored, not read as a replay");

    assert_eq!(house.total(), 96);
    assert_eq!(annex.total(), 96, "identical values, different meters");

    let back = store
        .readings(MALO)
        .expect("a point table")
        .range(START, START + Duration::days(1))
        .deliveries()
        .await
        .expect("typed read");

    let mut melos: Vec<String> = back
        .iter()
        .map(|d| {
            d.melo_id
                .as_ref()
                .expect("a keyed table stores it")
                .to_string()
        })
        .collect();
    melos.sort();
    assert_eq!(
        melos,
        vec![MELO.to_string(), MELO_2.to_string()],
        "two deliveries, one per meter — not one per row, which is what an \
         ordering that leaves the two interleaved produces"
    );
    assert!(
        back.iter().all(|d| d.readings.len() == 96),
        "each register comes back whole"
    );
}

#[tokio::test]
async fn a_point_delivery_naming_no_messlokation_is_refused() {
    // The column is `NOT NULL` in the hot table on such a table, so PostgreSQL
    // would catch it — with a message about a column constraint. The write path
    // says what it is actually about.
    let (store, _w) = point_store().await;

    let mut anonymous = zsg(START, 4, 100_000, 3);
    anonymous.melo_id = None;

    let err = store
        .append_readings(&[anonymous])
        .await
        .expect_err("a register with no meter names no reading")
        .to_string();
    assert!(err.contains("Messlokation"), "{err}");
    assert!(err.contains("identify_by_melo"), "{err}");
}

#[tokio::test]
async fn corrections_still_supersede_within_one_messlokation() {
    // The wider key must not break the narrower rule: two versions of *one*
    // meter's register still resolve to the higher, and the other meter is
    // untouched by it.
    let (store, _w) = point_store().await;

    store
        .append_readings(&[zsg_at(MELO, START, 4, 100_000, 3)])
        .await
        .expect("first version");
    store
        .append_readings(&[zsg_at(MELO_2, START, 4, 500_000, 3)])
        .await
        .expect("the other meter");

    let mut corrected = zsg_at(MELO, START, 4, 200_000, 3);
    corrected.version = meterstore::ScopedVersion::new(
        meterstore::VersionScope::for_interval("99", START, Sparte::Strom).expect("scope"),
        meterstore::Version::new(20_260_720_000_002).expect("version"),
    );
    store
        .append_readings(&[corrected])
        .await
        .expect("a correction");

    let back = store
        .readings(MALO)
        .expect("a point table")
        .range(START, START + Duration::hours(1))
        .deliveries()
        .await
        .expect("typed read");

    let value_of = |melo: &str| -> Decimal {
        back.iter()
            .find(|d| d.melo_id.as_ref().is_some_and(|m| m.to_string() == melo))
            .expect("both meters are present")
            .readings[0]
            .value
    };
    assert_eq!(value_of(MELO), Decimal::new(200_000, 0), "corrected");
    assert_eq!(value_of(MELO_2), Decimal::new(500_000, 0), "untouched");
}

#[tokio::test]
async fn a_late_correction_reconciles_per_messlokation() {
    // The cold tier's half of the same argument. Iceberg carries no constraint,
    // so a below-watermark write is reconciled against what is stored before it
    // lands — and that reconciliation has to key on the Messlokation too, or the
    // second meter's archived register is read as a replay of the first's and
    // dropped before it is ever written.
    let (store, _w) = point_store().await;

    store
        .append_readings(&[zsg_at(MELO, START, 4, 100_000, 3)])
        .await
        .expect("first meter");
    store
        .append_readings(&[zsg_at(MELO_2, START, 4, 100_000, 3)])
        .await
        .expect("second meter");

    store
        .archive(START + Duration::days(2), 4)
        .await
        .expect("archive");
    assert!(
        store.watermark().await.expect("watermark").get() > START,
        "the readings are below the boundary now"
    );

    // A correction for an archived instant, for one meter only.
    let mut corrected = zsg_at(MELO, START, 4, 200_000, 3);
    corrected.version = meterstore::ScopedVersion::new(
        meterstore::VersionScope::for_interval("99", START, Sparte::Strom).expect("scope"),
        meterstore::Version::new(20_260_720_000_002).expect("version"),
    );
    let outcome = store
        .append_readings(&[corrected])
        .await
        .expect("a late correction");
    assert_eq!(outcome.cold_rows, 4, "it went to Iceberg, not PostgreSQL");

    let back = store
        .readings(MALO)
        .expect("a point table")
        .range(START, START + Duration::hours(1))
        .deliveries()
        .await
        .expect("typed read");
    let value_of = |melo: &str| -> Decimal {
        back.iter()
            .find(|d| d.melo_id.as_ref().is_some_and(|m| m.to_string() == melo))
            .expect("both meters are present")
            .readings[0]
            .value
    };
    assert_eq!(value_of(MELO), Decimal::new(200_000, 0), "corrected");
    assert_eq!(
        value_of(MELO_2),
        Decimal::new(100_000, 0),
        "the meter next door is not a version of this one"
    );

    // And a replay of the correction is a duplicate, not a second row: two rows
    // at one version are two winners, and a historical scan elides resolution
    // precisely when the files hold one version.
    let mut replay = zsg_at(MELO, START, 4, 200_000, 3);
    replay.version = meterstore::ScopedVersion::new(
        meterstore::VersionScope::for_interval("99", START, Sparte::Strom).expect("scope"),
        meterstore::Version::new(20_260_720_000_002).expect("version"),
    );
    let again = store.append_readings(&[replay]).await.expect("replay");
    assert_eq!(again.total(), 0, "nothing written twice");
}

#[tokio::test]
async fn a_table_not_keyed_by_messlokation_refuses_the_collision_rather_than_dropping_it() {
    // `identify_by_melo(false)` is a legitimate declaration — a portfolio with
    // exactly one meter per market location has nothing to gain from the wider
    // key. It is legitimate right up to the day a second meter appears, and the
    // failure it would otherwise produce is the silent one: the two registers
    // agree at zero, `ON CONFLICT DO NOTHING` skips the second, and nothing
    // anywhere says a meter is missing.
    //
    // So the column is compared even where it is not part of the identity.
    let (store, _w) = point_store_keyed(false).await;

    store
        .append_readings(&[zsg_at(MELO, START, 4, 0, 0)])
        .await
        .expect("the first meter");

    let err = store
        .append_readings(&[zsg_at(MELO_2, START, 4, 0, 0)])
        .await
        .expect_err("the second meter must not be dropped in silence")
        .to_string();

    assert!(err.contains("Messlokation"), "{err}");
    assert!(
        err.contains("identify_by_melo"),
        "the message names the fix: {err}"
    );
}

#[tokio::test]
async fn a_redelivery_to_the_hot_tier_is_a_no_op_on_a_melo_keyed_table() {
    // The skipped-row check is a second query over the same join, and on a
    // melo-keyed table `melo_id` is both a core column and a merge-key column.
    // Named twice in one alias list it makes the join's reference to it
    // ambiguous — a SQL error, on the *ordinary* path, since every transport
    // worth deploying delivers at least once.
    let (store, _w) = point_store().await;

    let first = store
        .append_readings(&[zsg_at(MELO, START, 4, 100_000, 3)])
        .await
        .expect("first delivery");
    assert_eq!(first.total(), 4);

    let replay = store
        .append_readings(&[zsg_at(MELO, START, 4, 100_000, 3)])
        .await
        .expect("a replay is ordinary traffic");
    assert_eq!(replay.total(), 0, "nothing written twice");
    assert!(
        replay
            .displacements
            .iter()
            .all(|d| d.effect == meterstore::session::Effect::Duplicate),
        "and it is reported as the replay it is"
    );

    // A different value under the same version is still a producer error.
    let err = store
        .append_readings(&[zsg_at(MELO, START, 4, 999_000, 3)])
        .await
        .expect_err("a version identifies one assertion")
        .to_string();
    assert!(err.contains("version"), "{err}");
}

#[tokio::test]
async fn completeness_reports_a_row_per_meter() {
    // The report an operator runs *to find out what is wrong* must not be the
    // thing that is wrong. Two meters under one Marktlokation each deliver a
    // day of a shared channel; folded into one row that is 192 intervals against
    // an expectation of 96, and the report claims a surplus of 96 where nothing
    // is. The reverse hides a real gap: one meter short of four intervals nets
    // against the other's full day and the channel reads as complete.
    let (store, _w) = point_store().await;

    store
        .append_readings(&[zsg_at(MELO, START, 96, 100_000, 3)])
        .await
        .expect("the first meter, in full");
    store
        .append_readings(&[zsg_at(MELO_2, START, 92, 500_000, 3)])
        .await
        .expect("the second, four short");

    let report = store
        .completeness(START, START + Duration::days(1))
        .await
        .expect("completeness");

    assert_eq!(report.len(), 2, "a row per meter, not one folded row");
    let of = |melo: &str| {
        report
            .iter()
            .find(|r| r.identity.iter().any(|(_, v)| v == melo))
            .unwrap_or_else(|| panic!("{melo} is missing from the report"))
    };
    assert!(of(MELO).is_complete(), "the full meter reads complete");
    assert_eq!(of(MELO_2).missing, 4, "and the short one reads short");
    assert_eq!(of(MELO).surplus, 0, "neither is a duplicate of the other");

    // And the SQL surface carries the column, so an operator can see whose it is.
    let rows = store
        .query(&format!(
            "SELECT melo_id, missing FROM {}('{}', '{}') ORDER BY melo_id",
            meterstore::session::CompletenessFunction::NAME,
            START.date(),
            (START + Duration::days(1)).date(),
        ))
        .await
        .expect("query")
        .to_json()
        .expect("json");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["melo_id"].as_str(), Some(MELO));
    assert_eq!(rows[1]["missing"].as_i64(), Some(4));
}

#[tokio::test]
async fn the_current_meter_reading_is_one_row_not_a_history() {
    // What a register is asked for most often, and what a range read answers
    // expensively. `latest` resolves it with `ORDER BY … DESC LIMIT 1` at the
    // storage layer rather than by folding a decade of quarter-hours and taking
    // the maximum — on the one table §7.4 exists because it grows without bound.
    let (store, _warehouse) = point_store().await;
    store
        .append_readings(&[zsg(START, 96, 100_000, 3)])
        .await
        .expect("append");

    let latest = store
        .readings(MALO)
        .expect("a point table")
        .melo(MELO)
        .expect("a well-formed Zählpunktbezeichnung")
        .latest()
        .await
        .expect("latest")
        .expect("the meter has been read");

    assert_eq!(latest.at, START + Duration::minutes(15 * 95));
    assert_eq!(latest.value, Decimal::new(100_000 + 3 * 95, 0));
}

#[tokio::test]
async fn a_register_read_narrows_to_one_meter() {
    // Two meters under one Marktlokation carry the same register at the same
    // instants. Unnarrowed the read spans both — and folding them would
    // interleave two cumulative sequences, so differencing the result produces
    // advances belonging to neither meter.
    let (store, _warehouse) = point_store().await;
    store
        .append_readings(&[
            zsg_at(MELO, START, 4, 100_000, 3),
            zsg_at(MELO_2, START, 4, 500_000, 7),
        ])
        .await
        .expect("append");

    let query = || store.readings(MALO).expect("a point table");

    for (melo, first, step) in [(MELO, 100_000i64, 3i64), (MELO_2, 500_000, 7)] {
        let one = query()
            .melo(melo)
            .expect("melo")
            .range(START, START + Duration::hours(1))
            .collect()
            .await
            .expect("collect")
            .expect("readings");

        assert_eq!(
            one.melo_id.as_ref().map(ToString::to_string).as_deref(),
            Some(melo)
        );
        assert_eq!(one.readings.len(), 4);
        assert_eq!(one.readings[0].value, Decimal::new(first, 0));
        assert_eq!(one.readings[3].value, Decimal::new(first + step * 3, 0));
    }

    // And an unnarrowed fold is refused rather than silently interleaved.
    let err = query()
        .range(START, START + Duration::hours(1))
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("two meters"), "{err}");
    assert!(err.contains(".melo("), "the message names the fix: {err}");

    // The unfolded shape is still available, because an audit trail wants it.
    let both = query()
        .range(START, START + Duration::hours(1))
        .deliveries()
        .await
        .expect("deliveries");
    assert_eq!(both.len(), 2);
}

#[tokio::test]
async fn a_register_read_resolves_corrections_and_spans_the_boundary() {
    // The same guarantees the interval read gives: the value in force, from
    // whichever tier holds it.
    let (store, _warehouse) = point_store().await;
    store
        .append_readings(&[
            zsg(START, 96, 100_000, 3),
            zsg(START + Duration::DAY, 96, 100_288, 3),
        ])
        .await
        .expect("append");

    // A correction to the very first reading, at a higher version — and flagged
    // as one, which is what the quality filter below narrows on.
    let mut corrected = zsg(START, 1, 999_999, 0);
    corrected.readings[0].quality = QualityFlag::Corrected;
    corrected.version = meterstore::ScopedVersion::new(
        meterstore::VersionScope::for_interval("99", START, Sparte::Strom).expect("scope"),
        meterstore::Version::new(20_260_726_000_002).expect("version"),
    );
    store
        .append_readings(&[corrected])
        .await
        .expect("correction");

    let series = store
        .readings(MALO)
        .expect("a point table")
        .melo(MELO)
        .expect("melo")
        .range(START, START + Duration::days(2))
        .collect()
        .await
        .expect("collect")
        .expect("readings");

    assert_eq!(series.readings.len(), 192, "both days, each instant once");
    assert_eq!(
        series.readings[0].value,
        Decimal::new(999_999, 0),
        "the corrected value, not the superseded one"
    );
    assert!(
        series.readings.windows(2).all(|w| w[0].at < w[1].at),
        "ascending by instant"
    );

    // Quality narrows over the resolved view, so it sees the value in force.
    let measured = store
        .readings(MALO)
        .expect("a point table")
        .melo(MELO)
        .expect("melo")
        .range(START, START + Duration::days(2))
        .quality_in(&[QualityFlag::Measured])
        .values()
        .await
        .expect("values");
    assert_eq!(
        measured.len(),
        191,
        "the corrected reading carries a different flag"
    );
}

#[tokio::test]
async fn a_meter_reads_back_register_by_register() {
    // A meter *is* a set of registers — a total beside its HT and NT halves —
    // and `collect` cannot describe them together: `StoredReadings` holds one
    // register, and folding two interleaves two cumulative sequences so that
    // differencing the result gives advances belonging to neither.
    //
    // Asking for all of them used to mean a hand-written `SELECT DISTINCT
    // obis_code` and one typed read per register. This is that question, in one
    // scan, with resolution and the tier split still inside the store.
    let (store, _warehouse) = point_store().await;
    for obis in ["1-0:1.8.0", "1-0:1.8.1", "1-0:1.8.2"] {
        store
            .append_readings(&[zsg_register(MELO, obis, START, 4, 1_000, 5)])
            .await
            .expect("append");
    }

    let query = || store.readings(MALO).unwrap().melo(MELO).unwrap();

    let registers = query().channels().await.expect("channels");
    assert_eq!(
        registers
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["1-0:1.8.0", "1-0:1.8.1", "1-0:1.8.2"],
    );

    let by_register = query()
        .collect_by_channel()
        .await
        .expect("collect_by_channel");
    assert_eq!(by_register.len(), 3);
    for (register, folded) in &by_register {
        assert_eq!(&folded.obis_code, register);
        assert_eq!(folded.readings.len(), 4);
        // Each register's own sequence, unmixed: 1000, 1005, 1010, 1015 — which
        // is what makes differencing it mean something.
        assert_eq!(folded.readings[0].value, Decimal::new(1_000, 0));
        assert_eq!(folded.readings[3].value, Decimal::new(1_015, 0));
    }

    // The single-register read still refuses, so nothing was loosened.
    let err = query()
        .collect()
        .await
        .expect_err("three registers cannot be one Zählerstandsgang");
    assert!(err.to_string().contains("registers"), "{err}");
}

#[tokio::test]
async fn a_per_register_read_keeps_two_meters_apart() {
    // The refusal `collect_by_channel` keeps. A point table identifies a reading
    // by its Messlokation, so two meters carrying `1-0:1.8.0` under one
    // Marktlokation are two readings — splitting by register alone would fold
    // them, and the interleaved sequence differences into nonsense.
    let (store, _warehouse) = point_store().await;
    store
        .append_readings(&[
            zsg_at(MELO, START, 4, 1_000, 5),
            zsg_at(MELO_2, START, 4, 7_000, 9),
        ])
        .await
        .expect("append");

    let err = store
        .readings(MALO)
        .unwrap()
        .collect_by_channel()
        .await
        .expect_err("two meters on one register are two readings");
    let msg = err.to_string();
    assert!(msg.contains("two meters"), "{msg}");
    assert!(msg.contains(".melo(..)"), "the fix has to be named: {msg}");

    // Naming the meter makes it one reading again, and the map holds it.
    let mine = store
        .readings(MALO)
        .unwrap()
        .melo(MELO_2)
        .unwrap()
        .collect_by_channel()
        .await
        .expect("one meter");
    assert_eq!(mine.len(), 1);
    let register: metering::obis::ObisCode = "1-0:1.8.0".parse().unwrap();
    assert_eq!(mine[&register].readings[0].value, Decimal::new(7_000, 0));

    // And the register list spans both meters unless narrowed, which is a fact
    // about the list rather than a bug: it is a set of registers, not readings.
    assert_eq!(
        store
            .readings(MALO)
            .unwrap()
            .channels()
            .await
            .unwrap()
            .len(),
        1,
        "one register, reported by two meters"
    );
}
