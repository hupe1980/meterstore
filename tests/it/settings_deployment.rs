//! A configuration file that builds the deployment it describes.
//!
//! `[hot]` and `[cold]` were parsed, exposed as public fields and consumed by
//! nothing. The page describing this front end claims "no setting reachable from
//! one and not the other", and the two sections naming the *infrastructure* were
//! reachable from a file and from nowhere else — so a deployment configured from
//! TOML still hand-wired a pool and a catalogue, re-deriving settings the file
//! had already stated, and nothing checked that what it stated was buildable.
//!
//! This drives the whole path: parse, validate both sections, open the pool,
//! build the catalogue, create the tables and write and read a reading through
//! them.

#![cfg(feature = "testkit")]

use metering::interval::{MeterInterval, Sparte};
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use meterstore::Settings;
use meterstore::testkit::TestHarness;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const MALO: &str = "12345678905";

/// A file describing a SQL-catalogue deployment on a real database and a real
/// warehouse directory.
fn file(url: &str, warehouse: &std::path::Path) -> String {
    format!(
        r#"
[hot]
url = "{url}"
max_connections = 4

[cold]
catalog = "sql"
uri = "{url}"
warehouse = "file://{}"
namespace = "metering"
file_target_bytes = 8388608
metadata_pool_max_connections = 2

[[tables]]
name = "readings_versions"

[tables.archival]
settlement_lag = "1d"
archival_step = "1d"
"#,
        warehouse.display(),
    )
}

#[tokio::test]
async fn a_configuration_file_builds_a_working_store() {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");

    let settings = Settings::from_toml(&file(&url, warehouse.path())).expect("parse");
    let deployment = settings.connect().await.expect("connect");

    assert_eq!(deployment.tables.len(), 1);
    let config = deployment.tables[0].clone();
    assert_eq!(config.name(), TestHarness::TABLE);
    assert_eq!(config.settlement_lag(), Duration::DAY);

    // The tiers the file described, assembled into a store exactly as a
    // hand-wired deployment would.
    let cold = deployment.cold.cold();
    cold.create_table(config.name()).await.expect("cold table");

    let store = meterstore::MeterStore::builder()
        .hot(deployment.hot.clone() as std::sync::Arc<dyn meterstore::HotStore>)
        .cold(
            cold.clone() as std::sync::Arc<dyn meterstore::ColdStore>,
            cold.table_provider(config.name()).await.expect("provider"),
        )
        .table(config.clone())
        .build()
        .await
        .expect("store");

    store.create_tables().await.expect("create tables");

    // And it works: a reading in, the same reading out.
    let intervals = vec![MeterInterval {
        from: START,
        to: START + Duration::minutes(15),
        value: rust_decimal::Decimal::new(42, 0),
        quality: metering::QualityFlag::Measured,
        obis_code: "1-0:1.8.0".parse().ok(),
    }];
    let series = meterstore::encode::StoredSeries::new(
        MeasurementSeries::new(
            MALO.parse().expect("a valid MaLo-ID"),
            "1-0:1.8.0".parse().ok(),
            intervals,
            MeasurementSource::Mscons {
                pid: 13_005,
                message_ref: None,
                sender_mp_id: "99".to_string(),
            },
            START,
        ),
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("99", START, Sparte::Strom).expect("scope"),
            meterstore::Version::new(20_260_720_000_001).expect("version"),
        ),
        START,
    );

    store.append(&[series]).await.expect("append");

    let back = store
        .series(MALO)
        .expect("malo")
        .range(START, START + Duration::DAY)
        .intervals()
        .await
        .expect("read");
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].value, rust_decimal::Decimal::new(42, 0));

    // The pool comes back too, because the subject registry and the
    // application's own tables belong in the same database.
    assert!(!deployment.pool.is_closed());
}

#[tokio::test]
async fn a_file_naming_an_unopenable_warehouse_fails_at_validation() {
    // Not at the first commit, and not silently on local disk. The scheme
    // decides the object-store backend, and one whose feature was not compiled
    // in is a configuration error.
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");

    let mut settings = Settings::from_toml(&file(&url, warehouse.path())).expect("parse");
    "ftp://host/warehouse".clone_into(&mut settings.cold.warehouse);

    let err = settings
        .connect()
        .await
        .expect_err("an unsupported scheme must not build")
        .to_string();
    assert!(err.contains("ftp"), "{err}");

    // The table half is still fine, so a deployment wiring its own tiers is
    // unaffected — which is why the two checks are separate.
    settings.validate().expect("the tables are complete");
}
