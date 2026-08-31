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

    // The tiers the file described, assembled into a store — including the one
    // step that is not field access: the cold table has to exist before a table
    // provider can be opened over it.
    let store = deployment.store().await.expect("store");

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
                sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
            },
            START,
        ),
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("9900000000001", START, Sparte::Strom)
                .expect("scope"),
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

#[tokio::test]
async fn a_multi_table_file_builds_a_catalog_and_refuses_a_single_store() {
    // A file declaring two streams has no single store to build, and picking
    // one silently is the failure mode: a billing query would read whichever
    // table happened to win, and an ESA Typ-2 stream must never reach one.
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");

    let text = format!(
        "{}\n[[tables]]\nname = \"esa_typ2_versions\"\n\n[tables.archival]\nsettlement_lag = \"1d\"\narchival_step = \"1d\"\n",
        file(&url, warehouse.path()),
    );
    let deployment = Settings::from_toml(&text)
        .expect("parse")
        .connect()
        .await
        .expect("connect");

    let err = deployment
        .store()
        .await
        .expect_err("two tables, so there is no single store")
        .to_string();
    assert!(err.contains("readings_versions"), "{err}");
    assert!(err.contains("esa_typ2_versions"), "{err}");
    assert!(err.contains("catalog"), "{err}");

    // The catalog builds both, and a statement can name either.
    let catalog = deployment.catalog().await.expect("catalog");
    let mut names: Vec<&str> = catalog.tables().map(|t| t.table()).collect();
    names.sort_unstable();
    assert_eq!(names, ["esa_typ2_versions", "readings_versions"]);

    catalog
        .query("SELECT count(*) FROM readings")
        .await
        .expect("the resolved relation is registered");
}

#[tokio::test]
async fn a_declared_identity_column_reaches_both_tiers() {
    // The trap this closes: the cold table has to exist before a table provider
    // can be opened over it, so building a store from a file creates it — and an
    // identity column is a *leading partition field*, not just a column. Created
    // bare, the table would carry the default spec, and the deployment's own
    // `create_tables` would then refuse the table that was just made for it.
    //
    // There is no reconciling that afterwards: a partition spec is fixed at
    // creation.
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");

    let text = format!(
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
extra_columns = [
  {{ name = "tenant", identity = true }},
  {{ name = "bilanzkreis" }},
]

[tables.archival]
settlement_lag = "1d"
archival_step = "1d"
"#,
        warehouse.path().display(),
    );

    let deployment = Settings::from_toml(&text)
        .expect("parse")
        .connect()
        .await
        .expect("connect");

    let store = deployment
        .store()
        .await
        .expect("the cold table must be created with the declared identity columns");

    // The identity column is in the merge key, which is what makes two tenants'
    // readings two readings rather than one superseding the other.
    assert!(
        store.config().merge_key().iter().any(|c| c == "tenant"),
        "{:?}",
        store.config().merge_key()
    );

    // And it survives a second build over the same warehouse, which is the call
    // that would have failed on a bare creation.
    deployment.store().await.expect("idempotent");
}

#[tokio::test]
async fn a_subject_column_in_a_file_reaches_a_registry_that_can_erase() {
    // `subject_column` was parseable, validated and *unreachable*: nothing on the
    // deployment path built a `SubjectRegistry`, and `MeterStoreBuilder::build`
    // refuses a subject column without one. So every subcommand of a deployment
    // declaring one — `create`, `status`, and the § 60 Abs. 6 sweep the CLI
    // documents as needing exactly this — failed at startup with a message about
    // a registry no configuration file could supply.
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");
    let text = format!(
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

[privacy]
erasure_secret = "0123456789abcdef0123456789abcdef"

[[tables]]
name = "readings_versions"
subject_column = "subject_ref"

[tables.archival]
settlement_lag = "1d"
archival_step = "1d"
"#,
        warehouse.path().display(),
    );

    let deployment = Settings::from_toml(&text)
        .expect("parse")
        .connect()
        .await
        .expect("connect");
    assert!(
        deployment.registry.is_some(),
        "a declared subject column builds the registry it resolves against"
    );

    let store = deployment.store().await.expect("store");
    let registry = store
        .subject_registry()
        .expect("the store carries the deployment's registry");
    assert!(
        registry.suppresses_reregistration(),
        "the file supplied a key, so the suppression list is on"
    );

    // End to end: register, erase, and the trail is what `meterstore erasures`
    // reads. `create_tables` had to have created the registry's own two tables,
    // or none of this reaches a relation.
    let subject = store
        .register_subject("tenant-a:12345678905")
        .await
        .expect("register");
    store
        .erase_subject(&subject, "DSAR-2026-0042", "dpo", START)
        .await
        .expect("erase");
    let trail = registry.erasures(10).await.expect("trail");
    assert_eq!(trail.len(), 1);
    assert_eq!(trail[0].reason, "DSAR-2026-0042");
    assert_eq!(trail[0].actor, "dpo");
    // The trail proves an erasure happened without recording whom it concerned.
    assert_eq!(trail[0].subject, subject);

    // And the suppression list holds: a replaying pipeline does not re-link the
    // subject it just erased.
    let err = store
        .register_subject("tenant-a:12345678905")
        .await
        .expect_err("suppressed");
    assert!(err.to_string().contains("erased"), "{err}");
}

#[tokio::test]
async fn a_deployment_with_no_subject_column_builds_no_registry() {
    // The commonest configuration. A registry built anyway would create two
    // tables in a database that has no use for them.
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");

    let deployment = Settings::from_toml(&file(&url, warehouse.path()))
        .expect("parse")
        .connect()
        .await
        .expect("connect");
    assert!(deployment.registry.is_none());
    assert!(
        deployment
            .store()
            .await
            .expect("store")
            .subject_registry()
            .is_none()
    );
}
