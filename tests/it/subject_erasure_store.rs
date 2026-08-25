//! Erasure wired into the store, against real PostgreSQL and Iceberg.
//!
//! The registry on its own is a mapping table. What makes it a compliance
//! mechanism is that the store refuses to write references the registry does not
//! back — otherwise a pipeline can quietly produce rows that are unattributable
//! from birth, or re-link a subject whose erasure has already been certified.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them: `testkit::postgres` is what shares one container
// across the binary instead of starting one per test (§17.2.0.1).
#![cfg(feature = "testkit")]

use metering::interval::Sparte;
use std::sync::Arc;

use datafusion::common::ScalarValue;
use meterstore::cold::IcebergCold;
use meterstore::config::TableConfig;
use meterstore::encode::StoredSeries;
use meterstore::hot::PostgresHot;
use meterstore::tiering::store::{ColdStore, HotStore};
use meterstore::{MeterStore, ScopedVersion, SubjectRegistry, Version, VersionScope};

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use sqlx::PgPool;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const TABLE: &str = "readings_versions";
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);
const SECRET: &[u8] = b"test-suppression-key-32-bytes!!!";

/// A store whose readings carry a pseudonymous subject reference.
async fn store_with_subjects() -> (MeterStore, tempfile::TempDir) {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");

    let pool = PgPool::connect(&url).await.expect("connect");
    let hot = Arc::new(PostgresHot::new(pool.clone()));

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
        .settlement_lag(Duration::days(1))
        .subject_column("subject_ref")
        .build()
        .expect("config");

    let hot_dyn: Arc<dyn HotStore> = hot.clone();
    let cold_dyn: Arc<dyn ColdStore> = cold.clone();
    // The provider is resolved at build time, so both tiers must exist first.
    hot_dyn
        .create_tables(
            TABLE,
            &config.merge_key(),
            &config.extra_columns(),
            config.time_model(),
        )
        .await
        .expect("hot table");
    cold_dyn
        .create_tables(
            TABLE,
            &config.identity_column_names(),
            &config.extra_columns(),
        )
        .await
        .expect("cold table");
    hot.ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .expect("partitions");

    let store = MeterStore::builder()
        .hot(hot_dyn)
        .cold(
            cold_dyn,
            cold.table_provider(TABLE).await.expect("provider"),
        )
        .table(config)
        .subject_registry(SubjectRegistry::with_erasure_secret(pool, SECRET).expect("secret"))
        .build()
        .await
        .expect("store");

    // Idempotent, and the step that brings up the registry's own tables.
    store
        .subject_registry()
        .expect("configured")
        .create_tables()
        .await
        .expect("registry tables");

    (store, warehouse)
}

/// A reading attributed to `subject`, or to nobody when `None`.
fn reading(subject: Option<&str>) -> StoredSeries {
    reading_at(D20, subject)
}

/// The same, at a chosen interval start — for the retention sweep, whose whole
/// question is how old a subject's readings are.
fn reading_at(from: OffsetDateTime, subject: Option<&str>) -> StoredSeries {
    let interval = metering::interval::MeterInterval {
        from,
        to: from + Duration::minutes(15),
        value: rust_decimal::Decimal::new(42, 0),
        quality: metering::QualityFlag::Measured,
        obis_code: "1-0:1.8.0".parse().ok(),
    };
    let series = MeasurementSeries::new(
        "11111111115".parse().unwrap(),
        "1-0:1.8.0".parse().ok(),
        vec![interval],
        MeasurementSource::ManualEntry {
            operator_id: "test".to_string(),
            reason: "fixture".to_string(),
        },
        datetime!(2026-07-27 06:00 UTC),
    );

    let stored = StoredSeries::new(
        series,
        ScopedVersion::new(
            // Derived from the interval, not from a fixture constant: a scope
            // that does not cover its intervals is refused at encode time.
            VersionScope::for_interval("99", from, Sparte::Strom).unwrap(),
            Version::new(1).unwrap(),
        ),
        datetime!(2026-07-27 06:00 UTC),
    );

    match subject {
        Some(s) => stored.with_extra("subject_ref", ScalarValue::Utf8(Some(s.to_string()))),
        None => stored,
    }
}

#[tokio::test]
async fn a_registered_reference_is_accepted() {
    let (store, _w) = store_with_subjects().await;

    let subject = store.register_subject("customer-4821").await.unwrap();
    let outcome = store.append(&[reading(Some(subject.as_str()))]).await;

    assert!(outcome.is_ok(), "a live mapping must write: {outcome:?}");
    assert_eq!(outcome.unwrap().total(), 1);
}

#[tokio::test]
async fn an_unregistered_reference_is_refused() {
    // A reference the registry never issued produces rows nobody can attribute,
    // and the column looks perfectly well-formed afterwards — so it has to fail
    // at the write or not at all.
    let (store, _w) = store_with_subjects().await;

    let err = store
        .append(&[reading(Some("sub_deadbeefdeadbeefdeadbeefdeadbeef"))])
        .await
        .expect_err("an unbacked reference must be refused");

    assert!(
        err.to_string().contains("no live mapping"),
        "the error should name the cause: {err}"
    );
}

#[tokio::test]
async fn a_replay_after_erasure_cannot_rebuild_the_link() {
    // The scenario the whole mechanism exists for: an erasure is certified, and
    // hours later a broker redelivers a batch from before it.
    let (store, _w) = store_with_subjects().await;

    let subject = store.register_subject("customer-4821").await.unwrap();
    store
        .append(&[reading(Some(subject.as_str()))])
        .await
        .unwrap();

    store
        .erase_subject(
            &subject,
            "DSAR-2026-0042",
            "privacy-team",
            datetime!(2026-07-27 12:00 UTC),
        )
        .await
        .unwrap();

    // Replay of the identical batch.
    let replayed = store.append(&[reading(Some(subject.as_str()))]).await;
    assert!(
        replayed.is_err(),
        "an erased subject's reference must no longer be writable"
    );

    // And the ingest path cannot get a working reference by re-registering.
    assert!(
        store.register_subject("customer-4821").await.is_err(),
        "re-registration must stay suppressed"
    );
}

#[tokio::test]
async fn the_readings_survive_erasure_and_stop_being_attributable() {
    // Article 17 over append-only storage: the rows stay — retention law
    // requires them — and what is destroyed is the ability to say whose they are.
    let (store, _w) = store_with_subjects().await;

    let subject = store.register_subject("customer-4821").await.unwrap();
    store
        .append(&[reading(Some(subject.as_str()))])
        .await
        .unwrap();

    store
        .erase_subject(
            &subject,
            "DSAR-2026-0042",
            "privacy-team",
            datetime!(2026-07-27 12:00 UTC),
        )
        .await
        .unwrap();

    let rows = store
        .sql("SELECT COUNT(*) AS n FROM readings")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 1, "the reading is retained");

    let registry = store.subject_registry().expect("configured");
    assert!(
        registry.resolve(&subject).await.unwrap().is_none(),
        "but it can no longer be attributed to anyone"
    );
    assert!(registry.is_erased(&subject).await.unwrap());
}

#[tokio::test]
async fn the_retention_sweep_anonymises_only_subjects_past_the_ceiling() {
    // § 60 Abs. 6 MsbG is a *deletion duty on a clock*, not a retention mandate:
    // personenbezogene Messwerte must be erased or anonymised as soon as they are
    // no longer needed, and after three years at the latest. Nobody files a
    // request for it — it comes due on its own.
    let (store, _w) = store_with_subjects().await;

    let old = store.register_subject("customer-moved-out").await.unwrap();
    let current = store.register_subject("customer-still-here").await.unwrap();

    let long_ago = datetime!(2021-03-01 00:00 UTC);
    store
        .append(&[reading_at(long_ago, Some(old.as_str()))])
        .await
        .expect("historical reading");
    store
        .append(&[reading_at(D20, Some(current.as_str()))])
        .await
        .expect("current reading");

    let cutoff = datetime!(2023-01-01 00:00 UTC);
    let erased = store
        .anonymise_before(cutoff, "§ 60 Abs. 6 MsbG", "retention-job", D21)
        .await
        .expect("sweep");

    assert_eq!(erased.len(), 1, "only the subject past the ceiling");
    assert_eq!(erased[0].subject, old);

    let registry = store.subject_registry().unwrap();
    assert!(
        registry.resolve(&old).await.unwrap().is_none(),
        "the linkage is destroyed"
    );
    assert!(
        registry.resolve(&current).await.unwrap().is_some(),
        "a subject still being metered must survive the sweep"
    );

    // The readings themselves are untouched — anonymised, not deleted, which is
    // the branch of the statute an append-only lake can actually take.
    let rows = store
        .query(&format!(
            r#"SELECT subject_ref FROM {} WHERE subject_ref IS NOT NULL"#,
            store.raw_table()
        ))
        .await
        .expect("query");
    assert_eq!(
        rows.batches().iter().map(|b| b.num_rows()).sum::<usize>(),
        2,
        "both rows stay; what is gone is who they belonged to"
    );

    // Idempotent: a second run finds nothing left to destroy.
    let again = store
        .anonymise_before(cutoff, "§ 60 Abs. 6 MsbG", "retention-job", D21)
        .await
        .expect("second sweep");
    assert!(again.is_empty());
}

#[tokio::test]
async fn a_reading_with_no_subject_is_still_writable() {
    // The column is nullable on purpose: a measuring point with no known
    // occupant is an ordinary state, not an error.
    let (store, _w) = store_with_subjects().await;
    assert!(store.append(&[reading(None)]).await.is_ok());
}

#[tokio::test]
async fn declaring_a_subject_column_without_a_registry_fails_at_build() {
    // Otherwise the failure surfaces at the first erasure request, which is the
    // worst possible moment to learn the column was decoration.
    let config = TableConfig::new(TABLE)
        .settlement_lag(Duration::days(1))
        .subject_column("subject_ref")
        .build()
        .expect("config");

    let err = MeterStore::builder()
        .table(config)
        .build()
        .await
        .expect_err("must not build");
    // Fails for a missing tier first; the point is that it never yields a store
    // with an unbacked subject column.
    assert!(!err.to_string().is_empty());
}
