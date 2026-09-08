//! Erasure wired into the store, against real PostgreSQL and Iceberg.
//!
//! The registry on its own is a mapping table. What makes it a compliance
//! mechanism is that the store refuses to write references the registry does not
//! back — otherwise a pipeline can quietly produce rows that are unattributable
//! from birth, or re-link a subject whose erasure has already been certified.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them: `testkit::postgres` is what shares one container
// across the binary instead of starting one per test.
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
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use sqlx::PgPool;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const TABLE: &str = "readings_versions";
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);
const SECRET: &[u8] = b"test-suppression-key-32-bytes!!!";
/// A second subject-bearing stream, the shape every EDM deployment has.
const SECOND_TABLE: &str = "esa_typ2_versions";
/// An interval well past any retention ceiling the sweep tests use.
const OLD: OffsetDateTime = datetime!(2022-07-20 00:00 UTC);

/// A store whose readings carry a pseudonymous subject reference.
async fn store_with_subjects() -> (MeterStore, tempfile::TempDir) {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");

    let pool = PgPool::connect(&url).await.expect("connect");
    let hot = Arc::new(PostgresHot::new(pool.clone()));

    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory))
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

/// A catalog of two subject-bearing tables sharing one registry.
///
/// The shape every EDM deployment has — an authoritative Lastgang beside a second
/// stream — and the one the retention sweep has to be correct for, because the
/// registry is *deployment-wide*: one map keyed by natural identifier, so a
/// single erasure unlinks a subject in both tables at once.
async fn catalog_with_subjects() -> (meterstore::MeterCatalog, tempfile::TempDir) {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let pool = PgPool::connect(&url).await.expect("connect");
    let hot = Arc::new(PostgresHot::new(pool.clone()));

    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory))
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
    let registry = SubjectRegistry::with_erasure_secret(pool, SECRET).expect("secret");
    registry.create_tables().await.expect("registry tables");

    let mut builder = meterstore::MeterCatalog::builder();
    for name in [TABLE, SECOND_TABLE] {
        let config = TableConfig::new(name)
            .settlement_lag(Duration::days(1))
            .subject_column("subject_ref")
            .build()
            .expect("config");

        let hot_dyn: Arc<dyn HotStore> = hot.clone();
        let cold_dyn: Arc<dyn ColdStore> = cold.clone();
        hot_dyn
            .create_tables(
                name,
                &config.merge_key(),
                &config.extra_columns(),
                config.time_model(),
            )
            .await
            .expect("hot table");
        cold_dyn
            .create_tables(
                name,
                &config.identity_column_names(),
                &config.extra_columns(),
            )
            .await
            .expect("cold table");
        hot.ensure_partitions(name, D20, OLD + Duration::days(400), Duration::DAY)
            .await
            .expect("partitions");

        builder = builder.table(
            MeterStore::builder()
                .hot(hot_dyn)
                .cold(cold_dyn, cold.table_provider(name).await.expect("provider"))
                .table(config)
                .subject_registry(registry.clone()),
        );
    }

    (builder.build().await.expect("catalog"), warehouse)
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
            VersionScope::for_interval("9900000000001", from, Sparte::Strom).unwrap(),
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

    let subject = store
        .register_subject("customer-4821", D20, Sparte::Strom)
        .await
        .unwrap();
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
        .append(&[reading(Some("s2026_deadbeefdeadbeefdeadbeefdeadbeef"))])
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

    let subject = store
        .register_subject("customer-4821", D20, Sparte::Strom)
        .await
        .unwrap();
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
        store
            .register_subject("customer-4821", D20, Sparte::Strom)
            .await
            .is_err(),
        "re-registration must stay suppressed"
    );
}

#[tokio::test]
async fn the_readings_survive_erasure_and_stop_being_attributable() {
    // Article 17 over append-only storage: the rows stay — retention law
    // requires them — and what is destroyed is the ability to say whose they are.
    let (store, _w) = store_with_subjects().await;

    let subject = store
        .register_subject("customer-4821", D20, Sparte::Strom)
        .await
        .unwrap();
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

    let long_ago = datetime!(2021-03-01 00:00 UTC);
    let old = store
        .register_subject("customer-moved-out", long_ago, Sparte::Strom)
        .await
        .unwrap();
    let current = store
        .register_subject("customer-still-here", D20, Sparte::Strom)
        .await
        .unwrap();

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
    assert_eq!(erased[0].subject.as_ref(), Some(&old));

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
async fn a_reference_from_the_wrong_collection_year_is_refused_at_the_write() {
    // The guarantee the epoch buys is only real if a reference cannot be used on
    // another year's readings. Nothing downstream could tell: the column is
    // well-formed, the reference resolves, and the only symptom would be that the
    // sweep never comes due for those rows — years later, and unattributably.
    //
    // The year is in the reference, so the check costs a string parse.
    let (store, _w) = store_with_subjects().await;

    let this_year = store
        .register_subject("customer-4821", D20, Sparte::Strom)
        .await
        .expect("register 2026");

    let err = store
        .append(&[reading_at(OLD, Some(this_year.as_str()))])
        .await
        .expect_err("a 2026 reference must not attribute a 2022 reading");

    let msg = err.to_string();
    assert!(msg.contains("retention epoch"), "{msg}");
    assert!(msg.contains("2026") && msg.contains("2022"), "{msg}");

    // The right reference for that year is accepted, and it is a different one.
    let that_year = store
        .register_subject("customer-4821", OLD, Sparte::Strom)
        .await
        .expect("register 2022");
    assert_ne!(this_year, that_year);
    store
        .append(&[reading_at(OLD, Some(that_year.as_str()))])
        .await
        .expect("the reference minted for that year is accepted");
}

#[tokio::test]
async fn an_article_17_erasure_reaches_every_year_of_a_subject() {
    // A DSAR names a person, not a year. A caller holding this year's reference
    // and erasing only this year would be told the request was honoured while
    // last year's readings stayed attributable.
    let (store, _w) = store_with_subjects().await;
    let registry = store.subject_registry().expect("configured");

    let old = store
        .register_subject("customer-4821", OLD, Sparte::Strom)
        .await
        .expect("register 2022");
    let current = store
        .register_subject("customer-4821", D20, Sparte::Strom)
        .await
        .expect("register 2026");

    let erased = store
        .erase_subject(&current, "DSAR-2026-0042", "privacy-team", D21)
        .await
        .expect("erase");

    assert_eq!(erased.len(), 2, "both years: {erased:?}");
    assert!(registry.resolve(&old).await.unwrap().is_none());
    assert!(registry.resolve(&current).await.unwrap().is_none());
}

#[tokio::test]
async fn a_sweep_expires_one_year_of_a_subject_and_leaves_the_rest() {
    // The property the earlier design could not provide, and the reason a
    // reference is scoped to a collection year.
    //
    // § 60 Abs. 6 MsbG runs on *"der jeweilige Messwert"*: a value collected in
    // 2021 comes due at the end of 2024 whether or not the same customer is
    // still being metered today. Keyed to a subject's *latest* reading — as this
    // once was — an active customer's decade-old values stayed attributable for
    // as long as they stayed connected, and the sweep could never say so.
    //
    // Keyed to the epoch, the two are independent: 2021 goes, 2026 stays, and
    // the readings themselves are never consulted.
    let (catalog, _w) = catalog_with_subjects().await;
    let authoritative = catalog.table(TABLE).expect("first table");
    let second = catalog.table(SECOND_TABLE).expect("second table");
    let registry = authoritative.subject_registry().expect("configured");

    // One person, two collection years, one natural identifier.
    let then = authoritative
        .register_subject("customer-still-here", OLD, Sparte::Strom)
        .await
        .expect("register 2022");
    let now_ref = authoritative
        .register_subject("customer-still-here", D20, Sparte::Strom)
        .await
        .expect("register 2026");
    assert_ne!(then, now_ref, "a year is its own unit of erasure");

    authoritative
        .append(&[reading_at(OLD, Some(then.as_str()))])
        .await
        .expect("old reading");
    second
        .append(&[reading_at(D20, Some(now_ref.as_str()))])
        .await
        .expect("current reading");

    let cutoff = datetime!(2023-01-01 00:00 UTC);
    let erased = catalog
        .anonymise_before(cutoff, "§ 60 Abs. 6 MsbG", "retention-job", D21)
        .await
        .expect("sweep");

    assert_eq!(erased.len(), 1, "one epoch came due: {erased:?}");
    assert_eq!(erased[0].subject.as_ref(), Some(&then));
    assert!(
        registry.resolve(&then).await.unwrap().is_none(),
        "the expired year is unlinked"
    );
    assert!(
        registry.resolve(&now_ref).await.unwrap().is_some(),
        "and the current year is untouched — the same person, still metered"
    );

    // Idempotent.
    let again = catalog
        .anonymise_before(cutoff, "§ 60 Abs. 6 MsbG", "retention-job", D21)
        .await
        .expect("second sweep");
    assert!(again.is_empty());

    // The per-table sweep is the same operation: the registry is
    // deployment-wide and the epoch is on the mapping row, so neither sweep
    // looks at a table's contents and the two cannot disagree.
    let per_table = authoritative
        .anonymise_before(cutoff, "§ 60 Abs. 6 MsbG", "retention-job", D21)
        .await
        .expect("per-table sweep");
    assert!(
        per_table.is_empty(),
        "nothing is left due, whichever handle asks: {per_table:?}"
    );
}

#[tokio::test]
async fn the_scheduled_sweep_is_the_catalog_sweep() {
    // A maintenance loop must reach the same conclusion the manual sweep does,
    // or the scheduled duty is a second implementation of the rule.
    let (catalog, _w) = catalog_with_subjects().await;
    let store = catalog.table(TABLE).expect("first table");

    let subject = store
        .register_subject("customer-old", OLD, Sparte::Strom)
        .await
        .expect("register");
    store
        .append(&[reading_at(OLD, Some(subject.as_str()))])
        .await
        .expect("old reading");

    // Off by default: destroying a linkage uninvited would be taking a
    // compliance decision on the operator's behalf.
    let quiet = catalog.maintenance().run_once(D21).await.expect("cycle");
    assert!(quiet.anonymised.is_empty());
    assert!(quiet.retention_failure.is_none());
    assert!(
        store
            .subject_registry()
            .unwrap()
            .resolve(&subject)
            .await
            .unwrap()
            .is_some()
    );

    // Turned on, the same cycle applies the ceiling. `D21` is in 2026 and the
    // reading is from 2022, so three full calendar years have passed.
    let sweeping = catalog
        .maintenance()
        .anonymise_after(
            meterstore::Retention::CalendarYears(3),
            "§ 60 Abs. 6 MsbG",
            "maintenance",
        )
        .run_once(D21)
        .await
        .expect("cycle");

    assert_eq!(sweeping.subjects_anonymised(), 1, "{sweeping:?}");
    assert!(sweeping.healthy());
    assert!(
        store
            .subject_registry()
            .unwrap()
            .resolve(&subject)
            .await
            .unwrap()
            .is_none(),
        "the linkage is destroyed"
    );
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

#[tokio::test]
async fn a_sweep_needs_no_session_at_all_so_a_restricted_view_cannot_skew_it() {
    // A sweep whose due-date came from `max(from)` over the session running it
    // would be the one operation where a restricted read mode destroys data
    // rather than misreporting it: under `Historical`, which sees the cold tier
    // only, a customer metered daily looks last-seen at the final archived
    // interval — old enough to erase, still live, and no recovery path.
    //
    // The epoch is on the mapping row instead, so what comes due is a calendar
    // fact about a `(subject, year)` pair. No reading is consulted and every
    // handle gives the same answer, which is why no read mode has to be refused.
    let (store, _w) = store_with_subjects().await;

    let live = store
        .register_subject("customer-still-here", D20, Sparte::Strom)
        .await
        .unwrap();
    store
        .append(&[reading_at(D20, Some(live.as_str()))])
        .await
        .expect("a current reading, in the hot window");

    // A cutoff far in the future: under the old rule every restricted mode would
    // have found this subject due and erased it.
    let cutoff = datetime!(2030-01-01 00:00 UTC);

    for mode in [
        meterstore::ReadMode::Historical,
        meterstore::ReadMode::Operational,
    ] {
        let restricted = store.in_read_mode(mode).await.expect("derived session");
        let erased = restricted
            .anonymise_before(
                datetime!(2023-01-01 00:00 UTC),
                "§ 60 Abs. 6 MsbG",
                "job",
                D21,
            )
            .await
            .expect("a sweep decides from the calendar, not from what it can see");
        assert!(erased.is_empty(), "{mode:?} erased {erased:?}");
        assert!(
            store
                .subject_registry()
                .unwrap()
                .resolve(&live)
                .await
                .unwrap()
                .is_some(),
            "{mode:?} must not touch a live epoch"
        );
    }

    // And the 2026 epoch does come due once the cutoff passes it — from a
    // restricted session as readily as from the store it was derived from,
    // because neither consults a reading.
    let erased = store
        .in_read_mode(meterstore::ReadMode::Historical)
        .await
        .expect("derived session")
        .anonymise_before(cutoff, "§ 60 Abs. 6 MsbG", "retention-job", D21)
        .await
        .expect("sweep");
    assert_eq!(erased.len(), 1, "{erased:?}");
    assert_eq!(erased[0].subject.as_ref(), Some(&live));
}

/// The same reading, as gas.
///
/// Gas is the only commodity whose balancing day is not the calendar day, so it
/// is the only one where the epoch and the wall-clock year can disagree.
fn gas_reading_at(from: OffsetDateTime, subject: Option<&str>) -> StoredSeries {
    let interval = metering::interval::MeterInterval {
        from,
        to: from + Duration::minutes(15),
        value: rust_decimal::Decimal::new(42, 0),
        quality: metering::QualityFlag::Measured,
        obis_code: "7-1:99.33.0".parse().ok(),
    };
    let series = MeasurementSeries::new(
        "11111111115".parse().unwrap(),
        "7-1:99.33.0".parse().ok(),
        vec![interval],
        MeasurementSource::ManualEntry {
            operator_id: "test".to_string(),
            reason: "fixture".to_string(),
        },
        datetime!(2026-07-27 06:00 UTC),
    );

    let stored = StoredSeries::of(
        Sparte::Gas,
        series,
        ScopedVersion::new(
            VersionScope::for_interval("9900000000001", from, Sparte::Gas).unwrap(),
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
async fn a_gas_reading_in_the_first_hours_of_the_year_is_storable() {
    // The regression. `2026-01-01T00:00Z` is 01:00 on New Year's Day in Berlin:
    // calendar year 2026, and still Gastag 2025-12-31 — the day it is balanced,
    // settled and invoiced on.
    //
    // Were `register_subject` to read the epoch off the local calendar while the
    // write path checks it against the balancing year, the only reference the
    // public API could mint for such a reading would be one the write refuses —
    // and the error would prescribe the very call that cannot satisfy it. Six
    // hours of gas a year, unstorable.
    let (store, _w) = store_with_subjects().await;
    let at = datetime!(2026-01-01 0:00 UTC);

    let subject = store
        .register_subject("customer-4821", at, Sparte::Gas)
        .await
        .expect("register");
    assert_eq!(
        subject.epoch().unwrap(),
        2025,
        "the year the Gastag is balanced in, not the one the wall clock shows"
    );

    let outcome = store
        .append(&[gas_reading_at(at, Some(subject.as_str()))])
        .await;
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(outcome.unwrap().total(), 1);
}

#[tokio::test]
async fn the_same_instant_is_a_different_epoch_for_electricity() {
    // The other half of the rule: electricity balances on the calendar day, so
    // the same instant is 2026 — and the reference minted for gas must not be
    // accepted on it, because the two come due a year apart.
    let (store, _w) = store_with_subjects().await;
    let at = datetime!(2026-01-01 0:00 UTC);

    let gas = store
        .register_subject("customer-4821", at, Sparte::Gas)
        .await
        .expect("register gas");
    let power = store
        .register_subject("customer-4821", at, Sparte::Strom)
        .await
        .expect("register electricity");
    assert_eq!(power.epoch().unwrap(), 2026);
    assert_ne!(gas, power);

    store
        .append(&[reading_at(at, Some(power.as_str()))])
        .await
        .expect("the electricity reference is accepted on an electricity reading");

    let err = store
        .append(&[reading_at(at, Some(gas.as_str()))])
        .await
        .expect_err("a 2025 reference must not attribute a 2026 reading");
    let msg = err.to_string();
    assert!(msg.contains("retention epoch"), "{msg}");
    assert!(msg.contains("2025") && msg.contains("2026"), "{msg}");
}

#[tokio::test]
async fn a_subjects_epochs_can_be_enumerated_from_the_store() {
    // An Article 17 request names a person, so answering it starts with "which
    // years do we still link?". Without an enumeration a consumer has to guess a
    // window and probe it year by year.
    let (store, _w) = store_with_subjects().await;

    store
        .register_subject("customer-4821", OLD, Sparte::Strom)
        .await
        .expect("register 2022");
    store
        .register_subject("customer-4821", D20, Sparte::Strom)
        .await
        .expect("register 2026");

    assert_eq!(
        store.subject_epochs("customer-4821").await.unwrap(),
        vec![2022, 2026]
    );
    assert_eq!(
        store
            .subject_registrations("customer-4821")
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(store.subject_epochs("never-seen").await.unwrap().is_empty());
}

#[tokio::test]
async fn an_article_17_request_can_be_answered_from_the_identifier_alone() {
    // What a request actually carries: a customer number, not the opaque token
    // the lake stores.
    let (catalog, _w) = catalog_with_subjects().await;
    let authoritative = catalog.table(TABLE).expect("first table");
    let second = catalog.table(SECOND_TABLE).expect("second table");
    let registry = authoritative.subject_registry().expect("configured");

    let then = authoritative
        .register_subject("customer-4821", OLD, Sparte::Strom)
        .await
        .expect("register 2022");
    let now_ref = authoritative
        .register_subject("customer-4821", D20, Sparte::Strom)
        .await
        .expect("register 2026");

    authoritative
        .append(&[reading_at(OLD, Some(then.as_str()))])
        .await
        .expect("old reading");
    second
        .append(&[reading_at(D20, Some(now_ref.as_str()))])
        .await
        .expect("current reading");

    let erased = catalog
        .erase_subject_by_id("customer-4821", "DSAR-2026-0042", "privacy-team", D21)
        .await
        .expect("erase");

    assert_eq!(erased.len(), 2, "every epoch of the person: {erased:?}");
    assert!(registry.resolve(&then).await.unwrap().is_none());
    assert!(registry.resolve(&now_ref).await.unwrap().is_none());
    assert!(
        catalog
            .subject_epochs("customer-4821")
            .await
            .unwrap()
            .is_empty()
    );

    // The readings themselves survive in both tables — anonymised, not deleted.
    for (store, table) in [(authoritative, TABLE), (second, SECOND_TABLE)] {
        let rows = store
            .query(&format!(
                "SELECT subject_ref FROM {} WHERE subject_ref IS NOT NULL",
                store.raw_table()
            ))
            .await
            .unwrap_or_else(|e| panic!("{table}: {e}"));
        assert_eq!(
            rows.batches().iter().map(|b| b.num_rows()).sum::<usize>(),
            1,
            "{table} keeps its row"
        );
    }
}

#[tokio::test]
async fn a_request_that_arrives_before_the_ingest_is_still_honoured() {
    // The pipeline has not delivered this customer yet, so there is no linkage
    // to destroy — and the half of the request that matters is the other one.
    let (store, _w) = store_with_subjects().await;

    let recorded = store
        .erase_subject_by_id("customer-not-yet-here", "DSAR-2026-0100", "dpo", D21)
        .await
        .expect("erase");

    assert_eq!(recorded.len(), 1);
    assert!(recorded[0].subject.is_none(), "{recorded:?}");
    assert!(
        store
            .register_subject("customer-not-yet-here", D20, Sparte::Strom)
            .await
            .is_err(),
        "the delivery that follows must not rebuild the link"
    );
}
