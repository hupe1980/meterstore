//! Identity columns, end to end against real PostgreSQL and Iceberg.
//!
//! A tenant discriminator has to make two readings *different readings*. If it
//! were an attribute rather than part of the identity, two tenants reporting the
//! same measuring point would share a merge key and one tenant's correction
//! would supersede the other's reading — a cross-tenant data leak with no error
//! anywhere.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them: `testkit::postgres` is what shares one container
// across the binary instead of starting one per test.
#![cfg(feature = "testkit")]

use metering::series::interval::Sparte;
use std::sync::Arc;

use datafusion::common::ScalarValue;
use meterstore::arrow::datatypes::{DataType, Field};
use meterstore::cold::IcebergCold;
use meterstore::config::TableConfig;
use meterstore::encode::StoredSeries;
use meterstore::hot::PostgresHot;
use meterstore::tiering::store::{ColdStore, HotStore, WriteHints, stream_of};
use meterstore::{MeterStore, ScopedVersion, Version, VersionScope};

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use meterstore::delivery::{MeasurementSeries, MeasurementSource};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const TABLE: &str = "readings_versions";
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);

/// A store whose readings are identified by tenant as well as measuring point.
async fn store_with_tenant_identity() -> (MeterStore, tempfile::TempDir) {
    store_with_columns(|config| config).await
}

async fn store_with_columns(
    extend: impl FnOnce(TableConfig) -> TableConfig,
) -> (MeterStore, tempfile::TempDir) {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");

    let database = meterstore::Database::connect(&url).await.expect("database");
    let hot = Arc::new(PostgresHot::new(database));

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

    let config = extend(
        TableConfig::new(TABLE)
            .settlement_lag(Duration::days(1))
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            // A coded attribute column: its vocabulary is enforced by a DB CHECK.
            .attribute_column(meterstore::coded_column(
                "bilanzkreis",
                &["BK-1", "BK-2"],
                true,
            )),
    )
    .build()
    .expect("config");

    // One call creates both tiers with a consistent key and schema.
    let hot_dyn: Arc<dyn HotStore> = hot.clone();
    let cold_dyn: Arc<dyn ColdStore> = cold.clone();
    hot_dyn.create_tables(&config).await.expect("hot table");
    cold_dyn.create_tables(&config).await.expect("cold table");
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
        .build()
        .await
        .expect("store");

    (store, warehouse)
}

/// A reading for one tenant at a given version and value.
fn reading(tenant: &str, kwh: i64, version: u128) -> StoredSeries {
    reading_at(D20, tenant, kwh, version)
}

fn reading_at(at: OffsetDateTime, tenant: &str, kwh: i64, version: u128) -> StoredSeries {
    let interval = metering::series::interval::MeterInterval::new(
        at,
        at + Duration::minutes(15),
        rust_decimal::Decimal::new(kwh, 0),
        metering::QualityFlag::Measured,
    )
    .expect("a non-empty interval")
    .with_obis("1-0:1.8.0".parse().unwrap());
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

    StoredSeries::new(
        series,
        ScopedVersion::new(
            VersionScope::for_interval("9900000000003", at, Sparte::Strom).unwrap(),
            Version::new(version).unwrap(),
        ),
        datetime!(2026-07-27 06:00 UTC),
    )
    .with_extra("tenant", ScalarValue::Utf8(Some(tenant.to_string())))
    .with_extra("bilanzkreis", ScalarValue::Utf8(Some("BK-1".to_string())))
}

async fn sum_kwh(store: &MeterStore, sql: &str) -> i64 {
    use datafusion::arrow::array::AsArray;
    let batches = store.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0)
}

#[tokio::test]
async fn resolved_deliveries_keep_each_slots_version_and_source_attributes() {
    let (store, _warehouse) = store_with_tenant_identity().await;
    let first = reading("a", 10, 1);
    let mut next = reading_at(D21, "a", 20, 2)
        .with_extra("bilanzkreis", ScalarValue::Utf8(Some("BK-2".into())));
    next.recorded_at += Duration::HOUR;
    store
        .append(&[first.clone(), next.clone(), reading("b", 99, 7)])
        .await
        .unwrap();
    let scoped = store.scoped("tenant", "a").await.unwrap();

    // The same export spans both tiers after the first day settles.
    for archived in [false, true] {
        if archived {
            store.admin().archive(D21 + Duration::DAY, 4).await.unwrap();
        }
        let deliveries = scoped
            .series("11111111115")
            .unwrap()
            .range(D20, D21 + Duration::DAY)
            .collect_deliveries()
            .await
            .unwrap();
        assert_eq!(deliveries.len(), 2);
        for (actual, expected) in deliveries.iter().zip([&first, &next]) {
            assert_eq!(actual.version, expected.version);
            assert_eq!(actual.extra, expected.extra);
            assert_eq!(actual.recorded_at, expected.recorded_at);
            assert_eq!(actual.series.intervals, expected.series.intervals);
        }
    }
}

#[tokio::test]
async fn one_tenants_correction_does_not_supersede_anothers_reading() {
    let (store, _w) = store_with_tenant_identity().await;

    // Same MaLo, same interval, two tenants. B's version is higher.
    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .unwrap();
    store
        .append(&[reading("b", 40, 20_260_725_000_002)])
        .await
        .unwrap();

    let total = sum_kwh(&store, "SELECT CAST(SUM(value) AS BIGINT) FROM readings").await;
    assert_eq!(total, 50, "both tenants' readings survive: 10 + 40");

    let rows = sum_kwh(&store, "SELECT COUNT(*) FROM readings").await;
    assert_eq!(rows, 2, "two readings, not one");
}

#[tokio::test]
async fn a_correction_within_one_tenant_still_supersedes() {
    // The identity column must not break resolution, only scope it.
    let (store, _w) = store_with_tenant_identity().await;

    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .unwrap();
    store
        .append(&[reading("a", 40, 20_260_725_000_002)])
        .await
        .unwrap();

    let total = sum_kwh(&store, "SELECT CAST(SUM(value) AS BIGINT) FROM readings").await;
    assert_eq!(total, 40, "the correction wins within a tenant");
}

#[tokio::test]
async fn a_query_can_filter_by_tenant() {
    let (store, _w) = store_with_tenant_identity().await;

    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .unwrap();
    store
        .append(&[reading("b", 40, 20_260_720_000_001)])
        .await
        .unwrap();

    let only_a = sum_kwh(
        &store,
        "SELECT CAST(SUM(value) AS BIGINT) FROM readings WHERE tenant = 'a'",
    )
    .await;
    assert_eq!(only_a, 10);
}

#[tokio::test]
async fn attribute_columns_round_trip_without_joining_the_identity() {
    let (store, _w) = store_with_tenant_identity().await;
    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .unwrap();

    let batches = store
        .sql("SELECT bilanzkreis FROM readings")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    use datafusion::arrow::array::AsArray;
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "BK-1");
}

#[tokio::test]
async fn collect_resolved_recovers_attribute_columns_on_the_typed_read() {
    // A `MeasurementSeries` carries neither the commodity nor the deployment's
    // declared columns, so a caller reconstructing domain rows would have to
    // hard-code them back. `collect_resolved` folds them from the newest
    // contributing delivery instead.
    let (store, _w) = store_with_tenant_identity().await;
    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .unwrap();

    let resolved = store
        .series("11111111115")
        .unwrap()
        .column_eq("tenant", ScalarValue::Utf8(Some("a".to_string())))
        .unwrap()
        .range(D20, D21)
        .collect_resolved()
        .await
        .unwrap()
        .expect("the range holds a reading");

    assert_eq!(
        resolved.extra.get("bilanzkreis"),
        Some(&ScalarValue::Utf8(Some("BK-1".to_string()))),
        "the attribute column survives the typed read"
    );
    assert_eq!(
        resolved.extra.get("tenant"),
        Some(&ScalarValue::Utf8(Some("a".to_string()))),
        "the identity column is recovered too"
    );
    assert_eq!(resolved.series.intervals.len(), 1);
}

#[tokio::test]
async fn an_unscoped_typed_read_refuses_to_fold_two_tenants() {
    // `series()` filters by measuring point, two tenants report the same one,
    // and version resolution leaves a row per tenant per interval — so a fold
    // would produce a `MeasurementSeries` with two intervals at every instant.
    // `aggregate` sums them: the month doubles, with one tenant's readings
    // inside the other's series.
    let (store, _w) = store_with_tenant_identity().await;
    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .unwrap();
    store
        .append(&[reading("b", 25, 20_260_720_000_001)])
        .await
        .unwrap();

    let err = store
        .series("11111111115")
        .unwrap()
        .range(D20, D21)
        .collect()
        .await
        .expect_err("a series that spans tenants is not a series")
        .to_string();
    assert!(err.contains("two readings"), "{err}");
    assert!(err.contains("tenant="), "{err}");

    // Naming the tenant is the whole of the fix, and it returns that tenant's
    // reading rather than the sum of both.
    let scoped = store
        .series("11111111115")
        .unwrap()
        .column_eq("tenant", ScalarValue::Utf8(Some("a".to_string())))
        .unwrap()
        .range(D20, D21)
        .collect()
        .await
        .unwrap()
        .expect("tenant a has a reading");
    assert_eq!(scoped.intervals.len(), 1);
    assert_eq!(
        scoped.intervals[0].value(),
        rust_decimal::Decimal::new(10, 0)
    );

    // …and so is scoping the session, which is what a service exposing SQL uses.
    let confined = store.scoped("tenant", "b").await.unwrap();
    let only_b = confined
        .series("11111111115")
        .unwrap()
        .range(D20, D21)
        .collect()
        .await
        .unwrap()
        .expect("tenant b has a reading");
    assert_eq!(
        only_b.intervals[0].value(),
        rust_decimal::Decimal::new(25, 0)
    );
}

#[tokio::test]
async fn caller_supplied_sql_cannot_step_outside_the_query_surface() {
    // A row scope is enforced inside a table provider, so it can only confine
    // statements that go *through* one. DataFusion's SQL surface is wider than
    // `SELECT`, and three of its statements never touch a provider at all:
    //
    //   CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '<warehouse>/…'
    //
    // reads the cold tier's own Parquet — every tenant's rows, unscoped — and
    // `ctx.sql` *executes* DDL while planning it, so it is one round trip from
    // any surface that runs caller-supplied SQL. `COPY … TO` is the same door in
    // the other direction.
    let (store, warehouse) = store_with_tenant_identity().await;
    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .unwrap();

    let scoped = store.scoped("tenant", "a").await.unwrap();
    let escape = warehouse.path().join("escape.parquet");

    for sql in [
        format!(
            "CREATE EXTERNAL TABLE leak STORED AS PARQUET LOCATION '{}'",
            warehouse.path().display()
        ),
        format!(
            "COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
            escape.display()
        ),
        "CREATE TABLE leak2 AS SELECT 1 AS a".to_string(),
        "INSERT INTO readings_versions SELECT * FROM readings_versions".to_string(),
        "DROP TABLE readings".to_string(),
        "SET datafusion.execution.batch_size = 1".to_string(),
        // The wrapper that made a root-only check useless: planning a COPY is
        // what performs it, so EXPLAIN does not make it harmless.
        format!(
            "EXPLAIN COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
            escape.display()
        ),
    ] {
        let err = scoped
            .query(&sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("{sql} was accepted"));
        assert!(
            err.to_string().contains("not accepted here"),
            "{sql} failed for the wrong reason: {err}"
        );
    }

    assert!(!escape.exists(), "no statement may write a file");

    // And an ordinary query still works, scoped.
    let rows = scoped
        .query("SELECT COUNT(*) AS n FROM readings")
        .await
        .expect("a query is a query")
        .to_json()
        .unwrap();
    assert_eq!(rows[0]["n"].as_i64(), Some(1));

    // `EXPLAIN SELECT` is a read, and stays available.
    assert!(scoped.query("EXPLAIN SELECT 1").await.is_ok());
}

#[tokio::test]
async fn a_missing_identity_value_is_rejected_rather_than_defaulted() {
    // Silently writing a null tenant would merge readings across tenants.
    let (store, _w) = store_with_tenant_identity().await;

    let mut incomplete = reading("a", 10, 20_260_720_000_001);
    incomplete.extra.remove("tenant");

    assert!(
        store.append(&[incomplete]).await.is_err(),
        "a declared identity column must have a value"
    );
}

#[tokio::test]
async fn identity_columns_survive_archival_into_the_cold_tier() {
    // Extra columns are threaded end to end — encoder, both
    // tables, the `ON CONFLICT` target, the scan projection and the resolution
    // `PARTITION BY`. The **archival** scan was missed, and it is the one path
    // that carries them from PostgreSQL into Iceberg. A tenant discriminator
    // that vanishes on archival makes every historical row unattributable, and
    // merges two tenants' history into one merge key.
    let (store, _w) = store_with_tenant_identity().await;

    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .unwrap();
    store
        .append(&[reading("b", 40, 20_260_725_000_002)])
        .await
        .unwrap();

    // Seed the watermark at the start of the day, so the first window archived
    // is D20 itself. Without this the archiver starts at the epoch and spends
    // its window budget on empty 1970 days, and the test proves nothing.
    store
        .admin()
        .cold_store()
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            meterstore::watermark::ArchivalWindow::new(D20 - Duration::DAY, D20).unwrap(),
            Duration::DAY,
            D20,
        )
        .await
        .expect("seed watermark");

    store
        .admin()
        .archive(D21 + Duration::DAY, 4)
        .await
        .expect("archive");
    assert_eq!(
        store.watermark().await.unwrap().get(),
        D21,
        "the whole day must be in the cold tier"
    );

    let total = sum_kwh(&store, "SELECT CAST(SUM(value) AS BIGINT) FROM readings").await;
    assert_eq!(total, 50, "both tenants' readings must survive archival");

    let tenants = sum_kwh(
        &store,
        "SELECT COUNT(DISTINCT tenant) FROM readings WHERE tenant IS NOT NULL",
    )
    .await;
    assert_eq!(tenants, 2, "the discriminator must reach Iceberg");
}

#[tokio::test]
async fn the_overlap_exclusion_is_scoped_to_a_tenant() {
    // The failure a constraint built from the *core* merge key rather than the
    // actual primary key would produce: two operators reporting the same MaLo
    // and the same interval are two readings, and rejecting the second as an
    // overlap would be cross-tenant interference dressed up as data validation.
    let (store, _w) = store_with_tenant_identity().await;

    for tenant in ["9900000000003", "9900000000029"] {
        store
            .append(&[reading(tenant, 10, 20_260_720_000_001)])
            .await
            .unwrap_or_else(|e| panic!("{tenant} must be storable alongside the other: {e}"));
    }

    let result = store
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("query");
    use meterstore::arrow::array::AsArray;
    let n = result.batches()[0]
        .column(0)
        .as_primitive::<meterstore::arrow::datatypes::Int64Type>()
        .value(0);
    assert_eq!(n, 2, "both tenants' readings survive");
}

#[tokio::test]
async fn a_coded_attribute_column_refuses_a_value_outside_its_vocabulary() {
    // `bilanzkreis` is declared with `coded_column(&["BK-1", "BK-2"])`, so a value
    // outside that set must fail at the DB CHECK, not be stored and read back as an
    // unknown code — the same guarantee sparte/unit/quality already carry.
    let (store, _w) = store_with_tenant_identity().await;

    let bad = reading("a", 10, 20_260_720_000_001).with_extra(
        "bilanzkreis",
        ScalarValue::Utf8(Some("BK-BOGUS".to_string())),
    );
    assert!(
        store.append(&[bad]).await.is_err(),
        "a bilanzkreis outside the coded vocabulary must be rejected"
    );

    // An allowed value still writes.
    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .expect("an allowed bilanzkreis must insert");
}

// ── A session confined to one tenant ─────────────────────────────────────────

#[tokio::test]
async fn a_scoped_session_confines_caller_supplied_sql_to_one_tenant() {
    // The point: `query` runs SQL the caller wrote, so a service exposing an
    // ad-hoc SQL endpoint has no way to add a tenant predicate — and a
    // deny-list of relation names is a boundary that holds until someone adds a
    // table. The predicate is injected into the plan instead, below the
    // projection, so no statement can omit it.
    let (store, _w) = store_with_tenant_identity().await;

    store
        .append(&[
            reading("a", 10, 20_260_720_000_001),
            reading("b", 99, 20_260_720_000_001),
        ])
        .await
        .expect("both tenants");

    let unscoped = count(&store, "SELECT COUNT(*) FROM readings").await;
    assert_eq!(unscoped, 2, "both tenants are there to be found");

    let scoped = store.scoped("tenant", "a").await.expect("scope");
    assert_eq!(count(&scoped, "SELECT COUNT(*) FROM readings").await, 1);

    // The raw audit relation is confined too — a caller reaching past the
    // resolved view must not step outside the scope.
    assert_eq!(
        count(&scoped, "SELECT COUNT(*) FROM readings_versions").await,
        1
    );

    // And a statement that names the other tenant explicitly still cannot see
    // it: the enforced predicate is conjoined with whatever the caller wrote.
    assert_eq!(
        count(&scoped, "SELECT COUNT(*) FROM readings WHERE tenant = 'b'").await,
        0,
        "a caller cannot select its way out of the scope"
    );

    // Nor by unioning, aliasing or sub-selecting around it.
    assert_eq!(
        count(
            &scoped,
            "SELECT COUNT(*) FROM (SELECT * FROM readings UNION ALL SELECT * FROM readings) t"
        )
        .await,
        2,
        "the scope applies to each scan, so the union is two scoped scans"
    );

    // The value the scoped session returns is the scoped tenant's.
    let total = count(&scoped, "SELECT CAST(SUM(value) AS BIGINT) FROM readings").await;
    assert_eq!(total, 10, "tenant b's 99 is not in this session at all");
}

#[tokio::test]
async fn scoping_composes_and_does_not_come_off() {
    let (store, _w) = store_with_tenant_identity().await;
    store
        .append(&[
            reading("a", 10, 20_260_720_000_001),
            reading("b", 99, 20_260_720_000_001),
        ])
        .await
        .expect("both tenants");

    let scoped = store.scoped("tenant", "a").await.expect("scope");

    // A transaction-time read derived from a scoped store stays scoped: a
    // boundary a derived session drops is not a boundary.
    let then = scoped
        .as_known_at(datetime!(2027-01-01 00:00 UTC))
        .await
        .expect("as_known_at");
    assert_eq!(count(&then, "SELECT COUNT(*) FROM readings").await, 1);
    assert_eq!(then.row_scope().len(), 1);

    // A scoped handle cannot be re-pointed at another tenant. That is the
    // escalation the scope exists to prevent: hand this store to less-trusted
    // code and it must not be able to widen itself.
    let err = scoped.scoped("tenant", "b").await.unwrap_err().to_string();
    assert!(err.contains("already scoped"), "{err}");
    assert!(err.contains("narrows"), "{err}");

    // Re-scoping to the same value is idempotent rather than an error.
    let same = scoped.scoped("tenant", "a").await.expect("idempotent");
    assert_eq!(count(&same, "SELECT COUNT(*) FROM readings").await, 1);

    // And the store it was derived from is unaffected.
    let other = store.scoped("tenant", "b").await.expect("scope b");
    assert_eq!(count(&other, "SELECT COUNT(*) FROM readings").await, 1);
}

#[tokio::test]
async fn only_a_merge_key_column_can_scope_a_session() {
    // Not taste: a merge-key column partitions *readings*, so filtering before
    // or after version resolution gives the same winner. An attribute column
    // would slice through one reading's version history, so a scoped read would
    // resolve to a different value rather than returning fewer rows.
    //
    // The rule is stated over the merge key rather than over the word
    // "identity", because a table that identifies a reading by its Messlokation
    // puts a *core* column in the key and must be scopable on it too.
    let (store, _w) = store_with_tenant_identity().await;

    let err = store
        .scoped("bilanzkreis", "BK-1")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("merge key"), "{err}");
    assert!(
        err.contains("tenant"),
        "the message names what is available: {err}"
    );

    assert!(store.scoped("no_such_column", "x").await.is_err());
    assert!(store.scoped("malo_id", "41373559241").await.is_err());
}

#[tokio::test]
async fn a_scoped_handle_writes_only_inside_its_scope() {
    // A scope confines what a handle reaches, writes included. Refused before
    // anything is written, so the refusal is the whole effect.
    let (store, _w) = store_with_tenant_identity().await;
    let scoped = store.scoped("tenant", "a").await.expect("scope");

    let err = scoped
        .append(&[reading("b", 99, 20_260_720_000_001)])
        .await
        .expect_err("a tenant-a handle writing tenant b");
    assert!(matches!(err, meterstore::Error::Config(_)), "{err:?}");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM readings").await, 0);

    let err = scoped
        .hot_writer()
        .await
        .expect("writer")
        .append(&[reading("b", 99, 20_260_720_000_001)])
        .await
        .expect_err("the bulk writer is confined too");
    assert!(matches!(err, meterstore::Error::Config(_)), "{err:?}");

    scoped
        .append(&[reading("a", 99, 20_260_720_000_001)])
        .await
        .expect("its own tenant");
    assert_eq!(count(&scoped, "SELECT COUNT(*) FROM readings").await, 1);
}

#[tokio::test]
async fn a_scoped_handle_cannot_store_another_tenants_cold_row_twice() {
    // A late correction is reconciled against what the session sees. A tenant-a
    // handle sees none of tenant b's stored rows, so a redelivered tenant-b
    // reading would find nothing and be written a second time.
    let (store, _w) = store_with_tenant_identity().await;
    store
        .append(&[reading("b", 40, 20_260_720_000_001)])
        .await
        .expect("tenant b");
    store
        .admin()
        .cold_store()
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            meterstore::watermark::ArchivalWindow::new(D20 - Duration::DAY, D20).unwrap(),
            Duration::DAY,
            D20,
        )
        .await
        .expect("seed watermark");
    store
        .admin()
        .archive(D21 + Duration::DAY, 4)
        .await
        .expect("archive");

    let scoped = store.scoped("tenant", "a").await.expect("scope");
    scoped
        .append(&[reading("b", 40, 20_260_720_000_001)])
        .await
        .expect_err("outside the scope");

    assert_eq!(
        count(
            &store,
            "SELECT COUNT(*) FROM readings_versions WHERE tenant = 'b'"
        )
        .await,
        1
    );
}

/// A scalar `i64` from a query against `store`.
async fn count(store: &MeterStore, sql: &str) -> i64 {
    use meterstore::arrow::array::AsArray;
    let result = store.query(sql).await.expect("query");
    result.batches()[0]
        .column(0)
        .as_primitive::<meterstore::arrow::datatypes::Int64Type>()
        .value(0)
}

/// The audit that finds the mistake this whole suite exists to prevent, *after*
/// it has been made.
///
/// Declaring a tenant discriminator as an attribute is legal, writes succeed, and
/// nothing raises an error — so the only way to detect it is to ask the stored
/// rows whether the column behaves like identity. Here `tenant` is correctly an
/// identity column and `bilanzkreis` an attribute, so the report is the clean
/// one; the counts are what an operator compares against.
#[tokio::test]
async fn the_attribute_audit_reads_the_declaration_back_off_the_data() {
    let (store, _w) = store_with_tenant_identity().await;

    // Two tenants, one measuring point and interval. `tenant` is in the merge
    // key, so these are two keys rather than one.
    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .unwrap();
    store
        .append(&[reading("b", 40, 20_260_725_000_002)])
        .await
        .unwrap();

    let audit = store
        .admin()
        .audit_attribute_column("bilanzkreis")
        .await
        .unwrap();
    assert_eq!(audit.merge_keys, 2);
    assert_eq!(audit.merge_keys_with_several_values, 0);
    assert_eq!(audit.widest, 1);
    assert!(audit.is_measurable());
    assert_eq!(audit.repetition_ratio(), 0.0);

    // A correction restating the attribute is the ordinary case a real attribute
    // column produces, and it must not read as a mis-declaration on its own.
    let mut restated = reading("a", 11, 20_260_726_000_003);
    restated = restated.with_extra("bilanzkreis", ScalarValue::Utf8(Some("BK-2".to_string())));
    store.append(&[restated]).await.unwrap();

    let audit = store
        .admin()
        .audit_attribute_column("bilanzkreis")
        .await
        .unwrap();
    assert_eq!(
        audit.merge_keys, 2,
        "still two keys — a correction is not one"
    );
    assert_eq!(
        audit.merge_keys_with_several_values, 1,
        "tenant a's key now carries both Bilanzkreise across its versions"
    );
    assert_eq!(audit.widest, 2);
    assert_eq!(audit.repetition_ratio(), 0.5);

    // An identity column has nothing to answer, and a typo must not come back
    // clean: both are refused rather than reported.
    for name in ["tenant", "malo_id", "no_such_column"] {
        assert!(
            store.admin().audit_attribute_column(name).await.is_err(),
            "{name} is not a declared attribute column"
        );
    }

    // And the sweep over every declared attribute column reaches the same answer.
    let all = store.admin().audit_attribute_columns().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].column, "bilanzkreis");
    assert_eq!(all[0].merge_keys_with_several_values, 1);
}

/// A settlement price belongs to the versioned assertion, while the billing
/// group remains a descriptive attribute. A replay compares only the former.
#[tokio::test]
async fn assertion_attributes_identify_replays_on_both_tiers() {
    use meterstore::session::Effect;

    let (store, _warehouse) = store_with_columns(|config| {
        config.assertion_column(Field::new("spot", DataType::Utf8, true))
    })
    .await;
    let site = store.scoped("tenant", "ems").await.unwrap();
    let original = reading("ems", 10, 1);
    let mut priced =
        reading("ems", 10, 2).with_extra("spot", ScalarValue::Utf8(Some("-1.25".into())));
    priced.recorded_at += Duration::HOUR;
    let duplicate = original
        .clone()
        .with_extra("bilanzkreis", ScalarValue::Utf8(Some("BK-2".into())));
    let null_to_zero = original
        .clone()
        .with_extra("spot", ScalarValue::Utf8(Some("0".into())));
    let price_to_zero = priced
        .clone()
        .with_extra("spot", ScalarValue::Utf8(Some("0".into())));
    let conflict = |err: meterstore::Error| {
        assert!(err.to_string().contains("spot"), "{err}");
        assert!(
            matches!(err, meterstore::Error::IntegrityViolation { ref constraint, .. } if constraint.as_deref() == Some("version_identifies_one_assertion")),
            "{err}"
        );
    };
    site.append(std::slice::from_ref(&original)).await.unwrap();
    assert_eq!(
        site.append(std::slice::from_ref(&duplicate))
            .await
            .unwrap()
            .hot_rows,
        0
    );
    conflict(
        site.append(std::slice::from_ref(&null_to_zero))
            .await
            .unwrap_err(),
    );

    let writer = site.hot_writer().await.unwrap();
    // The insert and its conflict check share one transaction. A valid version
    // beside a conflicting replay cannot leak through the bulk writer.
    conflict(
        writer
            .append(&[priced.clone(), null_to_zero.clone()])
            .await
            .unwrap_err(),
    );
    let only = site.append(std::slice::from_ref(&priced)).await.unwrap();
    assert_eq!(only.hot_rows, 1);
    assert_eq!(
        only.displacements[0]
            .superseded
            .as_ref()
            .unwrap()
            .assertion_attributes["spot"],
        None
    );
    assert_eq!(
        only.displacements[0].written.assertion_attributes["spot"].as_deref(),
        Some("-1.25")
    );
    conflict(
        site.append(std::slice::from_ref(&price_to_zero))
            .await
            .unwrap_err(),
    );

    store.admin().archive(D21 + Duration::DAY, 4).await.unwrap();
    for replay in [&original, &priced] {
        let result = site.append(std::slice::from_ref(replay)).await.unwrap();
        assert_eq!((result.hot_rows, result.cold_rows), (0, 0));
        assert_eq!(result.displacements[0].effect, Effect::Duplicate);
    }
    conflict(site.append(&[null_to_zero]).await.unwrap_err());
    conflict(
        site.append(std::slice::from_ref(&price_to_zero))
            .await
            .unwrap_err(),
    );

    let mut zero = price_to_zero;
    zero.version = priced.version.next().unwrap();
    zero.recorded_at += Duration::HOUR;
    assert_eq!(
        site.append(std::slice::from_ref(&zero))
            .await
            .unwrap()
            .cold_rows,
        1
    );

    for (at, predicate) in [
        (original.recorded_at, "spot IS NULL"),
        (priced.recorded_at, "spot = '-1.25'"),
        (zero.recorded_at, "spot = '0'"),
    ] {
        let historical = site.as_known_at(at).await.unwrap();
        assert_eq!(
            sum_kwh(
                &historical,
                &format!("SELECT COUNT(*) FROM readings WHERE {predicate}")
            )
            .await,
            1
        );
    }

    // Reasserting an older version with an unchanged quantity but a different
    // price from the current winner authors a new version.
    let reapplied = site.append_authoritative(&[priced]).await.unwrap();
    assert_eq!(reapplied.cold_rows, 1);
    assert_eq!(
        sum_kwh(&site, "SELECT COUNT(*) FROM readings WHERE spot = '-1.25'").await,
        1
    );
}

#[tokio::test]
async fn single_tier_rejection_and_retry_preserve_both_visible_histories() {
    let (store, _warehouse) = store_with_columns(|c| c.require_single_tier(true)).await;
    let scoped = store.scoped("tenant", "a").await.unwrap();
    let originals = [reading_at(D20, "a", 10, 1), reading_at(D21, "a", 20, 1)];
    assert_eq!(scoped.append(&originals).await.unwrap().hot_rows, 2);
    store.admin().archive(D21 + Duration::DAY, 4).await.unwrap();
    assert_eq!(store.watermark().await.unwrap().get(), D21);

    let both = r#"SELECT "from", version, value FROM readings_versions ORDER BY "from", version"#;
    let before = scoped.query(both).await.unwrap().to_json().unwrap();
    assert_eq!(before.len(), 2);
    let mixed = [reading_at(D21, "a", 21, 4), reading_at(D20, "a", 11, 1)];
    for _ in 0..2 {
        let error = scoped.append(&mixed).await.unwrap_err();
        assert!(!error.is_retryable());
        assert!(
            matches!(error, meterstore::Error::MixedTierBatch { table, watermark }
            if table == TABLE && watermark.get() == D21)
        );
        assert_eq!(scoped.query(both).await.unwrap().to_json().unwrap(), before);
    }

    let writer = scoped.hot_writer().await.unwrap();
    assert!(matches!(
        writer.append(&mixed).await,
        Err(meterstore::Error::MixedTierBatch { .. })
    ));
    assert_eq!(scoped.query(both).await.unwrap().to_json().unwrap(), before);

    assert_eq!(
        scoped
            .append(&[reading_at(D20, "a", 11, 2)])
            .await
            .unwrap()
            .cold_rows,
        1
    );
    assert_eq!(scoped.append(&mixed[..1]).await.unwrap().hot_rows, 1);
}

/// Re-authoring two intervals preserves each delivery's timestamp and locates
/// checked identities in the same spelling as its stored displacement.
#[tokio::test]
async fn authoritative_retries_preserve_identity_and_transaction_time_per_delivery() {
    let (store, _warehouse) = store_with_columns(|c| {
        c.identity_column(meterstore::checked_column(
            "grid_meter",
            meterstore::ValueCheck::Melo,
            false,
        ))
    })
    .await;
    let make = |slot, version, quantity, recorded_at| {
        let mut delivery = reading("ems", quantity, version).with_extra(
            "grid_meter",
            ScalarValue::Utf8(Some(" de0001234567890123456789012345678 ".into())),
        );
        delivery.series.intervals = vec![
            metering::series::interval::MeterInterval::quarter_hour(
                slot,
                rust_decimal::Decimal::new(quantity, 0),
                metering::QualityFlag::Measured,
            )
            .unwrap(),
        ];
        delivery.recorded_at = recorded_at;
        delivery
    };
    let later_slot = D20 + Duration::minutes(15);
    store
        .append(&[make(D20, 10, 1, D21), make(later_slot, 10, 2, D21)])
        .await
        .unwrap();
    let first = D21 + Duration::HOUR;
    let second = first + Duration::HOUR;
    let report = store
        .append_authoritative(&[make(D20, 1, 3, first), make(later_slot, 1, 4, second)])
        .await
        .unwrap();
    assert_eq!(report.displacements.len(), 2);
    let historical = store.as_known_at(first).await.unwrap();
    assert_eq!(
        sum_kwh(
            &historical,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings"
        )
        .await,
        5
    );
    assert_eq!(
        sum_kwh(&store, "SELECT CAST(SUM(value) AS BIGINT) FROM readings").await,
        7
    );
}

/// The quantity is unchanged in every delivery. Only the selected term distinguishes versions.
#[tokio::test]
async fn selected_native_terms_identify_a_replay_on_both_tiers() {
    use metering::{MeterInterval, QualityFlag, Unit};
    use meterstore::{ReplayField, session::Effect};

    let (store, _warehouse) = store_with_columns(|c| {
        c.assert_on(ReplayField::Quality)
            .assert_on(ReplayField::Span)
            .assert_on(ReplayField::Unit)
    })
    .await;
    let mut original = reading("gas", 10, 1);
    original.sparte = Sparte::Gas;
    let interval = &original.series.intervals[0];
    let with_interval = |end, quality| {
        let mut delivery = original.clone();
        delivery.series.intervals =
            vec![MeterInterval::new(interval.from(), end, interval.value(), quality).unwrap()];
        delivery
    };
    let changes = [
        (
            "quality",
            with_interval(interval.to(), QualityFlag::Estimated),
        ),
        (
            "to",
            with_interval(interval.to() + Duration::MICROSECOND, interval.quality()),
        ),
        ("unit", original.clone().in_unit(Unit::CubicMetre)),
    ];
    let same_end = with_interval(interval.to() + Duration::NANOSECOND, interval.quality());
    store.append(std::slice::from_ref(&original)).await.unwrap();

    for cold in [false, true] {
        if cold {
            store.admin().archive(D21 + Duration::DAY, 4).await.unwrap();
            assert!(D20 < store.watermark().await.unwrap().get());
        }
        for (field, delivery) in &changes {
            let err = store
                .append(std::slice::from_ref(delivery))
                .await
                .unwrap_err();
            assert!(
                matches!(&err, meterstore::Error::IntegrityViolation {constraint: Some(name), detail, ..}
                    if name == "version_identifies_one_assertion" && detail.contains(&format!("{field:?}"))),
                "{err}"
            );
        }
        let same = store.append(std::slice::from_ref(&same_end)).await.unwrap();
        assert_eq!(same.displacements[0].effect, Effect::Duplicate);
        assert_eq!(same.total(), 0);

        // An otherwise insertable version alongside a conflict rolls back.
        let conflict = &[reading("another", 10, 1), changes[0].1.clone()];
        if cold {
            assert!(store.append(conflict).await.is_err());
        } else {
            assert!(
                store
                    .hot_writer()
                    .await
                    .unwrap()
                    .append(conflict)
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            sum_kwh(
                &store,
                "SELECT COUNT(*) FROM readings_versions WHERE tenant = 'another'"
            )
            .await,
            0
        );

        // Authored retries consult the current winner even when the requested
        // version already exists with an unchanged quantity.
        for (_, change) in &changes {
            let current = store
                .append_authoritative(std::slice::from_ref(&original))
                .await
                .unwrap();
            assert_eq!(current.total(), 0);
            let mut changed = change.clone();
            changed.version = current.displacements[0].current().version.next().unwrap();
            store.append(&[changed]).await.unwrap();
            let reapplied = store
                .append_authoritative(std::slice::from_ref(&original))
                .await
                .unwrap();
            assert_eq!(reapplied.total(), 1);
            assert_eq!(reapplied.displacements[0].effect, Effect::Superseded);
        }
    }
}

#[tokio::test]
async fn selecting_quality_leaves_unit_and_span_descriptive() {
    use metering::{MeterInterval, Unit};
    let (store, _warehouse) =
        store_with_columns(|c| c.assert_on(meterstore::ReplayField::Quality)).await;
    let mut original = reading("gas", 10, 1);
    original.sparte = Sparte::Gas;
    store.append(std::slice::from_ref(&original)).await.unwrap();
    let mut changed = original.in_unit(Unit::CubicMetre);
    let interval = &changed.series.intervals[0];
    changed.series.intervals = vec![
        MeterInterval::new(
            interval.from(),
            interval.to() + Duration::SECOND,
            interval.value(),
            interval.quality(),
        )
        .unwrap(),
    ];
    for cold in [false, true] {
        if cold {
            store.admin().archive(D21 + Duration::DAY, 4).await.unwrap();
        }
        assert_eq!(
            store
                .append(std::slice::from_ref(&changed))
                .await
                .unwrap()
                .total(),
            0
        );
    }
}

/// Exactly one correction can consume a slot's current scoped version. Both
/// contenders carry the same quantity, so treating a retry as a successful
/// duplicate would give two producers confirmation for the same transition.
#[tokio::test]
async fn a_predecessor_can_be_consumed_once_per_identity_in_either_tier() {
    let (store, _warehouse) = store_with_tenant_identity().await;
    let a = store.scoped("tenant", "a").await.unwrap();
    let b = store.scoped("tenant", "b").await.unwrap();
    let original = reading("a", 10, 7);
    let mut change = reading("a", 25, 7);
    change.recorded_at += Duration::HOUR;
    let absent = a.correct_if_current(&change).await.unwrap_err();
    assert!(matches!(
        absent,
        meterstore::Error::PredecessorMismatch { actual: None, .. }
    ));
    store
        .append(&[original, reading("b", 20, 30)])
        .await
        .unwrap();

    let mut wrong_scope = change.clone();
    wrong_scope.version = ScopedVersion::new(
        VersionScope::for_interval("9910000000010", D20, Sparte::Strom).unwrap(),
        change.version.version(),
    );
    let wrong = a.correct_if_current(&wrong_scope).await.unwrap_err();
    assert!(
        matches!(wrong, meterstore::Error::PredecessorMismatch { actual: Some(ref actual), .. } if actual == &change.version)
    );
    assert!(b.correct_if_current(&change).await.is_err());
    let mut empty = change.clone();
    empty.series.intervals.clear();
    assert!(a.correct_if_current(&empty).await.is_err());
    let mut multiple = change.clone();
    multiple
        .series
        .intervals
        .push(change.series.intervals[0].clone());
    assert!(a.correct_if_current(&multiple).await.is_err());

    for archived in [false, true] {
        if archived {
            store.admin().archive(D21 + Duration::DAY, 4).await.unwrap();
            assert!(store.watermark().await.unwrap().get() > D20);
        }
        let deliveries = a
            .series(change.series.malo_id)
            .unwrap()
            .range(D20, D21)
            .collect_deliveries()
            .await
            .unwrap();
        let [observed] = deliveries.as_slice() else {
            panic!("one slot")
        };
        assert_eq!(observed.version, change.version);
        let expected = observed.version.clone();
        let next = expected.next().unwrap();
        let other = store.scoped("tenant", "a").await.unwrap();
        let (left, right) = tokio::join!(
            a.correct_if_current(&change),
            other.correct_if_current(&change)
        );
        let (success, failure) = match (left, right) {
            (Ok(success), Err(failure)) | (Err(failure), Ok(success)) => (success, failure),
            pair => panic!("exactly one correction must succeed: {pair:?}"),
        };
        assert!(!failure.is_retryable());
        assert!(
            matches!(failure, meterstore::Error::PredecessorMismatch { expected: ref exp, actual: Some(ref actual) } if exp == &expected && actual == &next)
        );
        assert_eq!(success.effect, meterstore::session::Effect::Superseded);
        assert_eq!(success.superseded.unwrap().version, expected);
        assert_eq!(success.written.version, next);
        assert_eq!(success.written.recorded_at, change.recorded_at);
        assert_eq!(success.identity, [("tenant".to_string(), "a".to_string())]);
        let independent = reading("a", 70, next.version().get() + 2);
        a.append(std::slice::from_ref(&independent)).await.unwrap();
        change.version = next;
        let stale = a.correct_if_current(&change).await.unwrap_err();
        assert!(
            matches!(stale, meterstore::Error::PredecessorMismatch { actual: Some(ref actual), .. } if actual == &independent.version)
        );
        change.version = independent.version;
        change.recorded_at += Duration::HOUR;
    }
    assert_eq!(count(&b, "SELECT COUNT(*) FROM readings_versions").await, 1);
    assert_eq!(count(&a, "SELECT COUNT(*) FROM readings_versions").await, 5);
}
