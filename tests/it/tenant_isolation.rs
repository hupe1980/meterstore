//! Identity columns, end to end against real PostgreSQL and Iceberg.
//!
//! A tenant discriminator has to make two readings *different readings*. If it
//! were an attribute rather than part of the identity, two tenants reporting the
//! same measuring point would share a merge key and one tenant's correction
//! would supersede the other's reading — a cross-tenant data leak with no error
//! anywhere.

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
use iceberg_storage_opendal::OpenDalStorageFactory;
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const TABLE: &str = "readings_versions";
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);

/// A store whose readings are identified by tenant as well as measuring point.
async fn store_with_tenant_identity() -> (
    MeterStore,
    tempfile::TempDir,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default().start().await.expect("postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");

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
        .settlement_lag(Duration::days(1))
        .identity_column(Field::new("tenant", DataType::Utf8, false))
        // A coded attribute column: its vocabulary is enforced by a DB CHECK.
        .attribute_column(meterstore::coded_column(
            "bilanzkreis",
            &["BK-1", "BK-2"],
            true,
        ))
        .build()
        .expect("config");

    // One call creates both tiers with a consistent key and schema.
    let hot_dyn: Arc<dyn HotStore> = hot.clone();
    let cold_dyn: Arc<dyn ColdStore> = cold.clone();
    hot_dyn
        .create_tables(TABLE, &config.merge_key(), &config.extra_columns())
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
        .build()
        .await
        .expect("store");

    (store, warehouse, container)
}

/// A reading for one tenant at a given version and value.
fn reading(tenant: &str, kwh: i64, version: u128) -> StoredSeries {
    let interval = metering::interval::MeterInterval {
        from: D20,
        to: D20 + Duration::minutes(15),
        value_kwh: rust_decimal::Decimal::new(kwh, 0),
        quality: metering::QualityFlag::Measured,
        obis_code: "1-0:1.8.0".parse().ok(),
    };
    let series = MeasurementSeries::new(
        "11111111111",
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
            VersionScope::for_interval("99", D20).unwrap(),
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
async fn one_tenants_correction_does_not_supersede_anothers_reading() {
    let (store, _w, _c) = store_with_tenant_identity().await;

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
    let (store, _w, _c) = store_with_tenant_identity().await;

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
    let (store, _w, _c) = store_with_tenant_identity().await;

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
    let (store, _w, _c) = store_with_tenant_identity().await;
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
    // The typed read path used to drop everything the `MeasurementSeries` could
    // not carry — commodity aside — so a caller reconstructing domain rows had to
    // hard-code the provenance back. `collect_resolved` folds the declared extra
    // columns from the newest contributing delivery and hands them back, so the
    // round-trip preserves them instead of guessing.
    let (store, _w, _c) = store_with_tenant_identity().await;
    store
        .append(&[reading("a", 10, 20_260_720_000_001)])
        .await
        .unwrap();

    let resolved = store
        .series("11111111111")
        .column_eq("tenant", ScalarValue::Utf8(Some("a".to_string())))
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
async fn a_missing_identity_value_is_rejected_rather_than_defaulted() {
    // Silently writing a null tenant would merge readings across tenants.
    let (store, _w, _c) = store_with_tenant_identity().await;

    let mut incomplete = reading("a", 10, 20_260_720_000_001);
    incomplete.extra.remove("tenant");

    assert!(
        store.append(&[incomplete]).await.is_err(),
        "a declared identity column must have a value"
    );
}

#[tokio::test]
async fn identity_columns_survive_archival_into_the_cold_tier() {
    // §20.2 claimed extra columns were threaded end to end — encoder, both
    // tables, the `ON CONFLICT` target, the scan projection and the resolution
    // `PARTITION BY`. The **archival** scan was missed, and it is the one path
    // that carries them from PostgreSQL into Iceberg. A tenant discriminator
    // that vanishes on archival makes every historical row unattributable, and
    // merges two tenants' history into one merge key.
    let (store, _w, _c) = store_with_tenant_identity().await;

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
        .cold_store()
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            meterstore::watermark::ArchivalWindow::new(D20 - Duration::DAY, D20).unwrap(),
        )
        .await
        .expect("seed watermark");

    store
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
    let (store, _w, _c) = store_with_tenant_identity().await;

    for tenant in ["9900000000001", "9900000000002"] {
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
    let (store, _w, _c) = store_with_tenant_identity().await;

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
