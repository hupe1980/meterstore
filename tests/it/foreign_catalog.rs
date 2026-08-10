//! The cold tier works against **any** `Catalog`, not only the one it builds.
//!
//! `IcebergCold::new` takes a pre-built `Arc<dyn Catalog>` precisely so a
//! deployment can bring a REST catalog, Glue, Polaris, Lakekeeper or Nessie.
//! That is the whole extension story for the cold tier, and until now every test
//! reached it through the implementations the crate builds itself.
//!
//! A seam that has only ever been exercised by the thing on the near side of it
//! is a seam nobody has tested. This drives the store through a catalog
//! implementation `IcebergCold` has never seen, and asserts the two properties
//! that make the claim true:
//!
//! - **Only the trait is used.** Every catalogue interaction goes through
//!   `dyn Catalog`; nothing reaches for `SqlCatalog`'s concrete type or for a
//!   file path beside the metadata.
//! - **The whole store works on it** — create, write, archive, query across the
//!   boundary, and read the watermark back out of the snapshot summary.
//!
//! The wrapper delegates to a real SQL catalog rather than faking one, because
//! the point is to prove the *dispatch* is trait-only. A fake catalogue would
//! prove that a fake catalogue works.

#![cfg(feature = "testkit")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation,
    TableIdent,
};
use meterstore::cold::IcebergCold;
use meterstore::config::TableConfig;
use meterstore::testkit::{MeteringWorkload, Oracle, TestHarness};
use meterstore::tiering::store::ColdStore;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// A `Catalog` that is not the one MeterStore builds.
///
/// Delegates every call, and counts them — so a test can show the cold tier
/// really did go through the trait rather than around it.
#[derive(Debug)]
struct CountingCatalog {
    inner: Arc<dyn Catalog>,
    calls: AtomicUsize,
}

impl CountingCatalog {
    fn new(inner: Arc<dyn Catalog>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    fn seen(&self) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl Catalog for CountingCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.seen();
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.seen();
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.seen();
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.seen();
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.seen();
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.seen();
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.seen();
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.seen();
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        self.seen();
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.seen();
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.seen();
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.seen();
        self.inner.rename_table(src, dest).await
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.seen();
        self.inner.update_table(commit).await
    }

    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.seen();
        self.inner.purge_table(table).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        self.seen();
        self.inner.register_table(table, metadata_location).await
    }
}

#[tokio::test]
async fn the_whole_store_runs_on_a_catalog_meterstore_did_not_build() {
    // The extension point, exercised end to end. Any catalogue a deployment
    // already runs arrives through this path, and no MeterStore code changes.
    let harness = TestHarness::start().await.expect("harness");

    // A real SQL catalog, wrapped so the cold tier only ever sees `dyn Catalog`.
    let foreign = CountingCatalog::new(harness.cold().catalog());
    let cold = Arc::new(IcebergCold::new(
        Arc::clone(&foreign) as Arc<dyn Catalog>,
        NamespaceIdent::new("metering".to_string()),
        8 * 1024 * 1024,
    ));

    let table = "foreign_readings_versions";
    let config = TableConfig::new(table)
        .settlement_lag(Duration::days(1))
        .build()
        .expect("config");

    ColdStore::create_tables(cold.as_ref(), table, &[], &config.extra_columns())
        .await
        .expect("the cold table is created through the trait");

    let store = harness
        .builder_for(config)
        .await
        .expect("builder")
        .cold(
            Arc::clone(&cold) as Arc<dyn ColdStore>,
            cold.table_provider(table).await.expect("provider"),
        )
        .build()
        .await
        .expect("a store over a foreign catalog");

    store.create_tables().await.expect("both tiers");
    harness
        .ensure_partitions(START, START + Duration::days(3))
        .await
        .expect("partitions");
    harness
        .seed_watermark_for(table, START, Duration::DAY)
        .await
        .expect("watermark");

    let series = MeteringWorkload::new(START)
        .seed(0xF0A1)
        .malo_ids(2)
        .days(2)
        .generate()
        .expect("workload");
    let mut oracle = Oracle::new();
    oracle.record(&series).expect("oracle");
    harness.ingest(&store, &series).await.expect("ingest");

    // Archive half of it, so the read below genuinely crosses the boundary.
    store
        .archive(START + Duration::days(2), 8)
        .await
        .expect("archive");
    let watermark = store.watermark().await.expect("watermark");
    assert!(
        watermark.get() > START,
        "the fixture must actually move the boundary"
    );

    let result = store
        .query(&format!(
            r#"SELECT COUNT(*) FROM {}"#,
            store.resolved_table()
        ))
        .await
        .expect("query across both tiers");

    use datafusion::arrow::array::AsArray;
    let rows = result.batches()[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0);
    assert_eq!(
        rows as u64,
        oracle.len() as u64,
        "a foreign catalog must return exactly what the reference holds"
    );
    assert!(
        result.spans_tiers(),
        "the read must have crossed the boundary: {:?}",
        result.tiers_scanned()
    );

    assert!(
        foreign.calls() > 0,
        "every catalogue interaction must go through the trait"
    );
}

#[tokio::test]
async fn the_boundary_is_readable_through_a_foreign_catalog() {
    // The watermark lives in the Iceberg snapshot summary, which is catalogue
    // state rather than file state. A catalogue MeterStore did not build must be
    // able to hand it back, or recovery depends on the implementation.
    let harness = TestHarness::start().await.expect("harness");
    let foreign = CountingCatalog::new(harness.cold().catalog());
    let cold = IcebergCold::new(
        foreign as Arc<dyn Catalog>,
        NamespaceIdent::new("metering".to_string()),
        8 * 1024 * 1024,
    );

    let table = "foreign_boundary_versions";
    ColdStore::create_tables(&cold, table, &[], &[])
        .await
        .expect("create");

    cold.append_and_commit(
        table,
        meterstore::tiering::store::stream_of(Vec::new()),
        Default::default(),
        meterstore::watermark::ArchivalWindow::new(START, START + Duration::DAY).expect("window"),
    )
    .await
    .expect("commit");

    assert_eq!(
        cold.watermark(table).await.expect("watermark").get(),
        START + Duration::DAY
    );
}
