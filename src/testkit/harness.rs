//! A real PostgreSQL and a real Iceberg warehouse, in one call.
//!
//! Every integration suite in this repository used to open with the same eighty
//! lines: start a container, connect a pool, build a temporary warehouse, load a
//! SQL catalog, create both tiers, assemble a store. That is not incidental
//! duplication — it is the setup a *deployment* has to get right too, and eighty
//! lines copied six times is six places for it to drift.
//!
//! # Why the fixtures are real
//!
//! §17 draws the line at storage fidelity: the things worth testing here are the
//! ones a fake cannot fail at. Catalog compare-and-swap, advisory-lock scope,
//! partition detach visibility, Parquet round-tripping of a `Decimal128` — a
//! mock agrees with whatever the code does. So the harness starts the real
//! thing, and the cost is a container per suite.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use sqlx::PgPool;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use time::{Duration, OffsetDateTime};

use crate::cold::IcebergCold;
use crate::config::{TableConfig, ValidatedTableConfig};
use crate::error::Result;
use crate::hot::PostgresHot;
use crate::session::MeterStore;
use crate::tiering::store::{ColdStore, HotStore};
use crate::watermark::ArchivalWindow;

/// Both tiers, running, with a table already created.
///
/// Holds the container and the warehouse directory, so both live exactly as long
/// as the harness does — a dropped `TempDir` takes the Iceberg data with it, and
/// a test that let it go early would fail somewhere unrelated.
pub struct TestHarness {
    hot: Arc<PostgresHot>,
    cold: Arc<IcebergCold>,
    config: ValidatedTableConfig,
    url: String,
    _warehouse: tempfile::TempDir,
    _container: testcontainers::ContainerAsync<Postgres>,
}

impl std::fmt::Debug for TestHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestHarness")
            .field("table", &self.config.name())
            .finish_non_exhaustive()
    }
}

impl TestHarness {
    /// The PostgreSQL image every suite runs against.
    ///
    /// **Pinned, and not at the library default.** `testcontainers-modules`
    /// defaults to `11-alpine`, which is four majors below the ≥ 14 this design
    /// requires (§3.1) — so every suite would have been proving the store works
    /// on a version it does not claim to support, and would have missed anything
    /// that needs a newer one. 16 matches the §18 reference deployment.
    pub const POSTGRES_IMAGE_TAG: &'static str = "16-alpine";

    /// The table name the harness creates.
    ///
    /// Carries the `_versions` suffix, because that is the physical name: the
    /// table holds every version of every reading, and the resolved view is what
    /// queries should use (§13.7.2).
    pub const TABLE: &'static str = "readings_versions";

    /// Start both tiers with the default table configuration.
    pub async fn start() -> Result<Self> {
        Self::with_config(
            TableConfig::new(Self::TABLE)
                .settlement_lag(Duration::DAY)
                .build()?,
        )
        .await
    }

    /// Start both tiers with a specific table configuration.
    pub async fn with_config(config: ValidatedTableConfig) -> Result<Self> {
        let container = Postgres::default()
            .with_tag(Self::POSTGRES_IMAGE_TAG)
            .start()
            .await
            .map_err(|e| crate::Error::Storage(format!("starting postgres: {e}")))?;
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .map_err(|e| crate::Error::Storage(format!("mapping postgres port: {e}")))?;
        let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");

        let pool = PgPool::connect(&url)
            .await
            .map_err(|e| crate::Error::Storage(e.to_string()))?;
        let hot = Arc::new(PostgresHot::new(pool));

        let warehouse = tempfile::tempdir()
            .map_err(|e| crate::Error::Storage(format!("temp warehouse: {e}")))?;
        let catalog = SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::Fs))
            .load(
                "meterstore",
                HashMap::from([
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
            .map_err(|e| crate::Error::Storage(format!("sql catalog: {e}")))?;

        let cold = Arc::new(IcebergCold::new(
            Arc::new(catalog) as Arc<dyn Catalog>,
            NamespaceIdent::new("metering".to_string()),
            8 * 1024 * 1024,
        ));

        let harness = Self {
            hot,
            cold,
            config,
            url,
            _warehouse: warehouse,
            _container: container,
        };

        // One entry point, exactly as a deployment should use (§7.3): the hot
        // table's primary key, the cold schema and the resolution `PARTITION BY`
        // all have to agree, and creating them separately is where they drift.
        harness
            .hot
            .create_tables(
                harness.config.name(),
                &harness.config.merge_key(),
                &harness.config.extra_columns(),
            )
            .await?;
        harness
            .cold
            .create_tables(
                harness.config.name(),
                &harness.config.identity_column_names(),
                &harness.config.extra_columns(),
            )
            .await?;

        Ok(harness)
    }

    /// The hot tier.
    pub fn hot(&self) -> &Arc<PostgresHot> {
        &self.hot
    }

    /// The cold tier.
    pub fn cold(&self) -> &Arc<IcebergCold> {
        &self.cold
    }

    /// The table configuration both tiers were created with.
    pub fn config(&self) -> &ValidatedTableConfig {
        &self.config
    }

    /// The PostgreSQL connection URL, for a second pool or a raw statement.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The warehouse root on disk.
    ///
    /// Exposed so a test can open the written files with something **other than**
    /// this crate — which is the only way to check the claim in P2 that the
    /// output is standard rather than merely self-consistent.
    pub fn warehouse(&self) -> &std::path::Path {
        self._warehouse.path()
    }

    /// Every Parquet data file the cold tier has written, sorted by path.
    ///
    /// Found by walking the warehouse rather than by reading the manifests, on
    /// purpose: reading them through Iceberg would be asking this crate's
    /// dependency whether this crate's dependency can read its own output.
    pub fn parquet_files(&self) -> Vec<std::path::PathBuf> {
        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().is_some_and(|e| e == "parquet") {
                    out.push(path);
                }
            }
        }

        let mut out = Vec::new();
        walk(self._warehouse.path(), &mut out);
        out.sort();
        out
    }

    /// A store over both tiers.
    pub async fn store(&self) -> Result<MeterStore> {
        self.builder_for(self.config.clone()).await?.build().await
    }

    /// A builder for **another** table over the same two tiers.
    ///
    /// A deployment that holds a second stream — non-authoritative ESA values
    /// beside billing readings, say — needs two tables against one PostgreSQL
    /// and one Iceberg catalog. Feed the builders to
    /// [`MeterCatalog`](crate::MeterCatalog) to get them in one session.
    ///
    /// The table's storage is not created here; call
    /// [`MeterStore::create_tables`] or the catalog's equivalent.
    pub async fn builder_for(
        &self,
        config: ValidatedTableConfig,
    ) -> Result<crate::MeterStoreBuilder> {
        // Created first because the provider needs a table to load a schema from.
        self.cold
            .create_table_with(
                config.name(),
                &config.extra_columns(),
                &config.identity_column_names(),
            )
            .await?;

        Ok(MeterStore::builder()
            .hot(Arc::clone(&self.hot) as Arc<dyn HotStore>)
            .cold(
                Arc::clone(&self.cold) as Arc<dyn ColdStore>,
                self.cold.table_provider(config.name()).await?,
            )
            .table(config))
    }

    /// Make sure the hot tier can hold `[from, to)`.
    pub async fn ensure_partitions(&self, from: OffsetDateTime, to: OffsetDateTime) -> Result<()> {
        self.hot
            .ensure_partitions(self.config.name(), from, to, self.config.partition_step())
            .await
            .map(|_| ())
    }

    /// Place the tier boundary at `at` without archiving anything.
    ///
    /// Archival starts from the watermark, so a store that has never archived
    /// begins at the Unix epoch and spends its window budget on empty 1970 days.
    /// Seeding is how a test says "pretend everything before this is settled"
    /// without generating six decades of nothing.
    pub async fn seed_watermark(&self, at: OffsetDateTime) -> Result<()> {
        self.seed_watermark_for(self.config.name(), at, self.config.archival_step())
            .await
    }

    /// Place another table's boundary, for a harness hosting more than one.
    ///
    /// Without it a second table starts at the epoch, and its first archival run
    /// walks fifty years of empty windows before reaching any data — which is
    /// correct and takes long enough to look like a hang.
    pub async fn seed_watermark_for(
        &self,
        table: &str,
        at: OffsetDateTime,
        step: time::Duration,
    ) -> Result<()> {
        self.cold
            .append_and_commit(
                table,
                crate::tiering::store::stream_of(Vec::new()),
                crate::tiering::store::WriteHints::default(),
                ArchivalWindow::new(at - step, at)?,
            )
            .await
            .map(|_| ())
    }

    /// Write a workload's series through the store, tier-routed.
    ///
    /// Goes through [`MeterStore::append`] rather than raw SQL so the routing
    /// rule — a correction below the watermark goes to Iceberg, not PostgreSQL —
    /// is exercised rather than bypassed.
    pub async fn ingest(
        &self,
        store: &MeterStore,
        series: &[crate::encode::StoredSeries],
    ) -> Result<()> {
        for delivery in series {
            store.append(std::slice::from_ref(delivery)).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pinned_image_meets_the_documented_minimum() {
        // §3.1 requires PostgreSQL ≥ 14. The container library's default is
        // 11-alpine, so this has to be set rather than inherited.
        let major: u32 = TestHarness::POSTGRES_IMAGE_TAG
            .split('-')
            .next()
            .and_then(|m| m.parse().ok())
            .expect("the tag must start with a major version");
        assert!(major >= 14, "§3.1 requires PostgreSQL 14 or newer");
    }

    #[test]
    fn the_table_name_announces_that_it_holds_versions() {
        // The name is load-bearing (§13.7.2): the relation that looks like the
        // obvious thing to query must not be the one that double-counts.
        assert!(TestHarness::TABLE.ends_with("_versions"));
    }
}
