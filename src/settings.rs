//! The TOML front end over the same validated configuration.
//!
//! Everything here produces a [`ValidatedTableConfig`](crate::config::ValidatedTableConfig) — the builder's own output
//! type — so a deployment configured from a file and one configured in Rust pass
//! through exactly the same cross-field checks (§14). There is no second
//! validation path to keep in step, and no setting reachable from one and not the
//! other.
//!
//! # Why the file cannot construct the tiers
//!
//! It names them; it does not open them. A `hot.url` becomes a
//! [`HotSettings`] the application uses to build its own `PgPool`, because the
//! pool is usually shared with the rest of the service and MeterStore never owns
//! a connection (P3, §13.5). The same goes for the catalog: the file records
//! which one and where, and the application constructs it.
//!
//! # Environment interpolation
//!
//! `${VAR}` in any string is replaced from the environment, and a missing
//! variable is an error rather than an empty string. A connection URL that
//! silently became `postgresql://@/` would fail somewhere far from the typo.
//!
//! ```toml
//! [hot]
//! url = "${DATABASE_URL}"
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::Duration;

use crate::arrow::datatypes::{DataType, Field};
use crate::config::TableConfig;
use crate::error::{Error, Result};

/// A whole deployment's configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The hot tier's connection.
    #[serde(default)]
    pub hot: HotSettings,
    /// The cold tier's catalog and warehouse.
    #[serde(default)]
    pub cold: ColdSettings,
    /// One entry per managed table.
    #[serde(default)]
    pub tables: Vec<TableSettings>,
}

impl Settings {
    /// Parse TOML, interpolating `${VAR}` from the environment.
    pub fn from_toml(text: &str) -> Result<Self> {
        let interpolated = interpolate(text)?;
        toml::from_str(&interpolated)
            .map_err(|e| Error::config(format!("invalid meterstore configuration: {e}")))
    }

    /// Read and parse a configuration file.
    pub fn from_path(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::config(format!("cannot read {}: {e}", path.display())))?;
        Self::from_toml(&text)
    }

    /// Render back to TOML.
    ///
    /// Round-trips, so a deployment can normalise a hand-written file — and so a
    /// test can assert the parse did not quietly drop a setting.
    pub fn to_toml(&self) -> Result<String> {
        toml::to_string_pretty(self)
            .map_err(|e| Error::config(format!("cannot render configuration: {e}")))
    }

    /// Validate every table, in declaration order.
    ///
    /// Runs the full cross-field validation, so a file whose `settlement_lag`
    /// is shorter than its `archival_step` fails here rather than stranding
    /// corrections below the watermark in production (§8.1).
    pub fn validate(&self) -> Result<Vec<crate::config::ValidatedTableConfig>> {
        if self.tables.is_empty() {
            return Err(Error::config(
                "no [[tables]] declared: a store with no table has nothing to archive or query",
            ));
        }
        self.tables.iter().map(TableSettings::validate).collect()
    }

    /// Validate the tables **and** the `[hot]` and `[cold]` sections.
    ///
    /// [`validate`](Self::validate) checks only the tables, which is what a
    /// caller wiring the tiers by hand needs; this is what
    /// [`connect`](Self::connect) needs, and runs first.
    ///
    /// Separate rather than folded in, because a deployment may legitimately
    /// bring its own catalogue (`IcebergCold` takes any `Arc<dyn Catalog>`) and
    /// leave `[cold]` empty. Insisting on it in `validate` would refuse a file
    /// that is complete for the way it is used.
    pub fn validate_all(&self) -> Result<Vec<crate::config::ValidatedTableConfig>> {
        self.hot.validate()?;
        self.cold.validate()?;
        self.validate()
    }

    /// Build both tiers and every table from the file.
    ///
    /// Returns the connection pool as well, because an application almost always
    /// has its own use for it — the subject registry takes one, and so does
    /// whatever else the service keeps in the same database.
    ///
    /// # It stops at the tiers
    ///
    /// A [`MeterStore`](crate::MeterStore) needs a cold *table provider*, which
    /// needs the cold table to exist — and whether a deployment wants one store
    /// or a [`MeterCatalog`](crate::MeterCatalog) over all its tables is not
    /// something a configuration file decides. [`Deployment::store`] and
    /// [`Deployment::catalog`] are those two answers; this is what they are both
    /// built from, and what a deployment assembling its own takes instead.
    pub async fn connect(&self) -> Result<Deployment> {
        let tables = self.validate_all()?;
        let pool = self.hot.connect().await?;
        let cold = self.cold.build().await?;
        Ok(Deployment {
            hot: std::sync::Arc::new(self.hot.hot(pool.clone())),
            pool,
            cold,
            tables,
        })
    }

    /// The single table's validated configuration.
    ///
    /// Convenience for the common deployment, which manages one table. Errors
    /// when the file declares several, rather than silently picking the first.
    pub fn single_table(&self) -> Result<crate::config::ValidatedTableConfig> {
        match self.tables.as_slice() {
            [one] => one.validate(),
            [] => Err(Error::config("no [[tables]] declared")),
            many => Err(Error::config(format!(
                "{} tables declared; use Settings::validate to get all of them",
                many.len()
            ))),
        }
    }
}

/// How to reach PostgreSQL.
///
/// `Debug` is hand-written rather than derived: a connection URL carries a
/// password, and configuration is exactly what a service dumps into its startup
/// log (§19.7).
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HotSettings {
    /// Connection URL. `${VAR}` is interpolated from the environment.
    ///
    /// Never logged: the `Debug` impl redacts it, because a connection URL
    /// carries a password and configuration is exactly what gets dumped into a
    /// startup log.
    #[serde(default)]
    pub url: String,
    /// Pool size.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// How long a DDL statement waits for a lock before giving up.
    ///
    /// Defaults to `3s`. See
    /// [`PostgresHot::ddl_lock_timeout`](crate::PostgresHot::ddl_lock_timeout)
    /// for why there is one at all: PostgreSQL grants locks in arrival order, so
    /// a statement that waits for an `ACCESS EXCLUSIVE` lock blocks every reader
    /// and writer behind it, and both of the paths that take one here run on a
    /// schedule nobody chose.
    ///
    /// `0s` disables it, which is PostgreSQL's own default and this crate's
    /// advice against.
    #[serde(default = "default_ddl_lock_timeout")]
    pub ddl_lock_timeout: HumanDuration,
}

impl HotSettings {
    /// Refuse a section that names no database.
    ///
    /// So that a missing `url` is a configuration error naming the setting,
    /// rather than sqlx's opinion of a relative URL at the first connection.
    pub fn validate(&self) -> Result<()> {
        if self.url.trim().is_empty() {
            return Err(Error::config(
                "[hot] url is empty: the hot tier is PostgreSQL and there is nothing to \
                 connect to. Set it, or use ${DATABASE_URL} to take it from the \
                 environment",
            ));
        }
        if self.max_connections == 0 {
            return Err(Error::config(
                "[hot] max_connections is 0: a pool that can hand out no connection \
                 blocks the first query for ever rather than failing",
            ));
        }
        Ok(())
    }

    /// Build the hot tier this section describes, over an existing pool.
    ///
    /// Separate from [`connect`](Self::connect) because a deployment almost
    /// always brings its own pool — MeterStore never owns a connection — and
    /// this is what carries the file's settings onto it.
    pub fn hot(&self, pool: sqlx::PgPool) -> crate::hot::PostgresHot {
        crate::hot::PostgresHot::new(pool).ddl_lock_timeout(self.ddl_lock_timeout.0)
    }

    /// Open the pool this section describes.
    pub async fn connect(&self) -> Result<sqlx::PgPool> {
        self.validate()?;
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(self.max_connections)
            .connect(&self.url)
            .await
            .map_err(|e| {
                Error::Storage(format!(
                    "connecting to {}: {e}",
                    crate::error::redacted(&self.url)
                ))
            })
    }
}

impl Default for HotSettings {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: default_max_connections(),
            ddl_lock_timeout: default_ddl_lock_timeout(),
        }
    }
}

impl std::fmt::Debug for HotSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HotSettings")
            .field("url", &crate::error::redacted(&self.url))
            .field("max_connections", &self.max_connections)
            .field("ddl_lock_timeout", &self.ddl_lock_timeout)
            .finish()
    }
}

/// Matches `PostgresHot`'s own default, which is where the reasoning is.
fn default_ddl_lock_timeout() -> HumanDuration {
    HumanDuration(Duration::seconds(3))
}

const fn default_max_connections() -> u32 {
    16
}

/// Which Iceberg catalog, and where the warehouse lives.
///
/// `Debug` is hand-written for the same reason [`HotSettings`]'s is: a SQL
/// catalogue's [`uri`](Self::uri) is a PostgreSQL connection URL and therefore
/// carries a password — usually the hot tier's own, since the recommended
/// deployment puts the catalogue on the same database.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColdSettings {
    /// `rest` or `sql`.
    #[serde(default)]
    pub catalog: CatalogKind,
    /// Catalog endpoint (REST) or connection URL (SQL).
    ///
    /// Never logged in full: the `Debug` impl redacts it, because the SQL form
    /// is a connection URL and carries a password.
    #[serde(default)]
    pub uri: String,
    /// Warehouse root — `s3://…`, `gs://…`, or a local path.
    #[serde(default)]
    pub warehouse: String,
    /// Namespace the tables live in.
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// Target Parquet data-file size, in bytes.
    ///
    /// A cold-tier setting rather than a table one: it belongs to the object the
    /// Parquet writer lives in.
    #[serde(default = "default_file_target_bytes")]
    pub file_target_bytes: usize,
    /// Upper bound on the SQL catalogue's own metadata connection pool.
    ///
    /// Ignored by a REST catalogue, which opens no database of its own.
    #[serde(default = "default_metadata_pool")]
    pub metadata_pool_max_connections: u32,
    /// Object-store region, for an S3-family warehouse.
    ///
    /// Only the **non-secret** half of [`WarehouseAuth`] is expressible here.
    /// Keys are deliberately absent: the platform credential chain — environment,
    /// instance role, IRSA — is the recommended path, and a secret in a
    /// configuration file is a secret in a log. A deployment that genuinely needs
    /// explicit keys builds the tier from [`IcebergSqlCatalog`] directly.
    ///
    /// [`WarehouseAuth`]: crate::cold::WarehouseAuth
    /// [`IcebergSqlCatalog`]: crate::cold::IcebergSqlCatalog
    #[serde(default)]
    pub region: Option<String>,
    /// S3-compatible endpoint override — MinIO, Ceph, R2, LocalStack.
    #[serde(default)]
    pub endpoint: Option<String>,
}

impl std::fmt::Debug for ColdSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdSettings")
            .field("catalog", &self.catalog)
            .field("uri", &crate::error::redacted(&self.uri))
            // The warehouse names a bucket rather than carrying a credential, so
            // it stays legible — it is the field an operator reads this for.
            .field("warehouse", &self.warehouse)
            .field("namespace", &self.namespace)
            .field("file_target_bytes", &self.file_target_bytes)
            .field(
                "metadata_pool_max_connections",
                &self.metadata_pool_max_connections,
            )
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

const fn default_file_target_bytes() -> usize {
    crate::config::defaults::TARGET_FILE_SIZE
}

const fn default_metadata_pool() -> u32 {
    4
}

fn default_namespace() -> String {
    "metering".to_string()
}

impl ColdSettings {
    /// Refuse a section that cannot build a catalogue.
    ///
    /// Each refusal names the setting, so a `[cold]` naming no warehouse — or a
    /// scheme this build did not compile in — fails here rather than as an
    /// obscure failure at the first commit.
    pub fn validate(&self) -> Result<()> {
        if self.uri.trim().is_empty() {
            return Err(Error::config(match self.catalog {
                CatalogKind::Rest => {
                    "[cold] uri is empty: a REST catalog is reached by its \
                     endpoint and there is nothing to reach"
                }
                CatalogKind::Sql => {
                    "[cold] uri is empty: a SQL catalog keeps its metadata in \
                     PostgreSQL and there is no database named. It is normally the same URL \
                     as [hot] url"
                }
            }));
        }
        if self.warehouse.trim().is_empty() {
            return Err(Error::config(
                "[cold] warehouse is empty: the catalog holds metadata, and the data files \
                 need somewhere to live — file://, memory://, s3://, gs:// or abfss://",
            ));
        }
        if self.namespace.trim().is_empty() {
            return Err(Error::config("[cold] namespace must not be empty"));
        }
        if self.file_target_bytes == 0 {
            return Err(Error::config(
                "[cold] file_target_bytes is 0: the writer would roll a file per row",
            ));
        }
        if self.metadata_pool_max_connections == 0 && self.catalog == CatalogKind::Sql {
            return Err(Error::config(
                "[cold] metadata_pool_max_connections is 0: the SQL catalog could open no \
                 connection and every table load would block",
            ));
        }
        // The scheme decides the object-store backend, and a backend whose
        // feature was not compiled in is a configuration error rather than a
        // silent fallback to local disk. Asked here so it fails at validation
        // rather than at the first commit.
        crate::cold::catalog::warehouse_factory(&self.warehouse)?;
        Ok(())
    }

    /// Build the cold tier this section describes.
    pub async fn build(&self) -> Result<crate::cold::ColdTier> {
        self.validate()?;
        match self.catalog {
            CatalogKind::Sql => {
                crate::cold::IcebergSqlCatalog {
                    database_url: &self.uri,
                    warehouse_uri: &self.warehouse,
                    catalog_name: "meterstore",
                    namespace: &self.namespace,
                    file_target_bytes: self.file_target_bytes,
                    metadata_pool_max_connections: self.metadata_pool_max_connections,
                    auth: &crate::cold::WarehouseAuth {
                        region: self.region.clone(),
                        endpoint: self.endpoint.clone(),
                        // Deliberately never from a file — see `region`.
                        access_key_id: None,
                        secret_access_key: None,
                    },
                }
                .build()
                .await
            }
            #[cfg(feature = "rest-catalog")]
            CatalogKind::Rest => {
                crate::cold::IcebergRestCatalog {
                    uri: &self.uri,
                    warehouse_uri: &self.warehouse,
                    namespace: &self.namespace,
                    file_target_bytes: self.file_target_bytes,
                    props: std::collections::HashMap::new(),
                }
                .build()
                .await
            }
            #[cfg(not(feature = "rest-catalog"))]
            CatalogKind::Rest => Err(Error::config(
                "[cold] catalog = \"rest\" needs the meterstore `rest-catalog` feature, \
                 which was not compiled in",
            )),
        }
    }
}

/// Both tiers and every table, built from one configuration file.
///
/// What [`Settings::connect`] returns. Deliberately not a
/// [`MeterStore`](crate::MeterStore): that needs a cold table provider, which
/// needs the table to exist, and a deployment with several tables wants a
/// [`MeterCatalog`](crate::MeterCatalog) rather than N stores.
pub struct Deployment {
    /// The hot tier, ready for `MeterStoreBuilder::hot`.
    pub hot: std::sync::Arc<crate::hot::PostgresHot>,
    /// The pool behind it, for the subject registry and the application's own
    /// tables — both of which belong in the same database.
    pub pool: sqlx::PgPool,
    /// The cold tier, and the catalogue façade over it.
    pub cold: crate::cold::ColdTier,
    /// Every table the file declares, validated, in declaration order.
    pub tables: Vec<crate::config::ValidatedTableConfig>,
}

impl Deployment {
    /// A builder for one declared table, with both tiers already wired.
    ///
    /// The cold table is created first, because a
    /// [`TableProvider`](datafusion::catalog::TableProvider) cannot be opened
    /// over a table the catalogue does not hold yet — which is the one step that
    /// makes this more than field access, and the reason
    /// [`connect`](Settings::connect) stops short of it.
    ///
    /// Returned as a builder rather than a store so a caller can still add what
    /// a file cannot name: a [`SubjectRegistry`](crate::SubjectRegistry), a
    /// [`ReadMode`](crate::ReadMode), a session it already owns.
    pub async fn table(
        &self,
        config: crate::config::ValidatedTableConfig,
    ) -> Result<crate::MeterStoreBuilder> {
        use crate::tiering::ColdStore as _;

        let cold = self.cold.cold();
        // **With the declared columns, never bare.** The bare constructor would
        // create the table with the core schema and the default partition spec,
        // and a deployment declaring identity columns would then find its own
        // `create_tables` refusing the table this call had just made for it: the
        // identity columns are the *leading partition fields*, so getting them
        // wrong is not a difference a later call can reconcile.
        cold.create_tables(
            config.name(),
            &config.identity_column_names(),
            &config.extra_columns(),
        )
        .await?;
        let provider = cold.table_provider(config.name()).await?;
        Ok(crate::MeterStore::builder()
            .hot(self.hot.clone() as std::sync::Arc<dyn crate::HotStore>)
            .cold(cold as std::sync::Arc<dyn crate::ColdStore>, provider)
            .table(config))
    }

    /// Build a store over the **one** table the file declares.
    ///
    /// Errors when it declares several, rather than silently picking one — use
    /// [`catalog`](Self::catalog) for those, which is what makes a statement able
    /// to mention both.
    ///
    /// Creates the hot and cold tables as a side effect, exactly as
    /// [`MeterStore::create_tables`](crate::MeterStore::create_tables) does, so
    /// a fresh deployment is one call from usable.
    pub async fn store(&self) -> Result<crate::MeterStore> {
        let config = match self.tables.as_slice() {
            [one] => one.clone(),
            [] => return Err(Error::config("no [[tables]] declared")),
            many => {
                return Err(Error::config(format!(
                    "{} tables are declared ({}), so there is no single store to build. \
                     Use Deployment::catalog, which puts them in one session and lets a \
                     statement mention more than one",
                    many.len(),
                    many.iter()
                        .map(crate::config::ValidatedTableConfig::name)
                        .collect::<Vec<_>>()
                        .join(", "),
                )));
            }
        };
        let store = self.table(config).await?.build().await?;
        store.create_tables().await?;
        Ok(store)
    }

    /// Build a catalog over **every** table the file declares.
    ///
    /// One DataFusion session across all of them, so a statement can join two
    /// streams — and one maintenance schedule, while each table keeps its own
    /// watermark, archiver and lease.
    ///
    /// Creates every table's hot and cold relations as a side effect.
    pub async fn catalog(&self) -> Result<crate::MeterCatalog> {
        let mut builder = crate::MeterCatalog::builder();
        for config in &self.tables {
            builder = builder.table(self.table(config.clone()).await?);
        }
        let catalog = builder.build().await?;
        catalog.create_tables().await?;
        Ok(catalog)
    }
}

impl std::fmt::Debug for Deployment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deployment")
            .field(
                "tables",
                &self
                    .tables
                    .iter()
                    .map(crate::config::ValidatedTableConfig::name)
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// The catalog implementations this crate can drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogKind {
    /// Iceberg REST catalog. Every engine speaks it, so nothing extra is needed
    /// for external access (§13.7.1).
    #[default]
    Rest,
    /// PostgreSQL-backed SQL catalog. External engines need the JDBC catalog
    /// implementation, which support for is uneven.
    Sql,
}

/// One managed table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSettings {
    /// Physical table name.
    pub name: String,
    /// What a row's timestamps mean: `"interval"` (a Lastgang) or `"point"` (a
    /// Zählerstandsgang).
    ///
    /// Defaults to `"interval"`. The two are never one table: `value` is energy
    /// over a span on one and a cumulative register reading on the other, so
    /// summing them together produces a number with no meaning that looks
    /// exactly like a consumption total.
    #[serde(default)]
    pub time_model: crate::config::TimeModel,
    /// Whether the Messlokation is part of what identifies a reading.
    ///
    /// Omitted, it follows `time_model`: on for `"point"`, off for
    /// `"interval"`. A register belongs to a meter and a load profile belongs to
    /// a market location, and a Marktlokation may be measured by several
    /// Messlokationen — so setting this wrongly on a Zählerstandsgang means two
    /// meters' registers share a merge key.
    #[serde(default)]
    pub identify_by_melo: Option<bool>,
    /// Hot-tier layout.
    #[serde(default)]
    pub hot: TableHotSettings,
    /// Archival cadence and sizing.
    #[serde(default)]
    pub archival: ArchivalSettings,
    /// Snapshot retention.
    #[serde(default)]
    pub maintenance: MaintenanceSettings,
    /// Columns beyond the core schema (§7.3).
    ///
    /// The `identity` flag is the load-bearing one: an identity column joins the
    /// merge key, so two rows differing in it are different readings. A tenant
    /// discriminator declared as an attribute instead would let one tenant's
    /// correction supersede another's reading.
    #[serde(default)]
    pub extra_columns: Vec<ExtraColumn>,
    /// The column holding pseudonymous subject references (§19.4).
    #[serde(default)]
    pub subject_column: Option<String>,
}

impl TableSettings {
    /// Build and validate the table configuration.
    pub fn validate(&self) -> Result<crate::config::ValidatedTableConfig> {
        let mut config = TableConfig::new(&self.name)
            .time_model(self.time_model)
            .partition_headroom(self.hot.partition_headroom.0)
            .archival_step(self.archival.archival_step.0)
            .settlement_lag(self.archival.settlement_lag.0)
            .scan_chunk_rows(self.archival.scan_chunk_rows)
            .snapshot_retention(self.maintenance.snapshot_retention.0)
            .min_snapshots_to_keep(self.maintenance.min_snapshots_to_keep);

        if let Some(yes) = self.identify_by_melo {
            config = config.identify_by_melo(yes);
        }

        let mut seen = BTreeMap::new();
        for column in &self.extra_columns {
            seen.insert(column.name.clone(), column.identity);
            let field = column.field()?;
            config = if column.identity {
                config.identity_column(field)
            } else {
                config.attribute_column(field)
            };
        }

        if let Some(subject) = &self.subject_column {
            // Named here whether or not `extra_columns` also spells it out:
            // `TableConfig::subject_column` adopts an already-declared attribute
            // column rather than declaring a second one, so what this adds in
            // that case is the marker — and without the marker the write-path
            // check against the registry never runs.
            //
            // The identity case is caught here rather than by `build`, whose
            // message would be about a column declared twice rather than about
            // the merge key.
            if seen.get(subject) == Some(&true) {
                return Err(Error::config(format!(
                    "subject_column {subject:?} is also declared as an identity column: a \
                     pseudonymous reference must never join the merge key, or a correction \
                     derived from a re-registered reference silently fails to supersede the \
                     value it corrects (§19.4)"
                )));
            }
            config = config.subject_column(subject);
        }

        config.build()
    }
}

/// Hot-tier partitioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableHotSettings {
    /// How far ahead of the write frontier partitions are pre-created.
    #[serde(default = "default_headroom")]
    pub partition_headroom: HumanDuration,
}

impl Default for TableHotSettings {
    fn default() -> Self {
        Self {
            partition_headroom: default_headroom(),
        }
    }
}

fn default_headroom() -> HumanDuration {
    HumanDuration(crate::config::defaults::PARTITION_HEADROOM)
}

/// Archival cadence and sizing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivalSettings {
    /// How far behind wall clock archival stays. Must exceed the market's normal
    /// correction window, or a window closes while corrections are still arriving.
    #[serde(default = "default_settlement_lag")]
    pub settlement_lag: HumanDuration,
    /// Window size — **and the hot table's partition granularity**, which is the
    /// same number: a purge drops exactly one partition per archived window.
    #[serde(default = "default_archival_step")]
    pub archival_step: HumanDuration,
    /// Rows fetched per round trip when streaming a scan.
    ///
    /// The bound on archival's peak memory. Rows rather than measuring points:
    /// meters differ by orders of magnitude in how much they report, so a fixed
    /// number of *them* is a variable amount of memory.
    #[serde(default = "default_chunk")]
    pub scan_chunk_rows: usize,
}

impl Default for ArchivalSettings {
    fn default() -> Self {
        Self {
            settlement_lag: default_settlement_lag(),
            archival_step: default_archival_step(),
            scan_chunk_rows: default_chunk(),
        }
    }
}

fn default_settlement_lag() -> HumanDuration {
    HumanDuration(crate::config::defaults::SETTLEMENT_LAG)
}
fn default_archival_step() -> HumanDuration {
    HumanDuration(crate::config::defaults::ARCHIVAL_STEP)
}
const fn default_chunk() -> usize {
    crate::config::defaults::SCAN_CHUNK_ROWS
}

/// Snapshot retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceSettings {
    /// How long cold snapshots are kept.
    ///
    /// **Not a cleanup knob.** A snapshot is what makes a past settlement
    /// reproducible, so this decides how far back an audit can reach. The default
    /// is ten years rather than the days a general-purpose lakehouse would pick.
    #[serde(default = "default_retention")]
    pub snapshot_retention: HumanDuration,
    /// Snapshots kept regardless of age.
    #[serde(default = "default_min_snapshots")]
    pub min_snapshots_to_keep: usize,
}

impl Default for MaintenanceSettings {
    fn default() -> Self {
        Self {
            snapshot_retention: default_retention(),
            min_snapshots_to_keep: default_min_snapshots(),
        }
    }
}

fn default_retention() -> HumanDuration {
    HumanDuration(crate::config::defaults::SNAPSHOT_RETENTION)
}
const fn default_min_snapshots() -> usize {
    crate::config::defaults::MIN_SNAPSHOTS_TO_KEEP
}

/// A deployment column beyond the core schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtraColumn {
    /// Column name.
    pub name: String,
    /// Storage type. Only `string` is supported (§7.3).
    #[serde(default = "default_column_type")]
    pub r#type: String,
    /// Whether this column is part of a reading's **identity**.
    ///
    /// Identity columns join the merge key and are non-nullable. Get this wrong
    /// for a tenant discriminator and two tenants reporting the same measuring
    /// point share a merge key — a cross-tenant leak with no error anywhere.
    #[serde(default)]
    pub identity: bool,
    /// The fixed vocabulary this column accepts, if it has one.
    ///
    /// Renders a `CHECK … IN (…)` on the hot table, exactly as
    /// [`coded_column`](crate::config::coded_column) does from Rust — an
    /// ingestion source, a delivery status, a Zählzeit. A value outside the set
    /// fails the write rather than being read back later as an unknown code.
    ///
    /// Present because §14's whole claim for this file is that it is a front end
    /// over the *same* validated types, with no setting reachable from one and
    /// not the other.
    #[serde(default)]
    pub values: Option<Vec<String>>,
}

fn default_column_type() -> String {
    "string".to_string()
}

impl ExtraColumn {
    /// The declared column as a schema field, vocabulary and all.
    ///
    /// Identity columns are non-nullable by construction: a null cannot identify
    /// a reading, and in SQL it does not compare equal to itself.
    fn field(&self) -> Result<Field> {
        let nullable = !self.identity;
        let Some(values) = &self.values else {
            return Ok(Field::new(&self.name, self.data_type()?, nullable));
        };

        if self.data_type()? != DataType::Utf8 {
            return Err(Error::config(format!(
                "extra column {:?} declares `values` but is not a string column: a \
                 vocabulary is a set of codes",
                self.name
            )));
        }
        if values.is_empty() {
            return Err(Error::config(format!(
                "extra column {:?} declares an empty `values` set, which no write could \
                 satisfy; omit it to accept any string",
                self.name
            )));
        }
        // The set is rendered into a `CHECK … IN (…)` from field metadata, which
        // is comma-delimited — so a code containing one would split into two.
        if let Some(bad) = values.iter().find(|v| v.contains(',')) {
            return Err(Error::config(format!(
                "extra column {:?} has the code {bad:?}, which contains a comma — the \
                 delimiter the allowed-value set is carried with",
                self.name
            )));
        }

        Ok(crate::config::coded_column(
            &self.name,
            &values.iter().map(String::as_str).collect::<Vec<_>>(),
            nullable,
        ))
    }

    /// The Arrow type this column declares.
    fn data_type(&self) -> Result<DataType> {
        match self.r#type.as_str() {
            "string" | "utf8" | "text" => Ok(DataType::Utf8),
            other => Err(Error::config(format!(
                "extra column {:?} declares type {other:?}; only \"string\" is supported — \
                 every attribute deployments have wanted (tenant, Bilanzkreis, grid area) is a \
                 string, and supporting more needs a bind arm per type",
                self.name
            ))),
        }
    }
}

/// A duration written the way an operator writes one: `15m`, `1d`, `10y`.
///
/// `time::Duration` has no serde format that reads like a configuration file, and
/// a bare number of seconds would make `snapshot_retention = 315360000` a value
/// nobody can check by eye.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanDuration(pub Duration);

impl Serialize for HumanDuration {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&format_duration(self.0))
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        parse_duration(&text)
            .map(HumanDuration)
            .map_err(serde::de::Error::custom)
    }
}

/// Parse a duration the way a configuration file spells it.
///
/// The file format's own parser, exposed because the CLI takes durations on the
/// command line and `--interval 15m` had better mean what `interval = "15m"`
/// means. Two spellings of one unit is exactly the drift a single parser exists
/// to prevent.
pub fn parse_human_duration(text: &str) -> Result<Duration> {
    parse_duration(text).map_err(Error::config)
}

/// Render a duration the way a configuration file spells it.
///
/// The inverse of [`parse_human_duration`], and what the CLI prints so a value
/// it reports can be pasted straight back into the file.
#[must_use]
pub fn format_human_duration(d: Duration) -> String {
    format_duration(d)
}

/// Parse `250ms`, `30s`, `15m`, `6h`, `1d`, `2w`, `10y`.
///
/// A year is 365 days and a week is 7. Neither is a calendar unit here — these
/// configure retention and headroom, not interval arithmetic, and the calendar
/// that *does* matter is `metering`'s (§9.5).
fn parse_duration(text: &str) -> std::result::Result<Duration, String> {
    let trimmed = text.trim();
    let split = trimmed
        .find(|c: char| c.is_ascii_alphabetic())
        .ok_or_else(|| format!("{trimmed:?} has no unit; write 7d, 15m, 10y"))?;
    let (value, unit) = trimmed.split_at(split);

    let value: i64 = value
        .trim()
        .parse()
        .map_err(|_| format!("{value:?} is not a whole number"))?;

    // Milliseconds are their own arm rather than a fraction of a second: every
    // other unit here is a retention or a headroom, and the one setting written
    // below a second — a DDL lock timeout — is a wait, not a span.
    if unit.trim() == "ms" {
        return Ok(Duration::milliseconds(value));
    }

    let seconds = match unit.trim() {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 604_800,
        "y" => 31_536_000,
        other => {
            return Err(format!(
                "unknown unit {other:?}; use ms, s, m, h, d, w or y"
            ));
        }
    };

    value
        .checked_mul(seconds)
        .map(Duration::seconds)
        .ok_or_else(|| format!("{trimmed:?} overflows"))
}

/// The inverse of [`parse_duration`], choosing the largest exact unit.
///
/// Sub-second first, and it is not cosmetic: without it a `750ms` timeout
/// renders as `0s`, which is the spelling that *disables* the timeout. A
/// round-tripped file would then silently turn the setting off.
fn format_duration(d: Duration) -> String {
    let millis = d.whole_milliseconds();
    if millis % 1_000 != 0 {
        return format!("{millis}ms");
    }

    let s = d.whole_seconds();
    for (unit, size) in [
        ("y", 31_536_000),
        ("w", 604_800),
        ("d", 86_400),
        ("h", 3_600),
        ("m", 60),
    ] {
        if s != 0 && s % size == 0 {
            return format!("{}{unit}", s / size);
        }
    }
    format!("{s}s")
}

/// Replace every `${VAR}` from the environment.
///
/// A missing variable is an error. Substituting an empty string instead would
/// turn a typo in a variable name into a connection URL that fails somewhere far
/// from the mistake, with a message about the wrong thing.
fn interpolate(text: &str) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let (value, comment) = split_comment(line);
        interpolate_into(&mut out, value)?;
        out.push_str(comment);
    }
    Ok(out)
}

/// Split a line into its value part and its trailing `#` comment.
///
/// Comments are copied through untouched, which is not cosmetic: the file this
/// crate ships as a starter *documents* the interpolation, and a comment
/// explaining `${DATABASE_URL}` would otherwise be a reference to be resolved —
/// so the template could only be read by a process that already had every
/// variable it was describing.
///
/// A `#` inside a quoted string is not a comment. Tracked with a two-state scan
/// rather than a TOML parser, which is exactly enough: the input has already
/// been read as text and is about to be parsed properly, so this only has to
/// avoid making things *worse* than passing the line through whole.
fn split_comment(line: &str) -> (&str, &str) {
    let mut quote: Option<char> = None;
    for (i, c) in line.char_indices() {
        match (quote, c) {
            (None, '"' | '\'') => quote = Some(c),
            (Some(open), c) if c == open => quote = None,
            (None, '#') => return (&line[..i], &line[i..]),
            _ => {}
        }
    }
    (line, "")
}

/// Replace every `${VAR}` in one stretch of value text.
fn interpolate_into(out: &mut String, text: &str) -> Result<()> {
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let tail = &rest[start + 2..];
        let end = tail.find('}').ok_or_else(|| {
            Error::config("unterminated ${...} in configuration: no closing brace")
        })?;
        let name = &tail[..end];

        let value = std::env::var(name).map_err(|_| {
            Error::config(format!(
                "configuration references ${{{name}}}, which is not set in the environment"
            ))
        })?;
        out.push_str(&value);
        rest = &tail[end + 1..];
    }
    out.push_str(rest);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The configuration page's own example, key for key.
    ///
    /// Kept in step deliberately: a documented key this never parses is a key a
    /// reader copies and a `deny_unknown_fields` rejects.
    const EXAMPLE: &str = r#"
[hot]
url = "postgresql://edm@db.internal/prod"
max_connections = 32

[cold]
catalog = "rest"
uri = "https://catalog.internal"
warehouse = "s3://edm/meterstore"
namespace = "metering"
file_target_bytes = 536870912
metadata_pool_max_connections = 4
region = "eu-central-1"
endpoint = "https://minio.internal"

[[tables]]
name = "readings"
time_model = "interval"
subject_column = "subject_ref"
extra_columns = [
  { name = "tenant",        identity = true },
  { name = "bilanzkreis" },
  { name = "ingest_source", values = ["MSCONS", "SMGW"] },
]

[tables.hot]
partition_headroom = "14d"

[tables.archival]
settlement_lag = "7d"
archival_step = "1d"
scan_chunk_rows = 50000

[tables.maintenance]
snapshot_retention = "10y"
min_snapshots_to_keep = 20
"#;

    #[test]
    fn the_documented_example_parses_and_validates() {
        let settings = Settings::from_toml(EXAMPLE).unwrap();
        assert_eq!(settings.hot.max_connections, 32);
        assert_eq!(settings.cold.catalog, CatalogKind::Rest);

        let table = settings.single_table().unwrap();
        assert_eq!(table.name(), "readings");
        assert_eq!(table.archival_step(), Duration::DAY);
        assert_eq!(table.settlement_lag(), Duration::days(7));
    }

    #[test]
    fn an_identity_column_reaches_the_merge_key() {
        // The whole point of the flag. Declared as an attribute instead, two
        // tenants reporting one measuring point would share a merge key.
        let table = Settings::from_toml(EXAMPLE)
            .unwrap()
            .single_table()
            .unwrap();
        assert!(table.merge_key().contains(&"tenant".to_string()));
        assert!(!table.merge_key().contains(&"bilanzkreis".to_string()));
    }

    #[test]
    fn a_point_table_is_reachable_from_the_configuration_file() {
        // TOML is a front end over the same validated types, so a setting
        // reachable from the builder and not from a file is a broken promise —
        // and this one is a whole record type: a deployment configured from a
        // file could not declare a Zählerstandsgang at all.
        let s: Settings = toml::from_str(
            r#"
[hot]
url = "postgresql://edm@db/prod"

[cold]
catalog = "sql"
uri = "postgresql://edm@db/prod"
warehouse = "file:///tmp/wh"
namespace = "metering"

[[tables]]
name = "meter_reads_versions"
time_model = "point"
"#,
        )
        .expect("parse");

        let table = s.single_table().expect("validate");
        assert_eq!(table.time_model(), crate::config::TimeModel::Point);
        // And the key follows the shape without being spelled out.
        assert!(table.melo_in_merge_key());
    }

    #[test]
    fn the_messlokation_key_can_be_pinned_from_the_configuration_file() {
        let s: Settings = toml::from_str(
            r#"
[hot]
url = "postgresql://edm@db/prod"

[cold]
catalog = "sql"
uri = "postgresql://edm@db/prod"
warehouse = "file:///tmp/wh"
namespace = "metering"

[[tables]]
name = "meter_reads_versions"
time_model = "point"
identify_by_melo = false
"#,
        )
        .expect("parse");

        assert!(!s.single_table().expect("validate").melo_in_merge_key());
    }

    #[test]
    fn a_table_defaults_to_the_interval_shape() {
        let s: Settings = toml::from_str(EXAMPLE).expect("parse");
        let table = s.single_table().expect("validate");
        assert_eq!(table.time_model(), crate::config::TimeModel::Interval);
        assert!(!table.melo_in_merge_key());
    }

    #[test]
    fn the_subject_column_is_registered_as_an_attribute() {
        let table = Settings::from_toml(EXAMPLE)
            .unwrap()
            .single_table()
            .unwrap();
        assert_eq!(table.subject_column(), Some("subject_ref"));
        assert!(!table.merge_key().contains(&"subject_ref".to_string()));
    }

    #[test]
    fn the_infrastructure_sections_are_validated_too() {
        // They were parsed, exposed and consumed by nothing, so nothing checked
        // them: an empty `url`, an empty `warehouse`, a scheme this build cannot
        // open — all passed `validate` and surfaced, if at all, as an obscure
        // failure much later.
        let example: Settings = Settings::from_toml(EXAMPLE).unwrap();
        example
            .validate_all()
            .expect("the documented example is complete");

        for (mutate, expected) in [
            (
                Box::new(|s: &mut Settings| s.hot.url.clear()) as Box<dyn Fn(&mut Settings)>,
                "[hot] url",
            ),
            (
                Box::new(|s: &mut Settings| s.hot.max_connections = 0),
                "max_connections",
            ),
            (
                Box::new(|s: &mut Settings| s.cold.uri.clear()),
                "[cold] uri",
            ),
            (
                Box::new(|s: &mut Settings| s.cold.warehouse.clear()),
                "[cold] warehouse",
            ),
            (
                Box::new(|s: &mut Settings| s.cold.file_target_bytes = 0),
                "file_target_bytes",
            ),
            (
                Box::new(|s: &mut Settings| "ftp://host/wh".clone_into(&mut s.cold.warehouse)),
                "ftp",
            ),
        ] {
            let mut broken = example.clone();
            mutate(&mut broken);
            let err = broken
                .validate_all()
                .expect_err("a section that cannot build a tier must not validate")
                .to_string();
            assert!(err.contains(expected), "expected {expected:?} in {err}");

            // And the *table* validation is unchanged: a file complete for a
            // deployment wiring its own tiers still passes.
            broken
                .validate()
                .expect("validate checks the tables, which are still fine");
        }
    }

    #[test]
    fn the_cold_section_never_prints_its_password() {
        // The SQL form of `uri` is a PostgreSQL connection URL — usually the hot
        // tier's own — and configuration is what a service dumps at startup.
        let mut settings = Settings::from_toml(EXAMPLE).unwrap();
        settings.cold.catalog = CatalogKind::Sql;
        "postgresql://edm:hunter2@db.internal/prod".clone_into(&mut settings.cold.uri);

        let shown = format!("{:?}", settings.cold);
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(shown.contains("postgresql://<redacted>"), "{shown}");
        assert!(shown.contains("s3://edm/meterstore"), "{shown}");
    }

    #[test]
    fn a_subject_column_also_listed_in_extra_columns_is_still_the_subject_column() {
        // Naming the column in both places is one statement, not two — and the
        // marker is the half that matters: without it `subject_column()` is
        // `None`, the write-path check against the registry never runs, and the
        // builder's "subject column without a registry" refusal never fires.
        let toml = r#"
[[tables]]
name = "readings"
subject_column = "subject_ref"
extra_columns = [{ name = "subject_ref", values = ["A", "B"] }]
"#;
        let table = Settings::from_toml(toml).unwrap().single_table().unwrap();

        assert_eq!(table.subject_column(), Some("subject_ref"));
        // Declared once, not twice — and the declaration that survives is the
        // deployment's own, vocabulary and all.
        let declared: Vec<_> = table
            .attribute_columns()
            .iter()
            .filter(|f| f.name() == "subject_ref")
            .collect();
        assert_eq!(declared.len(), 1);
        assert_eq!(
            declared[0]
                .metadata()
                .get(crate::config::CHECK_VALUES_KEY)
                .map(String::as_str),
            Some("A,B"),
        );
        assert!(!table.merge_key().contains(&"subject_ref".to_string()));
    }

    #[test]
    fn a_subject_column_declared_as_identity_is_refused() {
        // In the merge key it looks harmless and is not: a correction whose
        // reference was re-derived gets a different key and fails to supersede.
        let toml = r#"
[[tables]]
name = "readings"
subject_column = "subject_ref"
extra_columns = [{ name = "subject_ref", identity = true }]
"#;
        let err = Settings::from_toml(toml)
            .unwrap()
            .single_table()
            .unwrap_err()
            .to_string();
        assert!(err.contains("merge key"), "{err}");
    }

    #[test]
    fn a_separate_partition_step_is_not_a_setting() {
        // The partition granularity *is* `archival_step`, so a file naming a
        // second key for it must fail rather than read as an accepted setting
        // that changes nothing.
        let toml = r#"
[[tables]]
name = "readings"
[tables.hot]
partition_step = "1w"
"#;
        let err = Settings::from_toml(toml).unwrap_err().to_string();
        assert!(err.contains("partition_step"), "{err}");
    }

    #[test]
    fn an_unknown_key_is_an_error_rather_than_ignored() {
        // A typo in a setting name must not leave the default silently in force.
        let toml = r#"
[[tables]]
name = "readings"
[tables.archival]
settlment_lag = "7d"
"#;
        assert!(Settings::from_toml(toml).is_err());
    }

    #[test]
    fn a_file_with_no_tables_is_an_error() {
        assert!(
            Settings::from_toml("[hot]\nurl = \"x\"\n")
                .unwrap()
                .validate()
                .is_err()
        );
    }

    #[test]
    fn several_tables_are_not_silently_narrowed_to_the_first() {
        let toml = "[[tables]]\nname = \"electricity\"\n\n[[tables]]\nname = \"gas\"\n";
        let settings = Settings::from_toml(toml).unwrap();
        assert_eq!(settings.validate().unwrap().len(), 2);
        assert!(settings.single_table().is_err());
    }

    #[test]
    fn durations_parse_the_way_operators_write_them() {
        assert_eq!(
            parse_duration("250ms").unwrap(),
            Duration::milliseconds(250)
        );
        assert_eq!(parse_duration("30s").unwrap(), Duration::seconds(30));
        assert_eq!(parse_duration("15m").unwrap(), Duration::minutes(15));
        assert_eq!(parse_duration("6h").unwrap(), Duration::hours(6));
        assert_eq!(parse_duration("7d").unwrap(), Duration::days(7));
        assert_eq!(parse_duration("2w").unwrap(), Duration::weeks(2));
        assert_eq!(parse_duration("10y").unwrap(), Duration::days(3_650));
    }

    #[test]
    fn a_duration_with_no_unit_is_rejected() {
        // `settlement_lag = 7` is ambiguous, and guessing days or seconds gets it
        // wrong by five orders of magnitude either way.
        assert!(parse_duration("7").is_err());
        assert!(parse_duration("7 fortnights").is_err());
    }

    #[test]
    fn the_hot_section_carries_a_ddl_lock_timeout() {
        // A file that says nothing gets the same default `PostgresHot` has, and
        // one that says something gets that. Both matter: this is the setting
        // that decides whether a background archival can stall ingest.
        let quiet: Settings = toml::from_str(
            r#"
            [hot]
            url = "postgresql://localhost/meterstore"
            [[tables]]
            name = "readings_versions"
            "#,
        )
        .unwrap();
        assert_eq!(quiet.hot.ddl_lock_timeout.0, Duration::seconds(3));

        let stated: Settings = toml::from_str(
            r#"
            [hot]
            url = "postgresql://localhost/meterstore"
            ddl_lock_timeout = "750ms"
            [[tables]]
            name = "readings_versions"
            "#,
        )
        .unwrap();
        assert_eq!(stated.hot.ddl_lock_timeout.0, Duration::milliseconds(750));

        // And it survives a round trip, so normalising a hand-written file does
        // not quietly drop it.
        let text = stated.to_toml().unwrap();
        let back = Settings::from_toml(&text).unwrap();
        assert_eq!(back.hot.ddl_lock_timeout.0, Duration::milliseconds(750));
    }

    #[test]
    fn durations_round_trip_through_the_file_format() {
        for text in ["250ms", "30s", "15m", "6h", "7d", "2w", "10y"] {
            let parsed = parse_duration(text).unwrap();
            assert_eq!(
                parse_duration(&format_duration(parsed)).unwrap(),
                parsed,
                "{text} did not round-trip"
            );
        }
    }

    #[test]
    fn settings_round_trip_through_toml() {
        let original = Settings::from_toml(EXAMPLE).unwrap();
        let rendered = original.to_toml().unwrap();
        let reparsed = Settings::from_toml(&rendered).unwrap();
        assert_eq!(
            reparsed.single_table().unwrap().merge_key(),
            original.single_table().unwrap().merge_key()
        );
    }

    #[test]
    fn environment_variables_are_interpolated() {
        // `CARGO_PKG_NAME` is set in the environment cargo runs tests in, so the
        // test needs no `set_var` — which this crate forbids anyway, since it
        // requires `unsafe` and is unsound alongside other threads.
        let settings =
            Settings::from_toml("[hot]\nurl = \"postgresql://${CARGO_PKG_NAME}\"\n").unwrap();
        assert_eq!(settings.hot.url, "postgresql://meterstore");
    }

    #[test]
    fn interpolation_handles_several_variables_and_surrounding_text() {
        let settings = Settings::from_toml(
            "[cold]\nwarehouse = \"s3://${CARGO_PKG_NAME}/${CARGO_PKG_NAME}\"\n",
        )
        .unwrap();
        assert_eq!(settings.cold.warehouse, "s3://meterstore/meterstore");
    }

    #[test]
    fn a_comment_may_document_a_placeholder_without_resolving_it() {
        // The starter file this crate writes explains the interpolation in a
        // comment. Interpolating comments too would make that file unreadable by
        // any process that did not already have the variable it was describing —
        // which is every process running `meterstore init`.
        let settings = Settings::from_toml(
            "# set url = \"${METERSTORE_DEFINITELY_UNSET}\" to take it from the environment\n\
             [hot]\n\
             url = \"postgresql://${CARGO_PKG_NAME}\"  # and here it is\n",
        )
        .expect("a comment is documentation, not a reference to resolve");
        assert_eq!(settings.hot.url, "postgresql://meterstore");
    }

    #[test]
    fn a_hash_inside_a_value_is_not_a_comment() {
        // A password may contain one, and treating it as a comment would silently
        // truncate the URL.
        let settings =
            Settings::from_toml("[hot]\nurl = \"postgresql://u:p#w@host/db\"\n").unwrap();
        assert_eq!(settings.hot.url, "postgresql://u:p#w@host/db");
    }

    #[test]
    fn an_unterminated_placeholder_is_an_error() {
        assert!(Settings::from_toml("[hot]\nurl = \"${OOPS\"\n").is_err());
    }

    #[test]
    fn a_missing_environment_variable_is_an_error() {
        // Substituting an empty string would turn a typo into a connection
        // failure somewhere far from the mistake.
        let err = Settings::from_toml("[hot]\nurl = \"${METERSTORE_DEFINITELY_UNSET}\"\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("METERSTORE_DEFINITELY_UNSET"), "{err}");
    }

    #[test]
    fn a_connection_url_is_never_printed_in_full() {
        // Configuration is exactly what gets dumped into a startup log.
        let settings = Settings::from_toml(EXAMPLE).unwrap();
        let shown = format!("{:?}", settings.hot);
        assert!(!shown.contains("db.internal"), "{shown}");
        assert!(shown.contains("postgresql://<redacted>"), "{shown}");
    }

    #[test]
    fn a_coded_column_is_declarable_from_a_file() {
        // §14's claim for this file is that it is a front end over the *same*
        // validated types, with no setting reachable from one and not the other.
        // §14: no setting is reachable from the builder and not from here.
        let toml = r#"
[[tables]]
name = "readings"
extra_columns = [{ name = "ingest_source", values = ["MSCONS", "SMGW"] }]
"#;
        let config = Settings::from_toml(toml).unwrap().single_table().unwrap();
        let column = config
            .attribute_columns()
            .iter()
            .find(|f| f.name() == "ingest_source")
            .expect("declared");

        assert_eq!(
            column
                .metadata()
                .get(crate::config::CHECK_VALUES_KEY)
                .map(String::as_str),
            Some("MSCONS,SMGW"),
            "the vocabulary must reach the field the hot-table DDL reads"
        );
    }

    #[test]
    fn a_vocabulary_that_could_not_survive_the_ddl_is_refused() {
        // The set is carried comma-delimited in field metadata, so a code
        // containing one would silently become two codes.
        for bad in [r#"values = ["A,B"]"#, "values = []"] {
            let toml = format!(
                r#"
[[tables]]
name = "readings"
extra_columns = [{{ name = "ingest_source", {bad} }}]
"#
            );
            assert!(
                Settings::from_toml(&toml).unwrap().single_table().is_err(),
                "{bad} was accepted"
            );
        }
    }

    #[test]
    fn a_non_string_extra_column_is_rejected_with_a_reason() {
        let toml = r#"
[[tables]]
name = "readings"
extra_columns = [{ name = "reading_count", type = "int64" }]
"#;
        let err = Settings::from_toml(toml)
            .unwrap()
            .single_table()
            .unwrap_err()
            .to_string();
        assert!(err.contains("string"), "{err}");
    }
}
