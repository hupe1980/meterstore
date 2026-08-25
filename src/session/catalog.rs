//! Several tables in one session.
//!
//! §15.3 fixes the consistency model: **each table has its own watermark, its
//! own archiver and its own lease**, and nothing is transactional across them.
//! That is deliberate — a cross-table commit would mean a distributed
//! transaction between PostgreSQL and an Iceberg catalog, which is precisely
//! the machinery this design exists without.
//!
//! What was missing was the *ergonomics*. A deployment holding electricity
//! readings beside a non-authoritative second stream — the shape every EDM
//! system has, since ESA Typ-2 values must never reach a billing query — ran a
//! handle per table, and paid for it twice: `system.tables` showed one row, so
//! there was no single place to see whether archival was keeping up; and no
//! query could mention both tables, because each handle owned a private
//! DataFusion catalog.
//!
//! [`MeterCatalog`] fixes both by sharing one [`SessionContext`] across N
//! [`MeterStore`]s. Each store is unchanged and still owns its own tiering; the
//! catalog owns only the query surface and the system tables.
//!
//! ```no_run
//! # use meterstore::{MeterCatalog, MeterStore};
//! # async fn example(
//! #     readings: meterstore::MeterStoreBuilder,
//! #     esa: meterstore::MeterStoreBuilder,
//! # ) -> meterstore::Result<()> {
//! let catalog = MeterCatalog::builder()
//!     .table(readings)
//!     .table(esa)
//!     .build()
//!     .await?;
//!
//! // One statement, both tables — impossible with two handles.
//! let joined = catalog
//!     .query(
//!         "SELECT r.malo_id, COUNT(*) FROM readings r \
//!          JOIN esa_typ2 e USING (malo_id) GROUP BY 1",
//!     )
//!     .await?;
//!
//! // And each table still archives on its own schedule.
//! catalog.table("readings").unwrap().archive(now(), 8).await?;
//! # Ok(())
//! # }
//! # fn now() -> time::OffsetDateTime { time::OffsetDateTime::UNIX_EPOCH }
//! ```

use std::collections::BTreeMap;

use datafusion::prelude::SessionContext;
use time::OffsetDateTime;

use crate::error::{Error, Result};

use super::query::QueryResult;
use super::store::{MeterStore, MeterStoreBuilder};

/// Several [`MeterStore`]s over one DataFusion session.
pub struct MeterCatalog {
    ctx: SessionContext,
    /// Keyed by the *configured* table name, which is what a caller knows.
    ///
    /// Ordered so `tables()` and `system.tables` are stable between runs — an
    /// operator comparing two status dumps should be diffing state, not
    /// hash order.
    stores: BTreeMap<String, MeterStore>,
}

impl std::fmt::Debug for MeterCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeterCatalog")
            .field("tables", &self.stores.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl MeterCatalog {
    /// Start building a catalog.
    pub fn builder() -> MeterCatalogBuilder {
        MeterCatalogBuilder::default()
    }

    /// The shared session, for callers that need DataFusion directly.
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// One table's handle, by its configured name.
    ///
    /// Also accepts the resolved name, since that is what appears in SQL and
    /// therefore what an operator has in front of them: a catalog holding
    /// `readings_versions` answers to `readings` too.
    pub fn table(&self, name: &str) -> Option<&MeterStore> {
        self.stores.get(name).or_else(|| {
            self.stores
                .values()
                .find(|s| super::store::resolved_name(s.config().name()) == name)
        })
    }

    /// A store for one table whose session holds **only that table**.
    ///
    /// ```no_run
    /// # async fn f(catalog: &meterstore::MeterCatalog, sql: &str)
    /// #     -> meterstore::Result<()> {
    /// let billing = catalog.isolated("readings").await?;
    /// let rows = billing.query(sql).await?;   // cannot name any other relation
    /// # Ok(()) }
    /// ```
    ///
    /// A catalog shares one `SessionContext` across its tables — that is what
    /// makes a cross-table join expressible — so any registered relation is
    /// reachable by naming it in caller-supplied SQL. Where a deployment
    /// deliberately keeps a stream *out* of a path, a deny-list of relation names
    /// is a boundary that holds until someone adds a table.
    ///
    /// This makes the isolation a property of the session instead: a statement
    /// naming another table fails to plan. Composes with
    /// [`MeterStore::scoped`](crate::MeterStore::scoped), which confines the rows
    /// as this confines the relations.
    ///
    /// The table keeps its own watermark, archiver and lease either way — this
    /// builds a query surface, not a second store.
    pub async fn isolated(&self, name: &str) -> Result<MeterStore> {
        let store = self.table(name).ok_or_else(|| {
            Error::config(format!(
                "this catalog holds no table {name:?}: it holds [{}]",
                self.stores
                    .keys()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
        store.in_own_session().await
    }

    /// Every table, in name order.
    pub fn tables(&self) -> impl Iterator<Item = &MeterStore> {
        self.stores.values()
    }

    /// How many tables this catalog hosts.
    pub fn len(&self) -> usize {
        self.stores.len()
    }

    /// Whether the catalog hosts no tables.
    ///
    /// Never true for a built catalog — the builder rejects an empty one — but
    /// present because clippy is right that `len` without `is_empty` is a trap.
    pub fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }

    /// Run SQL over every table in the catalog.
    ///
    /// The provenance a [`QueryResult`] carries is the **union** across the
    /// tables the plan touched: the watermarks of all of them, and the tiers
    /// any of them scanned. A single watermark would be a fiction here, because
    /// two tables genuinely have two boundaries (§15.3) and a figure spanning
    /// both was computed against both.
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        self.query_with_params(sql, Vec::new()).await
    }

    /// [`query`](Self::query) with positional parameters.
    ///
    /// The same guarantee a single store gives (§19.7): values reach the engine
    /// bound, never concatenated into the SQL text. Present because a catalog is
    /// *the* multi-table query surface — without it, the only way to filter a
    /// cross-table query by a MaLo-ID from a market message would be to build the
    /// string by hand, which is the thing the parameterised path exists to stop.
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: Vec<datafusion::scalar::ScalarValue>,
    ) -> Result<QueryResult> {
        let (first, watermarks) = self.watermarks_for(sql).await?;
        first.run(sql, params, watermarks).await
    }

    /// The boundaries a statement's answer should be attributed to, and a store
    /// to execute it through.
    ///
    /// **The tables the statement actually reads, not every table hosted.**
    ///
    /// Reporting all of them looks harmless and is not:
    /// [`QueryResult::watermark`](super::QueryResult::watermark) is the
    /// conservative boundary — the oldest of the reported ones — so a
    /// single-table query in a twenty-table catalog would be attributed to
    /// whichever unrelated table happens to archive least often. The figure would
    /// be reconciled against a boundary it was never computed against, which is
    /// precisely the confusion carrying provenance exists to prevent.
    ///
    /// Read off the logical plan, which is where relation names still exist: by
    /// the time the physical plan is built they have become scan nodes. A
    /// statement that reads no managed table — `SELECT 1`, or a query over
    /// `system.*` — has no tier boundary, and saying so beats attaching an
    /// arbitrary one.
    ///
    /// The store returned is only an executor: every table in the catalog shares
    /// one `SessionContext`, so any of them plans and runs the same statement
    /// identically.
    async fn watermarks_for(
        &self,
        sql: &str,
    ) -> Result<(
        &MeterStore,
        Vec<(String, crate::watermark::TieringWatermark)>,
    )> {
        let first = self
            .stores
            .values()
            .next()
            .ok_or_else(|| Error::config("catalog hosts no tables"))?;

        let plan = self
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let scanned = scanned_relations(&plan);

        let mut watermarks = Vec::with_capacity(self.stores.len());
        for (name, store) in &self.stores {
            // A store answers to both of the relations it registers (§13.7.2),
            // and a caller may legitimately query either.
            let touched =
                scanned.contains(&store.raw_table()) || scanned.contains(&store.resolved_table());
            if touched {
                watermarks.push((name.clone(), store.watermark().await?));
            }
        }

        Ok((first, watermarks))
    }

    /// What a statement would produce, **without running it**.
    ///
    /// The multi-table counterpart of
    /// [`MeterStore::describe`](crate::MeterStore::describe), and the half a
    /// Flight SQL client asks for before it fetches anything. The provenance is
    /// the union across the tables the plan touched, exactly as
    /// [`query`](Self::query)'s is.
    pub async fn describe(&self, sql: &str) -> Result<super::QueryDescription> {
        let (first, watermarks) = self.watermarks_for(sql).await?;
        first.describe_with(sql, watermarks).await
    }

    /// Run SQL over every table and **stream** its rows.
    ///
    /// [`query`](Self::query) collects every batch before returning one, which is
    /// right for a settlement total and wrong for the case where the rows *are*
    /// the answer — a BI tool pulling a year of quarter-hour readings across two
    /// streams, or an export. Peak memory is one batch either way here.
    ///
    /// The [`QueryDescription`](super::QueryDescription) comes back before any
    /// row, because a caller putting the boundaries on the wire needs them before
    /// it starts writing — and a catalog has *several*, which is precisely why
    /// they cannot be reconstructed afterwards.
    pub async fn stream(
        &self,
        sql: &str,
    ) -> Result<(
        super::QueryDescription,
        datafusion::execution::SendableRecordBatchStream,
    )> {
        self.stream_with_params(sql, Vec::new()).await
    }

    /// [`stream`](Self::stream) with positional parameters.
    pub async fn stream_with_params(
        &self,
        sql: &str,
        params: Vec<datafusion::scalar::ScalarValue>,
    ) -> Result<(
        super::QueryDescription,
        datafusion::execution::SendableRecordBatchStream,
    )> {
        let (first, watermarks) = self.watermarks_for(sql).await?;
        first.stream_at(sql, params, watermarks).await
    }

    /// Upkeep for **every** table, as one scheduled loop.
    ///
    /// Each table keeps its own watermark, archiver and lease (§15.3) — that
    /// cannot be otherwise and is not what this changes. What it changes is the
    /// *scheduling*: a deployment running a timer per table pays for the same
    /// thing this type exists to stop paying for — no single place to ask whether
    /// upkeep is keeping up, and one cadence per table to drift.
    ///
    /// ```no_run
    /// # async fn f(catalog: meterstore::MeterCatalog) {
    /// let running = catalog.maintenance().expire_snapshots(false).spawn();
    /// # running.shutdown().await;
    /// # }
    /// ```
    ///
    /// The outcome carries one row per table, so an alert names the table that is
    /// unhealthy rather than reporting a number nobody can act on.
    pub fn maintenance(&self) -> super::Maintenance {
        super::Maintenance::over(self.stores.values().cloned().collect())
    }

    /// Create every table's storage, in both tiers.
    ///
    /// Sequential rather than concurrent: creation is a one-off, and a failure
    /// half way through is easier to read when the tables were attempted in a
    /// known order.
    pub async fn create_tables(&self) -> Result<()> {
        for store in self.stores.values() {
            store.create_tables().await?;
        }
        Ok(())
    }

    /// Refresh `system.*` so its rows cover **every** table.
    ///
    /// The reason the catalog exists rather than being a `Vec<MeterStore>`: each
    /// store would otherwise overwrite the others' rows, and the last one to
    /// refresh would appear to be the only table in the deployment.
    pub async fn refresh_system_tables(&self, now: OffsetDateTime) -> Result<()> {
        let views: Vec<super::system::SystemTables<'_>> = self
            .stores
            .values()
            .map(|s| super::system::SystemTables::new(s.hot_store(), s.cold_store(), s.config()))
            .collect();
        super::system::register_all(&self.ctx, &views, now).await
    }

    /// Every table's status, in name order.
    pub async fn status(&self, now: OffsetDateTime) -> Result<Vec<super::system::TableStatus>> {
        let mut out = Vec::with_capacity(self.stores.len());
        for store in self.stores.values() {
            out.push(store.status(now).await?);
        }
        Ok(out)
    }

    /// Archive every table's closed windows.
    ///
    /// Sequential, and each table's failure is its own: a table whose archival
    /// fails leaves the others archived, because they share nothing that a
    /// partial run could corrupt. The error names the table that failed.
    pub async fn archive_all(
        &self,
        now: OffsetDateTime,
        max_windows: usize,
    ) -> Result<Vec<(String, Vec<crate::tiering::archive::ArchivalOutcome>)>> {
        let mut out = Vec::with_capacity(self.stores.len());
        for (name, store) in &self.stores {
            let outcome = store
                .archive(now, max_windows)
                .await
                .map_err(|e| Error::Storage(format!("archiving {name}: {e}")))?;
            out.push((name.clone(), outcome));
        }
        Ok(out)
    }

    /// Check the tiering invariant on every table.
    ///
    /// Returns the tables that violate it, each with what was inconsistent, so
    /// a caller learns about all of them rather than only the first.
    pub async fn verify_invariant(&self) -> Result<Vec<(String, String)>> {
        let mut out = Vec::new();
        for (name, store) in &self.stores {
            if let Err(e) = store.verify_invariant().await {
                match e {
                    Error::InvariantViolated { detail, .. } => out.push((name.clone(), detail)),
                    other => return Err(other),
                }
            }
        }
        Ok(out)
    }
}

/// Every relation a logical plan scans, by name.
///
/// Unqualified: the store registers into the default catalog and schema, so the
/// bare name is what identifies it, and a caller may write either
/// `readings` or `datafusion.public.readings`.
fn scanned_relations(
    plan: &datafusion::logical_expr::LogicalPlan,
) -> std::collections::HashSet<String> {
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::logical_expr::LogicalPlan;

    let mut found = std::collections::HashSet::new();
    // **With subqueries.** A scalar subquery's plan hangs off an *expression*,
    // not off `inputs()`, so a plain `apply` walks past it and
    // `SELECT (SELECT COUNT(*) FROM readings)` names no table at all — which
    // `QueryResult::watermark` reports as the epoch, that nothing has been
    // settled.
    //
    // Infallible visitor, so the traversal cannot fail — the closure only reads.
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            found.insert(scan.table_name.table().to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

#[async_trait::async_trait]
impl super::SqlSurface for MeterCatalog {
    fn label(&self) -> String {
        self.stores
            .values()
            .map(MeterStore::resolved_table)
            .collect::<Vec<_>>()
            .join(", ")
    }

    async fn describe_sql(&self, sql: &str) -> Result<super::QueryDescription> {
        self.describe(sql).await
    }

    async fn stream_sql(
        &self,
        sql: &str,
        params: Vec<datafusion::scalar::ScalarValue>,
    ) -> Result<(
        super::QueryDescription,
        datafusion::execution::SendableRecordBatchStream,
    )> {
        self.stream_with_params(sql, params).await
    }
}

/// Builds a [`MeterCatalog`] from per-table builders.
#[derive(Default)]
pub struct MeterCatalogBuilder {
    tables: Vec<MeterStoreBuilder>,
}

impl std::fmt::Debug for MeterCatalogBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeterCatalogBuilder")
            .field("tables", &self.tables.len())
            .finish()
    }
}

impl MeterCatalogBuilder {
    /// Add a table.
    ///
    /// The builder is taken whole rather than as its parts, so a table declared
    /// here is configured exactly as it would be standalone — including its own
    /// read mode, subject registry and registered name.
    pub fn table(mut self, table: MeterStoreBuilder) -> Self {
        self.tables.push(table);
        self
    }

    /// Build every table into one shared session.
    ///
    /// Fails if two tables would register under the same SQL name. DataFusion
    /// refuses the second registration itself, so this is not the only thing
    /// standing between a caller and the mistake — but its error names a
    /// relation the caller never registered by hand, from inside a builder they
    /// did not know was running. Checking first makes the message about the
    /// duplicate declaration, and avoids leaving the session half-populated.
    pub async fn build(self) -> Result<MeterCatalog> {
        if self.tables.is_empty() {
            return Err(Error::config(
                "a catalog needs at least one table: an empty one can answer no query \
                 and hides the missing configuration until the first request",
            ));
        }

        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_information_schema(true),
        );

        // Checked up front, before anything is registered: a collision found
        // half way through would leave the session holding some of the tables
        // and none of the caller's expectations.
        //
        // The check is on the names each table will *register*, not on the names
        // it was configured with. Those differ, and the difference is the whole
        // trap: §13.7.2 derives both relations from the physical name by adding
        // or stripping `_versions`, so `readings` and `readings_versions` are two
        // distinct configurations that register the same pair of relations.
        // Comparing configured names lets that pair through, and DataFusion then
        // rejects it with a message naming a relation the caller never wrote.
        let mut seen: BTreeMap<String, String> = BTreeMap::new();
        for builder in &self.tables {
            let configured = builder.table_name().ok_or_else(|| {
                Error::config("every table in a catalog needs a table configuration")
            })?;
            let (raw, resolved) = builder
                .registered_names()
                .expect("a builder with a table name has registered names");

            for relation in [raw, resolved] {
                if let Some(owner) = seen.get(&relation) {
                    return Err(Error::config(format!(
                        "tables {owner:?} and {configured:?} both register the relation \
                         {relation:?}: only one of them could answer to it, and a query \
                         naming it would silently read whichever won"
                    )));
                }
                seen.insert(relation, configured.to_string());
            }
        }

        let mut stores = BTreeMap::new();
        for builder in self.tables {
            let store = builder.session(ctx.clone()).build().await?;
            stores.insert(store.config().name().to_string(), store);
        }

        // A result reports one read mode, because a statement runs under one.
        // Two tables in different modes would make that report true of whichever
        // store happened to plan the query — and the modes are not cosmetic:
        // `Historical` reads no PostgreSQL, so a join between a historical table
        // and a unified one silently mixes a reproducible half with a mutable
        // one. Reject rather than average.
        let modes: Vec<(&str, _)> = stores
            .values()
            .map(|s| (s.config().name(), s.read_mode()))
            .collect();
        let uniform = modes.windows(2).all(|w| w[0].1 == w[1].1);
        if !uniform {
            return Err(Error::config(format!(
                "a catalog's tables must share one read mode, but these are {modes:?}: \
                 a result reports the single mode its statement ran under, and mixing \
                 them would attribute a figure half-computed from the mutable tier to \
                 a mode that excludes it"
            )));
        }

        Ok(MeterCatalog { ctx, stores })
    }
}
