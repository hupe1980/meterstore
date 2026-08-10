//! The `MeterStore` handle.
//!
//! Assembles both tiers into a query engine so callers do not have to. Without
//! it, using MeterStore means constructing a `PostgresHot`, an `IcebergCold`, a
//! cold table provider, a `TieredTableProvider`, a `SessionContext`, and
//! registering the calendar functions — six steps in a fixed order, every one of
//! which is the same every time.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;
use time::OffsetDateTime;
use tracing::{debug, info, warn};

use crate::config::ValidatedTableConfig;
use crate::error::{Error, Result};
use crate::planner::{ReadMode, TieredTableProvider};
use crate::tiering::store::{ColdStore, HotStore};
use crate::tiering::{ArchivalOutcome, Archiver};
use crate::watermark::TieringWatermark;

/// The name queries should use for a physical table.
///
/// Deliberately *not* the physical name. The physical table holds **every
/// version** of every reading — the audit trail — and summing it double-counts
/// corrected intervals. The resolved name is a view that keeps only the value
/// currently in force.
pub(crate) fn resolved_name(physical: &str) -> &str {
    physical.strip_suffix("_versions").unwrap_or(physical)
}

/// The name the raw, unresolved table is registered under.
///
/// Exposed deliberately: corrections history is what makes the store auditable,
/// and a query that wants every version should be able to ask for it — just not
/// by accident.
fn raw_name(physical: &str) -> String {
    if physical.ends_with("_versions") {
        physical.to_string()
    } else {
        format!("{physical}_versions")
    }
}

/// A queryable, archivable metering store.
#[derive(Clone)]
pub struct MeterStore {
    ctx: SessionContext,
    hot: Arc<dyn HotStore>,
    cold: Arc<dyn ColdStore>,
    config: ValidatedTableConfig,
    /// The cold half's DataFusion provider.
    ///
    /// Kept so a derived session — [`as_of`](MeterStore::as_of) — can be built
    /// from the same tiers without the caller reassembling them.
    cold_provider: Arc<dyn TableProvider>,
    /// Present when the deployment declared a subject column and a registry.
    registry: Option<crate::erasure::SubjectRegistry>,
    /// The mode every provider in this session was built with.
    ///
    /// Kept so a result can report which one produced it: a figure computed
    /// under `Historical` and one computed under `Unified` are different claims,
    /// and only the first is reproducible.
    mode: ReadMode,
    /// The boundary that was in force at the pinned snapshot, for an as-of
    /// session.
    ///
    /// A reproducible read must report the watermark it *ran against*, which is
    /// the one the pinned snapshot published — not today's. Reporting the current
    /// boundary alongside a settlement rerun would attach a number from one
    /// moment to a boundary from another, which is precisely the confusion
    /// carrying provenance exists to prevent.
    pinned_watermark: Option<TieringWatermark>,
}

impl std::fmt::Debug for MeterStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeterStore")
            .field("table", &self.config.name())
            .finish_non_exhaustive()
    }
}

impl MeterStore {
    /// Start building a store.
    pub fn builder() -> MeterStoreBuilder {
        MeterStoreBuilder::default()
    }

    /// The DataFusion session, for callers who need the engine directly.
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// The table this store manages.
    pub fn table(&self) -> &str {
        self.config.name()
    }

    /// The name queries should use — the version-resolved table.
    pub fn resolved_table(&self) -> String {
        resolved_name(self.config.name()).to_string()
    }

    /// The name of the raw table, holding every version.
    pub fn raw_table(&self) -> String {
        raw_name(self.config.name())
    }

    /// Run a SQL query across both tiers.
    ///
    /// Returns DataFusion's own `DataFrame`, so every expression, window function
    /// and output format works and there is no second query language to maintain.
    /// Use [`query`](Self::query) instead when the result needs its provenance.
    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        self.ctx
            .sql(query)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    /// The read mode this session was built with.
    pub fn read_mode(&self) -> ReadMode {
        self.mode
    }

    /// Run a query and keep the provenance of the result.
    ///
    /// The tier boundary and the tiers actually read are what make a figure
    /// reconcilable later (P1). A `SUM` alone cannot say whether it crossed the
    /// boundary, and two identical queries a minute apart can read the same rows
    /// from different tiers.
    ///
    /// Both facts are already available at plan time, so this costs a plan walk
    /// rather than a second query.
    pub async fn query(&self, sql: &str) -> Result<super::QueryResult> {
        self.query_with_params(sql, Vec::new()).await
    }

    /// [`query`](Self::query) with positional parameters.
    ///
    /// Values reach the engine as bound parameters and are never concatenated
    /// into the SQL text (§19.7), so a caller may pass a `malo_id` straight from
    /// a market message.
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: Vec<datafusion::scalar::ScalarValue>,
    ) -> Result<super::QueryResult> {
        let watermark = match self.pinned_watermark {
            Some(pinned) => pinned,
            None => self.watermark().await?,
        };
        self.run(
            sql,
            params,
            vec![(self.config.name().to_string(), watermark)],
        )
        .await
    }

    /// What a statement would produce, **without running it**.
    ///
    /// Plans the query — so a syntax error, an unknown column or an unknown
    /// relation is reported here — and returns the schema it would produce, the
    /// boundary it would run against and the tiers it would read, then stops.
    ///
    /// This is what a surface needing a schema before any row should call.
    /// Answering such a request by executing the query makes an Arrow Flight
    /// client's ordinary `GetFlightInfo` → `DoGet` sequence cost two full scans,
    /// and makes "preparing" a statement run it.
    ///
    /// Planning still reads the tier boundary, and for the resolved table the
    /// per-file statistics that decide elision — a catalogue read rather than a
    /// scan, which is the right price for describing a statement.
    pub async fn describe(&self, sql: &str) -> Result<super::QueryDescription> {
        let watermark = match self.pinned_watermark {
            Some(pinned) => pinned,
            None => self.watermark().await?,
        };
        self.describe_with(sql, vec![(self.config.name().to_string(), watermark)])
            .await
    }

    /// [`describe`](Self::describe), attributed to the given boundaries.
    pub(crate) async fn describe_with(
        &self,
        sql: &str,
        watermarks: Vec<(String, TieringWatermark)>,
    ) -> Result<super::QueryDescription> {
        let frame = self.sql(sql).await?;
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let plan = frame
            .create_physical_plan()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(super::QueryDescription::new(
            schema,
            watermarks,
            super::query::tiers_of(&plan),
            self.mode,
        ))
    }

    /// Plan and execute, attributing the result to the given boundaries.
    ///
    /// Split out so a [`MeterCatalog`] can run a statement that mentions several
    /// tables and have the result carry all of their watermarks — the same
    /// execution path, told the truth about how many boundaries were involved.
    ///
    /// [`MeterCatalog`]: super::MeterCatalog
    pub(crate) async fn run(
        &self,
        sql: &str,
        params: Vec<datafusion::scalar::ScalarValue>,
        watermarks: Vec<(String, TieringWatermark)>,
    ) -> Result<super::QueryResult> {
        let mut frame = self.sql(sql).await?;
        if !params.is_empty() {
            frame = frame
                .with_param_values(params)
                .map_err(|e| Error::Storage(e.to_string()))?;
        }

        let schema = Arc::new(frame.schema().as_arrow().clone());
        let plan = frame
            .create_physical_plan()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        // Read off the plan before executing it: the plan *is* the tier decision,
        // it belongs to this query alone, and asking the providers afterwards
        // would race any other query in flight.
        let tiers = super::query::tiers_of(&plan);

        let batches = datafusion::physical_plan::collect(plan, self.ctx.task_ctx())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(super::QueryResult::new(
            batches, schema, watermarks, tiers, self.mode,
        ))
    }

    /// This store's validated configuration.
    pub fn config(&self) -> &ValidatedTableConfig {
        &self.config
    }

    /// Read one measuring point as the domain type.
    ///
    /// The typed path (§13.4): rows come back version-resolved and tier-split, as
    /// a [`MeasurementSeries`] the `metering` crate computes with directly.
    ///
    /// [`MeasurementSeries`]: metering::measurement_series::MeasurementSeries
    pub fn series(&self, malo_id: impl Into<String>) -> super::SeriesQuery<'_> {
        super::SeriesQuery::new(self, malo_id)
    }

    /// Completeness of every channel over a range (§9.6).
    ///
    /// A missing interval is information, not an empty set. The expected count
    /// comes from each series' declared resolution and `metering`'s DST-aware
    /// calendar, so a 92-interval spring day is complete and a 96-interval autumn
    /// day is four short.
    pub async fn completeness(
        &self,
        from: time::OffsetDateTime,
        to: time::OffsetDateTime,
    ) -> Result<Vec<super::Completeness>> {
        let resolved = self
            .ctx
            .table_provider(self.resolved_table().as_str())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        super::completeness::compute(
            &self.ctx.state(),
            resolved,
            &self.resolved_table(),
            from,
            to,
        )
        .await
    }

    /// Every committed state of the cold table, newest first.
    ///
    /// The list an auditor's question resolves against: "which snapshot did the
    /// settlement run against" is answered by an id from here, and
    /// [`as_of`](Self::as_of) then reproduces it exactly.
    pub async fn snapshots(&self) -> Result<Vec<crate::tiering::store::SnapshotInfo>> {
        self.cold.snapshots(self.config.name()).await
    }

    /// A session pinned to a past state, for a reproducible read.
    ///
    /// This is the regulatory feature. MaBiS settlement must be reproducible, and
    /// an Iceberg snapshot plus an optional version ceiling reconstructs exactly
    /// what was known at a point in time — "the settlement as computed on the 8th
    /// working day".
    ///
    /// The returned store is **cold-only** and reads no PostgreSQL, because the
    /// hot tier keeps no history of itself: including it would make the answer
    /// depend on when the query ran.
    ///
    /// Pass `max_version` when the *domain* version axis must be pinned too. The
    /// snapshot alone pins what the store had been told; a snapshot taken after a
    /// correction landed holds both versions and resolution prefers the newer
    /// one, so without a ceiling a rerun reproduces the store's knowledge rather
    /// than the settlement's inputs.
    pub async fn as_of(
        &self,
        snapshot: crate::planner::SnapshotSelector,
        max_version: Option<crate::version::Version>,
    ) -> Result<Self> {
        // Resolved eagerly so a bad selector fails here rather than inside the
        // first query, where the error would arrive as an opaque scan failure.
        self.cold
            .snapshot_provider(self.config.name(), snapshot)
            .await?;

        // The boundary the pinned snapshot published, so results from this
        // session report the state they actually ran against.
        //
        // Walked back from the pinned snapshot rather than read off it, for the
        // same reason the cold tier's own lookup walks: a snapshot written out of
        // band — the compaction §10.3.1 recommends, run with Spark or PyIceberg —
        // is a valid Iceberg commit that carries no watermark. Reading only the
        // pinned one would report the epoch for a settlement rerun that ran
        // against a real boundary. The list is newest-first, so the suffix from
        // the pinned snapshot is its own history.
        let snapshots = self.snapshots().await?;
        let pinned_watermark = snapshots
            .iter()
            .position(|s| match snapshot {
                crate::planner::SnapshotSelector::Id(id) => s.snapshot_id == id,
                crate::planner::SnapshotSelector::Timestamp(at) => s.committed_at <= at,
            })
            .and_then(|i| snapshots[i..].iter().find_map(|s| s.watermark))
            .unwrap_or_else(TieringWatermark::empty);

        let mut builder = MeterStoreBuilder::default()
            .hot(Arc::clone(&self.hot))
            .cold(Arc::clone(&self.cold), Arc::clone(&self.cold_provider))
            .table(self.config.clone())
            .read_mode(ReadMode::AsOf {
                snapshot,
                max_version,
            });
        if let Some(registry) = self.registry.clone() {
            builder = builder.subject_registry(registry);
        }

        let mut store = builder.build().await?;
        store.pinned_watermark = Some(pinned_watermark);
        Ok(store)
    }

    /// A session that reads the data **as it was known at** `at`, across both tiers.
    ///
    /// The transaction-time counterpart to [`as_of`](Self::as_of). Where `as_of`
    /// pins the cold tier to an Iceberg snapshot and is therefore cold-only, this
    /// pins the row-level `recorded_at` axis every row carries in both tiers — so
    /// it answers "what did we believe at time `at`" for the recent (hot) window
    /// too, not only settled history. Only versions recorded at or before `at`
    /// enter resolution, so a correction delivered afterwards, and an interval
    /// first stored afterwards, are both invisible.
    ///
    /// Unlike `as_of` this pins no snapshot and needs no Iceberg machinery: it
    /// stays reproducible because archival only ever *moves* a row (with its
    /// `recorded_at`) between tiers, never rewrites the axis. The returned store
    /// reads at the current watermark; the ceiling, not the watermark, is what
    /// makes the read historical.
    pub async fn as_known_at(&self, at: time::OffsetDateTime) -> Result<Self> {
        let mut builder = MeterStoreBuilder::default()
            .hot(Arc::clone(&self.hot))
            .cold(Arc::clone(&self.cold), Arc::clone(&self.cold_provider))
            .table(self.config.clone())
            .read_mode(ReadMode::AsKnownAt(at));
        if let Some(registry) = self.registry.clone() {
            builder = builder.subject_registry(registry);
        }
        builder.build().await
    }

    /// The current tier boundary.
    pub async fn watermark(&self) -> Result<TieringWatermark> {
        self.cold.watermark(self.config.name()).await
    }

    /// An archiver for this store's table.
    pub fn archiver(&self) -> Archiver<Arc<dyn HotStore>, Arc<dyn ColdStore>> {
        Archiver::new(
            Arc::clone(&self.hot),
            Arc::clone(&self.cold),
            self.config.clone(),
        )
    }

    /// Scheduled upkeep for this store: archival, expiry, and the health check.
    ///
    /// Nothing runs until [`Maintenance::run_once`] or
    /// [`Maintenance::spawn`] is called. A store that started a background loop
    /// on construction would surprise a process that only wanted to read.
    ///
    /// [`Maintenance::run_once`]: crate::session::Maintenance::run_once
    /// [`Maintenance::spawn`]: crate::session::Maintenance::spawn
    pub fn maintenance(&self) -> super::Maintenance {
        super::Maintenance::new(self.clone())
    }

    /// Archive every window that is due, up to `max_windows`.
    pub async fn archive(
        &self,
        now: time::OffsetDateTime,
        max_windows: usize,
    ) -> Result<Vec<ArchivalOutcome>> {
        self.archiver().catch_up(now, max_windows).await
    }

    /// Expire cold-tier snapshots past the configured retention window.
    ///
    /// Returns how many were removed. Not called automatically: the retention
    /// window is a compliance decision, and a store that quietly expired
    /// snapshots would be deciding how far back a settlement can be reproduced.
    pub async fn expire_snapshots(&self, now: time::OffsetDateTime) -> Result<usize> {
        self.cold
            .expire_snapshots(
                self.config.name(),
                self.config.snapshot_retention(),
                self.config.min_snapshots_to_keep(),
                now,
            )
            .await
    }

    /// Put the tiering boundary back on the cold table's current snapshot.
    ///
    /// Run this after any out-of-band maintenance — the compaction or orphan
    /// cleanup this crate cannot perform itself, done with Spark or PyIceberg
    /// against the same table. Those produce valid Iceberg commits that carry no
    /// watermark, and the boundary is then only findable by walking back the
    /// parent chain, which snapshot expiry can punch a hole in.
    ///
    /// It republishes what the history already says, so it cannot move the
    /// boundary, and it is a no-op when the current snapshot already carries one.
    /// [`expire_snapshots`](Self::expire_snapshots) calls it first for that
    /// reason, so a deployment on the maintenance schedule need not.
    pub async fn reassert_watermark(&self) -> Result<bool> {
        Ok(self
            .cold
            .reassert_watermark(self.config.name())
            .await?
            .is_some())
    }

    /// Assert that no row sits in the wrong tier.
    pub async fn verify_invariant(&self) -> Result<()> {
        self.archiver().verify_invariant().await
    }

    /// Compare the configured schema against the cold table's (§11).
    ///
    /// Reports every difference, whether or not it is safe. Callers that want the
    /// table to *stop* on an unsafe one use
    /// [`Compatibility::require_safe`](crate::evolution::Compatibility::require_safe),
    /// which the archiver already does before every run.
    ///
    /// `None` when the cold store cannot report a schema. That is deliberately
    /// not "compatible": a check that cannot run has proved nothing, and saying so
    /// beats implying it passed.
    pub async fn check_schema(&self) -> Result<Option<crate::evolution::Compatibility>> {
        let Some(stored) = self.cold.stored_schema(self.config.name()).await? else {
            return Ok(None);
        };
        let configured = crate::encode::schema::storage_schema(&self.config.extra_columns());
        Ok(Some(crate::evolution::compare(&configured, &stored)))
    }

    /// Create both tiers' tables with this store's configuration.
    ///
    /// The single entry point on purpose. The hot table's primary key, the cold
    /// table's schema and the resolution view's `PARTITION BY` all have to agree
    /// on what identifies a reading; creating them separately means three places
    /// to keep in step, and a mismatch shows up as readings that fail to
    /// supersede rather than as an error.
    pub async fn create_tables(&self) -> Result<()> {
        let extra = self.config.extra_columns();
        self.hot
            .create_tables(self.config.name(), &self.config.merge_key(), &extra)
            .await?;
        self.cold
            .create_tables(
                self.config.name(),
                &self.config.identity_column_names(),
                &extra,
            )
            .await?;
        // The registry's tables belong to the same creation step: a subject
        // column whose registry has no tables accepts writes and fails the first
        // erasure request, which is exactly when failing is least useful.
        if let Some(registry) = &self.registry {
            registry.create_tables().await?;
        }
        info!(table = self.config.name(), "tables ready");
        Ok(())
    }

    /// Refresh `system.tables` and `system.config` in this session.
    ///
    /// Explicit rather than automatic: gathering the status reads the watermark
    /// and counts stranded rows, and a query should never silently pay for that.
    pub async fn refresh_system_tables(&self, now: time::OffsetDateTime) -> Result<()> {
        super::system::SystemTables::new(&self.hot, &self.cold, &self.config)
            .register(&self.ctx, now)
            .await
    }

    /// Current operational status, without going through SQL.
    pub async fn status(&self, now: time::OffsetDateTime) -> Result<super::system::TableStatus> {
        super::system::SystemTables::new(&self.hot, &self.cold, &self.config)
            .status(now)
            .await
    }

    /// The cold tier, for callers that need it directly.
    ///
    /// Exposed because the tier traits are the extension point (§5.1): a
    /// deployment may want to list snapshots, seed a watermark, or drive the
    /// store's own maintenance from outside the handle.
    pub fn cold_store(&self) -> &Arc<dyn ColdStore> {
        &self.cold
    }

    /// The hot tier, for callers that need it directly.
    pub fn hot_store(&self) -> &Arc<dyn HotStore> {
        &self.hot
    }

    /// The deployment's declared extra columns.
    pub fn extra_columns(&self) -> Vec<crate::arrow::datatypes::Field> {
        self.config.extra_columns()
    }

    /// Write a series, routing each interval to the tier that owns it.
    ///
    /// This is the only safe way to record a **late correction** — a restated
    /// value for an interval that has already been archived. Such a row cannot
    /// go to PostgreSQL: it would sit below the watermark, where no query looks
    /// for it, so the correction would be silently ignored while appearing to
    /// have been accepted.
    ///
    /// Routing is by `from` against the current watermark, the same rule the
    /// query path uses, so a value is written to the tier that will be read.
    ///
    /// # This appends; it never overwrites
    ///
    /// **A correction is a new row at a higher `version`, not an update.** That is
    /// MSCONS's own rule — the application handbook corrects a value by versioning
    /// it — and it is why the resolved `readings` relation applies
    /// latest-version-wins over the raw `readings_versions` one. A caller
    /// expecting update semantics gets an append, and the prior value stays
    /// readable, which is the point: it is what makes a past settlement
    /// reproducible.
    ///
    /// Nothing here deletes. The cold tier is append-only (Iceberg v2, no
    /// deletion vectors), and that follows from the versioning rule rather than
    /// constraining it — v3's row lineage would duplicate `version`, which is the
    /// better identifier because a network operator assigns it and an auditor can
    /// read it.
    ///
    /// Writing the same `(merge key, version)` twice is a no-op rather than an
    /// error, because every ingest transport delivers at least once. Writing a
    /// *different value* under an existing version is reported, not silently
    /// kept: a version identifies an assertion.
    ///
    /// For steady-state ingest of current data, [`hot_writer`](Self::hot_writer)
    /// avoids the boundary read this method performs on every call.
    pub async fn append(&self, series: &[crate::encode::StoredSeries]) -> Result<AppendOutcome> {
        self.check_subject_refs(series).await?;

        let watermark = self.watermark().await?;
        let table = self.config.name();

        let mut hot = Vec::new();
        let mut cold = Vec::new();
        for stored in series {
            let (below, at_or_above): (Vec<_>, Vec<_>) = stored
                .series
                .intervals
                .iter()
                .cloned()
                .partition(|i| i.from < watermark.get());

            if !at_or_above.is_empty() {
                let mut s = stored.clone();
                s.series.intervals = at_or_above;
                hot.push(s);
            }
            if !below.is_empty() {
                let mut s = stored.clone();
                s.series.intervals = below;
                cold.push(s);
            }
        }

        let mut outcome = AppendOutcome::default();

        if !hot.is_empty() {
            // A partitioned table rejects a row with no partition to hold it, so
            // an insert past the pre-created frontier fails outright. The
            // archiver maintains headroom on its own schedule, but a store that
            // has not archived yet — a fresh deployment, or a backfill reaching
            // further ahead than the headroom — would otherwise get a bare
            // "no partition of relation found for row" from PostgreSQL.
            let (first, last) = hot_bounds(&hot);
            self.hot
                .ensure_partitions(
                    table,
                    first,
                    // Exclusive, so the partition holding `last` must be created.
                    last + self.config.partition_step(),
                    self.config.partition_step(),
                )
                .await?;

            let batch = crate::encode::to_record_batch_with(&hot, &self.config.extra_columns())?;
            let reported = self
                .hot
                .append_reporting(table, &self.config.merge_key(), &[batch])
                .await?;
            outcome.hot_rows = reported
                .iter()
                .filter(|d| d.effect != crate::session::Effect::Duplicate)
                .count() as u64;
            outcome.displacements.extend(reported);
        }
        if !cold.is_empty() {
            // Only the cold half needs this. The hot tier enforces the same rule
            // with a constraint, which no code path can go around; Iceberg has no
            // constraints, so the check has to live where both tiers are visible.
            self.check_scope_agreement(&cold).await?;

            let batch = crate::encode::to_record_batch_with(&cold, &self.config.extra_columns())?;
            // Appended without moving the watermark: the tier boundary is about
            // which range each tier owns, and a correction does not change that.
            // The correction batch really is in memory here, so the bloom-filter
            // hint can be exact rather than a default.
            let hints = crate::tiering::store::WriteHints {
                distinct_malo_ids: Some(crate::encode::distinct_malo_ids(std::slice::from_ref(
                    &batch,
                ))),
            };
            outcome.cold_rows = self
                .cold
                .append_only(table, crate::tiering::store::stream_of(vec![batch]), hints)
                .await?
                .rows;
        }

        crate::observe::metrics()
            .late_corrections
            .add(outcome.cold_rows, &crate::observe::table(table));

        info!(
            table,
            hot = outcome.hot_rows,
            cold = outcome.cold_rows,
            "append routed"
        );
        Ok(outcome)
    }

    /// Open a bulk writer for **current** interval data.
    ///
    /// [`append`](Self::append) is the correct entry point for a delivery that
    /// might contain a late correction, and it pays for that on every call: it
    /// reads the tier boundary, which means loading Iceberg table metadata. For
    /// a service landing MSCONS or iMSys batches continuously that round trip is
    /// per *batch*, not per row — one catalog load per 96 values if a batch is
    /// one meter-day.
    ///
    /// This reads the boundary **once** and hands back a writer that reuses it.
    ///
    /// # Why reusing a boundary is safe here, and where it stops being
    ///
    /// The writer **refuses** any interval below the boundary it was opened at,
    /// rather than routing it to the cold tier. That is the whole safety
    /// argument: routing on a stale boundary could place a row below the true
    /// watermark, where no query looks; refusing cannot. A refusal names the
    /// interval and tells the caller to use [`append`](Self::append).
    ///
    /// A snapshot can only be stale in one direction — the watermark is
    /// monotonic (§6.3) — so the risk is a row accepted here that the true
    /// boundary has since passed. The margin against that is the **settlement
    /// lag**: archival never closes a window newer than `now - settlement_lag`,
    /// a week by default, while current data has `from` near now. A writer held
    /// for the length of an ingest run is nowhere near that margin; one held for
    /// days is, so reopen per run rather than caching one for the process
    /// lifetime. [`verify_invariant`](Self::verify_invariant) is the backstop.
    ///
    /// # This is also the write contract
    ///
    /// Applications used to be told to `INSERT` with their own driver, which
    /// meant reproducing the hot schema, its primary key, the canonical-OBIS
    /// `CHECK`, the Sparte/unit code lists and the intra-version overlap
    /// exclusion — a contract carried in prose, where drift shows up as readings
    /// that silently fail to supersede. Going through [`StoredSeries`] makes it
    /// compiler-checked instead.
    ///
    /// [`StoredSeries`]: crate::encode::StoredSeries
    pub async fn hot_writer(&self) -> Result<HotWriter<'_>> {
        Ok(HotWriter {
            store: self,
            watermark: self.watermark().await?,
            ensured: Default::default(),
        })
    }

    /// Irreversibly destroy this table and every reading in it, in both tiers.
    ///
    /// **The only operation in this crate that deletes stored readings.**
    /// Everything else is append-only (§4.2): a correction is a new version, a
    /// hot partition drop reclaims space for rows already durable in Iceberg, and
    /// erasure (§12.4) destroys a *mapping* rather than rows. That asymmetry is
    /// deliberate — a settlement must stay reproducible — and it means this is the
    /// operation to reach for when the answer really is "none of this data should
    /// exist any more".
    ///
    /// The two cases that need it:
    ///
    /// - **Decommissioning a tenant.** With a table per tenant (§15.2.1), this is
    ///   how their data leaves. Within a shared table there is no equivalent, and
    ///   there cannot be: removing one tenant's rows from an Iceberg table means
    ///   rewriting files, which `iceberg-rust` cannot do (§10.3.1).
    /// - **A statutory maximum retention.** Same constraint: expiry is
    ///   whole-table, so a period that differs per tenant needs a table per
    ///   tenant.
    ///
    /// # The name must be repeated
    ///
    /// `confirm` must equal this store's table name. A handle carries no visual
    /// indication of which table it points at, and this destroys data with no
    /// recovery path — so the caller states the name and the store checks it,
    /// rather than trusting that the right handle was reached for.
    ///
    /// # What is destroyed
    ///
    /// The PostgreSQL table with every partition, attached or detached; the
    /// Iceberg catalog entry; and the Parquet data files, manifests and metadata
    /// in object storage. Snapshots do not survive it, so `as_of` against this
    /// table stops working — that is the point.
    ///
    /// The subject registry is **not** touched: it is shared across tables, and
    /// destroying one table's readings says nothing about whether a subject's
    /// mapping should go. Use [`erase_subject`](Self::erase_subject) for that.
    pub async fn purge_table(&self, confirm: &str) -> Result<()> {
        let table = self.config.name();
        if confirm != table {
            return Err(Error::config(format!(
                "refusing to purge: this store manages {table:?} but the confirmation \
                 named {confirm:?}. The name is repeated because a purge destroys \
                 every reading in the table with no recovery path"
            )));
        }

        // Cold first. If this succeeds and the hot drop then fails, what remains
        // is a PostgreSQL table holding only the unarchived window — visible,
        // countable, and re-purgeable. The other order would leave archived data
        // in object storage with nothing in PostgreSQL to name it, which is the
        // state that looks like success and is not.
        self.cold.purge_table(table).await?;
        self.hot.drop_table(table).await?;

        warn!(table, "table purged: every reading destroyed in both tiers");
        Ok(())
    }

    /// The registry backing this table's subject column, if configured.
    pub fn subject_registry(&self) -> Option<&crate::erasure::SubjectRegistry> {
        self.registry.as_ref()
    }

    /// Register a natural identifier and get the reference to store.
    ///
    /// Convenience over [`SubjectRegistry::register`], so an ingest path that
    /// already holds a `MeterStore` does not have to thread the registry
    /// separately.
    ///
    /// [`SubjectRegistry::register`]: crate::erasure::SubjectRegistry::register
    pub async fn register_subject(&self, natural_id: &str) -> Result<crate::erasure::SubjectRef> {
        self.require_registry()?.register(natural_id).await
    }

    /// Destroy a subject's linkage, leaving the readings anonymous.
    ///
    /// The lake keeps every row. What disappears is the mapping that says whose
    /// they are, which is what Article 17 asks for over storage that cannot
    /// rewrite history.
    pub async fn erase_subject(
        &self,
        subject: &crate::erasure::SubjectRef,
        reason: &str,
        actor: &str,
        now: time::OffsetDateTime,
    ) -> Result<crate::erasure::ErasureRecord> {
        self.require_registry()?
            .erase(subject, reason, actor, now)
            .await
    }

    /// Anonymise every subject whose readings all predate `cutoff`.
    ///
    /// # This is a duty on a clock, not a request to wait for
    ///
    /// § 60 Abs. 6 MsbG obliges the Messstellenbetreiber to **erase or
    /// anonymise** personenbezogene Messwerte as soon as storing them is no
    /// longer necessary, *"spätestens jedoch nach drei Jahren ab dem Schluss des
    /// Kalenderjahres, in dem der jeweilige Messwert erhoben wurde"*.
    ///
    /// Three years is a **ceiling**, and the operative trigger is earlier. That
    /// is the opposite of a retention mandate, and it is worth stating because
    /// this design — and `metering` before 0.17 — described the provision
    /// backwards. A store built to keep personal metering values for three years
    /// *because the law says so* has it inverted.
    ///
    /// [`erase_subject`](Self::erase_subject) answers an Article 17 request, one
    /// subject at a time, when someone asks. This is the standing obligation:
    /// nobody asks, and it comes due anyway.
    ///
    /// # Why anonymising is the whole of it
    ///
    /// The statute says *löschen **oder** anonymisieren*, and the second branch
    /// is the one an immutable lake can take. Destroying the mapping leaves
    /// quantities against an opaque token — anonymous data, outside the
    /// Regulation by Recital 26 — while the settlement record stays reproducible,
    /// which is what the Eichrecht documentation duties and every later audit
    /// need. It is also `O(1)` per subject against a lake that cannot rewrite
    /// files at all (§10.3.1), so the branch MeterStore can take is also the one
    /// that costs nothing.
    ///
    /// # The trigger is the reading, not the registration
    ///
    /// A subject registered in 2020 may still be metered today, so a sweep keyed
    /// to registration would erase a live customer. The cutoff is applied to the
    /// **latest reading** attributed to each reference, over both tiers — a
    /// subject is anonymised only once every value it explains has passed the
    /// ceiling.
    ///
    /// `cutoff` is the caller's: the statutory ceiling is a calendar computation
    /// over the year a value was *erhoben*, and the earlier "no longer necessary"
    /// trigger is a business decision this crate has no view on.
    ///
    /// References the registry no longer resolves are skipped, so the sweep is
    /// idempotent and a re-run writes no second audit row.
    pub async fn anonymise_before(
        &self,
        cutoff: time::OffsetDateTime,
        reason: &str,
        actor: &str,
        now: time::OffsetDateTime,
    ) -> Result<Vec<crate::erasure::ErasureRecord>> {
        let registry = self.require_registry()?;
        let column = self.config.subject_column().ok_or_else(|| {
            Error::config(
                "no subject column is declared, so there is no linkage to destroy: \
                 without one the stored readings carry no reference to a person and \
                 § 60 Abs. 6 has nothing to act on here",
            )
        })?;

        // The **raw** relation, not the resolved one. A superseded version is
        // still a stored personal value, so a subject whose only recent row is a
        // correction that lost resolution has not passed the ceiling.
        let sql = format!(
            r#"SELECT "{column}" FROM {raw}
               WHERE "{column}" IS NOT NULL
               GROUP BY "{column}"
               HAVING max("{from}") < $1"#,
            raw = raw_name(self.config.name()),
            from = crate::encode::schema::col::FROM,
        );
        let due = self
            .query_with_params(
                &sql,
                vec![datafusion::scalar::ScalarValue::TimestampMicrosecond(
                    Some(micros(cutoff)),
                    Some("UTC".into()),
                )],
            )
            .await?;

        let mut references = std::collections::BTreeSet::new();
        for batch in due.batches() {
            let values = column_str(batch, column)?;
            for i in 0..batch.num_rows() {
                if !crate::arrow::array::Array::is_null(values, i) {
                    references.insert(values.value(i).to_string());
                }
            }
        }

        let mut erased = Vec::new();
        for reference in references {
            let subject = crate::erasure::SubjectRef::new(reference)?;
            // Already anonymised — by an Article 17 request, or by an earlier
            // sweep. Nothing left to destroy, and an audit row would claim an
            // erasure that did not happen on this run.
            if registry.resolve(&subject).await?.is_none() {
                continue;
            }
            erased.push(registry.erase(&subject, reason, actor, now).await?);
        }

        if !erased.is_empty() {
            warn!(
                table = self.config.name(),
                subjects = erased.len(),
                %cutoff,
                "anonymised subjects whose readings have passed the retention ceiling"
            );
        }
        Ok(erased)
    }

    fn require_registry(&self) -> Result<&crate::erasure::SubjectRegistry> {
        self.registry.as_ref().ok_or_else(|| {
            Error::config(
                "no subject registry is configured: declare a subject column and \
                 pass a registry to the builder",
            )
        })
    }

    /// Reject a write whose subject references have no live mapping.
    ///
    /// A reference that does not resolve means one of two things, and both are
    /// worth stopping. Either the pipeline invented it, in which case the rows
    /// are unattributable from the moment they land; or it belongs to a subject
    /// already erased, in which case a replay is rebuilding the link that
    /// erasure destroyed. Neither is visible in the data afterwards — the column
    /// looks perfectly well-formed either way — so it has to be caught here.
    ///
    /// Distinct references only: a batch is typically many intervals for a
    /// handful of subjects, so this is a small query however large the batch.
    /// Refuse a cold write that would give one reading a second network operator.
    ///
    /// A version is comparable only within its `(operator, month)` scope (§4.2),
    /// and resolution partitions by scope — so one reading carrying two operators
    /// yields two winners, both survive into the resolved view, and every sum
    /// over them doubles.
    ///
    /// The hot tier refuses this with a per-partition exclusion constraint, which
    /// nothing can go around. **Iceberg has no constraints**, and `append` routes
    /// a below-watermark interval straight there — so without this check the
    /// guard would be reachable only by not being late, which is backwards: a
    /// late correction is the delivery *most* likely to carry a stale operator,
    /// because it is the one assembled furthest from the original message.
    ///
    /// Scoped to the intervals actually being written, and to their measuring
    /// points, so the cost is proportional to the correction rather than to the
    /// history. A late correction is rare and small; a full-table scan here would
    /// be a tax on the common path to guard the uncommon one.
    async fn check_scope_agreement(&self, cold: &[crate::encode::StoredSeries]) -> Result<()> {
        use datafusion::scalar::ScalarValue;

        // (malo_id, obis_code, from) → the operator this batch asserts. The
        // identity columns are deliberately absent: a deployment may extend the
        // merge key, and this check is about the *reading*, which those columns
        // subdivide rather than redefine. Checking the coarser key is the safe
        // direction — it can report a conflict the finer key would allow, and
        // the message names both scopes so the caller can tell.
        let mut incoming: std::collections::BTreeMap<(String, String, OffsetDateTime), String> =
            Default::default();
        let mut malo_ids = std::collections::BTreeSet::new();
        let (mut lo, mut hi) = (None::<OffsetDateTime>, None::<OffsetDateTime>);

        for stored in cold {
            let operator = stored.version.scope().operator().to_string();
            for interval in &stored.series.intervals {
                let obis = interval
                    .obis_code
                    .or(stored.series.obis_code)
                    .map(|o| o.to_string())
                    .unwrap_or_default();
                incoming.insert(
                    (stored.series.malo_id.clone(), obis, interval.from),
                    operator.clone(),
                );
                malo_ids.insert(stored.series.malo_id.clone());
                lo = Some(lo.map_or(interval.from, |v: OffsetDateTime| v.min(interval.from)));
                hi = Some(hi.map_or(interval.from, |v: OffsetDateTime| v.max(interval.from)));
            }
        }

        let (Some(lo), Some(hi)) = (lo, hi) else {
            return Ok(());
        };

        // Parameterised throughout: a `malo_id` reaching here came off a market
        // message (§19.7).
        let mut params: Vec<ScalarValue> = vec![
            ScalarValue::TimestampMicrosecond(Some(micros(lo)), Some("UTC".into())),
            ScalarValue::TimestampMicrosecond(Some(micros(hi)), Some("UTC".into())),
        ];
        let mut placeholders = Vec::with_capacity(malo_ids.len());
        for (i, malo) in malo_ids.iter().enumerate() {
            params.push(ScalarValue::Utf8(Some(malo.clone())));
            placeholders.push(format!("${}", i + 3));
        }

        let sql = format!(
            r#"SELECT DISTINCT "{malo}", "{obis}", "{from}", "{scope}"
               FROM {raw}
               WHERE "{from}" >= $1 AND "{from}" <= $2
                 AND "{malo}" IN ({places})"#,
            malo = crate::encode::schema::col::MALO_ID,
            obis = crate::encode::schema::col::OBIS_CODE,
            from = crate::encode::schema::col::FROM,
            scope = crate::encode::schema::col::VERSION_SCOPE,
            raw = raw_name(self.config.name()),
            places = placeholders.join(", "),
        );

        let existing = self.query_with_params(&sql, params).await?;
        for batch in existing.batches() {
            let malo = column_str(batch, crate::encode::schema::col::MALO_ID)?;
            let obis = column_str(batch, crate::encode::schema::col::OBIS_CODE)?;
            let scope = column_str(batch, crate::encode::schema::col::VERSION_SCOPE)?;
            let from = batch
                .column_by_name(crate::encode::schema::col::FROM)
                .and_then(|c| {
                    c.as_any()
                        .downcast_ref::<crate::arrow::array::TimestampMicrosecondArray>()
                })
                .ok_or_else(|| {
                    Error::decode(crate::encode::schema::col::FROM, "expected a timestamp")
                })?;

            for i in 0..batch.num_rows() {
                let stored_scope = crate::version::VersionScope::parse(scope.value(i))?;
                let at =
                    OffsetDateTime::from_unix_timestamp_nanos(i128::from(from.value(i)) * 1_000)
                        .map_err(|e| {
                            Error::decode(crate::encode::schema::col::FROM, e.to_string())
                        })?;

                let key = (malo.value(i).to_string(), obis.value(i).to_string(), at);
                let Some(asserted) = incoming.get(&key) else {
                    continue;
                };
                if asserted != stored_scope.operator() {
                    return Err(Error::config(format!(
                        "reading {} {} at {at} is already stored under network operator {:?} \
                         but this delivery asserts {asserted:?}. A version is comparable only \
                         within its (operator, month) scope, so both would survive resolution \
                         and double every sum over them. Check that the scope carries the \
                         *network operator* rather than a forwarding party or a tenant",
                        key.0,
                        key.1,
                        stored_scope.operator(),
                    )));
                }
            }
        }

        Ok(())
    }

    async fn check_subject_refs(&self, series: &[crate::encode::StoredSeries]) -> Result<()> {
        let (Some(column), Some(registry)) = (self.config.subject_column(), &self.registry) else {
            return Ok(());
        };

        let mut refs = std::collections::BTreeSet::new();
        for stored in series {
            match stored.extra.get(column) {
                Some(datafusion::scalar::ScalarValue::Utf8(Some(value))) => {
                    refs.insert(value.clone());
                }
                // Absent or null: the column is nullable, and a reading whose
                // subject is genuinely unknown is a real state. It is simply not
                // linked to anyone, so there is nothing to verify.
                _ => continue,
            }
        }

        for reference in refs {
            let subject = crate::erasure::SubjectRef::new(reference)?;
            if registry.resolve(&subject).await?.is_none() {
                return Err(Error::config(format!(
                    "subject reference {subject} has no live mapping: it was \
                     either never registered, or erased — in which case this \
                     write is a replay that would re-link an erased subject"
                )));
            }
        }
        Ok(())
    }

    /// SQL that resolves a raw scan to the current value of each interval.
    ///
    /// Published for engines reading the Iceberg tables directly, which see
    /// every version and would otherwise double-count corrected intervals.
    pub fn resolution_sql(&self) -> String {
        crate::planner::version::resolution_sql_with_key(
            &self.raw_table(),
            &self.config.merge_key(),
            &self.config.extra_columns(),
            // The published text is the general current-knowledge resolution; a
            // transaction-time ceiling is a per-read concern, not part of it.
            None,
        )
    }
}

/// The earliest and latest interval start in a batch bound for the hot tier.
///
/// The caller has already established the slice is non-empty and that every
/// series in it has at least one interval, so both bounds exist.
fn hot_bounds(hot: &[crate::encode::StoredSeries]) -> (time::OffsetDateTime, time::OffsetDateTime) {
    let starts = || {
        hot.iter()
            .flat_map(|s| s.series.intervals.iter().map(|i| i.from))
    };
    (
        starts().min().expect("non-empty"),
        starts().max().expect("non-empty"),
    )
}

/// How many rows an [`append`] wrote to each tier.
///
/// [`append`]: MeterStore::append
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppendOutcome {
    /// Rows written to PostgreSQL.
    pub hot_rows: u64,
    /// Rows written to Iceberg — late corrections for archived intervals.
    pub cold_rows: u64,
    /// What each written row did to the value that was current.
    ///
    /// One entry per interval in the batch. This is what a caller building a
    /// correction audit trail needs and cannot get from a count: whether a write
    /// was a new reading, a correction that took effect, a backfill an existing
    /// higher version still outranks, or a replay that wrote nothing.
    ///
    /// Reading it separately would race the write — and be wrong exactly when
    /// two corrections arrive together, which is when an audit trail matters. On
    /// the hot tier the prior state and the insert share one transaction.
    ///
    /// See [`Displacement`](crate::session::Displacement).
    pub displacements: Vec<crate::session::Displacement>,
}

impl AppendOutcome {
    /// Total rows written.
    pub fn total(&self) -> u64 {
        self.hot_rows + self.cold_rows
    }

    /// Whether any row was a late correction.
    pub fn had_late_corrections(&self) -> bool {
        self.cold_rows > 0
    }
}

/// A bulk writer bound to one snapshot of the tier boundary.
///
/// Produced by [`MeterStore::hot_writer`]. Reuses the boundary it was opened
/// with, so a run of batches costs one Iceberg metadata load rather than one per
/// batch, and **refuses** anything below that boundary rather than routing it —
/// see [`MeterStore::hot_writer`] for why that refusal is the safety argument
/// rather than a limitation.
#[derive(Debug)]
pub struct HotWriter<'a> {
    store: &'a MeterStore,
    watermark: TieringWatermark,
    /// Partition bounds this writer has already ensured exist.
    ///
    /// The boundary read was not the only per-batch round trip: ensuring
    /// partitions costs one catalogue lookup **per partition per batch**, and a
    /// service landing meter-days spends the whole run re-asking about the same
    /// one or two. A partition cannot stop existing under a writer — only
    /// archival drops one, and only below the watermark, which this writer
    /// refuses to cross — so a bound confirmed once stays confirmed for the
    /// writer's life.
    ensured: std::sync::Mutex<std::collections::BTreeSet<OffsetDateTime>>,
}

impl HotWriter<'_> {
    /// The boundary this writer was opened at.
    ///
    /// Exposed so a caller can gate its own batch — splitting current data from
    /// late corrections before it gets here — rather than discovering the split
    /// as an error.
    pub fn watermark(&self) -> TieringWatermark {
        self.watermark
    }

    /// Write current data, returning the number of rows PostgreSQL accepted.
    ///
    /// Rows already present under the same `(merge key, version)` are skipped
    /// rather than rewritten, so a redelivery is a no-op — every ingest transport
    /// worth using delivers at least once. The returned count is what was
    /// *inserted*, so `rows < intervals` means a replay, not a loss.
    ///
    /// # Errors
    ///
    /// Refuses the whole batch if any interval starts below the boundary. That
    /// interval belongs to the cold tier, and writing it here would put it where
    /// no query looks. Pass the batch to [`MeterStore::append`], which routes.
    pub async fn append(&self, series: &[crate::encode::StoredSeries]) -> Result<u64> {
        let store = self.store;
        store.check_subject_refs(series).await?;

        // Checked before anything is written, so a mixed batch fails whole
        // rather than landing its current half and leaving the caller to work
        // out which intervals made it.
        for stored in series {
            for interval in &stored.series.intervals {
                if interval.from < self.watermark.get() {
                    return Err(Error::config(format!(
                        "interval starting {} is below the tier boundary {} and belongs \
                         to the cold tier: a hot writer refuses it rather than placing \
                         it where no query looks. Use MeterStore::append, which routes \
                         each interval to the tier that owns it",
                        interval.from, self.watermark,
                    )));
                }
            }
        }

        // Nothing to write, including the case that is not `is_empty()`: a
        // delivery of series that carry no intervals. `hot_bounds` has no answer
        // for that, and the honest report is that zero rows were written.
        if series.iter().all(|s| s.series.intervals.is_empty()) {
            return Ok(0);
        }

        let table = store.config.name();
        let step = store.config.partition_step();
        let (first, last) = hot_bounds(series);

        // Only the bounds this writer has not already confirmed. A run of
        // meter-days touches the same one or two partitions over and over, and
        // the catalogue lookup per partition per batch is pure repetition.
        let (from, until) = {
            let ensured = self.ensured.lock().expect("writer state");
            let mut needed = crate::watermark::align_to_step(first, step);
            let end = crate::watermark::align_to_step(last, step) + step;
            while needed < end && ensured.contains(&needed) {
                needed += step;
            }
            (needed, end)
        };
        if from < until {
            store
                .hot
                .ensure_partitions(table, from, until, step)
                .await?;
            let mut ensured = self.ensured.lock().expect("writer state");
            let mut confirmed = from;
            while confirmed < until {
                ensured.insert(confirmed);
                confirmed += step;
            }
        }

        let batch = crate::encode::to_record_batch_with(series, &store.config.extra_columns())?;
        let rows = store
            .hot
            .append(table, &store.config.merge_key(), &[batch])
            .await?;

        debug!(table, rows, "hot writer append");
        Ok(rows)
    }
}

/// Microseconds since the Unix epoch, for a bound timestamp parameter.
fn micros(at: OffsetDateTime) -> i64 {
    (at.unix_timestamp_nanos() / 1_000) as i64
}

/// A named string column, or a decode error rather than a panic.
fn column_str<'a>(
    batch: &'a crate::arrow::array::RecordBatch,
    name: &str,
) -> Result<&'a crate::arrow::array::StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| {
            c.as_any()
                .downcast_ref::<crate::arrow::array::StringArray>()
        })
        .ok_or_else(|| Error::decode(name, "expected a string column"))
}

/// Builder for [`MeterStore`].
#[derive(Default)]
pub struct MeterStoreBuilder {
    hot: Option<Arc<dyn HotStore>>,
    cold: Option<Arc<dyn ColdStore>>,
    cold_provider: Option<Arc<dyn datafusion::catalog::TableProvider>>,
    config: Option<ValidatedTableConfig>,
    mode: ReadMode,
    register_as: Option<String>,
    registry: Option<crate::erasure::SubjectRegistry>,
    session: Option<SessionContext>,
}

impl MeterStoreBuilder {
    /// The hot tier.
    pub fn hot(mut self, hot: Arc<dyn HotStore>) -> Self {
        self.hot = Some(hot);
        self
    }

    /// The cold tier, and its DataFusion provider.
    ///
    /// Both are needed and they are not interchangeable: the store reads the
    /// watermark through `ColdStore`, and scans through the provider.
    pub fn cold(
        mut self,
        cold: Arc<dyn ColdStore>,
        provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Self {
        self.cold = Some(cold);
        self.cold_provider = Some(provider);
        self
    }

    /// The table configuration.
    pub fn table(mut self, config: ValidatedTableConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// The configured table name, before the store is built.
    ///
    /// Lets a [`MeterCatalog`](crate::MeterCatalog) detect a duplicate *before*
    /// registering anything, so the error names the mistake rather than
    /// surfacing as DataFusion's "table already exists" from inside a builder
    /// the caller did not know was being run.
    pub fn table_name(&self) -> Option<&str> {
        self.config.as_ref().map(|c| c.name())
    }

    /// The SQL names this builder will register: the raw relation, then the
    /// resolved one.
    ///
    /// **Not the same thing as the configured name**, and the difference is what
    /// makes a naive uniqueness check wrong. §13.7.2 derives both from the
    /// physical name by adding or stripping `_versions`, so `readings` and
    /// `readings_versions` are two configurations that register exactly the same
    /// pair. `register_as` overrides the second, which is a third way for two
    /// tables to collide.
    pub fn registered_names(&self) -> Option<(String, String)> {
        let config = self.config.as_ref()?;
        Some((
            raw_name(config.name()),
            self.register_as
                .clone()
                .unwrap_or_else(|| resolved_name(config.name()).to_string()),
        ))
    }

    /// Restrict which tiers queries read.
    pub fn read_mode(mut self, mode: ReadMode) -> Self {
        self.mode = mode;
        self
    }

    /// Build into an existing DataFusion session instead of a fresh one.
    ///
    /// This is what lets several tables share a catalog, and therefore what
    /// makes a join across them expressible — see [`MeterCatalog`], which is the
    /// supported way to reach it. Each table keeps its own watermark, archiver
    /// and lease (§15.3); only the query surface is shared.
    ///
    /// [`MeterCatalog`]: crate::session::MeterCatalog
    pub fn session(mut self, ctx: SessionContext) -> Self {
        self.session = Some(ctx);
        self
    }

    /// The registry backing this table's subject column.
    ///
    /// Required whenever the configuration declares a
    /// [`subject_column`](crate::config::TableConfig::subject_column):
    /// without it, writes carry references that nothing can resolve and erasure
    /// has no mapping to destroy — the column would be decoration.
    pub fn subject_registry(mut self, registry: crate::erasure::SubjectRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Override the name the table is registered under.
    pub fn register_as(mut self, name: impl Into<String>) -> Self {
        self.register_as = Some(name.into());
        self
    }

    /// Assemble the store.
    ///
    /// Async because the resolved table is planned from its SQL at build time.
    pub async fn build(self) -> Result<MeterStore> {
        let hot = self
            .hot
            .ok_or_else(|| Error::config("hot tier is required"))?;
        let cold = self
            .cold
            .ok_or_else(|| Error::config("cold tier is required"))?;
        let provider = self
            .cold_provider
            .ok_or_else(|| Error::config("cold table provider is required"))?;
        let config = self
            .config
            .ok_or_else(|| Error::config("table configuration is required"))?;

        // A subject column without a registry is a column of strings nobody can
        // resolve and erasure cannot act on. Caught here rather than at the
        // first erasure request, which is the worst moment to discover it.
        if config.subject_column().is_some() && self.registry.is_none() {
            return Err(Error::config(
                "a subject column is declared but no subject registry was \
                 provided: the references would resolve to nothing and erasure \
                 would have no mapping to destroy",
            ));
        }

        // The cold table decides the union schema, so a configuration it cannot
        // hold has to be caught here. Left to the resolution planner it surfaces
        // as "No field named tenant" against a list of fourteen columns, which
        // says nothing about the actual mistake — declaring an identity column on
        // a table that already holds rows changes what "the same reading" means
        // (§11), and the operator needs to be told that, not shown a schema dump.
        if let Some(stored) = cold.stored_schema(config.name()).await? {
            let configured = crate::encode::schema::storage_schema(&config.extra_columns());
            crate::evolution::compare(&configured, &stored).require_safe(config.name())?;
        }

        // `information_schema` is off by default in DataFusion, and without it a
        // client cannot discover what tables exist — it can only query names it
        // was told out of band. That matters most for the surfaces where there
        // is nobody to tell: a BI tool over Flight SQL (§13.7.3) lists the
        // catalog before it queries anything, and an operator at a SQL prompt
        // does the same. It also makes the distinction between `readings` and
        // `readings_versions` (§13.7.2) discoverable rather than folklore.
        let ctx = match self.session {
            Some(existing) => existing,
            None => SessionContext::new_with_config(
                datafusion::prelude::SessionConfig::new().with_information_schema(true),
            ),
        };
        // Idempotent: DataFusion replaces a UDF registered twice under one name,
        // so a shared session picks these up from whichever table registers
        // first and the rest are no-ops.
        for udf in super::udf::all() {
            ctx.register_udf(udf);
        }

        let tiered = Arc::new(
            TieredTableProvider::new(
                config.name(),
                Arc::clone(&hot),
                Arc::clone(&cold),
                Arc::clone(&provider),
            )
            .with_scan_spec(config.scan_spec())
            .with_mode(self.mode),
        );

        // The raw table holds every version. Registering it under a name that
        // says so keeps the audit trail reachable without making it the thing a
        // careless `SELECT SUM(...)` hits.
        let raw = raw_name(config.name());
        ctx.register_table(&raw, Arc::clone(&tiered) as Arc<dyn TableProvider>)
            .map_err(|e| Error::Storage(e.to_string()))?;

        let resolved = self
            .register_as
            .unwrap_or_else(|| resolved_name(config.name()).to_string());

        // Resolution must partition by the same key the storage layer treats as
        // identity. A deployment that extends it — a tenant discriminator, say —
        // would otherwise have two tenants' readings competing to supersede one
        // another.
        //
        // Planned from the same SQL published for external engines, so the text
        // an operator runs in Spark and the plan this session executes cannot
        // drift apart.
        // A transaction-time (`AsKnownAt`) read bakes a `recorded_at` ceiling into
        // the resolution plan's inner scan, so only versions known by that instant
        // are ranked. It is a session constant, fixed for the life of this store.
        let resolution_plan = ctx
            .state()
            .create_logical_plan(&crate::planner::version::resolution_sql_with_key(
                &raw,
                &config.merge_key(),
                &config.extra_columns(),
                self.mode.recorded_at_ceiling(),
            ))
            .await
            .map_err(|e| Error::Storage(format!("planning resolution: {e}")))?;

        // A provider rather than a view: eliding resolution needs per-file
        // version statistics for the range being scanned, which a view cannot
        // see and a provider is handed on every call.
        let resolved_provider: Arc<dyn TableProvider> =
            Arc::new(crate::planner::ResolvedTableProvider::new(
                tiered,
                Arc::clone(&cold),
                config.name(),
                resolution_plan,
            ));
        ctx.register_table(&resolved, Arc::clone(&resolved_provider))
            .map_err(|e| Error::Storage(e.to_string()))?;

        // Completeness reads the *resolved* table: a corrected interval is one
        // interval, and counting both its versions would report a complete day
        // as having more intervals than the calendar allows.
        ctx.register_udtf(
            super::CompletenessFunction::NAME,
            Arc::new(super::CompletenessFunction::new(
                resolved_provider,
                resolved.clone(),
            )),
        );

        info!(
            table = config.name(),
            raw = %raw,
            resolved = %resolved,
            mode = ?self.mode,
            "meterstore ready"
        );

        Ok(MeterStore {
            ctx,
            hot,
            cold,
            config,
            cold_provider: provider,
            registry: self.registry,
            mode: self.mode,
            // Set by `as_of`, which is the only thing that pins a snapshot.
            pinned_watermark: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TableConfig;

    #[test]
    fn the_resolved_name_drops_the_versions_suffix() {
        assert_eq!(resolved_name("readings_versions"), "readings");
        assert_eq!(resolved_name("readings"), "readings");
        assert_eq!(resolved_name("gas_versions"), "gas");
    }

    #[test]
    fn the_raw_name_always_carries_the_suffix() {
        // Whichever name the table was configured with, the unresolved relation
        // must announce that it holds every version.
        assert_eq!(raw_name("readings_versions"), "readings_versions");
        assert_eq!(raw_name("readings"), "readings_versions");
    }

    #[test]
    fn the_two_names_never_collide() {
        for physical in ["readings", "readings_versions", "gas"] {
            assert_ne!(resolved_name(physical), raw_name(physical));
        }
    }

    #[test]
    fn a_suffix_only_at_the_end_is_stripped() {
        assert_eq!(resolved_name("versions_readings"), "versions_readings");
    }

    #[tokio::test]
    async fn building_without_a_tier_is_an_error() {
        let err = MeterStore::builder()
            .table(TableConfig::new("readings").build().unwrap())
            .build()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("hot tier"));
    }

    #[tokio::test]
    async fn building_without_a_table_config_is_an_error() {
        let err = MeterStore::builder().build().await.unwrap_err();
        assert!(err.to_string().contains("hot tier") || err.to_string().contains("table"));
    }
}
