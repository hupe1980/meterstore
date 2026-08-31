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
    /// Identity equalities every query in this session is confined to.
    ///
    /// Empty for an ordinary store. Set by [`MeterStore::scoped`], and carried
    /// into any session derived from this one so a reproducible read of a scoped
    /// store stays scoped.
    row_scope: Vec<(String, datafusion::scalar::ScalarValue)>,
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
    ///
    /// # Caller-supplied SQL: confine the session, not the statement
    ///
    /// Where the text comes from outside, the confinement has to be a property of
    /// the **session** — the statement is the thing you do not control.
    /// [`scoped`](Self::scoped) injects a merge-key equality *below the
    /// projection*, so no statement can omit it, alias around it or `UNION` past
    /// it; [`MeterCatalog::isolated`](super::MeterCatalog::isolated) does the same
    /// for relations. The two compose. Matching relation names against a
    /// deny-list does not: that boundary holds until someone adds a table.
    ///
    /// # Queries only
    ///
    /// Anything else is refused. DataFusion's SQL surface is wider than
    /// `SELECT`, and the wider part never touches a table provider — so
    /// `CREATE EXTERNAL TABLE … LOCATION` reads any path the process can,
    /// including the warehouse's own Parquet, and `COPY … TO` writes one. Both
    /// arrive as queries, and `ctx.sql` executes DDL as it plans it.
    ///
    /// The plan is therefore built without being run, checked, and only then
    /// handed back. That is what makes [`scoped`](Self::scoped) and
    /// [`MeterCatalog::isolated`](super::MeterCatalog::isolated) boundaries
    /// rather than conventions. [`context`](Self::context) is the unrestricted
    /// door.
    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        let state = self.ctx.state();
        // `create_logical_plan`, never `ctx.sql`: the latter *executes* a DDL
        // statement while planning it, so by the time there is a plan to inspect
        // the table has been created and the file written.
        let plan = state
            .create_logical_plan(query)
            .await
            .map_err(Error::from)?;
        require_read_only(&plan)?;
        Ok(DataFrame::new(state, plan))
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
    ///
    /// Everything [`sql`](Self::sql) says about caller-supplied text applies: it
    /// is refused unless it is a query, and a scan is confined by
    /// [`scoped`](Self::scoped) and
    /// [`MeterCatalog::isolated`](super::MeterCatalog::isolated) rather than by
    /// anything in the text.
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
        let plan = frame.create_physical_plan().await.map_err(Error::from)?;

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
        let plan = self.plan(sql, params).await?;
        let schema = plan.schema();

        // Read off the plan before executing it: the plan *is* the tier decision,
        // it belongs to this query alone, and asking the providers afterwards
        // would race any other query in flight.
        let tiers = super::query::tiers_of(&plan);

        let batches = datafusion::physical_plan::collect(plan, self.ctx.task_ctx())
            .await
            .map_err(Error::from)?;

        Ok(super::QueryResult::new(
            batches, schema, watermarks, tiers, self.mode,
        ))
    }

    /// Run a query and **stream** its rows, keeping the provenance.
    ///
    /// [`query`](Self::query) collects every batch before returning one, which is
    /// right for the figures this store mostly produces — a settlement total, a
    /// daily curve, a completeness report all fit in memory by construction. It
    /// is wrong for the case where the rows *are* the answer: a BI tool pulling a
    /// year of quarter-hour readings over Flight SQL, or an export.
    ///
    /// This plans the statement, reads the provenance off the plan, and hands
    /// back a stream that has not been executed yet — so peak memory is one batch
    /// rather than the whole result, which is the same bound archival keeps.
    ///
    /// The [`QueryDescription`] comes back **first**, before any row, because a
    /// caller that has to put the boundary on the wire needs it before it starts
    /// writing (P1). It is the same type [`describe`](Self::describe) returns, so
    /// the two surfaces cannot disagree about what a statement produces.
    ///
    /// This is what Flight SQL serves, so it is the surface most likely to be
    /// handed **caller-supplied SQL**: confine the session with
    /// [`scoped`](Self::scoped) and
    /// [`MeterCatalog::isolated`](super::MeterCatalog::isolated).
    ///
    /// [`QueryDescription`]: super::QueryDescription
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
    ///
    /// Values reach the engine bound, never concatenated into the SQL text, so a
    /// caller may pass a `malo_id` straight from a market message.
    pub async fn stream_with_params(
        &self,
        sql: &str,
        params: Vec<datafusion::scalar::ScalarValue>,
    ) -> Result<(
        super::QueryDescription,
        datafusion::execution::SendableRecordBatchStream,
    )> {
        let watermark = match self.pinned_watermark {
            Some(pinned) => pinned,
            None => self.watermark().await?,
        };
        self.stream_at(
            sql,
            params,
            vec![(self.config.name().to_string(), watermark)],
        )
        .await
    }

    /// The streaming counterpart of [`run`](Self::run), attributed to the given
    /// boundaries — so a [`MeterCatalog`] can stream a multi-table statement.
    ///
    /// [`MeterCatalog`]: super::MeterCatalog
    pub(crate) async fn stream_at(
        &self,
        sql: &str,
        params: Vec<datafusion::scalar::ScalarValue>,
        watermarks: Vec<(String, TieringWatermark)>,
    ) -> Result<(
        super::QueryDescription,
        datafusion::execution::SendableRecordBatchStream,
    )> {
        let plan = self.plan(sql, params).await?;
        let schema = plan.schema();
        // Off the plan, before execution: the plan *is* the tier decision, and
        // asking the providers afterwards would race any other query in flight.
        let tiers = super::query::tiers_of(&plan);

        let stream = datafusion::physical_plan::execute_stream(plan, self.ctx.task_ctx())
            .map_err(Error::from)?;

        Ok((
            super::QueryDescription::new(schema, watermarks, tiers, self.mode),
            stream,
        ))
    }

    /// Plan a statement, binding any parameters.
    async fn plan(
        &self,
        sql: &str,
        params: Vec<datafusion::scalar::ScalarValue>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let mut frame = self.sql(sql).await?;
        if !params.is_empty() {
            frame = frame.with_param_values(params).map_err(Error::from)?;
        }
        frame.create_physical_plan().await.map_err(Error::from)
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
    /// # The identifier is parsed, not taken on trust
    ///
    /// A MaLo-ID carries a check digit, so a transposition in the one thing that
    /// selects *whose* readings come back is detectable — and this is the last
    /// place it can still be detected, because past here a wrong-but-plausible
    /// identifier simply returns an empty series and no error. Accepts either a
    /// string, which is checked here, or a [`MaloId`] already parsed at the
    /// caller's own boundary, which costs nothing:
    ///
    /// ```no_run
    /// # async fn f(store: &meterstore::MeterStore) -> meterstore::Result<()> {
    /// let series = store.series("41373559241")?.collect().await?;
    /// # Ok(()) }
    /// ```
    ///
    /// # A series is one channel of one reading
    ///
    /// `collect` **refuses** a range that spans two channels or two readings.
    /// A meter reporting import and export carries two OBIS codes at the same
    /// instants; a shared store carries a row per tenant; a Mehrfamilienhaus
    /// carries a row per Messlokation. Each of those is a second interval at the
    /// same instant, and [`MeasurementSeries`] has no way to say so — folded,
    /// `metering::aggregate` sums both and the month comes back doubled.
    ///
    /// Name the one you meant with
    /// [`obis`](super::SeriesQuery::obis) or
    /// [`column_eq`](super::SeriesQuery::column_eq), or confine the session with
    /// [`scoped`](Self::scoped). A column that is not in the merge key never
    /// splits a series, so a Bilanzkreis reassigned between two deliveries is
    /// still one series.
    ///
    /// [`MeasurementSeries`]: metering::measurement_series::MeasurementSeries
    /// [`MaloId`]: metering::ids::MaloId
    pub fn series<M>(&self, malo_id: M) -> Result<super::SeriesQuery<'_>>
    where
        M: TryInto<metering::ids::MaloId>,
        M::Error: std::fmt::Display,
    {
        Ok(super::SeriesQuery::new(
            self,
            crate::encode::parse_malo(malo_id)?,
        ))
    }
    /// Read one measuring point's **registers** as the domain type.
    ///
    /// The point counterpart of [`series`](Self::series), and a builder for the
    /// same reasons: a point table identifies a reading by its Messlokation, so a
    /// Marktlokation with two meters returns two registers at every instant and a
    /// caller needs to be able to name one — and "what does the meter read now"
    /// is the question a register is asked most often, which
    /// [`ReadingsQuery::latest`] answers with a `LIMIT 1` rather than by folding
    /// a decade.
    ///
    /// ```no_run
    /// # async fn f(store: &meterstore::MeterStore) -> meterstore::Result<()> {
    /// let now = store
    ///     .readings("41373559241")?
    ///     .melo("DE0001234567890123456789012345")?
    ///     .obis("1-8-0")?
    ///     .latest()
    ///     .await?;
    /// # let _ = now;
    /// # Ok(()) }
    /// ```
    ///
    /// Only a table declared
    /// [`TimeModel::Point`](crate::config::TimeModel::Point) has registers to
    /// read; an interval table refuses, because `value` means interval energy
    /// there.
    ///
    /// [`ReadingsQuery::latest`]: crate::session::ReadingsQuery::latest
    pub fn readings<M>(&self, malo_id: M) -> Result<super::ReadingsQuery<'_>>
    where
        M: TryInto<metering::ids::MaloId>,
        M::Error: std::fmt::Display,
    {
        self.require_time_model(crate::config::TimeModel::Point, "readings")?;
        Ok(super::ReadingsQuery::new(
            self,
            crate::encode::parse_malo(malo_id)?,
        ))
    }

    /// Completeness of every channel over a range (§9.6).
    ///
    /// A missing interval is information, not an empty set. The expected count
    /// comes from each series' declared resolution and `metering`'s DST-aware
    /// calendar, so a 92-interval spring day is complete and a 96-interval autumn
    /// day is four short.
    ///
    /// Awaited directly for the plain report:
    ///
    /// ```no_run
    /// # async fn example(store: &meterstore::MeterStore) -> meterstore::Result<()> {
    /// # let (from, to) = (time::OffsetDateTime::UNIX_EPOCH, time::OffsetDateTime::UNIX_EPOCH);
    /// for row in store.completeness(from, to).await? {
    ///     if !row.is_complete() { /* … */ }
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// [`seen_since`](super::CompletenessQuery::seen_since) adds the one finding
    /// a range can never make about itself: a channel that delivered **nothing**
    /// produces no rows, so it is absent from the report rather than reported as
    /// empty — the strongest form of incompleteness, and the invisible one.
    ///
    /// [`malo`](super::CompletenessQuery::malo),
    /// [`obis`](super::CompletenessQuery::obis) and
    /// [`column_eq`](super::CompletenessQuery::column_eq) narrow it, in the scan
    /// rather than in the answer — a question about one meter should not cost a
    /// scan of the portfolio.
    ///
    /// # The range is the whole of what is judged
    ///
    /// Every balancing day between `from` and `to` counts, including days the
    /// channel delivered nothing on — which is what makes the report a check
    /// rather than a summary of what arrived.
    ///
    /// So a range reaching past the last delivery says so: run mid-month over a
    /// whole month, it reports the remainder as missing. Ask about a period that
    /// is over, or set `to` to the last settled instant. Consulting a clock
    /// instead would make one query answer differently over time.
    pub fn completeness(
        &self,
        from: time::OffsetDateTime,
        to: time::OffsetDateTime,
    ) -> super::CompletenessQuery<'_> {
        super::CompletenessQuery::new(self, from, to)
    }

    /// The resolved provider and merge-key discriminators a completeness run needs.
    pub(crate) async fn completeness_inputs(
        &self,
    ) -> Result<(
        std::sync::Arc<dyn datafusion::catalog::TableProvider>,
        String,
        Vec<String>,
    )> {
        let name = self.resolved_table();
        let resolved = self
            .ctx
            .table_provider(name.as_str())
            .await
            .map_err(Error::from)?;
        Ok((resolved, name, self.config.discriminator_columns()))
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

        // A reproducible read of a scoped store stays scoped, and keeps its
        // registry: a boundary that a derived session drops is not a boundary.
        // `to_builder` is where that list lives.
        let mut store = self
            .to_builder()
            .read_mode(ReadMode::AsOf {
                snapshot,
                max_version,
            })
            .build()
            .await?;
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
        self.derive(ReadMode::AsKnownAt(at)).await
    }

    /// The same store, reading a different set of tiers.
    ///
    /// The general form of [`as_known_at`](Self::as_known_at), and the one a
    /// service reaches for when the *caller* chooses:
    /// [`Historical`](ReadMode::Historical) answers a reporting query off the
    /// lake with no load on the operational database, and
    /// [`Operational`](ReadMode::Operational) answers a monitoring query off the
    /// recent window without an Iceberg round trip.
    ///
    /// Everything else about the session carries over — the row scope, the
    /// subject registry, both tiers — because a boundary a derived session drops
    /// is not a boundary.
    ///
    /// [`AsOf`](ReadMode::AsOf) is **refused** here. It has to resolve its
    /// snapshot and walk back the boundary that snapshot published, neither of
    /// which a mode value carries; [`as_of`](Self::as_of) is that constructor and
    /// it takes the arguments it needs.
    pub async fn in_read_mode(&self, mode: ReadMode) -> Result<Self> {
        if let ReadMode::AsOf { .. } = mode {
            return Err(Error::config(
                "a pinned snapshot is not just a read mode: it has to be resolved, and \
                 the boundary it published has to be walked back from it, or a settlement \
                 rerun reports the epoch as the boundary it ran against. Use \
                 MeterStore::as_of, which does both",
            ));
        }
        self.derive(mode).await
    }

    /// A session over the same tiers, table and confinement, in another mode.
    ///
    /// Written once because every derived session has to carry the *same* things
    /// forward, and the failure of forgetting one is silent: a scoped store whose
    /// derived session dropped the scope answers caller-supplied SQL over every
    /// tenant.
    async fn derive(&self, mode: ReadMode) -> Result<Self> {
        self.to_builder().read_mode(mode).build().await
    }

    /// Everything a derived session has to carry forward, as a builder.
    ///
    /// **The single place that list is written**, and the reason it is single:
    /// five sessions derive from a store — [`as_of`](Self::as_of),
    /// [`in_read_mode`](Self::in_read_mode), [`scoped`](Self::scoped),
    /// [`in_own_session`](Self::in_own_session) and
    /// [`MeterCatalog`](super::MeterCatalog)'s rebuild — and a field one of them
    /// forgets is silent. A scoped store whose derived session dropped the scope
    /// answers caller-supplied SQL over every tenant.
    ///
    /// The session is deliberately **not** carried: a derived store gets its own
    /// unless a caller puts it back with [`session`](MeterStoreBuilder::session),
    /// which is what a catalog does to keep its tables in one. Nor is
    /// `pinned_watermark`, which belongs to a resolved snapshot rather than to
    /// the configuration.
    pub(crate) fn to_builder(&self) -> MeterStoreBuilder {
        let mut builder = MeterStoreBuilder::default()
            .hot(Arc::clone(&self.hot))
            .cold(Arc::clone(&self.cold), Arc::clone(&self.cold_provider))
            .table(self.config.clone())
            .read_mode(self.mode)
            .row_scope(self.row_scope.clone());
        if let Some(registry) = self.registry.clone() {
            builder = builder.subject_registry(registry);
        }
        builder
    }

    /// The row scope this store would have if confined to `column = value`.
    ///
    /// The check half of [`scoped`](Self::scoped), separated so a catalog can
    /// run it over **every** table before confining any of them: a scope applied
    /// to three tables and refused by the fourth would otherwise leave a caller
    /// holding a boundary that covers some of their data.
    pub(crate) fn narrowed_scope(
        &self,
        column: &str,
        value: &str,
    ) -> Result<Option<Vec<(String, datafusion::scalar::ScalarValue)>>> {
        let identity = self.config.discriminator_columns();
        if !identity.iter().any(|c| c == column) {
            let declared = match identity.is_empty() {
                true => "none are declared".to_string(),
                false => format!("this table is keyed by [{}]", identity.join(", ")),
            };
            return Err(Error::config(format!(
                "{column:?} is not part of the merge key of {}, so a session cannot be \
                 scoped to it — {declared}. Only a merge-key column partitions readings, \
                 and only then does filtering leave one reading's version history intact: \
                 scoping on an attribute would return a different resolved value, not \
                 fewer rows",
                self.config.name(),
            )));
        }

        // A scoped session can never come to see more than it already could.
        // Re-scoping the same column to a *different* value would do exactly
        // that — hand a tenant-scoped store to less-trusted code and it could
        // widen to any other tenant — so it is refused rather than replaced.
        // Refused rather than narrowed to nothing, too: `tenant = 'a' AND tenant
        // = 'b'` is a caller bug, and answering it with zero rows is the kind of
        // quiet answer this crate declines to give.
        let mut scope = self.row_scope.clone();
        if let Some((_, held)) = scope.iter().find(|(c, _)| c == column) {
            let held = match held {
                datafusion::scalar::ScalarValue::Utf8(Some(v)) => v.as_str(),
                other => {
                    return Err(Error::config(format!(
                        "this session is already scoped on {column:?} to a non-string value \
                     ({other:?}), which cannot be re-scoped"
                    )));
                }
            };
            if held != value {
                return Err(Error::config(format!(
                    "this session is already scoped to {column} = {held:?} and cannot be \
                     re-scoped to {value:?}. A scope only ever narrows: re-pointing one \
                     would let a handle that had been confined to a tenant reach another, \
                     which is the boundary it exists to be. Scope the store it was derived \
                     from instead"
                )));
            }
            // Same value: idempotent, so there is nothing to add.
            return Ok(None);
        }
        scope.push((
            column.to_string(),
            datafusion::scalar::ScalarValue::Utf8(Some(value.to_string())),
        ));
        Ok(Some(scope))
    }

    /// A session confined to one value of a **merge-key column**.
    ///
    /// ```no_run
    /// # async fn f(store: &meterstore::MeterStore, sql: &str, tenant: &str)
    /// #     -> meterstore::Result<()> {
    /// let scoped = store.scoped("tenant", tenant).await?;
    /// let rows = scoped.query(sql).await?;   // caller-supplied SQL, one tenant
    /// # Ok(()) }
    /// ```
    ///
    /// [`query`](Self::query) runs **caller-supplied SQL**, so a service exposing
    /// one has no way to confine the scan; refusing relations by name is a
    /// boundary that holds until someone adds a table. The predicate is injected
    /// into the plan and **enforced** below the projection instead, exactly as a
    /// transaction-time ceiling is — the engine never sees it, so no statement
    /// can omit it, alias around it or `UNION` past it.
    ///
    /// # Only a merge-key column
    ///
    /// That is version resolution rather than taste. A merge-key column
    /// partitions *readings*: filtering before ranking or after gives the same
    /// winner. An attribute column does not — a correction that changed a
    /// Bilanzkreis would have its version history sliced apart by the filter, so
    /// the scoped read would resolve to a value the unscoped read does not
    /// return. Fewer rows is the intent; a different number is not.
    ///
    /// In practice that is the declared identity columns, plus `melo_id` on a
    /// table that [identifies a reading by its
    /// Messlokation](crate::config::TableConfig::identify_by_melo) — which is
    /// how one meter of a Mehrfamilienhaus is handed to code that must not see
    /// the others.
    ///
    /// # It composes, and it does not come off
    ///
    /// [`as_of`](Self::as_of) and [`as_known_at`](Self::as_known_at) on a scoped
    /// store stay scoped. Scoping a second merge-key column narrows further;
    /// re-scoping one already fixed is refused unless the value is identical,
    /// because a handle that could be re-pointed at another tenant is not a
    /// boundary.
    ///
    /// Writes are unaffected: [`append`](Self::append) routes by `from` and
    /// carries the identity in the row.
    pub async fn scoped(&self, column: &str, value: impl Into<String>) -> Result<Self> {
        // The check and the widened scope come from `narrowed_scope`, which a
        // catalog runs over every table before confining any of them — so the
        // rule about which columns may be scoped is stated once for both.
        let Some(scope) = self.narrowed_scope(column, &value.into())? else {
            // Already scoped to this exact value: idempotent.
            return self.in_own_session().await;
        };
        let mut store = self.to_builder().row_scope(scope).build().await?;
        store.pinned_watermark = self.pinned_watermark;
        Ok(store)
    }

    /// The same store over a **fresh session** holding only its own relations.
    ///
    /// The mechanism behind
    /// [`MeterCatalog::isolated`](super::MeterCatalog::isolated), and useful on
    /// its own to detach a store from a session it was built into. Every other
    /// property — read mode, row scope, pinned watermark, registry — is carried
    /// over; only the shared catalog is not.
    pub async fn in_own_session(&self) -> Result<Self> {
        let mut store = self.to_builder().build().await?;
        store.pinned_watermark = self.pinned_watermark;
        Ok(store)
    }

    /// The identity equalities every query in this session is confined to.
    pub fn row_scope(&self) -> &[(String, datafusion::scalar::ScalarValue)] {
        &self.row_scope
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
            .create_tables(
                self.config.name(),
                &self.config.merge_key(),
                &extra,
                self.config.time_model(),
            )
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
    /// **A correction is a new row at a higher `version`, not an update** — MSCONS
    /// corrects a value by versioning it, which is why the resolved `readings`
    /// relation applies latest-version-wins over the raw `readings_versions` one.
    /// A caller expecting update semantics gets an append, and the prior value
    /// stays readable, which is what makes a past settlement reproducible.
    /// Nothing here deletes.
    ///
    /// Writing the same `(merge key, version)` twice is a no-op rather than an
    /// error, because every ingest transport delivers at least once — in **both**
    /// tiers. PostgreSQL gets that from its primary key; the cold tier from a
    /// pre-flight read, since Iceberg has no constraints and a duplicate there is
    /// one version resolution cannot collapse. Writing a *different value* under
    /// an existing version is reported rather than kept: a version identifies one
    /// assertion.
    ///
    /// [`AppendOutcome::displacements`] covers both tiers, so a late correction
    /// reports what it displaced exactly as a current one does.
    ///
    /// # The boundary is read twice, and that is the point
    ///
    /// Routing is decided against a boundary read before the write, and archival
    /// can advance that boundary while the write is in flight — leaving an
    /// interval in PostgreSQL below the watermark, where no query looks. So the
    /// boundary is read **again** afterwards, and an append that lost the race
    /// routes a second time against the fresh one. Both writes are idempotent,
    /// so the second pass restores what the first wrote rather than duplicating
    /// it, and the returned outcome carries both rounds.
    ///
    /// For steady-state ingest of current data, [`hot_writer`](Self::hot_writer)
    /// reads the boundary once per run instead of twice per call, and refuses
    /// anything near it rather than routing — see there for when that trade is
    /// the right one and when it is not.
    pub async fn append(&self, series: &[crate::encode::StoredSeries]) -> Result<AppendOutcome> {
        self.require_current_knowledge("append")?;
        self.require_time_model(crate::config::TimeModel::Interval, "append")?;
        self.check_subject_refs(series).await?;

        self.require_melo(
            series
                .iter()
                .map(|s| (&s.series.malo_id, s.series.melo_id.is_some())),
        )?;

        let mut total = AppendOutcome::default();
        for _ in 0..BOUNDARY_ATTEMPTS {
            let watermark = self.watermark().await?;
            let round = self.append_routed(series, watermark).await?;
            let earliest_hot = series
                .iter()
                .flat_map(|s| s.series.intervals.iter().map(|i| i.from))
                .filter(|from| *from >= watermark.get())
                .min();
            total.absorb(round);

            if !self.boundary_moved_under(watermark, earliest_hot).await? {
                return Ok(total);
            }
        }
        Err(self.boundary_conflict())
    }

    /// [`append`](Self::append) against a boundary the caller has already read.
    async fn append_routed(
        &self,
        series: &[crate::encode::StoredSeries],
        watermark: TieringWatermark,
    ) -> Result<AppendOutcome> {
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
                    last + self.config.archival_step(),
                    self.config.archival_step(),
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
            // Held across the read *and* the write. `append_cold` decides what to
            // append from what is already stored, so two processes doing that
            // concurrently would both find no existing row and both write — the
            // reading stored twice at one version, which resolution cannot
            // collapse. The hot tier gets that exclusion from its primary key;
            // Iceberg has none.
            let lease = self.hot.cold_append_lease(table).await?;
            let written = self.append_cold(&cold, &mut outcome).await;
            if let Err(e) = lease.release().await {
                warn!(
                    table,
                    error = %e,
                    "could not release the cold-append claim; it dies with this session"
                );
            }
            written?;
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

    /// Append a **Zählerstandsgang** — register readings at instants.
    ///
    /// The point-series counterpart of [`append`](Self::append), routing each
    /// reading to the tier its instant belongs to.
    ///
    /// Only a table configured
    /// [`TimeModel::Point`](crate::config::TimeModel::Point) accepts these, and
    /// such a table refuses `append` in return: `value` is a cumulative register
    /// reading here and interval energy there, so one table holding both would
    /// have a column no aggregate could interpret.
    pub async fn append_readings(
        &self,
        readings: &[crate::encode::StoredReadings],
    ) -> Result<AppendOutcome> {
        self.require_current_knowledge("append_readings")?;
        self.require_time_model(crate::config::TimeModel::Point, "append_readings")?;
        self.check_reading_subject_refs(readings).await?;

        self.require_melo(readings.iter().map(|d| (&d.malo_id, d.melo_id.is_some())))?;

        let mut total = AppendOutcome::default();
        for _ in 0..BOUNDARY_ATTEMPTS {
            let watermark = self.watermark().await?;
            let round = self.append_readings_routed(readings, watermark).await?;
            let earliest_hot = readings
                .iter()
                .flat_map(|d| d.readings.iter().map(|r| r.at))
                .filter(|at| *at >= watermark.get())
                .min();
            total.absorb(round);

            if !self.boundary_moved_under(watermark, earliest_hot).await? {
                return Ok(total);
            }
        }
        Err(self.boundary_conflict())
    }

    /// [`append_readings`](Self::append_readings) against a boundary the caller
    /// has already read.
    async fn append_readings_routed(
        &self,
        readings: &[crate::encode::StoredReadings],
        watermark: TieringWatermark,
    ) -> Result<AppendOutcome> {
        let table = self.config.name();

        let mut hot = Vec::new();
        let mut cold = Vec::new();
        for delivery in readings {
            let (below, at_or_above): (Vec<_>, Vec<_>) = delivery
                .readings
                .iter()
                .cloned()
                .partition(|r| r.at < watermark.get());

            if !at_or_above.is_empty() {
                let mut d = delivery.clone();
                d.readings = at_or_above;
                hot.push(d);
            }
            if !below.is_empty() {
                let mut d = delivery.clone();
                d.readings = below;
                cold.push(d);
            }
        }

        let mut outcome = AppendOutcome::default();
        let extra = self.config.extra_columns();

        if !hot.is_empty() {
            let starts = || hot.iter().flat_map(|d| d.readings.iter().map(|r| r.at));
            let (first, last) = (
                starts().min().expect("non-empty"),
                starts().max().expect("non-empty"),
            );
            self.hot
                .ensure_partitions(
                    table,
                    first,
                    last + self.config.archival_step(),
                    self.config.archival_step(),
                )
                .await?;

            let batch = crate::encode::readings_to_record_batch_with(&hot, &extra)?;
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
            // The same claim, the same reason as the interval path: the decision
            // and the write have to be one step, or two processes both find no
            // stored row and both append.
            let lease = self.hot.cold_append_lease(table).await?;
            let written = self.append_cold_readings(&cold, &extra, &mut outcome).await;
            if let Err(e) = lease.release().await {
                warn!(
                    table,
                    error = %e,
                    "could not release the cold-append claim; it dies with this session"
                );
            }
            written?;
        }

        crate::observe::metrics()
            .late_corrections
            .add(outcome.cold_rows, &crate::observe::table(table));

        info!(
            table,
            hot = outcome.hot_rows,
            cold = outcome.cold_rows,
            "readings routed"
        );
        Ok(outcome)
    }

    /// Whether archival moved the tier boundary past a row this append just
    /// wrote to PostgreSQL.
    ///
    /// Routing reads the boundary before the write; archival advances it by
    /// committing to Iceberg, a different system sharing no transaction with the
    /// insert. Detaching a partition before archiving it closes most of the gap
    /// (§8.2), since an insert into one being archived fails outright. What is
    /// left is the moment after the drop, when `ensure_partitions` recreates the
    /// relation and the row lands below the boundary, where no query looks — not
    /// lost, invisible, and reported by nothing but `verify_invariant`.
    ///
    /// So the boundary is read again afterwards, at the cost of a second catalog
    /// load. `true` sends the caller round with the fresh one; both writes are
    /// idempotent, so the second pass restores rather than duplicates.
    /// [`hot_writer`](Self::hot_writer) pays once per run and refuses anything
    /// near the boundary instead.
    async fn boundary_moved_under(
        &self,
        routed_against: TieringWatermark,
        earliest_hot: Option<time::OffsetDateTime>,
    ) -> Result<bool> {
        // Nothing went to the hot tier, so nothing can be below the boundary.
        let Some(earliest) = earliest_hot else {
            return Ok(false);
        };

        let fresh = self.watermark().await?;
        if earliest >= fresh.get() {
            return Ok(false);
        }

        warn!(
            table = self.config.name(),
            routed_against = %routed_against,
            now = %fresh,
            earliest = %earliest,
            "the tier boundary advanced while this append was writing; re-routing"
        );
        Ok(true)
    }

    /// The failure a write that keeps losing to archival ends in.
    fn boundary_conflict(&self) -> Error {
        Error::InvariantViolated {
            table: self.config.name().to_string(),
            detail: format!(
                "the tier boundary advanced under this append {BOUNDARY_ATTEMPTS} times \
                 running. Archival advances the boundary at most one window per commit, so \
                 this means the intervals being written sit exactly where archival is \
                 working — write current data through hot_writer, or wait for the catch-up \
                 to finish"
            ),
        }
    }

    /// Refuse a delivery that names no Messlokation on a table keyed by it.
    ///
    /// The hot table's `NOT NULL` would catch it, and the message would be about
    /// a column constraint rather than about a Marktlokation being measured by
    /// more than one meter. The check is per delivery, not per row: a
    /// Messlokation is a property of the delivery.
    fn require_melo<'a>(
        &self,
        deliveries: impl IntoIterator<Item = (&'a metering::ids::MaloId, bool)>,
    ) -> Result<()> {
        if !self.config.melo_in_merge_key() {
            return Ok(());
        }
        for (malo, named) in deliveries {
            if !named {
                return Err(Error::encode(
                    crate::encode::schema::col::MELO_ID,
                    format!(
                        "{malo}: {} identifies a reading by its Messlokation and this \
                         delivery names none. A Marktlokation may be measured by several, \
                         so without it two meters' registers are one reading. Set it with \
                         with_melo_id, or declare TableConfig::identify_by_melo(false) if \
                         this deployment holds exactly one Messlokation per Marktlokation",
                        self.config.name(),
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Refuse a write whose shape is not the one this table was declared for.
    ///
    /// `value` means interval energy on one and a cumulative register reading on
    /// the other, so a table holding both would carry a column no aggregate
    /// could interpret. The two write paths therefore check rather than adapt.
    fn require_time_model(&self, wanted: crate::config::TimeModel, operation: &str) -> Result<()> {
        let actual = self.config.time_model();
        if actual == wanted {
            return Ok(());
        }
        Err(Error::config(format!(
            "{} is declared {actual}, so {operation} is not the write path for it. \
             `value` is interval energy on an INTERVAL table and a cumulative register \
             reading on a POINT one, so one table cannot hold both: summing the two \
             together produces a number with no meaning that looks exactly like a \
             consumption total. Declare a second table with \
             TableConfig::time_model({wanted})",
            self.config.name(),
        )))
    }

    /// [`check_subject_refs`](Self::check_subject_refs) over a point delivery.
    async fn check_reading_subject_refs(
        &self,
        readings: &[crate::encode::StoredReadings],
    ) -> Result<()> {
        let (Some(column), Some(registry)) = (self.config.subject_column(), &self.registry) else {
            return Ok(());
        };
        let mut refs = std::collections::BTreeSet::new();
        for delivery in readings {
            if let Some(datafusion::scalar::ScalarValue::Utf8(Some(value))) =
                delivery.extra.get(column)
            {
                refs.insert(value.clone());
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

    /// Append values **the operator authors**, ensuring each one becomes
    /// current.
    ///
    /// [`append`](Self::append) records a **delivery** — something a market
    /// partner sent, at the version they assigned — and being outranked by a
    /// newer one is the correct outcome for it. A value the *store's own
    /// operator* authors is not that: a § 60 Abs. 2 MsbG Ersatzwert, a
    /// correction after a dispute, a manual entry after a meter exchange must
    /// take effect **or be refused**. Through `append` such a value is silently
    /// shadowed — stored, audited, confirmed, and never current — which is
    /// precisely the case Ersatzwertbildung exists for, since the interval it
    /// replaces arrived under a real MSCONS version.
    ///
    /// The version each row carries is a *floor*. Where a higher one already
    /// holds the reading, this re-appends at
    /// [`ScopedVersion::next`](crate::version::ScopedVersion::next) of the one in
    /// force — continuing the **stored** sequence under the **stored** scope,
    /// because a version is comparable only within its own and the hot tier
    /// refuses a second network operator for one reading. That decision comes
    /// from [`Displacement::superseded`], which the write itself observed inside
    /// its own transaction; a read-then-write cannot say that.
    ///
    /// Every returned displacement is `Inserted` or `Superseded`, and
    /// [`Displacement::written`] carries the version the row actually landed at —
    /// which is what an audit trail records, not the one the caller asked for.
    /// Re-asserting a value already in force is a no-op rather than a version
    /// bump.
    ///
    /// # Errors
    ///
    /// After [`AUTHORITATIVE_ATTEMPTS`] rounds that still fail to take effect.
    /// That means another writer is authoring the same reading continuously,
    /// which is a conflict for an operator rather than something to retry
    /// through.
    ///
    /// [`Displacement::superseded`]: crate::session::Displacement::superseded
    /// [`Displacement::written`]: crate::session::Displacement::written
    pub async fn append_authoritative(
        &self,
        series: &[crate::encode::StoredSeries],
    ) -> Result<AppendOutcome> {
        use crate::session::Effect;

        self.require_current_knowledge("append_authoritative")?;

        let discriminators = self.config.discriminator_columns();
        let mut pending: Vec<crate::encode::StoredSeries> = series.to_vec();
        let mut outcome = AppendOutcome::default();

        for _ in 0..AUTHORITATIVE_ATTEMPTS {
            if pending.iter().all(|s| s.series.intervals.is_empty()) {
                return Ok(outcome);
            }

            let round = self.append(&pending).await?;
            outcome.hot_rows += round.hot_rows;
            outcome.cold_rows += round.cold_rows;

            // Everything that took effect is final. Everything that did not is
            // re-authored one version above whatever beat it.
            let mut retry: Vec<crate::encode::StoredSeries> = Vec::new();
            for displacement in round.displacements {
                if displacement.effect.changed_current_value() {
                    outcome.displacements.push(displacement);
                    continue;
                }
                // A duplicate whose stored value is already what we are
                // asserting *is* current, so there is nothing to take effect.
                let holds = displacement
                    .superseded
                    .as_ref()
                    .is_some_and(|p| p.value == displacement.written.value);
                if displacement.effect == Effect::Duplicate && holds {
                    outcome.displacements.push(displacement);
                    continue;
                }
                retry.push(reauthored(&pending, &discriminators, &displacement)?);
            }

            if retry.is_empty() {
                return Ok(outcome);
            }
            pending = merge_authored(retry);
        }

        Err(Error::InvariantViolated {
            table: self.config.name().to_string(),
            detail: format!(
                "{AUTHORITATIVE_ATTEMPTS} rounds of append_authoritative still did not take \
                 effect: another writer is authoring the same readings continuously. That is \
                 a conflict between two authors rather than something to retry through"
            ),
        })
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
    /// The writer **refuses** any interval below the boundary it was opened at
    /// rather than routing it, which is the whole safety argument: routing on a
    /// stale boundary could place a row below the true watermark, where no query
    /// looks; refusing cannot. The refusal names the interval and points at
    /// [`append`](Self::append).
    ///
    /// The watermark is monotonic (§6.3), so the risk is a row accepted here that
    /// the true boundary has since passed. The margin is the **settlement lag** —
    /// archival never closes a window newer than `now - settlement_lag`, a week
    /// by default, while current data has `from` near now. Reopen per ingest run
    /// rather than caching a writer for the process lifetime;
    /// [`verify_invariant`](Self::verify_invariant) is the backstop.
    ///
    /// That margin is thinnest in a **backfill**, whose `from` is old, and a
    /// **catch-up**, where archival advances several windows at once. Neither is
    /// what this writer is for: [`append`](Self::append) re-reads the boundary
    /// afterwards and re-routes, which is the cost this method exists to avoid.
    ///
    /// # This is also the write contract
    ///
    /// Writing with your own driver means reproducing the hot schema, its primary
    /// key, the canonical-OBIS `CHECK`, the Sparte/unit code lists and the
    /// intra-version overlap exclusion — a contract carried in prose, where drift
    /// shows up as readings that silently fail to supersede. Going through
    /// [`StoredSeries`] makes it compiler-checked instead.
    ///
    /// [`StoredSeries`]: crate::encode::StoredSeries
    pub async fn hot_writer(&self) -> Result<HotWriter<'_>> {
        self.require_current_knowledge("hot_writer")?;
        self.require_time_model(crate::config::TimeModel::Interval, "hot_writer")?;
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
    /// Three years is a **ceiling**, and the operative trigger is earlier — the
    /// opposite of a retention mandate. A store built to keep personal metering
    /// values for three years *because the law says so* has it inverted.
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
    ///
    /// # This table only
    ///
    /// The subject map is deployment-wide — one table keyed by natural identifier
    /// — so two meterstore tables registering the same identifier share one
    /// reference and one erasure unlinks **both**. A deployment holding more than
    /// one stream must therefore sweep with [`MeterCatalog::anonymise_before`],
    /// which takes the latest reading across every table; this one would destroy
    /// a linkage the other tables still depend on.
    ///
    /// [`MeterCatalog::anonymise_before`]: crate::MeterCatalog::anonymise_before
    pub async fn anonymise_before(
        &self,
        cutoff: time::OffsetDateTime,
        reason: &str,
        actor: &str,
        now: time::OffsetDateTime,
    ) -> Result<Vec<crate::erasure::ErasureRecord>> {
        // Erasure is irreversible and its due-date is `max(from)` over *this*
        // session, so a restricted view does not misreport here — it destroys.
        self.require_current_knowledge("anonymise_before")?;
        let registry = self.require_registry()?;
        if self.config.subject_column().is_none() {
            return Err(Error::config(
                "no subject column is declared, so there is no linkage to destroy: \
                 without one the stored readings carry no reference to a person and \
                 § 60 Abs. 6 has nothing to act on here",
            ));
        }

        let seen = self.subject_last_seen().await?.unwrap_or_default();
        let due: Vec<String> = seen
            .into_iter()
            .filter(|(_, last)| *last < cutoff)
            .map(|(reference, _)| reference)
            .collect();

        let erased = crate::erasure::anonymise(registry, &due, reason, actor, now).await?;
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

    /// The latest interval each pseudonymous reference in this table explains.
    ///
    /// `None` where the table declares no subject column, which is not the same
    /// as an empty map: one says the question does not apply here, the other
    /// that it does and nothing is attributed. A retention sweep across several
    /// tables has to tell them apart, because a subject is only due once **every**
    /// table that names it has passed the ceiling.
    ///
    /// The **raw** relation, not the resolved one: a superseded version is still
    /// a stored personal value, so a subject whose only recent row is a
    /// correction that lost resolution has not passed the ceiling.
    pub(crate) async fn subject_last_seen(
        &self,
    ) -> Result<Option<std::collections::BTreeMap<String, time::OffsetDateTime>>> {
        let Some(column) = self.config.subject_column() else {
            return Ok(None);
        };

        let sql = format!(
            r#"SELECT "{column}" AS reference, max("{from}") AS last_seen FROM {raw}
               WHERE "{column}" IS NOT NULL
               GROUP BY 1"#,
            raw = raw_name(self.config.name()),
            from = crate::encode::schema::col::FROM,
        );
        let rows = self.query(&sql).await?;

        let mut seen = std::collections::BTreeMap::new();
        for batch in rows.batches() {
            let references = column_str(batch, "reference")?;
            let last = batch
                .column_by_name("last_seen")
                .and_then(|c| {
                    c.as_any()
                        .downcast_ref::<crate::arrow::array::TimestampMicrosecondArray>()
                })
                .ok_or_else(|| Error::decode("last_seen", "expected a microsecond timestamp"))?;
            for i in 0..batch.num_rows() {
                if crate::arrow::array::Array::is_null(references, i)
                    || crate::arrow::array::Array::is_null(last, i)
                {
                    continue;
                }
                let at = crate::encode::schema::instant(last.value(i))?;
                seen.entry(references.value(i).to_string())
                    .and_modify(|held: &mut time::OffsetDateTime| *held = (*held).max(at))
                    .or_insert(at);
            }
        }
        Ok(Some(seen))
    }

    /// Refuse an operation whose *decision* is a query against this session,
    /// when the session is not reading current best knowledge.
    ///
    /// [`as_of`](Self::as_of) and [`as_known_at`](Self::as_known_at) return a
    /// store pinned to a past state, and [`Historical`]/[`Operational`] restrict
    /// which tiers are read. Those are **reading** postures, and two kinds of
    /// operation must not run through one.
    ///
    /// **The write path.** Every check it makes — that a replay is a replay, that
    /// a reading carries one network operator, what the write displaced — is a
    /// query against this session, so it would be answered from the pinned or
    /// half-visible view rather than from what is actually stored. The rows would
    /// land in the real table; only the reasoning about them would be wrong.
    ///
    /// **The retention sweep**, where the consequence is worse. It erases a
    /// subject whose latest reading predates a cutoff, and that latest reading is
    /// `max(from)` over *this session*. Under [`Historical`] the hot window is
    /// invisible, so a subject metered daily looks last-seen at the final
    /// archived interval — old enough to erase, while it is still live. Erasure
    /// has no recovery path, which makes this the one place a restricted view
    /// destroys data rather than merely misreporting it.
    ///
    /// The store the pinned one was derived from is unrestricted, so the fix is
    /// always to hold on to it rather than to reach through the derived handle.
    ///
    /// [`Historical`]: crate::planner::ReadMode::Historical
    /// [`Operational`]: crate::planner::ReadMode::Operational
    pub(crate) fn require_current_knowledge(&self, operation: &str) -> Result<()> {
        if self.mode == ReadMode::Unified {
            return Ok(());
        }
        Err(Error::config(format!(
            "{operation} is not available on a session in {:?} mode: this store reads a \
             pinned or restricted view, and the decision it makes is a query against what \
             the session can see — a replay, a second network operator, a displacement \
             report and a subject's latest reading would all be settled from the wrong \
             state. Use the store this one was derived from",
            self.mode,
        )))
    }

    fn require_registry(&self) -> Result<&crate::erasure::SubjectRegistry> {
        self.registry.as_ref().ok_or_else(|| {
            Error::config(
                "no subject registry is configured: declare a subject column and \
                 pass a registry to the builder",
            )
        })
    }

    /// [`append_cold`](Self::append_cold) for a point delivery.
    ///
    /// The reconciliation is shared — a register reading differs from an
    /// interval only in having no span end — so this is the encode and the write.
    async fn append_cold_readings(
        &self,
        cold: &[crate::encode::StoredReadings],
        extra: &[crate::arrow::datatypes::Field],
        outcome: &mut AppendOutcome,
    ) -> Result<()> {
        let table = self.config.name();
        let identity = self.config.discriminator_columns();
        let reconciled = self.reconcile_cold(reading_rows(cold, &identity)?).await?;

        let keep: std::collections::HashSet<ColdKey> = reconciled
            .iter()
            .filter(|o| o.write)
            .map(|o| ColdKey::of(&o.displacement))
            .collect();

        crate::observe::metrics().rows_deduplicated.add(
            reconciled.iter().filter(|o| !o.write).count() as u64,
            &crate::observe::table(table),
        );

        let cold = retain_readings(cold.to_vec(), &identity, &keep)?;
        outcome
            .displacements
            .extend(reconciled.into_iter().map(|o| o.displacement));

        if cold.is_empty() {
            return Ok(());
        }

        // Sorted into the order the Parquet footer declares. Archival gets that
        // from the hot scan's keyset cursor; a correction is written straight
        // from the delivery, so it has to be put there — see
        // `encode::sorted_for_storage`.
        let batch = crate::encode::sorted_for_storage(
            &crate::encode::readings_to_record_batch_with(&cold, extra)?,
        )?;
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
        Ok(())
    }

    /// Reconcile a late-correction batch against what is stored, then append
    /// what is left.
    ///
    /// The caller holds the cold-append lease across this, because the decision
    /// and the write have to be one step.
    async fn append_cold(
        &self,
        cold: &[crate::encode::StoredSeries],
        outcome: &mut AppendOutcome,
    ) -> Result<()> {
        let table = self.config.name();
        let reconciled = self
            .reconcile_cold(interval_rows(cold, &self.config.discriminator_columns())?)
            .await?;

        // Rows already stored at this `(merge key, version)` are dropped rather
        // than appended: resolution ranks one row per version, so a duplicate
        // makes two winners and doubles every sum over the interval.
        let keep: std::collections::HashSet<ColdKey> = reconciled
            .iter()
            .filter(|o| o.write)
            .map(|o| ColdKey::of(&o.displacement))
            .collect();

        // The same instrument as the hot tier's `ON CONFLICT` skips, so
        // `write.rows_deduplicated` is the redelivery rate for the store rather
        // than for one of its halves.
        crate::observe::metrics().rows_deduplicated.add(
            reconciled.iter().filter(|o| !o.write).count() as u64,
            &crate::observe::table(table),
        );

        let cold = retain_intervals(cold.to_vec(), &self.config.discriminator_columns(), &keep)?;
        outcome
            .displacements
            .extend(reconciled.into_iter().map(|o| o.displacement));

        if cold.is_empty() {
            return Ok(());
        }

        // Sorted into the order the Parquet footer declares, for the reason
        // `encode::sorted_for_storage` gives: archival satisfies it through the
        // hot scan's keyset cursor, a correction is written in delivery order
        // and would otherwise declare an order it is not in.
        let batch = crate::encode::sorted_for_storage(&crate::encode::to_record_batch_with(
            &cold,
            &self.config.extra_columns(),
        )?)?;
        // Appended without moving the watermark: the boundary is about which
        // range each tier owns, and a correction does not change that. The batch
        // is in memory, so the bloom-filter hint is exact.
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
        Ok(())
    }

    /// Read what the cold tier already holds for the readings a delivery
    /// asserts, and decide what may be written.
    ///
    /// The hot tier gets this from the database: a primary key on
    /// `(merge key, version)` makes a redelivery a no-op, and an exclusion
    /// constraint refuses a second network operator for one reading. **Iceberg
    /// has no constraints**, and [`append`](Self::append) routes a
    /// below-watermark interval straight to it — so a late correction, the
    /// delivery most likely to carry a stale operator *and* most likely to be
    /// replayed, would otherwise reach the one tier that cannot refuse either.
    ///
    /// Three outcomes, and the prior state that produced them:
    ///
    /// - **A second network operator** for one reading. Versions from two scopes
    ///   are incomparable, so both survive resolution and every sum doubles.
    /// - **A redelivery** — already stored at this exact `(merge key, version)`.
    ///   Reported [`Duplicate`](crate::session::Effect::Duplicate) and dropped,
    ///   because resolution ranks one row per *version* and a duplicate makes
    ///   two winners.
    /// - **A different value under an existing version**, which is a producer
    ///   error: a version identifies one assertion.
    ///
    /// # Keyed on the full merge key
    ///
    /// Identity columns included, as the hot tier's exclusion constraint is. The
    /// coarser `(malo_id, obis_code, from)` looks safer and is not: it would drop
    /// a second tenant's reading as a duplicate of the first.
    ///
    /// # Cost
    ///
    /// One query, scoped to the measuring points and interval range being
    /// written, so it is proportional to the correction rather than to the
    /// history.
    async fn reconcile_cold(&self, incoming: Vec<IncomingRow>) -> Result<Vec<ColdOutcome>> {
        use crate::session::{Displacement, Effect, StoredValue};
        use datafusion::scalar::ScalarValue;

        let identity = self.config.discriminator_columns();
        let mut malo_ids: std::collections::BTreeSet<String> = Default::default();
        let (mut lo, mut hi) = (None::<OffsetDateTime>, None::<OffsetDateTime>);
        for row in &incoming {
            malo_ids.insert(row.key.malo_id.clone());
            lo = Some(lo.map_or(row.key.from, |v: OffsetDateTime| v.min(row.key.from)));
            hi = Some(hi.map_or(row.key.from, |v: OffsetDateTime| v.max(row.key.from)));
        }

        let (Some(lo), Some(hi)) = (lo, hi) else {
            return Ok(Vec::new());
        };

        // Every stored version for the affected readings, not just the winner:
        // the winner alone cannot say whether *this* version is already present,
        // which is what separates a replay from a backfill.
        //
        // Parameterised throughout — a `malo_id` reaching here came off a market
        // message (§19.7).
        let mut params: Vec<ScalarValue> = vec![
            crate::encode::schema::timestamp_scalar(lo),
            crate::encode::schema::timestamp_scalar(hi),
        ];
        let mut placeholders = Vec::with_capacity(malo_ids.len());
        for (i, malo) in malo_ids.iter().enumerate() {
            params.push(ScalarValue::Utf8(Some(malo.clone())));
            placeholders.push(format!("${}", i + 3));
        }

        use crate::encode::schema::col;
        // `melo_id` is selected unconditionally above — it is compared on every
        // table, keyed by it or not — so listing it again here would put two
        // columns of one name in the result schema.
        let identity_select = identity
            .iter()
            .filter(|c| c.as_str() != col::MELO_ID)
            .map(|c| format!(r#", "{c}""#))
            .collect::<String>();
        let sql = format!(
            r#"SELECT "{malo}", "{obis}", "{from}", "{value}", "{unit}", "{quality}",
                      "{version}", "{scope}", "{recorded}", "{melo}"{identity_select}
               FROM {raw}
               WHERE "{from}" >= $1 AND "{from}" <= $2
                 AND "{malo}" IN ({places})"#,
            malo = col::MALO_ID,
            melo = col::MELO_ID,
            obis = col::OBIS_CODE,
            from = col::FROM,
            value = col::VALUE,
            unit = col::UNIT,
            quality = col::QUALITY,
            version = col::VERSION,
            scope = col::VERSION_SCOPE,
            recorded = col::RECORDED_AT,
            raw = raw_name(self.config.name()),
            places = placeholders.join(", "),
        );

        let existing = self.query_with_params(&sql, params).await?;

        // The highest version stored per reading, and every version stored for
        // it: the first decides displacement, the second decides replay.
        let mut current: std::collections::HashMap<ColdKey, StoredValue> = Default::default();
        let mut stored_versions: std::collections::HashMap<(ColdKey, u128), StoredValue> =
            Default::default();

        // What the cold tier already names as the Messlokation of each stored
        // version. Compared below for the same reason the hot tier's divergence
        // check asks about it: on a table not keyed by Messlokation, two meters
        // under one Marktlokation share a merge key, and a replay check that
        // ignored the column would drop the second meter's register as a
        // redelivery of the first — silently, whenever the two readings agree.
        let mut stored_melo: std::collections::HashMap<(ColdKey, u128), Option<String>> =
            Default::default();

        for batch in existing.batches() {
            for row in decode_cold_rows(batch, &identity)? {
                let (key, held, melo) = row;
                stored_melo.insert((key.clone(), held.version.version().get()), melo);
                match current.get(&key) {
                    // Two scopes already stored for one reading: the versions are
                    // not comparable, so both survive resolution and every sum
                    // over them doubles. The table is already in that state, so
                    // this is reported rather than attributed to the delivery.
                    Some(best) if best.version.scope() != held.version.scope() => {
                        return Err(Error::InvariantViolated {
                            table: self.config.name().to_string(),
                            detail: format!(
                                "reading {} {} at {} is stored under two version scopes, \
                                 {} and {} — versions are comparable only within one scope, \
                                 so both survive resolution and double every sum over them",
                                key.malo_id,
                                key.obis_code,
                                key.from,
                                best.version.scope(),
                                held.version.scope(),
                            ),
                        });
                    }
                    Some(best)
                        if best.version.try_cmp(&held.version)? != std::cmp::Ordering::Less => {}
                    _ => {
                        current.insert(key.clone(), held.clone());
                    }
                }
                stored_versions.insert((key, held.version.version().get()), held);
            }
        }

        let mut out = Vec::with_capacity(incoming.len());
        for row in incoming {
            let version = row.written.version.version().get();

            if let Some(held) = current.get(&row.key)
                && held.version.scope().operator() != row.written.version.scope().operator()
            {
                return Err(Error::config(format!(
                    "reading {} {} at {} is already stored under network operator {} \
                     but this delivery asserts {}. A version is comparable only \
                     within its (operator, month) scope, so both would survive resolution \
                     and double every sum over them. Check that the scope carries the \
                     *network operator* rather than a forwarding party or a tenant",
                    row.key.malo_id,
                    row.key.obis_code,
                    row.key.from,
                    held.version.scope().operator(),
                    row.written.version.scope().operator(),
                )));
            }

            // Already stored at this exact version. A replay is ordinary; a
            // restated value is a producer error.
            if let Some(held) = stored_versions.get(&(row.key.clone(), version)) {
                if let Some(stored) = stored_melo.get(&(row.key.clone(), version))
                    && *stored != row.melo
                {
                    // A delivery the store refused, not a state the store is in:
                    // the cold-tier twin of the hot path's `melo_id` check.
                    return Err(Error::IntegrityViolation {
                        table: self.config.name().to_string(),
                        constraint: Some("melo_identifies_the_reading".to_string()),
                        detail: format!(
                            "reading {} {} at {} is already stored at version {version} for \
                             Messlokation {:?} but this delivery names {:?}. A Marktlokation \
                             may be measured by several Messlokationen, and this table does \
                             not identify a reading by its — so two meters' registers share \
                             a merge key and one of them is read as a replay of the other. \
                             Declare TableConfig::identify_by_melo(true)",
                            row.key.malo_id,
                            row.key.obis_code,
                            row.key.from,
                            stored.as_deref().unwrap_or("<none>"),
                            row.melo.as_deref().unwrap_or("<none>"),
                        ),
                    });
                }
                if held.value != row.written.value {
                    return Err(Error::IntegrityViolation {
                        table: self.config.name().to_string(),
                        constraint: Some("version_identifies_one_assertion".to_string()),
                        detail: format!(
                            "reading {} {} at {} is already stored at version {version} with \
                             value {} but this delivery restates it as {} — a version \
                             identifies one assertion, so a corrected value needs a higher \
                             version",
                            row.key.malo_id,
                            row.key.obis_code,
                            row.key.from,
                            held.value,
                            row.written.value,
                        ),
                    });
                }
                out.push(ColdOutcome {
                    displacement: Displacement {
                        malo_id: row.key.malo_id.clone(),
                        obis_code: row.key.obis_code.clone(),
                        from: row.key.from,
                        to: row.to,
                        identity: row.key.identity.clone(),
                        effect: Effect::Duplicate,
                        superseded: current.get(&row.key).cloned(),
                        written: row.written,
                    },
                    write: false,
                });
                continue;
            }

            let prior = current.get(&row.key).cloned();
            let effect = match &prior {
                None => Effect::Inserted,
                Some(p) => match p.version.try_cmp(&row.written.version)? {
                    std::cmp::Ordering::Less => Effect::Superseded,
                    _ => Effect::Shadowed,
                },
            };
            if effect.changed_current_value() {
                current.insert(row.key.clone(), row.written.clone());
            }
            // Recorded before the write, so a second interval in this same batch
            // asserting the same `(merge key, version)` is seen as the replay it
            // is rather than appended twice — Iceberg would keep both.
            stored_versions.insert((row.key.clone(), version), row.written.clone());

            out.push(ColdOutcome {
                displacement: Displacement {
                    malo_id: row.key.malo_id.clone(),
                    obis_code: row.key.obis_code.clone(),
                    from: row.key.from,
                    to: row.to,
                    identity: row.key.identity,
                    effect,
                    superseded: prior,
                    written: row.written,
                },
                write: true,
            });
        }

        Ok(out)
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

    /// The column an external engine must group a daily aggregate by.
    ///
    /// The second rule that has to leave this crate for the open-format claim to
    /// hold, after [`resolution_sql`](Self::resolution_sql) — except that this
    /// one leaves as **data** rather than as an expression, which is the whole
    /// point. Gas is balanced on the 06:00–06:00 Gastag, and no SQL expresses
    /// that portably (see [`encode::schema`]), so the rule is applied once at
    /// write time and every reader groups on the answer:
    ///
    /// ```sql
    /// SELECT balancing_day, SUM(value) FROM readings GROUP BY 1;
    /// ```
    ///
    /// [`encode::schema`]: crate::encode::schema
    pub const fn balancing_day_column(&self) -> &'static str {
        crate::encode::schema::col::BALANCING_DAY
    }
}

/// Refuse a statement that is not a query.
///
/// Walks the whole plan rather than its root: `EXPLAIN` and `ANALYZE` wrap
/// another one, and planning a `COPY` is what performs it.
///
/// This closes the path from *caller-supplied* SQL — a Flight SQL client, an
/// ad-hoc endpoint — to the filesystem and to the session other tenants' tables
/// live in. A caller holding the store itself still has
/// [`MeterStore::context`](MeterStore::context).
fn require_read_only(plan: &datafusion::logical_expr::LogicalPlan) -> Result<()> {
    use datafusion::logical_expr::LogicalPlan;

    let refusal = |kind: &str, reaches: &str| {
        Err(Error::config(format!(
            "{kind} is not accepted here: this surface runs queries, and a statement that \
             {reaches} would step past the row scope and table isolation that make \
             caller-supplied SQL safe to run. Use MeterStore::context for a session with \
             no such boundary, and MeterStore::append to write readings"
        )))
    };

    match plan {
        LogicalPlan::Ddl(_) => {
            return refusal(
                "DDL",
                "registers a relation — an external table over the warehouse's own \
                 Parquet reads every tenant's rows, and never touches the provider that \
                 enforces a scope",
            );
        }
        LogicalPlan::Dml(_) => {
            return refusal(
                "DML",
                "writes rows outside the tier routing, so a correction for an archived \
                 interval would land where no query reads it",
            );
        }
        LogicalPlan::Copy(_) => {
            return refusal("COPY", "writes a file wherever the process can write");
        }
        LogicalPlan::Statement(statement) => {
            return refusal(
                &format!("the statement {}", statement.name()),
                "changes the session rather than reading from it",
            );
        }
        // Recursed into by name as well as through `inputs()`, which does expose
        // both today. `EXPLAIN COPY (…) TO '…'` is the shape that matters —
        // planning it is what performs it — and a check whose one job is to be
        // exhaustive should not rest on a pre-1.0 dependency continuing to list a
        // wrapper's inner plan.
        LogicalPlan::Explain(explain) => return require_read_only(&explain.plan),
        LogicalPlan::Analyze(analyze) => return require_read_only(&analyze.input),
        _ => {}
    }

    for input in plan.inputs() {
        require_read_only(input)?;
    }
    Ok(())
}

/// What names one reading in the cold tier: the full merge key.
///
/// The deployment's identity columns are part of it, exactly as they are part of
/// the hot table's primary key. Keyed on the coarser `(malo_id, obis_code, from)`
/// this would treat a second tenant's reading as a duplicate of the first, which
/// is data loss rather than deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ColdKey {
    malo_id: String,
    obis_code: String,
    from: OffsetDateTime,
    /// Identity column values, in configuration order.
    identity: Vec<(String, String)>,
}

impl ColdKey {
    /// The key a reported displacement describes.
    fn of(d: &crate::session::Displacement) -> Self {
        Self {
            malo_id: d.malo_id.clone(),
            obis_code: d.obis_code.clone(),
            from: d.from,
            identity: d.identity.clone(),
        }
    }
}

/// One interval a late correction asserts, before it is reconciled.
struct IncomingRow {
    key: ColdKey,
    /// `None` for a register reading, which has no span end.
    to: Option<OffsetDateTime>,
    /// The Messlokation the delivery names, whether or not it is in the key.
    ///
    /// Carried even where it is not part of the identity, because that is
    /// exactly when it has to be compared: two Messlokationen under one
    /// Marktlokation then share a merge key, and reconciliation would read the
    /// second meter's register as a replay of the first.
    melo: Option<String>,
    written: crate::session::StoredValue,
}

/// What [`MeterStore::reconcile_cold`] decided about one asserted interval.
struct ColdOutcome {
    displacement: crate::session::Displacement,
    /// Whether the row still has to be appended, or is already stored.
    write: bool,
}

/// The rows an interval delivery asserts, for reconciliation.
fn interval_rows(
    cold: &[crate::encode::StoredSeries],
    identity: &[String],
) -> Result<Vec<IncomingRow>> {
    let mut out = Vec::new();
    for stored in cold {
        // Rendered once per delivery: the identifiers are the same for every row.
        let malo = stored.series.malo_id.to_string();
        let melo = stored.series.melo_id.as_ref().map(ToString::to_string);
        let ident = discriminator_values(stored.series.melo_id.as_ref(), &stored.extra, identity)?;
        for interval in &stored.series.intervals {
            let code = interval
                .obis_code
                .or(stored.series.obis_code)
                .ok_or_else(|| {
                    Error::encode(
                        crate::encode::schema::col::OBIS_CODE,
                        format!("neither interval nor series {malo} carries one"),
                    )
                })?;
            out.push(IncomingRow {
                key: ColdKey {
                    malo_id: malo.clone(),
                    obis_code: crate::encode::canonical_obis(&code.to_string())?,
                    from: interval.from,
                    identity: ident.clone(),
                },
                to: Some(interval.to),
                melo: melo.clone(),
                written: crate::session::StoredValue {
                    value: interval.value,
                    unit: stored.unit,
                    quality: interval.quality,
                    version: stored.version.clone(),
                    recorded_at: stored.recorded_at,
                },
            });
        }
    }
    Ok(out)
}

/// The rows a point delivery asserts, for reconciliation.
///
/// The same shape as [`interval_rows`], with no span end — which is the whole of
/// what a register reading differs by, so the reconciliation itself is shared.
fn reading_rows(
    cold: &[crate::encode::StoredReadings],
    identity: &[String],
) -> Result<Vec<IncomingRow>> {
    let mut out = Vec::new();
    for stored in cold {
        let malo = stored.malo_id.to_string();
        let melo = stored.melo_id.as_ref().map(ToString::to_string);
        let ident = discriminator_values(stored.melo_id.as_ref(), &stored.extra, identity)?;
        let delivery_obis = stored.obis_code;
        for reading in &stored.readings {
            let code = reading.obis_code.unwrap_or(delivery_obis);
            out.push(IncomingRow {
                key: ColdKey {
                    malo_id: malo.clone(),
                    obis_code: crate::encode::canonical_obis(&code.to_string())?,
                    from: reading.at,
                    identity: ident.clone(),
                },
                to: None,
                melo: melo.clone(),
                written: crate::session::StoredValue {
                    value: reading.value,
                    unit: stored.unit,
                    quality: reading.quality,
                    version: stored.version.clone(),
                    recorded_at: stored.recorded_at,
                },
            });
        }
    }
    Ok(out)
}

/// The merge key's discriminator values for one delivery, in key order.
///
/// `melo_id` is read from the delivery itself rather than from `extra`: it is a
/// core column, and a table that
/// [identifies by it](crate::config::TableConfig::identify_by_melo) puts it in
/// the key alongside the declared identity columns.
pub(crate) fn discriminator_values(
    melo: Option<&metering::ids::MeloId>,
    supplied: &std::collections::BTreeMap<String, datafusion::scalar::ScalarValue>,
    columns: &[String],
) -> Result<Vec<(String, String)>> {
    columns
        .iter()
        .map(|name| {
            let value = match name.as_str() {
                crate::encode::schema::col::MELO_ID => {
                    melo.map(metering::ids::MeloId::to_string).ok_or_else(|| {
                        Error::encode(
                            crate::encode::schema::col::MELO_ID,
                            "this table identifies a reading by its Messlokation, and this \
                             delivery names none — a Marktlokation may be measured by \
                             several, so without it two meters' registers are one reading",
                        )
                    })?
                }
                _ => identity_value(supplied, name)?,
            };
            Ok((name.clone(), value))
        })
        .collect()
}

/// The value of a declared identity column on a series.
///
/// Identity columns are validated non-nullable and `Utf8`, so anything else is a
/// caller error rather than a state to tolerate: a missing tenant would otherwise
/// key the reading as a different reading from the one it corrects.
fn identity_value(
    supplied: &std::collections::BTreeMap<String, datafusion::scalar::ScalarValue>,
    name: &str,
) -> Result<String> {
    match supplied.get(name) {
        Some(datafusion::scalar::ScalarValue::Utf8(Some(value))) => Ok(value.clone()),
        Some(other) => Err(Error::encode(
            name,
            format!("identity column must be a non-null string, got {other:?}"),
        )),
        None => Err(Error::encode(
            name,
            "identity column has no value on this series, and it is part of what \
             names the reading",
        )),
    }
}

/// Keep only the intervals whose key is in `keep`, dropping emptied series.
///
/// The batch is rebuilt from the surviving intervals rather than written whole
/// and filtered afterwards, because the filtering decision is per interval and
/// the encoder works per series.
fn retain_intervals(
    cold: Vec<crate::encode::StoredSeries>,
    identity: &[String],
    keep: &std::collections::HashSet<ColdKey>,
) -> Result<Vec<crate::encode::StoredSeries>> {
    let mut out = Vec::with_capacity(cold.len());
    for mut stored in cold {
        let malo = stored.series.malo_id.to_string();
        let ident = discriminator_values(stored.series.melo_id.as_ref(), &stored.extra, identity)?;
        let series_obis = stored.series.obis_code;
        let mut kept = Vec::with_capacity(stored.series.intervals.len());
        for interval in std::mem::take(&mut stored.series.intervals) {
            // Unreachable: `reconcile_cold` has already refused a batch whose
            // interval names no channel. Spelled as an error rather than a
            // `continue` all the same — dropping a row here would be silent, and
            // silently losing a reading is the failure this whole path exists to
            // stop.
            let code = interval.obis_code.or(series_obis).ok_or_else(|| {
                Error::encode(
                    crate::encode::schema::col::OBIS_CODE,
                    format!("neither interval nor series {malo} carries one"),
                )
            })?;
            let key = ColdKey {
                malo_id: malo.clone(),
                obis_code: crate::encode::canonical_obis(&code.to_string())?,
                from: interval.from,
                identity: ident.clone(),
            };
            if keep.contains(&key) {
                kept.push(interval);
            }
        }
        if !kept.is_empty() {
            stored.series.intervals = kept;
            out.push(stored);
        }
    }
    Ok(out)
}

/// [`retain_intervals`] for a point delivery.
fn retain_readings(
    cold: Vec<crate::encode::StoredReadings>,
    identity: &[String],
    keep: &std::collections::HashSet<ColdKey>,
) -> Result<Vec<crate::encode::StoredReadings>> {
    let mut out = Vec::with_capacity(cold.len());
    for mut stored in cold {
        let malo = stored.malo_id.to_string();
        let ident = discriminator_values(stored.melo_id.as_ref(), &stored.extra, identity)?;
        let delivery_obis = stored.obis_code;
        let mut kept = Vec::with_capacity(stored.readings.len());
        for reading in std::mem::take(&mut stored.readings) {
            let code = reading.obis_code.unwrap_or(delivery_obis);
            let key = ColdKey {
                malo_id: malo.clone(),
                obis_code: crate::encode::canonical_obis(&code.to_string())?,
                from: reading.at,
                identity: ident.clone(),
            };
            if keep.contains(&key) {
                kept.push(reading);
            }
        }
        if !kept.is_empty() {
            stored.readings = kept;
            out.push(stored);
        }
    }
    Ok(out)
}

/// Decode the reconciliation query's rows into `(key, stored value)` pairs.
#[allow(clippy::type_complexity)]
fn decode_cold_rows(
    batch: &crate::arrow::array::RecordBatch,
    identity: &[String],
) -> Result<Vec<(ColdKey, crate::session::StoredValue, Option<String>)>> {
    use crate::arrow::array::{Array, Decimal128Array, TimestampMicrosecondArray};
    use crate::encode::schema::col;

    let malo = column_str(batch, col::MALO_ID)?;
    let melo = column_str(batch, col::MELO_ID)?;
    let obis = column_str(batch, col::OBIS_CODE)?;
    let unit = column_str(batch, col::UNIT)?;
    let quality = column_str(batch, col::QUALITY)?;
    let scope = column_str(batch, col::VERSION_SCOPE)?;
    let identity_columns = identity
        .iter()
        .map(|name| column_str(batch, name))
        .collect::<Result<Vec<_>>>()?;

    let timestamps = |name: &'static str| -> Result<&TimestampMicrosecondArray> {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
            .ok_or_else(|| Error::decode(name, "expected a timestamp column"))
    };
    let decimals = |name: &'static str| -> Result<&Decimal128Array> {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<Decimal128Array>())
            .ok_or_else(|| Error::decode(name, "expected a decimal column"))
    };

    let from = timestamps(col::FROM)?;
    let recorded_at = timestamps(col::RECORDED_AT)?;
    let value = decimals(col::VALUE)?;
    let version = decimals(col::VERSION)?;

    let scale = |raw: i128, s: i8| -> rust_decimal::Decimal {
        rust_decimal::Decimal::from_i128_with_scale(raw, u32::try_from(s).unwrap_or(0))
    };
    let instant = |micros: i64, what: &'static str| -> Result<OffsetDateTime> {
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
            .map_err(|e| Error::decode(what, e.to_string()))
    };

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let mut ident = Vec::with_capacity(identity.len());
        for (name, column) in identity.iter().zip(&identity_columns) {
            if column.is_null(i) {
                return Err(Error::decode(
                    name.as_str(),
                    "identity column is null, but it is part of what names a reading",
                ));
            }
            ident.push((name.clone(), column.value(i).to_string()));
        }
        let key = ColdKey {
            malo_id: malo.value(i).to_string(),
            obis_code: obis.value(i).to_string(),
            from: instant(from.value(i), col::FROM)?,
            identity: ident,
        };
        let held = crate::session::StoredValue {
            value: scale(value.value(i), crate::encode::schema::VALUE_SCALE),
            unit: metering::interval::MeasurementUnit::parse(unit.value(i)).ok_or_else(|| {
                Error::decode(
                    col::UNIT,
                    format!("{:?} is not a known unit", unit.value(i)),
                )
            })?,
            quality: quality
                .value(i)
                .parse()
                .map_err(|e| Error::decode(col::QUALITY, format!("{:?}: {e}", quality.value(i))))?,
            version: crate::version::ScopedVersion::new(
                crate::version::VersionScope::parse(scope.value(i))?,
                crate::version::Version::from_i128(version.value(i))?,
            ),
            recorded_at: instant(recorded_at.value(i), col::RECORDED_AT)?,
        };
        out.push((
            key,
            held,
            (!melo.is_null(i)).then(|| melo.value(i).to_string()),
        ));
    }
    Ok(out)
}

/// How many times [`MeterStore::append`] re-routes after archival moved the tier
/// boundary underneath it.
///
/// Each round costs one archival commit landing between this append's boundary
/// read and its write. Archival advances at most one window per commit and holds
/// an exclusive lease for the run, so losing twice in a row already means the
/// write is aimed at exactly the window being archived; a third loss is a
/// standing conflict rather than a race.
const BOUNDARY_ATTEMPTS: u32 = 3;

/// How many rounds [`MeterStore::append_authoritative`] will try before calling
/// it a conflict.
///
/// Each round is one `append`, and a round only repeats where a *higher* version
/// beat the one just written — so the loop converges unless another writer is
/// authoring the same reading continuously. Four is generous for that: two
/// authors racing settle in two, and a third round means something is wrong that
/// a fourth will not fix.
pub const AUTHORITATIVE_ATTEMPTS: usize = 4;

/// Rebuild the one interval a displacement describes, one version above whatever
/// currently holds it.
///
/// The version comes from [`ScopedVersion::next`] of the **stored** version, so
/// it continues that sequence under that scope. Everything else — the series
/// metadata, the identity columns, the commodity — is the caller's original, so
/// the re-authored row is the same assertion at a version that can take effect.
fn reauthored(
    pending: &[crate::encode::StoredSeries],
    discriminators: &[String],
    displacement: &crate::session::Displacement,
) -> Result<crate::encode::StoredSeries> {
    // Matched on the **whole** merge key, not on `(malo_id, from)`.
    //
    // A batch may carry one measuring point's channels, two tenants' readings
    // for it, or two Messlokationen under it — all sharing a `malo_id` and an
    // interval start. Located by that pair alone, a displacement reported for
    // one of them re-authors another: the wrong value is written, at a version
    // derived from a reading it is not about, and the reading that actually
    // needed authoring is left shadowed. Nothing downstream can see it.
    let matches =
        |s: &crate::encode::StoredSeries, i: &metering::interval::MeterInterval| -> Result<bool> {
            if s.series.malo_id.to_string() != displacement.malo_id || i.from != displacement.from {
                return Ok(false);
            }
            let Some(code) = i.obis_code.or(s.series.obis_code) else {
                return Ok(false);
            };
            if crate::encode::canonical_obis(&code.to_string())? != displacement.obis_code {
                return Ok(false);
            }
            Ok(
                discriminator_values(s.series.melo_id.as_ref(), &s.extra, discriminators)?
                    == displacement.identity,
            )
        };

    let mut located = None;
    for series in pending {
        for interval in &series.series.intervals {
            if matches(series, interval)? {
                located = Some((series, interval.clone()));
                break;
            }
        }
        if located.is_some() {
            break;
        }
    }

    let (source, interval) = located.ok_or_else(|| Error::InvariantViolated {
        table: displacement.malo_id.clone(),
        detail: format!(
            "a displacement was reported for {} {} at {} but no series in the batch \
             carries that reading",
            displacement.malo_id, displacement.obis_code, displacement.from
        ),
    })?;

    // `superseded` is guaranteed present for the effects that reach here —
    // `Shadowed` and `Duplicate` are only reached when a prior row was found.
    let held = displacement
        .superseded
        .as_ref()
        .ok_or_else(|| Error::InvariantViolated {
            table: displacement.malo_id.clone(),
            detail: format!(
                "{:?} was reported for {} at {} with no superseded value, so there is \
                 nothing to author above",
                displacement.effect, displacement.malo_id, displacement.from
            ),
        })?;

    let mut next = source.clone();
    next.series.intervals = vec![interval];
    next.version = held.version.next()?;
    Ok(next)
}

/// Fold re-authored single-interval series back into one delivery per
/// `(measuring point, version)`.
///
/// A round can produce many one-interval series for one meter, and appending
/// them separately would cost a boundary read and an encode per interval. They
/// merge whenever they agree on everything a decoded run has to agree on, which
/// here reduces to the series identity and the version they were re-authored at.
fn merge_authored(mut rows: Vec<crate::encode::StoredSeries>) -> Vec<crate::encode::StoredSeries> {
    rows.sort_by(|a, b| {
        a.series
            .malo_id
            .to_string()
            .cmp(&b.series.malo_id.to_string())
            .then_with(|| a.version.version().get().cmp(&b.version.version().get()))
            .then_with(|| {
                a.series
                    .intervals
                    .first()
                    .map(|i| i.from)
                    .cmp(&b.series.intervals.first().map(|i| i.from))
            })
    });

    let mut out: Vec<crate::encode::StoredSeries> = Vec::with_capacity(rows.len());
    for row in rows {
        match out.last_mut() {
            Some(last)
                if last.series.malo_id == row.series.malo_id
                    && last.version == row.version
                    && last.series.obis_code == row.series.obis_code
                    && last.extra == row.extra =>
            {
                last.series.intervals.extend(row.series.intervals);
            }
            _ => out.push(row),
        }
    }
    out
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

#[async_trait::async_trait]
impl crate::session::SqlSurface for MeterStore {
    fn label(&self) -> String {
        self.resolved_table()
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
    /// two corrections arrive together, which is when an audit trail matters.
    ///
    /// The two tiers close that race differently, and only one of them closes it
    /// completely. On the hot tier the prior state and the insert share **one
    /// transaction**, so nothing can land between them. Iceberg has no
    /// transaction a reader can join, so a cold row is reported against the state
    /// read immediately before its append. The gap is narrow by construction —
    /// late corrections are rare, and the archiver, the other cold writer, holds
    /// an exclusive lease — but it is a gap, and saying so beats implying
    /// otherwise.
    ///
    /// See [`Displacement`](crate::session::Displacement).
    pub displacements: Vec<crate::session::Displacement>,
}

impl AppendOutcome {
    /// Fold another round's result into this one.
    ///
    /// Used when [`append`](MeterStore::append) has to route a second time
    /// because archival moved the boundary underneath it. Rows are **added**
    /// rather than replaced, and the displacements of both rounds are kept: a
    /// re-routed interval really was written twice, once to each tier, and the
    /// second write reports itself as an insert into the tier that now owns it
    /// while the first reports the duplicate it has become.
    fn absorb(&mut self, other: Self) {
        self.hot_rows += other.hot_rows;
        self.cold_rows += other.cold_rows;
        self.displacements.extend(other.displacements);
    }

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
        self.store.require_melo(
            series
                .iter()
                .map(|s| (&s.series.malo_id, s.series.melo_id.is_some())),
        )?;
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
        let step = store.config.archival_step();
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
    row_scope: Vec<(String, datafusion::scalar::ScalarValue)>,
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

    /// Confine every query in this session to rows matching an identity
    /// equality.
    ///
    /// See [`MeterStore::scoped`], which validates the column and is the
    /// supported way to reach this.
    pub fn row_scope(mut self, scope: Vec<(String, datafusion::scalar::ScalarValue)>) -> Self {
        self.row_scope = scope;
        self
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
            .with_row_scope(self.row_scope.clone())
            .with_mode(self.mode),
        );

        // The raw table holds every version. Registering it under a name that
        // says so keeps the audit trail reachable without making it the thing a
        // careless `SELECT SUM(...)` hits.
        let raw = raw_name(config.name());
        ctx.register_table(&raw, Arc::clone(&tiered) as Arc<dyn TableProvider>)
            .map_err(Error::from)?;

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
            .map_err(Error::from)?;

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
            .map_err(Error::from)?;

        // Completeness reads the *resolved* table: a corrected interval is one
        // interval, and counting both its versions would report a complete day
        // as having more intervals than the calendar allows.
        ctx.register_udtf(
            super::CompletenessFunction::NAME,
            Arc::new(super::CompletenessFunction::new(
                resolved_provider,
                resolved.clone(),
                config.discriminator_columns(),
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
            row_scope: self.row_scope,
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
