//! The DataFusion table provider that spans both tiers.
//!
//! One logical table over PostgreSQL and Iceberg. A query's interval range is
//! cut at the watermark, each half is scanned from the tier that holds it, and
//! the results are concatenated.
//!
//! Concatenated, not merged: the halves are disjoint by construction, so
//! `UNION ALL` is correct and no deduplication is needed. That is the whole
//! payoff of the tiering design, and it is why this provider is short.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::datasource::TableType;
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use time::OffsetDateTime;
use tracing::debug;

use crate::arrow::array::RecordBatch;
use crate::arrow::datatypes::SchemaRef;
use crate::planner::predicate;
use crate::planner::split::{TierSplit, TimeRange, split};
use crate::tiering::store::{ColdStore, HotStore};
use crate::watermark::TieringWatermark;

/// Which past state of the cold tier a reproducible read pins to.
///
/// Iceberg keeps every commit as a snapshot, so "the table as it stood on the
/// 8th working day" is a lookup rather than a reconstruction. That is the whole
/// mechanism behind MaBiS settlement reruns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSelector {
    /// A specific Iceberg snapshot, by id.
    ///
    /// The exact form. Record the id a settlement ran against and it reproduces
    /// forever, independently of how the snapshot list later changes.
    Id(i64),
    /// The snapshot that was current at an instant.
    ///
    /// The convenient form, and the one an auditor asks for. Resolves to the
    /// newest snapshot committed at or before the instant.
    Timestamp(OffsetDateTime),
}

impl fmt::Display for SnapshotSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Id(id) => write!(f, "snapshot {id}"),
            Self::Timestamp(at) => write!(f, "as of {at}"),
        }
    }
}

/// Which tiers a query is allowed to read, and which state of them.
///
/// The default reads both. The others exist because some queries are better off
/// *not* reading a tier: regulatory reporting wants reproducibility and no load
/// on the operational database, while operational monitoring only cares about
/// the recent window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadMode {
    /// Both tiers, split at the watermark. Current best knowledge.
    #[default]
    Unified,
    /// Cold only, current snapshot. No load on PostgreSQL.
    Historical,
    /// Hot only. The recent window.
    Operational,
    /// A pinned historical state: one Iceberg snapshot, optionally with a
    /// ceiling on the domain version axis.
    ///
    /// "The settlement exactly as computed on the 8th working day." This is the
    /// regulatory read, and it is **cold only** by construction — the hot tier
    /// keeps no history of itself, so including it would make the result depend
    /// on when the query ran, which is the opposite of reproducible.
    ///
    /// The two axes are independent and both are needed. The snapshot pins
    /// *transaction* time: what the store had been told. `max_version` pins the
    /// *domain* version axis: which assertion was in force. A snapshot taken
    /// after a correction landed holds both versions, and resolution would
    /// prefer the newer one — so without the ceiling a rerun reproduces the
    /// store's knowledge rather than the settlement's inputs.
    AsOf {
        /// The snapshot to read.
        snapshot: SnapshotSelector,
        /// Highest MSCONS version to consider, if the version axis is pinned too.
        max_version: Option<crate::version::Version>,
    },
    /// A transaction-time read: the resolved value **as it was known at** an
    /// instant, across both tiers.
    ///
    /// Where [`AsOf`](Self::AsOf) pins the cold tier to an Iceberg snapshot — and
    /// is therefore cold-only — this pins the row-level `recorded_at` axis, which
    /// every row carries in *both* tiers. So it answers "what did we believe at
    /// time T" for recent (hot) data too, not only settled history. Only rows
    /// recorded at or before the instant enter version resolution, so a correction
    /// delivered later, and an interval first stored later, are both invisible.
    ///
    /// Reproducible without pinning a snapshot: archival only ever *moves* a row
    /// (with its `recorded_at`) from hot to cold, so a row recorded by T is
    /// readable from one tier or the other regardless of when the query runs.
    AsKnownAt(OffsetDateTime),
}

impl ReadMode {
    /// Narrow a split to the tiers this mode permits.
    fn apply(self, split: TierSplit) -> TierSplit {
        match self {
            // A transaction-time read spans both tiers: `recorded_at` lives on
            // every row, so the hot window is part of "what was known at T".
            Self::Unified | Self::AsKnownAt(_) => split,
            Self::Historical | Self::AsOf { .. } => TierSplit { hot: None, ..split },
            Self::Operational => TierSplit {
                cold: None,
                ..split
            },
        }
    }

    /// The pinned state, when this is a reproducible read.
    pub const fn as_of(self) -> Option<(SnapshotSelector, Option<crate::version::Version>)> {
        match self {
            Self::AsOf {
                snapshot,
                max_version,
            } => Some((snapshot, max_version)),
            _ => None,
        }
    }

    /// The transaction-time ceiling, when this is an [`AsKnownAt`](Self::AsKnownAt) read.
    pub const fn recorded_at_ceiling(self) -> Option<OffsetDateTime> {
        match self {
            Self::AsKnownAt(at) => Some(at),
            _ => None,
        }
    }

    /// Whether this mode must always run version resolution rather than eliding it.
    ///
    /// A pinned snapshot's statistics describe the current table, not the pinned
    /// one; a version ceiling and a transaction-time ceiling both change which
    /// version wins. None of these is provable from per-file statistics, so
    /// eliding resolution would silently return the wrong rows.
    pub const fn forces_resolution(self) -> bool {
        matches!(self, Self::AsOf { .. } | Self::AsKnownAt(_))
    }
}

/// The tier boundaries a statement is being planned against.
///
/// # Why the boundary travels with the plan
///
/// A caller reports the boundary an answer was computed against (P1), and the
/// providers decide the tier split. Left to read the watermark separately, those
/// are **two reads of a value archival is moving**: the report is taken before
/// planning and the split during it, so an archival commit in between makes the
/// label a statement about a boundary the answer was not computed against. For a
/// statement over several tables it is worse — every provider reads its own, at
/// its own moment, and no two are guaranteed consistent.
///
/// So the boundaries are read **once**, placed on the session config for the
/// duration of one physical plan, and read back here. One statement, one set of
/// boundaries, and the label is the thing that was used. A provider planned
/// without one — a caller reaching DataFusion directly through
/// [`MeterStore::context`](crate::MeterStore::context) — reads a fresh watermark
/// as before.
///
/// [`MeterStore::context`]: crate::session::MeterStore::context
#[derive(Debug, Clone, Default)]
pub struct PlannedWatermarks(std::collections::HashMap<String, TieringWatermark>);

impl PlannedWatermarks {
    /// Pin `table` to `watermark` for the plan this is attached to.
    pub fn with(mut self, table: impl Into<String>, watermark: TieringWatermark) -> Self {
        self.0.insert(table.into(), watermark);
        self
    }

    /// Whether anything is pinned.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The boundary pinned for `table` on this session, if any.
    pub fn of(state: &dyn Session, table: &str) -> Option<TieringWatermark> {
        state
            .config()
            .get_extension::<Self>()
            .and_then(|pinned| pinned.0.get(table).copied())
    }
}

/// A table spanning the hot and cold tiers.
pub struct TieredTableProvider {
    table: String,
    schema: SchemaRef,
    hot: Arc<dyn HotStore>,
    cold: Arc<dyn ColdStore>,
    cold_provider: Arc<dyn TableProvider>,
    mode: ReadMode,
    /// How the hot half pages through a range.
    ///
    /// Carries the merge key, because a chunked scan resumes by keyset and the
    /// cursor has to be unique per row — see [`ScanSpec::cursor_columns`].
    spec: crate::tiering::store::ScanSpec,
    /// Identity-column equalities every scan is confined to.
    ///
    /// Empty for an unscoped provider. See
    /// [`with_row_scope`](Self::with_row_scope).
    row_scope: Vec<(String, datafusion::scalar::ScalarValue)>,
    /// How long a cross-tier plan holds the reclamation floor, or `None` to
    /// leave reclamation to the wall-clock grace alone.
    max_pin_age: Option<time::Duration>,
}

impl fmt::Debug for TieredTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TieredTableProvider")
            .field("table", &self.table)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl TieredTableProvider {
    /// Build a provider over both tiers.
    ///
    /// `cold_provider` supplies the cold half — normally an
    /// `IcebergStaticTableProvider`, so partition pruning, bloom filters and
    /// page statistics all apply without this crate reimplementing them.
    ///
    /// **The schema is taken from the cold provider**, not from
    /// `encode::schema::storage_schema`. The two describe the same columns but
    /// spell the UTC offset differently (`+00:00` versus `UTC`), and a union
    /// requires them to match exactly. Adopting one side's schema and casting
    /// the other removes the discrepancy at a single point.
    pub fn new(
        table: impl Into<String>,
        hot: Arc<dyn HotStore>,
        cold: Arc<dyn ColdStore>,
        cold_provider: Arc<dyn TableProvider>,
    ) -> Self {
        let schema = cold_provider.schema();
        let core = crate::encode::schema::storage_schema(&[]);
        let extra: Vec<String> = schema
            .fields()
            .iter()
            .filter(|f| core.field_with_name(f.name()).is_err())
            .map(|f| f.name().clone())
            .collect();

        Self {
            table: table.into(),
            schema,
            hot,
            cold,
            cold_provider,
            mode: ReadMode::default(),
            // Defaults to the core merge key. A deployment that extends the
            // identity must say so with `with_merge_key`, or the hot scan pages
            // by a cursor that is not unique and silently drops rows.
            spec: crate::tiering::store::ScanSpec::new(
                crate::encode::schema::MERGE_KEY
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
                extra,
            ),
            row_scope: Vec::new(),
            max_pin_age: None,
        }
    }

    /// Hold the reclamation floor for as long as a cross-tier plan may live.
    ///
    /// A plan is cut at a boundary and scanned later, and the hot half is
    /// enumerated lazily — on first poll, which for a `UNION ALL` drained
    /// cold-side-first is however long the cold scan takes. Registering the
    /// boundary makes archival keep what the plan is entitled to, instead of the
    /// plan finding it gone and refusing.
    ///
    /// Unset means no registration and the wall-clock
    /// [`reader_grace`](crate::config::TableConfig::reader_grace) alone, which
    /// is also what a store with no pin registry gets.
    #[must_use]
    pub fn with_max_pin_age(mut self, age: time::Duration) -> Self {
        self.max_pin_age = Some(age);
        self
    }

    /// Confine every scan to rows matching these identity-column equalities.
    ///
    /// The predicate is **enforced**, not offered. A filter handed to
    /// `TableProvider::scan` is advisory — DataFusion normally re-applies it
    /// above the scan, so a provider may prune on it and return the rows anyway.
    /// This one is injected here, so the engine does not know it exists and would
    /// never re-apply it; it is therefore placed by this crate, below the
    /// projection, exactly as the transaction-time ceiling is.
    ///
    /// That is the whole point: it is what lets a service expose **caller-supplied
    /// SQL** over one tenant's rows. A predicate the caller could omit is not a
    /// boundary.
    ///
    /// # Only merge-key columns
    ///
    /// [`MeterStore::scoped`] refuses anything else, and the reason is version
    /// resolution rather than taste. A merge-key column partitions *readings*:
    /// filtering before ranking and filtering after give the same winner. An
    /// attribute column is not, so a correction that changed it would have its
    /// versions sliced apart by the filter, and the scoped read would resolve to
    /// a different value than the unscoped one.
    ///
    /// [`MeterStore::scoped`]: crate::session::MeterStore::scoped
    pub fn with_row_scope(mut self, scope: Vec<(String, datafusion::scalar::ScalarValue)>) -> Self {
        self.row_scope = scope;
        self
    }

    /// The identity equalities every scan through this provider is confined to.
    pub fn row_scope(&self) -> &[(String, datafusion::scalar::ScalarValue)] {
        &self.row_scope
    }

    /// The row scope as filter expressions.
    fn row_scope_filters(&self) -> Vec<Expr> {
        self.row_scope
            .iter()
            .map(|(column, value)| {
                datafusion::logical_expr::col(column)
                    .eq(datafusion::logical_expr::lit(value.clone()))
            })
            .collect()
    }

    /// Declare how the hot half must page through a range.
    ///
    /// Only the configuration knows which deployment columns are identity and
    /// which are attributes, and the hot scan needs the distinction: its keyset
    /// cursor must be unique per row, and with an identity column the core key
    /// is not. The chunk size travels with it so the query path and the archival
    /// path cannot end up bounded differently.
    pub fn with_scan_spec(mut self, spec: crate::tiering::store::ScanSpec) -> Self {
        self.spec = spec;
        self
    }

    /// Restrict which tiers this provider reads.
    pub fn with_mode(mut self, mode: ReadMode) -> Self {
        self.mode = mode;
        self
    }

    /// The read mode in force.
    pub const fn mode(&self) -> ReadMode {
        self.mode
    }

    /// The boundary this scan must use: the one the statement was planned
    /// against, or a fresh read when none was placed.
    ///
    /// See [`PlannedWatermarks`].
    async fn watermark_for(&self, state: &dyn Session) -> DfResult<TieringWatermark> {
        match PlannedWatermarks::of(state, &self.table) {
            Some(pinned) => Ok(pinned),
            None => self.watermark().await,
        }
    }

    /// The current tier boundary.
    pub async fn watermark(&self) -> DfResult<TieringWatermark> {
        self.cold.watermark(&self.table).await.map_err(external)
    }

    /// The tier split for the given filters against a **known** watermark.
    ///
    /// Separated from reading the watermark on purpose. A caller that decides
    /// something from the split — whether version resolution can be elided, say
    /// — and then scans must do both against the same boundary. Reading it twice
    /// leaves a window in which archival advances it between the decision and
    /// the scan, and the two then disagree about which tier owns a range.
    pub fn split_at(&self, watermark: TieringWatermark, filters: &[Expr]) -> TierSplit {
        let range = predicate::time_range(filters);
        self.mode.apply(split(range, watermark))
    }

    /// The tier split this provider would use for the given filters.
    ///
    /// Exposed for introspection and testing: the split is the decision worth
    /// inspecting, and reaching it should not require executing a query.
    pub async fn plan_split(&self, filters: &[Expr]) -> DfResult<(TieringWatermark, TierSplit)> {
        let watermark = self.watermark().await?;
        Ok((watermark, self.split_at(watermark, filters)))
    }

    /// Scan the hot tier over `range`, streaming and cast to the union schema.
    ///
    /// The rows never accumulate: PostgreSQL pages them, each page becomes a
    /// batch, and the batch reaches the engine before the next page is fetched.
    fn scan_hot(
        &self,
        range: TimeRange,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        pin: Option<Arc<HeldPin>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HotScanExec::new(
            self.schema.clone(),
            Arc::clone(&self.hot),
            self.table.clone(),
            range,
            self.spec.clone(),
            projection.cloned(),
            limit,
            pin,
        )?))
    }

    /// Scan the cold tier, adding the range as a filter so Iceberg can prune.
    async fn scan_cold(
        &self,
        state: &dyn Session,
        range: TimeRange,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let mut all = filters.to_vec();
        all.extend(predicate::range_filters(range));

        let Some((snapshot, max_version)) = self.mode.as_of() else {
            return self
                .cold_provider
                .scan(state, projection, &all, limit)
                .await;
        };

        let ceiling = max_version.map(predicate::version_ceiling);
        if let Some(expr) = &ceiling {
            // Also pushed down, so Iceberg can prune whole files whose `version`
            // bounds are entirely above the ceiling. The enforcement below is
            // what makes it correct; this is what makes it fast.
            all.push(expr.clone());
        }

        let pinned = self
            .cold
            .snapshot_provider(&self.table, snapshot)
            .await
            .map_err(external)?;

        // A pinned snapshot predates any schema evolution since, so its columns
        // may be a prefix of today's. Projection indices and the union schema
        // are the current ones, so a mismatch has to fail with the reason rather
        // than as an opaque Arrow error several layers down.
        if pinned.schema() != self.schema {
            return Err(DataFusionError::Plan(format!(
                "cannot read {} {snapshot}: its schema differs from the current one, so a \
                 reproducible read would silently change shape. Pin an explicit snapshot id \
                 from before the schema change, or query the raw table directly.",
                self.table,
            )));
        }

        let Some(ceiling) = ceiling else {
            return pinned.scan(state, projection, &all, limit).await;
        };

        // **The ceiling is applied, not merely offered.** A filter handed to
        // `TableProvider::scan` is advisory: the provider may use it for pruning
        // and return the rows anyway, because DataFusion normally re-applies it
        // above the scan. This one is injected here, so the engine does not know
        // it exists and would never re-apply it — and a settlement rerun that
        // quietly ignored its version ceiling would return today's corrections
        // under the heading of a past settlement.
        let scanned = pinned.scan(state, None, &all, None).await?;
        self.enforce(state, vec![ceiling], scanned, projection, limit)
    }

    /// Scan both tiers against an already-read watermark.
    ///
    /// The scan half of [`split_at`](Self::split_at): a caller that decided
    /// something from the split passes the same boundary here, so the decision
    /// and the scan cannot be based on different views of the tier layout.
    pub async fn scan_at(
        &self,
        state: &dyn Session,
        watermark: TieringWatermark,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let started = std::time::Instant::now();
        let split = self.split_at(watermark, filters);

        debug!(
            table = %self.table,
            %watermark,
            cold = split.cold.is_some(),
            hot = split.hot.is_some(),
            "tier split"
        );

        // A transaction-time read pins `recorded_at`, which lives on every row in
        // *both* tiers — so unlike the version ceiling this one has to be applied
        // to the union rather than to the pinned cold snapshot. And like that
        // one it must be *applied*, not merely pushed down: the engine never saw
        // it, so it will not re-apply it above the scan, and a read that quietly
        // ignored its own ceiling would return rows learned after the instant it
        // claims to reproduce.
        //
        // It therefore sits below the projection, since the projection need not
        // include `recorded_at`, and the limit moves above it, since a limit
        // applied first counts rows the filter would have removed.
        //
        // The row scope joins it for the same reason and with more force: it is
        // what makes caller-supplied SQL safe to run over one tenant's rows, so
        // a predicate the engine could drop would not be a boundary at all.
        let mut enforced: Vec<Expr> = self
            .mode
            .recorded_at_ceiling()
            .map(predicate::recorded_at_ceiling)
            .into_iter()
            .collect();
        enforced.extend(self.row_scope_filters());

        let (tier_projection, tier_limit) = match enforced.is_empty() {
            false => (None, None),
            true => (projection, limit),
        };

        // Handed to the tiers as ordinary filters as well, so Iceberg can prune
        // whole files — by `recorded_at` bounds, and by the identity partition
        // field a scoped read is confined to. That is the optimisation; the
        // filter below is the correctness.
        let mut tier_filters = filters.to_vec();
        tier_filters.extend(enforced.iter().cloned());

        // **The reclamation floor this plan needs held.**
        //
        // Whenever there is a hot half, which is the exact condition rather
        // than a heuristic: the hot range starts at the boundary this plan was
        // cut at, so every partition archival is about to take is one this plan
        // still asks for. A cold-only plan touches no partition and needs
        // nothing held.
        //
        // Not narrowed to cross-tier plans, though those are where the exposure
        // is longest — the hot child is enumerated on first poll and a
        // `UNION ALL` drains cold-side-first, so the wait is the cold scan. A
        // hot-only plan is exposed for however long its consumer takes to read
        // it, which is not this layer's to bound.
        //
        // Two statements against a database the plan has not otherwise touched,
        // against a watermark read that has already been to the Iceberg
        // catalogue. The pin lives on the hot scan and dies with it, so a plan
        // built and discarded releases it without executing.
        let pin = match (self.max_pin_age, split.hot.is_some()) {
            (Some(age), true) => {
                let now = OffsetDateTime::now_utc();
                let held = self
                    .hot
                    .pin_reader(&self.table, watermark.get(), now + age)
                    .await
                    .map_err(external)?
                    .map(|pin| Arc::new(HeldPin::new(Arc::clone(&self.hot), pin)));

                // **Pin, then verify** — CockroachDB's rule for protected
                // timestamps, because "the mere existence of a record does not
                // itself prove that the data has been protected". A pin written
                // after the reclaimer read the registry protects nothing, and
                // the drop that raced it is recorded in the same transaction it
                // happened in, so re-reading that record settles the question
                // exactly rather than probably.
                if held.is_some()
                    && let Some(reclaimed_below) = self
                        .hot
                        .reclaimed_below(&self.table)
                        .await
                        .map_err(external)?
                    && reclaimed_below > watermark.get()
                {
                    return Err(DataFusionError::Plan(format!(
                        "the hot tier reclaimed {} up to {reclaimed_below} while this plan was                          being made at boundary {watermark}, so the plan is already short a                          window. Retry: a fresh plan reads the new boundary and is whole",
                        self.table,
                    )));
                }
                held
            }
            _ => None,
        };

        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(2);
        if let Some(range) = split.cold {
            plans.push(
                self.scan_cold(state, range, tier_projection, &tier_filters, tier_limit)
                    .await?,
            );
        }
        if let Some(range) = split.hot {
            plans.push(self.scan_hot(range, tier_projection, tier_limit, pin)?);
        }

        // Planning latency, not scan latency: at this point nothing has been
        // read. Sharing one instrument with the scan would make a slow scan
        // invisible and a slow catalog look like a slow query.
        crate::observe::metrics().plan_duration.record(
            started.elapsed().as_secs_f64(),
            &crate::observe::table(&self.table),
        );

        let scanned = match plans.len() {
            // A provably empty range still needs a plan with the right schema.
            0 => {
                let empty = MemTable::try_new(self.schema.clone(), vec![vec![]])?;
                empty
                    .scan(state, tier_projection, &tier_filters, tier_limit)
                    .await?
            }
            1 => plans.pop().expect("length checked"),
            // Disjoint halves, so concatenation is the whole merge.
            _ => UnionExec::try_new(plans)?,
        };

        match enforced.is_empty() {
            true => Ok(scanned),
            false => self.enforce(state, enforced, scanned, projection, limit),
        }
    }

    /// Apply `filters` to `input` below the projection, restoring `projection`
    /// and `limit` above it.
    ///
    /// The shape every *enforced* — as opposed to merely pushed-down — predicate
    /// needs: a filter DataFusion does not know about cannot be re-applied by
    /// DataFusion, so this crate has to place it, and it has to place it where
    /// the column it reads still exists.
    ///
    /// Several are conjoined into one `FilterExec` rather than stacked, so a
    /// transaction-time ceiling and a tenant scope cost one pass between them.
    fn enforce(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
        input: Arc<dyn ExecutionPlan>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let filter = filters
            .into_iter()
            .reduce(datafusion::logical_expr::and)
            .ok_or_else(|| {
                DataFusionError::Internal("enforce called with no predicate".to_string())
            })?;
        let df_schema = datafusion::common::DFSchema::try_from(self.schema.as_ref().clone())?;
        let physical = state.create_physical_expr(filter, &df_schema)?;
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::filter::FilterExec::try_new(physical, input)?,
        );

        let projected: Arc<dyn ExecutionPlan> = match projection {
            None => filtered,
            Some(indices) => {
                let exprs = indices
                    .iter()
                    .map(|i| {
                        let field = self.schema.field(*i);
                        let column: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Column::new(field.name(), *i),
                        );
                        (column, field.name().clone())
                    })
                    .collect::<Vec<_>>();
                Arc::new(
                    datafusion::physical_plan::projection::ProjectionExec::try_new(
                        exprs, filtered,
                    )?,
                )
            }
        };

        Ok(match limit {
            None => projected,
            Some(fetch) => Arc::new(datafusion::physical_plan::limit::GlobalLimitExec::new(
                projected,
                0,
                Some(fetch),
            )),
        })
    }
}

/// A reclamation floor held for one plan's lifetime.
///
/// # Why `Drop` and not an explicit release
///
/// There is no end-of-query hook to hang one on. A plan may be built and never
/// executed, executed and abandoned half-drained, or dropped when a client
/// disconnects — and the only thing all three have in common is that the plan
/// tree goes away. So the release rides on that, and because `Drop` cannot
/// await, it is spawned.
///
/// Spawning makes it **best-effort**, which is why the pin also carries an
/// expiry: a release that never runs — no runtime, a process killed outright —
/// costs the floor nothing beyond
/// [`max_pin_age`](crate::config::TableConfig::max_pin_age).
struct HeldPin {
    hot: Arc<dyn HotStore>,
    pin: crate::tiering::store::ReaderPin,
}

impl HeldPin {
    fn new(hot: Arc<dyn HotStore>, pin: crate::tiering::store::ReaderPin) -> Self {
        Self { hot, pin }
    }
}

impl Drop for HeldPin {
    fn drop(&mut self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let hot = Arc::clone(&self.hot);
        let pin = self.pin;
        handle.spawn(async move {
            if let Err(e) = hot.release_pin(pin).await {
                tracing::debug!(error = %e, "could not release a reader pin; it will expire");
            }
        });
    }
}

/// Streams the hot tier into the engine.
///
/// A `MemTable` would be simpler, but it holds every batch in memory before the
/// first one is read. This yields each batch as PostgreSQL produces it, so a
/// query over the whole hot window costs one page of rows rather than all of
/// them.
pub(crate) struct HotScanExec {
    /// Schema after projection — what this plan actually emits.
    schema: SchemaRef,
    /// Schema before projection, used to cast incoming batches.
    source_schema: SchemaRef,
    /// The scan is described, not performed, until `execute`.
    ///
    /// Holding a live stream here would make the plan single-use: DataFusion may
    /// legitimately execute one plan more than once, and a consumed stream would
    /// either error or — worse — return nothing.
    hot: Arc<dyn HotStore>,
    table: String,
    range: TimeRange,
    spec: crate::tiering::store::ScanSpec,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: Arc<datafusion::physical_plan::PlanProperties>,
    /// The reclamation floor this scan's plan is entitled to, held until the
    /// plan and every stream it produced are gone.
    pin: Option<Arc<HeldPin>>,
}

impl HotScanExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        source_schema: SchemaRef,
        hot: Arc<dyn HotStore>,
        table: String,
        range: TimeRange,
        spec: crate::tiering::store::ScanSpec,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        pin: Option<Arc<HeldPin>>,
    ) -> DfResult<Self> {
        use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
        use datafusion::physical_plan::PlanProperties;
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

        let schema = match &projection {
            Some(indices) => Arc::new(source_schema.project(indices)?),
            None => source_schema.clone(),
        };

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            // One PostgreSQL scan, paged in order — a single partition.
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            schema,
            source_schema,
            hot,
            table,
            range,
            spec,
            projection,
            limit,
            properties,
            pin,
        })
    }
}

impl fmt::Debug for HotScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HotScanExec")
    }
}

impl datafusion::physical_plan::DisplayAs for HotScanExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "HotScanExec: streaming")?;
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for HotScanExec {
    fn name(&self) -> &str {
        "HotScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<datafusion::physical_plan::SendableRecordBatchStream> {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::StreamExt;

        // The scan starts here, not at plan time, so executing the same plan
        // twice runs it twice rather than finding a stream already spent.
        let hot = Arc::clone(&self.hot);
        let table = self.table.clone();
        let range = self.range;
        let spec = self.spec.clone();
        let table_name = self.table.clone();
        let source_schema = self.source_schema.clone();
        let projection = self.projection.clone();
        let mut remaining = self.limit.unwrap_or(usize::MAX);
        // Moved into the stream as well as held by the plan: a consumer that
        // drops the plan and keeps draining the stream must not have the floor
        // released out from under it.
        let pin = self.pin.clone();

        let batches = async_stream::stream! {
            let _pin = pin;
            let started = std::time::Instant::now();
            let mut inner = match hot.scan_range(&table, range, &spec).await {
                Ok(stream) => stream,
                Err(e) => {
                    yield Err(external(e));
                    return;
                }
            };

            while let Some(batch) = inner.next().await {
                if remaining == 0 {
                    break;
                }
                let mapped = batch
                    .map_err(external)
                    .and_then(|b| cast_to(&b, &source_schema))
                    .and_then(|b| match &projection {
                        Some(indices) => Ok(b.project(indices)?),
                        None => Ok(b),
                    });

                if let Ok(ref b) = mapped {
                    remaining = remaining.saturating_sub(b.num_rows());
                    crate::observe::metrics()
                        .rows_scanned
                        .add(b.num_rows() as u64, &crate::observe::table_tier(&table_name, "hot"));
                }
                yield mapped;
            }

            // Recorded when the stream is drained, so this is the time the rows
            // actually took rather than the time it took to describe the work.
            crate::observe::metrics().scan_duration.record(
                started.elapsed().as_secs_f64(),
                &crate::observe::table_tier(&table_name, "hot"),
            );
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            batches,
        )))
    }
}

/// Cast a batch's columns to a target schema.
///
/// A no-op for identical types and metadata-only for a timestamp whose unit
/// matches but whose zone is spelled differently.
fn cast_to(batch: &RecordBatch, schema: &SchemaRef) -> DfResult<RecordBatch> {
    if batch.schema() == *schema {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| crate::arrow::compute::cast(array, field.data_type()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Wrap a MeterStore error for DataFusion.
fn external(e: crate::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

#[async_trait]
impl TableProvider for TieredTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let watermark = self.watermark_for(state).await?;
        self.scan_at(state, watermark, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // **Never `Exact`, even for filters this provider does evaluate in full.**
        //
        // `Exact` tells DataFusion to *delete* the filter from the plan, so the
        // only thing standing between a caller and a wrong row is this
        // provider's promise. The hot half keeps that promise — the range is a
        // `WHERE` clause in the SQL. The cold half cannot: it hands the filters
        // to `iceberg-datafusion`, which converts them with a `filter_map` and
        // **silently discards whatever it cannot translate**. Today it translates
        // our range; a future version that stopped would produce rows outside
        // the requested window, with nothing above the scan to catch them and no
        // error anywhere.
        //
        // That is a guarantee held by a third party's internals, and P6 says
        // degrade rather than lie. `Inexact` costs one timestamp comparison per
        // row on a scan that has already read them, and the filters still reach
        // Iceberg for pruning — the optimisation that actually matters is
        // skipping files, not skipping the comparison.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode::schema;
    use crate::tiering::store::{CommitInfo, PartitionId};
    use crate::watermark::ArchivalWindow;

    use datafusion::logical_expr::{col as df_col, lit};
    use datafusion::prelude::SessionContext;
    use std::sync::Mutex;
    use time::macros::datetime;
    use time::{Duration, OffsetDateTime};

    const BOUNDARY: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

    /// Records which ranges each tier was asked for.
    #[derive(Default)]
    struct Calls {
        hot: Vec<TimeRange>,
    }

    struct FakeHot {
        rows: usize,
        calls: Mutex<Calls>,
    }

    #[async_trait]
    impl HotStore for FakeHot {
        async fn append_reporting(
            &self,
            _table: &str,
            _merge_key: &[String],
            _batches: &[RecordBatch],
        ) -> crate::error::Result<Vec<crate::session::Displacement>> {
            Ok(Vec::new())
        }

        async fn drop_table(&self, _table: &str) -> crate::error::Result<()> {
            Ok(())
        }

        async fn scan_range(
            &self,
            _table: &str,
            range: TimeRange,
            _spec: &crate::tiering::store::ScanSpec,
        ) -> crate::Result<crate::tiering::store::BatchStream> {
            self.calls.lock().unwrap().hot.push(range);
            let b = batch(self.rows);
            Ok(Box::pin(futures::stream::once(async move { Ok(b) })))
        }
        async fn create_tables(
            &self,
            _table: &str,
            _key: &[String],
            _extra: &[crate::arrow::datatypes::Field],
            _model: crate::config::TimeModel,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn append(
            &self,
            _t: &str,
            _k: &[String],
            batches: &[RecordBatch],
        ) -> crate::Result<u64> {
            Ok(batches.iter().map(|b| b.num_rows() as u64).sum())
        }

        async fn ensure_partitions(
            &self,
            _t: &str,
            _f: OffsetDateTime,
            _u: OffsetDateTime,
            _s: Duration,
        ) -> crate::Result<Vec<PartitionId>> {
            Ok(vec![])
        }
        async fn partition_exists(&self, _p: &PartitionId) -> crate::Result<bool> {
            Ok(true)
        }
        async fn detach_partition(&self, _p: &PartitionId) -> crate::Result<()> {
            Ok(())
        }
        async fn scan_detached(
            &self,
            _p: &PartitionId,
            _spec: &crate::tiering::store::ScanSpec,
        ) -> crate::Result<crate::tiering::store::BatchStream> {
            Ok(crate::tiering::store::stream_of(Vec::new()))
        }
        async fn drop_partition(
            &self,
            _p: &PartitionId,
            _below: time::OffsetDateTime,
        ) -> crate::Result<crate::tiering::store::Reclamation> {
            Ok(crate::tiering::store::Reclamation::Dropped)
        }
        async fn orphaned_partitions(&self, _t: &str) -> crate::Result<Vec<PartitionId>> {
            Ok(vec![])
        }
        async fn invariant_violations(&self, _t: &str, _w: TieringWatermark) -> crate::Result<u64> {
            Ok(0)
        }
    }

    struct FakeCold {
        watermark: TieringWatermark,
    }

    #[async_trait]
    impl ColdStore for FakeCold {
        async fn purge_table(&self, _table: &str) -> crate::error::Result<()> {
            Ok(())
        }

        async fn create_tables(
            &self,
            _table: &str,
            _identity: &[String],
            _extra: &[crate::arrow::datatypes::Field],
            _policy: &crate::tiering::store::MaintenancePolicy,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn watermark(&self, _t: &str) -> crate::Result<TieringWatermark> {
            Ok(self.watermark)
        }
        async fn append_and_commit(
            &self,
            _t: &str,
            _b: crate::tiering::store::BatchStream,
            _h: crate::tiering::store::WriteHints,
            w: ArchivalWindow,
            _step: time::Duration,
            _now: OffsetDateTime,
        ) -> crate::Result<CommitInfo> {
            Ok(CommitInfo {
                snapshot_id: 1,
                rows: 0,
                watermark: w.resulting_watermark(),
                added: None,
            })
        }
        async fn expire_snapshots(
            &self,
            _t: &str,
            _retain_for: time::Duration,
            _retain_last: usize,
            _now: OffsetDateTime,
        ) -> crate::Result<usize> {
            Ok(0)
        }

        async fn append_only(
            &self,
            _t: &str,
            _b: crate::tiering::store::BatchStream,
            _h: crate::tiering::store::WriteHints,
        ) -> crate::Result<CommitInfo> {
            Ok(CommitInfo {
                snapshot_id: 1,
                rows: 0,
                watermark: self.watermark,
                added: None,
            })
        }
    }

    fn batch(rows: usize) -> RecordBatch {
        use crate::arrow::array::{
            Decimal128Array, StringArray, TimestampMicrosecondArray, UInt8Array,
        };
        let s = schema::storage_schema(&[]);
        let columns = s
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                crate::arrow::datatypes::DataType::Utf8 => {
                    Arc::new(StringArray::from(vec!["x"; rows])) as _
                }
                crate::arrow::datatypes::DataType::UInt8 => {
                    Arc::new(UInt8Array::from(vec![0u8; rows])) as _
                }
                crate::arrow::datatypes::DataType::Date32 => {
                    Arc::new(crate::arrow::array::Date32Array::from(vec![0i32; rows])) as _
                }
                crate::arrow::datatypes::DataType::Timestamp(_, _) => {
                    Arc::new(TimestampMicrosecondArray::from(vec![0i64; rows]).with_timezone("UTC"))
                        as _
                }
                crate::arrow::datatypes::DataType::Decimal128(p, sc) => Arc::new(
                    Decimal128Array::from(vec![0i128; rows])
                        .with_precision_and_scale(*p, *sc)
                        .unwrap(),
                ) as _,
                other => panic!("unhandled {other:?}"),
            })
            .collect();
        RecordBatch::try_new(s, columns).unwrap()
    }

    /// A cold provider standing in for Iceberg, over the same schema.
    fn cold_provider(rows: usize) -> Arc<dyn TableProvider> {
        let s = schema::storage_schema(&[]);
        Arc::new(MemTable::try_new(s, vec![vec![batch(rows)]]).unwrap())
    }

    fn provider(hot_rows: usize, cold_rows: usize) -> TieredTableProvider {
        TieredTableProvider::new(
            "readings",
            Arc::new(FakeHot {
                rows: hot_rows,
                calls: Mutex::new(Calls::default()),
            }),
            Arc::new(FakeCold {
                watermark: TieringWatermark::new(BOUNDARY),
            }),
            cold_provider(cold_rows),
        )
    }

    fn ts(t: OffsetDateTime) -> Expr {
        lit(crate::encode::schema::timestamp_scalar(t))
    }

    async fn row_count(p: TieredTableProvider, filters: Vec<Expr>) -> usize {
        let ctx = SessionContext::new();
        let plan = p
            .scan(&ctx.state(), None, &filters, None)
            .await
            .expect("scan");
        datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("collect")
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    #[tokio::test]
    async fn a_historical_query_reads_only_the_cold_tier() {
        let p = provider(7, 3);
        let filters = vec![df_col(schema::col::FROM).lt(ts(datetime!(2026-07-10 00:00 UTC)))];

        let (_, split) = p.plan_split(&filters).await.unwrap();
        assert!(split.is_cold_only());
        assert_eq!(row_count(p, filters).await, 3);
    }

    #[tokio::test]
    async fn a_recent_query_reads_only_the_hot_tier() {
        let p = provider(7, 3);
        let filters = vec![df_col(schema::col::FROM).gt_eq(ts(datetime!(2026-07-25 00:00 UTC)))];

        let (_, split) = p.plan_split(&filters).await.unwrap();
        assert!(split.hot.is_some() && split.cold.is_none());
        assert_eq!(row_count(p, filters).await, 7);
    }

    #[tokio::test]
    async fn a_spanning_query_unions_both_tiers_without_deduplicating() {
        // The halves are disjoint, so the row count is the plain sum.
        let p = provider(7, 3);
        let filters = vec![
            df_col(schema::col::FROM).gt_eq(ts(datetime!(2026-07-10 00:00 UTC))),
            df_col(schema::col::FROM).lt(ts(datetime!(2026-07-30 00:00 UTC))),
        ];

        let (_, split) = p.plan_split(&filters).await.unwrap();
        assert!(split.spans_tiers());
        assert_eq!(row_count(p, filters).await, 10);
    }

    #[tokio::test]
    async fn an_unfiltered_query_reads_both_tiers() {
        let p = provider(7, 3);
        assert_eq!(row_count(p, vec![]).await, 10);
    }

    #[tokio::test]
    async fn the_hot_tier_is_asked_only_for_the_range_above_the_watermark() {
        // If the hot scan were handed the query's full range it would return
        // rows the cold tier also returns, and the union would double-count.
        let hot = Arc::new(FakeHot {
            rows: 1,
            calls: Mutex::new(Calls::default()),
        });
        let p = TieredTableProvider::new(
            "readings",
            hot.clone(),
            Arc::new(FakeCold {
                watermark: TieringWatermark::new(BOUNDARY),
            }),
            cold_provider(1),
        );

        let filters = vec![
            df_col(schema::col::FROM).gt_eq(ts(datetime!(2026-07-10 00:00 UTC))),
            df_col(schema::col::FROM).lt(ts(datetime!(2026-07-30 00:00 UTC))),
        ];
        let _ = row_count(p, filters).await;

        let calls = hot.calls.lock().unwrap();
        assert_eq!(calls.hot.len(), 1);
        assert_eq!(calls.hot[0].start(), Some(BOUNDARY));
        assert_eq!(calls.hot[0].end(), Some(datetime!(2026-07-30 00:00 UTC)));
    }

    #[tokio::test]
    async fn an_empty_range_produces_a_plan_that_returns_nothing() {
        // Contradictory bounds are legal SQL. They must not become a full scan.
        let p = provider(7, 3);
        let filters = vec![
            df_col(schema::col::FROM).gt_eq(ts(datetime!(2026-08-01 00:00 UTC))),
            df_col(schema::col::FROM).lt(ts(datetime!(2026-07-01 00:00 UTC))),
        ];

        let (_, split) = p.plan_split(&filters).await.unwrap();
        assert!(split.is_empty());
        assert_eq!(row_count(p, filters).await, 0);
    }

    #[tokio::test]
    async fn historical_mode_never_touches_the_hot_tier() {
        let hot = Arc::new(FakeHot {
            rows: 7,
            calls: Mutex::new(Calls::default()),
        });
        let p = TieredTableProvider::new(
            "readings",
            hot.clone(),
            Arc::new(FakeCold {
                watermark: TieringWatermark::new(BOUNDARY),
            }),
            cold_provider(3),
        )
        .with_mode(ReadMode::Historical);

        assert_eq!(row_count(p, vec![]).await, 3);
        assert!(
            hot.calls.lock().unwrap().hot.is_empty(),
            "reporting queries must place no load on PostgreSQL"
        );
    }

    #[tokio::test]
    async fn operational_mode_reads_only_the_recent_window() {
        let p = provider(7, 3).with_mode(ReadMode::Operational);
        assert_eq!(row_count(p, vec![]).await, 7);
    }

    #[tokio::test]
    async fn an_empty_watermark_sends_everything_to_the_hot_tier() {
        let p = TieredTableProvider::new(
            "readings",
            Arc::new(FakeHot {
                rows: 5,
                calls: Mutex::new(Calls::default()),
            }),
            Arc::new(FakeCold {
                watermark: TieringWatermark::empty(),
            }),
            cold_provider(3),
        );

        let (_, split) = p.plan_split(&[]).await.unwrap();
        assert!(split.cold.is_none(), "nothing has been archived");
        assert_eq!(row_count(p, vec![]).await, 5);
    }

    #[test]
    fn no_filter_is_ever_reported_exact() {
        // `Exact` deletes the filter from the plan, leaving this provider's
        // promise as the only thing between a caller and a wrong row. The hot
        // half keeps that promise; the cold half delegates to a converter that
        // silently drops what it cannot translate, so the promise cannot be
        // made — see `supports_filters_pushdown`.
        let p = provider(1, 1);
        let time = df_col(schema::col::FROM).gt_eq(ts(BOUNDARY));
        let other = df_col(schema::col::MALO_ID).eq(lit("12345678905"));

        let got = p.supports_filters_pushdown(&[&time, &other]).unwrap();
        assert!(
            got.iter()
                .all(|p| *p == TableProviderFilterPushDown::Inexact)
        );
    }

    #[test]
    fn read_mode_narrows_a_split() {
        let both = TierSplit {
            cold: Some(TimeRange::unbounded()),
            hot: Some(TimeRange::unbounded()),
        };
        assert!(ReadMode::Unified.apply(both).spans_tiers());
        assert!(ReadMode::Historical.apply(both).is_cold_only());
        assert!(ReadMode::Operational.apply(both).cold.is_none());
    }
}
