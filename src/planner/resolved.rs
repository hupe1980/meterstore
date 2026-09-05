//! The table applications should query: corrections already resolved.
//!
//! A correction is stored as a new row at a higher `version`, never as an
//! overwrite — that is the audit trail. So a raw scan returns every version of a
//! corrected interval, and summing it overstates the month. This provider keeps
//! the highest version per merge key.
//!
//! # Why a provider rather than a view
//!
//! A SQL view would be simpler and always correct — and always paid for. The
//! interesting property of metering data is that corrections are **rare and
//! recent**: a settlement query over last winter reads partitions where every
//! key has exactly one version, and ranking rows against nothing is pure
//! overhead.
//!
//! Proving that requires per-file `min`/`max` statistics for `version`, which
//! only the storage layer has and a view cannot see. A provider is asked to
//! `scan` a specific range and can read those statistics first, so the decision
//! is made per query with the range in hand.
//!
//! # When resolution is skipped
//!
//! Only when the scan is **historical** — entirely below the tiering watermark —
//! and no merge key can appear twice among the cold files in range. Each file
//! must provably hold one version, and two files may only disagree about which
//! version when their `from` bounds do not overlap: `from` is in the merge key,
//! so files covering different days cannot hold the same key however their
//! versions differ. Consecutive archival windows are exactly that, which is what
//! makes the optimisation fire on a year-long scan rather than only on a
//! single-day one. See [`super::version::plan`].
//!
//! Two separate things are going on there, and it is worth not conflating them.
//!
//! What makes reasoning about one tier *sound* is that the tiers hold disjoint
//! interval ranges and `append` routes a late correction to the tier that owns
//! its interval — so **every version of a given reading lives in the same
//! tier**. Without that, "no corrections among the cold files" would say nothing
//! about the reading as a whole, because a competing version could sit in the
//! other tier.
//!
//! Why the *hot* tier is excluded is then merely practical: PostgreSQL keeps no
//! per-file statistics, so proving the hot window correction-free would cost a
//! full scan of exactly the rows the optimisation exists to avoid. The hot
//! window is one settlement lag wide and is where corrections actually arrive,
//! so it always resolves — and it is small enough that this does not matter.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_plan::ExecutionPlan;

use crate::arrow::datatypes::SchemaRef;
use crate::tiering::store::ColdStore;

use super::provider::TieredTableProvider;
use super::version::{self, Resolution};

/// A tiered table with corrections resolved, eliding the work when statistics allow.
pub struct ResolvedTableProvider {
    /// The raw tiered table, every version present.
    raw: Arc<TieredTableProvider>,
    /// The cold tier, for the statistics that decide elision.
    cold: Arc<dyn ColdStore>,
    /// Physical table name, for statistics lookups and metrics.
    table: String,
    /// The resolution plan over the raw table, planned once at construction.
    ///
    /// Built from [`version::resolution_sql_with_key`] — the same text published
    /// for external engines — so the two cannot drift.
    resolution: LogicalPlan,
    schema: SchemaRef,
}

impl std::fmt::Debug for ResolvedTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedTableProvider")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl ResolvedTableProvider {
    /// Wrap a raw tiered table with resolution.
    pub fn new(
        raw: Arc<TieredTableProvider>,
        cold: Arc<dyn ColdStore>,
        table: impl Into<String>,
        resolution: LogicalPlan,
    ) -> Self {
        let schema = Arc::new(resolution.schema().as_arrow().clone());
        Self {
            raw,
            cold,
            table: table.into(),
            resolution,
            schema,
        }
    }

    /// Whether resolution can be skipped without reading statistics.
    ///
    /// `None` means the cheap checks were inconclusive and the decision needs
    /// the per-file statistics — which is the only case where the *order* of the
    /// two catalogue reads matters, and therefore the only case
    /// [`resolution_from_stats`](Self::resolution_from_stats) is reached.
    ///
    /// The watermark is passed in rather than read here, and the scan then uses
    /// the same value. Reading it twice left a window in which archival advanced
    /// the boundary between the decision and the scan.
    fn resolution_without_stats(
        &self,
        watermark: crate::watermark::TieringWatermark,
        filters: &[Expr],
    ) -> Option<Resolution> {
        // A pinned read is judged from statistics of the *current* snapshot,
        // which describe files the pinned snapshot may not contain and miss
        // corrections it does. A version ceiling and a transaction-time ceiling
        // each change which version wins. None is provable from here, so such a
        // read always resolves rather than eliding — eliding would scan the raw
        // rows directly and bypass the ceiling entirely.
        if self.raw.mode().forces_resolution() {
            return Some(Resolution::Required);
        }

        let split = self.raw.split_at(watermark, filters);

        // Any hot rows in range: the hot tier has no per-file statistics, so
        // nothing here can prove it correction-free.
        if split.hot.is_some() {
            return Some(Resolution::Required);
        }
        if split.cold.is_none() {
            // Neither tier in range — an empty scan resolves to itself.
            return Some(Resolution::Elided);
        }
        None
    }

    /// The decision the per-file statistics support.
    ///
    /// # This is read *after* the scan is built, and that ordering is the point
    ///
    /// `IcebergTableProvider::scan` loads the table when it is called and pins
    /// the snapshot it found; `version_stats` loads the table when *it* is
    /// called. Two loads, two snapshots — and read in the other order, a late
    /// correction committing between them would be present in the scan and
    /// absent from the evidence. Elision would fire on statistics that did not
    /// describe the files about to be read, and the corrected interval would come
    /// back twice, in a settlement figure, with nothing reporting it.
    ///
    /// Read afterwards, the statistics come from a snapshot **at least as new**
    /// as the scan's. A file the statistics know about and the scan will not read
    /// can only add overlap, and overlap only ever pushes the decision towards
    /// `Required` — the direction this module is already committed to, where
    /// being wrong costs a window function and never a wrong number.
    ///
    /// Errors are not propagated: failing to *prove* elision is not a query
    /// failure, it just means doing the work. A statistics read that fails should
    /// make the query slow, never wrong and never broken.
    async fn resolution_from_stats(
        &self,
        watermark: crate::watermark::TieringWatermark,
        filters: &[Expr],
    ) -> Resolution {
        let Some(cold) = self.raw.split_at(watermark, filters).cold else {
            return Resolution::Required;
        };

        // An open bound means "everything the tier holds on that side", so it
        // widens to the extremes rather than defaulting to a narrow window that
        // would exclude files the scan will actually read.
        let bounds = (
            cold.start().unwrap_or(time::OffsetDateTime::UNIX_EPOCH),
            cold.end().unwrap_or_else(|| {
                time::OffsetDateTime::UNIX_EPOCH + time::Duration::days(365_000)
            }),
        );

        match self.cold.version_stats(&self.table, bounds).await {
            Ok(stats) => version::plan(&stats),
            Err(_) => Resolution::Required,
        }
    }
}

#[async_trait]
impl TableProvider for ResolvedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let watermark = match super::PlannedWatermarks::of(state, &self.table) {
            Some(pinned) => pinned,
            None => self.raw.watermark().await?,
        };

        // Where the cheap checks settle it, nothing is built speculatively.
        // Where they do not, the raw scan is built **first** so the statistics
        // that judge it are read from a snapshot at least as new as the one it
        // pinned — see `resolution_from_stats`.
        let (resolution, raw_scan) = match self.resolution_without_stats(watermark, filters) {
            Some(decided) => (decided, None),
            None => {
                let scan = self
                    .raw
                    .scan_at(state, watermark, projection, filters, limit)
                    .await?;
                (
                    self.resolution_from_stats(watermark, filters).await,
                    Some(scan),
                )
            }
        };

        let metrics = crate::observe::metrics();
        let attrs = crate::observe::table(&self.table);
        metrics.merge_elision_decisions.add(1, &attrs);
        if resolution.is_elided() {
            metrics.merge_elided.add(1, &attrs);
        }

        if resolution.is_elided() {
            // The raw table already emits one row per key, so it *is* the
            // resolved table for this range — scanned against the same watermark
            // the elision decision was made against, and against the snapshot the
            // statistics were judged against.
            return match raw_scan {
                Some(scan) => Ok(scan),
                None => {
                    self.raw
                        .scan_at(state, watermark, projection, filters, limit)
                        .await
                }
            };
        }

        let mut builder = LogicalPlanBuilder::from(self.resolution.clone());
        // Pushed into the plan as well as being re-checked above it: the filters
        // are what the tier split reads to decide which tiers to touch at all,
        // so dropping them here would widen every scan to the full table.
        for filter in filters {
            builder = builder.filter(filter.clone())?;
        }
        if let Some(indices) = projection {
            let exprs = indices
                .iter()
                .map(|i| {
                    datafusion::logical_expr::col(datafusion::common::Column::from_name(
                        self.schema.field(*i).name(),
                    ))
                })
                .collect::<Vec<_>>();
            builder = builder.project(exprs)?;
        }
        if let Some(fetch) = limit {
            builder = builder.limit(0, Some(fetch))?;
        }

        state.create_physical_plan(&builder.build()?).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Always inexact. The filter reaches the tier split and the cold scan,
        // but a window function sits between them and the output, so the engine
        // must re-apply it above rather than trusting this provider to have.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
