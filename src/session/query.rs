//! Query results that carry the tier boundary they were computed against.
//!
//! P1 says correctness is observable rather than assumed, and this is where that
//! becomes concrete. A bare `Vec<RecordBatch>` from a tiered store is missing the
//! one fact needed to reason about it: *which boundary was in force*. Two
//! identical queries a minute apart can read the same rows from different tiers,
//! and a sum that looks stable is only stable because nothing was archived in
//! between.
//!
//! So a result records the watermark and which tiers actually produced rows.
//! Both are cheap — the watermark was read to plan the scan, and the tiers come
//! from the physical plan the engine already built.
//!
//! # Why the tiers are read off the plan
//!
//! The alternative is for the provider to report what it did through shared
//! state, which is unreliable the moment two queries run concurrently. The plan
//! is the authority: it *is* the decision, it belongs to this query alone, and
//! DataFusion hands it over before execution.
//!
//! The nodes are identified by **type**, not by name. A renamed node in a
//! pre-1.0 dependency then fails to compile, rather than silently reporting that
//! no cold tier was scanned — which would be a lie of exactly the kind P6
//! forbids.

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

use crate::arrow::array::RecordBatch;
use crate::arrow::datatypes::SchemaRef;
use crate::error::{Error, Result};
use crate::planner::ReadMode;
use crate::watermark::{Tier, TieringWatermark};

/// Rows, plus the provenance needed to reason about them.
#[derive(Debug, Clone)]
pub struct QueryResult {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    watermark: TieringWatermark,
    /// One entry per table the session hosts, in name order.
    ///
    /// A single-table query has exactly one and [`watermark`](Self::watermark)
    /// is it. A query across a [`MeterCatalog`] has several, because two tables
    /// genuinely have two boundaries (§15.3) — collapsing them to one number
    /// would be the fiction that carrying provenance exists to prevent.
    ///
    /// [`MeterCatalog`]: crate::session::MeterCatalog
    watermarks: Vec<(String, TieringWatermark)>,
    tiers: Vec<Tier>,
    read_mode: ReadMode,
}

impl QueryResult {
    /// Assemble a result from its batches and the provenance of its plan.
    pub(crate) fn new(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        watermarks: Vec<(String, TieringWatermark)>,
        tiers: Vec<Tier>,
        read_mode: ReadMode,
    ) -> Self {
        // The conservative boundary: below the oldest of them, every table
        // involved is settled. A newer one would claim settlement for a table
        // that has not reached it.
        let watermark = watermarks
            .iter()
            .map(|(_, w)| *w)
            .min()
            .unwrap_or_else(TieringWatermark::empty);
        Self {
            batches,
            schema,
            watermark,
            watermarks,
            tiers,
            read_mode,
        }
    }

    /// The rows.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Take the rows, dropping the provenance.
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    /// The result schema.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Total rows returned.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// The rows as JSON objects, one per row, column name → value.
    ///
    /// A convenience for the REST and serving surfaces: the batches already are
    /// the query's answer, and every consumer that speaks JSON would otherwise
    /// re-implement the same Arrow→JSON walk. Provenance is deliberately left out
    /// — [`watermark`](Self::watermark) and [`tiers_scanned`](Self::tiers_scanned)
    /// are read off the result and surfaced however the caller sees fit, rather
    /// than folded into every row.
    ///
    /// An empty result is an empty `Vec`, not `[null]`.
    pub fn to_json(&self) -> Result<Vec<serde_json::Value>> {
        if self.num_rows() == 0 {
            return Ok(Vec::new());
        }
        let mut buf = Vec::new();
        let mut writer = crate::arrow::json::ArrayWriter::new(&mut buf);
        for batch in &self.batches {
            writer
                .write(batch)
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        writer.finish().map_err(|e| Error::Storage(e.to_string()))?;
        match serde_json::from_slice(&buf).map_err(|e| Error::Storage(e.to_string()))? {
            serde_json::Value::Array(rows) => Ok(rows),
            // `ArrayWriter` always frames its output as an array; anything else
            // would be a contract change in arrow rather than a case to handle.
            other => Ok(vec![other]),
        }
    }

    /// The tier boundary this query ran against.
    ///
    /// The number to record alongside any figure that will be reconciled later.
    /// Two runs that disagree, with different watermarks, disagree for a reason.
    ///
    /// Across several tables this is the **oldest** of their boundaries — the
    /// conservative one, below which every table involved is settled. Use
    /// [`watermarks`](Self::watermarks) when the per-table boundaries matter,
    /// which they do for anything that will be reconciled.
    pub fn watermark(&self) -> TieringWatermark {
        self.watermark
    }

    /// Every table's boundary, in name order.
    pub fn watermarks(&self) -> &[(String, TieringWatermark)] {
        &self.watermarks
    }

    /// Which tiers produced rows, cold first.
    pub fn tiers_scanned(&self) -> &[Tier] {
        &self.tiers
    }

    /// Whether the query crossed the boundary.
    pub fn spans_tiers(&self) -> bool {
        self.tiers.contains(&Tier::Cold) && self.tiers.contains(&Tier::Hot)
    }

    /// Whether any row came from PostgreSQL.
    ///
    /// A reporting query that expected to be reproducible and finds this true has
    /// read the mutable tier, and its result is only valid for the moment it ran.
    pub fn touched_hot_tier(&self) -> bool {
        self.tiers.contains(&Tier::Hot)
    }

    /// The read mode the query ran under.
    pub fn read_mode(&self) -> ReadMode {
        self.read_mode
    }
}

/// What a statement *would* produce, without running it.
///
/// The provenance half of a [`QueryResult`] — the schema, the boundary and the
/// tiers a scan would touch — obtained by planning and stopping there.
///
/// # Why this is a separate call rather than an empty result
///
/// A `QueryResult` with no batches means *the query returned nothing*, which is
/// a real and different answer. Describing a statement is not executing it, and
/// conflating the two is how a surface ends up paying for a scan it never wanted:
/// Arrow Flight's `GetFlightInfo` and `CreatePreparedStatement` both need a
/// schema before any row is fetched, and answering them by running the query
/// makes a client's ordinary `GetFlightInfo` → `DoGet` sequence cost **two full
/// scans**.
///
/// Planning is not free either — it reads the tier boundary, and for the
/// resolved table the per-file statistics that decide elision — but that is a
/// catalogue read rather than a scan, which is what describing a statement
/// should cost.
#[derive(Debug, Clone)]
pub struct QueryDescription {
    schema: SchemaRef,
    watermark: TieringWatermark,
    watermarks: Vec<(String, TieringWatermark)>,
    tiers: Vec<Tier>,
    read_mode: ReadMode,
}

impl QueryDescription {
    pub(crate) fn new(
        schema: SchemaRef,
        watermarks: Vec<(String, TieringWatermark)>,
        tiers: Vec<Tier>,
        read_mode: ReadMode,
    ) -> Self {
        let watermark = watermarks
            .iter()
            .map(|(_, w)| *w)
            .min()
            .unwrap_or_else(TieringWatermark::empty);
        Self {
            schema,
            watermark,
            watermarks,
            tiers,
            read_mode,
        }
    }

    /// The schema the statement would produce.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// The tier boundary the statement would run against.
    pub fn watermark(&self) -> TieringWatermark {
        self.watermark
    }

    /// Every boundary involved, in name order.
    pub fn watermarks(&self) -> &[(String, TieringWatermark)] {
        &self.watermarks
    }

    /// Which tiers the plan would read from, cold first.
    pub fn tiers_scanned(&self) -> &[Tier] {
        &self.tiers
    }

    /// Whether the statement would cross the boundary.
    pub fn spans_tiers(&self) -> bool {
        self.tiers.contains(&Tier::Cold) && self.tiers.contains(&Tier::Hot)
    }

    /// Whether the statement would read PostgreSQL.
    pub fn touched_hot_tier(&self) -> bool {
        self.tiers.contains(&Tier::Hot)
    }

    /// The read mode it would run under.
    pub fn read_mode(&self) -> ReadMode {
        self.read_mode
    }
}

/// Which tiers a physical plan reads from.
///
/// Cold first, matching the order the tier split builds the union in, so the
/// list reads the way the timeline does.
pub(crate) fn tiers_of(plan: &Arc<dyn ExecutionPlan>) -> Vec<Tier> {
    let mut found = Vec::new();
    walk(plan, &mut found);
    let mut tiers = Vec::with_capacity(2);
    if found.contains(&Tier::Cold) {
        tiers.push(Tier::Cold);
    }
    if found.contains(&Tier::Hot) {
        tiers.push(Tier::Hot);
    }
    tiers
}

fn walk(plan: &Arc<dyn ExecutionPlan>, found: &mut Vec<Tier>) {
    let any = plan.as_any();
    if any
        .downcast_ref::<crate::planner::provider::HotScanExec>()
        .is_some()
    {
        found.push(Tier::Hot);
    } else if any
        .downcast_ref::<iceberg_datafusion::physical_plan::IcebergTableScan>()
        .is_some()
    {
        found.push(Tier::Cold);
    }
    for child in plan.children() {
        walk(child, found);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::datatypes::{DataType, Field, Schema};

    fn empty(tiers: Vec<Tier>) -> QueryResult {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        QueryResult::new(
            Vec::new(),
            schema,
            vec![(
                "readings_versions".to_string(),
                TieringWatermark::new(time::macros::datetime!(2026-07-20 00:00 UTC)),
            )],
            tiers,
            ReadMode::Unified,
        )
    }

    #[test]
    fn spanning_requires_both_tiers() {
        assert!(empty(vec![Tier::Cold, Tier::Hot]).spans_tiers());
        assert!(!empty(vec![Tier::Cold]).spans_tiers());
        assert!(!empty(vec![Tier::Hot]).spans_tiers());
        assert!(!empty(vec![]).spans_tiers());
    }

    #[test]
    fn a_reporting_query_can_tell_it_read_the_mutable_tier() {
        // The check a reproducibility claim rests on: if this is true, the
        // result is only valid for the instant it ran.
        assert!(empty(vec![Tier::Cold, Tier::Hot]).touched_hot_tier());
        assert!(!empty(vec![Tier::Cold]).touched_hot_tier());
    }

    #[test]
    fn the_watermark_travels_with_the_rows() {
        let r = empty(vec![Tier::Cold]);
        assert_eq!(
            r.watermark().get(),
            time::macros::datetime!(2026-07-20 00:00 UTC)
        );
    }

    #[test]
    fn an_empty_result_still_reports_row_count_zero() {
        assert_eq!(empty(vec![]).num_rows(), 0);
    }

    #[test]
    fn an_empty_result_serialises_to_an_empty_array() {
        // Not `[null]`, and not an error — a REST caller gets `[]`.
        assert_eq!(
            empty(vec![]).to_json().unwrap(),
            Vec::<serde_json::Value>::new()
        );
    }

    #[test]
    fn to_json_maps_each_row_to_an_object() {
        use crate::arrow::array::{Int64Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("malo_id", DataType::Utf8, false),
            Field::new("total_kwh", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["11111111115", "22222222220"])),
                Arc::new(Int64Array::from(vec![42, 7])),
            ],
        )
        .unwrap();
        let result = QueryResult::new(
            vec![batch],
            schema,
            vec![(
                "readings_versions".to_string(),
                TieringWatermark::new(time::macros::datetime!(2026-07-20 00:00 UTC)),
            )],
            vec![Tier::Cold],
            ReadMode::Unified,
        );

        let rows = result.to_json().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["malo_id"], "11111111115");
        assert_eq!(rows[0]["total_kwh"], 42);
        assert_eq!(rows[1]["malo_id"], "22222222220");
    }
}
