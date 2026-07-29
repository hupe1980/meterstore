//! Completeness: a missing interval is information, not an empty set.
//!
//! Regulators require knowing whether a series is complete, so an aggregate over
//! an incomplete month must not look like one over a complete month (P1). A
//! `SUM` cannot say that — it returns a smaller number and no reason.
//!
//! # Why the expected count is not 96
//!
//! `Europe/Berlin` gives **92**-interval and **100**-interval days at the DST
//! transitions. A check that assumes 96 raises a false alarm on every meter every
//! spring, and — worse — masks a genuine four-interval gap every autumn, which is
//! the direction that reaches a bill.
//!
//! The count comes from [`metering::calendar::intervals_in_day`] against the
//! series' own declared `resolution`. MeterStore does not implement it (P5), and
//! this module does not re-derive it: the aggregate below reports what was
//! *found*, and the expectation is asked of the domain per (day, resolution).
//!
//! # Where the work happens
//!
//! The heavy half — group a range by measuring point, channel and local day — is
//! a single DataFusion aggregate over the resolved table, so it prunes and
//! streams like any other query. The result is one row per meter-channel-day,
//! which is small: a month of 100 k meters is ~3 M rows in and ~3 M rows out at
//! day granularity, and the roll-up to one row per channel happens in Rust,
//! where the calendar lives.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{
    Expr, LogicalPlanBuilder, TableProviderFilterPushDown, TableType, col, lit,
};
use datafusion::physical_plan::ExecutionPlan;
use metering::IntervalResolution;
use metering::calendar;
use time::{Date, OffsetDateTime};

use crate::arrow::array::{Array, AsArray, Date32Array, Int64Array, RecordBatch, StringArray};
use crate::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use crate::encode::schema::col as column;
use crate::error::{Error, Result};

/// The completeness of one channel over a queried range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completeness {
    /// Marktlokation.
    pub malo_id: String,
    /// The measured channel.
    pub obis_code: String,
    /// The declared resolution, or `None` when the series does not carry one.
    ///
    /// Without it there is no expectation to compare against, and the row says
    /// so rather than assuming 15 minutes.
    pub resolution: Option<String>,
    /// Intervals the calendar says the range should hold.
    pub expected: u64,
    /// Intervals actually stored.
    pub actual: u64,
    /// `expected - actual`, floored at zero.
    ///
    /// More rows than expected is not a gap; it is a duplicate or a resolution
    /// mismatch, and [`Completeness::surplus`] reports it separately so the two
    /// are never confused.
    pub missing: u64,
    /// Rows beyond the expectation — a duplicate or a mis-declared resolution.
    pub surplus: u64,
    /// The first local day that is short, if any.
    pub first_gap: Option<Date>,
    /// Intervals carrying a substitute value (Ersatzwert).
    pub substituted: u64,
    /// Intervals whose quality bars them from billing.
    pub not_billable: u64,
}

impl Completeness {
    /// Whether the range holds exactly what the calendar expects.
    pub fn is_complete(&self) -> bool {
        self.missing == 0 && self.surplus == 0
    }

    /// Whether an expectation could be computed at all.
    ///
    /// False when the series declares no resolution, or declares a calendar one
    /// (`P1M`) that has no fixed interval count within a day. The row still
    /// reports `actual`; it just cannot call it complete or short.
    pub fn is_measurable(&self) -> bool {
        self.resolution.is_some() && self.expected > 0
    }
}

/// The daily grain the aggregate produces, before roll-up.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DailyRow {
    malo_id: String,
    obis_code: String,
    resolution: Option<String>,
    day: Date,
    actual: u64,
    substituted: u64,
    not_billable: u64,
}

/// The result schema, in the order §9.6 documents.
pub fn completeness_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("malo_id", DataType::Utf8, false),
        Field::new("obis_code", DataType::Utf8, false),
        Field::new("resolution", DataType::Utf8, true),
        Field::new("expected", DataType::Int64, false),
        Field::new("actual", DataType::Int64, false),
        Field::new("missing", DataType::Int64, false),
        Field::new("surplus", DataType::Int64, false),
        Field::new("first_gap", DataType::Date32, true),
        Field::new("substituted", DataType::Int64, false),
        Field::new("not_billable", DataType::Int64, false),
        Field::new("complete", DataType::Boolean, false),
    ]))
}

/// Encode completeness rows as a batch.
pub fn completeness_batch(rows: &[Completeness]) -> Result<RecordBatch> {
    use crate::arrow::array::BooleanArray;

    let epoch = Date::from_ordinal_date(1970, 1).expect("epoch is a valid date");
    let as_i64 = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);

    Ok(RecordBatch::try_new(
        completeness_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.malo_id.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.obis_code.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.resolution.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| as_i64(r.expected)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| as_i64(r.actual)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| as_i64(r.missing)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| as_i64(r.surplus)).collect::<Vec<_>>(),
            )),
            Arc::new(Date32Array::from(
                rows.iter()
                    .map(|r| r.first_gap.map(|d| (d - epoch).whole_days() as i32))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| as_i64(r.substituted))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| as_i64(r.not_billable))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(Completeness::is_complete)
                    .collect::<Vec<_>>(),
            )),
        ],
    )?)
}

/// Every quality flag `metering` treats as a substitute value.
fn is_substitute(quality: &str) -> bool {
    quality == metering::QualityFlag::Substituted.as_str()
}

/// Whether a stored quality string bars the reading from billing.
///
/// Delegated to `metering`: billability is § 60 Abs. 2 MsbG, and a local copy of
/// the rule would be a second thing to keep in step with the statute. An
/// unparseable flag is treated as not billable, which is the safe direction.
fn is_billable(quality: &str) -> bool {
    quality
        .parse::<metering::QualityFlag>()
        .map(|q| q.is_billable())
        .unwrap_or(false)
}

/// The aggregate that produces the daily grain.
///
/// Grouped by the **local** day, because that is the unit the expectation is
/// defined over. Grouping by UTC day would put 22:00–24:00 local in the wrong
/// bucket every day of the year, and the resulting counts would be short by two
/// hours at one end and long at the other — the exact error §9.5 exists to stop.
fn daily_plan(
    resolved: Arc<dyn TableProvider>,
    table: &str,
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> DfResult<datafusion::logical_expr::LogicalPlan> {
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion::logical_expr::ScalarUDF;

    let local_day = ScalarUDF::from(super::udf::LocalDay::default());
    let ts = |t: OffsetDateTime| {
        lit(ScalarValue::TimestampMicrosecond(
            Some((t.unix_timestamp_nanos() / 1_000) as i64),
            Some("UTC".into()),
        ))
    };

    // `quality` is a **group key**, not an aggregate. Substitution and
    // billability are domain rules — § 60 Abs. 2 MsbG for the latter — so the
    // query counts flags and Rust asks `metering` which of them mean what. A
    // `CASE` listing the billable set here would be a second copy of the statute
    // to keep in step (P5).
    LogicalPlanBuilder::scan(
        table.to_string(),
        datafusion::datasource::provider_as_source(resolved),
        None,
    )?
    .filter(
        col(column::FROM)
            .gt_eq(ts(from))
            .and(col(column::FROM).lt(ts(to))),
    )?
    .aggregate(
        vec![
            col(column::MALO_ID),
            col(column::OBIS_CODE),
            col(column::RESOLUTION),
            local_day.call(vec![col(column::FROM)]).alias("day"),
            col(column::QUALITY),
        ],
        vec![count(lit(1i64)).alias("actual")],
    )?
    .build()
}

/// Run the aggregate and roll it up to one row per channel.
pub(crate) async fn compute(
    state: &dyn Session,
    resolved: Arc<dyn TableProvider>,
    table: &str,
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> Result<Vec<Completeness>> {
    if to <= from {
        return Err(Error::config(format!(
            "completeness range end {to} must be after start {from}"
        )));
    }

    let plan = daily_plan(resolved, table, from, to)?;
    let physical = state.create_physical_plan(&plan).await?;
    let batches = datafusion::physical_plan::collect(physical, state.task_ctx()).await?;

    let mut daily: Vec<DailyRow> = Vec::new();
    for batch in &batches {
        daily.extend(decode_daily(batch)?);
    }

    Ok(roll_up(daily, from, to))
}

/// Read the aggregate's output back into typed rows.
fn decode_daily(batch: &RecordBatch) -> Result<Vec<DailyRow>> {
    let epoch = Date::from_ordinal_date(1970, 1).expect("epoch is a valid date");

    let text = |name: &str| -> Result<&StringArray> {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_string_opt::<i32>())
            .ok_or_else(|| Error::decode(name, "expected a string column"))
    };

    let malo = text(column::MALO_ID)?;
    let obis = text(column::OBIS_CODE)?;
    let resolution = text(column::RESOLUTION)?;
    let quality = text(column::QUALITY)?;
    let day = batch
        .column_by_name("day")
        .and_then(|c| c.as_any().downcast_ref::<Date32Array>())
        .ok_or_else(|| Error::decode("day", "expected a date column"))?;
    let actual = batch
        .column_by_name("actual")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| Error::decode("actual", "expected an i64 column"))?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        if day.is_null(i) {
            continue;
        }
        // Quality is a group key, so every row of this group carries the same
        // flag and the whole count either is or is not substituted.
        let count = actual.value(i).max(0) as u64;
        let flag = quality.value(i);
        out.push(DailyRow {
            malo_id: malo.value(i).to_string(),
            obis_code: obis.value(i).to_string(),
            resolution: (!resolution.is_null(i)).then(|| resolution.value(i).to_string()),
            day: epoch
                .checked_add(time::Duration::days(i64::from(day.value(i))))
                .ok_or_else(|| Error::decode("day", "date out of range"))?,
            actual: count,
            substituted: if is_substitute(flag) { count } else { 0 },
            not_billable: if is_billable(flag) { 0 } else { count },
        });
    }
    Ok(out)
}

/// Aggregate daily rows into per-channel completeness.
///
/// The expectation is asked of `metering` once per (day, resolution). A day
/// partly outside the queried range is expected to hold only the part inside it,
/// which is what makes a range that is not day-aligned report honestly rather
/// than claiming a gap at each end.
fn roll_up(daily: Vec<DailyRow>, from: OffsetDateTime, to: OffsetDateTime) -> Vec<Completeness> {
    use std::collections::BTreeMap;

    // Key by channel; within a channel, days are folded together.
    struct Accumulator {
        resolution: Option<String>,
        expected: u64,
        actual: u64,
        surplus: u64,
        first_gap: Option<Date>,
        substituted: u64,
        not_billable: u64,
    }

    let mut by_channel: BTreeMap<(String, String), Accumulator> = BTreeMap::new();
    // Days are visited once per quality flag, so the expectation must only be
    // added the first time a (channel, day) pair is seen.
    let mut counted: std::collections::BTreeSet<(String, String, Date)> = Default::default();
    // A day is short only once every flag for it has been folded in, so gaps are
    // decided after the fold rather than per row.
    let mut per_day: BTreeMap<(String, String, Date), (u64, u64)> = BTreeMap::new();

    for row in daily {
        let key = (row.malo_id.clone(), row.obis_code.clone());
        let entry = by_channel.entry(key.clone()).or_insert(Accumulator {
            resolution: row.resolution.clone(),
            expected: 0,
            actual: 0,
            surplus: 0,
            first_gap: None,
            substituted: 0,
            not_billable: 0,
        });
        entry.actual += row.actual;
        entry.substituted += row.substituted;
        entry.not_billable += row.not_billable;
        if entry.resolution.is_none() {
            entry.resolution = row.resolution.clone();
        }

        let day_key = (row.malo_id, row.obis_code, row.day);
        let expected = if counted.insert(day_key.clone()) {
            let n = expected_in_day(row.day, row.resolution.as_deref(), from, to);
            entry.expected += n;
            n
        } else {
            0
        };

        let slot = per_day.entry(day_key).or_insert((0, 0));
        slot.0 += row.actual;
        slot.1 += expected;
    }

    for ((malo, obis, day), (actual, expected)) in per_day {
        let Some(entry) = by_channel.get_mut(&(malo, obis)) else {
            continue;
        };
        // Zero expected means the day is unmeasurable — no declared resolution,
        // or a calendar one with no fixed count within a day. Every row in the
        // result is inside the queried range by construction, so this is never a
        // day that merely fell outside it. Judging such a day would report the
        // whole series as surplus, which reads as a duplicate problem when the
        // real state is "nothing to compare against".
        if expected == 0 {
            continue;
        }
        if expected > actual {
            entry.first_gap = Some(entry.first_gap.map_or(day, |d| d.min(day)));
        }
        entry.surplus += actual.saturating_sub(expected);
    }

    by_channel
        .into_iter()
        .map(|((malo_id, obis_code), a)| Completeness {
            malo_id,
            obis_code,
            resolution: a.resolution,
            expected: a.expected,
            actual: a.actual,
            missing: a.expected.saturating_sub(a.actual),
            surplus: a.surplus,
            first_gap: a.first_gap,
            substituted: a.substituted,
            not_billable: a.not_billable,
        })
        .collect()
}

/// How many intervals a local day should hold, clipped to the queried range.
///
/// Zero when the resolution is absent or is a calendar one — there is no fixed
/// count within a day for `P1M`, and inventing 96 would report a month-resolution
/// series as 95 intervals short every day.
fn expected_in_day(
    day: Date,
    resolution: Option<&str>,
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> u64 {
    let Some(parsed) = resolution.and_then(|r| r.parse::<IntervalResolution>().ok()) else {
        return 0;
    };
    let Some(full) = calendar::intervals_in_day(day, parsed) else {
        return 0;
    };
    let full = u64::from(full);

    let start = calendar::day_start_utc(day);
    let length = calendar::day_length(day);
    let end = start + length;

    // Fully inside the range: the common case, and the only one where the day's
    // own length is the whole answer.
    if start >= from && end <= to {
        return full;
    }

    // Partly outside: expect only the covered fraction. A range that is not
    // day-aligned — a billing period starting mid-day — must not report the
    // uncovered half as missing.
    let covered = end.min(to) - start.max(from);
    if covered <= time::Duration::ZERO {
        return 0;
    }
    let step = length / full as i32;
    if step <= time::Duration::ZERO {
        return 0;
    }
    (covered.whole_seconds() / step.whole_seconds()).max(0) as u64
}

/// `meter_completeness(from, to)` — completeness as a queryable table.
///
/// Registered by [`MeterStore`], so the table it reports on is the store's own.
/// The optional leading argument names it, matching §9.6's spelling; passing a
/// different name is an error rather than a silent report on the wrong table.
///
/// [`MeterStore`]: crate::session::MeterStore
#[derive(Debug)]
pub struct CompletenessFunction {
    resolved: Arc<dyn TableProvider>,
    table: String,
}

impl CompletenessFunction {
    /// Bind the function to a store's resolved table.
    pub fn new(resolved: Arc<dyn TableProvider>, table: impl Into<String>) -> Self {
        Self {
            resolved,
            table: table.into(),
        }
    }

    /// The SQL name.
    pub const NAME: &'static str = "meter_completeness";
}

impl datafusion::catalog::TableFunctionImpl for CompletenessFunction {
    fn call(&self, args: &[Expr]) -> DfResult<Arc<dyn TableProvider>> {
        let (name, from, to) = match args {
            [from, to] => (None, from, to),
            [name, from, to] => (Some(as_string(name)?), from, to),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "{}(from, to) or {}(table, from, to)",
                    Self::NAME,
                    Self::NAME
                )));
            }
        };

        if let Some(requested) = name
            && requested != self.table
            && requested != crate::session::store::resolved_name(&self.table)
        {
            return Err(DataFusionError::Plan(format!(
                "this store manages {:?}, not {requested:?}",
                self.table
            )));
        }

        Ok(Arc::new(CompletenessProvider {
            resolved: Arc::clone(&self.resolved),
            table: self.table.clone(),
            from: as_instant(from)?,
            to: as_instant(to)?,
        }))
    }
}

/// Reads a literal argument as text.
fn as_string(expr: &Expr) -> DfResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Ok(s.clone()),
        other => Err(DataFusionError::Plan(format!(
            "expected a string literal, got {other}"
        ))),
    }
}

/// Reads a literal argument as an instant.
///
/// Accepts the spellings SQL produces for a timestamp literal, including a bare
/// string — `'2026-03-01'` is what an operator types, and rejecting it in favour
/// of `TIMESTAMP '2026-03-01'` would be pedantry rather than safety.
fn as_instant(expr: &Expr) -> DfResult<OffsetDateTime> {
    use time::format_description::well_known::Rfc3339;

    let micros = match expr {
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(v), _), _) => Some(*v),
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(v), _), _) => Some(v * 1_000),
        Expr::Literal(ScalarValue::TimestampSecond(Some(v), _), _) => Some(v * 1_000_000),
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), _), _) => Some(v / 1_000),
        Expr::Literal(ScalarValue::Date32(Some(days)), _) => {
            Some(i64::from(*days) * 86_400 * 1_000_000)
        }
        _ => None,
    };

    if let Some(micros) = micros {
        return OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
            .map_err(|e| DataFusionError::Plan(format!("timestamp out of range: {e}")));
    }

    let text = as_string(expr)?;
    if let Ok(t) = OffsetDateTime::parse(&text, &Rfc3339) {
        return Ok(t);
    }
    // A bare date, which is how a settlement range is normally written.
    time::Date::parse(
        &text,
        &time::macros::format_description!("[year]-[month]-[day]"),
    )
    .map(|d| d.midnight().assume_utc())
    .map_err(|e| {
        DataFusionError::Plan(format!(
            "{text:?} is not an RFC 3339 timestamp or a YYYY-MM-DD date: {e}"
        ))
    })
}

/// The provider one `meter_completeness(...)` call produces.
struct CompletenessProvider {
    resolved: Arc<dyn TableProvider>,
    table: String,
    from: OffsetDateTime,
    to: OffsetDateTime,
}

impl std::fmt::Debug for CompletenessProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletenessProvider")
            .field("table", &self.table)
            .field("from", &self.from)
            .field("to", &self.to)
            .finish()
    }
}

#[async_trait]
impl TableProvider for CompletenessProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        completeness_schema()
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
        let rows = compute(
            state,
            Arc::clone(&self.resolved),
            &self.table,
            self.from,
            self.to,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let batch =
            completeness_batch(&rows).map_err(|e| DataFusionError::External(Box::new(e)))?;
        MemTable::try_new(completeness_schema(), vec![vec![batch]])?
            .scan(state, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::{date, datetime};

    const FROM: OffsetDateTime = datetime!(2026-03-01 00:00 UTC);
    const TO: OffsetDateTime = datetime!(2026-04-01 00:00 UTC);

    fn row(day: Date, actual: u64, quality: &str) -> DailyRow {
        DailyRow {
            malo_id: "12345678901".into(),
            obis_code: "1-0:1.8.0".into(),
            resolution: Some("PT15M".into()),
            day,
            actual,
            substituted: if quality == "SUBSTITUTED" { actual } else { 0 },
            not_billable: if is_billable(quality) { 0 } else { actual },
        }
    }

    #[test]
    fn a_full_ordinary_day_is_complete() {
        let out = roll_up(vec![row(date!(2026 - 03 - 02), 96, "MEASURED")], FROM, TO);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].expected, 96);
        assert_eq!(out[0].actual, 96);
        assert!(out[0].is_complete());
        assert_eq!(out[0].first_gap, None);
    }

    #[test]
    fn the_spring_forward_day_expects_92_not_96() {
        // The false alarm a hardcoded 96 raises for every meter every spring.
        let out = roll_up(vec![row(date!(2026 - 03 - 29), 92, "MEASURED")], FROM, TO);
        assert_eq!(out[0].expected, 92);
        assert!(out[0].is_complete(), "92 intervals is a complete DST day");
    }

    #[test]
    fn the_autumn_day_expects_100_so_a_gap_is_visible() {
        // The dangerous direction: assuming 96 would call a four-interval gap
        // complete, and the shortfall would reach a bill.
        let out = roll_up(
            vec![row(date!(2026 - 10 - 25), 96, "MEASURED")],
            datetime!(2026-10-01 00:00 UTC),
            datetime!(2026-11-01 00:00 UTC),
        );
        assert_eq!(out[0].expected, 100);
        assert_eq!(out[0].missing, 4);
        assert!(!out[0].is_complete());
        assert_eq!(out[0].first_gap, Some(date!(2026 - 10 - 25)));
    }

    #[test]
    fn the_first_gap_is_the_earliest_short_day() {
        let out = roll_up(
            vec![
                row(date!(2026 - 03 - 05), 90, "MEASURED"),
                row(date!(2026 - 03 - 02), 80, "MEASURED"),
                row(date!(2026 - 03 - 03), 96, "MEASURED"),
            ],
            FROM,
            TO,
        );
        assert_eq!(out[0].first_gap, Some(date!(2026 - 03 - 02)));
        assert_eq!(out[0].missing, 96 * 3 - (90 + 80 + 96));
    }

    #[test]
    fn substitutes_and_unbillable_rows_are_counted_separately() {
        // A day can be complete and still not billable, and an operator needs
        // to see both — a full month of substitutes is not the same as a gap.
        let out = roll_up(
            vec![
                row(date!(2026 - 03 - 02), 90, "MEASURED"),
                row(date!(2026 - 03 - 02), 6, "SUBSTITUTED"),
            ],
            FROM,
            TO,
        );
        assert_eq!(out[0].actual, 96);
        assert!(out[0].is_complete());
        assert_eq!(out[0].substituted, 6);
        assert_eq!(
            out[0].not_billable, 0,
            "SUBSTITUTED is billable under § 60 Abs. 2 MsbG"
        );
    }

    #[test]
    fn a_faulty_reading_is_present_but_not_billable() {
        let out = roll_up(
            vec![
                row(date!(2026 - 03 - 02), 90, "MEASURED"),
                row(date!(2026 - 03 - 02), 6, "FAULTY"),
            ],
            FROM,
            TO,
        );
        assert!(out[0].is_complete(), "the intervals are present");
        assert_eq!(out[0].not_billable, 6, "and six of them cannot be billed");
    }

    #[test]
    fn a_series_with_no_resolution_is_not_measurable() {
        // Without a declared resolution there is no expectation. Assuming 15
        // minutes would invent a gap or invent completeness.
        let mut r = row(date!(2026 - 03 - 02), 24, "MEASURED");
        r.resolution = None;
        let out = roll_up(vec![r], FROM, TO);
        assert!(!out[0].is_measurable());
        assert_eq!(out[0].expected, 0);
        assert_eq!(out[0].actual, 24);
        assert_eq!(
            out[0].surplus, 0,
            "with nothing to compare against, every row is not surplus"
        );
        assert_eq!(out[0].missing, 0);
        assert_eq!(out[0].first_gap, None);
    }

    #[test]
    fn a_calendar_resolution_has_no_daily_expectation() {
        let mut r = row(date!(2026 - 03 - 02), 1, "MEASURED");
        r.resolution = Some("P1M".into());
        let out = roll_up(vec![r], FROM, TO);
        assert_eq!(out[0].expected, 0, "a month is not n intervals in a day");
        assert!(!out[0].is_measurable());
        assert_eq!(out[0].surplus, 0);
    }

    #[test]
    fn duplicates_are_surplus_rather_than_negative_gaps() {
        // More rows than the calendar allows is a real condition — a duplicate,
        // or a mis-declared resolution — and it must not cancel out a gap
        // elsewhere in the range.
        let out = roll_up(
            vec![
                row(date!(2026 - 03 - 02), 120, "MEASURED"),
                row(date!(2026 - 03 - 03), 90, "MEASURED"),
            ],
            FROM,
            TO,
        );
        assert_eq!(out[0].surplus, 24);
        assert_eq!(out[0].first_gap, Some(date!(2026 - 03 - 03)));
        assert!(!out[0].is_complete());
    }

    #[test]
    fn a_range_that_covers_part_of_a_day_expects_part_of_it() {
        // A billing period starting at noon must not report the morning as
        // missing.
        let out = roll_up(
            vec![row(date!(2026 - 03 - 02), 48, "MEASURED")],
            datetime!(2026-03-02 11:00 UTC), // 12:00 Berlin
            datetime!(2026-03-03 00:00 UTC),
        );
        assert_eq!(out[0].expected, 48);
        assert!(out[0].is_complete());
    }

    #[test]
    fn channels_are_reported_separately() {
        let mut second = row(date!(2026 - 03 - 02), 96, "MEASURED");
        second.obis_code = "1-0:2.8.0".into();
        let out = roll_up(
            vec![row(date!(2026 - 03 - 02), 96, "MEASURED"), second],
            FROM,
            TO,
        );
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn a_batch_matches_the_published_schema() {
        let rows = roll_up(vec![row(date!(2026 - 03 - 02), 90, "MEASURED")], FROM, TO);
        let batch = completeness_batch(&rows).unwrap();
        assert_eq!(batch.schema(), completeness_schema());
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn expected_in_day_knows_the_dst_days() {
        assert_eq!(
            expected_in_day(date!(2026 - 03 - 29), Some("PT15M"), FROM, TO),
            92
        );
        assert_eq!(
            expected_in_day(
                date!(2026 - 10 - 25),
                Some("PT15M"),
                datetime!(2026-10-01 00:00 UTC),
                datetime!(2026-11-01 00:00 UTC)
            ),
            100
        );
    }
}
