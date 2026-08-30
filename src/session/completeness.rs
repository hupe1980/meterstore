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
//! # And why the day is not always the calendar day
//!
//! Gas balances on the **Gastag**, 06:00 to 06:00 local, so a gas channel's day
//! is not the day electricity's is. Both the bucketing and the expected count
//! follow the row's `sparte` ([`crate::planner::balancing_day`]), which matters
//! twice over at a DST transition: the clocks change at 02:00/03:00 local,
//! *before* the 06:00 boundary, so the 100-interval gas day is the one named
//! after the **Saturday** while the 100-interval calendar day is the Sunday.
//! Using calendar days for gas would report the Sunday four short and the
//! Saturday four in surplus — two findings, neither real, in the one report an
//! operator is meant to be able to trust.
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
use metering::interval::Sparte;
use time::{Date, OffsetDateTime};

use crate::planner::calendar as balancing;

use crate::arrow::array::{Array, AsArray, Date32Array, Int64Array, RecordBatch, StringArray};
use crate::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use crate::encode::schema::col as column;
use crate::error::{Error, Result};

/// The completeness of one channel over a queried range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completeness {
    /// Marktlokation.
    ///
    /// Deliberately the stored string rather than a parsed
    /// [`MaloId`](metering::ids::MaloId): completeness is the report an operator
    /// runs *to find out what is wrong*, and a single malformed identifier
    /// anywhere in the range must not take the other hundred thousand rows down
    /// with it. The typed read path parses; this one reports.
    pub malo_id: String,
    /// The measured channel.
    pub obis_code: String,
    /// The merge-key columns beyond `(malo_id, obis_code, from)`, in key order.
    ///
    /// A declared identity column, and `melo_id` where the table identifies a
    /// reading by its Messlokation. Empty on a plain single-tenant Lastgang.
    ///
    /// **Reported because it is grouped on**, and grouped on because a merge key
    /// is what makes two rows two readings. Folded together, two tenants — or
    /// the two meters of a Mehrfamilienhaus — put 192 intervals against an
    /// expectation of 96 and the report claims a surplus of 96 on a range where
    /// nothing is wrong. The reverse is worse: one tenant complete and the other
    /// missing a day reads as complete overall.
    pub identity: Vec<(String, String)>,
    /// The commodity, which decides **which day** the channel is balanced on:
    /// the Gastag for [`Sparte::Gas`], the Berlin calendar day for the rest.
    pub sparte: Sparte,
    /// The declared resolution, or `None` when the series does not carry one.
    ///
    /// Without it there is no expectation to compare against, and the row says
    /// so rather than assuming 15 minutes.
    ///
    /// **Grouped on, so this is the row's grid rather than one of several.** A
    /// channel converted from an hourly profile to a quarter-hourly one mid-range
    /// holds both, and it is the grid that decides whether a day of 24 values is
    /// complete or 72 short. Folded, the report would name an arbitrary one of
    /// them against a count drawn from both — arbitrary literally, since the
    /// aggregate yields its groups in no defined order.
    pub resolution: Option<String>,
    /// Intervals the calendar says the range should hold.
    pub expected: u64,
    /// Intervals actually stored.
    pub actual: u64,
    /// Intervals the range should hold and does not, summed **per balancing day**.
    ///
    /// Deliberately not `expected - actual` over the whole range. More rows than
    /// expected is not a negative gap — it is a duplicate or a mis-declared
    /// resolution, a different condition entirely — so a surplus on one day must
    /// not cancel a shortfall on another. Computed over the totals it would: a
    /// channel four intervals long on Tuesday and four short on Wednesday would
    /// report as complete, which is the one answer a completeness report must
    /// never give. [`Completeness::surplus`] carries the other direction.
    pub missing: u64,
    /// Rows beyond the expectation, summed per balancing day — a duplicate or a
    /// mis-declared resolution.
    pub surplus: u64,
    /// The first balancing day that is short, if any.
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

    /// Whether the channel delivered **nothing at all** over the range.
    ///
    /// The strongest form of incompleteness, and the one an aggregate over the
    /// range alone cannot see: a channel with no rows produces no groups, so it
    /// is absent from the report rather than reported as empty. A row like this
    /// exists only because the query was given a reference window to draw a
    /// roster of channels from — see
    /// [`CompletenessQuery::seen_since`](crate::session::CompletenessQuery::seen_since).
    ///
    /// In SQL it is `actual = 0`.
    pub fn is_silent(&self) -> bool {
        self.actual == 0
    }
}

/// The daily grain the aggregate produces, before roll-up.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DailyRow {
    malo_id: String,
    obis_code: String,
    /// The merge-key discriminators, in key order.
    identity: Vec<(String, String)>,
    sparte: Sparte,
    resolution: Option<String>,
    day: Date,
    actual: u64,
    substituted: u64,
    not_billable: u64,
}

/// The result schema, in the order §9.6 documents.
///
/// `discriminators` are the table's merge-key columns beyond
/// `(malo_id, obis_code, from)`. They sit next to the identifiers they extend,
/// because a report is read rather than indexed: a row naming a Marktlokation
/// and a channel but not the tenant or the meter it belongs to is not
/// actionable.
pub fn completeness_schema(discriminators: &[String]) -> SchemaRef {
    let mut fields = vec![
        Field::new("malo_id", DataType::Utf8, false),
        Field::new("obis_code", DataType::Utf8, false),
    ];
    fields.extend(
        discriminators
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, false)),
    );
    fields.extend([
        Field::new("sparte", DataType::Utf8, false),
        Field::new("resolution", DataType::Utf8, true),
        Field::new("expected", DataType::Int64, false),
        Field::new("actual", DataType::Int64, false),
        Field::new("missing", DataType::Int64, false),
        Field::new("surplus", DataType::Int64, false),
        Field::new("first_gap", DataType::Date32, true),
        Field::new("substituted", DataType::Int64, false),
        Field::new("not_billable", DataType::Int64, false),
        Field::new("complete", DataType::Boolean, false),
    ]);
    Arc::new(Schema::new(fields))
}

/// Encode completeness rows as a batch.
pub fn completeness_batch(rows: &[Completeness], discriminators: &[String]) -> Result<RecordBatch> {
    use crate::arrow::array::BooleanArray;

    let as_i64 = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);

    let mut columns: Vec<crate::arrow::array::ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.malo_id.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.obis_code.as_str())
                .collect::<Vec<_>>(),
        )),
    ];
    for (i, name) in discriminators.iter().enumerate() {
        columns.push(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| match r.identity.get(i) {
                    Some((held, value)) if held == name => Ok(value.as_str()),
                    // Unreachable through `compute`, which builds both from the
                    // one list. An error rather than a null, because a report
                    // whose columns and values had come apart would name the
                    // wrong tenant rather than no tenant.
                    _ => Err(Error::encode(
                        name,
                        "completeness row carries no value for this merge-key column",
                    )),
                })
                .collect::<Result<Vec<_>>>()?,
        )));
    }
    columns.extend::<Vec<crate::arrow::array::ArrayRef>>(vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.sparte.as_str()).collect::<Vec<_>>(),
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
                .map(|r| r.first_gap.map(crate::encode::schema::date32))
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
    ]);

    Ok(RecordBatch::try_new(
        completeness_schema(discriminators),
        columns,
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
/// Grouped by the **balancing** day, because that is the unit the expectation is
/// defined over. Grouping by UTC day would put 22:00–24:00 local in the wrong
/// bucket every day of the year, and the resulting counts would be short by two
/// hours at one end and long at the other — the exact error §9.5 exists to stop.
/// Grouping gas by the *calendar* day is the same error six hours wide, so the
/// bucket follows the row's Sparte.
fn daily_plan(
    resolved: Arc<dyn TableProvider>,
    table: &str,
    discriminators: &[String],
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> DfResult<datafusion::logical_expr::LogicalPlan> {
    use datafusion::functions_aggregate::expr_fn::count;

    let ts = |t: OffsetDateTime| lit(crate::encode::schema::timestamp_scalar(t));

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
        {
            let mut keys = vec![
                col(column::MALO_ID),
                col(column::OBIS_CODE),
                // Grouped on, not merely selected: the roll-up needs it to ask for
                // the right expected count. A measuring point has one commodity, so
                // this adds no cardinality.
                col(column::SPARTE),
                col(column::RESOLUTION),
                // The **stored** balancing day, not a UDF over `from` and `sparte`.
                // The encoder already asked `metering`'s calendar for it, once, at
                // the point the row was written; recomputing it here would be a
                // second derivation to keep in step. It is also plainly faster — a
                // column the engine can group on and collect statistics for, rather
                // than a scalar function it must invoke per row.
                col(column::BALANCING_DAY).alias("day"),
                col(column::QUALITY),
            ];
            // The merge key is what makes two rows two readings, so it is what a
            // per-channel count has to be grouped by. Without these, two tenants
            // reporting one measuring point put 192 intervals against an
            // expectation of 96 and the report claims a surplus that is not
            // there — and one tenant complete beside another missing a day reads
            // as complete overall.
            keys.extend(discriminators.iter().map(col));
            keys
        },
        vec![count(lit(1i64)).alias("actual")],
    )?
    .build()
}

/// The channels a range is expected to hold, drawn from an earlier window.
///
/// One row per `(malo_id, obis_code, discriminators, sparte, resolution)` — the
/// same key [`roll_up`] reports on, so a roster entry and a report row are
/// comparable without re-deriving either.
fn roster_plan(
    resolved: Arc<dyn TableProvider>,
    table: &str,
    discriminators: &[String],
    since: OffsetDateTime,
    until: OffsetDateTime,
) -> DfResult<datafusion::logical_expr::LogicalPlan> {
    let ts = |t: OffsetDateTime| lit(crate::encode::schema::timestamp_scalar(t));

    LogicalPlanBuilder::scan(
        table.to_string(),
        datafusion::datasource::provider_as_source(resolved),
        None,
    )?
    .filter(
        col(column::FROM)
            .gt_eq(ts(since))
            .and(col(column::FROM).lt(ts(until))),
    )?
    // Group keys only, no measure: what is wanted is the *set* of channels, and
    // counting them would make the reference window as expensive as the reported
    // one for an answer nothing reads.
    .aggregate(
        {
            let mut keys = vec![
                col(column::MALO_ID),
                col(column::OBIS_CODE),
                col(column::SPARTE),
                col(column::RESOLUTION),
            ];
            keys.extend(discriminators.iter().map(col));
            keys
        },
        Vec::<Expr>::new(),
    )?
    .build()
}

/// Run the aggregate and roll it up to one row per channel.
///
/// `seen_since`, when set, is the start of a **reference window** ending at
/// `from`: every channel that reported in it is expected to report in the
/// range, so one that does not comes back as a row with `actual = 0` rather
/// than as no row at all. See [`CompletenessQuery`].
pub(crate) async fn compute(
    state: &dyn Session,
    resolved: Arc<dyn TableProvider>,
    table: &str,
    discriminators: &[String],
    from: OffsetDateTime,
    to: OffsetDateTime,
    seen_since: Option<OffsetDateTime>,
) -> Result<Vec<Completeness>> {
    if to <= from {
        return Err(Error::config(format!(
            "completeness range end {to} must be after start {from}"
        )));
    }
    if let Some(since) = seen_since
        && since >= from
    {
        return Err(Error::config(format!(
            "the reference window {since} must start before the reported range {from}: \
             a roster drawn from the range itself can only contain channels the range \
             already reports, so it would find nothing silent"
        )));
    }

    let plan = daily_plan(Arc::clone(&resolved), table, discriminators, from, to)?;
    let physical = state.create_physical_plan(&plan).await?;
    let batches = datafusion::physical_plan::collect(physical, state.task_ctx()).await?;

    let mut daily: Vec<DailyRow> = Vec::new();
    for batch in &batches {
        daily.extend(decode_daily(batch, discriminators)?);
    }

    let mut rows = roll_up(daily, from, to);

    if let Some(since) = seen_since {
        let plan = roster_plan(resolved, table, discriminators, since, from)?;
        let physical = state.create_physical_plan(&plan).await?;
        let batches = datafusion::physical_plan::collect(physical, state.task_ctx()).await?;

        let mut roster = Vec::new();
        for batch in &batches {
            roster.extend(decode_roster(batch, discriminators)?);
        }
        rows.extend(silent_rows(&rows, roster, from, to));
        // The report is otherwise ordered by the roll-up's `BTreeMap`, and a
        // silent channel appended at the end would read as a different kind of
        // row rather than as one more channel. Sorted on the reported key.
        rows.sort_by(|a, b| {
            (&a.malo_id, &a.obis_code, &a.identity, &a.resolution).cmp(&(
                &b.malo_id,
                &b.obis_code,
                &b.identity,
                &b.resolution,
            ))
        });
    }

    Ok(rows)
}

/// One roster entry: a channel that reported in the reference window.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Channel {
    malo_id: String,
    obis_code: String,
    identity: Vec<(String, String)>,
    sparte: Sparte,
    resolution: Option<String>,
}

/// Read the roster aggregate's output back into channels.
fn decode_roster(batch: &RecordBatch, discriminators: &[String]) -> Result<Vec<Channel>> {
    let text = |name: &str| -> Result<&StringArray> {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_string_opt::<i32>())
            .ok_or_else(|| Error::decode(name, "expected a string column"))
    };

    let malo = text(column::MALO_ID)?;
    let obis = text(column::OBIS_CODE)?;
    let sparte = text(column::SPARTE)?;
    let resolution = text(column::RESOLUTION)?;
    let identity_columns = discriminators
        .iter()
        .map(|name| text(name))
        .collect::<Result<Vec<_>>>()?;

    (0..batch.num_rows())
        .map(|i| {
            Ok(Channel {
                malo_id: malo.value(i).to_string(),
                obis_code: obis.value(i).to_string(),
                identity: discriminators
                    .iter()
                    .zip(&identity_columns)
                    .map(|(name, column)| (name.clone(), column.value(i).to_string()))
                    .collect(),
                sparte: sparte.value(i).parse().map_err(|e| {
                    Error::decode(column::SPARTE, format!("{:?}: {e}", sparte.value(i)))
                })?,
                resolution: (!resolution.is_null(i)).then(|| resolution.value(i).to_string()),
            })
        })
        .collect()
}

/// A report row for every rostered channel the range does not mention.
///
/// The whole expectation is missing, and `first_gap` is the range's first
/// balancing day — which is what an operator needs to know: not that a value is
/// absent, but from when.
///
/// Matched on `(malo_id, obis_code, identity)` rather than on the full reported
/// key. A channel whose grid changed between the reference window and the range
/// is still reporting, and calling it silent because it now delivers at a
/// different resolution would be a finding about the roster rather than about
/// the data.
fn silent_rows(
    reported: &[Completeness],
    roster: Vec<Channel>,
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> Vec<Completeness> {
    use std::collections::BTreeSet;

    /// What names a reading, minus the grid it was delivered on.
    type Key = (String, String, Vec<(String, String)>);
    let key = |malo: &str, obis: &str, identity: &[(String, String)]| -> Key {
        (malo.to_string(), obis.to_string(), identity.to_vec())
    };

    let present: BTreeSet<Key> = reported
        .iter()
        .map(|r| key(&r.malo_id, &r.obis_code, &r.identity))
        .collect();

    let mut seen: BTreeSet<Key> = BTreeSet::new();
    roster
        .into_iter()
        .filter(|c| !present.contains(&key(&c.malo_id, &c.obis_code, &c.identity)))
        // A channel that changed grid inside the reference window appears twice
        // there and is one silent channel here.
        .filter(|c| seen.insert(key(&c.malo_id, &c.obis_code, &c.identity)))
        .map(|c| {
            let (expected, first_gap) =
                expected_over_range(from, to, c.resolution.as_deref(), c.sparte);
            Completeness {
                malo_id: c.malo_id,
                obis_code: c.obis_code,
                identity: c.identity,
                sparte: c.sparte,
                resolution: c.resolution,
                expected,
                actual: 0,
                missing: expected,
                surplus: 0,
                first_gap,
                substituted: 0,
                not_billable: 0,
            }
        })
        .collect()
}

/// How many intervals a range should hold for a channel that delivered none, and
/// the first balancing day it is short.
///
/// Summed per balancing day exactly as [`roll_up`] does, so a silent channel's
/// `expected` is the number a channel delivering everything would have reported
/// — including the 92- and 100-interval DST days, and the Gastag for gas.
fn expected_over_range(
    from: OffsetDateTime,
    to: OffsetDateTime,
    resolution: Option<&str>,
    sparte: Sparte,
) -> (u64, Option<Date>) {
    let mut day = balancing::balancing_day(from, sparte);
    let last = balancing::balancing_day(to - time::Duration::nanoseconds(1), sparte);

    let mut expected = 0u64;
    let mut first_gap = None;
    while day <= last {
        let n = expected_in_day(day, resolution, sparte, from, to);
        expected += n;
        if n > 0 && first_gap.is_none() {
            first_gap = Some(day);
        }
        let Some(next) = day.next_day() else { break };
        day = next;
    }
    (expected, first_gap)
}

/// Read the aggregate's output back into typed rows.
fn decode_daily(batch: &RecordBatch, discriminators: &[String]) -> Result<Vec<DailyRow>> {
    let text = |name: &str| -> Result<&StringArray> {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_string_opt::<i32>())
            .ok_or_else(|| Error::decode(name, "expected a string column"))
    };

    let malo = text(column::MALO_ID)?;
    let obis = text(column::OBIS_CODE)?;
    let sparte = text(column::SPARTE)?;
    let resolution = text(column::RESOLUTION)?;
    let quality = text(column::QUALITY)?;
    let identity_columns = discriminators
        .iter()
        .map(|name| text(name))
        .collect::<Result<Vec<_>>>()?;
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
            identity: discriminators
                .iter()
                .zip(&identity_columns)
                .map(|(name, column)| (name.clone(), column.value(i).to_string()))
                .collect(),
            // The commodity decides which day the row was bucketed into and
            // which expectation it is measured against, so an unrecognised one
            // is an error rather than an assumed `STROM`.
            sparte: sparte.value(i).parse().map_err(|e| {
                Error::decode(column::SPARTE, format!("{:?}: {e}", sparte.value(i)))
            })?,
            resolution: (!resolution.is_null(i)).then(|| resolution.value(i).to_string()),
            day: crate::encode::schema::date_of(day.value(i))?,
            actual: count,
            substituted: if is_substitute(flag) { count } else { 0 },
            not_billable: if is_billable(flag) { 0 } else { count },
        });
    }
    Ok(out)
}

/// Aggregate daily rows into per-channel completeness.
///
/// The expectation is asked of `metering` once per (day, resolution, sparte). A
/// day partly outside the queried range is expected to hold only the part inside
/// it, which is what makes a range that is not day-aligned report honestly rather
/// than claiming a gap at each end.
///
/// The channel key carries the Sparte **and the declared resolution**, because a
/// row is measured against an expectation and both of them decide which one
/// applies.
///
/// A measuring point has one commodity, so the Sparte normally changes nothing —
/// but if a channel ever held two, folding them into one row would measure gas
/// rows against the electricity day.
///
/// The resolution is the same argument and less hypothetical: a meter converted
/// from an hourly profile to a quarter-hourly one mid-month holds both, and it is
/// the *grid* that says whether a day of 24 values is complete or 72 short. Split
/// by grid, each row measures its own days and a clean conversion comes back as
/// two complete rows.
fn roll_up(daily: Vec<DailyRow>, from: OffsetDateTime, to: OffsetDateTime) -> Vec<Completeness> {
    use std::collections::BTreeMap;

    /// `(malo_id, obis_code, identity, sparte, resolution)` — what one reported
    /// row describes.
    ///
    /// The Sparte enters as its canonical code rather than as the enum, because
    /// the maps are ordered — a `BTreeMap` keeps the report deterministic — and
    /// `Sparte` is deliberately not `Ord`. The enum itself rides in the
    /// accumulator, so nothing has to parse the code back.
    type ChannelKey = (
        String,
        String,
        Vec<(String, String)>,
        &'static str,
        Option<String>,
    );

    // Key by channel; within a channel, days are folded together.
    struct Accumulator {
        identity: Vec<(String, String)>,
        sparte: Sparte,
        resolution: Option<String>,
        expected: u64,
        actual: u64,
        missing: u64,
        surplus: u64,
        first_gap: Option<Date>,
        substituted: u64,
        not_billable: u64,
    }

    let mut by_channel: BTreeMap<ChannelKey, Accumulator> = BTreeMap::new();
    // Days are visited once per quality flag, so the expectation must only be
    // added the first time a (channel, day) pair is seen.
    let mut counted: std::collections::BTreeSet<(ChannelKey, Date)> = Default::default();
    // A day is short only once every flag for it has been folded in, so gaps are
    // decided after the fold rather than per row.
    let mut per_day: BTreeMap<(ChannelKey, Date), (u64, u64)> = BTreeMap::new();

    for row in daily {
        let key: ChannelKey = (
            row.malo_id.clone(),
            row.obis_code.clone(),
            row.identity.clone(),
            row.sparte.as_str(),
            row.resolution.clone(),
        );
        let entry = by_channel.entry(key.clone()).or_insert(Accumulator {
            identity: row.identity.clone(),
            sparte: row.sparte,
            resolution: row.resolution.clone(),
            expected: 0,
            actual: 0,
            missing: 0,
            surplus: 0,
            first_gap: None,
            substituted: 0,
            not_billable: 0,
        });
        entry.actual += row.actual;
        entry.substituted += row.substituted;
        entry.not_billable += row.not_billable;

        let day_key = (key, row.day);
        let expected = if counted.insert(day_key.clone()) {
            let n = expected_in_day(row.day, row.resolution.as_deref(), row.sparte, from, to);
            entry.expected += n;
            n
        } else {
            0
        };

        let slot = per_day.entry(day_key).or_insert((0, 0));
        slot.0 += row.actual;
        slot.1 += expected;
    }

    for ((channel, day), (actual, expected)) in per_day {
        let Some(entry) = by_channel.get_mut(&channel) else {
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
        // Both directions are summed **per day** and neither is derived from the
        // channel totals. Over the totals a surplus on one day would net against
        // a shortfall on another and the channel would report as complete —
        // which is exactly the answer §9.6 says surplus must never be able to
        // produce, since the two are different conditions rather than opposite
        // signs of one.
        entry.missing += expected.saturating_sub(actual);
        entry.surplus += actual.saturating_sub(expected);
    }

    by_channel
        .into_iter()
        .map(
            |((malo_id, obis_code, ..), a): (ChannelKey, Accumulator)| Completeness {
                malo_id,
                obis_code,
                identity: a.identity,
                sparte: a.sparte,
                resolution: a.resolution,
                expected: a.expected,
                actual: a.actual,
                missing: a.missing,
                surplus: a.surplus,
                first_gap: a.first_gap,
                substituted: a.substituted,
                not_billable: a.not_billable,
            },
        )
        .collect()
}

/// How many intervals a balancing day should hold, clipped to the queried range.
///
/// Zero when the resolution is absent or is a calendar one — there is no fixed
/// count within a day for `P1M`, and inventing 96 would report a month-resolution
/// series as 95 intervals short every day.
///
/// The day's identity, length and interval count all come from `sparte`: for gas
/// that is the 06:00–06:00 Gastag, and taking its bounds from the calendar day
/// instead would clip the wrong six hours at each end of the range.
fn expected_in_day(
    day: Date,
    resolution: Option<&str>,
    sparte: Sparte,
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> u64 {
    let Some(parsed) = resolution.and_then(|r| r.parse::<IntervalResolution>().ok()) else {
        return 0;
    };
    let Some(full) = balancing::expected_intervals_in_balancing_day(day, parsed, sparte) else {
        return 0;
    };
    let full = u64::from(full);

    let (start, end) = balancing::balancing_day_bounds(day, sparte);
    let length = end - start;

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

/// A completeness report waiting to be run.
///
/// Returned by [`MeterStore::completeness`](crate::MeterStore::completeness) and
/// awaited directly, so the plain report is one call.
///
/// # The finding a range cannot make about itself
///
/// The report is an aggregate over the rows the range holds, so a channel with no
/// rows produces no groups and appears nowhere — the most severe incompleteness
/// there is, and the one nothing in the range can supply, because the missing
/// roster is precisely what the range does not contain.
///
/// [`seen_since`](Self::seen_since) supplies it from an earlier window: a channel
/// that reported between `since` and the start of the range and does not report
/// inside it comes back with `actual = 0`, the whole range as `missing`, and
/// `first_gap` on its first balancing day.
///
/// ```no_run
/// # async fn example(store: &meterstore::MeterStore) -> meterstore::Result<()> {
/// # let (from, to) = (time::OffsetDateTime::UNIX_EPOCH, time::OffsetDateTime::UNIX_EPOCH);
/// let report = store
///     .completeness(from, to)
///     .seen_since(from - time::Duration::days(30))
///     .await?;
///
/// for gone in report.iter().filter(|r| r.is_silent()) {
///     tracing::error!(malo = %gone.malo_id, obis = %gone.obis_code, "delivered nothing");
/// }
/// # Ok(()) }
/// ```
///
/// The window is the caller's because there is no honest default: too short and a
/// meter read monthly looks decommissioned, too long and every terminated
/// measuring point is a standing finding. What a roster means — "still in
/// service" — is master data this crate does not hold.
pub struct CompletenessQuery<'a> {
    store: &'a crate::session::MeterStore,
    from: OffsetDateTime,
    to: OffsetDateTime,
    seen_since: Option<OffsetDateTime>,
}

impl std::fmt::Debug for CompletenessQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletenessQuery")
            .field("from", &self.from)
            .field("to", &self.to)
            .field("seen_since", &self.seen_since)
            .finish_non_exhaustive()
    }
}

impl<'a> CompletenessQuery<'a> {
    pub(crate) fn new(
        store: &'a crate::session::MeterStore,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Self {
        Self {
            store,
            from,
            to,
            seen_since: None,
        }
    }

    /// Also report channels that reported since `since` and are **silent** in the
    /// range.
    ///
    /// `since` must be before the range starts: a roster drawn from the range
    /// itself can only hold channels the range already reports, so it would find
    /// nothing. See the type documentation for why the window is the caller's.
    #[must_use]
    pub fn seen_since(mut self, since: OffsetDateTime) -> Self {
        self.seen_since = Some(since);
        self
    }

    /// Run the report.
    ///
    /// The same thing `.await` does; spelled out for a caller that wants the
    /// call to look like a call.
    pub async fn run(self) -> Result<Vec<Completeness>> {
        let (resolved, table, discriminators) = self.store.completeness_inputs().await?;
        compute(
            &self.store.context().state(),
            resolved,
            &table,
            &discriminators,
            self.from,
            self.to,
            self.seen_since,
        )
        .await
    }
}

impl<'a> std::future::IntoFuture for CompletenessQuery<'a> {
    type Output = Result<Vec<Completeness>>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
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
    discriminators: Vec<String>,
}

impl CompletenessFunction {
    /// Bind the function to a store's resolved table.
    ///
    /// `discriminators` are the table's merge-key columns beyond
    /// `(malo_id, obis_code, from)` — what the report groups by and reports, so
    /// that two tenants' or two meters' rows are two rows rather than one wrong
    /// one.
    pub fn new(
        resolved: Arc<dyn TableProvider>,
        table: impl Into<String>,
        discriminators: Vec<String>,
    ) -> Self {
        Self {
            resolved,
            table: table.into(),
            discriminators,
        }
    }

    /// The SQL name.
    pub const NAME: &'static str = "meter_completeness";
}

impl datafusion::catalog::TableFunctionImpl for CompletenessFunction {
    fn call(&self, args: &[Expr]) -> DfResult<Arc<dyn TableProvider>> {
        // Arguments read left to right in **time order**, so the three-argument
        // form is `(seen_since, from, to)` and not `(from, to, seen_since)` —
        // the reference window ends where the reported range begins, and writing
        // it out of order is how an operator gets the two the wrong way round.
        //
        // A leading table name and a leading `seen_since` are told apart by
        // whether the argument reads as an instant. A table name never does:
        // `TableConfig` refuses anything that is not a plain identifier, and no
        // plain identifier parses as a date.
        let (name, seen_since, from, to) = match args {
            [from, to] => (None, None, from, to),
            [first, from, to] if as_instant(first).is_ok() => (None, Some(first), from, to),
            [name, from, to] => (Some(as_string(name)?), None, from, to),
            [name, since, from, to] => (Some(as_string(name)?), Some(since), from, to),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "{name}(from, to), {name}(seen_since, from, to), \
                     {name}(table, from, to) or {name}(table, seen_since, from, to)",
                    name = Self::NAME,
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
            discriminators: self.discriminators.clone(),
            from: as_instant(from)?,
            to: as_instant(to)?,
            seen_since: seen_since.map(as_instant).transpose()?,
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
    discriminators: Vec<String>,
    from: OffsetDateTime,
    to: OffsetDateTime,
    seen_since: Option<OffsetDateTime>,
}

impl std::fmt::Debug for CompletenessProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletenessProvider")
            .field("table", &self.table)
            .field("from", &self.from)
            .field("to", &self.to)
            .field("seen_since", &self.seen_since)
            .finish()
    }
}

#[async_trait]
impl TableProvider for CompletenessProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        completeness_schema(&self.discriminators)
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
            &self.discriminators,
            self.from,
            self.to,
            self.seen_since,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let batch = completeness_batch(&rows, &self.discriminators)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        MemTable::try_new(completeness_schema(&self.discriminators), vec![vec![batch]])?
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
        sparte_row(Sparte::Strom, day, actual, quality)
    }

    fn sparte_row(sparte: Sparte, day: Date, actual: u64, quality: &str) -> DailyRow {
        DailyRow {
            malo_id: "12345678905".into(),
            obis_code: "1-0:1.8.0".into(),
            identity: Vec::new(),
            sparte,
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
    fn a_surplus_on_one_day_cannot_hide_a_gap_on_another() {
        // The failure computing `missing` from the channel totals produced: four
        // intervals too many on the 2nd and four short on the 3rd net to zero, so
        // a channel with a real gap reported as complete. They are different
        // conditions — a duplicate and a shortfall — not opposite signs of one.
        let out = roll_up(
            vec![
                row(date!(2026 - 03 - 02), 100, "MEASURED"),
                row(date!(2026 - 03 - 03), 92, "MEASURED"),
            ],
            FROM,
            TO,
        );

        assert_eq!(out[0].expected, 192);
        assert_eq!(out[0].actual, 192);
        assert_eq!(out[0].missing, 4, "the 3rd is four intervals short");
        assert_eq!(out[0].surplus, 4, "the 2nd holds four too many");
        assert!(!out[0].is_complete());
        assert_eq!(out[0].first_gap, Some(date!(2026 - 03 - 03)));
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
    fn a_channel_that_changes_grid_reports_one_row_per_grid() {
        // A meter converted from an hourly profile to a quarter-hourly one holds
        // both, and it is the grid that says whether a day of 24 values is
        // complete or 72 short. Folded into one row the report named an
        // arbitrary one of them — arbitrary because the aggregate yields groups
        // in no defined order — against a count drawn from both. Split, each row
        // measures its own days against its own grid, and both are complete,
        // which is what they are.
        let mut hourly = row(date!(2026 - 03 - 02), 24, "MEASURED");
        hourly.resolution = Some("PT1H".into());
        let quarterly = row(date!(2026 - 03 - 03), 96, "MEASURED");

        let out = roll_up(vec![hourly, quarterly], FROM, TO);

        assert_eq!(out.len(), 2, "one row per grid: {out:?}");
        let by_grid: std::collections::BTreeMap<_, _> = out
            .iter()
            .map(|r| (r.resolution.clone().unwrap(), r))
            .collect();

        let hourly = by_grid["PT1H"];
        assert_eq!((hourly.expected, hourly.actual), (24, 24));
        assert!(hourly.is_complete() && hourly.is_measurable());

        let quarterly = by_grid["PT15M"];
        assert_eq!((quarterly.expected, quarterly.actual), (96, 96));
        assert!(quarterly.is_complete() && quarterly.is_measurable());
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
        let batch = completeness_batch(&rows, &[]).unwrap();
        assert_eq!(batch.schema(), completeness_schema(&[]));
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn two_readings_of_one_channel_are_two_rows() {
        // Two tenants — or the two meters of a Mehrfamilienhaus — each deliver a
        // full day for one measuring point and one channel. Folded, that is 192
        // intervals against an expectation of 96 and a surplus that is not there.
        // Worse in the other direction: one of them missing a day nets against
        // the other's full one and the channel reads as complete.
        let of = |tenant: &str, actual: u64| {
            let mut r = row(date!(2026 - 03 - 02), actual, "MEASURED");
            r.identity = vec![("tenant".to_string(), tenant.to_string())];
            r
        };

        let out = roll_up(vec![of("a", 96), of("b", 92)], FROM, TO);
        assert_eq!(out.len(), 2, "two readings, two rows");

        let tenant_a = out.iter().find(|r| r.identity[0].1 == "a").unwrap();
        let tenant_b = out.iter().find(|r| r.identity[0].1 == "b").unwrap();
        assert!(tenant_a.is_complete(), "a delivered the whole day");
        assert_eq!(tenant_b.missing, 4, "and b is four short, on its own row");
        assert_eq!(tenant_a.surplus, 0, "neither is a duplicate of the other");
    }

    #[test]
    fn the_reported_columns_follow_the_merge_key() {
        let mut r = row(date!(2026 - 03 - 02), 96, "MEASURED");
        r.identity = vec![("melo_id".to_string(), "DE00012345".to_string())];
        let rows = roll_up(vec![r], FROM, TO);

        let key = ["melo_id".to_string()];
        let batch = completeness_batch(&rows, &key).unwrap();
        assert_eq!(batch.schema(), completeness_schema(&key));

        use crate::arrow::array::AsArray;
        let column = batch.column_by_name("melo_id").expect("reported");
        assert_eq!(column.as_string::<i32>().value(0), "DE00012345");
    }

    #[test]
    fn expected_in_day_knows_the_dst_days() {
        assert_eq!(
            expected_in_day(
                date!(2026 - 03 - 29),
                Some("PT15M"),
                Sparte::Strom,
                FROM,
                TO
            ),
            92
        );
        assert_eq!(
            expected_in_day(
                date!(2026 - 10 - 25),
                Some("PT15M"),
                Sparte::Strom,
                datetime!(2026-10-01 00:00 UTC),
                datetime!(2026-11-01 00:00 UTC)
            ),
            100
        );
    }

    // ── gas balances on the Gastag ───────────────────────────────────────────

    #[test]
    fn a_gas_channels_dst_day_is_the_saturday_not_the_sunday() {
        // The clocks go back at 03:00 local on Sunday 25 October, which is
        // inside the Gastag that began Saturday 06:00. So the 100-interval gas
        // day is the 24th — the mirror image of the calendar day, which is what
        // makes using the wrong one produce two findings instead of none.
        let october = (
            datetime!(2026-10-01 00:00 UTC),
            datetime!(2026-11-01 00:00 UTC),
        );
        for (day, gas, strom) in [
            (date!(2026 - 10 - 24), 100, 96),
            (date!(2026 - 10 - 25), 96, 100),
        ] {
            assert_eq!(
                expected_in_day(day, Some("PT15M"), Sparte::Gas, october.0, october.1),
                gas,
                "gas {day}"
            );
            assert_eq!(
                expected_in_day(day, Some("PT15M"), Sparte::Strom, october.0, october.1),
                strom,
                "strom {day}"
            );
        }
    }

    #[test]
    fn a_full_gastag_is_complete_and_the_calendar_day_would_not_be() {
        // 100 intervals on the long Gastag is exactly right. Measured against
        // the calendar day it would read as four in surplus — a duplicate
        // finding, on a channel with no duplicates.
        let out = roll_up(
            vec![sparte_row(
                Sparte::Gas,
                date!(2026 - 10 - 24),
                100,
                "MEASURED",
            )],
            datetime!(2026-10-01 00:00 UTC),
            datetime!(2026-11-01 00:00 UTC),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].sparte, Sparte::Gas);
        assert_eq!(out[0].expected, 100);
        assert!(out[0].is_complete());
        assert_eq!(out[0].surplus, 0);
    }

    #[test]
    fn heat_and_water_keep_the_calendar_day() {
        // Only gas moved. A blanket "not electricity" rule would shift Wärme and
        // Wasser onto a 06:00 day nothing in the market uses.
        for sparte in [Sparte::Waerme, Sparte::Wasser] {
            assert_eq!(
                expected_in_day(
                    date!(2026 - 10 - 25),
                    Some("PT15M"),
                    sparte,
                    datetime!(2026-10-01 00:00 UTC),
                    datetime!(2026-11-01 00:00 UTC),
                ),
                100,
                "{sparte}"
            );
        }
    }

    #[test]
    fn a_gas_day_clipped_by_the_range_expects_only_the_covered_part() {
        // A range that starts at 00:00 local reaches into the Gastag that began
        // 06:00 the *previous* morning, and covers only its last six hours —
        // 00:00 to 06:00 local. Expecting a whole day there would report 72
        // intervals missing on the leading day of every gas query, which is the
        // false alarm the clipping exists to prevent.
        let from = datetime!(2026-07-14 22:00 UTC); // 00:00 local, 15 July
        let to = datetime!(2026-07-21 22:00 UTC);
        assert_eq!(
            expected_in_day(date!(2026 - 07 - 14), Some("PT15M"), Sparte::Gas, from, to),
            24,
            "the 00:00–06:00 tail of the Gastag that began on the 14th"
        );
        assert_eq!(
            expected_in_day(date!(2026 - 07 - 15), Some("PT15M"), Sparte::Gas, from, to),
            96,
            "wholly inside the range"
        );
    }

    #[test]
    fn two_commodities_on_one_channel_are_reported_separately() {
        // Not an expected state, but folding them would measure the gas rows
        // against the electricity day and say nothing about it.
        let out = roll_up(
            vec![
                sparte_row(Sparte::Strom, date!(2026 - 03 - 02), 96, "MEASURED"),
                sparte_row(Sparte::Gas, date!(2026 - 03 - 02), 96, "MEASURED"),
            ],
            FROM,
            TO,
        );
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(Completeness::is_complete));
    }

    fn channel(sparte: Sparte, resolution: Option<&str>) -> Channel {
        Channel {
            malo_id: "12345678905".into(),
            obis_code: "1-0:1.8.0".into(),
            identity: Vec::new(),
            sparte,
            resolution: resolution.map(str::to_string),
        }
    }

    #[test]
    fn a_channel_that_delivered_nothing_is_the_finding_the_range_cannot_make() {
        // The whole point. An aggregate over the range produces no group for a
        // channel with no rows, so the most severe incompleteness there is — a
        // meter that stopped entirely — is *absent* from the report rather than
        // reported as empty. The roster is the only thing that can supply it.
        let silent = silent_rows(&[], vec![channel(Sparte::Strom, Some("PT15M"))], FROM, TO);

        assert_eq!(silent.len(), 1);
        let row = &silent[0];
        assert!(row.is_silent());
        assert!(!row.is_complete());
        assert_eq!(row.actual, 0);
        // 2 976 quarter-hours, which is 31 × 96 — and arriving at it is the
        // whole of what the clipping does. The range is a *UTC* month, so it is
        // not aligned to a Berlin day at either end: the first balancing day is
        // 23 hours of it and the last is two, and the spring-forward Sunday in
        // between contributes 92 rather than 96. A report that expected 96 a day
        // over Berlin days would claim four intervals missing on 29 March and
        // another day's worth at the ends.
        assert_eq!(row.expected, 31 * 96);
        assert_eq!(row.missing, row.expected, "the whole range is missing");
        assert_eq!(row.surplus, 0);
        assert_eq!(row.first_gap, Some(date!(2026 - 03 - 01)));
    }

    #[test]
    fn a_channel_still_reporting_is_not_called_silent() {
        // Including when its grid changed. A meter converted from an hourly
        // profile to a quarter-hourly one reports under a different resolution
        // than the roster saw, and calling that a silent channel would be a
        // finding about the roster rather than about the data.
        let reported = roll_up(vec![row(date!(2026 - 03 - 02), 96, "MEASURED")], FROM, TO);
        let silent = silent_rows(
            &reported,
            vec![channel(Sparte::Strom, Some("PT1H"))],
            FROM,
            TO,
        );
        assert!(silent.is_empty(), "{silent:?}");
    }

    #[test]
    fn a_silent_gas_channel_is_measured_on_the_gastag() {
        // The Gastag runs 06:00 to 06:00, so a month-long range clips six hours
        // off each end day rather than expecting two whole extra days.
        let silent = silent_rows(&[], vec![channel(Sparte::Gas, Some("PT15M"))], FROM, TO);
        assert_eq!(silent.len(), 1);
        // The Gastage tile the timeline exactly as the calendar days do, so the
        // range holds the same number of quarter-hours however it is cut into
        // days — five hours of the first Gastag, twenty of the last.
        assert_eq!(silent[0].expected, 31 * 96);
        // And the first short day is a Gastag, which begins on 28 February.
        assert_eq!(silent[0].first_gap, Some(date!(2026 - 02 - 28)));
    }

    #[test]
    fn a_silent_channel_with_no_grid_reports_nothing_to_judge() {
        // No declared resolution means no expectation, in both directions: the
        // row must not claim a month-long gap it cannot measure.
        let silent = silent_rows(&[], vec![channel(Sparte::Strom, None)], FROM, TO);
        assert_eq!(silent.len(), 1);
        assert_eq!(silent[0].expected, 0);
        assert_eq!(silent[0].missing, 0);
        assert!(!silent[0].is_measurable());
        assert_eq!(silent[0].first_gap, None);
    }

    #[test]
    fn a_channel_that_changed_grid_and_went_silent_is_one_row() {
        // It appears twice in the roster — once per grid — and is one silent
        // channel, not two.
        let silent = silent_rows(
            &[],
            vec![
                channel(Sparte::Strom, Some("PT1H")),
                channel(Sparte::Strom, Some("PT15M")),
            ],
            FROM,
            TO,
        );
        assert_eq!(silent.len(), 1);
    }

    #[test]
    fn identity_keeps_two_tenants_silent_channels_apart() {
        // A tenant discriminator is what makes two rows two readings, so one
        // tenant reporting must not make the other's silence invisible.
        let tenant = |who: &str| Channel {
            identity: vec![("tenant".into(), who.into())],
            ..channel(Sparte::Strom, Some("PT15M"))
        };
        let reported = roll_up(
            vec![DailyRow {
                identity: vec![("tenant".into(), "a".into())],
                ..row(date!(2026 - 03 - 02), 96, "MEASURED")
            }],
            FROM,
            TO,
        );

        let silent = silent_rows(&reported, vec![tenant("a"), tenant("b")], FROM, TO);
        assert_eq!(silent.len(), 1);
        assert_eq!(silent[0].identity, vec![("tenant".into(), "b".into())]);
    }
}
