//! SQL functions for local calendar grouping.
//!
//! Daily and monthly aggregates must be grouped by **local** calendar day.
//! `Europe/Berlin` observes daylight saving, so the UTC day boundary sits at
//! 01:00 or 02:00 local — which means `date_trunc('day', "from")` produces wrong
//! daily sums for every customer on every day of the year, not only at the two
//! transitions. These functions exist so the correct grouping is the convenient
//! one.
//!
//! # Gas is grouped on a different day
//!
//! The German gas market balances on the **Gastag**, 06:00 to 06:00 local, not
//! on the calendar day — so `meter_local_day` is the *wrong* function for a gas
//! Lastgang in precisely the way `date_trunc` is wrong for an electricity one.
//! [`GasDay`] (`meter_gas_day`) is its counterpart, and [`BalancingDay`]
//! (`meter_balancing_day`) chooses between them from the row's `sparte`, so a
//! statement over a mixed table is written once and is right for both.
//!
//! The arithmetic is `metering::calendar`'s. These are wrappers: they convert
//! between Arrow arrays and the domain functions and nothing else. Their tests
//! assert that the wrapper preserves the upstream answer, not that the answer is
//! right — that is `metering`'s suite's job, plus the contract test in
//! `tests/it/calendar_delegation.rs`.

use std::any::Any;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use metering::IntervalResolution;
use metering::calendar;
use metering::interval::Sparte;
use time::{Date, OffsetDateTime};

use crate::arrow::array::{
    Array, Date32Array, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use crate::arrow::datatypes::{DataType, TimeUnit};
use crate::planner::calendar as balancing;

/// Days between the Unix epoch and a date, for `Date32`.
fn to_date32(date: Date) -> i32 {
    (date - Date::from_ordinal_date(1970, 1).expect("epoch is a valid date")).whole_days() as i32
}

/// The instant a microsecond timestamp represents.
fn from_micros(micros: i64) -> DfResult<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
        .map_err(|e| DataFusionError::Execution(format!("timestamp out of range: {e}")))
}

/// Accepts a UTC timestamp in any precision, so callers need not cast.
fn timestamp_signature() -> Signature {
    timestamp_signature_with(&[])
}

/// As [`timestamp_signature`], with `trailing` argument types appended.
///
/// The precision cross-product is the reason this is generated rather than
/// written out: a two-argument function accepting four time units in two
/// nullability spellings is eight exact signatures, and hand-listing them is how
/// one gets forgotten and a perfectly ordinary column fails to bind.
fn timestamp_signature_with(trailing: &[DataType]) -> Signature {
    Signature::one_of(
        [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ]
        .into_iter()
        .flat_map(|unit| {
            [Some("UTC".into()), None].map(|tz| {
                let mut args = vec![DataType::Timestamp(unit, tz)];
                args.extend_from_slice(trailing);
                TypeSignature::Exact(args)
            })
        })
        .collect(),
        Volatility::Immutable,
    )
}

/// Read a `Utf8` argument as an array, whether it arrived as a column or a
/// literal.
///
/// A literal is broadcast by `into_array`, so both spellings are handled by one
/// path — and, crucially, the value is read **per row**. `sparte` is a column on
/// every real table, and applying the first row's commodity to the rest would
/// group a whole gas table on the electricity day.
fn as_strings(args: &ScalarFunctionArgs, index: usize) -> DfResult<StringArray> {
    let array = args.args[index].clone().into_array(args.number_rows)?;
    Ok(crate::arrow::array::AsArray::as_string_opt::<i32>(&array)
        .ok_or_else(|| {
            DataFusionError::Execution(format!("argument {} must be a string", index + 1))
        })?
        .clone())
}

/// Parse a stored `sparte` code, naming the accepted set on failure.
fn parse_sparte(s: &str) -> DfResult<Sparte> {
    s.parse().map_err(|e| {
        DataFusionError::Execution(format!(
            "bad sparte {s:?}: {e} — expected one of {:?}",
            Sparte::CODES
        ))
    })
}

/// Coerce an argument to microsecond timestamps.
fn as_micros(args: &ScalarFunctionArgs) -> DfResult<TimestampMicrosecondArray> {
    let array = args.args[0].clone().into_array(args.number_rows)?;
    let cast = crate::arrow::compute::cast(
        &array,
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    )?;
    Ok(cast
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| DataFusionError::Execution("expected a timestamp argument".into()))?
        .clone())
}

/// `meter_local_day(ts)` — the Berlin calendar day an instant falls on.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LocalDay {
    signature: Signature,
}

impl Default for LocalDay {
    fn default() -> Self {
        Self {
            signature: timestamp_signature(),
        }
    }
}

impl ScalarUDFImpl for LocalDay {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_local_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let input = as_micros(&args)?;
        let mut out = Date32Array::builder(input.len());
        for i in 0..input.len() {
            if input.is_null(i) {
                out.append_null();
            } else {
                out.append_value(to_date32(calendar::local_day(from_micros(input.value(i))?)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `meter_gas_day(ts)` — the **Gastag** an instant falls on.
///
/// 06:00 to 06:00 `Europe/Berlin` (GaBi Gas, following Art. 3 Nr. 6 VO (EU)
/// 312/2014). The German gas market balances on this day and not on the calendar
/// one, so `meter_local_day` over a gas Lastgang books the 00:00–06:00 draw into
/// the previous Bilanzierungstag's neighbour — six hours a day, every day, with
/// totals that still look plausible.
///
/// ```sql
/// SELECT meter_gas_day("from") AS gastag, SUM(value)
/// FROM readings WHERE sparte = 'GAS'
/// GROUP BY 1
/// ```
///
/// Use [`BalancingDay`] instead when the statement spans commodities.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GasDay {
    signature: Signature,
}

impl Default for GasDay {
    fn default() -> Self {
        Self {
            signature: timestamp_signature(),
        }
    }
}

impl ScalarUDFImpl for GasDay {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_gas_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let input = as_micros(&args)?;
        let mut out = Date32Array::builder(input.len());
        for i in 0..input.len() {
            if input.is_null(i) {
                out.append_null();
            } else {
                out.append_value(to_date32(calendar::local_gas_day(from_micros(
                    input.value(i),
                )?)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `meter_balancing_day(ts, sparte)` — the day a reading is balanced on.
///
/// The Gastag for `'GAS'`, the Berlin calendar day for `'STROM'`, `'WAERME'` and
/// `'WASSER'`. This is the grouping key for a table that holds more than one
/// commodity, which is the ordinary case: the alternative is a `CASE` expression
/// repeated at every call site, and one of them eventually says `meter_local_day`
/// for gas.
///
/// ```sql
/// SELECT sparte, meter_balancing_day("from", sparte) AS day, SUM(value)
/// FROM readings
/// GROUP BY 1, 2
/// ```
///
/// The commodity is read **per row**, so a literal and a column both work and a
/// mixed scan is not grouped on whichever Sparte happened to sort first.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BalancingDay {
    signature: Signature,
}

impl Default for BalancingDay {
    fn default() -> Self {
        Self {
            signature: timestamp_signature_with(&[DataType::Utf8]),
        }
    }
}

impl ScalarUDFImpl for BalancingDay {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_balancing_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let input = as_micros(&args)?;
        let sparte = as_strings(&args, 1)?;

        let mut out = Date32Array::builder(input.len());
        for i in 0..input.len() {
            // A null commodity is not defaulted to the calendar day: that would
            // silently place gas rows on the wrong Bilanzierungstag, which is
            // the one failure this function exists to prevent. `sparte` is
            // non-nullable in the storage schema, so this is unreachable for a
            // stored row and null is the honest answer for anything else.
            if input.is_null(i) || sparte.is_null(i) {
                out.append_null();
                continue;
            }
            out.append_value(to_date32(balancing::balancing_day(
                from_micros(input.value(i))?,
                parse_sparte(sparte.value(i))?,
            )));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `meter_local_month(ts)` — the Berlin calendar month, as its first day.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LocalMonth {
    signature: Signature,
}

impl Default for LocalMonth {
    fn default() -> Self {
        Self {
            signature: timestamp_signature(),
        }
    }
}

impl ScalarUDFImpl for LocalMonth {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_local_month"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let input = as_micros(&args)?;
        let mut out = Date32Array::builder(input.len());
        for i in 0..input.len() {
            if input.is_null(i) {
                out.append_null();
            } else {
                out.append_value(to_date32(calendar::local_month(from_micros(
                    input.value(i),
                )?)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `meter_expected_intervals(day, resolution[, sparte])` — how many intervals a
/// balancing day contains.
///
/// 96 normally, **92** on the spring-forward day and **100** on the autumn one.
/// A completeness check that assumes 96 raises false alarms every spring and
/// masks a genuine four-interval gap every autumn.
///
/// # The optional third argument
///
/// Two arguments count a Berlin **calendar** day, which is right for
/// electricity, heat and water. Passing `sparte` counts the day that commodity
/// is actually balanced on — the **Gastag** for `'GAS'` — and must be paired
/// with [`BalancingDay`] rather than [`LocalDay`], since the count and the
/// bucketing have to describe the same day. The DST anomaly moves with it: the
/// long and short gas days are the ones named after the **Saturday**, because
/// the clocks change at 02:00/03:00 local, before the 06:00 boundary.
///
/// ```sql
/// SELECT meter_expected_intervals(
///          meter_balancing_day("from", sparte), resolution, sparte)
/// FROM readings
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ExpectedIntervals {
    signature: Signature,
}

impl Default for ExpectedIntervals {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Date32, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Date32, DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ExpectedIntervals {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_expected_intervals"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::UInt32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let days = args.args[0].clone().into_array(args.number_rows)?;
        let days = days
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| DataFusionError::Execution("first argument must be a date".into()))?;

        // Resolution is often a literal, in which case it is parsed once. When
        // it is a column it must be read **per row**: completeness groups a
        // range by measuring point, and a utility carries 15-minute profiles
        // alongside hourly and daily ones. Applying the first row's resolution
        // to the rest would report a full day of gaps for every series whose
        // resolution differs from whichever happened to sort first.
        let literal = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Some(Some(parse_resolution(s)?)),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => Some(None),
            _ => None,
        };

        let per_row = match literal {
            Some(_) => None,
            None => {
                let array = args.args[1].clone().into_array(args.number_rows)?;
                let strings = crate::arrow::array::AsArray::as_string_opt::<i32>(&array)
                    .ok_or_else(|| {
                        DataFusionError::Execution("second argument must be a string".into())
                    })?
                    .clone();
                Some(strings)
            }
        };

        // Absent third argument: the Berlin calendar day, which is what every
        // commodity but gas balances on.
        let sparte = match args.args.len() {
            3 => Some(as_strings(&args, 2)?),
            _ => None,
        };

        let mut out = UInt32Array::builder(days.len());
        for i in 0..days.len() {
            let resolution = match (&literal, &per_row) {
                (Some(value), _) => *value,
                (None, Some(strings)) if !strings.is_null(i) => {
                    Some(parse_resolution(strings.value(i))?)
                }
                _ => None,
            };
            let commodity = match &sparte {
                None => Some(Sparte::Strom),
                Some(codes) if codes.is_null(i) => None,
                Some(codes) => Some(parse_sparte(codes.value(i))?),
            };

            match (days.is_null(i), resolution, commodity) {
                (false, Some(res), Some(sparte)) => {
                    let date = Date::from_ordinal_date(1970, 1)
                        .expect("epoch")
                        .checked_add(time::Duration::days(i64::from(days.value(i))))
                        .ok_or_else(|| DataFusionError::Execution("date out of range".into()))?;
                    match balancing::expected_intervals_in_balancing_day(date, res, sparte) {
                        Some(n) => out.append_value(n),
                        // Calendar resolutions have no fixed interval count
                        // within a day; null is the honest answer.
                        None => out.append_null(),
                    }
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Parse an ISO 8601 resolution, the form storage uses.
fn parse_resolution(s: &str) -> DfResult<IntervalResolution> {
    s.parse()
        .map_err(|e| DataFusionError::Execution(format!("bad resolution {s:?}: {e}")))
}

/// Every calendar function, ready to register.
pub fn all() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::from(LocalDay::default()),
        ScalarUDF::from(GasDay::default()),
        ScalarUDF::from(BalancingDay::default()),
        ScalarUDF::from(LocalMonth::default()),
        ScalarUDF::from(ExpectedIntervals::default()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use time::macros::{date, datetime};

    /// A session with the calendar functions registered and one row of input.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        for udf in all() {
            ctx.register_udf(udf);
        }
        ctx
    }

    async fn one_date(sql: &str) -> Option<Date> {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date32 result");
        (!array.is_null(0)).then(|| {
            Date::from_ordinal_date(1970, 1)
                .unwrap()
                .checked_add(time::Duration::days(i64::from(array.value(0))))
                .unwrap()
        })
    }

    async fn one_u32(sql: &str) -> Option<u32> {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("uint32 result");
        (!array.is_null(0)).then(|| array.value(0))
    }

    #[tokio::test]
    async fn local_day_uses_the_berlin_boundary_not_the_utc_one() {
        // 23:00 UTC on the 20th is already the 21st in Berlin. This is the bug
        // `date_trunc('day', ...)` would introduce, silently, year-round.
        let got = one_date("SELECT meter_local_day(TIMESTAMP '2026-07-20T23:00:00Z')").await;
        assert_eq!(got, Some(date!(2026 - 07 - 21)));
    }

    #[tokio::test]
    async fn local_day_matches_metering_directly() {
        // The wrapper must not change the answer.
        for instant in [
            datetime!(2026-07-20 12:00 UTC),
            datetime!(2026-01-20 23:30 UTC),
            datetime!(2026-03-29 01:30 UTC),
            datetime!(2026-10-25 01:30 UTC),
        ] {
            let sql = format!(
                "SELECT meter_local_day(TIMESTAMP '{}')",
                instant
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap()
            );
            assert_eq!(
                one_date(&sql).await,
                Some(calendar::local_day(instant)),
                "wrapper disagreed with metering for {instant}"
            );
        }
    }

    #[tokio::test]
    async fn local_month_normalises_to_the_first() {
        assert_eq!(
            one_date("SELECT meter_local_month(TIMESTAMP '2026-07-20T12:00:00Z')").await,
            Some(date!(2026 - 07 - 01))
        );
        // Late-evening UTC on the last of the month is already the next month.
        assert_eq!(
            one_date("SELECT meter_local_month(TIMESTAMP '2026-07-31T23:00:00Z')").await,
            Some(date!(2026 - 08 - 01))
        );
    }

    #[tokio::test]
    async fn expected_intervals_knows_the_dst_days() {
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-07-20', 'PT15M')").await,
            Some(96)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-03-29', 'PT15M')").await,
            Some(92),
            "spring forward"
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-25', 'PT15M')").await,
            Some(100),
            "autumn back"
        );
    }

    #[tokio::test]
    async fn expected_intervals_handles_other_resolutions() {
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-25', 'PT1H')").await,
            Some(25)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-07-20', 'PT30M')").await,
            Some(48)
        );
    }

    #[tokio::test]
    async fn a_calendar_resolution_has_no_interval_count_within_a_day() {
        // A month is not a fixed number of intervals in a day; null, not a lie.
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-07-20', 'P1M')").await,
            None
        );
    }

    #[tokio::test]
    async fn a_resolution_column_is_read_per_row() {
        // A utility carries 15-minute profiles alongside hourly and daily ones,
        // so a completeness report groups rows of differing resolution. Reusing
        // the first row's value would report a day of gaps for every other one.
        let batches = ctx()
            .sql(
                "SELECT meter_expected_intervals(d, r) AS n FROM (
                   SELECT DATE '2026-07-20' AS d, 'PT15M' AS r
                   UNION ALL SELECT DATE '2026-07-20', 'PT1H'
                   UNION ALL SELECT DATE '2026-07-20', 'PT30M'
                 ) ORDER BY n",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let got: Vec<u32> = batches
            .iter()
            .flat_map(|b| {
                let a = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("uint32")
                    .clone();
                (0..a.len()).map(move |i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, vec![24, 48, 96]);
    }

    #[tokio::test]
    async fn a_null_resolution_in_a_column_yields_null_for_that_row_only() {
        let batches = ctx()
            .sql(
                "SELECT meter_expected_intervals(d, r) AS n FROM (
                   SELECT DATE '2026-07-20' AS d, CAST(NULL AS VARCHAR) AS r
                   UNION ALL SELECT DATE '2026-07-20', 'PT15M'
                 )",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut values = Vec::new();
        for b in &batches {
            let a = b.column(0).as_any().downcast_ref::<UInt32Array>().unwrap();
            for i in 0..a.len() {
                values.push((!a.is_null(i)).then(|| a.value(i)));
            }
        }
        values.sort();
        assert_eq!(values, vec![None, Some(96)]);
    }

    #[tokio::test]
    async fn nulls_propagate() {
        assert_eq!(
            one_date("SELECT meter_local_day(CAST(NULL AS TIMESTAMP))").await,
            None
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(CAST(NULL AS DATE), 'PT15M')").await,
            None
        );
    }

    #[tokio::test]
    async fn a_bad_resolution_is_an_error_not_a_wrong_number() {
        let result = ctx()
            .sql("SELECT meter_expected_intervals(DATE '2026-07-20', 'fortnightly')")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn local_day_groups_a_series_correctly() {
        // The whole point: two instants either side of the Berlin midnight must
        // land in different groups even though they share a UTC day.
        let batches = ctx()
            .sql(
                "SELECT meter_local_day(t) AS d, COUNT(*) AS n FROM (
                   SELECT TIMESTAMP '2026-07-20T21:00:00Z' AS t
                   UNION ALL SELECT TIMESTAMP '2026-07-20T23:00:00Z'
                 ) GROUP BY 1 ORDER BY 1",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "same UTC day, different Berlin days");
    }

    // ── the Gastag ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn gas_day_starts_at_0600_local_not_at_midnight() {
        // 03:00 UTC in July is 05:00 local — still the previous Gastag. One hour
        // later it is 06:00 local and the new one has begun.
        assert_eq!(
            one_date("SELECT meter_gas_day(TIMESTAMP '2026-07-15T03:00:00Z')").await,
            Some(date!(2026 - 07 - 14))
        );
        assert_eq!(
            one_date("SELECT meter_gas_day(TIMESTAMP '2026-07-15T04:00:00Z')").await,
            Some(date!(2026 - 07 - 15))
        );
        // Winter: 06:00 CET is 05:00 UTC, so the boundary moves with the offset.
        assert_eq!(
            one_date("SELECT meter_gas_day(TIMESTAMP '2026-01-15T04:59:00Z')").await,
            Some(date!(2026 - 01 - 14))
        );
        assert_eq!(
            one_date("SELECT meter_gas_day(TIMESTAMP '2026-01-15T05:00:00Z')").await,
            Some(date!(2026 - 01 - 15))
        );
    }

    #[tokio::test]
    async fn gas_day_matches_metering_directly() {
        // The wrapper must not change the answer — same contract as the
        // calendar-day wrapper above.
        for instant in [
            datetime!(2026-07-15 03:00 UTC),
            datetime!(2026-01-15 05:00 UTC),
            datetime!(2026-03-29 01:30 UTC),
            datetime!(2026-10-25 01:30 UTC),
            datetime!(2026-10-25 05:30 UTC),
        ] {
            let sql = format!(
                "SELECT meter_gas_day(TIMESTAMP '{}')",
                instant
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap()
            );
            assert_eq!(
                one_date(&sql).await,
                Some(calendar::local_gas_day(instant)),
                "wrapper disagreed with metering for {instant}"
            );
        }
    }

    #[tokio::test]
    async fn balancing_day_follows_the_commodity() {
        // The same instant, two commodities, two days. 22:15 UTC on 14 July is
        // 00:15 local on the 15th — a new calendar day, but still the Gastag of
        // the 14th.
        let at = "TIMESTAMP '2026-07-14T22:15:00Z'";
        assert_eq!(
            one_date(&format!("SELECT meter_balancing_day({at}, 'STROM')")).await,
            Some(date!(2026 - 07 - 15))
        );
        assert_eq!(
            one_date(&format!("SELECT meter_balancing_day({at}, 'GAS')")).await,
            Some(date!(2026 - 07 - 14))
        );
        // Heat and water are calendar-day commodities, not "everything that is
        // not electricity".
        for sparte in ["WAERME", "WASSER"] {
            assert_eq!(
                one_date(&format!("SELECT meter_balancing_day({at}, '{sparte}')")).await,
                Some(date!(2026 - 07 - 15)),
                "{sparte}"
            );
        }
    }

    #[tokio::test]
    async fn balancing_day_reads_the_commodity_per_row() {
        // A mixed scan must not be grouped on whichever Sparte sorted first.
        let batches = ctx()
            .sql(
                "SELECT sparte, meter_balancing_day(t, sparte) AS d FROM (
                   SELECT TIMESTAMP '2026-07-14T22:15:00Z' AS t, 'GAS' AS sparte
                   UNION ALL SELECT TIMESTAMP '2026-07-14T22:15:00Z', 'STROM'
                 ) ORDER BY sparte",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let epoch = Date::from_ordinal_date(1970, 1).unwrap();
        let days = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(
            epoch + time::Duration::days(i64::from(days.value(0))),
            date!(2026 - 07 - 14),
            "GAS sorts first"
        );
        assert_eq!(
            epoch + time::Duration::days(i64::from(days.value(1))),
            date!(2026 - 07 - 15),
            "STROM must not inherit the gas day"
        );
    }

    #[tokio::test]
    async fn balancing_day_rejects_an_unknown_commodity() {
        // Silently defaulting would place the rows on a day nobody asked for.
        let err = ctx()
            .sql("SELECT meter_balancing_day(TIMESTAMP '2026-07-15T03:00:00Z', 'OEL')")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(err.is_err(), "an unknown Sparte must not be defaulted");
    }

    #[tokio::test]
    async fn expected_intervals_puts_the_gas_dst_day_on_the_saturday() {
        // Autumn 2026: the clocks go back at 03:00 local on Sunday the 25th,
        // inside the gas day that began Saturday 06:00. So the 100-interval gas
        // day is the 24th while the 100-interval *calendar* day is the 25th —
        // and a check using the wrong one is wrong in both directions at once.
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-24', 'PT15M', 'GAS')").await,
            Some(100)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-25', 'PT15M', 'GAS')").await,
            Some(96)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-24', 'PT15M', 'STROM')").await,
            Some(96)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-25', 'PT15M', 'STROM')").await,
            Some(100)
        );
    }

    #[tokio::test]
    async fn expected_intervals_without_a_commodity_is_the_calendar_day() {
        // The two-argument form is unchanged, so existing statements keep their
        // meaning rather than silently switching to a gas day.
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-25', 'PT15M')").await,
            Some(100)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-03-29', 'PT15M')").await,
            Some(92)
        );
    }

    #[tokio::test]
    async fn expected_intervals_is_null_for_an_unknown_commodity_column() {
        // A null `sparte` yields null rather than an assumed calendar day.
        assert_eq!(
            one_u32(
                "SELECT meter_expected_intervals(DATE '2026-10-25', 'PT15M', \
                 CAST(NULL AS VARCHAR))"
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn gas_day_is_null_for_a_null_instant() {
        assert_eq!(
            one_date("SELECT meter_gas_day(CAST(NULL AS TIMESTAMP))").await,
            None
        );
        assert_eq!(
            one_date("SELECT meter_balancing_day(CAST(NULL AS TIMESTAMP), 'GAS')").await,
            None
        );
        assert_eq!(
            one_date(
                "SELECT meter_balancing_day(TIMESTAMP '2026-07-15T03:00:00Z', \
                 CAST(NULL AS VARCHAR))"
            )
            .await,
            None
        );
    }
}
