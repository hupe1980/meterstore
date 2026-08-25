//! Extracting a [`TimeRange`] from DataFusion filter expressions.
//!
//! Only the `from` column matters here: it decides which tier holds a row, so a
//! bound on it is what lets the planner skip a tier entirely. Every other
//! predicate is left to DataFusion.
//!
//! **The analysis is deliberately conservative.** Failing to recognise a bound
//! costs a wider scan; wrongly inferring one loses rows. So anything not
//! provably a bound on `from` widens the range rather than narrowing it, and
//! `OR` — where one branch may be unbounded — discards bounds entirely.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Between, BinaryExpr, Expr, Operator};
use time::OffsetDateTime;

use crate::encode::schema::col;
use crate::planner::split::TimeRange;

/// Extract the tightest provable bound on `from` from a set of filters.
///
/// Filters are implicitly `AND`ed, so bounds intersect.
pub fn time_range(filters: &[Expr]) -> TimeRange {
    filters
        .iter()
        .map(range_of)
        .fold(TimeRange::unbounded(), intersect)
}

/// Express a range as filters the cold provider can prune on.
///
/// The tier split narrows a query's range, and that narrowing is only useful to
/// Iceberg if it reaches the scan as a predicate — otherwise partition pruning
/// and row-group statistics see the original, wider bounds.
pub fn range_filters(range: TimeRange) -> Vec<Expr> {
    let mut out = Vec::with_capacity(2);
    if let Some(start) = range.start() {
        out.push(datafusion::logical_expr::col(col::FROM).gt_eq(timestamp_lit(start)));
    }
    if let Some(end) = range.end() {
        out.push(datafusion::logical_expr::col(col::FROM).lt(timestamp_lit(end)));
    }
    out
}

/// A ceiling on the domain version axis, for a reproducible read.
///
/// The Iceberg snapshot pins *transaction* time — what the store had been told
/// by a given moment. It does not pin the domain's own version axis, because a
/// snapshot taken after a correction landed contains both versions and
/// resolution would pick the newer one. Settlement reruns need the value that
/// was in force, so the ceiling is a second, independent bound: keep only
/// versions at or below `max`, then resolve among those.
pub fn version_ceiling(max: crate::version::Version) -> Expr {
    use crate::encode::schema::{VERSION_PRECISION, VERSION_SCALE};
    datafusion::logical_expr::col(col::VERSION).lt_eq(datafusion::logical_expr::lit(
        ScalarValue::Decimal128(Some(max.to_i128()), VERSION_PRECISION, VERSION_SCALE),
    ))
}

/// A ceiling on the transaction-time axis, for an "as known at" read.
///
/// Where [`version_ceiling`] pins which *assertion* was in force, this pins what
/// the store had been **told** — every row carries `recorded_at`, in both tiers,
/// so this is the one reproducible read that can include the hot window.
pub fn recorded_at_ceiling(at: OffsetDateTime) -> Expr {
    datafusion::logical_expr::col(col::RECORDED_AT).lt_eq(timestamp_lit(at))
}

/// A timestamp literal in the unit and zone the storage schema uses.
///
/// The encoding is [`schema::timestamp_scalar`]'s, so a bound this module builds
/// and a value the encoder wrote are the same shape by construction rather than
/// by two functions agreeing.
///
/// [`schema::timestamp_scalar`]: crate::encode::schema::timestamp_scalar
fn timestamp_lit(t: OffsetDateTime) -> Expr {
    datafusion::logical_expr::lit(crate::encode::schema::timestamp_scalar(t))
}

/// The range a single expression constrains `from` to.
fn range_of(filter: &Expr) -> TimeRange {
    match filter {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => intersect(range_of(left), range_of(right)),

            // A row satisfying either branch qualifies, so the result is the
            // union — and unioning with an unrecognised branch yields no usable
            // bound. Narrowing here would silently drop rows.
            Operator::Or => TimeRange::unbounded(),

            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq | Operator::Eq => {
                as_from_bound(left, *op, right).unwrap_or_else(TimeRange::unbounded)
            }

            _ => TimeRange::unbounded(),
        },

        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) if is_from_column(expr) => {
            match (as_timestamp(low), as_timestamp(high)) {
                // BETWEEN is inclusive on both sides; our upper bound is
                // exclusive, so the high value must still be matched.
                (Some(lo), Some(hi)) => TimeRange::new(Some(lo), Some(exclusive_after(hi))),
                _ => TimeRange::unbounded(),
            }
        }

        _ => TimeRange::unbounded(),
    }
}

/// Interpret `left op right` as a bound on `from`, if it is one.
///
/// Handles the literal on either side, flipping the operator when the column is
/// on the right.
fn as_from_bound(left: &Expr, op: Operator, right: &Expr) -> Option<TimeRange> {
    if is_from_column(left) {
        bound_from(op, as_timestamp(right)?)
    } else if is_from_column(right) {
        bound_from(flip(op), as_timestamp(left)?)
    } else {
        None
    }
}

/// The range implied by `from <op> value`.
fn bound_from(op: Operator, value: OffsetDateTime) -> Option<TimeRange> {
    Some(match op {
        Operator::Lt => TimeRange::new(None, Some(value)),
        Operator::LtEq => TimeRange::new(None, Some(exclusive_after(value))),
        Operator::Gt => TimeRange::new(Some(exclusive_after(value)), None),
        Operator::GtEq => TimeRange::new(Some(value), None),
        // A point lookup is a range of one instant.
        Operator::Eq => TimeRange::new(Some(value), Some(exclusive_after(value))),
        _ => return None,
    })
}

/// Mirror a comparison so the column can be treated as the left operand.
fn flip(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// The smallest instant strictly after `t`, at storage resolution.
///
/// Storage is microsecond-precision, so converting an inclusive bound to an
/// exclusive one is exact rather than an approximation.
fn exclusive_after(t: OffsetDateTime) -> OffsetDateTime {
    t + time::Duration::microseconds(1)
}

/// Whether an expression is the `from` column, or a **lossless** cast of it.
///
/// DataFusion routinely inserts a cast around a timestamp comparison — it
/// unifies the two sides on the finer unit, which for this column means
/// `Timestamp(Microsecond)` widened to `Timestamp(Nanosecond)` — and refusing to
/// see through that would give up the bound on ordinary queries.
///
/// But only a cast that **preserves the ordering of every value** may be seen
/// through, and that is the narrower rule this implements. A lossy cast
/// truncates: `CAST("from" AS DATE) = …` matches a whole day, and treating it as
/// a bound on `from` itself would extract a one-microsecond range and drop the
/// other 95 intervals — silently, since the rows simply are not in the scan. The
/// module's whole contract (§17.1) is that the analysis may only ever be wrong in
/// the widening direction, and an unrestricted cast breaks it.
///
/// So: the column itself, or a cast to a timestamp no coarser than the column's
/// own microseconds. Anything else is not recognised, the range stays unbounded,
/// and the query is merely slower.
fn is_from_column(expr: &Expr) -> bool {
    match expr {
        Expr::Column(c) => c.name == col::FROM,
        Expr::Cast(cast) if is_lossless_target(&cast.data_type) => is_from_column(&cast.expr),
        Expr::TryCast(cast) if is_lossless_target(&cast.data_type) => is_from_column(&cast.expr),
        _ => false,
    }
}

/// Whether casting `from` to this type keeps every stored instant distinct.
///
/// The stored column is `Timestamp(Microsecond, UTC)`, so microseconds and
/// nanoseconds round-trip and everything else — a coarser unit, a date, a string
/// — collapses instants that the predicate then cannot tell apart.
fn is_lossless_target(ty: &crate::arrow::datatypes::DataType) -> bool {
    use crate::arrow::datatypes::{DataType, TimeUnit};
    matches!(
        ty,
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
    )
}

/// Read a literal timestamp, whatever unit it was written in.
///
/// A cast **around the literal** is seen through only when it cannot move the
/// value, by the same rule [`is_from_column`] applies to the column side. The
/// asymmetry is tempting — the column's cast truncates rows, the literal's only
/// truncates one number — and it is not real: `from >= CAST(<nanos> AS
/// Timestamp(Second))` bounds `from` at the *truncated* second, which is below
/// the literal, so reading the literal instead narrows the range and drops the
/// rows in between. §17.1 permits error in the widening direction only, so an
/// unrecognised cast leaves the range unbounded and costs a scan.
fn as_timestamp(expr: &Expr) -> Option<OffsetDateTime> {
    let scalar = match expr {
        Expr::Literal(v, _) => v,
        Expr::Cast(cast) if is_lossless_target(&cast.data_type) => {
            return as_timestamp(&cast.expr);
        }
        Expr::TryCast(cast) if is_lossless_target(&cast.data_type) => {
            return as_timestamp(&cast.expr);
        }
        _ => return None,
    };

    let nanos: i128 = match scalar {
        ScalarValue::TimestampNanosecond(Some(v), _) => i128::from(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => i128::from(*v) * 1_000,
        ScalarValue::TimestampMillisecond(Some(v), _) => i128::from(*v) * 1_000_000,
        ScalarValue::TimestampSecond(Some(v), _) => i128::from(*v) * 1_000_000_000,
        _ => return None,
    };

    OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()
}

/// The tighter of two ranges.
fn intersect(a: TimeRange, b: TimeRange) -> TimeRange {
    let start = match (a.start(), b.start()) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    };
    let end = match (a.end(), b.end()) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    };
    TimeRange::new(start, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col as df_col, lit};
    use time::macros::datetime;

    const T10: OffsetDateTime = datetime!(2026-07-10 00:00 UTC);
    const T20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

    /// A timestamp literal in the unit storage uses.
    fn ts(t: OffsetDateTime) -> Expr {
        lit(crate::encode::schema::timestamp_scalar(t))
    }

    fn from() -> Expr {
        df_col(col::FROM)
    }

    #[test]
    fn range_filters_round_trip_through_extraction() {
        // What the split narrows must be re-extractable, or the cold scan is
        // handed bounds the planner cannot then use for pruning.
        let range = TimeRange::between(T10, T20);
        let recovered = time_range(&range_filters(range));
        assert_eq!(recovered, range);
    }

    #[test]
    fn range_filters_omit_absent_bounds() {
        assert!(range_filters(TimeRange::unbounded()).is_empty());
        assert_eq!(range_filters(TimeRange::new(Some(T10), None)).len(), 1);
        assert_eq!(range_filters(TimeRange::new(None, Some(T20))).len(), 1);
        assert_eq!(range_filters(TimeRange::between(T10, T20)).len(), 2);
    }

    #[test]
    fn no_filters_gives_an_unbounded_range() {
        assert_eq!(time_range(&[]), TimeRange::unbounded());
    }

    #[test]
    fn greater_than_or_equal_becomes_an_inclusive_lower_bound() {
        let r = time_range(&[from().gt_eq(ts(T10))]);
        assert_eq!(r.start(), Some(T10));
        assert_eq!(r.end(), None);
    }

    #[test]
    fn strictly_greater_than_excludes_the_bound_itself() {
        let r = time_range(&[from().gt(ts(T10))]);
        assert_eq!(r.start(), Some(exclusive_after(T10)));
        assert!(r.start().unwrap() > T10);
    }

    #[test]
    fn less_than_becomes_an_exclusive_upper_bound() {
        let r = time_range(&[from().lt(ts(T20))]);
        assert_eq!(r.end(), Some(T20));
        assert_eq!(r.start(), None);
    }

    #[test]
    fn less_than_or_equal_still_matches_the_bound() {
        // Our upper bound is exclusive, so <= must extend past the value or the
        // matching row is dropped.
        let r = time_range(&[from().lt_eq(ts(T20))]);
        assert!(r.end().unwrap() > T20);
    }

    #[test]
    fn a_conjunction_intersects_bounds() {
        let r = time_range(&[from().gt_eq(ts(T10)), from().lt(ts(T20))]);
        assert_eq!(r, TimeRange::between(T10, T20));
    }

    #[test]
    fn a_nested_and_intersects_too() {
        let r = time_range(&[from().gt_eq(ts(T10)).and(from().lt(ts(T20)))]);
        assert_eq!(r, TimeRange::between(T10, T20));
    }

    #[test]
    fn the_tightest_bound_wins() {
        let mid = datetime!(2026-07-15 00:00 UTC);
        let r = time_range(&[from().gt_eq(ts(T10)), from().gt_eq(ts(mid))]);
        assert_eq!(r.start(), Some(mid));
    }

    #[test]
    fn a_literal_on_the_left_flips_the_operator() {
        // `'2026-07-10' <= from` is the same as `from >= '2026-07-10'`.
        let r = time_range(&[ts(T10).lt_eq(from())]);
        assert_eq!(r.start(), Some(T10));
        assert_eq!(r.end(), None);
    }

    #[test]
    fn between_is_inclusive_on_both_sides() {
        let r = time_range(&[from().between(ts(T10), ts(T20))]);
        assert_eq!(r.start(), Some(T10));
        assert!(r.end().unwrap() > T20, "the high value must still match");
    }

    #[test]
    fn equality_is_a_single_instant() {
        let r = time_range(&[from().eq(ts(T10))]);
        assert_eq!(r.start(), Some(T10));
        assert!(r.end().unwrap() > T10);
        assert!(!r.is_empty());
    }

    #[test]
    fn a_disjunction_yields_no_bound() {
        // One branch could be unbounded, so narrowing would drop rows.
        let r = time_range(&[from().gt_eq(ts(T10)).or(from().lt(ts(T20)))]);
        assert_eq!(r, TimeRange::unbounded());
    }

    #[test]
    fn a_disjunction_does_not_poison_a_sibling_conjunct() {
        // The AND still contributes its own bound.
        let r = time_range(&[
            from().gt_eq(ts(T10)),
            from().gt_eq(ts(T20)).or(from().lt(ts(T10))),
        ]);
        assert_eq!(r.start(), Some(T10));
    }

    #[test]
    fn predicates_on_other_columns_are_ignored() {
        let r = time_range(&[df_col(col::MALO_ID).eq(lit("12345678905"))]);
        assert_eq!(r, TimeRange::unbounded());
    }

    #[test]
    fn a_bound_on_another_timestamp_column_is_not_used() {
        // `to` does not determine the tier; only `from` does.
        let r = time_range(&[df_col(col::TO).gt_eq(ts(T10))]);
        assert_eq!(r, TimeRange::unbounded());
    }

    /// `CAST(from AS <ty>)`, the shape DataFusion inserts around a comparison.
    fn cast_from(ty: crate::arrow::datatypes::DataType) -> Expr {
        Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(from()), ty))
    }

    fn timestamp(unit: crate::arrow::datatypes::TimeUnit) -> crate::arrow::datatypes::DataType {
        crate::arrow::datatypes::DataType::Timestamp(unit, Some("UTC".into()))
    }

    #[test]
    fn a_lossless_cast_around_the_column_is_seen_through() {
        use crate::arrow::datatypes::TimeUnit;

        // The column's own unit, and the finer one DataFusion unifies on when
        // the other side is a nanosecond literal. Both keep every stored instant
        // distinct, so the bound they carry is the column's bound.
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            let r = time_range(&[cast_from(timestamp(unit)).gt_eq(ts(T10))]);
            assert_eq!(r.start(), Some(T10), "{unit:?}");
        }
    }

    #[test]
    fn a_truncating_cast_is_not_mistaken_for_a_bound_on_the_column() {
        use crate::arrow::datatypes::{DataType, TimeUnit};

        // The failure this prevents. `CAST("from" AS DATE) = <an instant>` is
        // satisfied by every reading of that day, but read as a bound on `from`
        // itself it extracts a one-microsecond range — and the other 95 intervals
        // are dropped from the scan with nothing to notice it. Same for a cast
        // down to seconds or milliseconds, which collapses sub-unit instants.
        //
        // Widening is the only legal direction here (§17.1), so an unrecognised
        // shape must cost a scan rather than a row.
        for ty in [
            DataType::Date32,
            timestamp(TimeUnit::Second),
            timestamp(TimeUnit::Millisecond),
        ] {
            assert_eq!(
                time_range(&[cast_from(ty.clone()).eq(ts(T10))]),
                TimeRange::unbounded(),
                "{ty:?} truncates, so it cannot bound `from`"
            );
        }
    }

    #[test]
    fn a_lossless_cast_over_a_truncating_one_is_still_refused() {
        use crate::arrow::datatypes::{DataType, TimeUnit};

        // Nesting must not launder the truncation: casting the *day* back up to
        // microseconds restores the type but not the instant.
        let inner = cast_from(DataType::Date32);
        let outer = Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(inner),
            timestamp(TimeUnit::Microsecond),
        ));
        assert_eq!(time_range(&[outer.eq(ts(T10))]), TimeRange::unbounded());
    }

    #[test]
    fn timestamp_literals_in_other_units_are_understood() {
        let secs = lit(ScalarValue::TimestampSecond(
            Some(T10.unix_timestamp()),
            None,
        ));
        assert_eq!(time_range(&[from().gt_eq(secs)]).start(), Some(T10));

        let millis = lit(ScalarValue::TimestampMillisecond(
            Some(T10.unix_timestamp() * 1_000),
            None,
        ));
        assert_eq!(time_range(&[from().gt_eq(millis)]).start(), Some(T10));
    }

    #[test]
    fn a_truncating_cast_around_the_literal_yields_no_bound_either() {
        use crate::arrow::datatypes::{DataType, TimeUnit};

        // The mirror of `a_truncating_cast_is_not_mistaken_for_a_bound_on_the
        // _column`, and the direction that is easy to wave through: casting the
        // *literal* down to seconds truncates it, so `from >= CAST(t AS
        // TIMESTAMP(0))` admits rows from the start of that second onwards —
        // below the literal. Reading the literal instead narrows the range and
        // drops them. §17.1 permits widening only.
        let inner = ts(T10 + time::Duration::microseconds(500_000));
        for ty in [
            timestamp(TimeUnit::Second),
            timestamp(TimeUnit::Millisecond),
            DataType::Date32,
        ] {
            let literal = Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(inner.clone()),
                ty.clone(),
            ));
            assert_eq!(
                time_range(&[from().gt_eq(literal)]),
                TimeRange::unbounded(),
                "{ty:?} truncates the literal, so it cannot bound `from`"
            );
        }
    }

    #[test]
    fn a_widening_cast_around_the_literal_is_still_seen_through() {
        use crate::arrow::datatypes::TimeUnit;

        // DataFusion unifies the two sides on the finer unit, so this is the
        // shape an ordinary query produces. It moves no value and must not cost
        // the bound.
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            let literal = Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(ts(T10)),
                timestamp(unit),
            ));
            assert_eq!(time_range(&[from().gt_eq(literal)]).start(), Some(T10));
        }
    }

    #[test]
    fn a_null_timestamp_yields_no_bound() {
        let null = lit(ScalarValue::TimestampMicrosecond(None, None));
        assert_eq!(time_range(&[from().gt_eq(null)]), TimeRange::unbounded());
    }

    #[test]
    fn contradictory_bounds_produce_an_empty_range() {
        let r = time_range(&[from().gt_eq(ts(T20)), from().lt(ts(T10))]);
        assert!(r.is_empty());
    }

    #[test]
    fn every_recognised_shape_still_narrows_the_range() {
        // Pushdown is always `Inexact` — correctness must not rest on a
        // dependency's silent conversion — but the *narrowing* is what skips
        // files, and that is the optimisation worth having.
        for filter in [
            from().gt_eq(ts(T10)),
            from().lt(ts(T20)),
            from().gt_eq(ts(T10)).and(from().lt(ts(T20))),
            from().between(ts(T10), ts(T20)),
        ] {
            assert_ne!(
                time_range(std::slice::from_ref(&filter)),
                TimeRange::unbounded(),
                "{filter} must still narrow the scan"
            );
        }
    }

    #[test]
    fn unrecognised_shapes_widen_rather_than_narrow() {
        // Failing to recognise a bound must cost a wider scan, never a lost row.
        for filter in [
            df_col(col::MALO_ID).eq(lit("x")),
            from().gt_eq(ts(T10)).or(from().lt(ts(T20))),
            from().is_null(),
        ] {
            assert_eq!(
                time_range(std::slice::from_ref(&filter)),
                TimeRange::unbounded(),
                "{filter} must not be mistaken for a bound"
            );
        }
    }
}

/// The safety property, over generated expressions rather than chosen ones.
///
/// # Why this is a property test and not a table of cases
///
/// [`time_range`] is an *analysis*: it looks at an expression tree and claims a
/// bound. The claim is used to skip a tier entirely, so the only thing that
/// matters is the direction of its error. Too wide costs a scan. **Too narrow
/// loses rows, silently**, and the rows it loses are exactly the ones whose
/// expression shape nobody thought to write a case for.
///
/// The hand-written cases below cover the shapes someone thought of. This covers
/// the shapes nobody did — nesting, operand order, `OR` beneath `AND`,
/// unrecognised predicates mixed with recognised ones — by generating them and
/// asserting the one thing that must never fail:
///
/// > if a row satisfies the filter, the extracted range must contain it.
///
/// The reference is an independent evaluator over the same generated tree, for
/// the same reason the §17.3 oracle is: an evaluator that shared `range_of`'s
/// reasoning would agree with it about its mistakes.
#[cfg(test)]
mod properties {
    use super::*;
    use datafusion::logical_expr::{col as df_col, lit};
    use proptest::prelude::*;

    /// A filter shape, buildable as a DataFusion expression *and* evaluable
    /// directly — the two halves the property compares.
    #[derive(Debug, Clone)]
    enum Pred {
        /// `from <op> t`, seconds since the epoch.
        Cmp(Operator, i64),
        /// `t <op> from` — the same bound with the operands the other way round,
        /// which the analysis has to flip rather than ignore.
        Flipped(Operator, i64),
        /// `from BETWEEN lo AND hi`, inclusive at both ends.
        Between(i64, i64),
        /// A predicate on some other column. It constrains nothing about `from`,
        /// so any instant can satisfy it — and the analysis must not read a
        /// bound into it.
        Foreign,
        And(Box<Pred>, Box<Pred>),
        Or(Box<Pred>, Box<Pred>),
    }

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("in range")
    }

    fn ts(seconds: i64) -> Expr {
        lit(ScalarValue::TimestampMicrosecond(
            Some(seconds * 1_000_000),
            Some("UTC".into()),
        ))
    }

    impl Pred {
        fn to_expr(&self) -> Expr {
            let from = df_col(col::FROM);
            match self {
                Self::Cmp(op, t) => binary(from, *op, ts(*t)),
                Self::Flipped(op, t) => binary(ts(*t), *op, from),
                Self::Between(lo, hi) => from.between(ts(*lo), ts(*hi)),
                Self::Foreign => df_col(col::MALO_ID).eq(lit("12345678905")),
                Self::And(a, b) => a.to_expr().and(b.to_expr()),
                Self::Or(a, b) => a.to_expr().or(b.to_expr()),
            }
        }

        /// Whether a row whose `from` is `t` satisfies this filter.
        fn holds(&self, t: i64) -> bool {
            match self {
                Self::Cmp(op, v) => compare(t, *op, *v),
                // `t <op> from` is `from <flipped op> t`.
                Self::Flipped(op, v) => compare(*v, *op, t),
                Self::Between(lo, hi) => t >= *lo && t <= *hi,
                // Satisfiable at any instant.
                Self::Foreign => true,
                Self::And(a, b) => a.holds(t) && b.holds(t),
                Self::Or(a, b) => a.holds(t) || b.holds(t),
            }
        }
    }

    fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
    }

    fn compare(left: i64, op: Operator, right: i64) -> bool {
        match op {
            Operator::Lt => left < right,
            Operator::LtEq => left <= right,
            Operator::Gt => left > right,
            Operator::GtEq => left >= right,
            Operator::Eq => left == right,
            other => unreachable!("generator produces no {other:?}"),
        }
    }

    fn contains(range: TimeRange, t: i64) -> bool {
        let at = at(t);
        range.start().is_none_or(|s| at >= s) && range.end().is_none_or(|e| at < e)
    }

    /// Instants are drawn from a small window so a generated bound and a
    /// generated probe actually meet — over the whole `i64` range they never
    /// would, and every case would pass vacuously.
    const WINDOW: std::ops::Range<i64> = 0..40;

    fn leaf() -> impl Strategy<Value = Pred> {
        let op = prop_oneof![
            Just(Operator::Lt),
            Just(Operator::LtEq),
            Just(Operator::Gt),
            Just(Operator::GtEq),
            Just(Operator::Eq),
        ];
        prop_oneof![
            8 => (op.clone(), WINDOW).prop_map(|(o, t)| Pred::Cmp(o, t)),
            4 => (op, WINDOW).prop_map(|(o, t)| Pred::Flipped(o, t)),
            3 => (WINDOW, WINDOW).prop_map(|(a, b)| Pred::Between(a.min(b), a.max(b))),
            1 => Just(Pred::Foreign),
        ]
    }

    fn predicate() -> impl Strategy<Value = Pred> {
        leaf().prop_recursive(4, 24, 2, |inner| {
            prop_oneof![
                inner
                    .clone()
                    .prop_flat_map(move |a| leaf()
                        .prop_map(move |b| Pred::And(Box::new(a.clone()), Box::new(b)))),
                inner
                    .prop_flat_map(move |a| leaf()
                        .prop_map(move |b| Pred::Or(Box::new(a.clone()), Box::new(b)))),
            ]
        })
    }

    proptest! {
        /// **The property the tier split rests on.** A row the filter admits must
        /// lie inside the range the planner extracted, or the scan that range
        /// selects will not read it — and nothing downstream notices, because the
        /// row simply is not there.
        #[test]
        fn an_admitted_row_is_never_outside_the_extracted_range(
            pred in predicate(),
            probe in WINDOW,
        ) {
            let range = time_range(&[pred.to_expr()]);
            prop_assert!(
                !pred.holds(probe) || contains(range, probe),
                "{pred:?} admits {probe} but the extracted range {range:?} excludes it",
            );
        }

        /// The same, for a list of filters — which DataFusion hands over
        /// implicitly `AND`ed, so the bounds intersect and each intersection is
        /// another chance to narrow too far.
        #[test]
        fn intersecting_several_filters_stays_conservative(
            preds in prop::collection::vec(predicate(), 1..4),
            probe in WINDOW,
        ) {
            let exprs: Vec<Expr> = preds.iter().map(Pred::to_expr).collect();
            let range = time_range(&exprs);
            let admitted = preds.iter().all(|p| p.holds(probe));
            prop_assert!(
                !admitted || contains(range, probe),
                "{preds:?} admit {probe} but {range:?} excludes it",
            );
        }

        /// A range narrowed by the tier split is handed to the cold scan as
        /// filters, and must survive the round trip — otherwise Iceberg prunes
        /// against bounds wider than the ones the planner chose, or narrower.
        #[test]
        fn range_filters_re_extract_to_the_same_bounds(a in WINDOW, b in WINDOW) {
            let range = TimeRange::between(at(a.min(b)), at(a.max(b) + 1));
            prop_assert_eq!(time_range(&range_filters(range)), range);
        }
    }
}
