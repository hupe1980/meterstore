//! Splitting a query's time range across the two tiers.
//!
//! The tiers are disjoint by construction — a row's `from` alone decides which
//! one holds it — so a query's interval range can be cut at the watermark and
//! the halves unioned with no deduplication.
//!
//! Getting the boundary condition wrong in either direction is silent: too
//! inclusive and rows are counted twice, too exclusive and they vanish. The
//! split is therefore computed in one place and exhaustively tested.

use time::OffsetDateTime;

use crate::watermark::TieringWatermark;

/// A half-open time range `[start, end)`, either bound optional.
///
/// `None` means unbounded on that side. A query with no time predicate at all
/// is `TimeRange::unbounded()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    start: Option<OffsetDateTime>,
    end: Option<OffsetDateTime>,
}

impl TimeRange {
    /// A range with the given bounds.
    ///
    /// Bounds that cross produce an empty range rather than an error: a query
    /// like `from >= '2026-02-01' AND from < '2026-01-01'` is legal SQL that
    /// simply matches nothing.
    pub fn new(start: Option<OffsetDateTime>, end: Option<OffsetDateTime>) -> Self {
        Self { start, end }
    }

    /// The range matching everything.
    pub const fn unbounded() -> Self {
        Self {
            start: None,
            end: None,
        }
    }

    /// A closed range.
    pub fn between(start: OffsetDateTime, end: OffsetDateTime) -> Self {
        Self::new(Some(start), Some(end))
    }

    /// Inclusive lower bound, if any.
    pub const fn start(self) -> Option<OffsetDateTime> {
        self.start
    }

    /// Exclusive upper bound, if any.
    pub const fn end(self) -> Option<OffsetDateTime> {
        self.end
    }

    /// Whether this range matches nothing.
    pub fn is_empty(self) -> bool {
        matches!((self.start, self.end), (Some(s), Some(e)) if s >= e)
    }

    /// Whether both bounds are present.
    pub const fn is_bounded(self) -> bool {
        self.start.is_some() && self.end.is_some()
    }

    /// Narrow this range to end no later than `limit`.
    fn capped_at(self, limit: OffsetDateTime) -> Self {
        let end = match self.end {
            Some(e) => Some(e.min(limit)),
            None => Some(limit),
        };
        Self {
            start: self.start,
            end,
        }
    }

    /// Narrow this range to start no earlier than `limit`.
    fn floored_at(self, limit: OffsetDateTime) -> Self {
        let start = match self.start {
            Some(s) => Some(s.max(limit)),
            None => Some(limit),
        };
        Self {
            start,
            end: self.end,
        }
    }
}

/// Which tiers a query must read, and over what range each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierSplit {
    /// Range to read from the cold tier, if any.
    pub cold: Option<TimeRange>,
    /// Range to read from the hot tier, if any.
    pub hot: Option<TimeRange>,
}

impl TierSplit {
    /// Whether the query touches both tiers, and so needs a union.
    pub const fn spans_tiers(&self) -> bool {
        self.cold.is_some() && self.hot.is_some()
    }

    /// Whether the query reads nothing.
    pub const fn is_empty(&self) -> bool {
        self.cold.is_none() && self.hot.is_none()
    }

    /// Whether the query can be answered without touching PostgreSQL.
    ///
    /// True for historical reporting, which is worth knowing: such a query can
    /// run against the lake alone, with no load on the operational database.
    pub const fn is_cold_only(&self) -> bool {
        self.cold.is_some() && self.hot.is_none()
    }
}

/// Split a query's time range at the watermark.
///
/// The cold half is `[start, min(end, watermark))` and the hot half is
/// `[max(start, watermark), end)`. Because the watermark is the exclusive upper
/// bound of one and the inclusive lower bound of the other, the halves are
/// disjoint and gapless — which is what makes `UNION ALL` correct.
pub fn split(range: TimeRange, watermark: TieringWatermark) -> TierSplit {
    if range.is_empty() {
        return TierSplit {
            cold: None,
            hot: None,
        };
    }

    // An epoch watermark means no window has ever been committed, so the cold
    // tier holds nothing. Without this, an unbounded query would still plan a
    // cold scan of `(-inf, epoch)` — a correct range, but one that costs an
    // Iceberg planning round trip to return zero rows.
    if watermark == TieringWatermark::empty() {
        return TierSplit {
            cold: None,
            hot: Some(range),
        };
    }

    let boundary = watermark.get();

    // Cold: everything strictly below the watermark.
    let cold = match range.start {
        Some(s) if s >= boundary => None,
        _ => {
            let capped = range.capped_at(boundary);
            (!capped.is_empty()).then_some(capped)
        }
    };

    // Hot: everything at or above it.
    let hot = match range.end {
        Some(e) if e <= boundary => None,
        _ => {
            let floored = range.floored_at(boundary);
            (!floored.is_empty()).then_some(floored)
        }
    };

    TierSplit { cold, hot }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    const BOUNDARY: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

    fn watermark() -> TieringWatermark {
        TieringWatermark::new(BOUNDARY)
    }

    #[test]
    fn a_range_entirely_below_the_watermark_is_cold_only() {
        let r = TimeRange::between(
            datetime!(2026-07-01 00:00 UTC),
            datetime!(2026-07-10 00:00 UTC),
        );
        let s = split(r, watermark());

        assert_eq!(s.cold, Some(r));
        assert_eq!(s.hot, None);
        assert!(s.is_cold_only());
        assert!(!s.spans_tiers());
    }

    #[test]
    fn a_range_entirely_at_or_above_the_watermark_is_hot_only() {
        let r = TimeRange::between(
            datetime!(2026-07-25 00:00 UTC),
            datetime!(2026-07-30 00:00 UTC),
        );
        let s = split(r, watermark());

        assert_eq!(s.hot, Some(r));
        assert_eq!(s.cold, None);
        assert!(!s.is_cold_only());
    }

    #[test]
    fn a_range_spanning_the_watermark_is_cut_at_it() {
        let start = datetime!(2026-07-10 00:00 UTC);
        let end = datetime!(2026-07-30 00:00 UTC);
        let s = split(TimeRange::between(start, end), watermark());

        assert!(s.spans_tiers());
        assert_eq!(s.cold, Some(TimeRange::between(start, BOUNDARY)));
        assert_eq!(s.hot, Some(TimeRange::between(BOUNDARY, end)));
    }

    #[test]
    fn the_halves_are_disjoint_and_gapless() {
        // The property that makes UNION ALL correct: cold ends exactly where
        // hot begins, and neither includes the other's rows.
        let start = datetime!(2026-07-10 00:00 UTC);
        let end = datetime!(2026-07-30 00:00 UTC);
        let s = split(TimeRange::between(start, end), watermark());

        let cold = s.cold.unwrap();
        let hot = s.hot.unwrap();
        assert_eq!(cold.end().unwrap(), hot.start().unwrap());
        assert_eq!(cold.start().unwrap(), start);
        assert_eq!(hot.end().unwrap(), end);
    }

    #[test]
    fn the_boundary_instant_belongs_to_the_hot_tier() {
        // `from >= watermark` is hot. A range starting exactly at the boundary
        // must not read cold at all, or the same row is fetched twice.
        let s = split(
            TimeRange::between(BOUNDARY, datetime!(2026-07-25 00:00 UTC)),
            watermark(),
        );
        assert_eq!(s.cold, None);
        assert!(s.hot.is_some());
    }

    #[test]
    fn a_range_ending_exactly_at_the_boundary_is_cold_only() {
        // Exclusive upper bound: `from < watermark` is entirely cold.
        let s = split(
            TimeRange::between(datetime!(2026-07-10 00:00 UTC), BOUNDARY),
            watermark(),
        );
        assert!(s.is_cold_only());
        assert_eq!(s.hot, None);
    }

    #[test]
    fn an_unbounded_query_reads_both_tiers() {
        let s = split(TimeRange::unbounded(), watermark());

        assert!(s.spans_tiers());
        assert_eq!(s.cold.unwrap().end(), Some(BOUNDARY));
        assert_eq!(s.cold.unwrap().start(), None);
        assert_eq!(s.hot.unwrap().start(), Some(BOUNDARY));
        assert_eq!(s.hot.unwrap().end(), None);
    }

    #[test]
    fn a_half_open_range_keeps_its_open_side() {
        let after = TimeRange::new(Some(datetime!(2026-07-10 00:00 UTC)), None);
        let s = split(after, watermark());
        assert_eq!(s.hot.unwrap().end(), None);

        let before = TimeRange::new(None, Some(datetime!(2026-07-25 00:00 UTC)));
        let s = split(before, watermark());
        assert_eq!(s.cold.unwrap().start(), None);
    }

    #[test]
    fn an_empty_range_reads_nothing() {
        // Legal SQL that matches nothing must not be turned into a full scan.
        let inverted = TimeRange::between(
            datetime!(2026-02-01 00:00 UTC),
            datetime!(2026-01-01 00:00 UTC),
        );
        assert!(inverted.is_empty());

        let s = split(inverted, watermark());
        assert!(s.is_empty());
        assert!(!s.spans_tiers());
    }

    #[test]
    fn a_zero_width_range_reads_nothing() {
        let t = datetime!(2026-07-15 00:00 UTC);
        assert!(split(TimeRange::between(t, t), watermark()).is_empty());
    }

    #[test]
    fn an_empty_watermark_sends_everything_to_the_hot_tier() {
        // Nothing archived yet: there is no cold data to read.
        let s = split(
            TimeRange::between(
                datetime!(2026-07-01 00:00 UTC),
                datetime!(2026-07-30 00:00 UTC),
            ),
            TieringWatermark::empty(),
        );
        assert_eq!(s.cold, None);
        assert!(s.hot.is_some());
    }

    #[test]
    fn an_unbounded_query_against_an_empty_watermark_plans_no_cold_scan() {
        // Without the epoch short-circuit this would plan a cold scan of
        // `(-inf, epoch)` — correct, but a round trip for certainly zero rows.
        let s = split(TimeRange::unbounded(), TieringWatermark::empty());
        assert_eq!(s.cold, None);
        assert_eq!(s.hot, Some(TimeRange::unbounded()));
    }

    #[test]
    fn every_instant_in_the_query_lands_in_exactly_one_tier() {
        // Exhaustive check of the property the whole design rests on.
        let start = datetime!(2026-07-18 00:00 UTC);
        let end = datetime!(2026-07-22 00:00 UTC);
        let s = split(TimeRange::between(start, end), watermark());

        let mut probe = start;
        while probe < end {
            let in_cold = s.cold.is_some_and(|r| {
                r.start().is_none_or(|x| probe >= x) && r.end().is_none_or(|x| probe < x)
            });
            let in_hot = s.hot.is_some_and(|r| {
                r.start().is_none_or(|x| probe >= x) && r.end().is_none_or(|x| probe < x)
            });

            assert!(
                in_cold ^ in_hot,
                "{probe} landed in {} tiers, expected exactly 1",
                u8::from(in_cold) + u8::from(in_hot)
            );
            probe += time::Duration::hours(1);
        }
    }
}

/// The split's two properties, over generated inputs rather than chosen ones.
///
/// §6.3 is the whole correctness argument of the design, and it reduces to two
/// statements about this function. Both are silent when broken: too inclusive
/// and a row is counted twice, too exclusive and it vanishes. Neither shows up as
/// an error anywhere downstream, so the case nobody wrote is the case that ships.
#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("in range")
    }

    const WINDOW: std::ops::Range<i64> = 0..40;

    fn covers(range: TimeRange, t: OffsetDateTime) -> bool {
        range.start().is_none_or(|s| t >= s) && range.end().is_none_or(|e| t < e)
    }

    /// A bound, or none — so half-open and fully unbounded ranges are generated
    /// too, which is what an unfiltered query produces.
    fn bound() -> impl Strategy<Value = Option<i64>> {
        prop_oneof![1 => Just(None), 4 => WINDOW.prop_map(Some)]
    }

    proptest! {
        /// **Exhaustive and disjoint.** Every instant the query asked for lands in
        /// exactly one half — never both (which double-counts) and never neither
        /// (which loses the row). This is `UNION ALL` being correct, stated as a
        /// property rather than as a paragraph.
        #[test]
        fn every_instant_lands_in_exactly_one_tier(
            start in bound(),
            end in bound(),
            boundary in WINDOW,
            probe in WINDOW,
        ) {
            let range = TimeRange::new(start.map(at), end.map(at));
            let watermark = TieringWatermark::new(at(boundary));
            let split = split(range, watermark);
            let probe = at(probe);

            // Only instants the query actually asked for are in scope: the split
            // says nothing about the rest, and must not.
            prop_assume!(covers(range, probe));

            let cold = split.cold.is_some_and(|r| covers(r, probe));
            let hot = split.hot.is_some_and(|r| covers(r, probe));
            prop_assert!(
                cold ^ hot,
                "{probe} landed in {} halves of {split:?} at watermark {watermark}",
                u8::from(cold) + u8::from(hot),
            );
        }

        /// **The halves agree with the routing rule.** `tier_for` is the single
        /// place a row's tier is decided (§6.1); a split that disagreed with it
        /// would send a query to the tier that does not hold the row.
        #[test]
        fn each_half_holds_only_what_the_routing_rule_assigns_it(
            start in bound(),
            end in bound(),
            boundary in WINDOW,
            probe in WINDOW,
        ) {
            let range = TimeRange::new(start.map(at), end.map(at));
            let watermark = TieringWatermark::new(at(boundary));
            let split = split(range, watermark);
            let probe = at(probe);

            if split.cold.is_some_and(|r| covers(r, probe)) {
                prop_assert_eq!(watermark.tier_for(probe), crate::watermark::Tier::Cold);
            }
            if split.hot.is_some_and(|r| covers(r, probe)) {
                prop_assert_eq!(watermark.tier_for(probe), crate::watermark::Tier::Hot);
            }
        }
    }
}
