//! Which day a reading is balanced on, and how many intervals that day holds.
//!
//! # What this module decides
//!
//! MeterStore implements neither the calendar arithmetic nor the *choice* of
//! calendar. `metering::time::calendar` owns both, as [`DayBoundary`] — `Strom`
//! or `Gas` — handing out the [`Period`](metering::Period) of a day, month or
//! year, the day an instant falls in, and the interval count over a period.
//!
//! That leaves one thing here, and it is a **storage** fact rather than a
//! calendar one: a stored row carries its `sparte`, so the store knows which
//! boundary applies to it. [`day_boundary`] is that mapping, written once — a
//! `match sparte` per function would be one rule in five places to drift.
//! Everything else here is a two-line delegation, for callers holding a `Sparte`
//! rather than a `DayBoundary`.
//!
//! # The Gastag
//!
//! Electricity, heat and water balance on the Berlin **calendar day**: 00:00 to
//! 00:00 local. Gas does not: the German gas market balances on the **Gastag**,
//! 06:00 to 06:00 local (GaBi Gas, following the EU-wide definition of
//! `[VO (EU) 2017/459 Art. 3 Nr. 16]`, and the span over which the
//! BDEW/VKU/GEODE Leitfaden *SLP Gas* forms its daily mean temperatures).
//!
//! Grouping a gas Lastgang by the calendar day is the same class of error as
//! grouping an electricity one by the UTC day — it books the 00:00–06:00 draw
//! into the wrong Bilanzierungstag, six hours a day, every day, and the totals
//! still look plausible. It also misplaces the DST anomaly: the clocks change at
//! 02:00/03:00 local, *before* the 06:00 boundary, so the 23- and 25-hour gas
//! days are the ones named after the **Saturday**, not the transition Sunday. A
//! completeness check using calendar days would report the Sunday four intervals
//! short and the Saturday four in surplus, and both would be wrong.
//!
//! # And the Bilanzierungsmonat
//!
//! The boundary carries **up to the month**, which is the market's own rule
//! rather than an extrapolation — EDI@Energy *Allgemeine Festlegungen* v6.1c,
//! Kap. 3.1 spells the Bilanzierungsmonat Juni 2021 out as 01.06 00:00 to 01.07
//! 00:00 for Strom and 01.06 **06:00** to 01.07 **06:00** for Gas. That is what
//! [`balancing_month`] returns, and it is the month an MSCONS version scope is
//! keyed to — see [`VersionScope`](crate::version::VersionScope).
//! [`balancing_month_bounds`] gives it as a half-open UTC range, and
//! [`bilanzierungsmonat`] that range addressed by name — *"Juni 2026"*.
//!
//! [`DayBoundary`]: metering::time::calendar::DayBoundary

use metering::series::interval::Sparte;
use metering::{DayBoundary, Resolution};
use time::{Date, Duration, Month, OffsetDateTime};

/// The daily boundary a commodity is balanced on.
///
/// **The one rule this module owns.** `metering` supplies both boundaries and
/// everything computed from them; which of the two applies to a given row is a
/// fact about the row's `sparte`, and a stored row carries one.
///
/// ```rust
/// use meterstore::planner::day_boundary;
/// use metering::{DayBoundary, Sparte};
///
/// assert_eq!(day_boundary(Sparte::Gas), DayBoundary::Gas);
/// for other in [Sparte::Strom, Sparte::Waerme, Sparte::Wasser] {
///     assert_eq!(day_boundary(other), DayBoundary::Strom);
/// }
/// ```
#[must_use]
pub const fn day_boundary(sparte: Sparte) -> DayBoundary {
    match sparte {
        Sparte::Gas => DayBoundary::Gas,
        _ => DayBoundary::Strom,
    }
}

/// The day `instant` is balanced on, for a reading of commodity `sparte`.
///
/// The Gastag for [`Sparte::Gas`], the Berlin calendar day for everything else.
/// This is the grouping key every daily aggregate wants, the value the encoder
/// stores in `balancing_day`, and the one `meter_balancing_day` exposes to SQL.
///
/// ```rust
/// use meterstore::planner::balancing_day;
/// use metering::series::interval::Sparte;
/// use time::macros::{date, datetime};
///
/// // 03:00 UTC on 15 July is 05:00 local — before the 06:00 gas boundary.
/// let at = datetime!(2026-07-15 3:00 UTC);
/// assert_eq!(balancing_day(at, Sparte::Strom), Some(date!(2026 - 07 - 15)));
/// assert_eq!(balancing_day(at, Sparte::Gas), Some(date!(2026 - 07 - 14)));
/// ```
///
/// `None` outside the years `metering`'s calendar covers (1900–9998): there is
/// no tz rule to answer from, and a guessed day is a wrong one.
#[must_use]
pub fn balancing_day(instant: OffsetDateTime, sparte: Sparte) -> Option<Date> {
    day_boundary(sparte).day_of(instant)
}

/// The **Bilanzierungsmonat** `instant` falls in, as its first day.
///
/// A gas month runs 06:00 on the first to 06:00 on the first of the next, so it
/// is a whole number of Gastage rather than a calendar month shifted. An
/// interval at 02:00 local on 1 March therefore belongs to *February* for gas
/// and to March for everything else.
///
/// This is the month an MSCONS correction version is scoped to, which is why it
/// is here rather than left to the caller: getting it wrong makes a
/// correctly-scoped gas delivery fail at the write.
///
/// ```rust
/// use meterstore::planner::balancing_month;
/// use metering::series::interval::Sparte;
/// use time::macros::{date, datetime};
///
/// // 01:00 UTC on 1 March is 02:00 local: already March, but still the
/// // February Gastag — and so the February Bilanzierungsmonat.
/// let at = datetime!(2026-03-01 1:00 UTC);
/// assert_eq!(balancing_month(at, Sparte::Strom), Some(date!(2026 - 03 - 01)));
/// assert_eq!(balancing_month(at, Sparte::Gas), Some(date!(2026 - 02 - 01)));
/// ```
#[must_use]
pub fn balancing_month(instant: OffsetDateTime, sparte: Sparte) -> Option<Date> {
    balancing_day(instant, sparte)?.replace_day(1).ok()
}

/// The half-open UTC bounds `[start, end)` of a balancing day.
///
/// Consecutive days tile the timeline with no gap and no overlap, for both
/// definitions and across both DST transitions — which is what lets a
/// completeness roll-up clip a partly covered day to the fraction inside the
/// queried range.
#[must_use]
pub fn balancing_day_bounds(day: Date, sparte: Sparte) -> Option<(OffsetDateTime, OffsetDateTime)> {
    Some(day_boundary(sparte).day(day)?.range())
}

/// The half-open UTC bounds `[start, end)` of a **balancing month**.
///
/// [`balancing_day_bounds`] one period up: the month containing `day`, cut on
/// the boundary the commodity is balanced on. A gas month is a whole number of
/// Gastage — 01.06 06:00 to 01.07 06:00 — rather than a calendar month shifted,
/// and neither is a fixed number of hours, because a month may contain a DST
/// transition.
///
/// The range a settlement rerun or a completeness report over a settlement
/// period is stated in.
///
/// ```rust
/// use meterstore::planner::balancing_month_bounds;
/// use metering::series::interval::Sparte;
/// use time::macros::{date, datetime};
///
/// let (from, to) = balancing_month_bounds(date!(2026 - 06 - 15), Sparte::Strom).unwrap();
/// assert_eq!(from, datetime!(2026-05-31 22:00 UTC));
/// assert_eq!(to, datetime!(2026-06-30 22:00 UTC));
///
/// // The same span, six hours later.
/// let (gas_from, gas_to) = balancing_month_bounds(date!(2026 - 06 - 15), Sparte::Gas).unwrap();
/// assert_eq!(gas_from, datetime!(2026-06-01 4:00 UTC));
/// assert_eq!(gas_to, datetime!(2026-07-01 4:00 UTC));
/// ```
#[must_use]
pub fn balancing_month_bounds(
    day: Date,
    sparte: Sparte,
) -> Option<(OffsetDateTime, OffsetDateTime)> {
    bilanzierungsmonat(day.year(), day.month(), sparte)
}

/// The **Bilanzierungsmonat** the market names — *"Juni 2026"* — as a half-open
/// UTC range.
///
/// [`balancing_month_bounds`] addressed by name rather than by a date inside it.
/// Both boundaries are EDI@Energy *Allgemeine Festlegungen* v6.1c, Kap. 3.1:
/// Strom runs 00:00 to 00:00 and Gas 06:00 to 06:00.
///
/// ```rust
/// use meterstore::planner::bilanzierungsmonat;
/// use metering::series::interval::Sparte;
/// use time::Month;
/// use time::macros::datetime;
///
/// // March contains the spring-forward Sunday, so the month is an hour short
/// // of 31 days — for both commodities.
/// let (from, to) = bilanzierungsmonat(2026, Month::March, Sparte::Strom).unwrap();
/// assert_eq!(from, datetime!(2026-02-28 23:00 UTC));
/// assert_eq!((to - from).whole_hours(), 31 * 24 - 1);
///
/// let (gas_from, gas_to) = bilanzierungsmonat(2026, Month::March, Sparte::Gas).unwrap();
/// assert_eq!(gas_from, datetime!(2026-03-01 5:00 UTC));
/// assert_eq!((gas_to - gas_from).whole_hours(), 31 * 24 - 1);
/// ```
#[must_use]
pub fn bilanzierungsmonat(
    year: i32,
    month: Month,
    sparte: Sparte,
) -> Option<(OffsetDateTime, OffsetDateTime)> {
    Some(day_boundary(sparte).month(year, month)?.range())
}

/// How long a balancing day is — 23, 24 or 25 hours.
#[must_use]
pub fn balancing_day_length(day: Date, sparte: Sparte) -> Option<Duration> {
    Some(day_boundary(sparte).day(day)?.duration())
}

/// How many intervals of `resolution` a balancing day holds, for `sparte`.
///
/// `metering`'s [`Period::count`](metering::Period::count) over the balancing
/// day. `None` for a resolution coarser than a day (`P1M`, `P1Y`), which has no
/// fixed count within one — the honest answer, rather than an invented 96 — and
/// for a day outside the calendar's years.
///
/// ```rust
/// use meterstore::planner::expected_intervals_in_balancing_day as intervals;
/// use metering::Resolution;
/// use metering::series::interval::Sparte;
/// use time::macros::date;
///
/// let q = Resolution::QUARTER_HOUR;
/// assert_eq!(intervals(date!(2026 - 07 - 15), q, Sparte::Strom), Some(96));
///
/// // The 25-hour day is the Sunday for the calendar and the **Saturday** for
/// // gas: the clocks move at 03:00, inside the Gastag that began the morning
/// // before.
/// assert_eq!(intervals(date!(2026 - 10 - 25), q, Sparte::Strom), Some(100));
/// assert_eq!(intervals(date!(2026 - 10 - 24), q, Sparte::Gas), Some(100));
/// assert_eq!(intervals(date!(2026 - 10 - 25), q, Sparte::Gas), Some(96));
///
/// // A daily series expects one interval per day, gas or not; a monthly one has
/// // no count within a day at all.
/// assert_eq!(intervals(date!(2026 - 07 - 15), Resolution::Day, Sparte::Gas), Some(1));
/// assert_eq!(intervals(date!(2026 - 07 - 15), Resolution::Month, Sparte::Gas), None);
/// ```
#[must_use]
pub fn expected_intervals_in_balancing_day(
    day: Date,
    resolution: Resolution,
    sparte: Sparte,
) -> Option<u32> {
    day_boundary(sparte).day(day)?.count(resolution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::{date, datetime};

    #[test]
    fn the_only_rule_this_module_owns_is_the_sparte_mapping() {
        // Everything else delegates, so this is the one thing worth asserting
        // directly: gas is the exception and the other three are not.
        assert_eq!(day_boundary(Sparte::Gas), DayBoundary::Gas);
        for other in [Sparte::Strom, Sparte::Waerme, Sparte::Wasser] {
            assert_eq!(day_boundary(other), DayBoundary::Strom);
        }
    }

    #[test]
    fn the_gas_day_starts_six_hours_late() {
        // 05:00 local on 15 July is still the Gastag of the 14th; 07:00 is the
        // 15th. The calendar day never moves.
        let before = datetime!(2026-07-15 3:00 UTC); // 05:00 local
        let after = datetime!(2026-07-15 5:00 UTC); // 07:00 local

        assert_eq!(
            balancing_day(before, Sparte::Gas),
            Some(date!(2026 - 07 - 14))
        );
        assert_eq!(
            balancing_day(after, Sparte::Gas),
            Some(date!(2026 - 07 - 15))
        );
        for at in [before, after] {
            assert_eq!(
                balancing_day(at, Sparte::Strom),
                Some(date!(2026 - 07 - 15))
            );
        }
    }

    #[test]
    fn the_bilanzierungsmonat_carries_the_boundary_up() {
        // EDI@Energy Allgemeine Festlegungen v6.1c Kap. 3.1: the gas
        // Bilanzierungsmonat runs 06:00 on the first to 06:00 on the first of
        // the next, so the first six hours of a calendar month belong to the
        // previous one.
        let early = datetime!(2026-03-01 1:00 UTC); // 02:00 local, 1 March
        assert_eq!(
            balancing_month(early, Sparte::Strom),
            Some(date!(2026 - 03 - 01))
        );
        assert_eq!(
            balancing_month(early, Sparte::Gas),
            Some(date!(2026 - 02 - 01))
        );

        // And once past 06:00 local the two agree again.
        let later = datetime!(2026-03-01 6:00 UTC); // 07:00 local
        for sparte in [Sparte::Strom, Sparte::Gas] {
            assert_eq!(balancing_month(later, sparte), Some(date!(2026 - 03 - 01)));
        }
    }

    #[test]
    fn a_balancing_month_is_the_month_of_the_balancing_day() {
        // The property the delegation rests on, over a whole year of instants.
        let mut at = datetime!(2026-01-01 00:00 UTC);
        let end = datetime!(2027-01-01 00:00 UTC);
        while at < end {
            for sparte in [Sparte::Strom, Sparte::Gas] {
                let day = balancing_day(at, sparte).unwrap();
                let month = balancing_month(at, sparte).unwrap();
                assert_eq!(month.year(), day.year(), "{at} {sparte}");
                assert_eq!(month.month(), day.month(), "{at} {sparte}");
                assert_eq!(month.day(), 1, "{at} {sparte}");
            }
            at += Duration::hours(7);
        }
    }

    #[test]
    fn consecutive_balancing_days_tile_the_timeline() {
        // No gap and no overlap, across both transitions, for both boundaries —
        // which is what lets completeness clip a partly covered day.
        for sparte in [Sparte::Strom, Sparte::Gas] {
            for start in [date!(2026 - 03 - 28), date!(2026 - 10 - 23)] {
                let mut day = start;
                for _ in 0..4 {
                    let (_, end) = balancing_day_bounds(day, sparte).unwrap();
                    let next = day.next_day().expect("in range");
                    let (next_start, _) = balancing_day_bounds(next, sparte).unwrap();
                    assert_eq!(end, next_start, "{day} -> {next} ({sparte})");
                    day = next;
                }
            }
        }
    }

    #[test]
    fn the_dst_anomaly_sits_on_a_different_named_day_for_gas() {
        // The clocks change at 02:00/03:00 local, before the 06:00 boundary, so
        // the long and short Gastage are named after the Saturday while the
        // calendar ones are named after the Sunday. Getting this backwards would
        // report one day four short and its neighbour four in surplus.
        let q = Resolution::QUARTER_HOUR;

        assert_eq!(
            expected_intervals_in_balancing_day(date!(2026 - 10 - 24), q, Sparte::Gas),
            Some(100)
        );
        assert_eq!(
            expected_intervals_in_balancing_day(date!(2026 - 10 - 25), q, Sparte::Gas),
            Some(96)
        );
        assert_eq!(
            expected_intervals_in_balancing_day(date!(2026 - 10 - 25), q, Sparte::Strom),
            Some(100)
        );

        assert_eq!(
            expected_intervals_in_balancing_day(date!(2026 - 03 - 28), q, Sparte::Gas),
            Some(92)
        );
        assert_eq!(
            expected_intervals_in_balancing_day(date!(2026 - 03 - 29), q, Sparte::Gas),
            Some(96)
        );
        assert_eq!(
            expected_intervals_in_balancing_day(date!(2026 - 03 - 29), q, Sparte::Strom),
            Some(92)
        );
    }

    #[test]
    fn a_daily_resolution_expects_one_interval_on_both_boundaries() {
        // A count derived from the resolution's fixed length instead would
        // report a daily gas series unmeasurable — `P1D` has no fixed length —
        // while a daily electricity one expected 1.
        for sparte in [Sparte::Strom, Sparte::Gas] {
            assert_eq!(
                expected_intervals_in_balancing_day(date!(2026 - 07 - 15), Resolution::Day, sparte),
                Some(1),
                "{sparte}"
            );
            // A resolution coarser than a day still has no count within one.
            for coarse in [Resolution::Month, Resolution::Year] {
                assert_eq!(
                    expected_intervals_in_balancing_day(date!(2026 - 07 - 15), coarse, sparte),
                    None,
                    "{sparte} {coarse:?}"
                );
            }
        }
    }

    #[test]
    fn sub_quarter_hour_gas_resolutions_scale_with_the_day() {
        // Not only the quarter-hour: a one-minute gas profile expects 1 500
        // intervals on the long Gastag, and 1 440 on an ordinary one.
        let minute = Resolution::from_seconds(60).expect("a minute is a resolution");
        assert_eq!(
            expected_intervals_in_balancing_day(date!(2026 - 07 - 15), minute, Sparte::Gas),
            Some(1_440)
        );
        assert_eq!(
            expected_intervals_in_balancing_day(date!(2026 - 10 - 24), minute, Sparte::Gas),
            Some(1_500)
        );
    }

    #[test]
    fn day_length_and_bounds_agree() {
        for sparte in [Sparte::Strom, Sparte::Gas] {
            for day in [
                date!(2026 - 07 - 15),
                date!(2026 - 03 - 28),
                date!(2026 - 10 - 24),
            ] {
                let (start, end) = balancing_day_bounds(day, sparte).unwrap();
                assert_eq!(balancing_day_length(day, sparte), Some(end - start));
            }
        }
    }
}
