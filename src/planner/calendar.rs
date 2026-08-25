//! Which day a reading is balanced on, and how many intervals that day holds.
//!
//! # What this module decides
//!
//! MeterStore implements neither the calendar arithmetic (P5) nor the *choice* of
//! calendar. `metering::calendar` owns both, as [`DayBoundary`] — `Midnight` or
//! `Gastag` — carrying the whole period API: day start and end, the day an
//! instant falls in, day length, interval count, and the month and year
//! equivalents.
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
//! 06:00 to 06:00 local (GaBi Gas, following the EU-wide convention of Art. 3
//! Nr. 6 VO (EU) 312/2014, and the span over which the BDEW/VKU/GEODE Leitfaden
//! *SLP Gas* forms its daily mean temperatures).
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
//!
//! [`DayBoundary`]: metering::calendar::DayBoundary

use metering::IntervalResolution;
use metering::calendar::DayBoundary;
use metering::interval::Sparte;
use time::{Date, Duration, OffsetDateTime};

/// The daily boundary a commodity is balanced on.
///
/// **The one rule this module owns.** `metering` supplies both boundaries and
/// everything computed from them; which of the two applies to a given row is a
/// fact about the row's `sparte`, and a stored row carries one.
///
/// ```rust
/// use meterstore::planner::day_boundary;
/// use metering::calendar::DayBoundary;
/// use metering::interval::Sparte;
///
/// assert_eq!(day_boundary(Sparte::Gas), DayBoundary::Gastag);
/// for other in [Sparte::Strom, Sparte::Waerme, Sparte::Wasser] {
///     assert_eq!(day_boundary(other), DayBoundary::Midnight);
/// }
/// ```
#[must_use]
pub const fn day_boundary(sparte: Sparte) -> DayBoundary {
    match sparte {
        Sparte::Gas => DayBoundary::Gastag,
        _ => DayBoundary::Midnight,
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
/// use metering::interval::Sparte;
/// use time::macros::{date, datetime};
///
/// // 03:00 UTC on 15 July is 05:00 local — before the 06:00 gas boundary.
/// let at = datetime!(2026-07-15 3:00 UTC);
/// assert_eq!(balancing_day(at, Sparte::Strom), date!(2026 - 07 - 15));
/// assert_eq!(balancing_day(at, Sparte::Gas), date!(2026 - 07 - 14));
/// ```
#[must_use]
pub fn balancing_day(instant: OffsetDateTime, sparte: Sparte) -> Date {
    day_boundary(sparte).local_day(instant)
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
/// use metering::interval::Sparte;
/// use time::macros::{date, datetime};
///
/// // 01:00 UTC on 1 March is 02:00 local: already March, but still the
/// // February Gastag — and so the February Bilanzierungsmonat.
/// let at = datetime!(2026-03-01 1:00 UTC);
/// assert_eq!(balancing_month(at, Sparte::Strom), date!(2026 - 03 - 01));
/// assert_eq!(balancing_month(at, Sparte::Gas), date!(2026 - 02 - 01));
/// ```
#[must_use]
pub fn balancing_month(instant: OffsetDateTime, sparte: Sparte) -> Date {
    day_boundary(sparte).local_month(instant)
}

/// The half-open UTC bounds `[start, end)` of a balancing day.
///
/// Consecutive days tile the timeline with no gap and no overlap, for both
/// definitions and across both DST transitions — which is what lets a
/// completeness roll-up clip a partly covered day to the fraction inside the
/// queried range.
#[must_use]
pub fn balancing_day_bounds(day: Date, sparte: Sparte) -> (OffsetDateTime, OffsetDateTime) {
    let boundary = day_boundary(sparte);
    (boundary.day_start_utc(day), boundary.day_end_utc(day))
}

/// How long a balancing day is — 23, 24 or 25 hours.
#[must_use]
pub fn balancing_day_length(day: Date, sparte: Sparte) -> Duration {
    day_boundary(sparte).day_length(day)
}

/// How long the Gastag `day` is — 23, 24 or 25 hours.
///
/// The long and short ones are named after the **Saturday**: the clocks move at
/// 02:00/03:00 local, which falls inside the gas day that began 06:00 the
/// previous morning.
///
/// ```rust
/// use meterstore::planner::gas_day_length;
/// use time::macros::date;
///
/// assert_eq!(gas_day_length(date!(2026 - 10 - 24)).whole_hours(), 25);
/// assert_eq!(gas_day_length(date!(2026 - 10 - 25)).whole_hours(), 24);
/// ```
#[must_use]
pub fn gas_day_length(day: Date) -> Duration {
    DayBoundary::Gastag.day_length(day)
}

/// How many intervals of `resolution` a Gastag holds.
///
/// [`metering::calendar::intervals_in_day`] under the Gastag boundary. `None` for
/// a resolution coarser than a day (`P1M`, `P1Y`), which has no fixed count
/// within one — the honest answer, rather than an invented 96.
///
/// ```rust
/// use meterstore::planner::intervals_in_gas_day;
/// use metering::IntervalResolution;
/// use time::macros::date;
///
/// let q = IntervalResolution::QuarterHour;
/// assert_eq!(intervals_in_gas_day(date!(2026 - 07 - 15), q), Some(96));
/// // The 25-hour Gastag is Saturday's, not the transition Sunday's.
/// assert_eq!(intervals_in_gas_day(date!(2026 - 10 - 24), q), Some(100));
/// assert_eq!(intervals_in_gas_day(date!(2026 - 10 - 25), q), Some(96));
/// // A daily series expects one interval per day, gas or not.
/// assert_eq!(intervals_in_gas_day(date!(2026 - 07 - 15), IntervalResolution::Day), Some(1));
/// assert_eq!(intervals_in_gas_day(date!(2026 - 07 - 15), IntervalResolution::Month), None);
/// ```
#[must_use]
pub fn intervals_in_gas_day(day: Date, resolution: IntervalResolution) -> Option<u32> {
    DayBoundary::Gastag.intervals_in_day(day, resolution)
}

/// How many intervals of `resolution` a balancing day holds, for `sparte`.
///
/// [`metering::calendar::intervals_in_day`] under the boundary the commodity is
/// balanced on.
#[must_use]
pub fn expected_intervals_in_balancing_day(
    day: Date,
    resolution: IntervalResolution,
    sparte: Sparte,
) -> Option<u32> {
    day_boundary(sparte).intervals_in_day(day, resolution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::{date, datetime};

    #[test]
    fn the_only_rule_this_module_owns_is_the_sparte_mapping() {
        // Everything else delegates, so this is the one thing worth asserting
        // directly: gas is the exception and the other three are not.
        assert_eq!(day_boundary(Sparte::Gas), DayBoundary::Gastag);
        for other in [Sparte::Strom, Sparte::Waerme, Sparte::Wasser] {
            assert_eq!(day_boundary(other), DayBoundary::Midnight);
        }
    }

    #[test]
    fn the_gas_day_starts_six_hours_late() {
        // 05:00 local on 15 July is still the Gastag of the 14th; 07:00 is the
        // 15th. The calendar day never moves.
        let before = datetime!(2026-07-15 3:00 UTC); // 05:00 local
        let after = datetime!(2026-07-15 5:00 UTC); // 07:00 local

        assert_eq!(balancing_day(before, Sparte::Gas), date!(2026 - 07 - 14));
        assert_eq!(balancing_day(after, Sparte::Gas), date!(2026 - 07 - 15));
        for at in [before, after] {
            assert_eq!(balancing_day(at, Sparte::Strom), date!(2026 - 07 - 15));
        }
    }

    #[test]
    fn the_bilanzierungsmonat_carries_the_boundary_up() {
        // EDI@Energy Allgemeine Festlegungen v6.1c Kap. 3.1: the gas
        // Bilanzierungsmonat runs 06:00 on the first to 06:00 on the first of
        // the next, so the first six hours of a calendar month belong to the
        // previous one.
        let early = datetime!(2026-03-01 1:00 UTC); // 02:00 local, 1 March
        assert_eq!(balancing_month(early, Sparte::Strom), date!(2026 - 03 - 01));
        assert_eq!(balancing_month(early, Sparte::Gas), date!(2026 - 02 - 01));

        // And once past 06:00 local the two agree again.
        let later = datetime!(2026-03-01 6:00 UTC); // 07:00 local
        for sparte in [Sparte::Strom, Sparte::Gas] {
            assert_eq!(balancing_month(later, sparte), date!(2026 - 03 - 01));
        }
    }

    #[test]
    fn a_balancing_month_is_the_month_of_the_balancing_day() {
        // The property the delegation rests on, over a whole year of instants.
        let mut at = datetime!(2026-01-01 00:00 UTC);
        let end = datetime!(2027-01-01 00:00 UTC);
        while at < end {
            for sparte in [Sparte::Strom, Sparte::Gas] {
                let day = balancing_day(at, sparte);
                let month = balancing_month(at, sparte);
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
                    let (_, end) = balancing_day_bounds(day, sparte);
                    let next = day.next_day().expect("in range");
                    let (next_start, _) = balancing_day_bounds(next, sparte);
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
        let q = IntervalResolution::QuarterHour;

        assert_eq!(intervals_in_gas_day(date!(2026 - 10 - 24), q), Some(100));
        assert_eq!(intervals_in_gas_day(date!(2026 - 10 - 25), q), Some(96));
        assert_eq!(
            expected_intervals_in_balancing_day(date!(2026 - 10 - 25), q, Sparte::Strom),
            Some(100)
        );

        assert_eq!(intervals_in_gas_day(date!(2026 - 03 - 28), q), Some(92));
        assert_eq!(intervals_in_gas_day(date!(2026 - 03 - 29), q), Some(96));
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
                expected_intervals_in_balancing_day(
                    date!(2026 - 07 - 15),
                    IntervalResolution::Day,
                    sparte
                ),
                Some(1),
                "{sparte}"
            );
            // A resolution coarser than a day still has no count within one.
            for coarse in [IntervalResolution::Month, IntervalResolution::Year] {
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
        let minute = IntervalResolution::from_seconds(60).expect("a minute is a resolution");
        assert_eq!(
            intervals_in_gas_day(date!(2026 - 07 - 15), minute),
            Some(1_440)
        );
        assert_eq!(
            intervals_in_gas_day(date!(2026 - 10 - 24), minute),
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
                let (start, end) = balancing_day_bounds(day, sparte);
                assert_eq!(balancing_day_length(day, sparte), end - start);
            }
        }
    }
}
