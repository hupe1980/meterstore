//! Which day a reading is balanced on, and how many intervals that day holds.
//!
//! # Why this module exists at all
//!
//! MeterStore does not implement calendar arithmetic (P5) — `metering::calendar`
//! owns the `Europe/Berlin` conversion, the DST-aware day lengths and the
//! interval counts, and there is exactly one implementation of each. Everything
//! here **composes** those primitives; nothing here re-derives one.
//!
//! What is genuinely MeterStore's to decide is a storage question rather than a
//! calendar one: a row carries a `sparte`, so the store knows *which* of
//! `metering`'s two day definitions applies to it, and a query over a mixed
//! table should not have to be written twice.
//!
//! # The Gastag
//!
//! Electricity, heat and water balance on the Berlin **calendar day**: 00:00 to
//! 00:00 local. Gas balances on the **Gastag**: 06:00 to 06:00 local (GaBi Gas,
//! following the EU-wide convention of Art. 3 Nr. 6 VO (EU) 312/2014, and the
//! span over which the BDEW/VKU/GEODE Leitfaden *SLP Gas* forms its daily mean
//! temperatures).
//!
//! Grouping a gas Lastgang by the calendar day is the same class of error as
//! grouping an electricity one by the UTC day — it books the 00:00–06:00 draw
//! into the wrong Bilanzierungstag, six hours a day, every day, and the totals
//! still look plausible. It also misplaces the DST anomaly: the clocks change at
//! 02:00/03:00 local, *before* the 06:00 boundary, so the 23- and 25-hour gas
//! days are the ones named after the **Saturday**, not the transition Sunday. A
//! completeness check using calendar days would report the Sunday four intervals
//! short and the Saturday four in surplus, and both would be wrong.

use metering::IntervalResolution;
use metering::calendar;
use metering::interval::Sparte;
use time::{Date, Duration, OffsetDateTime};

/// The day `instant` is balanced on, for a reading of commodity `sparte`.
///
/// The Gastag for [`Sparte::Gas`], the Berlin calendar day for everything else.
/// This is the grouping key every daily aggregate wants, and the one
/// `meter_balancing_day` exposes to SQL.
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
    match sparte {
        Sparte::Gas => calendar::local_gas_day(instant),
        _ => calendar::local_day(instant),
    }
}

/// The half-open UTC bounds `[start, end)` of a balancing day.
///
/// Consecutive days tile the timeline with no gap and no overlap, for both
/// definitions and across both DST transitions — which is what lets a
/// completeness roll-up clip a partly covered day to the fraction inside the
/// queried range.
#[must_use]
pub fn balancing_day_bounds(day: Date, sparte: Sparte) -> (OffsetDateTime, OffsetDateTime) {
    match sparte {
        Sparte::Gas => (
            calendar::gas_day_start_utc(day),
            calendar::gas_day_end_utc(day),
        ),
        _ => (calendar::day_start_utc(day), calendar::day_end_utc(day)),
    }
}

/// How long a balancing day is — 23, 24 or 25 hours.
#[must_use]
pub fn balancing_day_length(day: Date, sparte: Sparte) -> Duration {
    let (start, end) = balancing_day_bounds(day, sparte);
    end - start
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
    calendar::gas_day_end_utc(day) - calendar::gas_day_start_utc(day)
}

/// How many intervals of `resolution` a Gastag holds.
///
/// The gas counterpart of [`metering::calendar::intervals_in_day`], and derived
/// the same way: the day's actual length divided by the resolution's fixed
/// length. `None` for a calendar resolution (`P1D`, `P1M`, `P1Y`), which has no
/// fixed count within a day — the honest answer, rather than an invented 96.
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
/// assert_eq!(intervals_in_gas_day(date!(2026 - 07 - 15), IntervalResolution::Day), None);
/// ```
#[must_use]
pub fn intervals_in_gas_day(day: Date, resolution: IntervalResolution) -> Option<u32> {
    let step = i64::from(resolution.fixed_seconds()?);
    // `fixed_seconds` returns `None` for `Custom(0)`, so the divisor is positive.
    u32::try_from(gas_day_length(day).whole_seconds() / step).ok()
}

/// How many intervals of `resolution` a balancing day holds, for `sparte`.
///
/// [`metering::calendar::intervals_in_day`] for every commodity but gas, and
/// [`intervals_in_gas_day`] for gas.
#[must_use]
pub fn expected_intervals_in_balancing_day(
    day: Date,
    resolution: IntervalResolution,
    sparte: Sparte,
) -> Option<u32> {
    match sparte {
        Sparte::Gas => intervals_in_gas_day(day, resolution),
        _ => calendar::intervals_in_day(day, resolution),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::{date, datetime};

    #[test]
    fn gas_reads_before_0600_belong_to_the_previous_gastag() {
        // The whole point: a reading at 00:15 local on the 15th is gas day 14.
        // Booking it on the 15th moves six hours of draw into the wrong
        // Bilanzierungstag, every day.
        let just_after_midnight = datetime!(2026-07-14 22:15 UTC); // 00:15 local, 15 July
        assert_eq!(
            balancing_day(just_after_midnight, Sparte::Strom),
            date!(2026 - 07 - 15)
        );
        assert_eq!(
            balancing_day(just_after_midnight, Sparte::Gas),
            date!(2026 - 07 - 14)
        );
    }

    #[test]
    fn heat_and_water_follow_the_calendar_day() {
        // Only gas is balanced on the 06:00 day; a blanket "not electricity"
        // rule would silently shift Wärme and Wasser too.
        let at = datetime!(2026-07-15 3:00 UTC); // 05:00 local
        for sparte in [Sparte::Strom, Sparte::Waerme, Sparte::Wasser] {
            assert_eq!(balancing_day(at, sparte), date!(2026 - 07 - 15), "{sparte}");
        }
        assert_eq!(balancing_day(at, Sparte::Gas), date!(2026 - 07 - 14));
    }

    #[test]
    fn the_dst_anomaly_lands_on_saturdays_gastag() {
        let q = IntervalResolution::QuarterHour;

        // Autumn 2026: the clocks go back at 03:00 local on Sunday the 25th,
        // which is inside the gas day that began Saturday the 24th at 06:00.
        assert_eq!(intervals_in_gas_day(date!(2026 - 10 - 24), q), Some(100));
        assert_eq!(intervals_in_gas_day(date!(2026 - 10 - 25), q), Some(96));
        // The calendar day puts it the other way round — which is exactly the
        // discrepancy that makes using the wrong one detectable.
        assert_eq!(
            calendar::intervals_in_day(date!(2026 - 10 - 24), q),
            Some(96)
        );
        assert_eq!(
            calendar::intervals_in_day(date!(2026 - 10 - 25), q),
            Some(100)
        );

        // Spring 2026: the short day is Saturday the 28th.
        assert_eq!(intervals_in_gas_day(date!(2026 - 03 - 28), q), Some(92));
        assert_eq!(intervals_in_gas_day(date!(2026 - 03 - 29), q), Some(96));
    }

    #[test]
    fn gas_days_tile_the_timeline_across_both_transitions() {
        // A completeness roll-up sums per day, so a gap or overlap between
        // consecutive days would double-count or lose a reading at the seam.
        for (mut day, last) in [
            (date!(2026 - 03 - 26), date!(2026 - 04 - 01)),
            (date!(2026 - 10 - 22), date!(2026 - 10 - 28)),
        ] {
            let mut cursor = balancing_day_bounds(day, Sparte::Gas).0;
            while day < last {
                let (start, end) = balancing_day_bounds(day, Sparte::Gas);
                assert_eq!(start, cursor, "gap or overlap before {day}");
                cursor = end;
                day = day.next_day().unwrap();
            }
        }
    }

    #[test]
    fn every_instant_lies_inside_the_day_it_is_attributed_to() {
        // The property that makes `balancing_day` and `balancing_day_bounds`
        // usable together: bucketing by one and clipping by the other must agree.
        let mut at = datetime!(2026-10-24 0:00 UTC);
        while at < datetime!(2026-10-27 0:00 UTC) {
            for sparte in [Sparte::Strom, Sparte::Gas] {
                let day = balancing_day(at, sparte);
                let (start, end) = balancing_day_bounds(day, sparte);
                assert!(
                    start <= at && at < end,
                    "{at} not inside {day} for {sparte}"
                );
            }
            at += Duration::minutes(15);
        }
    }

    #[test]
    fn a_calendar_resolution_has_no_count_within_a_gas_day() {
        for res in [
            IntervalResolution::Day,
            IntervalResolution::Month,
            IntervalResolution::Year,
        ] {
            assert_eq!(intervals_in_gas_day(date!(2026 - 07 - 15), res), None);
            assert_eq!(
                expected_intervals_in_balancing_day(date!(2026 - 07 - 15), res, Sparte::Gas),
                None
            );
        }
    }

    #[test]
    fn sub_quarter_hour_gas_resolutions_scale_with_the_day() {
        // Not only the quarter-hour: a one-minute gas profile expects 1 500
        // intervals on the long Gastag, and 1 440 on an ordinary one.
        let minute = IntervalResolution::Custom(60);
        assert_eq!(
            intervals_in_gas_day(date!(2026 - 07 - 15), minute),
            Some(1_440)
        );
        assert_eq!(
            intervals_in_gas_day(date!(2026 - 10 - 24), minute),
            Some(1_500)
        );
    }
}
