//! The DST properties MeterStore relies on, asserted against `metering`'s
//! calendar rather than a local reimplementation.
//!
//! These vectors previously guarded a downstream copy of this logic. The copy is
//! gone, but the properties still matter to completeness reporting, so they are
//! kept here as a contract test against the upstream crate.

use metering::IntervalResolution;
use metering::calendar::{
    DayKind, day_kind, day_length, gas_day_end_utc, gas_day_start_utc, intervals_in_day, local_day,
    local_gas_day,
};
use metering::interval::Sparte;
use meterstore::planner::{
    balancing_day, balancing_day_length, balancing_month, bilanzierungsmonat,
    expected_intervals_in_balancing_day,
};
use time::Duration;
use time::macros::{date, datetime};

#[test]
fn a_normal_day_has_96_quarter_hours() {
    let day = date!(2026 - 07 - 20);
    assert_eq!(day_length(day), Duration::hours(24));
    assert_eq!(day_kind(day), DayKind::Normal);
    assert_eq!(
        intervals_in_day(day, IntervalResolution::QuarterHour),
        Some(96)
    );
}

#[test]
fn the_spring_forward_day_has_92_quarter_hours() {
    let day = date!(2026 - 03 - 29);
    assert_eq!(day_length(day), Duration::hours(23));
    assert_eq!(day_kind(day), DayKind::ShortDay);
    assert_eq!(
        intervals_in_day(day, IntervalResolution::QuarterHour),
        Some(92)
    );
}

#[test]
fn the_autumn_back_day_has_100_quarter_hours() {
    let day = date!(2026 - 10 - 25);
    assert_eq!(day_length(day), Duration::hours(25));
    assert_eq!(day_kind(day), DayKind::LongDay);
    assert_eq!(
        intervals_in_day(day, IntervalResolution::QuarterHour),
        Some(100)
    );
}

#[test]
fn transitions_hold_across_years() {
    // Sourced from the tz database, not hardcoded to one year.
    for (spring, autumn) in [
        (date!(2025 - 03 - 30), date!(2025 - 10 - 26)),
        (date!(2027 - 03 - 28), date!(2027 - 10 - 31)),
    ] {
        assert_eq!(day_kind(spring), DayKind::ShortDay);
        assert_eq!(day_kind(autumn), DayKind::LongDay);
    }
}

#[test]
fn the_utc_day_boundary_is_not_the_local_one() {
    // The bug that silently corrupts every daily sum: 23:00 UTC on the 20th is
    // already the 21st in Berlin, so grouping on UTC days is wrong year-round.
    let instant = datetime!(2026-07-20 23:00 UTC);
    assert_eq!(instant.date(), date!(2026 - 07 - 20), "UTC day");
    assert_eq!(local_day(instant), date!(2026 - 07 - 21), "Berlin day");
}

#[test]
fn a_march_of_quarter_hours_is_four_short() {
    let total: u32 = (1..=31)
        .map(|d| {
            let day = time::Date::from_calendar_date(2026, time::Month::March, d).unwrap();
            intervals_in_day(day, IntervalResolution::QuarterHour).unwrap()
        })
        .sum();
    assert_eq!(total, 2_976 - 4);
}

// ── the Gastag ───────────────────────────────────────────────────────────────
//
// Gas is balanced on a 06:00–06:00 day, so the properties above have a second
// set that MeterStore's completeness reporting and `meter_balancing_day` rely
// on. Same principle: asserted against `metering`, not against a copy.

#[test]
fn the_gas_day_runs_from_0600_to_0600_local() {
    // Winter: 06:00 CET is 05:00 UTC. Summer: 06:00 CEST is 04:00 UTC. The
    // boundary is a *local* time, so it moves with the offset — which is why it
    // cannot be written as a fixed UTC hour.
    assert_eq!(
        gas_day_start_utc(date!(2026 - 01 - 15)),
        datetime!(2026-01-15 5:00 UTC)
    );
    assert_eq!(
        gas_day_start_utc(date!(2026 - 07 - 15)),
        datetime!(2026-07-15 4:00 UTC)
    );
    assert_eq!(
        gas_day_end_utc(date!(2026 - 07 - 15)),
        gas_day_start_utc(date!(2026 - 07 - 16))
    );
}

#[test]
fn the_long_and_short_gas_days_are_named_after_the_saturday() {
    // The clocks change at 02:00/03:00 local, which lies *before* the 06:00 gas
    // boundary — so the 23- and 25-hour Gastage are the ones that began on the
    // Saturday, not the transition Sunday. This is the property that makes a
    // completeness check on calendar days wrong in both directions at once.
    assert_eq!(
        balancing_day_length(date!(2026 - 10 - 24), Sparte::Gas).whole_hours(),
        25
    );
    assert_eq!(
        balancing_day_length(date!(2026 - 10 - 25), Sparte::Gas).whole_hours(),
        24
    );
    assert_eq!(
        balancing_day_length(date!(2026 - 03 - 28), Sparte::Gas).whole_hours(),
        23
    );
    assert_eq!(
        balancing_day_length(date!(2026 - 03 - 29), Sparte::Gas).whole_hours(),
        24
    );

    // Mirror image of the calendar day, quarter-hour by quarter-hour.
    let q = IntervalResolution::QuarterHour;
    assert_eq!(
        expected_intervals_in_balancing_day(date!(2026 - 10 - 24), q, Sparte::Gas),
        Some(100)
    );
    assert_eq!(intervals_in_day(date!(2026 - 10 - 24), q), Some(96));
    assert_eq!(
        expected_intervals_in_balancing_day(date!(2026 - 10 - 25), q, Sparte::Gas),
        Some(96)
    );
    assert_eq!(intervals_in_day(date!(2026 - 10 - 25), q), Some(100));
}

#[test]
fn a_gas_reading_before_0600_belongs_to_the_previous_gastag() {
    // The six hours a day a calendar-day grouping books into the wrong
    // Bilanzierungstag, with totals that still look plausible.
    let just_after_local_midnight = datetime!(2026-07-14 22:15 UTC);
    assert_eq!(
        local_day(just_after_local_midnight),
        date!(2026 - 07 - 15),
        "calendar day"
    );
    assert_eq!(
        local_gas_day(just_after_local_midnight),
        date!(2026 - 07 - 14),
        "Gastag"
    );
}

#[test]
fn balancing_day_delegates_to_whichever_calendar_the_commodity_uses() {
    // MeterStore's own contribution is only the dispatch; both answers are
    // `metering`'s, and this asserts the wrapper did not invent a third.
    let at = datetime!(2026-07-14 22:15 UTC);
    assert_eq!(
        balancing_day(at, metering::interval::Sparte::Gas),
        local_gas_day(at)
    );
    for sparte in [
        metering::interval::Sparte::Strom,
        metering::interval::Sparte::Waerme,
        metering::interval::Sparte::Wasser,
    ] {
        assert_eq!(balancing_day(at, sparte), local_day(at), "{sparte}");
    }
}

#[test]
fn a_balancing_month_delegates_the_same_way_and_tiles_the_same_timeline() {
    // The month is the same dispatch as the day, and the two have to agree:
    // `date_trunc('month', balancing_day)` is what an external engine reading
    // the Iceberg files uses to reach the Bilanzierungsmonat, and that is only
    // right because a settlement month is a whole number of balancing days.
    let at = datetime!(2026-03-01 01:00 UTC); // 02:00 local — the March/February seam
    assert_eq!(
        balancing_month(at, Sparte::Gas),
        first_of(local_gas_day(at)),
        "a gas month is the month of the Gastag, not of the calendar day"
    );
    assert_eq!(balancing_month(at, Sparte::Strom), first_of(local_day(at)));

    // Consecutive months tile with no gap and no overlap, for both boundaries
    // and across both transitions — the property a settlement rerun over
    // `bilanzierungsmonat` rests on.
    for sparte in [Sparte::Strom, Sparte::Gas] {
        let mut previous_end = None;
        for month in 1u8..=12 {
            let month = time::Month::try_from(month).unwrap();
            let (from, to) = bilanzierungsmonat(2026, month, sparte);
            assert!(from < to, "{sparte} {month:?}");
            if let Some(end) = previous_end {
                assert_eq!(from, end, "{sparte} {month:?} does not abut the previous");
            }
            previous_end = Some(to);
        }
        // And the twelve of them are exactly the year.
        let (year_from, _) = bilanzierungsmonat(2026, time::Month::January, sparte);
        assert_eq!(
            previous_end,
            Some(bilanzierungsmonat(2027, time::Month::January, sparte).0),
            "{sparte}"
        );
        assert_eq!(
            (previous_end.unwrap() - year_from).whole_hours(),
            365 * 24,
            "{sparte}: the two DST transitions cancel over a whole year"
        );
    }
}

/// The first of the month a date falls in.
fn first_of(day: time::Date) -> time::Date {
    day.replace_day(1).expect("the first is always valid")
}

#[test]
fn a_march_of_gas_days_is_also_four_short() {
    // The month total is the same either way — the transition moves *which* day
    // is long, not how much time March holds. A check that only ever compares
    // monthly totals would therefore never notice the difference, which is why
    // the per-day assertions above are the ones that matter.
    let total: u32 = (1..=31)
        .map(|d| {
            let day = time::Date::from_calendar_date(2026, time::Month::March, d).unwrap();
            expected_intervals_in_balancing_day(day, IntervalResolution::QuarterHour, Sparte::Gas)
                .unwrap()
        })
        .sum();
    assert_eq!(total, 2_976 - 4);
}
