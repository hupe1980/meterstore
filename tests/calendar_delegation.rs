//! The DST properties MeterStore relies on, asserted against `metering`'s
//! calendar rather than a local reimplementation.
//!
//! These vectors previously guarded a downstream copy of this logic. The copy is
//! gone, but the properties still matter to completeness reporting, so they are
//! kept here as a contract test against the upstream crate.

use metering::IntervalResolution;
use metering::calendar::{DayKind, day_kind, day_length, intervals_in_day, local_day};
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
