//! The tiering watermark — the boundary between hot and cold storage.
//!
//! One timestamp per table:
//!
//! ```text
//!   │◀──── Iceberg (cold, settled) ────▶│◀── Postgres (hot) ──▶│
//!   epoch                       tiering_watermark            now
//! ```
//!
//! The invariant is that a row's `from` alone decides its tier. This type
//! exists so that decision is made in one place and can be asserted, rather than
//! being re-derived by every caller that builds a predicate.

use std::fmt;

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::error::{Error, Result};

/// The Iceberg snapshot summary property the watermark is stored in.
///
/// Storing it *inside* the snapshot is what makes the advance atomic with the
/// data it describes: the Iceberg commit is a compare-and-swap, so there
/// is no state in which the rows are durable but the watermark is not.
pub const WATERMARK_PROPERTY: &str = "meterstore.tiering_watermark";

/// The snapshot summary property recording the archived interval range.
pub const ARCHIVED_RANGE_PROPERTY: &str = "meterstore.archived_range";

/// The snapshot summary property recording the archived row count.
pub const ROW_COUNT_PROPERTY: &str = "meterstore.row_count";

/// Which tier a given interval belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    /// Iceberg. `from < watermark`.
    Cold,
    /// PostgreSQL. `from >= watermark`.
    Hot,
}

/// The hot/cold boundary for one table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TieringWatermark(OffsetDateTime);

impl TieringWatermark {
    /// A watermark at the Unix epoch — nothing has been archived yet.
    pub const fn empty() -> Self {
        Self(OffsetDateTime::UNIX_EPOCH)
    }

    /// Construct a watermark at the given instant.
    pub const fn new(at: OffsetDateTime) -> Self {
        Self(at)
    }

    /// The boundary instant.
    pub const fn get(self) -> OffsetDateTime {
        self.0
    }

    /// Which tier an interval starting at `from` belongs to.
    ///
    /// This is the entire routing rule, and the only place it is written.
    pub fn tier_for(self, from: OffsetDateTime) -> Tier {
        if from < self.0 { Tier::Cold } else { Tier::Hot }
    }

    /// Whether advancing to `next` is legal.
    ///
    /// The watermark is monotonic: moving it backwards would place
    /// already-archived intervals back in the hot tier, where they do not exist.
    pub fn can_advance_to(self, next: Self) -> bool {
        next.0 >= self.0
    }

    /// Advance the watermark, rejecting a backwards move.
    pub fn advance_to(self, next: Self) -> Result<Self> {
        if !self.can_advance_to(next) {
            return Err(Error::InvariantViolated {
                table: "<unknown>".to_string(),
                detail: format!("watermark would move backwards: {self} -> {next}"),
            });
        }
        Ok(next)
    }

    /// Serialize for the Iceberg snapshot summary.
    pub fn to_property(self) -> Result<String> {
        self.0
            .format(&Rfc3339)
            .map_err(|e| Error::encode(WATERMARK_PROPERTY, e.to_string()))
    }

    /// Parse from the Iceberg snapshot summary.
    pub fn from_property(value: &str) -> Result<Self> {
        OffsetDateTime::parse(value, &Rfc3339)
            .map(Self)
            .map_err(|e| Error::decode(WATERMARK_PROPERTY, format!("{value:?}: {e}")))
    }
}

impl fmt::Display for TieringWatermark {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.format(&Rfc3339) {
            Ok(s) => f.write_str(&s),
            Err(_) => write!(f, "{:?}", self.0),
        }
    }
}

/// A half-open interval range `[from, to)` selected for archival.
///
/// Always a *closed* window: archival never runs up to `now()`, because late
/// data for the current window would land below the advanced watermark, where
/// no query would look for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchivalWindow {
    from: OffsetDateTime,
    to: OffsetDateTime,
}

impl ArchivalWindow {
    /// Construct a window, rejecting empty or inverted ranges.
    pub fn new(from: OffsetDateTime, to: OffsetDateTime) -> Result<Self> {
        if to <= from {
            return Err(Error::config(format!(
                "archival window end {to} must be after start {from}"
            )));
        }
        Ok(Self { from, to })
    }

    /// Inclusive lower bound.
    pub const fn from(self) -> OffsetDateTime {
        self.from
    }

    /// Exclusive upper bound.
    pub const fn to(self) -> OffsetDateTime {
        self.to
    }

    /// The watermark this window advances to once committed.
    pub const fn resulting_watermark(self) -> TieringWatermark {
        TieringWatermark(self.to)
    }

    /// Whether an interval start falls inside this window.
    pub fn contains(self, from: OffsetDateTime) -> bool {
        from >= self.from && from < self.to
    }

    /// Format for the `archived_range` snapshot property.
    pub fn to_property(self) -> Result<String> {
        let f = |t: OffsetDateTime| {
            t.format(&Rfc3339)
                .map_err(|e| Error::encode(ARCHIVED_RANGE_PROPERTY, e.to_string()))
        };
        Ok(format!("{}/{}", f(self.from)?, f(self.to)?))
    }
}

/// Truncate an instant down to a multiple of `step`, measured from the Unix epoch.
///
/// Hot partition bounds and archival windows are aligned the same way, which is
/// what makes a window correspond to exactly one partition (§7.2). Written once
/// here rather than per tier, because two implementations that rounded
/// differently would produce windows no partition holds — and the symptom would
/// be an empty archival run, not an error.
///
/// `rem_euclid`, not `%`: a pre-epoch instant would otherwise round *up*.
pub fn align_to_step(ts: OffsetDateTime, step: time::Duration) -> OffsetDateTime {
    let secs = ts.unix_timestamp();
    let step_s = step.whole_seconds().max(1);
    OffsetDateTime::from_unix_timestamp(secs - secs.rem_euclid(step_s)).unwrap_or(ts)
}

/// Select the next archival window.
///
/// Returns `None` when the horizon has not yet advanced past the watermark by a
/// full step — the common steady-state case, where the job should do nothing
/// rather than archive a partial window.
///
/// * `watermark` — where cold currently ends.
/// * `now` — wall clock.
/// * `settlement_lag` — how far behind wall clock archival stays, so that late
///   corrections still land in the hot tier.
/// * `step` — window size, which must equal the hot table's partition step.
pub fn next_window(
    watermark: TieringWatermark,
    now: OffsetDateTime,
    settlement_lag: time::Duration,
    step: time::Duration,
) -> Result<Option<ArchivalWindow>> {
    if step <= time::Duration::ZERO {
        return Err(Error::config("archival step must be positive"));
    }
    if settlement_lag < time::Duration::ZERO {
        return Err(Error::config("settlement lag must not be negative"));
    }

    let from = watermark.get();

    // A window must correspond to exactly one hot partition (§7.2), and
    // partitions are created on `align_to_step` boundaries. A watermark that is
    // not on one produces windows whose `PartitionId` names a relation nothing
    // ever creates — so every window looks empty, the watermark walks straight
    // past rows that are still in PostgreSQL, and they are stranded below it
    // with nothing but `invariant_violations` to say so.
    //
    // The only way to get there is to change `partition_step` on a table that
    // has already archived. That is a migration, not a setting, and it fails
    // here rather than in the data.
    if align_to_step(from, step) != from {
        return Err(Error::config(format!(
            "watermark {from} is not aligned to an archival step of {} seconds, so an \
             archival window would not correspond to a hot partition. The step of a \
             table that has already archived cannot be changed in place: create a new \
             table at the new step",
            step.whole_seconds(),
        )));
    }

    let horizon = now - settlement_lag;
    let to = from + step;

    if to > horizon {
        return Ok(None);
    }
    Ok(Some(ArchivalWindow::new(from, to)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Duration;
    use time::macros::datetime;

    const DAY: Duration = Duration::DAY;

    fn wm(s: &str) -> TieringWatermark {
        TieringWatermark::from_property(s).unwrap()
    }

    #[test]
    fn tier_boundary_is_half_open() {
        // `from < watermark` is cold; `from >= watermark` is hot. The boundary
        // instant itself must be hot, or a row could belong to neither tier.
        let w = TieringWatermark::new(datetime!(2026-07-20 00:00 UTC));

        assert_eq!(w.tier_for(datetime!(2026-07-19 23:59:59 UTC)), Tier::Cold);
        assert_eq!(w.tier_for(datetime!(2026-07-20 00:00 UTC)), Tier::Hot);
        assert_eq!(w.tier_for(datetime!(2026-07-20 00:00:01 UTC)), Tier::Hot);
    }

    #[test]
    fn empty_watermark_puts_everything_in_the_hot_tier() {
        let w = TieringWatermark::empty();
        assert_eq!(w.tier_for(datetime!(1970-01-01 00:00 UTC)), Tier::Hot);
        assert_eq!(w.tier_for(datetime!(2026-07-20 00:00 UTC)), Tier::Hot);
    }

    #[test]
    fn watermark_is_monotonic() {
        let w = TieringWatermark::new(datetime!(2026-07-20 00:00 UTC));
        let back = TieringWatermark::new(datetime!(2026-07-19 00:00 UTC));
        let fwd = TieringWatermark::new(datetime!(2026-07-21 00:00 UTC));

        assert!(!w.can_advance_to(back));
        assert!(w.can_advance_to(fwd));
        assert!(w.can_advance_to(w), "idempotent re-advance is legal");

        assert!(w.advance_to(back).is_err());
        assert_eq!(w.advance_to(fwd).unwrap(), fwd);
    }

    #[test]
    fn watermark_round_trips_through_the_snapshot_property() {
        let w = TieringWatermark::new(datetime!(2026-07-20 12:34:56 UTC));
        assert_eq!(wm(&w.to_property().unwrap()), w);
    }

    #[test]
    fn watermark_rejects_malformed_property() {
        assert!(TieringWatermark::from_property("not-a-timestamp").is_err());
        assert!(TieringWatermark::from_property("").is_err());
    }

    #[test]
    fn window_rejects_empty_and_inverted_ranges() {
        let t = datetime!(2026-07-20 00:00 UTC);
        assert!(ArchivalWindow::new(t, t).is_err());
        assert!(ArchivalWindow::new(t, t - DAY).is_err());
        assert!(ArchivalWindow::new(t, t + DAY).is_ok());
    }

    #[test]
    fn window_containment_is_half_open() {
        let w = ArchivalWindow::new(
            datetime!(2026-07-20 00:00 UTC),
            datetime!(2026-07-21 00:00 UTC),
        )
        .unwrap();

        assert!(w.contains(datetime!(2026-07-20 00:00 UTC)));
        assert!(w.contains(datetime!(2026-07-20 23:59:59 UTC)));
        assert!(!w.contains(datetime!(2026-07-21 00:00 UTC)));
        assert!(!w.contains(datetime!(2026-07-19 23:59:59 UTC)));
    }

    #[test]
    fn window_resulting_watermark_is_its_exclusive_end() {
        // Guarantees the tiers stay disjoint and gapless across an advance.
        let w = ArchivalWindow::new(
            datetime!(2026-07-20 00:00 UTC),
            datetime!(2026-07-21 00:00 UTC),
        )
        .unwrap();
        let next = w.resulting_watermark();

        assert_eq!(next.get(), w.to());
        assert_eq!(next.tier_for(datetime!(2026-07-20 12:00 UTC)), Tier::Cold);
        assert_eq!(next.tier_for(datetime!(2026-07-21 00:00 UTC)), Tier::Hot);
    }

    #[test]
    fn next_window_waits_until_a_full_step_is_below_the_horizon() {
        let w = TieringWatermark::new(datetime!(2026-07-01 00:00 UTC));
        let lag = Duration::days(7);

        // Horizon is 2026-07-01: the window [07-01, 07-02) ends after it.
        let now = datetime!(2026-07-08 00:00 UTC);
        assert!(next_window(w, now, lag, DAY).unwrap().is_none());

        // Horizon is 2026-07-02: the window now closes exactly at the horizon.
        let now = datetime!(2026-07-09 00:00 UTC);
        let got = next_window(w, now, lag, DAY).unwrap().unwrap();
        assert_eq!(got.from(), datetime!(2026-07-01 00:00 UTC));
        assert_eq!(got.to(), datetime!(2026-07-02 00:00 UTC));
    }

    #[test]
    fn next_window_never_reaches_into_the_settlement_lag() {
        // The whole point of the lag: late corrections must still find their
        // interval in the hot tier.
        let w = TieringWatermark::new(datetime!(2026-07-01 00:00 UTC));
        let now = datetime!(2026-07-09 12:00 UTC);
        let lag = Duration::days(7);

        let win = next_window(w, now, lag, DAY).unwrap().unwrap();
        assert!(
            win.to() <= now - lag,
            "window must close at or before the horizon"
        );
    }

    #[test]
    fn next_window_advances_one_step_at_a_time() {
        let lag = Duration::ZERO;
        let now = datetime!(2026-07-10 00:00 UTC);
        let mut w = TieringWatermark::new(datetime!(2026-07-01 00:00 UTC));

        let mut windows = Vec::new();
        while let Some(win) = next_window(w, now, lag, DAY).unwrap() {
            windows.push(win);
            w = win.resulting_watermark();
        }

        assert_eq!(windows.len(), 9);
        // Gapless and non-overlapping: each window starts where the last ended.
        for pair in windows.windows(2) {
            assert_eq!(pair[0].to(), pair[1].from());
        }
        assert_eq!(w.get(), now);
    }

    #[test]
    fn alignment_matches_the_hot_tier_partition_bounds() {
        assert_eq!(
            align_to_step(datetime!(2026-07-20 13:47:03 UTC), DAY),
            datetime!(2026-07-20 00:00 UTC)
        );
        assert_eq!(
            align_to_step(datetime!(2026-07-20 00:00 UTC), DAY),
            datetime!(2026-07-20 00:00 UTC)
        );
        assert_eq!(
            align_to_step(datetime!(2026-07-20 13:47 UTC), Duration::hours(6)),
            datetime!(2026-07-20 12:00 UTC)
        );
        // `rem_euclid`, not `%`: a pre-epoch instant must round down too.
        assert_eq!(
            align_to_step(datetime!(1969-12-31 13:00 UTC), DAY),
            datetime!(1969-12-31 00:00 UTC)
        );
    }

    #[test]
    fn a_watermark_off_the_step_grid_is_refused_rather_than_walked_past() {
        // The only way here is changing `partition_step` on a table that has
        // already archived. Left to run, every window would name a partition
        // relation nothing creates, so each would look empty, the watermark
        // would advance over rows still in PostgreSQL, and they would be
        // stranded below it with nothing but `invariant_violations` to say so.
        let misaligned = TieringWatermark::new(datetime!(2026-07-20 06:00 UTC));
        let now = datetime!(2026-08-01 00:00 UTC);

        let err = next_window(misaligned, now, Duration::ZERO, DAY).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("aligned"), "{msg}");

        // The same instant is aligned to a six-hour step, and that still works.
        assert!(
            next_window(misaligned, now, Duration::ZERO, Duration::hours(6))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn next_window_rejects_nonsensical_configuration() {
        let w = TieringWatermark::empty();
        let now = datetime!(2026-07-10 00:00 UTC);

        assert!(next_window(w, now, Duration::ZERO, Duration::ZERO).is_err());
        assert!(next_window(w, now, -Duration::days(1), DAY).is_err());
    }
}
