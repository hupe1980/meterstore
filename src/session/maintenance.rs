//! Scheduled upkeep: archival, snapshot expiry, and the invariant check.
//!
//! Three jobs with different cadences and very different consequences, which is
//! why they are one type with one entry point rather than three independent
//! timers:
//!
//! - **Archival** must run often enough that the hot tier stays bounded, and its
//!   lag is the thing to alert on.
//! - **Snapshot expiry** is a *compliance* decision, not cleanup: a snapshot is
//!   what makes a past settlement reproducible, so a run that expires too
//!   eagerly destroys the audit position the cold tier exists to hold. It is
//!   therefore opt-in and runs rarely.
//! - **The invariant check** is the alert. It is cheap and it is the only one
//!   whose failure means query results may already be wrong.
//!
//! # Nothing here holds state
//!
//! Every job is idempotent and recovers from an interruption on its own (§6.2),
//! so the scheduler is a loop and a clock. It keeps no checkpoint, because the
//! checkpoint is the Iceberg snapshot summary. Restarting the process loses
//! nothing.
//!
//! # The clock is injected
//!
//! `now` is a parameter, so a test can drive months of archival in milliseconds
//! and a deployment with skewed clocks fails visibly rather than subtly. Tiering
//! itself never reads a clock — it routes on `from`, a data value — so the only
//! thing wall time decides is *when* a window becomes eligible.

use time::{Duration, OffsetDateTime};
use tracing::{info, warn};

use crate::error::Result;
use crate::tiering::ArchivalOutcome;

/// What one maintenance cycle did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaintenanceOutcome {
    /// Archival runs performed, in order.
    pub archival: Vec<ArchivalOutcome>,
    /// Cold snapshots expired, when expiry was enabled for this cycle.
    pub snapshots_expired: usize,
    /// Rows found in the wrong tier. **Non-zero means results may be wrong.**
    pub invariant_violations: u64,
}

impl MaintenanceOutcome {
    /// Rows moved from hot to cold this cycle.
    pub fn rows_archived(&self) -> u64 {
        self.archival.iter().map(|o| o.rows).sum()
    }

    /// Windows archived this cycle.
    pub fn windows_archived(&self) -> usize {
        self.archival
            .iter()
            .filter(|o| o.archived_anything())
            .count()
    }

    /// Whether another process was doing the work.
    pub fn lease_contended(&self) -> bool {
        self.archival.iter().any(|o| o.lease_contended)
    }

    /// Whether the tiers still partition the data as they should.
    pub fn healthy(&self) -> bool {
        self.invariant_violations == 0
    }
}

/// Upkeep for one store, run on demand or on a schedule.
#[derive(Debug, Clone)]
pub struct Maintenance {
    store: crate::session::MeterStore,
    interval: Duration,
    max_windows: usize,
    expire_snapshots: bool,
}

impl Maintenance {
    /// Default cadence: often enough that a daily window is archived promptly
    /// without the job being the thing that wakes the database up.
    pub const DEFAULT_INTERVAL: Duration = Duration::minutes(15);

    /// How many windows one cycle will catch up at most.
    ///
    /// Bounded so a store that has been down for a month catches up over several
    /// cycles instead of holding one process for hours — each window is its own
    /// commit and independently recoverable, so stopping early costs nothing.
    pub const DEFAULT_MAX_WINDOWS: usize = 32;

    /// Build maintenance for a store, with defaults.
    pub fn new(store: crate::session::MeterStore) -> Self {
        Self {
            store,
            interval: Self::DEFAULT_INTERVAL,
            max_windows: Self::DEFAULT_MAX_WINDOWS,
            expire_snapshots: false,
        }
    }

    /// How often [`spawn`](Self::spawn) runs a cycle.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Cap the windows one cycle catches up.
    pub fn max_windows(mut self, max: usize) -> Self {
        self.max_windows = max.max(1);
        self
    }

    /// Also expire snapshots past the configured retention window.
    ///
    /// **Off by default, and deliberately so.** The retention window decides how
    /// far back a settlement can be reproduced, which is a compliance decision
    /// rather than a disk-space one. A store that quietly expired snapshots
    /// would be making that decision on the operator's behalf.
    pub fn expire_snapshots(mut self, enabled: bool) -> Self {
        self.expire_snapshots = enabled;
        self
    }

    /// Run one cycle.
    ///
    /// Archival first, then the invariant check, so the check sees the state the
    /// cycle produced rather than the one it started from. A failed archival
    /// aborts the cycle: expiring snapshots or reporting health on top of a
    /// half-finished archival would describe a state that does not exist.
    pub async fn run_once(&self, now: OffsetDateTime) -> Result<MaintenanceOutcome> {
        let archival = self.store.archive(now, self.max_windows).await?;

        let snapshots_expired = if self.expire_snapshots {
            self.store.expire_snapshots(now).await?
        } else {
            0
        };

        let status = self.store.status(now).await?;
        let invariant_violations = status.invariant_violations.max(0) as u64;

        if invariant_violations > 0 {
            warn!(
                table = self.store.table(),
                invariant_violations, "rows are in the wrong tier: query results may be wrong"
            );
        }

        let outcome = MaintenanceOutcome {
            archival,
            snapshots_expired,
            invariant_violations,
        };

        info!(
            table = self.store.table(),
            windows = outcome.windows_archived(),
            rows = outcome.rows_archived(),
            snapshots_expired,
            healthy = outcome.healthy(),
            "maintenance cycle"
        );
        Ok(outcome)
    }

    /// Run cycles on the configured interval, on the caller's runtime.
    ///
    /// The returned handle **owns** the loop: dropping it stops the loop after
    /// the cycle in flight, exactly as [`MaintenanceHandle::shutdown`] does. A
    /// handle that goes out of scope therefore cannot leave a background task
    /// archiving against a store nobody is watching.
    ///
    /// A failing cycle is logged and retried on the next tick rather than ending
    /// the loop: every failure mode in §15.2 is either transient or needs an
    /// operator, and neither is helped by the scheduler giving up silently.
    pub fn spawn(self) -> MaintenanceHandle {
        let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
        let period = std::time::Duration::from_secs(
            u64::try_from(self.interval.whole_seconds().max(1)).unwrap_or(900),
        );
        let table = self.store.table().to_string();

        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = &mut rx => {
                        info!(table = %table, "maintenance stopped");
                        return;
                    }
                    _ = ticker.tick() => {
                        // The clock is read here and nowhere deeper, so every
                        // decision in a cycle is made against one instant.
                        let now = OffsetDateTime::now_utc();
                        if let Err(e) = self.run_once(now).await {
                            warn!(table = %table, error = %e, "maintenance cycle failed; retrying next tick");
                        }
                    }
                }
            }
        });

        MaintenanceHandle {
            task,
            stop: Some(tx),
        }
    }
}

/// A running maintenance loop, which it owns.
#[derive(Debug)]
pub struct MaintenanceHandle {
    task: tokio::task::JoinHandle<()>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MaintenanceHandle {
    /// Ask the loop to stop after the cycle in flight, and wait for it.
    ///
    /// Graceful rather than an abort, because a cycle interrupted between the
    /// cold commit and the partition drop leaves an orphan for the next run to
    /// reclaim — recoverable, but pointless work to create on a clean shutdown.
    ///
    /// Dropping the handle does the same thing without waiting: the loop's stop
    /// signal is this handle's sender, so it fires either way.
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = (&mut self.task).await;
    }

    /// Whether the loop has ended.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watermark::{ArchivalWindow, TieringWatermark};
    use time::macros::datetime;

    fn window() -> ArchivalWindow {
        ArchivalWindow::new(
            datetime!(2026-07-20 00:00 UTC),
            datetime!(2026-07-21 00:00 UTC),
        )
        .unwrap()
    }

    fn archived(rows: u64) -> ArchivalOutcome {
        ArchivalOutcome {
            window: Some(window()),
            rows,
            watermark: window().resulting_watermark(),
            orphans_reclaimed: 0,
            partitions_created: 0,
            lease_contended: false,
        }
    }

    fn idle() -> ArchivalOutcome {
        ArchivalOutcome {
            window: None,
            rows: 0,
            watermark: TieringWatermark::empty(),
            orphans_reclaimed: 0,
            partitions_created: 3,
            lease_contended: false,
        }
    }

    fn contended() -> ArchivalOutcome {
        ArchivalOutcome {
            lease_contended: true,
            ..idle()
        }
    }

    #[test]
    fn an_outcome_sums_only_the_windows_that_moved_data() {
        let outcome = MaintenanceOutcome {
            archival: vec![archived(96), archived(4), idle()],
            snapshots_expired: 0,
            invariant_violations: 0,
        };
        assert_eq!(outcome.rows_archived(), 100);
        assert_eq!(outcome.windows_archived(), 2);
    }

    #[test]
    fn health_is_exactly_the_absence_of_violations() {
        let bad = MaintenanceOutcome {
            invariant_violations: 1,
            ..Default::default()
        };
        assert!(!bad.healthy());
        assert!(MaintenanceOutcome::default().healthy());
    }

    #[test]
    fn contention_is_reported_and_is_not_a_failure() {
        // Every replica runs the schedule and one wins. The others must not look
        // like failures, or the alert fires on a healthy deployment.
        let outcome = MaintenanceOutcome {
            archival: vec![contended()],
            ..Default::default()
        };
        assert!(outcome.lease_contended());
        assert!(outcome.healthy());
        assert_eq!(outcome.rows_archived(), 0);
    }

    #[test]
    fn the_defaults_are_sane() {
        // A cadence longer than the archival step would let the hot tier grow a
        // window at a time, and a zero window cap would make every cycle a
        // no-op with nothing reporting a failure.
        assert!(Maintenance::DEFAULT_INTERVAL < Duration::DAY);
        const { assert!(Maintenance::DEFAULT_MAX_WINDOWS >= 1) };
    }
}
