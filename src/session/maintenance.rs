//! Scheduled upkeep: archival, snapshot expiry, the retention sweep, and the
//! invariant check.
//!
//! Four jobs with different cadences and very different consequences, which is
//! why they are one type with one entry point rather than four independent
//! timers:
//!
//! - **Archival** must run often enough that the hot tier stays bounded, and its
//!   lag is the thing to alert on.
//! - **Snapshot expiry** is a *compliance* decision, not cleanup: a snapshot is
//!   what makes a past settlement reproducible, so a run that expires too
//!   eagerly destroys the audit position the cold tier exists to hold. It is
//!   therefore opt-in and runs rarely.
//! - **The retention sweep** ([`Maintenance::anonymise_after`]) is the other
//!   compliance decision, and the only job here that is irreversible. Also
//!   opt-in, and it runs across every table at once because the subject registry
//!   is deployment-wide.
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

/// What one maintenance cycle did to **one** table.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableMaintenance {
    /// The table this row describes.
    pub table: String,
    /// Archival runs performed, in order.
    pub archival: Vec<ArchivalOutcome>,
    /// Cold snapshots expired, when expiry was enabled for this cycle.
    pub snapshots_expired: usize,
    /// Rows found in the wrong tier. **Non-zero means results may be wrong.**
    pub invariant_violations: u64,
    /// Why this table's cycle did not complete, if it did not.
    ///
    /// Rendered rather than typed because it is a *report*: the cycle has already
    /// moved on to the next table, and what is left to do with this is log it and
    /// alert on it. A caller wanting to act on the error programmatically runs
    /// that table's [`archive`](crate::MeterStore::archive) itself.
    pub failure: Option<String>,
}

impl TableMaintenance {
    /// Rows moved from hot to cold.
    pub fn rows_archived(&self) -> u64 {
        self.archival.iter().map(|o| o.rows).sum()
    }

    /// Windows archived.
    pub fn windows_archived(&self) -> usize {
        self.archival
            .iter()
            .filter(|o| o.archived_anything())
            .count()
    }

    /// Whether another process was doing this table's work.
    pub fn lease_contended(&self) -> bool {
        self.archival.iter().any(|o| o.lease_contended)
    }

    /// Whether a run stopped because a lock was not available.
    ///
    /// Not a failure and not counted as one: the statement declined to queue for
    /// a lock rather than blocking every reader and writer behind it, and nothing
    /// was changed. It is worth surfacing because a table that defers *every*
    /// cycle is a table whose watermark is not moving, and the fix is on the
    /// database — a long-running query, or a session idle in a transaction —
    /// rather than here.
    pub fn deferred(&self) -> bool {
        self.archival.iter().any(|o| o.deferred)
    }

    /// Whether this table's cycle completed and its tiers still partition its
    /// data as they should.
    pub fn healthy(&self) -> bool {
        self.invariant_violations == 0 && self.failure.is_none()
    }
}

/// What one maintenance cycle did.
///
/// **Per table**, because §15.3 makes every table its own unit. A cycle that only
/// summed would report a deployment healthy while one of its tables was
/// quarantined, and would give an operator no name to act on. The aggregate
/// accessors fold the rows rather than replacing them, because "did anything
/// move" is a real question too.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaintenanceOutcome {
    /// One entry per table the cycle ran over, in name order.
    pub tables: Vec<TableMaintenance>,
    /// Subjects whose linkage this cycle destroyed, when a retention policy is
    /// configured.
    ///
    /// **Not per table.** The subject registry is deployment-wide, so a subject
    /// is due only once every table that names it has passed the ceiling — see
    /// [`Maintenance::anonymise_after`].
    pub anonymised: Vec<crate::erasure::ErasureRecord>,
    /// Why the retention sweep did not run, if it was configured and failed.
    ///
    /// Rendered rather than typed, for the reason
    /// [`TableMaintenance::failure`] is: the cycle has already moved on, and
    /// what is left to do with it is log it and alert on it.
    pub retention_failure: Option<String>,
}

impl MaintenanceOutcome {
    /// Rows moved from hot to cold this cycle, across every table.
    pub fn rows_archived(&self) -> u64 {
        self.tables
            .iter()
            .map(TableMaintenance::rows_archived)
            .sum()
    }

    /// Windows archived this cycle, across every table.
    pub fn windows_archived(&self) -> usize {
        self.tables
            .iter()
            .map(TableMaintenance::windows_archived)
            .sum()
    }

    /// Cold snapshots expired this cycle, across every table.
    pub fn snapshots_expired(&self) -> usize {
        self.tables.iter().map(|t| t.snapshots_expired).sum()
    }

    /// Rows found in the wrong tier, across every table.
    ///
    /// **Non-zero means results may be wrong**, and
    /// [`unhealthy`](Self::unhealthy) names which table.
    pub fn invariant_violations(&self) -> u64 {
        self.tables.iter().map(|t| t.invariant_violations).sum()
    }

    /// Whether another process was doing the work for any table.
    pub fn lease_contended(&self) -> bool {
        self.tables.iter().any(TableMaintenance::lease_contended)
    }

    /// Whether any table's run stopped because a lock was not available.
    pub fn deferred(&self) -> bool {
        self.tables.iter().any(TableMaintenance::deferred)
    }

    /// Whether every table's tiers still partition their data as they should,
    /// and the retention sweep — if configured — ran.
    pub fn healthy(&self) -> bool {
        self.tables.iter().all(TableMaintenance::healthy) && self.retention_failure.is_none()
    }

    /// The tables that are not healthy — what an alert should name.
    pub fn unhealthy(&self) -> impl Iterator<Item = &TableMaintenance> {
        self.tables.iter().filter(|t| !t.healthy())
    }

    /// The tables whose cycle failed, with the reason.
    ///
    /// A failed **retention sweep** appears here too, under the name
    /// `<retention>`: it is not a table's failure, but it is a failure an
    /// operator has to see, and a caller iterating this to build an alert must
    /// not have to know about a second place to look.
    pub fn failures(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tables
            .iter()
            .filter_map(|t| Some((t.table.as_str(), t.failure.as_deref()?)))
            .chain(
                self.retention_failure
                    .as_deref()
                    .map(|why| (RETENTION_LABEL, why)),
            )
    }

    /// Subjects anonymised this cycle.
    pub fn subjects_anonymised(&self) -> usize {
        self.anonymised.len()
    }
}

/// What a failed retention sweep is named in [`MaintenanceOutcome::failures`].
///
/// Angle-bracketed so it cannot collide with a table name: `TableConfig` refuses
/// anything that is not a plain identifier.
pub const RETENTION_LABEL: &str = "<retention>";

/// Upkeep for one store or a whole catalog, run on demand or on a schedule.
///
/// **One loop over N tables**, not N loops. Each table keeps its own watermark,
/// archiver and lease (§15.3); only the *scheduling* is shared, which is what
/// gives a deployment one place to ask whether upkeep is keeping up.
#[derive(Debug, Clone)]
pub struct Maintenance {
    /// The tables this loop maintains, in the order they are visited.
    stores: Vec<crate::session::MeterStore>,
    interval: Duration,
    max_windows: usize,
    expire_snapshots: bool,
    retention: Option<RetentionSweep>,
}

/// A configured § 60 Abs. 6 sweep: the policy, and who to record as having run it.
#[derive(Debug, Clone)]
struct RetentionSweep {
    policy: crate::erasure::Retention,
    reason: String,
    actor: String,
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

    /// Build maintenance for one store, with defaults.
    pub fn new(store: crate::session::MeterStore) -> Self {
        Self::over(vec![store])
    }

    /// Build maintenance over several stores — every table of a catalog.
    ///
    /// Visited in the order given, one after another. Sequential rather than
    /// concurrent for the reason `archive_all` is: each table's archival holds
    /// its own lease and its own PostgreSQL connections, and running twenty at
    /// once turns a background job into a load spike on the operational database
    /// it is meant to be relieving.
    ///
    /// A table whose cycle fails does not stop the others — the outcome names it
    /// and the next tick tries again — because the tables share nothing a partial
    /// run could corrupt.
    pub fn over(stores: Vec<crate::session::MeterStore>) -> Self {
        Self {
            stores,
            interval: Self::DEFAULT_INTERVAL,
            max_windows: Self::DEFAULT_MAX_WINDOWS,
            expire_snapshots: false,
            retention: None,
        }
    }

    /// The tables this loop maintains.
    pub fn tables(&self) -> Vec<&str> {
        self.stores.iter().map(|s| s.table()).collect()
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

    /// Also anonymise subjects whose readings have passed a retention ceiling.
    ///
    /// **Off by default.** § 60 Abs. 6 MsbG obliges the Messstellenbetreiber to
    /// erase or anonymise personenbezogene Messwerte as soon as storing them is
    /// no longer necessary, *"spätestens jedoch nach drei Jahren ab dem Schluss
    /// des Kalenderjahres"* — a duty on a clock rather than an answer to a
    /// request, which is why it belongs on a schedule. It is opt-in for the
    /// reason snapshot expiry is: destroying a linkage is irreversible, so a
    /// store that did it uninvited would take a compliance decision on the
    /// operator's behalf.
    ///
    /// # A catalog operation even with one table
    ///
    /// The sweep runs across **every** store this loop holds, and a subject is
    /// due only once all of them have passed the ceiling. The registry is
    /// deployment-wide — one map keyed by natural identifier — so a single
    /// erasure unlinks a subject everywhere, and sweeping per table would orphan
    /// readings the other tables still hold.
    ///
    /// `reason` and `actor` go into the audit trail, which is what makes an
    /// erasure provable to a regulator; neither may be empty.
    ///
    /// ```no_run
    /// # use meterstore::{Maintenance, Retention};
    /// # fn example(m: Maintenance) -> Maintenance {
    /// m.anonymise_after(Retention::CalendarYears(3), "§ 60 Abs. 6 MsbG", "retention-job")
    /// # }
    /// ```
    pub fn anonymise_after(
        mut self,
        policy: crate::erasure::Retention,
        reason: impl Into<String>,
        actor: impl Into<String>,
    ) -> Self {
        self.retention = Some(RetentionSweep {
            policy,
            reason: reason.into(),
            actor: actor.into(),
        });
        self
    }

    /// The retention policy this loop applies, if any.
    pub fn retention(&self) -> Option<crate::erasure::Retention> {
        self.retention.as_ref().map(|r| r.policy)
    }

    /// Run one cycle over every table.
    ///
    /// Per table: archival first, then the invariant check, so the check sees the
    /// state the cycle produced rather than the one it started from. A failed
    /// archival ends *that table's* half — expiring snapshots or reporting health
    /// on top of a half-finished archival would describe a state that does not
    /// exist — and the cycle moves on to the next table.
    ///
    /// # A failure is reported, not returned
    ///
    /// `Ok` with [`MaintenanceOutcome::failures`] populated, rather than `Err`.
    /// The tables share nothing a partial run could corrupt, and what fails here
    /// persists until an operator acts — a quarantined schema, an unreachable
    /// catalogue — so stopping would let one such table freeze archival for every
    /// other, whose hot tier then grows without bound for the length of the
    /// incident.
    ///
    /// [`healthy`](MaintenanceOutcome::healthy) is false while any table failed,
    /// so a caller checking only that still notices.
    pub async fn run_once(&self, now: OffsetDateTime) -> Result<MaintenanceOutcome> {
        let mut tables = Vec::with_capacity(self.stores.len());
        for store in &self.stores {
            // **A failing table does not end the cycle.** The tables share
            // nothing a partial run could corrupt, and the states that fail here
            // — a quarantined schema, an unreachable catalogue — persist until an
            // operator acts. Aborting would mean one quarantined table freezing
            // archival for every other, whose hot tier then grows without bound
            // for as long as the quarantine lasts: a second, larger incident
            // caused by the reporting of the first.
            tables.push(match self.run_table(store, now).await {
                Ok(done) => done,
                Err(e) => {
                    warn!(
                        table = store.table(),
                        error = %e,
                        "maintenance failed for this table; the cycle continues with the rest"
                    );
                    TableMaintenance {
                        table: store.table().to_string(),
                        failure: Some(e.to_string()),
                        ..Default::default()
                    }
                }
            });
        }

        // **After** archival, and over every store at once. The order is no
        // longer load-bearing — the sweep reads no relation, only the registry's
        // own `epoch` column — but it stays last because that is where a duty
        // that comes due on a clock belongs in a cycle whose other steps are
        // about moving data. Over every store because the registry is
        // deployment-wide: the stores are consulted to find it and to tell
        // "nothing was due" apart from "this deployment stores no reference".
        let (anonymised, retention_failure) = match &self.retention {
            None => (Vec::new(), None),
            Some(sweep) => {
                let cutoff = sweep.policy.cutoff(now);
                match super::catalog::anonymise_across(
                    self.stores.iter(),
                    cutoff,
                    &sweep.reason,
                    &sweep.actor,
                    now,
                )
                .await
                {
                    Ok(records) => (records, None),
                    Err(e) => {
                        warn!(
                            error = %e,
                            %cutoff,
                            "retention sweep failed; the cycle continues and the next tick retries"
                        );
                        (Vec::new(), Some(e.to_string()))
                    }
                }
            }
        };

        let outcome = MaintenanceOutcome {
            tables,
            anonymised,
            retention_failure,
        };
        info!(
            tables = outcome.tables.len(),
            windows = outcome.windows_archived(),
            rows = outcome.rows_archived(),
            snapshots_expired = outcome.snapshots_expired(),
            anonymised = outcome.subjects_anonymised(),
            failed = outcome.failures().count(),
            healthy = outcome.healthy(),
            "maintenance cycle"
        );
        Ok(outcome)
    }

    /// One table's half of a cycle.
    async fn run_table(
        &self,
        store: &crate::session::MeterStore,
        now: OffsetDateTime,
    ) -> Result<TableMaintenance> {
        let archival = store.archive(now, self.max_windows).await?;

        // Expiry is a **metadata mutation**, so it belongs to the replica that
        // won the archive lease and to no other. Every replica runs the same
        // schedule by design; a loser that went on to expire snapshots anyway
        // would have two processes rewriting one table's metadata on their own
        // clocks, racing for a compare-and-swap that only one can win and
        // reporting the loss as a failed cycle. It also re-stamps the boundary
        // onto the current snapshot, which is the one step that must not
        // interleave with a commit moving it.
        //
        // Reading `status` below is not a mutation, so every replica still does
        // it — the invariant is worth checking from wherever it is noticed.
        let contended = archival.iter().all(|o| o.lease_contended);
        let snapshots_expired = match self.expire_snapshots && !contended {
            true => store.expire_snapshots(now).await?,
            false => 0,
        };

        let status = store.status(now).await?;
        let invariant_violations = status.invariant_violations.max(0) as u64;
        if invariant_violations > 0 {
            warn!(
                table = store.table(),
                invariant_violations, "rows are in the wrong tier: query results may be wrong"
            );
        }

        Ok(TableMaintenance {
            table: store.table().to_string(),
            archival,
            snapshots_expired,
            invariant_violations,
            failure: None,
        })
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
        let tables = self.tables().join(", ");

        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = &mut rx => {
                        info!(tables = %tables, "maintenance stopped");
                        return;
                    }
                    _ = ticker.tick() => {
                        // The clock is read here and nowhere deeper, so every
                        // decision in a cycle is made against one instant.
                        let now = OffsetDateTime::now_utc();
                        if let Err(e) = self.run_once(now).await {
                            warn!(tables = %tables, error = %e, "maintenance cycle failed; retrying next tick");
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
            deferred: false,
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
            deferred: false,
        }
    }

    fn contended() -> ArchivalOutcome {
        ArchivalOutcome {
            lease_contended: true,
            ..idle()
        }
    }

    fn table(name: &str, archival: Vec<ArchivalOutcome>) -> TableMaintenance {
        TableMaintenance {
            table: name.to_string(),
            archival,
            snapshots_expired: 0,
            invariant_violations: 0,
            failure: None,
        }
    }

    #[test]
    fn an_outcome_sums_only_the_windows_that_moved_data() {
        let outcome = MaintenanceOutcome {
            tables: vec![table(
                "readings_versions",
                vec![archived(96), archived(4), idle()],
            )],
            ..Default::default()
        };
        assert_eq!(outcome.rows_archived(), 100);
        assert_eq!(outcome.windows_archived(), 2);
    }

    #[test]
    fn an_outcome_over_several_tables_folds_them_and_keeps_them_apart() {
        // The aggregate answers "did anything move"; the rows answer "which
        // table". A cycle that only summed would report a deployment healthy
        // while one of its tables was quarantined, and would give an operator no
        // name to act on.
        let outcome = MaintenanceOutcome {
            tables: vec![
                table("readings_versions", vec![archived(96)]),
                TableMaintenance {
                    invariant_violations: 3,
                    ..table("esa_typ2_versions", vec![archived(4)])
                },
            ],
            ..Default::default()
        };

        assert_eq!(outcome.rows_archived(), 100);
        assert_eq!(outcome.windows_archived(), 2);
        assert_eq!(outcome.invariant_violations(), 3);
        assert!(!outcome.healthy());

        let named: Vec<&str> = outcome.unhealthy().map(|t| t.table.as_str()).collect();
        assert_eq!(named, vec!["esa_typ2_versions"]);
        assert!(outcome.tables[0].healthy(), "the other table is fine");
    }

    #[test]
    fn a_failed_table_is_reported_rather_than_ending_the_cycle() {
        // Returning early would let one quarantined table freeze archival for
        // every other, whose hot tier then grows without bound for the length of
        // the quarantine — a second and larger incident caused by how the first
        // was reported. So a failure is a row, and `healthy` is false.
        let outcome = MaintenanceOutcome {
            tables: vec![
                table("readings_versions", vec![archived(96)]),
                TableMaintenance {
                    failure: Some("quarantined: column \"tenant\" …".to_string()),
                    ..table("esa_typ2_versions", Vec::new())
                },
            ],
            ..Default::default()
        };

        assert!(!outcome.healthy());
        assert_eq!(
            outcome.failures().map(|(t, _)| t).collect::<Vec<_>>(),
            vec!["esa_typ2_versions"],
        );
        // The table that did work still reports what it did.
        assert_eq!(outcome.rows_archived(), 96);
        assert!(outcome.tables[0].healthy());
    }

    #[test]
    fn health_is_exactly_the_absence_of_violations() {
        let bad = MaintenanceOutcome {
            tables: vec![TableMaintenance {
                invariant_violations: 1,
                ..table("readings_versions", Vec::new())
            }],
            ..Default::default()
        };
        assert!(!bad.healthy());
        // A cycle over no tables is vacuously healthy, which is right: it is what
        // a catalog reports before anything has been added to it.
        assert!(MaintenanceOutcome::default().healthy());
    }

    #[test]
    fn contention_is_reported_and_is_not_a_failure() {
        // Every replica runs the schedule and one wins. The others must not look
        // like failures, or the alert fires on a healthy deployment.
        let outcome = MaintenanceOutcome {
            tables: vec![table("readings_versions", vec![contended()])],
            ..Default::default()
        };
        assert!(outcome.lease_contended());
        assert!(outcome.healthy());
        assert_eq!(outcome.rows_archived(), 0);
    }

    #[test]
    fn a_failed_retention_sweep_is_a_failure_the_table_alert_already_sees() {
        // The sweep is not any one table's work, but an operator watching
        // `failures()` for a name must not have to know about a second place to
        // look — a retention duty silently not running is the failure mode.
        let outcome = MaintenanceOutcome {
            tables: vec![table("readings_versions", vec![archived(96)])],
            retention_failure: Some("no subject column is declared".to_string()),
            ..Default::default()
        };

        assert!(!outcome.healthy());
        assert_eq!(
            outcome.failures().map(|(t, _)| t).collect::<Vec<_>>(),
            vec![RETENTION_LABEL],
        );
        // And the table's own work still reports as done.
        assert_eq!(outcome.rows_archived(), 96);
        assert!(outcome.tables[0].healthy());
    }

    #[test]
    fn a_retention_policy_is_opt_in_and_readable_back() {
        // Destroying a linkage is irreversible, so a loop must never do it
        // uninvited — and a deployment that did opt in has to be able to see it.
        let plain = Maintenance::over(Vec::new());
        assert_eq!(plain.retention(), None);

        let sweeping = Maintenance::over(Vec::new()).anonymise_after(
            crate::erasure::Retention::CalendarYears(3),
            "§ 60 Abs. 6 MsbG",
            "retention-job",
        );
        assert_eq!(
            sweeping.retention(),
            Some(crate::erasure::Retention::CalendarYears(3))
        );
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
