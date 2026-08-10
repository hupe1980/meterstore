//! The archival job.
//!
//! The ordering below is the whole correctness argument, so it is worth stating
//! plainly:
//!
//! 1. **Detach, then scan.** Detaching first makes the partition invisible to
//!    writers while the archiver still reads it, so no row can be inserted into
//!    a partition that is mid-archival.
//! 2. **Commit cold, then drop hot.** A crash between them leaves an orphaned
//!    detached partition — data intact, invisible, reclaimable. The reverse
//!    order loses data permanently.
//! 3. **Reclaim orphans first.** A previous run that died mid-flight is repaired
//!    before new work starts, so the two states never compound.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use time::OffsetDateTime;
use tracing::{debug, info, warn};

use crate::config::ValidatedTableConfig;
use crate::error::{Error, Result};
use crate::tiering::store::{BatchStream, ColdStore, HotStore, PartitionId, WriteHints};
use crate::watermark::{ArchivalWindow, TieringWatermark, next_window};

/// Tally rows as they pass, without holding on to them.
///
/// The archiver has to compare what it scanned against what the cold store
/// committed (§8.2), and a streaming write gives it no other place to learn the
/// first number.
fn count_rows(batches: BatchStream, counter: Arc<AtomicU64>) -> BatchStream {
    use futures::StreamExt;
    Box::pin(batches.map(move |batch| {
        if let Ok(ref b) = batch {
            counter.fetch_add(b.num_rows() as u64, Ordering::Relaxed);
        }
        batch
    }))
}

/// What one archival run did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivalOutcome {
    /// The window archived, if any.
    pub window: Option<ArchivalWindow>,
    /// Rows moved to the cold tier.
    pub rows: u64,
    /// The watermark after the run.
    pub watermark: TieringWatermark,
    /// Orphaned partitions reclaimed before the run.
    pub orphans_reclaimed: usize,
    /// Hot partitions pre-created for future writes.
    pub partitions_created: usize,
    /// Whether another process held the archive lease, so this run did nothing.
    ///
    /// Not a failure. In a replicated deployment every replica runs the schedule
    /// and exactly one wins; the others report this and stop. An alert on
    /// archival failures should ignore it, and an alert on watermark lag will
    /// still fire if *nobody* is winning.
    pub lease_contended: bool,
}

impl ArchivalOutcome {
    /// Whether this run moved any data.
    pub fn archived_anything(&self) -> bool {
        self.window.is_some()
    }

    /// A run that did nothing because another archiver holds the lease.
    fn contended(watermark: TieringWatermark) -> Self {
        Self {
            window: None,
            rows: 0,
            watermark,
            orphans_reclaimed: 0,
            partitions_created: 0,
            lease_contended: true,
        }
    }
}

/// Moves settled intervals from the hot store to the cold store.
pub struct Archiver<H, C> {
    hot: H,
    cold: C,
    config: ValidatedTableConfig,
}

impl<H: HotStore, C: ColdStore> Archiver<H, C> {
    /// Build an archiver for one table.
    pub fn new(hot: H, cold: C, config: ValidatedTableConfig) -> Self {
        Self { hot, cold, config }
    }

    /// The table this archiver manages.
    pub fn table(&self) -> &str {
        self.config.name()
    }

    /// Run one archival cycle.
    ///
    /// Archives at most one window. Callers that want to catch up run this in a
    /// loop until [`ArchivalOutcome::archived_anything`] is false — one window
    /// per commit keeps each cold-tier transaction bounded and independently
    /// recoverable.
    pub async fn run_once(&self, now: OffsetDateTime) -> Result<ArchivalOutcome> {
        let started = std::time::Instant::now();
        let metrics = crate::observe::metrics();
        let attrs = crate::observe::table(self.config.name());

        // Exactly one archiver per table (§5.2). The window between detaching a
        // partition and dropping it is the only state where the tiering
        // invariant is relaxed, and it is only safe because one process owns it.
        // Two archivers would both target the window above the same watermark,
        // and one would commit rows the other had already taken.
        let Some(lease) = self.hot.try_archive_lease(self.config.name()).await? else {
            let watermark = self.cold.watermark(self.config.name()).await?;
            debug!(
                table = self.config.name(),
                "archive lease held elsewhere; skipping"
            );
            return Ok(ArchivalOutcome::contended(watermark));
        };

        let outcome = self.run_once_inner(now).await;

        // Released whether or not the run succeeded. A failed run must not keep
        // the table locked against the next attempt.
        if let Err(e) = lease.release().await {
            warn!(
                table = self.config.name(),
                error = %e,
                "could not release the archive lease; it dies with this session"
            );
        }

        if outcome.is_err() {
            metrics.archival_failures.add(1, &attrs);
        }
        if let Ok(ref o) = outcome {
            metrics.archival_rows.add(o.rows, &attrs);
            metrics
                .orphans_reclaimed
                .add(o.orphans_reclaimed as u64, &attrs);
            if o.archived_anything() {
                metrics.partitions_dropped.add(1, &attrs);
                metrics
                    .archival_duration
                    .record(started.elapsed().as_secs_f64(), &attrs);
            }
            metrics.watermark_lag.record(
                (now - o.watermark.get()).whole_seconds().max(0) as u64,
                &attrs,
            );
        }
        outcome
    }

    /// The archival cycle itself, wrapped by `run_once` for instrumentation.
    async fn run_once_inner(&self, now: OffsetDateTime) -> Result<ArchivalOutcome> {
        let table = self.config.name();

        // 0. Refuse to archive into a layout nobody agreed on (§11). Checked
        //    every run rather than at construction, because the cold table can be
        //    changed out of band — by an operator running compaction with Spark,
        //    or by a second deployment on an older configuration. Failing here
        //    freezes the watermark, which is the point: the rows stay in
        //    PostgreSQL, where they can still be corrected.
        self.verify_schema(table).await?;

        // 1. Repair anything a previous run left behind.
        let orphans_reclaimed = self.reclaim_orphans(table).await?;

        // 2. Keep the write frontier supplied with partitions. Done before
        //    archiving so a failure here surfaces even on an idle cycle —
        //    running out of partitions makes inserts fail, not just archival.
        let partitions_created = self
            .hot
            .ensure_partitions(
                table,
                now,
                now + self.config.partition_headroom(),
                self.config.partition_step(),
            )
            .await?
            .len();

        // Not the count created, and **not** the configured expectation — the
        // partitions that actually exist ahead of the write frontier. This was
        // `expected_hot_partitions()`, which is a pure function of configuration:
        // a constant, unaffected by an archiver that stopped creating partitions,
        // and therefore a gauge that could never reach the zero it is alerted on.
        // Reaching zero stops writes, so it has to be counted from reality.
        if let Some(starts) = self.hot.partition_starts(table).await? {
            crate::observe::metrics().hot_partitions_ahead.record(
                crate::tiering::store::partitions_ahead(&starts, now, self.config.partition_step())
                    as u64,
                &crate::observe::table(table),
            );
        }

        // 3. Pick a closed window, if one is due.
        let watermark = self.cold.watermark(table).await?;
        let Some(window) = next_window(
            watermark,
            now,
            self.config.settlement_lag(),
            self.config.archival_step(),
        )?
        else {
            debug!(table, %watermark, "no closed window due");
            return Ok(ArchivalOutcome {
                window: None,
                rows: 0,
                watermark,
                orphans_reclaimed,
                partitions_created,
                lease_contended: false,
            });
        };

        // 4. A window with no partition holds nothing, and there may be a great
        //    many of them in a row. Absorb the whole empty stretch into one
        //    commit rather than one per step.
        let window = self.widen_over_empty(table, window, now).await?;

        let rows = self.archive_window(table, window).await?;
        let watermark = watermark.advance_to(window.resulting_watermark())?;

        info!(
            table,
            from = %window.from(),
            to = %window.to(),
            rows,
            %watermark,
            "archived window"
        );

        Ok(ArchivalOutcome {
            window: Some(window),
            rows,
            watermark,
            orphans_reclaimed,
            partitions_created,
            lease_contended: false,
        })
    }

    /// Extend a window that holds no partition over the whole empty stretch.
    ///
    /// # The problem this solves is the first run, not a rare one
    ///
    /// A table that has never been archived has no snapshot, so its watermark is
    /// the **Unix epoch** — and [`next_window`] starts from the watermark. Left
    /// one step at a time, a deployment created in 2026 therefore commits one
    /// empty Iceberg snapshot per day since 1970 before it reaches a single real
    /// row: some twenty thousand commits, capped at a few dozen per maintenance
    /// cycle, and twenty thousand snapshots retained for the ten years §10.5
    /// keeps them. The store is unusable for days and its metadata never
    /// recovers. Every integration suite used to hide this by seeding the
    /// boundary by hand, which is the tell that it was a production gap rather
    /// than a test convenience.
    ///
    /// The same shape recurs whenever a table is idle for a stretch — a
    /// deployment that stops receiving one commodity, a backfill that starts in
    /// the middle of the history.
    ///
    /// # Why widening is safe
    ///
    /// Rows live in partitions. A range with no partition therefore holds no
    /// rows, so a window covering it archives nothing and the watermark may pass
    /// over it in one move: the §6.3 invariant is about *which tier owns a range*
    /// and both tiers own nothing here.
    ///
    /// The widened window still stops at the archival horizon, so it never
    /// reaches into the settlement lag, and it stays a whole multiple of
    /// `archival_step`, so a window still maps to exactly one partition when
    /// there is one.
    ///
    /// A store that cannot enumerate its partitions
    /// ([`HotStore::partition_starts`] returning `None`) keeps the
    /// one-window-per-commit behaviour, because "cannot say" must not be read as
    /// "nothing anywhere".
    async fn widen_over_empty(
        &self,
        table: &str,
        window: ArchivalWindow,
        now: OffsetDateTime,
    ) -> Result<ArchivalWindow> {
        if self
            .hot
            .partition_exists(&PartitionId::for_window(table, window))
            .await?
        {
            return Ok(window);
        }
        let Some(starts) = self.hot.partition_starts(table).await? else {
            return Ok(window);
        };

        let horizon = now - self.config.settlement_lag();
        // The next partition that does exist bounds the gap; with none, the
        // horizon does. Never past the horizon either way.
        let target = starts
            .into_iter()
            .filter(|start| *start >= window.to())
            .min()
            .unwrap_or(horizon)
            .min(horizon);

        let from = window.from();
        let step = self.config.archival_step().whole_seconds().max(1);
        // `next_window` already established that one whole step fits below the
        // horizon, so this is at least 1 and the window never shrinks.
        let steps = ((target - from).whole_seconds() / step).max(1);
        let widened = ArchivalWindow::new(from, from + time::Duration::seconds(steps * step))?;

        if widened != window {
            info!(
                table,
                from = %widened.from(),
                to = %widened.to(),
                steps,
                "no partition holds this range; advancing the watermark over it in one commit"
            );
        }
        Ok(widened)
    }

    /// Archive one window: detach, scan, commit, drop.
    async fn archive_window(&self, table: &str, window: ArchivalWindow) -> Result<u64> {
        let partition = PartitionId::for_window(table, window);

        // A window with no partition holds no rows: either nothing was ever
        // written for that period, or it predates deployment. It must still be
        // archived as an empty window, because refusing to advance the watermark
        // over a gap would stall archival on it permanently.
        if !self.hot.partition_exists(&partition).await? {
            debug!(
                table,
                from = %window.from(),
                "no partition for window; archiving as empty"
            );
            self.cold
                .append_and_commit(
                    table,
                    crate::tiering::store::stream_of(Vec::new()),
                    WriteHints::default(),
                    window,
                )
                .await?;
            return Ok(0);
        }

        // Detach first: the partition stays readable here but is invisible to
        // writers, so the set of rows being archived cannot grow underneath us.
        self.hot.detach_partition(&partition).await?;

        // Asked before the scan, while the partition is still whole: it sizes
        // the bloom filter on `malo_id`, which the writer fixes before it sees a
        // row and which §10.2 calls the highest-leverage setting in the layout.
        let hints = WriteHints {
            distinct_malo_ids: self.hot.distinct_malo_ids(&partition).await?,
        };

        let batches = self
            .hot
            .scan_detached(&partition, &self.config.scan_spec())
            .await?;

        // The rows are counted **as they stream**, not collected. Materialising
        // a partition to count it would put archival's peak memory in
        // proportion to the window — ~9.6 M rows for a day at 100 k measuring
        // points — which is the §18 budget it would blow first. The counter is
        // shared so the check below still compares what was scanned against what
        // was committed.
        let scanned = Arc::new(AtomicU64::new(0));
        let counted = count_rows(batches, Arc::clone(&scanned));

        // Commit before dropping. If the process dies here the partition is
        // orphaned but intact, and the next run reclaims it.
        let commit = self
            .cold
            .append_and_commit(table, counted, hints, window)
            .await?;

        let rows = scanned.load(Ordering::Relaxed);
        if commit.rows != rows {
            return Err(Error::InvariantViolated {
                table: table.to_string(),
                detail: format!(
                    "cold store committed {} rows but {rows} were scanned",
                    commit.rows
                ),
            });
        }

        // Only now is it safe to reclaim the space.
        self.hot.drop_partition(&partition).await?;

        Ok(rows)
    }

    /// Halt the table if the cold schema cannot hold what we are about to write.
    ///
    /// A store whose cold store cannot report its schema is not quarantined; it
    /// simply cannot be checked, and pretending otherwise would be the lie P6
    /// forbids in the other direction.
    async fn verify_schema(&self, table: &str) -> Result<()> {
        let Some(stored) = self.cold.stored_schema(table).await? else {
            return Ok(());
        };
        let configured = crate::encode::schema::storage_schema(&self.config.extra_columns());
        crate::evolution::compare(&configured, &stored).require_safe(table)
    }

    /// Reclaim partitions left detached by an interrupted run.
    ///
    /// A partition is only orphaned *after* its cold commit succeeded, so the
    /// data is already durable and the partition can simply be dropped. The
    /// watermark tells us so: anything wholly below it is committed.
    async fn reclaim_orphans(&self, table: &str) -> Result<usize> {
        let orphans = self.hot.orphaned_partitions(table).await?;
        if orphans.is_empty() {
            return Ok(0);
        }

        let watermark = self.cold.watermark(table).await?;
        let mut reclaimed = 0;

        for partition in orphans {
            if partition.start() < watermark.get() {
                warn!(
                    table,
                    partition = %partition.relation_name()?,
                    "reclaiming orphaned partition from an interrupted run"
                );
                self.hot.drop_partition(&partition).await?;
                reclaimed += 1;
            } else {
                // Detached but *not* covered by the watermark: the previous run
                // died before committing. Dropping would lose data, so this
                // needs an operator, not a retry.
                return Err(Error::InvariantViolated {
                    table: table.to_string(),
                    detail: format!(
                        "partition {} is detached but not covered by watermark {watermark}; \
                         it must be re-attached before archival can continue",
                        partition.relation_name()?
                    ),
                });
            }
        }

        Ok(reclaimed)
    }

    /// Archive until nothing further is due, up to `max_windows`.
    pub async fn catch_up(
        &self,
        now: OffsetDateTime,
        max_windows: usize,
    ) -> Result<Vec<ArchivalOutcome>> {
        let mut outcomes = Vec::new();
        for _ in 0..max_windows {
            let outcome = self.run_once(now).await?;
            let done = !outcome.archived_anything();
            outcomes.push(outcome);
            if done {
                break;
            }
        }
        Ok(outcomes)
    }

    /// Assert that the hot store holds exactly the rows at or above the
    /// watermark.
    pub async fn verify_invariant(&self) -> Result<()> {
        let table = self.config.name();
        let watermark = self.cold.watermark(table).await?;
        let violations = self.hot.invariant_violations(table, watermark).await?;
        if violations > 0 {
            return Err(Error::InvariantViolated {
                table: table.to_string(),
                detail: format!(
                    "{violations} rows are in the wrong tier for watermark {watermark}"
                ),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::RecordBatch;
    use crate::config::TableConfig;
    use crate::tiering::store::CommitInfo;
    use crate::tiering::store::ScanSpec;

    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use time::Duration;
    use time::macros::datetime;

    /// Where a fake store should fail, to exercise each crash window.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    enum FailAt {
        #[default]
        Never,
        AfterDetach,
        AfterColdCommit,
    }

    #[derive(Default)]
    struct FakeHotInner {
        /// start -> row count, for live partitions
        live: BTreeMap<OffsetDateTime, u64>,
        detached: BTreeMap<OffsetDateTime, u64>,
        dropped: Vec<OffsetDateTime>,
        created: Vec<OffsetDateTime>,
    }

    struct FakeHot {
        inner: Mutex<FakeHotInner>,
        fail_at: FailAt,
        /// Stands in for another process already holding the archive lease.
        lease_held_elsewhere: bool,
        /// Whether this store can answer `partition_starts`.
        enumerates_partitions: bool,
    }

    impl FakeHot {
        fn with_rows(rows: &[(OffsetDateTime, u64)]) -> Self {
            let mut inner = FakeHotInner::default();
            for (start, n) in rows {
                inner.live.insert(*start, *n);
            }
            Self {
                inner: Mutex::new(inner),
                fail_at: FailAt::Never,
                lease_held_elsewhere: false,
                enumerates_partitions: true,
            }
        }

        fn failing(mut self, at: FailAt) -> Self {
            self.fail_at = at;
            self
        }

        fn lease_held_elsewhere(mut self) -> Self {
            self.lease_held_elsewhere = true;
            self
        }

        /// A store that cannot enumerate its partitions, like a third-party
        /// `HotStore` that takes the trait's default.
        fn without_partition_listing(mut self) -> Self {
            self.enumerates_partitions = false;
            self
        }

        fn dropped(&self) -> Vec<OffsetDateTime> {
            self.inner.lock().unwrap().dropped.clone()
        }

        fn detached_starts(&self) -> Vec<OffsetDateTime> {
            self.inner
                .lock()
                .unwrap()
                .detached
                .keys()
                .copied()
                .collect()
        }
    }

    #[async_trait]
    impl HotStore for FakeHot {
        async fn append_reporting(
            &self,
            _table: &str,
            _merge_key: &[String],
            _batches: &[RecordBatch],
        ) -> Result<Vec<crate::session::Displacement>> {
            Ok(Vec::new())
        }

        async fn drop_table(&self, _table: &str) -> Result<()> {
            Ok(())
        }

        async fn try_archive_lease(
            &self,
            _table: &str,
        ) -> Result<Option<Box<dyn crate::tiering::store::ArchiveLease>>> {
            Ok(if self.lease_held_elsewhere {
                None
            } else {
                Some(Box::new(crate::tiering::store::UnenforcedLease))
            })
        }

        async fn create_tables(
            &self,
            _table: &str,
            _key: &[String],
            _extra: &[crate::arrow::datatypes::Field],
        ) -> Result<()> {
            Ok(())
        }

        async fn append(&self, _t: &str, _k: &[String], batches: &[RecordBatch]) -> Result<u64> {
            Ok(batches.iter().map(|b| b.num_rows() as u64).sum())
        }

        async fn ensure_partitions(
            &self,
            table: &str,
            from: OffsetDateTime,
            until: OffsetDateTime,
            step: Duration,
        ) -> Result<Vec<PartitionId>> {
            let mut inner = self.inner.lock().unwrap();
            let mut made = Vec::new();
            let mut t = from;
            while t < until {
                if let std::collections::btree_map::Entry::Vacant(e) = inner.live.entry(t) {
                    e.insert(0);
                    inner.created.push(t);
                    made.push(PartitionId::new(table, t));
                }
                t += step;
            }
            Ok(made)
        }

        async fn scan_range(
            &self,
            _table: &str,
            range: crate::planner::TimeRange,
            _spec: &ScanSpec,
        ) -> Result<crate::tiering::store::BatchStream> {
            let rows: u64 = {
                let inner = self.inner.lock().unwrap();
                inner
                    .live
                    .iter()
                    .filter(|(start, _)| {
                        range.start().is_none_or(|s| **start >= s)
                            && range.end().is_none_or(|e| **start < e)
                    })
                    .map(|(_, n)| *n)
                    .sum()
            };
            Ok(if rows == 0 {
                Box::pin(futures::stream::empty())
            } else {
                let b = fake_batch(rows);
                Box::pin(futures::stream::once(async move { Ok(b) }))
            })
        }

        async fn partition_exists(&self, partition: &PartitionId) -> Result<bool> {
            let inner = self.inner.lock().unwrap();
            Ok(inner.live.contains_key(&partition.start())
                || inner.detached.contains_key(&partition.start()))
        }

        async fn partition_starts(&self, _table: &str) -> Result<Option<Vec<OffsetDateTime>>> {
            if !self.enumerates_partitions {
                return Ok(None);
            }
            let inner = self.inner.lock().unwrap();
            let mut starts: Vec<OffsetDateTime> = inner
                .live
                .keys()
                .chain(inner.detached.keys())
                .copied()
                .collect();
            starts.sort();
            starts.dedup();
            Ok(Some(starts))
        }

        async fn detach_partition(&self, partition: &PartitionId) -> Result<()> {
            let mut inner = self.inner.lock().unwrap();
            let rows = inner.live.remove(&partition.start()).unwrap_or(0);
            inner.detached.insert(partition.start(), rows);
            if self.fail_at == FailAt::AfterDetach {
                return Err(Error::config("injected failure after detach"));
            }
            Ok(())
        }

        async fn scan_detached(
            &self,
            partition: &PartitionId,
            _spec: &ScanSpec,
        ) -> Result<crate::tiering::store::BatchStream> {
            let inner = self.inner.lock().unwrap();
            let rows = *inner
                .detached
                .get(&partition.start())
                .ok_or_else(|| Error::config("scan of a partition that is not detached"))?;
            Ok(crate::tiering::store::stream_of(if rows == 0 {
                vec![]
            } else {
                vec![fake_batch(rows)]
            }))
        }

        async fn drop_partition(&self, partition: &PartitionId) -> Result<()> {
            let mut inner = self.inner.lock().unwrap();
            inner.detached.remove(&partition.start());
            inner.dropped.push(partition.start());
            Ok(())
        }

        async fn orphaned_partitions(&self, table: &str) -> Result<Vec<PartitionId>> {
            let inner = self.inner.lock().unwrap();
            Ok(inner
                .detached
                .keys()
                .map(|s| PartitionId::new(table, *s))
                .collect())
        }

        async fn invariant_violations(
            &self,
            _table: &str,
            watermark: TieringWatermark,
        ) -> Result<u64> {
            let inner = self.inner.lock().unwrap();
            Ok(inner
                .live
                .iter()
                .filter(|(start, rows)| **start < watermark.get() && **rows > 0)
                .count() as u64)
        }
    }

    struct FakeCold {
        watermark: Mutex<TieringWatermark>,
        committed: Mutex<Vec<(ArchivalWindow, u64)>>,
        fail_at: FailAt,
    }

    impl FakeCold {
        fn new(watermark: OffsetDateTime) -> Self {
            Self {
                watermark: Mutex::new(TieringWatermark::new(watermark)),
                committed: Mutex::new(Vec::new()),
                fail_at: FailAt::Never,
            }
        }

        fn failing(mut self, at: FailAt) -> Self {
            self.fail_at = at;
            self
        }

        fn commits(&self) -> Vec<(ArchivalWindow, u64)> {
            self.committed.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ColdStore for FakeCold {
        async fn purge_table(&self, _table: &str) -> Result<()> {
            Ok(())
        }

        async fn create_tables(
            &self,
            _table: &str,
            _identity: &[String],
            _extra: &[crate::arrow::datatypes::Field],
        ) -> Result<()> {
            Ok(())
        }

        async fn watermark(&self, _table: &str) -> Result<TieringWatermark> {
            Ok(*self.watermark.lock().unwrap())
        }

        async fn append_and_commit(
            &self,
            _table: &str,
            batches: crate::tiering::store::BatchStream,
            _hints: WriteHints,
            window: ArchivalWindow,
        ) -> Result<CommitInfo> {
            let rows = drain(batches).await?;
            // Atomic: rows and watermark land together.
            *self.watermark.lock().unwrap() = window.resulting_watermark();
            self.committed.lock().unwrap().push((window, rows));

            if self.fail_at == FailAt::AfterColdCommit {
                return Err(Error::config("injected failure after cold commit"));
            }
            Ok(CommitInfo {
                snapshot_id: 1,
                rows,
                watermark: window.resulting_watermark(),
            })
        }

        async fn expire_snapshots(
            &self,
            _t: &str,
            _retain_for: time::Duration,
            _retain_last: usize,
            _now: OffsetDateTime,
        ) -> Result<usize> {
            Ok(0)
        }

        async fn append_only(
            &self,
            _table: &str,
            batches: crate::tiering::store::BatchStream,
            _hints: WriteHints,
        ) -> Result<CommitInfo> {
            let rows = drain(batches).await?;
            Ok(CommitInfo {
                snapshot_id: 2,
                rows,
                watermark: *self.watermark.lock().unwrap(),
            })
        }
    }

    /// Consume a stream, returning the row count — what a real cold store does
    /// on the way to writing Parquet.
    async fn drain(batches: crate::tiering::store::BatchStream) -> Result<u64> {
        use futures::StreamExt;
        let mut stream = batches;
        let mut rows = 0u64;
        while let Some(batch) = stream.next().await {
            rows += batch?.num_rows() as u64;
        }
        Ok(rows)
    }

    fn fake_batch(rows: u64) -> RecordBatch {
        use crate::arrow::array::StringArray;
        use std::sync::Arc;
        let schema = crate::encode::schema::storage_schema(&[]);
        let n = rows as usize;
        // Only the row count matters to the archiver; build a minimal valid batch.
        RecordBatch::try_new(
            schema.clone(),
            schema
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    crate::arrow::datatypes::DataType::Utf8 => {
                        Arc::new(StringArray::from(vec!["x"; n])) as _
                    }
                    crate::arrow::datatypes::DataType::UInt8 => {
                        Arc::new(crate::arrow::array::UInt8Array::from(vec![0u8; n])) as _
                    }
                    crate::arrow::datatypes::DataType::Timestamp(_, _) => Arc::new(
                        crate::arrow::array::TimestampMicrosecondArray::from(vec![0i64; n])
                            .with_timezone("UTC"),
                    )
                        as _,
                    crate::arrow::datatypes::DataType::Decimal128(p, s) => Arc::new(
                        crate::arrow::array::Decimal128Array::from(vec![0i128; n])
                            .with_precision_and_scale(*p, *s)
                            .unwrap(),
                    )
                        as _,
                    other => panic!("unhandled type {other:?}"),
                })
                .collect(),
        )
        .unwrap()
    }

    fn config() -> ValidatedTableConfig {
        TableConfig::new("readings")
            .settlement_lag(Duration::days(7))
            .build()
            .unwrap()
    }

    const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
    const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);

    #[tokio::test]
    async fn archives_one_closed_window() {
        let hot = FakeHot::with_rows(&[(D20, 96)]);
        let cold = FakeCold::new(D20);
        let archiver = Archiver::new(hot, cold, config());

        let out = archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap();

        assert!(out.archived_anything());
        assert_eq!(out.rows, 96);
        assert_eq!(out.watermark.get(), D21);
        assert_eq!(archiver.hot.dropped(), vec![D20]);
        assert_eq!(archiver.cold.commits().len(), 1);
    }

    #[tokio::test]
    async fn does_nothing_when_no_window_is_closed() {
        // Watermark is recent, so the next window still lies inside the lag.
        let hot = FakeHot::with_rows(&[(D20, 96)]);
        let cold = FakeCold::new(D20);
        let archiver = Archiver::new(hot, cold, config());

        let out = archiver
            .run_once(datetime!(2026-07-22 00:00 UTC))
            .await
            .unwrap();

        assert!(!out.archived_anything());
        assert_eq!(out.rows, 0);
        assert!(archiver.hot.dropped().is_empty());
        assert!(
            archiver.cold.commits().is_empty(),
            "nothing may be committed"
        );
    }

    #[tokio::test]
    async fn commits_cold_before_dropping_hot() {
        // Injected failure immediately after the cold commit: the partition must
        // still exist, detached, so no data is lost.
        let hot = FakeHot::with_rows(&[(D20, 96)]);
        let cold = FakeCold::new(D20).failing(FailAt::AfterColdCommit);
        let archiver = Archiver::new(hot, cold, config());

        assert!(
            archiver
                .run_once(datetime!(2026-07-30 00:00 UTC))
                .await
                .is_err()
        );

        assert!(
            archiver.hot.dropped().is_empty(),
            "must not drop after a failed commit"
        );
        assert_eq!(
            archiver.hot.detached_starts(),
            vec![D20],
            "partition must survive, detached and intact"
        );
    }

    #[tokio::test]
    async fn reclaims_an_orphan_left_by_an_interrupted_run() {
        // Reproduce the crash above, then run again.
        let hot = FakeHot::with_rows(&[(D20, 96)]);
        let cold = FakeCold::new(D20).failing(FailAt::AfterColdCommit);
        let archiver = Archiver::new(hot, cold, config());
        let _ = archiver.run_once(datetime!(2026-07-30 00:00 UTC)).await;

        // The cold commit did land, so the watermark advanced past the orphan.
        let hot = FakeHot {
            inner: Mutex::new(archiver.hot.inner.into_inner().unwrap()),
            fail_at: FailAt::Never,
            lease_held_elsewhere: false,
            enumerates_partitions: true,
        };
        let cold = FakeCold {
            watermark: Mutex::new(*archiver.cold.watermark.lock().unwrap()),
            committed: Mutex::new(archiver.cold.commits()),
            fail_at: FailAt::Never,
        };
        let archiver = Archiver::new(hot, cold, config());

        let out = archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap();

        assert_eq!(out.orphans_reclaimed, 1);
        assert!(archiver.hot.dropped().contains(&D20));
        assert!(archiver.hot.detached_starts().is_empty());
    }

    #[tokio::test]
    async fn refuses_to_drop_an_orphan_the_watermark_does_not_cover() {
        // Detached but never committed: dropping would lose data, so this needs
        // an operator rather than a silent retry.
        let hot = FakeHot::with_rows(&[(D20, 96)]).failing(FailAt::AfterDetach);
        let cold = FakeCold::new(D20);
        let archiver = Archiver::new(hot, cold, config());
        let _ = archiver.run_once(datetime!(2026-07-30 00:00 UTC)).await;

        let hot = FakeHot {
            inner: Mutex::new(archiver.hot.inner.into_inner().unwrap()),
            fail_at: FailAt::Never,
            lease_held_elsewhere: false,
            enumerates_partitions: true,
        };
        let archiver = Archiver::new(hot, FakeCold::new(D20), config());

        let err = archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvariantViolated { .. }));
        assert!(
            archiver.hot.dropped().is_empty(),
            "must not drop uncommitted data"
        );
    }

    #[tokio::test]
    async fn archiving_is_idempotent_across_repeated_runs() {
        let hot = FakeHot::with_rows(&[(D20, 96), (D21, 96)]);
        let cold = FakeCold::new(D20);
        let archiver = Archiver::new(hot, cold, config());
        let now = datetime!(2026-07-30 00:00 UTC);

        let first = archiver.catch_up(now, 10).await.unwrap();
        let archived: u64 = first.iter().map(|o| o.rows).sum();

        // Three windows are due (07-20, 07-21, 07-22); only two hold rows.
        assert_eq!(archived, 192);
        assert_eq!(archiver.cold.commits().len(), 3);

        // Running again must move nothing and must not double-count.
        let second = archiver.catch_up(now, 10).await.unwrap();
        assert!(second.iter().all(|o| !o.archived_anything()));
        assert_eq!(
            archiver.cold.commits().len(),
            3,
            "no window may be re-archived"
        );
    }

    #[tokio::test]
    async fn an_empty_stretch_is_crossed_in_one_commit() {
        // A day with no readings is normal — a meter can simply not report — and
        // an empty window must still advance the watermark, or archival stalls on
        // the gap forever and the hot tier grows without bound.
        //
        // But it must not advance one step at a time: nothing is there, so the
        // whole stretch up to the archival horizon is one commit.
        let hot = FakeHot::with_rows(&[]);
        let cold = FakeCold::new(D20);
        let archiver = Archiver::new(hot, cold, config());

        let out = archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap();

        assert!(out.archived_anything());
        assert_eq!(out.rows, 0);
        // now - settlement_lag, not one step past the old boundary.
        assert_eq!(out.watermark.get(), datetime!(2026-07-23 00:00 UTC));
        assert_eq!(archiver.cold.commits().len(), 1);
    }

    #[tokio::test]
    async fn a_table_that_has_never_archived_reaches_the_present_in_one_commit() {
        // The first run of a fresh deployment, which is the case this exists for.
        // With no snapshot the watermark is the Unix epoch, so stepping one day
        // at a time would mean ~20 600 empty Iceberg commits — days of catch-up
        // and a snapshot list that never recovers — before a single real row.
        let hot = FakeHot::with_rows(&[(D20, 96)]);
        let cold = FakeCold::new(OffsetDateTime::UNIX_EPOCH);
        let archiver = Archiver::new(hot, cold, config());

        let out = archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap();

        assert_eq!(archiver.cold.commits().len(), 1, "one commit, not 20 600");
        assert_eq!(out.rows, 0, "the skipped stretch holds nothing");
        // Stops exactly at the first partition that does hold rows, so the next
        // run archives it normally rather than skipping over it.
        assert_eq!(out.watermark.get(), D20);

        let next = archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap();
        assert_eq!(next.rows, 96);
        assert_eq!(next.watermark.get(), D21);
    }

    #[tokio::test]
    async fn the_skip_never_reaches_into_the_settlement_lag() {
        // The whole point of the lag: a window newer than the horizon is still
        // receiving corrections, so the boundary must not pass it however empty
        // the range looks.
        let hot = FakeHot::with_rows(&[]);
        let cold = FakeCold::new(OffsetDateTime::UNIX_EPOCH);
        let archiver = Archiver::new(hot, cold, config());

        let now = datetime!(2026-07-30 12:00 UTC);
        let out = archiver.run_once(now).await.unwrap();

        assert!(out.watermark.get() <= now - Duration::days(7));
        // Still a whole multiple of the step, measured from the epoch, so a
        // window keeps mapping to exactly one partition.
        assert_eq!(
            out.watermark.get(),
            crate::watermark::align_to_step(out.watermark.get(), Duration::DAY)
        );
    }

    #[tokio::test]
    async fn a_store_that_cannot_list_partitions_keeps_stepping() {
        // `partition_starts` returning `None` means "cannot say", which must not
        // be read as "no partitions anywhere" — that would advance the boundary
        // over rows the store simply could not describe.
        let hot = FakeHot::with_rows(&[]).without_partition_listing();
        let cold = FakeCold::new(D20);
        let archiver = Archiver::new(hot, cold, config());

        let out = archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap();

        assert_eq!(out.watermark.get(), D21, "one step, as before");
    }

    #[tokio::test]
    async fn catch_up_advances_gaplessly() {
        let hot = FakeHot::with_rows(&[(D20, 10), (D21, 20)]);
        let cold = FakeCold::new(D20);
        let archiver = Archiver::new(hot, cold, config());

        archiver
            .catch_up(datetime!(2026-07-30 00:00 UTC), 10)
            .await
            .unwrap();

        let commits = archiver.cold.commits();
        for pair in commits.windows(2) {
            assert_eq!(pair[0].0.to(), pair[1].0.from(), "windows must be gapless");
        }
    }

    #[tokio::test]
    async fn pre_creates_partitions_even_on_an_idle_cycle() {
        // Running out of partitions makes inserts fail, so this must not be
        // conditional on there being work to archive.
        let hot = FakeHot::with_rows(&[]);
        let cold = FakeCold::new(datetime!(2026-07-29 00:00 UTC));
        let archiver = Archiver::new(hot, cold, config());

        let out = archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap();

        assert!(!out.archived_anything());
        assert!(out.partitions_created > 0, "headroom must be maintained");
    }

    #[tokio::test]
    async fn detects_rows_stranded_in_the_wrong_tier() {
        let hot = FakeHot::with_rows(&[(D20, 96)]);
        let cold = FakeCold::new(D21); // claims D20 is already cold
        let archiver = Archiver::new(hot, cold, config());

        let err = archiver.verify_invariant().await.unwrap_err();
        assert!(matches!(err, Error::InvariantViolated { .. }));
    }

    #[tokio::test]
    async fn a_contended_lease_makes_the_run_a_no_op() {
        // Exactly one archiver per table (§5.2). A second replica running the
        // same schedule must discover it has nothing to do — not block, not
        // fail, and above all not detach a partition the first one owns.
        let hot = FakeHot::with_rows(&[(D20, 96)]).lease_held_elsewhere();
        let cold = FakeCold::new(D20);
        let archiver = Archiver::new(hot, cold, config());

        let out = archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap();

        assert!(out.lease_contended);
        assert!(!out.archived_anything());
        assert_eq!(out.watermark.get(), D20, "the boundary must not move");
        assert!(archiver.hot.detached_starts().is_empty());
        assert!(archiver.hot.dropped().is_empty());
        assert!(archiver.cold.commits().is_empty());
    }

    #[tokio::test]
    async fn a_granted_lease_is_reported_as_uncontended() {
        let hot = FakeHot::with_rows(&[(D20, 96)]);
        let cold = FakeCold::new(D20);
        let archiver = Archiver::new(hot, cold, config());

        let out = archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap();
        assert!(!out.lease_contended);
    }

    #[tokio::test]
    async fn invariant_holds_after_a_clean_archival() {
        let hot = FakeHot::with_rows(&[(D20, 96)]);
        let cold = FakeCold::new(D20);
        let archiver = Archiver::new(hot, cold, config());

        archiver
            .run_once(datetime!(2026-07-30 00:00 UTC))
            .await
            .unwrap();
        archiver.verify_invariant().await.unwrap();
    }
}
