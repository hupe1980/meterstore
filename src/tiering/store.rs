//! The two storage tiers, as traits.
//!
//! Both are defined here rather than beside their implementations so the
//! contract the archiver depends on is readable in one place — and so the
//! archiver can be tested against in-memory fakes without a database.

use async_trait::async_trait;
use time::OffsetDateTime;
use time::format_description::FormatItem;
use time::macros::format_description;

use futures::stream::BoxStream;

use crate::arrow::array::RecordBatch;
use crate::error::{Error, Result};
use crate::planner::TimeRange;
use crate::watermark::{ArchivalWindow, TieringWatermark};

/// A stream of record batches, owned so it can outlive the call that made it.
pub type BatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Present an already-materialised set of batches as a stream.
///
/// For the paths where the rows genuinely are in memory — a correction batch
/// being appended, a test fixture — so that the streaming interface does not
/// force a second shape on callers that do not need one.
pub fn stream_of(batches: Vec<RecordBatch>) -> BatchStream {
    Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
}

/// What a chunked hot-tier scan needs to know about a table's shape.
///
/// The two halves travel together rather than as loose slices because they are
/// easy to transpose and the consequences differ: getting `extra` wrong drops
/// columns loudly, getting `merge_key` wrong drops **rows** silently.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanSpec {
    /// The table's merge key (§7.3).
    merge_key: Vec<String>,
    /// Deployment columns to select alongside the core schema.
    extra: Vec<String>,
    /// Rows per round trip, or `None` for the store's own default.
    chunk_rows: Option<usize>,
    /// The partition granularity, or `None` if the caller did not say.
    ///
    /// A scan that knows the step knows a detached partition's exclusive end
    /// from its start, and can therefore skip one that cannot hold a row in
    /// range. Without it such a partition is scanned and returns nothing —
    /// correct, and wasted.
    partition_step: Option<time::Duration>,
}

impl ScanSpec {
    /// A spec for a table with the given merge key and deployment columns.
    pub fn new(merge_key: Vec<String>, extra: Vec<String>) -> Self {
        Self {
            merge_key,
            extra,
            chunk_rows: None,
            partition_step: None,
        }
    }

    /// Rows to fetch per round trip.
    ///
    /// **Rows, not measuring points.** An earlier setting bounded a chunk by
    /// `malo_id` range, which does not bound anything useful: meters differ by
    /// orders of magnitude in how much they report, so a fixed number of them is
    /// a variable amount of memory. A row count bounds the thing that actually
    /// has to fit.
    pub fn with_chunk_rows(mut self, rows: usize) -> Self {
        self.chunk_rows = Some(rows.max(1));
        self
    }

    /// The configured chunk size, if the caller set one.
    pub fn chunk_rows(&self) -> Option<usize> {
        self.chunk_rows
    }

    /// Declare the partition granularity of the table being scanned.
    pub fn with_partition_step(mut self, step: time::Duration) -> Self {
        self.partition_step = (step > time::Duration::ZERO).then_some(step);
        self
    }

    /// The partition granularity, if the caller declared one.
    pub fn partition_step(&self) -> Option<time::Duration> {
        self.partition_step
    }

    /// A spec for a table with no deployment columns.
    pub fn core() -> Self {
        Self::new(
            crate::encode::schema::MERGE_KEY
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            Vec::new(),
        )
    }

    /// The merge key.
    pub fn merge_key(&self) -> &[String] {
        &self.merge_key
    }

    /// The deployment columns to select.
    pub fn extra(&self) -> &[String] {
        &self.extra
    }

    /// The column tuple a chunked scan orders by and resumes from.
    ///
    /// Two properties, and the scan is wrong without either.
    ///
    /// **It must be unique per row.** A keyset cursor resumes at *strictly
    /// greater than* the last row it saw, so a chunk boundary landing inside a
    /// group of rows sharing the cursor silently discards the rest of that
    /// group. `(malo_id, from)` looks like a key and is not: the same measuring
    /// point and interval carries one row per OBIS channel, per identity column
    /// value, and per correction version. The hot table's primary key —
    /// the merge key plus `version` — is the narrowest tuple that is actually
    /// unique.
    ///
    /// **Its prefix must be the declared sort order.** Archived files carry
    /// `sorting_columns = (malo_id, from)` in the Parquet footer (§10.2), and a
    /// reader is entitled to trust it. Ordering by `(malo_id, obis_code, from,
    /// …)` would still be unique but would make that declaration false, so the
    /// remaining key columns are appended *after* `(malo_id, from)` rather than
    /// interleaved with it.
    ///
    /// The two orderings agree only narrowly: PostgreSQL sorts `malo_id` under
    /// the database collation, Parquet declares byte order, and eleven ASCII
    /// digits are a set every collation orders identically. A text column that
    /// could hold anything else must stay *out* of the declared prefix — which is
    /// why an identity column joins the cursor after it, never inside it.
    pub fn cursor_columns(&self) -> Vec<String> {
        let mut columns: Vec<String> = crate::encode::schema::SORT_COLUMNS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        for column in &self.merge_key {
            if !columns.contains(column) {
                columns.push(column.clone());
            }
        }
        columns.push(crate::encode::schema::col::VERSION.to_string());
        columns
    }
}

/// How many partitions can still hold a row written now or later.
///
/// **This is the runway before inserts start failing**, and it has to be counted
/// from the partitions that exist rather than derived from configuration. A
/// figure computed as `settlement_lag + headroom / step` is a constant: it says
/// what the runway *should* be, is unaffected by an archiver that stopped
/// running, and can therefore never reach zero — which makes it worthless as the
/// leading indicator for the one failure that stops writes outright.
///
/// A partition starting at or after the aligned current instant can hold a row
/// with `from >= now`. Zero means the next insert has nowhere to land.
pub fn partitions_ahead(
    starts: &[OffsetDateTime],
    now: OffsetDateTime,
    step: time::Duration,
) -> usize {
    // An instant that cannot be aligned is one no partition could ever hold, so
    // the honest count is zero — the same reading as "the frontier is exhausted",
    // which is what `partitions_ahead` is alerted on.
    let Ok(frontier) = crate::watermark::align_to_step(now, step) else {
        return 0;
    };
    starts.iter().filter(|start| **start >= frontier).count()
}

/// What the cold-tier writer needs to know before it sees any data.
///
/// A streaming write cannot look at the whole batch first, so anything the
/// Parquet writer must decide up front has to arrive separately.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteHints {
    /// Distinct `malo_id` values in what is about to be written.
    ///
    /// Sizes the bloom filter on the column §10.2 calls the highest-leverage
    /// one. `None` means unknown, and the writer falls back to a conservative
    /// default. Neither direction is a correctness matter — over-sizing costs
    /// metadata bytes, under-sizing costs false positives and therefore row
    /// groups read — which is why an estimate is acceptable here and a full
    /// materialisation to get an exact count would not be worth it.
    pub distinct_malo_ids: Option<u64>,
}

/// Identifies one time-range partition of the hot table.
///
/// Partitions are named from their lower bound, so the name is derivable from a
/// window and never needs to be looked up.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId {
    table: String,
    start: OffsetDateTime,
}

const PARTITION_SUFFIX: &[FormatItem<'_>] =
    format_description!("[year]_[month]_[day]_[hour][minute]");

impl PartitionId {
    /// Build the identifier for the partition starting at `start`.
    pub fn new(table: impl Into<String>, start: OffsetDateTime) -> Self {
        Self {
            table: table.into(),
            start,
        }
    }

    /// The partition covering a window's lower bound.
    pub fn for_window(table: impl Into<String>, window: ArchivalWindow) -> Self {
        Self::new(table, window.from())
    }

    /// The parent table.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The partition's inclusive lower bound.
    pub fn start(&self) -> OffsetDateTime {
        self.start
    }

    /// The partition's exclusive upper bound, given the granularity it was
    /// created at.
    ///
    /// The identifier records only the start, because that is what the relation
    /// name carries; the step comes from the table's configuration and is the
    /// same for every partition of it.
    pub fn end(&self, step: time::Duration) -> OffsetDateTime {
        self.start + step
    }

    /// Recover a partition identifier from a physical relation name.
    ///
    /// Inverse of [`PartitionId::relation_name`]. Used to rediscover partitions
    /// left behind by an interrupted run, where the only record of them is the
    /// catalog.
    pub fn from_relation_name(table: &str, relation: &str) -> Result<Self> {
        let suffix = relation
            .strip_prefix(table)
            .and_then(|r| r.strip_prefix('_'))
            .ok_or_else(|| {
                Error::decode(
                    "partition name",
                    format!("{relation:?} is not a partition of {table:?}"),
                )
            })?;
        let start = time::PrimitiveDateTime::parse(suffix, PARTITION_SUFFIX)
            .map_err(|e| Error::decode("partition name", format!("{suffix:?}: {e}")))?
            .assume_utc();
        Ok(Self {
            table: table.to_string(),
            start,
        })
    }

    /// The physical relation name, e.g. `readings_2026_07_20_0000`.
    ///
    /// Includes hour and minute so sub-daily partition steps do not collide.
    pub fn relation_name(&self) -> Result<String> {
        let suffix = self
            .start
            .format(PARTITION_SUFFIX)
            .map_err(|e| Error::encode("partition name", e.to_string()))?;
        Ok(format!("{}_{suffix}", self.table))
    }
}

/// An exclusive claim on one table, for one purpose, held for the duration of a
/// run.
///
/// Two purposes take one: archiving ([`HotStore::try_archive_lease`]) and
/// appending a late correction to the cold tier
/// ([`HotStore::cold_append_lease`]). Both are states where a second writer
/// would produce rows nothing downstream could detect.
///
/// Held as a guard rather than checked as a flag, so the claim cannot outlive
/// the run that took it. Release is explicit because `Drop` cannot await; a
/// process that dies without releasing loses its session, and a session-scoped
/// lock dies with it.
#[async_trait]
pub trait TableLease: Send + Sync + std::fmt::Debug {
    /// Give the claim up.
    async fn release(self: Box<Self>) -> Result<()>;
}

/// The hot tier: PostgreSQL, time-partitioned, holding `from >= watermark`.
#[async_trait]
pub trait HotStore: Send + Sync {
    /// Try to claim the right to archive `table`, without waiting.
    ///
    /// `Ok(None)` means another process holds the claim, which is an ordinary
    /// outcome for a scheduled job in a replicated deployment: the other process
    /// is doing the work, and this one should do nothing rather than block a
    /// worker or fail an alert.
    ///
    /// The default grants an **unenforced** lease and says so, because a store
    /// with no cross-process locking cannot honour the constraint — and quietly
    /// pretending to would be worse than a deployment that knows it runs one
    /// archiver. [`PostgresHot`] enforces it with a session-scoped advisory lock.
    ///
    /// [`PostgresHot`]: crate::hot::PostgresHot
    async fn try_archive_lease(&self, _table: &str) -> Result<Option<Box<dyn TableLease>>> {
        Ok(Some(Box::new(UnenforcedLease)))
    }

    /// Claim the right to append a late correction to `table`'s **cold** tier,
    /// waiting if another process holds it.
    ///
    /// The hot tier gets exclusion from its primary key. Iceberg has none, so
    /// two processes appending the same late correction both read no existing
    /// row, both write, and the reading is stored twice at one version —
    /// which version resolution cannot collapse, and which a historical scan
    /// then returns twice because the files provably hold a single version.
    ///
    /// **Waits**, unlike [`try_archive_lease`](Self::try_archive_lease): a
    /// second archiver has nothing to do, but a second correction has a write to
    /// land. The wait is bounded and late corrections are rare, so contention is
    /// brief.
    ///
    /// The default grants an **unenforced** lease and says so, for the same
    /// reason `try_archive_lease` does.
    async fn cold_append_lease(&self, _table: &str) -> Result<Box<dyn TableLease>> {
        Ok(Box::new(UnenforcedLease))
    }

    /// Ensure partitions exist from `from` through `until`.
    ///
    /// Called before writes, not only before archival: running out of
    /// pre-created partitions makes inserts fail outright.
    async fn ensure_partitions(
        &self,
        table: &str,
        from: OffsetDateTime,
        until: OffsetDateTime,
        step: time::Duration,
    ) -> Result<Vec<PartitionId>>;

    /// Create the table, its primary key, and any deployment columns.
    ///
    /// `time_model` decides two pieces of DDL and nothing else: whether `to` is
    /// `NOT NULL` with a forward check, and whether the partition carries the
    /// overlap exclusion. Instants cannot overlap, and a
    /// [`Point`](crate::config::TimeModel::Point) table has no span to check.
    async fn create_tables(
        &self,
        table: &str,
        merge_key: &[String],
        extra: &[crate::arrow::datatypes::Field],
        time_model: crate::config::TimeModel,
    ) -> Result<()>;

    /// Append rows to the live table.
    ///
    /// Every row must satisfy `from >= watermark`; the caller routes. Writing
    /// below the watermark would place a row in a tier no query reads it from,
    /// which is silent data loss rather than an error.
    ///
    /// `merge_key` must be the same key the table was created with — it is the
    /// conflict target that makes a redelivery idempotent, and a mismatch is
    /// rejected by the database rather than silently inserting a duplicate.
    async fn append(
        &self,
        table: &str,
        merge_key: &[String],
        batches: &[RecordBatch],
    ) -> Result<u64>;

    /// Scan the live table over a half-open interval range.
    ///
    /// Unlike [`HotStore::scan_detached`], this reads the attached table and is
    /// the path a query takes. `range` is the hot half of a tier split, so it
    /// never reaches below the watermark.
    ///
    /// Returns a **stream**, not a collection: a query may span the whole hot
    /// window, and materialising it before the first row reaches the engine
    /// would bound throughput by memory rather than by the query.
    async fn scan_range(
        &self,
        table: &str,
        range: TimeRange,
        spec: &ScanSpec,
    ) -> Result<BatchStream>;

    /// Whether a partition relation exists, attached or detached.
    async fn partition_exists(&self, partition: &PartitionId) -> Result<bool>;

    /// Every partition lower bound this store holds for `table`, ascending,
    /// attached or detached.
    ///
    /// The archiver uses it to step over a stretch that holds no partitions in
    /// **one** commit rather than one per window. That is not a micro-
    /// optimisation: a table that has never been archived reports the epoch
    /// watermark, so without it a fresh deployment commits one empty Iceberg
    /// snapshot per day since 1970 before it reaches any data.
    ///
    /// `None` means the store cannot enumerate them — deliberately distinct from
    /// `Some(vec![])`, which means it can and there are none. The archiver keeps
    /// the one-window-per-commit behaviour for the former and would otherwise
    /// read "no partitions anywhere" out of "cannot say".
    async fn partition_starts(&self, _table: &str) -> Result<Option<Vec<OffsetDateTime>>> {
        Ok(None)
    }

    /// Detach a partition, making it invisible to writers while remaining
    /// readable by the caller.
    ///
    /// Detaching before the scan is what stops a row being inserted into a
    /// partition that is mid-archival.
    async fn detach_partition(&self, partition: &PartitionId) -> Result<()>;

    /// Stream a detached partition's rows in the declared sort order.
    ///
    /// A **stream**, for the same reason `scan_range` is one and more urgently:
    /// a day at 100 k measuring points is ~9.6 M rows, and materialising a
    /// partition before writing any of it would make archival's peak memory
    /// proportional to the window rather than to the chunk size — which is the
    /// §18 budget it would blow first.
    async fn scan_detached(&self, partition: &PartitionId, spec: &ScanSpec) -> Result<BatchStream>;

    /// Distinct `malo_id` values in a detached partition, if the store can say.
    ///
    /// Feeds [`WriteHints::distinct_malo_ids`]. `None` is a legitimate answer —
    /// the writer then picks a default — so a store that cannot answer cheaply
    /// should not guess.
    async fn distinct_malo_ids(&self, _partition: &PartitionId) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Drop a detached partition.
    ///
    /// This is the purge, and it must remain `DROP TABLE`: a row-wise `DELETE`
    /// at metering volume produces millions of dead tuples per day and the
    /// vacuum debt that follows.
    async fn drop_partition(&self, partition: &PartitionId) -> Result<()>;

    /// Insert, and report what each row did to the value that was current.
    ///
    /// [`append`](Self::append) returns a count, which cannot distinguish a new
    /// reading from a correction from a backfill that changed nothing. A caller
    /// building an audit trail needs that distinction, and reading the prior
    /// state in a separate query races the write — wrong exactly when two
    /// corrections arrive together, which is when an audit trail matters.
    ///
    /// Implementations must take the prior state and the insert in **one
    /// transaction**, or the report is the race it exists to avoid.
    async fn append_reporting(
        &self,
        table: &str,
        merge_key: &[String],
        batches: &[RecordBatch],
    ) -> Result<Vec<crate::session::Displacement>>;

    /// Destroy the table and every partition of it.
    ///
    /// **This is not the purge.** [`drop_partition`](Self::drop_partition)
    /// reclaims space for rows already durable in the cold tier and destroys
    /// nothing. This destroys the hot half of a table outright, and is only
    /// correct as one step of decommissioning the whole table.
    async fn drop_table(&self, table: &str) -> Result<()>;

    /// Partitions that are detached but not yet dropped.
    ///
    /// Non-empty means a previous run died between the cold commit and the drop.
    /// The data is intact and invisible; the next run reclaims it.
    async fn orphaned_partitions(&self, table: &str) -> Result<Vec<PartitionId>>;

    /// Count rows the hot tier holds that belong to the cold one — those with
    /// `from < watermark`. Must be zero.
    ///
    /// **One direction, and that is the whole of what a hot store can answer.**
    /// The other half of the invariant — that every row at or above the watermark
    /// is here rather than in Iceberg — is not observable from this side: a hot
    /// store cannot see what the cold one holds. That half is established by the
    /// archiver's commit ordering instead, which never drops a partition until
    /// its rows are durable in the cold tier.
    async fn invariant_violations(&self, table: &str, watermark: TieringWatermark) -> Result<u64>;
}

/// The cold tier: Iceberg, holding `from < watermark`.
#[async_trait]
pub trait ColdStore: Send + Sync {
    /// Create the table with the deployment's declared extra columns.
    ///
    /// `identity` names the deployment's identity columns. The cold tier makes
    /// them the leading partition fields, so a tenant-scoped scan prunes at the
    /// manifest rather than by row filter.
    async fn create_tables(
        &self,
        table: &str,
        identity: &[String],
        extra: &[crate::arrow::datatypes::Field],
    ) -> Result<()>;

    /// Destroy the table, its metadata and its data files.
    ///
    /// The only way this crate deletes stored readings. Everything else is
    /// append-only (§4.2): a correction is a new version, erasure destroys a
    /// mapping rather than rows, and a hot partition drop reclaims space for
    /// rows that are already durable here.
    async fn purge_table(&self, table: &str) -> Result<()>;

    /// The durable watermark, read from the current snapshot's summary.
    ///
    /// Returns the epoch watermark when the table has no snapshots yet.
    async fn watermark(&self, table: &str) -> Result<TieringWatermark>;

    /// Append `batches` and advance the watermark, atomically.
    ///
    /// Atomicity is the whole contract: the rows and the watermark that
    /// describes them must become durable together, or a crash leaves a
    /// watermark claiming data that was never written.
    ///
    /// `now` is the caller's clock, recorded in the summary as
    /// [`ARCHIVED_AT_PROPERTY`](crate::watermark::ARCHIVED_AT_PROPERTY). It is
    /// what the reader grace is measured against, so that every archival
    /// decision reads the same clock rather than half of them reading Iceberg's.
    async fn append_and_commit(
        &self,
        table: &str,
        batches: BatchStream,
        hints: WriteHints,
        window: ArchivalWindow,
        now: OffsetDateTime,
    ) -> Result<CommitInfo>;

    /// Expire snapshots older than `retain_for`, keeping at least `retain_last`.
    async fn expire_snapshots(
        &self,
        table: &str,
        retain_for: time::Duration,
        retain_last: usize,
        now: OffsetDateTime,
    ) -> Result<usize>;

    /// Append rows without moving the watermark.
    ///
    /// Used for corrections to already-archived intervals, which cannot go to
    /// the hot tier without placing a row below the watermark.
    async fn append_only(
        &self,
        table: &str,
        batches: BatchStream,
        hints: WriteHints,
    ) -> Result<CommitInfo>;

    /// Put the tiering boundary back on the current snapshot.
    ///
    /// A commit from anything other than MeterStore — the out-of-band compaction
    /// §10.3.1 recommends — carries no watermark, so the boundary lookup has to
    /// walk back the parent chain to find one. That works, and it makes snapshot
    /// expiry dangerous: a hole anywhere in the chain strands the boundary and
    /// every query fails at once.
    ///
    /// Re-stamping republishes what the history already says, so it cannot move
    /// the boundary. `Ok(None)` when the current snapshot already carries it.
    /// The default is a no-op, for a store whose snapshots are not Iceberg's.
    async fn reassert_watermark(&self, _table: &str) -> Result<Option<CommitInfo>> {
        Ok(None)
    }

    /// Per-file statistics for the data files overlapping `range`.
    ///
    /// Feeds the decision to skip version resolution entirely, which needs two
    /// facts per file: the versions it holds, and the span of `from` it covers —
    /// files covering disjoint spans cannot share a merge key, so they are free
    /// to disagree about versions. See [`planner::version::plan`].
    ///
    /// The default is deliberately pessimistic — one file nothing is known about,
    /// which forces resolution — so a store that cannot supply statistics is slow
    /// rather than wrong.
    ///
    /// [`planner::version::plan`]: crate::planner::version::plan
    async fn version_stats(
        &self,
        _table: &str,
        _range: (OffsetDateTime, OffsetDateTime),
    ) -> Result<Vec<crate::planner::FileStats>> {
        Ok(vec![crate::planner::FileStats::unknown()])
    }

    /// A DataFusion provider pinned to a past state of the table.
    ///
    /// The read side of [`ReadMode::AsOf`]. The default **fails** rather than
    /// falling back to the current snapshot: a reproducible read that silently
    /// returns current data is worse than one that does not run, because the
    /// number it produces looks like a settlement rerun and is not one.
    ///
    /// [`ReadMode::AsOf`]: crate::planner::ReadMode::AsOf
    async fn snapshot_provider(
        &self,
        _table: &str,
        _at: crate::planner::SnapshotSelector,
    ) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        Err(Error::config(
            "this cold store cannot pin a past snapshot, so as-of reads are unavailable",
        ))
    }

    /// Snapshots MeterStore has committed, newest first.
    ///
    /// Exposed so an operator can find the snapshot a settlement ran against
    /// without reading Iceberg metadata by hand. Empty when the table has no
    /// history yet.
    async fn snapshots(&self, _table: &str) -> Result<Vec<SnapshotInfo>> {
        Ok(Vec::new())
    }

    /// The schema the cold table actually has, if it can be read.
    ///
    /// `None` means the store cannot report one, in which case the schema check
    /// (§11) is skipped rather than assumed to pass.
    async fn stored_schema(
        &self,
        _table: &str,
    ) -> Result<Option<crate::arrow::datatypes::SchemaRef>> {
        Ok(None)
    }
}

/// The lease a store with no cross-process locking can offer.
///
/// Named for what it is. A deployment running one writer per table is fine with
/// it; one running several against a store that cannot lock has a correctness
/// problem no lease type can fix.
#[derive(Debug)]
pub struct UnenforcedLease;

#[async_trait]
impl TableLease for UnenforcedLease {
    async fn release(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

/// One committed state of the cold table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// Iceberg snapshot id — the exact selector for a reproducible read.
    pub snapshot_id: i64,
    /// When the commit landed.
    pub committed_at: OffsetDateTime,
    /// The tiering watermark this snapshot published, if it carries one.
    ///
    /// `None` for a snapshot written by something other than MeterStore — an
    /// out-of-band compaction, say. Those are legitimate and readable; they just
    /// do not move the tier boundary.
    pub watermark: Option<TieringWatermark>,
    /// When *this crate* committed it, on the clock the caller supplied.
    ///
    /// Distinct from [`committed_at`](Self::committed_at), which is Iceberg's
    /// own timestamp and therefore a different clock. `None` for a foreign
    /// commit, and for a store that does not record one.
    pub archived_at: Option<OffsetDateTime>,
    /// Rows the commit added, as recorded in its summary.
    pub rows: Option<u64>,
}

/// Shared ownership of a hot store is a normal need: the archiver takes one
/// while the application keeps writing through another handle.
#[async_trait]
impl<T: HotStore + ?Sized> HotStore for std::sync::Arc<T> {
    async fn try_archive_lease(&self, table: &str) -> Result<Option<Box<dyn TableLease>>> {
        (**self).try_archive_lease(table).await
    }

    async fn cold_append_lease(&self, table: &str) -> Result<Box<dyn TableLease>> {
        (**self).cold_append_lease(table).await
    }

    async fn ensure_partitions(
        &self,
        table: &str,
        from: OffsetDateTime,
        until: OffsetDateTime,
        step: time::Duration,
    ) -> Result<Vec<PartitionId>> {
        (**self).ensure_partitions(table, from, until, step).await
    }

    async fn create_tables(
        &self,
        table: &str,
        merge_key: &[String],
        extra: &[crate::arrow::datatypes::Field],
        time_model: crate::config::TimeModel,
    ) -> Result<()> {
        (**self)
            .create_tables(table, merge_key, extra, time_model)
            .await
    }

    async fn append(
        &self,
        table: &str,
        merge_key: &[String],
        batches: &[RecordBatch],
    ) -> Result<u64> {
        (**self).append(table, merge_key, batches).await
    }

    async fn scan_range(
        &self,
        table: &str,
        range: TimeRange,
        spec: &ScanSpec,
    ) -> Result<BatchStream> {
        (**self).scan_range(table, range, spec).await
    }

    async fn partition_exists(&self, partition: &PartitionId) -> Result<bool> {
        (**self).partition_exists(partition).await
    }

    async fn partition_starts(&self, table: &str) -> Result<Option<Vec<OffsetDateTime>>> {
        (**self).partition_starts(table).await
    }

    async fn detach_partition(&self, partition: &PartitionId) -> Result<()> {
        (**self).detach_partition(partition).await
    }

    async fn scan_detached(&self, partition: &PartitionId, spec: &ScanSpec) -> Result<BatchStream> {
        (**self).scan_detached(partition, spec).await
    }

    async fn append_reporting(
        &self,
        table: &str,
        merge_key: &[String],
        batches: &[RecordBatch],
    ) -> Result<Vec<crate::session::Displacement>> {
        (**self).append_reporting(table, merge_key, batches).await
    }

    async fn drop_table(&self, table: &str) -> Result<()> {
        (**self).drop_table(table).await
    }

    async fn distinct_malo_ids(&self, partition: &PartitionId) -> Result<Option<u64>> {
        (**self).distinct_malo_ids(partition).await
    }

    async fn drop_partition(&self, partition: &PartitionId) -> Result<()> {
        (**self).drop_partition(partition).await
    }

    async fn orphaned_partitions(&self, table: &str) -> Result<Vec<PartitionId>> {
        (**self).orphaned_partitions(table).await
    }

    async fn invariant_violations(&self, table: &str, watermark: TieringWatermark) -> Result<u64> {
        (**self).invariant_violations(table, watermark).await
    }
}

/// As for [`HotStore`]: a cold store is commonly shared between the archiver
/// and whatever serves reads from it.
#[async_trait]
impl<T: ColdStore + ?Sized> ColdStore for std::sync::Arc<T> {
    async fn create_tables(
        &self,
        table: &str,
        identity: &[String],
        extra: &[crate::arrow::datatypes::Field],
    ) -> Result<()> {
        (**self).create_tables(table, identity, extra).await
    }

    async fn purge_table(&self, table: &str) -> Result<()> {
        (**self).purge_table(table).await
    }

    async fn watermark(&self, table: &str) -> Result<TieringWatermark> {
        (**self).watermark(table).await
    }

    async fn append_and_commit(
        &self,
        table: &str,
        batches: BatchStream,
        hints: WriteHints,
        window: ArchivalWindow,
        now: OffsetDateTime,
    ) -> Result<CommitInfo> {
        (**self)
            .append_and_commit(table, batches, hints, window, now)
            .await
    }

    async fn expire_snapshots(
        &self,
        table: &str,
        retain_for: time::Duration,
        retain_last: usize,
        now: OffsetDateTime,
    ) -> Result<usize> {
        (**self)
            .expire_snapshots(table, retain_for, retain_last, now)
            .await
    }

    async fn version_stats(
        &self,
        table: &str,
        range: (OffsetDateTime, OffsetDateTime),
    ) -> Result<Vec<crate::planner::FileStats>> {
        (**self).version_stats(table, range).await
    }

    async fn reassert_watermark(&self, table: &str) -> Result<Option<CommitInfo>> {
        (**self).reassert_watermark(table).await
    }

    async fn append_only(
        &self,
        table: &str,
        batches: BatchStream,
        hints: WriteHints,
    ) -> Result<CommitInfo> {
        (**self).append_only(table, batches, hints).await
    }

    async fn snapshot_provider(
        &self,
        table: &str,
        at: crate::planner::SnapshotSelector,
    ) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        (**self).snapshot_provider(table, at).await
    }

    async fn snapshots(&self, table: &str) -> Result<Vec<SnapshotInfo>> {
        (**self).snapshots(table).await
    }

    async fn stored_schema(
        &self,
        table: &str,
    ) -> Result<Option<crate::arrow::datatypes::SchemaRef>> {
        (**self).stored_schema(table).await
    }
}

/// The result of a successful cold-tier commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitInfo {
    /// Iceberg snapshot identifier.
    pub snapshot_id: i64,
    /// Rows written.
    pub rows: u64,
    /// The watermark after this commit.
    pub watermark: TieringWatermark,
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn relation_name_is_derived_from_the_lower_bound() {
        let p = PartitionId::new("readings", datetime!(2026-07-20 00:00 UTC));
        assert_eq!(p.relation_name().unwrap(), "readings_2026_07_20_0000");
    }

    #[test]
    fn sub_daily_partitions_do_not_collide() {
        let a = PartitionId::new("readings", datetime!(2026-07-20 00:00 UTC));
        let b = PartitionId::new("readings", datetime!(2026-07-20 06:00 UTC));
        assert_ne!(a.relation_name().unwrap(), b.relation_name().unwrap());
    }

    #[test]
    fn partition_for_window_uses_the_windows_lower_bound() {
        let w = ArchivalWindow::new(
            datetime!(2026-07-20 00:00 UTC),
            datetime!(2026-07-21 00:00 UTC),
        )
        .unwrap();
        let p = PartitionId::for_window("readings", w);
        assert_eq!(p.start(), w.from());
        assert_eq!(p.relation_name().unwrap(), "readings_2026_07_20_0000");
    }

    #[test]
    fn relation_name_round_trips() {
        let p = PartitionId::new("readings", datetime!(2026-07-20 06:30 UTC));
        let name = p.relation_name().unwrap();
        assert_eq!(
            PartitionId::from_relation_name("readings", &name).unwrap(),
            p
        );
    }

    #[test]
    fn from_relation_name_rejects_foreign_relations() {
        assert!(PartitionId::from_relation_name("readings", "other_2026_07_20_0000").is_err());
        assert!(PartitionId::from_relation_name("readings", "readings").is_err());
        assert!(PartitionId::from_relation_name("readings", "readings_garbage").is_err());
    }

    #[test]
    fn partitions_order_by_start() {
        let mut v = [
            PartitionId::new("readings", datetime!(2026-07-21 00:00 UTC)),
            PartitionId::new("readings", datetime!(2026-07-19 00:00 UTC)),
            PartitionId::new("readings", datetime!(2026-07-20 00:00 UTC)),
        ];
        v.sort();
        assert_eq!(v[0].start(), datetime!(2026-07-19 00:00 UTC));
        assert_eq!(v[2].start(), datetime!(2026-07-21 00:00 UTC));
    }
}
