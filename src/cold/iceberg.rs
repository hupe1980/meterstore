//! Apache Iceberg cold tier.
//!
//! The load-bearing detail is in [`IcebergCold::append_and_commit`]: the
//! tiering watermark is written into the Iceberg **snapshot summary**, in the
//! same commit as the data it describes. Iceberg commits are a compare-and-swap
//! on the catalog's metadata pointer, so the rows and the watermark become
//! durable together or not at all.
//!
//! That removes the classic tiering failure mode. With an external checkpoint
//! store there is always a window where one has landed and the other has not,
//! and a crash inside it either loses data or replays it. Here there is no such
//! window: recovery just reads the watermark back off the current snapshot.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use iceberg::spec::{DataFileFormat, FormatVersion};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use time::OffsetDateTime;
use tracing::{debug, info, warn};

use crate::arrow::array::RecordBatch;
use crate::encode::schema;
use crate::error::{Error, Result};
use crate::planner::SnapshotSelector;
use crate::tiering::store::{
    AddedFiles, BatchStream, ColdStore, CommitInfo, MaintenancePolicy, SnapshotInfo, WriteHints,
};
use crate::watermark::{
    ARCHIVAL_STEP_PROPERTY, ARCHIVED_AT_PROPERTY, ARCHIVED_RANGE_PROPERTY, ArchivalWindow,
    ROW_COUNT_PROPERTY, TieringWatermark, WATERMARK_PROPERTY,
};

/// An Iceberg-backed cold tier.
pub struct IcebergCold {
    catalog: std::sync::Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    target_file_size: usize,
}

impl std::fmt::Debug for IcebergCold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCold")
            .field("namespace", &self.namespace)
            .field("target_file_size", &self.target_file_size)
            .finish_non_exhaustive()
    }
}

impl IcebergCold {
    /// Wrap a catalog, writing into `namespace`.
    pub fn new(
        catalog: std::sync::Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        target_file_size: usize,
    ) -> Self {
        Self {
            catalog,
            namespace,
            target_file_size,
        }
    }

    /// The catalogue this tier commits through.
    ///
    /// Exposed because the catalogue *is* the extension point: `new` takes any
    /// `Arc<dyn Catalog>`, and a deployment that handed one in may need it back —
    /// to serve the read-only façade over it, or to run an operation this crate
    /// does not implement against the same tables.
    pub fn catalog(&self) -> std::sync::Arc<dyn Catalog> {
        std::sync::Arc::clone(&self.catalog)
    }

    fn ident(&self, table: &str) -> TableIdent {
        TableIdent::new(self.namespace.clone(), table.to_string())
    }

    /// The namespace this tier writes into.
    #[must_use]
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Create the namespace and table if they do not exist.
    ///
    /// The table is named `<table>_versions` because it holds **every** version
    /// of every reading, unresolved. An external engine that reads it naively
    /// and sums `value` double-counts corrected intervals, so the name must
    /// not be the one that looks like the obvious thing to query.
    pub async fn create_table(&self, table: &str) -> Result<Table> {
        self.create_table_with(table, &[], &[], &MaintenancePolicy::default())
            .await
    }

    /// Create the table with the deployment's declared extra columns.
    ///
    /// These must match the hot table's, or a row archived from one cannot be
    /// written to the other.
    ///
    /// `identity` names the deployment's identity columns. They become the
    /// **leading partition fields**, ahead of `month(from)`, because an identity
    /// column is by definition something every query filters on — so a scan
    /// scoped to one of them prunes at the manifest rather than by row filter.
    /// See the cold-tier layout.
    pub async fn create_table_with(
        &self,
        table: &str,
        extra: &[crate::arrow::datatypes::Field],
        identity: &[String],
        policy: &MaintenancePolicy,
    ) -> Result<Table> {
        if !self
            .catalog
            .namespace_exists(&self.namespace)
            .await
            .map_err(ice)?
        {
            self.catalog
                .create_namespace(&self.namespace, HashMap::new())
                .await
                .map_err(ice)?;
        }

        let ident = self.ident(table);
        if self.catalog.table_exists(&ident).await.map_err(ice)? {
            let existing = self.catalog.load_table(&ident).await.map_err(ice)?;
            check_partition_spec(table, &existing, identity)?;
            return self.reconcile_properties(existing, policy).await;
        }

        let arrow_schema = schema::storage_schema(extra);
        let iceberg_schema =
            iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(arrow_schema.as_ref())
                .map_err(ice)?;

        let creation = TableCreation::builder()
            .name(table.to_string())
            .partition_spec(partition_spec(&iceberg_schema, identity)?)
            .schema(iceberg_schema)
            .properties(policy_properties(policy))
            .build();

        let created = self
            .catalog
            .create_table(&self.namespace, creation)
            .await
            .map_err(ice)?;

        // `format-version` is a reserved property and cannot be requested at
        // creation, so it is verified instead. v3 adds deletion vectors and row
        // lineage — neither of which an append-only store needs — while its
        // reader support is still uneven, which would undercut the point of
        // storing regulated data in an open format. If a future library default
        // moves to v3, this fails loudly rather than silently migrating a decade
        // of history to a format some engines cannot read.
        let version = created.metadata().format_version();
        if version != FormatVersion::V2 {
            return Err(Error::config(format!(
                "cold table {table} was created as format version {version:?}, expected V2"
            )));
        }

        info!(table, "cold table created");
        Ok(created)
    }

    /// A DataFusion provider for the cold tier.
    ///
    /// Delegates the actual scan to `iceberg-datafusion`, so partition pruning,
    /// bloom filters and page statistics all apply without this crate
    /// reimplementing any of them. The result is the cold half of a
    /// [`TieredTableProvider`].
    ///
    /// **It reloads the table's metadata on every scan**, and that is a
    /// correctness requirement rather than freshness for its own sake. The
    /// obvious construction — build a static provider once and register it —
    /// freezes the snapshot at the moment the store was built. In the embedded
    /// topology one process both archives and serves queries, so after
    /// the first archival run the rows are gone from PostgreSQL (the partition
    /// was dropped) and invisible in Iceberg (the provider still points at the
    /// snapshot from before the commit). They would reappear only when the
    /// process restarted.
    ///
    /// [`TieredTableProvider`]: crate::planner::TieredTableProvider
    pub async fn table_provider(
        self: &std::sync::Arc<Self>,
        table: &str,
    ) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        // Loaded once here for the schema, which is stable between archival
        // commits — a change to it is a schema evolution, and the check halts the
        // table rather than letting the shape drift under a running query.
        let loaded = self.load(table).await?;
        let schema = std::sync::Arc::new(
            iceberg::arrow::schema_to_arrow_schema(loaded.metadata().current_schema())
                .map_err(ice)?,
        );

        Ok(std::sync::Arc::new(RefreshingProvider {
            cold: std::sync::Arc::clone(self),
            table: table.to_string(),
            schema,
        }))
    }

    /// Expire snapshots older than `retain_for`, keeping at least `retain_last`.
    ///
    /// Bounds metadata growth: every commit adds a snapshot, and archival commits
    /// once per window, so a table left alone accumulates them indefinitely and
    /// planning time grows with the list.
    ///
    /// The retention window is a **compliance setting, not a cleanup knob.**
    /// MaBiS settlement must be reproducible, and reproducibility is exactly the
    /// ability to read the table as it stood on a past date — which is what a
    /// snapshot is. Expiring aggressively to save metadata bytes destroys the
    /// audit position the cold tier exists to hold, so the default is ten years
    /// rather than the days a general-purpose lakehouse would choose.
    pub async fn expire_snapshots(
        &self,
        table: &str,
        retain_for: time::Duration,
        retain_last: usize,
        now: OffsetDateTime,
    ) -> Result<usize> {
        // **Before anything is expired**, put the boundary back on the current
        // snapshot. The walk-back through the parent chain is what makes a
        // foreign commit survivable, and a chain is exactly the thing expiry
        // punches holes in — see the anchor comment below. Re-stamping first
        // makes the walk unnecessary, which is a stronger guarantee than
        // protecting the path it would have taken.
        self.reassert_watermark(table).await?;

        let loaded = self.load(table).await?;
        let metadata = loaded.metadata();
        let before = metadata.snapshots().count();

        let cutoff_ms = (now - retain_for).unix_timestamp() * 1_000;

        // Chosen here rather than by `expire_older_than_ms`, because one snapshot
        // has to survive that neither age nor count identifies.
        let mut ordered: Vec<_> = metadata.snapshots().collect();
        ordered.sort_by_key(|s| (s.timestamp_ms(), s.snapshot_id()));

        // **The path from the current snapshot back to the boundary.**
        //
        // There is no native compaction, and the documented answer is out of band
        // with Spark or PyIceberg. Such a commit is a valid Iceberg snapshot that
        // knows nothing about tiering, so it carries no watermark and the lookup
        // walks back the *parent chain* to find one.
        //
        // Expiring by age alone breaks that walk — and not only by removing the
        // snapshot that carries the boundary. Removing any **intermediate**
        // ancestor is enough: the chain then has a hole, the walk stops at a
        // parent id that no longer resolves, and no boundary is found. A
        // maintenance job following this design's own advice would have bricked
        // the table, and the symptom is that every query fails at once.
        //
        // The re-stamp above normally makes this path one snapshot long. It is
        // still computed, because `expire_snapshots` is public and a caller may
        // reach it with a history this run did not create.
        let mut protected: std::collections::HashSet<i64> = Default::default();
        let mut walk = metadata.current_snapshot().cloned();
        while let Some(snapshot) = walk {
            protected.insert(snapshot.snapshot_id());
            if snapshot
                .summary()
                .additional_properties
                .contains_key(WATERMARK_PROPERTY)
            {
                break;
            }
            walk = snapshot
                .parent_snapshot_id()
                .and_then(|id| metadata.snapshot_by_id(id))
                .cloned();
        }

        protected.extend(
            ordered
                .iter()
                .rev()
                .take(retain_last.max(1))
                .map(|s| s.snapshot_id()),
        );

        let doomed: Vec<i64> = ordered
            .iter()
            .filter(|s| s.timestamp_ms() < cutoff_ms)
            .map(|s| s.snapshot_id())
            .filter(|id| !protected.contains(id))
            .collect();

        if doomed.is_empty() {
            return Ok(0);
        }

        let txn = Transaction::new(&loaded);
        // **`expire_older_than_ms` pinned to the epoch is what keeps the retention
        // window a compliance setting.**
        //
        // `iceberg`'s action runs its age path whether or not ids are named,
        // falling back to the table's `history.expire.max-snapshot-age-ms` —
        // default **five days**. That cuts through `snapshot_retention`,
        // `min_snapshots_to_keep` and the watermark-chain protection computed
        // above, none of which the library knows about, so one cycle could leave
        // no settlement older than a working week reproducible.
        //
        // The epoch selects nothing by age, so the ids computed here are the
        // whole of what is expired — the library's own documented way to expire
        // by id alone. Set on the **action**, so it holds whatever the table
        // says; the table separately publishes `history.expire.*` at this
        // deployment's real retention, which is for a *foreign* tool to read and
        // is deliberately not what this path consults.
        let action = txn
            .expire_snapshots()
            .expire_older_than_ms(0)
            .retain_last(retain_last.max(1))
            .expire_snapshot_ids(doomed);

        let committed = action
            .apply(txn)
            .map_err(ice)?
            .commit(self.catalog.as_ref())
            .await
            .map_err(ice)?;

        let after = committed.metadata().snapshots().count();
        let expired = before.saturating_sub(after);

        if expired > 0 {
            info!(table, expired, retain_last, "snapshots expired");
        }
        Ok(expired)
    }

    /// Put the tiering boundary back on the **current** snapshot.
    ///
    /// Every MeterStore commit stamps the boundary into its own snapshot summary.
    /// A commit from anything else does not — and this design explicitly
    /// recommends such commits, because compaction and orphan cleanup are blocked
    /// upstream, and the documented answer is to run them out of
    /// band with Spark or PyIceberg.
    ///
    /// After one, the boundary is still *findable*: the lookup walks back the
    /// parent chain. But it is no longer where every reader expects it, which
    /// costs a walk on every read and forces snapshot expiry to keep an ancestor
    /// alive indefinitely to avoid stranding it. Re-stamping restores the
    /// invariant that the current snapshot states the boundary.
    ///
    /// The value is **read from the history, never invented**: this republishes
    /// what the table already said, so it cannot move the boundary. It commits an
    /// empty append carrying the property, which is the same shape as an archival
    /// window that held no rows.
    ///
    /// `Ok(None)` when the current snapshot already carries it — so this is
    /// idempotent and cheap to run on a schedule.
    pub async fn reassert_watermark(&self, table: &str) -> Result<Option<CommitInfo>> {
        let loaded = self.load(table).await?;
        let Some(current) = loaded.metadata().current_snapshot() else {
            // Nothing has been committed, so there is no boundary to restate.
            return Ok(None);
        };
        if current
            .summary()
            .additional_properties
            .contains_key(WATERMARK_PROPERTY)
        {
            return Ok(None);
        }

        let watermark = watermark_of(&loaded)?;
        info!(
            table,
            %watermark,
            "current snapshot carries no boundary; restating it after an out-of-band commit"
        );
        self.append_with_summary(
            table,
            crate::tiering::store::stream_of(Vec::new()),
            WriteHints::default(),
            Summary::Preserve,
        )
        .await
        .map(Some)
    }

    /// Per-file `version` bounds for every live data file overlapping `range`.
    ///
    /// This is the input to [`planner::version::plan`], which decides whether a
    /// scan can skip resolution entirely. Iceberg records `min`/`max` per column
    /// per file in the manifests, so proving a partition correction-free costs a
    /// metadata read rather than a scan.
    ///
    /// `None` for a file means its statistics are missing or unreadable, which
    /// the planner treats as "may contain corrections" — the absence of
    /// statistics proves nothing.
    ///
    /// [`planner::version::plan`]: crate::planner::version::plan
    pub async fn version_stats(
        &self,
        table: &str,
        range: (OffsetDateTime, OffsetDateTime),
    ) -> Result<Vec<crate::planner::FileStats>> {
        use crate::encode::schema::col;
        use crate::planner::{FileStats, VersionStats};

        let loaded = self.load(table).await?;
        let metadata = loaded.metadata();
        let Some(snapshot) = metadata.current_snapshot() else {
            // No snapshot means no data, and an empty scan needs no resolution.
            return Ok(Vec::new());
        };

        let schema = metadata.current_schema();
        let field_id = |name: &str| schema.field_by_name(name).map(|f| f.id);
        // Without a version column there is nothing to reason about; treat every
        // file as unprovable rather than silently claiming elision.
        let (Some(version_id), Some(from_id)) = (field_id(col::VERSION), field_id(col::FROM))
        else {
            return Ok(vec![FileStats::unknown()]);
        };

        let file_io = loaded.file_io();
        let manifest_list = loaded
            .manifest_list_reader(snapshot)
            .load()
            .await
            .map_err(ice)?;

        let mut stats = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io).await.map_err(ice)?;
            for entry in manifest.entries() {
                // Deleted entries still appear in a manifest; only live data
                // files can contribute a version to a scan.
                if !entry.is_alive() {
                    continue;
                }
                let data_file = entry.data_file();

                // The file's own `from` span, which does two jobs. Files
                // provably outside the scan's range are dropped here — including
                // them would let one untouched historical file with a correction
                // disable elision for every query — and the span travels with the
                // ones that remain, because two files that do not overlap on
                // `from` cannot hold the same merge key and are therefore free to
                // carry different versions.
                let interval = match (
                    data_file.lower_bounds().get(&from_id),
                    data_file.upper_bounds().get(&from_id),
                ) {
                    (Some(lo), Some(hi)) => match (as_timestamp(lo), as_timestamp(hi)) {
                        (Some(lo), Some(hi)) => Some((lo, hi)),
                        _ => None,
                    },
                    _ => None,
                };
                if let Some((lo, hi)) = interval
                    && (hi < range.0 || lo >= range.1)
                {
                    continue;
                }

                let version = match (
                    data_file.lower_bounds().get(&version_id),
                    data_file.upper_bounds().get(&version_id),
                ) {
                    (Some(lo), Some(hi)) => match (as_i128(lo), as_i128(hi)) {
                        (Some(min), Some(max)) => Some(VersionStats { min, max }),
                        _ => None,
                    },
                    _ => None,
                };

                stats.push(FileStats { version, interval });
            }
        }

        Ok(stats)
    }

    /// Every snapshot of the table, newest first.
    ///
    /// The list an operator needs to answer "which snapshot did the 8th-working-day
    /// settlement run against". Snapshots MeterStore wrote carry a watermark;
    /// ones an out-of-band compaction wrote do not, and both are listed because
    /// both are readable.
    pub async fn snapshots(&self, table: &str) -> Result<Vec<SnapshotInfo>> {
        let loaded = self.load(table).await?;
        let metadata = loaded.metadata();

        let mut out: Vec<SnapshotInfo> = metadata
            .snapshots()
            .map(|s| {
                let properties = &s.summary().additional_properties;
                SnapshotInfo {
                    snapshot_id: s.snapshot_id(),
                    committed_at: OffsetDateTime::from_unix_timestamp_nanos(
                        i128::from(s.timestamp_ms()) * 1_000_000,
                    )
                    .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                    watermark: properties
                        .get(WATERMARK_PROPERTY)
                        .and_then(|v| TieringWatermark::from_property(v).ok()),
                    archived_at: properties.get(ARCHIVED_AT_PROPERTY).and_then(|v| {
                        OffsetDateTime::parse(v, &time::format_description::well_known::Rfc3339)
                            .ok()
                    }),
                    rows: properties
                        .get(ROW_COUNT_PROPERTY)
                        .and_then(|v| v.parse().ok()),
                }
            })
            .collect();

        out.sort_by(|a, b| {
            b.committed_at
                .cmp(&a.committed_at)
                .then(b.snapshot_id.cmp(&a.snapshot_id))
        });
        Ok(out)
    }

    /// Resolve a selector to a concrete snapshot id.
    ///
    /// A timestamp resolves to the newest snapshot committed at or before it —
    /// the state the table was actually in at that instant. An instant older than
    /// every snapshot has no answer and must not silently become the oldest one:
    /// that would return a table that never existed at the requested time.
    async fn resolve_snapshot(&self, table: &str, at: SnapshotSelector) -> Result<i64> {
        let loaded = self.load(table).await?;
        let metadata = loaded.metadata();

        match at {
            SnapshotSelector::Id(id) => {
                if metadata.snapshot_by_id(id).is_none() {
                    return Err(Error::config(format!(
                        "snapshot {id} is not in the history of {table}: it was either never \
                         committed, or expired — see the snapshot_retention setting, which is a \
                         compliance decision rather than a cleanup knob"
                    )));
                }
                Ok(id)
            }
            SnapshotSelector::Timestamp(instant) => {
                let cutoff_ms = instant.unix_timestamp() * 1_000 + i64::from(instant.millisecond());
                metadata
                    .snapshots()
                    .filter(|s| s.timestamp_ms() <= cutoff_ms)
                    .max_by_key(|s| (s.timestamp_ms(), s.snapshot_id()))
                    .map(|s| s.snapshot_id())
                    .ok_or_else(|| {
                        Error::config(format!(
                            "{table} has no snapshot at or before {instant}: the requested \
                             instant predates the table's history, so there is nothing to \
                             reproduce"
                        ))
                    })
            }
        }
    }

    /// Load a table, or report it missing.
    ///
    /// Public because "does this table already exist, and what shape is it"
    /// is a question an operator and a test both legitimately ask, and reaching
    /// for [`create_table`](Self::create_table) to answer it conflates two
    /// intentions — one of which now validates the layout.
    pub async fn load(&self, table: &str) -> Result<Table> {
        self.catalog
            .load_table(&self.ident(table))
            .await
            .map_err(ice)
    }

    /// Write a stream of batches as Parquet data files and return them.
    ///
    /// Streaming rather than taking a slice, because the caller's input is a
    /// day of a partition: at 100 k measuring points that is ~9.6 M rows, and
    /// materialising it to hand over would make archival's peak memory
    /// proportional to the window rather than to a chunk.
    ///
    /// Returns the row count alongside the files, since a streaming caller has
    /// no other way to learn it.
    async fn write_data_files(
        &self,
        table: &Table,
        mut batches: BatchStream,
        hints: WriteHints,
    ) -> Result<(Vec<iceberg::spec::DataFile>, u64)> {
        use futures::StreamExt;

        let props = super::parquet::writer_properties(
            hints.distinct_malo_ids.unwrap_or(DEFAULT_BLOOM_FILTER_NDV),
        );
        // The writer needs the table's own Iceberg schema, not our Arrow one:
        // field IDs are assigned at table creation and the two must agree.
        let iceberg_schema = table.metadata().current_schema().clone();

        // Batches arrive with a plain Arrow schema carrying no field IDs, but
        // the Parquet writer matches columns by ID. Re-wrap the same column
        // arrays in the ID-annotated schema derived from the table.
        let write_schema = std::sync::Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).map_err(ice)?,
        );
        let location = DefaultLocationGenerator::new(table.metadata()).map_err(ice)?;
        let names = DefaultFileNameGenerator::new(
            "data".to_string(),
            Some(file_suffix()),
            DataFileFormat::Parquet,
        );

        let rolling = RollingFileWriterBuilder::new(
            ParquetWriterBuilder::new(props, iceberg_schema.clone()),
            self.target_file_size,
            table.file_io().clone(),
            location,
            names,
        );
        let files = DataFileWriterBuilder::new(rolling);

        // The rows arrive sorted by `(malo_id, from)`, which says nothing about
        // the partition order — a day's readings interleave tenants freely. A
        // clustered writer requires partition-ordered input and would reject
        // that, so this fans out: one open writer per partition the window
        // actually touches.
        //
        // The count is bounded and small. A window is one day, so it lies in one
        // month; the only other partition field is the identity tuple, which is
        // the deployment's tenant set. A single-operator deployment opens one
        // writer; a service bureau opens one per operator whose meters reported
        // that day. Sorting the scan by tenant to allow the cheaper clustered
        // writer would trade that for a sort order the Parquet footer no longer
        // matches, which is a worse deal.
        let spec = table.metadata().default_partition_spec().clone();

        let mut rows = 0u64;
        let mut wrote_anything = false;

        let data_files = if spec.is_unpartitioned() {
            let mut writer = files.build(None).await.map_err(ice)?;
            while let Some(batch) = batches.next().await {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }
                rows += batch.num_rows() as u64;
                wrote_anything = true;
                writer
                    .write(align(&batch, &write_schema)?)
                    .await
                    .map_err(ice)?;
            }
            if !wrote_anything {
                return Ok((Vec::new(), 0));
            }
            writer.close().await.map_err(ice)?
        } else {
            use iceberg::arrow::{PartitionValueCalculator, RecordBatchPartitionSplitter};
            use iceberg::writer::partitioning::{PartitioningWriter, fanout_writer::FanoutWriter};

            let calculator =
                PartitionValueCalculator::try_new(&spec, &iceberg_schema).map_err(ice)?;
            let splitter = RecordBatchPartitionSplitter::try_new(
                iceberg_schema.clone(),
                spec.clone(),
                Some(calculator),
            )
            .map_err(ice)?;

            let mut writer = FanoutWriter::new(files);
            while let Some(batch) = batches.next().await {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }
                rows += batch.num_rows() as u64;
                wrote_anything = true;
                for (key, part) in splitter
                    .split(&align(&batch, &write_schema)?)
                    .map_err(ice)?
                {
                    writer.write(key, part).await.map_err(ice)?;
                }
            }
            if !wrote_anything {
                return Ok((Vec::new(), 0));
            }
            writer.close().await.map_err(ice)?
        };

        // A window with no rows is ordinary — a meter can simply not report —
        // and still has to advance the watermark, so it commits with no data
        // files rather than not committing. Closing a writer that never
        // saw a batch would produce an empty Parquet file for every such day;
        // both arms above return early instead.
        Ok((data_files, rows))
    }

    /// Bring an existing table's properties up to the contract it should state.
    ///
    /// A new table gets them at creation, so this is for a table that already
    /// exists and whose properties differ — whatever put it in that state, and
    /// including the ordinary case of a deployment that has just measured its
    /// file size or moved its retention. It runs on every `create_tables`, which
    /// is idempotent, so restating is a matter of calling it again.
    ///
    /// One of these is not a preference. `commit.retry.num-retries = 0` turns off
    /// the library's own retry so this crate's can run instead — see
    /// [`IcebergCold::append_with_summary`] for why re-applying a *fixed*
    /// snapshot summary against a refreshed base is wrong here.
    ///
    /// **Nothing is removed.** A property the deployment no longer declares —
    /// `declared_file_size` unset after being set — is left as it stands rather
    /// than deleted, because this cannot tell a deployment that withdrew a
    /// declaration from one whose configuration simply does not mention it, and
    /// silently restoring a compactor's 512 MiB assumption is the failure the
    /// property exists to stop.
    async fn reconcile_properties(
        &self,
        table: Table,
        policy: &MaintenancePolicy,
    ) -> Result<Table> {
        let current = table.metadata().properties();
        let wanted: Vec<(String, String)> = policy_properties(policy)
            .into_iter()
            .filter(|(key, value)| current.get(key) != Some(value))
            .collect();
        if wanted.is_empty() {
            return Ok(table);
        }

        let txn = Transaction::new(&table);
        let mut action = txn.update_table_properties();
        for (key, value) in &wanted {
            action = action.set(key.clone(), value.clone());
        }
        let updated = action
            .apply(txn)
            .map_err(ice)?
            .commit(self.catalog.as_ref())
            .await
            .map_err(ice)?;
        debug!(
            table = table.identifier().name(),
            restated = wanted.len(),
            "table properties restated"
        );
        Ok(updated)
    }

    /// Write `batches` and commit them with a snapshot summary derived from the
    /// base the commit actually lands on, retrying a lost race.
    ///
    /// # Why the retry cannot be left to the library
    ///
    /// `iceberg` 0.10 already retries a conflicting commit: it reloads the table
    /// and re-applies the same action — including the snapshot summary the action
    /// was **built with**, four times by default. For an ordinary append that is
    /// exactly right, because an append's summary describes only itself.
    ///
    /// Ours does not. It carries the tiering watermark, which is a property of
    /// the base the commit lands on, and re-publishing a stale one moves the
    /// boundary **backwards**. The path that gets there is ordinary traffic: a
    /// late-correction append reads the current watermark, an archival commit
    /// lands first, and the correction's retry then republishes the older value.
    /// Intervals PostgreSQL has already purged are claimed by the hot tier, which
    /// does not hold them, and they vanish from every unified query with nothing
    /// reporting a failure. That is the single failure the in-snapshot watermark
    /// exists to make
    /// impossible, reintroduced by a dependency being helpful.
    ///
    /// So library retry is off (`commit.retry.num-retries = 0`) and the loop is
    /// here, where the summary — and the monotonicity assertion behind it — is
    /// rebuilt from the refreshed base on every attempt. The data files are
    /// written once and reused: they are independent of which snapshot they land
    /// on, and re-writing them per attempt would rewrite the whole window.
    async fn append_with_summary(
        &self,
        table: &str,
        batches: BatchStream,
        hints: WriteHints,
        summary: Summary,
    ) -> Result<CommitInfo> {
        let mut base = self.load(table).await?;
        let (data_files, rows) = self.write_data_files(&base, batches, hints).await?;

        for attempt in 0..COMMIT_ATTEMPTS {
            let watermark = summary.watermark_for(table, &base)?;
            let mut properties = HashMap::from([
                (WATERMARK_PROPERTY.to_string(), watermark.to_property()?),
                (ROW_COUNT_PROPERTY.to_string(), rows.to_string()),
            ]);
            if let Summary::Advance(window, step, archived_at) = summary {
                properties.insert(ARCHIVED_RANGE_PROPERTY.to_string(), window.to_property()?);
                // The grid the boundary sits on, beside the boundary. Written
                // only on an advance: a correction append republishes the
                // watermark it found and cuts no window, so it has no step of
                // its own to state — and the walk finds the last one that did.
                properties.insert(
                    ARCHIVAL_STEP_PROPERTY.to_string(),
                    crate::watermark::step_to_property(step),
                );
                properties.insert(
                    ARCHIVED_AT_PROPERTY.to_string(),
                    archived_at
                        .format(&time::format_description::well_known::Rfc3339)
                        .map_err(|e| {
                            crate::error::Error::encode(ARCHIVED_AT_PROPERTY, e.to_string())
                        })?,
                );
            }

            let txn = Transaction::new(&base);
            let action = txn
                .fast_append()
                .add_data_files(data_files.clone())
                .set_snapshot_properties(properties);

            match action
                .apply(txn)
                .map_err(ice)?
                .commit(self.catalog.as_ref())
                .await
            {
                Ok(committed) => {
                    let snapshot_id = committed
                        .metadata()
                        .current_snapshot()
                        .map(|s| s.snapshot_id())
                        .unwrap_or_default();
                    debug!(table, rows, %watermark, snapshot_id, "cold commit");
                    return Ok(CommitInfo {
                        snapshot_id,
                        rows,
                        watermark,
                        // Taken from the files this commit wrote rather than read
                        // back out of the snapshot summary: it is the same
                        // number, and the caller wants it in order to compare it
                        // against a declared target.
                        added: (!data_files.is_empty()).then(|| AddedFiles {
                            bytes: data_files.iter().map(|f| f.file_size_in_bytes()).sum(),
                            files: data_files.len() as u64,
                        }),
                    });
                }
                Err(e) if e.kind() == iceberg::ErrorKind::CatalogCommitConflicts => {
                    // Someone else committed first. Reload and rebuild the
                    // summary against what is now current — which is the whole
                    // reason this loop is not the library's.
                    //
                    // No guard on `attempt` here: the loop bound is the guard.
                    // Carrying a second one meant the final conflict fell through
                    // to the arm below and returned a raw Iceberg error, so the
                    // exhaustion message naming the real cause was unreachable —
                    // and an operator saw a catalogue conflict rather than "a
                    // writer is committing continuously".
                    debug!(
                        table,
                        attempt, "commit conflict; re-deriving against a fresh base"
                    );
                    if attempt + 1 < COMMIT_ATTEMPTS {
                        tokio::time::sleep(backoff(attempt)).await;
                        base = self.load(table).await?;
                    }
                }
                Err(e) => {
                    // Not a lost race — a catalogue that refused, or one that may
                    // not have answered. The two are not the same: a refusal
                    // means nobody will ever commit these files, while silence
                    // means the commit may have landed and a snapshot may already
                    // reference them.
                    let refused = Self::commit_was_refused(&e);
                    self.discard(table, &base, &data_files, refused).await;
                    return Err(ice(e));
                }
            }
        }

        // Every attempt lost the compare-and-swap, which is the one failure that
        // says positively that nothing landed: a conflict *is* the catalogue
        // reporting that somebody else's commit is the one in place.
        self.discard(table, &base, &data_files, true).await;
        Err(Error::Storage(format!(
            "{table}: {COMMIT_ATTEMPTS} commit attempts all lost the compare-and-swap; \
             another writer is committing continuously"
        )))
    }

    /// Whether a failed commit definitively did not land.
    ///
    /// The distinction decides whether the data files it wrote may be deleted,
    /// and the two mistakes are not comparable: keeping a file the catalogue
    /// never referenced costs storage that out-of-band orphan removal reclaims,
    /// while deleting one a landed snapshot *does* reference destroys settled
    /// history with no recovery path.
    ///
    /// So this answers "refused" only for the kinds that say so, and treats
    /// everything else — including any variant added upstream, since `ErrorKind`
    /// is `#[non_exhaustive]` — as ambiguous.
    ///
    /// `Unexpected` is the ambiguous one that matters, and upstream names it
    /// exactly that: *"Iceberg don't know what happened here… for example,
    /// iceberg returns an internal service error."* A timeout, a reset
    /// connection and a 5xx all arrive as that kind, and none of them says
    /// whether the catalogue applied the update before the answer was lost.
    fn commit_was_refused(e: &iceberg::Error) -> bool {
        use iceberg::ErrorKind;
        matches!(
            e.kind(),
            ErrorKind::DataInvalid
                | ErrorKind::FeatureUnsupported
                | ErrorKind::NamespaceNotFound
                | ErrorKind::TableNotFound
                | ErrorKind::NamespaceAlreadyExists
                | ErrorKind::TableAlreadyExists
                | ErrorKind::PreconditionFailed
        )
    }

    /// Remove data files a commit wrote and never landed.
    ///
    /// Files are written before the commit and reused across its retries, so a
    /// run that exhausts them leaves unreferenced Parquet — the warehouse's only
    /// source of orphans, since nothing here removes a committed file. General
    /// orphan cleanup stays blocked on `FileIO` having no listing operation;
    /// this needs none, because the writer still holds every path.
    ///
    /// `refused` is the caller's verdict from [`Self::commit_was_refused`]. When
    /// the commit may have landed, **nothing is deleted**: the files are left for
    /// out-of-band orphan removal, which is the operation that reclaims them and
    /// the one this crate does not perform.
    ///
    /// It **re-reads first** even when refused: a re-read is one more chance to
    /// notice a file the current snapshot references, and deleting then would
    /// take it out from under a live snapshot. Anything referenced is left alone,
    /// and so is everything if the reload fails. That read is a second belt
    /// rather than the argument — a catalogue behind a cache, or one whose read
    /// lands on a replica, can answer from before the commit it just applied,
    /// which is precisely why an ambiguous outcome deletes nothing at all.
    ///
    /// Best-effort, and silent about its own failures: the caller is already
    /// returning the error that matters, and an orphan costs storage rather than
    /// correctness.
    async fn discard(
        &self,
        table: &str,
        base: &Table,
        files: &[iceberg::spec::DataFile],
        refused: bool,
    ) {
        if files.is_empty() {
            return;
        }

        if !refused {
            warn!(
                table,
                files = files.len(),
                "the catalogue did not say whether it applied this commit, so its data \
                 files are left in place: deleting one a landed snapshot references \
                 would destroy settled history, while keeping one it does not costs \
                 storage that orphan removal reclaims"
            );
            return;
        }

        let referenced = match self.referenced_paths(table).await {
            Ok(paths) => paths,
            Err(e) => {
                warn!(
                    table,
                    error = %e,
                    files = files.len(),
                    "could not confirm the failed commit left its data files unreferenced; \
                     leaving them in place"
                );
                return;
            }
        };

        let io = base.file_io();
        let (mut removed, mut kept) = (0usize, 0usize);
        for file in files {
            if referenced.contains(file.file_path()) {
                kept += 1;
                continue;
            }
            match io.delete(file.file_path()).await {
                Ok(()) => removed += 1,
                Err(e) => warn!(table, path = file.file_path(), error = %e, "orphan not removed"),
            }
        }

        if kept > 0 {
            // The commit landed after all, and the error the caller is about to
            // return is a lie about durability rather than about the data.
            warn!(
                table,
                kept, "the failed commit had in fact landed; its data files are live and were kept"
            );
        }
        if removed > 0 {
            info!(
                table,
                removed, "removed data files from a commit that never landed"
            );
        }
    }

    /// Every data-file path the table's current snapshot references.
    async fn referenced_paths(&self, table: &str) -> Result<HashSet<String>> {
        let loaded = self.load(table).await?;
        let Some(snapshot) = loaded.metadata().current_snapshot() else {
            return Ok(HashSet::new());
        };

        let file_io = loaded.file_io();
        let manifest_list = loaded
            .manifest_list_reader(snapshot)
            .load()
            .await
            .map_err(ice)?;

        let mut paths = HashSet::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io).await.map_err(ice)?;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    paths.insert(entry.data_file().file_path().to_string());
                }
            }
        }
        Ok(paths)
    }
}

/// What a commit's snapshot summary should say about the tier boundary.
#[derive(Debug, Clone, Copy)]
enum Summary {
    /// Archival: advance the boundary to the window's exclusive end, recording
    /// the caller's clock so the reader grace is measured on it.
    /// The step is carried explicitly rather than taken as `to - from`: a
    /// window widened over an empty stretch is legitimately wider than the step
    /// — the first commit of a fresh table spans from the epoch — so the width
    /// says nothing about the grid.
    Advance(ArchivalWindow, time::Duration, OffsetDateTime),
    /// A late correction: restate whatever the base already published. The
    /// boundary is about which tier owns a range, and a correction does not
    /// change that.
    Preserve,
}

impl Summary {
    /// The watermark to publish, given the base this attempt will land on.
    fn watermark_for(self, table: &str, base: &Table) -> Result<TieringWatermark> {
        let current = watermark_of(base)?;
        match self {
            Self::Preserve => Ok(current),
            Self::Advance(window, _, _) => {
                let next = window.resulting_watermark();
                // Asserted against the commit base, not only by the caller. An
                // archiver that read a stale watermark, or a second archiver
                // racing the first, would otherwise publish a boundary that moves
                // backwards over rows PostgreSQL has already purged.
                //
                // **Strictly** forwards, where `advance_to` permits equality —
                // deliberately, because a `Preserve` summary republishes the
                // boundary it found. For an archival commit equality is the one
                // case the retry loop admits: two archivers target `[W, W+step)`,
                // the first commits, the second loses the compare-and-swap,
                // re-derives `next` from its own window — still `W+step` — and
                // appends a **second copy of the same rows**.
                //
                // Nothing downstream reports that. The archiver's row counts
                // agree, and the invariant check counts rows in PostgreSQL, where
                // they are correctly absent; the duplicate lives in Iceberg and
                // doubles every sum over the window. The lease makes it rare
                // rather than impossible. A window already at the boundary was
                // archived by somebody, so the answer is to stop.
                if next.get() <= current.get() {
                    return Err(Error::InvariantViolated {
                        table: table.to_string(),
                        detail: format!(
                            "the window [{}, {}) is already committed: the boundary is \
                             at {current} and archiving it again would append a second \
                             copy of rows the cold tier already holds. Another writer \
                             committed this window — abort rather than duplicate it",
                            window.from(),
                            window.to(),
                        ),
                    });
                }
                current
                    .advance_to(table, next)
                    .map_err(|_| Error::InvariantViolated {
                        table: table.to_string(),
                        detail: format!(
                            "archiving [{}, {}) would move the watermark backwards from {current}",
                            window.from(),
                            window.to(),
                        ),
                    })
            }
        }
    }
}

/// Table property switching off the library's own commit retry.
const COMMIT_RETRIES_PROPERTY: &str = "commit.retry.num-retries";

/// The size a compactor's "is this file small?" test is a fraction of.
const TARGET_FILE_SIZE_PROPERTY: &str = "write.target-file-size-bytes";
/// Iceberg's own age cutoff for a snapshot expiry, in milliseconds.
const MAX_SNAPSHOT_AGE_PROPERTY: &str = "history.expire.max-snapshot-age-ms";
/// How many snapshots an expiry must leave behind whatever their age.
const MIN_SNAPSHOTS_PROPERTY: &str = "history.expire.min-snapshots-to-keep";
/// Whether superseded metadata files are removed as commits land.
const METADATA_DELETE_PROPERTY: &str = "write.metadata.delete-after-commit.enabled";
/// How many superseded metadata files survive that removal.
const METADATA_VERSIONS_PROPERTY: &str = "write.metadata.previous-versions-max";

/// How many superseded metadata files this crate keeps.
///
/// Iceberg's own default, declared rather than inherited: the *removal* is what
/// this table turns on, and a table that enables it without stating the bound
/// leaves the bound to whichever tool reads it next.
const METADATA_VERSIONS_MAX: usize = 100;

/// The table properties that state this table's maintenance contract.
///
/// # Why a property and not a runbook
///
/// Compaction and expiry are out of band by design, and every rule they can
/// break is one they will break, because a scheduled job runs at *its* defaults
/// and a paragraph is not in the loop. Iceberg already has names for three of
/// the four rules; writing them on the table is the only remedy that reaches a
/// tool whose operator never read this crate's documentation — including a
/// catalogue that maintains the table unasked, where there is no operator to
/// reach at all.
///
/// Metadata removal is on and bounded here rather than declared per deployment,
/// because unlike the other two it has no per-deployment answer: a superseded
/// metadata file is reachable through no snapshot this crate reads, the boundary
/// walk runs inside the *current* one, and every catalogue this crate supports
/// keeps its pointer in the catalogue rather than in the file.
fn policy_properties(policy: &MaintenancePolicy) -> HashMap<String, String> {
    let mut properties = HashMap::from([
        (COMMIT_RETRIES_PROPERTY.to_string(), "0".to_string()),
        (
            MAX_SNAPSHOT_AGE_PROPERTY.to_string(),
            (policy.snapshot_retention.whole_milliseconds().max(0)).to_string(),
        ),
        (
            MIN_SNAPSHOTS_PROPERTY.to_string(),
            policy.min_snapshots_to_keep.to_string(),
        ),
        (METADATA_DELETE_PROPERTY.to_string(), "true".to_string()),
        (
            METADATA_VERSIONS_PROPERTY.to_string(),
            METADATA_VERSIONS_MAX.to_string(),
        ),
    ]);
    // Only when the deployment measured one. A guess here is worse than silence:
    // too low and every ordinary file reads as oversized, which is the same
    // rewrite reached from the other side.
    if let Some(bytes) = policy.declared_file_size {
        properties.insert(TARGET_FILE_SIZE_PROPERTY.to_string(), bytes.to_string());
    }
    properties
}

/// How many times a commit is attempted before giving up.
///
/// Attempts, not retries: the loop runs this many times in total, so the
/// exhaustion diagnostic below can state the figure a reader will count.
const COMMIT_ATTEMPTS: u32 = 4;

/// Base backoff between commit attempts.
const COMMIT_BACKOFF_MS: u64 = 50;

/// Exponential backoff with full jitter, for attempt `n` counting from zero.
///
/// # Why jitter, when a lease already serialises archival
///
/// The lease stops two *archivers*, and the conflicts this loop exists for are
/// the ones it does not cover: an archival commit racing a late correction's
/// cold append, or a second deployment on the same warehouse. Those arrive
/// unsynchronised and then retry on the same schedule — a deterministic
/// `50 << attempt` makes two writers that collided once collide again at 100 ms,
/// and again at 200, which is the shape that turns one lost race into four.
///
/// Full jitter — uniform over `[0, base)` rather than `base ± something` —
/// because it is the variant that actually decorrelates: two writers picking
/// independently from the same interval separate on the first retry.
fn backoff(attempt: u32) -> std::time::Duration {
    let ceiling = COMMIT_BACKOFF_MS << attempt;
    let mut bytes = [0u8; 8];
    // A failure here is not worth propagating from a sleep. Falling back to the
    // full ceiling waits a correct amount of time and merely gives up the
    // decorrelation, which is a weaker retry rather than a wrong one.
    let jittered = if getrandom::fill(&mut bytes).is_ok() {
        u64::from_le_bytes(bytes) % ceiling.max(1)
    } else {
        ceiling
    };
    std::time::Duration::from_millis(jittered)
}

/// A cold-tier provider that reads the table's current snapshot on every scan.
///
/// See [`IcebergCold::table_provider`] for why a static provider is wrong here.
/// The cost is one catalog `load_table` per scan, which the tiered provider
/// already pays anyway to read the watermark — the boundary and the data it
/// describes have to come from the same commit, so both being fresh is the point
/// rather than an overhead.
struct RefreshingProvider {
    cold: std::sync::Arc<IcebergCold>,
    table: String,
    schema: crate::arrow::datatypes::SchemaRef,
}

impl std::fmt::Debug for RefreshingProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshingProvider")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl datafusion::catalog::TableProvider for RefreshingProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> crate::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        let external = |e: Error| datafusion::common::DataFusionError::External(Box::new(e));

        let loaded = self.cold.load(&self.table).await.map_err(external)?;
        let provider = iceberg_datafusion::IcebergStaticTableProvider::try_new_from_table(loaded)
            .await
            .map_err(|e| external(ice(e)))?;
        provider.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> datafusion::common::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
    {
        // Inexact throughout: filters still reach the Iceberg scan for pruning,
        // and the engine re-applies them above. Claiming `Exact` would let
        // DataFusion drop a filter on the strength of pruning, which prunes
        // *files* rather than rows.
        Ok(vec![
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact;
            filters.len()
        ])
    }
}

/// Re-wrap a batch's columns in the ID-annotated schema the writer expects.
///
/// Batches arrive with a plain Arrow schema carrying no field IDs, but the
/// Parquet writer matches columns by ID. Iceberg also spells the UTC offset
/// `+00:00` where the canonical schema says `UTC` — the same instant, a
/// different string — so each column is cast, which is metadata-only for
/// equal-unit timestamps.
///
/// The column *count* is checked rather than zipped: a `zip` silently truncates
/// to the shorter side, so a batch missing the deployment's columns would have
/// produced a file with the wrong shape instead of an error naming the problem.
///
/// # Columns are matched by name, never by position
///
/// Iceberg resolves columns by field id and this function is what decides which
/// id a given array lands under, so a positional match makes the *order* of the
/// declaration part of the stored contract — silently.
///
/// The failure it caused: `evolution::compare` matches strictly by name, so a
/// **permutation** of two same-typed attribute columns — `bilanzkreis`,
/// `netzgebiet` reordered to `netzgebiet`, `bilanzkreis` by an alphabetised TOML
/// or a second deployment — reports `is_identical()` and passes the compatibility
/// gate. Both are `Utf8` (config validation admits nothing else), so the
/// subsequent `cast` is a no-op and succeeds. Every row archived from then on
/// carries each value under the other's name, in the tier whose whole purpose is
/// to be read by engines that trust the schema. Nothing errors, ever.
///
/// Matching by name cannot express that mistake. The count check stays, because
/// by-name lookup alone would not notice a batch carrying a column the table does
/// not have.
fn align(
    batch: &RecordBatch,
    write_schema: &crate::arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    if batch.num_columns() != write_schema.fields().len() {
        return Err(Error::encode(
            "cold batch",
            format!(
                "batch has {} columns but the table's schema has {}: {:?} vs {:?}",
                batch.num_columns(),
                write_schema.fields().len(),
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
                write_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
            ),
        ));
    }

    let columns = write_schema
        .fields()
        .iter()
        .map(|field| {
            let array = batch.column_by_name(field.name()).ok_or_else(|| {
                Error::encode(
                    "cold batch",
                    format!(
                        "the table's schema has a column {:?} that the batch does not: {:?}",
                        field.name(),
                        batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect::<Vec<_>>(),
                    ),
                )
            })?;
            crate::arrow::compute::cast(array, field.data_type()).map_err(Error::from)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(write_schema.clone(), columns)?)
}

/// Bloom-filter sizing when the caller cannot say how many meters are involved.
///
/// The reference workload is 100 k measuring points, so a whole-day window
/// at that scale is the case worth sizing for. Over-sizing costs metadata bytes
/// and under-sizing costs false positives; neither is a correctness matter.
const DEFAULT_BLOOM_FILTER_NDV: u64 = 100_000;

/// A unique suffix so concurrent writers cannot collide on a data-file name.
///
/// **Random, not a clock.** A wall-clock reading is not unique across processes:
/// an archival run and a late-correction append in another process can sample the
/// same instant, and several platforms report far coarser than nanosecond
/// resolution regardless. Two writers agreeing on a suffix would produce the same
/// object key, and the second write would **overwrite a committed Parquet file** —
/// silent data loss in the tier whose entire job is to be durable.
///
/// 64 bits from the OS CSPRNG puts a collision beyond reach at any commit rate
/// this store will see, and it is the same entropy source a subject reference
/// already requires
/// for subject references.
fn file_suffix() -> String {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).expect("OS entropy source unavailable");
    format!("{:016x}", u64::from_be_bytes(bytes))
}

/// The partition spec: identity columns first, then `month(from)`.
///
/// # Why identity columns lead
///
/// Every query a multi-tenant deployment issues carries an equality predicate on
/// the tenant, because that is what tenancy *is*. Leading with it means a scan
/// eliminates other operators' files at the **manifest** level, before any
/// Parquet footer is opened. Without it, every scan reads every operator's files
/// and prunes by row filter — correct, and linear in the number of tenants.
///
/// It also gives erasure a bounded set of files to rewrite. Erasure pseudonymises
/// rather than rewriting, so this is not on the critical path, but a
/// tenant-scoped rewrite is the difference between touching one operator's data
/// and touching the warehouse.
///
/// # Why `month` rather than `day`
///
/// Archival runs one window per day by default, so `day(from)` would put each
/// window in its own partition and produce one small file set per day —
/// ~3 650 partitions per decade per tenant, which is how a lakehouse acquires a
/// small-file problem. A month groups ~30 windows, and the per-file `from`
/// statistics already prune within it.
///
/// # Why not `bucket(malo_id)`
///
/// Earlier drafts specified it. Rows are written sorted by `(malo_id, from)` and
/// carry a bloom filter on `malo_id`, which is what actually answers the
/// single-meter read; hashing into buckets would scatter that sort order across
/// files and add nothing a bloom filter does not already do.
fn partition_spec(
    schema: &iceberg::spec::Schema,
    identity: &[String],
) -> Result<iceberg::spec::UnboundPartitionSpec> {
    use iceberg::spec::{Transform, UnboundPartitionSpec};

    let field_id = |name: &str| -> Result<i32> {
        schema
            .field_by_name(name)
            .map(|f| f.id)
            .ok_or_else(|| Error::config(format!("partition column {name:?} is not in the schema")))
    };

    let mut builder = UnboundPartitionSpec::builder().with_spec_id(0);
    for name in identity {
        builder = builder
            .add_partition_field(field_id(name)?, name.clone(), Transform::Identity)
            .map_err(ice)?;
    }
    builder = builder
        .add_partition_field(
            field_id(schema::col::FROM)?,
            format!("{}_month", schema::col::FROM),
            Transform::Month,
        )
        .map_err(ice)?;

    Ok(builder.build())
}

/// Refuse an existing table whose partition layout is not the configured one.
///
/// Iceberg tables are created with a spec and `iceberg-rust` has no
/// `update_spec`, so a table created without a `tenant` partition field can
/// never acquire one. Loading it anyway would leave a deployment believing its
/// scans prune by tenant while every one of them reads every operator's
/// manifests — correct answers, silently linear in the number of tenants, with
/// nothing anywhere to indicate it.
///
/// This is the schema check's posture applied to layout: a table that
/// does not match its configuration halts and says why, instead of degrading
/// invisibly.
///
/// Compared by **field name**, not by id: a freshly built unbound spec has not
/// been assigned partition field ids, so ids would differ for two specs that are
/// the same spec.
fn check_partition_spec(table: &str, existing: &Table, identity: &[String]) -> Result<()> {
    let actual: Vec<&str> = existing
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();

    let month = format!("{}_month", schema::col::FROM);
    let expected: Vec<&str> = identity
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(month.as_str()))
        .collect();

    if actual == expected {
        return Ok(());
    }

    Err(Error::config(format!(
        "cold table {table} is partitioned by [{}] but this configuration wants \
         [{}]. Iceberg has no partition-spec evolution here, so the existing table \
         cannot acquire the difference: every scan would prune as the stored spec \
         allows, not as the configuration implies. Recreate the table, or declare \
         the identity columns the table was built with.",
        actual.join(", "),
        expected.join(", "),
    )))
}

/// Map an Iceberg failure into our error type.
fn ice(e: iceberg::Error) -> Error {
    Error::Storage(e.to_string())
}

#[async_trait]
impl ColdStore for IcebergCold {
    async fn create_tables(
        &self,
        table: &str,
        identity: &[String],
        extra: &[crate::arrow::datatypes::Field],
        policy: &MaintenancePolicy,
    ) -> Result<()> {
        self.create_table_with(table, extra, identity, policy)
            .await
            .map(|_| ())
    }

    async fn purge_table(&self, table: &str) -> Result<()> {
        let ident = self.ident(table);
        if !self.catalog.table_exists(&ident).await.map_err(ice)? {
            return Ok(());
        }
        // `purge_table` rather than `drop_table`: the latter removes only the
        // catalog entry, leaving every Parquet file in object storage — which
        // would make "the data is gone" false in the one operation that claims
        // it. This loads the metadata, drops the entry, then deletes the data
        // files, manifests and metadata.
        self.catalog.purge_table(&ident).await.map_err(ice)?;
        info!(table, "cold table purged");
        Ok(())
    }

    async fn watermark(&self, table: &str) -> Result<TieringWatermark> {
        let loaded = self.load(table).await?;
        watermark_of(&loaded)
    }

    async fn archival_step(&self, table: &str) -> Result<Option<time::Duration>> {
        let loaded = self.load(table).await?;
        archival_step_of(&loaded)
    }

    async fn append_and_commit(
        &self,
        table: &str,
        batches: BatchStream,
        hints: WriteHints,
        window: ArchivalWindow,
        step: time::Duration,
        now: OffsetDateTime,
    ) -> Result<CommitInfo> {
        // The watermark rides along in the same commit as the data. That is the
        // atomicity that makes recovery trivial — and the reason the summary is
        // derived from the commit base rather than fixed up front.
        self.append_with_summary(table, batches, hints, Summary::Advance(window, step, now))
            .await
    }

    async fn expire_snapshots(
        &self,
        table: &str,
        retain_for: time::Duration,
        retain_last: usize,
        now: OffsetDateTime,
    ) -> Result<usize> {
        IcebergCold::expire_snapshots(self, table, retain_for, retain_last, now).await
    }

    async fn version_stats(
        &self,
        table: &str,
        range: (OffsetDateTime, OffsetDateTime),
    ) -> Result<Vec<crate::planner::FileStats>> {
        IcebergCold::version_stats(self, table, range).await
    }

    async fn reassert_watermark(&self, table: &str) -> Result<Option<CommitInfo>> {
        IcebergCold::reassert_watermark(self, table).await
    }

    async fn snapshot_provider(
        &self,
        table: &str,
        at: SnapshotSelector,
    ) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let snapshot_id = self.resolve_snapshot(table, at).await?;
        let loaded = self.load(table).await?;
        let provider = iceberg_datafusion::IcebergStaticTableProvider::try_new_from_table_snapshot(
            loaded,
            snapshot_id,
        )
        .await
        .map_err(ice)?;
        debug!(table, snapshot_id, %at, "pinned cold provider");
        Ok(std::sync::Arc::new(provider))
    }

    async fn snapshots(&self, table: &str) -> Result<Vec<SnapshotInfo>> {
        IcebergCold::snapshots(self, table).await
    }

    async fn stored_schema(
        &self,
        table: &str,
    ) -> Result<Option<crate::arrow::datatypes::SchemaRef>> {
        // A table that does not exist yet has no schema to disagree with, and
        // that is an ordinary state: `MeterStore::create_tables` runs *after*
        // `build`, so a fresh deployment reaches the compatibility check before
        // there is anything to check against. Reporting "unknown" lets the check
        // skip rather than turning first-run into a failure.
        if !self
            .catalog
            .table_exists(&self.ident(table))
            .await
            .map_err(ice)?
        {
            return Ok(None);
        }
        let loaded = self.load(table).await?;
        let arrow = iceberg::arrow::schema_to_arrow_schema(loaded.metadata().current_schema())
            .map_err(ice)?;
        Ok(Some(std::sync::Arc::new(arrow)))
    }

    async fn append_only(
        &self,
        table: &str,
        batches: BatchStream,
        hints: WriteHints,
    ) -> Result<CommitInfo> {
        // A correction for an already-archived interval. It must not move the
        // watermark: the boundary is about which tier owns a time range, and that
        // has not changed. It must not move it *backwards* either, which is the
        // harder half — every commit re-states the watermark in its own summary,
        // so a value read before an archival commit landed would republish an
        // older boundary and make already-purged intervals vanish from every
        // query. `Preserve` re-reads it from whichever base the commit lands on,
        // including after a lost race.
        self.append_with_summary(table, batches, hints, Summary::Preserve)
            .await
    }
}

/// The durable watermark carried by a loaded table's snapshot history.
///
/// The current snapshot normally carries it, because every MeterStore commit
/// re-states it. It may not, though, and the reason is one this design actively
/// recommends: there is no native compaction, and the documented workaround is
/// to run it out of band with Spark or PyIceberg against the same standard
/// table. Such a snapshot is a perfectly valid Iceberg commit that simply knows
/// nothing about tiering.
///
/// So the lookup walks back along the parent chain to the most recent snapshot
/// MeterStore did write. Every foreign snapshot in between rewrote *files*, not
/// the interval range each tier owns, so the boundary is unchanged and the
/// older value is still the right one.
///
/// Only a table whose entire history carries no watermark is refused: that is
/// not a MeterStore table, and guessing a boundary for it would either
/// re-archive everything or strand it.
/// The nearest value of `key` in a snapshot summary, walking back the parent
/// chain from the current snapshot.
///
/// A foreign commit — an out-of-band compaction, a second deployment — carries
/// none of this crate's properties, so the current snapshot is not necessarily
/// the one that set them, and the walk is what makes those commits survivable.
///
/// Bounded by the snapshot count: a cycle in the parent chain would otherwise
/// hang the query path rather than fail it.
fn summary_property(table: &Table, key: &str) -> Option<String> {
    let metadata = table.metadata();
    let mut snapshot = metadata.current_snapshot()?.clone();
    for _ in 0..=metadata.snapshots().count() {
        if let Some(value) = snapshot.summary().additional_properties.get(key) {
            return Some(value.clone());
        }
        let parent = snapshot
            .parent_snapshot_id()
            .and_then(|id| metadata.snapshot_by_id(id))?;
        snapshot = parent.clone();
    }
    None
}

/// The archival step this table's windows were cut on, if any snapshot says.
///
/// `None` means no snapshot records one — a table with no history, or one whose
/// history predates the property. **Absent is not a mismatch**: a step that
/// cannot be read is a check that cannot run, and refusing on it would refuse
/// every table that ever archived without recording one.
fn archival_step_of(table: &Table) -> Result<Option<time::Duration>> {
    summary_property(table, ARCHIVAL_STEP_PROPERTY)
        .map(|v| crate::watermark::step_from_property(&v))
        .transpose()
}

fn watermark_of(table: &Table) -> Result<TieringWatermark> {
    let metadata = table.metadata();

    // No snapshot means nothing has been archived, so everything is hot.
    if metadata.current_snapshot().is_none() {
        return Ok(TieringWatermark::empty());
    }

    if let Some(value) = summary_property(table, WATERMARK_PROPERTY) {
        return TieringWatermark::from_property(&value);
    }

    Err(Error::InvariantViolated {
        table: table.identifier().name().to_string(),
        detail: format!(
            "no snapshot in the history of {} carries {WATERMARK_PROPERTY}; this table \
             was not written by MeterStore and the tier boundary cannot be determined",
            table.identifier()
        ),
    })
}

/// A `version` bound as an integer, if it is one.
///
/// `version` is stored as `Decimal128(20, 0)`, which Iceberg carries as an
/// unscaled 128-bit integer. Anything else means a writer disagreed with the
/// schema, and guessing would be worse than declining to prove elision.
fn as_i128(datum: &iceberg::spec::Datum) -> Option<i128> {
    use iceberg::spec::PrimitiveLiteral;
    match datum.literal() {
        PrimitiveLiteral::Int128(v) => Some(*v),
        PrimitiveLiteral::Long(v) => Some(i128::from(*v)),
        PrimitiveLiteral::Int(v) => Some(i128::from(*v)),
        _ => None,
    }
}

/// A `from` bound as a timestamp, if it is one.
fn as_timestamp(datum: &iceberg::spec::Datum) -> Option<OffsetDateTime> {
    use iceberg::spec::PrimitiveLiteral;
    let PrimitiveLiteral::Long(micros) = datum.literal() else {
        return None;
    };
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(*micros) * 1_000).ok()
}

#[cfg(test)]
mod tests {
    #[test]
    fn an_ambiguous_commit_failure_is_not_a_refusal() {
        // `Unexpected` is where a timeout, a reset connection and a 5xx arrive,
        // and upstream documents it as "Iceberg don't know what happened here".
        // Treating it as a refusal deletes the data files of a commit that may
        // already be referenced by a live snapshot.
        let ambiguous = iceberg::Error::new(iceberg::ErrorKind::Unexpected, "gateway timeout");
        assert!(!IcebergCold::commit_was_refused(&ambiguous));

        // And a lost race is not ambiguous at all — a conflict is the catalogue
        // saying somebody else's commit is the one in place.
        let conflict = iceberg::Error::new(
            iceberg::ErrorKind::CatalogCommitConflicts,
            "outdated metadata",
        );
        assert!(!IcebergCold::commit_was_refused(&conflict));
    }

    #[test]
    fn a_refused_commit_is_one_the_catalogue_said_no_to() {
        // The counterexample: if nothing counted as refused, the classifier
        // would be a constant and the files would leak on every failure.
        for kind in [
            iceberg::ErrorKind::DataInvalid,
            iceberg::ErrorKind::FeatureUnsupported,
            iceberg::ErrorKind::TableNotFound,
            iceberg::ErrorKind::NamespaceNotFound,
            iceberg::ErrorKind::PreconditionFailed,
        ] {
            let e = iceberg::Error::new(kind, "refused");
            assert!(
                IcebergCold::commit_was_refused(&e),
                "{kind:?} is a definitive refusal"
            );
        }
    }

    #[test]
    fn the_exhaustion_diagnostic_counts_the_attempts_the_loop_makes() {
        // `0..=COMMIT_ATTEMPTS` is one more attempt than the constant names, and
        // the conflict arm's own `attempt < COMMIT_ATTEMPTS` guard then sent the
        // last conflict to the generic arm — so the message below could never be
        // reached and an operator saw a raw catalogue error instead.
        assert_eq!((0..COMMIT_ATTEMPTS).count(), COMMIT_ATTEMPTS as usize);
    }

    #[test]
    fn backoff_grows_and_stays_inside_its_ceiling() {
        for attempt in 0..COMMIT_ATTEMPTS {
            let ceiling = COMMIT_BACKOFF_MS << attempt;
            for _ in 0..64 {
                let waited = backoff(attempt).as_millis() as u64;
                assert!(waited < ceiling, "attempt {attempt}: {waited} >= {ceiling}");
            }
        }
    }

    #[test]
    fn backoff_is_not_deterministic() {
        // The point of jitter: two writers that collide once must not collide
        // again on the same schedule. A fixed delay would make every sample equal.
        let samples: std::collections::HashSet<u128> =
            (0..64).map(|_| backoff(3).as_millis()).collect();
        assert!(samples.len() > 1, "backoff produced one value in 64 draws");
    }

    use super::*;

    #[test]
    fn file_name_suffixes_do_not_depend_on_the_clock() {
        // Sampled back to back, so a clock-derived suffix would repeat on any
        // platform whose timer is coarser than the loop. A repeat is not a tidy
        // collision error — it is one committed Parquet file overwriting another.
        let suffixes: std::collections::HashSet<String> =
            (0..1_000).map(|_| file_suffix()).collect();
        assert_eq!(suffixes.len(), 1_000);
        assert!(suffixes.iter().all(|s| s.len() == 16));
    }
}
