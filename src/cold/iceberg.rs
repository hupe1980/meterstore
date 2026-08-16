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

use std::collections::HashMap;

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
use tracing::{debug, info};

use crate::arrow::array::RecordBatch;
use crate::encode::schema;
use crate::error::{Error, Result};
use crate::planner::SnapshotSelector;
use crate::tiering::store::{BatchStream, ColdStore, CommitInfo, SnapshotInfo, WriteHints};
use crate::watermark::{
    ARCHIVED_RANGE_PROPERTY, ArchivalWindow, ROW_COUNT_PROPERTY, TieringWatermark,
    WATERMARK_PROPERTY,
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
    /// does not implement (§10.3.1) against the same tables.
    pub fn catalog(&self) -> std::sync::Arc<dyn Catalog> {
        std::sync::Arc::clone(&self.catalog)
    }

    fn ident(&self, table: &str) -> TableIdent {
        TableIdent::new(self.namespace.clone(), table.to_string())
    }

    /// Create the namespace and table if they do not exist.
    ///
    /// The table is named `<table>_versions` because it holds **every** version
    /// of every reading, unresolved. An external engine that reads it naively
    /// and sums `value` double-counts corrected intervals, so the name must
    /// not be the one that looks like the obvious thing to query.
    pub async fn create_table(&self, table: &str) -> Result<Table> {
        self.create_table_with(table, &[], &[]).await
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
    /// See §10.1.
    pub async fn create_table_with(
        &self,
        table: &str,
        extra: &[crate::arrow::datatypes::Field],
        identity: &[String],
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
            return self.disable_library_commit_retry(existing).await;
        }

        let arrow_schema = schema::storage_schema(extra);
        let iceberg_schema =
            iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(arrow_schema.as_ref())
                .map_err(ice)?;

        let creation = TableCreation::builder()
            .name(table.to_string())
            .partition_spec(partition_spec(&iceberg_schema, identity)?)
            .schema(iceberg_schema)
            .properties(HashMap::from([(
                COMMIT_RETRIES_PROPERTY.to_string(),
                "0".to_string(),
            )]))
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
    /// topology (§5.2) one process both archives and serves queries, so after
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
        // commits — a change to it is a schema evolution, and §11 halts the
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
        // §10.3.1 has no native compaction and recommends running it out of band
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
        let action = txn.expire_snapshots().expire_snapshot_ids(doomed);

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
    /// Every MeterStore commit stamps the boundary into its own snapshot summary
    /// (§6.2). A commit from anything else does not — and this design explicitly
    /// recommends such commits, because compaction and orphan cleanup are blocked
    /// upstream (§10.3.1, §10.5.1) and the documented answer is to run them out of
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
    ) -> Result<Vec<Option<crate::planner::VersionStats>>> {
        use crate::encode::schema::col;
        use crate::planner::VersionStats;

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
            return Ok(vec![None]);
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

                // Files provably outside the range cannot affect this scan, and
                // including them would let one untouched historical file with a
                // correction disable elision for every query.
                if let (Some(lo), Some(hi)) = (
                    data_file.lower_bounds().get(&from_id),
                    data_file.upper_bounds().get(&from_id),
                ) && let (Some(lo), Some(hi)) = (as_timestamp(lo), as_timestamp(hi))
                    && (hi < range.0 || lo >= range.1)
                {
                    continue;
                }

                stats.push(
                    match (
                        data_file.lower_bounds().get(&version_id),
                        data_file.upper_bounds().get(&version_id),
                    ) {
                        (Some(lo), Some(hi)) => match (as_i128(lo), as_i128(hi)) {
                            (Some(min), Some(max)) => Some(VersionStats { min, max }),
                            _ => None,
                        },
                        _ => None,
                    },
                );
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
    /// proportional to the window rather than to a chunk (§18).
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
        // files rather than not committing (§8.2). Closing a writer that never
        // saw a batch would produce an empty Parquet file for every such day;
        // both arms above return early instead.
        Ok((data_files, rows))
    }

    /// Turn off the library's own commit retry, so ours can run instead.
    ///
    /// See [`IcebergCold::append_with_summary`] for why re-applying a *fixed*
    /// snapshot summary against a refreshed base is wrong for this crate. Set at
    /// creation for new tables; this handles one created before the property
    /// existed, or by another tool.
    async fn disable_library_commit_retry(&self, table: Table) -> Result<Table> {
        if table.metadata().properties().get(COMMIT_RETRIES_PROPERTY) == Some(&"0".to_string()) {
            return Ok(table);
        }
        let txn = Transaction::new(&table);
        let action = txn
            .update_table_properties()
            .set(COMMIT_RETRIES_PROPERTY.to_string(), "0".to_string());
        let updated = action
            .apply(txn)
            .map_err(ice)?
            .commit(self.catalog.as_ref())
            .await
            .map_err(ice)?;
        debug!(
            table = table.identifier().name(),
            "library commit retry disabled; the watermark-preserving retry is ours"
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
    /// reporting a failure. That is the single failure §6.2 exists to make
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

        for attempt in 0..=COMMIT_ATTEMPTS {
            let watermark = summary.watermark_for(table, &base)?;
            let mut properties = HashMap::from([
                (WATERMARK_PROPERTY.to_string(), watermark.to_property()?),
                (ROW_COUNT_PROPERTY.to_string(), rows.to_string()),
            ]);
            if let Summary::Advance(window) = summary {
                properties.insert(ARCHIVED_RANGE_PROPERTY.to_string(), window.to_property()?);
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
                    });
                }
                Err(e)
                    if e.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                        && attempt < COMMIT_ATTEMPTS =>
                {
                    // Someone else committed first. Reload and rebuild the
                    // summary against what is now current — which is the whole
                    // reason this loop is not the library's.
                    debug!(
                        table,
                        attempt, "commit conflict; re-deriving against a fresh base"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(50 << attempt.min(6)))
                        .await;
                    base = self.load(table).await?;
                }
                Err(e) => return Err(ice(e)),
            }
        }

        Err(Error::Storage(format!(
            "{table}: {COMMIT_ATTEMPTS} commit attempts all lost the compare-and-swap; \
             another writer is committing continuously"
        )))
    }
}

/// What a commit's snapshot summary should say about the tier boundary.
#[derive(Debug, Clone, Copy)]
enum Summary {
    /// Archival: advance the boundary to the window's exclusive end.
    Advance(ArchivalWindow),
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
            Self::Advance(window) => {
                let next = window.resulting_watermark();
                // Asserted against the commit base, not only by the caller. An
                // archiver that read a stale watermark, or a second archiver
                // racing the first, would otherwise publish a boundary that moves
                // backwards over rows PostgreSQL has already purged.
                current
                    .advance_to(next)
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

/// How many times a lost compare-and-swap is re-derived and retried.
const COMMIT_ATTEMPTS: u32 = 4;

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

    let columns = batch
        .columns()
        .iter()
        .zip(write_schema.fields())
        .map(|(array, field)| crate::arrow::compute::cast(array, field.data_type()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(write_schema.clone(), columns)?)
}

/// Bloom-filter sizing when the caller cannot say how many meters are involved.
///
/// The §18 reference workload is 100 k measuring points, so a whole-day window
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
/// this store will see, and it is the same entropy source §19.4 already requires
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
/// It also gives erasure a bounded set of files to rewrite. §12.4 pseudonymises
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
/// carry a bloom filter on `malo_id` (§10.2), which is what actually answers the
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
/// This is §11's posture applied to layout rather than to schema: a table that
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
    ) -> Result<()> {
        self.create_table_with(table, extra, identity)
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

    async fn append_and_commit(
        &self,
        table: &str,
        batches: BatchStream,
        hints: WriteHints,
        window: ArchivalWindow,
    ) -> Result<CommitInfo> {
        // The watermark rides along in the same commit as the data. That is the
        // atomicity that makes recovery trivial — and the reason the summary is
        // derived from the commit base rather than fixed up front.
        self.append_with_summary(table, batches, hints, Summary::Advance(window))
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
    ) -> Result<Vec<Option<crate::planner::VersionStats>>> {
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
/// recommends: §10.3 has no native compaction, and the documented workaround is
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
fn watermark_of(table: &Table) -> Result<TieringWatermark> {
    let metadata = table.metadata();

    // No snapshot means nothing has been archived, so everything is hot.
    let Some(current) = metadata.current_snapshot() else {
        return Ok(TieringWatermark::empty());
    };

    let mut snapshot = current.clone();
    // Bounded by the snapshot count: a cycle in the parent chain would
    // otherwise hang the query path rather than fail it.
    for _ in 0..=metadata.snapshots().count() {
        if let Some(value) = snapshot
            .summary()
            .additional_properties
            .get(WATERMARK_PROPERTY)
        {
            return TieringWatermark::from_property(value);
        }
        let Some(parent) = snapshot
            .parent_snapshot_id()
            .and_then(|id| metadata.snapshot_by_id(id))
        else {
            break;
        };
        snapshot = parent.clone();
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
