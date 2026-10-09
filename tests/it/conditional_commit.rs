//! A cold commit lands only on the snapshot it was derived from.
//!
//! `iceberg`'s transaction reloads the table and rebases before it asks the
//! catalogue to compare-and-swap, so without a guard every check this crate runs
//! before a commit — the watermark a correction preserves, the window an
//! archival run claims — is a statement about a base the commit no longer sits
//! on. These tests put a competing commit exactly into that gap: a wrapper
//! catalogue runs it when the guarded commit reloads the table.

#![cfg(feature = "testkit")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation,
    TableIdent,
};
use meterstore::cold::IcebergCold;
use meterstore::config::TableConfig;
use meterstore::testkit::TestHarness;
use meterstore::tiering::store::{ColdStore, WriteHints, stream_of};
use meterstore::watermark::ArchivalWindow;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-01 00:00 UTC);

type Hook = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

/// Forwards every call; runs a one-shot hook before the `fire_on`-th
/// `load_table` returns.
struct Interleave {
    inner: Arc<dyn Catalog>,
    loads: AtomicUsize,
    fire_on: AtomicUsize,
    hook: Mutex<Option<Hook>>,
}

impl Interleave {
    fn new(inner: Arc<dyn Catalog>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            loads: AtomicUsize::new(0),
            fire_on: AtomicUsize::new(0),
            hook: Mutex::new(None),
        })
    }

    /// Run `hook` before the `n`-th `load_table` from now returns.
    fn arm(&self, n: usize, hook: Hook) {
        self.fire_on
            .store(self.loads.load(Ordering::SeqCst) + n, Ordering::SeqCst);
        *self.hook.lock().unwrap() = Some(hook);
    }
}

impl std::fmt::Debug for Interleave {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Interleave").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl Catalog for Interleave {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.inner.get_namespace(namespace).await
    }
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.inner.namespace_exists(namespace).await
    }
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.inner.update_namespace(namespace, properties).await
    }
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.inner.drop_namespace(namespace).await
    }
    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.inner.create_table(namespace, creation).await
    }
    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        let n = self.loads.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fire_on.load(Ordering::SeqCst) {
            let hook = self.hook.lock().unwrap().take();
            if let Some(hook) = hook {
                hook().await;
            }
        }
        self.inner.load_table(table).await
    }
    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.drop_table(table).await
    }
    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.inner.rename_table(src, dest).await
    }
    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.inner.update_table(commit).await
    }
    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.purge_table(table).await
    }
    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        self.inner.register_table(table, metadata_location).await
    }
}

/// A plain cold tier, a guarded one over the interleaving catalogue, and a table
/// whose boundary sits at `START`.
async fn setup(
    table: &str,
) -> (
    TestHarness,
    Arc<IcebergCold>,
    Arc<IcebergCold>,
    Arc<Interleave>,
) {
    let harness = TestHarness::start().await.expect("harness");
    let inner = harness.cold().catalog();
    let namespace = NamespaceIdent::new("metering".to_string());
    let plain = Arc::new(IcebergCold::new(
        Arc::clone(&inner),
        namespace.clone(),
        8 * 1024 * 1024,
    ));
    let interleave = Interleave::new(inner);
    let guarded = Arc::new(IcebergCold::new(
        Arc::clone(&interleave) as Arc<dyn Catalog>,
        namespace,
        8 * 1024 * 1024,
    ));

    let config = TableConfig::new(table).build().expect("config");
    ColdStore::create_tables(
        plain.as_ref(),
        table,
        &[],
        &config.extra_columns(),
        &config.maintenance_policy(),
    )
    .await
    .expect("cold table");
    advance(&plain, table, START - Duration::DAY, START)
        .await
        .expect("seed the boundary");
    (harness, plain, guarded, interleave)
}

async fn advance(
    cold: &IcebergCold,
    table: &str,
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> meterstore::Result<()> {
    cold.append_and_commit(
        table,
        stream_of(Vec::new()),
        WriteHints::default(),
        ArchivalWindow::new(from, to)?,
        Duration::DAY,
        to,
    )
    .await
    .map(|_| ())
}

/// A hook that archives `[START, START + 1d)` through the unguarded tier.
fn archive_next_day(plain: &Arc<IcebergCold>, table: &str) -> Hook {
    let plain = Arc::clone(plain);
    let table = table.to_string();
    Box::new(move || {
        Box::pin(async move {
            advance(&plain, &table, START, START + Duration::DAY)
                .await
                .expect("the competing archival commit lands");
        })
    })
}

#[tokio::test]
async fn a_correction_racing_archival_cannot_restate_an_older_boundary() {
    let table = "conditional_correction";
    let (_harness, plain, guarded, interleave) = setup(table).await;

    // The correction loads its base at `START` (load 1); the archival commit
    // lands when the correction's commit reloads the table (load 2).
    interleave.arm(2, archive_next_day(&plain, table));
    guarded
        .append_only(table, stream_of(Vec::new()), WriteHints::default())
        .await
        .expect("the correction lands");

    assert_eq!(
        plain.watermark(table).await.expect("watermark").get(),
        START + Duration::DAY,
        "a correction derived from the boundary at START landed over the archival \
         advance and restated the older boundary — the commit was not conditional"
    );
}

#[tokio::test]
async fn a_second_archival_commit_of_one_window_is_refused_not_landed_twice() {
    let table = "conditional_archival";
    let (_harness, plain, guarded, interleave) = setup(table).await;

    // Two archivers for one window — what a lost lease allows. The competing one
    // lands while the guarded one's commit reloads the table.
    interleave.arm(2, archive_next_day(&plain, table));
    let second = advance(&guarded, table, START, START + Duration::DAY).await;

    // Refused as a routine contended outcome, not as a fault an operator is
    // paged for: a lost lease is a deployment fact.
    assert!(
        matches!(second, Err(meterstore::Error::AlreadyArchived { .. })),
        "the second commit of [START, START+1d) must be refused, got {second:?}"
    );
    let window = ArchivalWindow::new(START, START + Duration::DAY)
        .and_then(|w| w.to_property())
        .expect("window");
    let loaded = plain.load(table).await.expect("load");
    let commits_of_window = loaded
        .metadata()
        .snapshots()
        .filter(|s| {
            s.summary()
                .additional_properties
                .get(meterstore::watermark::ARCHIVED_RANGE_PROPERTY)
                == Some(&window)
        })
        .count();
    assert_eq!(commits_of_window, 1, "the window landed more than once");
}

/// One generated day of readings, encoded as the cold tier stores them.
fn some_rows(seed: u64) -> Vec<meterstore::arrow::array::RecordBatch> {
    let series = meterstore::testkit::MeteringWorkload::new(START - Duration::DAY)
        .seed(seed)
        .malo_ids(1)
        .days(1)
        .generate()
        .expect("workload");
    vec![meterstore::encode::to_record_batch(&series).expect("encode")]
}

#[tokio::test]
async fn a_correction_whose_replay_check_went_stale_is_returned_for_redelivery() {
    let table = "conditional_stale_replay";
    let (_harness, plain, guarded, interleave) = setup(table).await;

    // A second correction carrying rows lands while the first commits. The first
    // decided which of its rows were replays against a base that no longer
    // holds; landing it anyway could store the same readings twice.
    let competing = Arc::clone(&plain);
    let name = table.to_string();
    interleave.arm(
        2,
        Box::new(move || {
            Box::pin(async move {
                competing
                    .append_only(&name, stream_of(some_rows(1)), WriteHints::default())
                    .await
                    .expect("the competing correction lands");
            })
        }),
    );
    let outcome = guarded
        .append_only(table, stream_of(some_rows(2)), WriteHints::default())
        .await;

    match outcome {
        Err(e) => assert!(e.is_retryable(), "redelivery is the remedy: {e}"),
        Ok(info) => panic!("a correction landed over a stale replay check: {info:?}"),
    }
}

/// Commit the way an out-of-band compaction does: valid Iceberg, saying nothing
/// about tiering, so the next MeterStore commit has to restate the boundary.
async fn foreign_commit(cold: &IcebergCold, table: &str) {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let loaded = cold.load(table).await.expect("load");
    let txn = Transaction::new(&loaded);
    txn.fast_append()
        .add_data_files(Vec::new())
        .set_snapshot_properties(HashMap::from([(
            "engine".to_string(),
            "out-of-band-compaction".to_string(),
        )]))
        .apply(txn)
        .expect("apply")
        .commit(cold.catalog().as_ref())
        .await
        .expect("a foreign commit");
}

#[tokio::test]
async fn a_restamp_racing_archival_cannot_restate_an_older_boundary() {
    let table = "conditional_restamp";
    let (_harness, plain, guarded, interleave) = setup(table).await;
    foreign_commit(&plain, table).await;

    // The re-stamp reads the boundary (load 1), derives its commit from the
    // table (load 2), and the archival commit lands as that commit reloads
    // (load 3). It takes no lease, so nothing else orders the two.
    interleave.arm(3, archive_next_day(&plain, table));
    guarded
        .reassert_watermark(table)
        .await
        .expect("the re-stamp lands or has nothing to do");

    assert_eq!(
        plain.watermark(table).await.expect("watermark").get(),
        START + Duration::DAY,
        "the re-stamp republished the boundary it read over the archival advance"
    );
}

#[tokio::test]
async fn expiry_racing_archival_cannot_restate_an_older_boundary() {
    let table = "conditional_expiry";
    let (_harness, plain, guarded, interleave) = setup(table).await;
    foreign_commit(&plain, table).await;

    // Expiry re-stamps first, after the archive lease is released: the same
    // three loads as a bare re-stamp.
    interleave.arm(3, archive_next_day(&plain, table));
    let _ = guarded
        .expire_snapshots(table, Duration::ZERO, 1, START + Duration::days(400))
        .await;

    assert_eq!(
        plain.watermark(table).await.expect("watermark").get(),
        START + Duration::DAY,
        "expiry's re-stamp republished the boundary it read over the archival advance"
    );
}

/// Commits through the inner catalogue, then answers as a timeout would: the
/// update landed and the caller was told nothing about it.
#[derive(Debug)]
struct LandsThenTimesOut {
    inner: Arc<dyn Catalog>,
}

#[async_trait::async_trait]
impl Catalog for LandsThenTimesOut {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.inner.get_namespace(namespace).await
    }
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.inner.namespace_exists(namespace).await
    }
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.inner.update_namespace(namespace, properties).await
    }
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.inner.drop_namespace(namespace).await
    }
    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.inner.create_table(namespace, creation).await
    }
    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        self.inner.load_table(table).await
    }
    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.drop_table(table).await
    }
    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.inner.rename_table(src, dest).await
    }
    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.inner.update_table(commit).await?;
        Err(iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "gateway timeout",
        ))
    }
    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.purge_table(table).await
    }
    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        self.inner.register_table(table, metadata_location).await
    }
}

#[tokio::test]
async fn a_commit_that_landed_unanswered_is_reported_as_landed() {
    // A timeout after the catalogue applied the update. Reported as a failure,
    // the caller retries and the rows land twice; the files discarded as if
    // nothing landed, a live snapshot references deleted files. The commit's
    // own id in the summary says which it was.
    let table = "conditional_ambiguous";
    let (harness, plain, _guarded, _interleave) = setup(table).await;
    let timing_out = IcebergCold::new(
        Arc::new(LandsThenTimesOut {
            inner: harness.cold().catalog(),
        }) as Arc<dyn Catalog>,
        NamespaceIdent::new("metering".to_string()),
        8 * 1024 * 1024,
    );

    let landed = timing_out
        .append_only(table, stream_of(some_rows(3)), WriteHints::default())
        .await
        .expect("the commit landed, and its id says so");
    assert!(landed.rows > 0, "{landed:?}");

    // The landed snapshot is this commit, and the files it references were
    // kept rather than discarded as an unknown outcome's would be.
    let loaded = plain.load(table).await.expect("load");
    let current = loaded.metadata().current_snapshot().expect("snapshot");
    assert!(
        current
            .summary()
            .additional_properties
            .contains_key(meterstore::watermark::COMMIT_ID_PROPERTY),
        "the current snapshot is the commit that landed"
    );
    assert!(
        !harness.parquet_files().is_empty(),
        "the landed snapshot's files are still in the warehouse"
    );
}
