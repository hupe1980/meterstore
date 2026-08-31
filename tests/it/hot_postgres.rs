//! Integration tests for the PostgreSQL hot tier, against a real server.
//!
//! These assert the properties that only a real database can show: that the
//! partition really is detached, that the purge really is `DROP TABLE` and
//! leaves no dead tuples, and that a detached partition really does survive a
//! crash intact.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them: `testkit::postgres` is what shares one container
// across the binary instead of starting one per test (§17.2.0.1).
#![cfg(feature = "testkit")]

use meterstore::arrow::array::RecordBatch;
use meterstore::encode::schema::col;
use meterstore::hot::PostgresHot;
use meterstore::tiering::store::{BatchStream, HotStore, PartitionId, ScanSpec};

/// Drain a scan stream, which is what a cold store does on the way to Parquet.
async fn collect_stream(stream: BatchStream) -> Vec<datafusion::arrow::array::RecordBatch> {
    use futures::StreamExt;
    stream.map(|b| b.expect("batch")).collect::<Vec<_>>().await
}
use meterstore::watermark::TieringWatermark;

use metering::measurement_series::MeasurementSource;
use sqlx::{PgPool, Row};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const TABLE: &str = "readings";
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);
const D22: OffsetDateTime = datetime!(2026-07-22 00:00 UTC);

/// A running Postgres plus a prepared hot table.
struct Harness {
    hot: PostgresHot,
}

impl Harness {
    async fn start() -> Self {
        let url = meterstore::testkit::postgres::fresh_database()
            .await
            .expect("postgres");

        let pool = PgPool::connect(&url).await.expect("connect");
        let hot = PostgresHot::new(pool);
        hot.create_table(TABLE).await.expect("create table");

        Self { hot }
    }

    fn pool(&self) -> &PgPool {
        self.hot.pool()
    }

    /// Insert `count` quarter-hour readings starting at `start`.
    ///
    /// The provenance payload is produced by serializing a real
    /// `MeasurementSource`, never hand-written: a literal fixture would silently
    /// drift from the actual representation.
    async fn insert_readings(&self, start: OffsetDateTime, count: i64) {
        let source = MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
        };
        let source_detail = serde_json::to_string(&source).expect("serialize source");

        for i in 0..count {
            let from = start + Duration::minutes(15 * i);
            sqlx::query(
                r#"INSERT INTO readings
                   (malo_id, melo_id, obis_code, sparte, "from", "to", value, unit,
                    quality, resolution, source_kind, source_detail, provenance,
                    version, version_scope, recorded_at, balancing_day)
                   VALUES ($1,$2,$3,'STROM',$4,$5,$6,'KWH',$7,$8,$9,$10,$11,$12,$13,$14,
                           CAST(($4 AT TIME ZONE 'Europe/Berlin') AS DATE))"#,
            )
            .bind("12345678905")
            .bind(Some("DE0001234567890123456789012345678"))
            .bind(meterstore::canonical_obis("1-0:1.8.0").unwrap())
            .bind(from)
            .bind(from + Duration::minutes(15))
            .bind(rust_decimal::Decimal::new(1_234_567, 6))
            .bind(metering::QualityFlag::Measured.as_str())
            .bind(Some("PT15M"))
            .bind("MSCONS")
            .bind(Some(source_detail.as_str()))
            .bind(Some("[]"))
            .bind(rust_decimal::Decimal::new(20_260_727_000_001, 0))
            .bind("9900000000001:2026-07")
            .bind(datetime!(2026-07-27 06:00 UTC))
            .execute(self.pool())
            .await
            .expect("insert reading");
        }
    }

    async fn row_count(&self) -> i64 {
        sqlx::query_scalar::<_, i64>(r#"SELECT count(*) FROM "readings""#)
            .fetch_one(self.pool())
            .await
            .expect("count")
    }

    async fn relation_exists(&self, name: &str) -> bool {
        sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1)")
            .bind(name)
            .fetch_one(self.pool())
            .await
            .expect("relation lookup")
    }

    /// Dead tuples Postgres believes the table holds.
    async fn dead_tuples(&self) -> i64 {
        sqlx::query_scalar::<_, Option<i64>>(
            "SELECT sum(n_dead_tup)::bigint FROM pg_stat_all_tables
              WHERE relname LIKE 'readings%'",
        )
        .fetch_one(self.pool())
        .await
        .expect("dead tuple stats")
        .unwrap_or(0)
    }
}

#[tokio::test]
async fn creates_partitions_across_a_range() {
    let h = Harness::start().await;

    let created = h
        .hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();

    assert_eq!(created.len(), 2);
    assert!(h.relation_exists("readings_2026_07_20_0000").await);
    assert!(h.relation_exists("readings_2026_07_21_0000").await);
}

#[tokio::test]
async fn ensure_partitions_is_idempotent() {
    let h = Harness::start().await;

    let first = h
        .hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();
    let second = h
        .hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();

    assert_eq!(first.len(), 2);
    assert!(second.is_empty(), "second run must create nothing");
}

#[tokio::test]
async fn partition_bounds_are_aligned_regardless_of_the_requested_start() {
    // Asking from mid-day must still produce a day-aligned partition, or a
    // window would span two partitions and the purge could not be a single drop.
    let h = Harness::start().await;

    h.hot
        .ensure_partitions(TABLE, datetime!(2026-07-20 13:47 UTC), D21, Duration::DAY)
        .await
        .unwrap();

    assert!(h.relation_exists("readings_2026_07_20_0000").await);
}

#[tokio::test]
async fn rows_route_to_the_partition_covering_their_interval() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();

    h.insert_readings(D20, 4).await;
    h.insert_readings(D21, 6).await;

    let in_first =
        sqlx::query_scalar::<_, i64>(r#"SELECT count(*) FROM "readings_2026_07_20_0000""#)
            .fetch_one(h.pool())
            .await
            .unwrap();
    assert_eq!(in_first, 4);
    assert_eq!(h.row_count().await, 10);
}

#[tokio::test]
async fn insert_fails_when_no_partition_covers_the_interval() {
    // This is why partitions are pre-created with headroom: running out does not
    // degrade, it stops writes outright.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    let result = sqlx::query(
        r#"INSERT INTO readings
           (malo_id, obis_code, sparte, "from", "to", value, unit, quality,
            source_kind, version, version_scope, recorded_at, balancing_day)
           VALUES ('1','1-0:1.8.0','STROM',$1,$2,1.0,'KWH','MEASURED','MSCONS',1,
                   '9900000000001:2026-07',$1,CAST(($1 AT TIME ZONE 'Europe/Berlin') AS DATE))"#,
    )
    .bind(D22)
    .bind(D22 + Duration::minutes(15))
    .execute(h.pool())
    .await;

    assert!(result.is_err(), "insert outside any partition must fail");
}

#[tokio::test]
async fn detach_hides_rows_from_the_parent_but_keeps_them_readable() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 8).await;
    assert_eq!(h.row_count().await, 8);

    let partition = PartitionId::new(TABLE, D20);
    h.hot.detach_partition(&partition).await.unwrap();

    assert_eq!(h.row_count().await, 0, "parent must no longer see the rows");
    assert!(
        h.relation_exists("readings_2026_07_20_0000").await,
        "the data must still exist"
    );

    let batches = collect_stream(
        h.hot
            .scan_detached(&partition, &ScanSpec::core())
            .await
            .unwrap(),
    )
    .await;
    let scanned: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(scanned, 8, "archiver must still be able to read it");
}

#[tokio::test]
async fn a_partition_being_archived_is_still_readable_through_the_query_path() {
    // Archival detaches a partition before it reads it and drops it only after
    // the cold commit lands. The watermark is published *by* that commit, so for
    // as long as writing a window takes — a day of 9.6 M rows is not an instant —
    // the rows are in neither place a query looks: not in the parent, because
    // they are detached; not in Iceberg, because the commit has not landed; and
    // the boundary still says the hot tier owns the range.
    //
    // So `scan_range` reads the parent *and* whatever is detached from it, or a
    // settlement running at that moment comes back a whole day short with
    // nothing reporting it.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 8).await;

    h.hot
        .detach_partition(&PartitionId::new(TABLE, D20))
        .await
        .unwrap();
    assert_eq!(h.row_count().await, 0, "the parent no longer holds them");

    let scanned: usize = collect_stream(
        h.hot
            .scan_range(
                TABLE,
                meterstore::planner::TimeRange::between(D20, D21),
                &ScanSpec::core(),
            )
            .await
            .unwrap(),
    )
    .await
    .iter()
    .map(|b| b.num_rows())
    .sum();

    assert_eq!(scanned, 8, "a query must not lose a window mid-archival");
}

#[tokio::test]
async fn a_detached_window_the_scan_does_not_ask_for_is_not_read_twice() {
    // The other half of the argument. Once the commit has landed the watermark
    // is past the window, so the hot half of any split starts above it — and the
    // rows, which are now in Iceberg, must not also come back from the orphan
    // that is still waiting to be dropped.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 8).await;
    h.insert_readings(D21, 4).await;

    h.hot
        .detach_partition(&PartitionId::new(TABLE, D20))
        .await
        .unwrap();

    let scanned: usize = collect_stream(
        h.hot
            .scan_range(
                TABLE,
                // What the split hands the hot tier once the watermark is D21.
                meterstore::planner::TimeRange::between(D21, D22),
                &ScanSpec::core(),
            )
            .await
            .unwrap(),
    )
    .await
    .iter()
    .map(|b| b.num_rows())
    .sum();

    assert_eq!(scanned, 4, "the archived window is Iceberg's now");
}

#[tokio::test]
async fn a_changed_merge_key_is_refused_rather_than_silently_ignored() {
    // `create_tables` runs on every start and is a no-op on an existing table,
    // which is what makes a *changed* declaration dangerous rather than merely
    // ineffective. Widen the merge key and restart: resolution partitions by the
    // new key while the table enforces the old primary key, so the second
    // reading the wider key exists to admit conflicts on the narrower one and is
    // skipped — invisible to the divergence check too, which joins on the new
    // key.
    let h = Harness::start().await;

    let widened: Vec<String> = ["malo_id", "melo_id", "obis_code", "from"]
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let err = h
        .hot
        .create_table_with_key(
            TABLE,
            &widened,
            &[],
            meterstore::config::TimeModel::Interval,
        )
        .await
        .expect_err("a merge key cannot change under an existing table")
        .to_string();

    assert!(err.contains("primary key"), "{err}");
    assert!(
        err.contains("melo_id"),
        "the message names the difference: {err}"
    );

    // The declaration it was created with is still accepted, so an ordinary
    // restart is unaffected.
    h.hot.create_table(TABLE).await.expect("idempotent");
}

#[tokio::test]
async fn a_changed_time_model_is_refused_too() {
    // `value` is interval energy on one table and a cumulative register reading
    // on the other. Switching the declaration under an existing table would
    // leave a column no aggregate can interpret.
    let h = Harness::start().await;

    let key: Vec<String> = ["malo_id", "obis_code", "from"]
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let err = h
        .hot
        .create_table_with_key(TABLE, &key, &[], meterstore::config::TimeModel::Point)
        .await
        .expect_err("an interval table cannot become a point table")
        .to_string();

    assert!(err.contains("INTERVAL") && err.contains("POINT"), "{err}");
}

#[tokio::test]
async fn detached_partition_survives_as_an_orphan() {
    // The crash window between the cold commit and the drop.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 4).await;

    h.hot
        .detach_partition(&PartitionId::new(TABLE, D20))
        .await
        .unwrap();

    let orphans = h.hot.orphaned_partitions(TABLE).await.unwrap();
    assert_eq!(orphans.len(), 1);
    assert_eq!(orphans[0].start(), D20);
}

/// A session holding `ACCESS SHARE` on the parent, as an ordinary query does.
///
/// Opened in its own transaction on its own connection and left open, so the
/// lock is still held while the test runs DDL from another connection. That is
/// exactly the shape of an analytical query running against the hot table while
/// the archival loop comes round.
struct HoldingAccessShare {
    _pool: PgPool,
    tx: Option<sqlx::Transaction<'static, sqlx::Postgres>>,
}

impl HoldingAccessShare {
    async fn on(url: &str, table: &str) -> Self {
        let pool = PgPool::connect(url).await.expect("connect");
        // Leaked so the transaction can outlive this call; released on `drop`.
        let leaked: &'static PgPool = Box::leak(Box::new(pool.clone()));
        let mut tx = leaked.begin().await.expect("begin");
        sqlx::query(&format!(r#"SELECT count(*) FROM "{table}""#))
            .fetch_one(&mut *tx)
            .await
            .expect("read the parent, taking ACCESS SHARE");
        Self {
            _pool: pool,
            tx: Some(tx),
        }
    }

    async fn release(mut self) {
        if let Some(tx) = self.tx.take() {
            tx.rollback().await.expect("rollback");
        }
    }
}

#[tokio::test]
async fn creating_a_partition_does_not_block_on_a_reader() {
    // `CREATE TABLE … PARTITION OF` takes ACCESS EXCLUSIVE on the parent, which
    // conflicts with the ACCESS SHARE every `SELECT` holds — so with that
    // spelling this test would sit out the DDL lock timeout and fail.
    //
    // Partition creation runs on the **write path**: an append reaching past the
    // pre-created frontier makes what it needs. Building the relation standalone
    // and attaching it takes only SHARE UPDATE EXCLUSIVE, which conflicts with
    // no read and no write at all, so ingest cannot be stalled by a query.
    let h = Harness::start().await;
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let pool = PgPool::connect(&url).await.expect("connect");
    let hot = PostgresHot::new(pool).ddl_lock_timeout(Duration::seconds(2));
    hot.create_table(TABLE).await.expect("create table");

    let reader = HoldingAccessShare::on(&url, TABLE).await;

    // The property, and the whole of it: with a conflicting ACCESS SHARE held for
    // the entire call, creation succeeds. Had it queued it would have sat out the
    // two-second DDL lock timeout and come back `LockTimeout` — the reader is not
    // released until afterwards, so there is no third outcome to be lucky about.
    let made = hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .expect("a reader must not be able to block partition creation");
    assert_eq!(made.len(), 1);

    // And the negative, which is what makes the positive mean something: the
    // spelling this crate does *not* use really does block on the same reader.
    //
    // Asserted rather than timed. A wall-clock bound on the call above would be
    // a weaker restatement of what `expect` already proves, and would fail on a
    // loaded machine for a reason that has nothing to do with locks — the suite
    // runs twenty-odd databases against one server.
    let mut blocking = PgPool::connect(&url)
        .await
        .expect("connect")
        .acquire()
        .await
        .expect("connection");
    sqlx::query("SET lock_timeout = '1s'")
        .execute(&mut *blocking)
        .await
        .expect("set");
    let naive = sqlx::query(&format!(
        r#"CREATE TABLE "{TABLE}_naive" PARTITION OF "{TABLE}"
               FOR VALUES FROM ('2026-07-21 00:00:00+00') TO ('2026-07-22 00:00:00+00')"#
    ))
    .execute(&mut *blocking)
    .await;
    let err = naive
        .expect_err("CREATE TABLE … PARTITION OF takes ACCESS EXCLUSIVE on the parent")
        .to_string();
    assert!(
        err.contains("lock timeout") || err.contains("canceling statement"),
        "it should have queued behind the reader's ACCESS SHARE and timed out: {err}"
    );

    reader.release().await;
    drop(h);
}

#[tokio::test]
async fn a_detach_that_cannot_get_its_lock_reports_a_lock_timeout() {
    // Detaching genuinely needs ACCESS EXCLUSIVE on the parent, and PostgreSQL
    // grants locks in arrival order — so a detach that *waits* also blocks every
    // reader and writer that arrives behind it. It gives up instead, and the
    // failure is typed so the archival loop can report the cycle as deferred
    // rather than failed.
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let pool = PgPool::connect(&url).await.expect("connect");
    let hot = PostgresHot::new(pool).ddl_lock_timeout(Duration::milliseconds(250));
    hot.create_table(TABLE).await.expect("create table");
    hot.ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    let reader = HoldingAccessShare::on(&url, TABLE).await;

    let err = hot
        .detach_partition(&PartitionId::new(TABLE, D20))
        .await
        .expect_err("a reader holds ACCESS SHARE, so the detach cannot proceed");

    assert!(
        matches!(err, meterstore::Error::LockTimeout { .. }),
        "expected a typed lock timeout, got {err:?}"
    );
    assert!(err.is_retryable());

    // And once the reader is gone the very same call succeeds — nothing was
    // left half-done.
    reader.release().await;
    hot.detach_partition(&PartitionId::new(TABLE, D20))
        .await
        .expect("the lock is free now");
}

#[tokio::test]
async fn a_created_partition_carries_its_bounds_and_no_redundant_check() {
    // The bound `CHECK` exists only so `ATTACH` can prove the partition
    // constraint from the catalogue and skip its validation scan. Left in place
    // it would be a second predicate evaluated on every inserted row, so it is
    // dropped again — and the partition constraint the attach installed is what
    // remains.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    let name = PartitionId::new(TABLE, D20).relation_name().unwrap();

    let bound: Option<String> = sqlx::query_scalar(
        "SELECT pg_get_expr(c.relpartbound, c.oid)
           FROM pg_class c WHERE c.relname = $1",
    )
    .bind(&name)
    .fetch_one(h.pool())
    .await
    .unwrap();
    let bound = bound.expect("the relation is attached as a partition");
    assert!(bound.contains("2026-07-20"), "{bound}");
    assert!(bound.contains("2026-07-21"), "{bound}");

    let checks: Vec<String> = sqlx::query_scalar(
        "SELECT conname FROM pg_constraint
          WHERE conrelid = $1::regclass AND contype = 'c'",
    )
    .bind(&name)
    .fetch_all(h.pool())
    .await
    .unwrap();
    assert!(
        !checks.iter().any(|c| c.ends_with("_bound")),
        "the redundant bound check must be dropped after the attach: {checks:?}"
    );

    // The parent's own CHECK constraints are still enforced on the partition, and
    // **exactly once**. `LIKE … INCLUDING CONSTRAINTS` puts a local copy on the
    // relation and the attach merges it with the inherited one; a copy the merge
    // failed to match would land beside it as `sparte_known1`, and every inserted
    // row would then be checked twice against the same predicate for ever.
    let parent_checks: Vec<String> = sqlx::query_scalar(
        "SELECT conname FROM pg_constraint
          WHERE conrelid = $1::regclass AND contype = 'c'
          ORDER BY conname",
    )
    .bind(TABLE)
    .fetch_all(h.pool())
    .await
    .unwrap();

    let mut inherited: Vec<&String> = checks
        .iter()
        .filter(|c| !c.starts_with(&name))
        .collect::<Vec<_>>();
    inherited.sort();
    assert_eq!(
        inherited,
        parent_checks.iter().collect::<Vec<_>>(),
        "the partition must carry the parent's checks once each, no more and no fewer"
    );
    assert!(
        parent_checks.iter().any(|c| c == "sparte_known"),
        "the parent's own vocabulary checks should be among them: {parent_checks:?}"
    );
}

#[tokio::test]
async fn attached_partitions_are_not_reported_as_orphans() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();

    assert!(h.hot.orphaned_partitions(TABLE).await.unwrap().is_empty());
}

#[tokio::test]
async fn scan_returns_the_storage_schema_in_merge_key_order() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 3).await;

    let partition = PartitionId::new(TABLE, D20);
    h.hot.detach_partition(&partition).await.unwrap();
    let batches = collect_stream(
        h.hot
            .scan_detached(&partition, &ScanSpec::core())
            .await
            .unwrap(),
    )
    .await;

    let batch = &batches[0];
    assert_eq!(
        batch.schema(),
        meterstore::encode::schema::storage_schema(&[])
    );

    // Values must survive the Postgres round trip exactly.
    let decoded = meterstore::encode::from_record_batch(batch).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].series.intervals.len(), 3);
    assert_eq!(
        decoded[0].series.intervals[0].value,
        "1.234567".parse::<rust_decimal::Decimal>().unwrap(),
        "decimals must not lose precision through NUMERIC"
    );

    let from = batch
        .column_by_name(col::FROM)
        .expect("from column present");
    assert_eq!(from.len(), 3);
}

#[tokio::test]
async fn scanning_an_empty_partition_yields_no_batches() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    let partition = PartitionId::new(TABLE, D20);
    h.hot.detach_partition(&partition).await.unwrap();

    assert!(
        collect_stream(
            h.hot
                .scan_detached(&partition, &ScanSpec::core())
                .await
                .unwrap()
        )
        .await
        .is_empty()
    );
}

#[tokio::test]
async fn purge_creates_no_dead_tuples() {
    // The whole reason the hot table is partitioned. A row-wise DELETE of this
    // data would leave one dead tuple per row for autovacuum to clean up.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 96).await;

    sqlx::query("ANALYZE").execute(h.pool()).await.unwrap();
    let before = h.dead_tuples().await;

    let partition = PartitionId::new(TABLE, D20);
    h.hot.detach_partition(&partition).await.unwrap();
    h.hot.drop_partition(&partition).await.unwrap();

    sqlx::query("ANALYZE").execute(h.pool()).await.unwrap();
    let after = h.dead_tuples().await;

    assert!(!h.relation_exists("readings_2026_07_20_0000").await);
    assert_eq!(
        after, before,
        "dropping a partition must not produce dead tuples"
    );
    assert_eq!(h.row_count().await, 0);
}

#[tokio::test]
async fn drop_is_idempotent() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    let partition = PartitionId::new(TABLE, D20);
    h.hot.detach_partition(&partition).await.unwrap();
    h.hot.drop_partition(&partition).await.unwrap();
    h.hot.drop_partition(&partition).await.unwrap();
}

#[tokio::test]
async fn a_non_canonical_obis_code_is_rejected_at_the_write() {
    // `1-0:1.8.0*255` and `1-0:1.8.0` denote the same channel. Storing both
    // would give one channel two merge keys, so a correction written one way
    // could not supersede a value written the other. The constraint turns that
    // silent resolution failure into an immediate write failure.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    for bad in ["1-0:1.8.0*255", "not-an-obis", "1-0:1.8"] {
        let result = sqlx::query(
            r#"INSERT INTO readings
               (malo_id, obis_code, sparte, "from", "to", value, unit, quality,
                source_kind, version, version_scope, recorded_at, balancing_day)
               VALUES ('1',$1,'STROM',$2,$3,1.0,'KWH','MEASURED','MSCONS',1,
                       '9900000000001:2026-07',$2,CAST(($2 AT TIME ZONE 'Europe/Berlin') AS DATE))"#,
        )
        .bind(bad)
        .bind(D20)
        .bind(D20 + Duration::minutes(15))
        .execute(h.pool())
        .await;

        assert!(result.is_err(), "{bad:?} must be rejected");
    }
}

#[tokio::test]
async fn a_storage_group_that_carries_information_is_accepted() {
    // `*255` means "unused" and is elided; any other group is real data — a
    // historical billing period, say — and must survive.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    sqlx::query(
        r#"INSERT INTO readings
           (malo_id, obis_code, sparte, "from", "to", value, unit, quality,
            source_kind, version, version_scope, recorded_at, balancing_day)
           VALUES ('1','1-0:1.8.0*1','STROM',$1,$2,1.0,'KWH','MEASURED','MSCONS',1,
                   '9900000000001:2026-07',$1,CAST(($1 AT TIME ZONE 'Europe/Berlin') AS DATE))"#,
    )
    .bind(D20)
    .bind(D20 + Duration::minutes(15))
    .execute(h.pool())
    .await
    .expect("a meaningful storage group must be storable");
}

#[tokio::test]
async fn quality_is_stored_as_its_stable_code_not_an_integer() {
    // The stored value is self-describing: an external engine reading the
    // Parquet sees SUBSTITUTED, not an opaque 2 whose meaning lives in our code.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 1).await;

    let stored: String = sqlx::query_scalar(r#"SELECT quality FROM readings LIMIT 1"#)
        .fetch_one(h.pool())
        .await
        .unwrap();
    assert_eq!(stored, metering::QualityFlag::Measured.as_str());
    assert_eq!(
        stored.parse::<metering::QualityFlag>().unwrap(),
        metering::QualityFlag::Measured
    );
}

/// The default merge key, for tests that declare no identity columns.
fn default_key() -> Vec<String> {
    meterstore::encode::schema::MERGE_KEY
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

/// A batch of quarter-hour readings in the storage schema.
fn batch(start: OffsetDateTime, count: usize, kwh: i64, version: i64) -> RecordBatch {
    use meterstore::arrow::array::{Decimal128Array, StringArray, TimestampMicrosecondArray};
    use meterstore::encode::schema;
    use std::sync::Arc;

    let schema_ref = schema::storage_schema(&[]);
    let micros = |t: OffsetDateTime| (t.unix_timestamp_nanos() / 1_000) as i64;
    let obis = meterstore::canonical_obis("1-0:1.8.0").unwrap();

    let froms: Vec<i64> = (0..count)
        .map(|i| micros(start + Duration::minutes(15 * i as i64)))
        .collect();
    let tos: Vec<i64> = froms.iter().map(|f| f + 15 * 60 * 1_000_000).collect();

    let columns: Vec<meterstore::arrow::array::ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["11111111115"; count])),
        Arc::new(StringArray::from(vec![None::<&str>; count])),
        Arc::new(StringArray::from(vec![obis.as_str(); count])),
        Arc::new(StringArray::from(vec!["STROM"; count])),
        Arc::new(TimestampMicrosecondArray::from(froms).with_timezone("UTC")),
        Arc::new(TimestampMicrosecondArray::from(tos).with_timezone("UTC")),
        Arc::new(
            Decimal128Array::from(vec![i128::from(kwh) * 1_000_000; count])
                .with_precision_and_scale(schema::VALUE_PRECISION, schema::VALUE_SCALE)
                .unwrap(),
        ),
        Arc::new(StringArray::from(vec!["KWH"; count])),
        Arc::new(StringArray::from(vec!["MEASURED"; count])),
        Arc::new(StringArray::from(vec!["PT15M"; count])),
        Arc::new(StringArray::from(vec!["MSCONS"; count])),
        Arc::new(StringArray::from(vec!["{}"; count])),
        Arc::new(StringArray::from(vec!["[]"; count])),
        Arc::new(
            Decimal128Array::from(vec![i128::from(version); count])
                .with_precision_and_scale(schema::VERSION_PRECISION, schema::VERSION_SCALE)
                .unwrap(),
        ),
        Arc::new(StringArray::from(vec!["9900000000001:2026-07"; count])),
        Arc::new(TimestampMicrosecondArray::from(vec![micros(D20); count]).with_timezone("UTC")),
        // The balancing day, as the encoder would derive it. STROM, so the
        // Berlin calendar day rather than the Gastag.
        Arc::new(meterstore::arrow::array::Date32Array::from(
            (0..count)
                .map(|i| {
                    let at = start + Duration::minutes(15 * i as i64);
                    let day =
                        meterstore::planner::balancing_day(at, metering::interval::Sparte::Strom);
                    (day - time::Date::from_ordinal_date(1970, 1).unwrap()).whole_days() as i32
                })
                .collect::<Vec<i32>>(),
        )),
    ];
    RecordBatch::try_new(schema_ref, columns).unwrap()
}

#[tokio::test]
async fn a_redelivered_batch_is_idempotent() {
    // Every real ingest transport delivers at least once — Kafka redelivers on
    // a failed commit, webhooks retry on a timeout. Replay is therefore normal,
    // not exceptional, and a store that errors on it cannot be driven by one.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    let b = batch(D20, 4, 10, 20_260_720_000_001);

    let first = h
        .hot
        .append(TABLE, &default_key(), std::slice::from_ref(&b))
        .await
        .unwrap();
    let second = h
        .hot
        .append(TABLE, &default_key(), std::slice::from_ref(&b))
        .await
        .expect("a replayed batch must not error");

    assert_eq!(first, 4, "first delivery writes every row");
    assert_eq!(second, 0, "replay writes nothing new");
    assert_eq!(h.row_count().await, 4, "and stores nothing twice");
}

#[tokio::test]
async fn the_same_version_may_not_carry_a_different_value() {
    // A version identifies an assertion. Two different values under one version
    // means a producer is wrong, and silently keeping either would bury it.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    h.hot
        .append(
            TABLE,
            &default_key(),
            &[batch(D20, 2, 10, 20_260_720_000_001)],
        )
        .await
        .unwrap();

    let err = h
        .hot
        .append(
            TABLE,
            &default_key(),
            &[batch(D20, 2, 99, 20_260_720_000_001)],
        )
        .await
        .expect_err("a differing value under the same version must be rejected, not ignored");

    // And typed as a refused *delivery*, not as a broken invariant: nothing
    // about the store is wrong, the producer sent a row that contradicts one it
    // already sent. Retrying it never succeeds, and it must not page whoever is
    // alerted on the tiering invariant.
    assert!(
        matches!(&err, meterstore::Error::IntegrityViolation { constraint, .. }
                 if constraint.as_deref() == Some("version_identifies_one_assertion")),
        "expected a typed integrity violation, got {err:?}"
    );
    assert!(!err.is_retryable());
}

#[tokio::test]
async fn a_range_scan_streams_in_bounded_chunks() {
    // The point of streaming: a query over the whole hot window must not
    // materialise it. With a chunk smaller than the range, the scan has to make
    // several round trips and emit several batches — if it returned one batch
    // it had collected everything first.
    use futures::StreamExt;

    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 25).await;

    let hot = PostgresHot::new(h.pool().clone()).scan_chunk_rows(10);
    let mut stream = hot
        .scan_range(
            TABLE,
            meterstore::planner::TimeRange::unbounded(),
            &ScanSpec::core(),
        )
        .await
        .unwrap();

    let mut batches = 0;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        assert!(
            batch.num_rows() <= 10,
            "a batch must not exceed the chunk size"
        );
        batches += 1;
        rows += batch.num_rows();
    }

    assert_eq!(rows, 25, "every row is still delivered");
    // **Exactly** the round trips the chunk size implies, not merely "several".
    // The two failure modes sit on either side of this number and a `>=` would
    // catch only one: a scan that materialised the range first yields 1 batch,
    // and a scan that made a round trip per row yields 25 — which passes both
    // `>= 3` and the per-batch size bound, silently.
    assert_eq!(
        batches, 3,
        "25 rows in chunks of 10 is three round trips: 10, 10, 5"
    );
}

#[tokio::test]
async fn a_streamed_scan_resumes_at_the_right_place() {
    // Keyset pagination: the second page must continue after the first, not
    // repeat it or skip past it.
    use futures::StreamExt;

    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 30).await;

    let hot = PostgresHot::new(h.pool().clone()).scan_chunk_rows(7);
    let mut stream = hot
        .scan_range(
            TABLE,
            meterstore::planner::TimeRange::unbounded(),
            &ScanSpec::core(),
        )
        .await
        .unwrap();

    let mut seen: Vec<i64> = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let from = batch
            .column_by_name(col::FROM)
            .unwrap()
            .as_any()
            .downcast_ref::<meterstore::arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        for i in 0..from.len() {
            seen.push(from.value(i));
        }
    }

    assert_eq!(seen.len(), 30, "no row lost or repeated across pages");
    let mut sorted = seen.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), 30, "no duplicates across page boundaries");
    assert_eq!(seen, sorted, "pages arrive in key order");
}

#[tokio::test]
async fn invariant_violations_counts_rows_below_the_watermark() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 5).await;
    h.insert_readings(D21, 7).await;

    // Watermark at D21: the five D20 rows are stranded in the hot tier.
    let stranded = h
        .hot
        .invariant_violations(TABLE, TieringWatermark::new(D21))
        .await
        .unwrap();
    assert_eq!(stranded, 5);

    // Watermark at D20: everything is correctly hot.
    let clean = h
        .hot
        .invariant_violations(TABLE, TieringWatermark::new(D20))
        .await
        .unwrap();
    assert_eq!(clean, 0);
}

#[tokio::test]
async fn corrections_coexist_with_the_values_they_supersede() {
    // A correction is a new row at a higher version, not an overwrite. The
    // primary key must permit both.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 1).await;

    sqlx::query(
        r#"INSERT INTO readings
           (malo_id, obis_code, sparte, "from", "to", value, unit, quality,
            source_kind, version, version_scope, recorded_at, balancing_day)
           VALUES ('12345678905','1-0:1.8.0','STROM',$1,$2,9.9,'KWH','CORRECTED','MSCONS',
                   20260728000002,'9900000000001:2026-07',$3,
                   CAST(($1 AT TIME ZONE 'Europe/Berlin') AS DATE))"#,
    )
    .bind(D20)
    .bind(D20 + Duration::minutes(15))
    .bind(datetime!(2026-07-28 06:00 UTC))
    .execute(h.pool())
    .await
    .expect("correction must be insertable alongside the original");

    assert_eq!(h.row_count().await, 2);

    let rows = sqlx::query(r#"SELECT version FROM readings WHERE "from" = $1 ORDER BY version"#)
        .bind(D20)
        .fetch_all(h.pool())
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let latest: rust_decimal::Decimal = rows[1].try_get(0).unwrap();
    assert_eq!(latest.to_string(), "20260728000002");
}

#[tokio::test]
async fn a_chunk_boundary_inside_a_tie_does_not_drop_rows() {
    // A keyset cursor resumes at *strictly greater than* the last row it saw, so
    // it must be unique per row. `(malo_id, "from")` looks like a key and is
    // not: one measuring point and interval carries a row per OBIS channel and
    // per correction version. With a chunk boundary landing inside such a group,
    // a non-unique cursor discards the rest of it — silently, and only once a
    // table is big enough for the boundary to fall in the wrong place.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    // Six rows that all share `(malo_id, "from")`: two channels, three versions.
    let source = MeasurementSource::Mscons {
        pid: 13_005,
        message_ref: None,
        sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
    };
    let detail = serde_json::to_string(&source).unwrap();
    for obis in ["1-0:1.8.0", "1-0:2.8.0"] {
        for version in [
            20_260_701_000_001i64,
            20_260_702_000_002,
            20_260_703_000_003,
        ] {
            sqlx::query(
                r#"INSERT INTO readings
                   (malo_id, melo_id, obis_code, sparte, "from", "to", value, unit,
                    quality, resolution, source_kind, source_detail, provenance,
                    version, version_scope, recorded_at, balancing_day)
                   VALUES ($1,NULL,$2,'STROM',$3,$4,1,'KWH','MEASURED','PT15M','MSCONS',
                           $5,'[]',$6,'9900000000001:2026-07',$7,
                           CAST(($3 AT TIME ZONE 'Europe/Berlin') AS DATE))"#,
            )
            .bind("12345678905")
            .bind(meterstore::canonical_obis(obis).unwrap())
            .bind(D20)
            .bind(D20 + Duration::minutes(15))
            .bind(detail.as_str())
            .bind(rust_decimal::Decimal::new(version, 0))
            .bind(D20)
            .execute(h.pool())
            .await
            .expect("insert");
        }
    }

    // A chunk size of two guarantees the boundary lands inside the tie.
    let paged = PostgresHot::new(h.pool().clone()).scan_chunk_rows(2);
    let batches = collect_stream(
        paged
            .scan_range(
                TABLE,
                meterstore::planner::TimeRange::unbounded(),
                &ScanSpec::core(),
            )
            .await
            .unwrap(),
    )
    .await;

    let scanned: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(scanned, 6, "every row must survive a chunked scan");
}

#[tokio::test]
async fn a_chunked_scan_returns_rows_in_the_declared_sort_order() {
    // Archived files carry `sorting_columns = (malo_id, from)` in the Parquet
    // footer, and a reader is entitled to trust it. The cursor extends that
    // order to uniqueness rather than replacing it.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 20).await;

    let paged = PostgresHot::new(h.pool().clone()).scan_chunk_rows(3);
    let batches = collect_stream(
        paged
            .scan_range(
                TABLE,
                meterstore::planner::TimeRange::unbounded(),
                &ScanSpec::core(),
            )
            .await
            .unwrap(),
    )
    .await;

    use datafusion::arrow::array::AsArray;
    let mut seen: Vec<(String, i64)> = Vec::new();
    for batch in &batches {
        // By name, not by position: a positional read here silently pointed at
        // the wrong column the moment one was inserted ahead of `from`.
        let malo = batch
            .column_by_name("malo_id")
            .expect("malo_id")
            .as_string::<i32>();
        let from = batch
            .column_by_name("from")
            .expect("from")
            .as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>();
        for i in 0..batch.num_rows() {
            seen.push((malo.value(i).to_string(), from.value(i)));
        }
    }

    assert_eq!(seen.len(), 20);
    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(seen, sorted, "chunks must not reorder the stream");
}

#[tokio::test]
async fn overlapping_intervals_in_one_version_are_refused() {
    // The double-count no key catches. The primary key is
    // `(malo_id, obis_code, from, version)`, so two rows for one channel at one
    // version *must* differ in `from` — and two ranges that differ in `from` can
    // still overlap. An hourly delivery followed by a quarter-hourly one for the
    // same channel and version leaves both stored, and every `SUM` over them is
    // inflated with nothing anywhere to indicate it.
    //
    // A correction is a different case and must stay legal: it carries a higher
    // version, so the exclusion is scoped to one version rather than to the
    // channel.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    let insert = |from: OffsetDateTime, to: OffsetDateTime, version: i64| {
        sqlx::query(
            r#"INSERT INTO readings
               (malo_id, obis_code, sparte, "from", "to", value, unit, quality,
                source_kind, version, version_scope, recorded_at, balancing_day)
               VALUES ('12345678905','1-0:1.8.0','STROM',$1,$2,1.0,'KWH','MEASURED',
                       'MSCONS',$3,'9900000000001:2026-07',$1,
                       CAST(($1 AT TIME ZONE 'Europe/Berlin') AS DATE))"#,
        )
        .bind(from)
        .bind(to)
        .bind(rust_decimal::Decimal::new(version, 0))
        .execute(h.pool())
    };

    insert(D20, D20 + Duration::hours(1), 20_260_720_000_001)
        .await
        .expect("the first delivery is fine");

    let clash = insert(
        D20 + Duration::minutes(15),
        D20 + Duration::minutes(30),
        20_260_720_000_001,
    )
    .await;
    let err = clash.expect_err("an overlapping range at the same version must be refused");
    let constraint = err
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::constraint);
    assert!(
        constraint.is_some_and(|c| c.ends_with("_no_overlap")),
        "the exclusion constraint should be the one that refused it, got {constraint:?}"
    );

    // A correction covering the same span is a *higher* version and is exactly
    // how MSCONS corrects a value. It must still be storable.
    insert(
        D20 + Duration::minutes(15),
        D20 + Duration::minutes(30),
        20_260_728_000_002,
    )
    .await
    .expect("a correction must remain legal");
}

#[tokio::test]
async fn a_malformed_version_scope_is_refused_by_the_table() {
    // Every other coded column carries a CHECK. This one carries the
    // load-bearing one: the `_one_operator` exclusion reads the operator back
    // with `split_part(version_scope, ':', 1)`, which takes the wrong half if a
    // second separator appears — so a scope that is not canonical would quietly
    // weaken the guard rather than fail.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    let insert = |scope: &'static str| {
        sqlx::query(
            r#"INSERT INTO readings
               (malo_id, obis_code, sparte, "from", "to", value, unit, quality,
                source_kind, version, version_scope, recorded_at, balancing_day)
               VALUES ('12345678905','1-0:1.8.0','STROM',$1,$2,1.0,'KWH','MEASURED',
                       'MSCONS',1,$3,$1,CAST(($1 AT TIME ZONE 'Europe/Berlin') AS DATE))"#,
        )
        .bind(D20)
        .bind(D20 + Duration::minutes(15))
        .bind(scope)
        .execute(h.pool())
    };

    for bad in [
        "9900000000001",          // no month
        "9900000000001:2026",     // no month part
        "9900000000001:2026-13",  // month out of range
        "990000000000:2026-07",   // twelve digits — a Marktpartner-ID is thirteen
        "99000000000012:2026-07", // fourteen
        "99:2026-07",             // an operator too short to be a Marktpartner-ID
        "a:b:2026-07",            // a second separator, which split_part would mis-read
        "2026-07",
    ] {
        assert!(
            insert(bad).await.is_err(),
            "{bad:?} is not a canonical version scope and must be refused"
        );
    }

    insert("9900000000001:2026-07")
        .await
        .expect("the canonical form must be storable");
}
