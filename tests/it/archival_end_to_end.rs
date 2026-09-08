//! Archival against a real PostgreSQL and a real Iceberg warehouse.
//!
//! The unit tests prove the archiver's ordering against in-memory fakes. These
//! prove the same properties survive contact with the actual storage engines —
//! in particular that the watermark really does ride inside the Iceberg snapshot
//! summary, and can be read back after the process that wrote it is gone.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them: `testkit::postgres` is what shares one container
// across the binary instead of starting one per test.
#![cfg(feature = "testkit")]

use std::sync::Arc;

use meterstore::cold::IcebergCold;
use meterstore::config::TableConfig;
use meterstore::hot::PostgresHot;
use meterstore::tiering::Archiver;
use meterstore::tiering::store::{ColdStore, HotStore, PartitionId, WriteHints, stream_of};
use meterstore::watermark::{ArchivalWindow, TieringWatermark};

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use metering::measurement_series::MeasurementSource;
use sqlx::PgPool;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const TABLE: &str = "readings_versions";
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);
const D22: OffsetDateTime = datetime!(2026-07-22 00:00 UTC);
const D23: OffsetDateTime = datetime!(2026-07-23 00:00 UTC);
const D24: OffsetDateTime = datetime!(2026-07-24 00:00 UTC);
const NOW: OffsetDateTime = datetime!(2026-07-30 00:00 UTC);

struct Harness {
    hot: PostgresHot,
    cold: Arc<IcebergCold>,
    /// The raw catalogue, so a test can commit the way a *foreign* tool would.
    catalog: Arc<dyn Catalog>,
    _warehouse: tempfile::TempDir,
}

impl Harness {
    async fn start() -> Self {
        let url = meterstore::testkit::postgres::fresh_database()
            .await
            .expect("postgres");

        let pool = PgPool::connect(&url).await.expect("connect");
        let hot = PostgresHot::new(pool);
        hot.create_table(TABLE).await.expect("create hot table");

        let warehouse = tempfile::tempdir().expect("temp warehouse");
        let warehouse_uri = format!("file://{}", warehouse.path().display());

        let catalog = SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory))
            .load(
                "meterstore",
                std::collections::HashMap::from([
                    (SQL_CATALOG_PROP_URI.to_string(), url.clone()),
                    (SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri),
                    (
                        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                        SqlBindStyle::DollarNumeric.to_string(),
                    ),
                ]),
            )
            .await
            .expect("build sql catalog");

        let catalog = Arc::new(catalog) as Arc<dyn Catalog>;
        let cold = Arc::new(IcebergCold::new(
            Arc::clone(&catalog),
            NamespaceIdent::new("metering".to_string()),
            8 * 1024 * 1024,
        ));
        cold.create_table(TABLE).await.expect("create cold table");

        Self {
            hot,
            cold,
            catalog,
            _warehouse: warehouse,
        }
    }

    async fn insert_readings(&self, start: OffsetDateTime, count: i64, version: i64) {
        let source = MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
        };
        let detail = serde_json::to_string(&source).unwrap();

        for i in 0..count {
            let from = start + Duration::minutes(15 * i);
            sqlx::query(&format!(
                r#"INSERT INTO "{TABLE}"
                   (malo_id, melo_id, obis_code, sparte, "from", "to", value, unit, quality,
                    resolution, source_kind, source_detail, provenance,
                    version, version_scope, recorded_at, balancing_day)
                   VALUES ($1,$2,$3,'STROM',$4,$5,$6,'KWH',$7,$8,$9,$10,$11,$12,$13,$14,
                           CAST(($4 AT TIME ZONE 'Europe/Berlin') AS DATE))"#
            ))
            .bind(format!("1234567890{}", i % 3))
            .bind(None::<String>)
            .bind(meterstore::canonical_obis("1-0:1.8.0").unwrap())
            .bind(from)
            .bind(from + Duration::minutes(15))
            .bind(rust_decimal::Decimal::new(1_234_567 + i, 6))
            .bind(metering::QualityFlag::Measured.as_str())
            .bind(Some("PT15M"))
            .bind("MSCONS")
            .bind(Some(detail.as_str()))
            .bind(Some("[]"))
            .bind(rust_decimal::Decimal::new(version, 0))
            .bind("9900000000001:2026-07")
            .bind(datetime!(2026-07-27 06:00 UTC))
            .execute(self.hot.pool())
            .await
            .expect("insert");
        }
    }

    async fn hot_rows(&self) -> i64 {
        sqlx::query_scalar::<_, i64>(&format!(r#"SELECT count(*) FROM "{TABLE}""#))
            .fetch_one(self.hot.pool())
            .await
            .unwrap()
    }

    /// Commit a snapshot the way an out-of-band maintenance tool does: a valid
    /// Iceberg commit that knows nothing about tiering, and therefore carries no
    /// watermark. This is what compaction with Spark or PyIceberg produces, and
    /// this design explicitly recommends running it.
    async fn foreign_commit(&self) {
        use iceberg::transaction::{ApplyTransactionAction, Transaction};

        let table = self.cold.load(TABLE).await.expect("load");
        let txn = Transaction::new(&table);
        // Some property, deliberately not the watermark: a snapshot with neither
        // files nor properties is refused, and what makes this commit *foreign*
        // is that it says nothing about tiering.
        let action = txn
            .fast_append()
            .add_data_files(Vec::new())
            .set_snapshot_properties(std::collections::HashMap::from([(
                "engine".to_string(),
                "out-of-band-compaction".to_string(),
            )]));
        action
            .apply(txn)
            .expect("apply")
            .commit(self.catalog.as_ref())
            .await
            .expect("a foreign commit is a perfectly valid Iceberg commit");
    }

    fn archiver(&self) -> Archiver<PostgresHot, Arc<IcebergCold>> {
        Archiver::new(
            self.hot.clone(),
            Arc::clone(&self.cold),
            TableConfig::new(TABLE)
                .settlement_lag(Duration::days(7))
                .build()
                .unwrap(),
        )
    }
}

#[tokio::test]
async fn a_new_table_reports_an_empty_watermark() {
    // No snapshot means nothing archived, so every interval is still hot.
    let h = Harness::start().await;
    let wm = h.cold.watermark(TABLE).await.unwrap();
    assert_eq!(wm, TieringWatermark::empty());
}

#[tokio::test]
async fn a_fresh_table_reaches_its_first_real_window_in_one_commit() {
    // The first run of a real deployment. With no snapshot the watermark is the
    // Unix epoch, so stepping one day at a time would mean ~20 600 empty Iceberg
    // commits — days of catch-up and a snapshot list that never recovers —
    // before a single row is archived. The archiver crosses the empty stretch in
    // one commit instead, stopping exactly at the first partition that exists.
    //
    // Driven from an unseeded boundary, which is the only way this shows up.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 96, 20_260_727_000_001).await;

    assert_eq!(
        h.cold.watermark(TABLE).await.unwrap(),
        TieringWatermark::empty()
    );

    let bootstrap = h.archiver().run_once(NOW).await.unwrap();
    assert_eq!(bootstrap.rows, 0, "the skipped stretch holds nothing");
    assert_eq!(
        bootstrap.watermark.get(),
        D20,
        "the boundary stops at the first partition that holds rows"
    );
    assert_eq!(
        h.cold.snapshots(TABLE).await.unwrap().len(),
        1,
        "one snapshot, not one per day since 1970"
    );

    // And the very next run archives that partition normally.
    let archived = h.archiver().run_once(NOW).await.unwrap();
    assert_eq!(archived.rows, 96);
    assert_eq!(archived.watermark.get(), D21);
    assert_eq!(h.hot_rows().await, 0);
}

#[tokio::test]
async fn archives_a_window_end_to_end() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 96, 20_260_727_000_001).await;
    assert_eq!(h.hot_rows().await, 96);

    // Start from D20 so the first window is exactly that partition.
    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D20 - Duration::DAY, D20).unwrap(),
            D20,
        )
        .await
        .unwrap();

    let outcome = h.archiver().run_once(NOW).await.unwrap();

    assert!(outcome.archived_anything());
    assert_eq!(outcome.rows, 96);
    assert_eq!(outcome.watermark.get(), D21);
    assert_eq!(h.hot_rows().await, 0, "hot tier must be purged");
}

#[tokio::test]
async fn the_watermark_survives_being_read_back_from_the_snapshot() {
    // The property that makes recovery trivial: the watermark is durable in the
    // same commit as the data, so a fresh reader finds it without any external
    // checkpoint store.
    let h = Harness::start().await;

    let window = ArchivalWindow::new(D20, D21).unwrap();
    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            window,
            NOW,
        )
        .await
        .unwrap();

    let read_back = h.cold.watermark(TABLE).await.unwrap();
    assert_eq!(read_back.get(), D21);
    assert_eq!(read_back, window.resulting_watermark());
}

#[tokio::test]
async fn the_watermark_advances_monotonically_across_commits() {
    let h = Harness::start().await;

    for (from, to) in [(D20, D21), (D21, D22)] {
        let w = ArchivalWindow::new(from, to).unwrap();
        h.cold
            .append_and_commit(TABLE, stream_of(Vec::new()), WriteHints::default(), w, NOW)
            .await
            .unwrap();
        assert_eq!(h.cold.watermark(TABLE).await.unwrap().get(), to);
    }
}

#[tokio::test]
async fn a_correction_append_does_not_move_the_watermark() {
    // Corrections for already-archived intervals go straight to the cold tier,
    // but the tier boundary itself is unchanged.
    let h = Harness::start().await;
    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D20, D21).unwrap(),
            D21,
        )
        .await
        .unwrap();
    let before = h.cold.watermark(TABLE).await.unwrap();

    h.cold
        .append_only(TABLE, stream_of(Vec::new()), WriteHints::default())
        .await
        .unwrap();

    assert_eq!(h.cold.watermark(TABLE).await.unwrap(), before);
}

#[tokio::test]
async fn catch_up_drains_a_backlog_and_then_stops() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 10, 20_260_727_000_001).await;
    h.insert_readings(D21, 20, 20_260_727_000_001).await;

    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D20 - Duration::DAY, D20).unwrap(),
            D20,
        )
        .await
        .unwrap();

    let archiver = h.archiver();
    let outcomes = archiver.catch_up(NOW, 20).await.unwrap();
    let moved: u64 = outcomes.iter().map(|o| o.rows).sum();

    assert_eq!(moved, 30);
    assert_eq!(h.hot_rows().await, 0);

    // A second pass must find nothing left to do.
    let again = archiver.catch_up(NOW, 20).await.unwrap();
    assert!(again.iter().all(|o| !o.archived_anything()));
}

#[tokio::test]
async fn the_invariant_holds_after_archival() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D22, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 8, 20_260_727_000_001).await;
    h.insert_readings(D21, 8, 20_260_727_000_001).await;

    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D20 - Duration::DAY, D20).unwrap(),
            D20,
        )
        .await
        .unwrap();

    let archiver = h.archiver();
    archiver.catch_up(NOW, 20).await.unwrap();
    archiver
        .verify_invariant()
        .await
        .expect("no row may sit in the wrong tier");
}

#[tokio::test]
async fn an_orphan_from_an_interrupted_run_is_reclaimed() {
    // Simulate a crash between the cold commit and the partition drop: detach
    // the partition and commit the window, then leave it.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 4, 20_260_727_000_001).await;

    let partition = PartitionId::new(TABLE, D20);
    h.hot.detach_partition(&partition).await.unwrap();
    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D20, D21).unwrap(),
            D21,
        )
        .await
        .unwrap();

    assert_eq!(h.hot.orphaned_partitions(TABLE).await.unwrap().len(), 1);

    // The data is durable in the cold tier, so the next run may reclaim it.
    let outcome = h.archiver().run_once(NOW).await.unwrap();

    assert_eq!(outcome.orphans_reclaimed, 1);
    assert!(h.hot.orphaned_partitions(TABLE).await.unwrap().is_empty());
}

#[tokio::test]
async fn parquet_files_are_actually_written_to_the_warehouse() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_readings(D20, 96, 20_260_727_000_001).await;

    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D20 - Duration::DAY, D20).unwrap(),
            D20,
        )
        .await
        .unwrap();
    h.archiver().run_once(NOW).await.unwrap();

    let mut parquet_files = 0;
    for entry in walkdir(h._warehouse.path()) {
        if entry.extension().is_some_and(|e| e == "parquet") {
            parquet_files += 1;
        }
    }
    assert!(
        parquet_files > 0,
        "archival must produce Parquet data files"
    );
}

/// Minimal recursive directory walk, to avoid another dependency.
fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

#[tokio::test]
async fn snapshot_expiry_bounds_metadata_growth() {
    // Every commit adds a snapshot and archival commits once per window, so a
    // table left alone accumulates them until planning time grows with the list.
    let h = Harness::start().await;

    for (from, to) in [(D20, D21), (D21, D22)] {
        h.cold
            .append_and_commit(
                TABLE,
                stream_of(Vec::new()),
                WriteHints::default(),
                ArchivalWindow::new(from, to).unwrap(),
                to,
            )
            .await
            .unwrap();
    }

    // Far in the future, so every snapshot is past the retention window — but
    // the floor still protects the most recent ones.
    let expired = h
        .cold
        .expire_snapshots(TABLE, Duration::days(1), 1, datetime!(2030-01-01 00:00 UTC))
        .await
        .unwrap();

    assert!(expired > 0, "old snapshots must be removable");
    // Whatever was expired, the table is still readable at its current state.
    assert_eq!(h.cold.watermark(TABLE).await.unwrap().get(), D22);
}

#[tokio::test]
async fn expiry_removes_exactly_what_meterstore_selected() {
    // `iceberg`'s expire action runs its **age** path whether or not ids are
    // named: with no cutoff set it falls back to the table's
    // `history.expire.max-snapshot-age-ms`, default five days. So a call that
    // names ids also expires everything older than that — straight through
    // `min_snapshots_to_keep`, through the ten-year `snapshot_retention` this
    // crate is configured with, and through the watermark-chain protection
    // computed here, none of which the library knows about.
    //
    // The property is set explicitly to make that deterministic without waiting
    // five days, and it is a second hazard in its own right: `history.expire.*`
    // is a *table* property, so the out-of-band compaction this design
    // recommends could set one and silently decide the deployment's retention.
    let h = Harness::start().await;

    for (from, to) in [(D20, D21), (D21, D22), (D22, D23), (D23, D24)] {
        h.cold
            .append_and_commit(
                TABLE,
                stream_of(Vec::new()),
                WriteHints::default(),
                ArchivalWindow::new(from, to).unwrap(),
                to,
            )
            .await
            .unwrap();
    }
    // Leaves the current snapshot carrying no watermark, so expiry re-stamps
    // first — the ordinary state after out-of-band maintenance.
    h.foreign_commit().await;

    {
        use iceberg::transaction::{ApplyTransactionAction, Transaction};
        let table = h.cold.load(TABLE).await.expect("load");
        let txn = Transaction::new(&table);
        let action = txn
            .update_table_properties()
            .set(
                "history.expire.max-snapshot-age-ms".to_string(),
                "1".to_string(),
            )
            .set(
                "history.expire.min-snapshots-to-keep".to_string(),
                "1".to_string(),
            );
        action
            .apply(txn)
            .expect("apply")
            .commit(h.catalog.as_ref())
            .await
            .expect("the property an out-of-band tool would set");
    }

    // Everything is past the retention window, so the floor is the only thing
    // deciding what survives — and it is meterstore's floor, not the library's.
    const KEEP: usize = 3;
    h.cold
        .expire_snapshots(TABLE, Duration::ZERO, KEEP, datetime!(2030-01-01 00:00 UTC))
        .await
        .unwrap();

    let left = h.cold.snapshots(TABLE).await.unwrap().len();
    assert_eq!(
        left, KEEP,
        "min_snapshots_to_keep is a compliance floor: the library's own age path \
         must not expire past it"
    );
    assert_eq!(h.cold.watermark(TABLE).await.unwrap().get(), D24);
}

#[tokio::test]
async fn expiry_never_removes_the_current_snapshot() {
    // Reproducibility is the cold tier's job; expiring everything would leave a
    // table that cannot answer a query at all.
    let h = Harness::start().await;
    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D20, D21).unwrap(),
            D21,
        )
        .await
        .unwrap();

    h.cold
        .expire_snapshots(TABLE, Duration::ZERO, 1, datetime!(2030-01-01 00:00 UTC))
        .await
        .unwrap();

    assert_eq!(
        h.cold.watermark(TABLE).await.unwrap().get(),
        D21,
        "the watermark lives in the current snapshot and must survive"
    );
}

#[tokio::test]
async fn expiry_cannot_strand_the_boundary_behind_a_foreign_commit() {
    // The hazard this design creates for itself. Compaction and orphan cleanup
    // are blocked upstream, and the documented workaround is to run them out of
    // band with Spark or PyIceberg. Such a commit is a perfectly valid Iceberg
    // snapshot that carries no watermark, so the boundary lookup walks back the
    // parent chain to find one.
    //
    // Which works — until expiry removes the ancestor it was walking back to.
    // Then no snapshot in the history carries a watermark, the lookup fails, and
    // the table cannot answer *any* query. A maintenance job following the
    // documentation would have bricked the table.
    let h = Harness::start().await;
    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D20, D21).unwrap(),
            D21,
        )
        .await
        .unwrap();

    // Two foreign commits, so the watermark-carrying snapshot is neither current
    // nor within a `retain_last` of 1.
    h.foreign_commit().await;
    h.foreign_commit().await;

    h.cold
        .expire_snapshots(TABLE, Duration::ZERO, 1, datetime!(2030-01-01 00:00 UTC))
        .await
        .unwrap();

    assert_eq!(
        h.cold.watermark(TABLE).await.unwrap().get(),
        D21,
        "expiry must not remove the last snapshot that carries the boundary"
    );
}

#[tokio::test]
async fn the_boundary_can_be_restamped_onto_a_foreign_snapshot() {
    // The durable fix rather than the guard. After out-of-band maintenance the
    // current snapshot carries no watermark, so every reader pays a walk back
    // through the parent chain and expiry has to keep an ancestor alive
    // indefinitely. Re-stamping moves the boundary onto the current snapshot,
    // which is where every other MeterStore commit puts it.
    let h = Harness::start().await;
    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D20, D21).unwrap(),
            D21,
        )
        .await
        .unwrap();
    h.foreign_commit().await;

    let snapshots = h.cold.snapshots(TABLE).await.unwrap();
    assert!(
        snapshots[0].watermark.is_none(),
        "the fixture must actually leave a foreign snapshot current"
    );

    let restamped = h.cold.reassert_watermark(TABLE).await.unwrap();
    assert!(restamped.is_some(), "a foreign current snapshot needs one");

    let snapshots = h.cold.snapshots(TABLE).await.unwrap();
    assert_eq!(
        snapshots[0].watermark.map(|w| w.get()),
        Some(D21),
        "the boundary is now on the current snapshot, unchanged in value"
    );

    // Idempotent: nothing to do when the current snapshot already carries it.
    assert!(h.cold.reassert_watermark(TABLE).await.unwrap().is_none());
}
