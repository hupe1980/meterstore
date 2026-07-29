//! Archival against a real PostgreSQL and a real Iceberg warehouse.
//!
//! The unit tests prove the archiver's ordering against in-memory fakes. These
//! prove the same properties survive contact with the actual storage engines —
//! in particular that the watermark really does ride inside the Iceberg snapshot
//! summary, and can be read back after the process that wrote it is gone.

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
use iceberg_storage_opendal::OpenDalStorageFactory;
use metering::measurement_series::MeasurementSource;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const TABLE: &str = "readings_versions";
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);
const D22: OffsetDateTime = datetime!(2026-07-22 00:00 UTC);
const NOW: OffsetDateTime = datetime!(2026-07-30 00:00 UTC);

struct Harness {
    hot: PostgresHot,
    cold: Arc<IcebergCold>,
    _warehouse: tempfile::TempDir,
    _container: testcontainers::ContainerAsync<Postgres>,
}

impl Harness {
    async fn start() -> Self {
        let container = Postgres::default().start().await.expect("start postgres");
        let port = container.get_host_port_ipv4(5432).await.expect("map port");
        let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");

        let pool = PgPool::connect(&url).await.expect("connect");
        let hot = PostgresHot::new(pool);
        hot.create_table(TABLE).await.expect("create hot table");

        let warehouse = tempfile::tempdir().expect("temp warehouse");
        let warehouse_uri = format!("file://{}", warehouse.path().display());

        let catalog = SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::Fs))
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

        let cold = Arc::new(IcebergCold::new(
            Arc::new(catalog) as Arc<dyn Catalog>,
            NamespaceIdent::new("metering".to_string()),
            8 * 1024 * 1024,
        ));
        cold.create_table(TABLE).await.expect("create cold table");

        Self {
            hot,
            cold,
            _warehouse: warehouse,
            _container: container,
        }
    }

    async fn insert_readings(&self, start: OffsetDateTime, count: i64, version: i64) {
        let source = MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        };
        let detail = serde_json::to_string(&source).unwrap();

        for i in 0..count {
            let from = start + Duration::minutes(15 * i);
            sqlx::query(&format!(
                r#"INSERT INTO "{TABLE}"
                   (malo_id, melo_id, obis_code, sparte, "from", "to", value, unit, quality,
                    resolution, source_kind, source_detail, provenance,
                    version, version_scope, recorded_at)
                   VALUES ($1,$2,$3,'STROM',$4,$5,$6,'KWH',$7,$8,$9,$10,$11,$12,$13,$14)"#
            ))
            .bind(format!("1234567890{}", i % 3))
            .bind(None::<String>)
            .bind(meterstore::canonical_obis("1-0:1.8.0").unwrap())
            .bind(from)
            .bind(from + Duration::minutes(15))
            .bind(rust_decimal::Decimal::new(1_234_567 + i, 6))
            .bind(metering::QualityFlag::Measured.as_str())
            .bind(Some("PT15M"))
            .bind("mscons")
            .bind(Some(detail.as_str()))
            .bind(Some("[]"))
            .bind(rust_decimal::Decimal::new(version, 0))
            .bind("99:2026-07")
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
        .append_and_commit(TABLE, stream_of(Vec::new()), WriteHints::default(), window)
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
            .append_and_commit(TABLE, stream_of(Vec::new()), WriteHints::default(), w)
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
