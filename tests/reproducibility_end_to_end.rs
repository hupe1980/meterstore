//! The features a regulator asks about, against real PostgreSQL and Iceberg.
//!
//! Four claims that are only worth making if they are tested end to end:
//!
//! - **Reproducibility.** A settlement rerun against a pinned snapshot and a
//!   version ceiling returns what was known then, not what is known now (§6.4).
//! - **Completeness.** A missing interval is information, and the expectation
//!   comes from the DST-aware calendar rather than a hardcoded 96 (§9.6).
//! - **Provenance.** Every result carries the boundary it was computed against,
//!   and says which tiers produced it (P1).
//! - **Single archiver.** Two archivers cannot both own the detach window (§5.2).
//!
//! Each of these is a claim about behaviour under real storage, so a fake would
//! prove nothing about the parts that actually fail — snapshot pinning, catalog
//! CAS, advisory-lock scope.

use std::sync::Arc;

use meterstore::cold::IcebergCold;
use meterstore::config::TableConfig;
use meterstore::hot::PostgresHot;
use meterstore::planner::{ReadMode, SnapshotSelector};
use meterstore::tiering::Archiver;
use meterstore::tiering::store::{ColdStore, HotStore, WriteHints, stream_of};
use meterstore::watermark::{ArchivalWindow, Tier};

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
const D18: OffsetDateTime = datetime!(2026-07-18 00:00 UTC);
const D19: OffsetDateTime = datetime!(2026-07-19 00:00 UTC);
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);

/// The version a first delivery carries, and the one a correction carries.
const V1: i64 = 20_260_701_000_001;
const V2: i64 = 20_260_715_000_002;

struct Harness {
    hot: Arc<PostgresHot>,
    cold: Arc<IcebergCold>,
    /// Kept so a test can open a **second** session — an advisory lock is
    /// session-scoped, so genuine contention needs a separate pool.
    url: String,
    _warehouse: tempfile::TempDir,
    _container: testcontainers::ContainerAsync<Postgres>,
}

impl Harness {
    async fn start() -> Self {
        let container = Postgres::default().start().await.expect("start postgres");
        let port = container.get_host_port_ipv4(5432).await.expect("map port");
        let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");

        let pool = PgPool::connect(&url).await.expect("connect");
        let hot = Arc::new(PostgresHot::new(pool));
        hot.create_table(TABLE).await.expect("create hot table");

        let warehouse = tempfile::tempdir().expect("temp warehouse");
        let catalog = SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::Fs))
            .load(
                "meterstore",
                std::collections::HashMap::from([
                    (SQL_CATALOG_PROP_URI.to_string(), url.clone()),
                    (
                        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                        format!("file://{}", warehouse.path().display()),
                    ),
                    (
                        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                        SqlBindStyle::DollarNumeric.to_string(),
                    ),
                ]),
            )
            .await
            .expect("sql catalog");

        let cold = Arc::new(IcebergCold::new(
            Arc::new(catalog) as Arc<dyn Catalog>,
            NamespaceIdent::new("metering".to_string()),
            8 * 1024 * 1024,
        ));
        cold.create_table(TABLE).await.expect("create cold table");

        Self {
            hot,
            cold,
            url,
            _warehouse: warehouse,
            _container: container,
        }
    }

    async fn insert(&self, malo: &str, start: OffsetDateTime, count: i64, kwh: i64, version: i64) {
        self.insert_with(malo, start, count, kwh, version, "MEASURED", "PT15M")
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_with(
        &self,
        malo: &str,
        start: OffsetDateTime,
        count: i64,
        kwh: i64,
        version: i64,
        quality: &str,
        resolution: &str,
    ) {
        let detail = serde_json::to_string(&MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        })
        .unwrap();

        let step = match resolution {
            "PT15M" => Duration::minutes(15),
            "PT1H" => Duration::hours(1),
            other => panic!("unhandled resolution {other}"),
        };

        for i in 0..count {
            let from = start + step * i as i32;
            sqlx::query(&format!(
                r#"INSERT INTO "{TABLE}"
                   (malo_id, melo_id, obis_code, sparte, "from", "to", value, unit,
                    quality, resolution, source_kind, source_detail, provenance,
                    version, version_scope, recorded_at)
                   VALUES ($1,NULL,$2,'STROM',$3,$4,$5,'KWH',$9,$10,'mscons',
                           $6,'[]',$7,'99:2026-07',$8)"#
            ))
            .bind(malo)
            .bind(meterstore::canonical_obis("1-0:1.8.0").unwrap())
            .bind(from)
            .bind(from + step)
            .bind(rust_decimal::Decimal::new(kwh, 0))
            .bind(detail.as_str())
            .bind(rust_decimal::Decimal::new(version, 0))
            .bind(datetime!(2026-07-27 06:00 UTC))
            .bind(quality)
            .bind(resolution)
            .execute(self.hot.pool())
            .await
            .expect("insert");
        }
    }

    fn config(&self) -> meterstore::ValidatedTableConfig {
        TableConfig::new(TABLE)
            .settlement_lag(Duration::days(1))
            .build()
            .unwrap()
    }

    /// Archive every window closed as of `now`, leaving the rest hot.
    async fn archive_through(&self, now: OffsetDateTime) {
        self.cold
            .append_and_commit(
                TABLE,
                stream_of(Vec::new()),
                WriteHints::default(),
                ArchivalWindow::new(D18 - Duration::DAY, D18).unwrap(),
            )
            .await
            .expect("seed watermark");

        Archiver::new(Arc::clone(&self.hot), Arc::clone(&self.cold), self.config())
            .catch_up(now, 32)
            .await
            .expect("archive");
    }

    async fn store(&self, mode: ReadMode) -> meterstore::MeterStore {
        meterstore::MeterStore::builder()
            .hot(Arc::clone(&self.hot) as Arc<dyn HotStore>)
            .cold(
                Arc::clone(&self.cold) as Arc<dyn ColdStore>,
                self.cold
                    .table_provider(TABLE)
                    .await
                    .expect("cold provider"),
            )
            .table(self.config())
            .read_mode(mode)
            .build()
            .await
            .expect("build store")
    }
}

/// The single numeric cell a counting query produces.
fn scalar(result: &meterstore::QueryResult) -> i64 {
    use datafusion::arrow::array::AsArray;
    result.batches()[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0)
}

// ---------------------------------------------------------------- provenance

#[tokio::test]
async fn every_result_carries_the_boundary_it_was_computed_against() {
    // P1: correctness is observable. Two identical queries a minute apart can
    // read the same rows from different tiers, and a bare number cannot say so.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 1, V1).await;
    h.insert("11111111111", D20, 96, 1, V1).await;
    h.archive_through(D21).await;

    let store = h.store(ReadMode::Unified).await;
    let result = store
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("query");

    assert_eq!(scalar(&result), 192);
    assert_eq!(result.watermark(), store.watermark().await.unwrap());
    assert!(result.spans_tiers(), "{:?}", result.tiers_scanned());
    assert_eq!(result.read_mode(), ReadMode::Unified);
}

#[tokio::test]
async fn a_reporting_query_can_prove_it_never_touched_postgres() {
    // The check a reproducibility claim rests on. Under Historical mode the hot
    // tier must not appear in the plan at all — not merely return no rows.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 1, V1).await;
    h.insert("11111111111", D20, 96, 1, V1).await;
    h.archive_through(D21).await;

    let result = h
        .store(ReadMode::Historical)
        .await
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("query");

    assert!(!result.touched_hot_tier());
    assert_eq!(result.tiers_scanned(), [Tier::Cold]);
}

// ------------------------------------------------------------ reproducibility

#[tokio::test]
async fn a_pinned_snapshot_reproduces_what_was_known_then() {
    // The MaBiS feature. A correction lands after the settlement ran; the rerun
    // must return the settlement's inputs, not the store's current knowledge.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    // One day, one meter, 10 kWh per interval. Archived through D20 so the
    // horizon reaches D19 and the whole of D18 lands in the cold tier — the
    // correction below is only a *late* correction if its interval is archived.
    h.insert("11111111111", D18, 96, 10, V1).await;
    h.archive_through(D20).await;

    let store = h.store(ReadMode::Unified).await;
    let settlement_snapshot = store.snapshots().await.expect("snapshots")[0].snapshot_id;

    // Afterwards a correction restates the day at 40 kWh per interval. It is
    // below the watermark, so it goes straight to Iceberg.
    let corrected = corrected_batch(&h).await;
    h.cold
        .append_only(TABLE, stream_of(vec![corrected]), WriteHints::default())
        .await
        .expect("late correction");

    let now = h.store(ReadMode::Unified).await;
    assert_eq!(
        scalar(
            &now.query("SELECT SUM(value)::BIGINT FROM readings")
                .await
                .unwrap()
        ),
        96 * 40,
        "current knowledge must reflect the correction"
    );

    let then = now
        .as_of(SnapshotSelector::Id(settlement_snapshot), None)
        .await
        .expect("pin the settlement snapshot");
    assert_eq!(
        scalar(
            &then
                .query("SELECT SUM(value)::BIGINT FROM readings")
                .await
                .unwrap()
        ),
        96 * 10,
        "the rerun must reproduce the settlement's own inputs"
    );
}

#[tokio::test]
async fn a_version_ceiling_pins_the_domain_axis_independently() {
    // The snapshot alone is not enough: a snapshot taken *after* a correction
    // holds both versions, and resolution prefers the newer one. The ceiling is
    // the second, independent bound.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 10, V1).await;
    h.archive_through(D20).await;

    let store = h.store(ReadMode::Unified).await;
    h.cold
        .append_only(
            TABLE,
            stream_of(vec![corrected_batch(&h).await]),
            WriteHints::default(),
        )
        .await
        .expect("late correction");

    // The newest snapshot contains both versions.
    let latest = h.store(ReadMode::Unified).await;
    let newest = latest.snapshots().await.unwrap()[0].snapshot_id;

    let unpinned = latest
        .as_of(SnapshotSelector::Id(newest), None)
        .await
        .unwrap();
    assert_eq!(
        scalar(
            &unpinned
                .query("SELECT SUM(value)::BIGINT FROM readings")
                .await
                .unwrap()
        ),
        96 * 40,
        "without a ceiling the newer version wins, as it should"
    );

    let pinned = latest
        .as_of(
            SnapshotSelector::Id(newest),
            Some(meterstore::Version::new(V1 as u128).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(
        scalar(
            &pinned
                .query("SELECT SUM(value)::BIGINT FROM readings")
                .await
                .unwrap()
        ),
        96 * 10,
        "the ceiling must exclude the later assertion"
    );
    drop(store);
}

#[tokio::test]
async fn a_reproducible_read_never_touches_the_mutable_tier() {
    // Including PostgreSQL would make the answer depend on when the query ran,
    // which is the opposite of reproducible.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 10, V1).await;
    h.insert("11111111111", D20, 96, 10, V1).await; // stays hot
    h.archive_through(D21).await;

    let store = h.store(ReadMode::Unified).await;
    let snapshot = store.snapshots().await.unwrap()[0].snapshot_id;
    let pinned = store
        .as_of(SnapshotSelector::Id(snapshot), None)
        .await
        .unwrap();

    let result = pinned
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("query");
    assert!(!result.touched_hot_tier());
    assert_eq!(scalar(&result), 96, "only the archived day");

    // And the provenance reports the boundary the *pinned* snapshot published,
    // not today's — attaching a number from one moment to a boundary from
    // another is exactly what carrying provenance exists to prevent.
    let listed = store.snapshots().await.unwrap();
    let published = listed
        .iter()
        .find(|s| s.snapshot_id == snapshot)
        .and_then(|s| s.watermark)
        .expect("a MeterStore snapshot publishes a watermark");
    assert_eq!(result.watermark(), published);
}

#[tokio::test]
async fn an_unknown_snapshot_is_refused_rather_than_silently_current() {
    // A reproducible read that quietly returns current data is worse than one
    // that does not run: the number looks like a settlement rerun and is not.
    let h = Harness::start().await;
    let store = h.store(ReadMode::Unified).await;
    let err = store
        .as_of(SnapshotSelector::Id(424_242), None)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("424242"), "{err}");
}

#[tokio::test]
async fn an_instant_before_the_table_existed_has_nothing_to_reproduce() {
    let h = Harness::start().await;
    h.archive_through(D19).await;
    let store = h.store(ReadMode::Unified).await;

    let err = store
        .as_of(
            SnapshotSelector::Timestamp(datetime!(2000-01-01 00:00 UTC)),
            None,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("predates"), "{err}");
}

// -------------------------------------------------------------- completeness

#[tokio::test]
async fn completeness_reports_a_gap_across_both_tiers() {
    // A short day must be visible whichever tier holds it, and the count must be
    // the calendar's rather than a hardcoded 96.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert("11111111111", D18, 96, 1, V1).await; // complete, archived
    h.insert("11111111111", D19, 90, 1, V1).await; // short, archived
    h.insert("11111111111", D20, 96, 1, V1).await; // complete, hot
    h.archive_through(D21).await;

    let rows = h
        .store(ReadMode::Unified)
        .await
        .completeness(D18, D21)
        .await
        .expect("completeness");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.expected, 96 * 3);
    assert_eq!(row.actual, 96 + 90 + 96);
    assert_eq!(row.missing, 6);
    assert!(!row.is_complete());
    // The gap is reported against the **Berlin** day, and that is the whole
    // reason §9.5 exists. The six missing intervals are the last six of the UTC
    // day 2026-07-19 — 22:30Z to 24:00Z — which in Berlin is 00:30 to 02:00 on
    // the *20th*. A completeness report grouped on UTC days would name the 19th
    // and send an operator looking at the wrong day's delivery.
    assert_eq!(row.first_gap, Some(time::macros::date!(2026 - 07 - 20)));
}

#[tokio::test]
async fn completeness_uses_each_series_own_resolution() {
    // A utility carries hourly channels alongside quarter-hourly ones. Applying
    // one series' resolution to another reports a full day of phantom gaps.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert("11111111111", D18, 96, 1, V1).await;
    h.insert_with("22222222222", D18, 24, 1, V1, "MEASURED", "PT1H")
        .await;

    let mut rows = h
        .store(ReadMode::Unified)
        .await
        .completeness(D18, D19)
        .await
        .expect("completeness");
    rows.sort_by(|a, b| a.malo_id.cmp(&b.malo_id));

    assert_eq!(rows[0].expected, 96);
    assert_eq!(rows[1].expected, 24, "the hourly series expects 24, not 96");
    assert!(rows.iter().all(|r| r.is_complete()));
}

#[tokio::test]
async fn completeness_counts_a_corrected_interval_once() {
    // Reading the raw table would count both versions and report a day as
    // holding more intervals than the calendar allows.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert("11111111111", D18, 96, 10, V1).await;
    h.insert("11111111111", D18, 96, 40, V2).await; // a correction, same intervals

    let rows = h
        .store(ReadMode::Unified)
        .await
        .completeness(D18, D19)
        .await
        .expect("completeness");

    assert_eq!(rows[0].actual, 96, "one interval, whatever its version");
    assert_eq!(rows[0].surplus, 0);
    assert!(rows[0].is_complete());
}

#[tokio::test]
async fn completeness_is_reachable_from_sql() {
    // The operator asking this question has a SQL client, not a compiler.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 90, 1, V1).await;

    let result = h
        .store(ReadMode::Unified)
        .await
        .query(
            "SELECT missing FROM meter_completeness('2026-07-18', '2026-07-19') \
             WHERE malo_id = '11111111111'",
        )
        .await
        .expect("completeness through SQL");

    assert_eq!(scalar(&result), 6);
}

// ------------------------------------------------------------ the typed path

#[tokio::test]
async fn the_typed_read_returns_the_domain_type_across_both_tiers() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 3, V1).await;
    h.insert("11111111111", D20, 96, 3, V1).await;
    h.archive_through(D21).await;

    let series = h
        .store(ReadMode::Unified)
        .await
        .series("11111111111")
        .obis("1-0:1.8.0")
        .unwrap()
        .range(D18, D21)
        .collect()
        .await
        .expect("typed read")
        .expect("rows exist");

    assert_eq!(series.malo_id, "11111111111");
    assert_eq!(series.intervals.len(), 192);
    // Ordered, so the domain layer's arithmetic sees a series that runs forwards.
    assert!(series.intervals.windows(2).all(|w| w[0].from < w[1].from));
    assert_eq!(series.intervals[0].from, D18);
    // The decimal survives the whole round trip.
    assert_eq!(
        series.intervals[0].value_kwh,
        rust_decimal::Decimal::new(3, 0)
    );
}

#[tokio::test]
async fn the_typed_read_returns_the_corrected_value() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 10, V1).await;
    h.insert("11111111111", D18, 96, 40, V2).await;

    let series = h
        .store(ReadMode::Unified)
        .await
        .series("11111111111")
        .range(D18, D19)
        .collect()
        .await
        .expect("typed read")
        .expect("rows exist");

    assert_eq!(series.intervals.len(), 96, "one row per interval");
    assert!(
        series
            .intervals
            .iter()
            .all(|i| i.value_kwh == rust_decimal::Decimal::new(40, 0)),
        "the correction must win"
    );
}

#[tokio::test]
async fn a_typed_read_of_a_meter_with_no_data_is_absence_not_zero() {
    let h = Harness::start().await;
    let series = h
        .store(ReadMode::Unified)
        .await
        .series("99999999999")
        .range(D18, D21)
        .collect()
        .await
        .expect("typed read");
    assert!(series.is_none());
}

#[tokio::test]
async fn a_malo_id_from_a_message_never_reaches_the_sql_text() {
    // §19.7: user values are bound, never concatenated. A quote in the input
    // must produce no rows rather than a syntax error or a wider scan.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 1, V1).await;

    let series = h
        .store(ReadMode::Unified)
        .await
        .series("' OR '1'='1")
        .range(D18, D21)
        .collect()
        .await
        .expect("the query must run, not fail to parse");
    assert!(series.is_none(), "and must match nothing");
}

// ---------------------------------------------------------- single archiver

#[tokio::test]
async fn only_one_archiver_may_hold_a_table_at_a_time() {
    // §5.2. The detach window is the one state where the tiering invariant is
    // relaxed, and it is only safe because exactly one process owns it.
    let h = Harness::start().await;

    let first = h
        .hot
        .try_archive_lease(TABLE)
        .await
        .expect("lease call")
        .expect("first caller wins");

    assert!(
        h.hot.try_archive_lease(TABLE).await.unwrap().is_none(),
        "a second caller must be refused while the first holds it"
    );

    // A different table is a different lease.
    assert!(
        h.hot
            .try_archive_lease("other_versions")
            .await
            .unwrap()
            .is_some()
    );

    first.release().await.expect("release");
    assert!(
        h.hot.try_archive_lease(TABLE).await.unwrap().is_some(),
        "releasing must hand the claim on"
    );
}

#[tokio::test]
async fn a_contended_archiver_does_nothing_rather_than_racing() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 1, V1).await;
    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D18 - Duration::DAY, D18).unwrap(),
        )
        .await
        .unwrap();

    let held = h
        .hot
        .try_archive_lease(TABLE)
        .await
        .unwrap()
        .expect("hold the lease");

    let outcome = Archiver::new(Arc::clone(&h.hot), Arc::clone(&h.cold), h.config())
        .run_once(D21)
        .await
        .expect("a contended run is not a failure");

    assert!(outcome.lease_contended);
    assert!(!outcome.archived_anything());
    assert_eq!(
        outcome.watermark.get(),
        D18,
        "the boundary must not have moved"
    );

    let still_hot: i64 = sqlx::query_scalar(&format!(r#"SELECT count(*) FROM "{TABLE}""#))
        .fetch_one(h.hot.pool())
        .await
        .unwrap();
    assert_eq!(still_hot, 96, "no partition may have been detached");

    held.release().await.unwrap();
}

// ------------------------------------------------------------ system tables

#[tokio::test]
async fn the_resolution_sql_is_reachable_from_a_sql_client() {
    // The primary mitigation for the version-resolution trap, available to the
    // person holding a Trino session (§13.7.2).
    let h = Harness::start().await;
    h.archive_through(D19).await;

    let store = h.store(ReadMode::Unified).await;
    store
        .refresh_system_tables(D21)
        .await
        .expect("refresh system tables");

    let result = store
        .query("SELECT value FROM system.resolution WHERE setting = 'resolution_sql'")
        .await
        .expect("query");

    use datafusion::arrow::array::AsArray;
    let sql = result.batches()[0]
        .column(0)
        .as_string::<i32>()
        .value(0)
        .to_string();
    assert!(sql.contains("ROW_NUMBER()"), "{sql}");
    assert!(sql.contains("readings_versions"), "{sql}");
    assert_eq!(sql, store.resolution_sql(), "one definition, not two");
}

#[tokio::test]
async fn system_snapshots_lists_what_a_reproducible_read_can_pin() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 1, V1).await;
    h.archive_through(D19).await;

    let store = h.store(ReadMode::Unified).await;
    store.refresh_system_tables(D21).await.expect("refresh");

    let result = store
        .query("SELECT COUNT(*) FROM system.snapshots WHERE written_by_meterstore")
        .await
        .expect("query");
    assert!(scalar(&result) >= 1);

    let listed = store.snapshots().await.unwrap();
    assert!(listed.iter().all(|s| s.watermark.is_some()));
    assert!(
        listed
            .windows(2)
            .all(|w| w[0].committed_at >= w[1].committed_at),
        "newest first"
    );
}

// ------------------------------------------------------------ schema safety

#[tokio::test]
async fn a_matching_schema_reports_no_changes() {
    let h = Harness::start().await;
    let compatibility = h
        .store(ReadMode::Unified)
        .await
        .check_schema()
        .await
        .expect("check")
        .expect("the Iceberg store can report a schema");
    assert!(
        compatibility.is_safe(),
        "unexpected: {:?}",
        compatibility.changes
    );
}

#[tokio::test]
async fn declaring_a_new_identity_column_quarantines_rather_than_corrupting() {
    // An identity column joins the merge key, so adding one to a table that
    // already holds rows changes what "the same reading" means. §11 halts.
    let h = Harness::start().await;
    let store = meterstore::MeterStore::builder()
        .hot(Arc::clone(&h.hot) as Arc<dyn HotStore>)
        .cold(
            Arc::clone(&h.cold) as Arc<dyn ColdStore>,
            h.cold.table_provider(TABLE).await.unwrap(),
        )
        .table(
            TableConfig::new(TABLE)
                .settlement_lag(Duration::days(1))
                .identity_column(datafusion::arrow::datatypes::Field::new(
                    "tenant",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    false,
                ))
                .build()
                .unwrap(),
        )
        .build()
        .await
        .expect_err("the store must refuse to open against a schema it cannot hold")
        .to_string();

    assert!(store.contains("quarantined"), "{store}");
    assert!(store.contains("tenant"), "{store}");
    assert!(
        store.contains("NOT NULL"),
        "the message must name the reason, not just the column: {store}"
    );
}

#[tokio::test]
async fn a_store_can_be_built_before_its_tables_exist() {
    // The documented order: build, then `create_tables`. The compatibility check
    // therefore runs before there is anything to check against, and must report
    // "unknown" rather than turning first-run into a failure.
    let container = Postgres::default().start().await.expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("map port");
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPool::connect(&url).await.expect("connect");
    let hot = Arc::new(PostgresHot::new(pool));

    let warehouse = tempfile::tempdir().expect("temp warehouse");
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::Fs))
        .load(
            "meterstore",
            std::collections::HashMap::from([
                (SQL_CATALOG_PROP_URI.to_string(), url.clone()),
                (
                    SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                    format!("file://{}", warehouse.path().display()),
                ),
                (
                    SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                    SqlBindStyle::DollarNumeric.to_string(),
                ),
            ]),
        )
        .await
        .expect("sql catalog");
    let cold = Arc::new(IcebergCold::new(
        Arc::new(catalog) as Arc<dyn Catalog>,
        NamespaceIdent::new("metering".to_string()),
        8 * 1024 * 1024,
    ));

    // The cold table has to exist for the provider, which is the one thing the
    // builder genuinely cannot do without — everything else is created after.
    cold.create_table(TABLE).await.expect("cold table");

    let store = meterstore::MeterStore::builder()
        .hot(Arc::clone(&hot) as Arc<dyn HotStore>)
        .cold(
            Arc::clone(&cold) as Arc<dyn ColdStore>,
            cold.table_provider(TABLE).await.unwrap(),
        )
        .table(
            TableConfig::new(TABLE)
                .settlement_lag(Duration::days(1))
                .build()
                .unwrap(),
        )
        .build()
        .await
        .expect("a store must build before create_tables runs");

    store.create_tables().await.expect("create tables");
    assert!(store.check_schema().await.unwrap().unwrap().is_safe());
}

#[tokio::test]
async fn a_safe_schema_difference_does_not_halt_the_table() {
    // Additive drift is ordinary. A nullable column the cold table has not seen
    // yet is exactly what Iceberg's field-id resolution makes free, so it must
    // not be treated like a narrowing change.
    let h = Harness::start().await;
    h.cold
        .create_table_with(
            TABLE,
            &[datafusion::arrow::datatypes::Field::new(
                "bilanzkreis",
                datafusion::arrow::datatypes::DataType::Utf8,
                true,
            )],
            &[],
        )
        .await
        .expect("cold table already exists; this is a no-op load");

    let compatibility = h
        .store(ReadMode::Unified)
        .await
        .check_schema()
        .await
        .unwrap()
        .unwrap();
    assert!(compatibility.is_safe(), "{:?}", compatibility.changes);
}

/// A restated day at 40 kWh per interval, encoded for a direct cold append.
async fn corrected_batch(h: &Harness) -> datafusion::arrow::array::RecordBatch {
    use metering::interval::{MeterInterval, QualityFlag};
    use metering::measurement_series::MeasurementSeries;
    use meterstore::encode::StoredSeries;
    use meterstore::{ScopedVersion, Version, VersionScope};

    let intervals: Vec<MeterInterval> = (0..96)
        .map(|i| {
            let from = D18 + Duration::minutes(15 * i);
            MeterInterval {
                from,
                to: from + Duration::minutes(15),
                value_kwh: rust_decimal::Decimal::new(40, 0),
                quality: QualityFlag::Corrected,
                obis_code: Some("1-0:1.8.0".parse().unwrap()),
            }
        })
        .collect();

    let mut series = MeasurementSeries::new(
        "11111111111".to_string(),
        Some("1-0:1.8.0".parse().unwrap()),
        intervals,
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        },
        datetime!(2026-08-01 06:00 UTC),
    );
    series.resolution = Some(metering::IntervalResolution::QuarterHour);

    let stored = StoredSeries::new(
        series,
        ScopedVersion::new(
            VersionScope::for_interval("99", D18).unwrap(),
            Version::new(V2 as u128).unwrap(),
        ),
        datetime!(2026-08-01 06:00 UTC),
    );

    let _ = h;
    meterstore::encode::to_record_batch(&[stored]).expect("encode")
}

#[tokio::test]
async fn two_archivers_racing_produce_one_archival_and_no_lost_rows() {
    // §5.2 asserted against genuine concurrency rather than a sequential
    // stand-in. The earlier lease tests take the lock and then check that a
    // second caller is refused, which proves the lock works but not that the
    // *archiver* is safe when two of them start at the same instant.
    //
    // Two pools mean two PostgreSQL sessions, and an advisory lock is
    // session-scoped — so this is the real contention, not a simulation. The
    // window at risk is between detaching a partition and dropping it: two
    // archivers would both target the window above the same watermark, both
    // detach it, and one would commit rows the other had already taken.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 1, V1).await;
    h.insert("11111111111", D19, 96, 1, V1).await;

    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D18 - Duration::DAY, D18).unwrap(),
        )
        .await
        .unwrap();

    // A second, independent pool: a different session, and therefore a real
    // competitor for the lock.
    let second_pool = PgPool::connect(&h.url).await.expect("second pool");
    let second_hot = Arc::new(PostgresHot::new(second_pool));

    let first = Archiver::new(Arc::clone(&h.hot), Arc::clone(&h.cold), h.config());
    let second = Archiver::new(second_hot, Arc::clone(&h.cold), h.config());

    let (a, b) = tokio::join!(first.run_once(D21), second.run_once(D21));
    let a = a.expect("first archiver");
    let b = b.expect("second archiver");

    // Exactly one did the work. Which one is a race, so the assertion is on the
    // count rather than on the identity.
    let archived = [&a, &b].iter().filter(|o| o.archived_anything()).count();
    let contended = [&a, &b].iter().filter(|o| o.lease_contended).count();
    assert_eq!(
        archived, 1,
        "exactly one archiver may own the detach window"
    );
    assert_eq!(
        contended, 1,
        "the loser must report contention, not failure"
    );

    // And nothing was lost or duplicated in the process.
    let store = h.store(ReadMode::Unified).await;
    store.verify_invariant().await.expect("invariant");
    assert_eq!(
        scalar(&store.query("SELECT COUNT(*) FROM readings").await.unwrap()),
        192,
        "both days must still be readable exactly once"
    );
}

#[tokio::test]
async fn writes_during_archival_are_never_stranded_below_the_watermark() {
    // The other half of §6.3's hard case. While a window is being archived, a
    // correction can arrive for an interval inside it — and if it were written
    // to PostgreSQL it would land below the advancing watermark, where no query
    // looks: accepted, and then silently invisible.
    //
    // `MeterStore::append` routes on the watermark it reads, so a correction
    // arriving *after* the boundary moves goes to Iceberg. This checks the row
    // is readable either way, which is the property that actually matters.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 10, V1).await;
    h.cold
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D18 - Duration::DAY, D18).unwrap(),
        )
        .await
        .unwrap();

    let store = h.store(ReadMode::Unified).await;
    store.archive(D20, 4).await.expect("archive D18");
    assert_eq!(store.watermark().await.unwrap().get(), D19);

    // A correction for the now-archived day. It must not reach PostgreSQL.
    let outcome = store
        .append(&[corrected_series()])
        .await
        .expect("late correction");
    assert!(
        outcome.had_late_corrections(),
        "an interval below the watermark must be routed to Iceberg"
    );
    assert_eq!(
        outcome.hot_rows, 0,
        "nothing may be written below the boundary"
    );

    store.verify_invariant().await.expect("invariant");

    let total = h
        .store(ReadMode::Unified)
        .await
        .query("SELECT SUM(value)::BIGINT FROM readings")
        .await
        .expect("query");
    assert_eq!(
        scalar(&total),
        96 * 40,
        "the correction must win, and be readable from the tier that owns it"
    );
}

/// The restated day the late-correction test writes.
fn corrected_series() -> meterstore::encode::StoredSeries {
    use metering::interval::{MeterInterval, QualityFlag};
    use metering::measurement_series::MeasurementSeries;
    use meterstore::encode::StoredSeries;
    use meterstore::{ScopedVersion, Version, VersionScope};

    let intervals: Vec<MeterInterval> = (0..96)
        .map(|i| {
            let from = D18 + Duration::minutes(15 * i);
            MeterInterval {
                from,
                to: from + Duration::minutes(15),
                value_kwh: rust_decimal::Decimal::new(40, 0),
                quality: QualityFlag::Corrected,
                obis_code: Some("1-0:1.8.0".parse().unwrap()),
            }
        })
        .collect();

    let mut series = MeasurementSeries::new(
        "11111111111".to_string(),
        Some("1-0:1.8.0".parse().unwrap()),
        intervals,
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        },
        datetime!(2026-08-01 06:00 UTC),
    );
    series.resolution = Some(metering::IntervalResolution::QuarterHour);

    StoredSeries::new(
        series,
        ScopedVersion::new(
            VersionScope::for_interval("99", D18).unwrap(),
            Version::new(V2 as u128).unwrap(),
        ),
        datetime!(2026-08-01 06:00 UTC),
    )
}
