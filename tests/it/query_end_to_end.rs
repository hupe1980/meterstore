//! SQL across both tiers, against real PostgreSQL and a real Iceberg warehouse.
//!
//! The unit tests prove the provider's routing against fakes. These prove the
//! whole path: rows written to PostgreSQL, archived to Iceberg, then read back
//! through one SQL statement that spans the watermark — and that the totals come
//! out right rather than doubled or short.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them: `testkit::postgres` is what shares one container
// across the binary instead of starting one per test (§17.2.0.1).
#![cfg(feature = "testkit")]

use std::sync::Arc;

use meterstore::cold::IcebergCold;
use meterstore::config::TableConfig;
use meterstore::encode::schema::col;
use meterstore::hot::PostgresHot;
use meterstore::planner::ReadMode;
use meterstore::tiering::Archiver;
use meterstore::tiering::store::{ColdStore, HotStore, WriteHints, stream_of};
use meterstore::watermark::ArchivalWindow;

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use metering::measurement_series::MeasurementSource;
use sqlx::PgPool;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const TABLE: &str = "readings_versions";
const D18: OffsetDateTime = datetime!(2026-07-18 00:00 UTC);
const D19: OffsetDateTime = datetime!(2026-07-19 00:00 UTC);
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);
/// Chosen so a one-day settlement lag leaves exactly the last day hot:
/// the horizon lands on D20, so [D18, D19) and [D19, D20) archive and D20 does
/// not. Several tests below depend on that split actually occurring.
const ARCHIVE_AS_OF: OffsetDateTime = D21;
/// Wall clock for status snapshots, a day past the last archived window.
const NOW_FOR_STATUS: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);

struct Harness {
    hot: Arc<PostgresHot>,
    cold: Arc<IcebergCold>,
    _warehouse: tempfile::TempDir,
}

impl Harness {
    async fn start() -> Self {
        let url = meterstore::testkit::postgres::fresh_database()
            .await
            .expect("postgres");

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
            _warehouse: warehouse,
        }
    }

    /// Insert `count` quarter-hour readings for one meter, starting at `start`.
    async fn insert(&self, malo: &str, start: OffsetDateTime, count: i64, kwh: i64) {
        self.insert_versioned(malo, start, count, kwh, 20_260_727_000_001)
            .await
    }

    /// Insert readings at a specific MSCONS version.
    ///
    /// A correction is a new row at a higher version, never an overwrite, so
    /// calling this twice for the same interval leaves both rows in place.
    async fn insert_versioned(
        &self,
        malo: &str,
        start: OffsetDateTime,
        count: i64,
        kwh: i64,
        version: i64,
    ) {
        self.insert_scoped(malo, start, count, kwh, version, "99:2026-07")
            .await
    }

    /// Insert readings at a specific version *and* version scope.
    async fn insert_scoped(
        &self,
        malo: &str,
        start: OffsetDateTime,
        count: i64,
        kwh: i64,
        version: i64,
        scope: &str,
    ) {
        self.try_insert_scoped(malo, start, count, kwh, version, scope)
            .await
            .expect("insert");
    }

    /// [`insert_scoped`](Self::insert_scoped), surfacing the database error.
    async fn try_insert_scoped(
        &self,
        malo: &str,
        start: OffsetDateTime,
        count: i64,
        kwh: i64,
        version: i64,
        scope: &str,
    ) -> Result<(), sqlx::Error> {
        let detail = serde_json::to_string(&MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        })
        .unwrap();

        for i in 0..count {
            let from = start + Duration::minutes(15 * i);
            sqlx::query(&format!(
                r#"INSERT INTO "{TABLE}"
                   (malo_id, melo_id, obis_code, sparte, "from", "to", value, unit, quality,
                    resolution, source_kind, source_detail, provenance,
                    version, version_scope, recorded_at)
                   VALUES ($1,NULL,$2,'STROM',$3,$4,$5,'KWH','MEASURED','PT15M','mscons',
                           $6,'[]',$7,$9,$8)"#
            ))
            .bind(malo)
            .bind(meterstore::canonical_obis("1-0:1.8.0").unwrap())
            .bind(from)
            .bind(from + Duration::minutes(15))
            .bind(rust_decimal::Decimal::new(kwh, 0))
            .bind(detail.as_str())
            .bind(rust_decimal::Decimal::new(version, 0))
            .bind(datetime!(2026-07-27 06:00 UTC))
            .bind(scope)
            .execute(self.hot.pool())
            .await?;
        }
        Ok(())
    }

    /// Archive every window closed as of `now`, leaving the rest hot.
    async fn archive_through(&self, now: OffsetDateTime) {
        // Seed the watermark so the first window is the one we want.
        self.cold
            .append_and_commit(
                TABLE,
                stream_of(Vec::new()),
                WriteHints::default(),
                ArchivalWindow::new(D18 - Duration::DAY, D18).unwrap(),
            )
            .await
            .expect("seed watermark");

        let archiver = Archiver::new(
            Arc::clone(&self.hot),
            Arc::clone(&self.cold),
            TableConfig::new(TABLE)
                .settlement_lag(Duration::days(1))
                .build()
                .unwrap(),
        );
        archiver.catch_up(now, 32).await.expect("archive");
    }

    /// A store whose `readings` view resolves corrections to their latest
    /// version, and whose `readings_versions` table exposes the raw history.
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
            .table(
                TableConfig::new(TABLE)
                    .settlement_lag(Duration::days(1))
                    .build()
                    .unwrap(),
            )
            .read_mode(mode)
            .build()
            .await
            .expect("build store")
    }

    async fn scalar(&self, mode: ReadMode, sql: &str) -> i64 {
        let store = self.store(mode).await;
        let batches = store
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let array = batches[0].column(0);
        use datafusion::arrow::array::AsArray;
        array
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .value(0)
    }
}

#[tokio::test]
async fn a_query_spanning_the_watermark_counts_each_row_once() {
    // The property the whole tiering design exists to make true. Three days of
    // readings, two archived and one still hot: the total must be the sum, not
    // doubled at the boundary and not short.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert("11111111111", D18, 96, 1).await;
    h.insert("11111111111", D19, 96, 1).await;
    h.insert("11111111111", D20, 96, 1).await;

    h.archive_through(ARCHIVE_AS_OF).await;

    // Sanity: the split really did happen — some rows are gone from Postgres.
    let still_hot: i64 = sqlx::query_scalar(&format!(r#"SELECT count(*) FROM "{TABLE}""#))
        .fetch_one(h.hot.pool())
        .await
        .unwrap();
    assert!(
        still_hot > 0 && still_hot < 288,
        "expected a real split, got {still_hot}"
    );

    assert_eq!(
        h.scalar(ReadMode::Unified, "SELECT COUNT(*) FROM readings")
            .await,
        288
    );
}

#[tokio::test]
async fn a_sum_across_the_boundary_is_correct() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert("11111111111", D18, 4, 10).await;
    h.insert("11111111111", D20, 6, 10).await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let total = h
        .scalar(
            ReadMode::Unified,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await;
    assert_eq!(total, 100, "10 readings of 10 kWh");
}

#[tokio::test]
async fn a_time_filter_restricts_to_one_tier() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert("11111111111", D18, 96, 1).await;
    h.insert("11111111111", D20, 96, 1).await;
    h.archive_through(ARCHIVE_AS_OF).await;

    // Entirely below the watermark: cold only.
    let cold = h
        .scalar(
            ReadMode::Unified,
            &format!(
                r#"SELECT COUNT(*) FROM readings
                   WHERE "{}" >= TIMESTAMP '2026-07-18 00:00:00'
                     AND "{}" <  TIMESTAMP '2026-07-19 00:00:00'"#,
                col::FROM,
                col::FROM
            ),
        )
        .await;
    assert_eq!(cold, 96);
}

#[tokio::test]
async fn historical_mode_sees_only_archived_data() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert("11111111111", D18, 96, 1).await;
    h.insert("11111111111", D20, 96, 1).await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let unified = h
        .scalar(ReadMode::Unified, "SELECT COUNT(*) FROM readings")
        .await;
    let historical = h
        .scalar(ReadMode::Historical, "SELECT COUNT(*) FROM readings")
        .await;
    let operational = h
        .scalar(ReadMode::Operational, "SELECT COUNT(*) FROM readings")
        .await;

    assert_eq!(unified, 192);
    assert!(historical > 0, "some data was archived");
    assert!(operational > 0, "some data is still hot");
    assert_eq!(
        historical + operational,
        unified,
        "the tiers must partition the data exactly"
    );
}

#[tokio::test]
async fn a_group_by_spans_both_tiers() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert("11111111111", D18, 4, 1).await;
    h.insert("22222222222", D18, 4, 1).await;
    h.insert("11111111111", D20, 4, 1).await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    let batches = store
        .sql(&format!(
            "SELECT {}, COUNT(*) AS n FROM readings GROUP BY 1 ORDER BY 1",
            col::MALO_ID
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "two distinct meters");
}

#[tokio::test]
async fn nothing_archived_yet_still_answers_from_the_hot_tier() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D20, 12, 1).await;

    // No archival: the watermark is at the epoch and there is no cold scan.
    assert_eq!(
        h.scalar(ReadMode::Unified, "SELECT COUNT(*) FROM readings")
            .await,
        12
    );
}

#[tokio::test]
async fn the_store_handle_registers_everything_needed_for_a_query() {
    // The whole point of the handle: no manual provider assembly, and the
    // calendar functions are already available.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 8, 3).await;
    h.insert("11111111111", D20, 8, 3).await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = meterstore::MeterStore::builder()
        .hot(Arc::clone(&h.hot) as Arc<dyn HotStore>)
        .cold(
            Arc::clone(&h.cold) as Arc<dyn ColdStore>,
            h.cold.table_provider(TABLE).await.unwrap(),
        )
        .table(
            TableConfig::new(TABLE)
                .settlement_lag(Duration::days(1))
                .build()
                .unwrap(),
        )
        .build()
        .await
        .expect("build store");

    // Registered as `readings`, not `readings_versions`.
    let batches = store
        .sql("SELECT COUNT(*) FROM readings")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    use datafusion::arrow::array::AsArray;
    let n = batches[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0);
    assert_eq!(n, 16, "both tiers, each row once");

    // The calendar functions are registered too.
    let grouped = store
        .sql(&format!(
            r#"SELECT meter_local_day("{}") AS d, COUNT(*) FROM readings GROUP BY 1"#,
            col::FROM
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let days: usize = grouped.iter().map(|b| b.num_rows()).sum();
    assert_eq!(days, 2, "two local calendar days");

    // And the watermark is reachable without touching the cold store directly.
    assert_eq!(store.watermark().await.unwrap().get(), D20);
    store
        .verify_invariant()
        .await
        .expect("tiers must partition the data");
}

/// A series of quarter-hour readings at a given version.
fn correction(
    malo: &str,
    start: OffsetDateTime,
    count: i64,
    kwh: i64,
    version: u128,
) -> meterstore::encode::StoredSeries {
    let intervals = (0..count)
        .map(|i| {
            let from = start + Duration::minutes(15 * i);
            metering::interval::MeterInterval {
                from,
                to: from + Duration::minutes(15),
                value: rust_decimal::Decimal::new(kwh, 0),
                quality: metering::QualityFlag::Corrected,
                obis_code: "1-0:1.8.0".parse().ok(),
            }
        })
        .collect();

    let mut series = metering::measurement_series::MeasurementSeries::new(
        malo,
        "1-0:1.8.0".parse().ok(),
        intervals,
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "99".to_string(),
        },
        datetime!(2026-07-26 06:00 UTC),
    );
    series.resolution = Some(metering::IntervalResolution::QuarterHour);

    meterstore::encode::StoredSeries::new(
        series,
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("99", start).unwrap(),
            meterstore::Version::new(version).unwrap(),
        ),
        datetime!(2026-07-26 06:00 UTC),
    )
}

#[tokio::test]
async fn a_corrected_interval_is_counted_once_at_its_latest_version() {
    // A correction is a new row at a higher version. Both rows are stored — that
    // is the audit trail — but a query must return only the value that currently
    // holds, or every corrected interval is double-counted in a number someone
    // bills from.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    // Original: 4 intervals at 10 kWh.
    h.insert_versioned("11111111111", D20, 4, 10, 20_260_720_000_001)
        .await;
    // Correction: the same 4 intervals, restated as 25 kWh.
    h.insert_versioned("11111111111", D20, 4, 25, 20_260_725_000_002)
        .await;

    // Both rows are present in storage.
    let stored: i64 = sqlx::query_scalar(&format!(r#"SELECT count(*) FROM "{TABLE}""#))
        .fetch_one(h.hot.pool())
        .await
        .unwrap();
    assert_eq!(
        stored, 8,
        "the original must be retained alongside the correction"
    );

    // But the query must see four intervals at the corrected value.
    let rows = h
        .scalar(ReadMode::Unified, "SELECT COUNT(*) FROM readings")
        .await;
    assert_eq!(rows, 4, "one row per interval, not one per version");

    let total = h
        .scalar(
            ReadMode::Unified,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await;
    assert_eq!(total, 100, "4 x 25 kWh, not 4 x 10 + 4 x 25");
}

#[tokio::test]
async fn a_correction_spanning_the_tier_boundary_resolves_correctly() {
    // The hard case: the original was archived to Iceberg, then a correction
    // arrived. Resolution has to work across the union, not within one tier.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert_versioned("11111111111", D18, 2, 10, 20_260_718_000_001)
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    // The correction arrives after archival, for an interval now in the cold
    // tier. Writing it to PostgreSQL would place it below the watermark, where
    // no query looks — so the store routes it to Iceberg instead.
    let store = h.store(ReadMode::Unified).await;
    let outcome = store
        .append(&[correction("11111111111", D18, 2, 99, 20_260_726_000_002)])
        .await
        .expect("append correction");

    assert!(
        outcome.had_late_corrections(),
        "a correction for an archived interval must be routed to the cold tier"
    );
    assert_eq!(outcome.hot_rows, 0, "nothing may land below the watermark");

    let total = h
        .scalar(
            ReadMode::Unified,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await;
    assert_eq!(total, 198, "2 x 99, the corrected value only");
}

#[tokio::test]
async fn an_empty_table_returns_no_rows() {
    let h = Harness::start().await;
    assert_eq!(
        h.scalar(ReadMode::Unified, "SELECT COUNT(*) FROM readings")
            .await,
        0
    );
}

#[tokio::test]
async fn a_filter_cutting_inside_a_cold_file_is_applied_exactly() {
    // We report time filters as `Exact`, which lets DataFusion drop them from
    // the plan. That is only safe if the cold scan really applies them — Iceberg
    // prunes whole files, and a filter cutting *within* one must still be
    // enforced row by row or extra rows leak through silently.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 1).await;
    h.archive_through(ARCHIVE_AS_OF).await;

    // Half of one archived day: 48 of the 96 quarter-hours.
    let half = h
        .scalar(
            ReadMode::Historical,
            r#"SELECT COUNT(*) FROM readings
               WHERE "from" >= TIMESTAMP '2026-07-18 00:00:00'
                 AND "from" <  TIMESTAMP '2026-07-18 12:00:00'"#,
        )
        .await;
    assert_eq!(
        half, 48,
        "a filter inside a file must be applied, not just pruned"
    );
}

#[tokio::test]
async fn a_correction_delivered_in_a_later_month_still_supersedes() {
    // MSCONS assigns versions per network operator per month. If the scope were
    // keyed to the *delivery* month, a January interval corrected in February
    // would carry a different scope from its original — resolution partitions by
    // scope, so both rows would survive and the total would be doubled.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    // Both rows carry the scope of the *interval's* month, which is what makes
    // their versions comparable — even though the correction was delivered in
    // August.
    h.insert_scoped("11111111111", D20, 2, 10, 20_260_720_000_001, "99:2026-07")
        .await;
    h.insert_scoped("11111111111", D20, 2, 40, 20_260_820_000_002, "99:2026-07")
        .await;

    let total = h
        .scalar(
            ReadMode::Unified,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await;
    assert_eq!(total, 80, "2 x 40, the correction only");
}

#[tokio::test]
async fn versions_in_different_scopes_do_not_resolve_against_each_other() {
    // If a writer keyed the scope to the *delivery* month instead of the
    // interval's, a July reading corrected in August would carry two scopes.
    // Their versions are not comparable, so resolution cannot pick a winner and
    // both rows survive — the total is doubled with no error anywhere.
    //
    // The guard is `VersionScope::for_interval`, which derives the month from
    // the interval, plus an encode-time check that every interval falls inside
    // its declared scope. Reaching this state requires bypassing both — which
    // this test does with raw SQL, precisely to pin what the guard prevents.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert_scoped("11111111111", D20, 2, 10, 20_260_720_000_001, "99:2026-07")
        .await;
    h.insert_scoped("11111111111", D20, 2, 40, 20_260_820_000_002, "99:2026-08")
        .await;

    let total = h
        .scalar(
            ReadMode::Unified,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await;
    assert_eq!(
        total, 100,
        "mismatched scopes leave both rows standing — 2 x 10 + 2 x 40"
    );
}

#[tokio::test]
async fn system_tables_answer_the_questions_an_incident_asks() {
    // Is archival keeping up, and is anything stranded in the wrong tier.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 8, 1).await;
    h.insert("11111111111", D20, 8, 1).await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    store.refresh_system_tables(NOW_FOR_STATUS).await.unwrap();

    let batches = store
        .sql(r#"SELECT "table", invariant_violations, healthy FROM system.tables"#)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    use datafusion::arrow::array::AsArray;
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0]
            .column(1)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .value(0),
        0,
        "nothing may be stranded below the watermark"
    );
    assert!(batches[0].column(2).as_boolean().value(0), "healthy");
}

#[tokio::test]
async fn system_status_reports_watermark_lag() {
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    let status = store.status(NOW_FOR_STATUS).await.unwrap();

    assert_eq!(status.watermark, D20, "two windows archived");
    assert!(
        status.watermark_lag_seconds > 0,
        "lag is what tells an operator archival has fallen behind"
    );
    assert!(status.healthy);
}

#[tokio::test]
async fn system_config_shows_the_settings_that_interact() {
    let h = Harness::start().await;
    let store = h.store(ReadMode::Unified).await;
    store.refresh_system_tables(NOW_FOR_STATUS).await.unwrap();

    let batches = store
        .sql(
            r#"SELECT value FROM system.config
               WHERE setting IN ('partition_step', 'archival_step')"#,
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    use datafusion::arrow::array::AsArray;
    let values = batches[0].column(0).as_string::<i32>();
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(
        values.value(0),
        values.value(1),
        "a mismatch here degrades purge to row-wise DELETE, so it must be visible"
    );
}

/// Sum `value` as an integer, for a store already built.
async fn sum_kwh(store: &meterstore::MeterStore, sql: &str) -> i64 {
    use datafusion::arrow::array::AsArray;
    let batches = store
        .sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("run");
    batches[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0)
}

/// Whether a plan ranks rows — the observable signature of version resolution.
async fn ranks_rows(store: &meterstore::MeterStore, sql: &str) -> bool {
    let plan = store
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let rendered = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    rendered.contains("WindowAggr") || rendered.contains("BoundedWindowAggExec")
}

#[tokio::test]
async fn a_correction_free_history_scan_skips_resolution_entirely() {
    // The optimisation the cold tier's layout exists to enable: Iceberg records
    // min/max `version` per file, so a partition with one version per key can be
    // proven correction-free from metadata and read without ranking anything.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_versioned("11111111111", D18, 4, 10, 1).await;
    h.insert_versioned("11111111111", D19, 4, 10, 1).await;
    h.archive_through(D21).await;

    let store = h.store(ReadMode::Unified).await;
    assert!(
        !ranks_rows(
            &store,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings WHERE \"from\" >= \
             '2026-07-18T00:00:00Z' AND \"from\" < '2026-07-19T00:00:00Z'"
        )
        .await,
        "no correction exists in range, so nothing should be ranked"
    );
}

#[tokio::test]
async fn a_corrected_history_scan_still_resolves() {
    // The other half, and the one that matters: eliding here would return a
    // corrected interval twice and overstate every sum over it. Statistics have
    // to *prove* the absence of corrections, and here they cannot.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_versioned("11111111111", D18, 4, 10, 1).await;
    h.insert_versioned("11111111111", D18, 4, 25, 2).await;
    h.archive_through(D21).await;

    let store = h.store(ReadMode::Unified).await;
    let sql = "SELECT CAST(SUM(value) AS BIGINT) FROM readings WHERE \"from\" >= \
               '2026-07-18T00:00:00Z' AND \"from\" < '2026-07-19T00:00:00Z'";

    assert!(
        ranks_rows(&store, sql).await,
        "a correction is present, so resolution must not be skipped"
    );
    assert_eq!(
        sum_kwh(&store, sql).await,
        100,
        "4 intervals at the corrected value of 25, not 140"
    );
}

#[tokio::test]
async fn a_scan_touching_the_hot_tier_always_resolves() {
    // The hot tier keeps no per-file statistics, and proving it correction-free
    // would cost a scan of exactly the rows the query was avoiding. So any query
    // reaching above the watermark resolves, whatever the cold side proves.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_versioned("11111111111", D18, 4, 10, 1).await;
    h.insert_versioned("11111111111", D20, 4, 10, 1).await;
    h.archive_through(D20).await;

    let store = h.store(ReadMode::Unified).await;
    assert!(
        ranks_rows(&store, "SELECT CAST(SUM(value) AS BIGINT) FROM readings").await,
        "an unbounded query spans both tiers and must resolve"
    );
}

#[tokio::test]
async fn eliding_returns_the_same_rows_as_resolving() {
    // The safety net under the optimisation: whichever path a query takes, the
    // answer must be identical. `readings_versions` holds one version per key
    // here, so the raw table and the resolved one must agree exactly.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_versioned("11111111111", D18, 4, 10, 1).await;
    h.insert_versioned("22222222222", D18, 4, 7, 1).await;
    h.archive_through(D21).await;

    let store = h.store(ReadMode::Unified).await;
    let where_clause = "WHERE \"from\" >= '2026-07-18T00:00:00Z' \
                        AND \"from\" < '2026-07-19T00:00:00Z'";

    assert_eq!(
        sum_kwh(
            &store,
            &format!("SELECT CAST(SUM(value) AS BIGINT) FROM readings {where_clause}")
        )
        .await,
        sum_kwh(
            &store,
            &format!("SELECT CAST(SUM(value) AS BIGINT) FROM readings_versions {where_clause}")
        )
        .await,
    );
}

#[tokio::test]
async fn a_long_lived_store_keeps_serving_rows_across_an_archival_run() {
    // The embedded topology (§5.2): one process both archives and answers
    // queries. A cold provider that froze its snapshot at construction would
    // make every archived row vanish — gone from PostgreSQL because the
    // partition was dropped, invisible in Iceberg because the provider still
    // pointed at the snapshot from before the commit — until the process
    // restarted. Nothing would report it; the totals would simply shrink.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 1).await;
    h.insert("11111111111", D19, 96, 1).await;
    h.insert("11111111111", D20, 96, 1).await;

    // Built *before* anything is archived, and used throughout.
    let store = h.store(ReadMode::Unified).await;
    let before = store
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("query");
    assert_eq!(count(&before), 288);

    // Seed the watermark and archive through the same handle.
    store
        .cold_store()
        .append_and_commit(
            TABLE,
            stream_of(Vec::new()),
            WriteHints::default(),
            ArchivalWindow::new(D18 - Duration::DAY, D18).unwrap(),
        )
        .await
        .expect("seed watermark");
    store.archive(ARCHIVE_AS_OF, 8).await.expect("archive");

    // Rows really did move: the hot tier is now short.
    let still_hot: i64 = sqlx::query_scalar(&format!(r#"SELECT count(*) FROM "{TABLE}""#))
        .fetch_one(h.hot.pool())
        .await
        .unwrap();
    assert!(still_hot < 288, "expected a real split, got {still_hot}");

    let after = store
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("query");
    assert_eq!(count(&after), 288, "no row may disappear on archival");
    assert!(after.spans_tiers(), "{:?}", after.tiers_scanned());
}

/// The single count a `COUNT(*)` produces.
fn count(result: &meterstore::QueryResult) -> i64 {
    use datafusion::arrow::array::AsArray;
    result.batches()[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0)
}

#[tokio::test]
async fn an_exact_filter_is_honoured_on_the_raw_table_too() {
    // `a_filter_cutting_inside_a_cold_file_is_applied_exactly` queries `readings`,
    // whose provider reports every filter `Inexact` — so DataFusion re-applies it
    // above the scan and the test passes whatever the tiers do. The **raw** table
    // is where the claim actually bites: `TieredTableProvider` reports time
    // filters `Exact`, DataFusion drops them, and nothing above the scan will
    // catch a row the cold half failed to exclude.
    //
    // That path runs through `iceberg-datafusion`, which converts DataFusion
    // expressions to Iceberg predicates with `filter_map` — silently discarding
    // anything it cannot translate. If our injected range filter were ever among
    // them, this query would return the whole day.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 1).await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let half = h
        .scalar(
            ReadMode::Historical,
            r#"SELECT COUNT(*) FROM readings_versions
               WHERE "from" >= TIMESTAMP '2026-07-18 00:00:00'
                 AND "from" <  TIMESTAMP '2026-07-18 12:00:00'"#,
        )
        .await;
    assert_eq!(
        half, 48,
        "the raw table must honour a filter it reported Exact"
    );
}

#[tokio::test]
async fn an_exact_filter_is_honoured_across_the_boundary_on_the_raw_table() {
    // The same claim where it is hardest: a range that cuts inside an archived
    // file *and* inside the hot window. Both halves must exclude their own
    // out-of-range rows, because nothing above the union will.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111111", D18, 96, 1).await; // archived
    h.insert("11111111111", D19, 96, 1).await; // archived
    h.insert("11111111111", D20, 96, 1).await; // stays hot
    h.archive_through(ARCHIVE_AS_OF).await;

    // Noon on the last archived day through noon on the hot day: 48 cold + 48 hot.
    let spanning = h
        .scalar(
            ReadMode::Unified,
            r#"SELECT COUNT(*) FROM readings_versions
               WHERE "from" >= TIMESTAMP '2026-07-19 12:00:00'
                 AND "from" <  TIMESTAMP '2026-07-20 12:00:00'"#,
        )
        .await;
    assert_eq!(
        spanning, 96,
        "48 from each tier, and nothing outside the range"
    );
}

#[tokio::test]
async fn two_network_operators_for_one_interval_do_not_silently_double_a_sum() {
    // The scope is (network operator, month), and the month half is guarded:
    // `VersionScope::for_interval` derives it from the interval and encoding
    // rejects a scope that does not cover its intervals. The **operator** half
    // has no such guard — it is whatever the caller passed.
    //
    // Two operators for one `(malo_id, obis_code, from)` therefore produce two
    // incomparable scopes, resolution picks a winner in each, and both survive
    // into `readings`. A billing `SUM` doubles with nothing anywhere to say so.
    //
    // The realistic cause is not a Netzbetreiberwechsel — those deliver for
    // *different* intervals. It is a caller passing the wrong identifier: the
    // forwarding party's MP-ID, or a tenant id, instead of the network
    // operator's. That is a one-line integration mistake with a silent,
    // financial consequence, which is exactly what this store exists to refuse.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert_scoped("11111111111", D20, 2, 10, 20_260_720_000_001, "99:2026-07")
        .await;

    // Same interval, same month, *different* operator.
    let second = h
        .try_insert_scoped("11111111111", D20, 2, 40, 20_260_720_000_002, "88:2026-07")
        .await;

    assert!(
        second.is_err(),
        "a second network operator for the same interval must be refused: \
         both rows would survive resolution and double every sum over them"
    );
}

#[tokio::test]
async fn a_late_correction_cannot_smuggle_a_second_operator_past_the_hot_guard() {
    // The hot tier refuses a second network operator for one reading. Iceberg
    // has no constraints at all, and `append` routes a below-watermark interval
    // straight there — so the guard must not be reachable only by going around
    // it. If it is, the same silent doubling returns for exactly the deliveries
    // most likely to carry a stale operator: late ones.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_scoped("11111111111", D18, 2, 10, 20_260_718_000_001, "99:2026-07")
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let before = h
        .scalar(
            ReadMode::Unified,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await;
    assert_eq!(before, 20, "two archived intervals at 10 each");

    // Same reading, different operator, below the watermark — so it routes cold.
    let store = h.store(ReadMode::Unified).await;
    let mut other = correction("11111111111", D18, 2, 40, 20_260_718_000_002);
    other.version = meterstore::ScopedVersion::new(
        meterstore::VersionScope::for_interval("88", D18).unwrap(),
        meterstore::Version::new(20_260_718_000_002).unwrap(),
    );
    let late = store.append(&[other]).await;

    let after = h
        .scalar(
            ReadMode::Unified,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await;

    let err = late.expect_err("the cold path must refuse a second operator too");
    let msg = err.to_string();
    assert!(msg.contains("network operator"), "{msg}");
    assert!(
        msg.contains("\"99\"") && msg.contains("\"88\""),
        "the message must name both scopes so the caller can tell which is wrong: {msg}"
    );
    assert_eq!(
        after, before,
        "and nothing may have been written before the refusal"
    );
}
