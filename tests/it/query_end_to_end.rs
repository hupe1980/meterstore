//! SQL across both tiers, against real PostgreSQL and a real Iceberg warehouse.
//!
//! The unit tests prove the provider's routing against fakes. These prove the
//! whole path: rows written to PostgreSQL, archived to Iceberg, then read back
//! through one SQL statement that spans the watermark — and that the totals come
//! out right rather than doubled or short.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them: `testkit::postgres` is what shares one container
// across the binary instead of starting one per test.
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
            .with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory))
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
        self.insert_scoped(malo, start, count, kwh, version, "9900000000001:2026-07")
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
            sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
        })
        .unwrap();

        for i in 0..count {
            let from = start + Duration::minutes(15 * i);
            sqlx::query(&format!(
                r#"INSERT INTO "{TABLE}"
                   (malo_id, melo_id, obis_code, sparte, "from", "to", value, unit, quality,
                    resolution, source_kind, source_detail, provenance,
                    version, version_scope, recorded_at, balancing_day)
                   VALUES ($1,NULL,$2,'STROM',$3,$4,$5,'KWH','MEASURED','PT15M','MSCONS',
                           $6,'[]',$7,$9,$8,
                           CAST(($3 AT TIME ZONE 'Europe/Berlin') AS DATE))"#
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
                D18,
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
async fn a_plan_built_before_archival_still_reads_the_window_archival_moved() {
    // The one place the tiering invariant is observable broken, and it is a
    // *reader* that observes it.
    //
    // A query decides its tier split from the watermark it reads when it is
    // **planned**, and reads the tiers when it **executes**. Archival between
    // those two moments moves a window from hot to cold: the plan is already
    // asking PostgreSQL for `[W, …)` and Iceberg for `[…, W)`, so if the hot
    // partition is gone by the time the plan runs, the window is in neither
    // half — one day short, silently, on the settlement query most likely to be
    // long enough to overlap a maintenance cycle.
    //
    // The fix is that archival does not reclaim the partition it just archived;
    // `reader_grace` keeps it detached, and a detached partition is still read.
    // This test is the regression, so it splits `plan` from `execute`
    // deliberately — which is exactly what `DataFrame::collect` does in one step.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert("11111111115", D18, 96, 1).await;
    h.insert("11111111115", D19, 96, 1).await;
    h.insert("11111111115", D20, 96, 1).await;

    let store = h.store(ReadMode::Unified).await;
    let ctx = store.context();

    // Plan now — this is where the watermark is read.
    let plan = store
        .sql(&format!(r#"SELECT count(*) AS n FROM "{TABLE}""#))
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");

    // Archival runs while the plan is held, moving two days out of PostgreSQL.
    h.archive_through(ARCHIVE_AS_OF).await;
    let still_hot: i64 = sqlx::query_scalar(&format!(r#"SELECT count(*) FROM "{TABLE}""#))
        .fetch_one(h.hot.pool())
        .await
        .unwrap();
    assert_eq!(still_hot, 96, "two days should have left the parent table");

    // Execute the plan built against the older boundary.
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("execute");
    use datafusion::arrow::array::AsArray;
    let n = batches[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0);

    assert_eq!(
        n, 288,
        "a plan made before the boundary moved must still see every row; \
         got {n} of 288, so archival reclaimed a partition a live plan needed"
    );
}

#[tokio::test]
async fn the_reported_boundary_is_the_one_the_plan_was_built_against() {
    // P1 says correctness is observable rather than assumed, and the observable
    // is this label. It was read separately from the split that used it — once
    // before planning for the report, once during planning by each provider — so
    // an archival commit in between made the label name a boundary the answer was
    // not computed against. Over several tables it compounded: every provider
    // read its own, at its own moment.
    //
    // The boundary now travels with the plan, so the two cannot differ. The
    // observable property is that a query reports the split it actually used:
    // rows below the reported watermark came from Iceberg, rows at or above it
    // from PostgreSQL, and the counts add up.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert("11111111115", D18, 96, 1).await;
    h.insert("11111111115", D19, 96, 1).await;
    h.insert("11111111115", D20, 96, 1).await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    let result = store
        .query(&format!(r#"SELECT count(*) AS n FROM "{TABLE}""#))
        .await
        .expect("query");

    let reported = result.watermark();
    assert_eq!(
        reported.get(),
        D20,
        "the reported boundary must be the live one"
    );

    // The rows the answer was computed from, split at the boundary it claims.
    let below: i64 = h
        .scalar(
            ReadMode::Unified,
            &format!(
                r#"SELECT count(*) FROM "{TABLE}" WHERE "from" < TIMESTAMP '2026-07-20T00:00:00Z'"#
            ),
        )
        .await;
    let at_or_above: i64 = h
        .scalar(
            ReadMode::Unified,
            &format!(
                r#"SELECT count(*) FROM "{TABLE}" WHERE "from" >= TIMESTAMP '2026-07-20T00:00:00Z'"#
            ),
        )
        .await;
    assert_eq!(below, 192, "two days are below the reported boundary");
    assert_eq!(at_or_above, 96, "one day is at or above it");
    assert!(
        result.touched_hot_tier(),
        "and the answer says it read the hot tier, because it did"
    );
}

#[tokio::test]
async fn describing_a_statement_agrees_with_running_it() {
    // `describe` is what a Flight SQL client calls before it starts rendering a
    // stream, and its whole justification is that answering by *running* the
    // query would cost two scans. That only holds if the two surfaces cannot
    // disagree — so the description has to come off the same physical plan the
    // run does, not off the logical one, where nullability and field metadata
    // are not yet settled.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert("11111111115", D18, 96, 1).await;
    h.insert("11111111115", D20, 96, 1).await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    let sql = format!(r#"SELECT malo_id, sum(value) AS kwh FROM "{TABLE}" GROUP BY 1"#);

    let described = store.describe(&sql).await.expect("describe");
    let ran = store.query(&sql).await.expect("query");

    assert_eq!(
        described.schema().as_ref(),
        ran.schema().as_ref(),
        "a description that does not match the result is worse than no description"
    );
    assert_eq!(described.watermark(), ran.watermark());
    assert_eq!(described.touched_hot_tier(), ran.touched_hot_tier());
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

    h.insert("11111111115", D18, 96, 1).await;
    h.insert("11111111115", D19, 96, 1).await;
    h.insert("11111111115", D20, 96, 1).await;

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

    h.insert("11111111115", D18, 4, 10).await;
    h.insert("11111111115", D20, 6, 10).await;
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

    h.insert("11111111115", D18, 96, 1).await;
    h.insert("11111111115", D20, 96, 1).await;
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

    h.insert("11111111115", D18, 96, 1).await;
    h.insert("11111111115", D20, 96, 1).await;
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

    h.insert("11111111115", D18, 4, 1).await;
    h.insert("22222222220", D18, 4, 1).await;
    h.insert("11111111115", D20, 4, 1).await;
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
    h.insert("11111111115", D20, 12, 1).await;

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
    h.insert("11111111115", D18, 8, 3).await;
    h.insert("11111111115", D20, 8, 3).await;
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
        malo.parse().expect("a valid MaLo-ID"),
        "1-0:1.8.0".parse().ok(),
        intervals,
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
        },
        datetime!(2026-07-26 06:00 UTC),
    );
    series.resolution = Some(metering::IntervalResolution::QuarterHour);

    meterstore::encode::StoredSeries::new(
        series,
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval(
                "9900000000001",
                start,
                metering::interval::Sparte::Strom,
            )
            .unwrap(),
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
    h.insert_versioned("11111111115", D20, 4, 10, 20_260_720_000_001)
        .await;
    // Correction: the same 4 intervals, restated as 25 kWh.
    h.insert_versioned("11111111115", D20, 4, 25, 20_260_725_000_002)
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

    h.insert_versioned("11111111115", D18, 2, 10, 20_260_718_000_001)
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    // The correction arrives after archival, for an interval now in the cold
    // tier. Writing it to PostgreSQL would place it below the watermark, where
    // no query looks — so the store routes it to Iceberg instead.
    let store = h.store(ReadMode::Unified).await;
    let outcome = store
        .append(&[correction("11111111115", D18, 2, 99, 20_260_726_000_002)])
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
async fn a_replayed_late_correction_is_not_stored_twice() {
    // The hot tier absorbs a replay with its primary key; Iceberg has no
    // constraints. Two rows at one version are two winners, and a historical
    // scan whose files hold a single version elides resolution entirely — so the
    // raw rows come back and the sum doubles with nothing reporting it.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert_versioned("11111111115", D18, 2, 10, 20_260_718_000_001)
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    let correction = correction("11111111115", D18, 2, 99, 20_260_726_000_002);

    let first = store
        .append(std::slice::from_ref(&correction))
        .await
        .expect("first");
    assert_eq!(first.cold_rows, 2, "the correction lands in the cold tier");

    let replay = store.append(&[correction]).await.expect("replay");
    assert_eq!(
        replay.cold_rows, 0,
        "a redelivery must write nothing to the cold tier"
    );
    assert!(
        replay
            .displacements
            .iter()
            .all(|d| d.effect == meterstore::session::Effect::Duplicate),
        "every replayed interval reports as a duplicate: {:?}",
        replay.displacements
    );

    let total = h
        .scalar(
            ReadMode::Unified,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await;
    assert_eq!(total, 198, "2 x 99 — the replay must not double it");

    // And the audit trail holds one row per version, not three.
    let raw = h
        .scalar(ReadMode::Unified, "SELECT COUNT(*) FROM readings_versions")
        .await;
    assert_eq!(
        raw, 4,
        "two originals and two corrections, nothing repeated"
    );
}

#[tokio::test]
async fn a_replayed_delivery_cannot_slip_past_an_elided_resolution() {
    // The sharpest form of the duplicate hazard, and the one that produces a
    // wrong *number* rather than an extra row.
    //
    // A delivery replayed after its window was archived carries the version it
    // always had. Stored twice, the cold files in range all still hold that one
    // version — so the planner proves the range correction-free and **elides
    // resolution**, the raw rows are returned, and the sum doubles. Resolution
    // would have masked it; elision is exactly the case where it cannot.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert_versioned("11111111115", D18, 2, 10, 20_260_718_000_001)
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    let replay = store
        .append(&[correction("11111111115", D18, 2, 10, 20_260_718_000_001)])
        .await
        .expect("a replay is ordinary traffic, not an error");
    assert_eq!(replay.cold_rows, 0, "nothing new to store");

    // Cold-only, so elision is on the table — and the answer has to be right
    // either way.
    let total = h
        .scalar(
            ReadMode::Historical,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await;
    assert_eq!(total, 20, "2 x 10 kWh, not 4 x 10");
}

#[tokio::test]
async fn concurrent_late_corrections_do_not_both_write() {
    // Reconciling reads what is stored and then writes what is left. Two
    // processes doing that at once would both find no stored row and both
    // append, storing the reading twice at one version — which resolution cannot
    // collapse, and which an elided historical scan then returns twice.
    //
    // The hot tier gets that exclusion from its primary key. Iceberg has none,
    // so it is a lease, and this is what proves the lease is doing the work.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_versioned("11111111115", D18, 2, 10, 20_260_718_000_001)
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    // Two independent stores over one database and one warehouse — the
    // replicated topology, not two handles that happen to share a lock.
    let a = h.store(ReadMode::Unified).await;
    let b = h.store(ReadMode::Unified).await;
    let correction = correction("11111111115", D18, 2, 99, 20_260_726_000_002);

    let (ra, rb) = tokio::join!(
        a.append(std::slice::from_ref(&correction)),
        b.append(std::slice::from_ref(&correction)),
    );
    let written: u64 = [ra.expect("a"), rb.expect("b")]
        .iter()
        .map(|o| o.cold_rows)
        .sum();
    assert_eq!(written, 2, "the correction lands once, not twice");

    // The audit trail holds two originals and two corrections, and the resolved
    // sum is the corrected value rather than double it.
    assert_eq!(
        h.scalar(ReadMode::Unified, "SELECT COUNT(*) FROM readings_versions")
            .await,
        4
    );
    assert_eq!(
        h.scalar(
            ReadMode::Historical,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await,
        198,
        "2 x 99 — and this range elides resolution, so a duplicate would show"
    );
}

#[tokio::test]
async fn a_late_correction_reports_what_it_displaced() {
    // The cold tier's displacement report. Documented from the start and, until
    // now, only ever produced for the hot tier — so a caller building a
    // correction audit trail silently got nothing for exactly the deliveries
    // that most need one.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert_versioned("11111111115", D18, 1, 10, 20_260_718_000_001)
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    let outcome = store
        .append(&[correction("11111111115", D18, 1, 99, 20_260_726_000_002)])
        .await
        .expect("append correction");

    assert_eq!(outcome.displacements.len(), 1);
    let d = &outcome.displacements[0];
    assert_eq!(d.effect, meterstore::session::Effect::Superseded);
    assert_eq!(d.from, D18);
    assert_eq!(
        d.to,
        Some(D18 + Duration::minutes(15)),
        "the interval end travels, not a collapse to its start"
    );
    assert_eq!(
        d.superseded.as_ref().expect("a prior value").value,
        rust_decimal::Decimal::new(10, 0)
    );
    assert_eq!(d.written.value, rust_decimal::Decimal::new(99, 0));
    assert!(d.value_changed());

    // A backfill the correction already outranks is stored and reports as such.
    let backfill = store
        .append(&[correction("11111111115", D18, 1, 55, 20_260_719_000_001)])
        .await
        .expect("append backfill");
    assert_eq!(backfill.cold_rows, 1, "it joins the audit trail");
    assert_eq!(
        backfill.displacements[0].effect,
        meterstore::session::Effect::Shadowed,
        "an existing higher version still wins"
    );
}

#[tokio::test]
async fn restating_a_cold_value_under_an_existing_version_is_refused() {
    // A version identifies one assertion. Redelivering an identical row is
    // ordinary; restating a different value under the same version means a
    // producer is wrong, and keeping either copy silently would bury that. The
    // hot tier already refused this; the cold tier kept both.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert_versioned("11111111115", D18, 1, 10, 20_260_718_000_001)
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    store
        .append(&[correction("11111111115", D18, 1, 99, 20_260_726_000_002)])
        .await
        .expect("the correction itself is fine");

    let err = store
        .append(&[correction("11111111115", D18, 1, 42, 20_260_726_000_002)])
        .await
        .expect_err("a different value under the same version must be refused");
    let message = err.to_string();
    assert!(message.contains("higher version"), "{message}");
}

#[tokio::test]
async fn a_streamed_query_carries_its_boundary_before_the_first_row() {
    // The ordering is the whole point. A surface that has to put the boundary on
    // the wire — Flight SQL writes the schema before any batch — cannot get it
    // from a result it has not finished computing, and collecting the result to
    // find out would make the server's peak memory whatever a client asked for.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_versioned("11111111115", D18, 96, 10, 20_260_718_000_001)
        .await;
    h.insert_versioned("11111111115", D20, 96, 10, 20_260_720_000_001)
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    let (described, stream) = store
        .stream(r#"SELECT "from", value FROM readings ORDER BY "from""#)
        .await
        .expect("stream");

    // Provenance is in hand before a single row has been read.
    assert_eq!(described.watermark().get(), D20);
    assert!(
        described.spans_tiers(),
        "the range covers both sides of the boundary: {:?}",
        described.tiers_scanned()
    );
    assert!(described.schema().field_with_name("value").is_ok());

    use futures::StreamExt;
    let batches: Vec<_> = stream.collect::<Vec<_>>().await;
    let rows: usize = batches
        .into_iter()
        .map(|b| b.expect("batch").num_rows())
        .sum();
    assert_eq!(rows, 192, "the stream yields exactly what a collect would");

    // And it agrees with the collecting path, so the two cannot drift.
    assert_eq!(
        h.scalar(ReadMode::Unified, "SELECT COUNT(*) FROM readings")
            .await,
        192
    );
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
    h.insert("11111111115", D18, 96, 1).await;
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
    h.insert_scoped(
        "11111111115",
        D20,
        2,
        10,
        20_260_720_000_001,
        "9900000000001:2026-07",
    )
    .await;
    h.insert_scoped(
        "11111111115",
        D20,
        2,
        40,
        20_260_820_000_002,
        "9900000000001:2026-07",
    )
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

    h.insert_scoped(
        "11111111115",
        D20,
        2,
        10,
        20_260_720_000_001,
        "9900000000001:2026-07",
    )
    .await;
    h.insert_scoped(
        "11111111115",
        D20,
        2,
        40,
        20_260_820_000_002,
        "9900000000001:2026-08",
    )
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
    h.insert("11111111115", D18, 8, 1).await;
    h.insert("11111111115", D20, 8, 1).await;
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
    // **Twice**, because it is a refresh: `MemorySchemaProvider::register_table`
    // refuses a name it already holds, and the second call is the first one a
    // maintenance loop or an operator dashboard makes.
    store
        .refresh_system_tables(NOW_FOR_STATUS)
        .await
        .expect("a refresh must be repeatable");

    let batches = store
        .sql(
            r#"SELECT setting, value FROM system.config
               WHERE setting IN ('archival_step', 'settlement_lag', 'partition_step')
               ORDER BY setting"#,
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    use datafusion::arrow::array::AsArray;
    let settings = batches[0].column(0).as_string::<i32>();
    let names: Vec<&str> = (0..batches[0].num_rows())
        .map(|i| settings.value(i))
        .collect();
    // `settlement_lag` shorter than `archival_step` strands corrections below
    // the watermark, so the pair that can disagree is shown side by side.
    assert_eq!(names, ["archival_step", "settlement_lag"]);
    // And `partition_step` is gone: the partition granularity *is* the archival
    // step, so there is no second number that could disagree with it.
    assert!(!names.contains(&"partition_step"));
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
    h.insert_versioned("11111111115", D18, 4, 10, 1).await;
    h.insert_versioned("11111111115", D19, 4, 10, 1).await;
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
    h.insert_versioned("11111111115", D18, 4, 10, 1).await;
    h.insert_versioned("11111111115", D18, 4, 25, 2).await;
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
async fn consecutive_archival_windows_elide_resolution_however_their_versions_differ() {
    // The optimisation the cold layout exists for, on the shape real data has.
    //
    // MSCONS versions ascend per delivery and archival commits one day per
    // window, so a year of history is one file per day at a different version
    // each. The first rule — every file in range must hold the *same* version —
    // is sound and was true of almost nothing: every scan wider than a single day
    // resolved, and the provider-rather-than-a-view argument bought nothing.
    //
    // `from` is in the merge key, so two files covering different days cannot
    // hold the same key however their versions differ. This asserts the
    // consequence twice over: the plan carries no window aggregate, and the sum
    // is the same one resolution would have produced.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();

    // Two days, two deliveries, two versions — as an operator would issue them.
    h.insert_versioned("11111111115", D18, 4, 10, 20_260_718_000_001)
        .await;
    h.insert_versioned("11111111115", D19, 4, 20, 20_260_719_000_001)
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    let historical = "SELECT CAST(SUM(value) AS BIGINT) FROM readings                       WHERE \"from\" >= '2026-07-18T00:00:00Z'                         AND \"from\" < '2026-07-20T00:00:00Z'";

    assert!(
        !ranks_rows(&store, historical).await,
        "two disjoint daily windows cannot share a merge key, so nothing needs ranking"
    );
    assert_eq!(
        h.scalar(ReadMode::Unified, historical).await,
        4 * 10 + 4 * 20,
        "and the answer is still the resolved one"
    );
}

#[tokio::test]
async fn a_late_correction_overlapping_an_archived_day_still_resolves() {
    // The other half, and the one that keeps the optimisation honest. A
    // correction appends a second cold file covering a day already archived, at a
    // higher version — the two overlap on `from`, so a key really does appear in
    // both and eliding would return the superseded value alongside the current
    // one.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D18, D21, Duration::DAY)
        .await
        .unwrap();
    h.insert_versioned("11111111115", D18, 4, 10, 20_260_718_000_001)
        .await;
    h.insert_versioned("11111111115", D19, 4, 20, 20_260_719_000_001)
        .await;
    h.archive_through(ARCHIVE_AS_OF).await;

    let store = h.store(ReadMode::Unified).await;
    store
        .append(&[correction("11111111115", D18, 4, 99, 20_260_726_000_002)])
        .await
        .expect("late correction");

    let historical = "SELECT CAST(SUM(value) AS BIGINT) FROM readings                       WHERE \"from\" >= '2026-07-18T00:00:00Z'                         AND \"from\" < '2026-07-20T00:00:00Z'";

    assert!(
        ranks_rows(&store, historical).await,
        "the correction's file overlaps the original's day, so the versions must be ranked"
    );
    assert_eq!(
        h.scalar(ReadMode::Unified, historical).await,
        4 * 99 + 4 * 20,
        "the corrected value only — eliding here would add the superseded 4 x 10"
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
    h.insert_versioned("11111111115", D18, 4, 10, 1).await;
    h.insert_versioned("11111111115", D20, 4, 10, 1).await;
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
    h.insert_versioned("11111111115", D18, 4, 10, 1).await;
    h.insert_versioned("22222222220", D18, 4, 7, 1).await;
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
    // The embedded topology: one process both archives and answers
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
    h.insert("11111111115", D18, 96, 1).await;
    h.insert("11111111115", D19, 96, 1).await;
    h.insert("11111111115", D20, 96, 1).await;

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
            D18,
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
    h.insert("11111111115", D18, 96, 1).await;
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
    h.insert("11111111115", D18, 96, 1).await; // archived
    h.insert("11111111115", D19, 96, 1).await; // archived
    h.insert("11111111115", D20, 96, 1).await; // stays hot
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

    h.insert_scoped(
        "11111111115",
        D20,
        2,
        10,
        20_260_720_000_001,
        "9900000000001:2026-07",
    )
    .await;

    // Same interval, same month, *different* operator.
    let second = h
        .try_insert_scoped(
            "11111111115",
            D20,
            2,
            40,
            20_260_720_000_002,
            "9900000000002:2026-07",
        )
        .await;

    assert!(
        second.is_err(),
        "a second network operator for the same interval must be refused: \
         both rows would survive resolution and double every sum over them"
    );
}

#[tokio::test]
async fn a_duplicated_scope_that_got_past_the_constraint_is_refused_at_the_read() {
    // The exclusion is what *prevents* two network operators for one reading.
    // `PostgresHot::integrity_constraints(false)` is a supported setting, and out
    // of band anything can write to a PostgreSQL table — so the state exists, and
    // in it resolution returns two winners that agree on channel and on every
    // discriminator. Nothing narrows them apart, and a fold sums both.
    //
    // Dropping the partition's constraint reaches exactly that state on a live
    // store, which is what makes this an end-to-end assertion rather than a
    // restatement of the unit test.
    let h = Harness::start().await;
    h.hot
        .ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .unwrap();

    h.insert_scoped(
        "11111111115",
        D20,
        2,
        10,
        20_260_720_000_001,
        "9900000000001:2026-07",
    )
    .await;

    sqlx::query(&format!(
        r#"ALTER TABLE "{TABLE}_2026_07_20_0000" DROP CONSTRAINT "{TABLE}_2026_07_20_0000_one_operator""#
    ))
    .execute(h.hot.pool())
    .await
    .expect("the constraint is what a deployment turns off");

    h.insert_scoped(
        "11111111115",
        D20,
        2,
        40,
        20_260_720_000_002,
        "9900000000002:2026-07",
    )
    .await;

    // A bare SQL `SUM` is the failure this cannot catch, and it is asserted so
    // the boundary of the guarantee is written down rather than assumed.
    let doubled = h
        .scalar(
            ReadMode::Unified,
            "SELECT CAST(SUM(value) AS BIGINT) FROM readings",
        )
        .await;
    assert_eq!(
        doubled, 100,
        "both scopes survive resolution, so SQL sums them: 2×10 + 2×40"
    );

    // The typed read does not hand back the number. It says what is wrong.
    let store = h.store(ReadMode::Unified).await;
    let err = store
        .series("11111111115")
        .unwrap()
        .range(D20, D21)
        .collect()
        .await
        .expect_err("a fold of two values at one instant must not return a series");

    assert!(
        matches!(err, meterstore::Error::InvariantViolated { .. }),
        "stored data that should not exist, not a delivery being refused: {err:?}"
    );
    let msg = err.to_string();
    assert!(msg.contains("version_scope"), "{msg}");
    drop(store);
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
    h.insert_scoped(
        "11111111115",
        D18,
        2,
        10,
        20_260_718_000_001,
        "9900000000001:2026-07",
    )
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
    let mut other = correction("11111111115", D18, 2, 40, 20_260_718_000_002);
    other.version = meterstore::ScopedVersion::new(
        meterstore::VersionScope::for_interval(
            "9900000000002",
            D18,
            metering::interval::Sparte::Strom,
        )
        .unwrap(),
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
        msg.contains("9900000000001") && msg.contains("9900000000002"),
        "the message must name both operators so the caller can tell which is wrong, \
         and as Marktpartner-IDs rather than as a Debug struct: {msg}"
    );
    assert_eq!(
        after, before,
        "and nothing may have been written before the refusal"
    );
}
