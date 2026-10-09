//! What the instruments an operator reads say, read back from a meter provider.
//!
//! A binary of its own: the crate's instruments bind to the global meter
//! provider at first use, so the in-memory provider is installed once, before
//! anything in the process records, and shared. Each test uses a table of its
//! own, so the readings of one never fold into another's.

#![cfg(feature = "testkit")]

use meterstore::config::TableConfig;
use meterstore::testkit::{MeteringWorkload, TestHarness};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// One in-memory provider for the process, installed before anything records.
fn telemetry() -> &'static (SdkMeterProvider, InMemoryMetricExporter) {
    static TELEMETRY: std::sync::OnceLock<(SdkMeterProvider, InMemoryMetricExporter)> =
        std::sync::OnceLock::new();
    TELEMETRY.get_or_init(|| {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        opentelemetry::global::set_meter_provider(provider.clone());
        (provider, exporter)
    })
}

/// The latest exported value of a `u64` instrument for one table: a gauge's
/// last point, a counter's cumulative sum.
fn value(name: &str, table: &str) -> Option<u64> {
    let (provider, exporter) = telemetry();
    provider.force_flush().expect("flush");
    let exported = exporter.get_finished_metrics().expect("metrics");
    let of_table =
        |kv: &opentelemetry::KeyValue| kv.key.as_str() == "table" && kv.value.as_str() == table;
    exported
        .iter()
        .rev()
        .flat_map(|r| r.scope_metrics())
        .flat_map(|s| s.metrics())
        .filter(|m| m.name() == name)
        .find_map(|m| match m.data() {
            AggregatedMetrics::U64(MetricData::Gauge(g)) => g
                .data_points()
                .filter(|p| p.attributes().any(of_table))
                .map(|p| p.value())
                .last(),
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                let points: Vec<u64> = sum
                    .data_points()
                    .filter(|p| p.attributes().any(of_table))
                    .map(|p| p.value())
                    .collect();
                (!points.is_empty()).then(|| points.iter().sum())
            }
            _ => None,
        })
}

/// [`value`] for the one point carrying `key = val` as well as the table.
fn value_at(name: &str, table: &str, key: &str, val: &str) -> Option<u64> {
    let (provider, exporter) = telemetry();
    provider.force_flush().expect("flush");
    let exported = exporter.get_finished_metrics().expect("metrics");
    let matches = |kv: &opentelemetry::KeyValue| {
        (kv.key.as_str() == "table" && kv.value.as_str() == table)
            || (kv.key.as_str() == key && kv.value.as_str() == val)
    };
    exported
        .iter()
        .rev()
        .flat_map(|r| r.scope_metrics())
        .flat_map(|s| s.metrics())
        .filter(|m| m.name() == name)
        .find_map(|m| match m.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                .data_points()
                .find(|p| p.attributes().filter(|kv| matches(kv)).count() == 2)
                .map(|p| p.value()),
            _ => None,
        })
}

/// A store over `table`, with partitions and a boundary at `START`.
async fn store_for(
    h: &TestHarness,
    config: meterstore::ValidatedTableConfig,
) -> meterstore::MeterStore {
    let table = config.name().to_string();
    let step = config.archival_step();
    let store = h
        .builder_for(config)
        .await
        .expect("builder")
        .build()
        .await
        .expect("store");
    store.admin().create_tables().await.expect("tables");
    store
        .admin()
        .hot_store()
        .ensure_partitions(&table, START, START + Duration::days(4), step)
        .await
        .expect("partitions");
    h.seed_watermark_for(&table, START, Duration::DAY)
        .await
        .expect("boundary, cut on a one-day step");
    store
}

#[tokio::test]
async fn the_lag_gauge_rises_while_archival_fails() {
    telemetry();
    let table = "gauges_failing_versions";
    let h = TestHarness::start().await.expect("harness");
    // A step the table's windows were not cut on: every run refuses.
    let failing = store_for(
        &h,
        TableConfig::new(table)
            .settlement_lag(Duration::DAY)
            .archival_step(Duration::hours(12))
            .build()
            .expect("config"),
    )
    .await;

    failing
        .admin()
        .archive(START + Duration::days(2), 1)
        .await
        .expect_err("the step does not match");
    assert_eq!(
        value("meterstore.tiering.watermark_lag", table),
        Some(Duration::days(2).whole_seconds() as u64),
        "recorded on a failing run"
    );

    failing
        .admin()
        .archive(START + Duration::days(3), 1)
        .await
        .expect_err("still failing");
    assert_eq!(
        value("meterstore.tiering.watermark_lag", table),
        Some(Duration::days(3).whole_seconds() as u64),
        "the lag keeps rising through the outage it exists to show"
    );
    assert!(
        value("meterstore.tiering.hot_partitions_ahead", table).is_some(),
        "the partition gauge is recorded on a failing run too"
    );
    assert_eq!(value("meterstore.archival.failures", table), Some(2));
}

#[tokio::test]
async fn a_healthy_run_reads_as_healthy() {
    // What an operator is told means "fine": one count per committed window,
    // the rows moved, no failure, no deferral, lag of one settlement lag.
    telemetry();
    let table = "gauges_healthy_versions";
    let h = TestHarness::start().await.expect("harness");
    let store = store_for(
        &h,
        TableConfig::new(table)
            .settlement_lag(Duration::DAY)
            .build()
            .expect("config"),
    )
    .await;
    h.ingest(
        &store,
        &MeteringWorkload::new(START)
            .seed(0x39)
            .malo_ids(1)
            .days(2)
            .generate()
            .expect("workload"),
    )
    .await
    .expect("ingest");
    store
        .admin()
        .archive(START + Duration::days(3), 4)
        .await
        .expect("archive");

    assert_eq!(value("meterstore.archival.windows", table), Some(2));
    assert_eq!(value("meterstore.archival.rows", table), Some(192));
    assert_eq!(value("meterstore.archival.failures", table), None);
    assert_eq!(value("meterstore.archival.deferred", table), None);
    assert_eq!(
        value("meterstore.tiering.watermark_lag", table),
        Some(Duration::DAY.whole_seconds() as u64)
    );
}

#[tokio::test]
async fn a_replica_that_loses_the_lease_still_records_lag() {
    // Every replica runs the schedule and all but one lose the lease. If those
    // recorded nothing, a dashboard reading any of them would see the value
    // from the last cycle it happened to win.
    telemetry();
    let table = "gauges_contended_versions";
    let h = TestHarness::start().await.expect("harness");
    let store = store_for(
        &h,
        TableConfig::new(table)
            .settlement_lag(Duration::DAY)
            .build()
            .expect("config"),
    )
    .await;

    let held = store
        .admin()
        .hot_store()
        .try_archive_lease(table)
        .await
        .expect("lease")
        .expect("free");
    let outcome = store
        .admin()
        .archive(START + Duration::days(3), 1)
        .await
        .expect("a contended run is not a failure");
    assert!(outcome.iter().all(|o| o.lease_contended), "{outcome:?}");
    assert_eq!(
        value("meterstore.tiering.watermark_lag", table),
        Some(Duration::days(3).whole_seconds() as u64)
    );
    held.release().await.expect("release");
}

#[tokio::test]
async fn a_plan_short_a_reclaimed_window_is_counted_where_it_happened() {
    use meterstore::tiering::store::{HotStore, PartitionId};
    telemetry();
    let table = "gauges_moved_versions";
    let h = TestHarness::start().await.expect("harness");
    let store = store_for(
        &h,
        TableConfig::new(table)
            .settlement_lag(Duration::DAY)
            .build()
            .expect("config"),
    )
    .await;
    let first = PartitionId::new(table, START);
    h.hot().detach_partition(&first).await.expect("detach");
    h.hot()
        .drop_partition(&first, START + Duration::DAY)
        .await
        .expect("drop");
    assert_eq!(value("meterstore.tiering.boundary_moved", table), None);

    store
        .query(&format!("SELECT count(*) FROM {table}"))
        .await
        .map(|_| ())
        .expect_err("a query short a window must not answer");
    assert!(
        value_at("meterstore.tiering.boundary_moved", table, "where", "plan")
            .is_some_and(|n| n >= 1),
        "counted at planning, where the reclaimed mark is first seen past the boundary"
    );
}
