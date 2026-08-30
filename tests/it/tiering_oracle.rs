//! The tiering oracle (§17.3), against real PostgreSQL and a real Iceberg
//! warehouse.
//!
//! > For any archival history and any query range, a query over the unified view
//! > must equal the same query against a single reference table holding every row
//! > ever written, with latest-version-wins applied.
//!
//! Every other suite asserts a specific behaviour with a hand-built fixture.
//! This one asserts the *property*, against a generated workload, and it is the
//! test that would catch a defect nobody thought to write a case for — the
//! chunk boundary landing inside a tie, the provider frozen at construction, the
//! extra column dropped on the way to Parquet. All three of those were real, and
//! all three are invisible to a fixture that happens to be small.
//!
//! The reference is a deliberately different implementation: a fold over a map
//! in Rust, rather than the window function the store plans. An oracle sharing
//! the implementation under test agrees with it about its mistakes.

#![cfg(feature = "testkit")]

use meterstore::planner::ReadMode;
use meterstore::testkit::{MeteringWorkload, Oracle, TestHarness};
use rust_decimal::Decimal;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

/// The instant the workloads start from.
const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// Run a workload through a store, archiving `archive_days` of it.
///
/// Returns the store and the reference to compare it against.
async fn run(
    workload: MeteringWorkload,
    archive_days: i64,
) -> (TestHarness, meterstore::MeterStore, Oracle) {
    let harness = TestHarness::start().await.expect("harness");
    let (from, to) = workload.range();

    harness
        .ensure_partitions(from, to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");

    let store = harness.store().await.expect("store");
    let series = workload.generate().expect("workload");

    let mut oracle = Oracle::new();
    oracle.record(&series).expect("oracle");
    harness.ingest(&store, &series).await.expect("ingest");

    if archive_days > 0 {
        // The horizon is `now - settlement_lag`, and the harness uses a one-day
        // lag, so this archives exactly `archive_days` windows.
        let now = from + Duration::days(archive_days + 1);
        store.archive(now, 64).await.expect("archive");
        assert_eq!(
            store.watermark().await.unwrap().get(),
            from + Duration::days(archive_days),
            "the boundary must land where the test expects, or it proves nothing"
        );
    }

    (harness, store, oracle)
}

/// The single `BIGINT` a counting query produces.
async fn scalar(store: &meterstore::MeterStore, sql: &str) -> i64 {
    use datafusion::arrow::array::AsArray;
    let result = store.query(sql).await.expect("query");
    result.batches()[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0)
}

/// The store's total, scaled to compare against the oracle's `Decimal`.
async fn total_kwh(
    store: &meterstore::MeterStore,
    from: OffsetDateTime,
    to: OffsetDateTime,
) -> Decimal {
    use datafusion::arrow::array::AsArray;
    let result = store
        .query(&format!(
            r#"SELECT COALESCE(SUM(value), 0) FROM readings
               WHERE "from" >= TIMESTAMP '{}' AND "from" < TIMESTAMP '{}'"#,
            from.format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
            to.format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        ))
        .await
        .expect("query");

    let array = result.batches()[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Decimal128Type>();
    Decimal::try_from_i128_with_scale(array.value(0), u32::from(array.scale() as u8))
        .expect("decimal in range")
        .normalize()
}

#[tokio::test]
async fn a_clean_workload_matches_the_reference_across_the_boundary() {
    // The base case: no corrections, no gaps, half archived. Every row must be
    // returned exactly once whichever tier holds it.
    let workload = MeteringWorkload::new(START)
        .seed(0x5EED)
        .malo_ids(5)
        .days(4);
    let (from, to) = workload.range();
    let (_h, store, oracle) = run(workload, 2).await;

    assert_eq!(
        scalar(&store, "SELECT COUNT(*) FROM readings").await as u64,
        oracle.row_count(from, to)
    );
    assert_eq!(
        total_kwh(&store, from, to).await,
        oracle.sum_kwh(from, to).normalize()
    );
}

#[tokio::test]
async fn corrections_resolve_to_the_same_values_the_reference_holds() {
    // A corrected interval must be returned once, at its latest version. Read
    // from the raw table it would appear twice, and the sum would be inflated by
    // exactly the corrections — the failure §13.7.2 exists to prevent.
    let workload = MeteringWorkload::new(START)
        .seed(0xC0DE)
        .malo_ids(4)
        .days(4)
        .with_corrections(0.15);
    let (from, to) = workload.range();
    let (_h, store, oracle) = run(workload, 2).await;

    assert_eq!(
        scalar(&store, "SELECT COUNT(*) FROM readings").await as u64,
        oracle.row_count(from, to),
        "one row per interval, whatever its version"
    );
    assert_eq!(
        total_kwh(&store, from, to).await,
        oracle.sum_kwh(from, to).normalize()
    );

    // And the raw table really does hold more, so the resolution above was doing
    // work rather than being trivially satisfied.
    let raw = scalar(&store, "SELECT COUNT(*) FROM readings_versions").await as u64;
    assert!(
        raw > oracle.row_count(from, to),
        "the workload must actually produce corrections"
    );
}

#[tokio::test]
async fn gaps_survive_the_round_trip_without_being_invented_or_lost() {
    let workload = MeteringWorkload::new(START)
        .seed(0xBEEF)
        .malo_ids(4)
        .days(3)
        .with_gaps(0.2);
    let (from, to) = workload.range();
    let (_h, store, oracle) = run(workload, 1).await;

    assert_eq!(
        scalar(&store, "SELECT COUNT(*) FROM readings").await as u64,
        oracle.row_count(from, to)
    );
}

#[tokio::test]
async fn every_measuring_point_totals_correctly_on_its_own() {
    // A whole-table sum can be right while individual meters are wrong — two
    // errors that cancel. Per-meter totals catch that.
    let workload = MeteringWorkload::new(START)
        .seed(0x1234)
        .malo_ids(6)
        .days(3)
        .with_corrections(0.1);
    let (from, to) = workload.range();
    let (_h, store, oracle) = run(workload, 2).await;

    for malo in oracle.malo_ids() {
        let result = store
            .query_with_params(
                r#"SELECT COALESCE(SUM(value), 0) FROM readings WHERE malo_id = $1"#,
                vec![datafusion::scalar::ScalarValue::Utf8(Some(malo.clone()))],
            )
            .await
            .expect("query");

        use datafusion::arrow::array::AsArray;
        let array = result.batches()[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Decimal128Type>();
        let got = Decimal::try_from_i128_with_scale(array.value(0), u32::from(array.scale() as u8))
            .expect("decimal")
            .normalize();

        assert_eq!(
            got,
            oracle.sum_kwh_for(&malo, from, to).normalize(),
            "meter {malo} disagrees with the reference"
        );
    }
}

#[tokio::test]
async fn a_workload_spanning_the_autumn_dst_day_round_trips() {
    // The 100-interval day. `to` is stored rather than derived, so the irregular
    // boundaries have to survive encoding, archival and the read back.
    let workload = MeteringWorkload::new(START)
        .seed(0xDA7E)
        .malo_ids(3)
        .days(3)
        .spanning_autumn_back();
    let (from, to) = workload.range();
    let (_h, store, oracle) = run(workload, 1).await;

    assert_eq!(
        scalar(&store, "SELECT COUNT(*) FROM readings").await as u64,
        oracle.row_count(from, to)
    );
    assert_eq!(
        total_kwh(&store, from, to).await,
        oracle.sum_kwh(from, to).normalize()
    );
}

#[tokio::test]
async fn a_workload_spanning_the_spring_dst_day_round_trips() {
    // The 92-interval day, where an implementation assuming 96 invents four
    // intervals that were never delivered.
    let workload = MeteringWorkload::new(START)
        .seed(0xDA7F)
        .malo_ids(3)
        .days(3)
        .spanning_spring_forward();
    let (from, to) = workload.range();
    let (_h, store, oracle) = run(workload, 1).await;

    assert_eq!(
        scalar(&store, "SELECT COUNT(*) FROM readings").await as u64,
        oracle.row_count(from, to)
    );
}

#[tokio::test]
async fn every_partial_range_matches_the_reference() {
    // "For **any** query range." A range cutting inside an archived file, inside
    // the hot window, and across the boundary all have to agree — the third is
    // where a tier split that is not exhaustive-and-disjoint shows up.
    let workload = MeteringWorkload::new(START).seed(0xA11).malo_ids(4).days(4);
    let (from, _) = workload.range();
    let (_h, store, oracle) = run(workload, 2).await;

    let boundary = from + Duration::days(2);
    let ranges = [
        (from, from + Duration::hours(6)), // inside cold
        (from, boundary),                  // exactly cold
        (boundary - Duration::hours(6), boundary + Duration::hours(6)), // across
        (boundary, boundary + Duration::days(2)), // exactly hot
        (boundary + Duration::hours(3), boundary + Duration::hours(9)), // inside hot
        (from + Duration::hours(18), boundary + Duration::days(1)), // wide
    ];

    for (lo, hi) in ranges {
        let rfc = |t: OffsetDateTime| {
            t.format(&time::format_description::well_known::Rfc3339)
                .unwrap()
        };
        let got = scalar(
            &store,
            &format!(
                r#"SELECT COUNT(*) FROM readings
                   WHERE "from" >= TIMESTAMP '{}' AND "from" < TIMESTAMP '{}'"#,
                rfc(lo),
                rfc(hi)
            ),
        )
        .await as u64;

        assert_eq!(
            got,
            oracle.row_count(lo, hi),
            "range [{}, {}) disagrees with the reference",
            rfc(lo),
            rfc(hi)
        );
    }
}

#[tokio::test]
async fn archiving_window_by_window_never_changes_the_answer() {
    // Archival must be invisible to a reader. The same query, run after every
    // window moves, has to keep returning the same number — which is the
    // property the frozen cold provider broke, silently.
    let workload = MeteringWorkload::new(START)
        .seed(0x5A1E)
        .malo_ids(4)
        .days(5);
    let (from, to) = workload.range();

    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(from, to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");

    let store = harness.store().await.expect("store");
    let series = workload.generate().expect("workload");
    let mut oracle = Oracle::new();
    oracle.record(&series).expect("oracle");
    harness.ingest(&store, &series).await.expect("ingest");

    let expected = oracle.row_count(from, to);
    assert_eq!(
        scalar(&store, "SELECT COUNT(*) FROM readings").await as u64,
        expected,
        "before any archival"
    );

    for day in 1..=4 {
        store
            .archive(from + Duration::days(day + 1), 1)
            .await
            .expect("archive");
        assert_eq!(
            scalar(&store, "SELECT COUNT(*) FROM readings").await as u64,
            expected,
            "after archiving day {day}"
        );
        store.verify_invariant().await.expect("invariant");
    }
}

#[tokio::test]
async fn a_historical_read_matches_the_reference_over_the_archived_range() {
    // Reporting reads take no dependency on PostgreSQL, so they must still be
    // complete over the range the cold tier owns.
    let workload = MeteringWorkload::new(START).seed(0x77).malo_ids(4).days(4);
    let (from, _) = workload.range();
    let (harness, store, oracle) = run(workload, 2).await;

    let boundary = from + Duration::days(2);
    let historical = meterstore::MeterStore::builder()
        .hot(harness.hot().clone() as std::sync::Arc<dyn meterstore::HotStore>)
        .cold(
            harness.cold().clone() as std::sync::Arc<dyn meterstore::ColdStore>,
            harness
                .cold()
                .table_provider(harness.config().name())
                .await
                .unwrap(),
        )
        .table(harness.config().clone())
        .read_mode(ReadMode::Historical)
        .build()
        .await
        .expect("historical store");

    let result = historical
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("query");
    assert!(!result.touched_hot_tier());

    use datafusion::arrow::array::AsArray;
    let got = result.batches()[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0) as u64;
    assert_eq!(got, oracle.row_count(from, boundary));
    drop(store);
}

#[tokio::test]
async fn completeness_agrees_with_the_gaps_the_workload_actually_left() {
    // Completeness is the store's own claim about what is missing. If it does
    // not match what the generator withheld, one of the two is wrong.
    let workload = MeteringWorkload::new(START)
        .seed(0x9E7)
        .malo_ids(3)
        .days(2)
        .with_gaps(0.1);
    let (from, to) = workload.range();
    let (_h, store, oracle) = run(workload, 1).await;

    let rows = store.completeness(from, to).await.expect("completeness");
    let reported: u64 = rows.iter().map(|r| r.actual).sum();
    assert_eq!(
        reported,
        oracle.row_count(from, to),
        "completeness must count the same rows the query returns"
    );
    assert!(
        rows.iter().any(|r| r.missing > 0),
        "the workload withheld intervals, so some must be reported missing"
    );
}

#[tokio::test]
async fn a_channel_that_stops_delivering_entirely_is_only_visible_against_a_roster() {
    // The finding a range cannot make about itself. Completeness is an aggregate
    // over the rows a range holds, so a measuring point that delivered *nothing*
    // produces no group and appears nowhere — the most severe incompleteness
    // there is, and the one the report was silent about. Nothing inside the
    // range can supply the missing channel, because the missing channel is
    // exactly what the range does not contain.
    //
    // Two meters report the first day; only one reports the second.
    let both = MeteringWorkload::new(START)
        .seed(0x5115)
        .malo_ids(2)
        .days(1);
    let (from, boundary) = both.range();

    let survivor = MeteringWorkload::new(boundary)
        .seed(0x5116)
        .malo_ids(1)
        .days(1);
    let (_, to) = survivor.range();

    let harness = TestHarness::start().await.expect("harness");
    harness
        .ensure_partitions(from, to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");
    let store = harness.store().await.expect("store");

    harness
        .ingest(&store, &both.generate().expect("first day"))
        .await
        .expect("ingest");
    harness
        .ingest(&store, &survivor.generate().expect("second day"))
        .await
        .expect("ingest");

    // Without a roster the second day looks perfect: the one meter that reported
    // is complete, and the one that vanished is not mentioned at all.
    let blind = store
        .completeness(boundary, to)
        .await
        .expect("completeness");
    assert_eq!(blind.len(), 1, "one channel reported: {blind:?}");
    assert!(blind.iter().all(|r| r.is_complete()));

    // With the first day as the reference window, the silence is a finding.
    let seeing = store
        .completeness(boundary, to)
        .seen_since(from)
        .await
        .expect("completeness");
    assert_eq!(
        seeing.len(),
        2,
        "both channels are accounted for: {seeing:?}"
    );

    let silent: Vec<_> = seeing.iter().filter(|r| r.is_silent()).collect();
    assert_eq!(silent.len(), 1, "exactly one went quiet: {seeing:?}");
    let gone = silent[0];
    assert_eq!(gone.actual, 0);
    assert_eq!(gone.missing, gone.expected);
    assert!(gone.expected > 0, "a full day is expected of it");
    assert!(!gone.is_complete());
    assert!(
        gone.first_gap.is_some(),
        "and an operator is told from when"
    );

    // The meter that kept reporting is untouched by the roster.
    let still_reporting: Vec<_> = seeing.iter().filter(|r| !r.is_silent()).collect();
    assert_eq!(still_reporting.len(), 1);
    assert!(still_reporting[0].is_complete());
    assert_ne!(still_reporting[0].malo_id, gone.malo_id);

    // And the same through SQL, where the reference window is the leading
    // argument because the arguments read in time order.
    let rows = store
        .query(&format!(
            "SELECT malo_id FROM meter_completeness('{from}', '{boundary}', '{to}') \
             WHERE actual = 0",
            from = from
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
            boundary = boundary
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
            to = to
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
        ))
        .await
        .expect("meter_completeness");
    assert_eq!(
        rows.batches().iter().map(|b| b.num_rows()).sum::<usize>(),
        1,
        "the table function reports the silent channel too"
    );
    drop(store);
}

/// Run a **Zählerstandsgang** workload through a point store, archiving
/// `archive_days` of it.
///
/// A second setup rather than a parameter on [`run`]: a point table is declared
/// `TimeModel::Point`, identifies a reading by its Messlokation, and is written
/// through a different entry point. Folding the two into one function would hide
/// exactly the differences the property is being asserted across.
async fn run_readings(
    workload: MeteringWorkload,
    archive_days: i64,
) -> (TestHarness, meterstore::MeterStore, Oracle) {
    use meterstore::config::{TableConfig, TimeModel};

    let config = TableConfig::new(TestHarness::TABLE)
        .time_model(TimeModel::Point)
        .settlement_lag(Duration::DAY)
        .build()
        .expect("point table configuration");

    let harness = TestHarness::with_config(config).await.expect("harness");
    let (from, to) = workload.range();

    harness
        .ensure_partitions(from, to + Duration::days(1))
        .await
        .expect("partitions");
    harness.seed_watermark(from).await.expect("watermark");

    let store = harness.store().await.expect("store");
    let deliveries = workload.generate_readings().expect("workload");

    // Keyed on the table's real merge key, which for a point table includes
    // `melo_id` — the reference has to mean by "the same reading" what the store
    // means, or it accuses a store that is behaving correctly.
    let mut oracle = Oracle::for_table(harness.config());
    oracle.record_readings(&deliveries).expect("oracle");
    harness
        .ingest_readings(&store, &deliveries)
        .await
        .expect("ingest");

    if archive_days > 0 {
        let now = from + Duration::days(archive_days + 1);
        store.archive(now, 64).await.expect("archive");
        assert_eq!(
            store.watermark().await.unwrap().get(),
            from + Duration::days(archive_days),
            "the boundary must land where the test expects, or it proves nothing"
        );
    }

    (harness, store, oracle)
}

#[tokio::test]
async fn a_zaehlerstandsgang_matches_the_reference_across_the_boundary() {
    // §17.3's property over the *other* record type.
    //
    // Two Messlokationen per Marktlokation, because that is the shape a merge key
    // without `melo_id` folds into one reading: the meters agree on the channel
    // and on the instants, so a store keyed on the Marktlokation alone silently
    // stores half of them.
    let workload = MeteringWorkload::new(START)
        .seed(0x2A17)
        .malo_ids(4)
        .days(4)
        .messlokationen(2);
    let (from, to) = workload.range();
    let (_h, store, oracle) = run_readings(workload, 2).await;

    assert_eq!(
        scalar(&store, "SELECT COUNT(*) FROM readings").await as u64,
        oracle.row_count(from, to),
        "every register reading, from whichever tier holds it"
    );
    assert_eq!(
        total_kwh(&store, from, to).await,
        oracle.sum_kwh(from, to).normalize(),
        "and the values, which for a register are cumulative rather than energy"
    );
}

#[tokio::test]
async fn corrected_register_readings_resolve_like_corrected_intervals() {
    // Resolution does not know the difference between a span and an instant, so
    // the property has to hold identically. Read from the raw table a corrected
    // reading appears twice, and summing registers is meaningless anyway — which
    // is why the two shapes are never one table.
    let workload = MeteringWorkload::new(START)
        .seed(0x2A18)
        .malo_ids(3)
        .days(4)
        .messlokationen(2)
        .with_corrections(0.15)
        .with_gaps(0.05);
    let (from, to) = workload.range();
    let (_h, store, oracle) = run_readings(workload, 2).await;

    assert_eq!(
        scalar(&store, "SELECT COUNT(*) FROM readings").await as u64,
        oracle.row_count(from, to),
    );
    assert_eq!(
        total_kwh(&store, from, to).await,
        oracle.sum_kwh(from, to).normalize(),
    );

    // The audit trail keeps both versions; the resolved view returns one.
    let raw = scalar(&store, "SELECT COUNT(*) FROM readings_versions").await as u64;
    assert!(
        raw > oracle.row_count(from, to),
        "corrections must be present in the raw table: {raw}"
    );
}

#[tokio::test]
async fn a_replayed_zaehlerstandsgang_changes_nothing_in_either_tier() {
    // Every transport worth using delivers at least once, and a point table's
    // late corrections reach Iceberg — which has no constraints — through the
    // same reconciliation an interval table's do. The reference is unchanged by
    // a replay, so the store must be too.
    let workload = MeteringWorkload::new(START)
        .seed(0x2A19)
        .malo_ids(2)
        .days(3)
        .messlokationen(2);
    let (from, to) = workload.range();
    let deliveries = workload.generate_readings().expect("workload");
    let (harness, store, oracle) = run_readings(workload, 2).await;

    // Replayed *after* archival, so half of them land in the cold tier.
    harness
        .ingest_readings(&store, &deliveries)
        .await
        .expect("replay");

    assert_eq!(
        scalar(&store, "SELECT COUNT(*) FROM readings").await as u64,
        oracle.row_count(from, to),
        "a redelivery must not add a reading"
    );
    assert_eq!(
        total_kwh(&store, from, to).await,
        oracle.sum_kwh(from, to).normalize(),
        "nor change a value"
    );
}
