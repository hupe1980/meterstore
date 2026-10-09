//! A schema change the gate admits is a change the store applies.
//!
//! `evolution::compare` classifies; `create_tables` applies an added nullable
//! column to both tiers; the gates refuse what is classified and not applied;
//! and every path that pairs a batch with a schema pairs it by name.

#![cfg(feature = "testkit")]

use datafusion::scalar::ScalarValue;
use meterstore::MeterStore;
use meterstore::arrow::datatypes::{DataType, Field};
use meterstore::config::{TableConfig, ValidatedTableConfig};
use meterstore::testkit::{MeteringWorkload, TestHarness};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

fn config(attributes: &[(&str, bool)]) -> ValidatedTableConfig {
    attributes
        .iter()
        .fold(
            TableConfig::new(TestHarness::TABLE).settlement_lag(Duration::DAY),
            |c, (name, nullable)| c.attribute_column(Field::new(*name, DataType::Utf8, *nullable)),
        )
        .build()
        .expect("config")
}

/// One day of one measuring point, each series carrying `extra`.
fn day(start: OffsetDateTime, extra: &[(&str, &str)]) -> Vec<meterstore::encode::StoredSeries> {
    MeteringWorkload::new(start)
        .seed(0x25)
        .malo_ids(1)
        .days(1)
        .generate()
        .expect("workload")
        .into_iter()
        .map(|s| {
            extra.iter().fold(s, |s, (name, value)| {
                s.with_extra(*name, ScalarValue::Utf8(Some((*value).to_string())))
            })
        })
        .collect()
}

async fn count(store: &MeterStore, sql: &str) -> i64 {
    use meterstore::arrow::array::AsArray;
    let result = store.query(sql).await.expect("query");
    result.batches()[0]
        .column(0)
        .as_primitive::<meterstore::arrow::datatypes::Int64Type>()
        .value(0)
}

async fn harness(config: ValidatedTableConfig) -> TestHarness {
    let h = TestHarness::with_config(config).await.expect("harness");
    h.ensure_partitions(START, START + Duration::days(4))
        .await
        .expect("partitions");
    h.seed_watermark(START).await.expect("watermark");
    h
}

#[tokio::test]
async fn a_nullable_column_added_later_archives() {
    let h = harness(config(&[])).await;
    let before = h.store().await.expect("store");
    h.ingest(&before, &day(START, &[])).await.expect("day one");

    let declared = config(&[("bilanzkreis", true)]);
    let after = h
        .builder_for(declared.clone())
        .await
        .expect("builder")
        .build()
        .await
        .expect("a pending column builds");

    // Declared and not yet applied: refused, naming the remedy — not a
    // retryable storage failure a supervisor would loop on.
    let err = after
        .append(&day(START + Duration::DAY, &[("bilanzkreis", "BK-1")]))
        .await
        .expect_err("the hot table has no such column yet");
    assert!(
        matches!(err, meterstore::Error::Config(_)) && err.to_string().contains("create_tables"),
        "{err:?}"
    );

    let err = after
        .hot_writer()
        .await
        .expect("writer")
        .append(&day(START + Duration::DAY, &[("bilanzkreis", "BK-1")]))
        .await
        .expect_err("the bulk writer meets the same missing column");
    assert!(
        matches!(err, meterstore::Error::Config(_)) && err.to_string().contains("create_tables"),
        "{err:?}"
    );

    after.admin().create_tables().await.expect("apply");
    after
        .append(&day(START + Duration::DAY, &[("bilanzkreis", "BK-1")]))
        .await
        .expect("day two, carrying the column");
    after
        .admin()
        .archive(START + Duration::days(3), 4)
        .await
        .expect("archive");
    assert_eq!(
        after.watermark().await.expect("watermark").get(),
        START + Duration::days(2),
        "both days archived"
    );

    let fresh = h
        .builder_for(declared)
        .await
        .unwrap()
        .build()
        .await
        .unwrap();
    assert_eq!(
        count(
            &fresh,
            "SELECT COUNT(*) FROM readings WHERE bilanzkreis IS NULL"
        )
        .await,
        96,
        "rows written before the column read null"
    );
    assert_eq!(
        count(
            &fresh,
            "SELECT COUNT(*) FROM readings WHERE bilanzkreis = 'BK-1'"
        )
        .await,
        96
    );
}

#[tokio::test]
async fn a_column_no_longer_declared_archives_with_its_values() {
    // Withdrawn from the declaration, kept by both tiers. Rows written while it
    // was declared still carry values, and archival must not null them.
    let h = harness(config(&[("bilanzkreis", true)])).await;
    let declaring = h.store().await.expect("store");
    h.ingest(&declaring, &day(START, &[("bilanzkreis", "BK-1")]))
        .await
        .expect("day one");

    let withdrawn = h
        .builder_for(config(&[]))
        .await
        .unwrap()
        .build()
        .await
        .expect("a withdrawn nullable column is safe");
    h.ingest(&withdrawn, &day(START + Duration::DAY, &[]))
        .await
        .expect("day two");
    withdrawn
        .admin()
        .archive(START + Duration::days(3), 4)
        .await
        .expect("archive");
    assert_eq!(
        withdrawn.watermark().await.unwrap().get(),
        START + Duration::days(2)
    );

    // A late correction under the withdrawn declaration carries no value for
    // the column, and the cold table still has it: written as null.
    let correction: Vec<_> = MeteringWorkload::new(START)
        .seed(0x25)
        .malo_ids(1)
        .days(1)
        .version(20_260_726_000_002)
        .generate()
        .expect("workload");
    let outcome = withdrawn
        .append(&correction)
        .await
        .expect("late correction");
    assert_eq!(outcome.cold_rows, 96, "it went to Iceberg");

    let reader = h
        .builder_for(config(&[("bilanzkreis", true)]))
        .await
        .unwrap()
        .build()
        .await
        .unwrap();
    assert_eq!(
        count(
            &reader,
            "SELECT COUNT(*) FROM readings_versions WHERE bilanzkreis = 'BK-1'"
        )
        .await,
        96,
        "the values written while the column was declared survive archival"
    );
    // The correction supersedes day one, and carries no value for the column.
    assert_eq!(
        count(
            &reader,
            "SELECT COUNT(*) FROM readings_versions WHERE bilanzkreis IS NULL"
        )
        .await,
        192,
        "day two and the correction, written as null"
    );
}

#[tokio::test]
async fn a_permuted_declaration_is_matched_by_name_on_the_hot_path() {
    let h = harness(config(&[("bilanzkreis", true), ("netzgebiet", true)])).await;
    let permuted = h
        .builder_for(config(&[("netzgebiet", true), ("bilanzkreis", true)]))
        .await
        .unwrap()
        .build()
        .await
        .expect("a permutation compares identical");
    h.ingest(
        &permuted,
        &day(
            START + Duration::DAY,
            &[("bilanzkreis", "BK-1"), ("netzgebiet", "NG-9")],
        ),
    )
    .await
    .expect("ingest");

    assert_eq!(
        count(
            &permuted,
            "SELECT COUNT(*) FROM readings WHERE bilanzkreis = 'BK-1' AND netzgebiet = 'NG-9'"
        )
        .await,
        96,
        "each value under its own name"
    );
}

#[tokio::test]
async fn a_widened_nullability_quarantines() {
    let h = harness(config(&[("bilanzkreis", false)])).await;
    let err = h
        .builder_for(config(&[("bilanzkreis", true)]))
        .await
        .unwrap()
        .build()
        .await
        .expect_err("the Iceberg field stays required");
    assert!(
        matches!(err, meterstore::Error::Quarantined { .. })
            && err.to_string().contains("DROP NOT NULL"),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_crate_created_table_is_identical_to_its_configuration() {
    let h = harness(config(&[("bilanzkreis", true)])).await;
    let compatibility = h
        .store()
        .await
        .unwrap()
        .admin()
        .check_schema()
        .await
        .unwrap()
        .expect("a cold table");
    assert!(compatibility.is_identical(), "{:?}", compatibility.changes);
}

#[tokio::test]
async fn a_snapshot_from_before_a_column_was_added_still_reads() {
    // A settlement rerun pins a snapshot. Adding an attribute afterwards must not
    // make it unreadable: the column it predates reads as null.
    let h = harness(config(&[])).await;
    let before = h.store().await.unwrap();
    h.ingest(&before, &day(START, &[])).await.unwrap();
    before
        .admin()
        .archive(START + Duration::days(2), 4)
        .await
        .unwrap();
    let pinned = before.admin().snapshots().await.unwrap()[0].snapshot_id;

    let declared = config(&[("bilanzkreis", true)]);
    let after = h
        .builder_for(declared.clone())
        .await
        .unwrap()
        .build()
        .await
        .unwrap();
    after.admin().create_tables().await.unwrap();

    let fresh = h
        .builder_for(declared)
        .await
        .unwrap()
        .build()
        .await
        .unwrap();
    let rerun = fresh
        .as_of(meterstore::planner::SnapshotSelector::Id(pinned), None)
        .await
        .expect("pin");
    assert_eq!(
        count(
            &rerun,
            "SELECT COUNT(*) FROM readings WHERE bilanzkreis IS NULL"
        )
        .await,
        96
    );
}

#[tokio::test]
async fn a_store_over_a_table_missing_a_declared_column_says_how_to_create_it() {
    use meterstore::tiering::store::{ColdStore, HotStore};
    let h = harness(config(&[])).await;
    // The provider is opened over the table as it is: without the column.
    let provider = h.cold().table_provider(TestHarness::TABLE).await.unwrap();
    let err = MeterStore::builder()
        .hot(std::sync::Arc::clone(h.hot()) as std::sync::Arc<dyn HotStore>)
        .cold(
            std::sync::Arc::clone(h.cold()) as std::sync::Arc<dyn ColdStore>,
            provider,
        )
        .table(config(&[("bilanzkreis", true)]))
        .build()
        .await
        .expect_err("the session would name a column the provider lacks");
    assert!(
        matches!(err, meterstore::Error::Config(_)) && err.to_string().contains("create_tables"),
        "{err:?}"
    );
}
