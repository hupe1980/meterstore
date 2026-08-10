//! Several tables in one session.
//!
//! The shape this exists for is real and specific. An EDM system holds
//! authoritative billing readings *and* a second stream that must never reach a
//! billing query — ESA "Werte nach Typ 2" are explicitly non-authoritative, so
//! keeping them in a separate table is what stops a `SUM` reaching them by
//! omission rather than by policy.
//!
//! Two tables meant two [`MeterStore`] handles, and two handles meant two
//! private DataFusion catalogs: no statement could mention both, and
//! `system.tables` showed whichever one was refreshed last. Neither is a
//! correctness bug; both are the kind of missing ergonomics that pushes an
//! operator into hand-written glue that then *is* a correctness bug.
//!
//! §15.3's consistency model is unchanged and asserted here: each table keeps
//! its own watermark, its own archiver and its own lease. Only the query
//! surface is shared.

#![cfg(feature = "testkit")]

use meterstore::config::TableConfig;
use meterstore::testkit::{MeteringWorkload, TestHarness};
use meterstore::{MeterCatalog, ValidatedTableConfig};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// The non-authoritative second stream, named so nothing mistakes it for
/// billing data.
const SECOND: &str = "esa_typ2_versions";

fn config(name: &str) -> ValidatedTableConfig {
    TableConfig::new(name)
        .settlement_lag(Duration::days(1))
        .build()
        .expect("config")
}

/// A catalog holding the billing table and the ESA table, both populated.
async fn two_tables() -> (TestHarness, MeterCatalog) {
    let harness = TestHarness::start().await.expect("harness");

    let catalog = MeterCatalog::builder()
        .table(
            harness
                .builder_for(config(TestHarness::TABLE))
                .await
                .expect("primary builder"),
        )
        .table(
            harness
                .builder_for(config(SECOND))
                .await
                .expect("secondary builder"),
        )
        .build()
        .await
        .expect("catalog");

    catalog.create_tables().await.expect("create both tables");

    for store in catalog.tables() {
        store
            .hot_store()
            .ensure_partitions(
                store.config().name(),
                START,
                START + Duration::days(3),
                Duration::DAY,
            )
            .await
            .expect("partitions");
        // Without a seeded boundary a table starts at the epoch, and its first
        // archival walks fifty years of empty windows before reaching any data.
        harness
            .seed_watermark_for(store.config().name(), START, Duration::DAY)
            .await
            .expect("watermark");
    }

    // Different populations, so a query that returned the wrong table's rows
    // would produce the wrong count rather than the right one by luck.
    let billing = MeteringWorkload::new(START)
        .seed(0xB111)
        .malo_ids(3)
        .days(2);
    let esa = MeteringWorkload::new(START)
        .seed(0xE5A)
        .malo_offset(500)
        .malo_ids(1)
        .days(2);

    catalog
        .table(TestHarness::TABLE)
        .expect("primary")
        .append(&billing.generate().expect("workload"))
        .await
        .expect("append billing");
    catalog
        .table(SECOND)
        .expect("secondary")
        .append(&esa.generate().expect("workload"))
        .await
        .expect("append esa");

    (harness, catalog)
}

/// The single value a counting query produces.
fn count(result: &meterstore::QueryResult) -> i64 {
    use meterstore::arrow::array::AsArray;
    result.batches()[0]
        .column(0)
        .as_primitive::<meterstore::arrow::datatypes::Int64Type>()
        .value(0)
}

#[tokio::test]
async fn one_statement_can_mention_both_tables() {
    // The ergonomic gap, stated as a test. With two handles this query is
    // unwritable: neither session's catalog knows the other's relation.
    let (_h, catalog) = two_tables().await;

    let joined = catalog
        .query(
            "SELECT COUNT(*) FROM readings r \
             JOIN esa_typ2 e ON r.malo_id = e.malo_id",
        )
        .await
        .expect("a cross-table join must plan and run");

    // The populations are disjoint by construction, so the join is empty — and
    // that is the point: it *planned*, which is what was impossible before.
    assert_eq!(count(&joined), 0);

    // A union over both is the query an operator actually writes, and it must
    // see both tables' rows.
    let both = catalog
        .query(
            "SELECT COUNT(*) FROM (\
               SELECT malo_id FROM readings UNION ALL SELECT malo_id FROM esa_typ2\
             ) AS all_rows",
        )
        .await
        .expect("union");

    let billing_only = catalog
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("billing");
    let esa_only = catalog
        .query("SELECT COUNT(*) FROM esa_typ2")
        .await
        .expect("esa");

    assert_eq!(count(&both), count(&billing_only) + count(&esa_only));
    assert!(count(&esa_only) > 0, "the second table must hold something");
    assert_ne!(
        count(&billing_only),
        count(&esa_only),
        "the two tables must differ, or a mix-up would be invisible"
    );
}

#[tokio::test]
async fn a_result_carries_every_table_boundary_not_one() {
    // P1 across tables. Two tables genuinely have two watermarks (§15.3), so a
    // result that reported one number would be attributing a figure spanning
    // both to a boundary that governs only one of them.
    let (_h, catalog) = two_tables().await;

    // Archive one table and not the other, so the boundaries actually differ —
    // otherwise this passes for the wrong reason.
    catalog
        .table(TestHarness::TABLE)
        .expect("primary")
        .archive(START + Duration::days(2), 1)
        .await
        .expect("archive");

    // A statement that genuinely spans both, so both boundaries govern it.
    let result = catalog
        .query("SELECT COUNT(*) FROM readings UNION ALL SELECT COUNT(*) FROM esa_typ2")
        .await
        .expect("query");

    let watermarks = result.watermarks();
    assert_eq!(watermarks.len(), 2, "one entry per table: {watermarks:?}");

    let primary = watermarks
        .iter()
        .find(|(n, _)| n == TestHarness::TABLE)
        .expect("primary boundary");
    let secondary = watermarks
        .iter()
        .find(|(n, _)| n == SECOND)
        .expect("secondary boundary");
    assert_ne!(
        primary.1, secondary.1,
        "the fixture must leave the two tables at different boundaries"
    );

    // The scalar `watermark()` is the conservative one — below it every table
    // involved is settled — which for these two is the unarchived table's.
    assert_eq!(result.watermark(), secondary.1.min(primary.1));
}

#[tokio::test]
async fn a_result_is_attributed_only_to_the_tables_it_read() {
    // The other half of P1, and the one a naive implementation gets wrong:
    // `watermark()` is the *conservative* boundary — the oldest reported — so
    // attributing every hosted table to every query means a single-table figure
    // is reconciled against whichever unrelated table archives least often.
    // Twenty tables would make the scalar meaningless.
    let (_h, catalog) = two_tables().await;
    catalog
        .table(TestHarness::TABLE)
        .expect("primary")
        .archive(START + Duration::days(2), 1)
        .await
        .expect("archive");

    let one = catalog
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("query");
    assert_eq!(
        one.watermarks().len(),
        1,
        "only the table the statement read: {:?}",
        one.watermarks()
    );
    assert_eq!(one.watermarks()[0].0, TestHarness::TABLE);
    assert_eq!(
        one.watermark(),
        catalog
            .table(TestHarness::TABLE)
            .expect("primary")
            .watermark()
            .await
            .expect("watermark"),
        "and the scalar is that table's own boundary"
    );

    // The raw versioned relation names the same table.
    let raw = catalog
        .query("SELECT COUNT(*) FROM readings_versions")
        .await
        .expect("query");
    assert_eq!(raw.watermarks().len(), 1);

    // A statement reading no managed table has no tier boundary to report, and
    // inventing one would be worse than reporting none.
    let none = catalog.query("SELECT 1").await.expect("query");
    assert!(none.watermarks().is_empty(), "{:?}", none.watermarks());
}

#[tokio::test]
async fn system_tables_show_every_table() {
    // The operational half. One handle per table meant `system.tables` had one
    // row and the others were invisible, which is the worst possible property
    // for the relation an operator opens during an incident.
    let (_h, catalog) = two_tables().await;
    // Archive both first: `system.snapshots` is a list of committed cold states,
    // and a table that has never archived legitimately has none — asserting
    // against an empty relation would prove nothing about attribution.
    catalog
        .archive_all(START + Duration::days(2), 4)
        .await
        .expect("archive");
    catalog
        .refresh_system_tables(START + Duration::days(3))
        .await
        .expect("refresh");

    for relation in ["tables", "config", "resolution", "snapshots"] {
        let result = catalog
            .query(&format!(
                "SELECT DISTINCT \"table\" FROM system.{relation} ORDER BY 1"
            ))
            .await
            .unwrap_or_else(|e| panic!("querying system.{relation}: {e}"));

        use meterstore::arrow::array::AsArray;
        let mut names = Vec::new();
        for batch in result.batches() {
            let column = batch.column(0).as_string::<i32>();
            for i in 0..batch.num_rows() {
                names.push(column.value(i).to_string());
            }
        }
        names.sort();
        assert_eq!(
            names,
            vec![SECOND.to_string(), TestHarness::TABLE.to_string()],
            "system.{relation} must attribute its rows to both tables"
        );
    }
}

#[tokio::test]
async fn each_table_keeps_its_own_watermark_and_archiver() {
    // §15.3 is unchanged by the shared session: archiving one table must not
    // move the other's boundary, because nothing is transactional across them
    // and pretending otherwise would be the distributed transaction this design
    // exists without.
    let (_h, catalog) = two_tables().await;

    let before = catalog
        .table(SECOND)
        .expect("secondary")
        .watermark()
        .await
        .expect("watermark");

    catalog
        .table(TestHarness::TABLE)
        .expect("primary")
        .archive(START + Duration::days(2), 1)
        .await
        .expect("archive");

    let after = catalog
        .table(SECOND)
        .expect("secondary")
        .watermark()
        .await
        .expect("watermark");
    assert_eq!(before, after, "one table's archival is its own");

    let primary = catalog
        .table(TestHarness::TABLE)
        .expect("primary")
        .watermark()
        .await
        .expect("watermark");
    assert!(primary > after, "and the archived one did move");
}

#[tokio::test]
async fn archiving_the_whole_catalog_reports_per_table() {
    let (_h, catalog) = two_tables().await;

    let outcomes = catalog
        .archive_all(START + Duration::days(2), 4)
        .await
        .expect("archive all");

    let names: Vec<&str> = outcomes.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        [SECOND, TestHarness::TABLE],
        "name order, both present"
    );

    for store in catalog.tables() {
        assert!(
            store.watermark().await.expect("watermark").get() > START,
            "{} did not advance",
            store.config().name()
        );
    }

    // And the invariant holds on both, which is the check that would catch an
    // archiver writing one table's rows into another's window.
    assert!(catalog.verify_invariant().await.expect("check").is_empty());
}

#[tokio::test]
async fn a_duplicate_table_name_is_refused() {
    // DataFusion's `register_table` replaces silently, so two tables under one
    // name would leave every query against it reading the second table's rows
    // with nothing to indicate it.
    let harness = TestHarness::start().await.expect("harness");

    let err = MeterCatalog::builder()
        .table(
            harness
                .builder_for(config(TestHarness::TABLE))
                .await
                .expect("first"),
        )
        .table(
            harness
                .builder_for(config(TestHarness::TABLE))
                .await
                .expect("second"),
        )
        .build()
        .await
        .expect_err("a duplicate name must be refused");

    let msg = err.to_string();
    assert!(msg.contains("readings_versions"), "{msg}");
    assert!(
        msg.contains("silently read whichever won"),
        "the message names the consequence: {msg}"
    );
}

#[tokio::test]
async fn an_empty_catalog_is_refused() {
    let err = MeterCatalog::builder()
        .build()
        .await
        .expect_err("an empty catalog answers no query");
    assert!(err.to_string().contains("at least one table"));
}

#[tokio::test]
async fn two_tables_whose_registered_names_collide_are_refused() {
    // `readings` and `readings_versions` are different *physical* names and the
    // same *registered* ones: both raw halves land on `readings_versions` and
    // both resolved halves on `readings` (§13.7.2). Comparing the configured
    // names alone lets the pair through, and the failure then arrives from
    // inside DataFusion naming a relation the caller never wrote down.
    let harness = TestHarness::start().await.expect("harness");

    let err = MeterCatalog::builder()
        .table(
            harness
                .builder_for(config("readings_versions"))
                .await
                .expect("first"),
        )
        .table(
            harness
                .builder_for(config("readings"))
                .await
                .expect("second"),
        )
        .build()
        .await
        .expect_err("colliding registered names must be refused");

    let msg = err.to_string();
    assert!(msg.contains("readings"), "{msg}");
    assert!(
        msg.contains("registers") || msg.contains("register"),
        "the message must be about the registered name, not the configured one: {msg}"
    );
}

#[tokio::test]
async fn a_catalog_with_mixed_read_modes_is_refused() {
    // A result reports the single mode its statement ran under. `Historical`
    // reads no PostgreSQL, so a join between a historical table and a unified
    // one mixes a reproducible half with a mutable one — and the report would be
    // true of whichever store happened to plan it.
    use meterstore::ReadMode;

    let harness = TestHarness::start().await.expect("harness");

    let err = MeterCatalog::builder()
        .table(
            harness
                .builder_for(config(TestHarness::TABLE))
                .await
                .expect("first"),
        )
        .table(
            harness
                .builder_for(config(SECOND))
                .await
                .expect("second")
                .read_mode(ReadMode::Historical),
        )
        .build()
        .await
        .expect_err("mixed read modes must be refused");

    let msg = err.to_string();
    assert!(msg.contains("read mode"), "{msg}");
    assert!(
        msg.contains("mutable tier"),
        "the message names the risk: {msg}"
    );
}

#[tokio::test]
async fn a_cross_table_query_takes_bound_parameters() {
    // §19.7 across tables. Without this the only way to filter a multi-table
    // query by a value from a market message is to build the string by hand.
    let (_h, catalog) = two_tables().await;

    let all = catalog
        .query("SELECT COUNT(*) FROM readings")
        .await
        .expect("unfiltered");

    let filtered = catalog
        .query_with_params(
            "SELECT COUNT(*) FROM readings WHERE malo_id = $1",
            vec![datafusion::scalar::ScalarValue::Utf8(Some(
                "10000000000".to_string(),
            ))],
        )
        .await
        .expect("a bound parameter must reach the engine");

    assert!(count(&filtered) > 0, "the fixture must contain that meter");
    assert!(
        count(&filtered) < count(&all),
        "and the parameter must actually have filtered"
    );
}

#[cfg(feature = "flight")]
#[tokio::test]
async fn a_flight_client_over_a_catalog_store_sees_every_table() {
    // Claimed in §13.8 as an emergent property rather than a feature, so it is
    // worth checking that it emerges. `FlightSqlServer` takes one `MeterStore`;
    // a store built inside a catalog carries the shared session, so pointing a
    // client at any of them should reach all of the tables.
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use futures::TryStreamExt;
    use meterstore::serve::FlightSqlServer;
    use tonic::transport::Channel;

    let (_h, catalog) = two_tables().await;
    let store = catalog.table(TestHarness::TABLE).expect("primary").clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    let (shutdown, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(FlightSqlServer::new(store).into_service())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = rx.await;
                },
            )
            .await
            .expect("flight server");
    });

    let channel = Channel::from_shared(format!("http://{address}"))
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightSqlServiceClient::new(channel);

    let info = client
        .execute(
            "SELECT table_name FROM information_schema.tables ORDER BY 1".to_string(),
            None,
        )
        .await
        .expect("get_flight_info");
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");
    let batches: Vec<_> = client
        .do_get(ticket)
        .await
        .expect("do_get")
        .try_collect()
        .await
        .expect("collect");

    use meterstore::arrow::array::AsArray;
    let mut names = Vec::new();
    for batch in &batches {
        let column = batch.column(0).as_string::<i32>();
        for i in 0..batch.num_rows() {
            names.push(column.value(i).to_string());
        }
    }

    for expected in [
        "readings",
        "readings_versions",
        "esa_typ2",
        "esa_typ2_versions",
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "a Flight client must see {expected}: {names:?}"
        );
    }

    drop(shutdown);
}
