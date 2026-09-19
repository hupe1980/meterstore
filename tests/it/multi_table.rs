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
//! The consistency model is unchanged and asserted here: each table keeps
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
            .admin()
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
async fn an_isolated_session_cannot_name_the_other_table() {
    // Sharing one `SessionContext` is what makes the join above expressible, and
    // it means any registered relation is reachable by naming it in
    // caller-supplied SQL. Where a deployment keeps a stream out of a path — ESA
    // "Werte nach Typ 2" must never reach a billing query — a deny-list of
    // relation names holds until someone adds a table.
    let (_h, catalog) = two_tables().await;

    // In the shared session both are reachable.
    assert!(catalog.query("SELECT COUNT(*) FROM esa_typ2").await.is_ok());

    let billing = catalog.isolated("readings").await.expect("isolated");

    // Its own relations still work, under both names.
    assert!(billing.query("SELECT COUNT(*) FROM readings").await.is_ok());
    assert!(
        billing
            .query("SELECT COUNT(*) FROM readings_versions")
            .await
            .is_ok()
    );

    // The other table is not in this session at all, so the statement fails to
    // *plan* rather than being caught by the caller's vigilance.
    for sql in [
        "SELECT COUNT(*) FROM esa_typ2",
        "SELECT COUNT(*) FROM esa_typ2_versions",
        "SELECT COUNT(*) FROM readings UNION ALL SELECT COUNT(*) FROM esa_typ2",
        "SELECT (SELECT COUNT(*) FROM esa_typ2)",
    ] {
        assert!(
            billing.query(sql).await.is_err(),
            "an isolated session must not reach the other table: {sql}"
        );
    }

    // And the catalog itself is unaffected — isolation builds a query surface,
    // not a second store.
    assert!(catalog.query("SELECT COUNT(*) FROM esa_typ2").await.is_ok());
    assert_eq!(catalog.len(), 2);
}

#[tokio::test]
async fn isolating_an_unknown_table_names_what_is_there() {
    let (_h, catalog) = two_tables().await;
    let err = catalog
        .isolated("no_such_table")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no_such_table"), "{err}");
    assert!(
        err.contains("readings"),
        "the message lists what is held: {err}"
    );
}

#[tokio::test]
async fn a_result_carries_every_table_boundary_not_one() {
    // P1 across tables. Two tables genuinely have two watermarks, so a
    // result that reported one number would be attributing a figure spanning
    // both to a boundary that governs only one of them.
    let (_h, catalog) = two_tables().await;

    // Archive one table and not the other, so the boundaries actually differ —
    // otherwise this passes for the wrong reason.
    catalog
        .table(TestHarness::TABLE)
        .expect("primary")
        .admin()
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
        .admin()
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

    // **A table named only inside a subquery is still a table the statement
    // read.** Attribution walks the logical plan, and a scalar subquery's plan
    // hangs off an *expression* rather than off `inputs()` — a statement
    // attributed to no table reports the epoch, that nothing has been settled.
    let nested = catalog
        .query(
            "SELECT (SELECT COUNT(*) FROM readings) AS billing, \
                    (SELECT COUNT(*) FROM esa_typ2) AS second",
        )
        .await
        .expect("query");
    let named: Vec<&str> = nested
        .watermarks()
        .iter()
        .map(|(t, _)| t.as_str())
        .collect();
    assert_eq!(named.len(), 2, "both subqueries name a table: {named:?}");
    assert!(nested.watermark() > meterstore::TieringWatermark::empty());

    // And a table reached through a CTE, which is the other shape a plain plan
    // walk sees differently from the SQL an operator wrote.
    let cte = catalog
        .query("WITH r AS (SELECT * FROM readings) SELECT COUNT(*) FROM r")
        .await
        .expect("query");
    assert_eq!(cte.watermarks().len(), 1, "{:?}", cte.watermarks());
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
    // The model is unchanged by the shared session: archiving one table must not
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
        .admin()
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
    // both resolved halves on `readings`. Comparing the configured
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
    // The serving posture across tables. Without this the only way to filter a
    // multi-table
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
                "10000000009".to_string(),
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
    // An emergent property rather than a feature, so it is
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

#[tokio::test]
async fn one_maintenance_loop_covers_every_table_and_names_them_apart() {
    // The operational half of what a catalogue is for. Each table keeps its own
    // watermark, archiver and lease; only the scheduling is shared, which
    // is what gives a deployment one place to ask whether upkeep is keeping up.
    let (_h, catalog) = two_tables().await;

    let maintenance = catalog.maintenance();
    let mut covered = maintenance.tables();
    covered.sort_unstable();
    assert_eq!(covered, vec![SECOND, TestHarness::TABLE]);

    // One cycle, both tables archived, each reported by name.
    let outcome = maintenance
        .run_once(START + Duration::days(2))
        .await
        .expect("cycle");

    assert_eq!(outcome.tables.len(), 2);
    let mut named: Vec<&str> = outcome.tables.iter().map(|t| t.table.as_str()).collect();
    named.sort_unstable();
    assert_eq!(named, vec![SECOND, TestHarness::TABLE]);

    assert!(outcome.rows_archived() > 0, "a window must have moved");
    assert!(outcome.healthy(), "{:?}", outcome.tables);
    assert_eq!(outcome.unhealthy().count(), 0);

    // And both tables really did advance, rather than one being visited twice.
    for store in catalog.tables() {
        assert!(
            store.watermark().await.expect("watermark").get() > START,
            "{} did not advance",
            store.config().name()
        );
    }
}

/// A table keyed by a `tenant` identity column, so a catalog of two of them can
/// be confined to one tenant as a whole.
fn tenanted(name: &str) -> ValidatedTableConfig {
    use datafusion::arrow::datatypes::{DataType, Field};
    TableConfig::new(name)
        .settlement_lag(Duration::days(1))
        .identity_column(Field::new("tenant", DataType::Utf8, false))
        .build()
        .expect("config")
}

/// One quarter-hour reading for a tenant, on a named table's shape.
fn tenant_reading(tenant: &str, kwh: i64, version: u128) -> meterstore::encode::StoredSeries {
    use datafusion::common::ScalarValue;
    use metering::interval::{MeterInterval, Sparte};
    use metering::measurement_series::{MeasurementSeries, MeasurementSource};

    let interval = MeterInterval {
        from: START,
        to: START + Duration::minutes(15),
        value: rust_decimal::Decimal::new(kwh, 0),
        quality: metering::QualityFlag::Measured,
        obis_code: "1-0:1.8.0".parse().ok(),
    };
    let series = MeasurementSeries::new(
        "11111111115".parse().unwrap(),
        "1-0:1.8.0".parse().ok(),
        vec![interval],
        MeasurementSource::ManualEntry {
            operator_id: "test".to_string(),
            reason: "fixture".to_string(),
        },
        datetime!(2026-07-27 06:00 UTC),
    );
    meterstore::encode::StoredSeries::new(
        series,
        meterstore::ScopedVersion::new(
            meterstore::VersionScope::for_interval("9900000000001", START, Sparte::Strom).unwrap(),
            meterstore::Version::new(version).unwrap(),
        ),
        datetime!(2026-07-27 06:00 UTC),
    )
    .with_extra("tenant", ScalarValue::Utf8(Some(tenant.to_string())))
}

/// Two tenanted tables in one catalog, each holding both tenants' rows.
async fn two_tenanted_tables() -> (TestHarness, MeterCatalog) {
    let harness = TestHarness::with_config(tenanted(TestHarness::TABLE))
        .await
        .expect("harness");

    let catalog = MeterCatalog::builder()
        .table(
            harness
                .builder_for(tenanted(TestHarness::TABLE))
                .await
                .expect("primary builder"),
        )
        .table(
            harness
                .builder_for(tenanted(SECOND))
                .await
                .expect("secondary builder"),
        )
        .build()
        .await
        .expect("catalog");
    catalog.create_tables().await.expect("create both");

    for store in catalog.tables() {
        store
            .admin()
            .hot_store()
            .ensure_partitions(
                store.config().name(),
                START,
                START + Duration::days(2),
                Duration::DAY,
            )
            .await
            .expect("partitions");
        harness
            .seed_watermark_for(store.config().name(), START, Duration::DAY)
            .await
            .expect("watermark");
        // Different values per tenant, so a scope that leaked would change the
        // number rather than happen to agree.
        store
            .append(&[
                tenant_reading("a", 10, 20_260_720_000_001),
                tenant_reading("b", 7, 20_260_720_000_002),
            ])
            .await
            .expect("append");
    }

    (harness, catalog)
}

#[tokio::test]
async fn a_catalog_confines_every_table_to_one_tenant() {
    // `isolated` confines the relations a session can name and
    // `MeterStore::scoped` confines one table's rows. A multi-tenant deployment
    // serving a whole catalog needs the cross-table join *and* the tenant
    // boundary, which is what this gives.
    let (_h, catalog) = two_tenanted_tables().await;

    async fn total(c: &MeterCatalog) -> i64 {
        count(
            &c.query("SELECT SUM(value)::BIGINT FROM readings")
                .await
                .expect("query"),
        )
    }

    // Unscoped, the catalog sees both tenants.
    assert_eq!(total(&catalog).await, 17);

    let confined = catalog.scoped("tenant", "a").await.expect("scoped");
    assert_eq!(total(&confined).await, 10, "one tenant's rows only");

    // Both tables, and the join that is the catalog's reason to exist still
    // plans — which is the half `isolated` could not give.
    let joined = confined
        .query(
            "SELECT SUM(r.value)::BIGINT FROM readings r \
             JOIN esa_typ2 e ON r.malo_id = e.malo_id",
        )
        .await
        .expect("a scoped catalog still joins");
    assert_eq!(count(&joined), 10, "the second table is scoped too");

    // Caller-supplied SQL cannot step past it: the predicate is injected below
    // the projection, so naming the other tenant returns nothing rather than
    // the other tenant's rows.
    let escape = confined
        .query("SELECT SUM(value)::BIGINT FROM readings WHERE tenant = 'b'")
        .await
        .expect("plans");
    assert_eq!(count(&escape), 0, "the scope is enforced, not advisory");

    // And the raw versions relation is scoped as well, or the audit trail would
    // be the way out.
    let raw = confined
        .query("SELECT COUNT(DISTINCT tenant)::BIGINT FROM readings_versions")
        .await
        .expect("plans");
    assert_eq!(count(&raw), 1);
}

#[tokio::test]
async fn a_catalog_scope_is_all_of_its_tables_or_none_of_them() {
    // A scope that covered three tables and silently skipped the fourth is not a
    // boundary. The refusal names the table that cannot carry it, and happens
    // before any table is confined.
    let harness = TestHarness::with_config(tenanted(TestHarness::TABLE))
        .await
        .expect("harness");
    let catalog = MeterCatalog::builder()
        .table(
            harness
                .builder_for(tenanted(TestHarness::TABLE))
                .await
                .expect("primary"),
        )
        // No `tenant` column at all.
        .table(
            harness
                .builder_for(config(SECOND))
                .await
                .expect("secondary"),
        )
        .build()
        .await
        .expect("catalog");

    let err = catalog
        .scoped("tenant", "a")
        .await
        .expect_err("one table cannot carry the scope")
        .to_string();
    assert!(err.contains(SECOND), "the message names the table: {err}");
    assert!(err.contains("tenant"), "{err}");
    assert!(
        err.contains("isolate"),
        "the message names the honest alternative: {err}"
    );

    // An attribute column is refused for the same reason it is on one store:
    // only a merge-key column partitions readings.
    assert!(catalog.scoped("bilanzkreis", "BK-1").await.is_err());
}

#[tokio::test]
async fn a_catalog_scope_only_ever_narrows() {
    let (_h, catalog) = two_tenanted_tables().await;
    let a = catalog.scoped("tenant", "a").await.expect("scoped");

    // Idempotent.
    a.scoped("tenant", "a").await.expect("same value");

    // And it cannot be re-pointed: a handle confined to one tenant that could
    // reach another is not a boundary.
    let err = a
        .scoped("tenant", "b")
        .await
        .expect_err("re-scoping widens")
        .to_string();
    assert!(err.contains("narrows"), "{err}");
}

#[tokio::test]
async fn a_derived_catalog_keeps_the_scope_it_was_given() {
    // A boundary a derived session drops is not a boundary — the property
    // `MeterStore` already holds, now across a catalog's rebuild.
    let (_h, catalog) = two_tenanted_tables().await;
    let confined = catalog.scoped("tenant", "a").await.expect("scoped");

    let historical = confined
        .in_read_mode(meterstore::ReadMode::Historical)
        .await
        .expect("historical");
    for store in historical.tables() {
        assert_eq!(
            store.row_scope().len(),
            1,
            "{} lost its scope",
            store.config().name()
        );
    }

    let known = confined
        .as_known_at(datetime!(2026-07-28 00:00 UTC))
        .await
        .expect("as_known_at");
    for store in known.tables() {
        assert_eq!(store.row_scope().len(), 1);
    }

    // `as_of` has no catalog form, and says why rather than inventing a
    // correspondence between two tables' snapshots.
    let err = catalog
        .in_read_mode(meterstore::ReadMode::AsOf {
            snapshot: meterstore::SnapshotSelector::Id(1),
            max_version: None,
        })
        .await
        .expect_err("a snapshot belongs to one table")
        .to_string();
    assert!(err.contains("one table"), "{err}");
    assert!(err.contains("as_known_at"), "{err}");
}

#[tokio::test]
async fn a_failing_table_reaches_the_caller_as_the_error_it_raised() {
    // `archive_all` must not wrap. Folding a per-table failure into a
    // `Storage` error flattens the taxonomy a caller matches on and, worse,
    // makes it **retryable** — `InvariantViolated` is the one condition this
    // crate is most emphatic must not be retried past, and archival is where it
    // surfaces. The table's name belongs in a log line, not in the message.
    let (_h, catalog) = two_tables().await;
    let store = catalog.table(SECOND).expect("secondary");

    // Break one table's hot half, so archival fails the same way through both
    // doors. Which variant it raises does not matter: what is asserted is that
    // the catalog hands back what the store raised.
    store
        .admin()
        .hot_store()
        .drop_table(SECOND)
        .await
        .expect("drop the hot half");

    let direct = store
        .admin()
        .archive(START + Duration::days(30), 4)
        .await
        .expect_err("the hot table is gone");
    let through_catalog = catalog
        .archive_all(START + Duration::days(30), 4)
        .await
        .expect_err("so the catalog cannot archive it either");

    assert_eq!(
        through_catalog.to_string(),
        direct.to_string(),
        "the catalog wrapped the error instead of passing it on"
    );
    assert_eq!(
        through_catalog.is_retryable(),
        direct.is_retryable(),
        "wrapping changed whether a supervisor will retry"
    );
}
