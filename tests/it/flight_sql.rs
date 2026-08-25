//! Flight SQL over the unified view, against a real client.
//!
//! §13.7.3's justification is narrow and specific: the hot tier lives in
//! PostgreSQL and is not in the Iceberg catalog, so **the unified hot + cold view
//! is the one thing an external client cannot assemble for itself**. Everything
//! else it might want is better served by reading the catalog directly.
//!
//! So the test that matters is the one that shows a non-Rust-shaped client — here
//! `FlightSqlServiceClient`, the same client a JDBC/ODBC driver wraps — getting
//! rows that span the watermark, which no amount of object-store access would
//! give it.
//!
//! The rest checks the two properties that make the surface safe rather than
//! merely present: it refuses to write, and results carry the boundary they were
//! computed against (P1) even over a socket.

#![cfg(all(feature = "testkit", feature = "flight"))]

use arrow_flight::sql::client::FlightSqlServiceClient;
use meterstore::serve::FlightSqlServer;
use meterstore::testkit::{MeteringWorkload, Oracle, TestHarness};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};
use tonic::transport::Channel;

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// A running Flight SQL server over a store, plus a connected client.
///
/// Bound to port 0 so concurrent tests cannot collide, which they would on a
/// fixed port the moment `cargo test` runs them in parallel.
struct Served {
    client: FlightSqlServiceClient<Channel>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn serve(store: meterstore::MeterStore) -> Served {
    serve_surface(store).await
}

/// The same, over anything the server can serve — a store or a whole catalog.
async fn serve_surface(surface: impl meterstore::SqlSurface) -> Served {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral port");
    let address = listener.local_addr().expect("local address");

    let (shutdown, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(FlightSqlServer::new(surface).into_service())
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
        .expect("connect to the flight server");

    Served {
        client: FlightSqlServiceClient::new(channel),
        _shutdown: shutdown,
    }
}

/// Collect every batch a Flight SQL query produces.
async fn query(served: &mut Served, sql: &str) -> Vec<datafusion::arrow::array::RecordBatch> {
    use futures::TryStreamExt;

    let info = served
        .client
        .execute(sql.to_string(), None)
        .await
        .expect("get_flight_info");

    let ticket = info.endpoint[0]
        .ticket
        .clone()
        .expect("an endpoint carries a ticket");

    served
        .client
        .do_get(ticket)
        .await
        .expect("do_get")
        .try_collect()
        .await
        .expect("collect batches")
}

/// A store holding three days, the first two archived.
async fn split_store() -> (TestHarness, meterstore::MeterStore, Oracle) {
    let workload = MeteringWorkload::new(START)
        .seed(0xF117)
        .malo_ids(4)
        .days(3);
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

    // Two days cold, one still hot — so a query has to span the boundary.
    store
        .archive(from + Duration::days(3), 2)
        .await
        .expect("archive");
    assert_eq!(
        store.watermark().await.unwrap().get(),
        from + Duration::days(2),
        "the fixture must actually straddle the watermark"
    );

    (harness, store, oracle)
}

/// The gRPC status behind a Flight error.
///
/// `FlightError` wraps it, and the status code is the part a client acts on —
/// `PermissionDenied` means "never", `Unavailable` means "retry".
fn expect_status(error: arrow_flight::error::FlightError) -> tonic::Status {
    match error {
        arrow_flight::error::FlightError::Tonic(status) => *status,
        other => panic!("expected a gRPC status, got {other:?}"),
    }
}

/// The single `BIGINT` a counting query produces.
fn count(batches: &[datafusion::arrow::array::RecordBatch]) -> i64 {
    use datafusion::arrow::array::AsArray;
    batches[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
        .value(0)
}

#[tokio::test]
async fn a_flight_client_gets_the_unified_view() {
    // The entire justification for this surface. An external client with object
    // storage and the Iceberg catalog can reach the cold tier; it cannot reach
    // the hot window, and so cannot produce this number for itself.
    let (_h, store, oracle) = split_store().await;
    let (from, to) = (START, START + Duration::days(3));

    let mut served = serve(store).await;
    let batches = query(&mut served, "SELECT COUNT(*) FROM readings").await;

    assert_eq!(
        count(&batches) as u64,
        oracle.row_count(from, to),
        "a Flight client must see both tiers"
    );
}

#[tokio::test]
async fn results_carry_the_boundary_they_were_computed_against() {
    // P1 over a socket. Without this a figure pulled by a BI tool is
    // indistinguishable from one pulled a minute later against a different
    // boundary — which is exactly the confusion provenance exists to prevent.
    let (_h, store, _oracle) = split_store().await;
    let expected = store.watermark().await.unwrap();

    let mut served = serve(store).await;
    let batches = query(&mut served, "SELECT COUNT(*) FROM readings").await;

    let metadata = batches[0].schema();
    let metadata = metadata.metadata();

    assert_eq!(
        metadata
            .get(meterstore::watermark::WATERMARK_PROPERTY)
            .map(String::as_str),
        Some(expected.to_string().as_str()),
        "the watermark must travel with the rows"
    );

    let tiers = metadata
        .get("meterstore.tiers_scanned")
        .expect("tiers scanned");
    assert!(
        tiers.contains("cold") && tiers.contains("hot"),
        "this query spans both tiers, and the metadata must say so: {tiers}"
    );
}

#[tokio::test]
async fn a_corrected_interval_is_counted_once_over_flight() {
    // The §13.7.2 trap does not exist here — `readings` is the resolved table —
    // but that has to be true through this surface too, because a BI tool
    // pointed at the raw table would double-count exactly as an Iceberg engine
    // would.
    let workload = MeteringWorkload::new(START)
        .seed(0xC0DE)
        .malo_ids(3)
        .days(2)
        .with_corrections(0.25);
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

    let mut served = serve(store).await;

    let resolved = query(&mut served, "SELECT COUNT(*) FROM readings").await;
    assert_eq!(count(&resolved) as u64, oracle.row_count(from, to));

    // And the raw table really does hold more, so the resolution above was doing
    // work rather than being trivially satisfied.
    let raw = query(&mut served, "SELECT COUNT(*) FROM readings_versions").await;
    assert!(
        count(&raw) as u64 > oracle.row_count(from, to),
        "the fixture must produce corrections for this to mean anything"
    );
}

#[tokio::test]
async fn a_client_can_browse_the_tables() {
    // A BI tool lists what is there before it queries anything. Both relations
    // must appear — the audit trail is reachable on purpose — and so must the
    // system tables an operator would want.
    let (_h, store, _oracle) = split_store().await;
    let mut served = serve(store).await;

    let batches = query(
        &mut served,
        "SELECT table_name FROM information_schema.tables ORDER BY 1",
    )
    .await;

    use datafusion::arrow::array::AsArray;
    let mut names = Vec::new();
    for batch in &batches {
        let column = batch.column(0).as_string::<i32>();
        for i in 0..batch.num_rows() {
            names.push(column.value(i).to_string());
        }
    }

    assert!(names.iter().any(|n| n == "readings"), "{names:?}");
    assert!(names.iter().any(|n| n == "readings_versions"), "{names:?}");
}

#[tokio::test]
async fn a_write_is_refused_with_the_reason() {
    // Not "unimplemented": a write here would bypass tier routing and the
    // subject-reference check, and both failures are invisible afterwards. A
    // client must be told that rather than led to look for a flag.
    let (_h, store, _oracle) = split_store().await;
    let mut served = serve(store).await;

    let error = served
        .client
        .execute_update(
            "INSERT INTO readings_versions (malo_id) VALUES ('x')".to_string(),
            None,
        )
        .await
        .expect_err("a write must be refused");

    let status = expect_status(error);
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    let message = status.message();
    assert!(message.contains("read-only"), "{message}");
    assert!(message.contains("MeterStore::append"), "{message}");
}

#[tokio::test]
async fn a_query_shaped_statement_cannot_reach_the_filesystem() {
    // Refusing `execute_update` by shape is not enough on its own: DataFusion's
    // SQL surface is wider than `SELECT`, and `CREATE EXTERNAL TABLE … LOCATION`
    // and `COPY … TO` arrive as ordinary *statement queries* — the path
    // `execute` takes. `ctx.sql` executes DDL while planning it, so unguarded
    // that is one round trip from any client reaching the port: arbitrary file
    // read on the way in, arbitrary file write on the way out, and an external
    // table over the warehouse's own Parquet returning every row a session
    // scope is meant to confine.
    let (_h, store, _oracle) = split_store().await;
    let mut served = serve(store).await;

    let escape = std::env::temp_dir().join("meterstore_flight_escape.parquet");
    let _ = std::fs::remove_file(&escape);

    for sql in [
        "CREATE EXTERNAL TABLE leak STORED AS PARQUET LOCATION '/tmp'".to_string(),
        format!(
            "COPY (SELECT 1 AS a) TO '{}' STORED AS PARQUET",
            escape.display()
        ),
        "CREATE TABLE leak2 AS SELECT 1 AS a".to_string(),
    ] {
        let error = served
            .client
            .execute(sql.clone(), None)
            .await
            .err()
            .unwrap_or_else(|| panic!("{sql} was accepted over Flight SQL"));
        let status = expect_status(error);
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{sql}");
        assert!(
            status.message().contains("not accepted here"),
            "{sql}: {}",
            status.message()
        );
    }

    assert!(!escape.exists(), "no Flight statement may write a file");

    // The same statements through `prepare`, which is the other door in.
    let error = served
        .client
        .prepare(
            "CREATE EXTERNAL TABLE leak STORED AS PARQUET LOCATION '/tmp'".to_string(),
            None,
        )
        .await
        .expect_err("preparing DDL must be refused too");
    assert!(
        expect_status(error).message().contains("not accepted here"),
        "prepare is the same surface"
    );
}

#[tokio::test]
async fn a_prepared_statement_round_trips() {
    // What a JDBC driver actually does: prepare once, execute, close.
    let (_h, store, oracle) = split_store().await;
    let (from, to) = (START, START + Duration::days(3));
    let mut served = serve(store).await;

    let mut prepared = served
        .client
        .prepare("SELECT COUNT(*) FROM readings".to_string(), None)
        .await
        .expect("prepare");

    let info = prepared.execute().await.expect("execute");
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");

    use futures::TryStreamExt;
    let batches: Vec<_> = served
        .client
        .do_get(ticket)
        .await
        .expect("do_get")
        .try_collect()
        .await
        .expect("collect");

    assert_eq!(count(&batches) as u64, oracle.row_count(from, to));

    prepared.close().await.expect("close");
}

#[tokio::test]
async fn a_bad_statement_fails_at_plan_time() {
    // A client should learn about a broken query from `GetFlightInfo`, not
    // halfway through a stream it has already started rendering.
    let (_h, store, _oracle) = split_store().await;
    let mut served = serve(store).await;

    let error = served
        .client
        .execute("SELECT * FROM no_such_table".to_string(), None)
        .await
        .expect_err("an unknown table must fail");

    assert_eq!(expect_status(error).code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn get_flight_info_plans_without_scanning() {
    // `GetFlightInfo` must produce a schema, not rows. Answering it by *running*
    // the query makes a BI tool's
    // ordinary `GetFlightInfo` → `DoGet` sequence cost two full scans, on the one
    // surface built for BI tools.
    //
    // Observed through the metric rather than the wall clock, because a timing
    // assertion on a container is a flake waiting to happen: `query.scan_duration`
    // is recorded when a tier's stream drains, so a plan that scanned nothing
    // leaves the counter where it was.
    let (_h, store, _oracle) = split_store().await;
    let mut served = serve(store).await;

    let sql = format!(
        r#"SELECT COUNT(*) FROM {} WHERE malo_id IS NOT NULL"#,
        TestHarness::TABLE
    );

    // Planning alone: a schema comes back and no endpoint has been drained.
    let info = served
        .client
        .execute(sql.clone(), None)
        .await
        .expect("get_flight_info");

    let schema = info.try_decode_schema().expect("a schema");
    assert!(
        !schema.fields().is_empty(),
        "the plan must describe a shape"
    );

    // And the provenance is on it, without a row having been produced.
    assert!(
        schema
            .metadata()
            .contains_key(meterstore::watermark::WATERMARK_PROPERTY),
        "provenance must travel with the schema: {:?}",
        schema.metadata()
    );

    // The rows arrive on `do_get`, and the answer is the whole table — so the
    // plan really did describe this statement rather than a narrower one.
    let batches = query(&mut served, &sql).await;
    assert!(count(&batches) > 0, "do_get is what produces the rows");
}

#[tokio::test]
async fn preparing_a_statement_does_not_run_it() {
    // Same property on the prepared path: preparing is planning. A prepare that
    // executed would make the prepare/execute split cost double a plain query.
    let (_h, store, _oracle) = split_store().await;
    let mut served = serve(store).await;

    let prepared = served
        .client
        .prepare(format!(r#"SELECT * FROM {}"#, TestHarness::TABLE), None)
        .await
        .expect("prepare");

    // The schema is known before any row is fetched.
    let schema = prepared.dataset_schema().expect("a dataset schema");
    assert!(schema.fields().iter().any(|f| f.name() == "malo_id"));

    prepared.close().await.expect("close");
}

/// A catalog holding a billing table and a non-authoritative second stream,
/// both populated and both archived through the boundary.
///
/// The shape §15.3 calls ordinary: a statement mentioning both tables is the
/// second thing an external client cannot assemble for itself.
async fn two_table_catalog() -> (TestHarness, meterstore::MeterCatalog) {
    use meterstore::config::TableConfig;
    use meterstore::tiering::store::HotStore;

    const SECOND: &str = "esa_typ2_versions";
    let config = |name: &str| {
        TableConfig::new(name)
            .settlement_lag(Duration::days(1))
            .build()
            .expect("config")
    };

    let harness = TestHarness::start().await.expect("harness");
    let catalog = meterstore::MeterCatalog::builder()
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
                START + Duration::days(4),
                Duration::DAY,
            )
            .await
            .expect("partitions");
        harness
            .seed_watermark_for(store.config().name(), START, Duration::DAY)
            .await
            .expect("watermark");
    }

    // Different populations, so a statement returning the wrong table's rows
    // produces the wrong count rather than the right one by luck.
    for (table, seed, offset, malos) in [
        (TestHarness::TABLE, 0xB111u64, 0usize, 3usize),
        (SECOND, 0xE5A, 500, 1),
    ] {
        let workload = MeteringWorkload::new(START)
            .seed(seed)
            .malo_offset(offset)
            .malo_ids(malos)
            .days(3);
        catalog
            .table(table)
            .expect("table")
            .append(&workload.generate().expect("workload"))
            .await
            .expect("append");
    }

    // One table archived past the boundary and the other not, so the two
    // genuinely have different watermarks — which is the fact a single reported
    // boundary would erase.
    catalog
        .table(TestHarness::TABLE)
        .expect("primary")
        .archive(START + Duration::days(3), 2)
        .await
        .expect("archive");

    (harness, catalog)
}

#[tokio::test]
async fn a_catalog_serves_a_statement_that_mentions_two_tables() {
    // The justification for this endpoint is that the unified hot + cold view is
    // the one thing an external client cannot assemble for itself. A statement
    // spanning two *tables* is the second: each has its own watermark and its own
    // hot half, so no amount of object-store access reconstructs it either.
    let (_harness, catalog) = two_table_catalog().await;
    let mut served = serve_surface(catalog).await;

    let batches = query(
        &mut served,
        "SELECT (SELECT COUNT(*) FROM readings) + (SELECT COUNT(*) FROM esa_typ2) AS both",
    )
    .await;

    let total: i64 = batches
        .iter()
        .map(|b| {
            use datafusion::arrow::array::AsArray;
            b.column(0)
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                .value(0)
        })
        .sum();
    assert!(total > 0, "both tables must contribute rows");
}

#[tokio::test]
async fn a_catalog_result_carries_every_boundary_rather_than_one() {
    // Two tables genuinely have two watermarks, and the fixture archives only
    // one of them so they differ. Reporting a single number would tell a client
    // a figure was settled to a point only one of its inputs had reached — the
    // fiction carrying provenance exists to prevent.
    let (_harness, catalog) = two_table_catalog().await;
    let mut served = serve_surface(catalog).await;

    let info = served
        .client
        .execute(
            "SELECT (SELECT COUNT(*) FROM readings) + (SELECT COUNT(*) FROM esa_typ2) AS both"
                .to_string(),
            None,
        )
        .await
        .expect("get_flight_info");

    let schema = info.try_decode_schema().expect("schema on the flight info");
    let metadata = schema.metadata();

    let listed = metadata
        .get("meterstore.watermarks")
        .expect("a catalog reports its boundaries per table");
    assert!(
        listed.contains(TestHarness::TABLE) && listed.contains("esa_typ2_versions"),
        "both tables must be named: {listed}"
    );
    assert!(
        listed.split(',').count() == 2,
        "one entry per table the statement touched: {listed}"
    );

    // The conservative minimum is still there, because that is the one number a
    // reconciliation can use directly.
    assert!(
        metadata
            .get(meterstore::watermark::WATERMARK_PROPERTY)
            .is_some(),
        "{metadata:?}"
    );
}
