//! Third-party interop: an **ADBC** client reaches the unified view.
//!
//! `flight_sql.rs` drives this surface with `FlightSqlServiceClient` and says the
//! client is "the same one a JDBC/ODBC driver wraps". That is an argument, and
//! this is the measurement: ADBC is the API a non-Rust consumer actually holds,
//! its Flight SQL driver is a separate implementation in another language, and it
//! makes calls the Rust client never makes.
//!
//! It found two things, and neither was visible from this side of the
//! connection. `GetTables` and its siblings were answered with the schema their
//! backing SQL happened to produce — carrying the tiering watermark in the
//! schema metadata and a non-nullable `catalog_name` — and the driver rejects
//! that outright, because those schemas are part of the wire contract rather
//! than an answer about readings. And `GetSqlInfo` was unimplemented, which is
//! survivable and therefore worse: the client falls back to a server with no
//! name, no version and no read-only flag.
//!
//! # One line this cannot silence
//!
//! Every connection prints *"Cannot disable autocommit; conn will not be DB-API
//! 2.0 compliant"*. That is the Python DB-API shim turning autocommit off to
//! satisfy its own contract, on a server that declares no transaction support
//! and, being read-only, has no use for one. It is the shim's, not this
//! server's, and no `SqlInfo` value stops it.
//!
//! # Network
//!
//! `pip install adbc_driver_flightsql` runs at test time, so this suite needs
//! outbound network — and the container has to reach a server on the host, which
//! is what `host.docker.internal` is mapped for. It lives in its own file for the
//! same reason the DuckDB and PyIceberg suites do: a run without network fails
//! here and nowhere else.

#![cfg(all(feature = "testkit", feature = "flight"))]

use meterstore::serve::FlightSqlServer;
use meterstore::testkit::{MeteringWorkload, Oracle, TestHarness};
use testcontainers::core::{Host, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// Printed last by every script, so waiting on it is deterministic.
const SENTINEL: &str = "__meterstore_done__";

/// Pinned, for the reason every image here is: an upstream release must not turn
/// this suite red for a reason unrelated to any change in this crate.
const PYTHON_IMAGE: (&str, &str) = ("python", "3.13-slim");

/// The ADBC packages under test. The manager is the part a consumer links
/// against; the driver is the Flight SQL implementation it loads.
const ADBC: &str = "adbc_driver_manager==1.8.0 adbc_driver_flightsql==1.8.0 pyarrow==21.0.0";

/// A Flight SQL server a container can reach, and the port it is on.
struct Served {
    port: u16,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

/// Serve on **`0.0.0.0`**, not loopback: the client is in a container, and a
/// server bound to loopback is one it cannot see.
async fn serve(store: meterstore::MeterStore) -> Served {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0")
        .await
        .expect("bind an ephemeral port");
    let port = listener.local_addr().expect("local address").port();

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

    Served {
        port,
        _shutdown: shutdown,
    }
}

/// Run a Python script with an ADBC connection already open as `conn`.
async fn adbc(port: u16, script: &str) -> Vec<String> {
    // stderr folded into stdout and the sentinel printed unconditionally: a
    // container that dies before its wait string is `EndOfStream` with nothing
    // attached, which says only that something went wrong. This way the failure
    // is in the output the assertion below reads.
    let program = format!(
        r#"set -u
pip install --quiet --disable-pip-version-check {ADBC}
python - <<'PYEOF' 2>&1 || true
import adbc_driver_flightsql.dbapi as flight_sql

# The driver opens the connection here, and everything it does before the first
# query happens inside this call.
conn = flight_sql.connect("grpc://host.docker.internal:{port}")
{script}
PYEOF
echo "{SENTINEL}"
"#
    );

    let _slot = crate::containers::engine_slot().await;

    let container = GenericImage::new(PYTHON_IMAGE.0, PYTHON_IMAGE.1)
        .with_wait_for(WaitFor::message_on_stdout(SENTINEL))
        .with_startup_timeout(crate::containers::STARTUP_TIMEOUT)
        // The whole point: the server runs in the test process, on the host.
        .with_host("host.docker.internal", Host::HostGateway)
        .with_cmd(vec!["bash".to_string(), "-c".to_string(), program])
        .start()
        .await
        .expect("start python");

    let stdout = container.stdout_to_vec().await.expect("python stdout");
    let text = String::from_utf8_lossy(&stdout).to_string();
    assert!(
        !text.contains("Traceback"),
        "the ADBC client failed:\n{text}"
    );

    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && *line != SENTINEL)
        // The autocommit warning and its `warnings.warn(` echo, which the DB-API
        // shim prints on every connection to a server without transactions — see
        // the module documentation. Dropped here rather than in each test,
        // narrowly enough that anything else still arrives; a traceback is caught
        // above, on the unfiltered text.
        .filter(|line| !line.contains("autocommit") && *line != "warnings.warn(")
        .map(str::to_string)
        .collect()
}

/// A store holding three days, the first two archived — so a query has to span
/// the watermark to be right.
async fn split_store() -> (TestHarness, meterstore::MeterStore, Oracle) {
    let workload = MeteringWorkload::new(START)
        .seed(0xADBC)
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

    store
        .admin()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adbc_client_opens_a_connection_and_reads_across_the_watermark() {
    // Two things at once, and the first is the one no Rust-side test can reach:
    // `flight_sql.connect` completes, which means every call the driver makes
    // before a query is answered. Then the query itself, whose answer only
    // exists because the server unified two tiers — an ADBC client with the
    // object store and the Iceberg catalogue could not compute it.
    let (_h, store, oracle) = split_store().await;
    let expected = oracle.row_count(START, START + Duration::days(3));
    let served = serve(store).await;

    let out = adbc(
        served.port,
        r#"
with conn.cursor() as cur:
    cur.execute("SELECT count(*) AS n FROM readings")
    print(cur.fetchone()[0])
"#,
    )
    .await;

    assert_eq!(
        out,
        vec![expected.to_string()],
        "an ADBC client must see every row, on both sides of the boundary"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adbc_client_can_list_what_it_may_query() {
    // The metadata path, which a BI tool walks before it runs anything and which
    // reaches `GetTables` rather than the query handlers. `readings` is the
    // resolved view and `readings_versions` the audit trail; both are reachable
    // on purpose, and a client that could see neither would have nothing to pick
    // from.
    let (_h, store, _oracle) = split_store().await;
    let served = serve(store).await;

    let out = adbc(
        served.port,
        r#"
tables = conn.adbc_get_objects(depth="tables").read_all().to_pylist()
names = sorted(
    t["table_name"]
    for cat in tables
    for schema in (cat["catalog_db_schemas"] or [])
    for t in (schema["db_schema_tables"] or [])
)
for name in names:
    print(name)
"#,
    )
    .await;

    assert!(
        out.contains(&"readings".to_string()),
        "the resolved view has to be listed: {out:?}"
    );
    assert!(
        out.contains(&"readings_versions".to_string()),
        "so does the audit trail: {out:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adbc_client_is_told_what_the_server_is() {
    // `GetSqlInfo` is the call a driver makes while opening a connection, and
    // answering it `Unimplemented` is survivable rather than fatal — which is
    // exactly why nothing caught it. What the driver falls back to is a server
    // with no name and no version, so a client cannot say what it reached and a
    // bug report cannot say which build produced the answer.
    //
    // The same response carries `FLIGHT_SQL_SERVER_READ_ONLY`, which this Python
    // shim does not surface but a BI tool acts on; that one is pinned in the unit
    // tests beside the builder.
    let (_h, store, _oracle) = split_store().await;
    let served = serve(store).await;

    let out = adbc(
        served.port,
        r#"
info = conn.adbc_get_info()
print("vendor=" + info["vendor_name"])
print("version=" + info["vendor_version"])
print("arrow=" + info["vendor_arrow_version"])
"#,
    )
    .await;

    assert!(
        out.iter().any(|line| line == "vendor=MeterStore"),
        "the server has to name itself, or a client cannot tell what it reached: {out:?}"
    );
    assert!(
        out.iter().any(|line| line == "version=0.14.0"),
        "and say which build, which is what a bug report needs: {out:?}"
    );
    assert!(
        out.iter().any(|line| line.starts_with("arrow=")),
        "and which Arrow version it encodes with, which is what the client decodes \
         against: {out:?}"
    );
}
