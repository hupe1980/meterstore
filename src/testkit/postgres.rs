//! One PostgreSQL container per process, and a fresh database per test.
//!
//! # Why this exists
//!
//! The line is drawn at storage fidelity: the properties worth testing here —
//! catalog compare-and-swap, advisory-lock scope, partition detach visibility,
//! `Decimal128` through Parquet — are exactly the ones a mock agrees with
//! whatever the code does. So the fixtures are real, and the cost is a
//! PostgreSQL.
//!
//! What is *not* required is a PostgreSQL **per test**. At a couple of hundred
//! integration tests that dominates the wall clock — every one paying a container
//! start, a health check and a connection handshake before it touches a line of
//! this crate — and it is flaky in a way that looks like a bug in the code under
//! test, since fifty containers racing for ports and memory produce connection
//! failures at whichever test happened to be starting.
//!
//! A container start is seconds; `CREATE DATABASE` is milliseconds, and gives
//! the **same** isolation. Every test still gets a database nothing else
//! touches, including its own Iceberg SQL catalog tables, so nothing about what
//! the suites prove changes.
//!
//! # The container is never dropped
//!
//! It lives in a process-wide `OnceCell` and outlives
//! every test, which is the point — a shared fixture that could be dropped by
//! whichever test finished first would be worse than no sharing at all.
//! `testcontainers` starts a reaper alongside it that removes the container when
//! the process exits, so nothing survives a run.
//!
//! # Opting out
//!
//! [`isolated_database`] starts a container of its own and hands back a guard.
//! Use it for a test that needs a server to itself — a restart, a configuration
//! change, a resource limit — where a shared instance would be a different
//! experiment.

use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::{Connection, PgConnection};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;

use crate::error::{Error, Result};

/// The PostgreSQL image every suite runs against.
///
/// **Pinned, and not at the library default.** `testcontainers-modules` defaults
/// to `11-alpine`, one major below the floor this design requires — so
/// every suite would have been proving the store works on a version it does not
/// claim to support, and `ATTACH PARTITION` would still have taken
/// `ACCESS EXCLUSIVE` on the parent there, so the lock properties the tests
/// assert would have been asserted against the one server where they do not
/// hold. 16 matches the reference deployment.
pub const IMAGE_TAG: &str = "16-alpine";

/// A running PostgreSQL, kept for the life of the process.
struct Shared {
    admin_url: String,
    _container: testcontainers::ContainerAsync<Postgres>,
}

static SHARED: OnceCell<Shared> = OnceCell::const_new();
static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

/// Connections the shared server accepts.
///
/// PostgreSQL's default is 100, which was ample when every test had a container
/// to itself and is not when they share one: `cargo test` runs one thread per
/// core, each test opens one or more pools, and `sqlx` pools default to ten
/// connections each. Exhausting the limit does not produce a tidy "too many
/// clients" — the server drops the connection and `sqlx` reports
/// `expected to read 5 bytes, got 0 bytes at EOF`, which reads as a broken test.
///
/// Raising it is nearly free: an idle backend slot costs a few hundred bytes of
/// shared memory, and these are short-lived test connections.
const MAX_CONNECTIONS: &str = "500";

/// Start a PostgreSQL container and return its superuser URL.
async fn start_container() -> Result<(String, testcontainers::ContainerAsync<Postgres>)> {
    let container = Postgres::default()
        .with_tag(IMAGE_TAG)
        .with_cmd([
            "postgres",
            "-c",
            &format!("max_connections={MAX_CONNECTIONS}"),
        ])
        .start()
        .await
        .map_err(|e| Error::Storage(format!("starting postgres: {e}")))?;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .map_err(|e| Error::Storage(format!("mapping postgres port: {e}")))?;
    Ok((
        format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres"),
        container,
    ))
}

/// Connect, retrying briefly while the server finishes coming up.
///
/// `testcontainers` waits for the readiness log line, which PostgreSQL emits
/// once during initdb *before* it restarts to accept real connections. The
/// window is short and the failure is a bare "connection refused" at whichever
/// test was unlucky — which reads as a defect in the code under test rather than
/// in the fixture, and is the more expensive kind of flake for exactly that
/// reason.
async fn connect_ready(url: &str) -> Result<PgConnection> {
    let mut last = None;
    for attempt in 0..20u32 {
        match PgConnection::connect(url).await {
            Ok(connection) => return Ok(connection),
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(
                    50 * u64::from(attempt + 1),
                ))
                .await;
            }
        }
    }
    Err(Error::Storage(format!(
        "postgres did not accept a connection: {}",
        last.expect("at least one attempt")
    )))
}

/// A connection URL for a database nothing else in this process uses.
///
/// The container is shared and started once; the database is created per call
/// and is as isolated as a separate server for everything these suites do — its
/// own tables, its own Iceberg SQL catalog, its own advisory-lock namespace is
/// the only thing it shares, and that is keyed by table name.
pub async fn fresh_database() -> Result<String> {
    let shared = SHARED
        .get_or_try_init(|| async {
            let (admin_url, container) = start_container().await?;
            // Fail here, once, rather than in whichever test drew the short
            // straw: a readiness problem is a fixture problem and should be
            // reported as one.
            connect_ready(&admin_url).await?.close().await.ok();
            Ok::<_, Error>(Shared {
                admin_url,
                _container: container,
            })
        })
        .await?;

    let name = format!(
        "meterstore_it_{}",
        NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
    );
    let mut admin = connect_ready(&shared.admin_url).await?;
    // Not parameterisable — an identifier never is — and not user input: the
    // name is this module's own counter.
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#))
        .execute(&mut admin)
        .await
        .map_err(|e| Error::Storage(format!("creating test database {name}: {e}")))?;
    admin.close().await.ok();

    Ok(shared
        .admin_url
        .rsplit_once('/')
        .map(|(prefix, _)| format!("{prefix}/{name}"))
        .expect("the admin URL carries a database path"))
}

/// A PostgreSQL nothing else shares, and the container keeping it alive.
///
/// Hold the guard for as long as the URL is in use; dropping it stops the
/// container. Prefer [`fresh_database`] unless the test genuinely needs a server
/// to itself.
pub struct IsolatedPostgres {
    /// Superuser connection URL.
    pub url: String,
    _container: testcontainers::ContainerAsync<Postgres>,
}

/// Start a PostgreSQL container for one caller.
pub async fn isolated_database() -> Result<IsolatedPostgres> {
    let (url, container) = start_container().await?;
    connect_ready(&url).await?.close().await.ok();
    Ok(IsolatedPostgres {
        url,
        _container: container,
    })
}
