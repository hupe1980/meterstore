//! Third-party interop: **DuckDB** reads what MeterStore wrote.
//!
//! `tests/interop.rs` proves the output is readable by a reader that is not this
//! crate — but that reader is still DataFusion, which shares `arrow-rs` and
//! `parquet-rs` with the writer. A bug in that shared layer would be invisible
//! to it.
//!
//! DuckDB is a genuinely independent implementation: a different language, a
//! different Parquet reader, and — through its `iceberg` extension — a different
//! reading of the Iceberg spec. That is what makes it able to falsify P2, and it
//! is the half §17.4 could not previously check.
//!
//! # Two levels, and the second is the one that matters
//!
//! **Parquet level.** `read_parquet` over the data files. Catches an encoding or
//! a type that only `parquet-rs` understands.
//!
//! **Iceberg level.** `iceberg_scan` over the table metadata — manifests,
//! manifest lists, snapshot summaries, the schema with its field IDs. This is
//! the one that catches a *metadata dialect* only `iceberg-rust` writes, which
//! was the risk left open when the hermetic suite went in. It is also the level a
//! real engine actually operates at: an engine is pointed at a table, not at a
//! list of files.
//!
//! # Network
//!
//! The Iceberg extension is downloaded by `INSTALL iceberg` at test time, so this
//! suite needs outbound network from the container. That is a real dependency and
//! it is why it lives in its own file: a run without network fails here and
//! nowhere else, which makes the cause obvious.
//!
//! # Two details of the setup, and what they say about the design
//!
//! **The warehouse is mounted at its own absolute path.** Iceberg metadata
//! records absolute file locations, so mounting it elsewhere would force
//! DuckDB's `allow_moved_paths` relocation — and then the test would prove the
//! relocation works rather than that the recorded paths do. Mounting at the
//! identical path means DuckDB follows the locations MeterStore actually wrote.
//!
//! **`unsafe_enable_version_guessing` is required, and that is a finding.**
//! DuckDB needs to know which metadata document is current. A REST catalog tells
//! an engine directly; the **SQL catalog** keeps the pointer in PostgreSQL and
//! writes no `version-hint.text` beside the files, so an engine pointed at the
//! bare directory has to glob and guess. §13.7.1 predicted that SQL-catalog
//! deployments need the façade for exactly this class of reason — this is that
//! prediction, demonstrated. It is a property of the catalog choice, not of the
//! files.

#![cfg(feature = "testkit")]

use std::path::Path;

use meterstore::testkit::{MeteringWorkload, Oracle, TestHarness};
use rust_decimal::Decimal;
use testcontainers::core::{Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

/// Printed last by every script, so waiting on it is deterministic.
///
/// A container that runs a command and exits is racy to observe otherwise: the
/// logs may be collected before the query has produced anything.
const SENTINEL: &str = "__meterstore_done__";

/// The DuckDB image, pinned.
///
/// `latest` would make an upstream release able to turn this suite red for a
/// reason that has nothing to do with a change here — and the whole point of the
/// suite is that a *failure means the output stopped being portable*. The risk
/// register's "pin exact versions" applies to the engines we test against as
/// much as to the crates we build on.
const DUCKDB_IMAGE: (&str, &str) = ("duckdb/duckdb", "1.5.5");

/// Run a DuckDB script against the mounted warehouse and return its CSV rows.
///
/// The image is distroless — no shell — so the container *is* the query: it runs
/// `/duckdb -csv -c <script>` and exits.
async fn duckdb(warehouse: &Path, script: &str) -> Vec<String> {
    let mount = warehouse.display().to_string();

    // `.bail off` is load-bearing. Without it DuckDB aborts the script on the
    // first error and exits before printing the sentinel, so the wait fails with
    // "end of stream" and the actual message is lost — which is the least useful
    // possible way to learn a query was wrong.
    let cmd = vec![
        "/duckdb".to_string(),
        "-csv".to_string(),
        "-c".to_string(),
        ".bail off".to_string(),
        "-c".to_string(),
        // Version guessing: see the module docs. The SQL catalog keeps the
        // metadata pointer in PostgreSQL rather than beside the files.
        "INSTALL iceberg; LOAD iceberg; SET unsafe_enable_version_guessing = true;".to_string(),
        "-c".to_string(),
        script.to_string(),
        "-c".to_string(),
        format!("SELECT '{SENTINEL}' AS done;"),
    ];

    let container = GenericImage::new(DUCKDB_IMAGE.0, DUCKDB_IMAGE.1)
        .with_wait_for(WaitFor::message_on_stdout(SENTINEL))
        // Mounted at its own path, so the absolute locations inside the Iceberg
        // metadata resolve exactly as written.
        .with_mount(Mount::bind_mount(mount.clone(), mount))
        .with_cmd(cmd)
        .start()
        .await
        .expect("start duckdb");

    let stdout = container.stdout_to_vec().await.expect("duckdb stdout");
    let text = String::from_utf8_lossy(&stdout).to_string();

    // Anything on stderr is DuckDB complaining, and a silent empty result would
    // otherwise look like a legitimate zero.
    let stderr = container.stderr_to_vec().await.unwrap_or_default();
    let errors = String::from_utf8_lossy(&stderr).to_string();
    assert!(
        !errors.to_lowercase().contains("error"),
        "duckdb reported an error:\n{errors}\n--- stdout ---\n{text}"
    );

    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && *l != SENTINEL && *l != "done")
        .map(str::to_string)
        .collect()
}

/// The table's directory, which is what an Iceberg engine is pointed at.
///
/// Derived from the warehouse layout rather than asked of `iceberg-rust`,
/// because the point is to approach the table the way a foreign engine does.
fn table_path(harness: &TestHarness) -> String {
    harness
        .warehouse()
        .join("metering")
        .join(TestHarness::TABLE)
        .display()
        .to_string()
}

/// A store with a workload ingested and everything archived.
async fn archived(workload: MeteringWorkload) -> (TestHarness, meterstore::MeterStore, Oracle) {
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

    store
        .archive(to + Duration::days(2), 64)
        .await
        .expect("archive");
    assert!(
        store.watermark().await.unwrap().get() >= to,
        "an external engine sees only the cold tier, so everything must reach it"
    );

    (harness, store, oracle)
}

/// The single value a one-column, one-row DuckDB result carries.
///
/// Unquoted, because DuckDB quotes a CSV field containing the separator and a
/// caller comparing against the raw value would otherwise see a stray `"`.
fn only(rows: &[String]) -> String {
    // The first line is the CSV header.
    assert_eq!(rows.len(), 2, "expected a header and one row, got {rows:?}");
    let value = rows[1].trim();
    match value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
        Some(inner) => inner.replace("\"\"", "\""),
        None => value.to_string(),
    }
}

#[tokio::test]
async fn duckdb_reads_the_parquet_files() {
    // The floor: a different Parquet implementation, in a different language,
    // sees the same rows.
    let workload = MeteringWorkload::new(START)
        .seed(0xD0CC)
        .malo_ids(4)
        .days(3);
    let (from, to) = workload.range();
    let (harness, _store, oracle) = archived(workload).await;

    let rows = duckdb(
        harness.warehouse(),
        &format!(
            "SELECT COUNT(*) AS n FROM read_parquet('{}/**/*.parquet');",
            harness.warehouse().display()
        ),
    )
    .await;

    assert_eq!(
        only(&rows).parse::<u64>().expect("a row count"),
        oracle.row_count(from, to)
    );
}

#[tokio::test]
async fn duckdb_reads_the_iceberg_table_through_its_metadata() {
    // The assertion that actually tests P2. DuckDB reads the manifests, the
    // manifest list and the snapshot — its own implementation of the spec — and
    // has to agree about which files belong to the table.
    let workload = MeteringWorkload::new(START)
        .seed(0x1CE1)
        .malo_ids(4)
        .days(3);
    let (from, to) = workload.range();
    let (harness, _store, oracle) = archived(workload).await;

    let table = table_path(&harness);
    let rows = duckdb(
        harness.warehouse(),
        &format!("SELECT COUNT(*) AS n FROM iceberg_scan('{table}');"),
    )
    .await;

    assert_eq!(
        only(&rows).parse::<u64>().expect("a row count"),
        oracle.row_count(from, to),
        "DuckDB's reading of the Iceberg metadata must agree with ours"
    );
}

#[tokio::test]
async fn duckdb_applying_the_published_sql_gets_the_right_answer() {
    // §13.7.2's mitigation against a foreign engine, which is the only setting
    // where it matters. The SQL comes from the store unmodified — the same text
    // `system.resolution` serves — with only the table name bound to an
    // `iceberg_scan` over the metadata.
    let workload = MeteringWorkload::new(START)
        .seed(0xC0FE)
        .malo_ids(4)
        .days(3)
        .with_corrections(0.2);
    let (from, to) = workload.range();
    let (harness, store, oracle) = archived(workload).await;

    let table = table_path(&harness);
    let resolution = store
        .resolution_sql()
        .replace("readings_versions", &format!("iceberg_scan('{table}')"));

    let rows = duckdb(
        harness.warehouse(),
        &format!("SELECT SUM(value) AS total FROM ({resolution}) AS resolved;"),
    )
    .await;

    let total: Decimal = only(&rows).parse().expect("a decimal total");
    assert_eq!(
        total.normalize(),
        oracle.sum_kwh(from, to).normalize(),
        "the published SQL must give a foreign engine MeterStore's own answer"
    );
}

#[tokio::test]
async fn duckdb_confirms_the_naive_query_double_counts() {
    // The hazard, demonstrated in the engine an operator would actually use.
    // §13.7.2 argues this is why the raw table is named `_versions`; here it is,
    // in DuckDB, rather than asserted.
    let workload = MeteringWorkload::new(START)
        .seed(0xBAD2)
        .malo_ids(3)
        .days(2)
        .with_corrections(0.3);
    let (from, to) = workload.range();
    let (harness, _store, oracle) = archived(workload).await;

    let table = table_path(&harness);
    let rows = duckdb(
        harness.warehouse(),
        &format!(
            "SELECT SUM(value) AS total \
             FROM iceberg_scan('{table}');"
        ),
    )
    .await;

    let naive: Decimal = only(&rows).parse().expect("a decimal total");
    assert!(
        naive.normalize() > oracle.sum_kwh(from, to).normalize(),
        "a raw sum must overstate; got {naive} against a true {}",
        oracle.sum_kwh(from, to)
    );
}

#[tokio::test]
async fn duckdb_sees_the_snapshot_history_a_reproducible_read_pins_to() {
    // Time travel is the second defensible-core claim, and it is only real if a
    // foreign engine can see the same snapshots. If DuckDB cannot enumerate
    // them, "reproduce the settlement as computed on the 8th working day" is a
    // promise only MeterStore can keep — which is the lock-in P2 rejects.
    let workload = MeteringWorkload::new(START)
        .seed(0x5AA9)
        .malo_ids(3)
        .days(3);
    let (harness, store, _oracle) = archived(workload).await;

    let ours = store.snapshots().await.expect("snapshots");
    assert!(ours.len() >= 2, "the fixture must commit several snapshots");

    let table = table_path(&harness);
    let rows = duckdb(
        harness.warehouse(),
        &format!("SELECT COUNT(*) AS n FROM iceberg_snapshots('{table}');"),
    )
    .await;

    assert_eq!(
        only(&rows).parse::<usize>().expect("a snapshot count"),
        ours.len(),
        "DuckDB must see every snapshot MeterStore committed"
    );
}

#[tokio::test]
async fn duckdb_sees_the_values_as_themselves() {
    // §7.1.1: the data is self-describing. Read by a foreign engine, quality is
    // `MEASURED` and the value is a decimal — not an integer code needing this
    // crate's source, and not a float that has already lost the settlement's
    // last place.
    let workload = MeteringWorkload::new(START)
        .seed(0x5E1F)
        .malo_ids(2)
        .days(1);
    let (harness, _store, _oracle) = archived(workload).await;

    let table = table_path(&harness);
    let rows = duckdb(
        harness.warehouse(),
        &format!(
            "SELECT quality || '|' || resolution || '|' || \
                    typeof(value) || '|' || typeof(\"from\") AS shape \
             FROM iceberg_scan('{table}') LIMIT 1;"
        ),
    )
    .await;

    let shape = only(&rows);
    let parts: Vec<&str> = shape.split('|').collect();
    assert_eq!(parts[0], "MEASURED", "quality must read as its own name");
    assert_eq!(parts[1], "PT15M", "resolution must be an ISO 8601 duration");
    assert!(
        parts[2].starts_with("DECIMAL"),
        "value must stay a decimal, got {}",
        parts[2]
    );
    assert!(
        parts[3].starts_with("TIMESTAMP"),
        "from must stay a timestamp, got {}",
        parts[3]
    );
}
