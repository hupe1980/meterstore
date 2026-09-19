//! Third-party interop: **PyIceberg** reads what MeterStore wrote.
//!
//! DuckDB (`interop_duckdb.rs`) already proves the output is not
//! `iceberg-rust`-specific: a different language, a different Parquet reader, a
//! different reading of the spec. So why a second foreign engine?
//!
//! Because PyIceberg is the one this design *tells operators to use*. Compaction
//! and orphan-file cleanup are blocked on the upstream `iceberg` crate, and the
//! documented workaround is to run them out of band with Spark or PyIceberg
//! against the same standard table. A workaround nobody has demonstrated is a
//! workaround nobody should rely on — and if PyIceberg could not open these
//! tables, the maintenance story would be a sentence with nothing behind it.
//!
//! It is also the reference implementation of the Iceberg spec in the sense that
//! matters here: it is maintained by the Iceberg project itself, so a
//! disagreement between it and `iceberg-rust` is a disagreement about the
//! *format*, not about one library's dialect.
//!
//! # What this asserts that DuckDB does not
//!
//! DuckDB reads through its own C++ Iceberg extension. PyIceberg reads through
//! the project's own Python implementation, and — the part no other suite covers
//! — it exposes the **table metadata as objects**: the schema with its field
//! IDs, the partition spec, the sort order, and the snapshot summaries where the
//! tiering watermark lives. Those are the things an out-of-band maintenance tool
//! actually manipulates.
//!
//! # Network
//!
//! `pip install pyiceberg` runs at test time, so this suite needs outbound
//! network. That is why it lives in its own file, exactly as the DuckDB suite
//! does: a run without network fails here and nowhere else, which makes the
//! cause obvious rather than mysterious.

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
const SENTINEL: &str = "__meterstore_done__";

/// The Python image, pinned for the same reason the DuckDB one is: an upstream
/// release must not be able to turn this suite red for a reason unrelated to any
/// change here.
const PYTHON_IMAGE: (&str, &str) = ("python", "3.13-slim");

/// The PyIceberg version under test. Pinned so a format regression is
/// attributable to a change here or to a specific upstream release, never to
/// "whatever pip resolved today".
const PYICEBERG: &str = "pyiceberg[pyarrow]==0.11.0";

/// Run a Python script against the mounted warehouse and return its stdout lines.
///
/// The table is opened with `StaticTable.from_metadata`, which is how a
/// maintenance tool reaches a table whose catalogue it does not share — and the
/// only route available here, because the SQL catalogue's pointer lives in
/// PostgreSQL rather than beside the files.
async fn pyiceberg(warehouse: &Path, script: &str) -> Vec<String> {
    let mount = warehouse.display().to_string();

    let program = format!(
        r#"set -eu
pip install --quiet --disable-pip-version-check '{PYICEBERG}'
python - <<'PYEOF'
{script}
print("{SENTINEL}")
PYEOF
"#
    );

    // Held until the container has been read and dropped: releasing earlier
    // would let the next test race this one for the network it is still using.
    let _slot = crate::containers::engine_slot().await;

    let container = GenericImage::new(PYTHON_IMAGE.0, PYTHON_IMAGE.1)
        .with_wait_for(WaitFor::message_on_stdout(SENTINEL))
        .with_startup_timeout(crate::containers::STARTUP_TIMEOUT)
        // Mounted at its own path: Iceberg metadata records absolute locations,
        // so mounting elsewhere would test a relocation fallback rather than the
        // paths MeterStore actually wrote.
        .with_mount(Mount::bind_mount(mount.clone(), mount))
        .with_cmd(vec!["bash".to_string(), "-c".to_string(), program])
        .start()
        .await
        .expect("start python");

    let stdout = container.stdout_to_vec().await.expect("python stdout");
    let text = String::from_utf8_lossy(&stdout).to_string();

    // A traceback goes to stderr, and a silent empty result would otherwise look
    // like a legitimate zero.
    let stderr = container.stderr_to_vec().await.unwrap_or_default();
    let errors = String::from_utf8_lossy(&stderr).to_string();
    assert!(
        !errors.contains("Traceback"),
        "pyiceberg raised:\n{errors}\n--- stdout ---\n{text}"
    );

    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && *l != SENTINEL)
        .map(str::to_string)
        .collect()
}

/// The newest metadata document, which is what an engine without the catalogue
/// has to be handed.
///
/// Picked by name, exactly as an operator would: the SQL catalogue keeps the
/// current pointer in PostgreSQL and writes no `version-hint.text` beside the
/// files. That is the same finding the DuckDB suite records as its reason for
/// needing version guessing — here it surfaces as the caller having to choose.
fn newest_metadata(harness: &TestHarness) -> String {
    let dir = harness
        .warehouse()
        .join("metering")
        .join(TestHarness::TABLE)
        .join("metadata");

    let mut candidates: Vec<_> = std::fs::read_dir(&dir)
        .expect("metadata directory")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    candidates.sort();
    candidates
        .pop()
        .expect("at least one metadata document")
        .display()
        .to_string()
}

/// A store with a workload ingested and everything archived into Iceberg.
async fn archived(workload: MeteringWorkload) -> (TestHarness, Oracle) {
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
        .admin()
        .archive(to + Duration::days(2), 64)
        .await
        .expect("archive");

    (harness, oracle)
}

/// A script that opens the table and runs `body` with `t` bound to it.
fn open(metadata: &str, body: &str) -> String {
    format!(
        r#"
from pyiceberg.table import StaticTable
t = StaticTable.from_metadata("{metadata}")
{body}
"#
    )
}

#[tokio::test]
async fn pyiceberg_reads_the_table_and_agrees_on_the_rows() {
    // The baseline claim, in the engine the maintenance story depends on: the
    // rows are there and there are exactly as many as the reference says.
    let workload = MeteringWorkload::new(START)
        .seed(0x9A11)
        .malo_ids(3)
        .days(2);
    let (harness, oracle) = archived(workload).await;

    let out = pyiceberg(
        harness.warehouse(),
        &open(
            &newest_metadata(&harness),
            "print(len(t.scan().to_arrow()))",
        ),
    )
    .await;

    let rows: usize = out.last().expect("a row count").parse().expect("a number");
    assert_eq!(
        rows as u64,
        oracle.len() as u64,
        "PyIceberg must see every archived row"
    );
}

#[tokio::test]
async fn pyiceberg_sees_the_schema_meterstore_declared() {
    // Field *ids*, not just names. Iceberg resolves columns by id, so a writer
    // that assigned them differently from what a reader expects produces a table
    // that opens and returns the wrong column — which is worse than one that
    // fails to open.
    let workload = MeteringWorkload::new(START)
        .seed(0x9A12)
        .malo_ids(1)
        .days(1);
    let (harness, _) = archived(workload).await;

    let out = pyiceberg(
        harness.warehouse(),
        &open(
            &newest_metadata(&harness),
            r#"
for f in t.schema().fields:
    print(f"{f.field_id}\t{f.name}\t{f.field_type}\t{'required' if f.required else 'optional'}")
"#,
        ),
    )
    .await;

    let by_name: std::collections::BTreeMap<&str, Vec<&str>> = out
        .iter()
        .filter_map(|line| {
            let mut parts = line.split('\t');
            let _id = parts.next()?;
            let name = parts.next()?;
            Some((name, parts.collect::<Vec<_>>()))
        })
        .collect();

    // The columns whose type a settlement depends on.
    assert_eq!(
        by_name.get("value").map(|f| f[0]),
        Some("decimal(18, 6)"),
        "the quantity must survive as an exact decimal: {out:?}"
    );
    assert_eq!(
        by_name.get("version").map(|f| f[0]),
        Some("decimal(20, 0)"),
        "an MSCONS version is 20 digits, past i64: {out:?}"
    );
    for required in ["malo_id", "obis_code", "from", "value", "unit", "sparte"] {
        assert_eq!(
            by_name.get(required).map(|f| f[1]),
            Some("required"),
            "{required} must be non-nullable: {out:?}"
        );
    }
    assert!(
        by_name.contains_key("melo_id") && by_name["melo_id"][1] == "optional",
        "a nullable column must stay nullable: {out:?}"
    );

    // `to` is optional so one schema can also describe a point table, where a
    // Zählerstand has no span end. On an *interval* table it is never null —
    // which the schema alone cannot state, so this asserts the data.
    assert_eq!(
        by_name.get("to").map(|f| f[1]),
        Some("optional"),
        "one schema describes both time models: {out:?}"
    );
    let nulls = pyiceberg(
        harness.warehouse(),
        &open(
            &newest_metadata(&harness),
            r#"
column = t.scan().to_arrow().column("to")
print(sum(1 for chunk in column.chunks for v in chunk if v is None))
"#,
        ),
    )
    .await;
    assert_eq!(
        nulls.first().map(String::as_str),
        Some("0"),
        "an interval table's rows all carry a span end: {nulls:?}"
    );
}

#[tokio::test]
async fn pyiceberg_sees_the_layout_the_design_specifies() {
    // The partition spec and sort order are what make a single-meter read cheap.
    // A foreign tool that could not see them would compact the table back into a
    // layout nothing prunes.
    let workload = MeteringWorkload::new(START)
        .seed(0x9A13)
        .malo_ids(2)
        .days(2);
    let (harness, _) = archived(workload).await;

    let out = pyiceberg(
        harness.warehouse(),
        &open(
            &newest_metadata(&harness),
            r#"
print("format-version", t.metadata.format_version)
for f in t.spec().fields:
    print("partition", f.name, f.transform)
for f in t.sort_order().fields:
    print("sort", f.source_id, f.direction)
"#,
        ),
    )
    .await;

    let joined = out.join("\n");
    assert!(
        joined.contains("format-version 2"),
        "v2 is deliberate; v3 narrows the reader set: {joined}"
    );
    assert!(
        joined.contains("partition from_month month"),
        "the cold table is partitioned by month(from): {joined}"
    );
}

#[tokio::test]
async fn pyiceberg_can_read_the_tiering_watermark() {
    // The whole recovery story rests on the watermark being *in* the snapshot
    // summary rather than in a sidecar. That is only true if an ordinary Iceberg
    // reader can get at it — an operator running maintenance out of band needs to
    // know which boundary the table is at before they touch anything.
    let workload = MeteringWorkload::new(START)
        .seed(0x9A14)
        .malo_ids(1)
        .days(2);
    let (harness, _) = archived(workload).await;

    let out = pyiceberg(
        harness.warehouse(),
        &open(
            &newest_metadata(&harness),
            r#"
s = t.metadata.current_snapshot()
print("watermark", s.summary.additional_properties.get("meterstore.tiering_watermark"))
print("range", s.summary.additional_properties.get("meterstore.archived_range"))
print("snapshots", len(t.metadata.snapshots))
"#,
        ),
    )
    .await;

    let joined = out.join("\n");
    assert!(
        joined.contains("watermark 2026-07-2"),
        "the boundary must be legible to any Iceberg reader: {joined}"
    );
    assert!(
        joined.contains("range 2026-07-2"),
        "and so must the range the commit archived: {joined}"
    );
}

#[tokio::test]
async fn the_published_resolution_sql_is_what_makes_a_sum_correct_in_pyiceberg() {
    // The hazard, demonstrated in the engine an operator would reach for, and
    // then the mitigation. A naive sum over the raw versioned rows double-counts
    // every corrected interval — silently, in a number someone bills from.
    //
    // PyIceberg has no SQL engine, so the resolution is expressed over the Arrow
    // table it returns. That is exactly what a maintenance script would do, and
    // it keeps the assertion about the *data* rather than about a dialect.
    let workload = MeteringWorkload::new(START)
        .seed(0x9A15)
        .malo_ids(3)
        .days(2)
        .with_corrections(0.3);
    let (harness, oracle) = archived(workload).await;

    let out = pyiceberg(
        harness.warehouse(),
        &open(
            &newest_metadata(&harness),
            r#"
import pyarrow.compute as pc
a = t.scan().to_arrow()

# Naive: every stored version, including superseded ones.
print("naive", pc.sum(a["value"]).as_py())

# Resolved: latest version per (malo_id, obis_code, from) within a scope —
# the same rule `store.resolution_sql()` publishes.
best = {}
for row in a.to_pylist():
    key = (row["malo_id"], row["obis_code"], row["from"], row["version_scope"])
    if key not in best or row["version"] > best[key]["version"]:
        best[key] = row
print("resolved", sum(r["value"] for r in best.values()))
print("rows", len(best))
"#,
        ),
    )
    .await;

    let value = |prefix: &str| -> Decimal {
        out.iter()
            .find_map(|l| l.strip_prefix(prefix))
            .expect(prefix)
            .trim()
            .parse()
            .expect("a decimal")
    };

    let expected = oracle.sum_kwh(START, START + Duration::days(3));
    let naive = value("naive ");
    let resolved = value("resolved ");

    assert_eq!(
        resolved, expected,
        "the published rule must reproduce MeterStore's own answer"
    );
    assert!(
        naive > expected,
        "and the naive sum must be demonstrably wrong — a mitigation for a \
         hazard nobody has shown is one nobody applies (naive {naive}, expected {expected})"
    );

    let rows: usize = out
        .iter()
        .find_map(|l| l.strip_prefix("rows "))
        .expect("a row count")
        .trim()
        .parse()
        .expect("a number");
    assert_eq!(rows, oracle.len(), "one row per reading after resolution");
}
