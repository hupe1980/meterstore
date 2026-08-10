# MeterStore

**Hot/cold tiered storage for metering time series.** PostgreSQL holds the recent
interval window at low latency; Apache Iceberg holds the history at analytical
scale. A single explicit timestamp separates them, and one SQL statement spans
both.

[![CI](https://github.com/hupe1980/meterstore/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/meterstore/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

📖 **[Documentation](https://hupe1980.github.io/meterstore)** · [API reference](https://docs.rs/meterstore) · [Changelog](CHANGELOG.md)

> **Pre-alpha, and unpublished on purpose.** Storage, tiering, archival,
> querying, reproducible reads and completeness work end to end against real
> PostgreSQL 16 and a real Iceberg warehouse. The API is still settling;
> integrating against a real workload is what settles it.

---

## The problem

An intelligent measuring system produces one value per measuring point, per OBIS
code, per interval. Fifteen minutes is the German settlement grain:

| Scale | Rows/day | Rows/year |
|-------|----------|-----------|
| 10 k measuring points | ~1 M | ~350 M |
| 100 k (mid-size utility) | ~9.6 M | ~3.5 B |
| 1 M (metering operator) | ~96 M | ~35 B |

Retention is regulatory — years to decades for the settlement record. PostgreSQL
handles the first row comfortably, the second with care, and the third not at all
without becoming a full-time job.

But the operational workload genuinely needs Postgres: recent data is written
continuously, corrected, and read transactionally by billing and market
communication. Meanwhile settlement, forecasting and grid analysis scan years
across hundreds of thousands of meters — an object-storage-and-columnar-format
problem.

**The data has a natural split most systems refuse to exploit:** recent intervals
are hot and still being corrected; historical intervals are cold and settled. The
boundary between them is a timestamp.

## How it works

```
  MeterInterval.from ─────────────────────────────────────▶

  │◀──── Iceberg (cold, settled) ────▶│
                                      │◀── Postgres (hot) ──▶│
  epoch                       tiering_watermark            now
```

A row's interval start alone decides its tier, so the tiers are disjoint by
construction — no deduplication, no merge, no double-counting. Four decisions
carry most of the weight:

- **The watermark lives inside the Iceberg snapshot.** Archival writes the tier
  boundary into the snapshot summary in the same commit as the data. Iceberg
  commits are a compare-and-swap, so rows and boundary become durable together or
  not at all.
- **Purge is `DROP TABLE`, never `DELETE`.** The hot table is time-partitioned, so
  archiving a window drops exactly one partition. Deleting a day of readings for
  100 k meters row by row would leave ~9.6 M dead tuples for autovacuum.
- **Corrections are versions, not overwrites.** MSCONS corrects a value by
  *versioning* it, so the store needs only Iceberg's `append` — and a past
  settlement stays reproducible.
- **Nothing on the archival path holds a window.** Peak memory is the chunk size,
  not the ~9.6 M-row window.

[How tiering works →](https://hupe1980.github.io/meterstore/docs/architecture/)

## Quick start

```bash
cargo add meterstore
```

```rust
use meterstore::prelude::*;
use meterstore::hot::PostgresHot;
use std::sync::Arc;
use time::Duration;

let hot = Arc::new(PostgresHot::new(pool));   // a pool you already own

let cold = IcebergSqlCatalog {
    database_url: &db_url,
    warehouse_uri: "s3://bucket/warehouse",   // or file:// memory:// gs:// abfss://
    catalog_name: "meterstore",
    namespace: "metering",
    file_target_bytes: 512 * 1024 * 1024,
    metadata_pool_max_connections: 4,
    auth: &WarehouseAuth { region: Some("eu-central-1".into()), ..Default::default() },
}.build().await?.cold();

let store = MeterStore::builder()
    .hot(hot)
    .cold(cold.clone(), cold.table_provider("readings_versions").await?)
    .table(
        TableConfig::new("readings_versions")
            .settlement_lag(Duration::days(7))
            .archival_step(Duration::DAY)
            .build()?,
    )
    .build()
    .await?;

store.create_tables().await?;
```

Then write and read:

```rust
// Routes each interval to the tier that owns it.
store.append(&[stored_series]).await?;

// One statement, both tiers — with the boundary it was computed against.
let result = store.query(r#"
    SELECT meter_local_day("from") AS day, SUM(value) AS kwh
    FROM readings
    WHERE malo_id = '12345678901'
      AND "from" >= '2025-01-01' AND "from" < '2026-01-01'
    GROUP BY 1 ORDER BY 1
"#).await?;

result.watermark();        // where cold ended and hot began
result.touched_hot_tier(); // whether the answer is only valid for now
```

`readings` is version-resolved; `readings_versions` is the raw audit trail. The
naming is load-bearing — see
[the version-resolution trap](https://hupe1980.github.io/meterstore/docs/interop/#the-version-resolution-trap)
before pointing an external engine at the warehouse.

[Getting started →](https://hupe1980.github.io/meterstore/docs/getting-started/)

## Requirements

| | Version | Why |
|---|---|---|
| Rust | 1.94 | Set by the dependency floor (`metering`, `iceberg`) |
| PostgreSQL | **14 or later** | Declarative range partitioning, so a purge is `DETACH` + `DROP TABLE` |
| `metering` | 0.17 or later | The domain layer — MeterStore stores its types, it does not redefine them |
| Apache Iceberg | format v2 | [Deliberately not v3](https://hupe1980.github.io/meterstore/docs/architecture/#format-version) |

The cold tier takes **any** `Arc<dyn Catalog>` — SQL, REST, Polaris, Lakekeeper,
Glue — and that seam is driven end to end by the test suite rather than asserted.
Two are built for you: a PostgreSQL-backed SQL catalogue on the same database as
the hot tier, and AWS S3 Tables behind the `s3tables` feature.
[Details](https://hupe1980.github.io/meterstore/docs/getting-started/).

MeterStore needs only `SELECT` plus ownership of its own tables: no server
configuration, no restart, no extension. That is what makes it deployable on RDS,
Cloud SQL and Azure Postgres, where an extension-based approach is not.

## Relationship to `metering`

```
metering    → what a measurement is, and how to compute with it   (zero I/O, no async)
meterstore  → where it lives, how it is tiered, how it is queried  (all I/O)
```

[`metering`](https://crates.io/crates/metering) owns intervals, units, quality
flags, DST-correct calendars, validation, Ersatzwertbildung, gas conversion and
aggregation. MeterStore adds exactly three things: **correction versioning**, the
**transaction-time axis**, and the **tiering boundary**.

That boundary is deliberate. Duplicating a domain rule here — a unit conversion, a
DST calendar — would create a second implementation to keep correct, and it would
drift.

## Documentation

| | |
|---|---|
| [Getting started](https://hupe1980.github.io/meterstore/docs/getting-started/) | Requirements, install, a store over both tiers |
| [Architecture](https://hupe1980.github.io/meterstore/docs/architecture/) | The watermark, the invariant, crash-safe archival |
| [Storage model](https://hupe1980.github.io/meterstore/docs/storage-model/) | Columns, the merge key, identity vs attribute, constraints |
| [Writing readings](https://hupe1980.github.io/meterstore/docs/writing/) | Routed writes, bulk ingest, idempotent redelivery |
| [Querying](https://hupe1980.github.io/meterstore/docs/querying/) | SQL across tiers, provenance, the typed series API |
| [Reproducibility](https://hupe1980.github.io/meterstore/docs/reproducibility/) | Settlement reruns on two independent time axes |
| [Completeness](https://hupe1980.github.io/meterstore/docs/completeness/) | DST-aware gap detection as a first-class query |
| [Operations](https://hupe1980.github.io/meterstore/docs/operations/) | Scheduling, system tables, metrics, failure matrix |
| [External engines](https://hupe1980.github.io/meterstore/docs/interop/) | Spark, Trino, DuckDB — and the trap to avoid |
| [Privacy and retention](https://hupe1980.github.io/meterstore/docs/privacy/) | Pseudonymisation and the three-year duty |
| [Configuration](https://hupe1980.github.io/meterstore/docs/configuration/) | TOML over the same validated types |

## Status

Working end to end against real infrastructure: encoding, both tiers, streaming
archival, tier-split queries, version resolution with statistics-based elision,
reproducible reads on both time axes, completeness, multi-table sessions, routed
writes, erasure, schema quarantine, and both serving surfaces.

**554 tests** — unit, property, and integration against real PostgreSQL 16 and a
real Iceberg warehouse, plus an independently implemented correctness oracle over
generated workloads. The open-format claim is checked by two foreign engines:
**DuckDB** reads the Parquet and the Iceberg metadata, and **PyIceberg** — the
Iceberg project's own implementation — reads the schema with its field ids, the
partition spec, the format version, and the tiering watermark out of the snapshot
summary.

Two claims are measured rather than targeted:

- **Compression vs PostgreSQL row storage: ~109×** (457 B/row against 4.2 B/row),
  measured over the whole partition tree including indexes. Read it with the
  caveats in the measurement suite — the fixture's low cardinality flatters it —
  but the >10× target is met with room to spare.
- **Archival memory is bounded by the chunk, not the window**, structurally: no
  stage on the path holds one.

Not yet done: query-latency benchmarks on reference hardware, so the p99 targets
remain aspirational; interop against Spark and Trino; a CLI. Compaction
and orphan-file cleanup are
[blocked upstream](https://hupe1980.github.io/meterstore/docs/operations/#maintenance-that-is-not-implemented)
rather than deferred.

## Development

Requires a Rust toolchain and, for integration tests, a running Docker daemon.

```bash
just            # list all recipes
just dev        # format + unit tests (no Docker)
just test       # full suite
just check      # everything CI runs
just site       # serve the documentation site
```

The integration suites are **one test binary against one PostgreSQL container**.
Cargo would otherwise compile each file under `tests/` into its own statically
linked executable — twenty-odd full copies of DataFusion, Arrow, Iceberg, sqlx and
tonic, about 9 GB, which exhausts a CI runner's disk and fails as a linker bus
error rather than as "no space left". A container per test cost seven times the
wall clock of a database per test, for the same isolation.

`datafusion`, `arrow`, `iceberg`, `parquet`, `metering`, `time`, `rust_decimal`
and `sqlx` must each appear exactly once in the dependency graph; `just deps`
fails the build otherwise. Two versions of `arrow` mean two incompatible
`RecordBatch` types, and two of `sqlx` mean two incompatible `PgPool` types —
neither fails obviously.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option. Part of the [mako](https://github.com/hupe1980/mako) platform.
