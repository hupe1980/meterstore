# MeterStore

**Hot/cold tiered storage for metering time series.** PostgreSQL holds the recent
interval window at low latency; Apache Iceberg holds the history at analytical
scale. A single explicit timestamp separates them, and one SQL statement spans
both.

[![CI](https://github.com/hupe1980/meterstore/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/meterstore/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

📖 **[Documentation](https://hupe1980.github.io/meterstore)** · [API reference](https://docs.rs/meterstore) · [Changelog](CHANGELOG.md)

> **Pre-1.0.** Releases can change the Rust API and SQL schema.

---

## 🧱 The problem

An intelligent measuring system produces one value per measuring point, per OBIS
code, per interval. Fifteen minutes is the German settlement grain:

| Scale | Rows/day | Rows/year |
|-------|----------|-----------|
| 10 k measuring points | ~1 M | ~350 M |
| 100 k (mid-size utility) | ~9.6 M | ~3.5 B |
| 1 M (metering operator) | ~96 M | ~35 B |

Recent readings need transactional writes. Settlement and forecasting scan
years of history. MeterStore routes both workloads by interval start.

## ⚙️ How it works

```
  MeterInterval::from() ──────────────────────────────────▶

  │◀──── Iceberg (cold, settled) ────▶│
                                      │◀── Postgres (hot) ──▶│
  epoch                       tiering_watermark            now
```

A row's interval start alone decides its tier, so the tiers are disjoint by
construction — no deduplication, no merge, no double-counting. Four decisions
carry most of the weight:

- 🔒 **The watermark lives inside the Iceberg snapshot.** Archival writes the tier
  boundary into the snapshot summary in the same commit as the data. Iceberg
  commits are a compare-and-swap, so rows and boundary become durable together or
  not at all.
- 🗑️ **Purge is `DROP TABLE`, never `DELETE` — and deferred.** An archived window
  is exactly one partition: detached when it is read, dropped a cycle later once
  no query planned against the old boundary can still need it. Deleting a day of
  readings for 100 k meters row by row would leave ~9.6 M dead tuples for
  autovacuum.
- 🧾 **Corrections are versions, not overwrites.** MSCONS corrects a value by
  *versioning* it, so the store needs only Iceberg's `append` — and a past
  settlement stays reproducible.
- 🌊 **Nothing on the archival path holds a window.** Peak memory is the chunk size,
  not the ~9.6 M-row window.

[How tiering works →](https://hupe1980.github.io/meterstore/docs/architecture/)

## 🚀 Quick start

Without writing a program:

```bash
cargo install meterstore --features cli

# Set DATABASE_URL to the PostgreSQL connection URL before validation.
meterstore init      # a commented starter configuration
meterstore check     # full validation — no database needed
meterstore create    # both tiers, every declared table
meterstore status    # boundary, lag, write runway, health
```

Use `meterstore query` for SQL, `meterstore completeness` to check coverage, and
`meterstore maintain` to run archival. [CLI reference](https://hupe1980.github.io/meterstore/docs/cli/).

As a library:

```bash
cargo add meterstore
```

```rust
use meterstore::prelude::*;

let deployment = Settings::from_path("meterstore.toml")?.connect().await?;
let store = deployment.store().await?;
```

Then write and read:

```rust
// Routes each interval to the tier that owns it.
store.append(&[stored_series]).await?;

// One statement, both tiers — with the boundary it was computed against.
let result = store.query(r#"
    SELECT meter_local_day("from") AS day, SUM(value) AS kwh
    FROM readings
    WHERE malo_id = '41373559241'
      AND "from" >= '2025-01-01' AND "from" < '2026-01-01'
    GROUP BY 1 ORDER BY 1
"#).await?;

result.watermark();        // where cold ended and hot began
result.touched_hot_tier(); // whether the answer is only valid for now
```

For settlement ingestion, `require_single_tier(true)` rejects a delivery across
the watermark before either tier publishes rows. Use `assert_on` to select native
quality, span and unit for replay checks, and `assertion_column` for an
application field such as price. Set `value_scale(7)` for **0.0000001 kWh**.

Use `collect_deliveries()` to read each winning slot with its original metadata
and version. A one-interval `correct_if_current` publishes the successor only
while that predecessor is current. An interrupted multi-round append reports
confirmed effects through `Error::PartialAppend`.
[Configure the settlement writer](https://hupe1980.github.io/meterstore/docs/writing/#settlement-ingestion).

## 📋 Requirements

| | Version | Why |
|---|---|---|
| Rust | 1.95 | Minimum supported toolchain |
| PostgreSQL | **15 or later** | Integration-tested on 15 and 18 |
| `metering` | **0.27** | The domain layer that supplies MeterStore's public measurement types |
| Apache Iceberg | format v2 | [Deliberately not v3](https://hupe1980.github.io/meterstore/docs/architecture/#format-version) |

The default features support SQL and REST Iceberg catalogs. Enable `s3tables`
for AWS S3 Tables or the matching `object-store-*` feature for cloud storage.

PostgreSQL provisioning needs `CREATE` on the schema and permission to install
`btree_gist` when integrity constraints are enabled. Unified reads need
`INSERT` and `DELETE` on `meterstore_pins`. Connections default to `verify-full`.
[Deployment requirements](https://hupe1980.github.io/meterstore/docs/getting-started/#requirements).

## 🔗 Relationship to `metering`

```
metering    → what a measurement is, and how to compute with it   (zero I/O, no async)
meterstore  → where it lives, how it is tiered, how it is queried  (all I/O)
```

[`metering`](https://crates.io/crates/metering) owns intervals, units, quality
flags, DST-correct calendars, the identifiers (`MaloId`, `MeloId`, `BdewCode`, `Eic`),
validation, Ersatzwertbildung, gas conversion and aggregation. MeterStore adds
exactly three things: **correction versioning**, the **transaction-time axis**,
and the **tiering boundary**.

## 📚 Documentation

| | |
|---|---|
| 🚀 [Getting started](https://hupe1980.github.io/meterstore/docs/getting-started/) | Requirements, install, a store over both tiers |
| ⚙️ [Architecture](https://hupe1980.github.io/meterstore/docs/architecture/) | The watermark, the invariant, crash-safe archival |
| 🗄️ [Storage model](https://hupe1980.github.io/meterstore/docs/storage-model/) | Columns, the merge key, identity vs attribute, constraints |
| ✍️ [Writing readings](https://hupe1980.github.io/meterstore/docs/writing/) | Routed writes, bulk ingest, idempotent redelivery |
| 🔍 [Querying](https://hupe1980.github.io/meterstore/docs/querying/) | SQL across tiers, provenance, the typed series API |
| ⏱️ [Reproducibility](https://hupe1980.github.io/meterstore/docs/reproducibility/) | Settlement reruns on two independent time axes |
| 🧩 [Completeness](https://hupe1980.github.io/meterstore/docs/completeness/) | DST-aware gap detection, including the channel that delivered nothing |
| 🔧 [Operations](https://hupe1980.github.io/meterstore/docs/operations/) | Scheduling, locks, system tables, metrics, failure matrix |
| 💻 [The CLI](https://hupe1980.github.io/meterstore/docs/cli/) | `meterstore` — check, create, status, archive, maintain, query, serve, and the rest |
| 🔌 [External engines](https://hupe1980.github.io/meterstore/docs/interop/) | Spark, Trino, DuckDB — and the trap to avoid |
| 🛡️ [Privacy and retention](https://hupe1980.github.io/meterstore/docs/privacy/) | Pseudonymisation, and the three-year duty as a scheduled job |
| 📝 [Configuration](https://hupe1980.github.io/meterstore/docs/configuration/) | TOML over the same validated types |

## 📊 Status

**1087 tests** — unit, property, doc and integration against real PostgreSQL 18 and
a real Iceberg warehouse, plus an independently implemented correctness oracle
over generated workloads:

| Coverage | Verification |
|---|---|
| **Concurrency** | Ingest, archival and reads run against one table at once — the only way to reach the states between two steps rather than inside one — and two replicas over separate pools race to archive it |
| **Foreign readers** | DuckDB and PyIceberg read the output and agree with it, down to the audit trail's timestamps |
| **Foreign clients** | An ADBC driver drives Flight SQL from the API a non-Rust consumer actually holds |
| **Locks** | Asserted against a real server holding a real conflicting lock |
| **The version floor** | The whole integration suite runs again on PostgreSQL 15 |
| **Compression** | Measured rather than targeted: ~109× (457 B/row against 4.2 B/row; the measurement suite carries the caveats) |

The retention sweep removes the subject mapping. The stored `malo_id` remains
personal data. See [privacy and retention](https://hupe1980.github.io/meterstore/docs/privacy/).

On PostgreSQL, set `max_pin_age` above the longest query you run. A query that
outlives its pin can fail if archival reclaims a partition it still needs.

## 🛠️ Development

Requires a Rust toolchain and, for integration tests, a running Docker daemon.

```bash
just            # list all recipes
just dev        # format + unit tests (no Docker)
just test       # full suite
just check      # everything CI runs
just cli status # run the command-line tool from source
just site       # serve the documentation site
```

The integration tests share a PostgreSQL container and use a separate database per test.
`just deps` checks the dependency graph for incompatible copies of core storage
types. The Iceberg SQL catalog uses SQLx 0.8 alongside the hot tier’s private
SQLx 0.9 pool.

## ⚖️ License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option. Part of the [mako](https://github.com/hupe1980/mako) platform.
