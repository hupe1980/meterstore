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

meterstore init      # a commented starter configuration
meterstore check     # full validation — no database needed
meterstore create    # both tiers, every declared table
meterstore status    # boundary, lag, write runway, health
```

`meterstore query` runs SQL across both tiers and prints the boundary the answer
was computed against; `meterstore completeness --month 2026-06` reports which
channels are short before a settlement run trusts a `SUM`; `meterstore erasures`
prints the audit trail a regulator asks for; `meterstore maintain` is the archival
loop as a foreground process, and `meterstore reassert-watermark` is the step to
run before somebody else's maintenance tool expires anything.
[The CLI →](https://hupe1980.github.io/meterstore/docs/cli/)

As a library:

```bash
cargo add meterstore
```

```rust
use meterstore::prelude::*;
use std::sync::Arc;
use time::Duration;

// The store opens its own pool; no `sqlx` type is in the API.
let database = Database::connect_with(&db_url, &DatabaseOptions::new().max_connections(16)).await?;
let hot = Arc::new(PostgresHot::new(database));

let cold = IcebergSqlCatalog {
    database_url: &db_url,
    warehouse_uri: "s3://bucket/warehouse",   // or file:// memory:// gs:// abfss://
    catalog_name: "meterstore",
    namespace: "metering",
    file_target_bytes: 512 * 1024 * 1024,
    metadata_pool_max_connections: 4,
    auth: &WarehouseAuth { region: Some("eu-central-1".into()), ..Default::default() },
}.build().await?.cold();

let table = TableConfig::new("readings_versions")
    .settlement_lag(Duration::days(7))
    .archival_step(Duration::DAY)
    .build()?;
cold.create_table_for(&table).await?;
let provider = cold.table_provider(table.name()).await?;
let store = MeterStore::builder()
    .hot(hot)
    .cold(cold, provider)
    .table(table)
    .build()
    .await?;
store.admin().create_tables().await?;
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

For settlement ingestion, set `require_single_tier(true)` to reject a delivery
that crosses the watermark before either tier publishes rows. Declare a price
with `assertion_column` to detect a changed price on replay. Set `value_scale(7)`
to round-trip **0.0000001 kWh** through archival and historical queries.
[Configure the settlement writer](https://hupe1980.github.io/meterstore/docs/writing/#settlement-ingestion).

Record shapes, identity versus attribute columns, checked identifiers, scoping,
completeness, erasure and settlement reruns are on the documentation site.

[Getting started →](https://hupe1980.github.io/meterstore/docs/getting-started/)

## 📋 Requirements

| | Version | Why |
|---|---|---|
| Rust | 1.95 | Set by `iceberg` 0.11's floor; `metering` and this crate's own syntax need 1.88 |
| PostgreSQL | **15 or later** | A support floor, not a syntax one — see below. The suite runs on 18 and whole again on 15 |
| `metering` | **0.26** | The domain layer — MeterStore stores its types, it does not redefine them |
| Apache Iceberg | format v2 | [Deliberately not v3](https://hupe1980.github.io/meterstore/docs/architecture/#format-version) |

Nothing here needs more than PostgreSQL 12: attaching a standalone-built
partition takes only `SHARE UPDATE EXCLUSIVE`, where `CREATE TABLE … PARTITION OF`
takes `ACCESS EXCLUSIVE` on the parent and would stall every insert behind one
long query
([Locks →](https://hupe1980.github.io/meterstore/docs/operations/#locks-and-why-ddl-gives-up)).
The floor is **15** because a support promise must name versions still patched;
CI runs the whole integration suite on 15 and 18.

The cold tier takes **any** `Arc<dyn Catalog>` — SQL, REST, Polaris, Lakekeeper,
Glue — and that seam is driven end to end by the test suite rather than asserted.
Three are built for you: a PostgreSQL-backed SQL catalogue on the same database as
the hot tier (`sql-catalog`, on by default), a REST catalogue (`rest-catalog`, on
by default), and AWS S3 Tables behind the `s3tables` feature. A configuration file
builds whichever it names — all three, by `catalog = "sql" | "rest" | "s3tables"` —
and `Settings::connect()` returns the store's `Database`, both tiers and every
validated table.
[Details](https://hupe1980.github.io/meterstore/docs/getting-started/).

MeterStore needs no server configuration, no restart, no superuser and no
extension beyond `btree_gist`, which ships in contrib, is *trusted*, and is created
on demand. That is what makes it deployable on RDS, Cloud SQL and Azure Postgres,
where an extension-based approach is not. It does need more than `SELECT`: `CREATE`
on the schema for its relations and on the database for the extension, and
`INSERT`/`DELETE` on `meterstore_pins` for every role that queries — a read-only
role cannot query the unified view. Connections use TLS: a URL naming no
`sslmode` connects with `verify-full`, and `prefer`/`allow` are refused.
[Requirements →](https://hupe1980.github.io/meterstore/docs/getting-started/#requirements)

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

That boundary is deliberate. Duplicating a domain rule here — a unit conversion, a
DST calendar — would create a second implementation to keep correct, and it would
drift.

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

**1075 tests** — unit, property, doc and integration against real PostgreSQL 18 and
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

The integration suites are **one test binary against one PostgreSQL container**.
Cargo would otherwise compile each file under `tests/` into its own statically
linked executable — twenty-odd full copies of DataFusion, Arrow, Iceberg, sqlx and
tonic, about 9 GB, which exhausts a CI runner's disk and fails as a linker bus
error rather than as "no space left". A database per test gives the isolation of a
container per test at a seventh of the wall clock.

`datafusion`, `arrow`, `iceberg`, `parquet`, `metering`, `time`, `rust_decimal`,
`sqlx` and `sqlx-postgres` must each appear exactly once in the dependency graph;
`just deps` fails the build otherwise. Two versions of `arrow` mean two incompatible
`RecordBatch` types, which does not fail obviously. The one admitted duplicate is
the `sqlx` 0.8 `iceberg-catalog-sql` uses for its own metadata pool. `sqlx` is
private — the store opens its own pool — so its version never constrains yours.

## ⚖️ License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option. Part of the [mako](https://github.com/hupe1980/mako) platform.
