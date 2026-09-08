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
- **Purge is `DROP TABLE`, never `DELETE` — and deferred.** An archived window
  is exactly one partition: detached when it is read, dropped a cycle later once
  no query planned against the old boundary can still need it. Deleting a day of
  readings for 100 k meters row by row would leave ~9.6 M dead tuples for
  autovacuum.
- **Corrections are versions, not overwrites.** MSCONS corrects a value by
  *versioning* it, so the store needs only Iceberg's `append` — and a past
  settlement stays reproducible.
- **Nothing on the archival path holds a window.** Peak memory is the chunk size,
  not the ~9.6 M-row window.

[How tiering works →](https://hupe1980.github.io/meterstore/docs/architecture/)

## Quick start

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
loop as a foreground process.
[The CLI →](https://hupe1980.github.io/meterstore/docs/cli/)

As a library:

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
    WHERE malo_id = '41373559241'
      AND "from" >= '2025-01-01' AND "from" < '2026-01-01'
    GROUP BY 1 ORDER BY 1
"#).await?;

result.watermark();        // where cold ended and hot began
result.touched_hot_tier(); // whether the answer is only valid for now
```

A table declares whether it holds **spans or instants**. A *Lastgang* is energy
over `[from, to)`; a *Zählerstandsgang* is a cumulative register value at an
instant, which BK6-24-174 (in force 06.06.2025) has made a primary record exactly
as voluminous as the Lastgang differenced out of it — and § 146 Abs. 4 AO means
it cannot be discarded afterwards. Both tier the same way, because everything
that does the tiering reads the start timestamp:

```rust
TableConfig::new("meter_reads_versions").time_model(TimeModel::Point)
store.append_readings(&[zaehlerstandsgang]).await?;   // metering::MeterReading
store.readings(malo)?.melo(melo)?.latest().await?;   // what the meter reads now
```

They are never the same table: `value` is interval energy on one and a register
reading on the other, and summing the two together gives a number with no meaning
that looks exactly like a consumption total.

The shape also decides what **names** a reading. A Marktlokation may be measured
by several Messlokationen, and both meters carry `1-0:1.8.0` at the same instants.
A load profile belongs to the market location, so `melo_id` labels it; a register
belongs to the *meter*, so it joins the merge key — by default on a point table,
`identify_by_melo` either way. Keyed wrongly, two meters that agree on a number
store as one reading.

A deployment's own columns are declared the same way, and an identifier among
them is **parsed rather than trusted** — the argument that makes `malo_id` a
`MaloId` rather than eleven digits. A Bilanzkreis is an ENTSO-E EIC with a check
character; a Lieferant is a thirteen-digit Marktpartner-ID:

```rust
TableConfig::new("readings_versions")
    .identity_column(Field::new("tenant", DataType::Utf8, false))            // joins the merge key
    .attribute_column(checked_column("bilanzkreis", ValueCheck::Eic(Some(EicType::Party)), true))
    .attribute_column(checked_column("lieferant", ValueCheck::Bdew, true))   // thirteen digits, no more
```

Values are parsed and stored canonicalised, so one Bilanzkreis in two spellings
cannot become two readings that never supersede each other.

Each scheme stops somewhere different and the documentation says where: the EIC
check character and the MaLo check digit are enforced, a MeLo has no check digit
and gains the casing instead, and a Marktpartner-ID's thirteenth digit is
deliberately **not** checked — BDEW's Bildungsvorschrift exempts GS1-issued GLNs,
which is why `version_scope` does not check its operator's digit either.

A `MeasurementSeries` holds one `obis_code`, so a typed read describes **one
channel** — folding import and export together sums to twice the truth. A
measuring point is a set of them, so there is a read for that too, in one scan:

```rust
store.series(malo)?.obis("1-0:1.8.0")?.range(from, to).collect().await?;   // one channel
store.series(malo)?.range(from, to).collect_by_channel().await?;          // all of them
```

A service exposing caller-supplied SQL confines it by **injection into the plan**,
so no statement can omit, alias or `UNION` past the boundary — including across a
join, where both sides carry their own:

```rust
store.scoped("tenant", t).await?;      // rows, one table
catalog.isolated("readings").await?;   // relations
catalog.scoped("tenant", t).await?;    // rows, every table — and the join still plans
```

The last is all-or-nothing: a column missing from some table's merge key is
refused, naming that table, before any table is confined. A scope that covered
three tables and skipped the fourth is not a boundary.

`readings` is version-resolved; `readings_versions` is the raw audit trail. The
naming is load-bearing — see
[the version-resolution trap](https://hupe1980.github.io/meterstore/docs/interop/#the-version-resolution-trap)
before pointing an external engine at the warehouse.

A `SUM` over an incomplete month returns a smaller number and no reason, so
**completeness is a query**. The expected count is the DST-aware calendar's — 92
on the spring day, 100 on the autumn one, and for gas both on the Gastag rather
than the Sunday. It covers **every balancing day of the range**, so a channel
that stopped mid-month is short by every day after it stopped.

The strongest finding is the one a range cannot make about itself: a channel that
delivered *nothing* produces no rows to aggregate at all, so it needs a roster
from an earlier window.

```rust
store.completeness(from, to).await?;                              // gaps within
store.completeness(from, to).seen_since(from - month).await?;     // and what went silent
```

```bash
# The settlement period, named the way the market names one. --sparte GAS cuts
# the same span at 06:00 local, where the gas Bilanzierungsmonat begins.
meterstore completeness --month 2026-06 --seen-since 30d --gaps-only
```

Personal data comes with a clock rather than a request. § 60 Abs. 6 MsbG says
erase or anonymise *at the latest* three years after the end of the year a value
was collected in — **per value**, so the unit of erasure is a `(subject,
collection year)` pair rather than a subject. A reference is minted for one year
and the sweep expires years independently, which is the only way an active
customer's 2021 readings can stop being attributable while their 2026 readings
stay linked:

```rust
let subject = store.register_subject("customer-4821", interval.from, sparte).await?;

catalog.maintenance()
    .anonymise_after(Retention::CalendarYears(3), "§ 60 Abs. 6 MsbG", "retention-job")
    .spawn();
```

The year is in the reference, so a reference used on another year's readings is
refused at the write — otherwise nothing downstream could tell, and the only
symptom would be a sweep that never came due. The sweep itself reads no readings
at all: it is one indexed `DELETE` against the registry.

`sparte` is in it because the epoch is the year of the day a reading is
**balanced** on, and for gas that is the Gastag — 06:00 to 06:00 local. One rule,
`retention_epoch(at, sparte)`, serves the mint and the write's check, so they
cannot disagree; for everything but gas it is the Berlin calendar year.

An Article 17 request names a person, not a year or an opaque token, so that is an
entry point too:

```rust
let years = catalog.subject_epochs("customer-4821").await?;
catalog.erase_subject_by_id("customer-4821", "DSAR-2026-0042", "privacy-team", now).await?;
```

A table declaring a `subject_column` needs the registry it resolves against, and
a configuration file supplies it — one for the whole deployment, because the
mapping is:

```toml
[privacy]
erasure_secret = "${METERSTORE_ERASURE_SECRET}"   # ≥ 32 bytes; turns on suppression
```

`meterstore erasures` reads the audit trail from a shell, because *"we deleted
it"* is not evidence. Every row records which duty it discharged, so
`--since`/`--until` and `--trigger` ask for a period rather than a page. There is
deliberately no `meterstore erase`: an Article 17 request usually reaches an
application's own tables too, and those must succeed or fail in **one
transaction** with the mapping — which `erase_in` gives and a CLI invocation
cannot.

The suppression key is a **ring**: a tombstone is `HMAC(key, identifier)` and the
identifier was destroyed with it, so it can never be re-keyed and a single key
could never be rotated. `erasure_secret` writes; `retired_erasure_secrets` keep
being read.

`meter_local_day` is not a convenience, and for **gas it is the wrong function**.
`Europe/Berlin` observes daylight saving, so the UTC day boundary sits at 01:00
or 02:00 local and grouping on UTC days is wrong every day of the year — but the
German gas market does not balance on the calendar day either. A *Gastag* runs
06:00 to 06:00 local, so a gas Lastgang grouped by the calendar day books six
hours a day into the neighbouring Bilanzierungstag, with totals that still look
plausible. `meter_balancing_day("from", sparte)` reads the commodity per row and
picks the right one:

```sql
SELECT sparte, meter_balancing_day("from", sparte) AS day, SUM(value)
FROM readings GROUP BY 1, 2;
```

The DST anomaly moves with the boundary: the clocks change *before* 06:00, so the
25-hour gas day is the one named after the **Saturday** while the 25-hour
calendar day is the Sunday.

And the boundary carries up to the **month**. The gas Bilanzierungsmonat runs
01.06 06:00 to 01.07 06:00 (EDI@Energy *Allgemeine Festlegungen* v6.1c, Kap. 3.1),
so an MSCONS version scope for a gas row is cut at 06:00 as well — which is why
every `VersionScope` constructor takes a `Sparte`:

```rust
// The network operator's Marktpartner-ID, parsed — because a wrong-but-plausible
// one is not an error, it is a different scope that nothing else shares.
VersionScope::for_interval("9900000000001", interval.from, Sparte::Gas)?
```

The month has its own function for the same reason the day does:
`meter_local_month` is the *calendar* month for every row, so grouping a gas
Lastgang by it books six hours into the neighbouring Bilanzierungsmonat twelve
times a year. `meter_balancing_month("from", sparte)` reads the commodity.

**An external engine does not get that function — so it gets the answer instead.**
SQL dialects differ on timestamp arithmetic, so no single published expression is
right everywhere. The encoder applies the calendar once, at write time, and stores
the answer:

```sql
-- Every engine. No zone conversion, no DST reasoning, no dialect.
SELECT balancing_day, SUM(value) FROM readings GROUP BY 1;

-- And the settlement month, because a Bilanzierungsmonat is a whole number of
-- balancing days — so this is a DATE operation rather than a calendar one.
SELECT date_trunc('month', balancing_day), SUM(value) FROM readings GROUP BY 1;
```

Direction is three-valued, not two booleans. **Both** `obis_is_import` and
`obis_is_export` are false for a register that has no direction at all —
Blindarbeit, a gas volume, a Zustandszahl — so `NOT obis_is_import(...)` sweeps
those in with the feed-in. `obis_direction(obis_code)` returns `'IMPORT'`,
`'EXPORT'` or null.

A Bilanzkreis and a Bilanzierungsgebiet share the alphabet, the length and the
check character; only **position 3** — the ENTSO-E object type — tells them apart.
`ValueCheck::Eic(Some(EicType::Party))` (`check = "EIC:X"` in TOML) pins it, and
unlike the check character that half is a regular expression, so PostgreSQL
enforces it on rows this crate did not write.

For a column that does not declare one, it is a query — and `eic_normalise` is
what tells *"not an EIC"* apart from *"an EIC whose type letter this build does
not list"*, which is the row a stricter parser downstream will reject:

```sql
SELECT DISTINCT bilanzkreis FROM readings
WHERE eic_normalise(bilanzkreis) IS NOT NULL
  AND eic_object_type(bilanzkreis) IS NULL;
```

[Getting started →](https://hupe1980.github.io/meterstore/docs/getting-started/)

## Requirements

| | Version | Why |
|---|---|---|
| Rust | 1.94 | Set by the dependency floor (`metering`, `iceberg`) |
| PostgreSQL | **12 or later** | `ATTACH PARTITION` takes only `SHARE UPDATE EXCLUSIVE` on the parent from 12 — see below |
| `metering` | **0.23 or later** | The domain layer — MeterStore stores its types, it does not redefine them |
| Apache Iceberg | format v2 | [Deliberately not v3](https://hupe1980.github.io/meterstore/docs/architecture/#format-version) |

Partition creation runs on the write path, and `CREATE TABLE … PARTITION OF`
takes `ACCESS EXCLUSIVE` on the parent — which, since PostgreSQL grants locks in
arrival order, lets one long query stall every subsequent insert. Partitions are
built standalone and *attached* instead.
[Locks →](https://hupe1980.github.io/meterstore/docs/operations/#locks-and-why-ddl-gives-up)

The cold tier takes **any** `Arc<dyn Catalog>` — SQL, REST, Polaris, Lakekeeper,
Glue — and that seam is driven end to end by the test suite rather than asserted.
Three are built for you: a PostgreSQL-backed SQL catalogue on the same database as
the hot tier (`sql-catalog`, on by default), a REST catalogue (`rest-catalog`, on
by default), and AWS S3 Tables behind the `s3tables` feature. A configuration file
builds whichever it names — all three, by `catalog = "sql" | "rest" | "s3tables"` —
and `Settings::connect()` returns the pool, both tiers and every validated table.
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
flags, DST-correct calendars, the identifiers (`MaloId`, `MeloId`, `BdewCode`, `Eic`),
validation, Ersatzwertbildung, gas conversion and aggregation. MeterStore adds
exactly three things: **correction versioning**, the **transaction-time axis**,
and the **tiering boundary**.

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
| [Completeness](https://hupe1980.github.io/meterstore/docs/completeness/) | DST-aware gap detection, including the channel that delivered nothing |
| [Operations](https://hupe1980.github.io/meterstore/docs/operations/) | Scheduling, locks, system tables, metrics, failure matrix |
| [The CLI](https://hupe1980.github.io/meterstore/docs/cli/) | `meterstore` — check, create, status, archive, maintain, query, serve |
| [External engines](https://hupe1980.github.io/meterstore/docs/interop/) | Spark, Trino, DuckDB — and the trap to avoid |
| [Privacy and retention](https://hupe1980.github.io/meterstore/docs/privacy/) | Pseudonymisation, and the three-year duty as a scheduled job |
| [Configuration](https://hupe1980.github.io/meterstore/docs/configuration/) | TOML over the same validated types |

## Status

Everything the documentation describes works end to end against real
infrastructure — both tiers, streaming archival, tier-split queries, reproducible
reads, completeness, multi-table sessions and both serving surfaces.

**950 tests**: unit, property, doc and integration against real PostgreSQL 16 and
a real Iceberg warehouse, plus an independently implemented correctness oracle over
generated workloads, covering both record shapes. Ingest, archival and reads also
run **against one table at once**, which is the only way to reach the states that
exist between two steps rather than inside one. **DuckDB** and **PyIceberg** read
the output and agree with it, down to the audit trail's timestamps. The lock
behaviour is asserted against a real server holding a real conflicting lock, not
argued. Compression against PostgreSQL row storage is
**measured** rather than targeted — ~109× (457 B/row against 4.2 B/row; the
measurement suite carries the caveats).

Missing: query-latency benchmarks on reference hardware, so the p99 targets remain
aspirational; Spark and Trino interop; a soak across two replicas, which is the
shape the archive lease exists for. Compaction and general orphan-file cleanup
[run out of band](https://hupe1980.github.io/meterstore/docs/operations/#compaction),
because `iceberg-rust` exposes neither.

## Development

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
error rather than as "no space left". A container per test cost seven times the
wall clock of a database per test, for the same isolation.

`datafusion`, `arrow`, `iceberg`, `parquet`, `metering`, `time`, `rust_decimal`,
`sqlx` and `sqlx-postgres` must each appear exactly once in the dependency graph;
`just deps` fails the build otherwise. Two versions of `arrow` mean two incompatible
`RecordBatch` types, and two of `sqlx` mean two incompatible `PgPool` types —
neither fails obviously.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option. Part of the [mako](https://github.com/hupe1980/mako) platform.
