# MeterStore

**Hot/cold tiered storage for metering time series.** PostgreSQL holds the recent interval window at low latency; Apache Iceberg holds the history at analytical scale. A single explicit timestamp separates them.

[![CI](https://github.com/hupe1980/meterstore/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/meterstore/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

> **Pre-alpha.** Storage, tiering, archival, querying, reproducible reads and completeness work end to end, covered by integration tests against real PostgreSQL 16 and a real Iceberg warehouse. A single SQL statement already spans both tiers, the whole store is checked against an independently implemented reference, and the compression claim is **measured** rather than asserted — see [Status](#status).

---

## The problem

An intelligent measuring system produces one value per measuring point, per OBIS code, per interval. Fifteen minutes is the settlement grain, and the one the table below is sized for — but iMSys already delivers finer, and MeterStore holds whatever resolution a series declares:

| Scale | Rows/day | Rows/year |
|-------|----------|-----------|
| 10 k measuring points | ~1 M | ~350 M |
| 100 k (mid-size utility) | ~9.6 M | ~3.5 B |
| 1 M (metering operator) | ~96 M | ~35 B |

At one value a minute the same population is fifteen times those numbers.

Retention is regulatory — years to decades, and nothing is ever deleted. PostgreSQL handles the first row of that table comfortably, the second with care, and the third not at all without becoming a full-time job.

But the operational workload genuinely needs Postgres: recent data is written continuously, corrected, and read transactionally by billing and market-communication processes. Meanwhile settlement, forecasting and regulatory reporting scan years across hundreds of thousands of meters — an object-storage-and-columnar-format problem.

The data has a natural split most systems refuse to exploit: **recent intervals are hot and still being corrected; historical intervals are cold and settled.** The boundary between them is a timestamp.

## How it works

```
  MeterInterval.from ──────────────────────────────────────────▶

  │◀──────── Iceberg (cold, settled) ────────▶│
                                              │◀── Postgres (hot) ──▶│
  epoch                              tiering_watermark            now
```

One timestamp per table. Everything below it lives in Iceberg; everything at or above it lives in Postgres. A row's interval start alone decides its tier, so the tiers are disjoint by construction — no deduplication, no merge, no double-counting.

Three design choices carry most of the weight:

**The watermark lives inside the Iceberg snapshot.** Archival writes the tier boundary into the snapshot summary, in the same commit as the data it describes. Iceberg commits are a compare-and-swap, so the rows and the watermark become durable together or not at all. There is no external checkpoint store to fall out of sync, and recovery is just reading the watermark back.

**Purge is `DROP TABLE`, never `DELETE`.** The hot table is time-partitioned, so archiving a window detaches and drops exactly one partition — an O(1) catalog operation. Deleting a day of readings for 100 k meters row-by-row would leave ~9.6 M dead tuples for autovacuum to clean up, competing with the workload the tiering exists to protect.

**Nothing on the archival path holds a window.** A day at 100 k measuring points is ~9.6 M rows. The detached partition is paged by keyset, the Parquet writer consumes the stream, and rows are counted as they pass rather than collected to count — so peak memory is the chunk size, not the window. The page cursor is the full primary key rather than `(malo_id, from)`, which is not unique: one interval carries a row per OBIS channel and per correction version, and a cursor that ties drops the remainder of the tie.

**Corrections are versions, not overwrites.** MSCONS specifies that a correction is made by versioning the value, so nothing is ever updated in place. That means the store only ever needs Iceberg's `append` — no deletes, no tombstones, no deletion vectors.

## Relationship to `metering`

MeterStore stores the types defined by [`metering`](https://crates.io/crates/metering); it does not redefine them and it does not compute with them.

```
metering    → what a measurement is, and how to compute with it   (zero I/O, no async)
meterstore  → where it lives, how it is tiered, how it is queried  (all I/O)
```

`metering` owns intervals, units, quality flags, DST-correct calendars, validation, Ersatzwertbildung, gas conversion, and aggregation. MeterStore adds exactly three things: **correction versioning**, the **transaction-time axis**, and the **tiering boundary**.

That boundary is deliberate. Duplicating a domain rule here — a unit conversion, a DST calendar — would create a second implementation to keep correct, and it would drift. MeterStore briefly carried its own Berlin calendar because `metering` had no public helper; `metering 0.15` added one, and the local copy was deleted the same day. `meterstore::planner` re-exports it so callers need not depend on both crates.

## Requirements

| | Version | Why |
|---|---|---|
| Rust | 1.94 | Set by the dependency floor (`metering` and `iceberg`), not by this crate's own syntax |
| PostgreSQL | **14 or later** | Declarative range partitioning, so purging an archived window is `DETACH` + `DROP TABLE` rather than a row-wise `DELETE`. The test suite pins 16, matching the reference deployment |
| `metering` | 0.16 or later | The string and serde representations became part of its public API, which is what let this crate delete its last local codec |
| Apache Iceberg | format v2 | Deliberately not v3 — see [Design notes](#design-notes) |

MeterStore needs only `SELECT` plus ownership of its own tables: no server
configuration, no restart, no extension. That is what makes it deployable on RDS,
Cloud SQL and Azure Postgres, where an extension-based approach is not.

## Quick start

```rust
use meterstore::prelude::*;
use meterstore::hot::PostgresHot;
use std::sync::Arc;

let hot = Arc::new(PostgresHot::new(pool));
hot.create_table("readings_versions").await?;

// The cold tier — a SqlCatalog over PostgreSQL, object-store backend chosen from
// the warehouse URI scheme. MeterStore builds the whole Iceberg catalog stack, so
// the application depends on neither `iceberg-catalog-sql` nor
// `iceberg-storage-opendal` directly.
let cold_tier = IcebergSqlCatalog {
    database_url: &db_url,
    warehouse_uri: "s3://bucket/warehouse",   // or file:// / memory:// / gs:// / abfss://
    catalog_name: "meterstore",
    namespace: "metering",
    file_target_bytes: 512 * 1024 * 1024,
    metadata_pool_max_connections: 4,
    auth: &WarehouseAuth { region: Some("eu-central-1".into()), ..Default::default() },
}.build().await?;
let cold = cold_tier.cold();
cold.create_table("readings_versions").await?;

let store = MeterStore::builder()
    .hot(hot)
    .cold(cold.clone(), cold.table_provider("readings_versions").await?)
    .table(
        TableConfig::new("readings_versions")
            .settlement_lag(Duration::days(7))   // stay behind the correction window
            .archival_step(Duration::DAY)        // one partition per commit
            .build()?,
    )
    .build()?;
```

That registers the table as `readings` — the physical name carries a `_versions` suffix because it holds every version of every reading, and the provider resolves it — along with the calendar functions.

`IcebergCold::new(catalog, …)` remains for a deployment that brings its own `Arc<dyn iceberg::Catalog>` (a REST catalog, Glue, …). `IcebergSqlCatalog` is the batteries-included path for the common case: `file://` and `memory://` warehouses work out of the box, while `s3://`, `gs://` and `abfss://` are behind the `object-store-s3` / `object-store-gcs` / `object-store-azure` features (or `object-store-all`) so a file-only build does not compile the cloud SDKs. `ColdTier::catalog_facade()` (feature `catalog-facade`) exposes the same catalog to Spark/Trino/DuckDB as a read-only Iceberg REST endpoint.

### Querying across both tiers

```rust
// Spans the watermark. Each row is counted once: the tiers are disjoint,
// so the halves are concatenated rather than merged.
let df = store.sql(r#"
    SELECT meter_local_day("from") AS day, SUM(value) AS kwh
    FROM readings
    WHERE malo_id = '12345678901'
    GROUP BY 1 ORDER BY 1
"#).await?;
```

`meter_local_day` groups by **Berlin** calendar day. `date_trunc('day', ...)` would group by UTC day, whose boundary sits at 01:00 or 02:00 local — producing wrong daily sums every day of the year, not just at the DST transitions.

### Results carry the boundary they were computed against

A number out of a tiered store is not self-describing: two identical queries a minute apart can read the same rows from different tiers, and a `SUM` cannot say so. `store.query()` keeps that provenance.

```rust
let result = store.query("SELECT SUM(value) FROM readings WHERE ...").await?;

tracing::info!(
    watermark = %result.watermark(),          // the boundary this ran against
    tiers = ?result.tiers_scanned(),          // [Cold] | [Hot] | [Cold, Hot]
    mode = ?result.read_mode(),
    "settlement input",
);
```

`result.touched_hot_tier()` is the check a reproducibility claim rests on: if it is true, the figure is only valid for the moment it ran.

Values are bound, never concatenated — `store.query_with_params(sql, params)` takes a `malo_id` straight from a market message.

### Knowing what a write displaced

A count cannot tell a new reading from a correction from a backfill that changed
nothing — and reading the prior state separately races the write, which is wrong
exactly when two corrections arrive together.

```rust
for d in store.append(&deliveries).await?.displacements {
    if !d.effect.changed_current_value() {
        continue;   // Shadowed by a newer version, or a replay. Nothing changed.
    }
    let prior = d.superseded.as_ref();       // what stopped being current
    audit(d.malo_id, d.from, prior, &d.written, d.value_changed(), d.quality_changed());
}
```

Being *stored* and becoming *current* are not the same thing:

| Effect | Meaning |
|---|---|
| `Inserted` | First value for this reading |
| `Superseded` | This became current; another stopped being |
| `Shadowed` | Stored, but an existing **higher** version still wins |
| `Duplicate` | Already present at this version; nothing was written |

Quality and unit travel with the value, because a substitute replaced by a
measurement is a change even when the number is identical, and a number without
its unit is half a fact. On the hot tier the prior state and the insert share one
transaction. It stays a convenience: `readings_versions` keeps every version and
remains authoritative.

### Reading one meter as a domain type

```rust
let series = store
    .series("12345678901")
    .obis("1-0:1.8.0")?              // canonicalised for you
    .range(jan_start, feb_start)
    .collect()
    .await?;

match series {
    Some(series) => { let period = aggregate(&series.intervals, &config); }
    // Absence is information, not an empty series: a `MeasurementSeries`
    // asserts a source, and with no values there is nobody to name.
    None => tracing::warn!("no readings in range"),
}
```

Rows come back version-resolved, tier-split and ordered by interval start. `.intervals()` returns the intervals directly for callers that genuinely want to treat an empty range as zero.

A `MeasurementSeries` is deliberately a channel of numbers: it carries no commodity and none of the deployment's declared identity/attribute columns. A caller reconstructing richer domain rows recovers them without dropping to SQL — `.collect_with_sparte()` adds the commodity, and `.collect_resolved()` returns a `ResolvedSeries { sparte, extra, series }` whose `extra` map holds the declared attribute/identity columns (tenant, reporting party, ingestion source, …) folded from the newest contributing delivery. This is the read counterpart to `StoredSeries::with_extra`: what a deployment writes alongside the values, it reads back here rather than reconstructing with guessed defaults.

Two generic retrievals every meter-data consumer needs are terminals on the same builder, so neither is hand-rolled over a materialised whole-series read:

```rust
// The current reading, resolved with ORDER BY from DESC LIMIT 1 at the storage
// layer — not by loading the history and taking the max in memory.
let now = store.series("12345678901").latest().await?;              // Option<MeterInterval>

// Quality filter pushed into the scan. The accepted set is the caller's — which
// qualities count as billable is a domain rule, so meterstore takes the set, not
// the policy. Matched after version resolution, so it sees the value in force.
let billable = store
    .series("12345678901")
    .quality_in(&[QualityFlag::Measured, QualityFlag::Substituted, /* … */])
    .range(jan_start, feb_start)
    .collect()
    .await?;
```

`.latest()` and `.quality_in(..)` compose with each other and with `.range()`/`.column_eq()` — `series(m).quality_in(&[Measured]).latest()` is the last *good* reading.

### Reproducible reads

MaBiS settlement must be reproducible. An Iceberg snapshot plus an optional version ceiling reconstructs exactly what was known at a point in time.

```rust
let snapshot = store.snapshots().await?[0].snapshot_id;   // or SnapshotSelector::Timestamp(..)

let then = store.as_of(
    SnapshotSelector::Id(snapshot),
    Some(Version::new(20_260_708_000_001)?),   // optional version ceiling
).await?;

let df = then.sql("SELECT malo_id, SUM(value) FROM readings GROUP BY 1").await?;
```

The two bounds are independent and both matter. The snapshot pins **transaction time** — what the store had been told. `max_version` pins the **domain version axis** — which assertion was in force. A snapshot taken after a correction landed holds both versions and resolution prefers the newer one, so without the ceiling a rerun reproduces the store's current knowledge rather than the settlement's inputs.

An as-of read is cold-only by construction: including the mutable tier would make the answer depend on when the query ran. An unknown snapshot, or an instant predating the table, is an error rather than a quiet fallback to current data.

### Transaction-time reads: as known at an instant

`as_of` pins the cold tier to an Iceberg snapshot, so it reconstructs settled history but not the recent (hot) window. When the question is "what did we believe at time *T*" and *T* is recent — a settlement auditor replaying last week, not last year — pin the row-level `recorded_at` axis instead, which every row carries in **both** tiers:

```rust
let then = store.as_known_at(t)?;                    // t: OffsetDateTime
let series = then.series("12345678901").range(from, to).collect().await?;
```

Only versions recorded at or before `t` enter version resolution, so a correction delivered later, **and an interval first stored later**, are both invisible — this reconstructs the *set* of readings, not merely their values. It stays reproducible without pinning a snapshot: archival only ever *moves* a row (with its `recorded_at`) from hot to cold, so a row recorded by `t` is readable regardless of when the query runs. Unlike `as_of`, an `as_known_at` read spans both tiers.

### Completeness

A missing interval is information, not an empty set. An aggregate over an incomplete month must not look like one over a complete month.

```rust
for row in store.completeness(march_start, april_start).await? {
    if !row.is_complete() {
        tracing::warn!(
            malo = %row.malo_id, expected = row.expected, actual = row.actual,
            missing = row.missing, first_gap = ?row.first_gap, "incomplete series",
        );
    }
}
```

Also queryable, because the person asking usually has a SQL client rather than a compiler:

```sql
SELECT * FROM meter_completeness('2026-03-01', '2026-04-01') WHERE NOT complete;
-- malo_id │ obis_code │ resolution │ expected │ actual │ missing │ surplus
--         │ first_gap │ substituted │ not_billable │ complete
```

`expected` comes from each series' own declared resolution and `metering`'s DST-aware calendar — **92** intervals on the spring-forward day and **100** on the autumn one. A check that assumed 96 would raise a false alarm on every meter every spring and, worse, mask a genuine four-interval gap every autumn. `first_gap` names the *Berlin* day, which is often not the UTC one.

`surplus` is reported separately from `missing`: more rows than the calendar allows is a duplicate or a mis-declared resolution, not a negative gap, and it must not cancel a shortfall elsewhere in the range.

### Commodities other than electricity

All four Sparten are storable, and the unit travels with the value:

| Sparte | Measured in | Billed in | Stored as |
|--------|-------------|-----------|-----------|
| `STROM` | kWh | kWh | `KWH` |
| `WAERME` | kWh_th | kWh_th | `KWH` |
| `GAS` | m³ | kWh_Hs | `KWH`, or `M3` for unconverted Betriebsvolumen |
| `WASSER` | m³ | **m³** | `M3` |

```rust
use metering::{MeasurementUnit, Sparte};

// Water settles in cubic metres — the one Sparte whose billing unit is a volume.
let water = StoredSeries::of(Sparte::Wasser, series, version, recorded_at);

// Gas archived before the Brennwert conversion says so.
let raw_gas = StoredSeries::of(Sparte::Gas, series, version, recorded_at)
    .in_unit(MeasurementUnit::CubicMetre);
```

The `value` column is deliberately not called `value_kwh`: a name that asserts a
unit is false for half of the table above. `sparte` and `unit` are core,
non-nullable columns, so no stored number is dimensionless, and a mixed portfolio
groups by the dimension it is summing:

```sql
SELECT sparte, unit, SUM(value) FROM readings GROUP BY 1, 2;
```

A unit the commodity cannot be expressed in — water in kWh — is **refused at the
write**, with a message naming the unit that would have been right. The rule is
`metering`'s (`Sparte::measured_unit`, `Sparte::billing_unit`); MeterStore only
enforces it, on both the write and the read path.

Neither column joins the merge key. A Marktlokation belongs to one commodity, so
the Sparte is functionally determined by `malo_id`; in the key, a correction that
spelled it differently would silently fail to supersede the value it corrects.

### Resolutions finer than 15 minutes

Nothing is written against 96. A series declares its own resolution, and the
expected interval count is asked of `metering`'s calendar per day — so 1-minute
data yields 1440 on an ordinary day and **1500** on the 25-hour autumn day:

```rust
let mut series = MeasurementSeries::new(/* … */);
series.resolution = Some(IntervalResolution::from_seconds(60).unwrap()); // PT60S
```

Completeness, the DST calendar and the `meter_expected_intervals` UDF all read
that declaration rather than assuming the settlement grain. `tests/it/sub_quarter_hour.rs`
asserts this end to end, including the DST day and a sub-quarter-hour water series.

### Several tables in one session

A deployment usually holds more than one stream. The clearest case: ESA "Werte
nach Typ 2" are non-authoritative, so they belong in a table a billing query
cannot reach *by construction* rather than by remembering a `WHERE` clause.

```rust
let catalog = MeterCatalog::builder()
    .table(readings_builder)
    .table(esa_typ2_builder)
    .build()
    .await?;

// One statement, both tables.
catalog.query("SELECT … FROM readings r JOIN esa_typ2 e USING (malo_id)").await?;

// Each table still archives on its own schedule, with its own lease.
catalog.table("readings").unwrap().archive(now, 8).await?;
```

Each table keeps its own watermark, archiver and advisory lock — nothing is
transactional across them, and that is deliberate: a cross-table commit would
mean a distributed transaction between PostgreSQL and an Iceberg catalog. What
the catalog adds is the query surface and one set of system tables:

```sql
SELECT "table", watermark, watermark_lag_seconds, healthy FROM system.tables;
```

Because two tables genuinely have two boundaries, a result carries both:

```rust
for (table, watermark) in result.watermarks() { … }
result.watermark();   // the conservative one — below it, every table is settled
```

### Retention, and what a partition drop does not do

**Dropping a hot partition destroys nothing.** It runs only after those rows are
durable in Iceberg, and a partition the watermark does not cover raises an
invariant violation rather than being dropped. The hot window is a latency and
cost decision, not a data-lifecycle one.

That leaves three separate questions, with three different answers:

| Question | Answer |
|---|---|
| A different hot window per tenant, in one table? | **No, and it cannot be.** The watermark decides which tier a query reads from; two windows in one relation would mean two boundaries and no honest way to report either. |
| Expire *part* of a table's history? | **Not implemented.** `expire_snapshots` removes snapshot metadata, not rows. Removing a subset means rewriting files, which `iceberg-rust` cannot do. |
| Remove a tenant entirely? | **`store.purge_table(name)`** — the PostgreSQL table with every partition, the Iceberg catalog entry, and the data files in object storage. |

```rust
// The name is repeated because a purge has no recovery path.
store.purge_table("readings_versions").await?;
```

This is the only operation in the crate that deletes stored readings. Everything
else is append-only, because a settlement must stay reproducible: a correction is
a new version, and erasure destroys a *mapping* rather than rows.

So a table per tenant is the answer when you need per-tenant data lifecycle or
blast-radius isolation — `MeterCatalog` makes that practical — and one table is
the answer otherwise. It is not a decision about "retention".

### Multi-tenancy, and the two identifiers that are not the same thing

**A tenant is not a market participant.** They answer different questions and
mixing them up produces a wrong bill, so they are separate throughout:

| | What it is | Where it lives |
|---|---|---|
| **Tenant** | *Whose installation this is.* An account, a customer of a service bureau, an isolation boundary. Opaque to this crate — it never parses one. | A deployment-declared `identity_column` |
| **Network operator** | *Who assigned this version.* A BDEW Codenummer, and half of the scope a version is comparable within (MSCONS assigns versions per operator per month). | `version_scope`, as `"<operator>:<YYYY-MM>"` |

One tenant routinely holds data from **many** network operators — a supplier
takes deliveries from every grid operator it has customers behind. And one
market participant may appear across several tenants in a bureau. Neither
determines the other.

Declare the tenant as an **identity** column and it joins the merge key, so two
tenants holding the same MaLo-ID are two readings and neither can supersede the
other:

```rust
TableConfig::new("readings_versions")
    .identity_column(Field::new("tenant", DataType::Utf8, false))
    .build()?
```

It also becomes the cold tier's leading partition field, ahead of `month(from)` —
so a tenant-scoped scan eliminates other tenants' files at the manifest, before a
Parquet footer is opened, and an erasure rewrite has a bounded file set.

The **operator** is not an identity column and must not be one. It is not part of
what identifies a reading; it is part of what makes two versions of that reading
comparable. Putting it in the merge key would turn a corrected reading into two
different readings.

Because that distinction is easy to lose at an integration boundary, the hot tier
refuses a second operator for one reading:

```
ERROR: conflicting key value violates exclusion constraint "..._one_operator"
```

Passing a forwarding party's MP-ID — or a tenant id — where the network operator
belongs would otherwise give one reading two incomparable scopes, leave both rows
standing in the resolved view, and double every sum over them.

This is guarded on **both** write paths. The hot tier uses a constraint, which
nothing can go around; Iceberg has none, and `append()` routes a below-watermark
interval straight there — so `append()` performs the same check itself before
writing the cold half. A guard on one path only would have been reachable simply
by being late, and a late correction is the delivery most likely to carry a stale
operator.

If you *do* want two parties' assertions about one reading side by side — to
reconcile what each reported — give the reporting party its own identity column.
They then become two readings that no aggregate can conflate, which states the
intent in the schema rather than resting on a constraint being turned off.

MeterStore does **not** enforce that a query carries a tenant predicate. That is
an authorization decision, and it belongs to the service holding the caller's
identity, not to the storage layer.

### Operational visibility

```sql
SELECT "table", watermark, watermark_lag_seconds, invariant_violations, healthy
FROM system.tables;
```

`healthy` is false when rows sit below the watermark but are still in PostgreSQL — the one condition that makes query results wrong, so it is the thing to alert on. `system.config` shows the settings that interact, notably `partition_step` against `archival_step`: valid individually, and a silent degradation to row-wise `DELETE` when they disagree.

Two more tables answer questions that otherwise need Iceberg metadata by hand:

```sql
SELECT value FROM system.resolution WHERE setting = 'resolution_sql';  -- for Trino/Spark
SELECT snapshot_id, committed_at, watermark FROM system.snapshots;     -- what as_of can pin
```

All four are snapshots. `store.refresh_system_tables(now)` recomputes them — explicit, so a query never silently pays for a round trip to both tiers.

### Archiving and maintenance

```rust
for outcome in store.archive(OffsetDateTime::now_utc(), 32).await? {
    if outcome.archived_anything() {
        println!("archived {} rows, watermark now {}", outcome.rows, outcome.watermark);
    }
}

store.verify_invariant().await?;   // no row may sit in the wrong tier
```

Or on a schedule:

```rust
let handle = store.maintenance()
    .interval(Duration::minutes(15))
    .expire_snapshots(false)     // off by default — retention is a compliance decision
    .spawn();
```

**Exactly one process may archive a table at a time.** The window between detaching a partition and dropping it is the only state where the tiering invariant is relaxed, and it is safe only because one process owns it. `PostgresHot` enforces this with a session-scoped advisory lock, so every replica can run the same schedule: one wins and the others report `lease_contended` and stop. That is not a failure and should not page anyone — the alert that still fires if *nobody* is winning is watermark lag.

`ReadMode::Historical` reads only Iceberg — no load on the operational database. `ReadMode::Operational` reads only the recent window. `ReadMode::AsOf` is the reproducible read above.

Run `cargo run --example encoding` for a tour of encoding, versioning and tier routing that needs no database.

### Deployment-specific columns

A column is either part of a reading's **identity** or **data about** it, and the distinction is load-bearing:

```rust
TableConfig::new("readings_versions")
    .identity_column(Field::new("tenant", DataType::Utf8, false))   // joins the merge key
    .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
```

A tenant discriminator must be an identity column. As an attribute, two tenants reporting the same measuring point would share a merge key and one tenant's correction would supersede the other's reading. Identity columns must be non-nullable, and extra columns are `Utf8` today.

An extra column whose values are a fixed vocabulary — an ingestion source, a delivery status — can be declared with `coded_column(name, &["…", "…"], nullable)`, which renders a DB `CHECK` on the hot table exactly like the built-in `sparte`/`unit`/`quality` columns. A value outside the set fails the write rather than being read back later as an unknown code. MeterStore stays domain-agnostic: it enforces whatever set the caller supplies, carried in the field's Arrow metadata so it does not disturb the type or the schema-evolution contract.

`store.create_tables()` creates both tiers from one configuration — the schema, primary key, conflict target and resolution `PARTITION BY` all have to agree, and a mismatch shows up as readings that fail to supersede rather than as an error.

### Configuration that must agree

Two settings are validated against each other at construction, because getting them wrong degrades silently rather than failing:

- `partition_step` **must equal** `archival_step`. A purge drops exactly one partition per archived window; any mismatch forces archival back to row-wise `DELETE`.
- `settlement_lag` must cover at least one `archival_step`, or a window can be archived while still receiving corrections.

### Configuration from a file

The builder is the API; TOML is a serde front end over the *same* validated type, so a file and a hand-built configuration pass through identical checks.

```toml
[hot]
url = "${DATABASE_URL}"          # ${VAR} is interpolated; a missing one is an error
max_connections = 16

[cold]
catalog = "rest"                 # or "sql"
uri = "https://catalog.internal"
warehouse = "s3://edm/meterstore"
namespace = "metering"

[[tables]]
name = "readings_versions"
subject_column = "subject_ref"
extra_columns = [
  { name = "tenant", identity = true },   # joins the merge key
  { name = "bilanzkreis" },               # data about a reading
]

[tables.hot]
partition_step = "1d"
partition_headroom = "14d"

[tables.archival]
settlement_lag = "7d"
archival_step = "1d"
scan_chunk_rows = 50_000

[tables.maintenance]
snapshot_retention = "10y"       # compliance, not cleanup
min_snapshots_to_keep = 20
```

```rust
let settings = Settings::from_path("meterstore.toml")?;
let config = settings.single_table()?;    // fully validated
```

Unknown keys are an error rather than a silently ignored typo, and the file names the tiers without opening them — the application builds its own `PgPool` and catalog, because MeterStore never owns a connection.

### External engines, without MeterStore in the data path

A REST-catalog deployment needs nothing: point Spark, Trino, DuckDB or PyIceberg
at the same endpoint and they read object storage directly, in parallel. A
**SQL-catalog** deployment needs a bridge, because JDBC-catalog support across
engines is uneven:

```rust
let router = CatalogFacade::new(catalog).router();   // `catalog-facade` feature
axum::serve(listener, router.layer(your_auth_layer())).await?;
```

It serves the spec's config, namespace and table-metadata routes and **has no
write path** — not a write path that is disabled. An external writer would place
rows in the cold tier without MeterStore knowing, and nothing downstream could
detect it: the files would be valid Iceberg, the invariant check only looks at
PostgreSQL, and the first symptom would be a figure that does not reconcile.
Mutating requests get `405` with that reason rather than `501`, which would
invite a retry against a future version.

It also carries no object-store credentials: the response says where the data is,
not how to authenticate to it. A router rather than a bound port, so you wrap it
in your own auth and TLS — it needs authentication before it leaves a trusted
network.

### Checking it against your own data

The properties this store claims are ones a *deployment* should be able to check
against its own configuration and volumes, so the harness and the oracle are part
of the public API rather than a `dev-dependency` trick:

```rust
// `testkit` feature.
let harness = TestHarness::start().await?;          // real Postgres + real Iceberg
let store = harness.store().await?;

let workload = MeteringWorkload::new(start)
    .seed(0x5EED)                                   // a failure replays from this alone
    .malo_ids(1_000)
    .days(30)
    .with_corrections(0.01)                         // rare and recent, as in practice
    .with_gaps(0.02)
    .spanning_autumn_back();                        // the 100-interval day

let series = workload.generate()?;
let mut oracle = Oracle::new();
oracle.record(&series)?;                            // latest-version-wins, per scope
harness.ingest(&store, &series).await?;
store.archive(now, 64).await?;

assert_eq!(rows_in(&store).await, oracle.row_count(from, to));
```

`Oracle` is a deliberately *different* implementation of resolution — a map fold
in Rust against the store's window function in SQL. An oracle that shared the
implementation under test would agree with it about its mistakes.

### A non-Rust client over both tiers

The Iceberg catalog is the right answer for almost everything: engines read
history straight from object storage, in parallel, with MeterStore nowhere in
the data path. Routing analytics through a server would be *worse*.

There is exactly one thing an external client cannot assemble for itself: the
hot tier lives in PostgreSQL and is not in the catalog, so **the unified view**
needs a surface. That is what Flight SQL is for — and what a BI tool reaches
through a Flight SQL JDBC/ODBC driver.

```rust
// `flight` feature. A tonic service, not a bound port: you add your own
// authentication interceptor and TLS before this leaves a trusted network.
tonic::transport::Server::builder()
    .add_service(FlightSqlServer::new(store).into_service())
    .serve(address)
    .await?;
```

It is **read-only**, and for one more reason than the catalog façade: a write
here would bypass `MeterStore::append`, and with it both the tier routing (a
correction below the watermark written to PostgreSQL is silently invisible) and
the subject-reference check that stops a replay re-linking an erased subject.
Mutating calls answer `PermissionDenied` naming both.

Results keep their provenance across the wire — the watermark, the tiers scanned
and the read mode ride along as Arrow **schema metadata**, so a BI tool that
keeps the schema keeps the boundary the figure was computed against.

### Is the output actually open?

P2 — vendor-neutral, decade-retention storage — is the main procurement argument,
and an untested argument is a hope. `tests/it/interop.rs` opens the written Parquet
**by path**, with a bare DataFusion session that has none of this crate's
providers and no Iceberg catalog, and checks that:

- the bytes are standard Parquet, readable without `iceberg-rust`;
- **the published resolution SQL, run unmodified against those files, reproduces
  MeterStore's own answer**;
- the naive sum over the same files is *wrong* — a mitigation for a hazard nobody
  has demonstrated is one nobody will apply;
- decimals keep full precision and quality reads as `MEASURED`, not an opaque
  integer;
- the footer really carries the declared sort order, the bloom filters and the
  page index, and the rows really are in that order.

That suite reads through DataFusion, which shares `arrow-rs` and `parquet-rs`
with the writer — so a bug in that shared layer would be invisible to it.
`tests/it/interop_duckdb.rs` closes the gap with **DuckDB** in a container: a
different language, a different Parquet reader, and its own reading of the
Iceberg spec. It checks both levels — `read_parquet` over the files, and
`iceberg_scan` over the manifests and snapshots — and confirms the published
resolution SQL gives the right answer *in DuckDB*, while the naive query
overstates.

One thing that fell out of it: DuckDB needs `unsafe_enable_version_guessing` to
open a **SQL-catalog** warehouse, because the metadata pointer lives in
PostgreSQL rather than in a `version-hint.text` beside the files. That is the
concrete argument for running the catalog façade — it removes the guess.

Spark, Trino and PyIceberg are still untested.

### Schema changes

The store compares the schema it would write against the one the cold table holds, at construction and before every archival run.

Additive changes are free — Iceberg identifies columns by immutable field id, so a new nullable column reads as null in every historical file. Anything that **narrows** halts the table: a retype Iceberg cannot promote, a decimal scale change, a `NOT NULL` addition, or a new identity column (which changes what "the same reading" means). The watermark freezes with it, so nothing is archived out of PostgreSQL — where it can still be corrected — into a layout nobody has agreed on. Other tables keep running.

```rust
match store.check_schema().await? {
    Some(c) if c.is_safe() => { /* proceed; c.changes lists the additive drift */ }
    Some(c) => for change in c.unsafe_changes() { eprintln!("{}", change.describe()) },
    None => { /* the cold store cannot report a schema, so nothing was proved */ }
}
```

## Development

Requires a Rust toolchain and, for integration tests, a running Docker daemon.

```bash
just            # list all recipes
just dev        # format + unit tests (no Docker)
just check      # everything CI runs
just unit       # unit tests only
just test       # full suite, needs Docker
just example    # run the worked example
```

| Recipe | What it does |
|--------|--------------|
| `just check` | fmt, clippy, tests, dependency check, docs |
| `just deps` | fails if a single-sourced crate appears at two versions |
| `just test-one NAME` | one test, with `RUST_LOG=meterstore=debug` |
| `just coverage` | HTML coverage report |
| `just bench` | CPU benchmarks: encode, decode, tier split, planning |

`cargo test --features testkit --test measured -- --nocapture` measures the
figures that need real storage. Compression is the one that matters most, because
the argument at the top of this file — that a row store cannot hold this volume economically — rests entirely on it:

```
compression: 19200 rows — postgres 8773632 B (457.0 B/row),
                          parquet    80713 B (  4.2 B/row), ratio 108.7×
```

PostgreSQL is measured across the whole partition tree with
`pg_total_relation_size` — indexes and row overhead included, because a
deployment keeping this data in PostgreSQL pays for those.

Read it with both caveats. The fixture's low cardinality flatters it: one OBIS
code, one version scope, and JSON columns that repeat per row in PostgreSQL while
Parquet dictionary-encodes them away. The small volume works the other way: the
per-file Parquet footer is a fixed cost a 512 MiB partition amortises. The honest
reading is that the >10× target is met with room to spare.

**Query latency is still unmeasured** — the p99 targets are what bloom filters are
supposed to deliver, and that needs the reference hardware and a realistic corpus.

### Single-sourced dependencies

`datafusion`, `arrow`, `iceberg`, `parquet`, `metering`, `time` and `rust_decimal` must each appear exactly once in the dependency graph. Two versions of `arrow` mean two incompatible `RecordBatch` types; two of `datafusion` mean two incompatible `TableProvider` traits. Neither fails obviously — a crate simply cannot be used with the other's types.

Arrow is therefore sourced through `datafusion::arrow` rather than as a direct dependency, and `just deps` fails the build if any of the others diverge.

## Status

| Area | State |
|------|-------|
| Storage encoding | ✅ `MeasurementSeries` ↔ Arrow, exact round trip |
| Correction versioning | ✅ scoped versions, cross-scope comparison refused |
| Tiering watermark | ✅ atomic, inside the Iceberg snapshot summary |
| Hot tier (PostgreSQL) | ✅ partitioned, detach/scan/drop, orphan recovery |
| Cold tier (Iceberg) | ✅ Parquet with bloom filters and delta encoding |
| Cold-tier construction | ✅ `IcebergSqlCatalog` builds the SqlCatalog + object-store backend; `file`/`memory` baseline, `s3`/`gcs`/`azure` behind features |
| Archival job | ✅ crash-safe ordering, idempotent, integration tested |
| Tier-split planning | ✅ predicate extraction, watermark split, version elision |
| DST calendar | ✅ delegated to `metering::calendar`, contract-tested |
| Query execution | ✅ `TieredTableProvider` — SQL spans both tiers, hot side streams |
| Session handle | ✅ `MeterStore` — one builder, SQL, archival, invariant check |
| Calendar functions | ✅ `meter_local_day`, `meter_local_month`, `meter_expected_intervals` |
| Version resolution | ✅ `readings` resolves corrections; `readings_versions` keeps the history |
| Routed writes | ✅ `store.append()` sends late corrections to the right tier |
| Identity columns | ✅ tenant-safe merge keys, wired through both tiers |
| Coded attribute columns | ✅ `coded_column(name, &[…])` renders a DB CHECK, like sparte/unit/quality |
| Erasure | ✅ pseudonymous references, wired into writes, with a suppression list |
| System tables | ✅ `system.tables`, `system.config`, `system.resolution`, `system.snapshots` |
| Metrics | ✅ OpenTelemetry API — no-op until you install an SDK |
| Merge elision | ✅ historical scans skip resolution when Iceberg statistics prove no corrections |
| Snapshot expiry | ✅ `store.expire_snapshots()`, 10-year default retention |
| Reproducible reads | ✅ `store.as_of()` — pinned snapshot plus an enforced version ceiling (cold tier) |
| Transaction-time reads | ✅ `store.as_known_at(t)` — `recorded_at` ceiling, both tiers; reconstructs values **and** set membership |
| Completeness | ✅ `store.completeness()` and `meter_completeness(from, to)`, DST-aware |
| Query provenance | ✅ `store.query()` — watermark and tiers scanned on every result |
| Typed series API | ✅ `store.series(malo).obis(..).range(..).collect()` → `MeasurementSeries`; `.collect_resolved()` recovers commodity + attribute columns |
| Latest / quality-filtered reads | ✅ `.latest()` (newest interval, no full scan) and `.quality_in(&[..])` (filter pushed into the scan) as builder terminals/options |
| Single-archiver lease | ✅ PostgreSQL advisory lock; contended runs are a no-op, not a failure |
| Schema evolution | ✅ compatibility check with quarantine on a narrowing change |
| Maintenance scheduler | ✅ `store.maintenance().spawn()` |
| File configuration | ✅ `Settings` — TOML over the same validated types |
| CDC seam | ✅ `ChangeSource` trait behind the `cdc` feature — unbuilt by design |
| Catalog facade | ✅ read-only Iceberg REST endpoint behind `catalog-facade` |
| Testkit + oracle | ✅ seeded workload generator and the tiering oracle, behind `testkit` |
| Interop (hermetic) | ✅ the files read, and the published SQL verified, without this crate |
| Benchmarks (CPU) | ✅ `cargo bench` — encode, decode, tier split, planning |
| Compression, measured | ✅ **~109×** vs PostgreSQL row storage, on real infrastructure |
| Interop (DuckDB) | ✅ a real foreign engine reads the Parquet **and** the Iceberg metadata |
| Flight SQL | ✅ unified hot + cold for a non-Rust client, behind `flight` |
| SQL discovery | ✅ `information_schema`, so a client can browse before it queries |
| All four Sparten | ✅ Strom / Gas / Wärme / Wasser, with the unit stored and checked |
| Sub-quarter-hour data | ✅ any declared resolution; completeness follows the calendar, not 96 |
| Cold partitioning | ✅ identity columns then `month(from)` — a tenant scan prunes at the manifest |
| Multi-table sessions | ✅ `MeterCatalog` — cross-table SQL, one `system.*`, per-table watermarks |
| Displacement reporting | ✅ `append()` says what each write did to the value that was current |
| Hot integrity constraints | ✅ per-partition GiST — no intra-version overlap, one network operator per reading |
| Bulk ingest path | ✅ `hot_writer()` — one boundary read per run, refuses below-boundary rows |
| Erasure in a caller's tx | ✅ `SubjectRegistry::erase_in` — one transaction across the seam |
| Table decommissioning | ✅ `purge_table()` — both tiers, data files included, name confirmed |
| Partial history expiry | ⛔ blocked upstream — no file rewrite; expiry is whole-table |
| **Blocked upstream** | |
| Compaction | ⛔ blocked upstream — see below |
| Orphan-file cleanup | ⛔ blocked upstream — `iceberg::io` cannot list |
| Iceberg view for `readings` | ⛔ blocked upstream — no `create_view` on `Catalog`; use the published SQL |
| **Not yet done** | |
| Interop (wider matrix) | ⬜ Spark / Trino / PyIceberg |
| Query-latency benchmarks | ⬜ the p99 targets bloom filters are supposed to deliver |
| Chaos suite | ⬜ slow storage, network partitions, sustained load — archiver contention *is* tested |
| Production surface | ⬜ CLI, health/readiness endpoints, exporters, runbooks |
| Published to crates.io | ⬜ deliberately not yet — packageable today, but the API has not met a real consumer |

**509 tests** — 296 unit, plus 211 integration against real PostgreSQL and a real Iceberg warehouse, including a property suite that checks the whole store against an independently implemented reference.

### Writing

**Use `hot_writer()` for steady-state ingest and `append()` for anything that might contain a late correction.** Both take `StoredSeries`, so the storage contract is compiler-checked rather than reproduced in your own SQL.

```rust
// Bulk ingest: reads the tier boundary once for the whole run.
let writer = store.hot_writer().await?;
for delivery in batches {
    writer.append(&delivery).await?;
}
```

`hot_writer` **refuses** any interval below the boundary it was opened at rather than routing it — routing on a stale snapshot could place a row where no query looks, and refusing cannot. The error points at `append()`, which does route. Reopen the writer per ingest run rather than holding one for the process lifetime; the margin is the settlement lag.

Writing to PostgreSQL with your own driver still works, and the rules below are what a conformant insert must satisfy. Prefer the typed path: these are the constraints where a mistake shows up as readings that silently fail to supersede.

**OBIS codes must be canonical.** `"1-0:1.8.0"` and `"1-0:1.8.0*255"` denote the same channel, and the column is part of the merge key, so two spellings mean a correction cannot supersede the value it corrects. Route codes through `meterstore::canonical_obis()`. The hot table also rejects non-canonical values with a `CHECK` constraint, so a mistake fails at the write rather than corrupting resolution later.

**Intervals must not overlap within one version, and one reading has one network operator.** Each hot partition carries two `EXCLUDE USING gist` constraints, both guarding a write that would produce a wrong number rather than an error: an hourly delivery followed by a quarter-hourly one for the same channel and version would leave both stored and every sum inflated; and one reading carrying two `version_scope` operators gives two incomparable scopes, both of which survive resolution and double every sum. Corrections stay legal — a higher version, same operator. Needs `btree_gist` (contrib), created on demand; `PostgresHot::integrity_constraints(false)` turns both off.

**Quality is written as its code, not an integer.** `'MEASURED'`, `'SUBSTITUTED'` — the values `metering::QualityFlag::as_str()` produces.

**Sparte and unit are mandatory, and must agree.** `'STROM'`/`'GAS'`/`'WAERME'`/`'WASSER'` and `'KWH'`/`'M3'` — `metering::Sparte::as_str()` and `MeasurementUnit::as_str()`. The hot table rejects anything else with a `CHECK` built from those code lists, and MeterStore additionally refuses a unit the Sparte cannot be expressed in.

**The version scope must come from the interval, not the delivery.** MSCONS assigns versions per operator per month, and resolution partitions by scope — so a July reading corrected in August must carry July's scope, or the two versions cannot supersede each other and both survive. Use `VersionScope::for_interval()`; encoding rejects a scope that does not cover its intervals.

**Corrections for archived intervals must not go to PostgreSQL.** They would land below the watermark, where no query looks — accepted, then silently ignored. Use `store.append()`, which routes each interval to the tier that owns it:

```rust
let outcome = store.append(&[corrected_series]).await?;
if outcome.had_late_corrections() {
    // Some intervals were already archived and went straight to Iceberg.
}
```

### Reading the raw table with another engine

The cold table is named `*_versions` because it holds **every** version of every reading, unresolved. An external engine that reads it naively and sums `value` **double-counts corrected intervals**.

**MeterStore resolves this itself** — `readings` applies latest-version-wins over the raw `readings_versions` table, using the SQL below, so the two definitions cannot drift. An external engine reading the Iceberg files directly must apply it, whether it found the table through its own REST catalog or through the façade above. `store.resolution_sql()` returns the exact statement, and `SELECT value FROM system.resolution WHERE setting = 'resolution_sql'` returns the same text to a SQL client:

```sql
SELECT "malo_id", "melo_id", … FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY "malo_id", "obis_code", "from", "version_scope"
    ORDER BY "version" DESC
  ) AS _meterstore_rank
  FROM readings_versions
) AS _meterstore_resolved WHERE _meterstore_rank = 1
```

Three details are load-bearing. The partition includes `version_scope`, because MSCONS assigns versions per network operator per month and ranking across scopes is meaningless. The outer projection is explicit rather than `SELECT *`, so the ranking column does not leak into the result. And the derived table is aliased, because PostgreSQL — which backs the SQL catalog, and is therefore a natural place to paste this — rejects an unaliased subquery; Trino, Spark, DuckDB and DataFusion all accept the alias, so one spelling works everywhere.

## Privacy

15-minute consumption is personal data — it reveals when a household wakes, sleeps and travels, and which appliances it runs. German law treats it that way, which is why the Smart-Meter-Gateway protection profile exists.

But **erasure requests are usually refused**: the MsbG requires retention, and GDPR Art. 17(3)(b) exempts processing under a legal obligation. The erasure path matters for data past its retention period, data kept beyond the legal minimum, and consent-based processing where consent is withdrawn.

For those cases MeterStore uses **pseudonymisation, not crypto-shredding**. Crypto-shredding needs one key per subject; Iceberg keys data per *file*, and one file holds thousands of measuring points, so destroying its key would erase all of them. Encrypting the value column instead would break delta encoding, min/max pruning and bloom filters.

So the lake stores an opaque `subject_ref` and `SubjectRegistry` holds the mapping in PostgreSQL, where deletion is real. Declare the column and hand the store a registry:

```rust
let store = MeterStore::builder()
    .table(TableConfig::new("readings_versions").subject_column("subject_ref").build()?)
    .subject_registry(SubjectRegistry::with_erasure_secret(pool, &secret)?)
    // ... hot, cold
    .build()
    .await?;

let subject = store.register_subject("customer-4821").await?;   // opaque reference

// Later: destroy the link. The readings stay; nothing can attribute them.
store.erase_subject(&subject, "DSAR-2026-0042", "privacy-team", now).await?;
```

Irreversible (the row is deleted, not flagged), auditable (an append-only record that deliberately omits the natural identifier), and per subject. References come from the OS CSPRNG, so one cannot be guessed or recomputed from the identifier it stands for.

**`append()` refuses a reference the registry does not back.** Either the pipeline invented it — rows nobody can attribute, from birth — or it belongs to an erased subject, meaning a replay is rebuilding the link erasure destroyed. Neither is visible in the data afterwards, so it fails at the write.

**The erasure secret is what keeps a subject erased.** Deleting the mapping leaves nothing to recognise the identifier by, so a broker redelivering an old batch would simply register it again and get a working reference. With a secret, erasure records a keyed hash of what it erased and `register` refuses anything matching. `SubjectRegistry::new` omits it and is honest about the consequence; use it only where ingest cannot replay. `lift_suppression` reverses a mistaken erasure — restoring the ability to register that identifier again, under a new reference, never the old link.

The module is optional — a deployment that does not need it declares no subject column.

## Design notes

**Iceberg v2, deliberately.** v3 adds deletion vectors and row lineage; an append-only store needs neither, and v3's reader support is still uneven. For data under a decade-long retention obligation, writing a format some engines cannot read would undercut the reason for using an open format at all. The version is verified after table creation, so a future library default moving to v3 fails loudly.

**No derive macro.** The canonical row types come from `metering` and cannot be derived on from here, and schema is configuration-driven rather than type-driven. A derive would be a second, competing source of truth.

**Stored values are `metering`'s own strings.** Quality is `MEASURED` / `SUBSTITUTED`, resolution is an ISO 8601 duration (`PT15M`, `P1D`), OBIS codes are canonical. Earlier versions pinned local integer codes as a hedge against upstream renames; `metering 0.16` made its string and serde representations part of its public API, covered by semver, so the hedge became unnecessary. The gain is that **the data is self-describing** — an engine reading the Parquet sees `SUBSTITUTED`, not an opaque `2` whose meaning lives in this crate's source.

**OBIS codes are stored canonically, and the database enforces it.** `1-0:1.8.0` and `1-0:1.8.0*255` denote the same channel, and the column is part of the merge key, so two spellings would let a correction fail to supersede the value it corrects. A storage group other than 255 is real information and is kept.

**`readings` is a table provider, not a SQL view.** Resolving corrections means ranking rows by version, and over history that is usually ranking each row against nothing — corrections are rare and recent. Iceberg records `min`/`max` version per file, so a scan can be proven correction-free from metadata. A view cannot see those statistics or know which range a query will touch; a provider is handed both. Historical scans that pass the proof run with no window function at all. Anything touching the hot tier always resolves — PostgreSQL keeps no such statistics, and proving it correction-free would mean scanning the rows the query was avoiding.

The resolution plan is built from the same SQL published for external engines, so the text you would run in Spark and the plan this session executes cannot drift.

**No compaction, and it is blocked rather than deferred.** The published `iceberg` crate can write data files and manifests, but exposes no way to *commit* a snapshot that removes files: there is no rewrite or overwrite action on `Transaction`, `TransactionAction` is crate-private so one cannot be added, and `TableCommit`'s builder is crate-private by explicit design. A compacted snapshot can be produced in full and not landed. The available reference implementation for Iceberg compaction in Rust does not depend on a released `iceberg` crate — it pins a fork.

The cost is smaller here than it would be elsewhere: merge elision is per-partition, and a partition only loses it if a correction lands in that partition, which is rare and concentrated in recent months. Small-file growth is bounded by the correction batching interval rather than by correction volume. Snapshot expiry — the growth that actually compounds — is implemented. Until upstream exposes a rewrite action, an operator who needs compaction can run it out-of-band with Spark or PyIceberg; the layout is standard Iceberg precisely so that works.

Orphan-file cleanup is blocked one step earlier, on the read path: `iceberg::io` has no listing operation, so there is no way to enumerate the warehouse and subtract what the manifests reference. Orphans cost storage rather than correctness, and the same out-of-band tools reclaim them.

Because out-of-band compaction is the recommended workaround, the watermark read tolerates it: a snapshot Spark or PyIceberg wrote carries no `meterstore.tiering_watermark`, so the lookup walks back along the parent chain to the most recent snapshot MeterStore did write. Those commits rewrote *files*, not the interval range each tier owns, so the older value is still the right one. `system.snapshots` shows which snapshots carry a watermark and which do not.

**A version ceiling is applied, not merely offered.** A filter handed to a DataFusion `TableProvider` is advisory — the provider may use it to prune files and return the rows anyway, because the engine normally re-applies it above the scan. The ceiling is injected by the tiered provider, so the engine does not know it exists and would never re-apply it. It is therefore pushed down *and* enforced with an explicit filter below the projection. A settlement rerun that quietly ignored its version ceiling would return today's corrections under the heading of a past settlement.

**Ingestion is archival, and CDC is a seam rather than a gap.** Logical decoding reads the WAL and touches no heap pages, which is a real advantage on a busy primary — but replicating rows out of PostgreSQL does not remove them, and the hot tier has to stay bounded either way. Both designs need the purge, and the purge is where the cost actually is. Once it is a partition drop, what remains of CDC's advantage does not pay for a replication slot that can fill the primary's disk when a consumer stalls. The `ChangeSource` trait exists behind the `cdc` feature so that a future latency requirement is a strategy swap rather than a restructure.

## License

Licensed under either of:

- [MIT License](./LICENSE-MIT)
- [Apache License, Version 2.0](./LICENSE-APACHE)
