+++
title = "Querying"
description = "SQL across both tiers, results that carry the boundary they were computed against, the typed series API, and the calendar functions that make daily sums correct."
weight = 5
+++

## SQL over the unified view

```rust
let df = store.sql(r#"
    SELECT meter_local_day("from") AS day, SUM(value) AS kwh
    FROM readings
    WHERE malo_id = '12345678901'
      AND "from" >= '2025-01-01' AND "from" < '2026-01-01'
    GROUP BY 1 ORDER BY 1
"#).await?;
```

`sql` returns DataFusion's own `DataFrame`, so every expression, window function
and output format works, and there is no second query language to maintain.
`.analytics()` gives the same thing bound to the unified view for callers who
prefer the builder.

Caller-supplied values are bound as parameters, never concatenated into the SQL
text, so a `malo_id` can come straight off a market message:

```rust
let result = store.query_with_params(
    "SELECT SUM(value) FROM readings WHERE malo_id = $1",
    vec![ScalarValue::Utf8(Some(malo.into()))],
).await?;
```

## Results carry their provenance

A bare `Vec<RecordBatch>` from a tiered store is missing the one fact needed to
reason about it: *which boundary was in force*. Two identical queries a minute
apart can read the same rows from different tiers, and a sum that looks stable is
only stable because nothing was archived in between.

```rust
let result = store.query(sql).await?;
result.watermark();         // the boundary this ran against
result.watermarks();        // per table, when a statement spans several
result.tiers_scanned();     // [Cold] | [Hot] | [Cold, Hot]
result.spans_tiers();
result.touched_hot_tier();  // the answer is only valid for now
result.read_mode();
```

Both facts are read off the **physical plan** before execution — the plan *is* the
tier decision, it belongs to this query alone, and asking the providers afterwards
would race any other query in flight. Nodes are identified by type rather than by
name, so a renamed node in a pre-1.0 dependency fails to compile rather than
silently reporting that no cold tier was scanned.

### Describing a statement without running it

```rust
let described = store.describe(sql).await?;
described.schema();          // what it would produce
described.watermark();       // the boundary it would run against
described.tiers_scanned();   // which tiers it would read
```

Plans the query — so a syntax error, an unknown column or an unknown relation is
reported here — and stops. This is what a surface needing a schema *before* any
row should call: answering such a request by executing the query makes an Arrow
Flight client's ordinary `GetFlightInfo` → `DoGet` sequence cost two full scans,
and makes "preparing" a statement run it.

Planning is not free — it reads the tier boundary, and for the resolved table the
per-file statistics that decide elision — but that is a catalogue read rather than
a scan, which is the right price for describing a statement.

## How a query is planned

```text
SQL → TieredTableProvider::scan()
    → extract the `from` range from the filters
    → compare against the watermark
        ├── entirely ≥ W  → PostgreSQL, streamed
        ├── entirely < W  → Iceberg
        └── spans W       → both, UNION ALL, split at W
    → version resolution, unless statistics prove it unnecessary
```

The **predicate extraction is deliberately conservative**: failing to recognise a
bound costs a wider scan, wrongly inferring one loses rows. So anything not
provably a bound on `from` widens the range, and `OR` — where one branch may be
unbounded — discards bounds entirely. That property is asserted over generated
filter trees, not only over the shapes someone thought to write down.

The cold half delegates to `iceberg-datafusion`, so partition pruning, bloom
filters and page statistics all apply. The hot half streams: PostgreSQL is paged
by keyset, each page becomes a batch, and the batch reaches the engine before the
next page is fetched. Memory is bounded by the chunk size rather than by the
range, and no page holds a transaction snapshot open — a cursor spanning the whole
scan would block vacuum on the hot table for its duration.

With no time predicate at all, both tiers are scanned. An unbounded scan over a
multi-billion-row history is almost always an accident.

### Skipping version resolution

Within a tier a key may have several versions, and resolution is latest-wins
within a scope. The interesting optimisation is **not doing it**: Iceberg keeps
per-file `min`/`max` statistics, so where they are equal across every file in
range, no key has a correction and the scan runs directly — no window function, no
sort, no repartition.

Corrections are rare and concentrated in recent months, so most historical
partitions take the direct path. Elision is conservative: statistics must *prove*
absence, and a missing statistic counts as proof of nothing.

It applies only to scans entirely below the watermark. That is not caution — it is
what makes per-tier reasoning sound. Both versions of a corrected reading always
live in the same tier, because the tiers hold disjoint ranges and `append` routes
a late correction to the tier that owns its interval. The hot tier is excluded for
a practical reason instead: PostgreSQL keeps no per-file statistics, and proving
the hot window correction-free would mean scanning exactly the rows the
optimisation was meant to avoid.

`EXPLAIN` shows the split, and whether resolution was elided.

## The typed series API

The unit of work is `metering`'s `MeasurementSeries`, so a caller that wants
`aggregate(&series.intervals, …)` need not decode Arrow to get there.

```rust
let series: Option<MeasurementSeries> = store
    .series("12345678901")
    .obis("1-0:1.8.0")?              // canonicalised on the way in
    .range(from, to)
    .quality_in(&[QualityFlag::Measured, QualityFlag::Substituted])
    .collect()
    .await?;
```

| Terminal | Returns |
|---|---|
| `.collect()` | `Option<MeasurementSeries>` |
| `.collect_with_sparte()` | …plus the commodity |
| `.collect_resolved()` | …plus the declared attribute/identity columns |
| `.latest()` | The newest interval, via `ORDER BY … DESC LIMIT 1` — not a full scan |
| `.intervals()` | Just the intervals, empty when the range holds none |
| `.collect_with_provenance()` | The series and the `QueryResult` |

**`collect` returns `Option`, and that is not an oversight.** A
`MeasurementSeries` asserts a `source` — who reported these values — and with no
values there is nobody to name. Fabricating one would put a delivery in the audit
trail that never happened. Use `.intervals()` when an empty range genuinely means
zero, and [completeness](@/docs/completeness.md) to find out *why* it is empty.

`.obis()` canonicalises because the code is part of the merge key: `1-0:1.8.0` and
`1-0:1.8.0*255` denote the same channel, and a literal comparison against the
stored spelling would silently return nothing.

`.column_eq(name, value)` scopes a read to an identity column. A measuring point
is only unique *within* the columns that join the merge key, so on a shared store
an unscoped read spans tenants and folds them into one series.

## Calendar functions

Daily and monthly aggregation must use **local** calendar days.

`Europe/Berlin` gives 92-interval and 100-interval days across the DST
transitions — and, more insidiously, the UTC day boundary sits at 01:00 or 02:00
local, so grouping on UTC days produces wrong daily sums *every* day of the year.

```sql
SELECT meter_local_day("from") AS day, SUM(value) FROM readings GROUP BY 1;
```

| Function | Returns |
|---|---|
| `meter_local_day(ts)` | The Berlin calendar day, as `Date32` |
| `meter_local_month(ts)` | The Berlin month, as its first day |
| `meter_expected_intervals(day, resolution)` | 96 normally, 92 in spring, 100 in autumn |

MeterStore does not implement any of this. The arithmetic is
`metering::calendar`'s, and these are thin wrappers whose tests assert that the
wrapper preserves the upstream answer — not that the answer is right, which is the
domain library's job.

## Several tables in one session

Each table owns its own watermark, archiver and lease; nothing is transactional
across them, and that is a design position rather than a limitation. What
`MeterCatalog` adds is the ability to *express* a question spanning two of them:

```rust
let catalog = MeterCatalog::builder()
    .table(readings_builder)
    .table(esa_typ2_builder)
    .build()
    .await?;

catalog.query("SELECT … FROM readings r JOIN esa_typ2 e USING (malo_id)").await?;
catalog.table("readings").unwrap().archive(now, 8).await?;   // still its own
```

The shape is not hypothetical: an EDM deployment holds authoritative billing
readings **and** a second stream that must never reach a billing query — ESA
"Werte nach Typ 2" are non-authoritative by Codeliste, so keeping them in a
separate table is what stops a `SUM` reaching them by omission rather than by
policy.

A result carries the boundaries of the tables its statement actually read. The
scalar `watermark()` is the conservative one — the oldest, below which every table
involved is settled — and attributing untouched tables to it would make that
number meaningless in a catalogue of any size.

A catalogue's tables must share one read mode: `Historical` reads no PostgreSQL,
so joining a historical table to a unified one would silently mix a reproducible
half with a mutable one.
