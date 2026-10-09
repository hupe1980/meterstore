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
    WHERE malo_id = '41373559241'
      AND "from" >= '2025-01-01' AND "from" < '2026-01-01'
    GROUP BY 1 ORDER BY 1
"#).await?;
```

`sql` returns DataFusion's own `DataFrame`. Caller-supplied values are bound as
parameters, never concatenated into the SQL text:

```rust
let result = store.query_with_params(
    "SELECT SUM(value) FROM readings WHERE malo_id = $1",
    vec![ScalarValue::Utf8(Some(malo.into()))],
).await?;
```

> **If the statement itself comes from outside** — an ad-hoc endpoint, a Flight
> SQL client, a user's report — confinement has to be a property of the
> *session*. Read [Confining a session](#confining-a-session) before exposing one.

## Results carry their provenance

Two identical queries a minute apart can read the same rows from different tiers,
so a result carries *which boundary was in force*:

```rust
let result = store.query(sql).await?;
result.watermark();         // the boundary this ran against
result.watermarks();        // per table, when a statement spans several
result.tiers_scanned();     // [Cold] | [Hot] | [Cold, Hot]
result.spans_tiers();
result.touched_hot_tier();  // the answer is only valid for now
result.read_mode();
```

Both are read off this query's **physical plan** before execution, not asked of
the providers afterwards, which would race other queries. Nodes are matched by
type rather than name, so a renamed node in a dependency fails to compile rather
than silently misreporting the tiers.

### Describing a statement without running it

```rust
let described = store.describe(sql).await?;
described.schema();          // what it would produce
described.watermark();       // the boundary it would run against
described.tiers_scanned();   // which tiers it would read
```

Plans the query — reporting a syntax error or unknown column or relation — and
stops. A surface needing a schema *before* any row calls this, so a Flight
client's `GetFlightInfo` → `DoGet` costs one scan, not two. Planning reads the
tier boundary and, for the resolved table, per-file statistics: a catalogue read,
not a scan.

### Streaming, when the rows *are* the answer

```rust
let (described, mut rows) = store.stream(sql).await?;

described.watermark();              // in hand before the first batch
while let Some(batch) = rows.next().await { … }
```

`query` collects every batch, which suits a settlement total, a daily curve or a
completeness report. For an export or a BI pull, where the rows are the answer,
`stream` hands back a stream that has not run yet: peak memory is one batch. The
`QueryDescription` — the same type `describe` returns — comes **first, before any
row**, which is what lets Flight SQL write the schema, watermark and tiers before
the first batch.

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

**Predicate extraction is conservative**: anything not provably a bound on `from`
widens the range, and `OR` discards bounds entirely, because a missed bound costs
a wider scan and a wrongly inferred one loses rows. A property test over
generated filter trees asserts it.

The cold half delegates to `iceberg-datafusion`, so partition pruning and file
statistics apply. The hot half is paged by keyset, one batch per page, so memory
is bounded by the chunk size and no snapshot is held open across the scan to block
vacuum. With no time predicate, both tiers are scanned in full.

### Skipping version resolution

Resolution is latest-wins within a scope. Where Iceberg's per-file `min`/`max`
statistics prove no merge key can appear twice, the scan skips it — no window
function, no sort, no repartition. Two things have to hold:

1. **Every file in range holds a single version.** A file spanning versions
   contains a correction on its own.
2. **Two files may only disagree about that version when their `from` bounds do
   not overlap.** `from` is in the merge key, so files covering different days
   cannot hold the same key however their versions differ.

A year of daily archival is 365 files at 365 versions, disjoint on `from`, so the
second rule is what lets it fire. A late correction breaks it, correctly: its file
overlaps an archived day at a higher version. A missing statistic proves nothing,
a file that will not state its intervals overlaps every other, and files are
grouped into runs of overlap — so elision can cost a window function but never
skip a needed one.

A **multi-tenant** table does not benefit: one file per tenant per window, all
covering the same day. The partition tuple could prove the files disjoint only if
the spec is the one MeterStore wrote, and a table repartitioned out of band could
not be trusted, so the inference is not made.

Elision applies only to scans entirely below the watermark. It is sound per tier
because every version of a reading lives in the tier that owns its interval;
PostgreSQL keeps no per-file statistics, so the hot tier always resolves.

`EXPLAIN` shows the split, and whether resolution was elided.

## The typed series API

The unit of work is a `MeasurementSeries` — `metering`'s `MeterInterval`s with
the delivery's identifiers, origin and trail — so no caller decodes Arrow.

```rust
let series: Option<MeasurementSeries> = store
    .series("41373559241")?          // check digit verified on the way in
    .obis("1-0:1.8.0")?              // canonicalised on the way in
    .range(from, to)
    .quality_in(&[QualityFlag::Measured, QualityFlag::Substituted])
    .collect()
    .await?;
```

`.series()` accepts a string, which it parses, or an already-parsed
`metering::MaloId`. **The parse is the point**: past here a transposed digit
returns an empty series and no error. The decoder parses `malo_id` and `melo_id`
back into `MaloId`/`MeloId` too, so a row from a bulk load or hand-run `INSERT`
cannot enter the typed path as an identifier it is not.

| Terminal | Returns |
|---|---|
| `.collect()` | `Option<MeasurementSeries>` |
| `.collect_with_sparte()` | …plus the commodity |
| `.collect_resolved()` | …plus the declared attribute/identity columns |
| `.collect_by_channel()` | `BTreeMap<ObisCode, ResolvedSeries>` — **every** channel, one scan |
| `.channels()` | The channels this range holds, without decoding an interval |
| `.latest()` | The newest interval, via `ORDER BY … DESC LIMIT 1` — not a full scan |
| `.intervals()` | Just the intervals, empty when the range holds none |
| `.collect_with_provenance()` | The series and the `QueryResult` |

**`collect` returns `Option`**: a `MeasurementSeries` names a `source`, and with
no values there is nobody to name. Use `.intervals()` when an empty range means
zero, and [completeness](@/docs/completeness.md) to find out *why* it is empty.

`.obis()` canonicalises: `1-0:1.8.0` and `1-0:1.8.0*255` are one channel, and a
literal comparison against the stored spelling would return nothing.

### A series is one channel of one reading

`collect` **refuses** a range spanning two channels or two readings.

`series()` filters by measuring point, which is coarser than the merge key: import
*and* export, a row per tenant, a row per meter. Folded into one
`MeasurementSeries`, `aggregate` would sum both. So the read says which it means:

```rust
store.series(malo)?.obis("1-0:1.8.0")?                       // one channel
store.series(malo)?.column_eq("tenant", tenant)?             // one reading
let confined = store.scoped("tenant", "a").await?;           // …or the session
```

`.column_eq(name, value)` accepts any **merge-key** column — a declared identity
column, or `melo_id` on a table that
[identifies a reading by its Messlokation](@/docs/storage-model.md#the-messlokation-may-be-part-of-the-identity).
An attribute column never splits a series: a Bilanzkreis reassigned mid-range is
one series with a changed attribute.

#### And one no narrowing can fix

Two values at one instant that are *not* two readings: resolution partitions by
the merge key **and `version_scope`**, so two network operators for one reading
leave two winners agreeing on channel and discriminators alike. The fold refuses
those with `InvariantViolated`: the stored rows are already wrong. Both write paths
refuse a second operator, so reaching it means
[integrity constraints are off](@/docs/storage-model.md#the-constraints-that-stop-a-wrong-number)
or something else wrote the rows. A `SUM` written in SQL is not covered — it will
be twice the truth.

### …but a measuring point is a set of them

Some questions mean the **whole point**: a billing period across HT, NT and total,
a Mehr-/Mindermengensaldo, an audit of a delivery.

```rust
let point = store.series(malo)?.range(from, to).collect_by_channel().await?;
for (channel, resolved) in &point {
    println!("{channel}: {} intervals", resolved.series.intervals.len());
}
```

**One scan**, split by channel, so every channel is resolved against **one**
boundary (`collect_by_channel_with_provenance` returns it) — unlike `SELECT
DISTINCT obis_code` plus a read each, which observes N boundaries.

**It still refuses to fold two readings.** Two tenants reporting `1-0:1.8.0` are
two readings; narrow with `.column_eq(..)`. `.channels()` lists the channels as a
`SELECT DISTINCT`, takes `&self`, and is narrowed by everything the builder is.

**A second Messlokation is a reading too**, on a Lastgang table declaring
`identify_by_melo(true)` — two meters under one Marktlokation, both carrying
`1-0:1.8.0` at the same instants. `.melo(..)` is the narrowing for that one:

```rust
let one_meter = store.series(malo)?
    .melo(melo)?                       // a MeloId, a &str or a String
    .obis("1-0:1.8.0")?
    .range(from, to)
    .collect().await?;
```

It **parses** where `.column_eq("melo_id", …)` does not: a truncated or
lower-cased literal would match nothing, indistinguishable from a meter that
reported nothing. An unnarrowed read over two meters is refused with *"spans two
readings … narrow the read with `.melo(..)`"* — or `.column_eq(..)` where the
difference is a tenant.

### Reading registers

```rust
let now = store.readings(malo)?
    .melo("DE0001234567890123456789012345678")?
    .obis("1-0:1.8.0")?
    .latest().await?;                                        // one row, not a scan

let month = store.readings(malo)?
    .melo(melo)?.range(from, to)
    .collect().await?;                                       // StoredReadings
```

A point table **identifies a reading by its Messlokation**, so over a
Marktlokation with two meters an unnarrowed `.collect()` is *refused*:
interleaving two cumulative sequences yields advances belonging to neither meter.
`latest` is `ORDER BY … DESC LIMIT 1` at the storage layer. `.deliveries()` returns
one `StoredReadings` per meter, channel and delivery, for an audit trail. The
per-register counterparts:

```rust
let registers = store.readings(malo)?.melo(melo)?.channels().await?;
let all       = store.readings(malo)?.melo(melo)?.collect_by_channel().await?;
```

## Calendar functions

Daily and monthly aggregation must use **local** calendar days: the UTC day
boundary sits at 01:00 or 02:00 in Berlin, so grouping on UTC days is wrong
*every* day, not only on the 92- and 100-interval DST days.

```sql
SELECT meter_local_day("from") AS day, SUM(value) FROM readings GROUP BY 1;
```

| Function | Returns |
|---|---|
| `meter_local_day(ts)` | The Berlin calendar day, as `Date32` |
| `meter_gas_day(ts)` | The **Gastag** — 06:00 to 06:00 local |
| `meter_balancing_day(ts, sparte)` | Whichever of the two the commodity uses |
| `meter_local_month(ts)` | The Berlin calendar month, as its first day |
| `meter_balancing_month(ts, sparte)` | The **Bilanzierungsmonat** — the same choice one period up |
| `meter_expected_intervals(day, resolution[, sparte])` | 96 normally, 92 in spring, 100 in autumn |

Every row also stores its balancing day, so `GROUP BY balancing_day` gives the
same buckets without a function call — and is what an engine reading the Iceberg
files directly uses ([external engines](@/docs/interop.md#the-gas-day-trap)).

The arithmetic is `metering::time::calendar`'s; these are thin wrappers over it.
That calendar covers the years 1900–9998, and each function returns `NULL` for an
instant outside them rather than guessing a rule.

### Gas is balanced on a different day

A **Gastag** runs 06:00 to 06:00 local time (GaBi Gas, following Art. 3 Nr. 16
VO (EU) 2017/459), so `meter_local_day` over a gas Lastgang books six hours a day
into the neighbouring Bilanzierungstag, with totals that still look plausible.

```sql
-- Right for a mixed table, and for a single-commodity one.
SELECT sparte, meter_balancing_day("from", sparte) AS day, SUM(value)
FROM readings
GROUP BY 1, 2;
```

`meter_balancing_day` reads `sparte` **per row**; `meter_gas_day` is the direct
form for a gas-only query.

The clocks change at 02:00/03:00 local — *before* 06:00 — so the 23- and 25-hour
gas days are the ones named after the **Saturday**, not the transition Sunday:

| 2026 | Calendar day | Gastag |
|---|---|---|
| Sat 24 Oct | 96 | **100** |
| Sun 25 Oct | **100** | 96 |
| Sat 28 Mar | 96 | **92** |
| Sun 29 Mar | **92** | 96 |

Pass `sparte` to `meter_expected_intervals` alongside `meter_balancing_day`, so
the bucketing and the expectation describe the same day. Heat and water stay on
the calendar day.

### The boundary carries up to the month

EDI@Energy *Allgemeine Festlegungen* v6.1c, Kap. 3.1
defines the Bilanzierungsmonat Juni 2021 as 01.06 00:00 to 01.07 00:00 for Strom
and 01.06 **06:00** to 01.07 **06:00** for Gas, so a gas month is a whole number
of Gastage rather than a calendar month shifted.

`meter_balancing_month(ts, sparte)` is `meter_balancing_day` one period up;
`meter_local_month` is the *calendar* month for every row.

```sql
SELECT sparte,
       meter_balancing_month("from", sparte) AS bilanzierungsmonat,
       SUM(value)
FROM readings
GROUP BY 1, 2;
```

An interval at 02:00 local on 1 March belongs to **February** for gas and to
March for everything else.

An external engine uses `date_trunc('month', balancing_day)` over the stored
column ([external engines](@/docs/interop.md#the-gas-day-trap)).

In Rust: `planner::balancing_month` is the month as its first day,
`planner::balancing_month_bounds` its half-open UTC range, and
`planner::bilanzierungsmonat(year, month, sparte)` that range addressed the way
the market addresses it — *"Juni 2026"*. Each returns an `Option`, `None` outside
the years 1900–9998, matching the SQL functions' `NULL`. It is the month an MSCONS
version scope is keyed to ([the storage model](@/docs/storage-model.md)).
`planner::day_boundary` maps a commodity to `metering`'s `DayBoundary`.

## Confining a session

`query` and `sql` run **caller-supplied SQL**, to which a service cannot reliably
add a tenant predicate. Three things confine a session instead, all injected into
the plan so no statement can omit, alias or `UNION` past them:

```rust
let tenant   = store.scoped("tenant", "a").await?;       // rows, one table
let billing  = catalog.isolated("readings").await?;      // relations
let confined = catalog.scoped("tenant", "a").await?;     // rows, every table
```

**`scoped` confines the rows.** The equality is enforced below the projection,
as a transaction-time ceiling is. `as_of` and `as_known_at` on a scoped store stay
scoped; re-scoping a column already fixed is refused, since a handle that could be
re-pointed at another tenant is not a boundary.

Only a **merge-key** column can scope a session — the declared identity columns,
plus `melo_id` where it identifies a reading. Such a column partitions *readings*,
so filtering before or after ranking gives the same winner; scoping on an
attribute would slice a version history apart and resolve to a different value.

**`isolated` confines the relations.** A catalog shares one `SessionContext`, so
any registered relation is reachable by naming it. An isolated session registers
one table's two relations and nothing else, so a statement naming another fails to
plan.

**`MeterCatalog::scoped` confines the rows of every table at once**; a join
plans with **both** sides carrying their own predicate:

```rust
let confined = catalog.scoped("tenant", tenant).await?;
confined.query("SELECT … FROM readings r JOIN esa_typ2 e USING (malo_id)").await?;
```

**Every table, or none.** A column missing from some table's merge key is
refused, naming that table, before any table is confined. A catalog whose tables
share no identity column uses `isolated` plus `MeterStore::scoped` instead.

Derived catalogs keep it: `as_known_at` and `in_read_mode` on a scoped catalog
stay scoped, and re-pointing a fixed column is refused on every table.

### And only queries run

Both boundaries live inside a table provider, and some of DataFusion's SQL never
touches one:

| Statement | What it reaches |
|---|---|
| `CREATE EXTERNAL TABLE … LOCATION '…'` | any path the process can read — **including the warehouse's own Parquet**, which is every tenant's rows, unscoped |
| `COPY (…) TO '…'` | any path the process can write |
| `CREATE TABLE … AS`, `INSERT`, `DROP`, `SET` | the session the other tables are registered in |

`ctx.sql` *executes* DDL as it plans it, so `query`, `sql` and `stream` plan
without running, refuse anything that is not a query, and only then execute.

`EXPLAIN SELECT` stays available; `EXPLAIN COPY … TO` does not, because planning
a `COPY` performs it. `store.context()` is the unrestricted door, for an
in-process caller that wants DataFusion itself.

Writes are confined too: `append`, `append_readings` and `hot_writer` on a scoped
store refuse a delivery naming another value of a scope column, because a late
correction is reconciled only against rows the session can see. Write another
tenant's readings through the unscoped store.

## OBIS predicates in SQL

OBIS is the other axis a metering aggregate cannot get right on its own:

```sql
SELECT malo_id, SUM(value)
  FROM readings
 WHERE obis_is_import(obis_code)          -- feed-in is a different register
   AND NOT obis_is_reactive(obis_code)    -- kvarh is not kWh
   AND NOT obis_is_maximum(obis_code)     -- D = 6 is a kW peak
   AND NOT obis_is_fehlerregister(obis_code)  -- E = 63 counts faults
 GROUP BY 1
```

| Function | |
|---|---|
| `obis_direction` | `'IMPORT'`, `'EXPORT'` or **null**. Value group **C**, and only for electricity |
| `obis_is_import` / `obis_is_export` | The same rule as two booleans, for a `WHERE` clause |
| `obis_is_reactive` | Blindarbeit — C = 3…8, the four quadrants included |
| `obis_is_lastgang` / `obis_is_zaehlerstand` / `obis_is_vorschub` / `obis_is_maximum` | Messart. D = 29 / 8 / 9 / 6 |
| `obis_is_fehlerregister` / `obis_is_total_register` | E = 63 is a fault counter; E = 0 is the total |
| `obis_tariff_register` | The tariff number, null for the total **and** for the fault counter |
| `obis_normalise` | The canonical spelling storage holds |

**They take no `sparte`:** `obis_is_import` tests value group A as well as C, so
it is false for a gas code, where C is a *Messgröße* rather than a direction.

### `obis_direction` is the primitive, and the two booleans are derived

**Both predicates are false for an undirected register** — Blindarbeit, a gas
volume, a Zustandszahl — so `NOT obis_is_import(obis_code)` does not mean
*export*. `obis_direction` gives the three-way grouping:

```sql
SELECT COALESCE(obis_direction(obis_code), 'UNDIRECTED') AS direction,
       SUM(value)
FROM readings
GROUP BY 1;
```

The strings are `metering::Direction`'s `serde` tags, so they compare literally
with a JSON payload. `metering::billing::aggregation::sum_by_direction` is the
Rust counterpart.

**There is no `obis_is_energy`**: composing a domain rule in the storage layer
starts a second implementation. Spell out the three predicates as above. The
total-vs-tariff rule stays in application code for the same reason.

## Quality is a code list, and the questions asked of it are statutory

`quality` holds `metering`'s own spelling — `MEASURED`, `SUBSTITUTED`,
`ESTIMATED`, `PRELIMINARY`, `FAULTY`, `CALCULATED`, `CORRECTED`, `UNKNOWN`.

| Function | |
|---|---|
| `quality_is_billable` | § 60 Abs. 2 MsbG, asked of the column |
| `quality_is_provisional` | Whether the value is still expected to change |
| `quality_market_code` | The MSCONS `QTY` Mengen-Qualifier, or **null** |

```sql
-- Not `quality IN ('MEASURED', 'SUBSTITUTED')`, which is a copy of a statute
-- that stops agreeing with it the day the list moves.
SELECT malo_id, SUM(value) FROM readings
 WHERE quality_is_billable(quality)
 GROUP BY 1;
```

`quality_market_code` is what a consumer building an MSCONS out of the warehouse
needs: `220` Wahrer Wert, `67` Ersatzwert, `187` Prognosewert, `Z18` Vorläufiger
Wert, `20` Nicht verwendbarer Wert.

**Null is an answer.** `CALCULATED`, `CORRECTED` and `UNKNOWN` have no qualifier
of their own, so these are the rows a message writer must resolve first:

```sql
SELECT DISTINCT quality FROM readings
 WHERE quality_market_code(quality) IS NULL;
```

A value outside the code list is an **error**, not a null.

## The other stored identifier

| Function | Returns |
|---|---|
| `eic_regelzone(code)` | `'TENNET'`, `'AMPRION'`, `'FIFTY_HERTZ'`, `'TRANSNET_BW'`, or null |
| `eic_object_type(code)` | The object-type letter — `'X'` party, `'Y'` area, … — or null |
| `eic_normalise(code)` | The canonical spelling, or null when the value is not an EIC at all |

A deployment declaring a `bilanzierungsgebiet` column ([checked
columns](@/docs/storage-model.md#checked-columns)) can group by its **Regelzone**
without a mapping table:

```sql
SELECT eic_regelzone(bilanzierungsgebiet) AS regelzone, SUM(value)
FROM readings
GROUP BY 1;
```

The rule is BDEW *Anwendungshilfe Energy Identification Codes* v1.0 §2.2.2: a
Bilanzierungsgebiet is a `Y` code under the German LIO `11`, and position 4 is the
Regelzone letter. Null for a Bilanzkreis, another issuing office's code, or a
string that is not an EIC, so one row of free text does not fail a report.

### What an EIC names is a query, not a constraint

Only **position 3**, the ENTSO-E object type, tells a Bilanzkreis from a
Bilanzierungsgebiet. A column can declare which it holds
([`EIC:X`](@/docs/storage-model.md#which-kind-of-eic-eic-x-and-the-rest)); where
it does not, this is the report:

```sql
SELECT DISTINCT bilanzkreis
FROM readings
WHERE eic_object_type(bilanzkreis) IS DISTINCT FROM 'X';
```

`'X'` a party, `'Y'` an area, `'Z'` a measurement point, `'W'` a resource object,
`'T'` a tie line, `'V'` a location, `'A'` a substation — the closed list of the
ENTSO-E *EIC Reference Manual* §4.2. German Bilanzkreise carry an `X`, which the
manual calls out as a national usage that remains valid.

`IS DISTINCT FROM` rather than `<>`, because the answer is three-valued.

### Two parsers, and the strict one runs first

`eic_object_type` returns null for two different findings:

1. the value is not an EIC at all — free text in an identifier column;
2. the value *is* an EIC, carrying an object-type letter this build's `metering`
   does not list.

`metering` parses the second tolerantly, because the list is ENTSO-E's to extend.
A **strict** parser elsewhere in the process may enumerate the seven letters and
reject, on a read, a row this store accepted. `eic_normalise(code)` separates the
two:

```sql
-- Not an EIC. Somebody wrote free text into an identifier column.
SELECT DISTINCT bilanzkreis FROM readings
WHERE bilanzkreis IS NOT NULL AND eic_normalise(bilanzkreis) IS NULL;

-- A well-formed EIC whose object type this build does not list — the rows a
-- strict downstream parser will reject.
SELECT DISTINCT bilanzkreis FROM readings
WHERE eic_normalise(bilanzkreis) IS NOT NULL
  AND eic_object_type(bilanzkreis) IS NULL;
```

Find them on your own schedule; declare `check = "EIC:X"` to stop accepting new
ones.

`eic_normalise` is also the EIC counterpart of `obis_normalise`: a checked column
holds only the trimmed uppercase form, so a literal join against a column written
elsewhere needs it.

## Several tables in one session

Each table owns its own watermark, archiver and lease; nothing is transactional
across them. `MeterCatalog` lets one statement span several:

```rust
let catalog = MeterCatalog::builder()
    .table(readings_builder)
    .table(esa_typ2_builder)
    .build()
    .await?;

catalog.query("SELECT … FROM readings r JOIN esa_typ2 e USING (malo_id)").await?;
catalog.stream("SELECT … FROM readings").await?;             // rows as the answer
catalog.describe("SELECT …").await?;                         // schema, no scan
catalog.table("readings").unwrap().admin().archive(now, 8).await?; // still its own
```

ESA "Werte nach Typ 2", for instance, are non-authoritative by Codeliste; a
separate table keeps them out of a billing `SUM` by construction.

A result carries the boundaries of the tables its statement read, subqueries
included; the scalar `watermark()` is the oldest of them. A statement that reads
no table reports the epoch. `stream` and `describe` are what let a catalogue be
served over [Flight SQL](@/docs/interop.md#flight-sql-and-when-not-to-use-it).

A catalogue's tables must share one read mode: joining a `Historical` table to a
unified one would mix a reproducible half with a mutable one.
