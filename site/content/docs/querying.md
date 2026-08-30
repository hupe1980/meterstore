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

`sql` returns DataFusion's own `DataFrame`, so every expression, window function
and output format works, and there is no second query language to maintain.

Caller-supplied values are bound as parameters, never concatenated into the SQL
text, so a `malo_id` can come straight off a market message:

```rust
let result = store.query_with_params(
    "SELECT SUM(value) FROM readings WHERE malo_id = $1",
    vec![ScalarValue::Utf8(Some(malo.into()))],
).await?;
```

> **If the statement itself comes from outside** — an ad-hoc endpoint, a Flight
> SQL client, a report a user wrote — the confinement has to be a property of the
> *session*, because the statement is the thing you do not control. That is
> [Confining a session](#confining-a-session), and it is the section to read
> before exposing any of these three.

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

### Streaming, when the rows *are* the answer

```rust
let (described, mut rows) = store.stream(sql).await?;

described.watermark();              // in hand before the first batch
while let Some(batch) = rows.next().await { … }
```

`query` collects every batch before returning one, which is right for what this
store mostly produces — a settlement total, a daily curve, a completeness report
all fit in memory by construction. It is wrong when the rows are the answer: a
year of quarter-hour readings for a portfolio is millions of them, and an export
or a BI pull should not put the whole result in the server's heap.

`stream` plans the statement, reads the provenance off the plan, and hands back a
stream that has not run yet. Peak memory is one batch rather than the result —
the same bound archival keeps.

The `QueryDescription` comes back **first, before any row**, and that ordering is
what makes the streaming Flight SQL path possible: the schema, with the watermark
and the tiers on it, has to be written to the socket before the first batch. It
is the same type `describe` returns, so the two surfaces cannot disagree.

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
per-file `min`/`max` statistics, so where they show that no merge key can appear
twice, the scan runs directly — no window function, no sort, no repartition.

Two things have to hold:

1. **Every file in range holds a single version.** A file spanning versions
   contains a correction on its own.
2. **Two files may only disagree about that version when their `from` bounds do
   not overlap.** `from` is in the merge key, so files covering different days
   cannot hold the same key however their versions differ.

The second is what makes the optimisation fire at all. MSCONS versions ascend
*per delivery* and archival commits one day per window, so a year of history is
365 files at 365 different versions — disjoint, and therefore correction-free.
Requiring them all to carry the *same* version is sound and true of almost
nothing.

A late correction is what breaks the rule, and correctly: it appends a file
covering a day already archived, at a higher version, so the two overlap on `from`
and a key really does appear in both.

Elision stays conservative in both directions. Statistics must *prove* absence, a
missing statistic proves nothing, a file that will not say which intervals it
covers is treated as overlapping every other, and files are grouped into runs of
overlap rather than compared pairwise — which can cost a window function and can
never skip one that was needed.

A **multi-tenant** table is the case it does not help: one file per tenant per
window, all covering the same day, so their bounds overlap and their versions
usually differ. The partition tuple would settle it — every partition field is
derived from a merge-key column — but that rests on the spec being the one
MeterStore wrote, and a table repartitioned out of band could carry a field on an
attribute column. Being wrong there returns a superseded row, so the inference is
left unmade.

It applies only to scans entirely below the watermark, and two separate things are
going on there.

What makes it *sound* to reason about one tier at a time is that every version of
a corrected reading lives in the **same** tier — the tiers hold disjoint ranges,
and `append` routes a late correction to the tier that owns its interval. Without
that, "no corrections among the cold files" would say nothing about the reading as
a whole.

Why the *hot* tier is excluded is then merely practical: PostgreSQL keeps no
per-file statistics, so proving the hot window correction-free would mean scanning
exactly the rows the optimisation was meant to avoid.

`EXPLAIN` shows the split, and whether resolution was elided.

## The typed series API

The unit of work is `metering`'s `MeasurementSeries`, so a caller that wants
`aggregate(&series.intervals, …)` need not decode Arrow to get there.

```rust
let series: Option<MeasurementSeries> = store
    .series("41373559241")?          // check digit verified on the way in
    .obis("1-0:1.8.0")?              // canonicalised on the way in
    .range(from, to)
    .quality_in(&[QualityFlag::Measured, QualityFlag::Substituted])
    .collect()
    .await?;
```

`.series()` accepts either a string, which it parses, or a `metering::MaloId`
already parsed at the caller's own boundary, which costs nothing. **The parse is
the point.** A MaLo-ID carries a check digit precisely so that a transposition is
detectable, and this is the last place it can still be detected: past here, a
wrong-but-plausible identifier returns an empty series and no error at all. The
same reasoning applies on the way out — the decoder parses `malo_id` and
`melo_id` back into `MaloId`/`MeloId`, so a row that reached storage through a
bulk load or a hand-run `INSERT` cannot enter the typed path as an identifier it
is not.

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

**`collect` returns `Option`.** A
`MeasurementSeries` asserts a `source` — who reported these values — and with no
values there is nobody to name. Fabricating one would put a delivery in the audit
trail that never happened. Use `.intervals()` when an empty range genuinely means
zero, and [completeness](@/docs/completeness.md) to find out *why* it is empty.

`.obis()` canonicalises because the code is part of the merge key: `1-0:1.8.0` and
`1-0:1.8.0*255` denote the same channel, and a literal comparison against the
stored spelling would silently return nothing.

### A series is one channel of one reading

`collect` **refuses** a range spanning two channels or two readings.

`series()` filters by measuring point, and that is coarser than the merge key: a
meter reporting import *and* export carries two OBIS codes at the same instants,
a shared store a row per tenant, a Mehrfamilienhaus a row per meter. Each is a
second interval at the same instant, which `MeasurementSeries` cannot express —
folded, `aggregate` sums both and the month doubles.

So the read says which one it meant:

```rust
store.series(malo)?.obis("1-0:1.8.0")?                       // one channel
store.series(malo)?.column_eq("tenant", tenant)?             // one reading
let confined = store.scoped("tenant", "a").await?;           // …or the session
```

`.column_eq(name, value)` accepts any **merge-key** column — a declared identity
column, or `melo_id` on a table that
[identifies a reading by its Messlokation](@/docs/storage-model.md#the-messlokation-may-be-part-of-the-identity).
A column that is *not* in the merge key never splits a series: a Bilanzkreis
reassigned partway through a range is one series with a changed attribute, and
refusing that would make an ordinary correction unreadable.

#### And one no narrowing can fix

Two values at one instant that are *not* two readings: resolution partitions by
the merge key **and `version_scope`**, so two network operators for one reading
leave two winners agreeing on channel and discriminators alike. The fold refuses
those with `InvariantViolated` — nothing is being rejected at a boundary, so
something that should not be true already is, in stored rows.

Both write paths refuse a second operator, so reaching it means
[integrity constraints are off](@/docs/storage-model.md#the-constraints-that-stop-a-wrong-number)
or something other than MeterStore wrote the rows. It matters most on the
**register** path: a Zählerstandsgang is *differenced*, so an arbitrary one of the
two values lands on both sides of a subtraction and the consumption between two
reads means nothing.

A `SUM` written in SQL is not covered — it will simply be twice the truth.

### …but a measuring point is a set of them

Naming one channel is right when the caller means one. Plenty of questions mean
the **whole point**: a billing period projecting the canonical Bezug across HT, NT
and total, a Mehr-/Mindermengensaldo, an audit of what a delivery contained.

```rust
let point = store.series(malo)?.range(from, to).collect_by_channel().await?;
for (channel, resolved) in &point {
    println!("{channel}: {} intervals", resolved.series.intervals.len());
}
```

**One scan, not one per channel.** It runs the same single query `collect` does
and splits the decoded rows, so every channel in the map was resolved against
**one** boundary — `collect_by_channel_with_provenance` returns it. `SELECT
DISTINCT obis_code` plus a read each is `1 + N` round trips against N boundaries
observed at N different moments, with resolution and the tier split outside the
store.

**It still refuses to fold two readings.** A reading is `(channel, merge-key
discriminators)`, not a channel alone — so two tenants reporting `1-0:1.8.0` are
two readings, and splitting by channel alone would fold exactly what the refusal
prevents. Narrow with `.column_eq(..)` and the map is one entry per channel again.

`.channels()` answers the list on its own, as a `SELECT DISTINCT` rather than a
fold, and takes `&self` so the builder survives. Both are narrowed by everything
the builder was, including `.column_eq(..)`: an unscoped list would name channels
belonging to a tenant the read cannot see.

### Reading registers

```rust
let now = store.readings(malo)?
    .melo("DE0001234567890123456789012345678")?
    .obis("1-8-0")?
    .latest().await?;                                        // one row, not a scan

let month = store.readings(malo)?
    .melo(melo)?.range(from, to)
    .collect().await?;                                       // StoredReadings
```

`.melo(..)` matters here in a way it does not for a Lastgang: a point table
**identifies a reading by its Messlokation**, so a Marktlokation with two meters
returns two registers at every instant. An unnarrowed `.collect()` is *refused*
rather than folded — interleaving two cumulative sequences does not produce a
doubled sum, it produces advances belonging to neither meter.

`latest` is the question a register is actually asked, and it is `ORDER BY … DESC
LIMIT 1` at the storage layer rather than a history folded in memory — on the one
table § 146 Abs. 4 AO forbids discarding.

`.deliveries()` returns the unfolded shape: one `StoredReadings` per meter, per
channel and per delivery, which is what an audit trail wants.

And a meter is a set of registers, so the point-table counterparts are there too:

```rust
let registers = store.readings(malo)?.melo(melo)?.channels().await?;
let all       = store.readings(malo)?.melo(melo)?.collect_by_channel().await?;
```

One scan, split by register, with the same refusal kept: two *meters* carrying
`1-0:1.8.0` under one Marktlokation are two readings, and `.melo(..)` is what
makes them one.

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
| `meter_gas_day(ts)` | The **Gastag** — 06:00 to 06:00 local |
| `meter_balancing_day(ts, sparte)` | Whichever of the two the commodity uses |
| `meter_local_month(ts)` | The Berlin month, as its first day |
| `meter_expected_intervals(day, resolution[, sparte])` | 96 normally, 92 in spring, 100 in autumn |

Every row also stores its balancing day, so `GROUP BY balancing_day` gives the
same buckets without a function call — and is what an engine reading the Iceberg
files directly uses ([external engines](@/docs/interop.md#the-gas-day-trap)).

The arithmetic is `metering::calendar`'s; these are thin wrappers over it.

### Gas is balanced on a different day

The German gas market does not balance on the calendar day. A **Gastag** runs
06:00 to 06:00 local time (GaBi Gas, following Art. 3 Nr. 6 VO (EU) 312/2014),
so `meter_local_day` over a gas Lastgang is wrong in precisely the way
`date_trunc('day', …)` is wrong over an electricity one: it books the
00:00–06:00 draw into the neighbouring Bilanzierungstag, six hours a day, every
day, and the totals still look plausible.

```sql
-- Right for a mixed table, and for a single-commodity one.
SELECT sparte, meter_balancing_day("from", sparte) AS day, SUM(value)
FROM readings
GROUP BY 1, 2;
```

`meter_balancing_day` reads `sparte` **per row**, so one statement is correct
across a portfolio. `meter_gas_day` is the direct form for a query already
restricted to gas.

The clocks change at 02:00/03:00 local — *before* 06:00 — so the 23- and 25-hour
gas days are the ones named after the **Saturday**, not the transition Sunday:

| 2026 | Calendar day | Gastag |
|---|---|---|
| Sat 24 Oct | 96 | **100** |
| Sun 25 Oct | **100** | 96 |
| Sat 28 Mar | 96 | **92** |
| Sun 29 Mar | **92** | 96 |

Passing `sparte` to `meter_expected_intervals` gives the count for the day that
commodity is actually balanced on. Pair it with `meter_balancing_day`: the
bucketing and the expectation have to describe the same day, and mixing them
reports a surplus on one day and a gap on the next.

Heat and water stay on the calendar day. The rule is *gas*, not
*everything that is not electricity*.

### The boundary carries up to the month

It is not only a day. EDI@Energy *Allgemeine Festlegungen* v6.1c, Kap. 3.1
defines the Bilanzierungsmonat Juni 2021 as 01.06 00:00 to 01.07 00:00 for Strom
and 01.06 **06:00** to 01.07 **06:00** for Gas, so a gas month is a whole number
of Gastage rather than a calendar month shifted.

`planner::balancing_month` is the Rust form, and it is what an MSCONS version
scope is keyed to — see
[the storage model](@/docs/storage-model.md). In Rust the mapping from a
commodity to its boundary is `planner::day_boundary`, and everything below it is
`metering`'s own `DayBoundary`.

## Confining a session

`query` and `sql` run **caller-supplied SQL**, so a service exposing an ad-hoc
endpoint has no way to add a tenant predicate — and a deny-list of relation names
is a boundary that holds until someone adds a table.

Two things confine a session instead. Both inject into the plan, so no statement
can omit, alias or `UNION` past them:

```rust
let tenant   = store.scoped("tenant", "a").await?;       // rows
let billing  = catalog.isolated("readings").await?;      // relations
```

**`scoped` confines the rows.** The equality is enforced below the projection,
as a transaction-time ceiling is. `as_of` and `as_known_at` on a scoped store stay
scoped; re-scoping a column already fixed is refused, since a handle that could be
re-pointed at another tenant is not a boundary.

Only a **merge-key** column can scope a session, and that is version resolution
rather than taste. Such a column partitions *readings*: filtering before ranking
or after gives the same winner. An attribute column does not — a correction that
changed a Bilanzkreis would have its version history sliced apart, so the scoped
read would resolve to a value the unscoped one does not return. Fewer rows is the
intent; a different number is not.

In practice that is the declared identity columns, plus `melo_id` on a table that
identifies a reading by its Messlokation — which is how a single meter of a
Mehrfamilienhaus is handed to code that must not see the others.

**`isolated` confines the relations.** A catalog shares one `SessionContext`, so
any registered relation is reachable by naming it. An isolated session registers
one table's two relations and nothing else, so a statement naming another fails to
plan.

### And only queries run

Both boundaries above live inside a table provider, so they confine statements
that go through one. DataFusion's SQL surface is wider than `SELECT`, and some of
it never touches a provider:

| Statement | What it reaches |
|---|---|
| `CREATE EXTERNAL TABLE … LOCATION '…'` | any path the process can read — **including the warehouse's own Parquet**, which is every tenant's rows, unscoped |
| `COPY (…) TO '…'` | any path the process can write |
| `CREATE TABLE … AS`, `INSERT`, `DROP`, `SET` | the session the other tables are registered in |

`ctx.sql` *executes* DDL as it plans it, so the first two are one round trip from
a Flight SQL client. `query`, `sql` and `stream` therefore plan without running,
refuse anything that is not a query, and only then execute — which is what makes
the two boundaries above boundaries rather than conventions.

`EXPLAIN SELECT` stays available; `EXPLAIN COPY … TO` does not, because planning
a `COPY` performs it. `store.context()` is the unrestricted door, for an
in-process caller that wants DataFusion itself.

Writes are unaffected: `append` routes by `from` and carries the identity in the
row.

## OBIS predicates in SQL

The calendar functions exist because the Gastag and the DST-correct Berlin month
are things a caller must not re-derive. OBIS is the other axis a metering
aggregate cannot get right on its own:

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
| `obis_is_import` / `obis_is_export` | Direction. Value group **C**, and only for electricity |
| `obis_is_reactive` | Blindarbeit — C = 3…8, the four quadrants included |
| `obis_is_lastgang` / `obis_is_zaehlerstand` / `obis_is_vorschub` / `obis_is_maximum` | Messart. D = 29 / 8 / 9 / 6 |
| `obis_is_fehlerregister` / `obis_is_total_register` | E = 63 is a fault counter; E = 0 is the total |
| `obis_tariff_register` | The tariff number, null for the total **and** for the fault counter |
| `obis_normalise` | The canonical spelling storage holds |

**They take no `sparte`:** `obis_is_import` tests value group A as well as C, so
it is false for a gas code without being told — C is a *Messgröße* for gas, not a
direction. A second source for the medium could disagree with the one in the code.

**There is deliberately no `obis_is_energy`.** It would be a composition — not
reactive, not a maximum, not a fault counter — and composing a domain rule in the
storage layer is how a second implementation starts. Spell it out as above, where
a reader can see which three rules it rests on. The total-vs-tariff rule stays in
application code for the same reason: it describes two registers' coverage, not
one row.

## Several tables in one session

Each table owns its own watermark, archiver and lease; nothing is transactional
across them. What `MeterCatalog` adds is the ability to *express* a question spanning two of them:

```rust
let catalog = MeterCatalog::builder()
    .table(readings_builder)
    .table(esa_typ2_builder)
    .build()
    .await?;

catalog.query("SELECT … FROM readings r JOIN esa_typ2 e USING (malo_id)").await?;
catalog.stream("SELECT … FROM readings").await?;             // rows as the answer
catalog.describe("SELECT …").await?;                         // schema, no scan
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

"Actually read" comes from the logical plan **including its subqueries**: a table
named only inside `(SELECT COUNT(*) FROM readings)` hangs off an expression rather
than off the plan's inputs, and a statement attributed to no table reports the
epoch — that nothing has been settled.

`stream` and `describe` are the pair `MeterStore` has, and they are what lets a
catalogue be served over
[Flight SQL](@/docs/interop.md#flight-sql-and-when-not-to-use-it).

A catalogue's tables must share one read mode: `Historical` reads no PostgreSQL,
so joining a historical table to a unified one would silently mix a reproducible
half with a mutable one.
