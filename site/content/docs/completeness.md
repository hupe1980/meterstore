+++
title = "Completeness"
description = "A missing interval is information, not an empty set. DST-aware gap detection as a first-class query."
weight = 7
+++

Regulators require knowing whether a series is complete, so **an aggregate over an
incomplete month must not look like one over a complete month**. A `SUM` cannot
say that — it returns a smaller number and no reason.

```rust
for row in store.completeness(from, to).await? {
    if !row.is_complete() {
        tracing::warn!(
            malo = %row.malo_id, obis = %row.obis_code, sparte = %row.sparte,
            expected = row.expected, actual = row.actual,
            missing = row.missing, surplus = row.surplus,
            first_gap = ?row.first_gap,
            "incomplete series",
        );
    }
}
```

`malo`, `melo`, `obis` and `column_eq` narrow it — in the **scan**, not in the
answer, so a question about one meter does not cost a scan of the portfolio:

```rust
store.completeness(from, to).malo("41373559241")?.await?;
store.completeness(from, to).melo(melo)?.await?;   // one meter of a Mehrfamilienhaus
```

Identifiers are parsed and canonicalised, so a mistyped one fails at the call
rather than returning an empty report. Prefer `melo` to `column_eq("melo_id", …)`:
a lower-cased literal there narrows to nothing and reports every channel
**silent**. The narrowing applies to the
[roster](#the-finding-a-range-cannot-make-about-itself) too.

Or as a table function:

```sql
SELECT * FROM meter_completeness('2026-03-01', '2026-04-01')
WHERE NOT complete OR NOT measurable;
```

A bare date is the start of that day **in Berlin**: `'2026-03-01'` is
23:00 UTC on 28 February, so the range above is the electricity month. A gas
range starts at 06:00 and is written as instants (`'2026-03-01T05:00:00Z'`); a
typed `TIMESTAMP` is taken as the instant it is. Over a
[catalog](@/docs/querying.md) with several tables the first argument names the
table — `meter_completeness('readings', '2026-03-01', '2026-04-01')` — and a
call without one is refused rather than answered about whichever table a default
would pick. The result's boundary and tiers are that table's.

`OR NOT measurable` matters: a channel with no fixed daily expectation is
`complete` (`missing = 0`, `surplus = 0`), so `WHERE NOT complete` alone drops
exactly the channels nobody could judge.

| Column | Meaning |
|---|---|
| `malo_id`, `obis_code` | The measuring point and the channel |
| *merge-key columns* | One column per declared identity column, and `melo_id` where the table [identifies a reading by its Messlokation](@/docs/storage-model.md#the-messlokation-may-be-part-of-the-identity) |
| `sparte` | The commodity — and therefore **which day** the row is measured on |
| `resolution` | The declared interval grid — and therefore **how many** values a day should hold. Grouped on, so it is this row's grid rather than one of several |
| `expected` | What the DST-aware calendar says the range should hold — **every** balancing day of it, not only the days that produced rows |
| `actual` | What is stored |
| `missing` | Intervals short, **summed per balancing day** |
| `surplus` | Intervals beyond the expectation, summed per balancing day |
| `first_gap` | The earliest short **balancing** day |
| `substituted` | Intervals carrying an Ersatzwert |
| `not_billable` | Intervals whose quality bars them from billing |
| `complete` | `missing == 0 && surplus == 0` — true, and not the same as "checked" |
| `measurable` | Whether there was an expectation to compare against at all |

`actual = 0` is a channel that delivered nothing — `is_silent()` in Rust — and
appears only when the query has a [reference window](#the-finding-a-range-cannot-make-about-itself).

## Every day of the range

The expectation covers every balancing day between `from` and `to`: a day with no
rows is a day whose whole expectation is missing, so a meter that stopped on
2 March does not report the month complete.

> **A range reaching past the last delivery says so.** A whole-month report run
> mid-month reports the remainder as missing; it consults no clock. Ask about
> periods that are over, or end the range at the last settled instant.

### Which grid an absent day belongs to

There is one row per channel **per grid**. A day nobody delivered on is charged
to the grid **last in force before it**, and a gap before the first delivery to
the first grid used — not to every grid, which would report a clean conversion as
two incomplete halves.

## The finding a range cannot make about itself

A channel with **no** rows in the range appears nowhere — a meter that stopped
entirely, a pipeline that dropped a Bilanzkreis. The roster comes from an earlier
window:

```rust
// Anything that reported in the month before this one is expected in it.
let report = store
    .completeness(from, to)
    .seen_since(from - Duration::days(30))
    .await?;

for gone in report.iter().filter(|r| r.is_silent()) {
    tracing::error!(malo = %gone.malo_id, obis = %gone.obis_code, "delivered nothing");
}
```

Such a channel comes back with `actual = 0`, the whole range `missing`, and
`first_gap` on its first balancing day. In SQL the reference window is the
**leading** argument:

```sql
-- Reported: March. Expected: whatever reported in February.
SELECT * FROM meter_completeness('2026-02-01', '2026-03-01', '2026-04-01')
WHERE actual = 0;
```

**The window is yours**: too short and a monthly-read meter looks decommissioned,
too long and every terminated measuring point is a standing finding. A channel
whose *grid* changed is not called silent.

## From the command line

```bash
meterstore completeness --month 2026-06 --seen-since 30d --gaps-only
```

`--month YYYY-MM` is the **Bilanzierungsmonat** — cut at midnight local for
electricity, 06:00 for gas with `--sparte GAS` — and `--malo`, `--melo` and
`--obis` narrow it, each parsed at the call. It exits zero whatever it finds;
`--format json` carries the rows plus `channels_incomplete`, `channels_silent`,
`channels_unmeasurable` and `intervals_missing`.
[The CLI →](@/docs/cli.md#asking-whether-a-month-is-complete)

## One row per reading, not per measuring point

The report groups by the **merge key**, so identity columns and a key-carrying
`melo_id` are both grouped on and reported — `row.identity` carries them in key
order.

Folded, two tenants or two meters delivering a full day would report a `surplus`
of 96, and one of them four short could net to **complete**. Scoping the session
narrows the report as it narrows a query.

### And one row per interval grid

`resolution` is grouped on because the *grid* decides whether a day of 24 values
is complete or 72 short; a meter converted from hourly to quarter-hourly
mid-month comes back as two complete rows.

**The day both grids delivered on is judged by the time it covers**: time no row
covers is `missing`, time more than one row covers is `surplus`, both in the finer
grid's intervals, and the day is charged to the grid in force at its end. A clean
conversion is complete; a partial re-grid shows as surplus.

## Why the expected count is not 96

`Europe/Berlin` gives **92**- and **100**-interval days at the DST transitions; a
check assuming 96 masks a genuine four-interval gap every autumn. The count is
`metering`'s `Period::count` over the balancing day at the declared `resolution` —
**1 500** for a one-minute grid in autumn. A **daily** series expects one interval
per day. A resolution coarser than a day (`P1M`, `P1Y`) has no fixed count, so
the row is not measurable (`is_measurable()`).

## And why the day is not always the calendar day

Gas balances on the **Gastag** — 06:00 to 06:00 local — so both the bucketing and
the expected count follow the row's `sparte` (`metering`'s `DayBoundary`, mapped
in `planner::day_boundary`). The clocks change *before* 06:00, so the long and
short gas days are named after the **Saturday**:

| 2026 | Calendar day | Gastag |
|---|---|---|
| Sat 24 Oct | 96 | **100** |
| Sun 25 Oct | **100** | 96 |

Heat and water stay on the calendar day.

## Four decisions this made concrete

**`first_gap` names a balancing day**, usually *not* the UTC one: six intervals
missing from the end of UTC 2026-07-19 are 00:30–02:00 Berlin on the 20th. For gas
it names the Gastag.

**It reads the resolved table**, so a corrected interval counts once.

**Surplus is not a negative gap, and both are summed per day**, so a long day
cannot net against a short one. `missing` and `expected - actual` can
legitimately disagree.

**No resolution means no expectation.** A series declaring none, or a calendar one
like `P1M`, reports `actual` and is `complete` **and** not `measurable`, in SQL and
Rust alike.

A range that is not day-aligned expects only what it covers on each end day,
counting **interval starts inside the range**: over `[00:00, 00:07)` the interval
at 00:00 is both expected and counted.

## Where the work happens

Grouping by measuring point, channel, commodity, grid and the stored balancing
day is one DataFusion aggregate over the resolved table, pruned and streamed like
any query. The roll-up to one row per channel happens in Rust, where billability
(§ 60 Abs. 2 MsbG) is asked of `metering` rather than restated in SQL. The
expectation is computed once per `(Sparte, resolution)`, and no absent day is
materialised.
