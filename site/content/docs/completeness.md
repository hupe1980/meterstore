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

`malo`, `obis` and `column_eq` narrow it — in the **scan**, not in the answer, so
a question about one meter does not cost a scan of the portfolio:

```rust
store.completeness(from, to).malo("41373559241")?.await?;
```

Identifiers are parsed and canonicalised, so a mistyped one fails at the call
rather than returning an empty report that reads as *"this meter is fine"*. The
narrowing applies to the [roster](#the-finding-a-range-cannot-make-about-itself)
too, or every channel outside it would come back as silent.

Or as a table function, for the operator holding a SQL client rather than a
compiler:

```sql
SELECT * FROM meter_completeness('2026-03-01', '2026-04-01') WHERE NOT complete;
```

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
| `complete` | `missing == 0 && surplus == 0` |

`actual = 0` on a row is a channel that delivered nothing at all — `is_silent()`
in Rust. Such a row exists only when the query was given a reference window, which
is the next section.

## Every day of the range

The expectation covers every balancing day between `from` and `to`, including days
the channel delivered nothing on: a day with no rows is a day whose whole
expectation is missing.

The aggregate underneath is a `GROUP BY`, which produces no group for an empty
day — so counting only the days that came back would measure a channel against
itself, and a meter that stopped on the 2nd of March would report the month
complete.

> **A range reaching past the last delivery says so.** A report over a whole
> month, run mid-month, reports the remainder as missing. Consulting a clock
> instead would make one report answer differently on two runs, which is what the
> [reproducibility](@/docs/reproducibility.md) machinery exists to prevent — so
> ask about periods that are over, or end the range at the last settled instant.

### Which grid an absent day belongs to

There is one row per channel **per grid**, so a day nobody delivered on has to be
charged to one of them. It goes to the grid **last in force before it**, and a gap
preceding the channel's first delivery to the first grid it used. Charging it to
every grid would report a clean hourly→quarter-hourly conversion as two badly
incomplete halves. For a single-grid channel — the ordinary case — this is simply
"the whole range".

## The finding a range cannot make about itself

The report is an aggregate over the rows the range holds, so a channel with **no**
rows produces no groups and appears nowhere — a meter that stopped delivering
entirely, a pipeline that dropped a Bilanzkreis, a market-location change nobody
carried through. Nothing inside the range can supply the missing roster, because
the missing roster is precisely what the range does not contain. It comes from an
earlier window:

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

Such a channel comes back with `actual = 0`, the whole range as `missing`, and
`first_gap` on its first balancing day — which is what an operator needs: not that
a value is absent, but from when.

In SQL the arguments read left to right in time order, so the reference window is
the **leading** one:

```sql
-- Reported: March. Expected: whatever reported in February.
SELECT * FROM meter_completeness('2026-02-01', '2026-03-01', '2026-04-01')
WHERE actual = 0;
```

**The window is yours, because there is no honest default.** Too short and a meter
read monthly looks decommissioned; too long and every terminated measuring point
is a standing finding. What a roster means — "still in service" — is master data
this crate does not hold.

A channel whose *grid* changed is still reporting and is not called silent: the
match is on what names a reading, not on the resolution it is delivered at.

## From the command line

The same question, without a compiler:

```bash
meterstore completeness --month 2026-06 --seen-since 30d --gaps-only
```

`--month YYYY-MM` is the **Bilanzierungsmonat** — cut at midnight local for
electricity, 06:00 for gas with `--sparte GAS` — and `--malo` / `--obis` narrow
it. It exits zero whatever it finds; `--format json` carries the rows plus
`channels_incomplete`, `channels_silent` and `intervals_missing`.
[The CLI →](@/docs/cli.md#asking-whether-a-month-is-complete)

## One row per reading, not per measuring point

The report groups by the **merge key**, so identity columns and a key-carrying
`melo_id` are both grouped on and reported — `row.identity` carries them in key
order.

That matters on a shared store. Two tenants, or the two meters of a
Mehrfamilienhaus, each deliver a full day of one channel: folded, that is 192
intervals against an expectation of 96 and a `surplus` of 96 where nothing is
wrong. Worse in the other direction — one of them four short nets against the
other's full day and the channel reads as **complete**, the one answer this
report must never give about a series with a real gap.

Scoping the session narrows the report as it narrows a query.

### And one row per interval grid

`resolution` is grouped on for the same reason `sparte` is: both decide which
expectation a row is measured against, and a channel can hold two of either. A
meter converted from an hourly profile to a quarter-hourly one mid-month is the
ordinary case, and it is the *grid* that says whether a day of 24 values is
complete or 72 short.

Folded, such a channel would report one arbitrary grid against a count drawn from
both — arbitrary literally, since the aggregate yields groups in no defined order.
Split, a clean conversion comes back as two complete rows, which is what it is.

## Why the expected count is not 96

`Europe/Berlin` gives **92**-interval and **100**-interval days at the DST
transitions. A check that assumes 96 raises a false alarm on every meter every
spring — and, worse, masks a genuine four-interval gap every autumn, which is the
direction that reaches a bill.

The count comes from `metering`'s own `DayBoundary::intervals_in_day` against the
series' declared `resolution`. At one value a minute the autumn expectation is
**1 500**, not 1 440. A resolution coarser than a day (`P1M`, `P1Y`) has no fixed
count within one, so the row reports what it found and declines to call it
complete or short — `is_measurable()` says which.

A **daily** series expects one interval per day, on both boundaries alike.

## And why the day is not always the calendar day

Gas balances on the **Gastag** — 06:00 to 06:00 local — so a gas channel's day is
not the day electricity's is. Both the bucketing and the expected count follow
the row's `sparte`, which is why it is a reported column rather than an implicit
assumption.

That choice is `metering`'s own `DayBoundary`. MeterStore adds one thing to it:
the mapping from a stored row's `sparte` to the boundary that applies, written
once in `planner::day_boundary`. Which of the two applies is a storage fact — a
row carries its commodity — rather than a calendar one.

This matters twice over at a DST transition. The clocks change at 02:00/03:00
local, *before* the 06:00 boundary, so the long and short gas days are the ones
named after the **Saturday**:

| 2026 | Calendar day | Gastag |
|---|---|---|
| Sat 24 Oct | 96 | **100** |
| Sun 25 Oct | **100** | 96 |

Reporting gas on calendar days would call the Saturday four intervals in surplus
and the Sunday four short — two findings, neither of them real, in the one report
an operator is meant to be able to trust. Heat and water stay on the calendar
day: the rule is *gas*, not *everything that is not electricity*.

## Four decisions this made concrete

**Grouped by the balancing day, and the report says so.** The expectation is
defined over a Berlin day, so `first_gap` names a Berlin day — which is usually
*not* the UTC one. Six intervals missing from the end of UTC 2026-07-19 are
00:30–02:00 Berlin on the 20th, and a report grouped on UTC days would send an
operator to the wrong day's delivery. For gas the same argument runs six hours
further: `first_gap` names the Gastag.

**It reads the resolved table.** Counting both versions of a corrected interval
would report a complete day as holding more intervals than the calendar allows.

**Surplus is not a negative gap, and both are summed per day.** More rows than
expected is a duplicate or a mis-declared resolution — a real and different
condition. Computed over range totals, a day four intervals long would net against
a day four intervals short and the channel would report *complete*, which is the
one answer a completeness report must never give about a series with a real gap.
So `missing` and `expected - actual` can legitimately disagree.

**No resolution means no expectation.** A series that declares none, or declares a
calendar one like `P1M` which has no fixed count within a day, reports `actual`
and admits it cannot judge. Assuming fifteen minutes would invent either a gap or
a completeness.

A range that is not day-aligned expects only the covered fraction of each end day,
so a billing period starting at noon does not report the morning as missing.

## Where the work happens

The heavy half — group a range by measuring point, channel, commodity, grid and
balancing day — is a single DataFusion aggregate over the resolved table, so it
prunes and streams like any other query. The balancing day is a **stored column**
rather than a function call, so the group-by is an ordinary column reference the
engine can collect statistics for.

The roll-up to one row per channel happens in Rust, where the calendar lives and
where billability can be *asked* of `metering` rather than restated as a `CASE`.
§ 60 Abs. 2 MsbG is a statute; a second copy of it in SQL is a second thing to
keep in step with it.

The roll-up walks the range's balancing days, so its cost is bounded by the
*period* as well as by the data. The expectation depends on `(Sparte, resolution)`
and never on the channel, so it is computed once per grid rather than once per
channel, and no absent day is materialised.
