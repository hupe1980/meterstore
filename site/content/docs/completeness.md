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
            malo = %row.malo_id, obis = %row.obis_code,
            expected = row.expected, actual = row.actual,
            missing = row.missing, surplus = row.surplus,
            first_gap = ?row.first_gap,
            "incomplete series",
        );
    }
}
```

Or as a table function, for the operator holding a SQL client rather than a
compiler:

```sql
SELECT * FROM meter_completeness('2026-03-01', '2026-04-01') WHERE NOT complete;
```

| Column | Meaning |
|---|---|
| `expected` | What the DST-aware calendar says the range should hold |
| `actual` | What is stored |
| `missing` | Intervals short, **summed per local day** |
| `surplus` | Intervals beyond the expectation, summed per local day |
| `first_gap` | The earliest short **Berlin** day |
| `substituted` | Intervals carrying an Ersatzwert |
| `not_billable` | Intervals whose quality bars them from billing |
| `complete` | `missing == 0 && surplus == 0` |

## Why the expected count is not 96

`Europe/Berlin` gives **92**-interval and **100**-interval days at the DST
transitions. A check that assumes 96 raises a false alarm on every meter every
spring — and, worse, masks a genuine four-interval gap every autumn, which is the
direction that reaches a bill.

The count comes from `metering::calendar::intervals_in_day` against the series'
own declared `resolution`. At one value a minute the autumn expectation is
**1 500**, not 1 440.

## Four decisions this made concrete

**Grouped by the local day, and the report says so.** The expectation is defined
over a Berlin day, so `first_gap` names a Berlin day — which is usually *not* the
UTC one. Six intervals missing from the end of UTC 2026-07-19 are 00:30–02:00
Berlin on the 20th, and a report grouped on UTC days would send an operator to the
wrong day's delivery.

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

The heavy half — group a range by measuring point, channel and local day — is a
single DataFusion aggregate over the resolved table, so it prunes and streams like
any other query. The roll-up to one row per channel happens in Rust, where the
calendar lives and where billability can be *asked* of `metering` rather than
restated as a `CASE`. § 60 Abs. 2 MsbG is a statute; a second copy of it in SQL is
a second thing to keep in step with it.
