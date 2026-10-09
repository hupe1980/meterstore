+++
title = "Reproducibility"
description = "Reproducing a past settlement: pinned Iceberg snapshots with an enforced version ceiling, and the transaction-time axis that covers the hot window too."
weight = 6
+++

MaBiS settlement must be reproducible: "the settlement exactly as computed on the
8th working day". MeterStore offers two answers to two different questions.

## `as_of` — pin a snapshot

```rust
let then = store.as_of(
    SnapshotSelector::Timestamp(settlement_date),
    Some(max_version),
).await?;

let series = then.series("41373559241")?.range(from, to).collect().await?;
```

An Iceberg snapshot is a state of the whole cold table, so this reconstructs what
the store had been told at a commit.

**It is cold-only.** The hot tier keeps no history of itself, so including it
would make the answer depend on when the query ran.

**The two axes are independent, and both are needed.** The snapshot pins
*transaction* time: what the store had been told. `max_version` pins the *domain*
version axis: which assertion was in force. A snapshot taken after a correction
landed holds both versions and resolution prefers the newer one, so without a
ceiling a rerun reproduces the store's current knowledge rather than the
settlement's inputs.

**The ceiling is enforced, not offered.** It is injected by the tiered provider,
which the engine does not know about, so it is applied as an explicit filter below
the projection rather than as an advisory pushed-down filter. It is offered to the
cold scan too, but `iceberg-datafusion` 0.10.1 does not translate decimals, so it
prunes no files.

An unknown snapshot, or an instant predating the table's history, is an
**error** rather than a fallback to the current snapshot.

Find the snapshot without reading Iceberg metadata by hand:

```sql
SELECT snapshot_id, committed_at, watermark, written_by_meterstore
FROM system.snapshots;
```

## `as_known_at` — pin the transaction-time axis

```rust
let then = store.as_known_at(instant).await?;
```

`as_of` answers "what did the *cold table* look like at commit C"; `as_known_at`
answers "what did we **believe** at instant T":

| | Pins | Tiers | Granularity |
|---|---|---|---|
| `as_of` | An Iceberg snapshot, plus an optional `max_version` | **Cold only** | One commit — an archival window |
| `as_known_at` | The row-level `recorded_at` column | **Both** | One delivery |

`recorded_at` is carried by every row in both tiers, so this mode covers the
**hot window**, where corrections arrive — "the settlement input on the 8th
working day, with the corrections landed by then and not after". It stays
reproducible because archival only *moves* a row between tiers, `recorded_at`
included.

- **The ceiling filters the inner scan, before ranking.** Above resolution it
  would discard a winner recorded after T and report the interval absent rather
  than the value then in force.
- **It reaches the raw relation too**, `readings_versions` as well as `readings`.
- **It forces resolution.** Per-file `version` statistics say nothing about which
  version wins under a transaction-time ceiling, so elision is off for this mode.

## Across several tables

A rerun spanning several tables needs one ceiling over all of them:

```rust
let then = catalog.as_known_at(instant).await?;   // every table, one axis
```

`recorded_at` means the same instant in every table. `catalog.in_read_mode(..)`
is the general form — `Historical` answers a reporting query across the catalog
off the lake, with no load on the operational database.

**`as_of` has no catalog counterpart**: a snapshot belongs to one table, and
nothing commits two atomically, so there is no id meaning the same moment in
both. Pin each table with `catalog.table(name)` (`None` for an undeclared name)
and its `as_of(..)`, recording each id, or use `as_known_at`. A scoped catalog
stays scoped through either.

## Which to reach for

| Question | Mode |
|---|---|
| "Reproduce the settlement exactly as it ran, from the snapshot we recorded" | `as_of` with the recorded `snapshot_id` |
| "What did we believe last Tuesday, including recent data still in Postgres?" | `as_known_at` |
| "The same, across every table in the deployment" | `MeterCatalog::as_known_at` |
| "What is true now, over settled history only, with no load on the database?" | `ReadMode::Historical` |
| "What is in the recent window?" | `ReadMode::Operational` |

## What a pinned session refuses

`append`, `append_readings`, `append_authoritative` and `hot_writer` are refused
on any session not reading current best knowledge — `as_of`, `as_known_at`,
`Historical` and `Operational` alike. The rule is that an operation whose
**decision is a query against this session** must not run through one: is this a
replay, does the reading carry another network operator, what did the write
displace. Through a pinned handle, a replayed correction invisible in the snapshot
would be stored twice.

The **retention sweep** (`MeterCatalog::anonymise_before`) is not refused: what
comes due is a calendar fact stored on the mapping row, not derived from readings,
so it gives the same answer through any handle. Archival, snapshot expiry and
`purge_table` (through `admin()`) read the tiers directly and are not refused
either. Write through the store the pinned one was derived from.

## What is not guaranteed

**Cross-table consistency.** Each table archives independently, so a query joining
two runs against two boundaries; `QueryResult::watermarks()` lists every one.

**Reproducibility across every schema change.** A snapshot pinned before a
nullable column was added still reads, with that column null. The changes that
quarantine a table are listed under
[schema evolution](@/docs/operations.md#schema-evolution).
