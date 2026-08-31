+++
title = "Reproducibility"
description = "Reproducing a past settlement: pinned Iceberg snapshots with an enforced version ceiling, and the transaction-time axis that covers the hot window too."
weight = 6
+++

MaBiS settlement must be reproducible. "The settlement exactly as computed on the
8th working day" is the requirement, and incumbent EDM systems build it by hand
with versioned shadow tables.

MeterStore offers two answers, because there are two different questions.

## `as_of` — pin a snapshot

```rust
let then = store.as_of(
    SnapshotSelector::Timestamp(settlement_date),
    Some(max_version),
).await?;

let series = then.series("41373559241")?.range(from, to).collect().await?;
```

An Iceberg snapshot is a state of the whole cold table, so this reconstructs what
the store had been told at a commit. Three properties make it trustworthy rather
than merely available:

**It is cold-only by construction.** The hot tier keeps no history of itself, so
including it would make the answer depend on when the query ran — the opposite of
reproducible.

**The two axes are independent, and both are needed.** The snapshot pins
*transaction* time: what the store had been told. `max_version` pins the *domain*
version axis: which assertion was in force. A snapshot taken after a correction
landed holds both versions and resolution prefers the newer one, so without a
ceiling a rerun reproduces the store's current knowledge rather than the
settlement's inputs.

**The ceiling is enforced, not offered.** A filter handed to a DataFusion
`TableProvider` is advisory — the provider may prune on it and return the rows
anyway, because the engine normally re-applies it above the scan. This one is
injected by the tiered provider, so the engine does not know it exists. It is
therefore pushed down *and* applied by an explicit filter below the projection. A
rerun that silently ignored its own ceiling would return today's corrections under
the heading of a past settlement.

An unknown snapshot, or an instant predating the table's history, is an **error**.
Falling back to the current snapshot would produce a number that looks like a
settlement rerun and is not.

Find the snapshot without reading Iceberg metadata by hand:

```sql
SELECT snapshot_id, committed_at, watermark, written_by_meterstore
FROM system.snapshots;
```

## `as_known_at` — pin the transaction-time axis

```rust
let then = store.as_known_at(instant).await?;
```

`as_of` answers "what did the *cold table* look like at commit C". `as_known_at`
answers "what did we **believe** at instant T", and the difference is which axis
carries the answer:

| | Pins | Tiers | Granularity |
|---|---|---|---|
| `as_of` | An Iceberg snapshot, plus an optional `max_version` | **Cold only** | One commit — an archival window |
| `as_known_at` | The row-level `recorded_at` column | **Both** | One delivery |

`recorded_at` is the transaction-time axis every row carries, in both tiers. So a
transaction-time read needs no snapshot machinery and — the part `as_of`
structurally cannot do — it covers the **hot window**, which is where corrections
actually arrive.

"What was the settlement input on the 8th working day, including the corrections
that had landed by then but not the ones that landed after" is this mode, not
`as_of`.

It stays reproducible without pinning anything because archival only ever *moves*
a row between tiers, carrying `recorded_at` with it. A row recorded by T is
readable from one tier or the other however many archival runs have happened
since.

Three details the implementation forced:

- **The ceiling filters the inner scan, before ranking.** Resolution is
  latest-version-wins; applying the ceiling *above* it would rank every version,
  pick one recorded after T, and then discard the row — reporting an interval as
  absent rather than reporting the value that was in force.
- **It reaches the raw relation too.** A session that claims to reproduce a past
  state must not hand back rows it had not been told about, whether you query
  `readings` or the audit relation `readings_versions`.
- **It forces resolution.** Per-file `version` statistics say nothing about which
  version wins under a transaction-time ceiling, so elision is off for this mode.

## Across several tables

A settlement rerun spanning the authoritative readings and a second stream needs
one ceiling over both, and only one of the two modes can give it:

```rust
let then = catalog.as_known_at(instant).await?;   // every table, one axis
```

`recorded_at` is a **row-level** column every table carries, so a single instant
means the same thing in all of them. `catalog.in_read_mode(..)` is the general
form — `Historical` answers a reporting query across the whole catalog off the
lake, with no load on the operational database.

**`as_of` deliberately has no catalog counterpart.** It pins an Iceberg
*snapshot*, and a snapshot belongs to one table: there is no id that means the
same moment in two of them, and nothing commits two atomically — the consistency
model is explicit that nothing is transactional across tables. A catalog-wide
`as_of` would have to invent a correspondence between per-table snapshots and
then call the result reproducible. Pin each table with
`catalog.table(name).as_of(..)` and record both ids, or use `as_known_at`, whose
axis is genuinely shared. Asking for it returns an error that says so.

A scoped catalog stays scoped through either, because a boundary a derived session
drops is not a boundary.

## Which to reach for

| Question | Mode |
|---|---|
| "Reproduce the settlement exactly as it ran, from the snapshot we recorded" | `as_of` with the recorded `snapshot_id` |
| "What did we believe last Tuesday, including recent data still in Postgres?" | `as_known_at` |
| "The same, across every table in the deployment" | `MeterCatalog::as_known_at` |
| "What is true now, over settled history only, with no load on the database?" | `ReadMode::Historical` |
| "What is in the recent window?" | `ReadMode::Operational` |

## What a pinned session refuses

`append`, `append_readings`, `append_authoritative`, `hot_writer` and
`anonymise_before` are refused on any session that is not reading current best
knowledge — `as_of`, `as_known_at`, `Historical` and `Operational` alike. The rule
is not "no writes"; it is that an operation whose **decision is a query against
this session** must not run through one.

For the write path, the rows would land in the real table and it is the
*reasoning* about them that would be wrong. Every check it makes is a query
against the session that is writing — is this a replay, does this reading already
carry another network operator, what did the write displace — so through a pinned
handle each is answered from a state that is deliberately not current. A replayed
correction invisible in the pinned snapshot would be stored twice.

For the **retention sweep** the consequence is worse. A subject is erased when its
latest reading predates the cutoff, and that latest reading is `max("from")` over
the session running the sweep. Under `Historical` the hot window is invisible, so
a subject metered daily looks last-seen at the final archived interval — old
enough to erase, and still live. Erasure has no recovery path, so this is the one
place a restricted view destroys data rather than misreporting it.

Archival, snapshot expiry and `purge_table` are *not* refused, and the same rule
explains why: none of them decides anything from the session. They read the tiers
directly, so a restricted query surface cannot change what they do.

The store the pinned one was derived from is unaffected, so the fix is always to
keep hold of it rather than reaching back through the derived handle.

## What is not guaranteed

**Cross-table consistency.** Each table archives independently, so a query joining
two runs against two boundaries. A cross-table commit would mean a distributed transaction
between PostgreSQL and an Iceberg catalogue, which is the machinery this design
exists without. It is *exposed* rather than hidden —
`QueryResult::watermarks()` lists every boundary involved.

**Reproducibility across a schema change.** A pinned snapshot predating a schema
change has a different shape, and a read that silently changed shape would be
worse than one that fails. It fails, naming the reason.
