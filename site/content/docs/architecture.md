+++
title = "Architecture"
description = "The tiering watermark, why the two tiers are disjoint by construction, how archival stays crash-safe, and what the design deliberately does not do."
weight = 2
+++

## The boundary is a timestamp

```text
  MeterInterval.from ─────────────────────────────────────▶

  │◀──── Iceberg (cold, settled) ────▶│
                                      │◀── Postgres (hot) ──▶│
  epoch                       tiering_watermark            now
```

One timestamp per table. Everything below it is in Iceberg; everything at or
above it is in PostgreSQL.

> **The invariant:** PostgreSQL contains exactly the rows with
> `from >= tiering_watermark`. Iceberg contains exactly the rows with
> `from < tiering_watermark`.

A general HTAP design would federate an OLTP table against a lake table and
deduplicate by primary key — requiring an in-memory tier, an LSN watermark,
tombstones, and a merge operator whose cost needs continuous monitoring. Here the
tiers are disjoint by construction, and the correctness argument is one sentence:
**a row's `from` determines its tier, and the watermark is the only boundary.**

That is why a query spanning both tiers is a `UNION ALL` — not a union, not a
merge, not a join. It is asserted as a property over generated inputs, not only
by fixtures: every instant of a query's range lands in exactly one tier.

The invariant itself is checked continuously and exposed at runtime in
`system.tables`, because everything else depends on it.

## The watermark rides inside the snapshot

The classic tiering bug is purging the source before the destination is durable.
MeterStore closes that window by storing the watermark in the Iceberg **snapshot
summary**:

```rust
let action = txn.fast_append()
    .add_data_files(data_files)
    .set_snapshot_properties(HashMap::from([
        ("meterstore.tiering_watermark".into(), watermark.to_property()?),
        ("meterstore.archived_range".into(),    window.to_property()?),
        ("meterstore.row_count".into(),         rows.to_string()),
    ]));

action.apply(txn)?.commit(catalog).await?;
```

Iceberg commits are a compare-and-swap on the catalogue's metadata pointer, so
data and watermark become durable in one indivisible operation. There is no state
where one exists without the other, and no external checkpoint store to fall out
of sync.

Recovery is therefore trivial: read the watermark from the current snapshot,
everything below it is durable, resume from there. A crash mid-archival
re-archives a range that was never purged — idempotent, because the commit either
happened or did not.

### Library commit retry is disabled

`iceberg-rust` retries a conflicting commit by refreshing the base and
**re-applying the same action** — including the snapshot summary it was built
with. For an ordinary append that is
right. For a summary that describes the base it lands on, it is not: a late
correction losing a race to an archival commit would republish its own older
watermark, over intervals PostgreSQL had already purged, with nothing reporting a
failure.

MeterStore therefore disables the library's retry on the table
(`commit.retry.num-retries = 0`) and owns the loop, re-deriving the summary and
the monotonicity assertion from the refreshed base on every attempt.

## Archival, in order

```text
  read watermark W
  → target the closed partition covering [W, W + step)
  → DETACH PARTITION            (O(1); invisible to writers, readable by us)
  → SELECT … ORDER BY cursor    (streamed, keyset-paged)
  → write Parquet, commit Iceberg with watermark = W + step
  → leave the partition detached
  → assert the invariant

  …and on a later cycle, once no plan can still need it:
  → DROP TABLE partition        (O(1), no dead tuples)
```

Every step of that ordering is load-bearing:

- **Only closed partitions are archived.** Never up to `now()` — late data for
  the current window would land below the watermark. The horizon lags wall clock
  by `settlement_lag` (default 7 days), which must exceed the market's normal
  correction window.
- **Detach before scanning**, so no row can be inserted into a partition that is
  mid-archival. Invisible to *writers* — never to readers: the watermark is
  published by the cold commit, so throughout the scan the range still belongs to
  the hot tier and a query must still find those rows. It does, because the hot
  scan never reads the parent table — see below.
- **Commit cold, and do not drop hot.** The reverse ordering loses data
  permanently; dropping *immediately after* loses it from any query already in
  flight — see [the reader grace](#the-reader-grace).
- **Purge is `DROP TABLE`.** Removing 9.6 M rows a day with `DELETE` is a
  first-order PostgreSQL anti-pattern: dead tuples, WAL amplification, index
  bloat, autovacuum storms on a multi-billion-row table.
- **Rows stream, never materialise.** Peak memory is the chunk size, by
  construction rather than by tuning.

### The hot scan reads partitions, not the parent {#partition-wise-scanning}

A partition relation keeps its name, its rows and its identity when it is
detached; only its parentage changes. So the hot tier is scanned by enumerating
its partitions and reading each **by name**.

Scanning the parent and adding whatever is currently detached is two reads of one
catalogue, and a detach landing between them puts the partition in neither — a
whole window missing from the answer, silently.

A partition created after the enumeration is not scanned: it can only hold rows
written after the query was entitled to see them. Each partition's upper bound is
the *next partition's start* rather than the configured step, so a gap left by an
idle stretch over-estimates its reach. Over-inclusion costs a scan that returns
nothing; under-inclusion loses rows.

### The reader grace {#the-reader-grace}

A query decides its tier split from the watermark it reads when **planned**, and
reads the tiers when it **executes**. Archival between the two moves a window
across a boundary the plan has already committed to:

```text
  t0  plan at watermark W          → Postgres: [W, …)   Iceberg: […, W)
  t1  archival commits [W, W+1d)     watermark := W+1d
  t2  execute                      → the day is in neither half
```

So the purge is **deferred**. The partition stays detached after its commit —
invisible to writers, excluded by predicate from every plan made after the
advance, still read by any plan made before it — and a later cycle reclaims it
once two conditions hold: the commit that moved the boundary is older than
`reader_grace`, **and** no live plan is registered below the window.

```toml
[tables.archival]
reader_grace = "1h"    # hysteresis, kept whatever anybody is reading
max_pin_age  = "6h"    # above the longest query this deployment runs
```

The clock alone would be a probability argument: a plan reads the boundary when
it is *planned* and enumerates hot partitions on *first poll*, which for a query
drained cold-side-first is however long the cold scan takes. So a plan whose
split has a hot half registers the boundary it was cut at, in the hot tier's own
PostgreSQL, and `drop_partition` consults that registry inside the transaction it
drops in. `max_pin_age` bounds the registration, because a query killed with its
process never deregisters — past it the floor advances and the over-running query
fails naming the window rather than returning without it.

The clock is the one MeterStore records in the snapshot summary
(`meterstore.archived_at`), not Iceberg's own commit timestamp, so the grace is
measured on the same clock every other archival decision is.

What an interrupted run leaves behind is exactly what a successful one leaves
behind, so the recovery path is the ordinary path. A cold store that cannot report
snapshot times cannot date an orphan, and gets no grace.

### Exactly one archiver per table

The detach window is only safe because one process owns it. `PostgresHot`
enforces that with a session-scoped advisory lock held as an RAII lease over its
own pooled connection — an advisory lock is *session*-scoped, so one taken on a
connection that goes back to the pool would be released the moment another caller
checked it out.

Every replica can therefore run the same schedule: one wins, the others report
`lease_contended` and stop. That is not a failure and should not be alerted on;
watermark lag is the alert that fires if *nobody* is winning.

Query processes are unrestricted and hold no mutable state at all.

## The first run

A table with no snapshot has no watermark, which reads as the Unix epoch. Taken
literally, a deployment created today would archive one empty window per day
since 1970 before reaching any data.

So an archival window whose partition does not exist extends to the next
partition that does — or to the archival horizon if none does. One commit
whatever the gap. The same rule covers every later idle stretch: a commodity that
stops reporting, a backfill starting mid-history, a table quiet over a holiday.

## Late corrections

A correction for an already-archived interval must **not** go to PostgreSQL: it
would sit below the watermark, where no query looks, so the correction would be
silently ignored while appearing to have been accepted.

`MeterStore::append` routes by `from` against the current watermark — the same
rule the query path uses — and appends a below-watermark interval straight to
Iceberg without moving the boundary. Corrections are rare and batchable, so the
extra commits are cheap.

## Format version {#format-version}

MeterStore writes **Iceberg v2**, deliberately. v3 adds deletion vectors, row
lineage, `VARIANT` and nanosecond timestamps; evaluated one at a time:

| v3 feature | Value here |
|---|---|
| Deletion vectors | **None.** The store is append-only; it issues no deletes. |
| Row lineage | **Negative.** It would duplicate `version`, which is the better identifier: assigned by the network operator, meaningful to an auditor, stable across re-ingestion. |
| Default column values | Real but small — would make an added column free instead of needing a backfill. |
| Nanosecond timestamps | None. Fifteen-minute intervals are stored at microsecond precision. |

The deciding argument is reader support, checked rather than assumed: as of
August 2026 Athena — among the most widely deployed SQL engines in AWS estates
— creates and reads v2 only, and Trino is not v3-ready either. For data under a
ten-year retention obligation, writing a format a significant share of engines
cannot read would undermine the openness Iceberg is chosen for.

The version is **verified after table creation** rather than requested, because
`format-version` is a reserved property. A future library default moving to v3
fails loudly instead of silently migrating a decade of history.

## What this deliberately is not

| Not this | Why |
|---|---|
| A database | PostgreSQL and Iceberg are. No storage engine, no MVCC, no WAL of our own. |
| A CDC pipeline | Archival is the ingestion path, and there is no `ChangeSource` seam waiting for one. Logical decoding reads the WAL and touches no heap pages, which is a genuine advantage on a busy primary — but replicating rows out of Postgres does not *remove* them, so both designs still need the purge, and the purge is where the cost is. |
| An ingest transport | A transport parses a wire format, authenticates a producer and decides what a valid reading is. A storage layer does none of those. |
| A domain library | Validation, substitute values, gas conversion, aggregation and the DST calendar belong to [`metering`](https://crates.io/crates/metering). Duplicating one would create a second implementation to keep correct, and it would drift. |
| Distributed query execution | Single-process. Ballista exists if that changes. |
| Sub-second lake visibility | Metering arrives in batches; hours is correct. |
