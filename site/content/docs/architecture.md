+++
title = "Architecture"
description = "The tiering watermark, why the two tiers are disjoint by construction, how archival stays crash-safe, and what the design deliberately does not do."
weight = 2
+++

## The boundary is a timestamp

```text
  MeterInterval::from() ──────────────────────────────────▶

  │◀──── Iceberg (cold, settled) ────▶│
                                      │◀── Postgres (hot) ──▶│
  epoch                       tiering_watermark            now
```

One timestamp per table. Everything below it is in Iceberg; everything at or
above it is in PostgreSQL.

> **The invariant:** PostgreSQL contains exactly the rows with
> `from >= tiering_watermark`. Iceberg contains exactly the rows with
> `from < tiering_watermark`.

The tiers are disjoint by construction — **a row's `from` determines its tier,
and the watermark is the only boundary** — so a query spanning both is a
`UNION ALL`: no deduplication by key, no tombstones, no merge operator. A property
test over generated inputs asserts that every instant of a query's range lands in
exactly one tier, and the invariant is checked at runtime and exposed in
`system.tables`.

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
data and watermark become durable together or not at all, with no external
checkpoint store to fall out of sync. Recovery reads the watermark from the
current snapshot and resumes; a crash mid-archival re-archives a range that was
never purged.

### Library commit retry is disabled

`iceberg-rust` retries a conflicting commit by **re-applying the same action**,
snapshot summary included. A late correction losing a race to an archival commit
would then silently republish its older watermark over intervals PostgreSQL had
already purged. MeterStore sets `commit.retry.num-retries = 0` on the table and
owns the loop, re-deriving the summary and the monotonicity check from the
refreshed base on every attempt.

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
- **Purge is `DROP TABLE`.** `DELETE` of 9.6 M rows a day means dead tuples, WAL
  amplification, index bloat and autovacuum storms.
- **Rows stream, never materialise.** Peak memory is the chunk size.

### The hot scan reads partitions, not the parent {#partition-wise-scanning}

A detached partition keeps its name and rows; only its parentage changes. So the
hot tier is scanned by enumerating its partitions and reading each **by name**.
Scanning the parent and adding whatever is detached would be two reads of one
catalogue, and a detach landing between them would silently drop a whole window.

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

The clock alone is not the safety argument: a plan enumerates hot partitions on
*first poll*, which for a query drained cold-side-first is however long the cold
scan takes. So a plan with a hot half registers the boundary it was cut at in the
hot tier's PostgreSQL, and `drop_partition` consults that registry inside the
transaction it drops in. `max_pin_age` bounds a registration, because a query
killed with its process never deregisters; past it, the over-running query fails
naming the window rather than returning without it.

The grace is measured from `meterstore.archived_at` in the snapshot summary, not
Iceberg's commit timestamp. An interrupted run leaves exactly what a successful one
does, so recovery is the ordinary path. A cold store that cannot report snapshot
times cannot date an orphan, and gets no grace.

### Exactly one archiver per table

The detach window is only safe because one process owns it. `PostgresHot` holds
a session-scoped advisory lock as an RAII lease on a connection checked out for
the run, and unlocks explicitly on it. If that unlock fails, the pooled connection
keeps every other archiver out until the pool recycles it or the `after_release`
hook `Settings` installs — `pg_advisory_unlock_all()` — clears it. **A deployment
building its own pool installs that hook itself.**

Every replica can run the same schedule: one wins, the others report
`lease_contended` and stop. That is not a failure; alert on watermark lag, which
fires if *nobody* is winning.

Query processes take no lease, but they do write: every plan with a hot half
registers the boundary it was cut at in `meterstore_pins` and removes it when the
scan ends, so a querying role needs `INSERT` and `DELETE` there, and a read-only
role or a hot standby cannot query the unified view.

## The first run

A table with no snapshot has no watermark, which reads as the Unix epoch. So an
archival window whose partition does not exist extends to the next partition that
does, or to the archival horizon: one commit whatever the gap. The same rule
covers every later idle stretch — a commodity that stops reporting, a backfill
starting mid-history.

## Late corrections

A correction for an already-archived interval must **not** go to PostgreSQL,
where it would sit below the watermark and no query would see it.
`MeterStore::append` routes by `from` against the current watermark — the query
path's rule — and appends a below-watermark interval straight to Iceberg without
moving the boundary.

## Format version {#format-version}

MeterStore writes **Iceberg v2**, deliberately. What v3 would add here:

| v3 feature | Value here |
|---|---|
| Deletion vectors | **None.** The store is append-only; it issues no deletes. |
| Row lineage | **Negative.** It would duplicate `version`, which is the better identifier: assigned by the network operator, meaningful to an auditor, stable across re-ingestion. |
| Default column values | Real but small — would make an added column free instead of needing a backfill. |
| Nanosecond timestamps | None. Fifteen-minute intervals are stored at microsecond precision. |

The deciding argument is reader support: as of August 2026 Athena creates and
reads v2 only, and Trino is not v3-ready. For data under a ten-year retention
obligation, a format many engines cannot read defeats the point of Iceberg.

The version is **verified after table creation**, because `format-version` is a
reserved property: a library default moving to v3 fails loudly instead of
silently migrating history.

## What this deliberately is not

| Not this | Why |
|---|---|
| A database | PostgreSQL and Iceberg are. No storage engine, no MVCC, no WAL of our own. |
| A CDC pipeline | Archival is the ingestion path. Logical decoding would spare the heap on a busy primary, but replicating rows out of Postgres does not *remove* them, and the purge is where the cost is. |
| An ingest transport | A transport parses a wire format, authenticates a producer and decides what a valid reading is. A storage layer does none of those. |
| A domain library | Validation, substitute values, gas conversion, aggregation and the DST calendar belong to [`metering`](https://crates.io/crates/metering). A second implementation here would drift. |
| Distributed query execution | Single-process. |
| Sub-second lake visibility | Metering arrives in batches; hours is correct. |
