+++
title = "Writing readings"
description = "Routed writes, the bulk path for steady-state ingest, idempotent redelivery, and knowing what a write displaced."
weight = 4
+++

## Two entry points, and when each is right

| | Use for | Cost |
|---|---|---|
| `store.append(&[…])` | A delivery that **might** contain a late correction | Reads the tier boundary on every call — an Iceberg metadata load |
| `store.hot_writer()` | Steady-state ingest of **current** data | Reads the boundary once, then reuses it |

`append` routes each interval to the tier that owns it. That is the only safe way
to record a correction for an already-archived interval: such a row cannot go to
PostgreSQL, because it would sit below the watermark where no query looks, so the
correction would be silently ignored while appearing to have been accepted.

```rust
let outcome = store.append(&[stored_series]).await?;
outcome.hot_rows;               // landed in PostgreSQL
outcome.cold_rows;              // late corrections, appended to Iceberg
outcome.had_late_corrections(); // whether any interval was below the boundary
```

For a service landing MSCONS or iMSys batches continuously, that boundary read is
per *batch* — one catalogue load per 96 values if a batch is one meter-day.
`hot_writer` reads it once:

```rust
let writer = store.hot_writer().await?;
for batch in stream {
    writer.append(&batch).await?;
}
```

### Why reusing a boundary is safe here

The writer **refuses** any interval below the boundary it was opened at, rather
than routing it. That is the whole safety argument: routing on a stale boundary
could place a row below the true watermark, where no query looks; refusing cannot.
A refusal names the interval and points at `append`.

A snapshot can only be stale in one direction, because the watermark is
monotonic. The margin against that is the **settlement lag** — archival never
closes a window newer than `now - settlement_lag`, a week by default, while
current data has `from` near now. A writer held for the length of an ingest run is
nowhere near that margin; one held for days is, so reopen per run rather than
caching one for the process lifetime.

## This appends; it never overwrites

**A correction is a new row at a higher `version`, not an update.** That is
MSCONS's own rule, and it is why the resolved `readings` relation applies
latest-version-wins over the raw `readings_versions` one. A caller expecting
update semantics gets an append, and the prior value stays readable — which is
what makes a past settlement reproducible.

## Redelivery is ordinary traffic

Every transport worth deploying delivers *at least* once: Kafka redelivers after
a failed offset commit, a webhook retries on a timeout, an operator replays a
file. A store that errored on redelivery could not be driven by one.

Writes therefore use `ON CONFLICT DO NOTHING` on the merge key plus version.
Replay is a no-op — and unlike `DO UPDATE` it writes no row version, so it leaves
no dead tuples for autovacuum.

**But a version identifies one assertion.** The same
`(malo_id, obis_code, from, version)` must always carry the same value.
Redelivering an identical row is fine; restating a *different* value under an
existing version means a producer is wrong, and silently keeping either copy would
bury that. The append path detects divergence and reports it. A corrected value
needs a higher version — which is the whole mechanism.

Rows go as arrays expanded with `unnest`, one statement per batch rather than one
per reading. At 96 values per meter per day, per-row round trips would be the
write path's entire cost.

## Knowing what a write displaced

A count cannot distinguish a new reading from a correction from a backfill that
changed nothing. A caller building a correction audit trail needs that
distinction, and the obvious way to get it — read the prior state, then write — is
a **race**, wrong exactly when two corrections arrive together, which is when an
audit trail is worth having.

So the store reports it:

| Effect | Meaning |
|---|---|
| `Inserted` | First value for this reading. |
| `Superseded` | This became current; another stopped being. |
| `Shadowed` | Stored, but an existing **higher** version still wins. |
| `Duplicate` | Already present at this version; nothing was written. |

```rust
for d in outcome.displacements {
    if d.effect.changed_current_value() {
        audit.record(d.malo_id, d.from, d.superseded, d.written);
    }
}
```

`Shadowed` is the one a naive design loses. Backfilling an older delivery after a
newer one has arrived is legitimate, joins the audit trail, and changes nothing
any query returns — a caller treating every accepted write as a change would
report a correction that never took effect.

Two details the obvious payload lacks: **quality travels with the value**, because
a substitute replaced by a measured reading is a change even when the number is
identical; and **the unit travels too**, because a value without one is
dimensionless.

On the hot tier the prior state and the insert share one transaction, so no write
can land between them. The cold tier cannot do the same — Iceberg has no
transaction a reader can join — so a late correction is reported against the state
read immediately before the append. The asymmetry is narrow: the cold tier has a
single writer.

## Partitions appear when you need them

A partitioned table rejects a row with no partition to hold it. Both write paths
ensure the partitions for the range they are about to write, so a fresh deployment
or a backfill reaching past the pre-created headroom does not get a bare
`no partition of relation found for row`.

Creation is serialised by an advisory lock, because the first batch of a new day
has every ingest worker creating the same partition at once — the ordinary
topology. The fast path is unaffected: an existing partition costs one catalogue
lookup, and `hot_writer` caches the ones it has already confirmed.

## Writing through your own driver

You can, but consider what it means: reproducing the hot schema, its primary key,
the canonical-OBIS `CHECK`, the Sparte/unit/quality code lists and the
intra-version overlap exclusion — a contract carried in prose, where drift shows
up as readings that silently fail to supersede.

Going through `StoredSeries` makes that contract compiler-checked instead. The
one rule you cannot get from the type is the tier routing, which is exactly what
`append` and `hot_writer` exist to enforce.
