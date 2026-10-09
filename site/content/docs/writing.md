+++
title = "Writing readings"
description = "Routed writes, the bulk path for steady-state ingest, idempotent redelivery, and knowing what a write displaced."
weight = 4
+++

## Two entry points, and when each is right

| | Use for | Cost |
|---|---|---|
| `store.append(&[…])` | A delivery that **might** contain a late correction, and any backfill | Reads the tier boundary before the write and again after it |
| `store.hot_writer()` | Steady-state ingest of **current** data | Reads the boundary once per run, then reuses it |

`append` routes each interval to the tier that owns it — the only safe way to
record a correction for an already-archived interval, which in PostgreSQL would
sit below the watermark where no query looks.

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

### The boundary can move under a write

Routing uses a boundary read *before* the write, and archival advances it in
Iceberg, which shares no transaction with the insert. An insert into a window
being archived finds its partition detached, writes nothing, and fails with the
retryable `BoundaryMoved`. After the drop, the PostgreSQL hot tier refuses to
recreate a partition below the table's mark in `meterstore_reclaimed` — same
error — because a recreated partition would put the row where the watermark has
claimed the range for the cold tier: not lost, but **invisible**. A hot tier that
keeps no reclaimed mark cannot refuse it.

- `append` and `append_readings` absorb it: they wait (50 ms, doubling), re-read
  the boundary and route afresh, up to five rounds, then return `BoundaryMoved`
  for the caller to retry. They also read the boundary **again** after writing and
  route a second time if it moved; both tiers' writes are idempotent, so the
  second pass restores rather than duplicates, and the outcome carries both rounds.
- A `hot_writer` re-reads its boundary on it, so a retry through the same writer
  lands the rows or refuses, by name, those now below the boundary.

### Why reusing a boundary is safe in `hot_writer`

The writer **refuses** any interval below the boundary it was opened at, rather
than routing it: routing on a stale boundary could place a row below the true
watermark; refusing cannot. A refusal names the interval and points at `append`.

The watermark is monotonic, so a snapshot can only be stale in one direction, and
the margin is the **settlement lag** (a week by default) between current data and
the newest window archival may close. Reopen the writer per ingest run rather than
caching one for days. Use `append` for a **backfill**, whose `from` is old, and
during a **catch-up**, when archival advances several windows in one run.

## This appends; it never overwrites

**A correction is a new row at a higher `version`, not an update.** The resolved
`readings` relation applies latest-version-wins over `readings_versions`, and the
prior value stays readable, which is what makes a past settlement reproducible.

## Redelivery is ordinary traffic

Transports deliver *at least* once. **A row already stored at the same `(merge key, version)` is skipped, in both
tiers** — reported as `Duplicate`, never appended twice.

| | How a replay is absorbed |
|---|---|
| **Hot** | `ON CONFLICT DO NOTHING` on the primary key — the merge key plus `version`. Unlike `DO UPDATE` it writes no row version, so a replay leaves no dead tuples for autovacuum. |
| **Cold** | A read of what is already stored for those readings, under a lease, immediately before the append. **Iceberg has no constraints**, so nothing else would stop it. |

A cold duplicate would be two winners at one version, and resolution is
[elided](@/docs/querying.md) for a scan whose files provably hold one version —
so the sum would be silently overstated.

**A version identifies one assertion.** The same
`(malo_id, obis_code, from, version)` must always carry the same value.
Redelivering an identical row is fine; restating a *different* value under an
existing version is refused in both tiers. A corrected value needs a higher
version.

Hot rows go as arrays expanded with `unnest`, one statement per batch.

### A delivery that states no version

Not every reading arrives with an MSCONS label — an SMGW push, a manual entry, a
CSV backfill. Resolution still has to order them, so `Version::arrival` derives
one from when the delivery was recorded:

```rust
ScopedVersion::new(scope, Version::arrival(recorded_at)?)
```

Unix **milliseconds**: 13 digits until November 2286, while MSCONS labels are
≥ 14, so an arrival-derived version always sorts *below* a stated one and a late
delivery carrying its own version wins. (Microseconds would outrank the network
operator; seconds would collide within a second.) `is_well_formed()` is false for
these.

## Values the operator authors

Two kinds of write, and only one of them is a *delivery*:

| | Meaning | Being outranked is |
|---|---|---|
| `append` | something a market partner sent, at the version they assigned | **correct** — a backfill after a correction is ordinary |
| `append_authoritative` | something **you** authored: a § 60 Abs. 2 MsbG Ersatzwert, a correction after a dispute, a manual entry after a meter exchange | **a silent failure** |

Through `append`, an authored value that a higher version already beats is stored
and quietly shadowed — audited, confirmed, never current. That is the ordinary
case for Ersatzwertbildung: the `FAULTY` interval being replaced arrived under a
real MSCONS version, so the version you can put on your own value is *lower* than
the one you have to beat.

```rust
let outcome = store.append_authoritative(&[ersatzwert]).await?;
// Every displacement is Inserted or Superseded. Anything else was retried.
for d in outcome.displacements {
    audit.record(d.malo_id, d.from, d.written.version);   // the version it landed at
}
```

The version each row carries is a **floor**, not an assertion. Where a higher one
already holds the reading, the store re-appends at `ScopedVersion::next` of the
one actually in force — continuing the *stored* sequence under the *stored*
scope, because a version is comparable only within its own and the hot tier
refuses a second network operator for one reading.

The decision reads `Displacement::superseded`, observed inside the write's own
transaction, which a caller's read-then-write could not do. Re-asserting a value
already in force is a no-op. After `AUTHORITATIVE_ATTEMPTS` rounds it gives up:
another writer is authoring the same reading continuously. That is refused as an
`IntegrityViolation` (`one_author_per_reading`): a conflict between producers to
settle, not something to retry and not a fault in the store.

## Knowing what a write displaced

A count cannot distinguish a new reading from a correction from a backfill that
changed nothing, and reading the prior state before writing is a **race**, wrong
exactly when two corrections arrive together. So the store reports it:

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

`Shadowed` changes nothing any query returns; a caller treating every accepted
write as a change would report a correction that never took effect.

**Quality and unit travel with the value.** A substitute replaced by a measured
reading is a change even when the number is identical, and a value without a unit
is dimensionless.

`displacements` covers both tiers. The hot tier reads the prior state and inserts
in one transaction; the cold tier holds an exclusive **cold-append lease** across
the read and the write, so two processes appending the same late correction
serialise. The loser waits up to about **14 seconds** — sized for a bulk
correction over an archived month, the largest write and the one time writers
queue — then fails with a retryable `Error::LockTimeout`, having changed nothing.

## Zählerstandsgänge

A **Lastgang** is energy over `[from, to)`. A **Zählerstandsgang** is a
cumulative register value at an instant — and since BK6-24-174 (in force
06.06.2025) a German MSB holds one per measuring point at the same cadence as
the Lastgang it is differenced into. The primary record is therefore *exactly as
voluminous* as the derived one, and § 146 Abs. 4 AO means it cannot be discarded
after differencing: a stored difference cannot reproduce the register values it
came from.

So a table declares which shape it holds:

```rust
let config = TableConfig::new("meter_reads_versions")
    .time_model(TimeModel::Point)
    .build()?;

store.append_readings(&[zaehlerstandsgang]).await?;      // StoredReadings of metering::prelude::MeterReading
let back = store.readings(malo)?.melo(melo)?             // ...and back
    .range(from, to).collect().await?;
```

Everything else — columns, version scoping, watermark, partitioning, routing,
late corrections — reads the *start* timestamp and is identical. Four things
differ:

| | `Interval` | `Point` |
|---|---|---|
| `to` | the span's exclusive end | **null** — an instant has no end |
| `value` | energy *in* the span | the **register's** cumulative reading |
| overlap exclusion | on: two spans may not overlap | off: instants cannot |
| `melo_id` | labels the row | **names** it — see below |

### A register belongs to a meter, not to a market location

A Marktlokation may be measured by more than one Messlokation, each carrying
`1-0:1.8.0` at the same instants. Keyed on the market location alone, the second
meter's reading reads as a redelivery of the first and is dropped where they agree.

So a point table puts `melo_id` in the merge key by default. The column is
`NOT NULL` there and a delivery naming no Messlokation is refused:

```rust
StoredReadings::new(malo, obis, Sparte::Strom, readings, source, version, recorded_at)
    .with_melo_id(melo)          // required on a point table
```

A table declared `identify_by_melo(false)` still compares the column. A second
meter's reading at a key already held — stored, or earlier in the same batch — is
refused as `melo_identifies_the_reading` in either tier, rather than taken for a
replay. See
[the storage model](@/docs/storage-model.md#the-messlokation-may-be-part-of-the-identity).

**They are never the same table.** `value` would mean two things in one column,
and summing Zählerstände gives a number with no meaning that looks exactly like a
consumption total. `append_readings` is refused on an interval table and `append`
on a point one, each naming the other. `to IS NULL` is the row-level signal, so an
external engine holding only the Parquet can tell them apart too. (A zero-width
interval is no substitute: `metering` computes `demand_kw` as energy over
duration.)

## Partitions appear when you need them

A partitioned table rejects a row with no partition to hold it. Both write paths
ensure the partitions for the range they are about to write, so a fresh deployment
or a backfill reaching past the pre-created headroom does not get a bare
`no partition of relation found for row`.

Creation is serialised by an advisory lock, since the first batch of a new day
has every ingest worker creating the same partition. An existing partition costs
one catalogue lookup, and `hot_writer` caches the ones it has confirmed.

## Writing through your own driver

It means reproducing the hot schema, its primary key, the canonical-OBIS `CHECK`,
the Sparte/unit/quality code lists and the intra-version overlap exclusion; drift
shows up as readings that silently fail to supersede. `StoredSeries` makes that
contract compiler-checked, and `append` and `hot_writer` enforce the one rule the
type cannot carry: tier routing.
