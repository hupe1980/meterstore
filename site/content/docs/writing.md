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

### The boundary can move under a write

Routing is decided against a boundary read *before* the write, and archival
advances that boundary in Iceberg — a different system, sharing no transaction
with the insert. Detaching a partition before archiving it closes most of the
gap, since an insert into one being archived fails outright. What is left is the
moment after the drop, when the partition is recreated and the insert lands in a
range the watermark has since claimed for the cold tier. The row is not lost; it
is **invisible**, which is worse.

So `append` reads the boundary **again** after writing and routes a second time
if it moved. Both writes are idempotent — the hot tier's primary key, the cold
tier's reconciliation — so the second pass restores rather than duplicates, and
the outcome carries both rounds.

### Why reusing a boundary is safe in `hot_writer`

The writer **refuses** any interval below the boundary it was opened at, rather
than routing it. That is the whole safety argument: routing on a stale boundary
could place a row below the true watermark; refusing cannot. A refusal names the
interval and points at `append`.

A snapshot can only be stale in one direction, because the watermark is
monotonic. The margin against that is the **settlement lag** — archival never
closes a window newer than `now - settlement_lag`, a week by default, while
current data has `from` near now. A writer held for the length of an ingest run is
nowhere near that margin; one held for days is, so reopen per run rather than
caching one for the process lifetime.

That margin is thinnest in two states: a **backfill**, whose `from` is old rather
than current, and a **catch-up**, where archival advances several windows in one
run. Use `append` for both — the second boundary read is what it is for.

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

**A row already stored at the same `(merge key, version)` is skipped, in both
tiers** — reported as `Duplicate`, never appended twice.

| | How a replay is absorbed |
|---|---|
| **Hot** | `ON CONFLICT DO NOTHING` on the primary key — the merge key plus `version`. Unlike `DO UPDATE` it writes no row version, so a replay leaves no dead tuples for autovacuum. |
| **Cold** | A read of what is already stored for those readings, under a lease, immediately before the append. **Iceberg has no constraints**, so nothing else would stop it. |

A cold duplicate is not merely untidy. Resolution ranks one row per *version*, so
two rows at one version are two winners — and resolution is
[elided entirely](@/docs/querying.md) for a historical scan whose files provably
hold a single version, which is exactly the shape a duplicated late correction
produces. The sum is overstated with nothing reporting it.

**And a version identifies one assertion.** The same
`(malo_id, obis_code, from, version)` must always carry the same value.
Redelivering an identical row is fine; restating a *different* value under an
existing version means a producer is wrong, and silently keeping either copy would
bury that. Both tiers refuse it. A corrected value needs a higher version — which
is the whole mechanism.

Rows go as arrays expanded with `unnest`, one statement per batch rather than one
per reading. At 96 values per meter per day, per-row round trips would be the
write path's entire cost.

### A delivery that states no version

Not every reading arrives with an MSCONS label — an SMGW push, a manual entry, a
CSV backfill. Resolution still has to order them, so `Version::arrival` derives
one from when the delivery was recorded:

```rust
ScopedVersion::new(scope, Version::arrival(recorded_at)?)
```

Unix **milliseconds**, and the unit is the whole of the decision. MSCONS labels
are ≥ 14 digits; a millisecond timestamp is 13 and stays 13 until November 2286,
so an arrival-derived version always sorts *below* a stated one and a late
delivery that finally carries its own version wins. Microseconds would be 16
digits — inside the band, silently outranking the network operator. Seconds would
be below it but not sub-second, so two writes in one second would collide on
`(merge key, version)`.

`is_well_formed()` stays false for these, which is what distinguishes "the
operator said so" from "we assigned one".

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

That decision comes from `Displacement::superseded`, which the write observed
inside its own transaction — a read-then-write cannot say that, which is why the
loop belongs here rather than in every caller.

Re-asserting a value already in force is a no-op, not a version bump. After
`AUTHORITATIVE_ATTEMPTS` rounds it gives up: another writer is authoring the same
reading continuously, which is a conflict for an operator rather than something
to retry through.

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

**Quality and unit travel with the value.** A substitute replaced by a measured
reading is a change even when the number is identical, and a value without a unit
is dimensionless.

`displacements` covers both tiers. The hot tier takes the prior state and the
insert in one transaction; the cold tier has no transaction a reader can join, so
it holds an exclusive **cold-append lease** across the read and the write
instead — two processes appending the same late correction serialise rather than
both finding no stored row.

## Zählerstandsgänge

A **Lastgang** is energy over `[from, to)`. A **Zählerstandsgang** is a
cumulative register value at an instant — and since BK6-24-174 (in force
06.06.2025) a German MSB holds one per measuring point at the same cadence as
the Lastgang it is differenced into. The primary record is therefore *exactly as
voluminous* as the derived one, and § 146 Abs. 4 AO means it cannot be discarded
after differencing: a stored difference cannot reproduce the register values it
came from.

Left in plain PostgreSQL it is the one table nobody may delete from and nothing
tiers out of. So a table declares which shape it holds:

```rust
let config = TableConfig::new("meter_reads_versions")
    .time_model(TimeModel::Point)
    .build()?;

store.append_readings(&[zaehlerstandsgang]).await?;      // metering::MeterReading
let back = store.readings(malo)?.melo(melo)?             // ...and back
    .range(from, to).collect().await?;
```

Everything else is identical — the same attribute and subject columns, the same
version scoping, the same watermark, the same partitioning, the same routed
writes and late corrections. All of that reads the *start* timestamp, and a
reading has one.

Four things differ:

| | `Interval` | `Point` |
|---|---|---|
| `to` | the span's exclusive end | **null** — an instant has no end |
| `value` | energy *in* the span | the **register's** cumulative reading |
| overlap exclusion | on: two spans may not overlap | off: instants cannot |
| `melo_id` | labels the row | **names** it — see below |

### A register belongs to a meter, not to a market location

A Marktlokation may be measured by more than one Messlokation, and both meters
carry `1-0:1.8.0` at the same instants. Keyed on the market location alone the
second reads as a restatement of the first — and where the two agree, which two
freshly installed meters do, one is dropped.

So a point table puts `melo_id` in the merge key by default. The column is
`NOT NULL` there and a delivery naming no Messlokation is refused:

```rust
StoredReadings::new(malo, obis, Sparte::Strom, readings, source, version, recorded_at)
    .with_melo_id(melo)          // required on a point table
```

`identify_by_melo` pins it either way. See
[the storage model](@/docs/storage-model.md#the-messlokation-may-be-part-of-the-identity).

**They are never the same table.** `value` would mean two things in one column,
and summing Zählerstände gives a number with no meaning that looks exactly like a
consumption total. `append_readings` is refused on an interval table and `append`
on a point one, each naming the other. `to IS NULL` is the row-level signal, so an
external engine holding only the Parquet can tell them apart too.

A zero-width interval would make `MeterInterval` a lie — `metering` computes
`demand_kw` as energy over duration — and leaves `value` meaning two things
anyway.

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

It means reproducing the hot schema, its primary key, the canonical-OBIS `CHECK`,
the Sparte/unit/quality code lists and the intra-version overlap exclusion — a
contract carried in prose, where drift shows up as readings that silently fail to
supersede.

`StoredSeries` makes that contract compiler-checked instead. The one rule the type
cannot carry is the tier routing, which is what `append` and `hot_writer` enforce.
