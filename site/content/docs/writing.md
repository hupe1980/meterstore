+++
title = "Writing readings"
description = "Routed writes, the bulk path for steady-state ingest, idempotent redelivery, and knowing what a write displaced."
weight = 4
+++

## Ingesting deliveries

| | Use for | Cost |
|---|---|---|
| `store.append(&[…])` | A delivery that **might** contain a late correction, and any backfill | Reads the tier boundary before the write and again after it |
| `store.hot_writer()` | Steady-state ingest of **current** data | Caches the boundary by default. Single-tier policy verifies it on each batch |

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
With the default routing policy, `hot_writer` caches it:

```rust
let writer = store.hot_writer().await?;
for batch in stream {
    writer.append(&batch).await?;
}
```

### Settlement ingestion

A single delivery can contain current and archived readings. The default routed
append commits its hot rows first. If the cold append subsequently fails, a
fresh query can already see the hot rows. For a producer that treats a delivery
as a unit, configure the table at provisioning.

```toml
[[tables]]
name = "readings_versions"
require_single_tier = true
value_scale = 7
assert_on = ["quality", "span", "unit"]
extra_columns = [{ name = "spot", assertion = true }]
```

| Producer receives | What to do |
|---|---|
| `MixedTierBatch` | Both tiers retain their previous readings. Split the delivery at the reported watermark (`from < watermark` goes cold), then submit each part as a separate delivery. |
| `LockTimeout` during archival | Retry the whole single-tier append after backoff. The hot write uses a shared transaction lock, so other hot writers can proceed together. |
| `IntegrityViolation` with `version_identifies_one_assertion` | Reconcile the source version. Send an intended correction at a higher version. |
| `Encode` for `value` | Inspect the value against the table’s fixed precision and scale. Supply an exactly representable value. |

Use `append` for settlement deliveries with producer-assigned versions.
A custom hot backend must implement guarded hot inserts and enforce the
cold-append lease to support this policy. Configure every writer of the table
with the same assertion fields. Inspect
`require_single_tier`, `assert_on`, `assertion_columns` and `value_scale` through
`system.config` to check a running deployment.

`assert_on` selects native reading terms. Quantity is always compared. An
interval's start identifies its slot, so `span` compares its stored end at
microsecond precision. Point tables accept `quality` and `unit`. The default
selection is empty. The Rust builder accepts one `ReplayField` per
`assert_on` call.

In this example, changing `spot` from null to `"0"` at the same version is a
conflict. Assertion columns store text, so agree on one decimal spelling across
producers. Publish a changed price at a higher version.

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

On a table with `require_single_tier`, the bulk writer also checks the current
boundary under the archival guard for every batch.

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

Transports deliver *at least* once. **A replay of the same assertion at the same `(merge key, version)` is skipped in both tiers** — reported as `Duplicate`, never appended twice.

| | How a replay is absorbed |
|---|---|
| **Hot** | `ON CONFLICT DO NOTHING` on the primary key — the merge key plus `version`. Unlike `DO UPDATE` it writes no row version, so a replay leaves no dead tuples for autovacuum. |
| **Cold** | A read of what is already stored for those readings, under a lease, immediately before the append. **Iceberg has no constraints**, so nothing else would stop it. |

A cold duplicate would be two winners at one version, and resolution is
[elided](@/docs/querying.md) for a scan whose files provably hold one version —
so the sum would be silently overstated.

**A version identifies one assertion.** The same
reading key and scoped version must always carry the same quantity, selected
native terms and declared assertion attributes.
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

For a restartable ingest session, persist the first receipt before assigning an
arrival version. Reuse it on every retry. Persist the original transaction time
as well when reconstructing rows. The receipt and the time the store learned a
value serve separate purposes. Two receipts in the same millisecond can produce
the same arrival version for a slot.

## Values the operator authors

Choose the write method by the promise you make to the caller.

| Method | Use it when | If another version is current |
|---|---|---|
| `append` | A market partner supplied the version. | A lower version is recorded as a backfill. |
| `correct_if_current` | The author approved a correction against a specific interval version. | A stale predecessor returns `PredecessorMismatch`. |
| `append_authoritative` | The operator requires the supplied assertion to become current. | The writer can author a successor above the current version. |

Use `append_authoritative` for an operator-authored replacement such as an
Ersatzwert for a faulty interval. Its version is a floor. If a higher version
holds, the writer retries with the successor in that version’s scope.

```rust
let outcome = store.append_authoritative(&[ersatzwert]).await?;
// A duplicate already in force is also acknowledged.
for d in outcome.displacements {
    if d.effect.changed_current_value() {
        audit.record(d.malo_id, d.from, d.written.version);
    }
}
```

A duplicate whose assertion already holds needs no further write. On sustained
contention, reconcile the confirmed work and the `one_author_per_reading`
conflict with the other author.

### Correcting a specific predecessor

Read the slot through a tenant-scoped handle and narrow the query by channel,
identity and time. `collect_deliveries()` returns the winners with their scoped
versions and delivery attributes. Check the original terms supplied by the
producer against that result. Build a one-interval correction carrying the
observed `version`, the replacement value and quality, the correction's source
and the time it was recorded.

```rust
let change = store.correct_if_current(&correction).await?;
let published_version = change.written.version;
```

The writer checks the predecessor and publishes its successor in one storage
operation. `Error::PredecessorMismatch { expected, actual }` means another
version is current, or the slot is absent (`actual == None`). Read the slot
again and reconcile the producer's intent before submitting a new correction.
A replay after a successful conditional write also sees the successor.

If archival moves the slot during a hot write (`BoundaryMoved`) or another
Iceberg commit advances the snapshot (`ConcurrentWrite`), retry with the same
approved predecessor. The next attempt checks it against the current tier.
After a lost acknowledgement, read the slot and its history to establish
whether the correction committed.

### Recovering an interrupted append

A routed append can commit hot rows before a cold write fails. An authoritative
append can commit several rounds as it raises versions. On
`Error::PartialAppend { confirmed, source }`, inspect the confirmed counts and
displacements, including shadowed insertions, and retain the underlying cause.
A final commit whose acknowledgement was lost may also be visible. Read the
affected versions before resuming an authored correction. If the process dies
before receiving a result, recovery likewise starts from the durable version
history and the producer's original receipt.

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
