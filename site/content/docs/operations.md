+++
title = "Operations"
description = "Scheduling archival, the system tables an operator opens during an incident, metrics worth alerting on, schema evolution, and the failure matrix."
weight = 8
+++

## Scheduling

```rust
let handle = store.admin().maintenance()
    .interval(Duration::minutes(15))
    .expire_snapshots(false)      // snapshot retention is a compliance decision
    .spawn();
```

Nothing runs until you call this. One cycle archives every due window (bounded,
so a store down for a month catches up over several cycles), optionally expires
snapshots, checks the invariant, and — where configured — runs the
[§ 60 Abs. 6 sweep](#the-retention-sweep), in that order. The handle **owns** the
loop: dropping it stops the loop after the cycle in flight, as `shutdown()` does.

Every replica can run the same schedule. One wins the archive lease; the others
report `lease_contended` and stop short of archival and **snapshot expiry**, but
still refresh `system.tables`, so an invariant violation is seen everywhere.
Neither contention nor `deferred` ([locks](#locks-and-why-ddl-gives-up)) is a
failure.

The lease is a session-scoped advisory lock on a connection the lease owns, and
lasts exactly as long as that PostgreSQL session. It is lost when
`idle_session_timeout` ends the session while the run is busy in Iceberg, or when
`pg_terminate_backend` or a restarted proxy cuts it, and it excludes nothing
behind a transaction-pooling PgBouncer — **connect the archiver directly or
through session pooling.** A lost lease lets a second archiver take the same
window; the commit guard refuses the duplicate commit as `AlreadyArchived`, the run
is reported `lease_contended`, and its detached partition is reclaimed later like
any archived window.

**One loop, however many tables.** A [catalogue](@/docs/querying.md#several-tables-in-one-session)
maintains all of its tables from one schedule:

```rust
let handle = catalog.maintenance().spawn();
```

Each table keeps its own watermark, archiver and lease; only the scheduling is
shared, and tables are visited one after another to avoid a load spike. The
outcome carries **one row per table**, so an alert names the table:

```rust
for table in outcome.unhealthy() {
    tracing::error!(table = %table.table, violations = table.invariant_violations);
}
```

### The retention sweep {#the-retention-sweep}

§ 60 Abs. 6 MsbG obliges the Messstellenbetreiber to erase or anonymise
personenbezogene Messwerte *"spätestens jedoch nach drei Jahren ab dem Schluss des
Kalenderjahres"* — a duty on a clock rather than an answer to a request.

```rust
let handle = catalog.maintenance()
    .anonymise_after(Retention::CalendarYears(3), "§ 60 Abs. 6 MsbG", "retention-job")
    .spawn();
```

Off by default: destroying a linkage is irreversible, so turning it on is the
compliance decision.

`CalendarYears(3)` is **not** `now - 3 years`. The clock starts at the *Schluss
des Kalenderjahres*, so a value collected on 2 January 2025 comes due on 31
December 2028; the rolling spelling would erase it a year early.
`Retention::Rolling(d)` covers the statute's other trigger — deletion as soon as
storing is no longer necessary — which can fall before the year-end ceiling.

**It reads no table.** What comes due is a `(subject, collection year)` pair
recorded on the mapping row, so the sweep is one indexed `DELETE` against the
registry, the same through any handle. See [Privacy and retention](@/docs/privacy.md).
A failed sweep appears in `outcome.failures()` as `<retention>`.

**A failing table does not end the cycle.** It becomes a row with a `failure` and
the rest are still maintained, so one quarantined schema or unreachable catalogue
does not freeze archival everywhere. `healthy()` is false while any table failed.

**The clock is a parameter.** Every archival, maintenance and status call takes
`now`; `Maintenance::spawn` and the CLI read the process clock once per cycle.
Routing reads no clock — it routes on `from` — so wall time decides only *when* a
window becomes eligible, and one thing more: a reader pin's expiry is stamped from
the **process** clock and expired against the **database's** `now()`, so a process
running behind the database shortens every pin by the skew.

## Locks, and why DDL gives up {#locks-and-why-ddl-gives-up}

PostgreSQL grants locks **in arrival order**, so a statement waiting for
`ACCESS EXCLUSIVE` blocks every reader and writer behind it. Behind one long query
or one session idle in a transaction, that is a total ingest outage. MeterStore
issues DDL on the **write path** (partition creation) and in the **archival loop**
(detach and drop), and guards each differently.

### Creation avoids the lock

MeterStore does not use `CREATE TABLE … PARTITION OF`, which takes
`ACCESS EXCLUSIVE` on the parent. It builds the relation standalone with its
integrity constraints (so the `GiST` index is built where nobody can contend), adds
a bound `CHECK` that lets the attach skip the validation scan, attaches it —
`SHARE UPDATE EXCLUSIVE`, conflicting with no read and no write — and drops the
redundant `CHECK`.

### Detach cannot, so it declines to wait

Detaching a partition needs the strong lock, so every DDL statement runs under a
`lock_timeout`:

```toml
[hot]
ddl_lock_timeout = "3s"    # 0s disables it — PostgreSQL's default, and the advice against
```

A detach that cannot get its lock gives up **having changed nothing**; the cycle
reports `deferred` and the next one tries again. Every second added to the timeout
is a second the whole table can stall for.

No drop follows the cold commit: the partition is always left detached and
reclaimed on a later cycle ([the reader grace](@/docs/architecture.md#the-reader-grace)).
A reclamation that cannot get its lock skips that partition; the cost is disk.

**Alert on watermark lag, not on deferral** (`outcome.tables[..].deferred()`). A
table deferring every cycle for an hour has something holding a conflicting lock —
look in `pg_stat_activity`, usually a session idle in a transaction. The lag gauge
is read from the cold tier, so while that is unreachable it holds its last value:
alert on `meterstore.archival.failures` beside it.

## Errors, and which to retry

`Error::is_retryable()` is the split: a lost connection and an unavailable lock
are worth retrying; a refused delivery, an invalid configuration and a statement
that will not plan are not.

| Error | Retryable | What it means |
|---|---|---|
| `LockTimeout` | yes | Nothing was changed. Something else holds a conflicting lock |
| `Storage` | yes | The backend failed — a connection, a timeout, a full disk |
| `BoundaryMoved` | yes | A write or a scan met the tiering boundary mid-move. A fresh plan, or the same delivery again, sees where it now is |
| `AlreadyArchived` | no | Another archiver committed this window first. The archiver reports it as a contended run, not a failure |
| `DataFusion` | *depends* | The engine's own errors: a statement that will not plan is not retried, a warehouse that could not be reached is |
| `IntegrityViolation` | no | The store refused a delivery. The delivery has to change |
| `InvariantViolated` | no | The store's own state is wrong. **An operator has to look** |
| `Quarantined` | no | An incompatible schema change. An operator has to resolve it |

`IntegrityViolation` means the store **stopped** something from becoming true — an
overlapping delivery, two network operators for one reading, a value restated
under an existing version; the producer acts. `InvariantViolated` means something
already is true that should not be; an operator acts. Do not page on the first.

An error this crate raises during a query keeps its variant through the engine,
so a `match` sees `BoundaryMoved` or `InvariantViolated`, not an opaque engine
error. Flight SQL carries the same split in the status
code: `UNAVAILABLE` for anything retryable, `INTERNAL` for `InvariantViolated`,
`DATA_LOSS` for a row that will not decode, `FAILED_PRECONDITION` for a
quarantined table and `INVALID_ARGUMENT` for a statement the client got wrong.

## System tables

```sql
SELECT "table", watermark, watermark_lag_seconds,
       hot_partitions, partitions_ahead, invariant_violations, healthy
FROM system.tables;
```

`healthy` covers the two ways a table stops working, and they fail differently:

- **`invariant_violations` — wrong answers now.** Rows below the watermark still
  in PostgreSQL, so a query can return the wrong number. **This is the alert.**
- **`partitions_ahead` — no answers shortly.** Existing partitions that can still
  hold a row written now or later; at zero the next insert fails. A table with
  **no partitions at all** is exempt (a new table has no frontier to run out of).
  A store that cannot enumerate its partitions reports `-1` and is *not* healthy.

Three more relations answer questions that otherwise need Iceberg metadata by
hand:

```sql
SELECT * FROM system.config;                                       -- settings that interact
SELECT value FROM system.resolution WHERE setting = 'resolution_sql';
SELECT value FROM system.resolution WHERE setting = 'balancing_day_column';
SELECT snapshot_id, committed_at, watermark FROM system.snapshots;
```

> **The `system` schema is not a stored table.** These are in-memory relations in
> the MeterStore process, computed by `store.admin().refresh_system_tables(now)`
> and refreshed only by calling it again, so an ordinary query never pays for a
> round trip to both tiers. An external engine pointed at the warehouse cannot see
> them.

`system.resolution` carries what an engine reading the Iceberg files directly
needs: version resolution as SQL to paste, and the balancing day as the *name of a
column*, since the Gastag has no portable SQL
([external engines](@/docs/interop.md#the-gas-day-trap)). `system.config` shows
the settings that *interact* side by side — a `settlement_lag` shorter than an
`archival_step` is valid alone and wrong beside it.

## Metrics

Instruments use the **OpenTelemetry API, not an SDK**: until your application
installs a meter provider, recording is a no-op. Every instrument carries a
`table` attribute and scan metrics add `tier`, except the two registry counters,
`subjects_erased` and `registrations_suppressed`, which are deployment-wide.

| Instrument | Why it exists |
|---|---|
| `meterstore.tiering.invariant_violations` | **The alert.** Non-zero means query results may be wrong. Everything else is degradation. |
| `meterstore.tiering.watermark_lag` | Archival falling behind. Read from the cold tier, so while that is unreachable the gauge holds its last value and `archival.failures` is the alert. |
| `meterstore.tiering.hot_partitions_ahead` | Reaching 0 stops writes outright. |
| `meterstore.archival.rows` / `.duration` / `.failures` | Throughput, and whether a window still fits its schedule. `.failures` is **the alert while the cold tier is unreachable**. |
| `meterstore.archival.deferred` | Cycles that stopped because a lock was not available. **Not a failure** — nothing changed and the next cycle retries. Explains lag; do not alert on it. |
| `meterstore.archival.windows` / `meterstore.partitions.reclaimed` | Not fault signals. Windows committed to the cold tier, and archived partitions actually dropped (past `reader_grace`, pinned by no reader). The second trailing the first by about `reader_grace` is normal; a widening gap is a pin or a lock holding the space. |
| `meterstore.partitions.reclaim_held` | Reclamations a live query's pin deferred. **Not a failure**; explains a widening gap above. |
| `meterstore.tiering.boundary_moved` | Plans, scans and writes that met the boundary mid-move, by `where` (`plan`, `scan`, `write`). Only `write` is retried by the crate; a `plan` or `scan` refusal needs the caller's retry (Flight `UNAVAILABLE`, CLI exit 75). Archival makes a trickle; a rate that does not fall is a query outliving `max_pin_age` or a writer holding a stale boundary. |
| `meterstore.write.rows` / `.rows_deduplicated` / `.late_corrections` | Ingest volume, the redelivery rate (expected to be non-zero), and corrections arriving after their interval was archived. |
| `meterstore.query.rows_scanned` | Rows read, by `tier` — recorded by the hot scan only, so it carries `tier=hot` and no cold figure. |
| `meterstore.query.plan_duration` / `.scan_duration` | Planning (a slow catalogue) and scanning (a slow query), kept apart. |
| `meterstore.subjects_erased` | **Compliance rather than health** — the only irreversible operation. By `trigger`: `retention` flat over a year is a sweep that is not running; `request` flat is ordinary. |
| `meterstore.registrations_suppressed` | Registrations refused because the identifier is on the suppression list. **Zero is expected**: each is a pipeline replaying data from before an Article 17 erasure, to be stopped upstream. |
| `meterstore.query.merge_elided` / `.merge_elision_decisions` | How often a scan of the resolved relation skipped version resolution. Moves with the **query mix** (hot-tier ranges and ceiling reads always resolve; a range reaching no tier counts as elided). A fall with no corrections behind it is files compacted out of band — see [compaction](#compaction). |

## Schema evolution

**MeterStore applies one kind of change and refuses the rest.** It compares the
schema it is configured to write against the one the cold table has — at `build`
and before every archival run — and `create_tables` (`meterstore create`,
`Deployment::store`) adds a **nullable** column the configuration declares to
both tiers: PostgreSQL gains it on the parent and on every detached partition,
Iceberg gains it as an optional field with a fresh id. Existing rows read null
for it.

| Change | What happens | Rewrite? |
|---|---|---|
| Add a **nullable** column | Applied by `create_tables`. Until then a build refuses, archival refuses and a write naming it refuses — each with a configuration error naming `create_tables`, never a retryable storage error | No |
| Stop declaring a **nullable** column | Both tiers keep it. Archival carries the values rows already hold; rows written from then on leave it null | No |
| Make a column **nullable**, or **NOT NULL** | **Quarantine** — the Iceberg field stays required and this crate cannot relax it; make the column optional out of band in both tiers | — |
| Add a **NOT NULL** column, drop a required one, retype a column | **Quarantine** | — |
| **Reorder** columns | Nothing — every path matches columns by name | No |
| **Rename** a column | Reads as a drop plus an add, and is judged as both | — |

A snapshot pinned before a column was added still reads after it: `as_of` reads
it through the current schema, and the column it predates is null.

MeterStore compares by **name**, not Iceberg field id, because a name is what the
encoder writes and an external engine queries. So a rename is a drop plus an add:
it passes for a nullable attribute column and halts the table for a non-nullable
one — every identity column, whose rename changes the merge key.

**Quarantine** halts the table and freezes its watermark until an operator
resolves it; the rows stay in PostgreSQL, still correctable, rather than being
archived into a layout nobody agreed on.

- **A decimal's precision may widen; its scale may not**, which would reinterpret
  every stored integer by a power of ten.
- **Both directions of a merge-key change quarantine.** Declaring an identity
  column is a NOT NULL addition; undeclaring one drops a required column and would
  let readings the wider key kept apart supersede each other.
- **A cold store that cannot report its schema is not treated as compatible.**
- The comparison runs before every archival run as well as at `build`, because the
  cold table can change out of band.

## Failure matrix

| Failure | Behaviour | Recovery |
|---|---|---|
| Crash mid-archival, pre-commit | Partition detached, not archived; invisible to writers, still **readable** by queries | **Automatic.** The next run archives it again — the partition already holds every row and the watermark never moved, so it is the state archival wants. `detach_partition` is idempotent for this reason |
| Crash post-commit | Detached partition — data intact, and exactly the state a *successful* run leaves | A later cycle reclaims it, once past the reader grace and unheld by any live plan |
| Hot partitions exhausted | **Inserts fail** | Alert on `partitions_ahead`; pre-creation is automatic but monitored |
| Object store unavailable | Archival backpressures; cold queries fail loudly | Automatic |
| Postgres unavailable | Writes and archival fail, and the next cycle retries; a query whose range reaches the hot window fails loudly; a query entirely below the watermark is unaffected — except under the `sql` catalogue, whose Iceberg metadata lives in the PostgreSQL its `uri` names, usually the same server: if that one is down, every load of the table fails and cold-only queries fail with it | Automatic once the database is back |
| Iceberg commit conflict | The commit lands only on the snapshot its checks ran against; a table that moved is reloaded and the commit re-derived, up to four attempts. A commit that already landed is recognised by its `meterstore.commit_id` and reported as landed | Automatic |
| Cold commit with no answer — a timeout, a reset connection, a 5xx | The table's history is searched for the commit's `meterstore.commit_id`: found, it is reported as landed; not found, its data files are left in place, because deleting one a landed snapshot references destroys settled history | Out-of-band orphan removal reclaims the space; see [compaction](#compaction) |
| A query outliving `max_pin_age` | Its reader pin expires, reclamation may drop a window it still reads, and the query fails with a retryable `BoundaryMoved` naming that window | Retry — a fresh plan reads the window in the cold tier; raise `max_pin_age` above the longest query |
| Archival drops a window while a query is being planned | The plan re-reads the reclaimed mark after pinning and fails with a retryable `BoundaryMoved` (Flight `UNAVAILABLE`, CLI exit 75) | Retry |
| `archival_step` changed on a table with archived windows | Archival refuses every run with a configuration error | Create a new table at the new step |
| A write into a window archival is detaching | PostgreSQL finds no partition for the row, raised as retryable `BoundaryMoved`; `append` waits — 50 ms, doubling — reads the boundary again and routes the row to the cold tier, up to five rounds, then returns the retryable error; a `hot_writer` re-reads its boundary so the caller's retry lands or is refused by name | Automatic for `append`; retry for `hot_writer` |
| Second archiver commits a window already committed | Its commit is refused as `AlreadyArchived` and reported as a contended run | Automatic |
| A suppression key dropped from the key ring | Suppressions recorded under it stop matching, so an erased subject could be registered again; reported by `orphaned_suppressions` and a startup warning, never refused | Put the key back in the ring, or stop the replay upstream — see [the privacy page](@/docs/privacy.md) |
| A stored row that no longer decodes | `Error::Decode` on read — see [the stored-data break](@/docs/storage-model.md#json-columns) | Read it with a build that decodes it, or as text from any engine |
| Second archiver on the same table | Lease refused; the run is a reported no-op | Automatic; alert on lag, not on contention |
| A DDL lock held by a long query | Statement gives up; the run is a reported no-op (`deferred`) | Automatic; alert on lag, not on deferral |
| Incompatible schema change | Table quarantined; rows stay correctable in PostgreSQL | Operator resolves |
| A declared column not yet applied | Build, archival and writes refuse with a configuration error naming `create_tables` | Run `create_tables` (`meterstore create`) |
| As-of read against an expired snapshot | Fails naming the snapshot | Pick another from `system.snapshots` |
| Clock skew | Routing is unaffected — it is on `from`, a data value — but when a window becomes eligible moves by the skew, and a process clock behind the database's shortens every reader pin by the skew | Keep the process and database clocks synchronised |
| DST transition | Handled by `metering` | By design |

**Degrade, don't lie**: when Iceberg is unreachable, cold queries fail rather than
returning only the hot window.

## Compaction and orphan files {#compaction}

MeterStore performs neither. Orphan removal needs an object-store listing, which
`iceberg-rust`'s `FileIO` lacks; compaction it refuses. Any Iceberg maintenance
tool can do this work on these standard tables, but a run keeps four rules, in
this order:

```text
  reassert-watermark   ← before anything expires
  → expire snapshots   ← at MeterStore's retention, never the tool's default
  → rewrite manifests  ← the metadata win, without touching data files
  → remove orphans     ← last, so it sees what the steps above left behind
  ✗ compact data files ← not on the readings table
```

The first two are correctness — [Snapshot expiry](#snapshot-expiry) says why —
and the last is the one that surprises.

**Do not compact the readings table's data files.** Archival writes one file per
window, each a single version over a disjoint range — the layout that lets a
historical scan skip version resolution. Collapse a month's thirty files into one
and its statistics read `version min: 1, max: 30`, so every query touching that
month pays a window function, a sort and a repartition from then on.

A compactor decides "small" as a fraction of `write.target-file-size-bytes`, so
**declare yours**:

```toml
[tables.archival]
declared_file_size = 41943040   # what this table's files actually come out at
```

Left out, the tool assumes Iceberg's 512 MiB default, reads a 40 MB daily file as
8 % of target, and rewrites the warehouse. The size follows from the window and
the portfolio (roughly 40 MB a day at 100 k measuring points); an archival run
reports the bytes per file it wrote while nothing is declared.

MeterStore removes its own orphan files only when the catalogue **refused** the
commit. When the answer is unknown — a timeout, a reset connection, a 5xx — the
files stay, since deleting one a landed snapshot references destroys settled
history. Out-of-band orphan removal reclaims that space.

## Snapshot expiry

Opt-in, with ten years retention by default: a snapshot is what makes a past
settlement reproducible. It reclaims **metadata, not readings** — the table is
append-only, so expiry bounds the metadata JSON parsed on **every** table load.
Unreferenced manifest lists and old metadata files stay on object storage until
out-of-band orphan removal.

### The retention window is the deployment's, and the table says so

`snapshot_retention` and `min_snapshots_to_keep` alone decide what MeterStore's
expiry removes: it pins `iceberg`'s own age cutoff (`max-snapshot-age-ms`,
**five days** by default) to the epoch on the action, so only the ids computed
against the configured retention expire. The table also **publishes**
`history.expire.max-snapshot-age-ms` and `history.expire.min-snapshots-to-keep` at
the configured figures, so a foreign expiry, which honours them, applies the same
policy rather than the five-day default.

### Re-stamp before a foreign tool expires snapshots

A foreign commit carries no watermark, so the boundary lookup walks back the
parent chain — and removing **any** ancestor on that walk strands it, failing
every query on an otherwise healthy table. `expire_snapshots` re-stamps the
boundary onto the current snapshot first; a foreign tool does not. So re-stamp at
the head of any out-of-band maintenance run, **before** anything expires
(afterwards there is nothing left to read). It cannot move the boundary and is a
no-op when the current snapshot carries one:

```bash
# Before a maintenance tool, or a catalogue that maintains the table for
# you, expires snapshots:
meterstore reassert-watermark
```

```rust
store.admin().reassert_watermark().await?;
```

Both are safe to run unconditionally and repeatedly.

## Removing data

`store.admin().purge_table(name)` destroys the PostgreSQL table with every partition
(detached ones included), the table's rows in the shared `meterstore_reclaimed`
and `meterstore_pins` bookkeeping, the Iceberg catalogue entry, and the data
files in object storage — the bookkeeping too, so a recreated table of the same
name does not inherit a reclaimed mark. It is the **only** operation that deletes
stored readings, and the name must be repeated because there is no recovery path.

There is no way to expire *part* of a table's history. For the obligation that
usually prompts the question, see [Privacy](@/docs/privacy.md): the statute asks
for the personal link to go, not the rows.

Dropping a hot partition destroys nothing: it runs only after those rows are
durable in Iceberg, and a partition the watermark does not cover raises an
invariant violation rather than being dropped.
