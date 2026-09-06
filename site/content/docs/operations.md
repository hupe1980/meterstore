+++
title = "Operations"
description = "Scheduling archival, the system tables an operator opens during an incident, metrics worth alerting on, schema evolution, and the failure matrix."
weight = 8
+++

## Scheduling

```rust
let handle = store.maintenance()
    .interval(Duration::minutes(15))
    .expire_snapshots(false)      // snapshot retention is a compliance decision
    .spawn();
```

Nothing runs until you call this. A store that started a background loop on
construction would surprise a process that only wanted to read.

One cycle archives every due window (bounded, so a store that has been down for a
month catches up over several cycles rather than holding one process for hours),
optionally expires snapshots, checks the invariant, and — where a retention policy
is configured — runs the [§ 60 Abs. 6 sweep](#the-retention-sweep). In that order,
so each step sees the state the cycle produced rather than the one it started
from.

The handle **owns** the loop: dropping it stops the loop after the cycle in
flight, exactly as `shutdown()` does, so a handle that goes out of scope cannot
leave a task archiving against a store nobody is watching.

Every replica can run the same schedule. One wins the archive lease; the others
report `lease_contended` and stop — including stopping short of **snapshot
expiry**, which is a metadata mutation and belongs to the winner. Reading
`system.tables` still happens everywhere, because it mutates nothing and an
invariant violation is worth noticing from wherever it is seen. Contention is not
a failure. Nor is `deferred`, the other benign no-op — see
[locks](#locks-and-why-ddl-gives-up).

**One loop, however many tables.** A [catalogue](@/docs/querying.md#several-tables-in-one-session)
maintains all of its tables from one schedule:

```rust
let handle = catalog.maintenance().spawn();
```

Each table keeps its own watermark, archiver and lease; only the *scheduling* is
shared. Tables are visited one after another, because twenty archivals at once
turns a background job into a load spike on the database it exists to relieve.

The outcome carries **one row per table**, so an alert names the table rather than
reporting a number nobody can act on:

```rust
for table in outcome.unhealthy() {
    tracing::error!(table = %table.table, violations = table.invariant_violations);
}
```

### The retention sweep {#the-retention-sweep}

The third job, and the only one that is a *duty on a clock* rather than an answer
to a request. § 60 Abs. 6 MsbG obliges the Messstellenbetreiber to erase or
anonymise personenbezogene Messwerte *"spätestens jedoch nach drei Jahren ab dem
Schluss des Kalenderjahres"*. Nobody asks; it comes due anyway.

```rust
let handle = catalog.maintenance()
    .anonymise_after(Retention::CalendarYears(3), "§ 60 Abs. 6 MsbG", "retention-job")
    .spawn();
```

Off by default, for the reason snapshot expiry is: destroying a linkage is
irreversible, so turning it on is the compliance decision.

`CalendarYears(3)` is **not** `now - 3 years`. The clock starts at the *Schluss
des Kalenderjahres*, so a value collected on 2 January 2025 comes due on 31
December 2028; the rolling spelling would erase it a year early.
`Retention::Rolling(d)` covers the earlier "no longer necessary" trigger.

**It reads no table.** What comes due is a `(subject, collection year)` pair, and
the year is recorded on the mapping row — so the sweep is one indexed `DELETE`
against the registry: no scan, no dependence on which rows a session can see, and
the same answer through any handle. See
[Privacy and retention](@/docs/privacy.md). A failed sweep appears in
`outcome.failures()` under the name `<retention>`, so an alert needs no second
place to look.

**A failing table does not end the cycle.** It becomes a row with a `failure` and
the rest are still maintained. What fails here persists until an operator acts — a
quarantined schema, an unreachable catalogue — so stopping would let one such
table freeze archival for every other, whose hot tier then grows without bound for
the length of the incident. `healthy()` is false while any table failed.

**The clock is a parameter, never read.** Every archival, maintenance and status
call takes `now`, so a test drives months in milliseconds and nothing depends on
when it ran. Tiering itself never reads a clock at all — it routes on `from`, a
data value — so the only thing wall time decides is *when* a window becomes
eligible.

## Locks, and why DDL gives up {#locks-and-why-ddl-gives-up}

PostgreSQL grants locks **in arrival order**. A statement waiting for an
`ACCESS EXCLUSIVE` lock therefore blocks every reader and writer that arrives
behind it, whether or not those would have conflicted with each other. One
long-running query on the hot table, or one session left idle in a transaction,
turns a background job into a total ingest outage that lasts as long as the query
does.

MeterStore's hot tier issues DDL on two schedules an operator does not choose:
partition creation on the **write path**, and partition detach and drop in the
**archival loop**. So both are made unable to do that, in two different ways.

### Creation avoids the lock

`CREATE TABLE … PARTITION OF` takes `ACCESS EXCLUSIVE` on the parent. MeterStore
does not use it. It builds the relation standalone, adds the bound `CHECK` that
lets the attach prove the partition constraint from the catalogue instead of
scanning, attaches it — `SHARE UPDATE EXCLUSIVE` on the parent from PostgreSQL 12,
which conflicts with no read and no write — and drops the now-redundant `CHECK`
again so no row is ever checked against it twice.

The integrity constraints go on **before** the attach, while the relation is still
invisible to every other session. A `GiST` index built on a table nobody can see
cannot contend with anything.

### Detach cannot, so it declines to wait

Removing a partition from a table's inheritance genuinely needs the strong lock.
Every DDL statement therefore runs under a `lock_timeout`:

```toml
[hot]
ddl_lock_timeout = "3s"    # 0s disables it — PostgreSQL's default, and the advice against
```

A detach that cannot get its lock promptly gives up **having changed nothing**.
The cycle reports `deferred` and the next one tries again. Raise the timeout on a
deployment that reports out of the same tables it writes; every second added is a
second the whole table can stall for.

No drop follows the cold commit at all, so there is no second place a timeout can
change state: the partition is *always* left detached and reclaimed on a later
cycle — see [the reader grace](@/docs/architecture.md#the-reader-grace). A
reclamation that cannot get its lock skips that partition and leaves the rest of
the cycle running, because the only cost of keeping it one cycle longer is disk.

**Alert on watermark lag, not on deferral** (`outcome.tables[..].deferred()`). A
cycle deferring once means a long query was in flight, which is ordinary. A table
deferring every cycle for an hour means something holds a conflicting lock
permanently — look for it in `pg_stat_activity`, usually a session idle in a
transaction.

## Errors, and which to retry

`Error::is_retryable()` is the split: a lost connection and an unavailable lock
are worth retrying; a refused delivery, an invalid configuration and a statement
that will not plan are not, and retrying them loops on a message that will never
change.

| Error | Retryable | What it means |
|---|---|---|
| `LockTimeout` | yes | Nothing was changed. Something else holds a conflicting lock |
| `Storage` | yes | The backend failed — a connection, a timeout, a full disk |
| `DataFusion` | *depends* | Both the planner and the object-store reader: a statement that will not plan is not retried, a warehouse that could not be reached is |
| `IntegrityViolation` | no | The store refused a delivery. The delivery has to change |
| `InvariantViolated` | no | The store's own state is wrong. **An operator has to look** |
| `Quarantined` | no | An incompatible schema change. An operator has to resolve it |

The last two are the pair worth keeping straight, and the difference is who is at
fault. `IntegrityViolation` means the store **stopped** something from becoming
true — an overlapping delivery, two network operators for one reading, a value
restated under an existing version. `InvariantViolated` means something already is
true that should not be. Whoever is paged for the second must not be woken by a
producer sending a bad row.

## System tables

```sql
SELECT "table", watermark, watermark_lag_seconds,
       hot_partitions, partitions_ahead, invariant_violations, healthy
FROM system.tables;
```

`healthy` covers the two ways a table stops working, and they fail differently:

- **`invariant_violations` — wrong answers now.** Rows below the watermark still
  in PostgreSQL. The tier split assumes a row's interval start decides where it
  lives, so anything else means a query can return the wrong number. **This is
  the alert.**
- **`partitions_ahead` — no answers shortly.** Partitions that can still hold a
  row written now or later. Reaching zero makes the next insert fail outright.
  Counted from the partitions that exist, not derived from
  `settlement_lag + headroom`: that figure is a constant, so it reports the runway
  a healthy deployment *would* have and cannot warn you about the one that
  stopped.

  A table with **no partitions at all** is exempt, because "not started" and
  "exhausted" show the same two numbers and mean opposite things. A table created
  a moment ago has no frontier to run out of, and reporting it degraded made the
  first status of every new deployment an alarm — which is how an alert stops
  being read. A store that cannot enumerate its partitions reports `-1` and is
  *not* called healthy: "cannot say" must not read as "fine".

Three more relations answer questions that otherwise need Iceberg metadata by
hand:

```sql
SELECT * FROM system.config;                                       -- settings that interact
SELECT value FROM system.resolution WHERE setting = 'resolution_sql';
SELECT value FROM system.resolution WHERE setting = 'balancing_day_column';
SELECT snapshot_id, committed_at, watermark FROM system.snapshots;
```

> **The `system` schema is not a stored table.** These are in-memory relations
> registered into the DataFusion session, computed when `refresh_system_tables`
> is called and refreshed only by calling it again — deliberately, so a query
> never silently pays for a round trip to both tiers. They live in the MeterStore
> process: nothing in PostgreSQL or in the Iceberg warehouse corresponds to them,
> and an external engine pointed at the warehouse cannot see them.

`system.resolution` carries the two rules an engine reading the Iceberg files
directly needs and cannot derive: version resolution, without which a corrected
interval is counted twice, and the balancing day, without which a gas Lastgang is
grouped six hours out of phase. The first is delivered as SQL to paste; the second
as the *name of a column*, because the Gastag has no portable SQL — so the rule is
applied at write time and every engine just groups on the answer. Both are
[explained in full](@/docs/interop.md#the-gas-day-trap).

`system.config` exists because the settings that matter *interact*: a
`settlement_lag` shorter than an `archival_step` is valid alone and wrong beside
it — a window closes while corrections for it are still arriving, and they land
below the watermark where no query looks. Seeing the two side by side is how that
gets noticed.

All four are snapshots computed when asked. `store.refresh_system_tables(now)`
recomputes them — deliberately explicit, so an ordinary query never silently pays
for a round trip to both tiers.

## Metrics

Instruments are created against the **OpenTelemetry API, not an SDK**. Until your
application installs a meter provider, recording is a no-op. That is the right
contract for a library: MeterStore decides what is worth measuring, the
application decides where measurements go.

Every instrument carries a `table` attribute; scan metrics add `tier`. The
retention counter carries neither — a subject's linkage is destroyed across the
whole deployment at once, so attributing it to a table would invite a sum that
double-counts it.

| Instrument | Why it exists |
|---|---|
| `meterstore.tiering.invariant_violations` | **The alert.** Non-zero means query results may be wrong. Everything else is degradation. |
| `meterstore.tiering.watermark_lag` | Archival falling behind. Trends toward failure before causing one. |
| `meterstore.tiering.hot_partitions_ahead` | Reaching 0 stops writes outright. |
| `meterstore.archival.rows` / `.duration` / `.failures` | Throughput, and whether a window still fits its schedule. |
| `meterstore.archival.deferred` | Cycles that stopped because a lock was not available. **Not a failure** — nothing was changed and the next cycle retries. Alert on watermark lag; read this to explain it. |
| `meterstore.partitions.dropped` / `.orphans_reclaimed` | Purge keeping up; non-zero orphans mean runs are being interrupted. |
| `meterstore.write.rows` / `.rows_deduplicated` / `.late_corrections` | Ingest volume, the redelivery rate (expected to be non-zero), and corrections arriving after their interval was archived. |
| `meterstore.query.plan_duration` / `.scan_duration` | **Two instruments, not one.** A single `query.duration` recorded at plan time measured only the time to build a plan — making a slow catalogue look like a slow query and hiding a slow scan behind fast planning. |
| `meterstore.subjects_erased` | **Compliance rather than health**, and the only irreversible operation this crate performs — the one counter whose *rise* is worth a look and whose flat zero is too. Split by a `trigger` attribute, because the readings are opposite: `trigger="retention"` flat over a year is a sweep that is not running (a spike is a cohort reaching the ceiling together); `trigger="request"` flat is ordinary. Summed, a stopped sweep would hide behind the occasional request. No `table` attribute — the registry is deployment-wide. |
| `meterstore.registrations_suppressed` | Registrations refused because the identifier is on the suppression list. **Zero is the expected reading**: each one is a pipeline replaying data from before an Article 17 erasure — refused here, and still to be fixed upstream. |
| `meterstore.query.merge_elided` / `.merge_elision_decisions` | The elided ratio — how often a historical scan skipped version resolution. It falls as corrections accumulate in the ranges being queried, which is the data changing rather than the layout degrading; see [compaction](#compaction). |

## Schema evolution

Metering schemas are regulator-defined and move on multi-year cycles, so this is
far smaller than in a general framework — but not zero.

**MeterStore detects schema drift; it does not perform schema evolution.** There
is no `ALTER`-equivalent in the crate: it compares the schema it is configured to
write against the one the cold table actually has, before every archival run, and
either proceeds or halts. Applying a change is an operator's job, out of band,
with the same Iceberg tooling that does [compaction](#compaction) — and this
table says which changes it will then accept.

| Change | Accepted? | Rewrite? |
|---|---|---|
| Add a **nullable** column | Yes | No |
| Widen decimal **precision** | Yes — Iceberg type promotion | No |
| Drop a **nullable** column | Yes | No |
| Add a **NOT NULL** column, drop a required one, narrow a type, change a decimal's **scale** | **Quarantine** | — |
| **Rename** a column | Reads as a drop plus an add, and is judged as both | — |

That last row is worth reading twice. Iceberg resolves columns by **field id**,
so a rename applied through Iceberg's own `UpdateSchema` costs nothing and
historical files keep reading. MeterStore compares by **name**, because a name is
what the encoder writes and what an external engine queries — so a rename is
invisible to it and arrives as a drop plus an add. For a nullable attribute
column both halves are safe and it passes; for a **non-nullable** one — every
identity column is one — both halves are unsafe and the table halts, which is
right rather than incidental, because renaming an identity column changes the
merge key.

Quarantine is the honest response to a change that cannot be applied safely. The
table halts, its watermark freezes, an operator resolves it. Freezing is the
point rather than a side effect: the rows stay in PostgreSQL, where they can still
be corrected, instead of being archived into a layout nobody has agreed on.
Silent corruption is never traded for uptime.

Three details:

- **A decimal's precision may widen; its scale may not.** Precision adds
  representable digits; changing scale reinterprets every stored integer by a
  factor of ten. In a settlement figure that is a silent order-of-magnitude error.
- **Both directions of a merge-key change quarantine.** An identity column is
  non-nullable, so *declaring* one arrives as a NOT NULL addition and
  *undeclaring* one as a dropped required column. The second is the more
  dangerous: the key narrows, two readings the wider key kept apart start
  competing in resolution, and one supersedes the other with no error anywhere.
- **A cold store that cannot report its schema is not treated as compatible.** The
  check simply did not run, and saying so beats implying it passed.

The comparison runs at `build` **and** before every archival run. The second is
not redundant: the cold table can change out of band — an operator running
compaction with Spark, or a second deployment on an older configuration.

## Failure matrix

| Failure | Behaviour | Recovery |
|---|---|---|
| Crash mid-archival, pre-commit | Partition detached, not archived; invisible to writers, still **readable** by queries | Next run refuses to drop it and names it for an operator to re-attach |
| Crash post-commit | Detached partition — data intact, and exactly the state a *successful* run leaves | A later cycle reclaims it, once past the reader grace |
| Hot partitions exhausted | **Inserts fail** | Alert on `partitions_ahead`; pre-creation is automatic but monitored |
| Object store unavailable | Archival backpressures; cold queries fail loudly | Automatic |
| Postgres unavailable | Archival retries; cold queries unaffected | Automatic |
| Iceberg commit conflict | Retried against the refreshed base, summary re-derived | Automatic |
| Second archiver on the same table | Lease refused; the run is a reported no-op | Automatic; alert on lag, not on contention |
| A DDL lock held by a long query | Statement gives up; the run is a reported no-op (`deferred`) | Automatic; alert on lag, not on deferral |
| Incompatible schema change | Table quarantined; rows stay correctable in PostgreSQL | Operator resolves |
| As-of read against an expired snapshot | Fails naming the snapshot | Pick another from `system.snapshots` |
| Clock skew | No impact — tiering is on `from`, a data value | — |
| DST transition | Handled by `metering` | By design |

**Degrade, don't lie** is the rule behind that table. When Iceberg is unreachable,
cold queries fail loudly rather than silently returning only the hot window.

## Compaction and orphan files {#compaction}

Both run **out of band**, with Spark or PyIceberg against the same standard
table — `iceberg-rust` can neither commit a snapshot that removes files (no
rewrite action, and `TableCommit`'s builder is crate-private) nor list a
warehouse to find files the manifests do not reference. The
[interop suite](@/docs/interop.md) checks that a foreign engine reads the schema,
the partition spec, the format version and the tiering watermark out of these
tables.

Neither costs correctness, and **compaction does not recover version elision**,
which is worth saying because it reads as if it should: a corrected reading has
two versions stored and appears twice however the bytes are arranged. Coarser
files in fact push elision the wrong way, since a scan reads whole files. The case
for compaction is the ordinary one — less manifest to plan against.

The one orphan source that is closed needs no listing: a commit that wrote its
data files and failed to land still holds every path, so it deletes them before
returning the error. It re-reads the table first, because a commit can fail
*after* landing.

## Snapshot expiry

Ten years by default, and opt-in: a snapshot is what makes a past settlement
reproducible, so retention is a compliance decision rather than a disk-space one.

It reclaims **metadata, not readings**. The table is append-only, so every data
file an old snapshot referenced is still referenced by the current one; what
expiry bounds is the metadata JSON, whose snapshot array grows with every commit
and is parsed on **every** table load. Unreferenced manifest lists and old
metadata files stay on object storage — removing those needs the listing operation
above.

### The retention window is meterstore's, not the table's

`snapshot_retention` and `min_snapshots_to_keep` decide what expiry removes, and
nothing else does — including `history.expire.*` on the Iceberg table, which
`iceberg`'s expire action would otherwise apply on top. Its age path runs whether
or not snapshot ids are named, defaulting to `max-snapshot-age-ms` of **five
days**, so a store could stop being able to reproduce a settlement older than a
working week.

MeterStore pins that cutoff to the epoch, which selects nothing: the ids computed
against the configured retention are the whole of what is expired. Since
`history.expire.*` is a *table* property, that also keeps out-of-band compaction
from deciding a deployment's retention by setting one.

### Do not expire snapshots from a foreign tool

A foreign commit carries no watermark, so the boundary lookup walks back the
parent chain to find one — and removing **any** ancestor on that walk strands it.
Not just the snapshot carrying the boundary: an intermediate one leaves a hole,
the walk stops at a parent id that no longer resolves, and every query fails at
once on a table that is otherwise healthy.

`expire_snapshots` re-stamps the boundary onto the current snapshot first, so
there is no chain to punch a hole in. `store.reassert_watermark()` is that step on
its own — run it after any out-of-band maintenance if you are not also running
expiry. It republishes what the history already says, cannot move the boundary,
and is a no-op when the current snapshot carries one:

```rust
// After compacting with Spark or PyIceberg:
store.reassert_watermark().await?;
```

## Removing data

`store.purge_table(name)` destroys the PostgreSQL table with every partition, the
Iceberg catalogue entry, and the data files in object storage. It is the **only**
operation in the crate that deletes stored readings — everything else is
append-only, because a settlement must stay reproducible.

The name must be repeated, because a handle carries no visual indication of which
table it points at and there is no recovery path.

There is no way to expire *part* of a table's history: removing a subset means
rewriting files. For the obligation that usually prompts the question, see
[Privacy](@/docs/privacy.md) — the statute asks for the personal link to go, not
the rows, and that is `O(1)`.

Dropping a hot partition, by contrast, destroys nothing: it runs only after those
rows are durable in Iceberg, and a partition the watermark does not cover raises
an invariant violation rather than being dropped.
