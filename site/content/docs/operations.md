+++
title = "Operations"
description = "Scheduling archival, the system tables an operator opens during an incident, metrics worth alerting on, schema evolution, and the failure matrix."
weight = 8
+++

## Scheduling

```rust
let handle = store.maintenance()
    .interval(Duration::minutes(15))
    .expire_snapshots(false)      // retention is a compliance decision
    .spawn();
```

Nothing runs until you call this. A store that started a background loop on
construction would surprise a process that only wanted to read.

One cycle archives every due window (bounded, so a store that has been down for a
month catches up over several cycles rather than holding one process for hours),
optionally expires snapshots, then checks the invariant — in that order, so the
check sees the state the cycle produced.

The handle **owns** the loop: dropping it stops the loop after the cycle in
flight, exactly as `shutdown()` does, so a handle that goes out of scope cannot
leave a task archiving against a store nobody is watching.

Every replica can run the same schedule. One wins the archive lease; the others
report `lease_contended` and stop. That is not a failure.

**The clock is a parameter, never read.** Every archival, maintenance and status
call takes `now`, so a test drives months in milliseconds and nothing depends on
when it ran. Tiering itself never reads a clock at all — it routes on `from`, a
data value — so the only thing wall time decides is *when* a window becomes
eligible.

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
`partition_step` that disagrees with `archival_step`, or a `settlement_lag`
shorter than a window, are each valid alone and wrong together. Seeing them side
by side is how that gets noticed.

All four are snapshots computed when asked. `store.refresh_system_tables(now)`
recomputes them — deliberately explicit, so an ordinary query never silently pays
for a round trip to both tiers.

## Metrics

Instruments are created against the **OpenTelemetry API, not an SDK**. Until your
application installs a meter provider, recording is a no-op. That is the right
contract for a library: MeterStore decides what is worth measuring, the
application decides where measurements go.

Every instrument carries a `table` attribute; scan metrics add `tier`.

| Instrument | Why it exists |
|---|---|
| `meterstore.tiering.invariant_violations` | **The alert.** Non-zero means query results may be wrong. Everything else is degradation. |
| `meterstore.tiering.watermark_lag` | Archival falling behind. Trends toward failure before causing one. |
| `meterstore.tiering.hot_partitions_ahead` | Reaching 0 stops writes outright. |
| `meterstore.archival.rows` / `.duration` / `.failures` | Throughput, and whether a window still fits its schedule. |
| `meterstore.partitions.dropped` / `.orphans_reclaimed` | Purge keeping up; non-zero orphans mean runs are being interrupted. |
| `meterstore.write.rows` / `.rows_deduplicated` / `.late_corrections` | Ingest volume, the redelivery rate (expected to be non-zero), and corrections arriving after their interval was archived. |
| `meterstore.query.plan_duration` / `.scan_duration` | **Two instruments, not one.** A single `query.duration` recorded at plan time measured only the time to build a plan — making a slow catalogue look like a slow query and hiding a slow scan behind fast planning. |
| `meterstore.query.merge_elided` / `.merge_elision_decisions` | The elided ratio — whether the cold layout is still earning its keep. |

## Schema evolution

Metering schemas are regulator-defined and move on multi-year cycles, so this is
far smaller than in a general framework — but not zero.

| Change | Action | Rewrite? |
|---|---|---|
| Add nullable column | Iceberg add column, fresh field id | No |
| Rename column | Field ids are stable, historical files still read | No |
| Widen decimal precision | Type promotion | No |
| Drop column | Marked deleted, retained for time travel | No |
| Add **NOT NULL** column, narrow a type, change the merge key | **Quarantine** | — |

Quarantine is the honest response to a change that cannot be applied safely. The
table halts, its watermark freezes, an operator resolves it. Freezing is the
point rather than a side effect: the rows stay in PostgreSQL, where they can still
be corrected, instead of being archived into a layout nobody has agreed on.
Silent corruption is never traded for uptime.

Two details:

- **A decimal's precision may widen; its scale may not.** Precision adds
  representable digits; changing scale reinterprets every stored integer by a
  factor of ten. In a settlement figure that is a silent order-of-magnitude error.
- **A cold store that cannot report its schema is not treated as compatible.** The
  check simply did not run, and saying so beats implying it passed.

The comparison runs at `build` **and** before every archival run. The second is
not redundant: the cold table can change out of band — an operator running
compaction with Spark, or a second deployment on an older configuration.

## Failure matrix

| Failure | Behaviour | Recovery |
|---|---|---|
| Crash mid-archival, pre-commit | Partition detached, not archived; invisible to writers | Invariant check reports; next run re-archives |
| Crash post-commit, pre-drop | Orphaned detached partition — data intact | Next run drops it |
| Hot partitions exhausted | **Inserts fail** | Alert on `partitions_ahead`; pre-creation is automatic but monitored |
| Object store unavailable | Archival backpressures; cold queries fail loudly | Automatic |
| Postgres unavailable | Archival retries; cold queries unaffected | Automatic |
| Iceberg commit conflict | Retried against the refreshed base, summary re-derived | Automatic |
| Second archiver on the same table | Lease refused; the run is a reported no-op | Automatic; alert on lag, not on contention |
| Incompatible schema change | Table quarantined; rows stay correctable in PostgreSQL | Operator resolves |
| As-of read against an expired snapshot | Fails naming the snapshot | Pick another from `system.snapshots` |
| Clock skew | No impact — tiering is on `from`, a data value | — |
| DST transition | Handled by `metering` | By design |

**Degrade, don't lie** is the rule behind that table. When Iceberg is unreachable,
cold queries fail loudly rather than silently returning only the hot window.

## Maintenance that is not implemented

Two jobs are blocked upstream rather than deferred:

- **Compaction.** `iceberg-rust` has no public way to land a snapshot that
  *removes* files: there is no rewrite action, and both `TransactionAction` and
  `TableCommit`'s builder are crate-private. Every byte of a compacted snapshot
  can be produced and not committed.
- **Orphan-file cleanup.** Blocked one step earlier, on the read path: finding
  orphans means listing the warehouse and subtracting what the manifests
  reference, and `FileIO` exposes no listing operation at all.

Neither costs correctness. Compaction would recover version elision for the few
partitions that lose it; orphans cost storage and arise only from commits that
failed after writing data files, which the compare-and-swap protocol makes rare.
An operator who needs either can run it out of band with Spark or PyIceberg
against the same standard table. The [interop suite](@/docs/interop.md) checks
that this works: PyIceberg reads the schema, the partition spec, the format
version and the tiering watermark out of these tables.

### The one rule an out-of-band tool must not break

A foreign commit is a perfectly valid Iceberg snapshot that says nothing about
tiering — so it carries no watermark, and the boundary lookup walks back the
parent chain to find one.

That works, and it makes snapshot expiry dangerous in a way that is not obvious:
removing **any** ancestor on that walk is enough to strand the boundary. Not just
the snapshot carrying it — an intermediate one leaves the chain with a hole, the
walk stops at a parent id that no longer resolves, and no boundary is found. Every
query then fails at once, on a table that is otherwise perfectly healthy. A
maintenance job following the advice above would have done that.

Two things prevent it:

- **`expire_snapshots` re-stamps the boundary first**, onto the current snapshot,
  so the walk is one snapshot long and there is no chain to punch a hole in. It
  then protects that path anyway, for a history it did not create.
- **`store.reassert_watermark()`** is the same operation on its own. Run it after
  any out-of-band maintenance if you are not also running expiry. It republishes
  what the history already says — it cannot move the boundary — and is a no-op
  when the current snapshot already carries one.

```rust
// After compacting with Spark or PyIceberg:
store.reassert_watermark().await?;
```

**Do not expire snapshots from the foreign tool.** Retention is a compliance
decision and MeterStore owns it; an external expiry knows nothing about the
boundary it might be removing.

Snapshot expiry **is** implemented, and it is the growth that actually compounds.
It defaults to ten years, because a snapshot is what makes a past settlement
reproducible: that is a compliance decision rather than a disk-space one, which is
also why it is opt-in.

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
