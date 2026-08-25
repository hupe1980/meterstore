+++
title = "The CLI"
description = "meterstore init, check, create, status, archive, maintain, query and serve — the same library, without a program to write."
weight = 9
+++

```bash
cargo install meterstore --features cli
```

A thin front end over the same public API a service uses: every subcommand is one
or two library calls, and nothing here is unreachable from Rust.

| | |
|---|---|
| `init` `check` | Write a starter configuration; validate it, connecting to nothing |
| `create` | Both tiers, every declared table. Idempotent |
| `status` | Boundary, lag, write runway, health. Non-zero exit when unhealthy |
| `archive` `maintain` | One-shot for cron; foreground loop for a sidecar |
| `query` `explain` | SQL across both tiers, with the boundary it ran against |
| `snapshots` | What a settlement rerun can pin to |
| `serve` | Flight SQL and the Iceberg REST façade |
| `purge` | Destroy a table. No recovery path |

Every command takes `-c/--config` (or `METERSTORE_CONFIG`) and defaults to
`meterstore.toml` in the working directory.

## Getting to a working store

```bash
meterstore init            # a commented starter configuration
$EDITOR meterstore.toml
meterstore check           # full validation — no database needed
meterstore create          # both tiers, every declared table
meterstore status
```

`check` connects to nothing, which is what makes it a CI step: a file whose
`archival_step` disagrees with its `partition_step` fails there rather than
degrading a purge to a row-wise `DELETE` in production.

## Keeping archival running

```bash
# The sidecar shape: a foreground loop, ctrl-c to stop.
meterstore maintain --interval 15m

# The cron shape: archive what is due, then exit.
meterstore archive --max-windows 8
```

Both are safe on **every** replica. One wins each table's archive lease and the
others report contention and stop, which is not a failure — see
[Operations](@/docs/operations.md#scheduling).

`maintain` stops after the cycle in flight rather than at the signal. For a cycle
mid-archival that is the difference between a clean stop and an orphaned
partition the next run has to reclaim.

Snapshot expiry is opt-in (`--expire-snapshots`), because a snapshot is what makes
a past settlement reproducible: retention is a compliance decision.

## Asking what the store holds

```bash
meterstore query "SELECT meter_balancing_day(\"from\", sparte) AS day, SUM(value)
                  FROM readings
                  WHERE malo_id = '41373559241'
                  GROUP BY 1 ORDER BY 1"
```

```text
+------------+------------+
| day        | sum(value) |
+------------+------------+
| 2026-07-19 | 412.750000 |
| 2026-07-20 | 408.125000 |
+------------+------------+

boundary  readings_versions: 2026-08-18T00:00:00Z
tiers     cold + hot
warning   this answer includes the hot window, so it is only valid for now —
          those intervals are still being corrected
```

**The footer is the point.** A `SUM` alone cannot say whether it crossed the tier
boundary, and two identical queries a minute apart can read the same rows from
different tiers. Every result carries the boundary it was computed against and
whether it reproduces.

`--historical` reads the settled history alone, with no load on PostgreSQL, and
the footer then says the answer does not change. `--operational` reads the recent
window alone, with no Iceberg round trip.

`explain` shows the schema and the same provenance without running the statement —
which tiers a query would touch is usually the thing worth knowing about a slow
one.

A statement of `-` reads from standard input, so a long query can live in a file:

```bash
meterstore query - < settlement.sql
```

## Machine output

`--format json` renders one document per invocation, with the provenance beside
the rows rather than under them:

```bash
meterstore status --format json | jq '.tables[] | select(.healthy | not)'
```

```json
{
  "rows": [{ "day": "2026-07-19", "sum(value)": "412.750000" }],
  "row_count": 1,
  "provenance": {
    "watermarks": [{ "table": "readings_versions", "watermark": "2026-08-18T00:00:00Z" }],
    "tiers_scanned": ["cold", "hot"],
    "reproducible": false
  }
}
```

Decimals are rendered as strings, not JSON numbers. Settlement is money, and a
double is not.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Failed, and retrying will not help — a bad statement, invalid configuration, a refused delivery |
| `75` | Failed transiently — the database was unreachable, a lock was not available. `EX_TEMPFAIL`, so a supervisor should try again |

`status` exits non-zero when a table is unhealthy, which is what makes it usable
as a monitoring check. The two ways a table stops working exit differently and the
message names which:

- **Rows stranded below the watermark.** Query results may be wrong. This is the
  alert.
- **No hot partition left ahead of the write frontier.** The next insert fails
  outright. Check that archival is running.

## Serving

```bash
meterstore serve --addr 127.0.0.1:50051 \
                 --catalog-addr 127.0.0.1:8181     # optional
```

Two surfaces, answering different questions.

**Flight SQL** carries the unified hot + cold view — the one surface an external
client cannot assemble for itself, because the hot tier is not in the Iceberg
catalogue.

**The catalogue façade** is a read-only Iceberg REST endpoint, and it is what a
**SQL-catalog** deployment needs so Spark, Trino, DuckDB and PyIceberg can read
the settled history straight from object storage — with this process in the
metadata path only, never the data path, so readers scale in parallel and it is
neither a bottleneck nor a single point of failure. A deployment already on a REST
catalogue needs none of it: point engines at the endpoint it already has, which is
why the flag is opt-in and Flight SQL's is not.

Analytics over settled history belong on the catalogue, read directly. Routing
them through Flight adds a proxy hop and serialises parallel reads through one
process.

One `ctrl-c` stops both. A server still answering metadata while its query surface
is down is worse than one that stopped, because a client cannot tell.

> **Unauthenticated, both of them.** This crate has no business deciding what kind
> of authentication a deployment uses, so the library hands back a tonic service
> and an axum router to wrap in your own interceptor and TLS, and the CLI binds
> them bare. Bind them to loopback or a trusted network, never to a public one.
> Read-only either way: Flight refuses every mutating call and every statement
> that is not a query, and the façade has no write path at all — an external
> writer appending through it would put rows in the cold tier without MeterStore
> knowing, and nothing downstream could detect it.

## Destroying a table

```bash
meterstore purge --table readings_versions --confirm readings_versions
```

The only operation in the crate that deletes stored readings — every partition,
the catalogue entry and the data files in object storage. There is no recovery
path, which is why the name has to be given twice.

## No `meterstore append`

A reading arrives as an MSCONS message, an SMGW push or a CSV a utility exports
its own way, and each needs a mapping this crate has no opinion about — which OBIS
code, which network operator issued the version, which Messlokation. That is
[Writing readings](@/docs/writing.md); `check` is what the CLI offers it.
