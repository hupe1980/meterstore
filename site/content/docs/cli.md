+++
title = "The CLI"
description = "meterstore init, check, create, status, archive, maintain, query, completeness and serve — the same library, without a program to write."
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
| `completeness` | Which channels are short, and which delivered nothing |
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
`settlement_lag` is shorter than its `archival_step` fails there rather than
stranding corrections below the watermark in production.

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
a past settlement reproducible: how far back a settlement can be reproduced is a
compliance decision.

So is the other opt-in job. `--anonymise-after-years 3` runs the § 60 Abs. 6 MsbG
sweep on the same schedule, destroying the linkage of every subject whose readings
have passed the ceiling **in every table**:

```bash
meterstore maintain --anonymise-after-years 3 --anonymise-actor retention-job
```

Off by default and irreversible when on. The years are full calendar years after
the year of collection, not `now - 3 years`: the statutory clock starts at the
*Schluss des Kalenderjahres*, and the rolling spelling would erase a January value
a year early. Needs a table declaring `subject_column` — see
[Privacy and retention](@/docs/privacy.md).

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

## Asking whether a month is complete

A `SUM` over an incomplete month returns a smaller number and no reason, so the
question has its own verb:

```bash
# The settlement period, named the way the market names one.
meterstore completeness --month 2026-06 --seen-since 30d
```

```text
TABLE                MALO          OBIS           SPARTE  RES      EXPECTED    ACTUAL   MISSING   SURPLUS  FIRST GAP     NOTE
readings_versions    41373559241   1-0:1.29.0     STROM   PT15M        2880      2880         0         0  —
readings_versions    56789012345   1-0:1.29.0     STROM   PT15M        2880      2784        96         0  2026-06-14
readings_versions    99887766554   1-0:1.29.0     STROM   PT15M        2880         0      2880         0  2026-06-01    delivered nothing in the range

3 channel(s) over [2026-05-31T22:00:00Z, 2026-06-30T22:00:00Z) — 2 incomplete, 1 silent, 2976 interval(s) missing
```

The expectation is the DST-aware calendar's, per balancing day: 92 intervals on
the spring day, 100 on the autumn one, and for gas both on the **Gastag** rather
than the Sunday. It covers **every day of the range**, so a channel that stopped
mid-month is short by every day after — and a report over a month that has not
finished reports the remainder as missing. Ask about periods that are over.
`--gaps-only` drops the complete rows.

**`--month YYYY-MM` is a Bilanzierungsmonat, not a calendar month.** The range
printed above starts at 22:00 UTC on 31 May because that is midnight in Berlin.
`--sparte GAS` cuts the same span at 06:00 local instead. Rows are always counted
against their *own* `sparte`; the flag decides where the **range** is cut, which
one value cannot do for two — so a table holding both is reported once per
commodity.

`--from`/`--to` take RFC 3339 instants for any other period, and `--malo` /
`--obis` narrow the report to one measuring point or one channel — in the scan,
so asking about one meter does not cost a scan of the portfolio.

**`--seen-since` is the finding a range cannot make about itself.** A channel that
delivered nothing produces no rows to aggregate, so it is absent from the report
rather than reported as empty — unless a roster is drawn from an earlier window.
There is no default, because what a roster means is master data this crate does
not hold: too short and a meter read monthly looks decommissioned, too long and
every terminated measuring point is a standing finding. `--malo` and `--obis`
narrow the roster too, or every channel outside them would come back as silent.

This exits **zero** whatever it finds. A gap is a fact to triage; `status` is the
check that fails, because a stranded row means query results are *wrong* rather
than incomplete. For a monitoring check, read the JSON — it carries
`channels_reported`, `channels_incomplete`, `channels_silent` and
`intervals_missing` beside the rows:

```bash
meterstore completeness --month 2026-06 --format json \
  | jq -e '.channels_incomplete == 0'
```

More on what the numbers mean, and why `missing` is not `expected - actual`, in
[Completeness](@/docs/completeness.md).

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
