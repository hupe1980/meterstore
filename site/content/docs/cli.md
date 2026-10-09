+++
title = "The CLI"
description = "meterstore init, check, create, status, archive, maintain, reassert-watermark, query, completeness, audit and serve — the same library, without a program to write."
weight = 9
+++

```bash
cargo install meterstore --features cli
```

`cli` brings Flight SQL, the catalogue façade and both catalogue features. A
warehouse in cloud object storage needs its store compiled in too —
`--features cli,object-store-s3` (or `-gcs`, `-azure`) — and S3 Tables needs
`s3tables`; naming one the binary was built without is an error that says so.

A thin front end over the public API: every subcommand is one or two library
calls.

| | |
|---|---|
| `init` `check` | Write a starter configuration; validate it, connecting to nothing |
| `create` | Both tiers, every declared table. Idempotent |
| `status` | Boundary, lag, write runway, health. Non-zero exit when unhealthy |
| `archive` `maintain` | One-shot for cron; foreground loop for a sidecar |
| `reassert-watermark` | The step that follows out-of-band maintenance |
| `query` `explain` | SQL across both tiers, with the boundary it ran against |
| `completeness` | Which channels are short, and which delivered nothing |
| `audit` | Whether an attribute column behaves like identity in the data |
| `snapshots` | What a settlement rerun can pin to |
| `erasures` | The erasure audit trail, deployment-wide |
| `serve` | Flight SQL and the Iceberg REST façade |
| `purge` | Destroy a table. No recovery path |

Every command takes `-c/--config` (or `METERSTORE_CONFIG`) and defaults to
`meterstore.toml` in the working directory. **Only the verbs that write create
tables**: `create`, `archive`, `maintain` and `reassert-watermark` create any
declared table that does not exist yet. The verbs that read — `status`, `query`,
`explain`, `completeness`, `audit`, `snapshots`, `erasures`, `serve`, `purge` —
open what exists and run no DDL, so they work on a role that may only read; a
table nobody has created is an error naming `meterstore create`.

## Getting to a working store

```bash
meterstore init            # a commented starter configuration
$EDITOR meterstore.toml
meterstore check           # full validation — no database needed
meterstore create          # both tiers, every declared table
meterstore status
```

`check` connects to nothing, so it runs in CI: a `settlement_lag` shorter than
its `archival_step` fails there rather than in production.

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

`maintain` stops after the cycle in flight rather than at the signal.

Snapshot expiry is opt-in (`--expire-snapshots`): how far back a settlement can be
reproduced is a compliance decision. So is `--anonymise-after-years 3`, which runs
the § 60 Abs. 6 MsbG sweep on the same schedule, destroying every subject linkage
whose **collection year** has passed the ceiling — deployment-wide, whether or not
the subject is still metered:

```bash
meterstore maintain --anonymise-after-years 3 --anonymise-actor retention-job
```

Irreversible. The years are full calendar years after the year of collection, not
`now - 3 years`: the statutory clock starts at the *Schluss des Kalenderjahres*.
It needs the deployment's subject registry, which exists when a table declares
`subject_column` or the database already holds `meterstore_subject_map`. See
[Privacy and retention](@/docs/privacy.md).

## After out-of-band maintenance

Orphan cleanup, manifest rewrites and a foreign tool's snapshot expiry run out of
band; compaction is refused ([Operations](@/docs/operations.md#compaction)). After
a foreign commit the boundary is found by walking back the parent chain, and
expiring **any** ancestor on that walk fails every query on the table.

```bash
# At the head of the same job, before the tool expires anything.
meterstore reassert-watermark
```

It **cannot move the boundary**, does nothing when the current snapshot already
carries one, and exits cleanly on a table that has never archived — safe to run
unconditionally. `--table` narrows it to one. `maintain --expire-snapshots`
re-stamps before its own expiry.

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

**The footer is the point**: the boundary the result was computed against, and
whether it reproduces. `--historical` reads the settled history alone, with no
load on PostgreSQL; `--operational` reads the recent window alone. `explain` shows
the schema and the same provenance without running the statement. A statement of
`-` reads from standard input:

```bash
meterstore query - < settlement.sql
```

## Asking whether a month is complete

```bash
# The settlement period, named the way the market names one.
meterstore completeness --month 2026-06 --seen-since 30d
```

```text
TABLE                MALO          OBIS           SPARTE  RES      EXPECTED    ACTUAL   MISSING   SURPLUS  FIRST GAP     NOTE
readings_versions    41373559241   1-0:1.29.0     STROM   PT15M        2880      2880         0         0  —
readings_versions    56789012345   1-0:1.29.0     STROM   PT15M        2880      2784        96         0  2026-06-14
readings_versions    99887766555   1-0:1.29.0     STROM   PT15M        2880         0      2880         0  2026-06-01    delivered nothing in the range

3 channel(s) over [2026-05-31T22:00:00Z, 2026-06-30T22:00:00Z) — 2 incomplete, 1 silent, 2976 interval(s) missing
```

The expectation is the DST-aware calendar's, per balancing day (the **Gastag** for
gas), over **every day of the range** — so a month not yet over reports the
remainder as missing. `--gaps-only` keeps every row that is short, long, silent
**or not measurable**.

**`--month YYYY-MM` is a Bilanzierungsmonat.** The range above starts at 22:00 UTC
on 31 May, midnight in Berlin; `--sparte GAS` cuts it at 06:00 local instead. Rows
are always counted against their own `sparte`, so a table holding both commodities
is reported once per commodity.

`--from`/`--to` take RFC 3339 instants for any other period. `--malo`, `--melo`
and `--obis` narrow the scan, each **parsed** so a typo fails rather than
narrowing to nothing.

**`--seen-since`** draws a roster from an earlier window, so a channel that
delivered nothing appears as silent instead of not at all. There is no default:
too short and a monthly-read meter looks decommissioned, too long and every
terminated measuring point is a standing finding.

This exits **zero** whatever it finds; `status` is the check that fails. For
monitoring, read the JSON — it carries `channels_reported`, `channels_incomplete`,
`channels_silent`, `channels_unmeasurable` and `intervals_missing`:

```bash
meterstore completeness --month 2026-06 --format json \
  | jq -e '.channels_incomplete == 0 and .channels_unmeasurable == 0'
```

Both halves matter: a channel with no declared resolution reports `complete`
without being checked. More in [Completeness](@/docs/completeness.md).

## Checking a declaration against the data

Declaring a tenant discriminator as an *attribute* rather than an identity column
raises no error, but one tenant then silently supersedes the other. So the rows
are asked:

```bash
meterstore audit
```

```text
TABLE                        COLUMN                       KEYS     REPEATED   WIDEST    SHARE
readings_versions            bilanzkreis                 84_112          37        2     0.0%
readings_versions            ingest_source               84_112      41_930        2    49.9%
```

`REPEATED` is how many merge keys carry more than one value of the column. A few
are corrections (the first row); half is an identity column in everything but the
declaration (the second). A **report, not a verdict**; it exits zero.
`--column` and `--table` narrow it; an identity column or an unknown name is
refused. It is a full group-by over the table, so run it deliberately.

## Machine output

`--format json` renders one document per invocation, with the provenance beside
the rows rather than under them:

```bash
meterstore status --format json | jq '.tables[] | select(.healthy | not)'
```

A query's document, for one:

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

Decimals are rendered as strings, not JSON numbers.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Failed, and retrying will not help — a bad statement, invalid configuration, a refused delivery |
| `75` | Failed transiently — every error `is_retryable()` calls so: the database was unreachable, a lock was not available, or `BoundaryMoved` — a plan, scan or write that met the tiering boundary while archival was moving it. `EX_TEMPFAIL`, so a supervisor should try again |

`status` exits `1` when a table is unhealthy, and the message names which way:

- **Rows stranded below the watermark.** Query results may be wrong. This is the
  alert.
- **No hot partition left ahead of the write frontier.** The next insert fails
  outright. Check that archival is running.

## Serving

```bash
meterstore serve --addr 127.0.0.1:50051 \
                 --catalog-addr 127.0.0.1:8181     # optional
```

**Flight SQL** carries the unified hot + cold view, which an external client
cannot assemble because the hot tier is not in the Iceberg catalogue. Point an
**ADBC** client at `grpc://host:port`, no driver-specific configuration.

**The catalogue façade** (`--catalog-addr`, opt-in) is a read-only Iceberg REST
endpoint, which a **SQL-catalog** deployment needs so Spark, Trino, DuckDB and
PyIceberg can read the settled history straight from object storage. A REST
catalogue deployment points engines at its own endpoint instead. Analytics over
settled history belong on the catalogue, not on Flight.

One `ctrl-c` stops both.

> **Unauthenticated, both of them.** The library hands back a tonic service and
> an axum router to wrap in your own authentication and TLS; the CLI binds them
> bare. Bind them to loopback or a trusted network, never to a public one. Both
> are read-only: Flight refuses every mutating call and every statement that is
> not a query, and the façade has no write path.

## Destroying a table

```bash
meterstore purge --table readings_versions --confirm readings_versions
```

The only operation in the crate that deletes stored readings — every partition,
the catalogue entry and the data files in object storage. There is no recovery
path, which is why the name has to be given twice.

## The erasure trail

```bash
meterstore erasures --limit 50

# What an auditor actually asks for: a period, and one duty.
meterstore erasures --since 2026-07-01T00:00:00Z --until 2026-10-01T00:00:00Z \
                    --trigger retention
```

Every erasure writes a row saying **when, why, by whom** and **which duty it
discharged** — and not *whose*, since the natural identifier is what is being
destroyed.

The TRIGGER column is `request` for an Article 17 erasure and `retention` for the
§ 60 Abs. 6 sweep — [different legal bases](@/docs/privacy.md#the-trail-as-evidence),
asked about separately.

`--since`/`--until` are half-open, so consecutive quarters tile. A backwards
period, an unknown `--trigger`, a non-positive `--limit`, and a deployment with no
subject registry are each refused or reported as such, never printed as an empty
trail that reads as "nothing was erased". The trail is deployment-wide.

`— (suppression only)` in SUBJECT is a request naming an identifier with no
mapping yet, honoured by refusing the identifier from then on. An indented
`suppression lifted …` line is an erasure against the wrong subject, released,
with who did it and why.

## No `meterstore erase`

An Article 17 request usually reaches an application's own tables too, which must
succeed or fail **together** with the mapping. A CLI invocation cannot enclose
them in its transaction; `SubjectRegistry::erase_in` and `erase_all_in` take one
the caller owns. [Privacy and retention →](@/docs/privacy.md) The retention duty
the CLI does run: `meterstore maintain --anonymise-after-years 3`.

## No `meterstore append`

Mapping an MSCONS message, an SMGW push or a utility's CSV to readings — which
OBIS code, which network operator, which Messlokation — is an application's job.
See [Writing readings](@/docs/writing.md).
