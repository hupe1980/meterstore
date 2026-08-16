+++
title = "External engines"
description = "Reading the history from Spark, Trino, DuckDB or PyIceberg with MeterStore out of the data path — and the version-resolution trap that silently double-counts if you skip one step."
weight = 9
+++

**The default answer for external engines is the Iceberg catalogue, not a
MeterStore endpoint.** Spark, Trino, DuckDB, PyIceberg and Snowflake read the
history *directly from object storage*. MeterStore is then in the metadata path
only — never the data path — so readers scale in parallel and MeterStore is
neither a bottleneck nor a single point of failure.

Two engines read the output in the test suite:

| Engine | What it reads |
|---|---|
| **DuckDB** | The Parquet files and the Iceberg metadata. Agrees with MeterStore on which files belong to the table, on the values, and on the snapshot count. Groups a gas Lastgang across a DST transition by `balancing_day` and matches MeterStore's own histogram. |
| **PyIceberg** | The schema with its field ids, the partition spec, the sort order, the format version, and the tiering watermark from the snapshot summary. |


Field ids matter because Iceberg resolves columns by id, not by name: a writer
that assigned them differently produces a table that opens and returns the wrong
column. PyIceberg is also the engine this documentation recommends for the
[maintenance MeterStore cannot do itself](@/docs/operations.md), so it is checked
rather than assumed.

## Resolving corrections {#the-version-resolution-trap}

`readings_versions` is a versioned table: a correction is a new row with a higher
`version`, never an update. Take the highest per reading.

```sql
SELECT malo_id, obis_code, "from", value, … FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY malo_id, obis_code, "from", version_scope
    ORDER BY version DESC
  ) AS _meterstore_rank
  FROM readings_versions
) AS _meterstore_resolved WHERE _meterstore_rank = 1
```

Two parts of that are not guessable:

- **`version_scope` is in the `PARTITION BY`.** MSCONS assigns versions per
  network operator per month, so versions from different scopes are not
  comparable and must not be ranked against each other.
- **Identity columns widen the merge key.** If the deployment declares any
  ([storage model](@/docs/storage-model.md)), add them to the `PARTITION BY` —
  omitting one resolves *across* it, so with a `tenant` column one tenant's
  correction supersedes another's reading.

## Gas is not on the calendar day {#the-gas-day-trap}

Every row carries a **`balancing_day`** column — the Berlin calendar day for
electricity, heat and water, the **Gastag** (06:00 to 06:00 local, GaBi Gas,
Art. 3 Nr. 6 VO (EU) 312/2014) for gas. Group by it:

```sql
SELECT balancing_day, SUM(value) FROM readings GROUP BY 1;
```

`date_trunc('day', "from")` is wrong twice over: `"from"` is UTC, so the Berlin
day boundary sits at 22:00 or 23:00 UTC; and gas is not balanced on the calendar
day at all. Neither error raises anything — both produce a plausible daily curve.

The column is computed at write time because SQL dialects differ on timestamp
arithmetic, so no single published expression is right everywhere.

At a DST transition the long and short gas days are the ones named after the
**Saturday**, because the clocks change before the 06:00 boundary:

| 2026 | Calendar day | Gastag |
|---|---|---|
| Sat 24 Oct | 96 | **100** |
| Sun 25 Oct | **100** | 96 |

The DuckDB suite groups a gas workload across that transition by the stored
column, matches MeterStore's own histogram, and checks that the naive UTC
grouping disagrees.

## Which catalogue you are on matters

| Catalogue | External access | Action |
|---|---|---|
| **REST** (Polaris, Lakekeeper, Nessie, Gravitino) | Point engines at the same endpoint | **Nothing to build** |
| **AWS S3 Tables** | Point engines at the table bucket; Athena, EMR and Glue already know it | **Nothing to build** |
| **SQL** (Postgres-backed) | Needs the JDBC catalogue implementation; Trino and Spark can, DuckDB and PyIceberg support is uneven | **Serve the façade** |

The reason is structural rather than a gap in any engine. A REST catalogue hands
an engine the current metadata pointer. The SQL catalogue keeps that pointer in
PostgreSQL and writes no `version-hint.text` beside the files, so an engine
pointed at the bare directory must glob and pick what looks newest — which can
select a metadata document that was never committed. DuckDB requires
`unsafe_enable_version_guessing = true` to open such a table at all.

### The catalogue façade

```rust
let router = cold_tier.catalog_facade().router();   // feature = "catalog-facade"
```

A read-only Iceberg REST Catalog endpoint implementing the spec's config,
namespace and table-metadata routes. Every Iceberg engine speaks this; nothing
MeterStore-specific is needed client-side.

Three decisions it makes explicit:

- **There is no write path, rather than a write path that is off.** Concurrent
  external writers would break the tiering invariant, and nothing downstream could
  detect it: the files would be valid Iceberg, the invariant check only looks at
  PostgreSQL, and the first symptom would be a figure that does not reconcile.
  Mutating routes answer **405** with that reason — not 501, which invites a
  client to retry against a future version.
- **It carries no object-store credentials.** The response says where the data is,
  not how to authenticate to it. Engines use their own credentials, which is what
  keeps the endpoint out of the data path — and means compromising it does not
  hand over the warehouse.
- **It returns a `Router`, not a bound port**, so you wrap it in your own
  authentication, TLS and tracing.

## Flight SQL, and when *not* to use it

| Consumer need | Right surface |
|---|---|
| Analytics over history | The Iceberg catalogue, direct read |
| A Rust application, in-process | The typed API |
| **Unified hot + cold from a non-Rust client** | **Flight SQL** |
| A BI tool over live data | Flight SQL JDBC/ODBC |

The hot tier lives in PostgreSQL and is not in the catalogue, so **the unified
view is the one thing external clients cannot assemble themselves.** That is
Flight's entire justification here — real but narrow. Routing analytics through it
would be *worse* than reading object storage directly: a proxy hop that serialises
parallel reads through one process.

```rust
let service = FlightSqlServer::new(store).into_service();   // feature = "flight"
```

- **Read-only.** A write here would bypass the tier routing and the subject-
  reference check, neither of which is recoverable afterwards, so every mutating
  call answers `PermissionDenied` naming both.
- **Results carry their boundary over the wire.** The watermark, the tiers scanned
  and the read mode travel as **Arrow schema metadata** on every response, so a BI
  tool that keeps the schema keeps the provenance.
- **The statement handle *is* the SQL.** A server-side cache would need eviction
  and would leak on a disconnected client, and at metering result sizes it buys
  nothing — the cost is the scan, not the parse.
- **`GetFlightInfo` plans; `DoGet` executes.** A client learns about a bad
  statement before it starts rendering a stream, and the rows are produced once.
  Answering the metadata call by *running* the query would make a BI tool's
  ordinary two-call sequence cost two full scans, on the one surface built for BI
  tools — and preparing a statement would execute it.
- **`into_service` returns a tonic service, not a bound port**, for the same
  reason the façade returns a router.

`information_schema` is enabled on the store's session, so a BI tool can list the
catalogue before it queries anything — and the distinction between `readings` and
`readings_versions` is discoverable rather than folklore.
