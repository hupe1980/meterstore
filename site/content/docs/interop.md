+++
title = "External engines"
description = "Reading the history from Spark, Trino, DuckDB or PyIceberg with MeterStore out of the data path — and the version-resolution trap that silently double-counts if you skip one step."
weight = 10
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
    ORDER BY version DESC, recorded_at DESC
  ) AS _meterstore_rank
  FROM readings_versions
) AS _meterstore_resolved WHERE _meterstore_rank = 1
```

Do not retype it. `system.resolution` carries this exact text for *your* table,
merge key and all, and `store.resolution_sql()` returns the same string — both
from the one definition the store itself plans against, so a pasted query and the
store cannot drift.

Three parts of it are not guessable:

- **`version_scope` is in the `PARTITION BY`.** MSCONS assigns versions per
  network operator per month, so versions from different scopes are not
  comparable and must not be ranked against each other.
- **The merge key may be wider than those three.** Declared identity columns
  join it, and so does `melo_id` on a table that
  [identifies a reading by its Messlokation](@/docs/storage-model.md#the-messlokation-may-be-part-of-the-identity)
  — which a Zählerstandsgang does by default. Omitting one resolves *across* it:
  with a `tenant` column one tenant's correction supersedes another's reading,
  and without `melo_id` on a point table one meter's register supersedes the
  meter next door's. This is the part not to retype from memory — take it from
  `system.resolution`, which carries your table's own key.
- **`recorded_at DESC` breaks ties.** MeterStore's own write paths refuse to
  store two rows at one `(merge key, version)`, so a tie should not arise — but
  a warehouse is a shared surface, and `ROW_NUMBER` with a partial ordering
  returns a different row on a different plan. A settlement that reproduces only
  sometimes is not reproducible.

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

The same boundary carries up to the **month**: EDI@Energy *Allgemeine
Festlegungen* v6.1c, Kap. 3.1 defines the gas Bilanzierungsmonat as 01.06 06:00
to 01.07 06:00, so `version_scope`'s `YYYY-MM` for a gas row is cut at 06:00 too.
A row at 02:00 local on 1 March carries February's scope. Reading it as a
calendar month splits one operator-month in two.

**And the stored column already answers that too.** A Bilanzierungsmonat is a
whole number of balancing days, so its first day is the first of the month
`balancing_day` falls in — which makes the monthly roll-up a `DATE` operation
with no zone conversion, no DST reasoning and no dialect:

```sql
-- Right for gas and for electricity, on every engine.
SELECT date_trunc('month', balancing_day) AS bilanzierungsmonat, SUM(value)
FROM readings
GROUP BY 1;
```

`date_trunc('month', "from")` is not the same expression, and is wrong for the
same two reasons the daily one is. MeterStore's own SQL has
`meter_balancing_month("from", sparte)`; the test suite pins the two to agree.

At a DST transition the long and short gas days are the ones named after the
**Saturday**, because the clocks change before the 06:00 boundary:

| 2026 | Calendar day | Gastag |
|---|---|---|
| Sat 24 Oct | 96 | **100** |
| Sun 25 Oct | **100** | 96 |

The DuckDB suite groups a gas workload across that transition by the stored
column, matches MeterStore's own histogram, and checks that the naive UTC
grouping disagrees.

## Decimals cross as strings

`QueryResult::to_json` renders `value`, `version` and any `SUM` over them as
`"123.456789"`, not `123.456789`. That is the one shape in which JSON carries an
exact decimal.

A JSON *number* cannot. Arrow renders the decimal exactly, and then every
ordinary reader — `serde_json` without `arbitrary_precision`, every JavaScript
engine, Python's `json` — parses it into an `f64`. At the full eighteen digits
the last one is simply gone: `123456789012.345678` reads back
`123456789012.34567`. No error, no warning, and a settlement figure that no
longer reconciles.

Enabling `arbitrary_precision` would fix `serde_json` and none of the consumers,
who are the point of a JSON surface. Timestamps are already strings for the same
reason. Non-decimal columns are unchanged: a count is still a number.

Arrow IPC and Flight SQL are unaffected — they carry `Decimal128` as itself.

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

```bash
meterstore serve --catalog-addr 127.0.0.1:8181     # the same thing, bound
```

A read-only Iceberg REST Catalog endpoint implementing the spec's config,
namespace and table-metadata routes. Every Iceberg engine speaks this; nothing
MeterStore-specific is needed client-side.

Four decisions it makes explicit:

- **It serves one namespace, not the whole catalogue.** A SQL catalogue is a table
  in a database, and a database is a thing organisations share.
  `cold_tier.catalog_facade()` is confined to the tier's own namespace and answers
  **404 `NoSuchNamespaceException`** for anything else, the listing included.
  `CatalogFacade::new(catalog)` serves everything, for a caller that means it.
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
  authentication, TLS and tracing. [The CLI](@/docs/cli.md#serving) binds it bare
  for a deployment that puts its own proxy in front, alongside Flight SQL and
  under one shutdown.

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
let service = FlightSqlServer::new(store).into_service();     // feature = "flight"
let service = FlightSqlServer::new(catalog).into_service();   // …or every table
```

Any Flight SQL client reaches it. From Python, that is ADBC and no
driver-specific configuration:

```python
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://meterstore:50051") as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM readings")   # both tiers, one answer
    print(cur.fetchone()[0])
```

The interop suite drives exactly that — a driver written in another language,
which validates the server's responses against the specification rather than
accepting what it is handed.

- **A store or a whole catalogue.** The server takes any `SqlSurface` — the pair
  of methods serving needs: plan a statement without running it, and run it as a
  stream. A statement spanning two
  [tables](@/docs/querying.md#several-tables-in-one-session) is the second thing
  an external client cannot assemble for itself, since each has its own watermark
  and its own hot half.
- **Read-only.** A write here would bypass the tier routing and the subject-
  reference check, neither of which is recoverable afterwards, so every mutating
  call answers `PermissionDenied` naming both.
- **Results carry their boundary over the wire.** The watermark, the tiers scanned
  and the read mode travel as **Arrow schema metadata** on every *query* response,
  so a BI tool that keeps the schema keeps the provenance. Not on the catalogue
  RPCs: `GetCatalogs`, `GetDbSchemas`, `GetTables` and `GetTableTypes` answer with
  the schemas the specification fixes, field for field down to nullability,
  because those are a wire contract rather than an answer about readings — and a
  conforming client rejects anything else. Over a catalogue,
  `meterstore.watermarks` names *each* table the statement touched and its
  boundary, beside the conservative minimum — two tables have two boundaries, and
  one number would claim a figure was settled to a point only half its inputs had
  reached.
- **Rows are streamed, never collected.** This is the surface built for BI tools,
  and a BI tool's query is the one whose rows genuinely are the answer. Buffering
  the result to send it would make the server's peak memory the size of whatever
  a client asked for, over a socket the client controls. The provenance is
  available before the first batch, which is what lets the schema go out first.
- **The server says what it is.** `GetSqlInfo` carries the name, the version, the
  Arrow version it encodes with, and `FLIGHT_SQL_SERVER_READ_ONLY` — the
  machine-readable half of the refusal above. A client that cannot see that flag
  offers the user an `INSERT` and delivers the permission error at the end of a
  session rather than not offering it.
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

`information_schema` is enabled on the session, so a BI tool can list the
catalogue before it queries anything — and the distinction between `readings` and
`readings_versions` is discoverable rather than folklore.
