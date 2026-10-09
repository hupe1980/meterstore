+++
title = "External engines"
description = "Reading the history from Spark, Trino, DuckDB or PyIceberg with MeterStore out of the data path — and the version-resolution trap that silently double-counts if you skip one step."
weight = 10
+++

**The default answer for external engines is the Iceberg catalogue, not a
MeterStore endpoint.** Spark, Trino, DuckDB, PyIceberg and Snowflake read the
history *directly from object storage*, with MeterStore out of the data path.

Three engines read the output in the test suite — two from the lake, one over
[Flight SQL](#flight-sql-and-when-not-to-use-it):

| Engine | What it reads |
|---|---|
| **DuckDB** | The Parquet files and the Iceberg metadata. Agrees with MeterStore on which files belong to the table, on the values, and on the snapshot count. Groups a gas Lastgang across a DST transition by `balancing_day` and matches MeterStore's own histogram. |
| **PyIceberg** | The schema with its field ids, the partition spec, the sort order, the format version, and the tiering watermark from the snapshot summary. |
| **ADBC** (Python, Flight SQL driver) | The unified view: opens a connection, lists what it may query, reads the server's `GetSqlInfo`, and runs a query spanning the watermark. |

Field ids are checked because Iceberg resolves columns by id: wrong ids give a
table that opens and returns the wrong column.

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

Do not retype it. `system.resolution` and `store.resolution_sql()` carry this
exact text for *your* table, merge key and all, from the definition the store
plans against. Three parts of it are not guessable:

- **`version_scope` is in the `PARTITION BY`.** MSCONS assigns versions per
  network operator per month, so versions from different scopes are not
  comparable and must not be ranked against each other.
- **The merge key may be wider than those three.** Declared identity columns
  join it, and so does `melo_id` on a table that
  [identifies a reading by its Messlokation](@/docs/storage-model.md#the-messlokation-may-be-part-of-the-identity)
  — which a Zählerstandsgang does by default. Omitting one resolves *across* it:
  one tenant's correction supersedes another's reading, or one meter's register
  the meter next door's.
- **`recorded_at DESC` breaks ties.** MeterStore refuses two rows at one
  `(merge key, version)`, but a warehouse is shared, and `ROW_NUMBER` over a
  partial ordering returns a different row on a different plan.

## Gas is not on the calendar day {#the-gas-day-trap}

Every row carries a **`balancing_day`** column — the Berlin calendar day for
electricity, heat and water, the **Gastag** (06:00 to 06:00 local, GaBi Gas,
Art. 3 Nr. 16 VO (EU) 2017/459) for gas. Group by it:

```sql
SELECT balancing_day, SUM(value) FROM readings GROUP BY 1;
```

`date_trunc('day', "from")` is wrong twice over: `"from"` is UTC, so the Berlin
day boundary sits at 22:00 or 23:00 UTC; and gas is not balanced on the calendar
day. Both produce a plausible daily curve. The column is computed at write time
because SQL dialects differ on timestamp arithmetic.

The same boundary carries up to the **month**: EDI@Energy *Allgemeine
Festlegungen* v6.1c, Kap. 3.1 defines the gas Bilanzierungsmonat as 01.06 06:00
to 01.07 06:00, so `version_scope`'s `YYYY-MM` for a gas row is cut at 06:00 too.
A row at 02:00 local on 1 March carries February's scope. A Bilanzierungsmonat is
a whole number of balancing days, so the monthly roll-up is a `DATE` operation on
the stored column:

```sql
-- Right for gas and for electricity, on every engine.
SELECT date_trunc('month', balancing_day) AS bilanzierungsmonat, SUM(value)
FROM readings
GROUP BY 1;
```

`date_trunc('month', "from")` is wrong for the same two reasons as the daily one.
The test suite pins this to agree with `meter_balancing_month("from", sparte)`.

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
`"123.456789"`, not `123.456789`: ordinary JSON readers — JavaScript, Python's
`json`, `serde_json` without `arbitrary_precision` — parse a number into an `f64`,
and `123456789012.345678` reads back `123456789012.34567`. Non-decimal columns are
unchanged.

Arrow IPC and Flight SQL are unaffected — they carry `Decimal128` as itself.

## Which catalogue you are on matters

| Catalogue | External access | Action |
|---|---|---|
| **REST** (Polaris, Lakekeeper, Nessie, Gravitino) | Point engines at the same endpoint | **Nothing to build** |
| **AWS S3 Tables** | Point engines at the table bucket; Athena, EMR and Glue already know it | **Nothing to build** |
| **SQL** (Postgres-backed) | Needs the JDBC catalogue implementation; Trino and Spark can, DuckDB and PyIceberg support is uneven | **Serve the façade** |

The SQL catalogue keeps the current metadata pointer in PostgreSQL and writes no
`version-hint.text`, so an engine pointed at the bare directory must guess the
newest metadata — possibly one never committed. DuckDB requires
`unsafe_enable_version_guessing = true` to open such a table at all.

### The catalogue façade

```rust
let router = cold_tier.catalog_facade().router();   // feature = "catalog-facade"
```

```bash
meterstore serve --catalog-addr 127.0.0.1:8181     # the same thing, bound
```

A read-only Iceberg REST Catalog endpoint implementing the spec's config,
namespace and table-metadata routes; every Iceberg engine speaks it.

- **It serves one namespace.** `cold_tier.catalog_facade()` answers
  **404 `NoSuchNamespaceException`** for any other, the listing included.
  `CatalogFacade::new(catalog)` serves everything.
- **There is no write path.** An external writer would break the tiering
  invariant undetectably. Mutating routes answer **405** with that reason.
- **It carries no object-store credentials.** Engines use their own, so
  compromising the endpoint does not hand over the warehouse.
- **It returns a `Router`, not a bound port**, for your own authentication, TLS
  and tracing. [The CLI](@/docs/cli.md#serving) binds it bare, beside Flight SQL.

## Flight SQL, and when *not* to use it

| Consumer need | Right surface |
|---|---|
| Analytics over history | The Iceberg catalogue, direct read |
| A Rust application, in-process | The typed API |
| **Unified hot + cold from a non-Rust client** | **Flight SQL** |
| A BI tool over live data | Flight SQL JDBC/ODBC |

The hot tier is not in the catalogue, so **the unified view is the one thing
external clients cannot assemble themselves.** Routing analytics through Flight
instead of reading object storage directly serialises parallel reads through one
process.

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

The interop suite drives exactly that.

- **A store or a whole catalogue.** The server takes any `SqlSurface`: plan a
  statement without running it, and run it as a stream. A statement spanning
  several [tables](@/docs/querying.md#several-tables-in-one-session) is also
  something an external client cannot assemble itself.
- **Read-only.** A write here would bypass tier routing and the subject-reference
  check, so every mutating call answers `PermissionDenied` naming both.
- **Results carry their boundary over the wire.** The watermark, the tiers scanned
  and the read mode travel as **Arrow schema metadata** on every *query* response,
  so a BI tool that keeps the schema keeps the provenance: `meterstore.tiering_watermark`
  (the conservative minimum), `meterstore.watermarks` (`table=instant`,
  comma-separated, one per table the statement touched), `meterstore.tiers_scanned`
  and `meterstore.read_mode`. The catalogue RPCs (`GetCatalogs`, `GetDbSchemas`,
  `GetTables`, `GetTableTypes`) answer with exactly the schemas the specification
  fixes, which a conforming client requires.
- **Rows are streamed, never collected**, so the server's peak memory is not
  whatever a client asked for. The provenance goes out with the schema, before the
  first batch.
- **`GetSqlInfo`** carries the name, the version, the Arrow version and
  `FLIGHT_SQL_SERVER_READ_ONLY`, so a client need not offer an `INSERT`.
- **The statement handle *is* the SQL**: no server-side cache to evict or leak.
- **`GetFlightInfo` plans; `DoGet` executes**, so a bad statement fails before a
  stream starts and the rows are produced once.
- **`into_service` returns a tonic service, not a bound port.**

`information_schema` is enabled, so a BI tool can list the catalogue — and see
`readings` beside `readings_versions` — before it queries.
