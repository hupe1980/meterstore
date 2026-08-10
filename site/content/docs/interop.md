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

That is the whole point of storing regulated data in an open format, and it is
tested rather than asserted, in two engines:

- **DuckDB** — a different language, a different Parquet reader and its own C++
  reading of the Iceberg spec — reads both the Parquet and the table metadata and
  agrees with MeterStore about which files belong to the table, what the values
  are, and how many snapshots exist.
- **PyIceberg**, the Iceberg project's own implementation, reads the schema *with
  its field ids* (Iceberg resolves columns by id, so a writer that assigned them
  differently produces a table that opens and returns the wrong column), the
  partition spec, the sort order, the format version, and the tiering watermark
  out of the snapshot summary.

That second one matters beyond format portability: PyIceberg is the engine this
documentation tells you to use for the maintenance MeterStore cannot do itself,
so it needs to be demonstrated rather than assumed.

## The version-resolution trap {#the-version-resolution-trap}

**This is a correctness bug, not an ergonomic one.** Read the tail of this section
before pointing any engine at the warehouse.

An external engine reading the Iceberg table sees **raw versioned rows**, with no
knowledge of latest-version-wins. A naive `SELECT SUM(value)` **double-counts
every corrected interval** — silently, in a number someone will bill from.

Worse: compaction collapses superseded versions, so most partitions converge to
one version per key and the naive query becomes *incidentally* correct. It works
in testing and fails after a correction lands.

Three mitigations, in the order they are actually available:

**1. The raw table is named honestly.** It is `readings_versions`, not `readings`.
The name that looks like the obvious thing to query must not be the one returning
wrong answers.

**2. The resolution SQL is published.** One definition, two surfaces:

```rust
let sql = store.resolution_sql();
```

```sql
SELECT value FROM system.resolution WHERE setting = 'resolution_sql';
```

Paste it into a Trino, Spark or DuckDB session. The derived table is aliased, so
the same text also runs in PostgreSQL — which rejects an unaliased subquery, and
is a likely place for an operator to paste it.

```sql
SELECT malo_id, obis_code, "from", value, … FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY malo_id, obis_code, "from", version_scope
    ORDER BY version DESC
  ) AS _meterstore_rank
  FROM readings_versions
) AS _meterstore_resolved WHERE _meterstore_rank = 1
```

Note the `version_scope` in the partition: MSCONS assigns versions per network
operator per month, so versions from different scopes are not comparable and must
not be ranked against each other.

**3. An Iceberg View named `readings`** would make this automatic for view-aware
engines. It is **blocked**: `iceberg-rust` exposes `ViewCreation` and `ViewUpdate`
types but no `create_view` on the `Catalog` trait. Tracked for when upstream lands
it — until then, (1) is the primary defence and the naming is load-bearing.

The interop suites assert that the naive sum really *is* wrong before asserting
that the published rule fixes it — in DuckDB as SQL, and in PyIceberg over the
Arrow table it returns, which is what a maintenance script would actually do. A
mitigation for a hazard nobody has demonstrated is one nobody will bother to
apply.

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
