+++
title = "Getting started"
description = "Requirements, installation, and a store over both tiers in about thirty lines of Rust."
weight = 1
+++

## Requirements

| | Version | Why |
|---|---|---|
| Rust | 1.95 | Set by `iceberg` 0.11's floor, not by this crate's own syntax or by `metering`, which need 1.88 |
| PostgreSQL | **15 or later** | A support floor, not a syntax one — see below. The suite runs on 18, and whole again on 15 |
| `metering` | **0.25** | The domain layer. MeterStore stores its types; it does not redefine them |
| Apache Iceberg | format v2 | Deliberately not v3 — see [Architecture](@/docs/architecture.md#format-version) |

MeterStore needs no server configuration, no restart and no superuser — which is
what makes it deployable on RDS, Cloud SQL and Azure Postgres. It does need more
than `SELECT`:

- **`CREATE` on the schema**, for the relations `create` and `create_tables` make.
- **`CREATE` on the database**, because the integrity constraints — on by
  default — run `CREATE EXTENSION IF NOT EXISTS btree_gist`. It ships in contrib
  and is a *trusted* extension, which a non-superuser holding that privilege may
  install. `PostgresHot::integrity_constraints(false)` drops the need, and the
  constraints with it.
- **`INSERT` and `DELETE` on `meterstore_pins`** for every role that queries:
  each plan with a hot half registers the boundary it was cut at there. A
  read-only role or a hot standby therefore cannot query the unified view.

### Why 15, when the SQL runs on 12

**The design needs 12.** `CREATE TABLE … PARTITION OF` takes `ACCESS EXCLUSIVE`
on the parent, and PostgreSQL queues locks in arrival order, so on the write path
one long query would stall every insert behind it. MeterStore builds the partition
standalone and *attaches* it, which from 12 takes only `SHARE UPDATE EXCLUSIVE`
and conflicts with no read and no write.
[Locks](@/docs/operations.md#locks-and-why-ddl-gives-up) covers the detach, which
does need the strong lock.

**The support floor is 15**, the oldest release still receiving fixes into 2027
and still under standard support on RDS, Cloud SQL and Azure Database (12 and 13
are end-of-life; 14 follows in November 2026). CI runs the whole integration suite
on 15 and on 18. An older server will very likely work, but nothing demonstrates
it and no security fix is coming for it.

On 18, fast-path lock slots are sized from `max_locks_per_transaction` rather than
fixed at sixteen per backend, which raises the ceiling for a deployment's own
statements that reach many partitions through the parent. MeterStore's hot scan
reads one partition per statement and does not approach it.

## Install

```bash
cargo add meterstore
```

Features. The two catalogue features are on by default; everything else is off
unless you need it:

| Feature | What it adds |
|---|---|
| `rest-catalog` *(default)* | `IcebergRestCatalog` — the cold tier on a REST catalogue |
| `sql-catalog` *(default)* | `IcebergSqlCatalog` — the cold tier's metadata in the hot tier's own PostgreSQL |
| `object-store-s3` / `-gcs` / `-azure` / `-all` | Cloud object stores, and the only thing that pulls OpenDAL. `file://` and `memory://` are always available without it |
| `catalog-facade` | A read-only Iceberg REST endpoint, for deployments on the SQL catalog |
| `s3tables` | AWS S3 Tables as the cold-tier catalogue (implies `object-store-s3`) |
| `flight` | Arrow Flight SQL over the unified hot + cold view |
| `cli` | The `meterstore` command-line tool (implies `flight`, `catalog-facade` and `sql-catalog`) |
| `testkit` | The real-infrastructure harness, workload generator and correctness oracle |

Both catalogue features are on by default and both can be turned off. Dropping
`sql-catalog` matters in a workspace that also links an embedded SQLite: it is
what reaches `sqlx`'s optional SQLite driver, and `libsqlite3-sys` declares
`links = "sqlite3"`, which cargo enforces across the whole resolve graph.

```bash
cargo add meterstore --no-default-features --features rest-catalog
```

## The shortest path: no Rust at all

```bash
cargo install meterstore --features cli

meterstore init            # a commented starter configuration
meterstore check           # validate it — no database needed
meterstore create          # both tiers, every declared table
meterstore status          # boundary, lag, runway, health
```

That is a working deployment. [The CLI](@/docs/cli.md) covers the rest —
archival on a schedule, queries with their provenance, Flight SQL on a socket.

Ingest is the one thing it does not do, and deliberately: a reading arrives as an
MSCONS message, an SMGW push or a CSV a utility exports its own way, and mapping
one to a `MeasurementSeries` is an application's job rather than a flag's.

## A store over both tiers

The shortest path in Rust is the same [configuration file](@/docs/configuration.md),
which builds both tiers, every validated table, and the store or catalog over
them:

```rust
let deployment = Settings::from_path("meterstore.toml")?.connect().await?;

// One declared table — creates both tiers' relations on the way.
let store = deployment.store().await?;

// Or every declared table in one session, so a statement can mention two.
let catalog = deployment.catalog().await?;
```

`connect` stops at the tiers; `store`/`catalog` create the tables, because a cold
table provider cannot be opened over a table the catalogue does not hold yet.
`deployment.table(config).await?` returns the builder instead, to add what a file
cannot name — a subject registry, a read mode, a session you already own — and
`open_table` does the same without DDL, for a role that may only read.

The longer path is the same thing spelled out, for an application that builds
the tiers in code:

```rust
use meterstore::prelude::*;
use meterstore::hot::PostgresHot;
use std::sync::Arc;
use time::Duration;

// The store opens its own pool, TLS by default; no `sqlx` type is in the API.
// One `Database` serves the hot tier and the subject registry.
let database = Database::connect_with(&db_url, &DatabaseOptions::new().max_connections(16)).await?;
let hot = Arc::new(PostgresHot::new(database));

// The cold tier. MeterStore builds the whole Iceberg catalog stack, so your
// application depends on neither `iceberg-catalog-sql` nor the object-store
// backend directly; the backend follows from the warehouse URI scheme.
let cold_tier = IcebergSqlCatalog {
    database_url: &db_url,
    warehouse_uri: "s3://bucket/warehouse",   // or file:// memory:// gs:// abfss://
    catalog_name: "meterstore",
    namespace: "metering",
    file_target_bytes: 512 * 1024 * 1024,
    metadata_pool_max_connections: 4,
    auth: &WarehouseAuth { region: Some("eu-central-1".into()), ..Default::default() },
}.build().await?;
let cold = cold_tier.cold();

let store = MeterStore::builder()
    .hot(hot)
    .cold(cold.clone(), cold.table_provider("readings_versions").await?)
    .table(
        TableConfig::new("readings_versions")
            .settlement_lag(Duration::days(7))   // stay behind the correction window
            .archival_step(Duration::DAY)        // one window per commit, one partition per window
            .build()?,
    )
    .build()
    .await?;

// Creates both tiers from one configuration. This is deliberately a single
// entry point: the hot table's primary key, the cold schema and the resolution
// view all have to agree on what identifies a reading, and creating them
// separately is where they drift.
store.admin().create_tables().await?;
```

### Catalogues and warehouses

The cold tier is an Iceberg catalogue plus an object store. `IcebergSqlCatalog`
builds the common one for you — a `SqlCatalog` on the same PostgreSQL that backs
the hot tier — but `IcebergCold::new` takes **any** `Arc<dyn Catalog>`, so a
deployment can bring its own; the test suite exercises one MeterStore did not
build.

| Catalogue | Status |
|---|---|
| **SQL** (PostgreSQL-backed) | Built for you by `IcebergSqlCatalog`, behind the `sql-catalog` feature *(default)*. Serve the [façade](@/docs/interop.md) for external engines |
| **REST** (Polaris, Lakekeeper, Nessie, Gravitino) | Built for you by `IcebergRestCatalog`, behind the `rest-catalog` feature *(default)*. Engines point at the same endpoint, so no façade is needed |
| **AWS S3 Tables** | Built for you by `S3TablesCatalog`, behind the `s3tables` feature — see below |
| Glue, Hive, anything else | Any `Arc<dyn Catalog>` works |

| Warehouse scheme | Feature |
|---|---|
| `file://`, `memory://` | Always available |
| `s3://` (and S3-compatible: MinIO, R2) | `object-store-s3` |
| `gs://` | `object-store-gcs` |
| `abfss://` | `object-store-azure` |

A warehouse scheme whose backend was not compiled in is an error at construction.

#### AWS S3 Tables

```rust
use meterstore::cold::S3TablesCatalog;

let cold_tier = S3TablesCatalog {
    table_bucket_arn: "arn:aws:s3tables:eu-central-1:123456789012:bucket/edm",
    namespace: "metering",
    file_target_bytes: 512 * 1024 * 1024,
    endpoint_url: None,               // Some(..) for LocalStack
    region: Some("eu-central-1"),
}.build().await?;
```

Credentials come from the ambient AWS chain — environment, profile, instance
metadata, IRSA — so there is no credential field. The warehouse is the table
bucket, named by ARN because S3 Tables owns the object layout; everything
downstream is unchanged.

This goes through S3 Tables' native API, not its Iceberg REST endpoint: that
endpoint authenticates with SigV4, which `iceberg-catalog-rest` cannot sign, so
`IcebergRestCatalog` cannot reach it.

### Why the table is called `readings_versions`

The physical table holds **every version of every reading** — the audit trail
that makes corrections reproducible. Summing it directly double-counts every
corrected interval.

So the store registers two relations:

| Relation | Contents |
|---|---|
| `readings` | Version-resolved. One row per reading, the value currently in force. **Query this.** |
| `readings_versions` | Every version. The audit trail. |

The relation that looks like the obvious thing to query must not be the one that
returns wrong answers. [External engines](@/docs/interop.md#the-version-resolution-trap)
covers why this matters most outside Rust.

## Write and read

```rust
// Writes route each interval to the tier that owns it.
store.append(&[stored_series]).await?;

// Read across both tiers, with the boundary the answer was computed against.
let result = store.query("SELECT SUM(value) FROM readings WHERE …").await?;

// Or as the domain type, version-resolved and ordered.
let series = store.series("41373559241")?     // the check digit is verified here
    .obis("1-0:1.8.0")?
    .range(from, to)
    .collect()
    .await?;
```

Continue with [Architecture](@/docs/architecture.md), or jump to
[Writing readings](@/docs/writing.md) if you have data to land.

## Running the suite

Integration tests need a running Docker daemon; unit tests do not.

```bash
just dev     # format + unit tests, no Docker
just test    # the whole suite
just check   # everything CI runs
```
