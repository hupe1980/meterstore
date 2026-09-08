+++
title = "Getting started"
description = "Requirements, installation, and a store over both tiers in about thirty lines of Rust."
weight = 1
+++

## Requirements

| | Version | Why |
|---|---|---|
| Rust | 1.94 | Set by the dependency floor (`metering`, `iceberg`), not by this crate's own syntax |
| PostgreSQL | **12 or later** | See below. The test suite pins 16 |
| `metering` | **0.23 or later** | The domain layer. MeterStore stores its types; it does not redefine them |
| Apache Iceberg | format v2 | Deliberately not v3 — see [Architecture](@/docs/architecture.md#format-version) |

MeterStore needs only `SELECT` plus ownership of its own tables. No server
configuration, no restart, no `CREATE EXTENSION` for the core path — which is what
makes it deployable on RDS, Cloud SQL and Azure Postgres, where an
extension-based approach is not.

### Why 12 and not 10

Range partitioning arrived in 10 and a partitioned primary key in 11; neither is
the floor. **12 is, because of a lock.** `CREATE TABLE … PARTITION OF` takes
`ACCESS EXCLUSIVE` on the parent, and PostgreSQL grants locks in arrival order —
so a statement waiting for it blocks every reader and writer behind it. Partition
creation runs on the **write path**, so that spelling lets one long query stall
every subsequent insert. MeterStore builds the relation standalone and *attaches*
it, which from 12 takes only `SHARE UPDATE EXCLUSIVE` — a lock that conflicts with
no read and no write.

Older servers still work; they just do not have the property documented here.
[Locks](@/docs/operations.md#locks-and-why-ddl-gives-up) covers the other half —
the detach that does need the strong lock.

## Install

```bash
cargo add meterstore
```

Optional features, all off unless you need them:

| Feature | What it adds |
|---|---|
| `rest-catalog` *(default)* | `IcebergRestCatalog` — the cold tier on a REST catalogue |
| `sql-catalog` *(default)* | `IcebergSqlCatalog` — the cold tier's metadata in the hot tier's own PostgreSQL |
| `object-store-s3` / `-gcs` / `-azure` / `-all` | Cloud object stores. `file://` and `memory://` are always available |
| `catalog-facade` | A read-only Iceberg REST endpoint, for deployments on the SQL catalog |
| `s3tables` | AWS S3 Tables as the cold-tier catalogue (implies `object-store-s3`) |
| `flight` | Arrow Flight SQL over the unified hot + cold view |
| `cli` | The `meterstore` command-line tool (implies `flight` and `catalog-facade`) |
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

`connect` stops at the tiers and `store`/`catalog` go the last step, because that
step is not field access: a cold table provider cannot be opened over a table the
catalogue does not hold yet. `deployment.table(config)` returns the builder if you
want to add what a file cannot name — a subject registry, a read mode, a session
you already own.

The longer path is the same thing spelled out, and it is what an application that
owns its own pool writes:

```rust
use meterstore::prelude::*;
use meterstore::hot::PostgresHot;
use std::sync::Arc;
use time::Duration;

// The hot tier wraps a pool you already own — MeterStore never opens a
// connection for you and never closes one.
let hot = Arc::new(PostgresHot::new(pool));

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
store.create_tables().await?;
```

### Catalogues and warehouses

The cold tier is an Iceberg catalogue plus an object store. `IcebergSqlCatalog`
builds the common one for you — a `SqlCatalog` on the same PostgreSQL that backs
the hot tier — but `IcebergCold::new` takes **any** `Arc<dyn Catalog>`, so a
deployment can bring its own. That extension point is exercised end to end in the
test suite against a catalogue MeterStore did not build, not merely asserted.

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

A warehouse whose scheme needs a backend that was not compiled in is a clear
error at construction, not a silent fallback.

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
metadata, IRSA — so there is deliberately no credential field: a store that
accepted an access key would be a second place for one to live.

The warehouse is the table bucket itself, identified by ARN rather than by URI,
because S3 Tables owns the object layout. Everything downstream — archival,
watermark, resolution, queries — is unchanged; a catalogue is a catalogue.

S3 Tables has two front doors, and searching for it finds the wrong one. Its *Iceberg REST endpoint* authenticates with
SigV4, and `iceberg-catalog-rest` cannot sign — `with_client` takes a concrete
`reqwest::Client`, which has no per-request interceptor, and a static
`Authorization` header cannot carry a per-request signature. Its *native API*
goes through the AWS SDK, which signs for itself. This uses the second. The REST
route being closed says nothing about the target.

### Why the table is called `readings_versions`

The physical table holds **every version of every reading** — the audit trail
that makes corrections reproducible. Summing it directly double-counts every
corrected interval.

So the store registers two relations:

| Relation | Contents |
|---|---|
| `readings` | Version-resolved. One row per reading, the value currently in force. **Query this.** |
| `readings_versions` | Every version. The audit trail. |

The name is load-bearing rather than cosmetic: the relation that looks like the
obvious thing to query must not be the one that returns wrong answers. See
[External engines](@/docs/interop.md#the-version-resolution-trap) for why this
matters most outside Rust.

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
