+++
title = "Getting started"
description = "Requirements, installation, and a store over both tiers in about thirty lines of Rust."
weight = 1
+++

## Requirements

| | Version | Why |
|---|---|---|
| Rust | 1.94 | Set by the dependency floor (`metering`, `iceberg`), not by this crate's own syntax |
| PostgreSQL | **14 or later** | Declarative range partitioning, so purging an archived window is `DETACH` + `DROP TABLE` rather than a row-wise `DELETE`. The test suite pins 16 |
| `metering` | 0.17 or later | The domain layer. MeterStore stores its types; it does not redefine them |
| Apache Iceberg | format v2 | Deliberately not v3 — see [Architecture](@/docs/architecture.md#format-version) |

MeterStore needs only `SELECT` plus ownership of its own tables. No server
configuration, no restart, no `CREATE EXTENSION` for the core path — which is what
makes it deployable on RDS, Cloud SQL and Azure Postgres, where an
extension-based approach is not.

## Install

```bash
cargo add meterstore
```

Optional features, all off unless you need them:

| Feature | What it adds |
|---|---|
| `rest-catalog` *(default)* | Iceberg REST catalog client |
| `object-store-s3` / `-gcs` / `-azure` | Cloud object stores. `file://` and `memory://` are always available |
| `catalog-facade` | A read-only Iceberg REST endpoint, for deployments on the SQL catalog |
| `s3tables` | AWS S3 Tables as the cold-tier catalogue (implies `object-store-s3`) |
| `flight` | Arrow Flight SQL over the unified hot + cold view |
| `testkit` | The real-infrastructure harness, workload generator and correctness oracle |

## A store over both tiers

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
            .archival_step(Duration::DAY)        // one partition per commit
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
| **SQL** (PostgreSQL-backed) | Built for you by `IcebergSqlCatalog`. Serve the [façade](@/docs/interop.md) for external engines |
| **REST** (Polaris, Lakekeeper, Nessie, Gravitino) | Construct with `iceberg-catalog-rest` and pass to `IcebergCold::new`. Engines point at the same endpoint |
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

**One thing worth knowing**, because searching for it finds the wrong answer:
S3 Tables has two front doors. Its *Iceberg REST endpoint* authenticates with
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
let series = store.series("12345678901")
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
