+++
title = "Configuration"
description = "The builder is the API and TOML is a front end over the same validated types, so a file and a hand-built configuration pass through identical checks."
weight = 11
+++

The builder is the API. TOML is a **serde front end over the same validated
types**, so there is no second validation path to keep in step, and no setting
reachable from one and not the other.

```toml
[hot]
url = "${DATABASE_URL}"          # ${VAR} is interpolated; a missing one is an error
max_connections = 16

[cold]
catalog = "rest"                 # or "sql"
uri = "https://catalog.internal" # endpoint (rest) or connection URL (sql)
warehouse = "s3://edm/meterstore"
namespace = "metering"
file_target_bytes = 536870912    # a cold-tier setting, not a table one
metadata_pool_max_connections = 4  # the sql catalogue's own pool
region = "eu-central-1"          # non-secret half of the S3 credentials
# endpoint = "https://minio.internal"   # S3-compatible stores

[[tables]]
name = "readings_versions"
time_model = "interval"          # or "point" — a Zählerstandsgang
# identify_by_melo = true        # omitted, it follows time_model
# The column holding pseudonymous subject references. Registered as an
# attribute, never as identity.
subject_column = "subject_ref"
# `identity` is the load-bearing flag: an identity column joins the merge key,
# so two rows differing in it are different readings.
extra_columns = [
  { name = "tenant",        identity = true },
  { name = "bilanzkreis" },
  { name = "ingest_source", values = ["MSCONS", "SMGW"] },   # renders a CHECK
]

[tables.hot]
partition_step = "1d"            # must equal archival_step
partition_headroom = "14d"       # pre-created ahead of the write frontier

[tables.archival]
settlement_lag = "7d"
archival_step = "1d"
scan_chunk_rows = 50_000

[tables.maintenance]
snapshot_retention = "10y"       # regulatory reproducibility, not a typo
min_snapshots_to_keep = 20
```

```rust
let settings = Settings::from_path("meterstore.toml")?;
let config = settings.single_table()?;   // fully validated
```

## Interval or point

A table declares which shape it holds. `TimeModel::Interval` is the default — a
Lastgang, energy over `[from, to)`. `TimeModel::Point` is a Zählerstandsgang:
register values at instants, `to` null, and `value` a cumulative reading rather
than energy.

```rust
TableConfig::new("meter_reads_versions").time_model(TimeModel::Point)
```

```toml
time_model = "point"
```

They are never the same table, because `value` would mean two things in one
column and no aggregate could tell them apart. See
[Zählerstandsgänge](@/docs/writing.md).

The shape also decides the **merge key**: a point table identifies a reading by
its `melo_id` as well, because a register belongs to a meter and a Marktlokation
may be measured by several Messlokationen. `identify_by_melo` pins that either
way — see
[the storage model](@/docs/storage-model.md#the-messlokation-may-be-part-of-the-identity).

## Settings that must agree

Two pairs are validated against each other at construction, because getting them
wrong degrades **silently** rather than failing:

- `partition_step` **must equal** `archival_step`. A purge drops exactly one
  partition per archived window; a coarser partition forces archival back to
  row-wise `DELETE` and the vacuum debt that follows, and a finer one multiplies
  partition count for no benefit.
- `settlement_lag` must cover at least one `archival_step`, or a window can be
  archived while it is still receiving corrections — stranding them below the
  watermark.

`system.config` shows them side by side, which is how a mismatch gets noticed.

## Five decisions the file format makes

**It can open the tiers, and does not have to.** `connect()` builds both and hands
the pool back, because it is usually shared with the rest of the service and with
the subject registry:

```rust
let deployment = Settings::from_path("meterstore.toml")?.connect().await?;
// deployment.hot, deployment.pool, deployment.cold, deployment.tables
```

An application that owns its pool reads `settings.hot` and `settings.cold` and
wires them itself instead.

`connect` stops short of a `MeterStore`: one needs a cold *table provider*, which
needs the table to exist, and a deployment with several tables wants a
[catalogue](@/docs/querying.md#several-tables-in-one-session) rather than N
stores. Neither is a configuration file's decision.

**`validate` checks the tables; `validate_all` checks the tiers too.** A
deployment bringing its own `Arc<dyn Catalog>` legitimately leaves `[cold]` empty,
so insisting on it always would refuse a file that is complete for the way it is
used. `connect` runs the second.

**An unknown key is an error.** A typo in `settlment_lag` must not leave the
default silently in force.

**Durations carry units.** `settlement_lag = 7` is ambiguous, and guessing days or
seconds is wrong by five orders of magnitude either way.

**`${VAR}` must resolve.** Substituting an empty string turns a typo in a variable
name into a connection failure somewhere far from the mistake.

**A connection URL is never printed in full.** Configuration is exactly what a
service dumps into its startup log, so `Debug` redacts it to its scheme.

## What is deliberately absent

**No `market_timezone`.** `metering` resolves `Europe/Berlin` internally, and a
second configurable timezone here would be a way to disagree with it.

**No `[serve]` section.** Both the catalogue façade and Flight SQL hand back a
*service* rather than binding a port, precisely so the deployment supplies
authentication and TLS. A `bind = "0.0.0.0:8181"` line would imply the crate
listens on it, and it does not.

**No `[observability]` section.** Instruments are created against the
OpenTelemetry API and your application installs the provider, so there is nothing
here to set.

**No `target_file_size` on a *table*.** It belongs to the cold tier, so it is
`[cold] file_target_bytes` and not `[tables.archival]` — a setting that read back
from `system.config` and changed nothing would be worse than one that is absent.

**No object-store keys.** `[cold]` takes `region` and `endpoint`, which are not
secrets, and nothing else. The platform credential chain — environment, instance
role, IRSA — is the recommended path, and a secret in a configuration file is a
secret in a log. A deployment that genuinely needs explicit keys builds the tier
from `IcebergSqlCatalog` directly, where `WarehouseAuth` has fields for them.

**No compaction or orphan-cleanup settings**, for a different reason: neither is
implementable against the published `iceberg` crate. See
[Operations](@/docs/operations.md#maintenance-that-is-not-implemented).

## Defaults

| Setting | Default | Rationale |
|---|---|---|
| `partition_step` / `archival_step` | 1 day | One partition per commit |
| `settlement_lag` | 7 days | Must exceed the market's correction window |
| `partition_headroom` | 14 days | Pre-created ahead of the write frontier |
| `scan_chunk_rows` | 50 000 | The bound on a scan's peak memory. **Rows, not measuring points** — meters differ by orders of magnitude in how much they report, so a fixed number of *them* is a variable amount of memory |
| `snapshot_retention` | 10 years | Reproducibility is a compliance requirement, not a lakehouse default |
| `min_snapshots_to_keep` | 20 | So expiry can never leave the table unreadable |
