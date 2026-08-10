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
uri = "https://catalog.internal"
warehouse = "s3://edm/meterstore"
namespace = "metering"

[[tables]]
name = "readings_versions"
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

**It names the tiers; it does not open them.** `hot.url` becomes a value your
application uses to build its own `PgPool`, because that pool is usually shared
with the rest of the service and MeterStore never owns a connection. Same for the
catalogue.

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
| `target_file_size` | 512 MiB | Standard Iceberg guidance |
| `snapshot_retention` | 10 years | Reproducibility is a compliance requirement, not a lakehouse default |
| `min_snapshots_to_keep` | 20 | So expiry can never leave the table unreadable |
