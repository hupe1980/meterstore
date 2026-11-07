+++
title = "Configuration"
description = "The builder is the API and TOML is a front end over the same validated types, so a file and a hand-built configuration pass through identical checks."
weight = 12
+++

The builder is the API. TOML is a **serde front end over the same validated
types**, so every setting is reachable from both and validated once.

```toml
[hot]
url = "${DATABASE_URL}"          # interpolated from the environment; a missing one is an error
max_connections = 16
ddl_lock_timeout = "3s"          # how long DDL waits for a lock before giving up

[cold]
catalog = "rest"                 # or "sql", or "s3tables"
uri = "https://catalog.internal" # endpoint (rest) or connection URL (sql)
warehouse = "s3://edm/meterstore"
namespace = "metering"
file_target_bytes = 536870912    # a cold-tier setting, not a table one
metadata_pool_max_connections = 4  # the sql catalogue's own pool
region = "eu-central-1"          # non-secret half of the S3 credentials
# endpoint = "https://minio.internal"   # S3-compatible stores

# Optional. The deployment gets its subject registry either way — when a table
# declares a `subject_column` or the database already holds the subject map;
# this section supplies the key that turns on suppression.
[privacy]
erasure_secret = "${METERSTORE_ERASURE_SECRET}"   # ≥ 32 bytes, turns on suppression
# Keys that no longer write tombstones and must still recognise them.
# retired_erasure_secrets = ["${METERSTORE_ERASURE_SECRET_2025}"]

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
  { name = "bilanzkreis",   check = "EIC:X" },               # an EIC, and a party code
  { name = "lieferant",     check = "BDEW" },                # a Marktpartner-ID, not 13 digits
  { name = "ingest_source", values = ["MSCONS", "SMGW"] },   # renders a CHECK
]

[tables.hot]
partition_headroom = "14d"       # pre-created ahead of the write frontier

[tables.archival]
settlement_lag = "7d"
archival_step = "1d"             # and the partition granularity — the same number
reader_grace = "1h"              # hysteresis: kept whatever anybody is reading
max_pin_age = "6h"               # above the longest query this deployment runs
# declared_file_size = 41943040  # what your cold files actually come out at
scan_chunk_rows = 50_000

[tables.maintenance]
snapshot_retention = "10y"       # regulatory reproducibility, not a typo
min_snapshots_to_keep = 20
```

```rust
let settings = Settings::from_path("meterstore.toml")?;
let config = settings.single_table()?;   // fully validated
```

`check` takes `"EIC"`, `"MALO"`, `"MELO"` or `"BDEW"` — the identifier schemes
`ValueCheck` knows. Each stops somewhere different, and
[checked columns](@/docs/storage-model.md#checked-columns) says where.

An EIC may name its **object type** — `"EIC:X"` a party (a Bilanzkreis),
`"EIC:Y"` an area (a Bilanzierungsgebiet), and the rest of ENTSO-E's list — which
the database enforces too. A letter this build does not list is refused at
`meterstore check`.

## Which catalogue `[cold]` names

Three, and each is a cargo feature as well as a `catalog =` value.

| `catalog =` | Feature | `uri` | `warehouse` |
|---|---|---|---|
| `"rest"` *(default)* | `rest-catalog` *(default)* | Catalogue endpoint | Warehouse URI |
| `"sql"` | `sql-catalog` *(default)* | PostgreSQL URL, normally `[hot] url` | Warehouse URI |
| `"s3tables"` | `s3tables` | unused | Table bucket **ARN** |

For S3 Tables, `warehouse` holds `arn:aws:s3tables:<region>:<account>:bucket/<name>`;
anything else is refused at `meterstore check`. Naming a catalogue whose feature
was not compiled in is an error that says so.

Turn `sql-catalog` off in a workspace that also links an embedded SQLite:
`libsqlite3-sys` declares `links = "sqlite3"`, which cargo enforces over the whole
resolve graph.

```bash
cargo add meterstore --no-default-features --features rest-catalog
```

## `[privacy]`

Optional; it supplies only the erasure keys. `Settings::connect()` builds one
subject registry for the whole deployment whenever a table declares a
`subject_column` or the database already holds `meterstore_subject_map` (a
deployment that stopped declaring one still owes the retention sweep). Two tables
registering the same natural identifier share a `SubjectRef`, and one erasure
unlinks both. A library caller building a `MeterStore` with a subject column and
no registry is refused at `build`.

`erasure_secret` is optional and at least 32 bytes. It turns on the
**suppression list**, without which a replaying pipeline silently re-links a
subject whose mapping was deleted.
[Why it is optional →](@/docs/privacy.md#configuring-it)

`retired_erasure_secrets` holds keys that no longer write tombstones but must
still recognise the ones they wrote — a tombstone cannot be re-keyed. Same
32-byte floor; retired keys without an `erasure_secret` are refused.
[Rotating the key →](@/docs/privacy.md#rotating-the-key)

`meterstore check` validates the same file connecting to nothing, so it runs in
CI. See [the CLI](@/docs/cli.md).

## Environment interpolation

A `${DATABASE_URL}`-style placeholder in any **value** is replaced from the
environment; a missing variable is an error, not an empty string. Comments are
left alone, and a `#` inside a quoted value is not a comment.

## Encrypted connections

Every PostgreSQL connection this crate opens from a URL — `[hot] url` and a SQL
catalogue's `[cold] uri` — is **verified TLS unless the URL says otherwise**:

| `sslmode` (in the URL, or `PGSSLMODE`) | What happens |
|---|---|
| *none* | `verify-full`: encrypted, and the server's certificate checked against its host name |
| `verify-full`, `verify-ca` | Honoured |
| `require` | Honoured: encrypted, **not** authenticated |
| `disable` | Honoured: plaintext, chosen explicitly |
| `prefer`, `allow` | **Refused** at validation — each falls back to plaintext without saying so |

A managed service's certificate chains to its provider's CA, which a host may not
trust: name it with `sslrootcert=/path/to/ca.pem`. A Unix-socket URL
(`?host=/var/run/postgresql`) never crosses a network and is left alone. The rule
holds for every connection the store makes, from a file or from code: the store
opens its own pool (`Database`), so there is no pool built elsewhere to escape it.

## `ddl_lock_timeout`

A DDL statement that cannot get its lock within it gives up having changed
nothing, and archival reports the cycle `deferred`; `"0s"` restores PostgreSQL's
own behaviour, where the same condition is an ingest outage. Every second added is
a second the table can stall for.
[Locks](@/docs/operations.md#locks-and-why-ddl-gives-up) has the argument.

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

They are never the same table ([Zählerstandsgänge](@/docs/writing.md)). The shape
also decides the **merge key**: a point table includes `melo_id`, because a
register belongs to a meter. `identify_by_melo` pins that either way — see
[the storage model](@/docs/storage-model.md#the-messlokation-may-be-part-of-the-identity).

## `archival_step` is also the partition granularity

One setting, not two: the purge is `DROP TABLE` only while a window is exactly
one partition.

It cannot be changed once a table has archived. Every archival commit records its
step, and archival refuses a run whose configured step differs, since changing it
in place would strand rows below the watermark. Create a new table at the new
step. Below one minute it is refused at construction: partitions are named
`<table>_YYYY_MM_DD_HHMM`.

## Settings that must agree

`declared_file_size` has no default: it is the size this table's cold files
actually come out at, which an archival run reports on every commit until you set
it. Unset, a maintenance tool measures against Iceberg's 512 MiB default and
rewrites files that are the size they should be.

`max_pin_age` must exceed the longest query the deployment runs, which nothing can
validate. A query that outlives it fails naming the window it lost; the cost of a
generous value is disk, only while a query runs.

`settlement_lag` must cover at least one `archival_step`, validated at
construction because the failure is **silent**: corrections for an archived window
land below the watermark. `system.config` shows the two side by side.

## Six decisions the file format makes

**It can open the tiers, and does not have to.** `connect()` builds both, over a
`Database` it opens from `[hot]`:

```rust
let deployment = Settings::from_path("meterstore.toml")?.connect().await?;
// deployment.hot, deployment.database, deployment.cold, deployment.registry,
// deployment.tables
```

An application assembling the tiers in code reads `settings.hot` and
`settings.cold` and wires them itself instead. Either way the store's connections
are its own: `[hot] max_connections` is this process's share of the server's
`max_connections`, counted beside the application's own pool.

`connect` stops short of a `MeterStore`, which needs the table to exist. The
`Deployment` finishes the job, creating the tables it touches: `store()` for a
file declaring exactly one table, `catalog()` for every declared table in one
[session](@/docs/querying.md#several-tables-in-one-session), and `table(config)`
for the builder of one. `open()` and `open_table(config)` do the same without DDL.

**`validate` checks the tables; `validate_all` checks the tiers too.** A
deployment bringing its own `Arc<dyn Catalog>` leaves `[cold]` empty. `connect`
runs the second.

**An unknown key is an error**, so a typo cannot leave a default in force.

**Durations carry units.** `settlement_lag = 7` is refused.

**`${VAR}` must resolve.**

**A connection URL is never printed in full.** `Debug` redacts it to its scheme.

## What is deliberately absent

**No `market_timezone`.** `metering` resolves `Europe/Berlin` internally.

**No `[serve]` section.** The catalogue façade and Flight SQL hand back a
*service* rather than binding a port, so the deployment supplies authentication
and TLS.

**No `[observability]` section.** Your application installs the OpenTelemetry
provider.

**No writer roll threshold on a *table*.** File rolling belongs to the cold tier:
`[cold] file_target_bytes`. `declared_file_size` is a different number — the size
this table's files come out at, published for maintenance tools.

**No object-store keys.** `[cold]` takes only `region` and `endpoint`; use the
platform credential chain (environment, instance role, IRSA). A deployment that
needs explicit keys builds the tier from `IcebergSqlCatalog` directly, where
`WarehouseAuth` has fields for them.

**No compaction or orphan-cleanup settings**: compaction is refused, and orphan
cleanup runs out of band ([Operations](@/docs/operations.md#compaction)). The
table carries the policy those tools must honour, in Iceberg's own property names:
`write.target-file-size-bytes` from `declared_file_size`,
`history.expire.max-snapshot-age-ms` and `history.expire.min-snapshots-to-keep`
from the retention settings, and `write.metadata.delete-after-commit.enabled` with
`write.metadata.previous-versions-max`.

## Defaults

| Setting | Default | Rationale |
|---|---|---|
| `archival_step` | 1 day | One window per commit, and one partition per window |
| `settlement_lag` | 7 days | Must exceed the market's correction window |
| `partition_headroom` | 14 days | Pre-created ahead of the write frontier |
| `reader_grace` | 1 hour | How long an archived partition stays readable after its cold commit, whatever anybody is reading. Hysteresis, not the safety argument |
| `max_pin_age` | 6 hours | The cap on how long one query's reader pin holds an archived partition |
| `scan_chunk_rows` | 50 000 | The bound on a scan's peak memory. **Rows, not measuring points** |
| `declared_file_size` | unset | Published as `write.target-file-size-bytes`; read it off an archival run |
| `snapshot_retention` | 10 years | Reproducibility is a compliance requirement |
| `min_snapshots_to_keep` | 20 | So expiry can never leave the table unreadable |
