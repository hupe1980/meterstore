+++
title = "Configuration"
description = "The builder is the API and TOML is a front end over the same validated types, so a file and a hand-built configuration pass through identical checks."
weight = 12
+++

The builder is the API. TOML is a **serde front end over the same validated
types**, so there is no second validation path to keep in step, and no setting
reachable from one and not the other.

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

# Only where a table declares a `subject_column`. Without this section such a
# deployment is refused at startup: the references would resolve to nothing.
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

An EIC may name its **object type**: `"EIC:X"` a party (a Bilanzkreis), `"EIC:Y"`
an area (a Bilanzierungsgebiet), and the rest of ENTSO-E's list. That is position
3 of the code and the only thing distinguishing the two, so declaring it is what
stops a Bilanzierungsgebiet from being stored as a Bilanzkreis — and unlike the
check character it is a regular expression, so the database enforces it too. A
letter this build does not list is refused at `meterstore check` rather than
degrading to a bare `"EIC"`.

## Which catalogue `[cold]` names

Three, and each is a cargo feature as well as a `catalog =` value.

| `catalog =` | Feature | `uri` | `warehouse` |
|---|---|---|---|
| `"rest"` *(default)* | `rest-catalog` *(default)* | Catalogue endpoint | Warehouse URI |
| `"sql"` | `sql-catalog` *(default)* | PostgreSQL URL, normally `[hot] url` | Warehouse URI |
| `"s3tables"` | `s3tables` | unused | Table bucket **ARN** |

S3 Tables owns the object layout, so it has no warehouse URI and no credentials to
forward — `warehouse` holds `arn:aws:s3tables:<region>:<account>:bucket/<name>`,
and anything that is not an ARN is refused at `meterstore check`. Naming a
catalogue whose feature was not compiled in is an error that says so.

Turn `sql-catalog` off in a workspace that also links an embedded SQLite: it
reaches `sqlx`'s optional SQLite driver, and `libsqlite3-sys` declares
`links = "sqlite3"`, which cargo enforces over the whole resolve graph whether or
not the feature is on.

```bash
cargo add meterstore --no-default-features --features rest-catalog
```

## `[privacy]`

Needed exactly when a table declares a `subject_column`, and inert otherwise. A
subject column names the column holding pseudonymous references, and a
deployment declaring one without a registry is **refused at startup** rather than
at the first erasure request: the references would resolve to nothing and an
Article 17 request would have no mapping to destroy.

`Settings::connect()` builds one registry for the whole deployment, because the
mapping is deployment-wide — two tables that register the same natural
identifier share a `SubjectRef`, and a single erasure unlinks both.

`erasure_secret` is optional and at least 32 bytes. It turns on the
**suppression list**, without which a replaying pipeline silently re-links a
subject whose mapping was deleted.
[Why it is optional →](@/docs/privacy.md#configuring-it)

`retired_erasure_secrets` holds the keys that no longer write tombstones and must
still recognise the ones they wrote — a tombstone cannot be re-keyed, so rotation
is additive and both entries are needed. Same 32-byte floor; retired keys without
an `erasure_secret` are refused.
[Rotating the key →](@/docs/privacy.md#rotating-the-key)

`meterstore check` validates the same file from a shell, connecting to nothing —
which is what makes it a CI step rather than a deployment-time surprise. See
[the CLI](@/docs/cli.md).

## Environment interpolation

A `${DATABASE_URL}`-style placeholder in any **value** is replaced from the
environment. A missing variable is an error rather than an empty string: a
connection URL that silently became `postgresql://@/` would fail somewhere far
from the typo.

Comments are left alone, so a file can document its own placeholders — which the
one `meterstore init` writes does. A `#` inside a quoted value is not a comment
either, because a password may contain one.

## `ddl_lock_timeout`

The only `[hot]` setting that is not about the pool. A DDL statement that cannot
get its lock within it gives up having changed nothing, and archival reports the
cycle as `deferred`; `"0s"` restores PostgreSQL's own behaviour, where the same
condition is an ingest outage. Raise it where the hot table carries long
transactions by design — every second added is a second the table can stall for.
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

They are never the same table, because `value` would mean two things in one
column and no aggregate could tell them apart. See
[Zählerstandsgänge](@/docs/writing.md).

The shape also decides the **merge key**: a point table identifies a reading by
its `melo_id` as well, because a register belongs to a meter and a Marktlokation
may be measured by several Messlokationen. `identify_by_melo` pins that either
way — see
[the storage model](@/docs/storage-model.md#the-messlokation-may-be-part-of-the-identity).

## `archival_step` is also the partition granularity

One setting, not two. The purge of an archived window is `DROP TABLE`, not
`DELETE`, and that only holds when a window is exactly one partition: a coarser
partition forces archival back to row-wise `DELETE` — millions of dead tuples a
day and the vacuum debt behind them — and a finer one multiplies partition count
for nothing.

It cannot be changed once a table has archived. The watermark sits on the old
grid, and a window off that grid names a partition relation nothing creates, so
`next_window` refuses rather than walking the boundary past rows PostgreSQL still
holds. Create a new table at the new step.

Below one minute it is refused at construction: a partition relation is named
`<table>_YYYY_MM_DD_HHMM`, so two consecutive sub-minute windows would name one.

## Settings that must agree

`declared_file_size` has no default and cannot be derived from one. It is the size
this table's cold data files actually come out at, which the archival window and
the portfolio decide; archive a window and read the figure off the run, which
reports it on every commit until you set it. Left unset, a maintenance tool
measures against Iceberg's 512 MiB default and rewrites files that are exactly the
size they should be.

`max_pin_age` must exceed the longest query the deployment runs. Nothing can
validate that — the store cannot know — so it is stated here. A plan registers the
tier boundary it was cut at and archival keeps what that plan is entitled to,
which is what makes a query safe against a boundary moving under it. Past
`max_pin_age` the floor advances anyway, because a query killed with its process
never deregisters and one such death must not hold a partition forever; a query
that outlives the cap fails naming the window it lost. The cost of a generous
value is disk, and only while a query is actually running.

`settlement_lag` must cover at least one `archival_step`. It is validated at
construction because getting it wrong degrades **silently**: a window can be
archived while it is still receiving corrections, and they land below the
watermark where no query looks.

`system.config` shows the two side by side, which is how a mismatch gets noticed.

## Six decisions the file format makes

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
implementable against the published `iceberg` crate. Both run out of band — see
[Operations](@/docs/operations.md#compaction). What the table does carry is the
policy those tools must honour, in Iceberg's own property names:
`write.target-file-size-bytes` from `declared_file_size`,
`history.expire.max-snapshot-age-ms` and `history.expire.min-snapshots-to-keep`
from the retention settings, and `write.metadata.delete-after-commit.enabled` with
`write.metadata.previous-versions-max` so superseded metadata does not accumulate
forever. A scheduled job runs at *its* defaults and never reads this page; the
table is the only place a rule reaches it.

## Defaults

| Setting | Default | Rationale |
|---|---|---|
| `archival_step` | 1 day | One window per commit, and one partition per window |
| `settlement_lag` | 7 days | Must exceed the market's correction window |
| `partition_headroom` | 14 days | Pre-created ahead of the write frontier |
| `reader_grace` | 1 hour | How long an archived partition stays readable after its cold commit, whatever anybody is reading. Hysteresis: it keeps reclamation from chasing every commit |
| `max_pin_age` | 6 hours | The cap on how long one query holds an archived partition. A query picks its tier split at *plan* time and reads the tiers at *execute* time, so a plan made before the boundary moved still needs rows PostgreSQL has just archived — it registers that boundary, and this bounds the registration |
| `scan_chunk_rows` | 50 000 | The bound on a scan's peak memory. **Rows, not measuring points** — meters differ by orders of magnitude in how much they report, so a fixed number of *them* is a variable amount of memory |
| `declared_file_size` | unset | What this table's Iceberg data files actually come out at, published as `write.target-file-size-bytes`. Unset, a compactor measures against Iceberg's 512 MiB default and rewrites files that are the size they should be — which destroys the statistics merge elision is proved from. No other setting decides it; archive a window and read it off the run |
| `snapshot_retention` | 10 years | Reproducibility is a compliance requirement, not a lakehouse default |
| `min_snapshots_to_keep` | 20 | So expiry can never leave the table unreadable |
