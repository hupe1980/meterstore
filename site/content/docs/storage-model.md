+++
title = "Storage model"
description = "The physical columns, the merge key that decides which reading supersedes which, identity versus attribute columns, and the constraints that stop a write producing a wrong number."
weight = 3
+++

## Physical columns

The Arrow and Iceberg encoding of a `MeasurementSeries` plus the three things
storage adds. Column names match `metering`'s field names deliberately, so the
mapping needs no lookup table.

| Column | Arrow | Iceberg | Source |
|---|---|---|---|
| `malo_id` | `Utf8` | `string` | 11-digit Marktlokation, check digit verified |
| `melo_id` | `Utf8`, nullable | `string` | 33-char Messlokation |
| `obis_code` | `Utf8` | `string` | Canonical form only |
| `sparte` | `Utf8` | `string` | `STROM`, `GAS`, `WAERME`, `WASSER` |
| `from` | `Timestamp(µs, UTC)` | `timestamptz` | Interval start, inclusive |
| `to` | `Timestamp(µs, UTC)` | `timestamptz` | Interval end — **stored, never derived** |
| `value` | `Decimal128(18,6)` | `decimal(18,6)` | The quantity |
| `unit` | `Utf8` | `string` | `KWH` or `M3` |
| `quality` | `Utf8` | `string` | `MEASURED`, `SUBSTITUTED`, … |
| `resolution` | `Utf8`, nullable | `string` | ISO 8601, e.g. `PT15M` |
| `source_kind` | `Utf8` | `string` | Filterable discriminant |
| `source_detail` | `Utf8`, nullable | `string` | JSON variant payload |
| `provenance` | `Utf8`, nullable | `string` | JSON audit trail |
| `version` | `Decimal128(20,0)` | `decimal(20,0)` | MSCONS correction version |
| `version_scope` | `Utf8` | `string` | `<operator>:<YYYY-MM>` |
| `recorded_at` | `Timestamp(µs, UTC)` | `timestamptz` | Transaction time |
| `balancing_day` | `Date32` | `date` | The local day this reading is booked on |

**Merge key:** `(malo_id, obis_code, from)` · **winner:** `max(version)` within
the same `version_scope`.

### `balancing_day` is derived, and stored anyway

The one place the crate persists something it can compute. The rule — Berlin
calendar day for electricity, heat and water, the **Gastag** (06:00 to 06:00
local) for gas — needs a zone conversion and a wall-clock six-hour shift, and SQL
dialects differ on that, so no single published expression is right everywhere.
Reading the Iceberg files directly is the intended access path, so the rule is
applied once at write time instead:

```sql
SELECT balancing_day, SUM(value) FROM readings GROUP BY 1;
```

There is exactly one writer, so it cannot drift: the encoder derives it from
`metering`'s calendar and nothing else sets it, asserted against that calendar
over every commodity across a DST weekend. A day's rows share one value, so it
dictionary-encodes to nearly nothing. The column is `NOT NULL` with no default,
so a hand-written `INSERT` that omits it fails at the write rather than storing a
wrong day.

### `to` is stored, not computed

Deriving `end = start + 15 min` is wrong across a DST transition, and
`MeterInterval` makes it impossible by carrying both bounds. A 100-interval
autumn day survives archival with its irregular boundaries intact.

### The quantity column carries no unit in its name

`Sparte::billing_unit(Wasser)` is **m³** — water is metered *and* settled in cubic
metres, and has no calorific value, so the gas conversion never applies to it. Gas
legitimately appears on both sides of the Brennwert conversion.

So `value`, `sparte` and `unit` are three core, non-nullable columns. No stored
number is dimensionless, and a mixed portfolio groups by the dimension it is
summing:

```sql
SELECT sparte, unit, SUM(value) FROM readings GROUP BY 1, 2;
```

A unit the commodity cannot be expressed in — water in kWh — is **refused at the
write**, with a message naming the unit that would have been right. The rule is
`metering`'s (`Sparte::measured_unit`, `Sparte::billing_unit`); MeterStore only
enforces it, on both the write and the read path.

Neither `sparte` nor `unit` joins the merge key: a Marktlokation belongs to one
commodity, so the Sparte is functionally determined by `malo_id`. In the key, a
correction that spelled it differently would silently fail to supersede rather
than fail loudly.

`sparte` earns its keep a second time on the read path. It is what tells the
store **which calendar a row is balanced on** — gas runs on the 06:00 Gastag and
everything else on the Berlin calendar day — so `meter_balancing_day` and
completeness can be right for a mixed table without the caller saying which is
which. See [the gas-day trap](@/docs/interop.md#the-gas-day-trap).

### The identifiers are parsed, not carried

`malo_id` is a `Utf8` column because Parquet, Arrow and PostgreSQL have no
eleven-digit-with-check-digit type — but the *value* is `metering`'s `MaloId`, on
both sides of the encoding. A MaLo-ID carries a check digit precisely so that a
transposed digit is detectable, and a store that read the column back without
checking would discard that protection at the last moment it could still be used:
past the decoder, a wrong-but-plausible identifier is simply a different
measuring point, with no error anywhere.

So decoding parses, and a failure is an error rather than a filtered row —
silently dropping readings whose key looks wrong would understate a settlement,
which is the failure direction this crate refuses everywhere else. `melo_id` is
checked structurally; the Zählpunktbezeichnung has no check digit to verify.

### Stored values are `metering`'s own strings

Every enum reaches storage as the string the domain defines for it —
`SUBSTITUTED`, not an opaque `2` whose meaning lives in this crate's source. Two
consequences: the data is self-describing to an external engine, and a new
upstream variant is no longer a decision, because the upstream name *is* the
code. Parquet dictionary-encodes a column of eight distinct values to almost
nothing, so the size cost is nil.

## Correction versioning

The MSCONS application handbook is explicit: a correction is performed by
**versioning the value**, not replacing it. The label is numeric, at least 14
digits, monotonically ascending, assigned by the network operator per month.

- **Nothing is ever deleted or updated in place.** No tombstones, no equality
  deletes, no deletion vectors. `iceberg-rust` has no row-level mutation path at
  all — a copy-on-write design would be blocked outright — and MeterStore needs
  only `append`.
- **The domain supplies its own sequence number.** No synthetic LSN. `version` is
  meaningful to auditors and stable across restarts and re-ingestion.
- **Version scope is `(network operator, month)`, and the month is the
  *interval's*.** Comparing versions across operators or months is meaningless,
  so resolution partitions by scope.

That last point is load-bearing. A scope keyed to the **delivery** month would
give a July reading corrected in August two different scopes, so neither could
supersede the other, both rows would survive resolution, and every sum over them
would be inflated — with no error anywhere. `VersionScope::for_interval` derives
it from the interval's *local* month, and encoding rejects a scope that does not
cover its intervals.

## Identity versus attribute columns

Deployments carry columns the core schema does not — a tenant discriminator,
Bilanzkreis, grid area. Declaring one forces a choice, because the two kinds
behave differently:

```rust
TableConfig::new("readings_versions")
    .identity_column(Field::new("tenant", DataType::Utf8, false))   // joins the merge key
    .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
```

**Identity columns join the merge key.** Two rows differing in one are different
readings, and neither can supersede the other. A tenant discriminator belongs
here: declared as an attribute instead, two tenants reporting the same measuring
point would share a merge key, and one tenant's correction would supersede the
other's reading — a cross-tenant leak with no error anywhere.

**Attribute columns carry data.** A correction may change them freely.

Identity columns must be non-nullable: a null cannot identify a reading, and in
SQL it does not compare equal to itself, so two such rows would never resolve
against each other.

The choice propagates to four places that must agree — the storage schema, the
hot table's primary key, the `ON CONFLICT` target that makes redelivery
idempotent, and the resolution view's `PARTITION BY`. `create_tables` is the
single entry point for exactly that reason.

Extra columns are `Utf8` only, and a declared name must be a plain identifier:
names are written into DDL and SQL as identifiers, which cannot be parameterised,
so the alphabet is restricted once rather than quoted carefully in five places.

### Coded columns

An extra column whose values are a fixed vocabulary — an ingestion source, a
delivery status — can declare it:

```rust
.attribute_column(coded_column("ingest_source", &["MSCONS", "SMGW"], true))
```

```toml
extra_columns = [{ name = "ingest_source", values = ["MSCONS", "SMGW"] }]
```

That renders a `CHECK … IN (…)` on the hot table, exactly like the built-in
`sparte`/`unit`/`quality` columns, so a value outside the set fails the write
rather than being read back later as an unknown code. MeterStore stays
domain-agnostic: it enforces whatever set you supply, carried in the field's
Arrow metadata so it disturbs neither the type nor schema evolution.

## A tenant is not a market participant

Two words that are easy to conflate, with a financial consequence:

| | Answers | Where it lives |
|---|---|---|
| **Tenant** | *Whose installation this is.* An account, a customer of a service bureau, an isolation boundary. Opaque to MeterStore. | A deployment-declared identity column |
| **Network operator** | *Who assigned this version.* A BDEW Codenummer, and half of the scope a version is comparable within. | `version_scope` |

Neither determines the other. One tenant routinely holds data from many network
operators — a supplier takes deliveries from every grid operator it has customers
behind — and one market participant may appear across several tenants in a bureau
installation.

So the tenant joins the merge key and the operator must not: the operator is not
part of what *identifies* a reading, it is part of what makes two versions of that
reading comparable. In the merge key it would turn a corrected reading into two
different readings.

## The constraints that stop a wrong number

The merge key plus `version` is a primary key, and it is not the whole guard: two
rows for one channel at one version must differ in `from`, and two ranges that
differ in `from` can still *overlap*. An hourly delivery followed by a
quarter-hourly one leaves both stored, and every aggregate over them
double-counts.

Each hot partition therefore carries two `EXCLUDE USING gist` constraints:

1. **No overlapping intervals within one version.** Scoped to a version, so a
   correction — a higher version covering the same span — stays legal. The
   equality columns are read from the parent's actual primary key, so a
   tenant-extended key produces a tenant-*scoped* exclusion rather than one that
   rejects another tenant's reading.
2. **One network operator per reading.** Two operators for one reading give two
   incomparable scopes, resolution picks a winner in each, and **both** survive
   into the resolved view. The realistic cause is not a grid-operator change —
   those deliver for different intervals — but a caller passing a forwarding
   party's MP-ID where the network operator belongs.

Iceberg cannot carry a constraint, and `append` routes a below-watermark interval
straight there, so the second rule is *also* checked by `append` before the cold
write — scoped to the intervals being written, so the cost is proportional to the
correction rather than to the history.

Both need `btree_gist`, which ships in contrib and is created on demand.
`PostgresHot::integrity_constraints(false)` turns them off; do that with a
measurement in hand, since it trades an overlap being *refused* for an overlap
being reported after the fact by [completeness](@/docs/completeness.md).

## Cold-tier layout

```text
warehouse/<namespace>/readings_versions/
  metadata/
  data/tenant=9900000000001/from_month=2026-03/data-<random>-0.parquet
```

| Choice | Value | Why |
|---|---|---|
| Partition spec | `identity(<identity columns>)`, then `month(from)` | An identity column is by definition something every query filters on, so leading with it eliminates other tenants' files at the *manifest*, before a footer is opened |
| Sort order | `(malo_id, from)` | "One meter, one year" becomes a contiguous scan of a few row groups. Declared in the Parquet footer, so a reader may rely on it |
| Row group | 256k rows | Bounds the per-writer buffer, which the fanout writer multiplies by open partitions — and sharpens row-group `malo_id` statistics |
| Bloom filters | `malo_id`, `obis_code` | Sized per column: the meter population for one, a code list for the other |
| Compression | ZSTD(3), page-level statistics | Best ratio/CPU balance for already well-encoded columns |

`month`, not `day`, because archival runs one window per day: a day transform
would put each window in its own partition — ~3 650 per decade per tenant, which
is how a lakehouse acquires a small-file problem.

No `bucket(malo_id)`: rows are already sorted by `(malo_id, from)` and carry a
bloom filter on `malo_id`, which is what actually answers the single-meter read.
Hashing into buckets would scatter that sort order across files and add nothing.

Iceberg's **hidden partitioning** means queries never mention `tenant=` or
`from_month=` — pruning follows from ordinary predicates.
