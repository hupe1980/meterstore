+++
title = "Storage model"
description = "The physical columns, the merge key that decides which reading supersedes which, identity versus attribute columns, and the constraints that stop a write producing a wrong number."
weight = 3
+++

## Physical columns

The Arrow and Iceberg encoding of a `MeasurementSeries` (`meterstore::delivery`)
plus the three things storage adds. Column names match `metering`'s field names.

| Column | Arrow | Iceberg | Source |
|---|---|---|---|
| `malo_id` | `Utf8` | `string` | 11-digit Marktlokation, check digit verified |
| `melo_id` | `Utf8`, nullable | `string` | 33-char Messlokation — **`NOT NULL` and part of the key** where the table [identifies a reading by it](#the-messlokation-may-be-part-of-the-identity) |
| `obis_code` | `Utf8` | `string` | Canonical form only |
| `sparte` | `Utf8` | `string` | `STROM`, `GAS`, `WAERME`, `WASSER` |
| `from` | `Timestamp(µs, UTC)` | `timestamptz` | Interval start, inclusive |
| `to` | `Timestamp(µs, UTC)` | `timestamptz` | Interval end — **stored, never derived** |
| `value` | `Decimal128(18,6)` | `decimal(18,6)` | The quantity |
| `unit` | `Utf8` | `string` | `KWH` or `M3` |
| `quality` | `Utf8` | `string` | `MEASURED`, `SUBSTITUTED`, … |
| `resolution` | `Utf8`, nullable | `string` | ISO 8601, e.g. `PT15M` |
| `source_kind` | `Utf8` | `string` | Filterable discriminant — the payload's own tag (`MSCONS`, …) |
| `source_detail` | `Utf8`, nullable | `string` | JSON variant payload |
| `provenance` | `Utf8`, nullable | `string` | JSON audit trail, [RFC 3339 timestamps](#json-columns) |
| `version` | `Decimal128(20,0)` | `decimal(20,0)` | MSCONS correction version |
| `version_scope` | `Utf8` | `string` | `<Marktpartner-ID>:<YYYY-MM>` — twenty-one characters, the **Bilanzierungsmonat**, cut at 06:00 for gas |
| `recorded_at` | `Timestamp(µs, UTC)` | `timestamptz` | Transaction time |
| `balancing_day` | `Date32` | `date` | The local day this reading is booked on |

`to` is the one column whose meaning depends on the table's **time model**: the
span's end on an interval table, null on a point one.

**Merge key:** `(malo_id, obis_code, from)` — plus `melo_id` on a table that
[identifies a reading by its Messlokation](#the-messlokation-may-be-part-of-the-identity),
plus any [identity columns](#identity-versus-attribute-columns) · **winner:**
`max(version)` within the same `version_scope`.

### `balancing_day` is derived, and stored anyway

The rule — Berlin calendar day for electricity, heat and water, the **Gastag**
(06:00 to 06:00 local) for gas — needs a zone conversion and a wall-clock six-hour
shift that SQL dialects spell differently, so it is applied once at write time for
engines reading the Iceberg files directly:

```sql
SELECT balancing_day, SUM(value) FROM readings GROUP BY 1;
```

Only the encoder sets it, from `metering`'s calendar. The column is `NOT NULL`
with no default, so a hand-written `INSERT` that omits it fails rather than
storing a wrong day.

### `source_kind` is the payload's own tag

`MeasurementSource` (in `meterstore::delivery`, with a frozen
`SCREAMING_SNAKE_CASE` spelling) is a data-carrying enum, so it takes two columns:
a discriminant that filters cheaply and a JSON payload that round-trips the
variant. The discriminant is **read off the serialised payload**, so the two
columns cannot disagree; renaming a variant moves both together, which is a
stored-data break the [stored-form corpus](#json-columns) refuses.

### Two JSON columns {#json-columns}

`source_detail` and `provenance` are the only columns holding serialised structure
rather than a scalar. Both are the `serde` output of `meterstore::delivery` — a
delivery's origin and its audit trail are process state, which `metering` does
not model — and every derive there states its spelling.

`source_detail` carries **nested** vocabularies — `AutoSubstitute` holds
`metering`'s `Method` and `SubstitutionReason`, `VirtualMeter` a
`VirtualMeterKind` — so retagging one of those leaves `source_kind` correct and
only the payload stops decoding.

`provenance` carries RFC 3339 timestamps, which is what lets an external engine
cast one straight out of the JSON:

```json
[{"occurred_at":"2026-03-01T00:00:00Z","event_type":"INGESTED","actor":"MSCONS","note":null}]
```

`delivery` formats that instant itself, because `time`'s serde shape depends on
its Cargo features, and the on-disk shape must not.

> A change to a serialised type is a **stored-data** break: rows already written
> stop deserialising. What a build writes is asserted byte for byte, and
> `tests/stored-form/` holds the rows earlier releases wrote; a build must decode
> them unless the file names the release that **broke** them, so a break fails the
> test suite rather than the next reader of the warehouse.
>
> One break is recorded. Rows written by 0.12.0–0.14.0 in three shapes raise
> `Error::Decode` under 0.15.0 and later: every `AUTO_SUBSTITUTE` payload (its
> `method` is `metering` 0.25's procedure codes), every resolution spelled in
> seconds (`PT60S` rather than `PT1M`), and a Marktpartner-ID failing both
> check-digit procedures. The rows stay on disk and in the lake, readable by a
> 0.14 build and by any engine reading the columns as text.

### Codes are stored canonically, and only canonically

`sparte`, `unit`, `quality` and `resolution` hold exactly the string the domain
writes. `metering`'s `FromStr` is lenient — it trims, ignores case and accepts
aliases such as `WÄRME` — which is right at ingest and wrong for `GROUP BY` keys,
where two spellings are two rows. The hot tier's `CHECK … IN (…)` is rendered
from the canonical `CODES`; **Iceberg has no constraints**, so decoding enforces
the same rule and a non-canonical spelling is a decode error naming the canonical
one. A grid is whole minutes dividing the hour, an hour, a day, a month or a
year: `PT15M`, never `PT900S`; `PT1M`, never `PT60S`.

### `to` is null on a point table

A **point** row — a Zählerstand — carries null, because an instant has no end,
and its `value` is the register's cumulative reading; the two shapes are never one
table ([Zählerstandsgänge](@/docs/writing.md)). An interval
table keeps `to` `NOT NULL` in its DDL, and the encoder refuses a null.

### `to` is stored, not computed

Deriving `end = start + 15 min` is wrong across a DST transition, and
`MeterInterval` makes it impossible by carrying both bounds. A 100-interval
autumn day survives archival with its irregular boundaries intact.

### The quantity column carries no unit in its name

Water is metered and settled in m³, and gas appears on both sides of the
Brennwert conversion, so `value`, `sparte` and `unit` are three core,
non-nullable columns and a mixed portfolio groups by the dimension it sums:

```sql
SELECT sparte, unit, SUM(value) FROM readings GROUP BY 1, 2;
```

A unit the commodity cannot be expressed in — water in kWh — is **refused at the
write**, naming the right one. The rule is `metering`'s (`Sparte::measured_unit`,
`Sparte::billing_unit`), enforced on write and read.

Neither joins the merge key: a Marktlokation belongs to one commodity, and in the
key a correction spelling it differently would silently fail to supersede.
`sparte` also tells the read path **which calendar a row is balanced on**, so
`meter_balancing_day` and completeness are right for a mixed table. See
[the gas-day trap](@/docs/interop.md#the-gas-day-trap).

### The identifiers are parsed, not carried

`malo_id` is a `Utf8` column, but the *value* is `metering`'s `MaloId` on both
sides of the encoding: past the decoder, a transposed digit is simply a different
measuring point. So decoding parses, and a failure is an error rather than a
filtered row, which would understate a settlement. `melo_id` is checked
structurally; the Zählpunktbezeichnung has no check digit.

### Stored values are their vocabulary's own strings

Every enum is stored as its vocabulary's own string — `metering`'s code
(`SUBSTITUTED`) or `meterstore::delivery`'s frozen spelling (`MSCONS`) — never an
opaque integer. The data is self-describing to an external engine, and Parquet
dictionary encoding makes the size cost nil.

## Correction versioning

In MSCONS a correction **versions the value** rather than replacing it
`[MSCONS AHB 3.2 Kap. 5.1]`. The label
is numeric, at least 14 digits, ascending, assigned by the network operator per
month.

- **Nothing is deleted or updated in place.** No tombstones, no equality deletes,
  no deletion vectors; MeterStore needs only Iceberg's `append`.
- **The domain supplies the sequence number.** `version` is meaningful to auditors
  and stable across re-ingestion.
- **Version scope is `(network operator, month)`, and the month is the
  *interval's*.** Resolution partitions by scope. A scope keyed to the
  **delivery** month would give a July reading corrected in August two scopes,
  both rows would survive resolution, and every sum would be silently inflated.
  `VersionScope::for_interval` derives it from the interval, and encoding rejects
  a scope that does not cover its intervals.

### The month is the Bilanzierungsmonat, and gas cuts it at 06:00

It is a *local* month: an interval starting `2026-07-31T23:00Z` is already August
in Berlin. For gas it is not the calendar month at all. EDI@Energy *Allgemeine Festlegungen*
v6.1c, Kap. 3.1 spells out the Bilanzierungsmonat Juni 2021 as 01.06 00:00 to
01.07 00:00 for Strom and 01.06 **06:00** to 01.07 **06:00** for Gas — the Gastag
boundary carries all the way up, so a gas month is a whole number of Gastage
rather than a calendar month shifted. An interval at 02:00 local on 1 March
belongs to *February's* gas scope.

So `VersionScope::for_interval` and `covers` take a `Sparte`:

```rust
let scope = VersionScope::for_interval("9900000000003", interval.from(), Sparte::Gas)?;
```

A gas Bilanzierungsmonat and a calendar month disagree by six hours at every
month boundary, and `covers` refuses whichever the store was not told to expect.

### The operator is parsed, not carried

It is the network operator's **Marktpartner-ID** — thirteen digits, what MSCONS
puts in `NAD+MS`, and the same identifier `MeasurementSource::Mscons` carries. The
constructors take `metering`'s `BdewCode`: a wrong-but-plausible operator is a
**different scope**, so its correction would never supersede and every sum would
be silently inflated. The stored column is a fixed twenty-one characters, with an
exact hot-table `CHECK` (`^[0-9]{13}:[0-9]{4}-(0[1-9]|1[0-2])$`).

The **check digit is enforced** — `BdewCode`'s rule: BDEW's procedure or GS1's,
because BDEW's *Identifikatoren in der Marktkommunikation* §2.3 admits GS1-issued
GLNs — so a transposed operator is refused when the scope is built.

## Identity versus attribute columns

Deployments carry columns the core schema does not — a tenant discriminator,
Bilanzkreis, grid area — and each is declared as one of two kinds:

```rust
TableConfig::new("readings_versions")
    .identity_column(Field::new("tenant", DataType::Utf8, false))   // joins the merge key
    .attribute_column(checked_column("bilanzkreis", ValueCheck::Eic(None), true))  // carries data
```

**Identity columns join the merge key.** Two rows differing in one are different
readings. A tenant discriminator belongs here: as an attribute, one tenant's
correction would silently supersede another's reading of the same measuring point.

**Attribute columns carry data.** A correction may change them freely.

The wrong choice raises no error, so it can be checked afterwards:
`store.admin().audit_attribute_column("tenant")` (or `meterstore audit`) counts
the merge keys for which the column takes more than one value. Corrections give a
handful; a misdeclared identity column gives a large share.

Identity columns must be non-nullable: a null cannot identify a reading, and in
SQL it does not compare equal to itself, so two such rows would never resolve
against each other.

The choice propagates to the storage schema, the hot primary key, the
`ON CONFLICT` target and the resolution view's `PARTITION BY`, which is why
`create_tables` is the single entry point.

Extra columns are `Utf8` only, and a declared name must be a plain identifier,
since identifiers cannot be parameterised. The **table** name is held to the same
rule and to at most 34 characters: PostgreSQL silently truncates identifiers at 63
bytes, and past 34 two derived constraint names on one partition
(`_YYYY_MM_DD_HHMM` plus `_one_operator`) would collide on the first write of a
new day. `build()` refuses it.

### Coded columns

An extra column whose values are a fixed vocabulary — an ingestion source, a
delivery status — can declare it:

```rust
.attribute_column(coded_column("ingest_source", &["MSCONS", "SMGW"], true))
```

```toml
extra_columns = [{ name = "ingest_source", values = ["MSCONS", "SMGW"] }]
```

That renders a `CHECK … IN (…)` on the hot table, like the built-in code columns,
so a value outside the set fails the write. The set rides in the field's Arrow
metadata, disturbing neither the type nor schema evolution.

### Checked columns

A column holding a market identifier can say so:

```rust
.attribute_column(checked_column("bilanzkreis", ValueCheck::Eic(Some(EicType::Party)), true))
.attribute_column(checked_column("bilanzierungsgebiet", ValueCheck::Eic(Some(EicType::Area)), true))
.attribute_column(checked_column("lieferant", ValueCheck::Bdew, true))
.attribute_column(checked_column("unterliegende_malo", ValueCheck::Malo, true))
```

```toml
extra_columns = [
  { name = "bilanzkreis",         check = "EIC:X" },
  { name = "bilanzierungsgebiet", check = "EIC:Y" },
  { name = "lieferant",           check = "BDEW" },
  { name = "unterliegende_malo",  check = "MALO" },
]
```

The write path parses every value with `metering`'s own type and stores its
**canonical spelling** — on an identity column, two spellings would be two
readings that never supersede each other.

**Each scheme stops somewhere, and it is not the same place.**

| `check` | Length | Shape | Arithmetic | Canonicalises |
|---|---|---|---|---|
| `EIC` | 16 | `0-9 A-Z -`, letter at 3, check character not `-` | **check character** | trim, uppercase |
| `EIC:X` … `EIC:A` | 16 | the same, with position 3 pinned to that object type | **check character** | trim, uppercase |
| `MALO` | 11 | digits, first not `0` | **check digit** | trim |
| `MELO` | 33 | 2 letters, 6 digits, 25 alphanumerics | none exists | trim, uppercase |
| `BDEW` | 13 | digits | BDEW's procedure, or GS1's | trim |

A Marktpartner-ID passes on either procedure (BDEW's Bildungsvorschrift §2.3
admits GS1-issued GLNs); one that passes on neither is refused. `MELO` has no
arithmetic but fixes the **casing**.

The hot table gets a `CHECK` for the *shape*, anchored on the stored form, so a
lower-case row from another writer is refused; the arithmetic is enforced only on
the write path, since no regular expression expresses a check digit. A row written
to PostgreSQL by something else gets the shape check and not the arithmetic.

`check` and `values` are mutually exclusive.

> **A DB constraint is created with the table.** `create_tables` is
> `CREATE TABLE IF NOT EXISTS`, so adding `values` or `check` to a column of a
> table that already exists starts the write-path validation and does not add the
> `CHECK`. Add it with `ALTER TABLE … ADD CONSTRAINT`, or recreate the table.
> Schema evolution does not flag it: the declaration rides in Arrow field
> metadata, which comparison ignores so that a vocabulary is not a schema change.

#### Which *kind* of EIC — `EIC:X` and the rest

A Bilanzkreis and a Bilanzierungsgebiet differ only at **position 3**, the
ENTSO-E object type: `X` a party, `Y` an area. A bare `check = "EIC"` column
accepts either, so a `Y` code in `bilanzkreis` would silently corrupt every MaBiS
grouping over it. `check = "EIC:X"` pins it, in the database constraint as well as
the write path:

```
EIC     ^[0-9A-Z-]{2}[A-Z][0-9A-Z-]{12}[0-9A-Z]$
EIC:X   ^[0-9A-Z-]{2}X[0-9A-Z-]{12}[0-9A-Z]$
```

The letters are ENTSO-E's list — `X` party, `Y` area, `Z` measurement point, `W`
resource object, `T` tie line, `V` location, `A` substation — and an unlisted one
such as `EIC:Q` is refused at declaration rather than degrading to bare `EIC`.

**Bare `EIC` stays tolerant**: `metering` parses an unlisted object type as `None`,
because the list is ENTSO-E's to extend. Where another, strict parser reads the
same codes,
[`eic_normalise` and `eic_object_type`](@/docs/querying.md#what-an-eic-names-is-a-query-not-a-constraint)
find the rows it would reject.

`eic_regelzone(code)` reads a Bilanzierungsgebiet's **Regelzone** off position 4 —
the grouping key of a MaBiS Summenzeitreihe.
[Querying →](@/docs/querying.md#the-other-stored-identifier)

## The Messlokation may be part of the identity

A **Marktlokation** may be measured by more than one **Messlokation** — a
Mehrfamilienhaus split into sub-measurements, an Einliegerwohnung with its own
meter.

| | Lastgang (`TimeModel::Interval`) | Zählerstandsgang (`TimeModel::Point`) |
|---|---|---|
| What a row is | the market location's load in a span | **a meter's** register reading at an instant |
| `melo_id` | labels the row | *names* it |
| In the merge key | no, by default | **yes**, by default |

A load profile belongs to the market location, one channel however many meters
produce it; keyed on the Messlokation, a correction against a re-registered meter
would fail to supersede. A register belongs to the *meter*: keyed on the market
location, two meters' readings collide — a differing pair is refused as a restated
version, an agreeing pair (two new meters at zero) loses one row to
`ON CONFLICT DO NOTHING`.

```rust
TableConfig::new("meter_reads_versions").time_model(TimeModel::Point)
TableConfig::new("readings_versions").identify_by_melo(true)   // or pin it either way
```

A sub-metering deployment may want the wider key on a Lastgang too. In the key, `melo_id` is `NOT NULL`, a delivery naming none is refused, and a
session can be [scoped](@/docs/querying.md#confining-a-session) to one
Messlokation. Out of it, both tiers still **compare** the column on a redelivery
and refuse a second Messlokation under an existing reading, naming
`identify_by_melo`.

## A tenant is not a market participant

Easy to conflate, with a financial consequence:

| | Answers | Where it lives |
|---|---|---|
| **Tenant** | *Whose installation this is.* An account, a customer of a service bureau, an isolation boundary. Opaque to MeterStore. | A deployment-declared identity column |
| **Network operator** | *Who assigned this version.* A 13-digit BDEW/DVGW Marktpartner-ID (`metering::ids::BdewCode`), and half of the scope a version is comparable within. | `version_scope` |

Neither determines the other: a supplier takes deliveries from every grid
operator it has customers behind. The tenant joins the merge key and the operator
must not — it makes two versions comparable rather than identifying the reading,
and in the key it would turn a corrected reading into two readings.

## The constraints that stop a wrong number

The merge key plus `version` is a primary key, and it is not the whole guard: two
rows for one channel at one version must differ in `from`, and two ranges that
differ in `from` can still *overlap*. A delivery carrying one hour as
`00:00–01:00` **and** the quarter-hours inside it leaves five rows standing where
four belong, and every aggregate over them double-counts.

Each hot partition therefore carries two `EXCLUDE USING gist` constraints:

1. **No overlapping intervals within one version.** Scoped to a version, because a
   correction *is* a higher version covering the same span: comparing across
   versions would refuse the one write the model is built around. The equality
   columns are read from the parent's actual primary key, so a tenant-extended key
   produces a tenant-*scoped* exclusion rather than one that rejects another
   tenant's reading.

   Rows at *different* versions may overlap. A **partial** re-grid leaves two
   winners covering one span, which nothing at the write can see; it surfaces as
   `surplus` in [completeness](@/docs/completeness.md).
2. **One network operator per reading.** Two operators for one reading give two
   incomparable scopes, resolution picks a winner in each, and **both** survive
   into the resolved view. The realistic cause is a caller passing a forwarding
   party's MP-ID where the network operator belongs.

Iceberg cannot carry a constraint, and `append` routes a below-watermark interval
straight there, so the second rule is *also* checked by `append` before the cold
write — scoped to the intervals being written, so the cost is proportional to the
correction rather than to the history.

Both need `btree_gist`, which ships in contrib and is created on demand.
`PostgresHot::integrity_constraints(false)` turns them off; do that with a
measurement in hand, because what is left is *detection* rather than refusal:

- An **overlap** shows up as `surplus` in [completeness](@/docs/completeness.md).
- A **duplicated scope** reaches the typed reads, which refuse to fold two values
  at one instant and raise `InvariantViolated`.
- Neither covers a `SUM` written in SQL. That will simply be twice the truth.

## Cold-tier layout

```text
warehouse/<namespace>/readings_versions/
  metadata/
  data/tenant=9900000000003/from_month=2026-03/data-<random>-0.parquet
```

| Choice | Value | Why |
|---|---|---|
| Partition spec | `identity(<identity columns>)`, then `month(from)` | An identity column is by definition something every query filters on, so leading with it eliminates other tenants' files at the *manifest*, before a footer is opened |
| Sort order | `(malo_id, from)` | "One meter, one year" becomes a contiguous scan of a few row groups. Declared in the Parquet footer, so a reader may rely on it |
| Row group | 256k rows | Bounds the per-writer buffer, which the fanout writer multiplies by open partitions — and sharpens row-group `malo_id` statistics |
| Bloom filters | `malo_id`, `obis_code` | Sized per column: the meter population for one, a code list for the other |
| Compression | ZSTD(3), page-level statistics | Best ratio/CPU balance for already well-encoded columns |

`month`, not `day`: archival runs one window per day, and a day transform would
give ~3 650 partitions per decade per tenant. No `bucket(malo_id)`: the sort order
and the `malo_id` bloom filter already answer the single-meter read. Iceberg's
**hidden partitioning** means queries never mention `tenant=` or `from_month=`.
