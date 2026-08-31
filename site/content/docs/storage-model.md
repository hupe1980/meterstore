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
| `version_scope` | `Utf8` | `string` | `<Marktpartner-ID>:<YYYY-MM>` — twenty characters, the **Bilanzierungsmonat**, cut at 06:00 for gas |
| `recorded_at` | `Timestamp(µs, UTC)` | `timestamptz` | Transaction time |
| `balancing_day` | `Date32` | `date` | The local day this reading is booked on |

`to` is the one column whose meaning depends on the table's **time model**: the
span's end on an interval table, null on a point one.


**Merge key:** `(malo_id, obis_code, from)` — plus `melo_id` on a table that
[identifies a reading by its Messlokation](#the-messlokation-may-be-part-of-the-identity),
plus any [identity columns](#identity-versus-attribute-columns) · **winner:**
`max(version)` within the same `version_scope`.

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

### `source_kind` is the payload's own tag

`MeasurementSource` is a data-carrying enum, so it needs two columns: a
discriminant that dictionary-encodes and filters cheaply, and a JSON payload that
round-trips the variant's fields exactly.

The discriminant is **read off the payload**, not written out beside it. A
hand-written list would be a second spelling of a vocabulary `metering` already
owns — and the payload in the very next column is renamed
`SCREAMING_SNAKE_CASE`, so an external engine filtering `source_kind = 'MSCONS'`
would find the two columns disagreeing.

Taking the tag from the serialised form means a variant renamed upstream moves
both columns together, and a variant added upstream needs no edit here.

### Two JSON columns {#json-columns}

`source_detail` and `provenance` are the only columns holding serialised structure
rather than a scalar. Both are `metering`'s own `serde` output: hand-writing
either would be a second copy of a vocabulary the domain owns.

`source_detail` carries a **nested** vocabulary — `VirtualMeter` holds a
`VirtualMeterKind`, so `PV_SELF_CONSUMPTION` is an upstream tag inside an upstream
payload. Retagged, `source_kind` still reads `VIRTUAL_METER` and still agrees with
the payload's outer key, so the discriminant check passes and only the payload
stops decoding.

`provenance` carries RFC 3339 timestamps, which is what lets an external engine
cast one straight out of the JSON:

```json
[{"occurred_at":"2026-03-01T00:00:00Z","event_type":"INGESTED","actor":"MSCONS","note":null}]
```

That spelling is `metering`'s (`wire::rfc3339`), not `time`'s. `time`'s own serde
impl is *feature-conditional* — a nine-element ordinal-date array without
`serde-human-readable`, a non-RFC-3339 string with it — so left to it, the on-disk
shape of an audit trail under a decades-long retention would be chosen by Cargo
feature unification.

> Anything persisted through `serde` has its stored shape decided by a
> *dependency*, and a change there is a **stored-data** break, not a wire-format
> one: rows already written stop deserialising. Both columns are asserted byte for
> byte in the test suite, so it fails here rather than at whoever reads the
> warehouse next.

### Codes are stored canonically, and only canonically

`sparte`, `unit`, `quality` and `resolution` hold exactly the string the domain
writes. `metering`'s `FromStr` is deliberately lenient — it trims, ignores case,
and accepts input aliases such as `WÄRME` for `WAERME`. That is right at an
ingest boundary and wrong for storage, because these columns are `GROUP BY` keys:
completeness groups by three of them, and two spellings of one commodity are two
rows in a report an operator is meant to be able to trust.

The hot tier refuses them already — its `CHECK … IN (…)` is rendered from `CODES`,
which is uppercase-canonical and excludes aliases. **Iceberg has no constraints**,
so decoding carries the same rule: a non-canonical spelling is a decode error
naming the canonical one, never a silently normalised value. `PT900S` and `PT15M`
are the same grid, and only `PT15M` is stored.

### `to` is null on a point table

One schema describes both time models, and `to` is what separates them: an
interval row carries the span's exclusive end, a **point** row — a Zählerstand —
carries null, because an instant has no end. `value` means the register's
cumulative reading there rather than energy over a span, which is why the two are
never the same table. See
[Zählerstandsgänge](@/docs/writing.md).

An interval table keeps `NOT NULL` in its own PostgreSQL DDL and the encoder
refuses a null before the write, so nothing loosens for a Lastgang.

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
it from the interval, and encoding rejects a scope that does not cover its
intervals.

### The month is the Bilanzierungsmonat, and gas cuts it at 06:00

It is a *local* month, because German market processes are defined in local time:
an interval starting `2026-07-31T23:00Z` is already August in Berlin.

For gas it is not the calendar month at all. EDI@Energy *Allgemeine Festlegungen*
v6.1c, Kap. 3.1 spells out the Bilanzierungsmonat Juni 2021 as 01.06 00:00 to
01.07 00:00 for Strom and 01.06 **06:00** to 01.07 **06:00** for Gas — the Gastag
boundary carries all the way up, so a gas month is a whole number of Gastage
rather than a calendar month shifted. An interval at 02:00 local on 1 March
belongs to *February's* gas scope.

So every `VersionScope` constructor takes a `Sparte`:

```rust
let scope = VersionScope::for_interval("9900000000001", interval.from, Sparte::Gas)?;
```

It is not decoration: a producer deriving the correct gas Bilanzierungsmonat and
one deriving the calendar month disagree by six hours at every month boundary,
and `covers` refuses whichever the store was not told to expect.

### The operator is parsed, not carried

It is the network operator's **Marktpartner-ID** — thirteen digits, what MSCONS
puts in `NAD+MS`, and the same identifier `MeasurementSource::Mscons` carries. The
constructors take `metering`'s `BdewCode`, for the reason the read paths take a
`MaloId` rather than eleven digits: past the constructor a wrong-but-plausible
operator is not an error, it is a **different scope**. The correction never
supersedes the value it corrects, both rows survive resolution, and every sum over
the reading is inflated with nothing anywhere reporting it.

That makes the stored column a fixed twenty characters, so the hot table's `CHECK`
is exact (`^[0-9]{13}:[0-9]{4}-(0[1-9]|1[0-2])$`) rather than "anything without a
colon", and the one-operator exclusion's `split_part(version_scope, ':', 1)` can
only ever take the half the type reports.

The **check digit is verified but not enforced**, which is `BdewCode`'s rule
rather than a choice made here: BDEW's *Identifikatoren in der
Marktkommunikation* §2.3 carves out GS1-issued GLNs, which use a different
procedure, so a well-formed Marktpartner-ID may legitimately fail the BDEW one.
`VersionScope::operator_has_bdew_check_digit()` reports it so an ingest boundary
can warn — the same restraint `Version::is_well_formed` applies to a short version
label.

## Identity versus attribute columns

Deployments carry columns the core schema does not — a tenant discriminator,
Bilanzkreis, grid area. Declaring one forces a choice, because the two kinds
behave differently:

```rust
TableConfig::new("readings_versions")
    .identity_column(Field::new("tenant", DataType::Utf8, false))   // joins the merge key
    .attribute_column(checked_column("bilanzkreis", ValueCheck::Eic(None), true))  // carries data
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

The **table** name is held to the same rule, and to a length: at most 34
characters. PostgreSQL truncates an identifier at 63 bytes without saying so, and
every name derived from the table's is longer — a partition adds
`_YYYY_MM_DD_HHMM`, and its integrity constraints add `_one_operator` on top of
that. Past 34 the two constraint names on one partition truncate to the same
string and the second `ADD CONSTRAINT` fails, on the first write of a new day
rather than at declaration. So it is refused at `build()`.

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

### Checked columns

A Bilanzkreis is not sixteen arbitrary characters and a Lieferant is not thirteen
arbitrary ones. Each is an identifier the market already validates, and a column
holding one can say so:

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

The same argument that makes `malo_id` a `MaloId` rather than eleven digits.
An identifier stored as an arbitrary string is a string that will one day differ
from the identifier it was meant to be, in a column nothing looks at until a
settlement run joins on it.

The write path parses every value with `metering`'s own type and stores that
type's **canonical spelling**. That matters most on an identity column, where one
identifier arriving in two spellings would be two readings that never supersede
each other.

**Each scheme stops somewhere, and it is not the same place.**

| `check` | Length | Shape | Arithmetic | Canonicalises |
|---|---|---|---|---|
| `EIC` | 16 | `0-9 A-Z -`, letter at 3, check character not `-` | **check character** | trim, uppercase |
| `EIC:X` … `EIC:A` | 16 | the same, with position 3 pinned to that object type | **check character** | trim, uppercase |
| `MALO` | 11 | digits, first not `0` | **check digit** | trim |
| `MELO` | 33 | 2 letters, 6 digits, 25 alphanumerics | none exists | trim, uppercase |
| `BDEW` | 13 | digits | **deliberately not checked** | trim |

The two with arithmetic are the two where a transposition is *detectable* while
the delivery that carried it is still in hand. `MELO` is still worth declaring
because it fixes the **casing** — a Messlokation in two casings on an identity
column is two readings — and `BDEW` because thirteen digits is the whole of the
rule that *can* be enforced: BDEW's Bildungsvorschrift §2.3 exempts GS1-issued
GLNs, which use a different check-digit procedure and are legitimate
Marktpartner-IDs. That is the same carve-out for which `version_scope`
deliberately does **not** check its operator's digit.

**Two checks, and each stops somewhere.** The hot table gets a `CHECK` for the
*shape* in the table above; the write path enforces whatever arithmetic the
scheme has, because a check digit is a function of the other digits and no
regular expression expresses one. A row written to PostgreSQL by something else
gets the first and not the second.

The shape patterns are anchored on the **stored** form: `EIC` and `MELO`
canonicalise to uppercase, so a row written in lower case by another writer is
refused rather than becoming a second spelling.

`check` and `values` are mutually exclusive: a closed vocabulary and an open
identifier scheme are two different claims about one column.

> **A DB constraint is created with the table.** `create_tables` is
> `CREATE TABLE IF NOT EXISTS`, so adding `values` or `check` to a column of a
> table that already exists starts the write-path validation and does not add the
> `CHECK`. Add it with `ALTER TABLE … ADD CONSTRAINT`, or recreate the table.
> Schema evolution does not flag it: the declaration rides in Arrow field
> metadata, which comparison ignores so that a vocabulary is not a schema change.

#### Which *kind* of EIC — `EIC:X` and the rest

A Bilanzkreis and a Bilanzierungsgebiet share the alphabet, the length, the
issuing office and the check character. The only thing that tells them apart is
**position 3**, the ENTSO-E object type: `X` a party, `Y` an area. So a bare
`check = "EIC"` column accepts either, and a `Y` code in the `bilanzkreis` column
passes the write path, passes the `CHECK`, and makes every MaBiS grouping over it
wrong — with no error anywhere.

`check = "EIC:X"` pins it. Unlike the check character, this half **is**
expressible as a regular expression, so it strengthens the database constraint as
well as the write path:

```
EIC     ^[0-9A-Z-]{2}[A-Z][0-9A-Z-]{12}[0-9A-Z]$
EIC:X   ^[0-9A-Z-]{2}X[0-9A-Z-]{12}[0-9A-Z]$
```

It is the one part of *"is this the right kind of EIC"* PostgreSQL can enforce on
a row this crate did not write. The letters are ENTSO-E's list — `X` party, `Y`
area, `Z` measurement point, `W` resource object, `T` tie line, `V` location,
`A` substation — and `EIC:Q` is refused at declaration rather than degrading to a
bare `EIC`, because a column declared as holding party codes and silently taking
any EIC is the outcome the declaration exists to rule out.

**Bare `EIC` stays deliberately tolerant.** `metering` parses an object type it
does not list as `None` rather than failing, because the list is ENTSO-E's to
extend and a store that refused an entry added after its release would reject
data the market has already issued. That matters when an EIC passes through
**two** parsers in one process and the other one is strict —
[`eic_normalise` and `eic_object_type`](@/docs/querying.md#what-an-eic-names-is-a-query-not-a-constraint)
are how those rows are found before the strict parser finds them for you.

`eic_regelzone(code)` reads a Bilanzierungsgebiet's **Regelzone** off position 4 —
the grouping key of a MaBiS Summenzeitreihe.
[Querying →](@/docs/querying.md#the-other-stored-identifier)

## The Messlokation may be part of the identity

A **Marktlokation** may be measured by more than one **Messlokation**, and that is
ordinary: a Mehrfamilienhaus split into sub-measurements, a house whose
Einliegerwohnung has its own meter. The network operator assigns them, many-to-one.

| | Lastgang (`TimeModel::Interval`) | Zählerstandsgang (`TimeModel::Point`) |
|---|---|---|
| What a row is | the market location's load in a span | **a meter's** register reading at an instant |
| `melo_id` | labels the row | *names* it |
| In the merge key | no, by default | **yes**, by default |

A load profile belongs to the market location, one channel however many meters
produce it — keying on the Messlokation there is the mistake the
[subject column](@/docs/privacy.md) avoids, where a correction against a
re-registered Messlokation gets a different key and fails to supersede.

A register belongs to the *meter*, so both meters under one market location carry
`1-0:1.8.0` at the same instants. Keyed on the market location they collide: a
differing pair is refused as a value restated under an existing version — loud,
about the wrong thing — and an agreeing pair, which two freshly installed meters
are at zero, has one of them dropped by `ON CONFLICT DO NOTHING`.

```rust
TableConfig::new("meter_reads_versions").time_model(TimeModel::Point)
TableConfig::new("readings_versions").identify_by_melo(true)   // or pin it either way
```

A portfolio with one Messlokation per Marktlokation has nothing to gain from the
wider key; a sub-metering deployment may want it on a Lastgang.

In the key, `melo_id` is `NOT NULL`, a delivery naming none is refused, and a
session can be [scoped](@/docs/querying.md#confining-a-session) to one
Messlokation. Out of it, both tiers still **compare** the column on a redelivery
and refuse a second Messlokation under an existing reading, naming
`identify_by_melo`.

## A tenant is not a market participant

Two words that are easy to conflate, with a financial consequence:

| | Answers | Where it lives |
|---|---|---|
| **Tenant** | *Whose installation this is.* An account, a customer of a service bureau, an isolation boundary. Opaque to MeterStore. | A deployment-declared identity column |
| **Network operator** | *Who assigned this version.* A 13-digit BDEW/DVGW Marktpartner-ID (`metering::BdewCode`), and half of the scope a version is comparable within. | `version_scope` |

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

   So two rows at *different* versions may overlap, and usually should. Resolution
   collapses the ordinary case — a re-grid restating every interval start
   supersedes each one on its merge key — but a **partial** re-grid leaves two
   winners covering the same span. Nothing at the write can see that; it surfaces
   as `surplus` in [completeness](@/docs/completeness.md).
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
measurement in hand, because what is left is *detection* rather than refusal:

- An **overlap** shows up as `surplus` in [completeness](@/docs/completeness.md).
- A **duplicated scope** reaches the typed reads, which refuse to fold two values
  at one instant and raise `InvariantViolated`.
- Neither covers a `SUM` written in SQL. That will simply be twice the truth.

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
