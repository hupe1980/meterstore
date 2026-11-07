+++
title = "Privacy and retention"
description = "Why 15-minute consumption is personal data, why § 60 Abs. 6 MsbG is a deletion duty rather than a retention mandate, and how pseudonymisation satisfies it over an append-only lake."
weight = 11
+++

## Is a load profile personal data?

Yes, and the granularity is why. An annual meter reading is one number and reveals
almost nothing. A **15-minute load profile** reveals when a household wakes,
sleeps, leaves and returns; how many people are present; when it goes on holiday;
and — through non-intrusive load monitoring — which appliances it runs.

It is information relating to an identifiable person under Art. 4 Nr. 1 DSGVO,
and German law treats it accordingly: the BSI Smart-Meter-Gateway protection
profile and the MsbG's rules on who may receive which granularity exist because
of it. The MaLo alone does not escape that — Erwägungsgrund 26 DSGVO asks what
means are *reasonably likely* to be used, and a utility holds the contract linking
measuring point to customer.

## The statute points at deletion, not retention

**§ 60 Abs. 6 MsbG is a deletion duty:**

> Der Messstellenbetreiber muss personenbezogene Messwerte … **löschen oder …
> anonymisieren**, sobald für seine Aufgabenwahrnehmung eine Speicherung
> personenbezogener Messwerte nicht mehr erforderlich ist, **spätestens jedoch
> nach drei Jahren** ab dem Schluss des Kalenderjahres, in dem der jeweilige
> Messwert erhoben wurde …

Three years is a **ceiling**, and the operative trigger is earlier still — as soon
as the data is no longer needed. A system built to retain personal metering values
for three years *because the law says so* has it inverted.

Retention obligations that do point the other way — the Eichrecht documentation
duties, and any longer period the Bundesnetzagentur sets — are about the
**settlement record**, not about values linked to a person:

| | Obligation | What satisfies it |
|---|---|---|
| Personal metering values | Erase or anonymise, ≤ 3 years | Destroy the linkage the store holds — [what that leaves](#pseudonymisation-not-crypto-shredding) |
| The settlement record | Keep, and keep it reproducible | The lake, unchanged |

The statute says *löschen **oder** anonymisieren*. An append-only lake cannot take
the first branch, so the store deletes the linkage it controls — a mapping row in
PostgreSQL. Whether that reaches the second branch is discussed below.

## Pseudonymisation, not crypto-shredding

Crypto-shredding — encrypt each subject's data under its own key, destroy the
key — needs one key per data subject. Iceberg's envelope encryption keys data per
*file*, and one Parquet file holds thousands of measuring points; one file per
subject would be a directory listing rather than a table. Encrypting the value
column per subject instead destroys delta encoding, min/max statistics and bloom
filters.

So the personal data in a metering series is treated as the **link** between a
consumption pattern and a person, and the store holds that link as a mapping row
it can delete. Deleting it leaves quantities attached to an opaque reference —
**and to their `malo_id`**, which stays in clear on every row in both tiers, in
the Iceberg column statistics and in the bloom filters. A utility holding the
contract that links a Marktlokation to a customer can still attribute them, so
what remains is **pseudonymised** data, not anonymous data under Recital 26 — the
same reasoning [the first section](#is-a-load-profile-personal-data) applies to
the MaLo. Whether deleting the mapping discharges § 60 Abs. 6's *anonymisieren*,
or the measuring point needs an opaque per-year key in the lake as well, is not
decided.

```rust
let readings = MeterStore::builder()
    .table(TableConfig::new("readings_versions").subject_column("subject_ref").build()?)
    .subject_registry(
        SubjectRegistry::with_erasure_secret(database.clone(), &secret)?
            // The application's own rows go with the mapping, in its transaction.
            .with_cascade(ErasureCascade::new("DELETE FROM billing_periods WHERE subject_ref = $1")?),
    );
    // … hot, cold

let catalog = MeterCatalog::builder().table(readings).build().await?;

// An opaque reference, for the collection year these readings belong to.
let subject = catalog.register_subject("customer-4821", interval.from(), sparte).await?;

// Later: destroy the link, in every year. The readings stay; nothing can
// attribute them. A request names a person, so it can be entered as one.
catalog.erase_subject_by_id("customer-4821", "DSAR-2026-0042", "privacy-team", now).await?;
```

**The subject API is on the catalog, not on a table**, because one registry spans
the deployment (below). A deployment with a single `MeterStore` reaches it through
`store.subject_registry()`.

| Property | How it is met |
|---|---|
| **Irreversible** | The mapping row is deleted, not flagged. There is no recovery path to disclose. |
| **Auditable** | An append-only record of what was erased, when, why and by whom — and deliberately *not* the natural identifier, which would preserve the link being destroyed. |
| **Per subject** | One reference, one mapping row. Erasing one leaves every other intact — the property file-keyed encryption cannot provide. |
| **Cheap** | `O(1)`. No key management, no rewrite, no lake mutation, every analytical property preserved. |

**It is an attribute column, never an identity column.** The reference is
determined by the reading, not part of what identifies it; in the merge key, a
correction carrying a re-registered reference would silently fail to supersede.

**One registry spans every table in a deployment**, though it is passed to each
store builder. The mapping lives in one `meterstore_subject_map` keyed by
`(natural identifier, collection year)`, so tables registering the same identifier
for the same year share one reference, and `erase_subject` unlinks every year of
it in every table — the authoritative readings **and**, say, an ESA "Werte nach
Typ 2" store, which is non-authoritative for settlement but personal all the same.

**What a subject *is* is your choice, and it is global.** A Marktlokation outlives
its occupants, so keying by measuring point alone erases a previous tenant's data
along with the requester's — `(tenant, MaLo)`, or an occupancy period, is usually
what is meant.

**The store refuses references the registry does not back** — an invented one, or
one belonging to an erased subject that a replay is relinking — at the write.

**References are checked for shape and refused when they are identifiers.**
`register_subject` draws 128 bits from the OS CSPRNG. `SubjectRef::new`, for a
deployment minting its own, requires `s<year>_<token>` with the year spelled
canonically and a token of 22–128 characters from `A-Z a-z 0-9 . _ -`, and refuses
a token that **parses** as a MaLo-ID, a Messlokation, an EIC or a BDEW code. It
cannot tell a keyed hash from an unkeyed one — `s2026_<sha256 of an email>` passes
and survives erasure as a re-identification path — so it does not certify that a
reference you minted is unlinkable.

## The unit of erasure is a collection year

§ 60 Abs. 6 runs on *"der jeweilige Messwert"* — **each value**, three years after
the end of the calendar year it was collected in. So a reference belongs to **one
collection year**, and the year is part of it: `s2026_9f3c…`. Two years give two
references, which the sweep expires independently:

```rust
let y2022 = catalog.register_subject("customer-4821", jan_2022, Sparte::Strom).await?;
let y2026 = catalog.register_subject("customer-4821", jan_2026, Sparte::Strom).await?;
assert_ne!(y2022, y2026);
```

**Pass an instant from the data, not `now()`.** A backfill of 2024 arriving today
must carry 2024's reference. A reference used on another year's readings is
refused at the write.

### The year is the one the readings are balanced in

`erasure::retention_epoch(at, sparte)` is the rule for both the mint and the
write's check: the year of the day a reading is **settled** on, which for gas is
the Gastag. A gas reading at `2026-01-01T00:00Z` is still Gastag 2025-12-31, epoch
2025, so a Gastag spanning New Year stays one series with one reference. A
delivery that crosses a balancing-year boundary is split at it.

The **sweep** compares the `epoch` integer against plain calendar years, the
direction that never keeps personal data past its ceiling.

### Answering a request that names a person

A DSAR names a customer number, a contract or an occupancy — never the opaque
token, never a year — so the identifier is an entry point of its own:

```rust
// What is still linked, if anything.
let years = catalog.subject_epochs("customer-4821").await?;   // e.g. [2024, 2025, 2026]

// Unlink every one of them, in every table.
let records = catalog
    .erase_subject_by_id("customer-4821", "DSAR-2026-0042", "privacy-team", now)
    .await?;
```

`erase_subject`, given a reference, resolves it to its identifier and destroys
**every** epoch behind it, one audit record per year. A reference whose year the
sweep has already expired is **refused** with `Error::SubjectUnresolvable` and
nothing is written — use `erase_subject_by_id`. A reference already erased by a
request returns the record the trail holds.

**A request may arrive before the delivery does.** With a suppression key, the
tombstone is written anyway and the later delivery is refused
(`ErasureRecord::subject` is `None`). Without a key, nothing is recorded and the
call returns empty.

## Keeping a subject erased

After erasure, **nothing distinguishes an erased identifier from one never seen**,
so a broker replaying a batch from before the erasure would register the
identifier again and rebuild the link. `with_erasure_secret` therefore records a
**keyed hash** of the erased identifier, and `register` refuses anything that
matches. Keyed, because identifiers come from small structured spaces an unkeyed
hash could be enumerated over. `SubjectRegistry::new` omits the secret, and replay
then defeats erasure — wrong for anything fed by a message broker.

`lift_subject_suppression` on the catalog (`lift_suppression` on the registry)
undoes an erasure carried out against the wrong subject: it allows registering
again under a new reference, restores no old link, and is itself audited —
`lifted_at`, `lifted_by` and `lift_reason`, returned as `ErasureRecord::lifted`.

`meterstore.registrations_suppressed` counts refused registrations. Zero is the
expected reading; each one is an upstream system replaying pre-erasure data.

### Rotating the key

The key must outlive every erasure and is not recoverable from the database;
losing it disables suppression for the subjects it covers, permanently. **A
tombstone can never be re-keyed** — the identifier was destroyed when it was
written — so the key is a **ring**: the first key writes, every key is checked,
and rotation is additive.

```toml
[privacy]
erasure_secret = "${METERSTORE_ERASURE_SECRET}"                  # writes
retired_erasure_secrets = ["${METERSTORE_ERASURE_SECRET_2025}"]  # still read
```

A retired key stops writing but stays in the ring for as long as its erasures
must stay suppressed — indefinitely, for Art. 17 DSGVO. Retired keys meet the same
32-byte floor, and `retired_erasure_secrets` without an `erasure_secret` is
refused at startup.

**A ring missing a key says so.** Each tombstone records a `key_id` derived from
the key that wrote it, so a deployment is told at startup which key is missing and
how many live suppressions it stopped honouring:

```text
WARN the erasure key ring no longer carries the key these suppressions were
     written under, so the subjects they cover can be re-registered by a replay;
     put the key back, or stop the replay upstream
     key_id=3f1a9c04d7b25e68 suppressions=412
```

`MeterCatalog::orphaned_suppressions` (or `SubjectRegistry::orphaned_suppressions`)
answers the same on demand, for a health endpoint; put a candidate key back and
watch the count shrink to identify one. Lifted suppressions are never counted.

**In memory the key is redacted *and* wiped**: `Debug` prints only whether one is
configured, and every clone a derived session holds is zeroized on drop. That does
not reach the buffer you passed in, an environment variable, or `hmac`'s per-call
key schedule. Object-store credentials are redacted but **not** wiped, since the
object store's own client holds them for the life of the process; the platform
credential chain is what keeps a key out of the process.

## The duty on a clock

`erase_subject` answers an Article 17 request. § 60 Abs. 6 comes due on its own,
so it is a job:

```rust
let handle = catalog.maintenance()
    .anonymise_after(Retention::CalendarYears(3), "§ 60 Abs. 6 MsbG", "retention-job")
    .spawn();
```

Every linkage whose collection year has passed the cutoff is destroyed, whether
or not the subject is still being metered.

- **Keyed to the collection year.** A customer registered in 2020 and still
  metered today has their 2020 linkage expire on schedule and their 2026 one
  untouched.
- **It reads no table.** The year is on the mapping row, so the sweep is one
  indexed `DELETE` against the registry, unaffected by any read mode.
- **Idempotent and resumable.** It deletes in batches, each its own transaction;
  an interrupted run is finished by running it again, with no second audit row.
- **`CalendarYears(3)` is not `now - 3 years`.** The statutory clock starts at the
  *Schluss des Kalenderjahres*, so a value collected on 2 January 2025 comes due on
  31 December 2028; the rolling spelling would erase it a year early.
- **`Rolling(d)` expires whole years** — the calendar years that ended more than
  `d` ago — for the earlier "nicht mehr erforderlich" trigger. Never early, but
  `Rolling(30 days)` on 15 January 2028 keeps a value from 2 January 2027. A
  sub-year policy is `erase_subject` on your own schedule.
- **No suppression tombstone.** An expiry is not a request to stop processing; a
  subject whose 2021 epoch expired must still be registrable for 2027.

`catalog.anonymise_before(cutoff, …)` is the same sweep run once. It lives on the
**catalog**, with no per-table form, because the registry is deployment-wide. It
does not need a table declaring a subject column: `Settings::connect()` builds the
registry whenever the database already holds `meterstore_subject_map`, so mapping
rows from a column no longer declared still expire. Only a catalog with no
registry at all (built in code without `MeterCatalogBuilder::subject_registry`)
refuses the sweep.

It returns a **`SweepOutcome`** — linkages destroyed, their collection years, the
batch count — and the per-subject trail is durable in `meterstore_erasures` before
it returns:

```rust
let swept = catalog.anonymise_before(cutoff, "§ 60 Abs. 6 MsbG", "retention-job", now).await?;
println!("{} linkages, epochs {:?}", swept.subjects, swept.epochs);
```

Deleting the mapping rewrites no byte of the lake; whether it suffices while
`malo_id` stays in clear is the question [above](#pseudonymisation-not-crypto-shredding).

## Configuring it

A table declaring a `subject_column` needs a registry; `MeterStore::builder`
refuses one without at `build`. A configuration file builds the registry itself,
so `[privacy]` supplies only the key:

```toml
[privacy]
erasure_secret = "${METERSTORE_ERASURE_SECRET}"   # ≥ 32 bytes; optional

[[tables]]
name = "readings_versions"
subject_column = "subject_ref"
```

`create_tables` checks the columns it is about to write, so a registry carrying
an earlier shape of these tables is refused there — naming what to drop — rather
than at the first erasure, as a missing-column error.

`Settings::connect()` builds **one** registry for the whole deployment, over the
hot tier's `Database`. The application's own rows that a request must reach —
billing periods, substitute-value logs — are declared as cascades, which every
request-triggered erasure runs in the transaction that destroys the mapping, with
the erased reference bound as `$1`; a failing cascade fails the erasure:

```toml
[privacy]
erasure_cascade = ["DELETE FROM billing_periods WHERE subject_ref = $1"]
```

The retention sweep runs none: it pseudonymises, and rows keyed by the reference
may carry retention duties of their own. A configuration with no subject column
and no existing `meterstore_subject_map` gets no registry and creates none of its
tables.

`erasure_secret`, which turns the suppression list on, is optional because a lost
key disables suppression permanently: a deployment that cannot yet hold a key
securely is better off knowing suppression is off.

`meterstore erasures` reads the audit trail from a shell; there is no
`meterstore erase` ([why](@/docs/cli.md#no-meterstore-erase)).

## The trail as evidence

Every row records **which duty it discharged** — `request` for Article 17,
`retention` for § 60 Abs. 6 — so a sweep that stopped running is not hidden behind
requests. The same value is the `trigger` attribute on
`meterstore.subjects_erased`.

`ErasureQuery` narrows it the way evidence is asked for — a period and a duty:

```rust
let q3 = catalog.erasures(
    &ErasureQuery::new()
        .since(datetime!(2026-07-01 0:00 UTC))
        .until(datetime!(2026-10-01 0:00 UTC))
        .trigger(ErasureTrigger::Retention)
        .limit(10_000),
).await?;
```

The period is half-open, so consecutive quarters tile. A backwards period or a
non-positive limit is **refused** rather than returning an empty result that reads
as *"nothing was erased"*.

## What it requires of the deployment

**The pseudonymous reference must be the *only* link.** A 15-minute series is
potentially re-identifiable by singling out, so if another system holds the same
series against a name, deleting this mapping achieves nothing. Erasure is a
property of the whole estate; this module guarantees its own part.

**Granularity is your choice**, and it is global — see above.

## General security posture

| Concern | Approach |
|---|---|
| Credentials | Environment-variable interpolation; never logged, `Debug` redacts a connection URL to its scheme |
| SQL injection | Whitelisted expression grammar, typed parameter binding. Caller values are never concatenated into SQL; declared column names are restricted to plain identifiers, since an identifier cannot be parameterised |
| Caller-supplied SQL | `query`, `sql` and `stream` plan without running and refuse anything that is not a query. DataFusion's surface is wider than `SELECT`: `CREATE EXTERNAL TABLE … LOCATION` reads any path the process can, `COPY … TO` writes one, and an external table over the warehouse's Parquet walks past a scoped session because it never touches the provider that enforces the scope. See [Confining a session](@/docs/querying.md#confining-a-session) |
| Least privilege | No `SUPERUSER`. `CREATE` on the schema and on the database (for the trusted `btree_gist` extension), ownership of its own tables for `DETACH`/`DROP`, and `INSERT`/`DELETE` on `meterstore_pins` for every role that queries — see [Requirements](@/docs/getting-started.md#requirements) |
| Transport | PostgreSQL connections opened from configuration are **verified TLS by default** (`sslmode=verify-full`); the silently-falling-back modes are refused, and plaintext needs `sslmode=disable` written out — see [encrypted connections](@/docs/configuration.md#encrypted-connections). Object storage is reached over the scheme its URI names |
| At rest | Object-store SSE. Per-subject encryption is deliberately not used |
| Serving surfaces | Read-only — the endpoints refuse mutating calls *and* the query path refuses non-query statements — and both hand back a service rather than binding a port, so authentication is yours to supply |
| Supply chain | `cargo-deny` in CI with a pinned lockfile. Every licence and advisory exception carries a written reason and a revisit condition |

Nothing here is legal advice. Deployments take their own.
