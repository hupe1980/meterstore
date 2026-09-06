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

It is information relating to an identifiable person under Art. 4(1), and German
law treats it accordingly: the BSI Smart-Meter-Gateway protection profile and the
MsbG's provisions on who may receive which granularity exist precisely because
this data is privacy-critical. The MaLo alone does not escape that — Recital 26
asks what means are *reasonably likely* to be used, and a utility holds the
contract linking measuring point to customer.

## The statute points at deletion, not retention

This is worth stating plainly, because it is easy to get backwards. **§ 60 Abs. 6
MsbG is a deletion duty:**

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
| Personal metering values | Erase or anonymise, ≤ 3 years | Destroy the linkage |
| The settlement record | Keep, and keep it reproducible | The lake, unchanged |

One store has to satisfy both, and it can, because the statute says *löschen
**oder** anonymisieren*. An append-only lake cannot take the first branch — and
does not have to, because the second is a mapping row in PostgreSQL.

## Pseudonymisation, not crypto-shredding

The usual answer for an immutable store is to encrypt each subject's data under
its own key and destroy the key. Regulators accept the technique — the EDPB, the
UK ICO and the French CNIL all recognise cryptographic erasure.

**It does not work here, for a structural reason rather than a missing feature.**
Crypto-shredding needs key granularity aligned to the erasure unit: one key per
data subject. Iceberg's envelope encryption keys data per *file*, and at metering
volume one Parquet file holds thousands of measuring points, so destroying its key
erases all of them. Aligning keys to subjects would mean one file per subject,
which at 100 k meters is a directory listing rather than a table.

Encrypting the value column per subject instead keeps the file layout but destroys
everything the cold tier's performance rests on: delta encoding needs adjacent
values to be numerically close, min/max statistics need comparable values, bloom
filters need stable equality. Ciphertext has none of those.

So the personal data in a metering series is treated as what it actually is — not
the numbers, but the **link** between a consumption pattern and a person. Break
the link and what remains is quantities attached to an opaque token: anonymous
data, outside the Regulation's scope by Recital 26.

```rust
let store = MeterStore::builder()
    .table(TableConfig::new("readings_versions").subject_column("subject_ref").build()?)
    .subject_registry(SubjectRegistry::with_erasure_secret(pool, &secret)?)
    // … hot, cold
    .build()
    .await?;

// An opaque reference, for the collection year these readings belong to.
let subject = store.register_subject("customer-4821", interval.from, sparte).await?;

// Later: destroy the link, in every year. The readings stay; nothing can
// attribute them. A request names a person, so it can be entered as one.
store.erase_subject_by_id("customer-4821", "DSAR-2026-0042", "privacy-team", now).await?;
```

| Property | How it is met |
|---|---|
| **Irreversible** | The mapping row is deleted, not flagged. There is no recovery path to disclose. |
| **Auditable** | An append-only record of what was erased, when, why and by whom — and deliberately *not* the natural identifier, which would preserve the link being destroyed. |
| **Per subject** | One reference, one mapping row. Erasing one leaves every other intact — the property file-keyed encryption cannot provide. |
| **Cheap** | `O(1)`. No key management, no rewrite, no lake mutation, every analytical property preserved. |

This is a stronger position than crypto-shredding, not merely a cheaper one: there
is no argument to have about whether ciphertext is still personal data, because
the linking data is actually gone.

**References come from the OS CSPRNG.** A reference an attacker can predict or
recompute is a re-identification path that survives erasure. The shape is
checked, since that is the checkable part: `s<year>_<token>`, token ≥ 22
characters from `A-Z a-z 0-9 . _ -`. Hex, base64url and a UUID all pass; the
customer number a pipeline substitutes when it has no reference to hand does not.

**It is an attribute column, never an identity column.** A measuring point
produces one reading per interval whoever occupies it, so the reference is
determined by the reading rather than part of what identifies it. In the merge key
it would look harmless and would not be: a correction whose reference was derived
slightly differently — a re-registration, a pipeline holding a stale mapping —
gets a different key and silently fails to supersede the value it corrects.

**One registry spans every table in a deployment.** It is passed to each store
builder, which reads as *per table* — and it is not. The mapping lives in one
`meterstore_subject_map` keyed by `(natural identifier, collection year)`, so two
tables that register the same natural id for the same year share one reference —
and `erase_subject`, which answers a request about a *person*, unlinks every year
of them in every table.

That is what an Article 17 request needs rather than an accident of the schema.
An erasure has to reach the authoritative readings **and** the non-authoritative
second stream: an ESA "Werte nach Typ 2" store is non-authoritative for
*settlement*, which says nothing about whether the data is personal. A registry
per table would leave one of them linked with nothing to report it.

The corollary is that **what a subject *is* is your choice, and it is global.**
A Marktlokation outlives its occupants, so keying by measuring point alone erases
a previous tenant's data along with the requester's — `(tenant, MaLo)`, or an
occupancy period, is usually what is meant.

**The store refuses references the registry does not back.** A reference that does
not resolve means either the pipeline invented it — rows unattributable from birth
— or it belongs to an already-erased subject, meaning a replay is rebuilding the
link erasure destroyed. Neither is visible in the data afterwards, so it fails at
the write.

## The unit of erasure is a collection year

§ 60 Abs. 6 runs on *"der jeweilige Messwert"* — **each value**, three years
after the end of the calendar year it was collected in. One reference covering a
subject's whole history cannot express that: destroy it and readings still inside
their period are orphaned, keep it and an active customer's decade-old values
stay attributable for as long as they remain connected.

So a reference belongs to **one collection year**, and the year is part of it:
`s2026_9f3c…`. Registering the same customer for two years gives two references,
and the sweep expires them independently:

```rust
let y2022 = store.register_subject("customer-4821", jan_2022, Sparte::Strom).await?;
let y2026 = store.register_subject("customer-4821", jan_2026, Sparte::Strom).await?;
assert_ne!(y2022, y2026);
```

**Pass an instant from the data, not `now()`.** A backfill of 2024 arriving today
must carry 2024's reference; `now()` would attach this year's and keep it
attributable four years too long. A reference used on another year's readings is
refused at the write — the year is in the reference, so the check is a string
parse. Without it nothing downstream could tell: the column stays well-formed,
the reference resolves, and the only symptom is a sweep that never comes due.

### The year is the one the readings are balanced in

`erasure::retention_epoch(at, sparte)` is the rule, and both sides read it — the
mint and the write's check are the same call. The commodity is in it because the
epoch is the year of the day a reading is **settled** on, and for gas that is the
Gastag, 06:00 to 06:00 local.

It matters for six hours a year. A gas reading at `2026-01-01T00:00Z` is 01:00 on
New Year's Day in Berlin and still Gastag 2025-12-31 — balanced in the December
Bilanzierungsmonat, invoiced with 2025, epoch 2025. That also keeps a delivery
whole: a Gastag spanning New Year has intervals in two local years but one
balancing year, so an MSCONS Lastgang for it stays one series with one reference.
A delivery that genuinely crosses a balancing-year boundary is split at it.

The **sweep** keeps the plain calendar year: the `epoch` column is one integer
shared by every commodity, so there is no `sparte` to ask it with, and taking the
gas boundary there would push every epoch a year later — the direction that keeps
personal data past its ceiling. For everything but gas the two are the same
number, and the year is Berlin's either way.

### Answering a request that names a person

A DSAR arrives with a customer number, a contract or an occupancy — never with
the opaque token the lake stores, and never with a year. So the identifier is an
entry point of its own:

```rust
// What is still linked, if anything.
let years = catalog.subject_epochs("customer-4821").await?;   // e.g. [2024, 2025, 2026]

// Unlink every one of them, in every table.
let records = catalog
    .erase_subject_by_id("customer-4821", "DSAR-2026-0042", "privacy-team", now)
    .await?;
```

`erase_subject`, given a reference, does the same thing: it resolves the
reference to its identifier and destroys **every** epoch behind it, one audit
record per year. Erasing only the year whose reference the caller happened to
hold would report the request as honoured while last year's readings stayed
attributable.

**A request may arrive before the delivery does.** There is then no linkage to
destroy, and the other half of the request still stands: *do not start*. With a
suppression key configured this writes the tombstone anyway and the delivery that
follows is refused — the audit row names no reference, so `ErasureRecord::subject`
is `None`. Without a key nothing could recognise the identifier later, so nothing
is recorded and the call returns empty.

## Keeping a subject erased

Deleting the mapping raises a problem the mechanism creates for itself:
afterwards, **nothing distinguishes an erased identifier from one never seen**. A
broker redelivers a batch from before the erasure, the ingest path registers the
identifier again, gets a fresh reference, and the link is rebuilt. The erasure was
real and lasted until the next replay.

The check cannot be built from what erasure leaves behind, because erasure
deliberately leaves nothing. So `with_erasure_secret` records a **keyed hash** of
the erased identifier, and `register` refuses anything that matches.

Keyed rather than plain because market-location and customer identifiers come from
small structured spaces: an unkeyed hash could be inverted by enumeration and the
tombstone would leak what it exists to forget. The key reduces it to an oracle
answering "was this one erased?" only for someone already holding both the
identifier and the key — the minimum needed to honour a request that says *stop
processing my data*, and the recognised practice for suppression lists.

`SubjectRegistry::new` omits the secret and documents that replay then defeats
erasure. That is the honest default for a deployment with no ingest replay, and
the wrong one for anything fed by a message broker.

`lift_suppression` is the escape hatch, because the list would otherwise make one
mistake unrecoverable: an erasure carried out against the wrong subject would lock
a real customer out permanently. Lifting restores the ability to *register* again,
under a new reference. It does not restore the old link, and the audit row
survives.

**Lifting is itself audited** — `lifted_at`, `lifted_by` and `lift_reason` on the
erasure rows it clears, returned as `ErasureRecord::lifted`. It reverses a
compliance decision, so the trail has to show who authorised it and not only that
a registration reappeared.

**Refused registrations are counted.** `meterstore.registrations_suppressed` is
the replay alarm: each one is a system upstream still carrying data from before an
erasure. Refused here, still to be fixed there. Zero is the expected reading.

### Rotating the key

The key must outlive every erasure and is not recoverable from the database.
Losing it exposes nothing; it silently disables suppression.

**A tombstone can never be re-keyed** — it is `HMAC(key, identifier)` and the
identifier was destroyed in the same transaction that wrote it. So the key is a
**ring**: the first key writes every new tombstone, every key is checked on a
lookup, and rotation is additive.

```toml
[privacy]
erasure_secret = "${METERSTORE_ERASURE_SECRET}"                  # writes
retired_erasure_secrets = ["${METERSTORE_ERASURE_SECRET_2025}"]  # still read
```

Retiring a key stops it writing; it does not mean it can be destroyed. A key stays
in the ring for as long as the erasures it recorded must stay suppressed —
indefinitely, for Article 17. What rotation bounds is a key's window as a *writing*
key, which is what a compromise of it costs. Retired keys meet the same 32-byte
floor, since it is the older tombstones they cover, and `retired_erasure_secrets`
without an `erasure_secret` is refused at startup as the half-finished rotation it
is.

**In memory it is redacted *and* wiped.** `Debug` prints only whether a key is
configured, so it cannot reach a log line; the buffer holding it is zeroized on
drop, so it does not linger in a freed heap page. The second matters because a
registry is cloned by every derived session — a reproducible read, a scoped one —
and each clone is another copy of a cryptographic key.

That is hygiene, not a claim the key exists in one place. It does not reach the
buffer you passed in, an environment variable the process still holds, or the key
schedule `hmac` derives per tombstone. Object-store credentials are redacted but
**not** wiped, and the difference is deliberate: an explicit S3 key is forwarded
into the object store's own client, which holds it for the life of the process, so
wiping MeterStore's copy would imply a protection that does not hold. The
credential chain — environment, instance role, IRSA — is what actually keeps a key
out of the process.

## The duty on a clock

`erase_subject` answers an Article 17 request. Nobody files § 60 Abs. 6 — it comes
due on its own, so it is a job rather than a call:

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
  indexed `DELETE` against the registry: no scan, and no read mode that can skew
  it. A due-date taken from `max("from")` over the sweeping session would be —
  under `Historical` a customer metered daily looks last-seen at the final
  archived interval, old enough to erase and still live, irreversibly.
- **Idempotent**, so it runs on a schedule and a re-run writes no second audit row.
- **`CalendarYears(3)` is not `now - 3 years`.** The statutory clock starts at the
  *Schluss des Kalenderjahres*, so a value collected on 2 January 2025 comes due on
  31 December 2028. The rolling spelling would erase it a year early — the
  direction that destroys data still inside its retention period.
  `Retention::Rolling(d)` exists for the earlier "no longer necessary" trigger,
  which is a business decision this crate has no view on.
- **No suppression tombstone.** Suppression exists so an Article 17 erasure
  survives a broker replay; an expiry is not a request to stop processing, and a
  subject whose 2021 epoch expired must still be registrable for 2027.

`store.anonymise_before(cutoff, …)` and `catalog.anonymise_before(cutoff, …)`
are the same sweep run once, for a deployment that schedules it elsewhere — and
the same operation as each other, since the registry is deployment-wide and
neither reads a reading.

This is also the answer to "there is no partial data expiry". The statute does not
require deleting rows; it requires that the values stop being personal, and that
is `O(1)` without rewriting a byte.

## Configuring it

A table declaring a `subject_column` needs a registry to resolve against, and a
deployment declaring one without a registry is **refused at startup** rather than
at the first erasure request:

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
same pool as the hot tier — because the mapping is deployment-wide, and because
erasure needs storage where deletion is real and in the same database as the
application's own tables. A configuration whose tables declare no subject column
gets no registry and creates none of its tables.

`erasure_secret` is what turns the suppression list on. It is optional because
the key must outlive every erasure and is not recoverable from the database:
losing it exposes nothing and silently disables suppression, which is the one
failure this crate cannot report — so a deployment that cannot yet hold a key
securely is better off knowing suppression is off than inventing one it will
lose.

`meterstore erasures` reads the audit trail from a shell. There is deliberately
no `meterstore erase`: an Article 17 request usually reaches an application's own
tables too, and those must succeed or fail in **one transaction** with the
mapping — which `SubjectRegistry::erase_in` gives and a CLI invocation cannot.

## The trail as evidence

Every row records **which duty it discharged** — `request` for Article 17,
`retention` for § 60 Abs. 6. They are different legal bases and are asked about
separately, and `reason` is free text a deployment writes for itself, so a sweep
that had stopped running would otherwise be invisible behind the requests that
kept arriving. The same value is the `trigger` attribute on
`meterstore.subjects_erased`, so the counter and the trail cannot disagree.

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
non-positive limit is **refused** rather than returning nothing: an empty result
reads as *"nothing was erased"*, which is the one answer an audit query must not
give by accident.

## What it requires of the deployment

**The pseudonymous reference must be the *only* link.** A 15-minute series is
potentially re-identifiable by singling out, so if another system holds the same
series against a name, deleting this mapping achieves nothing. Erasure is a
property of the whole estate; this module guarantees its own part.

**Granularity is your choice**, and it is global — see above: a Marktlokation
outlives its occupants, so `(tenant, MaLo)` or an occupancy period is usually what
a subject means.

## General security posture

| Concern | Approach |
|---|---|
| Credentials | Environment-variable interpolation; never logged, `Debug` redacts a connection URL to its scheme |
| SQL injection | Whitelisted expression grammar, typed parameter binding. Caller values are never concatenated into SQL; declared column names are restricted to plain identifiers, since an identifier cannot be parameterised |
| Caller-supplied SQL | `query`, `sql` and `stream` plan without running and refuse anything that is not a query. DataFusion's surface is wider than `SELECT`: `CREATE EXTERNAL TABLE … LOCATION` reads any path the process can, `COPY … TO` writes one, and an external table over the warehouse's Parquet walks past a scoped session because it never touches the provider that enforces the scope. See [Confining a session](@/docs/querying.md#confining-a-session) |
| Least privilege | `SELECT` plus ownership of its own tables for `DETACH`/`DROP`. No `SUPERUSER` |
| Transport | TLS for PostgreSQL and object storage; downgrade requires explicit opt-in |
| At rest | Object-store SSE. Per-subject encryption is deliberately not used |
| Serving surfaces | Read-only — the endpoints refuse mutating calls *and* the query path refuses non-query statements — and both hand back a service rather than binding a port, so authentication is yours to supply |
| Supply chain | `cargo-deny` in CI with a pinned lockfile. Every licence and advisory exception carries a written reason and a revisit condition, because an unexplained ignore looks like a check that passed |

Nothing here is legal advice. Deployments take their own.
