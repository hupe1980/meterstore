# Changelog

All notable changes to `meterstore` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

The crate is **unpublished** and pre-1.0. Until the first release every version
is a hard cut: breaking changes carry no deprecation shim, and the SQL schema
changes in place rather than through a migration.

## [0.12.0] — 2026-09-07

An audit pass. Two correctness defects that produced a plausible wrong number
rather than an error, a retention policy weaker than its name, a catalogue
backend no configuration file could reach, and a dependency graph that carried
more than it declared.

### A report ending mid-interval invented a surplus

`expected_in_day` clipped a partly-covered balancing day by dividing the covered
duration by the interval step. The scan underneath selects rows whose own `from`
is in the range, so the expectation has to be a count of **interval starts**, and
the two part whenever the range ends off the grid: over `[00:00, 00:07)` the
division expects nothing while the interval starting at 00:00 is inside the range
and counted in `actual`. Every report whose `to` was `now()` rather than a day
boundary could show a `surplus` of one on its final day — a finding that never
happened, in the report whose whole value is that its findings did.

The count is now `ceil((hi - day_start)/step) - ceil((lo - day_start)/step)`,
clamped to the day. Day-aligned ranges are unaffected, which is why this hid.

### Predicate extraction could narrow at the edge of the calendar

`next_stored_instant` fell back to its input when there was no next representable
instant, which made a `<=` bound exclusive of the value it includes and an `=`
bound empty. Unreachable in practice, and the wrong direction for a module whose
stated contract is that it may only ever err by widening. It returns `Option` now
and the callers turn `None` into *no bound*.

### `Retention::Rolling` expires whole years

Not a behaviour change — a documentation one, for a compliance setting whose
effect was much weaker than its name. The sweep deletes by `epoch`, and an epoch
is a year, so `Rolling(30 days)` swept on 15 January 2028 expires epochs before
2027 and keeps a value collected on 2 January 2027. It never erases early, which
is the half that matters for an irreversible operation, but nothing said so.
Documented, and pinned by a test that also asserts the never-early property.

### S3 Tables is reachable from a configuration file

`S3TablesCatalog` was a constructor with no `CatalogKind`, so an S3 Tables
deployment could be assembled in Rust and not described in TOML — which put every
CLI verb out of its reach, the CLI having no other way in. `catalog = "s3tables"`
now exists, taking the table bucket ARN in `warehouse` and no `uri`; the
warehouse-scheme check is skipped for it, because an ARN has no scheme.

### `meterstore audit` checks a declaration against the data

`MeterStore::audit_attribute_column` and `audit_attribute_columns`, and an `audit`
verb over them. Declaring a tenant discriminator as an *attribute* rather than an
identity column is legal, the writes succeed, and nothing raises an error — two
identities then share a merge key and one silently supersedes the other. Nothing
at write time can tell that from a column that genuinely is a fact about a
reading, so the question is asked of the stored rows: for how many merge keys does
the column take more than one value? A report rather than a verdict, since a
correction restating an attribute is the same shape at a smaller scale.

### The dependency graph declares what it carries

- **`iceberg-storage-opendal` was taking its default features**, so `opendal-s3`
  and the `reqsign` AWS stack compiled into every build — while the manifest
  claimed the cloud object stores were opt-in. They are now.
- **`sqlx` was taking its defaults** (`any`, `macros`, `migrate`, `json`), none of
  which this crate uses. Thirteen packages left the graph.
- **`sqlx/time` moved to `sqlx-postgres/time`.** `sqlx`'s own `time` feature names
  `sqlx-sqlite?/time`, and naming an optional dependency puts it in the resolve
  graph, where cargo's `links` uniqueness check runs — enough on its own to stop a
  workspace that also links an embedded SQLite from resolving.
  `sqlx-postgres/time` is `["dep:time", "sqlx-core/time"]` and mentions no SQLite;
  feature unification makes the two identical for this crate's code.
- **`iceberg-catalog-sql` is behind a `sql-catalog` feature**, in `default`. It
  reaches the same SQLite driver, so `default-features = false` plus another
  catalogue is now an escape hatch such a consumer can take.
- `sqlx-postgres` joins the single-sourced dependency gate.

### The `§N` markers are gone

166 of them across 33 source files cited a single-file predecessor of the design
notes and resolved to nothing — 120 in user-facing rustdoc, one in an error
message. Two were indistinguishable at a glance from the BDEW clause references
beside them. Every internal marker is now the thing it referred to, spelled out;
every `§` that remains cites a **named external** document, and the surrounding
sentence names it.

## [0.11.0] — 2026-09-06

Two defects in the subject registry, reported from an integration of 0.10 — one
made a class of gas reading impossible to store, the other left an Article 17
request answerable only by guesswork — and, from the audit they prompted, a
suppression key that could never be rotated, three compliance actions whose record
was thinner than the action, and two integrity floors that were convention rather
than check.

### A gas reading in the first hours of the year could not be stored

`register_subject(natural_id, at)` minted the reference for
`retention_epoch(at)`, the **local calendar year**. The write path validated it
against `balancing_day(at, sparte).year()`, the **balancing** year. For
electricity those always agree; for gas they disagree for six hours a year,
because the Gastag runs 06:00 to 06:00 — a reading at `2026-01-01T00:00Z`
(01:00 Berlin on 1 January) is balanced on Gastag **2025-12-31** while its local
year is **2026**.

The reference `register_subject` minted for that instant was the only one the
documented API could produce, and the write refused it — with an error naming
the very call that could not satisfy it:

> subject reference `s2026_…` belongs to retention epoch 2026 but this reading is
> balanced on 2025 … register one per year with
> `register_subject(natural_id, interval.from)`

There is now **one rule**, `erasure::retention_epoch(at, sparte)` — the balancing
year — and both the mint and the check are that call, so they cannot drift again:

- `SubjectRegistry::register`, `SubjectRegistry::lookup` and
  `MeterStore::register_subject` take a `Sparte`. `register_in_epoch` and
  `lookup_in_epoch` are the primitives for a caller that already holds the epoch.
- A Gastag spanning New Year has intervals in two local years but **one**
  balancing year, so an MSCONS Lastgang for that day stays one delivery with one
  reference. On the calendar rule it would have had to be split.
- Those six hours expire with 2025 rather than 2026 — a year earlier than the
  calendar reading of `[MsbG § 60 Abs. 6]` requires, which is the compliant
  direction, since three years is a ceiling.
- The **sweep** keeps the plain calendar year: the `epoch` column is one integer
  shared by every commodity, so there is no `sparte` to ask with, and taking the
  gas boundary there would push every epoch — electricity included — a year
  later.

### A natural identifier's epochs could not be enumerated

A reference belongs to one collection year, which is right. But `SubjectRegistry`
exposed `lookup(natural_id, at)` and nothing answering *"which epochs does this
identifier still have references for?"* — so honouring an Article 17 request, which
is about a person rather than a year, meant guessing a window and probing once per
candidate year, or reading `meterstore_subject_map` across the crate boundary. A
guess one year short reports a subject as fully erased while a mapping survives.

- `SubjectRegistry::registrations`, `epochs` and `references` enumerate what an
  identifier still has, oldest epoch first. `SubjectRegistration` is the row.
- `SubjectRegistry::erase_all(natural_id, …)` and `erase_all_in` take the
  identifier a request actually names. `MeterStore::erase_subject_by_id` and
  `MeterCatalog::erase_subject_by_id` are the store- and deployment-level forms,
  beside `subject_epochs` and `subject_registrations` on both.
- With a suppression key configured, `erase_all` honours a request for an
  identifier the deployment holds **no** mapping for — a request that arrived
  before the delivery did. There is no linkage to destroy, so what it writes is
  the tombstone, and the later registration is refused. `ErasureRecord::subject`
  is therefore `Option<SubjectRef>`, and `ErasureRecord::epoch()` reads the year
  off it.
- `meterstore_erasures` gains a `BIGSERIAL` primary key and `subject_ref` becomes
  a nullable `UNIQUE` column, so such a row can exist at all.

### Registration and erasure raced

`register` checked the suppression list and then inserted; `erase` wrote the
tombstone and deleted the mapping. Neither locked anything the other held —
registration is about a row that does not exist yet — so the interleaving that
ends with a live mapping for a subject the audit trail says was erased was
possible, and nothing downstream could report it. Both paths now take a
transaction-scoped advisory lock on the identifier before any row lock, so the
order is total and they cannot deadlock. `erase_in` needs a transaction rather
than a bare connection for it, and says so.

### Two write-path costs

- The store checked each subject reference in a batch with its own query. A
  delivery carries a reference per measuring point, so a set-membership test grew
  with batch size; `SubjectRegistry::resolvable` answers for the whole batch in
  one round trip, and deliberately does not hand back the identifiers behind it.
- The retention sweep issued a `SELECT … FOR UPDATE` and then a `DELETE` and an
  `INSERT` per row, holding a transaction open across all of them. It is now one
  `DELETE … RETURNING` and one bulk `INSERT` — the one operation here whose row
  count is unbounded, since a deployment's whole 2021 comes due on one January
  morning.

### The suppression key could not be rotated

The documentation said the key "must outlive every erasure and is not recoverable
from the database" and stopped there. The consequence it did not draw is that
such a key can never be **replaced**: a tombstone is `HMAC(key, identifier)` and
the identifier was destroyed in the same transaction that wrote it — deliberately,
since keeping it would defeat the erasure — so there is no input from which to
recompute the tag under a new key.

Changing the secret therefore stops every erasure recorded before the change from
being recognised, and re-registration re-opens for all of them at once. Nothing
reports it: the tombstones are still there and simply stop matching.

The registry now holds a **key ring**.

- `SubjectRegistry::with_erasure_keys(pool, &[current, ..retired])`. `keys[0]`
  writes every new tombstone; every key is checked on a lookup.
  `with_erasure_secret` is the one-key case of it.
- `[privacy] retired_erasure_secrets` is the configuration half. Rotation is
  additive: move the outgoing key there, put the new one in `erasure_secret`.
- Retiring is not destroying. A key stays in the ring for as long as the erasures
  it recorded must stay suppressed — indefinitely, for Article 17. What rotation
  buys is that a key's window as a *writing* key is bounded, which is what a
  compromise of it costs.
- Retired keys are held to the same 32-byte floor, because it is the **older**
  tombstones a retired key covers. A ring of retired keys with no
  `erasure_secret` is refused at startup as the half-finished rotation it is:
  every erasure from then on would record no tombstone at all.
- `lift_suppression` clears the tombstone under every key, so an identifier is
  genuinely liftable rather than half-lifted.

### Lifting a suppression left only a log line

It reverses a compliance decision — it lets an identifier be registered again
after a certified erasure — and the only record of it was a `warn!`. A reviewer
reading the trail saw an erasure, then a live registration for the same person,
and nothing in between to say who authorised the middle step.

`meterstore_erasures` gains `lifted_at`, `lifted_by` and `lift_reason`, written
onto the rows the lift clears and returned as `ErasureRecord::lifted`
([`SuppressionLift`]). `lift_suppression` takes `now`, like every other operation
in the crate — the clock is a parameter here, and this was the one place it was
not.

### The trail could not tell a statutory expiry from a request

The code has carried the distinction since the erasure counter existed; the audit
row did not. `reason` is caller-supplied free text and cannot carry it: two
deployments spell one duty differently, and a § 60 Abs. 6 sweep that had stopped
running was invisible behind the Article 17 requests that kept arriving.

- `ErasureTrigger` — `Request` or `Retention` — is now a column on every erasure
  row **and** the `trigger` attribute on `meterstore.subjects_erased`. One type,
  so the counter and the trail cannot come to disagree.
  `observe::TRIGGER_REQUEST`/`TRIGGER_RETENTION` are gone.
- `SubjectRegistry::erasures` takes an `ErasureQuery`: a half-open `since`/`until`
  period, a trigger, and a limit. A backwards period or a non-positive limit is
  **refused** rather than returning nothing — an empty result reads as "nothing
  was erased", which is the one answer an audit query must not give by accident.
- `meterstore erasures` grows `--since`, `--until` and `--trigger`, and prints the
  trigger and any lift.
- `MeterStore::erasures` and `MeterCatalog::erasures` expose the trail without
  reaching for the registry, beside `is_subject_suppressed` and
  `lift_subject_suppression`.

### A refused re-registration was logged and not counted

Every one is a pipeline handing the store an identifier from before an Article 17
erasure — a broker redelivering, a reprocessing job on an old offset, a nightly
import from a system that never learned. It is refused, so nothing is re-linked;
what it reports is that something upstream still carries data it should have
dropped. `meterstore.registrations_suppressed` is that alarm, and zero is the
expected reading.

### Two integrity floors were convention rather than check

- **A reference's shape.** `SubjectRef::new` accepted `s2026_abc`. The token must
  now be at least [`MIN_REFERENCE_TOKEN_CHARS`] (22) characters from
  `A-Z a-z 0-9 . _ -` — hex, base64url and a UUID all clear it. Nothing can tell
  128 random bits from a padded counter; what this rules out is the failure that
  happens, a pipeline with no reference to hand putting a customer number in the
  column. That resolves cleanly, looks exactly like a pseudonym in every table
  that stores it, and is a re-identification path that survives erasure.
- **The epoch, written twice.** `meterstore_subject_map` carries a `CHECK` that
  the reference's own `s<year>_` prefix matches its `epoch` column. A row where
  they disagreed would be swept on one year while refusing readings from the
  other, with both halves looking well formed.

### `create_tables` was silent about a registry it could not use

`CREATE TABLE IF NOT EXISTS` is idempotent and therefore silent: against a
database holding an earlier shape of these tables it created nothing, reported
success, and the divergence surfaced later as `column "trigger" does not exist`
— at an erasure, which is a compliance operation, from an error saying nothing
about what to do. It now checks the columns it is about to write and says to drop
and recreate the two tables, at the setup step where that can be acted on.

Recreate both registry tables; the crate is unpublished and the schema changes in
place. The mapping is rebuilt by re-registering; the audit trail is not, so export
it first if the deployment has erased anything.

[`SuppressionLift`]: https://docs.rs/meterstore/latest/meterstore/erasure/struct.SuppressionLift.html
[`MIN_REFERENCE_TOKEN_CHARS`]: https://docs.rs/meterstore/latest/meterstore/erasure/constant.MIN_REFERENCE_TOKEN_CHARS.html

## [0.10.0] — 2026-09-05

An audit found three ways a query could return a wrong number and one way the
retention duty could not be discharged at all. All four are the same kind of
defect — a decision taken from state read at one moment and served from state
read at another — and none would have raised an error.

`metering` 0.23 landed in the same release, and its replacement of an invented
vocabulary with the market's own fired the stored-shape risk this crate had
written down but never seen.

### A query planned before an archival run came back a window short

**The serious one.** A query decides its tier split from the tiering watermark it
reads when it is *planned*, and reads the tiers when it *executes*. Archival
between those two moments moved a window across a boundary the plan had already
committed to: the cold half was planned as `from < W` and excluded the day Iceberg
now held, the hot half asked PostgreSQL for it, and archival had just dropped the
partition.

A `SELECT count(*)` over three days of readings, planned before an archival run
and executed after it, returned **96 of 288 rows** — with no error, on the class
of long-running settlement query most likely to overlap a maintenance cycle. The
write path already re-routed an append the boundary moved under; the read path had
no equivalent guard.

The purge is now **deferred**. Archival leaves the partition detached — invisible
to writers, still read by any plan made before the advance, excluded by predicate
from every plan made after it — and a later cycle reclaims it once the commit that
moved the boundary is older than the new `reader_grace` (default 15 minutes,
settable per table and in `[tables.archival]`). The crash-recovery path is now the
normal path, so it is exercised on every run rather than only after a crash.

`ColdStore::append_and_commit` takes `now` and records it in the snapshot summary
as `meterstore.archived_at`. The grace is measured on the caller's clock rather
than on Iceberg's own commit timestamp, because the crate's rule is that the clock
is a parameter — and a safety property read from two clocks disagrees exactly
where a test drives months in milliseconds.

### A query running during a detach lost the window being archived

Found by the new concurrency suite on its first run, and the third instance of one
shape. Archival detaches a partition before reading it, and the hot scan read the
parent table **plus whatever was currently detached from it**. Those are two reads
of one catalogue: a detach landing between them puts the partition in neither, so
a count over a fixed, fully written range returned **3840 rows of 4608** — one day
short — while archival ran beside it.

The parent is no longer scanned at all. The partitions are enumerated once and
each is read by name, because a partition relation is stable under a detach: it
keeps its name, its rows and its identity, and only its parentage changes. The
special case is gone rather than patched, each row is read exactly once, and a
partition's upper bound comes from the next partition's start rather than the
configured step, so a gap left by an idle stretch is over-estimated rather than
skipped.

### Merge elision could double-count a correction

The same shape, one layer down. `IcebergTableProvider::scan` pins the snapshot it
loads; `version_stats` loads the table separately. Read in that order, a late
correction committing between the two was present in the files the scan would read
and absent from the evidence the elision decision was made on — so resolution was
skipped over files that needed it and the corrected interval came back twice.

The statistics are now read **after** the scan is built, so they come from a
snapshot at least as new as the one it pinned. A file they know about and the scan
will not read can only add overlap, and overlap only pushes towards resolving —
the direction where being wrong costs a window function rather than a restatement.

### § 60 Abs. 6 comes due per value, and the sweep worked per subject

`[MsbG § 60 Abs. 6]` runs on *"der jeweilige Messwert"*: each value, three years
after the end of the calendar year it was collected in. One reference covering a
subject's whole history cannot express that. The sweep erased a subject only once
**every** reading it explained had passed the cutoff, so a customer who stayed
connected kept a decade of values attributable indefinitely — and the due-date came
from `max("from")` over the session running the sweep, which made an irreversible
operation depend on which rows a read mode could see.

The unit of erasure is now `(subject, collection year)`:

- `SubjectRegistry::register(natural_id, at)` and
  `MeterStore::register_subject(natural_id, at)` take an instant **from the data**
  and mint a reference for that collection year. The year is part of the
  reference (`s2026_9f3c…`).
- A reference used on another year's readings is **refused at the write**. Without
  that check the guarantee would be a convention: the column stays well-formed,
  the reference resolves, and the only symptom is a sweep that never comes due.
- `anonymise_before` deletes mapping rows whose year has passed the ceiling. It
  reads **no table** — one indexed `DELETE` — so no read mode can skew it and none
  has to be refused. `MeterStore::anonymise_before` and
  `MeterCatalog::anonymise_before` are now the same operation.
- `erase_subject` still answers a request about a *person*: it destroys every
  epoch behind the reference it is given and returns one `ErasureRecord` per year,
  so `erase`, `erase_in` and `erase_subject` return `Vec<ErasureRecord>`.
- `meterstore_subject_map` gains an `epoch` column and is keyed by
  `(natural_id, epoch)`. Recreate it; the crate is unpublished and the schema
  changes in place.

`MeterStore::subject_last_seen` and the cross-table maximum behind the catalogue
sweep are deleted — the hazard they guarded was created by taking the due-date
from readings.

### A result's reported boundary was read separately from the one it used

`QueryResult::watermark()` says which boundary an answer was computed against, and
it was read *before* planning while the tier split was decided *during* it. An
archival commit in between made the label name a boundary the answer was not
computed against; over a catalogue every provider read its own, at its own moment,
and no two were guaranteed consistent.

The boundaries are now read once and travel with the plan
(`planner::PlannedWatermarks`, placed on the session config for one physical
plan), so the label is the thing that was used — and a statement costs half as
many catalogue round trips as before.

### A replica that lost the archive lease still expired snapshots

The lease is taken and released inside `archive`, and expiry ran after it — so
every replica did it, on its own clock, racing for a compare-and-swap only one can
win, in the one step that re-stamps the tier boundary onto the current snapshot.
The documentation said the losers "stop"; they did not. Expiry now runs only for
the replica that won the lease. Reading `system.tables` still runs everywhere,
because it mutates nothing.

### `metering` 0.23

`SubstitutionReason`'s variants are all new: EDI@Energy publishes the list
(`STS+Z40`, 28 reasons, MSCONS MIG 2.4c) and the crate had seven of its own
devising. `MeasurementSource::AutoSubstitute` carries one, so the payload written
into `source_detail` changed spelling — a **stored-data** break rather than a
wire-format one, since rows already written stop decoding. Unpublished, so this is
a hard cut.

What caught it was a compile error in a fixture that happened to name the renamed
variant. That is luck, not a check, so the whole `source_detail` payload of every
`MeasurementSource` variant is now a **literal in the test** rather than only the
outer `source_kind` tag. A nested vocabulary — `AutoSubstitute` holds a
`SubstitutionReason`, `VirtualMeter` a `VirtualMeterKind` — can be renamed
upstream while both columns keep agreeing with each other and every round-trip
test keeps passing, because they all read and write through the same impl.

The testkit's interval generator uses `MeterInterval::measured`/`with_obis`
rather than a struct literal, so a field added upstream cannot silently acquire a
default in generated workloads.

### Quality is queryable

The `quality` column was opaque in SQL, so a consumer asking the two questions
that get asked of it had to answer them itself:

| Function | |
|---|---|
| `quality_is_billable` | § 60 Abs. 2 MsbG, asked of the column rather than spelled `quality IN ('MEASURED', 'SUBSTITUTED')` |
| `quality_is_provisional` | Whether the value is still expected to change |
| `quality_market_code` | The MSCONS `QTY` Mengen-Qualifier — `220`, `67`, `187`, `Z18`, `20` |

`quality_market_code` returns **null** for `CALCULATED`, `CORRECTED` and
`UNKNOWN`, which is the useful half: those have no qualifier of their own, so the
rows a message writer must resolve before transmitting are exactly the ones it
cannot answer for. A value outside the code list is an error rather than a null —
the column is written from `QualityFlag::as_str` and constrained to that list, so
anything else means something wrote the warehouse that should not have.

Completeness already delegated billability upstream rather than keeping a local
copy of the statute; this is that delegation reaching SQL.

### Also

- `reader_grace` defaults to **1 hour**, not the maintenance interval. Matching
  the interval leaves a query exactly one cycle of protection, which is thin for
  the settlement scan this store exists for. It is reported in `system.config`,
  beside the other settings that are each valid alone and wrong beside one
  another.
- `MeteringWorkload::version` sets the version a generated delivery carries, so a
  test can emit a *sequence* of restatements rather than one. The concurrency
  suite needs it; a deployment running the testkit over its own configuration may
  too.
- `TieringWatermark::advance_to` takes the table name, so the
  `InvariantViolated` it raises names the table an operator is being paged about
  instead of `<unknown>`.
- `align_to_step` returns `Result`. An instant it cannot align used to come back
  unchanged — not on the grid, from the function whose job is to put it there,
  with the symptom surfacing as an archival window naming a partition nothing
  creates.
- `ScanSpec` carries the partition granularity, so a hot scan skips a detached
  partition that cannot hold a row in range rather than scanning it to find out.
- The prelude no longer re-exports `CHECK_VALUES_KEY`, `VALUE_CHECK_KEY`,
  `declared_value_check`, `AUTHORITATIVE_ATTEMPTS` or `RETENTION_LABEL`. They are
  Arrow metadata keys and retry counts, still reachable at their own paths and not
  things a deployment types.
- `describe` reported the **logical** plan's schema while `query` and `stream`
  reported the physical one, so the surface that tells a Flight SQL client what a
  statement produces could disagree with the surface that produces it. It now
  comes off the same physical plan, which also halves what a description costs.
- `expected_in_day` divided by an interval count it had not yet checked for zero.
  Unreachable today, and a panic in a query path is the wrong place to discover
  that it stopped being unreachable.
- The schema-evolution documentation said a rename costs nothing because Iceberg
  matches by field id. This crate compares by **name**, so a rename reads as a drop
  plus an add — free for a nullable attribute column, a quarantine for a required
  one. It also does not *perform* schema evolution at all; it detects drift and
  halts, and the documentation now says which changes it will accept once an
  operator applies them out of band.

## [0.9.0] — 2026-08-31

A configuration setting that could not be deployed at all, a multi-tenant catalog
that could not be confined, `metering` 0.22, and the identifier surfaces that
stopped short of where their own argument reaches —
a checked column that could only be an EIC and could not say *which* EIC, and
three read builders that could narrow to a Marktlokation but not to the meter
under it. Plus the guards `metering`'s own release round argued for: this crate
writes two JSON columns under a decades-long retention obligation and enables
`metering/serde` for everyone downstream of it, and both of those facts were
asserted in comments.

### A `subject_column` in a configuration file could not be started

The one that mattered. `subject_column` parses, validates, has a documentation
page and a CLI flag that names it as a prerequisite — and nothing on the
deployment path built the `SubjectRegistry` it resolves against, which
`MeterStoreBuilder::build` refuses to go without. `meterstore check` passed the
file, and then every subcommand that opens a store — `create`, `status`,
`archive`, `query`, `maintain` — failed:

```
a subject column is declared but no subject registry was provided: the
references would resolve to nothing and erasure would have no mapping to destroy
```

The message is right and the fix it implies was unreachable: no configuration
file could supply a registry. `meterstore maintain --anonymise-after-years 3` —
the § 60 Abs. 6 MsbG sweep, documented as needing exactly this — could therefore
never be run from a configuration file at all.

`Settings::connect` now builds **one** registry for the deployment whenever any
table declares a subject column, over the same pool as the hot tier, and hands it
to each table that asked for it. One rather than one per table because the
mapping is deployment-wide: two tables registering the same natural identifier
share a `SubjectRef`, and a single erasure has to unlink both. A file that
declares no subject column gets no registry and creates none of its tables.

A `[privacy]` section carries the one thing a registry needs that the pool does
not:

```toml
[privacy]
erasure_secret = "${METERSTORE_ERASURE_SECRET}"   # ≥ 32 bytes; optional
```

That key is what turns the **suppression list** on. Without it erasure works and
does not stay worked — once the mapping is deleted nothing distinguishes an
erased identifier from one never seen, so a pipeline replaying old messages
registers a fresh reference and silently re-links the subject. It is optional
rather than required because the key must outlive every erasure and is not
recoverable from the database: losing it exposes nothing and silently disables
suppression, which is the one failure this crate cannot report, so a deployment
that cannot yet hold a key securely is better off knowing than inventing one it
will lose.

Its length is checked by `meterstore check`, with no database — a key too short
to resist a brute-force turns the suppression tombstone into the oracle it exists
to prevent, and discovering that at the first process start means discovering it
in a deploy rather than in CI. `MIN_ERASURE_SECRET_BYTES` is the one number, so
the file front end and `SubjectRegistry::with_erasure_secret` cannot come to
disagree.

`Settings::to_toml` now says outright that it writes the resolved secrets rather
than the `${VAR}` placeholders. That was already true of the connection URL's
password and is what "round-trips" has to mean; the redaction this crate applies
is on `Debug`, which is where a configuration reaches a log by accident.

### The erasure trail was reachable only from Rust

"We deleted it" is not evidence, and an auditor's question does not arrive in a
Rust file.

```bash
meterstore erasures --limit 50
```

One list rather than one per table, because the registry is. A configuration
whose tables declare no `subject_column` holds no mapping and says so, rather
than printing an empty list that reads as *"nothing has been erased"*.

**There is deliberately no `meterstore erase`**, and this is the reason the
library already gives for `erase_in` existing: an Article 17 request usually
reaches an application's own tables too — billing periods, quality assessments,
substitute-value logs — and those must succeed or fail *in one transaction* with
the mapping. A CLI invocation commits its own and cannot enclose them, so the
failure mode would be the worst kind: a subject reported as erased whose derived
rows survived. The duty that comes due on its own is a different thing, and
`meterstore maintain --anonymise-after-years` does run it.

### A Bilanzkreis was checked and a Marktpartner-ID was not

`eic_column` made one argument — an identifier is *parsed rather than trusted* —
and applied it to one identifier. The columns a German deployment actually
declares are `bilanzkreis` and `bilanzierungsgebiet` (EIC), `lieferant`,
`messstellenbetreiber` and `netzbetreiber` (a thirteen-digit Marktpartner-ID),
and the underlying Marktlokation of a Kaskade or the parent of a Tranche (an
eleven-digit MaLo-ID, check digit and all). Two of those three groups had no
declaration at all and were plain `Utf8`, in a crate whose front page argues that
they should not be.

`ValueCheck` is the scheme, and `checked_column(name, check, nullable)` declares
one:

```rust
.attribute_column(checked_column("bilanzkreis", ValueCheck::Eic, true))
.attribute_column(checked_column("lieferant", ValueCheck::Bdew, true))
.attribute_column(checked_column("unterliegende_malo", ValueCheck::Malo, true))
```

```toml
extra_columns = [
  { name = "bilanzkreis", check = "EIC" },
  { name = "lieferant",   check = "BDEW" },
]
```

**Each stops somewhere different, and the type says where** rather than leaving a
reader to assume they are all equally strong:

| `check` | Length | Shape | Arithmetic | Canonicalises |
|---|---|---|---|---|
| `EIC` | 16 | `0-9 A-Z -`, letter at 3, check character not `-` | **check character** | trim, uppercase |
| `MALO` | 11 | digits, first not `0` | **check digit** | trim |
| `MELO` | 33 | 2 letters, 6 digits, 25 alphanumerics | none exists | trim, uppercase |
| `BDEW` | 13 | digits | **deliberately not checked** | trim |

The two with arithmetic are the two where a transposition is detectable while the
delivery that carried it is still in hand. `MELO` earns its declaration on the
**casing** — storage holds the uppercase form, so a Messlokation arriving in two
casings on an identity column would be two readings that never supersede each
other — and `BDEW` on the length and alphabet, which is the whole of the rule
that can be enforced: §2.3 of the Bildungsvorschrift exempts GS1-issued GLNs,
which use a different check-digit procedure and are legitimate Marktpartner-IDs.
That is the same carve-out for which `version_scope` deliberately does not check
its operator's digit, and it is now stated in the type rather than only in the
one place it was already reasoned about.

The shape patterns are anchored on the **stored** form, so a row written in lower
case by another writer is refused rather than becoming a second spelling. They
live on `ValueCheck` and the DDL reads them from there, so the hot table's `CHECK`
and the scheme cannot drift apart; `tests/it/checked_columns.rs` evaluates every
one of them in PostgreSQL's own POSIX engine, on the pattern the server is
actually holding.

**Breaking.** `eic_column` and `VALUE_CHECK_EIC` are gone —
`checked_column(name, ValueCheck::Eic(None), nullable)` and
`ValueCheck::Eic(None).as_str()`. One spelling per value, which is the same rule
this crate applies to the data.

`ValueCheck` is deliberately not `#[non_exhaustive]`, unlike `Error`: a scheme
added here is a scheme a caller matching on it has to decide about, and a `_` arm
would let that decision be skipped silently.

### `WHERE NOT complete` reported unjudgeable channels as fine

The completeness report's whole claim is that *an aggregate over an incomplete
month must not look like one over a complete month*. Its own documented SQL
example did exactly that.

A channel that declares no resolution — or a calendar one like `P1M`, which has no
fixed count within a day — has nothing to compare against. It reports
`missing = 0` and `surplus = 0`, and therefore `complete = true`, without anything
having been checked. Rust has had `Completeness::is_measurable` for precisely this
since the report existed; the **table function did not publish it**, so the two
surfaces disagreed about what had been verified and

```sql
SELECT * FROM meter_completeness(…) WHERE NOT complete;
```

silently dropped every channel nobody could judge.

`meter_completeness` now publishes a `measurable` column beside `complete`, and
the documented query is `WHERE NOT complete OR NOT measurable`. The CLI's JSON
summary gains `channels_unmeasurable` for the same reason — the monitoring check
the CLI page prints was `jq -e '.channels_incomplete == 0'`, which passes a month
nothing was verified about; it is now conjoined with the new counter. The
`--format table` output already flagged such a row in its `NOTE` column, which is
what made the omission visible on the other two surfaces.

### A retention sweep on a restricted session erased a live subject

The most serious of this round, and the only one that destroys data.

§ 60 Abs. 6 MsbG erases a subject once every reading it explains has passed the
ceiling, so the sweep computes each subject's **latest reading** and compares it
to the cutoff. That latest reading is `max("from")` over the session running the
sweep — and a session in `Historical`, `Operational`, `as_of` or `as_known_at`
mode reads a view that is deliberately not current.

Under `Historical` the hot window is invisible. A subject metered daily therefore
looks last-seen at the final **archived** interval: old enough to erase, and still
live. Under `as_known_at` every reading recorded after the ceiling is invisible,
with the same effect. Erasure is irreversible and the audit row records that it
happened, so nothing downstream reports it.

Reproduced before fixing, with the guard removed — one store, one subject, one
cutoff, two sessions:

```
UNRESTRICTED ERASED: 0          # correct: the subject has a current reading
PINNED ERASED: 1
LIVE SUBJECT STILL RESOLVES: false
```

`anonymise_before` and the catalog-wide sweep now require a session reading
current best knowledge, the same guard `append` already had. The guard is renamed
from `require_writable` to `require_current_knowledge`, because the rule was never
about writing: it is that an operation whose **decision is a query against this
session** must not run through a restricted one. Archival, snapshot expiry and
`purge_table` stay available on a pinned session for the same reason — none of
them decides anything from the session, they read the tiers directly.

The catalog sweep checks every table **before** erasing anything, since a subject
due-date is the latest reading across the deployment and one table reading a
partial view understates it.

The reproducibility page listed two refused operations; there are five.

### A configuration mistake told a supervisor to retry forever

`warehouse = "s3://…"` in a build without `object-store-s3`, or any unsupported
scheme, raised `Error::Storage` — which `is_retryable` reports as **true**, and
which the CLI maps to exit **75 (EX_TEMPFAIL)**. A supervisor reads that as *"try
again"*, so a misconfigured warehouse restart-loops forever on a mistake that can
never fix itself. `meterstore check` runs this path with no database, so it is
reachable in CI as well as at start-up.

`is_retryable`'s own documentation says false is for "everything describing the
*input*, where a retry loops forever on a message that will never change", and
the setting's own comment already called it "a configuration error". Both are now
`Error::Config`.

### `archive_all` flattened every table's error into one variant

`MeterCatalog::archive_all` wrapped each per-table failure as
`Error::Storage(format!("archiving {name}: {e}"))`. That destroys the variant a
caller matches on — which this crate's error documentation makes a point of — and
makes the result **retryable** whatever it was. `InvariantViolated` is the one
condition the crate is most emphatic must not be retried past, and archival is
exactly where it surfaces.

The error now reaches the caller as it was raised, with the table named in a log
line instead. `verify_invariant` on the same type already did this correctly,
which is what made the outlier visible.

### A multi-tenant catalog had to choose between the join and the boundary

`MeterStore::scoped` confines the **rows** a session can see and
`MeterCatalog::isolated` confines the **relations** it can name. The crate
documentation presented them as the pair a service exposing SQL wants — and the
pair left a hole exactly where a serving surface needs both, because `isolated`
returns a single-table store. A multi-tenant deployment putting a whole catalog
on a socket (which is what Flight SQL serves: it takes any `SqlSurface`, and
`MeterCatalog` is one) had two options, and both were wrong: isolate to one table
and lose the cross-table join that is the catalog's entire reason to exist, or
serve every tenant's rows to every tenant.

`MeterCatalog::scoped(column, value)` is the third corner. Every table is
confined, the predicate is injected into each plan and enforced below the
projection, and the join still plans — with **both** sides carrying their own:

```rust
let confined = catalog.scoped("tenant", tenant).await?;
confined.query("SELECT … FROM readings r JOIN esa_typ2 e USING (malo_id)").await?;
```

**Every table, or none.** A column that is not in some table's merge key is
refused, naming that table, *before any table is confined* — a scope that covered
three tables and silently skipped the fourth is not a boundary, it is a
boundary-shaped object that leaks one relation. A catalog whose tables do not
share an identity column cannot be scoped as a whole, and the error says so and
names the honest alternative. Which columns may scope a session is unchanged and
is `MeterStore::scoped`'s own rule: only a merge-key column partitions readings.

`MeterCatalog::as_known_at` and `::in_read_mode` come with it, and a scoped
catalog stays scoped through both. `as_known_at` is the **one** reproducible read
that can be catalog-wide: `recorded_at` is a row-level column every table carries,
so one instant is one meaningful ceiling across all of them.

**`as_of` deliberately has no catalog counterpart**, and asking for one returns an
error that explains rather than a shrug. It pins an Iceberg *snapshot*, and a
snapshot belongs to one table: there is no id that means the same moment in two of
them and nothing commits two atomically, so a catalog-wide `as_of` would have to
invent a correspondence between per-table snapshots and then call the result
reproducible.

#### And the invariant that made it possible was written four times

`MeterStore::derive`'s own documentation said it was "written once because every
derived session has to carry the *same* things forward, and the failure of
forgetting one is silent". It was written **four** times — `as_of`,
`in_read_mode`, `scoped` and `in_own_session` each reassembled the builder by
hand. Four copies of an invariant whose failure is exactly the one that comment
describes: a store gains a field, three of the four learn to carry it, and the
fourth returns a session that quietly does not — a scoped store whose derived
session dropped the scope answers caller-supplied SQL over every tenant.

`MeterStore::to_builder` is now that one place, and the catalog's rebuild uses it
too, so a derived *catalog* cannot drop what a derived store carries.

### A busy cold-append writer was reported as a stuck one

Two late corrections for the same table serialise on an advisory lease, because
the decision (what is already stored) and the write have to be one step — the hot
tier gets that exclusion from its primary key and Iceberg has none. The loser spun
for **~1.6 seconds** and then failed with

> another process has held the cold-append claim for the whole of 6 attempts. A
> late correction is rare, so this means a writer is stuck rather than busy

Both halves of that reasoning are optimistic. The Iceberg commit is a
compare-and-swap that *retries* under contention, and the Parquet write before it
is sized by the delivery — a redelivered month for one measuring point is not
small. A bulk correction run is also the one time late corrections are **not**
rare, which is exactly when several writers queue. So the error asserted a
diagnosis it could not support, on the workload most likely to produce it.

The budget is now ~14 s, computed from the backoff schedule in a `const` block so
the number in the message cannot drift from the number of sleeps.

**And it was the wrong error.** `Error::Storage` is what an unreachable database
raises; this is a lock a writer declined to queue for, which is
`Error::LockTimeout` — the variant whose documentation already says *"nothing was
changed, so it is safe to retry"*. Both are retryable, so nothing was silently
wrong, but conflating them wakes whoever is paged for a storage fault with
ordinary contention between two corrections.

### A load-sensitive test asserted wall clock instead of the property

`creating_a_partition_does_not_block_on_a_reader` bounded partition creation at
two seconds of wall clock to show it had not queued behind a reader's
`ACCESS SHARE`. That is a weaker restatement of what the call's own `expect`
already proves — the reader is held for the whole call and the DDL lock timeout is
two seconds, so a queued statement comes back `LockTimeout` and there is no third
outcome — and it fails on a loaded machine for a reason that has nothing to do
with locks: the suite runs twenty-odd databases against one server.

The timing bound is gone. In its place is the assertion that actually makes the
positive mean something, and it is deterministic: the spelling this crate does
*not* use — `CREATE TABLE … PARTITION OF` — is run against the same held reader
and must time out on the parent's `ACCESS EXCLUSIVE`.

### An Article 17 erasure was not counted at all

`meterstore.retention.subjects_anonymised` counted the § 60 Abs. 6 MsbG sweep and
nothing else, which left the crate's only irreversible operation half-instrumented:
an erasure performed on a data subject's request incremented no counter anywhere.

Folding the two into that counter would have been worse than the gap, because
their readings are **opposite**. A flat `retention` series means the sweep is not
running; a flat `request` series is an ordinary quarter. Summed, a deployment
whose sweep had silently stopped but which handled the occasional request would
look like one whose sweep was working — and that is the failure the counter's own
documentation is about.

So it is one counter, `meterstore.subjects_erased`, split by a `trigger`
attribute (`retention` / `request`), and both paths reach it. Counted only when a
linkage was **actually** destroyed: a repeat request is auditable and is not a
second erasure, so counting it would report a compliance event that did not
happen. The audit row is unchanged either way — `reason` is what a regulator
reads; the attribute is for whoever holds the dashboard.

### A Bilanzkreis and a Bilanzierungsgebiet were the same column type

They share the alphabet, the length, the issuing office and the check character.
The only thing distinguishing them is **position 3**, the ENTSO-E object type —
`X` a party, `Y` an area. So a `check = "EIC"` column accepted either, and a `Y`
code in the `bilanzkreis` column passed the write path, passed the `CHECK`, and
made every MaBiS grouping over it wrong, with no error anywhere.

`ValueCheck::Eic` now carries an `Option<EicType>`, and `check = "EIC:X"` is it
in a file. Unlike the check character, this half **is** expressible as a regular
expression:

```
EIC     ^[0-9A-Z-]{2}[A-Z][0-9A-Z-]{12}[0-9A-Z]$
EIC:X   ^[0-9A-Z-]{2}X[0-9A-Z-]{12}[0-9A-Z]$
```

— so declaring it strengthens the **database** constraint as well as the write
path. It is the one part of *"is this the right kind of EIC"* PostgreSQL can
enforce on a row this crate did not write, and it was being left on the table.
`ValueCheck::ALL` is the eleven declarations rather than the four schemes, tied
by a `const` assertion to `EicType::ALL` so a letter added upstream fails to
compile here rather than becoming undeclarable; `EIC:Q` is refused at declaration
rather than degrading to a bare `EIC`, because a column declared as holding party
codes and silently taking any EIC is the outcome the declaration exists to rule
out.

The write path's refusal names both sides — *"this column declares object type X
(Party), and the code carries Y (Area or Domain) at position 3"* — because
"invalid" would send a producer to re-read a code they already have in front of
them. It is deliberately a different message from a malformed one: nothing about
an area code is wrong, it is in the wrong column.

### Two parsers in one process, and the strict one runs first

Bare `EIC` stays tolerant of an object-type letter `metering` does not list — the
list is ENTSO-E's to extend, and a store that hard-failed on an entry added after
its release would refuse data the market has already issued. That is the right
call for a decade of retention, and it stops being invisible the moment an EIC
passes through **two** parsers in one process and the other one is strict, which
is now ordinary: a market library holding a Bilanzkreis as its own type may
enumerate the seven letters and refuse anything else. It then rejects a row this
store accepted, on a *read*, at a moment nobody chose.

`eic_object_type(code)` alone could not express that finding, because it is null
for two entirely different things: a value that is not an EIC at all, and an EIC
whose type letter is unlisted. One column of nulls, and only the second is
actionable. `eic_normalise(code)` separates them:

```sql
-- Not an EIC. Somebody wrote free text into an identifier column.
SELECT DISTINCT bilanzkreis FROM readings
WHERE bilanzkreis IS NOT NULL AND eic_normalise(bilanzkreis) IS NULL;

-- A well-formed EIC whose object type this build does not list — the rows a
-- strict downstream parser will reject.
SELECT DISTINCT bilanzkreis FROM readings
WHERE eic_normalise(bilanzkreis) IS NOT NULL
  AND eic_object_type(bilanzkreis) IS NULL;
```

Find them on your own schedule; declare `EIC:X` to stop accepting new ones.
`eic_normalise` is also the EIC counterpart of `obis_normalise` and does that job
for the same reason: a foreign table may hold a code in lower case or padded, and
a checked column holds only the trimmed uppercase form, so a literal join across
the two returns nothing.

### A typed series read could not name a Messlokation

`identify_by_melo(true)` puts `melo_id` in the merge key of a *Lastgang* table,
which is what a Marktlokation measured by two meters needs — and `SeriesQuery`
had no `melo`. The only narrowing was `column_eq("melo_id", ScalarValue::Utf8(…))`,
which takes a bare scalar and **does not parse it**.

A Zählpunktbezeichnung is 33 characters, has no check digit, and is stored
uppercase. So a truncated or lower-cased literal matches nothing at all —
silently, and an empty series is indistinguishable from a meter that reported
nothing. That is the report a settlement run would act on.

```rust
store.series(malo)?.melo(melo)?.obis("1-0:1.8.0")?.range(from, to).collect().await?;
```

It parses (`encode::parse_melo`, the counterpart of `parse_malo`) and takes
whatever the caller is holding — a `MeloId`, a `&str` or a `String`. The first is
the common one and now costs nothing: a read of this store hands back `MeloId`s,
and narrowing to one of them should not go back through a string.
`ReadingsQuery::melo` takes the same bound, where it used to take `&str` only.

`CompletenessQuery::melo` and `meterstore completeness --melo` close the same gap
on the third builder, and there the consequence is worse rather than merely
silent: `column_eq` already accepted `melo_id` and named *"a Messlokation"* in its
own documentation, so a lower-cased literal narrowed the report to nothing — and
an empty completeness report does not read as *"no rows matched"*, it reads as a
meter that has stopped delivering. That is the strongest finding the report can
make, produced by a typo. `CompletenessQuery::malo` takes the generic bound too,
so all three builders now accept a parsed identifier as readily as a string.

The refusal that already prevented the doubled fold now names the fix that
applies: *"narrow the read with `.melo(..)`"* where the two readings differ by
Messlokation, and `.column_eq(..)` where they differ by a tenant. It used to name
both regardless, and for a Messlokation the one it named first was the untyped
door.

### `metering` 0.22

The floor is 0.22. Nothing in this crate changed shape for it — no bare
`rust_decimal::Decimal` crosses a `serde` boundary here, so the feature change
that release is about is a no-op on this side, which the new guards below now
*establish* rather than assume.

What it does give is `TryFrom<String>` on all four identifiers. A generic
`impl TryInto<…>` bound is satisfied by the caller's own type and not by a deref
of it, so an owned `String` — which is what an MSCONS parser hands back — needed
`.as_str()` at one call site and `.parse()?` at the next for the very same value.
`VersionScope::new`, `::for_interval`, `parse_malo`, `parse_melo` and both
`melo(..)` builders now take a parsed identifier, a `&str` or a `String`
interchangeably, and check an owned one exactly as they check a borrowed one.

### The stored shape of two JSON columns was asserted in a comment

`source_detail` and `provenance` are the two columns this crate writes as JSON
rather than as a typed Arrow column, so they are the two where a representation
decided **elsewhere in the build graph** reaches disk — which is precisely what
`metering` 0.22 was reported for. Three guards, each mechanical:

- **Every `MeasurementSource` variant is exercised**, and a `match` with no `_`
  arm keeps the list complete: `MeasurementSource` is not `#[non_exhaustive]`, so
  a variant added upstream now *fails to compile* rather than going quietly
  untested. Three of the ten were not covered — `RetroactiveCorrection`,
  `RedispatchImport` and, in that test, `VirtualMeter`.
- **The ten tags are pinned as literals.** `source_kind` is the discriminant on
  every stored row and the string every external engine filters on. A retag
  upstream keeps both columns agreeing with each other, stops old rows decoding,
  and passes every round-trip test — they all read and write through the same
  impl.
- **No stored JSON column may hold a floating-point number.** A float is the one
  that would not be noticed: an exact decimal written as `30.0` reads back as
  `30.000000000000004` on some other engine's parser, and settlement is money —
  the reason `value` is a `Decimal128` in the first place. The scan walks arrays
  and objects, so a field inside an enum variant is covered, which is where the
  upstream version of this scan had missed one.

### Two claims about the wire format were comments

`metering` 0.22 came out of a report that `metering/serde` was changing a `serde`
contract for crates that never named it: the feature enabled
`rust_decimal/serde-str`, which **replaces** the global `Deserialize` for
`Decimal`, so a service with no dependency on `metering` at all passed its own
`cargo test -p …` and failed the workspace run. That is fixed upstream, and this
crate is on 0.22.

This crate is a library too. It writes two JSON columns under a decades-long
retention obligation and it enables `metering/serde` for everyone downstream of
it — so the same question has to have an answer here, and the answer was two
prose comments in `Cargo.toml`. `tests/it/wire_format.rs` makes them mechanical,
and needs neither Docker nor a feature flag:

- **No feature that *replaces* a representation.** `serde-str`, `serde-float`,
  `serde-arbitrary-precision` and `serde-human-readable` may not appear in the
  manifest in any spelling. The first cut of this test knew only the
  `crate/feature` path form and a mutation adding
  `features = ["serde-str"]` to the `rust_decimal` line walked straight past it —
  it checks by feature *name* now, which is the spelling-independent one.
- **No serde-derived type may leave a field's representation to the build
  graph.** A bare `OffsetDateTime`, `Date`, `Decimal` or `time::Duration` under a
  `derive(Serialize)` without a `#[serde(with = …)]` is the same defect one level
  down, and silent in both directions — a reader and a writer built with
  different features simply disagree. None exists today; nothing was keeping that
  true.
- **What this crate turns on for a consumer is only additive.**
  `metering/serde` resolves to `["dep:serde", "time/serde"]`, and `time/serde`
  adds impls that do not otherwise exist rather than replacing any.

Both guards were mutation-checked against the defect they describe, which is how
the hole in the first one was found.

### Two documentation conventions became mechanisms

Both were enforced by review, which is to say not enforced.

- **`no_item_doc_grows_into_a_chapter`** — no `///` block over 60 lines. A block
  that long stands between a reader and the item it describes; module docs are
  exempt, because a module's own documentation is where a long argument belongs.
- **`reference_docs_are_not_a_changelog`** — no *"an earlier version"*, *"a first
  draft"*, *"previously"* or the past-tense *"used to"* in `src/`, `tests/`, the
  site pages or the README. Documentation says what is true; a doc that narrates
  its own history is stale the moment the next change lands, and nothing detects
  it. `CHANGELOG.md` is the one file the check does not read, which is the point.

The second needed a grammar rule rather than a substring, because English spells
two unrelated things the same way: *"used to cast incoming batches"* is a
purpose and *"each used to reassemble the builder"* is a changelog. The purposive
sense is passive or sentence-initial; anything else is a subject doing something
it no longer does. It reads trailing comments as well as leading ones — two of the
violations it found were after code on the line.

It is a **floor rather than a proof**, and says so: the bare past tense ("it was
~1.6 s, on the reasoning that …") is deliberately not on the list, because "it was
written by", "it was in force" and "nothing was changed" are ordinary description
and a rule that fired on all of them would be ignored rather than obeyed. One
violation of that shape was found by hand.

Both checks were mutation-tested against the defect they describe, and both found
real violations — in prose written for this release and in prose that predated
it.

### Also

- **A refused checked value now says three things**: which column, the domain's
  own message about the shape it wanted, and why the check is at the *write*.
  `ValueCheck::noun` and `::why` are the two halves, and the second is what turns
  a format complaint into an instruction — *"the check digit is part of the
  identifier, so a transposition is detectable here and only here, while the
  delivery that carried it is still in hand"*, against a producer who can still
  resend it.
- `parse_melo` joins `parse_malo` and `canonical_obis` as a public entry point,
  so an application writing to the hot tier directly need not reach past this
  crate to parse the other half of a point table's merge key.
- The `meterstore init` template documents the four `check` schemes and the
  `[privacy]` section, both commented out, as it does every other optional
  setting.

## [0.8.0] — 2026-08-30

Three silent wrong answers — a completeness report that could not see a missing
day, a correction that changed a reading's unit without reporting a change, and a
version scope buildable in a shape nothing else accepted — plus `metering` 0.21
and the four things it makes possible that this crate had been doing without: a
settlement month in SQL, a direction that can say *neither*, a Bilanzkreis that
is checked rather than believed, and the completeness verb the command line never
had. Completeness also gained the narrowing every other read on a store already
has.

### A meter that stopped reported the month complete

`meterstore.completeness` is the answer to *"can this period be invoiced"*, and
it was summing its expectation over the days the aggregate returned. A `GROUP BY`
yields no group for a balancing day with nothing in it, so those days were not
merely uncounted — they were **not in the arithmetic at all**.

A channel that delivered the 2nd of March and then stopped therefore reported
`expected = 96, actual = 96, missing = 0, complete` for the whole of March. A
`SUM` over that month was short by twenty-nine days, and the report whose entire
purpose is to say so said nothing. `first_gap` was `None`.

The report's own documentation was already right — *"intervals the calendar says
the **range** should hold"* — and the code disagreed with it. The walk is now over
the range's balancing days, which is the same enumeration the silent-channel path
already used: the two shared nothing before, and a second walk is a second chance
to disagree about what a range contains.

Nothing here was reachable through the roster (`seen_since`), which finds the
channel that delivered *nothing*. This was the channel that delivered *something*
— which is the harder case to notice by eye and the commoner one in a real
portfolio.

**An absent day belongs to a grid.** The report is one row per channel per
interval grid, so a day nobody delivered on has to be charged somewhere: to every
grid would report a clean hourly→quarter-hourly conversion as two badly
incomplete halves, and to none is the bug itself. It goes to the grid **last in
force before it**, and a gap before the channel's first delivery to the first grid
it used. For a single-grid channel that is simply "the whole range".

**A range reaching past the last delivery now says so** — a report over a whole
month run mid-month reports the remainder as missing. That is the honest answer
to the question asked, and the alternative is worse: consulting a clock would make
one report answer differently on two runs. Written down rather than left to be
discovered.

The tests are the other half of the story. Most of them ran a one-day fixture
against a **one-month** range and asserted `is_complete()` — which, stated that
way, is asserting the opposite of what the report means. Each now states the
period it is about, and the helper that builds one is named for that reason.

The per-day walk was also the wrong shape for a portfolio. The expectation depends
on `(Sparte, resolution)` and never on the channel, so it is computed once per
grid rather than once per channel, and the days of a channel are walked once for
all of its grids together — a year-long report over 100 000 channels would
otherwise have materialised 36 million dates.

### The gas month had no function, and the gas day did

`meter_balancing_day(ts, sparte)` has always read the commodity per row, because
grouping a gas Lastgang on the calendar day books six hours a day into the
neighbouring Bilanzierungstag. One period up there was only
`meter_local_month`, which is the *calendar* month for every row — so the same
error, twelve times a year instead of three hundred and sixty-five, and every
total still plausible.

`meter_balancing_month(ts, sparte)` is the missing half. An interval at 02:00
local on 1 March belongs to **February** for gas and to March for everything
else, which is also the month an MSCONS version scope is keyed to — so this is
how a reader reproduces `version_scope`'s month half in SQL. In Rust:
`planner::balancing_month_bounds` for the half-open UTC range, and
`planner::bilanzierungsmonat(year, month, sparte)` for the range addressed the
way the market addresses it, *"Juni 2026"* — both `metering` 0.21's
`DayBoundary::month_range_utc` and `::bilanzierungsmonat` under the boundary the
commodity balances on.

Leaving the day covered and the month not was an asymmetry rather than a
decision, and it survived because the month is only wrong at its two ends.

### Direction cannot be two booleans

`obis_is_import` and `obis_is_export` are **both false** for a register that has
no direction at all — Blindarbeit, a gas volume, a Zustandszahl — and false is
also what `obis_is_import` says about a feed-in register. So
`NOT obis_is_import(obis_code)` does not mean *export*: it sweeps the undirected
registers in with it, which is how a Bezug total ends up carrying kvarh.

`metering` 0.21 made `direction()` the primitive and derived the two predicates
from it. `obis_direction(obis_code)` is that primitive in SQL — `'IMPORT'`,
`'EXPORT'` or **null** — and the three-way `GROUP BY` it enables is the shape a
bidirectional Zählpunkt wants. The strings are `Direction::as_str`, which is also
the `serde` tag, so a value from the function and one out of a JSON payload
compare literally. The booleans stay, because a `WHERE` clause over a
three-valued column needs an `IS NOT DISTINCT FROM` and nobody writes one.

### A Bilanzkreis was sixteen arbitrary characters

`TableConfig::attribute_column` has named *"Bilanzkreis, grid area"* in its own
documentation since it existed, and both were plain `Utf8`. They are not plain
strings: a Bilanzkreis and a Bilanzierungsgebiet are addressed by an **EIC**,
and an EIC carries a check character.

`config::eic_column(name, nullable)` — `check = "EIC"` in TOML — declares a
column whose values the write path parses with `metering::ids::Eic`. This is the
argument that already makes `malo_id` a `MaloId` rather than eleven digits, and
it is *stronger* here: the BDEW Codenummer's Bildungsvorschrift exempts
GS1-issued GLNs, which is why `version_scope` deliberately does not check its
digit, and the EIC scheme has no such carve-out.

The stored value is `Eic`'s **canonical spelling** — trimmed and uppercase — for
the reason the OBIS code is canonicalised: a checked column may be an identity
column, and an identifier arriving in two spellings would be two readings that
never supersede each other.

Two checks, and where each stops is written down rather than implied. The hot
table gets a `CHECK` for the *shape*; the check character is arithmetic over the
other fifteen and no regular expression expresses it, so it is enforced on the
write path. A row PostgreSQL accepts from another writer is well-shaped and not
necessarily well-formed. A `value_check` this build does not recognise fails the
write and renders a pattern nothing matches, rather than degrading the column to
an unconstrained one — which is the single outcome the declaration exists to
rule out.

### Completeness had no verb

It is one of the five things a caller usually wants, it has a documentation page,
and an operator holding a terminal during an incident could not ask it. Now:

```bash
meterstore completeness --month 2026-06 --seen-since 30d --gaps-only
```

`--month YYYY-MM` is the Bilanzierungsmonat, which is what the new
`planner::bilanzierungsmonat` is for — cut at midnight local for electricity and
at 06:00 for gas, `--sparte` choosing which. The flag decides where the *range*
is cut; every row is still counted against its own `sparte`, so a table holding
both commodities is reported once per commodity rather than once wrongly.
`--seen-since` is a duration before the range, because `--month` leaves a caller
with no start date to subtract from.

It exits **zero** whatever it finds. A gap is a fact to triage; `status` is the
check that fails, because a stranded row means query results are *wrong* rather
than incomplete. `--format json` carries `channels_incomplete`,
`channels_silent` and the range, for the monitoring check that wants an exit code
from `jq`.

### A completeness report could not be narrowed

`series` and `readings` both take a MaLo-ID and both narrow by channel, by
Messlokation and by any declared column. `completeness` took a range and nothing
else, so the only way to ask about one meter was to compute the whole portfolio's
report and filter the answer — a scan of everything for a question about one
thing, and the question an operator asks first during an incident.

`CompletenessQuery::malo`, `::obis` and `::column_eq` narrow it, and
`meterstore completeness --malo … --obis …` from a shell. The predicate goes into
**both** scans: the reported range and the `seen_since` roster. Drawn only over
the range, a narrowed report would find every channel outside the narrowing
missing from it and call each one silent.

Identifiers are parsed and canonicalised at the call, so a mistyped MaLo-ID fails
there rather than returning an empty report — which reads as *"this meter is
fine"*, the same failure the write path parses to prevent.

### A restatement in another unit read as no change at all

`Displacement::value_changed` compared the two quantities' **numbers** and not
their dimension, while the struct beside it carried the unit and its own
documentation said why: *"a value without it is dimensionless"*. Gas is the
commodity that makes that reachable — a delivery may hold Betriebsvolumen in m³
or the converted energy in kWh, both legitimate, and a correction may switch. A
reading restated from `100 m³` to `100 kWh` is the largest change a value can
undergo, and it reported `value_changed() == false`: a caller gating its audit
row on that predicate wrote nothing.

`Displacement::quality_changed`'s first line also claimed it answered *"whether
the quality changed while the quantity did not"*, which is not what it does and
not what its § 60 Abs. 2 use wants. The predicate is unchanged; the sentence is.

### A version scope could be built in a shape nothing else accepted

`VersionScope::new` renders its period as `{year:04}`, which pads and does not
truncate — so a year outside `0..=9999` came out as five characters or with a
sign. Three places anchor on four digits: this constructor, `VersionScope::parse`
(whose whole job is to accept *exactly* what `new` produces), and the hot table's
`version_scope_canonical` CHECK. A scope built from a year outside the range was
accepted by the first, refused by the second, and rejected by PostgreSQL at the
write with a constraint name instead of a reason.

`new` now names the range, and `parse` checks four **digits** rather than four
characters — `"-100"` is four characters and parses as an `i32`.

### Changed — breaking

- **`metering` 0.21 is the floor.** `ObisCode::is_import` and `is_export` moved
  to a by-value receiver upstream, being `const fn` derived from `direction()`;
  `settings::ExtraColumn` gained a `check` field.
- `check` and `values` on one extra column are refused: a closed vocabulary and
  an open identifier scheme are two different claims about the same column.
- **`session::calendar_udfs` is `session::sql_udfs`.** The set has held the OBIS
  predicates for some time and now holds the EIC accessor as well, so the old
  name described a third of what it returned.

### Smaller, from the same pass

- **`eic_regelzone(code)`** completes the EIC column: a Bilanzierungsgebiet's
  Regelzone is position 4 of its code (BDEW *Anwendungshilfe EIC* v1.0 §2.2.2),
  which is the grouping key of a MaBiS Summenzeitreihe and had needed a mapping
  table. Null rather than an error for a string that is not an EIC — unlike
  `obis_code`, the argument comes from a deployment column that may not be
  declared `check = "EIC"`, and one row of free text must not take a report down.
- **`planner::balancing_month_bounds` and `planner::bilanzierungsmonat`** —
  `balancing_day_bounds` one period up, and the same range addressed by name.
- The three session-derived `MeasurementSource` variants `metering` 0.21 added —
  `ChargeDetailRecord`, `ClockAlignedMeterValue`, `DeviceLog` — needed no
  encoder change, which is the property `encode_source` was written for. The
  round-trip test now covers them anyway, `evse_id` both present and absent: a
  `None` serialising to an absent key rather than a null would be a stored-shape
  change nothing else would catch.
- `meterstore init`'s template and the configuration documentation both show
  `check = "EIC"`, so the two spellings of a declaration stay in step.
- **A new integration suite runs the shape `CHECK` against a real server.**
  PostgreSQL's regular expressions are POSIX rather than PCRE, and the EIC
  pattern leans on bounded repetition and a trailing `-` inside a character
  class — exactly where a dialect difference would hide and produce a
  constraint that quietly admits everything. The suite reads the constraint
  back out of `pg_constraint` and evaluates the **deployed** pattern in the
  server, then asserts the seam in both directions: a badly-shaped code is
  refused by the database, and a well-shaped one with a wrong check character
  is *accepted* by it and refused by the write path.
- **A DB constraint is created with the table, and the documentation now says
  so.** `create_tables` is `CREATE TABLE IF NOT EXISTS`, so adding `values` or
  `check` to a column of a deployment that has already created its table starts
  the write-path validation and does not add the `CHECK`. Schema evolution does
  not flag it either, by design: the declaration rides in Arrow field metadata,
  which comparison ignores so that a vocabulary is not a schema change.
- **A new end-to-end suite drives the completeness report through both tiers.**
  The unit tests feed the roll-up hand-built daily rows, so what they cannot show
  is that the aggregate behaves the way the roll-up assumes — which is precisely
  where the missing-day bug lived. `completeness_end_to_end` writes real
  deliveries with real holes in them and checks the report against days that were
  never written.
- The contract test against `metering`'s calendar now covers the month:
  consecutive Bilanzierungsmonate abut with no gap for both boundaries, and the
  twelve of them are exactly 365 × 24 hours — the two DST transitions cancel
  over a year, which is what makes `date_trunc('month', balancing_day)` a safe
  roll-up for an external engine.

### Nothing changed on disk

A checked column is a plain `Utf8` column carrying one more piece of Arrow field
metadata, exactly as a coded column is — inert to schema evolution and to the
cold tier. The three new SQL functions are functions, and the completeness fix
changes an arithmetic, not a schema. No stored bytes moved.

The **answers** changed, and that is the point: a completeness report over a
range a channel did not fill will now say so, where it previously did not. A
deployment alerting on `missing == 0` should expect findings it was not getting.

## [0.7.0] — 2026-08-30

An audit of the seams where this crate meets its dependencies, one setting fewer,
the two findings a completeness report and a retention sweep could not make — and
`metering` 0.20, which takes two of this crate's own workarounds off it.

### A duplicated version scope doubled a total, silently

Resolution partitions by the merge key **and `version_scope`**, so two network
operators for one reading leave two winners that agree on channel *and* on every
discriminator. `refuse_mixed_readings` — the check that stops a typed read folding
two readings into one series — asks whether the rows describe two readings, and
these do not. It let them through, `merge` folded them, and
`metering::aggregate` summed both.

Both write paths refuse a second operator, so the state is not reachable through
them. It is reachable through `PostgresHot::integrity_constraints(false)`, which
is a supported setting, and through anything else that writes to a PostgreSQL
table — and §7.3 said in as many words that a duplicated scope then has *no*
after-the-fact detection. It has one now: the fold refuses two values at one
instant with `InvariantViolated`, on both the interval and the register path.

It matters more on the register path than the interval one. A Zählerstandsgang is
*differenced* to get consumption, so two values at one instant do not double a
sum — an arbitrary one of the two lands on both sides of a subtraction and the
figure between two reads bears no relation to anything.

The guarantee has an edge, and it is written down rather than implied: a `SUM` in
SQL is not covered and will simply be twice the truth. The constraint is what
prevents a duplicated scope; the typed reads are what notice one.

Finding it also turned up a unit-test fixture asserting the fold *accepted* two
deliveries at one instant — a state resolution cannot produce, since it keeps one
row per (merge key, version_scope). Its real subject was that a non-key column
does not split a series, which it now tests over two instants instead.

### A retention period too wide to represent erased everything

`Retention::CalendarYears` subtracts from the current year, and the conversion
fell back to subtracting **zero** when the period did not fit an `i32`. So a
nonsense period — a `u32` from a bad config, a units mix-up — did not mean "keep
everything", it put the cutoff at the start of *this* year and made every subject
in the store due at once. Erasure is irreversible.

Both arms now saturate towards keeping, and are clamped to a year the calendar can
answer for: `metering::calendar::year_start_utc` panics on `Date::MIN`, because
Berlin's midnight on `-9999-01-01` is an hour before it in UTC.

### Smaller, from the same pass

- `meterstore.retention.subjects_anonymised` counts what a sweep destroyed.
  Compliance rather than health — a flat zero over a year is a job that is not
  running — and the only counter here whose *rise* is worth a look. Recorded in
  one place, so the catalog sweep, the single-table sweep and the maintenance loop
  all reach it, and with no `table` attribute, because a linkage is destroyed
  across the whole deployment at once.
- The deduplicated-row count is a `saturating_sub`. It cannot underflow today, but
  it is a subtraction on the write path, and an underflow there panics in a debug
  build and wraps to ~2^64 in a release one — two different wrong answers to a
  question that only exists for a metric.
- `ScanSpec::cursor_columns` now records why the Parquet footer's declared sort
  order is safe: PostgreSQL sorts under the database collation and Parquet
  declares byte order, and they agree because a MaLo-ID is eleven ASCII digits. A
  text column that could hold anything else must stay out of the declared prefix.

### `metering` 0.20 — the version scope's operator is now a type

Requires `metering` **0.20**, and takes two things from it.

**`ids::BdewCode` is the operator half of a `VersionScope`.** It was an unparsed
string with two ad-hoc rules: not empty, and no `':'`. But this is the network
operator's Marktpartner-ID — thirteen digits, what MSCONS carries in `NAD+MS` —
and the crate's own doctrine for `MaloId` applies to it exactly: past the
constructor a wrong-but-plausible operator is not an error, it is a **different
scope**. The correction never supersedes the value it corrects, both rows survive
resolution, and every sum over the reading is inflated with nothing reporting it.

So the constructors take `impl TryInto<BdewCode>`, the way the read paths take
`impl TryInto<MaloId>`. Three things fall out. The stored column is a fixed twenty
characters, so the hot table's `CHECK` is exact — `^[0-9]{13}:[0-9]{4}-(0[1-9]|1[0-2])$`
rather than "anything without a colon". The one-operator exclusion's
`split_part(version_scope, ':', 1)` can only ever take the half the type reports,
because thirteen digits contain no separator — a rule that used to be written
twice and kept in step by hand. And `VersionScope::operator()` returns the code
rather than a `&str`.

The check digit is verified but **not enforced**, which is `BdewCode`'s rule
rather than a choice made here: BDEW's Anwendungshilfe §2.3 carves out GS1-issued
GLNs, which use a different procedure, so a well-formed Marktpartner-ID may
legitimately fail the BDEW one and refusing it would refuse data the market
issued. `VersionScope::operator_has_bdew_check_digit()` reports it, the same
restraint `Version::is_well_formed` applies to a short version label.

**The hand-written `provenance` encoder is gone.** It existed for one field:
`ProvenanceEntry::occurred_at` is a `time::OffsetDateTime`, whose serde impl is
feature-conditional, so the on-disk shape of an audit trail under a decades-long
retention was decided by Cargo feature unification rather than by any crate with
an opinion about it. `metering` 0.20 settles it upstream in a `wire` module — RFC
3339 for a human-readable format, `time`'s compact tuple for a binary one — which
makes the stored shape a deliberate, documented, tested choice by the crate that
owns the type, exactly the standing `source_detail`'s tags already had. Two rules
became one, and about seventy lines of encoder, decoder and argument went with it.
The column's byte shape changes (declaration order rather than alphabetical) and
is still asserted byte for byte, now pinning the upstream decision.

`MeasurementSource::Correction` also loses its `uuid::Uuid` upstream, so
`source_detail` carries `correction_ref` as a string. Nothing here constructed
one.

### Snapshot expiry ignored the retention window it was given

`iceberg`'s expire action runs its **age** path whether or not snapshot ids are
named: with no cutoff set it falls back to the table's
`history.expire.max-snapshot-age-ms`, default **five days**. This crate named ids
and set nothing else, so a single `--expire-snapshots` cycle expired everything
older than a working week — through the ten-year `snapshot_retention` it is
configured with, through `min_snapshots_to_keep`, and through the watermark-chain
protection it computes. The one guarantee the cold tier exists to hold, and it
survived because every test asked for expiry from a date in 2030, where
everything is past retention either way.

The cutoff is now pinned to the epoch, which selects nothing, so the ids computed
against the configured retention are the whole of what is expired. That closes a
second door at the same time: `history.expire.*` is a *table* property, so the
out-of-band compaction this design recommends could otherwise have set one and
silently decided the deployment's retention.

### A bound between two stored instants could skip the next row

DataFusion unifies a timestamp comparison on the finer of the two units, so a
literal written with sub-microsecond precision reaches the planner as nanoseconds
carrying a value *between* two stored instants. Converting it to an exclusive
bound added a whole microsecond, which landed past the next stored instant:
`"from" > TIMESTAMP '2026-07-10 00:00:00.0000005'` produced a lower bound of
`…0000015`, and the row at `…000001` — which satisfies the predicate — was outside
the range the tier split scanned. No error; the row simply was not read.

That is the one thing §17.1 says the analysis may never do. The bound is now
snapped to the microsecond grid the schema stores on, and the property that
guards it works in nanoseconds rather than whole seconds — which is why it never
generated the case. A 30 000-combination exhaustive check over the sub-microsecond
neighbourhood runs beside it, because a property drawing bounds and probes
independently meets this one only by luck.

### A retention sweep could orphan readings in a table it was not looking at

The subject registry is **deployment-wide**: one map keyed by natural identifier,
so two tables registering the same identifier share one `SubjectRef` and a single
erasure unlinks both. `MeterStore::anonymise_before` read one table. In the shape
every EDM deployment has — an authoritative Lastgang beside a second stream — the
first table to reach the ceiling therefore destroyed a linkage the other still
depended on: a measuring point whose Lastgang stopped three years ago but whose
register readings are current had the live ones orphaned, irreversibly, with
nothing reporting it.

`MeterCatalog::anonymise_before` applies the cutoff to the latest reading **in the
deployment**. The single-table method stays and says what it is scoped to.

### § 60 Abs. 6 had no schedule

Archival and snapshot expiry were jobs; the retention duty was a call. But nobody
files § 60 Abs. 6 — it comes due on its own, which is exactly what a scheduler is
for. `Maintenance::anonymise_after(Retention::CalendarYears(3), reason, actor)`
runs it on the maintenance cycle, off by default because destroying a linkage is
irreversible, and `meterstore maintain --anonymise-after-years 3` is the same
thing from a shell.

`Retention::CalendarYears(3)` is **not** `now - 3 years`. The statutory clock
starts at the *Schluss des Kalenderjahres*, so a value collected on 2 January 2025
comes due on 31 December 2028; the rolling spelling would erase it a year early,
which is the direction that destroys data still inside its period.
`Retention::Rolling(d)` is there for the earlier "no longer necessary" trigger.

### Completeness could not see the channel that delivered nothing

The report is an aggregate over the rows a range holds, so a channel with **no**
rows produced no groups and appeared nowhere — the most severe incompleteness
there is, and the one the report was silent about. Nothing inside the range can
supply the missing roster, because the missing roster is precisely what the range
does not contain.

`store.completeness(from, to).seen_since(since)` draws it from an earlier window:
a channel that reported between `since` and the start of the range and does not
report inside it comes back with `actual = 0`, the whole range as `missing`, and
`first_gap` on its first balancing day. In SQL the arguments read in time order —
`meter_completeness(seen_since, from, to)`. The window is the caller's because
"still in service" is master data this crate does not hold.

`completeness` now returns a builder rather than a future; `.await` on it is
unchanged.

### The catalogue façade served the whole catalogue

A SQL catalogue is a table in a database and a database is a thing organisations
share, so an unauthenticated endpoint that listed every namespace it happened to
hold was handing out somebody else's table metadata. `ColdTier::catalog_facade()`
is confined to the tier's own namespace: the listing reports one, every other
route answers `404 NoSuchNamespaceException` before the catalog is touched, and
`CatalogFacade::new` still serves everything for a caller that means it.

### One step instead of two

`partition_step` had to equal `archival_step`, was checked against it at
construction, and was shown beside it in `system.config` so a mismatch could be
noticed. The only thing two settings could express was the mistake. There is one,
`archival_step`, and it is the hot table's partition granularity as well; the old
key now fails the file as an unknown field.

A step below one minute is refused with it. A partition relation is named
`<table>_YYYY_MM_DD_HHMM`, so two consecutive sub-minute windows name one
relation — the second attach fails, and an orphan read back by name resolves to
the wrong window, where it could drop a partition still above the watermark.

### Smaller

- `planner::gas_day_length` and `intervals_in_gas_day` are gone. They were sugar
  over `balancing_day_length(day, Sparte::Gas)`, and the crate root exported the
  gas-only pair while the general ones were reachable only through `planner`. The
  general functions are exported instead.
- `evolution::compare`'s note that a rename "reads as a drop plus an add, both
  safe" was true only for a nullable column. For a non-nullable one — every
  identity column — both halves are unsafe and the table halts, which is the
  right answer rather than an accident of matching by name.
- `MaintenanceOutcome::failures` reports a failed retention sweep under
  `<retention>`, so an alert built on it needs no second place to look.

## [0.6.0] — 2026-08-25

A lock audit, the front end the crate did not have, and consumer feedback from an
MSB integrating against it.

### The typed API could not describe a measuring point

`collect` refuses a range spanning two channels, and the refusal is right — a
`MeasurementSeries` holds one `obis_code`, so folding import and export puts two
values at every instant and `aggregate` returns twice the truth. But a measuring
point **is** a set of registers, and the questions that are about all of them — a
billing period projecting the canonical Bezug across HT, NT and total, a
Mehr-/Mindermengensaldo, an audit of what a delivery contained — had no typed
answer at all.

What a consumer had to write instead was a hand-rolled `SELECT DISTINCT
obis_code` followed by one typed read per channel: SQL in front of the one API
whose whole point is that callers do not write SQL, `1 + N` round trips, and
version resolution and the tier split held outside the store by convention.

### A stored audit trail nobody could read

`provenance` was written with `serde_json::to_string`, and
`ProvenanceEntry::occurred_at` is a `time::OffsetDateTime` — whose `serde`
implementation is *feature-conditional*. With `serde-human-readable` off, as it
was, the column held `[2026,208,6,0,0,0,0,0,0]`.

Two things wrong with that, and the second is the sharper one. It is unreadable,
in files this crate tells operators to point Spark and Trino at — a timestamp
that reads as `208` is an audit trail nobody can audit. And the choice between
that shape and a string was being made by Cargo **feature unification**: the
on-disk representation of data under a decades-long retention depended on what
else happened to be in the binary that wrote it, and `time`'s deserialiser takes
the tuple path when the feature is off, so rows written while some other crate
had turned it on would silently stop decoding.

### The lock audit

PostgreSQL grants locks **in arrival order**, so a statement waiting for an
`ACCESS EXCLUSIVE` lock blocks every reader and writer that arrives behind it —
whether or not those would have conflicted with each other. This crate issued DDL
on two schedules nobody chooses the timing of, and neither of them knew that.

`CREATE TABLE … PARTITION OF` takes exactly that lock on the parent, and
partition creation runs on the **write path**: an append reaching past the
pre-created frontier makes what it needs. One long analytical query on the hot
table could therefore stall every subsequent insert for as long as it ran, and
the failure would read as "the database is slow" rather than as a lock queue.
Partitions are now built standalone and *attached*, which takes only
`SHARE UPDATE EXCLUSIVE` — a lock that conflicts with no read and no write at all.
The bound `CHECK` that lets the attach skip its validation scan is added and then
dropped again, so no row is ever checked against a redundant predicate.

The detach genuinely needs the strong lock, so it declines to *wait* for it
instead: every DDL statement now runs under a `lock_timeout`, and one that cannot
get its lock gives up having changed nothing. Both properties are asserted against
a real server holding a real conflicting lock rather than argued.

### The errors that came out of it

`pg` flattened every backend failure into `Error::Storage(String)`, in a crate
whose error type is documented as "callers match on variants and never parse
strings". A caller could not tell an unavailable lock from a lost connection, nor
either from an overlapping delivery the store refused on purpose.

That, followed outwards, found a worse conflation: a **refused delivery** raised
`InvariantViolated` — the variant documented as *the* thing to alert on, meaning
query results may be wrong — so a producer sending a bad row paged whoever was
watching the tiering invariant.

### Added

- **`SeriesQuery::collect_by_channel`** and **`ReadingsQuery::collect_by_channel`**
  — every channel (or register) in a range, each resolved on its own, from **one
  scan**. They keep the refusal that matters: a reading is `(channel, merge-key
  discriminators)`, so two tenants on one OBIS code, or two meters on one
  register, are still refused rather than folded. `…_with_provenance` returns the
  single boundary the whole map was computed against, which is exactly what the
  `1 + N` spelling cannot state.
- **`SeriesQuery::channels`** and **`ReadingsQuery::channels`** — the list on its
  own, as a `SELECT DISTINCT` rather than a fold, for an audit that wants to know
  what arrived without decoding a year of intervals. Both take `&self`, so the
  builder survives, and both are narrowed by everything the builder was narrowed
  by — an unscoped list would name channels belonging to a tenant the read cannot
  see.
- **`meterstore`, a command-line tool** (`cli` feature): `init`, `check`,
  `create`, `status`, `archive`, `maintain`, `query`, `explain`, `snapshots`,
  `serve` and `purge` — with `serve` binding both surfaces, Flight SQL and the
  read-only Iceberg REST façade, under one shutdown. A thin front end over the
  same public API — every result
  carries the tier boundary it was computed against, `--format json` renders it
  for a pipe, and `status` exits non-zero when a table is unhealthy so it works as
  a monitoring check. Deliberately no `append`: mapping an MSCONS message or an
  SMGW push to a `MeasurementSeries` is an application's job, not a flag's.
- `PostgresHot::ddl_lock_timeout`, and `ddl_lock_timeout` in `[hot]`. Three
  seconds by default; `0s` restores PostgreSQL's own behaviour, which is the
  queue-behind-me one.
- `Error::LockTimeout` and `Error::IntegrityViolation`, and `Error::is_retryable`
  to split the transient from the wrong.
- `ArchivalOutcome::deferred` and `TableMaintenance::deferred()` — a cycle that
  stopped because a lock was not available, reported like lease contention rather
  than as a failure. `ArchivalOutcome::is_benign_noop` covers both.
- `Deployment::store()`, `Deployment::catalog()` and `Deployment::table()`. A
  configuration file reached the tiers and stopped, leaving every deployment to
  repeat twenty lines of wiring — including the one step that is not field access,
  creating the cold table before a provider can be opened over it.
- `MeterStore::in_read_mode`, the general form of `as_known_at`. There was no way
  to derive a `Historical` or `Operational` session from an existing store.
- `settings::parse_human_duration` / `format_human_duration`, and an `ms` unit, so
  `--interval 15m` on the command line means what `interval = "15m"` means in the
  file.

### Changed

- **`provenance` is written explicitly, not through `serde`.** Timestamps are
  RFC 3339 and the event type is `metering`'s own stable code, so the column is
  encoded like every other column in the schema and its shape depends on nothing
  but this crate. **This is a stored-data change**: rows written by an earlier
  build hold the old array form and no longer decode. Recreate the table — the
  crate is unpublished and this is a hard cut.
  `source_detail` stays `serde`'s, deliberately: `MeasurementSource` is a
  seven-variant enum whose variants carry no timestamp, and hand-writing it would
  be a second copy of `metering`'s vocabulary.
- `the_stored_json_representation_is_pinned` asserts both JSON columns byte for
  byte, including the **nested** vocabulary: `MeasurementSource::VirtualMeter`
  holds a `VirtualMeterKind`, so `PV_SELF_CONSUMPTION` is an upstream tag stored
  inside an upstream payload inside `source_detail`. Retagged, `source_kind`
  would still read `VIRTUAL_METER` and still agree with the payload's outer key —
  the existing discriminant check passes and the payload just stops decoding.
  Everything else round-trips through the *current* `serde` impl, so such a
  change would have passed every test and broken every stored row. **Anything
  persisted through `serde` has its stored shape decided by a dependency**, and a
  change there is a stored-data break rather than a wire one.
- **Partition creation no longer takes `ACCESS EXCLUSIVE` on the parent.** This
  raises the effective PostgreSQL floor to **12**, where `ATTACH PARTITION`
  acquired the weaker lock. The documentation previously claimed 14 for a reason
  that was true of 10.
- A refused delivery raises `IntegrityViolation`, not `InvariantViolated` — in
  both tiers, and for both the restated-value and the wrong-Messlokation cases.
  `InvariantViolated` now means only that the store's own state is wrong.
- DataFusion failures keep their type instead of being stringified into
  `Storage`, so a statement that will not plan is distinguishable from a warehouse
  that cannot be reached — and is not retried.
- `system.tables.healthy` no longer reports a table with **no partitions** as
  degraded. "Not started" and "exhausted" show the same two numbers and mean
  opposite things, and calling the first degraded made the very first status of
  every new deployment an alarm.
- `MeterStore::sql`, `query`, `stream` and `MeterCatalog::query` now point at
  `scoped` and `isolated` from their own documentation. A reader who arrives at
  those looking for a way to confine a scan is exactly the reader who needs them,
  and they met the refusal list first.
- Environment interpolation skips comments, so a configuration file can document
  its own placeholders — which the one `meterstore init` writes does. A `#` inside
  a quoted value is no longer read as a comment either.

## [0.5.0] — 2026-08-25

Three audits, the `metering` 0.19 upgrade, and consumer feedback from an MSB
running this against a real workload.

The first audit found that the cold tier had no equivalent of the hot tier's
primary key: a replayed late correction was stored twice, and the optimisation
that makes historical scans fast is the one that then returns both copies — a
settlement sum came back **doubled**, with nothing reporting a problem.
`metering` 0.19 named a rule the crate had been getting wrong for a whole
commodity. The feedback found a silent precision loss, a documented feature that
could not be expressed, and a whole record type the crate had no place for.

The second audit went after the seams rather than the contents. It found three
ways to get a number that is silently short: a query running while a window was
being archived read **neither** tier for it; an append routed against a boundary
archival moved underneath it wrote rows nowhere any query looks; and the
Zählerstandsgang added above was keyed on the Marktlokation, while a register
belongs to the *Messlokation* — of which a Marktlokation may have several.

Following that last one outwards found the same mistake in three more places, all
of which fold two readings into one: the typed series read, the completeness
report, and — where a merge key was widened in configuration under an existing
table — the hot table's own primary key.

And it found that **read-only was not**. `query`, `sql` and `stream` ran whatever
SQL they were handed, and DataFusion's surface is wider than `SELECT`:
`CREATE EXTERNAL TABLE … LOCATION` reads any path the process can, `COPY … TO`
writes one, and an external table over the warehouse's own Parquet returns every
tenant's rows without ever touching the provider that enforces a scope. Over
Flight SQL that was one round trip from anyone who could reach the port.

The third audit went after the crate's *own* standards, on the theory that a rule
stated in one place and honoured in six is a rule with a seventh place it is not.
It found a promise the configuration file could not keep — a subject column named
in TOML **and** in `extra_columns` was registered as neither, so the deployment
that spelled its intent out twice got no reference checking at all — a schema
check that quarantined a merge key being widened but not one being narrowed, and
a Parquet footer declaring a sort order that only one of the two cold writers
actually produced. It also found the unit conversion at the centre of the storage
encoding written out **seven** times, five of them with an unchecked cast, a
credential printed verbatim by a derived `Debug` in the one constructor every
deployment writes, and — the largest of them — an optimisation that had been
argued for at length and fired on essentially nothing: version elision required
every cold file in range to carry the *same* version, which a year of daily
archival windows at ascending MSCONS versions never does.

Following the same theory through the **surfaces** found three things the crate
had built for one shape and not the other. The correctness oracle — the thing
§17.3 rests its whole argument on — keyed on a merge key that predated `melo_id`
joining it, and had no notion of register readings at all, so the record type
this release added was checked by hand-written cases alone. A multi-table
catalogue could be queried, could report its status and could be isolated, but
could not be **served**, streamed or maintained: three of the four costs a
handle-per-table pays, closed, and the fourth left open. And `[hot]` and `[cold]`
were parsed, exposed and consumed by nothing at all — a configuration file could
describe a deployment the crate had no way to build, and no way to tell it could
not.

**Breaking throughout**, as every pre-1.0 release here is.

### Added

- **Zählerstandsgänge.** A table declares its `TimeModel` — `Interval` (a
  Lastgang, energy over `[from, to)`) or `Point` (register values at instants).
  `StoredReadings`, `append_readings` and `readings` are the point-series
  counterparts of `StoredSeries`, `append` and `series`, carrying `metering`'s
  own `MeterReading`.

  The gap was structural: the crate was interval-shaped throughout, so the
  *derived* Lastgang tiered into Iceberg while its **source readings could not** —
  leaving the hot tier to grow without bound in the one table nobody may delete
  from. BK6-24-174 (in force 06.06.2025) means a German MSB holds a
  Zählerstandsgang per measuring point at the same cadence as the Lastgang, so the
  primary record is exactly as voluminous as the derived one, and § 146 Abs. 4 AO
  means it cannot be discarded after differencing.

  Everything else is unchanged — identity, attribute and subject columns, version
  scoping, watermark, partitioning, routed writes, late corrections — because all
  of it reads the *start* timestamp. Three things differ: `to` is null, `value` is
  a cumulative register reading, and the overlap exclusion is off because instants
  cannot overlap. Each write path refuses the other's shape.

- **`MeterStore::append_authoritative`** — for a value the operator *authors*
  rather than receives. Through `append` an Ersatzwert that a higher version
  already beats was stored and silently shadowed: the row landing, the audit trail
  written, the confirmation closing, and the value never becoming current. The
  version a row carries is a floor now; where a higher one holds the reading the
  store re-appends at `ScopedVersion::next` (also new) of the one in force,
  continuing the stored sequence under the stored scope.

- **`TableConfig::identify_by_melo`** and `time_model` in TOML. A **point table
  identifies a reading by its Messlokation** by default, and an interval table
  does not — see the fix below. `TimeModel` itself was unreachable from a
  configuration file, so a deployment configured from TOML could not declare a
  Zählerstandsgang at all, against the stated rule that TOML is a front end over
  the same validated types.

- **`ValidatedTableConfig::discriminator_columns`** — the merge-key columns
  beyond `(malo_id, obis_code, from)`. `identity_column_names` was that list only
  while `melo_id` could not be part of the key. `MeterStore::scoped` and
  `SeriesQuery::column_eq` accept them all, so a single meter of a
  Mehrfamilienhaus can be handed to code that must not see the others.

- **`Version::arrival`** — the version to give a delivery that states none. Unix
  milliseconds: 13 digits, so below the ≥14-digit MSCONS band until 2286 and
  always outranked by a stated version, and still sub-second.

- **`MeterStore::scoped`** and **`MeterCatalog::isolated`** — a session confined to
  one identity value, and one confined to a single table. Both inject into the
  plan and are enforced below the projection, so caller-supplied SQL cannot omit,
  alias or `UNION` past them.

- **`MeterStore::stream`** — plans a statement, returns the `QueryDescription`
  before the first row, then streams batches. Flight SQL uses it.

- **OBIS predicates in SQL** — `obis_is_import`/`_export`, `_reactive`,
  `_lastgang`/`_zaehlerstand`/`_vorschub`/`_maximum`,
  `_fehlerregister`/`_total_register`, `obis_tariff_register`, `obis_normalise`.
  Thin wrappers over `metering::obis`, like the calendar functions.

- **Cold-tier displacement reporting.** `AppendOutcome::displacements` covers both
  tiers; only the hot tier ever produced it.

- **`IcebergRestCatalog`, and `Settings::connect`.** Two gaps that turned out to
  be one.

  The `rest-catalog` feature is **on by default** and pulls the whole REST client
  and its HTTP stack — and nothing in the crate used it. Its stated justification
  was the read-only catalogue façade, which is built on axum and `dyn Catalog`
  and does not touch it. Meanwhile `CatalogKind::Rest` is what a configuration
  file that says nothing selects, and there was no constructor behind it: a
  default naming a catalogue the crate could not build. `IcebergRestCatalog` is
  that constructor, and it earns the dependency the default was already paying
  for.

  And `[hot]` and `[cold]` were parsed, exposed as public fields, and consumed by
  nothing. The page describing this front end claims "no setting reachable from
  one and not the other", while the two sections naming the *infrastructure* were
  reachable from a file and from nowhere else — so a TOML-configured deployment
  still hand-wired a pool and a catalogue, re-deriving what the file had already
  said, and settings the builder has (`file_target_bytes`,
  `metadata_pool_max_connections`, the non-secret half of the object-store
  credentials) had no TOML spelling at all.

  `Settings::connect()` now returns a `Deployment` — the pool, both tiers and
  every validated table. It stops short of a `MeterStore`, which needs a cold
  table provider and therefore an existing table, and which a multi-table
  deployment does not want one of anyway.

- **A typed read path for registers.** `MeterStore::readings(malo)` returns a
  `ReadingsQuery` — the point counterpart of `SeriesQuery` — where it took
  `(malo, from, to)` and returned every delivery in range.

  A builder because a point table has two needs a Lastgang does not. It
  **identifies a reading by its Messlokation**, so a Marktlokation with two meters
  returns two registers at every instant: `.melo(..)` names one, and an unnarrowed
  `.collect()` is refused rather than folded — interleaving two cumulative
  sequences does not produce a doubled sum, it produces advances belonging to
  neither meter. And `latest()` is the question a register is actually asked:
  "what does the meter read now" is an `ORDER BY … DESC LIMIT 1` at the storage
  layer, not a decade of quarter-hours folded in memory, on the one table § 146
  Abs. 4 AO forbids discarding.

  `.obis(..)`, `.column_eq(..)`, `.quality_in(..)`, `.range`/`.since`/`.until`,
  `.values()`, `.collect_with_provenance()` and `.deliveries()` — the last being
  the unfolded audit shape the old signature returned.

- **`MeterCatalog::maintenance`** — one scheduled loop over every table, where a
  deployment ran a timer per table. Each table keeps its own watermark, archiver
  and lease; what is shared is the scheduling, which is the third of the three
  costs a handle-per-table paid and the last one left. Tables are visited
  sequentially, because twenty archivals at once turns a background job into a
  load spike on the database it exists to relieve.

  `MaintenanceOutcome` is therefore **per table**: `tables: Vec<TableMaintenance>`
  with the aggregates folded on top, and `unhealthy()` naming the tables an alert
  should mention. A cycle that only summed would report a deployment healthy while
  one of its tables was quarantined — the failure `system.tables` showing one row
  already had.

  And a failing table is now a **row rather than an early return**. The states
  that fail here persist until an operator acts, so aborting would let one
  quarantined table freeze archival for every other — whose hot tier then grows
  without bound for the length of the quarantine, a second and larger incident
  caused by how the first was reported. `healthy()` is false while any table
  failed, and `failures()` names them.

- **`MeterCatalog::stream` and `describe`, and Flight SQL over a catalog.** The
  server took a `MeterStore`, so the deployment §15.3 calls ordinary — the
  authoritative readings beside a non-authoritative second stream — was the one
  shape with no serving surface at all. A statement mentioning both tables is the
  *second* thing an external client cannot assemble for itself, since each table
  has its own watermark and its own hot half; and `information_schema` on the
  same session was already listing every table such a client could not then
  query. `FlightSqlServer::new` now takes a `SqlSurface` — the pair of methods
  serving actually needs, implemented by both `MeterStore` and `MeterCatalog`.

  A catalog response carries a `meterstore.watermarks` entry naming each table
  the statement touched and the boundary it was at, beside the conservative
  minimum. Two tables genuinely have two boundaries.

- **The oracle covers Zählerstandsgänge, and Messlokationen.**
  `MeteringWorkload::generate_readings` produces register readings that are
  cumulative and monotonic — because that is what a register is, and a generator
  emitting independent draws would produce a series no meter could have — and
  `Oracle::record_readings` resolves them through the same fold the interval path
  uses. `MeteringWorkload::messlokationen` gives a Marktlokation several meters,
  which is the shape a merge key without `melo_id` folds into one reading.

  §17.3's property is stated about *any* query over the unified view, and half
  the shapes the store accepts were outside it: the store gained a whole write
  path for register readings in this release and nothing but hand-written cases
  checked it.

- **The catalog façade is tested as HTTP.** It was a documented serving surface
  whose only tests were over its helper functions, so neither of the two claims
  that make it usable — that a client can walk `GET /v1/config` to table metadata,
  and that a mutating verb is refused with the reason — had ever been exercised
  through the route table. Both are, along with `HEAD` (`tableExists`), an unknown
  table answering `NoSuchTableException` rather than a 500, and a path outside the
  served subset saying which subset that is.

### Security

- **Caller-supplied SQL could reach the filesystem and past every row scope.**
  `MeterStore::query`, `sql` and `stream` passed their text to `ctx.sql`, which
  *executes* DDL as it plans it. `CREATE EXTERNAL TABLE t STORED AS PARQUET
  LOCATION '<warehouse>'` therefore read the cold tier's own files — every
  tenant's rows, unscoped, because an external table never touches the provider
  a scope is enforced in — and `COPY (…) TO '…'` wrote a file wherever the
  process could. `MeterStore::scoped` and `MeterCatalog::isolated` were
  documented as boundaries caller-supplied SQL could not step past, and both
  could be stepped past. The Flight SQL endpoint, documented as read-only,
  refused only the *mutating calls*: these arrive as ordinary statement queries.

  The three surfaces now build the plan without running it, refuse anything that
  is not a query — DDL, DML, `COPY`, `SET`, and the same wrapped in `EXPLAIN`,
  since planning a `COPY` is what performs it — and only then execute.
  `MeterStore::context` remains the unrestricted door for the in-process caller
  who wants DataFusion itself.

### Fixed

- **The `[hot]` and `[cold]` sections were never validated.** `Settings::validate`
  checked the tables and returned them, which is all a caller wiring the tiers by
  hand needs — but its documentation says "the full cross-field validation", and
  an empty `url`, an empty `warehouse`, a zero pool size or a warehouse scheme the
  build did not compile in all passed. `validate_all` checks them, `connect` runs
  it first, and each refusal names the setting rather than surfacing later as
  sqlx's opinion of a relative URL.

- **`refresh_system_tables` could be called exactly once.**
  `MemorySchemaProvider::register_table` refuses a name it already holds, so the
  *second* call failed with "The table tables already exists" — and the second
  call is the first one a maintenance loop or an operator dashboard makes. A
  method whose whole purpose is to be called again worked only on a session that
  never had been. The relations are deregistered before being re-registered,
  which is what "refresh" meant.

- **A catalog query naming a table only inside a subquery was attributed to no
  boundary at all.** Attribution walks the logical plan for table scans, and a
  scalar subquery's plan hangs off an *expression* rather than off the plan's
  inputs — so `SELECT (SELECT COUNT(*) FROM readings)` named nothing,
  `QueryResult::watermark` fell back to the epoch, and a statement of perfectly
  ordinary shape came back claiming that nothing had been settled. That is the
  exact fiction carrying provenance exists to prevent, produced by the mechanism
  meant to prevent it. The walk descends into subqueries now, and a CTE is
  covered by the same test.

- **`Oracle::for_table` used the wrong half of the merge key.** It read
  `identity_columns`, which is `discriminator_columns` only while `melo_id`
  cannot join the key — and on a point table it always does. So the reference
  folded two meters of one Marktlokation into a single reading and would have
  reported a mismatch against a store behaving correctly, which is the worst way
  for a reference to be wrong: it accuses the thing it exists to check. Its
  documentation promised "the table's **actual** merge key", which is now what it
  reads.

- **The erasure key was redacted but not wiped.** `SubjectRegistry` is `Clone`
  and every derived session — `as_of`, `as_known_at`, `scoped`,
  `in_own_session` — clones it, so a deployment doing reproducible reads left a
  copy of a cryptographic key in a freed heap page for each one. The buffer is
  `Zeroizing` now.

  Hygiene rather than a claim the key exists in one place: it does not reach the
  caller's own buffer, an environment variable the process still holds, or the key
  schedule `hmac` derives per tombstone. Object-store credentials stay redacted
  and **un**wiped on purpose — an explicit S3 key is forwarded into the object
  store's client, which holds it for the life of the process, so wiping this
  crate's copy would imply a protection that does not hold.

- **A PostgreSQL password and an S3 secret key could reach a log line.**
  `IcebergSqlCatalog` and `WarehouseAuth` derived `Debug`, so the primary
  constructor a deployment writes printed `database_url` — the same connection URL
  as the hot tier's, password and all — and `secret_access_key` verbatim into any
  `tracing` field or error context that carried it. The rule was already
  established twice, for `HotSettings` and for `SubjectRegistry`'s erasure key,
  and the redaction helper both used is now shared rather than local to one of
  them. The shape survives (`postgresql://<redacted>`, `secret_access_key: true`),
  because that is the half an operator reads it for.

- **A subject column declared twice was registered as neither.** TOML's
  `subject_column` skipped its own registration when the name also appeared in
  `extra_columns` — on the reasoning that the column was already declared, which
  was true of the *column* and not of the **marker**. Without the marker
  `ValidatedTableConfig::subject_column` is `None`, so the write-path check
  against the `SubjectRegistry` never runs and the builder's "a subject column is
  declared but no subject registry was provided" refusal never fires. A
  deployment that spelled its intent out in both places — the natural thing to do
  when the column also carries a `values` vocabulary — held pseudonymous
  references and validated none of them, with the column present and every row
  looking right. `TableConfig::subject_column` is idempotent now: it adopts an
  attribute column of that name rather than declaring a second one, keeping a
  `coded_column`'s vocabulary, and sets the marker either way. Declaring it as an
  *identity* column is refused by `build()` as well as by TOML.

- **Undeclaring an identity column was classified as a safe schema change.**
  `evolution::compare` called every `Dropped` column safe, on Iceberg's rule that
  a dropped column is retained for time travel — true for a nullable one. Every
  identity column is non-nullable by validation, so *removing* one from
  configuration arrived as a dropped required column and passed, while *adding*
  one arrived as a non-nullable addition and quarantined the table. The direction
  that passed is the more dangerous of the two: the resolution view then
  partitions by the narrower merge key, two tenants' readings for one measuring
  point compete, and one supersedes the other with no error anywhere — the exact
  failure `identity_column` exists to prevent, reached from the other side.
  Dropping a **required** column now quarantines, and the message names the
  consequence rather than the column.

- **A late correction declared a sort order it was not in.** Every data file
  carries `sorting_columns = (malo_id, from)` in its Parquet footer, and a reader
  is entitled to act on it — skip a row group whose `malo_id` range cannot hold
  the meter it wants. Archival satisfies that for free, because the hot scan pages
  by a keyset cursor whose prefix is exactly those columns; a **late correction**
  is written straight from the delivery, in whatever order the delivery carried.
  The two cold writers were making one claim on different grounds and only one of
  them held, and a footer that declares an order the rows are not in produces a
  silently skipped row group rather than a slow scan. `encode::sorted_for_storage`
  puts the correction batch in that order before it is written — it is in memory
  and proportional to what changed — which sharpens the file's own row-group
  statistics as a side effect.

- **Completeness reported one arbitrary grid for a channel that held two.**
  `resolution` was grouped on in the aggregate and then dropped from the key the
  roll-up folds by, so a meter converted from an hourly profile to a
  quarter-hourly one mid-month came back as one row naming whichever grid the
  aggregate happened to yield first, against a count drawn from both. Arbitrary
  literally: DataFusion defines no order over group output, so the same data could
  report differently on two runs. It is the *grid* that decides whether a day of
  24 values is complete or 72 short — the same argument `sparte` was already in
  the key for — so it reports one row per grid, each measuring its own days.

- **Mutating requests to the catalog façade got a bare `405`.** Only the
  single-table route carried the refusal, so `createNamespace`, `createTable` and
  `dropNamespace` — the three an external writer actually attempts — were answered
  by axum's default with an empty body. An Iceberg client parses the spec's error
  envelope, so an empty body reads to it as a broken endpoint rather than as a
  read-only one, which is precisely the confusion `ApiError` exists to prevent.
  Every served route now answers with the reason, and `HEAD` still reaches the
  read handlers, because `namespaceExists` and `tableExists` are reads.

- **`SeriesQuery::latest` ordered partially.** `ORDER BY "from" DESC LIMIT 1` is
  not a total order, and the reads most likely to ask for "the current reading"
  are exactly the ones where the newest instant carries more than one row: a
  measuring point with import and export, a table keyed by Messlokation, a
  tenant-extended key. Two identical calls could return different rows. The rest
  of the merge key completes the order, as the published resolution SQL already
  did for the same reason.

- **`VersionScope::parse` accepted what `VersionScope::new` refuses.** It split on
  the *last* separator, so `"a:b:2026-03"` parsed to an operator the constructor
  rejects — and the hot tier's one-operator exclusion reads the operator back with
  `split_part(version_scope, ':', 1)`, which takes the other half. It also checked
  the period's length rather than its shape, so `"99:2026-99"` parsed. Neither
  failed loudly afterwards: `covers` answers `false` for a period it cannot read,
  so such a scope refuses **every** delivery it is checked against, with an encode
  error blaming the caller's Bilanzierungsmonat for a value that was malformed in
  the table.

- **A query lost a whole window while it was being archived.** Archival detaches
  a hot partition before it reads it and drops it only after the cold commit; the
  watermark is published *by* that commit, so throughout the scan the range still
  belonged to the hot tier while the rows were in neither the parent table nor
  Iceberg. A settlement running at that moment came back a day short — for as
  long as writing a day of 9.6 M rows takes — with nothing anywhere reporting it.
  The hot scan reads the parent **and** whatever is detached from it. It cannot
  double-count: a committed partition is below the watermark, and the hot half of
  a split starts at it.

- **An append could write rows below the watermark.** Routing is decided against
  a boundary read before the write, and archival advances that boundary from
  another system. Detaching before scanning closes most of the gap — an insert
  into a partition being archived fails outright — but not the moment after the
  drop, when the partition is recreated and the insert succeeds into a range the
  cold tier now owns. `append` reads the boundary again after writing and
  re-routes if it moved; both writes are idempotent, so the second pass restores
  rather than duplicates.

- **A Zählerstandsgang could not hold a Mehrfamilienhaus.** A Marktlokation may
  be measured by several Messlokationen, and each carries the same OBIS register
  at the same instants — so on a merge key of `(malo_id, obis_code, from)` the
  second meter is a restatement of the first. Where the two readings *agree*,
  which two freshly installed meters do, `ON CONFLICT DO NOTHING` dropped one
  with nothing to notice. `melo_id` now joins the merge key on a point table,
  where it is `NOT NULL` and a delivery naming none is refused. Where it is off,
  both tiers compare the column on a redelivery and refuse the collision by name
  rather than leaving the silent drop available.

- **A typed series read folded two readings into one.** `series()` filters by
  measuring point, so a meter reporting import *and* export, a shared store
  carrying a row per tenant, and a Mehrfamilienhaus carrying a row per meter each
  produced a second interval at the same instant — and `MeasurementSeries` has no
  way to say so. `metering::aggregate` summed both and the month came back
  doubled, with one party's readings inside another's series. `collect` refuses
  it and names the fix: `.obis(..)`, `.column_eq(..)`, or a scoped session. A
  column that is *not* in the merge key still never splits a series, so a
  Bilanzkreis reassigned between two deliveries reads as one.

- **Completeness reported a surplus that was not there.** The aggregate grouped
  by `(malo_id, obis_code, sparte, resolution, day, quality)` and not by the
  merge key, so two tenants each delivering a full day put 192 intervals against
  an expectation of 96. The other direction was worse: one of them four intervals
  short netted against the other's full day and the channel read as **complete**,
  which is the one answer a completeness report must never give. It groups by the
  merge key and reports it — `Completeness::identity`, and one column per
  merge-key column in `meter_completeness`.

- **A merge key changed in configuration was silently ignored.** `create_tables`
  runs on every start and `CREATE TABLE IF NOT EXISTS` is a no-op, so a widened
  key left resolution partitioning by the new one while the table enforced the
  old — and the second reading the wider key exists to admit conflicted on the
  narrower primary key, was skipped, and was invisible to the divergence check,
  which joins on the new key. The declaration is read back and compared, along
  with the time model, both of which the DDL cannot alter in place.

- **A cold commit that never landed left its data files behind.** They are the
  only orphans an append-only warehouse can produce, and the writer holds every
  path, so it deletes them — after re-reading the table, because a commit can
  fail *after* landing and deleting on that reading would take files out from
  under a live snapshot.

- **A typed read returned one delivery per row** on any table whose merge key is
  wider than three columns. `series` and `readings` ordered by
  `(malo_id, obis_code, from)`, which leaves two tenants' — or two meters' — rows
  interleaved, so every contiguous run was one row long and a meter-day came back
  as ninety-six deliveries of one reading. Both order by the whole merge key.

- **`append_authoritative` could re-author the wrong reading.** It located a
  displacement's series by `(malo_id, from)`, which names several rows on a table
  with an identity column or two Messlokationen: the wrong value was written, at
  a version derived from a reading it was not about, and the one that needed
  authoring stayed shadowed. It matches the full merge key and the channel.

- **A non-canonical OBIS code was accepted on decode.** `sparte`, `unit`,
  `quality` and `resolution` were checked against the spelling storage writes;
  `obis_code` — the one of them in the merge key — was parsed leniently, so
  `1-0:1.8.0*255` or a leading zero read back as a well-formed channel that a
  correction keyed on the canonical form would never supersede. A malformed code
  at series level was worse: `.parse().ok()` turned it into a series carrying no
  channel at all.

- **The planner could narrow a range rather than widen it.** A cast around the
  *literal* side of a comparison was seen through unconditionally, so
  `from >= CAST(t AS TIMESTAMP(0))` was read as a bound at `t` rather than at the
  truncated second below it — dropping the rows in between. §17.1 permits error
  in the widening direction only, and the rule the column side already applied
  now applies to both.

- **A misaligned watermark walked past rows.** Changing `partition_step` on a
  table that had already archived left the watermark off the step grid, so every
  window named a partition relation nothing creates, every window looked empty,
  and the boundary advanced over rows still in PostgreSQL. `next_window` refuses
  it and says why.

- **A long table name broke the first write of a new day.** PostgreSQL truncates
  an identifier at 63 bytes silently; a partition adds 16 characters to the table
  name and its integrity constraints another 13, so past 34 the two constraint
  names on one partition collide and the second `ADD CONSTRAINT` fails. Refused
  at `build()`.

- **A replayed late correction was stored twice.** `append` routes a
  below-watermark interval to Iceberg, which has no constraints, so nothing
  stopped the same `(merge key, version)` landing twice — and version resolution
  cannot collapse two rows at one version. Worse, resolution is elided entirely
  when the cold files in range provably hold a single version, which is exactly
  the shape a replay produces: the raw rows were returned and every `SUM` doubled.
  `append` now reconciles against what is stored before writing, keyed on the full
  merge key including identity columns.

- **A different value under an existing cold version is refused.** The hot tier
  already did this; the cold tier kept both copies.

- **The gas Bilanzierungsmonat is cut at 06:00.** EDI@Energy *Allgemeine
  Festlegungen* v6.1c, Kap. 3.1 defines the gas month as 01.06 06:00 to 01.07
  06:00, so an interval at 02:00 local on 1 March belongs to February's scope.
  `VersionScope` used the calendar month for every commodity, which **refused** a
  correctly-scoped gas delivery at the write and accepted the wrong one. Every
  constructor takes a `Sparte`.

- **A daily gas series was reported unmeasurable.** `intervals_in_gas_day` derived
  its count from `IntervalResolution::fixed_seconds`, which is `None` for `P1D`.

- **`to_json` lost the eighteenth digit of a decimal.** Arrow renders a
  `Decimal128` exactly and then every ordinary JSON reader parses it into an
  `f64`: `123456789012.345678` read back `123456789012.34567`, with no error.
  Decimals are JSON strings now.

- **`source_kind` was a second spelling of a domain vocabulary.** A hand-written
  list produced `mscons` while the JSON in the next column read `{"MSCONS": …}`,
  so an external engine filtering on the only spelling it could see matched
  nothing. The tag is read off the serialised form.

- **A non-canonical code is refused rather than normalised.** `metering` 0.19 made
  `FromStr` lenient — trims, ignores case, takes `WÄRME` for `WAERME`. These
  columns are `GROUP BY` keys, so two spellings of one commodity are two rows in a
  completeness report. The hot tier's `CHECK` already refused them; the decode path
  does now too.

- **The version-resolution SQL ordered partially.** `ORDER BY version DESC` alone
  lets `ROW_NUMBER` pick either of two tied rows. `recorded_at DESC` breaks it.

- **A pinned session refuses to be written through.** Every check the write path
  makes is a query against the session that is writing.

- **Table names are validated as plain identifiers**, as declared column names
  already were. `SeriesQuery::column_eq` checks its column against the declared
  set and returns `Result`.

- **`VersionScope::covers` allocated once per row** on the encode path.

- **The `testkit` generator produced a gas workload the store was right to
  refuse** — it split deliveries on calendar months.

- **`h2` advisory RUSTSEC-2026-0258**, reached through `hyper`/`tonic`.

- **`site/config.toml` failed to parse on zola ≥ 0.23** while CI's 0.22 pin
  stayed green.

- **`HotStore::invariant_violations`** documented two directions and checks one.
  **`write.rows_deduplicated`** counted only the hot tier.

### Changed

- **Version-resolution elision now fires on the shape real data has.** The rule
  was "every cold file in range holds a single version, and they all agree on
  which" — sound, and true of almost nothing. MSCONS versions ascend per
  *delivery* and archival commits one day per window, so a year of history is 365
  files at 365 different versions: every scan wider than a single day resolved,
  and the argument for a table provider rather than a SQL view bought nothing.

  The missing observation is that `from` is **in the merge key**, so two files
  whose `from` bounds do not overlap cannot hold the same key however their
  versions differ — and consecutive archival windows are exactly that. Versions
  now only have to agree among files that could share a key, which a late
  correction (a second file over an already-archived day, at a higher version)
  still does.

  `ColdStore::version_stats` therefore returns `planner::FileStats` — the file's
  version bounds *and* its interval span — rather than `Option<VersionStats>`.
  Unknown bounds are read as "overlaps everything", which collapses to the old
  rule, and files are grouped into runs of overlap rather than compared pairwise,
  so the approximation can only ever cost a window function rather than skip one
  that was needed. Asserted as a property in both directions: eliding implies no
  key can appear twice, and disjoint windows at arbitrary versions always elide.

- **The storage encoding's unit conversion is one function, not seven.** Every
  layer that had to put a timestamp into the schema's own type — the encoder, the
  hot tier's bind path, the predicate builder, the typed series read, the
  completeness aggregate, the system tables, the transaction-time ceiling —
  spelled `unix_timestamp_nanos() / 1_000` out for itself, and **five of them cast
  the `i128` result with `as i64`**, which wraps rather than fails. `Date32` had
  three copies and the `ScalarValue` literal four, two of which had independently
  got the `"UTC"` zone spelling right. `encode::schema` now owns `micros`,
  `instant`, `date32`, `date_of` and `timestamp_scalar`, and a test asserts what
  makes the first infallible — that `time`'s ±9999-year range fits `i64`
  microseconds — so a graph that enables `large-dates` through feature unification
  fails at this crate's boundary rather than as a wrapped timestamp inside a
  committed Parquet file.

- **`metering` 0.19 is the floor.** `calendar::DayBoundary` is what makes the gas
  Bilanzierungsmonat expressible. `planner::calendar` delegates to it, so five
  hand-rolled `match sparte` arms become one mapping — `planner::day_boundary`.
  `planner::balancing_month` is new.

### Removed

- **`tiering::ChangeSource`, `ChangeBatch` and `Position`** — a CDC seam with no
  implementation, caller or test. The argument it carried is now prose in
  `tiering`'s module documentation.

- **`TableConfig::target_file_size` and `max_rows_per_file`** — neither had a
  setter or a TOML key. `max_rows_per_file` was read by nothing;
  `target_file_size` was echoed in `system.config` while the value that reached
  the Parquet writer came from `IcebergSqlCatalog::file_target_bytes`.

### Documentation

- **"Compaction would recover version elision" was backwards.** Elision is a
  property of the *data*: a corrected reading has two versions stored and appears
  twice however the bytes are arranged, and nothing short of dropping the
  superseded version recovers it — which an audit trail may not do. What the
  layout affects is collateral loss, since a scan reads whole files and a
  **coarser** file makes more uncorrected keys share one with a corrected one. So
  compaction moves the elided ratio the wrong way; the one-file-per-window layout
  archival already produces is the one elision likes. The case for compaction is
  the ordinary one — less manifest to plan against — and that is what the page
  says now.

- **The overlap exclusion's motivating example did not motivate it.** The storage
  model introduced it with "an hourly delivery followed by a quarter-hourly one",
  which is a *correction* and therefore carries a higher version — and the
  constraint is scoped to one version, as it must be, since a correction is by
  definition a higher version covering the same span. The example is now a single
  delivery carrying both grids, and the corollary is stated rather than left
  implicit: two rows at different versions may overlap and usually should,
  resolution collapses the ordinary case, and a *partial* re-grid surfaces as
  `surplus` in completeness because it is only visible across rows.

- **The completeness report's column table omitted `resolution`**, which the
  schema has always carried and which is now also a grouping key.

- **Snapshot expiry reclaims metadata, not bytes of readings**, and now says so.
  Upstream's action rewrites metadata only, and the table is append-only anyway —
  every data file an old snapshot referenced is still referenced by the current
  one. What expiry bounds is the metadata JSON, whose snapshot array is parsed on
  every table load. That is the growth that compounds.

- **Scoping is stated over the merge key** rather than over the word "identity",
  since a table keyed by Messlokation puts a *core* column in the key and must be
  scopable on it.

- **A chunked scan is not a single snapshot**, and now says so. Each chunk is its
  own statement on its own connection — the deliberate trade against holding a
  transaction open for minutes on the busiest table in the schema. What it costs
  is bounded by what the hot tier is for: writes are appends at the frontier,
  which sorts last, while the reads that must reconcile are over closed periods.
  Where a read must see one instant, the answer is `ReadMode::AsKnownAt` or a
  pinned snapshot, both of which reproduce.

- **`hot_writer`'s stale-boundary margin is a margin, not a proof.** It is
  thinnest in a backfill and in an archival catch-up, neither of which the writer
  is for; `append` is safe in both, and the second boundary read is what that
  costs.

- **One `SubjectRegistry` spans every table in a deployment.** The mapping lives
  in one `meterstore_subject_map` keyed by natural identifier, so two tables
  registering the same id share a subject and one `erase` unlinks both.

## [0.4.0] — 2026-08-16

`metering` 0.18 typed two identifiers and named a second kind of day. Both were
gaps here, and the second one was silently wrong for a whole commodity.

The crate stored `malo_id` and `melo_id` as unvalidated strings while validating
OBIS codes to the digit — so a transposed digit in the one column that selects
*whose* readings come back was undetectable, even though the MaLo-ID carries a
check digit precisely to make it detectable. And it grouped every daily
aggregate and every completeness report on the **Berlin calendar day**, which is
right for electricity, heat and water and wrong for gas: the German gas market
balances on the Gastag, 06:00 to 06:00 local.

Naming the gas day was only half of it. The rule then has to reach an engine that
has never heard of this crate — and it turns out not to survive being written as
SQL, so the answer is **stored** as a `balancing_day` column instead of published
as an expression.

**Breaking throughout**, as every pre-1.0 release here is: `MeterStore::series`
now returns `Result`, `Completeness` carries a `sparte`, the completeness result
schema has a column more, and the storage schema gains `balancing_day` in both
tiers.

### Added

- **The Gastag.** `meter_gas_day(ts)` is the 06:00–06:00 local day the German
  gas market balances on (GaBi Gas, following Art. 3 Nr. 6 VO (EU) 312/2014);
  `meter_balancing_day(ts, sparte)` picks between it and the calendar day from
  the row's commodity, reading `sparte` **per row** so one statement is correct
  across a mixed portfolio. `meter_expected_intervals` takes an optional third
  `sparte` argument giving the count for the day that commodity is actually
  balanced on.

  In Rust: `planner::balancing_day`, `balancing_day_bounds`,
  `balancing_day_length`, `gas_day_length`, `intervals_in_gas_day` and
  `expected_intervals_in_balancing_day`, plus re-exports of `metering`'s
  `gas_day_start_utc`, `gas_day_end_utc`, `local_gas_day`, `day_end_utc` and
  `shift_back_days`. Each composes `metering`'s primitives; none re-derives a
  calendar rule (P5).

  The DST anomaly is the part that stays wrong even after switching to gas days,
  so it is pinned by test in three places. The clocks change at 02:00/03:00
  local — *before* the 06:00 boundary — so the 23- and 25-hour gas days are the
  ones named after the **Saturday**, the mirror image of the calendar day:

  | 2026 | Calendar day | Gastag |
  |---|---|---|
  | Sat 24 Oct | 96 | **100** |
  | Sun 25 Oct | **100** | 96 |

  Heat and water stay on the calendar day. The rule is *gas*, not *everything
  that is not electricity*, and that is asserted rather than assumed.
- **`encode::parse_malo`** — the counterpart of `canonical_obis` for the other
  half of the merge key. Generic over `TryInto<MaloId>`, so a caller already
  holding a parsed identifier passes it through at no cost.

- **`balancing_day` is now a stored column**, and the balancing-day rule no longer
  leaves this crate as SQL. **Breaking: the storage schema gains a column**, in
  both tiers.

  The rule — Berlin calendar day for electricity, heat and water; the 06:00–06:00
  **Gastag** for gas — needs a zone conversion and a wall-clock six-hour shift,
  and SQL dialects differ on exactly that. No single published expression is right
  everywhere, so the encoder applies `metering`'s calendar once, per row, at write
  time and every reader groups on the answer:

  ```sql
  SELECT balancing_day, SUM(value) FROM readings GROUP BY 1;
  ```

  This is the only derived value the crate persists, against its own rule that
  derived data is computed rather than stored. The objection that answers — a
  second source of truth that drifts — is met by there being exactly one writer,
  asserted against the calendar over every commodity across a whole DST weekend at
  quarter-hour grain. The column is `NOT NULL` with no default, so a hand-written
  `INSERT` that omits it fails at the write rather than storing a wrong day.

  Completeness now groups on the stored column instead of invoking a UDF per row,
  and the DuckDB interop suite checks the stored answer against MeterStore's own
  calendar across the autumn transition, histogram for histogram.

### Fixed

- **A gas Lastgang grouped by `meter_local_day` booked six hours a day into the
  wrong Bilanzierungstag.** Not at the transitions — every day of the year, with
  totals that still looked plausible. This is the same class of error as
  `date_trunc('day', …)` over an electricity series, which this crate already
  refused; it simply had no name for the gas case until `metering` 0.18 supplied
  one.
- **Documentation examples used a MaLo-ID whose check digit is wrong.** Every
  `12345678901` in the README, the site and the tests is now an identifier the
  Bildungsvorschrift actually admits.
- **The integration suite was flaky, and not because of the code.** The two
  foreign-engine suites start a container per test, and both do real network work
  before printing anything — DuckDB runs `INSTALL iceberg`, Python runs
  `pip install pyiceberg`. Cargo starts one test thread per core, so a dozen of
  those landed at once, starved each other, and whichever lost the race reported
  `WaitContainer(StartupTimeout)`. The failing *set* changed every run, which is
  the worst kind of red: it says nothing about the code, and a suite nobody can act
  on is a suite nobody reads.

  Fixed with a bound (`tests/it/containers.rs`) on how many foreign-engine
  containers exist at once, not merely a longer timeout — an unbounded suite scales
  its own load with the host's core count, so a bigger CI runner makes it *worse*.
  The full suite now passes in parallel, repeatedly, with no thread cap in CI.

- **The hot-tier write path could deadlock itself under concurrency.** After an
  insert skipped rows (an ordinary redelivery), the check that a skipped row is a
  true replay rather than a changed value under an existing version reached back
  to the *pool* for a second connection — while already holding one, and on the
  reporting path holding one inside an open transaction. With a pool of `n`, `n`
  concurrent writers each held one connection and each waited for an `n+1`th that
  could only free up when one of them finished. Nothing timed out; the write path
  simply stopped, under exactly the concurrency it was built for.

  The check now runs on the connection it was given. That is also the only
  spelling that is *correct*: it has to see the same snapshot as the insert it is
  checking, which a separate connection does not.

- **Predicate extraction could narrow a scan and lose rows.** `time_range` saw
  through *any* cast around `from`, so a truncating one — `CAST("from" AS DATE)`,
  or a cast down to seconds — was read as a bound on the column itself. A
  whole-day predicate then extracted a one-microsecond range and the other 95
  intervals were dropped from the scan, silently, because the rows simply were not
  there. It now sees through only casts that preserve every stored instant
  (microsecond and nanosecond timestamps); anything else leaves the range
  unbounded, which costs a wider scan and never a row. This restores the module's
  stated invariant that the analysis may only ever be wrong in the widening
  direction.

### Documentation

- Corrected the explanation of why resolution elision is cold-only, which
  contradicted itself in both the rustdoc and the querying guide. Tier
  disjointness is what makes per-tier reasoning *sound*; excluding the hot tier is
  a separate and merely *practical* point (PostgreSQL keeps no per-file
  statistics). The old wording asserted the first and then argued the second.
- The architecture guide's snapshot-summary sample showed an API that does not
  exist; it now shows the `fast_append` / `set_snapshot_properties` call the code
  actually makes.
- Repaired a doc comment on the hot writer that had two summary lines merged into
  one block, and a truncated sentence in the encoder.

## [0.3.0] — 2026-08-10

A correctness release, and a large one. A fresh table could not reach the present.
The tiering watermark could move backwards through a dependency's retry, and
could be stranded outright by following this project's own maintenance advice. A
completeness report could call a series with a real gap complete. Two ingest
workers could not open the same new partition. The correctness oracle disagreed
with a store behaving correctly. Several of these were documented as impossible.

Two things ran through most of them. Surfaces that were **declared, documented and
never computed** — a metric derived from configuration that could not reach the
value it was alerted on, a status column that always returned `-1`, a
property-test level built on a dependency nothing imported. And **dependencies
carried as claims rather than as code**, which is how one went five majors stale
without anything noticing.

`metering` 0.17 also arrived, and with it a statutory correction that inverts a
premise the privacy design rested on: § 60 Abs. 6 MsbG is a deletion duty, not a
retention mandate.

**Breaking throughout.** The crate is unpublished, so every rename is a hard cut:
`MeterInterval::value_kwh` → `value`, `system.tables.hot_rows` → `hot_partitions`
plus `partitions_ahead`, the `cdc` feature and its dependency removed, and
`Oracle::for_table` where `Oracle::new` was silently wrong for a table with
identity columns.

### Fixed

- **The supply-chain audit was reading a graph nobody deploys.** `cargo deny` ran
  without `--all-features`, and the cloud object stores are opt-in — so every
  dependency the S3/GCS/Azure features pull, including all three credential
  signers, was exempt from the licence and advisory gate by omission. It now runs
  with `--all-features` in both CI and the `justfile`, which immediately surfaced
  two findings no previous audit could have seen:

  - `rsa` 0.9 carries **RUSTSEC-2023-0071** (the Marvin Attack) with *no patched
    version available*. `deny.toml` previously asserted the pinned version was
    "now past" it, which was never true. Recorded as an accepted risk with the
    actual reasoning: the only use is client-side blinded PKCS#1 v1.5 *signing*
    of an OAuth assertion, with no remote party able to time the operation.
  - The `rsa` ban's wrapper list was missing `reqsign-google`, so the ban that
    exists to catch a private-key operation appearing somewhere new would not
    have caught one arriving through the GCS signer.
- **The error documentation named a metric that does not exist.**
  `Error::InvariantViolated` pointed at `meterstore_invariant_violations_total`,
  in Prometheus naming with a counter suffix; the instrument is the
  `meterstore.tiering.invariant_violations` gauge. An operator grepping for the
  documented name would have found nothing.
- **Snapshot expiry could make a table unreadable after out-of-band maintenance.**
  The documented workaround for compaction and orphan cleanup — run them with
  Spark or PyIceberg against the same table — produces valid Iceberg commits that
  carry no tiering watermark, so the boundary lookup walks back the parent chain
  to find one. Expiry then breaks that walk, and not only by removing the snapshot
  carrying the boundary: removing **any intermediate ancestor** leaves the chain
  with a hole, the walk stops at a parent id that no longer resolves, and *every*
  query fails at once. A maintenance job following this project's own advice would
  have bricked the table. Expiry now re-stamps the boundary onto the current
  snapshot first, so there is no chain to punch a hole in, and protects the path
  anyway for a history it did not create.
- **Arrow Flight `GetFlightInfo` executed the query instead of planning it.** The
  module documentation said it planned; it ran the statement, materialised every
  batch and threw the rows away to keep the schema. A BI tool's ordinary
  `GetFlightInfo` → `DoGet` sequence therefore cost **two full scans**, on the one
  surface built for BI tools — and `CreatePreparedStatement` executed the statement
  it was preparing. Both now plan and stop.
- **A batch could ship without its provenance.** The Flight stream re-stamps the
  schema carrying the watermark onto every batch and fell back to the original
  batch if that failed, silently dropping the one thing the re-stamp exists for.
- **The correctness oracle ignored identity columns.** It keyed on
  `(malo_id, obis_code, from)` — the core merge key — so a table declaring a
  tenant discriminator would have two tenants' readings folded into one key, with
  a winner picked across them. The oracle would then report a mismatch against a
  store behaving correctly, which is the worst way for a reference to be wrong.
  Unreachable in this crate's own suites, which use the default configuration, and
  reachable by exactly the deployments `testkit` is public *for*.
  `Oracle::for_table(&config)` now takes the merge key from the same validated
  configuration the store was built with. An equal version also no longer
  overwrites, matching `ON CONFLICT DO NOTHING`.
- **The documented property-test level did not exist.** `proptest` was a declared
  dev-dependency that nothing imported, while the design listed a whole test level
  built on it and claimed "property tests cover it" for the filter-pushdown
  contract. Three statements about how far to trust the suite, none of them true.
  The properties are real and load-bearing, so they were written rather than the
  claim deleted — predicate-extraction conservatism, tier-split exhaustiveness and
  disjointness, and elision conservatism, each against an independently
  implemented reference. Confirmed by mutation: making `<=` produce an exclusive
  bound leaves all eleven hand-written predicate cases passing and fails the
  property within a few dozen generated inputs.
- **Concurrent ingest workers could not open the same new partition.** Every
  writer ensures the partitions for the range it is about to write, so the first
  batch of a new day has every worker creating the same partition at once — the
  ordinary topology, not an unusual one. Check-then-create loses that race:
  `CREATE TABLE IF NOT EXISTS … PARTITION OF` does not suppress the collision, so
  the losers got `relation … already exists` and the batch failed. Creation now
  runs under a transaction-scoped advisory lock with the existence re-check
  inside it. The fast path is unchanged — an existing partition costs one
  catalogue lookup and never reaches the lock.
- **The `obis_code` bloom filter was sized for the meter population.** A filter
  costs about 9.6 bits per declared distinct value at 1 % false positives, so a
  column holding a few dozen OBIS codes got a ~120 KiB filter on every data file.
  Sized per column now, as the design always described it.
- **The alert for "writes are about to fail" was a constant.**
  `meterstore.tiering.hot_partitions_ahead` recorded
  `(settlement_lag + headroom) / partition_step` — a pure function of
  configuration. It reported the runway a healthy deployment *would* have, was
  unaffected by an archiver that had stopped creating partitions, and could
  therefore never reach the zero it is alerted on. It is now counted from the
  partitions the hot tier actually holds.
- **`system.tables.hot_rows` was always `-1`.** Counting the hot tier means
  scanning it, so the column was declared, documented, exposed — and never
  computed. A diagnostic that returns a sentinel is worse than an absent one,
  because an operator reads it as a number. Replaced by `hot_partitions` and
  `partitions_ahead`, which answer what row counts were wanted for at the cost of
  a catalog lookup. `healthy` now covers both ways a table stops working: wrong
  answers now (`invariant_violations`), and no answers shortly
  (`partitions_ahead` at zero).
- **Decoding could attribute one delivery's values to another's source.** The run
  key that groups rows back into series listed six columns by hand and omitted
  `source_kind`, `source_detail`, `provenance`, `resolution` and `recorded_at` —
  every series-level field that is read once per run. Two deliveries agreeing on
  version and scope but not on origin folded into a single series carrying the
  first one's provenance for all of it: the numbers stayed right and the audit
  trail lied. The key is now everything that is not an interval column, so a
  column added to the schema joins it automatically, and a decoded series is one
  OBIS channel as the MSCONS handbook defines it.
- **A catalog result carried every table's boundary, not the ones it read.**
  `QueryResult::watermark()` is the conservative boundary — the oldest reported —
  so a single-table query in a twenty-table catalog was attributed to whichever
  unrelated table archives least often. The relations a statement scans are now
  read off the logical plan, and only those tables' boundaries are attached.
- **Declared column names were unvalidated.** They reach PostgreSQL DDL, the
  resolution SQL, the `unnest` alias and the scan projection as quoted
  identifiers, which cannot be parameterised — and a name from a TOML file is
  user input. A declared name must now be a plain ASCII identifier, checked once
  where names enter the system.
- **A fresh table archived from 1970.** A table with no snapshot reports the
  Unix epoch as its tier boundary, and window selection starts there — so a
  deployment created in 2026 committed one empty Iceberg snapshot **per day
  since 1970** before reaching a single real row: some twenty thousand commits, a
  few dozen per maintenance cycle, and a snapshot list that ten-year retention
  never lets it recover from. An archival window whose partition does not exist
  now extends to the next partition that does, or to the archival horizon — one
  commit whatever the gap, which also covers every later idle stretch. Every
  integration suite had hidden this behind a `testkit` helper that seeded the
  boundary by hand.
- **The watermark could move backwards.** `iceberg` 0.10 retries a conflicting
  commit itself, refreshing the base and re-applying the same action —
  *including the snapshot summary it was built with*, four times by default. A
  late correction that lost a race to an archival commit therefore republished
  its own older watermark, over intervals PostgreSQL had already purged, with
  nothing reporting a failure. Library retry is now disabled on the table
  (`commit.retry.num-retries = 0`) and the retry loop is MeterStore's,
  re-deriving the summary and the monotonicity assertion from the refreshed base
  on each attempt.
- **Completeness let a surplus cancel a gap.** `missing` was `expected - actual`
  over the whole range, so a day four intervals long netted against a day four
  intervals short and the channel reported **complete** — the one answer a
  completeness report must never give about a series with a real gap. Both
  `missing` and `surplus` now accumulate per local day.
- **`as_known_at` did not reach the raw relation.** The transaction-time ceiling
  lived only inside the resolution plan, so `readings` honoured it and
  `readings_versions` — the audit relation — returned rows recorded after the
  instant the session claims to reproduce. It is now applied by an explicit
  filter on the tiered provider, below the projection, like the version ceiling.
- **Data-file names could collide.** The Parquet file-name suffix was a
  wall-clock nanosecond reading, which is neither unique across processes nor
  nanosecond-resolution on every platform. Two writers sampling the same instant
  produce the same object key, and the second write overwrites a committed data
  file. Now 64 bits from the OS CSPRNG.
- **`as_of` reported an epoch watermark** when the pinned snapshot was written
  out of band — an external compaction, which this design explicitly recommends
  as the workaround for having none of its own. The lookup now walks back from
  the pinned snapshot to the most recent MeterStore commit, as the cold tier's
  own watermark lookup does.
- **An empty delivery failed on a table with deployment columns**, and panicked
  in `hot_writer`. A zero-row batch now builds empty arrays of the declared type,
  and a series contributing no rows is skipped rather than validated.

### Added

- **`cold::S3TablesCatalog`**, behind the new `s3tables` feature — the cold tier
  over an AWS S3 Tables table bucket, identified by ARN. Credentials come from the
  ambient AWS chain, so there is no credential field to be a second place for a
  key to live. The feature implies `object-store-s3`, because a table bucket's
  data files are still in S3 and a catalogue that compiled but could not open a
  file would be a trap.

  S3 Tables was previously recorded as blocked upstream. That was wrong, and
  specifically wrong in a way worth naming: it has two front doors, and only the
  Iceberg REST one needs SigV4 signing that `iceberg-catalog-rest` cannot provide.
  The native API goes through the AWS SDK, which signs for itself.
- **`IcebergCold::catalog`**, and a suite that drives the whole store through a
  catalogue MeterStore did not build. The cold tier has always claimed to accept
  any `Arc<dyn Catalog>` — that is what makes REST, Polaris, Glue and Nessie a
  configuration choice — but every test reached it through the one
  implementation the crate builds itself, so the claim rested on inspection.
- **`MeterStore::reassert_watermark`** — put the tiering boundary back on the cold
  table's current snapshot after out-of-band maintenance. It republishes what the
  history already says, so it cannot move the boundary, and is a no-op when the
  current snapshot already carries one. `expire_snapshots` calls it first, so a
  deployment on the maintenance schedule need not.
- **`MeterStore::describe`** — what a statement would produce, without running it:
  the schema, the boundary it would run against, and the tiers it would read.
  Plans the query, so a syntax error or unknown relation is reported, and stops.
- **PyIceberg interop.** A second foreign engine, and specifically the one the
  documentation tells operators to use for the maintenance MeterStore cannot do
  itself. Five assertions in a container: the rows match the reference; the schema
  survives with its field ids and its decimal precisions; the partition spec, sort
  order and format version are legible; the **tiering watermark can be read out of
  the snapshot summary** by an ordinary Iceberg reader; and the published
  resolution rule reproduces MeterStore's answer while the naive sum is
  demonstrated to overstate. Compaction and orphan cleanup being "blocked upstream,
  run it out of band with PyIceberg" was previously a sentence with nothing behind
  it.
- **A documentation site**, built with Zola and deployed to GitHub Pages: a
  landing page and eleven guides covering the architecture, storage model,
  writing, querying, reproducibility, completeness, operations, external engines,
  privacy and configuration. `zola check` runs in CI, so a rename that leaves a
  dangling internal link fails the build rather than publishing a 404.
- **The README is a README again** — from 64 KB to about 11 KB. Pitch, the four
  decisions that carry the weight, one worked example, requirements and pointers;
  the depth lives on the site.
- **`values = [...]` on a TOML `extra_columns` entry**, declaring the same coded
  vocabulary `coded_column` declares from Rust. The file's whole claim is that it
  is a front end over the *same* validated types with no setting reachable from
  one and not the other; coded columns arrived on the builder and quietly made
  that false.
- **`MeterStore::anonymise_before`** — the standing retention sweep. § 60 Abs. 6
  MsbG obliges the Messstellenbetreiber to *löschen oder anonymisieren*
  personenbezogene Messwerte as soon as they are no longer needed, and after three
  years at the outside. `erase_subject` answers an Article 17 request; this
  answers the duty nobody files. Keyed to the **latest reading** attributed to
  each reference rather than to the registration, over the raw versioned relation
  because a superseded version is still a stored personal value. Idempotent.
- **`HotStore::partition_starts`** — enumerate a table's partition bounds.
  `None` means *cannot say*, deliberately distinct from `Some(vec![])`; a store
  that cannot answer keeps the one-window-per-commit behaviour.
- **`testkit::postgres`** — one PostgreSQL container per process and a fresh
  database per caller, with `isolated_database` as the opt-out.
- **`watermark::align_to_step`** — the alignment shared by hot partition bounds
  and archival windows, previously duplicated in the PostgreSQL tier.

### Changed

- **Three supply-chain advisory exceptions removed** because they no longer match
  anything: `tokio-tar` left the tree with the `testcontainers` upgrade, and the
  pinned `rsa` is past the Marvin-attack range. A stale ignore is exactly the
  failure the policy warns about — it looks like the check passed. The prose
  explaining each exception went with it.
- **`sqlx` joined the single-sourced dependency gate.** It was not covered, and it
  needs to be for the same reason `datafusion` is: two majors give two `PgPool`
  types, and this crate takes a pool from the application. Found by attempting the
  0.9 upgrade — whose compile-time dynamic-SQL audit would turn the injection
  promise into a checked one — and getting a graph with both 0.8 and 0.9 and a
  panic inside a connection pool rather than a build failure. `iceberg-catalog-sql`
  pins 0.8, so the upgrade waits for upstream; the gate now says so.
- **`subtle` and `tokio-test` removed** — declared, compiled, never used, the same
  shape as `rustcdc`.
- **Dependencies refreshed** where nothing upstream blocks them: `getrandom`
  0.3 → 0.4, `toml` 0.9 → 1.1, `criterion` 0.7 → 0.8, `testcontainers`
  0.23 → 0.27 with `testcontainers-modules` 0.11 → 0.15. `datafusion`/`arrow` stay
  on 53/58 because `iceberg-datafusion` 0.10.1 pins them, which is what the design
  already said would happen.
- **`rustcdc` and the `cdc` feature are gone.** The dependency was declared,
  compiled and never called — carried to signal intent, and five majors stale
  (0.7 against 0.12) without anything noticing, because nothing could. A feature
  flag whose only effect is to compile an unused dependency is not a seam. The
  `ChangeSource` trait it stood for is expressed in `futures::Stream`, so it is
  now unconditional and costs a definition, which is what the seam was always
  claimed to cost.
- **`HotWriter` caches the partitions it has confirmed.** Reading the tier
  boundary once was not the only per-batch round trip: ensuring partitions cost a
  catalogue lookup per partition per batch, and a run of meter-days re-asks about
  the same one or two throughout.
- **`metering` 0.16 → 0.17.** `MeterInterval::value_kwh` is now `value`, for
  precisely the reason the storage column already carried that name — the two
  agree again. The removed `smgw`, `tariff_window`, `register` and `demand`
  modules were not storage-relevant, so the rename was the whole of the
  migration.
- **The privacy documentation is rewritten around the statute it cites.**
  Earlier drafts held that Art. 17(3)(b) usually defeats an erasure request
  because energy law mandates retention. § 60 Abs. 6 MsbG is a **deletion duty**
  with three years as a *ceiling*; `metering` corrected the same inversion in its
  own documentation at 0.17. The pseudonymisation design is unchanged and is now
  the primary mechanism rather than insurance — the statute admits
  *anonymisieren* explicitly, which is the branch an append-only lake can take.
  "No partial data expiry" narrows from a blocker to a limitation on policies
  that genuinely demand the bytes gone.
- **The integration suite shares one PostgreSQL.** Roughly fifty container
  starts became one, per-test isolation moved from a container to a database, and
  the initial connect retries while the server finishes coming up. **122 s → 17 s**,
  and the intermittent connection failure that looked like a defect in the code
  under test is gone. Suites needing real infrastructure are now uniformly behind
  the `testkit` feature.

## [0.2.0] — 2026-08-08

Catalog construction and coded attribute columns. This changelog starts here; see
`git log` for what preceded it.
