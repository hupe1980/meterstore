# Changelog

All notable changes to `meterstore` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

The crate is **unpublished** and pre-1.0. Until the first release every version
is a hard cut: breaking changes carry no deprecation shim, and the SQL schema
changes in place rather than through a migration.

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
