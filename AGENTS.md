# AGENTS.md — meterstore

Hot/cold tiered storage for German metering time series: PostgreSQL holds the
recent interval window, Apache Iceberg holds the settled history, one timestamp
— the tiering watermark — separates them, and one SQL statement spans both.

`README.md` is the public entry point and `site/content/docs/**` the operator
guides; neither is restated here. This file is what an agent needs before
touching code.

## Ground rules

- **Breaking changes are fine.** The crate is published and pre-1.0: hard cuts,
  no deprecation shims, the SQL schema edited in place with no migration.
- **Docs and comments state current truth only** — no changelogs, no "used to",
  no backlog pointers. `CHANGELOG.md` is the one place history belongs, and
  `doc_conventions.rs` enforces the rule over `src/`, `tests/`, `site/` and
  `README.md`. It does not read `CHANGELOG.md`, which is the point, nor this
  file, which teaches the convention by counter-example below.

  It bites hardest on a regression test, where narrating the incident is the
  natural way to justify the check and the wrong way to write it. **State the
  invariant, then the failure it prevents, in the present.**

  ```text
  ✗  `align` used to zip columns positionally, so reordering two Utf8
     attribute columns transposed their values.
  ✓  Columns are matched by name: a positional pairing would make declaration
     order part of the stored contract, and `evolution::compare` — which gates
     the change — matches by name, so a permutation passes it.
  ```
- **Every regulatory claim cites a primary source, by clause.** `[MsbG § 60
  Abs. 6]`, `[BK6-24-174 MaBiS Kap. 4]`, `[Allg. Festlegungen 6.1c Kap. 3.1]`.
  A `§` reaches this codebase from a published document or not at all.
  `just references` fetches every one of those documents; they are not committed,
  because they are third-party publications under their own terms.
- **The domain is upstream.** Any rule about what a measurement *means* —
  calendars, units, quality, identifier check digits, Ersatzwertbildung — is
  `metering`'s. This crate holds storage facts. A second implementation here is
  a second thing to keep correct, and it will drift.
- **A defect class becomes a guard.** The deliverable for a bug is the check
  that makes it unrepresentable, and that check must **fail if the fix is
  reverted**. A test that passes against the old code proves nothing.

## Build and test

`just check` is the gate: `fmt-check lint test deps doc deny features
bench-check`. Nothing is done until it passes.

```bash
just dev          # fmt + unit tests only, no Docker — the inner loop
just check        # the gate, everything CI runs
just test         # the whole suite (needs Docker)
just unit         # unit tests only
just integration  # integration tests only (needs Docker)
just deps         # fail if a single-sourced crate appears twice
just features     # every feature set a published crate's users can select
```

### Six traps, each of which has produced a false verdict

- **`just dev` proves almost nothing.** Unit tests run without Docker, and
  everything this crate is actually about — partitions, locks, DDL, streaming,
  archival, Iceberg commits, both serving surfaces — lives behind Docker. A green
  `--lib` is the most misleading signal in this repository.
- **`cargo check --lib` compiles none of the tests.** `tests/it/` is one binary,
  so a breaking change can leave the library clean and seven call sites broken.
  **Use `cargo clippy --all-features --all-targets`** before reporting anything
  as compiling.
- **`--all-features` hides a feature bug entirely.** An optional dependency
  reached from non-optional code compiles fine as long as *something* enables it;
  the failure appears only for somebody who picked a narrower set — that is,
  after publishing, in their build. `just features` is the check.
- **Never pipe the gate through `tail` or `grep` for the verdict** — the pipe
  masks the exit code. Run it into a log with `echo EXIT:$?` appended, then read
  the exit line and grep for `test result: FAILED`.
- **Report counts from output, never from memory.** The same figure appears in
  several files and not every copy is guarded; see *Counts* below.
- **Nothing else may compile while the gate runs.** Two cargo invocations sharing
  `target/` race on the fingerprint directory, and the result reads as a build
  error: `failed to write target/debug/.fingerprint/… (os error 2)`, reported
  against whichever crate lost. An editor's rust-analyzer is enough to cause it.
  The tell is that the failure does not reproduce and that recipes *after* the
  failing one passed — a real feature bug cannot fail for `s3tables` and pass for
  `all`. Re-run alone before believing it, or set `CARGO_TARGET_DIR` to somewhere
  of its own.

## The dependency graph is load-bearing

`datafusion`, `arrow` (and every `arrow-*`), `iceberg`, `parquet`, `metering`,
`time`, `rust_decimal`, `sqlx` and `sqlx-postgres` must each appear **exactly
once** in the `--all-features` graph. `just deps` fails the build otherwise, and
fails when `cargo tree` cannot resolve, because the failure is otherwise
invisible: two `arrow` majors are two incompatible `RecordBatch` types and two
`TableProvider` traits. The error is a trait-bound mismatch on a type whose name
reads identically in both halves. CI runs the same recipe.

**One duplicate is admitted, by name:** the `sqlx` 0.8 `iceberg-catalog-sql`
requires for its own pool. `Cargo.toml` declares it as `sqlx-catalog`, because
its driver, runtime and TLS backend come from nowhere else; a unit test fails if
one is missing.

**The store owns its pool; `sqlx` is private.** `Database` is the only way a
connection is opened — TLS by the crate's rule, advisory locks cleared at
check-in — and no published signature names a `sqlx` type
(`no_published_signature_names_sqlx`). Work that must share an erasure's
transaction is an `ErasureCascade`, never the caller's transaction.

Arrow is reached **through DataFusion** (`crate::arrow`), never by a direct
`arrow::` path, so a single `use` cannot introduce skew. `Cargo.toml` does
declare `arrow` — the copy DataFusion already pulls, for the `json` feature
`QueryResult::to_json` needs — and `just deps` fails the build if it diverges.

**`iceberg-rust` sets the ceiling.** `iceberg-datafusion` 0.11 requires
`datafusion = "^54"` and `iceberg` 0.11 requires `arrow-array = "^58.4"`, and
single-sourcing makes both binding on the whole crate. DataFusion 55 and Arrow
59–60 exist and are unreachable while `iceberg-datafusion` is in the graph. That
is not a maintenance lag — do not "update" it.

## Where things are

| | |
|---|---|
| The public narrative | `site/content/docs/**` |
| The API contract | the crate's own rustdoc |
| History | `CHANGELOG.md`, and only there |
| Primary sources | `just references` fetches them; not committed |

A working set of design notes may also be present and is **not** part of the
repository: `concepts/`, whose roadmap is the one list of open work, and the
Spec Kit specifications each roadmap item names (`specs/`, `.specify/`). Treat
anything they say as context, never as the contract: the contract is the
rustdoc, the site and this file. Nothing tracked cites a specification —
`just concepts` checks it.

## Counts

Figures drift and only some of them are guarded. The test count lives in
`README.md`'s status section and in `CHANGELOG.md` where a release moves it —
`doc_conventions.rs` reads both. If the working notes are present they carry the
same figures with nothing checking them, so move every copy in the same pass and
take the numbers from test output rather than from memory.

## The two error variants that must not be conflated

| | Means | Who acts |
|---|---|---|
| `IntegrityViolation` | The store **stopped something from becoming true** — an overlapping delivery, two network operators for one reading | The producer |
| `InvariantViolated` | Something **already is true that should not be** — rows below the watermark still in PostgreSQL | An operator, now |

Whoever is paged for the second must not be woken by the first. `is_retryable()`
is the other split that matters: a lost connection and a declined lock are worth
retrying; a refused delivery and an invalid configuration never succeed on one.

## Known-wrong instincts

Things that look like bugs and are not, and things that look fine and are not.

- **`can_advance_to` permits equality, deliberately** — a correction append
  republishes the boundary unchanged. An *archival* commit is stricter and
  refuses it, because equality there means another writer already committed the
  window.
- **The hot scan reads partitions by name, never through the parent.** That is a
  correctness decision, not an optimisation. Reading the parent and adding
  whatever is detached is two reads of one catalogue, and a detach between them
  loses a whole window.
- **`lease_contended` and `deferred` are not failures.** Every replica running
  the same schedule is the intended deployment. Alert on watermark lag.
- **Compaction is a refusal, not a gap.** It destroys the file statistics merge
  elision proves itself from — thirty daily files at thirty versions become one
  file spanning thirty versions, and the planner can no longer prove what is still
  true. Read the maintenance contract before recommending any tool that rewrites
  data files.
- **`reader_grace` is hysteresis, not the safety argument.** What keeps a live
  plan whole is the reader-pin registry (`meterstore_pins`, D43): reclamation
  consults it in the transaction that drops, a pin taken during the drop is
  caught on the reader's side by re-reading `meterstore_reclaimed`, and a query
  that outlives `max_pin_age` fails naming the window it lost. A `HotStore` that
  cannot register a pin (the trait's default) has only the grace, and there the
  failure is silent: a plan that outlives `reader_grace` comes back one window
  short with no error. Pinning also writes on every hot-half plan, so a read-only
  role cannot query.
