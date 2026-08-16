# MeterStore development tasks.
#
# `just` with no arguments lists everything.

set shell := ["bash", "-uc"]

# Single-sourced crates: two versions of any of these in one graph means
# mutually incompatible types (a second `arrow` breaks `RecordBatch`, a second
# `datafusion` breaks `TableProvider`). Checked by `just deps`.
SINGLE_SOURCED := "datafusion|arrow|iceberg|parquet|metering|time|rust_decimal|sqlx"

_default:
    @just --list --unsorted

# Everything CI runs. Run this before pushing.
check: fmt-check lint test deps doc deny features bench-check

# Fast inner loop: format, then unit tests only (no Docker).
dev: fmt unit

# --- build ------------------------------------------------------------------

# Compile the library and all targets.
build:
    cargo build --all-targets --all-features

# Remove build artifacts and any warehouse left by a failed test.
clean:
    cargo clean
    rm -rf data

# --- test -------------------------------------------------------------------

# Unit tests only. No Docker required.
unit:
    cargo test --lib --all-features

# Integration tests. Requires a running Docker daemon.
integration:
    @just _require-docker
    cargo test --test '*' --all-features

# The whole suite.
#
# Unbounded. The old cap existed because every test started its own PostgreSQL
# container, so the limit was Docker's rather than the CPU's. The suites now share
# one container and take a database each (`testkit::postgres`), and that server is
# started with a raised `max_connections` — without which sharing simply moves the
# ceiling from container slots to backend slots, and the failure looks like a
# broken test rather than an exhausted resource.
#
# The foreign-engine suites (DuckDB, PyIceberg) do still start a container
# each, and are bounded on their own in `tests/it/containers.rs` — which is why
# this needs no cap.
#
# Set a number to pin the degree on a constrained machine: `just test-threads=4 test`.
test-threads := "0"

test:
    #!/usr/bin/env bash
    set -uo pipefail
    just _require-docker || exit 1
    if [ "{{test-threads}}" = "0" ]; then
      cargo test --all-features
    else
      cargo test --all-features -- --test-threads={{test-threads}}
    fi

# Run one test by name, with logs shown.
test-one NAME:
    RUST_LOG=meterstore=debug cargo test --all-features {{NAME}} -- --nocapture

# Re-run tests on change.
watch:
    cargo watch -x 'test --lib --all-features'

_require-docker:
    #!/usr/bin/env bash
    if ! docker info >/dev/null 2>&1; then
      echo "error: Docker is not running — integration tests need it." >&2
      echo "hint: run 'just unit' for the tests that do not." >&2
      exit 1
    fi

# --- quality ----------------------------------------------------------------

# Format in place.
fmt:
    cargo fmt --all

# Fail if anything is unformatted.
fmt-check:
    cargo fmt --all -- --check

# Clippy, warnings as errors.
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Build docs, warnings as errors.
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

# Open the docs in a browser.
doc-open:
    cargo doc --no-deps --all-features --open

# Every feature combination a published crate's users can select.
#
# `--all-features` hides this entirely: an optional dependency reached from
# non-optional code compiles fine as long as *something* enables it. The failure
# only appears for someone who picked a narrower set — that is, only after
# publishing, in someone else's build.
features:
    #!/usr/bin/env bash
    set -uo pipefail
    fail=0
    check() {
      local label="$1"; shift
      printf '%-24s' "$label"
      if out=$(cargo check --quiet --all-targets "$@" 2>&1); then
        echo "ok"
      else
        echo "FAILED"
        printf '%s\n' "$out" | grep -E '^error' -A 6 | head -20
        fail=1
      fi
    }
    check "no features"    --no-default-features
    check "rest-catalog"   --no-default-features --features rest-catalog
    check "flight"         --no-default-features --features flight
    check "catalog-facade" --no-default-features --features catalog-facade
    check "s3tables"       --no-default-features --features s3tables
    check "testkit"        --no-default-features --features testkit
    check "all"            --all-features
    if [ "$fail" -ne 0 ]; then
      echo "a feature combination does not build; a published crate's users can select it" >&2
      exit 1
    fi
    echo "feature matrix OK"

# Fail if a single-sourced crate appears at two versions.
deps:
    #!/usr/bin/env bash
    # `cargo tree -d` also lists a package twice when it is built for both host
    # and target at the same version. Only differing versions are a conflict.
    set -uo pipefail
    conflicts=$(cargo tree -d --edges normal 2>/dev/null \
      | grep -oE "^({{SINGLE_SOURCED}}) v[0-9.]+" \
      | sort -u \
      | awk '{ seen[$1]++ } END { for (n in seen) if (seen[n] > 1) print n }')
    if [ -n "$conflicts" ]; then
      echo "error: multiple versions of a single-sourced dependency:" >&2
      echo "$conflicts" >&2
      cargo tree -d --edges normal
      exit 1
    fi
    echo "single-sourced dependencies OK"

# Licence and advisory policy. Every exception in deny.toml carries a reason.
#
# `--all-features`: the cloud object stores are opt-in and pull the credential
# signers, so the default graph is one no deployment runs.
deny:
    #!/usr/bin/env bash
    set -uo pipefail
    if ! command -v cargo-deny >/dev/null 2>&1; then
      echo "cargo-deny is not installed; CI still runs it. \`cargo install cargo-deny\`" >&2
      exit 0
    fi
    cargo deny --all-features check

# --- site -------------------------------------------------------------------

# Serve the documentation site with live reload.
site:
    cd site && zola serve

# Build it, failing on a dangling internal link.
site-build:
    cd site && zola check && zola build

# --- examples ---------------------------------------------------------------

# Encoding, versioning and tiering, with no database.
example:
    cargo run --example encoding

# --- measurement ------------------------------------------------------------

# CPU-bound benchmarks: encode, decode, planning. No database needed.
#
# Throughput and compression are *not* here — they need real PostgreSQL and a
# real object store, and measuring them against fakes would produce numbers that
# look like the targets and mean nothing.
bench:
    cargo bench --bench encoding

# Compile the benchmarks and run one iteration each, without collecting samples.
# Fast enough for `just check` to keep them from rotting.
bench-check:
    cargo bench --bench encoding -- --test

# --- maintenance ------------------------------------------------------------

# Show outdated dependencies.
outdated:
    cargo outdated --root-deps-only

# Update the lockfile within existing version requirements.
update:
    cargo update
    @just deps

# Test coverage report.
coverage:
    cargo llvm-cov --all-features --html
    @echo "report: target/llvm-cov/html/index.html"
