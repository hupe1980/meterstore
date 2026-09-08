# MeterStore development tasks.
#
# `just` with no arguments lists everything.

set shell := ["bash", "-uc"]

# Single-sourced crates: two versions of any of these in one graph means
# mutually incompatible types (a second `arrow` breaks `RecordBatch`, a second
# `datafusion` breaks `TableProvider`). Checked by `just deps`.
SINGLE_SOURCED := "datafusion|arrow|iceberg|parquet|metering|time|rust_decimal|sqlx|sqlx-postgres"

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
    # `-D warnings`, because CI sets it globally and a feature set that only
    # *warns* — an import used by every backend but the one selected — fails
    # there and passes here otherwise. A local gate weaker than the remote one
    # is a gate that reports success for the run that matters.
    export RUSTFLAGS="${RUSTFLAGS:-} -D warnings"
    check() {
      local label="$1"; shift
      printf '%-24s' "$label"
      # `test --lib`, not `check`: a feature set can compile and still be wrong.
      # The documented example names both a catalogue and an object-store scheme,
      # so validating it needs both features present — a coupling a type check
      # cannot see. Unit tests only; the integration suite needs Docker and runs
      # once, against `--all-features`.
      if out=$(cargo test --quiet --lib "$@" 2>&1); then
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
    check "cli"            --no-default-features --features cli
    check "catalog-facade" --no-default-features --features catalog-facade
    check "sql-catalog"    --no-default-features --features sql-catalog
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

# --- cli --------------------------------------------------------------------

# Build the `meterstore` binary and print its help.
cli *ARGS:
    cargo run --features cli --bin meterstore -- {{ARGS}}

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

# --- sources ----------------------------------------------------------------

# Gitignored: the files are third-party publications under their own terms.
# Existing files are kept, so a re-run only fetches what is missing; anything
# that cannot be fetched is reported at the end and indexed in `specs/README.md`
# with its source.
#
# 📚 Rebuild `specs/` — the primary sources every citation is checked against
specs:
    #!/usr/bin/env bash
    set -uo pipefail
    ua='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36'
    missing=""
    # fetch DIR FILE URL [ALT_URL]
    fetch() {
        mkdir -p "specs/$1"
        if [ -s "specs/$1/$2" ]; then echo "kept     $1/$2"; return 0; fi
        for url in "$3" "${4:-}"; do
            [ -n "$url" ] || continue
            if curl -fsSL -A "$ua" --retry 3 --retry-delay 5 --max-time 900 \
                    -o "specs/$1/$2.part" "$url" \
                    && [ -s "specs/$1/$2.part" ] \
                    && { [ "${1}" = "format" ] || [ "$(file -b --mime-type "specs/$1/$2.part")" != "text/html" ]; }; then
                mv "specs/$1/$2.part" "specs/$1/$2"; echo "fetched  $1/$2"; return 0
            fi
            rm -f "specs/$1/$2.part"
        done
        echo "MISSING  $1/$2  <- $3" >&2
        missing="$missing  $1/$2  <- $3"$'\n'
        return 0
    }
    # law/ — the statutes, consolidated (gesetze-im-internet.de)
    fetch law msbg.pdf 'https://www.gesetze-im-internet.de/messbg/MsbG.pdf'
    fetch law enwg.pdf 'https://www.gesetze-im-internet.de/enwg_2005/EnWG.pdf'
    fetch law ao.pdf 'https://www.gesetze-im-internet.de/ao_1977/AO.pdf'
    fetch law bdsg.pdf 'https://www.gesetze-im-internet.de/bdsg_2018/BDSG.pdf'
    fetch law messeg.pdf 'https://www.gesetze-im-internet.de/messeg/MessEG.pdf'
    fetch law messev.pdf 'https://www.gesetze-im-internet.de/messev/MessEV.pdf'
    # eu/ — the Regulations, in the language the German market reads them in
    fetch eu dsgvo-vo-eu-2016-679.pdf \
        'https://eur-lex.europa.eu/legal-content/DE/TXT/PDF/?uri=CELEX:32016R0679'
    fetch eu vo-eu-312-2014-gasnetzkodex-bilanzierung-de.pdf \
        'https://eur-lex.europa.eu/legal-content/DE/TXT/PDF/?uri=CELEX:32014R0312'
    # bnetza/ — the Festlegungen behind reproducibility and the Zählerstandsgang
    fetch bnetza bk6-24-174-beschluss-20241024.pdf \
        'https://www.bundesnetzagentur.de/DE/Beschlusskammern/1_GZ/BK6-GZ/2024/BK6-24-174/Beschluss/BK6-24-174_Beschluss_vom_20241024.pdf?__blob=publicationFile&v=1'
    fetch bnetza bk6-24-174-mabis-lesefassung.pdf \
        'https://www.bundesnetzagentur.de/DE/Beschlusskammern/1_GZ/BK6-GZ/2024/BK6-24-174/Beschluss/BK6-24-174_MaBiS_Lesefassung.pdf?__blob=publicationFile&v=1'
    fetch bnetza bk6-24-174-gpke-teil1-lesefassung.pdf \
        'https://www.bundesnetzagentur.de/DE/Beschlusskammern/1_GZ/BK6-GZ/2024/BK6-24-174/Beschluss/BK6-24-174_GPKE_Teil1_Lesefassung.pdf?__blob=publicationFile&v=1'
    # edi-energy/ — the BDEW catalogue, one file per fileId
    fetch edi-energy mscons-ahb-3.1g.pdf 'https://www.bdew-mako.de/api/downloadFile/11929'
    fetch edi-energy mscons-ahb-3.2.pdf 'https://www.bdew-mako.de/api/downloadFile/12172'
    fetch edi-energy mscons-mig-2.4c.pdf 'https://www.bdew-mako.de/api/downloadFile/9645'
    fetch edi-energy mscons-mig-2.5.pdf 'https://www.bdew-mako.de/api/downloadFile/12175'
    fetch edi-energy allgemeine-festlegungen-6.1c.pdf 'https://www.bdew-mako.de/api/downloadFile/11916'
    fetch edi-energy allgemeine-festlegungen-6.1d.pdf 'https://www.bdew-mako.de/api/downloadFile/12145'
    fetch edi-energy codeliste-obis-kennzahlen-und-medien-2.5c.pdf 'https://www.bdew-mako.de/api/downloadFile/11918'
    fetch edi-energy codeliste-zeitreihentypen-1.1d.pdf 'https://www.bdew-mako.de/api/downloadFile/8852'
    # bdew/ — the Anwendungshilfen the identifier types are written against
    fetch bdew bdew-awh-identifikatoren-mako-v1.2.pdf \
        'https://www.bdew.de/media/documents/AWH_Identifikatoren-in-der-Marktkommunikation_Version.1.2.pdf'
    fetch bdew bdew-awh-malo-id-v1.0-20170428.pdf \
        'https://bdew-codes.de/Content/Files/MaLo/2017-04-28-BDEW-Anwendungshilfe-MaLo-ID_Version1.0_FINAL.PDF'
    fetch bdew bdew-awh-eic-vergabe-v1.0-20171218.pdf \
        'https://bdew-codes.de/Content/Files/EIC/Awh_20171218_EIC-Vergabe_V1-0.pdf'
    # entsoe/ — the coding scheme an EIC column is checked against
    fetch entsoe eic-reference-manual-5.5.pdf \
        'https://eepublicdownloads.entsoe.eu/clean-documents/EDI/Library/EIC_Reference_Manual_Release_5_5.pdf' \
        'https://www.entsoe.eu/Documents/EDI/Library/EIC_Reference_Manual_Release_5_5.pdf'
    # format/ — the storage formats, from the projects that define them
    fetch format iceberg-table-spec.md \
        'https://raw.githubusercontent.com/apache/iceberg/main/format/spec.md'
    fetch format iceberg-rest-catalog-open-api.yaml \
        'https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml'
    fetch format parquet-format.md \
        'https://raw.githubusercontent.com/apache/parquet-format/master/README.md'
    fetch format parquet.thrift \
        'https://raw.githubusercontent.com/apache/parquet-format/master/src/main/thrift/parquet.thrift'
    fetch format arrow-flight-sql.proto \
        'https://raw.githubusercontent.com/apache/arrow/main/format/FlightSql.proto'
    fetch format arrow-flight.proto \
        'https://raw.githubusercontent.com/apache/arrow/main/format/Flight.proto'
    fetch format rfc3339.txt 'https://www.rfc-editor.org/rfc/rfc3339.txt'
    fetch format rfc2104-hmac.txt 'https://www.rfc-editor.org/rfc/rfc2104.txt'
    # postgres/ — the manual, for the two chapters the hot tier lives inside
    fetch postgres postgresql-17-A4.pdf \
        'https://www.postgresql.org/files/documentation/pdf/17/postgresql-17-A4.pdf'
    # crypto/ — what the suppression list is built from
    fetch crypto nist-fips-198-1-hmac.pdf 'https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.198-1.pdf'
    fetch crypto nist-fips-180-4-sha2.pdf 'https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf'
    # privacy/ — the guidance the pseudonymisation argument is measured against
    fetch privacy edpb-guidelines-01-2025-pseudonymisation.pdf \
        'https://www.edpb.europa.eu/system/files/2025-01/edpb_guidelines_202501_pseudonymisation_en.pdf'
    fetch privacy bsi-tr-03109-1.pdf \
        'https://www.bsi.bund.de/SharedDocs/Downloads/DE/BSI/Publikationen/TechnischeRichtlinien/TR03109/TR03109-1.pdf?__blob=publicationFile&v=4'
    if [ -n "$missing" ]; then
        echo >&2
        echo "⚠️  not fetched (see specs/README.md for the source):" >&2
        printf '%s' "$missing" >&2
    fi
    echo "📚 specs/ rebuilt"
