//! Claims this repository makes about itself, checked against whatever would
//! make them true.
//!
//! Not what the code does — the rest of the suite is for that. These are the
//! sentences in the documentation whose truth lives somewhere no compiler looks:
//! a workflow file, a constant, an agreement between two of them. They are the
//! claims that rot silently, because nothing fails when they stop being true.
//!
//! Needs no database and no feature flag.

use std::path::PathBuf;

/// Read a file this repository **tracks**.
///
/// Only tracked files: a working copy carries design notes and scratch that a
/// fresh checkout does not, so a check reading one of those passes on the
/// machine that wrote it and fails in CI.
fn repo(relative: &str) -> String {
    std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative))
        .unwrap_or_else(|e| {
            panic!(
                "read {relative}: {e}\n\
                 If this file is gitignored it cannot be checked here — a fresh \
                 checkout does not have it."
            )
        })
}

#[test]
fn ci_runs_the_integration_suite_on_the_documented_floor() {
    // The documentation says the floor is *checked rather than promised*. That
    // sentence is true only while a job exists to check it, and a job is a YAML
    // file nobody compiles — so deleting it, renaming the variable or pointing it
    // at the default would leave the claim standing with nothing behind it.
    let ci = repo(".github/workflows/ci.yml");

    assert!(
        ci.contains("METERSTORE_POSTGRES_IMAGE_TAG"),
        "no job overrides the server version, so nothing runs against the floor"
    );

    // And it reads the version out of the code rather than repeating it. Two
    // spellings of one fact can disagree, and this pair would disagree in
    // silence: the job would keep passing against whatever version it named while
    // the documentation claimed another.
    assert!(
        ci.contains("pub const FLOOR: u32")
            && ci.contains("src/testkit/postgres.rs")
            && ci.contains(r#"METERSTORE_POSTGRES_IMAGE_TAG="$floor-alpine""#),
        "the floor job has to derive the version from `testkit::postgres::FLOOR`, \
         or the workflow and the documentation can drift apart"
    );
}

#[test]
fn no_document_names_a_floor_the_code_does_not_declare() {
    // Declared once, said in prose in several places. What rots is not the
    // wording — that can change freely — but the *number*: a floor moves in the
    // code and a page somewhere keeps promising the version before it, which is
    // a promise nobody is keeping and nothing else would notice.
    //
    // So this reads the number out of every "N or later" and "PostgreSQL N+" in
    // the documentation and requires each to be the declared one.
    let source = repo("src/testkit/postgres.rs");
    let floor: u32 = source
        .split_once("pub const FLOOR: u32 = ")
        .expect("a declared floor")
        .1
        .split(';')
        .next()
        .expect("a value")
        .trim()
        .parse()
        .expect("a major version");

    // Tracked files only. A fresh checkout has no design notes — they are
    // gitignored — so naming one here is a test that passes on the machine that
    // wrote it and fails in CI.
    let mut wrong = Vec::new();
    for file in ["README.md", "site/content/docs/getting-started.md"] {
        let text = repo(file);
        let mut named = Vec::new();

        // `**N or later**`, the form every requirements table uses, and any
        // other phrasing that ends in the same three words.
        for (index, _) in text.match_indices(" or later") {
            let head = &text[..index];
            let digits = head.len() - head.trim_end_matches(|c: char| c.is_ascii_digit()).len();
            // `0.24 or later` is `metering`'s minor, not a PostgreSQL major. A
            // dot in front means the digits are part of a longer version.
            if digits == 0 || head[..head.len() - digits].ends_with('.') {
                continue;
            }
            if let Ok(version) = head[head.len() - digits..].parse::<u32>() {
                named.push(version);
            }
        }
        for (index, _) in text.match_indices("PostgreSQL ") {
            let rest = &text[index + "PostgreSQL ".len()..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            if rest[digits.len()..].starts_with('+')
                && let Ok(version) = digits.parse::<u32>()
            {
                named.push(version);
            }
        }

        if named.is_empty() {
            wrong.push(format!("  {file}: names no floor at all"));
        }
        for version in named {
            if version != floor {
                wrong.push(format!(
                    "  {file}: promises PostgreSQL {version} or later, code declares {floor}"
                ));
            }
        }
    }

    assert!(
        wrong.is_empty(),
        "a document promises a floor the code does not:\n{}",
        wrong.join("\n")
    );
}

#[test]
fn ci_and_the_local_gate_check_the_same_feature_sets() {
    // `--all-features` hides a broken feature combination entirely, so the point
    // of both lists is the narrow sets. Spelled twice — once for the matrix CI
    // runs, once for `just features` — and a set present in only one is the
    // worst arrangement of the two: it either fails on a push nobody could have
    // caught locally, or passes locally on a gate weaker than the real one.
    fn sets(text: &str, line_start: &str, flags_from: impl Fn(&str) -> String) -> Vec<String> {
        text.lines()
            .map(str::trim)
            .filter(|line| line.starts_with(line_start))
            .map(|line| {
                flags_from(line)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect()
    }

    // `check "sql-catalog"    --no-default-features --features sql-catalog`
    let local = sets(&repo("justfile"), "check \"", |line| {
        line.split_once("\" ")
            .map_or_else(String::new, |(_, rest)| rest.to_string())
    });

    // `- { name: sql-catalog, flags: --no-default-features --features sql-catalog }`
    let remote = sets(&repo(".github/workflows/ci.yml"), "- { name:", |line| {
        line.split_once("flags:")
            .map_or_else(String::new, |(_, rest)| {
                rest.trim_end_matches('}').to_string()
            })
    });

    assert!(
        !local.is_empty() && !remote.is_empty(),
        "neither list may be empty"
    );

    let mut local_sorted = local.clone();
    let mut remote_sorted = remote.clone();
    local_sorted.sort();
    remote_sorted.sort();
    assert_eq!(
        local_sorted, remote_sorted,
        "`just features` and the CI matrix check different feature sets.\n\
         local:  {local:#?}\nremote: {remote:#?}"
    );
}
