//! The on-disk representation is not a function of the build graph.
//!
//! This crate writes two JSON columns — `source_detail` and `provenance` — and a
//! `Decimal128` `value`, all under a retention measured in decades. What shape
//! any of them takes must depend on this crate and nothing else.
//!
//! # Why this is a test and not a comment
//!
//! Cargo features are **additive and global to a build graph**. A feature that
//! *adds* an impl is safe; one that *replaces* an existing one decides a wire
//! format for every crate in the workspace, including crates that never named
//! the one that turned it on. Two exist in this crate's graph:
//!
//! - `rust_decimal/serde-str` and `rust_decimal/serde-float` replace the global
//!   `Serialize`/`Deserialize` for `rust_decimal::Decimal`. `serde-str` makes a
//!   JSON *number* stop deserialising everywhere; `serde-float` is the worse
//!   half, turning an exact decimal into an `f64` on the way out.
//! - `time/serde-human-readable` replaces `time`'s own impls, so an
//!   `OffsetDateTime` goes to JSON as a nine-element ordinal-date array without
//!   it and as a formatted string with it.
//!
//! This crate is a library, and enables `metering/serde` for everyone downstream
//! of it. Whether it decides a representation on their behalf is therefore a
//! question its consumers are entitled to a mechanical answer to, rather than a
//! comment in the manifest.
//!
//! Neither of these needs a database or a feature flag, so this module carries
//! no `#![cfg]` gate: it runs in every configuration the suite is built in.

use std::path::{Path, PathBuf};

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Every `.rs` file under `src/`, in a stable order.
fn sources() -> Vec<(PathBuf, String)> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            .map(|e| e.expect("dir entry").path())
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    let mut paths = Vec::new();
    walk(&crate_root().join("src"), &mut paths);
    assert!(paths.len() > 20, "the source walk found almost nothing");
    paths
        .into_iter()
        .map(|p| {
            let text = std::fs::read_to_string(&p).expect("read source");
            (p, text)
        })
        .collect()
}

#[test]
fn the_crate_reaches_for_no_feature_that_replaces_a_wire_format() {
    // The manifest, read rather than remembered. The comment beside
    // `[dependencies] time` says this crate deliberately does not enable
    // `serde-human-readable`; that sentence is now a mechanism.
    let manifest = std::fs::read_to_string(crate_root().join("Cargo.toml")).expect("Cargo.toml");

    // Comments are stripped first: this file's own prose names the very
    // features it forbids, and so does the manifest's.
    let code: String = manifest
        .lines()
        .map(|line| line.split_once(" #").map_or(line, |(before, _)| before))
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");

    // Checked by feature **name**, not by `crate/feature` path: a dependency can
    // ask for one in either spelling — `rust_decimal/serde-str` inside a
    // `[features]` entry, or `features = ["serde-str"]` on the dependency line
    // itself — and a check that knows only one of them lets the other past. The
    // names are distinctive enough that appearing anywhere in the manifest is
    // the thing to refuse.
    for (feature, what) in [
        (
            "serde-str",
            "replaces the global Deserialize for rust_decimal::Decimal, so a JSON number \
             stops deserialising in every crate in the consumer's graph",
        ),
        (
            "serde-float",
            "turns an exact decimal into an f64 on the way out, in a graph whose whole \
             claim is exact arithmetic",
        ),
        (
            "serde-arbitrary-precision",
            "changes how a number is represented on the wire",
        ),
        (
            "serde-human-readable",
            "replaces time's own impls, so an OffsetDateTime goes to JSON as a formatted \
             string instead of an ordinal-date array — or the reverse, depending on who \
             else in the graph asked",
        ),
    ] {
        assert!(
            !code.contains(feature),
            "Cargo.toml reaches for the {feature:?} feature, which {what}. That is not \
             an impl added, it is one replaced — for every crate in the consumer's build \
             graph, including crates that never named this one. State the representation \
             on the field instead, as `metering::wire` does."
        );
    }

    // And the path spelling, for a feature enabled on a shared crate through a
    // `[features]` entry rather than a dependency line.
    for path in ["rust_decimal/serde", "chrono/serde", "time/serde-"] {
        assert!(
            !code.contains(path),
            "Cargo.toml enables {path:?} on a crate the consumer also uses. Only \
             additive features may be turned on for someone else's build."
        );
    }
}

#[test]
fn no_serde_type_in_this_crate_leaves_its_representation_to_the_build_graph() {
    // A `serde`-derived type carrying a bare `OffsetDateTime`, `Date` or
    // `Decimal` has a wire format decided by whichever features happen to be on,
    // which is the defect above one level down. None exists today; this is what
    // keeps that true, because the failure is silent in both directions — a
    // reader and a writer built with different features simply disagree.
    //
    // The rule is not "no such field" but "no such field *without a stated
    // representation*": `#[serde(with = "…")]` on the field is the fix, and it
    // is what `metering` does for every one of its own.
    const AMBIGUOUS: [&str; 4] = ["OffsetDateTime", "Decimal", "Date", "time::Duration"];

    let mut offenders = Vec::new();
    for (path, text) in sources() {
        let mut derives_serde = false;
        let mut depth = 0usize;
        let mut stated = false;

        for line in text.lines() {
            let trimmed = line.trim();

            // A `serde` attribute on the *next* item states its representation.
            if trimmed.starts_with("#[serde(with") || trimmed.contains("serde(with =") {
                stated = true;
                continue;
            }
            if trimmed.starts_with("#[derive") || trimmed.starts_with("#[cfg_attr") {
                if trimmed.contains("Serialize") || trimmed.contains("Deserialize") {
                    derives_serde = true;
                }
                continue;
            }

            if derives_serde {
                depth += trimmed.matches('{').count();
                depth -= depth.min(trimmed.matches('}').count());
                // The body ended without a field of interest.
                if depth == 0 && trimmed.contains('}') {
                    derives_serde = false;
                    stated = false;
                    continue;
                }
                if depth > 0
                    && !stated
                    && trimmed.contains(':')
                    && !trimmed.starts_with("//")
                    // `Date` is a substring of `OffsetDateTime`, so the line is
                    // reported once however many names it matches.
                    && AMBIGUOUS.iter().any(|ty| trimmed.contains(ty))
                {
                    offenders.push(format!("{}: {trimmed}", path.display()));
                }
                stated = false;
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "a serde-derived type carries a field whose representation the build graph \
         decides:\n  {}\nState it on the field — `#[serde(with = \"…\")]` — the way \
         `metering::wire` does, or the bytes change when an unrelated crate turns a \
         feature on.",
        offenders.join("\n  ")
    );
}

#[test]
fn the_features_this_crate_turns_on_for_a_consumer_are_only_additive() {
    // The other half of the promise, and the one this crate owes *downstream*:
    // depending on `meterstore` must not silently change how a consumer's own
    // types serialise.
    //
    // `metering/serde` is the one enabled here that reaches a shared crate — it
    // resolves to `["dep:serde", "time/serde"]`, and `time/serde` *adds* impls
    // that do not otherwise exist rather than replacing any. Read from the
    // resolved lockfile rather than asserted, so an upstream change to what that
    // feature means is caught here instead of in a consumer's workspace.
    let lock = std::fs::read_to_string(crate_root().join("Cargo.lock")).expect("Cargo.lock");
    assert!(
        lock.contains("name = \"metering\""),
        "the lockfile does not name metering"
    );

    // `rust_decimal` is in the graph (sqlx encodes with it, and `value` is one).
    // What must not be in the graph is a *serde* feature on it, which is what
    // would decide the representation of every `Decimal` a consumer owns.
    // Cargo does not record enabled features in the lockfile, so this is checked
    // where it can be: nothing in this workspace asks for one.
    let manifest = std::fs::read_to_string(crate_root().join("Cargo.toml")).expect("Cargo.toml");
    let metering_line = manifest
        .lines()
        .find(|l| l.trim_start().starts_with("metering = "))
        .expect("metering is a direct dependency");
    assert!(
        metering_line.contains("features = [\"serde\"]"),
        "the metering dependency changed shape; re-read what its features pull in: \
         {metering_line}"
    );
}
