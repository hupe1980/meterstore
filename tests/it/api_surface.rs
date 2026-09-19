//! The size of the surface a caller holds, pinned rather than reviewed.
//!
//! Not a threshold — a snapshot. A cap would be a number somebody guessed, and
//! it would fire on the first honest addition while saying nothing about the
//! twentieth. These are the counts as they stand, so the check fails on
//! *change*: adding a method to one of these types costs one line here, which is
//! the moment to ask whether it belongs on this type at all.
//!
//! It exists because the answer was no, repeatedly. `MeterStore` reached
//! fifty-seven methods across six unrelated jobs before anybody counted, and
//! nothing in the build had an opinion. The operational sixteen are behind
//! `admin()` and the subject API is on the catalog; this is what keeps them
//! there.
//!
//! Needs no database and no feature flag.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Public **inherent** methods per type across `src/`.
///
/// Inherent only: a trait implementation's methods are the trait's surface, not
/// the type's, and a type is not made harder to hold by implementing `Debug`.
fn public_methods() -> BTreeMap<String, usize> {
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

    let mut files = Vec::new();
    walk(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );

    let mut counts = BTreeMap::new();
    for file in files {
        let source = std::fs::read_to_string(&file).expect("read source");
        let mut current: Option<String> = None;
        for line in source.lines() {
            if let Some(rest) = line.strip_prefix("impl") {
                // `impl Trait for Type` is the trait's surface; skip it.
                current = match rest.contains(" for ") {
                    true => None,
                    false => rest
                        .trim_start_matches(|c: char| c != ' ')
                        .split_whitespace()
                        .next()
                        .map(|name| name.split('<').next().unwrap_or(name).to_string()),
                };
            } else if let Some(name) = &current
                && (line.starts_with("    pub fn ")
                    || line.starts_with("    pub async fn ")
                    || line.starts_with("    pub const fn "))
            {
                *counts.entry(name.clone()).or_insert(0) += 1;
            }
        }
    }
    counts
}

#[test]
fn the_handles_a_caller_holds_stay_the_size_they_are() {
    // One job each, and the number is the evidence. `MeterStore` reads and
    // writes readings; `StoreAdmin` runs the deployment's operations against a
    // table; `MeterCatalog` is the deployment, which is why the subject API —
    // one map for the whole of it — lives there and has no per-table form.
    let expected = [("MeterStore", 32), ("StoreAdmin", 17), ("MeterCatalog", 32)];

    let counts = public_methods();
    let mut wrong = Vec::new();
    for (name, want) in expected {
        let got = counts.get(name).copied().unwrap_or(0);
        if got != want {
            wrong.push(format!("  {name}: {got} public methods, expected {want}"));
        }
    }

    assert!(
        wrong.is_empty(),
        "a caller-facing handle changed size:\n{}\n\
         If the method belongs here, update the number. If it is a deployment \
         operation, it belongs on `StoreAdmin`; if it is about subjects, the map \
         is deployment-wide and it belongs on `MeterCatalog`.",
        wrong.join("\n")
    );
}

#[test]
fn the_prelude_carries_only_what_a_caller_writes() {
    // A prelude is read on every file that imports it, so its cost is paid
    // everywhere and its benefit only where a name is actually typed. A type
    // that just comes back from a method needs no import at all.
    let source =
        std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"))
            .expect("read lib.rs");

    let body = source
        .split_once("pub mod prelude {")
        .expect("a prelude")
        .1
        .split_once("\n}")
        .expect("a closing brace")
        .0;

    let names = body
        .split(';')
        .filter(|item| item.contains("pub use"))
        .map(|item| match item.rsplit_once('{') {
            // A trailing comma inside the braces is rustfmt's, not a name.
            Some((_, group)) => group
                .trim_end_matches('}')
                .split(',')
                .filter(|name| !name.trim().is_empty())
                .count(),
            None => 1,
        })
        .sum::<usize>();

    assert_eq!(
        names, 33,
        "the prelude exports {names} names. If the new one is something a caller \
         has to *write* — a builder argument, a struct literal, a trait bound — \
         update the number. If it only ever comes back from a method, leave it at \
         its own path: the name is inferred and the import buys nothing."
    );
}
