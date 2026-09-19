//! Two documentation conventions, enforced rather than reviewed.
//!
//! Both are about the same thing: what a reader of a *reference* doc is entitled
//! to. They want to know what the thing does and why it is the way it is —
//! neither a chapter nor a diff.
//!
//! Needs no database and no feature flag, so this module carries no `#![cfg]`
//! gate.

use std::path::{Path, PathBuf};

/// Every documentation-bearing file: `src/`, `tests/`, the site pages, and
/// `README.md`.
///
/// Two root Markdown files are deliberately absent, for different reasons.
/// `CHANGELOG.md` is the one place history belongs, and the whole point of the
/// second check is that it belongs *only* there. `AGENTS.md` teaches the
/// convention by counter-example — it shows the past-tense phrasing beside the
/// present-tense one — so the check it exists to explain would fail it.
fn documented_files() -> Vec<(PathBuf, String)> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            .map(|e| e.expect("dir entry").path())
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs" || e == "md") {
                out.push(path);
            }
        }
    }

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut paths = Vec::new();
    for dir in ["src", "tests", "site/content"] {
        walk(&root.join(dir), &mut paths);
    }
    paths.push(root.join("README.md"));

    assert!(paths.len() > 40, "the file walk found almost nothing");
    paths
        .into_iter()
        .map(|p| {
            let text = std::fs::read_to_string(&p).expect("read");
            (p, text)
        })
        .collect()
}

/// The longest a single `///` block may run.
///
/// Module docs (`//!`) are exempt: a module's own documentation is where the
/// long-form argument belongs, and it is not standing between a reader and the
/// item after it.
const MAX_ITEM_DOC_LINES: usize = 60;

#[test]
fn no_item_doc_grows_into_a_chapter() {
    let mut offenders = Vec::new();

    for (path, text) in documented_files() {
        if path.extension().is_some_and(|e| e != "rs") {
            continue;
        }
        let mut run = 0usize;
        let mut start = 0usize;
        for (i, line) in text.lines().enumerate() {
            if line.trim_start().starts_with("///") {
                if run == 0 {
                    start = i + 1;
                }
                run += 1;
            } else {
                if run > MAX_ITEM_DOC_LINES {
                    offenders.push(format!("{}:{start} — {run} lines", path.display()));
                }
                run = 0;
            }
        }
        if run > MAX_ITEM_DOC_LINES {
            offenders.push(format!("{}:{start} — {run} lines", path.display()));
        }
    }

    assert!(
        offenders.is_empty(),
        "a doc comment runs past {MAX_ITEM_DOC_LINES} lines:\n  {}\n\
         A block that long buries the item after it. Move the argument to the \
         module documentation or to the guide, and leave the item saying what it \
         does and why.",
        offenders.join("\n  ")
    );
}

#[test]
fn reference_docs_are_not_a_changelog() {
    // Documentation says what is true. A reader arrives with a question about
    // the code in front of them, not about the code that used to be there — and
    // a doc that narrates its own history is stale the moment the next change
    // lands, in a way nothing detects.
    //
    // The phrases are the ones that only ever introduce history. "no longer" is
    // deliberately absent: "a column the configuration no longer declares" is a
    // *state* the schema comparison reports, not a note about a past release.
    // A floor rather than a proof. These are the spellings that *only* introduce
    // history; the bare past tense — "it was ~1.6 s, on the reasoning that …" —
    // is not on the list because "it was written by", "it was in force" and
    // "nothing was changed" are ordinary present-tense description, and a rule
    // that fired on all of them would be ignored rather than obeyed.
    const HISTORY: [&str; 15] = [
        "an earlier version",
        "a first draft",
        "previously",
        "in an earlier",
        "this release",
        "was renamed",
        "has been replaced",
        "we changed",
        "before this change",
        "the old behaviour",
        "the previous behaviour",
        "originally",
        "formerly",
        "at one point",
        "until recently",
    ];

    // "used to" needs a grammar rule rather than a substring, because English
    // spells two unrelated things the same way: *"used to cast incoming
    // batches"* is a purpose, and *"each used to reassemble the builder"* is a
    // changelog. The purposive sense is passive or sentence-initial — it follows
    // a form of "to be", a comma, or a full stop. Anything else is a subject
    // doing something it no longer does.
    const PURPOSIVE_LEAD_IN: [&str; 7] = ["is", "are", "be", "been", "being", "was", "were"];
    fn narrates_the_past(line: &str) -> bool {
        let lower = line.to_lowercase();
        let mut from = 0;
        while let Some(at) = lower[from..].find("used to ") {
            let at = from + at;
            let before = lower[..at].trim_end();
            // Sentence-initial, or the start of the comment's own text.
            let initial = before.is_empty()
                || before.ends_with('.')
                || before.ends_with(',')
                || before.ends_with("//")
                || before.ends_with("///")
                || before.ends_with("//!");
            let lead_in = before.rsplit([' ', '\t']).next().unwrap_or("");
            if !initial && !PURPOSIVE_LEAD_IN.contains(&lead_in) {
                return true;
            }
            from = at + "used to ".len();
        }
        false
    }

    /// The comment part of a Rust line, leading or trailing.
    ///
    /// `://` is skipped so a URL in a string literal is not read as one.
    fn comment_of(line: &str) -> Option<&str> {
        let bytes = line.as_bytes();
        (0..line.len().saturating_sub(1)).find_map(|i| {
            (bytes[i] == b'/' && bytes[i + 1] == b'/' && (i == 0 || bytes[i - 1] != b':'))
                .then(|| &line[i..])
        })
    }

    let mut offenders = Vec::new();
    for (path, text) in documented_files() {
        // This file names the phrases it forbids.
        if path.ends_with("doc_conventions.rs") {
            continue;
        }
        let is_rust = path.extension().is_some_and(|e| e == "rs");
        for (i, line) in text.lines().enumerate() {
            // In Rust, the comment part of the line — trailing comments carry
            // documentation too, and a check that only saw leading ones would
            // pass a file whose asides narrate its history. In Markdown, the
            // whole line.
            let line = match is_rust {
                false => line,
                true => match comment_of(line) {
                    Some(comment) => comment,
                    None => continue,
                },
            };
            let lower = line.to_lowercase();
            let found = HISTORY
                .iter()
                .find(|p| lower.contains(**p))
                .copied()
                .or_else(|| narrates_the_past(line).then_some("used to"));
            if let Some(found) = found {
                offenders.push(format!(
                    "{}:{}: {found:?} — {}",
                    path.display(),
                    i + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "a reference doc reads like a changelog:\n  {}\n\
         State what is true now. History belongs in CHANGELOG.md, which is the \
         one file this check does not read.",
        offenders.join("\n  ")
    );
}

#[test]
fn the_published_docs_are_not_a_backlog() {
    // The sibling of the check above, and the same argument pointed forward. A
    // reader of `site/content/docs` arrives with a question about the code in
    // front of them — not about the code somebody means to write. A page that
    // carries a plan is stale the moment the plan changes, and nobody editing
    // the plan goes looking for the page.
    //
    // **Narrower than the changelog check on purpose.** It reads the published
    // pages only. The README's status section names its gaps deliberately, which
    // is what a pre-1.0 README is for, and the design notes are a backlog by
    // definition.
    // Spellings that *only* introduce a plan, on the same principle as the list
    // above. A bare "not yet" is absent because "a deployment that cannot yet
    // hold a key securely" is a statement about the reader, and a bare "planned"
    // because a query is planned before it is executed — which is what half of
    // `querying.md` is about.
    const PLANS: [&str; 12] = [
        "not yet implemented",
        "not yet supported",
        "not yet exercised",
        "on the roadmap",
        "in a future release",
        "will be added",
        "is planned for",
        "are planned for",
        "we intend to",
        "todo:",
        "fixme",
        "coming soon",
    ];

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("site/content");
    let mut offenders = Vec::new();
    for (path, text) in documented_files() {
        if !path.starts_with(&root) {
            continue;
        }
        for (i, line) in text.lines().enumerate() {
            let lower = line.to_lowercase();
            if let Some(found) = PLANS.iter().find(|p| lower.contains(**p)) {
                offenders.push(format!(
                    "{}:{}: {found:?} — {}",
                    path.display(),
                    i + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "a published page carries a plan:\n  {}\n\
         Say what the code does. What it does not do yet belongs in the backlog, \
         which is not published — and a gap worth warning a reader about is a \
         limitation, stated as one.",
        offenders.join("\n  ")
    );
}
