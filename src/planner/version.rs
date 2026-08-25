//! Resolving corrections to the value that currently holds.
//!
//! A correction is a new row at a higher `version`, not an overwrite, so a
//! naive scan returns every version of a corrected interval and summing it
//! double-counts. Resolution keeps the highest version per merge key.
//!
//! The important optimisation is **not doing it**. Corrections are rare and
//! concentrated in recent months, so most historical partitions contain exactly
//! one version per key. Iceberg records per-file `min`/`max` statistics, so when
//! those are equal for `version` the file provably has no corrections and can be
//! scanned directly — no window function, no sort, no repartition.
//!
//! # Why the file's interval range is read as well as its versions
//!
//! One file at one version proves nothing on its own; what has to be proved is
//! that no *key* appears twice across the files a scan reads. `from` is **in the
//! merge key**, so two files whose `from` bounds do not overlap cannot hold the
//! same key whatever versions they carry.
//!
//! That is what makes the optimisation fire at all. MSCONS versions ascend per
//! delivery and archival commits one day per window, so a year of history is 365
//! files at 365 different versions — disjoint by construction. Requiring them all
//! to carry the *same* version is sound and true of almost nothing.
//!
//! Files whose bounds are unknown are treated as overlapping everything, which
//! collapses to that stricter rule. Iceberg bounds may only ever widen, never
//! narrow, so an overlap this sees that is not real costs a window function
//! rather than a wrong row.
//!
//! # What it deliberately does not use
//!
//! A **multi-tenant** table writes one file per identity value per window, all
//! covering the same day, so their bounds overlap and their versions usually
//! differ — such a scan still resolves. The partition tuple would settle it,
//! since every partition field this crate writes is derived from a merge-key
//! column.
//!
//! It is not used, because that rests on the spec being the one this crate wrote.
//! A table repartitioned out of band — the compaction §10.3.1 tells an operator
//! to run with Spark — could carry a field on an attribute column, and the
//! inference would then be silently wrong in the direction that returns a
//! superseded row.

use time::OffsetDateTime;

use crate::encode::schema::col;

/// What a scan must do to return correctly resolved rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    /// No corrections present: scan directly.
    Elided,
    /// Corrections present: resolve to the highest version per key.
    Required,
}

impl Resolution {
    /// Whether resolution can be skipped.
    pub const fn is_elided(self) -> bool {
        matches!(self, Self::Elided)
    }
}

/// Per-file version statistics, as recorded by Iceberg.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionStats {
    /// Lowest `version` in the file.
    pub min: i128,
    /// Highest `version` in the file.
    pub max: i128,
}

impl VersionStats {
    /// Statistics for a file holding a single version.
    pub const fn single(version: i128) -> Self {
        Self {
            min: version,
            max: version,
        }
    }

    /// Whether this file can contain a correction.
    pub const fn may_contain_corrections(self) -> bool {
        self.min != self.max
    }
}

/// What one data file's statistics say, as far as elision is concerned.
///
/// Two facts, and neither is sufficient alone: the versions the file holds, and
/// the span of `from` it covers. `None` means the file records no bound for that
/// column, which is read as "anything" — see [`plan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStats {
    /// The file's `version` bounds.
    pub version: Option<VersionStats>,
    /// The file's inclusive `from` bounds.
    ///
    /// Inclusive because that is what Iceberg records: the lowest and highest
    /// value actually present. Two files overlap when each one's lower bound is
    /// at or below the other's upper bound.
    pub interval: Option<(OffsetDateTime, OffsetDateTime)>,
}

impl FileStats {
    /// A file nothing is known about — the pessimistic default.
    pub const fn unknown() -> Self {
        Self {
            version: None,
            interval: None,
        }
    }

    /// A file holding one version over one interval span.
    pub const fn single(version: i128, from: (OffsetDateTime, OffsetDateTime)) -> Self {
        Self {
            version: Some(VersionStats::single(version)),
            interval: Some(from),
        }
    }
}

/// Decide whether a scan over `files` needs version resolution.
///
/// Resolution is elided only when no merge key can appear twice among the files
/// the scan reads. Two things would let it:
///
/// 1. **A file that spans versions**, which is a corrected key inside one file.
/// 2. **Two files at different versions that could share a key.** They could
///    share one exactly when their `from` bounds overlap, because `from` is in
///    the merge key — so files covering disjoint spans are free to disagree
///    about versions, and consecutive archival windows always do.
///
/// Missing statistics mean resolution is required. Statistics must *prove* the
/// absence of corrections; the absence of statistics proves nothing — so a file
/// with no version bounds forces resolution outright, and one with no interval
/// bounds is treated as overlapping every other file.
///
/// # What the proof rests on
///
/// One version among files that could share a key means no key appears twice
/// **only if no key is stored twice at that version**. Per-file statistics cannot
/// show that, and it is not assumed: the hot table's primary key is the merge key
/// plus `version`, and the cold tier's late-correction append reconciles against
/// what is stored, under a lease, before it writes.
///
/// # Grouping is conservative, not exact
///
/// Files are swept into *maximal runs* of overlap rather than compared pairwise,
/// so three files where the first and third are disjoint but both meet the second
/// land in one group. That can require resolution where a pairwise test would
/// not; it can never permit it where a pairwise test would not. §17.1's rule
/// again: the error may only ever be in the slow direction.
pub fn plan(files: &[FileStats]) -> Resolution {
    if files.is_empty() {
        return Resolution::Elided;
    }

    // Every file must be readable and hold exactly one version. Checked over all
    // of them before anything else: a file that spans versions holds a correction
    // on its own, whatever the others do, and stopping at the first file that
    // merely lacks a *span* would skip that check for the ones after it.
    let mut single: Vec<(i128, Option<(OffsetDateTime, OffsetDateTime)>)> =
        Vec::with_capacity(files.len());
    for file in files {
        let Some(stats) = file.version else {
            return Resolution::Required;
        };
        if stats.may_contain_corrections() {
            return Resolution::Required;
        }
        single.push((stats.min, file.interval));
    }

    // A file that will not say which intervals it covers could share a key with
    // any other, so every file has to agree on one version. That is the original
    // rule, and it is what a store supplying no interval bounds falls back to.
    if single.iter().any(|(_, span)| span.is_none()) {
        let first = single[0].0;
        return match single.iter().all(|(v, _)| *v == first) {
            true => Resolution::Elided,
            false => Resolution::Required,
        };
    }

    // Sweep into maximal runs of overlapping spans. A run break means every
    // remaining file starts strictly after everything seen so far ends, so no key
    // can cross it.
    let mut known: Vec<(OffsetDateTime, OffsetDateTime, i128)> = single
        .into_iter()
        .map(|(version, span)| {
            let (lo, hi) = span.expect("checked above");
            (lo, hi, version)
        })
        .collect();
    known.sort_by_key(|(lo, ..)| *lo);

    let mut run_end = known[0].1;
    let mut run_version = known[0].2;
    for &(lo, hi, version) in &known[1..] {
        if lo > run_end {
            run_end = hi;
            run_version = version;
            continue;
        }
        if version != run_version {
            return Resolution::Required;
        }
        run_end = run_end.max(hi);
    }

    Resolution::Elided
}

/// SQL that resolves a raw scan to the current value of each interval.
///
/// A correction is stored as a new row at a higher `version`, never as an
/// overwrite — that is the audit trail. A query must therefore keep only the
/// highest version per merge key, or every corrected interval is returned twice
/// and any `SUM` over it is overstated.
///
/// This is the single definition of that rule. [`MeterStore`] registers it as a
/// view over the raw tiered table, and the same text is published for engines
/// reading the Iceberg files directly, so the two cannot drift.
///
/// The partition includes `version_scope`: MSCONS assigns versions per network
/// operator per month, so versions from different scopes are not comparable and
/// must not be ranked against each other.
///
/// # The ordering is total, not just correct
///
/// `version DESC` alone leaves a tie unbroken, and `ROW_NUMBER` then picks
/// arbitrarily — a different row on a different plan, engine or file order. Both
/// write paths refuse two rows at one `(merge key, version)`, so a tie should be
/// unreachable; this text is nevertheless *published* for engines reading a
/// warehouse whose files something else may have written. `recorded_at DESC`
/// costs nothing when there is no tie and makes the answer reproducible when
/// there is.
///
/// [`MeterStore`]: crate::session::MeterStore
pub fn resolution_sql(table: &str) -> String {
    resolution_sql_with_key(table, &default_merge_key(), &[], None)
}

/// A `WHERE recorded_at <= <ceiling>` clause for the inner scan, or empty.
///
/// The bound is emitted as an `arrow_cast` of the microsecond epoch to the
/// column's exact type (`Timestamp(Microsecond, Some("UTC"))`), so it compares
/// like-for-like with no timezone coercion and no textual timestamp parsing. The
/// value is `meterstore`'s own (an `OffsetDateTime`), never caller string input,
/// so there is nothing to inject.
fn recorded_at_ceiling_clause(ceiling: Option<OffsetDateTime>) -> String {
    match ceiling {
        None => String::new(),
        Some(at) => {
            let micros = crate::encode::schema::micros(at);
            format!(
                "\n  WHERE \"{rec}\" <= arrow_cast({micros}, 'Timestamp(Microsecond, Some(\"UTC\"))')",
                rec = col::RECORDED_AT,
            )
        }
    }
}

/// The merge key when no identity columns are declared.
fn default_merge_key() -> Vec<String> {
    crate::encode::schema::MERGE_KEY
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

/// [`resolution_sql`] over an explicit merge key.
///
/// A deployment may extend a reading's identity — a tenant discriminator being
/// the obvious case. Resolution has to partition by the *same* key the storage
/// layer treats as identity, or two readings that are not the same reading would
/// compete, and one would supersede the other.
///
/// `recorded_at_ceiling` pins the **transaction-time** axis: when set, only rows
/// recorded at or before that instant enter the ranking, so resolution returns
/// the value that was in force *as known at that time* — a correction delivered
/// later, and an interval first stored later, are both invisible. `None` ranks
/// every version, which is the default current-knowledge read.
pub fn resolution_sql_with_key(
    table: &str,
    merge_key: &[String],
    extra: &[crate::arrow::datatypes::Field],
    recorded_at_ceiling: Option<OffsetDateTime>,
) -> String {
    // Columns are listed explicitly rather than `SELECT *` so the ranking column
    // does not leak into the result schema.
    let columns = crate::encode::schema::storage_schema(extra)
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    let partition = merge_key
        .iter()
        .map(|c| format!("\"{c}\""))
        .chain(std::iter::once(format!("\"{}\"", col::VERSION_SCOPE)))
        .collect::<Vec<_>>()
        .join(", ");

    // The derived table is aliased because this exact text is published for
    // external engines (§13.7.2) and PostgreSQL — the SQL catalog's own
    // database, and a likely place for an operator to paste it — rejects an
    // unaliased subquery outright. Trino, Spark, DuckDB and DataFusion all
    // accept the alias, so one spelling works everywhere.
    format!(
        r#"SELECT {columns} FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY {partition}
    ORDER BY "{version}" DESC, "{recorded}" DESC
  ) AS _meterstore_rank
  FROM {table}{ceiling}
) AS _meterstore_resolved WHERE _meterstore_rank = 1"#,
        version = col::VERSION,
        recorded = col::RECORDED_AT,
        ceiling = recorded_at_ceiling_clause(recorded_at_ceiling),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const V1: i128 = 20_260_701_000_001;
    const V2: i128 = 20_260_715_000_002;

    /// Day `n` of July 2026, as an archival window's inclusive `from` bounds.
    fn day(n: i64) -> (OffsetDateTime, OffsetDateTime) {
        let start = time::macros::datetime!(2026-07-01 00:00 UTC) + time::Duration::days(n);
        // The last interval of the day starts a quarter-hour before it ends,
        // which is what Iceberg records as the file's upper bound.
        (start, start + time::Duration::minutes(60 * 24 - 15))
    }

    #[test]
    fn an_empty_scan_needs_no_resolution() {
        assert_eq!(plan(&[]), Resolution::Elided);
    }

    #[test]
    fn single_version_files_elide_resolution() {
        let files = vec![FileStats::single(V1, day(0)); 4];
        assert!(plan(&files).is_elided());
    }

    #[test]
    fn a_file_spanning_versions_requires_resolution() {
        let files = vec![
            FileStats::single(V1, day(0)),
            FileStats {
                version: Some(VersionStats { min: V1, max: V2 }),
                interval: Some(day(1)),
            },
        ];
        assert_eq!(plan(&files), Resolution::Required);
    }

    #[test]
    fn files_at_differing_versions_over_disjoint_days_still_elide() {
        // The ordinary case: archival commits one day per window and MSCONS
        // versions ascend per delivery, so a year of history is 365 files at 365
        // versions. `from` is in the merge key, so files covering different days
        // cannot hold the same key however their versions differ.
        let files: Vec<FileStats> = (0..365)
            .map(|n| FileStats::single(V1 + i128::from(n), day(n)))
            .collect();
        assert!(plan(&files).is_elided(), "a year of daily windows");
    }

    #[test]
    fn files_at_differing_versions_that_overlap_require_resolution() {
        // A late correction appends a file covering a day already archived, at a
        // higher version. The two overlap on `from`, so a key really can appear
        // in both — this is what resolution is for.
        let files = vec![FileStats::single(V1, day(0)), FileStats::single(V2, day(0))];
        assert_eq!(plan(&files), Resolution::Required);

        // Partial overlap counts too: a correction spanning two days.
        let straddling = (day(0).0 + time::Duration::hours(12), day(1).1);
        assert_eq!(
            plan(&[
                FileStats::single(V1, day(0)),
                FileStats::single(V2, straddling),
            ]),
            Resolution::Required
        );
    }

    #[test]
    fn adjacent_days_do_not_count_as_overlapping() {
        // The boundary condition. Iceberg bounds are inclusive of the values
        // present, so one day's upper bound is strictly below the next day's
        // lower bound and the two are disjoint. Reading them as touching would
        // give up elision on every consecutive pair, which is every pair.
        assert!(plan(&[FileStats::single(V1, day(0)), FileStats::single(V2, day(1)),]).is_elided());

        // But a file whose span reaches exactly to the next file's first instant
        // does overlap it, and must not elide.
        let touching = (day(0).0, day(1).0);
        assert_eq!(
            plan(&[
                FileStats::single(V1, touching),
                FileStats::single(V2, day(1)),
            ]),
            Resolution::Required
        );
    }

    #[test]
    fn missing_version_statistics_require_resolution() {
        // Statistics must prove absence; their absence proves nothing.
        let files = vec![
            FileStats::single(V1, day(0)),
            FileStats {
                version: None,
                interval: Some(day(1)),
            },
        ];
        assert_eq!(plan(&files), Resolution::Required);
    }

    #[test]
    fn an_unknown_span_falls_back_to_requiring_one_version_everywhere() {
        // A file that will not say which intervals it covers could share a key
        // with any other, so the original rule applies: every file has to agree.
        let unknown = FileStats {
            version: Some(VersionStats::single(V1)),
            interval: None,
        };
        assert!(plan(&[unknown, FileStats::single(V1, day(9))]).is_elided());
        assert_eq!(
            plan(&[unknown, FileStats::single(V2, day(9))]),
            Resolution::Required
        );
        assert_eq!(plan(&[FileStats::unknown()]), Resolution::Required);
    }

    #[test]
    fn a_single_file_with_one_version_elides() {
        assert!(plan(&[FileStats::single(V1, day(0))]).is_elided());
    }

    #[test]
    fn version_stats_detect_a_mixed_file() {
        assert!(!VersionStats::single(V1).may_contain_corrections());
        assert!(VersionStats { min: V1, max: V2 }.may_contain_corrections());
    }

    #[test]
    fn resolution_sql_ranks_by_version_descending() {
        let sql = resolution_sql("readings_versions");
        assert!(sql.contains("ROW_NUMBER()"));
        assert!(sql.contains(r#"ORDER BY "version" DESC"#));
        assert!(sql.contains("_meterstore_rank = 1"));
    }

    #[test]
    fn resolution_sql_breaks_ties_deterministically() {
        // `version DESC` alone leaves ROW_NUMBER free to pick either of two tied
        // rows, and this text is published for engines reading files this crate
        // did not write. A settlement that reproduces only sometimes is not one.
        let sql = resolution_sql("readings_versions");
        assert!(
            sql.contains(r#"ORDER BY "version" DESC, "recorded_at" DESC"#),
            "{sql}"
        );
    }

    #[test]
    fn resolution_sql_aliases_the_derived_table() {
        // This text is published for external engines, and PostgreSQL — which
        // backs the SQL catalog, so it is a natural place to paste it — refuses
        // an unaliased subquery. One spelling has to work everywhere.
        let sql = resolution_sql("readings_versions");
        assert!(sql.contains(") AS _meterstore_resolved"), "{sql}");
    }

    #[test]
    fn resolution_sql_does_not_leak_the_ranking_column() {
        // `SELECT *` in the outer query would add `_meterstore_rank` to the
        // result schema, which callers would then see in `SELECT *`.
        let sql = resolution_sql("readings_versions");
        let outer = sql.split("FROM (").next().unwrap();
        assert!(!outer.contains("_meterstore_rank"));
        assert!(!outer.contains('*'), "outer projection must be explicit");
    }

    #[test]
    fn resolution_sql_projects_every_storage_column() {
        let sql = resolution_sql("readings_versions");
        for field in crate::encode::schema::storage_schema(&[]).fields() {
            assert!(
                sql.contains(field.name().as_str()),
                "{} missing from projection",
                field.name()
            );
        }
    }

    #[test]
    fn resolution_sql_partitions_by_the_full_merge_key_and_scope() {
        // Omitting version_scope would rank versions from different network
        // operators against each other, which is meaningless.
        let sql = resolution_sql("readings_versions");
        for column in [col::MALO_ID, col::OBIS_CODE, col::FROM, col::VERSION_SCOPE] {
            assert!(sql.contains(column), "{column} missing from PARTITION BY");
        }
    }

    #[test]
    fn resolution_sql_names_the_requested_table() {
        assert!(resolution_sql("custom_table").contains("custom_table"));
    }

    #[test]
    fn a_recorded_at_ceiling_filters_the_inner_scan_before_ranking() {
        let at = time::macros::datetime!(2026-07-27 06:00 UTC);
        let sql = resolution_sql_with_key("readings_versions", &default_merge_key(), &[], Some(at));
        // The ceiling must sit inside the windowed subquery, so only rows recorded
        // by the instant are ranked — not above it, which would filter on the
        // winner after resolution had already run.
        let inner = sql.split("AS _meterstore_resolved").next().unwrap();
        assert!(inner.contains(r#""recorded_at" <="#), "{sql}");
        assert!(inner.contains("arrow_cast"), "{sql}");
        assert!(inner.contains("WHERE"), "ceiling belongs in the inner scan");
    }

    #[test]
    fn no_ceiling_leaves_the_inner_scan_unfiltered() {
        // `recorded_at` is always a projected column, so the signal is a WHERE in
        // the inner scan (before the alias) — the only WHERE otherwise is the
        // outer `_meterstore_rank = 1`.
        let sql = resolution_sql_with_key("readings_versions", &default_merge_key(), &[], None);
        let inner = sql.split("AS _meterstore_resolved").next().unwrap();
        assert!(
            !inner.contains("WHERE"),
            "no ceiling → no inner filter: {sql}"
        );
    }
}

/// Elision's one property: it may only ever be *wrong in the slow direction*.
#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;

    fn at(hours: i64) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + time::Duration::hours(hours)
    }

    fn file() -> impl Strategy<Value = FileStats> {
        let version = prop_oneof![
            1 => Just(None),
            6 => (0i128..3, 0i128..3)
                .prop_map(|(a, b)| Some(VersionStats { min: a.min(b), max: a.max(b) })),
        ];
        let interval = prop_oneof![
            1 => Just(None),
            6 => (0i64..8, 0i64..8)
                .prop_map(|(a, b)| Some((at(a.min(b)), at(a.max(b))))),
        ];
        (version, interval).prop_map(|(version, interval)| FileStats { version, interval })
    }

    /// Whether two files could hold the same merge key.
    ///
    /// Unknown bounds mean "anything", so they could.
    fn may_share_a_key(a: &FileStats, b: &FileStats) -> bool {
        match (a.interval, b.interval) {
            (Some((a_lo, a_hi)), Some((b_lo, b_hi))) => a_lo <= b_hi && b_lo <= a_hi,
            _ => true,
        }
    }

    /// The condition eliding rests on, stated independently of how [`plan`]
    /// computes it: every file holds one version, and no two files that could
    /// share a key disagree about which version that is.
    ///
    /// Pairwise, where `plan` sweeps into runs — deliberately, because the sweep
    /// is the *conservative* approximation of this and the property below is
    /// one-directional for exactly that reason.
    fn at_most_one_row_per_key(files: &[FileStats]) -> bool {
        if !files
            .iter()
            .all(|f| f.version.is_some_and(|v| v.min == v.max))
        {
            return false;
        }
        for (i, a) in files.iter().enumerate() {
            for b in &files[i + 1..] {
                let (Some(va), Some(vb)) = (a.version, b.version) else {
                    return false;
                };
                if va.min != vb.min && may_share_a_key(a, b) {
                    return false;
                }
            }
        }
        true
    }

    proptest! {
        /// **Statistics must prove absence; their absence proves nothing.**
        /// Eliding returns the raw rows, so eliding wrongly hands back every
        /// superseded version of a corrected interval and every `SUM` over them
        /// is overstated.
        ///
        /// One-directional on purpose. `plan` groups files into maximal runs of
        /// overlap rather than comparing them pairwise, so it can refuse to elide
        /// where the pairwise condition holds — three files where the outer two
        /// are disjoint but both meet the middle one. That costs a window
        /// function. The converse would cost a wrong number, and is what this
        /// asserts cannot happen.
        #[test]
        fn eliding_implies_no_key_can_appear_twice(
            files in prop::collection::vec(file(), 0..8),
        ) {
            prop_assert!(
                !plan(&files).is_elided() || at_most_one_row_per_key(&files),
                "elided over {files:?}",
            );
        }

        /// And the optimisation has to actually fire, or the argument for a
        /// provider rather than a view is worth nothing. Disjoint spans at
        /// arbitrary versions are what consecutive archival windows produce, and
        /// they must always elide.
        #[test]
        fn disjoint_windows_at_any_versions_always_elide(
            versions in prop::collection::vec(0i128..1_000, 1..40),
        ) {
            let files: Vec<FileStats> = versions
                .into_iter()
                .enumerate()
                .map(|(i, v)| {
                    let day = i as i64 * 24;
                    FileStats::single(v, (at(day), at(day + 23)))
                })
                .collect();
            prop_assert!(plan(&files).is_elided(), "{files:?}");
        }

        /// Adding a file can only ever *remove* the right to elide. A scan that
        /// widens to cover more files must not become cheaper.
        #[test]
        fn widening_a_scan_never_grants_elision(
            files in prop::collection::vec(file(), 1..6),
            extra in file(),
        ) {
            let before = plan(&files).is_elided();
            let mut wider = files.clone();
            wider.push(extra);
            prop_assert!(before || !plan(&wider).is_elided());
        }

        /// The answer must not depend on the order the catalogue happened to
        /// list the files in.
        #[test]
        fn the_decision_is_independent_of_file_order(
            files in prop::collection::vec(file(), 0..8),
        ) {
            let forward = plan(&files);
            let mut reversed = files.clone();
            reversed.reverse();
            prop_assert_eq!(forward, plan(&reversed), "{:?}", files);
        }
    }
}
