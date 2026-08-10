//! Resolving corrections to the value that currently holds.
//!
//! A correction is a new row at a higher `version`, not an overwrite, so a
//! naive scan returns every version of a corrected interval and summing it
//! double-counts. Resolution keeps the highest version per merge key.
//!
//! The important optimisation is **not doing it**. Corrections are rare and
//! concentrated in recent months, so most historical partitions contain exactly
//! one version per key. Iceberg records per-file `min`/`max` statistics, so when
//! those are equal for `version` the partition provably has no corrections and
//! can be scanned directly — no window function, no sort, no repartition.

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

/// Decide whether a scan over `files` needs version resolution.
///
/// Resolution is elided only when **every** file provably holds one version and
/// no two files disagree — a key corrected in a later file would otherwise be
/// silently returned twice.
///
/// Missing statistics mean resolution is required. Statistics must *prove* the
/// absence of corrections; the absence of statistics proves nothing.
pub fn plan(files: &[Option<VersionStats>]) -> Resolution {
    if files.is_empty() {
        return Resolution::Elided;
    }

    let mut seen: Option<i128> = None;
    for file in files {
        let Some(stats) = file else {
            return Resolution::Required;
        };
        if stats.may_contain_corrections() {
            return Resolution::Required;
        }
        match seen {
            // Two files at different versions may hold the same key twice.
            Some(v) if v != stats.min => return Resolution::Required,
            _ => seen = Some(stats.min),
        }
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
            let micros = at.unix_timestamp_nanos() / 1_000;
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
    ORDER BY "{version}" DESC
  ) AS _meterstore_rank
  FROM {table}{ceiling}
) AS _meterstore_resolved WHERE _meterstore_rank = 1"#,
        version = col::VERSION,
        ceiling = recorded_at_ceiling_clause(recorded_at_ceiling),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const V1: i128 = 20_260_701_000_001;
    const V2: i128 = 20_260_715_000_002;

    #[test]
    fn an_empty_scan_needs_no_resolution() {
        assert_eq!(plan(&[]), Resolution::Elided);
    }

    #[test]
    fn single_version_files_elide_resolution() {
        let files = vec![Some(VersionStats::single(V1)); 4];
        assert!(plan(&files).is_elided());
    }

    #[test]
    fn a_file_spanning_versions_requires_resolution() {
        let files = vec![
            Some(VersionStats::single(V1)),
            Some(VersionStats { min: V1, max: V2 }),
        ];
        assert_eq!(plan(&files), Resolution::Required);
    }

    #[test]
    fn files_at_differing_versions_require_resolution() {
        // Each file holds one version, but a key corrected in the later file
        // would appear in both.
        let files = vec![
            Some(VersionStats::single(V1)),
            Some(VersionStats::single(V2)),
        ];
        assert_eq!(plan(&files), Resolution::Required);
    }

    #[test]
    fn missing_statistics_require_resolution() {
        // Statistics must prove absence; their absence proves nothing.
        let files = vec![Some(VersionStats::single(V1)), None];
        assert_eq!(plan(&files), Resolution::Required);
    }

    #[test]
    fn a_single_file_with_one_version_elides() {
        assert!(plan(&[Some(VersionStats::single(V1))]).is_elided());
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

    fn stats() -> impl Strategy<Value = Option<VersionStats>> {
        prop_oneof![
            1 => Just(None),
            6 => (0i128..4, 0i128..4)
                .prop_map(|(a, b)| Some(VersionStats { min: a.min(b), max: a.max(b) })),
        ]
    }

    proptest! {
        /// **Statistics must prove absence; their absence proves nothing.**
        /// Eliding resolution returns the raw rows, so eliding wrongly hands back
        /// every superseded version of a corrected interval and every `SUM` over
        /// them is overstated. The condition is therefore stated independently
        /// here and compared against the planner's answer.
        #[test]
        fn resolution_is_skipped_only_when_one_version_is_provable(
            files in prop::collection::vec(stats(), 0..8),
        ) {
            // Independently: every file must be readable, hold a single version,
            // and agree with the others on which version that is.
            let provable = files.iter().all(|f| f.is_some_and(|s| s.min == s.max))
                && files
                    .iter()
                    .filter_map(|f| f.map(|s| s.min))
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    <= 1;

            prop_assert_eq!(plan(&files).is_elided(), provable, "{:?}", files);
        }

        /// Adding a file can only ever *remove* the right to elide. A scan that
        /// widens to cover more files must not become cheaper.
        #[test]
        fn widening_a_scan_never_grants_elision(
            files in prop::collection::vec(stats(), 1..6),
            extra in stats(),
        ) {
            let before = plan(&files).is_elided();
            let mut wider = files.clone();
            wider.push(extra);
            prop_assert!(before || !plan(&wider).is_elided());
        }
    }
}
