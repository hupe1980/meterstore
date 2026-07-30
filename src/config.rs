//! Table and archival configuration.
//!
//! Validation happens once, at construction, with actionable messages. Settings
//! that must agree with each other are checked here rather than discovered as a
//! performance cliff in production.

use time::Duration;

use crate::arrow::datatypes::{DataType, Field};
use crate::error::{Error, Result};

/// Arrow field-metadata key under which an attribute column declares its
/// allowed-value set. The hot-table DDL reads it to render a `CHECK … IN (…)`;
/// it is inert everywhere else (schema evolution compares name/type/nullability
/// only, and the cold tier ignores it), so a coded column stays a plain `Utf8`
/// column with one extra constraint. See [`coded_column`].
pub const CHECK_VALUES_KEY: &str = "meterstore.check_values";

/// A `Utf8` attribute-column [`Field`] constrained to `allowed` values.
///
/// A deployment's coded columns — an ingestion source, a delivery status — get
/// the same DB-layer enforcement meterstore already gives `sparte`/`unit`/
/// `quality`: a value outside the set fails the write rather than being read back
/// later as an unknown flag. meterstore stays domain-agnostic — it renders
/// whatever set the caller supplies. Pass to
/// [`TableConfig::attribute_column`](TableConfig::attribute_column).
///
/// Codes are the domain's stable strings (e.g. `"MSCONS"`); a code must not
/// contain a comma, which delimits the set in the field metadata.
#[must_use]
pub fn coded_column(name: &str, allowed: &[&str], nullable: bool) -> Field {
    debug_assert!(
        allowed.iter().all(|c| !c.contains(',')),
        "coded_column values must not contain a comma"
    );
    Field::new(name, DataType::Utf8, nullable).with_metadata(std::collections::HashMap::from([(
        CHECK_VALUES_KEY.to_string(),
        allowed.join(","),
    )]))
}

/// Defaults chosen for German 15-minute metering at utility scale.
pub mod defaults {
    use time::Duration;

    /// Hot-table partition granularity.
    pub const PARTITION_STEP: Duration = Duration::DAY;
    /// Archival window size. Must equal [`PARTITION_STEP`].
    pub const ARCHIVAL_STEP: Duration = Duration::DAY;
    /// How far behind wall clock archival stays.
    pub const SETTLEMENT_LAG: Duration = Duration::weeks(1);
    /// How far ahead of the write frontier partitions are pre-created.
    pub const PARTITION_HEADROOM: Duration = Duration::weeks(2);
    /// Rows per Parquet file before rolling over.
    pub const MAX_ROWS_PER_FILE: usize = 5_000_000;
    /// Target Parquet data file size.
    pub const TARGET_FILE_SIZE: usize = 512 * 1024 * 1024;
    /// Rows fetched per round trip when streaming a scan.
    ///
    /// Bounds the memory a scan holds regardless of how much the range covers.
    /// Rows rather than measuring points: meters differ by orders of magnitude
    /// in how much they report, so a fixed number of *them* is a variable amount
    /// of memory.
    pub const SCAN_CHUNK_ROWS: usize = 50_000;
    /// How long cold snapshots are kept.
    ///
    /// Far longer than a general-purpose lakehouse default, because a snapshot
    /// is what makes a past settlement reproducible.
    pub const SNAPSHOT_RETENTION: Duration = Duration::days(3_653);
    /// Snapshots kept regardless of age.
    pub const MIN_SNAPSHOTS_TO_KEEP: usize = 20;
}

/// Everything MeterStore needs to know about one table.
#[derive(Debug, Clone)]
pub struct TableConfig {
    name: String,
    partition_step: Duration,
    archival_step: Duration,
    settlement_lag: Duration,
    partition_headroom: Duration,
    max_rows_per_file: usize,
    target_file_size: usize,
    scan_chunk_rows: usize,
    snapshot_retention: Duration,
    min_snapshots_to_keep: usize,
    identity_columns: Vec<Field>,
    attribute_columns: Vec<Field>,
    subject_column: Option<String>,
}

impl TableConfig {
    /// Start from the defaults for a table of the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partition_step: defaults::PARTITION_STEP,
            archival_step: defaults::ARCHIVAL_STEP,
            settlement_lag: defaults::SETTLEMENT_LAG,
            partition_headroom: defaults::PARTITION_HEADROOM,
            max_rows_per_file: defaults::MAX_ROWS_PER_FILE,
            target_file_size: defaults::TARGET_FILE_SIZE,
            scan_chunk_rows: defaults::SCAN_CHUNK_ROWS,
            snapshot_retention: defaults::SNAPSHOT_RETENTION,
            min_snapshots_to_keep: defaults::MIN_SNAPSHOTS_TO_KEEP,
            identity_columns: Vec::new(),
            attribute_columns: Vec::new(),
            subject_column: None,
        }
    }

    /// Set the hot-table partition granularity.
    pub fn partition_step(mut self, step: Duration) -> Self {
        self.partition_step = step;
        self
    }

    /// Set the archival window size.
    pub fn archival_step(mut self, step: Duration) -> Self {
        self.archival_step = step;
        self
    }

    /// Set how far behind wall clock archival stays.
    pub fn settlement_lag(mut self, lag: Duration) -> Self {
        self.settlement_lag = lag;
        self
    }

    /// Set how far ahead partitions are pre-created.
    pub fn partition_headroom(mut self, headroom: Duration) -> Self {
        self.partition_headroom = headroom;
        self
    }

    /// Set how many rows a streaming scan fetches per round trip.
    ///
    /// This is the bound on archival's peak memory: nothing on the path holds a
    /// window, so what it does hold is one chunk.
    pub fn scan_chunk_rows(mut self, rows: usize) -> Self {
        self.scan_chunk_rows = rows;
        self
    }

    /// How long cold snapshots are kept.
    pub fn snapshot_retention(mut self, retention: Duration) -> Self {
        self.snapshot_retention = retention;
        self
    }

    /// Snapshots kept regardless of age.
    pub fn min_snapshots_to_keep(mut self, count: usize) -> Self {
        self.min_snapshots_to_keep = count;
        self
    }

    /// Declare a column that is part of a reading's **identity**.
    ///
    /// Identity columns join the merge key, so two rows differing in one are
    /// different readings and neither can supersede the other.
    ///
    /// A tenant discriminator belongs here. Declared as an attribute instead, two
    /// tenants reporting the same measuring point would share a merge key, and
    /// one tenant's correction would supersede the other's reading — a data leak
    /// with no error anywhere.
    ///
    /// Identity columns must be non-nullable: a null cannot identify anything.
    pub fn identity_column(mut self, field: Field) -> Self {
        self.identity_columns.push(field);
        self
    }

    /// Declare a column that carries **data** about a reading.
    ///
    /// Bilanzkreis, grid area, tariff reference — values that describe a reading
    /// without distinguishing it from another. A correction may change them.
    pub fn attribute_column(mut self, field: Field) -> Self {
        self.attribute_columns.push(field);
        self
    }

    /// Declare the column holding pseudonymous subject references.
    ///
    /// Registers an attribute column of that name and marks it as the link to
    /// the [`SubjectRegistry`](crate::erasure::SubjectRegistry), so writes can
    /// be checked against it and erasure has something to act on.
    ///
    /// # Why this is an attribute and not an identity column
    ///
    /// A measuring point produces one reading per interval no matter who
    /// occupies it, so the reference is functionally determined by the reading
    /// rather than part of what identifies it. Putting it in the merge key would
    /// look harmless and would not be: a correction whose reference was derived
    /// slightly differently — a re-registration, a pipeline reading a stale
    /// mapping — gets a different key and silently fails to supersede the value
    /// it corrects. That is the same failure the OBIS canonicalisation exists to
    /// prevent, and it stays invisible until someone sums a corrected month.
    ///
    /// Erasure does not need it in the key. It destroys the *mapping*, which
    /// leaves every row unattributable regardless of where the column sits.
    pub fn subject_column(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        self.attribute_columns
            .push(Field::new(&name, DataType::Utf8, true));
        self.subject_column = Some(name);
        self
    }

    /// Validate and freeze.
    pub fn build(self) -> Result<ValidatedTableConfig> {
        if self.name.is_empty() {
            return Err(Error::config("table name must not be empty"));
        }
        if self.partition_step <= Duration::ZERO {
            return Err(Error::config("partition_step must be positive"));
        }
        if self.archival_step <= Duration::ZERO {
            return Err(Error::config("archival_step must be positive"));
        }

        // The purge is a partition drop, so an archival window must correspond to
        // exactly one partition. A coarser partition would force the archiver
        // back to row-wise DELETE — millions of dead tuples per day and the
        // vacuum debt that follows. A finer one multiplies partition count for
        // no benefit. Neither degrades loudly, so it is rejected here.
        if self.partition_step != self.archival_step {
            return Err(Error::config(format!(
                "partition_step ({}) must equal archival_step ({}): a purge drops \
                 exactly one partition per archived window, and any mismatch \
                 silently degrades the purge to row-wise DELETE",
                fmt_duration(self.partition_step),
                fmt_duration(self.archival_step),
            )));
        }

        if self.settlement_lag < Duration::ZERO {
            return Err(Error::config("settlement_lag must not be negative"));
        }
        // A lag shorter than one window means archival could close a window that
        // is still receiving corrections, stranding them below the watermark.
        if self.settlement_lag < self.archival_step {
            return Err(Error::config(format!(
                "settlement_lag ({}) must be at least one archival_step ({}), \
                 or corrections can arrive for an already-archived window",
                fmt_duration(self.settlement_lag),
                fmt_duration(self.archival_step),
            )));
        }
        if self.partition_headroom < self.partition_step {
            return Err(Error::config(
                "partition_headroom must cover at least one partition_step, \
                 or inserts will fail before a new partition exists",
            ));
        }
        if self.scan_chunk_rows == 0 {
            return Err(Error::config(
                "scan_chunk_rows must be positive: a zero-row chunk would page forever",
            ));
        }
        if self.max_rows_per_file == 0 {
            return Err(Error::config("max_rows_per_file must be positive"));
        }
        if self.snapshot_retention <= Duration::ZERO {
            return Err(Error::config("snapshot_retention must be positive"));
        }
        if self.min_snapshots_to_keep == 0 {
            return Err(Error::config(
                "min_snapshots_to_keep must be at least 1: expiring every snapshot \
                 would leave the table unreadable",
            ));
        }

        for f in self.identity_columns.iter().chain(&self.attribute_columns) {
            if !matches!(f.data_type(), DataType::Utf8) {
                return Err(Error::config(format!(
                    "extra column {:?} is {:?}; only Utf8 is supported today — every \
                     attribute deployments have wanted (tenant, Bilanzkreis, grid area) \
                     is a string, and supporting more needs a bind arm per type",
                    f.name(),
                    f.data_type()
                )));
            }
        }

        for f in &self.identity_columns {
            if f.is_nullable() {
                return Err(Error::config(format!(
                    "identity column {:?} must be non-nullable: a null cannot identify a reading",
                    f.name()
                )));
            }
        }

        let mut seen = std::collections::HashSet::new();
        for f in self.identity_columns.iter().chain(&self.attribute_columns) {
            if !seen.insert(f.name().clone()) {
                return Err(Error::config(format!("duplicate column {:?}", f.name())));
            }
            if crate::encode::schema::storage_schema(&[])
                .field_with_name(f.name())
                .is_ok()
            {
                return Err(Error::config(format!(
                    "extra column {:?} collides with a core column",
                    f.name()
                )));
            }
        }

        Ok(ValidatedTableConfig(self))
    }
}

/// A [`TableConfig`] that has passed validation.
///
/// Constructing one is the only way to reach the archival machinery, so the
/// checks in [`TableConfig::build`] cannot be skipped.
#[derive(Debug, Clone)]
pub struct ValidatedTableConfig(TableConfig);

impl ValidatedTableConfig {
    /// Table name.
    pub fn name(&self) -> &str {
        &self.0.name
    }
    /// Hot-table partition granularity.
    pub fn partition_step(&self) -> Duration {
        self.0.partition_step
    }
    /// Archival window size.
    pub fn archival_step(&self) -> Duration {
        self.0.archival_step
    }
    /// How far behind wall clock archival stays.
    pub fn settlement_lag(&self) -> Duration {
        self.0.settlement_lag
    }
    /// How far ahead partitions are pre-created.
    pub fn partition_headroom(&self) -> Duration {
        self.0.partition_headroom
    }
    /// Rows per Parquet file before rolling over.
    pub fn max_rows_per_file(&self) -> usize {
        self.0.max_rows_per_file
    }
    /// Target Parquet data file size.
    pub fn target_file_size(&self) -> usize {
        self.0.target_file_size
    }
    /// Rows fetched per round trip when streaming a scan.
    pub fn scan_chunk_rows(&self) -> usize {
        self.0.scan_chunk_rows
    }

    /// How a chunked scan of this table's hot tier must be shaped.
    ///
    /// Built here rather than at each call site so the merge key, the deployment
    /// columns and the chunk size cannot disagree between the query path and the
    /// archival path.
    pub fn scan_spec(&self) -> crate::tiering::store::ScanSpec {
        crate::tiering::store::ScanSpec::new(
            self.merge_key(),
            self.extra_columns()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        )
        .with_chunk_rows(self.scan_chunk_rows())
    }
    /// How long cold snapshots are kept.
    pub fn snapshot_retention(&self) -> Duration {
        self.0.snapshot_retention
    }

    /// Snapshots kept regardless of age.
    pub fn min_snapshots_to_keep(&self) -> usize {
        self.0.min_snapshots_to_keep
    }

    /// Columns that are part of a reading's identity.
    pub fn identity_columns(&self) -> &[Field] {
        &self.0.identity_columns
    }

    /// The identity columns' names, in declaration order.
    ///
    /// These are also the cold tier's leading partition fields: an identity
    /// column is by definition something every query filters on, which is
    /// exactly what a partition field should be.
    pub fn identity_column_names(&self) -> Vec<String> {
        self.0
            .identity_columns
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Columns that carry data about a reading.
    pub fn attribute_columns(&self) -> &[Field] {
        &self.0.attribute_columns
    }

    /// The column holding pseudonymous subject references, if declared.
    pub fn subject_column(&self) -> Option<&str> {
        self.0.subject_column.as_deref()
    }

    /// Every deployment-declared column, identity first.
    ///
    /// Identity columns come first so their position is stable as attributes are
    /// added — the storage schema appends them after the core columns in this
    /// order.
    pub fn extra_columns(&self) -> Vec<Field> {
        self.0
            .identity_columns
            .iter()
            .chain(&self.0.attribute_columns)
            .cloned()
            .collect()
    }

    /// The full merge key: the core columns plus any identity columns.
    ///
    /// This is what decides whether one reading supersedes another, so it is
    /// derived in one place and used by the schema, the hot-table primary key
    /// and the resolution SQL alike.
    pub fn merge_key(&self) -> Vec<String> {
        crate::encode::schema::MERGE_KEY
            .iter()
            .map(|s| (*s).to_string())
            .chain(self.0.identity_columns.iter().map(|f| f.name().clone()))
            .collect()
    }

    /// How many hot partitions should exist at steady state.
    ///
    /// Enough to cover the settlement lag plus the pre-creation headroom.
    pub fn expected_hot_partitions(&self) -> i64 {
        let span = self.0.settlement_lag + self.0.partition_headroom;
        (span.whole_seconds() / self.0.partition_step.whole_seconds()).max(1)
    }
}

/// Render a duration the way the configuration file spells it.
fn fmt_duration(d: Duration) -> String {
    let secs = d.whole_seconds();
    match secs {
        s if s % 86_400 == 0 => format!("{}d", s / 86_400),
        s if s % 3_600 == 0 => format!("{}h", s / 3_600),
        s if s % 60 == 0 => format!("{}m", s / 60),
        s => format!("{s}s"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::datatypes::DataType;

    fn base() -> TableConfig {
        TableConfig::new("readings")
    }

    #[test]
    fn coded_column_carries_its_vocabulary_in_metadata() {
        let f = coded_column("source", &["MSCONS", "DIRECT_PUSH"], true);
        assert_eq!(f.name(), "source");
        assert_eq!(f.data_type(), &DataType::Utf8);
        assert!(f.is_nullable());
        assert_eq!(
            f.metadata().get(CHECK_VALUES_KEY).map(String::as_str),
            Some("MSCONS,DIRECT_PUSH")
        );
    }

    #[test]
    fn a_plain_attribute_column_declares_no_vocabulary() {
        let f = Field::new("bilanzkreis", DataType::Utf8, true);
        assert!(f.metadata().get(CHECK_VALUES_KEY).is_none());
    }

    #[test]
    fn defaults_validate() {
        let c = base().build().unwrap();
        assert_eq!(c.name(), "readings");
        assert_eq!(c.partition_step(), Duration::DAY);
        assert_eq!(c.archival_step(), Duration::DAY);
    }

    #[test]
    fn partition_step_must_equal_archival_step() {
        // The mismatch that silently degrades purge to row-wise DELETE.
        let err = base()
            .partition_step(Duration::weeks(1))
            .archival_step(Duration::DAY)
            .build()
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("partition_step"), "{msg}");
        assert!(
            msg.contains("DELETE"),
            "message must name the consequence: {msg}"
        );
    }

    #[test]
    fn settlement_lag_must_cover_at_least_one_window() {
        // Otherwise a window can be archived while still receiving corrections.
        let err = base()
            .partition_step(Duration::days(7))
            .archival_step(Duration::days(7))
            .settlement_lag(Duration::DAY)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("settlement_lag"));
    }

    #[test]
    fn zero_settlement_lag_is_rejected_when_a_window_is_positive() {
        assert!(base().settlement_lag(Duration::ZERO).build().is_err());
    }

    #[test]
    fn headroom_must_cover_a_partition() {
        let err = base()
            .partition_headroom(Duration::hours(1))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("partition_headroom"));
    }

    #[test]
    fn non_positive_steps_are_rejected() {
        assert!(base().partition_step(Duration::ZERO).build().is_err());
        assert!(base().archival_step(-Duration::DAY).build().is_err());
    }

    #[test]
    fn empty_name_is_rejected() {
        assert!(TableConfig::new("").build().is_err());
    }

    #[test]
    fn the_scan_spec_carries_everything_a_chunked_scan_needs() {
        // Built in one place so the query path and the archival path cannot end
        // up with different keys, different columns or different bounds.
        let c = base()
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .scan_chunk_rows(1234)
            .build()
            .unwrap();
        let spec = c.scan_spec();

        assert_eq!(spec.merge_key(), c.merge_key().as_slice());
        assert_eq!(spec.extra(), ["tenant", "bilanzkreis"]);
        assert_eq!(spec.chunk_rows(), Some(1234));
        // The cursor must be unique per row, and start with the declared sort
        // order so the Parquet footer stays truthful.
        assert_eq!(
            spec.cursor_columns(),
            ["malo_id", "from", "obis_code", "tenant", "version"]
        );
    }

    #[test]
    fn a_zero_row_chunk_is_rejected() {
        assert!(base().scan_chunk_rows(0).build().is_err());
    }

    #[test]
    fn extra_columns_must_not_collide_with_core_columns() {
        let err = base()
            .attribute_column(Field::new("malo_id", DataType::Utf8, true))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("collides"));
    }

    #[test]
    fn extra_columns_must_be_unique_across_both_kinds() {
        let err = base()
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn identity_columns_join_the_merge_key() {
        // The point of the distinction: a tenant discriminator must make two
        // readings different readings, not merely differently-labelled ones.
        let c = base()
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            .build()
            .unwrap();
        assert_eq!(
            c.merge_key(),
            vec!["malo_id", "obis_code", "from", "tenant"]
        );
    }

    #[test]
    fn attribute_columns_stay_out_of_the_merge_key() {
        let c = base()
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .build()
            .unwrap();
        assert_eq!(c.merge_key(), vec!["malo_id", "obis_code", "from"]);
    }

    #[test]
    fn a_nullable_identity_column_is_rejected() {
        // A null cannot identify a reading, and in SQL it does not compare equal
        // to itself — two such rows would never resolve against each other.
        let err = base()
            .identity_column(Field::new("tenant", DataType::Utf8, true))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("non-nullable"));
    }

    #[test]
    fn identity_columns_come_before_attributes_in_the_schema() {
        let c = base()
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            .build()
            .unwrap();
        let names: Vec<_> = c.extra_columns().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, ["tenant", "bilanzkreis"]);
    }

    #[test]
    fn valid_extra_columns_are_kept_in_order() {
        let c = base()
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .attribute_column(Field::new("netzgebiet", DataType::Utf8, true))
            .build()
            .unwrap();
        let extra = c.extra_columns();
        let names: Vec<_> = extra.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["bilanzkreis", "netzgebiet"]);
    }

    #[test]
    fn expected_hot_partitions_covers_lag_plus_headroom() {
        // 7d lag + 14d headroom at 1d granularity.
        assert_eq!(base().build().unwrap().expected_hot_partitions(), 21);

        let weekly = base()
            .partition_step(Duration::weeks(1))
            .archival_step(Duration::weeks(1))
            .settlement_lag(Duration::weeks(2))
            .partition_headroom(Duration::weeks(2))
            .build()
            .unwrap();
        assert_eq!(weekly.expected_hot_partitions(), 4);
    }

    #[test]
    fn duration_formatting_is_human_readable() {
        assert_eq!(fmt_duration(Duration::DAY), "1d");
        assert_eq!(fmt_duration(Duration::weeks(1)), "7d");
        assert_eq!(fmt_duration(Duration::hours(6)), "6h");
        assert_eq!(fmt_duration(Duration::minutes(15)), "15m");
        assert_eq!(fmt_duration(Duration::seconds(90)), "90s");
    }
}
