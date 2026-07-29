//! The TOML front end over the same validated configuration.
//!
//! Everything here produces a [`ValidatedTableConfig`](crate::config::ValidatedTableConfig) — the builder's own output
//! type — so a deployment configured from a file and one configured in Rust pass
//! through exactly the same cross-field checks (§14). There is no second
//! validation path to keep in step, and no setting reachable from one and not the
//! other.
//!
//! # Why the file cannot construct the tiers
//!
//! It names them; it does not open them. A `hot.url` becomes a
//! [`HotSettings`] the application uses to build its own `PgPool`, because the
//! pool is usually shared with the rest of the service and MeterStore never owns
//! a connection (P3, §13.5). The same goes for the catalog: the file records
//! which one and where, and the application constructs it.
//!
//! # Environment interpolation
//!
//! `${VAR}` in any string is replaced from the environment, and a missing
//! variable is an error rather than an empty string. A connection URL that
//! silently became `postgresql://@/` would fail somewhere far from the typo.
//!
//! ```toml
//! [hot]
//! url = "${DATABASE_URL}"
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::Duration;

use crate::arrow::datatypes::{DataType, Field};
use crate::config::TableConfig;
use crate::error::{Error, Result};

/// A whole deployment's configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The hot tier's connection.
    #[serde(default)]
    pub hot: HotSettings,
    /// The cold tier's catalog and warehouse.
    #[serde(default)]
    pub cold: ColdSettings,
    /// One entry per managed table.
    #[serde(default)]
    pub tables: Vec<TableSettings>,
}

impl Settings {
    /// Parse TOML, interpolating `${VAR}` from the environment.
    pub fn from_toml(text: &str) -> Result<Self> {
        let interpolated = interpolate(text)?;
        toml::from_str(&interpolated)
            .map_err(|e| Error::config(format!("invalid meterstore configuration: {e}")))
    }

    /// Read and parse a configuration file.
    pub fn from_path(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::config(format!("cannot read {}: {e}", path.display())))?;
        Self::from_toml(&text)
    }

    /// Render back to TOML.
    ///
    /// Round-trips, so a deployment can normalise a hand-written file — and so a
    /// test can assert the parse did not quietly drop a setting.
    pub fn to_toml(&self) -> Result<String> {
        toml::to_string_pretty(self)
            .map_err(|e| Error::config(format!("cannot render configuration: {e}")))
    }

    /// Validate every table, in declaration order.
    ///
    /// Runs the full cross-field validation, so a file whose `partition_step`
    /// disagrees with its `archival_step` fails here rather than degrading the
    /// purge to a row-wise `DELETE` in production (§8.1).
    pub fn validate(&self) -> Result<Vec<crate::config::ValidatedTableConfig>> {
        if self.tables.is_empty() {
            return Err(Error::config(
                "no [[tables]] declared: a store with no table has nothing to archive or query",
            ));
        }
        self.tables.iter().map(TableSettings::validate).collect()
    }

    /// The single table's validated configuration.
    ///
    /// Convenience for the common deployment, which manages one table. Errors
    /// when the file declares several, rather than silently picking the first.
    pub fn single_table(&self) -> Result<crate::config::ValidatedTableConfig> {
        match self.tables.as_slice() {
            [one] => one.validate(),
            [] => Err(Error::config("no [[tables]] declared")),
            many => Err(Error::config(format!(
                "{} tables declared; use Settings::validate to get all of them",
                many.len()
            ))),
        }
    }
}

/// How to reach PostgreSQL.
///
/// `Debug` is hand-written rather than derived: a connection URL carries a
/// password, and configuration is exactly what a service dumps into its startup
/// log (§19.7).
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HotSettings {
    /// Connection URL. `${VAR}` is interpolated from the environment.
    ///
    /// Never logged: the `Debug` impl redacts it, because a connection URL
    /// carries a password and configuration is exactly what gets dumped into a
    /// startup log.
    #[serde(default)]
    pub url: String,
    /// Pool size.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

impl Default for HotSettings {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: default_max_connections(),
        }
    }
}

impl std::fmt::Debug for HotSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HotSettings")
            .field("url", &redact(&self.url))
            .field("max_connections", &self.max_connections)
            .finish()
    }
}

const fn default_max_connections() -> u32 {
    16
}

/// Which Iceberg catalog, and where the warehouse lives.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColdSettings {
    /// `rest` or `sql`.
    #[serde(default)]
    pub catalog: CatalogKind,
    /// Catalog endpoint (REST) or connection URL (SQL).
    #[serde(default)]
    pub uri: String,
    /// Warehouse root — `s3://…`, `gs://…`, or a local path.
    #[serde(default)]
    pub warehouse: String,
    /// Namespace the tables live in.
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

fn default_namespace() -> String {
    "metering".to_string()
}

/// The catalog implementations this crate can drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogKind {
    /// Iceberg REST catalog. Every engine speaks it, so nothing extra is needed
    /// for external access (§13.7.1).
    #[default]
    Rest,
    /// PostgreSQL-backed SQL catalog. External engines need the JDBC catalog
    /// implementation, which support for is uneven.
    Sql,
}

/// One managed table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSettings {
    /// Physical table name.
    pub name: String,
    /// Hot-tier layout.
    #[serde(default)]
    pub hot: TableHotSettings,
    /// Archival cadence and sizing.
    #[serde(default)]
    pub archival: ArchivalSettings,
    /// Snapshot retention.
    #[serde(default)]
    pub maintenance: MaintenanceSettings,
    /// Columns beyond the core schema (§7.3).
    ///
    /// The `identity` flag is the load-bearing one: an identity column joins the
    /// merge key, so two rows differing in it are different readings. A tenant
    /// discriminator declared as an attribute instead would let one tenant's
    /// correction supersede another's reading.
    #[serde(default)]
    pub extra_columns: Vec<ExtraColumn>,
    /// The column holding pseudonymous subject references (§19.4).
    #[serde(default)]
    pub subject_column: Option<String>,
}

impl TableSettings {
    /// Build and validate the table configuration.
    pub fn validate(&self) -> Result<crate::config::ValidatedTableConfig> {
        let mut config = TableConfig::new(&self.name)
            .partition_step(self.hot.partition_step.0)
            .partition_headroom(self.hot.partition_headroom.0)
            .archival_step(self.archival.archival_step.0)
            .settlement_lag(self.archival.settlement_lag.0)
            .scan_chunk_rows(self.archival.scan_chunk_rows)
            .snapshot_retention(self.maintenance.snapshot_retention.0)
            .min_snapshots_to_keep(self.maintenance.min_snapshots_to_keep);

        let mut seen = BTreeMap::new();
        for column in &self.extra_columns {
            seen.insert(column.name.clone(), column.identity);
            let field = Field::new(&column.name, column.data_type()?, !column.identity);
            config = if column.identity {
                config.identity_column(field)
            } else {
                config.attribute_column(field)
            };
        }

        if let Some(subject) = &self.subject_column {
            // Declaring it twice would be a duplicate-column error from
            // validation, with a message that does not explain the real mistake.
            match seen.get(subject) {
                Some(true) => {
                    return Err(Error::config(format!(
                        "subject_column {subject:?} is also declared as an identity column: a \
                         pseudonymous reference must never join the merge key, or a correction \
                         derived from a re-registered reference silently fails to supersede the \
                         value it corrects (§19.4)"
                    )));
                }
                // Already registered as an attribute, which is what
                // `subject_column` would have done anyway.
                Some(false) => {}
                None => config = config.subject_column(subject),
            }
        }

        config.build()
    }
}

/// Hot-tier partitioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableHotSettings {
    /// Partition granularity. **Must equal `archival.archival_step`** (§7.2).
    #[serde(default = "default_partition_step")]
    pub partition_step: HumanDuration,
    /// How far ahead of the write frontier partitions are pre-created.
    #[serde(default = "default_headroom")]
    pub partition_headroom: HumanDuration,
}

impl Default for TableHotSettings {
    fn default() -> Self {
        Self {
            partition_step: default_partition_step(),
            partition_headroom: default_headroom(),
        }
    }
}

fn default_partition_step() -> HumanDuration {
    HumanDuration(crate::config::defaults::PARTITION_STEP)
}
fn default_headroom() -> HumanDuration {
    HumanDuration(crate::config::defaults::PARTITION_HEADROOM)
}

/// Archival cadence and sizing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivalSettings {
    /// How far behind wall clock archival stays. Must exceed the market's normal
    /// correction window, or a window closes while corrections are still arriving.
    #[serde(default = "default_settlement_lag")]
    pub settlement_lag: HumanDuration,
    /// Window size — one partition per commit.
    #[serde(default = "default_archival_step")]
    pub archival_step: HumanDuration,
    /// Rows fetched per round trip when streaming a scan.
    ///
    /// The bound on archival's peak memory. Rows rather than measuring points:
    /// meters differ by orders of magnitude in how much they report, so a fixed
    /// number of *them* is a variable amount of memory.
    #[serde(default = "default_chunk")]
    pub scan_chunk_rows: usize,
}

impl Default for ArchivalSettings {
    fn default() -> Self {
        Self {
            settlement_lag: default_settlement_lag(),
            archival_step: default_archival_step(),
            scan_chunk_rows: default_chunk(),
        }
    }
}

fn default_settlement_lag() -> HumanDuration {
    HumanDuration(crate::config::defaults::SETTLEMENT_LAG)
}
fn default_archival_step() -> HumanDuration {
    HumanDuration(crate::config::defaults::ARCHIVAL_STEP)
}
const fn default_chunk() -> usize {
    crate::config::defaults::SCAN_CHUNK_ROWS
}

/// Snapshot retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceSettings {
    /// How long cold snapshots are kept.
    ///
    /// **Not a cleanup knob.** A snapshot is what makes a past settlement
    /// reproducible, so this decides how far back an audit can reach. The default
    /// is ten years rather than the days a general-purpose lakehouse would pick.
    #[serde(default = "default_retention")]
    pub snapshot_retention: HumanDuration,
    /// Snapshots kept regardless of age.
    #[serde(default = "default_min_snapshots")]
    pub min_snapshots_to_keep: usize,
}

impl Default for MaintenanceSettings {
    fn default() -> Self {
        Self {
            snapshot_retention: default_retention(),
            min_snapshots_to_keep: default_min_snapshots(),
        }
    }
}

fn default_retention() -> HumanDuration {
    HumanDuration(crate::config::defaults::SNAPSHOT_RETENTION)
}
const fn default_min_snapshots() -> usize {
    crate::config::defaults::MIN_SNAPSHOTS_TO_KEEP
}

/// A deployment column beyond the core schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtraColumn {
    /// Column name.
    pub name: String,
    /// Storage type. Only `string` is supported today (§7.3).
    #[serde(default = "default_column_type")]
    pub r#type: String,
    /// Whether this column is part of a reading's **identity**.
    ///
    /// Identity columns join the merge key and are non-nullable. Get this wrong
    /// for a tenant discriminator and two tenants reporting the same measuring
    /// point share a merge key — a cross-tenant leak with no error anywhere.
    #[serde(default)]
    pub identity: bool,
}

fn default_column_type() -> String {
    "string".to_string()
}

impl ExtraColumn {
    /// The Arrow type this column declares.
    fn data_type(&self) -> Result<DataType> {
        match self.r#type.as_str() {
            "string" | "utf8" | "text" => Ok(DataType::Utf8),
            other => Err(Error::config(format!(
                "extra column {:?} declares type {other:?}; only \"string\" is supported today — \
                 every attribute deployments have wanted (tenant, Bilanzkreis, grid area) is a \
                 string, and supporting more needs a bind arm per type",
                self.name
            ))),
        }
    }
}

/// A duration written the way an operator writes one: `15m`, `1d`, `10y`.
///
/// `time::Duration` has no serde format that reads like a configuration file, and
/// a bare number of seconds would make `snapshot_retention = 315360000` a value
/// nobody can check by eye.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanDuration(pub Duration);

impl Serialize for HumanDuration {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&format_duration(self.0))
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        parse_duration(&text)
            .map(HumanDuration)
            .map_err(serde::de::Error::custom)
    }
}

/// Parse `30s`, `15m`, `6h`, `1d`, `2w`, `10y`.
///
/// A year is 365 days and a week is 7. Neither is a calendar unit here — these
/// configure retention and headroom, not interval arithmetic, and the calendar
/// that *does* matter is `metering`'s (§9.5).
fn parse_duration(text: &str) -> std::result::Result<Duration, String> {
    let trimmed = text.trim();
    let split = trimmed
        .find(|c: char| c.is_ascii_alphabetic())
        .ok_or_else(|| format!("{trimmed:?} has no unit; write 7d, 15m, 10y"))?;
    let (value, unit) = trimmed.split_at(split);

    let value: i64 = value
        .trim()
        .parse()
        .map_err(|_| format!("{value:?} is not a whole number"))?;

    let seconds = match unit.trim() {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 604_800,
        "y" => 31_536_000,
        other => {
            return Err(format!("unknown unit {other:?}; use s, m, h, d, w or y"));
        }
    };

    value
        .checked_mul(seconds)
        .map(Duration::seconds)
        .ok_or_else(|| format!("{trimmed:?} overflows"))
}

/// The inverse of [`parse_duration`], choosing the largest exact unit.
fn format_duration(d: Duration) -> String {
    let s = d.whole_seconds();
    for (unit, size) in [
        ("y", 31_536_000),
        ("w", 604_800),
        ("d", 86_400),
        ("h", 3_600),
        ("m", 60),
    ] {
        if s != 0 && s % size == 0 {
            return format!("{}{unit}", s / size);
        }
    }
    format!("{s}s")
}

/// Replace every `${VAR}` from the environment.
///
/// A missing variable is an error. Substituting an empty string instead would
/// turn a typo in a variable name into a connection URL that fails somewhere far
/// from the mistake, with a message about the wrong thing.
fn interpolate(text: &str) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;

    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let tail = &rest[start + 2..];
        let end = tail.find('}').ok_or_else(|| {
            Error::config("unterminated ${...} in configuration: no closing brace")
        })?;
        let name = &tail[..end];

        let value = std::env::var(name).map_err(|_| {
            Error::config(format!(
                "configuration references ${{{name}}}, which is not set in the environment"
            ))
        })?;
        out.push_str(&value);
        rest = &tail[end + 1..];
    }

    out.push_str(rest);
    Ok(out)
}

/// Hide everything but the shape of a connection URL.
fn redact(url: &str) -> String {
    if url.is_empty() {
        return String::new();
    }
    match url.split_once("://") {
        Some((scheme, _)) => format!("{scheme}://<redacted>"),
        None => "<redacted>".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = r#"
[hot]
url = "postgresql://edm@db.internal/prod"
max_connections = 32

[cold]
catalog = "rest"
uri = "https://catalog.internal"
warehouse = "s3://edm/meterstore"
namespace = "metering"

[[tables]]
name = "readings"
subject_column = "subject_ref"
extra_columns = [
  { name = "tenant", identity = true },
  { name = "bilanzkreis" },
]

[tables.hot]
partition_step = "1d"
partition_headroom = "14d"

[tables.archival]
settlement_lag = "7d"
archival_step = "1d"
scan_chunk_rows = 50000

[tables.maintenance]
snapshot_retention = "10y"
min_snapshots_to_keep = 20
"#;

    #[test]
    fn the_documented_example_parses_and_validates() {
        let settings = Settings::from_toml(EXAMPLE).unwrap();
        assert_eq!(settings.hot.max_connections, 32);
        assert_eq!(settings.cold.catalog, CatalogKind::Rest);

        let table = settings.single_table().unwrap();
        assert_eq!(table.name(), "readings");
        assert_eq!(table.partition_step(), Duration::DAY);
        assert_eq!(table.settlement_lag(), Duration::days(7));
    }

    #[test]
    fn an_identity_column_reaches_the_merge_key() {
        // The whole point of the flag. Declared as an attribute instead, two
        // tenants reporting one measuring point would share a merge key.
        let table = Settings::from_toml(EXAMPLE)
            .unwrap()
            .single_table()
            .unwrap();
        assert!(table.merge_key().contains(&"tenant".to_string()));
        assert!(!table.merge_key().contains(&"bilanzkreis".to_string()));
    }

    #[test]
    fn the_subject_column_is_registered_as_an_attribute() {
        let table = Settings::from_toml(EXAMPLE)
            .unwrap()
            .single_table()
            .unwrap();
        assert_eq!(table.subject_column(), Some("subject_ref"));
        assert!(!table.merge_key().contains(&"subject_ref".to_string()));
    }

    #[test]
    fn a_subject_column_declared_as_identity_is_refused() {
        // In the merge key it looks harmless and is not: a correction whose
        // reference was re-derived gets a different key and fails to supersede.
        let toml = r#"
[[tables]]
name = "readings"
subject_column = "subject_ref"
extra_columns = [{ name = "subject_ref", identity = true }]
"#;
        let err = Settings::from_toml(toml)
            .unwrap()
            .single_table()
            .unwrap_err()
            .to_string();
        assert!(err.contains("merge key"), "{err}");
    }

    #[test]
    fn a_partition_step_that_disagrees_with_archival_is_refused() {
        // The mismatch that silently degrades purge to row-wise DELETE.
        let toml = r#"
[[tables]]
name = "readings"
[tables.hot]
partition_step = "1w"
[tables.archival]
archival_step = "1d"
"#;
        let err = Settings::from_toml(toml)
            .unwrap()
            .single_table()
            .unwrap_err()
            .to_string();
        assert!(err.contains("DELETE"), "{err}");
    }

    #[test]
    fn an_unknown_key_is_an_error_rather_than_ignored() {
        // A typo in a setting name must not leave the default silently in force.
        let toml = r#"
[[tables]]
name = "readings"
[tables.archival]
settlment_lag = "7d"
"#;
        assert!(Settings::from_toml(toml).is_err());
    }

    #[test]
    fn a_file_with_no_tables_is_an_error() {
        assert!(
            Settings::from_toml("[hot]\nurl = \"x\"\n")
                .unwrap()
                .validate()
                .is_err()
        );
    }

    #[test]
    fn several_tables_are_not_silently_narrowed_to_the_first() {
        let toml = "[[tables]]\nname = \"electricity\"\n\n[[tables]]\nname = \"gas\"\n";
        let settings = Settings::from_toml(toml).unwrap();
        assert_eq!(settings.validate().unwrap().len(), 2);
        assert!(settings.single_table().is_err());
    }

    #[test]
    fn durations_parse_the_way_operators_write_them() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::seconds(30));
        assert_eq!(parse_duration("15m").unwrap(), Duration::minutes(15));
        assert_eq!(parse_duration("6h").unwrap(), Duration::hours(6));
        assert_eq!(parse_duration("7d").unwrap(), Duration::days(7));
        assert_eq!(parse_duration("2w").unwrap(), Duration::weeks(2));
        assert_eq!(parse_duration("10y").unwrap(), Duration::days(3_650));
    }

    #[test]
    fn a_duration_with_no_unit_is_rejected() {
        // `settlement_lag = 7` is ambiguous, and guessing days or seconds gets it
        // wrong by five orders of magnitude either way.
        assert!(parse_duration("7").is_err());
        assert!(parse_duration("7 fortnights").is_err());
    }

    #[test]
    fn durations_round_trip_through_the_file_format() {
        for text in ["30s", "15m", "6h", "7d", "2w", "10y"] {
            let parsed = parse_duration(text).unwrap();
            assert_eq!(
                parse_duration(&format_duration(parsed)).unwrap(),
                parsed,
                "{text} did not round-trip"
            );
        }
    }

    #[test]
    fn settings_round_trip_through_toml() {
        let original = Settings::from_toml(EXAMPLE).unwrap();
        let rendered = original.to_toml().unwrap();
        let reparsed = Settings::from_toml(&rendered).unwrap();
        assert_eq!(
            reparsed.single_table().unwrap().merge_key(),
            original.single_table().unwrap().merge_key()
        );
    }

    #[test]
    fn environment_variables_are_interpolated() {
        // `CARGO_PKG_NAME` is set in the environment cargo runs tests in, so the
        // test needs no `set_var` — which this crate forbids anyway, since it
        // requires `unsafe` and is unsound alongside other threads.
        let settings =
            Settings::from_toml("[hot]\nurl = \"postgresql://${CARGO_PKG_NAME}\"\n").unwrap();
        assert_eq!(settings.hot.url, "postgresql://meterstore");
    }

    #[test]
    fn interpolation_handles_several_variables_and_surrounding_text() {
        let settings = Settings::from_toml(
            "[cold]\nwarehouse = \"s3://${CARGO_PKG_NAME}/${CARGO_PKG_NAME}\"\n",
        )
        .unwrap();
        assert_eq!(settings.cold.warehouse, "s3://meterstore/meterstore");
    }

    #[test]
    fn an_unterminated_placeholder_is_an_error() {
        assert!(Settings::from_toml("[hot]\nurl = \"${OOPS\"\n").is_err());
    }

    #[test]
    fn a_missing_environment_variable_is_an_error() {
        // Substituting an empty string would turn a typo into a connection
        // failure somewhere far from the mistake.
        let err = Settings::from_toml("[hot]\nurl = \"${METERSTORE_DEFINITELY_UNSET}\"\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("METERSTORE_DEFINITELY_UNSET"), "{err}");
    }

    #[test]
    fn a_connection_url_is_never_printed_in_full() {
        // Configuration is exactly what gets dumped into a startup log.
        let settings = Settings::from_toml(EXAMPLE).unwrap();
        let shown = format!("{:?}", settings.hot);
        assert!(!shown.contains("db.internal"), "{shown}");
        assert!(shown.contains("postgresql://<redacted>"), "{shown}");
    }

    #[test]
    fn a_non_string_extra_column_is_rejected_with_a_reason() {
        let toml = r#"
[[tables]]
name = "readings"
extra_columns = [{ name = "reading_count", type = "int64" }]
"#;
        let err = Settings::from_toml(toml)
            .unwrap()
            .single_table()
            .unwrap_err()
            .to_string();
        assert!(err.contains("string"), "{err}");
    }
}
