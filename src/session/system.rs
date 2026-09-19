//! `meterstore.system.*` — operational state as queryable tables.
//!
//! The handle already exposes the watermark and the invariant check as methods,
//! which serves a program. It does not serve the person holding a pager at 3am,
//! who has a SQL client and a question: is archival keeping up, is anything
//! stranded in the wrong tier, will inserts fail tonight.
//!
//! These are snapshots, computed when queried. They are deliberately cheap —
//! counts and a watermark read — because a diagnostic that is expensive to run
//! is one nobody runs during an incident.

use std::sync::Arc;

use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use time::OffsetDateTime;

use crate::arrow::array::{
    BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use crate::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use crate::config::ValidatedTableConfig;
use crate::error::{Error, Result};
use crate::tiering::store::{ColdStore, HotStore};

/// The schema name system tables are registered under.
pub const SCHEMA: &str = "system";

/// One row of `meterstore.system.tables`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStatus {
    /// The physical table.
    pub table: String,
    /// Where cold ends and hot begins.
    pub watermark: OffsetDateTime,
    /// How far behind wall clock the watermark sits.
    pub watermark_lag_seconds: i64,
    /// Hot partitions that exist, attached or detached.
    ///
    /// Partitions rather than rows: counting rows means scanning the hot tier,
    /// and these answer the same questions — is archival keeping up, is anything
    /// left behind — from a catalog lookup.
    pub hot_partitions: i64,
    /// Partitions that can still hold a row written now or later.
    ///
    /// **Reaching zero stops inserts outright.** The one number on this row that
    /// predicts a hard failure rather than describing one.
    pub partitions_ahead: i64,
    /// Rows below the watermark that are still in PostgreSQL.
    ///
    /// Must be zero. Anything else means a query can return wrong results,
    /// because the tier split assumes a row's interval start decides where it
    /// lives.
    pub invariant_violations: i64,
    /// Whether the tiers partition the data as they should.
    pub healthy: bool,
}

fn status_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table", DataType::Utf8, false),
        Field::new(
            "watermark",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("watermark_lag_seconds", DataType::Int64, false),
        Field::new("hot_partitions", DataType::Int64, false),
        Field::new("partitions_ahead", DataType::Int64, false),
        Field::new("invariant_violations", DataType::Int64, false),
        Field::new("healthy", DataType::Boolean, false),
    ]))
}

/// One row of `meterstore.system.config`.
///
/// Configuration is worth exposing because the settings that matter interact:
/// a partition step that disagrees with the archival step, or a settlement lag
/// shorter than a window, are both accepted individually and wrong together.
/// Seeing them side by side is how that gets noticed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    /// The table the setting belongs to.
    ///
    /// Carried per row rather than implied by the relation, because a session
    /// may host several tables and "which table is this `settlement_lag`
    /// for" is the first question a row raises once there is more than one.
    pub table: String,
    /// Setting name.
    pub setting: String,
    /// Its value, rendered.
    pub value: String,
}

fn config_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table", DataType::Utf8, false),
        Field::new("setting", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

/// Gathers operational state and registers it as queryable tables.
pub struct SystemTables<'a> {
    hot: &'a Arc<dyn HotStore>,
    cold: &'a Arc<dyn ColdStore>,
    config: &'a ValidatedTableConfig,
}

impl<'a> SystemTables<'a> {
    /// Build a collector over one store's tiers.
    pub fn new(
        hot: &'a Arc<dyn HotStore>,
        cold: &'a Arc<dyn ColdStore>,
        config: &'a ValidatedTableConfig,
    ) -> Self {
        Self { hot, cold, config }
    }

    /// Current status of the managed table.
    pub async fn status(&self, now: OffsetDateTime) -> Result<TableStatus> {
        let table = self.config.name();
        let watermark = self.cold.watermark(table).await?;
        let violations = self.hot.invariant_violations(table, watermark).await? as i64;

        crate::observe::metrics()
            .invariant_violations
            .record(violations.max(0) as u64, &crate::observe::table(table));

        // A catalog lookup, not a scan — which is the whole reason this replaced
        // a row count. `None` means the store cannot enumerate its partitions, so
        // the columns report `-1` for *that store* rather than inventing a
        // plausible number; every store this crate ships can answer.
        let partitions = self.hot.partition_starts(table).await?;
        let (hot_partitions, partitions_ahead) = match &partitions {
            Some(starts) => (
                starts.len() as i64,
                crate::tiering::store::partitions_ahead(starts, now, self.config.archival_step())
                    as i64,
            ),
            None => (-1, -1),
        };

        Ok(TableStatus {
            table: table.to_string(),
            watermark: watermark.get(),
            watermark_lag_seconds: (now - watermark.get()).whole_seconds(),
            hot_partitions,
            partitions_ahead,
            invariant_violations: violations,
            healthy: is_healthy(violations, hot_partitions, partitions_ahead),
        })
    }

    /// The settings that decide archival behaviour.
    pub fn config_entries(&self) -> Vec<ConfigEntry> {
        let c = self.config;
        let entry = |setting: &str, value: String| ConfigEntry {
            table: c.name().to_string(),
            setting: setting.to_string(),
            value,
        };
        vec![
            entry("table", c.name().to_string()),
            entry("merge_key", c.merge_key().join(", ")),
            entry(
                "archival_step",
                format!("{}s", c.archival_step().whole_seconds()),
            ),
            entry(
                "settlement_lag",
                format!("{}s", c.settlement_lag().whole_seconds()),
            ),
            entry(
                "partition_headroom",
                format!("{}s", c.partition_headroom().whole_seconds()),
            ),
            // Belongs beside the two above for the same reason they belong
            // beside each other: it is valid alone and wrong next to a query
            // that outlives it, and nothing can check that from here.
            entry(
                "reader_grace",
                format!("{}s", c.reader_grace().whole_seconds()),
            ),
            // And beside `reader_grace` for the same reason again: the grace is
            // the hysteresis, this is the cap on the quiescence, and a value
            // below the longest query turns a held window into a refused one.
            entry(
                "max_pin_age",
                format!("{}s", c.max_pin_age().whole_seconds()),
            ),
            entry(
                "expected_hot_partitions",
                c.expected_hot_partitions().to_string(),
            ),
            entry("scan_chunk_rows", c.scan_chunk_rows().to_string()),
            // Published on the cold table as `write.target-file-size-bytes`, so
            // a compactor's eligibility test measures against the size this
            // table really writes instead of Iceberg's 512 MiB default. Unset is
            // the status quo and not a safe default, which is why it reads as a
            // sentence rather than as a dash.
            entry(
                "declared_file_size",
                c.declared_file_size().map_or_else(
                    || "unset: a maintenance tool assumes 512 MiB".to_string(),
                    |bytes| bytes.to_string(),
                ),
            ),
            // What `to` and `value` mean on this table, which an external engine
            // cannot infer from a single row: `to IS NULL` is the signal, and a
            // table whose window happens to hold no readings shows nothing.
            // Summing a Zählerstandsgang produces a number with no meaning that
            // looks exactly like a consumption total.
            entry("time_model", c.time_model().as_str().to_string()),
            entry(
                "identity_columns",
                c.identity_columns()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
        ]
    }

    /// The SQL an external engine must apply to read the raw table correctly.
    ///
    /// This is the primary mitigation for the version-resolution trap,
    /// and it is a correctness matter rather than an ergonomic one: an engine
    /// reading the Iceberg files directly sees **every version** of a corrected
    /// interval, and a naive `SELECT SUM(value)` double-counts each one —
    /// silently, in a number someone will bill from.
    ///
    /// Exposed as a queryable row rather than only as a Rust method, because the
    /// person who needs it is holding a Trino session, not a compiler.
    pub fn resolution_entries(&self, raw_table: &str) -> Vec<ConfigEntry> {
        let table = self.config.name().to_string();
        vec![
            ConfigEntry {
                table: table.clone(),
                setting: "raw_table".to_string(),
                value: raw_table.to_string(),
            },
            ConfigEntry {
                table: table.clone(),
                setting: "resolution_sql".to_string(),
                value: crate::planner::version::resolution_sql_with_key(
                    raw_table,
                    &self.config.merge_key(),
                    &self.config.extra_columns(),
                    None,
                ),
            },
            // The second rule an external engine needs — and unlike the first
            // it is answered with a column rather than an expression. Gas
            // balances on the 06:00–06:00 Gastag, and SQL dialects differ on the
            // timestamp arithmetic that computes it, so the rule is applied at
            // write time and published as the name of the column holding its
            // answer.
            ConfigEntry {
                table: table.clone(),
                setting: "balancing_day_column".to_string(),
                value: crate::encode::schema::col::BALANCING_DAY.to_string(),
            },
            ConfigEntry {
                table,
                setting: "warning".to_string(),
                value: format!(
                    "{raw_table} holds every version of every reading. Summing it without \
                     the resolution SQL above double-counts every corrected interval. And \
                     group daily aggregates by the {} column, never by date_trunc('day', \
                     \"from\"): that is UTC rather than Berlin for every commodity, and gas \
                     is balanced on the 06:00-06:00 Gastag rather than the calendar day.",
                    crate::encode::schema::col::BALANCING_DAY,
                ),
            },
        ]
    }

    /// Every committed state of the cold table, newest first.
    pub async fn snapshot_entries(&self) -> Result<Vec<crate::tiering::store::SnapshotInfo>> {
        self.cold.snapshots(self.config.name()).await
    }

    /// Register the system tables into a session under the `system` schema.
    ///
    /// A real schema rather than dotted table names, so `system.tables` parses
    /// as a qualified reference and reads like every other catalog.
    ///
    /// Snapshots taken now. Re-register to refresh — deliberately explicit, so a
    /// query never silently pays for a round trip to both tiers.
    pub async fn register(&self, ctx: &SessionContext, now: OffsetDateTime) -> Result<()> {
        register_all(ctx, std::slice::from_ref(self), now).await
    }
}

/// Register the system tables for **every** table a session hosts.
///
/// One relation per concern, one row set per table, discriminated by the
/// `table` column. A session with several tables otherwise gets either
/// four relations per table — which no operator wants to `UNION` by hand — or
/// one relation whose rows cannot be attributed.
///
/// Re-registering replaces: these are snapshots computed when asked, and a
/// second call is a refresh rather than a duplicate.
pub async fn register_all(
    ctx: &SessionContext,
    tables: &[SystemTables<'_>],
    now: OffsetDateTime,
) -> Result<()> {
    use datafusion::catalog::MemorySchemaProvider;
    use datafusion::catalog::SchemaProvider;

    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| Error::Storage("default catalog missing".into()))?;

    let schema = match catalog.schema(SCHEMA) {
        Some(existing) => existing,
        None => {
            let created: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
            catalog
                .register_schema(SCHEMA, Arc::clone(&created))
                .map_err(Error::from)?;
            created
        }
    };

    let mut statuses = Vec::with_capacity(tables.len());
    let mut settings = Vec::new();
    let mut resolution = Vec::new();
    let mut snapshots = Vec::new();

    for t in tables {
        statuses.push(t.status(now).await?);
        settings.extend(t.config_entries());

        // The mitigation an external engine needs, reachable from the SQL client
        // the operator already has open.
        let raw = if t.config.name().ends_with("_versions") {
            t.config.name().to_string()
        } else {
            format!("{}_versions", t.config.name())
        };
        resolution.extend(t.resolution_entries(&raw));

        // Snapshots are what a reproducible read pins to, so finding one must
        // not require reading Iceberg metadata by hand.
        snapshots.push(snapshot_batch(
            t.config.name(),
            &t.snapshot_entries().await?,
        )?);
    }

    // **Deregistered first**, because these are snapshots and this is a *refresh*:
    // `MemorySchemaProvider::register_table` refuses a name it already holds, so
    // every call after the first would fail.
    //
    // A missing table is the ordinary case on the first pass, so a failure to
    // deregister is not one: only the registration that follows has to succeed.
    let register = |name: &str, schema_ref: SchemaRef, batches: Vec<RecordBatch>| {
        let _ = schema.deregister_table(name);
        schema
            .register_table(
                name.to_string(),
                Arc::new(MemTable::try_new(schema_ref, vec![batches])?),
            )
            .map_err(Error::from)?;
        Ok::<(), Error>(())
    };

    register("tables", status_schema(), vec![status_batch(&statuses)?])?;
    register("config", config_schema(), vec![config_batch(&settings)?])?;
    register(
        "resolution",
        config_schema(),
        vec![config_batch(&resolution)?],
    )?;
    register("snapshots", snapshot_schema(), snapshots)?;

    Ok(())
}

/// The schema of `system.snapshots`.
fn snapshot_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table", DataType::Utf8, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new(
            "committed_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "watermark",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("rows", DataType::Int64, true),
        Field::new("written_by_meterstore", DataType::Boolean, false),
    ]))
}

/// Encode the snapshot list as a batch.
pub fn snapshot_batch(
    table: &str,
    rows: &[crate::tiering::store::SnapshotInfo],
) -> Result<RecordBatch> {
    let micros = crate::encode::schema::micros;
    Ok(RecordBatch::try_new(
        snapshot_schema(),
        vec![
            Arc::new(StringArray::from(vec![table; rows.len()])),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|r| micros(r.committed_at))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|r| r.watermark.map(|w| micros(w.get())))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.rows.map(|n| i64::try_from(n).unwrap_or(i64::MAX)))
                    .collect::<Vec<_>>(),
            )),
            // A snapshot with no watermark was written by something else — an
            // out-of-band compaction, say. Legitimate, readable, and worth being
            // able to see, because it explains a gap in the watermark column.
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|r| r.watermark.is_some())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?)
}

/// Encode statuses as a batch.
pub fn status_batch(rows: &[TableStatus]) -> Result<RecordBatch> {
    let micros = crate::encode::schema::micros;
    Ok(RecordBatch::try_new(
        status_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.table.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter().map(|r| micros(r.watermark)).collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.watermark_lag_seconds)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.hot_partitions).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.partitions_ahead).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.invariant_violations)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.healthy).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

/// The two ways a table stops working, as one column.
///
/// **Wrong answers now** — rows below the watermark still in PostgreSQL — and
/// **no answers shortly** — no partition left that can hold a row written from
/// here on, which makes the next insert fail outright. An operator reading one
/// health column should not have to know the second is tracked elsewhere.
///
/// # A table with no partitions is *not started*, not *exhausted*
///
/// The two look identical in `partitions_ahead` and are opposites. A table
/// created a moment ago has no partitions and no frontier to run out of, and
/// reporting it degraded made the very first status of every new deployment an
/// alarm — which is how an alert stops being read. The condition becomes real
/// the moment the table holds a partition at all.
///
/// # A store that cannot count is not asserted healthy
///
/// [`HotStore::partition_starts`](crate::tiering::store::HotStore::partition_starts)
/// may answer `None`, which reports as `-1`. That is neither zero partitions nor
/// a runway, so it fails the check rather than passing it on a number that was
/// never obtained: "cannot say" must not read as "fine".
const fn is_healthy(violations: i64, hot_partitions: i64, partitions_ahead: i64) -> bool {
    violations == 0
        && match hot_partitions {
            0 => true,
            n if n < 0 => false,
            _ => partitions_ahead > 0,
        }
}

/// Encode configuration entries as a batch.
pub fn config_batch(rows: &[ConfigEntry]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        config_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.table.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.setting.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.value.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TableConfig;
    use time::macros::datetime;

    fn status(violations: i64) -> TableStatus {
        TableStatus {
            table: "readings_versions".to_string(),
            watermark: datetime!(2026-07-20 00:00 UTC),
            watermark_lag_seconds: 86_400,
            hot_partitions: 21,
            partitions_ahead: 14,
            invariant_violations: violations,
            healthy: violations == 0,
        }
    }

    #[test]
    fn a_status_batch_matches_its_schema() {
        let batch = status_batch(&[status(0)]).unwrap();
        assert_eq!(batch.schema(), status_schema());
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn health_covers_both_ways_a_table_stops_working() {
        // Wrong answers now, and no answers shortly: a table with no partition
        // ahead of the frontier rejects the next insert, and an operator reading
        // one health column should not have to know that lives elsewhere.
        assert!(is_healthy(0, 21, 14), "the ordinary steady state");
        assert!(!is_healthy(1, 21, 14), "wrong answers now");
        assert!(!is_healthy(0, 21, 0), "no answers shortly");
    }

    #[test]
    fn a_table_that_has_not_started_is_not_reported_degraded() {
        // Zero partitions and zero ahead are the same two numbers whether a
        // table was created a second ago or has run its runway out, and they
        // mean opposite things. Reporting the first as degraded made the very
        // first `status` of every new deployment an alarm.
        assert!(is_healthy(0, 0, 0), "created, never written to");
        assert!(!is_healthy(0, 1, 0), "one partition, none ahead: exhausted");

        // And a store that cannot enumerate its partitions reports -1, which is
        // not a runway. "Cannot say" must not read as "fine".
        assert!(!is_healthy(0, -1, -1));
    }

    #[test]
    fn the_write_runway_is_counted_from_partitions_that_exist() {
        use crate::tiering::store::partitions_ahead;
        use time::Duration;

        let starts = [
            datetime!(2026-07-18 00:00 UTC),
            datetime!(2026-07-19 00:00 UTC),
            datetime!(2026-07-20 00:00 UTC),
            datetime!(2026-07-21 00:00 UTC),
        ];
        // Mid-day: the partition holding `now` counts, and so does every later
        // one — those are where the next writes land.
        assert_eq!(
            partitions_ahead(&starts, datetime!(2026-07-20 13:47 UTC), Duration::DAY),
            2
        );
        // Past the last one: the very next insert has nowhere to go. This is the
        // value a configuration-derived gauge could never produce.
        assert_eq!(
            partitions_ahead(&starts, datetime!(2026-07-22 00:00 UTC), Duration::DAY),
            0
        );
        assert_eq!(
            partitions_ahead(&[], datetime!(2026-07-20 00:00 UTC), Duration::DAY),
            0
        );
    }

    #[test]
    fn config_exposes_the_settings_that_interact() {
        // `settlement_lag` and `archival_step` are each valid alone and wrong
        // together — a lag shorter than a window strands corrections below the
        // watermark. Showing them side by side is the point of the table.
        let config = TableConfig::new("readings").build().unwrap();
        let hot: Arc<dyn HotStore> = Arc::new(NoStore);
        let cold: Arc<dyn ColdStore> = Arc::new(NoStore);
        let entries = SystemTables::new(&hot, &cold, &config).config_entries();

        let names: Vec<_> = entries.iter().map(|e| e.setting.as_str()).collect();
        for expected in [
            "archival_step",
            "partition_headroom",
            "settlement_lag",
            "reader_grace",
            "max_pin_age",
            "merge_key",
            "expected_hot_partitions",
        ] {
            assert!(names.contains(&expected), "{expected} missing");
        }
    }

    #[test]
    fn config_reports_the_merge_key_including_identity_columns() {
        // If this disagrees with the table's primary key, corrections silently
        // fail to supersede — so it is worth being able to read it back.
        let config = TableConfig::new("readings")
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            .build()
            .unwrap();
        let hot: Arc<dyn HotStore> = Arc::new(NoStore);
        let cold: Arc<dyn ColdStore> = Arc::new(NoStore);
        let entries = SystemTables::new(&hot, &cold, &config).config_entries();

        let key = entries.iter().find(|e| e.setting == "merge_key").unwrap();
        assert!(key.value.contains("tenant"));
    }

    #[test]
    fn a_config_batch_matches_its_schema() {
        let batch = config_batch(&[ConfigEntry {
            table: "readings_versions".to_string(),
            setting: "x".into(),
            value: "y".into(),
        }])
        .unwrap();
        assert_eq!(batch.schema(), config_schema());
    }

    /// A store that is never called — these tests exercise pure config.
    struct NoStore;

    #[async_trait::async_trait]
    impl HotStore for NoStore {
        async fn append_reporting(
            &self,
            _: &str,
            _: &[String],
            _: &[RecordBatch],
        ) -> Result<Vec<crate::session::Displacement>> {
            Ok(Vec::new())
        }

        async fn drop_table(&self, _: &str) -> Result<()> {
            Ok(())
        }

        async fn create_tables(
            &self,
            _: &str,
            _: &[String],
            _: &[Field],
            _: crate::config::TimeModel,
        ) -> Result<()> {
            unreachable!()
        }
        async fn append(&self, _: &str, _: &[String], _: &[RecordBatch]) -> Result<u64> {
            unreachable!()
        }
        async fn scan_range(
            &self,
            _: &str,
            _: crate::planner::TimeRange,
            _: &crate::tiering::store::ScanSpec,
        ) -> Result<crate::tiering::store::BatchStream> {
            unreachable!()
        }
        async fn ensure_partitions(
            &self,
            _: &str,
            _: OffsetDateTime,
            _: OffsetDateTime,
            _: time::Duration,
        ) -> Result<Vec<crate::tiering::store::PartitionId>> {
            unreachable!()
        }
        async fn partition_exists(&self, _: &crate::tiering::store::PartitionId) -> Result<bool> {
            unreachable!()
        }
        async fn detach_partition(&self, _: &crate::tiering::store::PartitionId) -> Result<()> {
            unreachable!()
        }
        async fn scan_detached(
            &self,
            _: &crate::tiering::store::PartitionId,
            _: &crate::tiering::store::ScanSpec,
        ) -> Result<crate::tiering::store::BatchStream> {
            unreachable!()
        }
        async fn drop_partition(
            &self,
            _: &crate::tiering::store::PartitionId,
            _: time::OffsetDateTime,
        ) -> Result<crate::tiering::store::Reclamation> {
            unreachable!()
        }
        async fn orphaned_partitions(
            &self,
            _: &str,
        ) -> Result<Vec<crate::tiering::store::PartitionId>> {
            unreachable!()
        }
        async fn invariant_violations(
            &self,
            _: &str,
            _: crate::watermark::TieringWatermark,
        ) -> Result<u64> {
            unreachable!()
        }
    }

    #[async_trait::async_trait]
    impl ColdStore for NoStore {
        async fn purge_table(&self, _: &str) -> Result<()> {
            Ok(())
        }

        async fn create_tables(
            &self,
            _: &str,
            _: &[String],
            _: &[Field],
            _: &crate::tiering::store::MaintenancePolicy,
        ) -> Result<()> {
            unreachable!()
        }
        async fn watermark(&self, _: &str) -> Result<crate::watermark::TieringWatermark> {
            unreachable!()
        }
        async fn append_and_commit(
            &self,
            _: &str,
            _: crate::tiering::store::BatchStream,
            _: crate::tiering::store::WriteHints,
            _: crate::watermark::ArchivalWindow,
            _: time::Duration,
            _: time::OffsetDateTime,
        ) -> Result<crate::tiering::store::CommitInfo> {
            unreachable!()
        }
        async fn append_only(
            &self,
            _: &str,
            _: crate::tiering::store::BatchStream,
            _: crate::tiering::store::WriteHints,
        ) -> Result<crate::tiering::store::CommitInfo> {
            unreachable!()
        }
        async fn expire_snapshots(
            &self,
            _: &str,
            _: time::Duration,
            _: usize,
            _: OffsetDateTime,
        ) -> Result<usize> {
            unreachable!()
        }
    }
}
