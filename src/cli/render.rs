//! How the CLI puts a result on a terminal, and how it puts one in a pipe.
//!
//! Every subcommand renders through here so that **provenance is never dropped**:
//! a query's rows mean nothing without the boundary they were computed against,
//! so both formats carry it — the table as a footer line, the JSON as a sibling
//! field of the rows rather than a comment nobody parses.

use clap::ValueEnum;
use serde_json::{Value, json};
use time::format_description::well_known::Rfc3339;

use crate::error::{Error, Result};
use crate::session::system::TableStatus;
use crate::session::{QueryDescription, QueryResult};
use crate::tiering::ArchivalOutcome;
use crate::tiering::store::SnapshotInfo;
use crate::watermark::Tier;

/// How to render a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum Format {
    /// Aligned columns, for a terminal.
    #[default]
    Table,
    /// One JSON document, for a pipe.
    Json,
}

/// Print a JSON document, or the aligned form, per `format`.
fn emit(format: Format, json: &Value, table: impl FnOnce()) -> Result<()> {
    match format {
        Format::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(json).map_err(Error::Json)?
            );
            Ok(())
        }
        Format::Table => {
            table();
            Ok(())
        }
    }
}

/// An instant, in the one spelling both formats use.
fn instant(at: time::OffsetDateTime) -> String {
    at.format(&Rfc3339).unwrap_or_else(|_| at.to_string())
}

/// `system.tables`, for one invocation.
pub fn status(rows: &[TableStatus], format: Format) -> Result<()> {
    let document = json!({
        "tables": rows
            .iter()
            .map(|r| json!({
                "table": r.table,
                "watermark": instant(r.watermark),
                "watermark_lag_seconds": r.watermark_lag_seconds,
                "hot_partitions": r.hot_partitions,
                "partitions_ahead": r.partitions_ahead,
                "invariant_violations": r.invariant_violations,
                "healthy": r.healthy,
            }))
            .collect::<Vec<_>>(),
        "healthy": rows.iter().all(|r| r.healthy),
    });

    emit(format, &document, || {
        println!(
            "{:<28} {:<26} {:>10} {:>10} {:>10} {:>10}  HEALTH",
            "TABLE", "WATERMARK", "LAG", "PARTS", "AHEAD", "STRANDED"
        );
        for r in rows {
            println!(
                "{:<28} {:<26} {:>10} {:>10} {:>10} {:>10}  {}",
                r.table,
                instant(r.watermark),
                fmt_lag(r.watermark_lag_seconds),
                r.hot_partitions,
                r.partitions_ahead,
                r.invariant_violations,
                match r.healthy {
                    true => "ok",
                    false => "DEGRADED",
                },
            );
        }
        // Said in words rather than left to a reader to infer from a zero: these
        // are the two numbers worth acting on, and they fail differently.
        if rows.iter().any(|r| r.invariant_violations > 0) {
            println!(
                "\nSTRANDED is non-zero: rows sit below the watermark in PostgreSQL, \
                 where no query looks. Query results may be wrong."
            );
        }
        // `hot_partitions > 0` first: zero partitions and zero ahead are the
        // same two numbers on a table that has not started and on one that has
        // run out, and only the second is worth saying anything about.
        if rows
            .iter()
            .any(|r| r.hot_partitions > 0 && r.partitions_ahead <= 1)
        {
            println!(
                "\nAHEAD is nearly exhausted: at zero, inserts fail outright. \
                 Archival pre-creates partitions — check that it is running."
            );
        }
    })
}

/// What one table's archival run did, folded from its per-window outcomes.
#[derive(Debug, Clone)]
pub struct ArchiveLine {
    /// The table.
    pub table: String,
    /// Windows that moved data.
    pub windows: usize,
    /// Rows moved.
    pub rows: u64,
    /// Orphaned partitions reclaimed.
    pub orphans_reclaimed: usize,
    /// Whether another archiver owns this table right now.
    pub lease_contended: bool,
    /// Whether a statement declined to wait for a lock.
    pub deferred: bool,
    /// Where the boundary stands afterwards.
    pub watermark: time::OffsetDateTime,
}

impl ArchiveLine {
    /// Fold a table's outcomes into one row.
    pub fn of(table: &str, outcomes: &[ArchivalOutcome]) -> Self {
        Self {
            table: table.to_string(),
            windows: outcomes.iter().filter(|o| o.archived_anything()).count(),
            rows: outcomes.iter().map(|o| o.rows).sum(),
            orphans_reclaimed: outcomes.iter().map(|o| o.orphans_reclaimed).sum(),
            lease_contended: outcomes.iter().any(|o| o.lease_contended),
            deferred: outcomes.iter().any(|o| o.deferred),
            watermark: outcomes
                .last()
                .map_or(time::OffsetDateTime::UNIX_EPOCH, |o| o.watermark.get()),
        }
    }
}

/// One archival invocation.
pub fn archive(lines: &[ArchiveLine], format: Format) -> Result<()> {
    let document = json!({
        "tables": lines
            .iter()
            .map(|l| json!({
                "table": l.table,
                "windows": l.windows,
                "rows": l.rows,
                "orphans_reclaimed": l.orphans_reclaimed,
                "lease_contended": l.lease_contended,
                "deferred": l.deferred,
                "watermark": instant(l.watermark),
            }))
            .collect::<Vec<_>>(),
        "rows": lines.iter().map(|l| l.rows).sum::<u64>(),
    });

    emit(format, &document, || {
        println!(
            "{:<28} {:>8} {:>12} {:<26}  NOTE",
            "TABLE", "WINDOWS", "ROWS", "WATERMARK"
        );
        for l in lines {
            println!(
                "{:<28} {:>8} {:>12} {:<26}  {}",
                l.table,
                l.windows,
                l.rows,
                instant(l.watermark),
                // Neither of these is a failure, and both explain a run that
                // moved nothing — which otherwise reads as "up to date".
                match (l.lease_contended, l.deferred, l.orphans_reclaimed) {
                    (true, _, _) => "another archiver holds the lease".to_string(),
                    (_, true, _) => "deferred: a lock was not available".to_string(),
                    (_, _, n) if n > 0 => format!("reclaimed {n} orphaned partition(s)"),
                    _ => String::new(),
                },
            );
        }
    })
}

/// A query result, with the boundary it was computed against.
pub fn query(result: &QueryResult, format: Format) -> Result<()> {
    let document = json!({
        "rows": result.to_json()?,
        "row_count": result.num_rows(),
        "provenance": provenance(
            result.watermarks(),
            result.tiers_scanned(),
            result.touched_hot_tier(),
        ),
    });

    emit(format, &document, || {
        match crate::arrow::util::pretty::pretty_format_batches(result.batches()) {
            Ok(rendered) => println!("{rendered}"),
            Err(e) => println!("(cannot render {} row(s): {e})", result.num_rows()),
        }
        println!();
        print_provenance(
            result.watermarks(),
            result.tiers_scanned(),
            result.touched_hot_tier(),
        );
    })
}

/// A plan's shape, without running it.
pub fn describe(described: &QueryDescription, format: Format) -> Result<()> {
    let document = json!({
        "schema": described
            .schema()
            .fields()
            .iter()
            .map(|f| json!({ "name": f.name(), "type": f.data_type().to_string() }))
            .collect::<Vec<_>>(),
        "provenance": provenance(
            described.watermarks(),
            described.tiers_scanned(),
            described.touched_hot_tier(),
        ),
    });

    emit(format, &document, || {
        println!("{:<28} TYPE", "COLUMN");
        for field in described.schema().fields() {
            println!("{:<28} {}", field.name(), field.data_type());
        }
        println!();
        print_provenance(
            described.watermarks(),
            described.tiers_scanned(),
            described.touched_hot_tier(),
        );
    })
}

/// The cold tier's snapshot list.
pub fn snapshots(rows: &[(String, SnapshotInfo)], format: Format) -> Result<()> {
    let document = json!({
        "snapshots": rows
            .iter()
            .map(|(table, s)| json!({
                "table": table,
                "snapshot_id": s.snapshot_id,
                "committed_at": instant(s.committed_at),
                "watermark": s.watermark.map(|w| instant(w.get())),
                "rows": s.rows,
            }))
            .collect::<Vec<_>>(),
    });

    emit(format, &document, || {
        println!(
            "{:<28} {:>21} {:<26} {:<26} {:>12}",
            "TABLE", "SNAPSHOT", "COMMITTED", "WATERMARK", "ROWS"
        );
        for (table, s) in rows {
            println!(
                "{:<28} {:>21} {:<26} {:<26} {:>12}",
                table,
                s.snapshot_id,
                instant(s.committed_at),
                // A snapshot written out of band carries no boundary. That is
                // legitimate, and it is also the thing that makes expiring an
                // ancestor dangerous, so it is shown rather than blanked.
                s.watermark
                    .map_or_else(|| "— (foreign commit)".to_string(), |w| instant(w.get())),
                s.rows.map_or_else(|| "—".to_string(), |n| n.to_string()),
            );
        }
    })
}

/// The provenance both formats carry.
fn provenance(
    watermarks: &[(String, crate::watermark::TieringWatermark)],
    tiers: &[Tier],
    touched_hot: bool,
) -> Value {
    json!({
        "watermarks": watermarks
            .iter()
            .map(|(table, w)| json!({ "table": table, "watermark": instant(w.get()) }))
            .collect::<Vec<_>>(),
        "tiers_scanned": tiers.iter().map(tier_name).collect::<Vec<_>>(),
        // The one thing a caller has to know about a number before storing it:
        // an answer that touched the hot tier is only true as of now, because
        // those intervals are still being corrected.
        "reproducible": !touched_hot,
    })
}

fn print_provenance(
    watermarks: &[(String, crate::watermark::TieringWatermark)],
    tiers: &[Tier],
    touched_hot: bool,
) {
    for (table, w) in watermarks {
        println!("boundary  {table}: {}", instant(w.get()));
    }
    println!(
        "tiers     {}",
        match tiers.is_empty() {
            true => "none (the range matched nothing)".to_string(),
            false => tiers
                .iter()
                .map(|t| tier_name(t).to_string())
                .collect::<Vec<_>>()
                .join(" + "),
        }
    );
    println!(
        "{}",
        match touched_hot {
            true =>
                "warning   this answer includes the hot window, so it is only valid \
                 for now — those intervals are still being corrected",
            false => "reproducible  cold tier only: this answer does not change",
        }
    );
}

const fn tier_name(tier: &Tier) -> &'static str {
    match tier {
        Tier::Cold => "cold",
        Tier::Hot => "hot",
    }
}

fn fmt_lag(seconds: i64) -> String {
    let (n, unit) = match seconds.abs() {
        s if s >= 86_400 => (s / 86_400, "d"),
        s if s >= 3_600 => (s / 3_600, "h"),
        s if s >= 60 => (s / 60, "m"),
        s => (s, "s"),
    };
    format!("{n}{unit}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lag_is_reported_in_a_unit_a_person_can_read() {
        // A watermark eight days behind is `691200` seconds, and nobody reads
        // that as "the archival job has been down for a week".
        assert_eq!(fmt_lag(45), "45s");
        assert_eq!(fmt_lag(900), "15m");
        assert_eq!(fmt_lag(7_200), "2h");
        assert_eq!(fmt_lag(691_200), "8d");
        // A watermark ahead of the clock is nonsense rather than negative time.
        assert_eq!(fmt_lag(-30), "30s");
    }

    #[test]
    fn provenance_says_whether_an_answer_reproduces() {
        // The whole point of carrying the boundary. A number that touched the
        // hot tier is only true now; one from the cold tier alone is a fact.
        let watermark =
            crate::watermark::TieringWatermark::new(time::macros::datetime!(2026-07-20 00:00 UTC));
        let marks = vec![("readings_versions".to_string(), watermark)];

        let spanning = provenance(&marks, &[Tier::Cold, Tier::Hot], true);
        assert_eq!(spanning["reproducible"], json!(false));
        assert_eq!(spanning["tiers_scanned"], json!(["cold", "hot"]));

        let settled = provenance(&marks, &[Tier::Cold], false);
        assert_eq!(settled["reproducible"], json!(true));
        assert_eq!(
            settled["watermarks"][0]["watermark"],
            json!("2026-07-20T00:00:00Z")
        );
    }

    #[test]
    fn an_archive_line_folds_every_window_of_a_catch_up() {
        // `archive` runs several windows per invocation, and an operator wants
        // one line per table rather than one per commit.
        use crate::watermark::{ArchivalWindow, TieringWatermark};

        let window = |day: i64| {
            let from = time::macros::datetime!(2026-07-20 00:00 UTC) + time::Duration::days(day);
            ArchivalWindow::new(from, from + time::Duration::DAY).unwrap()
        };
        let outcome = |day: i64, rows: u64| ArchivalOutcome {
            window: Some(window(day)),
            rows,
            watermark: window(day).resulting_watermark(),
            orphans_reclaimed: 0,
            partitions_created: 0,
            lease_contended: false,
            deferred: false,
        };

        let line = ArchiveLine::of("readings_versions", &[outcome(0, 96), outcome(1, 100)]);
        assert_eq!(line.windows, 2);
        assert_eq!(line.rows, 196);
        assert_eq!(
            line.watermark,
            time::macros::datetime!(2026-07-22 00:00 UTC)
        );

        // A run that did nothing has to say *why*, or it reads as "up to date".
        let idle = ArchivalOutcome {
            window: None,
            rows: 0,
            watermark: TieringWatermark::empty(),
            orphans_reclaimed: 0,
            partitions_created: 0,
            lease_contended: false,
            deferred: true,
        };
        let line = ArchiveLine::of("readings_versions", &[idle]);
        assert_eq!(line.windows, 0);
        assert!(line.deferred);
    }
}
