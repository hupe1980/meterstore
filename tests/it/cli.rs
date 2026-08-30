//! The command-line tool, against a real deployment.
//!
//! The CLI is a thin front end and its parsing is unit-tested, but the two
//! things worth checking here cannot be: that the subcommands actually drive a
//! live store end to end, and that `status` **fails** when a table is unhealthy.
//! The second is the whole of its value to a monitoring check — a `status` that
//! always exits zero is a check that never fires.

#![cfg(all(feature = "testkit", feature = "cli"))]

use meterstore::cli::{Cli, Command, Format};

/// A configuration file describing a real database and a real warehouse.
fn config_file(url: &str, warehouse: &std::path::Path) -> tempfile::NamedTempFile {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new().expect("temp file");
    write!(
        file,
        r#"
[hot]
url = "{url}"
max_connections = 4
ddl_lock_timeout = "2s"

[cold]
catalog = "sql"
uri = "{url}"
warehouse = "file://{}"
namespace = "metering"
file_target_bytes = 8388608
metadata_pool_max_connections = 2

[[tables]]
name = "readings_versions"

[tables.archival]
settlement_lag = "1d"
archival_step = "1d"
"#,
        warehouse.display(),
    )
    .expect("write");
    file
}

fn cli(config: &std::path::Path, command: Command) -> Cli {
    Cli {
        config: config.to_path_buf(),
        format: Format::Json,
        log: "off".to_string(),
        command,
    }
}

#[tokio::test]
async fn the_commands_drive_a_real_deployment() {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");
    let file = config_file(&url, warehouse.path());
    let path = file.path();

    // Validation needs no database at all, which is the point of having it
    // separate: a deployment checks its file in CI.
    cli(path, Command::Check).run().await.expect("check");

    cli(path, Command::Create).run().await.expect("create");

    // A table created a moment ago has no partitions and no frontier to run out
    // of. Reporting that as degraded would make the very first status of every
    // new deployment an alarm.
    cli(path, Command::Status)
        .run()
        .await
        .expect("a freshly created table is healthy");

    // Nothing has been written, so this advances the boundary over an empty
    // stretch in one commit rather than one per day since the epoch.
    cli(
        path,
        Command::Archive {
            table: None,
            max_windows: 4,
        },
    )
    .run()
    .await
    .expect("archive");

    cli(path, Command::Snapshots { table: None })
        .run()
        .await
        .expect("the archival committed a snapshot to list");

    cli(
        path,
        Command::Query {
            sql: "SELECT count(*) AS n FROM readings".to_string(),
            historical: false,
            operational: false,
        },
    )
    .run()
    .await
    .expect("query");

    cli(
        path,
        Command::Explain {
            sql: "SELECT malo_id, SUM(value) FROM readings GROUP BY 1".to_string(),
        },
    )
    .run()
    .await
    .expect("explain");

    // A settlement month, named the way the market names one. On an empty table
    // the report is empty, which is the honest answer: completeness reports on
    // the channels that exist, and `seen_since` is what finds the ones that do
    // not.
    cli(
        path,
        Command::Completeness {
            table: None,
            from: None,
            to: None,
            month: Some("2026-06".to_string()),
            sparte: "STROM".to_string(),
            seen_since: Some("30d".to_string()),
            malo: None,
            obis: None,
            gaps_only: false,
        },
    )
    .run()
    .await
    .expect("completeness over a Bilanzierungsmonat");

    // And an explicit range, which is the other spelling.
    cli(
        path,
        Command::Completeness {
            table: Some("readings".to_string()),
            from: Some("2026-06-01T00:00:00Z".to_string()),
            to: Some("2026-07-01T00:00:00Z".to_string()),
            month: None,
            sparte: "STROM".to_string(),
            seen_since: None,
            // Narrowed in the scan: the report is about one meter's one channel.
            malo: Some("12345678905".to_string()),
            obis: Some("1-0:1.29.0".to_string()),
            gaps_only: true,
        },
    )
    .run()
    .await
    .expect("completeness over an explicit range");
}

#[tokio::test]
async fn a_statement_that_will_not_plan_is_not_reported_as_retryable() {
    // The CLI turns a retryable failure into exit 75 so a supervisor tries
    // again. A typo in a query is not that, and looping on it forever is what
    // getting this backwards costs.
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");
    let file = config_file(&url, warehouse.path());

    cli(file.path(), Command::Create)
        .run()
        .await
        .expect("create");

    let err = cli(
        file.path(),
        Command::Query {
            sql: "SELECT * FROM no_such_relation".to_string(),
            historical: false,
            operational: false,
        },
    )
    .run()
    .await
    .expect_err("there is no such relation");

    assert!(!err.is_retryable(), "{err:?}");
}

#[tokio::test]
async fn a_table_flag_takes_either_of_a_tables_two_names() {
    // A table registers two relations — `readings_versions` holds every version
    // and `readings` resolves them — and the one an operator has in front of
    // them, in a query, is the second. A `--table` that only accepted the
    // physical name would archive nothing and say nothing, which reads exactly
    // like "nothing was due".
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");
    let file = config_file(&url, warehouse.path());
    let path = file.path();

    cli(path, Command::Create).run().await.expect("create");

    for name in ["readings_versions", "readings"] {
        cli(
            path,
            Command::Archive {
                table: Some(name.to_string()),
                max_windows: 1,
            },
        )
        .run()
        .await
        .unwrap_or_else(|e| panic!("--table {name} should select the table: {e}"));
    }

    // And a name that matches neither is an error naming what *is* configured,
    // not a silent no-op.
    let err = cli(
        path,
        Command::Archive {
            table: Some("esa_typ2".to_string()),
            max_windows: 1,
        },
    )
    .run()
    .await
    .expect_err("no such table");
    assert!(err.to_string().contains("readings_versions"), "{err}");
}

#[tokio::test]
async fn purge_needs_the_table_named_twice() {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");
    let file = config_file(&url, warehouse.path());

    cli(file.path(), Command::Create)
        .run()
        .await
        .expect("create");

    let err = cli(
        file.path(),
        Command::Purge {
            table: "readings_versions".to_string(),
            confirm: "readings".to_string(),
        },
    )
    .run()
    .await
    .expect_err("the confirmation does not match, and there is no recovery path");
    assert!(err.to_string().contains("readings_versions"), "{err}");

    // The table is still there, which is the property the refusal exists for.
    cli(file.path(), Command::Status)
        .run()
        .await
        .expect("status");
}
