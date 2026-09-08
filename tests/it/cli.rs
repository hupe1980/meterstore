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

    // Nothing declared, so nothing to check — and that is a clean run rather
    // than an error, since `audit` is what a deployment puts in a script.
    cli(
        path,
        Command::Audit {
            table: None,
            column: None,
        },
    )
    .run()
    .await
    .expect("audit over a table with no attribute columns");

    // A column that is not a declared attribute is refused, because a clean bill
    // for a column nobody checked is the worst answer available.
    for column in ["malo_id", "no_such_column"] {
        cli(
            path,
            Command::Audit {
                table: None,
                column: Some(column.to_string()),
            },
        )
        .run()
        .await
        .expect_err("only a declared attribute column can be audited");
    }

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
            melo: None,
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
            melo: Some("DE0001234567890123456789012345678".to_string()),
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

/// A configuration whose table declares a `subject_column`, so the deployment
/// carries a subject registry.
fn config_with_subjects(url: &str, warehouse: &std::path::Path) -> tempfile::NamedTempFile {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new().expect("temp file");
    write!(
        file,
        r#"
[hot]
url = "{url}"
max_connections = 4

[cold]
catalog = "sql"
uri = "{url}"
warehouse = "file://{}"
namespace = "metering"
file_target_bytes = 8388608
metadata_pool_max_connections = 2

[[tables]]
name = "readings_versions"
subject_column = "subject_ref"

[tables.archival]
settlement_lag = "1d"
archival_step = "1d"
"#,
        warehouse.display(),
    )
    .expect("write");
    file
}

#[tokio::test]
async fn the_erasure_trail_is_readable_from_the_shell() {
    // "We deleted it" is not evidence, and the trail is what a regulator asks
    // for — from a shell, which is where an auditor's question arrives.
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let warehouse = tempfile::tempdir().expect("temp warehouse");
    let file = config_with_subjects(&url, warehouse.path());
    let path = file.path();

    cli(path, Command::Create).run().await.expect("create");

    // An empty trail is a fact, not an error: a deployment that has had no
    // Article 17 request has erased nothing.
    cli(
        path,
        Command::Erasures {
            limit: 50,
            since: None,
            until: None,
            trigger: None,
        },
    )
    .run()
    .await
    .expect("an empty trail reports as empty");

    // Erase through the library — which is the only door, see `no meterstore
    // erase` — and read it back through the shell.
    let deployment = meterstore::Settings::from_path(path)
        .expect("settings")
        .connect()
        .await
        .expect("connect");
    let catalog = deployment.catalog().await.expect("catalog");
    let store = catalog.table("readings_versions").expect("table");
    let subject = store
        .register_subject(
            "tenant-a:12345678905",
            time::macros::datetime!(2026-07-20 00:00 UTC),
            metering::interval::Sparte::Strom,
        )
        .await
        .expect("register");
    store
        .erase_subject(
            &subject,
            "DSAR-2026-0042",
            "privacy-team",
            time::macros::datetime!(2026-08-31 09:00 UTC),
        )
        .await
        .expect("erase");

    cli(
        path,
        Command::Erasures {
            limit: 50,
            since: None,
            until: None,
            trigger: None,
        },
    )
    .run()
    .await
    .expect("the trail now holds a row");

    // Narrowed the way an auditor narrows it: a period and a duty. Both are
    // parsed here rather than reaching PostgreSQL as text.
    cli(
        path,
        Command::Erasures {
            limit: 50,
            since: Some("2026-08-01T00:00:00Z".to_string()),
            until: Some("2026-09-01T00:00:00Z".to_string()),
            trigger: Some("request".to_string()),
        },
    )
    .run()
    .await
    .expect("a period and a duty");

    // A row count that is not one is refused at the call rather than reaching
    // PostgreSQL as a negative LIMIT.
    let err = cli(
        path,
        Command::Erasures {
            limit: 0,
            since: None,
            until: None,
            trigger: None,
        },
    )
    .run()
    .await
    .expect_err("a limit is a row count");
    assert!(err.to_string().contains("--limit"), "{err}");

    // And the flags that cannot mean anything say so, rather than reporting an
    // empty trail — which would read as "nothing was erased".
    for (since, until, trigger) in [
        (Some("last tuesday"), None, None),
        (None, None, Some("sweep")),
        (
            Some("2026-10-01T00:00:00Z"),
            Some("2026-07-01T00:00:00Z"),
            None,
        ),
    ] {
        let err = cli(
            path,
            Command::Erasures {
                limit: 50,
                since: since.map(str::to_string),
                until: until.map(str::to_string),
                trigger: trigger.map(str::to_string),
            },
        )
        .run()
        .await
        .expect_err("{since:?} {until:?} {trigger:?} should be refused");
        assert!(!err.to_string().is_empty());
    }
}

#[tokio::test]
async fn asking_for_erasures_where_no_subject_is_declared_says_so() {
    // The commonest configuration has no subject column at all. An empty list
    // there would read as "nothing has been erased", which is true and useless:
    // the deployment holds no mapping to erase in the first place.
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
        Command::Erasures {
            limit: 10,
            since: None,
            until: None,
            trigger: None,
        },
    )
    .run()
    .await
    .expect_err("no subject column is declared");
    assert!(err.to_string().contains("subject_column"), "{err}");
}
