//! The `meterstore` command-line tool.
//!
//! A thin front end over the same public API a service uses: every subcommand is
//! one or two library calls, and nothing here is unreachable from Rust.
//!
//! Two shapes of use. An **operator** answering a question during an incident
//! reaches for [`Status`](Command::Status) and [`Query`](Command::Query), both of
//! which read the deployment's own configuration file. A **deployment** needs
//! archival to run somewhere: [`Maintain`](Command::Maintain) is the foreground
//! loop a container or a systemd unit wants, [`Archive`](Command::Archive) the
//! one-shot form for cron.
//!
//! There is no `meterstore append`. A reading arrives as an MSCONS message, an
//! SMGW push or a CSV a utility exports its own way, and each needs a mapping
//! this crate has no opinion about — which OBIS code, which network operator
//! issued the version, which Messlokation.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::session::system::TableStatus;
use crate::settings::Settings;

mod render;

pub use render::Format;

/// Hot/cold tiered storage for metering time series.
#[derive(Debug, Parser)]
#[command(
    name = "meterstore",
    version,
    about,
    long_about = None,
    propagate_version = true
)]
pub struct Cli {
    /// Configuration file.
    #[arg(
        short,
        long,
        global = true,
        default_value = "meterstore.toml",
        env = "METERSTORE_CONFIG",
        value_name = "FILE"
    )]
    pub config: PathBuf,

    /// How to render results.
    #[arg(long, global = true, value_enum, default_value_t = Format::Table)]
    pub format: Format,

    /// Log filter, in `RUST_LOG` syntax.
    #[arg(
        long,
        global = true,
        env = "RUST_LOG",
        default_value = "meterstore=info"
    )]
    pub log: String,

    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// The subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Write a starter configuration file.
    ///
    /// Commented, and with the settings that interact placed next to each other —
    /// `settlement_lag` and `archival_step` are each valid alone and wrong when
    /// the lag is the shorter of the two.
    Init {
        /// Overwrite an existing file.
        #[arg(long)]
        force: bool,
    },

    /// Validate the configuration without connecting to anything.
    ///
    /// Runs the full cross-field validation, so a file whose `settlement_lag`
    /// is shorter than its `archival_step` fails here rather than stranding
    /// corrections below the watermark in production.
    Check,

    /// Create every table the configuration declares, in both tiers.
    ///
    /// Idempotent, and safe to run on every start: an existing table is left
    /// alone, and a *changed* declaration is refused rather than silently
    /// ignored.
    Create,

    /// Report each table's boundary, lag and health.
    ///
    /// `invariant_violations` is the one number to alert on — anything but zero
    /// means rows sit below the watermark in PostgreSQL, where no query looks.
    /// `partitions_ahead` is the one that predicts a failure rather than
    /// describing one: at zero, inserts stop.
    Status,

    /// Archive every window that is due, then stop.
    ///
    /// The cron shape. One window per commit, so a store that has been down for a
    /// month catches up over several runs rather than holding one process for
    /// hours — raise `--max-windows` to let a single run go further.
    Archive {
        /// Only this table.
        #[arg(long, value_name = "NAME")]
        table: Option<String>,
        /// Most windows to archive in this run, per table.
        #[arg(long, default_value_t = 8, value_name = "N")]
        max_windows: usize,
    },

    /// Run the maintenance loop in the foreground until interrupted.
    ///
    /// The sidecar shape. Archives what is due, optionally expires snapshots,
    /// then checks the invariant — and repeats. Every replica may run it: one
    /// wins each table's lease and the others report contention and stop, which
    /// is not a failure.
    Maintain {
        /// How often a cycle runs.
        #[arg(long, default_value = "15m", value_name = "DURATION")]
        interval: String,
        /// Also expire cold snapshots older than the configured retention.
        ///
        /// Off by default: retention is a compliance decision, because a snapshot
        /// is what makes a past settlement reproducible.
        #[arg(long)]
        expire_snapshots: bool,
        /// Also anonymise subjects whose readings have all passed a retention
        /// ceiling, given as full calendar years after the year of collection.
        ///
        /// `--anonymise-after-years 3` is § 60 Abs. 6 MsbG's ceiling: erase or
        /// anonymise *"spätestens nach drei Jahren ab dem Schluss des
        /// Kalenderjahres, in dem der jeweilige Messwert erhoben wurde"*. Not
        /// `now - 3 years`, which would erase a January value a year early.
        ///
        /// Off by default, and irreversible when on: destroying a linkage is a
        /// compliance decision, and turning this on is that decision. Requires a
        /// table declaring `subject_column`.
        #[arg(long, value_name = "YEARS")]
        anonymise_after_years: Option<u32>,
        /// Who the audit trail records as having run the sweep.
        #[arg(long, default_value = "meterstore maintain", value_name = "WHO")]
        anonymise_actor: String,
    },

    /// Run a query across both tiers.
    ///
    /// The result carries the boundary it was computed against, which is printed
    /// alongside it: a number from the hot window is only valid for now, and one
    /// from the cold tier alone reproduces.
    Query {
        /// The statement. Reads standard input when `-`.
        #[arg(value_name = "SQL")]
        sql: String,
        /// Read only the settled history, with no load on PostgreSQL.
        #[arg(long, conflicts_with = "operational")]
        historical: bool,
        /// Read only the recent hot window.
        #[arg(long, conflicts_with = "historical")]
        operational: bool,
    },

    /// Show the query plan and the tiers it would read, without running it.
    ///
    /// What a slow query is actually doing: which tiers, over what range, and
    /// whether version resolution was elided.
    Explain {
        /// The statement. Reads standard input when `-`.
        #[arg(value_name = "SQL")]
        sql: String,
    },

    /// List the cold tier's snapshots.
    ///
    /// One of these is what a settlement rerun pins to. The `watermark` column is
    /// where the boundary stood when it was committed.
    Snapshots {
        /// Only this table.
        #[arg(long, value_name = "NAME")]
        table: Option<String>,
    },

    /// Serve the store to external clients.
    ///
    /// Two surfaces, answering different questions. **Flight SQL** carries the
    /// unified hot + cold view — the one thing an external client cannot assemble
    /// for itself, because the hot tier is not in the Iceberg catalogue. The
    /// **catalogue façade** is a read-only Iceberg REST endpoint, which is what a
    /// SQL-catalog deployment needs so Spark, Trino, DuckDB and PyIceberg can read
    /// the settled history straight from object storage — with this process in the
    /// metadata path only, never the data path.
    ///
    /// A deployment already on a REST catalogue needs no façade: point engines at
    /// the endpoint it already has.
    ///
    /// **Unauthenticated, both of them.** This crate has no business deciding what
    /// kind of authentication a deployment uses, and the library hands back a
    /// tonic service and an axum router to wrap in your own interceptor, TLS and
    /// tracing. The CLI binds them bare, so bind them to a loopback address or a
    /// trusted network — never to a public one.
    Serve {
        /// Address to bind Flight SQL to.
        #[arg(long, default_value = "127.0.0.1:50051", value_name = "ADDR")]
        addr: String,
        /// Also serve the read-only Iceberg REST façade here.
        #[arg(long, value_name = "ADDR")]
        catalog_addr: Option<String>,
    },

    /// Destroy a table: every partition, the catalogue entry and the data files.
    ///
    /// The only operation in the crate that deletes stored readings. There is no
    /// recovery path, so the name has to be given twice.
    Purge {
        /// The table to destroy.
        #[arg(long, value_name = "NAME")]
        table: String,
        /// The same name again.
        #[arg(long, value_name = "NAME")]
        confirm: String,
    },
}

/// Run the CLI, returning a process exit code.
///
/// Errors are rendered to stderr rather than returned: a `Result` from `main`
/// prints the `Debug` form, which for this crate's error type is a struct dump
/// rather than the message written for a human.
#[must_use]
pub fn main() -> ExitCode {
    let cli = Cli::parse();
    install_tracing(&cli.log);

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("meterstore: cannot start the async runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run(&cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("meterstore: {e}");
            // A retryable failure is one a supervisor should try again on, and a
            // distinct code is how a shell script tells the two apart without
            // matching on the message.
            match e.is_retryable() {
                true => ExitCode::from(75), // EX_TEMPFAIL
                false => ExitCode::FAILURE,
            }
        }
    }
}

/// Install a subscriber, unless the embedding process already did.
///
/// A library must never do this; a binary is the process, so it may. `try_init`
/// rather than `init` so the CLI running inside a test harness that already set
/// one is not a panic.
fn install_tracing(filter: &str) {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info")))
        .with_writer(std::io::stderr)
        .try_init();
}

impl Cli {
    /// Run this invocation.
    ///
    /// Public so the commands can be driven against a real deployment from a
    /// test rather than only through a process — the alternative is a binary
    /// whose behaviour nothing checks, which for the one surface an operator
    /// reaches for during an incident is the wrong trade.
    ///
    /// [`main`] is this plus argument parsing, a tracing subscriber and the
    /// mapping from an error to an exit code.
    pub async fn run(&self) -> Result<()> {
        run(self).await
    }
}

/// Dispatch one invocation.
async fn run(cli: &Cli) -> Result<()> {
    match &cli.command {
        Command::Init { force } => init(&cli.config, *force),
        Command::Check => check(&cli.config),
        Command::Create => create(cli).await,
        Command::Status => status(cli).await,
        Command::Archive { table, max_windows } => {
            archive(cli, table.as_deref(), *max_windows).await
        }
        Command::Maintain {
            interval,
            expire_snapshots,
            anonymise_after_years,
            anonymise_actor,
        } => {
            maintain(
                cli,
                interval,
                *expire_snapshots,
                *anonymise_after_years,
                anonymise_actor,
            )
            .await
        }
        Command::Query {
            sql,
            historical,
            operational,
        } => query(cli, sql, *historical, *operational).await,
        Command::Explain { sql } => explain(cli, sql).await,
        Command::Snapshots { table } => snapshots(cli, table.as_deref()).await,
        Command::Serve { addr, catalog_addr } => serve(cli, addr, catalog_addr.as_deref()).await,
        Command::Purge { table, confirm } => purge(cli, table, confirm).await,
    }
}

/// The starter configuration [`Command::Init`] writes.
const TEMPLATE: &str = include_str!("template.toml");

fn init(path: &std::path::Path, force: bool) -> Result<()> {
    if path.exists() && !force {
        return Err(Error::config(format!(
            "{} already exists; pass --force to overwrite it",
            path.display()
        )));
    }
    std::fs::write(path, TEMPLATE)
        .map_err(|e| Error::config(format!("cannot write {}: {e}", path.display())))?;
    println!("wrote {}", path.display());
    println!("edit it, then run `meterstore check`");
    Ok(())
}

fn check(path: &std::path::Path) -> Result<()> {
    let settings = Settings::from_path(path)?;
    let tables = settings.validate_all()?;

    println!("{}: valid", path.display());
    for table in &tables {
        println!(
            "  {} — {} model, settlement lag {}, archival step {}",
            table.name(),
            match table.time_model().has_interval_end() {
                true => "interval",
                false => "point",
            },
            fmt_duration(table.settlement_lag()),
            fmt_duration(table.archival_step()),
        );
    }
    Ok(())
}

async fn create(cli: &Cli) -> Result<()> {
    let catalog = load(cli).await?;
    for store in catalog.tables() {
        println!("{}: ready", store.table());
    }
    Ok(())
}

async fn status(cli: &Cli) -> Result<()> {
    let catalog = load(cli).await?;
    let now = OffsetDateTime::now_utc();

    let mut rows = Vec::with_capacity(catalog.len());
    for store in catalog.tables() {
        rows.push(store.status(now).await?);
    }
    render::status(&rows, cli.format)?;

    // A non-zero exit is what a monitoring check reads, so an unhealthy table
    // has to reach it — and the message has to name the *actual* condition. The
    // two ways a table stops working fail differently and want different
    // responses, so reporting one as the other sends an operator to the wrong
    // place.
    let stranded: Vec<&TableStatus> = rows.iter().filter(|r| r.invariant_violations > 0).collect();
    if let Some(first) = stranded.first() {
        return Err(Error::InvariantViolated {
            table: names(&stranded),
            detail: format!(
                "{} row(s) sit below the watermark in PostgreSQL, where no query looks. \
                 Query results may be wrong",
                first.invariant_violations,
            ),
        });
    }

    let starved: Vec<&TableStatus> = rows.iter().filter(|r| !r.healthy).collect();
    match starved.is_empty() {
        true => Ok(()),
        false => Err(Error::config(format!(
            "{} has no hot partition left that can hold a row written from now on, so \
             the next insert fails outright. Archival pre-creates them — check that \
             `meterstore maintain` is running",
            names(&starved),
        ))),
    }
}

/// The table names in a status list, for an error that has to name them.
fn names(rows: &[&TableStatus]) -> String {
    rows.iter()
        .map(|r| r.table.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

async fn archive(cli: &Cli, only: Option<&str>, max_windows: usize) -> Result<()> {
    let catalog = load(cli).await?;
    let now = OffsetDateTime::now_utc();

    let mut lines = Vec::new();
    for store in selected(&catalog, only)? {
        let outcomes = store.archive(now, max_windows).await?;
        lines.push(render::ArchiveLine::of(store.table(), &outcomes));
    }
    render::archive(&lines, cli.format)
}

async fn maintain(
    cli: &Cli,
    interval: &str,
    expire_snapshots: bool,
    anonymise_after_years: Option<u32>,
    actor: &str,
) -> Result<()> {
    let every = crate::settings::parse_human_duration(interval)?;
    let catalog = load(cli).await?;

    let mut maintenance = catalog
        .maintenance()
        .interval(every)
        .expire_snapshots(expire_snapshots);
    if let Some(years) = anonymise_after_years {
        maintenance = maintenance.anonymise_after(
            crate::erasure::Retention::CalendarYears(years),
            "§ 60 Abs. 6 MsbG",
            actor,
        );
    }
    let handle = maintenance.spawn();

    tracing::info!(
        interval = %interval,
        tables = catalog.len(),
        anonymise_after_years,
        "maintenance loop running; press ctrl-c to stop"
    );

    // The loop owns itself, so a signal has to reach it rather than the process
    // simply ending: `shutdown` lets the cycle in flight finish, which for a
    // cycle mid-archival is the difference between an orphaned partition and a
    // clean stop.
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| Error::Storage(format!("cannot listen for ctrl-c: {e}")))?;
    tracing::info!("stopping after the cycle in flight");
    handle.shutdown().await;
    Ok(())
}

async fn query(cli: &Cli, sql: &str, historical: bool, operational: bool) -> Result<()> {
    let sql = read_sql(sql)?;
    let catalog = load(cli).await?;

    let mode = match (historical, operational) {
        (true, _) => Some(crate::ReadMode::Historical),
        (_, true) => Some(crate::ReadMode::Operational),
        _ => None,
    };

    let result = match mode {
        None => catalog.query(&sql).await?,
        Some(mode) => {
            // A read mode is a property of a store rather than of a statement, so
            // the single-table case is the only one that can honour it. Saying so
            // beats running the query against the wrong tiers.
            let store = single(&catalog)?;
            store.in_read_mode(mode).await?.query(&sql).await?
        }
    };
    render::query(&result, cli.format)
}

async fn explain(cli: &Cli, sql: &str) -> Result<()> {
    let sql = read_sql(sql)?;
    let catalog = load(cli).await?;
    let described = catalog.describe(&sql).await?;
    render::describe(&described, cli.format)
}

async fn snapshots(cli: &Cli, only: Option<&str>) -> Result<()> {
    let catalog = load(cli).await?;
    let mut rows = Vec::new();
    for store in selected(&catalog, only)? {
        for snapshot in store.snapshots().await? {
            rows.push((store.table().to_string(), snapshot));
        }
    }
    render::snapshots(&rows, cli.format)
}

/// The tables a `--table` flag selects, or all of them.
///
/// Resolved through the catalog, so the **resolved** name works too: `readings`
/// and `readings_versions` are the two relations of one table, and an operator
/// reading a query has the first in front of them. A name that matches neither is
/// an error naming what is configured, rather than a run that quietly does
/// nothing — which on `archive` would read exactly like "nothing was due".
fn selected<'a>(
    catalog: &'a crate::MeterCatalog,
    only: Option<&str>,
) -> Result<Vec<&'a crate::MeterStore>> {
    let Some(name) = only else {
        return Ok(catalog.tables().collect());
    };
    catalog.table(name).map(|store| vec![store]).ok_or_else(|| {
        Error::config(format!(
            "no table named {name:?} is configured; this deployment has {}",
            catalog
                .tables()
                .map(crate::MeterStore::table)
                .collect::<Vec<_>>()
                .join(", "),
        ))
    })
}

async fn serve(cli: &Cli, addr: &str, catalog_addr: Option<&str>) -> Result<()> {
    use crate::serve::FlightSqlServer;

    let deployment = Settings::from_path(&cli.config)?.connect().await?;
    let facade = deployment.cold.catalog_facade();
    let catalog = deployment.catalog().await?;

    let flight_addr = bind_address(addr)?;
    tracing::warn!(
        "served without authentication; bind to a trusted network only, and put \
         your own TLS and authentication in front of it"
    );

    // One shutdown signal for both surfaces, so ctrl-c cannot leave half a
    // deployment listening. `Notify` rather than a channel: both waiters need
    // the signal and neither should consume it.
    let stop = std::sync::Arc::new(tokio::sync::Notify::new());
    let signal = std::sync::Arc::clone(&stop);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutting down");
        signal.notify_waiters();
    });

    tracing::info!(addr = %flight_addr, tables = catalog.len(), "serving Flight SQL");
    let flight = {
        let stop = std::sync::Arc::clone(&stop);
        tonic::transport::Server::builder()
            .add_service(FlightSqlServer::new(catalog).into_service())
            .serve_with_shutdown(flight_addr, async move { stop.notified().await })
    };

    let Some(catalog_addr) = catalog_addr else {
        return flight
            .await
            .map_err(|e| Error::Storage(format!("Flight SQL server: {e}")));
    };

    let rest_addr = bind_address(catalog_addr)?;
    let listener = tokio::net::TcpListener::bind(rest_addr)
        .await
        .map_err(|e| Error::config(format!("cannot bind {rest_addr}: {e}")))?;
    tracing::info!(addr = %rest_addr, "serving the read-only Iceberg REST facade");
    let rest = axum::serve(listener, facade.router())
        .with_graceful_shutdown(async move { stop.notified().await });

    // Both, and the first failure ends the process. A server still answering
    // metadata while its query surface is down is worse than one that stopped,
    // because a client cannot tell the difference from the outside.
    tokio::try_join!(
        async {
            flight
                .await
                .map_err(|e| Error::Storage(format!("Flight SQL server: {e}")))
        },
        async {
            rest.await
                .map_err(|e| Error::Storage(format!("catalog facade: {e}")))
        },
    )?;
    Ok(())
}

/// Parse an address to bind, naming the shape rather than the parse error alone.
fn bind_address(addr: &str) -> Result<std::net::SocketAddr> {
    addr.parse().map_err(|e| {
        Error::config(format!(
            "{addr:?} is not an address to bind: {e}. Write it as host:port, for \
             example 127.0.0.1:50051"
        ))
    })
}

async fn purge(cli: &Cli, table: &str, confirm: &str) -> Result<()> {
    let catalog = load(cli).await?;
    let [store] = selected(&catalog, Some(table))?[..] else {
        unreachable!("a named selection is exactly one table")
    };
    store.purge_table(confirm).await?;
    println!("{table}: destroyed");
    Ok(())
}

/// Open the deployment the configuration file describes.
///
/// Always a catalog, even for one table: the subcommands all iterate, and a
/// single-table deployment is a catalog of one rather than a different shape.
async fn load(cli: &Cli) -> Result<crate::MeterCatalog> {
    Settings::from_path(&cli.config)?
        .connect()
        .await?
        .catalog()
        .await
}

/// The one store, for a subcommand that cannot mean several.
fn single(catalog: &crate::MeterCatalog) -> Result<&crate::MeterStore> {
    let mut tables = catalog.tables();
    match (tables.next(), tables.next()) {
        (Some(one), None) => Ok(one),
        _ => Err(Error::config(
            "--historical and --operational select which tiers one table is read \
             from, and this configuration declares several. Query them one at a \
             time, or drop the flag and read both tiers",
        )),
    }
}

/// A statement, or standard input when it is `-`.
fn read_sql(sql: &str) -> Result<String> {
    if sql != "-" {
        return Ok(sql.to_string());
    }
    use std::io::Read;
    let mut text = String::new();
    std::io::stdin()
        .read_to_string(&mut text)
        .map_err(|e| Error::config(format!("cannot read the statement from stdin: {e}")))?;
    Ok(text)
}

/// A duration the way the configuration file spells it.
fn fmt_duration(d: time::Duration) -> String {
    crate::settings::format_human_duration(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn the_argument_parser_is_well_formed() {
        // clap's own consistency check: duplicate flags, a `conflicts_with`
        // naming an argument that does not exist, a subcommand with no about.
        // All of them are panics at first use rather than compile errors.
        Cli::command().debug_assert();
    }

    #[test]
    fn the_template_is_a_configuration_file_that_validates() {
        // `meterstore init` writing something `meterstore check` rejects would be
        // the worst possible first impression, and nothing else would catch it:
        // the template is an opaque string to the compiler.
        let settings = Settings::from_toml(
            &TEMPLATE.replace("${DATABASE_URL}", "postgresql://localhost/meterstore"),
        )
        .expect("the template parses");
        settings
            .validate_all()
            .expect("the template passes full validation");
    }

    #[test]
    fn a_missing_config_file_names_itself() {
        let err = check(std::path::Path::new("/nonexistent/meterstore.toml"))
            .expect_err("there is no such file");
        assert!(err.to_string().contains("meterstore.toml"), "{err}");
    }

    #[test]
    fn the_two_read_mode_flags_are_mutually_exclusive() {
        // Both would be a query over no tier at all, and clap refusing it is
        // better than picking one.
        let err = Cli::try_parse_from([
            "meterstore",
            "query",
            "SELECT 1",
            "--historical",
            "--operational",
        ])
        .expect_err("--historical and --operational contradict each other");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn the_facade_is_opt_in_and_flight_is_not() {
        // Flight SQL is the surface a client cannot assemble for itself, so it
        // has a default. The façade is only needed by a SQL-catalog deployment —
        // one already on a REST catalogue has an endpoint engines understand —
        // so binding a second port has to be asked for.
        let bare = Cli::try_parse_from(["meterstore", "serve"]).expect("no address needed");
        let Command::Serve { addr, catalog_addr } = bare.command else {
            panic!("expected serve");
        };
        assert_eq!(addr, "127.0.0.1:50051");
        assert_eq!(catalog_addr, None);

        // And the default is loopback. A public bind on an unauthenticated
        // surface has to be typed out.
        assert!(addr.starts_with("127.0.0.1:"));
    }

    #[test]
    fn an_address_that_is_not_one_says_what_a_bind_address_looks_like() {
        let err = bind_address("50051").expect_err("a port alone is not an address");
        assert!(err.to_string().contains("host:port"), "{err}");
        assert!(bind_address("0.0.0.0:50051").is_ok());
    }

    #[test]
    fn purge_needs_the_name_twice() {
        assert!(
            Cli::try_parse_from(["meterstore", "purge", "--table", "readings_versions"]).is_err(),
            "there is no recovery path, so one mention is not enough"
        );
        assert!(
            Cli::try_parse_from([
                "meterstore",
                "purge",
                "--table",
                "readings_versions",
                "--confirm",
                "readings_versions",
            ])
            .is_ok()
        );
    }
}
