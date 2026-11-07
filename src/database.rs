//! The store's PostgreSQL connection pool.
//!
//! The store opens its own pool from a URL and sizing the deployment chooses,
//! and no driver type is in its API, so a caller's own driver upgrades alone.
//! Every connection it opens:
//!
//! - **follows the TLS rule:** `verify-full` unless the URL or `PGSSLMODE` says
//!   otherwise; `prefer` and `allow` are refused;
//! - **is cleared at check-in:** `pg_advisory_unlock_all()` runs on release, so
//!   an archive lease dropped without its release — a task abort, a `timeout`,
//!   shutdown — frees its lock instead of riding the connection back into the
//!   pool and wedging archival as `lease_contended`.
//!
//! Share one [`Database`] between the hot tier and the subject registry; they
//! live in the same database.

use std::str::FromStr;

use time::Duration;

use crate::error::{Error, Result};

/// A pool of connections to the hot tier's PostgreSQL.
///
/// Cheap to clone: every clone is a handle on the same pool. Opened with
/// [`connect`](Self::connect) or [`connect_with`](Self::connect_with), and
/// handed to [`PostgresHot::new`](crate::PostgresHot::new) and
/// [`SubjectRegistry::new`](crate::erasure::SubjectRegistry::new).
#[derive(Clone)]
pub struct Database {
    pool: sqlx::PgPool,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The URL carries a password, and `Debug` output is what reaches a log.
        f.debug_struct("Database")
            .field("size", &self.pool.size())
            .field("idle", &self.pool.num_idle())
            .finish_non_exhaustive()
    }
}

/// How a [`Database`] sizes and recycles its pool.
///
/// Defaults: ten connections, none kept open when idle, a 30 s wait for a
/// connection, idle connections closed after 10 min, every connection recycled
/// after 30 min, and `application_name = meterstore` so `pg_stat_activity` says
/// whose connections they are.
#[derive(Debug, Clone)]
pub struct DatabaseOptions {
    max_connections: u32,
    min_connections: u32,
    acquire_timeout: Duration,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    application_name: Option<String>,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_connections: 0,
            acquire_timeout: Duration::seconds(30),
            idle_timeout: Some(Duration::minutes(10)),
            max_lifetime: Some(Duration::minutes(30)),
            application_name: Some("meterstore".to_string()),
        }
    }
}

impl DatabaseOptions {
    /// The defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The most connections the pool opens. Zero is refused when the pool is
    /// opened: a pool that can hand out no connection blocks the first query
    /// for ever rather than failing.
    #[must_use]
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = n;
        self
    }

    /// Connections kept open while idle.
    #[must_use]
    pub fn min_connections(mut self, n: u32) -> Self {
        self.min_connections = n;
        self
    }

    /// How long a caller waits for a connection before the operation fails.
    #[must_use]
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// How long an idle connection is kept; `None` keeps it.
    #[must_use]
    pub fn idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// How long a connection lives before it is recycled; `None` never.
    #[must_use]
    pub fn max_lifetime(mut self, lifetime: Option<Duration>) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    /// The `application_name` every connection reports; `None` leaves the
    /// URL's or the server's.
    #[must_use]
    pub fn application_name(mut self, name: Option<String>) -> Self {
        self.application_name = name;
        self
    }

    fn pool_options(
        &self,
        url: &str,
    ) -> Result<(
        sqlx::postgres::PgPoolOptions,
        sqlx::postgres::PgConnectOptions,
    )> {
        if self.max_connections == 0 {
            return Err(Error::config(
                "max_connections is 0: a pool that can hand out no connection blocks \
                 the first query for ever rather than failing",
            ));
        }
        let url = crate::tls::require_tls(url, std::env::var("PGSSLMODE").ok().as_deref())?;
        let mut connect = sqlx::postgres::PgConnectOptions::from_str(&url).map_err(|e| {
            Error::config(format!(
                "{} is not a PostgreSQL URL: {e}",
                crate::error::redacted(&url)
            ))
        })?;
        if let Some(name) = &self.application_name {
            connect = connect.application_name(name);
        }
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(self.max_connections)
            .min_connections(self.min_connections)
            .acquire_timeout(std_duration("acquire_timeout", self.acquire_timeout)?)
            .idle_timeout(optional("idle_timeout", self.idle_timeout)?)
            .max_lifetime(optional("max_lifetime", self.max_lifetime)?)
            // See the module documentation: a lease dropped without its release
            // must not ride its connection back into the pool.
            .after_release(|conn, _| {
                Box::pin(async move {
                    sqlx::query("SELECT pg_advisory_unlock_all()")
                        .execute(conn)
                        .await?;
                    Ok(true)
                })
            });
        Ok((pool, connect))
    }
}

fn std_duration(setting: &str, d: Duration) -> Result<std::time::Duration> {
    std::time::Duration::try_from(d)
        .map_err(|_| Error::config(format!("{setting} is negative ({d})")))
}

fn optional(setting: &str, d: Option<Duration>) -> Result<Option<std::time::Duration>> {
    d.map(|d| std_duration(setting, d)).transpose()
}

impl Database {
    /// Open a pool with the default [`DatabaseOptions`].
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the URL is malformed or names `sslmode=prefer` or
    /// `allow`; [`Error::Storage`] when the first connection fails, with a hint
    /// when the failure is TLS.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with(url, &DatabaseOptions::default()).await
    }

    /// Open a pool, establishing one connection before returning so a wrong
    /// URL, credential or certificate fails here rather than on the first query.
    ///
    /// # Errors
    ///
    /// As [`connect`](Self::connect), and [`Error::Config`] for options it
    /// refuses.
    pub async fn connect_with(url: &str, options: &DatabaseOptions) -> Result<Self> {
        let (pool, connect) = options.pool_options(url)?;
        let pool = pool.connect_with(connect).await.map_err(|e| {
            Error::Storage(format!(
                "connecting to {}: {e}{}",
                crate::error::redacted(url),
                tls_hint(&e),
            ))
        })?;
        Ok(Self { pool })
    }

    /// Build a pool that connects on first use.
    ///
    /// For a service whose startup must not wait on the database. Nothing is
    /// checked against the server until the first query.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] for a malformed URL or refused options.
    pub fn connect_lazy_with(url: &str, options: &DatabaseOptions) -> Result<Self> {
        let (pool, connect) = options.pool_options(url)?;
        Ok(Self {
            pool: pool.connect_lazy_with(connect),
        })
    }

    /// Close every connection, waiting for those in use to be returned.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    pub(crate) fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}

/// What to tell an operator when a connection failed over TLS.
fn tls_hint(e: &sqlx::Error) -> &'static str {
    let text = e.to_string().to_ascii_lowercase();
    if text.contains("tls") || text.contains("ssl") || text.contains("certificate") {
        " — the connection is TLS by default (sslmode=verify-full): a server offering \
         no TLS, or a certificate this host cannot verify, fails here. Set sslrootcert= \
         for a private CA, or choose sslmode explicitly"
    } else {
        ""
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "postgres://u:secret@db.invalid/meter?sslmode=require";

    #[tokio::test]
    async fn zero_connections_is_refused_before_anything_is_opened() {
        let err = Database::connect_lazy_with(URL, &DatabaseOptions::new().max_connections(0))
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
    }

    #[tokio::test]
    async fn a_negative_timeout_is_refused() {
        let err = Database::connect_lazy_with(
            URL,
            &DatabaseOptions::new().acquire_timeout(Duration::seconds(-1)),
        )
        .unwrap_err();
        assert!(err.to_string().contains("acquire_timeout"), "{err}");
    }

    #[tokio::test]
    async fn the_tls_rule_applies_to_every_pool() {
        let err = Database::connect_lazy_with(
            "postgres://u@db.invalid/meter?sslmode=prefer",
            &DatabaseOptions::new(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
    }

    #[tokio::test]
    async fn the_debug_form_carries_no_url() {
        let db = Database::connect_lazy_with(URL, &DatabaseOptions::new()).unwrap();
        assert!(!format!("{db:?}").contains("secret"));
    }
}
