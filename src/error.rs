//! Error types for MeterStore.
//!
//! One error enum for the whole crate, and it is `#[non_exhaustive]`: callers
//! match on variants and never parse strings, so a new failure mode is an added
//! arm rather than a broken `if err.to_string().contains(..)`.

use std::result::Result as StdResult;

/// Convenience alias used throughout the crate.
pub type Result<T> = StdResult<T, Error>;

/// Everything MeterStore can fail with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A value could not be represented in the storage encoding.
    ///
    /// Raised at encode time, never mid-stream: an unrepresentable value is a
    /// programming or configuration error, not a data condition.
    #[error("cannot encode {what}: {reason}")]
    Encode {
        /// What was being encoded (column, type, or field name).
        what: String,
        /// Why it could not be represented.
        reason: String,
    },

    /// Stored data could not be decoded back into a `metering` type.
    ///
    /// Always a corruption or version-skew signal — the encoder is total over
    /// its input domain, so a decode failure means the bytes did not come from
    /// a compatible writer.
    #[error("cannot decode {what}: {reason}")]
    Decode {
        /// What was being decoded.
        what: String,
        /// Why it failed.
        reason: String,
    },

    /// An invariant the store's answers rest on does not hold.
    ///
    /// Chiefly the **tiering** invariant: a row's interval start alone decides
    /// its tier, so PostgreSQL holds exactly `from >= watermark` and Iceberg
    /// exactly `from < watermark`. Also the smaller ones that make a *resolved*
    /// reading well defined — one version scope per reading, above all, since two
    /// incomparable scopes leave two winners and double every sum over them.
    ///
    /// **Query results may be wrong while this is true**, and no retry changes
    /// that: it describes the state of the store, not the request that found it.
    ///
    /// It is deliberately **not** what a refused *delivery* raises — that is
    /// [`IntegrityViolation`](Self::IntegrityViolation), which means the store
    /// stopped something from becoming true. A caller that pages on this one
    /// should not be woken by a producer sending a bad row.
    ///
    /// **Alert on the gauge, not on this.** `meterstore.tiering.invariant_violations`
    /// and the `invariant_violations` column of `system.tables` are counted from
    /// the data, so they report the condition whether or not anything raised it.
    #[error("invariant violated for table {table}: {detail}")]
    InvariantViolated {
        /// Table the violation was detected on.
        table: String,
        /// Human-readable description of what was inconsistent.
        detail: String,
    },

    /// Configuration failed validation.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// A table is halted because its schema changed in a way that cannot be
    /// applied safely.
    ///
    /// The honest response to an incompatible change (§11). Rows already written
    /// would mean something different from rows about to be written, and no care
    /// downstream recovers that — so the table stops, its watermark freezes, and
    /// an operator resolves it. Other tables keep running.
    #[error("table {table} is quarantined: {detail}")]
    Quarantined {
        /// The halted table.
        table: String,
        /// Every change that could not be applied.
        detail: String,
    },

    /// A version comparison was attempted across incompatible scopes.
    ///
    /// MSCONS versions are only ordered within a (network operator, month)
    /// scope; comparing across scopes is meaningless.
    #[error("cannot compare versions across scopes: {left} vs {right}")]
    VersionScopeMismatch {
        /// Left-hand scope.
        left: String,
        /// Right-hand scope.
        right: String,
    },

    /// A statement gave up waiting for a lock rather than joining the queue
    /// behind it.
    ///
    /// **Nothing was changed**, so it is safe to retry: the archival loop reports
    /// it as [`deferred`](field@crate::ArchivalOutcome::deferred) rather than as a
    /// failure. It means something else held a conflicting lock for longer than
    /// [`ddl_lock_timeout`](crate::PostgresHot::ddl_lock_timeout) — usually a long
    /// analytical query or a session idle in a transaction.
    #[error(
        "{operation} on {relation} gave up after {waited_ms} ms waiting for a lock; \
         nothing was changed. Look for a long-running query or a session idle in a \
         transaction in pg_stat_activity"
    )]
    LockTimeout {
        /// The relation the statement was waiting on.
        relation: String,
        /// What was being attempted, for the log line.
        operation: String,
        /// How long it waited before giving up.
        waited_ms: u64,
    },

    /// A write was refused by a rule that exists to stop a wrong number:
    /// overlapping spans within one version, two network operators for one
    /// reading, a non-canonical OBIS code, a value restated under an existing
    /// version.
    ///
    /// Separate from [`Storage`](Self::Storage) because the two want opposite
    /// responses — a storage failure is retried, and this never succeeds on a
    /// retry, since the delivery itself has to change.
    /// [`constraint`](Self::IntegrityViolation::constraint) names the rule where
    /// one is reported, so a caller can branch without parsing the message.
    #[error("{table} refused a write: {detail}")]
    IntegrityViolation {
        /// The table the write was aimed at.
        table: String,
        /// The constraint's name, where the backend reported one.
        constraint: Option<String>,
        /// The backend's own description.
        detail: String,
    },

    /// A storage backend failed: a connection that dropped, a disk that filled, a
    /// catalogue that timed out. Retrying is the right default for all of it.
    ///
    /// Wraps the backend's own message rather than its error type, so adding a
    /// backend does not widen this enum or leak a driver type into the public
    /// API. The two conditions a caller reliably wants to tell apart are their own
    /// variants above.
    #[error("storage error: {0}")]
    Storage(String),

    /// A DataFusion-level failure.
    ///
    /// Kept as DataFusion's own error rather than flattened into a string, because
    /// it is the one variant that arrives from **caller-supplied SQL**: a service
    /// answers a statement that will not plan with a 400 and a warehouse it cannot
    /// reach with a 503, and cannot tell them apart from a message.
    /// [`is_retryable`](Self::is_retryable) splits it the same way.
    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::common::DataFusionError),

    /// Arrow-level failure (schema mismatch, array construction).
    #[error("arrow error: {0}")]
    Arrow(#[from] crate::arrow::error::ArrowError),

    /// Serialization failure when encoding a structured column payload.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

impl Error {
    /// Construct an [`Error::Encode`].
    pub fn encode(what: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Encode {
            what: what.into(),
            reason: reason.into(),
        }
    }

    /// Construct an [`Error::Decode`].
    pub fn decode(what: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Decode {
            what: what.into(),
            reason: reason.into(),
        }
    }

    /// Construct an [`Error::Config`].
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Whether retrying the same operation could succeed.
    ///
    /// True for the transient conditions — a lost connection, a lock the statement
    /// declined to queue for. False for everything describing the *input*, where a
    /// retry loops forever on a message that will never change, and for
    /// [`InvariantViolated`](Self::InvariantViolated) and
    /// [`Quarantined`](Self::Quarantined), where retrying past them is how a wrong
    /// answer gets served for a week.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::LockTimeout { .. } | Self::Storage(_) => true,
            // DataFusion is both the query *planner* and the thing that reads
            // the object store, so its errors fall on both sides. A statement
            // that will not plan never plans on a retry; a scan that could not
            // reach the warehouse might.
            Self::DataFusion(e) => matches!(
                e,
                datafusion::common::DataFusionError::ObjectStore(_)
                    | datafusion::common::DataFusionError::IoError(_)
                    | datafusion::common::DataFusionError::ResourcesExhausted(_)
                    | datafusion::common::DataFusionError::External(_)
            ),
            _ => false,
        }
    }
}

/// Hide everything but the shape of a connection URL or a secret.
///
/// A `Debug` impl is the one thing that turns a credential into a log line, and
/// this crate holds several: the hot tier's connection URL, the Iceberg
/// catalogue's (usually the same database, so the same password), and an S3
/// secret key. Configuration is what a service dumps at startup and what an error
/// context carries, so each of those types hand-writes `Debug` and redacts
/// through here.
///
/// The shape is kept because it is the useful half: `postgresql://<redacted>`
/// tells an operator which store failed to connect without telling a log
/// aggregator the password.
pub(crate) fn redacted(value: &str) -> String {
    if value.is_empty() {
        return String::new();
    }
    match value.split_once("://") {
        Some((scheme, _)) => format!("{scheme}://<redacted>"),
        None => "<redacted>".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_splits_the_transient_from_the_wrong() {
        // A supervisor loops on the first kind and gives up on the second, and
        // getting it backwards is either a hot loop on a message that will never
        // change or a delivery dropped on a blip.
        assert!(Error::Storage("connection reset".into()).is_retryable());
        assert!(
            Error::LockTimeout {
                relation: "readings_versions".into(),
                operation: "detach partition".into(),
                waited_ms: 3_000,
            }
            .is_retryable()
        );

        assert!(!Error::config("settlement_lag must be at least one archival_step").is_retryable());
        assert!(!Error::encode("version", "too many digits").is_retryable());
        assert!(
            !Error::IntegrityViolation {
                table: "readings_versions".into(),
                constraint: Some("version_identifies_one_assertion".into()),
                detail: "restated".into(),
            }
            .is_retryable()
        );

        // Neither of the two an operator has to look at. Retrying past them is
        // how a wrong answer gets served for a week.
        assert!(
            !Error::InvariantViolated {
                table: "readings_versions".into(),
                detail: "rows below the watermark".into(),
            }
            .is_retryable()
        );
        assert!(
            !Error::Quarantined {
                table: "readings_versions".into(),
                detail: "a required column was dropped".into(),
            }
            .is_retryable()
        );
    }

    #[test]
    fn a_statement_that_will_not_plan_is_not_retried() {
        // DataFusion is both the planner and the thing that reads the object
        // store, so its errors fall on both sides — and the planning half is the
        // one that arrives from caller-supplied SQL.
        use datafusion::common::DataFusionError;

        let unplannable = Error::DataFusion(DataFusionError::Plan("no such table".into()));
        assert!(!unplannable.is_retryable());

        let unreachable = Error::DataFusion(DataFusionError::ResourcesExhausted(
            "the warehouse is not answering".into(),
        ));
        assert!(unreachable.is_retryable());
    }

    #[test]
    fn a_connection_url_keeps_its_scheme_and_loses_its_secret() {
        // The useful half of a redacted value: which store failed to connect,
        // without the password reaching a log aggregator.
        assert_eq!(
            redacted("postgresql://u:p@host/db"),
            "postgresql://<redacted>"
        );
        assert_eq!(redacted("AKIAsecret"), "<redacted>");
        assert_eq!(redacted(""), "");
    }
}
