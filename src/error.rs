//! Error types for MeterStore.
//!
//! One error enum for the whole crate. Callers match on variants; they never
//! parse strings. See gap 6.

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

    /// The tiering invariant does not hold.
    ///
    /// The invariant is that a row's interval start alone decides its tier:
    /// PostgreSQL holds exactly `from >= watermark`, Iceberg exactly
    /// `from < watermark`.
    ///
    /// Query results may be wrong while this is true. Surfaced as
    /// `meterstore_invariant_violations_total` and alerted on.
    #[error("tiering invariant violated for table {table}: {detail}")]
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

    /// A storage backend failed.
    ///
    /// Wraps the backend's own message rather than its error type, so adding a
    /// backend does not widen this enum or leak a driver type into the public
    /// API.
    #[error("storage error: {0}")]
    Storage(String),

    /// A DataFusion-level failure, most often a scalar/array conversion.
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
}
