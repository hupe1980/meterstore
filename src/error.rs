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

    /// The tier boundary moved while this request was in flight.
    ///
    /// A write reached a partition archival had just detached, or a plan was
    /// cut at a boundary reclamation passed before it could be pinned. Nothing
    /// was written and nothing is wrong: the same request, made again, reads the
    /// new boundary and succeeds.
    #[error("table {table}: the tier boundary moved during the request; retry: {detail}")]
    BoundaryMoved {
        /// Table whose boundary moved.
        table: String,
        /// What was in flight.
        detail: String,
    },

    /// Another writer committed this archival window first.
    ///
    /// Not a fault: two archivers for one window is what a lost lease allows,
    /// and the commit guard is what stopped the second. The archiver reports it
    /// as a contended run; the detached partition is reclaimed on a later one.
    #[error("table {table}: the window [{from}, {to}) is already archived by another writer")]
    AlreadyArchived {
        /// Table the window belongs to.
        table: String,
        /// The window's start.
        from: time::OffsetDateTime,
        /// The window's exclusive end.
        to: time::OffsetDateTime,
    },

    /// An erasure entered through a reference the retention sweep expired.
    ///
    /// The sweep destroyed the only link from that reference to a person, which
    /// is what `[MsbG § 60 Abs. 6]` requires — so the store cannot tell whose
    /// other years are still linked, and erasing the reference alone would
    /// report a request honoured while they stay attributable. Enter the request
    /// by the identifier it names instead.
    #[error(
        "subject reference {reference} expired by the retention sweep at {expired_at}: the \
         identifier behind it is gone by design, so its other years cannot be found \
         through it. Erase by the identifier the request names (erase_all, \
         MeterCatalog::erase_subject_by_id)"
    )]
    SubjectUnresolvable {
        /// The reference the request named.
        reference: String,
        /// When the sweep expired it.
        expired_at: time::OffsetDateTime,
    },

    /// Configuration failed validation.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// A table is halted because its schema changed in a way that cannot be
    /// applied safely.
    ///
    /// The honest response to an incompatible change. Rows already written
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
    DataFusion(datafusion::common::DataFusionError),

    /// Arrow-level failure (schema mismatch, array construction).
    #[error("arrow error: {0}")]
    Arrow(crate::arrow::error::ArrowError),

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
            Self::LockTimeout { .. } | Self::Storage(_) | Self::BoundaryMoved { .. } => true,
            // DataFusion is both the query *planner* and the thing that reads
            // the object store, so its errors fall on both sides. A statement
            // that will not plan never plans on a retry; a scan that could not
            // reach the warehouse might.
            // An error of this crate's raised inside the engine never arrives
            // here: the conversion recovers it with its own variant. What is
            // left is the engine's own, and a foreign `External` says nothing
            // about whether a retry helps.
            Self::DataFusion(e) => matches!(
                e,
                datafusion::common::DataFusionError::ObjectStore(_)
                    | datafusion::common::DataFusionError::IoError(_)
                    | datafusion::common::DataFusionError::ResourcesExhausted(_)
            ),
            _ => false,
        }
    }
}

/// The engine's error, with this crate's own recovered from inside it.
///
/// A store error raised mid-scan crosses DataFusion as `External`, and a hash
/// repartition re-sends it as `Shared`; a plan wraps it in `Context`; Arrow
/// carries it as `ExternalError`. Converted as an opaque `DataFusion` error, an
/// `InvariantViolated` would read as retryable and a `match` on the variant
/// would miss it — so the variant is recovered, and with it its meaning.
impl From<datafusion::common::DataFusionError> for Error {
    fn from(e: datafusion::common::DataFusionError) -> Self {
        use datafusion::common::DataFusionError as Df;
        match e {
            Df::External(inner) => match inner.downcast::<Error>() {
                Ok(ours) => *ours,
                Err(other) => Self::DataFusion(Df::External(other)),
            },
            Df::Context(context, inner) => match Self::from(*inner) {
                Self::DataFusion(df) => Self::DataFusion(Df::Context(context, Box::new(df))),
                ours => ours,
            },
            Df::Shared(shared) => match std::sync::Arc::try_unwrap(shared) {
                Ok(inner) => Self::from(inner),
                Err(shared) => match ours_in(&shared) {
                    Some(ours) => ours.reissue(),
                    None => Self::DataFusion(Df::Shared(shared)),
                },
            },
            Df::ArrowError(arrow, backtrace) => match *arrow {
                crate::arrow::error::ArrowError::ExternalError(inner) => {
                    match inner.downcast::<Error>() {
                        Ok(ours) => *ours,
                        Err(other) => Self::DataFusion(Df::ArrowError(
                            Box::new(crate::arrow::error::ArrowError::ExternalError(other)),
                            backtrace,
                        )),
                    }
                }
                arrow => Self::DataFusion(Df::ArrowError(Box::new(arrow), backtrace)),
            },
            other => Self::DataFusion(other),
        }
    }
}

/// Arrow's error, with this crate's own recovered from its external slot — the
/// same recovery as for an engine error, for a stream that surfaces Arrow's
/// type directly.
impl From<crate::arrow::error::ArrowError> for Error {
    fn from(e: crate::arrow::error::ArrowError) -> Self {
        match e {
            crate::arrow::error::ArrowError::ExternalError(inner) => {
                match inner.downcast::<Error>() {
                    Ok(ours) => *ours,
                    Err(other) => {
                        Self::Arrow(crate::arrow::error::ArrowError::ExternalError(other))
                    }
                }
            }
            other => Self::Arrow(other),
        }
    }
}

/// This crate's error inside an engine error held by reference.
fn ours_in(e: &datafusion::common::DataFusionError) -> Option<&Error> {
    use datafusion::common::DataFusionError as Df;
    match e {
        Df::External(inner) => inner.downcast_ref::<Error>(),
        Df::Context(_, inner) => ours_in(inner),
        Df::Shared(inner) => ours_in(inner),
        Df::ArrowError(arrow, _) => match arrow.as_ref() {
            crate::arrow::error::ArrowError::ExternalError(inner) => inner.downcast_ref::<Error>(),
            _ => None,
        },
        _ => None,
    }
}

impl Error {
    /// A plan, scan or write that met the tiering boundary mid-move — the one
    /// way this crate raises [`Error::BoundaryMoved`].
    ///
    /// It counts the event on `meterstore.tiering.boundary_moved`, by `at`
    /// (`plan`, `scan`, `write`), so the instrument cannot miss a site: a
    /// struct literal elsewhere would raise the error without the count, and
    /// a test refuses one.
    pub(crate) fn boundary_moved(
        table: impl Into<String>,
        at: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        let table = table.into();
        crate::observe::metrics()
            .boundary_moved
            .add(1, &crate::observe::boundary_moved_at(&table, at));
        Self::BoundaryMoved {
            table,
            detail: detail.into(),
        }
    }

    /// [`boundary_moved`](Self::boundary_moved) for the end of a retry loop
    /// whose every round was already counted: the boundary was met once per
    /// round, and the failure that ends them is not another meeting.
    pub(crate) fn boundary_moved_exhausted(
        table: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self::BoundaryMoved {
            table: table.into(),
            detail: detail.into(),
        }
    }

    /// A copy of an error shared by several engine partitions.
    ///
    /// Every variant a store raises mid-scan carries owned text and is copied
    /// exactly; the wrappers around foreign errors keep their message.
    fn reissue(&self) -> Self {
        match self {
            Self::Encode { what, reason } => Self::Encode {
                what: what.clone(),
                reason: reason.clone(),
            },
            Self::Decode { what, reason } => Self::Decode {
                what: what.clone(),
                reason: reason.clone(),
            },
            Self::InvariantViolated { table, detail } => Self::InvariantViolated {
                table: table.clone(),
                detail: detail.clone(),
            },
            Self::Config(m) => Self::Config(m.clone()),
            Self::Quarantined { table, detail } => Self::Quarantined {
                table: table.clone(),
                detail: detail.clone(),
            },
            Self::VersionScopeMismatch { left, right } => Self::VersionScopeMismatch {
                left: left.clone(),
                right: right.clone(),
            },
            Self::LockTimeout {
                relation,
                operation,
                waited_ms,
            } => Self::LockTimeout {
                relation: relation.clone(),
                operation: operation.clone(),
                waited_ms: *waited_ms,
            },
            Self::IntegrityViolation {
                table,
                constraint,
                detail,
            } => Self::IntegrityViolation {
                table: table.clone(),
                constraint: constraint.clone(),
                detail: detail.clone(),
            },
            Self::Storage(m) => Self::Storage(m.clone()),
            Self::BoundaryMoved { table, detail } => Self::BoundaryMoved {
                table: table.clone(),
                detail: detail.clone(),
            },
            Self::AlreadyArchived { table, from, to } => Self::AlreadyArchived {
                table: table.clone(),
                from: *from,
                to: *to,
            },
            Self::SubjectUnresolvable {
                reference,
                expired_at,
            } => Self::SubjectUnresolvable {
                reference: reference.clone(),
                expired_at: *expired_at,
            },
            // A wrapper around a foreign error cannot be copied; its message
            // can, and so can whether a retry helps — a deterministic engine
            // error must not come back as a retryable storage failure.
            other if other.is_retryable() => Self::Storage(other.to_string()),
            other => Self::DataFusion(datafusion::common::DataFusionError::Execution(
                other.to_string(),
            )),
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

    fn violated() -> Error {
        Error::InvariantViolated {
            table: "readings".into(),
            detail: "rows below the watermark".into(),
        }
    }

    #[test]
    fn an_error_raised_inside_the_engine_keeps_its_variant() {
        // Every wrapper DataFusion puts around an error a scan raised: the plan's
        // context, a repartition's shared copy, Arrow's external slot. Lost in
        // any of them, an `InvariantViolated` reads as a retryable engine error.
        use datafusion::common::DataFusionError as Df;
        let external = || Df::External(Box::new(violated()));
        let wrapped = [
            external(),
            Df::Context("scan".into(), Box::new(external())),
            Df::Shared(std::sync::Arc::new(external())),
            Df::ArrowError(
                Box::new(crate::arrow::error::ArrowError::ExternalError(Box::new(
                    violated(),
                ))),
                None,
            ),
        ];
        for e in wrapped {
            let shown = format!("{e:?}");
            let err = Error::from(e);
            assert!(
                matches!(err, Error::InvariantViolated { .. }) && !err.is_retryable(),
                "{shown} arrived as {err:?}"
            );
        }

        // A copy still held elsewhere is reissued rather than unwrapped.
        let shared = std::sync::Arc::new(external());
        let _other_partition = std::sync::Arc::clone(&shared);
        let err = Error::from(Df::Shared(shared));
        assert!(matches!(err, Error::InvariantViolated { .. }), "{err:?}");

        // A shared copy of a store error wrapping something uncopyable keeps
        // whether a retry helps.
        let deterministic = Error::DataFusion(Df::Plan("no such column".into()));
        let shared = std::sync::Arc::new(Df::External(Box::new(deterministic)));
        let _other_partition = std::sync::Arc::clone(&shared);
        assert!(!Error::from(Df::Shared(shared)).is_retryable());

        // Arrow's own error carries ours the same way.
        let err = Error::from(crate::arrow::error::ArrowError::ExternalError(Box::new(
            violated(),
        )));
        assert!(matches!(err, Error::InvariantViolated { .. }), "{err:?}");

        // A foreign error in the external slot stays the engine's, and is not
        // presumed transient.
        let foreign = Error::from(Df::External("some udf failed".into()));
        assert!(matches!(foreign, Error::DataFusion(_)) && !foreign.is_retryable());
    }

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
