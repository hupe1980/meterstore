//! Making Article 17 erasure possible over an append-only lake.
//!
//! # Why not crypto-shredding
//!
//! The usual answer for an immutable store is to encrypt each subject's data
//! under its own key and destroy the key on request. Regulators accept it: the
//! EDPB (Guidelines 5/2019), the UK ICO and the French CNIL all recognise
//! cryptographic erasure, provided the algorithm is strong, the destruction is
//! irreversible, and it is auditable.
//!
//! It does not work here, for a structural reason rather than a missing feature.
//! Crypto-shredding requires **key granularity aligned to the erasure unit** —
//! one key per data subject. Iceberg's envelope encryption keys data per *file*:
//! a master key in a KMS, key-encryption keys in table metadata, and a data
//! key per file. At metering volume a single Parquet file holds readings for
//! thousands of measuring points, so destroying its key erases all of them.
//! Aligning keys to subjects would mean one file per subject, which at 100k
//! meters is not a table but a directory listing.
//!
//! Encrypting the value column per subject instead would preserve the file
//! layout but destroy everything that makes the cold tier fast: delta encoding
//! needs adjacent values to be numerically close, min/max statistics need
//! comparable values, and bloom filters need stable equality. Ciphertext has
//! none of those properties.
//!
//! # What works instead
//!
//! The personal data in a metering series is not the numbers — it is the *link*
//! between a consumption pattern and a person. Break the link and what remains
//! is a series of quantities attached to an opaque token, which is anonymous
//! data and outside the Regulation's scope (Recital 26).
//!
//! So the lake stores a **pseudonymous reference**, and the mapping from that
//! reference to a natural identifier lives in PostgreSQL — mutable storage where
//! a row can genuinely be deleted. Erasure deletes the mapping row. It is
//! `O(1)`, needs no key management, rewrites nothing, and leaves every
//! analytical property of the lake intact.
//!
//! It is also a stronger position than crypto-shredding: there is no argument to
//! have about whether ciphertext is still personal data, because the linking
//! data is actually gone rather than merely unreadable.
//!
//! # What this requires of the deployment
//!
//! **The pseudonymous reference must be the only link.** A 15-minute
//! consumption series is potentially re-identifiable by singling out — if
//! another system holds the same series against a name, deleting this mapping
//! achieves nothing. Erasure is a property of the whole estate, and this module
//! only guarantees its own part.
//!
//! **Granularity is the deployment's choice.** A reference may stand for a
//! customer, a contract, or an occupancy period at a measuring point. A market
//! location outlives its occupants, so keying by measuring point alone would
//! erase a previous tenant's data along with the requester's.

use sqlx::PgPool;
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::error::{Error, Result};

/// An opaque reference to a data subject, safe to store in the lake.
///
/// Carries no personal data itself: it is meaningful only through a mapping that
/// erasure destroys.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubjectRef(String);

impl SubjectRef {
    /// Wrap an externally generated reference.
    ///
    /// It must carry no personal data — a name, a meter serial or an email
    /// hashed without a secret would all survive erasure as a re-identification
    /// path. A random token is the safe choice.
    pub fn new(reference: impl Into<String>) -> Result<Self> {
        let reference = reference.into();
        if reference.trim().is_empty() {
            return Err(Error::config("subject reference must not be empty"));
        }
        Ok(Self(reference))
    }

    /// The reference as stored.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SubjectRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Proof that an erasure happened.
///
/// Recorded because a regulator may ask, and because "we deleted it" is not
/// evidence. Deliberately holds no natural identifier — that is the thing being
/// erased, and an audit trail that retains it would defeat the exercise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErasureRecord {
    /// The reference whose linkage was destroyed.
    pub subject: SubjectRef,
    /// When it happened.
    pub erased_at: OffsetDateTime,
    /// Why — a request identifier or ticket, not a person's details.
    pub reason: String,
    /// Who performed it.
    pub actor: String,
}

/// The mapping between natural identifiers and pseudonymous references.
///
/// Lives in PostgreSQL rather than the lake because erasure needs storage where
/// deletion is real.
#[derive(Clone)]
pub struct SubjectRegistry {
    pool: PgPool,
    /// Key for the suppression list, if the deployment configured one.
    ///
    /// Not `Debug`-printable — see the manual implementation below.
    erasure_secret: Option<Vec<u8>>,
}

impl std::fmt::Debug for SubjectRegistry {
    /// Redacts the suppression key.
    ///
    /// A registry is a plausible thing to include in a `tracing` field or an
    /// error context, and the key must not reach a log.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubjectRegistry")
            .field("suppression", &self.erasure_secret.is_some())
            .finish_non_exhaustive()
    }
}

impl SubjectRegistry {
    /// Wrap a connection pool, without a suppression list.
    ///
    /// Erasure works; what is missing is the ability to *keep* a subject erased.
    /// Once the mapping is deleted nothing distinguishes an erased identifier
    /// from one never seen, so a pipeline replaying old messages silently
    /// registers a fresh reference and re-links the subject. Use
    /// [`with_erasure_secret`](Self::with_erasure_secret) where replay is
    /// possible, which is every deployment fed by a message broker.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            erasure_secret: None,
        }
    }

    /// Wrap a connection pool and enforce a suppression list.
    ///
    /// Erasure then records a keyed hash of the identifier it erased, and
    /// [`register`](Self::register) refuses anything that matches. That is what
    /// makes the promise in `register`'s documentation true rather than
    /// aspirational.
    ///
    /// # Why a keyed hash rather than the identifier
    ///
    /// Storing the identifier would defeat the erasure it documents. Storing an
    /// *unkeyed* hash would be barely better: meter and market-location
    /// identifiers come from small structured spaces, so anyone with the table
    /// could enumerate candidates and invert it. The key turns the tombstone
    /// into an oracle that answers "was this one erased?" only for someone who
    /// already holds both the identifier and the key, which is the minimum
    /// needed to honour the request.
    ///
    /// Retaining that much is the recognised practice for suppression lists and
    /// sits within Article 17 — you cannot honour "do not process my data again"
    /// without some record of what not to process.
    ///
    /// # Operational note
    ///
    /// The key must outlive every erasure and is not recoverable from the
    /// database. Losing it does not expose anything; it silently disables
    /// suppression, since no future identifier will hash to a stored tombstone.
    pub fn with_erasure_secret(pool: PgPool, secret: &[u8]) -> Result<Self> {
        // A short key makes the oracle brute-forceable, which is the one thing
        // the construction is supposed to prevent.
        if secret.len() < 32 {
            return Err(Error::config(
                "erasure secret must be at least 32 bytes: a shorter key can be \
                 brute-forced, and the suppression list would then leak the \
                 identifiers it exists to forget",
            ));
        }
        Ok(Self {
            pool,
            erasure_secret: Some(secret.to_vec()),
        })
    }

    /// Whether a suppression list is enforced.
    pub fn suppresses_reregistration(&self) -> bool {
        self.erasure_secret.is_some()
    }

    /// Keyed hash of a natural identifier, or `None` without a configured key.
    fn tombstone(&self, natural_id: &str) -> Option<Vec<u8>> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let secret = self.erasure_secret.as_ref()?;
        let mut mac =
            <Hmac<Sha256>>::new_from_slice(secret).expect("HMAC accepts keys of any length");
        mac.update(natural_id.as_bytes());
        Some(mac.finalize().into_bytes().to_vec())
    }

    /// Create the registry's tables.
    ///
    /// Two of them, deliberately. The mapping is deletable; the audit trail is
    /// append-only and outlives what it describes.
    pub async fn create_tables(&self) -> Result<()> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS meterstore_subject_map (
                   subject_ref  TEXT PRIMARY KEY,
                   natural_id   TEXT NOT NULL UNIQUE,
                   registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
               )"#,
        )
        .execute(&self.pool)
        .await
        .map_err(pg)?;

        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS meterstore_erasures (
                   subject_ref     TEXT PRIMARY KEY,
                   erased_at       TIMESTAMPTZ NOT NULL,
                   reason          TEXT NOT NULL,
                   actor           TEXT NOT NULL,
                   -- Keyed hash of the erased identifier. NULL when the
                   -- deployment configured no suppression key, in which case a
                   -- replayed message can re-register the subject.
                   natural_id_hmac BYTEA
               )"#,
        )
        .execute(&self.pool)
        .await
        .map_err(pg)?;

        // Registration checks this on every miss, so it must not be a scan.
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS meterstore_erasures_hmac
                   ON meterstore_erasures (natural_id_hmac)
                WHERE natural_id_hmac IS NOT NULL"#,
        )
        .execute(&self.pool)
        .await
        .map_err(pg)?;

        info!(
            suppression = self.suppresses_reregistration(),
            "subject registry ready"
        );
        Ok(())
    }

    /// Register a natural identifier, returning its pseudonymous reference.
    ///
    /// Idempotent: registering the same identifier twice returns the same
    /// reference, so an ingest path may call it per batch without accumulating
    /// references for one subject.
    ///
    /// Refuses to re-register an identifier whose reference has been erased —
    /// **only when a suppression key is configured**
    /// ([`with_erasure_secret`](Self::with_erasure_secret)). Re-registration
    /// resurrects the link erasure destroyed, and it almost always means a stale
    /// pipeline is replaying data that should be dropped.
    ///
    /// Without a key the check is not merely disabled, it is impossible: erasure
    /// deletes the mapping, so nothing remains to recognise the identifier by.
    pub async fn register(&self, natural_id: &str) -> Result<SubjectRef> {
        if natural_id.trim().is_empty() {
            return Err(Error::config("natural identifier must not be empty"));
        }

        if let Some(existing) = self.lookup(natural_id).await? {
            return Ok(existing);
        }

        // Checked only after the lookup misses: a live mapping means the subject
        // was never erased, and the common path should not pay for the check.
        if let Some(tombstone) = self.tombstone(natural_id) {
            let suppressed = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM meterstore_erasures WHERE natural_id_hmac = $1)",
            )
            .bind(&tombstone)
            .fetch_one(&self.pool)
            .await
            .map_err(pg)?;

            if suppressed {
                warn!("registration refused for an erased identifier");
                return Err(Error::config(
                    "this identifier was erased and must not be re-registered: \
                     registering it would rebuild the link Article 17 destroyed. \
                     A subject who genuinely returns should arrive under a new \
                     identifier; if the erasure itself was mistaken, lift it \
                     explicitly with `lift_suppression`",
                ));
            }
        }

        let reference = new_reference();
        let inserted = sqlx::query_scalar::<_, String>(
            r#"INSERT INTO meterstore_subject_map (subject_ref, natural_id)
               VALUES ($1, $2)
               ON CONFLICT (natural_id) DO UPDATE SET natural_id = EXCLUDED.natural_id
               RETURNING subject_ref"#,
        )
        .bind(&reference)
        .bind(natural_id)
        .fetch_one(&self.pool)
        .await
        .map_err(pg)?;

        SubjectRef::new(inserted)
    }

    /// The reference for a natural identifier, if one is registered.
    pub async fn lookup(&self, natural_id: &str) -> Result<Option<SubjectRef>> {
        let found = sqlx::query_scalar::<_, String>(
            "SELECT subject_ref FROM meterstore_subject_map WHERE natural_id = $1",
        )
        .bind(natural_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(pg)?;

        found.map(SubjectRef::new).transpose()
    }

    /// Resolve a reference back to its natural identifier.
    ///
    /// `None` once erased — which is the whole point.
    pub async fn resolve(&self, subject: &SubjectRef) -> Result<Option<String>> {
        sqlx::query_scalar::<_, String>(
            "SELECT natural_id FROM meterstore_subject_map WHERE subject_ref = $1",
        )
        .bind(subject.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(pg)
    }

    /// Destroy the link between a reference and its subject.
    ///
    /// The lake keeps every reading. What it loses is any way to attribute them
    /// to a person, which is what Article 17 asks for and what leaves the
    /// remaining series anonymous.
    ///
    /// Irreversible by construction: the mapping row is deleted rather than
    /// flagged, so there is no recovery path to disclose. The audit row that
    /// replaces it records that erasure happened without recording whom it
    /// concerned.
    pub async fn erase(
        &self,
        subject: &SubjectRef,
        reason: &str,
        actor: &str,
        now: OffsetDateTime,
    ) -> Result<ErasureRecord> {
        let mut tx = self.pool.begin().await.map_err(pg)?;
        let record = self.erase_in(&mut tx, subject, reason, actor, now).await?;
        tx.commit().await.map_err(pg)?;
        Ok(record)
    }

    /// Erase inside a transaction the **caller** owns.
    ///
    /// [`erase`](Self::erase) opens and commits its own, which is right when
    /// destroying the mapping is the whole operation. It is wrong when it is one
    /// step of a larger cascade: an Article 17 request usually reaches an
    /// application's own tables too — billing periods, quality assessments,
    /// substitute-value logs — and those must succeed or fail *together* with the
    /// mapping. Two transactions cannot give that, and the failure mode is the
    /// worst kind: a subject reported as erased whose derived rows survived.
    ///
    /// Pass a `&mut Transaction` (which derefs to a connection) to enclose this
    /// step:
    ///
    /// ```no_run
    /// # async fn example(
    /// #     pool: &sqlx::PgPool,
    /// #     registry: &meterstore::SubjectRegistry,
    /// #     subject: &meterstore::SubjectRef,
    /// # ) -> meterstore::Result<()> {
    /// let mut tx = pool.begin().await.unwrap();
    /// registry
    ///     .erase_in(&mut tx, subject, "Art. 17 request", "dpo", now())
    ///     .await?;
    /// sqlx::query("DELETE FROM billing_periods WHERE subject_ref = $1")
    ///     .bind(subject.as_str())
    ///     .execute(&mut *tx)
    ///     .await
    ///     .unwrap();
    /// tx.commit().await.unwrap();
    /// # Ok(())
    /// # }
    /// # fn now() -> time::OffsetDateTime { time::OffsetDateTime::UNIX_EPOCH }
    /// ```
    ///
    /// The registry's tables must live in the same database as the caller's for
    /// this to mean anything — which they do, because the registry is
    /// constructed from a `PgPool` the application already owns.
    ///
    /// **Cold-tier exclusion is not part of this transaction and cannot be.** It
    /// is not a write at all: erasure destroys the *mapping*, which leaves every
    /// archived row unattributable wherever it sits (§12.4). There is nothing in
    /// object storage to roll back, so sequencing is not a concern.
    pub async fn erase_in(
        &self,
        conn: &mut sqlx::PgConnection,
        subject: &SubjectRef,
        reason: &str,
        actor: &str,
        now: OffsetDateTime,
    ) -> Result<ErasureRecord> {
        if reason.trim().is_empty() {
            return Err(Error::config("erasure needs a reason for the audit trail"));
        }

        let tx = conn;

        // The tombstone has to be computed before the delete, because after it
        // the identifier is gone — which is the point. `FOR UPDATE` holds the
        // row so a concurrent `register` cannot interleave between reading the
        // identifier and destroying it.
        let natural_id = sqlx::query_scalar::<_, String>(
            "SELECT natural_id FROM meterstore_subject_map WHERE subject_ref = $1 FOR UPDATE",
        )
        .bind(subject.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(pg)?;

        let tombstone = natural_id.as_deref().and_then(|id| self.tombstone(id));

        // Delete and audit atomically: an audit row without the deletion would
        // claim an erasure that did not happen, and a deletion without the audit
        // row would leave it unprovable.
        let deleted = sqlx::query("DELETE FROM meterstore_subject_map WHERE subject_ref = $1")
            .bind(subject.as_str())
            .execute(&mut *tx)
            .await
            .map_err(pg)?
            .rows_affected();

        // A repeat request keeps the first erasure's timestamp — that is when
        // the linkage actually died — but must not blank an existing tombstone,
        // which would quietly re-open re-registration.
        sqlx::query(
            r#"INSERT INTO meterstore_erasures
                   (subject_ref, erased_at, reason, actor, natural_id_hmac)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (subject_ref) DO UPDATE
                   SET natural_id_hmac =
                       COALESCE(meterstore_erasures.natural_id_hmac, EXCLUDED.natural_id_hmac)"#,
        )
        .bind(subject.as_str())
        .bind(now)
        .bind(reason)
        .bind(actor)
        .bind(tombstone.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(pg)?;

        if deleted == 0 {
            // Already erased, or never registered. Recording it either way keeps
            // a repeated request auditable rather than silently successful.
            warn!(%subject, "erasure requested for an unmapped reference");
        } else {
            info!(%subject, actor, "subject linkage destroyed");
        }

        Ok(ErasureRecord {
            subject: subject.clone(),
            erased_at: now,
            reason: reason.to_string(),
            actor: actor.to_string(),
        })
    }

    /// Whether an identifier is on the suppression list.
    ///
    /// Always `false` without a configured key, because there is then nothing to
    /// check against — not because the identifier was never erased.
    pub async fn is_suppressed(&self, natural_id: &str) -> Result<bool> {
        let Some(tombstone) = self.tombstone(natural_id) else {
            return Ok(false);
        };
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM meterstore_erasures WHERE natural_id_hmac = $1)",
        )
        .bind(&tombstone)
        .fetch_one(&self.pool)
        .await
        .map_err(pg)
    }

    /// Remove an identifier from the suppression list.
    ///
    /// For the case the suppression list would otherwise make unrecoverable: an
    /// erasure carried out against the wrong subject. Without this, one mistaken
    /// request permanently locks a real customer out of the system.
    ///
    /// It does **not** undo the erasure. The mapping is gone and the readings
    /// stay unattributable; what this restores is the ability to register the
    /// identifier again, under a **new** reference. Deliberately so — a lifted
    /// suppression must not silently re-link the old history.
    ///
    /// The audit row survives, so the sequence erase → lift → re-register stays
    /// visible to anyone reviewing what happened.
    pub async fn lift_suppression(
        &self,
        natural_id: &str,
        reason: &str,
        actor: &str,
    ) -> Result<bool> {
        if reason.trim().is_empty() {
            return Err(Error::config(
                "lifting a suppression needs a reason: it reverses a compliance \
                 action and must not be an anonymous edit",
            ));
        }
        let Some(tombstone) = self.tombstone(natural_id) else {
            return Err(Error::config(
                "no suppression key is configured, so there is no suppression to lift",
            ));
        };

        let lifted = sqlx::query(
            "UPDATE meterstore_erasures SET natural_id_hmac = NULL WHERE natural_id_hmac = $1",
        )
        .bind(&tombstone)
        .execute(&self.pool)
        .await
        .map_err(pg)?
        .rows_affected();

        if lifted > 0 {
            warn!(actor, reason, "erasure suppression lifted");
        }
        Ok(lifted > 0)
    }

    /// Whether a reference has been erased.
    pub async fn is_erased(&self, subject: &SubjectRef) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM meterstore_erasures WHERE subject_ref = $1)",
        )
        .bind(subject.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(pg)
    }

    /// The audit trail, most recent first.
    pub async fn erasures(&self, limit: i64) -> Result<Vec<ErasureRecord>> {
        let rows = sqlx::query_as::<_, (String, OffsetDateTime, String, String)>(
            r#"SELECT subject_ref, erased_at, reason, actor
                 FROM meterstore_erasures
                ORDER BY erased_at DESC
                LIMIT $1"#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(pg)?;

        rows.into_iter()
            .map(|(subject, erased_at, reason, actor)| {
                Ok(ErasureRecord {
                    subject: SubjectRef::new(subject)?,
                    erased_at,
                    reason,
                    actor,
                })
            })
            .collect()
    }
}

/// A random reference with no derivation from the subject.
///
/// Deriving it — hashing a meter serial, say — would leave a re-identification
/// path that survives erasure, because anyone holding the serial could recompute
/// the reference and find the readings again.
///
/// 128 bits from the operating system's CSPRNG. `std`'s `RandomState` would be
/// the convenient choice and is the wrong one: it is seeded once per thread and
/// then *incremented*, and its documentation is explicit that it is not
/// cryptographically secure. It exists to make `HashMap` collision attacks hard,
/// which is a weaker property than the one erasure needs.
fn new_reference() -> String {
    let mut bytes = [0u8; 16];
    // The OS entropy source failing is not a recoverable condition — continuing
    // would mean minting predictable references and calling them pseudonyms.
    getrandom::fill(&mut bytes).expect("OS entropy source unavailable");
    format!("sub_{}", hex(&bytes))
}

/// Lowercase hex, without pulling in a dependency for sixteen characters.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn pg(e: sqlx::Error) -> Error {
    Error::Storage(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reference_must_not_be_empty() {
        assert!(SubjectRef::new("").is_err());
        assert!(SubjectRef::new("   ").is_err());
        assert!(SubjectRef::new("sub_abc").is_ok());
    }

    #[test]
    fn generated_references_do_not_repeat() {
        let refs: std::collections::HashSet<_> = (0..1_000).map(|_| new_reference()).collect();
        assert_eq!(refs.len(), 1_000, "references must be unique");
    }

    #[test]
    fn a_generated_reference_is_not_derived_from_anything() {
        // Two calls must differ, or the reference is a function of its input and
        // erasure could be undone by recomputing it.
        assert_ne!(new_reference(), new_reference());
    }

    #[test]
    fn an_erasure_record_carries_no_natural_identifier() {
        // The audit trail outlives the mapping, so anything personal in it would
        // survive the erasure it documents.
        let record = ErasureRecord {
            subject: SubjectRef::new("sub_abc").unwrap(),
            erased_at: OffsetDateTime::UNIX_EPOCH,
            reason: "DSAR-2026-0042".to_string(),
            actor: "privacy-team".to_string(),
        };
        let rendered = format!("{record:?}");
        assert!(rendered.contains("sub_abc"));
        assert!(rendered.contains("DSAR-2026-0042"));
    }
}
