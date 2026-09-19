//! Making Article 17 erasure possible over an append-only lake.
//!
//! # Pseudonymisation, not crypto-shredding
//!
//! The personal data in a metering series is not the numbers — it is the *link*
//! between a consumption pattern and a person. Break the link and what remains is
//! a series of quantities against an opaque token, which is anonymous data and
//! outside the Regulation's scope (Recital 26).
//!
//! So the lake stores a **pseudonymous reference**, and the mapping from it to a
//! natural identifier lives in PostgreSQL — mutable storage where a row can
//! genuinely be deleted. Erasure deletes that row: `O(1)`, no key management,
//! nothing rewritten, every analytical property of the lake intact.
//!
//! Crypto-shredding is the usual answer for an immutable store and does not fit
//! this one: it needs a key per data subject, while Iceberg's envelope encryption
//! keys per *file*, and at metering volume one file holds thousands of measuring
//! points. The full argument is on the
//! [privacy page](https://hupe1980.github.io/meterstore/docs/privacy/).
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

use metering::interval::Sparte;
use sqlx::PgPool;
use time::OffsetDateTime;
use tracing::{info, warn};
use zeroize::Zeroizing;

use crate::error::{Error, Result};

/// An opaque reference to a data subject **in one retention epoch**, safe to
/// store in the lake.
///
/// Carries no personal data itself: it is meaningful only through a mapping that
/// erasure destroys.
///
/// # Why a year is part of it
///
/// § 60 Abs. 6 MsbG comes due per *value*, so the unit of erasure is
/// `(subject, collection year)`: one reference covering a whole history could
/// only either orphan readings still inside their period or keep decade-old ones
/// attributable. A sweep therefore erases epochs, consults no readings, and comes
/// due on the calendar.
///
/// The spelling is `s<year>_<32 hex>`. The year is not secret — a row's `from`
/// gives it away — and the entropy is the second half; what the prefix buys is
/// that a reference can be checked against the row it is written to without a
/// round trip, so one from the wrong year is refused at the write rather than
/// quietly defeating the sweep.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubjectRef(String);

/// How a reference spells its epoch.
const EPOCH_PREFIX: char = 's';

/// The fewest characters a reference's token may have.
///
/// 128 bits is the entropy [`SubjectRegistry::register`] mints with, and 22 is
/// how many characters that takes in the densest encoding anyone spells an
/// identifier in (base64url). Hex takes 32 and an RFC 4122 UUID 36, so both clear
/// it comfortably.
///
/// A floor on the **shape**, not a proof of unpredictability — nothing can tell
/// 128 random bits from a padded counter. It rules out the customer number or
/// meter serial a pipeline substitutes when it has no reference to hand, which
/// resolves cleanly, looks like a pseudonym in every column that stores it, and
/// is a re-identification path that survives erasure.
pub const MIN_REFERENCE_TOKEN_CHARS: usize = 22;

/// The longest token a reference may carry.
///
/// Not a security boundary — see [`SubjectRef::new`] for what length cannot
/// decide — but a bound on what reaches the `subject_ref` column, which is
/// indexed and carried on every stored reading. A UUID is 36 characters and a
/// SHA-256 in hex is 64; anything past this is a payload rather than a
/// pseudonym.
pub const MAX_REFERENCE_TOKEN_CHARS: usize = 128;

/// Whether a token is a market identifier wearing a pseudonym's clothes.
///
/// Checked with the domain's own parsers rather than by shape, because that is
/// the whole of what "is this actually identifying" can be decided by here: a
/// MaLo-ID has a check digit, an EIC has a check character, and a
/// Zählpunktbezeichnung has a fixed length and charset. A string that satisfies
/// one of them is not a random token that happens to look like it.
fn parses_as_an_identifier(token: &str) -> bool {
    token.parse::<metering::ids::MaloId>().is_ok()
        || token.parse::<metering::ids::MeloId>().is_ok()
        || token.parse::<metering::ids::Eic>().is_ok()
        || token.parse::<metering::ids::BdewCode>().is_ok()
}

impl SubjectRef {
    /// Wrap an externally generated reference.
    /// It must carry no personal data — a name, a meter serial or an email
    /// hashed without a secret would all survive erasure as a re-identification
    /// path — and it must name the retention epoch it belongs to, which
    /// [`SubjectRegistry::register`] does for you.
    ///
    /// The shape is checked: `s<year>_<token>`, the year in canonical form, and
    /// a token between [`MIN_REFERENCE_TOKEN_CHARS`] and
    /// [`MAX_REFERENCE_TOKEN_CHARS`] characters drawn from `A-Z a-z 0-9 . _ -`.
    /// That admits hex, base64url and a UUID — anything a foreign minter is
    /// likely to produce — while refusing the short, meaningful string a
    /// pipeline puts there when it has not been given a reference to use.
    ///
    /// A token that **parses as a market identifier** — a MaLo-ID, a
    /// Messlokation, an EIC or a BDEW code — is refused outright. Those carry
    /// check digits, so recognising one is not guesswork, and a length floor
    /// alone let the most identifying of them through: a Zählpunktbezeichnung is
    /// 33 uppercase alphanumerics and clears both the floor and the alphabet.
    ///
    /// # What this check cannot do
    ///
    /// It cannot tell a keyed hash from an unkeyed one. `s2026_<sha256 of an
    /// email>` and `s2026_<HMAC of the same email>` are both 64 hex characters
    /// and no inspection of the string distinguishes them — the first is a
    /// re-identification path that survives erasure, the second is not.
    ///
    /// So this validates a *shape* and refuses what it can recognise; it does not
    /// certify that a foreign reference is unlinkable. That remains the minter's
    /// obligation, and [`SubjectRegistry::register`] is the way to not have it:
    /// it draws 128 bits from the OS CSPRNG, which is unlinkable by construction
    /// rather than by assurance.
    pub fn new(reference: impl Into<String>) -> Result<Self> {
        let reference = reference.into();
        if reference.trim().is_empty() {
            return Err(Error::config("subject reference must not be empty"));
        }
        let this = Self(reference);
        this.parts()?;
        Ok(this)
    }

    /// Mint a reference for `epoch`, with no derivation from the subject.
    ///
    /// Deriving it — hashing a meter serial, say — would leave a
    /// re-identification path that survives erasure, because anyone holding the
    /// serial could recompute the reference and find the readings again.
    ///
    /// 128 bits from the operating system's CSPRNG. `std`'s `RandomState` would
    /// be the convenient choice and is the wrong one: it is seeded once per
    /// thread and then *incremented*, and its documentation is explicit that it
    /// is not cryptographically secure. It exists to make `HashMap` collision
    /// attacks hard, which is a weaker property than the one erasure needs.
    fn mint(epoch: i32) -> Self {
        let mut bytes = [0u8; 16];
        // The OS entropy source failing is not a recoverable condition —
        // continuing would mean minting predictable references and calling them
        // pseudonyms.
        getrandom::fill(&mut bytes).expect("OS entropy source unavailable");
        Self(format!("{EPOCH_PREFIX}{epoch}_{}", hex(&bytes)))
    }

    /// The reference as stored.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The retention epoch — the year of the day the values this reference may
    /// attribute are **balanced** on. See [`retention_epoch`].
    pub fn epoch(&self) -> Result<i32> {
        Ok(self.parts()?.0)
    }

    /// The epoch and the token, with the whole shape checked.
    ///
    /// One place, so `new` and `epoch` cannot come to disagree about what a
    /// well-formed reference is.
    fn parts(&self) -> Result<(i32, &str)> {
        let malformed = |why: &str| {
            Error::config(format!(
                "subject reference {:?} {why}: the shape is `s<year>_<token>` with a \
                 token of at least {MIN_REFERENCE_TOKEN_CHARS} characters from \
                 `A-Z a-z 0-9 . _ -`. The year is what lets a write check a reference \
                 against the year of the reading it is attached to, and the token is \
                 what makes it a pseudonym rather than a label. Mint one with \
                 `SubjectRegistry::register`",
                self.0
            ))
        };

        let rest = self
            .0
            .strip_prefix(EPOCH_PREFIX)
            .ok_or_else(|| malformed("does not name a retention epoch"))?;
        let (year, token) = rest
            .split_once('_')
            .ok_or_else(|| malformed("does not name a retention epoch"))?;
        // Canonical spelling only. `i32::from_str` accepts `+2026` and `02026`,
        // and the table's constraint is textual —
        // `starts_with(subject_ref, 's' || epoch::text || '_')` — so a
        // non-canonical year satisfies the Rust check on the write path and is
        // refused by the database on registration: two spellings of one fact,
        // disagreeing, which is what that constraint exists to prevent.
        let digits = year.strip_prefix('-').unwrap_or(year);
        if digits.is_empty()
            || !digits.bytes().all(|b| b.is_ascii_digit())
            || (digits.len() > 1 && digits.starts_with('0'))
        {
            return Err(malformed(
                "does not name a retention epoch in canonical form",
            ));
        }
        let year = year
            .parse::<i32>()
            .map_err(|_| malformed("does not name a retention epoch"))?;

        if token.chars().count() < MIN_REFERENCE_TOKEN_CHARS {
            return Err(malformed("carries too short a token to be a pseudonym"));
        }
        if token.chars().count() > MAX_REFERENCE_TOKEN_CHARS {
            return Err(malformed(
                "carries a token longer than any pseudonym needs to be",
            ));
        }
        if !token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        {
            return Err(malformed("has a token outside the permitted alphabet"));
        }
        // The length floor rules out the *short* meaningful strings. It does not
        // rule out the long ones, which are the more identifying: a
        // Zählpunktbezeichnung is 33 uppercase alphanumerics and clears both the
        // floor and the alphabet. So the identifiers this crate can recognise are
        // recognised and refused, rather than trusted to be too short to matter.
        if parses_as_an_identifier(token) {
            return Err(malformed(
                "is a market identifier rather than a pseudonym — storing one here \
                 would preserve the linkage that erasing the mapping row exists to \
                 destroy",
            ));
        }
        Ok((year, token))
    }
}

impl std::fmt::Display for SubjectRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a linkage was destroyed.
///
/// § 60 Abs. 6 MsbG and Article 17 are different duties with different legal
/// bases, and a regulator asks about them separately. `reason` is caller-supplied
/// free text and cannot carry it: two deployments spell one duty differently.
///
/// The same value is the `trigger` attribute on
/// [`Metrics::subjects_erased`](crate::observe::Metrics::subjects_erased), so the
/// counter and the trail agree by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ErasureTrigger {
    /// An Article 17 request — the duty that arrives when a subject asks.
    ///
    /// A flat zero is an ordinary quarter.
    Request,
    /// The § 60 Abs. 6 MsbG sweep — the duty that comes due on a clock.
    ///
    /// A flat zero over a year is a sweep that is **not running**.
    Retention,
}

impl ErasureTrigger {
    /// How it is stored and how it is labelled on a metric.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Retention => "retention",
        }
    }
}

impl std::fmt::Display for ErasureTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ErasureTrigger {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "request" => Ok(Self::Request),
            "retention" => Ok(Self::Retention),
            other => Err(Error::config(format!(
                "{other:?} is not an erasure trigger: it is `request` for an Article 17 \
                 erasure or `retention` for the § 60 Abs. 6 sweep"
            ))),
        }
    }
}

/// Live tombstones written under a key the ring no longer carries.
///
/// One per missing key. See
/// [`orphaned_suppressions`](SubjectRegistry::orphaned_suppressions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanedSuppressions {
    /// The missing key's name, hex, as it appears in the audit trail.
    ///
    /// Derived from the key, so an operator matches it by feeding a candidate
    /// key back to the ring and seeing the report shrink — not by recognising
    /// the string.
    pub key_id: String,
    /// How many live suppressions that key wrote and nothing can now match.
    pub suppressions: u64,
}

/// A suppression that was reversed, and by whom.
///
/// Lifting is the one action here that *undoes* a compliance decision — it lets
/// an identifier be registered again after an erasure — so leaving it to a log
/// line would put the only record of it in the one place that rotates away. A
/// reviewer reading the trail would see an erasure, then a live registration,
/// and nothing in between to say who authorised it.
///
/// It does not undo the erasure itself: the mapping is gone and the readings stay
/// unattributable. What it restores is the ability to register under a **new**
/// reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuppressionLift {
    /// When the suppression was lifted.
    pub at: OffsetDateTime,
    /// Who lifted it.
    pub actor: String,
    /// Why — the ticket that authorised reversing a compliance action.
    pub reason: String,
}

/// Proof that an erasure happened.
///
/// Recorded because a regulator may ask, and because "we deleted it" is not
/// evidence. Deliberately holds no natural identifier — that is the thing being
/// erased, and an audit trail that retains it would defeat the exercise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErasureRecord {
    /// The reference whose linkage was destroyed.
    ///
    /// `None` for the one case where there was no reference to name: an
    /// [`erase_all`](SubjectRegistry::erase_all) against an identifier this
    /// deployment holds no mapping for. That is not a failed request — it is an
    /// Article 17 request that arrived before the ingest did, and what it leaves
    /// behind is the suppression tombstone that keeps the identifier out.
    pub subject: Option<SubjectRef>,
    /// When it happened.
    pub erased_at: OffsetDateTime,
    /// Why — a request identifier or ticket, not a person's details.
    pub reason: String,
    /// Who performed it.
    pub actor: String,
    /// Which duty it discharged.
    pub trigger: ErasureTrigger,
    /// The suppression this erasure raised, if it has since been lifted.
    ///
    /// Present only where a suppression key was configured and
    /// [`lift_suppression`](SubjectRegistry::lift_suppression) has run against
    /// this identifier. The erasure itself is never undone.
    pub lifted: Option<SuppressionLift>,
}

impl ErasureRecord {
    /// The retention epoch the destroyed linkage covered.
    ///
    /// `None` when the record names no reference — a pre-emptive suppression
    /// belongs to no collection year, because no value has been collected.
    #[must_use]
    pub fn epoch(&self) -> Option<i32> {
        self.subject.as_ref().and_then(|s| s.epoch().ok())
    }
}

/// One live `(natural identifier, retention epoch)` mapping.
///
/// What [`SubjectRegistry::registrations`] answers, and the shape an Article 15
/// access request wants: *which years of this person does the store still hold a
/// link for, and since when*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectRegistration {
    /// The reference stored on that year's readings.
    pub subject: SubjectRef,
    /// The retention epoch it may attribute — see [`retention_epoch`].
    pub epoch: i32,
    /// When the mapping was created. Not when the values were collected.
    pub registered_at: OffsetDateTime,
}

/// The mapping between natural identifiers and pseudonymous references.
///
/// Lives in PostgreSQL rather than the lake because erasure needs storage where
/// deletion is real.
///
/// # One registry spans every table in a deployment
///
/// It is constructed per [`MeterStoreBuilder`], which reads as *per table*, and
/// it is not: the mapping lives in one `meterstore_subject_map` keyed by natural
/// identifier. So two tables that register the **same** natural id share one
/// [`SubjectRef`], and a single [`erase`](Self::erase) unlinks both.
///
/// That is the behaviour an Article 17 request needs rather than an accident of
/// the schema. An erasure has to reach the authoritative readings *and* the
/// non-authoritative second stream — an ESA "Werte nach Typ 2" store is
/// non-authoritative for **settlement**, which says nothing about whether the
/// data is personal. A registry per table would leave one of them linked, and
/// nothing would report it.
///
/// The corollary is that the **granularity of a subject is the deployment's
/// choice, and it is global**. Keying by measuring point alone erases a previous
/// tenant's data along with the requester's, because a Marktlokation outlives its
/// occupants; `(tenant, MaLo)` or an occupancy period is usually what is meant.
///
/// [`MeterStoreBuilder`]: crate::MeterStoreBuilder
#[derive(Clone)]
pub struct SubjectRegistry {
    pool: PgPool,
    /// The suppression list's keys, newest first, or empty for none.
    ///
    /// A ring rather than one key, because a tombstone cannot be re-keyed and a
    /// single key would therefore be one that can never be rotated — see
    /// [`with_erasure_keys`](Self::with_erasure_keys). The first writes, every
    /// one reads.
    ///
    /// Two separate protections on the material, because they answer different
    /// questions. [`Debug`] is hand-written below, so a key cannot reach a log
    /// line; [`Zeroizing`] wipes each allocation on drop, so it does not linger
    /// in freed heap pages, a core dump or swap.
    ///
    /// The second matters here because this type is [`Clone`] and every derived
    /// session — [`as_of`], [`as_known_at`], [`scoped`], [`in_own_session`] —
    /// clones it. Each clone is another copy of a cryptographic key, and without
    /// this each one would be left behind when its session was dropped.
    ///
    /// [`as_of`]: crate::MeterStore::as_of
    /// [`as_known_at`]: crate::MeterStore::as_known_at
    /// [`scoped`]: crate::MeterStore::scoped
    /// [`in_own_session`]: crate::MeterStore::in_own_session
    erasure_keys: Vec<Zeroizing<Vec<u8>>>,
}

impl std::fmt::Debug for SubjectRegistry {
    /// Redacts the suppression key.
    ///
    /// A registry is a plausible thing to include in a `tracing` field or an
    /// error context, and the key must not reach a log.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubjectRegistry")
            .field("suppression", &self.suppresses_reregistration())
            .field("erasure_keys", &self.erasure_keys.len())
            .finish_non_exhaustive()
    }
}

/// The shortest key [`SubjectRegistry::with_erasure_secret`] accepts.
///
/// A HMAC-SHA256 key, so this is the hash's own output width: shorter keys are
/// permitted by the construction and are exactly what makes the suppression
/// tombstone brute-forceable, since meter and market-location identifiers come
/// from small structured spaces. Public so the configuration front end can refuse
/// one at `meterstore check` rather than at the first process start.
pub const MIN_ERASURE_SECRET_BYTES: usize = 32;

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
            erasure_keys: Vec::new(),
        }
    }

    /// Wrap a connection pool and enforce a suppression list.
    ///
    /// Erasure then records a keyed hash of the identifier it erased, and
    /// [`register`](Self::register) refuses anything that matches. That is what
    /// makes the promise in `register`'s documentation true rather than
    /// aspirational.
    ///
    /// One key. [`with_erasure_keys`](Self::with_erasure_keys) is the same thing
    /// with a ring, which is what a deployment needs the first time it rotates.
    ///
    /// # Why a keyed hash rather than the identifier
    ///
    /// Storing the identifier would defeat the erasure it documents, and an
    /// *unkeyed* hash barely less: meter and market-location identifiers come
    /// from small structured spaces, so anyone with the table could enumerate
    /// candidates and invert it. The key reduces the tombstone to an oracle
    /// answering "was this one erased?" only for someone already holding both the
    /// identifier and the key — the minimum needed to honour a request that says
    /// *stop processing my data*, and the recognised practice for suppression
    /// lists.
    pub fn with_erasure_secret(pool: PgPool, secret: &[u8]) -> Result<Self> {
        Self::with_erasure_keys(pool, &[secret])
    }

    /// Wrap a connection pool and enforce a suppression list under a **key
    /// ring**.
    ///
    /// `keys[0]` writes every new tombstone; every key in the ring is checked
    /// when one is looked up. That is the whole of rotation, and the shape is
    /// forced: a tombstone is `HMAC(key, identifier)` and the identifier was
    /// destroyed in the same transaction that wrote it, so there is no input from
    /// which to recompute the tag under a new key.
    ///
    /// A retired key is therefore kept for as long as the erasures it recorded
    /// must stay suppressed — indefinitely, for Article 17. Retiring a key stops
    /// it writing; it does not mean it can be destroyed. What rotation bounds is a
    /// key's window as a **writing** key, which is what a compromise of it costs.
    ///
    /// Each key must be at least [`MIN_ERASURE_SECRET_BYTES`] — including a
    /// retired one, since it is the older tombstones that covers — and the ring
    /// must not be empty. An empty ring is [`new`](Self::new), which says out loud
    /// that suppression is off rather than looking as though it were on.
    ///
    /// The copies kept here are [`Zeroizing`]; see the field documentation for
    /// what that does and does not claim.
    pub fn with_erasure_keys(pool: PgPool, keys: &[&[u8]]) -> Result<Self> {
        if keys.is_empty() {
            return Err(Error::config(
                "an erasure key ring must hold at least one key: an empty ring \
                 enforces nothing, and `SubjectRegistry::new` is how a deployment \
                 says suppression is off rather than looking as though it were on",
            ));
        }
        // A short key makes the oracle brute-forceable, which is the one thing
        // the construction is supposed to prevent. Checked for retired keys too:
        // a weak key still in the ring is still a key an attacker can invert
        // tombstones with, and it is the *old* ones a retired key covers.
        for (i, key) in keys.iter().enumerate() {
            if key.len() < MIN_ERASURE_SECRET_BYTES {
                return Err(Error::config(format!(
                    "erasure key {i} is {} bytes and must be at least \
                     {MIN_ERASURE_SECRET_BYTES} bytes: a shorter key can be \
                     brute-forced, and the suppression list would then leak the \
                     identifiers it exists to forget",
                    key.len()
                )));
            }
        }
        Ok(Self {
            pool,
            erasure_keys: keys.iter().map(|k| Zeroizing::new(k.to_vec())).collect(),
        })
    }

    /// Suppressions this registry can no longer recognise, by the key that
    /// wrote them.
    ///
    /// Empty is the healthy answer and the usual one. A non-empty result means
    /// the ring has lost a key that live tombstones were written under, so every
    /// subject those cover is **no longer suppressed**: a replayed message
    /// re-registers them and the link erasure destroyed comes back.
    ///
    /// Nothing else can see this. A tombstone whose key is gone is still in the
    /// table and simply stops matching, which is exactly what a tombstone that
    /// does not apply looks like — so the failure had no symptom short of the
    /// subject reappearing (R6, R25). The key's name, stored beside the tag,
    /// is the whole of what makes it visible.
    ///
    /// Lifted suppressions are excluded: lifting clears the tag and its key
    /// name together, so a key retired after every suppression it wrote was
    /// lifted is not reported. A registry with **no** ring answers about every
    /// live tombstone, because a deployment that lost its only key is the same
    /// failure reached by a shorter road.
    ///
    /// # Repairing it
    ///
    /// Put the key back. There is no other repair: the tag cannot be recomputed
    /// under a different key, because the identifier was destroyed in the
    /// transaction that wrote it. If the key is genuinely gone, the subjects it
    /// covers cannot be kept out by this crate, and the deployment has to stop
    /// the replay upstream instead.
    pub async fn orphaned_suppressions(&self) -> Result<Vec<OrphanedSuppressions>> {
        sqlx::query_as::<_, (Vec<u8>, i64)>(
            r#"SELECT hmac_key_id, count(*)
                 FROM meterstore_erasures
                WHERE hmac_key_id IS NOT NULL
                  AND hmac_key_id <> ALL($1)
             GROUP BY hmac_key_id
             ORDER BY count(*) DESC"#,
        )
        .bind(self.ring_key_ids())
        .fetch_all(&self.pool)
        .await
        .map_err(pg)
        .map(|rows| {
            rows.into_iter()
                .map(|(id, count)| OrphanedSuppressions {
                    key_id: hex(&id),
                    suppressions: count.max(0) as u64,
                })
                .collect()
        })
    }

    /// Whether a suppression list is enforced.
    #[must_use]
    pub fn suppresses_reregistration(&self) -> bool {
        !self.erasure_keys.is_empty()
    }

    /// How many keys the ring holds — one, plus one per retired key.
    ///
    /// For a deployment reporting its own posture. The keys themselves are not
    /// reachable from here.
    #[must_use]
    pub fn erasure_key_count(&self) -> usize {
        self.erasure_keys.len()
    }

    /// Keyed hash of a natural identifier under the **writing** key.
    ///
    /// What a new tombstone is recorded as. `None` without a configured key.
    fn tombstone(&self, natural_id: &str) -> Option<Vec<u8>> {
        Some(mac(self.erasure_keys.first()?, natural_id))
    }

    /// The name of the key a new tombstone is written under.
    ///
    /// Stored beside the tag, because the tag cannot be recomputed: the
    /// identifier died in the transaction that wrote it. Without this, a key
    /// dropped from the ring leaves tombstones that are still there and simply
    /// stop matching, which is indistinguishable from tombstones that do not
    /// apply — the one failure this crate could not report.
    fn writing_key_id(&self) -> Option<Vec<u8>> {
        Some(key_id(self.erasure_keys.first()?))
    }

    /// Every key the ring can read a tombstone under, by name.
    fn ring_key_ids(&self) -> Vec<Vec<u8>> {
        self.erasure_keys.iter().map(|k| key_id(k)).collect()
    }

    /// Keyed hash under **every** key in the ring, for a lookup.
    ///
    /// Empty without a configured key, which is why every caller of it treats an
    /// empty result as *no suppression list* rather than as *not suppressed*.
    fn tombstones(&self, natural_id: &str) -> Vec<Vec<u8>> {
        self.erasure_keys
            .iter()
            .map(|key| mac(key, natural_id))
            .collect()
    }

    /// Whether any key in the ring recognises this identifier as erased.
    async fn suppressed_on(&self, conn: &mut sqlx::PgConnection, natural_id: &str) -> Result<bool> {
        let tombstones = self.tombstones(natural_id);
        if tombstones.is_empty() {
            return Ok(false);
        }
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM meterstore_erasures WHERE natural_id_hmac = ANY($1))",
        )
        .bind(&tombstones)
        .fetch_one(conn)
        .await
        .map_err(pg)
    }

    /// Create the registry's tables.
    ///
    /// Two of them, deliberately. The mapping is deletable; the audit trail is
    /// append-only and outlives what it describes.
    pub async fn create_tables(&self) -> Result<()> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS meterstore_subject_map (
                   subject_ref   TEXT PRIMARY KEY,
                   natural_id    TEXT NOT NULL,
                   -- The calendar year of the values this row may attribute.
                   -- `(natural_id, epoch)` rather than `natural_id` alone,
                   -- because § 60 Abs. 6 runs per value: a subject's 2020
                   -- readings come due while their 2026 readings are current,
                   -- and one row covering both could satisfy neither.
                   epoch         INTEGER NOT NULL,
                   registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                   UNIQUE (natural_id, epoch),
                   -- The epoch is written twice: as this column, which the
                   -- sweep selects on, and inside the reference, which the
                   -- write path parses without a round trip. Two spellings of
                   -- one fact can disagree, and a row where they did would be
                   -- swept on one year while refusing readings from the other
                   -- — with nothing to report it, since both look well formed.
                   CONSTRAINT meterstore_subject_map_epoch_matches_reference
                       CHECK (starts_with(subject_ref, 's' || epoch::text || '_'))
               )"#,
        )
        .execute(&self.pool)
        .await
        .map_err(pg)?;

        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS meterstore_erasures (
                   id              BIGSERIAL PRIMARY KEY,
                   -- The reference whose linkage died. NULL for the one case
                   -- that names none: an `erase_all` against an identifier no
                   -- mapping was ever created for, which leaves a suppression
                   -- tombstone and nothing else. UNIQUE rather than the primary
                   -- key so those rows can exist at all — PostgreSQL treats
                   -- NULLs as distinct in a unique index, and a surrogate key
                   -- keeps `ON CONFLICT (subject_ref)` working for the rest.
                   subject_ref     TEXT UNIQUE,
                   erased_at       TIMESTAMPTZ NOT NULL,
                   reason          TEXT NOT NULL,
                   actor           TEXT NOT NULL,
                   -- Which duty this discharged: 'request' or 'retention'.
                   -- `reason` is caller-supplied free text, so without this the
                   -- trail cannot answer either of the two questions a
                   -- regulator actually asks — show me the requests you
                   -- handled, and show me that your retention clock runs.
                   trigger         TEXT NOT NULL,
                   -- Keyed hash of the erased identifier. NULL when the
                   -- deployment configured no suppression key, in which case a
                   -- replayed message can re-register the subject — and NULL
                   -- again once a suppression has been lifted.
                   natural_id_hmac BYTEA,
                   -- Which key in the ring wrote that tag. Derived from the key
                   -- rather than named by the operator, and stored because the
                   -- tag cannot be recomputed under another one: the identifier
                   -- died in the transaction that wrote it. A key dropped from
                   -- the ring otherwise leaves tombstones that are still there
                   -- and simply stop matching, which looks exactly like
                   -- tombstones that do not apply.
                   hmac_key_id     BYTEA,
                   -- Set when the suppression was lifted. Lifting reverses a
                   -- compliance decision, so it is recorded on the row it
                   -- concerns rather than left to a log line that rotates away.
                   lifted_at       TIMESTAMPTZ,
                   lifted_by       TEXT,
                   lift_reason     TEXT,
                   CONSTRAINT meterstore_erasures_lift_is_whole
                       CHECK (num_nulls(lifted_at, lifted_by, lift_reason) IN (0, 3)),
                   -- A tag with no key name is unreportable and a key name with
                   -- no tag names nothing. The pair is the unit, so the database
                   -- refuses a row where one arrived without the other rather
                   -- than leaving a statement that forgot one to be found by the
                   -- report that could no longer be trusted.
                   CONSTRAINT meterstore_erasures_tombstone_is_whole
                       CHECK (num_nulls(natural_id_hmac, hmac_key_id) IN (0, 2))
               )"#,
        )
        .execute(&self.pool)
        .await
        .map_err(pg)?;

        // Before the indexes, which are the first statements that name a
        // column: against a database holding an earlier shape of these tables a
        // partial index would fail on the column it filters by, which is the
        // confusing error this check exists to replace.
        self.check_schema().await?;

        // The retention sweep selects on it, and it is the whole of what the
        // sweep looks at — no reading is consulted.
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS meterstore_subject_map_epoch
                   ON meterstore_subject_map (epoch)"#,
        )
        .execute(&self.pool)
        .await
        .map_err(pg)?;

        // `UNIQUE (natural_id, epoch)` already indexes `natural_id` as its
        // leading column, so enumerating a subject's epochs — which is what an
        // Article 17 request and an Article 15 one both start with — is an index
        // range scan and needs no index of its own.

        // Registration checks this on every miss, so it must not be a scan.
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS meterstore_erasures_hmac
                   ON meterstore_erasures (natural_id_hmac)
                WHERE natural_id_hmac IS NOT NULL"#,
        )
        .execute(&self.pool)
        .await
        .map_err(pg)?;

        // After the tables exist and before anything uses them, because this is
        // the one moment a deployment is still in a position to put the key
        // back. It reports rather than refuses: the trail is a historical fact
        // that restarting cannot repair, and the only way to make a refusal
        // pass would be to drop the audit trail — the one thing this table must
        // not lose.
        for orphan in self.orphaned_suppressions().await? {
            warn!(
                key_id = %orphan.key_id,
                suppressions = orphan.suppressions,
                "the erasure key ring no longer carries the key these suppressions \
                 were written under, so the subjects they cover can be re-registered \
                 by a replay; put the key back, or stop the replay upstream"
            );
        }

        info!(
            suppression = self.suppresses_reregistration(),
            keys = self.erasure_key_count(),
            "subject registry ready"
        );
        Ok(())
    }

    /// Refuse a registry whose tables predate the columns this version writes.
    ///
    /// `CREATE TABLE IF NOT EXISTS` is idempotent and therefore silent: against a
    /// database carrying an earlier shape of these tables it creates nothing and
    /// reports success, and the first divergence surfaces as `column "trigger"
    /// does not exist` at the first erasure — a compliance operation, from a
    /// stack trace that says nothing about what to do.
    ///
    /// The crate is unpublished and its schema changes in place rather than
    /// through migrations, so the answer really is *drop these two tables and
    /// call this again*. This is what says so, at the setup step where it can be
    /// acted on rather than at the request it would otherwise fail.
    ///
    /// Columns only. A constraint added in a later version is not detected here,
    /// which is the honest limit of a check this cheap.
    async fn check_schema(&self) -> Result<()> {
        for (table, expected) in [
            (
                "meterstore_subject_map",
                &["subject_ref", "natural_id", "epoch", "registered_at"][..],
            ),
            (
                "meterstore_erasures",
                &[
                    "id",
                    "subject_ref",
                    "erased_at",
                    "reason",
                    "actor",
                    "trigger",
                    "natural_id_hmac",
                    "hmac_key_id",
                    "lifted_at",
                    "lifted_by",
                    "lift_reason",
                ][..],
            ),
        ] {
            // `to_regclass` resolves through the connection's `search_path`, so
            // this asks about the table the queries above will actually hit —
            // `information_schema` matching on the bare name would answer for a
            // same-named table in another schema.
            let present: Vec<String> = sqlx::query_scalar(
                "SELECT attname FROM pg_attribute \
                  WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped",
            )
            .bind(table)
            .fetch_all(&self.pool)
            .await
            .map_err(pg)?;

            let missing: Vec<&str> = expected
                .iter()
                .copied()
                .filter(|column| !present.iter().any(|p| p == column))
                .collect();

            if !missing.is_empty() {
                return Err(Error::config(format!(
                    "{table} is missing {missing:?}, so it was created by an earlier \
                     version of this crate. The registry schema changes in place \
                     rather than through migrations — the crate is unpublished — so \
                     drop `meterstore_subject_map` and `meterstore_erasures` and call \
                     `create_tables` again. Both hold compliance state: the mapping is \
                     rebuilt by re-registering, and the audit trail is not, so export \
                     it first if this deployment has erased anything"
                )));
            }
        }
        Ok(())
    }

    /// Register a natural identifier, returning its pseudonymous reference.
    ///
    /// `at` is an instant **from the data** — any interval in the period being
    /// written — and `sparte` is the commodity those readings carry. Together
    /// they name the retention epoch through [`retention_epoch`], which is the
    /// same function the write path checks a reference with, so a reference this
    /// mints is by construction one that write accepts.
    ///
    /// Idempotent: registering the same identifier for the same epoch twice
    /// returns the same reference, so an ingest path may call it per batch
    /// without accumulating references for one subject.
    ///
    /// Refuses to re-register an identifier whose reference has been erased —
    /// **only when a suppression key is configured**
    /// ([`with_erasure_secret`](Self::with_erasure_secret)). Re-registration
    /// resurrects the link erasure destroyed, and it almost always means a stale
    /// pipeline is replaying data that should be dropped.
    ///
    /// Without a key the check is not merely disabled, it is impossible: erasure
    /// deletes the mapping, so nothing remains to recognise the identifier by.
    pub async fn register(
        &self,
        natural_id: &str,
        at: OffsetDateTime,
        sparte: Sparte,
    ) -> Result<SubjectRef> {
        self.register_in_epoch(natural_id, retention_epoch(at, sparte))
            .await
    }

    /// [`register`](Self::register) for a caller that already holds the epoch.
    ///
    /// The primitive the instant-and-commodity form is written in terms of. Use
    /// it where the epoch comes from somewhere other than an interval — a
    /// reference being re-minted after a `lift_suppression`, say, or a migration
    /// reading epochs out of an old mapping.
    pub async fn register_in_epoch(&self, natural_id: &str, epoch: i32) -> Result<SubjectRef> {
        check_natural_id(natural_id)?;

        // Outside a transaction and before any lock: a live mapping is the
        // common case by a wide margin, means the subject was never erased, and
        // should cost exactly one round trip.
        if let Some(existing) = self.lookup_in_epoch(natural_id, epoch).await? {
            return Ok(existing);
        }

        // Everything past here races an erasure of the same identifier, and the
        // outcome of losing that race is a resurrected linkage that nothing
        // reports. The advisory lock is taken on the identifier rather than on a
        // row because the row this is about to create does not exist yet, so
        // there is nothing to lock — which is precisely the window `erase` would
        // otherwise slip through.
        let mut tx = self.pool.begin().await.map_err(pg)?;
        lock_natural_id(&mut tx, natural_id).await?;

        // Re-read under the lock. An erasure that committed between the fast
        // path above and the lock leaves no mapping and a tombstone; one that
        // committed before it may have left neither.
        if let Some(existing) = lookup_in(&mut tx, natural_id, epoch).await? {
            tx.commit().await.map_err(pg)?;
            return Ok(existing);
        }

        // Checked against **every** key in the ring, so an erasure recorded
        // under a retired key still refuses the identifier.
        if self.suppressed_on(&mut tx, natural_id).await? {
            // Counted, not merely logged. Every one of these is a pipeline
            // handing the store data from before an Article 17 erasure —
            // refused here, and still to be fixed wherever it came from.
            crate::observe::metrics()
                .registrations_suppressed
                .add(1, &[]);
            warn!("registration refused for an erased identifier");
            return Err(Error::config(
                "this identifier was erased and must not be re-registered: \
                 registering it would rebuild the link Article 17 destroyed. \
                 A subject who genuinely returns should arrive under a new \
                 identifier; if the erasure itself was mistaken, lift it \
                 explicitly with `lift_suppression`",
            ));
        }

        let reference = SubjectRef::mint(epoch);
        let inserted = sqlx::query_scalar::<_, String>(
            r#"INSERT INTO meterstore_subject_map (subject_ref, natural_id, epoch)
               VALUES ($1, $2, $3)
               ON CONFLICT (natural_id, epoch) DO UPDATE SET natural_id = EXCLUDED.natural_id
               RETURNING subject_ref"#,
        )
        .bind(reference.as_str())
        .bind(natural_id)
        .bind(epoch)
        .fetch_one(&mut *tx)
        .await
        .map_err(pg)?;
        tx.commit().await.map_err(pg)?;

        SubjectRef::new(inserted)
    }

    /// The reference for a natural identifier in the epoch `at` falls in, if one
    /// is registered.
    ///
    /// `sparte` for the same reason [`register`](Self::register) takes it: for
    /// gas the epoch boundary is the Gastag's, and asking with the wrong
    /// commodity would answer for the wrong year over the six hours a year the
    /// two disagree.
    pub async fn lookup(
        &self,
        natural_id: &str,
        at: OffsetDateTime,
        sparte: Sparte,
    ) -> Result<Option<SubjectRef>> {
        self.lookup_in_epoch(natural_id, retention_epoch(at, sparte))
            .await
    }

    /// [`lookup`](Self::lookup) for a caller that already holds the epoch.
    pub async fn lookup_in_epoch(
        &self,
        natural_id: &str,
        epoch: i32,
    ) -> Result<Option<SubjectRef>> {
        let mut conn = self.pool.acquire().await.map_err(pg)?;
        lookup_in(&mut conn, natural_id, epoch).await
    }

    /// Every live mapping for a natural identifier, oldest epoch first.
    ///
    /// The enumeration an Article 17 request starts from and an Article 15 one
    /// answers with. A reference covers one collection year, so *"which years of
    /// this person does the store still link?"* is not a question
    /// [`lookup`](Self::lookup) can answer.
    ///
    /// Erased and expired epochs are absent, because their rows are gone — which
    /// makes this the live picture rather than a history. The history is
    /// [`erasures`](Self::erasures).
    pub async fn registrations(&self, natural_id: &str) -> Result<Vec<SubjectRegistration>> {
        let rows = sqlx::query_as::<_, (String, i32, OffsetDateTime)>(
            "SELECT subject_ref, epoch, registered_at FROM meterstore_subject_map \
             WHERE natural_id = $1 ORDER BY epoch",
        )
        .bind(natural_id)
        .fetch_all(&self.pool)
        .await
        .map_err(pg)?;

        rows.into_iter()
            .map(|(reference, epoch, registered_at)| {
                Ok(SubjectRegistration {
                    subject: SubjectRef::new(reference)?,
                    epoch,
                    registered_at,
                })
            })
            .collect()
    }

    /// The retention epochs a natural identifier still has a reference for,
    /// ascending.
    ///
    /// [`registrations`](Self::registrations) without the rest of the row.
    pub async fn epochs(&self, natural_id: &str) -> Result<Vec<i32>> {
        Ok(self
            .registrations(natural_id)
            .await?
            .into_iter()
            .map(|r| r.epoch)
            .collect())
    }

    /// The references a natural identifier still has, ordered by epoch.
    ///
    /// [`registrations`](Self::registrations) without the rest of the row.
    pub async fn references(&self, natural_id: &str) -> Result<Vec<SubjectRef>> {
        Ok(self
            .registrations(natural_id)
            .await?
            .into_iter()
            .map(|r| r.subject)
            .collect())
    }

    /// Which of `references` still have a live mapping.
    ///
    /// The set membership test a write path needs, in one round trip rather than
    /// one per reference — and deliberately *not*
    /// [`resolve`](Self::resolve)-shaped: it answers whether a reference is
    /// usable without handing back the identifiers behind a whole batch, which
    /// is personal data the caller asking this question has no need for.
    pub async fn resolvable(
        &self,
        references: &[String],
    ) -> Result<std::collections::HashSet<String>> {
        if references.is_empty() {
            return Ok(std::collections::HashSet::new());
        }
        let found = sqlx::query_scalar::<_, String>(
            "SELECT subject_ref FROM meterstore_subject_map WHERE subject_ref = ANY($1)",
        )
        .bind(references)
        .fetch_all(&self.pool)
        .await
        .map_err(pg)?;

        Ok(found.into_iter().collect())
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

    /// Destroy the link between a reference and its subject — in **every**
    /// retention epoch that subject has.
    ///
    /// The lake keeps every reading. What it loses is any way to attribute them
    /// to a person, which is what Article 17 asks for and what leaves the
    /// remaining series anonymous.
    ///
    /// A reference names one collection year; an Article 17 request names a
    /// person. So this resolves the reference to its identifier and unlinks
    /// every year of it — a caller holding this year's reference and getting
    /// only this year erased would be told the request was honoured while last
    /// year's readings stayed attributable.
    ///
    /// [`erase_all`](Self::erase_all) is the same operation entered from the
    /// identifier instead, which is what a request actually names.
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
    ) -> Result<Vec<ErasureRecord>> {
        self.erase_triggered_by(
            Subject::Reference(subject),
            reason,
            actor,
            now,
            ErasureTrigger::Request,
        )
        .await
    }

    /// Destroy every linkage a **natural identifier** has, in every epoch.
    ///
    /// The entry point an Article 17 request actually has: it names a person,
    /// not a year and not an opaque token the requester has never seen. Returns
    /// one record per epoch unlinked, oldest first.
    ///
    /// # When the identifier is not registered
    ///
    /// A request may arrive before the ingest does, or after a previous erasure
    /// already ran. There is then no linkage to destroy, and the other half of
    /// the request still stands: *do not start*. With a suppression key
    /// configured this writes the tombstone anyway and returns one record naming
    /// no reference — [`ErasureRecord::subject`] is `None` — so a later
    /// [`register`](Self::register) of that identifier is refused.
    ///
    /// Without a key there is nothing to record against and nothing that could
    /// refuse the identifier later, so this returns empty and logs that it did.
    pub async fn erase_all(
        &self,
        natural_id: &str,
        reason: &str,
        actor: &str,
        now: OffsetDateTime,
    ) -> Result<Vec<ErasureRecord>> {
        self.erase_triggered_by(
            Subject::Natural(natural_id),
            reason,
            actor,
            now,
            ErasureTrigger::Request,
        )
        .await
    }

    /// [`erase`](Self::erase) and [`erase_all`](Self::erase_all) in one
    /// transaction of this registry's own, saying what triggered it.
    ///
    /// The trigger is a metric attribute and nothing else — the audit row is the
    /// same either way, because `reason` is what a regulator reads. It exists
    /// because the two triggers have **opposite** readings: a flat `retention`
    /// series is a sweep that is not running, and a flat `request` series is an
    /// ordinary quarter. Summed into one counter, a deployment whose sweep had
    /// silently stopped but which handled the occasional Article 17 request would
    /// look like one whose sweep was working.
    pub(crate) async fn erase_triggered_by(
        &self,
        subject: Subject<'_>,
        reason: &str,
        actor: &str,
        now: OffsetDateTime,
        trigger: ErasureTrigger,
    ) -> Result<Vec<ErasureRecord>> {
        let mut tx = self.pool.begin().await.map_err(pg)?;
        let (records, destroyed) = self
            .erase_linkage(&mut tx, subject, reason, actor, now, trigger)
            .await?;
        tx.commit().await.map_err(pg)?;
        // Counted after the commit, and only when a linkage actually died: a
        // repeat request is auditable and is not a second erasure, so counting
        // it would report a compliance event that did not happen.
        if destroyed {
            crate::observe::metrics()
                .subjects_erased
                .add(1, &crate::observe::erasure_trigger(trigger));
        }
        Ok(records)
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
    /// **It must be a transaction, not a bare connection.** The lock that keeps
    /// a concurrent [`register`](Self::register) from resurrecting the linkage
    /// is transaction-scoped, so on a connection in autocommit it is released
    /// before the delete it was taken for.
    ///
    /// **Cold-tier exclusion is not part of this transaction and cannot be.** It
    /// is not a write at all: erasure destroys the *mapping*, which leaves every
    /// archived row unattributable wherever it sits. There is nothing in
    /// object storage to roll back, so sequencing is not a concern.
    pub async fn erase_in(
        &self,
        conn: &mut sqlx::PgConnection,
        subject: &SubjectRef,
        reason: &str,
        actor: &str,
        now: OffsetDateTime,
    ) -> Result<Vec<ErasureRecord>> {
        self.erase_in_owned(conn, Subject::Reference(subject), reason, actor, now)
            .await
    }

    /// [`erase_all`](Self::erase_all) inside a transaction the caller owns.
    ///
    /// The pairing [`erase_in`](Self::erase_in) has, for the entry point a
    /// request actually names. Everything `erase_in` documents applies.
    pub async fn erase_all_in(
        &self,
        conn: &mut sqlx::PgConnection,
        natural_id: &str,
        reason: &str,
        actor: &str,
        now: OffsetDateTime,
    ) -> Result<Vec<ErasureRecord>> {
        self.erase_in_owned(conn, Subject::Natural(natural_id), reason, actor, now)
            .await
    }

    /// The shared body of the two caller-owned-transaction forms.
    async fn erase_in_owned(
        &self,
        conn: &mut sqlx::PgConnection,
        subject: Subject<'_>,
        reason: &str,
        actor: &str,
        now: OffsetDateTime,
    ) -> Result<Vec<ErasureRecord>> {
        let (records, destroyed) = self
            .erase_linkage(conn, subject, reason, actor, now, ErasureTrigger::Request)
            .await?;
        // Counted as a *request*, which is what a caller-owned transaction is:
        // the cascade it encloses — billing periods, quality assessments — is an
        // Article 17 one. The retention sweep opens its own transaction and says
        // so. Counted before the caller's commit, which is the honest cost of
        // handing the transaction to them: a counter that only rose on commit
        // would need this crate to know when that happened.
        if destroyed {
            crate::observe::metrics()
                .subjects_erased
                .add(1, &crate::observe::erasure_trigger(ErasureTrigger::Request));
        }
        Ok(records)
    }

    /// The erasure itself, and whether a linkage was actually destroyed.
    ///
    /// The single mechanism under every public form, so the counting decision —
    /// which differs between them — is the only thing they do not share.
    async fn erase_linkage(
        &self,
        conn: &mut sqlx::PgConnection,
        subject: Subject<'_>,
        reason: &str,
        actor: &str,
        now: OffsetDateTime,
        trigger: ErasureTrigger,
    ) -> Result<(Vec<ErasureRecord>, bool)> {
        if reason.trim().is_empty() {
            return Err(Error::config("erasure needs a reason for the audit trail"));
        }

        let tx = conn;

        // Resolved **without** a row lock, deliberately. The advisory lock below
        // has to be the first lock this transaction takes, or it deadlocks
        // against `register`, which takes the advisory lock and only then
        // touches rows. A plain read is safe to resolve with because a
        // `subject_ref` is a unique random token: the identifier behind one
        // never changes, it only stops existing.
        let natural_id = match subject {
            Subject::Natural(id) => {
                check_natural_id(id)?;
                Some(id.to_string())
            }
            Subject::Reference(reference) => sqlx::query_scalar::<_, String>(
                "SELECT natural_id FROM meterstore_subject_map WHERE subject_ref = $1",
            )
            .bind(reference.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(pg)?,
        };

        // Held for the rest of the transaction, so a `register` of the same
        // identifier cannot slip between the tombstone being written and the
        // mapping being destroyed — the one interleaving that ends with a live
        // mapping for a suppressed subject and nothing to report it.
        if let Some(id) = natural_id.as_deref() {
            lock_natural_id(&mut *tx, id).await?;
        }

        // The tombstone has to be computed before the delete, because after it
        // the identifier is gone — which is the point. `ring` is the same
        // identifier under every key, so a pre-emptive suppression is not
        // written twice for a subject already tombstoned under a retired one.
        let tombstone = natural_id.as_deref().and_then(|id| self.tombstone(id));
        // Non-`None` exactly when `tombstone` is, which is what the row's
        // `num_nulls` constraint enforces on the way in.
        let writing_key = tombstone.is_some().then(|| self.writing_key_id()).flatten();
        let ring = natural_id
            .as_deref()
            .map(|id| self.tombstones(id))
            .unwrap_or_default();

        // **Every epoch of this subject, not just the one named.** A reference
        // covers one collection year; an Article 17 request covers a person. A
        // caller holding this year's reference and erasing only this year would
        // be told the request was honoured while last year's readings stayed
        // attributable — which is the failure this whole module exists to make
        // impossible.
        let mut targets: Vec<SubjectRef> = match natural_id.as_deref() {
            Some(id) => sqlx::query_scalar::<_, String>(
                "SELECT subject_ref FROM meterstore_subject_map WHERE natural_id = $1 \
                 ORDER BY epoch FOR UPDATE",
            )
            .bind(id)
            .fetch_all(&mut *tx)
            .await
            .map_err(pg)?
            .into_iter()
            .map(SubjectRef::new)
            .collect::<Result<_>>()?,
            None => Vec::new(),
        };

        // An unmapped *reference*: already erased, or never registered. Audit
        // the request against the reference given, so a repeat stays provable.
        // An unmapped *identifier* has no reference to audit against and is
        // handled below, because what it needs is a tombstone rather than a row.
        if targets.is_empty()
            && let Subject::Reference(reference) = subject
        {
            targets.push(reference.clone());
        }

        // Delete and audit atomically: an audit row without the deletion would
        // claim an erasure that did not happen, and a deletion without the audit
        // row would leave it unprovable.
        let mut deleted = 0;
        for target in &targets {
            deleted += sqlx::query("DELETE FROM meterstore_subject_map WHERE subject_ref = $1")
                .bind(target.as_str())
                .execute(&mut *tx)
                .await
                .map_err(pg)?
                .rows_affected();
        }

        // A repeat request keeps the first erasure's timestamp — that is when
        // the linkage actually died — but must not blank an existing tombstone,
        // which would quietly re-open re-registration.
        let mut records = Vec::with_capacity(targets.len());
        for target in targets {
            sqlx::query(
                r#"INSERT INTO meterstore_erasures
                       (subject_ref, erased_at, reason, actor, trigger, natural_id_hmac,
                        hmac_key_id)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (subject_ref) DO UPDATE
                       SET natural_id_hmac =
                           COALESCE(meterstore_erasures.natural_id_hmac, EXCLUDED.natural_id_hmac),
                           hmac_key_id =
                           COALESCE(meterstore_erasures.hmac_key_id, EXCLUDED.hmac_key_id)"#,
            )
            .bind(target.as_str())
            .bind(now)
            .bind(reason)
            .bind(actor)
            .bind(trigger.as_str())
            .bind(tombstone.as_deref())
            .bind(writing_key.as_deref())
            .execute(&mut *tx)
            .await
            .map_err(pg)?;

            records.push(ErasureRecord {
                subject: Some(target),
                erased_at: now,
                reason: reason.to_string(),
                actor: actor.to_string(),
                trigger,
                lifted: None,
            });
        }

        if records.is_empty() {
            // An identifier with no mapping at all. The request is still worth
            // honouring — its other half is *do not start* — but only a
            // suppression key can carry that forward, and the row is written at
            // most once so a repeated request cannot grow the table.
            match &tombstone {
                Some(hmac) => {
                    sqlx::query(
                        r#"INSERT INTO meterstore_erasures
                               (subject_ref, erased_at, reason, actor, trigger,
                                natural_id_hmac, hmac_key_id)
                           SELECT NULL, $1, $2, $3, $4, $5, $7
                            WHERE NOT EXISTS (
                                      SELECT 1 FROM meterstore_erasures
                                       WHERE natural_id_hmac = ANY($6))"#,
                    )
                    .bind(now)
                    .bind(reason)
                    .bind(actor)
                    .bind(trigger.as_str())
                    .bind(hmac.as_slice())
                    .bind(&ring)
                    .bind(writing_key.as_deref())
                    .execute(&mut *tx)
                    .await
                    .map_err(pg)?;

                    warn!(
                        actor,
                        "erasure requested for an identifier with no mapping: suppressed \
                         so it cannot be registered later"
                    );
                    records.push(ErasureRecord {
                        subject: None,
                        erased_at: now,
                        reason: reason.to_string(),
                        actor: actor.to_string(),
                        trigger,
                        lifted: None,
                    });
                }
                None => warn!(
                    actor,
                    "erasure requested for an identifier with no mapping and no \
                     suppression key configured: nothing was recorded, and a later \
                     registration of it cannot be refused"
                ),
            }
        }

        if deleted == 0 {
            // Already erased, or never registered. Recording it either way keeps
            // a repeated request auditable rather than silently successful.
            warn!("erasure requested for a subject with no live linkage");
        } else {
            info!(epochs = deleted, actor, "subject linkage destroyed");
        }

        Ok((records, deleted > 0))
    }

    /// The § 60 Abs. 6 sweep: destroy every linkage whose collection year ended
    /// before `cutoff`.
    ///
    /// # This looks at no readings at all
    ///
    /// The duty is on *"der jeweilige Messwert"* — a value's own collection
    /// year — so what comes due is a `(subject, epoch)` pair, and the epoch is
    /// recorded on the mapping row. The sweep is therefore pure registry
    /// maintenance: one `DELETE … WHERE epoch < …` against an indexed column,
    /// with no scan of any table, no dependence on which rows a session can see,
    /// and no way for a restricted read mode to make it destroy a live subject.
    ///
    /// An earlier design keyed it to the *latest* reading a subject explained.
    /// That was wrong in both directions at once: a subject still being metered
    /// kept its decade-old values attributable for as long as it stayed
    /// connected, and the cutoff came from a query — so a sweep run through a
    /// session that could not see the hot window would find a live customer's
    /// last reading years old and erase it, irreversibly.
    ///
    /// Idempotent and **resumable**: an epoch already erased has no mapping row
    /// left to delete, so a re-run writes no second audit row and counts
    /// nothing — and an interrupted sweep is completed by running it again.
    ///
    /// # Why this is batched, and why it does not return the references
    ///
    /// This is the only operation in the crate whose row count is unbounded by
    /// construction: a deployment's whole 2021 comes due on one January morning,
    /// which at a metering operator's scale is millions of mapping rows. It ran
    /// as a single `DELETE … RETURNING` inside one transaction, materialised
    /// every reference, and built one [`ErasureRecord`] per row — against a
    /// memory budget the rest of the crate holds *by construction* rather than by
    /// tuning, and inside the maintenance cycle that is otherwise bounded so a
    /// store which has been down for a month catches up over several cycles.
    ///
    /// So it deletes in bounded batches, each its own transaction.
    /// Peak memory is the batch, the lock is held for a batch, and a crash
    /// leaves the epochs already swept durably swept.
    ///
    /// It returns a **summary** rather than the references, because the audit
    /// trail this sweep writes is durable in `meterstore_erasures` before the
    /// call returns: handing back a row per erasure duplicated durable state
    /// into an unbounded `Vec` that the caller almost always only counted.
    /// [`SubjectRegistry::erasures`] reads the trail, with the limit this
    /// deliberately no longer needs.
    pub async fn expire_epochs_before(
        &self,
        cutoff: OffsetDateTime,
        reason: &str,
        actor: &str,
        now: OffsetDateTime,
    ) -> Result<SweepOutcome> {
        if reason.trim().is_empty() {
            return Err(Error::config("erasure needs a reason for the audit trail"));
        }
        // An epoch is due once the whole year is behind the cutoff: the year
        // containing the cutoff still holds values inside their period.
        let due_before = sweep_boundary(cutoff);

        let mut swept = SweepOutcome {
            subjects: 0,
            epochs: Vec::new(),
            batches: 0,
            swept_at: now,
        };
        let mut epochs = std::collections::BTreeSet::new();

        loop {
            // `ctid IN (… LIMIT …)` rather than `DELETE … LIMIT`, which
            // PostgreSQL does not have. The subquery picks the batch under the
            // same index the predicate uses; the delete then takes row locks for
            // exactly those.
            let mut tx = self.pool.begin().await.map_err(pg)?;
            let due = sqlx::query_as::<_, (String, i32)>(
                r#"DELETE FROM meterstore_subject_map
                    WHERE ctid IN (
                        SELECT ctid FROM meterstore_subject_map
                         WHERE epoch < $1
                         LIMIT $2
                    )
                RETURNING subject_ref, epoch"#,
            )
            .bind(due_before)
            .bind(SWEEP_BATCH)
            .fetch_all(&mut *tx)
            .await
            .map_err(pg)?;

            if due.is_empty() {
                // Nothing left. Roll back rather than commit an empty
                // transaction, and stop.
                drop(tx);
                break;
            }

            // No tombstone. Suppression exists so an Article 17 erasure survives
            // a broker replay; a retention expiry is not a request to stop
            // processing, and a subject whose 2020 epoch expired must still be
            // registrable for 2027.
            let refs: Vec<&str> = due
                .iter()
                .map(|(reference, _)| reference.as_str())
                .collect();
            sqlx::query(
                r#"INSERT INTO meterstore_erasures
                       (subject_ref, erased_at, reason, actor, trigger, natural_id_hmac)
                   SELECT reference, $2, $3, $4, 'retention', NULL
                     FROM unnest($1::text[]) AS reference
                   ON CONFLICT (subject_ref) DO NOTHING"#,
            )
            .bind(&refs)
            .bind(now)
            .bind(reason)
            .bind(actor)
            .execute(&mut *tx)
            .await
            .map_err(pg)?;
            tx.commit().await.map_err(pg)?;

            let count = due.len();
            epochs.extend(due.into_iter().map(|(_, epoch)| epoch));
            swept.subjects += count as u64;
            swept.batches += 1;

            // Report per batch rather than once at the end, so an interrupted
            // sweep still accounts for what it destroyed.
            crate::observe::metrics().subjects_erased.add(
                count as u64,
                &crate::observe::erasure_trigger(ErasureTrigger::Retention),
            );

            if count < SWEEP_BATCH as usize {
                break;
            }
        }

        swept.epochs = epochs.into_iter().collect();
        if swept.subjects > 0 {
            warn!(
                subjects = swept.subjects,
                batches = swept.batches,
                epochs = ?swept.epochs,
                %cutoff,
                "retention sweep destroyed linkages past the statutory ceiling"
            );
        }
        Ok(swept)
    }

    /// Whether an identifier is on the suppression list.
    ///
    /// Checked against every key in the ring, so an erasure recorded under a
    /// retired key still answers `true`.
    ///
    /// Always `false` without a configured key, because there is then nothing to
    /// check against — not because the identifier was never erased.
    pub async fn is_suppressed(&self, natural_id: &str) -> Result<bool> {
        if !self.suppresses_reregistration() {
            return Ok(false);
        }
        let mut conn = self.pool.acquire().await.map_err(pg)?;
        self.suppressed_on(&mut conn, natural_id).await
    }

    /// Remove an identifier from the suppression list, and record that it
    /// happened.
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
    /// It reverses a compliance decision, so it is **itself audited**: `actor`,
    /// `reason` and the instant are written onto the erasure rows they concern
    /// and come back as [`ErasureRecord::lifted`]. The erasure row survives, so
    /// erase → lift → re-register stays reviewable with the middle step attributed
    /// rather than inferred.
    ///
    /// Every key in the ring is cleared, so an identifier tombstoned under a
    /// retired key is genuinely liftable rather than half-lifted.
    pub async fn lift_suppression(
        &self,
        natural_id: &str,
        reason: &str,
        actor: &str,
        now: OffsetDateTime,
    ) -> Result<bool> {
        if reason.trim().is_empty() {
            return Err(Error::config(
                "lifting a suppression needs a reason: it reverses a compliance \
                 action and must not be an anonymous edit",
            ));
        }
        let tombstones = self.tombstones(natural_id);
        if tombstones.is_empty() {
            return Err(Error::config(
                "no suppression key is configured, so there is no suppression to lift",
            ));
        }

        let lifted = sqlx::query(
            r#"UPDATE meterstore_erasures
                  SET natural_id_hmac = NULL,
                      hmac_key_id     = NULL,
                      lifted_at       = $2,
                      lifted_by       = $3,
                      lift_reason     = $4
                WHERE natural_id_hmac = ANY($1)"#,
        )
        .bind(&tombstones)
        .bind(now)
        .bind(actor)
        .bind(reason)
        .execute(&self.pool)
        .await
        .map_err(pg)?
        .rows_affected();

        if lifted > 0 {
            warn!(actor, reason, rows = lifted, "erasure suppression lifted");
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

    /// The audit trail.
    ///
    /// Most recent first, narrowed by [`ErasureQuery`]. A record whose
    /// [`subject`](ErasureRecord::subject) is `None` is a pre-emptive
    /// suppression: a request that named an identifier this deployment held no
    /// mapping for, recorded so the refusal that follows is explicable.
    pub async fn erasures(&self, query: &ErasureQuery) -> Result<Vec<ErasureRecord>> {
        query.validate()?;

        // Bound positionally with `IS NULL OR` rather than by assembling a
        // predicate, so the statement text is fixed and every caller value stays
        // a parameter. The planner sees through the constant-null tests.
        let rows = sqlx::query_as::<
            _,
            (
                Option<String>,
                OffsetDateTime,
                String,
                String,
                String,
                Option<OffsetDateTime>,
                Option<String>,
                Option<String>,
            ),
        >(
            r#"SELECT subject_ref, erased_at, reason, actor, trigger,
                      lifted_at, lifted_by, lift_reason
                 FROM meterstore_erasures
                WHERE ($1::timestamptz IS NULL OR erased_at >= $1)
                  AND ($2::timestamptz IS NULL OR erased_at <  $2)
                  AND ($3::text        IS NULL OR trigger    = $3)
                ORDER BY erased_at DESC, id DESC
                LIMIT $4"#,
        )
        .bind(query.since)
        .bind(query.until)
        .bind(query.trigger.map(ErasureTrigger::as_str))
        .bind(query.limit)
        .fetch_all(&self.pool)
        .await
        .map_err(pg)?;

        rows.into_iter()
            .map(
                |(
                    subject,
                    erased_at,
                    reason,
                    actor,
                    trigger,
                    lifted_at,
                    lifted_by,
                    lift_reason,
                )| {
                    Ok(ErasureRecord {
                        subject: subject.map(SubjectRef::new).transpose()?,
                        erased_at,
                        reason,
                        actor,
                        trigger: trigger.parse()?,
                        // All three columns move together — a database
                        // constraint says so — so one being present is enough
                        // to read the other two.
                        lifted: lifted_at.map(|at| SuppressionLift {
                            at,
                            actor: lifted_by.unwrap_or_default(),
                            reason: lift_reason.unwrap_or_default(),
                        }),
                    })
                },
            )
            .collect()
    }
}

/// Which part of the audit trail to read.
///
/// A trail is evidence, and evidence is asked for by **period** and by **duty**:
/// § 60 Abs. 6 and Article 17 have different legal bases and a regulator asks
/// about them separately.
///
/// ```rust
/// use meterstore::{ErasureQuery, ErasureTrigger};
/// use time::macros::datetime;
///
/// // "Prove the retention sweep ran in the third quarter."
/// let q3 = ErasureQuery::new()
///     .since(datetime!(2026-07-01 0:00 UTC))
///     .until(datetime!(2026-10-01 0:00 UTC))
///     .trigger(ErasureTrigger::Retention)
///     .limit(1_000);
/// assert_eq!(q3.limit_value(), 1_000);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErasureQuery {
    since: Option<OffsetDateTime>,
    until: Option<OffsetDateTime>,
    trigger: Option<ErasureTrigger>,
    limit: i64,
}

/// How many mapping rows one retention-sweep transaction destroys.
///
/// The sweep is the only operation here whose row count is unbounded by
/// construction — a deployment's whole 2021 comes due on one January morning —
/// so it is the only one that needs a batch size at all. Large enough that a
/// realistic sweep is a handful of round trips, small enough that the lock on
/// `meterstore_subject_map` is never held long and peak memory is a batch.
const SWEEP_BATCH: i64 = 10_000;

/// What a retention sweep destroyed.
///
/// A summary rather than a row per subject. The sweep writes its audit trail to
/// `meterstore_erasures` before it returns, so handing back an
/// [`ErasureRecord`] each would duplicate durable state into an unbounded `Vec`
/// — and the epochs it names are the *years* swept, of which there are a
/// handful however many subjects they covered.
///
/// [`SubjectRegistry::erasures`] reads the trail itself, which is where a
/// caller that wants the individual references should look.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepOutcome {
    /// How many `(subject, epoch)` linkages were destroyed.
    pub subjects: u64,
    /// The collection years swept, ascending. Bounded by the number of calendar
    /// years past the ceiling, not by the number of subjects.
    pub epochs: Vec<i32>,
    /// How many transactions it took.
    ///
    /// The batch size is fixed and internal — a caller cannot tune it, and does
    /// not need to: what matters is that the sweep is bounded, not by how much.
    ///
    /// Reported because an interrupted sweep is resumable: a crash leaves the
    /// batches already committed durably swept, and re-running finishes the job.
    pub batches: usize,
    /// The `now` the caller passed, which is what the audit rows carry.
    pub swept_at: OffsetDateTime,
}

impl SweepOutcome {
    /// Whether the sweep found nothing due.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.subjects == 0
    }
}

/// How many rows an unnarrowed [`ErasureQuery`] returns.
///
/// A page rather than everything: the trail is append-only and outlives what it
/// describes, so a deployment reaching its retention ceiling has one row per
/// subject per year in it, and a default that read the lot would be a default
/// that eventually runs a server out of memory.
pub const DEFAULT_ERASURE_LIMIT: i64 = 100;

impl Default for ErasureQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl ErasureQuery {
    /// The most recent [`DEFAULT_ERASURE_LIMIT`] rows, whatever they are.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            since: None,
            until: None,
            trigger: None,
            limit: DEFAULT_ERASURE_LIMIT,
        }
    }

    /// Only erasures at or after this instant.
    #[must_use]
    pub const fn since(mut self, since: OffsetDateTime) -> Self {
        self.since = Some(since);
        self
    }

    /// Only erasures strictly before this instant.
    ///
    /// Half-open, like every other range in this crate, so consecutive periods
    /// tile without double-counting the row on the boundary.
    #[must_use]
    pub const fn until(mut self, until: OffsetDateTime) -> Self {
        self.until = Some(until);
        self
    }

    /// Only erasures discharging one duty.
    #[must_use]
    pub const fn trigger(mut self, trigger: ErasureTrigger) -> Self {
        self.trigger = Some(trigger);
        self
    }

    /// At most this many rows.
    #[must_use]
    pub const fn limit(mut self, limit: i64) -> Self {
        self.limit = limit;
        self
    }

    /// The row cap this query will apply.
    #[must_use]
    pub const fn limit_value(self) -> i64 {
        self.limit
    }

    /// Refuse a query that cannot mean what it says.
    fn validate(self) -> Result<()> {
        if self.limit <= 0 {
            return Err(Error::config(format!(
                "an erasure query's limit is a row count and must be positive; got {}",
                self.limit
            )));
        }
        if let (Some(since), Some(until)) = (self.since, self.until)
            && until <= since
        {
            return Err(Error::config(format!(
                "an erasure query's period is half-open `[since, until)`, so {until} \
                 does not follow {since}: as written it selects nothing, which reads \
                 as \"nothing was erased\""
            )));
        }
        Ok(())
    }
}

/// What an erasure was asked for by.
///
/// The two entry points to one mechanism. A reference is what a store holds on a
/// row; a natural identifier is what an Article 17 request names, and the
/// difference matters only until the identifier behind a reference is resolved.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Subject<'a> {
    /// A pseudonymous reference. Resolved to its identifier, then treated the
    /// same — so erasing by reference still reaches every epoch.
    Reference(&'a SubjectRef),
    /// The natural identifier itself.
    Natural(&'a str),
}

/// When personal metering values stop being personal.
///
/// Two shapes, because § 60 Abs. 6 MsbG has two triggers and only one of them is
/// a clock. The Messstellenbetreiber must erase or anonymise personenbezogene
/// Messwerte *as soon as* storing them is no longer necessary, *"spätestens
/// jedoch nach drei Jahren ab dem Schluss des Kalenderjahres, in dem der
/// jeweilige Messwert erhoben wurde"*. Three years is the **ceiling**; the
/// operative trigger is earlier and is a business decision.
///
/// A policy turns the instant a sweep runs at into the cutoff it applies to the
/// **collection year on each mapping row** — no reading is consulted, so what
/// comes due is a calendar fact. See
/// [`MeterCatalog::anonymise_before`](crate::MeterCatalog::anonymise_before).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    /// The statutory shape: `years` full calendar years after the end of the one
    /// a value was collected in.
    ///
    /// **Not `now - years`**, which is the tempting spelling and is wrong by up
    /// to a year in the direction that erases data still within its period. The
    /// deadline for a value collected on 2 January 2025 is 31 December 2028, not
    /// 2 January 2028, because the clock starts at the *Schluss des
    /// Kalenderjahres*. The year is Berlin's, because the statute is.
    CalendarYears(u32),
    /// A rolling window — the earlier *"nicht mehr erforderlich"* trigger, where
    /// a deployment has decided what that means.
    ///
    /// # It expires whole years, and a window shorter than one is not what it looks like
    ///
    /// The sweep deletes mapping rows by `epoch`, and an epoch is a **year** —
    /// that is [`retention_epoch`], and it is a year on purpose, because § 60
    /// Abs. 6's clock starts at the *Schluss des Kalenderjahres*. So this window
    /// does not select values by age; it selects the **calendar years that ended
    /// more than `window` ago**, and everything inside the current year and the
    /// most recent complete one stays whatever the window says:
    ///
    /// ```rust
    /// use meterstore::Retention;
    /// use time::{Duration, macros::datetime};
    ///
    /// // Thirty days, swept on 15 January 2028. Only epochs before 2027 go:
    /// // a value collected on 2 January 2027 is a year old and is kept.
    /// let cutoff = Retention::Rolling(Duration::days(30)).cutoff(datetime!(2028-01-15 00:00 UTC));
    /// assert_eq!(cutoff, datetime!(2027-12-16 00:00 UTC));
    /// ```
    ///
    /// The error is always in the direction that **keeps** data — nothing is ever
    /// erased before `window` has elapsed, which is the half that matters for an
    /// irreversible operation. But a deployment that sets `Rolling(30 days)`
    /// expecting a thirty-day linkage lifetime does not get one, and nothing at
    /// runtime will say so.
    ///
    /// Reaching below the year needs a finer column on the mapping row than the
    /// one § 60 Abs. 6 is written in. For a genuine sub-year policy, erase by
    /// request ([`SubjectRegistry::erase`]) on the deployment's own schedule
    /// instead.
    ///
    /// [`SubjectRegistry::erase`]: crate::erasure::SubjectRegistry::erase
    Rolling(time::Duration),
}

impl Retention {
    /// The instant a sweep run at `now` treats as the ceiling: an epoch is due
    /// once its whole calendar year lies before this.
    ///
    /// ```rust
    /// use meterstore::Retention;
    /// use time::macros::datetime;
    ///
    /// // Values collected in 2024 come due at the end of 2027, so a sweep any
    /// // time in 2028 covers them — and leaves 2025's alone until 2029.
    /// let cutoff = Retention::CalendarYears(3).cutoff(datetime!(2028-06-01 00:00 UTC));
    /// assert_eq!(cutoff, datetime!(2024-12-31 23:00 UTC)); // 2025-01-01 Berlin
    /// ```
    /// # Both arms saturate towards *nothing is due*
    ///
    /// A period wider than the calendar can express means "keep everything". The
    /// other reading — subtract nothing, so the cutoff is *this* year — makes
    /// every subject in the store due at once, and erasure has no undo.
    ///
    /// ```rust
    /// use meterstore::Retention;
    /// use time::macros::datetime;
    ///
    /// let now = datetime!(2028-06-01 00:00 UTC);
    /// // A period nothing can be older than: nothing comes due.
    /// assert!(Retention::CalendarYears(u32::MAX).cutoff(now) < datetime!(1970-01-01 00:00 UTC));
    /// assert!(Retention::Rolling(time::Duration::MAX).cutoff(now) < now);
    /// ```
    #[must_use]
    pub fn cutoff(self, now: OffsetDateTime) -> OffsetDateTime {
        match self {
            Self::CalendarYears(years) => {
                // Saturating in the direction that keeps data, and clamped to a
                // year the calendar can answer for. `Date::MIN` is `-9999-01-01`,
                // whose Berlin midnight is an hour *before* it in UTC — out of
                // range, and `metering::calendar::year_start_utc` panics rather
                // than folding it away. One year in is the earliest that
                // converts.
                let year = metering::calendar::local_year(now)
                    .saturating_sub(i32::try_from(years).unwrap_or(i32::MAX))
                    .max(EARLIEST_CUTOFF_YEAR);
                metering::calendar::year_start_utc(year)
            }
            // `checked_sub` for the same reason, onto the same floor, so both
            // arms of an unrepresentable period give the same answer.
            Self::Rolling(window) => now
                .checked_sub(window)
                .unwrap_or_else(|| metering::calendar::year_start_utc(EARLIEST_CUTOFF_YEAR)),
        }
    }
}

/// The earliest year [`Retention::cutoff`] will name.
///
/// `time::Date::MIN` is `-9999-01-01`, and Berlin's midnight on it is an hour
/// earlier still in UTC — outside what `OffsetDateTime` can hold, which
/// `metering::calendar::year_start_utc` surfaces as a panic rather than folding
/// away. One year in converts, and is far enough before any metering value that
/// a cutoff here means nothing is due.
const EARLIEST_CUTOFF_YEAR: i32 = -9998;

/// The retention epoch a reading falls in: the **balancing year** of the day it
/// is settled on.
///
/// The one rule for the epoch, read by both sides — the reference
/// [`SubjectRegistry::register`] mints and the check the write path applies are
/// this same call, which is why `sparte` is a parameter.
///
/// Local, because the statute is: *"der Schluss des Kalenderjahres"* is a German
/// calendar year, so an interval starting `2026-12-31T23:00Z` is already 2027 in
/// Berlin. The year rather than the month, because that is the granularity
/// § 60 Abs. 6 states its ceiling in.
///
/// # Why the commodity is part of it
///
/// For everything but gas the balancing year *is* the local calendar year. The
/// gas day runs 06:00 to 06:00, so the six hours after midnight on 1 January are
/// settled on the previous year's last Gastag: an interval at `2026-01-01T00:00Z`
/// is Gastag 2025-12-31, in the December Bilanzierungsmonat, invoiced with 2025 —
/// epoch 2025.
///
/// That keeps a delivery whole. A Gastag spanning New Year has intervals in two
/// local years but one balancing year, so it takes one reference; on the calendar
/// rule an MSCONS Lastgang for that day would have to be split. Erasing those six
/// hours with 2025 is a year earlier than the calendar reading of § 60 Abs. 6
/// requires, which is the compliant direction — three years is a **ceiling**.
///
/// ```rust
/// use meterstore::erasure::retention_epoch;
/// use metering::interval::Sparte;
/// use time::macros::datetime;
///
/// // 01:00 Berlin on New Year's Day: 2026 on the calendar, and still the
/// // Gastag — and so the epoch — of 2025.
/// let at = datetime!(2026-01-01 0:00 UTC);
/// assert_eq!(retention_epoch(at, Sparte::Strom), 2026);
/// assert_eq!(retention_epoch(at, Sparte::Gas), 2025);
/// ```
#[must_use]
pub fn retention_epoch(at: OffsetDateTime, sparte: Sparte) -> i32 {
    crate::planner::balancing_day(at, sparte).year()
}

/// The first epoch a sweep at `cutoff` must still keep: everything strictly
/// below it is due.
///
/// The **calendar** year of the cutoff, and deliberately not
/// [`retention_epoch`] — which cannot be used here, because the `epoch` column
/// is one integer and the registry is shared by tables of every commodity, so
/// there is no `sparte` to ask with.
///
/// It does not need one. The two rules differ over six hours a year, and only in
/// the direction that expires a gas epoch with the calendar year it was mostly
/// collected in. Reading the cutoff on the gas boundary instead would push
/// *every* epoch — electricity included — a whole year later, which is the
/// direction that keeps personal data past its ceiling.
fn sweep_boundary(cutoff: OffsetDateTime) -> i32 {
    metering::calendar::local_year(cutoff)
}

/// Refuse an identifier that names nobody.
fn check_natural_id(natural_id: &str) -> Result<()> {
    if natural_id.trim().is_empty() {
        return Err(Error::config("natural identifier must not be empty"));
    }
    Ok(())
}

/// The reference registered for `(natural_id, epoch)`, on a given connection.
///
/// Shared so the pooled read and the read inside a registration's transaction
/// cannot drift apart.
async fn lookup_in(
    conn: &mut sqlx::PgConnection,
    natural_id: &str,
    epoch: i32,
) -> Result<Option<SubjectRef>> {
    let found = sqlx::query_scalar::<_, String>(
        "SELECT subject_ref FROM meterstore_subject_map WHERE natural_id = $1 AND epoch = $2",
    )
    .bind(natural_id)
    .bind(epoch)
    .fetch_optional(conn)
    .await
    .map_err(pg)?;

    found.map(SubjectRef::new).transpose()
}

/// Serialise everything that can create or destroy a mapping for one identifier.
///
/// Registration and erasure race in a way row locks cannot settle, because the
/// row registration is about does not exist yet: `register` sees no mapping and
/// no tombstone, `erase` commits both, and `register` then inserts a live
/// mapping for a subject the audit trail says was erased. Nothing downstream can
/// tell, which is the shape of failure this module exists to rule out.
///
/// A transaction-scoped advisory lock is the cheap answer — no table, no row to
/// exist first, released by commit or rollback either way. Both paths take it
/// **before** any row lock so the order is total and they cannot deadlock.
///
/// The key is the first 64 bits of SHA-256 over the identifier rather than
/// PostgreSQL's `hashtext`, which is an internal function with no compatibility
/// promise. A collision costs two unrelated identifiers a moment of serialised
/// registration and nothing else.
async fn lock_natural_id(conn: &mut sqlx::PgConnection, natural_id: &str) -> Result<()> {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(natural_id.as_bytes());
    let key = i64::from_be_bytes(digest[..8].try_into().expect("SHA-256 gives 32 bytes"));

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(conn)
        .await
        .map_err(pg)?;
    Ok(())
}

/// HMAC-SHA256 of an identifier under one key.
///
/// The tombstone primitive, shared by the writing key and every retired one so a
/// tag written under a key is recognised by the same computation that wrote it.
fn mac(key: &[u8], natural_id: &str) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(natural_id.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Domain separator for a key's identifier, so it can never collide with a
/// tombstone tag written under the same key.
const KEY_ID_DOMAIN: &str = "meterstore.erasure-key-id.v1";

/// How many bytes of the derived tag identify a key.
///
/// Eight, because this only has to tell the keys of one ring apart and be
/// recognisable in a log line. Collisions between two keys in one ring would
/// report an orphan as present; at 2^-64 per pair that is not the failure worth
/// designing against.
const KEY_ID_BYTES: usize = 8;

/// A stable, public name for a key, derived from the key itself.
///
/// Not operator-chosen, and that is the point: the check exists because
/// operators mismanage the ring, so an identifier they could mislabel would fail
/// in exactly the cases it is for. Derived, it cannot disagree with the key it
/// names.
///
/// # What it gives away
///
/// It is a key check value, so somebody holding a *candidate* key can confirm
/// it. That is no worse than the tombstones themselves, which fall to the same
/// guess plus one identifier — and identifiers here are enumerable, an 11-digit
/// Marktlokations-ID. Both arguments rest on [`MIN_ERASURE_SECRET_BYTES`] of
/// real entropy, which is what the length floor is there to ask for.
fn key_id(key: &[u8]) -> Vec<u8> {
    mac(key, KEY_ID_DOMAIN)[..KEY_ID_BYTES].to_vec()
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

    /// A token of the width a pseudonym has to be, for the shape tests.
    const TOKEN: &str = "9f3c1d2e4b5a60718293a4b5c6";

    #[test]
    fn a_reference_must_not_be_empty() {
        assert!(SubjectRef::new("").is_err());
        assert!(SubjectRef::new("   ").is_err());
        assert!(SubjectRef::new(format!("s2026_{TOKEN}")).is_ok());
    }

    #[test]
    fn a_reference_must_name_its_retention_epoch() {
        // Without the epoch a reference cannot be checked against the year of
        // the reading it is attached to, and the sweep loses its whole basis.
        assert!(SubjectRef::new(format!("sub_{TOKEN}")).is_err());
        assert!(SubjectRef::new(format!("s_{TOKEN}")).is_err());
        assert!(SubjectRef::new("s2026_").is_err());
        assert!(SubjectRef::new(format!("2026_{TOKEN}")).is_err());
        assert_eq!(
            SubjectRef::new(format!("s2026_{TOKEN}"))
                .unwrap()
                .epoch()
                .unwrap(),
            2026
        );
        assert_eq!(
            SubjectRef::new(format!("s-1_{TOKEN}"))
                .unwrap()
                .epoch()
                .unwrap(),
            -1
        );
    }

    #[test]
    fn a_reference_whose_token_is_too_short_to_be_a_pseudonym_is_refused() {
        // The failure that actually happens: a pipeline with no reference to
        // hand puts something short and meaningful in the column. It resolves
        // nowhere, but the shape is the only thing that can catch it — a
        // customer number in this column is a re-identification path that
        // survives erasure and looks exactly like a pseudonym in every table
        // that stores it.
        assert!(SubjectRef::new("s2026_abc").is_err());
        assert!(SubjectRef::new("s2026_4821").is_err());
        assert!(SubjectRef::new("s2026_deadbeef").is_err());

        // One character short and one character long, so the floor is where it
        // says it is rather than nearby.
        let short = "a".repeat(MIN_REFERENCE_TOKEN_CHARS - 1);
        let just = "a".repeat(MIN_REFERENCE_TOKEN_CHARS);
        assert!(SubjectRef::new(format!("s2026_{short}")).is_err());
        assert!(SubjectRef::new(format!("s2026_{just}")).is_ok());
    }

    #[test]
    fn the_encodings_a_foreign_minter_uses_are_all_accepted() {
        // The floor is on the shape, not on this crate having minted it: a
        // deployment whose references come from another service must be able to
        // store them.
        for token in [
            "0123456789abcdef0123456789abcdef", // hex, what `mint` produces
            "3f8a1c02-7d4e-4b19-9a3e-15c7d2e6b408", // an RFC 4122 UUID
            "T3JwaGV1cy1XYXNILUhlcmU",          // base64url, 22 characters
            "a.b-c_d.e-f_g.h-i_j.k-l",          // the punctuation, all of it
        ] {
            assert!(
                SubjectRef::new(format!("s2026_{token}")).is_ok(),
                "{token} should be a well-formed reference"
            );
        }

        // And what is outside the alphabet stays outside it. A reference travels
        // through SQL, Parquet and object-store paths.
        for token in [
            "0123456789abcdef 0123456789abcde",
            "0123456789abcdef/0123456789abcde",
            "0123456789abcdef'0123456789abcde",
        ] {
            assert!(
                SubjectRef::new(format!("s2026_{token}")).is_err(),
                "{token:?} should be refused"
            );
        }
    }

    #[test]
    fn every_minted_reference_is_one_new_would_accept() {
        // `mint` and `new` are two descriptions of one shape, and a mint the
        // constructor refused would be a reference nothing could read back.
        for epoch in [-1, 0, 1970, 2026, 9999] {
            let minted = SubjectRef::mint(epoch);
            assert_eq!(minted.epoch().unwrap(), epoch);
            assert!(SubjectRef::new(minted.as_str()).is_ok(), "{minted}");
        }
    }

    #[test]
    fn an_erasure_trigger_round_trips_through_its_stored_spelling() {
        // The audit column and the metric attribute are the same string, so a
        // value written by one must be readable by the other.
        for trigger in [ErasureTrigger::Request, ErasureTrigger::Retention] {
            assert_eq!(trigger.as_str().parse::<ErasureTrigger>().unwrap(), trigger);
            assert_eq!(trigger.to_string(), trigger.as_str());
        }
        assert!("sweep".parse::<ErasureTrigger>().is_err());
    }

    #[test]
    fn an_erasure_query_that_selects_nothing_is_refused_rather_than_empty() {
        use time::macros::datetime;

        // Both mistakes read as "nothing was erased", which is the one answer an
        // audit query must never give by accident.
        assert!(ErasureQuery::new().limit(0).validate().is_err());
        assert!(ErasureQuery::new().limit(-1).validate().is_err());
        assert!(
            ErasureQuery::new()
                .since(datetime!(2026-10-01 0:00 UTC))
                .until(datetime!(2026-07-01 0:00 UTC))
                .validate()
                .is_err()
        );
        // An empty period is the same mistake spelled with one instant.
        assert!(
            ErasureQuery::new()
                .since(datetime!(2026-07-01 0:00 UTC))
                .until(datetime!(2026-07-01 0:00 UTC))
                .validate()
                .is_err()
        );
        assert!(
            ErasureQuery::new()
                .since(datetime!(2026-07-01 0:00 UTC))
                .until(datetime!(2026-10-01 0:00 UTC))
                .validate()
                .is_ok()
        );
        assert_eq!(ErasureQuery::new().limit_value(), DEFAULT_ERASURE_LIMIT);
    }

    #[tokio::test]
    async fn a_key_ring_reads_with_every_key_and_writes_with_the_first() {
        // Rotation is additive because a tombstone cannot be re-keyed: the
        // identifier it was computed from was destroyed in the same transaction
        // that wrote it. So the current key writes and every key reads.
        let current = [1u8; 32];
        let retired = [2u8; 32];
        let ring = SubjectRegistry::with_erasure_keys(lazy_pool(), &[&current, &retired]).unwrap();
        let only_current = SubjectRegistry::with_erasure_secret(lazy_pool(), &current).unwrap();
        let only_retired = SubjectRegistry::with_erasure_secret(lazy_pool(), &retired).unwrap();

        assert_eq!(ring.erasure_key_count(), 2);
        assert_eq!(
            ring.tombstone("41373559241"),
            only_current.tombstone("41373559241"),
            "new tombstones are written under the first key"
        );
        assert_eq!(
            ring.tombstones("41373559241"),
            vec![
                only_current.tombstone("41373559241").unwrap(),
                only_retired.tombstone("41373559241").unwrap(),
            ],
            "and a lookup covers both"
        );
    }

    #[tokio::test]
    async fn an_empty_key_ring_is_refused_rather_than_read_as_no_suppression() {
        // A ring that enforces nothing while looking configured is the shape of
        // a botched rotation. `new` is how a deployment says suppression is off.
        assert!(SubjectRegistry::with_erasure_keys(lazy_pool(), &[]).is_err());
        // And a weak *retired* key is refused too: it is the older tombstones
        // that key covers, so it is exactly the ones worth inverting.
        assert!(
            SubjectRegistry::with_erasure_keys(lazy_pool(), &[&[1u8; 32][..], &[2u8; 8][..]])
                .is_err()
        );
    }

    #[test]
    fn generated_references_do_not_repeat() {
        let refs: std::collections::HashSet<_> =
            (0..1_000).map(|_| SubjectRef::mint(2026)).collect();
        assert_eq!(refs.len(), 1_000, "references must be unique");
    }

    #[test]
    fn a_generated_reference_is_not_derived_from_anything() {
        // Two calls must differ, or the reference is a function of its input and
        // erasure could be undone by recomputing it.
        assert_ne!(SubjectRef::mint(2026), SubjectRef::mint(2026));
    }

    #[test]
    fn a_minted_reference_carries_the_epoch_it_was_minted_for() {
        assert_eq!(SubjectRef::mint(2024).epoch().unwrap(), 2024);
    }

    #[test]
    fn the_retention_epoch_is_the_berlin_year() {
        use time::macros::datetime;
        // 23:00 UTC on New Year's Eve is already next year in Berlin, and the
        // statute counts German calendar years.
        for sparte in [Sparte::Strom, Sparte::Gas] {
            assert_eq!(
                retention_epoch(datetime!(2026-12-31 22:59 UTC), sparte),
                2026
            );
        }
        assert_eq!(
            retention_epoch(datetime!(2026-12-31 23:00 UTC), Sparte::Strom),
            2027
        );
    }

    #[test]
    fn a_gas_reading_inside_the_gastag_boundary_belongs_to_the_year_it_is_balanced_in() {
        use time::macros::datetime;

        // The six hours a year the two calendars disagree. `2026-01-01T00:00Z`
        // is 01:00 on 1 January in Berlin — 2026 on the wall clock, and still
        // Gastag 2025-12-31, which is the day it is settled and invoiced on.
        //
        // This is the whole of what the sparte parameter is for. Mint from the
        // local year and check against the balancing one, and the only
        // reference the public API can produce for such a reading is one the
        // write refuses — a gas reading in that window is then unstorable.
        let at = datetime!(2026-01-01 0:00 UTC);
        assert_eq!(retention_epoch(at, Sparte::Gas), 2025);
        assert_eq!(retention_epoch(at, Sparte::Strom), 2026);

        // After 06:00 local the Gastag and the calendar agree again.
        let later = datetime!(2026-01-01 6:00 UTC); // 07:00 Berlin
        assert_eq!(retention_epoch(later, Sparte::Gas), 2026);
        assert_eq!(retention_epoch(later, Sparte::Strom), 2026);
    }

    #[test]
    fn a_rolling_window_expires_whole_years_and_never_erases_early() {
        use time::macros::datetime;

        // `Rolling` reads as "delete anything older than this"; what it can do is
        // expire the calendar years that ended more than the window ago, because
        // the mapping row carries a year and nothing finer.
        for (now, window_days, due_before) in [
            (datetime!(2028-06-01 00:00 UTC), 90i64, 2028),
            (datetime!(2028-02-01 00:00 UTC), 90, 2027),
            (datetime!(2028-01-15 00:00 UTC), 30, 2027),
            (datetime!(2028-06-01 00:00 UTC), 400, 2027),
        ] {
            let cutoff = Retention::Rolling(time::Duration::days(window_days)).cutoff(now);
            assert_eq!(
                sweep_boundary(cutoff),
                due_before,
                "Rolling({window_days}d) at {now}"
            );
        }

        // The half that must hold: an epoch is only ever swept once every value
        // it could hold is older than the window. The newest value in the newest
        // swept epoch is 31 December of `due_before - 1`, which must precede the
        // cutoff.
        for window_days in [1i64, 30, 90, 365, 400, 1_000] {
            let now = datetime!(2028-06-15 12:00 UTC);
            let cutoff = Retention::Rolling(time::Duration::days(window_days)).cutoff(now);
            let newest_swept = metering::calendar::year_start_utc(sweep_boundary(cutoff));
            assert!(
                newest_swept <= cutoff,
                "Rolling({window_days}d) would erase a value newer than the window"
            );
        }
    }

    #[test]
    fn the_sweep_boundary_is_the_calendar_year_of_the_cutoff() {
        use time::macros::datetime;

        // The registry's `epoch` column is one integer shared by every
        // commodity, so the sweep has no `sparte` to ask with — and must not
        // borrow the gas boundary, which would push every epoch a year later.
        assert_eq!(sweep_boundary(datetime!(2026-01-01 0:00 UTC)), 2026);
        assert_eq!(sweep_boundary(datetime!(2025-12-31 23:00 UTC)), 2026);
        assert_eq!(sweep_boundary(datetime!(2025-12-31 22:59 UTC)), 2025);
    }

    #[test]
    fn a_whole_gastag_shares_one_epoch_even_across_new_year() {
        use time::{Duration, macros::datetime};

        // A Gastag is the unit an MSCONS Lastgang is delivered in, and one
        // reference is stored per delivery. Gastag 2025-12-31 runs to 06:00 on
        // 1 January, so on the calendar rule its intervals would straddle two
        // epochs and the delivery could not be written at all.
        let start = datetime!(2025-12-31 5:00 UTC); // 06:00 Berlin
        let mut at = start;
        while at < start + Duration::hours(24) {
            assert_eq!(
                retention_epoch(at, Sparte::Gas),
                2025,
                "{at} left the epoch its Gastag is balanced in"
            );
            at += Duration::minutes(15);
        }
    }

    /// A pool that never connects — enough to construct a registry.
    fn lazy_pool() -> PgPool {
        PgPool::connect_lazy("postgresql://unused@localhost/unused").expect("a well-formed URL")
    }

    #[tokio::test]
    async fn the_erasure_key_never_reaches_a_log_line() {
        // A registry is a plausible thing to put in a `tracing` field or an error
        // context, and this one holds a cryptographic key.
        let registry = SubjectRegistry::with_erasure_secret(lazy_pool(), &[0xAB; 32]).unwrap();
        let shown = format!("{registry:?}");

        assert!(!shown.contains("171"), "{shown}");
        assert!(!shown.contains("ab"), "{shown}");
        assert!(shown.contains("suppression: true"), "{shown}");
        assert!(format!("{:?}", SubjectRegistry::new(lazy_pool())).contains("suppression: false"));
    }

    #[tokio::test]
    async fn a_cloned_registry_keeps_a_key_of_its_own() {
        // The key is wiped when a registry drops, and every derived session —
        // `as_of`, `scoped`, `in_own_session` — clones one. A clone sharing the
        // original's allocation would be wiped along with it, and suppression
        // would silently stop working on the session that outlived the other.
        let original = SubjectRegistry::with_erasure_secret(lazy_pool(), &[7; 32]).unwrap();
        let derived = original.clone();
        let expected = original
            .tombstone("41373559241")
            .expect("a key is configured");

        drop(original);

        assert_eq!(derived.tombstone("41373559241"), Some(expected));
    }

    #[tokio::test]
    async fn a_tombstone_is_keyed_rather_than_a_bare_hash() {
        // Meter and market-location identifiers come from small structured
        // spaces, so an unkeyed hash of one is invertible by anyone holding the
        // table. Two keys over one identifier must therefore differ.
        let a = SubjectRegistry::with_erasure_secret(lazy_pool(), &[1; 32]).unwrap();
        let b = SubjectRegistry::with_erasure_secret(lazy_pool(), &[2; 32]).unwrap();

        assert_ne!(a.tombstone("41373559241"), b.tombstone("41373559241"));
        assert_eq!(a.tombstone("41373559241"), a.tombstone("41373559241"));
        // And without a key there is no tombstone at all, rather than an unkeyed
        // one that would look like suppression and not be.
        assert!(
            SubjectRegistry::new(lazy_pool())
                .tombstone("41373559241")
                .is_none()
        );
    }

    #[test]
    fn the_statutory_ceiling_starts_at_the_end_of_the_collection_year() {
        use time::macros::datetime;

        // § 60 Abs. 6 MsbG: three years *ab dem Schluss des Kalenderjahres*. A
        // value collected on 2 January 2025 comes due on 31 December 2028, not
        // on 2 January 2028 — `now - 3 years` would erase it a year early, which
        // is the direction that destroys data still inside its retention period.
        let policy = Retention::CalendarYears(3);
        // Berlin's year boundary, so a UTC instant in the last hour of the year
        // is already the next one locally.
        assert_eq!(
            policy.cutoff(datetime!(2028-01-02 00:00 UTC)),
            datetime!(2024-12-31 23:00 UTC)
        );
        assert_eq!(
            policy.cutoff(datetime!(2028-12-31 23:00 UTC)),
            datetime!(2025-12-31 23:00 UTC)
        );

        // The cutoff does not move within a year, so a daily sweep is stable —
        // and a value's own year is either wholly due or wholly not.
        assert_eq!(
            policy.cutoff(datetime!(2028-03-01 00:00 UTC)),
            policy.cutoff(datetime!(2028-11-30 00:00 UTC)),
        );
    }

    #[test]
    fn a_rolling_window_is_measured_from_the_sweep() {
        use time::macros::datetime;

        let now = datetime!(2028-06-01 00:00 UTC);
        assert_eq!(
            Retention::Rolling(time::Duration::days(90)).cutoff(now),
            now - time::Duration::days(90)
        );
    }

    #[test]
    fn a_period_the_calendar_cannot_express_keeps_everything() {
        use time::macros::datetime;

        // The one arithmetic mistake here that cannot be undone. A retention
        // period too wide to represent has a safe reading — "keep everything" —
        // and a catastrophic one: fall back to subtracting nothing and the cutoff
        // becomes *this* year, so every subject in the store is due at once and
        // every linkage is destroyed. Both arms saturate towards keeping.
        let now = datetime!(2028-06-01 00:00 UTC);
        let epoch = datetime!(1970-01-01 00:00 UTC);

        for years in [u32::MAX, i32::MAX as u32, 100_000, 12_030, 10_000] {
            let cutoff = Retention::CalendarYears(years).cutoff(now);
            assert!(
                cutoff < epoch,
                "CalendarYears({years}) put the cutoff at {cutoff}, which makes \
                 readings due that are nowhere near the ceiling"
            );
        }
        assert!(Retention::Rolling(time::Duration::MAX).cutoff(now) < epoch);

        // And an ordinary period still works, so the guard has not swallowed it.
        assert_eq!(
            Retention::CalendarYears(3).cutoff(now),
            datetime!(2024-12-31 23:00 UTC)
        );
    }

    #[tokio::test]
    async fn a_short_erasure_key_is_refused() {
        // Short enough to brute-force is short enough to leak the identifiers the
        // suppression list exists to forget.
        assert!(SubjectRegistry::with_erasure_secret(lazy_pool(), &[0; 31]).is_err());
        assert!(SubjectRegistry::with_erasure_secret(lazy_pool(), &[0; 32]).is_ok());
    }

    #[test]
    fn an_erasure_record_carries_no_natural_identifier() {
        // The audit trail outlives the mapping, so anything personal in it would
        // survive the erasure it documents.
        let record = ErasureRecord {
            subject: Some(SubjectRef::mint(2026)),
            erased_at: OffsetDateTime::UNIX_EPOCH,
            reason: "DSAR-2026-0042".to_string(),
            actor: "privacy-team".to_string(),
            trigger: ErasureTrigger::Request,
            lifted: None,
        };
        let rendered = format!("{record:?}");
        assert!(rendered.contains("s2026_"));
        assert!(rendered.contains("DSAR-2026-0042"));
        assert!(rendered.contains("Request"));
    }

    #[test]
    fn a_market_identifier_is_not_a_pseudonym() {
        // The failure this exists for: the length floor rules out the *short*
        // identifiers and admitted the long ones, which are the more identifying.
        // A Zählpunktbezeichnung is 33 uppercase alphanumerics — it cleared both
        // the floor and the alphabet.
        let zpb = "DE0001234567890000000000000000123";
        assert_eq!(zpb.len(), 33);
        assert!(
            zpb.parse::<metering::ids::MeloId>().is_ok(),
            "fixture must be a real MeLo"
        );
        assert!(SubjectRef::new(format!("s2026_{zpb}")).is_err());

        // And the short ones stay refused, by the floor.
        assert!(SubjectRef::new("s2026_41373559241").is_err());
    }

    #[test]
    fn a_random_token_of_identifier_length_is_still_accepted() {
        // The refusal must key on parsing, not on length: a 33-character random
        // token is a perfectly good pseudonym and must not be caught by the rule
        // above.
        let token = "abcdefghijklmnopqrstuvwxyz0123456";
        assert_eq!(token.len(), 33);
        assert!(SubjectRef::new(format!("s2026_{token}")).is_ok());
    }

    #[test]
    fn an_epoch_must_be_spelled_canonically() {
        // `i32::from_str` accepts both of these, and the table's constraint is
        // textual — so they passed the Rust check and were refused by the
        // database, which is the disagreement that constraint exists to prevent.
        let token = "abcdefghijklmnopqrstuvwxyz";
        assert!(SubjectRef::new(format!("s+2026_{token}")).is_err());
        assert!(SubjectRef::new(format!("s02026_{token}")).is_err());
        assert!(SubjectRef::new(format!("s2026_{token}")).is_ok());
    }

    #[test]
    fn a_token_has_an_upper_bound_too() {
        let token = "a".repeat(MAX_REFERENCE_TOKEN_CHARS + 1);
        assert!(SubjectRef::new(format!("s2026_{token}")).is_err());
        let token = "a".repeat(MAX_REFERENCE_TOKEN_CHARS);
        assert!(SubjectRef::new(format!("s2026_{token}")).is_ok());
    }
}
