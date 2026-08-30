//! MSCONS correction versions and the scopes they are comparable within.
//!
//! The MSCONS application handbook specifies a numeric version
//! label of at least 14 digits, monotonically ascending, **assigned by the
//! network operator on a monthly basis**. The last clause is the one that bites:
//! a version is only meaningful relative to the (operator, month) that issued
//! it, so comparing versions across scopes silently picks wrong values.
//!
//! [`ScopedVersion`] makes that rule unrepresentable-if-violated: there is no
//! `Ord` impl, only [`ScopedVersion::try_cmp`], which errors on mismatch.

use std::cmp::Ordering;
use std::fmt;

use metering::ids::BdewCode;
use metering::interval::Sparte;
use time::OffsetDateTime;

use crate::error::{Error, Result};

/// The maximum value representable in the storage encoding, `Decimal128(20,0)`.
const MAX_VERSION: u128 = 10u128.pow(20) - 1;

/// Minimum digit count mandated by the MSCONS application handbook.
const MIN_DIGITS: u32 = 14;

/// An MSCONS correction version.
///
/// Ordering is deliberately **not** implemented — see [`ScopedVersion`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Version(u128);

impl Version {
    /// Construct a version, rejecting values the storage encoding cannot hold.
    ///
    /// Values with fewer than 14 digits are accepted but flagged by
    /// [`Version::is_well_formed`]: real deliveries occasionally carry short
    /// versions, and refusing to *store* them would lose data we have already
    /// received. Refusing to *compare* them would be equally wrong, since they
    /// still order correctly within a scope.
    pub fn new(value: u128) -> Result<Self> {
        if value > MAX_VERSION {
            return Err(Error::encode(
                "version",
                format!("{value} exceeds Decimal128(20,0) range"),
            ));
        }
        Ok(Self(value))
    }

    /// The raw numeric value.
    pub const fn get(self) -> u128 {
        self.0
    }

    /// Construct a version, **rejecting** anything MSCONS would not have issued.
    ///
    /// [`new`](Self::new) is deliberately permissive: a delivery that has already
    /// arrived carrying a short version is data we hold, and refusing to store it
    /// would lose it. That is the right rule for the *decode* path and the wrong
    /// one for an ingest that is choosing what to accept.
    ///
    /// This is the strict constructor for that boundary. Use it where a version
    /// comes off the wire, so a non-conformant label fails at the edge rather
    /// than entering the resolved view — where it still orders correctly within
    /// its scope, and is therefore invisible.
    pub fn mscons(value: u128) -> Result<Self> {
        let version = Self::new(value)?;
        if !version.is_well_formed() {
            return Err(Error::encode(
                "version",
                format!(
                    "{value} has {} digits; MSCONS assigns at least {MIN_DIGITS}. \
                     Use Version::new to store a short version that has already been \
                     received — this constructor is for validating one at ingest",
                    value.checked_ilog10().map_or(1, |d| d + 1),
                ),
            ));
        }
        Ok(version)
    }

    /// The version to give a delivery that **states none**, derived from when it
    /// arrived.
    ///
    /// Not every reading comes with an MSCONS version label: an SMGW push, a
    /// manual entry after a dispute, a CSV backfill. Resolution still has to
    /// order them.
    ///
    /// # Unix **milliseconds**, and neither neighbouring unit
    ///
    /// | Unit | Digits | |
    /// |---|---|---|
    /// | seconds | 10 | two writes in one second collide on `(merge key, version)` |
    /// | **milliseconds** | **13** | below the MSCONS band, and still sub-second |
    /// | microseconds | 16 | **inside** the band — outranks a stated version |
    ///
    /// MSCONS labels are ≥ 14 digits, so the smallest an operator can issue is
    /// `10_000_000_000_000` — which a millisecond timestamp does not reach until
    /// **20 November 2286**. An arrival-derived version therefore sorts below
    /// every stated one, whatever order they arrived in, for the whole of any
    /// retention period this store will see.
    ///
    /// [`is_well_formed`](Self::is_well_formed) stays `false` for these, which is
    /// what distinguishes "the operator said so" from "we assigned one".
    ///
    /// ```rust
    /// use meterstore::Version;
    /// use time::macros::datetime;
    ///
    /// let v = Version::arrival(datetime!(2026-08-25 06:00:00.123 UTC))?;
    /// assert_eq!(v.get(), 1_787_637_600_123);
    /// assert!(!v.is_well_formed(), "not an MSCONS label, and says so");
    /// assert!(v.get() < Version::mscons(10_000_000_000_000)?.get());
    /// # Ok::<(), meterstore::Error>(())
    /// ```
    pub fn arrival(recorded_at: OffsetDateTime) -> Result<Self> {
        let millis = recorded_at.unix_timestamp_nanos() / 1_000_000;
        let millis = u128::try_from(millis).map_err(|_| {
            Error::encode(
                "version",
                format!(
                    "{recorded_at} predates the Unix epoch, so it has no arrival-derived \
                     version: versions are unsigned and must ascend"
                ),
            )
        })?;
        Self::new(millis)
    }

    /// Whether this version has the ≥14 digits MSCONS mandates.
    ///
    /// Short versions are stored and ordered normally; this predicate exists so
    /// ingestion can warn rather than silently normalise regulated data. See
    /// [`mscons`](Self::mscons) to reject instead of warn.
    pub const fn is_well_formed(self) -> bool {
        self.0 >= 10u128.pow(MIN_DIGITS - 1)
    }

    /// Encode for the `version` column, `Decimal128(20,0)`.
    pub const fn to_i128(self) -> i128 {
        self.0 as i128
    }

    /// Decode from the `version` column.
    pub fn from_i128(value: i128) -> Result<Self> {
        u128::try_from(value)
            .map_err(|_| Error::decode("version", format!("negative value {value}")))
            .and_then(Self::new)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The (network operator, month) pair a [`Version`] is comparable within.
///
/// Stored as a canonical `"<operator>:<YYYY-MM>"` string so it dictionary-encodes
/// well and remains greppable in the lake — twenty characters, the same twenty
/// on every row, because the operator half is a [`BdewCode`] and those are
/// always thirteen digits.
///
/// # The operator is parsed, not carried
///
/// It is the **Marktpartner-ID** of the network operator that issued the
/// version — what MSCONS puts in `NAD+MS`, and the same identifier
/// [`MeasurementSource::Mscons`] carries. `metering` 0.20 gave it a type, and
/// this crate takes the type for the reason it takes [`MaloId`] rather than
/// eleven digits: past the constructor a wrong-but-plausible operator is simply
/// a *different scope*, so a correction fails to supersede the value it corrects
/// and both rows survive into the resolved view. There is no error anywhere and
/// the total is inflated.
///
/// The check digit is verified but **not enforced**, which is `BdewCode`'s
/// rule rather than a choice made here: BDEW's own Anwendungshilfe carves out
/// GS1-issued GLNs, which use a different procedure, so a well-formed
/// Marktpartner-ID may legitimately fail the BDEW one.
/// [`operator_has_bdew_check_digit`](Self::operator_has_bdew_check_digit)
/// reports it so ingestion can warn.
///
/// [`MeasurementSource::Mscons`]: metering::measurement_series::MeasurementSource::Mscons
/// [`MaloId`]: metering::ids::MaloId
///
/// # The month is the interval's, never the delivery's
///
/// MSCONS assigns versions per network operator per month, and resolution
/// partitions by scope — so two versions of one interval must share a scope or
/// neither can supersede the other. A July reading corrected in August still
/// belongs to July's scope; keying the scope to the delivery month instead
/// would leave both rows standing and double the total, with no error anywhere.
///
/// [`VersionScope::for_interval`] derives it correctly.
///
/// # And the month is the **Bilanzierungsmonat**, which gas cuts at 06:00
///
/// It is a local month rather than a UTC one, because German market processes
/// are defined in local time: an interval starting 2026-07-31T23:00Z is already
/// August in Berlin.
///
/// For gas it is not the calendar month at all. EDI@Energy *Allgemeine
/// Festlegungen* v6.1c, Kap. 3.1 defines the Bilanzierungsmonat Juni 2021 as
/// 01.06 00:00 to 01.07 00:00 for Strom and 01.06 **06:00** to 01.07 **06:00**
/// for Gas — the Gastag boundary carries all the way up, so a gas month is a
/// whole number of Gastage rather than a calendar month shifted. An interval at
/// 02:00 local on 1 March belongs to February's gas scope.
///
/// So every constructor here takes a [`Sparte`]. It is not decoration: before
/// this, a producer deriving the *correct* gas Bilanzierungsmonat had its
/// delivery **refused** by [`covers`](Self::covers) at the write, while one
/// deriving the calendar month was accepted — the store enforced the wrong rule
/// and enforced it firmly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VersionScope(String);

impl VersionScope {
    /// Build a scope from a network operator's Marktpartner-ID and an explicit
    /// year/month.
    ///
    /// **Prefer [`VersionScope::for_interval`].** This constructor cannot tell
    /// whether the month you pass is the interval's or the delivery's, and
    /// passing the delivery month breaks resolution silently — two versions of
    /// one reading end up in different scopes, neither supersedes the other,
    /// and every sum over them is inflated.
    ///
    /// It remains public for reconstructing a scope that is already known to be
    /// correct — parsing a stored value, or a test pinning a specific month.
    ///
    /// `operator` is anything that parses as a [`BdewCode`], so a `&str` from a
    /// configuration file or an MSCONS `NAD+MS` segment works directly and a
    /// malformed one fails here rather than becoming a scope nothing matches.
    pub fn new<O>(operator: O, year: i32, month: u8) -> Result<Self>
    where
        O: TryInto<BdewCode>,
        O::Error: fmt::Display,
    {
        let operator = parse_operator(operator)?;
        if !(1..=12).contains(&month) {
            return Err(Error::config(format!("month {month} out of range 1..=12")));
        }
        // Four digits, because that is what the canonical form is and what three
        // other places anchor on: [`parse`](Self::parse), the hot table's
        // `version_scope_canonical` CHECK, and every reader that splits the
        // stored string. `{year:04}` pads but does not truncate, so a year
        // outside this range renders as five characters or with a sign — a value
        // this constructor accepted, `parse` would refuse, and PostgreSQL would
        // reject at the write with a constraint name rather than a reason.
        if !(0..=9999).contains(&year) {
            return Err(Error::config(format!(
                "year {year} is outside 0..=9999, which is what a canonical scope's \
                 YYYY-MM period can hold"
            )));
        }
        Ok(Self(format!("{operator}:{year:04}-{month:02}")))
    }

    /// The scope an interval belongs to.
    ///
    /// This is the constructor to prefer. [`VersionScope::new`] cannot tell a
    /// delivery month from an interval month, and getting that wrong breaks
    /// resolution silently.
    ///
    /// `sparte` decides where the month is cut: gas on the 06:00 Gastag
    /// boundary, everything else at midnight. See the type documentation.
    ///
    /// ```rust
    /// use meterstore::VersionScope;
    /// use metering::interval::Sparte;
    /// use time::macros::datetime;
    ///
    /// // The network operator's Marktpartner-ID, as MSCONS carries it in NAD+MS.
    /// let nb = "9900000000001";
    ///
    /// // 02:00 local on 1 March: already March, still the February gas month.
    /// let at = datetime!(2026-03-01 1:00 UTC);
    /// assert_eq!(VersionScope::for_interval(nb, at, Sparte::Strom)?.period(), "2026-03");
    /// assert_eq!(VersionScope::for_interval(nb, at, Sparte::Gas)?.period(), "2026-02");
    ///
    /// // And it is parsed, so a wrong-but-plausible one fails here rather than
    /// // becoming a scope of its own that nothing else shares.
    /// assert!(VersionScope::for_interval("99", at, Sparte::Strom).is_err());
    /// # Ok::<(), meterstore::Error>(())
    /// ```
    pub fn for_interval<O>(
        operator: O,
        interval_start: OffsetDateTime,
        sparte: Sparte,
    ) -> Result<Self>
    where
        O: TryInto<BdewCode>,
        O::Error: fmt::Display,
    {
        let month = crate::planner::balancing_month(interval_start, sparte);
        Self::new(operator, month.year(), u8::from(month.month()))
    }

    /// The `YYYY-MM` period this scope covers.
    pub fn period(&self) -> &str {
        let at = self.0.find(':').expect("canonical form contains ':'");
        &self.0[at + 1..]
    }

    /// The network operator that assigned versions in this scope.
    ///
    /// The half before the separator, which is the half PostgreSQL's
    /// `split_part(version_scope, ':', 1)` takes in the one-operator exclusion.
    /// A [`BdewCode`] is thirteen digits and cannot contain one, so there is
    /// only ever the one and the two agree by construction.
    pub fn operator(&self) -> BdewCode {
        let at = self.0.find(':').expect("canonical form contains ':'");
        self.0[..at].parse().expect("constructed from a BdewCode")
    }

    /// Whether the operator's thirteenth digit matches the BDEW procedure.
    ///
    /// **Advisory**, and `false` does not mean the code is wrong: BDEW's
    /// Anwendungshilfe §2.3 carves out GS1-issued GLNs, which use a different
    /// check-digit procedure and are legitimate Marktpartner-IDs. Use it to warn
    /// at an ingest boundary, never to reject — the same restraint
    /// [`Version::is_well_formed`] applies to a short version label.
    pub fn operator_has_bdew_check_digit(&self) -> bool {
        self.operator().has_bdew_check_digit()
    }

    /// Whether an interval belongs to this scope.
    ///
    /// Used at encode time so a mismatch fails at the boundary rather than
    /// surfacing later as an inflated sum — which means it runs **once per row**,
    /// on the path a day of 100 k measuring points takes. Compared against the
    /// stored period in place rather than by formatting a second string to
    /// compare with: at ~9.6 M rows a day that allocation is the whole of what
    /// this function costs.
    ///
    /// `sparte` decides where the month is cut, exactly as in
    /// [`for_interval`](Self::for_interval). Passing the wrong one here is what
    /// turned a correctly-scoped gas delivery into a write error.
    pub fn covers(&self, interval_start: OffsetDateTime, sparte: Sparte) -> bool {
        let month = crate::planner::balancing_month(interval_start, sparte);
        let period = self.period();
        let Some((year, rest)) = period.split_once('-') else {
            return false;
        };
        year.parse::<i32>() == Ok(month.year()) && rest.parse::<u8>() == Ok(u8::from(month.month()))
    }

    /// Parse the canonical stored form.
    ///
    /// Held to exactly what [`new`](Self::new) produces, because a scope that is
    /// merely well-shaped fails quietly rather than loudly:
    /// [`covers`](Self::covers) answers `false` for a period it cannot read, so
    /// such a scope refuses **every** delivery it is checked against, blaming the
    /// caller's Bilanzierungsmonat for a value that was malformed in the table.
    /// And the hot tier's one-operator exclusion reads the operator back with
    /// `split_part(version_scope, ':', 1)` — the *first* half — so a second
    /// separator would leave the constraint comparing something other than what
    /// this type reports.
    ///
    /// The hot table carries the same rule as a `CHECK`. This is the other tier's
    /// half, since Iceberg has no constraints.
    pub fn parse(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        let malformed = || {
            Error::decode(
                "version_scope",
                format!(
                    "{s:?} is not a canonical version scope — it must be \
                     <operator>:<YYYY-MM>, with a 13-digit Marktpartner-ID as the \
                     operator and a month in 01..=12"
                ),
            )
        };
        // `split_once`, not `rsplit_once`: a Marktpartner-ID is thirteen digits
        // and cannot contain the separator, so the first one is the only one.
        let Some((operator, period)) = s.split_once(':') else {
            return Err(malformed());
        };
        let Some((year, month)) = period.split_once('-') else {
            return Err(malformed());
        };
        // Rendered back and compared, not merely parsed. `BdewCode`'s `FromStr`
        // trims — right at an ingest boundary, wrong here: `" 99…1:2026-07"`
        // would parse, be stored as it stands, and then be refused by the hot
        // table's `CHECK`, which anchors on thirteen digits. The one thing this
        // constructor exists to guarantee is that what it accepts is exactly what
        // `new` would have produced.
        let canonical = operator
            .parse::<BdewCode>()
            .is_ok_and(|code| code.as_str() == operator);
        // Four *digits*, not four characters: `"-100"` is four characters and
        // parses as an `i32`, and it is not something `new` can produce nor
        // something the stored column's `CHECK` accepts.
        if !canonical
            || year.len() != 4
            || !year.bytes().all(|b| b.is_ascii_digit())
            || month.len() != 2
            || !month.bytes().all(|b| b.is_ascii_digit())
            || !month.parse::<u8>().is_ok_and(|m| (1..=12).contains(&m))
        {
            return Err(malformed());
        }
        Ok(Self(s))
    }

    /// The canonical stored form.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for VersionScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Parse a caller-supplied network-operator code.
///
/// The counterpart of [`parse_malo`](crate::encode::parse_malo) for the other
/// identifier this crate is handed as a bare string, and the reason
/// [`VersionScope`]'s constructors take `impl TryInto<BdewCode>`: a code the
/// caller has already parsed passes through at no cost, while thirteen digits
/// off an MSCONS `NAD+MS` segment are checked here.
///
/// `metering`'s own message says what shape is expected; this adds what a wrong
/// one costs, because the failure is otherwise invisible — the delivery is
/// accepted into a scope nothing else shares, so the correction never supersedes
/// and every sum over the reading is inflated.
fn parse_operator<O>(operator: O) -> Result<BdewCode>
where
    O: TryInto<BdewCode>,
    O::Error: fmt::Display,
{
    operator.try_into().map_err(|e| {
        Error::config(format!(
            "a version scope's operator is the network operator's Marktpartner-ID, \
             as MSCONS carries it in NAD+MS: {e}. A version is only comparable within \
             the (operator, month) that issued it, so a wrong one is a scope of its \
             own — the correction never supersedes, and both rows survive into the \
             resolved view"
        ))
    })
}

/// A version together with the scope it is comparable within.
///
/// Deliberately has no [`Ord`] impl: ordering two versions is a fallible
/// operation because it is only defined within a single scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScopedVersion {
    scope: VersionScope,
    version: Version,
}

impl ScopedVersion {
    /// Pair a version with its scope.
    pub const fn new(scope: VersionScope, version: Version) -> Self {
        Self { scope, version }
    }

    /// The scope.
    pub const fn scope(&self) -> &VersionScope {
        &self.scope
    }

    /// The version.
    pub const fn version(&self) -> Version {
        self.version
    }

    /// Order against another version, or fail if the scopes differ.
    ///
    /// This is the whole point of the type. `max(version)` resolution
    /// must partition by scope first; calling this without doing so surfaces the
    /// mistake as an error instead of a wrong meter reading.
    pub fn try_cmp(&self, other: &Self) -> Result<Ordering> {
        if self.scope != other.scope {
            return Err(Error::VersionScopeMismatch {
                left: self.scope.0.clone(),
                right: other.scope.0.clone(),
            });
        }
        Ok(self.version.0.cmp(&other.version.0))
    }

    /// Whether `self` supersedes `other`. Errors if the scopes differ.
    pub fn supersedes(&self, other: &Self) -> Result<bool> {
        Ok(self.try_cmp(other)? == Ordering::Greater)
    }

    /// The next version **in this one's own scope**, so it supersedes it.
    ///
    /// The mechanism behind
    /// [`MeterStore::append_authoritative`](crate::MeterStore::append_authoritative):
    /// a value the operator authors — a § 60 Abs. 2 MsbG Ersatzwert, a
    /// correction — has to take effect, and the only lever the storage model
    /// gives for that is a higher version.
    ///
    /// **The scope is carried over, never re-derived.** A version is comparable
    /// only within its `(operator, month)` scope, and the hot tier's exclusion
    /// constraint refuses a second network operator for one reading — so minting
    /// a successor under the *caller's* scope would either be incomparable with
    /// what it means to supersede or be rejected outright. It continues the
    /// sequence that is already there.
    pub fn next(&self) -> Result<Self> {
        Ok(Self::new(
            self.scope.clone(),
            Version::new(self.version.get().saturating_add(1))?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Duration;

    #[test]
    fn a_scope_is_exactly_what_the_stored_column_can_hold() {
        // Three places anchor on `YYYY-MM` with four digits: this constructor,
        // `parse`, and the hot table's `version_scope_canonical` CHECK. A year
        // outside 0..=9999 renders as five characters or with a sign, which the
        // other two refuse — so it has to fail here, where the message can say
        // why.
        assert!(VersionScope::new("9900000000001", 2026, 7).is_ok());
        assert!(VersionScope::new("9900000000001", 0, 1).is_ok());
        assert!(VersionScope::new("9900000000001", 9999, 12).is_ok());

        for year in [-1, -100, 10_000, i32::MAX, i32::MIN] {
            let err = VersionScope::new("9900000000001", year, 7)
                .expect_err("outside the canonical form")
                .to_string();
            assert!(err.contains("0..=9999"), "{year}: {err}");
        }
    }

    #[test]
    fn parse_accepts_exactly_what_new_produces() {
        // The invariant `parse` documents. `"-100"` is four characters and
        // parses as an `i32`, which is why the check is on *digits*.
        let canonical = VersionScope::new("9900000000001", 2026, 7).unwrap();
        assert_eq!(
            VersionScope::parse(canonical.as_str().to_string()).unwrap(),
            canonical
        );
        for bad in [
            "9900000000001:-100-07",
            "9900000000001:20260-07",
            "9900000000001:2026-7",
            "9900000000001:2026-0a",
            "9900000000001:202a-07",
        ] {
            assert!(VersionScope::parse(bad).is_err(), "{bad}");
        }
    }

    /// A real BDEW Marktpartner-ID: thirteen digits, `99` = BDEW/Strom.
    const OPERATOR: &str = "9900000000001";
    /// A second one, for the comparisons that must not cross operators.
    const OTHER_OPERATOR: &str = "9900000000002";

    fn scope(op: &str, y: i32, m: u8) -> VersionScope {
        VersionScope::new(op, y, m).unwrap()
    }

    #[test]
    fn version_round_trips_through_storage_encoding() {
        let v = Version::new(20_260_727_000_001).unwrap();
        assert_eq!(Version::from_i128(v.to_i128()).unwrap(), v);
    }

    #[test]
    fn version_rejects_values_beyond_decimal128_20_0() {
        assert!(Version::new(MAX_VERSION).is_ok());
        assert!(Version::new(MAX_VERSION + 1).is_err());
    }

    #[test]
    fn version_rejects_negative_on_decode() {
        assert!(Version::from_i128(-1).is_err());
    }

    #[test]
    fn the_strict_constructor_refuses_a_short_version() {
        // The ingest boundary. A short version orders correctly within its scope,
        // so it is invisible once stored — the place to refuse it is the edge.
        let err = Version::mscons(42).unwrap_err().to_string();
        assert!(err.contains("14"), "{err}");
        assert!(
            err.contains("Version::new"),
            "the message must name the permissive path for data already received: {err}"
        );

        assert!(Version::mscons(20_260_727_000_001).is_ok());
        // And it still rejects what `new` rejects, rather than only the digits.
        assert!(Version::mscons(u128::MAX).is_err());
    }

    #[test]
    fn an_arrival_version_always_sorts_below_a_stated_one() {
        use time::macros::datetime;

        // The whole point of choosing milliseconds. A delivery that states no
        // version must never outrank one that does, whatever order they arrive
        // in — and the guarantee has to hold for the length of a retention
        // period, not just today.
        let floor = Version::mscons(10_000_000_000_000).unwrap();

        for at in [
            datetime!(1970-01-02 00:00 UTC),
            datetime!(2026-08-25 06:00:00.123 UTC),
            datetime!(2100-01-01 00:00 UTC),
            // Milliseconds stay 13 digits until 20 November 2286.
            datetime!(2286-11-20 00:00 UTC),
        ] {
            let v = Version::arrival(at).unwrap();
            assert!(v.get() < floor.get(), "{at}: {v} is not below {floor}");
            assert!(!v.is_well_formed(), "{at}: must not look MSCONS-issued");
        }
    }

    #[test]
    fn an_arrival_version_is_sub_second() {
        use time::macros::datetime;

        // Seconds would be safely below the MSCONS band and still wrong: two
        // writes for one interval inside one second would share a version, and
        // the second would be refused as a divergent restatement of the first.
        let a = Version::arrival(datetime!(2026-08-25 06:00:00.001 UTC)).unwrap();
        let b = Version::arrival(datetime!(2026-08-25 06:00:00.002 UTC)).unwrap();
        assert!(b.get() > a.get());
        assert_eq!(b.get() - a.get(), 1);
    }

    #[test]
    fn arrival_versions_ascend_with_arrival() {
        use time::macros::datetime;

        let mut previous = Version::arrival(datetime!(2026-01-01 00:00 UTC)).unwrap();
        for hours in 1..48 {
            let next =
                Version::arrival(datetime!(2026-01-01 00:00 UTC) + Duration::hours(hours)).unwrap();
            assert!(next.get() > previous.get());
            previous = next;
        }
    }

    #[test]
    fn a_pre_epoch_instant_has_no_arrival_version() {
        use time::macros::datetime;

        // Versions are unsigned and must ascend; a negative one cannot be stored
        // and would order wrongly if it could.
        let err = Version::arrival(datetime!(1969-12-31 23:59 UTC))
            .unwrap_err()
            .to_string();
        assert!(err.contains("epoch"), "{err}");
    }

    #[test]
    fn the_permissive_constructor_still_accepts_what_arrived() {
        // Refusing to *store* a short version that a network operator actually
        // sent would lose regulated data we already hold.
        assert!(Version::new(42).is_ok());
        assert!(!Version::new(42).unwrap().is_well_formed());
    }

    #[test]
    fn well_formedness_tracks_the_14_digit_rule() {
        assert!(Version::new(10_000_000_000_000).unwrap().is_well_formed());
        assert!(!Version::new(9_999_999_999_999).unwrap().is_well_formed());
    }

    #[test]
    fn short_versions_are_stored_and_still_order() {
        // Regulated data we already received must not be rejected, only flagged.
        let s = scope(OPERATOR, 2026, 7);
        let a = ScopedVersion::new(s.clone(), Version::new(1).unwrap());
        let b = ScopedVersion::new(s, Version::new(2).unwrap());
        assert!(b.supersedes(&a).unwrap());
    }

    #[test]
    fn versions_order_within_a_scope() {
        let s = scope(OPERATOR, 2026, 7);
        let older = ScopedVersion::new(s.clone(), Version::new(20_260_701_000_001).unwrap());
        let newer = ScopedVersion::new(s, Version::new(20_260_715_000_002).unwrap());

        assert_eq!(older.try_cmp(&newer).unwrap(), Ordering::Less);
        assert!(newer.supersedes(&older).unwrap());
        assert!(!older.supersedes(&newer).unwrap());
    }

    #[test]
    fn the_successor_supersedes_and_keeps_the_scope() {
        // A value the operator authors has to become current, and a higher
        // version is the only lever the model offers. The scope must be the
        // stored one: minting a successor under the caller's own operator would
        // be incomparable with what it is meant to supersede, and the hot tier's
        // one-operator exclusion would refuse it anyway.
        let stored = ScopedVersion::new(
            scope(OPERATOR, 2026, 7),
            Version::new(20_260_715_000_002).unwrap(),
        );
        let next = stored.next().unwrap();

        assert!(next.supersedes(&stored).unwrap());
        assert_eq!(next.scope(), stored.scope());
        assert_eq!(next.version().get(), stored.version().get() + 1);
    }

    #[test]
    fn the_successor_refuses_to_leave_the_storage_range() {
        // `Decimal128(20,0)` is the ceiling, and rolling over it would produce a
        // version that sorts *below* what it was meant to supersede.
        let at_ceiling =
            ScopedVersion::new(scope(OPERATOR, 2026, 7), Version::new(MAX_VERSION).unwrap());
        assert!(at_ceiling.next().is_err());
    }

    #[test]
    fn versions_do_not_compare_across_operators() {
        // This is the comparison that silently picks wrong values.
        let a = ScopedVersion::new(scope(OPERATOR, 2026, 7), Version::new(5).unwrap());
        let b = ScopedVersion::new(scope(OTHER_OPERATOR, 2026, 7), Version::new(9).unwrap());

        assert!(matches!(
            a.try_cmp(&b),
            Err(Error::VersionScopeMismatch { .. })
        ));
    }

    #[test]
    fn versions_do_not_compare_across_months() {
        let a = ScopedVersion::new(scope(OPERATOR, 2026, 7), Version::new(5).unwrap());
        let b = ScopedVersion::new(scope(OPERATOR, 2026, 8), Version::new(9).unwrap());

        assert!(a.try_cmp(&b).is_err());
    }

    #[test]
    fn a_scope_derived_from_an_interval_uses_the_local_month() {
        use time::macros::datetime;

        // 23:00 UTC on 31 July is already 1 August in Berlin, and the market
        // works in local time.
        let july =
            VersionScope::for_interval(OPERATOR, datetime!(2026-07-31 20:00 UTC), Sparte::Strom)
                .unwrap();
        let august =
            VersionScope::for_interval(OPERATOR, datetime!(2026-07-31 23:00 UTC), Sparte::Strom)
                .unwrap();

        assert_eq!(july.period(), "2026-07");
        assert_eq!(august.period(), "2026-08");
    }

    #[test]
    fn the_gas_bilanzierungsmonat_is_cut_at_the_gastag_boundary() {
        use time::macros::datetime;

        // EDI@Energy Allgemeine Festlegungen v6.1c Kap. 3.1: the gas
        // Bilanzierungsmonat Juni 2021 covers 01.06 06:00 to 01.07 06:00, so the
        // first six hours of a calendar month belong to the previous one.
        //
        // 01:00 UTC on 1 March is 02:00 local — March by the calendar, still the
        // February Gastag.
        let early = datetime!(2026-03-01 1:00 UTC);
        assert_eq!(
            VersionScope::for_interval(OPERATOR, early, Sparte::Strom)
                .unwrap()
                .period(),
            "2026-03"
        );
        assert_eq!(
            VersionScope::for_interval(OPERATOR, early, Sparte::Gas)
                .unwrap()
                .period(),
            "2026-02"
        );

        // Past 06:00 local the two agree again.
        let later = datetime!(2026-03-01 6:00 UTC);
        for sparte in [Sparte::Strom, Sparte::Gas] {
            assert_eq!(
                VersionScope::for_interval(OPERATOR, later, sparte)
                    .unwrap()
                    .period(),
                "2026-03"
            );
        }
    }

    #[test]
    fn a_correctly_scoped_gas_delivery_is_accepted() {
        use time::macros::datetime;

        // Checked against the calendar month instead, a producer deriving the
        // real gas Bilanzierungsmonat has its delivery refused at the write while
        // one deriving the calendar month is accepted: the wrong rule, enforced
        // firmly.
        let early = datetime!(2026-03-01 1:00 UTC);
        let correct = VersionScope::for_interval(OPERATOR, early, Sparte::Gas).unwrap();
        assert!(correct.covers(early, Sparte::Gas));

        let calendar_month = VersionScope::new(OPERATOR, 2026, 3).unwrap();
        assert!(
            !calendar_month.covers(early, Sparte::Gas),
            "the calendar month is not this interval's gas Bilanzierungsmonat"
        );
        // And it is still the right answer for every other commodity.
        assert!(calendar_month.covers(early, Sparte::Strom));
    }

    #[test]
    fn every_version_of_one_interval_derives_the_same_scope() {
        use time::macros::datetime;

        // The property resolution depends on: when the correction was delivered
        // is irrelevant, because the scope comes from the interval.
        let interval = datetime!(2026-07-20 06:00 UTC);
        for sparte in [Sparte::Strom, Sparte::Gas] {
            let original = VersionScope::for_interval(OPERATOR, interval, sparte).unwrap();
            let correction = VersionScope::for_interval(OPERATOR, interval, sparte).unwrap();
            assert_eq!(original, correction);
        }
    }

    #[test]
    fn a_derived_scope_always_covers_the_interval_it_came_from() {
        use time::macros::datetime;

        // The invariant the encode-time check rests on, over a year of instants
        // and both boundaries.
        let mut at = datetime!(2026-01-01 00:00 UTC);
        let end = datetime!(2027-01-01 00:00 UTC);
        while at < end {
            for sparte in [Sparte::Strom, Sparte::Gas, Sparte::Waerme, Sparte::Wasser] {
                let scope = VersionScope::for_interval(OPERATOR, at, sparte).unwrap();
                assert!(scope.covers(at, sparte), "{at} {sparte} {scope}");
            }
            at += time::Duration::hours(5);
        }
    }

    #[test]
    fn covers_accepts_only_intervals_in_the_scope_month() {
        use time::macros::datetime;

        let july =
            VersionScope::for_interval(OPERATOR, datetime!(2026-07-20 00:00 UTC), Sparte::Strom)
                .unwrap();
        assert!(july.covers(datetime!(2026-07-01 00:00 UTC), Sparte::Strom));
        assert!(july.covers(datetime!(2026-07-31 20:00 UTC), Sparte::Strom));
        assert!(!july.covers(datetime!(2026-08-01 00:00 UTC), Sparte::Strom));
        // Local, not UTC: this instant is already August in Berlin.
        assert!(!july.covers(datetime!(2026-07-31 23:00 UTC), Sparte::Strom));
    }

    #[test]
    fn operator_and_period_split_the_canonical_form() {
        let s = scope(OPERATOR, 2026, 3);
        assert_eq!(s.operator().as_str(), OPERATOR);
        assert_eq!(s.period(), "2026-03");
    }

    #[test]
    fn scope_round_trips_through_canonical_form() {
        let s = scope(OPERATOR, 2026, 3);
        assert_eq!(s.as_str(), "9900000000001:2026-03");
        assert_eq!(VersionScope::parse(s.as_str()).unwrap(), s);
    }

    #[test]
    fn parse_accepts_only_what_new_would_have_produced() {
        // A scope that is merely well-shaped fails quietly: `covers` answers
        // false for a period it cannot read, so it refuses *every* delivery it is
        // checked against, blaming the caller's Bilanzierungsmonat for a value
        // malformed in the table. And the hot tier's one-operator exclusion reads
        // the operator with `split_part(…, ':', 1)`, which takes the other half
        // of a scope carrying two separators.
        for bad in [
            "9900000000001:2026-13", // month out of range
            "9900000000001:2026-99",
            "9900000000001:2026-00",
            "9900000000001:20x6-03",  // year not a number
            "a:b:2026-03",            // operator carrying the separator
            ":2026-03",               // no operator
            "9900000000001:2026-3",   // unpadded month
            "9900000000001:202-003",  // seven characters, wrong shape
            "9900000000001",          // no separator at all
            "99:2026-03",             // the short spelling this crate used to take
            "990000000000:2026-03",   // twelve digits
            "99000000000012:2026-03", // fourteen
            // `BdewCode`'s own `FromStr` trims, which is right at an ingest
            // boundary and wrong for a stored value: this would parse, be kept
            // as it stands, and then be refused by the hot table's CHECK.
            " 9900000000001:2026-03",
            "9900000000001 :2026-03",
        ] {
            assert!(VersionScope::parse(bad).is_err(), "{bad:?} must not parse");
        }

        // And everything a constructor can produce still round-trips.
        for month in 1..=12 {
            let s = scope(OPERATOR, 2026, month);
            assert_eq!(VersionScope::parse(s.as_str()).unwrap(), s);
        }
    }

    #[test]
    fn the_operator_is_the_half_postgres_reads_back() {
        // The one-operator exclusion is `split_part(version_scope, ':', 1)`, so
        // `operator` has to split on the first separator, not the last. Both
        // constructors refuse an operator containing one, so the two agree.
        let s = scope(OPERATOR, 2026, 3);
        assert_eq!(s.operator().as_str(), s.as_str().split(':').next().unwrap());
    }

    #[test]
    fn the_operator_must_be_a_marktpartner_id() {
        // Past the constructor a wrong-but-plausible operator is simply a
        // *different scope*: the correction fails to supersede, both rows
        // survive resolution, and the total is inflated with no error anywhere.
        // So it is parsed here, exactly as `MaloId` is.
        for bad in [
            "99",             // the short spelling a fixture reaches for
            "bad:operator",   // the separator, which thirteen digits cannot hold
            "990000000000",   // twelve digits
            "99000000000012", // fourteen
            "99000000000x1",  // not all digits
            "",
        ] {
            assert!(
                VersionScope::new(bad, 2026, 7).is_err(),
                "{bad:?} is not a Marktpartner-ID"
            );
        }

        let err = VersionScope::new("99", 2026, 7).unwrap_err().to_string();
        assert!(
            err.contains("13-digit"),
            "metering's own shape message: {err}"
        );
        assert!(
            err.contains("NAD+MS"),
            "the message must name where the value comes from: {err}"
        );
        assert!(
            err.contains("supersedes"),
            "and what a wrong one costs, since the failure is otherwise invisible: {err}"
        );
    }

    #[test]
    fn a_gs1_gln_is_stored_and_flagged_rather_than_refused() {
        // BDEW's Anwendungshilfe §2.3 carves out GS1-issued GLNs, which use a
        // different check-digit procedure — so a well-formed Marktpartner-ID may
        // legitimately fail the BDEW one, and refusing it would refuse data the
        // market issued. Reported instead, like a short version label.
        let digits = "990098765432";
        let check = metering::ids::BdewCode::compute_check_digit(digits).expect("twelve digits");
        let consistent = VersionScope::new(&*format!("{digits}{check}"), 2026, 7).unwrap();
        assert!(consistent.operator_has_bdew_check_digit());

        let wrong_check = (check + 1) % 10;
        let inconsistent = VersionScope::new(&*format!("{digits}{wrong_check}"), 2026, 7).unwrap();
        assert!(!inconsistent.operator_has_bdew_check_digit());
        assert!(
            VersionScope::parse(inconsistent.as_str()).is_ok(),
            "stored and readable back: the flag is advisory, not a gate"
        );
    }

    #[test]
    fn scope_rejects_out_of_range_month() {
        assert!(VersionScope::new(OPERATOR, 2026, 0).is_err());
        assert!(VersionScope::new(OPERATOR, 2026, 13).is_err());
    }
}
