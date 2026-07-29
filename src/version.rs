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
/// well and remains greppable in the lake.
///
/// # The month is the interval's, never the delivery's
///
/// MSCONS assigns versions per network operator per month, and resolution
/// partitions by scope — so two versions of one interval must share a scope or
/// neither can supersede the other. A July reading corrected in August still
/// belongs to July's scope; keying the scope to the delivery month instead
/// would leave both rows standing and double the total, with no error anywhere.
///
/// [`VersionScope::for_interval`] derives it correctly. It uses the **local**
/// calendar month, because German market processes are defined in local time:
/// an interval starting 2026-07-31T23:00Z is already August in Berlin.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VersionScope(String);

impl VersionScope {
    /// Build a scope from a BDEW operator code and an explicit year/month.
    ///
    /// **Prefer [`VersionScope::for_interval`].** This constructor cannot tell
    /// whether the month you pass is the interval's or the delivery's, and
    /// passing the delivery month breaks resolution silently — two versions of
    /// one reading end up in different scopes, neither supersedes the other,
    /// and every sum over them is inflated.
    ///
    /// It remains public for reconstructing a scope that is already known to be
    /// correct — parsing a stored value, or a test pinning a specific month.
    pub fn new(operator: impl AsRef<str>, year: i32, month: u8) -> Result<Self> {
        let operator = operator.as_ref();
        if operator.is_empty() {
            return Err(Error::config("version scope operator must not be empty"));
        }
        if operator.contains(':') {
            return Err(Error::config(format!(
                "version scope operator {operator:?} must not contain ':' (the canonical separator)"
            )));
        }
        if !(1..=12).contains(&month) {
            return Err(Error::config(format!("month {month} out of range 1..=12")));
        }
        Ok(Self(format!("{operator}:{year:04}-{month:02}")))
    }

    /// The scope an interval belongs to.
    ///
    /// This is the constructor to prefer. [`VersionScope::new`] cannot tell a
    /// delivery month from an interval month, and getting that wrong breaks
    /// resolution silently.
    pub fn for_interval(operator: impl AsRef<str>, interval_start: OffsetDateTime) -> Result<Self> {
        let month = metering::calendar::local_month(interval_start);
        Self::new(operator, month.year(), u8::from(month.month()))
    }

    /// The `YYYY-MM` period this scope covers.
    pub fn period(&self) -> &str {
        let at = self.0.rfind(':').expect("canonical form contains ':'");
        &self.0[at + 1..]
    }

    /// The network operator that assigned versions in this scope.
    pub fn operator(&self) -> &str {
        let at = self.0.rfind(':').expect("canonical form contains ':'");
        &self.0[..at]
    }

    /// Whether an interval belongs to this scope.
    ///
    /// Used at encode time so a mismatch fails at the boundary rather than
    /// surfacing later as an inflated sum.
    pub fn covers(&self, interval_start: OffsetDateTime) -> bool {
        let month = metering::calendar::local_month(interval_start);
        self.period() == format!("{:04}-{:02}", month.year(), u8::from(month.month()))
    }

    /// Parse the canonical stored form.
    pub fn parse(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        let Some((operator, period)) = s.rsplit_once(':') else {
            return Err(Error::decode(
                "version_scope",
                format!("missing ':' in {s:?}"),
            ));
        };
        if operator.is_empty() || period.len() != 7 {
            return Err(Error::decode(
                "version_scope",
                format!("malformed scope {s:?}"),
            ));
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let s = scope("9900000000001", 2026, 7);
        let a = ScopedVersion::new(s.clone(), Version::new(1).unwrap());
        let b = ScopedVersion::new(s, Version::new(2).unwrap());
        assert!(b.supersedes(&a).unwrap());
    }

    #[test]
    fn versions_order_within_a_scope() {
        let s = scope("9900000000001", 2026, 7);
        let older = ScopedVersion::new(s.clone(), Version::new(20_260_701_000_001).unwrap());
        let newer = ScopedVersion::new(s, Version::new(20_260_715_000_002).unwrap());

        assert_eq!(older.try_cmp(&newer).unwrap(), Ordering::Less);
        assert!(newer.supersedes(&older).unwrap());
        assert!(!older.supersedes(&newer).unwrap());
    }

    #[test]
    fn versions_do_not_compare_across_operators() {
        // This is the comparison that silently picks wrong values.
        let a = ScopedVersion::new(scope("9900000000001", 2026, 7), Version::new(5).unwrap());
        let b = ScopedVersion::new(scope("9900000000002", 2026, 7), Version::new(9).unwrap());

        assert!(matches!(
            a.try_cmp(&b),
            Err(Error::VersionScopeMismatch { .. })
        ));
    }

    #[test]
    fn versions_do_not_compare_across_months() {
        let a = ScopedVersion::new(scope("9900000000001", 2026, 7), Version::new(5).unwrap());
        let b = ScopedVersion::new(scope("9900000000001", 2026, 8), Version::new(9).unwrap());

        assert!(a.try_cmp(&b).is_err());
    }

    #[test]
    fn a_scope_derived_from_an_interval_uses_the_local_month() {
        use time::macros::datetime;

        // 23:00 UTC on 31 July is already 1 August in Berlin, and the market
        // works in local time.
        let july = VersionScope::for_interval("99", datetime!(2026-07-31 20:00 UTC)).unwrap();
        let august = VersionScope::for_interval("99", datetime!(2026-07-31 23:00 UTC)).unwrap();

        assert_eq!(july.period(), "2026-07");
        assert_eq!(august.period(), "2026-08");
    }

    #[test]
    fn every_version_of_one_interval_derives_the_same_scope() {
        use time::macros::datetime;

        // The property resolution depends on: when the correction was delivered
        // is irrelevant, because the scope comes from the interval.
        let interval = datetime!(2026-07-20 06:00 UTC);
        let original = VersionScope::for_interval("99", interval).unwrap();
        let correction = VersionScope::for_interval("99", interval).unwrap();
        assert_eq!(original, correction);
    }

    #[test]
    fn covers_accepts_only_intervals_in_the_scope_month() {
        use time::macros::datetime;

        let july = VersionScope::for_interval("99", datetime!(2026-07-20 00:00 UTC)).unwrap();
        assert!(july.covers(datetime!(2026-07-01 00:00 UTC)));
        assert!(july.covers(datetime!(2026-07-31 20:00 UTC)));
        assert!(!july.covers(datetime!(2026-08-01 00:00 UTC)));
        // Local, not UTC: this instant is already August in Berlin.
        assert!(!july.covers(datetime!(2026-07-31 23:00 UTC)));
    }

    #[test]
    fn operator_and_period_split_the_canonical_form() {
        let s = scope("9900000000001", 2026, 3);
        assert_eq!(s.operator(), "9900000000001");
        assert_eq!(s.period(), "2026-03");
    }

    #[test]
    fn scope_round_trips_through_canonical_form() {
        let s = scope("9900000000001", 2026, 3);
        assert_eq!(s.as_str(), "9900000000001:2026-03");
        assert_eq!(VersionScope::parse(s.as_str()).unwrap(), s);
    }

    #[test]
    fn scope_rejects_separator_in_operator() {
        // Otherwise "a:b" + month would parse back to a different operator.
        assert!(VersionScope::new("bad:operator", 2026, 7).is_err());
    }

    #[test]
    fn scope_rejects_out_of_range_month() {
        assert!(VersionScope::new("9900000000001", 2026, 0).is_err());
        assert!(VersionScope::new("9900000000001", 2026, 13).is_err());
    }
}
