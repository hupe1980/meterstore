//! Table and archival configuration.
//!
//! Validation happens once, at construction, with actionable messages. Settings
//! that must agree with each other are checked here rather than discovered as a
//! performance cliff in production.

use time::Duration;

use crate::arrow::datatypes::{DataType, Field};
use crate::error::{Error, Result};

/// The EIC **object type**, re-exported from `metering`.
///
/// Here because it is part of this module's own signature: a
/// [`ValueCheck::Eic`] carries one, so a caller declaring a Bilanzkreis column
/// needs it in scope and should not have to reach past the crate whose function
/// they are calling to find it.
pub use metering::ids::EicType;

/// Arrow field-metadata key under which an attribute column declares its
/// allowed-value set. The hot-table DDL reads it to render a `CHECK … IN (…)`;
/// it is inert everywhere else (schema evolution compares name/type/nullability
/// only, and the cold tier ignores it), so a coded column stays a plain `Utf8`
/// column with one extra constraint. See [`coded_column`].
pub const CHECK_VALUES_KEY: &str = "meterstore.check_values";

/// A `Utf8` attribute-column [`Field`] constrained to `allowed` values.
///
/// A deployment's coded columns — an ingestion source, a delivery status — get
/// the same DB-layer enforcement meterstore already gives `sparte`/`unit`/
/// `quality`: a value outside the set fails the write rather than being read back
/// later as an unknown flag. meterstore stays domain-agnostic — it renders
/// whatever set the caller supplies. Pass to
/// [`TableConfig::attribute_column`](TableConfig::attribute_column).
///
/// Codes are the domain's stable strings (e.g. `"MSCONS"`); a code must not
/// contain a comma, which delimits the set in the field metadata.
#[must_use]
pub fn coded_column(name: &str, allowed: &[&str], nullable: bool) -> Field {
    debug_assert!(
        allowed.iter().all(|c| !c.contains(',')),
        "coded_column values must not contain a comma"
    );
    Field::new(name, DataType::Utf8, nullable).with_metadata(std::collections::HashMap::from([(
        CHECK_VALUES_KEY.to_string(),
        allowed.join(","),
    )]))
}

/// Arrow field-metadata key under which an attribute column declares the domain
/// identifier its values must parse as.
///
/// Read by the write path, which parses every supplied value with `metering`'s
/// own type and stores that type's canonical spelling. Like [`CHECK_VALUES_KEY`]
/// it is inert everywhere else, so a checked column stays a plain `Utf8` column.
/// See [`ValueCheck`] and [`checked_column`].
pub const VALUE_CHECK_KEY: &str = "meterstore.value_check";

/// The domain identifier scheme a [`checked_column`]'s values must parse as.
///
/// Every one of these is an identifier the German market already validates
/// somewhere, and the argument for parsing it here is the one that already makes
/// `malo_id` a [`MaloId`](metering::ids::MaloId) rather than eleven digits: an
/// identifier stored as an arbitrary string is a string that will one day differ
/// from the identifier it was meant to be, in a column nothing looks at until a
/// settlement run joins on it.
///
/// # Each of these stops somewhere, and it is not the same place
///
/// | | Length | Shape | Arithmetic | Canonicalises |
/// |---|---|---|---|---|
/// | [`Eic`](Self::Eic) | 16 | `0-9 A-Z -`, letter at 3, check character not `-` | **check character** | trim, uppercase |
/// | [`Malo`](Self::Malo) | 11 | digits, first not `0` | **check digit** | trim |
/// | [`Melo`](Self::Melo) | 33 | 2 letters, 6 digits, 25 alphanumerics | none exists | trim, uppercase |
/// | [`Bdew`](Self::Bdew) | 13 | digits | **deliberately not checked** | trim |
///
/// The two with arithmetic are the two where a transposition is *detectable*
/// while the delivery that carried it is still in hand. The other two are still
/// worth declaring: [`Melo`](Self::Melo) canonicalises the casing, and a
/// Messlokation in two casings on an identity column is two readings that never
/// supersede each other; [`Bdew`](Self::Bdew) fixes the length and the alphabet,
/// which is the whole of the rule that can be enforced — see its own
/// documentation for why the thirteenth digit is not.
///
/// Deliberately **not** `#[non_exhaustive]`, unlike [`Error`]: a
/// scheme added here is a scheme a caller matching on this has to decide about,
/// and a `_` arm would let that decision be skipped silently. It is the same
/// convention `metering` uses for its own closed vocabularies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ValueCheck {
    /// The sixteen-character ENTSO-E **Energy Identification Code**, which is
    /// how the German market addresses a Bilanzkreis, a Bilanzierungsgebiet, a
    /// Regelzone and a Metering Grid Area — the two columns
    /// [`TableConfig::attribute_column`] names in its own documentation, and the
    /// ones a MaBiS Summenzeitreihe groups on.
    ///
    /// An EIC carries a **check character**, and unlike the BDEW Codenummer the
    /// scheme has no carve-out, so a mistyped code is detectable.
    ///
    /// # The object type, when the column holds one kind
    ///
    /// `Some(_)` narrows the column to one **object type** — position 3 of the
    /// code, and the only thing distinguishing a Bilanzkreis (`X`, a party) from
    /// a Bilanzierungsgebiet (`Y`, an area). They share the alphabet, the
    /// length and the check character, so an unrefined `check = "EIC"` column
    /// accepts either and only the column *name* says which was meant. A `Y`
    /// code in the `bilanzkreis` column then passes the write path, passes the
    /// `CHECK`, and makes every MaBiS grouping over it wrong.
    ///
    /// Unlike the check character, this half **is** expressible as a regular
    /// expression, so declaring it strengthens the database constraint as well
    /// as the write path — it is the one part of *"is this the right kind of
    /// EIC"* PostgreSQL can enforce on a row this crate did not write.
    ///
    /// `None` accepts any EIC, including one whose type letter this build's
    /// `metering` does not list: the list is ENTSO-E's to extend, and a store
    /// that hard-failed on an entry added after its release would refuse data
    /// the market has already issued.
    ///
    /// That tolerance is worth knowing about when an EIC passes through **two**
    /// parsers in one process and the other one is strict — see
    /// [`eic_object_type`] and [`eic_normalise`], which are how the two are told
    /// apart in a query.
    ///
    /// [`eic_object_type`]: crate::session::udf::EicObjectType
    /// [`eic_normalise`]: crate::session::udf::EicNormalise
    Eic(Option<EicType>),
    /// The eleven-digit **Marktlokations-ID**, and the identifier a *Tranche*
    /// shares the format with.
    ///
    /// A deployment column holds one whenever a reading references a market
    /// location that is not the one it is keyed to — the underlying
    /// Marktlokation of a Kaskade, the parent of a Tranche. Its check digit is
    /// the *Lok- und Waggon-Kennzeichnungsverfahren* and has no exception, so it
    /// is enforced exactly as `malo_id`'s own is.
    Malo,
    /// The thirty-three-character **Messlokations-ID** (Zählpunktbezeichnung) of
    /// VDE-AR-N 4400 / DVGW G 2000.
    ///
    /// No check digit exists, so the structure is the whole of the rule: two
    /// uppercase letters, a six-digit Netzbetreiber number, then twenty-five
    /// alphanumerics. What this adds over a plain string is the **casing** —
    /// values are stored uppercase, so one Messlokation cannot arrive as two.
    Melo,
    /// The thirteen-digit **Marktpartner-ID**, the BDEW- or DVGW-Codenummer
    /// every market participant is addressed by.
    ///
    /// The commonest identifier on a deployment's own columns — a Lieferant, a
    /// Messstellenbetreiber, a Netzbetreiber — and the one whose validation stops
    /// soonest. BDEW *Identifikatoren in der Marktkommunikation* §2.3 gives the
    /// Prüfziffer the same procedure as the MaLo-ID and then carves out
    /// GS1-issued GLNs, which use a different one and are legitimate
    /// Marktpartner-IDs. So the thirteenth digit is **not** enforced, here or in
    /// [`VersionScope`](crate::VersionScope), and thirteen digits is the rule.
    Bdew,
}

impl ValueCheck {
    /// Every value this build accepts, in declaration order.
    ///
    /// The refined EIC forms are members rather than a separate list, so
    /// iterating this is the same thing as iterating the codes — the contract
    /// `metering`'s own coded enums keep between `ALL` and `CODES`.
    ///
    /// Built from [`EicType::ALL`], so a type letter added upstream extends this
    /// array without an edit. What forces the rest of the edit is exhaustiveness:
    /// [`as_str`](Self::as_str), [`shape_pattern`](Self::shape_pattern) and
    /// [`noun`](Self::noun) each match `Option<EicType>` with no `_` arm, so a
    /// new letter fails to compile until all three name it.
    // One unrefined `Eic`, one per `EicType`, and the three non-EIC schemes.
    pub const ALL: [Self; 1 + EicType::ALL.len() + 3] = {
        let types = EicType::ALL;
        // Slot 0 is the unrefined `Eic(None)` the array is filled with; slots
        // 1..=types.len() are the refinements; the last three are the rest.
        let mut out = [Self::Eic(None); 1 + EicType::ALL.len() + 3];
        let mut i = 0;
        while i < types.len() {
            out[1 + i] = Self::Eic(Some(types[i]));
            i += 1;
        }
        out[1 + types.len()] = Self::Malo;
        out[2 + types.len()] = Self::Melo;
        out[3 + types.len()] = Self::Bdew;
        out
    };

    /// The stable code stored in [`VALUE_CHECK_KEY`], which is also the value a
    /// TOML `check =` takes.
    ///
    /// A refined EIC is `EIC:` and the object-type letter — `"EIC:X"` for a
    /// Bilanzkreis, `"EIC:Y"` for a Bilanzierungsgebiet. The letter is
    /// [`EicType::as_str`], so the code, the `serde` tag and what
    /// `eic_object_type` returns are all one spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Eic(None) => "EIC",
            Self::Eic(Some(EicType::Party)) => "EIC:X",
            Self::Eic(Some(EicType::Area)) => "EIC:Y",
            Self::Eic(Some(EicType::MeasurementPoint)) => "EIC:Z",
            Self::Eic(Some(EicType::ResourceObject)) => "EIC:W",
            Self::Eic(Some(EicType::TieLine)) => "EIC:T",
            Self::Eic(Some(EicType::Location)) => "EIC:V",
            Self::Eic(Some(EicType::Substation)) => "EIC:A",
            Self::Malo => "MALO",
            Self::Melo => "MELO",
            Self::Bdew => "BDEW",
        }
    }

    /// The scheme a stored code names, if this build knows it.
    ///
    /// `None` rather than a default: a column declared as checked and silently
    /// written *unchecked* is the one outcome the declaration exists to rule
    /// out, so both readers — the write path and the DDL — refuse an unknown
    /// code rather than ignoring it. That covers `"EIC:Q"` as well as `"IBAN"`:
    /// a refinement this build cannot enforce must not degrade to one it can.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == code)
    }

    /// Every code this build knows, for an error message that says what to write
    /// instead.
    #[must_use]
    pub fn known_codes() -> Vec<&'static str> {
        Self::ALL.into_iter().map(Self::as_str).collect()
    }

    /// The POSIX regular expression the hot table's `CHECK` uses for this
    /// scheme's *shape*.
    ///
    /// The half of each rule a regular expression can carry, and it is anchored
    /// on the **stored** form: `Eic` and `Melo` canonicalise to uppercase, so the
    /// pattern requires uppercase and a row written in lower case by another
    /// writer is refused rather than becoming a second spelling.
    ///
    /// What each pattern cannot express is the arithmetic — the EIC check
    /// character and the MaLo check digit are functions of the other characters,
    /// and the write path enforces those. A row written to PostgreSQL by
    /// something other than this crate is therefore well-*shaped*, not
    /// necessarily well-*formed*.
    ///
    /// An EIC **object type** is not arithmetic, so a refined `Eic` pins
    /// position 3 in the pattern itself and the database enforces it in full.
    #[must_use]
    pub const fn shape_pattern(self) -> &'static str {
        match self {
            // The ENTSO-E Reference Manual's own alphabet: sixteen characters of
            // `0-9`, `A-Z` or `-`, an uppercase letter in position 3 (the object
            // type), and a check character that is never `-` — §5.2 forbids it,
            // so a body computing to one is never issued a code.
            Self::Eic(None) => "^[0-9A-Z-]{2}[A-Z][0-9A-Z-]{12}[0-9A-Z]$",
            // The same, with position 3 pinned to the declared object type.
            // Written out rather than composed, because a `const fn` cannot
            // build a `&'static str` — and written out is also what makes the
            // deployed pattern greppable from the constraint definition.
            Self::Eic(Some(EicType::Party)) => "^[0-9A-Z-]{2}X[0-9A-Z-]{12}[0-9A-Z]$",
            Self::Eic(Some(EicType::Area)) => "^[0-9A-Z-]{2}Y[0-9A-Z-]{12}[0-9A-Z]$",
            Self::Eic(Some(EicType::MeasurementPoint)) => "^[0-9A-Z-]{2}Z[0-9A-Z-]{12}[0-9A-Z]$",
            Self::Eic(Some(EicType::ResourceObject)) => "^[0-9A-Z-]{2}W[0-9A-Z-]{12}[0-9A-Z]$",
            Self::Eic(Some(EicType::TieLine)) => "^[0-9A-Z-]{2}T[0-9A-Z-]{12}[0-9A-Z]$",
            Self::Eic(Some(EicType::Location)) => "^[0-9A-Z-]{2}V[0-9A-Z-]{12}[0-9A-Z]$",
            Self::Eic(Some(EicType::Substation)) => "^[0-9A-Z-]{2}A[0-9A-Z-]{12}[0-9A-Z]$",
            // Eleven digits. No Codevergabestelle issues a leading zero, which
            // is why `MaloId` refuses one — a value that parses nowhere must not
            // pass the shape check either.
            Self::Malo => "^[1-9][0-9]{10}$",
            // Two uppercase letters, six digits, then twenty-five alphanumerics.
            // The Postleitzahl group is deliberately *not* required to be
            // numeric: real Zählpunktbezeichnungen carry letters there, and
            // `MeloId` accepts them.
            Self::Melo => "^[A-Z]{2}[0-9]{6}[0-9A-Z]{25}$",
            // Thirteen digits, and that is the entire enforceable rule.
            Self::Bdew => "^[0-9]{13}$",
        }
    }

    /// Parse `text` with `metering`'s own type and return that type's canonical
    /// spelling, or the error a producer should be shown.
    ///
    /// The write is the only place a check character is worth anything: a wrong
    /// one found in a settlement run a month later names a row and nothing that
    /// could correct it.
    ///
    /// `column` is taken rather than left to a wrapper because the whole message
    /// is composed here: what was supplied, what the scheme wanted, the domain's
    /// own account of the shape, and why the check is at the write. Split across
    /// two layers those four drift apart, and a refusal a producer cannot act on
    /// is a refusal that gets retried unchanged.
    pub fn canonicalise(self, column: &str, text: &str) -> Result<String> {
        let refuse = |reason: String| {
            Error::encode(
                column,
                format!("{text:?} is not {}: {reason} {}", self.noun(), self.why()),
            )
        };
        let parsed = |r: std::result::Result<String, metering::ParseError>| {
            r.map_err(|e| refuse(format!("{e}.")))
        };
        match self {
            Self::Eic(want) => {
                let eic: metering::ids::Eic = text
                    .parse()
                    .map_err(|e: metering::ParseError| refuse(format!("{e}.")))?;
                // The refinement, after the parse: an EIC of the wrong object
                // type is a well-formed code in the wrong column, which is a
                // different fault from a malformed one and deserves to be told
                // apart in the message.
                if let Some(want) = want {
                    let found = eic.object_type();
                    if found != Some(want) {
                        return Err(refuse(format!(
                            "this column declares object type {} ({}), and the code carries \
                             {} at position 3.",
                            want.as_str(),
                            want.name(),
                            found.map_or_else(
                                || format!(
                                    "{:?}, which is not a type letter this build lists",
                                    eic.as_str().chars().nth(2).unwrap_or('?')
                                ),
                                |f| format!("{} ({})", f.as_str(), f.name()),
                            ),
                        )));
                    }
                }
                Ok(eic.into())
            }
            Self::Malo => parsed(text.parse::<metering::ids::MaloId>().map(String::from)),
            Self::Melo => parsed(text.parse::<metering::ids::MeloId>().map(String::from)),
            Self::Bdew => parsed(text.parse::<metering::ids::BdewCode>().map(String::from)),
        }
    }

    /// What a value of this scheme is, as a noun phrase — *"an EIC"*, *"a
    /// MaLo-ID"*.
    ///
    /// The first half of a refusal, before the reason.
    #[must_use]
    pub const fn noun(self) -> &'static str {
        match self {
            Self::Eic(None) => "an EIC",
            Self::Eic(Some(EicType::Party)) => "an X (party) EIC",
            Self::Eic(Some(EicType::Area)) => "a Y (area) EIC",
            Self::Eic(Some(EicType::MeasurementPoint)) => "a Z (measurement point) EIC",
            Self::Eic(Some(EicType::ResourceObject)) => "a W (resource object) EIC",
            Self::Eic(Some(EicType::TieLine)) => "a T (tie-line) EIC",
            Self::Eic(Some(EicType::Location)) => "a V (location) EIC",
            Self::Eic(Some(EicType::Substation)) => "an A (substation) EIC",
            Self::Malo => "a MaLo-ID",
            Self::Melo => "a MeLo-ID",
            Self::Bdew => "a Marktpartner-ID",
        }
    }

    /// Why a producer should care that this one was refused.
    ///
    /// The last half, after the reason. It is the sentence that turns a format
    /// complaint into an instruction: for the two schemes with arithmetic it
    /// says *why here and nowhere later*, and for the two without it says what
    /// the shape is standing in for.
    #[must_use]
    pub const fn why(self) -> &'static str {
        match self {
            Self::Eic(_) => {
                "The check character is part of the code, so a transposition is detectable \
                 here — and only here, while the delivery that carried it is still in hand."
            }
            Self::Malo => {
                "The check digit is part of the identifier, so a transposition is detectable \
                 here — and only here, while the delivery that carried it is still in hand."
            }
            Self::Melo => {
                "There is no check digit, so the structure is the whole of the rule — and a \
                 value that fails it would have been stored as a Messlokation nothing else \
                 names."
            }
            Self::Bdew => {
                "The thirteenth digit is deliberately not checked, because BDEW's \
                 Bildungsvorschrift exempts GS1-issued GLNs — so thirteen digits is the \
                 whole of the rule that can be enforced, and a value failing it is not a \
                 Marktpartner-ID at all."
            }
        }
    }
}

impl std::fmt::Display for ValueCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ValueCheck {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::from_code(s).ok_or_else(|| {
            Error::config(format!(
                "unknown value check {s:?}; this build knows {:?}",
                Self::known_codes()
            ))
        })
    }
}

/// A `Utf8` column whose values must parse as a domain identifier.
///
/// A deployment's own columns get the treatment `malo_id` and `melo_id` already
/// get on the built-in ones: the value is **parsed rather than trusted**, and
/// what is stored is the domain type's canonical spelling. Pass to
/// [`TableConfig::attribute_column`] or [`TableConfig::identity_column`].
///
/// Storing such a value as a plain string defers every check to whoever reads it
/// next, which under a decade-long retention obligation is a settlement run that
/// cannot ask the producer to resend. Canonicalising matters most on an
/// [`identity_column`](TableConfig::identity_column): one identifier in two
/// spellings is two merge keys, so two versions of one reading never supersede
/// each other and every sum over them is inflated.
///
/// **Two checks, and each stops somewhere.** The hot table gets a `CHECK` for
/// the *shape* ([`ValueCheck::shape_pattern`]); the write path enforces whatever
/// arithmetic the scheme has ([`ValueCheck::canonicalise`]). A row written to
/// PostgreSQL by something else gets the first and not the second. [`ValueCheck`]
/// says where each scheme stops.
///
/// ```rust
/// use meterstore::{EicType, TableConfig, ValueCheck, checked_column};
///
/// let table = TableConfig::new("readings_versions")
///     // A Bilanzkreis is an X (party) code and a Bilanzierungsgebiet a Y
///     // (area) one. They differ in nothing else, so naming the object type is
///     // what keeps one out of the other's column.
///     .attribute_column(checked_column(
///         "bilanzkreis",
///         ValueCheck::Eic(Some(EicType::Party)),
///         true,
///     ))
///     .attribute_column(checked_column(
///         "bilanzierungsgebiet",
///         ValueCheck::Eic(Some(EicType::Area)),
///         true,
///     ))
///     .attribute_column(checked_column("lieferant", ValueCheck::Bdew, true))
///     .build()?;
/// assert_eq!(table.attribute_columns().len(), 3);
/// # Ok::<(), meterstore::Error>(())
/// ```
#[must_use]
pub fn checked_column(name: &str, check: ValueCheck, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable).with_metadata(std::collections::HashMap::from([(
        VALUE_CHECK_KEY.to_string(),
        check.as_str().to_string(),
    )]))
}

/// The domain identifier a field's values must parse as, if it declares one.
///
/// One reader for [`VALUE_CHECK_KEY`], so the write path and the DDL cannot
/// disagree about which columns are checked. The **raw** code rather than a
/// [`ValueCheck`], because a declaration this build does not know has to reach
/// both of them as itself: each refuses it, and refusing it by name is what tells
/// an operator which column to fix.
#[must_use]
pub fn declared_value_check(field: &Field) -> Option<&str> {
    field.metadata().get(VALUE_CHECK_KEY).map(String::as_str)
}

/// What a row's timestamps mean: a span, or an instant.
///
/// A **Lastgang** is energy over `[from, to)`. A **Zählerstandsgang** is a
/// cumulative register value at an instant, and BK6-24-174 (in force 06.06.2025)
/// means a German MSB holds one per measuring point at the Lastgang's own
/// cadence — so it is exactly as voluminous, and § 146 Abs. 4 AO forbids
/// discarding it after differencing.
///
/// Both tier the same way, because the watermark, the partition step, the merge
/// key and the balancing day all read the *start* timestamp. Three things
/// differ:
///
/// | | [`Interval`](Self::Interval) | [`Point`](Self::Point) |
/// |---|---|---|
/// | `to` | the span's exclusive end | **null** — an instant has no end |
/// | `value` | energy *in* the span | the register's cumulative reading |
/// | overlap exclusion | on: spans may not overlap | off: instants cannot |
///
/// **They are never one table.** `value` would mean two things in one column,
/// and summing Zählerstände gives a number with no meaning that looks exactly
/// like a consumption total. `to IS NULL` is the row-level signal, so an
/// external engine holding only the Parquet can tell them apart too.
///
/// A zero-width interval is the tempting shortcut and makes `MeterInterval` a
/// lie: `metering` computes `demand_kw` as energy over duration. A point series
/// carries [`MeterReading`](metering::reading::MeterReading).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum TimeModel {
    /// `[from, to)` — a Lastgang. `value` is the energy in the span.
    #[default]
    Interval,
    /// An instant — a Zählerstandsgang. `value` is the register's reading, `to`
    /// is null.
    Point,
}

impl TimeModel {
    /// Whether rows carry a span end.
    pub const fn has_interval_end(self) -> bool {
        matches!(self, Self::Interval)
    }

    /// The stable code, for `system.config` and error messages.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Interval => "INTERVAL",
            Self::Point => "POINT",
        }
    }
}

impl std::fmt::Display for TimeModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Defaults chosen for German 15-minute metering at utility scale.
pub mod defaults {
    use time::Duration;

    /// Archival window size, which is also the hot table's partition
    /// granularity — a purge drops exactly one partition per archived window.
    pub const ARCHIVAL_STEP: Duration = Duration::DAY;
    /// How far behind wall clock archival stays.
    pub const SETTLEMENT_LAG: Duration = Duration::weeks(1);
    /// How far ahead of the write frontier partitions are pre-created.
    pub const PARTITION_HEADROOM: Duration = Duration::weeks(2);
    /// How long an archived partition is retained after its cold commit before
    /// the space is reclaimed. Must outlast the longest query — see
    /// [`TableConfig::reader_grace`](super::TableConfig::reader_grace).
    ///
    /// An hour rather than the maintenance interval: matching it would leave a
    /// query exactly one cycle of protection, and the query this store exists
    /// for is a settlement scan over a year of a six-figure meter population.
    pub const READER_GRACE: Duration = Duration::hours(1);
    /// Target Parquet data file size, in bytes.
    ///
    /// **A cold-tier setting, not a table setting**, and here only as the
    /// recommended value. It is passed where it takes effect —
    /// [`IcebergSqlCatalog::file_target_bytes`] — because that is the object the
    /// Parquet writer belongs to.
    ///
    /// [`IcebergSqlCatalog::file_target_bytes`]: crate::cold::IcebergSqlCatalog::file_target_bytes
    pub const TARGET_FILE_SIZE: usize = 512 * 1024 * 1024;
    /// Rows fetched per round trip when streaming a scan.
    ///
    /// Bounds the memory a scan holds regardless of how much the range covers.
    /// Rows rather than measuring points: meters differ by orders of magnitude
    /// in how much they report, so a fixed number of *them* is a variable amount
    /// of memory.
    pub const SCAN_CHUNK_ROWS: usize = 50_000;
    /// How long cold snapshots are kept.
    ///
    /// Far longer than a general-purpose lakehouse default, because a snapshot
    /// is what makes a past settlement reproducible.
    pub const SNAPSHOT_RETENTION: Duration = Duration::days(3_653);
    /// Snapshots kept regardless of age.
    pub const MIN_SNAPSHOTS_TO_KEEP: usize = 20;
}

/// Everything MeterStore needs to know about one table.
#[derive(Debug, Clone)]
pub struct TableConfig {
    name: String,
    time_model: TimeModel,
    archival_step: Duration,
    settlement_lag: Duration,
    partition_headroom: Duration,
    reader_grace: Duration,
    scan_chunk_rows: usize,
    snapshot_retention: Duration,
    min_snapshots_to_keep: usize,
    melo_in_merge_key: Option<bool>,
    identity_columns: Vec<Field>,
    attribute_columns: Vec<Field>,
    subject_column: Option<String>,
}

impl TableConfig {
    /// Start from the defaults for a table of the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            time_model: TimeModel::Interval,
            archival_step: defaults::ARCHIVAL_STEP,
            settlement_lag: defaults::SETTLEMENT_LAG,
            partition_headroom: defaults::PARTITION_HEADROOM,
            reader_grace: defaults::READER_GRACE,
            scan_chunk_rows: defaults::SCAN_CHUNK_ROWS,
            snapshot_retention: defaults::SNAPSHOT_RETENTION,
            min_snapshots_to_keep: defaults::MIN_SNAPSHOTS_TO_KEEP,
            melo_in_merge_key: None,
            identity_columns: Vec::new(),
            attribute_columns: Vec::new(),
            subject_column: None,
        }
    }

    /// Make the **Messlokation** part of what identifies a reading.
    ///
    /// A Marktlokation may be measured by several Messlokationen, so which of
    /// them a row belongs to is either a label or the identity, depending on the
    /// shape. A load profile belongs to the market location — one channel
    /// however many meters produce it — while a register belongs to the *meter*,
    /// and two meters carry the same OBIS register at the same instants. Keyed
    /// on the Marktlokation alone the second reads as a restatement of the
    /// first.
    ///
    /// So this **defaults to the time model**: on for [`TimeModel::Point`], off
    /// for [`TimeModel::Interval`]. Calling it pins the choice either way.
    ///
    /// `melo_id` then joins [`merge_key`](ValidatedTableConfig::merge_key) — and
    /// so the hot table's primary key, its integrity constraints, the keyset
    /// cursor and the published resolution SQL — becomes `NOT NULL`, and a
    /// delivery naming none is refused.
    pub fn identify_by_melo(mut self, yes: bool) -> Self {
        self.melo_in_merge_key = Some(yes);
        self
    }

    /// Declare whether this table holds spans or instants.
    ///
    /// [`TimeModel::Interval`] by default — a Lastgang. Set
    /// [`TimeModel::Point`] for a Zählerstandsgang, whose rows are register
    /// values at an instant rather than energy over a span. See [`TimeModel`].
    pub fn time_model(mut self, model: TimeModel) -> Self {
        self.time_model = model;
        self
    }

    /// Set the archival window size — **and with it the hot table's partition
    /// granularity**, which is the same number.
    ///
    /// The purge is a partition drop, so one archived window has to be exactly
    /// one partition: a coarser partition would force archival back to a
    /// row-wise `DELETE` — millions of dead tuples a day and the vacuum debt
    /// behind them — and a finer one would multiply partition count for nothing.
    ///
    /// It cannot be changed once a table has archived: the watermark sits on the
    /// old grid, and a window off that grid names a partition relation nothing
    /// creates. [`next_window`](crate::watermark::next_window) refuses rather
    /// than walking the boundary past rows PostgreSQL still holds.
    pub fn archival_step(mut self, step: Duration) -> Self {
        self.archival_step = step;
        self
    }

    /// Set how far behind wall clock archival stays.
    pub fn settlement_lag(mut self, lag: Duration) -> Self {
        self.settlement_lag = lag;
        self
    }

    /// Set how far ahead partitions are pre-created.
    pub fn partition_headroom(mut self, headroom: Duration) -> Self {
        self.partition_headroom = headroom;
        self
    }

    /// Set how long an archived partition is retained before its space is
    /// reclaimed.
    ///
    /// A query reads the watermark when it is **planned** and reads the tiers
    /// when it **executes**, so a plan made before an archival commit still asks
    /// PostgreSQL for the window that commit moved. The partition stays detached
    /// and readable for this long afterwards; without it such a plan comes back
    /// one window short, silently.
    ///
    /// Set it above the longest query the deployment runs. The cost is disk —
    /// one extra partition per `reader_grace` worth of archival — and nothing
    /// else: a retained partition sits below every current boundary, so the range
    /// check skips it before it is scanned.
    ///
    /// **Not the maintenance interval.** That decides how often reclamation is
    /// attempted; this decides what is eligible. Equal values leave a query one
    /// cycle of protection, which is thin for a settlement scan.
    ///
    /// A cold store that cannot report snapshot commit times cannot date an
    /// orphan, and gets no grace.
    pub fn reader_grace(mut self, grace: Duration) -> Self {
        self.reader_grace = grace;
        self
    }

    /// Set how many rows a streaming scan fetches per round trip.
    ///
    /// This is the bound on archival's peak memory: nothing on the path holds a
    /// window, so what it does hold is one chunk.
    pub fn scan_chunk_rows(mut self, rows: usize) -> Self {
        self.scan_chunk_rows = rows;
        self
    }

    /// How long cold snapshots are kept.
    pub fn snapshot_retention(mut self, retention: Duration) -> Self {
        self.snapshot_retention = retention;
        self
    }

    /// Snapshots kept regardless of age.
    pub fn min_snapshots_to_keep(mut self, count: usize) -> Self {
        self.min_snapshots_to_keep = count;
        self
    }

    /// Declare a column that is part of a reading's **identity**.
    ///
    /// Identity columns join the merge key, so two rows differing in one are
    /// different readings and neither can supersede the other.
    ///
    /// A tenant discriminator belongs here. Declared as an attribute instead, two
    /// tenants reporting the same measuring point would share a merge key, and
    /// one tenant's correction would supersede the other's reading — a data leak
    /// with no error anywhere.
    ///
    /// Identity columns must be non-nullable: a null cannot identify anything.
    pub fn identity_column(mut self, field: Field) -> Self {
        self.identity_columns.push(field);
        self
    }

    /// Declare a column that carries **data** about a reading.
    ///
    /// Bilanzkreis, grid area, tariff reference — values that describe a reading
    /// without distinguishing it from another. A correction may change them.
    pub fn attribute_column(mut self, field: Field) -> Self {
        self.attribute_columns.push(field);
        self
    }

    /// Declare the column holding pseudonymous subject references.
    ///
    /// Registers an attribute column of that name and marks it as the link to
    /// the [`SubjectRegistry`](crate::erasure::SubjectRegistry), so writes can
    /// be checked against it and erasure has something to act on.
    ///
    /// # Why this is an attribute and not an identity column
    ///
    /// A measuring point produces one reading per interval no matter who
    /// occupies it, so the reference is functionally determined by the reading
    /// rather than part of what identifies it. Putting it in the merge key would
    /// look harmless and would not be: a correction whose reference was derived
    /// slightly differently — a re-registration, a pipeline reading a stale
    /// mapping — gets a different key and silently fails to supersede the value
    /// it corrects. That is the same failure the OBIS canonicalisation exists to
    /// prevent, and it stays invisible until someone sums a corrected month.
    ///
    /// Erasure does not need it in the key. It destroys the *mapping*, which
    /// leaves every row unattributable regardless of where the column sits.
    ///
    /// # Idempotent, because the column may already be declared
    ///
    /// A deployment that spells the column out —
    /// [`attribute_column`](Self::attribute_column), or an `extra_columns` entry
    /// in TOML — and *then* names it here is saying one thing, not two. An
    /// existing attribute column of that name is adopted rather than re-declared,
    /// which keeps a [`coded_column`]'s vocabulary intact.
    ///
    /// The marker is set either way, and it is the half that matters: without it
    /// [`ValidatedTableConfig::subject_column`] is `None`, the write-path check
    /// against the [`SubjectRegistry`](crate::erasure::SubjectRegistry) never
    /// runs, and a store holding pseudonymous references validates none of them.
    pub fn subject_column(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        if !self.attribute_columns.iter().any(|f| *f.name() == name) {
            self.attribute_columns
                .push(Field::new(&name, DataType::Utf8, true));
        }
        self.subject_column = Some(name);
        self
    }

    /// Validate and freeze.
    pub fn build(self) -> Result<ValidatedTableConfig> {
        // The same rule the declared columns get, and for a stronger reason: the
        // table name reaches PostgreSQL DDL, the partition relation names, the
        // resolution SQL and every scan, always as a quoted identifier and never
        // as a parameter — an identifier cannot be one. Validating it at the one
        // place it enters the system is what lets every one of those sites
        // interpolate it without thinking about quoting.
        //
        // It also keeps the partition-name round trip honest: a partition is
        // named `<table>_<yyyy>_<mm>_<dd>_<hhmm>` and parsed back by stripping the
        // table's own prefix, which a name carrying a quote or a space would make
        // ambiguous.
        if !is_plain_identifier(&self.name) {
            return Err(Error::config(format!(
                "table name {:?} must be a plain identifier — a letter or underscore \
                 followed by letters, digits or underscores. The name is written into \
                 DDL, partition relation names and SQL as an identifier, which cannot \
                 be parameterised",
                self.name
            )));
        }
        // PostgreSQL truncates an identifier at 63 bytes, silently. Every name
        // this crate derives from the table's is longer than the table's own: a
        // partition is `<table>_YYYY_MM_DD_HHMM` (+16) and its integrity
        // constraints add `_one_operator` (+13) on top. At 35 characters the two
        // constraint names on one partition start to truncate to the same
        // string, and `ADD CONSTRAINT` then fails on the second — on the *first
        // write of a new day*, long after the table was declared. Refusing here
        // is the difference between a configuration error and a 3 a.m. one.
        const MAX_TABLE_NAME: usize = 63 - PARTITION_SUFFIX_LEN - LONGEST_CONSTRAINT_SUFFIX;
        if self.name.len() > MAX_TABLE_NAME {
            return Err(Error::config(format!(
                "table name {:?} is {} characters; at most {MAX_TABLE_NAME} fit. \
                 PostgreSQL truncates an identifier at 63 bytes and this crate derives \
                 longer ones from it — a partition relation adds {PARTITION_SUFFIX_LEN} \
                 characters and its integrity constraints another \
                 {LONGEST_CONSTRAINT_SUFFIX}",
                self.name,
                self.name.len(),
            )));
        }
        if self.archival_step <= Duration::ZERO {
            return Err(Error::config("archival_step must be positive"));
        }
        // A partition relation is named `<table>_YYYY_MM_DD_HHMM` — minute
        // granularity, because that is what a name a human reads during an
        // incident should be. A sub-minute step makes two consecutive windows
        // name the *same* relation: the second `ATTACH PARTITION` fails on a
        // relation that already exists, or worse, `from_relation_name` reads an
        // orphan back as the wrong window and drops a partition that is still
        // above the watermark. Neither is discoverable from the setting.
        if self.archival_step < Duration::MINUTE {
            return Err(Error::config(format!(
                "archival_step is {}, and a partition relation is named to the minute \
                 (<table>_YYYY_MM_DD_HHMM) — two consecutive windows would name one \
                 relation. One minute is the floor",
                fmt_duration(self.archival_step),
            )));
        }

        if self.settlement_lag < Duration::ZERO {
            return Err(Error::config("settlement_lag must not be negative"));
        }
        // A lag shorter than one window means archival could close a window that
        // is still receiving corrections, stranding them below the watermark.
        if self.settlement_lag < self.archival_step {
            return Err(Error::config(format!(
                "settlement_lag ({}) must be at least one archival_step ({}), \
                 or corrections can arrive for an already-archived window",
                fmt_duration(self.settlement_lag),
                fmt_duration(self.archival_step),
            )));
        }
        if self.reader_grace < Duration::ZERO {
            return Err(Error::config(
                "reader_grace must not be negative: it is how long an archived \
                 partition is retained so a query planned before the boundary moved \
                 can still read it",
            ));
        }

        if self.partition_headroom < self.archival_step {
            return Err(Error::config(
                "partition_headroom must cover at least one archival_step, \
                 or inserts will fail before a new partition exists",
            ));
        }
        if self.scan_chunk_rows == 0 {
            return Err(Error::config(
                "scan_chunk_rows must be positive: a zero-row chunk would page forever",
            ));
        }
        if self.snapshot_retention <= Duration::ZERO {
            return Err(Error::config("snapshot_retention must be positive"));
        }
        if self.min_snapshots_to_keep == 0 {
            return Err(Error::config(
                "min_snapshots_to_keep must be at least 1: expiring every snapshot \
                 would leave the table unreadable",
            ));
        }

        for f in self.identity_columns.iter().chain(&self.attribute_columns) {
            if !matches!(f.data_type(), DataType::Utf8) {
                return Err(Error::config(format!(
                    "extra column {:?} is {:?}; only Utf8 is supported — every \
                     attribute deployments have wanted (tenant, Bilanzkreis, grid area) \
                     is a string, and supporting more needs a bind arm per type",
                    f.name(),
                    f.data_type()
                )));
            }
        }

        for f in &self.identity_columns {
            if f.is_nullable() {
                return Err(Error::config(format!(
                    "identity column {:?} must be non-nullable: a null cannot identify a reading",
                    f.name()
                )));
            }
        }

        // A pseudonymous reference must never join the merge key. A correction
        // whose reference was derived slightly differently — a re-registration,
        // a pipeline reading a stale mapping — would get a different key and
        // silently fail to supersede the value it corrects. Declared both ways,
        // `build` would otherwise fail with a duplicate-column error about a
        // mistake that is not the one being made.
        if let Some(subject) = &self.subject_column
            && self.identity_columns.iter().any(|f| f.name() == subject)
        {
            return Err(Error::config(format!(
                "{subject:?} is declared both as an identity column and as the subject                  column. A pseudonymous reference must not join the merge key: a correction                  carrying a re-derived reference would get a different key and silently fail                  to supersede the value it corrects. Erasure does not need it in the key — it                  destroys the mapping, which leaves every row unattributable wherever the                  column sits"
            )));
        }

        let mut seen = std::collections::HashSet::new();
        for f in self.identity_columns.iter().chain(&self.attribute_columns) {
            // A declared name reaches PostgreSQL DDL, the resolution SQL, the
            // `unnest` alias and the scan projection — all as a quoted
            // identifier, none of them parameterisable, because an identifier
            // never is. Restricting the alphabet at the one place a name enters
            // the system is what keeps every one of those sites from having to
            // think about quoting, and it also refuses a name that would need
            // quoting to be legible in the first place.
            if !is_plain_identifier(f.name()) {
                return Err(Error::config(format!(
                    "column name {:?} must be a plain identifier — a letter or underscore \
                     followed by letters, digits or underscores. Declared names are written \
                     into DDL and SQL as identifiers, which cannot be parameterised",
                    f.name()
                )));
            }
            if !seen.insert(f.name().clone()) {
                return Err(Error::config(format!("duplicate column {:?}", f.name())));
            }
            if crate::encode::schema::storage_schema(&[])
                .field_with_name(f.name())
                .is_ok()
            {
                return Err(Error::config(format!(
                    "extra column {:?} collides with a core column",
                    f.name()
                )));
            }
        }

        Ok(ValidatedTableConfig(self))
    }
}

/// A [`TableConfig`] that has passed validation.
///
/// Constructing one is the only way to reach the archival machinery, so the
/// checks in [`TableConfig::build`] cannot be skipped.
#[derive(Debug, Clone)]
pub struct ValidatedTableConfig(TableConfig);

impl ValidatedTableConfig {
    /// Table name.
    pub fn name(&self) -> &str {
        &self.0.name
    }
    /// Whether this table holds spans or instants.
    pub fn time_model(&self) -> TimeModel {
        self.0.time_model
    }
    /// Archival window size, which is also the hot table's partition
    /// granularity: one archived window is exactly one partition.
    pub fn archival_step(&self) -> Duration {
        self.0.archival_step
    }
    /// How far behind wall clock archival stays.
    pub fn settlement_lag(&self) -> Duration {
        self.0.settlement_lag
    }
    /// How far ahead partitions are pre-created.
    pub fn partition_headroom(&self) -> Duration {
        self.0.partition_headroom
    }
    /// How long an archived partition is retained before its space is reclaimed.
    pub fn reader_grace(&self) -> Duration {
        self.0.reader_grace
    }
    /// Rows fetched per round trip when streaming a scan.
    pub fn scan_chunk_rows(&self) -> usize {
        self.0.scan_chunk_rows
    }

    /// How a chunked scan of this table's hot tier must be shaped.
    ///
    /// Built here rather than at each call site so the merge key, the deployment
    /// columns and the chunk size cannot disagree between the query path and the
    /// archival path.
    pub fn scan_spec(&self) -> crate::tiering::store::ScanSpec {
        crate::tiering::store::ScanSpec::new(
            self.merge_key(),
            self.extra_columns()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        )
        .with_chunk_rows(self.scan_chunk_rows())
        .with_partition_step(self.archival_step())
    }
    /// How long cold snapshots are kept.
    pub fn snapshot_retention(&self) -> Duration {
        self.0.snapshot_retention
    }

    /// Snapshots kept regardless of age.
    pub fn min_snapshots_to_keep(&self) -> usize {
        self.0.min_snapshots_to_keep
    }

    /// Columns that are part of a reading's identity.
    pub fn identity_columns(&self) -> &[Field] {
        &self.0.identity_columns
    }

    /// The identity columns' names, in declaration order.
    ///
    /// These are also the cold tier's leading partition fields: an identity
    /// column is by definition something every query filters on, which is
    /// exactly what a partition field should be.
    pub fn identity_column_names(&self) -> Vec<String> {
        self.0
            .identity_columns
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Columns that carry data about a reading.
    pub fn attribute_columns(&self) -> &[Field] {
        &self.0.attribute_columns
    }

    /// The column holding pseudonymous subject references, if declared.
    pub fn subject_column(&self) -> Option<&str> {
        self.0.subject_column.as_deref()
    }

    /// Every deployment-declared column, identity first.
    ///
    /// Identity columns come first so their position is stable as attributes are
    /// added — the storage schema appends them after the core columns in this
    /// order.
    pub fn extra_columns(&self) -> Vec<Field> {
        self.0
            .identity_columns
            .iter()
            .chain(&self.0.attribute_columns)
            .cloned()
            .collect()
    }

    /// Whether the Messlokation is part of what identifies a reading.
    ///
    /// **Defaults to the time model** where
    /// [`identify_by_melo`](TableConfig::identify_by_melo) was not called: on
    /// for [`TimeModel::Point`], off for [`TimeModel::Interval`]. A register
    /// belongs to a meter and a load profile belongs to a market location, so
    /// that is the right answer for each shape, and the wrong one is the sort
    /// that shows up as one of two meters' readings quietly not being stored.
    pub fn melo_in_merge_key(&self) -> bool {
        self.0
            .melo_in_merge_key
            .unwrap_or(matches!(self.0.time_model, TimeModel::Point))
    }

    /// The full merge key: the core columns, the Messlokation if it identifies
    /// a reading here, and any identity columns.
    ///
    /// This is what decides whether one reading supersedes another, so it is
    /// derived in one place and used by the schema, the hot-table primary key
    /// and the resolution SQL alike.
    ///
    /// `melo_id` sits directly after `malo_id` rather than at the end: the key
    /// is also the hot table's primary index, and the dominant read narrows by
    /// measuring point first.
    pub fn merge_key(&self) -> Vec<String> {
        let mut key = vec![crate::encode::schema::col::MALO_ID.to_string()];
        if self.melo_in_merge_key() {
            key.push(crate::encode::schema::col::MELO_ID.to_string());
        }
        key.extend(
            crate::encode::schema::MERGE_KEY
                .iter()
                .filter(|c| **c != crate::encode::schema::col::MALO_ID)
                .map(|s| (*s).to_string()),
        );
        key.extend(self.0.identity_columns.iter().map(|f| f.name().clone()));
        key
    }

    /// The merge-key columns beyond the three every table shares.
    ///
    /// What separates two rows that agree on `(malo_id, obis_code, from)`: the
    /// Messlokation where it identifies a reading, then the declared identity
    /// columns. Every path that has to name a reading — the cold tier's
    /// reconciliation, the displacement report, the keyset cursor — asks for
    /// this rather than for `identity_column_names`, which is the same list only
    /// where `melo_id` is not in the key.
    pub fn discriminator_columns(&self) -> Vec<String> {
        let core = crate::encode::schema::MERGE_KEY;
        self.merge_key()
            .into_iter()
            .filter(|c| !core.contains(&c.as_str()))
            .collect()
    }

    /// How many hot partitions should exist at steady state.
    ///
    /// Enough to cover the settlement lag plus the pre-creation headroom.
    pub fn expected_hot_partitions(&self) -> i64 {
        let span = self.0.settlement_lag + self.0.partition_headroom;
        (span.whole_seconds() / self.0.archival_step.whole_seconds()).max(1)
    }
}

/// Characters a partition relation name adds to its table's: `_YYYY_MM_DD_HHMM`.
///
/// Kept next to the check that uses it rather than derived from
/// [`PartitionId::relation_name`](crate::tiering::store::PartitionId::relation_name),
/// because the format is a constant there and a length is what is needed here.
const PARTITION_SUFFIX_LEN: usize = "_2026_07_20_0000".len();

/// Characters the longest integrity-constraint name adds to a partition's:
/// `_one_operator`, from `hot::postgres`.
const LONGEST_CONSTRAINT_SUFFIX: usize = "_one_operator".len();

/// Whether `name` is a bare SQL identifier needing no quoting or escaping.
///
/// ASCII-only on purpose. A non-ASCII identifier is legal in both PostgreSQL and
/// Iceberg, but it has to survive DDL, Parquet field names, a `CHECK` rendered
/// into a string, and whatever engine an operator later points at the warehouse —
/// and the first place it goes wrong is the one nobody is watching.
fn is_plain_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Render a duration the way the configuration file spells it.
fn fmt_duration(d: Duration) -> String {
    let secs = d.whole_seconds();
    match secs {
        s if s % 86_400 == 0 => format!("{}d", s / 86_400),
        s if s % 3_600 == 0 => format!("{}h", s / 3_600),
        s if s % 60 == 0 => format!("{}m", s / 60),
        s => format!("{s}s"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::datatypes::DataType;

    fn base() -> TableConfig {
        TableConfig::new("readings")
    }

    #[test]
    fn coded_column_carries_its_vocabulary_in_metadata() {
        let f = coded_column("source", &["MSCONS", "DIRECT_PUSH"], true);
        assert_eq!(f.name(), "source");
        assert_eq!(f.data_type(), &DataType::Utf8);
        assert!(f.is_nullable());
        assert_eq!(
            f.metadata().get(CHECK_VALUES_KEY).map(String::as_str),
            Some("MSCONS,DIRECT_PUSH")
        );
    }

    #[test]
    fn a_checked_column_declares_the_identifier_scheme_its_values_must_parse_as() {
        for scheme in ValueCheck::ALL {
            let f = checked_column("c", scheme, true);
            assert_eq!(f.data_type(), &DataType::Utf8);
            assert!(f.is_nullable());
            assert_eq!(declared_value_check(&f), Some(scheme.as_str()));
            // A vocabulary and an identifier scheme are different claims, so
            // the one key does not carry the other's value.
            assert!(f.metadata().get(CHECK_VALUES_KEY).is_none());
        }
        assert_eq!(declared_value_check(&coded_column("s", &["A"], true)), None);
        assert_eq!(
            declared_value_check(&Field::new("plain", DataType::Utf8, true)),
            None
        );
    }

    #[test]
    fn a_checked_column_may_be_an_identity_column() {
        // Which is where the check earns most: a Bilanzkreis in the merge key
        // arriving in two spellings is two readings that never supersede each
        // other.
        let c = base()
            .identity_column(checked_column(
                "bilanzkreis",
                ValueCheck::Eic(Some(EicType::Party)),
                false,
            ))
            .build()
            .unwrap();
        assert!(c.merge_key().contains(&"bilanzkreis".to_string()));
        assert_eq!(
            declared_value_check(&c.identity_columns()[0]),
            Some("EIC:X")
        );
    }

    #[test]
    fn a_value_checks_code_round_trips_and_is_the_only_spelling() {
        // The code is written into Arrow field metadata, read back by the DDL
        // and by the write path, and typed into a TOML file by hand. One
        // spelling, and `from_code` is the only reader of it.
        for scheme in ValueCheck::ALL {
            assert_eq!(ValueCheck::from_code(scheme.as_str()), Some(scheme));
            assert_eq!(scheme.to_string(), scheme.as_str());
            assert_eq!(
                scheme.as_str().parse::<ValueCheck>().expect("known"),
                scheme
            );
        }
        assert_eq!(
            ValueCheck::known_codes(),
            vec![
                "EIC", "EIC:X", "EIC:Y", "EIC:Z", "EIC:W", "EIC:T", "EIC:V", "EIC:A", "MALO",
                "MELO", "BDEW",
            ]
        );
        // Every object-type letter `metering` lists is declarable. The const
        // assertion on `ALL` holds the count; this holds the mapping, so a
        // letter added upstream cannot be added to `ALL` under the wrong code.
        for ty in EicType::ALL {
            assert_eq!(
                ValueCheck::from_code(&format!("EIC:{}", ty.as_str())),
                Some(ValueCheck::Eic(Some(ty))),
                "{ty}"
            );
        }

        // A refinement this build cannot enforce must not degrade to one it can:
        // `EIC:Q` is refused outright rather than read as a bare `EIC`.
        for unknown in ["IBAN", "eic", "EIC:", "EIC:Q", "EIC:XX", "EIC:x", "MALO:X"] {
            assert_eq!(ValueCheck::from_code(unknown), None, "{unknown}");
        }

        // An unknown code names itself and the alternatives, so an operator who
        // typed one can see what to type instead.
        let err = "EIC:Q"
            .parse::<ValueCheck>()
            .expect_err("unknown")
            .to_string();
        assert!(err.contains("EIC:Q"), "{err}");
        assert!(err.contains("EIC:X"), "{err}");
    }

    #[test]
    fn canonicalising_is_the_domain_types_own_spelling() {
        // The whole point of a checked column: what is stored is the domain
        // type's canonical form, so one identifier cannot become two merge keys.
        let ok = |c: ValueCheck, text: &str| c.canonicalise("declared", text).unwrap();
        assert_eq!(
            ok(ValueCheck::Eic(None), "  11xbk0000000001a  "),
            "11XBK0000000001A"
        );
        assert_eq!(
            ok(ValueCheck::Melo, " de00056266802ao6g56m11sn51g21m24s "),
            "DE00056266802AO6G56M11SN51G21M24S"
        );
        assert_eq!(ok(ValueCheck::Malo, " 41373559241 "), "41373559241");
        assert_eq!(ok(ValueCheck::Bdew, " 9900987654321 "), "9900987654321");

        // And where each one stops. The two with arithmetic refuse a
        // transposition; the Marktpartner-ID cannot, because BDEW's own
        // Bildungsvorschrift carves out GS1-issued GLNs.
        let bad = |c: ValueCheck, text: &str| {
            c.canonicalise("declared", text)
                .expect_err(text)
                .to_string()
        };
        assert!(bad(ValueCheck::Eic(None), "11XBK0000000001B").contains("declared"));
        assert!(bad(ValueCheck::Malo, "41373559214").contains("check digit"));
        assert_eq!(ok(ValueCheck::Bdew, "9900987654320"), "9900987654320");
    }

    #[test]
    fn an_eic_of_the_wrong_object_type_is_refused_and_the_message_says_which() {
        // The failure this refinement exists for. `11XBK0000000001A` and
        // `11YN000000000016` are both valid EICs of the same length under the
        // same issuing office, and only position 3 says one is a Bilanzkreis and
        // the other a Bilanzierungsgebiet. Undeclared, the column takes either.
        let bilanzkreis = ValueCheck::Eic(Some(EicType::Party));
        assert_eq!(
            bilanzkreis
                .canonicalise("bilanzkreis", "11XBK0000000001A")
                .unwrap(),
            "11XBK0000000001A"
        );

        let err = bilanzkreis
            .canonicalise("bilanzkreis", "11YN000000000016")
            .expect_err("an area code is not a party code")
            .to_string();
        assert!(err.contains("bilanzkreis"), "{err}");
        // Both sides of the mismatch, because "invalid" would send the producer
        // to read the code they already have in front of them.
        assert!(err.contains("X (Party)"), "{err}");
        assert!(err.contains("Y (Area or Domain)"), "{err}");

        // A malformed code is still refused as malformed rather than as a type
        // mismatch: the two are different faults and the message says which.
        let malformed = bilanzkreis
            .canonicalise("bilanzkreis", "11XBK0000000001B")
            .expect_err("the check character is wrong")
            .to_string();
        assert!(!malformed.contains("object type"), "{malformed}");

        // And the unrefined form still takes either, which is what makes the
        // refinement a declaration rather than a rule of the scheme.
        for code in ["11XBK0000000001A", "11YN000000000016"] {
            assert!(
                ValueCheck::Eic(None).canonicalise("c", code).is_ok(),
                "{code}"
            );
        }
    }

    #[test]
    fn naming_an_already_declared_column_as_the_subject_column_adopts_it() {
        // One statement, not two. Pushing a second field of the same name would
        // fail `build` with a duplicate-column error about a mistake nobody
        // made, and would discard the deployment's own declaration — its
        // vocabulary included.
        let c = base()
            .attribute_column(coded_column("subject_ref", &["A", "B"], true))
            .subject_column("subject_ref")
            .build()
            .unwrap();

        assert_eq!(c.subject_column(), Some("subject_ref"));
        let declared: Vec<_> = c
            .attribute_columns()
            .iter()
            .filter(|f| f.name() == "subject_ref")
            .collect();
        assert_eq!(declared.len(), 1);
        assert_eq!(
            declared[0]
                .metadata()
                .get(CHECK_VALUES_KEY)
                .map(String::as_str),
            Some("A,B"),
        );
    }

    #[test]
    fn a_subject_column_may_not_be_an_identity_column() {
        // In the merge key it looks harmless and is not: a correction whose
        // reference was re-derived — a re-registration, a stale mapping — gets a
        // different key and silently fails to supersede the value it corrects.
        let err = base()
            .identity_column(Field::new("subject_ref", DataType::Utf8, false))
            .subject_column("subject_ref")
            .build()
            .unwrap_err()
            .to_string();
        assert!(err.contains("merge key"), "{err}");
    }

    #[test]
    fn a_plain_attribute_column_declares_no_vocabulary() {
        let f = Field::new("bilanzkreis", DataType::Utf8, true);
        assert!(f.metadata().get(CHECK_VALUES_KEY).is_none());
    }

    #[test]
    fn defaults_validate() {
        let c = base().build().unwrap();
        assert_eq!(c.name(), "readings");
        assert_eq!(c.archival_step(), Duration::DAY);
    }

    #[test]
    fn settlement_lag_must_cover_at_least_one_window() {
        // Otherwise a window can be archived while still receiving corrections.
        let err = base()
            .archival_step(Duration::days(7))
            .settlement_lag(Duration::DAY)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("settlement_lag"));
    }

    #[test]
    fn zero_settlement_lag_is_rejected_when_a_window_is_positive() {
        assert!(base().settlement_lag(Duration::ZERO).build().is_err());
    }

    #[test]
    fn headroom_must_cover_a_partition() {
        let err = base()
            .partition_headroom(Duration::hours(1))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("partition_headroom"));
    }

    #[test]
    fn non_positive_steps_are_rejected() {
        assert!(base().archival_step(Duration::ZERO).build().is_err());
        assert!(base().archival_step(-Duration::DAY).build().is_err());
    }

    #[test]
    fn a_sub_minute_step_is_refused_because_partitions_are_named_to_the_minute() {
        // `<table>_YYYY_MM_DD_HHMM`: at 30 seconds, two consecutive windows name
        // one relation. The second attach fails, and an orphan read back by name
        // resolves to the wrong window — so a partition still above the
        // watermark could be dropped.
        let err = base()
            .archival_step(Duration::seconds(30))
            .settlement_lag(Duration::minutes(5))
            .partition_headroom(Duration::minutes(5))
            .build()
            .unwrap_err()
            .to_string();
        assert!(err.contains("minute"), "{err}");

        // One minute is legal, and so is everything above it.
        assert!(
            base()
                .archival_step(Duration::MINUTE)
                .settlement_lag(Duration::minutes(5))
                .partition_headroom(Duration::minutes(5))
                .build()
                .is_ok()
        );
    }

    #[test]
    fn a_table_name_must_be_a_plain_identifier() {
        // The name reaches DDL, partition relation names and every scan as a
        // quoted identifier and never as a parameter. Declared *column* names
        // were already held to this rule; the table name carries more of it.
        assert!(TableConfig::new("").build().is_err());
        assert!(
            TableConfig::new("readings\"; DROP TABLE x --")
                .build()
                .is_err()
        );
        assert!(TableConfig::new("has space").build().is_err());
        assert!(TableConfig::new("1_leading_digit").build().is_err());
        assert!(TableConfig::new("Messwerte_Ä").build().is_err());

        assert!(TableConfig::new("readings").build().is_ok());
        assert!(TableConfig::new("readings_versions").build().is_ok());
        assert!(TableConfig::new("_private2").build().is_ok());
    }

    #[test]
    fn the_scan_spec_carries_everything_a_chunked_scan_needs() {
        // Built in one place so the query path and the archival path cannot end
        // up with different keys, different columns or different bounds.
        let c = base()
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .scan_chunk_rows(1234)
            .build()
            .unwrap();
        let spec = c.scan_spec();

        assert_eq!(spec.merge_key(), c.merge_key().as_slice());
        assert_eq!(spec.extra(), ["tenant", "bilanzkreis"]);
        assert_eq!(spec.chunk_rows(), Some(1234));
        // The cursor must be unique per row, and start with the declared sort
        // order so the Parquet footer stays truthful.
        assert_eq!(
            spec.cursor_columns(),
            ["malo_id", "from", "obis_code", "tenant", "version"]
        );
    }

    #[test]
    fn a_zero_row_chunk_is_rejected() {
        assert!(base().scan_chunk_rows(0).build().is_err());
    }

    #[test]
    fn a_column_name_that_is_not_an_identifier_is_refused() {
        // Declared names are written into DDL, the resolution SQL, the `unnest`
        // alias and the scan projection as quoted identifiers — none of which can
        // be parameterised. Restricting the alphabet once, here, is what lets
        // every one of those sites stop thinking about quoting.
        for bad in [
            r#"tenant" ; DROP TABLE readings --"#,
            "has space",
            "1leading_digit",
            "",
            "dotted.name",
            "kebab-case",
        ] {
            let err = base()
                .attribute_column(Field::new(bad, DataType::Utf8, true))
                .build()
                .unwrap_err();
            assert!(
                err.to_string().contains("plain identifier"),
                "{bad:?} was accepted: {err}"
            );
        }

        for good in ["tenant", "_private", "bilanzkreis_2", "NetzGebiet"] {
            assert!(
                base()
                    .attribute_column(Field::new(good, DataType::Utf8, true))
                    .build()
                    .is_ok(),
                "{good:?} was refused"
            );
        }
    }

    #[test]
    fn extra_columns_must_not_collide_with_core_columns() {
        let err = base()
            .attribute_column(Field::new("malo_id", DataType::Utf8, true))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("collides"));
    }

    #[test]
    fn extra_columns_must_be_unique_across_both_kinds() {
        let err = base()
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn identity_columns_join_the_merge_key() {
        // The point of the distinction: a tenant discriminator must make two
        // readings different readings, not merely differently-labelled ones.
        let c = base()
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            .build()
            .unwrap();
        assert_eq!(
            c.merge_key(),
            vec!["malo_id", "obis_code", "from", "tenant"]
        );
    }

    #[test]
    fn a_point_table_identifies_a_reading_by_its_messlokation_by_default() {
        // A register belongs to a meter. A Marktlokation may be measured by
        // several — a Mehrfamilienhaus, a house with an Einliegerwohnung — and
        // each of them carries the same OBIS register at the same instants, so
        // keyed on the Marktlokation alone the two are one reading.
        let point = base().time_model(TimeModel::Point).build().unwrap();
        assert!(point.melo_in_merge_key());
        assert_eq!(
            point.merge_key(),
            vec!["malo_id", "melo_id", "obis_code", "from"]
        );
        assert_eq!(point.discriminator_columns(), vec!["melo_id"]);
    }

    #[test]
    fn an_interval_table_does_not() {
        // A market location's load profile is one channel however many meters
        // produce it, so the Messlokation labels the row and does not name it.
        let lastgang = base().build().unwrap();
        assert!(!lastgang.melo_in_merge_key());
        assert_eq!(lastgang.merge_key(), vec!["malo_id", "obis_code", "from"]);
        assert!(lastgang.discriminator_columns().is_empty());
    }

    #[test]
    fn identify_by_melo_pins_the_choice_either_way() {
        // The default follows the time model; declaring it overrides that, for
        // a single-meter portfolio one way and a sub-metering one the other.
        assert!(
            !base()
                .time_model(TimeModel::Point)
                .identify_by_melo(false)
                .build()
                .unwrap()
                .melo_in_merge_key()
        );
        assert!(
            base()
                .identify_by_melo(true)
                .build()
                .unwrap()
                .melo_in_merge_key()
        );
    }

    #[test]
    fn the_messlokation_leads_the_key_and_identity_columns_follow() {
        // The merge key is also the hot table's primary index, and the dominant
        // read narrows by measuring point first.
        let c = base()
            .identify_by_melo(true)
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            .build()
            .unwrap();
        assert_eq!(
            c.merge_key(),
            vec!["malo_id", "melo_id", "obis_code", "from", "tenant"]
        );
        assert_eq!(c.discriminator_columns(), vec!["melo_id", "tenant"]);
        // The keyset cursor keeps the declared sort order as its prefix.
        assert_eq!(
            c.scan_spec().cursor_columns(),
            [
                "malo_id",
                "from",
                "melo_id",
                "obis_code",
                "tenant",
                "version"
            ]
        );
    }

    #[test]
    fn a_table_name_too_long_for_postgres_identifiers_is_refused() {
        // A partition adds 16 characters and its constraints another 13. Past
        // 34, PostgreSQL truncates at 63 bytes and the second constraint on a
        // partition collides with the first — on the day's first write, not at
        // declaration.
        let longest = "a".repeat(34);
        assert!(TableConfig::new(&longest).build().is_ok());

        let err = TableConfig::new("a".repeat(35)).build().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("63"), "{msg}");
        assert!(msg.contains("34"), "message must name the limit: {msg}");
    }

    #[test]
    fn attribute_columns_stay_out_of_the_merge_key() {
        let c = base()
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .build()
            .unwrap();
        assert_eq!(c.merge_key(), vec!["malo_id", "obis_code", "from"]);
    }

    #[test]
    fn a_nullable_identity_column_is_rejected() {
        // A null cannot identify a reading, and in SQL it does not compare equal
        // to itself — two such rows would never resolve against each other.
        let err = base()
            .identity_column(Field::new("tenant", DataType::Utf8, true))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("non-nullable"));
    }

    #[test]
    fn identity_columns_come_before_attributes_in_the_schema() {
        let c = base()
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            .build()
            .unwrap();
        let names: Vec<_> = c.extra_columns().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, ["tenant", "bilanzkreis"]);
    }

    #[test]
    fn valid_extra_columns_are_kept_in_order() {
        let c = base()
            .attribute_column(Field::new("bilanzkreis", DataType::Utf8, true))
            .attribute_column(Field::new("netzgebiet", DataType::Utf8, true))
            .build()
            .unwrap();
        let extra = c.extra_columns();
        let names: Vec<_> = extra.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["bilanzkreis", "netzgebiet"]);
    }

    #[test]
    fn expected_hot_partitions_covers_lag_plus_headroom() {
        // 7d lag + 14d headroom at 1d granularity.
        assert_eq!(base().build().unwrap().expected_hot_partitions(), 21);

        let weekly = base()
            .archival_step(Duration::weeks(1))
            .settlement_lag(Duration::weeks(2))
            .partition_headroom(Duration::weeks(2))
            .build()
            .unwrap();
        assert_eq!(weekly.expected_hot_partitions(), 4);
    }

    #[test]
    fn duration_formatting_is_human_readable() {
        assert_eq!(fmt_duration(Duration::DAY), "1d");
        assert_eq!(fmt_duration(Duration::weeks(1)), "7d");
        assert_eq!(fmt_duration(Duration::hours(6)), "6h");
        assert_eq!(fmt_duration(Duration::minutes(15)), "15m");
        assert_eq!(fmt_duration(Duration::seconds(90)), "90s");
    }
}
