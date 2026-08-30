//! SQL functions for local calendar grouping.
//!
//! Daily and monthly aggregates must be grouped by **local** calendar day.
//! `Europe/Berlin` observes daylight saving, so the UTC day boundary sits at
//! 01:00 or 02:00 local — which means `date_trunc('day', "from")` produces wrong
//! daily sums for every customer on every day of the year, not only at the two
//! transitions. These functions exist so the correct grouping is the convenient
//! one.
//!
//! # Gas is grouped on a different day
//!
//! The German gas market balances on the **Gastag**, 06:00 to 06:00 local, not
//! on the calendar day — so `meter_local_day` is the *wrong* function for a gas
//! Lastgang in precisely the way `date_trunc` is wrong for an electricity one.
//! [`GasDay`] (`meter_gas_day`) is its counterpart, and [`BalancingDay`]
//! (`meter_balancing_day`) chooses between them from the row's `sparte`, so a
//! statement over a mixed table is written once and is right for both.
//!
//! The boundary carries up to the **month**, so [`BalancingMonth`]
//! (`meter_balancing_month`) is the same choice one period up: the gas
//! Bilanzierungsmonat runs 01.06 06:00 to 01.07 06:00, and grouping it by
//! [`LocalMonth`] books six hours into the neighbouring settlement month twelve
//! times a year.
//!
//! # And the OBIS functions
//!
//! `ObisPredicate` wraps `metering::obis`'s own predicates, [`ObisDirection`]
//! the primitive the two directional ones are derived from — three-valued,
//! because a register may have no direction at all — and [`ObisTariffRegister`]
//! and [`ObisNormalise`] the two accessors a query needs.
//!
//! [`EicRegelzone`] does the same for the other identifier a deployment stores:
//! a Bilanzierungsgebiet's Regelzone is in position 4 of its EIC, which is a
//! paragraph of BDEW citation upstream and one line here.
//!
//! The arithmetic is `metering::calendar`'s. These are wrappers: they convert
//! between Arrow arrays and the domain functions and nothing else. Their tests
//! assert that the wrapper preserves the upstream answer, not that the answer is
//! right — that is `metering`'s suite's job, plus the contract test in
//! `tests/it/calendar_delegation.rs`.

use std::any::Any;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use metering::IntervalResolution;
use metering::calendar;
use metering::ids::{Eic, Regelzone};
use metering::interval::{Direction, Sparte};
use metering::obis::ObisCode;
use time::{Date, OffsetDateTime};

use crate::arrow::array::{
    Array, Date32Array, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use crate::arrow::datatypes::{DataType, TimeUnit};
use crate::planner::calendar as balancing;

/// Days between the Unix epoch and a date, for `Date32`.
///
/// The storage encoding's own, so a day one of these functions returns and a day
/// the encoder wrote into `balancing_day` are the same number by construction
/// rather than by two conversions agreeing.
fn to_date32(date: Date) -> i32 {
    crate::encode::schema::date32(date)
}

/// The instant a microsecond timestamp represents.
///
/// [`schema::instant`](crate::encode::schema::instant) in DataFusion's error
/// type: the value comes off a column, so a file this crate did not write can
/// put anything there and failing beats panicking.
fn from_micros(micros: i64) -> DfResult<OffsetDateTime> {
    crate::encode::schema::instant(micros)
        .map_err(|e| DataFusionError::Execution(format!("timestamp out of range: {e}")))
}

/// Accepts a UTC timestamp in any precision, so callers need not cast.
fn timestamp_signature() -> Signature {
    timestamp_signature_with(&[])
}

/// As [`timestamp_signature`], with `trailing` argument types appended.
///
/// The precision cross-product is the reason this is generated rather than
/// written out: a two-argument function accepting four time units in two
/// nullability spellings is eight exact signatures, and hand-listing them is how
/// one gets forgotten and a perfectly ordinary column fails to bind.
fn timestamp_signature_with(trailing: &[DataType]) -> Signature {
    Signature::one_of(
        [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ]
        .into_iter()
        .flat_map(|unit| {
            [Some("UTC".into()), None].map(|tz| {
                let mut args = vec![DataType::Timestamp(unit, tz)];
                args.extend_from_slice(trailing);
                TypeSignature::Exact(args)
            })
        })
        .collect(),
        Volatility::Immutable,
    )
}

/// Read a `Utf8` argument as an array, whether it arrived as a column or a
/// literal.
///
/// A literal is broadcast by `into_array`, so both spellings are handled by one
/// path — and, crucially, the value is read **per row**. `sparte` is a column on
/// every real table, and applying the first row's commodity to the rest would
/// group a whole gas table on the electricity day.
fn as_strings(args: &ScalarFunctionArgs, index: usize) -> DfResult<StringArray> {
    let array = args.args[index].clone().into_array(args.number_rows)?;
    Ok(crate::arrow::array::AsArray::as_string_opt::<i32>(&array)
        .ok_or_else(|| {
            DataFusionError::Execution(format!("argument {} must be a string", index + 1))
        })?
        .clone())
}

/// Parse a stored `sparte` code, naming the accepted set on failure.
fn parse_sparte(s: &str) -> DfResult<Sparte> {
    s.parse().map_err(|e| {
        DataFusionError::Execution(format!(
            "bad sparte {s:?}: {e} — expected one of {:?}",
            Sparte::CODES
        ))
    })
}

/// Coerce an argument to microsecond timestamps.
fn as_micros(args: &ScalarFunctionArgs) -> DfResult<TimestampMicrosecondArray> {
    let array = args.args[0].clone().into_array(args.number_rows)?;
    let cast = crate::arrow::compute::cast(
        &array,
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    )?;
    Ok(cast
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| DataFusionError::Execution("expected a timestamp argument".into()))?
        .clone())
}

/// Read a `Utf8` argument and parse each row as an OBIS code.
///
/// Nulls pass through as nulls; anything else that is not an OBIS code is an
/// error rather than a null, for the same reason a bad `sparte` is: storage
/// holds canonical codes and only canonical codes, so an unparseable one means
/// the row was written by something that did not honour that.
fn as_obis(args: &ScalarFunctionArgs) -> DfResult<(StringArray, Vec<Option<ObisCode>>)> {
    let raw = as_strings(args, 0)?;
    let mut out = Vec::with_capacity(raw.len());
    for i in 0..raw.len() {
        if raw.is_null(i) {
            out.push(None);
            continue;
        }
        let text = raw.value(i);
        out.push(Some(text.parse::<ObisCode>().map_err(|e| {
            DataFusionError::Execution(format!("{text:?} is not an OBIS code: {e}"))
        })?));
    }
    Ok((raw, out))
}

/// One of `metering::obis`'s predicates, as a SQL function.
///
/// # Why these are wrappers and nothing more
///
/// Exactly as the calendar functions are. "Which registers may be summed into
/// one kWh figure" is a rule about OBIS, and OBIS belongs to `metering` — the
/// direction lives in value group C *and only for electricity*, `E = 63` is a
/// fault counter rather than tariff 63, `D = 6` is a kW maximum and `D = 29` the
/// kWh load profile it is derived from. Each of those is one line here and a
/// paragraph of Codeliste citation upstream.
///
/// So there is deliberately no `obis_is_energy`: it would be a *composition* —
/// "not reactive, not a maximum, not a fault counter" — and composing a new
/// domain rule in the storage layer is how a second implementation starts. The
/// composition is spelled out in the documentation as SQL, where a reader can
/// see which three rules it rests on.
///
/// # The medium is already in the code
///
/// [`ObisCode::is_import`] tests `a == 1 && c == 1`, so it is false for a gas
/// code without being told the commodity: value group C is a Messgröße for gas,
/// not a direction, and value group A says which medium it is. Taking a `sparte`
/// argument would put a second source for that fact beside the one already in
/// the code, and the two could disagree.
struct ObisPredicate {
    name: &'static str,
    test: fn(&ObisCode) -> bool,
    signature: Signature,
}

impl std::fmt::Debug for ObisPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObisPredicate")
            .field("name", &self.name)
            .finish()
    }
}

impl PartialEq for ObisPredicate {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for ObisPredicate {}
impl std::hash::Hash for ObisPredicate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ObisPredicate {
    fn new(name: &'static str, test: fn(&ObisCode) -> bool) -> Self {
        Self {
            name,
            test,
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObisPredicate {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let (_, codes) = as_obis(&args)?;
        let out: crate::arrow::array::BooleanArray =
            codes.iter().map(|c| c.map(|c| (self.test)(&c))).collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// `obis_tariff_register(code)` — the tariff number, or null.
///
/// Null for the **total** register (`E = 0`) and null for the
/// **Fehlerregister** (`E = 63`), which is `metering`'s own rule: reporting the
/// fault counter as `63` invites a caller to bill it as tariff 63's consumption.
/// `obis_is_total_register` and `obis_is_fehlerregister` tell the two apart.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ObisTariffRegister {
    signature: Signature,
}

impl Default for ObisTariffRegister {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObisTariffRegister {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "obis_tariff_register"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::UInt8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let (_, codes) = as_obis(&args)?;
        let out: crate::arrow::array::UInt8Array = codes
            .iter()
            .map(|c| c.and_then(|c| c.tariff_register()))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// `obis_normalise(code)` — the canonical spelling storage holds.
///
/// The SQL counterpart of [`canonical_obis`](crate::canonical_obis). Useful for
/// joining against a table that was not written through this crate: the merge
/// key includes `obis_code`, so `1-0:1.8.0` and `1-0:1.8.0*255` are one channel
/// and a literal comparison against the wrong spelling returns nothing.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ObisNormalise {
    signature: Signature,
}

impl Default for ObisNormalise {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObisNormalise {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "obis_normalise"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let (_, codes) = as_obis(&args)?;
        let out: StringArray = codes.iter().map(|c| c.map(|c| c.to_string())).collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// `obis_direction(code)` — `'IMPORT'`, `'EXPORT'`, or null.
///
/// The primitive `obis_is_import` and `obis_is_export` are derived from, and the
/// difference is not presentational: **both are false for a register that has no
/// direction at all** — Blindarbeit, a gas volume, a Zustandszahl — and false is
/// also what `obis_is_import` says about a feed-in register. So
/// `NOT obis_is_import(obis_code)` does not mean "export"; it sweeps the
/// undirected registers in with it, which is how a Bezug total ends up carrying
/// kvarh.
///
/// The three-way `GROUP BY` is the shape a bidirectional Zählpunkt wants:
///
/// ```sql
/// SELECT COALESCE(obis_direction(obis_code), 'UNDIRECTED') AS direction,
///        SUM(value)
/// FROM readings
/// GROUP BY 1
/// ```
///
/// The strings are `Direction::as_str`, which is also the domain's `serde` tag,
/// so a value from here and one out of a JSON payload compare literally.
///
/// Null for a code with no direction **and** for a null code: SQL has one null,
/// and a sentinel for the other would be a value a reader has to know about.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ObisDirection {
    signature: Signature,
}

impl Default for ObisDirection {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObisDirection {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "obis_direction"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let (_, codes) = as_obis(&args)?;
        let out: StringArray = codes
            .iter()
            .map(|c| c.and_then(ObisCode::direction).map(Direction::as_str))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// Read a `Utf8` argument and parse each row as an EIC.
///
/// Unlike [`as_obis`], a value that is not one is a **null** rather than an
/// error. An OBIS code is a core column this crate wrote and canonicalised, so
/// an unparseable one is a statement about a broken writer; an EIC arrives in a
/// *deployment* column, which a table may or may not declare with
/// `check = "EIC"`.
fn as_eic(args: &ScalarFunctionArgs) -> DfResult<Vec<Option<Eic>>> {
    let raw = as_strings(args, 0)?;
    Ok((0..raw.len())
        .map(|i| match raw.is_null(i) {
            true => None,
            false => raw.value(i).parse::<Eic>().ok(),
        })
        .collect())
}

/// `eic_regelzone(code)` — the Regelzone a Bilanzierungsgebiet lies in.
///
/// `'TENNET'`, `'AMPRION'`, `'FIFTY_HERTZ'`, `'TRANSNET_BW'`, or null. The
/// grouping key of a MaBiS Summenzeitreihe, read off the code that is already
/// stored rather than joined in from a mapping table that can go stale.
///
/// The rule is `metering`'s, from BDEW *Anwendungshilfe Energy Identification
/// Codes* v1.0 §2.2.2: a Bilanzierungsgebiet is a `Y` code under the German LIO
/// `11`, and position 4 identifies the Regelzone — `N` TenneT, `R` Amprion, `V`
/// 50Hertz, `W` TransnetBW. A `Y` code cannot in general be told from a
/// Bilanzkreis's, because an EIC function is registry metadata; this one can,
/// because the same section excludes those four letters at position 4 for every
/// other `Y` function.
///
/// ```sql
/// SELECT eic_regelzone(bilanzierungsgebiet) AS regelzone, SUM(value)
/// FROM readings
/// GROUP BY 1
/// ```
///
/// Null for a Bilanzkreis, for another issuing office's code, and for anything
/// that is not an EIC — the last is a null rather than an error because the
/// argument comes from a *deployment* column that may not be declared
/// `check = "EIC"`, and one row of free text must not take a report down.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EicRegelzone {
    signature: Signature,
}

impl Default for EicRegelzone {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for EicRegelzone {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "eic_regelzone"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let out: StringArray = as_eic(&args)?
            .iter()
            .map(|e| e.and_then(|e| e.regelzone()).map(Regelzone::as_str))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// `meter_local_day(ts)` — the Berlin calendar day an instant falls on.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LocalDay {
    signature: Signature,
}

impl Default for LocalDay {
    fn default() -> Self {
        Self {
            signature: timestamp_signature(),
        }
    }
}

impl ScalarUDFImpl for LocalDay {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_local_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let input = as_micros(&args)?;
        let mut out = Date32Array::builder(input.len());
        for i in 0..input.len() {
            if input.is_null(i) {
                out.append_null();
            } else {
                out.append_value(to_date32(calendar::local_day(from_micros(input.value(i))?)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `meter_gas_day(ts)` — the **Gastag** an instant falls on.
///
/// 06:00 to 06:00 `Europe/Berlin` (GaBi Gas, following Art. 3 Nr. 6 VO (EU)
/// 312/2014). The German gas market balances on this day and not on the calendar
/// one, so `meter_local_day` over a gas Lastgang books the 00:00–06:00 draw into
/// the previous Bilanzierungstag's neighbour — six hours a day, every day, with
/// totals that still look plausible.
///
/// ```sql
/// SELECT meter_gas_day("from") AS gastag, SUM(value)
/// FROM readings WHERE sparte = 'GAS'
/// GROUP BY 1
/// ```
///
/// Use [`BalancingDay`] instead when the statement spans commodities.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GasDay {
    signature: Signature,
}

impl Default for GasDay {
    fn default() -> Self {
        Self {
            signature: timestamp_signature(),
        }
    }
}

impl ScalarUDFImpl for GasDay {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_gas_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let input = as_micros(&args)?;
        let mut out = Date32Array::builder(input.len());
        for i in 0..input.len() {
            if input.is_null(i) {
                out.append_null();
            } else {
                out.append_value(to_date32(calendar::local_gas_day(from_micros(
                    input.value(i),
                )?)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `meter_balancing_day(ts, sparte)` — the day a reading is balanced on.
///
/// The Gastag for `'GAS'`, the Berlin calendar day for `'STROM'`, `'WAERME'` and
/// `'WASSER'`. This is the grouping key for a table that holds more than one
/// commodity, which is the ordinary case: the alternative is a `CASE` expression
/// repeated at every call site, and one of them eventually says `meter_local_day`
/// for gas.
///
/// ```sql
/// SELECT sparte, meter_balancing_day("from", sparte) AS day, SUM(value)
/// FROM readings
/// GROUP BY 1, 2
/// ```
///
/// The commodity is read **per row**, so a literal and a column both work and a
/// mixed scan is not grouped on whichever Sparte happened to sort first.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BalancingDay {
    signature: Signature,
}

impl Default for BalancingDay {
    fn default() -> Self {
        Self {
            signature: timestamp_signature_with(&[DataType::Utf8]),
        }
    }
}

impl ScalarUDFImpl for BalancingDay {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_balancing_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let input = as_micros(&args)?;
        let sparte = as_strings(&args, 1)?;

        let mut out = Date32Array::builder(input.len());
        for i in 0..input.len() {
            // A null commodity is not defaulted to the calendar day: that would
            // silently place gas rows on the wrong Bilanzierungstag, which is
            // the one failure this function exists to prevent. `sparte` is
            // non-nullable in the storage schema, so this is unreachable for a
            // stored row and null is the honest answer for anything else.
            if input.is_null(i) || sparte.is_null(i) {
                out.append_null();
                continue;
            }
            out.append_value(to_date32(balancing::balancing_day(
                from_micros(input.value(i))?,
                parse_sparte(sparte.value(i))?,
            )));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `meter_local_month(ts)` — the Berlin calendar month, as its first day.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LocalMonth {
    signature: Signature,
}

impl Default for LocalMonth {
    fn default() -> Self {
        Self {
            signature: timestamp_signature(),
        }
    }
}

impl ScalarUDFImpl for LocalMonth {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_local_month"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let input = as_micros(&args)?;
        let mut out = Date32Array::builder(input.len());
        for i in 0..input.len() {
            if input.is_null(i) {
                out.append_null();
            } else {
                out.append_value(to_date32(calendar::local_month(from_micros(
                    input.value(i),
                )?)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `meter_balancing_month(ts, sparte)` — the Bilanzierungsmonat, as its first day.
///
/// [`BalancingDay`] one period up, and the same argument. EDI@Energy *Allgemeine
/// Festlegungen* v6.1c, Kap. 3.1 spells the Bilanzierungsmonat Juni 2021 out as
/// 01.06 **06:00** to 01.07 **06:00** for Gas and 00:00 to 00:00 for Strom, so an
/// interval at 02:00 local on 1 March belongs to **February** for gas and to
/// March for everything else.
///
/// [`LocalMonth`] is the *calendar* month for every row, so grouping a gas
/// Lastgang by it books six hours into the neighbouring Bilanzierungsmonat twelve
/// times a year, with totals that still look plausible.
///
/// It is also the month an MSCONS correction version is scoped to, so this is
/// how a reader reproduces the month half of `version_scope` in SQL:
///
/// ```sql
/// SELECT sparte,
///        meter_balancing_month("from", sparte) AS bilanzierungsmonat,
///        SUM(value)
/// FROM readings
/// GROUP BY 1, 2
/// ```
///
/// The commodity is read **per row**, like every other function here.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BalancingMonth {
    signature: Signature,
}

impl Default for BalancingMonth {
    fn default() -> Self {
        Self {
            signature: timestamp_signature_with(&[DataType::Utf8]),
        }
    }
}

impl ScalarUDFImpl for BalancingMonth {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_balancing_month"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let input = as_micros(&args)?;
        let sparte = as_strings(&args, 1)?;

        let mut out = Date32Array::builder(input.len());
        for i in 0..input.len() {
            // Null rather than a defaulted calendar month, for
            // `meter_balancing_day`'s reason: defaulting would put gas rows in
            // the wrong settlement month, which is what this function exists to
            // prevent.
            if input.is_null(i) || sparte.is_null(i) {
                out.append_null();
                continue;
            }
            out.append_value(to_date32(balancing::balancing_month(
                from_micros(input.value(i))?,
                parse_sparte(sparte.value(i))?,
            )));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `meter_expected_intervals(day, resolution[, sparte])` — how many intervals a
/// balancing day contains.
///
/// 96 normally, **92** on the spring-forward day and **100** on the autumn one.
/// A completeness check that assumes 96 raises false alarms every spring and
/// masks a genuine four-interval gap every autumn.
///
/// # The optional third argument
///
/// Two arguments count a Berlin **calendar** day, which is right for
/// electricity, heat and water. Passing `sparte` counts the day that commodity
/// is actually balanced on — the **Gastag** for `'GAS'` — and must be paired
/// with [`BalancingDay`] rather than [`LocalDay`], since the count and the
/// bucketing have to describe the same day. The DST anomaly moves with it: the
/// long and short gas days are the ones named after the **Saturday**, because
/// the clocks change at 02:00/03:00 local, before the 06:00 boundary.
///
/// ```sql
/// SELECT meter_expected_intervals(
///          meter_balancing_day("from", sparte), resolution, sparte)
/// FROM readings
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ExpectedIntervals {
    signature: Signature,
}

impl Default for ExpectedIntervals {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Date32, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Date32, DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ExpectedIntervals {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "meter_expected_intervals"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::UInt32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let days = args.args[0].clone().into_array(args.number_rows)?;
        let days = days
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| DataFusionError::Execution("first argument must be a date".into()))?;

        // Resolution is often a literal, in which case it is parsed once. When
        // it is a column it must be read **per row**: completeness groups a
        // range by measuring point, and a utility carries 15-minute profiles
        // alongside hourly and daily ones. Applying the first row's resolution
        // to the rest would report a full day of gaps for every series whose
        // resolution differs from whichever happened to sort first.
        let literal = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Some(Some(parse_resolution(s)?)),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => Some(None),
            _ => None,
        };

        let per_row = match literal {
            Some(_) => None,
            None => {
                let array = args.args[1].clone().into_array(args.number_rows)?;
                let strings = crate::arrow::array::AsArray::as_string_opt::<i32>(&array)
                    .ok_or_else(|| {
                        DataFusionError::Execution("second argument must be a string".into())
                    })?
                    .clone();
                Some(strings)
            }
        };

        // Absent third argument: the Berlin calendar day, which is what every
        // commodity but gas balances on.
        let sparte = match args.args.len() {
            3 => Some(as_strings(&args, 2)?),
            _ => None,
        };

        let mut out = UInt32Array::builder(days.len());
        for i in 0..days.len() {
            let resolution = match (&literal, &per_row) {
                (Some(value), _) => *value,
                (None, Some(strings)) if !strings.is_null(i) => {
                    Some(parse_resolution(strings.value(i))?)
                }
                _ => None,
            };
            let commodity = match &sparte {
                None => Some(Sparte::Strom),
                Some(codes) if codes.is_null(i) => None,
                Some(codes) => Some(parse_sparte(codes.value(i))?),
            };

            match (days.is_null(i), resolution, commodity) {
                (false, Some(res), Some(sparte)) => {
                    let date = crate::encode::schema::date_of(days.value(i))
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                    match balancing::expected_intervals_in_balancing_day(date, res, sparte) {
                        Some(n) => out.append_value(n),
                        // Calendar resolutions have no fixed interval count
                        // within a day; null is the honest answer.
                        None => out.append_null(),
                    }
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Parse an ISO 8601 resolution, the form storage uses.
fn parse_resolution(s: &str) -> DfResult<IntervalResolution> {
    s.parse()
        .map_err(|e| DataFusionError::Execution(format!("bad resolution {s:?}: {e}")))
}

/// Every calendar function, ready to register.
pub fn all() -> Vec<ScalarUDF> {
    let mut udfs = vec![
        ScalarUDF::from(LocalDay::default()),
        ScalarUDF::from(GasDay::default()),
        ScalarUDF::from(BalancingDay::default()),
        ScalarUDF::from(LocalMonth::default()),
        ScalarUDF::from(BalancingMonth::default()),
        ScalarUDF::from(ExpectedIntervals::default()),
        ScalarUDF::from(ObisTariffRegister::default()),
        ScalarUDF::from(ObisNormalise::default()),
        ScalarUDF::from(ObisDirection::default()),
        ScalarUDF::from(EicRegelzone::default()),
    ];
    // One-to-one with `metering::obis`'s own predicates. Listed rather than
    // generated so that adding one upstream is a deliberate act here, and so the
    // SQL name and the method it wraps sit on the same line.
    for (name, test) in [
        // `is_import` and `is_export` take `self` by value upstream — they are
        // `const fn` derived from `direction()`, which a `&self` receiver would
        // prevent. The closure is the coercion and nothing else; every other
        // predicate is a plain method reference.
        (
            "obis_is_import",
            (|c: &ObisCode| c.is_import()) as fn(&ObisCode) -> bool,
        ),
        ("obis_is_export", |c: &ObisCode| c.is_export()),
        ("obis_is_reactive", ObisCode::is_reactive),
        ("obis_is_lastgang", ObisCode::is_lastgang),
        ("obis_is_zaehlerstand", ObisCode::is_zaehlerstand),
        ("obis_is_vorschub", ObisCode::is_vorschub),
        ("obis_is_maximum", ObisCode::is_maximum),
        ("obis_is_fehlerregister", ObisCode::is_fehlerregister),
        ("obis_is_total_register", ObisCode::is_total_register),
    ] {
        udfs.push(ScalarUDF::from(ObisPredicate::new(name, test)));
    }
    udfs
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use time::macros::{date, datetime};

    /// A session with the calendar functions registered and one row of input.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        for udf in all() {
            ctx.register_udf(udf);
        }
        ctx
    }

    async fn one_date(sql: &str) -> Option<Date> {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date32 result");
        (!array.is_null(0)).then(|| crate::encode::schema::date_of(array.value(0)).unwrap())
    }

    async fn one_u32(sql: &str) -> Option<u32> {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("uint32 result");
        (!array.is_null(0)).then(|| array.value(0))
    }

    #[tokio::test]
    async fn local_day_uses_the_berlin_boundary_not_the_utc_one() {
        // 23:00 UTC on the 20th is already the 21st in Berlin. This is the bug
        // `date_trunc('day', ...)` would introduce, silently, year-round.
        let got = one_date("SELECT meter_local_day(TIMESTAMP '2026-07-20T23:00:00Z')").await;
        assert_eq!(got, Some(date!(2026 - 07 - 21)));
    }

    #[tokio::test]
    async fn local_day_matches_metering_directly() {
        // The wrapper must not change the answer.
        for instant in [
            datetime!(2026-07-20 12:00 UTC),
            datetime!(2026-01-20 23:30 UTC),
            datetime!(2026-03-29 01:30 UTC),
            datetime!(2026-10-25 01:30 UTC),
        ] {
            let sql = format!(
                "SELECT meter_local_day(TIMESTAMP '{}')",
                instant
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap()
            );
            assert_eq!(
                one_date(&sql).await,
                Some(calendar::local_day(instant)),
                "wrapper disagreed with metering for {instant}"
            );
        }
    }

    async fn one_string(sql: &str) -> Option<String> {
        use crate::arrow::array::AsArray;
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let column = batches[0].column(0).as_string::<i32>();
        (!column.is_null(0)).then(|| column.value(0).to_string())
    }

    async fn one_bool(sql: &str) -> Option<bool> {
        use crate::arrow::array::AsArray;
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let column = batches[0].column(0).as_boolean();
        (!column.is_null(0)).then(|| column.value(0))
    }

    #[tokio::test]
    async fn obis_predicates_answer_what_metering_answers() {
        // Wrappers, so what is asserted is that the wrapper preserves the
        // upstream answer — not that the answer is right, which is `metering`'s
        // suite's job.
        for (sql, want) in [
            ("obis_is_import('1-0:1.8.0')", true),
            ("obis_is_import('1-0:2.8.0')", false),
            ("obis_is_export('1-0:2.29.0')", true),
            // The Lastgang is the commonest code in MSCONS interval data, and
            // requiring D = 8 would report it as neither direction.
            ("obis_is_import('1-0:1.29.0')", true),
            ("obis_is_lastgang('1-0:1.29.0')", true),
            ("obis_is_zaehlerstand('1-0:1.8.0')", true),
            ("obis_is_vorschub('1-0:1.9.0')", true),
            // A kW peak, not a kWh quantity.
            ("obis_is_maximum('1-0:1.6.0')", true),
            ("obis_is_maximum('1-0:1.29.0')", false),
            // kvarh — the quadrant registers count too, not only C = 3/4.
            ("obis_is_reactive('1-0:5.8.0')", true),
            ("obis_is_reactive('1-0:1.8.0')", false),
            ("obis_is_fehlerregister('1-0:1.8.63')", true),
            ("obis_is_total_register('1-0:1.8.0')", true),
            ("obis_is_total_register('1-0:1.8.1')", false),
        ] {
            assert_eq!(
                one_bool(&format!("SELECT {sql}")).await,
                Some(want),
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn direction_is_medium_aware_without_being_told_the_commodity() {
        // Value group A carries the medium and value group C is a Messgröße for
        // gas rather than a direction, so `is_import` is false for a gas code
        // with no `sparte` argument. Taking one would be a second source for a
        // fact the code already states.
        assert_eq!(
            one_bool("SELECT obis_is_import('7-1:99.33.0')").await,
            Some(false)
        );
        assert_eq!(
            one_bool("SELECT obis_is_export('7-1:99.33.0')").await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn a_fault_counter_is_not_tariff_sixty_three() {
        use crate::arrow::array::AsArray;
        let batches = ctx()
            .sql(
                "SELECT obis_tariff_register('1-0:1.8.1') AS ht, \
                        obis_tariff_register('1-0:1.8.0') AS total, \
                        obis_tariff_register('1-0:1.8.63') AS fault",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let b = &batches[0];
        let value = |name: &str| {
            let c = b
                .column_by_name(name)
                .unwrap()
                .as_primitive::<crate::arrow::datatypes::UInt8Type>();
            (!c.is_null(0)).then(|| c.value(0))
        };
        assert_eq!(value("ht"), Some(1));
        assert_eq!(value("total"), None, "the total register is not a tariff");
        assert_eq!(
            value("fault"),
            None,
            "E = 63 is a fault counter, and reporting it as tariff 63 invites \
             billing it as consumption"
        );
    }

    #[tokio::test]
    async fn obis_normalise_matches_the_stored_spelling() {
        use crate::arrow::array::AsArray;
        let batches = ctx()
            .sql("SELECT obis_normalise('1-0:1.8.0*255') AS c")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches[0].column(0).as_string::<i32>().value(0),
            "1-0:1.8.0"
        );
    }

    #[tokio::test]
    async fn a_null_code_is_null_and_a_bad_one_is_an_error() {
        assert_eq!(
            one_bool("SELECT obis_is_import(CAST(NULL AS VARCHAR))").await,
            None
        );
        assert!(
            ctx()
                .sql("SELECT obis_is_import('not-an-obis-code')")
                .await
                .unwrap()
                .collect()
                .await
                .is_err(),
            "storage holds canonical codes, so an unparseable one is a statement \
             about the row rather than a null"
        );
    }

    #[tokio::test]
    async fn local_month_normalises_to_the_first() {
        assert_eq!(
            one_date("SELECT meter_local_month(TIMESTAMP '2026-07-20T12:00:00Z')").await,
            Some(date!(2026 - 07 - 01))
        );
        // Late-evening UTC on the last of the month is already the next month.
        assert_eq!(
            one_date("SELECT meter_local_month(TIMESTAMP '2026-07-31T23:00:00Z')").await,
            Some(date!(2026 - 08 - 01))
        );
    }

    #[tokio::test]
    async fn expected_intervals_knows_the_dst_days() {
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-07-20', 'PT15M')").await,
            Some(96)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-03-29', 'PT15M')").await,
            Some(92),
            "spring forward"
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-25', 'PT15M')").await,
            Some(100),
            "autumn back"
        );
    }

    #[tokio::test]
    async fn expected_intervals_handles_other_resolutions() {
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-25', 'PT1H')").await,
            Some(25)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-07-20', 'PT30M')").await,
            Some(48)
        );
    }

    #[tokio::test]
    async fn a_calendar_resolution_has_no_interval_count_within_a_day() {
        // A month is not a fixed number of intervals in a day; null, not a lie.
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-07-20', 'P1M')").await,
            None
        );
    }

    #[tokio::test]
    async fn a_resolution_column_is_read_per_row() {
        // A utility carries 15-minute profiles alongside hourly and daily ones,
        // so a completeness report groups rows of differing resolution. Reusing
        // the first row's value would report a day of gaps for every other one.
        let batches = ctx()
            .sql(
                "SELECT meter_expected_intervals(d, r) AS n FROM (
                   SELECT DATE '2026-07-20' AS d, 'PT15M' AS r
                   UNION ALL SELECT DATE '2026-07-20', 'PT1H'
                   UNION ALL SELECT DATE '2026-07-20', 'PT30M'
                 ) ORDER BY n",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let got: Vec<u32> = batches
            .iter()
            .flat_map(|b| {
                let a = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("uint32")
                    .clone();
                (0..a.len()).map(move |i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, vec![24, 48, 96]);
    }

    #[tokio::test]
    async fn a_null_resolution_in_a_column_yields_null_for_that_row_only() {
        let batches = ctx()
            .sql(
                "SELECT meter_expected_intervals(d, r) AS n FROM (
                   SELECT DATE '2026-07-20' AS d, CAST(NULL AS VARCHAR) AS r
                   UNION ALL SELECT DATE '2026-07-20', 'PT15M'
                 )",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut values = Vec::new();
        for b in &batches {
            let a = b.column(0).as_any().downcast_ref::<UInt32Array>().unwrap();
            for i in 0..a.len() {
                values.push((!a.is_null(i)).then(|| a.value(i)));
            }
        }
        values.sort();
        assert_eq!(values, vec![None, Some(96)]);
    }

    #[tokio::test]
    async fn nulls_propagate() {
        assert_eq!(
            one_date("SELECT meter_local_day(CAST(NULL AS TIMESTAMP))").await,
            None
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(CAST(NULL AS DATE), 'PT15M')").await,
            None
        );
    }

    #[tokio::test]
    async fn a_bad_resolution_is_an_error_not_a_wrong_number() {
        let result = ctx()
            .sql("SELECT meter_expected_intervals(DATE '2026-07-20', 'fortnightly')")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn local_day_groups_a_series_correctly() {
        // The whole point: two instants either side of the Berlin midnight must
        // land in different groups even though they share a UTC day.
        let batches = ctx()
            .sql(
                "SELECT meter_local_day(t) AS d, COUNT(*) AS n FROM (
                   SELECT TIMESTAMP '2026-07-20T21:00:00Z' AS t
                   UNION ALL SELECT TIMESTAMP '2026-07-20T23:00:00Z'
                 ) GROUP BY 1 ORDER BY 1",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "same UTC day, different Berlin days");
    }

    // ── the Gastag ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn gas_day_starts_at_0600_local_not_at_midnight() {
        // 03:00 UTC in July is 05:00 local — still the previous Gastag. One hour
        // later it is 06:00 local and the new one has begun.
        assert_eq!(
            one_date("SELECT meter_gas_day(TIMESTAMP '2026-07-15T03:00:00Z')").await,
            Some(date!(2026 - 07 - 14))
        );
        assert_eq!(
            one_date("SELECT meter_gas_day(TIMESTAMP '2026-07-15T04:00:00Z')").await,
            Some(date!(2026 - 07 - 15))
        );
        // Winter: 06:00 CET is 05:00 UTC, so the boundary moves with the offset.
        assert_eq!(
            one_date("SELECT meter_gas_day(TIMESTAMP '2026-01-15T04:59:00Z')").await,
            Some(date!(2026 - 01 - 14))
        );
        assert_eq!(
            one_date("SELECT meter_gas_day(TIMESTAMP '2026-01-15T05:00:00Z')").await,
            Some(date!(2026 - 01 - 15))
        );
    }

    #[tokio::test]
    async fn gas_day_matches_metering_directly() {
        // The wrapper must not change the answer — same contract as the
        // calendar-day wrapper above.
        for instant in [
            datetime!(2026-07-15 03:00 UTC),
            datetime!(2026-01-15 05:00 UTC),
            datetime!(2026-03-29 01:30 UTC),
            datetime!(2026-10-25 01:30 UTC),
            datetime!(2026-10-25 05:30 UTC),
        ] {
            let sql = format!(
                "SELECT meter_gas_day(TIMESTAMP '{}')",
                instant
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap()
            );
            assert_eq!(
                one_date(&sql).await,
                Some(calendar::local_gas_day(instant)),
                "wrapper disagreed with metering for {instant}"
            );
        }
    }

    #[tokio::test]
    async fn balancing_day_follows_the_commodity() {
        // The same instant, two commodities, two days. 22:15 UTC on 14 July is
        // 00:15 local on the 15th — a new calendar day, but still the Gastag of
        // the 14th.
        let at = "TIMESTAMP '2026-07-14T22:15:00Z'";
        assert_eq!(
            one_date(&format!("SELECT meter_balancing_day({at}, 'STROM')")).await,
            Some(date!(2026 - 07 - 15))
        );
        assert_eq!(
            one_date(&format!("SELECT meter_balancing_day({at}, 'GAS')")).await,
            Some(date!(2026 - 07 - 14))
        );
        // Heat and water are calendar-day commodities, not "everything that is
        // not electricity".
        for sparte in ["WAERME", "WASSER"] {
            assert_eq!(
                one_date(&format!("SELECT meter_balancing_day({at}, '{sparte}')")).await,
                Some(date!(2026 - 07 - 15)),
                "{sparte}"
            );
        }
    }

    #[tokio::test]
    async fn balancing_day_reads_the_commodity_per_row() {
        // A mixed scan must not be grouped on whichever Sparte sorted first.
        let batches = ctx()
            .sql(
                "SELECT sparte, meter_balancing_day(t, sparte) AS d FROM (
                   SELECT TIMESTAMP '2026-07-14T22:15:00Z' AS t, 'GAS' AS sparte
                   UNION ALL SELECT TIMESTAMP '2026-07-14T22:15:00Z', 'STROM'
                 ) ORDER BY sparte",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let epoch = Date::from_ordinal_date(1970, 1).unwrap();
        let days = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(
            epoch + time::Duration::days(i64::from(days.value(0))),
            date!(2026 - 07 - 14),
            "GAS sorts first"
        );
        assert_eq!(
            epoch + time::Duration::days(i64::from(days.value(1))),
            date!(2026 - 07 - 15),
            "STROM must not inherit the gas day"
        );
    }

    #[tokio::test]
    async fn balancing_day_rejects_an_unknown_commodity() {
        // Silently defaulting would place the rows on a day nobody asked for.
        let err = ctx()
            .sql("SELECT meter_balancing_day(TIMESTAMP '2026-07-15T03:00:00Z', 'OEL')")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(err.is_err(), "an unknown Sparte must not be defaulted");
    }

    #[tokio::test]
    async fn expected_intervals_puts_the_gas_dst_day_on_the_saturday() {
        // Autumn 2026: the clocks go back at 03:00 local on Sunday the 25th,
        // inside the gas day that began Saturday 06:00. So the 100-interval gas
        // day is the 24th while the 100-interval *calendar* day is the 25th —
        // and a check using the wrong one is wrong in both directions at once.
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-24', 'PT15M', 'GAS')").await,
            Some(100)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-25', 'PT15M', 'GAS')").await,
            Some(96)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-24', 'PT15M', 'STROM')").await,
            Some(96)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-25', 'PT15M', 'STROM')").await,
            Some(100)
        );
    }

    #[tokio::test]
    async fn expected_intervals_without_a_commodity_is_the_calendar_day() {
        // The two-argument form is unchanged, so existing statements keep their
        // meaning rather than silently switching to a gas day.
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-10-25', 'PT15M')").await,
            Some(100)
        );
        assert_eq!(
            one_u32("SELECT meter_expected_intervals(DATE '2026-03-29', 'PT15M')").await,
            Some(92)
        );
    }

    #[tokio::test]
    async fn expected_intervals_is_null_for_an_unknown_commodity_column() {
        // A null `sparte` yields null rather than an assumed calendar day.
        assert_eq!(
            one_u32(
                "SELECT meter_expected_intervals(DATE '2026-10-25', 'PT15M', \
                 CAST(NULL AS VARCHAR))"
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn balancing_month_follows_the_commodity() {
        // 01:00 UTC on 1 March is 02:00 local: already March by the calendar,
        // but still the February Gastag — and so the February
        // Bilanzierungsmonat. `meter_local_month` cannot see the difference,
        // which is the whole reason this function exists.
        let at = "TIMESTAMP '2026-03-01T01:00:00Z'";
        assert_eq!(
            one_date(&format!("SELECT meter_balancing_month({at}, 'STROM')")).await,
            Some(date!(2026 - 03 - 01))
        );
        assert_eq!(
            one_date(&format!("SELECT meter_balancing_month({at}, 'GAS')")).await,
            Some(date!(2026 - 02 - 01))
        );
        assert_eq!(
            one_date(&format!("SELECT meter_local_month({at})")).await,
            Some(date!(2026 - 03 - 01)),
            "the calendar month is the electricity answer for every commodity"
        );
        for sparte in ["WAERME", "WASSER"] {
            assert_eq!(
                one_date(&format!("SELECT meter_balancing_month({at}, '{sparte}')")).await,
                Some(date!(2026 - 03 - 01)),
                "{sparte}"
            );
        }
    }

    #[tokio::test]
    async fn balancing_month_reads_the_commodity_per_row() {
        // Same guarantee as `meter_balancing_day`: a mixed table is grouped
        // correctly by one statement, not by whichever Sparte sorted first.
        let batches = ctx()
            .sql(
                "SELECT sparte, meter_balancing_month(t, sparte) AS m FROM (
                   SELECT TIMESTAMP '2026-03-01T01:00:00Z' AS t, 'GAS' AS sparte
                   UNION ALL SELECT TIMESTAMP '2026-03-01T01:00:00Z', 'STROM'
                 ) ORDER BY sparte",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let epoch = Date::from_ordinal_date(1970, 1).unwrap();
        let months = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(
            epoch + time::Duration::days(i64::from(months.value(0))),
            date!(2026 - 02 - 01),
            "GAS sorts first"
        );
        assert_eq!(
            epoch + time::Duration::days(i64::from(months.value(1))),
            date!(2026 - 03 - 01),
            "STROM must not inherit the gas month"
        );
    }

    #[tokio::test]
    async fn balancing_month_refuses_an_unknown_commodity_and_propagates_nulls() {
        assert!(
            ctx()
                .sql("SELECT meter_balancing_month(TIMESTAMP '2026-03-01T01:00:00Z', 'OEL')")
                .await
                .unwrap()
                .collect()
                .await
                .is_err(),
            "an unknown Sparte must not be defaulted"
        );
        assert_eq!(
            one_date("SELECT meter_balancing_month(CAST(NULL AS TIMESTAMP), 'GAS')").await,
            None
        );
        assert_eq!(
            one_date(
                "SELECT meter_balancing_month(TIMESTAMP '2026-03-01T01:00:00Z', \
                 CAST(NULL AS VARCHAR))"
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn obis_direction_separates_undirected_from_export() {
        use crate::arrow::array::AsArray;
        let batches = ctx()
            .sql(
                "SELECT obis_direction('1-0:1.8.0') AS bezug, \
                        obis_direction('1-0:2.29.0') AS einspeisung, \
                        obis_direction('1-0:3.8.0') AS blind, \
                        obis_direction(CAST(NULL AS VARCHAR)) AS nothing",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let b = &batches[0];
        let value = |name: &str| {
            let c = b.column_by_name(name).unwrap().as_string::<i32>();
            (!c.is_null(0)).then(|| c.value(0).to_string())
        };
        assert_eq!(value("bezug").as_deref(), Some("IMPORT"));
        assert_eq!(value("einspeisung").as_deref(), Some("EXPORT"));
        // The finding the two booleans cannot make: Blindarbeit is neither, and
        // `NOT obis_is_import(...)` would have swept it in with the feed-in.
        assert_eq!(value("blind"), None);
        assert_eq!(value("nothing"), None);
    }

    #[tokio::test]
    async fn obis_direction_agrees_with_the_predicates_it_is_the_primitive_for() {
        // Upstream derives `is_import`/`is_export` from `direction()`; storage
        // exposes all three, so they must not be able to disagree in SQL either.
        for code in [
            "1-0:1.8.0",
            "1-0:2.8.0",
            "1-0:1.29.0",
            "1-0:2.29.0",
            "1-0:3.8.0",
            "7-1:99.33.0",
        ] {
            let direction = {
                use crate::arrow::array::AsArray;
                let batches = ctx()
                    .sql(&format!("SELECT obis_direction('{code}')"))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                let c = batches[0].column(0).as_string::<i32>();
                (!c.is_null(0)).then(|| c.value(0).to_string())
            };
            assert_eq!(
                direction.as_deref() == Some("IMPORT"),
                one_bool(&format!("SELECT obis_is_import('{code}')"))
                    .await
                    .unwrap(),
                "{code}"
            );
            assert_eq!(
                direction.as_deref() == Some("EXPORT"),
                one_bool(&format!("SELECT obis_is_export('{code}')"))
                    .await
                    .unwrap(),
                "{code}"
            );
        }
    }

    #[tokio::test]
    async fn eic_regelzone_reads_position_four_of_a_bilanzierungsgebiet() {
        for (code, want) in [
            // `11Y` under the German LIO, position 4 the Regelzone letter.
            ("11YN000000000016", Some("TENNET")),
            ("11YV00000000001D", Some("FIFTY_HERTZ")),
            // An `X` code is a Bilanzkreis, not a grid area — nothing to read.
            ("11XBK0000000001A", None),
            // Another issuing office, and the German rule does not apply to it.
            ("10X168Y4E6H0041Z", None),
        ] {
            assert_eq!(
                one_string(&format!("SELECT eic_regelzone('{code}')"))
                    .await
                    .as_deref(),
                want,
                "{code}"
            );
        }
    }

    #[tokio::test]
    async fn a_column_that_is_not_an_eic_is_null_rather_than_a_failed_report() {
        // Unlike `obis_code`, the argument comes from a *deployment* column that
        // may or may not be declared `check = "EIC"`. Failing the statement on
        // one row of free text would make the function unusable exactly where it
        // is most wanted.
        use crate::arrow::array::AsArray;
        let batches = ctx()
            .sql(
                "SELECT eic_regelzone(c) AS z FROM (
                   SELECT '11YN000000000016' AS c
                   UNION ALL SELECT 'not an eic'
                   UNION ALL SELECT CAST(NULL AS VARCHAR)
                 ) ORDER BY c",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut seen = Vec::new();
        for b in &batches {
            let c = b.column(0).as_string::<i32>();
            for i in 0..c.len() {
                seen.push((!c.is_null(i)).then(|| c.value(i).to_string()));
            }
        }
        seen.sort();
        assert_eq!(seen, vec![None, None, Some("TENNET".to_string())]);
    }

    #[tokio::test]
    async fn a_transposed_eic_reads_as_no_regelzone_rather_than_the_wrong_one() {
        // The check character is what makes that possible: `11YN000000000016`
        // with any other check character is not an EIC, so it cannot be parsed
        // into a plausible-looking TenneT grid area.
        assert_eq!(
            one_string("SELECT eic_regelzone('11YN000000000017')").await,
            None
        );
    }

    #[tokio::test]
    async fn gas_day_is_null_for_a_null_instant() {
        assert_eq!(
            one_date("SELECT meter_gas_day(CAST(NULL AS TIMESTAMP))").await,
            None
        );
        assert_eq!(
            one_date("SELECT meter_balancing_day(CAST(NULL AS TIMESTAMP), 'GAS')").await,
            None
        );
        assert_eq!(
            one_date(
                "SELECT meter_balancing_day(TIMESTAMP '2026-07-15T03:00:00Z', \
                 CAST(NULL AS VARCHAR))"
            )
            .await,
            None
        );
    }
}
