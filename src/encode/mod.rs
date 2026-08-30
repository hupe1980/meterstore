//! Encoding `metering` types into Arrow, and back.
//!
//! This module is the whole of MeterStore's contribution to the data itself:
//! `metering` says what a measurement *is*, and this says how it is *stored*.
//! Everything here must round-trip exactly — a lost decimal
//! place or a flattened quality flag is a wrong bill.

pub mod schema;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::arrow::array::{
    Array, ArrayRef, Decimal128Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use crate::arrow::datatypes::Field;
use datafusion::common::ScalarValue;
use metering::ids::{MaloId, MeloId};
use metering::interval::{MeasurementUnit, MeterInterval, Sparte};
use metering::measurement_series::{MeasurementSeries, MeasurementSource, ProvenanceEntry};
use metering::obis::ObisCode;
use rust_decimal::Decimal;
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::version::{ScopedVersion, Version, VersionScope};

use schema::{VALUE_PRECISION, VALUE_SCALE, VERSION_PRECISION, VERSION_SCALE, col};

/// A series plus the storage metadata that tiering adds.
#[derive(Debug, Clone)]
pub struct StoredSeries {
    /// The domain payload, owned by `metering`.
    pub series: MeasurementSeries,
    /// The commodity this series measures.
    ///
    /// [`MeasurementSeries`] does not carry it — a `metering` series is a channel
    /// of numbers, and the commodity is a property of the measuring point. Storage
    /// needs it anyway: it is what makes [`unit`](Self::unit) checkable, and what
    /// lets a query separate the m³ from the kWh.
    pub sparte: Sparte,
    /// The dimension of every value in the series.
    ///
    /// Must be one of the two units the Sparte admits — its
    /// [`measured_unit`](Sparte::measured_unit) or its
    /// [`billing_unit`](Sparte::billing_unit), which coincide for every Sparte
    /// but gas. Defaults to the billing unit; anything else is rejected when the
    /// series is encoded.
    pub unit: MeasurementUnit,
    /// MSCONS correction version and the scope it is comparable within.
    pub version: ScopedVersion,
    /// Transaction time — when this delivery was recorded.
    pub recorded_at: OffsetDateTime,
    /// Values for the deployment's declared extra columns, keyed by name.
    ///
    /// Non-nullable columns must be present — an identity column that is missing
    /// would silently merge readings that are not the same reading. A nullable
    /// column may be omitted and is stored as null.
    pub extra: BTreeMap<String, ScalarValue>,
}

impl StoredSeries {
    /// A series of the default commodity (electricity, kWh) with no
    /// deployment-specific columns.
    ///
    /// Defaulting to Strom is safe rather than convenient: gas and water are
    /// reached through [`of`](Self::of), and a caller who sets the Sparte but
    /// forgets the unit is caught at write time rather than storing a volume
    /// labelled as energy.
    pub fn new(
        series: MeasurementSeries,
        version: ScopedVersion,
        recorded_at: OffsetDateTime,
    ) -> Self {
        Self::of(Sparte::Strom, series, version, recorded_at)
    }

    /// A series of a named commodity, in that commodity's **billing** unit.
    ///
    /// Billing rather than measured, because that is the unit a stored series
    /// almost always holds: gas arrives as m³ and is converted at ingest, which
    /// is `metering`'s job and has happened by the time a series reaches storage.
    /// A deployment archiving unconverted Betriebsvolumen says so with
    /// [`in_unit`](Self::in_unit).
    pub fn of(
        sparte: Sparte,
        series: MeasurementSeries,
        version: ScopedVersion,
        recorded_at: OffsetDateTime,
    ) -> Self {
        Self {
            series,
            sparte,
            unit: sparte.billing_unit(),
            version,
            recorded_at,
            extra: BTreeMap::new(),
        }
    }

    /// Override the unit — for gas held as unconverted m³.
    pub fn in_unit(mut self, unit: MeasurementUnit) -> Self {
        self.unit = unit;
        self
    }

    /// Set a deployment-specific column value.
    pub fn with_extra(mut self, name: impl Into<String>, value: ScalarValue) -> Self {
        self.extra.insert(name.into(), value);
        self
    }
}

/// A **Zählerstandsgang** plus the storage metadata that tiering adds.
///
/// The point-series counterpart of [`StoredSeries`]. Where that carries
/// `metering`'s [`MeterInterval`]s — energy
/// over `[from, to)` — this carries its
/// [`MeterReading`]s: a cumulative register value at an instant.
///
/// [`MeterInterval`]: metering::interval::MeterInterval
/// [`MeterReading`]: metering::reading::MeterReading
///
/// A separate type rather than a flag, because `value` means two different
/// things and no aggregate can tell them apart — summing Zählerstände gives a
/// number with no meaning that looks exactly like a consumption total. Different
/// Rust types keep a caller from passing one where the other belongs;
/// [`TimeModel`](crate::config::TimeModel) keeps them out of one Iceberg table.
///
/// Everything else is identical — the same identity, attribute and subject
/// columns, version scoping, watermark and partitioning — because all of that
/// reads the *start* timestamp, and a reading has one.
#[derive(Debug, Clone)]
pub struct StoredReadings {
    /// The measuring point.
    pub malo_id: MaloId,
    /// The measuring location, if known.
    pub melo_id: Option<MeloId>,
    /// The register these readings come from.
    ///
    /// Required, unlike a series' — a Zählerstandsgang *is* one register's
    /// history, and a reading with no register named is not attributable to one.
    pub obis_code: ObisCode,
    /// The register values, ascending.
    pub readings: Vec<metering::reading::MeterReading>,
    /// How often the register is read.
    ///
    /// `metering::reading::detect_reading_cadence` derives it from the
    /// timestamps. Stored in the same column an interval series' resolution is,
    /// and read the same way by completeness: "how many values a day should
    /// there be" is one question for both.
    pub cadence: Option<metering::resolution::IntervalResolution>,
    /// Where the delivery came from.
    pub source: MeasurementSource,
    /// The delivery's audit trail.
    pub provenance: Vec<ProvenanceEntry>,
    /// The commodity this register meters.
    pub sparte: Sparte,
    /// The dimension of every value.
    pub unit: MeasurementUnit,
    /// MSCONS correction version and the scope it is comparable within.
    pub version: ScopedVersion,
    /// Transaction time — when this delivery was recorded.
    pub recorded_at: OffsetDateTime,
    /// Values for the deployment's declared extra columns, keyed by name.
    pub extra: BTreeMap<String, ScalarValue>,
}

impl StoredReadings {
    /// A Zählerstandsgang for one register of one measuring point.
    ///
    /// Defaults to the commodity's **billing** unit, for the same reason
    /// [`StoredSeries::of`] does.
    pub fn new(
        malo_id: MaloId,
        obis_code: ObisCode,
        sparte: Sparte,
        readings: Vec<metering::reading::MeterReading>,
        source: MeasurementSource,
        version: ScopedVersion,
        recorded_at: OffsetDateTime,
    ) -> Self {
        Self {
            malo_id,
            melo_id: None,
            obis_code,
            readings,
            cadence: None,
            source,
            provenance: Vec::new(),
            sparte,
            unit: sparte.billing_unit(),
            version,
            recorded_at,
            extra: BTreeMap::new(),
        }
    }

    /// Name the measuring location.
    #[must_use]
    pub fn with_melo_id(mut self, melo_id: MeloId) -> Self {
        self.melo_id = Some(melo_id);
        self
    }

    /// Override the unit — for gas held as unconverted m³.
    #[must_use]
    pub fn in_unit(mut self, unit: MeasurementUnit) -> Self {
        self.unit = unit;
        self
    }

    /// Declare how often the register is read.
    ///
    /// Pair with `metering::reading::detect_reading_cadence`, which derives it
    /// from the timestamps rather than assuming a quarter-hour.
    #[must_use]
    pub fn at_cadence(mut self, cadence: metering::resolution::IntervalResolution) -> Self {
        self.cadence = Some(cadence);
        self
    }

    /// Set a deployment-specific column value.
    #[must_use]
    pub fn with_extra(mut self, name: impl Into<String>, value: ScalarValue) -> Self {
        self.extra.insert(name.into(), value);
        self
    }
}

/// Reject a unit the commodity cannot be expressed in.
///
/// A Sparte admits exactly two: what its register advances in
/// ([`Sparte::measured_unit`]) and what it settles in
/// ([`Sparte::billing_unit`]). For Strom, Wärme and Wasser those coincide, so
/// there is one; only gas has both, being metered in m³ and billed in kWh.
///
/// This is the check that makes [`col::UNIT`] worth reading. Without it the
/// column records whatever the caller believed, and a water series labelled `KWH`
/// would sum straight into an electricity total with nothing to notice it. The
/// rule itself lives in `metering` (P5) — this only enforces it.
fn check_unit(sparte: Sparte, unit: MeasurementUnit, malo_id: &MaloId) -> Result<()> {
    if unit == sparte.measured_unit() || unit == sparte.billing_unit() {
        return Ok(());
    }
    Err(Error::encode(
        col::UNIT,
        format!(
            "{malo_id}: {sparte} is measured in {} and billed in {}, so it cannot be \
             stored as {unit}",
            sparte.measured_unit(),
            sparte.billing_unit(),
        ),
    ))
}

/// Parse a stored `malo_id` back into the domain type.
///
/// The column is `Utf8` because Parquet, Arrow and PostgreSQL have no
/// eleven-digit-with-check-digit type — but the *value* is a [`MaloId`], and
/// reading it back without checking would let a row written by something other
/// than this crate (a bulk load, a hand-run `INSERT`, a corrupted file) enter the
/// typed path as a well-formed identifier it is not. The check digit exists
/// precisely to catch a transposition, and a decoder that skips it throws that
/// protection away at the last moment it could still be used.
///
/// This is a decode error rather than a filtered row: silently dropping readings
/// whose key looks wrong would understate a settlement, which is the failure
/// direction this crate refuses everywhere else.
fn decode_malo(raw: &str) -> Result<MaloId> {
    raw.parse::<MaloId>()
        .map_err(|e| Error::decode(col::MALO_ID, format!("{raw:?}: {e}")))
}

/// Parse a stored `melo_id` back into the domain type.
///
/// Structural only — a Zählpunktbezeichnung carries no check digit — but the
/// 33-character shape still rejects a truncated or padded value.
fn decode_melo(raw: &str) -> Result<MeloId> {
    raw.parse::<MeloId>()
        .map_err(|e| Error::decode(col::MELO_ID, format!("{raw:?}: {e}")))
}

/// Parse a coded column, refusing a spelling storage does not write.
///
/// `metering`'s [`FromStr`] is deliberately lenient: it trims, ignores case, and
/// accepts input aliases — `WÄRME` for `WAERME`, `kwh` for `KWH`. That is right
/// at an ingest boundary and wrong here, because **storage holds the canonical
/// form and only the canonical form**.
///
/// These columns are `GROUP BY` keys: completeness groups by three of them, so
/// two spellings of one commodity are two rows in a report an operator is meant
/// to be able to trust. The hot tier's `CHECK … IN (…)` is rendered from `CODES`
/// and refuses them already; Iceberg has no constraints, so this is the read-side
/// half. A decode error rather than a silent normalisation, for the reason
/// [`decode_malo`] gives.
///
/// **[`col::OBIS_CODE`] is checked the same way, and there the argument is
/// strongest**: it is in the merge key. `ObisCode`'s `FromStr` accepts leading
/// zeros, surrounding whitespace and both spellings of the storage group, all of
/// which `Display` collapses onto one canonical form — so a non-canonical code
/// read back as a well-formed one is a channel that a correction, keyed on the
/// canonical spelling, would never supersede. The hot tier refuses it with a
/// `CHECK`; this is the other tier's half.
///
/// Compared through [`Display`], which every one of these types implements as its
/// `as_str` — so the check is "what would we have written?" rather than a second
/// list of accepted codes.
fn decode_code<T>(column: &str, raw: &str) -> Result<T>
where
    T: std::str::FromStr + std::fmt::Display,
    T::Err: std::fmt::Display,
{
    let parsed: T = raw
        .parse()
        .map_err(|e| Error::decode(column, format!("{raw:?}: {e}")))?;
    let canonical = parsed.to_string();
    if canonical != raw {
        return Err(Error::decode(
            column,
            format!(
                "{raw:?} is an accepted input spelling but not the canonical one — storage \
                 holds {canonical:?} and only {canonical:?}, because this column is a \
                 grouping key and two spellings of one value are two rows. Rewrite the \
                 row, or write it through this crate"
            ),
        ));
    }
    Ok(parsed)
}

/// Parse a stored `resolution`, refusing a spelling storage does not write.
///
/// [`decode_code`]'s rule over an ISO 8601 duration rather than a code list.
/// `IntervalResolution` normalises on the way in — `PT900S` parses back as
/// `QuarterHour`, which writes `PT15M` — so without this one interval grid could
/// sit in the column under two spellings, and completeness, which groups by it,
/// would report the meter twice.
///
/// It has no `Display` of its own, so the comparison is against
/// [`to_iso8601`](IntervalResolution::to_iso8601), which is what the encoder
/// writes.
fn decode_resolution(raw: &str) -> Result<metering::resolution::IntervalResolution> {
    let parsed: metering::resolution::IntervalResolution = raw
        .parse()
        .map_err(|e| Error::decode(col::RESOLUTION, format!("{raw:?}: {e}")))?;
    let canonical = parsed.to_iso8601();
    if canonical != raw {
        return Err(Error::decode(
            col::RESOLUTION,
            format!(
                "{raw:?} is an accepted spelling of {canonical:?} but not the canonical one — \
                 storage holds one spelling per grid, because completeness groups by this \
                 column and two spellings are two rows"
            ),
        ));
    }
    Ok(parsed)
}

/// Scale a [`Decimal`] into the fixed-point representation of `value`.
///
/// Rejects values that do not fit rather than truncating: silently dropping a
/// decimal place is exactly the class of bug this crate exists to avoid.
fn to_scaled_i128(value: Decimal) -> Result<i128> {
    let scale = u32::try_from(VALUE_SCALE).expect("scale is non-negative");

    // `Decimal::rescale` to a *lower* scale rounds silently, so it cannot be used
    // as the guard — it would quietly turn 0.0000001 kWh into 0. Normalising
    // first strips trailing zeros, so the remaining scale is the number of
    // significant decimal places the value actually needs.
    let mut v = value.normalize();
    if v.scale() > scale {
        return Err(Error::encode(
            col::VALUE,
            format!(
                "{value} needs {} decimal places, storage holds {VALUE_SCALE}",
                v.scale()
            ),
        ));
    }
    v.rescale(scale);

    let mantissa = v.mantissa();
    let limit = 10i128.pow(u32::from(VALUE_PRECISION));
    if mantissa >= limit || mantissa <= -limit {
        return Err(Error::encode(
            col::VALUE,
            format!("{value} exceeds Decimal128({VALUE_PRECISION},{VALUE_SCALE})"),
        ));
    }
    Ok(mantissa)
}

/// Inverse of [`to_scaled_i128`].
fn from_scaled_i128(raw: i128) -> Decimal {
    Decimal::from_i128_with_scale(
        raw,
        u32::try_from(VALUE_SCALE).expect("scale is non-negative"),
    )
}

/// Encode a source discriminant and its payload.
///
/// [`MeasurementSource`] is a data-carrying enum, so it needs two columns: a
/// stable discriminant that dictionary-encodes and filters cheaply, and a JSON
/// payload that preserves the variant's fields for exact round-tripping.
///
/// # The discriminant is read off the payload, not written out here
///
/// A hand-written `match` would be a second spelling of a vocabulary the domain
/// owns. `MeasurementSource` is externally tagged and renamed
/// `SCREAMING_SNAKE_CASE`, so the payload in the very next column reads
/// `{"MSCONS": {…}}` — and an external engine filtering
/// `source_kind = 'MSCONS'`, the only spelling it can see, would find the two
/// columns disagreeing.
///
/// Taking the tag *from* the serialised form means a variant renamed upstream
/// moves both columns together and a variant added upstream needs no edit here.
/// §4.1.1: the domain's vocabulary has one spelling, and it is the domain's.
fn encode_source(source: &MeasurementSource) -> Result<(String, String)> {
    let payload = serde_json::to_value(source)?;
    let kind = match &payload {
        // The ordinary case: an externally tagged data-carrying variant.
        serde_json::Value::Object(fields) if fields.len() == 1 => {
            fields.keys().next().expect("length checked").clone()
        }
        // A unit variant serialises to its bare tag. None exist today; handling
        // it costs a line and means one added upstream does not need one here.
        serde_json::Value::String(tag) => tag.clone(),
        other => {
            return Err(Error::encode(
                col::SOURCE_KIND,
                format!(
                    "{} does not serialise to a tagged form, so it has no \
                     discriminant to store: {other}",
                    std::any::type_name::<MeasurementSource>()
                ),
            ));
        }
    };
    Ok((kind, serde_json::to_string(&payload)?))
}

/// Encode a series' audit trail for the `provenance` column.
///
/// `metering`'s own `serde`, like [`encode_source`], so the stored vocabulary has
/// one spelling and it is the domain's. The instant lands as RFC 3339 because
/// `metering::wire` says so — `time`'s own impl is feature-conditional, which
/// would leave the on-disk shape of a decades-long audit trail to Cargo feature
/// unification.
fn encode_provenance(trail: &[ProvenanceEntry]) -> Result<String> {
    serde_json::to_string(trail).map_err(|e| Error::encode(col::PROVENANCE, e.to_string()))
}

/// The inverse of [`encode_provenance`].
///
/// Fallible where the encoder is total, for the reason every decoder here is:
/// the input is a string read back out of storage, which a file this crate did
/// not write may set to anything at all. The error names the column, so a
/// malformed audit trail reads as a storage fault rather than as a serde message
/// about a Rust type nobody outside this crate has heard of.
fn decode_provenance(raw: &str) -> Result<Vec<ProvenanceEntry>> {
    serde_json::from_str(raw).map_err(|e| Error::decode(col::PROVENANCE, format!("{raw:?}: {e}")))
}

/// Decode a source from its discriminant and payload.
///
/// The discriminant is authoritative for filtering; the payload is authoritative
/// for reconstruction. A mismatch means the row was written by an incompatible
/// writer, so it is an error rather than a silent preference for one column.
///
/// Both come from the same serialisation ([`encode_source`]), so the check can
/// only fire on a row this crate did not write — which is when it is worth
/// having.
fn decode_source(kind: &str, detail: Option<&str>) -> Result<MeasurementSource> {
    let detail = detail.ok_or_else(|| {
        Error::decode(
            col::SOURCE_DETAIL,
            format!("missing payload for kind {kind:?}"),
        )
    })?;
    let source: MeasurementSource = serde_json::from_str(detail)?;
    let (round_tripped, _) = encode_source(&source)?;
    if round_tripped != kind {
        return Err(Error::decode(
            col::SOURCE_KIND,
            format!("discriminant {kind:?} disagrees with payload {round_tripped:?}"),
        ));
    }
    Ok(source)
}

/// Resolve the OBIS code for an interval.
///
/// The interval wins when present, since a series may carry mixed channels.
/// Both are `Option<ObisCode>`, so this is a choice between validated values
/// rather than a parse that could fail on data already accepted.
fn obis_for(series: &MeasurementSeries, interval: &MeterInterval) -> Result<ObisCode> {
    interval.obis_code.or(series.obis_code).ok_or_else(|| {
        Error::encode(
            col::OBIS_CODE,
            format!(
                "neither interval nor series {} carries an OBIS code",
                series.malo_id
            ),
        )
    })
}

/// Encode one or more [`StoredSeries`] into a single [`RecordBatch`].
///
/// Extra deployment columns are not populated here; they are appended by the
/// caller that owns their values.
pub fn to_record_batch(stored: &[StoredSeries]) -> Result<RecordBatch> {
    to_record_batch_with(stored, &[])
}

/// Encode with the deployment's declared extra columns.
///
/// A **non-nullable** declared column must have a value on every series: those
/// are the identity columns, and a missing one would merge readings that are not
/// the same reading. A nullable column may be absent and becomes null, which is
/// what nullable was declared to mean.
/// The column vectors every storage batch is built from.
///
/// Shared by the interval path and the point path, because the two differ in
/// exactly one column — a Zählerstand has no `to` — and everything else about a
/// row is identical. Two hand-maintained copies of a seventeen-column assembly
/// is how a column ends up in the wrong position on one of them.
#[derive(Default)]
struct RowColumns {
    malo: Vec<String>,
    melo: Vec<Option<String>>,
    obis: Vec<String>,
    sparte: Vec<&'static str>,
    from: Vec<i64>,
    /// `None` on a point row: an instant has no end.
    to: Vec<Option<i64>>,
    value: Vec<i128>,
    unit: Vec<&'static str>,
    quality: Vec<&'static str>,
    resolution: Vec<Option<String>>,
    source_kind: Vec<String>,
    source_detail: Vec<Option<String>>,
    provenance: Vec<Option<String>>,
    version: Vec<i128>,
    version_scope: Vec<String>,
    recorded_at: Vec<i64>,
    balancing_day: Vec<i32>,
    /// One vector per declared extra column, in `extra` order.
    extra: Vec<Vec<ScalarValue>>,
}

/// What every row of one delivery shares, resolved once rather than per row.
struct SeriesFields {
    malo: String,
    melo: Option<String>,
    sparte: &'static str,
    unit: &'static str,
    resolution: Option<String>,
    source_kind: String,
    source_detail: String,
    provenance: String,
    version: i128,
    version_scope: String,
    recorded_at: i64,
    extra: Vec<ScalarValue>,
}

impl RowColumns {
    fn with_extra(extra: &[Field]) -> Self {
        Self {
            extra: vec![Vec::new(); extra.len()],
            ..Default::default()
        }
    }

    /// Append one row.
    ///
    /// `to` is `None` for a point row. `obis` and `balancing_day` are per row
    /// rather than per delivery: a series may carry mixed channels, and it may
    /// span a midnight or a 06:00 Gastag boundary.
    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        fields: &SeriesFields,
        obis: String,
        from: i64,
        to: Option<i64>,
        value: i128,
        quality: &'static str,
        balancing_day: i32,
    ) {
        self.malo.push(fields.malo.clone());
        self.melo.push(fields.melo.clone());
        self.obis.push(obis);
        self.sparte.push(fields.sparte);
        self.from.push(from);
        self.to.push(to);
        self.value.push(value);
        self.unit.push(fields.unit);
        self.quality.push(quality);
        self.resolution.push(fields.resolution.clone());
        self.source_kind.push(fields.source_kind.clone());
        self.source_detail.push(Some(fields.source_detail.clone()));
        self.provenance.push(Some(fields.provenance.clone()));
        self.version.push(fields.version);
        self.version_scope.push(fields.version_scope.clone());
        self.recorded_at.push(fields.recorded_at);
        self.balancing_day.push(balancing_day);
        for (column, value) in self.extra.iter_mut().zip(&fields.extra) {
            column.push(value.clone());
        }
    }

    /// Assemble the batch.
    fn finish(self, extra: &[Field]) -> Result<RecordBatch> {
        let tz: Arc<str> = "UTC".into();
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(self.malo)),
            Arc::new(StringArray::from(self.melo)),
            Arc::new(StringArray::from(self.obis)),
            Arc::new(StringArray::from(self.sparte)),
            Arc::new(TimestampMicrosecondArray::from(self.from).with_timezone(tz.clone())),
            Arc::new(TimestampMicrosecondArray::from(self.to).with_timezone(tz.clone())),
            Arc::new(
                Decimal128Array::from(self.value)
                    .with_precision_and_scale(VALUE_PRECISION, VALUE_SCALE)?,
            ),
            Arc::new(StringArray::from(self.unit)),
            Arc::new(StringArray::from(self.quality)),
            Arc::new(StringArray::from(self.resolution)),
            Arc::new(StringArray::from(self.source_kind)),
            Arc::new(StringArray::from(self.source_detail)),
            Arc::new(StringArray::from(self.provenance)),
            Arc::new(
                Decimal128Array::from(self.version)
                    .with_precision_and_scale(VERSION_PRECISION, VERSION_SCALE)?,
            ),
            Arc::new(StringArray::from(self.version_scope)),
            Arc::new(TimestampMicrosecondArray::from(self.recorded_at).with_timezone(tz)),
            Arc::new(crate::arrow::array::Date32Array::from(self.balancing_day)),
        ];

        for (field, values) in extra.iter().zip(self.extra) {
            // `iter_to_array` refuses an empty iterator, so a zero-row batch has
            // to build its extra columns another way. A zero-row batch is
            // ordinary — a delivery whose series carry no values — and it must
            // produce the same shape as any other, or a table with deployment
            // columns would fail on exactly the input a table without them
            // accepts.
            columns.push(match values.is_empty() {
                true => crate::arrow::array::new_empty_array(field.data_type()),
                false => ScalarValue::iter_to_array(values)?,
            });
        }

        Ok(RecordBatch::try_new(
            schema::storage_schema(extra),
            columns,
        )?)
    }
}

/// Resolve the deployment columns a delivery supplies, once for the whole
/// delivery.
///
/// A **non-nullable** declared column must have a value: those are the identity
/// columns, and a missing one would merge readings that are not the same
/// reading. A nullable column may be absent and becomes null, which is what
/// nullable was declared to mean.
fn resolve_extra(
    extra: &[Field],
    supplied: &BTreeMap<String, ScalarValue>,
) -> Result<Vec<ScalarValue>> {
    let mut out = Vec::with_capacity(extra.len());
    for field in extra {
        let value = match supplied.get(field.name()) {
            Some(value) => value.clone(),
            None if field.is_nullable() => ScalarValue::try_from(field.data_type())?,
            None => {
                return Err(Error::encode(
                    field.name(),
                    "declared column has no value on this delivery, and the \
                     column is not nullable"
                        .to_string(),
                ));
            }
        };
        if value.data_type() != *field.data_type() {
            return Err(Error::encode(
                field.name(),
                format!(
                    "declared as {:?} but value is {:?}",
                    field.data_type(),
                    value.data_type()
                ),
            ));
        }
        // An explicit null for a non-nullable column would otherwise fail deep
        // inside Arrow with a message that does not name the column.
        if value.is_null() && !field.is_nullable() {
            return Err(Error::encode(
                field.name(),
                "value is null but the column is not nullable".to_string(),
            ));
        }
        out.push(check_declared_value(field, value)?);
    }
    Ok(out)
}

/// Parse a declared column's value with the domain type it names, and store
/// that type's canonical spelling.
///
/// The write is the only place a check character is worth anything: a wrong one
/// found in a settlement run a month later names a row and nothing that could
/// correct it. See [`eic_column`](crate::config::eic_column).
///
/// Canonicalising rather than merely accepting, because a checked column may be
/// an identity column — one identifier in two spellings would be two merge keys,
/// the failure [`canonical_obis`](crate::canonical_obis) prevents for OBIS.
fn check_declared_value(field: &Field, value: ScalarValue) -> Result<ScalarValue> {
    let Some(check) = crate::config::declared_value_check(field) else {
        return Ok(value);
    };
    // The declaration is validated before the value, and unconditionally. A
    // metadata value this build does not know is refused rather than ignored —
    // a column declared as checked and silently written unchecked is the one
    // outcome the declaration exists to rule out — and doing it here rather
    // than in the parse arm means a delivery that happens to leave the column
    // null does not slip past the same mistake.
    if check != crate::config::VALUE_CHECK_EIC {
        return Err(Error::encode(
            field.name(),
            format!(
                "declares an unknown value check {check:?}; this build knows {:?}",
                crate::config::VALUE_CHECK_EIC
            ),
        ));
    }
    // A null passed the nullability check above, so there is nothing to parse.
    let ScalarValue::Utf8(Some(text)) = &value else {
        return Ok(value);
    };
    let eic: metering::ids::Eic = text.parse().map_err(|e| {
        Error::encode(
            field.name(),
            format!(
                "{text:?} is not an EIC: {e}. The check character is part of the \
                 code, so a transposition is detectable here — and only here, \
                 while the delivery that carried it is still in hand"
            ),
        )
    })?;
    Ok(ScalarValue::Utf8(Some(eic.as_str().to_string())))
}

/// The version scope check every write path makes, once per row.
///
/// A scope keyed to the delivery month rather than the interval's would leave
/// two versions of one reading unable to supersede each other, doubling any sum
/// over it. Nothing downstream can detect that, so it is rejected here.
///
/// The commodity is passed because the Bilanzierungsmonat is cut at 06:00 local
/// for gas: a value at 02:00 on 1 March belongs to February's gas scope, and
/// checking it against the calendar month refused the correctly-scoped delivery
/// and accepted the wrong one.
fn check_scope(version: &ScopedVersion, at: OffsetDateTime, sparte: Sparte) -> Result<()> {
    if version.scope().covers(at, sparte) {
        return Ok(());
    }
    Err(Error::encode(
        col::VERSION_SCOPE,
        format!(
            "value at {at} is not in scope {} for {sparte} — the scope must be \
             derived from the value's own Bilanzierungsmonat, not the delivery \
             month, and for gas that month is cut at 06:00 local rather than at \
             midnight. VersionScope::for_interval derives it",
            version.scope(),
        ),
    ))
}

/// Encode interval series with the deployment's declared extra columns.
///
/// A **non-nullable** declared column must have a value on every delivery: those
/// are the identity columns, and a missing one would merge readings that are not
/// the same reading. A nullable column may be absent and becomes null, which is
/// what nullable was declared to mean.
pub fn to_record_batch_with(stored: &[StoredSeries], extra: &[Field]) -> Result<RecordBatch> {
    let mut columns = RowColumns::with_extra(extra);

    for s in stored {
        check_unit(s.sparte, s.unit, &s.series.malo_id)?;
        let (kind, detail) = encode_source(&s.series.source)?;
        // A delivery with no values contributes no rows, so it has nothing to
        // label. Resolving the declared columns for it would fail a batch on a
        // column that would never have been written — and the row loop below
        // already contributes nothing, so the two have to agree.
        if s.series.intervals.is_empty() {
            continue;
        }
        let fields = SeriesFields {
            // Rendered once per delivery rather than once per row: the
            // identifiers are the same for every row, and a 96-row day would
            // otherwise format the same eleven digits ninety-six times.
            malo: s.series.malo_id.to_string(),
            melo: s.series.melo_id.as_ref().map(MeloId::to_string),
            sparte: s.sparte.as_str(),
            unit: s.unit.as_str(),
            resolution: s.series.resolution.map(|r| r.to_iso8601()),
            source_kind: kind,
            source_detail: detail,
            provenance: encode_provenance(&s.series.provenance)?,
            version: s.version.version().to_i128(),
            version_scope: s.version.scope().as_str().to_string(),
            recorded_at: schema::micros(s.recorded_at),
            extra: resolve_extra(extra, &s.extra)?,
        };

        for interval in &s.series.intervals {
            check_scope(&s.version, interval.from, s.sparte)?;

            if interval.to <= interval.from {
                return Err(Error::encode(
                    col::TO,
                    format!(
                        "interval end {} is not after start {} for {}",
                        interval.to, interval.from, s.series.malo_id
                    ),
                ));
            }

            columns.push(
                &fields,
                obis_for(&s.series, interval)?.to_string(),
                schema::micros(interval.from),
                Some(schema::micros(interval.to)),
                to_scaled_i128(interval.value)?,
                interval.quality.as_str(),
                // The one place the balancing day is derived. Asked of the
                // calendar per row rather than per delivery, because a delivery
                // may span the 06:00 Gastag boundary — or a midnight — and every
                // row of it does not share a day.
                schema::date32(crate::planner::balancing_day(interval.from, s.sparte)),
            );
        }
    }

    columns.finish(extra)
}

/// Encode Zählerstandsgänge into a single [`RecordBatch`].
///
/// The point-series counterpart of [`to_record_batch`]. Every column is the same
/// and carries the same meaning except two, and both differences are the whole
/// of what a point series is:
///
/// * `to` is **null** — a register reading is an instant, not a span.
/// * `value` is the register's cumulative reading rather than energy over a
///   span. That is why it is a different table, not a flag on this one.
///
/// The `resolution` column carries the reading **cadence**, which is the same
/// question completeness asks of an interval series: how many values a day
/// should there be.
pub fn readings_to_record_batch(stored: &[StoredReadings]) -> Result<RecordBatch> {
    readings_to_record_batch_with(stored, &[])
}

/// [`readings_to_record_batch`] with the deployment's declared extra columns.
pub fn readings_to_record_batch_with(
    stored: &[StoredReadings],
    extra: &[Field],
) -> Result<RecordBatch> {
    let mut columns = RowColumns::with_extra(extra);

    for s in stored {
        check_unit(s.sparte, s.unit, &s.malo_id)?;
        let (kind, detail) = encode_source(&s.source)?;
        // A delivery with no values contributes no rows, so it has nothing to
        // label. Resolving the declared columns for it would fail a batch on a
        // column that would never have been written — and the row loop below
        // already contributes nothing, so the two have to agree.
        if s.readings.is_empty() {
            continue;
        }
        let obis = s.obis_code.to_string();
        let fields = SeriesFields {
            malo: s.malo_id.to_string(),
            melo: s.melo_id.as_ref().map(MeloId::to_string),
            sparte: s.sparte.as_str(),
            unit: s.unit.as_str(),
            resolution: s.cadence.map(|r| r.to_iso8601()),
            source_kind: kind,
            source_detail: detail,
            provenance: encode_provenance(&s.provenance)?,
            version: s.version.version().to_i128(),
            version_scope: s.version.scope().as_str().to_string(),
            recorded_at: schema::micros(s.recorded_at),
            extra: resolve_extra(extra, &s.extra)?,
        };

        for reading in &s.readings {
            check_scope(&s.version, reading.at, s.sparte)?;
            columns.push(
                &fields,
                // A reading may name its own register; the delivery's is the
                // fallback, exactly as for an interval series.
                reading
                    .obis_code
                    .map_or_else(|| obis.clone(), |c| c.to_string()),
                schema::micros(reading.at),
                // The difference that makes this a point series.
                None,
                to_scaled_i128(reading.value)?,
                reading.quality.as_str(),
                schema::date32(crate::planner::balancing_day(reading.at, s.sparte)),
            );
        }
    }

    columns.finish(extra)
}

/// Put a batch into the sort order the Parquet footer declares.
///
/// Every data file this crate writes declares `(malo_id, from)` as its sort order
/// ([`cold::parquet`]), and a reader is entitled to act on that: skip a row group
/// whose `malo_id` range cannot contain the meter it wants, stop early, merge
/// without re-sorting. A footer declaring an order the rows are not in is worse
/// than no declaration at all, because what it produces is a *silently missing*
/// row rather than a slow scan.
///
/// Archival satisfies it for free — the hot scan pages by a keyset cursor whose
/// prefix is exactly these columns
/// ([`ScanSpec::cursor_columns`](crate::tiering::store::ScanSpec::cursor_columns)).
/// A **late correction** is written straight from the delivery, in whatever order
/// it carried, so it sorts here first. That batch is in memory and proportional
/// to what changed rather than to the history, and the sort sharpens the file's
/// own row-group statistics as a side effect.
///
/// [`cold::parquet`]: crate::cold::parquet
pub fn sorted_for_storage(batch: &RecordBatch) -> Result<RecordBatch> {
    use crate::arrow::compute::{SortColumn, lexsort_to_indices, take};

    // One row is trivially sorted, and `lexsort_to_indices` on an empty batch
    // would be a round trip for nothing.
    if batch.num_rows() < 2 {
        return Ok(batch.clone());
    }

    let mut columns = Vec::with_capacity(schema::SORT_COLUMNS.len());
    for name in schema::SORT_COLUMNS {
        columns.push(SortColumn {
            values: batch
                .column_by_name(name)
                .ok_or_else(|| Error::encode(name, "declared a sort column but not in the batch"))?
                .clone(),
            // Ascending, nulls last — the order `sorting_columns` declares. Both
            // sort columns are non-nullable, so the null half never applies.
            options: None,
        });
    }

    let indices = lexsort_to_indices(&columns, None)?;
    let sorted = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(batch.schema(), sorted)?)
}

/// Distinct `malo_id` values across a set of batches.
///
/// Sizes the Parquet bloom filter on the column §10.2 calls the highest-leverage
/// one. Only usable where the rows are already in memory — a streaming write
/// cannot look ahead, and takes an estimate instead (`WriteHints`).
pub fn distinct_malo_ids(batches: &[RecordBatch]) -> u64 {
    let mut seen = std::collections::HashSet::new();
    for batch in batches {
        if let Some(column) = batch.column_by_name(col::MALO_ID)
            && let Some(values) = column.as_any().downcast_ref::<StringArray>()
        {
            for i in 0..values.len() {
                if !values.is_null(i) {
                    seen.insert(values.value(i).to_string());
                }
            }
        }
    }
    seen.len() as u64
}

/// Downcast a column, producing a decode error rather than panicking.
pub(crate) fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .ok_or_else(|| Error::decode(name, "column missing"))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::decode(name, "unexpected array type"))
}

/// The columns that describe an individual reading rather than the delivery it
/// arrived in.
///
/// Everything *else* in the batch is series-level, and therefore what a decoded
/// run has to agree on — see [`from_record_batch`].
///
/// [`col::BALANCING_DAY`] belongs here because it is a function of `from`: a
/// delivery covering a whole day carries two of them (or, for gas, changes value
/// at 06:00 local), and treating it as series-level would cut one delivery into
/// a separate series per day.
pub const INTERVAL_COLUMNS: [&str; 5] = [
    col::FROM,
    col::TO,
    col::VALUE,
    col::QUALITY,
    col::BALANCING_DAY,
];

/// Decode a [`RecordBatch`] back into [`StoredSeries`], one per contiguous run of
/// rows agreeing on **every** series-level column.
///
/// Rows are grouped, not merged: version resolution is a query-planner concern,
/// not an encoding one.
///
/// # The run key is derived, not listed
///
/// A [`MeasurementSeries`] carries fields that describe the *delivery* — its
/// `source`, its `provenance`, its declared `resolution` — and those are read
/// once per run, from its first row. So a run must agree on them, or the decoded
/// series attributes one delivery's values to another delivery's source. That is
/// silent corruption of the audit trail this store exists to keep, and it is
/// invisible afterwards: the series looks perfectly well-formed.
///
/// A key listing those columns by hand is a key that forgets one: two deliveries
/// sharing a version and scope — one MSCONS, one SMGW — would fold into a single
/// series carrying the first one's origin for all of it.
///
/// So the key is everything that is not [`INTERVAL_COLUMNS`], which means a
/// column added to the schema joins it automatically rather than having to be
/// remembered. `obis_code` is in it deliberately: the MSCONS handbook defines a
/// time series as one channel, so a decoded series is one channel.
pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<StoredSeries>> {
    let core = schema::storage_schema(&[]);
    let extra_names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .filter(|f| core.field_with_name(f.name()).is_err())
        .map(|f| f.name().clone())
        .collect();

    let malo = column::<StringArray>(batch, col::MALO_ID)?;
    let melo = column::<StringArray>(batch, col::MELO_ID)?;
    let obis = column::<StringArray>(batch, col::OBIS_CODE)?;
    let sparte = column::<StringArray>(batch, col::SPARTE)?;
    let from = column::<TimestampMicrosecondArray>(batch, col::FROM)?;
    let to = column::<TimestampMicrosecondArray>(batch, col::TO)?;
    let value = column::<Decimal128Array>(batch, col::VALUE)?;
    let unit = column::<StringArray>(batch, col::UNIT)?;
    let quality = column::<StringArray>(batch, col::QUALITY)?;
    let resolution = column::<StringArray>(batch, col::RESOLUTION)?;
    let source_kind = column::<StringArray>(batch, col::SOURCE_KIND)?;
    let source_detail = column::<StringArray>(batch, col::SOURCE_DETAIL)?;
    let provenance = column::<StringArray>(batch, col::PROVENANCE)?;
    let version = column::<Decimal128Array>(batch, col::VERSION)?;
    let version_scope = column::<StringArray>(batch, col::VERSION_SCOPE)?;
    let recorded_at = column::<TimestampMicrosecondArray>(batch, col::RECORDED_AT)?;

    // Contiguous runs over every series-level column, found by Arrow rather than
    // by a hand-rolled comparison: `partition` returns the ranges of consecutive
    // rows that agree on all of them, which is precisely the definition above.
    let key_columns: Vec<crate::arrow::array::ArrayRef> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !INTERVAL_COLUMNS.contains(&f.name().as_str()))
        .map(|(i, _)| batch.column(i).clone())
        .collect();
    let starts: std::collections::HashSet<usize> = if batch.num_rows() == 0 {
        Default::default()
    } else {
        crate::arrow::compute::partition(&key_columns)?
            .ranges()
            .iter()
            .map(|r| r.start)
            .collect()
    };

    let mut out: Vec<StoredSeries> = Vec::new();

    for i in 0..batch.num_rows() {
        if starts.contains(&i) {
            let source = decode_source(
                source_kind.value(i),
                (!source_detail.is_null(i)).then(|| source_detail.value(i)),
            )?;
            // `IntervalResolution` normalises on parse — `PT900S` reads back as
            // `QuarterHour`, which writes `PT15M` — so the canonical check is
            // what stops one grid being stored under two ISO 8601 spellings.
            let res = match resolution.is_null(i) {
                true => None,
                false => Some(decode_resolution(resolution.value(i))?),
            };

            let prov: Vec<ProvenanceEntry> = if provenance.is_null(i) {
                Vec::new()
            } else {
                decode_provenance(provenance.value(i))?
            };

            let mut extra = BTreeMap::new();
            for name in &extra_names {
                let column = batch
                    .column_by_name(name)
                    .ok_or_else(|| Error::decode(name, "column vanished"))?;
                extra.insert(name.clone(), ScalarValue::try_from_array(column, i)?);
            }

            let sparte: Sparte = decode_code(col::SPARTE, sparte.value(i))?;
            let unit: MeasurementUnit = decode_code(col::UNIT, unit.value(i))?;
            let malo_id = decode_malo(malo.value(i))?;
            let melo_id = match melo.is_null(i) {
                true => None,
                false => Some(decode_melo(melo.value(i))?),
            };
            // The same rule as on the write path. A row that reached storage
            // before the check existed, or through a writer that bypassed it,
            // must not be handed back as if its dimension were sound.
            check_unit(sparte, unit, &malo_id)?;

            out.push(StoredSeries {
                extra,
                sparte,
                unit,
                series: MeasurementSeries {
                    malo_id,
                    melo_id,
                    // `Some`, always: the column is non-nullable and the decode
                    // below would have refused a code that does not parse. It
                    // was `.parse().ok()`, which turned a malformed key into a
                    // series carrying no channel at all — silently, and only on
                    // the half of the pair a reader is least likely to check.
                    obis_code: Some(decode_code(col::OBIS_CODE, obis.value(i))?),
                    resolution: res,
                    source,
                    intervals: Vec::new(),
                    provenance: prov,
                },
                version: ScopedVersion::new(
                    VersionScope::parse(version_scope.value(i))?,
                    Version::from_i128(version.value(i))?,
                ),
                recorded_at: schema::instant(recorded_at.value(i))?,
            });
        }

        // A null `to` is a *point* row — a register reading at an instant, whose
        // `value` is a cumulative reading rather than energy over a span.
        // Reading `to` anyway would take the null's zero and produce an interval
        // ending at the Unix epoch, silently.
        if to.is_null(i) {
            return Err(Error::decode(
                col::TO,
                format!(
                    "row {i} has no span end, so it is a register reading rather than an \
                     interval and its value means something else. Use \
                     readings_from_record_batch for a point table"
                ),
            ));
        }

        let series = out.last_mut().expect("pushed above");
        series.series.intervals.push(MeterInterval {
            from: schema::instant(from.value(i))?,
            to: schema::instant(to.value(i))?,
            value: from_scaled_i128(value.value(i)),
            quality: decode_code(col::QUALITY, quality.value(i))?,
            obis_code: Some(decode_code(col::OBIS_CODE, obis.value(i))?),
        });
    }

    Ok(out)
}

/// Decode a [`RecordBatch`] of point rows back into [`StoredReadings`].
///
/// The point counterpart of [`from_record_batch`], and it groups the same way:
/// one delivery per contiguous run of rows agreeing on every series-level
/// column, which for a Zählerstandsgang means everything but `from`, `value`,
/// `quality` and `balancing_day`.
///
/// # It refuses a row with a `to`
///
/// A row carrying a span end is an *interval* row, and its `value` is energy
/// over that span rather than a register reading. Decoding it as a reading would
/// hand back a number that means something else entirely, so the shape is
/// checked rather than assumed — the same posture the unit check takes.
pub fn readings_from_record_batch(batch: &RecordBatch) -> Result<Vec<StoredReadings>> {
    let core = schema::storage_schema(&[]);
    let extra_names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .filter(|f| core.field_with_name(f.name()).is_err())
        .map(|f| f.name().clone())
        .collect();

    let malo = column::<StringArray>(batch, col::MALO_ID)?;
    let melo = column::<StringArray>(batch, col::MELO_ID)?;
    let obis = column::<StringArray>(batch, col::OBIS_CODE)?;
    let sparte = column::<StringArray>(batch, col::SPARTE)?;
    let from = column::<TimestampMicrosecondArray>(batch, col::FROM)?;
    let to = column::<TimestampMicrosecondArray>(batch, col::TO)?;
    let value = column::<Decimal128Array>(batch, col::VALUE)?;
    let unit = column::<StringArray>(batch, col::UNIT)?;
    let quality = column::<StringArray>(batch, col::QUALITY)?;
    let resolution = column::<StringArray>(batch, col::RESOLUTION)?;
    let source_kind = column::<StringArray>(batch, col::SOURCE_KIND)?;
    let source_detail = column::<StringArray>(batch, col::SOURCE_DETAIL)?;
    let provenance = column::<StringArray>(batch, col::PROVENANCE)?;
    let version = column::<Decimal128Array>(batch, col::VERSION)?;
    let version_scope = column::<StringArray>(batch, col::VERSION_SCOPE)?;
    let recorded_at = column::<TimestampMicrosecondArray>(batch, col::RECORDED_AT)?;

    let key_columns: Vec<crate::arrow::array::ArrayRef> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !INTERVAL_COLUMNS.contains(&f.name().as_str()))
        .map(|(i, _)| batch.column(i).clone())
        .collect();
    let starts: std::collections::HashSet<usize> = if batch.num_rows() == 0 {
        Default::default()
    } else {
        crate::arrow::compute::partition(&key_columns)?
            .ranges()
            .iter()
            .map(|r| r.start)
            .collect()
    };

    let mut out: Vec<StoredReadings> = Vec::new();
    for i in 0..batch.num_rows() {
        if !to.is_null(i) {
            return Err(Error::decode(
                col::TO,
                format!(
                    "row {i} carries a span end, so it is an interval and its value is \
                     energy over that span rather than a register reading. A point table \
                     stores no {:?}; use from_record_batch for an interval table",
                    col::TO
                ),
            ));
        }

        if starts.contains(&i) {
            let source = decode_source(
                source_kind.value(i),
                (!source_detail.is_null(i)).then(|| source_detail.value(i)),
            )?;
            let cadence = match resolution.is_null(i) {
                true => None,
                false => Some(decode_resolution(resolution.value(i))?),
            };
            let prov: Vec<ProvenanceEntry> = match provenance.is_null(i) {
                true => Vec::new(),
                false => decode_provenance(provenance.value(i))?,
            };
            let mut extra = BTreeMap::new();
            for name in &extra_names {
                let column = batch
                    .column_by_name(name)
                    .ok_or_else(|| Error::decode(name, "column vanished"))?;
                extra.insert(name.clone(), ScalarValue::try_from_array(column, i)?);
            }

            let sparte: Sparte = decode_code(col::SPARTE, sparte.value(i))?;
            let unit: MeasurementUnit = decode_code(col::UNIT, unit.value(i))?;
            let malo_id = decode_malo(malo.value(i))?;
            check_unit(sparte, unit, &malo_id)?;

            out.push(StoredReadings {
                malo_id,
                melo_id: match melo.is_null(i) {
                    true => None,
                    false => Some(decode_melo(melo.value(i))?),
                },
                obis_code: decode_code(col::OBIS_CODE, obis.value(i))?,
                readings: Vec::new(),
                cadence,
                source,
                provenance: prov,
                sparte,
                unit,
                version: ScopedVersion::new(
                    VersionScope::parse(version_scope.value(i))?,
                    Version::from_i128(version.value(i))?,
                ),
                recorded_at: schema::instant(recorded_at.value(i))?,
                extra,
            });
        }

        let delivery = out.last_mut().expect("pushed above");
        delivery.readings.push(metering::reading::MeterReading {
            at: schema::instant(from.value(i))?,
            value: from_scaled_i128(value.value(i)),
            quality: decode_code(col::QUALITY, quality.value(i))?,
            obis_code: Some(decode_code(col::OBIS_CODE, obis.value(i))?),
        });
    }

    Ok(out)
}

/// Canonicalise an OBIS code for storage.
///
/// **Storage holds the canonical form, and only the canonical form.** The column
/// is part of the merge key, so two spellings of one channel would mean a
/// correction could not supersede the value it corrects.
///
/// Thin wrapper over [`ObisCode::normalize`], kept because applications writing
/// to the hot tier directly need it and should not have to reach past this crate
/// to find it. The hot table also enforces the canonical shape with a
/// constraint, so a mistake fails at the write.
pub fn canonical_obis(code: &str) -> Result<String> {
    ObisCode::normalize(code).map_err(|e| Error::encode(col::OBIS_CODE, format!("{code:?}: {e}")))
}

/// Parse a caller-supplied MaLo-ID.
///
/// The counterpart of [`canonical_obis`] for the other half of the merge key,
/// and the reason read entry points take `impl TryInto<MaloId>` rather than a
/// string: a `MaloId` already parsed by the caller passes through at no cost
/// (its `TryInto` is infallible), while a raw eleven digits off a market message
/// is checked here — length, Vergabestelle and check digit — instead of silently
/// selecting a measuring point that does not exist.
pub fn parse_malo<M>(malo_id: M) -> Result<MaloId>
where
    M: TryInto<MaloId>,
    M::Error: std::fmt::Display,
{
    malo_id
        .try_into()
        .map_err(|e| Error::encode(col::MALO_ID, e.to_string()))
}

/// Build the Arrow [`Field`]s for extra deployment columns.
pub fn extra_field(name: &str, ty: crate::arrow::datatypes::DataType) -> Field {
    Field::new(name, ty, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::datatypes::DataType;
    use metering::QualityFlag;
    use metering::resolution::IntervalResolution;
    use time::macros::datetime;

    /// A batch whose `malo_id` column carries the given values.
    fn batch_with_malos(ids: &[&str]) -> RecordBatch {
        let schema = schema::storage_schema(&[]);
        let n = ids.len();
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                crate::arrow::datatypes::DataType::Utf8 => {
                    if f.name() == col::MALO_ID {
                        Arc::new(StringArray::from(ids.to_vec())) as _
                    } else {
                        Arc::new(StringArray::from(vec!["x"; n])) as _
                    }
                }
                crate::arrow::datatypes::DataType::Timestamp(_, _) => {
                    Arc::new(TimestampMicrosecondArray::from(vec![0i64; n]).with_timezone("UTC"))
                        as _
                }
                crate::arrow::datatypes::DataType::Date32 => {
                    Arc::new(crate::arrow::array::Date32Array::from(vec![0i32; n])) as _
                }
                crate::arrow::datatypes::DataType::Decimal128(p, s) => Arc::new(
                    Decimal128Array::from(vec![0i128; n])
                        .with_precision_and_scale(*p, *s)
                        .unwrap(),
                ) as _,
                other => panic!("unhandled {other:?}"),
            })
            .collect();
        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn distinct_malo_ids_counts_uniques_not_rows() {
        // The bloom filter is sized by distinct values, not row count; sizing it
        // by rows would over-allocate by two orders of magnitude at 96 intervals
        // per meter per day.
        let b = batch_with_malos(&["a", "a", "b", "c", "c", "c"]);
        assert_eq!(distinct_malo_ids(&[b]), 3);
    }

    #[test]
    fn distinct_malo_ids_spans_batches() {
        let a = batch_with_malos(&["a", "b"]);
        let b = batch_with_malos(&["b", "c"]);
        assert_eq!(distinct_malo_ids(&[a, b]), 3);
    }

    #[test]
    fn sorting_for_storage_produces_the_order_the_footer_declares() {
        // Two meters interleaved and out of time order, which is exactly what a
        // late correction assembled from a delivery looks like.
        let batch = batch_with_malos(&["b", "a", "b", "a"]);
        let starts = TimestampMicrosecondArray::from(vec![20i64, 30, 10, 5]).with_timezone("UTC");
        let index = batch.schema().index_of(col::FROM).unwrap();
        let mut columns = batch.columns().to_vec();
        columns[index] = Arc::new(starts);
        let batch = RecordBatch::try_new(batch.schema(), columns).unwrap();

        let sorted = sorted_for_storage(&batch).unwrap();

        let malo = column::<StringArray>(&sorted, col::MALO_ID).unwrap();
        let from = column::<TimestampMicrosecondArray>(&sorted, col::FROM).unwrap();
        let observed: Vec<(&str, i64)> = (0..sorted.num_rows())
            .map(|i| (malo.value(i), from.value(i)))
            .collect();
        assert_eq!(observed, vec![("a", 5), ("a", 30), ("b", 10), ("b", 20)]);
        assert_eq!(sorted.schema(), batch.schema());
    }

    #[test]
    fn sorting_for_storage_leaves_a_trivial_batch_alone() {
        for ids in [vec![], vec!["a"]] {
            let batch = batch_with_malos(&ids);
            let sorted = sorted_for_storage(&batch).unwrap();
            assert_eq!(sorted.num_rows(), batch.num_rows());
            assert_eq!(sorted.schema(), batch.schema());
        }
    }

    #[test]
    fn distinct_malo_ids_of_nothing_is_zero() {
        assert_eq!(distinct_malo_ids(&[]), 0);
    }

    const INGESTED_AT: OffsetDateTime = datetime!(2026-07-27 06:00 UTC);

    /// The scope an interval belongs to — always derived, never guessed.
    fn scope_for(interval: OffsetDateTime) -> VersionScope {
        VersionScope::for_interval("9900000000001", interval, Sparte::Strom).unwrap()
    }

    /// A MaLo-ID from its canonical spelling — the check digit is real, so the
    /// literals in these tests are the ones the Bildungsvorschrift admits.
    fn malo(s: &str) -> MaloId {
        s.parse().expect("test MaLo-ID is well-formed")
    }

    fn source() -> MeasurementSource {
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: Some("MSG-1".to_string()),
            sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
        }
    }

    fn series(intervals: Vec<MeterInterval>) -> StoredSeries {
        let first_start = intervals.first().map(|i| i.from).unwrap_or(INGESTED_AT);
        StoredSeries {
            extra: BTreeMap::new(),
            sparte: Sparte::Strom,
            unit: MeasurementUnit::KiloWattHour,
            series: {
                let mut s = MeasurementSeries::new(
                    malo("12345678905"),
                    "1-0:1.8.0".parse().ok(),
                    intervals,
                    source(),
                    // Injected rather than read from the clock, so a series
                    // built twice from the same inputs is identical.
                    INGESTED_AT,
                )
                .with_melo_id("DE0001234567890123456789012345678".parse().unwrap());
                s.resolution = Some(IntervalResolution::QuarterHour);
                s
            },
            version: ScopedVersion::new(
                scope_for(first_start),
                Version::new(20_260_727_000_001).unwrap(),
            ),
            recorded_at: datetime!(2026-07-27 10:00 UTC),
        }
    }

    #[test]
    fn a_nullable_extra_column_may_be_absent() {
        // The config permits nullable attribute columns, so requiring a value
        // for every one of them contradicted it: a Bilanzkreis not yet assigned,
        // or a measuring point with no known occupant, is an ordinary state.
        use crate::arrow::array::Array;

        let s = series(vec![quarter(
            datetime!(2026-07-20 00:00 UTC),
            "1.5",
            QualityFlag::Measured,
        )]);
        let field = Field::new("bilanzkreis", DataType::Utf8, true);

        let batch = to_record_batch_with(&[s], std::slice::from_ref(&field)).unwrap();
        let column = batch.column_by_name("bilanzkreis").unwrap();
        assert_eq!(column.null_count(), 1, "absent means null, not an error");
    }

    #[test]
    fn a_batch_with_no_rows_keeps_its_declared_shape() {
        // A delivery whose series carry no intervals is ordinary. Building the
        // extra columns through `iter_to_array`, which refuses an empty
        // iterator, made a table *with* deployment columns fail on exactly the
        // input a table without them accepts.
        let field = Field::new("tenant", DataType::Utf8, false);

        let empty = to_record_batch_with(&[], std::slice::from_ref(&field)).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(
            empty.schema(),
            schema::storage_schema(std::slice::from_ref(&field))
        );

        let no_intervals = series(vec![]);
        let batch = to_record_batch_with(&[no_intervals], std::slice::from_ref(&field)).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn a_non_nullable_extra_column_must_be_supplied() {
        // Identity columns are validated non-nullable precisely so a missing
        // tenant cannot silently become a null that groups with everyone else.
        let s = series(vec![quarter(
            datetime!(2026-07-20 00:00 UTC),
            "1.5",
            QualityFlag::Measured,
        )]);
        let field = Field::new("tenant", DataType::Utf8, false);

        let err = to_record_batch_with(&[s], std::slice::from_ref(&field)).unwrap_err();
        assert!(err.to_string().contains("tenant"), "{err}");
    }

    #[test]
    fn an_explicit_null_in_a_non_nullable_column_is_named() {
        // Without the check this fails inside Arrow with a message that does not
        // say which column was at fault.
        let s = series(vec![quarter(
            datetime!(2026-07-20 00:00 UTC),
            "1.5",
            QualityFlag::Measured,
        )])
        .with_extra("tenant", ScalarValue::Utf8(None));
        let field = Field::new("tenant", DataType::Utf8, false);

        let err = to_record_batch_with(&[s], std::slice::from_ref(&field)).unwrap_err();
        assert!(err.to_string().contains("tenant"), "{err}");
        assert!(err.to_string().contains("null"), "{err}");
    }

    #[test]
    fn a_checked_column_parses_its_value_and_stores_the_canonical_spelling() {
        // The write is where a check character is still worth something: the
        // delivery that carried the code is in hand, and a settlement run a
        // month later can only name the row.
        let s = series(vec![quarter(
            datetime!(2026-07-20 00:00 UTC),
            "1.5",
            QualityFlag::Measured,
        )])
        // Lowercase and padded — a shape a CSV or a hand-edited mapping
        // produces, and one that would be a *second* merge key if the column
        // were an identity one.
        .with_extra(
            "bilanzkreis",
            ScalarValue::Utf8(Some("  11xbk0000000001a  ".to_string())),
        );
        let field = crate::config::eic_column("bilanzkreis", true);

        let batch = to_record_batch_with(&[s], std::slice::from_ref(&field)).unwrap();
        let stored = batch
            .column_by_name("bilanzkreis")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(stored.value(0), "11XBK0000000001A");
    }

    #[test]
    fn a_transposed_check_character_is_refused_at_the_write() {
        // `11XBK0000000001A` is a valid EIC; `...1B` is the same sixteen
        // characters with the check character wrong, which is exactly what a
        // typo or a transposition upstream produces. A plain string column
        // would have stored it.
        let s = series(vec![quarter(
            datetime!(2026-07-20 00:00 UTC),
            "1.5",
            QualityFlag::Measured,
        )])
        .with_extra(
            "bilanzkreis",
            ScalarValue::Utf8(Some("11XBK0000000001B".to_string())),
        );
        let field = crate::config::eic_column("bilanzkreis", true);

        let err = to_record_batch_with(&[s], std::slice::from_ref(&field))
            .unwrap_err()
            .to_string();
        assert!(err.contains("bilanzkreis"), "{err}");
        assert!(err.contains("EIC"), "{err}");
    }

    #[test]
    fn a_null_in_a_nullable_checked_column_is_not_parsed() {
        // Nullable means "may be absent", and absent is not "the empty string is
        // not an EIC".
        let s = series(vec![quarter(
            datetime!(2026-07-20 00:00 UTC),
            "1.5",
            QualityFlag::Measured,
        )]);
        let field = crate::config::eic_column("bilanzkreis", true);
        let batch = to_record_batch_with(&[s], std::slice::from_ref(&field)).unwrap();
        assert!(batch.column_by_name("bilanzkreis").unwrap().is_null(0));
    }

    #[test]
    fn a_value_check_this_build_does_not_know_is_refused_rather_than_ignored() {
        // A column declared as checked and written unchecked is the one outcome
        // the declaration exists to rule out — so an unrecognised declaration
        // fails the write rather than degrading to a plain string column.
        let s = series(vec![quarter(
            datetime!(2026-07-20 00:00 UTC),
            "1.5",
            QualityFlag::Measured,
        )])
        .with_extra("odd", ScalarValue::Utf8(Some("anything".to_string())));
        let field = Field::new("odd", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                crate::config::VALUE_CHECK_KEY.to_string(),
                "IBAN".to_string(),
            )]),
        );

        let err = to_record_batch_with(&[s], std::slice::from_ref(&field))
            .unwrap_err()
            .to_string();
        assert!(err.contains("odd"), "{err}");
        assert!(err.contains("IBAN"), "{err}");

        // And a delivery that leaves the column null must not slip past it: the
        // declaration is wrong whatever this particular row holds.
        let absent = series(vec![quarter(
            datetime!(2026-07-20 00:00 UTC),
            "1.5",
            QualityFlag::Measured,
        )]);
        assert!(
            to_record_batch_with(&[absent], std::slice::from_ref(&field)).is_err(),
            "an unrecognised declaration is a configuration fault, not a row's"
        );
    }

    fn quarter(from: OffsetDateTime, kwh: &str, q: QualityFlag) -> MeterInterval {
        MeterInterval {
            from,
            to: from + time::Duration::minutes(15),
            value: kwh.parse().unwrap(),
            quality: q,
            obis_code: "1-0:1.8.0".parse().ok(),
        }
    }

    #[test]
    fn round_trip_preserves_a_simple_series() {
        let base = datetime!(2026-03-01 00:00 UTC);
        let input = series(vec![
            quarter(base, "1.234567", QualityFlag::Measured),
            quarter(
                base + time::Duration::minutes(15),
                "0.5",
                QualityFlag::Estimated,
            ),
        ]);

        let batch = to_record_batch(std::slice::from_ref(&input)).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let out = from_record_batch(&batch).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].series.malo_id, input.series.malo_id);
        assert_eq!(out[0].series.melo_id, input.series.melo_id);
        assert_eq!(out[0].series.resolution, input.series.resolution);
        assert_eq!(out[0].version, input.version);
        assert_eq!(out[0].recorded_at, input.recorded_at);
        assert_eq!(out[0].series.intervals.len(), 2);
    }

    /// The same batch with one column's values replaced — the shape a row that
    /// something other than this crate wrote arrives in.
    fn with_column(batch: &RecordBatch, name: &str, values: Vec<&str>) -> RecordBatch {
        let index = batch.schema().index_of(name).unwrap();
        let mut columns = batch.columns().to_vec();
        columns[index] = Arc::new(StringArray::from(values));
        RecordBatch::try_new(batch.schema(), columns).unwrap()
    }

    #[test]
    fn a_non_canonical_obis_code_is_refused_on_decode() {
        // `obis_code` is in the merge key, so this is where the canonical-form
        // rule matters most: `ObisCode`'s `FromStr` takes leading zeros,
        // surrounding whitespace and the redundant `*255` storage group, and
        // `Display` collapses all of them onto one spelling. Read back leniently,
        // a row stored under any of the others is a channel that a correction —
        // keyed on the canonical form — would never supersede. The hot tier's
        // `CHECK` refuses them; Iceberg has no constraints, so this is its half.
        let input = series(vec![quarter(
            datetime!(2026-03-01 00:00 UTC),
            "1.5",
            QualityFlag::Measured,
        )]);
        let batch = to_record_batch(&[input]).unwrap();
        assert!(
            from_record_batch(&batch).is_ok(),
            "the canonical form decodes"
        );

        for spelling in ["1-0:1.8.0*255", "01-0:1.8.0", " 1-0:1.8.0"] {
            let err = from_record_batch(&with_column(&batch, col::OBIS_CODE, vec![spelling]))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("canonical"),
                "{spelling:?} was accepted or refused for the wrong reason: {err}"
            );
        }
    }

    #[test]
    fn round_trip_preserves_decimal_precision_exactly() {
        // No f64 anywhere in the path.
        let base = datetime!(2026-03-01 00:00 UTC);
        for raw in ["0.000001", "123456789012.345678", "-0.000001", "0"] {
            let input = series(vec![quarter(base, raw, QualityFlag::Measured)]);
            let batch = to_record_batch(std::slice::from_ref(&input)).unwrap();
            let out = from_record_batch(&batch).unwrap();
            let got = out[0].series.intervals[0].value;
            assert_eq!(
                got,
                raw.parse::<Decimal>().unwrap(),
                "value {raw} did not round-trip exactly"
            );
        }
    }

    #[test]
    fn encoding_rejects_values_needing_more_precision_than_it_can_hold() {
        // Truncating silently would be the worst possible failure here.
        let base = datetime!(2026-03-01 00:00 UTC);
        let input = series(vec![quarter(base, "0.0000001", QualityFlag::Measured)]);
        assert!(matches!(
            to_record_batch(&[input]),
            Err(Error::Encode { .. })
        ));
    }

    #[test]
    fn round_trip_preserves_every_quality_flag() {
        // Substitute must never become Measured.
        let base = datetime!(2026-03-01 00:00 UTC);
        let flags = [
            QualityFlag::Measured,
            QualityFlag::Estimated,
            QualityFlag::Substituted,
            QualityFlag::Calculated,
            QualityFlag::Corrected,
            QualityFlag::Preliminary,
            QualityFlag::Faulty,
            QualityFlag::Unknown,
        ];
        let intervals: Vec<_> = flags
            .iter()
            .enumerate()
            .map(|(i, &q)| quarter(base + time::Duration::minutes(15 * i as i64), "1.0", q))
            .collect();

        let batch = to_record_batch(&[series(intervals)]).unwrap();
        let out = from_record_batch(&batch).unwrap();

        let got: Vec<_> = out[0].series.intervals.iter().map(|i| i.quality).collect();
        assert_eq!(got, flags);
    }

    #[test]
    fn round_trip_preserves_irregular_interval_boundaries() {
        // A 100-interval autumn day survives with its irregular
        // boundaries intact, because `to` is stored rather than recomputed as
        // `from + resolution`.
        let dst_back = datetime!(2026-10-25 00:00 UTC);
        let odd = MeterInterval {
            from: dst_back,
            to: dst_back + time::Duration::minutes(37), // deliberately not 15
            value: "2.5".parse().unwrap(),
            quality: QualityFlag::Measured,
            obis_code: "1-0:1.8.0".parse().ok(),
        };

        let batch = to_record_batch(&[series(vec![odd.clone()])]).unwrap();
        let out = from_record_batch(&batch).unwrap();

        assert_eq!(out[0].series.intervals[0].from, odd.from);
        assert_eq!(out[0].series.intervals[0].to, odd.to);
        assert_eq!(
            out[0].series.intervals[0].to - out[0].series.intervals[0].from,
            time::Duration::minutes(37)
        );
    }

    #[test]
    fn encoding_rejects_a_scope_that_does_not_cover_the_interval() {
        // The failure this prevents is silent: two versions of one reading in
        // different scopes cannot resolve against each other, so both survive
        // and every sum over them is inflated.
        let january = datetime!(2026-01-15 00:00 UTC);
        let mut input = series(vec![quarter(january, "1.0", QualityFlag::Measured)]);

        // Force the mistake this guards against: a scope keyed to the month the
        // correction was *delivered* rather than the month it describes.
        input.version = ScopedVersion::new(
            VersionScope::new("9900000000001", 2026, 7).unwrap(),
            Version::new(20_260_715_000_002).unwrap(),
        );
        assert!(matches!(
            to_record_batch(std::slice::from_ref(&input)),
            Err(Error::Encode { .. })
        ));

        // Derived from the interval, it encodes.
        input.version = ScopedVersion::new(
            VersionScope::for_interval("9900000000001", january, Sparte::Strom).unwrap(),
            Version::new(20_260_115_000_001).unwrap(),
        );
        assert!(to_record_batch(&[input]).is_ok());
    }

    #[test]
    fn a_correction_derives_the_same_scope_as_the_value_it_corrects() {
        // Delivered later, but scoped to the interval — so the two are
        // comparable and the correction can win.
        let interval = datetime!(2026-03-01 00:00 UTC);
        let scope = VersionScope::for_interval("9900000000001", interval, Sparte::Strom).unwrap();

        let original = ScopedVersion::new(scope.clone(), Version::new(20_260_301_000_001).unwrap());
        let correction = ScopedVersion::new(scope, Version::new(20_260_415_000_002).unwrap());

        assert!(correction.supersedes(&original).unwrap());
    }

    #[test]
    fn encoding_rejects_non_positive_intervals() {
        let base = datetime!(2026-03-01 00:00 UTC);
        let bad = MeterInterval {
            from: base,
            to: base,
            value: Decimal::ONE,
            quality: QualityFlag::Measured,
            obis_code: "1-0:1.8.0".parse().ok(),
        };
        assert!(to_record_batch(&[series(vec![bad])]).is_err());
    }

    #[test]
    fn round_trip_preserves_source_variant_payload() {
        let base = datetime!(2026-03-01 00:00 UTC);
        let mut input = series(vec![quarter(base, "1.0", QualityFlag::Measured)]);
        input.series.source = MeasurementSource::SmgwDirectPush {
            device_id: "SMGW-42".to_string(),
            session_id: "S-1".to_string(),
        };

        let batch = to_record_batch(std::slice::from_ref(&input)).unwrap();
        let out = from_record_batch(&batch).unwrap();

        match &out[0].series.source {
            MeasurementSource::SmgwDirectPush {
                device_id,
                session_id,
            } => {
                assert_eq!(device_id, "SMGW-42");
                assert_eq!(session_id, "S-1");
            }
            other => panic!("source variant lost: {other:?}"),
        }
    }

    #[test]
    fn two_deliveries_sharing_a_version_do_not_merge_into_one_source() {
        // The corruption a hand-listed run key produced. Two deliveries for one
        // measuring point that happen to agree on version and scope — one MSCONS,
        // one from the gateway — folded into a single series carrying whichever
        // source sorted first, for *all* the intervals. The numbers stayed right
        // and the audit trail lied, which is the harder failure to find.
        let base = datetime!(2026-03-01 00:00 UTC);

        let mut mscons = series(vec![quarter(base, "1.0", QualityFlag::Measured)]);
        mscons.series.source = MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
        };

        let mut gateway = series(vec![quarter(
            base + time::Duration::minutes(15),
            "2.0",
            QualityFlag::Measured,
        )]);
        gateway.series.source = MeasurementSource::SmgwDirectPush {
            device_id: "SMGW-42".to_string(),
            session_id: "S-1".to_string(),
        };
        // Same version, same scope, same measuring point: everything the old key
        // looked at agrees.
        assert_eq!(mscons.version, gateway.version);

        let batch = to_record_batch(&[mscons, gateway]).unwrap();
        let out = from_record_batch(&batch).unwrap();

        assert_eq!(out.len(), 2, "two deliveries, two series");
        assert!(matches!(
            out[0].series.source,
            MeasurementSource::Mscons { .. }
        ));
        assert!(matches!(
            out[1].series.source,
            MeasurementSource::SmgwDirectPush { .. }
        ));
    }

    #[test]
    fn a_decoded_series_is_one_channel() {
        // The MSCONS handbook defines a time series as one named series per OBIS
        // code. A run spanning two channels would put both under one series-level
        // `obis_code`, which claims a channel for values that are not on it.
        let base = datetime!(2026-03-01 00:00 UTC);
        let mut a = series(vec![quarter(base, "1.0", QualityFlag::Measured)]);
        a.series.obis_code = "1-0:1.8.0".parse().ok();
        a.series.intervals[0].obis_code = "1-0:1.8.0".parse().ok();

        let mut b = series(vec![quarter(base, "2.0", QualityFlag::Measured)]);
        b.series.obis_code = "1-0:2.8.0".parse().ok();
        b.series.intervals[0].obis_code = "1-0:2.8.0".parse().ok();

        let batch = to_record_batch(&[a, b]).unwrap();
        let out = from_record_batch(&batch).unwrap();

        assert_eq!(out.len(), 2);
        for decoded in &out {
            let channel = decoded.series.obis_code.expect("a channel");
            assert!(
                decoded
                    .series
                    .intervals
                    .iter()
                    .all(|i| i.obis_code == Some(channel)),
                "every interval must be on the channel the series names"
            );
        }
    }

    #[test]
    fn source_kind_column_is_the_payloads_own_tag() {
        // The two columns spell one discriminator, and the spelling is the
        // domain's. A hand-written `match` here produced "mscons" while the
        // payload in the next column said "MSCONS", so an external engine
        // filtering on the only spelling it could see matched nothing.
        let base = datetime!(2026-03-01 00:00 UTC);
        let input = series(vec![quarter(base, "1.0", QualityFlag::Measured)]);
        let batch = to_record_batch(&[input]).unwrap();

        let column = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };

        let kind = column(col::SOURCE_KIND);
        assert_eq!(kind, "MSCONS", "the tag `metering` itself writes");

        let payload: serde_json::Value = serde_json::from_str(&column(col::SOURCE_DETAIL)).unwrap();
        assert_eq!(
            payload.as_object().unwrap().keys().collect::<Vec<_>>(),
            vec![&kind],
            "the discriminant column must be the payload's own key"
        );
    }

    #[test]
    fn the_stored_json_representation_is_pinned() {
        // `source_detail` and `provenance` are **stored** JSON — in the hot
        // table's TEXT columns and in the cold tier's Parquet, which external
        // engines are meant to read directly. So their shape is not a wire
        // format that can be renegotiated between two versions of this crate: it
        // is on disk, under a retention measured in decades, and a change to it
        // is a change to data already written.
        //
        // Nothing else here would catch that. Every other test round-trips
        // through the *current* serde impl, so an upstream retag, a renamed
        // variant, or a `time` feature flipped on by some unrelated crate in the
        // graph would pass all of them and break every stored row. This is the
        // one that fails instead, here, with the reason attached.
        let (kind, detail) = encode_source(&source()).unwrap();
        assert_eq!(kind, "MSCONS");
        assert_eq!(
            detail,
            r#"{"MSCONS":{"message_ref":"MSG-1","pid":13005,"sender_mp_id":"9900000000001"}}"#,
            "the stored shape of MeasurementSource changed. Rows already in \
             source_detail are in the old shape and will not decode — this is a \
             stored-data break, not a serialisation detail"
        );

        // The variant that embeds a *second* upstream vocabulary, which is the
        // one this test exists for. `MeasurementSource::VirtualMeter` carries a
        // `VirtualMeterKind`, so `PV_SELF_CONSUMPTION` is a `metering` tag stored
        // inside a `metering` payload inside this column — and if it is ever
        // retagged, `source_kind` stays `VIRTUAL_METER` and agrees with itself
        // while the payload silently stops decoding. The discriminant check in
        // `decode_source` cannot see that; only this can.
        let (kind, detail) = encode_source(&MeasurementSource::VirtualMeter {
            rule: metering::aggregation_rule::VirtualMeterKind::PvSelfConsumption,
            source_ids: vec!["12345678905".to_string()],
        })
        .unwrap();
        assert_eq!(kind, "VIRTUAL_METER");
        assert_eq!(
            detail,
            r#"{"VIRTUAL_METER":{"rule":"PV_SELF_CONSUMPTION","source_ids":["12345678905"]}}"#,
            "a nested vocabulary in source_detail changed. Rows written for \
             virtual-meter series are in the old shape and no longer decode — and \
             source_kind still reads VIRTUAL_METER, so nothing else reports it"
        );

        // The audit trail, and its timestamp above all. `time`'s own serde impl
        // is feature-conditional — `occurred_at` lands as
        // `[2026,60,0,0,0,0,0,0,0]` wherever `serde-human-readable` is off — so
        // this pins `metering::wire`'s decision to write RFC 3339 instead.
        let entry = ProvenanceEntry {
            occurred_at: datetime!(2026-03-01 00:00 UTC),
            event_type: metering::measurement_series::ProvenanceEventType::Ingested,
            actor: "MSCONS".to_string(),
            note: None,
        };
        assert_eq!(
            encode_provenance(std::slice::from_ref(&entry)).unwrap(),
            r#"[{"occurred_at":"2026-03-01T00:00:00Z","event_type":"INGESTED","actor":"MSCONS","note":null}]"#,
            "the stored shape of a provenance entry changed. Rows already in the \
             provenance column are in the old shape — this is a stored-data \
             break, and an audit trail is the one column that must stay readable. \
             An `occurred_at` that is not RFC 3339 means `metering::wire` changed"
        );

        // And it decodes back, which is the half that matters on the read path.
        assert_eq!(
            decode_provenance(&encode_provenance(std::slice::from_ref(&entry)).unwrap()).unwrap(),
            vec![entry],
        );
    }

    #[test]
    fn a_provenance_trail_survives_every_event_type_and_a_note() {
        // Derived from `metering`'s own list, so an event type added upstream is
        // covered without an edit here — the same rule the source tags follow.
        use metering::measurement_series::ProvenanceEventType;

        let trail: Vec<ProvenanceEntry> = ProvenanceEventType::ALL
            .iter()
            .enumerate()
            .map(|(i, event_type)| ProvenanceEntry {
                occurred_at: datetime!(2026-03-01 00:00 UTC) + time::Duration::seconds(i as i64),
                event_type: *event_type,
                actor: format!("actor-{i}"),
                note: (i % 2 == 0).then(|| format!("note {i}")),
            })
            .collect();

        assert_eq!(
            decode_provenance(&encode_provenance(&trail).unwrap()).unwrap(),
            trail,
        );
        assert_eq!(encode_provenance(&[]).unwrap(), "[]");
        assert_eq!(decode_provenance("[]").unwrap(), Vec::new());
    }

    #[test]
    fn a_malformed_provenance_column_fails_rather_than_decoding_to_nothing() {
        // The input is a string read back out of storage, which a file this crate
        // did not write may set to anything. Each is a decode error naming the
        // column and what is wrong with it, not a serde error naming a Rust type.
        for bad in [
            r#"{"occurred_at":"2026-03-01T00:00:00Z"}"#,
            r#"[{"event_type":"INGESTED","actor":"a","note":null}]"#,
            r#"[{"occurred_at":"the first of March","event_type":"INGESTED","actor":"a","note":null}]"#,
            r#"[{"occurred_at":"2026-03-01T00:00:00Z","event_type":"ingested","actor":"a","note":null}]"#,
            r#"[{"occurred_at":"2026-03-01T00:00:00Z","event_type":"INGESTED","actor":7,"note":null}]"#,
        ] {
            let err = decode_provenance(bad).expect_err("{bad}");
            assert!(
                err.to_string().contains(col::PROVENANCE) || err.to_string().contains("json"),
                "{bad}: {err}"
            );
        }
    }

    #[test]
    fn every_source_variant_stores_the_tag_its_payload_carries() {
        // Derived rather than listed, so a variant added upstream is covered
        // here without an edit — which is the whole reason the `match` went.
        use metering::substitute::{SubstituteMethod, SubstitutionReason};

        let sources = [
            source(),
            MeasurementSource::SmgwDirectPush {
                device_id: "d".into(),
                session_id: "s".into(),
            },
            MeasurementSource::ManualEntry {
                operator_id: "op".into(),
                reason: "dispute".into(),
            },
            MeasurementSource::AutoSubstitute {
                method: SubstituteMethod::ZeroFill,
                reason: SubstitutionReason::GatewayCommFailure,
            },
            // The session-derived provenance `metering` 0.21 added. Nothing in
            // `encode_source` needed an edit for them — which is the property
            // this test exists to keep true — but the `Option` field is worth
            // exercising both ways, since a `None` that serialised to an absent
            // key rather than a null would be a stored-shape change.
            MeasurementSource::ChargeDetailRecord {
                cdr_id: "CDR-1".into(),
                evse_id: Some("DE*ABC*E1234*1".into()),
            },
            MeasurementSource::ClockAlignedMeterValue {
                transaction_id: "TX-9".into(),
                evse_id: None,
            },
            MeasurementSource::DeviceLog {
                device_id: "wallbox-7".into(),
                register: Some("1-0:1.8.0".into()),
            },
        ];

        for source in sources {
            let (kind, detail) = encode_source(&source).unwrap();
            let payload: serde_json::Value = serde_json::from_str(&detail).unwrap();
            assert_eq!(
                payload.as_object().unwrap().keys().next().unwrap(),
                &kind,
                "{source:?}"
            );
            assert_eq!(decode_source(&kind, Some(&detail)).unwrap(), source);
        }
    }

    #[test]
    fn a_non_canonical_code_is_refused_rather_than_normalised() {
        // `FromStr` is lenient on purpose — it trims, ignores case, and takes
        // `WÄRME` for `WAERME`. Right at an ingest boundary, wrong for storage:
        // these columns are `GROUP BY` keys, so two spellings of one commodity
        // are two rows in a completeness report. The hot tier's CHECK refuses
        // them; Iceberg has no constraints, so this is the read-side half.
        use metering::interval::Sparte;

        assert_eq!(
            decode_code::<Sparte>(col::SPARTE, "WAERME").unwrap(),
            Sparte::Waerme
        );
        for accepted_but_not_written in ["WÄRME", "waerme", " WAERME ", "Waerme"] {
            let err = decode_code::<Sparte>(col::SPARTE, accepted_but_not_written)
                .expect_err(accepted_but_not_written)
                .to_string();
            assert!(err.contains("WAERME"), "{err}");
            assert!(err.contains("canonical"), "{err}");
        }

        // The same for the other two coded columns.
        assert!(decode_code::<MeasurementUnit>(col::UNIT, "KWH").is_ok());
        assert!(decode_code::<MeasurementUnit>(col::UNIT, "kwh").is_err());
        assert!(decode_code::<QualityFlag>(col::QUALITY, "MEASURED").is_ok());
        assert!(decode_code::<QualityFlag>(col::QUALITY, "measured").is_err());

        // And an unknown code is still a plain parse failure.
        assert!(decode_code::<Sparte>(col::SPARTE, "FERNKAELTE").is_err());
    }

    #[test]
    fn every_code_this_crate_writes_decodes_back() {
        // The other half: the guard must not refuse anything the encoder emits.
        // Driven from `CODES` so a variant added upstream is covered without an
        // edit here.
        use metering::interval::Sparte;

        for code in Sparte::CODES {
            assert!(decode_code::<Sparte>(col::SPARTE, code).is_ok(), "{code}");
        }
        for code in MeasurementUnit::CODES {
            assert!(
                decode_code::<MeasurementUnit>(col::UNIT, code).is_ok(),
                "{code}"
            );
        }
        for code in QualityFlag::CODES {
            assert!(
                decode_code::<QualityFlag>(col::QUALITY, code).is_ok(),
                "{code}"
            );
        }
    }

    #[test]
    fn a_resolution_is_stored_under_one_iso8601_spelling() {
        use metering::resolution::IntervalResolution;

        // `PT900S` and `PT15M` are the same grid, and `IntervalResolution`
        // normalises the first to the second on parse — so without the check a
        // foreign writer could split one meter across two completeness rows.
        assert_eq!(
            decode_resolution("PT15M").unwrap(),
            IntervalResolution::QuarterHour
        );
        let err = decode_resolution("PT900S").unwrap_err().to_string();
        assert!(err.contains("PT15M"), "{err}");

        // Everything the encoder writes reads back.
        for r in [
            IntervalResolution::QuarterHour,
            IntervalResolution::Hour,
            IntervalResolution::Day,
            IntervalResolution::Month,
            IntervalResolution::from_seconds(60).unwrap(),
        ] {
            assert_eq!(decode_resolution(&r.to_iso8601()).unwrap(), r, "{r:?}");
        }
    }

    #[test]
    fn decoding_rejects_discriminant_payload_mismatch() {
        assert!(
            decode_source(
                "MANUAL_ENTRY",
                Some(&serde_json::to_string(&source()).unwrap())
            )
            .is_err()
        );
        // And the pre-0.5.0 spelling is a mismatch now, not a silent accept.
        assert!(decode_source("mscons", Some(&serde_json::to_string(&source()).unwrap())).is_err());
    }

    #[test]
    fn multiple_series_group_back_correctly() {
        let base = datetime!(2026-03-01 00:00 UTC);
        let mut a = series(vec![quarter(base, "1.0", QualityFlag::Measured)]);
        a.series.malo_id = malo("11111111115");
        let mut b = series(vec![
            quarter(base, "2.0", QualityFlag::Measured),
            quarter(
                base + time::Duration::minutes(15),
                "3.0",
                QualityFlag::Measured,
            ),
        ]);
        b.series.malo_id = malo("22222222220");

        let batch = to_record_batch(&[a, b]).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let out = from_record_batch(&batch).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].series.malo_id, malo("11111111115"));
        assert_eq!(out[0].series.intervals.len(), 1);
        assert_eq!(out[1].series.malo_id, malo("22222222220"));
        assert_eq!(out[1].series.intervals.len(), 2);
    }

    #[test]
    fn corrections_stay_separate_rows_rather_than_being_merged() {
        // Encoding must not resolve versions — that is the planner's job.
        let base = datetime!(2026-03-01 00:00 UTC);
        let mut v1 = series(vec![quarter(base, "1.0", QualityFlag::Measured)]);
        v1.version = ScopedVersion::new(scope_for(base), Version::new(20_260_701_000_001).unwrap());
        let mut v2 = series(vec![quarter(base, "9.0", QualityFlag::Corrected)]);
        v2.version = ScopedVersion::new(scope_for(base), Version::new(20_260_715_000_002).unwrap());

        let batch = to_record_batch(&[v1, v2]).unwrap();
        assert_eq!(
            batch.num_rows(),
            2,
            "correction must not overwrite the original"
        );

        let out = from_record_batch(&batch).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out[1].version.supersedes(&out[0].version).unwrap());
    }

    #[test]
    fn round_trip_preserves_provenance() {
        // Provenance is not derivable, so dropping it would be silent data loss
        // in a store that claims regulatory audit value.
        use metering::measurement_series::ProvenanceEventType;

        let base = datetime!(2026-03-01 00:00 UTC);
        let mut input = series(vec![quarter(base, "1.0", QualityFlag::Measured)]);
        input
            .series
            .record_event(ProvenanceEventType::Ingested, "test-actor", INGESTED_AT);
        let expected = input.series.provenance.len();
        assert!(expected > 0);

        let batch = to_record_batch(std::slice::from_ref(&input)).unwrap();
        let out = from_record_batch(&batch).unwrap();

        assert_eq!(out[0].series.provenance.len(), expected);
        let last = out[0].series.provenance.last().unwrap();
        assert_eq!(last.actor, "test-actor");
    }

    #[test]
    fn worst_quality_is_derived_not_stored() {
        // `metering` computes this from the intervals on demand. Persisting it
        // would create a second source of truth that can disagree with them.
        let base = datetime!(2026-03-01 00:00 UTC);
        let input = series(vec![
            quarter(base, "1.0", QualityFlag::Measured),
            quarter(
                base + time::Duration::minutes(15),
                "1.0",
                QualityFlag::Faulty,
            ),
        ]);

        let batch = to_record_batch(std::slice::from_ref(&input)).unwrap();
        assert!(
            batch.column_by_name("worst_quality").is_none(),
            "derived field must not be a column"
        );

        let out = from_record_batch(&batch).unwrap();
        assert_eq!(out[0].series.worst_quality(), input.series.worst_quality());
    }

    #[test]
    fn obis_canonicalisation_is_idempotent() {
        // The property storage depends on: canonicalising twice changes nothing,
        // so a value read back and rewritten keeps the same merge key.
        let once = canonical_obis("1-0:1.8.0").unwrap();
        assert_eq!(canonical_obis(&once).unwrap(), once);
    }

    #[test]
    fn obis_short_and_canonical_spellings_converge() {
        // These are the same channel and must produce the same stored value, or
        // a correction written one way cannot supersede a value written the
        // other way.
        assert_eq!(
            canonical_obis("1-0:1.8.0").unwrap(),
            canonical_obis("1-0:1.8.0*255").unwrap()
        );
    }

    #[test]
    fn obis_canonicalisation_rejects_nonsense() {
        assert!(canonical_obis("not-an-obis-code").is_err());
        assert!(canonical_obis("").is_err());
    }

    #[test]
    fn the_encoder_always_writes_the_canonical_form() {
        let base = datetime!(2026-03-01 00:00 UTC);
        let input = series(vec![quarter(base, "1.0", QualityFlag::Measured)]);
        let batch = to_record_batch(&[input]).unwrap();

        let stored = batch
            .column_by_name(col::OBIS_CODE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(stored, canonical_obis("1-0:1.8.0").unwrap());
    }

    /// The `balancing_day` column of a batch, as dates.
    fn stored_days(batch: &RecordBatch) -> Vec<time::Date> {
        let column = batch
            .column_by_name(col::BALANCING_DAY)
            .unwrap()
            .as_any()
            .downcast_ref::<crate::arrow::array::Date32Array>()
            .expect("date32 column");
        (0..column.len())
            .map(|i| schema::date_of(column.value(i)).expect("a stored day is in range"))
            .collect()
    }

    #[test]
    fn the_stored_balancing_day_is_the_calendars_answer_for_every_row() {
        // The answer to the one real objection to storing a derived value: it
        // can drift from what it was derived from. It cannot drift here as long
        // as this holds, so the property is asserted rather than argued —
        // against `metering`'s calendar, over every commodity, across a whole
        // DST weekend at quarter-hour grain.
        let mut at = datetime!(2026-10-24 00:00 UTC);
        let mut intervals = Vec::new();
        while at < datetime!(2026-10-26 00:00 UTC) {
            intervals.push(quarter(at, "1.0", QualityFlag::Measured));
            at += time::Duration::minutes(15);
        }

        for sparte in [Sparte::Strom, Sparte::Gas, Sparte::Waerme, Sparte::Wasser] {
            let mut s = series(intervals.clone());
            s.sparte = sparte;
            s.unit = sparte.billing_unit();
            // October, so the scope the encoder demands is October's.
            s.version = ScopedVersion::new(
                scope_for(intervals[0].from),
                Version::new(20_261_024_000_001).unwrap(),
            );

            let batch = to_record_batch(&[s]).unwrap();
            let got = stored_days(&batch);
            assert_eq!(got.len(), intervals.len());
            for (interval, day) in intervals.iter().zip(got) {
                assert_eq!(
                    day,
                    crate::planner::balancing_day(interval.from, sparte),
                    "{sparte} at {}",
                    interval.from
                );
            }
        }
    }

    #[test]
    fn a_gas_series_crossing_0600_local_stores_two_different_days() {
        // The column is per row, not per series — which is why it belongs to the
        // interval columns. A gas delivery spanning the 06:00 boundary carries
        // two Gastage, and a series-level value would book half of it wrongly.
        let before = datetime!(2026-07-15 03:45 UTC); // 05:45 local
        let after = datetime!(2026-07-15 04:00 UTC); // 06:00 local

        let mut s = series(vec![
            quarter(before, "1.0", QualityFlag::Measured),
            quarter(after, "2.0", QualityFlag::Measured),
        ]);
        s.sparte = Sparte::Gas;
        s.unit = Sparte::Gas.billing_unit();

        let batch = to_record_batch(&[s]).unwrap();
        assert_eq!(
            stored_days(&batch),
            vec![
                time::macros::date!(2026 - 07 - 14),
                time::macros::date!(2026 - 07 - 15)
            ],
            "the Gastag turns over at 06:00 local, mid-series"
        );

        // And the two rows still decode as **one** delivery: the day varies per
        // row, so it must not split the run.
        assert_eq!(from_record_batch(&batch).unwrap().len(), 1);
    }

    #[test]
    fn empty_input_produces_an_empty_batch_with_the_right_schema() {
        let batch = to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema::storage_schema(&[]));
        assert!(from_record_batch(&batch).unwrap().is_empty());
    }
}
