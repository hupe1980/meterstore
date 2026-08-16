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

/// Convert an [`OffsetDateTime`] to microseconds since the Unix epoch.
fn to_micros(ts: OffsetDateTime) -> Result<i64> {
    i64::try_from(ts.unix_timestamp_nanos() / 1_000)
        .map_err(|_| Error::encode("timestamp", format!("{ts} out of microsecond range")))
}

/// Days between the Unix epoch and a date, the `Date32` encoding.
fn to_date32(date: time::Date) -> i32 {
    (date - time::Date::from_ordinal_date(1970, 1).expect("epoch is a valid date")).whole_days()
        as i32
}

/// Convert microseconds since the Unix epoch back to an [`OffsetDateTime`].
fn from_micros(micros: i64) -> Result<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
        .map_err(|e| Error::decode("timestamp", format!("{micros}: {e}")))
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
fn encode_source(source: &MeasurementSource) -> Result<(String, String)> {
    let kind = match source {
        MeasurementSource::Mscons { .. } => "mscons",
        MeasurementSource::SmgwDirectPush { .. } => "smgw_direct_push",
        MeasurementSource::ManualEntry { .. } => "manual_entry",
        MeasurementSource::AutoSubstitute { .. } => "auto_substitute",
        MeasurementSource::RetroactiveCorrection { .. } => "retroactive_correction",
        MeasurementSource::VirtualMeter { .. } => "virtual_meter",
        MeasurementSource::RedispatchImport { .. } => "redispatch_import",
    };
    Ok((kind.to_string(), serde_json::to_string(source)?))
}

/// Decode a source from its discriminant and payload.
///
/// The discriminant is authoritative for filtering; the payload is authoritative
/// for reconstruction. A mismatch means the row was written by an incompatible
/// writer, so it is an error rather than a silent preference for one column.
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
pub fn to_record_batch_with(stored: &[StoredSeries], extra: &[Field]) -> Result<RecordBatch> {
    let rows: usize = stored.iter().map(|s| s.series.intervals.len()).sum();

    let mut malo: Vec<String> = Vec::with_capacity(rows);
    let mut melo: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut obis = Vec::with_capacity(rows);
    let mut sparte = Vec::with_capacity(rows);
    let mut from = Vec::with_capacity(rows);
    let mut to = Vec::with_capacity(rows);
    let mut value = Vec::with_capacity(rows);
    let mut unit = Vec::with_capacity(rows);
    let mut quality = Vec::with_capacity(rows);
    let mut resolution: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut source_kind = Vec::with_capacity(rows);
    let mut source_detail: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut provenance: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut version = Vec::with_capacity(rows);
    let mut version_scope = Vec::with_capacity(rows);
    let mut recorded_at = Vec::with_capacity(rows);
    let mut balancing_day = Vec::with_capacity(rows);

    for s in stored {
        check_unit(s.sparte, s.unit, &s.series.malo_id)?;
        let (kind, detail) = encode_source(&s.series.source)?;
        let res = s.series.resolution.map(|r| r.to_iso8601());
        let prov = serde_json::to_string(&s.series.provenance)?;
        let recorded = to_micros(s.recorded_at)?;
        let ver = s.version.version().to_i128();
        let scope = s.version.scope().as_str().to_string();
        // Rendered once per series rather than once per interval: the identifiers
        // are the same for every row of a delivery, and a 96-row day would
        // otherwise format the same eleven digits ninety-six times.
        let malo_str = s.series.malo_id.to_string();
        let melo_str = s.series.melo_id.as_ref().map(MeloId::to_string);

        for interval in &s.series.intervals {
            // A scope keyed to the delivery month rather than the interval's
            // would leave two versions of one reading unable to supersede each
            // other, doubling any sum over it. Nothing downstream can detect
            // that, so it is rejected here.
            if !s.version.scope().covers(interval.from) {
                return Err(Error::encode(
                    col::VERSION_SCOPE,
                    format!(
                        "interval starting {} is not in scope {} — the scope must be \
                         derived from the interval's local month, not the delivery month",
                        interval.from,
                        s.version.scope()
                    ),
                ));
            }

            if interval.to <= interval.from {
                return Err(Error::encode(
                    col::TO,
                    format!(
                        "interval end {} is not after start {} for {}",
                        interval.to, interval.from, s.series.malo_id
                    ),
                ));
            }

            malo.push(malo_str.clone());
            melo.push(melo_str.clone());
            obis.push(obis_for(&s.series, interval)?.to_string());
            sparte.push(s.sparte.as_str());
            from.push(to_micros(interval.from)?);
            to.push(to_micros(interval.to)?);
            value.push(to_scaled_i128(interval.value)?);
            unit.push(s.unit.as_str());
            quality.push(interval.quality.as_str());
            resolution.push(res.clone());
            source_kind.push(kind.clone());
            source_detail.push(Some(detail.clone()));
            provenance.push(Some(prov.clone()));
            version.push(ver);
            version_scope.push(scope.clone());
            recorded_at.push(recorded);
            // The one place the balancing day is derived. Asked of the calendar
            // per row rather than per series, because a series may span the
            // 06:00 Gastag boundary — or a midnight — and every row of it does
            // not share a day.
            balancing_day.push(to_date32(crate::planner::balancing_day(
                interval.from,
                s.sparte,
            )));
        }
    }

    let tz: Arc<str> = "UTC".into();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(malo)),
        Arc::new(StringArray::from(melo)),
        Arc::new(StringArray::from(obis)),
        Arc::new(StringArray::from(sparte)),
        Arc::new(TimestampMicrosecondArray::from(from).with_timezone(tz.clone())),
        Arc::new(TimestampMicrosecondArray::from(to).with_timezone(tz.clone())),
        Arc::new(
            Decimal128Array::from(value).with_precision_and_scale(VALUE_PRECISION, VALUE_SCALE)?,
        ),
        Arc::new(StringArray::from(unit)),
        Arc::new(StringArray::from(quality)),
        Arc::new(StringArray::from(resolution)),
        Arc::new(StringArray::from(source_kind)),
        Arc::new(StringArray::from(source_detail)),
        Arc::new(StringArray::from(provenance)),
        Arc::new(
            Decimal128Array::from(version)
                .with_precision_and_scale(VERSION_PRECISION, VERSION_SCALE)?,
        ),
        Arc::new(StringArray::from(version_scope)),
        Arc::new(TimestampMicrosecondArray::from(recorded_at).with_timezone(tz)),
        Arc::new(crate::arrow::array::Date32Array::from(balancing_day)),
    ];

    let mut columns = columns;
    for field in extra {
        let mut values = Vec::with_capacity(rows);
        for s in stored {
            // A series with no intervals contributes no rows, so it has nothing
            // to label. Demanding a value for it would make a batch fail on a
            // column that would never have been written — and the core columns
            // above already skip it silently, so the two must agree.
            if s.series.intervals.is_empty() {
                continue;
            }
            // A nullable column may simply be absent: a Bilanzkreis that is not
            // yet assigned, or a measuring point with no known occupant, is an
            // ordinary state rather than an encoding failure. Identity columns
            // are non-nullable by validation, so they still have to be supplied.
            let value = match s.extra.get(field.name()) {
                Some(value) => value.clone(),
                None if field.is_nullable() => ScalarValue::try_from(field.data_type())?,
                None => {
                    return Err(Error::encode(
                        field.name(),
                        "declared column has no value on this series, and the \
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
            // An explicit null for a non-nullable column would otherwise fail
            // deep inside Arrow with a message that does not name the column.
            if value.is_null() && !field.is_nullable() {
                return Err(Error::encode(
                    field.name(),
                    "value is null but the column is not nullable".to_string(),
                ));
            }
            for _ in 0..s.series.intervals.len() {
                values.push(value.clone());
            }
        }
        // `iter_to_array` refuses an empty iterator, so a zero-row batch has to
        // build its extra columns another way. A zero-row batch is ordinary — a
        // delivery whose series carry no intervals — and it must produce the same
        // shape as any other, or a table with deployment columns would fail on
        // exactly the input a table without them accepts.
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
fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
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
/// An earlier key listed six columns by hand and omitted `source_kind`,
/// `source_detail`, `provenance`, `resolution` and `recorded_at`. Two deliveries
/// that happened to share a version and scope — one MSCONS, one SMGW — folded
/// into one series carrying the first one's origin for all of it.
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
            let res = if resolution.is_null(i) {
                None
            } else {
                Some(resolution.value(i).parse().map_err(|e| {
                    Error::decode(col::RESOLUTION, format!("{:?}: {e}", resolution.value(i)))
                })?)
            };

            let prov: Vec<ProvenanceEntry> = if provenance.is_null(i) {
                Vec::new()
            } else {
                serde_json::from_str(provenance.value(i))?
            };

            let mut extra = BTreeMap::new();
            for name in &extra_names {
                let column = batch
                    .column_by_name(name)
                    .ok_or_else(|| Error::decode(name, "column vanished"))?;
                extra.insert(name.clone(), ScalarValue::try_from_array(column, i)?);
            }

            let sparte: Sparte = sparte
                .value(i)
                .parse()
                .map_err(|e| Error::decode(col::SPARTE, format!("{:?}: {e}", sparte.value(i))))?;
            let unit = MeasurementUnit::parse(unit.value(i)).ok_or_else(|| {
                Error::decode(
                    col::UNIT,
                    format!(
                        "{:?} is not one of {:?}",
                        unit.value(i),
                        MeasurementUnit::CODES
                    ),
                )
            })?;
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
                    obis_code: obis.value(i).parse().ok(),
                    resolution: res,
                    source,
                    intervals: Vec::new(),
                    provenance: prov,
                },
                version: ScopedVersion::new(
                    VersionScope::parse(version_scope.value(i))?,
                    Version::from_i128(version.value(i))?,
                ),
                recorded_at: from_micros(recorded_at.value(i))?,
            });
        }

        let series = out.last_mut().expect("pushed above");
        series.series.intervals.push(MeterInterval {
            from: from_micros(from.value(i))?,
            to: from_micros(to.value(i))?,
            value: from_scaled_i128(value.value(i)),
            quality: quality
                .value(i)
                .parse()
                .map_err(|e| Error::decode(col::QUALITY, format!("{:?}: {e}", quality.value(i))))?,
            obis_code: Some(
                obis.value(i).parse().map_err(|e| {
                    Error::decode(col::OBIS_CODE, format!("{:?}: {e}", obis.value(i)))
                })?,
            ),
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
    fn distinct_malo_ids_of_nothing_is_zero() {
        assert_eq!(distinct_malo_ids(&[]), 0);
    }

    const INGESTED_AT: OffsetDateTime = datetime!(2026-07-27 06:00 UTC);

    /// The scope an interval belongs to — always derived, never guessed.
    fn scope_for(interval: OffsetDateTime) -> VersionScope {
        VersionScope::for_interval("9900000000001", interval).unwrap()
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
            sender_mp_id: "9900000000001".to_string(),
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
            VersionScope::for_interval("9900000000001", january).unwrap(),
            Version::new(20_260_115_000_001).unwrap(),
        );
        assert!(to_record_batch(&[input]).is_ok());
    }

    #[test]
    fn a_correction_derives_the_same_scope_as_the_value_it_corrects() {
        // Delivered later, but scoped to the interval — so the two are
        // comparable and the correction can win.
        let interval = datetime!(2026-03-01 00:00 UTC);
        let scope = VersionScope::for_interval("9900000000001", interval).unwrap();

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
            sender_mp_id: "99".to_string(),
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
    fn source_kind_column_is_filterable_and_matches_payload() {
        let base = datetime!(2026-03-01 00:00 UTC);
        let input = series(vec![quarter(base, "1.0", QualityFlag::Measured)]);
        let batch = to_record_batch(&[input]).unwrap();

        let kinds = batch
            .column_by_name(col::SOURCE_KIND)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(kinds.value(0), "mscons");
    }

    #[test]
    fn decoding_rejects_discriminant_payload_mismatch() {
        assert!(
            decode_source(
                "manual_entry",
                Some(&serde_json::to_string(&source()).unwrap())
            )
            .is_err()
        );
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
        let epoch = time::Date::from_ordinal_date(1970, 1).unwrap();
        let column = batch
            .column_by_name(col::BALANCING_DAY)
            .unwrap()
            .as_any()
            .downcast_ref::<crate::arrow::array::Date32Array>()
            .expect("date32 column");
        (0..column.len())
            .map(|i| epoch + time::Duration::days(i64::from(column.value(i))))
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
