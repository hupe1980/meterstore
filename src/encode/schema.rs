//! The storage schema: `metering` types plus what tiering adds.
//!
//! Column names match `metering`'s field names deliberately, so the mapping
//! needs no lookup table.
//!
//! # The one deliberate deviation: `value`, not `value_kwh`
//!
//! [`MeterInterval::value_kwh`] is named for electricity, and the name is only
//! true for three of the four Sparten. Water is metered **and billed** in m³
//! ([`Sparte::billing_unit`]), and gas registers m³ of Betriebsvolumen before the
//! Brennwert conversion. A column called `value_kwh` holding a volume is a lie an
//! analyst reads straight past, so the column is `value` and the unit that
//! qualifies it is stored beside it.
//!
//! That is why [`col::SPARTE`] and [`col::UNIT`] are core columns rather than
//! deployment-declared extras: a number whose dimension is configuration is a
//! number no query can safely sum.
//!
//! [`MeterInterval::value_kwh`]: metering::interval::MeterInterval::value_kwh
//! [`Sparte::billing_unit`]: metering::Sparte::billing_unit
//!
//! Arrow types here are the *logical* ones. Dictionary encoding is applied by the
//! Parquet writer, not by using Arrow `Dictionary` types — Parquet's
//! `RLE_DICTIONARY` applies to plain string/int columns just as well, and keeping
//! Arrow arrays flat avoids dictionary-unification cost when concatenating
//! batches from different scan chunks.

use std::sync::Arc;

use crate::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

/// Decimal precision for `value`.
///
/// **18 is load-bearing, not arbitrary.** Parquet stores decimals of precision
/// ≤ 18 as `INT64`, which permits `DELTA_BINARY_PACKED` encoding; precision 19+
/// falls back to `FIXED_LEN_BYTE_ARRAY` and loses it.
pub const VALUE_PRECISION: u8 = 18;

/// Decimal scale for `value`. Six places is well beyond metering practice
/// and leaves headroom for gas conversion factors.
pub const VALUE_SCALE: i8 = 6;

/// Precision for `version`, matching MSCONS's ≥14-digit numeric label.
pub const VERSION_PRECISION: u8 = 20;

/// Scale for `version` — an integer.
pub const VERSION_SCALE: i8 = 0;

/// Timestamps are microsecond-precision UTC throughout.
///
/// Nanoseconds would be absurd for 15-minute intervals, and milliseconds would
/// not round-trip `time::OffsetDateTime` values that carry sub-millisecond
/// components from upstream systems.
pub const TS_UNIT: TimeUnit = TimeUnit::Microsecond;

/// Column names, in schema order.
pub mod col {
    /// 11-digit Marktlokations-ID.
    pub const MALO_ID: &str = "malo_id";
    /// 33-character Messlokations-ID, if known.
    pub const MELO_ID: &str = "melo_id";
    /// OBIS code in canonical `a-b:c.d.e*f` form.
    pub const OBIS_CODE: &str = "obis_code";
    /// Commodity, as `metering::Sparte`'s stable code: `STROM`, `GAS`,
    /// `WAERME`, `WASSER`.
    pub const SPARTE: &str = "sparte";
    /// Interval start, UTC, inclusive.
    pub const FROM: &str = "from";
    /// Interval end, UTC, exclusive. Stored, never derived.
    pub const TO: &str = "to";
    /// The measured quantity, in [`UNIT`].
    ///
    /// Not `value_kwh`: water is m³ and gas may be either side of the Brennwert
    /// conversion. See the module docs.
    pub const VALUE: &str = "value";
    /// The dimension of [`VALUE`], as `metering::MeasurementUnit`'s stable code:
    /// `KWH` or `M3`.
    ///
    /// Stored per row rather than derived from [`SPARTE`], because gas has two
    /// legitimate units — m³ as registered, kWh once converted — and which one a
    /// row holds is a fact about that row, not about the commodity.
    pub const UNIT: &str = "unit";
    /// Reading quality, as `metering`'s stable string code.
    pub const QUALITY: &str = "quality";
    /// Expected interval resolution.
    pub const RESOLUTION: &str = "resolution";
    /// Provenance discriminant.
    pub const SOURCE_KIND: &str = "source_kind";
    /// Provenance payload, JSON.
    pub const SOURCE_DETAIL: &str = "source_detail";
    /// Per-series audit trail, JSON array. Not derivable — must be stored or lost.
    pub const PROVENANCE: &str = "provenance";
    /// MSCONS correction version.
    pub const VERSION: &str = "version";
    /// Scope the version is comparable within.
    pub const VERSION_SCOPE: &str = "version_scope";
    /// Transaction time — when we learned the value.
    pub const RECORDED_AT: &str = "recorded_at";
}

/// The timestamp type used by all time columns.
pub fn timestamp_type() -> DataType {
    DataType::Timestamp(TS_UNIT, Some("UTC".into()))
}

/// Build the storage schema.
///
/// `extra` carries per-deployment attributes (Bilanzkreis, grid area, …) declared
/// in configuration rather than in a Rust type. They are appended
/// after the core columns so core field positions stay stable across deployments.
pub fn storage_schema(extra: &[Field]) -> SchemaRef {
    let mut fields = vec![
        Field::new(col::MALO_ID, DataType::Utf8, false),
        Field::new(col::MELO_ID, DataType::Utf8, true),
        Field::new(col::OBIS_CODE, DataType::Utf8, false),
        Field::new(col::SPARTE, DataType::Utf8, false),
        Field::new(col::FROM, timestamp_type(), false),
        Field::new(col::TO, timestamp_type(), false),
        Field::new(
            col::VALUE,
            DataType::Decimal128(VALUE_PRECISION, VALUE_SCALE),
            false,
        ),
        Field::new(col::UNIT, DataType::Utf8, false),
        Field::new(col::QUALITY, DataType::Utf8, false),
        Field::new(col::RESOLUTION, DataType::Utf8, true),
        Field::new(col::SOURCE_KIND, DataType::Utf8, false),
        Field::new(col::SOURCE_DETAIL, DataType::Utf8, true),
        Field::new(col::PROVENANCE, DataType::Utf8, true),
        Field::new(
            col::VERSION,
            DataType::Decimal128(VERSION_PRECISION, VERSION_SCALE),
            false,
        ),
        Field::new(col::VERSION_SCOPE, DataType::Utf8, false),
        Field::new(col::RECORDED_AT, timestamp_type(), false),
    ];
    fields.extend_from_slice(extra);
    Arc::new(Schema::new(fields))
}

/// Compile-time guard: precision > 18 forces Parquet to `FIXED_LEN_BYTE_ARRAY`
/// and silently loses `DELTA_BINARY_PACKED` on `value`.
const _: () = assert!(
    VALUE_PRECISION <= 18,
    "value precision above 18 loses DELTA_BINARY_PACKED encoding"
);

/// The columns that identify a row for merge resolution.
///
/// [`col::SPARTE`] is deliberately absent. A Marktlokation belongs to exactly one
/// commodity, so the Sparte is functionally determined by `malo_id` rather than
/// part of what distinguishes one reading from another. In the merge key it would
/// look harmless and would not be: a correction delivered with the Sparte spelled
/// differently — or defaulted by a pipeline that never set it — gets a different
/// key and silently fails to supersede the value it corrects. That is the same
/// failure OBIS canonicalisation exists to prevent.
pub const MERGE_KEY: [&str; 3] = [col::MALO_ID, col::OBIS_CODE, col::FROM];

/// Columns that should carry a Parquet bloom filter.
///
/// These are the equality-predicate columns in the dominant read pattern
/// ("one meter, one year"), where the bloom filter is the decisive pruning layer.
pub const BLOOM_FILTER_COLUMNS: [&str; 2] = [col::MALO_ID, col::OBIS_CODE];

/// Columns that benefit from `DELTA_BINARY_PACKED`: sorted timestamps and the
/// decimal value all store small increments rather than full-width values.
pub const DELTA_ENCODED_COLUMNS: [&str; 3] = [col::FROM, col::TO, col::VALUE];

/// The sort order written into the Parquet footer.
pub const SORT_COLUMNS: [&str; 2] = [col::MALO_ID, col::FROM];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_expected_core_columns() {
        let s = storage_schema(&[]);
        assert_eq!(s.fields().len(), 16);
        assert_eq!(s.field(0).name(), col::MALO_ID);
        assert_eq!(s.field(15).name(), col::RECORDED_AT);
    }

    #[test]
    fn the_value_column_is_never_dimensionless() {
        // The whole point of the rename. A quantity whose unit is implied by the
        // column name is only correct for Strom and Waerme; water is m³ and gas
        // is m³ until it is converted. Both columns are non-nullable so no row
        // can exist whose number has no dimension.
        let s = storage_schema(&[]);
        assert!(
            s.field_with_name("value_kwh").is_err(),
            "the old name is gone"
        );
        for name in [col::VALUE, col::UNIT, col::SPARTE] {
            let f = s.field_with_name(name).unwrap();
            assert!(!f.is_nullable(), "{name} must be present on every row");
        }
    }

    #[test]
    fn sparte_stays_out_of_the_merge_key() {
        // A MaLo is single-commodity, so the Sparte is functionally determined.
        // In the key, a correction that spelled it differently would fail to
        // supersede rather than fail loudly.
        assert!(!MERGE_KEY.contains(&col::SPARTE));
        assert!(!MERGE_KEY.contains(&col::UNIT));
    }

    #[test]
    fn extra_columns_append_without_shifting_core_positions() {
        let base = storage_schema(&[]);
        let extended = storage_schema(&[
            Field::new("bilanzkreis", DataType::Utf8, true),
            Field::new("netzgebiet", DataType::Utf8, true),
        ]);

        for (i, f) in base.fields().iter().enumerate() {
            assert_eq!(extended.field(i).name(), f.name());
            assert_eq!(extended.field(i).data_type(), f.data_type());
        }
        assert_eq!(extended.fields().len(), 18);
    }

    #[test]
    fn interval_bounds_are_both_present_and_non_nullable() {
        // `to` is stored, never derived. A nullable `to` would
        // invite recomputation as `from + resolution`, which is wrong across DST.
        let s = storage_schema(&[]);
        let to = s.field_with_name(col::TO).unwrap();
        assert!(!to.is_nullable());
        assert_eq!(to.data_type(), &timestamp_type());
    }

    #[test]
    fn timestamps_carry_utc_zone() {
        let s = storage_schema(&[]);
        for name in [col::FROM, col::TO, col::RECORDED_AT] {
            match s.field_with_name(name).unwrap().data_type() {
                DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                    assert_eq!(tz.as_ref(), "UTC", "{name} must be UTC-tagged");
                }
                other => panic!("{name} has unexpected type {other:?}"),
            }
        }
    }

    #[test]
    fn merge_key_columns_all_exist_and_are_non_nullable() {
        let s = storage_schema(&[]);
        for name in MERGE_KEY {
            let f = s.field_with_name(name).unwrap();
            assert!(!f.is_nullable(), "{name} is part of the merge key");
        }
    }

    #[test]
    fn tuning_column_lists_reference_real_columns() {
        let s = storage_schema(&[]);
        for name in BLOOM_FILTER_COLUMNS
            .iter()
            .chain(&DELTA_ENCODED_COLUMNS)
            .chain(&SORT_COLUMNS)
        {
            assert!(s.field_with_name(name).is_ok(), "{name} not in schema");
        }
    }
}
