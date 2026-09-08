//! The storage schema: `metering` types plus what tiering adds.
//!
//! Column names match `metering`'s field names deliberately, so the mapping
//! needs no lookup table.
//!
//! # Why the quantity column is `value`, and carries its own dimension
//!
//! A field named for kilowatt-hours is true for three of the four Sparten and
//! false for the fourth. Water is metered **and billed** in m³
//! ([`Sparte::billing_unit`]), and gas registers m³ of Betriebsvolumen before the
//! Brennwert conversion. A column called `value_kwh` holding a volume is a lie an
//! analyst reads straight past, so the column is [`col::VALUE`] and the unit that
//! qualifies it is stored beside it.
//!
//! That is why [`col::SPARTE`] and [`col::UNIT`] are core columns rather than
//! deployment-declared extras: a number whose dimension is configuration is a
//! number no query can safely sum.
//!
//! [`metering`] names the field the same way, so the mapping stays a rename-free
//! one.
//!
//! [`Sparte::billing_unit`]: metering::Sparte::billing_unit
//!
//! # Why `balancing_day` is stored, when derived values are not
//!
//! This crate refuses to persist anything it can compute: `worst_quality` is
//! derived on demand precisely so there is no second copy to disagree with the
//! intervals. [`col::BALANCING_DAY`] is the one deliberate exception, and the
//! reason is not convenience.
//!
//! The rule is: the Berlin calendar day for electricity, heat and water, and the
//! **Gastag** — 06:00 to 06:00 local — for gas. Deriving it needs a zone
//! conversion and a **wall-clock** (not absolute) six-hour shift, and SQL
//! dialects differ on exactly that, so no single published expression is right
//! everywhere.
//!
//! Reading the Iceberg files directly is the *intended* access path, so a rule an
//! external engine cannot express is a rule that will be got wrong — silently,
//! in a daily total that still looks plausible. Storing the answer removes the
//! derivation from every reader instead of publishing four spellings of it and
//! hoping.
//!
//! The usual objection — a second source of truth that can drift — is answered by
//! there being exactly one writer: [`to_record_batch_with`] derives it from
//! `metering`'s calendar, and nothing else sets it. It also earns its 4 bytes
//! back: a day's rows share one value, so it dictionary-encodes to nearly
//! nothing, and it is the natural grouping key for every daily aggregate.
//!
//! [`to_record_batch_with`]: super::to_record_batch_with
//!
//! Arrow types here are the *logical* ones. Dictionary encoding is applied by the
//! Parquet writer, not by using Arrow `Dictionary` types — Parquet's
//! `RLE_DICTIONARY` applies to plain string/int columns just as well, and keeping
//! Arrow arrays flat avoids dictionary-unification cost when concatenating
//! batches from different scan chunks.

use std::sync::Arc;

use crate::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use crate::error::{Error, Result};

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
    ///
    /// **Null on a point table.** An instant has no end, and `to IS NULL` is
    /// what tells a reader — including one holding only the Parquet — that
    /// [`VALUE`] is a cumulative register reading rather than energy over a
    /// span. See [`TimeModel`](crate::config::TimeModel).
    pub const TO: &str = "to";
    /// The measured quantity, in [`UNIT`].
    ///
    /// Never named for a unit: water is m³ and gas may be either side of the
    /// Brennwert conversion. See the module docs.
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
    /// The **balancing day** this reading is booked on, as a local `Date32`.
    ///
    /// The Berlin calendar day for electricity, heat and water; the *Gastag* —
    /// 06:00 to 06:00 local — for gas. Derived from [`FROM`] and [`SPARTE`] by
    /// `metering`'s calendar at encode time, and stored because **no external
    /// engine can derive it portably**. See the module documentation.
    pub const BALANCING_DAY: &str = "balancing_day";
}

/// The timestamp type used by all time columns.
pub fn timestamp_type() -> DataType {
    DataType::Timestamp(TS_UNIT, Some("UTC".into()))
}

/// An instant in the unit the storage schema uses.
///
/// The single conversion into the storage encoding. Every layer that puts a
/// timestamp into a row, a bind parameter or a predicate goes through this, so
/// none of them can round or wrap differently from the others.
///
/// # Infallible, and what makes it so
///
/// `time` without the `large-dates` feature bounds `OffsetDateTime` at ±9999
/// years — ±3.2 × 10^17 microseconds, comfortably inside `i64` — so every value
/// that can be constructed converts, and a `Result` here would be an error arm no
/// caller could produce and every caller would have to handle.
///
/// That is an assumption about a *dependency's* feature set, and feature
/// unification means something else in the graph could enable `large-dates`
/// without a line changing here. It is therefore asserted rather than assumed:
/// `the_storage_encoding_holds_every_representable_instant` fails at this crate's
/// boundary rather than as a wrapped timestamp inside a committed Parquet file.
#[must_use]
pub fn micros(instant: time::OffsetDateTime) -> i64 {
    // The saturating fallback is unreachable while the test below passes; it is
    // here so that a graph which does enable `large-dates` clamps to a wrong-but-
    // bounded instant instead of wrapping to a plausible one in the far past.
    i64::try_from(instant.unix_timestamp_nanos() / 1_000).unwrap_or(i64::MAX)
}

/// The inverse of [`micros`].
///
/// Fallible where [`micros`] is not, and the asymmetry is the point: the input
/// here is a number read back out of storage, which a file this crate did not
/// write may set to anything at all. Failing beats panicking, and beats an
/// instant in the year 300 000.
pub fn instant(micros: i64) -> Result<time::OffsetDateTime> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
        .map_err(|e| Error::decode("timestamp", format!("{micros}: {e}")))
}

/// A local date in the `Date32` encoding — days since the Unix epoch.
#[must_use]
pub fn date32(date: time::Date) -> i32 {
    (date - EPOCH).whole_days() as i32
}

/// The inverse of [`date32`], failing rather than panicking on a value out of
/// range — for the reason [`instant`] gives.
pub fn date_of(days: i32) -> Result<time::Date> {
    EPOCH
        .checked_add(time::Duration::days(i64::from(days)))
        .ok_or_else(|| Error::decode(col::BALANCING_DAY, format!("{days} is out of range")))
}

/// A timestamp literal in the exact type the storage schema declares.
///
/// Both halves matter: the *unit*, so the engine compares like for like rather
/// than coercing, and the `"UTC"` zone spelling, which has to be the schema's own
/// or a comparison against the column carries a cast.
#[must_use]
pub fn timestamp_scalar(instant: time::OffsetDateTime) -> datafusion::common::ScalarValue {
    datafusion::common::ScalarValue::TimestampMicrosecond(Some(micros(instant)), Some("UTC".into()))
}

/// The Unix epoch as a date — the origin `Date32` counts from.
const EPOCH: time::Date = time::macros::date!(1970 - 01 - 01);

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
        // Nullable, because a point table has no span end. An interval table
        // still refuses a null with a `NOT NULL` in its own DDL and with the
        // encoder's own check, so nothing loosens for a Lastgang — what changes
        // is that one schema can describe both, which is what lets a
        // Zählerstandsgang reuse every part of the tiering machinery.
        Field::new(col::TO, timestamp_type(), true),
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
        Field::new(col::BALANCING_DAY, DataType::Date32, false),
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
    fn the_storage_encoding_holds_every_representable_instant() {
        // What makes `micros` infallible. `time` without `large-dates` bounds
        // `OffsetDateTime` at ±9999 years, which fits `i64` microseconds with
        // three orders of magnitude to spare — so there is no error arm a caller
        // could ever produce.
        //
        // That is an assumption about a *dependency's feature set*, and feature
        // unification means something else in the graph can enable `large-dates`
        // without a line changing here. This is where that stops being silent:
        // the alternative is a timestamp saturating or wrapping inside a
        // committed Parquet file, discovered by whoever reconciles it.
        use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

        for date in [Date::MIN, Date::MAX] {
            for at in [Time::MIDNIGHT, Time::MAX] {
                let instant = PrimitiveDateTime::new(date, at).assume_utc();
                let nanos = instant.unix_timestamp_nanos() / 1_000;
                assert!(
                    i64::try_from(nanos).is_ok(),
                    "{instant} does not fit the storage encoding — has `large-dates` \
                     been enabled somewhere in the graph?"
                );
                assert_eq!(micros(instant), nanos as i64);
            }
        }

        // And the inverse round-trips over the range the store actually holds.
        let mut at = time::macros::datetime!(1970-01-01 00:00 UTC);
        while at < time::macros::datetime!(2100-01-01 00:00 UTC) {
            assert_eq!(instant(micros(at)).unwrap(), at);
            at += time::Duration::days(97);
        }
        assert_eq!(
            instant(micros(OffsetDateTime::UNIX_EPOCH)).unwrap(),
            OffsetDateTime::UNIX_EPOCH
        );
    }

    #[test]
    fn a_date_round_trips_through_the_date32_encoding() {
        use time::macros::date;

        for d in [
            date!(1970 - 01 - 01),
            date!(2026 - 03 - 29),
            date!(2026 - 10 - 25),
            date!(2100 - 12 - 31),
        ] {
            assert_eq!(date_of(date32(d)).unwrap(), d);
        }
        assert_eq!(date32(date!(1970 - 01 - 01)), 0);
        // Read back from storage, so a value a file this crate did not write may
        // set to anything must fail rather than panic.
        assert!(date_of(i32::MAX).is_err());
        assert!(date_of(i32::MIN).is_err());
        assert!(instant(i64::MAX).is_err());
    }

    #[test]
    fn a_timestamp_literal_carries_the_columns_exact_type() {
        // Both halves were got wrong independently before this was one function:
        // the unit, so the engine compares like for like, and the zone spelling,
        // which has to be the schema's own or the comparison carries a cast.
        let scalar = timestamp_scalar(time::macros::datetime!(2026-07-20 00:00 UTC));
        assert_eq!(
            scalar.data_type(),
            *storage_schema(&[])
                .field_with_name(col::FROM)
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn schema_has_expected_core_columns() {
        let s = storage_schema(&[]);
        assert_eq!(s.fields().len(), 17);
        assert_eq!(s.field(0).name(), col::MALO_ID);
        assert_eq!(s.field(15).name(), col::RECORDED_AT);
        assert_eq!(s.field(16).name(), col::BALANCING_DAY);
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
        assert_eq!(extended.fields().len(), 19);
    }

    #[test]
    fn the_interval_end_is_stored_and_never_derived() {
        // `to` is stored rather than computed as `from + resolution`, which is
        // wrong across a DST transition. It is *nullable* only so one schema can
        // also describe a point table, where an instant has no end — an interval
        // table's own DDL keeps `NOT NULL`, and the encoder refuses a null there
        // before it reaches the database.
        let s = storage_schema(&[]);
        let to = s.field_with_name(col::TO).unwrap();
        assert_eq!(to.data_type(), &timestamp_type());
        assert!(to.is_nullable(), "a point row has no end");

        // `from` is not: every row is somewhere on the timeline, and it is what
        // decides the tier.
        assert!(!s.field_with_name(col::FROM).unwrap().is_nullable());
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
