//! Parquet writer tuning for metering data.
//!
//! `iceberg-rust` accepts full `WriterProperties`, so every encoding decision is
//! ours. These are chosen for this data shape rather than left at library
//! defaults, and each one earns its place:
//!
//! * A **bloom filter on `malo_id`** is the decisive pruning layer for the
//!   dominant read ("one meter, one year"). Without it, a single-meter query
//!   reads every row group in the partition; with it, the question "is this
//!   meter in this row group?" is answered from a few KiB of metadata.
//! * **Delta encoding** on the timestamp and value columns stores small
//!   increments instead of full-width values. Interval starts are sorted and
//!   evenly spaced, which is close to the best case for it.
//! * **Page-level statistics** enable page pruning inside a surviving row group.
//!   Row-group-only statistics silently disable that layer.

use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use crate::encode::schema::{BLOOM_FILTER_COLUMNS, DELTA_ENCODED_COLUMNS, col};

/// Bloom filter false-positive rate.
///
/// 1% keeps the filter small while making a wrong row-group read rare. Lower
/// costs metadata bytes on every file for a marginal reduction in an already
/// cheap miss.
const BLOOM_FPP: f64 = 0.01;

/// Declared cardinality of the `obis_code` bloom filter.
///
/// OBIS codes come from a standardised code list, and a deployment reads a
/// handful of channels per measuring point — import, export, reactive, a few
/// tariff registers. 256 is generous for that and costs about 300 bytes per
/// file; the measuring-point count, which this column was previously sized with,
/// costs ~120 KiB for the same information.
const OBIS_CODE_NDV: u64 = 256;

/// Rows per data page.
///
/// Smaller pages make page-level pruning finer-grained at negligible metadata
/// cost. 20k rows is roughly two days of quarter-hour readings for one meter.
const PAGE_ROW_LIMIT: usize = 20_000;

/// Rows per row group.
///
/// **Set explicitly because it bounds memory, not only pruning granularity.**
/// A Parquet writer buffers a whole row group before it can flush one, so this
/// is the per-writer memory floor — and §10.1's cold layout runs a *fanout*
/// writer, holding one open writer per partition the archival window touches.
/// Peak is therefore `open partitions × this buffer`, where open partitions is
/// the number of distinct identity tuples (typically tenants) reporting that
/// day. Left at the `parquet` crate's default of 1 048 576 rows, that product is
/// an unstated dependency on a library constant, in the one place §18 gives a
/// memory budget.
///
/// 256k rows is ~2 700 meter-days of quarter-hour readings. It also sharpens
/// pruning: row-group `malo_id` min/max spans a quarter as many meters, so the
/// statistics eliminate more before the bloom filter is consulted.
const ROW_GROUP_ROW_LIMIT: usize = 256 * 1024;

/// Build writer properties for a metering data file.
///
/// `expected_malo_ids` sizes the bloom filter. Passing a number far below the
/// truth inflates the false-positive rate; far above wastes space. The archiver
/// knows the distinct count for the window it is writing, so it can be precise.
pub fn writer_properties(expected_malo_ids: u64) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("level 3 is valid"),
        ))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(PAGE_ROW_LIMIT)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROW_LIMIT))
        // Off by default: bloom filters are only worth their bytes on the
        // columns actually used for equality lookups.
        .set_bloom_filter_enabled(false);

    // Sized per column, not from one number. A bloom filter's size is driven by
    // its declared cardinality — at 1 % false positives, roughly 9.6 bits per
    // distinct value — so giving `obis_code` the *measuring point* count builds a
    // ~120 KiB filter for a column that holds a few dozen values. §10.2 has
    // always described it as low-ndv; the code sized both alike.
    for (name, ndv) in [
        (col::MALO_ID, expected_malo_ids.max(1)),
        (col::OBIS_CODE, OBIS_CODE_NDV),
    ] {
        debug_assert!(
            BLOOM_FILTER_COLUMNS.contains(&name),
            "a sized column must be one the schema declares filterable"
        );
        let path = ColumnPath::from(name);
        builder = builder
            .set_column_bloom_filter_enabled(path.clone(), true)
            .set_column_bloom_filter_fpp(path.clone(), BLOOM_FPP)
            .set_column_bloom_filter_ndv(path, ndv);
    }

    for name in DELTA_ENCODED_COLUMNS {
        let path = ColumnPath::from(name);
        builder = builder
            // Dictionary encoding takes precedence when enabled, so it must be
            // turned off for the columns where delta encoding is the point.
            .set_column_dictionary_enabled(path.clone(), false)
            .set_column_encoding(path, Encoding::DELTA_BINARY_PACKED);
    }

    // Low-cardinality identifiers and codes: dictionary encoding is near-free
    // and shrinks these columns to almost nothing.
    for name in [
        col::MALO_ID,
        col::MELO_ID,
        col::OBIS_CODE,
        col::QUALITY,
        col::RESOLUTION,
        col::SOURCE_KIND,
        col::VERSION_SCOPE,
    ] {
        builder = builder.set_column_dictionary_enabled(ColumnPath::from(name), true);
    }

    // Declare the sort order in the footer, so a reader can exploit it rather
    // than rediscovering it. The archival scan orders by a cursor whose *prefix*
    // is exactly these columns (`ScanSpec::cursor_columns`), which is why that
    // cursor appends the remaining key columns after `(malo_id, from)` instead
    // of interleaving them: a footer that declared an order the rows are not in
    // would be worse than no declaration at all.
    builder = builder.set_sorting_columns(Some(sorting_columns()));

    builder.build()
}

/// The declared sort order, as column indices into the storage schema.
///
/// Parquet identifies sorting columns by leaf position rather than by name, so
/// this is derived from the schema rather than written out — a hand-written
/// index would silently point at the wrong column the first time one is added.
fn sorting_columns() -> Vec<parquet::file::metadata::SortingColumn> {
    let schema = crate::encode::schema::storage_schema(&[]);
    crate::encode::schema::SORT_COLUMNS
        .iter()
        .filter_map(|name| schema.index_of(name).ok())
        .map(|index| parquet::file::metadata::SortingColumn {
            column_idx: index as i32,
            descending: false,
            // The sort columns are `malo_id` and `from`, both non-nullable, so
            // this never applies; declared explicitly rather than defaulted.
            nulls_first: false,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_row_group_size_is_ours_rather_than_the_library_default() {
        // It bounds the per-writer buffer, and §10.1's fanout writer holds one
        // per partition — so inheriting a library constant here would leave
        // §18's memory budget resting on a number this crate never chose.
        let props = writer_properties(1_000);
        assert_eq!(props.max_row_group_row_count(), Some(ROW_GROUP_ROW_LIMIT));
        // Compared against the default the writer would otherwise take, so this
        // fails if a future `parquet` release drops below our choice and the
        // comment above stops being true.
        let inherited = WriterProperties::builder()
            .build()
            .max_row_group_row_count()
            .unwrap_or(usize::MAX);
        assert!(
            ROW_GROUP_ROW_LIMIT < inherited,
            "the point is to be below the parquet default ({inherited}), not merely explicit"
        );
        assert!(
            ROW_GROUP_ROW_LIMIT > props.data_page_row_count_limit(),
            "a row group must hold more than one page or page pruning does nothing"
        );
    }

    #[test]
    fn bloom_filters_are_enabled_only_on_lookup_columns() {
        let props = writer_properties(1_000);

        for name in BLOOM_FILTER_COLUMNS {
            assert!(
                props
                    .bloom_filter_properties(&ColumnPath::from(name))
                    .is_some(),
                "{name} must carry a bloom filter"
            );
        }
        // The value column is never an equality predicate; a filter there is
        // pure overhead.
        assert!(
            props
                .bloom_filter_properties(&ColumnPath::from(col::VALUE))
                .is_none()
        );
    }

    #[test]
    fn bloom_filter_ndv_tracks_the_supplied_cardinality() {
        let small = writer_properties(10);
        let large = writer_properties(1_000_000);

        let path = ColumnPath::from(col::MALO_ID);
        let a = small.bloom_filter_properties(&path).unwrap().ndv;
        let b = large.bloom_filter_properties(&path).unwrap().ndv;
        assert!(a < b, "ndv must reflect the window's distinct meters");
    }

    #[test]
    fn the_channel_filter_is_sized_for_channels_not_for_meters() {
        // A bloom filter costs ~9.6 bits per declared distinct value at 1 % fpp,
        // so sizing `obis_code` with the measuring-point count builds a ~120 KiB
        // filter for a column holding a few dozen codes. Both columns were
        // previously given the same number.
        let props = writer_properties(1_000_000);
        let obis = props
            .bloom_filter_properties(&ColumnPath::from(col::OBIS_CODE))
            .unwrap()
            .ndv;
        let malo = props
            .bloom_filter_properties(&ColumnPath::from(col::MALO_ID))
            .unwrap()
            .ndv;

        assert_eq!(obis, OBIS_CODE_NDV);
        assert!(
            obis < malo,
            "a code list is not as wide as a meter population"
        );
    }

    #[test]
    fn bloom_filter_ndv_is_never_zero() {
        // An empty window would otherwise produce a degenerate filter.
        let props = writer_properties(0);
        let ndv = props
            .bloom_filter_properties(&ColumnPath::from(col::MALO_ID))
            .unwrap()
            .ndv;
        assert!(ndv >= 1);
    }

    #[test]
    fn timestamp_and_value_columns_use_delta_encoding() {
        let props = writer_properties(100);
        for name in DELTA_ENCODED_COLUMNS {
            let path = ColumnPath::from(name);
            assert_eq!(
                props.encoding(&path),
                Some(Encoding::DELTA_BINARY_PACKED),
                "{name} must be delta encoded"
            );
            assert!(
                !props.dictionary_enabled(&path),
                "{name} must not be dictionary encoded, or delta encoding is ignored"
            );
        }
    }

    #[test]
    fn identifier_columns_stay_dictionary_encoded() {
        let props = writer_properties(100);
        assert!(props.dictionary_enabled(&ColumnPath::from(col::MALO_ID)));
        assert!(props.dictionary_enabled(&ColumnPath::from(col::OBIS_CODE)));
    }

    #[test]
    fn page_level_statistics_are_enabled() {
        // Row-group-only statistics would silently disable page pruning.
        let props = writer_properties(100);
        assert_eq!(
            props.statistics_enabled(&ColumnPath::from(col::FROM)),
            EnabledStatistics::Page
        );
    }

    #[test]
    fn the_footer_declares_the_sort_order() {
        // §10.2: readers are entitled to trust this, so it has to be present
        // *and* has to match the order the archival scan actually produces.
        let props = writer_properties(100);
        let declared = props.sorting_columns().expect("a declared sort order");

        let schema = crate::encode::schema::storage_schema(&[]);
        let expected: Vec<i32> = crate::encode::schema::SORT_COLUMNS
            .iter()
            .map(|name| schema.index_of(name).unwrap() as i32)
            .collect();

        assert_eq!(
            declared.iter().map(|c| c.column_idx).collect::<Vec<_>>(),
            expected
        );
        assert!(declared.iter().all(|c| !c.descending), "ascending only");
    }

    #[test]
    fn the_declared_sort_order_is_a_prefix_of_the_scan_cursor() {
        // The archival scan pages by a cursor that must be unique per row; the
        // footer declares only `(malo_id, from)`. If the cursor did not *start*
        // with those, the rows would not arrive in the order the footer claims.
        let cursor = crate::tiering::store::ScanSpec::core().cursor_columns();
        for (i, name) in crate::encode::schema::SORT_COLUMNS.iter().enumerate() {
            assert_eq!(&cursor[i], name);
        }
    }

    #[test]
    fn compression_is_zstd() {
        let props = writer_properties(100);
        assert!(matches!(
            props.compression(&ColumnPath::from(col::VALUE)),
            Compression::ZSTD(_)
        ));
    }
}
