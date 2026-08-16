//! What MeterStore adds to `metering`, end to end.
//!
//! Run with `cargo run --example encoding`.
//!
//! This exercises the layer that is implemented today: the storage encoding,
//! correction versioning, and the tiering boundary. The hot and cold stores are
//! not wired up yet, so nothing here touches a database.

use meterstore::encode::{StoredSeries, from_record_batch, to_record_batch};
use meterstore::watermark::{Tier, next_window};
use meterstore::{TieringWatermark, Version, VersionScope};

use metering::interval::MeterInterval;
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use metering::{QualityFlag, resolution::IntervalResolution};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

/// One day of 15-minute readings for a single measuring point.
fn day_of_readings(start: OffsetDateTime, version: u128) -> StoredSeries {
    let intervals: Vec<MeterInterval> = (0..96)
        .map(|i| {
            let from = start + Duration::minutes(15 * i);
            MeterInterval {
                from,
                to: from + Duration::minutes(15),
                // Deliberately six decimal places: this must survive exactly.
                value: format!("0.2{:05}", i).parse().unwrap(),
                quality: QualityFlag::Measured,
                obis_code: "1-0:1.8.0".parse().ok(),
            }
        })
        .collect();

    let mut series = MeasurementSeries::new(
        // Parsed, not asserted: `MaloId` verifies the check digit, so a
        // transposition in the identifier fails here rather than filing a day of
        // readings against a measuring point that does not exist.
        "12345678905".parse().expect("a valid MaLo-ID"),
        "1-0:1.8.0".parse().ok(),
        intervals,
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: Some("MSCONS-2026-07-20-001".to_string()),
            sender_mp_id: "9900000000001".to_string(),
        },
        // Ingestion time is injected, so this example is reproducible.
        datetime!(2026-07-27 06:00 UTC),
    );
    series.resolution = Some(IntervalResolution::QuarterHour);

    StoredSeries::new(
        series,
        meterstore::ScopedVersion::new(
            // Derived from the interval, never from the delivery month.
            VersionScope::for_interval("9900000000001", start).unwrap(),
            Version::new(version).unwrap(),
        ),
        datetime!(2026-07-27 06:00 UTC),
    )
}

fn main() -> meterstore::Result<()> {
    // ---- 1. Encode a day of metering data ------------------------------------
    let day = datetime!(2026-07-20 00:00 UTC);
    let original = day_of_readings(day, 20_260_721_000_001);

    let batch = to_record_batch(std::slice::from_ref(&original))?;
    println!(
        "encoded {} rows, {} columns",
        batch.num_rows(),
        batch.num_columns()
    );
    println!(
        "columns: {}",
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // ---- 2. Round-trip is exact ----------------------------------------------
    let decoded = from_record_batch(&batch)?;
    let back = &decoded[0];

    assert_eq!(back.series.intervals.len(), 96);
    assert_eq!(back.series.malo_id, original.series.malo_id);
    for (a, b) in original.series.intervals.iter().zip(&back.series.intervals) {
        assert_eq!(a.value, b.value, "decimal must round-trip exactly");
        assert_eq!(a.from, b.from);
        assert_eq!(a.to, b.to, "interval end is stored, never recomputed");
        assert_eq!(a.quality, b.quality);
    }
    println!("round-trip exact: 96 intervals, decimals and boundaries preserved");

    // `metering` owns this derivation — we recompute rather than store it.
    println!(
        "worst quality (derived by metering): {:?}",
        back.series.worst_quality()
    );

    // ---- 3. A correction supersedes, it does not overwrite --------------------
    let correction = day_of_readings(day, 20_260_725_000_002);
    let both = to_record_batch(&[original.clone(), correction.clone()])?;
    println!(
        "\noriginal + correction = {} rows (nothing was overwritten)",
        both.num_rows()
    );
    println!(
        "correction supersedes original: {}",
        correction.version.supersedes(&original.version)?
    );

    // Across scopes the comparison is refused rather than answered wrongly.
    let other_operator = meterstore::ScopedVersion::new(
        VersionScope::new("9900000000002", 2026, 7)?,
        Version::new(99_999_999_999_999)?,
    );
    match other_operator.supersedes(&original.version) {
        Err(e) => println!("cross-scope comparison correctly refused: {e}"),
        Ok(_) => unreachable!("versions must not compare across operators"),
    }

    // ---- 4. The tiering boundary ---------------------------------------------
    let watermark = TieringWatermark::new(datetime!(2026-07-20 00:00 UTC));
    println!("\nwatermark: {watermark}");
    for probe in [
        datetime!(2026-07-19 23:45 UTC),
        datetime!(2026-07-20 00:00 UTC),
        datetime!(2026-07-25 12:00 UTC),
    ] {
        let tier = watermark.tier_for(probe);
        let store = match tier {
            Tier::Cold => "Iceberg",
            Tier::Hot => "Postgres",
        };
        println!("  {probe} -> {tier:?} ({store})");
    }

    // ---- 5. Window selection respects the settlement lag ----------------------
    let now = datetime!(2026-07-30 03:00 UTC);
    let lag = Duration::days(7);
    match next_window(watermark, now, lag, Duration::DAY)? {
        Some(window) => {
            println!(
                "\nnext archival window: {} .. {}",
                window.from(),
                window.to()
            );
            println!("  advances watermark to {}", window.resulting_watermark());
            assert!(window.to() <= now - lag, "must not reach into the lag");
        }
        None => println!("\nnothing to archive yet"),
    }

    // With a longer lag the same call declines to archive a partial window.
    if next_window(watermark, now, Duration::days(30), Duration::DAY)?.is_none() {
        println!("  with a 30d settlement lag: correctly declines to archive");
    }

    Ok(())
}
