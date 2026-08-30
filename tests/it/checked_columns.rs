//! `check = "EIC"` columns, end to end against a real PostgreSQL.
//!
//! The claim `config::eic_column` makes is deliberately two-sided, and only a
//! real server can show where each side stops: the hot table carries a `CHECK`
//! for the code's **shape**, and the **check character** — arithmetic over the
//! other fifteen, which no regular expression expresses — is enforced on the
//! write path. A reader who believes the constraint is total would be wrong,
//! so this suite pins both halves and the seam between them.
//!
//! The regular expression is the part worth running against the server rather
//! than reasoning about: PostgreSQL's POSIX engine is not PCRE, and
//! `[0-9A-Z-]{12}` leans on a trailing `-` inside a character class and on
//! bounded repetition — exactly where a dialect difference would hide.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them.
#![cfg(feature = "testkit")]

use meterstore::config::{TableConfig, TimeModel, eic_column};
use meterstore::encode::{StoredSeries, to_record_batch_with};
use meterstore::hot::PostgresHot;
use meterstore::tiering::store::HotStore;
use meterstore::{ScopedVersion, Version, VersionScope};

use datafusion::common::ScalarValue;
use metering::interval::{MeterInterval, QualityFlag, Sparte};
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use sqlx::PgPool;
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const TABLE: &str = "readings_versions";
const COLUMN: &str = "bilanzierungsgebiet";
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);

/// A valid Bilanzierungsgebiet EIC: German LIO `11`, `Y` function, `N` for the
/// TenneT Regelzone at position 4, and the check character the ENTSO-E
/// algorithm computes for the other fifteen.
const VALID: &str = "11YN000000000016";
/// The same sixteen characters with the check character wrong. Well-shaped —
/// PostgreSQL cannot tell — and not an EIC.
const WRONG_CHECK_CHARACTER: &str = "11YN000000000017";

async fn hot_with_a_checked_column() -> (PostgresHot, TableConfig) {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let pool = PgPool::connect(&url).await.expect("connect");
    let hot = PostgresHot::new(pool);

    let declared = TableConfig::new(TABLE).attribute_column(eic_column(COLUMN, true));
    let config = declared.clone().build().expect("config");
    hot.create_tables(
        TABLE,
        &config.merge_key(),
        &config.extra_columns(),
        TimeModel::Interval,
    )
    .await
    .expect("hot table");
    hot.ensure_partitions(TABLE, D20, D21, Duration::DAY)
        .await
        .expect("partitions");

    (hot, declared)
}

/// One quarter-hour delivery, optionally naming a Bilanzierungsgebiet.
fn delivery(bilanzierungsgebiet: Option<&str>) -> StoredSeries {
    let obis = "1-0:1.8.0".parse().ok();
    let series = MeasurementSeries::new(
        "12345678905".parse().expect("MaLo-ID"),
        obis,
        vec![MeterInterval {
            from: D20,
            to: D20 + Duration::minutes(15),
            value: "1.5".parse().expect("decimal"),
            quality: QualityFlag::Measured,
            obis_code: obis,
        }],
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "9900000000001".parse().expect("Marktpartner-ID"),
        },
        datetime!(2026-07-27 06:00 UTC),
    );

    let stored = StoredSeries::new(
        series,
        ScopedVersion::new(
            VersionScope::for_interval("9900000000001", D20, Sparte::Strom).expect("scope"),
            Version::new(20_260_727_000_001).expect("version"),
        ),
        datetime!(2026-07-27 06:00 UTC),
    );
    match bilanzierungsgebiet {
        None => stored,
        Some(code) => stored.with_extra(COLUMN, ScalarValue::Utf8(Some(code.to_string()))),
    }
}

#[tokio::test]
async fn the_write_path_canonicalises_and_the_database_keeps_the_canonical_form() {
    let (hot, declared) = hot_with_a_checked_column().await;
    let config = declared.build().expect("config");

    // Lowercase and padded — the shape a CSV or a hand-edited mapping produces.
    let batch = to_record_batch_with(
        &[delivery(Some("  11yn000000000016  "))],
        &config.extra_columns(),
    )
    .expect("a valid EIC, however it was typed");

    hot.append(TABLE, &config.merge_key(), &[batch])
        .await
        .expect("append");

    let stored: Option<String> =
        sqlx::query_scalar(&format!(r#"SELECT "{COLUMN}" FROM "{TABLE}""#))
            .fetch_one(hot.pool())
            .await
            .expect("read back");
    assert_eq!(
        stored.as_deref(),
        Some(VALID),
        "the stored value is the domain type's canonical spelling, so a column \
         in the merge key cannot hold one identifier under two keys"
    );
}

#[tokio::test]
async fn a_wrong_check_character_never_reaches_the_database() {
    let (_hot, declared) = hot_with_a_checked_column().await;
    let config = declared.build().expect("config");

    let err = to_record_batch_with(
        &[delivery(Some(WRONG_CHECK_CHARACTER))],
        &config.extra_columns(),
    )
    .expect_err("the check character is part of the code")
    .to_string();
    assert!(err.contains(COLUMN), "{err}");
    assert!(err.contains("EIC"), "{err}");
}

#[tokio::test]
async fn the_database_carries_the_shape_and_says_so_in_its_own_regex_engine() {
    let (hot, _) = hot_with_a_checked_column().await;

    // The constraint as the *server* holds it, not a copy of the string this
    // crate rendered — so a change to either side has to be a deliberate one.
    let definition: String = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(c.oid)
           FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
          WHERE t.relname = $1 AND c.conname = $2",
    )
    .bind(TABLE)
    .bind(format!("{COLUMN}_shape"))
    .fetch_one(hot.pool())
    .await
    .expect("the checked column declares a shape constraint");
    assert!(definition.contains("~"), "{definition}");

    // PostgreSQL's POSIX engine, on the pattern it is actually holding.
    for (code, want) in [
        (VALID, true),
        // Well-shaped and not an EIC. This is the seam: the database cannot
        // tell, and the write path is what does.
        (WRONG_CHECK_CHARACTER, true),
        ("10X---ENTSOE---L", true),
        ("11YN00000000001", false),  // fifteen characters
        ("11yn000000000016", false), // the column holds what was stored
        ("11-N000000000016", false), // position 3 is an object-type letter
        ("11YN00000000001-", false), // §5.2 forbids `-` as a check character
        ("11YN0000000000!6", false),
    ] {
        let matched: bool = sqlx::query_scalar(&format!(
            "SELECT $1 {}",
            definition
                .trim_start_matches("CHECK ((")
                .trim_end_matches("))")
                .split_once('~')
                .map(|(_, pattern)| format!("~{pattern}"))
                .expect("the constraint is a regex match"),
        ))
        .bind(code)
        .fetch_one(hot.pool())
        .await
        .expect("evaluate the deployed pattern");
        assert_eq!(matched, want, "{code}");
    }
}

#[tokio::test]
async fn the_database_refuses_a_badly_shaped_value_from_a_writer_that_is_not_this_crate() {
    // The constraint's whole purpose: MeterStore's own write path already
    // parses, so the CHECK only ever fires on a row something else wrote — and
    // that is exactly when it is worth having.
    let (hot, _) = hot_with_a_checked_column().await;

    let insert = |code: &'static str| {
        let pool = hot.pool().clone();
        async move {
            sqlx::query(&format!(
                r#"INSERT INTO "{TABLE}"
                   (malo_id, obis_code, sparte, "from", "to", value, unit, quality,
                    source_kind, source_detail, version, version_scope, recorded_at,
                    balancing_day, "{COLUMN}")
                   VALUES ('12345678905','1-0:1.8.0','STROM',$1,$2,1.5,'KWH','MEASURED',
                           'MSCONS','{{}}',$3,'9900000000001:2026-07',$4,DATE '2026-07-20',$5)"#
            ))
            .bind(D20)
            .bind(D20 + Duration::minutes(15))
            .bind(rust_decimal::Decimal::new(20_260_727_000_001, 0))
            .bind(datetime!(2026-07-27 06:00 UTC))
            .bind(code)
            .execute(&pool)
            .await
        }
    };

    assert!(
        insert("not-an-eic").await.is_err(),
        "a badly shaped code must fail at the DB layer"
    );

    // And the honest edge, asserted rather than left to be discovered: the
    // database accepts a well-shaped code whose check character is wrong.
    insert(WRONG_CHECK_CHARACTER)
        .await
        .expect("the shape check cannot see the check character");
}
