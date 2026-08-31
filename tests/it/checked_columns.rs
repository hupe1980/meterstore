//! `check = …` columns, end to end against a real PostgreSQL.
//!
//! The claim `config::checked_column` makes is deliberately two-sided, and only
//! a real server can show where each side stops: the hot table carries a `CHECK`
//! for the identifier's **shape**, and whatever arithmetic the scheme has — an
//! EIC check character, a MaLo check digit, both functions of the other
//! characters, which no regular expression expresses — is enforced on the write
//! path. A reader who believes the constraint is total would be wrong, so this
//! suite pins both halves and the seam between them.
//!
//! The regular expressions are the part worth running against the server rather
//! than reasoning about: PostgreSQL's POSIX engine is not PCRE, and
//! `[0-9A-Z-]{12}` leans on a trailing `-` inside a character class and on
//! bounded repetition — exactly where a dialect difference would hide.

// Real infrastructure, so the fixtures live behind `testkit` like every other
// suite that needs them.
#![cfg(feature = "testkit")]

use meterstore::config::{EicType, TableConfig, TimeModel, ValueCheck, checked_column};
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
const D20: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);
const D21: OffsetDateTime = datetime!(2026-07-21 00:00 UTC);

/// A valid Bilanzierungsgebiet EIC: German LIO `11`, `Y` function, `N` for the
/// TenneT Regelzone at position 4, and the check character the ENTSO-E
/// algorithm computes for the other fifteen.
const VALID: &str = "11YN000000000016";
/// The same sixteen characters with the check character wrong. Well-shaped —
/// PostgreSQL cannot tell — and not an EIC.
const WRONG_CHECK_CHARACTER: &str = "11YN000000000017";

/// The column each scheme is declared on — derived from the code, so one table
/// carries every declaration this build accepts and the DDL is exercised for
/// every pattern in one `CREATE TABLE`.
fn column_of(scheme: ValueCheck) -> String {
    format!("c_{}", scheme.as_str().to_lowercase().replace(':', "_"))
}

/// A value the domain type accepts, per scheme.
///
/// The EIC bodies share a shape and differ only at position 3, so the check
/// character is the only thing distinguishing them — which is what makes them
/// fixtures rather than a second implementation of the algorithm.
fn valid_of(scheme: ValueCheck) -> &'static str {
    match scheme {
        ValueCheck::Eic(None) => VALID,
        ValueCheck::Eic(Some(EicType::Party)) => "11XBK0000000001A",
        ValueCheck::Eic(Some(EicType::Area)) => "11YBK0000000001X",
        ValueCheck::Eic(Some(EicType::MeasurementPoint)) => "11ZBK0000000001J",
        ValueCheck::Eic(Some(EicType::ResourceObject)) => "11WBK0000000001O",
        ValueCheck::Eic(Some(EicType::TieLine)) => "11TBK0000000001T",
        ValueCheck::Eic(Some(EicType::Location)) => "11VBK00000000011",
        ValueCheck::Eic(Some(EicType::Substation)) => "11ABK0000000002Y",
        ValueCheck::Malo => "41373559241",
        ValueCheck::Melo => "DE00056266802AO6G56M11SN51G21M24S",
        ValueCheck::Bdew => "9900987654321",
    }
}

/// The same value as it is commonly *typed* — padded, and lower-cased where the
/// scheme canonicalises case.
fn as_typed(scheme: ValueCheck) -> String {
    match scheme {
        ValueCheck::Eic(_) | ValueCheck::Melo => {
            format!("  {}  ", valid_of(scheme).to_lowercase())
        }
        _ => format!("  {}  ", valid_of(scheme)),
    }
}

/// A table declaring every checked column this build knows.
fn declared() -> TableConfig {
    ValueCheck::ALL
        .into_iter()
        .fold(TableConfig::new(TABLE), |config, scheme| {
            config.attribute_column(checked_column(&column_of(scheme), scheme, true))
        })
}

async fn hot_with_checked_columns() -> (PostgresHot, TableConfig) {
    let url = meterstore::testkit::postgres::fresh_database()
        .await
        .expect("postgres");
    let pool = PgPool::connect(&url).await.expect("connect");
    let hot = PostgresHot::new(pool);

    let declared = declared();
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

/// One quarter-hour delivery, optionally setting one checked column.
fn delivery(column: Option<(&str, &str)>) -> StoredSeries {
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
    match column {
        None => stored,
        Some((name, code)) => stored.with_extra(name, ScalarValue::Utf8(Some(code.to_string()))),
    }
}

#[tokio::test]
async fn the_write_path_canonicalises_and_the_database_keeps_the_canonical_form() {
    let (hot, declared) = hot_with_checked_columns().await;
    let config = declared.build().expect("config");

    for scheme in ValueCheck::ALL {
        let column = column_of(scheme);
        // Padded, and lower-cased where the scheme canonicalises case — the
        // shape a CSV or a hand-edited mapping produces, and one that would be
        // a *second* merge key if the column were an identity one.
        let typed = as_typed(scheme);
        let batch = to_record_batch_with(
            &[delivery(Some((&column, &typed)))],
            &config.extra_columns(),
        )
        .unwrap_or_else(|e| panic!("a valid {scheme}, however it was typed: {e}"));

        hot.append(TABLE, &config.merge_key(), &[batch])
            .await
            .expect("append");

        let stored: Option<String> =
            sqlx::query_scalar(&format!(r#"SELECT "{column}" FROM "{TABLE}""#))
                .fetch_one(hot.pool())
                .await
                .expect("read back");
        assert_eq!(
            stored.as_deref(),
            Some(valid_of(scheme)),
            "the stored value is the domain type's canonical spelling, so a column \
             in the merge key cannot hold one identifier under two keys"
        );

        sqlx::query(&format!(r#"DELETE FROM "{TABLE}""#))
            .execute(hot.pool())
            .await
            .expect("clear");
    }
}

#[tokio::test]
async fn a_transposition_the_scheme_can_detect_never_reaches_the_database() {
    let (_hot, declared) = hot_with_checked_columns().await;
    let config = declared.build().expect("config");

    // The two schemes that carry arithmetic. A transposition here is caught
    // while the delivery that carried it is still in hand.
    for (scheme, transposed) in [
        (ValueCheck::Eic(None), WRONG_CHECK_CHARACTER),
        (ValueCheck::Malo, "41373559214"),
    ] {
        let column = column_of(scheme);
        let err = to_record_batch_with(
            &[delivery(Some((&column, transposed)))],
            &config.extra_columns(),
        )
        .expect_err("the check character is part of the code")
        .to_string();
        assert!(err.contains(&column), "{err}");
    }

    // A well-formed EIC of the wrong object type is the write path's other
    // refusal, and it is a different fault: nothing about the code is malformed,
    // it is simply not what this column holds. Both of these parse as EICs.
    let party = ValueCheck::Eic(Some(EicType::Party));
    let column = column_of(party);
    let err = to_record_batch_with(
        &[delivery(Some((
            &column,
            valid_of(ValueCheck::Eic(Some(EicType::Area))),
        )))],
        &config.extra_columns(),
    )
    .expect_err("an area code is not a party code")
    .to_string();
    assert!(err.contains(&column), "{err}");
    assert!(err.contains("X (Party)"), "{err}");
    assert!(err.contains("Y (Area or Domain)"), "{err}");

    // And the honest edge. A Marktpartner-ID's thirteenth digit is *not*
    // checked — BDEW §2.3 carves out GS1-issued GLNs — so a transposed one is
    // accepted here and by the database. Asserted rather than left to be
    // discovered, because "checked column" invites the opposite assumption.
    to_record_batch_with(
        &[delivery(Some((
            &column_of(ValueCheck::Bdew),
            "9900987654312",
        )))],
        &config.extra_columns(),
    )
    .expect("thirteen digits is the whole of the rule a Marktpartner-ID has");
}

#[tokio::test]
async fn the_database_carries_the_shape_and_says_so_in_its_own_regex_engine() {
    let (hot, _) = hot_with_checked_columns().await;

    let party = ValueCheck::Eic(Some(EicType::Party));

    // Every code below is either something the domain type accepts — and so
    // must reach the database — or something it refuses on shape, which the
    // server has to refuse too.
    let cases: &[(ValueCheck, &str, bool)] = &[
        (ValueCheck::Eic(None), VALID, true),
        // Well-shaped and not an EIC. This is the seam: the database cannot
        // tell, and the write path is what does.
        (ValueCheck::Eic(None), WRONG_CHECK_CHARACTER, true),
        (ValueCheck::Eic(None), "10X---ENTSOE---L", true),
        (ValueCheck::Eic(None), "11YN00000000001", false), // fifteen characters
        (ValueCheck::Eic(None), "11yn000000000016", false), // the column holds what was stored
        (ValueCheck::Eic(None), "11-N000000000016", false), // position 3 is an object-type letter
        (ValueCheck::Eic(None), "11YN00000000001-", false), // §5.2 forbids `-` as a check character
        (ValueCheck::Eic(None), "11YN0000000000!6", false),
        // A valid EIC carrying a type letter the manual does not list. The
        // unrefined column takes it — the list is ENTSO-E's to extend — and it
        // is the row a strict downstream parser will reject.
        (ValueCheck::Eic(None), "11QBK0000000001Y", true),
        // A code whose type letter this build does not list is refused by every
        // refinement, which is the point: `Q` is not `X`.
        (party, "11QBK0000000001Y", false),
        (ValueCheck::Malo, "41373559241", true),
        (ValueCheck::Malo, "41373559214", true), // the check digit is the write path's half
        (ValueCheck::Malo, "04137355924", false), // no Vergabestelle issues a leading zero
        (ValueCheck::Malo, "4137355924", false), // ten digits
        (ValueCheck::Malo, "4137355924A", false),
        (ValueCheck::Melo, "DE00056266802AO6G56M11SN51G21M24S", true),
        (ValueCheck::Melo, "de00056266802ao6g56m11sn51g21m24s", false),
        (ValueCheck::Melo, "DEX0056266802AO6G56M11SN51G21M24S", false), // 3–8 are digits
        (ValueCheck::Melo, "DE00056266802AO6G56M11SN51G21M24", false),  // 32 characters
        (ValueCheck::Bdew, "9900987654321", true),
        (ValueCheck::Bdew, "9900987654320", true), // a GS1 GLN fails BDEW's procedure and is valid
        (ValueCheck::Bdew, "990098765432", false), // twelve digits
        (ValueCheck::Bdew, "99009876543210", false), // fourteen
        (ValueCheck::Bdew, "99009876543A1", false),
    ];

    // The refinement's own cases, built from `EicType::ALL` rather than listed:
    // every declared type must accept its own code and refuse every other one,
    // and a letter added upstream is covered without an edit here. This is the
    // half of "is this the right kind of EIC" a regular expression *can* carry.
    let mut refinements: Vec<(ValueCheck, &str, bool)> = Vec::new();
    for want in EicType::ALL {
        for held in EicType::ALL {
            refinements.push((
                ValueCheck::Eic(Some(want)),
                valid_of(ValueCheck::Eic(Some(held))),
                want == held,
            ));
        }
    }
    let cases: Vec<(ValueCheck, &str, bool)> = cases.iter().copied().chain(refinements).collect();

    for scheme in ValueCheck::ALL {
        let column = column_of(scheme);
        // The constraint as the *server* holds it, not a copy of the string this
        // crate rendered — so a change to either side has to be a deliberate one.
        let definition: String = sqlx::query_scalar(
            "SELECT pg_get_constraintdef(c.oid)
               FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
              WHERE t.relname = $1 AND c.conname = $2",
        )
        .bind(TABLE)
        .bind(format!("{column}_shape"))
        .fetch_one(hot.pool())
        .await
        .unwrap_or_else(|e| panic!("{scheme} declares no shape constraint: {e}"));
        assert!(definition.contains('~'), "{definition}");

        let predicate = definition
            .trim_start_matches("CHECK ((")
            .trim_end_matches("))")
            .split_once('~')
            .map(|(_, pattern)| format!("~{pattern}"))
            .expect("the constraint is a regex match");

        // Every declaration this build accepts is exercised against the server,
        // or a pattern could be added and never evaluated by the engine that
        // runs it — which is the one place a dialect difference would show.
        let mut ran = 0;
        for (_, code, want) in cases.iter().filter(|(s, _, _)| *s == scheme) {
            let matched: bool = sqlx::query_scalar(&format!("SELECT $1 {predicate}"))
                .bind(code)
                .fetch_one(hot.pool())
                .await
                .expect("evaluate the deployed pattern");
            assert_eq!(matched, *want, "{scheme}: {code}");
            ran += 1;
        }
        assert!(ran > 0, "{scheme} has no case in this table");
    }
}

#[tokio::test]
async fn the_database_refuses_a_badly_shaped_value_from_a_writer_that_is_not_this_crate() {
    // The constraint's whole purpose: MeterStore's own write path already
    // parses, so the CHECK only ever fires on a row something else wrote — and
    // that is exactly when it is worth having.
    let (hot, _) = hot_with_checked_columns().await;

    let insert = |column: String, code: &'static str| {
        let pool = hot.pool().clone();
        async move {
            sqlx::query(&format!(
                r#"INSERT INTO "{TABLE}"
                   (malo_id, obis_code, sparte, "from", "to", value, unit, quality,
                    source_kind, source_detail, version, version_scope, recorded_at,
                    balancing_day, "{column}")
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

    for (column, bad) in [
        (column_of(ValueCheck::Eic(None)), "not-an-eic"),
        // A perfectly good EIC, in a column that declares the other kind. This
        // one the database catches on its own — the object type is shape, not
        // arithmetic, which is what makes declaring it worth doing.
        (
            column_of(ValueCheck::Eic(Some(EicType::Party))),
            "11YBK0000000001X",
        ),
        (column_of(ValueCheck::Malo), "04137355924"),
        (
            column_of(ValueCheck::Melo),
            "de00056266802ao6g56m11sn51g21m24s",
        ),
        (column_of(ValueCheck::Bdew), "99009876543"),
    ] {
        assert!(
            insert(column.clone(), bad).await.is_err(),
            "a badly shaped {column} must fail at the DB layer: {bad}"
        );
    }

    // And the honest edge, asserted rather than left to be discovered: the
    // database accepts a well-shaped code whose check character is wrong.
    insert(column_of(ValueCheck::Eic(None)), WRONG_CHECK_CHARACTER)
        .await
        .expect("the shape check cannot see the check character");
}

#[tokio::test]
async fn a_value_check_this_build_does_not_know_is_refused_by_both_halves() {
    // A column declared as checked and written unchecked is the one outcome the
    // declaration exists to rule out. The DDL renders a pattern nothing matches
    // and the write path refuses the declaration outright, so the two halves
    // cannot disagree about whether a column is constrained.
    //
    // `EIC:Q` is the case worth having beside `IBAN`: a refinement this build
    // cannot enforce must not degrade to the unrefined check it can.
    use datafusion::arrow::datatypes::{DataType, Field};

    for declaration in ["IBAN", "EIC:Q"] {
        let url = meterstore::testkit::postgres::fresh_database()
            .await
            .expect("postgres");
        let pool = PgPool::connect(&url).await.expect("connect");
        let hot = PostgresHot::new(pool);

        let odd = Field::new("odd", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                meterstore::config::VALUE_CHECK_KEY.to_string(),
                declaration.to_string(),
            )]),
        );
        let config = TableConfig::new(TABLE)
            .attribute_column(odd)
            .build()
            .expect("config");
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

        let err = to_record_batch_with(
            &[delivery(Some(("odd", "11XBK0000000001A")))],
            &config.extra_columns(),
        )
        .expect_err("an unrecognised declaration must not degrade to a plain column")
        .to_string();
        assert!(err.contains("odd"), "{err}");
        assert!(err.contains(declaration), "{err}");

        // The database's half: a pattern nothing matches, so no writer gets a
        // value into the column either.
        let accepted: bool = sqlx::query_scalar(
            "SELECT '11XBK0000000001A' ~ (
                 SELECT substring(pg_get_constraintdef(c.oid) from '~ ''(.*)''::text')
                   FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
                  WHERE t.relname = $1 AND c.conname = 'odd_shape')",
        )
        .bind(TABLE)
        .fetch_one(hot.pool())
        .await
        .expect("the unknown declaration still rendered a constraint");
        assert!(
            !accepted,
            "an unrecognised value check ({declaration}) must not leave the column \
             unconstrained"
        );
    }
}
