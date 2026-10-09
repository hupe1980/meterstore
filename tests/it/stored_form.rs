//! Every row a published release wrote still decodes.
//!
//! The byte-for-byte literals in `encode` pin what *this* build writes. They
//! cannot protect what an *earlier* build wrote: when this crate adopts an
//! upstream rename, the literals are edited to the new spelling, every test
//! passes, and the rows already on disk — under a retention measured in decades
//! — are what stops decoding. Nothing else here reads a row this build did not
//! write.
//!
//! So each release leaves behind the stored spelling of every vocabulary column
//! it writes, in `tests/stored-form/`, and every later build must decode all of
//! them through the public read path. A row leaves a file only when a release
//! breaks it on purpose; the file's `broken_by` names that release, and the
//! changelog records it as a stored-data break.
//!
//! The corpus lags the build between releases, by design: what a release writes
//! is recorded when it is tagged — `release.yml` refuses a tag without it — and
//! the generator below enumerates every source variant exhaustively.
//!
//! Only the columns that carry a vocabulary are recorded — the two JSON
//! payloads and the codes. The rest of the row (identifiers, instants, the
//! decimal) is built fresh by the current encoder, because its spelling is this
//! crate's own fixed-width encoding rather than somebody else's vocabulary, and
//! because the schema may gain a column without old rows being wrong.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use metering::Resolution;
use metering::series::interval::{MeterInterval, QualityFlag, Sparte};
use metering::series::reading::MeterReading;
use meterstore::arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use meterstore::delivery::{
    MeasurementSeries, MeasurementSource, ProvenanceEntry, ProvenanceEventType,
};
use meterstore::encode::schema::col;
use meterstore::encode::{
    StoredReadings, StoredSeries, from_record_batch, readings_from_record_batch,
    readings_to_record_batch, to_record_batch,
};
use rust_decimal::Decimal;
use time::OffsetDateTime;
use time::macros::datetime;

/// The columns whose spelling is a vocabulary, in the order a file lists them.
const COLUMNS: [&str; 8] = [
    col::SOURCE_KIND,
    col::SOURCE_DETAIL,
    col::PROVENANCE,
    col::SPARTE,
    col::UNIT,
    col::QUALITY,
    col::RESOLUTION,
    col::VERSION_SCOPE,
];

/// Mid-month and midday, so the electricity and gas Bilanzierungsmonat agree and
/// a row may carry either commodity under one version scope.
const AT: OffsetDateTime = datetime!(2026-07-15 12:00 UTC);

type Row = BTreeMap<String, Option<String>>;

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/stored-form")
}

// --- the fixtures one release writes ----------------------------------------

fn mscons() -> MeasurementSource {
    MeasurementSource::Mscons {
        pid: 13_005,
        message_ref: Some("MSG-1".into()),
        sender_mp_id: "9900000000003".parse().expect("a valid Marktpartner-ID"),
    }
}

/// One value of every source variant, and every code a variant nests.
///
/// The `match` is what keeps the list complete: a variant added to
/// `MeasurementSource` fails to compile here rather than leaving a shape of
/// `source_detail` no corpus has recorded.
fn every_source() -> Vec<MeasurementSource> {
    use metering::vee::substitute::{Method, SubstitutionReason};
    use meterstore::delivery::VirtualMeterKind;

    let mut all = vec![
        mscons(),
        MeasurementSource::Mscons {
            pid: 13_008,
            message_ref: None,
            sender_mp_id: "9900000000003".parse().unwrap(),
        },
        MeasurementSource::SmgwDirectPush {
            device_id: "d".into(),
            session_id: "s".into(),
        },
        MeasurementSource::ManualEntry {
            operator_id: "op".into(),
            reason: "dispute".into(),
        },
        MeasurementSource::RetroactiveCorrection {
            correction_ref: "COR-42".into(),
            corrected_by: "clearing".into(),
        },
        MeasurementSource::RedispatchImport {
            pid: 13_020,
            activation_ref: Some("ACT-7".into()),
        },
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
    for method in Method::ALL {
        all.push(MeasurementSource::AutoSubstitute {
            method,
            reason: SubstitutionReason::CommunicationFailure,
        });
    }
    for reason in SubstitutionReason::ALL {
        all.push(MeasurementSource::AutoSubstitute {
            method: Method::Interpolation,
            reason,
        });
    }
    for rule in VirtualMeterKind::ALL {
        all.push(MeasurementSource::VirtualMeter {
            rule,
            source_ids: vec!["12345678905".into()],
        });
    }

    fn _every_variant_is_above(s: &MeasurementSource) {
        match s {
            MeasurementSource::Mscons { .. }
            | MeasurementSource::SmgwDirectPush { .. }
            | MeasurementSource::ManualEntry { .. }
            | MeasurementSource::AutoSubstitute { .. }
            | MeasurementSource::RetroactiveCorrection { .. }
            | MeasurementSource::VirtualMeter { .. }
            | MeasurementSource::RedispatchImport { .. }
            | MeasurementSource::ChargeDetailRecord { .. }
            | MeasurementSource::ClockAlignedMeterValue { .. }
            | MeasurementSource::DeviceLog { .. } => {}
        }
    }
    all
}

/// Every event type, with and without a note.
fn every_provenance() -> Vec<ProvenanceEntry> {
    ProvenanceEventType::ALL
        .iter()
        .enumerate()
        .map(|(i, event_type)| ProvenanceEntry {
            occurred_at: datetime!(2026-07-16 06:00 UTC) + time::Duration::seconds(i as i64),
            event_type: *event_type,
            actor: format!("actor-{i}"),
            note: (i % 2 == 0).then(|| format!("note {i}")),
        })
        .collect()
}

/// Every grid the build can store, including the sub-quarter-hour ones.
fn every_resolution() -> Vec<Option<Resolution>> {
    let mut all: Vec<Option<Resolution>> = [
        "PT15M", "PT30M", "PT1H", "P1D", "P1M", "P1Y", "PT1M", "PT5M",
    ]
    .iter()
    .map(|s| Some(s.parse().expect("a grid this release writes")))
    .collect();
    all.push(None);
    all
}

/// Every commodity in each unit it admits.
fn every_sparte_unit() -> Vec<(Sparte, metering::Unit)> {
    let mut all = Vec::new();
    for sparte in Sparte::ALL {
        all.push((sparte, sparte.billing_unit()));
        if sparte.measured_unit() != sparte.billing_unit() {
            all.push((sparte, sparte.measured_unit()));
        }
    }
    all
}

/// The varying parts of one stored delivery.
#[derive(Clone)]
struct Shape {
    source: MeasurementSource,
    provenance: Vec<ProvenanceEntry>,
    sparte: Sparte,
    unit: metering::Unit,
    quality: QualityFlag,
    resolution: Option<Resolution>,
    /// The Marktpartner-ID the version is scoped to — parsed by `metering`'s
    /// rules on the way back in, so a tightening of them reaches stored rows.
    operator: &'static str,
}

impl Default for Shape {
    fn default() -> Self {
        Self {
            source: mscons(),
            provenance: Vec::new(),
            sparte: Sparte::Strom,
            unit: Sparte::Strom.billing_unit(),
            quality: QualityFlag::Measured,
            resolution: "PT15M".parse().ok(),
            operator: "9900000000003",
        }
    }
}

/// One shape per value of every vocabulary, each varied alone.
fn every_shape() -> Vec<Shape> {
    let base = Shape::default();
    let mut all = vec![base.clone()];
    all.extend(every_source().into_iter().map(|source| Shape {
        source,
        ..base.clone()
    }));
    all.push(Shape {
        provenance: every_provenance(),
        ..base.clone()
    });
    all.extend(every_sparte_unit().into_iter().map(|(sparte, unit)| Shape {
        sparte,
        unit,
        ..base.clone()
    }));
    all.extend(QualityFlag::ALL.into_iter().map(|quality| Shape {
        quality,
        ..base.clone()
    }));
    all.extend(every_resolution().into_iter().map(|resolution| Shape {
        resolution,
        ..base.clone()
    }));
    // An operator under each check-digit procedure the market issues: BDEW's,
    // and GS1's, which the Bildungsvorschrift admits.
    all.push(Shape {
        operator: "9900987654320",
        ..base.clone()
    });
    all
}

fn version(shape: &Shape) -> meterstore::ScopedVersion {
    meterstore::ScopedVersion::new(
        meterstore::VersionScope::for_interval(shape.operator, AT, shape.sparte).unwrap(),
        meterstore::Version::new(1).unwrap(),
    )
}

fn series_of(shape: &Shape) -> StoredSeries {
    let mut series = MeasurementSeries::new(
        "12345678905".parse().unwrap(),
        "1-0:1.8.0".parse().ok(),
        vec![
            MeterInterval::new(
                AT,
                AT + time::Duration::minutes(15),
                Decimal::new(1234, 3),
                shape.quality,
            )
            .expect("a non-empty interval")
            .with_obis("1-0:1.8.0".parse().unwrap()),
        ],
        shape.source.clone(),
        AT,
    );
    series.resolution = shape.resolution;
    series.provenance = shape.provenance.clone();
    StoredSeries::of(shape.sparte, series, version(shape), AT).in_unit(shape.unit)
}

fn readings_of(shape: &Shape) -> StoredReadings {
    let mut readings = StoredReadings::new(
        "12345678905".parse().unwrap(),
        "1-0:1.8.0".parse().unwrap(),
        shape.sparte,
        vec![MeterReading {
            at: AT,
            value: Decimal::new(98_765, 1),
            quality: shape.quality,
            obis_code: None,
        }],
        shape.source.clone(),
        version(shape),
        AT,
    )
    .in_unit(shape.unit);
    readings.cadence = shape.resolution;
    readings.provenance = shape.provenance.clone();
    readings
}

// --- reading and writing the stored columns ---------------------------------

fn row_of(batch: &RecordBatch) -> Row {
    COLUMNS
        .iter()
        .map(|name| {
            let column = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("{name}: not in the batch"));
            let strings = column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("{name}: not a string column"));
            let value = (!strings.is_null(0)).then(|| strings.value(0).to_string());
            (name.to_string(), value)
        })
        .collect()
}

/// `batch` with its vocabulary columns replaced by `row`'s.
fn with_row(batch: &RecordBatch, row: &Row) -> RecordBatch {
    let schema = batch.schema();
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| match row.get(field.name()) {
            Some(value) => Arc::new(StringArray::from(vec![value.clone()])) as ArrayRef,
            None => batch.column(i).clone(),
        })
        .collect();
    RecordBatch::try_new(schema, columns).expect("same schema")
}

/// Decode `row` through the public read path and write it back.
///
/// Both halves, because a row that decodes is not enough: a decoder that
/// normalises a retired spelling onto a current one reads the row and changes
/// what a `GROUP BY` over the column returns. The row must come back exactly as
/// stored.
fn check(kind: &str, row: &Row) -> Result<(), String> {
    let base = match kind {
        "series" => to_record_batch(&[series_of(&Shape::default())]),
        "readings" => readings_to_record_batch(&[readings_of(&Shape::default())]),
        other => return Err(format!("unknown row kind {other:?}")),
    }
    .map_err(|e| format!("building the base row: {e}"))?;
    let stored = with_row(&base, row);

    let rewritten = match kind {
        "series" => from_record_batch(&stored)
            .and_then(|decoded| to_record_batch(&decoded))
            .map_err(|e| e.to_string())?,
        _ => readings_from_record_batch(&stored)
            .and_then(|decoded| readings_to_record_batch(&decoded))
            .map_err(|e| e.to_string())?,
    };
    let back = row_of(&rewritten);
    // Only the columns the file records: a release whose corpus predates a
    // column is held to what it did record.
    for name in COLUMNS
        .iter()
        .copied()
        .filter(|name| row.contains_key(*name))
    {
        if back.get(name) != row.get(name) {
            return Err(format!(
                "{name} decodes but is written back as {:?}, not {:?} — the stored \
                 spelling is no longer this build's",
                back.get(name).cloned().flatten(),
                row.get(name).cloned().flatten(),
            ));
        }
    }
    Ok(())
}

struct Corpus {
    path: PathBuf,
    series: Vec<Row>,
    readings: Vec<Row>,
}

fn load() -> Vec<Corpus> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(corpus_dir())
        .expect("tests/stored-form exists")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let text = std::fs::read_to_string(&path).unwrap();
            let json: serde_json::Value =
                serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let rows = |key: &str| -> Vec<Row> {
                serde_json::from_value(json[key].clone())
                    .unwrap_or_else(|e| panic!("{}: {key}: {e}", path.display()))
            };
            Corpus {
                series: rows("series"),
                readings: rows("readings"),
                path,
            }
        })
        .collect()
}

// --- the tests --------------------------------------------------------------

#[test]
fn stored_form_decodes_every_release() {
    let corpora = load();
    assert!(
        !corpora.is_empty(),
        "tests/stored-form holds no release — a check that reads nothing has checked nothing"
    );

    let mut failures = Vec::new();
    for corpus in &corpora {
        for (kind, rows) in [("series", &corpus.series), ("readings", &corpus.readings)] {
            assert!(
                !rows.is_empty(),
                "{}: no {kind} rows",
                corpus.path.display()
            );
            for (i, row) in rows.iter().enumerate() {
                if let Err(e) = check(kind, row) {
                    failures.push(format!("{} {kind}[{i}]: {e}", corpus.path.display()));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "rows a published release wrote no longer decode as written — a stored-data \
         break. Accepting one means removing the rows from the release's file, naming \
         the release that broke them under `broken_by`, and saying so in the \
         changelog:\n{}",
        failures.join("\n")
    );
}

#[test]
fn the_corpus_check_catches_a_planted_rename() {
    let corpora = load();
    let row = |tag: &str| -> Row {
        corpora
            .iter()
            .flat_map(|c| &c.series)
            .find(|r| r.get(col::SOURCE_KIND) == Some(&Some(tag.to_string())))
            .unwrap_or_else(|| panic!("the corpus holds a {tag} row"))
            .clone()
    };
    let replace = |mut row: Row, name: &str, from: &str, to: &str| -> Row {
        let value = row[name].clone().unwrap();
        assert!(value.contains(from), "{name} {value:?} contains {from:?}");
        row.insert(name.to_string(), Some(value.replace(from, to)));
        row
    };

    // A nested code renamed: the outer tag still agrees with itself.
    let nested = replace(
        row("VIRTUAL_METER"),
        col::SOURCE_DETAIL,
        "\"SUM\"",
        "\"TOTAL\"",
    );
    let err = check("series", &nested).expect_err("a renamed nested code");
    assert!(
        err.contains("source_detail") || err.contains("json"),
        "{err}"
    );

    // The outer tag renamed in both columns at once.
    let tag = replace(
        row("MSCONS"),
        col::SOURCE_DETAIL,
        "\"MSCONS\"",
        "\"EDIFACT\"",
    );
    let tag = replace(tag, col::SOURCE_KIND, "MSCONS", "EDIFACT");
    check("series", &tag).expect_err("a renamed outer tag");

    // A code column renamed.
    let quality = replace(row("MSCONS"), col::QUALITY, "MEASURED", "MEASURED_VALUE");
    let err = check("series", &quality).expect_err("a renamed quality code");
    assert!(err.contains(col::QUALITY), "{err}");

    // Decodes, and is written back in another spelling: the instant parses as
    // RFC 3339 either way, so only the write-back comparison sees it.
    let mut respelled = row("MSCONS");
    respelled.insert(
        col::PROVENANCE.to_string(),
        Some(
            r#"[{"occurred_at":"2026-07-16T06:00:00+00:00","event_type":"INGESTED","actor":"a","note":null}]"#
                .to_string(),
        ),
    );
    let err = check("series", &respelled).expect_err("a respelled instant");
    assert!(err.contains("written back"), "{err}");
}

/// Write the corpus for the version `Cargo.toml` names. `just stored-form` runs it.
///
/// Refuses to overwrite: a committed file is what that release wrote, and a
/// regenerated one is what *this* build writes, which is the thing being
/// checked against it.
#[test]
#[ignore = "writes tests/stored-form/<version>.json; run by `just stored-form`"]
fn write_this_release() {
    let version = env!("CARGO_PKG_VERSION");
    let path = corpus_dir().join(format!("{version}.json"));
    assert!(
        !path.exists(),
        "{} exists — a release's corpus is written once",
        path.display()
    );

    let series: Vec<Row> = every_shape()
        .iter()
        .map(|s| row_of(&to_record_batch(&[series_of(s)]).expect("encode")))
        .collect();
    let readings: Vec<Row> = every_shape()
        .iter()
        .map(|s| row_of(&readings_to_record_batch(&[readings_of(s)]).expect("encode")))
        .collect();
    let json = serde_json::json!({
        "releases": [version],
        "written_by": "just stored-form",
        "series": series,
        "readings": readings,
    });
    std::fs::create_dir_all(corpus_dir()).unwrap();
    std::fs::write(&path, serde_json::to_string_pretty(&json).unwrap() + "\n").unwrap();
}
