//! A harness, a workload generator, and the assertions that matter.
//!
//! Behind the `testkit` feature, and part of the public API on purpose: the
//! properties in §17.3 are what a *deployment* needs to be able to check, not
//! only what this crate needs to check about itself. A utility integrating
//! MeterStore should be able to run the same oracle against its own
//! configuration and its own volumes.
//!
//! # The oracle
//!
//! > For any archival history and any query range, a query over the unified
//! > view must equal the same query against a single reference table holding
//! > every row ever written, with latest-version-wins applied.
//!
//! [`Oracle`] is that sentence, executable. It keeps every row the workload ever
//! produced, resolves them the way the domain says to, and compares against what
//! the store returns. It is deliberately a *different implementation* of
//! resolution — a `BTreeMap` fold in Rust rather than a window function in SQL —
//! because an oracle that shared the implementation under test would agree with
//! it about everything, including its mistakes.
//!
//! # Why the generator is seeded
//!
//! A failure nobody can reproduce is a failure nobody can fix.
//! [`MeteringWorkload`] takes a seed and derives everything from it, so a
//! failing run is replayable from the seed alone. The generator is a small
//! explicit PRNG rather than a dependency, so the sequence is stable across
//! toolchains and crate versions — a workload that changed shape when a
//! transitive dependency bumped would quietly stop testing what it used to.

use std::collections::BTreeMap;

use metering::ids::{MaloId, MeloId};
use metering::interval::{MeasurementUnit, MeterInterval, QualityFlag, Sparte};
use metering::measurement_series::{MeasurementSeries, MeasurementSource};
use metering::resolution::IntervalResolution;
use rust_decimal::Decimal;
use time::{Duration, OffsetDateTime};

use crate::encode::{StoredReadings, StoredSeries};
use crate::error::{Error, Result};
use crate::version::{ScopedVersion, Version, VersionScope};

pub mod harness;
pub mod postgres;

pub use harness::TestHarness;

/// A deterministic PRNG.
///
/// SplitMix64: three lines, no dependency, and a fixed sequence for a given
/// seed forever. A workload whose shape drifted when a transitive dependency
/// bumped would quietly stop testing what it used to, and the failure mode is
/// that coverage silently narrows rather than that anything breaks.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// Seed the generator.
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next value in the sequence.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value below `n`.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    /// Whether an event with probability `p` occurs.
    pub fn chance(&mut self, p: f64) -> bool {
        let scale = 1_000_000u64;
        self.below(scale) < (p.clamp(0.0, 1.0) * scale as f64) as u64
    }
}

/// A generated metering workload.
///
/// Shaped like the real thing rather than like uniform noise, because the
/// properties under test are sensitive to exactly the ways metering data is not
/// uniform: corrections are rare and recent, some intervals never arrive, and
/// the day length changes twice a year.
#[derive(Debug, Clone)]
pub struct MeteringWorkload {
    seed: u64,
    malo_ids: usize,
    days: i64,
    start: OffsetDateTime,
    resolution: Duration,
    correction_rate: f64,
    gap_rate: f64,
    operator: String,
    malo_offset: usize,
    sparte: Sparte,
    unit: MeasurementUnit,
    messlokationen: usize,
}

impl MeteringWorkload {
    /// A workload starting at `start`, with defaults matching §17.3.
    pub fn new(start: OffsetDateTime) -> Self {
        Self {
            seed: 0x5EED,
            malo_ids: 10,
            days: 3,
            start,
            resolution: Duration::minutes(15),
            correction_rate: 0.0,
            gap_rate: 0.0,
            operator: "9900000000001".to_string(),
            malo_offset: 0,
            sparte: Sparte::Strom,
            unit: Sparte::Strom.billing_unit(),
            messlokationen: 0,
        }
    }

    /// Give each Marktlokation `n` **Messlokationen**, each reporting the same
    /// channel at the same instants.
    ///
    /// Zero — the default — names none, which is the shape a plain Lastgang has.
    ///
    /// Any other value is the shape a Mehrfamilienhaus or a house with an
    /// Einliegerwohnung has, and the one that separates a store keyed by the
    /// Messlokation from one that is not: the meters agree on the channel, on the
    /// instants and — at installation — on the number, so a merge key without
    /// `melo_id` folds them into one reading and `ON CONFLICT DO NOTHING` drops
    /// the second with nothing to notice.
    ///
    /// Pair with `TableConfig::identify_by_melo(true)` (or a point table, where
    /// it is the default), and with [`Oracle::for_table`], which then keys on the
    /// same thing the store does.
    pub fn messlokationen(mut self, n: usize) -> Self {
        self.messlokationen = n;
        self
    }

    /// Measure a different commodity, in that commodity's billing unit.
    ///
    /// Water is the one that matters most here: it is billed in m³, so a store
    /// that only ever saw electricity would never notice it was treating the
    /// unit as decoration. Override the unit with [`in_unit`](Self::in_unit) for
    /// gas held as unconverted Betriebsvolumen.
    pub fn sparte(mut self, sparte: Sparte) -> Self {
        self.sparte = sparte;
        self.unit = sparte.billing_unit();
        self
    }

    /// Store the values in a unit other than the Sparte's billing unit.
    pub fn in_unit(mut self, unit: MeasurementUnit) -> Self {
        self.unit = unit;
        self
    }

    /// Report at an interval other than 15 minutes.
    ///
    /// Sub-quarter-hourly data is what iMSys can already deliver and what §14a
    /// steering will need, and the interval count per day is not 96 — so a
    /// workload that only ever generated quarter-hours would leave every
    /// resolution-dependent path (completeness, the DST calendar, the expected
    /// interval UDF) asserted against a single value.
    ///
    /// Rejected if it does not divide a day evenly: a workload that generated a
    /// ragged final interval would fail for a reason that has nothing to do with
    /// what it was written to test.
    pub fn resolution(mut self, resolution: Duration) -> Result<Self> {
        let seconds = resolution.whole_seconds();
        if seconds <= 0 || Duration::DAY.whole_seconds() % seconds != 0 {
            return Err(Error::config(format!(
                "workload resolution {resolution} must be a positive divisor of 24 h"
            )));
        }
        self.resolution = resolution;
        Ok(self)
    }

    /// Fix the seed. A failing run is replayable from this alone.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// How many measuring points report.
    pub fn malo_ids(mut self, n: usize) -> Self {
        self.malo_ids = n.max(1);
        self
    }

    /// How many days the workload covers.
    pub fn days(mut self, n: i64) -> Self {
        self.days = n.max(1);
        self
    }

    /// Shift the generated MaLo-IDs so two workloads describe different meters.
    ///
    /// MaLo-IDs are derived from the index, not from the seed, so two workloads
    /// of the same size describe the *same* measuring points however they are
    /// seeded. That is right for replaying one population and wrong for composing
    /// several — and composing them is exactly what a multi-Sparte portfolio is,
    /// since a Marktlokation belongs to one commodity and `sparte` is deliberately
    /// not part of the merge key.
    pub fn malo_offset(mut self, offset: usize) -> Self {
        self.malo_offset = offset;
        self
    }

    /// The share of intervals that are later corrected.
    ///
    /// Rare by default because they are rare in practice, and because that is
    /// the property merge elision depends on (§9.2).
    pub fn with_corrections(mut self, rate: f64) -> Self {
        self.correction_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// The share of intervals that never arrive.
    ///
    /// A gap is ordinary — a meter can simply not report — and completeness
    /// exists to say so (§9.6). A workload with none of them never exercises it.
    pub fn with_gaps(mut self, rate: f64) -> Self {
        self.gap_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Start the workload on the day German local time loses an hour.
    ///
    /// The 92-interval day. A workload that never spans one leaves the most
    /// error-prone fixture in the domain untested.
    pub fn spanning_spring_forward(mut self) -> Self {
        self.start = time::macros::datetime!(2026-03-28 00:00 UTC);
        self.days = self.days.max(3);
        self
    }

    /// Start the workload on the day German local time gains an hour.
    ///
    /// The 100-interval day, and the dangerous direction: a check that assumed
    /// 96 would call a four-interval shortfall complete.
    pub fn spanning_autumn_back(mut self) -> Self {
        self.start = time::macros::datetime!(2026-10-24 00:00 UTC);
        self.days = self.days.max(3);
        self
    }

    /// The interval range the workload covers.
    pub fn range(&self) -> (OffsetDateTime, OffsetDateTime) {
        (self.start, self.start + Duration::days(self.days))
    }

    /// Generate the series, in delivery order.
    ///
    /// Corrections come *after* the deliveries they correct, which is what makes
    /// the version axis meaningful: a store that happened to apply them in the
    /// other order would pass a test whose input arrived pre-sorted.
    pub fn generate(&self) -> Result<Vec<StoredSeries>> {
        let mut rng = Rng::new(self.seed);
        let mut out = Vec::new();
        let mut corrections: Vec<(usize, Option<usize>, OffsetDateTime, Decimal)> = Vec::new();

        for day in 0..self.days {
            let day_start = self.start + Duration::days(day);
            let steps = Duration::DAY.whole_seconds() / self.resolution.whole_seconds();

            for malo in 0..self.malo_ids {
                for meter in self.meters() {
                    let mut intervals = Vec::new();
                    for step in 0..steps {
                        let from = day_start + self.resolution * step as i32;
                        if rng.chance(self.gap_rate) {
                            continue;
                        }
                        let kwh = Decimal::new(rng.below(500) as i64 + 1, 2);
                        if rng.chance(self.correction_rate) {
                            corrections.push((malo, meter, from, kwh + Decimal::new(100, 2)));
                        }
                        intervals.push(self.interval(from, kwh));
                    }
                    // A delivery is split per version scope, not per day. A UTC
                    // day is not inside one local month: 2026-07-31T22:00Z is
                    // already August in Berlin, so a day-aligned batch at a month
                    // end spans two scopes and encoding rejects it (§4.2). Real
                    // ingestion has the same constraint, which is the point of
                    // generating it.
                    for (_, group) in group_by_scope(intervals, self.sparte) {
                        let anchor = group[0].from;
                        out.push(self.stored(malo, meter, group, FIRST_VERSION, anchor)?);
                    }
                }
            }
        }

        // Corrections are grouped per measuring point *and meter* so each lands
        // as one delivery, which is how a market message arrives.
        let mut by_meter: BTreeMap<(usize, Option<usize>), Vec<MeterInterval>> = BTreeMap::new();
        for (malo, meter, from, kwh) in corrections {
            by_meter
                .entry((malo, meter))
                .or_default()
                .push(self.interval(from, kwh));
        }
        for ((malo, meter), mut intervals) in by_meter {
            intervals.sort_by_key(|i| i.from);
            // A version scope covers one local month, so a correction batch
            // spanning a month boundary has to be split — encoding rejects a
            // scope that does not cover its intervals (§4.2).
            for (_, group) in group_by_scope(intervals, self.sparte) {
                let anchor = group[0].from;
                out.push(self.stored(malo, meter, group, CORRECTION_VERSION, anchor)?);
            }
        }

        Ok(out)
    }

    /// Generate **Zählerstandsgänge** — register readings at instants.
    ///
    /// The point-series counterpart of [`generate`](Self::generate).
    ///
    /// The values are **cumulative and monotonic**, because that is what a
    /// register is: differencing two of them is the derived Lastgang, and a
    /// generator emitting independent draws would produce a series no meter could
    /// have. Corrections restate a reading *upwards* at a higher version, so the
    /// series stays monotonic after resolution too.
    ///
    /// Register readings are keyed by the **Messlokation** where the table says
    /// so, so this defaults to one meter per Marktlokation rather than none —
    /// `identify_by_melo` is on by default for a point table, and a delivery
    /// naming no Messlokation is refused there.
    pub fn generate_readings(&self) -> Result<Vec<StoredReadings>> {
        use metering::reading::MeterReading;

        let mut rng = Rng::new(self.seed);
        let mut out = Vec::new();
        let mut corrections: Vec<(usize, usize, OffsetDateTime, Decimal)> = Vec::new();
        // One running register per (measuring point, meter), so a reading only
        // ever ascends — across days as well as within one.
        let mut registers: BTreeMap<(usize, usize), Decimal> = BTreeMap::new();

        for day in 0..self.days {
            let day_start = self.start + Duration::days(day);
            let steps = Duration::DAY.whole_seconds() / self.resolution.whole_seconds();

            for malo in 0..self.malo_ids {
                for meter in 0..self.messlokationen.max(1) {
                    let mut readings = Vec::new();
                    for step in 0..steps {
                        let at = day_start + self.resolution * step as i32;
                        let advance = Decimal::new(rng.below(500) as i64 + 1, 2);
                        let register = registers.entry((malo, meter)).or_default();
                        *register += advance;
                        if rng.chance(self.gap_rate) {
                            // The register still advanced; the *reading* is what
                            // did not arrive. A gap that also froze the register
                            // would make the next value understate consumption.
                            continue;
                        }
                        if rng.chance(self.correction_rate) {
                            corrections.push((malo, meter, at, *register + Decimal::new(100, 2)));
                        }
                        readings.push(MeterReading {
                            at,
                            value: *register,
                            quality: QualityFlag::Measured,
                            obis_code: "1-0:1.8.0".parse().ok(),
                        });
                    }
                    for (_, group) in group_readings_by_scope(readings, self.sparte) {
                        let anchor = group[0].at;
                        out.push(self.stored_readings(
                            malo,
                            meter,
                            group,
                            FIRST_VERSION,
                            anchor,
                        )?);
                    }
                }
            }
        }

        let mut by_meter: BTreeMap<(usize, usize), Vec<MeterReading>> = BTreeMap::new();
        for (malo, meter, at, value) in corrections {
            by_meter
                .entry((malo, meter))
                .or_default()
                .push(MeterReading {
                    at,
                    value,
                    quality: QualityFlag::Corrected,
                    obis_code: "1-0:1.8.0".parse().ok(),
                });
        }
        for ((malo, meter), mut readings) in by_meter {
            readings.sort_by_key(|r| r.at);
            for (_, group) in group_readings_by_scope(readings, self.sparte) {
                let anchor = group[0].at;
                out.push(self.stored_readings(malo, meter, group, CORRECTION_VERSION, anchor)?);
            }
        }

        Ok(out)
    }

    /// The meters of one Marktlokation, or a single unnamed one.
    fn meters(&self) -> Vec<Option<usize>> {
        match self.messlokationen {
            0 => vec![None],
            n => (0..n).map(Some).collect(),
        }
    }

    fn interval(&self, from: OffsetDateTime, kwh: Decimal) -> MeterInterval {
        MeterInterval {
            from,
            to: from + self.resolution,
            value: kwh,
            quality: QualityFlag::Measured,
            obis_code: "1-0:1.8.0".parse().ok(),
        }
    }

    fn stored(
        &self,
        malo: usize,
        meter: Option<usize>,
        intervals: Vec<MeterInterval>,
        version: u128,
        scope_anchor: OffsetDateTime,
    ) -> Result<StoredSeries> {
        let malo_id = malo_id(self.malo_offset + malo);
        let recorded_at = intervals.last().map(|i| i.to).unwrap_or(scope_anchor);

        let mut series = MeasurementSeries::new(
            malo_id,
            "1-0:1.8.0".parse().ok(),
            intervals,
            MeasurementSource::Mscons {
                pid: 13_005,
                message_ref: None,
                sender_mp_id: self.operator.clone(),
            },
            recorded_at,
        );
        // Declared explicitly rather than left to `obis_code.default_resolution()`,
        // which answers for the channel and not for this delivery. At anything but
        // 15 minutes the two disagree, and completeness would then measure the
        // series against an expectation nothing in the workload produced.
        series.resolution = Some(self.declared_resolution());
        series.melo_id = meter.map(|m| melo_id(self.malo_offset + malo, m));

        Ok(StoredSeries::of(
            self.sparte,
            series,
            ScopedVersion::new(
                VersionScope::for_interval(&self.operator, scope_anchor, self.sparte)?,
                Version::new(version)?,
            ),
            recorded_at,
        )
        .in_unit(self.unit))
    }

    /// One delivery of register readings.
    fn stored_readings(
        &self,
        malo: usize,
        meter: usize,
        readings: Vec<metering::reading::MeterReading>,
        version: u128,
        scope_anchor: OffsetDateTime,
    ) -> Result<StoredReadings> {
        let malo_id = malo_id(self.malo_offset + malo);
        let recorded_at = readings
            .last()
            .map(|r| r.at + self.resolution)
            .unwrap_or(scope_anchor);

        Ok(StoredReadings::new(
            malo_id,
            "1-0:1.8.0".parse().expect("a canonical OBIS code"),
            self.sparte,
            readings,
            MeasurementSource::Mscons {
                pid: 13_005,
                message_ref: None,
                sender_mp_id: self.operator.clone(),
            },
            ScopedVersion::new(
                VersionScope::for_interval(&self.operator, scope_anchor, self.sparte)?,
                Version::new(version)?,
            ),
            recorded_at,
        )
        .with_melo_id(melo_id(self.malo_offset + malo, meter))
        // The cadence answers the same question a series' resolution does — how
        // many values a day should there be — so completeness measures a
        // Zählerstandsgang against the grid it was generated on.
        .at_cadence(self.declared_resolution())
        .in_unit(self.unit))
    }

    /// The workload's interval length as the domain spells it.
    fn declared_resolution(&self) -> IntervalResolution {
        let seconds = u32::try_from(self.resolution.whole_seconds())
            .expect("the builder rejects non-positive resolutions");
        IntervalResolution::from_seconds(seconds)
            .expect("the builder rejects a zero-length resolution")
    }
}

/// Split intervals into runs sharing a **Bilanzierungsmonat**.
///
/// Not the calendar month: for gas the month is cut at 06:00 local, like the
/// Gastag it is built from, so a run boundary at midnight would put the first six
/// hours of a calendar month in the wrong scope — and `covers` would then refuse
/// the delivery this generator produced. The workload must be one a real ingest
/// could send, or the oracle is comparing against something the store is right to
/// reject.
fn group_by_scope(
    intervals: Vec<MeterInterval>,
    sparte: Sparte,
) -> Vec<(time::Date, Vec<MeterInterval>)> {
    let mut out: Vec<(time::Date, Vec<MeterInterval>)> = Vec::new();
    for interval in intervals {
        let month = crate::planner::balancing_month(interval.from, sparte);
        match out.last_mut() {
            Some((m, group)) if *m == month => group.push(interval),
            _ => out.push((month, vec![interval])),
        }
    }
    out
}

/// [`group_by_scope`] over register readings, which carry an instant rather than
/// a span.
fn group_readings_by_scope(
    readings: Vec<metering::reading::MeterReading>,
    sparte: Sparte,
) -> Vec<(time::Date, Vec<metering::reading::MeterReading>)> {
    let mut out: Vec<(time::Date, Vec<metering::reading::MeterReading>)> = Vec::new();
    for reading in readings {
        let month = crate::planner::balancing_month(reading.at, sparte);
        match out.last_mut() {
            Some((m, group)) if *m == month => group.push(reading),
            _ => out.push((month, vec![reading])),
        }
    }
    out
}

/// The version a first delivery carries.
const FIRST_VERSION: u128 = 20_260_101_000_001;
/// The version a correction carries. Higher, so it supersedes within its scope.
const CORRECTION_VERSION: u128 = 20_260_201_000_002;

/// A synthetic Marktlokations-ID, `n` places into a contiguous block.
///
/// The check digit is **computed**, not padded onto ten digits and hoped for:
/// `MaloId` enforces the Bildungsvorschrift at the parse, so a generator that
/// emitted eleven arbitrary digits would fail to build a single row of workload.
/// Deriving it from `metering`'s own [`MaloId::compute_check_digit`] also means
/// the fixture cannot disagree with the validator it is checked by.
fn malo_id(n: usize) -> MaloId {
    // Ten digits with a leading `1`, which is a DVGW Vergabestelle — inside the
    // `1`–`9` the scheme allows, and stable for every `n` a test will use.
    let prefix = format!("{:010}", 1_000_000_000u64 + n as u64);
    let check = MaloId::compute_check_digit(&prefix).expect("ten ASCII digits");
    format!("{prefix}{check}")
        .parse()
        .expect("a computed check digit is the one the parser recomputes")
}

/// A synthetic Zählpunktbezeichnung for meter `meter` of measuring point `malo`.
///
/// Thirty-three ASCII alphanumerics, two uppercase letters then six digits —
/// `MeloId` enforces exactly that, so a generator emitting anything else would
/// fail to build a single row. Derived from both indices so two meters of one
/// Marktlokation differ, which is the whole point of generating them.
fn melo_id(malo: usize, meter: usize) -> MeloId {
    format!("DE{:06}{:025}", malo % 1_000_000, meter)
        .parse()
        .expect("the shape MeloId's parser enforces")
}

/// What identifies one reading in the reference: the core key, plus whatever the
/// deployment declared as identity.
type OracleKey = (String, String, OffsetDateTime, Vec<String>);

/// The reference implementation the store is checked against (§17.3).
///
/// Holds every row the workload ever produced and resolves them independently:
/// a fold over a map in Rust, rather than the window function the store plans.
/// Two implementations that share code agree about their shared mistakes, which
/// is the one thing an oracle must not do.
///
/// # Tell it the merge key
///
/// [`Oracle::new`] keys on `(malo_id, obis_code, from)`, which is right only for
/// a table with no identity columns and no key-carrying Messlokation. A
/// deployment with either has a *wider* notion of "the same reading", and an
/// oracle using the narrower one folds two tenants' — or two meters' — readings
/// into a single key and picks a winner across them. It would then disagree with
/// a store that is behaving correctly, which is the worst way for a reference to
/// be wrong: it accuses the thing it exists to check.
///
/// [`Oracle::for_table`] takes the configuration the store was built with, so the
/// two cannot disagree about what identifies a reading.
///
/// ```no_run
/// # use meterstore::testkit::Oracle;
/// # fn example(config: &meterstore::ValidatedTableConfig) {
/// let oracle = Oracle::for_table(config);
/// # let _ = oracle;
/// # }
/// ```
#[derive(Debug, Default, Clone)]
pub struct Oracle {
    /// The identity columns beyond the core key, in merge-key order.
    identity: Vec<String>,
    /// Key → (version scope, version, value in force).
    rows: BTreeMap<OracleKey, (String, u128, Decimal)>,
}

impl Oracle {
    /// An empty reference over the core merge key.
    ///
    /// Correct for a table with no identity columns. Prefer
    /// [`for_table`](Self::for_table), which cannot disagree with the store.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty reference over the table's **actual** merge key.
    ///
    /// Reads `discriminator_columns` from the same validated configuration the
    /// store was built with, so "the same reading" means one thing in both. That
    /// list rather than `identity_columns`, because a table identifying a reading
    /// by its Messlokation puts a *core* column in the key.
    pub fn for_table(config: &crate::config::ValidatedTableConfig) -> Self {
        Self {
            identity: config.discriminator_columns(),
            rows: BTreeMap::new(),
        }
    }

    /// The discriminator values of a delivery, in merge-key order.
    ///
    /// A missing value is an error rather than a default: every column in this
    /// list is non-nullable in the key, so a delivery without one could not have
    /// been written, and silently substituting an empty string would merge it
    /// with every other delivery that is also missing one.
    ///
    /// `melo_id` is read from the delivery rather than from `extra` because it is
    /// a core column: it lives on the series, not among the deployment's declared
    /// ones.
    fn identity_of(&self, stored: &StoredSeries) -> Result<Vec<String>> {
        self.discriminators(
            &stored.series.malo_id,
            stored.series.melo_id.as_ref(),
            &stored.extra,
        )
    }

    /// [`identity_of`](Self::identity_of) for a register delivery.
    fn identity_of_readings(&self, stored: &StoredReadings) -> Result<Vec<String>> {
        self.discriminators(&stored.malo_id, stored.melo_id.as_ref(), &stored.extra)
    }

    fn discriminators(
        &self,
        malo_id: &MaloId,
        melo_id: Option<&MeloId>,
        extra: &std::collections::BTreeMap<String, datafusion::common::ScalarValue>,
    ) -> Result<Vec<String>> {
        self.identity
            .iter()
            .map(|name| {
                let value = match name.as_str() {
                    n if n == crate::encode::schema::col::MELO_ID => {
                        melo_id.map(std::string::ToString::to_string)
                    }
                    _ => match extra.get(name) {
                        Some(datafusion::common::ScalarValue::Utf8(Some(value))) => {
                            Some(value.clone())
                        }
                        _ => None,
                    },
                };
                value.ok_or_else(|| {
                    Error::encode(
                        name,
                        format!(
                            "{malo_id} identifies a reading by {name:?}, but this delivery \
                             carries no value for it — the store would have refused the write"
                        ),
                    )
                })
            })
            .collect()
    }

    /// Record everything a delivery asserted, applying latest-version-wins.
    ///
    /// Versions are compared **within a scope only** (§4.2). Two versions in
    /// different scopes are not ordered, so neither supersedes the other and
    /// both would survive resolution — which is exactly the inflation
    /// `VersionScope::for_interval` exists to prevent, so the oracle has to
    /// model it rather than assume it away.
    pub fn record(&mut self, series: &[StoredSeries]) -> Result<()> {
        for stored in series {
            let scope = stored.version.scope().as_str().to_string();
            let version = stored.version.version().get();
            let identity = self.identity_of(stored)?;

            for interval in &stored.series.intervals {
                let obis = interval
                    .obis_code
                    .or(stored.series.obis_code)
                    .ok_or_else(|| Error::encode("obis_code", "no channel on interval or series"))?
                    .to_string();
                let key = (
                    stored.series.malo_id.to_string(),
                    obis,
                    interval.from,
                    identity.clone(),
                );

                self.observe(key, scope.clone(), version, interval.value)?;
            }
        }
        Ok(())
    }

    /// Record a **Zählerstandsgang** delivery, applying latest-version-wins.
    ///
    /// The point-series counterpart of [`record`](Self::record), and it resolves
    /// identically because resolution is identical: a register reading is keyed
    /// by its instant exactly as an interval is keyed by its start, and the
    /// version axis does not know the difference. Only where the fields are read
    /// from differs.
    pub fn record_readings(&mut self, deliveries: &[StoredReadings]) -> Result<()> {
        for stored in deliveries {
            let scope = stored.version.scope().as_str().to_string();
            let version = stored.version.version().get();
            let identity = self.identity_of_readings(stored)?;

            for reading in &stored.readings {
                let obis = reading.obis_code.unwrap_or(stored.obis_code).to_string();
                let key = (
                    stored.malo_id.to_string(),
                    obis,
                    reading.at,
                    identity.clone(),
                );
                self.observe(key, scope.clone(), version, reading.value)?;
            }
        }
        Ok(())
    }

    /// Fold one asserted value into the reference.
    ///
    /// Shared by both record paths so the two cannot resolve differently — which
    /// they must not, because the store does not either.
    fn observe(
        &mut self,
        key: OracleKey,
        scope: String,
        version: u128,
        value: Decimal,
    ) -> Result<()> {
        match self.rows.get(&key) {
            // Same scope: a strictly higher version supersedes. Equal is
            // deliberately *not* an overwrite — the store inserts with
            // `ON CONFLICT DO NOTHING`, so the first value at a version is the
            // one that stays, and a divergent restatement under an existing
            // version is refused rather than accepted. An oracle that took the
            // last would disagree with a correct store.
            Some((existing, seen, _)) if *existing == scope => {
                if version > *seen {
                    self.rows.insert(key, (scope, version, value));
                }
            }
            // A different scope is not comparable. The generator never produces
            // one for the same key, so this is a guard against the *test*
            // drifting rather than the store.
            Some((existing, _, _)) => {
                return Err(Error::VersionScopeMismatch {
                    left: existing.clone(),
                    right: scope,
                });
            }
            None => {
                self.rows.insert(key, (scope, version, value));
            }
        }
        Ok(())
    }

    /// Rows the reference expects in `[from, to)`.
    pub fn row_count(&self, from: OffsetDateTime, to: OffsetDateTime) -> u64 {
        self.rows
            .keys()
            .filter(|(_, _, start, _)| *start >= from && *start < to)
            .count() as u64
    }

    /// The sum of the values in force over `[from, to)`.
    pub fn sum_kwh(&self, from: OffsetDateTime, to: OffsetDateTime) -> Decimal {
        self.rows
            .iter()
            .filter(|((_, _, start, _), _)| *start >= from && *start < to)
            .map(|(_, (_, _, value))| *value)
            .sum()
    }

    /// The sum of the values in force for one measuring point.
    pub fn sum_kwh_for(&self, malo_id: &str, from: OffsetDateTime, to: OffsetDateTime) -> Decimal {
        self.rows
            .iter()
            .filter(|((malo, _, start, _), _)| malo == malo_id && *start >= from && *start < to)
            .map(|(_, (_, _, value))| *value)
            .sum()
    }

    /// Every measuring point the reference knows about.
    pub fn malo_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .rows
            .keys()
            .map(|(malo, _, _, _)| malo.clone())
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Total rows held.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the reference is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    const START: OffsetDateTime = datetime!(2026-07-20 00:00 UTC);

    #[test]
    fn an_oracle_over_a_tenant_table_keeps_the_tenants_apart() {
        use crate::arrow::datatypes::{DataType, Field};
        use crate::config::TableConfig;
        use datafusion::common::ScalarValue;

        // Two tenants reporting the same measuring point at the same instant are
        // two readings, not one. An oracle keyed on the core merge key folds them
        // into a single row and picks a winner across them — then reports a
        // mismatch against a store that is behaving correctly, which is the worst
        // way for a reference to be wrong.
        let config = TableConfig::new("readings_versions")
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            .build()
            .expect("config");

        let base = MeteringWorkload::new(START).malo_ids(1).days(1);
        let for_tenant = |tenant: &str| -> Vec<StoredSeries> {
            base.clone()
                .generate()
                .expect("workload")
                .into_iter()
                .map(|s| s.with_extra("tenant", ScalarValue::Utf8(Some(tenant.into()))))
                .collect()
        };

        let mut aware = Oracle::for_table(&config);
        aware.record(&for_tenant("a")).expect("tenant a");
        aware.record(&for_tenant("b")).expect("tenant b");

        let mut naive = Oracle::new();
        naive.record(&for_tenant("a")).expect("tenant a");
        naive.record(&for_tenant("b")).expect("tenant b");

        assert_eq!(
            aware.len(),
            naive.len() * 2,
            "the tenant-aware reference holds both tenants' readings"
        );
    }

    #[test]
    fn an_oracle_over_a_melo_keyed_table_keeps_the_meters_apart() {
        use crate::config::{TableConfig, TimeModel};

        // The same argument as the tenant case, on the column that made
        // `discriminator_columns` necessary: a Marktlokation may be measured by
        // several Messlokationen, and a point table identifies a reading by its.
        // `for_table` read `identity_columns`, which is that list only while
        // `melo_id` cannot join the key — so on the one shape where it always
        // does, the reference folded two meters into one reading and would have
        // accused a correct store.
        let config = TableConfig::new("meter_reads_versions")
            .time_model(TimeModel::Point)
            .build()
            .expect("config");
        assert!(config.melo_in_merge_key());

        let workload = MeteringWorkload::new(START)
            .malo_ids(1)
            .days(1)
            .messlokationen(2);
        let deliveries = workload.generate_readings().expect("workload");

        let mut aware = Oracle::for_table(&config);
        aware.record_readings(&deliveries).expect("record");

        let mut naive = Oracle::new();
        naive.record_readings(&deliveries).expect("record");

        assert_eq!(
            aware.len(),
            naive.len() * 2,
            "two meters at one Marktlokation are two registers, not one"
        );
    }

    #[test]
    fn a_generated_zaehlerstandsgang_only_ever_ascends() {
        // A register is cumulative: differencing two readings is the derived
        // Lastgang. A generator emitting independent draws would produce a series
        // no meter could have, and would hide any bug that depends on the values
        // ascending — including across a day boundary and across a gap, where the
        // register keeps advancing even though the reading did not arrive.
        let deliveries = MeteringWorkload::new(START)
            .malo_ids(2)
            .days(3)
            .messlokationen(2)
            .with_gaps(0.1)
            .generate_readings()
            .expect("workload");

        let mut latest: BTreeMap<(String, String), (OffsetDateTime, Decimal)> = BTreeMap::new();
        for delivery in &deliveries {
            // Corrections restate a value upwards at a higher version, so they
            // are not part of the monotonic first-delivery sequence.
            if delivery.version.version().get() != FIRST_VERSION {
                continue;
            }
            let melo = delivery
                .melo_id
                .as_ref()
                .expect("a named meter")
                .to_string();
            for reading in &delivery.readings {
                let key = (delivery.malo_id.to_string(), melo.clone());
                if let Some((previous_at, previous)) = latest.get(&key) {
                    assert!(reading.at > *previous_at, "{key:?} went back in time");
                    assert!(
                        reading.value > *previous,
                        "{key:?}: {} is not above {previous}",
                        reading.value
                    );
                }
                latest.insert(key, (reading.at, reading.value));
            }
        }
        assert!(!latest.is_empty(), "the workload produced no registers");
    }

    #[test]
    fn readings_and_intervals_resolve_the_same_way() {
        // Resolution does not know the difference between a span and an instant,
        // so the two record paths must fold identically — a correction at a
        // higher version wins, and a replay at an existing one does not.
        let workload = MeteringWorkload::new(START)
            .malo_ids(1)
            .days(1)
            .messlokationen(1)
            .with_corrections(0.2);

        let deliveries = workload.generate_readings().expect("workload");
        let mut once = Oracle::new();
        once.record_readings(&deliveries).expect("record");

        let mut twice = Oracle::new();
        twice.record_readings(&deliveries).expect("first");
        twice.record_readings(&deliveries).expect("replay");

        assert_eq!(once.len(), twice.len(), "a replay adds no readings");
        let (from, to) = workload.range();
        assert_eq!(
            once.sum_kwh(from, to),
            twice.sum_kwh(from, to),
            "a replay changes no value"
        );
    }

    #[test]
    fn a_missing_identity_value_is_refused_rather_than_defaulted() {
        // Identity columns are non-nullable by validation, so a series without
        // one could not have been written. Substituting a default would merge it
        // with every other series that is also missing one.
        use crate::arrow::datatypes::{DataType, Field};
        use crate::config::TableConfig;

        let config = TableConfig::new("readings_versions")
            .identity_column(Field::new("tenant", DataType::Utf8, false))
            .build()
            .expect("config");

        let series = MeteringWorkload::new(START)
            .malo_ids(1)
            .days(1)
            .generate()
            .expect("workload");

        let err = Oracle::for_table(&config)
            .record(&series)
            .expect_err("a series with no tenant must be refused");
        assert!(err.to_string().contains("tenant"), "{err}");
    }

    #[test]
    fn a_redelivery_at_the_same_version_keeps_the_first_value() {
        // The store inserts with `ON CONFLICT DO NOTHING`, so the first value at
        // a version is the one that stays. An oracle that took the last would
        // disagree with a correct store on every replayed batch.
        let mut first = MeteringWorkload::new(START)
            .malo_ids(1)
            .days(1)
            .generate()
            .expect("workload");
        let mut restated = first.clone();
        for s in &mut restated {
            for i in &mut s.series.intervals {
                i.value += Decimal::ONE;
            }
        }

        let mut oracle = Oracle::new();
        oracle.record(&first).expect("first");
        let before = oracle.sum_kwh(START, START + Duration::days(1));
        oracle.record(&restated).expect("restated");

        assert_eq!(
            oracle.sum_kwh(START, START + Duration::days(1)),
            before,
            "an equal version must not overwrite"
        );
        first.clear();
    }

    #[test]
    fn the_generator_is_reproducible_from_its_seed() {
        // A failure nobody can reproduce is a failure nobody can fix.
        let workload = MeteringWorkload::new(START).seed(42).with_corrections(0.1);
        let a = workload.generate().unwrap();
        let b = workload.generate().unwrap();

        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(x.series.malo_id, y.series.malo_id);
            assert_eq!(x.series.intervals.len(), y.series.intervals.len());
            assert_eq!(x.version, y.version);
        }
    }

    #[test]
    fn different_seeds_produce_different_workloads() {
        let a = MeteringWorkload::new(START)
            .seed(1)
            .with_gaps(0.2)
            .generate()
            .unwrap();
        let b = MeteringWorkload::new(START)
            .seed(2)
            .with_gaps(0.2)
            .generate()
            .unwrap();

        let rows =
            |v: &[StoredSeries]| -> usize { v.iter().map(|s| s.series.intervals.len()).sum() };
        assert_ne!(rows(&a), rows(&b), "one seed must not stand in for another");
    }

    #[test]
    fn a_workload_with_no_corrections_has_one_version_per_key() {
        let series = MeteringWorkload::new(START)
            .malo_ids(3)
            .days(2)
            .generate()
            .unwrap();
        assert!(
            series
                .iter()
                .all(|s| s.version.version().get() == FIRST_VERSION),
            "corrections must be opt-in: elision depends on them being rare"
        );
    }

    #[test]
    fn corrections_arrive_after_the_deliveries_they_correct() {
        // A store that applied them in the other order would pass a test whose
        // input happened to arrive pre-sorted.
        let series = MeteringWorkload::new(START)
            .malo_ids(2)
            .days(2)
            .seed(7)
            .with_corrections(0.5)
            .generate()
            .unwrap();

        let first_correction = series
            .iter()
            .position(|s| s.version.version().get() == CORRECTION_VERSION)
            .expect("the workload must produce corrections");
        assert!(
            series[..first_correction]
                .iter()
                .all(|s| s.version.version().get() == FIRST_VERSION)
        );
    }

    #[test]
    fn the_oracle_keeps_the_highest_version_within_a_scope() {
        let workload = MeteringWorkload::new(START).malo_ids(1).days(1);
        let base = workload.generate().unwrap();

        let mut oracle = Oracle::new();
        oracle.record(&base).unwrap();
        let before = oracle.sum_kwh(START, START + Duration::DAY);

        // Restate the same intervals at a higher version, +1 kWh each.
        let mut corrected = base.clone();
        for stored in &mut corrected {
            for interval in &mut stored.series.intervals {
                interval.value += Decimal::ONE;
            }
            stored.version = ScopedVersion::new(
                stored.version.scope().clone(),
                Version::new(CORRECTION_VERSION).unwrap(),
            );
        }
        oracle.record(&corrected).unwrap();

        let intervals = oracle.row_count(START, START + Duration::DAY);
        assert_eq!(
            oracle.sum_kwh(START, START + Duration::DAY),
            before + Decimal::from(intervals),
            "each interval counted once, at its corrected value"
        );
    }

    #[test]
    fn the_oracle_counts_a_corrected_interval_once() {
        let workload = MeteringWorkload::new(START).malo_ids(2).days(1).seed(9);
        let series = workload.generate().unwrap();
        let distinct: usize = series
            .iter()
            .flat_map(|s| {
                s.series
                    .intervals
                    .iter()
                    .map(|i| (s.series.malo_id, i.from))
            })
            .collect::<std::collections::BTreeSet<_>>()
            .len();

        let mut oracle = Oracle::new();
        oracle.record(&series).unwrap();
        assert_eq!(oracle.len(), distinct);
    }

    #[test]
    fn a_gap_rate_actually_removes_intervals() {
        let full = MeteringWorkload::new(START).malo_ids(4).days(1).seed(3);
        let holey = full.clone().with_gaps(0.25);

        let rows = |w: &MeteringWorkload| -> usize {
            w.generate()
                .unwrap()
                .iter()
                .map(|s| s.series.intervals.len())
                .sum()
        };
        assert!(rows(&holey) < rows(&full));
    }

    #[test]
    fn the_dst_workloads_span_their_transition() {
        let spring = MeteringWorkload::new(START).spanning_spring_forward();
        let (from, to) = spring.range();
        assert!(from <= time::macros::datetime!(2026-03-29 00:00 UTC));
        assert!(to > time::macros::datetime!(2026-03-29 00:00 UTC));

        let autumn = MeteringWorkload::new(START).spanning_autumn_back();
        let (from, to) = autumn.range();
        assert!(from <= time::macros::datetime!(2026-10-25 00:00 UTC));
        assert!(to > time::macros::datetime!(2026-10-25 00:00 UTC));
    }

    #[test]
    fn generated_malo_ids_carry_a_valid_check_digit() {
        // The hot table's OBIS constraint is not the only shape that matters; a
        // MaLo is 11 digits *whose last one is derived from the other ten*, and a
        // fixture that ignored that would not be exercising the real key — nor
        // would it survive the round trip, since decoding parses the column back.
        for n in [0, 1, 42, 999, 123_456] {
            let id = malo_id(n);
            let text = id.to_string();
            assert_eq!(text.len(), 11);
            assert!(text.chars().all(|c| c.is_ascii_digit()));
            // Re-parsing is the check: `MaloId`'s `FromStr` recomputes the digit.
            assert_eq!(text.parse::<MaloId>().unwrap(), id);
        }

        // Distinct offsets are distinct measuring points, which the workload's
        // per-MaLo grouping depends on.
        let ids: std::collections::BTreeSet<_> = (0..64).map(malo_id).collect();
        assert_eq!(ids.len(), 64);
    }

    #[test]
    fn a_workload_spanning_a_month_boundary_splits_its_correction_scopes() {
        // A scope covers one Bilanzierungsmonat, and encoding rejects a scope
        // that does not cover its intervals. A correction batch spanning the
        // boundary has to become two deliveries.
        //
        // Driven for **gas** as well as electricity, because the gas month is cut
        // at 06:00 local: splitting the runs at midnight put the first six hours
        // of a calendar month in the previous month's scope, and the generator
        // would then produce a workload the store is right to refuse.
        for sparte in [Sparte::Strom, Sparte::Gas, Sparte::Waerme, Sparte::Wasser] {
            let series = MeteringWorkload::new(datetime!(2026-07-30 00:00 UTC))
                .sparte(sparte)
                .malo_ids(1)
                .days(4)
                .seed(11)
                .with_corrections(0.5)
                .generate()
                .unwrap();

            assert!(!series.is_empty(), "{sparte}");
            for stored in &series {
                for interval in &stored.series.intervals {
                    assert!(
                        stored.version.scope().covers(interval.from, stored.sparte),
                        "scope {} does not cover {} ({sparte})",
                        stored.version.scope(),
                        interval.from
                    );
                }
            }
        }
    }

    #[test]
    fn a_gas_workload_straddling_a_month_start_lands_in_two_scopes() {
        // The case the midnight split got wrong. A gas workload beginning before
        // 06:00 local on the first of a month has its opening intervals in the
        // *previous* Bilanzierungsmonat, so the generator must emit two
        // deliveries rather than one mislabelled batch.
        let series = MeteringWorkload::new(datetime!(2026-02-28 23:00 UTC))
            .sparte(Sparte::Gas)
            .malo_ids(1)
            .days(2)
            .seed(7)
            .generate()
            .unwrap();

        let scopes: std::collections::BTreeSet<_> = series
            .iter()
            .map(|s| s.version.scope().period().to_string())
            .collect();
        assert!(
            scopes.contains("2026-02") && scopes.contains("2026-03"),
            "a gas workload across 1 March must split at 06:00 local, got {scopes:?}"
        );

        for stored in &series {
            for interval in &stored.series.intervals {
                assert!(
                    stored.version.scope().covers(interval.from, stored.sparte),
                    "scope {} does not cover {}",
                    stored.version.scope(),
                    interval.from
                );
            }
        }
    }
}
