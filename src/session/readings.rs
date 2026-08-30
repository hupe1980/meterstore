//! The typed read path for a **Zählerstandsgang**.
//!
//! [`SeriesQuery`](super::SeriesQuery) for register readings, and it exists for
//! the same reason: a caller that wants `metering`'s types should not have to
//! decode Arrow to get them.
//!
//! # Why it is a builder
//!
//! A point table **identifies a reading by its Messlokation** by default, so a
//! Marktlokation with two meters returns two registers per instant, and
//! [`melo`](ReadingsQuery::melo) is how one of them is named.
//!
//! And the question a register is asked most often is *what does the meter read
//! now* — an `ORDER BY … DESC LIMIT 1` at the storage layer rather than a scan of
//! a decade followed by a maximum in memory, on the table §7.4 exists because it
//! grows without bound.
//!
//! # It refuses to fold two registers, and says which
//!
//! Two meters' registers at one instant are two readings, and [`StoredReadings`]
//! holds one Messlokation. Folding them puts two cumulative values at one
//! timestamp, and unlike a Lastgang — where the failure is a doubled sum — here
//! differencing the result produces advances that alternate between two meters
//! and mean nothing at all.

use metering::QualityFlag;
use metering::ids::{MaloId, MeloId};
use metering::measurement_series::{MeasurementSource, ProvenanceEntry};
use metering::obis::ObisCode;
use metering::reading::MeterReading;
use metering::resolution::IntervalResolution;
use time::OffsetDateTime;

use std::collections::BTreeMap;

use datafusion::common::ScalarValue;

use crate::encode::StoredReadings;
use crate::encode::schema::col;
use crate::error::{Error, Result};

/// A read of one measuring point's registers, built up and then collected.
///
/// Obtained from [`MeterStore::readings`](crate::session::MeterStore::readings).
#[derive(Debug, Clone)]
pub struct ReadingsQuery<'a> {
    store: &'a crate::session::MeterStore,
    malo_id: MaloId,
    melo_id: Option<MeloId>,
    obis_code: Option<String>,
    from: Option<OffsetDateTime>,
    to: Option<OffsetDateTime>,
    filters: Vec<(String, ScalarValue)>,
    quality: Vec<QualityFlag>,
    latest_only: bool,
}

impl<'a> ReadingsQuery<'a> {
    /// Start a read for one measuring point.
    pub(crate) fn new(store: &'a crate::session::MeterStore, malo_id: MaloId) -> Self {
        Self {
            store,
            malo_id,
            melo_id: None,
            obis_code: None,
            from: None,
            to: None,
            filters: Vec::new(),
            quality: Vec::new(),
            latest_only: false,
        }
    }

    /// Restrict to one **Messlokation** — one meter.
    ///
    /// The narrowing a point table usually needs: a Marktlokation may be measured
    /// by several, each carrying the same register at the same instants, so an
    /// unnarrowed read spans them. Parsed on the way in, so a truncated or padded
    /// Zählpunktbezeichnung fails here rather than silently matching nothing.
    pub fn melo(mut self, melo_id: &str) -> Result<Self> {
        self.melo_id = Some(
            melo_id
                .parse::<MeloId>()
                .map_err(|e| Error::encode(col::MELO_ID, format!("{melo_id:?}: {e}")))?,
        );
        Ok(self)
    }

    /// Restrict to one register.
    ///
    /// Canonicalised immediately, so a caller may pass whichever spelling they
    /// hold — the column is in the merge key, and a literal comparison against a
    /// non-canonical spelling silently returns nothing.
    pub fn obis(mut self, obis_code: &str) -> Result<Self> {
        self.obis_code = Some(crate::encode::canonical_obis(obis_code)?);
        Ok(self)
    }

    /// Restrict to rows whose declared column `name` equals `value`.
    ///
    /// The same rule [`SeriesQuery::column_eq`](super::SeriesQuery::column_eq)
    /// follows, and the name is checked against the store's declared columns
    /// rather than interpolated on trust: a column name cannot be a bound
    /// parameter in any dialect.
    pub fn column_eq(mut self, name: &str, value: ScalarValue) -> Result<Self> {
        let accepted = filterable_columns(self.store);
        if !accepted.iter().any(|c| c == name) {
            return Err(Error::config(format!(
                "{name:?} is not a filterable column of {}: this store accepts [{}]. \
                 Column names are written into SQL as identifiers, which cannot be \
                 parameterised, so only declared ones are accepted",
                self.store.table(),
                accepted.join(", "),
            )));
        }
        self.filters.push((name.to_string(), value));
        Ok(self)
    }

    /// Restrict to readings whose resolved quality is one of `flags`.
    ///
    /// Matched **after** version resolution, so it sees the value currently in
    /// force rather than a superseded one. An empty slice is a no-op.
    #[must_use]
    pub fn quality_in(mut self, flags: &[QualityFlag]) -> Self {
        self.quality.extend_from_slice(flags);
        self
    }

    /// Restrict to a half-open range `[from, to)`.
    ///
    /// **Strongly recommended** for anything but [`latest`](Self::latest): a
    /// Zählerstandsgang is stored at the Lastgang's own cadence, so an unbounded
    /// read is a decade of quarter-hours.
    #[must_use]
    pub fn range(mut self, from: OffsetDateTime, to: OffsetDateTime) -> Self {
        self.from = Some(from);
        self.to = Some(to);
        self
    }

    /// Read from `from` onwards.
    #[must_use]
    pub fn since(mut self, from: OffsetDateTime) -> Self {
        self.from = Some(from);
        self
    }

    /// Read up to, but excluding, `to`.
    #[must_use]
    pub fn until(mut self, to: OffsetDateTime) -> Self {
        self.to = Some(to);
        self
    }

    /// Run the read and fold it into one delivery-shaped result.
    ///
    /// Rows arrive version-resolved, so each instant appears once carrying the
    /// register value currently in force, ordered ascending.
    ///
    /// **`None` means the range holds no readings**, deliberately rather than an
    /// empty [`StoredReadings`]: it asserts a `source` — who reported these — and
    /// with no values there is nobody to name.
    pub async fn collect(self) -> Result<Option<StoredReadings>> {
        Ok(self.resolve().await?.0)
    }

    /// Just the readings, empty when the range holds none.
    pub async fn values(self) -> Result<Vec<MeterReading>> {
        Ok(self
            .collect()
            .await?
            .map(|r| r.readings)
            .unwrap_or_default())
    }

    /// The most recent register reading, or `None` when there is none.
    ///
    /// **What a meter currently reads** — the question a Zählerstandsgang is
    /// asked most often, and the one a range read answers expensively. Resolved
    /// with `ORDER BY … DESC LIMIT 1` at the storage layer rather than by folding
    /// a history and taking its maximum.
    ///
    /// With no [`range`](Self::range) it is the newest reading ever stored; with
    /// one, the newest inside it. It spans registers and meters unless narrowed
    /// with [`obis`](Self::obis) or [`melo`](Self::melo) — the order is completed
    /// by the rest of the merge key so the choice among them is deterministic,
    /// but it is still a choice, and narrowing is how to make it deliberately.
    ///
    /// For the current value of **every** register — which is what a meter
    /// reading actually is — ask [`channels`](Self::channels) and then `latest`
    /// per register. Deliberately not one call: each `latest` is an
    /// `ORDER BY … DESC LIMIT 1` the index answers, so a meter's four registers
    /// cost four point lookups rather than the history scan a single windowed
    /// query would need on the one table § 146 Abs. 4 AO forbids discarding.
    pub async fn latest(mut self) -> Result<Option<MeterReading>> {
        self.latest_only = true;
        Ok(self
            .resolve()
            .await?
            .0
            .and_then(|r| r.readings.into_iter().next_back()))
    }

    /// As [`collect`](Self::collect), but keeping the query's provenance.
    ///
    /// The tier boundary the read ran against is what makes a later
    /// reconciliation possible.
    pub async fn collect_with_provenance(
        self,
    ) -> Result<(Option<StoredReadings>, super::QueryResult)> {
        self.resolve().await
    }

    /// Every delivery in range, unfolded.
    ///
    /// One [`StoredReadings`] per contiguous run of rows agreeing on every
    /// delivery-level column — the audit shape, where `collect` gives the domain
    /// one. A caller reconstructing who reported what wants this; a caller
    /// computing with the register wants `collect`.
    pub async fn deliveries(self) -> Result<Vec<StoredReadings>> {
        Ok(self.scan().await?.0)
    }

    /// The registers this range holds, in OBIS order.
    ///
    /// A meter **is** a set of registers — `1-0:1.8.0` beside its HT and NT
    /// counterparts, a feed-in register beside a consumption one — and a range
    /// carries whichever of them were actually delivered. This is that question,
    /// as a `SELECT DISTINCT` rather than a fold, so it costs one aggregate
    /// instead of decoding a decade of readings.
    ///
    /// Narrowed by everything this builder was narrowed by, including
    /// [`melo`](Self::melo). That matters here more than on a Lastgang: a point
    /// table identifies a reading by its Messlokation, so an unnarrowed list over
    /// a Marktlokation with two meters names the union of both meters' registers.
    ///
    /// It is a set of **registers**, not of readings: two meters carrying the same
    /// register both appear once.
    /// [`collect_by_channel`](Self::collect_by_channel) is what refuses to fold
    /// those together.
    pub async fn channels(&self) -> Result<Vec<ObisCode>> {
        let (conditions, params) = self.predicate();
        let sql = format!(
            r#"SELECT DISTINCT "{obis}" FROM "{table}" WHERE {conditions} ORDER BY 1"#,
            obis = col::OBIS_CODE,
            table = self.store.resolved_table(),
            conditions = conditions.join(" AND "),
        );

        let result = self.store.query_with_params(&sql, params).await?;
        let mut out = Vec::new();
        for batch in result.batches() {
            let codes =
                crate::encode::column::<crate::arrow::array::StringArray>(batch, col::OBIS_CODE)?;
            for i in 0..batch.num_rows() {
                out.push(codes.value(i).parse().map_err(|e| {
                    Error::decode(
                        col::OBIS_CODE,
                        format!("{:?} is not an OBIS code: {e}", codes.value(i)),
                    )
                })?);
            }
        }
        // Sorted **in Rust**, not left to the `ORDER BY`. The column is text, and
        // OBIS codes do not sort as text the way they sort as codes: `1-0:10.8.0`
        // precedes `1-0:2.8.0` alphabetically and follows it numerically. The
        // `ORDER BY` is kept so the batches themselves are deterministic; this is
        // what decides the answer.
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    /// Every register in this range, each resolved on its own.
    ///
    /// What a meter reads, where [`collect`](Self::collect) describes one register
    /// of it — and `collect` must refuse the rest, since [`StoredReadings`] *is*
    /// one register's history and folding two gives advances that alternate
    /// between them.
    ///
    /// **One scan**, split in Rust, so every register is resolved against one tier
    /// boundary rather than N read at N different moments.
    ///
    /// **It still refuses to fold two readings.** A reading is `(register,
    /// merge-key discriminators)`, and a point table keys on the Messlokation by
    /// default — so two meters carrying `1-0:1.8.0` are refused exactly as
    /// `collect` refuses them. Narrow with [`melo`](Self::melo) and the map is one
    /// entry per register again.
    ///
    /// Empty when the range holds nothing, which is the statement `collect`'s
    /// `None` makes.
    pub async fn collect_by_channel(self) -> Result<BTreeMap<ObisCode, StoredReadings>> {
        Ok(self.collect_by_channel_with_provenance().await?.0)
    }

    /// As [`collect_by_channel`](Self::collect_by_channel), but keeping the
    /// query's provenance.
    ///
    /// One [`QueryResult`](super::QueryResult) for the whole map, because it was
    /// one scan.
    pub async fn collect_by_channel_with_provenance(
        self,
    ) -> Result<(BTreeMap<ObisCode, StoredReadings>, super::QueryResult)> {
        let malo_id = self.malo_id;
        let discriminators = self.store.config().discriminator_columns();
        let (stored, result) = self.scan().await?;
        Ok((split_by_register(malo_id, &discriminators, stored)?, result))
    }

    /// The shared read: run the range query, then fold.
    async fn resolve(self) -> Result<(Option<StoredReadings>, super::QueryResult)> {
        let malo_id = self.malo_id;
        let obis = self.obis_code.clone();
        let discriminators = self.store.config().discriminator_columns();
        let (stored, result) = self.scan().await?;
        Ok((
            merge(malo_id, obis.as_deref(), &discriminators, stored)?,
            result,
        ))
    }

    /// The `WHERE` clause this read narrows to, and the values it binds.
    ///
    /// Extracted because three reads share it — the fold, the per-register split
    /// and the register list — and a second copy would be a second place for a
    /// filter to be forgotten. On a point table that is the Messlokation, which
    /// is the one narrowing a Marktlokation with two meters cannot do without.
    ///
    /// **Every value is bound**; only column *names* reach the SQL text, and each
    /// is either a core column or one [`column_eq`](Self::column_eq) checked
    /// against the store's declared set.
    fn predicate(&self) -> (Vec<String>, Vec<ScalarValue>) {
        let mut conditions = vec![format!(r#""{}" = $1"#, col::MALO_ID)];
        let mut params: Vec<ScalarValue> = vec![ScalarValue::Utf8(Some(self.malo_id.to_string()))];

        // Every value is bound; only the column *name* is interpolated, and every
        // name here is either a core column or one checked against the store's
        // declared set (§19.7).
        let mut bind = |sql: &str, value: ScalarValue| {
            params.push(value);
            conditions.push(sql.replace("$?", &format!("${}", params.len())));
        };

        if let Some(melo) = &self.melo_id {
            bind(
                &format!(r#""{}" = $?"#, col::MELO_ID),
                ScalarValue::Utf8(Some(melo.to_string())),
            );
        }
        if let Some(obis) = &self.obis_code {
            bind(
                &format!(r#""{}" = $?"#, col::OBIS_CODE),
                ScalarValue::Utf8(Some(obis.clone())),
            );
        }
        // The instant a register was read is stored in `from`: a reading has no
        // span, so `to` is null and the start column is the whole of its time.
        if let Some(from) = self.from {
            bind(
                &format!(r#""{}" >= $?"#, col::FROM),
                crate::encode::schema::timestamp_scalar(from),
            );
        }
        if let Some(to) = self.to {
            bind(
                &format!(r#""{}" < $?"#, col::FROM),
                crate::encode::schema::timestamp_scalar(to),
            );
        }
        for (name, value) in &self.filters {
            bind(&format!(r#""{name}" = $?"#), value.clone());
        }
        if !self.quality.is_empty() {
            let start = params.len() + 1;
            let placeholders = (0..self.quality.len())
                .map(|i| format!("${}", start + i))
                .collect::<Vec<_>>()
                .join(", ");
            conditions.push(format!(r#""{}" IN ({placeholders})"#, col::QUALITY));
            for q in &self.quality {
                params.push(ScalarValue::Utf8(Some(q.as_str().to_owned())));
            }
        }

        (conditions, params)
    }

    /// Run the range query and decode it, without folding.
    async fn scan(&self) -> Result<(Vec<StoredReadings>, super::QueryResult)> {
        let (conditions, params) = self.predicate();
        let merge_key = self.store.config().merge_key();
        // Ordered by the **whole** merge key so decoding sees contiguous runs of
        // one delivery. A point table identifies a reading by its Messlokation, so
        // two meters produce two rows per instant: ordered without it they
        // interleave, every run is one row long, and a day of one meter comes back
        // as ninety-six deliveries of one reading.
        let tail = match self.latest_only {
            true => format!(
                r#"ORDER BY "{at}" DESC, {rest} LIMIT 1"#,
                at = col::FROM,
                rest = super::series::key_order_without_start(&merge_key),
            ),
            false => format!("ORDER BY {}", super::series::merge_key_order(&merge_key)),
        };

        let sql = format!(
            r#"SELECT * FROM "{table}" WHERE {conditions} {tail}"#,
            table = self.store.resolved_table(),
            conditions = conditions.join(" AND "),
        );

        let result = self.store.query_with_params(&sql, params).await?;
        let mut stored = Vec::new();
        for batch in result.batches() {
            stored.extend(crate::encode::readings_from_record_batch(batch)?);
        }
        Ok((stored, result))
    }
}

/// The columns a read may be narrowed by, beyond the ones with their own method.
fn filterable_columns(store: &crate::session::MeterStore) -> Vec<String> {
    let mut accepted: Vec<String> = store
        .config()
        .extra_columns()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    for column in store.config().discriminator_columns() {
        if column != col::MELO_ID && !accepted.contains(&column) {
            accepted.push(column);
        }
    }
    accepted
}

/// Fold decoded deliveries into one.
///
/// The counterpart of [`series::merge`](super::series), and it refuses the same
/// thing for a sharper reason: two meters' registers folded together produce
/// advances that alternate between them, so differencing the result is not a
/// wrong number but a meaningless one.
/// Group decoded deliveries by register and fold each group on its own.
///
/// The split half of [`ReadingsQuery::collect_by_channel`], separated so the
/// property that matters can be checked without a database: two registers split,
/// and two *readings* within one register still refuse.
///
/// Grouped rather than run-detected, for the reason the series path gives: the
/// scan's order already makes each register contiguous, and relying on that would
/// make the split silently wrong the day the order changes.
fn split_by_register(
    malo_id: MaloId,
    discriminators: &[String],
    stored: Vec<StoredReadings>,
) -> Result<BTreeMap<ObisCode, StoredReadings>> {
    let mut grouped: BTreeMap<ObisCode, Vec<StoredReadings>> = BTreeMap::new();
    for delivery in stored {
        grouped
            .entry(delivery.obis_code)
            .or_default()
            .push(delivery);
    }

    let mut out = BTreeMap::new();
    for (register, deliveries) in grouped {
        // `merge` runs the same refusal `collect` does. Within one register it can
        // only fire on differing discriminators — a second Messlokation, a tenant
        // — which is the half that still means two readings.
        if let Some(folded) = merge(
            malo_id,
            Some(&register.to_string()),
            discriminators,
            deliveries,
        )? {
            out.insert(register, folded);
        }
    }
    Ok(out)
}

fn merge(
    malo_id: MaloId,
    obis_code: Option<&str>,
    discriminators: &[String],
    mut stored: Vec<StoredReadings>,
) -> Result<Option<StoredReadings>> {
    if stored.is_empty() {
        return Ok(None);
    }
    refuse_mixed_registers(&stored, discriminators)?;

    // Newest delivery last, so the fields taken below are the current ones.
    stored.sort_by_key(|s| s.recorded_at);

    let mut readings: Vec<MeterReading> = Vec::new();
    let mut melo_id: Option<MeloId> = None;
    let mut cadence: Option<IntervalResolution> = None;
    let mut provenance: Vec<ProvenanceEntry> = Vec::new();
    let mut source: Option<MeasurementSource> = None;
    let mut recorded_at: Option<OffsetDateTime> = None;
    let mut sparte = None;
    let mut unit = None;
    let mut version = None;
    let mut extra = BTreeMap::new();
    let mut obis = None;

    for delivery in stored {
        readings.extend(delivery.readings);
        if delivery.melo_id.is_some() {
            melo_id = delivery.melo_id;
        }
        if delivery.cadence.is_some() {
            cadence = delivery.cadence;
        }
        provenance.extend(delivery.provenance);
        obis = Some(delivery.obis_code);
        source = Some(delivery.source);
        recorded_at = Some(delivery.recorded_at);
        sparte = Some(delivery.sparte);
        unit = Some(delivery.unit);
        version = Some(delivery.version);
        extra = delivery.extra;
    }

    readings.sort_by_key(|r| r.at);
    refuse_repeated_instants(malo_id, &readings)?;

    let (Some(source), Some(recorded_at), Some(sparte), Some(unit), Some(obis), Some(version)) =
        (source, recorded_at, sparte, unit, obis, version)
    else {
        return Ok(None);
    };
    // An explicit `.obis(..)` is authoritative; without one the deliveries agreed
    // on a channel because `refuse_mixed_registers` checked that they did.
    let obis = match obis_code {
        Some(code) => code.parse().map_err(|e| {
            Error::decode(col::OBIS_CODE, format!("{code:?} is not an OBIS code: {e}"))
        })?,
        None => obis,
    };

    let mut out = StoredReadings::new(
        malo_id,
        obis,
        sparte,
        readings,
        source,
        // The version axis is resolved away by the time rows reach here, so this
        // is a label rather than a comparison: the newest contributing delivery's
        // version is the one describing the values that survived resolution.
        version,
        recorded_at,
    )
    .in_unit(unit);
    out.melo_id = melo_id;
    out.cadence = cadence;
    if !provenance.is_empty() {
        out.provenance = provenance;
    }
    out.extra = extra;
    Ok(Some(out))
}

/// Refuse to fold deliveries that are not one register.
/// Refuse a folded register history that holds two values at one instant.
///
/// The Zählerstandsgang counterpart of the interval path's check, and it matters
/// more: a register history is *differenced* to get consumption, so an arbitrary
/// one of the two values lands on both sides of a subtraction and the figure
/// between two reads means nothing. `metering::consumption_between` cannot
/// notice.
///
/// [`refuse_mixed_registers`] cannot see this case — see the interval path's
/// `refuse_repeated_instants` for why.
fn refuse_repeated_instants(
    malo_id: MaloId,
    readings: &[metering::reading::MeterReading],
) -> Result<()> {
    // Sorted by `at` on the way in, so a repeat is adjacent.
    for pair in readings.windows(2) {
        if pair[0].at != pair[1].at {
            continue;
        }
        return Err(Error::InvariantViolated {
            table: malo_id.to_string(),
            detail: format!(
                "two register readings survived resolution for one meter at {at}: \
                 {a} and {b}. Resolution partitions by the merge key and version_scope, \
                 so the cause is almost always two network operators for one reading — \
                 which both write paths refuse, unless integrity constraints are off or \
                 something other than meterstore wrote these rows. Differencing them \
                 would produce a consumption figure that means nothing",
                at = pair[0].at,
                a = pair[0].value,
                b = pair[1].value,
            ),
        });
    }
    Ok(())
}

fn refuse_mixed_registers(stored: &[StoredReadings], discriminators: &[String]) -> Result<()> {
    let key_of = |s: &StoredReadings| -> Result<(String, Vec<(String, String)>)> {
        Ok((
            s.obis_code.to_string(),
            crate::session::store::discriminator_values(
                s.melo_id.as_ref(),
                &s.extra,
                discriminators,
            )?,
        ))
    };

    let first = key_of(&stored[0])?;
    for other in &stored[1..] {
        let key = key_of(other)?;
        if key == first {
            continue;
        }
        let differs = match first.0 == key.0 {
            false => format!("two registers ({} and {})", first.0, key.0),
            true => format!("two meters ({} and {})", render(&first.1), render(&key.1)),
        };
        return Err(Error::config(format!(
            "{malo} spans {differs} over this range, and a Zählerstandsgang describes one — \
             folding them interleaves two cumulative sequences, so differencing the result \
             produces advances that belong to neither meter. Narrow the read with \
             .obis(..), .melo(..) or .column_eq(..), or scope the session",
            malo = stored[0].malo_id,
        )));
    }
    Ok(())
}

/// Render a merge-key discriminator tuple for an error message.
fn render(values: &[(String, String)]) -> String {
    match values.is_empty() {
        true => "<none>".to_string(),
        false => values
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", "),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::{ScopedVersion, Version, VersionScope};
    use metering::interval::Sparte;
    use metering::reading::MeterReading;
    use rust_decimal::Decimal;
    use time::macros::datetime;

    fn malo() -> MaloId {
        "12345678905".parse().expect("a valid MaLo-ID")
    }

    fn source() -> MeasurementSource {
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: None,
            sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
        }
    }

    /// One register reading of the given register, at a fixed instant.
    ///
    /// The instant is shared on purpose: two cumulative values at one timestamp
    /// is exactly what makes a fold meaningless — differencing the result gives
    /// advances that alternate between two registers.
    fn one_reading(obis: &str) -> StoredReadings {
        let at = datetime!(2026-07-20 00:00 UTC);
        let code = obis.parse().expect("a valid OBIS code");
        StoredReadings::new(
            malo(),
            code,
            Sparte::Strom,
            vec![MeterReading {
                at,
                value: Decimal::new(1_234, 0),
                quality: metering::QualityFlag::Measured,
                obis_code: Some(code),
            }],
            source(),
            ScopedVersion::new(
                VersionScope::for_interval("9900000000001", at, Sparte::Strom).unwrap(),
                Version::new(20_260_720_000_001).unwrap(),
            ),
            at,
        )
    }

    #[test]
    fn two_register_values_at_one_instant_are_refused_rather_than_differenced() {
        // Worse here than on the interval path. A register history is
        // *differenced* to get consumption, so two values at one instant do not
        // merely double a sum — an arbitrary one of the two lands on both sides
        // of a subtraction and the consumption between two reads becomes a number
        // with no relationship to anything. `refuse_mixed_registers` cannot see
        // it: resolution partitions by the merge key *and* `version_scope`, so
        // two operators for one reading leave two winners that agree on
        // everything it looks at.
        let mut second = one_reading("1-0:1.8.0");
        second.readings[0].value = Decimal::new(9_999, 0);
        second.recorded_at += time::Duration::hours(1);

        let err = merge(malo(), None, &[], vec![one_reading("1-0:1.8.0"), second]).unwrap_err();

        assert!(
            matches!(err, Error::InvariantViolated { .. }),
            "stored data that should not exist, not a delivery being refused: {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("version_scope"), "{msg}");
        assert!(msg.contains("1234") && msg.contains("9999"), "both: {msg}");
    }

    #[test]
    fn a_register_read_at_successive_instants_still_folds() {
        // The guard must not fire on what the fold exists for: a register read
        // twice, which is the whole of a Zählerstandsgang.
        let first = one_reading("1-0:1.8.0");
        let mut later = one_reading("1-0:1.8.0");
        later.readings[0].at += time::Duration::minutes(15);
        later.readings[0].value = Decimal::new(1_240, 0);
        later.recorded_at += time::Duration::hours(1);

        let folded = merge(malo(), None, &[], vec![first, later])
            .unwrap()
            .expect("one history");
        assert_eq!(folded.readings.len(), 2);
    }

    #[test]
    fn a_per_register_read_splits_what_the_fold_refuses() {
        // A meter *is* a set of registers, and "what does this meter read" is a
        // question about all of them. `collect` cannot answer it — StoredReadings
        // holds one register — so this is the shape that can.
        let split = split_by_register(
            malo(),
            &[],
            vec![one_reading("1-0:1.8.1"), one_reading("1-0:1.8.2")],
        )
        .expect("two registers are two histories, not an error");

        assert_eq!(split.len(), 2);
        for code in ["1-0:1.8.1", "1-0:1.8.2"] {
            let register: ObisCode = code.parse().unwrap();
            assert_eq!(split[&register].obis_code, register);
            assert_eq!(split[&register].readings.len(), 1);
        }
    }

    #[test]
    fn a_per_register_read_still_refuses_two_meters_on_one_register() {
        // A point table identifies a reading by its Messlokation by default, so
        // two meters under one Marktlokation carrying `1-0:1.8.0` are two
        // readings. Splitting by register alone would fold them, and differencing
        // the fold produces advances that alternate between meters.
        let a = one_reading("1-0:1.8.0")
            .with_melo_id("DE0001112223334445556667778889990".parse().unwrap());
        let b = one_reading("1-0:1.8.0")
            .with_melo_id("DE0009998887776665554443332221110".parse().unwrap());

        let err = split_by_register(malo(), &[col::MELO_ID.to_string()], vec![a, b])
            .expect_err("two meters on one register are two readings");
        let msg = err.to_string();
        assert!(msg.contains("two meters"), "{msg}");
        assert!(msg.contains(".melo(..)"), "the fix has to be named: {msg}");
    }

    #[test]
    fn a_per_register_read_of_nothing_is_an_empty_map() {
        assert!(
            split_by_register(malo(), &[], Vec::new())
                .unwrap()
                .is_empty()
        );
    }
}
