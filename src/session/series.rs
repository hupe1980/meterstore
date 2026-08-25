//! The typed read path: a query in, a `metering` type out.
//!
//! The unit of work is [`MeasurementSeries`], because that is what the domain
//! layer computes with. A caller that wants `aggregate(&series.intervals, …)`
//! should not have to decode Arrow to get there, and one that wants a `DataFrame`
//! should not have to go through this.
//!
//! There is nothing to derive and nothing to declare (§13.6). The row type is
//! `metering`'s, so a derive macro could not be implemented for it anyway, and
//! per-deployment columns are configuration rather than fields.
//!
//! # Values never reach the SQL text
//!
//! `malo_id` and `obis_code` are caller-supplied, so they are bound as
//! parameters. The OBIS code is canonicalised on the way in for a second reason:
//! it is part of the merge key, so `1-0:1.8.0` and `1-0:1.8.0*255` are the same
//! channel and a literal comparison against the stored spelling would silently
//! return nothing (§7.1.2).

use std::collections::BTreeMap;

use datafusion::common::ScalarValue;
use metering::ids::{MaloId, MeloId};
use metering::interval::QualityFlag;
use metering::interval::Sparte;
use metering::measurement_series::{MeasurementSeries, MeasurementSource, ProvenanceEntry};
use metering::obis::ObisCode;
use time::OffsetDateTime;

use crate::encode::schema::col;
use crate::error::{Error, Result};

/// A version-resolved series with everything the storage layer knows about it
/// that a [`MeasurementSeries`] cannot carry on its own.
///
/// Returned by [`SeriesQuery::collect_resolved`]. `MeasurementSeries` is a channel
/// of numbers, so the commodity and the deployment's declared attribute/identity
/// columns (tenant, reporting party, ingestion source, allocation version, …) ride
/// alongside it here — folded from the newest contributing delivery — rather than
/// being dropped and reconstructed with guessed defaults.
#[derive(Debug, Clone)]
pub struct ResolvedSeries {
    /// The commodity the measuring point meters.
    pub sparte: Sparte,
    /// Values of the deployment's declared extra columns, keyed by column name.
    pub extra: BTreeMap<String, ScalarValue>,
    /// The version-resolved interval series.
    pub series: MeasurementSeries,
}

/// A read of one channel, built up and then collected.
///
/// Obtained from [`MeterStore::series`](crate::session::MeterStore::series).
#[derive(Debug, Clone)]
pub struct SeriesQuery<'a> {
    store: &'a crate::session::MeterStore,
    malo_id: MaloId,
    obis_code: Option<String>,
    from: Option<OffsetDateTime>,
    to: Option<OffsetDateTime>,
    filters: Vec<(String, ScalarValue)>,
    quality: Vec<QualityFlag>,
    latest_only: bool,
}

impl<'a> SeriesQuery<'a> {
    /// Start a read for one measuring point.
    pub(crate) fn new(store: &'a crate::session::MeterStore, malo_id: MaloId) -> Self {
        Self {
            store,
            malo_id,
            obis_code: None,
            from: None,
            to: None,
            filters: Vec::new(),
            quality: Vec::new(),
            latest_only: false,
        }
    }

    /// Restrict to rows whose declared column `name` equals `value`.
    ///
    /// A measuring point is only unique **within** the columns that join the merge
    /// key. A deployment that declares an identity column — a tenant, a reporting
    /// party — stores two parties' readings for one MaLo as distinct rows, but an
    /// unscoped [`series`](crate::session::MeterStore::series) read spans them and
    /// folds them into a single series (§4.2). Naming the identity that scopes the
    /// read keeps them apart. Repeatable: each call adds one equality predicate,
    /// and the value is bound as a parameter, never concatenated into the SQL.
    ///
    /// The **name** cannot be a parameter — no SQL dialect parameterises an
    /// identifier — so it is checked against the store's declared columns rather
    /// than interpolated on trust. An unknown name is refused here, naming what is
    /// available, instead of reaching the engine as a fragment of SQL.
    pub fn column_eq(mut self, name: &str, value: ScalarValue) -> Result<Self> {
        // Declared columns, plus every merge-key column beyond the core three —
        // which is the same list except on a table that identifies a reading by
        // its Messlokation, where `melo_id` is a *core* column doing an identity
        // column's job. Refusing it there would leave the one read that needs
        // separating (two meters under one Marktlokation) unable to ask for it.
        let mut accepted: Vec<String> = self
            .store
            .config()
            .extra_columns()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        for column in self.store.config().discriminator_columns() {
            if !accepted.contains(&column) {
                accepted.push(column);
            }
        }

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

    /// Restrict to intervals whose resolved quality is one of `flags`.
    ///
    /// A read filter on the core `quality` column, pushed into the scan rather than
    /// applied by the caller after materialising the whole series. This is the one
    /// predicate every billing-facing caller writes — "exclude `FAULTY`/`UNKNOWN`",
    /// or "measured only" — and hand-rolling it per call site invites drift, so it
    /// lives here once. Quality is matched **after** version resolution, so it sees
    /// the value currently in force, not a superseded one. Passing an empty slice
    /// is a no-op (no filter). Repeatable: further calls extend the accepted set.
    #[must_use]
    pub fn quality_in(mut self, flags: &[QualityFlag]) -> Self {
        self.quality.extend_from_slice(flags);
        self
    }

    /// Restrict to one channel.
    ///
    /// Canonicalised immediately, so a caller may pass whichever spelling they
    /// hold. Omitting it reads every channel of the measuring point, which is
    /// only useful when the caller intends to split them afterwards.
    pub fn obis(mut self, obis_code: &str) -> Result<Self> {
        self.obis_code = Some(crate::encode::canonical_obis(obis_code)?);
        Ok(self)
    }

    /// Restrict to a half-open interval range `[from, to)`.
    ///
    /// **Strongly recommended.** Without it both tiers are scanned in full, which
    /// over a multi-billion-row history is almost always an accident.
    pub fn range(mut self, from: OffsetDateTime, to: OffsetDateTime) -> Self {
        self.from = Some(from);
        self.to = Some(to);
        self
    }

    /// Read from `from` onwards.
    pub fn since(mut self, from: OffsetDateTime) -> Self {
        self.from = Some(from);
        self
    }

    /// Read up to, but excluding, `to`.
    pub fn until(mut self, to: OffsetDateTime) -> Self {
        self.to = Some(to);
        self
    }

    /// Run the read and return the domain type.
    ///
    /// Rows arrive version-resolved, so each interval appears once carrying the
    /// value currently in force, ordered by start.
    ///
    /// **`None` means the range holds no rows**, and that is deliberately not an
    /// empty series. A `MeasurementSeries` asserts a `source` — who reported these
    /// values — and with no values there is nobody to name. Fabricating one would
    /// put a source in the audit trail that never delivered anything. Absence is
    /// information (P1); use [`intervals`](Self::intervals) when the caller
    /// genuinely wants to treat it as zero, or
    /// [`completeness`](crate::session::MeterStore::completeness) to find out why
    /// it is absent.
    pub async fn collect(self) -> Result<Option<MeasurementSeries>> {
        Ok(self.collect_with_provenance().await?.0)
    }

    /// As [`collect`](Self::collect), but also naming the commodity.
    ///
    /// A [`MeasurementSeries`] is deliberately a channel of numbers and carries no
    /// Sparte — the commodity is a property of the measuring point, held at the
    /// storage layer (see [`StoredSeries::sparte`](crate::encode::StoredSeries::sparte)).
    /// A caller reconstructing domain reads still needs it, though: a gas series
    /// read back with the Sparte forgotten is indistinguishable from electricity.
    /// This returns it from the stored rows rather than making the caller guess.
    ///
    /// `None` has the same meaning as in [`collect`](Self::collect): the range
    /// held no rows.
    pub async fn collect_with_sparte(self) -> Result<Option<(Sparte, MeasurementSeries)>> {
        Ok(self.resolve().await?.0.map(|r| (r.sparte, r.series)))
    }

    /// As [`collect`](Self::collect), but also returning the commodity **and** the
    /// values of the deployment's declared attribute/identity columns for the
    /// series (tenant, reporting party, ingestion source, …), taken from the newest
    /// contributing delivery.
    ///
    /// This is the read counterpart to [`StoredSeries::with_extra`]: a
    /// [`MeasurementSeries`] is a channel of numbers and carries none of these, so
    /// a caller reconstructing richer domain rows recovers them here instead of
    /// hard-coding defaults. `None` means the range held no rows.
    ///
    /// [`StoredSeries::with_extra`]: crate::encode::StoredSeries::with_extra
    pub async fn collect_resolved(self) -> Result<Option<ResolvedSeries>> {
        Ok(self.resolve().await?.0)
    }

    /// The single most recent interval, or `None` when the range holds none.
    ///
    /// "What is the current reading" is not a whole-history question, so this
    /// resolves it with `ORDER BY from DESC LIMIT 1` at the storage layer rather
    /// than folding the entire series and taking the maximum in memory — an
    /// unbounded scan to return one row. With no [`range`](Self::range) it is the
    /// newest interval ever stored; with one, the newest inside it.
    ///
    /// **It spans channels, meters and identity values** unless narrowed with
    /// [`obis`](Self::obis) or [`column_eq`](Self::column_eq) — a measuring point
    /// reporting import and export has two rows at the newest instant, and so
    /// does a table keyed by Messlokation or extended with a tenant. The rest of
    /// the merge key completes the order so the choice among them is
    /// deterministic, but it is still a choice.
    pub async fn latest(self) -> Result<Option<metering::interval::MeterInterval>> {
        Ok(self
            .latest_resolved()
            .await?
            .and_then(|r| r.series.intervals.into_iter().next_back()))
    }

    /// As [`latest`](Self::latest), but also returning the commodity and the
    /// declared attribute/identity columns for that interval (see
    /// [`collect_resolved`](Self::collect_resolved)).
    pub async fn latest_resolved(mut self) -> Result<Option<ResolvedSeries>> {
        self.latest_only = true;
        Ok(self.resolve().await?.0)
    }

    /// Just the intervals, empty when the range holds none.
    ///
    /// The shape `metering`'s computations actually take —
    /// `aggregate(&intervals, …)` — and the one where an empty range is an
    /// ordinary answer rather than a missing series.
    pub async fn intervals(self) -> Result<Vec<metering::interval::MeterInterval>> {
        Ok(self
            .collect()
            .await?
            .map(|s| s.intervals)
            .unwrap_or_default())
    }

    /// As [`collect`](Self::collect), but keeping the query's provenance.
    ///
    /// The tier boundary the read ran against is what makes a later
    /// reconciliation possible, so it is available without going through SQL.
    pub async fn collect_with_provenance(
        self,
    ) -> Result<(Option<MeasurementSeries>, super::QueryResult)> {
        let (resolved, result) = self.resolve().await?;
        Ok((resolved.map(|r| r.series), result))
    }

    /// The shared read: run the range query and fold the rows into one series,
    /// keeping the commodity, the declared attribute columns, and the tier-boundary
    /// provenance the public collectors each project a subset of.
    async fn resolve(self) -> Result<(Option<ResolvedSeries>, super::QueryResult)> {
        let mut conditions = vec![format!(r#""{}" = $1"#, col::MALO_ID)];
        let mut params: Vec<ScalarValue> = vec![ScalarValue::Utf8(Some(self.malo_id.to_string()))];

        if let Some(obis) = &self.obis_code {
            conditions.push(format!(r#""{}" = ${}"#, col::OBIS_CODE, params.len() + 1));
            params.push(ScalarValue::Utf8(Some(obis.clone())));
        }
        if let Some(from) = self.from {
            conditions.push(format!(r#""{}" >= ${}"#, col::FROM, params.len() + 1));
            params.push(crate::encode::schema::timestamp_scalar(from));
        }
        if let Some(to) = self.to {
            conditions.push(format!(r#""{}" < ${}"#, col::FROM, params.len() + 1));
            params.push(crate::encode::schema::timestamp_scalar(to));
        }
        // Identity/extra-column scoping (e.g. tenant), so a shared store does not
        // fold two parties' readings for one MaLo into a single series.
        for (name, value) in &self.filters {
            conditions.push(format!(r#""{}" = ${}"#, name, params.len() + 1));
            params.push(value.clone());
        }
        // Quality filter on the core `quality` column, applied over the resolved
        // relation so it sees the value in force. Codes are `metering`'s stable
        // strings and are bound as parameters, never concatenated.
        if !self.quality.is_empty() {
            let placeholders = (0..self.quality.len())
                .map(|i| format!("${}", params.len() + 1 + i))
                .collect::<Vec<_>>()
                .join(", ");
            conditions.push(format!(r#""{}" IN ({placeholders})"#, col::QUALITY));
            for q in &self.quality {
                params.push(ScalarValue::Utf8(Some(q.as_str().to_owned())));
            }
        }

        // Ordered by the **whole** merge key so decoding sees contiguous runs of
        // one series, which is what `from_record_batch` groups on. Not by
        // `(malo_id, obis_code, from)`: a table with an identity column — or one
        // that identifies a reading by its Messlokation — stores several rows
        // per interval, and ordering that leaves them interleaved breaks a run
        // at every row, so a day comes back as ninety-six one-interval groups.
        // Correct, and useless.
        //
        // A `latest` read inverts to newest-first and takes a single interval,
        // so the whole history need not be scanned to answer "what is the
        // current reading".
        let tail = if self.latest_only {
            // Newest first, then the rest of the merge key — a **total** order,
            // for the same reason the published resolution SQL breaks its ties:
            // `"from" DESC LIMIT 1` alone leaves the winner to whichever row the
            // plan happened to produce first whenever the newest instant carries
            // more than one row. It does exactly that on the reads most likely to
            // want it — a measuring point with two channels, a table keyed by
            // Messlokation, a tenant-extended key — so "the current reading"
            // could differ between two identical calls. Narrowing with
            // `obis`/`column_eq` is the way to *choose* a row; the tie-break is
            // what makes the unnarrowed answer reproducible.
            format!(
                r#"ORDER BY "{from}" DESC, {rest} LIMIT 1"#,
                from = col::FROM,
                rest = key_order_without_start(&self.store.config().merge_key()),
            )
        } else {
            format!(
                "ORDER BY {}",
                merge_key_order(&self.store.config().merge_key()),
            )
        };
        let sql = format!(
            r#"SELECT * FROM "{table}" WHERE {conditions} {tail}"#,
            table = self.store.resolved_table(),
            conditions = conditions.join(" AND "),
        );

        let result = self.store.query_with_params(&sql, params).await?;

        let mut stored = Vec::new();
        for batch in result.batches() {
            stored.extend(crate::encode::from_record_batch(batch)?);
        }

        Ok((
            merge(
                self.malo_id,
                self.obis_code.as_deref(),
                &self.store.config().discriminator_columns(),
                stored,
            )?,
            result,
        ))
    }
}

/// An `ORDER BY` list over a merge key, ending on the interval start.
///
/// `from` is moved to the end wherever it sits in the key: a run is a series,
/// and a series is contiguous in time. Everything before it is what separates
/// one series from another.
pub(crate) fn merge_key_order(merge_key: &[String]) -> String {
    merge_key
        .iter()
        .filter(|c| c.as_str() != col::FROM)
        .map(|c| format!("\"{c}\""))
        .chain(std::iter::once(format!("\"{}\"", col::FROM)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The merge key with the interval start removed, as an `ORDER BY` list.
///
/// The tie-break for a `latest` read, which has already ordered on `from`.
/// Never empty: `malo_id` is in every merge key and is not the start.
pub(crate) fn key_order_without_start(merge_key: &[String]) -> String {
    merge_key
        .iter()
        .filter(|c| c.as_str() != col::FROM)
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Fold decoded rows into one series.
///
/// Decoding groups rows by the identity it can see in the batch, which splits a
/// channel wherever the version or the delivery changed. A caller asked for a
/// *series*, so those groups are folded back together — the version axis has
/// already been resolved away by the time rows reach here, so there is exactly
/// one row per interval and no risk of double-counting in the fold.
///
/// The series-level fields come from the newest contributing delivery, because
/// that is the one whose `source` and `provenance` describe the values that
/// survived resolution.
fn merge(
    malo_id: MaloId,
    obis_code: Option<&str>,
    discriminators: &[String],
    mut stored: Vec<crate::encode::StoredSeries>,
) -> Result<Option<ResolvedSeries>> {
    if stored.is_empty() {
        return Ok(None);
    }

    refuse_mixed_readings(&stored, discriminators)?;

    // Newest delivery last, so the fields taken below are the current ones.
    stored.sort_by_key(|s| s.recorded_at);

    let mut intervals = Vec::new();
    let mut source: Option<MeasurementSource> = None;
    let mut provenance: Vec<ProvenanceEntry> = Vec::new();
    let mut melo_id: Option<MeloId> = None;
    let mut resolution = None;
    let mut recorded_at: Option<OffsetDateTime> = None;
    // A measuring point has one commodity, so every delivery agrees on it; taking
    // the newest keeps it consistent with the other fields above if they ever do
    // not.
    let mut sparte: Option<Sparte> = None;
    // The deployment's declared attribute/identity columns (e.g. tenant, source,
    // reporting party). Folded from the newest delivery like every field above, so
    // a caller reconstructing domain rows can recover them instead of guessing.
    let mut extra: BTreeMap<String, ScalarValue> = BTreeMap::new();

    for series in stored {
        intervals.extend(series.series.intervals);
        source = Some(series.series.source);
        provenance.extend(series.series.provenance);
        if series.series.melo_id.is_some() {
            melo_id = series.series.melo_id;
        }
        if series.series.resolution.is_some() {
            resolution = series.series.resolution;
        }
        recorded_at = Some(series.recorded_at);
        sparte = Some(series.sparte);
        extra = series.extra;
    }

    intervals.sort_by_key(|i| i.from);

    let obis = match obis_code {
        Some(code) => Some(code.parse::<ObisCode>().map_err(|e| {
            Error::decode(col::OBIS_CODE, format!("{code:?} is not an OBIS code: {e}"))
        })?),
        // Unfiltered reads may span channels, so the series-level code is only
        // set when every interval agrees on one. Claiming a single channel for a
        // mixed series would misdescribe it.
        None => single_channel(&intervals),
    };

    // `MeasurementSeries::new` takes the ingestion time rather than reading a
    // clock, so a series read twice is byte-identical. Both unwraps are backed by
    // the non-empty check above: at least one delivery contributed.
    let (Some(source), Some(recorded_at), Some(sparte)) = (source, recorded_at, sparte) else {
        return Ok(None);
    };

    let mut series = MeasurementSeries::new(malo_id, obis, intervals, source, recorded_at);
    series.melo_id = melo_id;
    series.resolution = resolution;
    // `new` seeds a provenance entry of its own; the stored trail is the record
    // that matters, so it replaces rather than extends it.
    if !provenance.is_empty() {
        series.provenance = provenance;
    }
    Ok(Some(ResolvedSeries {
        sparte,
        extra,
        series,
    }))
}

/// The channel and the merge-key discriminators one decoded group carries — what
/// makes two groups two *readings* rather than two deliveries of one.
type ReadingKey = (Option<String>, Vec<(String, String)>);

/// Refuse to fold rows that are not one series.
///
/// Version resolution leaves one row per `(merge key, from)`, so the fold below
/// is lossless exactly while the rows share a merge key. Where they do not there
/// are two intervals at one instant, which `MeasurementSeries` cannot express —
/// `metering::aggregate` sums both and the month doubles.
///
/// Two ordinary queries reach that: a meter reporting import *and* export, whose
/// two OBIS codes a type holding one `obis_code` cannot describe; and two
/// readings that are not the same reading — a tenant discriminator, a second
/// Messlokation — where the fold puts one party's data in another's series.
///
/// The fix is naming which was meant, so the error names it.
fn refuse_mixed_readings(
    stored: &[crate::encode::StoredSeries],
    discriminators: &[String],
) -> Result<()> {
    let key_of = |s: &crate::encode::StoredSeries| -> Result<ReadingKey> {
        Ok((
            s.series.obis_code.map(|c| c.to_string()),
            crate::session::store::discriminator_values(
                s.series.melo_id.as_ref(),
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

        let (channel, identity) = (&first.0, &first.1);
        // The channel first: it is the difference a caller is likelier to have
        // meant, and `.obis(..)` is the narrower fix.
        let differs = if channel != &key.0 {
            format!(
                "two channels ({} and {})",
                channel.as_deref().unwrap_or("<none>"),
                key.0.as_deref().unwrap_or("<none>"),
            )
        } else {
            format!("two readings ({} and {})", render(identity), render(&key.1))
        };

        return Err(Error::config(format!(
            "{malo} spans {differs} over this range, and a MeasurementSeries can only \
             describe one — folding them puts two values at the same instant into one \
             series, which sums to twice the truth with nothing to notice it. Narrow the \
             read with .obis(..) or .column_eq(..), or scope the session",
            malo = stored[0].series.malo_id,
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

/// The one channel every interval shares, if they do share one.
fn single_channel(intervals: &[metering::interval::MeterInterval]) -> Option<ObisCode> {
    let mut seen: Option<ObisCode> = None;
    for interval in intervals {
        match (seen, interval.obis_code) {
            (_, None) => return None,
            (None, Some(code)) => seen = Some(code),
            (Some(a), Some(b)) if a == b => {}
            _ => return None,
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode::StoredSeries;
    use crate::version::{ScopedVersion, Version, VersionScope};
    use metering::interval::{MeterInterval, QualityFlag};
    use rust_decimal::Decimal;
    use time::macros::datetime;

    fn interval(from: OffsetDateTime, kwh: i64) -> MeterInterval {
        MeterInterval {
            from,
            to: from + time::Duration::minutes(15),
            value: Decimal::new(kwh, 0),
            quality: QualityFlag::Measured,
            obis_code: Some("1-0:1.8.0".parse().unwrap()),
        }
    }

    /// The MaLo-ID these tests read: check digit included, because `MaloId`
    /// verifies it and eleven arbitrary digits would not parse.
    fn malo() -> MaloId {
        "12345678905".parse().unwrap()
    }

    fn source() -> MeasurementSource {
        MeasurementSource::Mscons {
            pid: 13_005,
            message_ref: Some("MSG-1".to_owned()),
            sender_mp_id: "9900000000001".to_owned(),
        }
    }

    fn stored(intervals: Vec<MeterInterval>, recorded_at: OffsetDateTime) -> StoredSeries {
        let scope = VersionScope::for_interval("99", intervals[0].from, Sparte::Strom).unwrap();
        StoredSeries::new(
            MeasurementSeries::new(
                malo(),
                Some("1-0:1.8.0".parse().unwrap()),
                intervals,
                source(),
                recorded_at,
            ),
            ScopedVersion::new(scope, Version::new(20_260_701_000_001).unwrap()),
            recorded_at,
        )
    }

    /// One interval of the given channel, at a fixed instant.
    ///
    /// The instant is shared on purpose: two rows at one instant is exactly what
    /// makes a fold a double-count.
    fn one_interval(obis: &str) -> StoredSeries {
        let from = datetime!(2026-07-20 00:00 UTC);
        let mut s = stored(
            vec![MeterInterval {
                from,
                to: from + time::Duration::minutes(15),
                value: rust_decimal::Decimal::new(15, 1),
                quality: metering::QualityFlag::Measured,
                obis_code: obis.parse().ok(),
            }],
            from,
        );
        s.series.obis_code = obis.parse().ok();
        s
    }

    #[test]
    fn the_latest_read_orders_totally() {
        // `"from" DESC LIMIT 1` alone is not a total order, and the reads most
        // likely to want "the current reading" are exactly the ones where the
        // newest instant carries more than one row: a measuring point with two
        // channels, a table keyed by Messlokation, a tenant-extended key. Two
        // identical calls could then return different rows. The published
        // resolution SQL breaks its ties for the same reason.
        let key = vec![
            col::MALO_ID.to_string(),
            col::MELO_ID.to_string(),
            col::OBIS_CODE.to_string(),
            col::FROM.to_string(),
            "tenant".to_string(),
        ];
        let rest = key_order_without_start(&key);

        assert_eq!(
            rest, r#""malo_id", "melo_id", "obis_code", "tenant""#,
            "every merge-key column but the start, in key order"
        );
        assert!(!rest.contains(col::FROM), "the read already ordered on it");

        // Never empty: `malo_id` is in every merge key.
        assert!(
            !key_order_without_start(&[col::MALO_ID.to_string(), col::FROM.to_string(),])
                .is_empty()
        );
    }

    #[test]
    fn folding_two_channels_into_one_series_is_refused() {
        // A meter reporting import and export carries two OBIS codes at the same
        // instants. `MeasurementSeries` holds one `obis_code`, so a folded pair
        // is a value the type cannot describe — and `metering::aggregate` sums
        // both, returning import plus export under the heading of consumption.
        let mut export = one_interval("1-0:2.8.0");
        export.recorded_at += time::Duration::hours(1);

        let err = merge(malo(), None, &[], vec![one_interval("1-0:1.8.0"), export])
            .unwrap_err()
            .to_string();

        assert!(err.contains("two channels"), "{err}");
        assert!(
            err.contains("1-0:1.8.0") && err.contains("1-0:2.8.0"),
            "{err}"
        );
        assert!(err.contains(".obis("), "the message names the fix: {err}");
    }

    #[test]
    fn folding_two_tenants_into_one_series_is_refused() {
        // Worse than a wrong total: it is one party's readings inside another's
        // series, from a read that named neither.
        let of = |tenant: &str| {
            let mut s = one_interval("1-0:1.8.0");
            s.extra.insert(
                "tenant".to_string(),
                ScalarValue::Utf8(Some(tenant.to_string())),
            );
            s
        };

        let err = merge(
            malo(),
            Some("1-0:1.8.0"),
            &["tenant".to_string()],
            vec![of("a"), of("b")],
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("two readings"), "{err}");
        assert!(
            err.contains("tenant=a") && err.contains("tenant=b"),
            "{err}"
        );
        assert!(err.contains(".column_eq("), "{err}");
    }

    #[test]
    fn a_column_that_is_not_in_the_merge_key_does_not_split_a_series() {
        // The rule is the merge key, not "any column that differs". A Bilanzkreis
        // reassigned between two deliveries is one series with a changed
        // attribute, and refusing that would make an ordinary correction
        // unreadable.
        let of = |bk: &str, hours: i64| {
            let mut s = one_interval("1-0:1.8.0");
            s.extra.insert(
                "bilanzkreis".to_string(),
                ScalarValue::Utf8(Some(bk.into())),
            );
            s.recorded_at += time::Duration::hours(hours);
            s
        };

        assert!(
            merge(
                malo(),
                Some("1-0:1.8.0"),
                &[],
                vec![of("BK-1", 0), of("BK-2", 1)]
            )
            .unwrap()
            .is_some()
        );
    }

    #[test]
    fn decoded_groups_fold_back_into_one_series() {
        // Decoding splits a channel wherever the delivery changed. A caller
        // asked for a series, so the groups have to come back together.
        let a = stored(
            vec![interval(datetime!(2026-07-20 00:00 UTC), 1)],
            datetime!(2026-07-21 00:00 UTC),
        );
        let b = stored(
            vec![interval(datetime!(2026-07-20 00:15 UTC), 2)],
            datetime!(2026-07-22 00:00 UTC),
        );

        let ResolvedSeries { series, .. } = merge(malo(), Some("1-0:1.8.0"), &[], vec![b, a])
            .unwrap()
            .expect("rows were supplied");
        assert_eq!(series.intervals.len(), 2);
        assert_eq!(series.malo_id, malo());
    }

    #[test]
    fn intervals_come_back_in_time_order() {
        // Whatever order the tiers produced them in, the domain layer expects a
        // series to run forwards.
        let later = stored(
            vec![interval(datetime!(2026-07-20 12:00 UTC), 1)],
            datetime!(2026-07-21 00:00 UTC),
        );
        let earlier = stored(
            vec![interval(datetime!(2026-07-20 00:00 UTC), 2)],
            datetime!(2026-07-21 00:00 UTC),
        );

        let ResolvedSeries { series, .. } =
            merge(malo(), Some("1-0:1.8.0"), &[], vec![later, earlier])
                .unwrap()
                .expect("rows were supplied");
        assert_eq!(series.intervals[0].from, datetime!(2026-07-20 00:00 UTC));
        assert_eq!(series.intervals[1].from, datetime!(2026-07-20 12:00 UTC));
    }

    #[test]
    fn an_empty_read_is_absence_rather_than_an_empty_series() {
        // A series asserts a source — who reported these values. With no values
        // there is nobody to name, and inventing one would put a delivery in the
        // audit trail that never happened.
        assert!(
            merge(malo(), Some("1-0:1.8.0"), &[], Vec::new())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn the_newest_delivery_supplies_the_series_level_fields() {
        // Those fields describe the values that survived resolution, which came
        // from the most recent delivery.
        let mut old = stored(
            vec![interval(datetime!(2026-07-20 00:00 UTC), 1)],
            datetime!(2026-07-21 00:00 UTC),
        );
        old.series.resolution = None;
        let mut new = stored(
            vec![interval(datetime!(2026-07-20 00:15 UTC), 2)],
            datetime!(2026-07-25 00:00 UTC),
        );
        new.series.resolution = Some(metering::IntervalResolution::QuarterHour);

        let ResolvedSeries { series, .. } = merge(malo(), Some("1-0:1.8.0"), &[], vec![new, old])
            .unwrap()
            .expect("rows were supplied");
        assert_eq!(
            series.resolution,
            Some(metering::IntervalResolution::QuarterHour)
        );
    }

    #[test]
    fn a_mixed_channel_read_carries_no_series_level_obis() {
        // Claiming one channel for a series holding two would misdescribe it.
        let mut other = interval(datetime!(2026-07-20 00:15 UTC), 2);
        other.obis_code = Some("1-0:2.8.0".parse().unwrap());
        let mixed = stored(
            vec![interval(datetime!(2026-07-20 00:00 UTC), 1), other],
            datetime!(2026-07-21 00:00 UTC),
        );

        let ResolvedSeries { series, .. } = merge(malo(), None, &[], vec![mixed])
            .unwrap()
            .expect("rows were supplied");
        assert_eq!(series.obis_code, None);
    }

    #[test]
    fn a_single_channel_read_recovers_its_obis_without_being_told() {
        let one = stored(
            vec![interval(datetime!(2026-07-20 00:00 UTC), 1)],
            datetime!(2026-07-21 00:00 UTC),
        );
        let ResolvedSeries { series, .. } = merge(malo(), None, &[], vec![one])
            .unwrap()
            .expect("rows were supplied");
        assert_eq!(series.obis_code, Some("1-0:1.8.0".parse().unwrap()));
    }

    #[test]
    fn the_stored_provenance_trail_survives_the_fold() {
        // Provenance is not derivable, so losing it here would be silent data
        // loss in a store that claims audit value.
        let mut s = stored(
            vec![interval(datetime!(2026-07-20 00:00 UTC), 1)],
            datetime!(2026-07-21 00:00 UTC),
        );
        let trail = s.series.provenance.clone();
        assert!(!trail.is_empty(), "the fixture must carry a trail");
        s.series.provenance = trail.clone();

        let ResolvedSeries { series, .. } = merge(malo(), Some("1-0:1.8.0"), &[], vec![s])
            .unwrap()
            .expect("rows were supplied");
        assert_eq!(series.provenance, trail);
    }

    #[test]
    fn the_commodity_survives_the_fold() {
        // `MeasurementSeries` carries no Sparte, so a caller reconstructing domain
        // reads has only what `merge` hands back. Losing it here would relabel
        // every gas and water series as electricity — silently, since the numbers
        // look the same.
        let scope =
            VersionScope::for_interval("99", datetime!(2026-07-20 00:00 UTC), Sparte::Strom)
                .unwrap();
        let gas = StoredSeries::of(
            Sparte::Gas,
            MeasurementSeries::new(
                malo(),
                Some("7-1:3.0.0".parse().unwrap()),
                vec![interval(datetime!(2026-07-20 00:00 UTC), 1)],
                source(),
                datetime!(2026-07-21 00:00 UTC),
            ),
            ScopedVersion::new(scope, Version::new(20_260_701_000_001).unwrap()),
            datetime!(2026-07-21 00:00 UTC),
        );

        let ResolvedSeries { sparte, .. } = merge(malo(), None, &[], vec![gas])
            .unwrap()
            .expect("rows were supplied");
        assert_eq!(sparte, Sparte::Gas);
    }
}
