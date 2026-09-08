//! The typed read path: a query in, a `metering` type out.
//!
//! The unit of work is [`MeasurementSeries`], because that is what the domain
//! layer computes with. A caller that wants `aggregate(&series.intervals, …)`
//! should not have to decode Arrow to get there, and one that wants a `DataFrame`
//! should not have to go through this.
//!
//! There is nothing to derive and nothing to declare. The row type is
//! `metering`'s, so a derive macro could not be implemented for it anyway, and
//! per-deployment columns are configuration rather than fields.
//!
//! # Values never reach the SQL text
//!
//! `malo_id` and `obis_code` are caller-supplied, so they are bound as
//! parameters. The OBIS code is canonicalised on the way in for a second reason:
//! it is part of the merge key, so `1-0:1.8.0` and `1-0:1.8.0*255` are the same
//! channel and a literal comparison against the stored spelling would silently
//! return nothing.

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
    /// folds them into a single series. Naming the identity that scopes the
    /// read keeps them apart. Repeatable: each call adds one equality predicate,
    /// and the value is bound as a parameter, never concatenated into the SQL.
    ///
    /// The **name** cannot be a parameter — no SQL dialect parameterises an
    /// identifier — so it is checked against the store's declared columns rather
    /// than interpolated on trust. An unknown name is refused here, naming what is
    /// available, instead of reaching the engine as a fragment of SQL.
    pub fn column_eq(self, name: &str, value: ScalarValue) -> Result<Self> {
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
        Ok(self.with_filter(name.to_string(), value))
    }

    /// Add one equality predicate. The single push site, so `melo` and
    /// `column_eq` cannot come to differ about how a filter is carried.
    fn with_filter(mut self, name: String, value: ScalarValue) -> Self {
        self.filters.push((name, value));
        self
    }

    /// Restrict to one **Messlokation** — one meter.
    ///
    /// The narrowing a table that declares
    /// [`identify_by_melo`](crate::config::TableConfig::identify_by_melo) cannot
    /// do without: `melo_id` joins the merge key there, so one Marktlokation
    /// measured by two meters stores two series per channel and an unnarrowed
    /// read folds both into one — which sums a load profile to twice the truth.
    ///
    /// On a table where `melo_id` only *labels* a row it is still a filter,
    /// selecting the rows that name this meter.
    ///
    /// # Why this exists beside `column_eq`
    ///
    /// [`column_eq`](Self::column_eq) accepts `melo_id` on such a table and
    /// takes a bare `ScalarValue`, which is **not parsed**: a Zählpunktbezeichnung
    /// is 33 characters with no check digit, stored uppercase, and a truncated or
    /// lower-cased literal matches nothing at all. Silently — an empty series
    /// looks exactly like a meter that reported nothing, and that is the report
    /// a settlement run would act on. This one parses
    /// ([`parse_melo`](crate::encode::parse_melo)) and takes whatever the caller
    /// is holding: a [`MeloId`], a `&str` or a `String`.
    pub fn melo<M>(self, melo_id: M) -> Result<Self>
    where
        M: TryInto<MeloId>,
        M::Error: std::fmt::Display,
    {
        let melo = crate::encode::parse_melo(melo_id)?;
        // Pushed directly rather than through `column_eq`, whose accepted set is
        // the *declared* columns plus the discriminators: `melo_id` is a core
        // column on every table, and refusing it where it merely labels a row
        // would make the typed narrowing narrower than the untyped one.
        Ok(self.with_filter(
            col::MELO_ID.to_string(),
            ScalarValue::Utf8(Some(melo.to_string())),
        ))
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
    ///
    /// For the newest value of **each** channel, ask
    /// [`channels`](Self::channels) and then `latest` per channel. That is
    /// deliberately not one call: each `latest` is an `ORDER BY … DESC LIMIT 1`
    /// the index answers, so the cost is a handful of point lookups rather than
    /// the whole-history scan a single windowed query would need — which is the
    /// opposite trade from [`collect_by_channel`](Self::collect_by_channel),
    /// where reading each channel in turn means scanning the range N times.
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

    /// The channels this range holds, in OBIS order.
    ///
    /// A measuring point **is** a set of registers — a Bezug channel beside HT and
    /// NT, import beside export — and there is no way to know which of them a
    /// range actually carries without asking. This is that question, and it is a
    /// `SELECT DISTINCT` rather than a fold, so it costs one aggregate instead of
    /// decoding every interval.
    ///
    /// Narrowed by everything this builder was narrowed by: the range, the quality
    /// filter and any [`column_eq`](Self::column_eq). That matters most for the
    /// last — on a shared store, an unscoped list would name channels belonging to
    /// a tenant the caller may not be reading.
    ///
    /// It is a set of **channels**, not of readings: where two tenants report the
    /// same OBIS code, it appears once. [`collect_by_channel`](Self::collect_by_channel)
    /// is what refuses to fold those together.
    ///
    /// Takes `&self`, so the builder survives to be collected afterwards.
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
                out.push(codes.value(i).parse::<ObisCode>().map_err(|e| {
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

    /// Every channel in this range, each resolved as its own series.
    ///
    /// The whole measuring point, where [`collect`](Self::collect) describes one
    /// channel of it — a billing period projecting Bezug across HT, NT and total,
    /// a Mehr-/Mindermengensaldo, an audit of what a delivery contained.
    ///
    /// **One scan**, split in Rust: the same single query `collect` runs, so every
    /// channel is resolved against one tier boundary rather than N boundaries read
    /// at N different moments. `SELECT DISTINCT` plus a read per channel is
    /// `1 + N` round trips and holds resolution and the tier split outside the
    /// store by convention.
    ///
    /// **It still refuses to fold two readings.** A reading is `(channel,
    /// merge-key discriminators)`, so two tenants — or two meters — on one OBIS
    /// code are refused exactly as `collect` refuses them. Narrow with
    /// [`column_eq`](Self::column_eq) and the map is one entry per channel again.
    ///
    /// Empty when the range holds nothing, which is the statement `collect`'s
    /// `None` makes.
    ///
    /// ```no_run
    /// # async fn f(store: &meterstore::MeterStore, malo: &str,
    /// #            from: time::OffsetDateTime, to: time::OffsetDateTime)
    /// #     -> meterstore::Result<()> {
    /// let point = store.series(malo)?.range(from, to).collect_by_channel().await?;
    /// for (channel, resolved) in &point {
    ///     println!("{channel}: {} intervals", resolved.series.intervals.len());
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn collect_by_channel(self) -> Result<BTreeMap<ObisCode, ResolvedSeries>> {
        Ok(self.collect_by_channel_with_provenance().await?.0)
    }

    /// As [`collect_by_channel`](Self::collect_by_channel), but keeping the
    /// query's provenance.
    ///
    /// One [`QueryResult`](super::QueryResult) for the whole map, because it was
    /// one scan: every channel here was computed against the same tier boundary,
    /// which is precisely what the `1 + N` spelling cannot say.
    pub async fn collect_by_channel_with_provenance(
        self,
    ) -> Result<(BTreeMap<ObisCode, ResolvedSeries>, super::QueryResult)> {
        let malo_id = self.malo_id;
        let discriminators = self.store.config().discriminator_columns();
        let (stored, result) = self.scan().await?;
        Ok((split_by_channel(malo_id, &discriminators, stored)?, result))
    }

    /// The `WHERE` clause this read narrows to, and the values it binds.
    ///
    /// Extracted because three reads share it — the fold, the per-channel split
    /// and the channel list — and a second copy would be a second place for a
    /// filter to be forgotten. That failure is silent in the worst direction:
    /// [`channels`](Self::channels) listing a tenant's channels unscoped, or a
    /// quality filter applying to the fold and not to the list.
    ///
    /// **Every value is bound.** Only column *names* reach the SQL text, and
    /// every one of them is either a core column or one
    /// [`column_eq`](Self::column_eq) checked against the store's declared set.
    fn predicate(&self) -> (Vec<String>, Vec<ScalarValue>) {
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

        (conditions, params)
    }

    /// Run the read and decode it, without folding.
    async fn scan(&self) -> Result<(Vec<crate::encode::StoredSeries>, super::QueryResult)> {
        let (conditions, params) = self.predicate();

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
        Ok((stored, result))
    }

    /// The shared single-series read: scan, then fold the rows into one series.
    ///
    /// Every collector that returns *one* [`ResolvedSeries`] goes through here,
    /// and each projects a subset of what it returns — the commodity, the declared
    /// attribute columns, or the tier-boundary provenance.
    async fn resolve(self) -> Result<(Option<ResolvedSeries>, super::QueryResult)> {
        let malo_id = self.malo_id;
        let obis_code = self.obis_code.clone();
        let discriminators = self.store.config().discriminator_columns();
        let (stored, result) = self.scan().await?;
        Ok((
            merge(malo_id, obis_code.as_deref(), &discriminators, stored)?,
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
    refuse_repeated_instants(malo_id, &intervals)?;

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

/// Refuse a folded series that holds two values at one instant.
///
/// [`refuse_mixed_readings`] asks whether the rows describe two *readings* — a
/// second channel, a second tenant, a second Messlokation. This is the case it
/// cannot see: resolution partitions by the merge key **and `version_scope`**, so
/// two scopes for one reading leave two winners agreeing on every part of that
/// key, and `metering::aggregate` sums both.
///
/// Both write paths refuse a second network operator, so reaching here means
/// `PostgresHot::integrity_constraints(false)` or a writer that is not this
/// crate. [`Error::InvariantViolated`] rather than `IntegrityViolation`: nothing
/// is being refused at a boundary, something that should not be true already is.
fn refuse_repeated_instants(
    malo_id: MaloId,
    intervals: &[metering::interval::MeterInterval],
) -> Result<()> {
    // Sorted by `from` on the way in, so a repeat is adjacent.
    for pair in intervals.windows(2) {
        if pair[0].from != pair[1].from {
            continue;
        }
        return Err(Error::InvariantViolated {
            table: malo_id.to_string(),
            detail: format!(
                "two values survived resolution for one reading at {at}: {a} and {b}. \
                 Resolution partitions by the merge key and version_scope, so the cause \
                 is almost always two network operators for one reading — which both \
                 write paths refuse, unless integrity constraints are off or something \
                 other than meterstore wrote these rows. Folding them into one series \
                 would sum to twice the truth",
                at = pair[0].from,
                a = pair[0].value,
                b = pair[1].value,
            ),
        });
    }
    Ok(())
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
        let (differs, fix) = if channel != &key.0 {
            (
                format!(
                    "two channels ({} and {})",
                    channel.as_deref().unwrap_or("<none>"),
                    key.0.as_deref().unwrap_or("<none>"),
                ),
                ".obis(..)",
            )
        } else {
            // Which identity column actually differs decides which narrowing to
            // name. A caller told to reach for `.column_eq("melo_id", …)` would
            // be sent to the untyped door for a value that has to be parsed —
            // and an unparsed Zählpunktbezeichnung matches nothing at all.
            let melo_differs = identity
                .iter()
                .zip(&key.1)
                .any(|((name, a), (_, b))| name == col::MELO_ID && a != b);
            (
                format!("two readings ({} and {})", render(identity), render(&key.1)),
                if melo_differs {
                    ".melo(..)"
                } else {
                    ".column_eq(..)"
                },
            )
        };

        return Err(Error::config(format!(
            "{malo} spans {differs} over this range, and a MeasurementSeries can only \
             describe one — folding them puts two values at the same instant into one \
             series, which sums to twice the truth with nothing to notice it. Narrow the \
             read with {fix}, or scope the session",
            malo = stored[0].series.malo_id,
        )));
    }
    Ok(())
}

/// Group decoded rows by channel and fold each group on its own.
///
/// The split half of [`SeriesQuery::collect_by_channel`], separated so the
/// property that matters can be checked without a database: two channels split,
/// and two *readings* within one channel still refuse.
///
/// Grouped rather than run-detected. The scan orders by the whole merge key, so
/// one channel's rows are already contiguous — but relying on that would make the
/// split silently wrong if the order ever changed, and the failure would be a
/// channel returned twice with half its intervals each.
fn split_by_channel(
    malo_id: MaloId,
    discriminators: &[String],
    stored: Vec<crate::encode::StoredSeries>,
) -> Result<BTreeMap<ObisCode, ResolvedSeries>> {
    let mut grouped: BTreeMap<ObisCode, Vec<crate::encode::StoredSeries>> = BTreeMap::new();
    for series in stored {
        // Always `Some` off the decode path: `obis_code` is non-nullable in
        // storage and `from_record_batch` refuses a code that does not parse.
        // Reported rather than assumed, because the alternative to a channel is a
        // silently dropped series.
        let channel = series.series.obis_code.ok_or_else(|| {
            Error::decode(
                col::OBIS_CODE,
                format!(
                    "a decoded series for {malo_id} carries no channel, so it cannot be placed \
                     in a per-channel read"
                ),
            )
        })?;
        grouped.entry(channel).or_default().push(series);
    }

    let mut out = BTreeMap::new();
    for (channel, series) in grouped {
        // `merge` runs the same refusal `collect` does. Within one channel it can
        // only fire on differing discriminators, which is the half that still
        // means two readings.
        if let Some(resolved) = merge(malo_id, Some(&channel.to_string()), discriminators, series)?
        {
            out.insert(channel, resolved);
        }
    }
    Ok(out)
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
            sender_mp_id: "9900000000001".parse().expect("a valid Marktpartner-ID"),
        }
    }

    fn stored(intervals: Vec<MeterInterval>, recorded_at: OffsetDateTime) -> StoredSeries {
        let scope =
            VersionScope::for_interval("9900000000001", intervals[0].from, Sparte::Strom).unwrap();
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
    fn a_per_channel_read_splits_what_the_fold_refuses() {
        // The whole point of `collect_by_channel`: a measuring point *is* a set
        // of registers, and asking for all of them must not require the caller to
        // discover the list with hand-written SQL and then read each one.
        let split = split_by_channel(
            malo(),
            &[],
            vec![one_interval("1-0:1.8.0"), one_interval("1-0:2.8.0")],
        )
        .expect("two channels are two series, not an error");

        assert_eq!(split.len(), 2);
        let import: ObisCode = "1-0:1.8.0".parse().unwrap();
        let export: ObisCode = "1-0:2.8.0".parse().unwrap();
        assert_eq!(split[&import].series.obis_code, Some(import));
        assert_eq!(split[&export].series.obis_code, Some(export));

        // One interval each, at the same instant — which is exactly the shape a
        // fold would have doubled.
        assert_eq!(split[&import].series.intervals.len(), 1);
        assert_eq!(split[&export].series.intervals.len(), 1);
        assert_eq!(
            split[&import].series.intervals[0].from,
            split[&export].series.intervals[0].from
        );
    }

    #[test]
    fn a_per_channel_read_still_refuses_two_readings_on_one_channel() {
        // A reading is `(channel, discriminators)`, not a channel alone. Splitting
        // by channel and stopping there would fold two tenants — or the two meters
        // of a Mehrfamilienhaus — into one series on the same OBIS code, which is
        // the exact double-count `collect`'s second refusal exists to prevent.
        let mut a = one_interval("1-0:1.8.0");
        let mut b = one_interval("1-0:1.8.0");
        a.extra.insert(
            "tenant".to_string(),
            ScalarValue::Utf8(Some("alpha".to_string())),
        );
        b.extra.insert(
            "tenant".to_string(),
            ScalarValue::Utf8(Some("beta".to_string())),
        );

        let err = split_by_channel(malo(), &["tenant".to_string()], vec![a, b])
            .expect_err("two tenants on one channel are two readings");
        let msg = err.to_string();
        assert!(msg.contains("two readings"), "{msg}");
        assert!(msg.contains("tenant=alpha"), "{msg}");

        // And naming the tenant is what makes it one reading again.
        let mut scoped = one_interval("1-0:1.8.0");
        scoped.extra.insert(
            "tenant".to_string(),
            ScalarValue::Utf8(Some("alpha".to_string())),
        );
        let split = split_by_channel(malo(), &["tenant".to_string()], vec![scoped]).unwrap();
        assert_eq!(split.len(), 1);
    }

    #[test]
    fn channels_come_back_in_obis_order_not_alphabetical_order() {
        // The column is text, and OBIS codes do not sort as text the way they
        // sort as codes: `1-0:10.8.0` precedes `1-0:2.8.0` alphabetically and
        // follows it numerically. A list ordered by the database alone would put
        // a two-digit value group in the wrong place, which for a report read top
        // to bottom is the kind of wrong nobody checks.
        let mut codes: Vec<ObisCode> = ["1-0:2.8.0", "1-0:10.8.0", "1-0:1.8.0"]
            .iter()
            .map(|c| c.parse().unwrap())
            .collect();
        codes.sort_unstable();

        assert_eq!(
            codes.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ["1-0:1.8.0", "1-0:2.8.0", "1-0:10.8.0"],
        );
        // …which is not what the database's own text order would have produced.
        let mut text: Vec<&str> = vec!["1-0:2.8.0", "1-0:10.8.0", "1-0:1.8.0"];
        text.sort_unstable();
        assert_ne!(
            text,
            codes.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "if the two agreed, sorting in Rust would prove nothing"
        );
    }

    #[test]
    fn a_per_channel_read_of_nothing_is_an_empty_map() {
        // The same statement `collect`'s `None` makes, in the shape a map has.
        assert!(
            split_by_channel(malo(), &[], Vec::new())
                .unwrap()
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
    fn two_values_at_one_instant_are_refused_rather_than_summed() {
        // The case `refuse_mixed_readings` cannot see. Resolution partitions by
        // the merge key *and* `version_scope`, so two network operators for one
        // reading leave two winners agreeing on channel and discriminators —
        // same instant, two values, and the fold would hand `metering::aggregate`
        // both. Both write paths refuse a second operator, so getting here means
        // integrity constraints are off or something else wrote the rows; this is
        // the after-the-fact detection a duplicated scope otherwise has
        // none of.
        let mut second = one_interval("1-0:1.8.0");
        second.series.intervals[0].value = rust_decimal::Decimal::new(99, 1);
        second.recorded_at += time::Duration::hours(1);

        let err = merge(malo(), None, &[], vec![one_interval("1-0:1.8.0"), second]).unwrap_err();

        assert!(
            matches!(err, Error::InvariantViolated { .. }),
            "stored data that should not exist, not a delivery being refused: {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("version_scope"), "{msg}");
        assert!(
            msg.contains("1.5") && msg.contains("9.9"),
            "both values: {msg}"
        );
    }

    #[test]
    fn ordinary_deliveries_of_one_series_still_fold() {
        // The guard must not fire on the thing the fold exists for: two
        // deliveries covering *different* instants of one channel.
        let first = one_interval("1-0:1.8.0");
        let mut later = one_interval("1-0:1.8.0");
        later.series.intervals[0].from += time::Duration::minutes(15);
        later.series.intervals[0].to += time::Duration::minutes(15);
        later.recorded_at += time::Duration::hours(1);

        let folded = merge(malo(), None, &[], vec![first, later])
            .unwrap()
            .expect("one series");
        assert_eq!(folded.series.intervals.len(), 2);
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
        // reassigned partway through a range splits the decode into two groups —
        // `from_record_batch` starts a new one wherever any non-interval column
        // changes — and the fold has to put them back, or an ordinary attribute
        // change would make a series unreadable.
        //
        // The two groups cover **different instants**, which is the only shape
        // resolution can actually produce: it keeps one row per (merge key,
        // version_scope), so one channel cannot hold two rows at one instant with
        // a Bilanzkreis to tell them apart. A fixture that overlapped them would
        // be asserting the fold accepts a state the store cannot be in, which is
        // what `two_values_at_one_instant_are_refused_rather_than_summed` says it
        // must not.
        let of = |bk: &str, quarters: i64| {
            let mut s = one_interval("1-0:1.8.0");
            let shift = time::Duration::minutes(15 * quarters);
            s.series.intervals[0].from += shift;
            s.series.intervals[0].to += shift;
            s.extra.insert(
                "bilanzkreis".to_string(),
                ScalarValue::Utf8(Some(bk.into())),
            );
            s.recorded_at += time::Duration::hours(quarters);
            s
        };

        let folded = merge(
            malo(),
            Some("1-0:1.8.0"),
            &[],
            vec![of("BK-1", 0), of("BK-2", 1)],
        )
        .unwrap()
        .expect("one series");
        assert_eq!(folded.series.intervals.len(), 2, "both groups are kept");
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
        let scope = VersionScope::for_interval(
            "9900000000001",
            datetime!(2026-07-20 00:00 UTC),
            Sparte::Strom,
        )
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
