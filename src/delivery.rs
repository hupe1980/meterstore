//! What a delivery is, beyond its values: where it came from and what happened
//! to it.
//!
//! `metering` describes a measurement — its interval, value, quality, unit and
//! calendar. The **origin** of a delivery and its **audit trail** are process
//! state rather than quantities, so they are this crate's: the store is what has
//! to answer, years later, which MSCONS a value arrived in.
//!
//! # These types are a stored form
//!
//! [`MeasurementSource`] is the `source_kind` and `source_detail` columns, and a
//! [`ProvenanceEntry`] trail is the `provenance` column, in PostgreSQL and in
//! every Parquet file under a retention measured in decades. Their `serde`
//! output is therefore not a wire format that two versions can renegotiate: a
//! renamed variant or field is a row that no longer decodes. Every derive here
//! states its spelling explicitly, the encoder pins each one byte for byte, and
//! the stored-form corpus holds every release to decoding what earlier ones
//! wrote.
//!
//! Where a payload nests a vocabulary the domain publishes —
//! [`Method`] and
//! [`SubstitutionReason`] — the
//! domain's own strings are stored. [`VirtualMeterKind`] has no upstream
//! home, so it is owned here with its spelling frozen.

use metering::ids::BdewCode;
use metering::vee::substitute::{Method, SubstitutionReason};
use metering::{MaloId, MeloId, MeterInterval, ObisCode, QualityFlag, Resolution};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// How a delivery entered the system.
///
/// Externally tagged with `SCREAMING_SNAKE_CASE` tags, so a payload reads
/// `{"MSCONS": {…}}` and the encoder takes `source_kind` from that tag rather
/// than from a second spelling. An `Option` field serialises as `null`, never as
/// an absent key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MeasurementSource {
    /// An EDIFACT MSCONS message — the standard market-communication path.
    Mscons {
        /// The MSCONS Prüfidentifikator, e.g. 13005.
        pid: u32,
        /// The EDIFACT message reference.
        message_ref: Option<String>,
        /// The Marktpartner-ID of the sending network or metering operator.
        sender_mp_id: BdewCode,
    },
    /// Pushed by a smart-meter gateway directly, outside EDIFACT.
    SmgwDirectPush {
        /// The gateway's device ID.
        device_id: String,
        /// The session ID, for idempotency.
        session_id: String,
    },
    /// Entered by an operator.
    ManualEntry {
        /// Who entered it.
        operator_id: String,
        /// Why.
        reason: String,
    },
    /// A substitute value formed by `metering::vee::substitute`.
    AutoSubstitute {
        /// The procedure that formed it.
        method: Method,
        /// Why a substitute was needed.
        reason: SubstitutionReason,
    },
    /// A correction applied after an earlier value was found wrong.
    RetroactiveCorrection {
        /// The correction's reference.
        correction_ref: String,
        /// Who corrected it.
        corrected_by: String,
    },
    /// Computed from other meters' series.
    VirtualMeter {
        /// How the inputs were combined.
        rule: VirtualMeterKind,
        /// The MaLo-IDs it was computed from.
        source_ids: Vec<String>,
    },
    /// Imported from Redispatch 2.0.
    RedispatchImport {
        /// The Prüfidentifikator of the carrying message.
        pid: u32,
        /// The activation's reference, if any.
        activation_ref: Option<String>,
    },
    /// An e-mobility charge detail record.
    ChargeDetailRecord {
        /// The CDR's ID.
        cdr_id: String,
        /// The EVSE, if known.
        evse_id: Option<String>,
    },
    /// A clock-aligned meter value from a charging station.
    ClockAlignedMeterValue {
        /// The charging transaction.
        transaction_id: String,
        /// The EVSE, if known.
        evse_id: Option<String>,
    },
    /// Read from a device's own log.
    DeviceLog {
        /// The device.
        device_id: String,
        /// The register read, if named.
        register: Option<String>,
    },
}

impl MeasurementSource {
    /// A human-readable name for the origin — the `actor` of the `INGESTED`
    /// entry [`MeasurementSeries::new`] records.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Mscons { .. } => "MSCONS",
            Self::SmgwDirectPush { .. } => "SMGW-Direktpush",
            Self::ManualEntry { .. } => "Manuelle Eingabe",
            Self::AutoSubstitute { .. } => "Automatischer Ersatzwert",
            Self::RetroactiveCorrection { .. } => "Nachträgliche Korrektur",
            Self::VirtualMeter { .. } => "Virtueller Zähler",
            Self::RedispatchImport { .. } => "Redispatch 2.0",
            Self::ChargeDetailRecord { .. } => "Ladevorgang (CDR)",
            Self::ClockAlignedMeterValue { .. } => "Ladepunkt-Zählwert",
            Self::DeviceLog { .. } => "Gerätelog",
        }
    }
}

/// How a virtual meter combined its inputs.
///
/// Owned here because the domain does not model virtual meters, and a
/// `VIRTUAL_METER` row must still say what it was. Frozen: a variant may be
/// added, never renamed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum VirtualMeterKind {
    /// The sum of the inputs.
    Sum,
    /// One input minus the others.
    Residual,
    /// PV self-consumption: generation minus feed-in.
    PvSelfConsumption,
    /// GGV allocation by a constant key.
    GgvConstantAllocation,
    /// GGV allocation in proportion to consumption.
    GgvProportionalAllocation,
}

impl VirtualMeterKind {
    /// Every kind, in declaration order.
    pub const ALL: [Self; 5] = [
        Self::Sum,
        Self::Residual,
        Self::PvSelfConsumption,
        Self::GgvConstantAllocation,
        Self::GgvProportionalAllocation,
    ];
}

/// One event in a delivery's audit trail.
///
/// Fields serialise in declaration order, and `occurred_at` as RFC 3339 —
/// formatted here rather than by `time`'s own impl, which is
/// feature-conditional and would leave the on-disk shape of an audit trail to
/// Cargo feature unification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenanceEntry {
    /// When it happened.
    #[serde(with = "rfc3339")]
    pub occurred_at: OffsetDateTime,
    /// What happened.
    pub event_type: ProvenanceEventType,
    /// Who or what did it.
    pub actor: String,
    /// A free-text note.
    pub note: Option<String>,
}

/// What a [`ProvenanceEntry`] records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProvenanceEventType {
    /// The delivery was received.
    Ingested,
    /// Its quality was assessed.
    QualityAssessed,
    /// A substitute was formed for it.
    SubstituteGenerated,
    /// It was corrected.
    Corrected,
    /// It was archived.
    Archived,
    /// Its subject was pseudonymised.
    Anonymised,
}

impl ProvenanceEventType {
    /// Every event type, in declaration order.
    pub const ALL: [Self; 6] = [
        Self::Ingested,
        Self::QualityAssessed,
        Self::SubstituteGenerated,
        Self::Corrected,
        Self::Archived,
        Self::Anonymised,
    ];
}

/// One delivery of interval values for one measuring point.
///
/// The values are `metering`'s [`MeterInterval`]s; everything around them —
/// the identifiers, the declared grid, the origin and the trail — is what the
/// store keeps alongside.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasurementSeries {
    /// The Marktlokation.
    pub malo_id: MaloId,
    /// The Messlokation, if known.
    pub melo_id: Option<MeloId>,
    /// The channel, where every interval shares one.
    pub obis_code: Option<ObisCode>,
    /// The declared grid. Defaults to the channel's own.
    pub resolution: Option<Resolution>,
    /// How the delivery entered the system.
    pub source: MeasurementSource,
    /// The values, ascending by start.
    pub intervals: Vec<MeterInterval>,
    /// The audit trail, oldest first.
    pub provenance: Vec<ProvenanceEntry>,
}

impl MeasurementSeries {
    /// A delivery, with an `INGESTED` entry stamped `ingested_at`.
    ///
    /// The instant is a parameter rather than a clock read, so two series built
    /// from equal inputs are equal — which is what makes a stored round trip
    /// testable by equality.
    #[must_use]
    pub fn new(
        malo_id: MaloId,
        obis_code: Option<ObisCode>,
        intervals: Vec<MeterInterval>,
        source: MeasurementSource,
        ingested_at: OffsetDateTime,
    ) -> Self {
        let actor = source.label().to_owned();
        Self {
            malo_id,
            melo_id: None,
            obis_code,
            resolution: obis_code.and_then(|o| o.default_resolution()),
            source,
            intervals,
            provenance: vec![ProvenanceEntry {
                occurred_at: ingested_at,
                event_type: ProvenanceEventType::Ingested,
                actor,
                note: None,
            }],
        }
    }

    /// Name the Messlokation.
    #[must_use]
    pub fn with_melo_id(mut self, melo_id: MeloId) -> Self {
        self.melo_id = Some(melo_id);
        self
    }

    /// Declare the grid, overriding the channel's default.
    #[must_use]
    pub const fn with_resolution(mut self, resolution: Resolution) -> Self {
        self.resolution = Some(resolution);
        self
    }

    /// The worst quality across the values, or `UNKNOWN` for none.
    ///
    /// Derived on demand rather than stored: a stored copy is a second source of
    /// truth that any edit to `intervals` invalidates.
    #[must_use]
    pub fn worst_quality(&self) -> QualityFlag {
        QualityFlag::worst_of(self.intervals.iter().map(MeterInterval::quality))
    }

    /// Append an event to the trail.
    pub fn record_event(
        &mut self,
        event_type: ProvenanceEventType,
        actor: impl Into<String>,
        occurred_at: OffsetDateTime,
    ) {
        self.provenance.push(ProvenanceEntry {
            occurred_at,
            event_type,
            actor: actor.into(),
            note: None,
        });
    }
}

/// An instant as RFC 3339, in a human-readable format and a binary one alike.
///
/// One spelling for both, unlike `time`'s split: this column is only ever JSON,
/// and a second representation is a second thing to keep stable.
mod rfc3339 {
    use serde::{Deserialize, Deserializer, Serializer, de, ser};
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;

    pub(super) fn serialize<S: Serializer>(
        value: &OffsetDateTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.format(&Rfc3339).map_err(ser::Error::custom)?)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<OffsetDateTime, D::Error> {
        let text = <std::borrow::Cow<'_, str>>::deserialize(deserializer)?;
        OffsetDateTime::parse(&text, &Rfc3339).map_err(de::Error::custom)
    }
}
