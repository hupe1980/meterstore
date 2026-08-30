//! What a write actually changed.
//!
//! Nothing here is a delete. In an append-only store a correction is a **new row
//! at a higher version**, and what changes is which version wins resolution. So
//! "displacement" is not "the row that was overwritten" — there is no such row —
//! but *"the value that stopped being current, and the one that took its place"*.
//!
//! # Why storage reports this at all
//!
//! MeterStore does not compute with readings (§3), and this is not a computation:
//! it is the store saying what its own write did. The alternative is for a caller
//! to read the prior state in a separate query and hope nothing landed in
//! between — which is a race, and an audit trail built on a race is worse than
//! none, because it is wrong exactly when two corrections arrive together.
//!
//! The regulatory shape this serves is a caller's concern, not this crate's. What
//! the crate owes is enough to build one without a second query: which value
//! stopped being current, which took its place, and **whether the current value
//! changed at all**. The last is the one a naive design gets wrong — see
//! [`Effect`].
//!
//! # It is a convenience, not the source of truth
//!
//! Every version is kept. `readings_versions` is the audit trail, and it stays
//! authoritative: a displacement report can always be reconstructed from it. What
//! this saves is the extra round trip and the race, not the history.

use metering::QualityFlag;
use metering::interval::MeasurementUnit;
use rust_decimal::Decimal;
use time::OffsetDateTime;

use crate::version::ScopedVersion;

/// One stored assertion about a reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredValue {
    /// The quantity, in [`unit`](Self::unit).
    pub value: Decimal,
    /// What the quantity is measured in.
    ///
    /// Carried because a value without it is dimensionless: water settles in m³
    /// and gas may sit on either side of the Brennwert conversion (§4.1.2). An
    /// audit row recording a changed number without its unit records half a fact.
    pub unit: MeasurementUnit,
    /// The reading's quality.
    ///
    /// Carried because a change *of quality* is a change even when the number is
    /// identical — a substitute value replaced by a measured one is exactly the
    /// event a caller tracking § 60 Abs. 2 obligations is waiting for, and it can
    /// leave the quantity untouched.
    pub quality: QualityFlag,
    /// The MSCONS correction version, with the scope it is comparable within.
    ///
    /// A [`ScopedVersion`] rather than a bare number, because ordering two
    /// versions is only defined inside one scope — comparing across scopes is
    /// the mistake §4.2 exists to prevent, and the type refuses it.
    pub version: ScopedVersion,
    /// Transaction time — when the store learned this value.
    pub recorded_at: OffsetDateTime,
}

/// What a write did to the value that was current.
///
/// The distinction that matters: **being stored and becoming current are not the
/// same thing.** A caller writing an audit row on every accepted write would
/// record changes that did not happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// First value for this reading. Nothing was superseded.
    Inserted,
    /// This value became current, and another stopped being current.
    Superseded,
    /// Stored, but an existing **higher** version still wins.
    ///
    /// Legitimate and easy to miss: backfilling an older delivery after a newer
    /// one has already arrived adds to the audit trail without changing what any
    /// query returns. A caller that treated this as a change would report a
    /// correction that never took effect.
    Shadowed,
    /// Already present at this exact version; nothing was written.
    ///
    /// Ordinary traffic rather than an error — every transport worth using
    /// delivers at least once. Distinguished from [`Inserted`](Self::Inserted) so
    /// a replay is not mistaken for a new reading.
    Duplicate,
}

impl Effect {
    /// Whether the value a query returns for this reading changed.
    ///
    /// The predicate to gate an audit row on. True only for
    /// [`Inserted`](Self::Inserted) and [`Superseded`](Self::Superseded).
    pub fn changed_current_value(self) -> bool {
        matches!(self, Self::Inserted | Self::Superseded)
    }
}

/// What one write did to one reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Displacement {
    /// Marktlokation.
    ///
    /// The stored column text, not a parsed
    /// [`MaloId`](metering::ids::MaloId) — the same rule as
    /// [`Completeness::malo_id`](crate::session::Completeness::malo_id). The
    /// domain payload is typed on both sides of the encoding; the storage
    /// layer's *report* structs key on what the column holds, so a malformed
    /// value shows up in the audit trail as itself rather than failing the write
    /// that would have recorded it.
    pub malo_id: String,
    /// The measured channel, canonical.
    pub obis_code: String,
    /// Interval start.
    pub from: OffsetDateTime,
    /// Interval end (exclusive), or `None` for a register reading.
    ///
    /// Not part of the merge key — a reading is named by its start — but carried
    /// so an audit row can report the interval it covered without collapsing to a
    /// zero-width `[from, from)`. Threaded from the written row, not the key.
    ///
    /// `None` on a [`Point`](crate::config::TimeModel::Point) table, where the
    /// row is a Zählerstand at an instant and there is no span to report. An
    /// `Option` rather than a repeat of `from`, because a zero-width span is a
    /// span that reads as real.
    pub to: Option<OffsetDateTime>,
    /// The merge-key columns beyond `(malo_id, obis_code, from)`, in key order.
    ///
    /// The deployment's declared identity columns, and `melo_id` where the table
    /// [identifies a reading by its
    /// Messlokation](crate::config::TableConfig::identify_by_melo).
    ///
    /// Part of what names the reading, not decoration: with a `tenant` identity
    /// column, `(malo_id, obis_code, from)` alone can name two different
    /// readings, and a report keyed on it would attribute one tenant's
    /// correction to another's value. The same is true of the two meters of a
    /// Mehrfamilienhaus.
    pub identity: Vec<(String, String)>,
    /// What this write did.
    pub effect: Effect,
    /// The value that was current before, if there was one.
    ///
    /// `None` for [`Effect::Inserted`]. Present for
    /// [`Superseded`](Effect::Superseded) and — importantly — for
    /// [`Shadowed`](Effect::Shadowed) and [`Duplicate`](Effect::Duplicate) too,
    /// where it is *still* current: the caller can see that its write did not
    /// take, and what holds instead.
    pub superseded: Option<StoredValue>,
    /// The value this write asserted.
    pub written: StoredValue,
}

impl Displacement {
    /// The value a query will return for this reading after the write.
    pub fn current(&self) -> &StoredValue {
        match self.effect {
            Effect::Inserted | Effect::Superseded => &self.written,
            // Both leave the prior value in force. `superseded` is guaranteed
            // present in these arms by construction — the effect is only reached
            // when a prior row was found — so the fallback is unreachable rather
            // than a silent wrong answer.
            Effect::Shadowed | Effect::Duplicate => {
                self.superseded.as_ref().unwrap_or(&self.written)
            }
        }
    }

    /// Whether the quantity changed, ignoring version and quality.
    ///
    /// A redelivery that restates the same number under a new version is a
    /// correction in the MSCONS sense and *not* a change in the settled amount.
    /// Callers that reconcile money care about the difference.
    pub fn value_changed(&self) -> bool {
        match &self.superseded {
            Some(prior) => self.effect.changed_current_value() && prior.value != self.written.value,
            None => self.effect.changed_current_value(),
        }
    }

    /// Whether the quality changed while the quantity did not.
    ///
    /// The § 60 Abs. 2 shape: a substitute value replaced by a measured one,
    /// where the number may be identical and the obligation still discharges.
    pub fn quality_changed(&self) -> bool {
        match &self.superseded {
            Some(prior) => {
                self.effect.changed_current_value() && prior.quality != self.written.quality
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::{Version, VersionScope};
    use time::macros::datetime;

    fn value(v: i64, version: u128, quality: QualityFlag) -> StoredValue {
        StoredValue {
            value: Decimal::new(v, 0),
            unit: MeasurementUnit::KiloWattHour,
            quality,
            version: ScopedVersion::new(
                VersionScope::new("9900000000001", 2026, 7).unwrap(),
                Version::new(version).unwrap(),
            ),
            recorded_at: datetime!(2026-07-27 06:00 UTC),
        }
    }

    fn displacement(
        effect: Effect,
        prior: Option<StoredValue>,
        written: StoredValue,
    ) -> Displacement {
        Displacement {
            malo_id: "12345678905".into(),
            obis_code: "1-0:1.8.0".into(),
            from: datetime!(2026-07-20 00:00 UTC),
            to: Some(datetime!(2026-07-20 00:15 UTC)),
            identity: Vec::new(),
            effect,
            superseded: prior,
            written,
        }
    }

    #[test]
    fn only_a_real_change_gates_an_audit_row() {
        // The distinction a naive design loses: being stored and becoming
        // current are different, and only the second is a change to report.
        assert!(Effect::Inserted.changed_current_value());
        assert!(Effect::Superseded.changed_current_value());
        assert!(!Effect::Shadowed.changed_current_value());
        assert!(!Effect::Duplicate.changed_current_value());
    }

    #[test]
    fn a_shadowed_write_reports_the_value_that_still_holds() {
        // Backfilling an older delivery after a newer one adds to the audit
        // trail and changes nothing a query returns. The caller must be able to
        // see that, and see what holds instead.
        let winner = value(50, 20_260_728_000_009, QualityFlag::Measured);
        let d = displacement(
            Effect::Shadowed,
            Some(winner.clone()),
            value(10, 20_260_720_000_001, QualityFlag::Measured),
        );
        assert_eq!(d.current(), &winner);
        assert!(!d.value_changed());
    }

    #[test]
    fn a_restated_quantity_is_a_correction_but_not_a_change_in_the_amount() {
        // MSCONS corrects by versioning, so a redelivery under a new version is
        // a correction even when the number is identical. Money does not move.
        let d = displacement(
            Effect::Superseded,
            Some(value(10, 20_260_720_000_001, QualityFlag::Measured)),
            value(10, 20_260_728_000_002, QualityFlag::Corrected),
        );
        assert!(d.effect.changed_current_value());
        assert!(!d.value_changed(), "the quantity is the same");
        assert!(d.quality_changed(), "but the quality is not");
    }

    #[test]
    fn a_substitute_replaced_by_a_measurement_is_visible_even_at_an_equal_value() {
        // The § 60 Abs. 2 shape: the obligation discharges on the quality
        // change, which can leave the quantity untouched.
        let d = displacement(
            Effect::Superseded,
            Some(value(10, 20_260_720_000_001, QualityFlag::Substituted)),
            value(10, 20_260_728_000_002, QualityFlag::Measured),
        );
        assert!(d.quality_changed());
        assert!(!d.value_changed());
    }

    #[test]
    fn a_first_value_supersedes_nothing_and_still_counts_as_a_change() {
        let d = displacement(
            Effect::Inserted,
            None,
            value(10, 20_260_720_000_001, QualityFlag::Measured),
        );
        assert!(d.value_changed());
        assert!(
            !d.quality_changed(),
            "there is no prior quality to differ from"
        );
        assert_eq!(d.current().value, Decimal::new(10, 0));
    }
}
