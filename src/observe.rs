//! Metrics.
//!
//! Instruments are created against the OpenTelemetry **API**, not an SDK. Until
//! an application installs a meter provider the whole module is a no-op, which
//! is the contract a library should offer: it decides what is worth measuring,
//! the application decides where measurements go — Prometheus, OTLP, or nowhere.
//!
//! # What is worth measuring
//!
//! Two questions an operator actually asks, and one that decides whether the
//! cold tier is behaving:
//!
//! - **Is anything wrong right now?** `invariant_violations` is the only metric
//!   that means query results may be incorrect. Everything else is degradation.
//! - **Will something be wrong soon?** `watermark_lag` and
//!   `hot_partitions_ahead` both trend toward failure before they cause one —
//!   running out of partitions stops writes outright.
//! - **How much of the history is settled?** `merge_elided` over
//!   `merge_elision_decisions` says how often a historical scan skipped version
//!   resolution. It falls as corrections accumulate in the ranges being queried
//!   — which is the *data* changing, not the layout degrading. Compaction would
//!   not recover it and would cost some of it: a corrected reading has two
//!   versions stored however the bytes are arranged, and a coarser file makes
//!   more uncorrected keys share one with it.

use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::{KeyValue, global};

/// The instrument namespace.
const SCOPE: &str = "meterstore";

/// Every instrument MeterStore records.
pub struct Metrics {
    /// Rows moved from hot to cold.
    pub archival_rows: Counter<u64>,
    /// How long one archival window took.
    pub archival_duration: Histogram<f64>,
    /// Archival runs that failed.
    pub archival_failures: Counter<u64>,
    /// Archival cycles that stopped because a lock was not available.
    ///
    /// **Not a failure**, and separate from one for that reason: nothing was
    /// changed and the next cycle retries. It is worth counting because a table
    /// that defers *every* cycle is a table whose watermark is not moving, and
    /// the cause is on the database — a long query, or a session idle in a
    /// transaction — rather than here. Alert on
    /// [`watermark_lag`](Self::watermark_lag); read this to explain it.
    pub archival_deferred: Counter<u64>,
    /// Partitions dropped after a successful commit.
    pub partitions_dropped: Counter<u64>,
    /// Partitions reclaimed from an interrupted run.
    pub orphans_reclaimed: Counter<u64>,

    /// Rows written to the hot tier.
    pub rows_written: Counter<u64>,
    /// Rows skipped as already present — the redelivery rate.
    pub rows_deduplicated: Counter<u64>,
    /// Corrections routed to the cold tier because their interval was archived.
    pub late_corrections: Counter<u64>,
    /// Subjects whose linkage was destroyed, by what triggered it.
    ///
    /// **Compliance rather than health**, and the only irreversible operation
    /// this crate performs — so it is the one counter here whose *rise* is worth
    /// a look, and the one whose flat zero is too.
    ///
    /// Split by [`erasure_trigger`] because the two have opposite readings and
    /// summing them hides both:
    ///
    /// - `retention` is the § 60 Abs. 6 MsbG sweep, which comes due on a clock.
    ///   A flat zero over a year is a sweep that is **not running**.
    /// - `request` is an Article 17 erasure, which arrives when a data subject
    ///   asks. A flat zero is ordinary; a rise is a thing to know about.
    ///
    /// Folded together, a deployment whose sweep never ran but which handled the
    /// occasional request would look like one whose sweep was working.
    ///
    /// No `table` attribute: the registry is deployment-wide and a subject's
    /// linkage is destroyed everywhere at once, so attributing the count to a
    /// table would invite a per-table sum that double-counts it.
    pub subjects_erased: Counter<u64>,

    /// Registrations refused because the identifier is on the suppression list.
    ///
    /// **The replay alarm.** Every one of these is a pipeline handing the store
    /// an identifier whose linkage Article 17 destroyed — a broker redelivering a
    /// batch from before the erasure, a reprocessing job reading an old offset, a
    /// nightly import from a system that never learned. The registration is
    /// refused, so nothing is re-linked; what the counter reports is that
    /// something upstream is still carrying data it should have dropped, which is
    /// a finding in its own right and is invisible from the erasure counter.
    ///
    /// Zero is the expected reading. A rise that does not decay means an upstream
    /// system needs the erasure applying to it too.
    pub registrations_suppressed: Counter<u64>,

    /// Rows read, by tier.
    pub rows_scanned: Counter<u64>,
    /// How long it took to *plan* a tiered scan.
    ///
    /// Distinct from [`Metrics::scan_duration`] on purpose. Planning is a
    /// catalog round trip plus, for the resolved table, a manifest read; scanning
    /// is the data. Sharing one instrument made a slow catalog look like a slow
    /// query and hid a slow scan behind fast planning.
    pub plan_duration: Histogram<f64>,
    /// How long a tier's scan took to drain, by tier.
    pub scan_duration: Histogram<f64>,
    /// Scans that reached the elision decision at all.
    pub merge_elision_decisions: Counter<u64>,
    /// Scans that skipped version resolution.
    ///
    /// Divided by `merge_elision_decisions` this is the elided ratio: how much
    /// of the queried history is settled enough to read without ranking. See the
    /// module documentation for why compaction is not the lever it looks like.
    pub merge_elided: Counter<u64>,

    /// Seconds between the watermark and wall clock.
    pub watermark_lag: Gauge<u64>,
    /// Hot partitions left before inserts start failing.
    pub hot_partitions_ahead: Gauge<u64>,
    /// **Non-zero means query results may be wrong.** The alert.
    pub invariant_violations: Gauge<u64>,
}

impl Metrics {
    /// Build instruments against a meter.
    pub fn new(meter: &Meter) -> Self {
        Self {
            archival_rows: meter
                .u64_counter("meterstore.archival.rows")
                .with_description("Rows moved from the hot tier to the cold tier")
                .build(),
            archival_duration: meter
                .f64_histogram("meterstore.archival.duration")
                .with_unit("s")
                .with_description("Time to archive one window")
                .build(),
            archival_failures: meter
                .u64_counter("meterstore.archival.failures")
                .with_description("Archival runs that did not complete")
                .build(),
            archival_deferred: meter
                .u64_counter("meterstore.archival.deferred")
                .with_description(
                    "Archival runs that stopped because a lock was not available. \
                     Not a failure: nothing was changed and the next cycle retries",
                )
                .build(),
            subjects_erased: meter
                .u64_counter("meterstore.subjects_erased")
                .with_description(
                    "Subjects whose linkage was destroyed, by trigger. Irreversible. \
                     trigger=retention is the § 60 Abs. 6 sweep, where a flat zero over \
                     a year means it is not running; trigger=request is an Article 17 \
                     erasure, where a rise is the thing to know about",
                )
                .build(),
            registrations_suppressed: meter
                .u64_counter("meterstore.registrations_suppressed")
                .with_description(
                    "Registrations refused because the identifier is on the suppression \
                     list. Zero is the expected reading: each one is a pipeline replaying \
                     data from before an Article 17 erasure, which is refused here and \
                     still needs fixing upstream",
                )
                .build(),
            partitions_dropped: meter
                .u64_counter("meterstore.partitions.dropped")
                .with_description("Hot partitions dropped after a durable cold commit")
                .build(),
            orphans_reclaimed: meter
                .u64_counter("meterstore.partitions.orphans_reclaimed")
                .with_description("Detached partitions reclaimed from an interrupted run")
                .build(),

            rows_written: meter
                .u64_counter("meterstore.write.rows")
                .with_description("Rows written to the hot tier")
                .build(),
            rows_deduplicated: meter
                .u64_counter("meterstore.write.rows_deduplicated")
                .with_description("Rows skipped as already present — the redelivery rate")
                .build(),
            late_corrections: meter
                .u64_counter("meterstore.write.late_corrections")
                .with_description("Corrections routed to the cold tier")
                .build(),

            rows_scanned: meter
                .u64_counter("meterstore.query.rows_scanned")
                .with_description("Rows read, by tier")
                .build(),
            plan_duration: meter
                .f64_histogram("meterstore.query.plan_duration")
                .with_unit("s")
                .with_description("Time to plan a tiered scan — catalog and manifest reads")
                .build(),
            scan_duration: meter
                .f64_histogram("meterstore.query.scan_duration")
                .with_unit("s")
                .with_description("Time to drain one tier's scan")
                .build(),
            merge_elision_decisions: meter
                .u64_counter("meterstore.query.merge_elision_decisions")
                .with_description("Scans that evaluated whether resolution could be skipped")
                .build(),
            merge_elided: meter
                .u64_counter("meterstore.query.merge_elided")
                .with_description(
                    "Scans that skipped version resolution — over decisions, the \
                     elided ratio",
                )
                .build(),

            watermark_lag: meter
                .u64_gauge("meterstore.tiering.watermark_lag")
                .with_unit("s")
                .with_description("Seconds between the tiering watermark and wall clock")
                .build(),
            hot_partitions_ahead: meter
                .u64_gauge("meterstore.tiering.hot_partitions_ahead")
                .with_description("Partitions remaining before inserts fail")
                .build(),
            invariant_violations: meter
                .u64_gauge("meterstore.tiering.invariant_violations")
                .with_description(
                    "Rows below the watermark still in the hot tier — non-zero means \
                     query results may be wrong",
                )
                .build(),
        }
    }
}

/// The process-wide instruments.
///
/// One set, built on first use. Instruments are cheap to clone and cheaper to
/// record against, but constructing them per call would allocate on a hot path
/// for no benefit.
pub fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| Metrics::new(&global::meter(SCOPE)))
}

/// The attribute every instrument carries.
pub fn table(name: &str) -> [KeyValue; 1] {
    [KeyValue::new("table", name.to_string())]
}

/// What triggered an erasure, for [`Metrics::subjects_erased`].
///
/// The two are counted apart rather than summed because their readings are
/// opposite: a flat `retention` series is a sweep that is not running, and a flat
/// `request` series is an ordinary quarter.
///
/// The value comes from [`ErasureTrigger`](crate::erasure::ErasureTrigger) rather
/// than from a string constant of
/// this module's own, so the attribute on the counter and the column in the
/// audit trail cannot come to disagree about how a duty is spelled.
pub fn erasure_trigger(trigger: crate::erasure::ErasureTrigger) -> [KeyValue; 1] {
    [KeyValue::new("trigger", trigger.as_str())]
}

/// Table plus tier, for metrics that distinguish them.
pub fn table_tier(name: &str, tier: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new("table", name.to_string()),
        KeyValue::new("tier", tier),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erasure::ErasureTrigger;

    #[test]
    fn instruments_build_without_an_sdk() {
        // The library must be usable by an application that installs no meter
        // provider at all; recording then goes nowhere rather than panicking.
        let m = metrics();
        m.archival_rows.add(1, &table("readings"));
        m.watermark_lag.record(42, &table("readings"));
        m.plan_duration.record(0.01, &table("readings"));
        m.scan_duration
            .record(0.01, &table_tier("readings", "cold"));
        // Deployment-wide, so it carries no table attribute — the empty slice is
        // as much a case as any other.
        m.subjects_erased
            .add(1, &erasure_trigger(ErasureTrigger::Request));
        m.subjects_erased
            .add(1, &erasure_trigger(ErasureTrigger::Retention));
        m.registrations_suppressed.add(1, &[]);
    }

    #[test]
    fn the_two_erasure_triggers_are_distinct_attribute_values() {
        // They are counted apart because their readings are opposite: a flat
        // `retention` series is a sweep that is not running, and a flat `request`
        // series is an ordinary quarter. One value for both would fold the two
        // and hide the first.
        assert_ne!(
            ErasureTrigger::Retention.as_str(),
            ErasureTrigger::Request.as_str()
        );
        assert_ne!(
            erasure_trigger(ErasureTrigger::Retention)[0].value,
            erasure_trigger(ErasureTrigger::Request)[0].value
        );
        assert_eq!(
            erasure_trigger(ErasureTrigger::Request)[0].key.as_str(),
            "trigger"
        );
    }

    #[test]
    fn the_instrument_set_is_built_once() {
        assert!(std::ptr::eq(metrics(), metrics()));
    }

    #[test]
    fn attributes_name_the_table() {
        let attrs = table("readings_versions");
        assert_eq!(attrs[0].key.as_str(), "table");
        assert_eq!(attrs[0].value.as_str(), "readings_versions");
    }

    #[test]
    fn tier_attributes_distinguish_the_halves() {
        // Rows scanned is only meaningful split by tier: the same total means
        // very different things depending on where it came from.
        let cold = table_tier("readings", "cold");
        let hot = table_tier("readings", "hot");
        assert_ne!(cold[1].value.as_str(), hot[1].value.as_str());
    }
}
