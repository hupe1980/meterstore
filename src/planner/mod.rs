//! Query planning across the two tiers.
//!
//! Local calendar arithmetic is **not** here: `metering::calendar` owns the
//! `Europe/Berlin` conversion, day lengths and DST-aware interval counts. This
//! crate re-exports the parts the query layer uses so callers need not depend on
//! both, but the implementation is upstream and there is only one of it.
//!
//! # Two kinds of day
//!
//! Electricity, heat and water balance on the **Berlin calendar day**, 00:00 to
//! 00:00 local. Gas does not: the German gas market balances on the **Gastag**,
//! 06:00 to 06:00 local (GaBi Gas, following Art. 3 Nr. 6 VO (EU) 312/2014).
//! Both are re-exported, and [`balancing_day`] picks between them from the
//! commodity so that a caller holding a mixed table does not have to.

pub mod calendar;
pub mod predicate;
pub mod provider;
pub mod resolved;
pub mod split;
pub mod version;

pub use metering::calendar::{
    DayKind, day_end_utc, day_kind, day_length, day_start_utc, gas_day_end_utc, gas_day_start_utc,
    intervals_in_day, local_day, local_gas_day, local_month, shift_back_days,
};

pub use calendar::{
    balancing_day, balancing_day_bounds, balancing_day_length, expected_intervals_in_balancing_day,
    gas_day_length, intervals_in_gas_day,
};
pub use predicate::{range_filters, time_range};
pub use provider::{ReadMode, SnapshotSelector, TieredTableProvider};
pub use resolved::ResolvedTableProvider;
pub use split::{TierSplit, TimeRange, split};
pub use version::{Resolution, VersionStats, resolution_sql};
