//! Query planning across the two tiers.
//!
//! Local calendar arithmetic is **not** here: `metering::calendar` owns the
//! `Europe/Berlin` conversion, day lengths and DST-aware interval counts. This
//! crate re-exports the parts the query layer uses so callers need not depend on
//! both, but the implementation is upstream and there is only one of it.

pub mod predicate;
pub mod provider;
pub mod resolved;
pub mod split;
pub mod version;

pub use metering::calendar::{
    DayKind, day_kind, day_length, day_start_utc, intervals_in_day, local_day, local_month,
};

pub use predicate::{range_filters, time_range};
pub use provider::{ReadMode, SnapshotSelector, TieredTableProvider};
pub use resolved::ResolvedTableProvider;
pub use split::{TierSplit, TimeRange, split};
pub use version::{Resolution, VersionStats, resolution_sql};
