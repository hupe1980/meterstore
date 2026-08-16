//! # MeterStore
//!
//! Hot/cold tiered store for metering time series: PostgreSQL holds the recent
//! interval window, Apache Iceberg holds the history, and a single explicit
//! timestamp — the *tiering watermark* — separates them.
//!
//! MeterStore is the persistence layer of the [mako](https://github.com/hupe1980/mako)
//! platform. It stores the types defined by the [`metering`] crate; it does not
//! redefine them and it does not compute with them.
//!
//! ## What this crate owns
//!
//! Exactly three things beyond storage mechanics:
//!
//! 1. **Correction versioning** — [`version`], implementing MSCONS's rule that
//!    versions are only comparable within a (network operator, month) scope.
//! 2. **The transaction-time axis** — `recorded_at`, giving bitemporality
//!    alongside `metering`'s valid time (`from`/`to`).
//! 3. **The tiering boundary** — [`watermark`].
//!
//! Everything else — DST calendars, unit conversion, quality semantics,
//! validation, Ersatzwertbildung — belongs to [`metering`]. Duplicating any of
//! it here would create a second implementation to keep correct, and it would
//! drift.
//!
//! [`planner`] re-exports `metering::calendar` for convenience, so callers get
//! DST-correct local days without depending on both crates directly. The
//! implementation is upstream and there is only one of it. The one thing
//! [`planner::calendar`] adds is not calendar arithmetic but a storage fact:
//! a stored row carries its `sparte`, so the store knows *which* of `metering`'s
//! two day definitions applies to it.
//!
//! ## The five things a caller usually wants
//!
//! - [`MeterStore::sql`] — DataFusion's own `DataFrame` over a table that spans
//!   both tiers. [`MeterStore::query`] returns the same rows plus the boundary
//!   they were computed against (P1).
//! - [`MeterStore::series`] — one measuring point as a `metering`
//!   `MeasurementSeries`, version-resolved and tier-split.
//! - [`MeterStore::as_of`] — the same queries against a pinned Iceberg snapshot
//!   and an optional version ceiling, for a settlement rerun.
//! - [`MeterStore::completeness`] — whether a range holds what the DST-aware
//!   calendar says it should, because a missing interval is information rather
//!   than an empty set.
//! - [`MeterCatalog`] — several tables in one session, when a deployment holds
//!   more than one stream and needs a statement that mentions both.
//!
//! ## What is stored
//!
//! All four Sparten. `value` carries the quantity and `unit` its dimension,
//! because water is metered *and billed* in m³ and gas may sit on either side of
//! the Brennwert conversion — a column named for kilowatt-hours would be wrong
//! for half of them. A unit the commodity cannot be expressed in is refused at
//! the write.
//!
//! Any declared resolution, not only the quarter-hour: completeness asks
//! `metering`'s calendar per day, so one-minute data expects 1 440 intervals on
//! an ordinary day and 1 500 on the 25-hour autumn one.
//!
//! And **two kinds of day**. Electricity, heat and water are balanced on the
//! Berlin calendar day; gas is balanced on the *Gastag*, 06:00 to 06:00 local.
//! Grouping a gas Lastgang by the calendar day books its 00:00–06:00 draw into
//! the neighbouring Bilanzierungstag — six hours a day, every day — so the
//! bucketing and the expected interval count both follow the row's Sparte, in
//! SQL through `meter_balancing_day` and in Rust through
//! [`planner::balancing_day`].
//!
//! An external engine has neither function, and SQL dialects differ on timestamp
//! arithmetic — so the answer is stored. Every row carries a `balancing_day`
//! column derived once by the encoder, and reading the Iceberg files directly
//! needs a `GROUP BY` and no calendar reasoning. It is the single derived value
//! this crate persists; [`encode::schema`](crate::encode::schema) argues the
//! exception.
//!
//! Identifiers are parsed rather than trusted. `malo_id` and `melo_id` are
//! `metering`'s [`MaloId`] and [`MeloId`] on both sides of the encoding: a
//! MaLo-ID carries a check digit so that a transposition is detectable, and a
//! store that accepted eleven arbitrary digits would throw that away at the one
//! point it still mattered.
//!
//! [`MaloId`]: metering::ids::MaloId
//! [`MeloId`]: metering::ids::MeloId
//!
//! ## Status
//!
//! Pre-alpha, and **unpublished on purpose**: the API is still settling, and
//! integrating against a real workload is what settles it. Encoding, tiering,
//! streaming archival, tier-split query
//! execution, reproducible reads, completeness, multi-table sessions and both
//! serving surfaces are implemented, and checked against an independently
//! implemented reference over generated workloads ([`testkit`]) on real
//! PostgreSQL and a real Iceberg warehouse. The planner's safety properties —
//! that an extracted range never excludes a row the filter admits, and that
//! every instant lands in exactly one tier — are asserted over generated inputs
//! rather than chosen ones.
//!
//! The output is checked to be readable **without this crate** — the files
//! opened by a bare Parquet reader, the published resolution SQL verified
//! against them, and a real **DuckDB** container reading both the Parquet and
//! the Iceberg metadata and agreeing. That is the substance of the open-format
//! claim.
//!
//! Compression against PostgreSQL row storage is measured on real
//! infrastructure — ~109×, comfortably past the >10× the design targets.
//!
//! Serving surfaces: a read-only Iceberg REST façade (`catalog-facade`) for
//! SQL-catalog deployments, and Flight SQL (`flight`) for the one thing an
//! external client cannot assemble for itself — the unified hot + cold view.
//!
//! The cold tier accepts any `Arc<dyn Catalog>`; two are built for you —
//! [`IcebergSqlCatalog`] over the same PostgreSQL as the hot tier, and
//! [`cold::S3TablesCatalog`] over an AWS S3 Tables table bucket
//! (`s3tables`).
//!
//! Not yet done: Spark and Trino interop, and the query-latency benchmarks, so
//! the p99 targets remain aspirational. Compaction and
//! orphan-file cleanup are blocked on the upstream `iceberg` crate.
//!
//! Full documentation: <https://hupe1980.github.io/meterstore>
//!
//! [`MeterStore::sql`]: crate::session::MeterStore::sql
//! [`MeterStore::query`]: crate::session::MeterStore::query
//! [`MeterStore::series`]: crate::session::MeterStore::series
//! [`MeterStore::as_of`]: crate::session::MeterStore::as_of
//! [`MeterStore::completeness`]: crate::session::MeterStore::completeness
//! [`MeterCatalog`]: crate::session::MeterCatalog

// Doc comments throughout cite `§N`. Those are cross-references between the
// design notes this crate's maintainers keep, not links a reader needs to
// follow: everything required to *use* the crate is in the documentation here,
// and the reasoning behind each decision is written out at
// <https://hupe1980.github.io/meterstore>. The markers exist so that changing a
// behaviour is traceable to the argument it rested on.

#![forbid(unsafe_code)]
#![warn(missing_docs, clippy::all)]

/// Arrow, re-exported from DataFusion.
///
/// Sourced through `datafusion` rather than as a direct dependency so the graph
/// cannot contain two incompatible `arrow` versions — a mismatch would make
/// `RecordBatch` and `TableProvider` types mutually unusable. Every module uses
/// `crate::arrow`, never a direct `arrow::` path.
pub use datafusion::arrow;

pub mod cold;
pub mod config;
pub mod encode;
pub mod erasure;
pub mod error;
pub mod evolution;
pub mod hot;
pub mod observe;
pub mod planner;
pub mod serve;
pub mod session;
pub mod settings;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod tiering;
pub mod version;
pub mod watermark;

#[cfg(feature = "s3tables")]
pub use cold::S3TablesCatalog;
pub use cold::{ColdTier, IcebergCold, IcebergSqlCatalog, WarehouseAuth};
pub use config::{CHECK_VALUES_KEY, TableConfig, ValidatedTableConfig, coded_column};
pub use encode::{canonical_obis, parse_malo};
pub use erasure::{ErasureRecord, SubjectRef, SubjectRegistry};
pub use error::{Error, Result};
pub use evolution::{Compatibility, SchemaChange};
pub use hot::PostgresHot;
pub use planner::{
    ReadMode, Resolution, SnapshotSelector, TierSplit, TieredTableProvider, TimeRange,
    balancing_day, intervals_in_gas_day,
};
pub use session::{
    Completeness, HotWriter, Maintenance, MaintenanceOutcome, MeterCatalog, MeterCatalogBuilder,
    MeterStore, MeterStoreBuilder, QueryDescription, QueryResult, ResolvedSeries, SeriesQuery,
};
pub use settings::Settings;
pub use tiering::{ArchivalOutcome, Archiver, ColdStore, HotStore, SnapshotInfo};
pub use version::{ScopedVersion, Version, VersionScope};
pub use watermark::{Tier, TieringWatermark};

/// Common imports for working with MeterStore.
pub mod prelude {
    #[cfg(feature = "s3tables")]
    pub use crate::cold::S3TablesCatalog;
    pub use crate::cold::{ColdTier, IcebergCold, IcebergSqlCatalog, WarehouseAuth};
    pub use crate::config::{CHECK_VALUES_KEY, TableConfig, ValidatedTableConfig, coded_column};
    pub use crate::encode::StoredSeries;
    pub use crate::erasure::{ErasureRecord, SubjectRef, SubjectRegistry};
    pub use crate::error::{Error, Result};
    pub use crate::evolution::{Compatibility, SchemaChange};
    pub use crate::hot::PostgresHot;
    pub use crate::planner::{
        ReadMode, Resolution, SnapshotSelector, TierSplit, TieredTableProvider, TimeRange,
        balancing_day, intervals_in_gas_day,
    };
    pub use crate::session::{
        Completeness, HotWriter, Maintenance, MaintenanceOutcome, MeterCatalog,
        MeterCatalogBuilder, MeterStore, MeterStoreBuilder, QueryDescription, QueryResult,
        ResolvedSeries, SeriesQuery,
    };
    pub use crate::settings::Settings;
    pub use crate::tiering::{ArchivalOutcome, Archiver, ColdStore, HotStore, SnapshotInfo};
    pub use crate::version::{ScopedVersion, Version, VersionScope};
    pub use crate::watermark::{Tier, TieringWatermark};
}
