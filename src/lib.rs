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
//! implementation is upstream and there is only one of it. The *choice* between
//! the two calendars is upstream too, as
//! [`DayBoundary`](metering::calendar::DayBoundary) — so the one thing
//! [`planner::calendar`] adds is neither arithmetic nor the choice, but a
//! storage fact: a stored row carries its `sparte`, so the store knows which
//! boundary applies to it. That mapping is [`planner::day_boundary`], written
//! once.
//!
//! ## The five things a caller usually wants
//!
//! - [`MeterStore::sql`] — DataFusion's own `DataFrame` over a table that spans
//!   both tiers. [`MeterStore::query`] returns the same rows plus the boundary
//!   they were computed against (P1), and [`MeterStore::stream`] returns that
//!   boundary *before* the rows, for a result too large to hold.
//! - [`MeterStore::series`] — one measuring point as a `metering`
//!   `MeasurementSeries`, version-resolved and tier-split. `collect` describes
//!   **one channel**, because a `MeasurementSeries` holds one `obis_code`;
//!   [`collect_by_channel`] describes the whole measuring point, splitting one
//!   scan into a series per channel rather than reading each in turn.
//! - [`MeterStore::as_of`] — the same queries against a pinned Iceberg snapshot
//!   and an optional version ceiling, for a settlement rerun.
//! - [`MeterStore::completeness`] — whether a range holds what the DST-aware
//!   calendar says it should, because a missing interval is information rather
//!   than an empty set. [`seen_since`] adds the finding a range cannot make about
//!   itself: a channel that delivered *nothing* has no rows to aggregate, so it
//!   needs a roster drawn from an earlier window.
//! - [`MeterCatalog`] — several tables in one session, when a deployment holds
//!   more than one stream and needs a statement that mentions both.
//!
//! Three more that a service exposing SQL will want: [`MeterStore::scoped`]
//! confines a session to one identity value, [`MeterCatalog::isolated`] confines
//! it to one table, and [`MeterCatalog::scoped`] confines **every** table of a
//! catalog to one identity value — which is what a multi-tenant deployment
//! putting a whole catalog on a socket needs, since the first two together force
//! a choice between the cross-table join and the tenant boundary. All three
//! inject into the plan, so caller-supplied SQL cannot step past them.
//! [`MeterStore::append_authoritative`] is the write path for a value the
//! operator authors rather than receives.
//!
//! ## What is stored
//!
//! **Two shapes**, declared per table by [`TimeModel`]. A *Lastgang* is energy
//! over `[from, to)`; a *Zählerstandsgang* is a cumulative register value at an
//! instant, which BK6-24-174 has made a primary record as voluminous as the
//! Lastgang derived from it. They are never the same table, because `value`
//! would mean two things in one column and no aggregate could tell them apart.
//!
//! The shape also decides what *names* a reading. A Marktlokation may be
//! measured by several Messlokationen, and both meters carry the same OBIS
//! register at the same instants. A load profile belongs to the market location,
//! so `melo_id` labels it; a register belongs to the meter, so `melo_id` names
//! it and joins the merge key. A point table does that by default;
//! [`TableConfig::identify_by_melo`] pins it either way.
//!
//! [`TableConfig::identify_by_melo`]: crate::config::TableConfig::identify_by_melo
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
//! [`planner::balancing_day`]. The month is the same choice one period up —
//! `meter_balancing_month` and [`planner::balancing_month`].
//!
//! The boundary carries up to the **month**, which is the market's own rule:
//! EDI@Energy *Allgemeine Festlegungen* v6.1c, Kap. 3.1 defines the gas
//! Bilanzierungsmonat as 01.06 06:00 to 01.07 06:00. So a version scope — which
//! is `(network operator, month)` — is cut at 06:00 for gas too, and every
//! [`VersionScope`] constructor takes a `Sparte` for that reason.
//!
//! An external engine has neither function, and SQL dialects differ on timestamp
//! arithmetic — so the answer is stored. Every row carries a `balancing_day`
//! column derived once by the encoder, and reading the Iceberg files directly
//! needs a `GROUP BY` and no calendar reasoning. It is the single derived value
//! this crate persists; [`encode::schema`] argues the exception.
//!
//! Identifiers are parsed rather than trusted. `malo_id` and `melo_id` are
//! `metering`'s [`MaloId`] and [`MeloId`] on both sides of the encoding: a
//! MaLo-ID carries a check digit so that a transposition is detectable, and a
//! store that accepted eleven arbitrary digits would throw that away at the one
//! point it still mattered.
//!
//! A deployment's own columns get the same treatment when they hold an
//! identifier. [`checked_column`] declares one whose values must parse as a
//! [`ValueCheck`] — an [`Eic`], the ENTSO-E code a Bilanzkreis is addressed by;
//! a [`MaloId`] or [`MeloId`], for a reading that references a measuring point
//! it is not keyed to; or a [`BdewCode`], the Marktpartner-ID of a Lieferant or
//! Messstellenbetreiber. Values are stored canonicalised, so such a column may
//! sit in the merge key without two spellings becoming two readings.
//!
//! Each scheme stops somewhere different, and [`ValueCheck`] says where: the EIC
//! check character and the MaLo check digit are enforced, a MeLo has no check
//! digit to enforce, and a Marktpartner-ID's thirteenth digit is deliberately
//! *not* checked — BDEW's Bildungsvorschrift carves out GS1-issued GLNs, which
//! is the same reason [`VersionScope`] does not check it either.
//!
//! An EIC column may name the **object type** it holds —
//! `ValueCheck::Eic(Some(EicType::Party))` for a Bilanzkreis, `Area` for a
//! Bilanzierungsgebiet. The two share the alphabet, the length and the check
//! character, so position 3 is the only thing that tells them apart — and unlike
//! the check character it is expressible as a regular expression, so declaring it
//! strengthens the hot table's `CHECK` as well as the write path.
//!
//! [`MaloId`]: metering::ids::MaloId
//! [`MeloId`]: metering::ids::MeloId
//! [`Eic`]: metering::ids::Eic
//! [`BdewCode`]: metering::ids::BdewCode
//! [`checked_column`]: crate::config::checked_column
//! [`ValueCheck`]: crate::config::ValueCheck
//!
//! ## Tiers and surfaces
//!
//! The cold tier accepts any `Arc<dyn Catalog>`; three are built for you —
//! [`IcebergSqlCatalog`] over the same PostgreSQL as the hot tier,
//! [`cold::IcebergRestCatalog`] over Polaris/Lakekeeper/Nessie/Gravitino
//! (`rest-catalog`, on by default), and [`cold::S3TablesCatalog`] over an AWS S3
//! Tables table bucket (`s3tables`). [`Settings::connect`] builds whichever a
//! configuration file names, along with the pool and every validated table.
//!
//! Two serving surfaces: a read-only Iceberg REST façade (`catalog-facade`) for
//! SQL-catalog deployments, and Flight SQL (`flight`) for the unified hot + cold
//! view — the one thing an external client cannot assemble for itself. Flight
//! serves any [`SqlSurface`], so a whole [`MeterCatalog`] goes on a socket as
//! readily as one table.
//!
//! And a command line, behind `cli`: [`cli`] is the `meterstore` binary over the
//! same public API — `check`, `create`, `status`, `archive`, `maintain`,
//! `query`, `completeness`, `audit` and `serve` among its verbs — for the questions an
//! operator asks during an incident and the archival loop a deployment has to
//! run somewhere.
//!
//! [`Settings::connect`]: crate::settings::Settings::connect
//!
//! ## Errors carry what to do about them
//!
//! [`Error`] is `#[non_exhaustive]` and callers match on variants rather than
//! parsing strings. [`Error::is_retryable`] is the split that matters most: a
//! lost connection and a lock a statement declined to wait for are worth
//! retrying, and a refused delivery, an invalid configuration and a statement
//! that will not plan are not — retrying those is a loop on a message that will
//! never change.
//!
//! Two are deliberately distinct and are the pair most often conflated.
//! [`Error::IntegrityViolation`] means the store **stopped something from
//! becoming true**: an overlapping delivery, two network operators for one
//! reading, a value restated under an existing version. The producer has to
//! change. [`Error::InvariantViolated`] means something **already is true that
//! should not be** — rows below the watermark still in PostgreSQL, two version
//! scopes for one reading. An operator has to look, and whoever is paged for the
//! second must not be woken by the first.
//!
//! [`Error::is_retryable`]: crate::error::Error::is_retryable
//! [`Error::IntegrityViolation`]: crate::error::Error::IntegrityViolation
//! [`Error::InvariantViolated`]: crate::error::Error::InvariantViolated
//!
//! ## Status
//!
//! Pre-alpha, and **unpublished on purpose**: the API is still settling, and
//! integrating against a real workload is what settles it. What is implemented,
//! what is measured and what is not yet done are in the
//! [repository README](https://github.com/hupe1980/meterstore#status);
//! [`testkit`] is the reference the store is checked against, and is public so a
//! deployment can run it over its own configuration.
//!
//! Full documentation: <https://hupe1980.github.io/meterstore>
//!
//! [`MeterStore::sql`]: crate::session::MeterStore::sql
//! [`MeterStore::query`]: crate::session::MeterStore::query
//! [`MeterStore::stream`]: crate::session::MeterStore::stream
//! [`MeterStore::series`]: crate::session::MeterStore::series
//! [`MeterStore::as_of`]: crate::session::MeterStore::as_of
//! [`MeterStore::completeness`]: crate::session::MeterStore::completeness
//! [`MeterCatalog`]: crate::session::MeterCatalog
//! [`MeterCatalog::isolated`]: crate::session::MeterCatalog::isolated
//! [`MeterCatalog::scoped`]: crate::session::MeterCatalog::scoped
//! [`MeterStore::scoped`]: crate::session::MeterStore::scoped
//! [`MeterStore::append_authoritative`]: crate::session::MeterStore::append_authoritative
//! [`SqlSurface`]: crate::session::SqlSurface
//! [`TimeModel`]: crate::config::TimeModel
//! [`collect_by_channel`]: crate::session::SeriesQuery::collect_by_channel
//! [`seen_since`]: crate::session::CompletenessQuery::seen_since

// A `§` in a doc comment is always a clause of a *named external* document — a
// statute, a BDEW Anwendungshilfe, the ENTSO-E EIC Reference Manual — and the
// surrounding sentence names it. Nothing here cites this crate's own notes by
// number: the reasoning is written out beside the code and at
// <https://hupe1980.github.io/meterstore>.

#![forbid(unsafe_code)]
#![warn(missing_docs, clippy::all)]

/// Arrow, re-exported from DataFusion.
///
/// Sourced through `datafusion` rather than as a direct dependency so the graph
/// cannot contain two incompatible `arrow` versions — a mismatch would make
/// `RecordBatch` and `TableProvider` types mutually unusable. Every module uses
/// `crate::arrow`, never a direct `arrow::` path.
pub use datafusion::arrow;

#[cfg(feature = "cli")]
pub mod cli;
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

#[cfg(feature = "rest-catalog")]
pub use cold::IcebergRestCatalog;
#[cfg(feature = "sql-catalog")]
pub use cold::IcebergSqlCatalog;
#[cfg(feature = "s3tables")]
pub use cold::S3TablesCatalog;
pub use cold::{ColdTier, IcebergCold, WarehouseAuth};
pub use config::{
    CHECK_VALUES_KEY, EicType, TableConfig, TimeModel, VALUE_CHECK_KEY, ValidatedTableConfig,
    ValueCheck, checked_column, coded_column, declared_value_check,
};
pub use encode::{StoredReadings, canonical_obis, parse_malo, parse_melo};
pub use erasure::{
    DEFAULT_ERASURE_LIMIT, ErasureQuery, ErasureRecord, ErasureTrigger, MIN_ERASURE_SECRET_BYTES,
    MIN_REFERENCE_TOKEN_CHARS, Retention, SubjectRef, SubjectRegistration, SubjectRegistry,
    SuppressionLift, retention_epoch,
};
pub use error::{Error, Result};
pub use evolution::{Compatibility, SchemaChange};
pub use hot::PostgresHot;
pub use planner::{
    ReadMode, Resolution, SnapshotSelector, TierSplit, TieredTableProvider, TimeRange,
    balancing_day, balancing_day_bounds, balancing_day_length, balancing_month,
    balancing_month_bounds, bilanzierungsmonat, day_boundary, expected_intervals_in_balancing_day,
};
pub use session::{
    AUTHORITATIVE_ATTEMPTS, AttributeAudit, Completeness, CompletenessQuery, HotWriter,
    Maintenance, MaintenanceOutcome, MeterCatalog, MeterCatalogBuilder, MeterStore,
    MeterStoreBuilder, QueryDescription, QueryResult, RETENTION_LABEL, ReadingsQuery,
    ResolvedSeries, SeriesQuery, SqlSurface, TableMaintenance,
};
pub use settings::{Deployment, PrivacySettings, Settings};
pub use tiering::{ArchivalOutcome, Archiver, ColdStore, HotStore, SnapshotInfo};
pub use version::{ScopedVersion, Version, VersionScope};
pub use watermark::{Tier, TieringWatermark};

/// Common imports for working with MeterStore.
/// The names a caller actually types.
///
/// Narrower than the crate root on purpose: everything here is something a
/// deployment writes in its own code. The Arrow metadata keys a declaration
/// rides in, the retry counts, the label a scheduled sweep records — those are
/// reachable at their own paths and are not things anyone imports.
pub mod prelude {
    #[cfg(feature = "rest-catalog")]
    pub use crate::cold::IcebergRestCatalog;
    #[cfg(feature = "sql-catalog")]
    pub use crate::cold::IcebergSqlCatalog;
    #[cfg(feature = "s3tables")]
    pub use crate::cold::S3TablesCatalog;
    pub use crate::cold::{ColdTier, IcebergCold, WarehouseAuth};
    pub use crate::config::{
        EicType, TableConfig, TimeModel, ValidatedTableConfig, ValueCheck, checked_column,
        coded_column,
    };
    pub use crate::encode::{StoredReadings, StoredSeries};
    pub use crate::erasure::{
        ErasureQuery, ErasureRecord, ErasureTrigger, MIN_ERASURE_SECRET_BYTES, Retention,
        SubjectRef, SubjectRegistration, SubjectRegistry, SuppressionLift, retention_epoch,
    };
    pub use crate::error::{Error, Result};
    pub use crate::evolution::{Compatibility, SchemaChange};
    pub use crate::hot::PostgresHot;
    pub use crate::planner::{
        ReadMode, Resolution, SnapshotSelector, TierSplit, TieredTableProvider, TimeRange,
        balancing_day, balancing_day_bounds, balancing_day_length, balancing_month,
        balancing_month_bounds, bilanzierungsmonat, day_boundary,
        expected_intervals_in_balancing_day,
    };
    pub use crate::session::{
        AttributeAudit, Completeness, CompletenessQuery, HotWriter, Maintenance,
        MaintenanceOutcome, MeterCatalog, MeterCatalogBuilder, MeterStore, MeterStoreBuilder,
        QueryDescription, QueryResult, ReadingsQuery, ResolvedSeries, SeriesQuery, SqlSurface,
        TableMaintenance,
    };
    pub use crate::settings::{Deployment, PrivacySettings, Settings};
    pub use crate::tiering::{ArchivalOutcome, Archiver, ColdStore, HotStore, SnapshotInfo};
    pub use crate::version::{ScopedVersion, Version, VersionScope};
    pub use crate::watermark::{Tier, TieringWatermark};
}
