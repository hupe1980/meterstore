//! The session layer: a handle that wires both tiers into a query engine.

pub mod catalog;
pub mod completeness;
pub mod displacement;
pub mod maintenance;
pub mod query;
pub mod readings;
pub mod series;
pub mod store;
pub mod surface;
pub mod system;
pub mod udf;

pub use catalog::{MeterCatalog, MeterCatalogBuilder};
pub use completeness::{Completeness, CompletenessFunction, CompletenessQuery};
pub use displacement::{Displacement, Effect, StoredValue};
pub use maintenance::{Maintenance, MaintenanceOutcome, RETENTION_LABEL, TableMaintenance};
pub use query::{QueryDescription, QueryResult};
pub use readings::ReadingsQuery;
pub use series::{ResolvedSeries, SeriesQuery};
pub use store::{
    AUTHORITATIVE_ATTEMPTS, AppendOutcome, AttributeAudit, HotWriter, MeterStore,
    MeterStoreBuilder, StoreAdmin,
};
pub use surface::SqlSurface;
pub use system::{ConfigEntry, SystemTables, TableStatus, register_all as register_system_tables};
/// Every SQL function this crate registers — the calendar, OBIS and EIC ones —
/// ready for a caller's own `SessionContext`.
pub use udf::all as sql_udfs;
