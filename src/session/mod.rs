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
pub use completeness::{Completeness, CompletenessFunction};
pub use displacement::{Displacement, Effect, StoredValue};
pub use maintenance::{Maintenance, MaintenanceOutcome, TableMaintenance};
pub use query::{QueryDescription, QueryResult};
pub use readings::ReadingsQuery;
pub use series::{ResolvedSeries, SeriesQuery};
pub use store::{AUTHORITATIVE_ATTEMPTS, AppendOutcome, HotWriter, MeterStore, MeterStoreBuilder};
pub use surface::SqlSurface;
pub use system::{ConfigEntry, SystemTables, TableStatus, register_all as register_system_tables};
pub use udf::all as calendar_udfs;
