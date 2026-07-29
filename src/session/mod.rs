//! The session layer: a handle that wires both tiers into a query engine.

pub mod catalog;
pub mod completeness;
pub mod displacement;
pub mod maintenance;
pub mod query;
pub mod series;
pub mod store;
pub mod system;
pub mod udf;

pub use catalog::{MeterCatalog, MeterCatalogBuilder};
pub use completeness::{Completeness, CompletenessFunction};
pub use displacement::{Displacement, Effect, StoredValue};
pub use maintenance::{Maintenance, MaintenanceOutcome};
pub use query::QueryResult;
pub use series::{ResolvedSeries, SeriesQuery};
pub use store::{AppendOutcome, HotWriter, MeterStore, MeterStoreBuilder};
pub use system::{ConfigEntry, SystemTables, TableStatus, register_all as register_system_tables};
pub use udf::all as calendar_udfs;
