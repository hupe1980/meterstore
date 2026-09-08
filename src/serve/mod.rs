//! Serving surfaces for engines that are not this process.
//!
//! The default answer for an external engine is **the Iceberg catalog, not an
//! endpoint here**: Spark, Trino, DuckDB and PyIceberg read the history straight
//! out of object storage, in parallel, with MeterStore nowhere in the data path.
//! This module exists for the one case where that is not enough — a
//! deployment on the SQL catalog, whose JDBC support across engines is uneven —
//! and it serves metadata only.
#[cfg(feature = "catalog-facade")]
pub mod catalog;
#[cfg(feature = "flight")]
pub mod flight;

#[cfg(feature = "catalog-facade")]
pub use catalog::CatalogFacade;
#[cfg(feature = "flight")]
pub use flight::FlightSqlServer;
