//! The cold tier: settled history, in Apache Iceberg.

pub mod catalog;
pub mod iceberg;
pub mod parquet;

#[cfg(feature = "rest-catalog")]
pub use catalog::IcebergRestCatalog;
#[cfg(feature = "sql-catalog")]
pub use catalog::IcebergSqlCatalog;
#[cfg(feature = "s3tables")]
pub use catalog::S3TablesCatalog;
pub use catalog::{ColdTier, WarehouseAuth};
pub use iceberg::IcebergCold;
