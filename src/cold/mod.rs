//! The cold tier: settled history, in Apache Iceberg.

pub mod iceberg;
pub mod parquet;

pub use iceberg::IcebergCold;
