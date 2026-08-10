//! Every integration suite, as one test binary.
//!
//! Cargo compiles each top-level file in `tests/` into its own test binary.
//! With this crate's dependency graph (DataFusion, Arrow, Iceberg, sqlx,
//! tonic, axum), each linked executable is roughly 360 MB in a debug build,
//! largely because of debug information. Twenty-two such binaries consume
//! around 8 GB of disk, enough to exhaust a GitHub Actions runner.
//!
//! The resulting failure is not a tidy "no space left on device":
//! `rust-lld` typically crashes with a bus error while extending the output
//! file, which can easily be mistaken for a linker bug.
//!
//! Placing the suites under `tests/it/` makes them modules of a single
//! integration test crate instead. Each suite keeps its own `#![cfg]` gates,
//! fixtures, and test names, while the dependency graph is linked only once.

mod archival_end_to_end;
mod calendar_delegation;
mod cold_partitioning;
mod commodities;
mod displacement;
mod erasure;
mod flight_sql;
mod foreign_catalog;
mod hot_postgres;
mod hot_writer;
mod interop;
mod interop_duckdb;
mod interop_pyiceberg;
mod measured;
mod multi_table;
mod purge_table;
mod query_end_to_end;
mod reproducibility_end_to_end;
mod sub_quarter_hour;
mod subject_erasure_store;
mod tenant_isolation;
mod tiering_oracle;
mod transaction_time;
mod typed_reads;
