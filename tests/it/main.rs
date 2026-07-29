//! Every integration suite, as one test binary.
//!
//! Cargo compiles each top-level file in `tests/` into its own binary, and each
//! one links this crate's whole dependency graph — DataFusion, Arrow, Iceberg,
//! sqlx, tonic, axum — **statically**. At ~360 MB of unoptimised code per link,
//! twenty-two of them is roughly 8 GB of binaries, which exhausts a CI runner's
//! disk. The failure is not a tidy "no space left": `rust-lld` takes a **bus
//! error** writing an output it cannot extend, which reads as a linker bug and
//! is not one.
//!
//! Living under `tests/it/` makes them modules of one binary instead. Nothing
//! about a suite changes — each file keeps its own `#![cfg]` gates, fixtures and
//! names — but the graph is linked once rather than per file.

mod archival_end_to_end;
mod calendar_delegation;
mod cold_partitioning;
mod commodities;
mod displacement;
mod erasure;
mod flight_sql;
mod hot_postgres;
mod hot_writer;
mod interop;
mod interop_duckdb;
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
