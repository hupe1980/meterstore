//! Moving settled intervals from the hot store to the cold store.
//!
//! # Archival *is* the ingestion path
//!
//! There is no change-data-capture seam here, and that is a decision rather than
//! a gap. The comparison is closer than it first looks — logical decoding reads
//! the WAL and touches no heap pages, which is a real advantage on a busy primary
//! — but it does not solve the expensive half. Replicating rows out of PostgreSQL
//! does not *remove* them, and the hot tier has to stay bounded either way. Both
//! designs need the purge, and the purge is where the cost actually is; once it
//! is a partition drop ([`store::HotStore::drop_partition`]), what remains of
//! CDC's advantage does not pay for a replication slot that can fill the
//! primary's disk when a consumer stalls.
//!
//! There is deliberately no `ChangeSource` seam waiting for the day lake latency
//! has to drop below an hour either: a trait designed without a consumer is
//! designed against a guess, and the shape it would need is the one the first
//! real source imposes.

pub mod archive;
pub mod store;

pub use archive::{ArchivalOutcome, Archiver};
pub use store::{ColdStore, HotStore, PartitionId, SnapshotInfo, TableLease};
