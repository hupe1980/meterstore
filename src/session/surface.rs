//! The read surface a serving endpoint needs, and nothing else.
//!
//! Two methods, because two is what serving needs: plan a statement without
//! running it, and run it as a stream. [`MeterStore`](super::MeterStore) and
//! [`MeterCatalog`](super::MeterCatalog) both implement it, so
//! [`FlightSqlServer`](crate::serve::FlightSqlServer) serves one table or every
//! table without knowing which it has.
//!
//! That matters because §15.3's ordinary deployment holds authoritative readings
//! *beside* a non-authoritative second stream, and the statement a BI tool wants
//! is frequently the one that mentions both — which only a catalog can express.
//!
//! # Deliberately not `query`
//!
//! Collecting a result is the wrong default for a surface an outsider can point
//! at: peak memory becomes the size of whatever a client asked for, on a socket
//! the client controls. The in-process caller who wants a `Vec<RecordBatch>` has
//! [`MeterStore::query`](super::MeterStore::query) and
//! [`MeterCatalog::query`](super::MeterCatalog::query) directly.

use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::scalar::ScalarValue;

use crate::error::Result;

use super::QueryDescription;

/// Something that answers SQL and says what it answered against.
///
/// Implemented by [`MeterStore`](super::MeterStore) — one table — and
/// [`MeterCatalog`](super::MeterCatalog) — several, sharing one session.
#[async_trait]
pub trait SqlSurface: Send + Sync + 'static {
    /// A short name for logs and `Debug`. Not addressable, not a table name.
    fn label(&self) -> String;

    /// What a statement would produce, **without running it**.
    ///
    /// The schema, the boundaries it would run against, and the tiers it would
    /// read. A surface that answered this by executing would make an Arrow Flight
    /// client's ordinary `GetFlightInfo` → `DoGet` sequence cost two full scans.
    async fn describe_sql(&self, sql: &str) -> Result<QueryDescription>;

    /// Plan a statement, then stream its rows.
    ///
    /// The [`QueryDescription`] comes back **before** the first batch, because a
    /// caller putting the boundary on the wire needs it before it starts writing
    /// (P1).
    async fn stream_sql(
        &self,
        sql: &str,
        params: Vec<ScalarValue>,
    ) -> Result<(QueryDescription, SendableRecordBatchStream)>;
}
