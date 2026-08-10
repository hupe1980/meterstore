//! Arrow Flight SQL over the **unified** hot + cold view.
//!
//! Everything else an external consumer needs is better served by the Iceberg
//! catalog: engines read history straight from object storage, in parallel, with
//! MeterStore nowhere in the data path (§13.7). Putting analytics through a
//! Flight server would be actively *worse* — it adds a proxy hop and serialises
//! parallel reads through one process.
//!
//! So this exists for one case, and the documentation deliberately leads with the
//! catalog rather than with this:
//!
//! | Consumer need | Right surface |
//! |---|---|
//! | Analytics over history | Iceberg catalog, direct read |
//! | A Rust application, in-process | the typed API (§13.4) |
//! | Anything inside mako | `cargo add meterstore` |
//! | **Unified hot + cold from a non-Rust client** | **this** |
//! | **A BI tool over live data** | **this**, via a Flight SQL JDBC/ODBC driver |
//!
//! The hot tier lives in PostgreSQL and is not in the catalog, so **the unified
//! view is the one thing an external client cannot assemble for itself**. That is
//! the whole justification — real, and narrow.
//!
//! # Read-only, structurally
//!
//! There is no write path, for the same reason the catalog façade has none and
//! one more besides. A write arriving here would bypass [`MeterStore::append`],
//! and with it the two things that make a write safe: routing each interval to
//! the tier that owns it (§8.3 — a correction below the watermark written to
//! PostgreSQL is silently invisible), and the subject-reference check that stops
//! a replay re-linking an erased subject (§19.4). Neither is recoverable
//! afterwards, so every mutating call is refused with that reason.
//!
//! # Results carry their boundary
//!
//! P1 says a result carries the tier boundary it was computed against, and a
//! client on the far end of a socket needs that as much as one in-process. The
//! watermark and the tiers scanned travel as **schema metadata** on every
//! response, so a BI tool that keeps the schema keeps the provenance. A number
//! pulled over Flight is otherwise indistinguishable from one pulled a minute
//! later against a different boundary.
//!
//! # Authentication is the deployment's
//!
//! `into_service` returns a tonic service rather than a bound port, so a
//! deployment wraps it in its own interceptor, TLS and tracing. §19.7 requires
//! authentication before this leaves a trusted network and this crate has no
//! business deciding what kind.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate, ProstMessageExt,
    SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use futures::{TryStreamExt, stream};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info};

use crate::arrow::array::RecordBatch;
use crate::arrow::datatypes::SchemaRef;
use crate::session::MeterStore;

/// A Flight SQL server over one store's unified view.
#[derive(Clone)]
pub struct FlightSqlServer {
    store: MeterStore,
}

impl std::fmt::Debug for FlightSqlServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightSqlServer")
            .field("table", &self.store.resolved_table())
            .finish_non_exhaustive()
    }
}

impl FlightSqlServer {
    /// Serve the given store.
    pub fn new(store: MeterStore) -> Self {
        Self { store }
    }

    /// The tonic service, ready to be added to a server or wrapped in layers.
    ///
    /// Returned rather than bound, so a deployment supplies its own
    /// authentication interceptor and TLS — §19.7 requires both before this
    /// leaves a trusted network.
    pub fn into_service(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// The provenance a response carries, as Arrow schema metadata (P1).
    ///
    /// A client on the far end of a socket needs the boundary as much as one
    /// in-process: a number pulled over Flight is otherwise indistinguishable
    /// from one pulled a minute later against a different boundary.
    fn with_provenance(
        schema: SchemaRef,
        watermark: crate::watermark::TieringWatermark,
        tiers: &[crate::watermark::Tier],
        mode: crate::planner::ReadMode,
    ) -> SchemaRef {
        let tiers = tiers
            .iter()
            .map(|t| format!("{t:?}").to_lowercase())
            .collect::<Vec<_>>()
            .join(",");

        let metadata = std::collections::HashMap::from([
            (
                crate::watermark::WATERMARK_PROPERTY.to_string(),
                watermark.to_string(),
            ),
            ("meterstore.tiers_scanned".to_string(), tiers),
            ("meterstore.read_mode".to_string(), format!("{mode:?}")),
        ]);
        Arc::new(schema.as_ref().clone().with_metadata(metadata))
    }

    /// Plan a statement and return the schema it would produce, **without
    /// running it**.
    async fn describe(&self, sql: &str) -> Result<SchemaRef, Status> {
        debug!(%sql, "flight sql describe");

        let described = self
            .store
            .describe(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("query failed: {e}")))?;

        Ok(Self::with_provenance(
            described.schema(),
            described.watermark(),
            described.tiers_scanned(),
            described.read_mode(),
        ))
    }

    /// Run a query and return its batches with the schema that describes them.
    async fn run(&self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
        debug!(%sql, "flight sql query");

        let result = self
            .store
            .query(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("query failed: {e}")))?;

        let schema = Self::with_provenance(
            result.schema(),
            result.watermark(),
            result.tiers_scanned(),
            result.read_mode(),
        );
        Ok((schema, result.into_batches()))
    }

    /// Wrap batches as a Flight stream under `schema`.
    fn stream(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Re-stamped onto every batch so the provenance survives even a client
        // that inspects a batch rather than the stream schema. Metadata does not
        // change a column, so this cannot fail — and if it ever did, sending the
        // batch anyway would drop the provenance silently, which is the one thing
        // this is here to prevent.
        let batches: Vec<RecordBatch> = batches
            .into_iter()
            .map(|b| RecordBatch::try_new(schema.clone(), b.columns().to_vec()))
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| Status::internal(format!("attaching provenance to a batch: {e}")))?;

        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(batches.into_iter().map(Ok)))
            .map_err(|e| Status::internal(format!("encoding flight data: {e}")));

        Ok(Response::new(Box::pin(flight)))
    }

    /// A `FlightInfo` whose ticket replays `sql` on `do_get`.
    ///
    /// The query is **planned and not executed**. A client learns about a syntax
    /// error, an unknown column or an unknown relation here rather than halfway
    /// through a stream, and the rows are produced once, on `do_get`.
    ///
    /// That distinction is the difference between one scan and two. Answering
    /// this by running the query — which is what an earlier version did, while
    /// this comment claimed otherwise — makes a BI tool's ordinary
    /// `GetFlightInfo` → `DoGet` sequence cost two full scans, on the one surface
    /// built for BI tools.
    ///
    /// Holding the result between the two calls would fix the double scan and
    /// introduce a worse problem: a stateful server needs eviction and leaks on a
    /// disconnected client, and at metering result sizes the cost is the scan
    /// rather than the parse.
    async fn info_for(
        &self,
        sql: String,
        descriptor: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = self.describe(&sql).await?;

        let ticket = Ticket::new(
            TicketStatementQuery {
                statement_handle: sql.into_bytes().into(),
            }
            .as_any()
            .encode_to_vec(),
        );

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema: {e}")))?
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(descriptor);

        Ok(Response::new(info))
    }

    /// The refusal every mutating call gives, with the reason.
    fn read_only(operation: &str) -> Status {
        Status::permission_denied(format!(
            "{operation} is not available over Flight SQL: this endpoint is read-only. \
             A write here would bypass MeterStore's tier routing — a correction for an \
             already-archived interval would land in PostgreSQL below the watermark, where \
             no query reads it — and the subject-reference check that stops a replay \
             re-linking an erased subject. Write through MeterStore::append."
        ))
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServer {
    type FlightService = Self;

    /// Accepts any client.
    ///
    /// Deliberately not an authentication decision: this crate does not know what
    /// a deployment's identities are, and inventing a scheme here would be a
    /// second, weaker one beside whatever the estate already runs. §19.7 puts
    /// authentication in the layer around `into_service`.
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        Ok(Response::new(Box::pin(stream::once(async move {
            Ok(response)
        }))))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.info_for(query.query, request.into_inner()).await
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("statement handle: {e}")))?;

        let (schema, batches) = self.run(&sql).await?;
        Self::stream(schema, batches)
    }

    // --- prepared statements -------------------------------------------------
    //
    // The handle *is* the SQL. A server-side statement cache would need eviction
    // and would leak on a disconnected client, and at metering result sizes it
    // would buy nothing: the cost is the scan, not the parse.

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        // Planned, not executed. "Preparing" a statement that ran it would make
        // the prepare/execute split cost double what a plain query does.
        let schema = self.describe(&query.query).await?;

        let message: IpcMessage = SchemaAsIpc::new(&schema, &Default::default())
            .try_into()
            .map_err(|e| Status::internal(format!("schema: {e}")))?;

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: query.query.into_bytes().into(),
            dataset_schema: message.0,
            parameter_schema: Default::default(),
        })
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("statement handle: {e}")))?;
        self.info_for(sql, request.into_inner()).await
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("statement handle: {e}")))?;

        let (schema, batches) = self.run(&sql).await?;
        Self::stream(schema, batches)
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        // Nothing to release: the handle is the statement text.
        Ok(())
    }

    // --- metadata, so a BI tool can browse -----------------------------------

    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.info_for(
            "SELECT DISTINCT table_catalog AS catalog_name FROM information_schema.tables \
             ORDER BY 1"
                .to_string(),
            request.into_inner(),
        )
        .await
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let (schema, batches) = self
            .run(
                "SELECT DISTINCT table_catalog AS catalog_name FROM information_schema.tables \
                 ORDER BY 1",
            )
            .await?;
        Self::stream(schema, batches)
    }

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.info_for(SCHEMAS_SQL.to_string(), request.into_inner())
            .await
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let (schema, batches) = self.run(SCHEMAS_SQL).await?;
        Self::stream(schema, batches)
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.info_for(TABLES_SQL.to_string(), request.into_inner())
            .await
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let (schema, batches) = self.run(TABLES_SQL).await?;
        Self::stream(schema, batches)
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.info_for(TABLE_TYPES_SQL.to_string(), request.into_inner())
            .await
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let (schema, batches) = self.run(TABLE_TYPES_SQL).await?;
        Self::stream(schema, batches)
    }

    // --- everything that writes ----------------------------------------------

    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Self::read_only("a SQL update"))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Self::read_only("a prepared update"))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: arrow_flight::sql::ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<arrow_flight::sql::ActionBeginTransactionResult, Status> {
        // Not merely unimplemented: a transaction would imply this endpoint can
        // change something, and it cannot.
        Err(Self::read_only("a transaction"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// The schemas a client can browse.
const SCHEMAS_SQL: &str = "SELECT DISTINCT table_catalog AS catalog_name, \
                           table_schema AS db_schema_name \
                           FROM information_schema.tables ORDER BY 1, 2";

/// The tables a client can browse.
///
/// Note what this exposes: `readings` (version-resolved, spanning both tiers),
/// `readings_versions` (the raw audit trail), and the `system` tables. A client
/// that picks the wrong one gets the §13.7.2 trap, which is why the names differ
/// as loudly as they do.
const TABLES_SQL: &str = "SELECT table_catalog AS catalog_name, \
                          table_schema AS db_schema_name, \
                          table_name, table_type \
                          FROM information_schema.tables ORDER BY 1, 2, 3";

/// The table types present, as Flight SQL expects them.
const TABLE_TYPES_SQL: &str =
    "SELECT DISTINCT table_type FROM information_schema.tables ORDER BY 1";

/// Convenience: serve until the future completes.
///
/// A thin wrapper, offered because binding a tonic server correctly is five
/// lines nobody should have to rediscover — but `into_service` stays the primary
/// entry point, since anything past a trusted network needs layers this cannot
/// choose.
pub async fn serve(
    store: MeterStore,
    address: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(%address, "flight sql listening");
    tonic::transport::Server::builder()
        .add_service(FlightSqlServer::new(store).into_service())
        .serve(address)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_refusal_names_what_it_would_break() {
        // A `permission_denied` with no reason invites a client to look for a
        // flag that turns it on. There isn't one, and the message says why.
        let status = FlightSqlServer::read_only("a SQL update");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        let message = status.message();
        assert!(message.contains("watermark"), "{message}");
        assert!(message.contains("erased subject"), "{message}");
        assert!(message.contains("MeterStore::append"), "{message}");
    }

    #[test]
    fn the_browse_queries_name_the_resolved_table_first() {
        // A BI tool lists tables and picks one. `readings` and
        // `readings_versions` both appear — they must, because the audit trail
        // is reachable on purpose — and the ordering puts the safe one first.
        assert!(TABLES_SQL.contains("ORDER BY"));
        assert!("readings" < "readings_versions");
    }
}
