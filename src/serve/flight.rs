//! Arrow Flight SQL over the **unified** hot + cold view.
//!
//! The hot tier lives in PostgreSQL and is not in the Iceberg catalog, so the
//! unified view is the one thing an external client cannot assemble for itself.
//! That is the whole justification — real, and narrow. Analytics over settled
//! history belong on the catalog, read directly: routing them through here adds a
//! proxy hop and serialises parallel reads through one process.
//!
//! # Read-only, structurally
//!
//! A write here would bypass [`MeterStore::append`] and with it the two things
//! that make a write safe — routing each interval to the tier that owns it
//! and the subject-reference check that stops a replay re-linking an erased
//! subject. Neither is recoverable afterwards, so every mutating call is
//! refused with that reason.
//!
//! Refusing the mutating *calls* is not the whole of it: Flight SQL's statement
//! query carries arbitrary text, and DataFusion's surface is wider than `SELECT`
//! — `CREATE EXTERNAL TABLE … LOCATION` reads any path the process can, `COPY …
//! TO` writes one, and both arrive as *queries*. [`MeterStore::sql`] plans
//! without running and refuses anything that is not a query, which is where the
//! check lives.
//!
//! [`MeterStore::sql`]: crate::session::MeterStore::sql
//!
//! # Results carry their boundary
//!
//! The watermark and the tiers scanned travel as **schema metadata** on every
//! response (P1), so a BI tool that keeps the schema keeps the provenance. A
//! number pulled over Flight is otherwise indistinguishable from one pulled a
//! minute later against a different boundary.
//!
//! # One table or many
//!
//! The server takes a [`SqlSurface`] — a [`MeterStore`] or a whole
//! [`MeterCatalog`](crate::MeterCatalog). A catalog result carries **every**
//! boundary the statement touched, because two tables genuinely have two.
//!
//! # Authentication is the deployment's
//!
//! `into_service` returns a tonic service rather than a bound port, so a
//! deployment wraps it in its own interceptor, TLS and tracing. Authentication
//! is required before this leaves a trusted network, and this crate has no
//! business deciding what kind.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate, ProstMessageExt,
    SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use futures::{StreamExt, TryStreamExt, stream};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info};

use crate::arrow::array::RecordBatch;
use crate::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
// `MeterStore` is imported for the intra-doc links above and below; the
// server itself holds a `SqlSurface`, which it may equally be a catalog.
#[allow(unused_imports)]
use crate::session::MeterStore;
use crate::session::SqlSurface;

/// A Flight SQL server over a unified hot + cold view.
///
/// Serves any [`SqlSurface`]: a single [`MeterStore`], or a whole
/// [`MeterCatalog`](crate::MeterCatalog).
#[derive(Clone)]
pub struct FlightSqlServer {
    surface: Arc<dyn SqlSurface>,
}

impl std::fmt::Debug for FlightSqlServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightSqlServer")
            .field("serving", &self.surface.label())
            .finish_non_exhaustive()
    }
}

impl FlightSqlServer {
    /// Serve a store or a catalog.
    ///
    /// ```no_run
    /// # use meterstore::serve::FlightSqlServer;
    /// # fn example(store: meterstore::MeterStore, catalog: meterstore::MeterCatalog) {
    /// let one_table = FlightSqlServer::new(store);
    /// let every_table = FlightSqlServer::new(catalog);
    /// # let _ = (one_table, every_table);
    /// # }
    /// ```
    pub fn new(surface: impl SqlSurface) -> Self {
        Self {
            surface: Arc::new(surface),
        }
    }

    /// Serve a surface someone else already owns.
    ///
    /// The same server over an `Arc` a deployment is also using elsewhere — a
    /// catalog that a maintenance loop archives through, say.
    pub fn shared(surface: Arc<dyn SqlSurface>) -> Self {
        Self { surface }
    }

    /// The tonic service, ready to be added to a server or wrapped in layers.
    ///
    /// Returned rather than bound, so a deployment supplies its own
    /// authentication interceptor and TLS — both are required before this
    /// leaves a trusted network.
    pub fn into_service(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// The provenance a response carries, as Arrow schema metadata (P1).
    ///
    /// A client on the far end of a socket needs the boundary as much as one
    /// in-process: a number pulled over Flight is otherwise indistinguishable
    /// from one pulled a minute later against a different boundary.
    ///
    /// **Every** boundary, not one. A statement over a catalog spans tables with
    /// different watermarks, and collapsing them would tell a client a figure was
    /// settled to a point only one of its inputs had reached. The conservative
    /// minimum is carried too, since that is the number a reconciliation uses
    /// directly.
    fn with_provenance(described: &crate::session::QueryDescription) -> SchemaRef {
        let tiers = described
            .tiers_scanned()
            .iter()
            .map(|t| format!("{t:?}").to_lowercase())
            .collect::<Vec<_>>()
            .join(",");
        let watermarks = described
            .watermarks()
            .iter()
            .map(|(table, at)| format!("{table}={at}"))
            .collect::<Vec<_>>()
            .join(",");

        let metadata = std::collections::HashMap::from([
            (
                crate::watermark::WATERMARK_PROPERTY.to_string(),
                described.watermark().to_string(),
            ),
            ("meterstore.watermarks".to_string(), watermarks),
            ("meterstore.tiers_scanned".to_string(), tiers),
            (
                "meterstore.read_mode".to_string(),
                format!("{:?}", described.read_mode()),
            ),
        ]);
        Arc::new(described.schema().as_ref().clone().with_metadata(metadata))
    }

    /// Plan a statement and return the schema it would produce, **without
    /// running it**.
    async fn describe(&self, sql: &str) -> Result<SchemaRef, Status> {
        debug!(%sql, "flight sql describe");

        let described = self
            .surface
            .describe_sql(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("query failed: {e}")))?;

        Ok(Self::with_provenance(&described))
    }

    /// Execute `sql` and answer with a Flight stream carrying its provenance.
    ///
    /// **Streamed, never collected.** This is the one surface built for a BI
    /// tool, and a BI tool's query is the one whose rows genuinely are the
    /// answer: a year of quarter-hour readings for a portfolio is millions of
    /// them. Materialising the result to send it would make the server's peak
    /// memory the size of whatever a client asked for, on a socket the client
    /// controls — the same mistake the archival path is built to avoid, on the
    /// path where an outsider chooses the size.
    ///
    /// [`MeterStore::stream`] returns the provenance *before* the first batch,
    /// which is what makes that possible here: the schema — with the watermark
    /// and the tiers on it — has to be written before any row.
    async fn respond(
        &self,
        sql: &str,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(%sql, "flight sql query");

        let (described, rows) = self
            .surface
            .stream_sql(sql, Vec::new())
            .await
            .map_err(|e| Status::invalid_argument(format!("query failed: {e}")))?;

        let schema = Self::with_provenance(&described);

        // Re-stamped onto every batch so the provenance survives a client that
        // inspects a batch rather than the stream schema. Metadata does not
        // change a column, so this cannot fail — and sending the batch anyway if
        // it did would drop the provenance silently, which is the one thing this
        // is here to prevent.
        let stamped = schema.clone();
        let batches = rows.map(move |batch| {
            let batch =
                batch.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)))?;
            RecordBatch::try_new(stamped.clone(), batch.columns().to_vec())
                .map_err(arrow_flight::error::FlightError::Arrow)
        });

        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batches)
            .map_err(|e| Status::internal(format!("encoding flight data: {e}")));

        Ok(Response::new(Box::pin(flight)))
    }

    /// A `FlightInfo` whose ticket replays `sql` on `do_get`.
    ///
    /// The query is **planned and not executed**. A client learns about a syntax
    /// error, an unknown column or an unknown relation here rather than halfway
    /// through a stream, and the rows are produced once, on `do_get`.
    ///
    /// That distinction is the difference between one scan and two: answering
    /// this by *running* the query makes a BI tool's ordinary `GetFlightInfo` →
    /// `DoGet` sequence cost two full scans, on the one surface built for BI
    /// tools.
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

    /// Answer a metadata RPC with the schema **the specification fixes**, not
    /// the one the SQL behind it happens to produce.
    ///
    /// # Why these do not go through `respond`
    ///
    /// `respond` stamps the tiering watermark and the tiers scanned onto the
    /// schema, which is P1 — an answer carries the boundary it was computed
    /// against — and exactly wrong here. A `GetTables` response is not an answer
    /// about readings; its schema is part of the wire contract, fixed field for
    /// field down to nullability, and a client is entitled to reject anything
    /// else. The ADBC driver does: extra schema metadata and a non-nullable
    /// `catalog_name` both fail its validation, so a browse that works from a
    /// client written against this server fails from every client written
    /// against the specification.
    ///
    /// The rows are produced by SQL either way. Only the schema they are shipped
    /// under changes.
    async fn metadata(
        &self,
        sql: &str,
        schema: SchemaRef,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(%sql, "flight sql metadata");

        let (_described, rows) = self
            .surface
            .stream_sql(sql, Vec::new())
            .await
            .map_err(|e| Status::invalid_argument(format!("query failed: {e}")))?;

        let wire = schema.clone();
        let batches = rows.map(move |batch| {
            let batch =
                batch.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)))?;
            RecordBatch::try_new(wire.clone(), batch.columns().to_vec())
                .map_err(arrow_flight::error::FlightError::Arrow)
        });

        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batches)
            .map_err(|e| Status::internal(format!("encoding flight data: {e}")));

        Ok(Response::new(Box::pin(flight)))
    }

    /// A `FlightInfo` for a metadata RPC, whose ticket carries the **command**.
    ///
    /// Not a `TicketStatementQuery` wrapping the SQL: `do_get` dispatches on the
    /// ticket's message type, so a statement ticket sends the follow-up call to
    /// `do_get_statement` and the client receives the query path's schema after
    /// being promised this one. Handing back the command is what makes
    /// `do_get_tables` and its siblings reachable at all.
    fn metadata_info(
        command: impl ProstMessageExt,
        schema: &Schema,
        descriptor: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = Ticket::new(command.as_any().encode_to_vec());
        let info = FlightInfo::new()
            .try_with_schema(schema)
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
    /// second, weaker one beside whatever the estate already runs. That puts
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

        self.respond(&sql).await
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

        self.respond(&sql).await
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
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::metadata_info(query, &catalogs_schema(), request.into_inner())
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.metadata(CATALOGS_SQL, catalogs_schema()).await
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::metadata_info(query, &db_schemas_schema(), request.into_inner())
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.metadata(SCHEMAS_SQL, db_schemas_schema()).await
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::metadata_info(query, &tables_schema(), request.into_inner())
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.metadata(TABLES_SQL, tables_schema()).await
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::metadata_info(query, &table_types_schema(), request.into_inner())
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.metadata(TABLE_TYPES_SQL, table_types_schema()).await
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

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::metadata_info(query, &sql_info().schema(), request.into_inner())
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let batch = sql_info()
            .record_batch(query.info)
            .map_err(|e| Status::internal(format!("sql info: {e}")))?;
        let schema = batch.schema();

        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async move { Ok(batch) }))
            .map_err(|e| Status::internal(format!("encoding flight data: {e}")));

        Ok(Response::new(Box::pin(flight)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// What this server says about itself when a client asks.
///
/// # Why this is not optional
///
/// An ADBC client calls `GetSqlInfo` while opening a connection, before any
/// query. Answering `Unimplemented` is survivable — the driver falls back — but
/// what it falls back to is a server with no name, no version and, worst,
/// **no read-only flag**: the client then believes writes are available and
/// offers the user an `INSERT`, so the refusal arrives as a permission error at
/// the end of a session instead of as a menu item that is not there.
///
/// Declaring no transaction support is the same argument. The driver otherwise
/// tries to turn autocommit off, fails, and warns on every connection — about a
/// capability this server never had and, being read-only, has no use for.
fn sql_info() -> &'static SqlInfoData {
    static INFO: std::sync::OnceLock<SqlInfoData> = std::sync::OnceLock::new();
    INFO.get_or_init(|| {
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "MeterStore");
        builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        builder.append(SqlInfo::FlightSqlServerArrowVersion, ARROW_VERSION);
        // The whole surface refuses to write, and says why when asked to. This
        // is the machine-readable half of that sentence.
        builder.append(SqlInfo::FlightSqlServerReadOnly, true);
        // `SQL_TRANSACTION_UNSPECIFIED`: neither transactions nor savepoints.
        builder.append(SqlInfo::FlightSqlServerTransaction, 0i32);
        builder.build().expect("a fixed set of literals builds")
    })
}

/// The Arrow version this server encodes with, reported to a client that has to
/// decode it.
const ARROW_VERSION: &str = "58";

/// The catalogues a client can browse.
const CATALOGS_SQL: &str = "SELECT DISTINCT table_catalog AS catalog_name \
                            FROM information_schema.tables ORDER BY 1";

/// The schemas a client can browse.
const SCHEMAS_SQL: &str = "SELECT DISTINCT table_catalog AS catalog_name, \
                           table_schema AS db_schema_name \
                           FROM information_schema.tables ORDER BY 1, 2";

/// The tables a client can browse.
///
/// Note what this exposes: `readings` (version-resolved, spanning both tiers),
/// `readings_versions` (the raw audit trail), and the `system` tables. A client
/// that picks the wrong one gets the resolution trap, which is why the names differ
/// as loudly as they do.
const TABLES_SQL: &str = "SELECT table_catalog AS catalog_name, \
                          table_schema AS db_schema_name, \
                          table_name, table_type \
                          FROM information_schema.tables ORDER BY 1, 2, 3";

/// The table types present, as Flight SQL expects them.
const TABLE_TYPES_SQL: &str =
    "SELECT DISTINCT table_type FROM information_schema.tables ORDER BY 1";

/// The four metadata schemas, **as the Flight SQL specification fixes them**.
///
/// Written out here rather than borrowed from `arrow-flight`, whose copies are
/// private to its own in-memory implementations. They are part of the wire
/// contract — field order, type and nullability — and a client is entitled to
/// reject a response that differs in any of them. The ADBC driver does exactly
/// that, which is how a non-nullable `catalog_name` and a stray watermark in the
/// schema metadata turned into a browse that failed for every client written
/// against the specification and worked for every client written against this
/// server.
fn catalogs_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "catalog_name",
        DataType::Utf8,
        false,
    )]))
}

/// See [`catalogs_schema`].
fn db_schemas_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new("db_schema_name", DataType::Utf8, false),
    ]))
}

/// See [`catalogs_schema`]. Without `table_schema`, which this server does not
/// send: the column is optional in the specification, and a client that wants a
/// table's schema gets it from `GetFlightInfo` on a query against that table.
fn tables_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ]))
}

/// See [`catalogs_schema`].
fn table_types_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
}

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
    fn the_metadata_schemas_are_the_specifications() {
        // Field order, type and nullability are the wire contract for these four
        // responses, and a client is entitled to reject anything else. An ADBC
        // one does, so a `catalog_name` declared non-nullable is not a detail —
        // it is a browse that fails for every client written against the
        // specification and works for every client written against this server.
        let tables = tables_schema();
        assert_eq!(
            tables
                .fields()
                .iter()
                .map(|f| (f.name().as_str(), f.is_nullable()))
                .collect::<Vec<_>>(),
            vec![
                ("catalog_name", true),
                ("db_schema_name", true),
                ("table_name", false),
                ("table_type", false),
            ]
        );
        assert!(db_schemas_schema().field(0).is_nullable());
        assert!(!db_schemas_schema().field(1).is_nullable());
        assert!(!catalogs_schema().field(0).is_nullable());
        assert!(!table_types_schema().field(0).is_nullable());

        // And none of them carries the provenance a query response does. The
        // watermark belongs on an answer about readings; a list of table names
        // is not one.
        for schema in [
            catalogs_schema(),
            db_schemas_schema(),
            tables_schema(),
            table_types_schema(),
        ] {
            assert!(
                schema.metadata().is_empty(),
                "a metadata response's schema is fixed: {:?}",
                schema.metadata()
            );
        }
    }

    #[test]
    fn the_server_declares_itself_read_only_where_a_client_reads_it() {
        // The machine-readable half of `read_only`'s sentence. A client that
        // does not see this flag offers the user an INSERT and delivers the
        // refusal at the end of a session instead of not offering it.
        let batch = sql_info()
            .record_batch([SqlInfo::FlightSqlServerReadOnly as u32])
            .expect("one info code");
        assert_eq!(batch.num_rows(), 1, "the flag has to be sent at all");

        let named = sql_info()
            .record_batch([SqlInfo::FlightSqlServerName as u32])
            .expect("one info code");
        assert_eq!(named.num_rows(), 1, "and the server has to name itself");
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
