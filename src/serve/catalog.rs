//! A read-only Iceberg REST Catalog endpoint.
//!
//! Every Iceberg engine speaks the [REST catalog protocol], so exposing one is
//! how Spark, Trino, DuckDB and PyIceberg read the history **directly from
//! object storage** with nothing MeterStore-specific installed client-side.
//! MeterStore stays in the metadata path only — never the data path — so readers
//! scale in parallel and this process is neither a bottleneck nor a single point
//! of failure (P2, §13.7).
//!
//! [REST catalog protocol]: https://iceberg.apache.org/rest-catalog-spec/
//!
//! # When this is needed, and when it is not
//!
//! | Catalog in use | What external engines need |
//! |---|---|
//! | **REST** (Polaris, Lakekeeper, Nessie, Gravitino) | Nothing. Point them at the same endpoint. |
//! | **SQL** (PostgreSQL-backed) | This. The JDBC catalog exists but support is uneven — Trino and Spark manage, DuckDB and PyIceberg less so. |
//!
//! So this is a bridge for SQL-catalog deployments, not a component every
//! deployment runs. A REST-catalog deployment that started this would be adding
//! a hop for no reason.
//!
//! # Read-only, and not as a default that can be flipped
//!
//! There is no write path here at all, and that is a correctness position rather
//! than an unfinished feature. The §6.3 invariant says PostgreSQL holds exactly
//! the rows at or above the watermark and Iceberg exactly those below. An
//! external writer appending through this endpoint would place rows in the cold
//! tier without MeterStore knowing, and nothing downstream could detect it — the
//! files would be valid Iceberg, the invariant check only looks at PostgreSQL,
//! and the first symptom would be a number that does not reconcile.
//!
//! Mutating routes therefore answer `405` with that reason, rather than `501`.
//! The distinction matters: `501` invites a client to retry against a future
//! version.
//!
//! # It carries no credentials of its own
//!
//! The response tells a client where the data is; it does not tell it how to
//! authenticate to object storage. Engines use their own credentials, which is
//! what keeps this endpoint out of the data path and means compromising it does
//! not hand over the warehouse.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use serde::Serialize;
use tracing::{debug, info};

/// Serves table metadata for one catalog.
#[derive(Clone)]
pub struct CatalogFacade {
    catalog: Arc<dyn Catalog>,
}

impl std::fmt::Debug for CatalogFacade {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogFacade").finish_non_exhaustive()
    }
}

impl CatalogFacade {
    /// Serve the given catalog.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// The router, ready to be served or nested under a larger application.
    ///
    /// Returned rather than bound to a port, so a deployment can put its own
    /// authentication, TLS termination and tracing layers around it. §19.7 is
    /// explicit that this needs authentication before it leaves a trusted
    /// network, and a router is the shape that lets a caller add it.
    pub fn router(self) -> Router {
        Router::new()
            .route("/v1/config", get(config))
            .route("/v1/namespaces", get(list_namespaces))
            .route("/v1/namespaces/{namespace}", get(load_namespace))
            .route("/v1/namespaces/{namespace}/tables", get(list_tables))
            .route(
                "/v1/namespaces/{namespace}/tables/{table}",
                get(load_table).fallback(any(read_only)),
            )
            // Anything else that mutates, including routes a future spec version
            // adds: refused by shape rather than by enumeration.
            .fallback(any(not_found))
            .with_state(self)
    }
}

/// The separator the REST spec uses for multi-level namespaces in a URL path.
const NAMESPACE_SEPARATOR: char = '\u{1F}';

/// Parse a namespace from its URL-encoded form.
fn namespace_of(raw: &str) -> NamespaceIdent {
    let parts: Vec<String> = raw
        .split(NAMESPACE_SEPARATOR)
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .collect();
    NamespaceIdent::from_vec(parts.clone()).unwrap_or_else(|_| NamespaceIdent::new(raw.to_string()))
}

/// `GET /v1/config` — the handshake every Iceberg client makes first.
async fn config() -> Json<ConfigResponse> {
    Json(ConfigResponse {
        defaults: HashMap::new(),
        overrides: HashMap::new(),
    })
}

#[derive(Serialize)]
struct ConfigResponse {
    defaults: HashMap<String, String>,
    overrides: HashMap<String, String>,
}

/// `GET /v1/namespaces`
async fn list_namespaces(State(facade): State<CatalogFacade>) -> Result<Response, ApiError> {
    let namespaces = facade
        .catalog
        .list_namespaces(None)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(NamespacesResponse {
        namespaces: namespaces.iter().map(|n| n.as_ref().to_vec()).collect(),
    })
    .into_response())
}

#[derive(Serialize)]
struct NamespacesResponse {
    namespaces: Vec<Vec<String>>,
}

/// `GET /v1/namespaces/{namespace}`
async fn load_namespace(
    State(facade): State<CatalogFacade>,
    Path(namespace): Path<String>,
) -> Result<Response, ApiError> {
    let ident = namespace_of(&namespace);
    let found = facade
        .catalog
        .get_namespace(&ident)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(NamespaceResponse {
        namespace: ident.as_ref().to_vec(),
        properties: found.properties().clone(),
    })
    .into_response())
}

#[derive(Serialize)]
struct NamespaceResponse {
    namespace: Vec<String>,
    properties: HashMap<String, String>,
}

/// `GET /v1/namespaces/{namespace}/tables`
async fn list_tables(
    State(facade): State<CatalogFacade>,
    Path(namespace): Path<String>,
) -> Result<Response, ApiError> {
    let ident = namespace_of(&namespace);
    let tables = facade
        .catalog
        .list_tables(&ident)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(TablesResponse {
        identifiers: tables
            .into_iter()
            .map(|t| TableIdentifier {
                namespace: t.namespace().as_ref().to_vec(),
                name: t.name().to_string(),
            })
            .collect(),
    })
    .into_response())
}

#[derive(Serialize)]
struct TablesResponse {
    identifiers: Vec<TableIdentifier>,
}

#[derive(Serialize)]
struct TableIdentifier {
    namespace: Vec<String>,
    name: String,
}

/// `GET /v1/namespaces/{namespace}/tables/{table}`
///
/// The route that matters. The response carries the table metadata a client
/// needs to plan its own scan — schema, partition spec, snapshots, manifest
/// locations — after which it reads object storage directly and this process is
/// out of the picture.
async fn load_table(
    State(facade): State<CatalogFacade>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let ident = TableIdent::new(namespace_of(&namespace), table.clone());
    let loaded = facade
        .catalog
        .load_table(&ident)
        .await
        .map_err(ApiError::from)?;

    debug!(%table, "served table metadata");

    Ok(Json(LoadTableResponse {
        metadata_location: loaded.metadata_location().map(str::to_string),
        metadata: loaded.metadata().clone(),
        // Deliberately empty: object-store credentials are the client's, which
        // is what keeps this endpoint out of the data path (§13.7).
        config: HashMap::new(),
    })
    .into_response())
}

#[derive(Serialize)]
struct LoadTableResponse {
    #[serde(rename = "metadata-location", skip_serializing_if = "Option::is_none")]
    metadata_location: Option<String>,
    metadata: iceberg::spec::TableMetadata,
    config: HashMap<String, String>,
}

/// Any mutating request against a table route.
async fn read_only() -> ApiError {
    ApiError {
        status: StatusCode::METHOD_NOT_ALLOWED,
        kind: "MethodNotAllowedException",
        message: "this catalog is read-only: an external writer would place rows in the cold \
                  tier without MeterStore knowing, breaking the invariant that PostgreSQL holds \
                  exactly the rows at or above the tiering watermark. Write through MeterStore."
            .to_string(),
    }
}

/// Anything the façade does not implement.
async fn not_found() -> ApiError {
    ApiError {
        status: StatusCode::NOT_FOUND,
        kind: "NotFoundException",
        message: "this endpoint serves the read-only subset of the Iceberg REST catalog spec: \
                  config, namespaces, and table metadata"
            .to_string(),
    }
}

/// An error in the shape the REST catalog spec defines.
///
/// Clients parse this; a bare status code with an HTML body would make a
/// misconfigured endpoint look like a network fault.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    kind: &'static str,
    message: String,
}

impl From<iceberg::Error> for ApiError {
    fn from(error: iceberg::Error) -> Self {
        // The spec distinguishes "no such table" from "something broke", and a
        // client retries only one of them.
        let status = match error.kind() {
            iceberg::ErrorKind::TableNotFound | iceberg::ErrorKind::NamespaceNotFound => {
                StatusCode::NOT_FOUND
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let kind = if status == StatusCode::NOT_FOUND {
            "NoSuchTableException"
        } else {
            "InternalServerError"
        };
        Self {
            status,
            kind,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Body {
            error: Inner,
        }
        #[derive(Serialize)]
        struct Inner {
            message: String,
            r#type: String,
            code: u16,
        }

        info!(status = %self.status, kind = self.kind, "catalog facade refused a request");
        (
            self.status,
            Json(Body {
                error: Inner {
                    message: self.message,
                    r#type: self.kind.to_string(),
                    code: self.status.as_u16(),
                },
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_single_level_namespace_parses() {
        assert_eq!(namespace_of("metering").as_ref(), &["metering".to_string()]);
    }

    #[test]
    fn a_multi_level_namespace_splits_on_the_unit_separator() {
        // The REST spec encodes namespace levels with 0x1F rather than a dot,
        // because a dot is legal inside a level.
        let ident = namespace_of("a\u{1F}b\u{1F}c");
        assert_eq!(
            ident.as_ref(),
            &["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn a_namespace_containing_a_dot_stays_one_level() {
        assert_eq!(
            namespace_of("edm.metering").as_ref(),
            &["edm.metering".to_string()]
        );
    }

    #[tokio::test]
    async fn a_mutating_request_is_refused_with_the_reason() {
        // 405 rather than 501: a client must not read this as "not yet".
        let error = read_only().await;
        assert_eq!(error.status, StatusCode::METHOD_NOT_ALLOWED);
        assert!(error.message.contains("watermark"), "{}", error.message);
    }

    #[tokio::test]
    async fn the_config_handshake_is_empty_rather_than_absent() {
        // Every Iceberg client calls this first and fails if it 404s, even
        // though there is nothing to override.
        let Json(config) = config().await;
        assert!(config.defaults.is_empty());
        assert!(config.overrides.is_empty());
    }
}
