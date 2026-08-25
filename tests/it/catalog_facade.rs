//! The read-only Iceberg REST façade, exercised as HTTP.
//!
//! §13.7.1's whole claim is that a SQL-catalog deployment can point Spark,
//! Trino, DuckDB or PyIceberg at this endpoint and have them read the history
//! straight from object storage. Two properties carry that claim, and both are
//! about the *wire* rather than about the handlers:
//!
//! * **The read routes exist and answer with real metadata.** Every Iceberg
//!   client opens with `GET /v1/config` and then walks namespaces to a table; a
//!   route that 404s stops it at the handshake.
//! * **A mutating verb is refused with the reason, in the envelope the spec
//!   defines.** An external writer would place rows in the cold tier without
//!   MeterStore knowing, which is the one thing §6.3 cannot survive. A bodiless
//!   `405` would be refused too — and would read to a client as a broken
//!   endpoint rather than as a read-only one, which is precisely the confusion
//!   `ApiError` exists to prevent.
//!
//! Driven through the router with `tower`'s `oneshot` rather than a bound port:
//! the thing under test is the route table and the response body, and a socket
//! adds nothing to either.

#![cfg(all(feature = "testkit", feature = "catalog-facade"))]

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use meterstore::serve::CatalogFacade;
use meterstore::testkit::TestHarness;
use tower::ServiceExt;

/// The namespace the harness creates its table in.
const NAMESPACE: &str = "metering";

async fn facade(harness: &TestHarness) -> Router {
    CatalogFacade::new(harness.cold().catalog()).router()
}

async fn call(app: &Router, method: Method, path: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let json = match bytes.is_empty() {
        true => serde_json::Value::Null,
        false => serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    };
    (status, json)
}

#[tokio::test]
async fn a_client_can_walk_the_handshake_to_a_table() {
    // The sequence every Iceberg client performs before it reads a byte of data.
    let harness = TestHarness::start().await.expect("harness");
    let app = facade(&harness).await;

    let (status, config) = call(&app, Method::GET, "/v1/config").await;
    assert_eq!(status, StatusCode::OK);
    assert!(config.get("defaults").is_some(), "{config}");

    let (status, namespaces) = call(&app, Method::GET, "/v1/namespaces").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        namespaces["namespaces"]
            .as_array()
            .expect("array")
            .iter()
            .any(|n| n[0] == NAMESPACE),
        "{namespaces}"
    );

    let (status, tables) = call(
        &app,
        Method::GET,
        &format!("/v1/namespaces/{NAMESPACE}/tables"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        tables["identifiers"]
            .as_array()
            .expect("array")
            .iter()
            .any(|t| t["name"] == TestHarness::TABLE),
        "{tables}"
    );

    let (status, table) = call(
        &app,
        Method::GET,
        &format!("/v1/namespaces/{NAMESPACE}/tables/{}", TestHarness::TABLE),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // The response has to carry enough for a client to plan its own scan, and to
    // carry **no** credentials of its own — that is what keeps this endpoint out
    // of the data path.
    assert!(table["metadata-location"].is_string(), "{table}");
    assert!(table["metadata"]["schemas"].is_array(), "{table}");
    assert_eq!(table["config"], serde_json::json!({}), "{table}");
}

#[tokio::test]
async fn every_served_route_refuses_a_mutating_verb_with_the_reason() {
    // An Iceberg client parses the spec's error envelope, so axum's bare 405 with
    // an empty body reads to it as a broken endpoint rather than a read-only one
    // — and `createNamespace`, `createTable` and `dropNamespace` are the three a
    // client is most likely to attempt.
    let harness = TestHarness::start().await.expect("harness");
    let app = facade(&harness).await;
    let table = format!("/v1/namespaces/{NAMESPACE}/tables/{}", TestHarness::TABLE);

    for (method, path) in [
        (Method::POST, "/v1/namespaces".to_string()),
        (Method::DELETE, format!("/v1/namespaces/{NAMESPACE}")),
        (Method::POST, format!("/v1/namespaces/{NAMESPACE}/tables")),
        (Method::POST, table.clone()),
        (Method::DELETE, table),
    ] {
        let (status, body) = call(&app, method.clone(), &path).await;
        assert_eq!(
            status,
            StatusCode::METHOD_NOT_ALLOWED,
            // 405 rather than 501: a client must not read this as "not yet".
            "{method} {path} answered {status}"
        );
        assert_eq!(body["error"]["code"], 405, "{method} {path}: {body}");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("watermark")),
            "{method} {path} must say why, not just no: {body}"
        );
    }
}

#[tokio::test]
async fn a_head_request_is_a_read_and_still_answered() {
    // `namespaceExists` and `tableExists` are HEAD requests, and PyIceberg makes
    // both. They are reads, so the method fallback must not swallow them.
    let harness = TestHarness::start().await.expect("harness");
    let app = facade(&harness).await;

    for path in [
        format!("/v1/namespaces/{NAMESPACE}"),
        format!("/v1/namespaces/{NAMESPACE}/tables/{}", TestHarness::TABLE),
    ] {
        let (status, _) = call(&app, Method::HEAD, &path).await;
        assert_eq!(status, StatusCode::OK, "HEAD {path}");
    }
}

#[tokio::test]
async fn an_unknown_table_is_not_found_rather_than_a_server_error() {
    // The spec distinguishes them and a client retries only one.
    let harness = TestHarness::start().await.expect("harness");
    let app = facade(&harness).await;

    let (status, body) = call(
        &app,
        Method::GET,
        &format!("/v1/namespaces/{NAMESPACE}/tables/no_such_table"),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "NoSuchTableException", "{body}");
}

#[tokio::test]
async fn a_route_outside_the_served_subset_says_so() {
    let harness = TestHarness::start().await.expect("harness");
    let app = facade(&harness).await;

    let (status, body) = call(&app, Method::GET, "/v1/oauth/tokens").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("read-only subset")),
        "{body}"
    );
}
