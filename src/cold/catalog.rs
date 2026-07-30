//! Constructing the cold tier: an Iceberg `SqlCatalog` over PostgreSQL, backed by
//! an object store chosen from the warehouse URI.
//!
//! [`IcebergCold`] deliberately takes a pre-built
//! `Arc<dyn Catalog>`, so a deployment can bring a REST catalog, Glue, or anything
//! else. But the *common* deployment — a `SqlCatalog` on the same PostgreSQL that
//! backs the hot tier, over an object-store warehouse — is the same wiring every
//! time: pick the OpenDAL backend from the URI scheme, forward object-store
//! credentials into the catalog properties, and bound the catalog's metadata pool.
//!
//! That wiring lives here rather than in each application, because it is
//! infrastructure this crate already owns the dependencies for. An application
//! that used to reach for `iceberg-catalog-sql` and `iceberg-storage-opendal`
//! directly needs neither once it calls [`IcebergSqlCatalog::build`].
//!
//! # Object-store backends are features
//!
//! `file://` and `memory://` are always available. The cloud backends are behind
//! features so a file-only deployment does not compile the AWS/GCS/Azure SDKs:
//! `object-store-s3`, `object-store-gcs`, `object-store-azure` (or `object-store-all`).
//! A warehouse whose scheme needs a backend that was not compiled in is a clear
//! error at construction, not a silent fallback.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};

use crate::error::{Error, Result};

use super::IcebergCold;

/// Object-store credentials for an S3-family warehouse.
///
/// Consulted only for S3 and S3-compatible schemes (`s3://`, `minio://`, `r2://`);
/// `file://` / `memory://` / `gs://` / `abfss://` ignore it. Fields left `None`
/// fall back to the platform's standard credential chain — environment variables,
/// an EC2 / IRSA instance role — which is the recommended production path (no
/// secrets in config). Explicit keys exist for S3-compatible stores without an
/// instance role (MinIO, Ceph, R2, LocalStack). GCS and Azure authenticate purely
/// through their platform chains (ADC / managed identity), so they take no fields
/// here.
#[derive(Debug, Clone, Default)]
pub struct WarehouseAuth {
    /// S3 region, or `client.region`.
    pub region: Option<String>,
    /// S3-compatible endpoint override; its presence switches on path-style addressing.
    pub endpoint: Option<String>,
    /// S3 access key ID (omit to use the instance-role / env credential chain).
    pub access_key_id: Option<String>,
    /// S3 secret access key (omit to use the credential chain).
    pub secret_access_key: Option<String>,
}

/// The `://` scheme of a warehouse URI (`file` when there is none).
fn warehouse_scheme(warehouse_uri: &str) -> &str {
    warehouse_uri
        .split_once("://")
        .map_or("file", |(scheme, _)| scheme)
}

/// Whether a scheme is S3 or S3-compatible.
fn is_s3_scheme(scheme: &str) -> bool {
    matches!(scheme, "s3" | "s3a" | "s3n" | "minio" | "r2")
}

/// The OpenDAL storage backend for a warehouse URI, chosen by scheme.
///
/// A scheme whose backend feature was not compiled in is an error rather than a
/// silent fallback to the local filesystem, which would write a "cloud" warehouse
/// to disk and fail only later, obscurely.
fn warehouse_factory(warehouse_uri: &str) -> Result<Arc<dyn iceberg::io::StorageFactory>> {
    use iceberg_storage_opendal::OpenDalStorageFactory;
    let scheme = warehouse_scheme(warehouse_uri);
    Ok(match scheme {
        "file" => Arc::new(OpenDalStorageFactory::Fs),
        "memory" => Arc::new(OpenDalStorageFactory::Memory),
        "s3" | "s3a" | "s3n" | "minio" | "r2" => {
            #[cfg(feature = "object-store-s3")]
            {
                Arc::new(OpenDalStorageFactory::S3 {
                    customized_credential_load: None,
                })
            }
            #[cfg(not(feature = "object-store-s3"))]
            return Err(Error::Storage(format!(
                "warehouse scheme {scheme:?} needs the meterstore `object-store-s3` feature, which was not compiled in"
            )));
        }
        "gs" | "gcs" => {
            #[cfg(feature = "object-store-gcs")]
            {
                Arc::new(OpenDalStorageFactory::Gcs)
            }
            #[cfg(not(feature = "object-store-gcs"))]
            return Err(Error::Storage(format!(
                "warehouse scheme {scheme:?} needs the meterstore `object-store-gcs` feature, which was not compiled in"
            )));
        }
        "abfss" | "abfs" | "azdls" => {
            #[cfg(feature = "object-store-azure")]
            {
                Arc::new(OpenDalStorageFactory::Azdls)
            }
            #[cfg(not(feature = "object-store-azure"))]
            return Err(Error::Storage(format!(
                "warehouse scheme {scheme:?} needs the meterstore `object-store-azure` feature, which was not compiled in"
            )));
        }
        other => {
            return Err(Error::Storage(format!(
                "unsupported warehouse scheme {other:?}"
            )));
        }
    })
}

/// Inject S3-family credential/endpoint props (the `SqlCatalog` forwards catalog
/// props to the FileIO, so these reach the S3 operator). No-op for non-S3 schemes.
fn apply_warehouse_auth(
    props: &mut HashMap<String, String>,
    warehouse_uri: &str,
    auth: &WarehouseAuth,
) {
    use iceberg::io::{
        CLIENT_REGION, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
        S3_SECRET_ACCESS_KEY,
    };
    if !is_s3_scheme(warehouse_scheme(warehouse_uri)) {
        return;
    }
    if let Some(region) = &auth.region {
        props.insert(S3_REGION.into(), region.clone());
        props.insert(CLIENT_REGION.into(), region.clone());
    }
    if let Some(endpoint) = &auth.endpoint {
        props.insert(S3_ENDPOINT.into(), endpoint.clone());
        // A custom endpoint means an S3-compatible store (MinIO, Ceph, R2), which
        // addresses buckets path-style rather than virtual-hosted.
        props.insert(S3_PATH_STYLE_ACCESS.into(), "true".into());
    }
    if let Some(key) = &auth.access_key_id {
        props.insert(S3_ACCESS_KEY_ID.into(), key.clone());
    }
    if let Some(secret) = &auth.secret_access_key {
        props.insert(S3_SECRET_ACCESS_KEY.into(), secret.clone());
    }
}

/// Everything needed to build a `SqlCatalog`-backed [`IcebergCold`] cold tier.
///
/// The catalog's metadata lives in `database_url` (normally the same PostgreSQL as
/// the hot tier), and the table files in `warehouse_uri`'s object store. Pass it to
/// [`build`](Self::build).
#[derive(Debug, Clone)]
pub struct IcebergSqlCatalog<'a> {
    /// PostgreSQL URL for the catalog's own metadata (create/load table).
    pub database_url: &'a str,
    /// Object-store warehouse URI; its scheme selects the backend.
    pub warehouse_uri: &'a str,
    /// Catalog name (the `SqlCatalog` metadata namespace key).
    pub catalog_name: &'a str,
    /// Iceberg namespace the tables live in.
    pub namespace: &'a str,
    /// Target Parquet file size in the cold tier, in bytes.
    pub file_target_bytes: usize,
    /// Upper bound on the catalog's *metadata* connection pool.
    ///
    /// The `SqlCatalog` opens its own PostgreSQL pool for create/load-table only —
    /// low concurrency. Bound it small so several catalogs plus the application's
    /// own pool and the hot tier do not exhaust PostgreSQL's connection slots; the
    /// `SqlCatalog` default is 10.
    pub metadata_pool_max_connections: u32,
    /// Object-store credentials (consulted for S3-family schemes only).
    pub auth: &'a WarehouseAuth,
}

impl IcebergSqlCatalog<'_> {
    /// Build the cold tier: a `SqlCatalog` over PostgreSQL, an OpenDAL object-store
    /// backend chosen from the warehouse scheme, and an [`IcebergCold`] writing into
    /// the namespace.
    pub async fn build(&self) -> Result<ColdTier> {
        let storage = |e: iceberg::Error| Error::Storage(e.to_string());

        // Credential/endpoint props ride in the catalog props, which the SqlCatalog
        // forwards to the FileIO.
        let mut props = HashMap::from([
            (
                SQL_CATALOG_PROP_URI.to_string(),
                self.database_url.to_string(),
            ),
            (
                SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                self.warehouse_uri.to_string(),
            ),
            (
                SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                SqlBindStyle::DollarNumeric.to_string(),
            ),
            (
                "pool.max-connections".to_string(),
                self.metadata_pool_max_connections.to_string(),
            ),
        ]);
        apply_warehouse_auth(&mut props, self.warehouse_uri, self.auth);

        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalogBuilder::default()
                .with_storage_factory(warehouse_factory(self.warehouse_uri)?)
                .load(self.catalog_name, props)
                .await
                .map_err(storage)?,
        );
        let cold = Arc::new(IcebergCold::new(
            Arc::clone(&catalog),
            NamespaceIdent::new(self.namespace.to_string()),
            self.file_target_bytes,
        ));
        Ok(ColdTier { cold, catalog })
    }
}

/// A constructed Iceberg cold tier and the catalog handle behind it.
///
/// [`cold`](Self::cold) is what a [`MeterStore`](crate::MeterStore) is built over;
/// [`catalog_facade`](Self::catalog_facade) exposes the same catalog to external
/// engines as a read-only Iceberg REST endpoint. The raw `Arc<dyn Catalog>` stays
/// private, so a caller never has to name — or depend on — `iceberg` itself.
pub struct ColdTier {
    cold: Arc<IcebergCold>,
    // Read only by `catalog_facade`; without that feature the handle is still held
    // (it is what keeps the catalog alive) but has no consumer.
    #[cfg_attr(not(feature = "catalog-facade"), allow(dead_code))]
    catalog: Arc<dyn Catalog>,
}

impl std::fmt::Debug for ColdTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdTier")
            .field("cold", &self.cold)
            .finish_non_exhaustive()
    }
}

impl ColdTier {
    /// The cold tier, for [`MeterStore::builder`](crate::MeterStore::builder).
    #[must_use]
    pub fn cold(&self) -> Arc<IcebergCold> {
        Arc::clone(&self.cold)
    }

    /// A read-only Iceberg REST catalog façade over this tier, for external engines
    /// (Spark, Trino, DuckDB, PyIceberg).
    #[cfg(feature = "catalog-facade")]
    #[must_use]
    pub fn catalog_facade(&self) -> crate::serve::CatalogFacade {
        crate::serve::CatalogFacade::new(Arc::clone(&self.catalog))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheme_defaults_to_file_when_absent() {
        assert_eq!(warehouse_scheme("/var/lib/warehouse"), "file");
        assert_eq!(warehouse_scheme("file:///tmp/wh"), "file");
        assert_eq!(warehouse_scheme("s3://bucket/prefix"), "s3");
        assert_eq!(warehouse_scheme("gs://bucket"), "gs");
        assert_eq!(warehouse_scheme("memory://"), "memory");
    }

    #[test]
    fn the_always_on_backends_build_without_a_feature() {
        // file:// and memory:// are the baseline, so these succeed in any build.
        assert!(warehouse_factory("file:///tmp/wh").is_ok());
        assert!(warehouse_factory("memory://").is_ok());
    }

    #[test]
    fn an_unknown_scheme_is_an_error_not_a_local_fallback() {
        // The old behaviour silently wrote a "cloud" warehouse to local disk.
        let err = warehouse_factory("ftp://host/wh").unwrap_err().to_string();
        assert!(err.contains("ftp"), "{err}");
    }

    #[test]
    fn auth_props_are_injected_only_for_s3_schemes() {
        use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION};

        let auth = WarehouseAuth {
            region: Some("eu-central-1".into()),
            endpoint: Some("http://minio:9000".into()),
            access_key_id: Some("AK".into()),
            secret_access_key: Some("SK".into()),
        };

        let mut s3 = HashMap::new();
        apply_warehouse_auth(&mut s3, "s3://bucket", &auth);
        assert_eq!(s3.get(S3_REGION).map(String::as_str), Some("eu-central-1"));
        assert_eq!(
            s3.get(S3_ENDPOINT).map(String::as_str),
            Some("http://minio:9000")
        );
        // A custom endpoint switches on path-style addressing.
        assert_eq!(
            s3.get(S3_PATH_STYLE_ACCESS).map(String::as_str),
            Some("true")
        );
        assert!(s3.contains_key(S3_ACCESS_KEY_ID));

        // A file warehouse ignores the credentials entirely.
        let mut file = HashMap::new();
        apply_warehouse_auth(&mut file, "file:///tmp/wh", &auth);
        assert!(file.is_empty(), "no S3 props for a file warehouse");
    }
}
