//! A cold commit that lands only on the snapshot it was derived from.
//!
//! `iceberg`'s `Transaction::commit` reloads the table and re-applies its
//! actions to the reload on every attempt — the first included — before it asks
//! the catalogue to compare-and-swap. The catalogue then validates the *reload*,
//! not the base the caller read. Every check this crate runs before a commit —
//! strict advance, the watermark a correction preserves, the window an archival
//! run claims — is a statement about the base the caller read, so a commit
//! rebased onto a newer snapshot carries a summary derived from state it never
//! saw: a correction republishes an older boundary over a newer one, a second
//! archiver lands a window twice.
//!
//! [`BaseGuard`] closes that gap at the one seam a caller holds. It is the
//! catalogue passed to `commit`, and it answers the commit's reload: if the
//! table's current snapshot is no longer the base the commit was derived from,
//! the reload fails as a compare-and-swap conflict. The commit loop then reloads
//! and re-derives the summary against what is current — the same thing it does
//! when the catalogue itself reports the conflict — and the catalogue's own
//! requirement, built from a reload that matched, asserts exactly the base the
//! checks ran against. Iceberg Java's `SnapshotAncestryValidator` is the same
//! idea expressed inside the library.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit,
    TableCreation, TableIdent,
};

/// The catalogue a conditional commit is made through.
///
/// Every call but `load_table` forwards unchanged. `load_table` forwards and
/// then refuses a table whose current snapshot is not `base`.
pub(crate) struct BaseGuard {
    inner: Arc<dyn Catalog>,
    base: Option<i64>,
}

impl BaseGuard {
    /// A guard that admits a commit only onto `base`'s current snapshot.
    pub(crate) fn on(inner: Arc<dyn Catalog>, base: &Table) -> Self {
        Self {
            inner,
            base: base.metadata().current_snapshot().map(|s| s.snapshot_id()),
        }
    }
}

impl std::fmt::Debug for BaseGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseGuard")
            .field("base", &self.base)
            .finish()
    }
}

#[async_trait]
impl Catalog for BaseGuard {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        let loaded = self.inner.load_table(table).await?;
        let current = loaded
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id());
        if current != self.base {
            return Err(iceberg::Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!(
                    "{table}: the table moved from snapshot {:?} to {current:?} after this \
                     commit was derived; re-derive against the current snapshot",
                    self.base
                ),
            )
            // Not for the library to retry: its retry would rebase, which is the
            // thing this refuses. The caller's loop re-derives instead.
            .with_retryable(false));
        }
        Ok(loaded)
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.inner.update_table(commit).await
    }

    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.purge_table(table).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        self.inner.register_table(table, metadata_location).await
    }
}
