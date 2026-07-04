use std::pin::Pin;

use sqlx::SqlitePool;

use crate::store::{OperationStore, StoreError};

/// [`OperationStore`] implementation backed by a local SQLite database.
///
/// Operations are persisted in insertion order. This store is the foundation
/// for offline operation and the outgoing queue that a future drain task will
/// deliver to lores-node.
pub(crate) struct LocalOperationStore {
    pool: SqlitePool,
}

impl LocalOperationStore {
    pub(crate) async fn new(pool: SqlitePool) -> Result<Self, sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS lores_app_operations (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                payload    BLOB    NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (unixepoch())
            )",
        )
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }
}

impl OperationStore for LocalOperationStore {
    fn publish(
        &mut self,
        payload: Vec<u8>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), StoreError>> + Send + '_>> {
        Box::pin(async move {
            sqlx::query("INSERT INTO lores_app_operations (payload) VALUES (?)")
                .bind(payload)
                .execute(&self.pool)
                .await
                .map(|_| ())
                .map_err(|e| StoreError(e.to_string()))
        })
    }
}
