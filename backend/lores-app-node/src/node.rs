use std::sync::Arc;

use serde::Serialize;
use sqlx::SqlitePool;
use tokio::sync::{broadcast, Mutex};

use crate::grpc::GrpcOperationStore;
use crate::local::LocalOperationStore;
use crate::store::OperationStore;

/// The central node handle used by application code.
///
/// Generic over the operation type `Op` — the application supplies its own
/// operation enum and `AppNode` handles serialization, loopback broadcast, and
/// error logging consistently across all transport backends.
///
/// Construct via the named constructors rather than directly:
/// ```no_run
/// # use lores_app_node::AppNode;
/// # #[derive(Clone, serde::Serialize)] enum Op {}
/// let node = AppNode::<Op>::grpc("http://[::1]:50051".into(), [0u8; 32], "my-app");
/// ```
pub struct AppNode<Op> {
    pub region_id: [u8; 32],
    pub namespace: String,
    transport: Arc<Mutex<Box<dyn OperationStore>>>,
    event_tx: broadcast::Sender<Op>,
}

impl<Op> Clone for AppNode<Op> {
    fn clone(&self) -> Self {
        Self {
            region_id: self.region_id,
            namespace: self.namespace.clone(),
            transport: self.transport.clone(),
            event_tx: self.event_tx.clone(),
        }
    }
}

impl<Op: Clone + Serialize + Send + 'static> AppNode<Op> {
    fn new(
        region_id: [u8; 32],
        namespace: impl Into<String>,
        transport: Box<dyn OperationStore>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(64);
        Self {
            region_id,
            namespace: namespace.into(),
            transport: Arc::new(Mutex::new(transport)),
            event_tx,
        }
    }

    /// Create a local-only `AppNode` backed by a SQLite store.
    ///
    /// Operations are persisted locally and never forwarded to a remote node.
    pub async fn local(
        pool: SqlitePool,
        region_id: [u8; 32],
        namespace: impl Into<String>,
    ) -> Result<Self, sqlx::Error> {
        let store = LocalOperationStore::new(pool).await?;
        Ok(Self::new(region_id, namespace, Box::new(store)))
    }

    /// Create an `AppNode` connected to an external lores-node via gRPC.
    ///
    /// Uses a lazy connection — no network call until the first publish.
    pub fn grpc(grpc_addr: String, region_id: [u8; 32], namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let transport = GrpcOperationStore::connect_lazy(grpc_addr, region_id, namespace.clone())
            .expect("failed to build gRPC transport endpoint");
        Self::new(region_id, namespace, Box::new(transport))
    }

    /// Subscribe to operations published through this node (loopback).
    pub fn subscribe(&self) -> broadcast::Receiver<Op> {
        self.event_tx.subscribe()
    }

    /// Serialize and publish an operation, then broadcast it locally.
    pub async fn publish(&self, operation: &Op) {
        match serde_json::to_vec(operation) {
            Ok(payload) => {
                let mut t = self.transport.lock().await;
                if let Err(e) = t.publish(payload).await {
                    tracing::error!("Failed to publish operation: {e}");
                }
            }
            Err(e) => tracing::error!("Failed to serialize operation: {e}"),
        }
        let _ = self.event_tx.send(operation.clone());
    }
}
