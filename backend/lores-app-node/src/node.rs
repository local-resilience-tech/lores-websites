use std::sync::Arc;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::{broadcast, Mutex};

use crate::grpc::GrpcOperationStore;
use crate::local::LocalOperationStore;
use crate::outbox::OutboxStore;
use crate::store::{OperationStore, StoreError};

/// Errors emitted by the node that consumers (e.g. a WebSocket handler) may
/// want to surface directly to users.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeError {
    /// No region has been bound to this app/instance on the remote server.
    RegionNotBound(String),
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::RegionNotBound(msg) => write!(f, "{msg}"),
        }
    }
}

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
/// let node = AppNode::<Op>::grpc("http://[::1]:50051".into(), "my-app-id", "my-instance");
/// ```
pub struct AppNode<Op> {
    pub app_id: String,
    pub instance_id: String,
    transport: Arc<Mutex<Box<dyn OperationStore>>>,
    event_tx: broadcast::Sender<Op>,
    error_tx: broadcast::Sender<NodeError>,
}

impl<Op> Clone for AppNode<Op> {
    fn clone(&self) -> Self {
        Self {
            app_id: self.app_id.clone(),
            instance_id: self.instance_id.clone(),
            transport: self.transport.clone(),
            event_tx: self.event_tx.clone(),
            error_tx: self.error_tx.clone(),
        }
    }
}

impl<Op: Clone + Serialize + Send + 'static> AppNode<Op> {
    fn new(
        app_id: impl Into<String>,
        instance_id: impl Into<String>,
        transport: Box<dyn OperationStore>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(64);
        let (error_tx, _) = broadcast::channel(16);
        Self {
            app_id: app_id.into(),
            instance_id: instance_id.into(),
            transport: Arc::new(Mutex::new(transport)),
            event_tx,
            error_tx,
        }
    }

    /// Create a local-only `AppNode` backed by a SQLite store.
    ///
    /// Operations are persisted locally and never forwarded to a remote node.
    pub async fn local(
        pool: SqlitePool,
        app_id: impl Into<String>,
        instance_id: impl Into<String>,
    ) -> Result<Self, sqlx::Error> {
        let store = LocalOperationStore::new(pool).await?;
        Ok(Self::new(app_id, instance_id, Box::new(store)))
    }

    /// Create an `AppNode` that persists to a local SQLite store and forwards
    /// to lores-node via gRPC, using the local row id as an idempotency key.
    ///
    /// If gRPC delivery fails the operation is retained locally for a future
    /// drain attempt.
    pub async fn grpc_with_local(
        pool: SqlitePool,
        grpc_addr: String,
        app_id: impl Into<String>,
        instance_id: impl Into<String>,
    ) -> Result<Self, sqlx::Error> {
        let app_id = app_id.into();
        let instance_id = instance_id.into();
        let local = LocalOperationStore::new(pool).await?;
        let remote = GrpcOperationStore::connect_lazy(grpc_addr, &app_id, &instance_id)
            .expect("failed to build gRPC transport endpoint");
        let store = OutboxStore::new(local, remote);
        Ok(Self::new(app_id, instance_id, Box::new(store)))
    }

    /// Create an `AppNode` connected to an external lores-node via gRPC.
    ///
    /// Uses a lazy connection — no network call until the first publish.
    pub fn grpc(
        grpc_addr: String,
        app_id: impl Into<String>,
        instance_id: impl Into<String>,
    ) -> Self {
        let app_id = app_id.into();
        let instance_id = instance_id.into();
        let transport = GrpcOperationStore::connect_lazy(grpc_addr, &app_id, &instance_id)
            .expect("failed to build gRPC transport endpoint");
        Self::new(app_id, instance_id, Box::new(transport))
    }

    /// Subscribe to operations published through this node (loopback).
    pub fn subscribe(&self) -> broadcast::Receiver<Op> {
        self.event_tx.subscribe()
    }

    /// Subscribe to node-level errors (e.g. [`NodeError::RegionNotBound`]).
    pub fn subscribe_errors(&self) -> broadcast::Receiver<NodeError> {
        self.error_tx.subscribe()
    }

    /// Serialize and publish an operation, then broadcast it locally.
    pub async fn publish(&self, operation: &Op) -> Result<(), StoreError> {
        match serde_json::to_vec(operation) {
            Ok(payload) => {
                let mut t = self.transport.lock().await;
                t.publish(payload, None).await?;
            }
            Err(e) => tracing::error!("Failed to serialize operation: {e}"),
        }
        let _ = self.event_tx.send(operation.clone());
        Ok(())
    }

    /// Drive the remote subscription in a loop, broadcasting incoming operations
    /// and errors to all subscribers.
    ///
    /// This method runs until the stream ends or a non-retryable error occurs.
    /// Call it with `tokio::spawn` from your application's `main`.
    pub async fn run(&self)
    where
        Op: for<'de> Deserialize<'de>,
    {
        loop {
            let stream_result = {
                let mut t = self.transport.lock().await;
                t.subscribe().await
            };

            let mut stream = match stream_result {
                Ok(s) => s,
                Err(StoreError::RegionNotBound(msg)) => {
                    tracing::warn!("Subscribe failed — region not bound: {msg}");
                    let _ = self.error_tx.send(NodeError::RegionNotBound(msg));
                    return;
                }
                Err(StoreError::Other(msg)) => {
                    tracing::error!("Subscribe failed: {msg}");
                    return;
                }
            };

            while let Some(item) = stream.next().await {
                match item {
                    Ok(payload) => match serde_json::from_slice::<Op>(&payload) {
                        Ok(op) => {
                            let _ = self.event_tx.send(op);
                        }
                        Err(e) => tracing::warn!("Failed to deserialize incoming operation: {e}"),
                    },
                    Err(StoreError::Other(msg)) => {
                        tracing::warn!("Error on subscription stream: {msg}");
                    }
                    Err(StoreError::RegionNotBound(msg)) => {
                        tracing::warn!("Region unbound mid-stream: {msg}");
                        let _ = self.error_tx.send(NodeError::RegionNotBound(msg));
                        return;
                    }
                }
            }

            tracing::info!("Subscription stream ended, reconnecting…");
        }
    }
}
