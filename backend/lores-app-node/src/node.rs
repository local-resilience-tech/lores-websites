use std::sync::Arc;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::{broadcast, watch, Mutex};

use crate::backoff::Backoff;
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
    /// The remote gRPC server could not be reached or did not respond.
    GrpcUnavailable(String),
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::RegionNotBound(msg) => write!(f, "{msg}"),
            NodeError::GrpcUnavailable(msg) => write!(f, "{msg}"),
        }
    }
}

/// The central node handle used by application code.
///
/// Generic over the operation type `Op` — the application supplies its own
/// operation enum and `AppNode` handles serialization, loopback broadcast, and
/// error logging consistently across all operation store backends.
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
    operation_store: Arc<Mutex<Box<dyn OperationStore>>>,
    event_tx: broadcast::Sender<Op>,
    error_tx: watch::Sender<Option<NodeError>>,
}

impl<Op> Clone for AppNode<Op> {
    fn clone(&self) -> Self {
        Self {
            app_id: self.app_id.clone(),
            instance_id: self.instance_id.clone(),
            operation_store: self.operation_store.clone(),
            event_tx: self.event_tx.clone(),
            error_tx: self.error_tx.clone(),
        }
    }
}

impl<Op: Clone + Serialize + Send + 'static> AppNode<Op> {
    fn new(
        app_id: impl Into<String>,
        instance_id: impl Into<String>,
        operation_store: Box<dyn OperationStore>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(64);
        let (error_tx, _) = watch::channel(None);
        Self {
            app_id: app_id.into(),
            instance_id: instance_id.into(),
            operation_store: Arc::new(Mutex::new(operation_store)),
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
            .expect("failed to build gRPC opereration store");
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
        let operation_store = GrpcOperationStore::connect_lazy(grpc_addr, &app_id, &instance_id)
            .expect("failed to build gRPC operation store");
        Self::new(app_id, instance_id, Box::new(operation_store))
    }

    /// Subscribe to operations published through this node (loopback).
    pub fn subscribe(&self) -> broadcast::Receiver<Op> {
        self.event_tx.subscribe()
    }

    /// Watch the current node error state.
    ///
    /// The receiver immediately reflects the current value, so callers that
    /// subscribe after an error was set will see it right away.
    pub fn subscribe_errors(&self) -> watch::Receiver<Option<NodeError>> {
        self.error_tx.subscribe()
    }

    /// Replay all locally-stored operations, broadcasting each through the
    /// event channel.
    pub async fn replay(&self)
    where
        Op: for<'de> Deserialize<'de>,
    {
        let stream_result = {
            let mut t = self.operation_store.lock().await;
            t.replay().await
        };

        let mut stream = match stream_result {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Replay failed: {e}");
                return;
            }
        };

        let mut count = 0usize;
        while let Some(item) = stream.next().await {
            match item {
                Ok(payload) => {
                    if self.broadcast_payload(&payload) {
                        count += 1;
                    }
                }
                Err(e) => tracing::warn!("Error reading replayed operation: {e}"),
            }
        }

        tracing::info!(count, "replay complete");
    }

    /// Serialize and publish an operation, then broadcast it locally.
    pub async fn publish(&self, operation: &Op) -> Result<(), StoreError> {
        let payload = serde_json::to_vec(operation).map_err(|e| {
            tracing::error!("Failed to serialize operation: {e}");
            StoreError::Other(format!("Failed to serialize operation: {e}"))
        })?;
        let mut t = self.operation_store.lock().await;
        t.publish(payload, None).await?;
        drop(t);
        let _ = self.event_tx.send(operation.clone());
        Ok(())
    }

    /// Drive the remote subscription in a loop, broadcasting incoming operations
    /// and errors to all subscribers.
    ///
    /// Retries on all transient failures with exponential backoff (1 s → 60 s).
    /// The backoff resets whenever the error variant changes (e.g. `GrpcUnavailable`
    /// → `RegionNotBound`). Call it with `tokio::spawn` from your application's `main`.
    pub async fn run(&self)
    where
        Op: for<'de> Deserialize<'de>,
    {
        let mut backoff = Backoff::new();

        loop {
            let Some(mut stream) = self.try_subscribe(&mut backoff).await else {
                continue;
            };

            while let Some(item) = stream.next().await {
                if !self.process_stream_item(item, &mut backoff).await {
                    break;
                }
            }

            tracing::info!("Subscription stream ended, reconnecting…");
        }
    }

    /// Handle one item from the subscription stream.
    /// Returns `true` to keep iterating, `false` to break and reconnect.
    async fn process_stream_item(
        &self,
        item: Result<Vec<u8>, StoreError>,
        backoff: &mut Backoff,
    ) -> bool
    where
        Op: for<'de> Deserialize<'de>,
    {
        match item {
            Ok(payload) => {
                self.broadcast_payload(&payload);
                true
            }
            Err(err @ StoreError::Other(_)) => {
                tracing::warn!(
                    "Error on subscription stream (retrying in {:?})",
                    backoff.current
                );
                backoff
                    .set_error_and_advance(&self.error_tx, map_store_error(err))
                    .await;
                false
            }
            Err(err @ StoreError::RegionNotBound(_)) => {
                tracing::warn!(
                    "Region unbound mid-stream (retrying in {:?})",
                    backoff.current
                );
                backoff
                    .set_error_and_advance(&self.error_tx, map_store_error(err))
                    .await;
                false
            }
        }
    }

    async fn try_subscribe(&self, backoff: &mut Backoff) -> Option<crate::store::OperationStream> {
        match self.operation_store.lock().await.subscribe().await {
            Ok(s) => {
                self.error_tx.send_replace(None);
                backoff.reset();
                Some(s)
            }
            Err(err @ StoreError::RegionNotBound(_)) => {
                tracing::warn!(
                    "Subscribe failed — region not bound (retrying in {:?})",
                    backoff.current
                );
                backoff
                    .set_error_and_advance(&self.error_tx, map_store_error(err))
                    .await;
                None
            }
            Err(err @ StoreError::Other(_)) => {
                tracing::error!("Subscribe failed (retrying in {:?})", backoff.current);
                backoff
                    .set_error_and_advance(&self.error_tx, map_store_error(err))
                    .await;
                None
            }
        }
    }
}

impl<Op: Clone + Serialize + Send + for<'de> Deserialize<'de> + 'static> AppNode<Op> {
    /// Deserialize a raw payload and broadcast it on the event channel.
    /// Returns `true` if the operation was successfully broadcast.
    fn broadcast_payload(&self, payload: &[u8]) -> bool {
        match serde_json::from_slice::<Op>(payload) {
            Ok(op) => {
                let _ = self.event_tx.send(op);
                true
            }
            Err(e) => {
                tracing::warn!("Failed to deserialize operation: {e}");
                false
            }
        }
    }
}

fn map_store_error(err: StoreError) -> NodeError {
    match err {
        StoreError::RegionNotBound(msg) => NodeError::RegionNotBound(msg),
        StoreError::Other(msg) => NodeError::GrpcUnavailable(msg),
    }
}
