use lores_app_node::{grpc::GrpcBackend, AppNode as BaseAppNode};
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::operations::AppOperation;

pub mod operations;

type Inner = BaseAppNode<GrpcBackend>;

#[derive(Clone)]
pub struct AppNode {
    inner: Arc<Inner>,
    event_tx: broadcast::Sender<AppOperation>,
}

impl AppNode {
    pub fn connect(grpc_addr: String, region_id: [u8; 32], namespace: impl Into<String>) -> Self {
        let backend =
            GrpcBackend::connect_lazy(grpc_addr).expect("failed to connect to panda gRPC endpoint");
        let namespace = namespace.into();
        let inner = Arc::new(BaseAppNode::new(region_id, namespace, backend));
        let (event_tx, _) = broadcast::channel(64);
        Self { inner, event_tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AppOperation> {
        self.event_tx.subscribe()
    }

    pub async fn publish(&self, operation: &AppOperation) {
        match serde_json::to_vec(operation) {
            Ok(payload) => {
                if let Err(e) = self.inner.publish_raw(payload).await {
                    tracing::error!("Failed to publish operation: {e}");
                }
            }
            Err(e) => tracing::error!("Failed to serialize operation: {e}"),
        }
        // Broadcast to local event handlers (ignore if no subscribers)
        let _ = self.event_tx.send(operation.clone());
    }
}
