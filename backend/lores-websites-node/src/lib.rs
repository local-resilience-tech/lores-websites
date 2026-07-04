use lores_p2panda_client::PandaClient;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::Mutex;

use crate::operations::AppOperation;

pub mod operations;

#[derive(Clone)]
pub struct AppNode {
    pub region_id: [u8; 32],
    pub namespace: String,
    panda: Arc<Mutex<PandaClient>>,
    event_tx: broadcast::Sender<AppOperation>,
}

impl AppNode {
    pub fn connect(grpc_addr: String, region_id: [u8; 32], namespace: impl Into<String>) -> Self {
        let panda =
            PandaClient::connect_lazy(grpc_addr).expect("failed to connect to panda gRPC endpoint");
        let (event_tx, _) = broadcast::channel(64);
        Self {
            region_id,
            namespace: namespace.into(),
            panda: Arc::new(Mutex::new(panda)),
            event_tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AppOperation> {
        self.event_tx.subscribe()
    }

    pub async fn publish(&self, operation: &AppOperation) {
        match serde_json::to_vec(operation) {
            Ok(payload) => {
                let mut client = self.panda.lock().await;
                if let Err(e) = client
                    .publish(self.region_id, &self.namespace, payload)
                    .await
                {
                    tracing::error!("Failed to publish operation: {e}");
                }
            }
            Err(e) => tracing::error!("Failed to serialize operation: {e}"),
        }
        // Broadcast to local event handlers (ignore if no subscribers)
        let _ = self.event_tx.send(operation.clone());
    }
}
