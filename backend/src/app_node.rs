use std::sync::Arc;

use lores_p2panda_client::PandaClient;
use serde::Serialize;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AppNode {
    pub region_id: [u8; 32],
    pub namespace: String,
    panda: Arc<Mutex<PandaClient>>,
}

impl AppNode {
    pub fn connect(grpc_addr: String, region_id: [u8; 32], namespace: impl Into<String>) -> Self {
        let panda =
            PandaClient::connect_lazy(grpc_addr).expect("failed to connect to panda gRPC endpoint");
        Self {
            region_id,
            namespace: namespace.into(),
            panda: Arc::new(Mutex::new(panda)),
        }
    }

    pub async fn publish(&self, operation: &impl Serialize) {
        match serde_json::to_vec(operation) {
            Ok(payload) => {
                let mut client = self.panda.lock().await;
                if let Err(e) = client
                    .publish(self.region_id, &self.namespace, payload)
                    .await
                {
                    eprintln!("Failed to publish operation: {e}");
                }
            }
            Err(e) => eprintln!("Failed to serialize operation: {e}"),
        }
    }
}
