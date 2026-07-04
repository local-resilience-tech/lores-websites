use std::sync::Arc;

use lores_p2panda_client::PandaClient;
use serde::Serialize;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AppNode {
    pub region_id: [u8; 32],
    pub namespace: String,
    pub panda: Arc<Mutex<PandaClient>>,
}

impl AppNode {
    pub fn new(
        region_id: [u8; 32],
        namespace: impl Into<String>,
        panda: Arc<Mutex<PandaClient>>,
    ) -> Self {
        Self {
            region_id,
            namespace: namespace.into(),
            panda,
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
