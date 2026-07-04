use lores_p2panda_client::PandaClient;

use crate::backend::{NodeBackend, PublishError};

/// [`NodeBackend`] that forwards operations directly to a lores-node instance
/// via gRPC using [`PandaClient`].
pub struct GrpcBackend {
    client: PandaClient,
}

impl GrpcBackend {
    /// Create a backend with a lazy gRPC connection — no network call is made
    /// until the first publish.
    pub fn connect_lazy(grpc_addr: String) -> Result<Self, tonic::transport::Error> {
        let client = PandaClient::connect_lazy(grpc_addr)?;
        Ok(Self { client })
    }
}

impl NodeBackend for GrpcBackend {
    async fn publish(
        &mut self,
        region_id: [u8; 32],
        namespace: &str,
        payload: Vec<u8>,
    ) -> Result<(), PublishError> {
        self.client
            .publish(region_id, namespace, payload)
            .await
            .map(|_| ())
            .map_err(|e| PublishError(e.to_string()))
    }
}
