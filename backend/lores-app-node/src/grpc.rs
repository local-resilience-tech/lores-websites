use std::pin::Pin;

use lores_p2panda_client::PandaClient;

use crate::store::{OperationStore, StoreError};

/// [`OperationStore`] implementation that forwards operations to a lores-node
/// instance via gRPC using [`PandaClient`].
pub(crate) struct GrpcOperationStore {
    client: PandaClient,
    region_id: [u8; 32],
    namespace: String,
}

impl GrpcOperationStore {
    pub(crate) fn connect_lazy(
        grpc_addr: String,
        region_id: [u8; 32],
        namespace: String,
    ) -> Result<Self, tonic::transport::Error> {
        let client = PandaClient::connect_lazy(grpc_addr)?;
        Ok(Self {
            client,
            region_id,
            namespace,
        })
    }
}

impl OperationStore for GrpcOperationStore {
    fn publish(
        &mut self,
        payload: Vec<u8>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), StoreError>> + Send + '_>> {
        Box::pin(async move {
            self.client
                .publish(self.region_id, &self.namespace, payload)
                .await
                .map(|_| ())
                .map_err(|e| StoreError(e.to_string()))
        })
    }
}
