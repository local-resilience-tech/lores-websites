use std::pin::Pin;

use lores_p2panda_client::PandaClient;

use crate::store::{OperationStore, StoreError};

/// [`OperationStore`] implementation that forwards operations to a lores-node
/// instance via gRPC using [`PandaClient`].
pub(crate) struct GrpcOperationStore {
    client: PandaClient,
    app_id: String,
    instance_id: String,
}

impl GrpcOperationStore {
    pub(crate) fn connect_lazy(
        grpc_addr: String,
        app_id: impl Into<String>,
        instance_id: impl Into<String>,
    ) -> Result<Self, tonic::transport::Error> {
        let client = PandaClient::connect_lazy(grpc_addr)?;
        Ok(Self {
            client,
            app_id: app_id.into(),
            instance_id: instance_id.into(),
        })
    }
}

impl OperationStore for GrpcOperationStore {
    fn publish(
        &mut self,
        payload: Vec<u8>,
        idempotency_key: Option<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), StoreError>> + Send + '_>> {
        Box::pin(async move {
            self.client
                .publish(
                    &self.app_id,
                    &self.instance_id,
                    payload,
                    idempotency_key.map(|k| k.into_bytes()),
                )
                .await
                .map(|_| ())
                .map_err(|e| StoreError(e.to_string()))
        })
    }
}
