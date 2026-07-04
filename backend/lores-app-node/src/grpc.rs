use std::pin::Pin;

use lores_p2panda_client::PandaClient;

use crate::transport::{Transport, TransportError};

/// [`Transport`] implementation that forwards operations to a lores-node
/// instance via gRPC using [`PandaClient`].
pub(crate) struct GrpcTransport {
    client: PandaClient,
}

impl GrpcTransport {
    pub(crate) fn connect_lazy(grpc_addr: String) -> Result<Self, tonic::transport::Error> {
        let client = PandaClient::connect_lazy(grpc_addr)?;
        Ok(Self { client })
    }
}

impl Transport for GrpcTransport {
    fn publish(
        &mut self,
        region_id: [u8; 32],
        namespace: &str,
        payload: Vec<u8>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send + '_>> {
        let namespace = namespace.to_owned();
        Box::pin(async move {
            self.client
                .publish(region_id, &namespace, payload)
                .await
                .map(|_| ())
                .map_err(|e| TransportError(e.to_string()))
        })
    }
}
