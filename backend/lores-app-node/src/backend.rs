use std::future::Future;

/// Error returned by [`NodeBackend::publish`].
#[derive(Debug)]
pub struct PublishError(pub String);

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for PublishError {}

/// Abstraction over operation transport backends.
///
/// Implementors can be swapped into [`AppNode`] to change how published
/// operations are delivered — e.g. direct gRPC, a queued/retry wrapper, or a
/// local in-process node for standalone / test use.
pub trait NodeBackend: Send + Sync + 'static {
    fn publish(
        &mut self,
        region_id: [u8; 32],
        namespace: &str,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(), PublishError>> + Send;
}

/// Central node handle used by application code.
///
/// Takes a [`NodeBackend`] that determines how operations are delivered.
/// Construct with a specific backend, e.g. [`crate::grpc::GrpcBackend`].
pub struct AppNode<B: NodeBackend> {
    pub region_id: [u8; 32],
    pub namespace: String,
    backend: tokio::sync::Mutex<B>,
}

impl<B: NodeBackend> AppNode<B> {
    pub fn new(region_id: [u8; 32], namespace: impl Into<String>, backend: B) -> Self {
        Self {
            region_id,
            namespace: namespace.into(),
            backend: tokio::sync::Mutex::new(backend),
        }
    }

    /// Publish a raw payload to this node's region and namespace.
    pub async fn publish_raw(&self, payload: Vec<u8>) -> Result<(), PublishError> {
        self.backend
            .lock()
            .await
            .publish(self.region_id, &self.namespace, payload)
            .await
    }
}
