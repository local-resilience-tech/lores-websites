use std::future::Future;
use std::pin::Pin;

/// Error returned by a [`Transport`] publish call.
#[derive(Debug)]
pub enum StoreError {
    /// No region has been bound to the given app/instance on the server.
    RegionNotBound(String),
    /// Any other error.
    Other(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::RegionNotBound(msg) => write!(f, "{msg}"),
            StoreError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// Internal trait over raw-bytes operation delivery.
///
/// App developers never interact with this directly — they use [`crate::AppNode`]
/// and its named constructors (`grpc`, etc.).
pub(crate) trait OperationStore: Send + Sync + 'static {
    fn publish(
        &mut self,
        payload: Vec<u8>,
        idempotency_key: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + '_>>;
}
