use std::future::Future;
use std::pin::Pin;

/// Error returned by a [`Transport`] publish call.
#[derive(Debug)]
pub struct TransportError(pub String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TransportError {}

/// Internal trait over raw-bytes operation delivery.
///
/// App developers never interact with this directly — they use [`crate::AppNode`]
/// and its named constructors (`grpc`, etc.).
pub(crate) trait Transport: Send + Sync + 'static {
    fn publish(
        &mut self,
        region_id: [u8; 32],
        namespace: &str,
        payload: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;
}
