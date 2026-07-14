use std::sync::Arc;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch, Mutex};

use crate::backoff::Backoff;
use crate::node::{map_store_error, NodeError};
use crate::store::{OperationStore, StoreError};

/// Drives a remote subscription in a loop, reconnecting with exponential
/// backoff on any failure.
pub(crate) struct LiveSubscription<Op> {
    operation_store: Arc<Mutex<Box<dyn OperationStore>>>,
    event_tx: broadcast::Sender<Op>,
    error_tx: watch::Sender<Option<NodeError>>,
}

impl<Op: Clone + Serialize + Send + 'static> LiveSubscription<Op> {
    pub(crate) fn new(
        operation_store: Arc<Mutex<Box<dyn OperationStore>>>,
        event_tx: broadcast::Sender<Op>,
        error_tx: watch::Sender<Option<NodeError>>,
    ) -> Self {
        Self {
            operation_store,
            event_tx,
            error_tx,
        }
    }

    /// Run the subscription loop forever. Call with `tokio::spawn`.
    pub(crate) async fn run(&self)
    where
        Op: for<'de> Deserialize<'de>,
    {
        let mut backoff = Backoff::new();

        loop {
            let Some(mut stream) = self.try_subscribe(&mut backoff).await else {
                continue;
            };

            while let Some(item) = stream.next().await {
                match item {
                    Ok(payload) => self.process_stream_item(&payload),
                    Err(e) => {
                        self.handle_mid_stream_error(e);
                        backoff.reset();
                        break;
                    }
                }
            }

            tracing::info!("Subscription stream ended, reconnecting…");
        }
    }

    async fn try_subscribe(&self, backoff: &mut Backoff) -> Option<crate::store::OperationStream> {
        match self.operation_store.lock().await.subscribe().await {
            Ok(s) => {
                self.error_tx.send_replace(None);
                backoff.reset();
                Some(s)
            }
            Err(err @ StoreError::RegionNotBound(_)) => {
                tracing::warn!(
                    "Subscribe failed — region not bound (retrying in {:?})",
                    backoff.current
                );
                backoff
                    .set_error_and_advance(&self.error_tx, map_store_error(err))
                    .await;
                None
            }
            Err(err @ StoreError::Other(_)) => {
                tracing::error!("Subscribe failed (retrying in {:?})", backoff.current);
                backoff
                    .set_error_and_advance(&self.error_tx, map_store_error(err))
                    .await;
                None
            }
        }
    }

    fn handle_mid_stream_error(&self, err: StoreError) {
        tracing::warn!("Stream disconnected, reconnecting: {err}");
        self.error_tx.send_replace(Some(map_store_error(err)));
    }

    /// Deserialize and broadcast a single payload.
    fn process_stream_item(&self, payload: &[u8])
    where
        Op: for<'de> Deserialize<'de>,
    {
        match serde_json::from_slice::<Op>(payload) {
            Ok(op) => {
                let _ = self.event_tx.send(op);
            }
            Err(e) => tracing::warn!("Failed to deserialize operation: {e}"),
        }
    }
}
