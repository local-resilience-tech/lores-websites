use lores_websites_node::{operations::AppOperation, LoresWebsiteNode};

use crate::AppState;

pub fn register_event_handlers(node: &LoresWebsiteNode, state: AppState) {
    let mut rx = node.subscribe();

    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(AppOperation::WebsiteCreatedV1(data)) => {
                    let website = crate::public_api::websites::Website {
                        name: data.name,
                        description: data.description,
                    };
                    state.websites.lock().await.push(website);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Event handler lagged, skipped {n} events");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}
