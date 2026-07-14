use lores_websites_node::{operations::AppOperation, LoresWebsiteNode};

use crate::AppState;

pub fn register_event_handlers(node: &LoresWebsiteNode, state: AppState) {
    let mut rx = node.subscribe();

    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(AppOperation::WebsiteCreatedV1(data)) => {
                    let result = sqlx::query(
                        "INSERT INTO websites (id, name, description) VALUES (?, ?, ?)",
                    )
                    .bind(&data.id)
                    .bind(&data.name)
                    .bind(&data.description)
                    .execute(&state.db)
                    .await;

                    if let Err(e) = result {
                        tracing::error!("Failed to project WebsiteCreatedV1: {e}");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Event handler lagged, skipped {n} events");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}
