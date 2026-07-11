use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    Extension,
};
use lores_websites_node::LoresWebsiteNode;

pub async fn handler(
    ws: WebSocketUpgrade,
    Extension(node): Extension<LoresWebsiteNode>,
) -> impl IntoResponse {
    tracing::debug!("WebSocket upgrade request received");
    ws.on_upgrade(|socket| handle_socket(socket, node))
}

async fn handle_socket(mut socket: WebSocket, node: LoresWebsiteNode) {
    tracing::info!("WebSocket client connected");
    let mut error_rx = node.subscribe_errors();

    loop {
        // `changed()` marks the current value as seen and waits for the next
        // change. We call `borrow_and_update()` first so that an error already
        // set before this client connected is sent immediately on connect.
        let error = error_rx.borrow_and_update().clone();

        if let Some(error) = error {
            tracing::info!("Forwarding node error to WebSocket client: {error}");
            let msg = serde_json::json!({ "type": "error", "error": error });
            if socket
                .send(Message::Text(msg.to_string().into()))
                .await
                .is_err()
            {
                tracing::warn!("Failed to send message to WebSocket client, closing");
                break;
            }
        }

        // Wait for the next change.
        if error_rx.changed().await.is_err() {
            tracing::warn!("Error channel closed");
            break;
        }
    }

    tracing::info!("WebSocket client disconnected");
}
