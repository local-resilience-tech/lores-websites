use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    Extension,
};
use lores_websites_node::LoresWebsiteNode;
use serde::Serialize;
use tokio::sync::watch;

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Status { ready: bool },
    Error { error: String },
}

pub async fn handler(
    ws: WebSocketUpgrade,
    Extension(node): Extension<LoresWebsiteNode>,
    Extension(ready_rx): Extension<watch::Receiver<bool>>,
) -> impl IntoResponse {
    tracing::debug!("WebSocket upgrade request received");
    ws.on_upgrade(|socket| handle_socket(socket, node, ready_rx))
}

async fn handle_socket(
    mut socket: WebSocket,
    node: LoresWebsiteNode,
    mut ready_rx: watch::Receiver<bool>,
) {
    tracing::info!("WebSocket client connected");

    // Send the current ready state immediately on connect.
    let ready = *ready_rx.borrow_and_update();
    let msg = ServerMessage::Status { ready };
    if socket
        .send(Message::Text(serde_json::to_string(&msg).unwrap().into()))
        .await
        .is_err()
    {
        tracing::warn!("Failed to send status to WebSocket client, closing");
        return;
    }

    // If replay is in progress, wait for it to finish and notify the client.
    if !ready {
        loop {
            if ready_rx.changed().await.is_err() {
                tracing::warn!("Ready channel closed before becoming ready");
                return;
            }
            if *ready_rx.borrow() {
                break;
            }
        }
        let msg = ServerMessage::Status { ready: true };
        if socket
            .send(Message::Text(serde_json::to_string(&msg).unwrap().into()))
            .await
            .is_err()
        {
            tracing::warn!("Failed to send ready status to WebSocket client, closing");
            return;
        }
    }

    let mut error_rx = node.subscribe_errors();

    loop {
        // `changed()` marks the current value as seen and waits for the next
        // change. We call `borrow_and_update()` first so that an error already
        // set before this client connected is sent immediately on connect.
        let error = error_rx.borrow_and_update().clone();

        if let Some(error) = error {
            tracing::info!("Forwarding node error to WebSocket client: {error}");
            let msg = ServerMessage::Error {
                error: error.to_string(),
            };
            if socket
                .send(Message::Text(serde_json::to_string(&msg).unwrap().into()))
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
