use axum::{extract::ws::WebSocketUpgrade, response::IntoResponse};

pub async fn handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(|_socket| async {})
}
