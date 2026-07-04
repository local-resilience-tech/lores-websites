use axum::{http::StatusCode, response::IntoResponse, Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    node::AppNode,
    operations::{AppOperation, WebsiteCreatedDataV1},
    AppState,
};

#[derive(Clone, Serialize, ToSchema)]
pub struct Website {
    pub name: String,
    pub description: String,
}

#[derive(Deserialize, ToSchema)]
pub struct CreateWebsiteData {
    pub name: String,
    pub description: String,
}

pub fn router() -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(websites_index))
        .routes(routes!(create_website))
}

#[utoipa::path(
    get,
    path = "/",
    responses(
        (status = 200, body = Vec<Website>),
    )
)]
pub async fn websites_index(Extension(state): Extension<AppState>) -> impl IntoResponse {
    let websites = state.websites.lock().await;
    Json(websites.clone())
}

#[utoipa::path(
    post,
    path = "/",
    request_body = CreateWebsiteData,
    responses(
        (status = 201, body = Website),
    )
)]
pub async fn create_website(
    Extension(state): Extension<AppState>,
    Extension(node): Extension<AppNode>,
    Json(payload): Json<CreateWebsiteData>,
) -> impl IntoResponse {
    let website = Website {
        name: payload.name.clone(),
        description: payload.description.clone(),
    };

    // Broadcast a "website created" operation over the lores-p2panda network.
    node.publish(&AppOperation::WebsiteCreatedV1(WebsiteCreatedDataV1 {
        name: payload.name.clone(),
        description: payload.description.clone(),
    }))
    .await;

    state.websites.lock().await.push(website.clone());
    (StatusCode::CREATED, Json(website))
}
