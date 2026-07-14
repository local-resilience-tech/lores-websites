use axum::{http::StatusCode, response::IntoResponse, Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::AppState;
use lores_websites_node::{
    operations::{AppOperation, WebsiteCreatedDataV1},
    LoresWebsiteNode,
};

#[derive(Clone, Serialize, ToSchema, sqlx::FromRow)]
pub struct Website {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Deserialize, ToSchema)]
pub struct CreateWebsiteData {
    pub name: String,
    pub description: Option<String>,
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
    match sqlx::query_as::<_, Website>("SELECT id, name, description FROM websites")
        .fetch_all(&state.db)
        .await
    {
        Ok(websites) => Json(websites).into_response(),
        Err(e) => {
            tracing::error!("Failed to query websites: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
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
    Extension(app_node): Extension<LoresWebsiteNode>,
    Json(payload): Json<CreateWebsiteData>,
) -> impl IntoResponse {
    let id = uuid::Uuid::new_v4().to_string();

    match app_node
        .publish(&AppOperation::WebsiteCreatedV1(WebsiteCreatedDataV1 {
            id: id.clone(),
            name: payload.name.clone(),
            description: payload.description.clone(),
        }))
        .await
    {
        Ok(()) => {
            let website = Website {
                id,
                name: payload.name,
                description: payload.description,
            };
            (StatusCode::CREATED, Json(website)).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to publish operation: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
