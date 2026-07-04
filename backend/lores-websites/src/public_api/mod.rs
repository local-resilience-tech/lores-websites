pub mod websites;

use utoipa_axum::router::OpenApiRouter;

pub fn router() -> OpenApiRouter {
    OpenApiRouter::new().nest("/websites", websites::router())
}
