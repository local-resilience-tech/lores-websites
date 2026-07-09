use axum::routing::get;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

use crate::static_server::frontend_handler;

mod events;
mod public_api;
mod realtime;
mod static_server;

const PANDA_GRPC_ADDR_ENV: &str = "PANDA_GRPC_ADDR";
const PANDA_GRPC_ADDR_DEFAULT: &str = "http://127.0.0.1:50051";

const APP_ID_ENV: &str = "LORES_APP_ID";
const APP_ID_DEFAULT: &str = "lores-websites";

const INSTANCE_ID_ENV: &str = "LORES_INSTANCE_ID";
const INSTANCE_ID_DEFAULT: &str = "default";

#[derive(Clone)]
pub struct AppState {
    pub websites: Arc<Mutex<Vec<public_api::websites::Website>>>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    #[derive(OpenApi)]
    #[openapi()]
    struct ApiDoc;

    let panda_grpc_addr =
        std::env::var(PANDA_GRPC_ADDR_ENV).unwrap_or_else(|_| PANDA_GRPC_ADDR_DEFAULT.to_string());
    let app_id = std::env::var(APP_ID_ENV).unwrap_or_else(|_| APP_ID_DEFAULT.to_string());
    let instance_id =
        std::env::var(INSTANCE_ID_ENV).unwrap_or_else(|_| INSTANCE_ID_DEFAULT.to_string());

    let node = lores_websites_node::connect(panda_grpc_addr, &app_id, &instance_id);

    let state = AppState {
        websites: Arc::new(Mutex::new(vec![
            public_api::websites::Website {
                name: "Example Site".to_string(),
                description: "A static site hosted on Lores.".to_string(),
            },
            public_api::websites::Website {
                name: "My Blog".to_string(),
                description: "Personal blog built with a static generator.".to_string(),
            },
            public_api::websites::Website {
                name: "Portfolio".to_string(),
                description: "Design and development portfolio.".to_string(),
            },
        ])),
    };

    events::register_event_handlers(&node, state.clone());

    let (api_router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .nest("/public_api", public_api::router())
        .split_for_parts();

    // Write openapi.json to disk
    let openapi_json = api
        .to_pretty_json()
        .expect("failed to serialize OpenAPI spec");
    std::fs::write("openapi.json", &openapi_json).expect("failed to write openapi.json");

    let app = api_router
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api.clone()))
        .route("/ws/{region_id}", get(realtime::handler))
        .fallback_service(get(frontend_handler))
        .layer(axum::Extension(state))
        .layer(axum::Extension(node));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("backend listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind backend listener");

    axum::serve(listener, app)
        .await
        .expect("backend server error");
}
