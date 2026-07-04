use axum::routing::get;
use lores_p2panda_client::PandaClient;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

use crate::static_server::frontend_handler;

mod node;
mod operations;
mod public_api;
mod realtime;
mod static_server;

const PANDA_GRPC_ADDR_DEFAULT: &str = "http://127.0.0.1:50051";
const APP_NAMESPACE: &str = "static-sites:v1";

#[derive(Clone)]
pub struct AppState {
    pub panda: Arc<Mutex<PandaClient>>,
    pub channels: Arc<Mutex<HashMap<[u8; 32], broadcast::Sender<Vec<u8>>>>>,
    pub app_namespace: String,
    pub websites: Arc<Mutex<Vec<public_api::websites::Website>>>,
}

#[tokio::main]
async fn main() {
    #[derive(OpenApi)]
    #[openapi()]
    struct ApiDoc;

    let panda_grpc_addr =
        std::env::var("PANDA_GRPC_ADDR").unwrap_or_else(|_| PANDA_GRPC_ADDR_DEFAULT.to_string());

    let panda = PandaClient::connect_lazy(panda_grpc_addr)
        .expect("failed to connect to panda gRPC endpoint");
    let panda = Arc::new(Mutex::new(panda));

    let state = AppState {
        panda: panda.clone(),
        channels: Arc::new(Mutex::new(HashMap::new())),
        app_namespace: APP_NAMESPACE.to_string(),
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
        .layer(axum::Extension(node::AppNode::new(
            [0u8; 32],
            APP_NAMESPACE,
            panda,
        )));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("backend listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind backend listener");

    axum::serve(listener, app)
        .await
        .expect("backend server error");
}
