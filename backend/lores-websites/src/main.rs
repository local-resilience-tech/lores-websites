use axum::routing::get;
use sqlx::SqlitePool;
use std::net::SocketAddr;
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

const DATA_DIR_ENV: &str = "DATA_DIR";
const DATA_DIR_DEFAULT: &str = "../data";

#[derive(Clone)]
pub struct AppState {
    pub db: SqlitePool,
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

    let data_dir = std::env::var(DATA_DIR_ENV).unwrap_or_else(|_| DATA_DIR_DEFAULT.to_string());

    let (db, should_replay) = lores_websites_node::create_projection_db()
        .await
        .expect("failed to create projection database");

    let operations_db_path = format!("{data_dir}/operations.sqlite");
    let operations_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect(&format!("sqlite://{operations_db_path}?mode=rwc"))
        .await
        .expect("failed to open operations database");

    let node =
        lores_websites_node::connect(operations_pool, panda_grpc_addr, &app_id, &instance_id)
            .await
            .expect("failed to connect node");

    let state = AppState { db };

    events::register_event_handlers(&node, state.clone());

    if should_replay {
        node.replay().await;
    }

    let run_node = node.clone();
    tokio::spawn(async move { run_node.run().await });

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
