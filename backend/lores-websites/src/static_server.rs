use std::env;

use axum::{
    body::Body,
    http::{header::ETAG, HeaderValue, Request, Response, StatusCode, Uri},
};
use lazy_static::lazy_static;
use tower::ServiceExt;
use tower_http::services::{ServeDir, ServeFile};

const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

lazy_static! {
    pub static ref FRONTEND_PATH: String =
        env::var("FRONTEND_PATH").unwrap_or_else(|_| "../frontend/dist".to_string());
}

pub async fn frontend_handler(uri: Uri) -> Result<Response<Body>, (StatusCode, String)> {
    if uri.path().starts_with("/api") {
        tracing::warn!(
            "API call detected, not serving static files for URI: {}",
            uri
        );
        return Err((StatusCode::NOT_FOUND, "API endpoint not found".to_string()));
    }

    let res = serve_dir(uri.clone(), FRONTEND_PATH.clone()).await?;

    match res.status() {
        StatusCode::OK => {
            let mut res = res;
            apply_etag_header(&mut res);
            Ok(res)
        }
        StatusCode::NOT_FOUND => {
            // serve the HTML file for SPA routing
            let spa_file_path_string = FRONTEND_PATH.clone() + "/index.html";
            tracing::info!(
                "Serving SPA index.html for URI: {}, using path {}",
                uri.clone(),
                spa_file_path_string
            );
            let mut response = serve_file(uri.clone(), spa_file_path_string).await?;
            apply_etag_header(&mut response);
            Ok(response)
        }
        other => {
            // If the status is not OK or NOT_FOUND, return the response as is
            tracing::warn!("Got other status: {}", other);
            return Ok(res);
        }
    }
}

async fn serve_file(
    uri: Uri,
    spa_file_path_string: String,
) -> Result<Response<Body>, (StatusCode, String)> {
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();

    match ServeFile::new(spa_file_path_string).oneshot(req).await {
        Ok(res) => Ok(res.map(|b| Body::new(b))),
        Err(err) => {
            tracing::error!("Error serving static file: {}", err);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Something went wrong: {}", err),
            ));
        }
    }
}

async fn serve_dir(uri: Uri, static_dir: String) -> Result<Response<Body>, (StatusCode, String)> {
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();

    // `ServeDir` implements `tower::Service` so we can call it with `tower::ServiceExt::oneshot`
    match ServeDir::new(static_dir).oneshot(req).await {
        Ok(res) => Ok(res.map(|b| Body::new(b))),
        Err(err) => {
            tracing::error!("Error serving static file: {}", err);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Something went wrong: {}", err),
            ));
        }
    }
}

fn apply_etag_header(response: &mut Response<Body>) {
    response
        .headers_mut()
        .insert(ETAG, HeaderValue::from_str(&app_version_etag()).unwrap());
}

fn app_version_etag() -> String {
    format!("\"lores-static-sites-{}\"", APP_VERSION)
}
