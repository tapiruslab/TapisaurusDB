//! Embedded TapirusDB / TapisaurusDB Studio Web UI (`GET /studio/`).
//!
//! Serves the zero-dependency Studio v2 static bundle directly from `tapisaurus serve`
//! on the same origin (`:27018`) so administrators and developers get an instant
//! database workbench with zero CORS setup or external containers.

use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Redirect},
};

const STUDIO_HTML: &str = include_str!("../../assets/studio/index.html");
const STUDIO_CSS: &str = include_str!("../../assets/studio/workspace.css");
const STUDIO_JS: &str = include_str!("../../assets/studio/workspace.js");
const STUDIO_FAVICON: &str = include_str!("../../assets/studio/favicon.svg");

pub async fn studio_redirect() -> impl IntoResponse {
    Redirect::permanent("/studio/")
}

pub async fn studio_index() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        STUDIO_HTML,
    )
}

pub async fn studio_css() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STUDIO_CSS,
    )
}

pub async fn studio_js() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        STUDIO_JS,
    )
}

pub async fn studio_favicon() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "image/svg+xml")],
        STUDIO_FAVICON,
    )
}
