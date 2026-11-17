//! TapirSync (`TPSYNC01`) Edge-to-Cloud Replication API Handlers.
//!
//! Bridges single-file **TapirusDB (`.tapir`)** edge databases with **TapisaurusDB**
//! server clusters over HTTP/REST and `TPSYNC01` binary wire frames.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use super::{ApiResponse, AppState};

#[derive(Debug, Default, Deserialize)]
pub struct TapirSyncPullRequest {
    #[serde(default)]
    pub collections: Option<Vec<String>>,
    #[serde(default)]
    pub format: Option<String>,
}

/// GET /v1/tapirsync/status
pub async fn tapirsync_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let summary = state.db.tapirsync_registry().status_summary();
    Json(ApiResponse::ok(summary))
}

/// POST /v1/tapirsync/push
///
/// Accepts either a raw `TPSYNC01` binary wire frame (`application/octet-stream`)
/// or a JSON-encoded `TapirSyncBundle` (`application/json`).
pub async fn tapirsync_push(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> (StatusCode, Json<ApiResponse<tapisaurus_core::TapirSyncAck>>) {
    let bundle = match tapisaurus_core::TapirSyncBundle::from_bytes(&body) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::err(format!("Invalid TapirSync bundle: {e}"))),
            );
        }
    };

    match state.db.tapirsync_apply_bundle(&bundle) {
        Ok(ack) => (StatusCode::OK, Json(ApiResponse::ok(ack))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::err(format!(
                "Failed to apply TapirSync bundle: {e}"
            ))),
        ),
    }
}

/// POST /v1/tapirsync/pull
///
/// Exports a `TapirSyncBundle` from `TapisaurusDB` for edge `.tapir` synchronization.
/// When `{"format": "binary"}` is requested, responds with raw `TPSYNC01` wire bytes.
pub async fn tapirsync_pull(State(state): State<Arc<AppState>>, body: Bytes) -> Response {
    let req: TapirSyncPullRequest = if body.is_empty() {
        TapirSyncPullRequest::default()
    } else {
        serde_json::from_slice(&body).unwrap_or_default()
    };

    let bundle = match state.db.tapirsync_export_bundle(req.collections.as_deref()) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<()>::err(format!(
                    "Failed to export TapirSync bundle: {e}"
                ))),
            )
                .into_response();
        }
    };

    if req
        .format
        .as_deref()
        .is_some_and(|f| f.eq_ignore_ascii_case("binary"))
    {
        match bundle.to_bytes() {
            Ok(bytes) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/octet-stream")],
                bytes,
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<()>::err(format!(
                    "Failed to encode TPSYNC01 wire frame: {e}"
                ))),
            )
                .into_response(),
        }
    } else {
        (StatusCode::OK, Json(ApiResponse::ok(bundle))).into_response()
    }
}
