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
use std::collections::BTreeMap;

use super::{ApiResponse, AppState};

#[derive(Debug, Default, Deserialize)]
pub struct TapirSyncPullRequest {
    #[serde(default)]
    pub collections: Option<Vec<String>>,
    #[serde(default)]
    pub format: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct StudioSyncTable {
    pub table: String,
    #[serde(default)]
    pub rows: Vec<serde_json::Value>,
}

#[derive(Debug, Default, Deserialize)]
pub struct StudioSyncNode {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub properties: serde_json::Value,
}

#[derive(Debug, Default, Deserialize)]
pub struct StudioSyncEdge {
    pub id: String,
    pub from_id: String,
    pub to_id: String,
    pub relation: String,
    #[serde(default = "default_edge_weight")]
    pub weight: f32,
    #[serde(default)]
    pub properties: serde_json::Value,
}

fn default_edge_weight() -> f32 {
    1.0
}

#[derive(Debug, Default, Deserialize)]
pub struct StudioSyncMemory {
    pub memory_id: u64,
    pub content: String,
    #[serde(default = "default_mem_importance")]
    pub importance: f32,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

fn default_mem_importance() -> f32 {
    0.5
}

#[derive(Debug, Default, Deserialize)]
pub struct StudioSyncRequest {
    #[serde(default)]
    pub source_node_id: Option<String>,
    #[serde(default)]
    pub tables: Vec<StudioSyncTable>,
    #[serde(default)]
    pub graph_nodes: Vec<StudioSyncNode>,
    #[serde(default)]
    pub graph_edges: Vec<StudioSyncEdge>,
    #[serde(default)]
    pub memories: Vec<StudioSyncMemory>,
}

/// POST /v1/tapirsync/sync-tables
///
/// Accepts structured tables, graph nodes/edges, and AI memories from Tapirus Studio
/// and seals them into a CRC32-verified `TapirSyncBundle` applied atomically via TapirSync.
pub async fn tapirsync_sync_tables(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StudioSyncRequest>,
) -> (StatusCode, Json<ApiResponse<tapisaurus_core::TapirSyncAck>>) {
    let now_ms = chrono::Utc::now().timestamp_millis().max(1) as u64;
    let node_id = req
        .source_node_id
        .unwrap_or_else(|| "tapirus-studio-browser".to_string());
    let mut hlc = tapisaurus_core::TapirSyncHlc::new(node_id.clone(), now_ms);
    let mut records = Vec::new();

    for tbl in req.tables {
        for (idx, row_val) in tbl.rows.into_iter().enumerate() {
            if let Some(obj) = row_val.as_object() {
                let row_id = obj
                    .get("id")
                    .or_else(|| obj.get("_id"))
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_else(|| format!("row_{}", idx + 1));
                let mut cols = BTreeMap::new();
                for (k, v) in obj {
                    cols.insert(k.clone(), v.clone());
                }
                records.push(tapisaurus_core::TapirSyncRecord::TableRowUpsert {
                    table: tbl.table.clone(),
                    row_id,
                    columns: cols,
                    hlc: hlc.tick(now_ms),
                });
            }
        }
    }

    for n in req.graph_nodes {
        records.push(tapisaurus_core::TapirSyncRecord::GraphVertexUpsert {
            vertex_id: n.id,
            label: n.label,
            properties: n.properties,
            vector: None,
            hlc: hlc.tick(now_ms),
        });
    }

    for e in req.graph_edges {
        records.push(tapisaurus_core::TapirSyncRecord::GraphEdgeUpsert {
            edge_id: e.id,
            from_id: e.from_id,
            to_id: e.to_id,
            relation: e.relation,
            weight: e.weight,
            properties: e.properties,
            hlc: hlc.tick(now_ms),
        });
    }

    let now_secs = now_ms / 1000;
    for m in req.memories {
        records.push(tapisaurus_core::TapirSyncRecord::AgentMemoryUpsert {
            memory_id: m.memory_id,
            content: m.content,
            vector: None,
            importance: m.importance,
            tags: m.tags,
            namespace: m.namespace,
            session_id: m.session_id,
            created_at: now_secs,
            hlc: hlc.tick(now_ms),
        });
    }

    let target_seq =
        state.db.tapirsync_registry().status_summary().server_export_sequence + records.len() as u64;
    let bundle = tapisaurus_core::TapirSyncBundle::new(
        node_id,
        0,
        target_seq,
        hlc,
        records,
        None,
    );

    match state.db.tapirsync_apply_bundle(&bundle) {
        Ok(ack) => (StatusCode::OK, Json(ApiResponse::ok(ack))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::err(format!("Failed to apply Studio TapirSync: {e}"))),
        ),
    }
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
