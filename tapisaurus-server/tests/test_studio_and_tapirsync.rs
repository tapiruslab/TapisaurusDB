//! Integration tests for embedded Tapirus Studio (`/studio/`), CORS defaults,
//! collection listing (`GET /v1/collections`), and Studio TapirSync push/pull.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

use tapisaurus_server::api::{create_router, AppState};

fn setup_app() -> (axum::Router, String) {
    let db = Arc::new(tapisaurus_query::DatabaseContext::new());
    let auth = Arc::new(tapisaurus_security::auth::AuthManager::new(
        b"test-secret-key-1234567890123456",
    ));
    let geo = Arc::new(tapisaurus_core::cluster::GeoReplicationEngine::new(
        "test-region".to_string(),
    ));
    let token = auth
        .generate_token("admin", tapisaurus_security::auth::Role::Admin, 3600)
        .unwrap();

    let state = Arc::new(AppState {
        db,
        auth,
        user_store: Arc::new(tapisaurus_security::UserStore::new()),
        backup_schedule: Arc::new(parking_lot::RwLock::new(Default::default())),
        geo_replication: geo,
        metrics: Arc::new(Default::default()),
    });

    (create_router(state), token)
}

#[tokio::test]
async fn test_embedded_studio_routes_and_cors() {
    let (app, _token) = setup_app();

    let res = app
        .clone()
        .oneshot(Request::builder().uri("/studio/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let html = String::from_utf8(
        axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains(r#"<meta name="tapirus-mode" content="server">"#));
    assert!(html.contains("TapirusDB Studio"));

    let css = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/studio/workspace.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(css.status(), StatusCode::OK);

    let js = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/studio/workspace.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(js.status(), StatusCode::OK);

    // Verify default CORS allows https://tapirusdb.com
    let cors = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/v1/health")
                .header(header::ORIGIN, "https://tapirusdb.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cors.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        cors.headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|v| v.to_str().ok()),
        Some("https://tapirusdb.com")
    );
}

#[tokio::test]
async fn test_studio_tapirsync_roundtrip_and_collections_list() {
    let (app, token) = setup_app();

    // 1. Push local .tapir tables, graph, and memories from Studio
    let push_body = json!({
        "source_node_id": "studio-test-vault",
        "tables": [
            {
                "table": "customers",
                "rows": [
                    { "id": 1, "name": "Acme Robotics", "region": "APAC" },
                    { "id": 2, "name": "Helios Health", "region": "EU" }
                ]
            }
        ],
        "graph_nodes": [
            { "id": "1", "label": "Service", "properties": { "name": "AuthGateway" } }
        ],
        "graph_edges": [],
        "memories": [
            { "memory_id": 1, "content": "XChaCha20 vault policy", "importance": 0.9, "tags": ["security"], "namespace": "policies" }
        ]
    });

    let push_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/tapirsync/sync-tables")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(push_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(push_res.status(), StatusCode::OK);
    let push_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(push_res.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(push_json["success"], true);
    assert_eq!(push_json["data"]["accepted"], true);
    assert!(push_json["data"]["records_applied"].as_u64().unwrap_or(0) >= 4);

    // 2. Verify GET /v1/collections lists synced customers collection
    let cols_res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/collections")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cols_res.status(), StatusCode::OK);
    let cols_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(cols_res.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let cols = cols_json["data"].as_array().unwrap();
    assert!(cols.iter().any(|c| c["name"] == "customers"));

    // 3. Pull TapirSync bundle back to Studio
    let pull_res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/tapirsync/pull")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pull_res.status(), StatusCode::OK);
    let pull_json: Value = serde_json::from_slice(
        &axum::body::to_bytes(pull_res.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(pull_json["success"], true);
    assert!(!pull_json["data"]["records"].as_array().unwrap().is_empty());
}
