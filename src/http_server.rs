use crate::config::MemoryConfig;
use crate::store::Store;
use crate::sync::{
    apply_sync_envelope, build_status, export_dirty_envelope, export_sync_envelope,
    remote_export_envelope, remote_import_envelope, SyncTransferEnvelope,
};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

// ── HTTP state ──────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Mutex<Store>>,
    pub config: MemoryConfig,
    pub auth_token: String,
}

impl AppState {
    fn check_auth(&self, headers: &HeaderMap) -> Result<(), (StatusCode, Json<ApiResponse>)> {
        let token = headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.strip_prefix("Bearer ").unwrap_or(v))
            .unwrap_or("");

        if token != self.auth_token {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some("Invalid or missing auth token".into()),
                }),
            ));
        }
        Ok(())
    }

    /// Acquire the store guard (always exclusive — `std::sync::Mutex`),
    /// returning HTTP 500 on mutex poison.
    fn lock_store(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Store>, (StatusCode, Json<ApiResponse>)> {
        self.store.lock().map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some("Internal server error".into()),
                }),
            )
        })
    }
}

// ── Request/Response types ──────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    pub category: String,
    pub fields: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WriteBody {
    pub category: String,
    pub data: serde_json::Value,
    pub reason: Option<String>,
    pub expected_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub query: String,
    pub limit: Option<u32>,
    pub category: Option<String>,
    pub updated_after: Option<String>,
    pub updated_before: Option<String>,
    pub actor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteBody {
    pub category: String,
}

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    pub category: String,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub filter: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SyncExportBody {
    pub categories: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SyncImportBody {
    pub envelope: SyncTransferEnvelope,
    pub conflict_strategy: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SyncPullRemoteBody {
    pub categories: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ── Routes ──────────────────────────────────────────────────────

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handle_health))
        .route("/api/v1/read", get(handle_read))
        .route("/api/v1/write", post(handle_write))
        .route("/api/v1/search", get(handle_search))
        .route("/api/v1/list", get(handle_list))
        .route("/api/v1/delete", post(handle_delete))
        .route("/api/v1/history", get(handle_history))
        .route("/api/v1/stats", get(handle_stats))
        .route("/api/v1/status", get(handle_status))
        .route("/api/v1/doctor", get(handle_doctor))
        .route("/api/v1/manifest", get(handle_manifest))
        .route("/api/v1/sync/export", post(handle_sync_export))
        .route("/api/v1/sync/import", post(handle_sync_import))
        .route("/api/v1/sync/push-dirty", post(handle_sync_push_dirty))
        .route("/api/v1/sync/pull-remote", post(handle_sync_pull_remote))
        .with_state(state)
}

// ── Handlers ────────────────────────────────────────────────────

async fn handle_health() -> Json<ApiResponse> {
    Json(ApiResponse {
        success: true,
        data: Some(serde_json::json!({
            "service": "mcp-memory-rs",
            "version": env!("CARGO_PKG_VERSION"),
            "status": "ok",
        })),
        error: None,
    })
}

async fn handle_read(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<ReadQuery>,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let store = state.lock_store()?;
    match store.read(&params.category) {
        Ok(value) => {
            let result = if let Some(fields) = &params.fields {
                filter_fields_json(&value, fields)
            } else {
                value
            };

            let meta = store.get_meta(&params.category).ok().flatten();
            let mut data = serde_json::to_value(&result).unwrap_or(result);

            if let (Some(obj), Some(m)) = (data.as_object_mut(), meta) {
                obj.insert("success".into(), serde_json::json!(true));
                obj.insert("category".into(), serde_json::json!(params.category));
                obj.insert(
                    "_meta".into(),
                    serde_json::json!({
                        "hash": m.content_hash,
                        "updated_at": m.updated_at,
                        "updated_by": m.updated_by,
                        "size_bytes": m.size_bytes,
                    }),
                );
            }

            Ok(Json(ApiResponse {
                success: true,
                data: Some(data),
                error: None,
            }))
        }
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_write(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<WriteBody>,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let mut store = state.lock_store()?;
    let actor = state.config.acl.device_name.clone();

    match store.write(
        &body.category,
        &body.data,
        body.reason.as_deref(),
        &actor,
        body.expected_hash.as_deref(),
    ) {
        Ok(meta) => Ok(Json(ApiResponse {
            success: true,
            data: Some(serde_json::json!({
                "category": meta.name,
                "hash": meta.content_hash,
                "updated_at": meta.updated_at,
                "size_bytes": meta.size_bytes,
            })),
            error: None,
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<SearchQuery>,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let store = state.lock_store()?;
    let limit = params.limit.unwrap_or(20);

    match store.search_advanced(
        &params.query,
        params.category.as_deref(),
        params.updated_after.as_deref(),
        params.updated_before.as_deref(),
        params.actor.as_deref(),
        limit,
    ) {
        Ok(results) => Ok(Json(ApiResponse {
            success: true,
            data: Some(serde_json::json!({
                "query": params.query,
                "total": results.len(),
                "results": results,
            })),
            error: None,
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_list(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let store = state.lock_store()?;

    match (store.list(), store.stats()) {
        (Ok(categories), Ok(stats)) => Ok(Json(ApiResponse {
            success: true,
            data: Some(serde_json::json!({
                "summary": stats,
                "files": categories,
            })),
            error: None,
        })),
        (Err(e), _) | (_, Err(e)) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<DeleteBody>,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let mut store = state.lock_store()?;
    let actor = state.config.acl.device_name.clone();

    match store.delete(&body.category, &actor) {
        Ok(()) => Ok(Json(ApiResponse {
            success: true,
            data: Some(serde_json::json!({ "deleted": body.category })),
            error: None,
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_history(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HistoryQuery>,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let store = state.lock_store()?;
    let limit = params.limit.unwrap_or(20);

    match store.history(&params.category, limit) {
        Ok(versions) => Ok(Json(ApiResponse {
            success: true,
            data: Some(serde_json::json!({
                "category": params.category,
                "versions": versions,
            })),
            error: None,
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let store = state.lock_store()?;
    match store.stats() {
        Ok(stats) => Ok(Json(ApiResponse {
            success: true,
            data: Some(stats),
            error: None,
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let store = state.lock_store()?;
    match build_status(&store, &state.config) {
        Ok(status) => Ok(Json(ApiResponse {
            success: true,
            data: Some(serde_json::to_value(status).unwrap_or(serde_json::json!({}))),
            error: None,
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_doctor(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let store = state.lock_store()?;
    let db_exists = state.config.db_path().exists();
    let categories_dir_exists = state.config.categories_dir().exists();
    let backups_dir_exists = state.config.backups_dir().exists();
    let stats = store.stats().ok();
    let remote_configured = !state.config.sync.remote_url.trim().is_empty();
    Ok(Json(ApiResponse {
        success: true,
        data: Some(serde_json::json!({
            "device": state.config.acl.device_name,
            "base_dir": state.config.resolved_base_dir(),
            "db_exists": db_exists,
            "categories_dir_exists": categories_dir_exists,
            "backups_dir_exists": backups_dir_exists,
            "remote_configured": remote_configured,
            "remote_url": state.config.sync.remote_url,
            "stats": stats,
        })),
        error: None,
    }))
}

async fn handle_manifest(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let store = state.lock_store()?;
    let device = state.config.acl.device_name.clone();

    match crate::sync::build_manifest(&store, &device) {
        Ok(manifest) => Ok(Json(ApiResponse {
            success: true,
            data: Some(serde_json::to_value(manifest).unwrap_or(serde_json::json!({}))),
            error: None,
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

// ── Sync handlers ───────────────────────────────────────────────

async fn handle_sync_export(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SyncExportBody>,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let store = state.lock_store()?;
    let device = state.config.acl.device_name.clone();
    let categories = if body.categories.is_empty() {
        match store.list() {
            Ok(items) => items.into_iter().map(|item| item.name).collect::<Vec<_>>(),
            Err(e) => {
                return Ok(Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                }));
            }
        }
    } else {
        body.categories
    };

    match export_sync_envelope(&store, &categories, &device) {
        Ok(envelope) => Ok(Json(ApiResponse {
            success: true,
            data: Some(serde_json::to_value(envelope).unwrap_or(serde_json::json!({}))),
            error: None,
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_sync_import(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SyncImportBody>,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let mut store = state.lock_store()?;
    let device = state.config.acl.device_name.clone();
    let strategy = body
        .conflict_strategy
        .as_deref()
        .unwrap_or(&state.config.sync.conflict_strategy);

    match apply_sync_envelope(&mut store, &body.envelope, strategy, &device) {
        Ok(result) => Ok(Json(ApiResponse {
            success: true,
            data: Some(serde_json::to_value(result).unwrap_or(serde_json::json!({}))),
            error: None,
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_sync_push_dirty(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let (envelope, dirty_names) = {
        let store = state.lock_store()?;
        let envelope = match export_dirty_envelope(&store, &state.config.acl.device_name) {
            Ok(envelope) => envelope,
            Err(e) => {
                return Ok(Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                }));
            }
        };
        let dirty_names = store
            .dirty_entries()
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| entry.category_name)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        (envelope, dirty_names)
    };

    if envelope.categories.is_empty() && envelope.deleted.is_empty() {
        return Ok(Json(ApiResponse {
            success: true,
            data: Some(serde_json::json!({
                "pushed": [],
                "pulled": [],
                "conflicts": [],
                "errors": [],
            })),
            error: None,
        }));
    }

    match remote_import_envelope(&state.config, &envelope).await {
        Ok(mut result) => Ok(Json(ApiResponse {
            success: true,
            data: {
                if result.errors.is_empty() {
                    let store = state.lock_store()?;
                    let _ = store.clear_dirty(dirty_names.iter().map(String::as_str));
                    let _ = store.set_sync_state(
                        "last_sync",
                        &serde_json::json!({
                            "direction": "push",
                            "remote": state.config.sync.remote_url,
                            "timestamp": chrono::Utc::now().to_rfc3339(),
                            "pushed": dirty_names.len(),
                            "conflicts": result.conflicts.len(),
                            "errors": result.errors.len(),
                        }),
                    );
                }
                result.pushed = dirty_names;
                Some(serde_json::to_value(result).unwrap_or(serde_json::json!({})))
            },
            error: None,
        })),
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_sync_pull_remote(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SyncPullRemoteBody>,
) -> Result<Json<ApiResponse>, (StatusCode, Json<ApiResponse>)> {
    state.check_auth(&headers)?;
    let categories = body.categories.unwrap_or_default();
    match remote_export_envelope(&state.config, &categories).await {
        Ok(envelope) => {
            let mut store = state.lock_store()?;
            let result = apply_sync_envelope(
                &mut store,
                &envelope,
                &state.config.sync.conflict_strategy,
                &state.config.acl.device_name,
            );
            match result {
                Ok(result) => {
                    let _ = store.set_sync_state(
                        "last_sync",
                        &serde_json::json!({
                            "direction": "pull",
                            "remote": state.config.sync.remote_url,
                            "timestamp": chrono::Utc::now().to_rfc3339(),
                            "pulled": result.pulled.len(),
                            "conflicts": result.conflicts.len(),
                            "errors": result.errors.len(),
                        }),
                    );
                    Ok(Json(ApiResponse {
                        success: true,
                        data: Some(serde_json::to_value(result).unwrap_or(serde_json::json!({}))),
                        error: None,
                    }))
                }
                Err(e) => Ok(Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(e.to_string()),
                })),
            }
        }
        Err(e) => Ok(Json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
        })),
    }
}

// ── Helper ──────────────────────────────────────────────────────

fn filter_fields_json(value: &serde_json::Value, fields_spec: &str) -> serde_json::Value {
    let fields: Vec<&str> = fields_spec.split(',').map(|s| s.trim()).collect();
    if fields.is_empty() || fields_spec == "*" {
        return value.clone();
    }
    match value {
        serde_json::Value::Object(map) => {
            let mut filtered = serde_json::Map::new();
            for field in &fields {
                if let Some(v) = map.get(*field) {
                    filtered.insert((*field).into(), v.clone());
                }
            }
            serde_json::Value::Object(filtered)
        }
        _ => value.clone(),
    }
}

// ── Start server ────────────────────────────────────────────────

pub async fn start_http_server(config: MemoryConfig) -> anyhow::Result<()> {
    let store = Store::new(&config)?;
    let auth_token = std::env::var("MCP_MEMORY_TOKEN").map_err(|_| {
        anyhow::anyhow!(
            "MCP_MEMORY_TOKEN is not set. \
             The HTTP server requires an explicit auth token for security. \
             Set the environment variable before starting: \
             export MCP_MEMORY_TOKEN=<your-secret-token>"
        )
    })?;

    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port)
        .parse()
        .map_err(|e: std::net::AddrParseError| anyhow::anyhow!("Invalid address: {}", e))?;

    let state = AppState {
        store: Arc::new(Mutex::new(store)),
        config: config.clone(),
        auth_token,
    };

    let app = build_router(state);

    tracing::info!("HTTP server starting on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // for oneshot

    fn make_config(label: &str) -> MemoryConfig {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("mcp_http_test_{}_{}", label, id));
        let _ = std::fs::remove_dir_all(&dir);
        MemoryConfig {
            server: crate::config::ServerConfig {
                mode: "offline".into(),
                host: "127.0.0.1".into(),
                port: 3110,
            },
            storage: crate::config::StorageConfig {
                base_dir: dir.clone(),
                backup_retention_days: 30,
                max_versions: 100,
            },
            acl: crate::config::AclConfig {
                admin_devices: vec!["device-b".into()],
                device_name: "device-b".into(),
                device_categories: vec!["device-b".into()],
            },
            sync: crate::config::SyncConfig::default(),
        }
    }

    fn make_app(label: &str) -> (Router, std::path::PathBuf) {
        let config = make_config(label);
        let dir = config.resolved_base_dir();
        let store = Store::new(&config).unwrap();
        let state = AppState {
            store: Arc::new(Mutex::new(store)),
            config,
            auth_token: "test_token".into(),
        };
        (build_router(state), dir)
    }

    fn cleanup(dir: &std::path::PathBuf) {
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn start_http_server_fails_fast_without_token() {
        // No other test reads or writes MCP_MEMORY_TOKEN (they inject
        // auth_token into AppState directly), so this is race-free.
        std::env::remove_var("MCP_MEMORY_TOKEN");
        let config = make_config("failfast_no_token");
        let dir = config.resolved_base_dir();
        let err = start_http_server(config)
            .await
            .expect_err("HTTP server must refuse to start without MCP_MEMORY_TOKEN");
        assert!(err.to_string().contains("MCP_MEMORY_TOKEN"));
        cleanup(&dir);
    }

    #[tokio::test]
    async fn test_health_no_auth() {
        let (app, dir) = make_app("health");
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        cleanup(&dir);
    }

    #[tokio::test]
    async fn test_auth_required() {
        let (app, dir) = make_app("auth");
        let req = Request::builder()
            .uri("/api/v1/list")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        cleanup(&dir);
    }

    #[tokio::test]
    async fn test_sync_export_import_roundtrip() {
        let config_a = make_config("sync_a");
        let dir_a = config_a.resolved_base_dir();
        let mut store_a = Store::new(&config_a).unwrap();
        store_a
            .write(
                "synced_cat",
                &serde_json::json!({"v": 42}),
                None,
                "device_a",
                None,
            )
            .unwrap();

        // Export from A
        let envelope =
            crate::sync::export_sync_envelope(&store_a, &["synced_cat".to_string()], "device_a")
                .unwrap();
        assert_eq!(envelope.categories.len(), 1);
        assert_eq!(envelope.categories[0].name, "synced_cat");

        // Import into B
        let config_b = make_config("sync_b");
        let dir_b = config_b.resolved_base_dir();
        let mut store_b = Store::new(&config_b).unwrap();

        let result = crate::sync::apply_sync_envelope(
            &mut store_b,
            &envelope,
            "last_write_wins",
            "device_b",
        )
        .unwrap();
        assert_eq!(result.pulled, vec!["synced_cat"]);

        let data_b = store_b.read("synced_cat").unwrap();
        assert_eq!(data_b["v"], 42);
        let meta_b = store_b.get_meta("synced_cat").unwrap().unwrap();
        assert_eq!(meta_b.updated_by, "device_a");

        cleanup(&dir_a);
        cleanup(&dir_b);
    }

    #[tokio::test]
    async fn test_manifest_endpoint() {
        let config = make_config("manifest");
        let dir = config.resolved_base_dir();
        let mut store = Store::new(&config).unwrap();
        store
            .write("cat1", &serde_json::json!({"x": 1}), None, "tester", None)
            .unwrap();

        let manifest = crate::sync::build_manifest(&store, "device-b").unwrap();
        assert_eq!(manifest.categories.len(), 1);
        assert_eq!(manifest.categories[0].name, "cat1");
        assert!(!manifest.merkle_root.is_empty());

        cleanup(&dir);
    }

    #[tokio::test]
    async fn test_stats_endpoint() {
        let config = make_config("stats");
        let dir = config.resolved_base_dir();
        let mut store = Store::new(&config).unwrap();
        store
            .write(
                "stat_cat",
                &serde_json::json!({"n": 1}),
                None,
                "tester",
                None,
            )
            .unwrap();

        let stats = store.stats().unwrap();
        assert!(stats["categories"].as_i64().unwrap() >= 1);

        cleanup(&dir);
    }
}
