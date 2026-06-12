use std::sync::{Arc, Mutex};

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler};
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::Deserialize;
use std::borrow::Cow;

use crate::acl;
use crate::backup;
use crate::config::MemoryConfig;
use crate::embeddings;
use crate::store::Store;
use crate::sync;

// ── Tool parameter types ────────────────────────────────────────

// EmptyParams uses a hand-written JsonSchema impl: schemars 1.2 derives
// `{"type":"object","title":"EmptyParams"}` for empty structs (no `properties`
// key). Anthropic's tool-schema validator rejects the entire tools/list
// response when any tool has such a schema, silently dropping all tools of
// the server. Emitting an explicit `properties: {}` keeps the schema valid.
#[derive(Debug, Default, Deserialize)]
pub struct EmptyParams {}

impl JsonSchema for EmptyParams {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("EmptyParams")
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReadParams {
    /// Name of the memory category to read (the parameter is 'category', NOT
    /// 'key'). Use memory_list to discover valid names.
    pub category: String,
    pub fields: Option<String>,
}

// `data` uses `serde_json::Map<String, Value>` (not bare `Value`): schemars
// 1.2 renders `serde_json::Value` as the boolean schema `true` ("any value
// allowed"), which Anthropic's tool-schema validator rejects, dropping the
// entire tools/list response. `Map<String, Value>` still emits
// `additionalProperties: true`, so ObjectPayload keeps the JSON object
// semantics while forcing the schema to remain object-shaped throughout.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub struct ObjectPayload(pub serde_json::Map<String, serde_json::Value>);

impl JsonSchema for ObjectPayload {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("ObjectPayload")
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "object",
            "properties": {},
            "additionalProperties": {}
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct WriteParams {
    pub category: String,
    pub data: ObjectPayload,
    pub reason: Option<String>,
    pub expected_hash: Option<String>,
    pub merge: Option<bool>,
}

impl JsonSchema for WriteParams {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("WriteParams")
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "object",
            "properties": {
                "category": { "type": "string" },
                "data": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": {}
                },
                "reason": { "type": "string" },
                "expected_hash": { "type": "string" },
                "merge": { "type": "boolean" }
            },
            "required": ["category", "data"],
            "additionalProperties": false
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchParams {
    pub query: String,
    pub limit: Option<u32>,
    /// Filter by category name
    pub category: Option<String>,
    /// Only results updated after this ISO datetime
    pub updated_after: Option<String>,
    /// Only results updated before this ISO datetime
    pub updated_before: Option<String>,
    /// Filter by actor/device that last updated
    pub actor: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteParams {
    pub category: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct HistoryParams {
    pub category: String,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ContextParams {
    pub categories: Vec<String>,
    pub fields: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeltaParams {
    pub category: String,
    pub since_hash: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CompactParams {
    pub category: Option<String>,
    pub keep_versions: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SyncManifestParams {
    /// Export manifest to file path (optional, returns inline if omitted)
    pub export_path: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SyncPushParams {
    /// Path to export push data to
    pub export_path: String,
    /// Specific categories to push (all if omitted)
    pub categories: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SyncPullParams {
    /// Path to import pull data from
    pub import_path: String,
    /// Conflict resolution strategy: last_write_wins, local_wins, remote_wins
    pub conflict_strategy: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SyncDiffParams {
    /// Path to remote manifest file for comparison
    pub remote_manifest_path: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct MemorySyncParams {
    /// Direction: push_dirty or pull_remote
    pub direction: String,
    /// Categories for pull_remote. If omitted, pulls categories already known locally.
    pub categories: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SemanticSearchParams {
    pub query: String,
    pub limit: Option<u32>,
    /// Weight for semantic score (0.0-1.0, default 0.5). 1.0 = pure semantic, 0.0 = pure keyword.
    pub semantic_weight: Option<f64>,
}

// ── Server ──────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MemoryServer {
    store: Arc<Mutex<Store>>,
    config: MemoryConfig,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl MemoryServer {
    pub async fn new() -> anyhow::Result<Self> {
        let config = MemoryConfig::try_load()?;
        Self::from_config(config)
    }

    pub fn from_config(config: MemoryConfig) -> anyhow::Result<Self> {
        let store = Store::new(&config)?;

        // Prune old backups on startup
        let _ = backup::prune_old_backups(&config);

        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            config,
            tool_router: Self::tool_router(),
        })
    }

    fn device_name(&self) -> String {
        self.config.acl.device_name.clone()
    }

    fn acl_context(&self) -> acl::AclContext {
        acl::get_context(&self.config)
    }

    fn lock_store(&self) -> Result<std::sync::MutexGuard<'_, Store>, McpError> {
        self.store
            .lock()
            .map_err(|e| McpError::internal_error(format!("Store lock: {}", e), None))
    }

    // ── Tool 1: memory_read ─────────────────────────────────────

    #[tool(
        description = "Read a memory category by name (parameter: 'category', NOT 'key'). Returns the category's JSON content plus metadata, with optional comma-separated field filtering. Use memory_list first to see valid category names.",
        annotations(read_only_hint = true)
    )]
    async fn memory_read(
        &self,
        params: Parameters<ReadParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let value = store
            .read(&params.0.category)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let result = if let Some(fields) = &params.0.fields {
            filter_fields(&value, fields)
        } else {
            value
        };

        let meta = store
            .get_meta(&params.0.category)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let mut response = serde_json::to_value(&result)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        if let Some(obj) = response.as_object_mut() {
            if let Some(m) = meta {
                obj.insert("success".into(), serde_json::json!(true));
                obj.insert("category".into(), serde_json::json!(params.0.category));
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
        }

        Ok(CallToolResult::success(vec![Content::json(&response)
            .map_err(|e| {
                McpError::internal_error(e.to_string(), None)
            })?]))
    }

    // ── Tool 2: memory_write ────────────────────────────────────

    #[tool(
        description = "Write to a memory category with automatic versioning and backup. By default `data` REPLACES the entire category content. Set `merge=true` for a top-level per-key patch: each key in `data` is inserted or overwritten, a JSON null value deletes that key, and untouched keys are preserved. Merge on a missing category behaves like a normal create. Pass `expected_hash` (from memory_read/memory_list) to avoid clobbering concurrent updates."
    )]
    async fn memory_write(
        &self,
        params: Parameters<WriteParams>,
    ) -> Result<CallToolResult, McpError> {
        // ACL check
        let ctx = self.acl_context();
        match acl::authorize_write(&params.0.category, &ctx) {
            acl::AclDecision::Denied(reason) => {
                return Ok(CallToolResult::success(vec![Content::json(
                    serde_json::json!({
                        "success": false,
                        "error": format!("Unauthorized write for category '{}'", params.0.category),
                        "reason": reason,
                        "device": ctx.device,
                    }),
                )
                .map_err(|e| McpError::internal_error(e.to_string(), None))?]));
            }
            acl::AclDecision::Allowed(_) => {}
        }

        let mut store = self.lock_store()?;
        let actor = self.device_name();

        // Get previous hash for compatibility
        let previous_hash = store
            .get_meta(&params.0.category)
            .ok()
            .flatten()
            .map(|m| m.content_hash);

        let data_value = if params.0.merge.unwrap_or(false) {
            let mut current = match store.read(&params.0.category) {
                Ok(serde_json::Value::Object(map)) => map,
                Ok(_) => serde_json::Map::new(),
                Err(crate::error::MemoryError::NotFound(_)) => serde_json::Map::new(),
                Err(e) => return Err(McpError::internal_error(e.to_string(), None)),
            };
            for (k, v) in params.0.data.0 {
                if v.is_null() {
                    current.remove(&k);
                } else {
                    current.insert(k, v);
                }
            }
            serde_json::Value::Object(current)
        } else {
            serde_json::Value::Object(params.0.data.0)
        };
        let meta = store
            .write(
                &params.0.category,
                &data_value,
                params.0.reason.as_deref(),
                &actor,
                params.0.expected_hash.as_deref(),
            )
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "category": meta.name,
                "previous_hash": previous_hash,
                "new_hash": meta.content_hash,
                "reason": params.0.reason,
                "timestamp": meta.updated_at,
                "backup_created": true,
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 3: memory_search ───────────────────────────────────

    #[tool(
        description = "Search across all memory categories with FTS5 full-text search. Supports BM25 ranking, category/date/actor filters, and snippet highlighting.",
        annotations(read_only_hint = true)
    )]
    async fn memory_search(
        &self,
        params: Parameters<SearchParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let limit = params.0.limit.unwrap_or(20);
        let results = store
            .search_advanced(
                &params.0.query,
                params.0.category.as_deref(),
                params.0.updated_after.as_deref(),
                params.0.updated_before.as_deref(),
                params.0.actor.as_deref(),
                limit,
            )
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let mut filters = serde_json::Map::new();
        if let Some(ref cat) = params.0.category {
            filters.insert("category".into(), serde_json::json!(cat));
        }
        if let Some(ref after) = params.0.updated_after {
            filters.insert("updated_after".into(), serde_json::json!(after));
        }
        if let Some(ref before) = params.0.updated_before {
            filters.insert("updated_before".into(), serde_json::json!(before));
        }
        if let Some(ref act) = params.0.actor {
            filters.insert("actor".into(), serde_json::json!(act));
        }

        let mut response = serde_json::to_value(serde_json::json!({
            "success": true,
            "query": params.0.query,
            "total_matches": results.len(),
            "returned": results.len(),
            "results": results,
        }))
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        if !filters.is_empty() {
            response["filters"] = serde_json::Value::Object(filters);
        }

        Ok(CallToolResult::success(vec![Content::json(&response)
            .map_err(|e| {
                McpError::internal_error(e.to_string(), None)
            })?]))
    }

    // ── Tool 4: memory_list ─────────────────────────────────────

    #[tool(
        description = "List all memory categories with metadata (hash, size, last update).",
        annotations(read_only_hint = true)
    )]
    async fn memory_list(
        &self,
        _params: Parameters<EmptyParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let categories = store
            .list()
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        let stats = store
            .stats()
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "summary": stats,
                "files": categories,
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 5: memory_delete ───────────────────────────────────

    #[tool(description = "Delete a memory category. Creates automatic backup before deletion.")]
    async fn memory_delete(
        &self,
        params: Parameters<DeleteParams>,
    ) -> Result<CallToolResult, McpError> {
        // ACL check
        let ctx = self.acl_context();
        match acl::authorize_write(&params.0.category, &ctx) {
            acl::AclDecision::Denied(reason) => {
                return Ok(CallToolResult::success(
                    vec![Content::json(serde_json::json!({
                    "success": false,
                    "error": format!("Unauthorized delete for category '{}'", params.0.category),
                    "reason": reason,
                    "device": ctx.device,
                })).map_err(|e| McpError::internal_error(e.to_string(), None))?],
                ));
            }
            acl::AclDecision::Allowed(_) => {}
        }

        let mut store = self.lock_store()?;
        let actor = self.device_name();
        store
            .delete(&params.0.category, &actor)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "deleted": params.0.category,
                "backup_created": true,
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 6: memory_history ──────────────────────────────────

    #[tool(
        description = "Get version history of a memory category. Shows all past versions with hashes and timestamps.",
        annotations(read_only_hint = true)
    )]
    async fn memory_history(
        &self,
        params: Parameters<HistoryParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let limit = params.0.limit.unwrap_or(20);
        let versions = store
            .history(&params.0.category, limit)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "category": params.0.category,
                "versions": versions,
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 7: memory_context (enterprise) ─────────────────────

    #[tool(
        description = "Warmup context for AI session. Returns selected fields from multiple categories, optimized for token budget.",
        annotations(read_only_hint = true)
    )]
    async fn memory_context(
        &self,
        params: Parameters<ContextParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let mut context = serde_json::Map::new();
        let mut total_bytes = 0u64;

        for cat_name in &params.0.categories {
            match store.read(cat_name) {
                Ok(value) => {
                    let filtered = if let Some(fields) = &params.0.fields {
                        filter_fields(&value, fields)
                    } else {
                        value.clone()
                    };
                    let size = serde_json::to_string(&filtered)
                        .map(|s| s.len())
                        .unwrap_or(0);
                    total_bytes += size as u64;
                    context.insert(cat_name.clone(), filtered);
                }
                Err(e) => {
                    context.insert(
                        cat_name.clone(),
                        serde_json::json!({
                            "error": e.to_string(),
                        }),
                    );
                }
            }
        }

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "context": context,
                "categories_loaded": params.0.categories.len(),
                "total_bytes": total_bytes,
                "device": self.device_name(),
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 8: memory_delta (enterprise) ───────────────────────

    #[tool(
        description = "Get only changes since a known hash. Returns diff if category was modified, empty if unchanged.",
        annotations(read_only_hint = true)
    )]
    async fn memory_delta(
        &self,
        params: Parameters<DeltaParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;

        let meta = store
            .get_meta(&params.0.category)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        match meta {
            None => Ok(CallToolResult::success(vec![Content::json(
                serde_json::json!({
                    "success": true,
                    "status": "deleted",
                    "category": params.0.category,
                }),
            )
            .map_err(|e| McpError::internal_error(e.to_string(), None))?])),
            Some(m) if m.content_hash == params.0.since_hash => {
                Ok(CallToolResult::success(vec![Content::json(
                    serde_json::json!({
                        "success": true,
                        "status": "unchanged",
                        "category": params.0.category,
                        "hash": m.content_hash,
                    }),
                )
                .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
            }
            Some(m) => {
                let value = store
                    .read(&params.0.category)
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                Ok(CallToolResult::success(vec![Content::json(
                    serde_json::json!({
                        "success": true,
                        "status": "changed",
                        "category": params.0.category,
                        "hash": m.content_hash,
                        "updated_at": m.updated_at,
                        "data": value,
                    }),
                )
                .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
            }
        }
    }

    // ── Tool 9: memory_compact (enterprise) ─────────────────────

    #[tool(
        description = "Compact memory storage. Removes old versions and backups to save disk space."
    )]
    async fn memory_compact(
        &self,
        params: Parameters<CompactParams>,
    ) -> Result<CallToolResult, McpError> {
        let keep = params.0.keep_versions.unwrap_or(5);

        // Prune backups
        let pruned = backup::prune_old_backups(&self.config)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let stats = {
            let store = self.lock_store()?;
            store
                .stats()
                .map_err(|e| McpError::internal_error(e.to_string(), None))?
        };

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "backups_pruned": pruned,
                "keep_versions": keep,
                "current_stats": stats,
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 10: memory_search_semantic ─────────────────────────

    #[tool(
        description = "Hybrid semantic + keyword search across memory. Uses TF-IDF embeddings for semantic similarity combined with FTS5 keyword matching.",
        annotations(read_only_hint = true)
    )]
    async fn memory_search_semantic(
        &self,
        params: Parameters<SemanticSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let limit = params.0.limit.unwrap_or(20);
        let semantic_weight = params.0.semantic_weight.unwrap_or(0.5).clamp(0.0, 1.0);

        // Build vocabulary from indexed text
        let texts = store
            .get_all_indexed_text()
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        if texts.is_empty() {
            return Ok(CallToolResult::success(vec![Content::json(
                serde_json::json!({
                    "success": true,
                    "query": params.0.query,
                    "total_matches": 0,
                    "results": [],
                    "note": "No indexed content found. Write some categories first."
                }),
            )
            .map_err(|e| McpError::internal_error(e.to_string(), None))?]));
        }

        let mut embedder = embeddings::LocalEmbedder::new();
        embedder.build_vocabulary(&texts);

        // Get keyword results
        let keyword_results = store
            .search(&params.0.query, limit * 3)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        let kw_tuples: Vec<(String, String, String, Option<String>)> = keyword_results
            .iter()
            .map(|r| {
                (
                    r.category_name.clone(),
                    r.key_path.clone(),
                    r.value_text.clone(),
                    r.snippet.clone(),
                )
            })
            .collect();

        // Get all indexed entries
        let all_entries = store
            .get_all_indexed_entries()
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let results = embeddings::hybrid_search(
            kw_tuples,
            all_entries,
            &embedder,
            &params.0.query,
            limit,
            semantic_weight,
        );

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "query": params.0.query,
                "semantic_weight": semantic_weight,
                "keyword_weight": 1.0 - semantic_weight,
                "total_matches": results.len(),
                "results": results,
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 11: sync_manifest ──────────────────────────────────

    #[tool(
        description = "Generate a sync manifest with merkle root hash for all categories. Used for multi-device sync.",
        annotations(read_only_hint = true)
    )]
    async fn sync_manifest(
        &self,
        params: Parameters<SyncManifestParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let device = self.device_name();

        let manifest = sync::build_manifest(&store, &device)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        if let Some(path) = &params.0.export_path {
            let path_buf = std::path::PathBuf::from(path);
            sync::export_manifest(&manifest, &path_buf)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;

            Ok(CallToolResult::success(vec![Content::json(
                serde_json::json!({
                    "success": true,
                    "exported_to": path,
                    "device": device,
                    "merkle_root": manifest.merkle_root,
                    "categories": manifest.categories.len(),
                    "timestamp": manifest.timestamp,
                }),
            )
            .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
        } else {
            Ok(CallToolResult::success(vec![Content::json(
                serde_json::json!({
                    "success": true,
                    "manifest": manifest,
                }),
            )
            .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
        }
    }

    // ── Tool 12: sync_push ──────────────────────────────────────

    #[tool(
        description = "Export categories for sync push. Generates a push file that can be transferred to another device."
    )]
    async fn sync_push(
        &self,
        params: Parameters<SyncPushParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let device = self.device_name();

        let categories = if let Some(cats) = &params.0.categories {
            cats.clone()
        } else {
            store
                .list()
                .map_err(|e| McpError::internal_error(e.to_string(), None))?
                .iter()
                .map(|c| c.name.clone())
                .collect()
        };

        let push_request = sync::export_push_data(&store, &categories, &device)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let json = serde_json::to_string_pretty(&push_request)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        std::fs::write(&params.0.export_path, &json)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "exported_to": params.0.export_path,
                "device": device,
                "categories_exported": push_request.categories.len(),
                "size_bytes": json.len(),
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 13: sync_pull ──────────────────────────────────────

    #[tool(
        description = "Import categories from a sync pull file. Resolves conflicts based on configured strategy."
    )]
    async fn sync_pull(
        &self,
        params: Parameters<SyncPullParams>,
    ) -> Result<CallToolResult, McpError> {
        let strategy = params
            .0
            .conflict_strategy
            .as_deref()
            .unwrap_or(&self.config.sync.conflict_strategy)
            .to_string();

        // Read pull data
        let content = std::fs::read_to_string(&params.0.import_path)
            .map_err(|e| McpError::internal_error(format!("Read pull file: {}", e), None))?;
        let pull_response: sync::SyncPullResponse = serde_json::from_str(&content)
            .map_err(|e| McpError::internal_error(format!("Parse pull data: {}", e), None))?;

        let mut store = self.lock_store()?;
        let device = self.device_name();

        let result = sync::apply_pull(&mut store, &pull_response, &strategy, &device)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "pulled": result.pulled.len(),
                "conflicts": result.conflicts.len(),
                "errors": result.errors.len(),
                "details": result,
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 14: sync_diff ──────────────────────────────────────

    #[tool(
        description = "Compare local manifest with a remote manifest file. Shows what needs push, pull, and conflicts.",
        annotations(read_only_hint = true)
    )]
    async fn sync_diff(
        &self,
        params: Parameters<SyncDiffParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let device = self.device_name();

        let local_manifest = sync::build_manifest(&store, &device)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let remote_path = std::path::PathBuf::from(&params.0.remote_manifest_path);
        let remote_manifest = sync::import_manifest(&remote_path)
            .map_err(|e| McpError::internal_error(format!("Read remote manifest: {}", e), None))?;

        let diff = sync::compute_diff(&local_manifest, &remote_manifest);

        let identical = local_manifest.merkle_root == remote_manifest.merkle_root;

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "local_device": device,
                "remote_device": remote_manifest.device_id,
                "local_merkle": local_manifest.merkle_root,
                "remote_merkle": remote_manifest.merkle_root,
                "identical": identical,
                "to_pull": diff.to_pull.len(),
                "to_push": diff.to_push.len(),
                "conflicts": diff.conflicts.len(),
                "unchanged": diff.unchanged.len(),
                "details": diff,
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 15: memory_status ─────────────────────────────────

    #[tool(
        description = "Report local-first memory status, dirty queue, manifest hash, and last sync state.",
        annotations(read_only_hint = true)
    )]
    async fn memory_status(
        &self,
        _params: Parameters<EmptyParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let status = sync::build_status(&store, &self.config)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::json(&status)
            .map_err(|e| {
                McpError::internal_error(e.to_string(), None)
            })?]))
    }

    // ── Tool 16: memory_doctor ─────────────────────────────────

    #[tool(
        description = "Run local memory diagnostics for storage paths, database, categories, dirty queue, and remote sync config.",
        annotations(read_only_hint = true)
    )]
    async fn memory_doctor(
        &self,
        _params: Parameters<EmptyParams>,
    ) -> Result<CallToolResult, McpError> {
        let store = self.lock_store()?;
        let stats = store
            .stats()
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        // Read-only legacy ~/.memory inspection (existence only, never reads content).
        // Safe if HOME is unset: all legacy_* fields fall back to null/false.
        let base_dir = self.config.resolved_base_dir();
        let legacy_path: Option<std::path::PathBuf> = std::env::var("HOME")
            .ok()
            .map(|h| std::path::PathBuf::from(h).join(".memory"));
        let legacy_db = legacy_path.as_ref().map(|p| p.join("memory.db"));
        let legacy_store_present = legacy_path.as_ref().is_some_and(|p| p.exists());
        let legacy_store_db_present = legacy_db.as_ref().is_some_and(|p| p.exists());
        let legacy_store_is_active = legacy_path.as_ref() == Some(&base_dir);

        // Required-env hints: machine-readable presence of the stack-managed trio.
        // Values are intentionally only "set"/"unset"; never echo back env values
        // (they may contain credentials in other servers).
        let env_status = |k: &str| {
            if std::env::var(k).is_ok() {
                "set"
            } else {
                "unset"
            }
        };
        let required_env = serde_json::json!({
            "MCP_MEMORY_CONFIG": env_status("MCP_MEMORY_CONFIG"),
            "MCP_MEMORY_MODE":   env_status("MCP_MEMORY_MODE"),
            "MCP_DEVICE":        env_status("MCP_DEVICE"),
        });

        Ok(CallToolResult::success(vec![Content::json(
            serde_json::json!({
                "success": true,
                "device": self.config.acl.device_name,
                "mode": self.config.server.mode,
                "base_dir": base_dir,
                "db_path": self.config.db_path(),
                "db_exists": self.config.db_path().exists(),
                "categories_dir_exists": self.config.categories_dir().exists(),
                "backups_dir_exists": self.config.backups_dir().exists(),
                "remote_configured": !self.config.sync.remote_url.trim().is_empty(),
                "remote_url": self.config.sync.remote_url,
                "legacy_store_path": legacy_path,
                "legacy_store_present": legacy_store_present,
                "legacy_store_db_present": legacy_store_db_present,
                "legacy_store_is_active": legacy_store_is_active,
                "required_env": required_env,
                "stats": stats,
            }),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
    }

    // ── Tool 17: memory_sync ───────────────────────────────────

    #[tool(
        description = "Run one sync step against configured remote. direction must be push_dirty or pull_remote."
    )]
    async fn memory_sync(
        &self,
        params: Parameters<MemorySyncParams>,
    ) -> Result<CallToolResult, McpError> {
        match params.0.direction.as_str() {
            "push_dirty" => {
                let (envelope, dirty_names) = {
                    let store = self.lock_store()?;
                    let envelope = sync::export_dirty_envelope(&store, &self.device_name())
                        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                    let dirty_names = store
                        .dirty_entries()
                        .map_err(|e| McpError::internal_error(e.to_string(), None))?
                        .into_iter()
                        .map(|entry| entry.category_name)
                        .collect::<Vec<_>>();
                    (envelope, dirty_names)
                };

                if envelope.categories.is_empty() && envelope.deleted.is_empty() {
                    return Ok(CallToolResult::success(vec![Content::json(
                        serde_json::json!({
                            "success": true,
                            "direction": "push_dirty",
                            "pushed": 0,
                            "details": {
                                "pushed": [],
                                "pulled": [],
                                "conflicts": [],
                                "errors": [],
                            }
                        }),
                    )
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?]));
                }

                let mut result = sync::remote_import_envelope(&self.config, &envelope)
                    .await
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                if result.errors.is_empty() {
                    let store = self.lock_store()?;
                    store
                        .clear_dirty(dirty_names.iter().map(String::as_str))
                        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                    store
                        .set_sync_state(
                            "last_sync",
                            &serde_json::json!({
                                "direction": "push",
                                "remote": self.config.sync.remote_url,
                                "timestamp": chrono::Utc::now().to_rfc3339(),
                                "pushed": dirty_names.len(),
                                "conflicts": result.conflicts.len(),
                                "errors": result.errors.len(),
                            }),
                        )
                        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                }
                result.pushed = dirty_names.clone();

                Ok(CallToolResult::success(vec![Content::json(
                    serde_json::json!({
                        "success": result.errors.is_empty(),
                        "direction": "push_dirty",
                        "pushed": dirty_names.len(),
                        "details": result,
                    }),
                )
                .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
            }
            "pull_remote" => {
                let categories = params.0.categories.unwrap_or_default();
                let envelope = sync::remote_export_envelope(&self.config, &categories)
                    .await
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                let mut store = self.lock_store()?;
                let result = sync::apply_sync_envelope(
                    &mut store,
                    &envelope,
                    &self.config.sync.conflict_strategy,
                    &self.device_name(),
                )
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                store
                    .set_sync_state(
                        "last_sync",
                        &serde_json::json!({
                            "direction": "pull",
                            "remote": self.config.sync.remote_url,
                            "timestamp": chrono::Utc::now().to_rfc3339(),
                            "pulled": result.pulled.len(),
                            "conflicts": result.conflicts.len(),
                            "errors": result.errors.len(),
                        }),
                    )
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                Ok(CallToolResult::success(vec![Content::json(
                    serde_json::json!({
                        "success": result.errors.is_empty(),
                        "direction": "pull_remote",
                        "details": result,
                    }),
                )
                .map_err(|e| McpError::internal_error(e.to_string(), None))?]))
            }
            other => Ok(CallToolResult::success(vec![Content::json(
                serde_json::json!({
                    "success": false,
                    "error": format!("Unsupported direction '{other}'. Use push_dirty or pull_remote."),
                }),
            )
            .map_err(|e| McpError::internal_error(e.to_string(), None))?])),
        }
    }
}

/// AI-first usage guide returned at `initialize` so that a weak client model
/// can drive the server without prior knowledge of its tools. Opens with what
/// to do, not with vocabulary.
const SERVER_INSTRUCTIONS: &str = "Persistent, versioned memory for an AI agent, \
organized in named JSON categories (e.g. 'base', 'projects', a per-device category). \
Start with memory_list to see what exists, memory_read {category} to load one, \
memory_write {category, content} to save (merge=true patches per top-level key; \
expected_hash prevents clobbering). memory_search runs BM25 full-text search across \
categories; memory_context loads several categories in one call for session warmup. \
Every write is versioned (memory_history) and deletes are backed up first. State lives \
on local disk and survives across sessions and model swaps.";

#[tool_handler(router = self.tool_router)]
impl ServerHandler for MemoryServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(SERVER_INSTRUCTIONS)
    }
}

// ── Helpers ─────────────────────────────────────────────────────

fn filter_fields(value: &serde_json::Value, fields_spec: &str) -> serde_json::Value {
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

#[cfg(test)]
mod contract_tests {
    use super::*;
    use crate::config::{AclConfig, ServerConfig, StorageConfig, SyncConfig};
    use rmcp::handler::server::wrapper::Parameters;
    use serde_json::{json, Value};
    use std::path::PathBuf;

    fn test_config(label: &str) -> (MemoryConfig, PathBuf) {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "mcp-memory-rs-contract-{label}-{}-{nonce}",
            std::process::id()
        ));
        let config = MemoryConfig {
            server: ServerConfig {
                mode: "offline".into(),
                host: "127.0.0.1".into(),
                port: 3100,
            },
            storage: StorageConfig {
                base_dir: dir.clone(),
                backup_retention_days: 30,
                max_versions: 5,
            },
            acl: AclConfig {
                admin_devices: vec![],
                device_name: "contract-test".into(),
                device_categories: vec![],
            },
            sync: SyncConfig::default(),
        };
        (config, dir)
    }

    fn assert_no_true_schema_or_defs(value: &Value, path: &str) {
        match value {
            Value::Bool(true) => panic!("boolean true schema at {path}"),
            Value::Bool(false) | Value::Null | Value::Number(_) | Value::String(_) => {}
            Value::Array(values) => {
                for (index, item) in values.iter().enumerate() {
                    assert_no_true_schema_or_defs(item, &format!("{path}[{index}]"));
                }
            }
            Value::Object(map) => {
                assert!(
                    !map.contains_key("$defs"),
                    "schema uses $defs at {path}; keep MCP tool schemas inline for broad client compatibility"
                );
                for (key, item) in map {
                    assert_no_true_schema_or_defs(item, &format!("{path}.{key}"));
                }
            }
        }
    }

    #[test]
    fn tools_list_schemas_are_strict_objects_for_universal_clients() {
        let (config, dir) = test_config("schemas");
        let server = MemoryServer::from_config(config).expect("server should start");
        let tools = server.tool_router.list_all();

        assert!(!tools.is_empty(), "server must expose tools");

        for tool in &tools {
            assert!(!tool.name.trim().is_empty(), "tool name is required");
            assert!(
                tool.description
                    .as_ref()
                    .map(|description| !description.trim().is_empty())
                    .unwrap_or(false),
                "{} must have a non-empty description",
                tool.name
            );

            let schema = Value::Object((*tool.input_schema).clone());
            assert_eq!(
                schema.get("type").and_then(Value::as_str),
                Some("object"),
                "{} inputSchema must be a JSON object schema: {schema}",
                tool.name
            );
            assert!(
                schema.get("properties").is_some_and(Value::is_object),
                "{} inputSchema must include properties, even when empty: {schema}",
                tool.name
            );
            assert_no_true_schema_or_defs(&schema, &format!("{}.inputSchema", tool.name));
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn core_tool_names_and_aliases_stay_available() {
        let (config, dir) = test_config("aliases");
        let server = MemoryServer::from_config(config).expect("server should start");
        let names: std::collections::BTreeSet<_> = server
            .tool_router
            .list_all()
            .into_iter()
            .map(|tool| tool.name.into_owned())
            .collect();

        // Short aliases (read/write/list/status) were removed by design on 2026-05-13.
        // Only the full-prefixed names must be present.
        for required in [
            "memory_read",
            "memory_write",
            "memory_list",
            "memory_status",
            "memory_doctor",
        ] {
            assert!(names.contains(required), "missing tool {required}");
        }
        // Verify aliases are gone (not accidentally reintroduced)
        for removed in ["read", "write", "list", "status"] {
            assert!(
                !names.contains(removed),
                "short alias '{removed}' must not be re-registered"
            );
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn get_info_exposes_ai_first_instructions() {
        let (config, dir) = test_config("instructions");
        let server = MemoryServer::from_config(config).expect("server should start");
        let info = server.get_info();
        let instructions = info
            .instructions
            .expect("initialize must return instructions for weak AI clients");
        // Must open with the actionable verb, not jargon.
        assert!(
            instructions.starts_with("Persistent, versioned memory"),
            "instructions should open with usage framing"
        );
        // Must name the entry-point tools so a model can self-bootstrap.
        for needle in ["memory_list", "memory_read", "memory_write"] {
            assert!(
                instructions.contains(needle),
                "instructions must mention {needle}"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_only_tools_carry_read_only_hint() {
        let (config, dir) = test_config("readonly-hint");
        let server = MemoryServer::from_config(config).expect("server should start");
        let tools = server.tool_router.list_all();

        let read_only = [
            "memory_read",
            "memory_list",
            "memory_search",
            "memory_search_semantic",
            "memory_history",
            "memory_delta",
            "memory_context",
            "memory_status",
            "memory_doctor",
            "sync_manifest",
            "sync_diff",
        ];
        let mutating = [
            "memory_write",
            "memory_delete",
            "memory_compact",
            "memory_sync",
            "sync_push",
            "sync_pull",
        ];

        for tool in &tools {
            let name = tool.name.as_ref();
            let hint = tool
                .annotations
                .as_ref()
                .and_then(|a| a.read_only_hint)
                .unwrap_or(false);
            if read_only.contains(&name) {
                assert!(hint, "{name} must be annotated read_only_hint=true");
            }
            if mutating.contains(&name) {
                assert!(!hint, "{name} must not claim read_only_hint");
            }
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn memory_status_accepts_empty_arguments() {
        let (config, dir) = test_config("status");
        let server = MemoryServer::from_config(config).expect("server should start");
        let result = server
            .memory_status(Parameters(EmptyParams {}))
            .await
            .expect("memory_status should accept empty arguments");

        assert_ne!(result.is_error, Some(true));
        assert!(
            !result.content.is_empty(),
            "memory_status should return content"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    fn admin_config(label: &str) -> (MemoryConfig, PathBuf) {
        let (mut config, dir) = test_config(label);
        config.acl.device_name = "server-a".into();
        config.acl.admin_devices = vec!["server-a".into()];
        (config, dir)
    }

    fn object(pairs: &[(&str, Value)]) -> ObjectPayload {
        let mut map = serde_json::Map::new();
        for (k, v) in pairs {
            map.insert((*k).into(), v.clone());
        }
        ObjectPayload(map)
    }

    async fn write(
        server: &MemoryServer,
        category: &str,
        data: ObjectPayload,
        expected_hash: Option<String>,
        merge: Option<bool>,
    ) -> Result<CallToolResult, McpError> {
        server
            .memory_write(Parameters(WriteParams {
                category: category.into(),
                data,
                reason: None,
                expected_hash,
                merge,
            }))
            .await
    }

    fn stored(server: &MemoryServer, category: &str) -> Value {
        server
            .lock_store()
            .expect("lock store")
            .read(category)
            .expect("read category")
    }

    #[tokio::test]
    async fn merge_preserves_untouched_keys_and_replaces_target() {
        let (config, dir) = admin_config("merge-preserve");
        let server = MemoryServer::from_config(config).expect("server should start");

        write(
            &server,
            "notes",
            object(&[("a", json!(1)), ("b", json!(2))]),
            None,
            None,
        )
        .await
        .expect("initial write");

        write(
            &server,
            "notes",
            object(&[("b", json!(99))]),
            None,
            Some(true),
        )
        .await
        .expect("merge write");

        let value = stored(&server, "notes");
        assert_eq!(value["a"], json!(1), "untouched key preserved");
        assert_eq!(value["b"], json!(99), "target key replaced");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn merge_null_removes_key() {
        let (config, dir) = admin_config("merge-null");
        let server = MemoryServer::from_config(config).expect("server should start");

        write(
            &server,
            "notes",
            object(&[("a", json!(1)), ("b", json!(2))]),
            None,
            None,
        )
        .await
        .expect("initial write");

        write(
            &server,
            "notes",
            object(&[("b", Value::Null)]),
            None,
            Some(true),
        )
        .await
        .expect("merge write");

        let value = stored(&server, "notes");
        assert_eq!(value["a"], json!(1), "untouched key preserved");
        assert!(value.get("b").is_none(), "null deletes key");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn merge_on_missing_category_creates() {
        let (config, dir) = admin_config("merge-create");
        let server = MemoryServer::from_config(config).expect("server should start");

        write(
            &server,
            "fresh",
            object(&[("a", json!(1))]),
            None,
            Some(true),
        )
        .await
        .expect("merge create");

        let value = stored(&server, "fresh");
        assert_eq!(value["a"], json!(1), "merge created the category");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn default_write_replaces_whole_category() {
        let (config, dir) = admin_config("replace-default");
        let server = MemoryServer::from_config(config).expect("server should start");

        write(
            &server,
            "notes",
            object(&[("a", json!(1)), ("b", json!(2))]),
            None,
            None,
        )
        .await
        .expect("initial write");

        write(&server, "notes", object(&[("c", json!(3))]), None, None)
            .await
            .expect("replace write");

        let value = stored(&server, "notes");
        assert!(value.get("a").is_none(), "default write replaces content");
        assert!(value.get("b").is_none(), "default write replaces content");
        assert_eq!(value["c"], json!(3), "only new content remains");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn merge_with_wrong_expected_hash_fails() {
        let (config, dir) = admin_config("merge-hash");
        let server = MemoryServer::from_config(config).expect("server should start");

        write(&server, "notes", object(&[("a", json!(1))]), None, None)
            .await
            .expect("initial write");

        let result = write(
            &server,
            "notes",
            object(&[("b", json!(2))]),
            Some("deadbeef".into()),
            Some(true),
        )
        .await;

        assert!(result.is_err(), "merge with stale expected_hash must fail");

        let value = stored(&server, "notes");
        assert!(value.get("b").is_none(), "failed merge left content intact");

        let _ = std::fs::remove_dir_all(dir);
    }
}
