use crate::config::MemoryConfig;
use crate::error::Result;
use crate::store::{CategoryMeta, Store};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

// ── Data types ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncManifest {
    pub device_id: String,
    pub timestamp: String,
    pub merkle_root: String,
    pub categories: Vec<CategorySyncEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategorySyncEntry {
    pub name: String,
    pub content_hash: String,
    pub updated_at: String,
    pub updated_by: String,
    pub merkle_leaf: String,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPushRequest {
    pub source_device: String,
    pub categories: Vec<PushEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushEntry {
    pub name: String,
    pub data: serde_json::Value,
    pub content_hash: String,
    pub updated_at: String,
    pub updated_by: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPullResponse {
    pub categories: Vec<PullEntry>,
    pub deleted: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullEntry {
    pub name: String,
    pub data: serde_json::Value,
    pub content_hash: String,
    pub updated_at: String,
    pub updated_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    pub pushed: Vec<String>,
    pub pulled: Vec<String>,
    pub conflicts: Vec<ConflictEntry>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStatus {
    pub device_id: String,
    pub remote_url: String,
    pub local_merkle_root: String,
    pub categories: usize,
    pub dirty_categories: usize,
    pub last_sync: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictEntry {
    pub category: String,
    pub local_hash: String,
    pub remote_hash: String,
    pub resolution: ConflictResolution,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConflictResolution {
    LocalWins,
    RemoteWins,
    Merged,
    Skipped,
}

// ── Canonical sync transfer envelope ────────────────────────────

/// Canonical file-based sync envelope used for both push and pull.
/// This is the single format for sync data transfer between stores.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncTransferEnvelope {
    pub source_device: String,
    pub exported_at: String,
    pub categories: Vec<SyncTransferEntry>,
    #[serde(default)]
    pub deleted: Vec<String>,
}

/// A single category entry in the sync transfer envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncTransferEntry {
    pub name: String,
    pub data: serde_json::Value,
    pub content_hash: String,
    pub updated_at: String,
    pub updated_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

// ── Merkle tree ─────────────────────────────────────────────────

/// Compute merkle root from sorted category hashes.
/// Each leaf = SHA256(name + ":" + content_hash).
/// Root = SHA256 of all leaves concatenated (sorted).
pub fn compute_merkle_root(entries: &[(&str, &str)]) -> String {
    if entries.is_empty() {
        return "empty".to_string();
    }

    let mut leaves: Vec<String> = entries
        .iter()
        .map(|(name, hash)| {
            let mut hasher = Sha256::new();
            hasher.update(format!("{}:{}", name, hash).as_bytes());
            format!("{:x}", hasher.finalize())
        })
        .collect();
    leaves.sort();

    let mut hasher = Sha256::new();
    for leaf in &leaves {
        hasher.update(leaf.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

// ── Sync manifest ───────────────────────────────────────────────

pub fn build_manifest(store: &Store, device_id: &str) -> Result<SyncManifest> {
    let categories = store.list()?;
    let mut entries = Vec::new();
    let mut leaf_data = Vec::new();

    for cat in &categories {
        let version: u64 = get_category_version(store, &cat.name)?;
        let leaf = compute_leaf(&cat.name, &cat.content_hash);
        leaf_data.push((cat.name.as_str(), cat.content_hash.as_str()));
        entries.push(CategorySyncEntry {
            name: cat.name.clone(),
            content_hash: cat.content_hash.clone(),
            updated_at: cat.updated_at.clone(),
            updated_by: cat.updated_by.clone(),
            merkle_leaf: leaf,
            version,
        });
    }

    // Sort entries by name for deterministic merkle root
    let mut leaf_refs: Vec<(&str, &str)> = leaf_data.iter().map(|(n, h)| (*n, *h)).collect();
    leaf_refs.sort_by_key(|(n, _)| *n);

    Ok(SyncManifest {
        device_id: device_id.to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        merkle_root: compute_merkle_root(&leaf_refs),
        categories: entries,
    })
}

fn compute_leaf(name: &str, hash: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{}:{}", name, hash).as_bytes());
    format!("{:x}", hasher.finalize())
}

fn get_category_version(store: &Store, name: &str) -> Result<u64> {
    // Use the latest version entry ID as version number
    let versions = store.history(name, 1)?;
    Ok(versions.first().map(|v| v.id as u64).unwrap_or(0))
}

// ── Diff computation ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncDiff {
    /// Categories that exist remotely but not locally (need pull)
    pub to_pull: Vec<CategorySyncEntry>,
    /// Categories that exist locally but not remotely (need push)
    pub to_push: Vec<String>,
    /// Categories with different hashes (potential conflict)
    pub conflicts: Vec<(String, String, String)>, // (name, local_hash, remote_hash)
    /// Categories that are identical
    pub unchanged: Vec<String>,
}

pub fn compute_diff(local: &SyncManifest, remote: &SyncManifest) -> SyncDiff {
    let local_map: std::collections::HashMap<&str, &CategorySyncEntry> = local
        .categories
        .iter()
        .map(|e| (e.name.as_str(), e))
        .collect();

    let remote_map: std::collections::HashMap<&str, &CategorySyncEntry> = remote
        .categories
        .iter()
        .map(|e| (e.name.as_str(), e))
        .collect();

    let mut to_pull = Vec::new();
    let mut to_push = Vec::new();
    let mut conflicts = Vec::new();
    let mut unchanged = Vec::new();

    // Find all category names
    let mut all_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for name in local_map.keys() {
        all_names.insert(name);
    }
    for name in remote_map.keys() {
        all_names.insert(name);
    }

    for name in all_names {
        match (local_map.get(name), remote_map.get(name)) {
            (None, Some(remote)) => {
                to_pull.push((*remote).clone());
            }
            (Some(_local), None) => {
                to_push.push(name.to_string());
            }
            (None, None) => {
                // Should not happen, skip
            }
            (Some(local_entry), Some(remote_entry)) => {
                if local_entry.content_hash == remote_entry.content_hash {
                    unchanged.push(name.to_string());
                } else {
                    // Conflict: compare timestamps for default resolution
                    conflicts.push((
                        name.to_string(),
                        local_entry.content_hash.clone(),
                        remote_entry.content_hash.clone(),
                    ));
                }
            }
        }
    }

    SyncDiff {
        to_pull,
        to_push,
        conflicts,
        unchanged,
    }
}

// ── Conflict resolution ─────────────────────────────────────────

pub fn resolve_conflict(
    local: &CategoryMeta,
    remote: &PullEntry,
    strategy: &str,
) -> ConflictResolution {
    match strategy {
        "last_write_wins" => {
            if remote.updated_at >= local.updated_at {
                ConflictResolution::RemoteWins
            } else {
                ConflictResolution::LocalWins
            }
        }
        "local_wins" => ConflictResolution::LocalWins,
        "remote_wins" => ConflictResolution::RemoteWins,
        _ => {
            // Default: last_write_wins
            if remote.updated_at >= local.updated_at {
                ConflictResolution::RemoteWins
            } else {
                ConflictResolution::LocalWins
            }
        }
    }
}

// ── File-based sync (export/import manifest) ────────────────────

pub fn export_manifest(manifest: &SyncManifest, path: &PathBuf) -> Result<()> {
    let json = serde_json::to_string_pretty(manifest)?;
    std::fs::write(path, json)?;
    Ok(())
}

pub fn import_manifest(path: &PathBuf) -> Result<SyncManifest> {
    let content = std::fs::read_to_string(path)?;
    let manifest: SyncManifest = serde_json::from_str(&content)?;
    Ok(manifest)
}

// ── Canonical envelope export/import ────────────────────────────

/// Export a sync envelope from the store for the given category names.
/// Produces a SyncTransferEnvelope that can be consumed by apply_sync_envelope.
pub fn export_sync_envelope(
    store: &Store,
    categories: &[String],
    device_id: &str,
) -> Result<SyncTransferEnvelope> {
    let mut entries = Vec::new();
    for name in categories {
        match store.read(name) {
            Ok(data) => {
                if let Some(meta) = store.get_meta(name)? {
                    entries.push(SyncTransferEntry {
                        name: name.clone(),
                        data,
                        content_hash: meta.content_hash,
                        updated_at: meta.updated_at,
                        updated_by: meta.updated_by,
                        reason: None,
                    });
                }
            }
            Err(e) => {
                tracing::warn!("Skipping {} for sync export: {}", name, e);
            }
        }
    }
    Ok(SyncTransferEnvelope {
        source_device: device_id.to_string(),
        exported_at: chrono::Utc::now().to_rfc3339(),
        categories: entries,
        deleted: vec![],
    })
}

/// Export only locally dirty categories. Deleted entries are carried in `deleted`.
pub fn export_dirty_envelope(store: &Store, device_id: &str) -> Result<SyncTransferEnvelope> {
    let dirty = store.dirty_entries()?;
    let categories = dirty
        .iter()
        .filter(|entry| entry.operation != "delete")
        .map(|entry| entry.category_name.clone())
        .collect::<Vec<_>>();
    let mut envelope = export_sync_envelope(store, &categories, device_id)?;
    envelope.deleted = dirty
        .iter()
        .filter(|entry| entry.operation == "delete")
        .map(|entry| entry.category_name.clone())
        .collect();
    Ok(envelope)
}

/// Write a sync envelope to a file.
pub fn write_envelope(envelope: &SyncTransferEnvelope, path: &PathBuf) -> Result<()> {
    let json = serde_json::to_string_pretty(envelope)?;
    std::fs::write(path, json)?;
    Ok(())
}

/// Read a sync envelope from a file.
pub fn read_envelope(path: &PathBuf) -> Result<SyncTransferEnvelope> {
    let content = std::fs::read_to_string(path)?;
    let envelope: SyncTransferEnvelope = serde_json::from_str(&content)?;
    Ok(envelope)
}

/// Apply a sync envelope to a store, preserving remote metadata.
/// Uses `upsert_synced_category` to keep remote updated_at/updated_by.
pub fn apply_sync_envelope(
    store: &mut Store,
    envelope: &SyncTransferEnvelope,
    conflict_strategy: &str,
    local_device_id: &str,
) -> Result<SyncResult> {
    let mut result = SyncResult {
        pushed: vec![],
        pulled: vec![],
        conflicts: vec![],
        errors: vec![],
    };

    for entry in &envelope.categories {
        match store.get_meta(&entry.name) {
            Ok(Some(local_meta)) => {
                if local_meta.content_hash != entry.content_hash {
                    // Build a temporary PullEntry for resolve_conflict compat
                    let remote = PullEntry {
                        name: entry.name.clone(),
                        data: entry.data.clone(),
                        content_hash: entry.content_hash.clone(),
                        updated_at: entry.updated_at.clone(),
                        updated_by: entry.updated_by.clone(),
                    };
                    let resolution = resolve_conflict(&local_meta, &remote, conflict_strategy);
                    match resolution {
                        ConflictResolution::RemoteWins => {
                            match store.upsert_synced_category(
                                &entry.name,
                                &entry.data,
                                &entry.content_hash,
                                &entry.updated_at,
                                &entry.updated_by,
                                Some(&format!("Sync pull from {}", envelope.source_device)),
                            ) {
                                Ok(_) => {
                                    result.pulled.push(entry.name.clone());
                                    store.clear_dirty([entry.name.as_str()])?;
                                    result.conflicts.push(ConflictEntry {
                                        category: entry.name.clone(),
                                        local_hash: local_meta.content_hash,
                                        remote_hash: entry.content_hash.clone(),
                                        resolution,
                                    });
                                }
                                Err(e) => result.errors.push(format!("{}: {}", entry.name, e)),
                            }
                        }
                        ConflictResolution::LocalWins => {
                            result.conflicts.push(ConflictEntry {
                                category: entry.name.clone(),
                                local_hash: local_meta.content_hash,
                                remote_hash: entry.content_hash.clone(),
                                resolution,
                            });
                        }
                        _ => {}
                    }
                }
            }
            Ok(None) => {
                // New category — import preserving remote metadata
                match store.upsert_synced_category(
                    &entry.name,
                    &entry.data,
                    &entry.content_hash,
                    &entry.updated_at,
                    &entry.updated_by,
                    Some(&format!("Sync pull from {}", envelope.source_device)),
                ) {
                    Ok(_) => {
                        result.pulled.push(entry.name.clone());
                        store.clear_dirty([entry.name.as_str()])?;
                    }
                    Err(e) => result.errors.push(format!("{}: {}", entry.name, e)),
                }
            }
            Err(e) => result.errors.push(format!("{}: {}", entry.name, e)),
        }
    }

    // Apply deletions
    for name in &envelope.deleted {
        match store.delete(name, local_device_id) {
            Ok(_) => {
                store.clear_dirty([name.as_str()])?;
                result.pulled.push(format!("(deleted) {}", name));
            }
            Err(e) => result.errors.push(format!("delete {} : {}", name, e)),
        }
    }

    Ok(result)
}

pub fn build_status(store: &Store, config: &MemoryConfig) -> Result<SyncStatus> {
    let manifest = build_manifest(store, &config.acl.device_name)?;
    let dirty = store.dirty_entries()?;
    Ok(SyncStatus {
        device_id: config.acl.device_name.clone(),
        remote_url: config.sync.remote_url.clone(),
        local_merkle_root: manifest.merkle_root,
        categories: manifest.categories.len(),
        dirty_categories: dirty.len(),
        last_sync: store.get_sync_state("last_sync")?,
    })
}

#[derive(Debug, Deserialize)]
struct RemoteApiResponse {
    success: bool,
    data: Option<serde_json::Value>,
    error: Option<String>,
}

fn remote_client(config: &MemoryConfig) -> Result<reqwest::Client> {
    if config.sync.remote_url.trim().is_empty() {
        return Err(crate::error::MemoryError::Sync(
            "sync.remote_url is not configured".into(),
        ));
    }
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| crate::error::MemoryError::Sync(e.to_string()))
}

async fn remote_json<T: for<'de> Deserialize<'de>>(request: reqwest::RequestBuilder) -> Result<T> {
    let response = request
        .send()
        .await
        .map_err(|e| crate::error::MemoryError::Sync(e.to_string()))?;
    let status = response.status();
    let payload: RemoteApiResponse = response
        .json()
        .await
        .map_err(|e| crate::error::MemoryError::Sync(e.to_string()))?;
    if !status.is_success() || !payload.success {
        return Err(crate::error::MemoryError::Sync(
            payload
                .error
                .unwrap_or_else(|| format!("remote returned HTTP {status}")),
        ));
    }
    let data = payload
        .data
        .ok_or_else(|| crate::error::MemoryError::Sync("remote response missing data".into()))?;
    serde_json::from_value(data).map_err(|e| crate::error::MemoryError::Sync(e.to_string()))
}

fn with_auth(request: reqwest::RequestBuilder, config: &MemoryConfig) -> reqwest::RequestBuilder {
    match config.remote_token() {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

pub async fn sync_pull_from_remote(
    store: &mut Store,
    config: &MemoryConfig,
    categories: &[String],
) -> Result<SyncResult> {
    let client = remote_client(config)?;
    let url = format!(
        "{}/api/v1/sync/export",
        config.sync.remote_url.trim_end_matches('/')
    );
    let request = with_auth(
        client
            .post(url)
            .json(&serde_json::json!({ "categories": categories })),
        config,
    );
    let envelope: SyncTransferEnvelope = remote_json(request).await?;
    let result = apply_sync_envelope(
        store,
        &envelope,
        &config.sync.conflict_strategy,
        &config.acl.device_name,
    )?;
    store.set_sync_state(
        "last_sync",
        &serde_json::json!({
            "direction": "pull",
            "remote": config.sync.remote_url,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "pulled": result.pulled.len(),
            "conflicts": result.conflicts.len(),
            "errors": result.errors.len(),
        }),
    )?;
    Ok(result)
}

pub async fn remote_export_envelope(
    config: &MemoryConfig,
    categories: &[String],
) -> Result<SyncTransferEnvelope> {
    let client = remote_client(config)?;
    let url = format!(
        "{}/api/v1/sync/export",
        config.sync.remote_url.trim_end_matches('/')
    );
    let request = with_auth(
        client
            .post(url)
            .json(&serde_json::json!({ "categories": categories })),
        config,
    );
    remote_json(request).await
}

pub async fn remote_import_envelope(
    config: &MemoryConfig,
    envelope: &SyncTransferEnvelope,
) -> Result<SyncResult> {
    let client = remote_client(config)?;
    let url = format!(
        "{}/api/v1/sync/import",
        config.sync.remote_url.trim_end_matches('/')
    );
    let request = with_auth(
        client.post(url).json(&serde_json::json!({
            "envelope": envelope,
            "conflict_strategy": config.sync.conflict_strategy,
        })),
        config,
    );
    remote_json(request).await
}

pub async fn sync_push_dirty_to_remote(
    store: &mut Store,
    config: &MemoryConfig,
) -> Result<SyncResult> {
    let envelope = export_dirty_envelope(store, &config.acl.device_name)?;
    if envelope.categories.is_empty() && envelope.deleted.is_empty() {
        return Ok(SyncResult {
            pushed: vec![],
            pulled: vec![],
            conflicts: vec![],
            errors: vec![],
        });
    }

    let mut result = remote_import_envelope(config, &envelope).await?;
    let dirty_names = store
        .dirty_entries()?
        .into_iter()
        .map(|entry| entry.category_name)
        .collect::<Vec<_>>();
    store.clear_dirty(dirty_names.iter().map(String::as_str))?;
    result.pushed = dirty_names;
    store.set_sync_state(
        "last_sync",
        &serde_json::json!({
            "direction": "push",
            "remote": config.sync.remote_url,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "pushed": result.pushed.len(),
            "conflicts": result.conflicts.len(),
            "errors": result.errors.len(),
        }),
    )?;
    Ok(result)
}

// ── Legacy wrappers (backward compat) ───────────────────────────

pub fn export_push_data(
    store: &Store,
    categories: &[String],
    device_id: &str,
) -> Result<SyncPushRequest> {
    let envelope = export_sync_envelope(store, categories, device_id)?;
    Ok(SyncPushRequest {
        source_device: envelope.source_device,
        categories: envelope
            .categories
            .into_iter()
            .map(|e| PushEntry {
                name: e.name,
                data: e.data,
                content_hash: e.content_hash,
                updated_at: e.updated_at,
                updated_by: e.updated_by,
                reason: e.reason,
            })
            .collect(),
    })
}

pub fn apply_pull(
    store: &mut Store,
    pull_response: &SyncPullResponse,
    conflict_strategy: &str,
    device_id: &str,
) -> Result<SyncResult> {
    // Convert legacy format to envelope
    let envelope = SyncTransferEnvelope {
        source_device: device_id.to_string(),
        exported_at: chrono::Utc::now().to_rfc3339(),
        categories: pull_response
            .categories
            .iter()
            .map(|e| SyncTransferEntry {
                name: e.name.clone(),
                data: e.data.clone(),
                content_hash: e.content_hash.clone(),
                updated_at: e.updated_at.clone(),
                updated_by: e.updated_by.clone(),
                reason: None,
            })
            .collect(),
        deleted: pull_response.deleted.clone(),
    };
    apply_sync_envelope(store, &envelope, conflict_strategy, device_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merkle_root_empty() {
        let root = compute_merkle_root(&[]);
        assert_eq!(root, "empty");
    }

    #[test]
    fn test_merkle_root_deterministic() {
        let entries: &[(&str, &str)] = &[("base", "hash1"), ("projects", "hash2")];
        let root1 = compute_merkle_root(entries);
        let root2 = compute_merkle_root(entries);
        assert_eq!(root1, root2);
        assert_ne!(root1, "empty");
    }

    #[test]
    fn test_merkle_root_order_independent() {
        let a: &[(&str, &str)] = &[("base", "hash1"), ("projects", "hash2")];
        let b: &[(&str, &str)] = &[("projects", "hash2"), ("base", "hash1")];
        assert_eq!(compute_merkle_root(a), compute_merkle_root(b));
    }

    #[test]
    fn test_merkle_root_different_data() {
        let a: &[(&str, &str)] = &[("base", "hash1")];
        let b: &[(&str, &str)] = &[("base", "hash2")];
        assert_ne!(compute_merkle_root(a), compute_merkle_root(b));
    }

    fn make_entry(name: &str, hash: &str) -> CategorySyncEntry {
        CategorySyncEntry {
            name: name.to_string(),
            content_hash: hash.to_string(),
            updated_at: "2026-04-10T00:00:00Z".to_string(),
            updated_by: "test".to_string(),
            merkle_leaf: "leaf".to_string(),
            version: 1,
        }
    }

    fn make_local_meta(name: &str, hash: &str, updated_at: &str) -> crate::store::CategoryMeta {
        crate::store::CategoryMeta {
            name: name.to_string(),
            namespace: "default".to_string(),
            content_hash: hash.to_string(),
            updated_at: updated_at.to_string(),
            updated_by: "device_a".to_string(),
            write_policy: "device".to_string(),
            size_bytes: 100,
        }
    }

    fn make_manifest(entries: Vec<CategorySyncEntry>) -> SyncManifest {
        SyncManifest {
            device_id: "test".to_string(),
            timestamp: "2026-04-10T00:00:00Z".to_string(),
            merkle_root: "test_root".to_string(),
            categories: entries,
        }
    }

    #[test]
    fn test_compute_diff_identical() {
        let entry = make_entry("base", "hash1");
        let local = make_manifest(vec![entry.clone()]);
        let remote = make_manifest(vec![entry]);
        let diff = compute_diff(&local, &remote);
        assert!(diff.to_pull.is_empty());
        assert!(diff.to_push.is_empty());
        assert!(diff.conflicts.is_empty());
        assert_eq!(diff.unchanged, vec!["base"]);
    }

    #[test]
    fn test_compute_diff_pull_needed() {
        let local = make_manifest(vec![]);
        let remote = make_manifest(vec![make_entry("base", "hash1")]);
        let diff = compute_diff(&local, &remote);
        assert_eq!(diff.to_pull.len(), 1);
        assert_eq!(diff.to_pull[0].name, "base");
        assert!(diff.to_push.is_empty());
    }

    #[test]
    fn test_compute_diff_push_needed() {
        let local = make_manifest(vec![make_entry("base", "hash1")]);
        let remote = make_manifest(vec![]);
        let diff = compute_diff(&local, &remote);
        assert!(diff.to_pull.is_empty());
        assert_eq!(diff.to_push, vec!["base"]);
    }

    #[test]
    fn test_compute_diff_conflict() {
        let local = make_manifest(vec![make_entry("base", "hash_local")]);
        let remote = make_manifest(vec![make_entry("base", "hash_remote")]);
        let diff = compute_diff(&local, &remote);
        assert_eq!(diff.conflicts.len(), 1);
        assert_eq!(diff.conflicts[0].0, "base");
        assert_eq!(diff.conflicts[0].1, "hash_local");
        assert_eq!(diff.conflicts[0].2, "hash_remote");
    }

    #[test]
    fn test_resolve_conflict_last_write_wins_remote() {
        let local = make_local_meta("base", "local_hash", "2026-04-10T00:00:00Z");
        let remote = PullEntry {
            name: "base".to_string(),
            data: serde_json::json!({}),
            content_hash: "remote_hash".to_string(),
            updated_at: "2026-04-10T01:00:00Z".to_string(),
            updated_by: "device_b".to_string(),
        };
        let result = resolve_conflict(&local, &remote, "last_write_wins");
        assert_eq!(result, ConflictResolution::RemoteWins);
    }

    #[test]
    fn test_resolve_conflict_last_write_wins_local() {
        let local = make_local_meta("base", "local_hash", "2026-04-10T02:00:00Z");
        let remote = PullEntry {
            name: "base".to_string(),
            data: serde_json::json!({}),
            content_hash: "remote_hash".to_string(),
            updated_at: "2026-04-10T01:00:00Z".to_string(),
            updated_by: "device_b".to_string(),
        };
        let result = resolve_conflict(&local, &remote, "last_write_wins");
        assert_eq!(result, ConflictResolution::LocalWins);
    }

    #[test]
    fn test_resolve_conflict_local_wins_strategy() {
        let local = make_local_meta("base", "local_hash", "2026-04-10T00:00:00Z");
        let remote = PullEntry {
            name: "base".to_string(),
            data: serde_json::json!({}),
            content_hash: "remote_hash".to_string(),
            updated_at: "2026-04-10T99:00:00Z".to_string(),
            updated_by: "device_b".to_string(),
        };
        let result = resolve_conflict(&local, &remote, "local_wins");
        assert_eq!(result, ConflictResolution::LocalWins);
    }

    #[test]
    fn test_resolve_conflict_remote_wins_strategy() {
        let local = make_local_meta("base", "local_hash", "2026-04-10T99:00:00Z");
        let remote = PullEntry {
            name: "base".to_string(),
            data: serde_json::json!({}),
            content_hash: "remote_hash".to_string(),
            updated_at: "2026-04-10T00:00:00Z".to_string(),
            updated_by: "device_b".to_string(),
        };
        let result = resolve_conflict(&local, &remote, "remote_wins");
        assert_eq!(result, ConflictResolution::RemoteWins);
    }

    #[test]
    fn test_export_import_manifest_roundtrip() {
        let manifest = make_manifest(vec![make_entry("base", "hash1")]);
        let dir = std::env::temp_dir().join(format!("mcp_test_manifest_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("manifest.json");
        export_manifest(&manifest, &path).unwrap();
        let loaded = import_manifest(&path).unwrap();
        assert_eq!(loaded.device_id, "test");
        assert_eq!(loaded.categories.len(), 1);
        assert_eq!(loaded.categories[0].name, "base");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Canonical envelope tests ──────────────────────────────────

    #[test]
    fn test_envelope_serialize_deserialize_roundtrip() {
        let envelope = SyncTransferEnvelope {
            source_device: "device-b".to_string(),
            exported_at: "2026-04-10T12:00:00Z".to_string(),
            categories: vec![SyncTransferEntry {
                name: "base".to_string(),
                data: serde_json::json!({"key": "value"}),
                content_hash: "abc123".to_string(),
                updated_at: "2026-04-10T11:00:00Z".to_string(),
                updated_by: "server-a".to_string(),
                reason: Some("test".to_string()),
            }],
            deleted: vec!["old_cat".to_string()],
        };
        let json = serde_json::to_string_pretty(&envelope).unwrap();
        let loaded: SyncTransferEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded, envelope);
    }

    #[test]
    fn test_envelope_file_roundtrip() {
        let envelope = SyncTransferEnvelope {
            source_device: "device-b".to_string(),
            exported_at: "2026-04-10T12:00:00Z".to_string(),
            categories: vec![SyncTransferEntry {
                name: "test".to_string(),
                data: serde_json::json!({"v": 1}),
                content_hash: "hash".to_string(),
                updated_at: "2026-04-10T11:00:00Z".to_string(),
                updated_by: "device-b".to_string(),
                reason: None,
            }],
            deleted: vec![],
        };
        let dir = std::env::temp_dir().join(format!("mcp_test_env_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("envelope.json");
        write_envelope(&envelope, &path).unwrap();
        let loaded = read_envelope(&path).unwrap();
        assert_eq!(loaded, envelope);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Temp-store E2E helpers ─────────────────────────────────────

    fn make_store_config(label: &str) -> (crate::config::MemoryConfig, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("mcp_sync_e2e_{}_{}", label, id));
        let _ = std::fs::remove_dir_all(&dir);
        let config = crate::config::MemoryConfig {
            server: crate::config::ServerConfig {
                mode: "offline".into(),
                host: "127.0.0.1".into(),
                port: 3100,
            },
            storage: crate::config::StorageConfig {
                base_dir: dir.clone(),
                backup_retention_days: 30,
                max_versions: 100,
            },
            acl: crate::config::AclConfig::default(),
            sync: crate::config::SyncConfig::default(),
        };
        (config, dir)
    }

    fn cleanup_store(dir: &PathBuf) {
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_sync_new_category_a_to_b() {
        let (config_a, dir_a) = make_store_config("a");
        let (config_b, dir_b) = make_store_config("b");

        let mut store_a = Store::new(&config_a).unwrap();
        let mut store_b = Store::new(&config_b).unwrap();

        // A writes a category
        store_a
            .write(
                "shared_cat",
                &serde_json::json!({"data": "from_a"}),
                None,
                "device_a",
                None,
            )
            .unwrap();

        // Export from A
        let envelope =
            export_sync_envelope(&store_a, &["shared_cat".to_string()], "device_a").unwrap();
        assert_eq!(envelope.categories.len(), 1);
        assert_eq!(envelope.categories[0].name, "shared_cat");
        assert_eq!(envelope.categories[0].updated_by, "device_a");

        // Import into B
        let result =
            apply_sync_envelope(&mut store_b, &envelope, "last_write_wins", "device_b").unwrap();
        assert_eq!(result.pulled, vec!["shared_cat"]);

        // Verify B has the data with A's metadata
        let data_b = store_b.read("shared_cat").unwrap();
        assert_eq!(data_b["data"], "from_a");
        let meta_b = store_b.get_meta("shared_cat").unwrap().unwrap();
        assert_eq!(meta_b.updated_by, "device_a"); // preserved!
        assert_eq!(meta_b.content_hash, envelope.categories[0].content_hash);

        cleanup_store(&dir_a);
        cleanup_store(&dir_b);
    }

    #[test]
    fn test_sync_update_preserves_remote_metadata() {
        let (config_a, dir_a) = make_store_config("a");
        let (config_b, dir_b) = make_store_config("b");

        let mut store_a = Store::new(&config_a).unwrap();
        let mut store_b = Store::new(&config_b).unwrap();

        // B has old version
        store_b
            .write(
                "shared_cat",
                &serde_json::json!({"version": 1}),
                None,
                "device_b",
                None,
            )
            .unwrap();

        // A has new version with A's timestamp
        let meta_a = store_a
            .write(
                "shared_cat",
                &serde_json::json!({"version": 2}),
                None,
                "device_a",
                None,
            )
            .unwrap();

        // Export from A, import into B
        let envelope =
            export_sync_envelope(&store_a, &["shared_cat".to_string()], "device_a").unwrap();
        let result =
            apply_sync_envelope(&mut store_b, &envelope, "remote_wins", "device_b").unwrap();
        assert!(result.pulled.contains(&"shared_cat".to_string()));

        // B's metadata should now reflect A's data
        let meta_b = store_b.get_meta("shared_cat").unwrap().unwrap();
        assert_eq!(meta_b.content_hash, meta_a.content_hash);
        assert_eq!(meta_b.updated_by, "device_a"); // remote updated_by preserved
        assert_eq!(meta_b.updated_at, meta_a.updated_at); // remote updated_at preserved

        cleanup_store(&dir_a);
        cleanup_store(&dir_b);
    }

    #[test]
    fn test_sync_diff_alignment_after_import() {
        let (config_a, dir_a) = make_store_config("a");
        let (config_b, dir_b) = make_store_config("b");

        let mut store_a = Store::new(&config_a).unwrap();
        let mut store_b = Store::new(&config_b).unwrap();

        // A has 2 categories, B has 0
        store_a
            .write("cat1", &serde_json::json!({"v": 1}), None, "device_a", None)
            .unwrap();
        store_a
            .write("cat2", &serde_json::json!({"v": 2}), None, "device_a", None)
            .unwrap();

        let manifest_a = build_manifest(&store_a, "device_a").unwrap();
        let manifest_b = build_manifest(&store_b, "device_b").unwrap();

        // Before sync: B needs to pull everything
        let diff_before = compute_diff(&manifest_b, &manifest_a);
        assert_eq!(diff_before.to_pull.len(), 2);

        // Sync all from A to B
        let names: Vec<String> = manifest_a
            .categories
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let envelope = export_sync_envelope(&store_a, &names, "device_a").unwrap();
        apply_sync_envelope(&mut store_b, &envelope, "remote_wins", "device_b").unwrap();

        // After sync: manifests should align
        let manifest_a2 = build_manifest(&store_a, "device_a").unwrap();
        let manifest_b2 = build_manifest(&store_b, "device_b").unwrap();
        let diff_after = compute_diff(&manifest_b2, &manifest_a2);
        assert!(diff_after.to_pull.is_empty(), "no more pulls needed");
        assert!(diff_after.to_push.is_empty(), "no more pushes needed");
        assert!(diff_after.conflicts.is_empty(), "no conflicts");
        assert_eq!(diff_after.unchanged.len(), 2);

        // Merkle roots should match
        assert_eq!(manifest_a2.merkle_root, manifest_b2.merkle_root);

        cleanup_store(&dir_a);
        cleanup_store(&dir_b);
    }

    #[test]
    fn test_sync_conflict_local_wins() {
        let (config_a, dir_a) = make_store_config("a");
        let (config_b, dir_b) = make_store_config("b");

        let mut store_a = Store::new(&config_a).unwrap();
        let mut store_b = Store::new(&config_b).unwrap();

        // Both have "shared" with different content
        store_a
            .write(
                "shared",
                &serde_json::json!({"src": "a"}),
                None,
                "device_a",
                None,
            )
            .unwrap();
        store_b
            .write(
                "shared",
                &serde_json::json!({"src": "b"}),
                None,
                "device_b",
                None,
            )
            .unwrap();

        let meta_b_before = store_b.get_meta("shared").unwrap().unwrap();

        let envelope = export_sync_envelope(&store_a, &["shared".to_string()], "device_a").unwrap();
        let result =
            apply_sync_envelope(&mut store_b, &envelope, "local_wins", "device_b").unwrap();

        // Local wins: B should keep its data
        assert!(result.pulled.is_empty());
        assert_eq!(result.conflicts.len(), 1);
        assert_eq!(
            result.conflicts[0].resolution,
            ConflictResolution::LocalWins
        );

        let meta_b_after = store_b.get_meta("shared").unwrap().unwrap();
        assert_eq!(meta_b_after.content_hash, meta_b_before.content_hash); // unchanged

        cleanup_store(&dir_a);
        cleanup_store(&dir_b);
    }

    #[test]
    fn test_sync_conflict_remote_wins() {
        let (config_a, dir_a) = make_store_config("a");
        let (config_b, dir_b) = make_store_config("b");

        let mut store_a = Store::new(&config_a).unwrap();
        let mut store_b = Store::new(&config_b).unwrap();

        store_a
            .write(
                "shared",
                &serde_json::json!({"src": "a"}),
                None,
                "device_a",
                None,
            )
            .unwrap();
        store_b
            .write(
                "shared",
                &serde_json::json!({"src": "b"}),
                None,
                "device_b",
                None,
            )
            .unwrap();

        let envelope = export_sync_envelope(&store_a, &["shared".to_string()], "device_a").unwrap();
        let result =
            apply_sync_envelope(&mut store_b, &envelope, "remote_wins", "device_b").unwrap();

        assert_eq!(result.pulled.len(), 1);
        assert_eq!(result.conflicts.len(), 1);
        assert_eq!(
            result.conflicts[0].resolution,
            ConflictResolution::RemoteWins
        );

        // B now has A's data with A's metadata
        let data_b = store_b.read("shared").unwrap();
        assert_eq!(data_b["src"], "a");
        let meta_b = store_b.get_meta("shared").unwrap().unwrap();
        assert_eq!(meta_b.updated_by, "device_a");

        cleanup_store(&dir_a);
        cleanup_store(&dir_b);
    }

    #[test]
    fn test_sync_deleted_category_import() {
        let (config_b, dir_b) = make_store_config("b");

        let mut store_b = Store::new(&config_b).unwrap();

        // B has a category that A says is deleted
        store_b
            .write(
                "to_remove",
                &serde_json::json!({"x": 1}),
                None,
                "device_b",
                None,
            )
            .unwrap();
        assert!(store_b.read("to_remove").is_ok());

        // Envelope with deleted entry
        let envelope = SyncTransferEnvelope {
            source_device: "device_a".to_string(),
            exported_at: "2026-04-10T12:00:00Z".to_string(),
            categories: vec![],
            deleted: vec!["to_remove".to_string()],
        };

        let result =
            apply_sync_envelope(&mut store_b, &envelope, "remote_wins", "device_b").unwrap();
        assert!(result.pulled.iter().any(|s| s.contains("to_remove")));
        assert!(store_b.read("to_remove").is_err()); // deleted
        cleanup_store(&dir_b);
    }

    #[test]
    fn test_export_dirty_envelope_includes_upserts_and_deletes() {
        let (config, dir) = make_store_config("dirty");
        let mut store = Store::new(&config).unwrap();

        store
            .write("cat1", &serde_json::json!({"v": 1}), None, "device_a", None)
            .unwrap();
        store
            .write("cat2", &serde_json::json!({"v": 2}), None, "device_a", None)
            .unwrap();
        store.delete("cat2", "device_a").unwrap();

        let envelope = export_dirty_envelope(&store, "device_a").unwrap();
        assert_eq!(envelope.source_device, "device_a");
        assert_eq!(envelope.categories.len(), 1);
        assert_eq!(envelope.categories[0].name, "cat1");
        assert_eq!(envelope.deleted, vec!["cat2"]);

        cleanup_store(&dir);
    }

    #[test]
    fn test_sync_push_output_accepted_by_pull() {
        let (config_a, dir_a) = make_store_config("a");
        let (config_b, dir_b) = make_store_config("b");

        let mut store_a = Store::new(&config_a).unwrap();
        let store_b = Store::new(&config_b).unwrap();

        store_a
            .write(
                "cat1",
                &serde_json::json!({"val": 42}),
                None,
                "device_a",
                None,
            )
            .unwrap();
        store_a
            .write(
                "cat2",
                &serde_json::json!({"arr": [1, 2, 3]}),
                None,
                "device_a",
                None,
            )
            .unwrap();

        // Push from A
        let envelope = export_sync_envelope(
            &store_a,
            &["cat1".to_string(), "cat2".to_string()],
            "device_a",
        )
        .unwrap();

        // Write to file
        let dir = std::env::temp_dir().join(format!("mcp_sync_file_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("transfer.json");
        write_envelope(&envelope, &path).unwrap();

        // Read back and apply to B
        let loaded = read_envelope(&path).unwrap();
        let mut store_b_mut = store_b;
        let result =
            apply_sync_envelope(&mut store_b_mut, &loaded, "remote_wins", "device_b").unwrap();

        assert_eq!(result.pulled.len(), 2);
        assert_eq!(store_b_mut.read("cat1").unwrap()["val"], 42);
        assert_eq!(
            store_b_mut.read("cat2").unwrap()["arr"],
            serde_json::json!([1, 2, 3])
        );

        // Metadata preserved
        let meta = store_b_mut.get_meta("cat1").unwrap().unwrap();
        assert_eq!(meta.updated_by, "device_a");

        let _ = std::fs::remove_dir_all(&dir);
        cleanup_store(&dir_a);
        cleanup_store(&dir_b);
    }
}
