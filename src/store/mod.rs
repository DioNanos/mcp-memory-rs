pub mod json_files;
pub mod log;
pub mod sqlite;

use crate::config::MemoryConfig;
use crate::error::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryMeta {
    pub name: String,
    pub namespace: String,
    pub content_hash: String,
    pub updated_at: String,
    pub updated_by: String,
    pub write_policy: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionEntry {
    pub id: i64,
    pub category_name: String,
    pub content_hash: String,
    pub reason: Option<String>,
    pub actor: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub category_name: String,
    pub key_path: String,
    pub value_text: String,
    pub snippet: Option<String>,
    pub rank: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirtyEntry {
    pub category_name: String,
    pub operation: String,
    pub content_hash: Option<String>,
    pub marked_at: String,
}

pub struct Store {
    db: rusqlite::Connection,
    config: MemoryConfig,
}

impl Store {
    pub fn new(config: &MemoryConfig) -> Result<Self> {
        let base_dir = config.resolved_base_dir();
        std::fs::create_dir_all(&base_dir)?;
        std::fs::create_dir_all(config.categories_dir())?;
        std::fs::create_dir_all(config.backups_dir())?;

        let db_path = config.db_path();
        let db = rusqlite::Connection::open(&db_path)?;
        let store = Self {
            db,
            config: config.clone(),
        };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<()> {
        self.db.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS category (
                name TEXT PRIMARY KEY,
                namespace TEXT NOT NULL DEFAULT 'default',
                content_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                updated_by TEXT NOT NULL,
                write_policy TEXT NOT NULL DEFAULT 'device',
                size_bytes INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS category_version (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_name TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                diff_json TEXT,
                reason TEXT,
                actor TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (category_name) REFERENCES category(name)
            );

            CREATE TABLE IF NOT EXISTS search_index (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_name TEXT NOT NULL,
                key_path TEXT NOT NULL,
                value_text TEXT,
                FOREIGN KEY (category_name) REFERENCES category(name)
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_dirty (
                category_name TEXT PRIMARY KEY,
                operation TEXT NOT NULL,
                content_hash TEXT,
                marked_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_version_category
                ON category_version(category_name, id DESC);
            CREATE INDEX IF NOT EXISTS idx_search_category
                ON search_index(category_name);
            CREATE INDEX IF NOT EXISTS idx_sync_dirty_marked
                ON sync_dirty(marked_at);
        ",
        )?;

        // FTS5 virtual table — create if not exists (separate to avoid batch issues)
        self.db
            .execute_batch(
                "
            CREATE VIRTUAL TABLE IF NOT EXISTS search_fts USING fts5(
                category_name,
                key_path,
                value_text,
                content='search_index',
                content_rowid='id'
            );
        ",
            )
            .map_err(|e| {
                tracing::warn!("FTS5 creation note (may already exist): {}", e);
                e
            })
            .ok();

        // FTS5 triggers to keep in sync with search_index
        self.db.execute_batch(
            "
            CREATE TRIGGER IF NOT EXISTS search_fts_ai AFTER INSERT ON search_index BEGIN
                INSERT INTO search_fts(rowid, category_name, key_path, value_text)
                    VALUES (new.id, new.category_name, new.key_path, new.value_text);
            END;

            CREATE TRIGGER IF NOT EXISTS search_fts_ad AFTER DELETE ON search_index BEGIN
                INSERT INTO search_fts(search_fts, rowid, category_name, key_path, value_text)
                    VALUES ('delete', old.id, old.category_name, old.key_path, old.value_text);
            END;
        ",
        )?;

        // Schema version tracking
        let schema_ver: i64 = self
            .db
            .query_row(
                "SELECT CAST(COALESCE(value, '0') AS INTEGER) FROM sync_state WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        if schema_ver < 2 {
            tracing::info!("Upgrading schema to v2 (FTS5)...");
            self.rebuild_fts_index()?;
            self.db.execute(
                "INSERT OR REPLACE INTO sync_state (key, value, updated_at) VALUES ('schema_version', '2', ?1)",
                [chrono::Utc::now().to_rfc3339()],
            )?;
        }

        Ok(())
    }

    fn rebuild_fts_index(&self) -> Result<()> {
        // Clear and rebuild FTS from search_index data
        self.db
            .execute("INSERT INTO search_fts(search_fts) VALUES ('rebuild')", [])?;
        tracing::info!("FTS5 index rebuilt");
        Ok(())
    }

    pub fn read(&self, name: &str) -> Result<serde_json::Value> {
        let path = self.config.categories_dir().join(format!("{}.json", name));
        if !path.exists() {
            // Check if exists in DB but no file
            let count: i64 = self.db.query_row(
                "SELECT COUNT(*) FROM category WHERE name = ?1",
                [name],
                |r| r.get(0),
            )?;
            if count == 0 {
                return Err(self.not_found(name));
            }
        }
        let content = std::fs::read_to_string(&path)
            .map_err(|_| crate::error::MemoryError::NotFound(name.into()))?;
        let value: serde_json::Value = serde_json::from_str(&content)?;
        Ok(value)
    }

    /// Build a teaching NotFound error that lists the available category names
    /// (capped) so a calling AI can recover without a separate memory_list.
    fn not_found(&self, name: &str) -> crate::error::MemoryError {
        const MAX_LISTED: usize = 30;
        match self.list() {
            Ok(cats) if !cats.is_empty() => {
                let total = cats.len();
                let mut names: Vec<String> = cats.into_iter().map(|c| c.name).collect();
                names.truncate(MAX_LISTED);
                let mut listed = names.join(", ");
                if total > MAX_LISTED {
                    listed.push_str(&format!(", … (+{} more)", total - MAX_LISTED));
                }
                crate::error::MemoryError::NotFound(format!("{name}; available: [{listed}]"))
            }
            _ => crate::error::MemoryError::NotFound(format!("{name}; no categories exist yet")),
        }
    }

    pub fn read_with_meta(&self, name: &str) -> Result<(serde_json::Value, CategoryMeta)> {
        let value = self.read(name)?;
        let meta = self
            .get_meta(name)?
            .ok_or_else(|| crate::error::MemoryError::NotFound(name.into()))?;
        Ok((value, meta))
    }

    pub fn write(
        &mut self,
        name: &str,
        value: &serde_json::Value,
        reason: Option<&str>,
        actor: &str,
        expected_hash: Option<&str>,
    ) -> Result<CategoryMeta> {
        // Validate name
        if name.is_empty() || name.contains('/') || name.contains('\\') || name.contains("..") {
            return Err(crate::error::MemoryError::InvalidName(name.into()));
        }

        let json_str = serde_json::to_string_pretty(value)?;
        let content_hash = Self::compute_hash(&json_str);
        let now = chrono::Utc::now().to_rfc3339();
        let size_bytes = json_str.len() as u64;

        // Check optimistic concurrency if hash provided
        if let Some(expected) = expected_hash {
            if let Ok(Some(meta)) = self.get_meta(name) {
                if meta.content_hash != expected {
                    return Err(crate::error::MemoryError::ConcurrencyConflict {
                        expected: expected.into(),
                        actual: meta.content_hash,
                    });
                }
            }
        }

        let namespace = "default";
        let write_policy = "device";

        // Backup existing file before overwriting
        let path = self.config.categories_dir().join(format!("{}.json", name));
        if path.exists() {
            let ts = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S");
            let backup_path = self
                .config
                .backups_dir()
                .join(format!("{}.{}.json", name, ts));
            let _ = std::fs::copy(&path, &backup_path);
        }

        // Write JSON file
        std::fs::write(&path, &json_str)?;

        // Upsert in SQLite
        let existing: Option<String> = self
            .db
            .query_row(
                "SELECT content_hash FROM category WHERE name = ?1",
                [name],
                |r| r.get(0),
            )
            .ok();

        if existing.is_some() {
            self.db.execute(
                "UPDATE category SET content_hash = ?1, updated_at = ?2, updated_by = ?3, size_bytes = ?4 WHERE name = ?5",
                rusqlite::params![content_hash, now, actor, size_bytes, name],
            )?;
        } else {
            self.db.execute(
                "INSERT INTO category (name, namespace, content_hash, updated_at, updated_by, write_policy, size_bytes) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![name, namespace, content_hash, now, actor, write_policy, size_bytes],
            )?;
        }

        // Version entry
        self.db.execute(
            "INSERT INTO category_version (category_name, content_hash, reason, actor, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![name, content_hash, reason, actor, now],
        )?;

        // Update search index
        self.update_search_index(name, value)?;
        self.mark_dirty(name, "upsert", Some(&content_hash))?;

        // Prune old versions
        self.prune_versions(name)?;

        let hash_short = content_hash[..8.min(content_hash.len())].to_string();
        let meta = CategoryMeta {
            name: name.into(),
            namespace: namespace.into(),
            content_hash,
            updated_at: now,
            updated_by: actor.into(),
            write_policy: write_policy.into(),
            size_bytes,
        };

        tracing::info!(
            "Write category '{}' ({} bytes, hash={})",
            name,
            size_bytes,
            hash_short
        );
        Ok(meta)
    }

    /// True if `name` exists and is a log-kind category. Missing category = false.
    pub fn is_log_category(&self, name: &str) -> Result<bool> {
        match self.read(name) {
            Ok(v) => Ok(log::is_log(&v)),
            Err(crate::error::MemoryError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Append `data` as a server-stamped entry to a log category (creating it if
    /// missing), applying retention, then persisting via the normal versioned
    /// write path. Errors if the category exists and is not a log.
    pub fn append(
        &mut self,
        name: &str,
        data: serde_json::Value,
        retention: Option<log::Retention>,
        actor: &str,
    ) -> Result<CategoryMeta> {
        let existing = match self.read(name) {
            Ok(v) => Some(v),
            Err(crate::error::MemoryError::NotFound(_)) => None,
            Err(e) => return Err(e),
        };

        // Effective retention: explicit override > stored on the log > default.
        let effective = retention
            .or_else(|| existing.as_ref().and_then(log::stored_retention))
            .unwrap_or_default();

        let now = chrono::Utc::now().to_rfc3339();
        let new_value =
            log::append_entry(existing.as_ref(), data, effective, &now).map_err(|e| match e {
                log::LogError::NotALog => crate::error::MemoryError::KindMismatch(format!(
                    "category '{name}' is kind=memory; use memory_write, or migrate it to a log"
                )),
                log::LogError::MalformedLog => crate::error::MemoryError::KindMismatch(format!(
                    "category '{name}' is marked as a log but is structurally malformed"
                )),
            })?;

        self.write(name, &new_value, Some("append"), actor, None)
    }

    pub fn list(&self) -> Result<Vec<CategoryMeta>> {
        let mut stmt = self.db.prepare(
            "SELECT name, namespace, content_hash, updated_at, updated_by, write_policy, size_bytes FROM category ORDER BY name"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(CategoryMeta {
                name: row.get(0)?,
                namespace: row.get(1)?,
                content_hash: row.get(2)?,
                updated_at: row.get(3)?,
                updated_by: row.get(4)?,
                write_policy: row.get(5)?,
                size_bytes: row.get(6)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn delete(&mut self, name: &str, actor: &str) -> Result<()> {
        // Backup before delete
        let path = self.config.categories_dir().join(format!("{}.json", name));
        if path.exists() {
            let ts = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S");
            let backup_path = self
                .config
                .backups_dir()
                .join(format!("{}.{}.deleted.json", name, ts));
            let _ = std::fs::copy(&path, &backup_path);
            std::fs::remove_file(&path)?;
        }

        self.db
            .execute("DELETE FROM search_index WHERE category_name = ?1", [name])?;
        self.db.execute(
            "DELETE FROM category_version WHERE category_name = ?1",
            [name],
        )?;
        self.db
            .execute("DELETE FROM category WHERE name = ?1", [name])?;
        self.mark_dirty(name, "delete", None)?;
        tracing::info!("Delete category '{}' by {}", name, actor);
        Ok(())
    }

    pub fn get_meta(&self, name: &str) -> Result<Option<CategoryMeta>> {
        let result = self.db.query_row(
            "SELECT name, namespace, content_hash, updated_at, updated_by, write_policy, size_bytes FROM category WHERE name = ?1",
            [name],
            |row| {
                Ok(CategoryMeta {
                    name: row.get(0)?,
                    namespace: row.get(1)?,
                    content_hash: row.get(2)?,
                    updated_at: row.get(3)?,
                    updated_by: row.get(4)?,
                    write_policy: row.get(5)?,
                    size_bytes: row.get(6)?,
                })
            },
        );
        match result {
            Ok(meta) => Ok(Some(meta)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn history(&self, name: &str, limit: u32) -> Result<Vec<VersionEntry>> {
        let mut stmt = self.db.prepare(
            "SELECT id, category_name, content_hash, reason, actor, timestamp FROM category_version WHERE category_name = ?1 ORDER BY id DESC LIMIT ?2"
        )?;
        let rows = stmt.query_map(rusqlite::params![name, limit], |row| {
            Ok(VersionEntry {
                id: row.get(0)?,
                category_name: row.get(1)?,
                content_hash: row.get(2)?,
                reason: row.get(3)?,
                actor: row.get(4)?,
                timestamp: row.get(5)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn search(&self, query: &str, limit: u32) -> Result<Vec<SearchResult>> {
        self.search_advanced(query, None, None, None, None, limit)
    }

    /// Advanced FTS5 search with BM25 ranking, filters, and snippet highlighting.
    pub fn search_advanced(
        &self,
        query: &str,
        category: Option<&str>,
        updated_after: Option<&str>,
        updated_before: Option<&str>,
        actor: Option<&str>,
        limit: u32,
    ) -> Result<Vec<SearchResult>> {
        // Try FTS5 first
        let results =
            self.search_fts(query, category, updated_after, updated_before, actor, limit)?;
        if !results.is_empty() {
            return Ok(results);
        }

        // Fallback to LIKE search
        let pattern = format!("%{}%", query.replace('%', "\\%").replace('_', "\\_"));
        let mut sql = String::from(
            "SELECT si.category_name, si.key_path, si.value_text, 1.0 as rank FROM search_index si",
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![];

        if category.is_some()
            || updated_after.is_some()
            || updated_before.is_some()
            || actor.is_some()
        {
            sql.push_str(" JOIN category c ON si.category_name = c.name");
        }

        sql.push_str(" WHERE si.value_text LIKE ?1");
        params.push(Box::new(pattern));

        if let Some(cat) = category {
            sql.push_str(" AND si.category_name = ?");
            params.push(Box::new(cat.to_string()));
        }
        if let Some(after) = updated_after {
            sql.push_str(" AND c.updated_at >= ?");
            params.push(Box::new(after.to_string()));
        }
        if let Some(before) = updated_before {
            sql.push_str(" AND c.updated_at <= ?");
            params.push(Box::new(before.to_string()));
        }
        if let Some(act) = actor {
            sql.push_str(" AND c.updated_by = ?");
            params.push(Box::new(act.to_string()));
        }
        sql.push_str(" ORDER BY rank LIMIT ?");

        let limit_i64 = limit as i64;
        params.push(Box::new(limit_i64));

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.db.prepare(&sql)?;
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok(SearchResult {
                category_name: row.get(0)?,
                key_path: row.get(1)?,
                value_text: row.get(2)?,
                snippet: None,
                rank: row.get(3)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }

        // Last fallback: scan files directly
        if results.is_empty() {
            results = self.search_in_files(query, limit)?;
        }

        Ok(results)
    }

    fn search_fts(
        &self,
        query: &str,
        category: Option<&str>,
        updated_after: Option<&str>,
        updated_before: Option<&str>,
        actor: Option<&str>,
        limit: u32,
    ) -> Result<Vec<SearchResult>> {
        // Build FTS5 query with snippet and BM25 ranking
        let fts_query = query
            .split_whitespace()
            .filter(|w| !w.is_empty())
            .map(|w| format!("\"{}\"*", w.replace('"', "")))
            .collect::<Vec<_>>()
            .join(" OR ");

        if fts_query.is_empty() {
            return Ok(vec![]);
        }

        let mut sql = String::from(
            "SELECT fts.category_name, fts.key_path, fts.value_text, \
             bm25(search_fts) as rank, \
             snippet(search_fts, 2, '⟨', '⟩', '…', 32) as snippet \
             FROM search_fts fts",
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![];

        // Add category join if filters need it
        let need_join = category.is_some()
            || updated_after.is_some()
            || updated_before.is_some()
            || actor.is_some();
        if need_join {
            sql.push_str(" JOIN category c ON fts.category_name = c.name");
        }

        sql.push_str(" WHERE search_fts MATCH ?");
        params.push(Box::new(fts_query));

        if let Some(cat) = category {
            sql.push_str(" AND fts.category_name = ?");
            params.push(Box::new(cat.to_string()));
        }
        if let Some(after) = updated_after {
            sql.push_str(" AND c.updated_at >= ?");
            params.push(Box::new(after.to_string()));
        }
        if let Some(before) = updated_before {
            sql.push_str(" AND c.updated_at <= ?");
            params.push(Box::new(before.to_string()));
        }
        if let Some(act) = actor {
            sql.push_str(" AND c.updated_by = ?");
            params.push(Box::new(act.to_string()));
        }

        sql.push_str(" ORDER BY rank LIMIT ?");
        params.push(Box::new(limit as i64));

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.db.prepare(&sql)?;

        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            let rank: f64 = row.get(3)?;
            Ok(SearchResult {
                category_name: row.get(0)?,
                key_path: row.get(1)?,
                value_text: row.get(2)?,
                snippet: row.get(4)?,
                rank: -rank, // BM25 returns negative for better ranking
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            match row {
                Ok(r) => results.push(r),
                Err(e) => {
                    tracing::warn!("FTS search row error: {}", e);
                    // FTS might not be available, return empty to trigger fallback
                    return Ok(vec![]);
                }
            }
        }
        Ok(results)
    }

    fn search_in_files(&self, query: &str, limit: u32) -> Result<Vec<SearchResult>> {
        let query_lower = query.to_lowercase();
        let categories = self.list()?;
        let mut results = Vec::new();

        for cat in &categories {
            if let Ok(value) = self.read(&cat.name) {
                self.search_recursive(&query_lower, &cat.name, "", &value, &mut results, limit);
            }
            if results.len() >= limit as usize {
                break;
            }
        }
        Ok(results)
    }

    fn search_recursive(
        &self,
        query: &str,
        category: &str,
        path: &str,
        value: &serde_json::Value,
        results: &mut Vec<SearchResult>,
        limit: u32,
    ) {
        if results.len() >= limit as usize {
            return;
        }
        match value {
            serde_json::Value::String(s) if s.to_lowercase().contains(query) => {
                results.push(SearchResult {
                    category_name: category.into(),
                    key_path: path.into(),
                    value_text: s.clone(),
                    snippet: None,
                    rank: 1.0,
                });
            }
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    let child_path = if path.is_empty() {
                        k.clone()
                    } else {
                        format!("{}.{}", path, k)
                    };
                    self.search_recursive(query, category, &child_path, v, results, limit);
                }
            }
            serde_json::Value::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    let child_path = format!("{}[{}]", path, i);
                    self.search_recursive(query, category, &child_path, v, results, limit);
                }
            }
            _ => {}
        }
    }

    fn update_search_index(&self, name: &str, value: &serde_json::Value) -> Result<()> {
        self.db
            .execute("DELETE FROM search_index WHERE category_name = ?1", [name])?;
        self.index_recursive(name, "", value)?;
        Ok(())
    }

    fn index_recursive(&self, category: &str, path: &str, value: &serde_json::Value) -> Result<()> {
        match value {
            serde_json::Value::String(s) if !s.is_empty() => {
                self.db.execute(
                        "INSERT INTO search_index (category_name, key_path, value_text) VALUES (?1, ?2, ?3)",
                        rusqlite::params![category, path, s],
                    )?;
            }
            serde_json::Value::Number(n) => {
                self.db.execute(
                    "INSERT INTO search_index (category_name, key_path, value_text) VALUES (?1, ?2, ?3)",
                    rusqlite::params![category, path, n.to_string()],
                )?;
            }
            serde_json::Value::Object(map) => {
                for (k, v) in map {
                    let child_path = if path.is_empty() {
                        k.clone()
                    } else {
                        format!("{}.{}", path, k)
                    };
                    self.index_recursive(category, &child_path, v)?;
                }
            }
            serde_json::Value::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    let child_path = format!("{}[{}]", path, i);
                    self.index_recursive(category, &child_path, v)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn prune_versions(&self, name: &str) -> Result<()> {
        let max = self.config.storage.max_versions;
        self.db.execute(
            "DELETE FROM category_version WHERE category_name = ?1 AND id NOT IN (SELECT id FROM category_version WHERE category_name = ?2 ORDER BY id DESC LIMIT ?3)",
            rusqlite::params![name, name, max],
        )?;
        Ok(())
    }

    pub fn stats(&self) -> Result<serde_json::Value> {
        let categories: i64 = self
            .db
            .query_row("SELECT COUNT(*) FROM category", [], |r| r.get(0))?;
        let versions: i64 =
            self.db
                .query_row("SELECT COUNT(*) FROM category_version", [], |r| r.get(0))?;
        let index_entries: i64 =
            self.db
                .query_row("SELECT COUNT(*) FROM search_index", [], |r| r.get(0))?;

        let total_size: u64 = self.list()?.iter().map(|c| c.size_bytes).sum();
        let dirty_count: i64 = self
            .db
            .query_row("SELECT COUNT(*) FROM sync_dirty", [], |r| r.get(0))?;

        Ok(serde_json::json!({
            "categories": categories,
            "total_versions": versions,
            "search_index_entries": index_entries,
            "dirty_categories": dirty_count,
            "total_size_bytes": total_size,
            "total_size_human": format_size(total_size),
        }))
    }

    pub fn read_version(&self, version_id: i64) -> Result<Option<serde_json::Value>> {
        // Find the version and reconstruct from backup or compute
        let version: VersionEntry = match self.db.query_row(
            "SELECT id, category_name, content_hash, reason, actor, timestamp FROM category_version WHERE id = ?1",
            [version_id],
            |row| Ok(VersionEntry {
                id: row.get(0)?,
                category_name: row.get(1)?,
                content_hash: row.get(2)?,
                reason: row.get(3)?,
                actor: row.get(4)?,
                timestamp: row.get(5)?,
            }),
        ) {
            Ok(v) => v,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        // Try to find backup with matching timestamp
        let backups_dir = self.config.backups_dir();
        let entries = std::fs::read_dir(&backups_dir)?;
        for entry in entries {
            let entry = entry?;
            let fname = entry.file_name().to_string_lossy().to_string();
            if fname.starts_with(&version.category_name) && fname.contains(&version.timestamp[..10])
            {
                let content = std::fs::read_to_string(entry.path())?;
                let value: serde_json::Value = serde_json::from_str(&content)?;
                let hash = Self::compute_hash(&serde_json::to_string_pretty(&value)?);
                if hash == version.content_hash {
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }

    /// Import a category from external source (migration). Does NOT create backup.
    pub fn import_category(
        &self,
        name: &str,
        value: &serde_json::Value,
        actor: &str,
    ) -> Result<CategoryMeta> {
        // Kind boundary: an import (e.g. Node.js migration) must not silently
        // clobber an existing append-only log with a declarative value. Fail
        // closed — if the kind cannot be determined (corrupt file), abort the
        // import rather than overwrite.
        if !log::is_log(value) && self.is_log_category(name)? {
            return Err(crate::error::MemoryError::KindMismatch(format!(
                "category '{name}' is kind=log; refusing to overwrite it with a non-log import"
            )));
        }

        let json_str = serde_json::to_string_pretty(value)?;
        let content_hash = Self::compute_hash(&json_str);
        let now = chrono::Utc::now().to_rfc3339();
        let size_bytes = json_str.len() as u64;
        let namespace = "default";
        let write_policy = "device";

        // Check if already exists
        let existing: Option<String> = self
            .db
            .query_row(
                "SELECT content_hash FROM category WHERE name = ?1",
                [name],
                |r| r.get(0),
            )
            .ok();

        if let Some(ref hash) = existing {
            if hash == &content_hash {
                // Same content, skip
                return self
                    .get_meta(name)?
                    .ok_or_else(|| crate::error::MemoryError::Other("import meta missing".into()));
            }
        }

        // Write JSON file
        let path = self.config.categories_dir().join(format!("{}.json", name));
        std::fs::write(&path, &json_str)?;

        // Upsert in SQLite
        if existing.is_some() {
            self.db.execute(
                "UPDATE category SET content_hash = ?1, updated_at = ?2, updated_by = ?3, size_bytes = ?4 WHERE name = ?5",
                rusqlite::params![content_hash, now, actor, size_bytes, name],
            )?;
        } else {
            self.db.execute(
                "INSERT INTO category (name, namespace, content_hash, updated_at, updated_by, write_policy, size_bytes) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![name, namespace, content_hash, now, actor, write_policy, size_bytes],
            )?;
        }

        // Version entry
        self.db.execute(
            "INSERT INTO category_version (category_name, content_hash, reason, actor, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![name, content_hash, "Import from Node.js", actor, now],
        )?;

        // Update search index
        self.update_search_index(name, value)?;

        tracing::info!("Imported category '{}' ({} bytes)", name, size_bytes);

        Ok(CategoryMeta {
            name: name.into(),
            namespace: namespace.into(),
            content_hash,
            updated_at: now,
            updated_by: actor.into(),
            write_policy: write_policy.into(),
            size_bytes,
        })
    }

    pub fn dirty_entries(&self) -> Result<Vec<DirtyEntry>> {
        let mut stmt = self.db.prepare(
            "SELECT category_name, operation, content_hash, marked_at FROM sync_dirty ORDER BY marked_at, category_name",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(DirtyEntry {
                category_name: row.get(0)?,
                operation: row.get(1)?,
                content_hash: row.get(2)?,
                marked_at: row.get(3)?,
            })
        })?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    pub fn clear_dirty<I, S>(&self, categories: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for category in categories {
            self.db.execute(
                "DELETE FROM sync_dirty WHERE category_name = ?1",
                [category.as_ref()],
            )?;
        }
        Ok(())
    }

    pub fn set_sync_state(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        self.db.execute(
            "INSERT OR REPLACE INTO sync_state (key, value, updated_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                key,
                serde_json::to_string(value)?,
                chrono::Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    }

    pub fn get_sync_state(&self, key: &str) -> Result<Option<serde_json::Value>> {
        let value: std::result::Result<String, rusqlite::Error> = self.db.query_row(
            "SELECT value FROM sync_state WHERE key = ?1",
            [key],
            |row| row.get(0),
        );
        match value {
            Ok(raw) => Ok(Some(serde_json::from_str(&raw)?)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn sync_status(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "stats": self.stats()?,
            "dirty": self.dirty_entries()?,
            "last_sync": self.get_sync_state("last_sync")?,
        }))
    }

    fn mark_dirty(&self, name: &str, operation: &str, content_hash: Option<&str>) -> Result<()> {
        self.db.execute(
            "INSERT OR REPLACE INTO sync_dirty (category_name, operation, content_hash, marked_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![name, operation, content_hash, chrono::Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// Insert or update a category from a sync operation, preserving remote metadata.
    /// Unlike write(), this does NOT overwrite updated_at/updated_by with local values.
    /// It stores the remote content_hash, updated_at, and updated_by as-is.
    pub fn upsert_synced_category(
        &mut self,
        name: &str,
        value: &serde_json::Value,
        remote_content_hash: &str,
        remote_updated_at: &str,
        remote_updated_by: &str,
        reason: Option<&str>,
    ) -> Result<CategoryMeta> {
        if name.is_empty() || name.contains('/') || name.contains('\\') || name.contains("..") {
            return Err(crate::error::MemoryError::InvalidName(name.into()));
        }

        let json_str = serde_json::to_string_pretty(value)?;
        let size_bytes = json_str.len() as u64;
        let namespace = "default";
        let write_policy = "device";

        // Backup existing file before overwriting
        let path = self.config.categories_dir().join(format!("{}.json", name));
        if path.exists() {
            let ts = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S");
            let backup_path = self
                .config
                .backups_dir()
                .join(format!("{}.{}.json", name, ts));
            let _ = std::fs::copy(&path, &backup_path);
        }

        // Write JSON file
        std::fs::write(&path, &json_str)?;

        // Upsert in SQLite with REMOTE metadata
        let existing: Option<String> = self
            .db
            .query_row(
                "SELECT content_hash FROM category WHERE name = ?1",
                [name],
                |r| r.get(0),
            )
            .ok();

        if existing.is_some() {
            self.db.execute(
                "UPDATE category SET content_hash = ?1, updated_at = ?2, updated_by = ?3, size_bytes = ?4 WHERE name = ?5",
                rusqlite::params![remote_content_hash, remote_updated_at, remote_updated_by, size_bytes, name],
            )?;
        } else {
            self.db.execute(
                "INSERT INTO category (name, namespace, content_hash, updated_at, updated_by, write_policy, size_bytes) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![name, namespace, remote_content_hash, remote_updated_at, remote_updated_by, write_policy, size_bytes],
            )?;
        }

        // Version entry
        let now = chrono::Utc::now().to_rfc3339();
        let reason_text = reason.unwrap_or("Sync import");
        self.db.execute(
            "INSERT INTO category_version (category_name, content_hash, reason, actor, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![name, remote_content_hash, reason_text, remote_updated_by, now],
        )?;

        // Update search index
        self.update_search_index(name, value)?;

        tracing::info!(
            "Sync upsert category '{}' ({} bytes, remote_at={}, remote_by={})",
            name,
            size_bytes,
            remote_updated_at,
            remote_updated_by
        );

        Ok(CategoryMeta {
            name: name.into(),
            namespace: namespace.into(),
            content_hash: remote_content_hash.into(),
            updated_at: remote_updated_at.into(),
            updated_by: remote_updated_by.into(),
            write_policy: write_policy.into(),
            size_bytes,
        })
    }

    pub fn get_all_indexed_text(&self) -> Result<Vec<String>> {
        let mut stmt = self.db.prepare("SELECT value_text FROM search_index")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut texts = Vec::new();
        for row in rows {
            texts.push(row?);
        }
        Ok(texts)
    }

    pub fn get_all_indexed_entries(&self) -> Result<Vec<(String, String, String)>> {
        let mut stmt = self
            .db
            .prepare("SELECT category_name, key_path, value_text FROM search_index")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    fn compute_hash(data: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MemoryConfig, ServerConfig, StorageConfig};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_config(label: &str) -> (MemoryConfig, PathBuf) {
        let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "mcp_test_store_{}_{}_{}",
            label,
            std::process::id(),
            id
        ));
        let _ = std::fs::remove_dir_all(&dir);
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
            acl: crate::config::AclConfig::default(),
            sync: crate::config::SyncConfig::default(),
        };
        (config, dir)
    }

    fn cleanup(dir: &PathBuf) {
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_store_append_creates_log_and_appends() {
        let (config, dir) = unique_config("append");
        let mut store = Store::new(&config).unwrap();

        store
            .append("sess_log", serde_json::json!({"n": 1}), None, "tester")
            .unwrap();
        store
            .append("sess_log", serde_json::json!({"n": 2}), None, "tester")
            .unwrap();

        let v = store.read("sess_log").unwrap();
        assert!(log::is_log(&v));
        let entries = v["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1]["data"], serde_json::json!({"n": 2}));
        assert!(
            entries[1]["ts"].as_str().unwrap().contains('T'),
            "server stamps ts"
        );
        assert!(store.is_log_category("sess_log").unwrap());
        cleanup(&dir);
    }

    #[test]
    fn test_store_append_rejects_memory_category() {
        let (config, dir) = unique_config("append_rej");
        let mut store = Store::new(&config).unwrap();

        store
            .write(
                "mem_cat",
                &serde_json::json!({"state": "ok"}),
                None,
                "tester",
                None,
            )
            .unwrap();
        let err = store
            .append("mem_cat", serde_json::json!({"x": 1}), None, "tester")
            .unwrap_err();
        assert!(matches!(err, crate::error::MemoryError::KindMismatch(_)));
        assert!(!store.is_log_category("mem_cat").unwrap());
        cleanup(&dir);
    }

    #[test]
    fn test_store_append_caps_at_max_entries() {
        let (config, dir) = unique_config("append_cap");
        let mut store = Store::new(&config).unwrap();

        let ret = Some(log::Retention {
            max_entries: Some(3),
            max_age_days: None,
        });
        for n in 0..5 {
            store
                .append("capped", serde_json::json!({"n": n}), ret, "tester")
                .unwrap();
        }
        let v = store.read("capped").unwrap();
        let entries = v["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 3, "bounded log");
        assert_eq!(
            entries[0]["data"],
            serde_json::json!({"n": 2}),
            "oldest dropped"
        );
        cleanup(&dir);
    }

    #[test]
    fn test_import_does_not_clobber_existing_log() {
        let (config, dir) = unique_config("import_log");
        let mut store = Store::new(&config).unwrap();

        store
            .append("evt", serde_json::json!({"n": 1}), None, "tester")
            .unwrap();
        // A migration importing a plain memory value over the log name must fail.
        let err = store
            .import_category("evt", &serde_json::json!({"state": "ok"}), "migration")
            .unwrap_err();
        assert!(matches!(err, crate::error::MemoryError::KindMismatch(_)));
        // log left intact
        let v = store.read("evt").unwrap();
        assert!(log::is_log(&v));
        assert_eq!(v["entries"].as_array().unwrap().len(), 1);
        cleanup(&dir);
    }

    #[test]
    fn test_import_fails_closed_on_corrupt_target() {
        let (config, dir) = unique_config("import_corrupt");
        let cat_path = config.categories_dir().join("evt.json");
        let mut store = Store::new(&config).unwrap();

        store
            .append("evt", serde_json::json!({"n": 1}), None, "tester")
            .unwrap();
        std::fs::write(&cat_path, "{ not valid json").unwrap();

        let err = store
            .import_category("evt", &serde_json::json!({"state": "ok"}), "migration")
            .unwrap_err();
        // must NOT degrade to "not a log" and clobber: any non-NotFound read
        // error aborts the import.
        assert!(!matches!(err, crate::error::MemoryError::NotFound(_)));
        cleanup(&dir);
    }

    #[test]
    fn test_store_create_and_read() {
        let (config, dir) = unique_config("test");
        let mut store = Store::new(&config).unwrap();

        let data = serde_json::json!({"key": "value", "num": 42});
        store
            .write("test_cat", &data, None, "test_actor", None)
            .unwrap();

        let read_back = store.read("test_cat").unwrap();
        assert_eq!(read_back["key"], "value");
        assert_eq!(read_back["num"], 42);
        cleanup(&dir);
    }

    #[test]
    fn test_store_write_creates_backup() {
        let (config, dir) = unique_config("test");
        let mut store = Store::new(&config).unwrap();

        let v1 = serde_json::json!({"version": 1});
        store
            .write("backup_test", &v1, None, "actor", None)
            .unwrap();

        let v2 = serde_json::json!({"version": 2});
        store
            .write("backup_test", &v2, None, "actor", None)
            .unwrap();

        // Backup dir should have at least one file
        let backups = std::fs::read_dir(config.backups_dir()).unwrap();
        let count = backups.count();
        assert!(count >= 1, "expected at least 1 backup, got {}", count);
        cleanup(&dir);
    }

    #[test]
    fn test_store_delete_removes_file() {
        let (config, dir) = unique_config("test");
        let mut store = Store::new(&config).unwrap();

        store
            .write(
                "to_delete",
                &serde_json::json!({"x": 1}),
                None,
                "actor",
                None,
            )
            .unwrap();
        assert!(config.categories_dir().join("to_delete.json").exists());

        store.delete("to_delete", "actor").unwrap();
        assert!(!config.categories_dir().join("to_delete.json").exists());
        assert!(store.read("to_delete").is_err());
        cleanup(&dir);
    }

    #[test]
    fn test_store_tracks_dirty_writes_and_deletes() {
        let (config, dir) = unique_config("dirty");
        let mut store = Store::new(&config).unwrap();

        store
            .write(
                "dirty_cat",
                &serde_json::json!({"x": 1}),
                None,
                "actor",
                None,
            )
            .unwrap();
        let dirty = store.dirty_entries().unwrap();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].category_name, "dirty_cat");
        assert_eq!(dirty[0].operation, "upsert");
        assert!(dirty[0].content_hash.is_some());

        store.delete("dirty_cat", "actor").unwrap();
        let dirty = store.dirty_entries().unwrap();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].category_name, "dirty_cat");
        assert_eq!(dirty[0].operation, "delete");
        assert!(dirty[0].content_hash.is_none());

        store.clear_dirty(["dirty_cat"]).unwrap();
        assert!(store.dirty_entries().unwrap().is_empty());
        cleanup(&dir);
    }

    #[test]
    fn test_store_concurrency_conflict() {
        let (config, dir) = unique_config("test");
        let mut store = Store::new(&config).unwrap();

        store
            .write(
                "conflict_test",
                &serde_json::json!({"v": 1}),
                None,
                "actor",
                None,
            )
            .unwrap();

        let result = store.write(
            "conflict_test",
            &serde_json::json!({"v": 2}),
            None,
            "actor",
            Some("wrong_hash"),
        );
        assert!(result.is_err());
        cleanup(&dir);
    }

    #[test]
    fn test_store_invalid_name() {
        let (config, dir) = unique_config("test");
        let mut store = Store::new(&config).unwrap();

        for bad_name in &["", "has/slash", "has\\backslash", "has..dots"] {
            let result = store.write(bad_name, &serde_json::json!({}), None, "actor", None);
            assert!(result.is_err(), "expected error for name '{}'", bad_name);
        }
        cleanup(&dir);
    }

    #[test]
    fn test_store_search_basic() {
        let (config, dir) = unique_config("test");
        let mut store = Store::new(&config).unwrap();

        store
            .write(
                "search_test",
                &serde_json::json!({
                    "title": "unicorno magico nella foresta",
                    "desc": "una descrizione generica"
                }),
                None,
                "actor",
                None,
            )
            .unwrap();

        let results = store.search("unicorno", 10).unwrap();
        assert!(!results.is_empty(), "search should find 'unicorno'");
        assert_eq!(results[0].category_name, "search_test");
        cleanup(&dir);
    }

    #[test]
    fn test_store_history() {
        let (config, dir) = unique_config("test");
        let mut store = Store::new(&config).unwrap();

        store
            .write(
                "hist",
                &serde_json::json!({"v": 1}),
                Some("first"),
                "actor",
                None,
            )
            .unwrap();
        store
            .write(
                "hist",
                &serde_json::json!({"v": 2}),
                Some("second"),
                "actor",
                None,
            )
            .unwrap();

        let history = store.history("hist", 10).unwrap();
        assert!(history.len() >= 2, "should have at least 2 history entries");
        cleanup(&dir);
    }

    #[test]
    fn test_store_stats() {
        let (config, dir) = unique_config("test");
        let mut store = Store::new(&config).unwrap();

        store
            .write(
                "stat_cat",
                &serde_json::json!({"data": "test"}),
                None,
                "actor",
                None,
            )
            .unwrap();

        let stats = store.stats().unwrap();
        assert!(stats["categories"].as_i64().unwrap() >= 1);
        assert!(stats["total_size_bytes"].as_i64().unwrap() > 0);
        cleanup(&dir);
    }

    #[test]
    fn test_store_list_categories() {
        let (config, dir) = unique_config("test");
        let mut store = Store::new(&config).unwrap();

        store
            .write("cat_a", &serde_json::json!({"a": 1}), None, "actor", None)
            .unwrap();
        store
            .write("cat_b", &serde_json::json!({"b": 2}), None, "actor", None)
            .unwrap();

        let list = store.list().unwrap();
        let names: Vec<&str> = list.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"cat_a"));
        assert!(names.contains(&"cat_b"));
        cleanup(&dir);
    }

    #[test]
    fn test_store_read_nonexistent() {
        let (config, dir) = unique_config("test");
        let store = Store::new(&config).unwrap();

        let result = store.read("does_not_exist");
        assert!(result.is_err());
        cleanup(&dir);
    }

    #[test]
    fn test_read_missing_lists_available_categories() {
        let (config, dir) = unique_config("notfound");
        let mut store = Store::new(&config).unwrap();
        store
            .write("base", &serde_json::json!({"a": 1}), None, "actor", None)
            .unwrap();
        store
            .write(
                "projects",
                &serde_json::json!({"b": 2}),
                None,
                "actor",
                None,
            )
            .unwrap();

        let err = store.read("nope").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("nope"), "names the missing category: {msg}");
        assert!(msg.contains("available:"), "lists available: {msg}");
        assert!(msg.contains("base"), "mentions existing 'base': {msg}");
        assert!(
            msg.contains("projects"),
            "mentions existing 'projects': {msg}"
        );
        cleanup(&dir);
    }

    #[test]
    fn test_read_missing_on_empty_store() {
        let (config, dir) = unique_config("empty");
        let store = Store::new(&config).unwrap();

        let err = store.read("nope").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no categories exist yet"),
            "empty-store hint: {msg}"
        );
        cleanup(&dir);
    }

    #[test]
    fn test_store_get_meta() {
        let (config, dir) = unique_config("test");
        let mut store = Store::new(&config).unwrap();

        store
            .write(
                "meta_test",
                &serde_json::json!({"x": 1}),
                None,
                "tester",
                None,
            )
            .unwrap();

        let meta = store.get_meta("meta_test").unwrap().unwrap();
        assert_eq!(meta.name, "meta_test");
        assert_eq!(meta.updated_by, "tester");
        assert!(!meta.content_hash.is_empty());
        assert!(meta.size_bytes > 0);
        cleanup(&dir);
    }

    #[test]
    fn test_store_import_category() {
        let (config, dir) = unique_config("test");
        let store = Store::new(&config).unwrap();

        let data = serde_json::json!({"imported": true, "count": 99});
        let meta = store
            .import_category("migrated_cat", &data, "migration")
            .unwrap();
        assert_eq!(meta.name, "migrated_cat");
        assert_eq!(meta.updated_by, "migration");

        let read_back = store.read("migrated_cat").unwrap();
        assert_eq!(read_back["imported"], true);
        cleanup(&dir);
    }
}
