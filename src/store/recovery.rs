//! Recovery of stores left by the pre-0.3.2 protocol.
//!
//! Before 0.3.2 a write rewrote the category JSON *before* its SQLite rows,
//! each statement autocommitting. A failed statement (typically
//! `SQLITE_BUSY`) left the file newer than the row. Sync imports had a
//! second flaw: the row recorded the peer's hash while the file held the
//! local serialization. Either way `content_hash` no longer names the bytes
//! on disk, and the 0.3.2 reader (which serves the bytes that hash to the
//! committed value) cannot serve the category at all.
//!
//! Recovery is a deliberate, two-step act:
//!
//! 1. `plan()` — read-only, opens nothing as a `Store` (no PRAGMA, schema or
//!    reconcile): for every divergent category it records the hash the
//!    database holds, the hash of the file, what kind of value the file is,
//!    whether a backup copy of the database version exists, and a semantic
//!    delta against that backup when one exists. The result is a manifest
//!    the operator reviews.
//! 2. `adopt()` — under the write lock, per category: refuses if the store
//!    has pending staged/parked files or an interrupted restore, if the JSON
//!    is invalid, or if either hash moved since the manifest was written;
//!    then the row is re-pointed at the file's hash, a version row records
//!    the recovery, the search index is rebuilt and the category is marked
//!    dirty. A copy of the adopted file goes to `backups/`. Nothing is
//!    deleted.

use crate::config::MemoryConfig;
use crate::error::{MemoryError, Result};
use crate::store::{atomic_json, snapshot, Store};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

/// One divergent category, as found by `plan()`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanEntry {
    pub name: String,
    /// Hash the database holds (`None`: no row at all).
    pub db_hash: Option<String>,
    pub db_updated_at: Option<String>,
    pub db_updated_by: Option<String>,
    /// Hash of the file on disk.
    pub file_hash: String,
    pub file_size: u64,
    /// `log`, `memory` or `invalid` (unparseable JSON: cannot be adopted).
    pub kind: String,
    /// Backup file whose bytes hash to `db_hash`, if any (the database
    /// version is then still recoverable from there).
    pub db_version_backup: Option<String>,
    /// What adopting the file changes relative to the database version,
    /// when the database version's bytes are available.
    pub delta: Option<Delta>,
}

/// Semantic difference between the database version and the file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum Delta {
    /// Append-only log: entry ids present in the file but not in the
    /// database version, ids missing from the file, duplicate ids.
    Log {
        added_ids: Vec<u64>,
        missing_ids: Vec<u64>,
        duplicate_ids: Vec<u64>,
        /// Ids present in both whose entry (`ts`/`data`) differs — must be
        /// empty for a pure "lost appends" divergence.
        changed_common_ids: Vec<u64>,
        entries_db: usize,
        entries_file: usize,
        /// `true` when the file is a well-formed log: every entry has a
        /// numeric id and ids are strictly increasing.
        file_log_valid: bool,
    },
    /// Object: top-level keys added, removed, changed.
    Map {
        added_keys: Vec<String>,
        removed_keys: Vec<String>,
        changed_keys: Vec<String>,
    },
    /// Same JSON value, different bytes (formatting only).
    FormattingOnly,
    /// Values of different shapes.
    Other,
}

/// The manifest `plan()` writes and `adopt()` requires.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub generated_at: String,
    pub base_dir: String,
    pub journal_mode: String,
    pub entries: Vec<PlanEntry>,
}

/// Read-only: find every category whose committed hash matches no file and
/// describe it. Opens SQLite read-only; never constructs a `Store`.
pub fn plan(config: &MemoryConfig) -> Result<Plan> {
    let report = Store::verify_store(config)?;
    if !report.pending.is_empty() {
        return Err(MemoryError::Inconsistent(format!(
            "store has pending staged/parked files for {:?}: settle them first (open once with the current binary)",
            report.pending
        )));
    }
    if let Some(m) = &report.interrupted_restore {
        return Err(MemoryError::Inconsistent(format!(
            "interrupted restore marker present: {m}"
        )));
    }
    let db = rusqlite::Connection::open_with_flags(
        config.db_path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    db.execute_batch("PRAGMA query_only=ON")?;
    let cat_dir = config.categories_dir();
    let mut entries = Vec::new();
    for name in &report.mismatches {
        let row: Option<(String, String, String)> = match db.query_row(
            "SELECT content_hash, updated_at, updated_by FROM category WHERE name = ?1",
            [name],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ) {
            Ok(v) => Some(v),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(e.into()),
        };
        let path = atomic_json::json_path(&cat_dir, name);
        let bytes = fs::read(&path)?;
        let file_text = String::from_utf8_lossy(&bytes).to_string();
        let file_hash = Store::hash_of(&file_text);
        let file_value: Option<serde_json::Value> = serde_json::from_str(&file_text).ok();
        let kind = match &file_value {
            None => "invalid",
            Some(v) if crate::store::log::is_log(v) => "log",
            Some(_) => "memory",
        }
        .to_string();
        let db_hash = row.as_ref().map(|r| r.0.clone());
        let (db_version_backup, db_value) = match &db_hash {
            Some(h) => find_backup_with_hash(&config.backups_dir(), name, h)?,
            None => (None, None),
        };
        let delta = match (&file_value, &db_value) {
            (Some(f), Some(d)) => Some(semantic_delta(d, f)),
            _ => None,
        };
        entries.push(PlanEntry {
            name: name.clone(),
            db_hash,
            db_updated_at: row.as_ref().map(|r| r.1.clone()),
            db_updated_by: row.as_ref().map(|r| r.2.clone()),
            file_hash,
            file_size: bytes.len() as u64,
            kind,
            db_version_backup,
            delta,
        });
    }
    Ok(Plan {
        generated_at: chrono::Utc::now().to_rfc3339(),
        base_dir: config.resolved_base_dir().display().to_string(),
        journal_mode: report.journal_mode,
        entries,
    })
}

/// Search `backups/<name>.*.json` for a file whose bytes hash to `hash`.
fn find_backup_with_hash(
    backups: &Path,
    name: &str,
    hash: &str,
) -> Result<(Option<String>, Option<serde_json::Value>)> {
    let prefix = format!("{name}.");
    let mut found: Vec<(String, String)> = Vec::new();
    if let Ok(dir) = fs::read_dir(backups) {
        for entry in dir.flatten() {
            let fname = entry.file_name().to_string_lossy().to_string();
            if !fname.starts_with(&prefix) || !fname.ends_with(".json") {
                continue;
            }
            let text = match fs::read_to_string(entry.path()) {
                Ok(t) => t,
                Err(_) => continue,
            };
            if Store::hash_of(&text) == hash {
                found.push((fname, text));
            }
        }
    }
    found.sort();
    match found.pop() {
        Some((fname, text)) => Ok((Some(fname), serde_json::from_str(&text).ok())),
        None => Ok((None, None)),
    }
}

fn semantic_delta(db: &serde_json::Value, file: &serde_json::Value) -> Delta {
    if db == file {
        return Delta::FormattingOnly;
    }
    let ids = |v: &serde_json::Value| -> Vec<u64> {
        v.get("entries")
            .and_then(|e| e.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|e| e.get("id").and_then(|i| i.as_u64()))
                    .collect()
            })
            .unwrap_or_default()
    };
    match (db, file) {
        (serde_json::Value::Object(_), serde_json::Value::Object(_))
            if crate::store::log::is_log(db) && crate::store::log::is_log(file) =>
        {
            let db_ids = ids(db);
            let file_ids = ids(file);
            let db_set: BTreeSet<u64> = db_ids.iter().copied().collect();
            let file_set: BTreeSet<u64> = file_ids.iter().copied().collect();
            let mut seen = BTreeSet::new();
            let mut dup = Vec::new();
            for id in &file_ids {
                if !seen.insert(*id) {
                    dup.push(*id);
                }
            }
            let by_id =
                |v: &serde_json::Value| -> std::collections::BTreeMap<u64, serde_json::Value> {
                    v.get("entries")
                        .and_then(|e| e.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|e| {
                                    e.get("id").and_then(|i| i.as_u64()).map(|i| (i, e.clone()))
                                })
                                .collect()
                        })
                        .unwrap_or_default()
                };
            let db_map = by_id(db);
            let file_map = by_id(file);
            let changed_common_ids: Vec<u64> = db_set
                .intersection(&file_set)
                .filter(|id| db_map.get(id) != file_map.get(id))
                .copied()
                .collect();
            let file_log_valid = file
                .get("entries")
                .and_then(|e| e.as_array())
                .map(|a| {
                    a.iter()
                        .all(|e| e.get("id").and_then(|i| i.as_u64()).is_some())
                        && file_ids.windows(2).all(|w| w[0] < w[1])
                })
                .unwrap_or(false);
            Delta::Log {
                added_ids: file_set.difference(&db_set).copied().collect(),
                missing_ids: db_set.difference(&file_set).copied().collect(),
                duplicate_ids: dup,
                changed_common_ids,
                entries_db: db_ids.len(),
                entries_file: file_ids.len(),
                file_log_valid,
            }
        }
        (serde_json::Value::Object(d), serde_json::Value::Object(f)) => {
            let dk: BTreeSet<&String> = d.keys().collect();
            let fk: BTreeSet<&String> = f.keys().collect();
            Delta::Map {
                added_keys: fk.difference(&dk).map(|k| k.to_string()).collect(),
                removed_keys: dk.difference(&fk).map(|k| k.to_string()).collect(),
                changed_keys: dk
                    .intersection(&fk)
                    .filter(|k| d[**k] != f[**k])
                    .map(|k| k.to_string())
                    .collect(),
            }
        }
        _ => Delta::Other,
    }
}

/// What `adopt()` did for one category.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AdoptOutcome {
    AlreadyConsistent,
    Adopted {
        previous_db_hash: Option<String>,
        file_hash: String,
        adopted_copy: String,
    },
}

impl Store {
    /// Adopt every entry of a reviewed plan in **one** write transaction:
    /// all preconditions are checked under the lock before anything is
    /// written, and either every category is adopted or none is. An entry
    /// whose file still hashes to `entry.file_hash` and whose row already
    /// holds that hash is reported `AlreadyConsistent` (a rerun of the same
    /// plan is a no-op, +0 versions).
    pub fn adopt_plan(&mut self, entries: &[PlanEntry]) -> Result<Vec<(String, AdoptOutcome)>> {
        if let Some(m) = snapshot::interrupted_restore(&self.config) {
            return Err(MemoryError::Inconsistent(format!(
                "interrupted restore marker present: {}",
                m.display()
            )));
        }
        let tx = self.begin_write()?;
        let dir = self.config.categories_dir();
        // Preflight every entry under the lock.
        type Todo = Option<(String, serde_json::Value, Option<String>)>;
        let mut work: Vec<(&PlanEntry, Todo)> = Vec::new();
        for entry in entries {
            let name = entry.name.as_str();
            atomic_json::validate_name(name)?;
            if entry.kind == "invalid" {
                return Err(MemoryError::Inconsistent(format!(
                    "'{name}': plan marks the file as invalid JSON; not adoptable"
                )));
            }
            if atomic_json::has_pending(&dir, name) {
                return Err(MemoryError::Inconsistent(format!(
                    "'{name}' has staged/parked files: refusing to adopt"
                )));
            }
            let content = fs::read_to_string(atomic_json::json_path(&dir, name))?;
            let value: serde_json::Value = serde_json::from_str(&content)?;
            let file_hash = Self::hash_of(&content);
            if file_hash != entry.file_hash {
                return Err(MemoryError::ConcurrencyConflict {
                    expected: entry.file_hash.clone(),
                    actual: file_hash,
                });
            }
            let db_hash = self.get_meta(name)?.map(|m| m.content_hash);
            if db_hash.as_deref() == Some(file_hash.as_str()) {
                // Already adopted (rerun of the same plan): nothing to write.
                work.push((entry, None));
                continue;
            }
            if db_hash != entry.db_hash {
                return Err(MemoryError::ConcurrencyConflict {
                    expected: entry.db_hash.clone().unwrap_or_else(|| "<no row>".into()),
                    actual: db_hash.unwrap_or_else(|| "<no row>".into()),
                });
            }
            work.push((entry, Some((content, value, db_hash))));
        }
        // Apply.
        let mut outcomes = Vec::new();
        for (entry, todo) in work {
            let name = entry.name.as_str();
            let Some((content, value, db_hash)) = todo else {
                outcomes.push((entry.name.clone(), AdoptOutcome::AlreadyConsistent));
                continue;
            };
            let file_hash = Self::hash_of(&content);
            let ts = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S%.3f");
            let adopted_copy = format!("{name}.{ts}.adopted.json");
            fs::copy(
                atomic_json::json_path(&dir, name),
                self.config.backups_dir().join(&adopted_copy),
            )?;
            let now = chrono::Utc::now().to_rfc3339();
            let size_sql = content.len() as i64;
            let reason = format!(
                "recovery: adopted on-disk json (db had {})",
                db_hash
                    .as_deref()
                    .map(|h| &h[..8.min(h.len())])
                    .unwrap_or("no row")
            );
            if db_hash.is_some() {
                self.db.execute(
                    "UPDATE category SET content_hash = ?1, updated_at = ?2, updated_by = ?3, size_bytes = ?4 WHERE name = ?5",
                    rusqlite::params![file_hash, now, "recovery", size_sql, name],
                )?;
            } else {
                self.db.execute(
                    "INSERT INTO category (name, namespace, content_hash, updated_at, updated_by, write_policy, size_bytes) VALUES (?1, 'default', ?2, ?3, 'recovery', 'device', ?4)",
                    rusqlite::params![name, file_hash, now, size_sql],
                )?;
            }
            self.db.execute(
                "INSERT INTO category_version (category_name, content_hash, reason, actor, timestamp) VALUES (?1, ?2, ?3, 'recovery', ?4)",
                rusqlite::params![name, file_hash, reason, now],
            )?;
            self.update_search_index(name, &value)?;
            self.mark_dirty(name, "upsert", Some(&file_hash))?;
            self.prune_versions(name)?;
            tracing::warn!("adopt_json '{name}': {reason}");
            outcomes.push((
                entry.name.clone(),
                AdoptOutcome::Adopted {
                    previous_db_hash: db_hash,
                    file_hash,
                    adopted_copy,
                },
            ));
        }
        tx.commit()?;
        Ok(outcomes)
    }

    /// Single-entry convenience over `adopt_plan`.
    pub fn adopt_json(&mut self, entry: &PlanEntry) -> Result<AdoptOutcome> {
        Ok(self.adopt_plan(std::slice::from_ref(entry))?.remove(0).1)
    }
}
