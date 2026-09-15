//! Crash-safe category JSON files.
//!
//! The content of a category lives in `categories/<name>.json`; its hash,
//! versions and dirty flag live in SQLite. A mutation must leave the two in
//! agreement even if the process dies half-way, and must stay correct while
//! other processes read or write the same category. The protocol:
//!
//! 1. inside the write transaction, the new content is written to a
//!    *staged* file with a name unique to this transaction
//!    (`<name>.json.new.<pid>-<nanos>`) and fsynced;
//! 2. the SQLite rows (with the new hash) are committed;
//! 3. only then the staged file is renamed over `<name>.json`.
//!
//! Recovery is unambiguous and happens **only under the write lock** (at
//! open and at the start of every mutation), where no transaction can be in
//! flight: a staged file whose hash equals the hash SQLite holds for that
//! category is a commit that was cut before step 3 — it is promoted; any
//! other staged file belongs to a transaction that never committed — it is
//! discarded. Readers never rename or delete anything: they pick, among the
//! final file, the staged files and the parked deletes, the content whose
//! hash equals the committed one and serve it (`committed_content`). A
//! reader that mutated on a hash read a moment earlier could promote a
//! *newer* uncommitted stage carrying the same bytes (ABA); serving instead
//! of renaming has no such window.
//!
//! Deletes park the file as `<name>.json.deleted.<pid>-<nanos>` before the
//! commit and move it to the backups directory after; undoing a parked
//! delete follows the same lock rule.

use crate::error::{MemoryError, Result};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

const STAGED_MARK: &str = ".json.new.";
const DELETED_MARK: &str = ".json.deleted.";

pub fn json_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}.json"))
}

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}-{nanos}", std::process::id())
}

/// A staged file waiting for the SQLite commit before promotion.
#[must_use = "a staged file must be promoted after COMMIT (or dropped to leave it for recovery)"]
pub struct Staged {
    staged: PathBuf,
    final_path: PathBuf,
}

impl Staged {
    /// Rename the staged file over the final one and fsync the directory.
    /// A missing staged file means a concurrent reconciler already promoted
    /// this very commit (it could only do so because the hash matched the
    /// committed row), so that is success too.
    pub fn promote(self) -> Result<()> {
        match fs::rename(&self.staged, &self.final_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(
                    "staged file {} already promoted by another process",
                    self.staged.display()
                );
            }
            Err(e) => return Err(e.into()),
        }
        fsync_dir(self.final_path.parent());
        Ok(())
    }

    /// Path of the staged file (for tests / crash injection).
    pub fn staged_path(&self) -> &Path {
        &self.staged
    }
}

/// Write `content` to a fresh `<name>.json.new.<unique>` file. Both the file
/// and its directory entry are fsynced *before* the caller commits: after a
/// power loss the committed hash must always find its staged file.
pub fn stage(dir: &Path, name: &str, content: &str) -> Result<Staged> {
    let staged = dir.join(format!("{name}{STAGED_MARK}{}", unique_suffix()));
    let mut f = fs::File::create(&staged)?;
    f.write_all(content.as_bytes())?;
    f.sync_all()?;
    fsync_dir_strict(dir)?;
    Ok(Staged {
        staged,
        final_path: json_path(dir, name),
    })
}

/// Outcome of settling one category's staged / parked files.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Settled {
    pub promoted: usize,
    pub discarded: usize,
    pub restored: usize,
    pub completed: usize,
}

/// Settle every staged write of `name` against the hash SQLite holds.
/// **Caller must hold the write lock**: a non-matching staged file is then
/// provably dead and is discarded.
pub fn settle_staged(
    dir: &Path,
    name: &str,
    db_hash: Option<&str>,
    hash_of: fn(&str) -> String,
) -> Result<Settled> {
    let mut out = Settled::default();
    for path in staged_files(dir, name)? {
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue, // raced away
            Err(e) => return Err(e.into()),
        };
        let matches = db_hash.map(|h| h == hash_of(&content)).unwrap_or(false);
        if matches {
            match fs::rename(&path, json_path(dir, name)) {
                Ok(()) => {
                    fsync_dir(Some(dir));
                    tracing::warn!("recovery: promoted staged write for category '{name}'");
                    out.promoted += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        } else {
            match fs::remove_file(&path) {
                Ok(()) => {
                    tracing::warn!(
                        "recovery: discarded aborted staged write for category '{name}'"
                    );
                    out.discarded += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(out)
}

/// Where a reader found the committed content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Final,
    Staged,
    Parked,
}

/// Read-only resolution for readers: the content of `name` whose hash is
/// `committed_hash`, looking at the final file first, then staged files,
/// then parked deletes. Nothing is renamed or removed. `None` means no file
/// carries the committed bytes (a writer is between commit and promote and
/// the caller may retry, or the store predates the protocol).
pub fn committed_content(
    dir: &Path,
    name: &str,
    committed_hash: &str,
    hash_of: fn(&str) -> String,
) -> Result<Option<(String, Source)>> {
    let mut candidates: Vec<(PathBuf, Source)> = vec![(json_path(dir, name), Source::Final)];
    candidates.extend(
        staged_files(dir, name)?
            .into_iter()
            .map(|p| (p, Source::Staged)),
    );
    candidates.extend(
        deleted_files(dir, name)?
            .into_iter()
            .map(|p| (p, Source::Parked)),
    );
    for (path, source) in candidates {
        match fs::read_to_string(&path) {
            Ok(content) => {
                if hash_of(&content) == committed_hash {
                    return Ok(Some((content, source)));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(None)
}

/// A deletion parked until the SQLite commit.
#[must_use = "a staged delete must be promoted after COMMIT (or dropped to leave it for recovery)"]
pub struct StagedDelete {
    parked: PathBuf,
    name: String,
}

impl StagedDelete {
    /// After COMMIT: move the parked file into the backups directory. A
    /// missing parked file means a reconciler already completed this delete.
    pub fn promote(self, backups_dir: &Path) -> Result<()> {
        match finish_delete(&self.parked, backups_dir, &self.name) {
            Ok(()) => Ok(()),
            Err(MemoryError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Park `<name>.json` as `<name>.json.deleted.<unique>` (inside the write
/// lock). A missing file is not an error: the SQLite rows may still need
/// deleting.
pub fn stage_delete(dir: &Path, name: &str) -> Result<Option<StagedDelete>> {
    let src = json_path(dir, name);
    if !src.exists() {
        return Ok(None);
    }
    let parked = dir.join(format!("{name}{DELETED_MARK}{}", unique_suffix()));
    fs::rename(&src, &parked)?;
    fsync_dir_strict(dir)?;
    Ok(Some(StagedDelete {
        parked,
        name: name.to_string(),
    }))
}

/// Settle parked deletes of `name`. **Caller must hold the write lock.**
/// With the row gone the delete committed: complete it. With the row still
/// present the delete never committed: restore the file.
pub fn settle_deleted(
    dir: &Path,
    name: &str,
    db_has_row: bool,
    backups_dir: &Path,
) -> Result<Settled> {
    let mut out = Settled::default();
    for parked in deleted_files(dir, name)? {
        if db_has_row {
            match fs::rename(&parked, json_path(dir, name)) {
                Ok(()) => {
                    fsync_dir(Some(dir));
                    tracing::warn!("recovery: restored '{name}' from an aborted delete");
                    out.restored += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        } else {
            match finish_delete(&parked, backups_dir, name) {
                Ok(()) => {
                    tracing::warn!("recovery: completed interrupted delete of '{name}'");
                    out.completed += 1;
                }
                Err(MemoryError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
    }
    Ok(out)
}

/// Whether `name` has any staged or parked file at all (cheap pre-check).
pub fn has_pending(dir: &Path, name: &str) -> bool {
    matches!(staged_files(dir, name), Ok(v) if !v.is_empty())
        || matches!(deleted_files(dir, name), Ok(v) if !v.is_empty())
}

/// Names that have at least one staged or parked file.
pub fn pending_names(dir: &Path) -> Result<Vec<String>> {
    let mut out = std::collections::BTreeSet::new();
    for entry in fs::read_dir(dir)? {
        let fname = entry?.file_name();
        let fname = fname.to_string_lossy();
        if let Some(i) = fname.find(STAGED_MARK) {
            out.insert(fname[..i].to_string());
        } else if let Some(i) = fname.find(DELETED_MARK) {
            out.insert(fname[..i].to_string());
        }
    }
    Ok(out.into_iter().collect())
}

/// Category names that have a final `.json` file.
pub fn json_names(dir: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let fname = entry?.file_name();
        let fname = fname.to_string_lossy();
        if fname.contains(STAGED_MARK) || fname.contains(DELETED_MARK) {
            continue;
        }
        if let Some(name) = fname.strip_suffix(".json") {
            out.push(name.to_string());
        }
    }
    out.sort();
    Ok(out)
}

/// Copy `src` into `backups_dir` as `<name>.<timestamp><suffix>`; failures
/// are logged, never fatal (a backup copy must not block the write).
pub fn backup_copy(backups_dir: &Path, src: &Path, name: &str, suffix: &str) {
    if !src.exists() {
        return;
    }
    let ts = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S%.3f");
    let dest = backups_dir.join(format!("{name}.{ts}{suffix}"));
    if let Err(e) = fs::copy(src, &dest) {
        tracing::warn!("backup copy of '{name}' failed: {e}");
    }
}

/// Validate a category name for use as a file name.
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.contains('/') || name.contains('\\') || name.contains("..") {
        return Err(MemoryError::InvalidName(name.into()));
    }
    Ok(())
}

fn staged_files(dir: &Path, name: &str) -> Result<Vec<PathBuf>> {
    files_with_prefix(dir, &format!("{name}{STAGED_MARK}"))
}

fn deleted_files(dir: &Path, name: &str) -> Result<Vec<PathBuf>> {
    files_with_prefix(dir, &format!("{name}{DELETED_MARK}"))
}

fn files_with_prefix(dir: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with(prefix) {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

fn finish_delete(parked: &Path, backups_dir: &Path, name: &str) -> Result<()> {
    let ts = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S%.3f");
    let dest = backups_dir.join(format!("{name}.{ts}.deleted.json"));
    if fs::rename(parked, &dest).is_err() {
        // Backups may live on another filesystem: copy + remove.
        fs::copy(parked, &dest)?;
        fs::remove_file(parked)?;
    }
    Ok(())
}

/// Best-effort directory fsync (after promotions: the rename is already
/// recoverable either way).
fn fsync_dir(dir: Option<&Path>) {
    if let Some(dir) = dir {
        if let Ok(d) = fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
}

/// Directory fsync that must succeed: used before a commit depends on the
/// directory entry being durable.
fn fsync_dir_strict(dir: &Path) -> Result<()> {
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(s: &str) -> String {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(s.as_bytes()))
    }

    fn tmp(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mcp-memory-atomic-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn stage_then_promote_replaces_final_and_is_idempotent() {
        let dir = tmp("promote");
        fs::write(json_path(&dir, "c"), "old").unwrap();
        let s = stage(&dir, "c", "new").unwrap();
        assert_eq!(fs::read_to_string(json_path(&dir, "c")).unwrap(), "old");
        let staged_path = s.staged_path().to_path_buf();
        // A locked settle promotes it first (hash matches): our promote is still Ok.
        assert_eq!(
            settle_staged(&dir, "c", Some(&h("new")), h)
                .unwrap()
                .promoted,
            1
        );
        assert!(!staged_path.exists());
        s.promote().unwrap();
        assert_eq!(fs::read_to_string(json_path(&dir, "c")).unwrap(), "new");
    }

    #[test]
    fn unique_staged_names_do_not_collide() {
        let dir = tmp("unique");
        let a = stage(&dir, "c", "1").unwrap();
        let b = stage(&dir, "c", "2").unwrap();
        assert_ne!(a.staged_path(), b.staged_path());
        assert_eq!(staged_files(&dir, "c").unwrap().len(), 2);
        b.promote().unwrap();
        assert_eq!(fs::read_to_string(json_path(&dir, "c")).unwrap(), "2");
        // a's file is still there, untouched by b's promotion
        assert!(a.staged_path().exists());
    }

    #[test]
    fn readers_serve_committed_bytes_without_touching_files() {
        let dir = tmp("reader");
        fs::write(json_path(&dir, "c"), "old").unwrap();
        let _s = stage(&dir, "c", "inflight").unwrap();
        // Committed hash is still "old": the final file wins, stage untouched.
        let (content, src) = committed_content(&dir, "c", &h("old"), h).unwrap().unwrap();
        assert_eq!((content.as_str(), src), ("old", Source::Final));
        // Committed hash moved to "inflight" (owner between commit and rename):
        // the staged bytes are served, still nothing renamed.
        let (content, src) = committed_content(&dir, "c", &h("inflight"), h)
            .unwrap()
            .unwrap();
        assert_eq!((content.as_str(), src), ("inflight", Source::Staged));
        assert_eq!(staged_files(&dir, "c").unwrap().len(), 1);
        assert_eq!(fs::read_to_string(json_path(&dir, "c")).unwrap(), "old");
        // Nothing matches: None, and still nothing touched.
        assert!(committed_content(&dir, "c", &h("other"), h)
            .unwrap()
            .is_none());
        // The locked pass is the only thing that discards.
        let r = settle_staged(&dir, "c", Some(&h("old")), h).unwrap();
        assert_eq!(r.discarded, 1);
        assert!(staged_files(&dir, "c").unwrap().is_empty());
        assert_eq!(fs::read_to_string(json_path(&dir, "c")).unwrap(), "old");
    }

    #[test]
    fn staged_delete_promote_undo_and_reader_view() {
        let dir = tmp("del");
        let backups = dir.join("backups");
        fs::create_dir_all(&backups).unwrap();
        fs::write(json_path(&dir, "c"), "x").unwrap();
        let sd = stage_delete(&dir, "c").unwrap().unwrap();
        assert!(!json_path(&dir, "c").exists());
        // reader with the row still present: served the parked bytes, nothing changed
        let (content, src) = committed_content(&dir, "c", &h("x"), h).unwrap().unwrap();
        assert_eq!((content.as_str(), src), ("x", Source::Parked));
        assert_eq!(deleted_files(&dir, "c").unwrap().len(), 1);
        // committed (row gone): a locked settle completes it; owner's promote is a no-op
        assert_eq!(
            settle_deleted(&dir, "c", false, &backups)
                .unwrap()
                .completed,
            1
        );
        sd.promote(&backups).unwrap();
        assert_eq!(fs::read_dir(&backups).unwrap().count(), 1);
        // aborted delete under the lock: restored
        fs::write(json_path(&dir, "d"), "y").unwrap();
        let _sd = stage_delete(&dir, "d").unwrap().unwrap();
        assert_eq!(
            settle_deleted(&dir, "d", true, &backups).unwrap().restored,
            1
        );
        assert_eq!(fs::read_to_string(json_path(&dir, "d")).unwrap(), "y");
        assert!(stage_delete(&dir, "missing").unwrap().is_none());
    }

    #[test]
    fn listing_separates_final_and_pending() {
        let dir = tmp("list");
        fs::write(json_path(&dir, "a"), "1").unwrap();
        fs::write(json_path(&dir, "b"), "2").unwrap();
        let _s = stage(&dir, "b", "3").unwrap();
        let _t = stage(&dir, "z", "4").unwrap();
        let _d = stage_delete(&dir, "a").unwrap();
        assert_eq!(json_names(&dir).unwrap(), vec!["b".to_string()]);
        assert_eq!(
            pending_names(&dir).unwrap(),
            vec!["a".to_string(), "b".to_string(), "z".to_string()]
        );
        assert!(has_pending(&dir, "z"));
        assert!(!has_pending(&dir, "q"));
    }
}
