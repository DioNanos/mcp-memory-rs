//! Consistent snapshots of the whole store (SQLite + category JSON files).
//!
//! Copying `memory.db` is not a backup: in WAL mode the newest commits may
//! live only in `memory.db-wal`, and the JSON files can move independently.
//! A snapshot therefore:
//!
//! 1. takes the cross-process write lock on a *second* connection (writers
//!    block; readers keep going — WAL),
//! 2. runs `VACUUM INTO` from the store connection, which produces a compact,
//!    self-contained database (WAL included, no `-wal`/`-shm` to carry),
//! 3. copies every `categories/*.json` (staged/parked files are settled by
//!    the open-time reconcile, never copied),
//! 4. writes `manifest.json`, then releases the lock.
//!
//! Because writers were blocked for the whole of 2-3, the database and the
//! JSON files in the snapshot describe the same instant.
//!
//! Restore is the inverse copy and **requires every mcp-memory-rs process on
//! the store to be stopped**: SQLite connections that stay open across a file
//! swap read garbage. The CLI asks for an explicit acknowledgement flag.

use crate::config::MemoryConfig;
use crate::error::{MemoryError, Result};
use crate::store::txn::{self, WriteTxn};
use std::fs;
use std::path::{Path, PathBuf};

pub const MANIFEST: &str = "manifest.json";
pub const DB_FILE: &str = "memory.db";
pub const CATEGORIES_DIR: &str = "categories";
pub const BACKUPS_DIR: &str = "backups";
/// Present in `base_dir` from just before the restore swap until just after
/// it. The store refuses to open while it exists.
pub const RESTORE_MARKER: &str = "RESTORE_IN_PROGRESS";

/// The marker path if a restore swap was interrupted, else `None`.
pub fn interrupted_restore(config: &MemoryConfig) -> Option<PathBuf> {
    let p = config.resolved_base_dir().join(RESTORE_MARKER);
    p.exists().then_some(p)
}

/// What to include beyond the current state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    /// Also copy `backups/` (pre-overwrite/pre-delete copies): needed for
    /// `read_version` / `memory_history` payloads, otherwise only the
    /// current content and the version *metadata* are covered.
    pub with_backups: bool,
}

/// What a snapshot contains, for humans and for restore-time checks.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub created_at: String,
    pub device: String,
    pub sqlite_version: String,
    pub categories: usize,
    pub source_base_dir: PathBuf,
    /// Coverage: `true` when `backups/` was copied too.
    #[serde(default)]
    pub includes_backups: bool,
    #[serde(default)]
    pub backup_files: usize,
    /// `backups/<file>` → sha256 of its bytes, for every included backup.
    #[serde(default)]
    pub backups: std::collections::BTreeMap<String, String>,
}

/// Write a consistent snapshot of the store into `dest` (created; must not
/// already contain a snapshot).
pub fn create(
    config: &MemoryConfig,
    store_conn: &rusqlite::Connection,
    dest: &Path,
    options: Options,
    settle_pending: impl FnOnce() -> Result<()>,
) -> Result<Manifest> {
    if dest.join(MANIFEST).exists() {
        return Err(MemoryError::Other(format!(
            "snapshot destination {} already holds a snapshot",
            dest.display()
        )));
    }
    fs::create_dir_all(dest.join(CATEGORIES_DIR))?;

    // Hold the write lock from a separate connection: `VACUUM INTO` cannot
    // run inside a transaction on the connection that executes it.
    let guard_conn = rusqlite::Connection::open(config.db_path())?;
    txn::apply_pragmas(&guard_conn, &config.storage)?;
    let guard = WriteTxn::begin(&guard_conn, &config.storage)?;
    // With writers blocked, settle any staged/parked file so the JSON copy
    // matches the committed rows exactly.
    settle_pending()?;

    let db_out = dest.join(DB_FILE);
    let db_out_str = db_out.to_string_lossy().replace('\'', "''");
    store_conn.execute_batch(&format!("VACUUM INTO '{db_out_str}'"))?;

    let mut categories = 0usize;
    let src_dir = config.categories_dir();
    for name in crate::store::atomic_json::json_names(&src_dir)? {
        let fname = format!("{name}.json");
        copy_synced(
            &src_dir.join(&fname),
            &dest.join(CATEGORIES_DIR).join(&fname),
        )?;
        categories += 1;
    }
    fs::File::open(&db_out)?.sync_all()?;

    let mut backup_files = 0usize;
    let mut backups = std::collections::BTreeMap::new();
    if options.with_backups {
        let src_b = config.backups_dir();
        let dst_b = dest.join(BACKUPS_DIR);
        fs::create_dir_all(&dst_b)?;
        if src_b.exists() {
            for entry in fs::read_dir(&src_b)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue; // restore-parking dirs and the like
                }
                let dst = dst_b.join(entry.file_name());
                copy_synced(&entry.path(), &dst)?;
                backups.insert(
                    entry.file_name().to_string_lossy().to_string(),
                    file_sha256(&dst)?,
                );
                backup_files += 1;
            }
        }
        fs::File::open(&dst_b)?.sync_all()?;
    }

    let manifest = Manifest {
        format: 1,
        created_at: chrono::Utc::now().to_rfc3339(),
        device: config.acl.device_name.clone(),
        sqlite_version: txn::sqlite_version().to_string(),
        categories,
        source_base_dir: config.resolved_base_dir(),
        includes_backups: options.with_backups,
        backup_files,
        backups,
    };
    fs::write(
        dest.join(MANIFEST),
        serde_json::to_string_pretty(&manifest)?,
    )?;
    fs::File::open(dest.join(MANIFEST))?.sync_all()?;
    fs::File::open(dest.join(CATEGORIES_DIR))?.sync_all()?;
    fs::File::open(dest)?.sync_all()?;
    guard.rollback()?;
    // Never report a snapshot we could not read back.
    verify(dest)?;
    Ok(manifest)
}

/// Copy a file and fsync the copy.
fn copy_synced(src: &Path, dst: &Path) -> Result<()> {
    fs::copy(src, dst)?;
    fs::File::open(dst)?.sync_all()?;
    Ok(())
}

fn file_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let bytes = fs::read(path)?;
    Ok(format!("{:x}", Sha256::digest(&bytes)))
}

/// Check that a snapshot is internally consistent: every category row in its
/// database has its JSON file with the recorded hash, no extra files, count
/// equal to the manifest, and the database passes `integrity_check`.
pub fn verify(src: &Path) -> Result<Manifest> {
    let manifest = read_manifest(src)?;
    let db = rusqlite::Connection::open_with_flags(
        src.join(DB_FILE),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let ok: String = db.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if ok != "ok" {
        return Err(MemoryError::Inconsistent(format!(
            "snapshot database integrity_check: {ok}"
        )));
    }
    let mut stmt = db.prepare("SELECT name, content_hash FROM category ORDER BY name")?;
    let rows: Vec<(String, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    let cat_dir = src.join(CATEGORIES_DIR);
    let files = crate::store::atomic_json::json_names(&cat_dir)?;
    let mut problems = Vec::new();
    for (name, hash) in &rows {
        let path = cat_dir.join(format!("{name}.json"));
        if !path.is_file() {
            problems.push(format!("{name}: not a regular file"));
            continue;
        }
        match fs::read_to_string(&path) {
            Ok(content) => {
                if crate::store::Store::hash_of(&content) != *hash {
                    problems.push(format!("{name}: file hash != database hash"));
                }
            }
            Err(e) => problems.push(format!("{name}: {e}")),
        }
    }
    let known: std::collections::BTreeSet<&str> = rows.iter().map(|(n, _)| n.as_str()).collect();
    for f in &files {
        if !known.contains(f.as_str()) {
            problems.push(format!("{f}: file without database row"));
        }
    }
    if rows.len() != manifest.categories {
        problems.push(format!(
            "manifest says {} categories, database has {}",
            manifest.categories,
            rows.len()
        ));
    }
    if manifest.includes_backups {
        // Every included backup: present, and byte-identical to the hash
        // recorded when the snapshot was taken; no unlisted file.
        let b_dir = src.join(BACKUPS_DIR);
        let mut seen = std::collections::BTreeSet::new();
        for (fname, hash) in &manifest.backups {
            let path = b_dir.join(fname);
            match file_sha256(&path) {
                Ok(h) if h == *hash => {}
                Ok(_) => problems.push(format!("backups/{fname}: hash != manifest")),
                Err(e) => problems.push(format!("backups/{fname}: {e}")),
            }
            seen.insert(fname.clone());
        }
        match fs::read_dir(&b_dir) {
            Ok(dir) => {
                for entry in dir {
                    let entry = entry?;
                    let fname = entry.file_name().to_string_lossy().to_string();
                    if !entry.file_type()?.is_file() {
                        problems.push(format!("backups/{fname}: not a regular file"));
                    } else if !seen.contains(&fname) {
                        problems.push(format!("backups/{fname}: not in manifest"));
                    }
                }
            }
            Err(e) => problems.push(format!("backups/: {e}")),
        }
        if manifest.backups.len() != manifest.backup_files {
            problems.push(format!(
                "manifest says {} backup files, lists {}",
                manifest.backup_files,
                manifest.backups.len()
            ));
        }
    }
    if !problems.is_empty() {
        return Err(MemoryError::Inconsistent(format!(
            "snapshot {} failed verification: {}",
            src.display(),
            problems.join("; ")
        )));
    }
    Ok(manifest)
}

/// Read and validate a snapshot's manifest.
pub fn read_manifest(src: &Path) -> Result<Manifest> {
    let raw = fs::read_to_string(src.join(MANIFEST))
        .map_err(|e| MemoryError::Other(format!("{} is not a snapshot ({e})", src.display())))?;
    let m: Manifest = serde_json::from_str(&raw)?;
    if m.format != 1 {
        return Err(MemoryError::Other(format!(
            "unsupported snapshot format {}",
            m.format
        )));
    }
    if !src.join(DB_FILE).exists() {
        return Err(MemoryError::Other("snapshot has no memory.db".into()));
    }
    Ok(m)
}

/// Replace the live store with `src`. The caller guarantees no process has
/// the store open. Two phases: (1) the snapshot is verified, copied into a
/// staging directory *beside* the live store and verified again there —
/// nothing live is touched and any failure leaves the store as it was;
/// (2) the live pieces are parked under `<base>/restore-parked-<ts>/` and
/// the staged ones renamed into place. Phase 2 is a handful of renames on
/// one filesystem: it cannot fail half-copied, and the parked state makes a
/// restore itself reversible.
pub fn restore(config: &MemoryConfig, src: &Path) -> Result<Manifest> {
    let manifest = verify(src)?;
    let base = config.resolved_base_dir();
    fs::create_dir_all(&base)?;
    let db_path = config.db_path();
    let cats = config.categories_dir();
    let live_b = config.backups_dir();
    let ts = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S%.3f");

    // Phase 1: stage a full, fsynced copy and verify it as a snapshot. Any
    // failure here removes the staging directory and leaves the live store
    // byte-untouched.
    let staging = base.join(format!("restore-staging-{ts}"));
    let stage_all = || -> Result<()> {
        fs::create_dir_all(staging.join(CATEGORIES_DIR))?;
        copy_synced(&src.join(MANIFEST), &staging.join(MANIFEST))?;
        copy_synced(&src.join(DB_FILE), &staging.join(DB_FILE))?;
        for entry in fs::read_dir(src.join(CATEGORIES_DIR))? {
            let entry = entry?;
            copy_synced(
                &entry.path(),
                &staging.join(CATEGORIES_DIR).join(entry.file_name()),
            )?;
        }
        if manifest.includes_backups {
            fs::create_dir_all(staging.join(BACKUPS_DIR))?;
            for fname in manifest.backups.keys() {
                copy_synced(
                    &src.join(BACKUPS_DIR).join(fname),
                    &staging.join(BACKUPS_DIR).join(fname),
                )?;
            }
            fs::File::open(staging.join(BACKUPS_DIR))?.sync_all()?;
        }
        fs::File::open(staging.join(CATEGORIES_DIR))?.sync_all()?;
        fs::File::open(&staging)?.sync_all()?;
        verify(&staging)
            .map(|_| ())
            .map_err(|e| MemoryError::Inconsistent(format!("staged copy failed verification: {e}")))
    };
    if let Err(e) = stage_all() {
        let _ = fs::remove_dir_all(&staging);
        return Err(MemoryError::Inconsistent(format!(
            "restore aborted before touching the live store: {e}"
        )));
    }

    // Phase 2: park the live pieces, rename the staged ones into place.
    // The renames are copy-safe but NOT atomic as a group: a crash between
    // them leaves a mix of parked/staged/live pieces. The marker makes that
    // state fail-closed (`Store::new` refuses) until someone recovers by
    // hand from restore-parked-<ts>/ and restore-staging-<ts>/.
    let marker = base.join(RESTORE_MARKER);
    fs::write(
        &marker,
        format!(
            "restore from {} started {ts}; parked=restore-parked-{ts} staging=restore-staging-{ts}\n",
            src.display()
        ),
    )?;
    fs::File::open(&marker)?.sync_all()?;
    fs::File::open(&base)?.sync_all()?;
    let parking = base.join(format!("restore-parked-{ts}"));
    fs::create_dir_all(&parking)?;
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let p = PathBuf::from(format!("{}{}", db_path.display(), suffix));
        if p.exists() {
            fs::rename(&p, parking.join(p.file_name().unwrap()))?;
        }
    }
    if cats.exists() {
        fs::rename(&cats, parking.join(CATEGORIES_DIR))?;
    }
    fs::rename(staging.join(DB_FILE), &db_path)?;
    fs::rename(staging.join(CATEGORIES_DIR), &cats)?;
    if manifest.includes_backups {
        if live_b.exists() {
            fs::rename(&live_b, parking.join(BACKUPS_DIR))?;
        }
        fs::rename(staging.join(BACKUPS_DIR), &live_b)?;
    }
    fs::rename(
        staging.join(MANIFEST),
        parking.join("restored-from-manifest.json"),
    )?;
    let _ = fs::remove_dir(&staging);
    fs::File::open(&base)?.sync_all()?;
    fs::remove_file(&marker)?;
    fs::File::open(&base)?.sync_all()?;
    tracing::info!(
        "restored snapshot from {} ({} categories); previous state parked in {}",
        src.display(),
        manifest.categories,
        parking.display()
    );
    Ok(manifest)
}
