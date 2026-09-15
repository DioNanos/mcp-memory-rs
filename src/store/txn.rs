//! Cross-process write transactions for the SQLite metadata store.
//!
//! Every mutation of the store (JSON file + SQLite rows) runs inside a
//! `BEGIN IMMEDIATE` transaction. Acquiring the RESERVED lock up-front turns
//! SQLite into the mutex that serializes writers *across processes*, which is
//! what protects the read-modify-write of the category JSON files as well.
//!
//! Lock acquisition is retried with jittered backoff inside one total
//! deadline (`storage.lock_deadline_ms`); the per-statement SQLite busy
//! handler (`storage.sqlite_busy_timeout_ms`) is kept short so the loop, not
//! SQLite, owns the budget. `SQLITE_BUSY` (another connection holds the
//! lock) is retried; `SQLITE_LOCKED` (same connection / shared cache) is not:
//! it signals a programming error and is surfaced with its extended code.

use crate::config::StorageConfig;
use crate::error::{MemoryError, Result};
use rusqlite::{ffi, Connection};
use std::time::{Duration, Instant};

/// Smallest and largest pause between two lock attempts.
const BACKOFF_MIN: Duration = Duration::from_millis(5);
const BACKOFF_MAX: Duration = Duration::from_millis(250);

/// Result of classifying a rusqlite error for retry purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockErrorKind {
    /// Another connection holds the lock: safe to retry.
    Busy(i32),
    /// Contention within this connection (shared cache / nested use): do not retry.
    Locked(i32),
    /// Anything else.
    Other,
}

/// Classify a rusqlite error by its primary SQLite result code.
pub fn classify(err: &rusqlite::Error) -> LockErrorKind {
    match err {
        rusqlite::Error::SqliteFailure(e, _) => match e.code {
            ffi::ErrorCode::DatabaseBusy => LockErrorKind::Busy(e.extended_code),
            ffi::ErrorCode::DatabaseLocked => LockErrorKind::Locked(e.extended_code),
            _ => LockErrorKind::Other,
        },
        _ => LockErrorKind::Other,
    }
}

/// Retry `op` while it fails with `SQLITE_BUSY`, until `deadline` passes.
///
/// `what` names the statement for logging. The wait between attempts grows
/// from 5 ms to 250 ms with ±25 % jitter and never overshoots the deadline.
pub fn retry_busy<T>(
    what: &str,
    deadline_ms: u64,
    op: impl FnMut() -> rusqlite::Result<T>,
) -> Result<T> {
    retry_busy_on(None, 0, what, deadline_ms, op)
}

/// `retry_busy` that also caps SQLite's own busy handler on `conn` to what
/// is left of the budget before every attempt, so a single statement can
/// never wait past the deadline; the configured value is restored after.
pub fn retry_busy_on<T>(
    conn: Option<&Connection>,
    busy_timeout_ms: u64,
    what: &str,
    deadline_ms: u64,
    mut op: impl FnMut() -> rusqlite::Result<T>,
) -> Result<T> {
    let started = Instant::now();
    let deadline = started + Duration::from_millis(deadline_ms);
    let mut backoff = BACKOFF_MIN;
    let mut attempts: u32 = 0;
    let result = loop {
        attempts += 1;
        if let Some(c) = conn {
            let left = deadline.saturating_duration_since(Instant::now());
            let cap = Duration::from_millis(busy_timeout_ms).min(left);
            if let Err(e) = c.busy_timeout(cap) {
                break Err(e.into());
            }
        }
        match op() {
            Ok(v) => {
                if attempts > 1 {
                    tracing::debug!(
                        "{what}: lock acquired after {} attempts / {} ms",
                        attempts,
                        started.elapsed().as_millis()
                    );
                }
                break Ok(v);
            }
            Err(e) => match classify(&e) {
                LockErrorKind::Busy(code) => {
                    let now = Instant::now();
                    if now >= deadline {
                        let waited_ms = started.elapsed().as_millis() as u64;
                        tracing::warn!(
                            "{what}: still busy after {attempts} attempts / {waited_ms} ms (extended code {code}); giving up"
                        );
                        break Err(MemoryError::Busy { waited_ms, code });
                    }
                    let remaining = deadline - now;
                    std::thread::sleep(jitter(backoff).min(remaining));
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
                LockErrorKind::Locked(code) => {
                    tracing::error!("{what}: SQLITE_LOCKED (extended code {code}); not retried");
                    break Err(MemoryError::Locked { code });
                }
                LockErrorKind::Other => break Err(e.into()),
            },
        }
    };
    if let Some(c) = conn {
        // Restore the configured per-statement handler for ordinary reads.
        let _ = c.busy_timeout(Duration::from_millis(busy_timeout_ms));
    }
    result
}

/// ±25 % jitter so that N processes waking together do not re-collide.
fn jitter(d: Duration) -> Duration {
    // Cheap, dependency-free pseudo-randomness: sub-microsecond clock bits.
    let nanos = Instant::now().elapsed().subsec_nanos() as u64 ^ (std::process::id() as u64);
    let pct = 75 + (nanos % 51); // 75..=125
    d * pct as u32 / 100
}

/// An open `BEGIN IMMEDIATE` transaction. Rolls back on drop unless
/// `commit()` was called.
pub struct WriteTxn<'c> {
    conn: &'c Connection,
    /// When the lock wait started: BEGIN and COMMIT share one budget.
    started: Instant,
    deadline_ms: u64,
    busy_timeout_ms: u64,
    done: bool,
}

impl<'c> WriteTxn<'c> {
    /// Acquire the write lock (with bounded retry) and open the transaction.
    pub fn begin(conn: &'c Connection, storage: &StorageConfig) -> Result<Self> {
        let deadline_ms = storage.lock_deadline_ms;
        let busy_timeout_ms = storage.sqlite_busy_timeout_ms;
        let started = Instant::now();
        retry_busy_on(
            Some(conn),
            busy_timeout_ms,
            "BEGIN IMMEDIATE",
            deadline_ms,
            || conn.execute_batch("BEGIN IMMEDIATE"),
        )?;
        Ok(Self {
            conn,
            started,
            deadline_ms,
            busy_timeout_ms,
            done: false,
        })
    }

    /// Budget still available to this transaction, in ms (0 = exhausted).
    pub fn remaining_ms(&self) -> u64 {
        self.deadline_ms
            .saturating_sub(self.started.elapsed().as_millis() as u64)
    }

    /// The connection, for statements executed inside the transaction.
    pub fn conn(&self) -> &'c Connection {
        self.conn
    }

    /// Commit. A `SQLITE_BUSY` on COMMIT leaves the transaction open, so the
    /// COMMIT itself is retried — within what is *left* of the budget that
    /// BEGIN started, never a fresh one. A first attempt is always made.
    pub fn commit(mut self) -> Result<()> {
        let remaining = self.remaining_ms();
        let r = retry_busy_on(
            Some(self.conn),
            self.busy_timeout_ms,
            "COMMIT",
            remaining,
            || self.conn.execute_batch("COMMIT"),
        );
        match r {
            Ok(()) => {
                self.done = true;
                Ok(())
            }
            Err(e) => {
                // Drop will roll back whatever is still open.
                Err(e)
            }
        }
    }

    /// Explicit rollback (same as dropping, but reports errors).
    pub fn rollback(mut self) -> Result<()> {
        self.done = true;
        self.conn.execute_batch("ROLLBACK")?;
        Ok(())
    }
}

impl Drop for WriteTxn<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.done = true;
            if let Err(e) = self.conn.execute_batch("ROLLBACK") {
                // A failed ROLLBACK on an already-aborted transaction is benign
                // (SQLite may have rolled back on its own); keep it visible.
                tracing::debug!("ROLLBACK on drop: {e}");
            }
        }
    }
}

/// Apply the connection-level PRAGMAs from config. `journal_mode` is
/// persistent in the database file; the others are per-connection.
pub fn apply_pragmas(conn: &Connection, storage: &StorageConfig) -> Result<()> {
    conn.busy_timeout(Duration::from_millis(storage.sqlite_busy_timeout_ms))?;

    let journal = match storage.journal_mode.to_ascii_lowercase().as_str() {
        "wal" => "WAL",
        "delete" => "DELETE",
        other => {
            return Err(MemoryError::Other(format!(
                "storage.journal_mode must be \"wal\" or \"delete\", got {other:?}"
            )))
        }
    };
    let synchronous = match storage.synchronous.to_ascii_lowercase().as_str() {
        "full" => "FULL",
        "normal" => "NORMAL",
        other => {
            return Err(MemoryError::Other(format!(
                "storage.synchronous must be \"full\" or \"normal\", got {other:?}"
            )))
        }
    };

    // Switching journal mode needs the write lock when other connections are
    // open; it is a no-op once the file already carries the requested mode.
    let applied: String = retry_busy("PRAGMA journal_mode", storage.lock_deadline_ms, || {
        conn.query_row(&format!("PRAGMA journal_mode={journal}"), [], |r| r.get(0))
    })?;
    if !applied.eq_ignore_ascii_case(journal) {
        return Err(MemoryError::Other(format!(
            "PRAGMA journal_mode={journal} not applied (database reports {applied})"
        )));
    }
    conn.execute_batch(&format!("PRAGMA synchronous={synchronous}"))?;
    // A write re-indexes the whole category: a DELETE with FTS5 triggers plus
    // a `NOT IN (SELECT …)` prune, which SQLite backs with a statement
    // journal / temp b-tree in a temp FILE by default (measured: ~10k pwrite
    // of 4 KB per append into /var/tmp/etilqs_*). Keep those in memory: they
    // are a few MB and never part of durability. A 16 MB page cache keeps the
    // rest of the transaction from spilling before COMMIT.
    conn.execute_batch("PRAGMA temp_store=MEMORY; PRAGMA cache_size=-16384")?;
    Ok(())
}

/// Version of the SQLite library actually linked (bundled).
pub fn sqlite_version() -> &'static str {
    rusqlite::version()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn storage(deadline_ms: u64) -> StorageConfig {
        StorageConfig {
            base_dir: std::env::temp_dir(),
            backup_retention_days: 1,
            max_versions: 10,
            journal_mode: "wal".into(),
            synchronous: "full".into(),
            sqlite_busy_timeout_ms: 20,
            lock_deadline_ms: deadline_ms,
        }
    }

    fn tmp_db(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mcp-memory-txn-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("t.db")
    }

    #[test]
    fn busy_from_other_connection_is_retried_then_reported() {
        let path = tmp_db("busy");
        let a = Connection::open(&path).unwrap();
        let b = Connection::open(&path).unwrap();
        let st = storage(300);
        apply_pragmas(&a, &st).unwrap();
        apply_pragmas(&b, &st).unwrap();
        a.execute_batch("CREATE TABLE t(x)").unwrap();

        let holder = WriteTxn::begin(&a, &st).unwrap();
        let started = Instant::now();
        let err = WriteTxn::begin(&b, &st).err().expect("must be busy");
        let waited = started.elapsed();
        match err {
            MemoryError::Busy { waited_ms, code } => {
                assert!(waited_ms >= 250, "waited_ms={waited_ms}");
                assert_eq!(code, ffi::SQLITE_BUSY);
            }
            other => panic!("expected Busy, got {other:?}"),
        }
        assert!(waited >= Duration::from_millis(250), "{waited:?}");
        assert!(waited < Duration::from_millis(2_000), "{waited:?}");
        drop(holder); // rollback

        // Lock released: the same call now succeeds quickly.
        let t = WriteTxn::begin(&b, &st).unwrap();
        t.conn().execute("INSERT INTO t VALUES (1)", []).unwrap();
        t.commit().unwrap();
        let n: i64 = a
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn drop_without_commit_rolls_back() {
        let path = tmp_db("rollback");
        let a = Connection::open(&path).unwrap();
        let st = storage(200);
        apply_pragmas(&a, &st).unwrap();
        a.execute_batch("CREATE TABLE t(x)").unwrap();
        {
            let t = WriteTxn::begin(&a, &st).unwrap();
            t.conn().execute("INSERT INTO t VALUES (1)", []).unwrap();
            // dropped here
        }
        let n: i64 = a
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn waiting_writer_gets_the_lock_when_released_in_time() {
        let path = tmp_db("handoff");
        let a = Connection::open(&path).unwrap();
        let st = storage(2_000);
        apply_pragmas(&a, &st).unwrap();
        a.execute_batch("CREATE TABLE t(x)").unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let holder = WriteTxn::begin(&a, &st).unwrap();
        let path2 = path.clone();
        let st2 = st.clone();
        let b2 = barrier.clone();
        let h = std::thread::spawn(move || {
            let b = Connection::open(&path2).unwrap();
            apply_pragmas(&b, &st2).unwrap();
            b2.wait();
            let started = Instant::now();
            let t = WriteTxn::begin(&b, &st2).unwrap();
            t.conn().execute("INSERT INTO t VALUES (2)", []).unwrap();
            t.commit().unwrap();
            started.elapsed()
        });
        barrier.wait();
        std::thread::sleep(Duration::from_millis(150));
        drop(holder);
        let waited = h.join().unwrap();
        assert!(waited >= Duration::from_millis(100), "{waited:?}");
        assert!(waited < Duration::from_millis(1_500), "{waited:?}");
    }

    #[test]
    fn begin_and_commit_share_one_budget() {
        let path = tmp_db("budget");
        let a = Connection::open(&path).unwrap();
        let st = storage(300);
        apply_pragmas(&a, &st).unwrap();
        a.execute_batch("CREATE TABLE t(x)").unwrap();
        let t = WriteTxn::begin(&a, &st).unwrap();
        assert!(t.remaining_ms() <= 300);
        std::thread::sleep(Duration::from_millis(120));
        let left = t.remaining_ms();
        assert!(left <= 180 && left > 0, "remaining={left}");
        std::thread::sleep(Duration::from_millis(250));
        assert_eq!(t.remaining_ms(), 0, "budget exhausted");
        // An exhausted budget still allows the single COMMIT attempt.
        t.commit().unwrap();
    }

    #[test]
    fn statement_busy_handler_never_outlives_the_budget() {
        let path = tmp_db("cap");
        let a = Connection::open(&path).unwrap();
        let b = Connection::open(&path).unwrap();
        let mut st = storage(300);
        st.sqlite_busy_timeout_ms = 5_000; // far larger than the deadline
        apply_pragmas(&a, &st).unwrap();
        apply_pragmas(&b, &st).unwrap();
        a.execute_batch("CREATE TABLE t(x)").unwrap();
        let _holder = WriteTxn::begin(&a, &st).unwrap();
        let started = Instant::now();
        let err = WriteTxn::begin(&b, &st).err().expect("busy");
        let waited = started.elapsed();
        assert!(matches!(err, MemoryError::Busy { .. }));
        assert!(waited < Duration::from_millis(1_000), "capped: {waited:?}");
        assert!(waited >= Duration::from_millis(280), "{waited:?}");
    }

    #[test]
    fn pragmas_are_applied_and_validated() {
        let path = tmp_db("pragma");
        let a = Connection::open(&path).unwrap();
        apply_pragmas(&a, &storage(100)).unwrap();
        let jm: String = a
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(jm.to_ascii_lowercase(), "wal");
        let sync: i64 = a.query_row("PRAGMA synchronous", [], |r| r.get(0)).unwrap();
        assert_eq!(sync, 2, "FULL");
        let mut bad = storage(100);
        bad.journal_mode = "memory".into();
        assert!(apply_pragmas(&a, &bad).is_err());
        assert!(!sqlite_version().is_empty());
    }
}
