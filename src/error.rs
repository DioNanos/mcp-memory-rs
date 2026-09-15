use thiserror::Error;

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Category not found: {0}")]
    NotFound(String),

    #[error("Category already exists: {0}")]
    AlreadyExists(String),

    #[error("Hash mismatch (optimistic concurrency): expected {expected}, got {actual}")]
    ConcurrencyConflict { expected: String, actual: String },

    #[error("ACL denied: device '{device}' cannot write to '{category}'")]
    AclDenied { device: String, category: String },

    #[error("Invalid category name: {0}")]
    InvalidName(String),

    #[error("Category kind mismatch: {0}")]
    KindMismatch(String),

    #[error("Sync error: {0}")]
    Sync(String),

    /// The cross-process write lock could not be acquired within the
    /// configured deadline (SQLITE_BUSY from another connection).
    #[error("Store busy: another writer held the lock for {waited_ms} ms (sqlite code {code}); retry later")]
    Busy { waited_ms: u64, code: i32 },

    /// SQLITE_LOCKED: contention inside the same connection / shared cache.
    /// Not retried — it indicates a programming error, not another process.
    #[error("Store locked within this connection (sqlite code {code}); not retried")]
    Locked { code: i32 },

    /// On-disk state (JSON files vs SQLite metadata) could not be reconciled.
    #[error("Store inconsistent: {0}")]
    Inconsistent(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, MemoryError>;
