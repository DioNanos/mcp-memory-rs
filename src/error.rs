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

    #[error("Sync error: {0}")]
    Sync(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, MemoryError>;
