use crate::config::MemoryConfig;
use crate::error::Result;

/// Prune backups older than retention_days
pub fn prune_old_backups(config: &MemoryConfig) -> Result<u64> {
    let backups_dir = config.backups_dir();
    if !backups_dir.exists() {
        return Ok(0);
    }

    let retention = chrono::Duration::days(config.storage.backup_retention_days as i64);
    let cutoff = chrono::Utc::now() - retention;
    let mut removed = 0u64;

    for entry in std::fs::read_dir(&backups_dir)? {
        let entry = entry?;
        if let Ok(metadata) = entry.metadata() {
            if let Ok(modified) = metadata.modified() {
                let modified: chrono::DateTime<chrono::Utc> = modified.into();
                if modified < cutoff {
                    std::fs::remove_file(entry.path())?;
                    removed += 1;
                }
            }
        }
    }

    if removed > 0 {
        tracing::info!(
            "Pruned {} old backups (retention: {} days)",
            removed,
            config.storage.backup_retention_days
        );
    }
    Ok(removed)
}
