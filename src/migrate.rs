use crate::config::MemoryConfig;
use crate::store::Store;
use std::path::Path;

pub struct MigrationResult {
    pub total_files: usize,
    pub imported: usize,
    pub skipped: usize,
    pub errors: Vec<String>,
}

pub fn migrate_from_nodejs(
    source_dir: &Path,
    config: &MemoryConfig,
    dry_run: bool,
) -> anyhow::Result<MigrationResult> {
    let mut result = MigrationResult {
        total_files: 0,
        imported: 0,
        skipped: 0,
        errors: Vec::new(),
    };

    if !source_dir.exists() {
        anyhow::bail!("Source directory does not exist: {}", source_dir.display());
    }

    let entries = std::fs::read_dir(source_dir)?;
    let mut json_files: Vec<_> = entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .collect();

    json_files.sort_by_key(|e| e.file_name());
    result.total_files = json_files.len();

    let store = if !dry_run {
        Some(Store::new(config)?)
    } else {
        None
    };

    for entry in &json_files {
        let path = entry.path();
        let file_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                result
                    .errors
                    .push(format!("{}: read error: {}", file_name, e));
                result.skipped += 1;
                result.skipped += 1;
                continue;
            }
        };

        let value: serde_json::Value = match serde_json::from_str(&content) {
            Ok(v) => v,
            Err(e) => {
                result
                    .errors
                    .push(format!("{}: JSON parse error: {}", file_name, e));
                result.skipped += 1;
                result.skipped += 1;
                continue;
            }
        };

        if dry_run {
            println!("  [DRY-RUN] {} ({} bytes)", file_name, content.len());
            result.imported += 1;
            continue;
        }

        let store = store.as_ref().unwrap();
        match store.import_category(&file_name, &value, "migration") {
            Ok(_) => {
                result.imported += 1;
            }
            Err(e) => {
                result
                    .errors
                    .push(format!("{}: import error: {}", file_name, e));
                result.skipped += 1;
                result.skipped += 1;
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("mcp_test_migrate_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_config(base: &Path) -> MemoryConfig {
        MemoryConfig {
            server: crate::config::ServerConfig {
                mode: "offline".into(),
                host: "127.0.0.1".into(),
                port: 3100,
            },
            storage: crate::config::StorageConfig {
                base_dir: base.to_path_buf(),
                backup_retention_days: 30,
                max_versions: 100,
            },
            acl: crate::config::AclConfig::default(),
            sync: crate::config::SyncConfig::default(),
        }
    }

    #[test]
    fn test_migrate_nonexistent_dir() {
        let result = migrate_from_nodejs(
            Path::new("/tmp/no_such_dir_for_mcp_test"),
            &make_config(Path::new("/tmp")),
            false,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_migrate_dry_run_no_files_written() {
        let src = temp_dir("dryrun");
        let dest = temp_dir("dryrun_dest");

        // Write a valid JSON file in source
        let json_path = src.join("test_category.json");
        std::fs::write(&json_path, r#"{"key": "value"}"#).unwrap();

        let result = migrate_from_nodejs(&src, &make_config(&dest), true).unwrap();
        assert_eq!(result.total_files, 1);
        assert_eq!(result.imported, 1);
        assert_eq!(result.skipped, 0);

        // dest should have no categories dir (dry run)
        assert!(!dest.join("categories").exists());

        let _ = std::fs::remove_dir_all(&src);
        let _ = std::fs::remove_dir_all(&dest);
    }

    #[test]
    fn test_migrate_skips_invalid_json() {
        let src = temp_dir("invalid");
        let dest = temp_dir("invalid_dest");

        let bad = src.join("bad.json");
        std::fs::write(&bad, "not json at all {{{").unwrap();
        let good = src.join("good.json");
        std::fs::write(&good, r#"{"ok": true}"#).unwrap();

        let result = migrate_from_nodejs(&src, &make_config(&dest), true).unwrap();
        assert_eq!(result.total_files, 2);
        assert!(result.skipped >= 1);
        assert!(!result.errors.is_empty());

        let _ = std::fs::remove_dir_all(&src);
        let _ = std::fs::remove_dir_all(&dest);
    }

    #[test]
    fn test_migrate_ignores_non_json() {
        let src = temp_dir("nonjson");
        let dest = temp_dir("nonjson_dest");

        std::fs::write(src.join("data.txt"), "text").unwrap();
        std::fs::write(src.join("valid.json"), r#"{"v": 1}"#).unwrap();

        let result = migrate_from_nodejs(&src, &make_config(&dest), true).unwrap();
        assert_eq!(result.total_files, 1); // only .json counted

        let _ = std::fs::remove_dir_all(&src);
        let _ = std::fs::remove_dir_all(&dest);
    }
}
