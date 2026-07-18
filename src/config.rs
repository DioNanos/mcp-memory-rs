use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Server modes accepted by `MCP_MEMORY_MODE` env var.
const VALID_MEMORY_MODES: &[&str] = &["offline", "http"];
const CONFIG_RELATIVE_PATH: &str = "mcp-memory-rs/config.toml";
const LEGACY_CWD_CONFIG: &str = "memory-config.toml";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub acl: AclConfig,
    #[serde(default)]
    pub sync: SyncConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_base_dir")]
    pub base_dir: PathBuf,
    #[serde(default = "default_backup_retention")]
    pub backup_retention_days: u32,
    #[serde(default = "default_max_versions")]
    pub max_versions: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclConfig {
    #[serde(default)]
    pub admin_devices: Vec<String>,
    #[serde(default = "default_device_name")]
    pub device_name: String,
    /// List of device names that are allowed to write their own category.
    /// Each entry `<name>` allows device `<name>` to write category `<name>`
    /// and `workflow_<name>`. Add the device_name of every node in your fleet.
    /// Default: empty (devices can still write agent-scoped `<prefix>_*` categories).
    #[serde(default)]
    pub device_categories: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    #[serde(default)]
    pub remote_url: String,
    #[serde(default)]
    pub remote_token_file: String,
    #[serde(default = "default_sync_interval")]
    pub sync_interval_secs: u64,
    #[serde(default = "default_conflict_strategy")]
    pub conflict_strategy: String,
}

fn default_mode() -> String {
    "offline".into()
}
fn default_host() -> String {
    "127.0.0.1".into()
}
fn default_port() -> u16 {
    3100
}
fn default_base_dir() -> PathBuf {
    PathBuf::from("~/.memory")
}
fn default_backup_retention() -> u32 {
    30
}
fn default_max_versions() -> u32 {
    100
}
fn default_device_name() -> String {
    std::env::var("MCP_DEVICE").unwrap_or_else(|_| {
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".into())
    })
}
fn default_sync_interval() -> u64 {
    300
}
fn default_conflict_strategy() -> String {
    "last_write_wins".into()
}

impl Default for AclConfig {
    fn default() -> Self {
        Self {
            admin_devices: vec![],
            device_name: default_device_name(),
            device_categories: vec![],
        }
    }
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            remote_url: String::new(),
            sync_interval_secs: default_sync_interval(),
            conflict_strategy: default_conflict_strategy(),
            remote_token_file: String::new(),
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                mode: default_mode(),
                host: default_host(),
                port: default_port(),
            },
            storage: StorageConfig {
                base_dir: default_base_dir(),
                backup_retention_days: default_backup_retention(),
                max_versions: default_max_versions(),
            },
            acl: AclConfig {
                admin_devices: vec![],
                device_name: default_device_name(),
                device_categories: vec![],
            },
            sync: SyncConfig {
                remote_url: String::new(),
                remote_token_file: String::new(),
                sync_interval_secs: default_sync_interval(),
                conflict_strategy: default_conflict_strategy(),
            },
        }
    }
}

impl MemoryConfig {
    /// Backward-compat panicking wrapper. Prefer [`Self::try_load`] in new code
    /// to surface env validation errors (e.g. invalid `MCP_MEMORY_MODE`) without
    /// aborting the process.
    pub fn load() -> Self {
        Self::try_load()
            .expect("MemoryConfig::load failed; use try_load() for non-panicking variant")
    }

    /// Load configuration from TOML + env overrides.
    ///
    /// Resolution order is explicit `MCP_MEMORY_CONFIG`, XDG config, the
    /// conventional `$HOME/.config` path, then the legacy cwd-local
    /// `memory-config.toml`.  This makes a bare subprocess converge on the same
    /// managed store as MCP clients instead of silently creating `~/.memory`.
    /// Set `MCP_MEMORY_REQUIRE_CONFIG=1` to fail closed when no config exists.
    pub fn try_load() -> Result<Self> {
        let explicit = match std::env::var_os("MCP_MEMORY_CONFIG") {
            Some(value) if value.is_empty() => {
                bail!("MCP_MEMORY_CONFIG is set but empty")
            }
            value => value.map(PathBuf::from),
        };
        let require_config = parse_bool_env("MCP_MEMORY_REQUIRE_CONFIG")?;
        let xdg = std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from);
        let home = std::env::var_os("HOME").map(PathBuf::from);
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

        if let Some(config_path) = discover_config_path(explicit, xdg, home, &cwd) {
            return Self::load_config_file(&config_path);
        }
        if require_config {
            bail!(
                "MCP_MEMORY_REQUIRE_CONFIG=1 but no config was found; set MCP_MEMORY_CONFIG or create ~/.config/mcp-memory-rs/config.toml"
            );
        }

        // Backward-compatible standalone mode. Managed deployments should use
        // the auto-discovered XDG config and strict mode above.
        let mut cfg = Self::default();
        cfg.apply_env_overrides()?;
        cfg.warn_if_legacy_store_orphan();
        Ok(cfg)
    }

    fn load_config_file(config_path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(config_path)
            .with_context(|| format!("cannot read config {}", config_path.display()))?;
        let mut cfg = toml::from_str::<Self>(&content)
            .with_context(|| format!("invalid config {}", config_path.display()))?;
        tracing::info!("Loaded config from {}", config_path.display());
        cfg.apply_env_overrides()?;
        cfg.warn_if_legacy_store_orphan();
        Ok(cfg)
    }

    /// Emit a stderr advisory if the legacy `$HOME/.memory/memory.db` store is
    /// present but the active configuration points to a different `base_dir`.
    ///
    /// Scope: catches the common mis-configuration where a process is spawned
    /// without `MCP_MEMORY_CONFIG`/`MCP_MEMORY_DIR` and the default fallback
    /// (`~/.memory`) silently diverges from the intended stack-managed store.
    /// No warning is emitted when the active `base_dir` is exactly the legacy
    /// path (fallback used intentionally) or when no legacy DB exists.
    fn warn_if_legacy_store_orphan(&self) {
        let Ok(home) = std::env::var("HOME") else {
            return;
        };
        let legacy = PathBuf::from(home).join(".memory");
        let legacy_db = legacy.join("memory.db");
        if !legacy_db.exists() {
            return;
        }
        let active = self.resolved_base_dir();
        if active == legacy {
            return;
        }
        tracing::warn!(
            "legacy memory store present at {} but active base_dir is {}; ensure MCP_MEMORY_CONFIG is set correctly, or remove the legacy store if no longer needed",
            legacy.display(),
            active.display()
        );
    }

    fn apply_env_overrides(&mut self) -> Result<()> {
        if let Ok(mode) = std::env::var("MCP_MEMORY_MODE") {
            if !VALID_MEMORY_MODES.contains(&mode.as_str()) {
                bail!(
                    "MCP_MEMORY_MODE='{}' is invalid; expected one of: {}",
                    mode,
                    VALID_MEMORY_MODES.join(", ")
                );
            }
            self.server.mode = mode;
        }
        if let Ok(host) = std::env::var("MCP_MEMORY_HOST") {
            self.server.host = host;
        }
        if let Ok(port) = std::env::var("MCP_MEMORY_PORT") {
            if let Ok(port) = port.parse::<u16>() {
                self.server.port = port;
            }
        }
        if let Ok(dir) = std::env::var("MCP_MEMORY_DIR") {
            self.storage.base_dir = PathBuf::from(dir);
        }
        if let Ok(device) = std::env::var("MCP_DEVICE") {
            self.acl.device_name = device;
        }
        if let Ok(remote_url) = std::env::var("MCP_MEMORY_REMOTE_URL") {
            self.sync.remote_url = remote_url;
        }
        if let Ok(token_file) = std::env::var("MCP_MEMORY_REMOTE_TOKEN_FILE") {
            self.sync.remote_token_file = token_file;
        }
        Ok(())
    }

    pub fn resolved_base_dir(&self) -> PathBuf {
        let path = if self.storage.base_dir.starts_with("~") {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
            PathBuf::from(
                self.storage
                    .base_dir
                    .to_string_lossy()
                    .replacen('~', &home, 1),
            )
        } else {
            self.storage.base_dir.clone()
        };
        path
    }

    pub fn db_path(&self) -> PathBuf {
        self.resolved_base_dir().join("memory.db")
    }

    pub fn categories_dir(&self) -> PathBuf {
        self.resolved_base_dir().join("categories")
    }

    pub fn backups_dir(&self) -> PathBuf {
        self.resolved_base_dir().join("backups")
    }

    pub fn remote_token(&self) -> Option<String> {
        if let Ok(token) = std::env::var("MCP_MEMORY_REMOTE_TOKEN") {
            let token = token.trim().to_string();
            if !token.is_empty() {
                return Some(token);
            }
        }
        if self.sync.remote_token_file.trim().is_empty() {
            return None;
        }
        std::fs::read_to_string(&self.sync.remote_token_file)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }
}

fn discover_config_path(
    explicit: Option<PathBuf>,
    xdg_config_home: Option<PathBuf>,
    home: Option<PathBuf>,
    cwd: &Path,
) -> Option<PathBuf> {
    // An explicit path is authoritative even when missing: load_config_file
    // will return a clear error instead of silently falling back.
    if explicit.is_some() {
        return explicit;
    }

    let mut candidates = Vec::with_capacity(3);
    if let Some(xdg) = xdg_config_home.filter(|p| !p.as_os_str().is_empty()) {
        candidates.push(xdg.join(CONFIG_RELATIVE_PATH));
    }
    if let Some(home) = home {
        let conventional = home.join(".config").join(CONFIG_RELATIVE_PATH);
        if !candidates.contains(&conventional) {
            candidates.push(conventional);
        }
    }
    candidates.push(cwd.join(LEGACY_CWD_CONFIG));
    candidates.into_iter().find(|path| path.is_file())
}

fn parse_bool_env(name: &str) -> Result<bool> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(false);
    };
    let value = value.to_string_lossy();
    parse_bool_value(name, &value)
}

fn parse_bool_value(name: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" | "" => Ok(false),
        _ => bail!(
            "{}='{}' is invalid; expected one of: 1, true, yes, on, 0, false, no, off",
            name,
            value
        ),
    }
}

// Minimal hostname fallback (avoid extra crate)
mod hostname {
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    pub fn get() -> Result<OsString, ()> {
        #[cfg(unix)]
        {
            let mut buf = [0u8; 256];
            let len = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut _, buf.len()) };
            if len == 0 {
                Ok(OsString::from_vec(buf[..strlen(&buf)].to_vec()))
            } else {
                Err(())
            }
        }
        #[cfg(not(unix))]
        {
            Err(())
        }
    }
    fn strlen(s: &[u8]) -> usize {
        s.iter().position(|&b| b == 0).unwrap_or(s.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "mcp-memory-config-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn test_default_config_values() {
        let cfg = MemoryConfig::default();
        assert_eq!(cfg.server.mode, "offline");
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 3100);
        assert_eq!(cfg.storage.backup_retention_days, 30);
        assert_eq!(cfg.storage.max_versions, 100);
        assert_eq!(cfg.sync.sync_interval_secs, 300);
        assert_eq!(cfg.sync.conflict_strategy, "last_write_wins");
        assert_eq!(cfg.sync.remote_token_file, "");
    }

    #[test]
    fn test_resolved_base_dir_tilde_expansion() {
        let mut cfg = MemoryConfig::default();
        cfg.storage.base_dir = PathBuf::from("~/test_memory");
        let resolved = cfg.resolved_base_dir();
        assert!(!resolved.to_string_lossy().starts_with('~'));
        assert!(resolved.to_string_lossy().contains("test_memory"));
    }

    #[test]
    fn test_db_path_and_categories_dir() {
        let mut cfg = MemoryConfig::default();
        cfg.storage.base_dir = PathBuf::from("/tmp/mcp_test_paths");
        assert_eq!(
            cfg.db_path(),
            PathBuf::from("/tmp/mcp_test_paths/memory.db")
        );
        assert_eq!(
            cfg.categories_dir(),
            PathBuf::from("/tmp/mcp_test_paths/categories")
        );
        assert_eq!(
            cfg.backups_dir(),
            PathBuf::from("/tmp/mcp_test_paths/backups")
        );
    }

    #[test]
    fn test_config_from_toml_string() {
        let toml = r#"
[server]
mode = "http"
host = "0.0.0.0"
port = 8080

[storage]
base_dir = "/data/memory"
backup_retention_days = 7
max_versions = 50

[acl]
admin_devices = ["server-a", "device-b"]
device_name = "device-b"
device_categories = ["server-a", "device-b", "tablet-c"]

[sync]
remote_url = "http://127.0.0.1:3110"
remote_token_file = "/tmp/token"
"#;
        let cfg: MemoryConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.server.mode, "http");
        assert_eq!(cfg.server.port, 8080);
        assert_eq!(cfg.storage.base_dir, PathBuf::from("/data/memory"));
        assert_eq!(cfg.acl.admin_devices, vec!["server-a", "device-b"]);
        assert_eq!(cfg.acl.device_name, "device-b");
        assert_eq!(
            cfg.acl.device_categories,
            vec!["server-a", "device-b", "tablet-c"]
        );
        assert_eq!(cfg.sync.remote_url, "http://127.0.0.1:3110");
        assert_eq!(cfg.sync.remote_token_file, "/tmp/token");
    }

    #[test]
    fn test_env_overrides_apply_to_loaded_config() {
        let mut cfg = MemoryConfig::default();
        cfg.server.mode = "http".into();
        cfg.server.port = 3110;
        cfg.acl.device_name = "from_config".into();

        let old_mode = std::env::var("MCP_MEMORY_MODE").ok();
        let old_device = std::env::var("MCP_DEVICE").ok();
        std::env::set_var("MCP_MEMORY_MODE", "offline");
        std::env::set_var("MCP_DEVICE", "tablet-c");

        cfg.apply_env_overrides()
            .expect("apply_env_overrides should succeed with valid env values");

        match old_mode {
            Some(value) => std::env::set_var("MCP_MEMORY_MODE", value),
            None => std::env::remove_var("MCP_MEMORY_MODE"),
        }
        match old_device {
            Some(value) => std::env::set_var("MCP_DEVICE", value),
            None => std::env::remove_var("MCP_DEVICE"),
        }

        assert_eq!(cfg.server.mode, "offline");
        assert_eq!(cfg.server.port, 3110);
        assert_eq!(cfg.acl.device_name, "tablet-c");
    }

    #[test]
    fn test_config_discovery_prefers_explicit_then_xdg_then_home_then_cwd() {
        let root = test_dir("discovery");
        let xdg = root.join("xdg");
        let home = root.join("home");
        let cwd = root.join("cwd");
        let xdg_cfg = xdg.join(CONFIG_RELATIVE_PATH);
        let home_cfg = home.join(".config").join(CONFIG_RELATIVE_PATH);
        let cwd_cfg = cwd.join(LEGACY_CWD_CONFIG);
        for path in [&xdg_cfg, &home_cfg, &cwd_cfg] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, "[storage]\nbase_dir='/tmp/test'\n").unwrap();
        }

        assert_eq!(
            discover_config_path(None, Some(xdg.clone()), Some(home.clone()), &cwd),
            Some(xdg_cfg)
        );
        fs::remove_file(xdg.join(CONFIG_RELATIVE_PATH)).unwrap();
        assert_eq!(
            discover_config_path(None, Some(xdg), Some(home.clone()), &cwd),
            Some(home_cfg)
        );
        fs::remove_file(home.join(".config").join(CONFIG_RELATIVE_PATH)).unwrap();
        assert_eq!(
            discover_config_path(None, None, Some(home), &cwd),
            Some(cwd_cfg)
        );

        let explicit = root.join("explicit-missing.toml");
        assert_eq!(
            discover_config_path(Some(explicit.clone()), None, None, &cwd),
            Some(explicit)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn test_config_discovery_returns_none_without_any_config() {
        let root = test_dir("none");
        assert_eq!(
            discover_config_path(None, Some(root.join("xdg")), Some(root.join("home")), &root),
            None
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn test_require_config_flag_is_strict_and_validated() {
        for value in ["1", "true", "YES", "on"] {
            assert!(parse_bool_value("MCP_MEMORY_REQUIRE_CONFIG", value).unwrap());
        }
        for value in ["0", "false", "NO", "off", ""] {
            assert!(!parse_bool_value("MCP_MEMORY_REQUIRE_CONFIG", value).unwrap());
        }
        assert!(parse_bool_value("MCP_MEMORY_REQUIRE_CONFIG", "maybe").is_err());
    }

    #[test]
    fn test_discovered_invalid_config_fails_instead_of_falling_back() {
        let root = test_dir("invalid");
        let config = root.join("config.toml");
        fs::write(&config, "not valid toml = [").unwrap();
        let err = MemoryConfig::load_config_file(&config).unwrap_err();
        assert!(err.to_string().contains("invalid config"));
        fs::remove_dir_all(root).unwrap();
    }
}
