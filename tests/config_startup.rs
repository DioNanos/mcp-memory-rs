use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn test_home(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "mcp-memory-startup-{name}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn strict_mode_without_config_fails_without_creating_legacy_store() {
    let home = test_home("strict");
    let output = Command::new(env!("CARGO_BIN_EXE_mcp-memory-rs"))
        .env_clear()
        .env("HOME", &home)
        .env("MCP_MEMORY_MODE", "offline")
        .env("MCP_MEMORY_REQUIRE_CONFIG", "1")
        .current_dir(&home)
        .stdin(Stdio::null())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("MCP_MEMORY_REQUIRE_CONFIG=1"));
    assert!(!home.join(".memory").exists());
    fs::remove_dir_all(home).unwrap();
}

#[test]
fn bare_process_auto_discovers_home_config_and_never_creates_legacy_store() {
    let home = test_home("autodiscovery");
    let managed = home.join("managed-memory");
    let config = home.join(".config/mcp-memory-rs/config.toml");
    fs::create_dir_all(config.parent().unwrap()).unwrap();
    fs::write(
        &config,
        format!(
            "[server]\nmode='offline'\n\n[storage]\nbase_dir='{}'\n",
            managed.display()
        ),
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_mcp-memory-rs"))
        .env_clear()
        .env("HOME", &home)
        .env("MCP_MEMORY_MODE", "offline")
        .env("MCP_MEMORY_REQUIRE_CONFIG", "1")
        .current_dir(&home)
        .stdin(Stdio::null())
        .output()
        .unwrap();

    // stdin chiuso senza handshake e' intenzionalmente un client MCP invalido;
    // l'oggetto del test e' la selezione dello store prima del transport.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Loaded config from"), "{stderr}");
    assert!(!stderr.contains("MCP_MEMORY_REQUIRE_CONFIG=1 but no config was found"));
    assert!(managed.join("memory.db").is_file());
    assert!(!home.join(".memory").exists());
    fs::remove_dir_all(home).unwrap();
}
