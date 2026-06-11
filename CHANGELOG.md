# Changelog

## 0.2.0 — 2026-06-12

First publishable release.

### Breaking changes (vs internal 0.1.x)

- **HTTP mode requires `MCP_MEMORY_TOKEN`.** The server previously fell back
  to a default token with a warning; it now refuses to start without an
  explicit token. Stdio (offline) mode is unaffected.

  Migration: `export MCP_MEMORY_TOKEN=<secret>` before starting with `--http`
  (or in the service unit environment).

- **Device write ACL is config-driven.** The previous hardcoded device list is
  gone. A device may write its own `<name>` / `workflow_<name>` categories
  only if `<name>` is listed in `[acl] device_categories` in the config.
  With the new default (empty list) no per-device categories are writable —
  fail closed. Admin devices and agent-scoped `<x>-agent` → `<x>_*` writes are
  unaffected.

  Migration: add every fleet node to the config of each instance, e.g.

  ```toml
  [acl]
  admin_devices = ["server-a"]
  device_name = "server-a"
  device_categories = ["server-a", "laptop-b", "phone-c"]
  ```

### Changed

- License switched from MIT to Apache-2.0.
- `config/example.toml` fully documented; `base_dir` example fixed (`~` is
  expanded, `$HOME` never was).
- HTTP handlers return 500 instead of panicking on a poisoned store lock.
- `json-patch` dependency dropped; `tower` moved to dev-dependencies.

### Added

- README, LICENSE, CI (fmt + clippy `-D warnings` + tests + release build).
- Package metadata for crates.io.
