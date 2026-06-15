# Changelog

## 0.2.2 — 2026-06-15

Packaging and docs. No functional or API changes.

### Changed

- README now surfaces CI and test-count status badges.
- Applied `rustfmt` so the CI format gate passes cleanly.

## 0.2.1 — 2026-06-12

AI-first discoverability. No breaking changes.

### Added

- **`initialize` instructions.** The server now returns an AI-first
  instructions string describing the memory model and the entry-point tools
  (`memory_list` → `memory_read` → `memory_write`), so a weak client model can
  drive the server without prior knowledge of its surface.
- README **"AI client compatibility"** section and a
  `default_tools_approval_mode = "approve"` line in the Codex config snippet.

### Changed

- **`rmcp` bumped 1.3 → 1.7** for a lenient MCP handshake. Under 1.3.0 a client
  that omitted `notifications/initialized` could hang on `tools/list` with no
  error; 1.7.0 (already used by the companion `mcp-vl-msa-rs`) does not.
- **`memory_read` now teaches its parameter.** The tool and field descriptions
  state explicitly that the parameter is `category`, not `key`.
- **Better not-found errors.** Reading a missing category returns
  `Category not found: <name>; available: [a, b, …]` (capped at 30) — or a
  "no categories exist yet" hint on an empty store — so a model can self-correct
  without a separate `memory_list`.

### Internal

- Regression test pinning `readOnlyHint` on the eleven read-only tools and its
  absence on the six mutating tools.

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
