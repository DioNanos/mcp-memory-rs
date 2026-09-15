# Changelog

## 0.3.2 — 2026-09-15

Cross-process write safety. Fixes `SQLite error: database is locked` under
concurrent writers and the lost-update window in `memory_append`.

### Fixed

- Every mutation (`write`, `append`, `delete`, import, sync upsert, dirty
  queue, sync state) now runs inside a `BEGIN IMMEDIATE` transaction, so the
  SQLite write lock serializes writers across processes — including the
  read-modify-write of the category JSON. Previously the JSON was rewritten
  outside any lock and each SQL statement autocommitted on its own.
- Lock acquisition is retried with jittered backoff inside one total budget
  (`storage.lock_deadline_ms`, default 10 s) instead of failing on the first
  `SQLITE_BUSY`; on expiry the call returns `MemoryError::Busy { waited_ms,
  code }`. `SQLITE_LOCKED` (same-connection contention) is never retried and
  is reported with its extended code.
- Category JSON files are written crash-safely (staged file → commit → rename)
  with unambiguous recovery on open and read; deletes park the file first and
  complete after commit. Interrupted writes and deletes can no longer leave
  the JSON and SQLite disagreeing.
- Append latency: SQLite statement journals and temp b-trees now live in
  memory (`temp_store=MEMORY`, 16 MB page cache). Measured on a 320-entry log:
  ~12 000 `pwrite` per append → ~200.

### Added

- `[storage]` options `journal_mode` (default `wal`), `synchronous` (default
  `full`), `sqlite_busy_timeout_ms` (200), `lock_deadline_ms` (10000).
- `mcp-memory-rs --snapshot <dir>` — consistent snapshot (writers blocked,
  `VACUUM INTO` + category files + `manifest.json`).
- `mcp-memory-rs --restore <dir> --stopped` — restore a snapshot, parking the
  previous store under `backups/restore-<ts>/`.
- `--version` prints the bundled SQLite version.
- Multi-process tests: 8 processes × 40 appends with zero loss/duplicates,
  lock deadline, crash before/after commit, interrupted delete, snapshot with
  an un-checkpointed WAL restored identically, tampered snapshot refused
  without touching the live store.
- `--restore` verifies the snapshot first (`integrity_check`, one JSON file
  per category row with the recorded hash, no extra files, manifest count);
  snapshot and restore fsync every copied file. BEGIN and COMMIT share one
  lock budget; the staged file's directory entry is fsynced before COMMIT.
- Readers never rename or delete files (an earlier draft let them promote a
  staged file, which could complete a *newer* uncommitted stage carrying the
  same bytes); they serve whichever file carries the committed hash.
- `memory_write merge=true`, the log/memory kind check and `expected_hash`
  now run inside the write transaction (`Store::write_with`): two processes
  patching disjoint keys cannot lose each other's update.
- `--snapshot --with-backups` copies `backups/` too; the manifest states the
  coverage (`includes_backups`, `backup_files`). Restore parks the previous
  store under `<base_dir>/restore-parked-<ts>/`.
- Tests: readers vs alternating writers (same bytes re-committed), 8
  processes × 40 merges keep every key, history payloads restored into a
  fresh store from a snapshot with backups.
- `memory_read` (MCP and HTTP) returns content and metadata as one coherent
  pair (`Store::read_with_meta`): the hash is always the hash of the bytes
  returned, so an `expected_hash` write can never carry a stale value over a
  newer commit. Readers retry briefly, bounded, while a writer is between
  COMMIT and rename.
- SQLite's per-statement busy handler is capped to what is left of the lock
  budget before every BEGIN/COMMIT attempt: no statement can wait past
  `lock_deadline_ms`.
- Snapshot manifests list every included backup file with its sha256;
  `verify()` (after snapshot, before restore) checks each one, so an altered
  or unlisted backup is refused.
- `mcp-memory-rs --verify-store`: read-only consistency report of a live
  store (integrity, db-vs-json per category, orphan and pending files).
- Restore is two-phase: the snapshot is copied into a staging directory
  beside the store and verified there; only then are the live pieces parked
  and the staged ones renamed into place. A failure before the swap leaves
  the live store byte-untouched. Verification also rejects non-regular
  files and an unreadable `backups/` directory. **Limit**: the swap is a
  few renames, copy-safe but not atomic as a group; a `RESTORE_IN_PROGRESS`
  marker in `base_dir` covers it — the store refuses to open while it
  exists, and recovery is by hand from `restore-parked-<ts>/` and
  `restore-staging-<ts>/` with every process stopped.
- Sync imports keep the store invariant: `content_hash` is always the hash
  of the local bytes; the peer's hash (computed over the peer's own
  serialization) is remembered in `sync_state` (`remote_hash:<category>`)
  so the next sync recognizes an already-imported remote version without
  a loop, and an interrupted sync write is recovered like any other.
  Previously the row held the peer's hash while the file held the local
  serialization — a divergence recreated at every sync.
- `--adopt-json --plan <out.json>` (read-only: divergent categories with
  database hash, file hash, kind, backup of the database version, semantic
  delta) and `--adopt-json --apply <plan.json>` (under the lock, with
  preconditions on both hashes, a copy of the adopted file in `backups/`,
  a `recovery` version row, reindex, dirty): recovery for stores left by
  the pre-0.3.2 protocol, where the file is newer than the row.
- The sync alias (`remote_hash:<category>`) only counts while the local
  bytes are still exactly the imported ones and the proposing device is the
  one that sent them; a local write voids it (a newer remote version is then
  re-evaluated by the conflict policy), `delete` removes it.
- `--adopt-json --apply` adopts the whole plan in one transaction (all or
  nothing) and a rerun of an applied plan is a no-op (+0 versions). Log
  deltas report `changed_common_ids` and whether the file is a well-formed
  log (numeric, strictly increasing ids).
- Restore removes its staging directory on any phase-1 failure.
- `--verify-store` is read-only by construction: it opens SQLite with
  `SQLITE_OPEN_READ_ONLY` and never constructs a `Store` (no PRAGMA, no
  schema init, no reconcile), so it can inspect a store still owned by an
  older binary without changing a byte or settling pending files. A store
  with pending staged/parked files, or an interrupted restore, is not clean.

### Changed

- rusqlite 0.33 → 0.40 (bundled SQLite refreshed; `size_bytes` is bound as
  `i64`, SQLite has no unsigned integers).

### Upgrade notes

- The first open with the new binary switches the database to WAL mode
  (persistent). Older binaries still using the same file keep working but do
  not take the new lock protocol: restart them.
- Never copy or delete `memory.db-wal` by hand; use `--snapshot`.

## 0.3.1 — 2026-07-18

Configuration safety hardening. Managed installations no longer split into a
stale `~/.memory` store when a subprocess omits `MCP_MEMORY_CONFIG`.

### Added

- Automatic config discovery in this order: explicit `MCP_MEMORY_CONFIG`,
  `$XDG_CONFIG_HOME/mcp-memory-rs/config.toml`,
  `$HOME/.config/mcp-memory-rs/config.toml`, then the legacy cwd-local
  `memory-config.toml`.
- `MCP_MEMORY_REQUIRE_CONFIG=1` strict mode. When enabled, startup fails before
  creating storage if no config can be found.
- Regression tests proving that strict startup cannot create `~/.memory` and
  that a bare subprocess converges on the managed config/store.

### Changed

- Explicit, discovered, unreadable, or malformed config files now fail with a
  clear error instead of silently falling back to defaults.

## 0.3.0 — 2026-06-17

Append-only log categories. No breaking changes — existing memory categories
and `memory_write`/`merge` semantics are untouched.

### Added

- **`memory_append` tool & log categories.** A category can now be a bounded,
  append-only **log** (`{"_kind":"log","_retention":{…},"entries":[…]}`) instead
  of a declarative memory map. `memory_append` stamps each entry with a UTC
  timestamp and a monotonic id, and auto-prunes by retention (`max_entries`,
  default 200, and/or `max_age_days`). This stops append-only event streams
  (e.g. session journals) from bloating memory categories without bound.

### Changed

- **Kind boundary, fail-closed.** `memory_write`, the HTTP `/api/v1/write`
  endpoint, and the migration `import_category` all refuse to clobber a log
  category, and refuse the write if the category kind cannot be determined
  (corrupt file) rather than degrading to "not a log". `memory_append` on a
  memory category is rejected — never a silent conversion. Sync replication is
  intentionally exempt so logs still propagate between devices.

### Notes

- A log is append-only **per store**; cross-device sync is whole-category
  last-write-wins, not a CRDT union. Concurrent cross-device appends can drop
  one side until the next prune. Entry ids reserve a future union/tombstone
  merge.

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
