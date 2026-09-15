# mcp-memory-rs

[![CI](https://img.shields.io/github/actions/workflow/status/DioNanos/mcp-memory-rs/ci.yml?branch=main&style=flat-square&logo=githubactions&logoColor=white&label=CI)](https://github.com/DioNanos/mcp-memory-rs/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/badge/tests-106%20passing-2ea44f?style=flat-square)](https://github.com/DioNanos/mcp-memory-rs/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![Local First](https://img.shields.io/badge/local--first-no%20cloud%20deps-111827?style=flat-square)](#why)

Local-first MCP memory server in pure Rust. Persistent, versioned, queryable
memory for AI agents — owned by the user, not by a platform.

State lives on your disk as plain JSON categories backed by SQLite (FTS5).
Every write is versioned and backed up. A fleet of devices syncs through an
optional HTTP endpoint with merkle manifests and optimistic concurrency.
No cloud account, no embedding service, no network dependency in the default
mode.

## Why

Agent memory tied to a vendor dies with the vendor session. This server keeps
the agent's long-lived state — identity, projects, infrastructure notes,
per-device workflows — in files you can read, grep, diff, and back up
yourself. Any MCP client (Claude Code, Codex, or anything speaking MCP stdio)
gets the same memory; swapping the model does not lose the state.

Design choices that follow from that:

- **Local-first**: the stdio server works fully offline against local storage.
  Sync is a separate, explicit step — never a hidden dependency.
- **Plain storage**: one JSON file per category plus a SQLite index. The
  database can be rebuilt from the files; the files are the truth.
- **Versioning by default**: every write keeps history; deletes create a
  backup first. `memory_history` and `memory_compact` manage the tail.
- **Concurrency-safe writes**: `expected_hash` turns clobbering races between
  concurrent agents into explicit conflicts.
- **Zero ML in the core**: full-text search is SQLite FTS5 with BM25;
  `memory_search_semantic` adds an optional TF-IDF hybrid. No model downloads.

> **The pair:** for *corpus* recall (chunked documents, BM25 retrieval,
> original-text injection) see the companion server
> [mcp-vl-msa-rs](https://github.com/DioNanos/mcp-vl-msa-rs). This server holds
> the curated agent state; that one holds the queryable corpus.

```mermaid
flowchart LR
    A["AI agent<br/>(any MCP client)"]
    A -->|"curated state<br/>read / write / sync"| M["mcp-memory-rs<br/><i>the notebook</i>"]
    A -->|"corpus recall<br/>index / search / fetch"| V["mcp-vl-msa-rs<br/><i>the library</i>"]
    M --- D1[("JSON categories<br/>SQLite FTS5")]
    V --- D2[("tantivy BM25<br/>collections")]
```

## Install

**Prebuilt binary** (recommended) — download the archive for your platform from
the [latest release](https://github.com/DioNanos/mcp-memory-rs/releases/latest),
extract, and point your MCP client at the binary:

```bash
tar xzf mcp-memory-rs-x86_64-unknown-linux-gnu.tar.gz
install -m755 mcp-memory-rs-*/mcp-memory-rs ~/.local/bin/
```

Prebuilt targets (Linux + Android): `x86_64-unknown-linux-gnu`,
`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-gnu`,
`aarch64-unknown-linux-musl` (edge / ARM / Termux), `aarch64-linux-android`.

**macOS**: no prebuilt binary is shipped (it would need Apple code-signing).
Install from source instead — `cargo install` compiles it on your Mac in one
command, no signing needed:

```bash
cargo install --git https://github.com/DioNanos/mcp-memory-rs --locked
```

`--locked` uses the committed `Cargo.lock` (reproducible build).

## Quick start

```bash
cargo build --release

# stdio MCP server, offline mode (default)
./target/release/mcp-memory-rs
```

Claude Code (`~/.claude.json`) or any MCP client:

```json
{
  "mcpServers": {
    "memory": {
      "command": "/path/to/mcp-memory-rs",
      "env": {
        "MCP_MEMORY_CONFIG": "/home/me/.config/mcp-memory-rs/config.toml",
        "MCP_MEMORY_REQUIRE_CONFIG": "1",
        "MCP_MEMORY_MODE": "offline",
        "MCP_DEVICE": "my-laptop"
      }
    }
  }
}
```

Codex (`~/.codex/config.toml`):

```toml
[mcp_servers.memory]
command = "/path/to/mcp-memory-rs"
env = { MCP_MEMORY_CONFIG = "/home/me/.config/mcp-memory-rs/config.toml", MCP_MEMORY_REQUIRE_CONFIG = "1", MCP_MEMORY_MODE = "offline", MCP_DEVICE = "my-laptop" }
# auto-approve read-only tools (memory_read/list/search/…); writes still gated
default_tools_approval_mode = "approve"
```

Configuration is TOML — copy [`config/example.toml`](config/example.toml) to
`~/.config/mcp-memory-rs/config.toml` and adjust it. The server auto-discovers
that XDG-style path. Environment variables override file values (`MCP_DEVICE`,
`MCP_MEMORY_MODE`, `MCP_MEMORY_DIR`, …); managed deployments should set
`MCP_MEMORY_REQUIRE_CONFIG=1` so a missing config fails closed.

## Memory model

Memory is organized in **categories**: named JSON documents (`base`,
`projects`, `my-laptop`, `workflow_my-laptop`, …). Reads return the document
plus metadata (content hash, size, last writer, timestamp). Writes replace the
category or, with `merge=true`, patch it per top-level key.

### Access control

Multi-device fleets get a small, explicit ACL (`[acl]` in the config):

| Rule | Effect |
|---|---|
| `admin_devices` | Listed devices write everything. |
| `device_categories` | Device `name` writes category `name` and `workflow_name` — its own namespace only. |
| agent scope | Device `foo-agent` writes `foo_*` categories. |
| everyone | All devices read everything. |

Unknown categories are denied for non-admin writers — fail closed.

### Fleet sync

Each node runs local-first; one node (or any number) exposes the HTTP API as a
sync remote. `sync_manifest` builds a merkle-rooted fingerprint of all
categories; `sync_diff` compares manifests; `memory_sync` pushes dirty
categories or pulls remote changes. Conflicts between nodes are resolved by
the configured `conflict_strategy` (last-write-wins by default);
`expected_hash` preconditions protect direct writes (MCP and HTTP), not the
sync envelope.

The HTTP API is an **admin/sync plane**: the bearer token grants full access
to every category. Per-category ACL applies on the MCP stdio surface, where
the device identity is known locally; HTTP callers are identified only by the
shared token, so no per-device ACL is enforced there.

The HTTP server **requires** `MCP_MEMORY_TOKEN` and refuses to start without
it. Bind it to loopback (the default) and tunnel between nodes; do not expose
it to the public internet.

```bash
MCP_MEMORY_TOKEN=<secret> ./target/release/mcp-memory-rs --http
```

## Tool surface

| Tool | Description |
|---|---|
| `memory_read` | Read a category (optional field filtering). |
| `memory_write` | Replace or merge-patch a category; versioned; `expected_hash` precondition. |
| `memory_append` | Append a timestamped entry to a bounded append-only **log** category; auto-prunes by `max_entries`/`max_age_days`. For event streams / session journals, so they don't bloat memory categories. |
| `memory_delete` | Delete a category (backup created first). |
| `memory_list` | All categories with hash/size/last-update metadata. |
| `memory_search` | FTS5 full-text search, BM25 ranking, category/date/actor filters, snippets. |
| `memory_search_semantic` | Hybrid TF-IDF + FTS5 search. |
| `memory_history` | Version history of a category. |
| `memory_delta` | Changes since a known hash (cheap polling). |
| `memory_context` | Multi-category warmup read, token-budget oriented. |
| `memory_compact` | Prune old versions and backups. |
| `memory_status` | Local-first status: dirty queue, manifest hash, last sync. |
| `memory_doctor` | Diagnostics: paths, database, categories, sync config. |
| `sync_manifest` | Merkle-rooted manifest of all categories. |
| `sync_diff` | Compare local vs remote manifest: push/pull/conflict sets. |
| `sync_push` / `sync_pull` | Export/import sync envelopes. |
| `memory_sync` | One sync step against the configured remote (`push_dirty` / `pull_remote`). |

The same surface is available over HTTP (`/api/v1/*`) for non-MCP consumers;
`/health` is unauthenticated, everything else requires the bearer token.

### AI client compatibility

The server is built to be self-explanatory to a weak client model:

- At `initialize` it returns an **instructions** string describing the memory
  model and the entry-point tools. Some lightweight clients ignore this field;
  if your client never surfaces it, read the tool descriptions instead — they
  carry the same guidance (e.g. `memory_read` takes `category`, not `key`).
- Read-only tools are annotated `readOnlyHint`, so a client such as Codex can
  auto-approve them. On Codex, an `unsupported call` / `user cancelled`
  result usually means the tool-approval gate fired, not a server fault — set
  `default_tools_approval_mode = "approve"` (see the Codex snippet above).
- The server speaks the standard MCP handshake. A client must complete
  `initialize` and send `notifications/initialized` like any MCP client.

## Storage layout

With no config and strict mode disabled, standalone `base_dir` remains
`~/.memory` for compatibility. The auto-discovered example config uses
`~/.local/state/mcp-memory-rs` (XDG-style). Config resolution is: explicit
`MCP_MEMORY_CONFIG`, `$XDG_CONFIG_HOME`, `$HOME/.config`, legacy cwd-local
`memory-config.toml`, then standalone defaults. Either way the layout is:

```
<base_dir>/
├── categories/   # one .json file per category — the source of truth
├── backups/      # automatic pre-delete/pre-overwrite backups
├── memory.db     # SQLite: FTS5 index, versions, sync state (rebuildable)
└── memory.db-wal # write-ahead log (WAL mode; do not copy or delete by hand)
```

### Concurrency (many processes, one store)

Every writer — one stdio server per client plus the optional `--http`
instance — opens its own SQLite connection on the same file. Since 0.3.2 each
mutation runs inside a `BEGIN IMMEDIATE` transaction: SQLite is the mutex that
serializes writers *across processes*, so an append (read → modify → write) can
never lose another process's entry. Lock acquisition is retried with jittered
backoff inside one total budget; when it expires the call fails with a clear
`Store busy … retry later` error instead of hanging or corrupting anything.

```toml
[storage]
journal_mode = "wal"           # "wal" (default) or "delete"
synchronous = "full"           # "full" (default, durable) or "normal"
sqlite_busy_timeout_ms = 200   # per-statement SQLite busy handler
lock_deadline_ms = 10000       # total wait for the write lock per mutation
```

Category JSON files are written crash-safely: the new content is staged next
to the file (a name unique to the transaction), the SQLite rows are
committed, then the staged file is renamed into place. A process that dies in
between leaves a staged file whose fate is decided by its hash under the next
write lock (matching the committed hash → promoted; otherwise → discarded).
Readers never rename or delete anything: they serve whichever file carries
the committed hash. `memory_write merge=true`, the log/memory kind check and
the optional `expected_hash` compare-and-swap all run inside the same lock.

### Snapshot and restore

`memory.db` alone is not a backup (WAL, JSON files). Use the CLI:

```bash
mcp-memory-rs --snapshot /path/to/snap-2026-09-15 --with-backups  # db + json + backups + manifest
mcp-memory-rs --restore  /path/to/snap-2026-09-15 --stopped
mcp-memory-rs --verify-store                                      # read-only: db vs json, pending files
```

`--verify-store` never opens a `Store` (no PRAGMA, schema or reconcile): it is
safe on a store another binary still owns. A store written by a version older
than 0.3.2 can hold categories whose file is newer than the database row (a
write whose SQL failed): `--adopt-json --plan out.json` describes them
read-only and `--adopt-json --apply out.json` adopts the files, with
preconditions, under the lock. Restore's final swap is a few
renames — copy-safe, not atomic as a group: a `RESTORE_IN_PROGRESS` marker
keeps the store closed until an interrupted swap is recovered by hand.

A snapshot blocks writers for its duration (readers continue), runs
`VACUUM INTO` and copies the category files, so both halves describe the same
instant; it is verified before it is reported. Coverage is explicit in
`manifest.json`: without `--with-backups` only the current content and the
version *metadata* are covered — `memory_history` payloads (`read_version`)
live in `backups/` and need `--with-backups`. Restore verifies the snapshot
(integrity, one file per category with the recorded hash) **before** touching
anything, then replaces the live store and parks the previous one under
`<base_dir>/restore-parked-<timestamp>/`; it **requires every mcp-memory-rs
process on that store to be stopped first** — the `--stopped` flag is your
word for it.

## Building and testing

```bash
cargo build --release   # single static-friendly binary
cargo test              # unit + integration tests
cargo clippy --all-targets -- -D warnings
```

No build-time network access, no C dependencies beyond bundled SQLite.

## License

Apache-2.0. See [LICENSE](LICENSE).
