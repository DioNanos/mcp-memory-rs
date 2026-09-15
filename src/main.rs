use anyhow::Result;
use mcp_memory_rs::config::MemoryConfig;
use mcp_memory_rs::migrate;
use mcp_memory_rs::MemoryServer;
use rmcp::ServiceExt;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mcp_memory_rs=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let args: Vec<String> = std::env::args().collect();

    // Handle --version / -V short-circuit (before any MCP stdio attempt)
    if matches!(
        args.get(1).map(|s| s.as_str()),
        Some("--version") | Some("-V")
    ) {
        println!(
            "mcp-memory-rs {} (sqlite {})",
            env!("CARGO_PKG_VERSION"),
            mcp_memory_rs::store::Store::sqlite_version()
        );
        return Ok(());
    }

    // Handle --snapshot / --restore (store maintenance, no MCP)
    match args.get(1).map(|s| s.as_str()) {
        Some("--snapshot") => return run_snapshot(&args),
        Some("--restore") => return run_restore(&args),
        Some("--verify-store") => return run_verify_store(),
        Some("--adopt-json") => return run_adopt_json(&args),
        Some("--stress-append") => return run_stress_append(&args),
        Some("--stress-merge") => return run_stress_merge(&args),
        _ => {}
    }

    // Handle --migrate subcommand
    if args.get(1).map(|s| s.as_str()) == Some("--migrate") {
        return run_migrate(&args);
    }

    // Handle --http mode
    if args.get(1).map(|s| s.as_str()) == Some("--http") {
        let config = MemoryConfig::try_load()?;
        return mcp_memory_rs::http_server::start_http_server(config).await;
    }

    // Handle --help
    if args.get(1).map(|s| s.as_str()) == Some("--help")
        || args.get(1).map(|s| s.as_str()) == Some("-h")
    {
        eprintln!("mcp-memory-rs — Enterprise MCP Memory Server");
        eprintln!();
        eprintln!("Usage:");
        eprintln!("  mcp-memory-rs              Start MCP server (stdio)");
        eprintln!("  mcp-memory-rs --http       Start HTTP server");
        eprintln!(
            "  mcp-memory-rs --migrate <dir> [--dry-run]  Import from Node.js memory_context"
        );
        eprintln!("  mcp-memory-rs --snapshot <dir> [--with-backups]  Consistent snapshot (db + json [+ backups] + manifest)");
        eprintln!("  mcp-memory-rs --restore <dir> --stopped     Restore a snapshot (ALL store processes must be stopped)");
        eprintln!("  mcp-memory-rs --verify-store                Read-only consistency report (db vs json, pending files)");
        eprintln!("  mcp-memory-rs --adopt-json --plan <out.json> | --apply <plan.json>   Recovery of pre-0.3.2 divergences");
        eprintln!();
        eprintln!("Environment:");
        eprintln!("  MCP_MEMORY_DIR     Storage directory (standalone default: ~/.memory)");
        eprintln!("  MCP_DEVICE         Device identity for ACL");
        eprintln!("  MCP_MEMORY_CONFIG  Explicit config file path");
        eprintln!("  MCP_MEMORY_REQUIRE_CONFIG  1/true to fail if no config is found");
        eprintln!("  MCP_MEMORY_HOST    HTTP bind host (default: 127.0.0.1)");
        eprintln!("  MCP_MEMORY_PORT    HTTP bind port (default: 3100)");
        eprintln!("  MCP_MEMORY_TOKEN   Auth token for HTTP mode (required for --http)");
        eprintln!("  MCP_MEMORY_MODE    Server mode: 'offline' (stdio) or 'http'");
        return Ok(());
    }

    // Check env for HTTP mode
    let config = MemoryConfig::try_load()?;
    if config.server.mode == "http" {
        return mcp_memory_rs::http_server::start_http_server(config).await;
    }

    // Default: run MCP server (stdio) — reuse already-loaded config to avoid
    // double config load (and double legacy-store warning) in the stdio path.
    let server = MemoryServer::from_config(config)?;
    let service = server
        .serve(rmcp::transport::io::stdio())
        .await
        .map_err(|e| anyhow::anyhow!("MCP server error: {e}"))?;

    service.waiting().await?;
    Ok(())
}

fn run_migrate(args: &[String]) -> Result<()> {
    let source_dir = args.get(2).map(PathBuf::from).ok_or_else(|| {
        anyhow::anyhow!("Usage: mcp-memory-rs --migrate <source_dir> [--dry-run]")
    })?;

    let dry_run = args.contains(&"--dry-run".to_string());

    let config = MemoryConfig::try_load()?;

    if dry_run {
        println!("DRY RUN — no changes will be made");
    }
    println!("Source: {}", source_dir.display());
    println!("Target: {}", config.resolved_base_dir().display());
    println!();

    let result = migrate::migrate_from_nodejs(&source_dir, &config, dry_run)?;

    println!();
    println!("Migration summary:");
    println!("  Total files:  {}", result.total_files);
    println!("  Imported:     {}", result.imported);
    println!("  Skipped:      {}", result.skipped);
    if !result.errors.is_empty() {
        println!("  Errors:");
        for err in &result.errors {
            println!("    - {}", err);
        }
    }

    Ok(())
}

fn run_snapshot(args: &[String]) -> Result<()> {
    let dest = args
        .get(2)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("Usage: mcp-memory-rs --snapshot <dest_dir>"))?;
    let options = mcp_memory_rs::store::snapshot::Options {
        with_backups: args.iter().any(|a| a == "--with-backups"),
    };
    let config = MemoryConfig::try_load()?;
    let store = mcp_memory_rs::store::Store::new(&config)?;
    let manifest = store.snapshot(&dest, options)?;
    println!(
        "snapshot written to {} ({} categories, {} backup files{}, sqlite {}, created {})",
        dest.display(),
        manifest.categories,
        manifest.backup_files,
        if manifest.includes_backups {
            ""
        } else {
            " — backups NOT included, history payloads uncovered"
        },
        manifest.sqlite_version,
        manifest.created_at
    );
    Ok(())
}

fn run_restore(args: &[String]) -> Result<()> {
    let src = args.get(2).map(PathBuf::from).ok_or_else(|| {
        anyhow::anyhow!("Usage: mcp-memory-rs --restore <snapshot_dir> --stopped")
    })?;
    if !args.iter().any(|a| a == "--stopped") {
        anyhow::bail!(
            "refusing to restore: stop EVERY mcp-memory-rs process using this store first, \
             then re-run with --stopped (open SQLite connections would read garbage after the swap)"
        );
    }
    let config = MemoryConfig::try_load()?;
    let manifest = mcp_memory_rs::store::snapshot::restore(&config, &src)?;
    println!(
        "restored {} categories from {} (snapshot of {} taken {})",
        manifest.categories,
        src.display(),
        manifest.source_base_dir.display(),
        manifest.created_at
    );
    // Read-only check of the result (no Store open: the caller decides when
    // the new protocol first touches the restored files).
    let report = mcp_memory_rs::store::Store::verify_store(&config)?;
    println!(
        "restored store: {} categories, clean={}",
        report.categories,
        report.is_clean()
    );
    if !report.is_clean() {
        anyhow::bail!(
            "restored store is NOT clean: {}",
            serde_json::to_string(&report)?
        );
    }
    Ok(())
}

/// Test helper (used by the multi-process concurrency test): append `n`
/// entries `{tag, seq}` to `category`, one process, exit non-zero on the
/// first error. Not part of the supported CLI surface.
fn run_stress_append(args: &[String]) -> Result<()> {
    let usage = "Usage: mcp-memory-rs --stress-append <category> <n> <tag> [max_entries]";
    let category = args.get(2).ok_or_else(|| anyhow::anyhow!(usage))?;
    let n: u32 = args
        .get(3)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| anyhow::anyhow!(usage))?;
    let tag = args.get(4).ok_or_else(|| anyhow::anyhow!(usage))?;
    let max_entries: u64 = args.get(5).and_then(|v| v.parse().ok()).unwrap_or(1000);
    let config = MemoryConfig::try_load()?;
    let mut store = mcp_memory_rs::store::Store::new(&config)?;
    let retention = mcp_memory_rs::store::log::Retention {
        max_entries: Some(max_entries),
        max_age_days: None,
    };
    let actor = config.acl.device_name.clone();
    for seq in 0..n {
        store.append(
            category,
            serde_json::json!({ "tag": tag, "seq": seq }),
            Some(retention),
            &actor,
        )?;
    }
    println!("{tag}: {n} appends ok");
    Ok(())
}

/// Test helper: `n` top-level merges of `{<tag>_<i>: i}` into `category`
/// from one process. Used to prove merges from several processes never lose
/// each other's keys.
fn run_stress_merge(args: &[String]) -> Result<()> {
    let usage = "Usage: mcp-memory-rs --stress-merge <category> <n> <tag>";
    let category = args.get(2).ok_or_else(|| anyhow::anyhow!(usage))?;
    let n: u32 = args
        .get(3)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| anyhow::anyhow!(usage))?;
    let tag = args.get(4).ok_or_else(|| anyhow::anyhow!(usage))?;
    let config = MemoryConfig::try_load()?;
    let mut store = mcp_memory_rs::store::Store::new(&config)?;
    let actor = config.acl.device_name.clone();
    for i in 0..n {
        let mut patch = serde_json::Map::new();
        patch.insert(format!("{tag}_{i}"), serde_json::json!(i));
        store.write_merge(category, &patch, Some("stress-merge"), &actor, None)?;
    }
    println!("{tag}: {n} merges ok");
    Ok(())
}

fn run_verify_store() -> Result<()> {
    let config = MemoryConfig::try_load()?;
    // Read-only by construction: never opens a Store (no PRAGMA, no schema
    // init, no reconcile), so it can inspect a store still owned by an
    // older binary without changing a byte.
    let report = mcp_memory_rs::store::Store::verify_store(&config)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.is_clean() {
        anyhow::bail!("store is NOT consistent (see report)");
    }
    Ok(())
}

fn run_adopt_json(args: &[String]) -> Result<()> {
    use mcp_memory_rs::store::recovery;
    let usage = "Usage:\n  mcp-memory-rs --adopt-json --plan <out.json>    read-only: write the recovery plan\n  mcp-memory-rs --adopt-json --apply <plan.json>  adopt exactly what the plan describes";
    let mode = args.get(2).map(|s| s.as_str());
    let file = args.get(3).map(PathBuf::from);
    let config = MemoryConfig::try_load()?;
    match (mode, file) {
        (Some("--plan"), Some(out)) => {
            let plan = recovery::plan(&config)?;
            std::fs::write(&out, serde_json::to_string_pretty(&plan)?)?;
            println!(
                "plan written to {} ({} divergent categories, journal_mode={})",
                out.display(),
                plan.entries.len(),
                plan.journal_mode
            );
            for e in &plan.entries {
                println!(
                    "  {}: kind={} db={} file={} backup_of_db_version={} delta={}",
                    e.name,
                    e.kind,
                    e.db_hash.as_deref().map(|h| &h[..8]).unwrap_or("<none>"),
                    &e.file_hash[..8],
                    e.db_version_backup.as_deref().unwrap_or("none"),
                    e.delta
                        .as_ref()
                        .map(|d| serde_json::to_string(d).unwrap_or_default())
                        .unwrap_or_else(|| "n/a (db version bytes not available)".into())
                );
            }
            Ok(())
        }
        (Some("--apply"), Some(plan_path)) => {
            let plan: recovery::Plan = serde_json::from_str(&std::fs::read_to_string(&plan_path)?)?;
            if plan.base_dir != config.resolved_base_dir().display().to_string() {
                anyhow::bail!(
                    "plan is for base_dir {} but the config points to {}",
                    plan.base_dir,
                    config.resolved_base_dir().display()
                );
            }
            // Read-only preflight of the whole list before the first commit:
            // an entry must be either still exactly as planned, or already
            // adopted (file unchanged and now committed) — a rerun.
            let fresh = recovery::plan(&config)?;
            let clean_now = mcp_memory_rs::store::Store::verify_store(&config)?;
            for e in &plan.entries {
                if e.kind == "invalid" {
                    anyhow::bail!("preflight: '{}' is invalid JSON, not adoptable", e.name);
                }
                match fresh.entries.iter().find(|f| f.name == e.name) {
                    Some(f) if f.db_hash == e.db_hash && f.file_hash == e.file_hash => {}
                    Some(f) => anyhow::bail!(
                        "preflight: '{}' changed since the plan (db {:?}→{:?}, file {}→{})",
                        e.name,
                        e.db_hash,
                        f.db_hash,
                        &e.file_hash[..8],
                        &f.file_hash[..8]
                    ),
                    None => {
                        let on_disk = std::fs::read_to_string(
                            config.categories_dir().join(format!("{}.json", e.name)),
                        )?;
                        if mcp_memory_rs::store::Store::hash_of(&on_disk) != e.file_hash
                            || clean_now.mismatches.iter().any(|m| m == &e.name)
                        {
                            anyhow::bail!(
                                "preflight: '{}' is no longer divergent but its file is not the planned one",
                                e.name
                            );
                        }
                        // already adopted: adopt_plan reports AlreadyConsistent
                    }
                }
            }
            // First Store open with this binary: on a pre-0.3.2 store this is
            // the moment the journal switches to WAL. All entries are adopted
            // in ONE transaction: on any error nothing is committed.
            let mut store = mcp_memory_rs::store::Store::new(&config)?;
            let outcomes = store.adopt_plan(&plan.entries)?;
            drop(store);
            for (name, outcome) in &outcomes {
                println!("{name}: {}", serde_json::to_string(outcome)?);
            }
            let report = mcp_memory_rs::store::Store::verify_store(&config)?;
            println!(
                "after: clean={} mismatches={:?} journal_mode={}",
                report.is_clean(),
                report.mismatches,
                report.journal_mode
            );
            if !report.is_clean() {
                anyhow::bail!("adopt-json left the store NOT clean");
            }
            Ok(())
        }
        _ => anyhow::bail!(usage),
    }
}
