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
        println!("mcp-memory-rs {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
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
        eprintln!();
        eprintln!("Environment:");
        eprintln!("  MCP_MEMORY_DIR     Storage directory (default: ~/.memory)");
        eprintln!("  MCP_DEVICE         Device identity for ACL");
        eprintln!("  MCP_MEMORY_CONFIG  Config file path");
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
