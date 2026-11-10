// SPDX-License-Identifier: Apache-2.0
use clap::Parser;
use rmcp::{ServiceExt, transport::async_rw::AsyncRwTransport};
use std::{path::PathBuf, sync::Arc};
use undercurrent_config::{ConfigDiscovery, UndercurrentPaths, load_config};
use undercurrent_mcp::{Connection, MAX_MESSAGE_BYTES, Server};

#[derive(Parser)]
#[command(
    name = "uc-mcp",
    version,
    about = "Local stdio MCP server for an existing Undercurrent daemon"
)]
struct Args {
    /// Absolute runtime home; otherwise use UNDERCURRENT_HOME or the native default.
    #[arg(long)]
    home: Option<PathBuf>,
    /// Expose only observation tools and reject mutation calls.
    #[arg(long)]
    read_only: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut discovery = ConfigDiscovery::from_process_env(None);
    if let Some(home) = args.home {
        discovery.path_env.undercurrent_home = Some(home);
    }
    let paths = UndercurrentPaths::resolve(&discovery.path_env)?;
    let config = load_config(&discovery)?;
    let connection = Arc::new(Connection::new(
        paths.home().to_owned(),
        config.settings.requests.request_bytes,
    ));
    let server = Server::new(connection.clone(), args.read_only);
    let transport = AsyncRwTransport::new(tokio::io::stdin(), tokio::io::stdout())
        .with_max_line_length(MAX_MESSAGE_BYTES);
    let running = server.serve(transport).await?;
    tokio::select! {
        result=running.waiting() => { result?; }
        result=tokio::signal::ctrl_c() => { result?; }
    }
    tokio::task::spawn_blocking(move || connection.close()).await?;
    Ok(())
}
