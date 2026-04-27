use anyhow::Result;
use jmap_client::client::Client;
use rusqlite::Connection;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::Config;
use crate::jmap::session;
use crate::sync::engine;

/// What triggered a sync cycle.
#[derive(Debug, Clone)]
pub enum SyncTrigger {
    RemoteChange,
    LocalChange,
    Initial,
}

/// Run the daemon loop: initial sync, then react to triggers.
pub async fn run(client: &Client, conn: &Connection, config: &Config) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<SyncTrigger>(32);

    // Run initial sync
    info!("Running initial sync before entering watch mode");
    if let Err(e) = engine::sync(client, conn, config, false).await {
        error!("Initial sync failed: {}", e);
    }

    // Get session info for EventSource URL
    let session_info = session::session_info(client)?;
    let token = config.account.token()?;
    let maildir_root = config.maildir_path();

    // Spawn SSE listener
    let sse_tx = tx.clone();
    let es_url = session_info.event_source_url.clone();
    let es_token = token.clone();
    let sse_handle = tokio::spawn(async move {
        if let Err(e) =
            super::eventsource::listen(&es_url, &es_token, sse_tx).await
        {
            error!("SSE listener error: {}", e);
        }
    });

    // Spawn filesystem watcher
    let fs_tx = tx.clone();
    let fs_root = maildir_root.clone();
    let debounce = config.watch.debounce_secs;
    let fs_handle = tokio::spawn(async move {
        if let Err(e) = super::watcher::watch(&fs_root, debounce, fs_tx).await {
            error!("Filesystem watcher error: {}", e);
        }
    });

    // Drop our own sender so the channel closes when both spawned tasks end
    drop(tx);

    info!("Watch mode active. Press Ctrl+C to stop.");

    // Main event loop
    while let Some(trigger) = rx.recv().await {
        info!("Sync triggered by {:?}", trigger);
        if let Err(e) = engine::sync(client, conn, config, false).await {
            error!("Sync failed: {}", e);
        }
    }

    info!("Watch mode shutting down");

    // Clean up
    sse_handle.abort();
    fs_handle.abort();

    Ok(())
}
