use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::Config;
use crate::state::queries;
use crate::sync::engine::SyncEngine;
use crate::sync::plan::SyncDirection;

/// What triggered a sync cycle.
#[derive(Debug, Clone)]
pub enum SyncTrigger {
    RemoteChange,
    LocalChange,
    Initial,
}

/// Run the daemon loop: initial sync, then react to triggers.
pub async fn run(conn: &Connection, config: &Config) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<SyncTrigger>(32);

    let hook = super::hook::Hook::new(config.watch.post_arrival_command.clone());
    if hook.is_enabled() {
        info!("Post-arrival hook configured");
    }

    // One engine for the lifetime of the watcher: opens its session in
    // `connect`, drives the initial sync, and is reused on every
    // trigger below. SSE setup pulls the event-source URL and account
    // id straight off the engine -- no separate `Client` floating
    // through the runner.
    let engine = SyncEngine::connect(conn, config).await?;

    // Run initial sync
    info!("Running initial sync before entering watch mode");
    match engine.run(false, SyncDirection::Both).await {
        Ok(outcome) => {
            if outcome.downloaded > 0 {
                hook.trigger().await;
            }
        }
        Err(e) => error!("Initial sync failed: {}", e),
    }

    // Get session info for EventSource URL
    let session_info = engine.session_info()?;
    let token = config.account.token()?;
    let maildir_root = config.maildir_path();
    let account_id = session_info.account_id.clone();
    let ping_interval = config.watch.ping_interval;

    // Seed the SSE dedup cache from current DB state so the first event
    // after the initial sync isn't a guaranteed redundant trigger.
    let mut initial_states: HashMap<String, String> = HashMap::new();
    for entity_type in ["Email", "Mailbox"] {
        if let Some(state) = queries::get_jmap_state(conn, account_id.as_ref(), entity_type)? {
            initial_states.insert(entity_type.to_string(), state);
        }
    }

    // Spawn SSE listener
    let sse_tx = tx.clone();
    let es_url = session_info.event_source_url.clone();
    let es_token = token.clone();
    let es_account = account_id.clone();
    let sse_handle = tokio::spawn(async move {
        if let Err(e) = super::eventsource::listen(
            &es_url,
            &es_token,
            &es_account,
            ping_interval,
            initial_states,
            sse_tx,
        )
        .await
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
        match engine.run(false, SyncDirection::Both).await {
            Ok(outcome) => {
                if outcome.downloaded > 0 {
                    hook.trigger().await;
                }
            }
            Err(e) => error!("Sync failed: {}", e),
        }
    }

    info!("Watch mode shutting down");

    // Clean up
    sse_handle.abort();
    fs_handle.abort();

    Ok(())
}
