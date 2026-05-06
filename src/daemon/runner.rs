use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use super::{RECONNECT_INITIAL_BACKOFF, RECONNECT_MAX_BACKOFF};
use crate::config::Config;
use crate::jmap::retry::is_transient_error;
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

/// Why the inner `session` returned. The outer `run` uses this to
/// decide whether to break the watch loop or rebuild the engine and
/// start another session.
enum SessionExit {
    /// The trigger channel closed (all senders dropped). Break out.
    ChannelClosed,
    /// `engine.run` failed with a transient error and the retry layer
    /// gave up. Reconnect with backoff and start a new session.
    TransportError(anyhow::Error),
}

/// Run the daemon: spawn the FS watcher, do the one-time initial sync,
/// and enter a watch loop that rebuilds the JMAP session on transport
/// errors.
pub async fn run(conn: &Connection, config: &Config) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<SyncTrigger>(32);

    let hook = super::hook::Hook::new(config.watch.post_arrival_command.clone());
    if hook.is_enabled() {
        info!("Post-arrival hook configured");
    }

    // FS watcher is independent of JMAP and survives reconnects --
    // spawn it once for the daemon's lifetime.
    let fs_tx = tx.clone();
    let fs_root = config.maildir_path();
    let debounce = config.watch.debounce_secs;
    let fs_handle = tokio::spawn(async move {
        if let Err(e) = super::watcher::watch(&fs_root, debounce, fs_tx).await {
            error!("Filesystem watcher error: {}", e);
        }
    });

    // Initial bootstrap: connect once (retrying transient failures
    // with backoff; hard failures like a bad token propagate so the
    // daemon dies loudly rather than spinning), then run the initial
    // sync once. The initial sync is the daemon's "catch up since
    // last shutdown" cycle; we don't repeat it on reconnect because
    // the DB cursor is intact and the next regular trigger will
    // reconcile any events missed during the disconnect window via
    // Email/changes.
    let mut engine = connect_with_backoff(conn, config).await?;
    info!("Running initial sync before entering watch mode");
    match engine.run(false, SyncDirection::Both).await {
        Ok(outcome) => {
            if outcome.downloaded > 0 {
                hook.trigger().await;
            }
        }
        Err(e) if is_transient_error(&e) => {
            warn!(
                "Initial sync hit transient error ({:#}); watch mode will reconnect on the first trigger",
                e
            );
        }
        Err(e) => error!("Initial sync failed: {:#}", e),
    }

    info!("Watch mode active. Press Ctrl+C to stop.");

    // Watch loop. Each iteration runs a "session" (post-initial-sync
    // setup + drain triggers) until either the channel closes or
    // `engine.run` reports a transient error past the retry layer; on
    // the latter we sleep with backoff and rebuild the engine. Backoff
    // doubles per consecutive transport error and is reset only by
    // `session` on the first successful sync cycle -- a successful
    // reconnect alone isn't enough evidence the link is healthy
    // (server may accept the session but reject Email/changes), so we
    // hold backoff elevated until real work completes.
    let mut backoff = RECONNECT_INITIAL_BACKOFF;
    loop {
        match session(&engine, conn, config, &hook, &tx, &mut rx, &mut backoff).await {
            Ok(SessionExit::ChannelClosed) => break,
            Ok(SessionExit::TransportError(e)) => {
                warn!(
                    "Reconnecting after transport error ({:#}); backoff {:?}",
                    e, backoff
                );
                tokio::time::sleep(backoff).await;
                engine = connect_with_backoff(conn, config).await?;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
            }
            Err(e) => {
                error!("Session aborted: {:#}", e);
                break;
            }
        }
    }

    info!("Watch mode shutting down");
    fs_handle.abort();
    Ok(())
}

/// One pass over the trigger channel. Performs the post-initial-sync
/// setup (session metadata, token, dedup-cache seed, SSE listener
/// spawn), then drives `engine.run` per trigger until the channel
/// closes or a transient error escapes the retry layer.
async fn session<'a>(
    engine: &SyncEngine<'a>,
    conn: &Connection,
    config: &Config,
    hook: &super::hook::Hook,
    tx: &mpsc::Sender<SyncTrigger>,
    rx: &mut mpsc::Receiver<SyncTrigger>,
    backoff: &mut Duration,
) -> Result<SessionExit> {
    // Post-initial-sync steps -- redone on every reconnect so a
    // freshly-built engine picks up rotated tokens, fresh URLs, and a
    // current view of the DB cursor before the SSE listener starts.
    // Setup-time failures (keychain hiccup, session metadata missing)
    // route through TransportError so the outer reconnect path
    // handles them with backoff instead of taking the daemon down.
    let session_info = match engine.session_info() {
        Ok(s) => s,
        Err(e) => return Ok(SessionExit::TransportError(e)),
    };
    let token = match config.account.token() {
        Ok(t) => t,
        Err(e) => return Ok(SessionExit::TransportError(e)),
    };
    let account_id = session_info.account_id.clone();
    let ping_interval = config.watch.ping_interval;

    // Seed the SSE dedup cache from current DB state so the first
    // event after (re)connect isn't a guaranteed redundant trigger.
    let mut initial_states: HashMap<String, String> = HashMap::new();
    for entity_type in ["Email", "Mailbox"] {
        if let Some(state) = queries::get_jmap_state(conn, account_id.as_ref(), entity_type)? {
            initial_states.insert(entity_type.to_string(), state);
        }
    }

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

    // Trigger loop. Break out with the appropriate SessionExit so the
    // outer `run` can decide between shutdown and reconnect; abort the
    // SSE listener on the way out either way.
    let exit = loop {
        let Some(trigger) = rx.recv().await else {
            break SessionExit::ChannelClosed;
        };
        info!("Sync triggered by {:?}", trigger);
        match engine.run(false, SyncDirection::Both).await {
            Ok(outcome) => {
                // A successful cycle means the link is healthy --
                // reset the outer backoff so the next disconnect
                // starts fresh rather than at whatever cap we hit.
                *backoff = RECONNECT_INITIAL_BACKOFF;
                if outcome.downloaded > 0 {
                    hook.trigger().await;
                }
            }
            Err(e) if is_transient_error(&e) => {
                break SessionExit::TransportError(e);
            }
            Err(e) => error!("Sync failed: {:#}", e),
        }
    };

    sse_handle.abort();
    Ok(exit)
}

/// Open a JMAP session, retrying transient failures forever with
/// exponential backoff. Hard errors (e.g. 401 Unauthorized from a bad
/// token, 4xx from a malformed config) propagate immediately so the
/// daemon dies loudly rather than burning CPU in a retry loop the user
/// can't escape without intervention.
async fn connect_with_backoff<'a>(
    conn: &'a Connection,
    config: &'a Config,
) -> Result<SyncEngine<'a>> {
    let mut backoff = RECONNECT_INITIAL_BACKOFF;
    loop {
        match SyncEngine::connect(conn, config).await {
            Ok(engine) => return Ok(engine),
            Err(e) if is_transient_error(&e) => {
                warn!("Connect failed ({:#}); retrying in {:?}", e, backoff);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
            }
            Err(e) => return Err(e),
        }
    }
}
