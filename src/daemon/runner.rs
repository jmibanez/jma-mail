use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::{RECONNECT_INITIAL_BACKOFF, RECONNECT_MAX_BACKOFF};
use crate::config::Config;
use crate::jmap::retry::is_transient_error;
use crate::state::queries;
use crate::sync::engine::{ScanScope, SyncEngine};
use crate::sync::plan::SyncDirection;
use crate::sync::self_writes::SelfWriteCache;

/// What triggered a sync cycle. `LocalChange` carries the FS event
/// paths that drove the watcher so the runner can surface them when
/// the cycle ends up doing nothing -- diagnostic for spurious
/// triggers, where the path list is the only clue to what wrote.
#[derive(Debug, Clone)]
pub enum SyncTrigger {
    RemoteChange,
    LocalChange(Vec<PathBuf>),
    Initial,
}

impl fmt::Display for SyncTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RemoteChange => f.write_str("RemoteChange"),
            Self::LocalChange(paths) => write!(f, "LocalChange ({} FS event(s))", paths.len()),
            Self::Initial => f.write_str("Initial"),
        }
    }
}

/// Combine a batch of triggers (the first plus everything absorbed
/// during the coalescing window) into a single representative for
/// one sync cycle. The split signature pins the non-empty
/// precondition at the type level: there is always at least the
/// initial trigger, which the trigger loop just received.
///
/// Dominance order, broadest first: `Initial` wins if present (it's
/// a full bootstrap, not a per-trigger delta), then `RemoteChange`
/// (we're looking at server-side state too), otherwise
/// `LocalChange` carrying every path from every absorbed
/// `LocalChange` trigger. The order matches "broader scan wins" --
/// once we've decided we need a non-LocalChange shape, the
/// LocalChange path set is irrelevant.
///
/// The path-drop on RemoteChange/Initial assumes those triggers
/// always imply a full local scan. If a future cycle shape ever
/// pairs RemoteChange with path-narrowed local work, this merge
/// becomes lossy and needs revisiting.
fn coalesce_triggers(first: SyncTrigger, rest: Vec<SyncTrigger>) -> SyncTrigger {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut has_initial = false;
    let mut has_remote = false;
    for t in std::iter::once(first).chain(rest) {
        match t {
            SyncTrigger::Initial => has_initial = true,
            SyncTrigger::RemoteChange => has_remote = true,
            SyncTrigger::LocalChange(p) => paths.extend(p),
        }
    }
    if has_initial {
        SyncTrigger::Initial
    } else if has_remote {
        SyncTrigger::RemoteChange
    } else {
        SyncTrigger::LocalChange(paths)
    }
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

    // Shared between the executor (records every disk write it
    // performs) and the watcher (drops fsevents batches whose every
    // path matches a recent self-write). Catches the new/->cur/
    // MUA-promotion-with-same-flags echo that the watcher's
    // all-live-new filter alone can't suppress -- the cur/ path on
    // that promotion forces the batch through, and the cache
    // recognises it as the predicted promotion target.
    //
    // TTL is derived from the trigger pipeline knobs (default 5s
    // when debounce_secs=2 and coalesce_window_ms=500); see
    // `WatchConfig::effective_self_write_ttl` for the formula and
    // the override path.
    let self_writes = Arc::new(SelfWriteCache::new(config.watch.effective_self_write_ttl()));

    // FS watcher is independent of JMAP and survives reconnects --
    // spawn it once for the daemon's lifetime.
    let fs_tx = tx.clone();
    let fs_root = config.maildir_path();
    let debounce = config.watch.debounce_secs;
    let fs_self_writes = self_writes.clone();
    let fs_handle = tokio::spawn(async move {
        if let Err(e) = super::watcher::watch(&fs_root, debounce, fs_tx, Some(fs_self_writes)).await
        {
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
    let mut engine = connect_with_backoff(conn, config, self_writes.clone()).await?;
    crate::notify!("Running initial sync before entering watch mode");
    match engine
        .run(false, SyncDirection::Both, ScanScope::Full)
        .await
    {
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

    crate::notify!("Watch mode active. Press Ctrl+C to stop.");

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
                engine = connect_with_backoff(conn, config, self_writes.clone()).await?;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
            }
            Err(e) => {
                error!("Session aborted: {:#}", e);
                break;
            }
        }
    }

    crate::notify!("Watch mode shutting down");
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
    // Coalescing window for the trigger loop: hold open after the
    // first trigger to absorb back-to-back debouncer batches into one
    // cycle. See `WatchConfig::coalesce_window_ms` for the rationale
    // (cross-folder move halves must land in the same scan_paths
    // call, otherwise reconcile can't pair source-delete with
    // dest-create and the move degrades into destroy + reupload).
    let coalesce_window = Duration::from_millis(config.watch.coalesce_window_ms);

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
    //
    // Each iteration receives one trigger, then holds open the
    // coalescing window for additional triggers before running the
    // cycle. The window resets every time another trigger lands, so
    // bursts collapse naturally and the cycle only starts once the
    // pipe goes quiet.
    let exit = loop {
        let Some(first) = rx.recv().await else {
            break SessionExit::ChannelClosed;
        };
        // Drain the pipe until the coalescing window elapses without
        // a new trigger. Channel-closed mid-drain (Ok(None)) just
        // exits the inner loop with whatever we collected; the outer
        // `rx.recv` on the next iteration will return None and break
        // with ChannelClosed. Worth at most one extra cycle on the
        // way out.
        let mut rest: Vec<SyncTrigger> = Vec::new();
        loop {
            match tokio::time::timeout(coalesce_window, rx.recv()).await {
                Ok(Some(next)) => rest.push(next),
                Ok(None) => break,
                Err(_) => break,
            }
        }
        let coalesced_count = 1 + rest.len();
        let trigger = coalesce_triggers(first, rest);
        if coalesced_count > 1 {
            info!(
                "Sync triggered by {} ({} triggers coalesced)",
                trigger, coalesced_count
            );
        } else {
            info!("Sync triggered by {}", trigger);
        }
        // LocalChange triggers carry the FS event paths from the
        // coalesced batch; route them through ScanScope::Paths so the
        // engine classifies only the (folder, maildir_id) groups those
        // paths touched. RemoteChange and Initial fall back to a full
        // per-folder walk -- RemoteChange has no local hint, Initial
        // happens once at startup, and a coalesced batch promoted to
        // either of those by the dominance order also drops to the
        // safe O(N) shape (see coalesce_triggers).
        let scope = match &trigger {
            SyncTrigger::LocalChange(paths) => ScanScope::Paths(paths.clone()),
            SyncTrigger::RemoteChange | SyncTrigger::Initial => ScanScope::Full,
        };
        match engine.run(false, SyncDirection::Both, scope).await {
            Ok(outcome) => {
                // A successful cycle means the link is healthy --
                // reset the outer backoff so the next disconnect
                // starts fresh rather than at whatever cap we hit.
                *backoff = RECONNECT_INITIAL_BACKOFF;
                if outcome.downloaded > 0 {
                    hook.trigger().await;
                }
                if outcome.already_in_sync
                    && let SyncTrigger::LocalChange(paths) = &trigger
                {
                    let formatted: Vec<String> =
                        paths.iter().map(|p| p.display().to_string()).collect();
                    debug!(
                        "LocalChange trigger produced no work; FS event path(s): {}",
                        formatted.join(", ")
                    );
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
///
/// Threads the self-write cache through every successful connect
/// (initial bootstrap and post-transport-error reconnects alike) so
/// the reconnect path can't silently regress self-echo suppression
/// by forgetting to call `engine.set_self_writes`.
async fn connect_with_backoff<'a>(
    conn: &'a Connection,
    config: &'a Config,
    self_writes: Arc<SelfWriteCache>,
) -> Result<SyncEngine<'a>> {
    let mut backoff = RECONNECT_INITIAL_BACKOFF;
    loop {
        match SyncEngine::connect(conn, config).await {
            Ok(mut engine) => {
                engine.set_self_writes(self_writes);
                return Ok(engine);
            }
            Err(e) if is_transient_error(&e) => {
                warn!("Connect failed ({:#}); retrying in {:?}", e, backoff);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A single trigger passes through unchanged; coalesce_triggers
    /// is identity on a one-element batch. Pins the no-op case so a
    /// future tweak to the dominance logic doesn't accidentally
    /// rewrite single-trigger semantics.
    #[test]
    fn coalesce_single_local_change_is_identity() {
        let p = PathBuf::from("/Mail/INBOX/cur/file:2,S");
        let merged = coalesce_triggers(SyncTrigger::LocalChange(vec![p.clone()]), Vec::new());
        match merged {
            SyncTrigger::LocalChange(paths) => assert_eq!(paths, vec![p]),
            other => panic!("expected LocalChange, got {:?}", other),
        }
    }

    /// Multiple LocalChange triggers concatenate their path lists.
    /// This is the path-driven scan's correctness lever: a
    /// cross-folder move whose source-delete and dest-create renames
    /// land in separate debouncer batches must end up in the same
    /// path set so reconcile's move pre-pass can pair them.
    #[test]
    fn coalesce_merges_local_change_paths() {
        let a = PathBuf::from("/Mail/INBOX/cur/a:2,");
        let b = PathBuf::from("/Mail/Spam/cur/a:2,");
        let merged = coalesce_triggers(
            SyncTrigger::LocalChange(vec![a.clone()]),
            vec![SyncTrigger::LocalChange(vec![b.clone()])],
        );
        match merged {
            SyncTrigger::LocalChange(paths) => assert_eq!(paths, vec![a, b]),
            other => panic!("expected merged LocalChange, got {:?}", other),
        }
    }

    /// A RemoteChange in the batch dominates LocalChange: the merged
    /// trigger surfaces as RemoteChange so the cycle does a full
    /// scan (we have no way to narrow on a server-driven change). The
    /// LocalChange paths are intentionally dropped -- once a full
    /// scan is happening, the scan reads live FS state across every
    /// folder and the path list adds no information.
    #[test]
    fn coalesce_remote_change_dominates_local() {
        let p = PathBuf::from("/Mail/INBOX/cur/file:2,S");
        let merged = coalesce_triggers(
            SyncTrigger::LocalChange(vec![p]),
            vec![SyncTrigger::RemoteChange],
        );
        assert!(matches!(merged, SyncTrigger::RemoteChange));
    }

    /// Initial dominates everything else: it's the bootstrap shape
    /// and implies a full first-pass walk regardless of what else
    /// arrived. Defensive coverage -- Initial is sent once at startup
    /// outside the trigger loop and shouldn't appear in coalesced
    /// batches in practice, but the dominance order needs to hold if
    /// it ever does.
    #[test]
    fn coalesce_initial_dominates_remote_and_local() {
        let p = PathBuf::from("/Mail/INBOX/cur/file:2,S");
        let merged = coalesce_triggers(
            SyncTrigger::LocalChange(vec![p]),
            vec![SyncTrigger::RemoteChange, SyncTrigger::Initial],
        );
        assert!(matches!(merged, SyncTrigger::Initial));
    }
}
