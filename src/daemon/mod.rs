pub mod eventsource;
pub mod hook;
pub mod runner;
pub mod watcher;

use std::time::Duration;

/// Reconnect pacing shared across the daemon: the SSE listener uses
/// these between its internal connect attempts, and the runner uses
/// them between session-rebuild attempts. Defining once here so the
/// two layers can't drift.
pub(super) const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
pub(super) const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(60);
