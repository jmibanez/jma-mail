pub mod classify;
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

/// Slack added on top of the negotiated SSE `ping` interval before
/// the listener's watchdog gives up on a stream and reconnects.
/// Covers clock skew between us and the JMAP server plus normal
/// network jitter -- the actual ping arrives close to the negotiated
/// interval but the server's interval starts ticking at its own
/// clock, not ours.
pub(super) const PING_WATCHDOG_SLACK: Duration = Duration::from_secs(5);
