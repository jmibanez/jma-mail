//! Interactive TUI for `jma watch`.
//!
//! Layout: bottom half is a streaming log pane (driven by a tracing
//! Layer that captures `info!+` events into a ring buffer); top half
//! is the metrics row -- network bandwidth with live download
//! progress on the left, recently synced messages on the right. A
//! single-row status bar at the very bottom carries `notify!`
//! milestones and the active sync phase.
//!
//! Activation rules: `jma watch` only, stdout must be a TTY, and the
//! user must not have passed `--no-tui`. One-shot commands (`sync`,
//! `pull`, `push`, etc.) never get a TUI -- a screen that flashes up
//! and tears back down on a short run is worse than the plain
//! stderr/stdout it would replace.
//!
//! The layer is installed at `tracing` init time so events flow into
//! the shared `TuiState` from the moment the registry comes online.
//! `mark_active(true)` is set as soon as the watch command finds it
//! is running with a TUI, before the alternate screen is up and
//! before any startup work logs. Config load, lock acquisition, and
//! DB open all emit INFO lines, and the daemon (which starts
//! concurrently with the render thread) adds its own connect and
//! initial-sync logs -- all in the window before the first frame.
//! Left un-gated those lines dribble onto the real terminal and
//! survive as orphaned scrollback above the restored screen. The
//! layer captures the same events into the ring buffer, so the log
//! pane replays them once rendering starts; gating stderr/stdout off
//! from that point keeps the terminal clean without dropping anything.

pub mod layer;
pub mod render;
pub mod state;
pub mod writer;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

pub use layer::TuiLayer;
pub use state::TuiState;

/// True while the TUI owns the terminal. While it is set, tracing's
/// stderr output and `notify!`'s stdout are withheld from the raw
/// terminal and captured by the log ring buffer / status line
/// instead, so neither dribbles onto the screen before the first
/// frame nor corrupts the rendered display after it. Set before the
/// first frame is drawn, cleared on teardown.
static ACTIVE: AtomicBool = AtomicBool::new(false);

/// Process-wide handle on the shared TUI state. Held both by the
/// `TuiLayer` (writes into it from tracing events) and by the render
/// loop (reads from it on every frame). Installed exactly once via
/// `install`, queried by callers that conditionally enable TUI flow
/// in `cmd_watch`.
static STATE: OnceLock<Arc<TuiState>> = OnceLock::new();

/// Build the TuiLayer and stash a handle to its shared state.
/// `log_level` is the log pane's admission floor -- the user's
/// verbosity dial, shared with the stderr filter. Call once at
/// tracing init time if the runtime decided we want a TUI;
/// idempotent on the OnceLock side (subsequent calls would reuse the
/// installed state) but the layer itself can only be installed once
/// on the subscriber, so callers should not call this twice in one
/// process.
pub fn install(log_level: tracing::Level) -> TuiLayer {
    let state = Arc::new(TuiState::new());
    let _ = STATE.set(state.clone());
    TuiLayer::new(state, log_level)
}

/// Shared state handle if the TUI was installed this process,
/// otherwise None. Lets `cmd_watch` decide whether to spawn the
/// render task without re-checking CLI flags.
pub fn state() -> Option<Arc<TuiState>> {
    STATE.get().cloned()
}

/// Whether the TUI currently owns the terminal -- i.e. whether raw
/// stderr/stdout output is being withheld to protect the display.
pub fn is_active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

/// Set whether the TUI owns the terminal. Pass `true` once a TUI run
/// is committed, before anything draws or logs; pass `false` on
/// teardown. While `true`, raw stderr/stdout output is withheld to
/// protect the display, so it must be cleared before any final
/// message -- an error on the way out -- is expected to reach the user.
pub fn mark_active(v: bool) {
    ACTIVE.store(v, Ordering::Relaxed);
}

/// Route a formatted `notify!` message into the TUI status line.
/// Called by the `notify!` macro when the TUI is active; no-op
/// otherwise (no shared state to push into). Exposed at the module
/// root so the macro can stay agnostic of `TuiState` internals.
pub fn push_status(message: String) {
    if let Some(state) = STATE.get() {
        state.set_status(message);
    }
}
