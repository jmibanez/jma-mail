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
//! `mark_active(true)` is only flipped once the alternate screen is
//! up and rendering starts -- before that, fmt_layer is still writing
//! to stderr and `notify!` is still writing to stdout. The
//! mark_active flag is what those two output channels consult to
//! suppress themselves, so flipping it any earlier would silence
//! pre-render stderr/stdout for no reason.

pub mod layer;
pub mod render;
pub mod state;
pub mod writer;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

pub use layer::TuiLayer;
pub use state::TuiState;

/// Set to true once the TUI's alternate screen is up. Read by the
/// fmt_layer writer gate (`writer::GatedStderr`) and the `notify!`
/// macro so neither corrupts the rendered display.
static ACTIVE: AtomicBool = AtomicBool::new(false);

/// Process-wide handle on the shared TUI state. Held both by the
/// `TuiLayer` (writes into it from tracing events) and by the render
/// loop (reads from it on every frame). Installed exactly once via
/// `install`, queried by callers that conditionally enable TUI flow
/// in `cmd_watch`.
static STATE: OnceLock<Arc<TuiState>> = OnceLock::new();

/// Build the TuiLayer and stash a handle to its shared state. Call
/// once at tracing init time if the runtime decided we want a TUI;
/// idempotent on the OnceLock side (subsequent calls would reuse the
/// installed state) but the layer itself can only be installed once
/// on the subscriber, so callers should not call this twice in one
/// process.
pub fn install() -> TuiLayer {
    let state = Arc::new(TuiState::new());
    let _ = STATE.set(state.clone());
    TuiLayer::new(state)
}

/// Shared state handle if the TUI was installed this process,
/// otherwise None. Lets `cmd_watch` decide whether to spawn the
/// render task without re-checking CLI flags.
pub fn state() -> Option<Arc<TuiState>> {
    STATE.get().cloned()
}

/// True when the alternate screen is up and rendering. Consulted by
/// fmt_layer's writer and the `notify!` macro to gate themselves off.
pub fn is_active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

/// Flip the active flag. The render loop sets it to true right after
/// entering the alternate screen and back to false right before
/// leaving, so the un-suppressed stderr/stdout window matches the
/// terminal-is-clean window exactly.
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
