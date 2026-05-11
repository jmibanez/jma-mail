//! Cargo-style status text for the normal verbosity level.
//!
//! At default verbosity the tracing filter is clamped to `warn`, so
//! the user sees no info-level log output. Milestone status -- "Sync
//! complete", "Watch mode active", "Downloading N messages" -- is
//! emitted via `notify!`, which is independent of the tracing log
//! levels and writes plain text to stdout (no timestamp, no level
//! prefix) so it reads like cargo's own progress lines. Tracing logs
//! go to stderr instead, so the two streams stay separable.
//!
//! `--quiet` suppresses `notify!` output too: `-q` is the "errors
//! only" floor for every output channel, not just tracing.

use std::sync::OnceLock;

static QUIET: OnceLock<bool> = OnceLock::new();

/// Wire up the global `--quiet` flag. Call once from `main`, after
/// parsing the CLI but before any code that might call `notify!`.
pub fn init_quiet(quiet: bool) {
    let _ = QUIET.set(quiet);
}

/// Whether `--quiet` was requested. Consulted by `notify!`; callers
/// shouldn't usually need to read it directly.
#[doc(hidden)]
pub fn is_quiet() -> bool {
    QUIET.get().copied().unwrap_or(false)
}
