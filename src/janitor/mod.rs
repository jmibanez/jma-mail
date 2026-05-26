//! Janitorial tasks: maintenance work that operates on the
//! maildir tree or the state DB but isn't part of a sync cycle's
//! critical path. Each task is independently runnable so callers
//! (the engine's Phase 0, the `jma janitor` CLI, future scheduled
//! jobs) can compose only what they need.
//!
//! Today this module owns two tasks -- `dedupe` (per-folder local
//! Message-ID dedupe) and `remotededupe` (per-mailbox server-side
//! Message-ID dedupe via `Email/set`) -- with stubs reserved for
//! tasks already on the roadmap:
//!
//! - `prune`: drift remediation per issue #8 (orphan local
//!   maildirs, stale `mailbox_map` rows, server-deleted folders
//!   still living on disk). Belongs here because it's a maildir
//!   tree operation the daemon doesn't need to perform on every
//!   cycle.
//! - `db_gc`: state DB compaction once we identify concrete
//!   bloat sources (e.g. orphan `local_state` rows, `jmap_state`
//!   rows for retired entity types). Owner of any future
//!   `VACUUM` invocation against `state.db`.
//!
//! New tasks should follow the same shape: a single `run`
//! entry point that takes a small, owned input set and returns a
//! plan-or-result type the caller can render for dry-run preview
//! or feed to follow-on logic.

pub mod dedupe;
pub mod rebindfolders;
pub mod remotededupe;
