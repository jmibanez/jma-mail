//! Janitorial tasks: maintenance work that operates on the
//! maildir tree or the state DB but isn't part of a sync cycle's
//! critical path. Each task is independently runnable so callers
//! (the engine's Phase 0, the `jma janitor` CLI, future scheduled
//! jobs) can compose only what they need.
//!
//! Today this module owns `dedupe` (per-folder local Message-ID
//! dedupe), `remotededupe` (per-mailbox server-side Message-ID dedupe
//! via `Email/set`), `rebindfolders` (rebind sentinel-less maildirs
//! by Message-ID probing), and `prune` (maildir/DB drift remediation
//! per issue #8 -- orphan local maildirs, stale `mailbox_map` rows,
//! folders dropped from `[sync].mailboxes`), with a stub reserved for
//! a task already on the roadmap:
//!
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
pub mod prune;
pub mod rebindfolders;
pub mod remotededupe;
