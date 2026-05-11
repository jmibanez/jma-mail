use anyhow::Result;
use notify_debouncer_mini::{DebouncedEventKind, new_debouncer};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::runner::SyncTrigger;
use crate::sync::self_writes::SelfWriteCache;

/// Whether a notify event path should drive a sync trigger. We only
/// care about events on actual maildir message files, which by
/// convention live in `<folder>/cur/` (delivered+seen) or
/// `<folder>/new/` (delivered, awaiting first read). Everything else
/// inside the watched root is noise: `<folder>/tmp/` is mid-delivery
/// scratch, and the maildir root itself can hold sidecar files
/// jma writes (`.jma.db` and its `-wal` / `-shm` siblings
/// in WAL mode, `.jma.lock`, etc.) that shouldn't kick a sync.
/// Read-only commands like `mailboxes` and `status` open the state DB
/// to consult the discovery cache, which by itself touches the WAL
/// and SHM siblings -- without this filter, running them alongside
/// `watch` would spuriously fire `LocalChange` triggers.
///
/// The filename half of the check is an allowlist on shape rather
/// than a denylist of known sidecar prefixes: the maildir spec
/// requires the unique-name component to start with a unix
/// timestamp, so the first character of any legitimate message
/// filename is an ASCII digit. Anything else inside cur/ or new/ --
/// dot-prefixed sidecars (.DS_Store, .#editor-swap, MUA locks),
/// letter-prefixed drop-ins (README, notes.bak), backup-tool
/// scratch (~tmp), future sidecar conventions we don't know about
/// today -- fails the digit-prefix test and is dropped at the
/// watcher. Filtering here avoids the downstream coalesce +
/// scan_paths chain entirely for those events.
///
/// The maildir crate's MailEntries iterator does the same skip
/// internally for its `.starts_with('.')` case but doesn't expose
/// the predicate as a public function. Worth lifting upstream --
/// exposing it would let downstream consumers skip the
/// re-implementation -- but the spec invariant is short enough to
/// inline for now.
fn is_maildir_message_path(path: &Path) -> bool {
    let s = path.to_string_lossy();
    if !(s.contains("/cur/") || s.contains("/new/")) {
        return false;
    }
    path.file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.chars().next())
        .is_some_and(|c| c.is_ascii_digit())
}

/// Whether a coalesced batch of relevant maildir paths can possibly
/// produce a `LocalChange` from `scan_paths`. Returning `false`
/// lets the watcher drop the batch before it reaches the trigger
/// channel, avoiding the mpsc send, the runner's coalesce wait, the
/// `engine.run` allocations, and the `Email/changes` round-trip on
/// the next cycle. The biggest line item is the network round-trip;
/// suppressing the wakeup matters for laptop-on-battery use.
///
/// Per-path verdict:
///
/// - `cur/` path: always potentially classifiable. Keep.
/// - live `new/` path: jma's contract is that `new/` events are
///   seen-only. The DB was already updated when jma delivered the
///   file, and an MUA's eventual promotion to `cur/` is what
///   actually triggers a real change. Drop.
/// - missing `new/` path: a file disappearing from `new/` while
///   anchored in the DB drives `scan_paths`' explicit_deletes
///   branch and emits `DeletedMessage`. Keep -- silently swallowing
///   a delete would let the server retain a message the user
///   removed.
///
/// The batch is droppable iff *every* path is a live `new/` path.
/// Any `cur/` path or any unlinked `new/` path keeps the batch.
fn batch_might_emit_changes(paths: &[PathBuf]) -> bool {
    paths.iter().any(|p| {
        let s = p.to_string_lossy();
        if s.contains("/cur/") {
            return true;
        }
        // Otherwise it's a `new/` path (the existing
        // is_maildir_message_path filter restricts to /cur/ or
        // /new/). Only droppable when the file still exists --
        // missing means a deletion that scan_paths must see.
        !p.exists()
    })
}

/// Watch local maildir directories for filesystem changes.
/// `self_writes`, when set, lets the watcher drop fsevents batches
/// whose every path matches a recent jma write -- the
/// new/->cur/ MUA-promotion-with-same-flags echo and similar.
/// CLI commands don't run a watcher; the daemon always passes
/// `Some(...)`.
pub async fn watch(
    maildir_root: &Path,
    debounce_secs: u64,
    tx: mpsc::Sender<SyncTrigger>,
    self_writes: Option<Arc<SelfWriteCache>>,
) -> Result<()> {
    crate::notify!("Watching maildir at {} for changes", maildir_root.display());

    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::channel::<Vec<PathBuf>>(100);

    let mut debouncer = new_debouncer(
        Duration::from_secs(debounce_secs),
        move |result: Result<Vec<notify_debouncer_mini::DebouncedEvent>, notify::Error>| {
            match result {
                Ok(events) => {
                    let paths: Vec<PathBuf> = events
                        .into_iter()
                        .filter(|e| {
                            matches!(e.kind, DebouncedEventKind::Any)
                                && is_maildir_message_path(&e.path)
                        })
                        .map(|e| e.path)
                        .collect();
                    if paths.is_empty() {
                        return;
                    }
                    if !batch_might_emit_changes(&paths) {
                        debug!(
                            "Skipping all-live-new fsevents batch ({} path(s)); scan would emit nothing",
                            paths.len()
                        );
                        return;
                    }
                    if let Some(cache) = &self_writes
                        && cache.matches_all(&paths)
                    {
                        debug!(
                            "Skipping fsevents batch ({} path(s)); all paths matched recent self-writes",
                            paths.len()
                        );
                        return;
                    }
                    let _ = notify_tx.blocking_send(paths);
                }
                Err(e) => {
                    warn!("Filesystem watcher error: {}", e);
                }
            }
        },
    )?;

    debouncer
        .watcher()
        .watch(maildir_root, notify::RecursiveMode::Recursive)?;

    info!("Filesystem watcher started");

    while let Some(paths) = notify_rx.recv().await {
        debug!("Local filesystem change detected ({} path(s))", paths.len());
        if tx.send(SyncTrigger::LocalChange(paths)).await.is_err() {
            info!("Sync channel closed, shutting down watcher");
            break;
        }
    }

    info!("Filesystem watcher ended");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn fires_on_cur_message() {
        let p = PathBuf::from("/home/u/Mail/INBOX/cur/1700000000.M1.host:2,S");
        assert!(is_maildir_message_path(&p));
    }

    #[test]
    fn fires_on_new_message() {
        let p = PathBuf::from("/home/u/Mail/INBOX/new/1700000000.M1.host");
        assert!(is_maildir_message_path(&p));
    }

    #[test]
    fn skips_tmp_message() {
        let p = PathBuf::from("/home/u/Mail/INBOX/tmp/1700000000.M1.host");
        assert!(!is_maildir_message_path(&p));
    }

    #[test]
    fn skips_state_db_and_wal_sidecars() {
        for name in [".jma.db", ".jma.db-wal", ".jma.db-shm"] {
            let p = PathBuf::from(format!("/home/u/Mail/{}", name));
            assert!(
                !is_maildir_message_path(&p),
                "{} should not fire a trigger",
                name
            );
        }
    }

    #[test]
    fn skips_lock_files() {
        for name in [".jma.lock", ".jma.db.lock"] {
            let p = PathBuf::from(format!("/home/u/Mail/{}", name));
            assert!(
                !is_maildir_message_path(&p),
                "{} should not fire a trigger",
                name
            );
        }
    }

    /// Anything whose filename doesn't start with an ASCII digit
    /// fails the maildir-spec shape check. Pins the allowlist's
    /// rejection surface across the categories that historically
    /// slipped through: dot-prefixed sidecars (finder artefacts,
    /// editor swap files, MUA locks), letter-prefixed drop-ins
    /// (READMEs, notes), symbol-prefixed scratch (backup tmps).
    /// A maildir message filename starts with a unix timestamp per
    /// spec, so every legitimate path begins with a digit.
    #[test]
    fn rejects_non_maildir_filenames_in_cur_or_new() {
        for path in [
            "/home/u/Mail/INBOX/cur/.DS_Store",
            "/home/u/Mail/INBOX/cur/.#emacs-swap",
            "/home/u/Mail/INBOX/new/.DS_Store",
            "/home/u/Mail/INBOX/cur/README",
            "/home/u/Mail/INBOX/cur/notes.bak",
            "/home/u/Mail/INBOX/cur/~tmp-backup",
        ] {
            let p = PathBuf::from(path);
            assert!(
                !is_maildir_message_path(&p),
                "{} should not fire a trigger",
                path
            );
        }
    }

    #[test]
    fn fires_on_nested_maildir_plus_plus_folder() {
        // Maildir++ uses `.foldername` directories for nested
        // folders; the `/cur/` substring check still picks up
        // messages there.
        let p = PathBuf::from("/home/u/Mail/INBOX/.archive/cur/1700000000.M1.host:2,S");
        assert!(is_maildir_message_path(&p));
    }

    #[test]
    fn skips_folder_directory_itself() {
        // The `cur` directory at the folder root, no trailing slash.
        // A folder being created (e.g. by sync provisioning a new
        // mailbox) shouldn't fire on the directory event alone --
        // any actual message inside will.
        let p = PathBuf::from("/home/u/Mail/INBOX/cur");
        assert!(!is_maildir_message_path(&p));
    }

    /// All-live-new batch: every path lives under a `new/` subdir
    /// AND the file is still there. Per `scan_paths`' contract this
    /// can't produce a LocalChange (new/ events are seen-only when
    /// the file is still present). The watcher drops the batch
    /// without firing a trigger. Pins the optimization that catches
    /// jma's own delivery echoing back through fsevents.
    #[test]
    fn all_live_new_batch_does_not_emit_changes() {
        let dir = tempfile::tempdir().unwrap();
        let inbox_new = dir.path().join("INBOX/new");
        let archive_new = dir.path().join("Archive/new");
        std::fs::create_dir_all(&inbox_new).unwrap();
        std::fs::create_dir_all(&archive_new).unwrap();
        let p1 = inbox_new.join("1700000000.M1.host");
        let p2 = archive_new.join("1700000001.M2.host");
        std::fs::write(&p1, b"").unwrap();
        std::fs::write(&p2, b"").unwrap();
        assert!(!batch_might_emit_changes(&[p1, p2]));
    }

    /// Missing `new/` path: the file was unlinked between fsevents
    /// firing and the watcher callback running. With the DB
    /// anchored to that id, `scan_paths` would emit a
    /// `DeletedMessage` via the explicit_deletes branch. Dropping
    /// the batch would silently swallow the deletion. Pins that the
    /// predicate keeps the batch in this case; mirrored by
    /// `maildir_ops::scan::tests::scan_paths_missing_new_path_emits_deleted_when_db_anchored`
    /// which pins the scan-side contract this depends on.
    #[test]
    fn missing_new_path_keeps_batch() {
        let dir = tempfile::tempdir().unwrap();
        let inbox_new = dir.path().join("INBOX/new");
        std::fs::create_dir_all(&inbox_new).unwrap();
        let missing = inbox_new.join("1700000000.M1.host");
        // Note: we deliberately do NOT create the file.
        assert!(batch_might_emit_changes(&[missing]));
    }

    /// All-cur batch: every path under a `cur/` subdir. These are
    /// the events `scan_paths` actually classifies, so the watcher
    /// must not drop them. The predicate doesn't even stat cur/
    /// paths; the substring check short-circuits.
    #[test]
    fn all_cur_batch_emits_changes() {
        let paths = vec![
            PathBuf::from("/home/u/Mail/INBOX/cur/1700000000.M1.host:2,S"),
            PathBuf::from("/home/u/Mail/Archive/cur/1700000001.M2.host:2,FS"),
        ];
        assert!(batch_might_emit_changes(&paths));
    }

    /// Mixed cur+new batch: arrives when an MUA promotes a `new/`
    /// file to `cur/` (both source and destination paths fire).
    /// The cur/ side is real work for `scan_paths`, so the batch
    /// must pass through. Regression net: an over-eager filter
    /// that dropped any batch with a new/ path would silently lose
    /// every flag-changing promotion.
    #[test]
    fn mixed_batch_emits_changes() {
        let paths = vec![
            PathBuf::from("/home/u/Mail/INBOX/new/1700000000.M1.host:2,F"),
            PathBuf::from("/home/u/Mail/INBOX/cur/1700000000.M1.host:2,FS"),
        ];
        assert!(batch_might_emit_changes(&paths));
    }
}
