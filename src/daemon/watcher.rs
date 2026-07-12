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
/// scratch, and the maildir root itself can hold sidecar files jma
/// writes inside its private namespace (see
/// `crate::maildir_ops::namespace`) that shouldn't kick a sync.
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

/// Whether a notify event path is a candidate for structural
/// classification. This is a selector, not a verdict: it nominates
/// non-message paths under `maildir_root` that promote the whole
/// batch to a Full-scope cycle. It does not itself confirm a
/// structural change occurred.
///
/// A path qualifies as a candidate when it sits under
/// `maildir_root`, is NOT in jma's private namespace (see
/// `crate::maildir_ops::namespace::is_jma_private`, which covers
/// both the `.jma.*` and `.jma-*` prefixes), and does NOT
/// contain `/cur/`, `/new/`, or `/tmp/` as a substring within
/// the relative-to-root path (those are either message-file
/// events handled by `is_maildir_message_path` or mid-delivery
/// scratch). The remaining cases capture folder-level events
/// (the folder directory itself, the `cur`/`new`/`tmp` directory
/// trio at the folder root, or other non-message paths within a
/// folder's lifecycle). The substring check runs on the
/// relative path so a maildir root whose own ancestor name
/// happens to contain those segments doesn't suppress
/// detection.
fn is_structural_candidate_path(path: &Path, maildir_root: &Path) -> bool {
    let Ok(rel) = path.strip_prefix(maildir_root) else {
        return false;
    };
    if rel.as_os_str().is_empty() {
        return false;
    }
    for component in rel.components() {
        if let std::path::Component::Normal(seg) = component
            && let Some(seg_str) = seg.to_str()
            && crate::maildir_ops::namespace::is_jma_private(seg_str)
        {
            return false;
        }
    }
    let rel_str = rel.to_string_lossy();
    !(rel_str.contains("/cur/") || rel_str.contains("/new/") || rel_str.contains("/tmp/"))
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

/// Classify one coalesced batch of event paths into the trigger it
/// should produce, applying the two message-path filters
/// (`batch_might_emit_changes`, self-write matching) regardless of
/// whether the batch also carries structural candidates. Returns
/// `None` when the batch collapses to nothing worth sending.
fn classify_batch(
    paths: Vec<PathBuf>,
    maildir_root: &Path,
    self_writes: Option<&SelfWriteCache>,
) -> Option<SyncTrigger> {
    let (message_paths, structural_candidates): (Vec<PathBuf>, Vec<PathBuf>) =
        paths.into_iter().partition(|p| is_maildir_message_path(p));
    let structural_candidates: Vec<PathBuf> = structural_candidates
        .into_iter()
        .filter(|p| is_structural_candidate_path(p, maildir_root))
        .collect();

    // The two batch-level filters apply to the message-path set
    // regardless of whether the batch also has structural
    // candidates: a filtered-out message set contributes nothing,
    // whether it rides along with a structural trigger or would
    // have gone out alone.
    let message_paths = if message_paths.is_empty() {
        Vec::new()
    } else if !batch_might_emit_changes(&message_paths) {
        debug!(
            "Dropping all-live-new message path(s) ({} path(s)); scan would emit nothing",
            message_paths.len()
        );
        Vec::new()
    } else if self_writes.is_some_and(|cache| cache.matches_all(&message_paths)) {
        debug!(
            "Dropping {} message path(s); all matched recent self-writes",
            message_paths.len()
        );
        Vec::new()
    } else {
        message_paths
    };

    if !structural_candidates.is_empty() {
        Some(SyncTrigger::LocalStructuralChange {
            candidates: structural_candidates,
            message_paths,
        })
    } else if !message_paths.is_empty() {
        Some(SyncTrigger::LocalChange(message_paths))
    } else {
        None
    }
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

    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::channel::<SyncTrigger>(100);
    let maildir_root_owned = maildir_root.to_path_buf();

    let mut debouncer = new_debouncer(
        Duration::from_secs(debounce_secs),
        move |result: Result<Vec<notify_debouncer_mini::DebouncedEvent>, notify::Error>| {
            match result {
                Ok(events) => {
                    // A batch with any structural candidate needs a
                    // Full-scope cycle (the destructive-arm matrix +
                    // bidirectional folder lifecycle can't be safely
                    // run in Paths scope), but the message paths in
                    // the same batch are still useful diagnostic
                    // context for that cycle, so they ride along
                    // instead of being dropped.
                    let paths: Vec<PathBuf> = events
                        .into_iter()
                        .filter(|e| matches!(e.kind, DebouncedEventKind::Any))
                        .map(|e| e.path)
                        .collect();
                    if let Some(trigger) =
                        classify_batch(paths, &maildir_root_owned, self_writes.as_deref())
                    {
                        let _ = notify_tx.blocking_send(trigger);
                    }
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
    // Reported only once the debouncer is actually watching -- a
    // setup failure above returns Err before this, and the runner's
    // spawn wrapper turns that exit into a "down" report.
    tracing::event!(
        target: crate::tui::layer::TARGET_TUI_CONN,
        tracing::Level::TRACE,
        channel = "watcher",
        state = "connected",
    );

    while let Some(trigger) = notify_rx.recv().await {
        debug!("Local filesystem change detected ({})", trigger);
        if tx.send(trigger).await.is_err() {
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
        // mailbox) shouldn't fire as a message-path trigger --
        // structural detection handles it via the separate
        // `is_structural_candidate_path` predicate.
        let p = PathBuf::from("/home/u/Mail/INBOX/cur");
        assert!(!is_maildir_message_path(&p));
    }

    #[test]
    fn structural_fires_on_top_level_folder_create() {
        let root = PathBuf::from("/home/u/Mail");
        let p = PathBuf::from("/home/u/Mail/Projects");
        assert!(is_structural_candidate_path(&p, &root));
    }

    #[test]
    fn structural_fires_on_maildir_trio_dirs() {
        // The cur/new/tmp directory trio appearing under a folder
        // root: the user (or jma's executor) is provisioning or
        // tearing down a maildir. Each of the three dirs is a
        // folder-level event and must promote the cycle to Full
        // scope.
        let root = PathBuf::from("/home/u/Mail");
        for sub in ["cur", "new", "tmp"] {
            let p = PathBuf::from(format!("/home/u/Mail/Projects/{}", sub));
            assert!(
                is_structural_candidate_path(&p, &root),
                "{} should classify as structural",
                p.display()
            );
        }
    }

    #[test]
    fn structural_fires_on_nested_fs_layout_folder() {
        // Dovecot `LAYOUT=fs` nests folders as recursive
        // directories. A new `Personal/Notes` maildir is a
        // structural event at the leaf.
        let root = PathBuf::from("/home/u/Mail");
        let p = PathBuf::from("/home/u/Mail/Personal/Notes");
        assert!(is_structural_candidate_path(&p, &root));
    }

    #[test]
    fn structural_skips_jma_private_namespace() {
        let root = PathBuf::from("/home/u/Mail");
        // `is_jma_private` covers both `.jma.*` (db, db-wal, lock,
        // discovery, mapping sentinel) and `.jma-*` (rescue dir
        // for unmapped files staged before destructive folder
        // sync). Both prefix shapes must be rejected by the
        // structural classifier so the watcher doesn't fire on
        // jma's own internal writes.
        for name in [
            ".jma.lock",
            ".jma.db",
            ".jma.db-wal",
            ".jma.discovery",
            ".jma-rescue",
        ] {
            let p = PathBuf::from(format!("/home/u/Mail/{}", name));
            assert!(
                !is_structural_candidate_path(&p, &root),
                "{} must not fire structural; it's private jma state",
                p.display()
            );
        }
    }

    #[test]
    fn structural_skips_message_paths_under_cur_new_tmp() {
        // Files inside cur/, new/, tmp/ are message-level events
        // (or mid-delivery scratch); the message-path predicate
        // covers them. Structural detection only sees folder-level
        // events.
        let root = PathBuf::from("/home/u/Mail");
        for path in [
            "/home/u/Mail/INBOX/cur/1700000000.M1.host:2,S",
            "/home/u/Mail/INBOX/new/1700000000.M1.host",
            "/home/u/Mail/INBOX/tmp/1700000000.M1.host",
        ] {
            let p = PathBuf::from(path);
            assert!(
                !is_structural_candidate_path(&p, &root),
                "{} must not fire structural; it's a message-level path",
                path
            );
        }
    }

    #[test]
    fn structural_skips_path_outside_maildir_root() {
        let root = PathBuf::from("/home/u/Mail");
        let p = PathBuf::from("/home/u/Documents/Foo");
        assert!(!is_structural_candidate_path(&p, &root));
    }

    #[test]
    fn structural_skips_root_event() {
        let root = PathBuf::from("/home/u/Mail");
        assert!(!is_structural_candidate_path(&root, &root));
    }

    #[test]
    fn structural_substring_check_is_relative_to_root() {
        // The /cur/, /new/, /tmp/ substring suppression runs on
        // the relative-to-root path. A user whose maildir root
        // sits at a path that already contains one of those
        // segments (`/home/u/curated/Mail`) would otherwise have
        // every event suppressed -- the prefix `/cur/` would match
        // anywhere in the full path string.
        let root = PathBuf::from("/home/u/curated/Mail");
        let p = PathBuf::from("/home/u/curated/Mail/Projects");
        assert!(is_structural_candidate_path(&p, &root));
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

    /// A batch mixing a structural candidate (folder create) with
    /// message paths (a normal delivery elsewhere) must carry both
    /// sets on the structural trigger rather than dropping the
    /// message paths -- the whole point of the struct variant is
    /// that a Full-scope cycle triggered by a folder event still
    /// gets the FS-event paths as diagnostic context.
    #[test]
    fn classify_batch_carries_both_candidates_and_message_paths() {
        let root = PathBuf::from("/home/u/Mail");
        let candidate = PathBuf::from("/home/u/Mail/Projects");
        let message = PathBuf::from("/home/u/Mail/INBOX/cur/1700000000.M1.host:2,S");
        let trigger = classify_batch(vec![candidate.clone(), message.clone()], &root, None)
            .expect("batch should produce a trigger");
        match trigger {
            SyncTrigger::LocalStructuralChange {
                candidates,
                message_paths,
            } => {
                assert_eq!(candidates, vec![candidate]);
                assert_eq!(message_paths, vec![message]);
            }
            other => panic!("expected LocalStructuralChange, got {:?}", other),
        }
    }

    /// A batch whose message paths are all matched by the self-write
    /// cache but which also has a structural candidate still sends
    /// the structural trigger -- the candidate isn't subject to the
    /// self-write filter -- but with an empty message_paths, since
    /// the message side was fully suppressed.
    #[test]
    fn classify_batch_structural_survives_self_write_filtered_messages() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let inbox_cur = root.join("INBOX/cur");
        std::fs::create_dir_all(&inbox_cur).unwrap();
        let message = inbox_cur.join("1700000000.M1.host:2,S");
        std::fs::write(&message, b"").unwrap();
        let candidate = root.join("Projects");

        let cache = SelfWriteCache::new(Duration::from_secs(60));
        cache.record([message.clone()]);

        let trigger = classify_batch(vec![candidate.clone(), message], &root, Some(&cache))
            .expect("batch should produce a trigger");
        match trigger {
            SyncTrigger::LocalStructuralChange {
                candidates,
                message_paths,
            } => {
                assert_eq!(candidates, vec![candidate]);
                assert!(message_paths.is_empty());
            }
            other => panic!("expected LocalStructuralChange, got {:?}", other),
        }
    }

    /// A new/->cur/ MUA promotion pair is entirely message-shaped:
    /// neither path sits outside cur/new/tmp, so neither qualifies
    /// as a structural candidate. Pins that ordinary message
    /// traffic never gets diverted onto the structural route -- it
    /// must stay on LocalChange so it still gets Paths-scope
    /// treatment.
    #[test]
    fn new_to_cur_promotion_produces_no_structural_candidates() {
        let root = PathBuf::from("/home/u/Mail");
        let new_path = PathBuf::from("/home/u/Mail/INBOX/new/1700000000.M1.host:2,F");
        let cur_path = PathBuf::from("/home/u/Mail/INBOX/cur/1700000000.M1.host:2,FS");
        for p in [&new_path, &cur_path] {
            assert!(
                !is_structural_candidate_path(p, &root),
                "{} must not be a structural candidate",
                p.display()
            );
        }
        let trigger = classify_batch(vec![new_path, cur_path], &root, None)
            .expect("batch should produce a trigger");
        assert!(matches!(trigger, SyncTrigger::LocalChange(_)));
    }
}
