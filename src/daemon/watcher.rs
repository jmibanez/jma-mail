use anyhow::Result;
use notify_debouncer_mini::{DebouncedEventKind, new_debouncer};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::runner::SyncTrigger;

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
fn is_maildir_message_path(path: &Path) -> bool {
    let s = path.to_string_lossy();
    s.contains("/cur/") || s.contains("/new/")
}

/// Watch local maildir directories for filesystem changes.
pub async fn watch(
    maildir_root: &Path,
    debounce_secs: u64,
    tx: mpsc::Sender<SyncTrigger>,
) -> Result<()> {
    info!("Watching maildir at {} for changes", maildir_root.display());

    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::channel::<Vec<PathBuf>>(100);

    let mut debouncer = new_debouncer(
        Duration::from_secs(debounce_secs),
        move |result: Result<Vec<notify_debouncer_mini::DebouncedEvent>, notify::Error>| {
            match result {
                Ok(events) => {
                    // Forward the relevant FS paths so the runner can
                    // surface them when a `LocalChange` cycle ends up
                    // doing nothing -- the path list is the only clue
                    // to what wrote.
                    let paths: Vec<PathBuf> = events
                        .into_iter()
                        .filter(|e| {
                            matches!(e.kind, DebouncedEventKind::Any)
                                && is_maildir_message_path(&e.path)
                        })
                        .map(|e| e.path)
                        .collect();
                    if !paths.is_empty() {
                        let _ = notify_tx.blocking_send(paths);
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
}
