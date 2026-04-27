use anyhow::Result;
use notify_debouncer_mini::{new_debouncer, DebouncedEventKind};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::runner::SyncTrigger;

/// Watch local maildir directories for filesystem changes.
pub async fn watch(
    maildir_root: &Path,
    debounce_secs: u64,
    tx: mpsc::Sender<SyncTrigger>,
) -> Result<()> {
    info!("Watching maildir at {} for changes", maildir_root.display());

    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::channel(100);

    let mut debouncer = new_debouncer(
        Duration::from_secs(debounce_secs),
        move |result: Result<Vec<notify_debouncer_mini::DebouncedEvent>, notify::Error>| {
            match result {
                Ok(events) => {
                    let relevant = events.iter().any(|e| {
                        matches!(e.kind, DebouncedEventKind::Any)
                            && !e.path.to_string_lossy().contains("/tmp/")
                    });
                    if relevant {
                        let _ = notify_tx.blocking_send(());
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

    while notify_rx.recv().await.is_some() {
        debug!("Local filesystem change detected");
        if tx.send(SyncTrigger::LocalChange).await.is_err() {
            info!("Sync channel closed, shutting down watcher");
            break;
        }
    }

    info!("Filesystem watcher ended");
    Ok(())
}
