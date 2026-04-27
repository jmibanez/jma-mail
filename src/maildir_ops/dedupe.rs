use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

use crate::maildir_ops::flags::extract_id;
use crate::maildir_ops::headers::parse_message_id_from_file;
use crate::maildir_ops::store;

/// One entry in the local message-ID index.
#[derive(Debug, Clone)]
pub struct LocalEntry {
    pub folder: String,
    pub maildir_id: String,
    pub path: PathBuf,
}

/// In-memory index of `Message-ID -> [location]` built from the maildir at
/// dedupe time. A single Message-ID can have one entry per folder (the same
/// message copied into multiple mailboxes is a legitimate user action, not a
/// duplicate). Used by the pull path to skip re-downloading messages we
/// already have on disk even when the state DB has been wiped.
#[derive(Debug, Default)]
pub struct LocalIndex {
    pub by_message_id: HashMap<String, Vec<LocalEntry>>,
}

/// Walk every synced maildir folder, parse Message-IDs out of each file, and
/// dedupe **within each folder**: when several files in the same folder share
/// a Message-ID, delete the newest by mtime (the most recently introduced
/// copy is, by construction, the duplicate jmapsync wrote on top of an
/// existing file). Cross-folder copies of the same Message-ID are preserved
/// — a user copying a message into another mailbox is a distinct instance.
/// The kept file (oldest mtime) per folder becomes an entry in the index.
pub fn dedupe_and_index(
    maildir_root: &Path,
    folders: &[String],
) -> Result<LocalIndex> {
    // Group by (folder, msgid) so dedupe is per-folder.
    let mut groups: HashMap<
        (String, String),
        Vec<(PathBuf, std::time::SystemTime, String)>,
    > = HashMap::new();

    for folder in folders {
        let folder_path = maildir_root.join(folder);
        for sub in &["cur", "new"] {
            let dir = folder_path.join(sub);
            if !dir.exists() {
                continue;
            }
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let filename = entry.file_name().to_string_lossy().to_string();
                let maildir_id = extract_id(&filename).to_string();

                let msgid = match parse_message_id_from_file(&path) {
                    Ok(Some(id)) => id,
                    Ok(None) => continue,
                    Err(e) => {
                        warn!("Failed to read headers from {}: {}", path.display(), e);
                        continue;
                    }
                };

                let mtime = entry
                    .metadata()
                    .and_then(|m| m.modified())
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

                groups
                    .entry((folder.clone(), msgid))
                    .or_default()
                    .push((path, mtime, maildir_id));
            }
        }
    }

    let mut index = LocalIndex::default();
    let mut deleted = 0usize;

    for ((folder, msgid), mut entries) in groups {
        // Oldest mtime first.
        entries.sort_by_key(|e| e.1);
        let mut iter = entries.into_iter();
        let Some((keep_path, _, keep_id)) = iter.next() else {
            continue;
        };

        index
            .by_message_id
            .entry(msgid.clone())
            .or_default()
            .push(LocalEntry {
                folder: folder.clone(),
                maildir_id: keep_id.clone(),
                path: keep_path.clone(),
            });

        for (_, _, dup_id) in iter {
            // Same Message-ID, same folder — delete this newer copy via the
            // maildir API so any maildir-level bookkeeping is honored.
            let md = store::ensure_maildir(&maildir_root.join(&folder))?;
            if let Err(e) = store::delete_message(&md, &dup_id) {
                warn!(
                    "Failed to delete duplicate {} in {}: {}",
                    dup_id, folder, e
                );
                continue;
            }

            info!(
                "Removed in-folder duplicate of Message-ID <{}>: {}/{} (kept {}/{})",
                msgid, folder, dup_id, folder, keep_id
            );
            deleted += 1;
        }
    }

    if deleted > 0 {
        info!("Dedupe pass removed {} duplicate file(s)", deleted);
    } else {
        debug!("Dedupe pass: no duplicates found");
    }

    Ok(index)
}

/// Heuristic: do any synced folders carry mbsync's per-channel state files?
/// Purely a hint for log clarity -- the adoption path works regardless of
/// what populated the maildir.
pub fn detect_mbsync_state(maildir_root: &Path, folders: &[String]) -> bool {
    const MARKERS: &[&str] = &[
        ".mbsyncstate",
        ".mbsyncstate.new",
        ".mbsyncstate.lock",
        ".uidvalidity",
    ];
    for folder in folders {
        let folder_path = maildir_root.join(folder);
        for marker in MARKERS {
            if folder_path.join(marker).exists() {
                return true;
            }
        }
    }
    false
}
