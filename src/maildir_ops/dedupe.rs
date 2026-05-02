use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tracing::{debug, info, warn};

use crate::ids::{MaildirId, MessageId};
use crate::maildir_ops::flags::extract_id;
use crate::maildir_ops::headers::parse_message_id_from_file;
use crate::maildir_ops::store;

/// Per-folder Message-ID — the dedupe scope. Two files share a group
/// iff they're in the same folder and parse to the same Message-ID.
#[derive(PartialEq, Eq, Hash)]
struct GroupKey {
    folder: String,
    msgid: MessageId,
}

/// One file that might be the kept copy for a `GroupKey`. Built up
/// during the maildir walk; the oldest mtime per group wins.
struct Candidate {
    path: PathBuf,
    mtime: SystemTime,
    maildir_id: MaildirId,
}

/// One entry in the local message-ID index.
#[derive(Debug, Clone)]
pub struct LocalEntry {
    pub folder: String,
    pub maildir_id: MaildirId,
    pub path: PathBuf,
}

/// In-memory index of `Message-ID -> [location]` built from the maildir at
/// dedupe time. A single Message-ID can have one entry per folder (the same
/// message copied into multiple mailboxes is a legitimate user action, not a
/// duplicate). Used by the pull path to skip re-downloading messages we
/// already have on disk even when the state DB has been wiped.
#[derive(Debug, Default)]
pub struct LocalIndex {
    pub by_message_id: HashMap<MessageId, Vec<LocalEntry>>,
}

/// Walk every synced maildir folder, parse Message-IDs out of each file, and
/// dedupe **within each folder**: when several files in the same folder share
/// a Message-ID, delete the newest by mtime (the most recently introduced
/// copy is, by construction, the duplicate jmapsync wrote on top of an
/// existing file). Cross-folder copies of the same Message-ID are preserved
/// — a user copying a message into another mailbox is a distinct instance.
/// The kept file (oldest mtime) per folder becomes an entry in the index.
pub fn dedupe_and_index(maildir_root: &Path, folders: &[String]) -> Result<LocalIndex> {
    let mut groups: HashMap<GroupKey, Vec<Candidate>> = HashMap::new();

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
                let maildir_id: MaildirId = extract_id(&filename).into();

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
                    .unwrap_or(SystemTime::UNIX_EPOCH);

                groups
                    .entry(GroupKey {
                        folder: folder.clone(),
                        msgid,
                    })
                    .or_default()
                    .push(Candidate {
                        path,
                        mtime,
                        maildir_id,
                    });
            }
        }
    }

    let mut index = LocalIndex::default();
    let mut deleted = 0usize;

    for (GroupKey { folder, msgid }, mut candidates) in groups {
        // Oldest mtime first.
        candidates.sort_by_key(|c| c.mtime);
        let mut iter = candidates.into_iter();
        let Some(keep) = iter.next() else {
            continue;
        };

        index
            .by_message_id
            .entry(msgid.clone())
            .or_default()
            .push(LocalEntry {
                folder: folder.clone(),
                maildir_id: keep.maildir_id.clone(),
                path: keep.path.clone(),
            });

        for dup in iter {
            // Same Message-ID, same folder — delete this newer copy via the
            // maildir API so any maildir-level bookkeeping is honored.
            let md = store::ensure_maildir(&maildir_root.join(&folder))?;
            if let Err(e) = store::delete_message(&md, dup.maildir_id.as_ref()) {
                warn!(
                    "Failed to delete duplicate {} in {}: {}",
                    dup.maildir_id, folder, e
                );
                continue;
            }

            info!(
                "Removed in-folder duplicate of Message-ID <{}>: {}/{} (kept {}/{})",
                msgid, folder, dup.maildir_id, folder, keep.maildir_id
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
