use anyhow::Result;
use maildir::Maildir;
use std::collections::HashMap;
use std::path::Path;
use std::time::SystemTime;
use tracing::{debug, error, info, warn};

use crate::ids::{MaildirId, MessageId};
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
    mtime: SystemTime,
    maildir_id: MaildirId,
}

/// One entry in the local message-ID index.
#[derive(Debug, Clone)]
pub struct LocalEntry {
    pub folder: String,
    pub maildir_id: MaildirId,
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
/// copy is, by construction, the duplicate jma wrote on top of an
/// existing file). Cross-folder copies of the same Message-ID are preserved
/// — a user copying a message into another mailbox is a distinct instance.
///
/// `on_kept` fires once per (folder, Message-ID) group, with the kept
/// file's identifying tuple. Callers that want a `LocalIndex` build one
/// inside the closure; callers that don't pass a no-op and pay nothing
/// for indexing.
pub fn dedupe<F>(maildir_root: &Path, folders: &[String], mut on_kept: F) -> Result<()>
where
    F: FnMut(&str, &MessageId, &MaildirId),
{
    // Walk each folder in parallel: pure file I/O on independent
    // subtrees with no shared state. Folder count is typically O(10)
    // for real accounts, so one scoped thread per folder gives
    // near-linear speedup on SSD without needing a pool. Uncapped
    // for now -- a chunk-based cap would load-balance poorly on the
    // INBOX-dominant shape of real mail (one big folder + many
    // small ones), so the right cap shape needs measurement on a
    // many-folder account before it lands.
    let scan_results: Vec<Vec<(MessageId, Candidate)>> =
        std::thread::scope(|s| -> Result<Vec<Vec<(MessageId, Candidate)>>> {
            let handles: Vec<_> = folders
                .iter()
                .map(|folder| s.spawn(|| scan_folder(maildir_root, folder)))
                .collect();
            handles
                .into_iter()
                .map(|h| match h.join() {
                    Ok(r) => r,
                    Err(p) => std::panic::resume_unwind(p),
                })
                .collect()
        })?;

    let mut groups: HashMap<GroupKey, Vec<Candidate>> = HashMap::new();
    for (folder, partial) in folders.iter().zip(scan_results) {
        for (msgid, candidate) in partial {
            groups
                .entry(GroupKey {
                    folder: folder.clone(),
                    msgid,
                })
                .or_default()
                .push(candidate);
        }
    }

    let mut deleted = 0usize;

    for (GroupKey { folder, msgid }, mut candidates) in groups {
        // Oldest mtime first.
        candidates.sort_by_key(|c| c.mtime);
        let mut iter = candidates.into_iter();
        let Some(keep) = iter.next() else {
            continue;
        };

        on_kept(&folder, &msgid, &keep.maildir_id);

        for dup in iter {
            // Same Message-ID, same folder -- delete this newer copy
            // via the maildir API so any maildir-level bookkeeping is
            // honored.
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

    Ok(())
}

/// Walk one folder's cur/ and new/, parse the Message-ID out of each
/// file, and emit `(msgid, candidate)` tuples for the caller to merge.
/// Runs in its own thread under `dedupe`'s scoped-thread fan-out; the
/// folder identity isn't returned because the caller already knows
/// which folder this result came from via the input ordering.
fn scan_folder(maildir_root: &Path, folder: &str) -> Result<Vec<(MessageId, Candidate)>> {
    // Delegate "what counts as a maildir file" to the crate:
    // list_cur/list_new filter dot-prefixed entries (.DS_Store,
    // .nfsXXXX) and enforce the `<unique>:2,<flags>` convention.
    // Maildir::from(PathBuf) is a pure constructor, and the
    // iterators yield nothing when the subdir is missing, so
    // dedupe stays non-mutating and the "skip missing folder"
    // behavior is preserved.
    let md = Maildir::from(maildir_root.join(folder));
    let mut out = Vec::new();
    for entry in md.list_cur().chain(md.list_new()) {
        let entry = entry?;
        let maildir_id = MaildirId::from(entry.id());
        let path = entry.path().clone();

        let msgid = match parse_message_id_from_file(&path)? {
            Some(id) => id,
            None => {
                error!(
                    "Skipping {} ({}): no Message-ID header. \
                     jma requires Message-ID to anchor idempotency; \
                     fix the file or remove it.",
                    maildir_id,
                    path.display()
                );
                continue;
            }
        };

        let mtime = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);

        out.push((msgid, Candidate { mtime, maildir_id }));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maildir_ops::store::ensure_maildir;
    use std::fs;
    use tempfile::TempDir;

    /// Finder writes .DS_Store metadata into folders it browses,
    /// including a maildir's cur/ and new/. dedupe walks via the
    /// maildir crate's list_cur/list_new iterators specifically
    /// because they filter dot-prefixed entries — guard that
    /// delegation so a future "let's avoid the crate dependency
    /// here" refactor can't silently regress and trip the
    /// require-Message-ID error path on every sync.
    #[test]
    fn dedupe_skips_dot_prefixed_files() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,S");
        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";
        fs::write(inbox_path.join("cur").join(&filename), body).unwrap();

        // Dot-prefixed files in both cur/ and new/ — .DS_Store is
        // arbitrary binary noise that won't parse as a header block.
        fs::write(inbox_path.join("cur").join(".DS_Store"), b"\x00\x01\x02").unwrap();
        fs::write(inbox_path.join("new").join(".keep"), b"").unwrap();

        let folders = vec!["INBOX".to_string()];
        let mut index = LocalIndex::default();
        dedupe(tmp.path(), &folders, |folder, msgid, mid| {
            index
                .by_message_id
                .entry(msgid.clone())
                .or_default()
                .push(LocalEntry {
                    folder: folder.to_string(),
                    maildir_id: mid.clone(),
                });
        })
        .unwrap();

        assert_eq!(
            index.by_message_id.len(),
            1,
            "expected one indexed Message-ID, got {:?}",
            index.by_message_id
        );
        let entries = index
            .by_message_id
            .get(&MessageId::from("a@x"))
            .expect("real mail file should be indexed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].folder, "INBOX");
        assert_eq!(entries[0].maildir_id.as_ref(), unique);
    }

    /// Suffix-bearing new/ files (the shape jma writes when delivering
    /// an unseen message with server-set flags like F) must dedupe
    /// correctly: the canonical id (sans `:2,<flags>`) surfaces on
    /// `on_kept`, and a duplicate in new/ gets deleted on disk. Pre-
    /// fix this regressed because (a) `entry.id()` returns the full
    /// filename for new/ entries so the kept id leaked the suffix, and
    /// (b) the maildir crate's id-keyed `delete` couldn't find the
    /// canonical id in new/ -- duplicates would be detected but
    /// deletion would silently fail.
    #[test]
    fn dedupe_handles_suffix_bearing_new_files() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";

        // Older copy in cur/ (the legitimate one), newer duplicate in
        // new/ with a `:2,F` suffix (the shape we now write on
        // delivery).
        let kept_unique = "1700000000.M1.host";
        let dup_unique = "1700000005.M2.host";
        let kept_filename = format!("{kept_unique}:2,S");
        let dup_filename = format!("{dup_unique}:2,F");
        fs::write(inbox_path.join("cur").join(&kept_filename), body).unwrap();
        fs::write(inbox_path.join("new").join(&dup_filename), body).unwrap();

        // Pin both sides of the ordering relation explicitly so the
        // test doesn't rely on ambient mtime granularity (and survives
        // a future edit that removes one of the two adjustments).
        let now = SystemTime::now();
        let kept_mtime = now - std::time::Duration::from_secs(60);
        fs::File::open(inbox_path.join("cur").join(&kept_filename))
            .and_then(|f| f.set_modified(kept_mtime))
            .unwrap();
        fs::File::open(inbox_path.join("new").join(&dup_filename))
            .and_then(|f| f.set_modified(now))
            .unwrap();

        let folders = vec!["INBOX".to_string()];
        let mut index = LocalIndex::default();
        dedupe(tmp.path(), &folders, |folder, msgid, mid| {
            index
                .by_message_id
                .entry(msgid.clone())
                .or_default()
                .push(LocalEntry {
                    folder: folder.to_string(),
                    maildir_id: mid.clone(),
                });
        })
        .unwrap();

        // Duplicate file is gone from disk.
        assert!(
            !inbox_path.join("new").join(&dup_filename).exists(),
            "duplicate new/ file must have been removed"
        );
        assert!(
            inbox_path.join("cur").join(&kept_filename).exists(),
            "kept cur/ file must still exist"
        );

        // on_kept saw the canonical id, not the suffix-bearing form.
        let entries = index
            .by_message_id
            .get(&MessageId::from("a@x"))
            .expect("kept file should be indexed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].maildir_id.as_ref(), kept_unique);
    }

    /// Parallel per-folder fan-out must merge results so that
    /// (a) Message-IDs unique to each folder both surface to the
    /// caller, and (b) the same Message-ID present in two different
    /// folders is preserved as two distinct LocalEntry rows -- the
    /// "user copied the message into another mailbox" case dedupe
    /// explicitly does not flatten. Pin both invariants here so a
    /// regression in the merge step (wrong indexing, lost partial
    /// results, cross-folder collapse) trips a test.
    #[test]
    fn dedupe_walks_multiple_folders() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let sent_path = tmp.path().join("Sent");
        let _inbox = ensure_maildir(&inbox_path).unwrap();
        let _sent = ensure_maildir(&sent_path).unwrap();

        // a@x lives in both folders -- a legitimate user-side copy
        // across mailboxes that dedupe must not collapse.
        let body_a = "Message-ID: <a@x>\r\n\r\nbody-a\r\n";
        let body_b = "Message-ID: <b@x>\r\n\r\nbody-b\r\n";
        fs::write(
            inbox_path.join("cur").join("1700000000.M1.host:2,S"),
            body_a,
        )
        .unwrap();
        fs::write(
            inbox_path.join("cur").join("1700000001.M2.host:2,S"),
            body_b,
        )
        .unwrap();
        fs::write(sent_path.join("cur").join("1700000002.M3.host:2,S"), body_a).unwrap();

        let folders = vec!["INBOX".to_string(), "Sent".to_string()];
        let mut index = LocalIndex::default();
        dedupe(tmp.path(), &folders, |folder, msgid, mid| {
            index
                .by_message_id
                .entry(msgid.clone())
                .or_default()
                .push(LocalEntry {
                    folder: folder.to_string(),
                    maildir_id: mid.clone(),
                });
        })
        .unwrap();

        assert_eq!(index.by_message_id.len(), 2, "expected two Message-IDs");

        let a_entries = index
            .by_message_id
            .get(&MessageId::from("a@x"))
            .expect("a@x must be present in both folders");
        assert_eq!(a_entries.len(), 2, "a@x must appear once per folder");
        let mut a_folders: Vec<&str> = a_entries.iter().map(|e| e.folder.as_str()).collect();
        a_folders.sort();
        assert_eq!(a_folders, vec!["INBOX", "Sent"]);

        let b_entries = index
            .by_message_id
            .get(&MessageId::from("b@x"))
            .expect("b@x must be present in INBOX");
        assert_eq!(b_entries.len(), 1);
        assert_eq!(b_entries[0].folder, "INBOX");
    }
}
