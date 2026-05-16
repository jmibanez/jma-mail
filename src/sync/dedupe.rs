use anyhow::Result;
use maildir::Maildir;
use std::collections::HashMap;
use std::path::Path;
use std::time::SystemTime;
use tracing::{debug, error, info, warn};

use crate::ids::{MaildirId, MessageId};
use crate::maildir_ops::headers::parse_message_id_from_file;
use crate::maildir_ops::store;

/// Per-folder Message-ID -- the dedupe scope. Two files share a group
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

/// One `(folder, Message-ID)` group's surviving file. Emitted by
/// `plan_dedupe` so callers can rebuild a `LocalIndex` (or any other
/// per-kept derivation) without re-walking the maildir.
#[derive(Debug, Clone)]
pub struct KeptEntry {
    pub folder: String,
    pub message_id: MessageId,
    pub maildir_id: MaildirId,
}

/// One file `plan_dedupe` classified as a duplicate to be removed. The
/// surviving file's id rides along for context in logs and the dry-run
/// printout.
#[derive(Debug, Clone)]
pub struct DedupeDeletion {
    pub folder: String,
    pub message_id: MessageId,
    pub maildir_id: MaildirId,
    pub kept_maildir_id: MaildirId,
}

/// The classified outcome of a dedupe pass: which file survives in each
/// `(folder, Message-ID)` group, and which duplicates would be removed.
/// `plan_dedupe` produces this without touching disk; `apply_dedupe`
/// (or the dry-run renderer) decides what to do with it.
#[derive(Debug, Default)]
pub struct DedupePlan {
    pub kept: Vec<KeptEntry>,
    pub deletions: Vec<DedupeDeletion>,
}

/// Walk every synced maildir folder, parse Message-IDs out of each
/// file, and classify per-folder duplicates: when several files in the
/// same folder share a Message-ID, the oldest by mtime is kept and the
/// rest are marked for deletion (the most recently introduced copy is,
/// by construction, the duplicate jma wrote on top of an existing
/// file). Cross-folder copies of the same Message-ID are preserved --
/// a user copying a message into another mailbox is a distinct
/// instance.
///
/// Pure: this function does not delete any files. Pair with
/// `apply_dedupe` to execute the plan, or inspect `plan.deletions`
/// for a dry-run preview.
pub fn plan_dedupe(maildir_root: &Path, folders: &[String]) -> Result<DedupePlan> {
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

    let mut plan = DedupePlan::default();

    for (GroupKey { folder, msgid }, mut candidates) in groups {
        // Oldest mtime first.
        candidates.sort_by_key(|c| c.mtime);
        let mut iter = candidates.into_iter();
        let Some(keep) = iter.next() else {
            continue;
        };

        plan.kept.push(KeptEntry {
            folder: folder.clone(),
            message_id: msgid.clone(),
            maildir_id: keep.maildir_id.clone(),
        });

        for dup in iter {
            plan.deletions.push(DedupeDeletion {
                folder: folder.clone(),
                message_id: msgid.clone(),
                maildir_id: dup.maildir_id,
                kept_maildir_id: keep.maildir_id.clone(),
            });
        }
    }

    Ok(plan)
}

/// Execute a `DedupePlan` against disk. Deletes each planned duplicate
/// via the maildir API so any maildir-level bookkeeping is honored;
/// per-file failures are logged at `warn!` and the pass continues.
pub fn apply_dedupe(maildir_root: &Path, plan: &DedupePlan) -> Result<()> {
    let mut deleted = 0usize;

    for d in &plan.deletions {
        let md = store::ensure_maildir(&maildir_root.join(&d.folder))?;
        if let Err(e) = store::delete_message(&md, d.maildir_id.as_ref()) {
            warn!(
                "Failed to delete duplicate {} in {}: {}",
                d.maildir_id, d.folder, e
            );
            continue;
        }

        info!(
            "Removed in-folder duplicate of Message-ID <{}>: {}/{} (kept {}/{})",
            d.message_id, d.folder, d.maildir_id, d.folder, d.kept_maildir_id
        );
        deleted += 1;
    }

    if deleted > 0 {
        info!("Dedupe pass removed {} duplicate file(s)", deleted);
    } else {
        debug!("Dedupe pass: no duplicates to delete");
    }

    Ok(())
}

/// Walk one folder's cur/ and new/, parse the Message-ID out of each
/// file, and emit `(msgid, candidate)` tuples for the caller to merge.
/// Runs in its own thread under `plan_dedupe`'s scoped-thread fan-out;
/// the folder identity isn't returned because the caller already knows
/// which folder this result came from via the input ordering.
fn scan_folder(maildir_root: &Path, folder: &str) -> Result<Vec<(MessageId, Candidate)>> {
    // Delegate "what counts as a maildir file" to the crate:
    // list_cur/list_new filter dot-prefixed entries (.DS_Store,
    // .nfsXXXX) and enforce the `<unique>:2,<flags>` convention.
    // Maildir::from(PathBuf) is a pure constructor, and the
    // iterators yield nothing when the subdir is missing, so
    // plan_dedupe stays non-mutating and the "skip missing folder"
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

    /// Build the `LocalIndex` shape engine.rs derives from a plan's
    /// `kept` list. Tests use this so their assertions read against the
    /// same projection the production caller will.
    fn index_from_kept(plan: &DedupePlan) -> LocalIndex {
        let mut index = LocalIndex::default();
        for k in &plan.kept {
            index
                .by_message_id
                .entry(k.message_id.clone())
                .or_default()
                .push(LocalEntry {
                    folder: k.folder.clone(),
                    maildir_id: k.maildir_id.clone(),
                });
        }
        index
    }

    /// Finder writes .DS_Store metadata into folders it browses,
    /// including a maildir's cur/ and new/. plan_dedupe walks via the
    /// maildir crate's list_cur/list_new iterators specifically
    /// because they filter dot-prefixed entries -- guard that
    /// delegation so a future "let's avoid the crate dependency
    /// here" refactor can't silently regress and trip the
    /// require-Message-ID error path on every sync.
    #[test]
    fn plan_dedupe_skips_dot_prefixed_files() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,S");
        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";
        fs::write(inbox_path.join("cur").join(&filename), body).unwrap();

        // Dot-prefixed files in both cur/ and new/ -- .DS_Store is
        // arbitrary binary noise that won't parse as a header block.
        fs::write(inbox_path.join("cur").join(".DS_Store"), b"\x00\x01\x02").unwrap();
        fs::write(inbox_path.join("new").join(".keep"), b"").unwrap();

        let folders = vec!["INBOX".to_string()];
        let plan = plan_dedupe(tmp.path(), &folders).unwrap();
        let index = index_from_kept(&plan);

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
        assert!(
            plan.deletions.is_empty(),
            "no duplicates -> no deletions, got {:?}",
            plan.deletions
        );
    }

    /// Suffix-bearing new/ files (the shape jma writes when delivering
    /// an unseen message with server-set flags like F) must dedupe
    /// correctly: the canonical id (sans `:2,<flags>`) surfaces in
    /// `plan.kept`, and a duplicate in new/ gets deleted on disk by
    /// apply_dedupe. Pre-fix this regressed because (a) `entry.id()`
    /// returns the full filename for new/ entries so the kept id leaked
    /// the suffix, and (b) the maildir crate's id-keyed `delete`
    /// couldn't find the canonical id in new/ -- duplicates would be
    /// detected but deletion would silently fail.
    #[test]
    fn apply_dedupe_handles_suffix_bearing_new_files() {
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
        let plan = plan_dedupe(tmp.path(), &folders).unwrap();

        // plan.deletions classified the new/ file as the duplicate; the
        // canonical id surfaces (not the suffix-bearing form).
        assert_eq!(plan.deletions.len(), 1);
        assert_eq!(plan.deletions[0].folder, "INBOX");
        assert_eq!(plan.deletions[0].maildir_id.as_ref(), dup_unique);
        assert_eq!(plan.deletions[0].kept_maildir_id.as_ref(), kept_unique);

        apply_dedupe(tmp.path(), &plan).unwrap();

        // Duplicate file is gone from disk.
        assert!(
            !inbox_path.join("new").join(&dup_filename).exists(),
            "duplicate new/ file must have been removed"
        );
        assert!(
            inbox_path.join("cur").join(&kept_filename).exists(),
            "kept cur/ file must still exist"
        );

        // The kept tuple in plan.kept carries the canonical id.
        let index = index_from_kept(&plan);
        let entries = index
            .by_message_id
            .get(&MessageId::from("a@x"))
            .expect("kept file should be indexed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].maildir_id.as_ref(), kept_unique);
    }

    /// Without calling apply_dedupe, plan_dedupe must not touch disk.
    /// Pins the load-bearing property the dry-run path depends on: a
    /// `jma sync --dry-run` that calls plan_dedupe + reads plan.deletions
    /// for its printout must leave the duplicate file in place.
    #[test]
    fn plan_dedupe_does_not_touch_disk() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";
        let kept_unique = "1700000000.M1.host";
        let dup_unique = "1700000005.M2.host";
        let kept_filename = format!("{kept_unique}:2,S");
        let dup_filename = format!("{dup_unique}:2,S");
        fs::write(inbox_path.join("cur").join(&kept_filename), body).unwrap();
        fs::write(inbox_path.join("cur").join(&dup_filename), body).unwrap();

        let now = SystemTime::now();
        let kept_mtime = now - std::time::Duration::from_secs(60);
        fs::File::open(inbox_path.join("cur").join(&kept_filename))
            .and_then(|f| f.set_modified(kept_mtime))
            .unwrap();
        fs::File::open(inbox_path.join("cur").join(&dup_filename))
            .and_then(|f| f.set_modified(now))
            .unwrap();

        let folders = vec!["INBOX".to_string()];
        let plan = plan_dedupe(tmp.path(), &folders).unwrap();

        assert_eq!(plan.deletions.len(), 1);
        assert_eq!(plan.deletions[0].maildir_id.as_ref(), dup_unique);

        // Both files still on disk: plan_dedupe is pure.
        assert!(
            inbox_path.join("cur").join(&kept_filename).exists(),
            "kept file must still exist after plan_dedupe alone"
        );
        assert!(
            inbox_path.join("cur").join(&dup_filename).exists(),
            "duplicate file must still exist after plan_dedupe alone -- \
             dry-run depends on this"
        );
    }

    /// Parallel per-folder fan-out must merge results so that
    /// (a) Message-IDs unique to each folder both surface to the
    /// caller, and (b) the same Message-ID present in two different
    /// folders is preserved as two distinct KeptEntry rows -- the
    /// "user copied the message into another mailbox" case dedupe
    /// explicitly does not flatten. Pin both invariants here so a
    /// regression in the merge step (wrong indexing, lost partial
    /// results, cross-folder collapse) trips a test.
    #[test]
    fn plan_dedupe_walks_multiple_folders() {
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
        let plan = plan_dedupe(tmp.path(), &folders).unwrap();
        let index = index_from_kept(&plan);

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

        assert!(
            plan.deletions.is_empty(),
            "no per-folder duplicates -> no deletions, got {:?}",
            plan.deletions
        );
    }
}
