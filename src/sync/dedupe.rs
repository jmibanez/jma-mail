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

/// Which maildir subdirectory a candidate lives in. Tracked so the
/// planner can refuse to delete duplicates that span `cur/` and `new/`
/// within a single group -- such a pair represents an in-flight MUA
/// copy+unlink promotion, where deleting either side races the MUA's
/// follow-up unlink and risks leaving the user with zero copies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Subdir {
    Cur,
    New,
}

/// One file that might be the kept copy for a `GroupKey`. Built up
/// during the maildir walk; the oldest mtime per group wins.
struct Candidate {
    mtime: SystemTime,
    maildir_id: MaildirId,
    subdir: Subdir,
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
/// file).
///
/// Two scope exemptions, both load-bearing:
/// 1. Cross-folder copies of the same Message-ID are preserved -- a
///    user copying a message into another mailbox is a distinct
///    instance, not a duplicate to clean up.
/// 2. Within a folder, a group whose candidates span `cur/` and `new/`
///    where one side's qmail unique id is an extending-prefix of the
///    other's gets a kept entry but no deletions. That pair almost
///    always means an MUA `rename(2)` promotion landed between our
///    `cur/` and `new/` readdirs (every realistic MUA preserves the
///    unique across promotion; mbsync additionally appends
///    `,U=<imap-uid>` to the unique, hence "extending-prefix" rather
///    than strict equality). Deleting either side risks losing the
///    file if the MUA's bookkeeping still expects both names to
///    resolve. Once the promotion's caller settles, the pair
///    collapses to in-subdir and gets cleaned next cycle. Cross-subdir
///    pairs whose ids are not in a prefix relationship are real
///    duplicates (e.g., jma re-delivered into `new/` while `cur/`
///    already held a separate copy) and get the normal mtime-based
///    dedupe.
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

        // A cross-subdir pair where one candidate's id is an
        // extending-prefix of the other's is almost certainly
        // a `rename(2)` promotion we observed across two
        // separate readdirs (one of cur/, one of new/). Every
        // realistic MUA that promotes new/ -> cur/ uses
        // rename(2) and preserves the qmail unique portion of
        // the filename. mbsync additionally appends `,U=<uid>`
        // to the unique when it learns the IMAP UID, so the
        // shorter id (new/ side) is followed by `,` in the
        // longer id (cur/ side) for that case. A pair without
        // an extending-prefix relationship is a real duplicate
        // (jma re-delivered the message while a different copy
        // already lived in the other subdir) and gets normal
        // mtime-based dedupe.
        //
        // Skip deletions for the whole group when any such
        // prefix-paired cross-subdir pair exists -- the kept
        // entry already emitted above stays, so LocalIndex
        // resolves correctly for adoption. Once the MUA
        // finishes the promotion the pair collapses to
        // in-subdir and gets cleaned next cycle.
        let all: Vec<Candidate> = std::iter::once(keep).chain(iter).collect();
        if has_in_flight_promotion(&all) {
            continue;
        }

        let (keep, dups) = all.split_first().expect("group has at least one candidate");
        for dup in dups {
            plan.deletions.push(DedupeDeletion {
                folder: folder.clone(),
                message_id: msgid.clone(),
                maildir_id: dup.maildir_id.clone(),
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

/// True iff one of `a`/`b` is the `,`-extending prefix of the other
/// (or they're equal). Used to recognize an MUA `rename(2)` promotion
/// we observed across two readdirs as a single in-flight file rather
/// than two distinct duplicates. Equality covers the pure-rename case
/// (mutt, neomutt, notmuch, mu, Gnus, Dovecot, offlineimap); the
/// extension-with-comma case covers mbsync's `,U=<imap-uid>` infix
/// that's appended to the unique during promotion.
///
/// The comma boundary is load-bearing: a bare `starts_with` would
/// false-positive on two unrelated uniques that happen to share a
/// string prefix (e.g., a hostname `host` vs `hostnew`).
fn is_extending_prefix(a: &str, b: &str) -> bool {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if short == long {
        return true;
    }
    long.starts_with(short) && long.as_bytes().get(short.len()) == Some(&b',')
}

/// True iff the group contains at least one cross-subdir pair where
/// the qmail unique ids are in an extending-prefix relationship --
/// the signature of an MUA promotion that landed between our two
/// readdirs. Quadratic over candidates within a single
/// (folder, Message-ID) group; group sizes are typically 1-3 in
/// real use.
fn has_in_flight_promotion(candidates: &[Candidate]) -> bool {
    for (i, c1) in candidates.iter().enumerate() {
        for c2 in &candidates[i + 1..] {
            if c1.subdir != c2.subdir
                && is_extending_prefix(c1.maildir_id.as_ref(), c2.maildir_id.as_ref())
            {
                return true;
            }
        }
    }
    false
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
    for entry in md.list_cur() {
        push_candidate(entry?, Subdir::Cur, &mut out)?;
    }
    for entry in md.list_new() {
        push_candidate(entry?, Subdir::New, &mut out)?;
    }
    Ok(out)
}

/// Shared cur/new entry classification, factored out so the two walk
/// loops in `scan_folder` stay parallel. Returns Ok(()) for both
/// "added a candidate" and "skipped (no Message-ID header)" -- the
/// caller treats both as non-failures.
fn push_candidate(
    entry: maildir::MailEntry,
    subdir: Subdir,
    out: &mut Vec<(MessageId, Candidate)>,
) -> Result<()> {
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
            return Ok(());
        }
    };

    let mtime = std::fs::metadata(&path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);

    out.push((
        msgid,
        Candidate {
            mtime,
            maildir_id,
            subdir,
        },
    ));
    Ok(())
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
    /// correctly when paired with another new/ duplicate: the canonical
    /// id (sans `:2,<flags>`) surfaces in `plan.kept`, and the
    /// suffix-bearing duplicate gets deleted on disk by apply_dedupe.
    /// Pre-fix this regressed because (a) `entry.id()` returns the full
    /// filename for new/ entries so the kept id leaked the suffix, and
    /// (b) the maildir crate's id-keyed `delete` couldn't find the
    /// canonical id in new/ -- duplicates would be detected but
    /// deletion would silently fail. The case is exercised on new/+new/
    /// rather than cur/+new/ because the cross-subdir pair is now
    /// exempt from deletion (see the exempts-cur-new test below); the
    /// in-subdir variant is what carries the regression-guard load.
    #[test]
    fn apply_dedupe_handles_suffix_bearing_new_new_duplicate() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";

        // Older bare-id new/ file (the legitimate one), newer duplicate
        // in new/ with a `:2,F` suffix (the shape jma writes on
        // delivery with non-Seen flag preservation).
        let kept_unique = "1700000000.M1.host";
        let dup_unique = "1700000005.M2.host";
        let dup_filename = format!("{dup_unique}:2,F");
        fs::write(inbox_path.join("new").join(kept_unique), body).unwrap();
        fs::write(inbox_path.join("new").join(&dup_filename), body).unwrap();

        // Pin both sides of the ordering relation explicitly so the
        // test doesn't rely on ambient mtime granularity (and survives
        // a future edit that removes one of the two adjustments).
        let now = SystemTime::now();
        let kept_mtime = now - std::time::Duration::from_secs(60);
        fs::File::open(inbox_path.join("new").join(kept_unique))
            .and_then(|f| f.set_modified(kept_mtime))
            .unwrap();
        fs::File::open(inbox_path.join("new").join(&dup_filename))
            .and_then(|f| f.set_modified(now))
            .unwrap();

        let folders = vec!["INBOX".to_string()];
        let plan = plan_dedupe(tmp.path(), &folders).unwrap();

        // plan.deletions classified the suffix-bearing new/ file as the
        // duplicate; the canonical id surfaces (not the suffix-bearing
        // form).
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
            inbox_path.join("new").join(kept_unique).exists(),
            "kept new/ file must still exist"
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

    /// Same unique id in cur/ and new/ -- the canonical signature of
    /// an MUA `rename(2)` promotion (mutt, neomutt, notmuch, mu, Gnus,
    /// Dovecot, offlineimap all preserve the unique). plan.deletions
    /// must be empty so apply_dedupe is a no-op for the pair, but
    /// plan.kept still emits a single entry so LocalIndex resolves
    /// the Message-ID for adoption. Pins the Low #7 fix: deleting
    /// either side risks losing the file if the MUA's bookkeeping
    /// still expects both names to resolve.
    #[test]
    fn plan_dedupe_exempts_cur_new_same_unique_pair() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";
        let unique = "1700000000.M1.host";
        // cur/ side carries the `:2,<flags>` infostring an MUA
        // appends on promotion; new/ side keeps the bare unique.
        let cur_filename = format!("{unique}:2,S");
        fs::write(inbox_path.join("cur").join(&cur_filename), body).unwrap();
        fs::write(inbox_path.join("new").join(unique), body).unwrap();

        // Pin mtimes so the kept side is deterministic.
        let now = SystemTime::now();
        let cur_mtime = now - std::time::Duration::from_secs(60);
        fs::File::open(inbox_path.join("cur").join(&cur_filename))
            .and_then(|f| f.set_modified(cur_mtime))
            .unwrap();
        fs::File::open(inbox_path.join("new").join(unique))
            .and_then(|f| f.set_modified(now))
            .unwrap();

        let folders = vec!["INBOX".to_string()];
        let plan = plan_dedupe(tmp.path(), &folders).unwrap();

        assert!(
            plan.deletions.is_empty(),
            "in-flight rename pair must not produce deletions; got {:?}",
            plan.deletions
        );
        assert_eq!(
            plan.kept.len(),
            1,
            "exactly one kept entry per (folder, Message-ID) group"
        );
        // Both sides share the unique; the kept entry's id is that
        // shared value regardless of which subdir won the mtime
        // contest.
        assert_eq!(plan.kept[0].maildir_id.as_ref(), unique);

        apply_dedupe(tmp.path(), &plan).unwrap();

        assert!(
            inbox_path.join("cur").join(&cur_filename).exists(),
            "cur/ side of the in-flight pair must survive apply_dedupe"
        );
        assert!(
            inbox_path.join("new").join(unique).exists(),
            "new/ side of the in-flight pair must survive apply_dedupe"
        );
    }

    /// mbsync promotion appends `,U=<imap-uid>` to the qmail unique
    /// during the `new/` -> `cur/` rename. So when we observe the
    /// pair mid-readdir, the new/ id is the bare unique and the cur/
    /// id is the unique-plus-`,U=42`. The exemption must still fire
    /// (extending-prefix with `,` boundary), because the file is one
    /// physical file the rename has already committed atomically; we
    /// just observed it from both directories before mbsync's caller
    /// reconciled state.
    #[test]
    fn plan_dedupe_exempts_mbsync_uid_extension_pair() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";
        let base = "1700000000.M1.host";
        // cur/ side has mbsync's `,U=42` infix between the base and
        // the `:2,<flags>` infostring.
        let cur_filename = format!("{base},U=42:2,FS");
        fs::write(inbox_path.join("cur").join(&cur_filename), body).unwrap();
        fs::write(inbox_path.join("new").join(base), body).unwrap();

        let now = SystemTime::now();
        let cur_mtime = now - std::time::Duration::from_secs(60);
        fs::File::open(inbox_path.join("cur").join(&cur_filename))
            .and_then(|f| f.set_modified(cur_mtime))
            .unwrap();
        fs::File::open(inbox_path.join("new").join(base))
            .and_then(|f| f.set_modified(now))
            .unwrap();

        let folders = vec!["INBOX".to_string()];
        let plan = plan_dedupe(tmp.path(), &folders).unwrap();

        assert!(
            plan.deletions.is_empty(),
            "mbsync `,U=` extension must still trigger the exemption; got {:?}",
            plan.deletions
        );
        assert_eq!(plan.kept.len(), 1);
    }

    /// Cross-subdir pair whose ids are unrelated (different qmail
    /// uniques) is *not* an in-flight rename -- it's a real duplicate
    /// (e.g., jma re-delivered the message while a different copy
    /// already lived in the other subdir). The narrowed exemption
    /// must let normal mtime-based dedupe run for this case;
    /// otherwise we'd keep both copies indefinitely waiting for a
    /// promotion that's never coming.
    #[test]
    fn plan_dedupe_deletes_cur_new_distinct_uniques() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";
        let kept_unique = "1700000000.M1.host";
        let dup_unique = "1700000005.M2.host";
        let cur_filename = format!("{kept_unique}:2,S");
        let new_filename = dup_unique;
        fs::write(inbox_path.join("cur").join(&cur_filename), body).unwrap();
        fs::write(inbox_path.join("new").join(new_filename), body).unwrap();

        // cur/ side is older -- the kept side under the normal
        // mtime rule.
        let now = SystemTime::now();
        let cur_mtime = now - std::time::Duration::from_secs(60);
        fs::File::open(inbox_path.join("cur").join(&cur_filename))
            .and_then(|f| f.set_modified(cur_mtime))
            .unwrap();
        fs::File::open(inbox_path.join("new").join(new_filename))
            .and_then(|f| f.set_modified(now))
            .unwrap();

        let folders = vec!["INBOX".to_string()];
        let plan = plan_dedupe(tmp.path(), &folders).unwrap();

        assert_eq!(plan.deletions.len(), 1);
        assert_eq!(plan.deletions[0].maildir_id.as_ref(), dup_unique);
        assert_eq!(plan.deletions[0].kept_maildir_id.as_ref(), kept_unique);

        apply_dedupe(tmp.path(), &plan).unwrap();

        assert!(
            inbox_path.join("cur").join(&cur_filename).exists(),
            "kept cur/ side must remain after apply_dedupe"
        );
        assert!(
            !inbox_path.join("new").join(new_filename).exists(),
            "duplicate new/ side must be removed -- distinct uniques are not exempt"
        );
    }

    /// Pin the boundary semantics of `is_extending_prefix`:
    /// - equal strings are an extending prefix of each other (the
    ///   pure-rename case)
    /// - a string followed by `,<extension>` is extended-by the bare
    ///   prefix (mbsync's `,U=42` case)
    /// - a string that's a bare lexical prefix without the `,`
    ///   boundary is NOT an extending prefix (defends against
    ///   coincidental string overlap between unrelated uniques like
    ///   `host` and `hostnew`)
    #[test]
    fn is_extending_prefix_boundary_cases() {
        assert!(is_extending_prefix("X", "X"));
        assert!(is_extending_prefix("1700.M1.host", "1700.M1.host"));
        assert!(is_extending_prefix("X", "X,U=42"));
        assert!(is_extending_prefix("X,U=42", "X"));
        assert!(is_extending_prefix(
            "1700.M1.host",
            "1700.M1.host,U=12345,W=42"
        ));
        // No comma boundary -- two distinct uniques that happen to
        // share a string prefix.
        assert!(!is_extending_prefix("host", "hostnew"));
        assert!(!is_extending_prefix("1700.M1.host", "1700.M1.hostextra"));
        // Different roots altogether.
        assert!(!is_extending_prefix("1700.M1.host", "1700.M2.host"));
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
