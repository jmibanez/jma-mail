//! Dedupe janitorial task: identify per-folder Message-ID
//! duplicates and (unless `dry_run`) remove younger byte-identical copies.
//! Thin orchestration over `sync::dedupe::plan_dedupe` +
//! `sync::dedupe::apply_dedupe`; the heavy lifting (Message-ID
//! parsing, mtime-based survivor selection, MUA-promotion
//! exemption) still lives there.
//!
//! The split exists so the sync engine's Phase 0 and the `jma
//! janitor dedupe` CLI hit the same code path. Engine callers
//! pass the dirty folder list computed against
//! `folder_checkpoint`; CLI callers pass every folder the DB
//! knows about. The function itself is folder-list-driven and
//! doesn't know which caller asked.

use anyhow::Result;
use std::collections::HashSet;
use std::path::Path;
use tracing::warn;

use crate::ids::MaildirId;
use crate::sync::dedupe::{self, DedupePlan};

/// Run the dedupe task across `folders`. An empty slice is a
/// no-op that returns `DedupePlan::default()` -- this is the
/// path the engine takes on `ScanScope::Paths` cycles and on
/// Full cycles where `folder_checkpoint` says nothing has
/// changed. Non-empty walks the folders, classifies duplicates,
/// and applies the deletions unless `dry_run` suppresses them.
///
/// Returns the plan so callers can render it (dry-run preview,
/// CLI summary) or feed it forward (the engine's `LocalIndex`
/// bridge for post-recovery sync).
pub fn run(
    maildir_root: &Path,
    folders: &[String],
    dry_run: bool,
    tracked_maildir_ids: &HashSet<MaildirId>,
) -> Result<DedupePlan> {
    if folders.is_empty() {
        return Ok(DedupePlan::default());
    }
    let _phase = tracing::info_span!(target: crate::profile::TARGET_PHASE, "dedupe").entered();
    let mut plan = dedupe::plan_dedupe(maildir_root, folders)?;

    // A tracked filename is not merely a redundant pathname: it is the
    // local half of a persisted JMAP Email binding. Deleting it here
    // without atomically transferring that binding to the kept filename
    // makes the normal scan report a user deletion. Under server-wins
    // that immediately re-downloads the file; under local-wins it can
    // destroy the server object. This exact shape occurs after a partial
    // bootstrap: downloads commit per message, then a later upload error
    // prevents the mailbox/cursor checkpoint from publishing. Refuse the
    // automatic deletion and leave both byte-identical paths in place.
    // Reconcile can bind/ignore the untracked copy without manufacturing
    // a false local-delete event.
    plan.deletions.retain(|deletion| {
        if tracked_maildir_ids.contains(&deletion.maildir_id) {
            warn!(
                "Skipping automatic dedupe of tracked file {}/{} (Message-ID {}): \
                 deleting a message_map/local_state anchor would turn recovery \
                 into a false local deletion and a re-download loop",
                deletion.folder, deletion.maildir_id, deletion.message_id
            );
            false
        } else {
            true
        }
    });
    if !dry_run {
        dedupe::apply_dedupe(maildir_root, &plan)?;
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::MaildirId;
    use std::collections::HashSet;
    use std::fs;
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile::tempdir;

    /// Build a maildir folder under `root` and seed `cur/` with one
    /// file carrying the given Message-ID header. Returns the file
    /// path so callers can grow per-message duplicate sets.
    fn seed_message(
        root: &Path,
        folder: &str,
        filename: &str,
        message_id: &str,
    ) -> std::path::PathBuf {
        let folder_path = root.join(folder);
        let cur = folder_path.join("cur");
        fs::create_dir_all(&cur).unwrap();
        fs::create_dir_all(folder_path.join("new")).unwrap();
        fs::create_dir_all(folder_path.join("tmp")).unwrap();
        let path = cur.join(filename);
        fs::write(
            &path,
            format!("Message-ID: {message_id}\r\nSubject: t\r\n\r\nbody"),
        )
        .unwrap();
        path
    }

    /// Empty folder list is the engine's `ScanScope::Paths` /
    /// clean-checkpoint path. The duplicates on disk must remain
    /// untouched and the plan must come back default.
    #[test]
    fn empty_folder_list_is_a_no_op() {
        let dir = tempdir().unwrap();
        let a = seed_message(dir.path(), "INBOX", "1.host:2,", "<a@x>");
        sleep(Duration::from_millis(20));
        let b = seed_message(dir.path(), "INBOX", "2.host:2,", "<a@x>");

        let plan = run(dir.path(), &[], false, &HashSet::new()).unwrap();

        assert!(plan.kept.is_empty(), "default plan has no kept entries");
        assert!(plan.deletions.is_empty(), "default plan has no deletions");
        assert!(a.exists(), "older duplicate must remain on disk");
        assert!(b.exists(), "younger duplicate must remain on disk");
    }

    /// Non-empty folder list with a duplicate pair walks the
    /// folder, picks the older mtime as kept, and removes the
    /// younger file from disk.
    #[test]
    fn non_empty_folder_list_walks_and_deletes_duplicates() {
        let dir = tempdir().unwrap();
        let a = seed_message(dir.path(), "INBOX", "1.host:2,", "<a@x>");
        sleep(Duration::from_millis(20));
        let b = seed_message(dir.path(), "INBOX", "2.host:2,", "<a@x>");

        let plan = run(dir.path(), &["INBOX".to_string()], false, &HashSet::new()).unwrap();

        assert_eq!(plan.kept.len(), 1);
        assert_eq!(plan.deletions.len(), 1);
        assert!(a.exists(), "older mtime wins -- must remain on disk");
        assert!(
            !b.exists(),
            "younger duplicate must be removed on non-dry-run"
        );
    }

    /// Under dry-run the plan still classifies the duplicate but
    /// `apply_dedupe` is suppressed -- both files remain on disk
    /// so the caller can preview the action without committing.
    #[test]
    fn dry_run_returns_plan_without_deleting() {
        let dir = tempdir().unwrap();
        let a = seed_message(dir.path(), "INBOX", "1.host:2,", "<a@x>");
        sleep(Duration::from_millis(20));
        let b = seed_message(dir.path(), "INBOX", "2.host:2,", "<a@x>");

        let plan = run(dir.path(), &["INBOX".to_string()], true, &HashSet::new()).unwrap();

        assert_eq!(plan.deletions.len(), 1);
        assert!(a.exists());
        assert!(b.exists(), "dry-run must not touch disk");
    }

    #[test]
    fn tracked_duplicate_is_never_deleted() {
        let dir = tempdir().unwrap();
        let a = seed_message(dir.path(), "INBOX", "1.host:2,", "<a@x>");
        sleep(Duration::from_millis(20));
        let b = seed_message(dir.path(), "INBOX", "2.host:2,", "<a@x>");
        let protected = HashSet::from([MaildirId::from("2.host")]);

        let plan = run(dir.path(), &["INBOX".to_string()], false, &protected).unwrap();

        assert!(plan.deletions.is_empty());
        assert!(a.exists());
        assert!(
            b.exists(),
            "a DB-tracked filename must survive automatic dedupe"
        );
    }
}
