//! Maildir-vs-DB drift: compare the `mailbox_map` folder set against
//! the maildir tree on disk.
//!
//! `compute_drift` is a pure read over the state DB and the
//! filesystem -- no network, no mutation -- returning a structured
//! result rather than printing. `cmd_status` renders it for the
//! human-facing drift report. Keeping the computation here, separate
//! from the status command's presentation, lets any caller reason
//! about drift from one definition instead of re-deriving it.

use anyhow::Result;
use rusqlite::Connection;
use std::collections::BTreeSet;
use std::path::Path;

use crate::maildir_ops::namespace::is_jma_private;
use crate::state::queries;

/// Outcome of comparing the cached `mailbox_map` folder set against
/// the on-disk maildir tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftReport {
    /// The maildir root directory does not exist -- there is nothing
    /// to compare against.
    MaildirRootMissing,
    /// `mailbox_map` has no rows yet, so the DB has no record of any
    /// synced folder to compare against disk.
    NoMailboxMap,
    /// Neither degenerate precondition applies (root exists and the
    /// mailbox map is non-empty), so the two sides were compared.
    /// Either vector may be empty; two empty vectors means the maildir
    /// tree and the DB agree.
    Drift {
        /// Folders found on disk that have no `mailbox_map` row.
        only_disk: Vec<String>,
        /// Folders recorded in `mailbox_map` whose maildir is absent
        /// from disk.
        only_db: Vec<String>,
    },
}

/// Compare the folders recorded in `mailbox_map` against the maildir
/// directories under `maildir_root`. When both degenerate conditions
/// hold, `MaildirRootMissing` takes priority over `NoMailboxMap`.
pub fn compute_drift(conn: &Connection, maildir_root: &Path) -> Result<DriftReport> {
    let known: BTreeSet<String> = queries::list_known_maildir_folders(conn)?
        .into_iter()
        .collect();

    if !maildir_root.exists() {
        return Ok(DriftReport::MaildirRootMissing);
    }
    if known.is_empty() {
        return Ok(DriftReport::NoMailboxMap);
    }

    // Walk recursively to find every directory that looks like a
    // maildir (has `cur/` underneath). This is the only shape that
    // works across all three folder layouts:
    //   - Flat:      <root>/foo.bar/cur                (depth 1)
    //   - MaildirPP: <root>/.foo.bar/cur               (depth 1, leading dot)
    //   - Fs:        <root>/foo/bar/cur                (depth N>=1)
    let on_disk = find_maildir_folders(maildir_root);

    let only_disk: Vec<String> = on_disk.difference(&known).cloned().collect();
    // For "in DB not on disk", trust the per-folder existence check
    // rather than set-difference: it's layout-independent (a stored
    // `maildir_folder` of `parent/child` joins onto the root with the
    // FS separator, regardless of whether the *layout* uses `/` or
    // `.`) and avoids being fooled by a recursive walk that missed
    // something.
    let only_db: Vec<String> = known
        .iter()
        .filter(|f| !maildir_root.join(f).join("cur").is_dir())
        .cloned()
        .collect();

    Ok(DriftReport::Drift { only_disk, only_db })
}

/// Return the relative path of every maildir-shaped folder under
/// `root` -- a directory containing a `cur/` subdirectory, the marker
/// `ensure_maildir` creates, recognized under any folder layout.
/// jma's own private namespace (see
/// `maildir_ops::namespace::is_jma_private`) is excluded. Best
/// effort: unreadable directories are silently skipped, since a
/// partial drift report is more useful than none.
pub fn find_maildir_folders(root: &Path) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    walk_for_maildirs(root, root, &mut found);
    found
}

fn walk_for_maildirs(root: &Path, dir: &Path, found: &mut BTreeSet<String>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return,
    };
    for entry in entries.filter_map(|e| e.ok()) {
        // `DirEntry::file_type()` doesn't traverse symlinks, so a
        // symlink pointing at a directory has `is_dir() == false` here
        // and gets filtered out before recursion -- no symlink loops.
        // Side effect: a real maildir tree behind a symlink at the
        // root won't show up in the drift report, which is acceptable
        // for a diagnostic.
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if is_jma_private(&name_str) {
            continue;
        }
        if name_str == "cur" || name_str == "new" || name_str == "tmp" {
            continue;
        }
        let path = entry.path();
        if path.join("cur").is_dir()
            && let Ok(rel) = path.strip_prefix(root)
        {
            found.insert(rel.to_string_lossy().into_owned());
        }
        walk_for_maildirs(root, &path, found);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a fake maildir at `<root>/<folder>` with the cur/new/tmp
    /// triplet that `find_maildir_folders` keys on.
    fn touch_maildir(root: &Path, folder: &str) {
        let path = root.join(folder);
        for sub in ["cur", "new", "tmp"] {
            std::fs::create_dir_all(path.join(sub)).unwrap();
        }
    }

    #[test]
    fn finds_flat_layout_folders() {
        let dir = tempfile::tempdir().unwrap();
        touch_maildir(dir.path(), "INBOX");
        touch_maildir(dir.path(), "Archive");
        touch_maildir(dir.path(), "[Airmail].Sent");

        let found = find_maildir_folders(dir.path());

        let expected: BTreeSet<String> = ["INBOX", "Archive", "[Airmail].Sent"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(found, expected);
    }

    /// Pins the bug fix: the prior `starts_with('.')` filter dropped
    /// every Maildir++ folder. The recursive walk lets dotted folders
    /// through while still skipping our own `.jma.*` markers.
    #[test]
    fn finds_maildir_pp_layout_folders() {
        let dir = tempfile::tempdir().unwrap();
        touch_maildir(dir.path(), ".INBOX");
        touch_maildir(dir.path(), ".Archive");
        touch_maildir(dir.path(), ".[Airmail].Sent");

        let found = find_maildir_folders(dir.path());

        let expected: BTreeSet<String> = [".INBOX", ".Archive", ".[Airmail].Sent"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(found, expected);
    }

    /// Pins the bug fix: the prior top-level-only walk missed every
    /// nested Fs mailbox at depth >= 2. The recursive walk records
    /// each maildir at whatever depth it lives.
    #[test]
    fn finds_fs_layout_nested_folders() {
        let dir = tempfile::tempdir().unwrap();
        touch_maildir(dir.path(), "INBOX");
        touch_maildir(dir.path(), "[Airmail]");
        touch_maildir(dir.path(), "[Airmail]/Sent");
        touch_maildir(dir.path(), "[Airmail]/Drafts");

        let found = find_maildir_folders(dir.path());

        let expected: BTreeSet<String> =
            ["INBOX", "[Airmail]", "[Airmail]/Sent", "[Airmail]/Drafts"]
                .into_iter()
                .map(String::from)
                .collect();
        assert_eq!(found, expected);
    }

    /// `.jma.db`, `.jma.lock`, etc. live at the maildir root
    /// alongside synced folders. They must not be reported as drift.
    #[test]
    fn skips_jma_state_markers() {
        let dir = tempfile::tempdir().unwrap();
        touch_maildir(dir.path(), "INBOX");
        // Mimic the on-disk shape of jma's own state files.
        std::fs::File::create(dir.path().join(".jma.db")).unwrap();
        std::fs::File::create(dir.path().join(".jma.lock")).unwrap();
        // Also a `.jma.foo` directory, just to confirm the filter
        // matches by prefix not by extension.
        std::fs::create_dir_all(dir.path().join(".jma.cache").join("cur")).unwrap();

        let found = find_maildir_folders(dir.path());

        let expected: BTreeSet<String> = ["INBOX"].into_iter().map(String::from).collect();
        assert_eq!(found, expected);
    }

    /// A directory without `cur/` is just a regular directory, not a
    /// maildir. Don't claim it.
    #[test]
    fn ignores_directories_without_cur() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("notes")).unwrap();
        std::fs::create_dir_all(dir.path().join("staging")).unwrap();
        touch_maildir(dir.path(), "INBOX");

        let found = find_maildir_folders(dir.path());

        let expected: BTreeSet<String> = ["INBOX"].into_iter().map(String::from).collect();
        assert_eq!(found, expected);
    }
}
