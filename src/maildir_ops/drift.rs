//! Maildir-vs-DB drift: classify the discrepancies between the
//! `mailbox_map` rows and the maildir tree on disk.
//!
//! `compute_drift` is a pure read over the state DB and the
//! filesystem -- no network, no mutation -- returning a structured
//! classification rather than printing, so any caller can reason
//! about drift from one definition instead of re-deriving it.

use anyhow::Result;
use rusqlite::Connection;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use tracing::warn;

use crate::ids::JmapMailboxId;
use crate::jmap::mailbox::{MailboxSelectionInput, get_selected_mailboxes};
use crate::maildir_ops::namespace::is_jma_private;
use crate::maildir_ops::sentinel;
use crate::state::queries::{self, MailboxRecord};

/// Outcome of comparing the cached `mailbox_map` rows against the
/// on-disk maildir tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftReport {
    /// The maildir root directory does not exist -- there is nothing
    /// to compare against.
    MaildirRootMissing,
    /// `mailbox_map` has no rows yet, so the DB has no record of any
    /// synced folder to compare against disk.
    NoMailboxMap,
    /// The two sides were compared. Every field of `DriftClasses` may
    /// be empty; all empty means the maildir tree and the DB agree and
    /// nothing has dropped out of the sync set.
    Drift(DriftClasses),
}

/// The classified discrepancies between `mailbox_map` and the maildir
/// tree.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DriftClasses {
    /// Folders on disk with no `mailbox_map` row, not part of a
    /// pending local rename -- untracked strays.
    pub disk_only: Vec<String>,
    /// `mailbox_map` rows whose maildir is absent from disk, not part
    /// of a pending local rename -- orphaned cache rows.
    pub db_only: Vec<String>,
    /// Folders present on both sides whose mailbox is no longer
    /// selected by `[sync].mailboxes`. Always empty when the config
    /// syncs every mailbox (empty list).
    pub config_dropped: Vec<String>,
    /// Local folder renames not yet pushed to the server: the maildir
    /// moved from `db_folder` (where `mailbox_map` still points) to
    /// `disk_folder` (where the `.jma.mapping` sentinel now lives),
    /// both bound to the same mailbox id.
    pub rename_in_flight: Vec<RenameInFlight>,
}

/// A local rename detected from disk: `mailbox_map` still names
/// `db_folder`, but the sentinel for that mailbox id now lives under
/// `disk_folder`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameInFlight {
    pub db_folder: String,
    pub disk_folder: String,
}

/// Compare the `mailbox_map` rows against the maildir directories under
/// `maildir_root`, classifying each discrepancy. `mailboxes` /
/// `case_insensitive` are the `[sync]` selection, used to flag folders
/// that have dropped out of the sync set. When both degenerate
/// conditions hold, `MaildirRootMissing` takes priority over
/// `NoMailboxMap`.
pub fn compute_drift(
    conn: &Connection,
    maildir_root: &Path,
    mailboxes: &[String],
    case_insensitive: bool,
) -> Result<DriftReport> {
    if !maildir_root.exists() {
        return Ok(DriftReport::MaildirRootMissing);
    }
    let rows = queries::get_all_mailboxes(conn)?;
    if rows.is_empty() {
        return Ok(DriftReport::NoMailboxMap);
    }

    let on_disk = find_maildir_folders(maildir_root);
    let known: BTreeSet<String> = rows.iter().map(|r| r.maildir_folder.clone()).collect();

    // A row's maildir is present when its `<folder>/cur` exists. This
    // per-folder check is layout-independent (the stored
    // `maildir_folder` joins onto the root with the FS separator) and
    // isn't fooled by a recursive walk that missed something.
    let present = |folder: &str| maildir_root.join(folder).join("cur").is_dir();

    // Raw discrepancies, before pulling out rename pairs.
    let disk_orphans: Vec<String> = on_disk.difference(&known).cloned().collect();
    let db_orphans: Vec<&MailboxRecord> = rows
        .iter()
        .filter(|r| !present(&r.maildir_folder))
        .collect();

    // A local rename not yet pushed shows up as a disk orphan (the new
    // path) paired with a db orphan (the old path) whose `mailbox_map`
    // id matches the new path's `.jma.mapping` sentinel.
    let db_orphan_by_id: HashMap<JmapMailboxId, &MailboxRecord> = db_orphans
        .iter()
        .map(|r| (r.jmap_mailbox_id.clone(), *r))
        .collect();
    let mut rename_in_flight = Vec::new();
    let mut paired_disk: HashSet<String> = HashSet::new();
    let mut paired_db_ids: HashSet<JmapMailboxId> = HashSet::new();
    for disk_folder in &disk_orphans {
        // Best-effort: an unreadable sentinel (permission denied, EIO)
        // can't tell us whether this disk orphan is a rename target, so
        // treat it like a missing sentinel -- leave the folder unpaired
        // and let it fall through to `disk_only` rather than aborting
        // the whole drift report over one stray folder.
        let mapping = match sentinel::read(&maildir_root.join(disk_folder)) {
            Ok(Some(m)) => m,
            Ok(None) => continue,
            Err(e) => {
                warn!(
                    "Failed to read sentinel under {disk_folder}; treating as an unpaired stray for drift ({e:#})"
                );
                continue;
            }
        };
        if paired_db_ids.contains(&mapping.jmap_mailbox_id) {
            continue;
        }
        if let Some(db_row) = db_orphan_by_id.get(&mapping.jmap_mailbox_id) {
            rename_in_flight.push(RenameInFlight {
                db_folder: db_row.maildir_folder.clone(),
                disk_folder: disk_folder.clone(),
            });
            paired_disk.insert(disk_folder.clone());
            paired_db_ids.insert(mapping.jmap_mailbox_id.clone());
        }
    }

    let disk_only: Vec<String> = disk_orphans
        .into_iter()
        .filter(|f| !paired_disk.contains(f))
        .collect();
    let db_only: Vec<String> = db_orphans
        .iter()
        .filter(|r| !paired_db_ids.contains(&r.jmap_mailbox_id))
        .map(|r| r.maildir_folder.clone())
        .collect();

    // Folders present on both sides but no longer selected by the sync
    // config, computed with the same path-based selection the engine
    // uses (so status can't disagree with what sync would sync).
    // Skipped when the config syncs everything; rows with no cached
    // remote_path can't be tested, so they're left unflagged.
    let config_dropped: Vec<String> = if mailboxes.is_empty() {
        Vec::new()
    } else {
        // Build inputs from every row with a remote_path, not just the
        // present ones: subtree selection needs ancestors in the set
        // even when an ancestor's own maildir is gone. The present()
        // filter below keeps absent rows (db-orphans) out of the
        // result.
        let inputs: Vec<MailboxSelectionInput> = rows
            .iter()
            .filter_map(|r| {
                r.remote_path.as_deref().map(|path| MailboxSelectionInput {
                    path,
                    role: r.role.as_deref(),
                })
            })
            .collect();
        let selected = get_selected_mailboxes(mailboxes, &inputs, case_insensitive, true);
        rows.iter()
            .filter(|r| present(&r.maildir_folder))
            .filter(|r| {
                r.remote_path
                    .as_deref()
                    .is_some_and(|path| !selected.contains(path))
            })
            .map(|r| r.maildir_folder.clone())
            .collect()
    };

    Ok(DriftReport::Drift(DriftClasses {
        disk_only,
        db_only,
        config_dropped,
        rename_in_flight,
    }))
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

    use crate::state::db;

    /// Build a fake maildir at `<root>/<folder>` with the cur/new/tmp
    /// triplet that `find_maildir_folders` keys on.
    fn touch_maildir(root: &Path, folder: &str) {
        let path = root.join(folder);
        for sub in ["cur", "new", "tmp"] {
            std::fs::create_dir_all(path.join(sub)).unwrap();
        }
    }

    /// Insert a top-level `mailbox_map` row whose `remote_path` equals
    /// its folder name (the top-level case where path == leaf).
    fn upsert_mailbox(conn: &Connection, id: &str, name: &str, folder: &str, role: Option<&str>) {
        queries::upsert_mailbox(
            conn,
            &MailboxRecord {
                jmap_mailbox_id: id.into(),
                name: name.to_string(),
                role: role.map(String::from),
                parent_id: None,
                maildir_folder: folder.to_string(),
                sort_order: 0,
                remote_path: Some(folder.to_string()),
            },
        )
        .unwrap();
    }

    /// Write a `.jma.mapping` sentinel binding `folder` to mailbox `id`.
    fn write_sentinel(root: &Path, folder: &str, id: &str, server_name: &str) {
        sentinel::write(
            &root.join(folder),
            &sentinel::MailboxMapping {
                jmap_mailbox_id: id.into(),
                parent_jmap_mailbox_id: None,
                server_name: server_name.to_string(),
            },
        )
        .unwrap();
    }

    /// Unwrap `DriftReport::Drift`; the classification tests never hit
    /// the degenerate variants.
    fn classes(report: DriftReport) -> DriftClasses {
        match report {
            DriftReport::Drift(c) => c,
            other => panic!("expected Drift, got {other:?}"),
        }
    }

    #[test]
    fn in_sync_reports_no_drift() {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        touch_maildir(dir.path(), "INBOX");
        upsert_mailbox(&conn, "M1", "INBOX", "INBOX", Some("inbox"));

        let c = classes(compute_drift(&conn, dir.path(), &[], false).unwrap());
        assert_eq!(c, DriftClasses::default());
    }

    #[test]
    fn empty_mailbox_map_is_no_mailbox_map() {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        assert_eq!(
            compute_drift(&conn, dir.path(), &[], false).unwrap(),
            DriftReport::NoMailboxMap
        );
    }

    #[test]
    fn disk_only_folder_is_a_stray() {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        upsert_mailbox(&conn, "M1", "INBOX", "INBOX", Some("inbox"));
        touch_maildir(dir.path(), "INBOX");
        touch_maildir(dir.path(), "Stray");

        let c = classes(compute_drift(&conn, dir.path(), &[], false).unwrap());
        assert_eq!(c.disk_only, vec!["Stray".to_string()]);
        assert!(c.db_only.is_empty());
        assert!(c.config_dropped.is_empty());
        assert!(c.rename_in_flight.is_empty());
    }

    #[test]
    fn db_row_without_maildir_is_an_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        upsert_mailbox(&conn, "M1", "Archive", "Archive", None);

        let c = classes(compute_drift(&conn, dir.path(), &[], false).unwrap());
        assert_eq!(c.db_only, vec!["Archive".to_string()]);
        assert!(c.disk_only.is_empty());
        assert!(c.rename_in_flight.is_empty());
    }

    #[test]
    fn folder_dropped_from_config_is_flagged() {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        touch_maildir(dir.path(), "INBOX");
        touch_maildir(dir.path(), "Archive");
        upsert_mailbox(&conn, "M1", "INBOX", "INBOX", Some("inbox"));
        upsert_mailbox(&conn, "M2", "Archive", "Archive", None);

        // Config selects only INBOX; Archive is present on both sides
        // but no longer in the sync set.
        let c = classes(compute_drift(&conn, dir.path(), &["INBOX".to_string()], false).unwrap());
        assert_eq!(c.config_dropped, vec!["Archive".to_string()]);
        assert!(c.disk_only.is_empty());
        assert!(c.db_only.is_empty());
        assert!(c.rename_in_flight.is_empty());
    }

    #[test]
    fn empty_config_never_flags_config_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        touch_maildir(dir.path(), "INBOX");
        touch_maildir(dir.path(), "Archive");
        upsert_mailbox(&conn, "M1", "INBOX", "INBOX", Some("inbox"));
        upsert_mailbox(&conn, "M2", "Archive", "Archive", None);

        let c = classes(compute_drift(&conn, dir.path(), &[], false).unwrap());
        assert_eq!(
            c,
            DriftClasses::default(),
            "sync-all (empty config) must not flag config-dropped or any other drift"
        );
    }

    #[test]
    fn local_rename_pairs_disk_and_db_orphans() {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        // mailbox_map still points id M9 at the old folder; no maildir
        // there. The maildir now lives at "New" with a sentinel binding
        // it back to M9.
        upsert_mailbox(&conn, "M9", "Old", "Old", None);
        touch_maildir(dir.path(), "New");
        write_sentinel(dir.path(), "New", "M9", "Old");

        let c = classes(compute_drift(&conn, dir.path(), &[], false).unwrap());
        assert_eq!(
            c.rename_in_flight,
            vec![RenameInFlight {
                db_folder: "Old".to_string(),
                disk_folder: "New".to_string(),
            }]
        );
        assert!(
            c.disk_only.is_empty(),
            "the renamed-to folder is not a stray"
        );
        assert!(
            c.db_only.is_empty(),
            "the renamed-from row is not an orphan"
        );
    }

    /// A disk orphan whose sentinel can't be read (here the sentinel
    /// path is a directory, which makes `std::fs::read` fail with an
    /// I/O error rather than NotFound) must not abort the whole drift
    /// report. Best-effort: the folder is left unpaired and surfaces
    /// as a stray in `disk_only`.
    #[test]
    fn unreadable_sentinel_does_not_abort_drift() {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        upsert_mailbox(&conn, "M1", "INBOX", "INBOX", Some("inbox"));
        touch_maildir(dir.path(), "INBOX");
        touch_maildir(dir.path(), "Stray");
        // Force `sentinel::read` down its Err arm: a directory at the
        // sentinel path reads back as EISDIR, not NotFound.
        std::fs::create_dir_all(sentinel::sentinel_path_for(&dir.path().join("Stray"))).unwrap();

        let c = classes(compute_drift(&conn, dir.path(), &[], false).unwrap());
        assert_eq!(c.disk_only, vec!["Stray".to_string()]);
        assert!(c.rename_in_flight.is_empty());
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
