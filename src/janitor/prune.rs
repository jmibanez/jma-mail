//! Prune janitorial task: turn the maildir/DB drift report into a
//! plan for clearing it, with a per-entry safety label.
//!
//! Offline -- reads only the state DB, the maildir tree, and the
//! `[sync]` config (no network). `plan` classifies each drift entry
//! and labels how risky removing it is; `apply` (separate) acts on the
//! labelled plan. The split keeps the same safety labels driving both
//! the preview and the apply-side gate.

use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;

use crate::jmap::mailbox::{MailboxSelectionInput, get_selected_mailboxes};
use crate::maildir_ops::drift::{DriftReport, RenameInFlight, compute_drift};
use crate::maildir_ops::store;
use crate::state::queries;

/// The drift class a prune entry came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneClass {
    /// On disk with no `mailbox_map` row -- an untracked stray.
    DiskOnlyStray,
    /// A `mailbox_map` row whose maildir is gone from disk -- orphaned
    /// cache rows.
    DbOnlyOrphan,
    /// Present on both sides but no longer selected by
    /// `[sync].mailboxes`.
    ConfigDropped,
}

/// Whether an entry can be pruned by a plain `--apply`, or which force
/// flag it requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneSafety {
    /// Removable without a force flag.
    Safe,
    /// Removal would delete a non-empty maildir; requires
    /// `--force-non-empty`.
    NeedsForceNonEmpty,
    /// The folder is still selected by `[sync].mailboxes`, so clearing
    /// its rows just makes the next sync re-download it; requires
    /// `--force-in-config` (or drop the folder from the config first).
    NeedsForceInConfig,
}

/// One prunable folder and what removing it entails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneEntry {
    /// The maildir folder name (`mailbox_map.maildir_folder` / the
    /// on-disk relative path).
    pub folder: String,
    pub class: PruneClass,
    /// Files in the folder's `cur/` + `new/`. Always 0 for a
    /// `DbOnlyOrphan`, whose maildir is already gone.
    pub file_count: usize,
    /// Whether removal touches the disk tree. False for a
    /// `DbOnlyOrphan`, which is a DB-cascade-only cleanup.
    pub removes_disk: bool,
    pub safety: PruneSafety,
}

/// The classified prune plan: prunable entries plus the renames the
/// planner deliberately leaves alone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrunePlan {
    pub entries: Vec<PruneEntry>,
    /// Local renames not yet pushed to the server. Never pruned -- the
    /// next sync settles them -- but surfaced so a preview can explain
    /// the skip rather than silently ignoring them.
    pub skipped_renames: Vec<RenameInFlight>,
}

/// Classify the maildir-vs-DB drift into a prune plan, labelling each
/// entry with the force flag (if any) its removal needs. `mailboxes` /
/// `case_insensitive` are the `[sync]` selection. A `MaildirRootMissing`
/// or `NoMailboxMap` drift state yields an empty plan.
pub fn plan(
    conn: &Connection,
    maildir_root: &Path,
    mailboxes: &[String],
    case_insensitive: bool,
) -> Result<PrunePlan> {
    let classes = match compute_drift(conn, maildir_root, mailboxes, case_insensitive)? {
        DriftReport::Drift(c) => c,
        DriftReport::MaildirRootMissing | DriftReport::NoMailboxMap => {
            return Ok(PrunePlan::default());
        }
    };

    let mut entries = Vec::new();

    // Disk-bearing classes: removal recurses the maildir, so a folder
    // with mail in it needs --force-non-empty.
    for folder in classes.disk_only {
        let file_count = count_folder_files(maildir_root, &folder);
        entries.push(PruneEntry {
            safety: non_empty_safety(file_count),
            folder,
            class: PruneClass::DiskOnlyStray,
            file_count,
            removes_disk: true,
        });
    }
    for folder in classes.config_dropped {
        let file_count = count_folder_files(maildir_root, &folder);
        entries.push(PruneEntry {
            safety: non_empty_safety(file_count),
            folder,
            class: PruneClass::ConfigDropped,
            file_count,
            removes_disk: true,
        });
    }

    // DbOnlyOrphan: the maildir is already gone, so removal is a DB
    // cascade only -- no --force-non-empty axis. The gate is config
    // membership: a folder still in the sync set would just re-download
    // next cycle, so clearing its rows needs --force-in-config.
    if !classes.db_only.is_empty() {
        let rows = queries::get_all_mailboxes(conn)?;
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
        let path_by_folder: HashMap<&str, Option<&str>> = rows
            .iter()
            .map(|r| (r.maildir_folder.as_str(), r.remote_path.as_deref()))
            .collect();
        for folder in classes.db_only {
            // No cached remote_path means we can't test membership;
            // treat as still-in-set so it needs the explicit flag.
            let in_sync_set = match path_by_folder.get(folder.as_str()).copied().flatten() {
                Some(path) => selected.contains(path),
                None => true,
            };
            entries.push(PruneEntry {
                safety: if in_sync_set {
                    PruneSafety::NeedsForceInConfig
                } else {
                    PruneSafety::Safe
                },
                folder,
                class: PruneClass::DbOnlyOrphan,
                file_count: 0,
                removes_disk: false,
            });
        }
    }

    Ok(PrunePlan {
        entries,
        skipped_renames: classes.rename_in_flight,
    })
}

fn non_empty_safety(file_count: usize) -> PruneSafety {
    if file_count == 0 {
        PruneSafety::Safe
    } else {
        PruneSafety::NeedsForceNonEmpty
    }
}

/// Count the mail files in a folder's `cur/` + `new/`. A folder that
/// can't be opened as a maildir (already gone, never created) counts
/// as 0.
fn count_folder_files(maildir_root: &Path, folder: &str) -> usize {
    match store::try_open_maildir(&maildir_root.join(folder)) {
        Some(maildir) => maildir.list_cur().chain(maildir.list_new()).count(),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maildir_ops::sentinel;
    use crate::state::db;
    use crate::state::queries::MailboxRecord;
    use tempfile::tempdir;

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

    fn touch_maildir(root: &Path, folder: &str) {
        for sub in ["cur", "new", "tmp"] {
            std::fs::create_dir_all(root.join(folder).join(sub)).unwrap();
        }
    }

    /// Drop a maildir-shaped file into a folder's `cur/` so the folder
    /// counts as non-empty.
    fn seed_file(root: &Path, folder: &str, uniq: &str) {
        let cur = root.join(folder).join("cur");
        std::fs::create_dir_all(&cur).unwrap();
        std::fs::write(
            cur.join(format!("{uniq}.host:2,")),
            b"Message-ID: <x@example.com>\r\nSubject: t\r\n\r\nbody",
        )
        .unwrap();
    }

    fn entry<'a>(plan: &'a PrunePlan, folder: &str) -> &'a PruneEntry {
        plan.entries
            .iter()
            .find(|e| e.folder == folder)
            .unwrap_or_else(|| panic!("no entry for {folder}; got {:?}", plan.entries))
    }

    #[test]
    fn no_mailbox_map_yields_empty_plan() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let p = plan(&conn, dir.path(), &[], false).unwrap();
        assert_eq!(p, PrunePlan::default());
    }

    #[test]
    fn empty_stray_is_safe_non_empty_needs_force() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        // A row so the map isn't empty; INBOX synced and in sync.
        upsert_mailbox(&conn, "M1", "INBOX", "INBOX", Some("inbox"));
        touch_maildir(dir.path(), "INBOX");
        // Two strays: one empty, one with a file.
        touch_maildir(dir.path(), "EmptyStray");
        touch_maildir(dir.path(), "FullStray");
        seed_file(dir.path(), "FullStray", "1");

        let p = plan(&conn, dir.path(), &[], false).unwrap();

        let empty = entry(&p, "EmptyStray");
        assert_eq!(empty.class, PruneClass::DiskOnlyStray);
        assert_eq!(empty.safety, PruneSafety::Safe);
        assert!(empty.removes_disk);

        let full = entry(&p, "FullStray");
        assert_eq!(full.class, PruneClass::DiskOnlyStray);
        assert_eq!(full.file_count, 1);
        assert_eq!(full.safety, PruneSafety::NeedsForceNonEmpty);
    }

    #[test]
    fn config_dropped_non_empty_needs_force_non_empty() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        touch_maildir(dir.path(), "INBOX");
        touch_maildir(dir.path(), "Archive");
        seed_file(dir.path(), "Archive", "1");
        upsert_mailbox(&conn, "M1", "INBOX", "INBOX", Some("inbox"));
        upsert_mailbox(&conn, "M2", "Archive", "Archive", None);

        // Config selects only INBOX -> Archive is config-dropped.
        let p = plan(&conn, dir.path(), &["INBOX".to_string()], false).unwrap();

        let archive = entry(&p, "Archive");
        assert_eq!(archive.class, PruneClass::ConfigDropped);
        assert_eq!(archive.safety, PruneSafety::NeedsForceNonEmpty);
        assert!(archive.removes_disk);
    }

    #[test]
    fn db_orphan_not_in_config_is_safe() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        touch_maildir(dir.path(), "INBOX");
        upsert_mailbox(&conn, "M1", "INBOX", "INBOX", Some("inbox"));
        // Archive has a row but no maildir, and isn't in the config.
        upsert_mailbox(&conn, "M2", "Archive", "Archive", None);

        let p = plan(&conn, dir.path(), &["INBOX".to_string()], false).unwrap();

        let archive = entry(&p, "Archive");
        assert_eq!(archive.class, PruneClass::DbOnlyOrphan);
        assert_eq!(archive.safety, PruneSafety::Safe);
        assert!(!archive.removes_disk);
        assert_eq!(archive.file_count, 0);
    }

    #[test]
    fn db_orphan_still_in_config_needs_force_in_config() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        touch_maildir(dir.path(), "INBOX");
        upsert_mailbox(&conn, "M1", "INBOX", "INBOX", Some("inbox"));
        // Archive has a row but no maildir, and IS in the config.
        upsert_mailbox(&conn, "M2", "Archive", "Archive", None);

        let p = plan(
            &conn,
            dir.path(),
            &["INBOX".to_string(), "Archive".to_string()],
            false,
        )
        .unwrap();

        let archive = entry(&p, "Archive");
        assert_eq!(archive.class, PruneClass::DbOnlyOrphan);
        assert_eq!(archive.safety, PruneSafety::NeedsForceInConfig);
    }

    #[test]
    fn pending_rename_is_skipped_not_pruned() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        // Row points M9 at "Old" (no maildir); maildir lives at "New"
        // with a sentinel binding it back to M9.
        upsert_mailbox(&conn, "M9", "Old", "Old", None);
        touch_maildir(dir.path(), "New");
        sentinel::write(
            &dir.path().join("New"),
            &sentinel::MailboxMapping {
                jmap_mailbox_id: "M9".into(),
                parent_jmap_mailbox_id: None,
                server_name: "Old".to_string(),
            },
        )
        .unwrap();

        let p = plan(&conn, dir.path(), &[], false).unwrap();

        assert!(
            p.entries.is_empty(),
            "a pending rename must not produce prune entries; got {:?}",
            p.entries
        );
        assert_eq!(p.skipped_renames.len(), 1);
        assert_eq!(p.skipped_renames[0].db_folder, "Old");
        assert_eq!(p.skipped_renames[0].disk_folder, "New");
    }
}
