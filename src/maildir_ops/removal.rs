//! Destructive removal of a single maildir folder tree, with the
//! state-DB cascade that keeps the two sides consistent.
//!
//! Centralizing removal here gives every in-crate destroy path one
//! safe implementation -- the path-escape guard and the
//! rescue-before-delete protection live in exactly one place rather
//! than being reimplemented per caller. `remove_maildir_tree` is
//! `pub(crate)` and free of `sync::` dependencies for that reason.

use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashSet;
use std::path::Path;
use tracing::{info, warn};

use crate::ids::{JmapMailboxId, MaildirId};
use crate::maildir_ops::{namespace, store};
use crate::state::queries;

/// Whether `remove_maildir_tree` actually took the folder down.
///
/// The distinction is load-bearing for callers that drop the
/// `mailbox_map` row afterwards: a row may only be dropped once the
/// tree is gone and its cascade has run. Dropping it on a `Skipped`
/// outcome would orphan the folder on disk and its `message_map`
/// rows while losing the id binding, defeating the retry the skip
/// leaves room for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemovalOutcome {
    /// The tree is gone (removed here, or already absent) and the
    /// state-DB cascade ran. Safe for the caller to drop the
    /// `mailbox_map` row.
    Removed,
    /// The removal was refused or aborted; the folder and its rows
    /// are left intact for a later retry. The caller must not drop
    /// the `mailbox_map` row.
    Skipped,
}

/// Delete an entire maildir folder tree and cascade the state-DB rows
/// that reference it (`message_map`, `local_state`,
/// `folder_checkpoint`). `maildir_root` must be canonical -- absolute,
/// with `~`, `.`/`..`, and symlinks already resolved (obtain it from
/// `std::fs::canonicalize` or `Config::canonical_maildir_root`). The
/// path-escape guard leans on that: a canonical root means `folder` is
/// the only input that can introduce an escape, so the guard has just
/// one untrusted value to vet. `mailbox_id` is the folder's bound
/// mailbox, or `None` for a row-less folder (an untracked stray): with
/// `None` every file is treated as unmapped (so all are rescued) and
/// the `message_map` cascade is skipped. The `mailbox_map` row itself
/// is not touched here -- the caller owns that.
///
/// Caller-visible guarantees:
/// - Refuses (`Skipped`) if `folder` would resolve outside
///   `maildir_root` -- a `..` component, an absolute path, or a
///   symlink pointing out of the tree -- so a bad row can't reach
///   outside it.
/// - Never silently drops a file the DB doesn't know about: unmapped
///   files are moved to the rescue maildir first, and a rescue
///   failure leaves the folder intact for a later retry.
/// - Idempotent: an already-absent folder still drains its rows and
///   reports `Removed`.
///
/// Returns `Removed` when the tree is gone and the cascade ran, or
/// `Skipped` (which also logs the reason) when the removal was
/// refused or aborted. `Err` is reserved for state-DB failures.
pub(crate) fn remove_maildir_tree(
    conn: &Connection,
    maildir_root: &Path,
    folder: &str,
    mailbox_id: Option<&JmapMailboxId>,
) -> Result<RemovalOutcome> {
    let target = maildir_root.join(folder);
    // Path-safety guard. `maildir_root` is canonical by contract, so
    // `folder` is the only input that can escape the tree. When the
    // target exists, `canonicalize` resolves any symlinks and we check
    // the resolved path stays under the root (catches a symlinked
    // folder pointing outside). When it doesn't exist (canonicalize ->
    // NotFound), fall back to a lexical check on `folder`: refuse a
    // `..` component (a rule-renamed folder can carry one) and refuse
    // an absolute `folder`, which `join` would let replace the root
    // entirely.
    let escaped = match std::fs::canonicalize(&target) {
        Ok(canon) => !canon.starts_with(maildir_root),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Path::new(folder)
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
                || !target.starts_with(maildir_root)
        }
        Err(e) => {
            warn!(
                "remove_maildir_tree: cannot canonicalize {}: {}; skipping",
                target.display(),
                e
            );
            return Ok(RemovalOutcome::Skipped);
        }
    };
    if escaped {
        warn!(
            "remove_maildir_tree: refusing to remove {} -- resolves outside \
             maildir_root {}",
            target.display(),
            maildir_root.display()
        );
        return Ok(RemovalOutcome::Skipped);
    }
    // A row-less folder (a stray) has no id to show in the logs.
    let id_label = mailbox_id.map_or_else(|| "-".to_string(), |id| id.to_string());
    // Refuse to remove a folder that still contains a nested maildir:
    // remove_dir_all would take that child's whole tree (and its mail)
    // too, while only this folder's own cur/+new/ get a rescue pass.
    // Callers that prune a parent and child together process them
    // deepest-first, so a child still present here is one that is NOT
    // being removed -- destroying it would lose mail outside this
    // entry's scope and skip its cascade. Leave it for the caller to
    // handle the child first (or explicitly).
    if contains_nested_maildir(&target) {
        warn!(
            "remove_maildir_tree {}/ (id {}): contains a nested maildir not being \
             removed; skipping so its mail isn't destroyed with the parent",
            folder, id_label
        );
        return Ok(RemovalOutcome::Skipped);
    }
    // A folder that is a maildir on disk (store::is_maildir) but not
    // ready to enumerate (no cur/) is one that lost its cur/ -- e.g. an
    // empty-dir cleanup tool removed the empty cur/ while new/ still
    // held messages. Without healing, the rescue pass below would skip
    // it and remove_dir_all would drop those messages. Recreate the
    // missing cur/ so the rescue sees and protects them, the same heal
    // an MUA performs on open. A folder that isn't a maildir at all has
    // no messages to rescue, so the removal below handles it directly.
    if store::is_maildir(&target)
        && store::try_open_maildir(&target).is_none()
        && let Err(e) = store::ensure_maildir(&target)
    {
        warn!(
            "remove_maildir_tree {}/ (id {}): folder has new/ but no cur/ and \
             healing it failed: {}; skipping so a later retry can rescue its mail",
            folder, id_label, e
        );
        return Ok(RemovalOutcome::Skipped);
    }
    match rescue_unmapped_files(conn, maildir_root, &target, mailbox_id) {
        Ok(0) => {}
        Ok(n) => {
            info!(
                "remove_maildir_tree {}/ (id {}): rescued {} unmapped file(s) to {}/",
                folder,
                id_label,
                n,
                namespace::RESCUE_FOLDER_NAME
            );
        }
        Err(e) => {
            warn!(
                "remove_maildir_tree {}/ (id {}): rescue failed: {}; aborting \
                 destroy this cycle, will retry next cycle once the rescue \
                 dir is writable",
                folder, id_label, e
            );
            return Ok(RemovalOutcome::Skipped);
        }
    }
    match std::fs::remove_dir_all(&target) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            warn!(
                "remove_maildir_tree: remove_dir_all {} failed: {}; \
                 skipping cascade cleanup so a manual retry next cycle \
                 can clear the rows once disk catches up",
                target.display(),
                e
            );
            return Ok(RemovalOutcome::Skipped);
        }
    }
    // Per-folder transaction so an fs failure on folder N+1 doesn't
    // roll back folder N's cascade. The fs op already ran before this
    // point and is irreversible; a wide txn that included it would
    // leave a half-deleted state on partial failure.
    let txn = conn.unchecked_transaction()?;
    let messages_removed = match mailbox_id {
        Some(id) => queries::delete_messages_by_jmap_mailbox_id(&txn, id)?,
        None => 0,
    };
    let state_removed = queries::delete_local_state_by_folder(&txn, folder)?;
    queries::delete_folder_checkpoint(&txn, folder)?;
    txn.commit()?;
    info!(
        "remove_maildir_tree {}/ (id {}): removed maildir, dropped {} \
         message_map row(s) and {} local_state row(s)",
        folder, id_label, messages_removed, state_removed
    );
    Ok(RemovalOutcome::Removed)
}

/// Whether `dir` contains a nested maildir at any depth below itself --
/// a descendant directory that is itself a maildir (`store::is_maildir`).
/// `dir`'s own `cur/`/`new/`/`tmp/` leaves and jma's private namespace
/// are skipped, so only genuine sub-mailboxes count. Used to refuse a
/// `remove_dir_all` that would swallow a child maildir the caller
/// didn't ask to remove.
fn contains_nested_maildir(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str == "cur" || name_str == "new" || name_str == "tmp" {
            continue;
        }
        if namespace::is_jma_private(&name_str) {
            continue;
        }
        let path = entry.path();
        if store::is_maildir(&path) || contains_nested_maildir(&path) {
            return true;
        }
    }
    false
}

/// Move any file in `folder_path` that the state DB doesn't know
/// about (unmapped, local-only files) into the rescue maildir, so a
/// subsequent delete of the folder can't destroy them; mapped files
/// are left in place. Returns the number rescued; `Err` if any file
/// couldn't be moved, signaling the caller to abort the delete and
/// retry later.
fn rescue_unmapped_files(
    conn: &Connection,
    maildir_root: &Path,
    folder_path: &Path,
    jmap_mailbox_id: Option<&JmapMailboxId>,
) -> Result<usize> {
    let Some(maildir) = store::try_open_maildir(folder_path) else {
        return Ok(0);
    };
    // A row-less folder has no mapped files, so the known set is empty
    // and every file is rescued.
    let known: HashSet<MaildirId> = match jmap_mailbox_id {
        Some(id) => queries::get_messages_by_jmap_mailbox_id(conn, id)?
            .into_iter()
            .filter_map(|m| m.maildir_id)
            .collect(),
        None => HashSet::new(),
    };
    let rescue_root = maildir_root.join(namespace::RESCUE_FOLDER_NAME);
    let mut rescued = 0usize;
    let mut rescue_dir_created = false;
    for entry in maildir.list_cur().chain(maildir.list_new()) {
        let entry = entry?;
        if known.contains(entry.id()) {
            continue;
        }
        if !rescue_dir_created {
            store::ensure_maildir(&rescue_root).map_err(|e| {
                anyhow::anyhow!("ensure_maildir {} failed: {}", rescue_root.display(), e)
            })?;
            rescue_dir_created = true;
        }
        let src = entry.path();
        let filename = src.file_name().ok_or_else(|| {
            anyhow::anyhow!("rescue source has no filename component: {}", src.display())
        })?;
        let dst = rescue_root.join("cur").join(filename);
        info!(
            "remove_maildir_tree: rescuing unmapped file {} -> {}",
            src.display(),
            dst.display()
        );
        std::fs::rename(src, &dst).map_err(|e| {
            anyhow::anyhow!(
                "rename {} -> {} failed: {}",
                src.display(),
                dst.display(),
                e
            )
        })?;
        rescued += 1;
    }
    Ok(rescued)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maildir_ops::sentinel::{self, MailboxMapping};
    use crate::maildir_ops::store::{ensure_maildir, store_message};
    use crate::state::db;
    use crate::state::queries::MessageRecord;
    use tempfile::tempdir;

    /// Canonicalize the root the way the batch caller does, then
    /// drive `remove_maildir_tree` for one folder bound to `mailbox_id`.
    fn remove_one(
        conn: &Connection,
        maildir_root: &Path,
        folder: &str,
        mailbox_id: &str,
    ) -> RemovalOutcome {
        let root_canon = std::fs::canonicalize(maildir_root).unwrap();
        remove_maildir_tree(
            conn,
            &root_canon,
            folder,
            Some(&JmapMailboxId::from(mailbox_id)),
        )
        .unwrap()
    }

    /// A row-less folder (`mailbox_id = None`, an untracked stray) has
    /// no mapped files, so every file is rescued and the tree removed.
    #[test]
    fn row_less_folder_rescues_all_files() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path();
        let folder = "Stray";
        let folder_path = maildir_root.join(folder);
        let maildir = ensure_maildir(&folder_path).unwrap();
        let id = store_message(
            &maildir,
            b"Message-ID: <s@example.com>\r\nSubject: x\r\n\r\nbody\r\n",
            "",
        )
        .unwrap();

        let root_canon = std::fs::canonicalize(maildir_root).unwrap();
        let outcome = remove_maildir_tree(&conn, &root_canon, folder, None).unwrap();

        assert_eq!(outcome, RemovalOutcome::Removed);
        assert!(!folder_path.exists(), "stray maildir must be removed");
        let rescue_cur = maildir_root.join(namespace::RESCUE_FOLDER_NAME).join("cur");
        let rescued: Vec<String> = std::fs::read_dir(&rescue_cur)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            rescued.len(),
            1,
            "the stray's file is unmapped -> rescued; got {rescued:?}"
        );
        assert!(rescued[0].contains(id.as_ref()));
    }

    /// Happy path: maildir exists with one bound file + matching DB
    /// rows; removal takes the tree down and drains every row
    /// pointing at the folder/id.
    #[test]
    fn destroys_folder_and_cascades_cleanup() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path();

        let folder = "Archive";
        let folder_path = maildir_root.join(folder);
        let maildir = ensure_maildir(&folder_path).unwrap();
        sentinel::write(
            &folder_path,
            &MailboxMapping {
                jmap_mailbox_id: "MB-ARCH".into(),
                parent_jmap_mailbox_id: None,
                server_name: folder.to_string(),
            },
        )
        .unwrap();
        let maildir_id = store_message(
            &maildir,
            b"Message-ID: <a@example.com>\r\nSubject: x\r\n\r\nbody\r\n",
            "",
        )
        .unwrap();
        queries::upsert_message(
            &conn,
            &MessageRecord {
                jmap_email_id: "E1".into(),
                jmap_blob_id: Some("B1".into()),
                jmap_thread_id: Some("T1".into()),
                jmap_mailbox_id: "MB-ARCH".into(),
                maildir_id: Some(maildir_id.clone()),
                message_id: "a@example.com".into(),
                flags: "".into(),
                jmap_keywords: "{}".into(),
            },
        )
        .unwrap();
        queries::upsert_local_state(&conn, &maildir_id, folder, "", None).unwrap();

        assert_eq!(
            remove_one(&conn, maildir_root, folder, "MB-ARCH"),
            RemovalOutcome::Removed
        );

        assert!(!folder_path.exists(), "maildir tree must be removed");
        assert!(
            queries::get_message_by_jmap_id(&conn, &"E1".into())
                .unwrap()
                .is_none(),
            "message_map row for the dead id must be gone"
        );
        assert!(
            queries::get_local_state_for_folder(&conn, folder)
                .unwrap()
                .is_empty(),
            "local_state for the destroyed folder must be drained"
        );
        assert!(
            queries::get_folder_checkpoint(&conn, folder)
                .unwrap()
                .is_none(),
            "folder_checkpoint row must be gone so next-cycle dedupe doesn't trip"
        );
    }

    /// Path-safety: a `folder` that path-traverses out of
    /// `maildir_root` must be refused; nothing on disk is touched
    /// (the row's existence outside the root is the producer's bug
    /// to fix).
    #[test]
    fn refuses_path_traversal() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path().join("root");
        std::fs::create_dir_all(&maildir_root).unwrap();

        // Create a sibling directory the call will try to remove via
        // `../sibling`. If the guard fails, this directory + file
        // would vanish.
        let sibling = dir.path().join("sibling");
        std::fs::create_dir_all(&sibling).unwrap();
        let canary = sibling.join("canary");
        std::fs::write(&canary, b"must survive").unwrap();

        assert_eq!(
            remove_one(&conn, &maildir_root, "../sibling", "MB-EVIL"),
            RemovalOutcome::Skipped
        );

        assert!(
            canary.exists(),
            "path-traversal target must not be touched; canary file gone implies the guard failed"
        );
    }

    /// Path-safety on the NotFound branch: the joined target doesn't
    /// exist on disk, so `canonicalize` returns NotFound.
    /// `PathBuf::starts_with` is component-wise lexical -- without
    /// the `ParentDir`-component refusal, `root/../missing` would
    /// slip past the prefix check (root is still the lexical
    /// prefix), and the DB cascade would still drain the dead id's
    /// rows. Pin that the guard refuses.
    #[test]
    fn refuses_path_traversal_when_target_absent() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path().join("root");
        std::fs::create_dir_all(&maildir_root).unwrap();

        // Pre-seed a `message_map` row pointing at MB-EVIL so the
        // cascade leg has something to drain -- if the guard fails,
        // this row would vanish.
        queries::upsert_message(
            &conn,
            &MessageRecord {
                jmap_email_id: "E-canary".into(),
                jmap_blob_id: None,
                jmap_thread_id: None,
                jmap_mailbox_id: "MB-EVIL".into(),
                maildir_id: Some(MaildirId::from("FOO.host")),
                message_id: "canary@example.com".into(),
                flags: "".into(),
                jmap_keywords: "{}".into(),
            },
        )
        .unwrap();

        assert_eq!(
            remove_one(&conn, &maildir_root, "../missing", "MB-EVIL"),
            RemovalOutcome::Skipped
        );

        assert!(
            queries::get_message_by_jmap_id(&conn, &"E-canary".into())
                .unwrap()
                .is_some(),
            "guard must refuse on `../X` even when the target is absent; \
             the DB cascade must not run for a refused action"
        );
    }

    /// Path-safety against a symlink escape via an *intermediate*
    /// path component. The folder is `link/child`, where `link` is a
    /// symlink out of the root to an external directory.
    /// `canonicalize` follows the link and resolves the target
    /// outside the root, so the guard refuses. This is the
    /// discriminating case: path resolution follows intermediate
    /// symlinks, so without the guard `remove_dir_all(root/link/child)`
    /// would recurse into the out-of-tree `external/child` and
    /// destroy the canary. (A *top-level* symlink wouldn't
    /// discriminate: `remove_dir_all` removes the link entry itself
    /// without following it.) The `../X` tests cover the lexical
    /// branch; this pins the symlink-following one.
    #[cfg(unix)]
    #[test]
    fn refuses_symlink_escape() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path().join("root");
        std::fs::create_dir_all(&maildir_root).unwrap();

        // An external tree outside the root: external/child holds a
        // canary that would be destroyed if the guard let
        // remove_dir_all run through the symlink.
        let external_child = dir.path().join("external").join("child");
        std::fs::create_dir_all(&external_child).unwrap();
        let canary = external_child.join("canary");
        std::fs::write(&canary, b"must survive").unwrap();

        // Inside the root, `link` is a symlink to the external
        // directory; the call targets `link/child`.
        std::os::unix::fs::symlink(dir.path().join("external"), maildir_root.join("link")).unwrap();

        assert_eq!(
            remove_one(&conn, &maildir_root, "link/child", "MB-LINK"),
            RemovalOutcome::Skipped
        );

        assert!(
            canary.exists(),
            "guard must refuse a path that resolves outside the root via an \
             intermediate symlink; canary gone implies remove_dir_all ran \
             through the link and destroyed the out-of-tree target"
        );
    }

    /// Idempotent on already-absent folder: an external `rm -rf`
    /// between detection and execution makes `remove_dir_all` see
    /// NotFound. The cascade cleanup still runs so any stale rows
    /// still pointing at the folder/id get drained.
    #[test]
    fn idempotent_on_absent_folder() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path();

        // No folder on disk, but pre-seed DB rows so the cleanup leg
        // has work to do.
        queries::upsert_message(
            &conn,
            &MessageRecord {
                jmap_email_id: "E1".into(),
                jmap_blob_id: None,
                jmap_thread_id: None,
                jmap_mailbox_id: "MB-ARCH".into(),
                maildir_id: Some(MaildirId::from("FOO.host")),
                message_id: "a@example.com".into(),
                flags: "".into(),
                jmap_keywords: "{}".into(),
            },
        )
        .unwrap();
        queries::upsert_local_state(&conn, &MaildirId::from("FOO.host"), "Archive", "", None)
            .unwrap();

        assert_eq!(
            remove_one(&conn, maildir_root, "Archive", "MB-ARCH"),
            RemovalOutcome::Removed,
            "an already-absent folder still reaches the desired end state"
        );

        assert!(
            queries::get_message_by_jmap_id(&conn, &"E1".into())
                .unwrap()
                .is_none(),
            "message_map cascade cleanup runs even when the folder was already gone"
        );
        assert!(
            queries::get_local_state_for_folder(&conn, "Archive")
                .unwrap()
                .is_empty(),
            "local_state cascade cleanup runs even when the folder was already gone"
        );
    }

    /// Two files in the folder: one with a `message_map` row
    /// (mapped), one without (local-only draft). After removal, the
    /// mapped file is gone (with the folder) and the unmapped file
    /// lands in `<root>/.jma-rescue/cur/`.
    #[test]
    fn rescues_unmapped_files_to_rescue_maildir() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path();

        let folder = "Archive";
        let folder_path = maildir_root.join(folder);
        let maildir = ensure_maildir(&folder_path).unwrap();
        let mapped_id = store_message(
            &maildir,
            b"Message-ID: <mapped@example.com>\r\nSubject: x\r\n\r\nbody\r\n",
            "",
        )
        .unwrap();
        let unmapped_id = store_message(
            &maildir,
            b"Message-ID: <unmapped@example.com>\r\nSubject: y\r\n\r\nbody\r\n",
            "",
        )
        .unwrap();
        queries::upsert_message(
            &conn,
            &MessageRecord {
                jmap_email_id: "E-MAPPED".into(),
                jmap_blob_id: None,
                jmap_thread_id: None,
                jmap_mailbox_id: "MB-ARCH".into(),
                maildir_id: Some(mapped_id.clone()),
                message_id: "mapped@example.com".into(),
                flags: "".into(),
                jmap_keywords: "{}".into(),
            },
        )
        .unwrap();

        remove_one(&conn, maildir_root, folder, "MB-ARCH");

        assert!(!folder_path.exists(), "folder maildir destroyed");
        // Rescue funnels all files into `cur/` regardless of their
        // source subdir -- rescued mail is no longer "new" in the
        // maildir-notification sense.
        let rescue_cur = maildir_root.join(namespace::RESCUE_FOLDER_NAME).join("cur");
        let rescued: Vec<String> = std::fs::read_dir(&rescue_cur)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            rescued.len(),
            1,
            "exactly one file rescued, got {:?}",
            rescued
        );
        assert!(
            rescued[0].contains(unmapped_id.as_ref()),
            "rescued file's name should contain the unmapped maildir_id ({}); got {}",
            unmapped_id,
            rescued[0]
        );
        assert!(
            !rescued[0].contains(mapped_id.as_ref()),
            "the mapped file must NOT appear in rescue dir; got {}",
            rescued[0]
        );
    }

    /// A maildir whose (empty) `cur/` was removed -- e.g. by an
    /// empty-dir cleanup tool -- while `new/` still holds mail is not
    /// maildir-shaped, so without healing the rescue pass would skip it
    /// and `remove_dir_all` would silently drop the `new/` messages.
    /// `remove_maildir_tree` recreates the missing `cur/` so the
    /// unmapped message is rescued before the folder is removed.
    #[test]
    fn heals_and_rescues_maildir_missing_cur() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path();

        let folder = "Archive";
        let folder_path = maildir_root.join(folder);
        let maildir = ensure_maildir(&folder_path).unwrap();
        // Empty flags route to new/; no message_map row -> unmapped.
        let unmapped_id = store_message(
            &maildir,
            b"Message-ID: <local@example.com>\r\nSubject: x\r\n\r\nbody\r\n",
            "",
        )
        .unwrap();
        // Simulate the cleanup: drop the empty cur/, leaving new/ + its
        // message behind.
        std::fs::remove_dir(folder_path.join("cur")).unwrap();
        assert!(!folder_path.join("cur").exists());
        assert!(folder_path.join("new").is_dir());

        let outcome = remove_one(&conn, maildir_root, folder, "MB-ARCH");

        assert_eq!(outcome, RemovalOutcome::Removed);
        assert!(!folder_path.exists(), "folder removed after rescue");
        let rescue_cur = maildir_root.join(namespace::RESCUE_FOLDER_NAME).join("cur");
        let rescued: Vec<String> = std::fs::read_dir(&rescue_cur)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            rescued.len(),
            1,
            "the unmapped new/ message must be rescued, not destroyed; got {rescued:?}"
        );
        assert!(
            rescued[0].contains(unmapped_id.as_ref()),
            "rescued file should carry the unmapped id {unmapped_id}; got {}",
            rescued[0]
        );
    }

    /// A folder containing a nested maildir is refused: `remove_dir_all`
    /// would swallow the child's tree (and its mail) too, but only the
    /// parent's own files get a rescue pass. The destroy is skipped so
    /// the caller removes the child first (or explicitly). Protects both
    /// the prune and sync destroy paths.
    #[test]
    fn refuses_folder_with_nested_maildir() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path();
        let parent = "Parent";
        ensure_maildir(&maildir_root.join(parent)).unwrap();
        let child = ensure_maildir(&maildir_root.join(parent).join("Child")).unwrap();
        store_message(
            &child,
            b"Message-ID: <c@example.com>\r\nSubject: x\r\n\r\nbody\r\n",
            "",
        )
        .unwrap();

        let outcome = remove_one(&conn, maildir_root, parent, "MB-PARENT");

        assert_eq!(
            outcome,
            RemovalOutcome::Skipped,
            "a folder containing a nested maildir must be refused"
        );
        assert!(
            maildir_root.join(parent).join("Child").join("new").is_dir(),
            "the nested child maildir must be left untouched"
        );
    }

    /// Every file in the folder has a `message_map` row; no rescue
    /// needed. The `.jma-rescue` directory is not created
    /// (lazy-creation contract: zero-touch when nothing needs
    /// rescuing).
    #[test]
    fn no_rescue_dir_when_every_file_is_mapped() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path();

        let folder = "Archive";
        let folder_path = maildir_root.join(folder);
        let maildir = ensure_maildir(&folder_path).unwrap();
        let mapped_id = store_message(
            &maildir,
            b"Message-ID: <a@example.com>\r\nSubject: x\r\n\r\nbody\r\n",
            "",
        )
        .unwrap();
        queries::upsert_message(
            &conn,
            &MessageRecord {
                jmap_email_id: "E1".into(),
                jmap_blob_id: None,
                jmap_thread_id: None,
                jmap_mailbox_id: "MB-ARCH".into(),
                maildir_id: Some(mapped_id),
                message_id: "a@example.com".into(),
                flags: "".into(),
                jmap_keywords: "{}".into(),
            },
        )
        .unwrap();

        remove_one(&conn, maildir_root, folder, "MB-ARCH");

        assert!(!folder_path.exists(), "folder destroyed");
        assert!(
            !maildir_root.join(namespace::RESCUE_FOLDER_NAME).exists(),
            "no rescue dir should be created when nothing needed rescuing"
        );
    }

    /// Rescue failure aborts the destroy: the folder stays on disk
    /// and the cascade DB cleanup is skipped so a later cycle can
    /// retry. Provoke the failure by pre-creating a *directory* at
    /// the rescue destination path the unmapped file would
    /// `fs::rename` into -- rename onto an existing directory fails
    /// with EISDIR.
    #[test]
    fn rescue_failure_aborts_destroy_and_keeps_db_rows() {
        let dir = tempdir().unwrap();
        let conn = db::open_in_memory().unwrap();
        let maildir_root = dir.path();

        let folder = "Archive";
        let folder_path = maildir_root.join(folder);
        let maildir = ensure_maildir(&folder_path).unwrap();
        let unmapped_id = store_message(
            &maildir,
            b"Message-ID: <unmapped@example.com>\r\nSubject: y\r\n\r\nbody\r\n",
            "",
        )
        .unwrap();
        // Pre-create `.jma-rescue/cur/<unmapped_id...>` as a
        // directory so `fs::rename` fails (rescue always targets
        // cur/, regardless of where the source file lives). The
        // source file lives in `new/` because `store_message` with
        // empty flags routes through `store_new_with_flags`.
        let src_filename = std::fs::read_dir(folder_path.join("new"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .file_name();
        let rescue_cur = maildir_root.join(namespace::RESCUE_FOLDER_NAME).join("cur");
        std::fs::create_dir_all(&rescue_cur).unwrap();
        std::fs::create_dir_all(rescue_cur.join(&src_filename)).unwrap();

        // Pre-seed a DB row so we can detect that cascade cleanup
        // did NOT run.
        queries::upsert_message(
            &conn,
            &MessageRecord {
                jmap_email_id: "E-OTHER".into(),
                jmap_blob_id: None,
                jmap_thread_id: None,
                jmap_mailbox_id: "MB-ARCH".into(),
                maildir_id: Some(MaildirId::from("SOMETHING-ELSE")),
                message_id: "other@example.com".into(),
                flags: "".into(),
                jmap_keywords: "{}".into(),
            },
        )
        .unwrap();

        assert_eq!(
            remove_one(&conn, maildir_root, folder, "MB-ARCH"),
            RemovalOutcome::Skipped,
            "a rescue failure must report Skipped so the caller keeps the row"
        );

        assert!(
            folder_path.exists(),
            "folder maildir must remain on disk after rescue failure"
        );
        assert!(
            folder_path.join("new").join(&src_filename).exists(),
            "the unmapped file must remain in the folder after rescue failure"
        );
        assert!(
            queries::get_message_by_jmap_id(&conn, &"E-OTHER".into())
                .unwrap()
                .is_some(),
            "cascade DB cleanup must be skipped so next-cycle retry has \
             the same input state -- ie unmapped maildir id still mapped to E-OTHER"
        );

        // Sanity: the unmapped id is unchanged (we'd rather see
        // retry produce the same rescue attempt next cycle than
        // discover the file vanished).
        assert!(unmapped_id.as_ref().chars().count() > 0);
    }
}
