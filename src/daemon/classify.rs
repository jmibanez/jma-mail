//! Classification of structural candidate paths, run by the watcher
//! before it emits a `SyncTrigger`, so folder-level filesystem noise
//! never enters the trigger pipeline: a `LocalStructuralChange` that
//! reaches the runner always starts a full-scope cycle.
//!
//! A debounced batch can carry paths the watcher couldn't attribute
//! to a bound folder's `cur/`/`new/` -- real folder creates, renames,
//! and teardowns, but also sidecar writes next to the maildir trio
//! (`.DS_Store`, `.uidvalidity`), foreign index directories
//! (`.notmuch/`) churning temp files, junk directory trees, and bare
//! `mkdir`s. None of the latter need a full scan.
//!
//! A candidate fires on either of two signals, both cheap:
//!
//! - It is a `cur`/`new`/`tmp` path event, gated on the folder
//!   sentinel: a missing trio always fires (a folder being torn
//!   down), and an intact trio fires only when its parent folder
//!   has no `.jma.mapping` sentinel (a folder jma has not bound
//!   yet, i.e. genuinely new). FSEvents on macOS can coalesce heavy
//!   in-`cur/` churn -- renames and hardlinks that never leave the
//!   folder, such as Gnus/nnmaildir's bookkeeping -- up into a
//!   directory-level event naming `cur` itself; without the gate
//!   that echo would read as a folder appearing. An intact trio
//!   under a sentinel'd folder is neither a creation (already
//!   bound) nor a teardown (still there), so it is noise.
//! - It is a maildir on disk (`store::is_maildir`), unguarded by the
//!   sentinel. A folder that appears by create, rename, or move
//!   stats as a maildir; a rename moves the trio atomically without
//!   emitting trio events, so rename detection rides this signal on
//!   the new name. A renamed folder carries its sentinel with it, so
//!   gating this arm on the sentinel would suppress legitimate
//!   renames -- it stays unguarded on purpose.
//!
//! Everything else -- a plain file, a missing non-trio path, a bare
//! directory -- is noise. This is deliberately accurate rather than
//! precise: it wakes a cycle for every folder that appears or is torn
//! down and stays quiet for foreign churn, without consulting the
//! binding table or walking subtrees. Two structural changes carry
//! neither signal at the candidate path and so wait for the next
//! bootstrap or reconnect full scan: a folder moved *out* of the tree,
//! and a nested tree moved *in* under a bare (non-maildir) parent.

use std::path::Path;

use crate::maildir_ops::{sentinel, store};

/// Whether `candidate`'s last component is a maildir subdir name.
/// The trio is created and torn down as whole directories, so an event
/// on one of these names marks a folder appearing or being removed.
/// Message traffic touches files inside `cur/`, never the `cur`
/// directory itself, so this stays a clean structural signal.
fn is_trio_path(candidate: &Path) -> bool {
    matches!(
        candidate.file_name().and_then(|n| n.to_str()),
        Some("cur" | "new" | "tmp")
    )
}

/// Whether a trio candidate's parent folder carries jma's binding
/// sentinel. `candidate` is a `cur`/`new`/`tmp` path; its parent is
/// the folder that would hold `.jma.mapping`. A root-level trio
/// (`<root>/cur`) has the maildir root as its parent, which jma never
/// sentinels, so this is `false` there -- the depth-1 case falls
/// through to firing, same as an unbound folder.
fn trio_parent_is_sentineled(candidate: &Path) -> bool {
    candidate
        .parent()
        .is_some_and(|parent| sentinel::sentinel_path_for(parent).exists())
}

/// Per-candidate verdict: does this event mark a folder created,
/// renamed, moved in, or torn down?
///
/// - The maildir root itself, or a path not under it, never fires.
/// - A `cur`/`new`/`tmp` path event fires iff the trio is gone (a
///   folder being torn down) or its parent folder has no
///   `.jma.mapping` sentinel (a folder jma hasn't bound, i.e.
///   genuinely new). An intact trio under a sentinel'd folder is
///   churn echo -- FSEvents can roll heavy in-`cur/` bookkeeping
///   (renames and hardlinks that never leave the folder) up into a
///   directory-level event naming `cur` itself, and a folder jma has
///   already bound can be neither created nor torn down while its
///   trio is still there.
/// - Any other path fires iff it is a maildir now (`store::is_maildir`)
///   -- a folder that appeared by create, rename, or move, which a
///   rename delivers atomically without trio events. This arm stays
///   unguarded by the sentinel: a renamed folder carries its sentinel
///   with it, so gating here would suppress legitimate renames.
/// - Everything else (sidecar files, index directories, missing
///   non-trio paths, bare directories) is noise.
fn fires(candidate: &Path, maildir_root: &Path) -> bool {
    let Ok(rel) = candidate.strip_prefix(maildir_root) else {
        return false;
    };
    if rel.as_os_str().is_empty() {
        return false;
    }
    if is_trio_path(candidate) {
        return !candidate.exists() || !trio_parent_is_sentineled(candidate);
    }
    store::is_maildir(candidate)
}

/// Whether any candidate in the batch marks a structural change. One
/// live signal is enough: path-scoped scanning can't classify
/// structural drift, so the watcher promotes the whole batch to a
/// `LocalStructuralChange` trigger (a full-scope cycle). A batch
/// where every candidate is noise produces no structural trigger --
/// the watcher falls back to the batch's message paths, or to
/// sending nothing.
pub fn batch_fires(candidates: &[impl AsRef<Path>], maildir_root: &Path) -> bool {
    candidates.iter().any(|c| fires(c.as_ref(), maildir_root))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maildir_ops::store::ensure_maildir;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn mkdir(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
    }

    /// Drop a `.jma.mapping` sentinel into `folder`, as if jma had
    /// already bound it. Content is irrelevant to the classifier,
    /// which only checks presence, so this skips the full
    /// `sentinel::write` + `MailboxMapping` machinery in favour of a
    /// bare file at the real sentinel path.
    fn write_sentinel(folder: &Path) {
        std::fs::write(sentinel::sentinel_path_for(folder), b"").unwrap();
    }

    /// A maildir on disk fires -- the appearance signal covers creates,
    /// renames (on the new name), and moving a maildir folder in.
    #[test]
    fn maildir_folder_fires() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let inbox = root.join("INBOX");
        ensure_maildir(&inbox).unwrap();

        assert!(batch_fires(&[inbox], root));
    }

    /// A `cur`/`new`/`tmp` path event fires on the name alone, whether
    /// or not the folder still exists -- this is the teardown signal,
    /// and the path is typically already gone by classify time.
    #[test]
    fn trio_path_event_fires_even_when_gone() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let gone_trio = root.join("Gone").join("cur");

        assert!(batch_fires(&[gone_trio], root));
    }

    /// A recursive delete emits the folder's trio dirs as their own
    /// events; the folder itself stats as gone, but the trio signal
    /// fires the batch.
    #[test]
    fn recursive_delete_fires_via_trio() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let inbox = root.join("INBOX");
        let candidates = vec![
            inbox.clone(),
            inbox.join("cur"),
            inbox.join("new"),
            inbox.join("tmp"),
        ];

        assert!(batch_fires(&candidates, root));
    }

    /// A rename fires on the new name (a maildir on disk); the old name
    /// stats as gone and does not carry a trio event, but the new name
    /// is enough.
    #[test]
    fn rename_fires_on_new_name() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let archive = root.join("Archive");
        ensure_maildir(&archive).unwrap();
        let old_gone = root.join("INBOX");

        assert!(batch_fires(&[archive, old_gone], root));
    }

    /// A rename lands the sentinel at the new name along with the
    /// trio; the sentinel must not suppress the `is_maildir` arm that
    /// detects the rename -- that gate is trio-only.
    #[test]
    fn rename_of_sentineled_folder_fires_on_new_name() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let archive = root.join("Archive");
        ensure_maildir(&archive).unwrap();
        write_sentinel(&archive);
        let old_gone = root.join("INBOX");

        assert!(batch_fires(&[archive, old_gone], root));
    }

    /// The incident class: FSEvents can coalesce heavy in-`cur/`
    /// churn (e.g. Gnus/nnmaildir renaming into `cur/` and
    /// hardlinking out to `.nnmaildir/num/`) up into one event naming
    /// `cur` itself. On a folder jma has already bound (sentinel
    /// present) and whose trio is still intact, that event is not a
    /// creation or a teardown -- it's noise, and must not fire.
    #[test]
    fn trio_event_on_sentineled_folder_is_noise() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let inbox = root.join("INBOX");
        ensure_maildir(&inbox).unwrap();
        write_sentinel(&inbox);

        assert!(!batch_fires(&[inbox.join("cur")], root));
    }

    /// Same shape without a sentinel: the folder is genuinely new (or
    /// not yet bound), so the trio event must still fire -- the gate
    /// only silences churn on folders jma already knows about.
    #[test]
    fn trio_event_on_unsentineled_folder_fires() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let inbox = root.join("INBOX");
        ensure_maildir(&inbox).unwrap();

        assert!(batch_fires(&[inbox.join("cur")], root));
    }

    /// A missing trio always fires regardless of the sentinel: a
    /// bound folder's teardown is exactly the case the trio signal
    /// exists to catch, so the sentinel check is short-circuited by
    /// the existence check first.
    #[test]
    fn teardown_of_sentineled_folder_fires() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let inbox = root.join("INBOX");
        ensure_maildir(&inbox).unwrap();
        write_sentinel(&inbox);
        let candidates = vec![
            inbox.join("cur"),
            inbox.join("new"),
            inbox.join("tmp"),
            inbox.clone(),
        ];
        std::fs::remove_dir_all(&inbox).unwrap();

        assert!(batch_fires(&candidates, root));
    }

    /// Foreign index churn (notmuch's `.notmuch/xapian/`) is files and
    /// non-maildir directories, with no trio event anywhere -- the
    /// batch is noise. This is the false positive the classifier
    /// exists to kill.
    #[test]
    fn index_directory_batch_is_noise() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let xapian = root.join(".notmuch").join("xapian");
        mkdir(&xapian);
        let glass = xapian.join("postlist.glass");
        std::fs::write(&glass, b"x").unwrap();

        assert!(!batch_fires(&[glass, xapian, root.join(".notmuch")], root));
    }

    /// A sidecar file at folder level (`.uidvalidity`) is not a maildir
    /// and not a trio event, so it is noise.
    #[test]
    fn sidecar_file_is_noise() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let inbox = root.join("INBOX");
        ensure_maildir(&inbox).unwrap();
        let sidecar = inbox.join(".uidvalidity");
        std::fs::write(&sidecar, b"1").unwrap();

        assert!(!batch_fires(&[sidecar], root));
    }

    /// A missing non-trio path (an index temp file renamed away, or a
    /// folder moved out of the tree) carries neither signal and is
    /// noise.
    #[test]
    fn missing_non_trio_path_is_noise() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let gone = root.join("NeverExisted");

        assert!(!batch_fires(&[gone], root));
    }

    /// A bare directory (an ordinary `mkdir` that never got a trio) is
    /// not a maildir and carries no trio event, so it is noise.
    #[test]
    fn bare_mkdir_is_noise() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let bare = root.join("JustMade");
        mkdir(&bare);

        assert!(!batch_fires(&[bare], root));
    }

    /// Accepted limitation, pinned: a nested tree moved in under a bare
    /// (non-maildir) parent emits one event on the bare top, which is
    /// neither a maildir nor a trio event -- noise here, recovered by
    /// the next full scan.
    #[test]
    fn nested_tree_under_bare_parent_is_noise() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let bare = root.join("Personal");
        ensure_maildir(&bare.join("Notes")).unwrap();
        // The moved-in event lands on the bare top, not the nested
        // maildir.
        assert!(!batch_fires(std::slice::from_ref(&bare), root));
    }

    /// The maildir signal is position- and name-independent: a maildir
    /// fires whether at depth 1, nested at depth 2, or dot-prefixed.
    #[test]
    fn maildir_fires_regardless_of_depth_or_name() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let top = root.join("Archive");
        ensure_maildir(&top).unwrap();
        let nested = root.join("Outer").join("Inner");
        ensure_maildir(&nested).unwrap();
        let dotted = root.join(".Archive.2024");
        ensure_maildir(&dotted).unwrap();

        for candidate in [top, nested, dotted] {
            assert!(
                batch_fires(std::slice::from_ref(&candidate), root),
                "maildir candidate {} should fire",
                candidate.display()
            );
        }
    }

    /// The maildir root itself never fires; a structural event on the
    /// root has an empty relative path.
    #[test]
    fn root_itself_is_noise() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        assert!(!batch_fires(&[root.to_path_buf()], root));
    }

    /// A path outside the maildir root never fires.
    #[test]
    fn path_outside_root_is_noise() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("Mail");
        let outside: PathBuf = tmp.path().join("Elsewhere").join("cur");

        assert!(!batch_fires(&[outside], &root));
    }
}
