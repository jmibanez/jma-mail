use anyhow::{Context, Result};
use maildir::{Maildir, TemporaryMailFile};
use std::path::Path;
use tracing::{Level, debug, event};

use crate::ids::MaildirId;
use crate::maildir_ops::entries;
use crate::profile::TARGET_FILE_OP;

/// Ensure a maildir folder exists with cur/new/tmp subdirectories.
pub fn ensure_maildir(path: &Path) -> Result<Maildir> {
    let md = Maildir::from(path.to_path_buf());
    md.create_dirs()
        .with_context(|| format!("Failed to create maildir dirs at {}", path.display()))?;
    Ok(md)
}

/// Non-creating handle for either full or partial Maildir shape. A missing
/// peer is not an absent folder. Authoritative enumeration must use entries::read
/// so an unreadable present peer is not silently treated as empty.
pub fn try_open_maildir(path: &Path) -> Option<Maildir> {
    if is_maildir(path) {
        Some(Maildir::from(path.to_path_buf()))
    } else {
        None
    }
}

/// Whether `path` is a maildir folder present on disk: it has a `cur/`
/// or a `new/` subdirectory. A folder with only `new/` is a maildir
/// whose (empty) `cur/` was removed -- it is still present and may hold
/// mail, so it counts. This is the single source of truth for "is this
/// a maildir on disk," shared by the drift walk and the `present`
/// predicate so they can't drift apart.
///
/// This is a presence predicate, not proof that present peers can be read.
/// entries::read performs that validation without healing directories.
pub fn is_maildir(path: &Path) -> bool {
    path.join("cur").is_dir() || path.join("new").is_dir()
}

/// Store a raw email message into a maildir, routing unseen messages
/// (no `S` in `flags`) to `new/` and seen messages to `cur/`. The
/// `:2,<flags>` info suffix is appended in both subfolders so that
/// non-Seen flags set by server-side filters (e.g. `\Flagged` from a
/// Sieve `imap4flags` rule) survive the MUA's first new/ -> cur/
/// promotion. Returns the maildir unique ID assigned to the message.
///
/// Strict maildir says files in `new/` carry no info suffix. We side
/// with mbsync and OfflineIMAP, which preserve the suffix in `new/`
/// anyway -- without it, an MUA's promotion rebuilds the suffix as
/// just `S`, scan sees the non-Seen flags drop off disk, and reconcile
/// pushes "remove $flagged" (etc.) back to the server, silently
/// undoing the filter that set the flag in the first place. Dovecot's
/// own writer doesn't produce this shape (it routes flagged unseen
/// mail to cur/ instead), but its reader accepts suffix-bearing
/// new/ files unchanged.
///
/// Both branches go through one tmp/ -> destination rename so the
/// file becomes visible to MUAs in `new/` (or `cur/`) only with its
/// final filename in place. A bare-then-renamed-with-suffix sequence
/// would expose a window in which an MUA could promote the bare
/// new/ file to cur/ before the suffix landed.
pub fn store_message(maildir: &Maildir, data: &[u8], flags: &str) -> Result<MaildirId> {
    let (id, subdir) = if flags.contains('S') {
        let id = maildir
            .store_cur_with_flags(data, flags)
            .context("Failed to store message in maildir cur/")?;
        (id, "cur")
    } else {
        let id = maildir
            .store_new_with_flags(data, flags)
            .context("Failed to store message in maildir new/")?;
        (id, "new")
    };
    event!(target: TARGET_FILE_OP, Level::TRACE, op = "store");
    debug!(
        "Stored message {} with flags '{}' in {}/",
        id, flags, subdir
    );
    Ok(MaildirId::from(id))
}

/// Open a fresh `tmp/` file in the maildir, returning an owned handle
/// that callers stream bytes into via its `Write` impl. The handle's
/// `Drop` unlinks the file unless `finalize_message` consumed it, so
/// abandoned downloads (cancellation, mid-stream HTTP failure) don't
/// leak.
///
/// Paired with `finalize_message` to split the buffered `store_message`
/// path into a producer (open then stream) and consumer (fsync, rename,
/// and DB commit) for the JMAP download pipeline. The upstream call
/// uses the same `secs.#counterMnanosPpid.hostname` filename scheme
/// and EEXIST-retry uniqueness loop that the buffered path does,
/// so parallel openers in the same maildir stay collision-safe.
pub fn open_tmp(maildir: &Maildir) -> Result<TemporaryMailFile> {
    maildir
        .open_file_in_tmp()
        .context("Failed to open tmp/ file in maildir")
}

/// Promote a tmp-streamed file into `new/` (or `cur/` when `S` is in
/// `flags`), matching the routing in `store_message` for the buffered
/// path. The upstream call performs the `fsync_file` (F_BARRIERFSYNC
/// on Apple with the `barrier_fsync` feature, plain `sync_all`
/// elsewhere) and the tmp -> destination rename; on success the
/// `TemporaryMailFile` Drop guard is disarmed, on failure it unlinks
/// the orphan tmp file.
pub fn finalize_message(
    maildir: &Maildir,
    tmp: TemporaryMailFile,
    flags: &str,
) -> Result<MaildirId> {
    let (id, subdir) = if flags.contains('S') {
        let id = maildir
            .move_tmp_to_cur_with_flags(tmp, flags)
            .context("Failed to promote tmp file into maildir cur/")?;
        (id, "cur")
    } else {
        let id = maildir
            .move_tmp_to_new_with_flags(tmp, flags)
            .context("Failed to promote tmp file into maildir new/")?;
        (id, "new")
    };
    event!(target: TARGET_FILE_OP, Level::TRACE, op = "store");
    debug!(
        "Stored message {} with flags '{}' in {}/",
        id, flags, subdir
    );
    Ok(MaildirId::from(id))
}

/// Delete a message from a maildir by its unique ID.
pub fn delete_message(maildir: &Maildir, id: &str) -> Result<()> {
    maildir
        .delete(id)
        .with_context(|| format!("Failed to delete message {}", id))?;
    event!(target: TARGET_FILE_OP, Level::TRACE, op = "delete");
    debug!("Deleted message {}", id);
    Ok(())
}

/// Set flags on a message (replaces all existing flags). Only
/// applies to messages already in `cur/`; the maildir crate's
/// underlying `set_flags` won't find a `new/` file. Sync effects should
/// use `set_flags_if_unchanged` to validate their captured generation
/// and handle either subdirectory without overwriting a newer MUA edit.
pub fn set_flags(maildir: &Maildir, id: &str, flags: &str) -> Result<()> {
    maildir
        .set_flags(id, flags)
        .with_context(|| format!("Failed to set flags on message {}", id))?;
    event!(target: TARGET_FILE_OP, Level::TRACE, op = "set_flags");
    debug!("Set flags '{}' on message {}", flags, id);
    Ok(())
}

/// Apply a flag decision only to the filename generation it observed.
/// A newer MUA rename is left alone for reconciliation, not overwritten by
/// searching for the latest suffix. Unseen new-side messages stay in new/.
pub fn set_flags_if_unchanged(
    maildir: &Maildir,
    id: &str,
    expected: &str,
    flags: &str,
) -> Result<()> {
    let mut found = Vec::new();
    for entry in entries::read(maildir.path())? {
        let entry = entry?;
        if entry.maildir_id.as_ref() == id && entry.path.is_file() {
            found.push(entry);
        }
    }
    if found.len() != 1 {
        anyhow::bail!("Expected one local path for {id}, found {}", found.len());
    }
    let entry = found.pop().unwrap();
    if entry.flags != expected {
        anyhow::bail!("Local flags for {id} changed since observation; replanning required");
    }
    let source = &entry.path;
    let subdir = if entry.in_new && !flags.contains('S') {
        "new"
    } else {
        "cur"
    };
    let target = maildir.path().join(subdir).join(format!("{id}:2,{flags}"));
    if source != &target {
        if target.exists() {
            anyhow::bail!(
                "Refusing to overwrite existing flag target {}",
                target.display()
            );
        }
        ensure_message_subdir(target.parent().unwrap())?;
        std::fs::rename(source, &target)
            .with_context(|| format!("Failed to apply flag snapshot for {id}"))?;
        #[cfg(unix)]
        {
            std::fs::File::open(source.parent().unwrap())?.sync_all()?;
            if source.parent() != target.parent() {
                std::fs::File::open(target.parent().unwrap())?.sync_all()?;
            }
        }
    }
    event!(target: TARGET_FILE_OP, Level::TRACE, op = "set_flags");
    Ok(())
}

/// True if the message with this id currently lives in `new/`. Used
/// for self-write-cache predictions, not as mutation authorization.
/// The snapshot-aware setter performs its own unique-path validation.
pub fn message_is_in_new(maildir: &Maildir, id: &str) -> bool {
    maildir
        .list_new()
        .any(|e| e.map(|entry| entry.id() == id).unwrap_or(false))
}

/// Explicitly promote a message from `new/` to `cur/`, attaching
/// the given suffix. Unlike `set_flags`, this searches `new/`.
/// Ordinary flag synchronization uses `set_flags_if_unchanged` instead:
/// a non-Seen keyword change alone does not require promotion.
///
/// Refuses to clobber an existing cur/-side file at the destination
/// path. That's an illegal-but-observed state (typically left over
/// by a crash mid-promotion in a prior MUA or sync run): silently
/// overwriting it would lose data, and `fs::rename` on POSIX is
/// happy to do exactly that. Warn-and-skip in the caller is the
/// right recovery -- the next cycle re-evaluates with both files
/// visible to dedupe.
///
/// The `list_new` + `fs::rename` pair is TOCTOU with concurrent MUA
/// promotions of the same id. An ENOENT during the rename surfaces
/// as a context'd error and bubbles up cleanly through
/// `apply_update_local_flags`'s warn-and-skip path; the next sync
/// cycle catches up.
pub fn promote_to_cur_with_flags(maildir: &Maildir, id: &str, flags: &str) -> Result<()> {
    let entry = entries::read(maildir.path())?
        .find_map(|e| match e {
            Ok(entry) if entry.in_new && entry.maildir_id.as_ref() == id => Some(Ok(entry)),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })
        .transpose()
        .with_context(|| format!("iterate new/ in {}", maildir.path().display()))?
        .with_context(|| {
            format!(
                "Message {} not found in {}/new/",
                id,
                maildir.path().display()
            )
        })?;
    let src = entry.path;
    let dest = maildir.path().join("cur").join(format!("{id}:2,{flags}"));
    if dest.exists() {
        return Err(anyhow::anyhow!(
            "Refusing to promote {} to {}: destination already exists \
             (illegal maildir state, likely a prior crash mid-promotion); \
             dedupe pass will reconcile on the next cycle",
            id,
            dest.display()
        ));
    }
    ensure_message_subdir(dest.parent().unwrap())?;
    std::fs::rename(&src, &dest).with_context(|| {
        format!(
            "Failed to promote {} from new/ to cur/ at {}",
            id,
            dest.display()
        )
    })?;
    event!(target: TARGET_FILE_OP, Level::TRACE, op = "promote");
    debug!(
        "Promoted message {} from new/ to cur/ with flags '{}'",
        id, flags
    );
    Ok(())
}

/// Move one unique live identity, preserving its actual filename/suffix.
/// Returns the raw moved flags for the local observation mirror. Like the
/// previous Maildir move helper, the destination is cur/, including for a
/// new/-side source. Refuse ambiguity and target collisions rather than
/// choosing a random copy or overwriting it. This is snapshot validation,
/// not atomic exclusion against arbitrary concurrent MUA writers.
pub fn move_message(from: &Maildir, to: &Maildir, id: &str) -> Result<String> {
    let mut found = Vec::new();
    for entry in entries::read(from.path())? {
        let entry = entry?;
        if entry.maildir_id.as_ref() == id && entry.path.is_file() {
            found.push(entry);
        }
    }
    anyhow::ensure!(
        found.len() == 1,
        "Expected one move source for {id}, found {}",
        found.len()
    );
    let entry = found.pop().unwrap();
    let flags = entry.flags.clone();
    let source = &entry.path;
    let filename = source.file_name().context("Move source has no filename")?;
    let target = to.path().join("cur").join(filename);
    anyhow::ensure!(
        source != &target && !target.exists(),
        "Refusing existing move target {}",
        target.display()
    );
    for entry in entries::read(to.path())? {
        anyhow::ensure!(
            entry?.maildir_id.as_ref() != id,
            "Move target already contains identity {id}"
        );
    }
    ensure_message_subdir(target.parent().unwrap())?;
    std::fs::rename(source, &target).with_context(|| format!("Failed to move message {id}"))?;
    #[cfg(unix)]
    {
        std::fs::File::open(source.parent().unwrap())?.sync_all()?;
        std::fs::File::open(target.parent().unwrap())?.sync_all()?;
    }
    event!(target: TARGET_FILE_OP, Level::TRACE, op = "move");
    debug!("Moved message {} to {:?}", id, to.path());
    Ok(flags)
}

/// Only a real message write may provision its destination leaf. Do not use
/// create_dir_all: a concurrently vanished mailbox must not be recreated.
fn ensure_message_subdir(path: &Path) -> Result<()> {
    match std::fs::create_dir(path) {
        Ok(()) => {
            #[cfg(unix)]
            std::fs::File::open(path.parent().context("Message destination has no parent")?)?
                .sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("Cannot create message destination {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the upstream invariant that `Maildir::create_dirs()`
    /// cascades through missing intermediate parents (it calls
    /// `fs::create_dir_all` internally for `cur`/`new`/`tmp`). The
    /// `Fs` folder layout relies on this: a nested mailbox like
    /// `[Airmail]/Sent` materialises as
    /// `<root>/[Airmail]/Sent/{cur,new,tmp}` without `ensure_maildir`
    /// doing any parent scaffolding of its own. If this test goes red
    /// after a `maildir` crate bump or an `ensure_maildir` refactor,
    /// the `Fs` layout is silently broken.
    #[test]
    fn ensure_maildir_creates_nested_path() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("A").join("B").join("C");
        ensure_maildir(&nested).expect("nested ensure_maildir should succeed");
        for sub in ["cur", "new", "tmp"] {
            assert!(
                nested.join(sub).is_dir(),
                "{} should exist after ensure_maildir",
                nested.join(sub).display()
            );
        }
    }

    #[test]
    fn move_snapshot_returns_actual_raw_suffix_from_cur_and_new() {
        for subdir in ["cur", "new"] {
            let dir = tempfile::tempdir().unwrap();
            let from = ensure_maildir(&dir.path().join("from")).unwrap();
            let to = ensure_maildir(&dir.path().join("to")).unwrap();
            let source = from.path().join(subdir).join("id:2,FX");
            std::fs::write(&source, b"same message bytes").unwrap();
            assert_eq!(move_message(&from, &to, "id").unwrap(), "FX");
            assert!(!source.exists());
            assert_eq!(
                std::fs::read(to.path().join("cur/id:2,FX")).unwrap(),
                b"same message bytes"
            );
            assert_eq!(from.list_cur().chain(from.list_new()).count(), 0);
            assert_eq!(to.list_cur().chain(to.list_new()).count(), 1);
        }
    }

    #[test]
    fn move_snapshot_refuses_an_existing_target_identity() {
        let dir = tempfile::tempdir().unwrap();
        let from = ensure_maildir(&dir.path().join("from")).unwrap();
        let to = ensure_maildir(&dir.path().join("to")).unwrap();
        let source = from.path().join("cur/id:2,SX");
        let target = to.path().join("cur/id:2,SX");
        std::fs::write(&source, b"source").unwrap();
        std::fs::write(&target, b"do not overwrite").unwrap();
        assert!(move_message(&from, &to, "id").is_err());
        assert_eq!(std::fs::read(source).unwrap(), b"source");
        assert_eq!(std::fs::read(target).unwrap(), b"do not overwrite");
    }

    #[test]
    fn move_snapshot_refuses_duplicate_source_identities() {
        let dir = tempfile::tempdir().unwrap();
        let from = ensure_maildir(&dir.path().join("from")).unwrap();
        let to = ensure_maildir(&dir.path().join("to")).unwrap();
        let source = from.path().join("cur/id:2,SX");
        let duplicate = from.path().join("new/id:2,FX");
        std::fs::write(&source, b"source").unwrap();
        std::fs::write(&duplicate, b"duplicate").unwrap();
        assert!(move_message(&from, &to, "id").is_err());
        assert!(source.is_file() && duplicate.is_file());
        assert_eq!(to.list_cur().count(), 0);
    }

    #[test]
    fn flag_snapshot_preserves_new_cur_placement_and_suffix_extensions() {
        for (initial, desired, expected_subdir) in [
            ("F", "", "new"),
            ("", "F", "new"),
            ("F", "S", "cur"),
            ("S", "", "cur"),
            ("SX", "FX", "cur"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let md = ensure_maildir(dir.path()).unwrap();
            let id = store_message(&md, b"body", initial).unwrap();
            set_flags_if_unchanged(&md, id.as_ref(), initial, desired).unwrap();
            let expected = dir
                .path()
                .join(expected_subdir)
                .join(format!("{id}:2,{desired}"));
            assert_eq!(std::fs::read(expected).unwrap(), b"body");
            assert_eq!(md.list_cur().chain(md.list_new()).count(), 1);
        }
    }

    #[test]
    fn flag_snapshot_refuses_a_newer_local_edit_or_a_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let md = ensure_maildir(dir.path()).unwrap();
        let id = store_message(&md, b"body", "FS").unwrap();
        assert!(set_flags_if_unchanged(&md, id.as_ref(), "S", "").is_err());
        assert_eq!(md.list_cur().next().unwrap().unwrap().flags(), "FS");
        assert!(set_flags_if_unchanged(&md, "absent", "", "S").is_err());
    }

    #[test]
    fn flag_snapshot_refuses_duplicate_ids_and_existing_targets() {
        let dir = tempfile::tempdir().unwrap();
        let md = ensure_maildir(dir.path()).unwrap();
        let id = store_message(&md, b"body", "S").unwrap();
        let source = dir.path().join("cur").join(format!("{id}:2,S"));
        let duplicate = dir.path().join("new").join(format!("{id}:2,F"));
        std::fs::copy(&source, &duplicate).unwrap();
        assert!(set_flags_if_unchanged(&md, id.as_ref(), "S", "FS").is_err());
        assert!(source.is_file() && duplicate.is_file());
        std::fs::remove_file(duplicate).unwrap();
        let target = dir.path().join("cur").join(format!("{id}:2,FS"));
        std::fs::create_dir(&target).unwrap();
        assert!(set_flags_if_unchanged(&md, id.as_ref(), "S", "FS").is_err());
        assert!(source.is_file() && target.is_dir());
    }

    /// Read the single delivered filename from a maildir subfolder,
    /// failing the test if the subfolder doesn't have exactly one
    /// entry.
    fn only_file_in(subdir: &Path) -> String {
        let entries: Vec<_> = std::fs::read_dir(subdir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries.len(), 1, "{}: {:?}", subdir.display(), entries);
        entries.into_iter().next().unwrap()
    }

    /// Unseen messages with no flags land in `new/`. The maildir
    /// crate's `store_new_with_flags` emits the `:2,` separator
    /// unconditionally (matching mbsync's `maildir_make_flags`),
    /// so the file is `new/<id>:2,` rather than the strict-spec
    /// bare `new/<id>` shape -- the suffix-aware iterator parses
    /// the empty-flags form cleanly and downstream consumers see
    /// `entry.flags() == ""`.
    #[test]
    fn unseen_unflagged_message_lands_in_new() {
        let dir = tempfile::tempdir().unwrap();
        let md = ensure_maildir(dir.path()).unwrap();
        let id = store_message(&md, b"raw body", "").unwrap();
        let id_str: &str = id.as_ref();

        let name = only_file_in(&dir.path().join("new"));
        assert!(
            name.starts_with(id_str),
            "expected file starting with {} in new/, got {:?}",
            id_str,
            name
        );
        assert!(
            name.ends_with(":2,"),
            "expected empty :2, suffix on unseen-unflagged delivery, got {:?}",
            name
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("cur")).unwrap().count(),
            0,
            "cur/ must stay empty for an unseen delivery"
        );
    }

    /// Unseen messages with non-Seen flags (e.g. `F` from a Sieve
    /// imap4flags rule) land in `new/` *with* the `:2,<flags>` suffix.
    /// Without it, the MUA's first new/ -> cur/ promotion would drop
    /// the F flag, scan would emit FlagsChanged, and reconcile would
    /// push "remove $flagged" back to the server -- silently undoing
    /// the server-side filter. mbsync and OfflineIMAP both preserve
    /// the suffix in new/ for this exact reason; Dovecot's reader
    /// accepts the shape too, even though Dovecot's own writer routes
    /// flagged unseen mail to cur/ instead.
    ///
    /// Also pins the atomicity invariant: `tmp/` must be empty after
    /// delivery (the file lands at its final new/ filename via one
    /// rename, no transient intermediate). And the round-trip
    /// invariant: the iterator parses the suffix back into canonical
    /// `entry.id()` + `entry.flags()` so consumers don't need to
    /// re-canonicalise.
    #[test]
    fn unseen_flagged_message_lands_in_new_with_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let md = ensure_maildir(dir.path()).unwrap();
        let id = store_message(&md, b"raw body", "F").unwrap();
        let id_str: &str = id.as_ref();

        let name = only_file_in(&dir.path().join("new"));
        assert!(
            name.starts_with(id_str),
            "expected file starting with {} in new/, got {:?}",
            id_str,
            name
        );
        assert!(
            name.ends_with(":2,F"),
            "expected :2,F suffix on unseen-flagged delivery, got {:?}",
            name
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("cur")).unwrap().count(),
            0,
            "cur/ must stay empty for an unseen delivery"
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0,
            "tmp/ must be empty after delivery (atomic rename, no leftovers)"
        );

        // Round-trip: the iterator should parse the suffix and surface
        // the canonical id + flags, so consumers can match against the
        // DB without re-canonicalising. Pre-iterator-fix, this asserted
        // the broken upstream shape (`entry.id()` = full filename,
        // `entry.flags()` = "").
        let entries: Vec<_> = md.list_new().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id(), id_str);
        assert_eq!(entries[0].flags(), "F");
    }

    /// Seen messages (S in flags) land in `cur/` with the full flags
    /// suffix preserved. Pins the cur-path branch of the routing
    /// decision so a refactor of the conditional doesn't invert it.
    #[test]
    fn seen_message_lands_in_cur_with_flags() {
        let dir = tempfile::tempdir().unwrap();
        let md = ensure_maildir(dir.path()).unwrap();
        let id = store_message(&md, b"raw body", "FS").unwrap();
        let id_str: &str = id.as_ref();

        let name = only_file_in(&dir.path().join("cur"));
        assert!(
            name.starts_with(id_str),
            "expected file starting with {} in cur/, got {:?}",
            id_str,
            name
        );
        assert!(
            name.ends_with(":2,FS"),
            "expected :2,FS suffix on seen delivery, got {:?}",
            name
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("new")).unwrap().count(),
            0,
            "new/ must stay empty for a seen delivery"
        );
    }
}
