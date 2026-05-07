use anyhow::{Context, Result};
use maildir::Maildir;
use std::path::Path;
use tracing::debug;

use crate::ids::MaildirId;

/// Ensure a maildir folder exists with cur/new/tmp subdirectories.
pub fn ensure_maildir(path: &Path) -> Result<Maildir> {
    let md = Maildir::from(path.to_path_buf());
    md.create_dirs()
        .with_context(|| format!("Failed to create maildir dirs at {}", path.display()))?;
    Ok(md)
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
    debug!("Deleted message {}", id);
    Ok(())
}

/// Set flags on a message (replaces all existing flags).
pub fn set_flags(maildir: &Maildir, id: &str, flags: &str) -> Result<()> {
    maildir
        .set_flags(id, flags)
        .with_context(|| format!("Failed to set flags on message {}", id))?;
    debug!("Set flags '{}' on message {}", flags, id);
    Ok(())
}

/// Move a message from one maildir to another.
pub fn move_message(from: &Maildir, to: &Maildir, id: &str) -> Result<()> {
    from.move_to(id, to)
        .with_context(|| format!("Failed to move message {}", id))?;
    debug!("Moved message {} to {:?}", id, to.path());
    Ok(())
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
