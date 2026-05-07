use anyhow::Result;
use maildir::Maildir;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, error};

use crate::ids::{MaildirId, MessageId};
use crate::maildir_ops::headers::parse_message_id_from_file;

/// A change detected in the local maildir.
#[derive(Debug)]
pub enum LocalChange {
    /// A new message file appeared that we don't have in the DB.
    ///
    /// `message_id` is required: scan refuses to emit `NewMessage` for a
    /// file whose `Message-ID` header is missing or unparseable. Without
    /// it, the disposable-state-DB invariant breaks (a wipe + re-sync
    /// would re-upload the file as a fresh server email rather than
    /// adopting the existing one), so the file is dropped at the scan
    /// boundary with an `error!` and stays on disk untouched.
    NewMessage {
        maildir_id: MaildirId,
        folder: String,
        flags: String,
        path: PathBuf,
        message_id: MessageId,
        /// On-disk byte size at scan time, captured by the same
        /// pass that opens the file for Message-ID parsing. Carried
        /// so reconcile can refuse oversized uploads without doing
        /// its own I/O. `0` for files whose size couldn't be
        /// stat'd; reconcile treats that as "let it through and let
        /// the upload path surface the error."
        size_bytes: u64,
    },
    /// A message file we had recorded is now missing.
    DeletedMessage {
        maildir_id: MaildirId,
        folder: String,
    },
    /// The flags on a message file changed.
    FlagsChanged {
        maildir_id: MaildirId,
        folder: String,
        old_flags: String,
        new_flags: String,
    },
}

/// Scan a maildir folder and detect changes vs. the known state.
///
/// `known_state` maps maildir_id -> (folder, flags) from the DB.
pub fn scan_folder(
    maildir: &Maildir,
    folder_name: &str,
    known_state: &HashMap<MaildirId, (String, String)>,
) -> Result<(Vec<LocalChange>, Vec<MaildirId>)> {
    let mut changes = Vec::new();
    let mut seen_ids = Vec::new();

    // Scan cur/ directory
    for entry in maildir.list_cur() {
        let entry = entry?;
        let maildir_id = MaildirId::from(entry.id());
        let flags = entry.flags().to_string();

        seen_ids.push(maildir_id.clone());

        match known_state.get(&maildir_id) {
            // Maildir-id-preserving cross-folder move: same unique part
            // of the filename, different folder than the DB recorded.
            // Treat the destination side as a NewMessage so reconcile's
            // move pre-pass can pair it (by Message-ID) with the
            // DeletedMessage scan_folder will emit when it walks the
            // original folder. Without this, the folder mismatch is
            // silently swallowed and the move degrades into a destroy
            // + re-upload (or, after partial state drift, a backwards
            // MoveLocal that undoes the user's move).
            Some((known_folder, _)) if known_folder != folder_name => {
                debug!(
                    "Cross-folder rename detected: {} now in {} (was {})",
                    maildir_id, folder_name, known_folder
                );
                let path = entry.path().to_path_buf();
                let Some(message_id) = require_message_id(&maildir_id, &path)? else {
                    continue;
                };
                let size_bytes = stat_size(&path);
                changes.push(LocalChange::NewMessage {
                    maildir_id,
                    folder: folder_name.to_string(),
                    flags,
                    path,
                    message_id,
                    size_bytes,
                });
            }
            Some((_, known_flags)) => {
                if *known_flags != flags {
                    debug!(
                        "Flags changed for {}: '{}' -> '{}'",
                        maildir_id, known_flags, flags
                    );
                    changes.push(LocalChange::FlagsChanged {
                        maildir_id,
                        folder: folder_name.to_string(),
                        old_flags: known_flags.clone(),
                        new_flags: flags,
                    });
                }
            }
            None => {
                debug!("New message in cur/: {}", maildir_id);
                let path = entry.path().to_path_buf();
                let Some(message_id) = require_message_id(&maildir_id, &path)? else {
                    continue;
                };
                let size_bytes = stat_size(&path);
                changes.push(LocalChange::NewMessage {
                    maildir_id,
                    folder: folder_name.to_string(),
                    flags,
                    path,
                    message_id,
                    size_bytes,
                });
            }
        }
    }

    // Scan new/ directory (messages not yet seen)
    for entry in maildir.list_new() {
        let entry = entry?;
        let maildir_id = MaildirId::from(entry.id());

        seen_ids.push(maildir_id.clone());

        // Same logic as the cur/ loop: emit NewMessage when the id is
        // unknown OR known-but-in-a-different-folder (id-preserving
        // cross-folder rename into new/). Files in new/ have no
        // ":2,<flags>" suffix per maildir convention.
        let cross_folder = known_state
            .get(&maildir_id)
            .is_some_and(|(known_folder, _)| known_folder != folder_name);
        if !known_state.contains_key(&maildir_id) || cross_folder {
            if cross_folder {
                debug!(
                    "Cross-folder rename into new/: {} -> {}",
                    maildir_id, folder_name
                );
            } else {
                debug!("New message in new/: {}", maildir_id);
            }
            let path = entry.path().to_path_buf();
            let Some(message_id) = require_message_id(&maildir_id, &path)? else {
                continue;
            };
            let size_bytes = stat_size(&path);
            changes.push(LocalChange::NewMessage {
                maildir_id,
                folder: folder_name.to_string(),
                flags: String::new(),
                path,
                message_id,
                size_bytes,
            });
        }
    }

    // Detect deletions: entries in known_state for this folder that we didn't see
    for (id, (folder, _)) in known_state {
        if folder == folder_name && !seen_ids.contains(id) {
            debug!("Deleted message: {} (was in {})", id, folder);
            changes.push(LocalChange::DeletedMessage {
                maildir_id: id.clone(),
                folder: folder_name.to_string(),
            });
        }
    }

    Ok((changes, seen_ids))
}

/// Capture the file's on-disk byte size, returning 0 if stat fails.
/// Plumbed onto `LocalChange::NewMessage` so reconcile can refuse
/// oversized uploads without doing its own I/O. Stat failures fall
/// through as 0 — we'd rather let the upload path surface a clear
/// per-file error than swallow the change at scan time on transient
/// metadata failures.
fn stat_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Parse the file's `Message-ID` header, returning `Ok(Some(_))` when
/// a usable id is present and `Ok(None)` (with an `error!` log) when
/// the header is missing — the user produced an RFC-violating file
/// that sync can't anchor idempotently, so the caller skips it.
///
/// I/O errors (file vanished mid-scan, EACCES, malformed UTF-8 in a
/// header) propagate as `Err` and abort the scan: those are systemic
/// problems the user can't fix on a per-file basis, and continuing
/// past them risks misclassifying transient failures as "user data
/// problem."
fn require_message_id(maildir_id: &MaildirId, path: &Path) -> Result<Option<MessageId>> {
    match parse_message_id_from_file(path)? {
        Some(mid) => Ok(Some(mid)),
        None => {
            error!(
                "Skipping {} ({}): no Message-ID header. \
                 jma requires Message-ID to anchor idempotency; \
                 fix the file or remove it.",
                maildir_id,
                path.display()
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maildir_ops::store::ensure_maildir;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn write_message(dir: &Path, sub: &str, filename: &str, body: &str) {
        let path = dir.join(sub).join(filename);
        fs::write(&path, body).unwrap();
    }

    /// Maildir-id-preserving cross-folder move: the spec recommends that
    /// movers preserve the unique part of the filename. When that
    /// happens, scan_folder must emit a NewMessage on the destination
    /// side so reconcile's move pre-pass can pair it with the
    /// DeletedMessage emitted on the source side. Without this, the
    /// folder mismatch is silently swallowed at the flags-match branch
    /// and the move degrades into a destroy + re-upload (or, worse, a
    /// backwards MoveLocal once DB and server diverge).
    #[test]
    fn cross_folder_id_preserving_move_emits_new_on_destination() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let spam_path = tmp.path().join("Spam");
        let _inbox = ensure_maildir(&inbox_path).unwrap();
        let spam = ensure_maildir(&spam_path).unwrap();

        // The MUA preserved the unique part across folders.
        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,FS");
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        write_message(&spam_path, "cur", &filename, body);

        // DB still believes the file lives in INBOX with the same flags
        // (the MUA didn't change them, only the folder).
        let mut known = HashMap::new();
        known.insert(unique.into(), ("INBOX".to_string(), "FS".to_string()));

        let (changes, _seen) = scan_folder(&spam, "Spam", &known).unwrap();

        assert_eq!(
            changes.len(),
            1,
            "destination scan must emit one change for an id-preserving move, got {:?}",
            changes
        );
        match &changes[0] {
            LocalChange::NewMessage {
                maildir_id,
                folder,
                message_id,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(folder, "Spam");
                assert_eq!(message_id.as_ref(), "a@x");
            }
            other => panic!("expected NewMessage on destination scan, got {:?}", other),
        }
    }

    /// The source-side scan during the same move: file is gone from
    /// INBOX, scan_folder must emit DeletedMessage(INBOX). Reconcile's
    /// move pre-pass will pair this with the destination-side
    /// NewMessage by Message-ID. Regression guard for the existing
    /// deletion detection.
    #[test]
    fn cross_folder_id_preserving_move_emits_deleted_on_source() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();
        // INBOX is empty (file was renamed away).

        let unique = "1700000000.M1.host";
        let mut known = HashMap::new();
        known.insert(unique.into(), ("INBOX".to_string(), "FS".to_string()));

        let (changes, _seen) = scan_folder(&inbox, "INBOX", &known).unwrap();

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::DeletedMessage { maildir_id, folder } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(folder, "INBOX");
            }
            other => panic!("expected DeletedMessage on source scan, got {:?}", other),
        }
    }

    /// Same folder, same maildir_id, different flags: still emits
    /// FlagsChanged (not NewMessage). Guards the fix from
    /// over-triggering when only the suffix changed.
    #[test]
    fn same_folder_flag_change_emits_flags_changed() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,FS");
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        write_message(&inbox_path, "cur", &filename, body);

        let mut known = HashMap::new();
        known.insert(unique.into(), ("INBOX".to_string(), "F".to_string()));

        let (changes, _seen) = scan_folder(&inbox, "INBOX", &known).unwrap();

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::FlagsChanged {
                maildir_id,
                old_flags,
                new_flags,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(old_flags, "F");
                assert_eq!(new_flags, "FS");
            }
            other => panic!("expected FlagsChanged, got {:?}", other),
        }
    }

    /// Maildir-id-regenerating cross-folder move (Gnus's nnmaildir,
    /// mu4e, mbsync's own moves). The destination file has a brand-new
    /// unique part — scan must emit DeletedMessage(src) and
    /// NewMessage(dst) sharing only the Message-ID, so reconcile's
    /// move pre-pass can pair them. This is the path the move pre-pass
    /// was originally written for; this test guards the scan-layer end
    /// of it. Combined with the reconcile-side
    /// `cross_folder_local_move_emits_move_remote_and_adopt` test,
    /// this validates the full Gnus-style move pipeline.
    #[test]
    fn cross_folder_id_changing_move_emits_both_halves() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let spam_path = tmp.path().join("Spam");
        let inbox = ensure_maildir(&inbox_path).unwrap();
        let spam = ensure_maildir(&spam_path).unwrap();
        // INBOX is empty (Gnus removed the source file as part of the move).
        // Spam has the new file with a fresh unique id.
        let old_id = "1700000000.M1.host";
        let new_id = "1700000001.M2.host";
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        let new_filename = format!("{new_id}:2,FS");
        write_message(&spam_path, "cur", &new_filename, body);

        // DB still believes the file is in INBOX with its old id.
        let mut known = HashMap::new();
        known.insert(old_id.into(), ("INBOX".to_string(), "FS".to_string()));

        let (inbox_changes, _) = scan_folder(&inbox, "INBOX", &known).unwrap();
        let (spam_changes, _) = scan_folder(&spam, "Spam", &known).unwrap();

        assert_eq!(
            inbox_changes.len(),
            1,
            "expected one DeletedMessage on INBOX"
        );
        match &inbox_changes[0] {
            LocalChange::DeletedMessage { maildir_id, folder } => {
                assert_eq!(maildir_id.as_ref(), old_id);
                assert_eq!(folder, "INBOX");
            }
            other => panic!("expected DeletedMessage, got {:?}", other),
        }

        assert_eq!(spam_changes.len(), 1, "expected one NewMessage on Spam");
        match &spam_changes[0] {
            LocalChange::NewMessage {
                maildir_id,
                folder,
                message_id,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), new_id);
                assert_eq!(folder, "Spam");
                assert_eq!(message_id.as_ref(), "a@x");
            }
            other => panic!("expected NewMessage, got {:?}", other),
        }
    }

    /// Cross-folder move into new/ (MUA stages the file in the
    /// destination's new/ rather than cur/). Same fix path: emit
    /// NewMessage so reconcile pairs it with the source DeletedMessage.
    #[test]
    fn cross_folder_id_preserving_move_into_new_emits_new() {
        let tmp = TempDir::new().unwrap();
        let spam_path = tmp.path().join("Spam");
        let spam = ensure_maildir(&spam_path).unwrap();

        let unique = "1700000000.M1.host";
        // new/ filenames have no :2, suffix.
        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";
        write_message(&spam_path, "new", unique, body);

        let mut known = HashMap::new();
        known.insert(unique.into(), ("INBOX".to_string(), "".to_string()));

        let (changes, _seen) = scan_folder(&spam, "Spam", &known).unwrap();

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::NewMessage {
                maildir_id,
                folder,
                message_id,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(folder, "Spam");
                assert_eq!(message_id.as_ref(), "a@x");
            }
            other => panic!("expected NewMessage on new/ scan, got {:?}", other),
        }
    }

    /// A file with no Message-ID header (RFC 5322 says it SHOULD be
    /// present, but isn't a hard MUST) must NOT be ingested: jma
    /// anchors idempotency on Message-ID, and emitting NewMessage
    /// without one would either drop the message at reconcile or
    /// produce a server-side duplicate after a state DB wipe. Scan
    /// drops the file (logs error!) so it stays on disk untouched.
    #[test]
    fn scan_folder_skips_file_without_message_id() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();

        // No Message-ID header — only a Subject + body.
        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,");
        let body = "Subject: no msgid\r\n\r\nbody\r\n";
        write_message(&inbox_path, "cur", &filename, body);

        let known = HashMap::new();
        let (changes, seen) = scan_folder(&inbox, "INBOX", &known).unwrap();

        assert!(
            changes.is_empty(),
            "expected no LocalChanges for a file without Message-ID, got {:?}",
            changes
        );
        // The file is still seen on disk, so it counts as observed —
        // we just refuse to emit a NewMessage for it.
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].as_ref(), unique);
    }

    /// Same folder, file already known to the DB, but the file's
    /// Message-ID header is missing. This hits the
    /// `Some((_, known_flags))` branch where `require_message_id`
    /// isn't even called (flags match), so no `NewMessage` is emitted
    /// — but more importantly: the maildir_id IS pushed onto
    /// `seen_ids` before any matching, so the trailing
    /// deletion-detection loop must NOT spuriously emit a
    /// DeletedMessage for it. Pins the "seen_ids tracks every walked
    /// file regardless of whether we emit a change for it" invariant.
    #[test]
    fn scan_folder_known_file_without_message_id_emits_nothing() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,FS");
        let body = "Subject: no msgid\r\n\r\nbody\r\n";
        write_message(&inbox_path, "cur", &filename, body);

        // DB has the file in the same folder with the same flags.
        let mut known = HashMap::new();
        known.insert(unique.into(), ("INBOX".to_string(), "FS".to_string()));

        let (changes, _seen) = scan_folder(&inbox, "INBOX", &known).unwrap();

        assert!(
            changes.is_empty(),
            "no changes expected for an unchanged known file, got {:?}",
            changes
        );
    }

    /// Destination-side scan of a Message-ID-less cross-folder move:
    /// the cross-folder branch must run `require_message_id` before
    /// emitting NewMessage. With no Message-ID, the move pre-pass in
    /// reconcile (which keys on Message-ID) couldn't pair this with
    /// any source-side delete anyway, so we refuse at the scan
    /// boundary and the destination produces zero changes.
    #[test]
    fn scan_folder_skips_cross_folder_move_without_message_id() {
        let tmp = TempDir::new().unwrap();
        let spam_path = tmp.path().join("Spam");
        let spam = ensure_maildir(&spam_path).unwrap();

        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,FS");
        // No Message-ID header.
        let body = "Subject: no msgid\r\n\r\nbody\r\n";
        write_message(&spam_path, "cur", &filename, body);

        // DB believes the file lives in INBOX.
        let mut known = HashMap::new();
        known.insert(unique.into(), ("INBOX".to_string(), "FS".to_string()));

        let (changes, _seen) = scan_folder(&spam, "Spam", &known).unwrap();

        assert!(
            changes.is_empty(),
            "expected no LocalChanges for a Message-ID-less cross-folder move, got {:?}",
            changes
        );
    }
}
