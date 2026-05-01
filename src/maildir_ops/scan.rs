use anyhow::Result;
use maildir::Maildir;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::debug;

use crate::maildir_ops::flags::{extract_flags, extract_id};
use crate::maildir_ops::headers::parse_message_id_from_file;

/// A change detected in the local maildir.
#[derive(Debug)]
pub enum LocalChange {
    /// A new message file appeared that we don't have in the DB.
    NewMessage {
        maildir_id: String,
        folder: String,
        flags: String,
        path: PathBuf,
        /// RFC 5322 Message-ID parsed from the file at scan time. None if
        /// the header was missing or unreadable; reconcile treats that as
        /// "not safe to dedupe against the server" and falls through to a
        /// plain upload.
        message_id: Option<String>,
    },
    /// A message file we had recorded is now missing.
    DeletedMessage { maildir_id: String, folder: String },
    /// The flags on a message file changed.
    FlagsChanged {
        maildir_id: String,
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
    known_state: &HashMap<String, (String, String)>,
) -> Result<(Vec<LocalChange>, Vec<String>)> {
    let mut changes = Vec::new();
    let mut seen_ids = Vec::new();

    // Scan cur/ directory
    for entry in maildir.list_cur() {
        let entry = entry?;
        let filename = entry
            .path()
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let maildir_id = extract_id(&filename).to_string();
        let flags = extract_flags(&filename).to_string();

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
                let message_id = match parse_message_id_from_file(&path) {
                    Ok(Some(mid)) => Some(mid),
                    Ok(None) => None,
                    Err(e) => {
                        debug!("Failed to parse Message-ID for {}: {}", maildir_id, e);
                        None
                    }
                };
                changes.push(LocalChange::NewMessage {
                    maildir_id,
                    folder: folder_name.to_string(),
                    flags,
                    path,
                    message_id,
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
                let message_id = match parse_message_id_from_file(&path) {
                    Ok(Some(mid)) => Some(mid),
                    Ok(None) => {
                        debug!("New message {} has no Message-ID header", maildir_id);
                        None
                    }
                    Err(e) => {
                        debug!("Failed to parse Message-ID for {}: {}", maildir_id, e);
                        None
                    }
                };
                changes.push(LocalChange::NewMessage {
                    maildir_id,
                    folder: folder_name.to_string(),
                    flags,
                    path,
                    message_id,
                });
            }
        }
    }

    // Scan new/ directory (messages not yet seen)
    for entry in maildir.list_new() {
        let entry = entry?;
        let filename = entry
            .path()
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let maildir_id = extract_id(&filename).to_string();

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
            let message_id = match parse_message_id_from_file(&path) {
                Ok(Some(mid)) => Some(mid),
                Ok(None) => {
                    debug!("New message {} has no Message-ID header", maildir_id);
                    None
                }
                Err(e) => {
                    debug!("Failed to parse Message-ID for {}: {}", maildir_id, e);
                    None
                }
            };
            changes.push(LocalChange::NewMessage {
                maildir_id,
                folder: folder_name.to_string(),
                flags: String::new(),
                path,
                message_id,
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
        known.insert(unique.to_string(), ("INBOX".to_string(), "FS".to_string()));

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
                assert_eq!(maildir_id, unique);
                assert_eq!(folder, "Spam");
                assert_eq!(message_id.as_deref(), Some("a@x"));
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
        known.insert(unique.to_string(), ("INBOX".to_string(), "FS".to_string()));

        let (changes, _seen) = scan_folder(&inbox, "INBOX", &known).unwrap();

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::DeletedMessage { maildir_id, folder } => {
                assert_eq!(maildir_id, unique);
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
        known.insert(unique.to_string(), ("INBOX".to_string(), "F".to_string()));

        let (changes, _seen) = scan_folder(&inbox, "INBOX", &known).unwrap();

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::FlagsChanged {
                maildir_id,
                old_flags,
                new_flags,
                ..
            } => {
                assert_eq!(maildir_id, unique);
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
        known.insert(old_id.to_string(), ("INBOX".to_string(), "FS".to_string()));

        let (inbox_changes, _) = scan_folder(&inbox, "INBOX", &known).unwrap();
        let (spam_changes, _) = scan_folder(&spam, "Spam", &known).unwrap();

        assert_eq!(
            inbox_changes.len(),
            1,
            "expected one DeletedMessage on INBOX"
        );
        match &inbox_changes[0] {
            LocalChange::DeletedMessage { maildir_id, folder } => {
                assert_eq!(maildir_id, old_id);
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
                assert_eq!(maildir_id, new_id);
                assert_eq!(folder, "Spam");
                assert_eq!(message_id.as_deref(), Some("a@x"));
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
        known.insert(unique.to_string(), ("INBOX".to_string(), "".to_string()));

        let (changes, _seen) = scan_folder(&spam, "Spam", &known).unwrap();

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::NewMessage {
                maildir_id,
                folder,
                message_id,
                ..
            } => {
                assert_eq!(maildir_id, unique);
                assert_eq!(folder, "Spam");
                assert_eq!(message_id.as_deref(), Some("a@x"));
            }
            other => panic!("expected NewMessage on new/ scan, got {:?}", other),
        }
    }
}
