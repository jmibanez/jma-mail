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
            Some((_known_folder, known_flags)) => {
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

        if !known_state.contains_key(&maildir_id) {
            debug!("New message in new/: {}", maildir_id);
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
