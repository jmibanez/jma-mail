//! Cached, selected flag history for offline status. This does not contact the
//! server, acknowledge a generation, or claim all mailbox placement is settled.

use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;
use std::fmt;

use crate::config::SyncConfig;
use crate::jmap::mailbox::{MailboxSelectionInput, get_selected_mailboxes};
use crate::state::{flag_sync, queries};

#[derive(Debug)]
pub struct CachedFlagStatus {
    pub flags: flag_sync::FlagPending,
    /// With a configured filter, missing cached paths cannot be guessed into
    /// or out of the selected scope. Surface that uncertainty explicitly.
    pub unclassified_mailboxes: usize,
}

impl fmt::Display for CachedFlagStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Flag histories (cached selected bindings): {} pending, {} blocked/review, {} unverified",
            self.flags.pending.len(),
            self.flags.blocked.len(),
            self.flags.unverified.len()
        )?;
        if self.unclassified_mailboxes > 0 {
            write!(
                f,
                "; {} bound mailbox(es) lack cached paths and could not be classified",
                self.unclassified_mailboxes
            )?;
        }
        Ok(())
    }
}

pub fn cached_flag_status(conn: &Connection, sync: &SyncConfig) -> Result<CachedFlagStatus> {
    let bound = flag_sync::bound_mailbox_ids(conn)?;
    if sync.mailboxes.is_empty() {
        return Ok(CachedFlagStatus {
            flags: flag_sync::summarize(conn, &bound)?,
            unclassified_mailboxes: 0,
        });
    }
    let mailboxes = queries::get_all_mailboxes(conn)?;
    let inputs: Vec<_> = mailboxes
        .iter()
        .filter_map(|mailbox| {
            mailbox
                .remote_path
                .as_deref()
                .map(|path| MailboxSelectionInput {
                    path,
                    role: mailbox.role.as_deref(),
                })
        })
        .collect();
    let selected =
        get_selected_mailboxes(&sync.mailboxes, &inputs, sync.case_insensitive_match, true);
    let paths: HashMap<_, _> = mailboxes
        .iter()
        .map(|mailbox| (&mailbox.jmap_mailbox_id, mailbox.remote_path.as_deref()))
        .collect();
    let mut selected_ids = Vec::new();
    let mut unclassified_mailboxes = 0;
    for id in bound {
        match paths.get(&id).copied().flatten() {
            Some(path) if selected.contains(path) => selected_ids.push(id),
            Some(_) => {}
            None => unclassified_mailboxes += 1,
        }
    }
    Ok(CachedFlagStatus {
        flags: flag_sync::summarize(conn, &selected_ids)?,
        unclassified_mailboxes,
    })
}
