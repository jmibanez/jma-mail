use std::collections::{HashMap, HashSet};

use crate::config::ConflictStrategy;
use crate::maildir_ops::flags::flags_to_keywords;
use crate::maildir_ops::scan::LocalChange;
use crate::state::queries::MessageRecord;
use crate::sync::plan::{SyncAction, SyncPlan};

/// Reconcile remote changes and local changes into a sync plan.
///
/// `remote_changes`: the JMAP changes response
/// `local_changes`: changes detected in the local maildir
/// `known_messages`: all messages we know about (from DB), keyed by maildir_id
/// `strategy`: conflict resolution strategy
/// `mailboxes`: mapping of (jmap_mailbox_id, maildir_folder)
pub fn reconcile(
    remote_created: &[String],
    remote_updated: &[String],
    remote_destroyed: &[String],
    local_changes: &[LocalChange],
    known_messages_by_maildir: &HashMap<String, MessageRecord>,
    known_messages_by_jmap: &HashMap<String, MessageRecord>,
    strategy: ConflictStrategy,
    mailboxes: &[(String, String)],
    new_email_state: Option<String>,
) -> SyncPlan {
    let mut plan = SyncPlan::new();
    plan.new_email_state = new_email_state;

    // Track which JMAP IDs have local changes, for conflict detection
    let local_changed_jmap_ids: HashSet<String> = local_changes
        .iter()
        .filter_map(|lc| match lc {
            LocalChange::FlagsChanged { maildir_id, .. }
            | LocalChange::DeletedMessage { maildir_id, .. } => known_messages_by_maildir
                .get(maildir_id)
                .map(|m| m.jmap_email_id.clone()),
            _ => None,
        })
        .collect();

    // Process remote created (no conflict possible -- new messages)
    for jmap_id in remote_created {
        // Will be handled by pull -- just note them in the plan
        // The actual download happens during execution
        plan.actions.push(SyncAction::DownloadMessage {
            jmap_email_id: jmap_id.clone(),
            jmap_blob_id: String::new(), // filled during execution
            mailbox_id: String::new(),
            maildir_folder: String::new(),
            keywords: HashMap::new(),
        });
    }

    // Process remote updated -- check for conflicts with local flag changes
    for jmap_id in remote_updated {
        if local_changed_jmap_ids.contains(jmap_id) {
            // Conflict! Both sides changed this message.
            match strategy {
                ConflictStrategy::ServerWins => {
                    // Remote update wins -- will update local flags during pull
                    // No need to push
                }
                ConflictStrategy::LocalWins => {
                    // Local change wins -- push local flags, ignore remote update
                    if let Some(msg) = known_messages_by_jmap.get(jmap_id) {
                        if let Some(ref maildir_id) = msg.maildir_id {
                            // Find matching local change to get new flags
                            if let Some(lc) = local_changes.iter().find(|lc| match lc {
                                LocalChange::FlagsChanged { maildir_id: id, .. } => {
                                    id == maildir_id
                                }
                                _ => false,
                            }) {
                                if let LocalChange::FlagsChanged { new_flags, .. } = lc {
                                    let keywords = flags_to_keywords(new_flags);
                                    plan.actions.push(SyncAction::UpdateRemoteKeywords {
                                        jmap_email_id: jmap_id.clone(),
                                        keywords,
                                    });
                                }
                            }
                        }
                    }
                }
                ConflictStrategy::NewestWins => {
                    // For now, default to server-wins for newest-wins
                    // (would need timestamps comparison for proper implementation)
                }
            }
        }
        // Non-conflicting remote updates are handled by the pull phase
    }

    // Process remote destroyed
    for jmap_id in remote_destroyed {
        if let Some(msg) = known_messages_by_jmap.get(jmap_id) {
            if let (Some(ref maildir_id), Some(ref folder)) =
                (&msg.maildir_id, &msg.maildir_folder)
            {
                plan.actions.push(SyncAction::DeleteLocal {
                    maildir_id: maildir_id.clone(),
                    maildir_folder: folder.clone(),
                });
            }
        }
    }

    // Process local changes (non-conflicting ones)
    for change in local_changes {
        match change {
            LocalChange::NewMessage {
                maildir_id,
                folder,
                flags: _,
                path,
            } => {
                // Find mailbox ID for this folder
                if let Some((mailbox_id, _)) = mailboxes.iter().find(|(_, f)| f == folder) {
                    plan.actions.push(SyncAction::UploadMessage {
                        maildir_id: maildir_id.clone(),
                        maildir_folder: folder.clone(),
                        file_path: path.clone(),
                        mailbox_id: mailbox_id.clone(),
                    });
                }
            }
            LocalChange::FlagsChanged {
                maildir_id,
                new_flags,
                ..
            } => {
                // Check if this is a conflict (already handled above)
                if let Some(msg) = known_messages_by_maildir.get(maildir_id) {
                    if !local_changed_jmap_ids.contains(&msg.jmap_email_id)
                        || matches!(strategy, ConflictStrategy::LocalWins)
                    {
                        let keywords = flags_to_keywords(new_flags);
                        plan.actions.push(SyncAction::UpdateRemoteKeywords {
                            jmap_email_id: msg.jmap_email_id.clone(),
                            keywords,
                        });
                    }
                }
            }
            LocalChange::DeletedMessage { maildir_id, .. } => {
                if let Some(msg) = known_messages_by_maildir.get(maildir_id) {
                    if !remote_destroyed.contains(&msg.jmap_email_id) {
                        plan.actions.push(SyncAction::DestroyRemote {
                            jmap_email_id: msg.jmap_email_id.clone(),
                        });
                    }
                }
            }
        }
    }

    plan
}
