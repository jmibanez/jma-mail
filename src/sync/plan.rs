use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use crate::ids::{JmapBlobId, JmapEmailId, JmapMailboxId, JmapThreadId, MaildirId, MessageId};

/// A message known by its local maildir handle. The Message-ID rides
/// along so logs can name the message in human-readable form, and so
/// downstream reconcile/execute can use it as the idempotency anchor
/// without re-parsing the file. Required: scan refuses to construct a
/// `LocalId` for a file with no parseable Message-ID, so by the time
/// any code holds one, the id is guaranteed.
#[derive(Debug, Clone)]
pub struct LocalId {
    pub maildir_id: MaildirId,
    pub message_id: MessageId,
}

/// A message known by its opaque JMAP server id. Carries the
/// Message-ID for human-readable logging and as the cross-boundary
/// idempotency anchor: scan and reconcile both refuse to construct a
/// `RemoteId` for a message without one (see `maildir_ops::scan` and
/// `sync::reconcile::process_remote_emails`), so by the time any
/// downstream code holds one, the id is guaranteed.
#[derive(Debug, Clone)]
pub struct RemoteId {
    pub jmap_email_id: JmapEmailId,
    pub message_id: MessageId,
}

/// A message bound on both sides — same RFC 5322 message known
/// locally as `maildir_id` and remotely as `jmap_email_id`. Same
/// Message-ID guarantee as `LocalId` and `RemoteId`.
#[derive(Debug, Clone)]
pub struct BoundId {
    pub maildir_id: MaildirId,
    pub jmap_email_id: JmapEmailId,
    pub message_id: MessageId,
}

impl fmt::Display for LocalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.maildir_id, self.message_id)
    }
}

impl fmt::Display for RemoteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.jmap_email_id, self.message_id)
    }
}

impl fmt::Display for BoundId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{} ({})",
            self.maildir_id, self.jmap_email_id, self.message_id
        )
    }
}

impl BoundId {
    pub fn as_remote(&self) -> RemoteId {
        RemoteId {
            jmap_email_id: self.jmap_email_id.clone(),
            message_id: self.message_id.clone(),
        }
    }
}

/// A single action to perform during sync.
#[derive(Debug)]
pub enum SyncAction {
    // Server -> Local
    DownloadMessage {
        id: RemoteId,
        jmap_blob_id: JmapBlobId,
        jmap_thread_id: JmapThreadId,
        mailbox_id: JmapMailboxId,
        maildir_folder: String,
        keywords: HashMap<String, bool>,
    },
    UpdateLocalFlags {
        id: BoundId,
        maildir_folder: String,
        new_flags: String,
        keywords: HashMap<String, bool>,
        jmap_blob_id: JmapBlobId,
        jmap_thread_id: JmapThreadId,
        mailbox_id: JmapMailboxId,
    },
    DeleteLocal {
        id: BoundId,
        maildir_folder: String,
    },
    MoveLocal {
        id: BoundId,
        from_folder: String,
        to_folder: String,
    },

    /// Bind an existing local file to a known server email (no download,
    /// no upload — pure DB write). Pre-empts the alreadyExists path on
    /// push and the redundant download path on pull.
    AdoptLocalMessage {
        id: BoundId,
        maildir_folder: String,
        jmap_blob_id: Option<JmapBlobId>,
        jmap_thread_id: Option<JmapThreadId>,
        mailbox_id: JmapMailboxId,
        keywords: HashMap<String, bool>,
        /// When the adopt rebinds an existing JMAP id from one local
        /// maildir_id to another (cross-folder local move), the old
        /// local_state row needs to be cleaned up so subsequent scans
        /// don't keep emitting DeletedMessage for it. Just a bare
        /// `MaildirId` (not a `LocalId` bundle) — this is purely a
        /// DB-cleanup hint, never logged as identity.
        old_maildir_id: Option<MaildirId>,
    },

    // Local -> Server
    UploadMessage {
        id: LocalId,
        maildir_folder: String,
        file_path: PathBuf,
        mailbox_id: JmapMailboxId,
        /// Maildir flags suffix captured at scan time. Plumbed
        /// through from `LocalChange::NewMessage` so the executor
        /// doesn't have to re-parse the on-disk filename, and so
        /// the keywords we upload match the ones the plan was
        /// built against.
        flags: String,
    },
    UpdateRemoteKeywords {
        id: RemoteId,
        keywords: HashMap<String, bool>,
    },
    DestroyRemote {
        id: RemoteId,
    },
    MoveRemote {
        id: RemoteId,
        /// Full target set of mailbox ids the email should belong to
        /// after the move. We send this as a full-replacement
        /// `mailboxIds` in Email/set rather than a per-key patch
        /// because jmap-client 0.4.1 cannot serialize a `null` value
        /// for `mailboxIds/{id}` (its patch map is typed `bool`), and
        /// servers like Fastmail correctly reject `false` for a
        /// `Id[Boolean]` set-membership map. Computed at planning
        /// time; for jmapsync's single-mailbox-per-email DB model
        /// this is just `[to_mailbox_id]`.
        target_mailbox_ids: Vec<JmapMailboxId>,
        /// Folder names of the source / destination mailboxes,
        /// plumbed through purely so logs can name folders instead
        /// of opaque JMAP mailbox ids.
        from_folder: String,
        to_folder: String,
    },
}

impl SyncAction {
    /// Which side of a sync this action belongs to.
    pub fn direction(&self) -> ActionDirection {
        match self {
            SyncAction::DownloadMessage { .. }
            | SyncAction::UpdateLocalFlags { .. }
            | SyncAction::DeleteLocal { .. }
            | SyncAction::MoveLocal { .. } => ActionDirection::Pull,
            SyncAction::UploadMessage { .. }
            | SyncAction::UpdateRemoteKeywords { .. }
            | SyncAction::DestroyRemote { .. }
            | SyncAction::MoveRemote { .. } => ActionDirection::Push,
            SyncAction::AdoptLocalMessage { .. } => ActionDirection::Both,
        }
    }
}

/// Which direction(s) an action moves data in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionDirection {
    Pull,
    Push,
    Both,
}

/// Top-level sync mode. Selects which side(s) of the plan execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    Both,
    PullOnly,
    PushOnly,
}

/// A computed plan of sync actions to execute.
#[derive(Debug, Default)]
pub struct SyncPlan {
    pub actions: Vec<SyncAction>,
    pub new_email_state: Option<String>,
    pub new_mailbox_state: Option<String>,
}

impl SyncPlan {
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn download_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, SyncAction::DownloadMessage { .. }))
            .count()
    }

    pub fn upload_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, SyncAction::UploadMessage { .. }))
            .count()
    }

    pub fn flag_update_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    SyncAction::UpdateLocalFlags { .. } | SyncAction::UpdateRemoteKeywords { .. }
                )
            })
            .count()
    }

    pub fn delete_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    SyncAction::DeleteLocal { .. } | SyncAction::DestroyRemote { .. }
                )
            })
            .count()
    }

    pub fn adopt_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, SyncAction::AdoptLocalMessage { .. }))
            .count()
    }

    /// Split the plan into (kept, dropped) according to `direction`.
    /// AdoptLocalMessage is always kept — it is byte-identical and pure
    /// DB, so adopting in pull-only or push-only mode is still strictly
    /// progress. Pull-only keeps pull-side actions; push-only keeps
    /// push-side; Both keeps everything.
    pub fn into_filtered(self, direction: SyncDirection) -> (SyncPlan, Vec<SyncAction>) {
        let SyncPlan {
            actions,
            new_email_state,
            new_mailbox_state,
        } = self;

        let mut kept = Vec::with_capacity(actions.len());
        let mut dropped = Vec::new();

        for action in actions {
            if matches!(
                (direction, action.direction()),
                (SyncDirection::Both, _)
                    | (_, ActionDirection::Both)
                    | (SyncDirection::PullOnly, ActionDirection::Pull)
                    | (SyncDirection::PushOnly, ActionDirection::Push)
            ) {
                kept.push(action);
            } else {
                dropped.push(action);
            }
        }

        (
            SyncPlan {
                actions: kept,
                new_email_state,
                new_mailbox_state,
            },
            dropped,
        )
    }
}

impl fmt::Display for SyncPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return writeln!(f, "Nothing to do.");
        }
        writeln!(f, "Sync plan:")?;
        writeln!(f, "  Downloads:    {}", self.download_count())?;
        writeln!(f, "  Uploads:      {}", self.upload_count())?;
        writeln!(f, "  Adoptions:    {}", self.adopt_count())?;
        writeln!(f, "  Flag updates: {}", self.flag_update_count())?;
        writeln!(f, "  Deletes:      {}", self.delete_count())?;
        writeln!(f)?;

        for action in &self.actions {
            match action {
                SyncAction::DownloadMessage {
                    id, maildir_folder, ..
                } => writeln!(f, "  [PULL]  Download {} -> {}/", id, maildir_folder)?,
                SyncAction::UpdateLocalFlags { id, new_flags, .. } => {
                    writeln!(f, "  [PULL]  Update flags on {}: '{}'", id, new_flags)?
                }
                SyncAction::DeleteLocal { id, .. } => writeln!(f, "  [PULL]  Delete local {}", id)?,
                SyncAction::MoveLocal {
                    id,
                    from_folder,
                    to_folder,
                } => writeln!(
                    f,
                    "  [PULL]  Move {} from {}/ to {}/",
                    id, from_folder, to_folder
                )?,
                SyncAction::AdoptLocalMessage {
                    id, maildir_folder, ..
                } => writeln!(
                    f,
                    "  [BOTH]  Adopt {}/{} as {}",
                    maildir_folder,
                    id.maildir_id,
                    id.as_remote()
                )?,
                SyncAction::UploadMessage {
                    id, maildir_folder, ..
                } => writeln!(f, "  [PUSH] Upload {} from {}/", id, maildir_folder)?,
                SyncAction::UpdateRemoteKeywords { id, .. } => {
                    writeln!(f, "  [PUSH] Update keywords on {}", id)?
                }
                SyncAction::DestroyRemote { id } => writeln!(f, "  [PUSH] Destroy {}", id)?,
                SyncAction::MoveRemote {
                    id,
                    from_folder,
                    to_folder,
                    ..
                } => writeln!(
                    f,
                    "  [PUSH] Move {} from {}/ to {}/",
                    id, from_folder, to_folder
                )?,
            }
        }
        Ok(())
    }
}
