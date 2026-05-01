use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

/// A message known by its local maildir handle. The optional Message-ID
/// rides along so logs can name the message in human-readable form.
#[derive(Debug, Clone)]
pub struct LocalId {
    pub maildir_id: String,
    pub message_id: Option<String>,
}

/// A message known by its opaque JMAP server id.
#[derive(Debug, Clone)]
pub struct RemoteId {
    pub jmap_email_id: String,
    pub message_id: Option<String>,
}

/// A message bound on both sides — same RFC 5322 message known
/// locally as `maildir_id` and remotely as `jmap_email_id`.
#[derive(Debug, Clone)]
pub struct BoundId {
    pub maildir_id: String,
    pub jmap_email_id: String,
    pub message_id: Option<String>,
}

impl fmt::Display for LocalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message_id {
            Some(m) => write!(f, "{} ({})", self.maildir_id, m),
            None => write!(f, "{}", self.maildir_id),
        }
    }
}

impl fmt::Display for RemoteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message_id {
            Some(m) => write!(f, "{} ({})", self.jmap_email_id, m),
            None => write!(f, "{}", self.jmap_email_id),
        }
    }
}

impl fmt::Display for BoundId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message_id {
            Some(m) => write!(f, "{}/{} ({})", self.maildir_id, self.jmap_email_id, m),
            None => write!(f, "{}/{}", self.maildir_id, self.jmap_email_id),
        }
    }
}

impl BoundId {
    pub fn as_local(&self) -> LocalId {
        LocalId {
            maildir_id: self.maildir_id.clone(),
            message_id: self.message_id.clone(),
        }
    }
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
        jmap_blob_id: String,
        jmap_thread_id: String,
        mailbox_id: String,
        maildir_folder: String,
        keywords: HashMap<String, bool>,
    },
    UpdateLocalFlags {
        id: BoundId,
        maildir_folder: String,
        new_flags: String,
        keywords: HashMap<String, bool>,
        jmap_blob_id: String,
        jmap_thread_id: String,
        mailbox_id: String,
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
        jmap_blob_id: String,
        jmap_thread_id: String,
        mailbox_id: String,
        keywords: HashMap<String, bool>,
        /// When the adopt rebinds an existing JMAP id from one local
        /// maildir_id to another (cross-folder local move), the old
        /// local_state row needs to be cleaned up so subsequent scans
        /// don't keep emitting DeletedMessage for it. Bare String
        /// (not LocalId) — purely a DB-cleanup hint, never logged as
        /// identity.
        old_maildir_id: Option<String>,
    },

    // Local -> Server
    UploadMessage {
        id: LocalId,
        maildir_folder: String,
        file_path: PathBuf,
        mailbox_id: String,
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
        from_mailbox_id: String,
        to_mailbox_id: String,
        /// Folder names of `from_mailbox_id` / `to_mailbox_id`, plumbed
        /// through purely so logs can name folders instead of opaque
        /// JMAP mailbox ids.
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
#[derive(Debug)]
pub struct SyncPlan {
    pub actions: Vec<SyncAction>,
    pub new_email_state: Option<String>,
    pub new_mailbox_state: Option<String>,
}

impl SyncPlan {
    pub fn new() -> Self {
        Self {
            actions: Vec::new(),
            new_email_state: None,
            new_mailbox_state: None,
        }
    }

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
            let action_dir = action.direction();
            let keep = match (direction, action_dir) {
                (SyncDirection::Both, _) => true,
                (_, ActionDirection::Both) => true,
                (SyncDirection::PullOnly, ActionDirection::Pull) => true,
                (SyncDirection::PushOnly, ActionDirection::Push) => true,
                _ => false,
            };
            if keep {
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
