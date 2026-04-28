use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

/// A single action to perform during sync.
#[derive(Debug)]
pub enum SyncAction {
    // Server -> Local
    DownloadMessage {
        jmap_email_id: String,
        jmap_blob_id: String,
        mailbox_id: String,
        maildir_folder: String,
        keywords: HashMap<String, bool>,
    },
    UpdateLocalFlags {
        maildir_id: String,
        maildir_folder: String,
        new_flags: String,
    },
    DeleteLocal {
        maildir_id: String,
        maildir_folder: String,
    },
    MoveLocal {
        maildir_id: String,
        from_folder: String,
        to_folder: String,
    },

    // Local -> Server
    UploadMessage {
        maildir_id: String,
        maildir_folder: String,
        file_path: PathBuf,
        mailbox_id: String,
    },
    UpdateRemoteKeywords {
        jmap_email_id: String,
        keywords: HashMap<String, bool>,
    },
    DestroyRemote {
        jmap_email_id: String,
    },
    MoveRemote {
        jmap_email_id: String,
        from_mailbox_id: String,
        to_mailbox_id: String,
    },
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
}

impl fmt::Display for SyncPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return writeln!(f, "Nothing to do.");
        }
        writeln!(f, "Sync plan:")?;
        writeln!(f, "  Downloads:    {}", self.download_count())?;
        writeln!(f, "  Uploads:      {}", self.upload_count())?;
        writeln!(f, "  Flag updates: {}", self.flag_update_count())?;
        writeln!(f, "  Deletes:      {}", self.delete_count())?;
        writeln!(f)?;

        for action in &self.actions {
            match action {
                SyncAction::DownloadMessage {
                    jmap_email_id,
                    maildir_folder,
                    ..
                } => writeln!(
                    f,
                    "  [PULL]  Download {} -> {}/",
                    jmap_email_id, maildir_folder
                )?,
                SyncAction::UpdateLocalFlags {
                    maildir_id,
                    new_flags,
                    ..
                } => writeln!(
                    f,
                    "  [PULL]  Update flags on {}: '{}'",
                    maildir_id, new_flags
                )?,
                SyncAction::DeleteLocal { maildir_id, .. } => {
                    writeln!(f, "  [PULL]  Delete local {}", maildir_id)?
                }
                SyncAction::MoveLocal {
                    maildir_id,
                    from_folder,
                    to_folder,
                } => writeln!(
                    f,
                    "  [PULL]  Move {} from {}/ to {}/",
                    maildir_id, from_folder, to_folder
                )?,
                SyncAction::UploadMessage {
                    maildir_id,
                    maildir_folder,
                    ..
                } => writeln!(f, "  [PUSH] Upload {} from {}/", maildir_id, maildir_folder)?,
                SyncAction::UpdateRemoteKeywords { jmap_email_id, .. } => {
                    writeln!(f, "  [PUSH] Update keywords on {}", jmap_email_id)?
                }
                SyncAction::DestroyRemote { jmap_email_id } => {
                    writeln!(f, "  [PUSH] Destroy {}", jmap_email_id)?
                }
                SyncAction::MoveRemote {
                    jmap_email_id,
                    from_mailbox_id,
                    to_mailbox_id,
                } => writeln!(
                    f,
                    "  [PUSH] Move {} from {} to {}",
                    jmap_email_id, from_mailbox_id, to_mailbox_id
                )?,
            }
        }
        Ok(())
    }
}
