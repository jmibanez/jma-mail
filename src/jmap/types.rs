use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ids::{JmapAccountId, JmapBlobId, JmapEmailId, JmapMailboxId, JmapThreadId, MessageId};

/// Represents a JMAP Email object with the properties we care about.
#[derive(Debug, Clone)]
pub struct EmailObject {
    pub id: JmapEmailId,
    pub blob_id: JmapBlobId,
    pub thread_id: JmapThreadId,
    pub mailbox_ids: HashMap<JmapMailboxId, bool>,
    pub keywords: HashMap<String, bool>,
    pub message_id: Option<Vec<MessageId>>,
    pub subject: Option<String>,
}

/// Represents a JMAP Mailbox object.
#[derive(Debug, Clone)]
pub struct MailboxObject {
    pub id: JmapMailboxId,
    pub name: String,
    pub parent_id: Option<JmapMailboxId>,
    pub role: Option<String>,
    pub sort_order: u32,
    pub total_emails: u64,
    pub unread_emails: u64,
}

/// Result of a JMAP Email/changes call.
#[derive(Debug)]
pub struct ChangesResponse {
    pub old_state: String,
    pub new_state: String,
    pub created: Vec<JmapEmailId>,
    pub updated: Vec<JmapEmailId>,
    pub destroyed: Vec<JmapEmailId>,
    pub has_more_changes: bool,
}

/// JMAP session info extracted after connecting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub api_url: String,
    pub download_url: String,
    pub upload_url: String,
    pub event_source_url: String,
    pub account_id: JmapAccountId,
    pub username: String,
}
