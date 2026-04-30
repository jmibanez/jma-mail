use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a JMAP Email object with the properties we care about.
#[derive(Debug, Clone)]
pub struct EmailObject {
    pub id: String,
    pub blob_id: String,
    pub thread_id: String,
    pub mailbox_ids: HashMap<String, bool>,
    pub keywords: HashMap<String, bool>,
    pub message_id: Option<Vec<String>>,
    pub subject: Option<String>,
}

/// Represents a JMAP Mailbox object.
#[derive(Debug, Clone)]
pub struct MailboxObject {
    pub id: String,
    pub name: String,
    pub parent_id: Option<String>,
    pub role: Option<String>,
    pub sort_order: u32,
    pub total_emails: u64,
    pub unread_emails: u64,
}

/// Result of a JMAP */changes call.
#[derive(Debug)]
pub struct ChangesResponse {
    pub old_state: String,
    pub new_state: String,
    pub created: Vec<String>,
    pub updated: Vec<String>,
    pub destroyed: Vec<String>,
    pub has_more_changes: bool,
}

/// JMAP session info extracted after connecting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub api_url: String,
    pub download_url: String,
    pub upload_url: String,
    pub event_source_url: String,
    pub account_id: String,
    pub username: String,
}
