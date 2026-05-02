use anyhow::{Context, Result};
use jmap_client::client::Client;
use jmap_client::mailbox;
use tracing::{debug, info};

use crate::ids::JmapMailboxId;
use crate::jmap::types::MailboxObject;

/// Decide whether `mb` matches any entry in the user's configured mailbox
/// list. An empty list means "sync everything".
///
/// `INBOX` is treated as a magic alias for the JMAP inbox role -- this
/// is the IMAP convention and matches what most users expect when they
/// see "INBOX" in a config file. Other entries match by name, exactly
/// or case-insensitively depending on `case_insensitive`.
pub fn is_mailbox_synced(
    config_entries: &[String],
    mb: &MailboxObject,
    case_insensitive: bool,
) -> bool {
    if config_entries.is_empty() {
        return true;
    }
    for entry in config_entries {
        if entry == "INBOX" && mb.role.as_deref() == Some("inbox") {
            return true;
        }
        let matches_name = if case_insensitive {
            entry.eq_ignore_ascii_case(&mb.name)
        } else {
            entry == &mb.name
        };
        if matches_name {
            return true;
        }
    }
    false
}

/// Fetch all mailboxes from the server using the convenience helper.
pub async fn get_all(client: &Client) -> Result<Vec<MailboxObject>> {
    let mut request = client.build();
    let get_request = request
        .get_mailbox()
        .account_id(client.default_account_id());
    get_request.properties([
        mailbox::Property::Id,
        mailbox::Property::Name,
        mailbox::Property::ParentId,
        mailbox::Property::Role,
        mailbox::Property::SortOrder,
        mailbox::Property::TotalEmails,
        mailbox::Property::UnreadEmails,
    ]);

    let response = request
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch mailboxes: {}", e))?;

    let mailbox_response = response
        .unwrap_method_responses()
        .pop()
        .context("No response for mailbox get")?;

    let get_response = mailbox_response
        .unwrap_get_mailbox()
        .map_err(|e| anyhow::anyhow!("Failed to parse mailbox response: {}", e))?;

    let state = get_response.state().to_string();
    let mailboxes: Vec<MailboxObject> = get_response
        .list()
        .iter()
        .map(|mb| {
            let id = JmapMailboxId::from(mb.id().unwrap_or_default());
            let name = mb.name().unwrap_or("(unnamed)").to_string();
            let parent_id = mb.parent_id().map(JmapMailboxId::from);
            let role = mb.role();
            let role_str = match role {
                jmap_client::mailbox::Role::None => None,
                other => Some(format!("{:?}", other).to_lowercase()),
            };
            let sort_order = mb.sort_order();
            let total_emails = mb.total_emails() as u64;
            let unread_emails = mb.unread_emails() as u64;

            debug!(
                "Mailbox: {} (id={}, role={:?}, total={}, unread={})",
                name, id, role_str, total_emails, unread_emails
            );

            MailboxObject {
                id,
                name,
                parent_id,
                role: role_str,
                sort_order,
                total_emails,
                unread_emails,
            }
        })
        .collect();

    info!("Fetched {} mailboxes (state: {})", mailboxes.len(), state);

    Ok(mailboxes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mb(name: &str, role: Option<&str>) -> MailboxObject {
        MailboxObject {
            id: JmapMailboxId::from("mb1"),
            name: name.to_string(),
            parent_id: None,
            role: role.map(str::to_string),
            sort_order: 0,
            total_emails: 0,
            unread_emails: 0,
        }
    }

    #[test]
    fn empty_config_syncs_everything() {
        assert!(is_mailbox_synced(&[], &mb("Inbox", Some("inbox")), false));
        assert!(is_mailbox_synced(&[], &mb("Random", None), false));
    }

    #[test]
    fn inbox_alias_matches_inbox_role() {
        let entries = vec!["INBOX".to_string()];
        assert!(is_mailbox_synced(
            &entries,
            &mb("Inbox", Some("inbox")),
            false
        ));
        // Even if the server localized the name:
        assert!(is_mailbox_synced(
            &entries,
            &mb("Indbakke", Some("inbox")),
            false
        ));
    }

    #[test]
    fn inbox_alias_does_not_match_non_inbox_role() {
        let entries = vec!["INBOX".to_string()];
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Inbox", Some("archive")),
            false
        ));
        assert!(!is_mailbox_synced(&entries, &mb("Inbox", None), false));
    }

    #[test]
    fn exact_name_match_is_case_sensitive_by_default() {
        let entries = vec!["Archive".to_string()];
        assert!(is_mailbox_synced(
            &entries,
            &mb("Archive", Some("archive")),
            false
        ));
        assert!(!is_mailbox_synced(
            &entries,
            &mb("archive", Some("archive")),
            false
        ));
    }

    #[test]
    fn case_insensitive_flag_loosens_name_match() {
        let entries = vec!["archive".to_string()];
        assert!(is_mailbox_synced(
            &entries,
            &mb("Archive", Some("archive")),
            true
        ));
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Archive", Some("archive")),
            false
        ));
    }

    #[test]
    fn unmatched_entry_does_not_sync() {
        let entries = vec!["Sent".to_string(), "Drafts".to_string()];
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Spam", Some("junk")),
            false
        ));
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Spam", Some("junk")),
            true
        ));
    }
}
