use anyhow::{Context, Result};
use jmap_client::client::Client;
use jmap_client::mailbox;
use tracing::{debug, info};

use crate::jmap::types::MailboxObject;

/// Fetch all mailboxes from the server using the convenience helper.
pub async fn get_all(client: &Client) -> Result<Vec<MailboxObject>> {
    let mut request = client.build();
    let get_request = request.get_mailbox().account_id(client.default_account_id());
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
            let id = mb.id().unwrap_or_default().to_string();
            let name = mb.name().unwrap_or("(unnamed)").to_string();
            let parent_id = mb.parent_id().map(|p| p.to_string());
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

    info!(
        "Fetched {} mailboxes (state: {})",
        mailboxes.len(),
        state
    );

    Ok(mailboxes)
}
