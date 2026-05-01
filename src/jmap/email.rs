use anyhow::{Context, Result};
use jmap_client::client::Client;
use jmap_client::email;
use std::collections::HashMap;
use tracing::{debug, info};

use crate::jmap::retry::with_retry;
use crate::jmap::types::{ChangesResponse, EmailObject};
use crate::sync::plan::LocalId;

/// Properties we request for Email/get calls.
fn email_properties() -> Vec<email::Property> {
    vec![
        email::Property::Id,
        email::Property::BlobId,
        email::Property::ThreadId,
        email::Property::MailboxIds,
        email::Property::Keywords,
        email::Property::MessageId,
    ]
}

/// Fetch emails by IDs using the request builder.
pub async fn get_by_ids(client: &Client, ids: &[&str]) -> Result<Vec<EmailObject>> {
    with_retry("Email/get", || async {
        let mut request = client.build();
        let get_request = request.get_email().account_id(client.default_account_id());
        get_request.ids(ids.iter().map(|s| s.to_string()));
        get_request.properties(email_properties());

        let response = request
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch emails: {}", e))?;

        let email_response = response
            .unwrap_method_responses()
            .pop()
            .context("No response for email get")?;

        let get_response = email_response
            .unwrap_get_email()
            .map_err(|e| anyhow::anyhow!("Failed to parse email response: {}", e))?;

        Ok(get_response.list().iter().map(parse_email_object).collect())
    })
    .await
}

/// Query all email IDs in a mailbox, paginated. `folder_name` is used
/// only for log readability — the JMAP request identifies the mailbox
/// by `mailbox_id`.
pub async fn query_mailbox(
    client: &Client,
    mailbox_id: &str,
    folder_name: &str,
) -> Result<Vec<String>> {
    let mut all_ids = Vec::new();
    let mut position: usize = 0;
    let page_size: usize = 100;

    loop {
        let ids: Vec<String> = with_retry("Email/query", || async {
            let mut request = client.build();
            let query = request
                .query_email()
                .account_id(client.default_account_id());
            query
                .filter(email::query::Filter::in_mailbox(mailbox_id))
                .position(position as i32)
                .limit(page_size);

            let response = request
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to query emails: {}", e))?;

            let query_response = response
                .unwrap_method_responses()
                .pop()
                .context("No response for email query")?;

            let result = query_response
                .unwrap_query_email()
                .map_err(|e| anyhow::anyhow!("Failed to parse email query response: {}", e))?;

            Ok(result.ids().iter().map(|id| id.to_string()).collect())
        })
        .await?;
        let count = ids.len();
        all_ids.extend(ids);

        debug!(
            "Queried {} page at position {}: got {} emails (total so far: {})",
            folder_name,
            position,
            count,
            all_ids.len()
        );

        if count < page_size {
            break;
        }

        position += page_size;
    }

    info!(
        "Queried {} email IDs from {} ({})",
        all_ids.len(),
        folder_name,
        mailbox_id
    );
    Ok(all_ids)
}

/// Fetch the current Email state by issuing Email/get with an empty id list.
/// Use this to bootstrap the state for delta sync after a full initial pull.
pub async fn get_current_state(client: &Client) -> Result<String> {
    with_retry("Email/get (state bootstrap)", || async {
        let mut request = client.build();
        let get_request = request.get_email().account_id(client.default_account_id());
        get_request.ids(Vec::<String>::new());
        get_request.properties(vec![email::Property::Id]);

        let response = request
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch email state: {}", e))?;

        let email_response = response
            .unwrap_method_responses()
            .pop()
            .context("No response for email state get")?;

        let get_response = email_response
            .unwrap_get_email()
            .map_err(|e| anyhow::anyhow!("Failed to parse email state response: {}", e))?;

        Ok(get_response.state().to_string())
    })
    .await
}

/// Fetch email changes since a given state using convenience helper.
pub async fn get_changes(client: &Client, since_state: &str) -> Result<ChangesResponse> {
    let changes = with_retry("Email/changes", || async {
        client
            .email_changes(since_state, Some(500))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch email changes: {}", e))
    })
    .await?;

    let result = ChangesResponse {
        old_state: changes.old_state().to_string(),
        new_state: changes.new_state().to_string(),
        created: changes.created().iter().map(|id| id.to_string()).collect(),
        updated: changes.updated().iter().map(|id| id.to_string()).collect(),
        destroyed: changes
            .destroyed()
            .iter()
            .map(|id| id.to_string())
            .collect(),
        has_more_changes: changes.has_more_changes(),
    };

    info!(
        "Email changes: {} created, {} updated, {} destroyed (state {} -> {})",
        result.created.len(),
        result.updated.len(),
        result.destroyed.len(),
        result.old_state,
        result.new_state
    );

    Ok(result)
}

/// One unit of work for `set_email_batch`. Collapses keyword changes,
/// mailbox moves, and destroys onto a single Email/set method call so
/// the server sees one transaction per cycle instead of N.
#[derive(Debug, Clone)]
pub enum EmailSetOp {
    /// Patch keywords on an email. Map is `keyword -> set?`.
    Keywords {
        email_id: String,
        keywords: HashMap<String, bool>,
    },
    /// Replace the email's full mailbox-id set. The caller computes
    /// the target set; `set_email_batch` issues a full-replacement
    /// `mailboxIds` update. Used both for cross-mailbox moves and
    /// (in principle) any other membership change.
    SetMailboxes {
        email_id: String,
        target_mailbox_ids: Vec<String>,
    },
    /// Destroy an email by id.
    Destroy { email_id: String },
}

/// Outcome of `set_email_batch`. The two `failed_*` sets contain ids
/// the server rejected per-row (notUpdated / notDestroyed); callers
/// should suppress DB mirror for those ids.
#[derive(Debug, Default)]
pub struct EmailSetOutcome {
    pub failed_updates: std::collections::HashSet<String>,
    pub failed_destroys: std::collections::HashSet<String>,
}

/// Apply a batch of Email/set operations in a single JMAP method call.
///
/// JMAP allows arbitrarily many `update` and `destroy` entries in one
/// Email/set, capped server-side by `maxObjectsInSet`. Per-id failures
/// land in `notUpdated`/`notDestroyed` and are surfaced as warnings;
/// they do not fail the batch.
///
/// Moves are emitted as a *full replacement* of `mailboxIds` (the
/// caller-supplied `target_mailbox_ids`) rather than a per-key patch.
/// Per RFC 8621 §4.1.1, `mailboxIds` is `Id[Boolean]` whose values are
/// always `true`; removing a key requires a `null` patch value (RFC
/// 8620 §5.3). jmap-client 0.4.1 cannot serialize `null` for a
/// mailboxIds patch (its patch map is typed `bool`), and Fastmail
/// correctly rejects `mailboxIds/{id}: false`. Full replacement
/// sidesteps the issue at the cost of stripping any out-of-band
/// mailbox memberships not in the target set — acceptable for
/// jmapsync's single-mailbox-per-email model.
pub async fn set_email_batch(client: &Client, ops: &[EmailSetOp]) -> Result<EmailSetOutcome> {
    if ops.is_empty() {
        return Ok(EmailSetOutcome::default());
    }

    let outcome = with_retry("Email/set (batch)", || async {
        let mut request = client.build();
        {
            let set = request.set_email().account_id(client.default_account_id());
            for op in ops {
                match op {
                    EmailSetOp::Keywords { email_id, keywords } => {
                        let upd = set.update(email_id);
                        for (kw, val) in keywords {
                            upd.keyword(kw, *val);
                        }
                    }
                    EmailSetOp::SetMailboxes {
                        email_id,
                        target_mailbox_ids,
                    } => {
                        set.update(email_id).mailbox_ids(target_mailbox_ids.clone());
                    }
                    EmailSetOp::Destroy { email_id } => {
                        set.destroy([email_id.as_str()]);
                    }
                }
            }
        }

        let response = request
            .send_single::<jmap_client::core::response::EmailSetResponse>()
            .await
            .map_err(|e| anyhow::anyhow!("Email/set batch failed: {}", e))?;

        let mut outcome = EmailSetOutcome::default();
        if let Some(not_updated) = response.not_updated_ids() {
            for id in not_updated {
                tracing::warn!("Email/set batch: notUpdated {}", id);
                outcome.failed_updates.insert(id.clone());
            }
        }
        if let Some(not_destroyed) = response.not_destroyed_ids() {
            for id in not_destroyed {
                tracing::warn!("Email/set batch: notDestroyed {}", id);
                outcome.failed_destroys.insert(id.clone());
            }
        }

        Ok(outcome)
    })
    .await?;

    debug!("Applied {} Email/set operations in one call", ops.len());
    Ok(outcome)
}

/// Normalize line endings to CRLF for RFC 5322 wire format.
/// Maildir messages are typically stored with bare LF; JMAP servers
/// reject those with `invalidEmail: Message contains bare newlines`.
fn normalize_crlf(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() + input.len() / 32);
    let mut prev = 0u8;
    for &b in input {
        if b == b'\n' && prev != b'\r' {
            out.push(b'\r');
        }
        out.push(b);
        prev = b;
    }
    out
}

/// Import a raw email message (RFC 5322) into a mailbox using convenience helper.
/// `local` and `folder_name` are used only for log readability.
pub async fn import_email(
    client: &Client,
    raw_message: &[u8],
    mailbox_id: &str,
    folder_name: &str,
    local: &LocalId,
    keywords: &HashMap<String, bool>,
) -> Result<String> {
    let keyword_list: Vec<String> = keywords
        .iter()
        .filter(|(_, v)| **v)
        .map(|(k, _)| k.clone())
        .collect();

    let keyword_opt: Option<Vec<String>> = if keyword_list.is_empty() {
        None
    } else {
        Some(keyword_list)
    };

    let normalized = normalize_crlf(raw_message);

    let email = with_retry("Email/import", || async {
        client
            .email_import(
                normalized.clone(),
                [mailbox_id.to_string()],
                keyword_opt.clone(),
                None,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to import email: {}", e))
    })
    .await?;

    let email_id = email.id().unwrap_or_default().to_string();

    info!(
        "Imported email from {} into {} ({}) -> JMAP {}",
        local, folder_name, mailbox_id, email_id
    );
    Ok(email_id)
}

/// Download the raw blob of an email.
pub async fn download_blob(client: &Client, blob_id: &str) -> Result<Vec<u8>> {
    let data = client
        .download(blob_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to download blob {}: {}", blob_id, e))?;

    debug!("Downloaded blob {} ({} bytes)", blob_id, data.len());
    Ok(data)
}

fn parse_email_object(email: &jmap_client::email::Email<jmap_client::Get>) -> EmailObject {
    let id = email.id().unwrap_or_default().to_string();
    let blob_id = email.blob_id().unwrap_or_default().to_string();
    let thread_id = email.thread_id().unwrap_or_default().to_string();
    let message_id = email
        .message_id()
        .map(|ids| ids.iter().map(|s| s.to_string()).collect());

    let mailbox_ids: HashMap<String, bool> = email
        .mailbox_ids()
        .iter()
        .map(|id| (id.to_string(), true))
        .collect();

    let keywords: HashMap<String, bool> = email
        .keywords()
        .iter()
        .map(|kw| (kw.to_string(), true))
        .collect();

    EmailObject {
        id,
        blob_id,
        thread_id,
        mailbox_ids,
        keywords,
        message_id,
        subject: None,
    }
}
