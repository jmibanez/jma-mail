use anyhow::{Context, Result};
use jmap_client::client::Client;
use jmap_client::email;
use std::collections::HashMap;
use tracing::{debug, info};

use crate::jmap::retry::with_retry;
use crate::jmap::types::{ChangesResponse, EmailObject};

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

/// Query all email IDs in a mailbox, paginated.
pub async fn query_mailbox(
    client: &Client,
    mailbox_id: &str,
    max_results: Option<u64>,
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
            "Queried page at position {}: got {} emails (total so far: {})",
            position,
            count,
            all_ids.len()
        );

        if count < page_size {
            break;
        }

        position += page_size;

        if let Some(max) = max_results {
            if all_ids.len() as u64 >= max {
                all_ids.truncate(max as usize);
                break;
            }
        }
    }

    info!(
        "Queried {} email IDs from mailbox {}",
        all_ids.len(),
        mailbox_id
    );
    Ok(all_ids)
}

/// Reverse-resolve RFC 5322 Message-IDs to JMAP email IDs.
///
/// Issues batched `Email/query` requests with a `header:Message-ID` filter
/// (substring match per JMAP spec). Returns a map of `Message-ID -> email_id`
/// for those the server recognises. Used by the adoption path that bootstraps
/// `message_map` from a pre-populated maildir.
pub async fn resolve_by_message_ids(
    client: &Client,
    message_ids: &[String],
) -> Result<HashMap<String, String>> {
    let mut out: HashMap<String, String> = HashMap::new();
    if message_ids.is_empty() {
        return Ok(out);
    }
    // Stay conservative; JMAP servers commonly cap maxCallsInRequest at ~16.
    const BATCH: usize = 16;

    for chunk in message_ids.chunks(BATCH) {
        let mut request = client.build();
        for mid in chunk {
            let q = request
                .query_email()
                .account_id(client.default_account_id());
            q.filter(email::query::Filter::header(
                "Message-ID",
                Some(mid.as_str()),
            ));
            q.limit(1);
        }

        let response = request
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed batched Message-ID resolve: {}", e))?;

        let responses = response.unwrap_method_responses();
        if responses.len() != chunk.len() {
            return Err(anyhow::anyhow!(
                "Batched response length mismatch: expected {}, got {}",
                chunk.len(),
                responses.len()
            ));
        }

        for (mid, resp) in chunk.iter().zip(responses) {
            let qr = resp
                .unwrap_query_email()
                .map_err(|e| anyhow::anyhow!("Failed to parse query for <{}>: {}", mid, e))?;
            if let Some(id) = qr.ids().first() {
                out.insert(mid.clone(), id.to_string());
            }
        }
    }

    debug!(
        "Resolved {} of {} Message-IDs to email IDs",
        out.len(),
        message_ids.len()
    );
    Ok(out)
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

/// Update keywords on an email using convenience helper.
pub async fn set_keywords(
    client: &Client,
    email_id: &str,
    keywords: &HashMap<String, bool>,
) -> Result<()> {
    // Use individual keyword set/unset
    for (keyword, value) in keywords {
        with_retry("Email/set (keyword)", || async {
            client
                .email_set_keyword(email_id, keyword, *value)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to set keyword {} on email {}: {}",
                        keyword,
                        email_id,
                        e
                    )
                })
        })
        .await?;
    }

    debug!("Updated keywords for email {}", email_id);
    Ok(())
}

/// Move an email between mailboxes using convenience helper.
pub async fn move_to_mailbox(
    client: &Client,
    email_id: &str,
    from_mailbox_id: &str,
    to_mailbox_id: &str,
) -> Result<()> {
    with_retry("Email/set (mailbox remove)", || async {
        client
            .email_set_mailbox(email_id, from_mailbox_id, false)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to remove from mailbox: {}", e))
    })
    .await?;
    with_retry("Email/set (mailbox add)", || async {
        client
            .email_set_mailbox(email_id, to_mailbox_id, true)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to add to mailbox: {}", e))
    })
    .await?;

    debug!(
        "Moved email {} from mailbox {} to {}",
        email_id, from_mailbox_id, to_mailbox_id
    );
    Ok(())
}

/// Destroy (permanently delete) emails using convenience helper.
pub async fn destroy(client: &Client, email_ids: &[&str]) -> Result<()> {
    for id in email_ids {
        with_retry("Email/set (destroy)", || async {
            client
                .email_destroy(id)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to destroy email {}: {}", id, e))
        })
        .await?;
    }

    info!("Destroyed {} emails", email_ids.len());
    Ok(())
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
pub async fn import_email(
    client: &Client,
    raw_message: &[u8],
    mailbox_id: &str,
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

    info!("Imported email into mailbox {}: {}", mailbox_id, email_id);
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
