use anyhow::Result;
use jmap_client::client::Client;
use rusqlite::Connection;
use tracing::{debug, info};

use crate::jmap::email as jmap_email;
use crate::maildir_ops::{flags::keywords_to_flags, store};
use crate::state::queries::{self, MessageRecord};

/// Pull new and changed messages from server to local maildir.
/// Returns the new JMAP email state string.
pub async fn pull(
    client: &Client,
    conn: &Connection,
    account_id: &str,
    mailboxes: &[(String, String)], // (jmap_mailbox_id, maildir_folder)
    maildir_root: &std::path::Path,
    max_messages: u64,
) -> Result<String> {
    let current_state = queries::get_jmap_state(conn, account_id, "Email")?;

    match current_state {
        None => {
            info!("No previous state -- performing initial pull");
            initial_pull(client, conn, account_id, mailboxes, maildir_root, max_messages).await
        }
        Some(state) => {
            info!("Delta pull from state: {}", state);
            delta_pull(client, conn, account_id, &state, mailboxes, maildir_root).await
        }
    }
}

/// Initial full pull: query all emails in each mailbox, download them.
async fn initial_pull(
    client: &Client,
    conn: &Connection,
    account_id: &str,
    mailboxes: &[(String, String)],
    maildir_root: &std::path::Path,
    max_messages: u64,
) -> Result<String> {
    for (mailbox_id, folder_name) in mailboxes {
        let maildir_path = maildir_root.join(folder_name);
        let maildir = store::ensure_maildir(&maildir_path)?;

        let max = if max_messages > 0 {
            Some(max_messages)
        } else {
            None
        };
        let email_ids = jmap_email::query_mailbox(client, mailbox_id, max).await?;

        info!(
            "Downloading {} messages from {} into {}/",
            email_ids.len(),
            folder_name,
            folder_name
        );

        // Fetch in batches of 50
        for chunk in email_ids.chunks(50) {
            let id_refs: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();
            let emails = jmap_email::get_by_ids(client, &id_refs).await?;

            for email in &emails {
                let flags = keywords_to_flags(&email.keywords);
                let blob = jmap_email::download_blob(client, &email.blob_id).await?;
                let maildir_id = store::store_message(&maildir, &blob, &flags)?;

                let message_id = email
                    .message_id
                    .as_ref()
                    .and_then(|ids| ids.first())
                    .cloned();
                let keywords_json = serde_json::to_string(&email.keywords)?;

                queries::upsert_message(
                    conn,
                    &MessageRecord {
                        jmap_email_id: email.id.clone(),
                        jmap_blob_id: Some(email.blob_id.clone()),
                        jmap_thread_id: Some(email.thread_id.clone()),
                        mailbox_id: mailbox_id.clone(),
                        maildir_id: Some(maildir_id.clone()),
                        maildir_folder: Some(folder_name.clone()),
                        message_id,
                        flags: flags.clone(),
                        jmap_keywords: keywords_json,
                        size: Some(email.size as i64),
                        received_at: email.received_at.clone(),
                    },
                )?;

                queries::upsert_local_state(
                    conn,
                    &maildir_id,
                    folder_name,
                    &flags,
                    Some(email.size as i64),
                    None,
                )?;
            }
        }

    }

    // Bootstrap delta-sync state with a real Email state from the server.
    // Email/changes since "0" returns cannotCalculateChanges; use Email/get
    // with an empty id list to read the current state instead.
    let state = jmap_email::get_current_state(client).await?;

    queries::set_jmap_state(conn, account_id, "Email", &state)?;
    info!("Initial pull complete. State: {}", state);

    Ok(state)
}

/// Delta pull: use Email/changes to get only what changed since last sync.
async fn delta_pull(
    client: &Client,
    conn: &Connection,
    account_id: &str,
    since_state: &str,
    mailboxes: &[(String, String)],
    maildir_root: &std::path::Path,
) -> Result<String> {
    let mut state = since_state.to_string();

    loop {
        let changes = match jmap_email::get_changes(client, &state).await {
            Ok(c) => c,
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("Cannot calculate changes") {
                    info!("Server cannot calculate changes; falling back to full re-sync");
                    // Clear state and re-do initial pull
                    queries::set_jmap_state(conn, account_id, "Email", "")?;
                    return Box::pin(initial_pull(
                        client,
                        conn,
                        account_id,
                        mailboxes,
                        maildir_root,
                        0,
                    ))
                    .await;
                }
                return Err(e);
            }
        };

        // Handle created messages
        if !changes.created.is_empty() {
            process_created(client, conn, &changes.created, mailboxes, maildir_root).await?;
        }

        // Handle updated messages (keyword/mailbox changes)
        if !changes.updated.is_empty() {
            process_updated(client, conn, &changes.updated, mailboxes, maildir_root).await?;
        }

        // Handle destroyed messages
        if !changes.destroyed.is_empty() {
            process_destroyed(conn, &changes.destroyed, maildir_root)?;
        }

        state = changes.new_state;

        if !changes.has_more_changes {
            break;
        }
    }

    queries::set_jmap_state(conn, account_id, "Email", &state)?;
    info!("Delta pull complete. New state: {}", state);

    Ok(state)
}

async fn process_created(
    client: &Client,
    conn: &Connection,
    created_ids: &[String],
    mailboxes: &[(String, String)],
    maildir_root: &std::path::Path,
) -> Result<()> {
    let id_refs: Vec<&str> = created_ids.iter().map(|s| s.as_str()).collect();

    for chunk in id_refs.chunks(50) {
        let emails = jmap_email::get_by_ids(client, chunk).await?;

        for email in &emails {
            // Find which of our synced mailboxes this email belongs to
            let folder = find_folder_for_email(email, mailboxes);
            let Some((mailbox_id, folder_name)) = folder else {
                debug!("Email {} not in any synced mailbox, skipping", email.id);
                continue;
            };

            let maildir_path = maildir_root.join(&folder_name);
            let maildir = store::ensure_maildir(&maildir_path)?;

            let flags = keywords_to_flags(&email.keywords);
            let blob = jmap_email::download_blob(client, &email.blob_id).await?;
            let maildir_id = store::store_message(&maildir, &blob, &flags)?;

            let message_id = email
                .message_id
                .as_ref()
                .and_then(|ids| ids.first())
                .cloned();
            let keywords_json = serde_json::to_string(&email.keywords)?;

            queries::upsert_message(
                conn,
                &MessageRecord {
                    jmap_email_id: email.id.clone(),
                    jmap_blob_id: Some(email.blob_id.clone()),
                    jmap_thread_id: Some(email.thread_id.clone()),
                    mailbox_id,
                    maildir_id: Some(maildir_id.clone()),
                    maildir_folder: Some(folder_name.clone()),
                    message_id,
                    flags: flags.clone(),
                    jmap_keywords: keywords_json,
                    size: Some(email.size as i64),
                    received_at: email.received_at.clone(),
                },
            )?;

            queries::upsert_local_state(
                conn,
                &maildir_id,
                &folder_name,
                &flags,
                Some(email.size as i64),
                None,
            )?;

            info!("Downloaded new email {} -> {}/{}", email.id, folder_name, maildir_id);
        }
    }

    Ok(())
}

async fn process_updated(
    client: &Client,
    conn: &Connection,
    updated_ids: &[String],
    mailboxes: &[(String, String)],
    maildir_root: &std::path::Path,
) -> Result<()> {
    let id_refs: Vec<&str> = updated_ids.iter().map(|s| s.as_str()).collect();

    for chunk in id_refs.chunks(50) {
        let emails = jmap_email::get_by_ids(client, chunk).await?;

        for email in &emails {
            let existing = queries::get_message_by_jmap_id(conn, &email.id)?;
            let Some(existing) = existing else {
                // Message is new to us (maybe was in a mailbox we weren't syncing before)
                debug!("Updated email {} not in local DB, treating as new", email.id);
                continue;
            };

            let new_flags = keywords_to_flags(&email.keywords);

            // Update flags if changed
            if let (Some(ref maildir_id), Some(ref folder)) =
                (&existing.maildir_id, &existing.maildir_folder)
            {
                if existing.flags != new_flags {
                    let maildir_path = maildir_root.join(folder);
                    let maildir = store::ensure_maildir(&maildir_path)?;
                    store::set_flags(&maildir, maildir_id, &new_flags)?;

                    queries::upsert_local_state(conn, maildir_id, folder, &new_flags, None, None)?;

                    info!(
                        "Updated flags for {} ({}): '{}' -> '{}'",
                        email.id, maildir_id, existing.flags, new_flags
                    );
                }
            }

            // Update the message record
            let keywords_json = serde_json::to_string(&email.keywords)?;
            let folder = find_folder_for_email(email, mailboxes);
            let (mailbox_id, folder_name) = folder.unwrap_or((
                existing.mailbox_id.clone(),
                existing.maildir_folder.clone().unwrap_or_default(),
            ));

            queries::upsert_message(
                conn,
                &MessageRecord {
                    jmap_email_id: email.id.clone(),
                    jmap_blob_id: Some(email.blob_id.clone()),
                    jmap_thread_id: Some(email.thread_id.clone()),
                    mailbox_id,
                    maildir_id: existing.maildir_id.clone(),
                    maildir_folder: Some(folder_name),
                    message_id: existing.message_id.clone(),
                    flags: new_flags,
                    jmap_keywords: keywords_json,
                    size: Some(email.size as i64),
                    received_at: email.received_at.clone(),
                },
            )?;
        }
    }

    Ok(())
}

fn process_destroyed(
    conn: &Connection,
    destroyed_ids: &[String],
    maildir_root: &std::path::Path,
) -> Result<()> {
    for jmap_id in destroyed_ids {
        let existing = queries::get_message_by_jmap_id(conn, jmap_id)?;
        if let Some(existing) = existing {
            if let (Some(ref maildir_id), Some(ref folder)) =
                (&existing.maildir_id, &existing.maildir_folder)
            {
                let maildir_path = maildir_root.join(folder);
                let maildir = store::ensure_maildir(&maildir_path)?;
                if let Err(e) = store::delete_message(&maildir, maildir_id) {
                    debug!("Failed to delete local message {} (may already be gone): {}", maildir_id, e);
                }
                queries::delete_local_state(conn, maildir_id)?;
            }
            queries::delete_message_by_jmap_id(conn, jmap_id)?;
            info!("Deleted local copy of destroyed email {}", jmap_id);
        }
    }
    Ok(())
}

fn find_folder_for_email(
    email: &crate::jmap::types::EmailObject,
    mailboxes: &[(String, String)],
) -> Option<(String, String)> {
    for (mailbox_id, folder_name) in mailboxes {
        if email.mailbox_ids.contains_key(mailbox_id) {
            return Some((mailbox_id.clone(), folder_name.clone()));
        }
    }
    None
}
