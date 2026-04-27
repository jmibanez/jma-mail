use anyhow::Result;
use jmap_client::client::Client;
use rusqlite::Connection;
use tracing::info;

use crate::jmap::email as jmap_email;
use crate::maildir_ops::flags::flags_to_keywords;
use crate::maildir_ops::scan::LocalChange;
use crate::state::queries;

/// Push local changes to the server.
pub async fn push(
    client: &Client,
    conn: &Connection,
    changes: Vec<LocalChange>,
    mailboxes: &[(String, String)], // (jmap_mailbox_id, maildir_folder)
) -> Result<()> {
    if changes.is_empty() {
        info!("No local changes to push");
        return Ok(());
    }

    info!("Pushing {} local changes to server", changes.len());

    for change in changes {
        match change {
            LocalChange::NewMessage {
                maildir_id,
                folder,
                flags,
                path,
            } => {
                push_new_message(client, conn, &maildir_id, &folder, &flags, &path, mailboxes)
                    .await?;
            }
            LocalChange::FlagsChanged {
                maildir_id,
                folder,
                old_flags: _,
                new_flags,
            } => {
                push_flag_change(client, conn, &maildir_id, &folder, &new_flags).await?;
            }
            LocalChange::DeletedMessage {
                maildir_id,
                folder: _,
            } => {
                push_delete(client, conn, &maildir_id).await?;
            }
        }
    }

    Ok(())
}

async fn push_new_message(
    client: &Client,
    conn: &Connection,
    maildir_id: &str,
    folder: &str,
    flags: &str,
    path: &std::path::Path,
    mailboxes: &[(String, String)],
) -> Result<()> {
    // Find the JMAP mailbox ID for this folder
    let mailbox_id = mailboxes
        .iter()
        .find(|(_, f)| f == folder)
        .map(|(id, _)| id.clone());

    let Some(mailbox_id) = mailbox_id else {
        info!(
            "Skipping upload of {} - folder {} not in synced mailboxes",
            maildir_id, folder
        );
        return Ok(());
    };

    let raw_message = std::fs::read(path)?;
    let keywords = flags_to_keywords(flags);

    let jmap_email_id =
        jmap_email::import_email(client, &raw_message, &mailbox_id, &keywords).await?;

    let keywords_json = serde_json::to_string(&keywords)?;

    queries::upsert_message(
        conn,
        &queries::MessageRecord {
            jmap_email_id: jmap_email_id.clone(),
            jmap_blob_id: None,
            jmap_thread_id: None,
            mailbox_id,
            maildir_id: Some(maildir_id.to_string()),
            maildir_folder: Some(folder.to_string()),
            message_id: None,
            flags: flags.to_string(),
            jmap_keywords: keywords_json,
            size: Some(raw_message.len() as i64),
            received_at: None,
        },
    )?;

    queries::upsert_local_state(
        conn,
        maildir_id,
        folder,
        flags,
        Some(raw_message.len() as i64),
        None,
    )?;

    info!(
        "Uploaded local message {} -> JMAP {}",
        maildir_id, jmap_email_id
    );
    Ok(())
}

async fn push_flag_change(
    client: &Client,
    conn: &Connection,
    maildir_id: &str,
    folder: &str,
    new_flags: &str,
) -> Result<()> {
    let msg = queries::get_message_by_maildir_id(conn, maildir_id)?;
    let Some(msg) = msg else {
        info!(
            "Skipping flag update for {} - not in message map",
            maildir_id
        );
        return Ok(());
    };

    let keywords = flags_to_keywords(new_flags);
    jmap_email::set_keywords(client, &msg.jmap_email_id, &keywords).await?;

    let keywords_json = serde_json::to_string(&keywords)?;
    queries::upsert_message(
        conn,
        &queries::MessageRecord {
            flags: new_flags.to_string(),
            jmap_keywords: keywords_json,
            ..msg
        },
    )?;

    queries::upsert_local_state(conn, maildir_id, folder, new_flags, None, None)?;

    info!("Pushed flag change for {} -> '{}'", maildir_id, new_flags);
    Ok(())
}

async fn push_delete(
    client: &Client,
    conn: &Connection,
    maildir_id: &str,
) -> Result<()> {
    let msg = queries::get_message_by_maildir_id(conn, maildir_id)?;
    let Some(msg) = msg else {
        info!(
            "Skipping delete for {} - not in message map",
            maildir_id
        );
        return Ok(());
    };

    jmap_email::destroy(client, &[&msg.jmap_email_id]).await?;

    queries::delete_message_by_jmap_id(conn, &msg.jmap_email_id)?;
    queries::delete_local_state(conn, maildir_id)?;

    info!(
        "Destroyed remote email {} for local delete {}",
        msg.jmap_email_id, maildir_id
    );
    Ok(())
}
