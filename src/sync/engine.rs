use anyhow::Result;
use jmap_client::client::Client;
use rusqlite::Connection;
use std::collections::HashMap;
use tracing::info;

use crate::config::Config;
use crate::jmap::{email as jmap_email, mailbox as jmap_mailbox};
use crate::maildir_ops::{dedupe, scan, store};
use crate::state::queries;
use crate::sync::{pull, push, reconcile};

/// Resolve the list of mailboxes to sync, returning (jmap_id, folder_name) pairs.
pub async fn resolve_mailboxes(
    client: &Client,
    conn: &Connection,
    config: &Config,
) -> Result<Vec<(String, String)>> {
    let remote_mailboxes = jmap_mailbox::get_all(client).await?;

    let mut synced = Vec::new();

    for mb in &remote_mailboxes {
        // Use the literal "INBOX" for the inbox role to match the mbsync
        // convention (and the magic alias accepted in [sync].mailboxes), so
        // pre-provisioned and sync-created folders agree regardless of the
        // server's display name (e.g. "Inbox", "Indbakke").
        let folder_name: String = if mb.role.as_deref() == Some("inbox") {
            "INBOX".to_string()
        } else {
            mb.name.clone()
        };

        // If mailboxes filter is set, only sync those
        if !jmap_mailbox::is_mailbox_synced(
            &config.sync.mailboxes,
            mb,
            config.sync.case_insensitive_match,
        ) {
            continue;
        }

        // Store in DB
        queries::upsert_mailbox(
            conn,
            &queries::MailboxRecord {
                jmap_mailbox_id: mb.id.clone(),
                name: mb.name.clone(),
                role: mb.role.clone(),
                parent_id: mb.parent_id.clone(),
                maildir_folder: folder_name.clone(),
                sort_order: mb.sort_order as i32,
            },
        )?;

        // Ensure local maildir exists
        let maildir_path = config.maildir_path().join(&folder_name);
        store::ensure_maildir(&maildir_path)?;

        synced.push((mb.id.clone(), folder_name));
    }

    info!("Syncing {} mailboxes", synced.len());
    Ok(synced)
}

/// Outcome of one sync iteration -- enough for the daemon loop to know
/// whether to fire the post-arrival hook.
#[derive(Debug, Default, Clone, Copy)]
pub struct SyncOutcome {
    pub downloaded: usize,
}

/// Run a full bidirectional sync.
pub async fn sync(
    client: &Client,
    conn: &Connection,
    config: &Config,
    dry_run: bool,
) -> Result<SyncOutcome> {
    let account_id = client.default_account_id().to_string();
    let mailboxes = resolve_mailboxes(client, conn, config).await?;
    let maildir_root = config.maildir_path();

    // Phase 0: dedupe local maildir by Message-ID and build an index for the
    // pull phase. Running this before scan/pull guarantees that any duplicate
    // jmapsync wrote in a previous run is removed before it can be picked up
    // as a "new local message" and pushed back to the server.
    let folder_names: Vec<String> = mailboxes.iter().map(|(_, f)| f.clone()).collect();
    let local_index = dedupe::dedupe_and_index(&maildir_root, &folder_names)?;

    // Phase 1: Get remote changes
    let email_state = queries::get_jmap_state(conn, &account_id, "Email")?;
    let remote_changes = if let Some(ref state) = email_state {
        match jmap_email::get_changes(client, state).await {
            Ok(changes) => Some(changes),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("Cannot calculate changes") {
                    info!("Server cannot calculate changes; clearing state for full re-sync");
                    queries::set_jmap_state(conn, &account_id, "Email", "")?;
                    None
                } else {
                    return Err(e);
                }
            }
        }
    } else {
        None
    };

    // Phase 2: Scan local changes
    let mut all_local_changes = Vec::new();
    for (_, folder_name) in &mailboxes {
        let maildir_path = maildir_root.join(folder_name);
        let maildir = store::ensure_maildir(&maildir_path)?;
        let known_state = queries::get_local_state_for_folder(conn, folder_name)?;
        let (changes, _seen) = scan::scan_folder(&maildir, folder_name, &known_state)?;
        all_local_changes.extend(changes);
    }

    if let Some(ref remote) = remote_changes {
        // Phase 3: Reconcile and build plan
        let mut known_by_maildir: HashMap<String, queries::MessageRecord> = HashMap::new();
        let mut known_by_jmap: HashMap<String, queries::MessageRecord> = HashMap::new();

        for (_, folder_name) in &mailboxes {
            let messages = queries::get_messages_by_folder(conn, folder_name)?;
            for msg in messages {
                if let Some(ref mid) = msg.maildir_id {
                    known_by_maildir.insert(
                        mid.clone(),
                        queries::MessageRecord {
                            jmap_email_id: msg.jmap_email_id.clone(),
                            jmap_blob_id: msg.jmap_blob_id.clone(),
                            jmap_thread_id: msg.jmap_thread_id.clone(),
                            mailbox_id: msg.mailbox_id.clone(),
                            maildir_id: msg.maildir_id.clone(),
                            maildir_folder: msg.maildir_folder.clone(),
                            message_id: msg.message_id.clone(),
                            flags: msg.flags.clone(),
                            jmap_keywords: msg.jmap_keywords.clone(),
                            size: msg.size,
                            received_at: msg.received_at.clone(),
                        },
                    );
                }
                known_by_jmap.insert(msg.jmap_email_id.clone(), msg);
            }
        }

        let plan = reconcile::reconcile(
            &remote.created,
            &remote.updated,
            &remote.destroyed,
            &all_local_changes,
            &known_by_maildir,
            &known_by_jmap,
            config.sync.conflict_strategy,
            &mailboxes,
            Some(remote.new_state.clone()),
        );

        if dry_run {
            print!("{}", plan);
            return Ok(SyncOutcome::default());
        }

        if plan.is_empty() && all_local_changes.is_empty() {
            info!("Already in sync");
            return Ok(SyncOutcome::default());
        }

        // Execute: pull phase
        let outcome = pull::pull(
            client,
            conn,
            &account_id,
            &mailboxes,
            &maildir_root,
            config.sync.max_messages,
            &local_index,
            config.sync.download_concurrency,
        )
        .await?;

        // Execute: push phase
        push::push(client, conn, all_local_changes, &mailboxes).await?;

        info!("Sync complete");
        Ok(SyncOutcome {
            downloaded: outcome.downloaded,
        })
    } else {
        // No previous state or cannotCalculateChanges -- do full pull then push
        if dry_run {
            println!("Full sync required (no previous state). Use without --dry-run to execute.");
            return Ok(SyncOutcome::default());
        }

        let outcome = pull::pull(
            client,
            conn,
            &account_id,
            &mailboxes,
            &maildir_root,
            config.sync.max_messages,
            &local_index,
            config.sync.download_concurrency,
        )
        .await?;

        push::push(client, conn, all_local_changes, &mailboxes).await?;

        info!("Sync complete");
        Ok(SyncOutcome {
            downloaded: outcome.downloaded,
        })
    }
}

/// Run pull only (server -> local).
pub async fn pull_only(client: &Client, conn: &Connection, config: &Config) -> Result<SyncOutcome> {
    let account_id = client.default_account_id().to_string();
    let mailboxes = resolve_mailboxes(client, conn, config).await?;
    let maildir_root = config.maildir_path();

    let folder_names: Vec<String> = mailboxes.iter().map(|(_, f)| f.clone()).collect();
    let local_index = dedupe::dedupe_and_index(&maildir_root, &folder_names)?;

    let outcome = pull::pull(
        client,
        conn,
        &account_id,
        &mailboxes,
        &maildir_root,
        config.sync.max_messages,
        &local_index,
        config.sync.download_concurrency,
    )
    .await?;

    info!("Pull complete");
    Ok(SyncOutcome {
        downloaded: outcome.downloaded,
    })
}

/// Run push only (local -> server).
pub async fn push_only(client: &Client, conn: &Connection, config: &Config) -> Result<()> {
    let mailboxes = resolve_mailboxes(client, conn, config).await?;
    let maildir_root = config.maildir_path();

    let folder_names: Vec<String> = mailboxes.iter().map(|(_, f)| f.clone()).collect();
    dedupe::dedupe_and_index(&maildir_root, &folder_names)?;

    let mut all_local_changes = Vec::new();
    for (_, folder_name) in &mailboxes {
        let maildir_path = maildir_root.join(folder_name);
        let maildir = store::ensure_maildir(&maildir_path)?;
        let known_state = queries::get_local_state_for_folder(conn, folder_name)?;
        let (changes, _seen) = scan::scan_folder(&maildir, folder_name, &known_state)?;
        all_local_changes.extend(changes);
    }

    push::push(client, conn, all_local_changes, &mailboxes).await?;

    info!("Push complete");
    Ok(())
}
