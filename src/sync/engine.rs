use anyhow::Result;
use jmap_client::client::Client;
use rusqlite::Connection;
use std::collections::HashMap;
use tracing::{info, warn};

use crate::config::Config;
use crate::jmap::{email as jmap_email, mailbox as jmap_mailbox, types::EmailObject};
use crate::maildir_ops::{dedupe, scan, store};
use crate::state::queries;
use crate::sync::execute;
use crate::sync::plan::{SyncAction, SyncDirection};
use crate::sync::reconcile;

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

/// Build the three message_map indices the reconcile step consumes.
fn build_known_indices(
    conn: &Connection,
    mailboxes: &[(String, String)],
) -> Result<(
    HashMap<String, queries::MessageRecord>,
    HashMap<String, queries::MessageRecord>,
    HashMap<String, Vec<queries::MessageRecord>>,
)> {
    let mut by_maildir: HashMap<String, queries::MessageRecord> = HashMap::new();
    let mut by_jmap: HashMap<String, queries::MessageRecord> = HashMap::new();
    let mut by_message_id: HashMap<String, Vec<queries::MessageRecord>> = HashMap::new();

    for (_, folder_name) in mailboxes {
        let messages = queries::get_messages_by_folder(conn, folder_name)?;
        for msg in messages {
            if let Some(ref mid) = msg.maildir_id {
                by_maildir.insert(mid.clone(), msg.clone());
            }
            if let Some(ref message_id) = msg.message_id {
                by_message_id
                    .entry(message_id.clone())
                    .or_default()
                    .push(msg.clone());
            }
            by_jmap.insert(msg.jmap_email_id.clone(), msg);
        }
    }
    Ok((by_maildir, by_jmap, by_message_id))
}

/// Outcome of one sync iteration -- enough for the daemon loop to know
/// whether to fire the post-arrival hook.
#[derive(Debug, Default, Clone, Copy)]
pub struct SyncOutcome {
    pub downloaded: usize,
}

/// Single orchestration path. `direction` selects which side(s) of the
/// plan execute; adoption always runs.
pub async fn run(
    client: &Client,
    conn: &Connection,
    config: &Config,
    dry_run: bool,
    direction: SyncDirection,
) -> Result<SyncOutcome> {
    let account_id = client.default_account_id().to_string();
    let mailboxes = resolve_mailboxes(client, conn, config).await?;
    let maildir_root = config.maildir_path();

    // Phase 0: dedupe + index. Always before scan so newly-introduced
    // duplicates from a prior aborted run don't get treated as local
    // changes to push.
    let folder_names: Vec<String> = mailboxes.iter().map(|(_, f)| f.clone()).collect();
    let local_index = dedupe::dedupe_and_index(&maildir_root, &folder_names)?;

    // Phase 1: scan local changes.
    let mut all_local_changes = Vec::new();
    for (_, folder_name) in &mailboxes {
        let maildir_path = maildir_root.join(folder_name);
        let maildir = store::ensure_maildir(&maildir_path)?;
        let known_state = queries::get_local_state_for_folder(conn, folder_name)?;
        let (changes, _seen) = scan::scan_folder(&maildir, folder_name, &known_state)?;
        all_local_changes.extend(changes);
    }

    // Phase 2: collect remote changes.
    let (remote_emails, remote_destroyed, new_state, used_initial_path) =
        fetch_remote_state(client, conn, &account_id, &mailboxes).await?;

    // Phase 3: build known indices and reconcile.
    let (known_by_maildir, known_by_jmap, known_by_message_id) =
        build_known_indices(conn, &mailboxes)?;

    let plan = reconcile::reconcile(
        &remote_emails,
        &remote_destroyed,
        &all_local_changes,
        &known_by_maildir,
        &known_by_jmap,
        &known_by_message_id,
        &local_index,
        &mailboxes,
        config.sync.conflict_strategy,
        Some(new_state),
    );

    if dry_run {
        print!("{}", plan);
        return Ok(SyncOutcome::default());
    }

    if plan.is_empty() {
        info!("Already in sync");
        return Ok(SyncOutcome::default());
    }

    // Phase 4: filter by direction; warn on every dropped non-adoption
    // action so the user sees that pull-only / push-only suppressed
    // something they may have wanted.
    let (filtered, dropped) = plan.into_filtered(direction);
    log_dropped(direction, &dropped);

    // Phase 5: execute.
    let outcome =
        execute::execute(client, conn, config, filtered, &maildir_root, &account_id).await?;

    if used_initial_path {
        info!("Initial sync complete ({} downloaded)", outcome.downloaded);
    } else {
        info!("Sync complete ({} downloaded)", outcome.downloaded);
    }
    Ok(outcome)
}

/// Returns `(emails, destroyed, new_state, used_initial_path)`.
///
/// On state-present: loops Email/changes until has_more_changes is
/// false, accumulating ids; then Email/get on (created ∪ updated).
/// On no-state (or cannotCalculateChanges): Email/query per mailbox,
/// then Email/get; new_state via get_current_state.
async fn fetch_remote_state(
    client: &Client,
    conn: &Connection,
    account_id: &str,
    mailboxes: &[(String, String)],
) -> Result<(Vec<EmailObject>, Vec<String>, String, bool)> {
    let cursor = queries::get_jmap_state(conn, account_id, "Email")?;

    if let Some(state) = cursor {
        let mut current = state;
        let mut all_created: Vec<String> = Vec::new();
        let mut all_updated: Vec<String> = Vec::new();
        let mut all_destroyed: Vec<String> = Vec::new();

        let final_state = loop {
            let res = jmap_email::get_changes(client, &current).await;
            match res {
                Ok(changes) => {
                    all_created.extend(changes.created.clone());
                    all_updated.extend(changes.updated.clone());
                    all_destroyed.extend(changes.destroyed.clone());
                    let next = changes.new_state.clone();
                    if !changes.has_more_changes {
                        break next;
                    }
                    current = next;
                }
                Err(e) => {
                    let s = e.to_string();
                    if s.contains("Cannot calculate changes") {
                        info!("Server cannot calculate changes; falling back to initial pull");
                        queries::set_jmap_state(conn, account_id, "Email", "")?;
                        return initial_remote_state(client, mailboxes).await;
                    }
                    return Err(e);
                }
            }
        };

        let mut fetch_ids: Vec<String> = all_created;
        for u in all_updated {
            if !fetch_ids.contains(&u) {
                fetch_ids.push(u);
            }
        }
        let emails = batched_get(client, &fetch_ids).await?;
        Ok((emails, all_destroyed, final_state, false))
    } else {
        initial_remote_state(client, mailboxes).await
    }
}

async fn initial_remote_state(
    client: &Client,
    mailboxes: &[(String, String)],
) -> Result<(Vec<EmailObject>, Vec<String>, String, bool)> {
    let mut all_ids: Vec<String> = Vec::new();
    for (mailbox_id, _) in mailboxes {
        let ids = jmap_email::query_mailbox(client, mailbox_id).await?;
        for id in ids {
            if !all_ids.contains(&id) {
                all_ids.push(id);
            }
        }
    }
    let emails = batched_get(client, &all_ids).await?;
    let state = jmap_email::get_current_state(client).await?;
    Ok((emails, Vec::new(), state, true))
}

async fn batched_get(client: &Client, ids: &[String]) -> Result<Vec<EmailObject>> {
    let mut out: Vec<EmailObject> = Vec::new();
    for chunk in ids.chunks(50) {
        let id_refs: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();
        let batch = jmap_email::get_by_ids(client, &id_refs).await?;
        out.extend(batch);
    }
    Ok(out)
}

fn log_dropped(direction: SyncDirection, dropped: &[SyncAction]) {
    for a in dropped {
        match a {
            SyncAction::DownloadMessage {
                jmap_email_id,
                maildir_folder,
                ..
            } => warn!(
                "{:?}: dropped DownloadMessage {} -> {}",
                direction, jmap_email_id, maildir_folder
            ),
            SyncAction::UpdateLocalFlags {
                jmap_email_id,
                maildir_id,
                new_flags,
                ..
            } => warn!(
                "{:?}: dropped UpdateLocalFlags on {} ({}) -> '{}'",
                direction, maildir_id, jmap_email_id, new_flags
            ),
            SyncAction::DeleteLocal {
                jmap_email_id,
                maildir_id,
                ..
            } => warn!(
                "{:?}: dropped DeleteLocal {} ({})",
                direction, maildir_id, jmap_email_id
            ),
            SyncAction::MoveLocal {
                jmap_email_id,
                from_folder,
                to_folder,
                ..
            } => warn!(
                "{:?}: dropped MoveLocal {} {} -> {}",
                direction, jmap_email_id, from_folder, to_folder
            ),
            SyncAction::UploadMessage {
                maildir_id,
                maildir_folder,
                ..
            } => warn!(
                "{:?}: dropped UploadMessage {} from {}",
                direction, maildir_id, maildir_folder
            ),
            SyncAction::UpdateRemoteKeywords { jmap_email_id, .. } => warn!(
                "{:?}: dropped UpdateRemoteKeywords on {}",
                direction, jmap_email_id
            ),
            SyncAction::DestroyRemote { jmap_email_id } => {
                warn!("{:?}: dropped DestroyRemote {}", direction, jmap_email_id)
            }
            SyncAction::MoveRemote { jmap_email_id, .. } => {
                warn!("{:?}: dropped MoveRemote {}", direction, jmap_email_id)
            }
            // Adoption is always kept; it never appears here.
            SyncAction::AdoptLocalMessage { .. } => {}
        }
    }
}

/// Run a full bidirectional sync.
pub async fn sync(
    client: &Client,
    conn: &Connection,
    config: &Config,
    dry_run: bool,
) -> Result<SyncOutcome> {
    run(client, conn, config, dry_run, SyncDirection::Both).await
}

/// Run pull only (server -> local). Adoption still runs.
pub async fn pull_only(client: &Client, conn: &Connection, config: &Config) -> Result<SyncOutcome> {
    run(client, conn, config, false, SyncDirection::PullOnly).await
}

/// Run push only (local -> server). Adoption still runs.
pub async fn push_only(client: &Client, conn: &Connection, config: &Config) -> Result<()> {
    run(client, conn, config, false, SyncDirection::PushOnly).await?;
    Ok(())
}
