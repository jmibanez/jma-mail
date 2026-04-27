use anyhow::Result;
use futures_util::stream::{self, StreamExt};
use jmap_client::client::Client;
use rusqlite::Connection;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::jmap::email as jmap_email;
use crate::jmap::types::EmailObject;
use crate::maildir_ops::dedupe::{self, LocalIndex};
use crate::maildir_ops::{flags::keywords_to_flags, store};
use crate::state::queries::{self, MessageRecord};

/// Resolve the effective per-pull download concurrency by clamping the
/// configured value to the server's advertised `maxConcurrentRequests` so
/// we don't routinely earn rate-limit errors. `maxConcurrentRequests`
/// covers the *combined* method-call + blob endpoints, so we treat it as
/// the upper bound on parallel downloads.
fn effective_concurrency(client: &Client, configured: usize) -> usize {
    let session = client.session();
    let cap = session
        .core_capabilities()
        .map(|c| c.max_concurrent_requests());
    match cap {
        Some(server) => configured.min(server).max(1),
        None => configured.max(1),
    }
}

/// Substring-match the jmap-client error Display to detect a JMAP
/// request-level "limit" problem (RFC 8620 §3.6.1) or HTTP 429/503.
/// jmap-client formats `JMAPError::Limit` as the literal "Limit" inside
/// a `ProblemDetails` Display; for transport errors we fall back to
/// status code substrings since the typed status isn't surfaced.
fn is_rate_limit_error(err: &anyhow::Error) -> bool {
    let s = err.to_string();
    s.contains("Request failed: Limit")
        || s.contains("status 429")
        || s.contains("status 503")
        || s.contains("rateLimit")
}

/// Outcome of a pull cycle.
pub struct PullOutcome {
    /// New JMAP Email state cursor to persist.
    pub state: String,
    /// Number of messages actually downloaded + written to the maildir
    /// in this cycle. Excludes rebinds (Message-ID already present
    /// locally) and pure flag updates.
    pub downloaded: usize,
}

/// Pull new and changed messages from server to local maildir.
pub async fn pull(
    client: &Client,
    conn: &Connection,
    account_id: &str,
    mailboxes: &[(String, String)], // (jmap_mailbox_id, maildir_folder)
    maildir_root: &std::path::Path,
    max_messages: u64,
    index: &LocalIndex,
    download_concurrency: usize,
) -> Result<PullOutcome> {
    let mut concurrency = effective_concurrency(client, download_concurrency);
    if concurrency != download_concurrency {
        info!(
            "Clamped download concurrency from {} to {} per server maxConcurrentRequests",
            download_concurrency, concurrency
        );
    }

    let current_state = queries::get_jmap_state(conn, account_id, "Email")?;

    match current_state {
        None => {
            info!("No previous state -- performing initial pull");
            initial_pull(
                client,
                conn,
                account_id,
                mailboxes,
                maildir_root,
                max_messages,
                index,
                &mut concurrency,
            )
            .await
        }
        Some(state) => {
            info!("Delta pull from state: {}", state);
            delta_pull(
                client,
                conn,
                account_id,
                &state,
                mailboxes,
                maildir_root,
                index,
                &mut concurrency,
            )
            .await
        }
    }
}

/// Initial full pull: query all emails in each mailbox, download them.
///
/// When the local maildir already has files (e.g. it was populated by mbsync
/// or a previous run whose state DB was wiped), an adoption pre-pass
/// reverse-resolves local Message-IDs against the server and fills
/// `message_map` without downloading. The per-mailbox enumeration then only
/// fetches metadata + bodies for the residual server-side messages.
async fn initial_pull(
    client: &Client,
    conn: &Connection,
    account_id: &str,
    mailboxes: &[(String, String)],
    maildir_root: &std::path::Path,
    max_messages: u64,
    index: &LocalIndex,
    concurrency: &mut usize,
) -> Result<PullOutcome> {
    let mut downloaded = 0usize;
    // Adoption pass: if the maildir already has messages, populate
    // message_map by reverse-querying JMAP rather than walking the full
    // mailbox and Email/get'ing every metadata record.
    let folders: Vec<String> = mailboxes.iter().map(|(_, f)| f.clone()).collect();
    if !index.by_message_id.is_empty() {
        let mbsync = dedupe::detect_mbsync_state(maildir_root, &folders);
        let suffix = if mbsync { " (mbsync state detected)" } else { "" };
        info!(
            "Adopting {} existing local message(s) before initial pull{}",
            index.by_message_id.len(),
            suffix
        );
        let adopted = adopt_existing(client, conn, mailboxes, index).await?;
        info!(
            "Adopted {} of {} local message(s) from server metadata",
            adopted,
            index.by_message_id.len()
        );
    }

    // Snapshot known IDs after adoption so the per-mailbox loop can skip
    // metadata fetches for messages we just mapped.
    let known_jmap_ids: HashSet<String> = queries::get_all_jmap_email_ids(conn)?;

    for (mailbox_id, folder_name) in mailboxes {
        let max = if max_messages > 0 {
            Some(max_messages)
        } else {
            None
        };
        let email_ids = jmap_email::query_mailbox(client, mailbox_id, max).await?;

        let total = email_ids.len();
        let residual: Vec<String> = email_ids
            .into_iter()
            .filter(|id| !known_jmap_ids.contains(id))
            .collect();

        info!(
            "Mailbox {}: {} on server, {} already mapped, {} to fetch",
            folder_name,
            total,
            total - residual.len(),
            residual.len()
        );

        // Fetch metadata + body in batches of 50
        for chunk in residual.chunks(50) {
            let id_refs: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();
            let emails = jmap_email::get_by_ids(client, &id_refs).await?;

            let plans: Vec<IngestPlan> = emails
                .into_iter()
                .map(|email| IngestPlan {
                    email,
                    mailbox_id: mailbox_id.clone(),
                    folder_name: folder_name.clone(),
                })
                .collect();
            downloaded +=
                ingest_emails(client, conn, plans, maildir_root, index, concurrency).await?;
        }
    }

    // Bootstrap delta-sync state with a real Email state from the server.
    // Email/changes since "0" returns cannotCalculateChanges; use Email/get
    // with an empty id list to read the current state instead.
    let state = jmap_email::get_current_state(client).await?;

    queries::set_jmap_state(conn, account_id, "Email", &state)?;
    info!(
        "Initial pull complete. State: {} ({} downloaded)",
        state, downloaded
    );

    Ok(PullOutcome { state, downloaded })
}

/// Reverse-resolve local Message-IDs to JMAP email IDs and populate
/// `message_map` + `local_state` for the matches. No downloads, no maildir
/// writes -- the local files are taken as authoritative.
///
/// Verifies each match: the Email/get response must contain the local
/// Message-ID in its `messageId` array. Substring filter false-positives
/// (rare but possible) are dropped.
async fn adopt_existing(
    client: &Client,
    conn: &Connection,
    mailboxes: &[(String, String)],
    index: &LocalIndex,
) -> Result<usize> {
    let mids: Vec<String> = index.by_message_id.keys().cloned().collect();
    let resolved = jmap_email::resolve_by_message_ids(client, &mids).await?;
    if resolved.is_empty() {
        return Ok(0);
    }

    let resolved_ids: Vec<String> = resolved.values().cloned().collect();
    let mut adopted = 0usize;

    for chunk in resolved_ids.chunks(50) {
        let id_refs: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();
        let emails = jmap_email::get_by_ids(client, &id_refs).await?;

        for email in &emails {
            // Find which of our local Message-IDs this email actually carries.
            // Drops substring-match false positives.
            let local_mid = email
                .message_id
                .as_ref()
                .and_then(|ids| {
                    ids.iter()
                        .find(|m| index.by_message_id.contains_key(*m))
                        .cloned()
                });
            let Some(local_mid) = local_mid else {
                debug!(
                    "Adopt: server email {} has no Message-ID matching the local index, skipping",
                    email.id
                );
                continue;
            };
            let entries = match index.by_message_id.get(&local_mid) {
                Some(es) => es,
                None => continue,
            };

            // Pick a local copy whose folder corresponds to a JMAP mailbox
            // that the server says this email belongs to. JMAP allows
            // multi-mailbox membership; the message_map stores one binding
            // per email_id, so we record just one. Other on-disk copies in
            // other folders will be rebound on the per-mailbox pull pass via
            // lookup_existing.
            let bind = mailboxes.iter().find_map(|(mid, folder)| {
                if !email.mailbox_ids.contains_key(mid) {
                    return None;
                }
                entries
                    .iter()
                    .find(|e| &e.folder == folder)
                    .map(|e| (mid.clone(), e))
            });
            let Some((mailbox_id, entry)) = bind else {
                debug!(
                    "Adopt: server email {} (Message-ID <{}>) has no local copy in any of its mailboxes' folders",
                    email.id, local_mid
                );
                continue;
            };

            let flags = keywords_to_flags(&email.keywords);
            let keywords_json = serde_json::to_string(&email.keywords)?;

            queries::upsert_message(
                conn,
                &MessageRecord {
                    jmap_email_id: email.id.clone(),
                    jmap_blob_id: Some(email.blob_id.clone()),
                    jmap_thread_id: Some(email.thread_id.clone()),
                    mailbox_id,
                    maildir_id: Some(entry.maildir_id.clone()),
                    maildir_folder: Some(entry.folder.clone()),
                    message_id: Some(local_mid.clone()),
                    flags: flags.clone(),
                    jmap_keywords: keywords_json,
                    size: Some(email.size as i64),
                    received_at: email.received_at.clone(),
                },
            )?;

            queries::upsert_local_state(
                conn,
                &entry.maildir_id,
                &entry.folder,
                &flags,
                Some(email.size as i64),
                None,
            )?;

            adopted += 1;
        }
    }

    Ok(adopted)
}

/// Delta pull: use Email/changes to get only what changed since last sync.
async fn delta_pull(
    client: &Client,
    conn: &Connection,
    account_id: &str,
    since_state: &str,
    mailboxes: &[(String, String)],
    maildir_root: &std::path::Path,
    index: &LocalIndex,
    concurrency: &mut usize,
) -> Result<PullOutcome> {
    let mut state = since_state.to_string();
    let mut downloaded = 0usize;

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
                        index,
                        concurrency,
                    ))
                    .await;
                }
                return Err(e);
            }
        };

        // Handle created messages
        if !changes.created.is_empty() {
            downloaded += process_created(
                client,
                conn,
                &changes.created,
                mailboxes,
                maildir_root,
                index,
                concurrency,
            )
            .await?;
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
    info!(
        "Delta pull complete. New state: {} ({} downloaded)",
        state, downloaded
    );

    Ok(PullOutcome { state, downloaded })
}

async fn process_created(
    client: &Client,
    conn: &Connection,
    created_ids: &[String],
    mailboxes: &[(String, String)],
    maildir_root: &std::path::Path,
    index: &LocalIndex,
    concurrency: &mut usize,
) -> Result<usize> {
    let id_refs: Vec<&str> = created_ids.iter().map(|s| s.as_str()).collect();
    let mut downloaded = 0usize;

    for chunk in id_refs.chunks(50) {
        let emails = jmap_email::get_by_ids(client, chunk).await?;

        let plans: Vec<IngestPlan> = emails
            .into_iter()
            .filter_map(|email| match find_folder_for_email(&email, mailboxes) {
                Some((mailbox_id, folder_name)) => Some(IngestPlan {
                    email,
                    mailbox_id,
                    folder_name,
                }),
                None => {
                    debug!("Email {} not in any synced mailbox, skipping", email.id);
                    None
                }
            })
            .collect();

        if plans.is_empty() {
            continue;
        }
        downloaded +=
            ingest_emails(client, conn, plans, maildir_root, index, concurrency).await?;
    }

    Ok(downloaded)
}

/// One unit of pull-side work: an `EmailObject` that should land in the
/// given maildir folder under the given JMAP mailbox.
struct IngestPlan {
    email: EmailObject,
    mailbox_id: String,
    folder_name: String,
}

/// Reconcile a batch of remote emails into the local maildir + DB.
///
/// First sweeps rebinds (Message-ID already known locally — no download
/// required) inline, then drives the remaining downloads through a
/// `buffer_unordered` stream so up to `*concurrency` blob fetches run in
/// flight at once. Successes are committed to SQLite serially in the
/// consumer loop because `rusqlite::Connection` is `!Sync`.
///
/// On a JMAP "limit" / HTTP 429 / 503 response we halve `*concurrency`,
/// pause briefly, and retry only the failed items. The lowered
/// concurrency persists for the rest of the pull.
async fn ingest_emails(
    client: &Client,
    conn: &Connection,
    plans: Vec<IngestPlan>,
    maildir_root: &std::path::Path,
    index: &LocalIndex,
    concurrency: &mut usize,
) -> Result<usize> {
    if plans.is_empty() {
        return Ok(0);
    }
    let mut downloaded = 0usize;

    let mut downloads: Vec<IngestPlan> = Vec::with_capacity(plans.len());
    for plan in plans {
        let message_id = plan
            .email
            .message_id
            .as_ref()
            .and_then(|ids| ids.first())
            .cloned();
        let existing = message_id
            .as_deref()
            .and_then(|m| lookup_existing(conn, index, m, &plan.folder_name));
        match existing {
            Some((maildir_id, recorded_folder)) => {
                debug!(
                    "Rebinding existing local copy for email {} (Message-ID <{}>) at {}/{}",
                    plan.email.id,
                    message_id.as_deref().unwrap_or("?"),
                    recorded_folder,
                    maildir_id,
                );
                commit_email(
                    conn,
                    &plan.email,
                    &plan.mailbox_id,
                    &maildir_id,
                    &recorded_folder,
                )?;
            }
            None => downloads.push(plan),
        }
    }

    let mut pending = downloads;
    while !pending.is_empty() {
        let n = (*concurrency).max(1);
        let futures = pending.iter().enumerate().map(|(i, plan)| {
            let blob_id = plan.email.blob_id.as_str();
            async move {
                let res = jmap_email::download_blob(client, blob_id).await;
                (i, res)
            }
        });
        let mut stream = stream::iter(futures).buffer_unordered(n);

        let mut succeeded: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut rate_limited = false;
        let mut hard_error: Option<anyhow::Error> = None;

        while let Some((i, result)) = stream.next().await {
            match result {
                Ok(blob) => succeeded.push((i, blob)),
                Err(e) => {
                    if is_rate_limit_error(&e) {
                        rate_limited = true;
                        debug!(
                            "Rate-limited downloading email {}: {}",
                            pending[i].email.id, e
                        );
                    } else if hard_error.is_none() {
                        hard_error = Some(e);
                    }
                }
            }
        }
        drop(stream);

        let succeeded_idx: HashSet<usize> = succeeded.iter().map(|(i, _)| *i).collect();

        for (i, blob) in &succeeded {
            let plan = &pending[*i];
            let flags = keywords_to_flags(&plan.email.keywords);
            let maildir_path = maildir_root.join(&plan.folder_name);
            let maildir = store::ensure_maildir(&maildir_path)?;
            let mid = store::store_message(&maildir, blob, &flags)?;
            info!(
                "Downloaded new email {} -> {}/{}",
                plan.email.id, plan.folder_name, mid
            );
            commit_email(conn, &plan.email, &plan.mailbox_id, &mid, &plan.folder_name)?;
            downloaded += 1;
        }

        if let Some(e) = hard_error {
            return Err(e);
        }

        pending = pending
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !succeeded_idx.contains(i))
            .map(|(_, p)| p)
            .collect();

        if pending.is_empty() {
            break;
        }

        if rate_limited {
            let new = (n / 2).max(1);
            if new < n {
                warn!(
                    "Hit JMAP rate limit; lowering download concurrency from {} to {}",
                    n, new
                );
                *concurrency = new;
            } else {
                warn!(
                    "Hit JMAP rate limit at minimum concurrency ({}); backing off and retrying",
                    n
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        } else {
            // No rate-limit and no hard error, but pending still has items?
            // That would mean the stream closed without yielding for those
            // items, which buffer_unordered doesn't do. Bail rather than spin.
            anyhow::bail!(
                "Download stream stalled with {} items remaining",
                pending.len()
            );
        }
    }

    Ok(downloaded)
}

/// Persist a single email's mapping + local_state. Used by both the
/// rebind path (no download) and the download path (after maildir write).
fn commit_email(
    conn: &Connection,
    email: &EmailObject,
    mailbox_id: &str,
    maildir_id: &str,
    recorded_folder: &str,
) -> Result<()> {
    let flags = keywords_to_flags(&email.keywords);
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
            mailbox_id: mailbox_id.to_string(),
            maildir_id: Some(maildir_id.to_string()),
            maildir_folder: Some(recorded_folder.to_string()),
            message_id,
            flags: flags.clone(),
            jmap_keywords: keywords_json,
            size: Some(email.size as i64),
            received_at: email.received_at.clone(),
        },
    )?;

    queries::upsert_local_state(
        conn,
        maildir_id,
        recorded_folder,
        &flags,
        Some(email.size as i64),
        None,
    )?;

    Ok(())
}

/// Look up an existing local file for a Message-ID **in a specific folder**.
/// Tries the persistent `message_map` first, then falls back to the
/// in-memory index built by the dedupe pass (which covers the "DB nuked but
/// maildir intact" case). Folder-scoped because cross-folder copies of the
/// same Message-ID are distinct instances — a JMAP delivery for folder B
/// must not rebind against a local copy that happens to live in folder A.
fn lookup_existing(
    conn: &Connection,
    index: &LocalIndex,
    message_id: &str,
    target_folder: &str,
) -> Option<(String, String)> {
    if let Ok(Some(rec)) =
        queries::get_message_by_message_id_in_folder(conn, message_id, target_folder)
    {
        if let (Some(mid), Some(folder)) = (rec.maildir_id, rec.maildir_folder) {
            return Some((mid, folder));
        }
    }

    if let Some(entries) = index.by_message_id.get(message_id) {
        if let Some(entry) = entries.iter().find(|e| e.folder == target_folder) {
            return Some((entry.maildir_id.clone(), entry.folder.clone()));
        }
    }

    None
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
