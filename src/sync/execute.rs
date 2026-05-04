use anyhow::Result;
use futures_util::stream::{self, StreamExt};
use jmap_client::client::Client;
use rusqlite::Connection;
use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::ids::{JmapAccountId, JmapEmailId};
use crate::jmap::email::{self as jmap_email, EmailSetOp};
use crate::jmap::limits;
use crate::jmap::retry::is_transient_error;
use crate::maildir_ops::{flags::keywords_to_flags, store};
use crate::state::queries::{self, MessageRecord};
use crate::sync::engine::SyncOutcome;
use crate::sync::plan::{BoundId, RemoteId, SyncAction, SyncPlan};

/// Walk a SyncPlan in dependency order:
/// adopt → download → local-flags → local-move → local-delete →
/// upload → remote-keywords → remote-move → remote-destroy.
///
/// Adoptions run first so subsequent actions on the same maildir_id /
/// jmap_email_id see the binding. Server-side mutations come last so
/// pull-side state is settled before we report it back.
pub async fn execute(
    client: &Client,
    conn: &Connection,
    config: &Config,
    plan: SyncPlan,
    maildir_root: &Path,
    account_id: &JmapAccountId,
) -> Result<SyncOutcome> {
    let mut concurrency = limits::concurrent_requests(client, config.sync.download_concurrency);
    if concurrency != config.sync.download_concurrency {
        info!(
            "Clamped download concurrency from {} to {} per server maxConcurrentRequests",
            config.sync.download_concurrency, concurrency
        );
    }

    let SyncPlan {
        actions,
        new_email_state,
        new_mailbox_state: _,
    } = plan;

    let mut unconditional_adopts = Vec::new();
    let mut move_pair_adopts = Vec::new();
    let mut downloads = Vec::new();
    let mut local_flags = Vec::new();
    let mut local_moves = Vec::new();
    let mut local_deletes = Vec::new();
    let mut uploads = Vec::new();
    let mut remote_keywords = Vec::new();
    let mut remote_moves = Vec::new();
    let mut remote_destroys = Vec::new();

    for action in actions {
        match action {
            // A move-pair adopt mirrors the DB half of a local cross-folder
            // move and is paired with a MoveRemote in the same plan. Defer
            // it until after the MoveRemote has actually landed on the
            // server -- otherwise a per-id MoveRemote failure leaves the
            // DB advanced to the destination while the server still has
            // the email at the source, and the next reconcile cycle's
            // divergence check emits a backwards MoveLocal.
            SyncAction::AdoptLocalMessage {
                old_maildir_id: Some(_),
                ..
            } => move_pair_adopts.push(action),
            SyncAction::AdoptLocalMessage { .. } => unconditional_adopts.push(action),
            SyncAction::DownloadMessage { .. } => downloads.push(action),
            SyncAction::UpdateLocalFlags { .. } => local_flags.push(action),
            SyncAction::MoveLocal { .. } => local_moves.push(action),
            SyncAction::DeleteLocal { .. } => local_deletes.push(action),
            SyncAction::UploadMessage { .. } => uploads.push(action),
            SyncAction::UpdateRemoteKeywords { .. } => remote_keywords.push(action),
            SyncAction::MoveRemote { .. } => remote_moves.push(action),
            SyncAction::DestroyRemote { .. } => remote_destroys.push(action),
        }
    }

    adopt_messages(conn, unconditional_adopts)?;
    let downloaded = run_downloads(client, conn, downloads, maildir_root, &mut concurrency).await?;
    update_local_flags(conn, local_flags, maildir_root)?;
    move_local_messages(conn, local_moves, maildir_root)?;
    delete_local_messages(conn, local_deletes, maildir_root)?;
    upload_messages(client, conn, uploads).await?;
    let outcome =
        apply_remote_set(client, conn, remote_keywords, remote_moves, remote_destroys).await?;
    apply_move_pair_adopts(conn, move_pair_adopts, &outcome.failed_updates)?;
    let failed_remote_actions = outcome.failed_updates.len() + outcome.failed_destroys.len();

    // Intentionally outside the per-phase transactions: if the process
    // dies between the last phase commit and this write, the cursor
    // stays at the previous value and the next cycle replays
    // Email/changes against the already-mirrored DB. Idempotent.
    if let Some(state) = new_email_state {
        queries::set_jmap_state(conn, account_id.as_ref(), "Email", &state)?;
        debug!("Persisted new Email state: {}", state);
    }

    Ok(SyncOutcome {
        downloaded,
        failed_remote_actions,
    })
}

/// Bind already-on-server messages to existing local files (DB only).
/// Pure-DB phase, so the whole loop runs in one transaction: a panic
/// or error mid-loop rolls the entire phase back instead of leaving
/// half the adoptions committed.
fn adopt_messages(conn: &Connection, actions: Vec<SyncAction>) -> Result<()> {
    if actions.is_empty() {
        return Ok(());
    }
    let txn = conn.unchecked_transaction()?;
    for action in actions {
        commit_adopt(&txn, action)?;
    }
    txn.commit()?;
    Ok(())
}

/// Apply the move-pair adopts deferred from `execute()` until after
/// `apply_remote_set`. Skip any whose paired MoveRemote landed in
/// `failed_updates`: the server still has the email at the source,
/// so committing the DB to the destination would diverge the two
/// sides and produce a backwards MoveLocal next cycle.
fn apply_move_pair_adopts(
    conn: &Connection,
    actions: Vec<SyncAction>,
    failed_updates: &HashSet<JmapEmailId>,
) -> Result<()> {
    if actions.is_empty() {
        return Ok(());
    }
    let txn = conn.unchecked_transaction()?;
    for action in actions {
        let SyncAction::AdoptLocalMessage { ref id, .. } = action else {
            continue;
        };
        if failed_updates.contains(&id.jmap_email_id) {
            warn!(
                "Skipping move-pair adopt for {}: paired MoveRemote rejected by server. \
                 DB retains source folder; the move will be re-detected and re-attempted next cycle.",
                id
            );
            continue;
        }
        commit_adopt(&txn, action)?;
    }
    txn.commit()?;
    Ok(())
}

/// DB writes for a single AdoptLocalMessage. Shared between the
/// up-front adopt phase and the post-remote-set move-pair phase so
/// both go through the same row-shape and ordering.
fn commit_adopt(conn: &Connection, action: SyncAction) -> Result<()> {
    let SyncAction::AdoptLocalMessage {
        id,
        maildir_folder,
        jmap_blob_id,
        jmap_thread_id,
        mailbox_id,
        keywords,
        old_maildir_id,
    } = action
    else {
        return Ok(());
    };
    let bound_for_log = id.clone();
    let BoundId {
        maildir_id,
        jmap_email_id,
        message_id,
    } = id;
    if let Some(old) = old_maildir_id.as_ref() {
        queries::delete_local_state(conn, old)?;
    }
    let flags = keywords_to_flags(&keywords);
    let keywords_json = serde_json::to_string(&keywords)?;
    queries::upsert_message(
        conn,
        &MessageRecord {
            jmap_email_id: jmap_email_id.clone(),
            jmap_blob_id,
            jmap_thread_id,
            mailbox_id,
            maildir_id: Some(maildir_id.clone()),
            maildir_folder: Some(maildir_folder.clone()),
            message_id,
            flags: flags.clone(),
            jmap_keywords: keywords_json,
        },
    )?;
    queries::upsert_local_state(conn, &maildir_id, &maildir_folder, &flags, None)?;
    debug!(
        "Adopted {}/{} as {}",
        maildir_folder,
        maildir_id,
        bound_for_log.as_remote()
    );
    Ok(())
}

fn update_local_flags(
    conn: &Connection,
    actions: Vec<SyncAction>,
    maildir_root: &Path,
) -> Result<()> {
    for action in actions {
        let SyncAction::UpdateLocalFlags {
            id,
            maildir_folder,
            new_flags,
            keywords,
            jmap_blob_id,
            jmap_thread_id,
            mailbox_id,
        } = action
        else {
            continue;
        };
        let bound_for_log = id.clone();
        let BoundId {
            maildir_id,
            jmap_email_id,
            message_id,
        } = id;
        let maildir_path = maildir_root.join(&maildir_folder);
        let maildir = store::ensure_maildir(&maildir_path)?;
        if let Err(e) = store::set_flags(&maildir, maildir_id.as_ref(), &new_flags) {
            warn!(
                "Failed to set flags for {} in {}: {}",
                maildir_id, maildir_folder, e
            );
            continue;
        }
        let keywords_json = serde_json::to_string(&keywords)?;
        // Per-iteration transaction: pair the two DB writes so this
        // row's message_map and local_state never disagree on flags
        // even if the second write fails.
        let txn = conn.unchecked_transaction()?;
        queries::upsert_message(
            &txn,
            &MessageRecord {
                jmap_email_id,
                jmap_blob_id: Some(jmap_blob_id),
                jmap_thread_id: Some(jmap_thread_id),
                mailbox_id,
                maildir_id: Some(maildir_id.clone()),
                maildir_folder: Some(maildir_folder.clone()),
                message_id,
                flags: new_flags.clone(),
                jmap_keywords: keywords_json,
            },
        )?;
        queries::upsert_local_state(&txn, &maildir_id, &maildir_folder, &new_flags, None)?;
        txn.commit()?;
        info!("Updated local flags for {}: '{}'", bound_for_log, new_flags);
    }
    Ok(())
}

fn move_local_messages(
    conn: &Connection,
    actions: Vec<SyncAction>,
    maildir_root: &Path,
) -> Result<()> {
    for action in actions {
        let SyncAction::MoveLocal {
            id,
            from_folder,
            to_folder,
        } = action
        else {
            continue;
        };
        let bound_for_log = id.clone();
        let BoundId {
            maildir_id,
            jmap_email_id,
            ..
        } = id;
        let from = store::ensure_maildir(&maildir_root.join(&from_folder))?;
        let to = store::ensure_maildir(&maildir_root.join(&to_folder))?;
        if let Err(e) = store::move_message(&from, &to, maildir_id.as_ref()) {
            warn!(
                "Failed to move {} from {} to {}: {}",
                bound_for_log, from_folder, to_folder, e
            );
            continue;
        }
        // Per-iteration transaction: the read, the conditional upsert
        // of message_map, and the upsert of local_state must all land
        // together so this row's two tables never disagree on the
        // destination folder. With DEFERRED semantics the writer lock
        // is only taken on the first write, so an external (non-engine)
        // writer that bypasses the flock could in principle slip a
        // change in between the read and the write -- accepted as
        // layer-3 divergence; the next sync cycle re-reconciles.
        let txn = conn.unchecked_transaction()?;
        let flags = if let Some(rec) = queries::get_message_by_jmap_id(&txn, &jmap_email_id)? {
            let preserved_flags = rec.flags.clone();
            queries::upsert_message(
                &txn,
                &MessageRecord {
                    maildir_folder: Some(to_folder.clone()),
                    ..rec
                },
            )?;
            preserved_flags
        } else {
            String::new()
        };
        queries::upsert_local_state(&txn, &maildir_id, &to_folder, &flags, None)?;
        txn.commit()?;
        info!(
            "Moved {} from {} to {}",
            bound_for_log, from_folder, to_folder
        );
    }
    Ok(())
}

fn delete_local_messages(
    conn: &Connection,
    actions: Vec<SyncAction>,
    maildir_root: &Path,
) -> Result<()> {
    for action in actions {
        let SyncAction::DeleteLocal { id, maildir_folder } = action else {
            continue;
        };
        let bound_for_log = id.clone();
        let BoundId {
            maildir_id,
            jmap_email_id,
            ..
        } = id;
        let maildir = store::ensure_maildir(&maildir_root.join(&maildir_folder))?;
        if let Err(e) = store::delete_message(&maildir, maildir_id.as_ref()) {
            debug!(
                "Failed to delete local {} (may already be gone): {}",
                maildir_id, e
            );
        }
        // Per-iteration transaction: the local_state delete and the
        // message_map delete must both land or neither, so a failure
        // mid-pair doesn't leave an orphan row that would re-emit
        // DeletedMessage every cycle.
        let txn = conn.unchecked_transaction()?;
        queries::delete_local_state(&txn, &maildir_id)?;
        queries::delete_message_by_jmap_id(&txn, &jmap_email_id)?;
        txn.commit()?;
        info!("Deleted local copy of destroyed {}", bound_for_log);
    }
    Ok(())
}

async fn upload_messages(
    client: &Client,
    conn: &Connection,
    actions: Vec<SyncAction>,
) -> Result<()> {
    for action in actions {
        let SyncAction::UploadMessage {
            id,
            maildir_folder,
            file_path,
            mailbox_id,
            flags,
        } = action
        else {
            continue;
        };
        let raw_message = match std::fs::read(&file_path) {
            Ok(b) => b,
            Err(e) => {
                warn!("Failed to read {} for upload: {}", file_path.display(), e);
                continue;
            }
        };
        let keywords = crate::maildir_ops::flags::flags_to_keywords(&flags);

        let result = jmap_email::import_email(
            client,
            &raw_message,
            mailbox_id.as_ref(),
            &maildir_folder,
            &id,
            &keywords,
        )
        .await;
        match result {
            Ok(jmap_email_id) => {
                let keywords_json = serde_json::to_string(&keywords)?;
                // Per-iteration transaction: pair the message_map and
                // local_state upserts so a failure between them can't
                // leave the just-uploaded message visible in only one
                // of the two tables.
                let txn = conn.unchecked_transaction()?;
                queries::upsert_message(
                    &txn,
                    &MessageRecord {
                        jmap_email_id: jmap_email_id.clone(),
                        jmap_blob_id: None,
                        jmap_thread_id: None,
                        mailbox_id: mailbox_id.clone(),
                        maildir_id: Some(id.maildir_id.clone()),
                        maildir_folder: Some(maildir_folder.clone()),
                        message_id: id.message_id.clone(),
                        flags: flags.clone(),
                        jmap_keywords: keywords_json,
                    },
                )?;
                queries::upsert_local_state(&txn, &id.maildir_id, &maildir_folder, &flags, None)?;
                txn.commit()?;
                let target = RemoteId {
                    jmap_email_id,
                    message_id: id.message_id.clone(),
                };
                info!("Uploaded local message {} -> {}", id.maildir_id, target);
            }
            Err(e) => {
                let s = e.to_string();
                if s.contains("alreadyExists") {
                    // Reconcile's adopt path should have caught this -
                    // a same-Message-ID server email already exists in
                    // a folder we know about. Hitting this branch
                    // means we raced another writer (another mail
                    // client uploaded the same message between our
                    // scan and our import), or our message_map index
                    // is missing a row reconcile would have used.
                    // Either way it's self-healing: the next cycle
                    // sees the server's copy and adopts. Don't fail
                    // the run.
                    warn!(
                        "Upload of {} from {} hit alreadyExists; skipping. Reconcile will adopt the existing server copy on the next cycle.",
                        id, maildir_folder
                    );
                } else {
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

/// Collapse all remote-side mutations onto a single Email/set call.
///
/// JMAP lets us put arbitrary `update` and `destroy` entries in one
/// method call; we exploit that to send keyword patches, mailbox
/// moves, and destroys together. After the server confirms, we mirror
/// each successful op into the local DB. Per-id failures are skipped
/// so we don't drift the DB out of sync with the server.
async fn apply_remote_set(
    client: &Client,
    conn: &Connection,
    keywords: Vec<SyncAction>,
    moves: Vec<SyncAction>,
    destroys: Vec<SyncAction>,
) -> Result<jmap_email::EmailSetOutcome> {
    let mut ops: Vec<EmailSetOp> = Vec::new();
    for action in &keywords {
        if let SyncAction::UpdateRemoteKeywords { id, keywords } = action {
            ops.push(EmailSetOp::Keywords {
                email_id: id.jmap_email_id.clone(),
                keywords: keywords.clone(),
            });
        }
    }
    for action in &moves {
        if let SyncAction::MoveRemote {
            id,
            target_mailbox_ids,
            ..
        } = action
        {
            ops.push(EmailSetOp::SetMailboxes {
                email_id: id.jmap_email_id.clone(),
                target_mailbox_ids: target_mailbox_ids.clone(),
            });
        }
    }
    for action in &destroys {
        if let SyncAction::DestroyRemote { id } = action {
            ops.push(EmailSetOp::Destroy {
                email_id: id.jmap_email_id.clone(),
            });
        }
    }

    if ops.is_empty() {
        return Ok(jmap_email::EmailSetOutcome::default());
    }

    let outcome = jmap_email::set_email_batch(client, &ops).await?;

    // One transaction wraps the entire post-network mirroring section.
    // The server's outcome is fixed; either we reflect all of it into
    // the local DB or none of it (next cycle re-detects the missing
    // mirrorings via Email/changes and retries). Pure-DB after this
    // point, so the txn is short-lived.
    let txn = conn.unchecked_transaction()?;

    // Mirror keyword updates into the local DB.
    for action in keywords {
        let SyncAction::UpdateRemoteKeywords { id, keywords } = action else {
            continue;
        };
        if outcome.failed_updates.contains(&id.jmap_email_id) {
            warn!(
                "UpdateRemoteKeywords for {} rejected by server. \
                 DB retains stale keywords; will be re-detected and re-attempted next cycle.",
                id
            );
            continue;
        }
        if let Some(rec) = queries::get_message_by_jmap_id(&txn, &id.jmap_email_id)? {
            let keywords_json = serde_json::to_string(&keywords)?;
            let flags = keywords_to_flags(&keywords);
            let maildir_id = rec.maildir_id.clone();
            let maildir_folder = rec.maildir_folder.clone();
            queries::upsert_message(
                &txn,
                &MessageRecord {
                    flags: flags.clone(),
                    jmap_keywords: keywords_json,
                    ..rec
                },
            )?;
            if let (Some(mid), Some(folder)) = (maildir_id, maildir_folder) {
                queries::upsert_local_state(&txn, &mid, &folder, &flags, None)?;
            }
        }
        info!("Updated remote keywords for {}", id);
    }

    // Mirror moves: log success on the happy path, and warn with full
    // from/to context on failure so the user can correlate the per-id
    // notUpdated entry from the JMAP layer with the intended operation.
    // The paired AdoptLocalMessage is held back in `apply_move_pair_adopts`,
    // which logs the DB consequence separately.
    for action in moves {
        let SyncAction::MoveRemote {
            id,
            from_folder,
            to_folder,
            ..
        } = action
        else {
            continue;
        };
        if outcome.failed_updates.contains(&id.jmap_email_id) {
            warn!(
                "MoveRemote {} from {} to {} rejected by server. \
                 DB retains source folder; will be re-detected and re-attempted next cycle.",
                id, from_folder, to_folder
            );
        } else {
            info!("Moved remote {} from {} to {}", id, from_folder, to_folder);
        }
    }

    // Mirror destroys. Look up the maildir_id binding before
    // deleting the message_map row so we can also clear the
    // matching local_state row -- otherwise scan would keep
    // emitting DeletedMessage for the orphan every cycle.
    for action in destroys {
        let SyncAction::DestroyRemote { id } = action else {
            continue;
        };
        if outcome.failed_destroys.contains(&id.jmap_email_id) {
            warn!(
                "DestroyRemote {} rejected by server. \
                 Local file is already gone; server still holds the message. \
                 Next scan will re-emit DeletedMessage and the destroy will be re-attempted.",
                id
            );
            continue;
        }
        if let Some(rec) = queries::get_message_by_jmap_id(&txn, &id.jmap_email_id)?
            && let Some(mid) = rec.maildir_id.as_ref()
        {
            queries::delete_local_state(&txn, mid)?;
        }
        queries::delete_message_by_jmap_id(&txn, &id.jmap_email_id)?;
        info!("Destroyed remote {}", id);
    }

    txn.commit()?;
    Ok(outcome)
}

/// Run all DownloadMessage actions concurrently with the rate-limit
/// halving behavior the old pull path had.
async fn run_downloads(
    client: &Client,
    conn: &Connection,
    actions: Vec<SyncAction>,
    maildir_root: &Path,
    concurrency: &mut usize,
) -> Result<usize> {
    if actions.is_empty() {
        return Ok(0);
    }
    let mut downloaded = 0usize;
    let mut pending = actions;

    while !pending.is_empty() {
        let n = (*concurrency).max(1);
        let futures = pending.iter().enumerate().map(|(i, action)| {
            let blob_id = match action {
                SyncAction::DownloadMessage { jmap_blob_id, .. } => jmap_blob_id.clone(),
                _ => unreachable!("non-download in downloads bucket"),
            };
            async move {
                let res = jmap_email::download_blob(client, &blob_id).await;
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
                    if is_transient_error(&e) {
                        rate_limited = true;
                        if let SyncAction::DownloadMessage { id, .. } = &pending[i] {
                            debug!("Rate-limited downloading email {}: {}", id, e);
                        }
                    } else if hard_error.is_none() {
                        hard_error = Some(e);
                    }
                }
            }
        }
        drop(stream);

        let succeeded_idx: HashSet<usize> = succeeded.iter().map(|(i, _)| *i).collect();

        for (i, blob) in &succeeded {
            if let SyncAction::DownloadMessage {
                id,
                jmap_blob_id,
                jmap_thread_id,
                mailbox_id,
                maildir_folder,
                keywords,
            } = &pending[*i]
            {
                let flags = keywords_to_flags(keywords);
                let maildir_path = maildir_root.join(maildir_folder);
                let maildir = store::ensure_maildir(&maildir_path)?;
                let mid = store::store_message(&maildir, blob, &flags)?;
                info!("Downloaded new email {} -> {}/{}", id, maildir_folder, mid);
                let keywords_json = serde_json::to_string(keywords)?;
                // Per-iteration transaction: pair the message_map and
                // local_state upserts so the just-stored maildir file
                // doesn't end up bound in only one of the two tables.
                let txn = conn.unchecked_transaction()?;
                queries::upsert_message(
                    &txn,
                    &MessageRecord {
                        jmap_email_id: id.jmap_email_id.clone(),
                        jmap_blob_id: Some(jmap_blob_id.clone()),
                        jmap_thread_id: Some(jmap_thread_id.clone()),
                        mailbox_id: mailbox_id.clone(),
                        maildir_id: Some(mid.clone()),
                        maildir_folder: Some(maildir_folder.clone()),
                        message_id: id.message_id.clone(),
                        flags: flags.clone(),
                        jmap_keywords: keywords_json,
                    },
                )?;
                queries::upsert_local_state(&txn, &mid, maildir_folder, &flags, None)?;
                txn.commit()?;
                downloaded += 1;
            }
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
            anyhow::bail!(
                "Download stream stalled with {} items remaining",
                pending.len()
            );
        }
    }

    Ok(downloaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{JmapEmailId, MaildirId};
    use crate::state::db;
    use std::collections::HashMap;

    fn seed_inbox_record(conn: &Connection) {
        queries::upsert_message(
            conn,
            &MessageRecord {
                jmap_email_id: "E1".into(),
                jmap_blob_id: Some("B1".into()),
                jmap_thread_id: Some("T1".into()),
                mailbox_id: "MB-INBOX".into(),
                maildir_id: Some("M-OLD".into()),
                maildir_folder: Some("INBOX".into()),
                message_id: "a@x".into(),
                flags: "S".into(),
                jmap_keywords: r#"{"$seen":true}"#.into(),
            },
        )
        .unwrap();
        queries::upsert_local_state(conn, &MaildirId::from("M-OLD"), "INBOX", "S", None).unwrap();
    }

    fn move_pair_adopt() -> SyncAction {
        let mut keywords = HashMap::new();
        keywords.insert("$seen".to_string(), true);
        SyncAction::AdoptLocalMessage {
            id: BoundId {
                maildir_id: "M-NEW".into(),
                jmap_email_id: "E1".into(),
                message_id: "a@x".into(),
            },
            maildir_folder: "Spam".into(),
            jmap_blob_id: Some("B1".into()),
            jmap_thread_id: Some("T1".into()),
            mailbox_id: "MB-SPAM".into(),
            keywords,
            old_maildir_id: Some("M-OLD".into()),
        }
    }

    /// When a paired MoveRemote lands in `failed_updates`, the move-pair
    /// adopt must NOT commit. Otherwise the local DB advances to the
    /// destination folder while the server still has the email at the
    /// source, and the next reconcile cycle's mailbox-divergence check
    /// at handle_known_remote emits a backwards MoveLocal to "fix" the
    /// phantom drift -- silently undoing the user's local move.
    #[test]
    fn move_pair_adopt_skipped_when_move_remote_fails() {
        let conn = db::open_in_memory().unwrap();
        seed_inbox_record(&conn);

        let mut failed: HashSet<JmapEmailId> = HashSet::new();
        failed.insert(JmapEmailId::from("E1"));

        apply_move_pair_adopts(&conn, vec![move_pair_adopt()], &failed).unwrap();

        let rec = queries::get_message_by_jmap_id(&conn, &JmapEmailId::from("E1"))
            .unwrap()
            .expect("E1 must still exist in message_map");
        assert_eq!(
            rec.maildir_folder.as_deref(),
            Some("INBOX"),
            "DB folder must remain INBOX -- server still has it there"
        );
        assert_eq!(
            rec.maildir_id.as_ref().map(AsRef::as_ref),
            Some("M-OLD"),
            "DB maildir_id must remain M-OLD -- the rebind to M-NEW is contingent on MoveRemote success"
        );

        // local_state for M-OLD must also remain so the next scan
        // pairs DeletedMessage(INBOX, M-OLD) + NewMessage(Spam, M-NEW)
        // correctly and the move pre-pass can re-emit the lossless
        // pair on the retry.
        let inbox_state = queries::get_local_state_for_folder(&conn, "INBOX").unwrap();
        assert!(
            inbox_state.contains_key("M-OLD"),
            "local_state(INBOX, M-OLD) must remain on a failed adopt"
        );
        let spam_state = queries::get_local_state_for_folder(&conn, "Spam").unwrap();
        assert!(
            !spam_state.contains_key("M-NEW"),
            "local_state(Spam, M-NEW) must not exist when adopt was skipped"
        );
    }

    /// Happy path counterpart: with no MoveRemote failure, the move-pair
    /// adopt commits and the DB rebinds to the destination folder /
    /// maildir_id, dropping the old local_state row.
    #[test]
    fn move_pair_adopt_committed_when_move_remote_succeeds() {
        let conn = db::open_in_memory().unwrap();
        seed_inbox_record(&conn);

        let failed: HashSet<JmapEmailId> = HashSet::new();

        apply_move_pair_adopts(&conn, vec![move_pair_adopt()], &failed).unwrap();

        let rec = queries::get_message_by_jmap_id(&conn, &JmapEmailId::from("E1"))
            .unwrap()
            .expect("E1 must still exist in message_map");
        assert_eq!(rec.maildir_folder.as_deref(), Some("Spam"));
        assert_eq!(rec.maildir_id.as_ref().map(AsRef::as_ref), Some("M-NEW"));

        let inbox_state = queries::get_local_state_for_folder(&conn, "INBOX").unwrap();
        assert!(
            !inbox_state.contains_key("M-OLD"),
            "old local_state row must be cleaned up after a successful adopt"
        );
        let spam_state = queries::get_local_state_for_folder(&conn, "Spam").unwrap();
        assert!(spam_state.contains_key("M-NEW"));
    }
}
