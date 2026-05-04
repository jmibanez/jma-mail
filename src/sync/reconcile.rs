use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{debug, error, warn};

use crate::config::ConflictStrategy;
use crate::ids::{JmapBlobId, JmapEmailId, JmapMailboxId, JmapThreadId, MaildirId, MessageId};
use crate::jmap::types::EmailObject;
use crate::maildir_ops::dedupe::LocalIndex;
use crate::maildir_ops::flags::{flags_to_keywords, keywords_to_flags};
use crate::maildir_ops::scan::LocalChange;
use crate::state::queries::MessageRecord;
use crate::sync::plan::{BoundId, LocalId, RemoteId, SyncAction, SyncPlan};

/// Look up a `MessageRecord` (or records) by whichever ID kind you
/// happen to hold: a maildir basename, a JMAP email id, or an RFC
/// 5322 Message-ID. The Message-ID side is one-to-many because the
/// same Message-ID can legitimately appear in more than one folder.
/// Built once per cycle from `message_map` and consulted everywhere
/// reconcile needs to ask "do I already know about this?".
///
/// The three projections hold `Arc<MessageRecord>` rather than
/// `MessageRecord` by value: `build_known_indices` wraps each row
/// once and the projections share via refcount, instead of cloning
/// the full record into two of the three maps. Consumer code reads
/// fields through `Arc`'s deref so the call shape is unchanged.
#[derive(Default)]
pub struct MessageRecordIndex {
    pub by_maildir: HashMap<MaildirId, Arc<MessageRecord>>,
    pub by_jmap: HashMap<JmapEmailId, Arc<MessageRecord>>,
    pub by_message_id: HashMap<MessageId, Vec<Arc<MessageRecord>>>,
}

/// Bundle of immutable inputs and derived indices that every reconcile
/// helper needs. Passed by `&` so helpers stay short on parameters and
/// share a single source of truth for the cycle's data.
struct ReconcileCtx<'a> {
    remote_emails: &'a [EmailObject],
    mailboxes: &'a [(JmapMailboxId, String)],
    strategy: ConflictStrategy,
    known_by_maildir: &'a HashMap<MaildirId, Arc<MessageRecord>>,
    known_by_jmap: &'a HashMap<JmapEmailId, Arc<MessageRecord>>,
    known_by_message_id: &'a HashMap<MessageId, Vec<Arc<MessageRecord>>>,
    local_index: &'a LocalIndex,
    local_flag_changes: HashMap<JmapEmailId, &'a LocalChange>,
    local_deletes: HashSet<JmapEmailId>,
    destroyed_set: HashSet<&'a str>,
    max_upload_size: usize,
}

/// One reconcile cycle's inputs, gathered into a single struct so the
/// caller -- and the function signature -- doesn't have to thread eight
/// positional parameters.
///
/// `remote_emails`: Email/get result for the union of created+updated
/// (initial pull passes the full enumerated set here).
/// `remote_destroyed`: JMAP email IDs the server says are gone.
/// `local_changes`: scan output (NewMessage now carries Message-ID).
/// `known`: bundled indices of message_map for fast lookup.
/// `local_index`: dedupe-pass index of on-disk Message-IDs.
/// `mailboxes`: synced (jmap_mailbox_id, folder_name) pairs.
/// `strategy`: how to break local-vs-remote ties.
/// `new_email_state`: cursor to stamp into the resulting plan; `None`
/// for tests that don't care about state advancement.
pub struct ReconcileInput<'a> {
    pub remote_emails: &'a [EmailObject],
    pub remote_destroyed: &'a [JmapEmailId],
    pub local_changes: &'a [LocalChange],
    pub known: &'a MessageRecordIndex,
    pub local_index: &'a LocalIndex,
    pub mailboxes: &'a [(JmapMailboxId, String)],
    pub strategy: ConflictStrategy,
    pub new_email_state: Option<String>,
    /// Effective `maxSizeUpload` cap for this cycle. Resolved by the
    /// caller (engine) via `limits::max_size_upload(client)` so
    /// reconcile stays I/O-free. NewMessage entries whose
    /// `size_bytes` exceeds this don't get an `UploadMessage` in
    /// the plan.
    pub max_upload_size: usize,
}

/// Reconcile remote changes and local changes into a sync plan.
pub fn reconcile(input: ReconcileInput<'_>) -> SyncPlan {
    let ReconcileInput {
        remote_emails,
        remote_destroyed,
        local_changes,
        known,
        local_index,
        mailboxes,
        strategy,
        new_email_state,
        max_upload_size,
    } = input;
    let known_by_maildir = &known.by_maildir;
    let known_by_jmap = &known.by_jmap;
    let known_by_message_id = &known.by_message_id;
    let mut plan = SyncPlan {
        new_email_state,
        ..SyncPlan::default()
    };

    // Quickly look up "did the local side change flags on this JMAP id?"
    let local_flag_changes: HashMap<JmapEmailId, &LocalChange> = local_changes
        .iter()
        .filter_map(|lc| match lc {
            LocalChange::FlagsChanged { maildir_id, .. } => known_by_maildir
                .get(maildir_id)
                .map(|m| (m.jmap_email_id.clone(), lc)),
            _ => None,
        })
        .collect();

    let mut local_deletes: HashSet<JmapEmailId> = local_changes
        .iter()
        .filter_map(|lc| match lc {
            LocalChange::DeletedMessage { maildir_id, .. } => known_by_maildir
                .get(maildir_id)
                .map(|m| m.jmap_email_id.clone()),
            _ => None,
        })
        .collect();

    let destroyed_set: HashSet<&str> = remote_destroyed.iter().map(JmapEmailId::as_ref).collect();

    // Cross-folder local move detection: scan emits a paired
    // DeletedMessage(src) + NewMessage(dst) for a user-driven move.
    // Pair them by Message-ID so we can emit a single MoveRemote +
    // rebind, avoiding the lossy DestroyRemote + UploadMessage shape
    // (which would lose the JMAP id, thread, and keyword history).
    let news_by_message_id: HashMap<&MessageId, &LocalChange> = local_changes
        .iter()
        .filter_map(|c| match c {
            LocalChange::NewMessage { message_id, .. } => Some((message_id, c)),
            _ => None,
        })
        .collect();

    let mut detected_moves: Vec<DetectedMove> = Vec::new();
    let mut consumed_news: HashSet<MaildirId> = HashSet::new();
    let mut consumed_deletes: HashSet<JmapEmailId> = HashSet::new();

    for change in local_changes {
        let LocalChange::DeletedMessage {
            maildir_id: old_id,
            folder: src_folder,
        } = change
        else {
            continue;
        };
        let Some(rec) = known_by_maildir.get(old_id) else {
            continue;
        };
        let mid = &rec.message_id;
        let Some(LocalChange::NewMessage {
            maildir_id: new_id,
            folder: dst_folder,
            flags: new_flags,
            ..
        }) = news_by_message_id.get(mid).copied()
        else {
            continue;
        };
        if dst_folder == src_folder {
            continue;
        }
        let Some(dst_mailbox_id) = mailboxes
            .iter()
            .find(|(_, f)| f == dst_folder)
            .map(|(m, _)| m.clone())
        else {
            continue;
        };

        detected_moves.push(DetectedMove {
            jmap_email_id: rec.jmap_email_id.clone(),
            to_mailbox_id: dst_mailbox_id,
            old_maildir_id: old_id.clone(),
            new_maildir_id: new_id.clone(),
            from_folder: src_folder.clone(),
            new_folder: dst_folder.clone(),
            new_flags: new_flags.clone(),
            jmap_blob_id: rec.jmap_blob_id.clone(),
            jmap_thread_id: rec.jmap_thread_id.clone(),
            message_id: mid.clone(),
            prior_flags: rec.flags.clone(),
        });
        consumed_news.insert(new_id.clone());
        consumed_deletes.insert(rec.jmap_email_id.clone());
    }

    // A delete that's been paired into a cross-folder move is not a
    // delete from the server's perspective -- it is the source half of
    // a MoveRemote we are about to emit. Leaving it in `local_deletes`
    // would let handle_known_remote interpret a coincident server-side
    // update on the same id as a delete-vs-update conflict, and under
    // ServerWins it would re-download into the source folder, undoing
    // the user's move. Strip the paired ids so only genuine local
    // deletes can trigger the conflict path.
    for jid in &consumed_deletes {
        local_deletes.remove(jid);
    }

    let ctx = ReconcileCtx {
        remote_emails,
        mailboxes,
        strategy,
        known_by_maildir,
        known_by_jmap,
        known_by_message_id,
        local_index,
        local_flag_changes,
        local_deletes,
        destroyed_set,
        max_upload_size,
    };

    // Track local maildir_ids that have been claimed by an adoption emitted
    // from the remote side, so processing of LocalChange::NewMessage doesn't
    // also emit an upload for the same file.
    let mut adopted_maildir_ids: HashSet<MaildirId> = HashSet::new();

    // JMAP ids whose local DestroyRemote should be suppressed because the
    // server-side update won the local-delete-vs-remote-update conflict.
    let mut deletes_overruled_by_server: HashSet<JmapEmailId> = HashSet::new();

    process_remote_emails(
        &ctx,
        &mut adopted_maildir_ids,
        &mut deletes_overruled_by_server,
        &mut plan,
    );

    process_remote_destroys(remote_destroyed, ctx.known_by_jmap, &mut plan);

    emit_detected_moves(&detected_moves, &mut plan);

    process_local_changes(
        local_changes,
        &ctx,
        &adopted_maildir_ids,
        &deletes_overruled_by_server,
        &consumed_news,
        &consumed_deletes,
        &mut plan,
    );

    plan
}

/// One paired DeletedMessage(src) + NewMessage(dst) discovered during
/// the move pre-pass.
struct DetectedMove {
    jmap_email_id: JmapEmailId,
    to_mailbox_id: JmapMailboxId,
    old_maildir_id: MaildirId,
    new_maildir_id: MaildirId,
    from_folder: String,
    new_folder: String,
    new_flags: String,
    jmap_blob_id: Option<JmapBlobId>,
    jmap_thread_id: Option<JmapThreadId>,
    message_id: MessageId,
    /// Flags as recorded in message_map at the start of the cycle;
    /// compared against new_flags to decide whether the move should
    /// also push keyword changes.
    prior_flags: String,
}

fn emit_detected_moves(moves: &[DetectedMove], plan: &mut SyncPlan) {
    for m in moves {
        plan.actions.push(SyncAction::MoveRemote {
            id: RemoteId {
                jmap_email_id: m.jmap_email_id.clone(),
                message_id: m.message_id.clone(),
            },
            // jmapsync's DB binds each email to exactly one mailbox,
            // so the target set after the move is just the destination.
            // If the email also lives in unsynced JMAP mailboxes (e.g.
            // server-side label rules), this full-replacement strips
            // those memberships -- accepted for now; the alternative
            // is a per-cycle Email/get to read the current set first.
            target_mailbox_ids: vec![m.to_mailbox_id.clone()],
            from_folder: m.from_folder.clone(),
            to_folder: m.new_folder.clone(),
        });
        let keywords = flags_to_keywords(&m.new_flags);
        plan.actions.push(SyncAction::AdoptLocalMessage {
            id: BoundId {
                maildir_id: m.new_maildir_id.clone(),
                jmap_email_id: m.jmap_email_id.clone(),
                message_id: m.message_id.clone(),
            },
            maildir_folder: m.new_folder.clone(),
            jmap_blob_id: m.jmap_blob_id.clone(),
            jmap_thread_id: m.jmap_thread_id.clone(),
            mailbox_id: m.to_mailbox_id.clone(),
            keywords: keywords.clone(),
            old_maildir_id: Some(m.old_maildir_id.clone()),
        });
        if m.prior_flags != m.new_flags {
            plan.actions.push(SyncAction::UpdateRemoteKeywords {
                id: RemoteId {
                    jmap_email_id: m.jmap_email_id.clone(),
                    message_id: m.message_id.clone(),
                },
                keywords,
            });
        }
    }
}

fn process_remote_emails(
    ctx: &ReconcileCtx<'_>,
    adopted_maildir_ids: &mut HashSet<MaildirId>,
    deletes_overruled_by_server: &mut HashSet<JmapEmailId>,
    plan: &mut SyncPlan,
) {
    for email in ctx.remote_emails {
        if ctx.destroyed_set.contains(email.id.as_ref()) {
            // Will be handled by the destroyed pass.
            continue;
        }

        let mailbox_match = ctx
            .mailboxes
            .iter()
            .find(|(mid, _)| email.mailbox_ids.contains_key(mid));
        let Some((target_mailbox_id, target_folder)) = mailbox_match else {
            debug!(
                "Remote email {} not in any synced mailbox, skipping",
                email.id
            );
            continue;
        };

        let matched = RemoteMatch {
            email,
            target_mailbox_id,
            target_folder,
        };

        // Path 1: already bound by JMAP id -- flag/move updates only.
        // The wire-format Message-ID is irrelevant here: the existing
        // message_map row already anchors this email, so we proceed
        // even if Email/get omitted the header field. The strict gate
        // below only applies to unknown JMAP ids, where there's no DB
        // anchor to fall back on.
        if let Some(existing) = ctx.known_by_jmap.get(email.id.as_ref()) {
            handle_known_remote(ctx, &matched, existing, deletes_overruled_by_server, plan);
            continue;
        }

        let local_msg_id = email
            .message_id
            .as_ref()
            .and_then(|ids| ids.first())
            .cloned();

        // Unknown JMAP id and no Message-ID: refuse to ingest.
        // jmapsync's idempotency invariant requires Message-ID as the
        // adopt anchor across state-DB wipes; downloading without one
        // means the next state-DB wipe would re-download the same
        // bytes as a fresh local file (dup on disk) instead of
        // adopting the existing one. Skip the email; it stays on the
        // server, and the cycle still advances state. error! because
        // the user (or the server admin) must fix the source: sync
        // cannot synthesize a Message-ID retroactively.
        let Some(local_msg_id) = local_msg_id else {
            error!(
                "Skipping remote email {} (folder {}): no Message-ID in Email/get response. \
                 jmapsync requires Message-ID to anchor idempotency across state-DB wipes; \
                 the server returned an RFC-violating email and we won't ingest it.",
                email.id, target_folder
            );
            continue;
        };

        // Path 2: not bound by JMAP id, but Message-ID matches a local file
        // (either via DB carry-over from a half-completed prior run, or via
        // the dedupe-pass index after a state DB wipe). Adopt it.
        if try_adopt_remote(ctx, &matched, &local_msg_id, adopted_maildir_ids, plan) {
            continue;
        }

        // Path 3: nothing local -- download.
        plan.actions.push(SyncAction::DownloadMessage {
            id: RemoteId {
                jmap_email_id: email.id.clone(),
                message_id: local_msg_id,
            },
            jmap_blob_id: email.blob_id.clone(),
            jmap_thread_id: email.thread_id.clone(),
            mailbox_id: target_mailbox_id.clone(),
            maildir_folder: target_folder.clone(),
            keywords: email.keywords.clone(),
        });
    }
}

/// One iteration's worth of "this remote email maps to that local target":
/// the email itself plus the chosen mailbox/folder pair. Computed once in
/// `process_remote_emails` and threaded through both the JMAP-bound and
/// adopt paths.
#[derive(Clone, Copy)]
struct RemoteMatch<'a> {
    email: &'a EmailObject,
    target_mailbox_id: &'a JmapMailboxId,
    target_folder: &'a str,
}

fn handle_known_remote(
    ctx: &ReconcileCtx<'_>,
    matched: &RemoteMatch<'_>,
    existing: &MessageRecord,
    deletes_overruled_by_server: &mut HashSet<JmapEmailId>,
    plan: &mut SyncPlan,
) {
    let RemoteMatch {
        email,
        target_mailbox_id,
        target_folder,
    } = *matched;

    // Conflict: local deleted the file while the server updated
    // it. Resolve before any flag/move emission, since the local
    // copy is gone either way.
    if ctx.local_deletes.contains(&email.id) {
        match resolve_delete_conflict(&email.id, ctx.strategy) {
            DeleteWinner::Server => {
                deletes_overruled_by_server.insert(email.id.clone());
                // Re-download to restore the deleted local file.
                // The orphaned local_state row from the prior
                // maildir_id will be cleaned by the next scan
                // cycle's idempotent DeletedMessage path.
                plan.actions.push(SyncAction::DownloadMessage {
                    id: RemoteId {
                        jmap_email_id: email.id.clone(),
                        message_id: existing.message_id.clone(),
                    },
                    jmap_blob_id: email.blob_id.clone(),
                    jmap_thread_id: email.thread_id.clone(),
                    mailbox_id: target_mailbox_id.clone(),
                    maildir_folder: target_folder.to_string(),
                    keywords: email.keywords.clone(),
                });
            }
            DeleteWinner::Local => {
                // Skip remote-side actions; the local-loop will
                // emit DestroyRemote unimpeded.
            }
        }
        return;
    }

    let new_flags = keywords_to_flags(&email.keywords);
    let local_changed = ctx.local_flag_changes.contains_key(&email.id);
    let server_flag_change = existing.flags != new_flags;

    if server_flag_change && local_changed {
        let resolved = resolve_flag_conflict(
            &email.id,
            existing,
            email,
            ctx.local_flag_changes.get(&email.id).copied(),
            ctx.strategy,
        );
        match resolved {
            FlagWinner::Server => {
                emit_local_flag_update(plan, existing, email, target_mailbox_id);
            }
            FlagWinner::Local => {
                if let Some(LocalChange::FlagsChanged { new_flags, .. }) =
                    ctx.local_flag_changes.get(&email.id).copied()
                {
                    plan.actions.push(SyncAction::UpdateRemoteKeywords {
                        id: RemoteId {
                            jmap_email_id: email.id.clone(),
                            message_id: existing.message_id.clone(),
                        },
                        keywords: flags_to_keywords(new_flags),
                    });
                }
            }
        }
    } else if server_flag_change {
        emit_local_flag_update(plan, existing, email, target_mailbox_id);
    }

    // Mailbox-membership change: server claims the email lives in
    // a different folder than where our local copy is bound.
    // TODO: cross-detect when the local copy was *also* moved
    // (scan emits NewMessage in dest + DeletedMessage in src,
    // not a true Move). For now, blindly follow the server.
    if let Some(local_folder) = &existing.maildir_folder
        && local_folder != target_folder
        && let Some(local_maildir_id) = &existing.maildir_id
    {
        plan.actions.push(SyncAction::MoveLocal {
            id: BoundId {
                maildir_id: local_maildir_id.clone(),
                jmap_email_id: email.id.clone(),
                message_id: existing.message_id.clone(),
            },
            from_folder: local_folder.clone(),
            to_folder: target_folder.to_string(),
        });
    }
}

fn try_adopt_remote(
    ctx: &ReconcileCtx<'_>,
    matched: &RemoteMatch<'_>,
    mid: &MessageId,
    adopted_maildir_ids: &mut HashSet<MaildirId>,
    plan: &mut SyncPlan,
) -> bool {
    let RemoteMatch {
        email,
        target_mailbox_id,
        target_folder,
    } = *matched;

    let push_adopt =
        |plan: &mut SyncPlan, adopted: &mut HashSet<MaildirId>, maildir_id: MaildirId| {
            adopted.insert(maildir_id.clone());
            plan.actions.push(SyncAction::AdoptLocalMessage {
                id: BoundId {
                    maildir_id,
                    jmap_email_id: email.id.clone(),
                    message_id: mid.clone(),
                },
                maildir_folder: target_folder.to_string(),
                jmap_blob_id: Some(email.blob_id.clone()),
                jmap_thread_id: Some(email.thread_id.clone()),
                mailbox_id: target_mailbox_id.clone(),
                keywords: email.keywords.clone(),
                old_maildir_id: None,
            });
        };

    if let Some(recs) = ctx.known_by_message_id.get(mid)
        && let Some(maildir_id) = recs.iter().find_map(|r| {
            if r.maildir_folder.as_deref() == Some(target_folder) {
                r.maildir_id.clone()
            } else {
                None
            }
        })
    {
        push_adopt(plan, adopted_maildir_ids, maildir_id);
        return true;
    }

    if let Some(entries) = ctx.local_index.by_message_id.get(mid)
        && let Some(entry) = entries.iter().find(|e| e.folder == target_folder)
    {
        push_adopt(plan, adopted_maildir_ids, entry.maildir_id.clone());
        return true;
    }

    false
}

fn process_remote_destroys(
    remote_destroyed: &[JmapEmailId],
    known_by_jmap: &HashMap<JmapEmailId, Arc<MessageRecord>>,
    plan: &mut SyncPlan,
) {
    for jmap_id in remote_destroyed {
        if let Some(msg) = known_by_jmap.get(jmap_id)
            && let (Some(maildir_id), Some(folder)) = (&msg.maildir_id, &msg.maildir_folder)
        {
            plan.actions.push(SyncAction::DeleteLocal {
                id: BoundId {
                    maildir_id: maildir_id.clone(),
                    jmap_email_id: jmap_id.clone(),
                    message_id: msg.message_id.clone(),
                },
                maildir_folder: folder.clone(),
            });
        }
    }
}

fn process_local_changes(
    local_changes: &[LocalChange],
    ctx: &ReconcileCtx<'_>,
    adopted_maildir_ids: &HashSet<MaildirId>,
    deletes_overruled_by_server: &HashSet<JmapEmailId>,
    consumed_news: &HashSet<MaildirId>,
    consumed_deletes: &HashSet<JmapEmailId>,
    plan: &mut SyncPlan,
) {
    for change in local_changes {
        match change {
            LocalChange::NewMessage { maildir_id, .. } => {
                if consumed_news.contains(maildir_id) {
                    continue;
                }
                handle_local_new(ctx, change, adopted_maildir_ids, plan);
            }
            LocalChange::FlagsChanged {
                maildir_id,
                new_flags,
                ..
            } => handle_local_flags(ctx, maildir_id, new_flags, plan),
            LocalChange::DeletedMessage { maildir_id, .. } => {
                if let Some(rec) = ctx.known_by_maildir.get(maildir_id)
                    && consumed_deletes.contains(&rec.jmap_email_id)
                {
                    continue;
                }
                handle_local_delete(ctx, maildir_id, deletes_overruled_by_server, plan)
            }
        }
    }
}

/// Decide what to do with a `LocalChange::NewMessage`: adopt
/// against an existing server email, refuse as a duplicate, refuse
/// as oversized, or emit `UploadMessage`. Caller is `process_local_changes`,
/// which has already filtered out moved-paired entries via `consumed_news`.
fn handle_local_new(
    ctx: &ReconcileCtx<'_>,
    change: &LocalChange,
    adopted_maildir_ids: &HashSet<MaildirId>,
    plan: &mut SyncPlan,
) {
    let LocalChange::NewMessage {
        maildir_id,
        folder,
        flags,
        path,
        message_id,
        size_bytes,
    } = change
    else {
        unreachable!("handle_local_new called with non-NewMessage variant");
    };
    if adopted_maildir_ids.contains(maildir_id) {
        // Already covered by an AdoptLocalMessage emitted above.
        return;
    }

    let mailbox_id = ctx
        .mailboxes
        .iter()
        .find(|(_, f)| f == folder)
        .map(|(m, _)| m.clone());
    let Some(mailbox_id) = mailbox_id else {
        debug!(
            "Local new message {} in unsynced folder {}, skipping",
            maildir_id, folder
        );
        return;
    };

    // If we can match the local Message-ID against a known server
    // email (DB index), adopt instead of upload. This is the
    // alreadyExists guard.
    if let Some(recs) = ctx.known_by_message_id.get(message_id)
        && let Some(rec) = recs
            .iter()
            .find(|r| r.maildir_folder.as_deref() == Some(folder))
    {
        let keywords =
            serde_json::from_str::<HashMap<String, bool>>(&rec.jmap_keywords).unwrap_or_default();
        plan.actions.push(SyncAction::AdoptLocalMessage {
            id: BoundId {
                maildir_id: maildir_id.clone(),
                jmap_email_id: rec.jmap_email_id.clone(),
                message_id: message_id.clone(),
            },
            maildir_folder: folder.to_string(),
            jmap_blob_id: rec.jmap_blob_id.clone(),
            jmap_thread_id: rec.jmap_thread_id.clone(),
            mailbox_id,
            keywords,
            old_maildir_id: None,
        });
        return;
    }

    // Local Message-ID exists upstream but the existing DB record is
    // bound to a different folder, and there's no matching local
    // delete to pair this NewMessage against (handled in the move
    // pre-pass). This is a duplicate the user introduced manually --
    // either by copying a file across folders or by an external MUA
    // racing us. Uploading would be rejected with alreadyExists every
    // cycle and never converge, so skip the upload and warn loudly so
    // the user can decide which copy to keep.
    if let Some(recs) = ctx.known_by_message_id.get(message_id)
        && let Some(other) = recs.iter().find(|r| r.maildir_id.is_some())
    {
        warn!(
            "Local file {}/{} duplicates Message-ID {} already mapped to JMAP {} in folder {} (no paired delete to interpret as a move). Skipping upload to avoid alreadyExists; remove one copy to converge.",
            folder,
            maildir_id,
            message_id,
            other.jmap_email_id,
            other.maildir_folder.as_deref().unwrap_or("?"),
        );
        return;
    }

    // Size cap is enforced here, at plan time, rather than in
    // execute: catching the oversized file at this point keeps it
    // out of the upload concurrency budget entirely (a slot held by
    // a doomed upload is a slot another message could have used)
    // and lets dry-run show a plan that matches what execute would
    // actually attempt. A too-large file is user-actionable (the
    // user has to remove it from the maildir), so error! rather
    // than warn!.
    if *size_bytes > ctx.max_upload_size as u64 {
        error!(
            "Skipping upload of {} from {}: size {} bytes exceeds server/client cap of {} bytes. \
             Remove the file from the maildir.",
            maildir_id, folder, size_bytes, ctx.max_upload_size
        );
        return;
    }

    plan.actions.push(SyncAction::UploadMessage {
        id: LocalId {
            maildir_id: maildir_id.clone(),
            message_id: message_id.clone(),
        },
        maildir_folder: folder.to_string(),
        file_path: path.to_path_buf(),
        mailbox_id,
        flags: flags.to_string(),
    });
}

fn handle_local_flags(
    ctx: &ReconcileCtx<'_>,
    maildir_id: &MaildirId,
    new_flags: &str,
    plan: &mut SyncPlan,
) {
    let Some(msg) = ctx.known_by_maildir.get(maildir_id) else {
        return;
    };
    // Conflict cases handled inline above when emitting the
    // server-side action; here we emit only the no-conflict push.
    let server_also_changed = ctx.remote_emails.iter().any(|e| {
        e.id == msg.jmap_email_id && {
            let server_flags = keywords_to_flags(&e.keywords);
            server_flags != msg.flags
        }
    });
    if server_also_changed {
        return;
    }
    plan.actions.push(SyncAction::UpdateRemoteKeywords {
        id: RemoteId {
            jmap_email_id: msg.jmap_email_id.clone(),
            message_id: msg.message_id.clone(),
        },
        keywords: flags_to_keywords(new_flags),
    });
}

fn handle_local_delete(
    ctx: &ReconcileCtx<'_>,
    maildir_id: &MaildirId,
    deletes_overruled_by_server: &HashSet<JmapEmailId>,
    plan: &mut SyncPlan,
) {
    let Some(msg) = ctx.known_by_maildir.get(maildir_id) else {
        return;
    };
    if ctx.destroyed_set.contains(msg.jmap_email_id.as_ref()) {
        return;
    }
    if deletes_overruled_by_server.contains(&msg.jmap_email_id) {
        return;
    }
    plan.actions.push(SyncAction::DestroyRemote {
        id: RemoteId {
            jmap_email_id: msg.jmap_email_id.clone(),
            message_id: msg.message_id.clone(),
        },
    });
}

/// Outcome of a local-delete vs remote-update conflict resolution.
enum DeleteWinner {
    /// Server's update wins: re-download to restore the local file.
    Server,
    /// Local delete wins: proceed with DestroyRemote.
    Local,
}

fn resolve_delete_conflict(jmap_id: &JmapEmailId, strategy: ConflictStrategy) -> DeleteWinner {
    match strategy {
        ConflictStrategy::ServerWins => {
            warn!(
                "Delete-vs-update conflict on {}: server-wins, restoring local file",
                jmap_id
            );
            DeleteWinner::Server
        }
        ConflictStrategy::LocalWins => {
            warn!(
                "Delete-vs-update conflict on {}: local-wins, destroying remote",
                jmap_id
            );
            DeleteWinner::Local
        }
    }
}

/// Outcome of a flag conflict resolution.
enum FlagWinner {
    Server,
    Local,
}

fn resolve_flag_conflict(
    jmap_id: &JmapEmailId,
    _local_record: &MessageRecord,
    _server_email: &EmailObject,
    _local_change: Option<&LocalChange>,
    strategy: ConflictStrategy,
) -> FlagWinner {
    match strategy {
        ConflictStrategy::ServerWins => {
            warn!(
                "Flag conflict on {}: server-wins, dropping local change",
                jmap_id
            );
            FlagWinner::Server
        }
        ConflictStrategy::LocalWins => {
            warn!(
                "Flag conflict on {}: local-wins, dropping server change",
                jmap_id
            );
            FlagWinner::Local
        }
    }
}

fn emit_local_flag_update(
    plan: &mut SyncPlan,
    existing: &MessageRecord,
    email: &EmailObject,
    target_mailbox_id: &JmapMailboxId,
) {
    let (Some(maildir_id), Some(folder)) = (&existing.maildir_id, &existing.maildir_folder) else {
        return;
    };
    let new_flags = keywords_to_flags(&email.keywords);
    plan.actions.push(SyncAction::UpdateLocalFlags {
        id: BoundId {
            maildir_id: maildir_id.clone(),
            jmap_email_id: email.id.clone(),
            message_id: existing.message_id.clone(),
        },
        maildir_folder: folder.clone(),
        new_flags,
        keywords: email.keywords.clone(),
        jmap_blob_id: email.blob_id.clone(),
        jmap_thread_id: email.thread_id.clone(),
        mailbox_id: target_mailbox_id.clone(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maildir_ops::dedupe::{LocalEntry, LocalIndex};
    use std::path::PathBuf;

    fn mailboxes() -> Vec<(JmapMailboxId, String)> {
        vec![
            ("MB-INBOX".into(), "INBOX".into()),
            ("MB-ARCH".into(), "Archive".into()),
        ]
    }

    fn email(id: &str, mailbox_id: &str, flags: &str, message_id: Option<&str>) -> EmailObject {
        let mut mailbox_ids = HashMap::new();
        mailbox_ids.insert(mailbox_id.into(), true);
        EmailObject {
            id: id.into(),
            blob_id: format!("blob-{id}").into(),
            thread_id: format!("thr-{id}").into(),
            mailbox_ids,
            keywords: flags_to_keywords(flags),
            message_id: message_id.map(|m| vec![m.into()]),
            subject: None,
        }
    }

    fn record(
        jmap_id: &str,
        mailbox_id: &str,
        folder: &str,
        maildir_id: Option<&str>,
        flags: &str,
        message_id: &str,
    ) -> MessageRecord {
        let kw = flags_to_keywords(flags);
        MessageRecord {
            jmap_email_id: jmap_id.into(),
            jmap_blob_id: Some(format!("blob-{jmap_id}").into()),
            jmap_thread_id: Some(format!("thr-{jmap_id}").into()),
            mailbox_id: mailbox_id.into(),
            maildir_id: maildir_id.map(Into::into),
            maildir_folder: Some(folder.into()),
            message_id: message_id.into(),
            flags: flags.into(),
            jmap_keywords: serde_json::to_string(&kw).unwrap(),
        }
    }

    /// Build the message_map indices the way the engine does.
    fn indices(records: &[MessageRecord]) -> MessageRecordIndex {
        let mut idx = MessageRecordIndex::default();
        for r in records {
            let rec = Arc::new(r.clone());
            if let Some(ref m) = rec.maildir_id {
                idx.by_maildir.insert(m.clone(), Arc::clone(&rec));
            }
            idx.by_message_id
                .entry(rec.message_id.clone())
                .or_default()
                .push(Arc::clone(&rec));
            idx.by_jmap.insert(rec.jmap_email_id.clone(), rec);
        }
        idx
    }

    fn empty_index() -> LocalIndex {
        LocalIndex::default()
    }

    fn run(
        remote_emails: &[EmailObject],
        remote_destroyed: &[JmapEmailId],
        local_changes: &[LocalChange],
        records: &[MessageRecord],
        local_index: &LocalIndex,
        strategy: ConflictStrategy,
    ) -> SyncPlan {
        let known = indices(records);
        let mailboxes = mailboxes();
        reconcile(ReconcileInput {
            remote_emails,
            remote_destroyed,
            local_changes,
            known: &known,
            local_index,
            mailboxes: &mailboxes,
            strategy,
            new_email_state: None,
            // Tests pass usize::MAX so the size cap never bites
            // unless a test explicitly opts in to it.
            max_upload_size: usize::MAX,
        })
    }

    /// Server has an email we've never seen and no local file matches its
    /// Message-ID — pull path emits a download.
    #[test]
    fn unknown_remote_email_emits_download() {
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[],
            &[],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert_eq!(plan.download_count(), 1);
        assert!(matches!(
            plan.actions[0],
            SyncAction::DownloadMessage { id: RemoteId { ref jmap_email_id, .. }, .. } if jmap_email_id.as_ref() == "E1"
        ));
    }

    /// Server returned an email with no Message-ID at all (the
    /// `messageId` field absent or empty). With no JMAP-id binding in
    /// our DB and no Message-ID to anchor adoption against, ingesting
    /// would break the disposable-state-DB invariant: a wipe + resync
    /// would re-download the same bytes as a fresh local file. Refuse
    /// at the reconcile boundary -- emit zero actions and let the
    /// cycle advance state. The email stays on the server untouched.
    #[test]
    fn unknown_remote_email_without_message_id_emits_nothing() {
        let plan = run(
            &[email("E1", "MB-INBOX", "S", None)],
            &[],
            &[],
            &[],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(
            plan.is_empty(),
            "expected no actions for an unknown remote without Message-ID, got {:?}",
            plan.actions
        );
    }

    /// Server returned an Email/get response with no Message-ID, but
    /// we already have a binding for this JMAP id (existing
    /// message_map row from a prior cycle that anchored it). The
    /// binding itself is the idempotency anchor; we don't need the
    /// wire-format Message-ID to safely apply flag updates. Refusing
    /// known emails on a missing wire field would mean any server
    /// that strips Message-ID from updates breaks flag sync forever.
    #[test]
    fn known_remote_email_without_message_id_still_updates_flags() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", None)],
            &[],
            &[],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(
            plan.actions.iter().any(|a| matches!(
                a,
                SyncAction::UpdateLocalFlags { new_flags, .. } if new_flags == "S"
            )),
            "expected UpdateLocalFlags despite the missing wire Message-ID, got {:?}",
            plan.actions
        );
    }

    /// Remote email lives in a mailbox not in the synced set: skip silently
    /// (no action, not even a download).
    #[test]
    fn remote_email_in_unsynced_mailbox_skipped() {
        let plan = run(
            &[email("E1", "MB-OTHER", "S", Some("<a@x>"))],
            &[],
            &[],
            &[],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(plan.is_empty());
    }

    /// Adoption via message_map: same Message-ID, same folder, has a local
    /// maildir_id — emit AdoptLocalMessage instead of DownloadMessage. This
    /// exercises the carry-over-from-stale-DB path.
    #[test]
    fn adopt_via_known_message_id_in_db() {
        let rec = record("STALE-ID", "MB-INBOX", "INBOX", Some("MID-1"), "S", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert_eq!(plan.adopt_count(), 1);
        assert_eq!(plan.download_count(), 0);
        let SyncAction::AdoptLocalMessage {
            id:
                BoundId {
                    jmap_email_id,
                    maildir_id,
                    ..
                },
            ..
        } = &plan.actions[0]
        else {
            panic!("expected adopt");
        };
        assert_eq!(jmap_email_id.as_ref(), "E1");
        assert_eq!(maildir_id.as_ref(), "MID-1");
    }

    /// Adoption via local_index: state DB is empty, but the maildir holds a
    /// file with the matching Message-ID in the target folder.
    #[test]
    fn adopt_via_local_index_when_db_is_empty() {
        let mut idx = empty_index();
        idx.by_message_id.insert(
            "<a@x>".into(),
            vec![LocalEntry {
                folder: "INBOX".into(),
                maildir_id: "FILE-1".into(),
            }],
        );
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[],
            &[],
            &idx,
            ConflictStrategy::ServerWins,
        );
        assert_eq!(plan.adopt_count(), 1);
        let SyncAction::AdoptLocalMessage {
            id:
                BoundId {
                    maildir_id,
                    jmap_email_id,
                    ..
                },
            ..
        } = &plan.actions[0]
        else {
            panic!("expected adopt");
        };
        assert_eq!(maildir_id.as_ref(), "FILE-1");
        assert_eq!(jmap_email_id.as_ref(), "E1");
    }

    /// Known JMAP id, server keywords differ from message_map: emit a
    /// pull-side flag update, no upload.
    #[test]
    fn server_keyword_change_emits_update_local_flags() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(matches!(
            plan.actions[0],
            SyncAction::UpdateLocalFlags { ref new_flags, .. } if new_flags == "S"
        ));
    }

    /// Server reports the email has moved between mailboxes: emit MoveLocal.
    #[test]
    fn server_mailbox_change_emits_move_local() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-1"), "S", "<a@x>");
        let plan = run(
            &[email("E1", "MB-ARCH", "S", Some("<a@x>"))],
            &[],
            &[],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            SyncAction::MoveLocal {
                from_folder, to_folder, ..
            } if from_folder == "INBOX" && to_folder == "Archive"
        )));
    }

    /// Local-delete vs server-update with ServerWins: re-download to
    /// restore the file; do not emit DestroyRemote.
    #[test]
    fn delete_vs_update_server_wins_redownloads() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[LocalChange::DeletedMessage {
                maildir_id: "M-1".into(),
                folder: "INBOX".into(),
            }],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert_eq!(plan.download_count(), 1);
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DestroyRemote { .. }))
        );
    }

    /// Local-delete vs server-update with LocalWins: emit DestroyRemote;
    /// suppress the redownload.
    #[test]
    fn delete_vs_update_local_wins_destroys_remote() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[LocalChange::DeletedMessage {
                maildir_id: "M-1".into(),
                folder: "INBOX".into(),
            }],
            &[rec],
            &empty_index(),
            ConflictStrategy::LocalWins,
        );
        assert_eq!(plan.download_count(), 0);
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            SyncAction::DestroyRemote { id: RemoteId { jmap_email_id, .. } } if jmap_email_id.as_ref() == "E1"
        )));
    }

    /// Flag conflict (both sides changed keywords) with ServerWins: pull
    /// down the server flags, drop the local push.
    #[test]
    fn flag_conflict_server_wins() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[LocalChange::FlagsChanged {
                maildir_id: "M-1".into(),
                folder: "INBOX".into(),
                old_flags: "".into(),
                new_flags: "F".into(),
            }],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(matches!(
            plan.actions[0],
            SyncAction::UpdateLocalFlags { ref new_flags, .. } if new_flags == "S"
        ));
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::UpdateRemoteKeywords { .. }))
        );
    }

    /// Flag conflict with LocalWins: push local keywords, suppress the
    /// server-side flag update.
    #[test]
    fn flag_conflict_local_wins() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[LocalChange::FlagsChanged {
                maildir_id: "M-1".into(),
                folder: "INBOX".into(),
                old_flags: "".into(),
                new_flags: "F".into(),
            }],
            &[rec],
            &empty_index(),
            ConflictStrategy::LocalWins,
        );
        assert!(
            plan.actions
                .iter()
                .any(|a| matches!(a, SyncAction::UpdateRemoteKeywords { .. }))
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::UpdateLocalFlags { .. }))
        );
    }

    /// A NewMessage whose `size_bytes` exceeds `max_upload_size`
    /// must NOT produce an UploadMessage in the plan. The check
    /// belongs at plan time so the file never enters the upload
    /// concurrency budget; dry-run also reflects what execute will
    /// actually attempt.
    #[test]
    fn local_new_oversized_skips_upload() {
        let known = indices(&[]);
        let mailboxes = mailboxes();
        let local_index = empty_index();
        let local_changes = [LocalChange::NewMessage {
            maildir_id: "M-BIG".into(),
            folder: "INBOX".into(),
            flags: "".into(),
            path: PathBuf::from("/tmp/m-big"),
            message_id: "<big@x>".into(),
            size_bytes: 2_000,
        }];
        let plan = reconcile(ReconcileInput {
            remote_emails: &[],
            remote_destroyed: &[],
            local_changes: &local_changes,
            known: &known,
            local_index: &local_index,
            mailboxes: &mailboxes,
            strategy: ConflictStrategy::ServerWins,
            new_email_state: None,
            max_upload_size: 1_000,
        });
        assert_eq!(plan.upload_count(), 0);
        assert!(plan.actions.is_empty());
    }

    /// Local NewMessage with no Message-ID match anywhere: upload.
    #[test]
    fn local_new_with_no_match_uploads() {
        let plan = run(
            &[],
            &[],
            &[LocalChange::NewMessage {
                maildir_id: "M-NEW".into(),
                folder: "INBOX".into(),
                flags: "S".into(),
                path: PathBuf::from("/tmp/m-new"),
                message_id: "<new@x>".into(),
                size_bytes: 0,
            }],
            &[],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert_eq!(plan.upload_count(), 1);
    }

    /// Local NewMessage whose Message-ID is already mapped to a different
    /// folder, with no paired delete: skip upload (alreadyExists guard).
    #[test]
    fn local_new_dup_message_id_in_other_folder_skips_upload() {
        let rec = record("E1", "MB-ARCH", "Archive", Some("M-EXISTING"), "", "<a@x>");
        let plan = run(
            &[],
            &[],
            &[LocalChange::NewMessage {
                maildir_id: "M-DUP".into(),
                folder: "INBOX".into(),
                flags: "".into(),
                path: PathBuf::from("/tmp/m-dup"),
                message_id: "<a@x>".into(),
                size_bytes: 0,
            }],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(plan.is_empty());
    }

    /// Local DeletedMessage for a known JMAP id: emit DestroyRemote.
    #[test]
    fn local_delete_emits_destroy_remote() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[],
            &[],
            &[LocalChange::DeletedMessage {
                maildir_id: "M-1".into(),
                folder: "INBOX".into(),
            }],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(matches!(
            plan.actions[0],
            SyncAction::DestroyRemote { id: RemoteId { ref jmap_email_id, .. } } if jmap_email_id.as_ref() == "E1"
        ));
    }

    /// Local DeletedMessage for an id the server *also* destroyed: emit
    /// only DeleteLocal (from the destroy pass), not DestroyRemote.
    #[test]
    fn local_delete_paired_with_remote_destroy_collapses() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[],
            &["E1".into()],
            &[LocalChange::DeletedMessage {
                maildir_id: "M-1".into(),
                folder: "INBOX".into(),
            }],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DestroyRemote { .. }))
        );
        assert!(
            plan.actions
                .iter()
                .any(|a| matches!(a, SyncAction::DeleteLocal { .. }))
        );
    }

    /// Move pre-pass: paired DeletedMessage(src) + NewMessage(dst) sharing
    /// a Message-ID becomes one MoveRemote + Adopt, not Destroy + Upload.
    #[test]
    fn cross_folder_local_move_emits_move_remote_and_adopt() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-OLD"), "", "<a@x>");
        let plan = run(
            &[],
            &[],
            &[
                LocalChange::DeletedMessage {
                    maildir_id: "M-OLD".into(),
                    folder: "INBOX".into(),
                },
                LocalChange::NewMessage {
                    maildir_id: "M-NEW".into(),
                    folder: "Archive".into(),
                    flags: "".into(),
                    path: PathBuf::from("/tmp/m-new"),
                    message_id: "<a@x>".into(),
                    size_bytes: 0,
                },
            ],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            SyncAction::MoveRemote { id: RemoteId { jmap_email_id, .. }, target_mailbox_ids, .. }
                if jmap_email_id.as_ref() == "E1" && target_mailbox_ids == &vec![JmapMailboxId::from("MB-ARCH")]
        )));
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            SyncAction::AdoptLocalMessage {
                id: BoundId { maildir_id, .. }, old_maildir_id: Some(old), ..
            } if maildir_id.as_ref() == "M-NEW" && old.as_ref() == "M-OLD"
        )));
        // Neither side of the lossy Destroy + Upload shape may appear.
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DestroyRemote { .. }))
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::UploadMessage { .. }))
        );
    }

    /// Cross-folder move where the destination file also gained a flag:
    /// expect MoveRemote + Adopt + UpdateRemoteKeywords.
    #[test]
    fn cross_folder_move_with_flag_change_pushes_keywords() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-OLD"), "", "<a@x>");
        let plan = run(
            &[],
            &[],
            &[
                LocalChange::DeletedMessage {
                    maildir_id: "M-OLD".into(),
                    folder: "INBOX".into(),
                },
                LocalChange::NewMessage {
                    maildir_id: "M-NEW".into(),
                    folder: "Archive".into(),
                    flags: "S".into(),
                    path: PathBuf::from("/tmp/m-new"),
                    message_id: "<a@x>".into(),
                    size_bytes: 0,
                },
            ],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(
            plan.actions
                .iter()
                .any(|a| matches!(a, SyncAction::UpdateRemoteKeywords { .. }))
        );
    }

    /// Maildir-id-preserving cross-folder move: the MUA renamed the
    /// file across folders without changing the unique part of the
    /// filename (the maildir spec recommends this). scan_folder now
    /// emits paired DeletedMessage(src) + NewMessage(dst) with the
    /// SAME maildir_id, and the move pre-pass must still pair them by
    /// Message-ID and produce the lossless MoveRemote + Adopt shape.
    /// The Adopt's old_maildir_id and new maildir_id are identical,
    /// which is the signal to execute that the DB row only needs its
    /// folder updated.
    #[test]
    fn cross_folder_move_with_preserved_maildir_id_pairs_correctly() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M1"), "FS", "<a@x>");
        let plan = run(
            &[],
            &[],
            &[
                LocalChange::DeletedMessage {
                    maildir_id: "M1".into(),
                    folder: "INBOX".into(),
                },
                LocalChange::NewMessage {
                    // Same id as the deleted side -- id-preserving move.
                    maildir_id: "M1".into(),
                    folder: "Archive".into(),
                    flags: "FS".into(),
                    path: PathBuf::from("/tmp/m1"),
                    message_id: "<a@x>".into(),
                    size_bytes: 0,
                },
            ],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(
            plan.actions.iter().any(|a| matches!(
                a,
                SyncAction::MoveRemote { id: RemoteId { jmap_email_id, .. }, target_mailbox_ids, .. }
                    if jmap_email_id.as_ref() == "E1" && target_mailbox_ids == &vec![JmapMailboxId::from("MB-ARCH")]
            )),
            "expected MoveRemote, got {:?}",
            plan.actions
        );
        assert!(
            plan.actions.iter().any(|a| matches!(
                a,
                SyncAction::AdoptLocalMessage {
                    id: BoundId { maildir_id, .. },
                    maildir_folder,
                    old_maildir_id: Some(old),
                    ..
                } if maildir_id.as_ref() == "M1" && old.as_ref() == "M1" && maildir_folder == "Archive"
            )),
            "expected AdoptLocalMessage with old==new maildir_id, got {:?}",
            plan.actions
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DestroyRemote { .. })),
            "id-preserving move must not degrade into DestroyRemote"
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::UploadMessage { .. })),
            "id-preserving move must not degrade into UploadMessage"
        );
    }

    /// A cross-folder local move while the server *also* returns the
    /// email in remote_emails (unrelated update from another client, or
    /// an initial pull where every email is enumerated). The move
    /// pre-pass pairs the delete and the new -- so the delete-vs-update
    /// conflict path in handle_known_remote must not also fire on the
    /// same id. Otherwise ServerWins re-downloads into the source
    /// folder and effectively undoes the user's move.
    #[test]
    fn cross_folder_move_with_concurrent_remote_update_does_not_redownload() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-OLD"), "", "<a@x>");
        let plan = run(
            // Server's view still has the email in INBOX (the local
            // move hasn't been pushed yet).
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[
                LocalChange::DeletedMessage {
                    maildir_id: "M-OLD".into(),
                    folder: "INBOX".into(),
                },
                LocalChange::NewMessage {
                    maildir_id: "M-NEW".into(),
                    folder: "Archive".into(),
                    flags: "".into(),
                    path: PathBuf::from("/tmp/m-new"),
                    message_id: "<a@x>".into(),
                    size_bytes: 0,
                },
            ],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        // The move detection still has to produce its MoveRemote+Adopt.
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            SyncAction::MoveRemote { id: RemoteId { jmap_email_id, .. }, target_mailbox_ids, .. }
                if jmap_email_id.as_ref() == "E1" && target_mailbox_ids == &vec![JmapMailboxId::from("MB-ARCH")]
        )));
        // And critically: no spurious re-download into the source.
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DownloadMessage { .. })),
            "delete-vs-update conflict must not fire for a delete already paired into a move"
        );
    }

    /// Server destroy of a known id: emit DeleteLocal carrying the
    /// resolved maildir_id and folder.
    #[test]
    fn remote_destroy_emits_delete_local() {
        let rec = record("E1", "MB-INBOX", "INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[],
            &["E1".into()],
            &[],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(matches!(
            plan.actions[0],
            SyncAction::DeleteLocal { id: BoundId { ref maildir_id, .. }, .. } if maildir_id.as_ref() == "M-1"
        ));
    }
}
