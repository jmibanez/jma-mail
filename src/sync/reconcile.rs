use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use tracing::{debug, error, warn};

use crate::config::{AllowDestructiveFolderSync, ConflictStrategy, FolderLayout, SyncConfig};
use crate::ids::{JmapBlobId, JmapEmailId, JmapMailboxId, JmapThreadId, MaildirId, MessageId};
use crate::jmap::types::{EmailObject, MailboxFolderBinding, MaybeReference};
use crate::maildir_ops::flags::{flags_to_keyword_patch, flags_to_keywords, keywords_to_flags};
use crate::maildir_ops::layout::decompose_folder_string;
use crate::maildir_ops::scan::LocalChange;
use crate::state::queries::MessageRecord;
use crate::sync::bindings::{LocalOrphanRecord, MailboxBindings};
use crate::sync::dedupe::LocalIndex;
use crate::sync::plan::{BoundId, LocalId, RemoteId, SyncAction, SyncPlan};

/// Tunables that govern reconcile's decision-making, bundled so the
/// caller threads one parameter instead of two parallel slots that
/// every helper would otherwise have to forward in lockstep. Both
/// fields come straight off `SyncConfig`; `from_sync_config` is the
/// canonical lift.
#[derive(Debug, Clone, Copy)]
pub struct SyncPolicy {
    pub conflict_strategy: ConflictStrategy,
    pub allow_destructive_folder_sync: AllowDestructiveFolderSync,
}

impl SyncPolicy {
    /// Lift the policy slots out of a `SyncConfig` so reconcile's
    /// caller doesn't have to thread two parallel parameters.
    pub fn from_sync_config(sync: &SyncConfig) -> Self {
        Self {
            conflict_strategy: sync.conflict_strategy,
            allow_destructive_folder_sync: sync.allow_destructive_folder_sync,
        }
    }

    /// True iff the policy permits destroying a local
    /// maildir folder in response to a server-side mailbox
    /// deletion (i.e. `allow_destructive_folder_sync` is
    /// `delete-local` or `both`). Reconcile gates its pull-
    /// side destructive arm on this; the resurrect arm gates
    /// on the negation.
    pub fn allows_delete_of_local_folder(&self) -> bool {
        self.allow_destructive_folder_sync.allows_delete_local()
    }

    /// True iff the policy permits issuing JMAP
    /// `Mailbox/set { destroy }` in response to a local
    /// maildir deletion (i.e. `allow_destructive_folder_sync`
    /// is `delete-remote` or `both`). Reconcile gates its
    /// push-side destructive arm on this.
    pub fn allows_delete_of_remote_folder(&self) -> bool {
        self.allow_destructive_folder_sync.allows_delete_remote()
    }
}

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
    mailboxes: &'a MailboxBindings,
    policy: SyncPolicy,
    known_by_maildir: &'a HashMap<MaildirId, Arc<MessageRecord>>,
    known_by_jmap: &'a HashMap<JmapEmailId, Arc<MessageRecord>>,
    known_by_message_id: &'a HashMap<MessageId, Vec<Arc<MessageRecord>>>,
    local_index: &'a LocalIndex,
    /// Per-file on-disk filename flag suffix from this cycle's scan
    /// (`scan::ScanResult.local_flags`). Reconcile reads it at
    /// adoption emit sites so `commit_adopt` can seed
    /// `local_state.flags` from filesystem truth rather than from
    /// the server's keyword-derived projection. For path-driven
    /// scans this map is partial (only event-set maildir_ids); for
    /// full scans it covers every cur/+new/ file. Adoption emit
    /// sites that miss this map fall back to the matched DB
    /// record's `flags` column.
    local_flags: &'a HashMap<MaildirId, String>,
    local_flag_changes: HashMap<JmapEmailId, &'a LocalChange>,
    local_deletes: HashSet<JmapEmailId>,
    destroyed_set: HashSet<&'a str>,
    max_upload_size: usize,
    /// Mirror of `ReconcileInput::used_initial_path`. See that field
    /// for the reasoning.
    used_initial_path: bool,
    /// Mirror of `ReconcileInput::folder_layout`. Drives layout-aware
    /// decomposition in the `LocalFolderCreated` -> `CreateRemoteMailbox`
    /// emit path.
    folder_layout: FolderLayout,
    /// Mirror of `ReconcileInput::hierarchy_separator`. Paired with
    /// `folder_layout` for the decomposition.
    hierarchy_separator: char,
    /// Mirror of `ReconcileInput::maildir_root`. Used to convert a
    /// `LocalChange::LocalFolderCreated`'s absolute path back to the
    /// relative folder string the bindings map uses.
    maildir_root: &'a Path,
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
/// `policy`: bundled tiebreaker (`conflict_strategy`) plus the
/// destructive-folder-sync gate; see `SyncPolicy`.
/// `new_email_state`: cursor to stamp into the resulting plan; `None`
/// for tests that don't care about state advancement.
pub struct ReconcileInput<'a> {
    pub remote_emails: &'a [EmailObject],
    pub remote_destroyed: &'a [JmapEmailId],
    pub local_changes: &'a [LocalChange],
    pub known: &'a MessageRecordIndex,
    pub local_index: &'a LocalIndex,
    /// Per-file MaildirId -> filename-flag-suffix from this cycle's
    /// `scan::ScanResult.local_flags`. See `ReconcileCtx.local_flags`
    /// for the read-side contract.
    pub local_flags: &'a HashMap<MaildirId, String>,
    pub mailboxes: &'a MailboxBindings,
    pub policy: SyncPolicy,
    pub new_email_state: Option<String>,
    /// Effective `maxSizeUpload` cap for this cycle. Resolved by the
    /// caller (engine) via `limits::max_size_upload(client)` so
    /// reconcile stays I/O-free. NewMessage entries whose
    /// `size_bytes` exceeds this don't get an `UploadMessage` in
    /// the plan.
    pub max_upload_size: usize,
    /// True iff this cycle's `remote_emails` came from the full
    /// initial-pull path (`initial_remote_state`) rather than from
    /// `Email/changes`. On the initial path, `remote_emails` is the
    /// full survivor set across synced mailboxes; on the
    /// incremental path it's just this cycle's created+updated ids.
    /// Reconcile uses this to gate the Branch A case-iii carry-over
    /// rebind: only the initial path can prove that an old
    /// jmap_email_id absent from `remote_emails` is genuinely gone
    /// from the server. In incremental mode the same absence could
    /// mean "alive but unchanged this cycle," and silently rebinding
    /// would lose track of a live duplicate locally.
    pub used_initial_path: bool,
    /// Configured folder layout. Threaded through so that the
    /// `LocalFolderCreated` -> `CreateRemoteMailbox` emit path can
    /// decompose the on-disk folder string into (leaf_name,
    /// parent_folder) under the right convention.
    pub folder_layout: FolderLayout,
    /// Configured hierarchy separator. Same purpose as
    /// `folder_layout`: drives the per-layout decomposition. Inert
    /// under `Fs` (which always splits on `/`).
    pub hierarchy_separator: char,
    /// Maildir root on disk. Used to convert a
    /// `LocalChange::LocalFolderCreated`'s absolute `path` back to
    /// the relative folder string consumers can look up against
    /// `MailboxBindings::by_folder`.
    pub maildir_root: &'a Path,
}

/// Reconcile remote changes and local changes into a sync plan.
pub fn reconcile(input: ReconcileInput<'_>) -> SyncPlan {
    let ReconcileInput {
        remote_emails,
        remote_destroyed,
        local_changes,
        known,
        local_index,
        local_flags,
        mailboxes,
        policy,
        new_email_state,
        max_upload_size,
        used_initial_path,
        folder_layout,
        hierarchy_separator,
        maildir_root,
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
            binding: src_binding,
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
            binding: dst_binding,
            flags: new_flags,
            ..
        }) = news_by_message_id.get(mid).copied()
        else {
            continue;
        };
        // Whole-binding compare rather than just `.jmap_mailbox_id`:
        // future folder-lifecycle work can produce two bindings that
        // share an id but disagree on `maildir_folder` (e.g. a rename
        // that wasn't applied to local_state yet), and we want the
        // move-pair path to fire in that case too.
        if dst_binding == src_binding {
            continue;
        }

        detected_moves.push(DetectedMove {
            jmap_email_id: rec.jmap_email_id.clone(),
            old_maildir_id: old_id.clone(),
            new_maildir_id: new_id.clone(),
            from_folder: src_binding.maildir_folder.clone(),
            to_binding: Arc::clone(dst_binding),
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
        policy,
        known_by_maildir,
        known_by_jmap,
        known_by_message_id,
        local_index,
        local_flags,
        local_flag_changes,
        local_deletes,
        destroyed_set,
        max_upload_size,
        used_initial_path,
        folder_layout,
        hierarchy_separator,
        maildir_root,
    };

    // Track local maildir_ids that have been claimed by an adoption emitted
    // from the remote side, so processing of LocalChange::NewMessage doesn't
    // also emit an upload for the same file.
    let mut adopted_maildir_ids: HashSet<MaildirId> = HashSet::new();

    // JMAP ids whose local DestroyRemote should be suppressed because the
    // server-side update won the local-delete-vs-remote-update conflict.
    let mut deletes_overruled_by_server: HashSet<JmapEmailId> = HashSet::new();

    // JMAP ids consumed by a destroy+create-with-shared-Message-ID
    // rebind in `try_adopt_remote`'s Branch A. `process_remote_
    // destroys` skips these so we don't emit a `DeleteLocal` against
    // a maildir_id we just rebound to the new JMAP id (which would
    // also trip the unique-on-maildir-id index under execute's
    // adopt-before-delete phase order).
    let mut consumed_remote_destroys: HashSet<JmapEmailId> = HashSet::new();

    process_remote_emails(
        &ctx,
        &mut adopted_maildir_ids,
        &mut deletes_overruled_by_server,
        &mut consumed_remote_destroys,
        &mut plan,
    );

    // Folder-level creation runs first in the emit order so the
    // plan reads top-down: maildirs come into being before any
    // action that writes into them. The executor's bucket order
    // already runs CreateLocalMailbox before everything else, so
    // wire ordering doesn't change semantics -- only the dry-run
    // display order.
    emit_create_local_mailboxes(ctx.mailboxes, &mut plan);
    emit_rename_mailboxes(ctx.mailboxes, &mut plan);
    emit_local_orphan_resurrects(&ctx, &mut plan);

    process_remote_destroys(
        remote_destroyed,
        ctx.known_by_jmap,
        ctx.mailboxes,
        &consumed_remote_destroys,
        &mut plan,
    );

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

    // Local-orphan destructive arm: gates on
    // `allows_delete_of_local_folder`. Runs LAST so it sees
    // every action other emitters produced and can decide per
    // orphan how to dispatch (no-Blocker, ServerWins+Blocker,
    // LocalWins+Blocker).
    finalize_local_orphan_handling(&ctx, &mut plan);

    // Remote-orphan destructive arm: gates on
    // `allows_delete_of_remote_folder`. Independent of the pull-
    // side finalizer (different orphan slot). ServerWins+Blocker
    // orphans were already pre-handled engine-side and removed
    // from `remote_orphans()` before reconcile ran; here we only
    // see no-Blocker and LocalWins+Blocker cases.
    finalize_remote_orphan_handling(&ctx, &mut plan);

    plan
}

/// Role of a `SyncAction` with respect to one local orphan,
/// per the destructive-arm matrix:
///   * `Blocker`: a cycle-local user-initiated action that
///     targets the orphan (`UploadMessage` into the orphan
///     folder, `MoveRemote` whose destination is the orphan,
///     `UpdateRemoteKeywords` on an email bound to the
///     orphan, `AdoptLocalMessage` pair targeting the
///     orphan). Triggers conflict-strategy dispatch.
///   * `Moot`: an action whose effect is subsumed by the
///     orphan's own destroy (server already destroyed the
///     orphan's emails; per-message destroys / moves out /
///     local deletes against orphan-bound emails are
///     redundant). Dropped from the plan regardless of
///     strategy.
///   * `Neutral`: doesn't reference the orphan; left alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrphanActionRole {
    Neutral,
    Moot,
    Blocker,
}

/// Does this binding's id match the orphan? `Reference` is
/// the symbolic creation-id form, emitted only by the
/// resurrect path's `UploadMessage`s, which run under
/// non-destructive policy (where this classifier doesn't
/// fire). Every binding the classifier sees comes through
/// scan_set or `mailboxes.by_id` and carries
/// `MaybeReference::Value`, so the `Reference` arm is
/// defensive and returns `false`.
fn binding_targets_orphan(binding: &MailboxFolderBinding, dead_id: &JmapMailboxId) -> bool {
    match &binding.jmap_mailbox_id {
        MaybeReference::Value(id) => id == dead_id,
        MaybeReference::Reference(_) => false,
    }
}

/// Does this `RemoteId`'s `message_map` row point at the
/// orphan's now-dead `jmap_mailbox_id`?
fn email_in_orphan(
    id: &RemoteId,
    dead_id: &JmapMailboxId,
    known_by_jmap: &HashMap<JmapEmailId, Arc<MessageRecord>>,
) -> bool {
    known_by_jmap
        .get(&id.jmap_email_id)
        .map(|r| &r.jmap_mailbox_id == dead_id)
        .unwrap_or(false)
}

fn classify_action_against_orphan(
    action: &SyncAction,
    dead_id: &JmapMailboxId,
    folder: &str,
    known_by_jmap: &HashMap<JmapEmailId, Arc<MessageRecord>>,
) -> OrphanActionRole {
    match action {
        SyncAction::UploadMessage { binding, .. } => {
            if binding_targets_orphan(binding, dead_id) {
                OrphanActionRole::Blocker
            } else {
                OrphanActionRole::Neutral
            }
        }
        SyncAction::MoveRemote {
            target_mailbox_ids,
            from_folder,
            to_folder,
            ..
        } => {
            if target_mailbox_ids.iter().any(|id| id == dead_id) || to_folder == folder {
                // Move INTO orphan: Blocker
                OrphanActionRole::Blocker
            } else if from_folder == folder {
                // Move OUT of orphan: Moot (orphan destroy
                // already obviates the per-email move)
                OrphanActionRole::Moot
            } else {
                OrphanActionRole::Neutral
            }
        }
        SyncAction::UpdateRemoteKeywords { id, .. } => {
            if email_in_orphan(id, dead_id, known_by_jmap) {
                OrphanActionRole::Blocker
            } else {
                OrphanActionRole::Neutral
            }
        }
        SyncAction::DestroyRemote { id } => {
            if email_in_orphan(id, dead_id, known_by_jmap) {
                OrphanActionRole::Moot
            } else {
                OrphanActionRole::Neutral
            }
        }
        SyncAction::AdoptLocalMessage {
            binding,
            old_maildir_id,
            ..
        } => {
            if old_maildir_id.is_some() && binding_targets_orphan(binding, dead_id) {
                OrphanActionRole::Blocker
            } else {
                OrphanActionRole::Neutral
            }
        }
        // `MoveLocal` and `DeleteLocal` producers gate on
        // `mailboxes.by_id(...)` being `Some`, which excludes
        // orphans by construction -- the orphan's id is the
        // dead one, not in the live set. So neither variant
        // can target an orphan and both fall through to
        // Neutral. Pull-side mailbox actions are likewise
        // never orphan-targeted.
        _ => OrphanActionRole::Neutral,
    }
}

/// Destructive-arm finalizer. Gated on
/// `allows_delete_of_local_folder()`. For each local orphan,
/// classifies plan actions against the orphan and dispatches
/// per the matrix:
///
///   * **No Blocker**: drop Moots; emit `DeleteLocalFolder`.
///     The user opted in to destructive folder sync; the
///     orphan's content (including any pre-existing files)
///     vanishes with the folder. The destroy is unconditional
///     here regardless of conflict strategy -- there is no
///     conflict to resolve, so `LocalWins` doesn't apply.
///
///   * **Blocker + `ServerWins`**: emit counter-actions for
///     each `MoveRemote` Blocker (reverse `MoveLocal` from
///     orphan to source folder). Drop every Blocker (the
///     `UploadMessage`/`UpdateRemoteKeywords` ones are
///     consume-on-destroy; the `MoveRemote` is paired with
///     its counter). Drop Moots. Emit `DeleteLocalFolder`.
///     Server's destroy wins; user's just-made changes are
///     reversed locally where possible and discarded where
///     not.
///
///   * **Blocker + `LocalWins`**: drop Blockers and Moots,
///     emit resurrect (`CreateRemoteMailbox` +
///     per-`orphan.messages` `UploadMessage`s via
///     `emit_resurrect_for`). User's intent wins -- the
///     mailbox is re-created server-side and the orphan's
///     contents get re-uploaded. The user's cycle-local
///     actions are NOT replayed against the resurrected
///     mailbox today (the resurrect path uploads from the
///     `orphan.messages` snapshot, which captures most of
///     the user's content; race-window files that arrived
///     after the snapshot are detected on the next cycle
///     when scan walks the resurrected mailbox).
///
/// The no-Blocker arm above is unconditional on conflict
/// strategy because there is no concurrent user action to
/// reconcile against; conflict strategy only enters the
/// dispatch when at least one Blocker has been classified.
fn finalize_local_orphan_handling(ctx: &ReconcileCtx<'_>, plan: &mut SyncPlan) {
    if !ctx.policy.allows_delete_of_local_folder() {
        return;
    }
    if ctx.mailboxes.local_orphans().is_empty() {
        return;
    }

    let mut keep_mask: Vec<bool> = vec![true; plan.actions.len()];
    let mut additions: Vec<SyncAction> = Vec::new();
    let mut needs_resurrect: Vec<usize> = Vec::new();

    for (orphan_idx, orphan) in ctx.mailboxes.local_orphans().iter().enumerate() {
        let (blockers, moots) = classify_actions_against_orphan(plan, orphan, ctx, &keep_mask);
        dispatch_orphan_decision(
            ctx,
            orphan,
            orphan_idx,
            &blockers,
            &moots,
            plan,
            &mut keep_mask,
            &mut additions,
            &mut needs_resurrect,
        );
    }

    apply_orphan_decisions(plan, &keep_mask, additions, &needs_resurrect, ctx);
}

/// Walk `plan.actions` and bucket indices that target `orphan` into
/// (blockers, moots). Skips entries already masked off by an earlier
/// orphan's pass so per-orphan decisions don't double-count.
fn classify_actions_against_orphan(
    plan: &SyncPlan,
    orphan: &LocalOrphanRecord,
    ctx: &ReconcileCtx<'_>,
    keep_mask: &[bool],
) -> (Vec<usize>, Vec<usize>) {
    let dead_id = orphan
        .binding
        .jmap_mailbox_id
        .expect_resolved("classify_actions_against_orphan -- orphan binding is resolved");
    let folder = &orphan.binding.maildir_folder;

    let mut blockers = Vec::new();
    let mut moots = Vec::new();
    for (i, action) in plan.actions.iter().enumerate() {
        if !keep_mask[i] {
            continue;
        }
        match classify_action_against_orphan(action, dead_id, folder, ctx.known_by_jmap) {
            OrphanActionRole::Blocker => blockers.push(i),
            OrphanActionRole::Moot => moots.push(i),
            OrphanActionRole::Neutral => {}
        }
    }
    (blockers, moots)
}

/// Per-orphan three-arm dispatch. Mutates `keep_mask` (to drop
/// blockers/moots), `additions` (to emit `DeleteLocalFolder` and any
/// counter-`MoveLocal`s), and `needs_resurrect` (when the strategy
/// is to preserve local content via the resurrect path).
#[allow(clippy::too_many_arguments)]
fn dispatch_orphan_decision(
    ctx: &ReconcileCtx<'_>,
    orphan: &LocalOrphanRecord,
    orphan_idx: usize,
    blockers: &[usize],
    moots: &[usize],
    plan: &SyncPlan,
    keep_mask: &mut [bool],
    additions: &mut Vec<SyncAction>,
    needs_resurrect: &mut Vec<usize>,
) {
    if blockers.is_empty() {
        resolve_no_blocker(orphan, moots, keep_mask, additions);
        return;
    }
    match ctx.policy.conflict_strategy {
        ConflictStrategy::ServerWins => resolve_server_wins_with_blocker(
            ctx, orphan, blockers, moots, plan, keep_mask, additions,
        ),
        ConflictStrategy::LocalWins => {
            resolve_local_wins_with_blocker(orphan_idx, blockers, moots, keep_mask, needs_resurrect)
        }
    }
}

/// No cycle-local Blocker: the user opted in to destructive folder
/// sync, so drop Moots and emit the destroy. Any pre-existing
/// content vanishes with the folder.
fn resolve_no_blocker(
    orphan: &LocalOrphanRecord,
    moots: &[usize],
    keep_mask: &mut [bool],
    additions: &mut Vec<SyncAction>,
) {
    for i in moots {
        keep_mask[*i] = false;
    }
    additions.push(SyncAction::DeleteLocalFolder {
        binding: Arc::clone(&orphan.binding),
    });
}

/// Server's destroy wins despite a cycle-local user action
/// targeting the orphan. For each `MoveRemote`-into-orphan Blocker,
/// emit a counter-`MoveLocal` that physically returns the file to
/// its source folder before the destroy fires. Other Blocker
/// variants (`UploadMessage`, `UpdateRemoteKeywords`) are
/// consume-on-destroy -- dropping them is enough.
fn resolve_server_wins_with_blocker(
    ctx: &ReconcileCtx<'_>,
    orphan: &LocalOrphanRecord,
    blockers: &[usize],
    moots: &[usize],
    plan: &SyncPlan,
    keep_mask: &mut [bool],
    additions: &mut Vec<SyncAction>,
) {
    let folder = &orphan.binding.maildir_folder;
    let dead_id = orphan
        .binding
        .jmap_mailbox_id
        .expect_resolved("resolve_server_wins_with_blocker -- orphan binding is resolved");
    for i in blockers {
        if let Some(counter) = build_counter_move_local(&plan.actions[*i], folder, dead_id, ctx) {
            additions.push(counter);
        }
    }
    for i in blockers {
        keep_mask[*i] = false;
    }
    for i in moots {
        keep_mask[*i] = false;
    }
    additions.push(SyncAction::DeleteLocalFolder {
        binding: Arc::clone(&orphan.binding),
    });
}

/// User's intent (preserve local content) wins. Drop Blockers and
/// Moots; defer resurrect emission to the apply phase. The
/// resurrect path's `orphan.messages` sweep captures the snapshot
/// at orphan-detection time; race-window files arriving after the
/// snapshot are caught next cycle.
fn resolve_local_wins_with_blocker(
    orphan_idx: usize,
    blockers: &[usize],
    moots: &[usize],
    keep_mask: &mut [bool],
    needs_resurrect: &mut Vec<usize>,
) {
    for i in blockers {
        keep_mask[*i] = false;
    }
    for i in moots {
        keep_mask[*i] = false;
    }
    needs_resurrect.push(orphan_idx);
}

/// For a `MoveRemote`-into-orphan Blocker, build the
/// `MoveLocal { from: orphan, to: source }` counter that reverses
/// the user's move before the orphan-destroy fires. Returns `None`
/// for non-`MoveRemote` actions and for two warn-and-drop cases:
/// the source folder isn't in the live set (also an orphan, or
/// otherwise gone), or `known_by_jmap_to_maildir_id` can't
/// reconstruct the file's local id (cycle-local adopt with no
/// `message_map` row yet). In both warn-and-drop cases the file
/// gets destroyed with the orphan folder rather than rescued.
fn build_counter_move_local(
    action: &SyncAction,
    orphan_folder: &str,
    dead_id: &JmapMailboxId,
    ctx: &ReconcileCtx<'_>,
) -> Option<SyncAction> {
    let SyncAction::MoveRemote {
        id, from_folder, ..
    } = action
    else {
        return None;
    };
    let Some(source_binding) = ctx.mailboxes.by_folder(from_folder) else {
        warn!(
            "ServerWins counter-MoveLocal for orphan {}: source folder {} is not in live set; \
             dropping counter and the user's move both -- file destroyed with orphan folder",
            dead_id, from_folder
        );
        return None;
    };
    let Some(maildir_id) = known_by_jmap_to_maildir_id(&id.jmap_email_id, ctx.known_by_jmap) else {
        warn!(
            "ServerWins counter-MoveLocal for orphan {}: no maildir_id known for {} \
             (cycle-local adopt with no message_map row yet); dropping counter and the user's \
             move both -- file destroyed with orphan folder",
            dead_id, id.jmap_email_id
        );
        return None;
    };
    Some(SyncAction::MoveLocal {
        id: BoundId {
            maildir_id,
            jmap_email_id: id.jmap_email_id.clone(),
            message_id: id.message_id.clone(),
        },
        from_folder: orphan_folder.to_string(),
        to_binding: Arc::clone(source_binding),
    })
}

/// Commit the per-orphan decisions back to the plan: filter actions
/// by `keep_mask`, append `additions` (counter-moves + destroys),
/// then emit resurrect actions for any orphan whose strategy is to
/// preserve local content.
fn apply_orphan_decisions(
    plan: &mut SyncPlan,
    keep_mask: &[bool],
    additions: Vec<SyncAction>,
    needs_resurrect: &[usize],
    ctx: &ReconcileCtx<'_>,
) {
    let kept: Vec<SyncAction> = std::mem::take(&mut plan.actions)
        .into_iter()
        .enumerate()
        .filter_map(|(i, a)| if keep_mask[i] { Some(a) } else { None })
        .collect();
    plan.actions = kept;
    plan.actions.extend(additions);
    for &orphan_idx in needs_resurrect {
        emit_resurrect_for(&ctx.mailboxes.local_orphans()[orphan_idx], ctx, plan);
    }
}

/// Role of a cycle-local server email change with respect to one
/// remote orphan (push-side destructive arm). The orphan's id is
/// known dead from the local side (user deleted the maildir); a
/// remote_emails entry whose `mailbox_ids` contains the orphan's
/// id is the Blocker signal.
///
///   * `Move`: the email's cycle-start cached membership points at
///     a different (live) mailbox. The server moved it INTO the
///     orphan; the prior source is recoverable. Under `LocalWins`,
///     paired with a counter `MoveRemote` that puts the email back
///     at its source before the destroy fires. Under jma's single-
///     mailbox-per-email DB model `prior_source` is the email's
///     full prior membership; the multi-mailbox case (an email in
///     `{orphan, live_A}` with two prior members) is out of scope.
///   * `Consumed`: either the server added a new email directly to
///     the orphan (no cached row) or the server only changed
///     keywords on an email already in the orphan (cached row's
///     mailbox matches the orphan id). Either way the email goes
///     away with the mailbox under `LocalWins` -- no counter, no
///     observable difference between the two cases at dispatch.
enum RemoteOrphanBlocker {
    Move {
        id: JmapEmailId,
        message_id: MessageId,
        prior_source: JmapMailboxId,
    },
    Consumed,
}

/// Per-email classifier. Returns `None` for any email whose
/// `mailbox_ids` does not contain the orphan's id; otherwise the
/// cached `message_map` row decides between `Move` and `Consumed`.
fn classify_email_against_remote_orphan(
    email: &EmailObject,
    dead_id: &JmapMailboxId,
    known_by_jmap: &HashMap<JmapEmailId, Arc<MessageRecord>>,
) -> Option<RemoteOrphanBlocker> {
    if !email.mailbox_ids.contains_key(dead_id) {
        return None;
    }
    match known_by_jmap.get(&email.id) {
        Some(prior) if &prior.jmap_mailbox_id != dead_id => Some(RemoteOrphanBlocker::Move {
            id: email.id.clone(),
            message_id: prior.message_id.clone(),
            prior_source: prior.jmap_mailbox_id.clone(),
        }),
        _ => Some(RemoteOrphanBlocker::Consumed),
    }
}

/// Push-side destructive-arm finalizer. Gated on
/// `allows_delete_of_remote_folder()`. For each remote orphan,
/// classifies cycle-local server email changes (`ctx.remote_emails`)
/// targeting the orphan and dispatches per the matrix:
///
///   * **No Blocker**: emit `DestroyRemoteMailbox`. The user opted
///     in to destructive folder sync; the server-side mailbox goes
///     away. No conflict to resolve, so conflict strategy doesn't
///     apply -- mirrors the pull-side no-Blocker arm.
///
///   * **Blocker + `LocalWins`**: the user's intent (destroy this
///     mailbox) wins. For each `Move` Blocker emit a counter
///     `MoveRemote { from: orphan, to: prior_source }` BEFORE the
///     destroy so the server's just-made move gets reversed and
///     the email lands back at its source. `Consumed` Blockers
///     (new emails / keyword changes) go away with the mailbox.
///     Then emit `DestroyRemoteMailbox`.
///
///   * **Blocker + `ServerWins`**: handled engine-side before
///     reconcile runs. `SyncEngine::resurrect_remote_orphans_for_
///     server_wins` hoists the orphan back into the live set,
///     drops its `message_map` rows, and fetches the orphan's
///     server contents; reconcile then re-creates the maildir
///     via `CreateLocalMailbox` and downloads each email via
///     `DownloadMessage` (the local files are gone with the
///     deleted maildir, so the adopt branch has nothing to match
///     against). The pre-pass also removes the resurrected
///     orphan from `remote_orphans()` so this finalizer never
///     sees it -- only no-Blocker and `LocalWins`-with-Blocker
///     cases survive.
fn finalize_remote_orphan_handling(ctx: &ReconcileCtx<'_>, plan: &mut SyncPlan) {
    if !ctx.policy.allows_delete_of_remote_folder() {
        return;
    }
    if ctx.mailboxes.remote_orphans().is_empty() {
        return;
    }
    for orphan in ctx.mailboxes.remote_orphans() {
        let dead_id = orphan
            .binding
            .jmap_mailbox_id
            .expect_resolved("finalize_remote_orphan_handling -- orphan binding is resolved");
        let blockers: Vec<RemoteOrphanBlocker> = ctx
            .remote_emails
            .iter()
            .filter_map(|email| {
                classify_email_against_remote_orphan(email, dead_id, ctx.known_by_jmap)
            })
            .collect();
        if blockers.is_empty() {
            plan.actions.push(SyncAction::DestroyRemoteMailbox {
                binding: Arc::clone(&orphan.binding),
            });
            continue;
        }
        // LocalWins-with-Blocker: per-Move counter-action then
        // destroy. ServerWins-with-Blocker was pre-handled engine-
        // side so it can't reach here; if it does (shouldn't),
        // treat as no-conflict and destroy the same way.
        for blocker in &blockers {
            if let RemoteOrphanBlocker::Move {
                id,
                message_id,
                prior_source,
            } = blocker
                && let Some(counter) =
                    build_counter_move_remote(id, message_id, prior_source, &orphan.binding, ctx)
            {
                plan.actions.push(counter);
            }
        }
        plan.actions.push(SyncAction::DestroyRemoteMailbox {
            binding: Arc::clone(&orphan.binding),
        });
    }
}

/// For a `Move` Blocker (server moved email INTO orphan from a
/// recoverable prior source), build the `MoveRemote { from:
/// orphan, to: prior_source }` counter that reverses the move
/// before the orphan-destroy fires. Returns `None` when the
/// prior source isn't in the live set (also an orphan, or
/// otherwise gone); in that warn-and-drop case the email gets
/// destroyed with the orphan mailbox.
fn build_counter_move_remote(
    id: &JmapEmailId,
    message_id: &MessageId,
    prior_source: &JmapMailboxId,
    orphan_binding: &MailboxFolderBinding,
    ctx: &ReconcileCtx<'_>,
) -> Option<SyncAction> {
    let Some(source_binding) = ctx.mailboxes.by_id(prior_source) else {
        warn!(
            "LocalWins counter-MoveRemote for orphan {}: prior source {} is not in live set; \
             dropping counter -- email {} gets destroyed with the orphan mailbox",
            orphan_binding.remote_path, prior_source, id
        );
        return None;
    };
    Some(SyncAction::MoveRemote {
        id: RemoteId {
            jmap_email_id: id.clone(),
            message_id: message_id.clone(),
        },
        target_mailbox_ids: vec![prior_source.clone()],
        from_folder: orphan_binding.maildir_folder.clone(),
        to_folder: source_binding.maildir_folder.clone(),
    })
}

/// Look up a maildir_id given a jmap_email_id via the
/// `known_by_jmap` index. Used by counter-action emission to
/// reconstruct the `BoundId` for a `MoveLocal` derived from a
/// `MoveRemote` whose original `id: RemoteId` lacks the
/// maildir_id.
fn known_by_jmap_to_maildir_id(
    jmap_email_id: &JmapEmailId,
    known_by_jmap: &HashMap<JmapEmailId, Arc<MessageRecord>>,
) -> Option<MaildirId> {
    known_by_jmap
        .get(jmap_email_id)
        .and_then(|r| r.maildir_id.clone())
}

/// One paired DeletedMessage(src) + NewMessage(dst) discovered during
/// the move pre-pass.
struct DetectedMove {
    jmap_email_id: JmapEmailId,
    old_maildir_id: MaildirId,
    new_maildir_id: MaildirId,
    from_folder: String,
    to_binding: Arc<MailboxFolderBinding>,
    new_flags: String,
    jmap_blob_id: Option<JmapBlobId>,
    jmap_thread_id: Option<JmapThreadId>,
    message_id: MessageId,
    /// Flags as recorded in message_map at the start of the cycle;
    /// compared against new_flags to decide whether the move should
    /// also push keyword changes.
    prior_flags: String,
}

/// Emit one `CreateLocalMailbox` action per binding that
/// `resolve_mailboxes` flagged as first-cycle-new (server-known
/// but absent from the pre-upsert `mailbox_map` cache). The
/// executor's first phase translates each into `ensure_maildir`
/// plus `sentinel::write` before any downstream phase assumes
/// the folder is present on disk.
///
/// Empty in steady state -- once a mailbox has been synced
/// once, its `mailbox_map` row sticks around across cycles and
/// the helper has nothing to emit.
fn emit_create_local_mailboxes(mailboxes: &MailboxBindings, plan: &mut SyncPlan) {
    for new in mailboxes.new_mailboxes() {
        plan.actions.push(SyncAction::CreateLocalMailbox {
            binding: Arc::new(new.binding.clone()),
            parent_jmap_mailbox_id: new.parent_jmap_mailbox_id.clone(),
            replaces_orphan_id: new.replaces_orphan_id.clone(),
        });
    }
}

/// Emit one rename action per binding that `resolve_mailboxes`
/// flagged with a changed folder name, dispatching on the
/// `RenameDirection` discriminator the record carries:
/// `Pull` -> `RenameLocalMailbox` (server renamed; bring disk
/// into agreement); `Push` -> `RenameRemoteMailbox` (user
/// `mv`-ed locally or a `LocalWins` conflict resolved that way;
/// push the new name + parent to the server). The executor walks
/// each direction's phase in push order, which
/// `resolve_mailboxes` already sorts shallowest-first. The
/// ordering matters for different reasons on each side:
///
/// - Pull: under `LAYOUT=fs` a parent's single `fs::rename`
///   moves the descendant subtree, and each descendant's own
///   iteration then runs against the already-moved target via
///   the executor's source-missing/target-present branch.
/// - Push: a child's `decompose_disk_path_to_jmap` resolves its
///   new parent_id by looking the new parent path up in the
///   pre-cycle `cached_by_folder` snapshot, which is computed
///   once before the loop and never advanced mid-cycle.
///   Single-leaf renames (only the child moved on disk; its
///   parent path is unchanged) converge this cycle: the disk
///   parent path equals the cached parent path, decompose
///   hits the snapshot, and shallowest-first ordering ensures
///   the parent's `RenameRemoteMailbox` lands first under the
///   executor so the JMAP-level parent_id resolves correctly.
///   Recursive renames (parent and child both moved on disk
///   as a unit) take one cycle per level: the child's disk
///   parent path is the NEW path, absent from the pre-cycle
///   snapshot, so decompose returns "untracked parent" and the
///   decision falls to `Unchanged`. The parent's executor-
///   phase upsert advances `mailbox_map` for the next cycle's
///   snapshot, and the child decomposes successfully there.
///
/// Empty in steady state.
fn emit_rename_mailboxes(mailboxes: &MailboxBindings, plan: &mut SyncPlan) {
    use crate::sync::bindings::RenameDirection;
    for renamed in mailboxes.renamed_mailboxes() {
        match renamed.direction {
            RenameDirection::Pull => {
                plan.actions.push(SyncAction::RenameLocalMailbox {
                    from_folder: renamed.from_folder.clone(),
                    from_db_folder: renamed.from_db_folder.clone(),
                    binding: Arc::new(renamed.binding.clone()),
                    parent_jmap_mailbox_id: renamed.parent_jmap_mailbox_id.clone(),
                });
            }
            RenameDirection::Push => {
                plan.actions.push(SyncAction::RenameRemoteMailbox {
                    from_folder: renamed.from_db_folder.clone(),
                    binding: Arc::new(renamed.binding.clone()),
                    parent_jmap_mailbox_id: renamed.parent_jmap_mailbox_id.clone(),
                });
            }
        }
    }
}

/// Emit resurrect actions for each `LocalOrphanRecord` whose
/// policy permits non-destructive handling. The default
/// (`allow_destructive_folder_sync = None`) and the
/// `delete-remote` variant both forbid destroying the local
/// side, so they fall through here: re-create the mailbox
/// server-side and upload every cataloged message. The
/// destructive variants (`delete-local`/`both`) skip this
/// emission -- under those policies the local maildir is
/// allowed to be destroyed in response to a server-side
/// deletion, so the resurrect path (which preserves local
/// content) doesn't apply.
///
/// Each orphan produces one `CreateRemoteMailbox` keyed on
/// the orphan's `maildir_folder` (creation_refs key) plus one
/// `UploadMessage` per cataloged message. The upload's
/// `binding.jmap_mailbox_id` carries a
/// `MaybeReference::Reference(folder)` that the executor
/// resolves to the freshly-assigned server id via
/// `resolve_upload_references`. The create's
/// `replaces_orphan_id` carries the dead id so the executor
/// pre-deletes the orphan's stale `message_map` rows before
/// the new uploads write fresh ones.
fn emit_local_orphan_resurrects(ctx: &ReconcileCtx<'_>, plan: &mut SyncPlan) {
    if ctx.policy.allows_delete_of_local_folder() {
        return;
    }
    for orphan in ctx.mailboxes.local_orphans() {
        emit_resurrect_for(orphan, ctx, plan);
    }
}

/// Emit the `CreateRemoteMailbox` + per-message
/// `UploadMessage` pair that resurrects one local orphan.
/// Factored out of `emit_local_orphan_resurrects` so the
/// destructive arm's LocalWins-with-Blocker branch can reach
/// the same emission when a per-orphan conflict resolution
/// requires preserving the local content despite the user
/// having opted in to destructive folder sync.
fn emit_resurrect_for(orphan: &LocalOrphanRecord, ctx: &ReconcileCtx<'_>, plan: &mut SyncPlan) {
    let dead_id = orphan
        .binding
        .jmap_mailbox_id
        .expect_resolved("emit_resurrect_for -- orphan binding is resolved")
        .clone();
    let folder = orphan.binding.maildir_folder.clone();
    plan.actions.push(SyncAction::CreateRemoteMailbox {
        name: orphan.binding.server_name.clone(),
        parent_jmap_mailbox_id: orphan
            .parent_jmap_mailbox_id
            .clone()
            .map(MaybeReference::Value),
        role: None,
        folder: folder.clone(),
        replaces_orphan_id: Some(dead_id),
    });
    for message in &orphan.messages {
        if message.size_bytes > ctx.max_upload_size as u64 {
            warn!(
                "Skipping orphan-resurrect upload for {} ({} bytes > server cap \
                 {}); file stays on disk in {}",
                message.message_id, message.size_bytes, ctx.max_upload_size, folder
            );
            continue;
        }
        plan.actions.push(SyncAction::UploadMessage {
            id: LocalId {
                maildir_id: message.maildir_id.clone(),
                message_id: message.message_id.clone(),
            },
            binding: Arc::new(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Reference(folder.clone()),
                server_name: orphan.binding.server_name.clone(),
                maildir_folder: folder.clone(),
                remote_path: orphan.binding.remote_path.clone(),
            }),
            file_path: message.path.clone(),
            flags: message.flags.clone(),
        });
    }
}

fn emit_detected_moves(moves: &[DetectedMove], plan: &mut SyncPlan) {
    for m in moves {
        plan.actions.push(SyncAction::MoveRemote {
            id: RemoteId {
                jmap_email_id: m.jmap_email_id.clone(),
                message_id: m.message_id.clone(),
            },
            // jma's DB binds each email to exactly one mailbox,
            // so the target set after the move is just the destination.
            // If the email also lives in unsynced JMAP mailboxes (e.g.
            // server-side label rules), this full-replacement strips
            // those memberships -- accepted for now; the alternative
            // is a per-cycle Email/get to read the current set first.
            target_mailbox_ids: vec![
                m.to_binding
                    .jmap_mailbox_id
                    .expect_resolved("reconcile::move_pair -- MoveRemote target")
                    .clone(),
            ],
            from_folder: m.from_folder.clone(),
            to_folder: m.to_binding.maildir_folder.clone(),
        });
        let keywords = flags_to_keywords(&m.new_flags);
        plan.actions.push(SyncAction::AdoptLocalMessage {
            id: BoundId {
                maildir_id: m.new_maildir_id.clone(),
                jmap_email_id: m.jmap_email_id.clone(),
                message_id: m.message_id.clone(),
            },
            binding: Arc::clone(&m.to_binding),
            jmap_blob_id: m.jmap_blob_id.clone(),
            jmap_thread_id: m.jmap_thread_id.clone(),
            keywords: keywords.clone(),
            filename_flags: m.new_flags.clone(),
            old_maildir_id: Some(m.old_maildir_id.clone()),
            old_jmap_email_id: None,
        });
        if m.prior_flags != m.new_flags {
            plan.actions.push(SyncAction::UpdateRemoteKeywords {
                id: RemoteId {
                    jmap_email_id: m.jmap_email_id.clone(),
                    message_id: m.message_id.clone(),
                },
                keywords,
                filename_flags: m.new_flags.clone(),
            });
        }
    }
}

fn process_remote_emails(
    ctx: &ReconcileCtx<'_>,
    adopted_maildir_ids: &mut HashSet<MaildirId>,
    deletes_overruled_by_server: &mut HashSet<JmapEmailId>,
    consumed_remote_destroys: &mut HashSet<JmapEmailId>,
    plan: &mut SyncPlan,
) {
    for email in ctx.remote_emails {
        if ctx.destroyed_set.contains(email.id.as_ref()) {
            // Will be handled by the destroyed pass.
            continue;
        }

        let mailbox_match = ctx.mailboxes.iter().find(|b| {
            email.mailbox_ids.contains_key(
                b.jmap_mailbox_id
                    .expect_resolved("reconcile::process_remote_emails -- mailbox membership"),
            )
        });
        let Some(binding) = mailbox_match else {
            debug!(
                "Remote email {} not in any synced mailbox, skipping",
                email.id
            );
            continue;
        };

        let matched = RemoteMatch {
            email,
            target_binding: binding,
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
        // jma's idempotency invariant requires Message-ID as the
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
                 jma requires Message-ID to anchor idempotency across state-DB wipes; \
                 the server returned an RFC-violating email and we won't ingest it.",
                email.id, binding.maildir_folder
            );
            continue;
        };

        // Path 2: not bound by JMAP id, but Message-ID matches a local file
        // (either via DB carry-over from a half-completed prior run, or via
        // the dedupe-pass index after a state DB wipe). Adopt it.
        if try_adopt_remote(
            ctx,
            &matched,
            &local_msg_id,
            adopted_maildir_ids,
            consumed_remote_destroys,
            plan,
        ) {
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
            binding: Arc::clone(binding),
            keywords: email.keywords.clone(),
        });
    }
}

/// One iteration's worth of "this remote email maps to that local target":
/// the email itself plus the chosen mailbox binding. Computed once in
/// `process_remote_emails` and threaded through both the JMAP-bound and
/// adopt paths.
#[derive(Clone, Copy)]
struct RemoteMatch<'a> {
    email: &'a EmailObject,
    target_binding: &'a Arc<MailboxFolderBinding>,
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
        target_binding,
    } = *matched;

    // Conflict: local deleted the file while the server updated
    // it. Resolve before any flag/move emission, since the local
    // copy is gone either way.
    if ctx.local_deletes.contains(&email.id) {
        match resolve_delete_conflict(&email.id, ctx.policy.conflict_strategy) {
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
                    binding: Arc::clone(target_binding),
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
            ctx.policy.conflict_strategy,
        );
        match resolved {
            FlagWinner::Server => {
                emit_local_flag_update(plan, existing, email, ctx.mailboxes);
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
                        filename_flags: new_flags.clone(),
                    });
                }
            }
        }
    } else if server_flag_change {
        emit_local_flag_update(plan, existing, email, ctx.mailboxes);
    }

    // Mailbox-membership change: server claims the email lives in
    // a different mailbox than where our local copy is bound.
    // TODO: cross-detect when the local copy was *also* moved
    // (scan emits NewMessage in dest + DeletedMessage in src,
    // not a true Move). For now, blindly follow the server.
    if existing.jmap_mailbox_id
        != *target_binding
            .jmap_mailbox_id
            .expect_resolved("reconcile::handle_known_remote -- membership compare")
        && let Some(from_binding) = ctx.mailboxes.by_id(&existing.jmap_mailbox_id)
        && let Some(local_maildir_id) = &existing.maildir_id
    {
        plan.actions.push(SyncAction::MoveLocal {
            id: BoundId {
                maildir_id: local_maildir_id.clone(),
                jmap_email_id: email.id.clone(),
                message_id: existing.message_id.clone(),
            },
            from_folder: from_binding.maildir_folder.clone(),
            to_binding: Arc::clone(target_binding),
        });
    }
}

fn try_adopt_remote(
    ctx: &ReconcileCtx<'_>,
    matched: &RemoteMatch<'_>,
    mid: &MessageId,
    adopted_maildir_ids: &mut HashSet<MaildirId>,
    consumed_remote_destroys: &mut HashSet<JmapEmailId>,
    plan: &mut SyncPlan,
) -> bool {
    let RemoteMatch {
        email,
        target_binding,
    } = *matched;

    let push_adopt = |plan: &mut SyncPlan,
                      adopted: &mut HashSet<MaildirId>,
                      maildir_id: MaildirId,
                      filename_flags: String,
                      old_jmap_email_id: Option<JmapEmailId>| {
        let bound_id = BoundId {
            maildir_id: maildir_id.clone(),
            jmap_email_id: email.id.clone(),
            message_id: mid.clone(),
        };
        adopted.insert(maildir_id);
        plan.actions.push(SyncAction::AdoptLocalMessage {
            id: bound_id.clone(),
            binding: Arc::clone(target_binding),
            jmap_blob_id: Some(email.blob_id.clone()),
            jmap_thread_id: Some(email.thread_id.clone()),
            keywords: email.keywords.clone(),
            filename_flags: filename_flags.clone(),
            old_maildir_id: None,
            old_jmap_email_id,
        });
        emit_adoption_flag_reconciliation(
            plan,
            ctx.policy.conflict_strategy,
            &AdoptionReconciliation {
                bound_id: &bound_id,
                binding: target_binding,
                server_keywords: &email.keywords,
                jmap_blob_id: Some(&email.blob_id),
                jmap_thread_id: Some(&email.thread_id),
                on_disk_flags: &filename_flags,
            },
        );
    };

    // Branch A: a DB row already exists for this Message-ID. Three
    // sub-cases distinguished by what the server says about the
    // matched record's jmap_email_id:
    //   (i)  In this cycle's destroyed_set -- the server destroyed
    //        Email A and re-created Email B with the same wire-
    //        format Message-ID. Pair them into a single rebind:
    //        adopt the file under B's id and tell commit_adopt to
    //        drop A's row in the same txn so the unique-on-maildir-
    //        id index doesn't refuse the new row. Mark A as consumed
    //        so the destroy pass doesn't re-emit DeleteLocal against
    //        the file we just rebound.
    //   (ii) Alive on the server (A's id appears in remote_emails,
    //        or we lack positive evidence of staleness) -- two
    //        coexisting Emails sharing a Message-ID (duplicate
    //        delivery, migration-tool import, or an older jma upload
    //        bug). Refuse the adopt: an upsert with the same
    //        maildir_id and a new jmap_email_id violates the
    //        partial-unique index, rolling the cycle txn back. Emit
    //        a warn pointing at `jma janitor remotededupe` and
    //        return as if handled, so the executor neither downloads
    //        a third copy nor stalls the cycle. The duplicate stays
    //        on the server until the janitor scan blob-checks the
    //        group and destroys the extras.
    //   (iii) Provably gone from the server -- carry-over-from-
    //        stale-DB. A's id is absent from this cycle's
    //        remote_emails AND the cycle used the initial-pull path,
    //        meaning remote_emails is the full survivor set across
    //        synced mailboxes. Only with both conditions can we
    //        treat absence as "destroyed without notification."
    //        Rebind to B and let commit_adopt drop A's stale row.
    //        Same code path as (i) minus the consumed-destroys
    //        entry. The incremental-mode equivalent ("A absent
    //        because it didn't change this cycle") deliberately
    //        falls into (ii): in incremental mode Email/changes
    //        can't distinguish "destroyed without notification"
    //        from "alive but unchanged," so silently rebinding to
    //        B and dropping A's row would set up a flip-flop --
    //        the next cycle in which A's flags mutate would find
    //        no row for A, re-adopt the file under A's id, then
    //        the cycle after that would do the same in reverse.
    // Per RFC 8620 JMAP ids are stable for an Email's lifetime, so
    // a "different JMAP id with the same Message-ID" always means a
    // different Email object, not a server-side id rewrite.
    //
    // Filename-truth source priority: this cycle's scan first
    // (always accurate when the path was in the event set or a
    // full scan ran), then the DB record's `flags` column as a
    // steady-state fallback. The DB column is what `commit_adopt`
    // or the apply_remote_set mirror last wrote -- accurate when
    // filename and server keywords already agree at last sync time,
    // which is the steady-state contract that `commit_adopt`'s
    // split and `emit_adoption_flag_reconciliation` keep current.
    if let Some(recs) = ctx.known_by_message_id.get(mid)
        && let Some(rec) = recs.iter().find(|r| {
            r.jmap_mailbox_id
                == *target_binding
                    .jmap_mailbox_id
                    .expect_resolved("reconcile::try_adopt_known -- target compare")
        })
        && let Some(maildir_id) = rec.maildir_id.clone()
    {
        let in_destroyed = ctx.destroyed_set.contains(rec.jmap_email_id.as_ref());
        let old_in_remote_emails = ctx
            .remote_emails
            .iter()
            .any(|e| e.id.as_ref() == rec.jmap_email_id.as_ref());
        let provably_gone = !in_destroyed && !old_in_remote_emails && ctx.used_initial_path;
        if !in_destroyed && !provably_gone {
            warn!(
                "Server-side duplicate Message-ID in {}: {} is bound to JMAP id \
                 {} (maildir_id {}); new JMAP id {} carries the same header. \
                 Skipping adoption to avoid a partial-unique violation -- run \
                 `jma janitor remotededupe` to destroy the duplicate copies.",
                target_binding.maildir_folder, mid, rec.jmap_email_id, maildir_id, email.id
            );
            return true;
        }
        let filename_flags = ctx
            .local_flags
            .get(&maildir_id)
            .cloned()
            .unwrap_or_else(|| rec.flags.clone());
        let old_jmap_email_id = rec.jmap_email_id.clone();
        if in_destroyed {
            consumed_remote_destroys.insert(old_jmap_email_id.clone());
        }
        push_adopt(
            plan,
            adopted_maildir_ids,
            maildir_id,
            filename_flags,
            Some(old_jmap_email_id),
        );
        return true;
    }

    // Cold-start rescue: state DB is empty, the in-memory index from
    // this cycle's dedupe walk identifies the file, and the full
    // scan that fires alongside cold-start populates local_flags
    // exhaustively. The unwrap_or_default fallback is defensive only
    // -- it would mean a future refactor decoupled dedupe from
    // scan's walk; an empty filename suffix degrades safely (the
    // next scan's FlagsChanged path catches the drift).
    //
    // Per-cycle duplicate guard: if a prior iteration in this same
    // cycle already adopted the local file (server returned two
    // Email objects sharing the Message-ID), refuse the second
    // adopt for the same reason Branch A sub-case ii does -- the
    // partial-unique index would reject the upsert. Warn and skip.
    if let Some(entries) = ctx.local_index.by_message_id.get(mid)
        && let Some(entry) = entries
            .iter()
            .find(|e| e.folder == target_binding.maildir_folder)
    {
        if adopted_maildir_ids.contains(&entry.maildir_id) {
            warn!(
                "Server-side duplicate Message-ID in {}: {} was already adopted \
                 to maildir_id {} earlier in this cycle; new JMAP id {} carries \
                 the same header. Skipping adoption -- run `jma janitor \
                 remotededupe` to destroy the duplicate copies on the server.",
                target_binding.maildir_folder, mid, entry.maildir_id, email.id
            );
            return true;
        }
        let filename_flags = ctx
            .local_flags
            .get(&entry.maildir_id)
            .cloned()
            .unwrap_or_default();
        push_adopt(
            plan,
            adopted_maildir_ids,
            entry.maildir_id.clone(),
            filename_flags,
            None,
        );
        return true;
    }

    false
}

fn process_remote_destroys(
    remote_destroyed: &[JmapEmailId],
    known_by_jmap: &HashMap<JmapEmailId, Arc<MessageRecord>>,
    mailboxes: &MailboxBindings,
    consumed_remote_destroys: &HashSet<JmapEmailId>,
    plan: &mut SyncPlan,
) {
    for jmap_id in remote_destroyed {
        // A destroy that was paired with a same-Message-ID create
        // earlier in this cycle (see `try_adopt_remote`'s Branch A
        // rebind sub-case) is not a delete from the local
        // filesystem's perspective -- it is the source half of an
        // AdoptLocalMessage that already rebound the file to the
        // new JMAP id. Emitting DeleteLocal here would delete the
        // file we just adopted, and execute's adopt-before-delete
        // phase order would also trip the unique-on-maildir-id
        // index even if the data-loss risk weren't enough on its
        // own.
        if consumed_remote_destroys.contains(jmap_id) {
            continue;
        }
        if let Some(msg) = known_by_jmap.get(jmap_id)
            && let Some(maildir_id) = &msg.maildir_id
            && let Some(binding) = mailboxes.by_id(&msg.jmap_mailbox_id)
        {
            plan.actions.push(SyncAction::DeleteLocal {
                id: BoundId {
                    maildir_id: maildir_id.clone(),
                    jmap_email_id: jmap_id.clone(),
                    message_id: msg.message_id.clone(),
                },
                maildir_folder: binding.maildir_folder.clone(),
            });
        }
    }
}

/// Emit the post-adoption action that bridges the standard-six gap
/// between the filename's flag suffix and `keywords_to_flags(server_
/// keywords)`. Adoption itself just records what each side currently
/// has; if the on-disk file disagrees on any of the six maildir-
/// mappable keywords, this is where `conflict_strategy` actually
/// decides which view propagates.
///
/// Called from each `AdoptLocalMessage` emit site, so all three
/// adoption paths (cold-start rescue, alreadyExists-on-upload guard,
/// post-prior-cycle `known_by_message_id` hit) get the same
/// convergence behavior. No-ops when filename and server-derived
/// flags already agree, so the steady-state case stays a pure
/// adoption with no extra wire traffic.
///
/// Non-standard server keywords ($imported, $hasattachment,
/// $x-me-annot-2, user-defined labels) are untouched: under
/// `LocalWins` we send an explicit-six patch via
/// `flags_to_keyword_patch`, leaving every non-standard key
/// unmentioned in the wire request; under `ServerWins` we rename
/// the local file to match the server's standard-six derivation and
/// the local filename has no way to carry non-standard keywords
/// anyway.
///
/// `jmap_blob_id` and `jmap_thread_id` are `Option` because
/// `MessageRecord` carries them as such (a row may exist before
/// blob/thread binding completes). `ServerWins` needs both to
/// emit `UpdateLocalFlags`; if either is missing, we skip the
/// reconciliation step and rely on the next sync cycle's
/// regular flag-update path to catch up.
/// Bundles the per-emit-site context `emit_adoption_flag_
/// reconciliation` needs. Grouping these here keeps the function
/// signature under the clippy too_many_arguments ceiling and makes
/// the two emit sites read identically (each builds one of these
/// from whatever shape it has -- an `EmailObject` for the remote
/// adopt path, a `MessageRecord` for the local NewMessage adopt
/// path).
struct AdoptionReconciliation<'a> {
    bound_id: &'a BoundId,
    binding: &'a Arc<MailboxFolderBinding>,
    server_keywords: &'a HashMap<String, bool>,
    jmap_blob_id: Option<&'a JmapBlobId>,
    jmap_thread_id: Option<&'a JmapThreadId>,
    on_disk_flags: &'a str,
}

fn emit_adoption_flag_reconciliation(
    plan: &mut SyncPlan,
    strategy: ConflictStrategy,
    rec: &AdoptionReconciliation<'_>,
) {
    let server_flags = keywords_to_flags(rec.server_keywords);
    if rec.on_disk_flags == server_flags {
        return;
    }
    match strategy {
        ConflictStrategy::ServerWins => {
            let (Some(blob_id), Some(thread_id)) = (rec.jmap_blob_id, rec.jmap_thread_id) else {
                debug!(
                    "Adoption flag reconciliation for {} skipped under ServerWins: \
                     missing blob_id/thread_id on the matched record. Next cycle's \
                     regular flag-update path will reconcile.",
                    rec.bound_id
                );
                return;
            };
            plan.actions.push(SyncAction::UpdateLocalFlags {
                id: rec.bound_id.clone(),
                binding: Arc::clone(rec.binding),
                new_flags: server_flags,
                keywords: rec.server_keywords.clone(),
                jmap_blob_id: blob_id.clone(),
                jmap_thread_id: thread_id.clone(),
            });
        }
        ConflictStrategy::LocalWins => {
            plan.actions.push(SyncAction::UpdateRemoteKeywords {
                id: RemoteId {
                    jmap_email_id: rec.bound_id.jmap_email_id.clone(),
                    message_id: rec.bound_id.message_id.clone(),
                },
                keywords: flags_to_keyword_patch(rec.on_disk_flags),
                filename_flags: rec.on_disk_flags.to_string(),
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
    // Per-cycle dedup set for folders already pushed as a
    // CreateRemoteMailbox. The chain-walking emitter naturally
    // hits shared ancestors twice when two `LocalFolderCreated`
    // events share a parent (e.g. `Foo.Bar` and `Foo.Quux` both
    // chain through `Foo`), and the executor would otherwise
    // issue two `Mailbox/set { create }` for `Foo` -- the second
    // would `alreadyExists`-reject, warn-and-continue, but the
    // round-trip is wasted and the log line is misleading.
    let mut emitted_remote_creates: HashSet<String> = HashSet::new();
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
            LocalChange::LocalFolderCreated { path, sentinel } => {
                handle_local_folder_created(
                    ctx,
                    path,
                    sentinel.as_ref(),
                    &mut emitted_remote_creates,
                    plan,
                );
            }
            // LocalFolderDeleted / LocalFolderRenamed have no consumer
            // yet; their server-side mirror actions land in later
            // commits.
            LocalChange::LocalFolderDeleted { .. } | LocalChange::LocalFolderRenamed { .. } => {}
        }
    }
}

/// Decide what to do with a `LocalChange::LocalFolderCreated`:
///
/// - If `sentinel` is `Some`, this folder was previously bound to a
///   JMAP mailbox; the cache lost the binding (state DB nuke, or
///   the sentinel survived a partial-failure cycle). Recovery is
///   `janitor::rebindfolders`'s job, not reconcile's. Skip.
/// - If `sentinel` is `None`, the folder is genuinely new locally.
///   Walk the parent chain via repeated `decompose_folder_string`
///   calls, stopping at the first bound ancestor (or the top of
///   the tree). Each unbound ancestor in the chain becomes its own
///   `CreateRemoteMailbox` emitted top-down; children whose parent
///   is being created in the same cycle carry
///   `Some(MaybeReference::Reference(parent_folder))` so the
///   executor's `creation_refs` table can resolve the parent id
///   once the parent's `Mailbox/set { create }` returns.
///   Already-bound parents flow as
///   `Some(MaybeReference::Value(parent_id))`. Top-level mailboxes
///   carry `None`.
///
/// Role is always `None`: a locally-created folder has no JMAP
/// role binding (the inbox/archive/sent/etc. roles only attach
/// server-side via Mailbox/get).
fn handle_local_folder_created(
    ctx: &ReconcileCtx<'_>,
    path: &Path,
    sentinel: Option<&crate::maildir_ops::sentinel::MailboxMapping>,
    emitted: &mut HashSet<String>,
    plan: &mut SyncPlan,
) {
    if let Some(m) = sentinel {
        debug!(
            "LocalFolderCreated at {} carries sentinel for {}; rebindfolders owns recovery, \
             skipping CreateRemoteMailbox",
            path.display(),
            m.jmap_mailbox_id
        );
        return;
    }
    let Ok(rel) = path.strip_prefix(ctx.maildir_root) else {
        debug!(
            "LocalFolderCreated path {} is not under maildir_root; skipping",
            path.display()
        );
        return;
    };
    let Some(rel_str) = rel.to_str() else {
        debug!(
            "LocalFolderCreated path {} is not UTF-8; skipping",
            path.display()
        );
        return;
    };

    // Walk leaf-to-root collecting (folder, leaf, parent_folder)
    // for every unbound ancestor. Stop at the first bound parent or
    // at the top-level (parent_folder is None).
    let mut chain: Vec<(String, String, Option<String>)> = Vec::new();
    let mut current = rel_str.to_string();
    loop {
        let (leaf_name, parent_folder) =
            decompose_folder_string(&current, ctx.folder_layout, ctx.hierarchy_separator);
        if leaf_name.is_empty() {
            debug!(
                "LocalFolderCreated decomposed to empty leaf name at {}; skipping",
                path.display()
            );
            return;
        }
        chain.push((current.clone(), leaf_name, parent_folder.clone()));
        match parent_folder {
            None => break,
            Some(pf) => {
                if ctx.mailboxes.by_folder(&pf).is_some() {
                    break;
                }
                current = pf;
            }
        }
    }

    // Reverse to root-to-leaf order so the executor processes the
    // parent before the child fires.
    chain.reverse();
    for (folder, leaf, parent_folder) in chain {
        if !emitted.insert(folder.clone()) {
            // A prior LocalFolderCreated in this cycle already
            // queued a Create for this ancestor. Skip the
            // duplicate -- the executor's `creation_refs` will
            // resolve a child's Reference against the first
            // emission.
            continue;
        }
        let parent_ref = match parent_folder {
            None => None,
            Some(pf) => match ctx.mailboxes.by_folder(&pf) {
                Some(binding) => Some(MaybeReference::Value(
                    binding
                        .jmap_mailbox_id
                        .expect_resolved("reconcile::handle_local_folder_created -- parent lookup")
                        .clone(),
                )),
                None => Some(MaybeReference::Reference(pf)),
            },
        };
        plan.actions.push(SyncAction::CreateRemoteMailbox {
            name: leaf,
            parent_jmap_mailbox_id: parent_ref,
            role: None,
            folder,
            replaces_orphan_id: None,
        });
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
        binding,
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

    // scan resolved the binding at the producer boundary; reconcile
    // trusts the typed binding it carried in.
    let folder = binding.maildir_folder.as_str();
    let mailbox_id = binding
        .jmap_mailbox_id
        .expect_resolved("reconcile::handle_local_new -- DB write target")
        .clone();

    // If we can match the local Message-ID against a known server
    // email (DB index), adopt instead of upload. This is the
    // alreadyExists guard.
    if let Some(recs) = ctx.known_by_message_id.get(message_id)
        && let Some(rec) = recs.iter().find(|r| r.jmap_mailbox_id == mailbox_id)
    {
        let keywords =
            serde_json::from_str::<HashMap<String, bool>>(&rec.jmap_keywords).unwrap_or_default();
        let bound_id = BoundId {
            maildir_id: maildir_id.clone(),
            jmap_email_id: rec.jmap_email_id.clone(),
            message_id: message_id.clone(),
        };
        plan.actions.push(SyncAction::AdoptLocalMessage {
            id: bound_id.clone(),
            binding: Arc::clone(binding),
            jmap_blob_id: rec.jmap_blob_id.clone(),
            jmap_thread_id: rec.jmap_thread_id.clone(),
            keywords: keywords.clone(),
            filename_flags: flags.clone(),
            old_maildir_id: None,
            old_jmap_email_id: None,
        });
        // Adoption-time flag reconciliation: the local NewMessage's
        // filename flag suffix may disagree with the DB record's
        // stored server keyword set. Same convergence step
        // try_adopt_remote applies -- ServerWins renames the file,
        // LocalWins pushes an explicit-six patch. Skips silently
        // when filename and server-derived flags already agree.
        emit_adoption_flag_reconciliation(
            plan,
            ctx.policy.conflict_strategy,
            &AdoptionReconciliation {
                bound_id: &bound_id,
                binding,
                server_keywords: &keywords,
                jmap_blob_id: rec.jmap_blob_id.as_ref(),
                jmap_thread_id: rec.jmap_thread_id.as_ref(),
                on_disk_flags: flags,
            },
        );
        return;
    }

    // Local Message-ID exists upstream but the existing DB record is
    // bound to a different mailbox, and there's no matching local
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
            ctx.mailboxes
                .by_id(&other.jmap_mailbox_id)
                .map(|b| b.maildir_folder.as_str())
                .unwrap_or("?"),
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
        binding: Arc::clone(binding),
        file_path: path.to_path_buf(),
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
        filename_flags: new_flags.to_string(),
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
    mailboxes: &MailboxBindings,
) {
    let Some(maildir_id) = &existing.maildir_id else {
        return;
    };
    // The DB row's recorded mailbox is where the file is bound on
    // disk; the file lives under that binding's folder regardless
    // of which mailbox the server-side keyword change came from.
    // The cycle's `mailboxes` table resolves the row's id to its
    // binding so the executor knows the right on-disk path.
    let Some(local_binding) = mailboxes.by_id(&existing.jmap_mailbox_id) else {
        return;
    };
    let new_flags = keywords_to_flags(&email.keywords);
    plan.actions.push(SyncAction::UpdateLocalFlags {
        id: BoundId {
            maildir_id: maildir_id.clone(),
            jmap_email_id: email.id.clone(),
            message_id: existing.message_id.clone(),
        },
        binding: Arc::clone(local_binding),
        new_flags,
        keywords: email.keywords.clone(),
        jmap_blob_id: email.blob_id.clone(),
        jmap_thread_id: email.thread_id.clone(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::JmapMailboxId;
    use crate::jmap::types::{MailboxFolderBinding, MaybeReference};
    use crate::sync::dedupe::{LocalEntry, LocalIndex};
    use std::path::PathBuf;

    fn mailboxes() -> MailboxBindings {
        let mut b = MailboxBindings::builder();
        b.insert(MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
            server_name: "Inbox".to_string(),
            maildir_folder: "INBOX".to_string(),
            remote_path: "INBOX".to_string(),
        });
        b.insert(MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value("MB-ARCH".into()),
            server_name: "Archive".to_string(),
            maildir_folder: "Archive".to_string(),
            remote_path: "Archive".to_string(),
        });
        b.build()
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
            size: 0,
        }
    }

    fn record(
        jmap_id: &str,
        mailbox_id: &str,
        maildir_id: Option<&str>,
        flags: &str,
        message_id: &str,
    ) -> MessageRecord {
        let kw = flags_to_keywords(flags);
        MessageRecord {
            jmap_email_id: jmap_id.into(),
            jmap_blob_id: Some(format!("blob-{jmap_id}").into()),
            jmap_thread_id: Some(format!("thr-{jmap_id}").into()),
            jmap_mailbox_id: mailbox_id.into(),
            maildir_id: maildir_id.map(Into::into),
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
        run_with_local_flags(
            remote_emails,
            remote_destroyed,
            local_changes,
            records,
            local_index,
            &HashMap::new(),
            strategy,
        )
    }

    /// Same as `run`, but threads an explicit on-disk-flags-by-maildir-id
    /// map through to reconcile. Tests that need to exercise adoption-
    /// time filename-truth lookups (or, later, the adoption flag
    /// reconciliation step that builds on it) use this to inject what
    /// production gets from `scan::ScanResult.local_flags`.
    fn run_with_local_flags(
        remote_emails: &[EmailObject],
        remote_destroyed: &[JmapEmailId],
        local_changes: &[LocalChange],
        records: &[MessageRecord],
        local_index: &LocalIndex,
        local_flags: &HashMap<MaildirId, String>,
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
            local_flags,
            mailboxes: &mailboxes,
            policy: SyncPolicy {
                conflict_strategy: strategy,
                allow_destructive_folder_sync: AllowDestructiveFolderSync::None,
            },
            new_email_state: None,
            // Tests pass usize::MAX so the size cap never bites
            // unless a test explicitly opts in to it.
            max_upload_size: usize::MAX,
            // Default the test bench to the initial-pull path so
            // `remote_emails` is treated as the full survivor set;
            // tests that need to exercise the incremental-mode
            // disambiguation set up via `run_incremental` instead.
            used_initial_path: true,
            folder_layout: FolderLayout::Flat,
            hierarchy_separator: '.',
            maildir_root: std::path::Path::new("/tmp/jma-test-maildir-root"),
        })
    }

    /// Same as `run`, but flags this cycle as the incremental path
    /// (`Email/changes` rather than initial pull). Used by tests that
    /// pin Branch A case-iii's incremental-mode behavior: an old
    /// jmap_email_id absent from `remote_emails` is "alive but
    /// unchanged" in this mode, not "destroyed without notification."
    fn run_incremental(
        remote_emails: &[EmailObject],
        remote_destroyed: &[JmapEmailId],
        local_changes: &[LocalChange],
        records: &[MessageRecord],
        local_index: &LocalIndex,
        strategy: ConflictStrategy,
    ) -> SyncPlan {
        let known = indices(records);
        let mailboxes = mailboxes();
        let local_flags: HashMap<MaildirId, String> = HashMap::new();
        reconcile(ReconcileInput {
            remote_emails,
            remote_destroyed,
            local_changes,
            known: &known,
            local_index,
            local_flags: &local_flags,
            mailboxes: &mailboxes,
            policy: SyncPolicy {
                conflict_strategy: strategy,
                allow_destructive_folder_sync: AllowDestructiveFolderSync::None,
            },
            new_email_state: None,
            max_upload_size: usize::MAX,
            used_initial_path: false,
            folder_layout: FolderLayout::Flat,
            hierarchy_separator: '.',
            maildir_root: std::path::Path::new("/tmp/jma-test-maildir-root"),
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
        let rec = record("E1", "MB-INBOX", Some("M-1"), "", "<a@x>");
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
        let rec = record("STALE-ID", "MB-INBOX", Some("MID-1"), "S", "<a@x>");
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

    /// Cold-start initial pull regression: state DB is empty AND scan
    /// emitted a NewMessage for the local file (it's unknown to the
    /// DB), AND the server reports an email with the same Message-ID.
    /// Reconcile must emit exactly one AdoptLocalMessage and NO
    /// UploadMessage. Emitting both -- the failure mode before commit
    /// bdde764 / 8058ed0 (2026-05-01) -- created a duplicate Email
    /// object server-side every time jma ran against an existing
    /// maildir with a fresh DB. Hundreds of such duplicates landed in
    /// real accounts before the guard at handle_local_new (consulting
    /// `adopted_maildir_ids`) suppressed the upload.
    ///
    /// The guard works because process_remote_emails runs before
    /// process_local_changes and populates `adopted_maildir_ids` with
    /// the maildir_ids it adopted from the remote side. By the time
    /// handle_local_new looks at this NewMessage, the maildir_id is
    /// already in the set and the upload is short-circuited.
    #[test]
    fn cold_start_adopts_local_file_without_re_uploading() {
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
            // Scan saw the local file and emitted NewMessage; the DB
            // is empty so it has no prior binding for it.
            &[LocalChange::NewMessage {
                maildir_id: "FILE-1".into(),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
                flags: "S".into(),
                path: PathBuf::from("/tmp/file-1"),
                message_id: "<a@x>".into(),
                size_bytes: 0,
            }],
            // No message_map rows -- cold start.
            &[],
            &idx,
            ConflictStrategy::ServerWins,
        );

        // Exactly one adoption for this file; no upload.
        assert_eq!(
            plan.adopt_count(),
            1,
            "expected one AdoptLocalMessage, got plan: {:?}",
            plan.actions
        );
        assert_eq!(
            plan.upload_count(),
            0,
            "Upload would re-import a message the server already has; \
             this is the cold-start dedup-guard failure mode. Plan: {:?}",
            plan.actions
        );
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
            panic!("expected adopt as first action, got {:?}", plan.actions[0]);
        };
        assert_eq!(maildir_id.as_ref(), "FILE-1");
        assert_eq!(jmap_email_id.as_ref(), "E1");
    }

    /// Cold-start adoption against a maildir file whose on-disk flag
    /// suffix already matches `keywords_to_flags(server)` emits a
    /// bare adoption and nothing else. Pins the equality-skip half of
    /// `emit_adoption_flag_reconciliation` so the steady-state case
    /// stays a pure DB write.
    #[test]
    fn adopt_with_matching_flags_emits_no_reconciliation() {
        let mut idx = empty_index();
        idx.by_message_id.insert(
            "<a@x>".into(),
            vec![LocalEntry {
                folder: "INBOX".into(),
                maildir_id: "FILE-1".into(),
            }],
        );
        let mut local_flags = HashMap::new();
        local_flags.insert(MaildirId::from("FILE-1"), "FS".to_string());
        let plan = run_with_local_flags(
            // Server keywords map to "FS" -- exactly the on-disk suffix.
            &[email("E1", "MB-INBOX", "FS", Some("<a@x>"))],
            &[],
            &[],
            &[],
            &idx,
            &local_flags,
            ConflictStrategy::ServerWins,
        );
        assert_eq!(plan.adopt_count(), 1);
        assert_eq!(
            plan.flag_update_count(),
            0,
            "matching flags must not emit a reconciliation action: {:?}",
            plan.actions
        );
    }

    /// Cold-start adoption: server has $forwarded ("P" in maildir
    /// suffix terms) but the on-disk file is `:2,FS`. Under
    /// `ServerWins`, the adoption must be paired with an
    /// `UpdateLocalFlags` that renames the local file to add the
    /// missing P so the next scan doesn't classify the divergence
    /// as a local FlagsChanged.
    #[test]
    fn adopt_reconciles_filename_vs_server_flag_drift_server_wins() {
        let mut idx = empty_index();
        idx.by_message_id.insert(
            "<a@x>".into(),
            vec![LocalEntry {
                folder: "INBOX".into(),
                maildir_id: "FILE-1".into(),
            }],
        );
        // Server email carries both a standard ($forwarded -> P) and a
        // non-standard ($imported) keyword; the non-standard side must
        // ride through to UpdateLocalFlags.keywords so the eventual
        // jmap_keywords mirror preserves the server's full set.
        let mut server = email("E1", "MB-INBOX", "FPS", Some("<a@x>"));
        server.keywords.insert("$imported".into(), true);
        let mut local_flags = HashMap::new();
        local_flags.insert(MaildirId::from("FILE-1"), "FS".to_string());

        let plan = run_with_local_flags(
            &[server],
            &[],
            &[],
            &[],
            &idx,
            &local_flags,
            ConflictStrategy::ServerWins,
        );
        assert_eq!(plan.adopt_count(), 1);
        let local_updates: Vec<&SyncAction> = plan
            .actions
            .iter()
            .filter(|a| matches!(a, SyncAction::UpdateLocalFlags { .. }))
            .collect();
        assert_eq!(
            local_updates.len(),
            1,
            "expected one UpdateLocalFlags alongside adopt under ServerWins: {:?}",
            plan.actions
        );
        let SyncAction::UpdateLocalFlags {
            id,
            new_flags,
            keywords,
            ..
        } = local_updates[0]
        else {
            unreachable!();
        };
        assert_eq!(id.maildir_id.as_ref(), "FILE-1");
        assert_eq!(id.jmap_email_id.as_ref(), "E1");
        assert_eq!(new_flags, "FPS", "must rename file to match server");
        assert_eq!(keywords.get("$forwarded"), Some(&true));
        assert_eq!(
            keywords.get("$imported"),
            Some(&true),
            "non-standard server keyword must pass through to the action"
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::UpdateRemoteKeywords { .. })),
            "ServerWins must not push remote keywords during adoption: {:?}",
            plan.actions
        );
    }

    /// Same flag-drift scenario as the server-wins test, but with
    /// `LocalWins`. The adoption must be paired with an
    /// `UpdateRemoteKeywords` carrying an explicit-six patch so the
    /// server clears `$forwarded` (and any other standard keyword
    /// the local file doesn't carry). Non-standard keywords like
    /// `$imported` / `$x-me-annot-2` must NOT appear in the patch
    /// -- the per-key wire shape leaves keys we don't mention alone.
    #[test]
    fn adopt_reconciles_filename_vs_server_flag_drift_local_wins() {
        let mut idx = empty_index();
        idx.by_message_id.insert(
            "<a@x>".into(),
            vec![LocalEntry {
                folder: "INBOX".into(),
                maildir_id: "FILE-1".into(),
            }],
        );
        let mut server = email("E1", "MB-INBOX", "FPS", Some("<a@x>"));
        server.keywords.insert("$imported".into(), true);
        let mut local_flags = HashMap::new();
        local_flags.insert(MaildirId::from("FILE-1"), "FS".to_string());

        let plan = run_with_local_flags(
            &[server],
            &[],
            &[],
            &[],
            &idx,
            &local_flags,
            ConflictStrategy::LocalWins,
        );
        assert_eq!(plan.adopt_count(), 1);
        let remote_updates: Vec<&SyncAction> = plan
            .actions
            .iter()
            .filter(|a| matches!(a, SyncAction::UpdateRemoteKeywords { .. }))
            .collect();
        assert_eq!(
            remote_updates.len(),
            1,
            "expected one UpdateRemoteKeywords alongside adopt under LocalWins: {:?}",
            plan.actions
        );
        let SyncAction::UpdateRemoteKeywords {
            id,
            keywords,
            filename_flags,
        } = remote_updates[0]
        else {
            unreachable!();
        };
        assert_eq!(id.jmap_email_id.as_ref(), "E1");
        assert_eq!(
            filename_flags, "FS",
            "filename_flags must carry the on-disk suffix so the mirror writes \
             local_state.flags = filename rather than deriving from the patch"
        );
        // Explicit-six patch: every standard keyword present with the
        // local file's value, $forwarded explicitly false to clear it.
        assert_eq!(keywords.get("$flagged"), Some(&true));
        assert_eq!(keywords.get("$seen"), Some(&true));
        assert_eq!(keywords.get("$forwarded"), Some(&false));
        assert_eq!(keywords.get("$draft"), Some(&false));
        assert_eq!(keywords.get("$answered"), Some(&false));
        assert_eq!(keywords.get("$deleted"), Some(&false));
        assert!(
            !keywords.contains_key("$imported"),
            "patch must leave non-standard server keywords alone: {:?}",
            keywords
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::UpdateLocalFlags { .. })),
            "LocalWins must not rename the local file during adoption: {:?}",
            plan.actions
        );
    }

    /// `handle_local_new`'s alreadyExists adopt path must call the
    /// same reconciliation step `try_adopt_remote` does. A local
    /// NewMessage with Message-ID matching a DB record whose stored
    /// server keywords disagree on the standard six emits both the
    /// adopt and the side-effecting reconciliation action -- under
    /// ServerWins, an UpdateLocalFlags that renames the file to the
    /// server-derived suffix.
    #[test]
    fn already_exists_adopt_reconciles_flag_drift_server_wins() {
        // DB has the record with server keywords including $forwarded
        // (jmap_keywords) and the projected `flags` column set to "FPS".
        let mut keywords_map = HashMap::new();
        keywords_map.insert("$flagged".to_string(), true);
        keywords_map.insert("$seen".to_string(), true);
        keywords_map.insert("$forwarded".to_string(), true);
        let rec = MessageRecord {
            jmap_email_id: "E1".into(),
            jmap_blob_id: Some("B1".into()),
            jmap_thread_id: Some("T1".into()),
            jmap_mailbox_id: "MB-INBOX".into(),
            maildir_id: Some("M-EXISTING".into()),
            message_id: "<a@x>".into(),
            flags: "FPS".into(),
            jmap_keywords: serde_json::to_string(&keywords_map).unwrap(),
        };
        // Local NewMessage with the same Message-ID but `:2,FS` -- no P.
        let new_change = LocalChange::NewMessage {
            maildir_id: "M-NEW".into(),
            binding: Arc::new(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                server_name: "INBOX".to_string(),
                maildir_folder: "INBOX".to_string(),
                remote_path: "INBOX".to_string(),
            }),
            flags: "FS".into(),
            path: PathBuf::from("/tmp/m-new"),
            message_id: "<a@x>".into(),
            size_bytes: 0,
        };
        let plan = run(
            &[],
            &[],
            &[new_change],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        // Adopted (no upload), AND reconciliation emitted UpdateLocalFlags.
        assert_eq!(plan.adopt_count(), 1);
        assert_eq!(plan.upload_count(), 0);
        let local_updates: Vec<&SyncAction> = plan
            .actions
            .iter()
            .filter(|a| matches!(a, SyncAction::UpdateLocalFlags { .. }))
            .collect();
        assert_eq!(
            local_updates.len(),
            1,
            "alreadyExists adopt must reconcile flag drift under ServerWins: {:?}",
            plan.actions
        );
        let SyncAction::UpdateLocalFlags { id, new_flags, .. } = local_updates[0] else {
            unreachable!();
        };
        assert_eq!(id.maildir_id.as_ref(), "M-NEW");
        assert_eq!(
            id.jmap_email_id.as_ref(),
            "E1",
            "reconciliation must share the adopt's BoundId.jmap_email_id"
        );
        assert_eq!(new_flags, "FPS");
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::UpdateRemoteKeywords { .. })),
            "ServerWins must not push remote keywords on the alreadyExists adopt path: {:?}",
            plan.actions
        );
    }

    /// Destroy + re-create with the same wire-format Message-ID:
    /// server destroys Email A and creates Email B sharing the
    /// Message-ID header. Reconcile must coalesce them into a single
    /// rebind: AdoptLocalMessage on B carrying `old_jmap_email_id=
    /// Some(A)` so commit_adopt drops A's row inside the same txn,
    /// and `process_remote_destroys` skips A (no DeleteLocal). Before
    /// this fix execute's adopt-before-delete phase order would
    /// commit the new B row first and trip the unique-on-maildir-id
    /// partial index, rolling the whole txn back and stalling the
    /// cycle indefinitely.
    #[test]
    fn destroy_plus_create_with_shared_message_id_rebinds_atomically() {
        // DB anchors the old email A under maildir_id FILE-1 in INBOX.
        let rec_a = record("A", "MB-INBOX", Some("FILE-1"), "S", "<a@x>");
        // Server destroys A and creates B with the same Message-ID.
        let plan = run(
            &[email("B", "MB-INBOX", "S", Some("<a@x>"))],
            &[JmapEmailId::from("A")],
            &[],
            &[rec_a],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        // Exactly one AdoptLocalMessage, rebinding FILE-1 from A to B.
        let adopts: Vec<&SyncAction> = plan
            .actions
            .iter()
            .filter(|a| matches!(a, SyncAction::AdoptLocalMessage { .. }))
            .collect();
        assert_eq!(
            adopts.len(),
            1,
            "expected one rebind AdoptLocalMessage: {:?}",
            plan.actions
        );
        let SyncAction::AdoptLocalMessage {
            id,
            old_jmap_email_id,
            ..
        } = adopts[0]
        else {
            unreachable!();
        };
        assert_eq!(id.jmap_email_id.as_ref(), "B");
        assert_eq!(id.maildir_id.as_ref(), "FILE-1");
        assert_eq!(
            old_jmap_email_id.as_ref().map(JmapEmailId::as_ref),
            Some("A"),
            "rebind must carry the old jmap_email_id so commit_adopt drops A's row"
        );
        // No DeleteLocal for A -- the rebind consumes the destroy.
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DeleteLocal { .. })),
            "destroy must be consumed by the rebind, not emit DeleteLocal: {:?}",
            plan.actions
        );
    }

    /// Same-Message-ID collision where the server still acknowledges
    /// BOTH ids: DB anchors Email A bound to FILE-1, and this cycle's
    /// remote_emails includes both A (unchanged) and a sibling B
    /// sharing the Message-ID header. remote_destroyed is empty -- A
    /// is alive. Adopting B onto FILE-1 would upsert a second
    /// message_map row with maildir_id=FILE-1, which the partial-
    /// unique index refuses, rolling the cycle txn back. Reconcile
    /// must refuse the adopt and surface the duplicate as a warn so
    /// the user can run `jma janitor remotededupe`. No second
    /// AdoptLocalMessage, no DownloadMessage, no DeleteLocal.
    #[test]
    fn shared_message_id_with_old_id_alive_skips_adoption() {
        let rec_a = record("A", "MB-INBOX", Some("FILE-1"), "S", "<a@x>");
        let plan = run(
            &[
                // A still alive on the server in this cycle's view.
                email("A", "MB-INBOX", "S", Some("<a@x>")),
                // B is the new sibling with the same Message-ID.
                email("B", "MB-INBOX", "S", Some("<a@x>")),
            ],
            // empty remote_destroyed -- A is NOT being destroyed.
            &[],
            &[],
            &[rec_a],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        // No AdoptLocalMessage for B (A's known-by-jmap hit is a
        // no-op flag-only path that emits nothing here).
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::AdoptLocalMessage { .. })),
            "duplicate Message-ID must not produce an adopt for the new id: {:?}",
            plan.actions
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DownloadMessage { .. })),
            "duplicate must not fall through to download a third copy: {:?}",
            plan.actions
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DeleteLocal { .. })),
            "duplicate must not trigger a local delete: {:?}",
            plan.actions
        );
    }

    /// Carry-over-from-stale-DB twin of the duplicate test on the
    /// *initial-pull* path: DB has an old jmap_id STALE-ID bound to
    /// FILE-1, the server reports E1 with the same Message-ID, and
    /// STALE-ID does NOT appear in this cycle's remote_emails. With
    /// `used_initial_path = true`, `remote_emails` is the full
    /// survivor set across synced mailboxes, so absence proves
    /// staleness. The right behavior is a rebind: emit one
    /// AdoptLocalMessage carrying `old_jmap_email_id=Some(STALE-ID)`
    /// so commit_adopt drops the stale row in the same txn. Without
    /// the rebind, commit_adopt's upsert would conflict with the
    /// partial-unique index.
    #[test]
    fn shared_message_id_with_old_id_absent_rebinds_on_initial_path() {
        let rec = record("STALE-ID", "MB-INBOX", Some("FILE-1"), "S", "<a@x>");
        let plan = run(
            // Only E1 in this cycle; STALE-ID is not in remote_emails.
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        let adopts: Vec<&SyncAction> = plan
            .actions
            .iter()
            .filter(|a| matches!(a, SyncAction::AdoptLocalMessage { .. }))
            .collect();
        assert_eq!(adopts.len(), 1, "carry-over case must emit one adopt");
        let SyncAction::AdoptLocalMessage {
            id:
                BoundId {
                    jmap_email_id,
                    maildir_id,
                    ..
                },
            old_jmap_email_id,
            ..
        } = adopts[0]
        else {
            unreachable!();
        };
        assert_eq!(jmap_email_id.as_ref(), "E1");
        assert_eq!(maildir_id.as_ref(), "FILE-1");
        assert_eq!(
            old_jmap_email_id.as_ref().map(JmapEmailId::as_ref),
            Some("STALE-ID"),
            "carry-over rebind must drop the stale row in the same txn"
        );
    }

    /// Incremental-path twin: the same DB anchor (STALE-ID -> FILE-1)
    /// and the same `remote_emails` payload, but this cycle used
    /// `Email/changes` (`used_initial_path = false`). Now absence of
    /// STALE-ID from remote_emails does NOT prove it's gone -- it
    /// could be alive but unchanged. Default to the duplicate-skip
    /// warn so a live duplicate isn't silently rebound away.
    #[test]
    fn shared_message_id_with_old_id_absent_in_incremental_mode_skips_adoption() {
        let rec = record("STALE-ID", "MB-INBOX", Some("FILE-1"), "S", "<a@x>");
        let plan = run_incremental(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[],
            &[rec],
            &empty_index(),
            ConflictStrategy::ServerWins,
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::AdoptLocalMessage { .. })),
            "incremental mode without positive staleness evidence must not \
             rebind blindly: {:?}",
            plan.actions
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DownloadMessage { .. })),
            "skip must not fall through to download: {:?}",
            plan.actions
        );
    }

    /// Cold-start variant: state DB is empty, the local maildir
    /// already has FILE-1 carrying Message-ID <a@x>, and the server
    /// returns *two* Email objects sharing that header. The first
    /// claims FILE-1 via the local_index rescue; the second hits the
    /// same rescue path but `adopted_maildir_ids` already contains
    /// FILE-1, so the second adopt must be refused -- otherwise
    /// commit_adopt would try two message_map upserts with the same
    /// maildir_id and the partial-unique would reject the second.
    #[test]
    fn cold_start_shared_message_id_skips_second_adoption() {
        let mut idx = empty_index();
        idx.by_message_id.insert(
            "<a@x>".into(),
            vec![LocalEntry {
                folder: "INBOX".into(),
                maildir_id: "FILE-1".into(),
            }],
        );
        let plan = run(
            &[
                email("E1", "MB-INBOX", "S", Some("<a@x>")),
                email("E2", "MB-INBOX", "S", Some("<a@x>")),
            ],
            &[],
            &[],
            // No message_map rows -- cold start.
            &[],
            &idx,
            ConflictStrategy::ServerWins,
        );
        // Exactly one adopt: E1 took FILE-1, E2 was refused.
        let adopts: Vec<&SyncAction> = plan
            .actions
            .iter()
            .filter(|a| matches!(a, SyncAction::AdoptLocalMessage { .. }))
            .collect();
        assert_eq!(
            adopts.len(),
            1,
            "cold-start dupe must adopt only the first: {:?}",
            plan.actions
        );
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DownloadMessage { .. })),
            "second duplicate must not fall through to download: {:?}",
            plan.actions
        );
    }

    /// Rebind + flag drift: server destroys A and creates B with the
    /// shared Message-ID, AND on-disk filename suffix disagrees with
    /// `keywords_to_flags(B.keywords)`. The rebind path must reuse
    /// `emit_adoption_flag_reconciliation` -- under ServerWins emit
    /// both the rebind adopt (with `old_jmap_email_id=Some(A)`) and
    /// an UpdateLocalFlags that brings the filename in line with
    /// B's server-derived flags.
    #[test]
    fn rebind_also_reconciles_flag_drift_server_wins() {
        // DB has A bound to FILE-1 in INBOX with flags "S".
        let rec_a = record("A", "MB-INBOX", Some("FILE-1"), "S", "<a@x>");
        // B's server keywords include $forwarded => derived "PS".
        let server = email("B", "MB-INBOX", "PS", Some("<a@x>"));
        // On-disk file is still ":2,S" (no P) -- the divergence.
        let mut local_flags = HashMap::new();
        local_flags.insert(MaildirId::from("FILE-1"), "S".to_string());

        let plan = run_with_local_flags(
            &[server],
            &[JmapEmailId::from("A")],
            &[],
            &[rec_a],
            &empty_index(),
            &local_flags,
            ConflictStrategy::ServerWins,
        );
        // Rebind adopt with old_jmap_email_id=Some(A).
        let SyncAction::AdoptLocalMessage {
            old_jmap_email_id, ..
        } = plan
            .actions
            .iter()
            .find(|a| matches!(a, SyncAction::AdoptLocalMessage { .. }))
            .expect("rebind adopt missing")
        else {
            unreachable!();
        };
        assert_eq!(
            old_jmap_email_id.as_ref().map(JmapEmailId::as_ref),
            Some("A")
        );
        // Plus an UpdateLocalFlags that renames FILE-1 to add P.
        let SyncAction::UpdateLocalFlags { new_flags, .. } = plan
            .actions
            .iter()
            .find(|a| matches!(a, SyncAction::UpdateLocalFlags { .. }))
            .expect("flag reconciliation missing on rebind path")
        else {
            unreachable!();
        };
        assert_eq!(new_flags, "PS");
        // And A is consumed, not deleted.
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, SyncAction::DeleteLocal { .. }))
        );
    }

    /// Known JMAP id, server keywords differ from message_map: emit a
    /// pull-side flag update, no upload.
    #[test]
    fn server_keyword_change_emits_update_local_flags() {
        let rec = record("E1", "MB-INBOX", Some("M-1"), "", "<a@x>");
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
        let rec = record("E1", "MB-INBOX", Some("M-1"), "S", "<a@x>");
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
                from_folder, to_binding, ..
            } if from_folder == "INBOX" && to_binding.maildir_folder == "Archive"
        )));
    }

    /// Local-delete vs server-update with ServerWins: re-download to
    /// restore the file; do not emit DestroyRemote.
    #[test]
    fn delete_vs_update_server_wins_redownloads() {
        let rec = record("E1", "MB-INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[LocalChange::DeletedMessage {
                maildir_id: "M-1".into(),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
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
        let rec = record("E1", "MB-INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[LocalChange::DeletedMessage {
                maildir_id: "M-1".into(),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
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
        let rec = record("E1", "MB-INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[LocalChange::FlagsChanged {
                maildir_id: "M-1".into(),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
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
        let rec = record("E1", "MB-INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[LocalChange::FlagsChanged {
                maildir_id: "M-1".into(),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
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

    /// Regression pin for the `local_state.flags` invariant the
    /// `apply_remote_set` mirror relies on: every UpdateRemoteKeywords
    /// emit site must carry `filename_flags = on-disk filename
    /// suffix`. Without this, an additive patch (the shape
    /// `handle_local_flags` produces) lets the mirror derive
    /// `local_state.flags` from the merged-server view -- which
    /// after the keyword-merge fix includes server keywords the
    /// patch didn't touch, e.g. a still-set $flagged that the user
    /// removed locally. Next scan would then see `local_state.flags
    /// != entry.flags` and resurrect the phantom-FlagsChanged loop
    /// that the column-semantics split was written to eliminate.
    #[test]
    fn handle_local_flags_carries_filename_flags_for_mirror() {
        // DB has the message with $flagged + $seen (server view).
        let rec = record("E1", "MB-INBOX", Some("M-1"), "FS", "<a@x>");
        // User removed $flagged: on-disk filename is now ":2,S".
        let plan = run(
            &[email("E1", "MB-INBOX", "FS", Some("<a@x>"))],
            &[],
            &[LocalChange::FlagsChanged {
                maildir_id: "M-1".into(),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
                old_flags: "FS".into(),
                new_flags: "S".into(),
            }],
            &[rec],
            &empty_index(),
            ConflictStrategy::LocalWins,
        );
        let SyncAction::UpdateRemoteKeywords { filename_flags, .. } = plan
            .actions
            .iter()
            .find(|a| matches!(a, SyncAction::UpdateRemoteKeywords { .. }))
            .expect("handle_local_flags must emit UpdateRemoteKeywords")
        else {
            unreachable!();
        };
        assert_eq!(
            filename_flags, "S",
            "filename_flags must carry the on-disk suffix so the mirror's \
             local_state.flags write stays consistent with the filesystem"
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
            binding: Arc::new(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                server_name: "INBOX".to_string(),
                maildir_folder: "INBOX".to_string(),
                remote_path: "INBOX".to_string(),
            }),
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
            local_flags: &HashMap::new(),
            mailboxes: &mailboxes,
            policy: SyncPolicy {
                conflict_strategy: ConflictStrategy::ServerWins,
                allow_destructive_folder_sync: AllowDestructiveFolderSync::None,
            },
            new_email_state: None,
            max_upload_size: 1_000,
            used_initial_path: true,
            folder_layout: FolderLayout::Flat,
            hierarchy_separator: '.',
            maildir_root: std::path::Path::new("/tmp/jma-test-maildir-root"),
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
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
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
        let rec = record("E1", "MB-ARCH", Some("M-EXISTING"), "", "<a@x>");
        let plan = run(
            &[],
            &[],
            &[LocalChange::NewMessage {
                maildir_id: "M-DUP".into(),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
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
        let rec = record("E1", "MB-INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[],
            &[],
            &[LocalChange::DeletedMessage {
                maildir_id: "M-1".into(),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
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
        let rec = record("E1", "MB-INBOX", Some("M-1"), "", "<a@x>");
        let plan = run(
            &[],
            &["E1".into()],
            &[LocalChange::DeletedMessage {
                maildir_id: "M-1".into(),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
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
        let rec = record("E1", "MB-INBOX", Some("M-OLD"), "", "<a@x>");
        let plan = run(
            &[],
            &[],
            &[
                LocalChange::DeletedMessage {
                    maildir_id: "M-OLD".into(),
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                        server_name: "INBOX".to_string(),
                        maildir_folder: "INBOX".to_string(),
                        remote_path: "INBOX".to_string(),
                    }),
                },
                LocalChange::NewMessage {
                    maildir_id: "M-NEW".into(),
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value("MB-ARCH".into()),
                        server_name: "Archive".to_string(),
                        maildir_folder: "Archive".to_string(),
                        remote_path: "Archive".to_string(),
                    }),
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
        let rec = record("E1", "MB-INBOX", Some("M-OLD"), "", "<a@x>");
        let plan = run(
            &[],
            &[],
            &[
                LocalChange::DeletedMessage {
                    maildir_id: "M-OLD".into(),
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                        server_name: "INBOX".to_string(),
                        maildir_folder: "INBOX".to_string(),
                        remote_path: "INBOX".to_string(),
                    }),
                },
                LocalChange::NewMessage {
                    maildir_id: "M-NEW".into(),
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value("MB-ARCH".into()),
                        server_name: "Archive".to_string(),
                        maildir_folder: "Archive".to_string(),
                        remote_path: "Archive".to_string(),
                    }),
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
        let rec = record("E1", "MB-INBOX", Some("M1"), "FS", "<a@x>");
        let plan = run(
            &[],
            &[],
            &[
                LocalChange::DeletedMessage {
                    maildir_id: "M1".into(),
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                        server_name: "INBOX".to_string(),
                        maildir_folder: "INBOX".to_string(),
                        remote_path: "INBOX".to_string(),
                    }),
                },
                LocalChange::NewMessage {
                    // Same id as the deleted side -- id-preserving move.
                    maildir_id: "M1".into(),
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value("MB-ARCH".into()),
                        server_name: "Archive".to_string(),
                        maildir_folder: "Archive".to_string(),
                        remote_path: "Archive".to_string(),
                    }),
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
                    binding,
                    old_maildir_id: Some(old),
                    ..
                } if maildir_id.as_ref() == "M1" && old.as_ref() == "M1" && binding.maildir_folder == "Archive"
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
        let rec = record("E1", "MB-INBOX", Some("M-OLD"), "", "<a@x>");
        let plan = run(
            // Server's view still has the email in INBOX (the local
            // move hasn't been pushed yet).
            &[email("E1", "MB-INBOX", "S", Some("<a@x>"))],
            &[],
            &[
                LocalChange::DeletedMessage {
                    maildir_id: "M-OLD".into(),
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                        server_name: "INBOX".to_string(),
                        maildir_folder: "INBOX".to_string(),
                        remote_path: "INBOX".to_string(),
                    }),
                },
                LocalChange::NewMessage {
                    maildir_id: "M-NEW".into(),
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value("MB-ARCH".into()),
                        server_name: "Archive".to_string(),
                        maildir_folder: "Archive".to_string(),
                        remote_path: "Archive".to_string(),
                    }),
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
        let rec = record("E1", "MB-INBOX", Some("M-1"), "", "<a@x>");
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

    /// Empty `new_mailboxes()` on bindings: reconcile emits no
    /// `CreateLocalMailbox` actions. Pins steady-state behavior
    /// where every mailbox already has a `mailbox_map` row from a
    /// prior cycle.
    #[test]
    fn emit_create_local_mailboxes_no_op_in_steady_state() {
        let mailboxes = mailboxes();
        let mut plan = SyncPlan::default();
        emit_create_local_mailboxes(&mailboxes, &mut plan);
        assert_eq!(plan.folder_create_count(), 0);
    }

    /// New-mailbox bindings produce one `CreateLocalMailbox` each
    /// with the binding + parent jmap mailbox id carried through.
    #[test]
    fn emit_create_local_mailboxes_emits_one_per_new_binding() {
        let mut mailboxes = MailboxBindings::builder();
        let inbox = MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
            server_name: "Inbox".to_string(),
            maildir_folder: "INBOX".to_string(),
            remote_path: "Inbox".to_string(),
        };
        let child = MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value("MB-CHILD".into()),
            server_name: "Notes".to_string(),
            maildir_folder: "Notes".to_string(),
            remote_path: "Inbox/Notes".to_string(),
        };
        mailboxes.insert(inbox.clone());
        mailboxes.insert(child.clone());
        mailboxes.push_new_mailbox(inbox.clone(), None, None);
        mailboxes.push_new_mailbox(child.clone(), Some("MB-INBOX".into()), None);
        let mailboxes = mailboxes.build();
        let mut plan = SyncPlan::default();
        emit_create_local_mailboxes(&mailboxes, &mut plan);

        assert_eq!(plan.folder_create_count(), 2);
        let mut seen_ids: Vec<String> = Vec::new();
        for a in &plan.actions {
            if let SyncAction::CreateLocalMailbox {
                binding,
                parent_jmap_mailbox_id,
                ..
            } = a
            {
                let id = binding
                    .jmap_mailbox_id
                    .expect_resolved("test -- CreateLocalMailbox binding id");
                seen_ids.push(id.as_ref().to_string());
                if id.as_ref() == "MB-CHILD" {
                    assert_eq!(
                        parent_jmap_mailbox_id.as_ref().map(|p| p.as_ref()),
                        Some("MB-INBOX"),
                        "child parent id must round-trip into the action"
                    );
                }
            }
        }
        seen_ids.sort();
        assert_eq!(seen_ids, vec!["MB-CHILD", "MB-INBOX"]);
    }

    /// Run reconcile through a custom (folder_layout, separator,
    /// maildir_root) so the LocalFolderCreated emit path can exercise
    /// per-layout decomposition. Keeps every other input minimal so
    /// the only thing in the plan is what the LocalFolderCreated arm
    /// produced.
    fn run_with_layout(
        local_changes: &[LocalChange],
        mailboxes: &MailboxBindings,
        folder_layout: FolderLayout,
        hierarchy_separator: char,
        maildir_root: &Path,
    ) -> SyncPlan {
        let local_index = LocalIndex::default();
        let local_flags = HashMap::new();
        let known = indices(&[]);
        reconcile(ReconcileInput {
            remote_emails: &[],
            remote_destroyed: &[],
            local_changes,
            known: &known,
            local_index: &local_index,
            local_flags: &local_flags,
            mailboxes,
            policy: SyncPolicy {
                conflict_strategy: ConflictStrategy::ServerWins,
                allow_destructive_folder_sync: AllowDestructiveFolderSync::None,
            },
            new_email_state: None,
            max_upload_size: usize::MAX,
            used_initial_path: true,
            folder_layout,
            hierarchy_separator,
            maildir_root,
        })
    }

    /// LocalFolderCreated with `sentinel: None` and no parent
    /// (top-level Flat folder) emits one `CreateRemoteMailbox` with
    /// the folder's leaf name, no parent, and no role.
    #[test]
    fn local_folder_created_flat_top_level_emits_create_remote_mailbox() {
        let mailboxes = mailboxes();
        let root = PathBuf::from("/tmp/jma-test-root");
        let folder = root.join("Projects");
        let local_changes = vec![LocalChange::LocalFolderCreated {
            path: folder,
            sentinel: None,
        }];
        let plan = run_with_layout(&local_changes, &mailboxes, FolderLayout::Flat, '.', &root);
        assert_eq!(plan.remote_folder_create_count(), 1);
        match &plan.actions[0] {
            SyncAction::CreateRemoteMailbox {
                name,
                parent_jmap_mailbox_id,
                role,
                ..
            } => {
                assert_eq!(name, "Projects");
                assert!(parent_jmap_mailbox_id.is_none());
                assert!(role.is_none());
            }
            other => panic!("expected CreateRemoteMailbox, got {:?}", other),
        }
    }

    /// LocalFolderCreated under Flat with a separator-joined name
    /// decomposes into (leaf, parent), looks up the parent folder
    /// against bindings, and emits CreateRemoteMailbox with the
    /// resolved parent id.
    #[test]
    fn local_folder_created_flat_with_bound_parent_emits_with_parent_id() {
        let mut mailboxes = MailboxBindings::builder();
        mailboxes.insert(MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value("MB-PARENT".into()),
            server_name: "Parent".to_string(),
            maildir_folder: "Parent".to_string(),
            remote_path: "Parent".to_string(),
        });
        let mailboxes = mailboxes.build();
        let root = PathBuf::from("/tmp/jma-test-root");
        let folder = root.join("Parent.Child");
        let local_changes = vec![LocalChange::LocalFolderCreated {
            path: folder,
            sentinel: None,
        }];
        let plan = run_with_layout(&local_changes, &mailboxes, FolderLayout::Flat, '.', &root);
        assert_eq!(plan.remote_folder_create_count(), 1);
        match &plan.actions[0] {
            SyncAction::CreateRemoteMailbox {
                name,
                parent_jmap_mailbox_id,
                ..
            } => {
                assert_eq!(name, "Child");
                let parent_id = parent_jmap_mailbox_id
                    .as_ref()
                    .expect("parent must be present")
                    .expect_resolved("test -- parent must be resolved");
                assert_eq!(parent_id.as_ref(), "MB-PARENT");
            }
            other => panic!("expected CreateRemoteMailbox, got {:?}", other),
        }
    }

    /// Minimum chain: `Parent.Child` under Flat with neither
    /// Parent nor Child bound emits two CreateRemoteMailbox
    /// actions -- Parent (top-level) and Child (parent=
    /// Reference("Parent")). The executor's creation_refs table
    /// will resolve the Reference once Parent's create returns.
    #[test]
    fn local_folder_created_flat_emits_two_creates_for_one_missing_parent() {
        let mailboxes = MailboxBindings::builder().build();
        let root = PathBuf::from("/tmp/jma-test-root");
        let folder = root.join("Parent.Child");
        let local_changes = vec![LocalChange::LocalFolderCreated {
            path: folder,
            sentinel: None,
        }];
        let plan = run_with_layout(&local_changes, &mailboxes, FolderLayout::Flat, '.', &root);
        assert_eq!(plan.remote_folder_create_count(), 2);
        match &plan.actions[0] {
            SyncAction::CreateRemoteMailbox {
                name,
                parent_jmap_mailbox_id,
                ..
            } => {
                assert_eq!(name, "Parent");
                assert!(parent_jmap_mailbox_id.is_none());
            }
            other => panic!("expected CreateRemoteMailbox at [0], got {:?}", other),
        }
        match &plan.actions[1] {
            SyncAction::CreateRemoteMailbox {
                name,
                parent_jmap_mailbox_id,
                ..
            } => {
                assert_eq!(name, "Child");
                match parent_jmap_mailbox_id {
                    Some(MaybeReference::Reference(s)) => assert_eq!(s, "Parent"),
                    other => panic!("expected Reference(\"Parent\"), got {:?}", other),
                }
            }
            other => panic!("expected CreateRemoteMailbox at [1], got {:?}", other),
        }
    }

    /// LocalFolderCreated with `sentinel: Some(...)` is rebindfolders'
    /// territory (cache lost a binding the disk still pins). Reconcile
    /// must skip rather than emit a duplicate-creating
    /// CreateRemoteMailbox.
    #[test]
    fn local_folder_created_with_sentinel_skips_emission() {
        let mailboxes = mailboxes();
        let root = PathBuf::from("/tmp/jma-test-root");
        let folder = root.join("Projects");
        let local_changes = vec![LocalChange::LocalFolderCreated {
            path: folder,
            sentinel: Some(crate::maildir_ops::sentinel::MailboxMapping {
                jmap_mailbox_id: JmapMailboxId::from("MB-LOST"),
                parent_jmap_mailbox_id: None,
                server_name: "Projects".to_string(),
            }),
        }];
        let plan = run_with_layout(&local_changes, &mailboxes, FolderLayout::Flat, '.', &root);
        assert_eq!(plan.remote_folder_create_count(), 0);
    }

    /// Under MaildirPP, a dot-prefixed nested folder decomposes
    /// correctly: `.Parent.Child` -> name=Child, parent=".Parent".
    /// With `.Parent` bound, CreateRemoteMailbox carries the
    /// resolved parent id.
    #[test]
    fn local_folder_created_maildir_pp_decomposes_dot_prefixed_nested() {
        let mut mailboxes = MailboxBindings::builder();
        mailboxes.insert(MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value("MB-PARENT".into()),
            server_name: "Parent".to_string(),
            maildir_folder: ".Parent".to_string(),
            remote_path: "Parent".to_string(),
        });
        let mailboxes = mailboxes.build();
        let root = PathBuf::from("/tmp/jma-test-root");
        let folder = root.join(".Parent.Child");
        let local_changes = vec![LocalChange::LocalFolderCreated {
            path: folder,
            sentinel: None,
        }];
        let plan = run_with_layout(
            &local_changes,
            &mailboxes,
            FolderLayout::MaildirPP,
            '.',
            &root,
        );
        assert_eq!(plan.remote_folder_create_count(), 1);
        match &plan.actions[0] {
            SyncAction::CreateRemoteMailbox {
                name,
                parent_jmap_mailbox_id,
                ..
            } => {
                assert_eq!(name, "Child");
                let parent_id = parent_jmap_mailbox_id
                    .as_ref()
                    .expect("parent must be present")
                    .expect_resolved("test -- parent must be resolved");
                assert_eq!(parent_id.as_ref(), "MB-PARENT");
            }
            other => panic!("expected CreateRemoteMailbox, got {:?}", other),
        }
    }

    /// Two LocalFolderCreated events that share an ancestor under
    /// Flat (`Foo.Alpha` and `Foo.Beta`, neither bound, `Foo` also
    /// unbound) must dedupe the shared `Foo` create -- the chain
    /// emitter would naturally walk it twice, but the executor
    /// only needs (and should only issue) one Mailbox/set { create }
    /// for `Foo`.
    #[test]
    fn local_folder_created_dedupes_shared_ancestor_across_chains() {
        let mailboxes = MailboxBindings::builder().build();
        let root = PathBuf::from("/tmp/jma-test-root");
        let local_changes = vec![
            LocalChange::LocalFolderCreated {
                path: root.join("Foo.Alpha"),
                sentinel: None,
            },
            LocalChange::LocalFolderCreated {
                path: root.join("Foo.Beta"),
                sentinel: None,
            },
        ];
        let plan = run_with_layout(&local_changes, &mailboxes, FolderLayout::Flat, '.', &root);
        // Expect three creates: Foo (once), Foo.Alpha, Foo.Beta.
        assert_eq!(
            plan.remote_folder_create_count(),
            3,
            "Foo must dedupe across both chains, got {:?}",
            plan.actions
        );
        let folders: Vec<String> = plan
            .actions
            .iter()
            .filter_map(|a| match a {
                SyncAction::CreateRemoteMailbox { folder, .. } => Some(folder.clone()),
                _ => None,
            })
            .collect();
        assert!(folders.contains(&"Foo".to_string()));
        assert!(folders.contains(&"Foo.Alpha".to_string()));
        assert!(folders.contains(&"Foo.Beta".to_string()));
        let foo_count = folders.iter().filter(|f| *f == "Foo").count();
        assert_eq!(
            foo_count, 1,
            "Foo must appear exactly once, got {folders:?}"
        );
    }

    /// Under Flat with separator `.`, a user-created `Foo.Bar.Baz`
    /// where neither `Foo` nor `Foo.Bar` exist server-side must
    /// emit a chain of three CreateRemoteMailbox actions, top-down:
    /// `Foo` (top-level), `Foo.Bar` (parent=Reference("Foo")), and
    /// `Foo.Bar.Baz` (parent=Reference("Foo.Bar")). The executor's
    /// creation_refs table resolves the Reference parents in order
    /// as each parent's create returns.
    #[test]
    fn local_folder_created_flat_emits_chained_creates_for_missing_parents() {
        let mailboxes = MailboxBindings::builder().build();
        let root = PathBuf::from("/tmp/jma-test-root");
        let folder = root.join("Foo.Bar.Baz");
        let local_changes = vec![LocalChange::LocalFolderCreated {
            path: folder,
            sentinel: None,
        }];
        let plan = run_with_layout(&local_changes, &mailboxes, FolderLayout::Flat, '.', &root);
        assert_eq!(
            plan.remote_folder_create_count(),
            3,
            "expected three chained creates"
        );

        // Top-down order: Foo (top-level), then Foo.Bar (refers to
        // Foo), then Foo.Bar.Baz (refers to Foo.Bar).
        match &plan.actions[0] {
            SyncAction::CreateRemoteMailbox {
                name,
                parent_jmap_mailbox_id,
                folder,
                ..
            } => {
                assert_eq!(name, "Foo");
                assert!(parent_jmap_mailbox_id.is_none(), "top-level has no parent");
                assert_eq!(folder, "Foo");
            }
            other => panic!("expected CreateRemoteMailbox at [0], got {:?}", other),
        }
        match &plan.actions[1] {
            SyncAction::CreateRemoteMailbox {
                name,
                parent_jmap_mailbox_id,
                folder,
                ..
            } => {
                assert_eq!(name, "Bar");
                match parent_jmap_mailbox_id {
                    Some(MaybeReference::Reference(s)) => assert_eq!(s, "Foo"),
                    other => panic!("expected Reference(\"Foo\"), got {:?}", other),
                }
                assert_eq!(folder, "Foo.Bar");
            }
            other => panic!("expected CreateRemoteMailbox at [1], got {:?}", other),
        }
        match &plan.actions[2] {
            SyncAction::CreateRemoteMailbox {
                name,
                parent_jmap_mailbox_id,
                folder,
                ..
            } => {
                assert_eq!(name, "Baz");
                match parent_jmap_mailbox_id {
                    Some(MaybeReference::Reference(s)) => assert_eq!(s, "Foo.Bar"),
                    other => panic!("expected Reference(\"Foo.Bar\"), got {:?}", other),
                }
                assert_eq!(folder, "Foo.Bar.Baz");
            }
            other => panic!("expected CreateRemoteMailbox at [2], got {:?}", other),
        }
    }

    /// Under Fs, `Parent/Child` decomposes on `/` regardless of
    /// the configured separator. With `Parent` bound, the parent
    /// id resolves.
    #[test]
    fn local_folder_created_fs_decomposes_slash_nested() {
        let mut mailboxes = MailboxBindings::builder();
        mailboxes.insert(MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value("MB-ARCH".into()),
            server_name: "Archive".to_string(),
            maildir_folder: "Archive".to_string(),
            remote_path: "Archive".to_string(),
        });
        let mailboxes = mailboxes.build();
        let root = PathBuf::from("/tmp/jma-test-root");
        let folder = root.join("Archive").join("2024");
        let local_changes = vec![LocalChange::LocalFolderCreated {
            path: folder,
            sentinel: None,
        }];
        let plan = run_with_layout(&local_changes, &mailboxes, FolderLayout::Fs, '.', &root);
        assert_eq!(plan.remote_folder_create_count(), 1);
        match &plan.actions[0] {
            SyncAction::CreateRemoteMailbox {
                name,
                parent_jmap_mailbox_id,
                ..
            } => {
                assert_eq!(name, "2024");
                let parent_id = parent_jmap_mailbox_id
                    .as_ref()
                    .expect("parent must be present")
                    .expect_resolved("test -- parent must be resolved");
                assert_eq!(parent_id.as_ref(), "MB-ARCH");
            }
            other => panic!("expected CreateRemoteMailbox, got {:?}", other),
        }
    }

    mod orphan_resurrect {
        use super::*;
        use crate::sync::bindings::OrphanMessage;

        fn bindings_with_orphan(messages: Vec<OrphanMessage>) -> MailboxBindings {
            let mut b = MailboxBindings::builder();
            b.push_local_orphan(
                MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value(JmapMailboxId::from("MB-ARCH")),
                    server_name: "Archive".to_string(),
                    maildir_folder: "Archive".to_string(),
                    remote_path: "Archive".to_string(),
                },
                None,
            );
            let captured = messages.clone();
            b.populate_local_orphan_messages(|_| Ok(captured.clone()))
                .unwrap();
            b.build()
        }

        fn orphan_msg(
            jmap_email_id: Option<&str>,
            maildir_id: &str,
            message_id: &str,
        ) -> OrphanMessage {
            OrphanMessage {
                jmap_email_id: jmap_email_id.map(JmapEmailId::from),
                maildir_id: MaildirId::from(maildir_id),
                message_id: MessageId::from(message_id),
                path: std::path::PathBuf::from(format!("/tmp/jma-test/Archive/cur/{}", maildir_id)),
                flags: "S".to_string(),
                size_bytes: 100,
            }
        }

        fn run_with_orphan(
            mailboxes: &MailboxBindings,
            allow_destructive: AllowDestructiveFolderSync,
        ) -> SyncPlan {
            let local_index = LocalIndex::default();
            let local_flags = HashMap::new();
            let known = indices(&[]);
            reconcile(ReconcileInput {
                remote_emails: &[],
                remote_destroyed: &[],
                local_changes: &[],
                known: &known,
                local_index: &local_index,
                local_flags: &local_flags,
                mailboxes,
                policy: SyncPolicy {
                    conflict_strategy: ConflictStrategy::ServerWins,
                    allow_destructive_folder_sync: allow_destructive,
                },
                new_email_state: None,
                max_upload_size: usize::MAX,
                used_initial_path: true,
                folder_layout: FolderLayout::Flat,
                hierarchy_separator: '.',
                maildir_root: std::path::Path::new("/tmp/jma-test-maildir-root"),
            })
        }

        /// Under the default policy (`None`, non-destructive),
        /// a local orphan with both bound and local-only
        /// messages emits one `CreateRemoteMailbox` (with
        /// `replaces_orphan_id = Some(MB-ARCH)`) plus one
        /// `UploadMessage` per cataloged message. Each upload's
        /// `binding.jmap_mailbox_id` is a `Reference` keyed on
        /// the orphan's `maildir_folder` -- the executor's
        /// `creation_refs` table resolves it after the create
        /// lands.
        #[test]
        fn local_orphan_under_none_policy_emits_resurrect() {
            let bindings = bindings_with_orphan(vec![
                orphan_msg(Some("E1"), "FOO.host", "<bound-1@example.com>"),
                orphan_msg(None, "BAR.host", "<local-only-1@example.com>"),
            ]);

            let plan = run_with_orphan(&bindings, AllowDestructiveFolderSync::None);

            let creates: Vec<_> = plan
                .actions
                .iter()
                .filter_map(|a| match a {
                    SyncAction::CreateRemoteMailbox {
                        name,
                        folder,
                        replaces_orphan_id,
                        ..
                    } => Some((name.clone(), folder.clone(), replaces_orphan_id.clone())),
                    _ => None,
                })
                .collect();
            assert_eq!(creates.len(), 1, "one CreateRemoteMailbox for the orphan");
            assert_eq!(creates[0].0, "Archive");
            assert_eq!(creates[0].1, "Archive");
            assert_eq!(
                creates[0].2.as_ref().map(|id| id.as_ref()),
                Some("MB-ARCH"),
                "replaces_orphan_id carries the dead mailbox id"
            );

            let uploads: Vec<_> = plan
                .actions
                .iter()
                .filter_map(|a| match a {
                    SyncAction::UploadMessage { id, binding, .. } => {
                        Some((id.maildir_id.as_ref().to_string(), binding.clone()))
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(uploads.len(), 2, "one UploadMessage per orphan message");
            for (_, binding) in &uploads {
                match &binding.jmap_mailbox_id {
                    MaybeReference::Reference(folder) => assert_eq!(folder, "Archive"),
                    other => panic!("expected Reference(Archive), got {:?}", other),
                }
            }
        }

        /// Under `delete-local` (the user opted in to
        /// destructive folder sync on the pull side), the
        /// policy permits destroying the local side, so the
        /// resurrect path must not fire. Pins the policy gate.
        #[test]
        fn local_orphan_under_delete_local_policy_skips_resurrect() {
            let bindings = bindings_with_orphan(vec![orphan_msg(
                Some("E1"),
                "FOO.host",
                "<bound-1@example.com>",
            )]);

            let plan = run_with_orphan(&bindings, AllowDestructiveFolderSync::DeleteLocal);

            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::CreateRemoteMailbox { .. })),
                "no CreateRemoteMailbox should be emitted under delete-local; got {:?}",
                plan.actions
            );
            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::UploadMessage { .. })),
                "no UploadMessage should be emitted under delete-local"
            );
        }

        /// Symmetrically, `Both` (delete-local + delete-remote)
        /// also opts in to destructive local handling and
        /// must skip resurrect.
        #[test]
        fn local_orphan_under_both_policy_skips_resurrect() {
            let bindings = bindings_with_orphan(vec![orphan_msg(
                Some("E1"),
                "FOO.host",
                "<bound-1@example.com>",
            )]);

            let plan = run_with_orphan(&bindings, AllowDestructiveFolderSync::Both);

            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::CreateRemoteMailbox { .. })),
                "no CreateRemoteMailbox under Both"
            );
            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::UploadMessage { .. })),
                "no UploadMessage under Both"
            );
        }

        /// `delete-remote` permits destructive push-side
        /// handling but NOT pull-side -- so local orphans
        /// (server deleted the mailbox) still resurrect.
        #[test]
        fn local_orphan_under_delete_remote_policy_still_resurrects() {
            let bindings = bindings_with_orphan(vec![orphan_msg(
                Some("E1"),
                "FOO.host",
                "<bound-1@example.com>",
            )]);

            let plan = run_with_orphan(&bindings, AllowDestructiveFolderSync::DeleteRemote);

            let creates_count = plan
                .actions
                .iter()
                .filter(|a| matches!(a, SyncAction::CreateRemoteMailbox { .. }))
                .count();
            assert_eq!(
                creates_count, 1,
                "delete-remote doesn't permit destroying the local side, \
                 so local-orphan resurrect still fires"
            );
        }
    }

    mod orphan_destructive {
        use super::*;
        use crate::sync::bindings::OrphanMessage;

        fn bindings_with_orphan(messages: Vec<OrphanMessage>) -> MailboxBindings {
            let mut b = MailboxBindings::builder();
            b.insert(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Value(JmapMailboxId::from("MB-INBOX")),
                server_name: "INBOX".to_string(),
                maildir_folder: "INBOX".to_string(),
                remote_path: "INBOX".to_string(),
            });
            b.push_local_orphan(
                MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value(JmapMailboxId::from("MB-ARCH")),
                    server_name: "Archive".to_string(),
                    maildir_folder: "Archive".to_string(),
                    remote_path: "Archive".to_string(),
                },
                None,
            );
            let captured = messages.clone();
            b.populate_local_orphan_messages(|_| Ok(captured.clone()))
                .unwrap();
            b.build()
        }

        fn orphan_msg(jmap_email_id: Option<&str>, maildir_id: &str) -> OrphanMessage {
            OrphanMessage {
                jmap_email_id: jmap_email_id.map(JmapEmailId::from),
                maildir_id: MaildirId::from(maildir_id),
                message_id: MessageId::from(format!("{}@example.com", maildir_id)),
                path: std::path::PathBuf::from(format!("/tmp/jma-test/Archive/cur/{}", maildir_id)),
                flags: "S".to_string(),
                size_bytes: 100,
            }
        }

        fn record_for(jmap_email_id: &str, mailbox_id: &str, maildir_id: &str) -> MessageRecord {
            MessageRecord {
                jmap_email_id: jmap_email_id.into(),
                jmap_blob_id: Some(format!("B-{}", jmap_email_id).into()),
                jmap_thread_id: Some(format!("T-{}", jmap_email_id).into()),
                jmap_mailbox_id: mailbox_id.into(),
                maildir_id: Some(maildir_id.into()),
                message_id: format!("{}@example.com", jmap_email_id).into(),
                flags: "S".into(),
                jmap_keywords: "{}".into(),
            }
        }

        fn run_destructive(
            mailboxes: &MailboxBindings,
            local_changes: &[LocalChange],
            allow_destructive: AllowDestructiveFolderSync,
            strategy: ConflictStrategy,
            records: &[MessageRecord],
        ) -> SyncPlan {
            let local_index = LocalIndex::default();
            let local_flags = HashMap::new();
            let known = indices(records);
            reconcile(ReconcileInput {
                remote_emails: &[],
                remote_destroyed: &[],
                local_changes,
                known: &known,
                local_index: &local_index,
                local_flags: &local_flags,
                mailboxes,
                policy: SyncPolicy {
                    conflict_strategy: strategy,
                    allow_destructive_folder_sync: allow_destructive,
                },
                new_email_state: None,
                max_upload_size: usize::MAX,
                used_initial_path: true,
                folder_layout: FolderLayout::Flat,
                hierarchy_separator: '.',
                maildir_root: std::path::Path::new("/tmp/jma-test-maildir-root"),
            })
        }

        /// Under `delete-local` + no concurrent user action
        /// targeting the orphan, the destructive arm emits a
        /// single `DeleteLocalFolder` for the orphan. No
        /// counter-actions, no resurrect.
        #[test]
        fn destroys_under_delete_local_no_blocker() {
            let bindings = bindings_with_orphan(vec![orphan_msg(Some("E1"), "FOO.host")]);
            let plan = run_destructive(
                &bindings,
                &[],
                AllowDestructiveFolderSync::DeleteLocal,
                ConflictStrategy::ServerWins,
                &[],
            );

            let destroys: Vec<_> = plan
                .actions
                .iter()
                .filter_map(|a| match a {
                    SyncAction::DeleteLocalFolder { binding } => Some(binding.clone()),
                    _ => None,
                })
                .collect();
            assert_eq!(destroys.len(), 1, "one DeleteLocalFolder for the orphan");
            assert_eq!(destroys[0].maildir_folder, "Archive");
            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::CreateRemoteMailbox { .. })),
                "no resurrect under destructive-no-Blocker"
            );
        }

        /// `DestroyRemote` on an email bound to the orphan is
        /// Moot -- the orphan-destroy subsumes it. The Moot
        /// action gets dropped from the plan; the folder
        /// destroy proceeds.
        #[test]
        fn destroyremote_against_orphan_email_is_moot() {
            let bindings = bindings_with_orphan(vec![]);
            // Pre-existing message_map row: email E1 bound to
            // orphan MB-ARCH.
            let records = vec![record_for("E1", "MB-ARCH", "FOO.host")];
            // LocalChange::DeletedMessage on that maildir_id
            // -- process_local_changes turns this into
            // DestroyRemote.
            let local_changes = vec![LocalChange::DeletedMessage {
                maildir_id: MaildirId::from("FOO.host"),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value(JmapMailboxId::from("MB-ARCH")),
                    server_name: "Archive".to_string(),
                    maildir_folder: "Archive".to_string(),
                    remote_path: "Archive".to_string(),
                }),
            }];

            let plan = run_destructive(
                &bindings,
                &local_changes,
                AllowDestructiveFolderSync::DeleteLocal,
                ConflictStrategy::ServerWins,
                &records,
            );

            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::DestroyRemote { .. })),
                "DestroyRemote against orphan-bound email must be Moot-dropped; \
                 got plan: {:?}",
                plan.actions
            );
            assert!(
                plan.actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::DeleteLocalFolder { .. })),
                "DeleteLocalFolder must still be emitted"
            );
        }

        /// Under `delete-local` + `ServerWins`, a Blocker
        /// `UpdateRemoteKeywords` against an orphan-bound
        /// email is consume-on-destroy: drop the action; the
        /// orphan destroy proceeds. The email is going away
        /// with the folder; the keyword update is moot.
        #[test]
        fn server_wins_drops_keyword_blocker_and_destroys() {
            let bindings = bindings_with_orphan(vec![]);
            let records = vec![record_for("E1", "MB-ARCH", "FOO.host")];
            let local_changes = vec![LocalChange::FlagsChanged {
                maildir_id: MaildirId::from("FOO.host"),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value(JmapMailboxId::from("MB-ARCH")),
                    server_name: "Archive".to_string(),
                    maildir_folder: "Archive".to_string(),
                    remote_path: "Archive".to_string(),
                }),
                old_flags: "S".into(),
                new_flags: "SF".into(),
            }];

            let plan = run_destructive(
                &bindings,
                &local_changes,
                AllowDestructiveFolderSync::DeleteLocal,
                ConflictStrategy::ServerWins,
                &records,
            );

            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::UpdateRemoteKeywords { .. })),
                "UpdateRemoteKeywords Blocker must be consume-on-destroy under \
                 ServerWins; got plan: {:?}",
                plan.actions
            );
            assert!(
                plan.actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::DeleteLocalFolder { .. })),
                "DeleteLocalFolder must still be emitted"
            );
        }

        /// Under `delete-local` + `LocalWins`, a Blocker
        /// `UpdateRemoteKeywords` against an orphan-bound
        /// email triggers resurrect instead of destroy. The
        /// Blocker is dropped (its email gets re-uploaded with
        /// the new flags via the file's filename); the orphan
        /// is recreated server-side via `CreateRemoteMailbox`.
        #[test]
        fn local_wins_with_blocker_resurrects_no_destroy() {
            let bindings = bindings_with_orphan(vec![orphan_msg(Some("E1"), "FOO.host")]);
            let records = vec![record_for("E1", "MB-ARCH", "FOO.host")];
            let local_changes = vec![LocalChange::FlagsChanged {
                maildir_id: MaildirId::from("FOO.host"),
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value(JmapMailboxId::from("MB-ARCH")),
                    server_name: "Archive".to_string(),
                    maildir_folder: "Archive".to_string(),
                    remote_path: "Archive".to_string(),
                }),
                old_flags: "S".into(),
                new_flags: "SF".into(),
            }];

            let plan = run_destructive(
                &bindings,
                &local_changes,
                AllowDestructiveFolderSync::DeleteLocal,
                ConflictStrategy::LocalWins,
                &records,
            );

            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::DeleteLocalFolder { .. })),
                "LocalWins-with-Blocker must NOT emit destroy; got plan: {:?}",
                plan.actions
            );
            let creates: Vec<_> = plan
                .actions
                .iter()
                .filter_map(|a| match a {
                    SyncAction::CreateRemoteMailbox {
                        name,
                        replaces_orphan_id,
                        ..
                    } => Some((name.clone(), replaces_orphan_id.clone())),
                    _ => None,
                })
                .collect();
            assert_eq!(creates.len(), 1, "resurrect emits CreateRemoteMailbox");
            assert_eq!(creates[0].0, "Archive");
            assert_eq!(
                creates[0].1.as_ref().map(|id| id.as_ref()),
                Some("MB-ARCH"),
                "CreateRemoteMailbox carries replaces_orphan_id"
            );
            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::UpdateRemoteKeywords { .. })),
                "Blocker UpdateRemoteKeywords must be dropped under LocalWins; \
                 the file's flags survive via the filename suffix"
            );
        }

        /// `MoveRemote` whose source is the orphan folder is
        /// Moot regardless of strategy -- the orphan's destroy
        /// subsumes the per-email move.
        #[test]
        fn moveremote_out_of_orphan_is_moot() {
            let action = SyncAction::MoveRemote {
                id: RemoteId {
                    jmap_email_id: "E1".into(),
                    message_id: "x@example.com".into(),
                },
                target_mailbox_ids: vec!["MB-INBOX".into()],
                from_folder: "Archive".to_string(),
                to_folder: "INBOX".to_string(),
            };
            let known = indices(&[]);
            let role = classify_action_against_orphan(
                &action,
                &"MB-ARCH".into(),
                "Archive",
                &known.by_jmap,
            );
            assert_eq!(role, OrphanActionRole::Moot);
        }

        /// `MoveRemote` whose target is the orphan folder is
        /// a Blocker -- user moved a file INTO the orphan.
        #[test]
        fn moveremote_into_orphan_is_blocker() {
            let action = SyncAction::MoveRemote {
                id: RemoteId {
                    jmap_email_id: "E1".into(),
                    message_id: "x@example.com".into(),
                },
                target_mailbox_ids: vec!["MB-ARCH".into()],
                from_folder: "INBOX".to_string(),
                to_folder: "Archive".to_string(),
            };
            let known = indices(&[]);
            let role = classify_action_against_orphan(
                &action,
                &"MB-ARCH".into(),
                "Archive",
                &known.by_jmap,
            );
            assert_eq!(role, OrphanActionRole::Blocker);
        }

        /// `UploadMessage` whose binding targets the orphan
        /// (via Value or Reference) is a Blocker.
        #[test]
        fn uploadmessage_targeting_orphan_is_blocker() {
            let action = SyncAction::UploadMessage {
                id: LocalId {
                    maildir_id: MaildirId::from("FOO.host"),
                    message_id: MessageId::from("x@example.com"),
                },
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value(JmapMailboxId::from("MB-ARCH")),
                    server_name: "Archive".to_string(),
                    maildir_folder: "Archive".to_string(),
                    remote_path: "Archive".to_string(),
                }),
                file_path: std::path::PathBuf::from("/tmp/foo"),
                flags: "S".to_string(),
            };
            let known = indices(&[]);
            let role = classify_action_against_orphan(
                &action,
                &"MB-ARCH".into(),
                "Archive",
                &known.by_jmap,
            );
            assert_eq!(role, OrphanActionRole::Blocker);
        }

        /// Under `None` policy the destructive arm is gated
        /// off; no `DeleteLocalFolder` emitted (resurrect
        /// path handles the orphan).
        #[test]
        fn no_destroy_under_none_policy() {
            let bindings = bindings_with_orphan(vec![]);
            let plan = run_destructive(
                &bindings,
                &[],
                AllowDestructiveFolderSync::None,
                ConflictStrategy::ServerWins,
                &[],
            );
            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::DeleteLocalFolder { .. })),
                "None policy must not emit destroy"
            );
            assert!(
                plan.actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::CreateRemoteMailbox { .. })),
                "None policy must emit resurrect via CreateRemoteMailbox"
            );
        }

        /// Under `delete-local` + `ServerWins` with a
        /// `MoveRemote`-into-orphan Blocker (user moved a
        /// file from a live folder INTO the orphan this
        /// cycle), finalize emits a counter `MoveLocal` from
        /// the orphan back to the source folder before the
        /// destroy. The original `MoveRemote` is dropped;
        /// `DeleteLocalFolder` proceeds. The file lands back
        /// at its source folder; the orphan goes away with
        /// `remove_dir_all`.
        #[test]
        fn server_wins_emits_counter_movelocal_for_moveremote_into_orphan() {
            let bindings = bindings_with_orphan(vec![]);
            let records = vec![record_for("E1", "MB-INBOX", "OLD.host")];
            let local_changes = vec![
                LocalChange::DeletedMessage {
                    maildir_id: "OLD.host".into(),
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                        server_name: "INBOX".to_string(),
                        maildir_folder: "INBOX".to_string(),
                        remote_path: "INBOX".to_string(),
                    }),
                },
                LocalChange::NewMessage {
                    maildir_id: "NEW.host".into(),
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value("MB-ARCH".into()),
                        server_name: "Archive".to_string(),
                        maildir_folder: "Archive".to_string(),
                        remote_path: "Archive".to_string(),
                    }),
                    flags: "".into(),
                    path: std::path::PathBuf::from("/tmp/jma-test/Archive/cur/NEW.host"),
                    message_id: "E1@example.com".into(),
                    size_bytes: 0,
                },
            ];

            let plan = run_destructive(
                &bindings,
                &local_changes,
                AllowDestructiveFolderSync::DeleteLocal,
                ConflictStrategy::ServerWins,
                &records,
            );

            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::MoveRemote { .. })),
                "ServerWins must drop the MoveRemote Blocker; got plan: {:?}",
                plan.actions
            );
            let counters: Vec<_> = plan
                .actions
                .iter()
                .filter_map(|a| match a {
                    SyncAction::MoveLocal {
                        id,
                        from_folder,
                        to_binding,
                    } => Some((id.clone(), from_folder.clone(), to_binding.clone())),
                    _ => None,
                })
                .collect();
            assert_eq!(counters.len(), 1, "one counter MoveLocal for the Blocker");
            assert_eq!(
                counters[0].0.maildir_id.as_ref(),
                "OLD.host",
                "maildir_id resolved via known_by_jmap"
            );
            assert_eq!(
                counters[0].1, "Archive",
                "counter source is the orphan folder"
            );
            assert_eq!(
                counters[0].2.maildir_folder, "INBOX",
                "counter destination is the original source folder"
            );
            assert!(
                plan.actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::DeleteLocalFolder { .. })),
                "DeleteLocalFolder must still be emitted after counter-MoveLocal"
            );
        }
    }

    /// Push-side destructive arm: the matrix in
    /// `finalize_remote_orphan_handling` decides per remote
    /// orphan whether to emit `DestroyRemoteMailbox` (and, under
    /// LocalWins + Blocker, paired counter `MoveRemote`s) or
    /// leave it alone. ServerWins+Blocker is pre-handled engine-
    /// side and is exercised in `tests/engine_jmap.rs` end-to-
    /// end; the tests below cover the reconcile-side arms.
    mod remote_orphan_destructive {
        use super::*;

        fn bindings_with_remote_orphan() -> MailboxBindings {
            let mut b = MailboxBindings::builder();
            // Prior source of any Move-blocker. Lives in the
            // live set so `by_id` lookups succeed.
            b.insert(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Value(JmapMailboxId::from("MB-INBOX")),
                server_name: "INBOX".to_string(),
                maildir_folder: "INBOX".to_string(),
                remote_path: "INBOX".to_string(),
            });
            b.push_remote_orphan(
                Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value(JmapMailboxId::from("MB-ARCH")),
                    server_name: "Archive".to_string(),
                    maildir_folder: "Archive".to_string(),
                    remote_path: "Archive".to_string(),
                }),
                None,
                0,
            );
            b.build()
        }

        fn record(jmap_id: &str, mailbox_id: &str, maildir_id: &str) -> MessageRecord {
            MessageRecord {
                jmap_email_id: jmap_id.into(),
                jmap_blob_id: Some(format!("B-{}", jmap_id).into()),
                jmap_thread_id: Some(format!("T-{}", jmap_id).into()),
                jmap_mailbox_id: mailbox_id.into(),
                maildir_id: Some(maildir_id.into()),
                message_id: format!("{}@example.com", jmap_id).into(),
                flags: "S".into(),
                jmap_keywords: "{}".into(),
            }
        }

        fn run_remote_destructive(
            mailboxes: &MailboxBindings,
            remote_emails: &[EmailObject],
            allow_destructive: AllowDestructiveFolderSync,
            strategy: ConflictStrategy,
            records: &[MessageRecord],
        ) -> SyncPlan {
            let local_index = LocalIndex::default();
            let local_flags = HashMap::new();
            let known = indices(records);
            reconcile(ReconcileInput {
                remote_emails,
                remote_destroyed: &[],
                local_changes: &[],
                known: &known,
                local_index: &local_index,
                local_flags: &local_flags,
                mailboxes,
                policy: SyncPolicy {
                    conflict_strategy: strategy,
                    allow_destructive_folder_sync: allow_destructive,
                },
                new_email_state: None,
                max_upload_size: usize::MAX,
                used_initial_path: true,
                folder_layout: FolderLayout::Flat,
                hierarchy_separator: '.',
                maildir_root: std::path::Path::new("/tmp/jma-test-maildir-root"),
            })
        }

        /// No cycle-local activity targeting the orphan: under
        /// `delete-remote` + any strategy, the orphan is destroyed
        /// server-side. No conflict to resolve, so strategy
        /// doesn't enter.
        #[test]
        fn destroys_under_delete_remote_no_blocker() {
            let bindings = bindings_with_remote_orphan();
            let plan = run_remote_destructive(
                &bindings,
                &[],
                AllowDestructiveFolderSync::DeleteRemote,
                ConflictStrategy::ServerWins,
                &[],
            );

            let destroys: Vec<_> = plan
                .actions
                .iter()
                .filter_map(|a| match a {
                    SyncAction::DestroyRemoteMailbox { binding } => Some(binding.clone()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                destroys.len(),
                1,
                "one DestroyRemoteMailbox for the orphan; got {:?}",
                plan.actions
            );
            assert_eq!(destroys[0].remote_path, "Archive");
        }

        /// `None` policy gates off the push-side destructive arm;
        /// no `DestroyRemoteMailbox` emitted regardless of orphan
        /// state.
        #[test]
        fn no_destroy_under_none_policy() {
            let bindings = bindings_with_remote_orphan();
            let plan = run_remote_destructive(
                &bindings,
                &[],
                AllowDestructiveFolderSync::None,
                ConflictStrategy::ServerWins,
                &[],
            );
            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::DestroyRemoteMailbox { .. })),
                "None policy must not emit DestroyRemoteMailbox; got {:?}",
                plan.actions
            );
        }

        /// `delete-local` permits destructive pull-side handling
        /// but NOT push-side -- so remote orphans (user deleted
        /// the maildir) are left alone.
        #[test]
        fn no_destroy_under_delete_local_policy() {
            let bindings = bindings_with_remote_orphan();
            let plan = run_remote_destructive(
                &bindings,
                &[],
                AllowDestructiveFolderSync::DeleteLocal,
                ConflictStrategy::ServerWins,
                &[],
            );
            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::DestroyRemoteMailbox { .. })),
                "delete-local must not emit DestroyRemoteMailbox; got {:?}",
                plan.actions
            );
        }

        /// LocalWins + Move Blocker: server moved an email INTO
        /// the orphan from a live source. Counter `MoveRemote`
        /// puts it back at the source, then `DestroyRemoteMailbox`
        /// fires. Order matters -- the move must land before the
        /// mailbox is destroyed.
        #[test]
        fn local_wins_with_move_blocker_emits_counter_move_then_destroy() {
            let bindings = bindings_with_remote_orphan();
            // Cached row: E1 was in MB-INBOX at cycle start.
            let records = vec![record("E1", "MB-INBOX", "FILE-1")];
            // Server-side delta: E1 is now in MB-ARCH (the orphan).
            let emails = vec![email("E1", "MB-ARCH", "S", Some("E1@example.com"))];

            let plan = run_remote_destructive(
                &bindings,
                &emails,
                AllowDestructiveFolderSync::DeleteRemote,
                ConflictStrategy::LocalWins,
                &records,
            );

            // Locate the counter MoveRemote and the
            // DestroyRemoteMailbox; assert order.
            let move_idx = plan
                .actions
                .iter()
                .position(|a| matches!(a, SyncAction::MoveRemote { .. }))
                .unwrap_or_else(|| {
                    panic!("counter MoveRemote missing; got plan: {:?}", plan.actions)
                });
            let destroy_idx = plan
                .actions
                .iter()
                .position(|a| matches!(a, SyncAction::DestroyRemoteMailbox { .. }))
                .unwrap_or_else(|| {
                    panic!("DestroyRemoteMailbox missing; got plan: {:?}", plan.actions)
                });
            assert!(
                move_idx < destroy_idx,
                "counter MoveRemote must precede DestroyRemoteMailbox in the plan; \
                 got move_idx={}, destroy_idx={}",
                move_idx,
                destroy_idx
            );

            let SyncAction::MoveRemote {
                id,
                target_mailbox_ids,
                from_folder,
                to_folder,
            } = &plan.actions[move_idx]
            else {
                unreachable!()
            };
            assert_eq!(id.jmap_email_id.as_ref(), "E1");
            assert_eq!(
                target_mailbox_ids,
                &vec![JmapMailboxId::from("MB-INBOX")],
                "counter targets the cycle-start cached mailbox"
            );
            assert_eq!(
                from_folder, "Archive",
                "counter source is the orphan folder"
            );
            assert_eq!(to_folder, "INBOX", "counter dest is the prior source");
        }

        /// LocalWins + Consumed Blocker (server added a new email
        /// directly to the orphan -- no cached row): no counter
        /// emitted, but `DestroyRemoteMailbox` still fires. The
        /// new email goes away with the mailbox.
        #[test]
        fn local_wins_with_consumed_blocker_just_destroys() {
            let bindings = bindings_with_remote_orphan();
            // Server-side delta: brand-new E1, only in the orphan,
            // no cached row.
            let emails = vec![email("E1", "MB-ARCH", "S", Some("E1@example.com"))];

            let plan = run_remote_destructive(
                &bindings,
                &emails,
                AllowDestructiveFolderSync::DeleteRemote,
                ConflictStrategy::LocalWins,
                &[],
            );

            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::MoveRemote { .. })),
                "Consumed blocker emits no counter; got plan: {:?}",
                plan.actions
            );
            assert_eq!(
                plan.actions
                    .iter()
                    .filter(|a| matches!(a, SyncAction::DestroyRemoteMailbox { .. }))
                    .count(),
                1,
                "DestroyRemoteMailbox still fires; got plan: {:?}",
                plan.actions
            );
        }

        /// LocalWins + Move Blocker whose prior source is no
        /// longer in the live set (also an orphan, or otherwise
        /// gone): the counter is warn-dropped, the email gets
        /// destroyed with the mailbox, the destroy still fires.
        #[test]
        fn local_wins_move_blocker_warns_when_prior_source_missing() {
            let bindings = bindings_with_remote_orphan();
            // Cached row points at MB-GONE which is NOT in the
            // live set.
            let records = vec![record("E1", "MB-GONE", "FILE-1")];
            let emails = vec![email("E1", "MB-ARCH", "S", Some("E1@example.com"))];

            let plan = run_remote_destructive(
                &bindings,
                &emails,
                AllowDestructiveFolderSync::DeleteRemote,
                ConflictStrategy::LocalWins,
                &records,
            );

            assert!(
                !plan
                    .actions
                    .iter()
                    .any(|a| matches!(a, SyncAction::MoveRemote { .. })),
                "no counter MoveRemote when prior source is missing; got plan: {:?}",
                plan.actions
            );
            assert_eq!(
                plan.actions
                    .iter()
                    .filter(|a| matches!(a, SyncAction::DestroyRemoteMailbox { .. }))
                    .count(),
                1,
                "DestroyRemoteMailbox still fires after warn-dropping the counter"
            );
        }

        /// Classifier unit: email whose `mailbox_ids` does NOT
        /// include the orphan id returns `None`.
        #[test]
        fn classify_email_returns_none_when_orphan_not_in_mailbox_ids() {
            let email = email("E1", "MB-INBOX", "S", Some("E1@example.com"));
            let known = indices(&[]);
            let result = classify_email_against_remote_orphan(
                &email,
                &JmapMailboxId::from("MB-ARCH"),
                &known.by_jmap,
            );
            assert!(result.is_none());
        }

        /// Classifier unit: cached row's mailbox differs from
        /// orphan id -> Move Blocker carrying prior_source.
        #[test]
        fn classify_email_as_move_blocker_when_prior_source_differs() {
            let email = email("E1", "MB-ARCH", "S", Some("E1@example.com"));
            let records = vec![record("E1", "MB-INBOX", "FILE-1")];
            let known = indices(&records);
            let result = classify_email_against_remote_orphan(
                &email,
                &JmapMailboxId::from("MB-ARCH"),
                &known.by_jmap,
            );
            match result {
                Some(RemoteOrphanBlocker::Move {
                    id,
                    message_id,
                    prior_source,
                }) => {
                    assert_eq!(id.as_ref(), "E1");
                    assert_eq!(message_id.as_ref(), "E1@example.com");
                    assert_eq!(prior_source.as_ref(), "MB-INBOX");
                }
                other => panic!("expected Move blocker, got {:?}", other.is_some()),
            }
        }

        /// Classifier unit: cached row's mailbox equals orphan id
        /// (server just changed keywords on an email already in
        /// the orphan) -> Consumed blocker.
        #[test]
        fn classify_email_as_consumed_when_prior_source_matches_orphan() {
            let email = email("E1", "MB-ARCH", "S", Some("E1@example.com"));
            let records = vec![record("E1", "MB-ARCH", "FILE-1")];
            let known = indices(&records);
            let result = classify_email_against_remote_orphan(
                &email,
                &JmapMailboxId::from("MB-ARCH"),
                &known.by_jmap,
            );
            assert!(matches!(result, Some(RemoteOrphanBlocker::Consumed)));
        }

        /// Classifier unit: no cached row at all (server added a
        /// new email directly to the orphan) -> Consumed blocker.
        #[test]
        fn classify_email_as_consumed_when_no_prior_row() {
            let email = email("E1", "MB-ARCH", "S", Some("E1@example.com"));
            let known = indices(&[]);
            let result = classify_email_against_remote_orphan(
                &email,
                &JmapMailboxId::from("MB-ARCH"),
                &known.by_jmap,
            );
            assert!(matches!(result, Some(RemoteOrphanBlocker::Consumed)));
        }
    }
}
