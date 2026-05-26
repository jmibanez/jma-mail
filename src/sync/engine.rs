use anyhow::Result;
use futures_util::stream::{self, StreamExt};
use jmap_client::client::Client;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::ids::{JmapAccountId, JmapEmailId, JmapMailboxId, MaildirId};
use crate::jmap::{
    email as jmap_email, limits, mailbox as jmap_mailbox, session,
    types::{EmailObject, MailboxFolderBinding, MailboxObject, MaybeReference, SessionInfo},
};
use crate::maildir_ops::layout::{
    FolderLayoutDefinition, decompose_folder_string, resolve_folder_path,
};
use crate::maildir_ops::namespace::is_jma_private;
use crate::maildir_ops::scan::LocalChange;
use crate::maildir_ops::{scan, store};
use crate::state::queries;
use crate::sync::bindings::{MailboxBindings, MailboxBindingsBuilder};
use crate::sync::dedupe::{LocalEntry, LocalIndex};
use crate::sync::execute::Executor;
use crate::sync::plan::{SyncAction, SyncDirection};
use crate::sync::reconcile::{self, MessageRecordIndex, ReconcileInput};
use crate::sync::self_writes::SelfWriteCache;
use tracing::Instrument;

/// How the local-side scan should be carried out for one cycle.
/// `Full` walks every synced folder; `Paths(...)` classifies only
/// the `(folder, maildir_id)` groups the supplied event paths
/// touched. The daemon picks `Paths` for `LocalChange` triggers
/// (where the watcher hands us the authoritative change set) and
/// `Full` for everything else (Initial, RemoteChange, CLI one-shots,
/// post-disconnect catch-up).
#[derive(Debug, Clone)]
pub enum ScanScope {
    Full,
    Paths(Vec<PathBuf>),
}

/// Outcome of one sync iteration -- enough for the daemon loop to know
/// whether to fire the post-arrival hook and to flag a degraded cycle.
#[derive(Debug, Default, Clone, Copy)]
pub struct SyncOutcome {
    pub downloaded: usize,
    /// Local messages successfully pushed to the server via
    /// Email/import this cycle. Counts only the successes; uploads
    /// the server rejected (or whose import returned an error) are
    /// excluded so the number matches what actually landed remotely.
    pub uploaded: usize,
    /// Flag/keyword updates that landed this cycle, summed across
    /// both directions: maildir flag changes mirrored from server
    /// keyword diffs (`UpdateLocalFlags`) and server keyword pushes
    /// from local flag diffs (`UpdateRemoteKeywords`). Skipped or
    /// rejected updates are excluded so the count matches what
    /// actually changed on at least one side.
    pub flag_updates: usize,
    /// Cross-folder moves that landed this cycle, summed across both
    /// directions: server folder changes mirrored to local maildir
    /// (`MoveLocal`) and local folder changes pushed to the server
    /// (`MoveRemote`). Skipped or rejected moves are excluded.
    pub moved: usize,
    /// Deletions that landed this cycle, summed across both
    /// directions: server destroys mirrored to local
    /// (`DeleteLocal`) and local deletions pushed to the server
    /// (`DestroyRemote`). Skipped or rejected destroys are excluded.
    pub deleted: usize,
    /// Per-id failures from the remote-side Email/set batch
    /// (notUpdated + notDestroyed). Each one is also logged at warn
    /// level with id and folder context; this count is the
    /// at-a-glance summary so the user doesn't have to grep across a
    /// long cycle log to notice anything went wrong. These conditions
    /// are self-healing -- the next cycle re-detects and re-attempts
    /// each rejected action -- so they stay below the `error!` bar.
    pub failed_remote_actions: usize,
    /// True when reconcile produced an empty plan -- neither side had
    /// work to do. The daemon uses this to flag spurious `LocalChange`
    /// triggers (FS notification fired but nothing actually changed)
    /// so the user can correlate the no-op cycle with the FS event
    /// paths that drove it.
    pub already_in_sync: bool,
}

/// Bundles the immutable state every engine helper threads through
/// (client, DB connection, config, account id) for the engine's
/// lifetime -- one-shot CLI commands drop it at the end of the call,
/// the daemon keeps it for the whole watch loop and reuses it across
/// every trigger. The struct lets us add another shared field without
/// touching every helper signature. Helpers that take only a
/// `&Connection` (e.g. `build_known_indices`) and stateless helpers
/// (`log_dropped`) stay free functions because they don't need the
/// client; keeping them off `SyncEngine` lets unit tests drive them
/// with just an in-memory `Connection`.
pub struct SyncEngine<'a> {
    client: Arc<Client>,
    conn: &'a Connection,
    config: &'a Config,
    account_id: JmapAccountId,
    /// Cache of paths jma has just written, shared with the
    /// daemon's filesystem watcher so it can drop self-echo
    /// fsevents without firing a sync cycle. None for one-shot CLI
    /// commands -- they don't run a watcher and have no consumer
    /// for the cache.
    self_writes: Option<Arc<SelfWriteCache>>,
}

impl<'a> SyncEngine<'a> {
    /// Open a JMAP session for `config`'s account and bind it to the
    /// given DB connection. The engine owns the resulting `Client` for
    /// the rest of its lifetime; `cmd_sync`/`cmd_pull`/`cmd_push` build
    /// one and drop it at the end of the command, while
    /// `daemon::runner::run` builds one and drives it across every
    /// trigger.
    pub async fn connect(conn: &'a Connection, config: &'a Config) -> Result<Self> {
        let client = session::connect(&config.account, conn).await?;
        let account_id: JmapAccountId = client.default_account_id().into();
        Ok(Self {
            client: Arc::new(client),
            conn,
            config,
            account_id,
            self_writes: None,
        })
    }

    /// Attach a self-write cache so that the daemon's executor
    /// records every disk-mutating store operation it performs and
    /// the watcher can drop self-echo fsevents that would otherwise
    /// drive no-op sync cycles. CLI one-shots leave this unset --
    /// they don't run a watcher and the cache would just be a
    /// memory leak across the call.
    pub fn set_self_writes(&mut self, cache: Arc<SelfWriteCache>) {
        self.self_writes = Some(cache);
    }

    /// Snapshot the JMAP session metadata the daemon needs for SSE
    /// setup (event-source URL, account id, etc.). Delegates to
    /// `session::session_info` against the engine's owned `Client`.
    pub fn session_info(&self) -> Result<SessionInfo> {
        session::session_info(&self.client)
    }

    /// Connect a fresh engine and run a full bidirectional sync.
    /// Convenience entry point for `cmd_sync`; the daemon, which keeps
    /// one engine across many triggers, drives `run` directly instead.
    pub async fn sync(
        conn: &'a Connection,
        config: &'a Config,
        dry_run: bool,
    ) -> Result<SyncOutcome> {
        Self::connect(conn, config)
            .await?
            .run(dry_run, SyncDirection::Both, ScanScope::Full)
            .await
    }

    /// Connect a fresh engine and pull only (server -> local). Adoption
    /// still runs.
    pub async fn pull_only(
        conn: &'a Connection,
        config: &'a Config,
        dry_run: bool,
    ) -> Result<SyncOutcome> {
        Self::connect(conn, config)
            .await?
            .run(dry_run, SyncDirection::PullOnly, ScanScope::Full)
            .await
    }

    /// Connect a fresh engine and push only (local -> server). Adoption
    /// still runs.
    pub async fn push_only(
        conn: &'a Connection,
        config: &'a Config,
        dry_run: bool,
    ) -> Result<SyncOutcome> {
        Self::connect(conn, config)
            .await?
            .run(dry_run, SyncDirection::PushOnly, ScanScope::Full)
            .await
    }

    /// Single orchestration path. `direction` selects which side(s) of
    /// the plan execute; adoption always runs. `scan_scope` selects
    /// between a full per-folder walk and a path-driven scan keyed off
    /// fsevents. Public so the daemon can drive its long-lived engine
    /// across triggers without reconnecting.
    pub async fn run(
        &self,
        dry_run: bool,
        direction: SyncDirection,
        scan_scope: ScanScope,
    ) -> Result<SyncOutcome> {
        let mailboxes = self.resolve_mailboxes().await?;
        let maildir_root = self.config.maildir_path();

        // Push-only on a fresh config has nothing local to upload:
        // CreateLocalMailbox actions would be dropped by the
        // direction filter and the cycle would burn through scan,
        // remote-state fetch, and reconcile only to plan zero work.
        // Bail upfront with an actionable message so the user knows
        // a pull-side cycle is the prerequisite. Only fires when
        // resolve_mailboxes returned at least one synced binding --
        // an empty bindings set has its own failure modes (filter
        // excludes everything, server returned no mailboxes) and is
        // not this commit's concern. Fires under `--dry-run` too:
        // the preview of zero work is no more useful than the
        // error, and the error is actionable.
        if matches!(direction, SyncDirection::PushOnly) && !mailboxes.is_empty() {
            let provisioned = mailboxes
                .iter()
                .filter(|b| {
                    store::try_open_maildir(&maildir_root.join(&b.maildir_folder)).is_some()
                })
                .count();
            if provisioned == 0 {
                return Err(anyhow::anyhow!(
                    "No local maildirs are provisioned yet; run `jma sync` or `jma pull` \
                     first to land them, then push will have something to upload."
                ));
            }
        }

        // Phase 0: classify duplicates and (conditionally) index. Always
        // before scan so newly-introduced duplicates from a prior aborted
        // run don't get treated as local changes to push.
        //
        // Reconcile only consults `LocalIndex` when its DB-derived
        // lookup misses (initial sync, post-`c7a86e6`-recovery wipe, or
        // any other state where `message_map` is empty). In steady
        // state every remote Message-ID resolves through the DB, the
        // index is allocated and never read. Skip the build entirely
        // when the DB has rows: plan_dedupe still runs (cheap pure
        // walk), `local_index` stays at default-empty, and
        // Empty-HashMap lookups return None -- which is exactly what
        // the existing reconcile stage-2 check already handles.
        //
        // Plan now, apply later: `apply_dedupe` only runs on non-dry
        // cycles, so `jma sync --dry-run` no longer destroys local
        // duplicates as a side effect of inspecting "what would
        // happen." The plan also feeds (a) the dry-run printout below
        // and (b) the post-scan filter, which suppresses scan's view
        // of the duplicates so reconcile sees the post-dedupe shape
        // even under dry-run.
        //
        // Path-scoped cycles skip the dedupe walk entirely. The walk
        // is folder-wide -- it parses Message-IDs out of every cur/
        // and new/ file across every synced folder -- so its cost
        // doesn't shrink when the trigger only touches a handful of
        // paths. Paths-scope only fires from the daemon's LocalChange
        // triggers, where (a) the DB always has rows (Initial seeded
        // it via Full), so the LocalIndex bridge is unused; (b)
        // dry-run never applies (the daemon never runs dry); and (c)
        // any duplicate the cycle misses is caught by the next
        // Full-scope trigger (SSE pulse, Initial, CLI sync), which
        // arrives soon on a connected daemon.
        //
        // Within a Full cycle, the walk is further narrowed against
        // `folder_checkpoint`: any folder whose current (cur_mtime,
        // new_mtime, cur_count, new_count) snapshot matches the row
        // written at the tail of the last successful cycle cannot
        // have grown a per-folder Message-ID duplicate since then.
        // mtime advances on any add, remove, or in-place rename;
        // count advances only on add/remove. Requiring both to match
        // means an in-place rename (the common MUA flag-flip path)
        // is correctly recognised as not-dupe-inducing. A missing
        // checkpoint row (first sync, post-recovery wipe, freshly
        // added folder) is treated as dirty, so the LocalIndex
        // bridge still gets populated on first sync.
        let folder_names: Vec<String> =
            mailboxes.iter().map(|b| b.maildir_folder.clone()).collect();
        let dedupe_targets: Vec<String> = if matches!(scan_scope, ScanScope::Full) {
            compute_dirty_folders(self.conn, &maildir_root, &folder_names)?
        } else {
            Vec::new()
        };
        let dedupe_plan = crate::janitor::dedupe::run(&maildir_root, &dedupe_targets, dry_run)?;
        let mut local_index = LocalIndex::default();
        if !queries::has_message_map_rows(self.conn)? {
            for kept in &dedupe_plan.kept {
                local_index
                    .by_message_id
                    .entry(kept.message_id.clone())
                    .or_default()
                    .push(LocalEntry {
                        folder: kept.folder.clone(),
                        maildir_id: kept.maildir_id.clone(),
                    });
            }
        }

        // Phase 1: scan local changes. `Full` walks every synced
        // folder; `Paths` classifies only the (folder, maildir_id)
        // groups touched by the supplied event paths -- the daemon's
        // common case, where the watcher already told us exactly
        // which files moved.
        let (scan_changes, local_flags) = {
            let _phase =
                tracing::info_span!(target: crate::profile::TARGET_PHASE, "scan").entered();
            match &scan_scope {
                ScanScope::Full => {
                    let mut changes = Vec::new();
                    let mut local_flags: HashMap<MaildirId, String> = HashMap::new();
                    for binding in mailboxes.iter() {
                        let maildir_path = maildir_root.join(&binding.maildir_folder);
                        // A live binding whose maildir doesn't yet
                        // exist on disk is the first-cycle case for
                        // a server-known mailbox; the
                        // `CreateLocalMailbox` action emitted by
                        // reconcile will create it in the executor's
                        // first phase. Skip the walk here -- no
                        // local files to classify -- without side-
                        // effecting the folder into existence.
                        let Some(maildir) = store::try_open_maildir(&maildir_path) else {
                            continue;
                        };
                        let known_state = hydrate_known_state(self.conn, binding)?;
                        let result = scan::scan_folder(&maildir, binding, &known_state)?;
                        changes.extend(result.changes);
                        local_flags.extend(result.local_flags);
                    }
                    // Folder-discovery pass: enumerate maildir-shaped
                    // directories under `maildir_root` that no binding
                    // covers and emit a `LocalFolderCreated` for each.
                    // Walk strategy is layout-aware -- see
                    // `scan::discover_unbound_folders`. Full-scope is
                    // the comprehensive sweep that catches folders
                    // created while the daemon wasn't running (or
                    // missed by fsevents); `scan_paths` emits the same
                    // variant per-event for folders the watcher saw
                    // live, so both scopes converge on the same shape.
                    changes.extend(scan::discover_unbound_folders(
                        &maildir_root,
                        self.config.sync.folder_layout,
                        &mailboxes,
                    ));
                    (changes, local_flags)
                }
                ScanScope::Paths(paths) => {
                    let mut known_states = HashMap::new();
                    for binding in mailboxes.iter() {
                        // Path-driven cycles only fire for fsevent
                        // paths the watcher already saw, so a
                        // maildir-shaped folder absent at this
                        // point means no events referenced it.
                        // Skip the state hydration for absent
                        // folders rather than creating them as a
                        // side effect.
                        if store::try_open_maildir(&maildir_root.join(&binding.maildir_folder))
                            .is_none()
                        {
                            continue;
                        }
                        let state = hydrate_known_state(self.conn, binding)?;
                        known_states.insert(binding.maildir_folder.clone(), state);
                    }
                    let result = scan::scan_paths(
                        &maildir_root,
                        paths,
                        &known_states,
                        &mailboxes,
                        self.config.sync.folder_layout,
                    )?;
                    (result.changes, result.local_flags)
                }
            }
        };

        // Suppress scan's view of files plan_dedupe flagged for
        // removal. Under non-dry-run apply_dedupe ran above and the
        // file is gone, so scan can't see it anyway -- the filter is
        // a no-op. Under dry-run the file is still on disk and scan
        // emits NewMessage/FlagsChanged against it; without this
        // filter reconcile would print a plan with phantom actions
        // for the soon-to-be-deduped duplicates, lying about what a
        // real `jma sync` would do.
        let all_local_changes: Vec<LocalChange> = if dry_run && !dedupe_plan.deletions.is_empty() {
            let suppressed: HashSet<(String, MaildirId)> = dedupe_plan
                .deletions
                .iter()
                .map(|d| (d.folder.clone(), d.maildir_id.clone()))
                .collect();
            scan_changes
                .into_iter()
                .filter(|c| {
                    let key = match c {
                        LocalChange::NewMessage {
                            binding,
                            maildir_id,
                            ..
                        }
                        | LocalChange::FlagsChanged {
                            binding,
                            maildir_id,
                            ..
                        }
                        | LocalChange::DeletedMessage {
                            binding,
                            maildir_id,
                            ..
                        } => (binding.maildir_folder.clone(), maildir_id.clone()),
                        // Folder-lifecycle variants are not message-level
                        // and don't participate in dedupe; always pass
                        // through.
                        LocalChange::LocalFolderCreated { .. }
                        | LocalChange::LocalFolderDeleted { .. }
                        | LocalChange::LocalFolderRenamed { .. } => return true,
                    };
                    !suppressed.contains(&key)
                })
                .collect()
        } else {
            scan_changes
        };

        // Path-scan short-circuit: a LocalChange-driven cycle whose
        // scan classified nothing has no local work to send and no
        // remote signal that anything moved (a server-side change
        // arrives via RemoteChange, not LocalChange). Skip Phase 2
        // (Email/changes per folder) and everything after. The
        // Mailbox/get in `resolve_mailboxes` has already run by this
        // point and is unavoidable -- the mailbox list drives the
        // scan -- but suppressing the N x Email/changes round-trips
        // is the bulk of the saving. Without this, every spurious
        // fsevent that survives the watcher filter (an mbsync write
        // missed by the self-write cache, Time Machine touching
        // attributes, a backup tool brushing a real message) costs
        // a full per-folder JMAP delta fetch. RemoteChange and
        // Initial coalesce to Full scope (see `coalesce_triggers`)
        // so they bypass this entirely and always fetch.
        if matches!(scan_scope, ScanScope::Paths(_)) && all_local_changes.is_empty() {
            debug!("Path-scan produced no local changes; skipping remote fetch");
            return Ok(SyncOutcome {
                already_in_sync: true,
                ..Default::default()
            });
        }

        // Phase 2: collect remote changes.
        let (remote_emails, remote_destroyed, new_state, used_initial_path) = self
            .fetch_remote_state(&mailboxes)
            .instrument(tracing::info_span!(
                target: crate::profile::TARGET_PHASE,
                "fetch_remote",
            ))
            .await?;

        // Phase 3: build known indices and reconcile.
        let plan = {
            let _phase =
                tracing::info_span!(target: crate::profile::TARGET_PHASE, "reconcile").entered();
            let known = build_known_indices(self.conn, &mailboxes)?;
            reconcile::reconcile(ReconcileInput {
                remote_emails: &remote_emails,
                remote_destroyed: &remote_destroyed,
                local_changes: &all_local_changes,
                known: &known,
                local_index: &local_index,
                local_flags: &local_flags,
                mailboxes: &mailboxes,
                policy: reconcile::SyncPolicy::from_sync_config(&self.config.sync),
                new_email_state: Some(new_state),
                max_upload_size: limits::max_size_upload(&self.client),
                used_initial_path,
                folder_layout: self.config.sync.folder_layout,
                hierarchy_separator: self.config.sync.hierarchy_separator,
                maildir_root: &maildir_root,
            })
        };

        // Snapshot before `into_filtered`: empty here means reconcile
        // produced nothing, not "everything got filtered out by
        // pull-only/push-only" -- those still need the regular log.
        // Computed before the dry-run branch so the field stays
        // truthful for any caller that consumes the outcome. Dedupe
        // deletions count as work for the purposes of this flag:
        // saying "already in sync" while N duplicates are queued for
        // removal would be a lie under both dry-run (the user is
        // about to see them in the plan) and non-dry-run (apply_dedupe
        // just ran and removed them).
        let already_in_sync = plan.is_empty() && dedupe_plan.deletions.is_empty();

        if dry_run {
            if !dedupe_plan.deletions.is_empty() {
                println!(
                    "Dedupe pass: would remove {} duplicate file(s):",
                    dedupe_plan.deletions.len()
                );
                for d in &dedupe_plan.deletions {
                    println!(
                        "  [DEDUPE] {} ({}) in {}/  (keeping {})",
                        d.maildir_id, d.message_id, d.folder, d.kept_maildir_id
                    );
                }
                println!();
            }
            print!("{}", plan);
            return Ok(SyncOutcome {
                already_in_sync,
                ..Default::default()
            });
        }

        // An empty plan still has to flow through the executor so the
        // cursor write at the tail of `execute` runs. Without that,
        // the next `Email/changes` call replays the same "no-op for
        // us" updates -- a self-induced echo from a recent push, a
        // server-side change to a field we don't model, or anything
        // else that produced no plan action -- and we'd loop forever
        // re-fetching them. The per-phase methods inside `execute`
        // are all empty-vec no-ops, so this costs nothing user-
        // visible beyond the cursor advance.
        if already_in_sync {
            debug!("Plan is empty; running executor to advance the cursor only");
        } else {
            debug!("Plan to execute {}", plan);
        }

        // Phase 4: filter by direction; warn on every dropped non-adoption
        // action so the user sees that pull-only / push-only suppressed
        // something they may have wanted.
        let (filtered, dropped) = plan.into_filtered(direction);
        log_dropped(direction, &dropped);

        debug!(
            "Executing {:?} direction-filtered plan {}",
            direction, filtered
        );

        // Unconditional cache mutations from `resolve_mailboxes`:
        // metadata refreshes for `Unchanged`/`CacheStale` bindings.
        // Applied before the executor so the executor sees a
        // mailbox_map that already reflects the just-fetched server
        // view. The dry-run early return above ensures this
        // doesn't fire when the user only asked for a plan.
        apply_unconditional_mailbox_writes(self.conn, mailboxes.mailbox_metadata_writes())?;

        // Phase 5: execute.
        let executor = Executor::new(
            Arc::clone(&self.client),
            self.conn,
            self.config,
            self.self_writes.clone(),
        );
        let mut outcome = executor
            .execute(filtered)
            .instrument(tracing::info_span!(
                target: crate::profile::TARGET_PHASE,
                "execute",
            ))
            .await?;
        outcome.already_in_sync = already_in_sync;

        if outcome.failed_remote_actions > 0 {
            warn!(
                "Cycle completed with {} rejected remote action(s); see preceding warnings",
                outcome.failed_remote_actions
            );
        }

        // Phase 6: record per-folder checkpoints. Snapshot every
        // synced folder after the executor has landed its writes,
        // so the next cycle's Phase 0 dirty check sees the
        // post-cycle state. Done for every Full cycle (Paths
        // cycles skip both dedupe and this write, since the
        // watcher events drive the same recheck cadence). Failures
        // here are non-fatal: a missing or out-of-date checkpoint
        // row just means the next Full cycle re-walks dedupe for
        // that folder, which is the safe direction. Placed outside
        // any transaction with the executor's cursor write -- if
        // the process dies between cursor advance and checkpoint
        // write, the next cycle replays cleanly off the cursor
        // and re-walks dedupe.
        //
        // Partial-failure cycles (`failed_remote_actions > 0`)
        // still checkpoint: the cursor advanced too, so the same
        // idempotency contract that lets the next cycle replay
        // unaffected work also covers the per-folder snapshot
        // here.
        if matches!(scan_scope, ScanScope::Full)
            && let Err(e) = record_folder_checkpoints(self.conn, &maildir_root, &folder_names)
        {
            warn!("Failed to record folder_checkpoint rows; next cycle will re-walk dedupe: {e:#}");
        }

        if already_in_sync {
            crate::notify!("Already in sync");
        } else if used_initial_path {
            crate::notify!(
                "Initial sync complete ({} downloaded, {} uploaded, {} flag updates, {} moved, {} deleted)",
                outcome.downloaded,
                outcome.uploaded,
                outcome.flag_updates,
                outcome.moved,
                outcome.deleted
            );
        } else {
            crate::notify!(
                "Sync complete ({} downloaded, {} uploaded, {} flag updates, {} moved, {} deleted)",
                outcome.downloaded,
                outcome.uploaded,
                outcome.flag_updates,
                outcome.moved,
                outcome.deleted
            );
        }
        Ok(outcome)
    }

    /// Resolve the set of mailboxes to sync, returning a
    /// [`MailboxBindings`] indexed for O(1) lookup by either id or
    /// on-disk folder. Every downstream consumer (scan, reconcile,
    /// execute) takes a reference to the same bundle so the
    /// `(mailbox_id, folder)` pair never has to be rebuilt from a
    /// tuple or rediscovered via lookup.
    pub async fn resolve_mailboxes(&self) -> Result<MailboxBindings> {
        // The shell does every I/O read up front and every I/O
        // write at the end; `compute_mailbox_resolution` is the
        // pure decision core in between. All reads happen before
        // any writes so a single upsert mid-cycle can't shift
        // what the decision loop observes.
        let remote_mailboxes = jmap_mailbox::get_all(&self.client).await?;
        let cached_records: HashMap<JmapMailboxId, queries::MailboxRecord> =
            queries::get_all_mailboxes(self.conn)?
                .into_iter()
                .map(|m| (m.jmap_mailbox_id.clone(), m))
                .collect();

        // Walk the maildir tree once, building an id -> on-disk
        // path map from `.jma.mapping` sentinels. Used for
        // local-rename detection: a tracked id whose sentinel
        // lives at a different path than the cache (and the
        // server) is the "user `mv`-ed the maildir" case. Empty
        // for first-cycle syncs (no sentinels exist yet).
        let maildir_root = self.config.maildir_path();
        let disk_sentinels = walk_sentinels(&maildir_root);

        let name_cap = limits::max_size_mailbox_name(&self.client);
        let layout_definition = FolderLayoutDefinition::from_config(self.config, name_cap);

        // Per-synced-mailbox disk-state pre-pass for the Unchanged
        // arm's `needs_create` check. Reading for every synced
        // mailbox (not just the ones that land in Unchanged) is
        // the trade for keeping the decision loop free of I/O;
        // the cost is per-folder local-FS reads, which scan
        // already pays many times over each cycle.
        let by_id: HashMap<JmapMailboxId, &MailboxObject> = remote_mailboxes
            .iter()
            .map(|mb| (mb.id.clone(), mb))
            .collect();
        let mut unchanged_disk_states: HashMap<JmapMailboxId, UnchangedDiskState> = HashMap::new();
        for mb in &remote_mailboxes {
            if !jmap_mailbox::is_mailbox_synced(
                &self.config.sync.mailboxes,
                mb,
                &by_id,
                self.config.sync.case_insensitive_match,
            ) {
                continue;
            }
            let server_folder = resolve_folder_path(mb, &by_id, &layout_definition)?;
            let folder_path = maildir_root.join(&server_folder);
            let maildir_present = store::try_open_maildir(&folder_path).is_some();
            let sentinel = if maildir_present {
                crate::maildir_ops::sentinel::read(&folder_path)?
            } else {
                None
            };
            unchanged_disk_states.insert(
                mb.id.clone(),
                UnchangedDiskState {
                    maildir_present,
                    sentinel,
                },
            );
        }

        // Pure decision core: takes the I/O snapshot above, emits
        // a fully-populated builder. No I/O happens inside.
        let synced = compute_mailbox_resolution(
            &MailboxesInput {
                remote_mailboxes: &remote_mailboxes,
                cached_records: &cached_records,
                disk_sentinels: &disk_sentinels,
                unchanged_disk_states: &unchanged_disk_states,
            },
            &self.config.sync,
            &layout_definition,
        )?;

        let synced = synced.build();
        crate::notify!("Syncing {} mailboxes", synced.len());
        Ok(synced)
    }

    /// Returns `(emails, destroyed, new_state, used_initial_path)`.
    ///
    /// On state-present: loops Email/changes until has_more_changes is
    /// false, accumulating ids; then Email/get on (created ∪ updated).
    /// On no-state (or cannotCalculateChanges): Email/query per mailbox,
    /// then Email/get; new_state via get_current_state.
    async fn fetch_remote_state(
        &self,
        mailboxes: &MailboxBindings,
    ) -> Result<(Vec<EmailObject>, Vec<JmapEmailId>, String, bool)> {
        let cursor = queries::get_jmap_state(self.conn, self.account_id.as_ref(), "Email")?;

        if let Some(state) = cursor {
            let mut current = state;
            let mut all_created: Vec<JmapEmailId> = Vec::new();
            let mut all_updated: Vec<JmapEmailId> = Vec::new();
            let mut all_destroyed: Vec<JmapEmailId> = Vec::new();

            let final_state = loop {
                let res = jmap_email::get_changes(&self.client, &current).await;
                match res {
                    Ok(changes) => {
                        all_created.extend(changes.created);
                        all_updated.extend(changes.updated);
                        all_destroyed.extend(changes.destroyed);
                        let next = changes.new_state.clone();
                        if !changes.has_more_changes {
                            break next;
                        }
                        current = next;
                    }
                    Err(e) => {
                        if jmap_email::is_cannot_calculate_changes(&e) {
                            info!("Server cannot calculate changes; falling back to initial pull");
                            queries::set_jmap_state(
                                self.conn,
                                self.account_id.as_ref(),
                                "Email",
                                "",
                            )?;
                            return self.initial_remote_state(mailboxes).await;
                        }
                        return Err(e);
                    }
                }
            };

            let mut seen: HashSet<JmapEmailId> = all_created.iter().cloned().collect();
            let mut fetch_ids: Vec<JmapEmailId> = all_created;
            for u in all_updated {
                if seen.insert(u.clone()) {
                    fetch_ids.push(u);
                }
            }
            let emails = self.batched_get(&fetch_ids).await?;
            Ok((emails, all_destroyed, final_state, false))
        } else {
            self.initial_remote_state(mailboxes).await
        }
    }

    async fn initial_remote_state(
        &self,
        mailboxes: &MailboxBindings,
    ) -> Result<(Vec<EmailObject>, Vec<JmapEmailId>, String, bool)> {
        let mut all_ids: Vec<JmapEmailId> = Vec::new();
        let mut seen: HashSet<JmapEmailId> = HashSet::new();

        // Fan out per-mailbox Email/query in parallel. `query_mailbox`
        // now also fans out within a mailbox (probe + parallel page
        // fetch via `calculateTotal`), so the same `n` budget applies
        // at two levels: up to `n` mailboxes in flight, each issuing
        // up to `n` concurrent page requests. The product oversubscribes
        // the server's `maxConcurrentRequests` in the worst case; in
        // practice the inner fan-out is short-lived (a mailbox's page
        // count is small relative to `n` for typical accounts) and
        // jmap-client's transient-error retries cover the rest.
        // Result order is non-deterministic with `buffer_unordered`;
        // the downstream consumers (`batched_get` and the seen-set
        // dedupe) are both order-agnostic.
        let n = limits::concurrent_requests(&self.client, self.config.sync.download_concurrency);
        let client = &self.client;
        let futures = mailboxes.iter().map(|binding| async move {
            jmap_email::query_mailbox(
                client,
                binding
                    .jmap_mailbox_id
                    .expect_resolved("engine::initial_remote_state -- per-mailbox JMAP query")
                    .as_ref(),
                &binding.maildir_folder,
                n,
            )
            .await
        });
        let mut stream = stream::iter(futures).buffer_unordered(n);

        while let Some(result) = stream.next().await {
            let ids = result?;
            for id in ids {
                if seen.insert(id.clone()) {
                    all_ids.push(id);
                }
            }
        }

        let emails = self.batched_get(&all_ids).await?;
        let state = jmap_email::get_current_state(&self.client).await?;
        Ok((emails, Vec::new(), state, true))
    }

    async fn batched_get(&self, ids: &[JmapEmailId]) -> Result<Vec<EmailObject>> {
        // Fan the Email/get chunks out through buffer_unordered with
        // the same server-cap-aware ceiling the download stream uses.
        // The chunks themselves are non-overlapping slices of `ids`,
        // so the per-batch calls have no ordering or data dependency
        // on each other; reconcile keys off email ID and is
        // order-agnostic. On a fresh-server initial pull the metadata
        // phase used to serialize N = ceil(total / chunk_size) Email/
        // get round-trips back to back; this collapses that to
        // ceil(N / concurrency) parallel rounds.
        let chunk_size = limits::max_objects_in_get(&self.client);
        let n = limits::concurrent_requests(&self.client, self.config.sync.download_concurrency);
        let client = &self.client;
        let futures = ids
            .chunks(chunk_size)
            .map(|chunk| async move { jmap_email::get_by_ids(client, chunk).await });
        let mut stream = stream::iter(futures).buffer_unordered(n);
        let mut out: Vec<EmailObject> = Vec::new();
        while let Some(batch) = stream.next().await {
            out.extend(batch?);
        }
        Ok(out)
    }
}

/// Walk a mailbox's parent chain through `by_id` and return how
/// many ancestors it has. Used to order `resolve_mailboxes`'s loop
/// shallowest-first so a parent rename under `LAYOUT=fs` runs
/// before any descendant rename in the same cycle. The 1024 cap is
/// a guard against pathological cyclic input -- a real account
/// nesting that deep would already be unworkable in MUAs.
fn parent_chain_depth(mb: &MailboxObject, by_id: &HashMap<JmapMailboxId, &MailboxObject>) -> usize {
    let mut depth = 0usize;
    let mut current = mb.parent_id.as_ref();
    while let Some(pid) = current {
        depth += 1;
        if depth > 1024 {
            break;
        }
        current = by_id.get(pid).and_then(|p| p.parent_id.as_ref());
    }
    depth
}

/// Compare each synced folder's current `(cur, new)` snapshot
/// against the row written by the last successful cycle. Returns
/// the folders whose snapshot differs (or whose checkpoint row is
/// absent entirely) -- the set Phase 0 must walk for dedupe to
/// remain authoritative. Folders the maildir has just been
/// `ensure_maildir`'d are treated as dirty on first sight: a
/// freshly added folder has no row, so its snapshot trivially
/// doesn't match anything.
fn compute_dirty_folders(
    conn: &Connection,
    maildir_root: &std::path::Path,
    folder_names: &[String],
) -> Result<Vec<String>> {
    let mut dirty = Vec::new();
    for folder in folder_names {
        let path = maildir_root.join(folder);
        // First-cycle folders haven't been created on disk yet --
        // `CreateLocalMailbox` lands in the executor, not in
        // resolve_mailboxes. Treat the absent case as
        // "trivially clean" (no files, no dedupe needed) without
        // a side-effecting `ensure_maildir`. The snapshot below
        // reads `cur/`/`new/`; a maildir-shaped folder absent at
        // this point can't have grown duplicates since the last
        // checkpoint (it has no files at all).
        if !path.join("cur").is_dir() {
            continue;
        }
        let current = crate::maildir_ops::snapshot::snapshot_folder(&path)?;
        let recorded = queries::get_folder_checkpoint(conn, folder)?;
        match recorded {
            Some(prev) if prev == current => {
                debug!("folder_checkpoint match -- skipping dedupe for {}", folder);
            }
            _ => dirty.push(folder.clone()),
        }
    }
    Ok(dirty)
}

/// Apply the unconditional cache mutations resolved during the
/// per-cycle decision pass: metadata refreshes for
/// `Unchanged`/`CacheStale` bindings. The path does not check
/// disk state -- the metadata writes target rows whose
/// `maildir_folder` already matches the live binding. The
/// caller is responsible for the dry-run short-circuit: this
/// function unconditionally writes when invoked, matching the
/// shape of `apply_pending_mailbox_writes`.
fn apply_unconditional_mailbox_writes(
    conn: &Connection,
    metadata_writes: &[queries::MailboxRecord],
) -> Result<()> {
    for record in metadata_writes {
        queries::upsert_mailbox(conn, record)?;
    }
    Ok(())
}

/// Snapshot every synced folder after a successful cycle and upsert
/// the row that the next cycle's `compute_dirty_folders` will compare
/// against. Stat'd unconditionally for every synced folder (not just
/// dedupe targets) because Phase 5 may have downloaded into a
/// folder Phase 0 classified as clean -- without re-snapshotting,
/// the unchanged checkpoint row would be stale, and the next cycle
/// would flag the folder dirty and re-walk dedupe for nothing.
fn record_folder_checkpoints(
    conn: &Connection,
    maildir_root: &std::path::Path,
    folder_names: &[String],
) -> Result<()> {
    for folder in folder_names {
        let path = maildir_root.join(folder);
        // Skip folders that don't yet have a maildir shape on
        // disk -- a first-cycle mailbox whose
        // `CreateLocalMailbox` phase hasn't actually landed
        // (dry-run, or pull-only mode with no plan execution)
        // has nothing to checkpoint. The next cycle's
        // `compute_dirty_folders` will treat the still-absent
        // folder as trivially clean too.
        if !path.join("cur").is_dir() {
            continue;
        }
        let snapshot = crate::maildir_ops::snapshot::snapshot_folder(&path)?;
        queries::upsert_folder_checkpoint(conn, folder, &snapshot)?;
    }
    Ok(())
}

/// Hydrate one folder's `local_state` rows into the
/// `(JmapMailboxId, flags)` shape `scan_folder` / `scan_paths` want.
/// `get_local_state_for_folder` filters its `SELECT` on `maildir_folder
/// = binding.maildir_folder`, so every row's recorded folder equals
/// the binding's; substituting `binding.jmap_mailbox_id` for it is
/// lossless. Pulled out as a helper because the Full and Paths scan
/// arms ran the same five lines back-to-back.
fn hydrate_known_state(
    conn: &Connection,
    binding: &MailboxFolderBinding,
) -> Result<HashMap<MaildirId, (JmapMailboxId, String)>> {
    let raw = queries::get_local_state_for_folder(conn, &binding.maildir_folder)?;
    let mailbox_id = binding
        .jmap_mailbox_id
        .expect_resolved("hydrate_known_state -- DB-derived state needs a resolved id")
        .clone();
    Ok(raw
        .into_iter()
        .map(|(id, (_folder, flags))| (id, (mailbox_id.clone(), flags)))
        .collect())
}

/// Build the three message_map projections the reconcile step consumes.
/// Free function rather than a `SyncEngine` method because it only
/// needs `&Connection` -- keeping it free lets the in-module unit tests
/// drive it from an in-memory DB without fabricating a JMAP client.
///
/// Lookup is keyed by `jmap_mailbox_id`, not `maildir_folder`: the id
/// is stable across a server-side rename whose `local_state` /
/// `message_map.maildir_folder` catch-up hasn't run yet, so a renamed
/// mailbox's in-flight cycle still finds its rows. Without this an
/// in-flight rename made every known row invisible to reconcile,
/// which then treated the freshly-walked files at the new path as
/// unbound and emitted redundant Adopt or Upload actions.
fn build_known_indices(
    conn: &Connection,
    mailboxes: &MailboxBindings,
) -> Result<MessageRecordIndex> {
    let mut idx = MessageRecordIndex::default();

    for binding in mailboxes.iter() {
        let mailbox_id = binding
            .jmap_mailbox_id
            .expect_resolved("build_known_indices -- live bindings must be resolved");
        let messages = queries::get_messages_by_jmap_mailbox_id(conn, mailbox_id)?;
        for msg in messages {
            // Wrap once; the three projections share via Arc
            // refcount instead of cloning the full record into two
            // of them. Per-cycle peak shrinks roughly 2.5x at the
            // big-mailbox limit because a record's heap data
            // (Strings, Options) is allocated once instead of three
            // times.
            let rec = Arc::new(msg);
            if let Some(ref mid) = rec.maildir_id {
                idx.by_maildir.insert(mid.clone(), Arc::clone(&rec));
            }
            idx.by_message_id
                .entry(rec.message_id.clone())
                .or_default()
                .push(Arc::clone(&rec));
            idx.by_jmap.insert(rec.jmap_email_id.clone(), rec);
        }
    }
    Ok(idx)
}

/// What `resolve_mailboxes` should do with one cached + server +
/// disk triple. Decided per mailbox before any DB write; the
/// caller then applies the cache upsert and bindings push in one
/// place so the on-disk view, the cache view, and the plan
/// emitted to reconcile all derive from the same decision.
#[derive(Debug)]
enum MailboxDecision {
    /// All three views agree on the folder (or the cache has no
    /// row yet and the server is the only signal). Nothing to
    /// emit; cache upsert advances `mailbox_map.maildir_folder`
    /// to the server-resolved value (a no-op when they already
    /// match).
    Unchanged,
    /// The cache has no row for this id -- first cycle the user
    /// has seen this mailbox. Emits `CreateLocalMailbox`.
    FirstCycle,
    /// Cache disagrees with server, disk follows cache. Server
    /// renamed the mailbox; emit `RenameLocalMailbox` to bring
    /// disk into agreement.
    ServerRename { from_folder: String },
    /// Cache and server agree, disk disagrees. User `mv`-ed the
    /// maildir locally; emit `RenameRemoteMailbox` to push the
    /// new name (and parent, if reparented) to the server.
    /// `from_folder` is the pre-rename cached path (the one the
    /// executor's `local_state` catch-up rewrites from).
    LocalRename {
        from_folder: String,
        new_name: String,
        new_parent_jmap_mailbox_id: Option<JmapMailboxId>,
        disk_folder: String,
    },
    /// Cache is stale but server and disk already agree on the
    /// new folder (a prior cycle's rename landed before the
    /// cache update could). Nothing to emit; the cache upsert
    /// catches up silently.
    CacheStale,
    /// All three views disagree, `conflict_strategy = ServerWins`.
    /// Server's rename wins; emit `RenameLocalMailbox` to rewrite
    /// disk from its locally-renamed location to the server's
    /// view. The user's local rename is overwritten.
    ConflictServerWins { from_folder: String },
    /// All three views disagree, `conflict_strategy = LocalWins`.
    /// User's local rename wins; emit `RenameRemoteMailbox` to
    /// push the disk's view to the server. The server's rename
    /// is overwritten. `from_folder` is the pre-rename cached
    /// path (see `LocalRename`).
    ConflictLocalWins {
        from_folder: String,
        new_name: String,
        new_parent_jmap_mailbox_id: Option<JmapMailboxId>,
        disk_folder: String,
    },
}

/// Per-cycle, per-mailbox-id inputs threaded into
/// `decide_mailbox_action` and the decompose helper.
/// `cached_by_folder` is the reverse index over the pre-cycle
/// `mailbox_map` snapshot that lets the rename push side
/// resolve a disk parent path back to a `JmapMailboxId` in O(1).
struct DecisionCtx<'a> {
    layout: &'a FolderLayoutDefinition,
    cached_by_folder: &'a HashMap<&'a str, &'a JmapMailboxId>,
    conflict_strategy: crate::config::ConflictStrategy,
}

/// Per-mailbox disk snapshot read by the shell pre-pass and
/// consumed by the Unchanged-arm's `needs_create` check inside
/// the pure decision loop. `maildir_present` tracks
/// `try_open_maildir`'s `Some`/`None` outcome at
/// `maildir_root.join(server_folder)`. `sentinel` carries the
/// parsed `.jma.mapping` content when the maildir was present,
/// or `None` either because the maildir was absent or because
/// the sentinel was missing / unparseable. Hoisting these into
/// a per-id map lets the decision loop stay free of I/O.
struct UnchangedDiskState {
    maildir_present: bool,
    sentinel: Option<crate::maildir_ops::sentinel::MailboxMapping>,
}

/// Bundle of every input the pure mailbox-resolution core reads.
/// The shell does all the I/O up front (JMAP `Mailbox/get`, the
/// `mailbox_map` SELECTs, the sentinel walk, per-mailbox disk
/// state, per-sentinel content) and hands this struct to
/// `compute_mailbox_resolution`. Keeping the inputs in one place
/// makes the shell/core boundary obvious: anything the core needs
/// that isn't in here is a static / pure value (e.g. layout,
/// sync config) passed as a separate argument.
struct MailboxesInput<'a> {
    remote_mailboxes: &'a [MailboxObject],
    cached_records: &'a HashMap<JmapMailboxId, queries::MailboxRecord>,
    disk_sentinels: &'a HashMap<JmapMailboxId, String>,
    unchanged_disk_states: &'a HashMap<JmapMailboxId, UnchangedDiskState>,
}

/// Join the parent's already-resolved server path with the leaf
/// `name`, falling back to the bare name when the parent is
/// absent or not yet in `remote_paths`. Shared between the
/// topological remote_paths prep-pass and the `LocalRename` /
/// `ConflictLocalWins` arm's post-rename `push_remote_path`
/// computation -- both perform the same join.
fn compose_remote_path(
    parent_jmap_mailbox_id: Option<&JmapMailboxId>,
    name: &str,
    remote_paths: &HashMap<JmapMailboxId, String>,
) -> String {
    match parent_jmap_mailbox_id.and_then(|p| remote_paths.get(p)) {
        Some(parent_path) => format!("{}/{}", parent_path, name),
        None => name.to_string(),
    }
}

/// Translate a `MailboxDecision` into the action-level builder
/// calls. The first match inside the per-mailbox loop already
/// emitted the per-id `MailboxRecord` (`push_mailbox_metadata_write`
/// or `push_pending_mailbox_write`); this helper handles the
/// second half so the loop body reads as "decide -> record ->
/// build binding -> dispatch."
fn dispatch_decision_to_builder(
    synced: &mut MailboxBindingsBuilder,
    decision: &MailboxDecision,
    mb: &MailboxObject,
    binding: &MailboxFolderBinding,
    cached_folder: Option<&str>,
    remote_paths: &HashMap<JmapMailboxId, String>,
    unchanged_disk_states: &HashMap<JmapMailboxId, UnchangedDiskState>,
) {
    use crate::sync::bindings::RenameDirection;
    match decision {
        MailboxDecision::FirstCycle => {
            synced.push_new_mailbox(binding.clone(), mb.parent_id.clone(), None);
        }
        MailboxDecision::ServerRename { from_folder } => {
            // Clean server rename: cache, server, disk all agreed
            // at cycle start. fs source = DB source = cached
            // folder = `from_folder`.
            synced.push_renamed_mailbox(
                RenameDirection::Pull,
                from_folder.clone(),
                from_folder.clone(),
                binding.clone(),
                mb.parent_id.clone(),
            );
        }
        MailboxDecision::ConflictServerWins { from_folder } => {
            // 3-way conflict, server wins. `from_folder` =
            // disk_folder (where the user mv'd to; fs::rename
            // reads from there). DB-source = `cached_folder`
            // (where `local_state` rows were last written -- the
            // user's `mv` didn't update them). The two diverge,
            // so the action carries both.
            let db_src = cached_folder
                .expect("ConflictServerWins requires a cached row")
                .to_string();
            synced.push_renamed_mailbox(
                RenameDirection::Pull,
                from_folder.clone(),
                db_src,
                binding.clone(),
                mb.parent_id.clone(),
            );
        }
        MailboxDecision::LocalRename {
            from_folder,
            new_name,
            new_parent_jmap_mailbox_id,
            disk_folder,
        }
        | MailboxDecision::ConflictLocalWins {
            from_folder,
            new_name,
            new_parent_jmap_mailbox_id,
            disk_folder,
        } => {
            // Push side: no fs::rename runs (the user already
            // `mv`-ed the maildir). fs source is moot; DB source
            // is the cached folder. Pass the cached folder in
            // both slots so the executor's
            // `rename_remote_mailboxes` reads it as the DB-rewrite
            // source via `action.from_folder`.
            let push_remote_path =
                compose_remote_path(new_parent_jmap_mailbox_id.as_ref(), new_name, remote_paths);
            synced.push_renamed_mailbox(
                RenameDirection::Push,
                from_folder.clone(),
                from_folder.clone(),
                MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value(mb.id.clone()),
                    server_name: new_name.clone(),
                    maildir_folder: disk_folder.clone(),
                    remote_path: push_remote_path,
                },
                new_parent_jmap_mailbox_id.clone(),
            );
        }
        MailboxDecision::Unchanged => {
            // Steady state per the 3-way diff, but disk may still
            // need attention. The reachable case is: maildir
            // present, sentinel missing or stale -- recreate so
            // the executor's idempotent `ensure_maildir +
            // sentinel::write` rebinds it. Absent-maildir bindings
            // flow through the scan-side
            // `LocalChange::LocalFolderDeleted` emission; see the
            // conversion loop in `run`.
            let disk_state = unchanged_disk_states
                .get(&mb.id)
                .expect("unchanged_disk_states populated for every synced mailbox above");
            if disk_state.maildir_present {
                let needs_create = match &disk_state.sentinel {
                    None => true,
                    Some(m) => {
                        m.jmap_mailbox_id != mb.id
                            || m.server_name != mb.name
                            || m.parent_jmap_mailbox_id != mb.parent_id
                    }
                };
                if needs_create {
                    synced.push_new_mailbox(binding.clone(), mb.parent_id.clone(), None);
                }
            }
        }
        MailboxDecision::CacheStale => {}
    }
}

/// Pure-decision core of `resolve_mailboxes`. Takes the upfront
/// I/O snapshot and produces a fully-populated
/// `MailboxBindingsBuilder`: the live binding set, the
/// new/renamed slots, and the `mailbox_metadata_writes` cache-
/// write slot that the engine's drain pass applies
/// (`apply_unconditional_mailbox_writes` before the executor).
/// The shell finalizes with `.build()`; no `mailbox_map` writes
/// happen in the shell.
fn compute_mailbox_resolution(
    input: &MailboxesInput<'_>,
    sync_config: &crate::config::SyncConfig,
    layout: &FolderLayoutDefinition,
) -> Result<MailboxBindingsBuilder> {
    let by_id: HashMap<JmapMailboxId, &MailboxObject> = input
        .remote_mailboxes
        .iter()
        .map(|mb| (mb.id.clone(), mb))
        .collect();
    let cached_by_folder: HashMap<&str, &JmapMailboxId> = input
        .cached_records
        .values()
        .map(|r| (r.maildir_folder.as_str(), &r.jmap_mailbox_id))
        .collect();
    let mut ordered: Vec<&MailboxObject> = input.remote_mailboxes.iter().collect();
    ordered.sort_by_key(|mb| parent_chain_depth(mb, &by_id));

    let mut remote_paths: HashMap<JmapMailboxId, String> = HashMap::new();
    for mb in &ordered {
        let path = compose_remote_path(mb.parent_id.as_ref(), &mb.name, &remote_paths);
        remote_paths.insert(mb.id.clone(), path);
    }

    let mut synced = MailboxBindings::builder();

    for mb in ordered {
        // Parent-aware filter: a config entry naming any ancestor
        // (or the mailbox itself) includes this mailbox. So
        // `mailboxes = ["[Airmail]"]` syncs `[Airmail]` plus every
        // descendant. Empty filter means "sync everything".
        if !jmap_mailbox::is_mailbox_synced(
            &sync_config.mailboxes,
            mb,
            &by_id,
            sync_config.case_insensitive_match,
        ) {
            continue;
        }

        // Translate the JMAP hierarchy to a single on-disk folder
        // name under the user-chosen layout. Defaults preserve the
        // pre-hierarchy behavior: Flat with `.` produces the leaf
        // name unchanged for depth-1 mailboxes.
        let server_folder = resolve_folder_path(mb, &by_id, layout)?;
        let cached_folder = input
            .cached_records
            .get(&mb.id)
            .map(|r| r.maildir_folder.as_str());
        let disk_folder = input.disk_sentinels.get(&mb.id).map(String::as_str);

        // remote_path was computed for every server-known
        // mailbox in the pre-pass above; the synced-set entry
        // must be there.
        let remote_path = remote_paths
            .get(&mb.id)
            .cloned()
            .expect("remote_paths populated for every server-known mailbox above");

        let path_was_ruled = layout.path_was_resolved_by_rule(mb, &by_id);
        let decision = decide_mailbox_action(
            mb,
            cached_folder,
            &server_folder,
            disk_folder,
            path_was_ruled,
            &DecisionCtx {
                layout,
                cached_by_folder: &cached_by_folder,
                conflict_strategy: sync_config.conflict_strategy,
            },
        );

        // Build the per-id `mailbox_map` row and route it to the
        // metadata-write slot. `apply_unconditional_mailbox_writes`
        // drains the slot before the executor runs, so all
        // server-winning outcomes land their cache row this
        // cycle. Local-rename arms emit no record at all: the
        // cache deliberately stays at the pre-rename triple
        // until `Mailbox/set { update }` lands; writing the
        // server's stale fields here would leave the cache
        // half-updated, and on `--dry-run` or per-action failure
        // the next cycle's sentinel walk re-emits the same
        // `RenameRemoteMailbox` to converge cleanly.
        let record = queries::MailboxRecord {
            jmap_mailbox_id: mb.id.clone(),
            name: mb.name.clone(),
            role: mb.role.clone(),
            parent_id: mb.parent_id.clone(),
            maildir_folder: server_folder.clone(),
            sort_order: mb.sort_order as i32,
            remote_path: Some(remote_path.clone()),
        };
        match &decision {
            MailboxDecision::FirstCycle
            | MailboxDecision::Unchanged
            | MailboxDecision::ServerRename { .. }
            | MailboxDecision::CacheStale
            | MailboxDecision::ConflictServerWins { .. } => {
                synced.push_mailbox_metadata_write(record);
            }
            MailboxDecision::LocalRename { .. } | MailboxDecision::ConflictLocalWins { .. } => {}
        }

        // The binding's `maildir_folder` is what scan walks
        // this cycle. For local-winning outcomes scan walks
        // the disk path (where the messages actually live);
        // for server-winning outcomes scan walks the
        // server-resolved path (which the rename phase will
        // populate, or is already populated for the
        // unchanged/first-cycle cases).
        //
        // For LocalRename / ConflictLocalWins, the binding
        // is internally split across server-side and disk-
        // side views by design: `maildir_folder = disk_folder`
        // (post-rename disk path, used by scan) while
        // `server_name = mb.name` and `remote_path` are the
        // pre-rename server view (`Mailbox/set { update }`
        // hasn't pushed yet; the cache stays anchored to the
        // pre-rename triple per the local-wins skip above).
        // No live consumer reads `server_name` on the
        // LocalRename code path; the split is harmless today
        // and converges after the executor's rename phase
        // lands the JMAP push.
        let walk_folder = match &decision {
            MailboxDecision::LocalRename { disk_folder, .. }
            | MailboxDecision::ConflictLocalWins { disk_folder, .. } => disk_folder.clone(),
            _ => server_folder.clone(),
        };
        let binding = MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value(mb.id.clone()),
            server_name: mb.name.clone(),
            maildir_folder: walk_folder,
            remote_path: remote_path.clone(),
        };

        dispatch_decision_to_builder(
            &mut synced,
            &decision,
            mb,
            &binding,
            cached_folder,
            &remote_paths,
            input.unchanged_disk_states,
        );

        synced.insert(binding);
    }

    Ok(synced)
}

/// Three-way diff over `(cached_folder, server_folder, disk_folder)`
/// returning the action `resolve_mailboxes` should apply for this
/// mailbox. Pure -- no I/O, no DB writes -- so the caller can apply
/// the cache upsert and the bindings push together based on the
/// returned decision.
///
/// When the local-rename path would require deriving a new
/// `(name, parent_id)` pair from a disk path that the configured
/// layout cannot decompose, or whose decomposed parent path
/// doesn't resolve to a cached mailbox id, the decision falls
/// back to `Unchanged` (silently logging the reason) rather than
/// emitting a `RenameRemoteMailbox` the executor would reject:
/// the next cycle's sentinel walk picks the same configuration
/// back up, so the failure stays surfaced as drift rather than
/// vanishing into a one-shot warn.
fn decide_mailbox_action(
    mb: &MailboxObject,
    cached_folder: Option<&str>,
    server_folder: &str,
    disk_folder: Option<&str>,
    path_was_ruled: bool,
    ctx: &DecisionCtx<'_>,
) -> MailboxDecision {
    use crate::config::ConflictStrategy::ServerWins;

    // No cached row -> first-cycle case; nothing else to compare.
    let Some(cached_folder) = cached_folder else {
        return MailboxDecision::FirstCycle;
    };

    let cache_vs_server_diverges = cached_folder != server_folder;
    let disk_diverges = disk_folder.is_some_and(|d| d != cached_folder);

    match (cache_vs_server_diverges, disk_diverges, disk_folder) {
        // All views agree, or disk has no sentinel (e.g. user
        // hasn't pulled this mailbox yet on this machine).
        (false, false, _) => MailboxDecision::Unchanged,

        // Server renamed; disk still at cached path.
        (true, false, _) => MailboxDecision::ServerRename {
            from_folder: cached_folder.to_string(),
        },

        // Disk diverges from cache; resolve who else moved.
        // Pattern uses `_` so `cache_vs_server_diverges` inside
        // the body refers to the outer let-binding, not a
        // pattern-shadowed fresh `bool` (which would compile but
        // mislead readers into thinking the match guards on it).
        (_, true, Some(disk)) => {
            // Cache stale + disk caught up + server caught up
            // (a prior cycle's rename completed but the cache
            // update didn't): all three end up with disk ==
            // server, cache lagging. Silent recovery.
            if cache_vs_server_diverges && disk == server_folder {
                return MailboxDecision::CacheStale;
            }

            // ConflictServerWins skips decomposition entirely --
            // it only needs the disk path to know where to
            // fs::rename from, and the server's resolved name is
            // already correct. Branch before the decompose so a
            // rule-mapped or layout-undecomposable disk path
            // still recovers via the executor's
            // `RenameLocalMailbox`.
            if cache_vs_server_diverges && matches!(ctx.conflict_strategy, ServerWins) {
                return MailboxDecision::ConflictServerWins {
                    from_folder: disk.to_string(),
                };
            }

            // Local-winning outcomes (LocalRename + LocalWins)
            // both push the disk's view back to the server, so
            // both need (name, parent_id) decomposed from the
            // disk path. Rule-mapped mailboxes refuse here: the
            // disk path was produced by a rename rule, and the
            // rule's inverse is not generally computable;
            // pushing a naive decomposition would silently
            // overwrite the JMAP-side name with whatever the
            // user typed in place of the rule's output, fighting
            // the rule on every subsequent cycle.
            if path_was_ruled {
                warn!(
                    "mailbox {} disk path {} is produced by a rename rule; \
                     refusing to push the disk-vs-cache divergence (the rule's \
                     inverse is not reliably computable, so a push could silently \
                     overwrite the JMAP-side name). The guard fires for any \
                     rule-mapped path whose disk view diverges from cache -- \
                     user-initiated rename, partial-failure recovery, fresh \
                     rule applied this cycle, etc.",
                    mb.id, disk
                );
                return MailboxDecision::Unchanged;
            }

            let (new_name, parent_id) =
                match decompose_disk_path_to_jmap(disk, ctx.layout, ctx.cached_by_folder) {
                    Ok(decomposed) => decomposed,
                    Err(e) => {
                        debug!(
                            "mailbox {} disk path {} does not decompose to a \
                             JMAP (name, parent_id) pair: {}; deferring to \
                             next cycle (self-healing if disk or cache catches up)",
                            mb.id, disk, e
                        );
                        return MailboxDecision::Unchanged;
                    }
                };

            if !cache_vs_server_diverges {
                MailboxDecision::LocalRename {
                    from_folder: cached_folder.to_string(),
                    new_name,
                    new_parent_jmap_mailbox_id: parent_id,
                    disk_folder: disk.to_string(),
                }
            } else {
                MailboxDecision::ConflictLocalWins {
                    from_folder: cached_folder.to_string(),
                    new_name,
                    new_parent_jmap_mailbox_id: parent_id,
                    disk_folder: disk.to_string(),
                }
            }
        }

        // disk_diverges = disk_folder.is_some_and(...) so the
        // pattern (_, true, None) is unreachable.
        (_, true, None) => unreachable!("disk_diverges true requires Some(disk_folder)"),
    }
}

/// Decompose `disk_folder` (relative to `maildir_root`) into the
/// JMAP (`name`, `parent_id`) pair `Mailbox/set { update }` needs.
/// The leaf segment is the new `name`; the parent path is
/// resolved back to a cached `jmap_mailbox_id` via the
/// `mailbox_map` cache. An untracked parent path (e.g. user nested
/// under a folder that no mailbox_map row points at) is rejected
/// with an actionable error; the caller surfaces it as drift.
fn decompose_disk_path_to_jmap(
    disk_folder: &str,
    layout_definition: &FolderLayoutDefinition,
    cached_by_folder: &HashMap<&str, &JmapMailboxId>,
) -> Result<(String, Option<JmapMailboxId>)> {
    let (leaf, parent_path) = decompose_folder_string(
        disk_folder,
        layout_definition.layout(),
        layout_definition.separator(),
    );
    if leaf.is_empty() {
        anyhow::bail!("disk folder {:?} decomposed to empty leaf", disk_folder);
    }
    let Some(parent_path) = parent_path else {
        return Ok((leaf, None));
    };
    let parent_id = cached_by_folder
        .get(parent_path.as_str())
        .map(|id| (*id).clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "disk parent path {:?} does not match any cached mailbox_map row",
                parent_path
            )
        })?;
    Ok((leaf, Some(parent_id)))
}

/// Walk `maildir_root` for every directory containing a
/// `.jma.mapping` sentinel, returning a `jmap_mailbox_id -> folder
/// path (relative to maildir_root)` map. The walk skips
/// `cur`/`new`/`tmp` (maildir-internal leaves) and `.jma.*` (jma's
/// own metadata) so it terminates on a deep tree without
/// re-reading maildir contents. Read errors are logged at debug
/// and skipped: a single broken folder shouldn't bring the cycle
/// down, and the next janitor pass will surface it.
fn walk_sentinels(maildir_root: &std::path::Path) -> HashMap<JmapMailboxId, String> {
    let mut out = HashMap::new();
    walk_sentinels_inner(maildir_root, maildir_root, &mut out);
    out
}

fn walk_sentinels_inner(
    root: &std::path::Path,
    dir: &std::path::Path,
    out: &mut HashMap<JmapMailboxId, String>,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if is_jma_private(name_str.as_ref()) {
            continue;
        }
        if name_str == "cur" || name_str == "new" || name_str == "tmp" {
            continue;
        }
        let path = entry.path();
        if path.join("cur").is_dir() {
            match crate::maildir_ops::sentinel::read(&path) {
                Ok(Some(mapping)) => {
                    let rel = match path.strip_prefix(root) {
                        Ok(p) => p.to_string_lossy().into_owned(),
                        Err(_) => continue,
                    };
                    out.insert(mapping.jmap_mailbox_id, rel);
                }
                Ok(None) => {}
                Err(e) => {
                    debug!(
                        "sentinel walk: read failed at {} ({}); skipping",
                        path.display(),
                        e
                    );
                }
            }
        }
        walk_sentinels_inner(root, &path, out);
    }
}

fn log_dropped(direction: SyncDirection, dropped: &[SyncAction]) {
    for a in dropped {
        match a {
            SyncAction::DownloadMessage { id, binding, .. } => warn!(
                "{:?}: dropped DownloadMessage {} -> {}",
                direction, id, binding.maildir_folder
            ),
            SyncAction::UpdateLocalFlags { id, new_flags, .. } => warn!(
                "{:?}: dropped UpdateLocalFlags on {} -> '{}'",
                direction, id, new_flags
            ),
            SyncAction::DeleteLocal { id, .. } => {
                warn!("{:?}: dropped DeleteLocal {}", direction, id)
            }
            SyncAction::MoveLocal {
                id,
                from_folder,
                to_binding,
            } => warn!(
                "{:?}: dropped MoveLocal {} {} -> {}",
                direction, id, from_folder, to_binding.maildir_folder
            ),
            SyncAction::UploadMessage { id, binding, .. } => warn!(
                "{:?}: dropped UploadMessage {} from {}",
                direction, id, binding.maildir_folder
            ),
            SyncAction::UpdateRemoteKeywords { id, .. } => {
                warn!("{:?}: dropped UpdateRemoteKeywords on {}", direction, id)
            }
            SyncAction::DestroyRemote { id } => {
                warn!("{:?}: dropped DestroyRemote {}", direction, id)
            }
            SyncAction::MoveRemote {
                id,
                from_folder,
                to_folder,
                ..
            } => warn!(
                "{:?}: dropped MoveRemote {} ({} -> {})",
                direction, id, from_folder, to_folder
            ),
            SyncAction::CreateLocalMailbox { binding, .. } => warn!(
                "{:?}: dropped CreateLocalMailbox {}/",
                direction, binding.maildir_folder
            ),
            SyncAction::RenameLocalMailbox {
                from_folder,
                binding,
                ..
            } => warn!(
                "{:?}: dropped RenameLocalMailbox {}/ -> {}/",
                direction, from_folder, binding.maildir_folder
            ),
            SyncAction::CreateRemoteMailbox { name, .. } => {
                warn!("{:?}: dropped CreateRemoteMailbox {:?}", direction, name)
            }
            SyncAction::RenameRemoteMailbox {
                from_folder,
                binding,
                ..
            } => warn!(
                "{:?}: dropped RenameRemoteMailbox {} -> {:?} (disk: {}/ -> {}/)",
                direction,
                binding.jmap_mailbox_id,
                binding.remote_path,
                from_folder,
                binding.maildir_folder
            ),
            // Adoption is always kept; it never appears here.
            SyncAction::AdoptLocalMessage { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{JmapEmailId, MaildirId, MessageId};
    use crate::state::db;
    use crate::state::queries::MessageRecord;

    fn record(
        jmap_id: &str,
        mailbox_id: &str,
        maildir_id: Option<&str>,
        message_id: &str,
    ) -> MessageRecord {
        MessageRecord {
            jmap_email_id: jmap_id.into(),
            jmap_blob_id: Some(format!("blob-{jmap_id}").into()),
            jmap_thread_id: Some(format!("thr-{jmap_id}").into()),
            jmap_mailbox_id: mailbox_id.into(),
            maildir_id: maildir_id.map(Into::into),
            message_id: message_id.into(),
            flags: "S".into(),
            jmap_keywords: r#"{"$seen":true}"#.into(),
        }
    }

    fn seed(conn: &Connection, records: &[MessageRecord]) {
        for r in records {
            queries::upsert_message(conn, r).unwrap();
        }
    }

    fn mailboxes(folders: &[(&str, &str)]) -> MailboxBindings {
        let mut b = MailboxBindings::builder();
        for (id, folder) in folders {
            b.insert(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Value((*id).into()),
                server_name: (*folder).to_string(),
                maildir_folder: (*folder).to_string(),
                remote_path: (*folder).to_string(),
            });
        }
        b.build()
    }

    /// No mailboxes -> no rows queried -> all three projections empty.
    #[test]
    fn build_known_indices_with_no_mailboxes_returns_empty() {
        let conn = db::open_in_memory().unwrap();
        seed(&conn, &[record("E1", "MB-INBOX", Some("M1"), "<a@x>")]);

        let idx = build_known_indices(&conn, &MailboxBindings::builder().build()).unwrap();

        assert!(idx.by_jmap.is_empty());
        assert!(idx.by_maildir.is_empty());
        assert!(idx.by_message_id.is_empty());
    }

    /// A single fully-bound row appears in all three projections, keyed
    /// on its respective identifier. The `Vec` in `by_message_id` holds
    /// exactly one entry.
    #[test]
    fn build_known_indices_single_record_in_all_three_projections() {
        let conn = db::open_in_memory().unwrap();
        seed(&conn, &[record("E1", "MB-INBOX", Some("M1"), "<a@x>")]);

        let idx = build_known_indices(&conn, &mailboxes(&[("MB-INBOX", "INBOX")])).unwrap();

        assert_eq!(idx.by_jmap.len(), 1);
        assert_eq!(idx.by_maildir.len(), 1);
        assert_eq!(idx.by_message_id.len(), 1);
        assert!(idx.by_jmap.contains_key(&JmapEmailId::from("E1")));
        assert!(idx.by_maildir.contains_key(&MaildirId::from("M1")));
        let bucket = idx
            .by_message_id
            .get(&MessageId::from("<a@x>"))
            .expect("message_id must index the row");
        assert_eq!(bucket.len(), 1);
    }

    /// A row whose `maildir_id` is NULL (e.g. mid-flight before adoption
    /// landed the maildir binding) is invisible to `by_maildir` but still
    /// indexed by jmap id and Message-ID. Matches the explicit `if let
    /// Some(ref mid)` guard in build_known_indices.
    #[test]
    fn build_known_indices_skips_by_maildir_when_maildir_id_is_none() {
        let conn = db::open_in_memory().unwrap();
        seed(&conn, &[record("E1", "MB-INBOX", None, "<a@x>")]);

        let idx = build_known_indices(&conn, &mailboxes(&[("MB-INBOX", "INBOX")])).unwrap();

        assert_eq!(idx.by_jmap.len(), 1);
        assert!(idx.by_maildir.is_empty());
        assert_eq!(
            idx.by_message_id
                .get(&MessageId::from("<a@x>"))
                .map(Vec::len),
            Some(1)
        );
    }

    /// Same Message-ID in two different folders (legitimate: same email
    /// delivered to Inbox and a Sent/thread folder) collapses to two
    /// entries in `by_message_id` while staying 1:1 in `by_jmap` and
    /// `by_maildir`. This is the 1:N invariant the engine relies on for
    /// post-DB-wipe adoption.
    #[test]
    fn build_known_indices_groups_same_message_id_across_folders() {
        let conn = db::open_in_memory().unwrap();
        seed(
            &conn,
            &[
                record("E1", "MB-INBOX", Some("M1"), "<a@x>"),
                record("E2", "MB-ARCH", Some("M2"), "<a@x>"),
            ],
        );

        let idx = build_known_indices(
            &conn,
            &mailboxes(&[("MB-INBOX", "INBOX"), ("MB-ARCH", "Archive")]),
        )
        .unwrap();

        assert_eq!(idx.by_jmap.len(), 2);
        assert_eq!(idx.by_maildir.len(), 2);
        let bucket = idx
            .by_message_id
            .get(&MessageId::from("<a@x>"))
            .expect("both rows must group under the shared Message-ID");
        assert_eq!(bucket.len(), 2);
        let jmap_ids: HashSet<&JmapEmailId> = bucket.iter().map(|r| &r.jmap_email_id).collect();
        assert!(jmap_ids.contains(&JmapEmailId::from("E1")));
        assert!(jmap_ids.contains(&JmapEmailId::from("E2")));
    }

    /// `mailboxes` is the binding filter: rows whose `jmap_mailbox_id`
    /// isn't in the listed bindings are not indexed.
    /// `get_messages_by_jmap_mailbox_id` queries only the listed
    /// mailboxes; nothing enumerates the table. Pinning this prevents
    /// a refactor from accidentally widening the scope to "every row
    /// in message_map".
    #[test]
    fn build_known_indices_only_indexes_listed_folders() {
        let conn = db::open_in_memory().unwrap();
        seed(
            &conn,
            &[
                record("E1", "MB-INBOX", Some("M1"), "<a@x>"),
                record("E2", "MB-ARCH", Some("M2"), "<b@x>"),
                record("E3", "MB-SPAM", Some("M3"), "<c@x>"),
            ],
        );

        // Only INBOX listed; Archive and Spam rows must be invisible.
        let idx = build_known_indices(&conn, &mailboxes(&[("MB-INBOX", "INBOX")])).unwrap();

        assert_eq!(idx.by_jmap.len(), 1);
        assert!(idx.by_jmap.contains_key(&JmapEmailId::from("E1")));
        assert!(!idx.by_jmap.contains_key(&JmapEmailId::from("E2")));
        assert!(!idx.by_jmap.contains_key(&JmapEmailId::from("E3")));
    }

    /// All three projections hold the *same* `Arc<MessageRecord>` for a
    /// given row (refcount-shared, not three independent clones). This
    /// is the documented memory-saving contract: a record's heap data
    /// is allocated once instead of three times.
    #[test]
    fn build_known_indices_shares_arc_across_projections() {
        let conn = db::open_in_memory().unwrap();
        seed(&conn, &[record("E1", "MB-INBOX", Some("M1"), "<a@x>")]);

        let idx = build_known_indices(&conn, &mailboxes(&[("MB-INBOX", "INBOX")])).unwrap();

        let from_jmap = idx.by_jmap.get(&JmapEmailId::from("E1")).unwrap();
        let from_maildir = idx.by_maildir.get(&MaildirId::from("M1")).unwrap();
        let from_msgid = &idx.by_message_id.get(&MessageId::from("<a@x>")).unwrap()[0];

        assert!(
            Arc::ptr_eq(from_jmap, from_maildir),
            "by_jmap and by_maildir must share the same Arc"
        );
        assert!(
            Arc::ptr_eq(from_jmap, from_msgid),
            "by_jmap and by_message_id must share the same Arc"
        );
    }

    mod folder_checkpoint {
        use super::*;
        use std::fs;
        use tempfile::tempdir;

        fn make_maildir(root: &std::path::Path, folder: &str) -> std::path::PathBuf {
            let path = root.join(folder);
            crate::maildir_ops::store::ensure_maildir(&path).unwrap();
            path
        }

        /// First-ever cycle: no `folder_checkpoint` rows exist, so
        /// every synced folder is dirty. This is what unlocks the
        /// initial dedupe walk that populates `LocalIndex` for the
        /// recovery-from-empty-DB case.
        #[test]
        fn compute_dirty_folders_treats_missing_row_as_dirty() {
            let conn = db::open_in_memory().unwrap();
            let dir = tempdir().unwrap();
            make_maildir(dir.path(), "INBOX");
            make_maildir(dir.path(), "Archive");

            let dirty = compute_dirty_folders(
                &conn,
                dir.path(),
                &["INBOX".to_string(), "Archive".to_string()],
            )
            .unwrap();

            assert_eq!(dirty.len(), 2);
            assert!(dirty.contains(&"INBOX".to_string()));
            assert!(dirty.contains(&"Archive".to_string()));
        }

        /// After `record_folder_checkpoints` has captured the
        /// current state, an unchanged folder is clean on the next
        /// cycle -- no walk, no Message-ID parsing.
        #[test]
        fn compute_dirty_folders_skips_folder_matching_checkpoint() {
            let conn = db::open_in_memory().unwrap();
            let dir = tempdir().unwrap();
            let folder_path = make_maildir(dir.path(), "INBOX");
            fs::write(folder_path.join("cur").join("1234.host:2,S"), b"body").unwrap();

            record_folder_checkpoints(&conn, dir.path(), &["INBOX".to_string()]).unwrap();

            let dirty = compute_dirty_folders(&conn, dir.path(), &["INBOX".to_string()]).unwrap();
            assert!(dirty.is_empty(), "checkpoint matches; folder must be clean");
        }

        /// Adding a file after the checkpoint moves count past the
        /// recorded value, which forces dirty even if mtime
        /// somehow held steady. Count is the structural guardrail.
        #[test]
        fn compute_dirty_folders_flags_count_change() {
            let conn = db::open_in_memory().unwrap();
            let dir = tempdir().unwrap();
            let folder_path = make_maildir(dir.path(), "INBOX");
            record_folder_checkpoints(&conn, dir.path(), &["INBOX".to_string()]).unwrap();

            fs::write(folder_path.join("new").join("9999.host:2,"), b"new body").unwrap();

            let dirty = compute_dirty_folders(&conn, dir.path(), &["INBOX".to_string()]).unwrap();
            assert_eq!(dirty, vec!["INBOX".to_string()]);
        }

        /// Each call overwrites the prior row -- the checkpoint
        /// is a snapshot, not a log. Concretely, recording an
        /// older state then a newer state must leave the newer
        /// state's `cur_count` in the row.
        #[test]
        fn record_folder_checkpoints_upserts_in_place() {
            let conn = db::open_in_memory().unwrap();
            let dir = tempdir().unwrap();
            let folder_path = make_maildir(dir.path(), "INBOX");

            record_folder_checkpoints(&conn, dir.path(), &["INBOX".to_string()]).unwrap();
            fs::write(folder_path.join("cur").join("1.host:2,"), b"x").unwrap();
            record_folder_checkpoints(&conn, dir.path(), &["INBOX".to_string()]).unwrap();

            let row = queries::get_folder_checkpoint(&conn, "INBOX")
                .unwrap()
                .unwrap();
            assert_eq!(row.cur_count, 1);
        }
    }
}
