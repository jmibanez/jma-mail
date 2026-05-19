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
    types::{EmailObject, MailboxObject, SessionInfo},
};
use crate::maildir_ops::layout::{FolderLayoutDefinition, resolve_folder_path};
use crate::maildir_ops::scan::LocalChange;
use crate::maildir_ops::{scan, store};
use crate::state::queries;
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
        let folder_names: Vec<String> = mailboxes.iter().map(|(_, f)| f.clone()).collect();
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
                    for (_, folder_name) in &mailboxes {
                        let maildir_path = maildir_root.join(folder_name);
                        let maildir = store::ensure_maildir(&maildir_path)?;
                        let known_state =
                            queries::get_local_state_for_folder(self.conn, folder_name)?;
                        let result = scan::scan_folder(&maildir, folder_name, &known_state)?;
                        changes.extend(result.changes);
                        local_flags.extend(result.local_flags);
                    }
                    (changes, local_flags)
                }
                ScanScope::Paths(paths) => {
                    let mut known_states = HashMap::new();
                    for (_, folder_name) in &mailboxes {
                        // Make sure the maildir on disk exists, matching
                        // the side-effect the Full path used to provide;
                        // some downstream code assumes the directory tree
                        // is in place.
                        store::ensure_maildir(&maildir_root.join(folder_name))?;
                        let state = queries::get_local_state_for_folder(self.conn, folder_name)?;
                        known_states.insert(folder_name.clone(), state);
                    }
                    let result = scan::scan_paths(&maildir_root, paths, &known_states)?;
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
                            folder, maildir_id, ..
                        }
                        | LocalChange::FlagsChanged {
                            folder, maildir_id, ..
                        }
                        | LocalChange::DeletedMessage { folder, maildir_id } => {
                            (folder.clone(), maildir_id.clone())
                        }
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
                strategy: self.config.sync.conflict_strategy,
                new_email_state: Some(new_state),
                max_upload_size: limits::max_size_upload(&self.client),
                used_initial_path,
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

    /// Resolve the list of mailboxes to sync, returning (jmap_id, folder_name) pairs.
    pub async fn resolve_mailboxes(&self) -> Result<Vec<(JmapMailboxId, String)>> {
        let remote_mailboxes = jmap_mailbox::get_all(&self.client).await?;

        // Index by id so the filter and the folder-path resolver can
        // walk the parent chain.
        let by_id: HashMap<JmapMailboxId, &MailboxObject> = remote_mailboxes
            .iter()
            .map(|mb| (mb.id.clone(), mb))
            .collect();

        let name_cap = limits::max_size_mailbox_name(&self.client);
        let mut synced = Vec::new();
        let layout_definition = FolderLayoutDefinition::from_config(self.config, name_cap);

        for mb in &remote_mailboxes {
            // Parent-aware filter: a config entry naming any ancestor
            // (or the mailbox itself) includes this mailbox. So
            // `mailboxes = ["[Airmail]"]` syncs `[Airmail]` plus every
            // descendant. Empty filter means "sync everything".
            if !jmap_mailbox::is_mailbox_synced(
                &self.config.sync.mailboxes,
                mb,
                &by_id,
                self.config.sync.case_insensitive_match,
            ) {
                continue;
            }

            // Translate the JMAP hierarchy to a single on-disk folder
            // name under the user-chosen layout. Defaults preserve the
            // pre-hierarchy behavior: Flat with `.` produces the leaf
            // name unchanged for depth-1 mailboxes.
            let folder_name = resolve_folder_path(mb, &by_id, &layout_definition)?;

            // Store in DB
            queries::upsert_mailbox(
                self.conn,
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
            let maildir_path = self.config.maildir_path().join(&folder_name);
            store::ensure_maildir(&maildir_path)?;

            synced.push((mb.id.clone(), folder_name));
        }

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
        mailboxes: &[(JmapMailboxId, String)],
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
        mailboxes: &[(JmapMailboxId, String)],
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
        let futures = mailboxes
            .iter()
            .map(|(mailbox_id, folder_name)| async move {
                jmap_email::query_mailbox(client, mailbox_id.as_ref(), folder_name, n).await
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
        // ensure_maildir runs again in Phase 1 (scan), but snapshot
        // needs cur/ and new/ to exist now. The call is cheap and
        // idempotent.
        crate::maildir_ops::store::ensure_maildir(&path)?;
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
        let snapshot = crate::maildir_ops::snapshot::snapshot_folder(&path)?;
        queries::upsert_folder_checkpoint(conn, folder, &snapshot)?;
    }
    Ok(())
}

/// Build the three message_map projections the reconcile step consumes.
/// Free function rather than a `SyncEngine` method because it only
/// needs `&Connection` -- keeping it free lets the in-module unit tests
/// drive it from an in-memory DB without fabricating a JMAP client.
fn build_known_indices(
    conn: &Connection,
    mailboxes: &[(JmapMailboxId, String)],
) -> Result<MessageRecordIndex> {
    let mut idx = MessageRecordIndex::default();

    for (_, folder_name) in mailboxes {
        let messages = queries::get_messages_by_folder(conn, folder_name)?;
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

fn log_dropped(direction: SyncDirection, dropped: &[SyncAction]) {
    for a in dropped {
        match a {
            SyncAction::DownloadMessage {
                id, maildir_folder, ..
            } => warn!(
                "{:?}: dropped DownloadMessage {} -> {}",
                direction, id, maildir_folder
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
                to_folder,
            } => warn!(
                "{:?}: dropped MoveLocal {} {} -> {}",
                direction, id, from_folder, to_folder
            ),
            SyncAction::UploadMessage {
                id, maildir_folder, ..
            } => warn!(
                "{:?}: dropped UploadMessage {} from {}",
                direction, id, maildir_folder
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
        folder: &str,
        maildir_id: Option<&str>,
        message_id: &str,
    ) -> MessageRecord {
        MessageRecord {
            jmap_email_id: jmap_id.into(),
            jmap_blob_id: Some(format!("blob-{jmap_id}").into()),
            jmap_thread_id: Some(format!("thr-{jmap_id}").into()),
            mailbox_id: mailbox_id.into(),
            maildir_id: maildir_id.map(Into::into),
            maildir_folder: Some(folder.into()),
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

    fn mailboxes(folders: &[(&str, &str)]) -> Vec<(JmapMailboxId, String)> {
        folders
            .iter()
            .map(|(id, folder)| ((*id).into(), (*folder).to_string()))
            .collect()
    }

    /// No mailboxes -> no rows queried -> all three projections empty.
    #[test]
    fn build_known_indices_with_no_mailboxes_returns_empty() {
        let conn = db::open_in_memory().unwrap();
        seed(
            &conn,
            &[record("E1", "MB-INBOX", "INBOX", Some("M1"), "<a@x>")],
        );

        let idx = build_known_indices(&conn, &[]).unwrap();

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
        seed(
            &conn,
            &[record("E1", "MB-INBOX", "INBOX", Some("M1"), "<a@x>")],
        );

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
        seed(&conn, &[record("E1", "MB-INBOX", "INBOX", None, "<a@x>")]);

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
                record("E1", "MB-INBOX", "INBOX", Some("M1"), "<a@x>"),
                record("E2", "MB-ARCH", "Archive", Some("M2"), "<a@x>"),
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

    /// `mailboxes` is the folder filter: rows whose `maildir_folder`
    /// isn't in the listed folders are not indexed. `get_messages_by_folder`
    /// queries only the listed folders; nothing enumerates the table.
    /// Pinning this prevents a refactor from accidentally widening the
    /// scope to "every row in message_map".
    #[test]
    fn build_known_indices_only_indexes_listed_folders() {
        let conn = db::open_in_memory().unwrap();
        seed(
            &conn,
            &[
                record("E1", "MB-INBOX", "INBOX", Some("M1"), "<a@x>"),
                record("E2", "MB-ARCH", "Archive", Some("M2"), "<b@x>"),
                record("E3", "MB-SPAM", "Spam", Some("M3"), "<c@x>"),
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
        seed(
            &conn,
            &[record("E1", "MB-INBOX", "INBOX", Some("M1"), "<a@x>")],
        );

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
