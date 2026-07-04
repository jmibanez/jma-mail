use anyhow::Result;
use futures_util::stream::{self, StreamExt};
use jmap_client::client::Client;
use maildir::TemporaryMailFile;
use reqwest::Client as HttpClient;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{Instrument, debug, info, warn};

use crate::config::Config;
use crate::domain::{MailboxFolderBinding, MaybeReference};
use crate::ids::{JmapAccountId, JmapEmailId, JmapMailboxId};
use crate::jmap::email::{self as jmap_email, EmailSetOp};
use crate::jmap::limits;
use crate::jmap::retry::is_transient_error;
use crate::maildir_ops::{flags::keywords_to_flags, store};
use crate::state::queries::{self, MessageRecord};
use crate::sync::engine::SyncOutcome;
use crate::sync::plan::{BoundId, LocalId, RemoteId, SyncAction, SyncPlan};
use crate::sync::self_writes::SelfWriteCache;

/// Above this many queued downloads, demote the per-message `info!`
/// line to `debug!` and switch to a periodic `info!` heartbeat. Keeps
/// initial-sync logs scannable: a 50K-message first sync produces
/// roughly one progress line per minute instead of 50K per-message
/// lines, while small daemon-cycle batches stay verbose.
const VERBOSE_DOWNLOAD_THRESHOLD: usize = 100;

/// Wall-clock interval between progress heartbeats once per-message
/// logging is suppressed. Short enough to feel live during a long
/// initial sync without flooding the log.
const DOWNLOAD_PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

/// Per-cycle download progress state. Shared between batches inside
/// `download_messages` so the heartbeat clock and verbose-mode
/// decision carry over the rate-limit-driven concurrency halving.
struct DownloadProgress {
    downloaded: usize,
    total: usize,
    verbose_per_message: bool,
    last_progress: Instant,
}

/// Outcome of one parallel download batch. Drives the retry-with-
/// halved-concurrency loop in `download_messages`: `to_retry` is
/// whatever the batch couldn't land, and `rate_limited` decides
/// whether to halve before the next pass.
struct DownloadBatchOutcome {
    to_retry: Vec<SyncAction>,
    rate_limited: bool,
}

/// Bundles the immutable per-cycle state every execute helper threads
/// through (client, DB connection, config, maildir root, account id).
/// The struct lets us add another shared field without touching every
/// helper signature; helpers that genuinely need only one of these
/// (e.g. the pure-DB adopt commits) stay as free functions and take
/// just what they use.
pub struct Executor<'a> {
    client: Arc<Client>,
    conn: &'a Connection,
    config: &'a Config,
    maildir_root: PathBuf,
    account_id: JmapAccountId,
    /// Optional self-write cache. Set by the daemon (and shared
    /// with the watcher) so disk writes here suppress the
    /// corresponding fsevents echoes; left None for one-shot CLI
    /// commands that have no watcher to feed.
    self_writes: Option<Arc<SelfWriteCache>>,
}

impl<'a> Executor<'a> {
    pub fn new(
        client: Arc<Client>,
        conn: &'a Connection,
        config: &'a Config,
        self_writes: Option<Arc<SelfWriteCache>>,
    ) -> Result<Self> {
        let account_id: JmapAccountId = client.default_account_id().into();
        let maildir_root = config.canonical_maildir_root()?;
        Ok(Self {
            client,
            conn,
            config,
            maildir_root,
            account_id,
            self_writes,
        })
    }

    /// Walk a SyncPlan in dependency order:
    /// adopt -> download -> local-flags -> local-move -> local-delete ->
    /// delete-local-folder -> upload -> remote-keywords -> remote-move ->
    /// remote-destroy.
    ///
    /// Adoptions run first so subsequent actions on the same maildir_id /
    /// jmap_email_id see the binding. Server-side mutations come last so
    /// pull-side state is settled before we report it back.
    pub async fn execute(&self, plan: SyncPlan) -> Result<SyncOutcome> {
        let SyncPlan {
            actions,
            new_email_state,
        } = plan;

        let mut remote_mailbox_destroys = Vec::new();
        let mut local_creates = Vec::new();
        let mut local_renames = Vec::new();
        let mut unconditional_adopts = Vec::new();
        let mut move_pair_adopts = Vec::new();
        let mut downloads = Vec::new();
        let mut local_flags = Vec::new();
        let mut local_moves = Vec::new();
        let mut local_deletes = Vec::new();
        let mut local_folder_deletes = Vec::new();
        let mut remote_creates = Vec::new();
        let mut remote_renames = Vec::new();
        let mut uploads = Vec::new();
        let mut remote_keywords = Vec::new();
        let mut remote_moves = Vec::new();
        let mut remote_destroys = Vec::new();

        for action in actions {
            match action {
                SyncAction::CreateLocalMailbox { .. } => local_creates.push(action),
                SyncAction::RenameLocalMailbox { .. } => local_renames.push(action),
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
                SyncAction::DeleteLocalFolder { .. } => local_folder_deletes.push(action),
                SyncAction::CreateRemoteMailbox { .. } => remote_creates.push(action),
                SyncAction::RenameRemoteMailbox { .. } => remote_renames.push(action),
                SyncAction::UploadMessage { .. } => uploads.push(action),
                SyncAction::UpdateRemoteKeywords { .. } => remote_keywords.push(action),
                SyncAction::MoveRemote { .. } => remote_moves.push(action),
                SyncAction::DestroyRemote { .. } => remote_destroys.push(action),
                SyncAction::DestroyRemoteMailbox { .. } => remote_mailbox_destroys.push(action),
            }
        }

        // CreateLocalMailbox runs first so every downstream phase
        // can assume the maildir tree and sentinel are in place
        // for any newly synced mailbox: Download writes into
        // cur/, UpdateLocalFlags renames within it, MoveLocal's
        // destination has to exist, and so on.
        self.create_local_mailboxes(local_creates)?;
        // RenameLocalMailbox runs right after creates so the
        // tree settles into its final per-cycle shape before any
        // phase that addresses folders by path. Download writes
        // into `binding.maildir_folder`, UpdateLocalFlags reads
        // `local_state.maildir_folder` (which the rename rewrites
        // in place), MoveLocal resolves both `from_folder` and
        // `to_binding.maildir_folder`. Reconcile pushes renames
        // shallowest-first so a parent's `fs::rename` of the
        // subtree lets each descendant's own action recover via
        // the source-missing/target-present branch.
        self.rename_local_mailboxes(local_renames)?;
        adopt_messages(self.conn, unconditional_adopts)?;
        let downloaded = self.download_messages(downloads).await?;
        let local_flags_attempted = local_flags.len();
        let local_flag_updates = self.update_local_flags(local_flags)?;
        let local_moves_attempted = local_moves.len();
        let local_moves_count = self.move_local_messages(local_moves)?;
        let local_deletes_count = self.delete_local_messages(local_deletes)?;
        // DeleteLocalFolder runs after every per-message
        // local-side phase so any MoveLocal counter-action
        // that drained the folder gets to land first; once
        // the recursive remove fires, the folder + every file
        // inside is gone regardless. Cascade DB cleanup
        // (`message_map`, `local_state`, `folder_checkpoint`)
        // follows so the next cycle's dedupe + lookup don't
        // trip on rows pointing at a destroyed path.
        delete_local_folders(self.conn, &self.maildir_root, local_folder_deletes)?;
        // Remote mailbox creates land before uploads so a plan that emits
        // CreateRemoteMailbox + UploadMessage into the new mailbox in the
        // same cycle finds the destination already provisioned. Returns
        // the populated `creation_refs` table so subsequent
        // `UploadMessage` actions whose `binding.jmap_mailbox_id` carries
        // a `MaybeReference::Reference(folder)` (orphan-resurrect path)
        // can be resolved against the server-assigned ids.
        let creation_refs = self.create_remote_mailboxes(remote_creates).await?;
        // Shape-before-content: settle the server-side mailbox
        // tree (creates, then renames) before any phase that
        // addresses a mailbox by id and would care about its
        // current name. Mailbox/set { update } resolves
        // parent_id against the server's already-assigned ids
        // (not against any creation_id back-reference issued
        // earlier this batch), so the executor could in
        // principle run uploads and the rename in either order;
        // keeping renames first matches the local-side phase
        // order (create_local -> rename_local -> downloads) and
        // makes the per-cycle JMAP trace easier to read.
        self.rename_remote_mailboxes(remote_renames).await?;
        let uploads = resolve_upload_references(uploads, &creation_refs);
        let upload_results = self.upload_messages(uploads).await?;
        let uploaded = upload_results.uploaded;
        let (outcome, remote_counts) = self
            .apply_remote_set(remote_keywords, remote_moves, remote_destroys)
            .await?;
        // DestroyRemoteMailbox runs after every per-email push
        // phase so any cycle-local Upload/Move/Update targeting
        // the soon-to-be-destroyed mailbox lands first. Matches
        // the pull-side `delete_local_folders` "run after
        // per-message phases" discipline.
        self.destroy_remote_mailboxes(remote_mailbox_destroys)
            .await?;
        let flag_updates = local_flag_updates + remote_counts.keyword_updates;
        let moved = local_moves_count + remote_counts.moves;
        let deleted = local_deletes_count + remote_counts.destroys;
        apply_move_pair_adopts(self.conn, move_pair_adopts, &outcome.failed_updates)?;
        let failed_remote_actions = outcome.failed_updates.len() + outcome.failed_destroys.len();

        // RFC 8620 section 7.1 cursor ratchet, combined across every
        // server-state-advancing call we made this cycle (every
        // Email/import plus the trailing Email/set batch). Each call
        // contributes one `oldState -> newState` edge to a chain map
        // keyed on `oldState`; we walk forward from the cursor
        // reconcile gave us. If every edge in the map is consumed in
        // a single contiguous walk, the chain is intact (no third-
        // party write landed between our calls) and we ratchet the
        // cursor to the walk's end. If any edge is unreachable from
        // the cursor, or any of our calls returned a missing chain
        // half (chain_intact false), we leave the cursor alone --
        // the next cycle's `Email/changes` from the unmoved cursor
        // catches up cleanly through the cursor-advance-on-empty-plan
        // path.
        let set_intact = matches!(
            (&outcome.chain_old, &outcome.chain_new),
            (None, None) | (Some(_), Some(_))
        );
        let combined_intact = upload_results.chain_intact && set_intact;
        let mut edges: HashMap<String, String> = upload_results.chain_pairs.into_iter().collect();
        if let (Some(o), Some(n)) = (outcome.chain_old, outcome.chain_new) {
            edges.insert(o, n);
        }
        // A pull-side FS op that warn-and-skipped this cycle (a
        // MoveLocal or UpdateLocalFlags whose on-disk rename failed)
        // left its paired DB write unwritten. Advancing the cursor
        // past the delta that produced it would strand the change
        // silently -- see `resolve_email_cursor`.
        let pull_apply_incomplete =
            local_flag_updates < local_flags_attempted || local_moves_count < local_moves_attempted;
        let new_email_state = resolve_email_cursor(
            new_email_state,
            pull_apply_incomplete,
            combined_intact,
            &edges,
        );

        // Intentionally outside the per-phase transactions: if the process
        // dies between the last phase commit and this write, the cursor
        // stays at the previous value and the next cycle replays
        // Email/changes against the already-mirrored DB. Idempotent.
        if let Some(state) = new_email_state {
            queries::set_jmap_state(self.conn, self.account_id.as_ref(), "Email", &state)?;
            debug!("Persisted new Email state: {}", state);
        }

        Ok(SyncOutcome {
            downloaded,
            uploaded,
            flag_updates,
            moved,
            deleted,
            failed_remote_actions,
            // Engine sets these from the unfiltered plan / bindings;
            // executor doesn't have the visibility to compute them.
            already_in_sync: false,
            remote_orphans_detected: 0,
            remote_orphan_total_emails: 0,
        })
    }

    /// Returns the number of local flag updates the maildir + DB
    /// accepted this cycle. Per-message `set_flags` failures are
    /// logged at warn and skipped here; the count reflects only the
    /// updates that actually landed.
    fn update_local_flags(&self, actions: Vec<SyncAction>) -> Result<usize> {
        apply_update_local_flags(
            self.conn,
            &self.maildir_root,
            self.self_writes.as_deref(),
            actions,
        )
    }

    /// Returns the number of cross-folder local moves that landed
    /// (file moved + DB row updated). Per-message `store::move_message`
    /// failures are logged at warn and skipped here.
    fn move_local_messages(&self, actions: Vec<SyncAction>) -> Result<usize> {
        let mut succeeded = 0usize;
        for action in actions {
            let SyncAction::MoveLocal {
                id,
                from_folder,
                to_binding,
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
            let from = store::ensure_maildir(&self.maildir_root.join(&from_folder))?;
            let to = store::ensure_maildir(&self.maildir_root.join(&to_binding.maildir_folder))?;
            if let Err(e) = store::move_message(&from, &to, maildir_id.as_ref()) {
                warn!(
                    "Failed to move {} from {} to {}: {}",
                    bound_for_log, from_folder, to_binding.maildir_folder, e
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
            let txn = self.conn.unchecked_transaction()?;
            let flags = if let Some(rec) = queries::get_message_by_jmap_id(&txn, &jmap_email_id)? {
                let preserved_flags = rec.flags.clone();
                queries::upsert_message(
                    &txn,
                    &MessageRecord {
                        jmap_mailbox_id: to_binding
                            .jmap_mailbox_id
                            .expect_resolved(
                                "execute::move_local_messages -- jmap_mailbox_id DB write",
                            )
                            .clone(),
                        ..rec
                    },
                )?;
                preserved_flags
            } else {
                String::new()
            };
            queries::upsert_local_state(
                &txn,
                &maildir_id,
                &to_binding.maildir_folder,
                &flags,
                None,
            )?;
            txn.commit()?;
            succeeded += 1;
            info!(
                "Moved {} from {} to {}",
                bound_for_log, from_folder, to_binding.maildir_folder
            );
        }
        Ok(succeeded)
    }

    /// Returns the number of local deletions that landed (DB rows
    /// removed). A `store::delete_message` failure for a file that
    /// was already gone is normal -- logged at debug -- and the DB
    /// cleanup still runs, so the count tracks "message gone from
    /// local" rather than "file successfully unlinked."
    fn delete_local_messages(&self, actions: Vec<SyncAction>) -> Result<usize> {
        let mut succeeded = 0usize;
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
            let maildir = store::ensure_maildir(&self.maildir_root.join(&maildir_folder))?;
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
            let txn = self.conn.unchecked_transaction()?;
            queries::delete_local_state(&txn, &maildir_id)?;
            queries::delete_message_by_jmap_id(&txn, &jmap_email_id)?;
            txn.commit()?;
            succeeded += 1;
            info!("Deleted local copy of destroyed {}", bound_for_log);
        }
        Ok(succeeded)
    }

    /// Execute `CreateLocalMailbox` actions: `ensure_maildir`
    /// on the resolved path (creates `cur/`/`new/`/`tmp/` if
    /// missing) and stamp the `.jma.mapping` sentinel so the
    /// local-folder -> JMAP-mailbox binding survives a state-DB
    /// nuke. Idempotent: re-running against an already-created
    /// folder is harmless (both helpers are idempotent), so a
    /// partial-failure retry the next cycle is safe -- the
    /// emitter in `resolve_mailboxes` re-fires whenever the
    /// on-disk state is inconsistent with the server-known
    /// binding.
    ///
    /// Per-folder failures are logged at warn and skipped so a
    /// single broken create doesn't cascade into the rest of
    /// the cycle.
    fn create_local_mailboxes(&self, actions: Vec<SyncAction>) -> Result<()> {
        for action in actions {
            let SyncAction::CreateLocalMailbox {
                binding,
                parent_jmap_mailbox_id,
                replaces_orphan_id,
            } = action
            else {
                continue;
            };
            // Push-side ServerWins-with-Blocker resurrect: the
            // engine pre-pass hoisted this binding back into the
            // first-cycle slot with `replaces_orphan_id` set to
            // the id whose cached `message_map` rows are now
            // stale (the user deleted the local maildir; the rows
            // point at maildir_ids that no longer exist on disk).
            // Drop them BEFORE ensure_maildir so a partial-failure
            // retry next cycle re-runs the same SQL idempotently;
            // downstream DownloadMessage actions then write fresh
            // rows for the same id against the recreated maildir.
            if let Some(dead_id) = &replaces_orphan_id
                && let Err(e) = queries::delete_messages_by_jmap_mailbox_id(self.conn, dead_id)
            {
                warn!(
                    "CreateLocalMailbox for {}: failed to drop stale message_map rows for \
                     orphan id {}: {}; downstream DownloadMessage actions may collide with \
                     stale rows on the unique-on-maildir-id index",
                    binding.maildir_folder, dead_id, e
                );
                // Continue: the maildir provisioning is still
                // worth doing; the row collision (if any)
                // surfaces as a per-download failure rather
                // than blocking the whole create.
            }
            let folder_path = self.maildir_root.join(&binding.maildir_folder);
            if let Err(e) = store::ensure_maildir(&folder_path) {
                warn!(
                    "Failed to create maildir for new mailbox {} at {}: {}; \
                     subsequent actions targeting this folder will likely also fail",
                    binding.jmap_mailbox_id,
                    folder_path.display(),
                    e
                );
                continue;
            }
            if let Err(e) = crate::maildir_ops::sentinel::write(
                &folder_path,
                &crate::maildir_ops::sentinel::MailboxMapping {
                    jmap_mailbox_id: binding
                        .jmap_mailbox_id
                        .expect_resolved("execute::create_local_mailboxes -- sentinel write")
                        .clone(),
                    parent_jmap_mailbox_id: parent_jmap_mailbox_id.clone(),
                    server_name: binding.server_name.clone(),
                },
            ) {
                warn!(
                    "Created maildir for new mailbox {} but sentinel write failed at {}: {}; \
                     the next sync will detect the missing sentinel and retry the write",
                    binding.jmap_mailbox_id,
                    folder_path.display(),
                    e
                );
                // Folder exists; downstream actions can land. The
                // sentinel is recoverable; the folder is the
                // load-bearing part.
            }
            info!(
                "Created local maildir for new mailbox: {}",
                binding.maildir_folder
            );
        }
        Ok(())
    }

    /// Execute `RenameLocalMailbox` actions: `fs::rename` the
    /// maildir from `from_folder` to `binding.maildir_folder`,
    /// rewrite every `local_state` row whose folder column was
    /// the pre-rename path, and refresh the sentinel at the new
    /// path so its `server_name` and `parent` reflect the
    /// current `Mailbox/get` view.
    ///
    /// Same-cycle idempotency on the source-missing/target-
    /// present shape: under `LAYOUT=fs` a parent's rename moves
    /// the descendant subtree in one `fs::rename`, so each
    /// descendant's own queued action sees its `from_folder`
    /// already absent and the destination already present and
    /// runs the local_state + sentinel catch-up against the
    /// already-moved folder.
    ///
    /// Cross-mountpoint renames (`EXDEV`) are logged at warn
    /// and skipped: jma assumes a single-mountpoint maildir
    /// tree (see project_jma_folder_lifecycle_plan memory),
    /// and a copy-with-fsync fallback is out of scope until a
    /// real user report surfaces. The failed rename surfaces
    /// as drift -- the next cycle's diff against the cache
    /// will not re-emit it, so the user must resolve the
    /// mountpoint split or migrate the folder by hand for
    /// catch-up to resume.
    ///
    /// Per-folder failures are logged at warn and skipped so a
    /// single broken rename doesn't cascade into the rest of
    /// the cycle.
    fn rename_local_mailboxes(&self, actions: Vec<SyncAction>) -> Result<()> {
        for action in actions {
            let SyncAction::RenameLocalMailbox {
                from_folder,
                from_db_folder,
                binding,
                parent_jmap_mailbox_id,
            } = action
            else {
                continue;
            };
            let old_path = self.maildir_root.join(&from_folder);
            let new_path = self.maildir_root.join(&binding.maildir_folder);

            match std::fs::rename(&old_path, &new_path) {
                Ok(()) => {
                    info!(
                        "Server-side mailbox rename followed on disk: {} -> {}",
                        from_folder, binding.maildir_folder
                    );
                }
                // `ErrorKind::CrossesDevices` is the portable kind; the
                // `raw_os_error() == EXDEV` fallback covers any platform
                // whose error mapping doesn't lift the errno into the
                // typed kind yet.
                Err(e)
                    if e.kind() == std::io::ErrorKind::CrossesDevices
                        || e.raw_os_error() == Some(libc::EXDEV) =>
                {
                    warn!(
                        "Server-side mailbox rename {} -> {} crosses mountpoints; \
                         jma assumes a single-mountpoint maildir tree and refuses \
                         the move. Place both paths on the same filesystem (or \
                         migrate the folder manually and rerun sync) to proceed.",
                        from_folder, binding.maildir_folder
                    );
                    continue;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && new_path.exists() => {
                    debug!(
                        "Rename source {} already absent and destination {} present; \
                         assuming a prior cycle completed the rename, applying DB catch-up",
                        from_folder, binding.maildir_folder
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to rename mailbox folder on disk {} -> {}: {}",
                        from_folder, binding.maildir_folder, e
                    );
                    continue;
                }
            }

            match queries::rename_local_state_folder(
                self.conn,
                &from_db_folder,
                &binding.maildir_folder,
            ) {
                Ok(rows) if rows > 0 => debug!(
                    "Rewrote {} local_state rows from {} to {}",
                    rows, from_db_folder, binding.maildir_folder
                ),
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        "FS rename {} -> {} landed but local_state catch-up failed: {}; \
                         the local_state row stays anchored to the old folder; the next \
                         sync will see the same cached/resolved disagreement and retry \
                         the rewrite",
                        from_folder, binding.maildir_folder, e
                    );
                    continue;
                }
            }

            // Refresh the sentinel at the new path: the pre-rename
            // sentinel travels with the maildir under fs::rename,
            // but its `server_name` may be stale if the server
            // also renamed the display name alongside the path.
            if let Err(e) = crate::maildir_ops::sentinel::write(
                &new_path,
                &crate::maildir_ops::sentinel::MailboxMapping {
                    jmap_mailbox_id: binding
                        .jmap_mailbox_id
                        .expect_resolved("execute::rename_local_mailboxes -- sentinel refresh")
                        .clone(),
                    parent_jmap_mailbox_id,
                    server_name: binding.server_name.clone(),
                },
            ) {
                warn!(
                    "Server-side rename {} -> {} landed but sentinel refresh failed: {}; \
                     the next sync will detect the stale sentinel and retry the write",
                    from_folder, binding.maildir_folder, e
                );
            }
        }
        Ok(())
    }

    /// Execute `CreateRemoteMailbox` actions by issuing one
    /// `Mailbox/set { create }` per action via
    /// `jmap::mailbox::create`. Per-action failures are logged
    /// at warn and skipped so a single rejected create doesn't
    /// cascade into the rest of the cycle; the next reconcile
    /// re-emits the action from the still-untracked local
    /// folder.
    ///
    /// Sequential to honour the parent-before-child invariant the
    /// emitter establishes: chained creates carry
    /// `MaybeReference::Reference(parent_folder)` for the parent
    /// id, and the per-cycle `creation_refs` table records each
    /// successful create's `(folder, returned_id)` so the next
    /// action's `Reference` resolves. A child whose `Reference`
    /// doesn't resolve (the parent's create was rejected or its
    /// emission was skipped) is warn-skipped; the next cycle's
    /// emit pass re-runs the chain.
    ///
    /// On a successful create, write the local sentinel + ensure
    /// the maildir tree in the same cycle, mirroring
    /// `create_local_mailboxes`. Without this the local folder
    /// would carry no `.jma.mapping` until the next cycle's
    /// `resolve_mailboxes` saw the freshly-created server mailbox
    /// and emitted its own `CreateLocalMailbox`; a state-DB nuke
    /// between cycles would leave the binding recoverable only by
    /// `janitor::rebindfolders`'s Message-ID probing, which is
    /// slow and may not even disambiguate. Failures here are
    /// warn-and-continue: the maildir/sentinel are recoverable
    /// from the server-side mailbox the next cycle's resolve
    /// detects.
    async fn create_remote_mailboxes(
        &self,
        actions: Vec<SyncAction>,
    ) -> Result<HashMap<String, JmapMailboxId>> {
        let mut creation_refs: HashMap<String, JmapMailboxId> = HashMap::new();
        let maildir_root = &self.maildir_root;
        for action in actions {
            let SyncAction::CreateRemoteMailbox {
                name,
                parent_jmap_mailbox_id,
                role,
                folder,
                replaces_orphan_id,
            } = action
            else {
                continue;
            };
            let resolved_parent: Option<JmapMailboxId> = match &parent_jmap_mailbox_id {
                None => None,
                Some(MaybeReference::Value(id)) => Some(id.clone()),
                Some(MaybeReference::Reference(parent_folder)) => {
                    match creation_refs.get(parent_folder) {
                        Some(id) => Some(id.clone()),
                        None => {
                            warn!(
                                "Skipping CreateRemoteMailbox {:?}: parent reference {:?} \
                                 unresolved (parent create missing or rejected this cycle); \
                                 next cycle will re-emit",
                                folder, parent_folder
                            );
                            continue;
                        }
                    }
                }
            };
            // Resurrect path: clean stale `message_map` rows
            // pointing at the dead `jmap_mailbox_id` before the
            // JMAP create runs. Subsequent `UploadMessage`
            // actions in this cycle will write fresh rows
            // bound to the new server-side id; the unique-on-
            // `maildir_id` index would reject the upsert
            // otherwise. Runs after parent resolution so a
            // parent-unresolved skip above doesn't strand the
            // rows with no create attempt to surface.
            if let Some(dead_id) = replaces_orphan_id.as_ref() {
                let removed = queries::delete_messages_by_jmap_mailbox_id(self.conn, dead_id)?;
                debug!(
                    "Resurrect cleanup: dropped {} message_map row(s) for dead \
                     mailbox id {} before re-creating as {:?}",
                    removed, dead_id, folder
                );
            }
            match crate::jmap::mailbox::create(
                &self.client,
                &name,
                resolved_parent.as_ref(),
                role.as_deref(),
            )
            .await
            {
                Ok(new_id) => {
                    let folder_path = maildir_root.join(&folder);
                    // Gate the sentinel write on a successful
                    // `ensure_maildir`: attempting to write a
                    // sentinel inside a nonexistent directory
                    // produces a second, misleading warn line
                    // downstream of the real cause. The
                    // `creation_refs` insert still runs either way
                    // -- the server-side mailbox exists, so a
                    // child's Reference must still resolve regardless
                    // of the local-side outcome.
                    match store::ensure_maildir(&folder_path) {
                        Ok(_) => {
                            if let Err(e) = crate::maildir_ops::sentinel::write(
                                &folder_path,
                                &crate::maildir_ops::sentinel::MailboxMapping {
                                    jmap_mailbox_id: new_id.clone(),
                                    parent_jmap_mailbox_id: resolved_parent.clone(),
                                    server_name: name.clone(),
                                },
                            ) {
                                warn!(
                                    "Created remote mailbox {} but sentinel write failed \
                                     at {}: {}; the next sync will detect the missing \
                                     sentinel and retry the write",
                                    new_id,
                                    folder_path.display(),
                                    e
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Created remote mailbox {} but local ensure_maildir failed \
                                 at {}: {}; next sync's resolve_mailboxes will detect the \
                                 missing folder and retry both the maildir and sentinel",
                                new_id,
                                folder_path.display(),
                                e
                            );
                        }
                    }
                    creation_refs.insert(folder, new_id);
                }
                Err(e) => {
                    warn!("Failed to create remote mailbox {:?}: {}", name, e);
                }
            }
        }
        Ok(creation_refs)
    }

    /// Execute `RenameRemoteMailbox` actions by issuing one
    /// `Mailbox/set { update }` per action (name + parentId
    /// rewritten atomically) via `jmap::mailbox::update_name_
    /// and_parent`. On success, upsert `mailbox_map` with the
    /// new name, parent, and disk folder so the cache advances
    /// only once the server agrees with the local view --
    /// preserving the "cache reflects what is on disk and
    /// accepted by the server" invariant under both `--dry-run`
    /// (which skips the action) and per-action failure (which
    /// leaves the cache pointing at the pre-rename folder so
    /// the next cycle's sentinel walk re-emits the action).
    ///
    /// Per-action failures are logged at warn and skipped so
    /// one server-side rejection (`alreadyExists` from a name
    /// collision, `invalidProperties` from an over-long name,
    /// ...) doesn't cascade into the rest of the cycle.
    async fn rename_remote_mailboxes(&self, actions: Vec<SyncAction>) -> Result<()> {
        let maildir_root = &self.maildir_root;
        for action in actions {
            let SyncAction::RenameRemoteMailbox {
                from_folder,
                binding,
                parent_jmap_mailbox_id,
            } = action
            else {
                continue;
            };
            let mailbox_id = binding
                .jmap_mailbox_id
                .expect_resolved("rename_remote_mailboxes -- binding must be resolved");
            if let Err(e) = crate::jmap::mailbox::update_name_and_parent(
                &self.client,
                mailbox_id,
                &binding.server_name,
                parent_jmap_mailbox_id.as_ref(),
            )
            .await
            {
                warn!(
                    "Failed to rename remote mailbox {} -> {:?}: {}",
                    mailbox_id, binding.server_name, e
                );
                continue;
            }
            // Server accepted the rename; advance the cache to
            // match. Read the existing row only for role and
            // sort_order (the action carries the post-rename
            // binding + parent id + pre-rename folder); preserve
            // those fields across the upsert so a transient
            // server-side change to them this cycle doesn't get
            // clobbered.
            let existing = match queries::get_mailbox(self.conn, mailbox_id) {
                Ok(Some(row)) => row,
                Ok(None) => {
                    warn!(
                        "Renamed remote mailbox {} succeeded but mailbox_map row vanished; \
                         next sync cycle's Mailbox/get will rebuild it",
                        mailbox_id
                    );
                    continue;
                }
                Err(e) => {
                    warn!(
                        "Renamed remote mailbox {} succeeded but mailbox_map read failed: {}",
                        mailbox_id, e
                    );
                    continue;
                }
            };
            if let Err(e) = queries::upsert_mailbox(
                self.conn,
                &queries::MailboxRecord {
                    jmap_mailbox_id: mailbox_id.clone(),
                    name: binding.server_name.clone(),
                    role: existing.role,
                    parent_id: parent_jmap_mailbox_id.clone(),
                    maildir_folder: binding.maildir_folder.clone(),
                    sort_order: existing.sort_order,
                    remote_path: Some(binding.remote_path.clone()),
                },
            ) {
                warn!(
                    "Renamed remote mailbox {} -> {:?} but mailbox_map upsert failed: {}; \
                     next sync cycle's Mailbox/get will catch up",
                    mailbox_id, binding.server_name, e
                );
                continue;
            }

            // local_state rows are keyed by `maildir_folder` --
            // rewrite them at the new path so the next cycle's
            // scan + hydrate path finds its rows where the
            // messages actually live, skipping the one-cycle
            // "new file with no DB row" / adopt churn the
            // unchanged-key path would otherwise produce.
            if from_folder != binding.maildir_folder {
                match queries::rename_local_state_folder(
                    self.conn,
                    &from_folder,
                    &binding.maildir_folder,
                ) {
                    Ok(rows) if rows > 0 => debug!(
                        "Rewrote {} local_state rows from {} to {}",
                        rows, from_folder, binding.maildir_folder
                    ),
                    Ok(_) => {}
                    Err(e) => warn!(
                        "Renamed remote mailbox {} -> {:?} but local_state catch-up \
                         failed: {}; next cycle's adopt path will rebind from \
                         Message-ID",
                        mailbox_id, binding.server_name, e
                    ),
                }
            }

            // Refresh the sentinel at the disk path so its
            // `server_name` reflects the just-pushed JMAP name
            // (the rename moved the directory, the sentinel
            // travels with it, but its name field was the old
            // server_name until this write).
            let disk_path = maildir_root.join(&binding.maildir_folder);
            if let Err(e) = crate::maildir_ops::sentinel::write(
                &disk_path,
                &crate::maildir_ops::sentinel::MailboxMapping {
                    jmap_mailbox_id: mailbox_id.clone(),
                    parent_jmap_mailbox_id,
                    server_name: binding.server_name.clone(),
                },
            ) {
                warn!(
                    "Renamed remote mailbox {} -> {:?} but sentinel refresh failed: {}; \
                     `janitor rebindfolders` will rebind it after the next sync",
                    mailbox_id, binding.server_name, e
                );
            }
        }
        Ok(())
    }

    /// Push-side mirror of `delete_local_folders`. Issues one
    /// `Mailbox/set { destroy }` per action via
    /// `jmap::mailbox::destroy`, with `onDestroyRemoveEmails: true`
    /// so a mailbox holding cycle-local stale email content
    /// vanishes with the folder rather than failing the destroy.
    /// On success drops the `mailbox_map` row inline -- the
    /// next-cycle cache-vs-server diff would drop it anyway, but
    /// inline cleanup keeps `MailboxBindings`'s invariants tight.
    ///
    /// Server-side rejection (per-id `notDestroyed`) logs at
    /// `warn!` and continues to the next action; the cache row
    /// stays in place so a subsequent cycle can retry.
    async fn destroy_remote_mailboxes(&self, actions: Vec<SyncAction>) -> Result<()> {
        for action in actions {
            let SyncAction::DestroyRemoteMailbox { binding } = action else {
                continue;
            };
            let jmap_mailbox_id = binding
                .jmap_mailbox_id
                .expect_resolved("destroy_remote_mailboxes -- binding must be resolved");
            let label = binding.remote_path.as_str();
            if let Err(e) = crate::jmap::mailbox::destroy(&self.client, jmap_mailbox_id).await {
                warn!(
                    "Failed to destroy remote mailbox {} (id {}): {}; \
                     mailbox_map row stays for next-cycle retry",
                    label, jmap_mailbox_id, e
                );
                continue;
            }
            if let Err(e) = queries::delete_mailbox(self.conn, jmap_mailbox_id) {
                warn!(
                    "Destroyed remote mailbox {} (id {}) but mailbox_map drop failed: {}; \
                     next cycle's cache-vs-server diff will catch up",
                    label, jmap_mailbox_id, e
                );
            }
        }
        Ok(())
    }

    async fn upload_messages(&self, actions: Vec<SyncAction>) -> Result<UploadResults> {
        if actions.is_empty() {
            return Ok(UploadResults {
                chain_pairs: Vec::new(),
                chain_intact: true,
                uploaded: 0,
            });
        }
        // Phase span wraps the parallel upload work end-to-end. The
        // per-blob spans inside `import_email` populate the upload
        // aggregate; this span's wall-clock is the denominator for the
        // effective upload-bytes-per-second figure.
        let phase = tracing::info_span!(target: crate::profile::TARGET_PHASE, "upload_blobs");
        async move {
            let n = limits::upload_concurrency(&self.client, self.config.sync.upload_concurrency);
            if n != self.config.sync.upload_concurrency {
                info!(
                    "Clamped upload concurrency from {} to {} per server maxConcurrentUpload",
                    self.config.sync.upload_concurrency, n
                );
            }
            let outcome = run_upload_stream(&self.client, &actions, n).await;
            self.commit_uploaded_messages(&actions, &outcome.succeeded)?;
            let uploaded = outcome.succeeded.len();
            if let Some(e) = outcome.hard_error {
                return Err(e);
            }
            Ok(UploadResults {
                chain_pairs: outcome.chain_pairs,
                chain_intact: outcome.chain_intact,
                uploaded,
            })
        }
        .instrument(phase)
        .await
    }

    /// Mirror each successful upload into `message_map` + `local_state`.
    /// rusqlite's Connection isn't Send, so this runs serially after
    /// the parallel stream drains -- the DB writes can't sit inside
    /// the futures. Per-iteration transactions pair the two upserts
    /// so a mid-loop crash can't leave a row in only one table.
    fn commit_uploaded_messages(
        &self,
        pending: &[SyncAction],
        succeeded: &[(usize, JmapEmailId)],
    ) -> Result<()> {
        for (i, jmap_email_id) in succeeded {
            let SyncAction::UploadMessage {
                id, binding, flags, ..
            } = &pending[*i]
            else {
                unreachable!("non-upload in uploads bucket");
            };
            let keywords = crate::maildir_ops::flags::flags_to_keywords(flags);
            let keywords_json = serde_json::to_string(&keywords)?;
            let txn = self.conn.unchecked_transaction()?;
            queries::upsert_message(
                &txn,
                &MessageRecord {
                    jmap_email_id: jmap_email_id.clone(),
                    jmap_blob_id: None,
                    jmap_thread_id: None,
                    jmap_mailbox_id: binding
                        .jmap_mailbox_id
                        .expect_resolved(
                            "execute::commit_uploaded_messages -- jmap_mailbox_id DB write",
                        )
                        .clone(),
                    maildir_id: Some(id.maildir_id.clone()),
                    message_id: id.message_id.clone(),
                    flags: flags.clone(),
                    jmap_keywords: keywords_json,
                },
            )?;
            queries::upsert_local_state(
                &txn,
                &id.maildir_id,
                &binding.maildir_folder,
                flags,
                None,
            )?;
            txn.commit()?;
            let target = RemoteId {
                jmap_email_id: jmap_email_id.clone(),
                message_id: id.message_id.clone(),
            };
            info!("Uploaded local message {} -> {}", id.maildir_id, target);
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
    /// Returns the JMAP-layer outcome plus a per-category count of
    /// remote operations the server accepted. Each count matches the
    /// semantics of the local-side counter it pairs with (skips
    /// server-rejected ids), so summing across directions gives the
    /// total for the cycle.
    async fn apply_remote_set(
        &self,
        keywords: Vec<SyncAction>,
        moves: Vec<SyncAction>,
        destroys: Vec<SyncAction>,
    ) -> Result<(jmap_email::EmailSetOutcome, ApplyRemoteSetCounts)> {
        let mut ops: Vec<EmailSetOp> = Vec::new();
        for action in &keywords {
            if let SyncAction::UpdateRemoteKeywords { id, keywords, .. } = action {
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
            return Ok((
                jmap_email::EmailSetOutcome::default(),
                ApplyRemoteSetCounts::default(),
            ));
        }

        let outcome = jmap_email::set_email_batch(&self.client, &ops).await?;

        // One transaction wraps the entire post-network mirroring section.
        // The server's outcome is fixed; either we reflect all of it into
        // the local DB or none of it (next cycle re-detects the missing
        // mirrorings via Email/changes and retries). Pure-DB after this
        // point, so the txn is short-lived.
        let txn = self.conn.unchecked_transaction()?;

        // Mirror keyword updates into the local DB.
        let mut counts = ApplyRemoteSetCounts::default();
        for action in keywords {
            let SyncAction::UpdateRemoteKeywords {
                id,
                keywords,
                filename_flags,
            } = action
            else {
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
                // Split the two flag-column writes by source, same
                // discipline `commit_adopt` follows: message_map.
                // flags derives from the merged-server view (what
                // we believe the server has after the patch);
                // local_state.flags reflects the on-disk filename
                // suffix carried in via `filename_flags`. Pre-fix
                // the mirror derived both from the patch HashMap,
                // which silently worked for additive `flags_to_
                // keywords(filename)` patches (their `keywords_to_
                // flags` reduction *happens* to equal the filename)
                // but broke as soon as the patch shape diverged --
                // see the doc on `UpdateRemoteKeywords.filename_
                // flags`.
                let merged = apply_keyword_patch(&rec.jmap_keywords, &keywords);
                let keywords_json = serde_json::to_string(&merged)?;
                let server_flags = keywords_to_flags(&merged);
                let maildir_id = rec.maildir_id.clone();
                queries::upsert_message(
                    &txn,
                    &MessageRecord {
                        flags: server_flags,
                        jmap_keywords: keywords_json,
                        ..rec
                    },
                )?;
                if let Some(mid) = maildir_id {
                    queries::update_local_state_flags(&txn, &mid, &filename_flags)?;
                }
            }
            counts.keyword_updates += 1;
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
                counts.moves += 1;
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
            counts.destroys += 1;
            info!("Destroyed remote {}", id);
        }

        txn.commit()?;
        Ok((outcome, counts))
    }

    /// Run all DownloadMessage actions concurrently with the rate-limit
    /// halving behavior the old pull path had.
    async fn download_messages(&self, actions: Vec<SyncAction>) -> Result<usize> {
        if actions.is_empty() {
            return Ok(0);
        }
        // Phase span wraps the parallel download work end-to-end --
        // including retry passes for transient rate-limit halving.
        // The per-blob spans inside `download_blob` feed the download
        // aggregate; this span's wall-clock is the denominator for
        // the effective download-bytes-per-second figure.
        let phase = tracing::info_span!(target: crate::profile::TARGET_PHASE, "download_blobs");
        async move {
            // Server-cap-aware download concurrency. The clamp is logged
            // here rather than at the top of `execute` so quiet "advance
            // the cursor only" cycles (empty plan, no downloads) don't
            // emit an info line that has no workload to describe.
            let mut concurrency =
                limits::concurrent_requests(&self.client, self.config.sync.download_concurrency);
            if concurrency != self.config.sync.download_concurrency {
                info!(
                    "Clamped download concurrency from {} to {} per server maxConcurrentRequests",
                    self.config.sync.download_concurrency, concurrency
                );
            }

            let total = actions.len();
            // Up-front summary so the user sees that work is queued before
            // any blob actually lands -- otherwise initial sync of a fresh
            // server is silent for as long as the first download takes.
            // Skip the one-message case: steady-state daemon cycles already
            // log the per-message line below, and a single-message preamble
            // is just noise.
            if total > 1 {
                crate::notify!("Downloading {} messages", total);
            }
            // Above the threshold, suppress per-message info and emit a
            // periodic heartbeat instead. `last_progress` is reset on each
            // heartbeat (and only advances when a store actually
            // committed), so a rate-limited stall doesn't spam an
            // unchanging percentage.
            let mut progress = DownloadProgress {
                downloaded: 0,
                total,
                verbose_per_message: total <= VERBOSE_DOWNLOAD_THRESHOLD,
                last_progress: Instant::now(),
            };

            let mut pending = actions;
            while !pending.is_empty() {
                let n = concurrency.max(1);
                let batch = std::mem::take(&mut pending);
                let outcome = self.download_batch(batch, n, &mut progress).await?;
                pending = outcome.to_retry;
                if pending.is_empty() {
                    break;
                }
                if outcome.rate_limited {
                    let new = (n / 2).max(1);
                    if new < n {
                        warn!(
                            "Hit JMAP rate limit; lowering download concurrency from {} to {}",
                            n, new
                        );
                        concurrency = new;
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

            Ok(progress.downloaded)
        }
        .instrument(phase)
        .await
    }

    /// Run one parallel download batch. Returns the actions that hit
    /// a transient error and should be retried, plus a flag the caller
    /// uses to decide whether to halve concurrency before the retry
    /// pass. Hard errors abort the batch immediately.
    ///
    /// Downloads run on a spawned producer task that feeds a bounded
    /// mpsc channel; this task drains the channel and performs the
    /// disk + DB writes inline. Connection isn't Send, so the writer
    /// must stay on this task -- but moving the download stream off
    /// it means the producer keeps pulling blobs into the channel
    /// while a write is in progress, instead of stalling between
    /// stream.next() calls.
    async fn download_batch(
        &self,
        batch: Vec<SyncAction>,
        concurrency: usize,
        progress: &mut DownloadProgress,
    ) -> Result<DownloadBatchOutcome> {
        // Pooled HTTP client scoped to this batch attempt. Built here
        // (not on the executor) so the underlying reqwest connection
        // pool is shared across the parallel fan-out below, and so it
        // is dropped at the end of every batch attempt -- a rate-
        // limit retry pass therefore starts from a fresh pool rather
        // than inheriting possibly-poisoned connections.
        let http = jmap_email::build_blob_http_client(&self.client)?;

        // Bounded channel: at most `concurrency` completed blobs are
        // buffered between producer and writer. Each item carries a
        // `TemporaryMailFile` (an open `tmp/` file handle) rather
        // than the blob bytes -- peak memory is now ~16 KB per
        // in-flight blob (one HTTP chunk in the bytes_stream loop),
        // not the full message size.
        let (tx, mut rx) = mpsc::channel(concurrency);
        let producer = tokio::spawn(download_producer(
            http,
            Arc::clone(&self.client),
            batch,
            concurrency,
            Arc::new(self.maildir_root.clone()),
            tx,
        ));

        let mut rate_limited = false;
        let mut hard_error: Option<anyhow::Error> = None;
        let mut to_retry: Vec<SyncAction> = Vec::new();
        let mut blocked_recv_us: u64 = 0;
        let mut recv_count: u64 = 0;

        loop {
            let t = Instant::now();
            let item = rx.recv().await;
            blocked_recv_us += t.elapsed().as_micros() as u64;
            let Some((action, result)) = item else { break };
            recv_count += 1;
            match result {
                Ok(tmp) => {
                    if let Err(e) = self.store_downloaded_message(&action, tmp, progress) {
                        hard_error = Some(e);
                        break;
                    }
                }
                Err(e) => {
                    if is_transient_error(&e) {
                        rate_limited = true;
                        if let SyncAction::DownloadMessage { id, .. } = &action {
                            debug!("Rate-limited downloading email {}: {}", id, e);
                        }
                        to_retry.push(action);
                    } else if hard_error.is_none() {
                        hard_error = Some(e);
                    }
                }
            }
        }
        // Closing the receiver makes the next tx.send in the producer
        // fail; the producer then breaks and drops its buffer_unordered
        // stream, cancelling any in-flight HTTP futures.
        drop(rx);
        let producer_stats = producer.await.expect("download producer task panicked");

        // Channel backpressure telemetry: large blocked_send_us with
        // small blocked_recv_us means the consumer is the bottleneck
        // (producer often waited for channel space); the inverse means
        // the producer is the bottleneck. Both small means the rates
        // matched. Emitted at debug! so it's visible under -vv when an
        // operator is investigating download performance, but stays
        // out of normal -v milestone output. Target is a literal
        // string rather than a `TARGET_CHANNEL` constant because this
        // event deliberately sits outside the `ProfileLayer`
        // aggregator -- it's one-event-per-batch diagnostic for the
        // regular tracing sink.
        debug!(
            target: "jma::profile::channel",
            producer_blocked_send_us = producer_stats.blocked_send_us,
            consumer_blocked_recv_us = blocked_recv_us,
            producer_send_count = producer_stats.send_count,
            consumer_recv_count = recv_count,
            "download channel backpressure stats",
        );

        if let Some(e) = hard_error {
            return Err(e);
        }
        Ok(DownloadBatchOutcome {
            to_retry,
            rate_limited,
        })
    }

    /// Promote the producer-streamed `tmp/` file into `new/`/`cur/`,
    /// record the message in the DB, and advance progress accounting.
    /// The `F_BARRIERFSYNC` on the tmp file and the SQLite WAL fsync
    /// stay on this task so the maildir-write -> DB-commit ordering
    /// invariant (barrier orders the file content before the WAL
    /// fsync that publishes the DB row) is preserved per-message.
    fn store_downloaded_message(
        &self,
        action: &SyncAction,
        tmp: TemporaryMailFile,
        progress: &mut DownloadProgress,
    ) -> Result<()> {
        let SyncAction::DownloadMessage {
            id,
            jmap_blob_id,
            jmap_thread_id,
            binding,
            keywords,
        } = action
        else {
            unreachable!("non-download in downloads bucket");
        };
        let flags = keywords_to_flags(keywords);
        let maildir_path = self.maildir_root.join(&binding.maildir_folder);
        // Producer-side `ensure_maildir` already created `cur/new/tmp`
        // before opening the tmp file; constructing a fresh `Maildir`
        // handle here is just a `PathBuf` wrap.
        let maildir = maildir::Maildir::from(maildir_path.clone());
        let mid = store::finalize_message(&maildir, tmp, &flags)?;
        if let Some(cache) = &self.self_writes {
            // Suppress the fsevents echo of this delivery and -- for
            // new/ deliveries -- the same-flag cur/ promotion an MUA
            // may rename to next. A flag-changing promotion (e.g.
            // MUA adds S on read) won't match the predicted path, so
            // it falls through to a real classify cycle.
            //
            // The flag string used here must byte-match what the
            // maildir crate writes. `keywords_to_flags` already
            // returns canonical (sorted, deduped) output and the
            // maildir crate writes it verbatim into the suffix; if
            // either side ever diverges from canonical-sorted, this
            // prediction silently misses every delivery.
            let subdir = if flags.contains('S') { "cur" } else { "new" };
            let suffix = format!("{}:2,{}", mid.as_ref(), flags);
            let delivered = maildir_path.join(subdir).join(&suffix);
            let mut paths = vec![delivered];
            if subdir == "new" {
                paths.push(maildir_path.join("cur").join(&suffix));
            }
            cache.record(paths);
        }
        let keywords_json = serde_json::to_string(keywords)?;
        // Per-iteration transaction: pair the message_map and
        // local_state upserts so the just-stored maildir file
        // doesn't end up bound in only one of the two tables.
        let txn = self.conn.unchecked_transaction()?;
        queries::upsert_message(
            &txn,
            &MessageRecord {
                jmap_email_id: id.jmap_email_id.clone(),
                jmap_blob_id: Some(jmap_blob_id.clone()),
                jmap_thread_id: Some(jmap_thread_id.clone()),
                jmap_mailbox_id: binding
                    .jmap_mailbox_id
                    .expect_resolved(
                        "execute::store_downloaded_message -- jmap_mailbox_id DB write",
                    )
                    .clone(),
                maildir_id: Some(mid.clone()),
                message_id: id.message_id.clone(),
                flags: flags.clone(),
                jmap_keywords: keywords_json,
            },
        )?;
        queries::upsert_local_state(&txn, &mid, &binding.maildir_folder, &flags, None)?;
        txn.commit()?;
        progress.downloaded += 1;
        if progress.verbose_per_message {
            info!(
                "Downloaded new email {} -> {}/{}",
                id, binding.maildir_folder, mid
            );
        } else {
            debug!(
                "Downloaded new email {} -> {}/{}",
                id, binding.maildir_folder, mid
            );
            if progress.last_progress.elapsed() >= DOWNLOAD_PROGRESS_INTERVAL {
                let pct = (progress.downloaded * 100) / progress.total;
                crate::notify!(
                    "Downloaded {}/{} ({}%) so far",
                    progress.downloaded,
                    progress.total,
                    pct
                );
                progress.last_progress = Instant::now();
            }
        }
        Ok(())
    }
}

/// Swap any `MaybeReference::Reference(folder)` in an
/// `UploadMessage`'s binding to `MaybeReference::Value(id)`
/// using the `creation_refs` map populated by
/// `create_remote_mailboxes`. Used for the orphan-resurrect
/// path, where reconcile emits `CreateRemoteMailbox` for the
/// dead mailbox plus one `UploadMessage` per orphan message
/// in the same plan; the upload's binding can only carry a
/// reference until the create returns the server-assigned id.
///
/// Drops `UploadMessage` actions whose reference is unresolved
/// (the parent create failed or was warn-skipped), with a
/// `warn!`; the file stays on disk for the next cycle to
/// retry. Non-`UploadMessage` actions and uploads with
/// already-resolved bindings pass through unchanged.
fn resolve_upload_references(
    uploads: Vec<SyncAction>,
    creation_refs: &HashMap<String, JmapMailboxId>,
) -> Vec<SyncAction> {
    uploads
        .into_iter()
        .filter_map(|action| match action {
            SyncAction::UploadMessage {
                id,
                binding,
                file_path,
                flags,
            } => {
                if binding.jmap_mailbox_id.is_resolved() {
                    return Some(SyncAction::UploadMessage {
                        id,
                        binding,
                        file_path,
                        flags,
                    });
                }
                let resolved_id = match binding.jmap_mailbox_id.resolve_with(creation_refs) {
                    Some(id) => id,
                    None => {
                        // `is_resolved` was false above, so the
                        // unresolved case is a `Reference` whose
                        // key is missing from `creation_refs`.
                        let folder = match &binding.jmap_mailbox_id {
                            MaybeReference::Reference(f) => f.as_str(),
                            MaybeReference::Value(_) => unreachable!("is_resolved == false"),
                        };
                        warn!(
                            "Skipping UploadMessage {}: parent mailbox reference {:?} \
                             unresolved (CreateRemoteMailbox missing or rejected this \
                             cycle); next cycle will re-emit",
                            id, folder
                        );
                        return None;
                    }
                };
                Some(SyncAction::UploadMessage {
                    id,
                    binding: Arc::new(MailboxFolderBinding {
                        jmap_mailbox_id: MaybeReference::Value(resolved_id),
                        server_name: binding.server_name.clone(),
                        maildir_folder: binding.maildir_folder.clone(),
                        remote_path: binding.remote_path.clone(),
                    }),
                    file_path,
                    flags,
                })
            }
            other => Some(other),
        })
        .collect()
}

/// Pump downloaded blob tmp-file handles into `tx` from a parallel
/// `buffer_unordered` stream. Runs on its own tokio task so the
/// writer side of the channel (which performs blocking
/// barrier-fsync + rename + SQLite work inline) doesn't stall the
/// stream between completions. Each per-action future opens a fresh
/// `tmp/` file in the action's target maildir and streams the HTTP
/// body straight into it, so the bytes never sit in memory; only
/// the open `TemporaryMailFile` handle crosses the channel. Exits
/// on stream exhaustion or when the receiver is dropped; dropping
/// the stream cancels any in-flight HTTP futures and the partial
/// tmp files are unlinked by `TemporaryMailFile::drop`.
async fn download_producer(
    http: HttpClient,
    jmap: Arc<Client>,
    batch: Vec<SyncAction>,
    concurrency: usize,
    maildir_root: Arc<PathBuf>,
    tx: mpsc::Sender<(SyncAction, Result<TemporaryMailFile>)>,
) -> ChannelStats {
    let mut blocked_send_us: u64 = 0;
    let mut send_count: u64 = 0;
    let futures = batch.into_iter().map(|action| {
        // reqwest::Client::clone is internally an Arc bump; the
        // Arc<Client> clone is an explicit refcount bump. Both are
        // O(1) per future.
        let http = http.clone();
        let jmap = Arc::clone(&jmap);
        let maildir_root = Arc::clone(&maildir_root);
        let (blob_id, maildir_folder) = match &action {
            SyncAction::DownloadMessage {
                jmap_blob_id,
                binding,
                ..
            } => (jmap_blob_id.clone(), binding.maildir_folder.clone()),
            _ => unreachable!("non-download in downloads bucket"),
        };
        async move {
            let maildir_path = maildir_root.join(&maildir_folder);
            let res = match store::ensure_maildir(&maildir_path) {
                Ok(maildir) => {
                    jmap_email::download_blob(&http, &jmap, &blob_id, || store::open_tmp(&maildir))
                        .await
                }
                Err(e) => Err(e),
            };
            (action, res)
        }
    });
    let mut stream = stream::iter(futures).buffer_unordered(concurrency);
    while let Some(item) = stream.next().await {
        let t = Instant::now();
        let sent = tx.send(item).await.is_ok();
        blocked_send_us += t.elapsed().as_micros() as u64;
        if !sent {
            break;
        }
        send_count += 1;
    }
    ChannelStats {
        blocked_send_us,
        send_count,
    }
}

/// Producer-side channel telemetry returned to `download_batch` so the
/// caller can emit a single backpressure summary event per batch
/// alongside its own consumer-side recv timings.
struct ChannelStats {
    blocked_send_us: u64,
    send_count: u64,
}

/// Walk a state-advance chain forward from `cursor`. The chain is the
/// set of `(oldState -> newState)` edges produced by every server-
/// state-advancing call we made in the cycle (Email/import per
/// upload, plus the Email/set batch). The chain is "intact" iff
/// every edge is consumed in one contiguous walk that begins at
/// `cursor`: starting at the cursor we look up the next state in
/// the map, advance, and repeat until either the lookup misses or
/// the map is exhausted. If we walked all `edges.len()` entries
/// successfully, return the walked end (the chain's `newState`); if
/// any edge was unreachable (a third-party write landed mid-cycle
/// and our calls don't form a single contiguous run), return
/// `cursor` unchanged so the next cycle's `Email/changes` advances
/// the cursor through the regular path.
fn walk_chain(cursor: Option<String>, edges: &HashMap<String, String>) -> Option<String> {
    if edges.is_empty() {
        return cursor;
    }
    let mut current = cursor.clone();
    let mut walked = 0usize;
    while let Some(c) = current.as_deref() {
        if let Some(next) = edges.get(c) {
            current = Some(next.clone());
            walked += 1;
            // Server is a trust boundary: a cycle would otherwise
            // wedge this loop forever. The two shapes that reach us
            // are a multi-edge cycle (e.g. S0->S1, S1->S0) and a
            // self-loop (S0->S0), the latter being what
            // EmailSetOutcome::merge produces when it happily
            // coalesces a server state cycle back to the initial
            // state. A valid forward chain visits each edge at most
            // once, so any walk past `edges.len()` is a cycle.
            if walked > edges.len() {
                break;
            }
        } else {
            break;
        }
    }
    if walked == edges.len() {
        current
    } else {
        cursor
    }
}

/// Decide the Email-state cursor to persist at the tail of a cycle.
///
/// Returns `None` -- hold the cursor at its prior on-disk value --
/// when `pull_apply_incomplete` is set. That flag means a pull-side
/// filesystem op warn-and-skipped this cycle: a `MoveLocal` or
/// `UpdateLocalFlags` whose on-disk rename failed, so the handler
/// logged and continued without its paired DB write. Advancing the
/// cursor past the `Email/changes` delta that produced the skipped
/// action would strand it: next cycle's delta runs from the advanced
/// cursor and never re-reports the email, and the local scan can't
/// re-derive it either (the file and its `local_state` row are both
/// unchanged), so the change is lost silently. Holding the cursor
/// lets the next cycle re-apply the action idempotently.
///
/// Otherwise ratchets as usual: walks the chain from `reconcile_state`
/// when `combined_intact`, or leaves the cursor at `reconcile_state`
/// when a server call returned a missing chain half.
fn resolve_email_cursor(
    reconcile_state: Option<String>,
    pull_apply_incomplete: bool,
    combined_intact: bool,
    edges: &HashMap<String, String>,
) -> Option<String> {
    if pull_apply_incomplete {
        return None;
    }
    if combined_intact {
        walk_chain(reconcile_state, edges)
    } else {
        reconcile_state
    }
}

/// Execute `DeleteLocalFolder` actions, removing each named local
/// maildir folder and the state-DB rows that reference it. Best
/// effort per folder: one that can't be removed is logged and
/// skipped rather than failing the batch.
fn delete_local_folders(
    conn: &Connection,
    maildir_root: &std::path::Path,
    actions: Vec<SyncAction>,
) -> Result<()> {
    if actions.is_empty() {
        return Ok(());
    }
    // `maildir_root` is already canonical (Executor::new resolves it via
    // Config::canonical_maildir_root), which is exactly what
    // remove_maildir_tree's contract requires -- pass it straight
    // through. A target that can't be resolved at removal time is
    // guarded per-folder inside remove_maildir_tree.
    for action in actions {
        let SyncAction::DeleteLocalFolder { binding } = action else {
            continue;
        };
        let mailbox_id = binding.jmap_mailbox_id.expect_resolved(
            "delete_local_folders -- binding id must be resolved before reaching the executor",
        );
        crate::maildir_ops::removal::remove_maildir_tree(
            conn,
            maildir_root,
            &binding.maildir_folder,
            Some(mailbox_id),
        )?;
    }
    Ok(())
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

/// Apply the move-pair adopts deferred from `Executor::execute` until
/// after `apply_remote_set`. Skip any whose paired MoveRemote landed in
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

/// Apply the local-side flag updates from a reconcile plan.
fn apply_update_local_flags(
    conn: &Connection,
    maildir_root: &Path,
    self_writes: Option<&SelfWriteCache>,
    actions: Vec<SyncAction>,
) -> Result<usize> {
    let mut succeeded = 0usize;
    for action in actions {
        let SyncAction::UpdateLocalFlags {
            id,
            binding,
            new_flags,
            keywords,
            jmap_blob_id,
            jmap_thread_id,
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
        let maildir_path = maildir_root.join(&binding.maildir_folder);
        let maildir = store::ensure_maildir(&maildir_path)?;
        // A `new/` file has never carried a `:2,<flags>` view that
        // the maildir crate's `set_flags` can find. Reaching it from
        // the JMAP side -- typically the server flipping `$seen` --
        // is the canonical "client first observes this message"
        // moment, so the executor promotes it from `new/` to `cur/`
        // and attaches the new flag set as the info suffix in the
        // same rename. Steady-state cur/ flag updates take the
        // existing path.
        let was_in_new = store::message_is_in_new(&maildir, maildir_id.as_ref());
        let store_result = if was_in_new {
            store::promote_to_cur_with_flags(&maildir, maildir_id.as_ref(), &new_flags)
        } else {
            store::set_flags(&maildir, maildir_id.as_ref(), &new_flags)
        };
        if let Err(e) = store_result {
            warn!(
                "Failed to set flags for {} in {}: {}",
                maildir_id, binding.maildir_folder, e
            );
            continue;
        }
        // Predict the post-rename cur/ filename and feed it to the
        // self-write cache so the watcher drops the fsevents echo of
        // our own rename instead of waking the daemon for a no-op
        // cycle. `new_flags` is already canonical (sorted/deduped)
        // out of `keywords_to_flags`, byte-matching what the maildir
        // crate writes into the suffix. For a new/->cur/ promotion
        // we also predict the cleared new/ side, mirroring the
        // delivery path's prediction shape so MUAs that would have
        // promoted the file independently are still suppressed.
        if let Some(cache) = self_writes {
            let suffix = format!("{}:2,{}", maildir_id.as_ref(), new_flags);
            let mut paths = vec![maildir_path.join("cur").join(&suffix)];
            if was_in_new {
                paths.push(maildir_path.join("new").join(&suffix));
            }
            cache.record(paths);
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
                jmap_mailbox_id: binding
                    .jmap_mailbox_id
                    .expect_resolved(
                        "execute::apply_update_local_flags -- jmap_mailbox_id DB write",
                    )
                    .clone(),
                maildir_id: Some(maildir_id.clone()),
                message_id,
                flags: new_flags.clone(),
                jmap_keywords: keywords_json,
            },
        )?;
        queries::upsert_local_state(&txn, &maildir_id, &binding.maildir_folder, &new_flags, None)?;
        txn.commit()?;
        succeeded += 1;
        info!("Updated local flags for {}: '{}'", bound_for_log, new_flags);
    }
    Ok(succeeded)
}

/// Merge a JMAP keyword patch into our DB's view of the server's
/// keyword set. `existing_json` is `MessageRecord.jmap_keywords` as
/// the DB stores it (a `HashMap<String, bool>` serialized to JSON);
/// `patch` is the keywords map we just sent via `Email/set`'s per-key
/// `keywords/<name>` shape. Returns the merged HashMap.
///
/// JMAP patch semantics under jmap-client's wire shape: a `true`
/// value sets (or keeps) the key on the server; a `false` value
/// serializes as `keywords/<name>: null` and clears the key. Keys
/// not mentioned in the patch are untouched on the server.
///
/// The mirror's whole point is to reflect that: non-standard server
/// keywords ($imported, $hasattachment, $x-me-annot-2, user-defined
/// labels) that adoption captured and that the patch never mentioned
/// must survive the round-trip. Overwriting `jmap_keywords` with
/// the patch alone (the pre-fix behavior) silently truncated those
/// entries out of the local DB even though the server still had
/// them.
///
/// A malformed `existing_json` (corrupted row, schema mismatch)
/// degrades to "start with an empty map and apply the patch" --
/// equivalent to the pre-fix overwrite, but at least the patch
/// itself lands cleanly. Better than panicking in the mirror.
fn apply_keyword_patch(
    existing_json: &str,
    patch: &HashMap<String, bool>,
) -> HashMap<String, bool> {
    let mut merged: HashMap<String, bool> = match serde_json::from_str(existing_json) {
        Ok(m) => m,
        Err(e) => {
            // Self-healing: the next full Email/get round-trip
            // overwrites the column with a fresh serialization.
            // warn! (not error!) per the log-level convention; the
            // row recovers automatically without user intervention.
            warn!(
                "Malformed jmap_keywords JSON; degrading to empty merge base: {}",
                e
            );
            HashMap::new()
        }
    };
    for (k, v) in patch {
        if *v {
            merged.insert(k.clone(), true);
        } else {
            merged.remove(k);
        }
    }
    merged
}

/// DB writes for a single AdoptLocalMessage. Shared between the
/// up-front adopt phase and the post-remote-set move-pair phase so
/// both go through the same row-shape and ordering.
fn commit_adopt(conn: &Connection, action: SyncAction) -> Result<()> {
    let SyncAction::AdoptLocalMessage {
        id,
        binding,
        jmap_blob_id,
        jmap_thread_id,
        keywords,
        filename_flags,
        old_maildir_id,
        old_jmap_email_id,
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
    // Destroy+create-with-shared-Message-ID rebind: drop the old
    // jmap_email_id's row before upserting the new row that targets
    // the same maildir_id. Without this the unique partial index on
    // message_map(maildir_id) refuses the upsert and the whole
    // commit txn rolls back, leaving the cycle stuck because the
    // next iteration will reproduce the same conflicting plan.
    if let Some(old) = old_jmap_email_id.as_ref() {
        queries::delete_message_by_jmap_id(conn, old)?;
    }
    // Split the two flag columns by semantic: `message_map.flags`
    // derives from the server's `keywords` (the standard-six
    // projection of what we believe the server has), while
    // `local_state.flags` is the on-disk filename suffix carried
    // through `filename_flags`. Seeding both from the server-derived
    // value (the pre-fix behavior) caused the next scan's
    // `known_flags != entry.flags` comparison to fire a phantom
    // FlagsChanged for any message whose filename and server
    // keywords disagreed on the standard six -- which then drove an
    // UpdateRemoteKeywords push, whose mirror in apply_remote_set
    // truncated `jmap_keywords` to just the patched standard
    // entries. Two birds with one fix: split the seed source.
    let server_flags = keywords_to_flags(&keywords);
    let keywords_json = serde_json::to_string(&keywords)?;
    queries::upsert_message(
        conn,
        &MessageRecord {
            jmap_email_id: jmap_email_id.clone(),
            jmap_blob_id,
            jmap_thread_id,
            jmap_mailbox_id: binding
                .jmap_mailbox_id
                .expect_resolved("execute::commit_adopt -- jmap_mailbox_id DB write")
                .clone(),
            maildir_id: Some(maildir_id.clone()),
            message_id,
            flags: server_flags,
            jmap_keywords: keywords_json,
        },
    )?;
    queries::upsert_local_state(
        conn,
        &maildir_id,
        &binding.maildir_folder,
        &filename_flags,
        None,
    )?;
    debug!(
        "Adopted {}/{} as {}",
        binding.maildir_folder,
        maildir_id,
        bound_for_log.as_remote()
    );
    Ok(())
}

/// Per-future state for a single upload — extracted from the
/// `SyncAction` so the async block can `move` it without keeping a
/// borrow on the original `Vec<SyncAction>` alive across awaits.
struct UploadJob {
    id: LocalId,
    binding: Arc<MailboxFolderBinding>,
    file_path: PathBuf,
    flags: String,
}

/// Outcome of a single upload future. `Skipped` covers the
/// non-fatal cases: oversize-cap, stat failure, read failure, and
/// the server's `alreadyExists` (logged inside `upload_one`). Hard
/// JMAP errors are returned as `Err` and propagated by the caller.
/// `Uploaded` carries the Email/import response's
/// `oldState`/`newState` pair so the cycle-level cursor ratchet can
/// chain through all imports plus the trailing Email/set.
enum UploadOutcome {
    Uploaded {
        email_id: JmapEmailId,
        chain_old: Option<String>,
        chain_new: Option<String>,
    },
    Skipped,
}

/// Per-category counts of remote operations the server accepted
/// inside `apply_remote_set`. Each field is the remote-side
/// counterpart of one local-side counter the executor computes
/// elsewhere -- summed by `execute` into the matching `SyncOutcome`
/// field. Server-rejected ids (the `outcome.failed_*` sets) are
/// excluded so each count matches what actually changed remotely.
#[derive(Debug, Default)]
struct ApplyRemoteSetCounts {
    keyword_updates: usize,
    moves: usize,
    destroys: usize,
}

/// Aggregated result of `upload_messages`. Carries the per-import
/// chain pairs the executor's section 7.1 ratchet needs, plus a flag that
/// goes false if any successful import returned a missing/empty
/// state. Used by the all-or-nothing combined-chain check at the
/// end of `execute`: if any of our calls (import or set) couldn't
/// supply a complete chain edge, the cycle's combined chain is
/// considered broken and the cursor stays at the pre-ratchet value.
#[derive(Debug, Default)]
struct UploadResults {
    chain_pairs: Vec<(String, String)>,
    chain_intact: bool,
    /// Number of uploads the server accepted this cycle. Excludes
    /// per-upload errors and `UploadOutcome::Skipped` (the import
    /// helper's "server already had this Message-ID" branch), so the
    /// count matches what actually landed remotely rather than what
    /// the executor attempted.
    uploaded: usize,
}

/// Outcome of `run_upload_stream`: per-future result aggregation
/// surfaced to the caller for the post-stream DB commit and the
/// cycle-level chain ratchet.
///
/// `succeeded` carries `(index, JmapEmailId)` pairs; the index
/// points back into the input slice so `commit_uploaded_messages`
/// can recover full upload context (folder, mailbox, flags) for
/// each successful import even though `buffer_unordered` returns
/// out of order.
struct UploadStreamOutcome {
    succeeded: Vec<(usize, JmapEmailId)>,
    chain_pairs: Vec<(String, String)>,
    chain_intact: bool,
    /// First hard error any future returned. Surfaced after the
    /// stream drains so all in-flight uploads still get a chance to
    /// land before the caller propagates the failure.
    hard_error: Option<anyhow::Error>,
}

/// Drive `actions` through `upload_one` in parallel with `concurrency`
/// in-flight. Errors don't abort the stream -- in-flight futures
/// drain and the first hard error is reported back via the outcome's
/// `hard_error` field. Per-future `enumerate` indices flow through
/// each completion so the post-stream DB commit can correlate
/// successes with their originating action even though the stream
/// returns out of order.
async fn run_upload_stream(
    client: &Client,
    actions: &[SyncAction],
    concurrency: usize,
) -> UploadStreamOutcome {
    let futures = actions.iter().enumerate().map(|(i, action)| {
        let SyncAction::UploadMessage {
            id,
            binding,
            file_path,
            flags,
        } = action
        else {
            unreachable!("non-upload in uploads bucket");
        };
        let job = UploadJob {
            id: id.clone(),
            binding: Arc::clone(binding),
            file_path: file_path.clone(),
            flags: flags.clone(),
        };
        async move { (i, upload_one(client, job).await) }
    });
    let mut stream = stream::iter(futures).buffer_unordered(concurrency);

    let mut succeeded: Vec<(usize, JmapEmailId)> = Vec::new();
    let mut chain_pairs: Vec<(String, String)> = Vec::new();
    let mut chain_intact = true;
    let mut hard_error: Option<anyhow::Error> = None;

    while let Some((i, result)) = stream.next().await {
        match result {
            Ok(UploadOutcome::Uploaded {
                email_id,
                chain_old,
                chain_new,
            }) => {
                succeeded.push((i, email_id));
                match (chain_old, chain_new) {
                    (Some(o), Some(n)) => chain_pairs.push((o, n)),
                    _ => chain_intact = false,
                }
            }
            Ok(UploadOutcome::Skipped) => {}
            Err(e) => {
                if hard_error.is_none() {
                    hard_error = Some(e);
                }
            }
        }
    }
    UploadStreamOutcome {
        succeeded,
        chain_pairs,
        chain_intact,
        hard_error,
    }
}

/// One upload's worth of work: read + import. Sync `std::fs::read`
/// is acceptable inside the future — the blocking is bounded by the
/// upload concurrency cap, file IO is brief, and per-future reading
/// keeps memory bounded by N * max_size rather than total_files *
/// max_size if we pre-buffered everything. Reconcile already
/// refused oversized files via `max_upload_size`, so any file that
/// reaches here is within the cap.
async fn upload_one(client: &Client, job: UploadJob) -> Result<UploadOutcome> {
    let UploadJob {
        id,
        binding,
        file_path,
        flags,
    } = job;
    let raw_message = match std::fs::read(&file_path) {
        Ok(b) => b,
        Err(e) => {
            warn!("Failed to read {} for upload: {}", file_path.display(), e);
            return Ok(UploadOutcome::Skipped);
        }
    };
    let keywords = crate::maildir_ops::flags::flags_to_keywords(&flags);

    match jmap_email::import_email(
        client,
        &raw_message,
        binding
            .jmap_mailbox_id
            .expect_resolved("execute::upload_one -- JMAP Email/import target mailbox")
            .as_ref(),
        &binding.maildir_folder,
        &id,
        &keywords,
    )
    .await
    {
        Ok(result) => Ok(UploadOutcome::Uploaded {
            email_id: result.email_id,
            chain_old: result.chain_old,
            chain_new: result.chain_new,
        }),
        Err(e) => {
            let s = e.to_string();
            if s.contains("alreadyExists") {
                // Reconcile's adopt path should have caught this —
                // a same-Message-ID server email already exists in
                // a folder we know about. Hitting this branch means
                // we raced another writer (another mail client
                // uploaded the same message between our scan and
                // our import), or our message_map index is missing
                // a row reconcile would have used. Either way it's
                // self-healing: the next cycle sees the server's
                // copy and adopts. Don't fail the run.
                warn!(
                    "Upload of {} from {} hit alreadyExists; skipping. Reconcile will adopt the existing server copy on the next cycle.",
                    id, binding.maildir_folder
                );
                Ok(UploadOutcome::Skipped)
            } else {
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::MaybeReference;
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
                jmap_mailbox_id: "MB-INBOX".into(),
                maildir_id: Some("M-OLD".into()),
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
            binding: Arc::new(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Value("MB-SPAM".into()),
                server_name: "Spam".to_string(),
                maildir_folder: "Spam".to_string(),
                remote_path: "Spam".to_string(),
            }),
            jmap_blob_id: Some("B1".into()),
            jmap_thread_id: Some("T1".into()),
            keywords,
            filename_flags: "S".into(),
            old_maildir_id: Some("M-OLD".into()),
            old_jmap_email_id: None,
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
            rec.jmap_mailbox_id.as_ref(),
            "MB-INBOX",
            "DB mailbox binding must remain INBOX -- server still has it there"
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
        assert_eq!(rec.jmap_mailbox_id.as_ref(), "MB-SPAM");
        assert_eq!(rec.maildir_id.as_ref().map(AsRef::as_ref), Some("M-NEW"));

        let inbox_state = queries::get_local_state_for_folder(&conn, "INBOX").unwrap();
        assert!(
            !inbox_state.contains_key("M-OLD"),
            "old local_state row must be cleaned up after a successful adopt"
        );
        let spam_state = queries::get_local_state_for_folder(&conn, "Spam").unwrap();
        assert!(spam_state.contains_key("M-NEW"));
    }

    /// `commit_adopt` must split the two flag columns by semantic:
    /// `message_map.flags` derives from the server's `keywords` (the
    /// standard-six projection of what we believe the server has),
    /// while `local_state.flags` is the on-disk filename suffix
    /// carried in through `filename_flags`. Seeding both from the
    /// server-derived value (the pre-fix behavior) caused the next
    /// scan to classify the filename-vs-DB divergence as a phantom
    /// FlagsChanged for any message whose filename suffix and server
    /// keywords disagreed on the standard six, which drove a spurious
    /// UpdateRemoteKeywords push whose mirror truncated `jmap_keywords`
    /// to the patched standard entries -- silent DB corruption of the
    /// full server keyword set.
    #[test]
    fn commit_adopt_splits_message_map_flags_from_local_state_flags() {
        let conn = db::open_in_memory().unwrap();
        let mut keywords = HashMap::new();
        keywords.insert("$flagged".to_string(), true);
        keywords.insert("$seen".to_string(), true);
        keywords.insert("$forwarded".to_string(), true);
        // Plus a non-standard keyword the filename can't carry.
        keywords.insert("$imported".to_string(), true);

        // On-disk filename is `:2,FS` -- no P even though server has
        // $forwarded. The pre-fix code would have written "FPS" into
        // both columns.
        commit_adopt(
            &conn,
            SyncAction::AdoptLocalMessage {
                id: BoundId {
                    maildir_id: "FILE-1".into(),
                    jmap_email_id: "E1".into(),
                    message_id: "a@x".into(),
                },
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                    server_name: "INBOX".to_string(),
                    maildir_folder: "INBOX".to_string(),
                    remote_path: "INBOX".to_string(),
                }),
                jmap_blob_id: Some("B1".into()),
                jmap_thread_id: Some("T1".into()),
                keywords,
                filename_flags: "FS".into(),
                old_maildir_id: None,
                old_jmap_email_id: None,
            },
        )
        .unwrap();

        // message_map.flags: server-derived standard-six projection.
        let rec = queries::get_message_by_jmap_id(&conn, &JmapEmailId::from("E1"))
            .unwrap()
            .expect("E1 must be present after commit_adopt");
        assert_eq!(
            rec.flags, "FPS",
            "message_map.flags must reflect server-derived standard six"
        );
        // jmap_keywords still carries the full set including $imported.
        assert!(
            rec.jmap_keywords.contains("$imported"),
            "jmap_keywords must preserve non-standard keywords: {}",
            rec.jmap_keywords
        );

        // local_state.flags: on-disk filename truth, NOT server-derived.
        let inbox_state = queries::get_local_state_for_folder(&conn, "INBOX").unwrap();
        let (folder, flags) = inbox_state
            .get(&MaildirId::from("FILE-1"))
            .expect("local_state row for FILE-1 must exist");
        assert_eq!(folder, "INBOX");
        assert_eq!(
            flags, "FS",
            "local_state.flags must reflect the on-disk filename suffix, \
             not keywords_to_flags(server) -- the next scan's \
             `known_flags != entry.flags` comparison would otherwise emit \
             a phantom FlagsChanged"
        );
    }

    /// `apply_keyword_patch` must merge the patch into the existing
    /// server-keyword view rather than overwriting. Overwriting
    /// would truncate non-standard server keywords ($imported,
    /// $hasattachment, $x-me-annot-2, user-defined labels) that
    /// adoption captured and that the patch never mentioned --
    /// the server still has them, but the DB's `jmap_keywords`
    /// column would silently lose them. Merge contract: keep keys
    /// not in the patch unchanged, set keys with `true`, remove
    /// keys with `false`.
    #[test]
    fn apply_keyword_patch_preserves_non_standard_keywords_on_additive_patch() {
        let existing = r#"{"$imported":true,"$flagged":true,"$x-me-annot-2":true}"#;
        let mut patch = HashMap::new();
        // Additive patch: just sets $seen. No mention of the existing
        // entries -- they must survive the round-trip.
        patch.insert("$seen".to_string(), true);

        let merged = apply_keyword_patch(existing, &patch);

        assert_eq!(merged.get("$seen"), Some(&true), "new key must be set");
        assert_eq!(
            merged.get("$flagged"),
            Some(&true),
            "existing standard keyword must survive"
        );
        assert_eq!(
            merged.get("$imported"),
            Some(&true),
            "existing non-standard keyword must survive (this was the bug)"
        );
        assert_eq!(
            merged.get("$x-me-annot-2"),
            Some(&true),
            "existing non-standard keyword must survive"
        );
    }

    /// Explicit-six patch (the shape `flags_to_keyword_patch`
    /// emits for adoption LocalWins): standard keywords with
    /// explicit `false` must clear those keys, while non-standard
    /// keywords unmentioned in the patch must survive.
    #[test]
    fn apply_keyword_patch_removes_false_keys_and_preserves_non_standard() {
        let existing = r#"{"$forwarded":true,"$flagged":true,"$seen":true,"$imported":true}"#;
        // Explicit-six patch matching on-disk ":2,FS" (no P): $seen
        // and $flagged stay, $forwarded and the rest clear.
        let mut patch = HashMap::new();
        patch.insert("$flagged".to_string(), true);
        patch.insert("$seen".to_string(), true);
        patch.insert("$forwarded".to_string(), false);
        patch.insert("$draft".to_string(), false);
        patch.insert("$answered".to_string(), false);
        patch.insert("$deleted".to_string(), false);

        let merged = apply_keyword_patch(existing, &patch);

        assert_eq!(merged.get("$flagged"), Some(&true));
        assert_eq!(merged.get("$seen"), Some(&true));
        assert!(
            !merged.contains_key("$forwarded"),
            "false patch must clear the standard keyword: {:?}",
            merged
        );
        assert_eq!(
            merged.get("$imported"),
            Some(&true),
            "non-standard keyword unmentioned in the patch must survive: {:?}",
            merged
        );
        // Keys we cleared shouldn't pollute the merged shape with
        // `false` sentinels -- they're just absent.
        assert!(
            !merged.contains_key("$draft"),
            "absent-in-existing + false-in-patch must stay absent: {:?}",
            merged
        );
    }

    /// Empty or malformed `existing_json` degrades to "apply patch
    /// to an empty map" rather than panicking. Same end-state as
    /// the pre-fix overwrite behavior; preserves the patch itself.
    #[test]
    fn apply_keyword_patch_handles_malformed_existing_json() {
        let mut patch = HashMap::new();
        patch.insert("$seen".to_string(), true);

        // "" is malformed JSON per serde_json (same branch as
        // "not json"), not "from_str of empty == empty map".
        let from_empty = apply_keyword_patch("", &patch);
        let from_garbage = apply_keyword_patch("not json", &patch);

        for merged in [from_empty, from_garbage] {
            assert_eq!(merged.get("$seen"), Some(&true));
            assert_eq!(
                merged.len(),
                1,
                "malformed existing must degrade to patch-only: {:?}",
                merged
            );
        }
    }

    fn edges_from(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(o, n)| ((*o).to_string(), (*n).to_string()))
            .collect()
    }

    /// Empty edge map -> walk is a no-op, cursor passes through.
    #[test]
    fn walk_chain_empty_edges_returns_cursor_unchanged() {
        let edges = HashMap::new();
        assert_eq!(walk_chain(Some("S0".into()), &edges).as_deref(), Some("S0"));
        assert_eq!(walk_chain(None, &edges), None);
    }

    /// Single edge starting at the cursor: ratchet to its newState.
    #[test]
    fn walk_chain_single_edge_advances_cursor() {
        let edges = edges_from(&[("S0", "S1")]);
        assert_eq!(walk_chain(Some("S0".into()), &edges).as_deref(), Some("S1"));
    }

    /// Three edges chained from the cursor: ratchet to the chain's end.
    #[test]
    fn walk_chain_consecutive_edges_advance_cursor() {
        let edges = edges_from(&[("S0", "S1"), ("S1", "S2"), ("S2", "S3")]);
        assert_eq!(walk_chain(Some("S0".into()), &edges).as_deref(), Some("S3"));
    }

    /// Two edges form a chain from the cursor, but a third edge sits
    /// disconnected (a third-party write landed mid-cycle and our
    /// later import resumed from the post-third-party state). All-
    /// or-nothing: don't ratchet at all, cursor stays at S0.
    #[test]
    fn walk_chain_unreachable_edge_breaks_ratchet() {
        let edges = edges_from(&[("S0", "S1"), ("S1", "S2"), ("Sx", "Sy")]);
        assert_eq!(walk_chain(Some("S0".into()), &edges).as_deref(), Some("S0"));
    }

    /// All edges exist but none start at the cursor: no walking
    /// possible, cursor preserved.
    #[test]
    fn walk_chain_cursor_not_in_chain_returns_unchanged() {
        let edges = edges_from(&[("Sx", "Sy")]);
        assert_eq!(walk_chain(Some("S0".into()), &edges).as_deref(), Some("S0"));
    }

    /// `None` cursor with any non-empty chain: nothing to ratchet
    /// against; preserve `None`. (In practice this means reconcile
    /// handed us no `new_email_state` -- bail out cleanly rather
    /// than picking an arbitrary chain start.)
    #[test]
    fn walk_chain_none_cursor_with_non_empty_edges_stays_none() {
        let edges = edges_from(&[("S0", "S1")]);
        assert_eq!(walk_chain(None, &edges), None);
    }

    /// Hostile/buggy server returning state edges that form a cycle
    /// (S0 -> S1 -> S0) must not wedge the walk. Treat as
    /// chain-not-intact: cursor passes through unchanged so the
    /// next cycle's Email/changes catches up.
    #[test]
    fn walk_chain_cyclic_edges_do_not_wedge() {
        let edges = edges_from(&[("S0", "S1"), ("S1", "S0")]);
        assert_eq!(walk_chain(Some("S0".into()), &edges).as_deref(), Some("S0"));
    }

    /// Self-loop edge (S0 -> S0) -- the shape EmailSetOutcome::merge
    /// emits when it coalesces a server state cycle back to the
    /// initial state -- must also terminate.
    #[test]
    fn walk_chain_self_loop_does_not_wedge() {
        let edges = edges_from(&[("S0", "S0")]);
        assert_eq!(walk_chain(Some("S0".into()), &edges).as_deref(), Some("S0"));
    }

    /// Larger cycle (S0 -> S1 -> S2 -> S0) terminates with the
    /// cursor preserved. The guard's bound is independent of cycle
    /// length, so this is belt-and-braces alongside the 2-cycle case.
    #[test]
    fn walk_chain_three_cycle_does_not_wedge() {
        let edges = edges_from(&[("S0", "S1"), ("S1", "S2"), ("S2", "S0")]);
        assert_eq!(walk_chain(Some("S0".into()), &edges).as_deref(), Some("S0"));
    }

    /// Regression pin: a pull-side FS op warn-skipped this cycle (a
    /// MoveLocal or UpdateLocalFlags whose on-disk rename failed, so
    /// its paired DB write never ran). The Email cursor must be HELD
    /// (`None`) even though the chain is intact and reconcile handed
    /// us a fresh state -- advancing would move past the
    /// `Email/changes` delta that produced the skipped action, and
    /// the next cycle could neither re-report it (cursor moved on)
    /// nor re-derive it from the local scan (file + local_state
    /// unchanged), silently stranding the change.
    #[test]
    fn cursor_held_when_pull_apply_incomplete() {
        let edges = edges_from(&[("S0", "S1")]);
        assert_eq!(
            resolve_email_cursor(Some("S1".into()), true, true, &edges),
            None
        );
    }

    /// Baseline: no pull-side skip + intact chain ratchets to the
    /// chain's end, exactly as the bare `walk_chain` would.
    #[test]
    fn cursor_advances_when_pull_side_complete() {
        let edges = edges_from(&[("S0", "S1")]);
        assert_eq!(
            resolve_email_cursor(Some("S0".into()), false, true, &edges).as_deref(),
            Some("S1")
        );
    }

    /// No pull-side skip but a server call returned a missing chain
    /// half (`combined_intact` false): the cursor stays at the
    /// reconcile state without walking, preserving prior behavior.
    #[test]
    fn cursor_preserved_when_chain_not_intact() {
        let edges = edges_from(&[("S0", "S1")]);
        assert_eq!(
            resolve_email_cursor(Some("S0".into()), false, false, &edges).as_deref(),
            Some("S0")
        );
    }

    /// A message downloaded as unread lands in `new/` with no
    /// `:2,...` suffix. When the server later marks it `$seen`,
    /// reconcile emits `UpdateLocalFlags` and the executor must
    /// promote the file from `new/<id>` to `cur/<id>:2,S`, not just
    /// rename it in place: the `maildir` crate's `set_flags` only
    /// searches `cur/`, so a naive call against a `new/` file
    /// silently no-ops and the user's "mark read" gesture never
    /// reaches disk. Pins the promotion step.
    #[test]
    fn update_local_flags_promotes_new_to_cur_when_message_becomes_seen() {
        use crate::maildir_ops::store;
        use std::collections::HashMap;

        let temp = tempfile::tempdir().unwrap();
        let maildir_root = temp.path();
        let inbox = maildir_root.join("INBOX");
        let maildir = store::ensure_maildir(&inbox).unwrap();
        // Deliver the message as unseen-no-flags: lands in INBOX/new/.
        let mid = store::store_message(&maildir, b"raw body", "").unwrap();
        assert_eq!(
            std::fs::read_dir(inbox.join("new")).unwrap().count(),
            1,
            "precondition: file must be in INBOX/new/ before the test fires"
        );

        let conn = db::open_in_memory().unwrap();
        queries::upsert_message(
            &conn,
            &MessageRecord {
                jmap_email_id: "E1".into(),
                jmap_blob_id: Some("B1".into()),
                jmap_thread_id: Some("T1".into()),
                jmap_mailbox_id: "MB-INBOX".into(),
                maildir_id: Some(mid.clone()),
                message_id: "a@x".into(),
                flags: "".into(),
                jmap_keywords: "{}".into(),
            },
        )
        .unwrap();
        queries::upsert_local_state(&conn, &mid, "INBOX", "", None).unwrap();

        let mut keywords = HashMap::new();
        keywords.insert("$seen".to_string(), true);
        let action = SyncAction::UpdateLocalFlags {
            id: BoundId {
                maildir_id: mid.clone(),
                jmap_email_id: "E1".into(),
                message_id: "a@x".into(),
            },
            binding: Arc::new(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                server_name: "INBOX".to_string(),
                maildir_folder: "INBOX".to_string(),
                remote_path: "INBOX".to_string(),
            }),
            new_flags: "S".into(),
            keywords,
            jmap_blob_id: "B1".into(),
            jmap_thread_id: "T1".into(),
        };

        let count = apply_update_local_flags(&conn, maildir_root, None, vec![action]).unwrap();
        assert_eq!(count, 1, "the flag update must report success");

        // File-on-disk assertion -- the user-visible outcome.
        let cur_entries: Vec<String> = std::fs::read_dir(inbox.join("cur"))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            cur_entries.len(),
            1,
            "exactly one file should land in INBOX/cur/ after promotion"
        );
        assert!(
            cur_entries[0].ends_with(":2,S"),
            "promoted file must carry `:2,S` suffix (got {:?})",
            cur_entries[0]
        );
        assert_eq!(
            std::fs::read_dir(inbox.join("new")).unwrap().count(),
            0,
            "INBOX/new/ must be empty after the promotion to cur/"
        );

        // DB assertion -- message_map.flags reflects the new state.
        let rec = queries::get_message_by_jmap_id(&conn, &JmapEmailId::from("E1"))
            .unwrap()
            .expect("E1 must still exist");
        assert_eq!(
            rec.flags, "S",
            "message_map.flags must update alongside the file"
        );
    }

    /// When the daemon is running with a self-write cache attached,
    /// a new/->cur/ promotion driven by an `UpdateLocalFlags` action
    /// must register both the cleared `new/<id>:2,<new_flags>` path
    /// and the populated `cur/<id>:2,<new_flags>` path with the
    /// cache, mirroring the download path's prediction shape. Without
    /// this, the watcher's fsevents stream surfaces our own rename as
    /// a phantom local change, the trigger loop wakes, and reconcile
    /// pays an `Email/changes` round-trip per server-side `$seen`
    /// flip.
    #[test]
    fn update_local_flags_records_self_writes_on_promote() {
        use crate::maildir_ops::store;
        use crate::sync::self_writes::SelfWriteCache;
        use std::collections::HashMap;
        use std::time::Duration;

        let temp = tempfile::tempdir().unwrap();
        let maildir_root = temp.path();
        let inbox = maildir_root.join("INBOX");
        let maildir = store::ensure_maildir(&inbox).unwrap();
        let mid = store::store_message(&maildir, b"raw body", "").unwrap();

        let conn = db::open_in_memory().unwrap();
        queries::upsert_message(
            &conn,
            &MessageRecord {
                jmap_email_id: "E1".into(),
                jmap_blob_id: Some("B1".into()),
                jmap_thread_id: Some("T1".into()),
                jmap_mailbox_id: "MB-INBOX".into(),
                maildir_id: Some(mid.clone()),
                message_id: "a@x".into(),
                flags: "".into(),
                jmap_keywords: "{}".into(),
            },
        )
        .unwrap();
        queries::upsert_local_state(&conn, &mid, "INBOX", "", None).unwrap();

        let mut keywords = HashMap::new();
        keywords.insert("$seen".to_string(), true);
        let action = SyncAction::UpdateLocalFlags {
            id: BoundId {
                maildir_id: mid.clone(),
                jmap_email_id: "E1".into(),
                message_id: "a@x".into(),
            },
            binding: Arc::new(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
                server_name: "INBOX".to_string(),
                maildir_folder: "INBOX".to_string(),
                remote_path: "INBOX".to_string(),
            }),
            new_flags: "S".into(),
            keywords,
            jmap_blob_id: "B1".into(),
            jmap_thread_id: "T1".into(),
        };

        let cache = SelfWriteCache::new(Duration::from_secs(5));
        let count = apply_update_local_flags(&conn, maildir_root, Some(&cache), vec![action])
            .expect("apply_update_local_flags");
        assert_eq!(count, 1);

        // The watcher's fast path probes the cache against the batch
        // it sees. For a new/->cur/ promotion the predicted paths are
        // both sides of the rename, so the watcher's batch-of-two for
        // our own rename matches cleanly and `matches_all` evicts.
        let cur_path = inbox.join("cur").join(format!("{}:2,S", mid.as_ref()));
        let new_path = inbox.join("new").join(format!("{}:2,S", mid.as_ref()));
        assert!(
            cache.matches_all(&[cur_path, new_path]),
            "self-write cache must hold both sides of the new/->cur/ promotion"
        );
    }

    mod resolve_upload_refs {
        use super::*;
        use crate::ids::MessageId;
        use crate::sync::plan::LocalId;

        fn upload_with_binding(binding: Arc<MailboxFolderBinding>) -> SyncAction {
            SyncAction::UploadMessage {
                id: LocalId {
                    maildir_id: MaildirId::from("FOO.host"),
                    message_id: MessageId::from("msg-1@example.com"),
                },
                binding,
                file_path: std::path::PathBuf::from("/tmp/jma-test/Archive/cur/FOO.host"),
                flags: "S".to_string(),
            }
        }

        fn reference_binding(folder: &str) -> Arc<MailboxFolderBinding> {
            Arc::new(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Reference(folder.to_string()),
                server_name: folder.to_string(),
                maildir_folder: folder.to_string(),
                remote_path: folder.to_string(),
            })
        }

        /// Reference matches a `creation_refs` entry: swap to
        /// `Value(id)` so the upload commit's
        /// `expect_resolved` succeeds.
        #[test]
        fn resolved_reference_swaps_to_value() {
            let mut refs = HashMap::new();
            refs.insert("Archive".to_string(), JmapMailboxId::from("MB-NEW-2"));
            let uploads = vec![upload_with_binding(reference_binding("Archive"))];

            let out = resolve_upload_references(uploads, &refs);

            assert_eq!(out.len(), 1);
            match &out[0] {
                SyncAction::UploadMessage { binding, .. } => match &binding.jmap_mailbox_id {
                    MaybeReference::Value(id) => assert_eq!(id.as_ref(), "MB-NEW-2"),
                    other => panic!("expected Value, got {:?}", other),
                },
                other => panic!("expected UploadMessage, got {:?}", other),
            }
        }

        /// Unresolved reference (parent create failed or
        /// rejected): warn-and-drop the upload so the next
        /// cycle can re-emit.
        #[test]
        fn unresolved_reference_drops_action() {
            let refs = HashMap::new();
            let uploads = vec![upload_with_binding(reference_binding("Archive"))];

            let out = resolve_upload_references(uploads, &refs);

            assert!(
                out.is_empty(),
                "unresolved Reference must drop the upload; got {:?}",
                out
            );
        }

        /// Already-resolved Value bindings pass through
        /// unchanged.
        #[test]
        fn value_binding_passes_through() {
            let refs = HashMap::new();
            let value_binding = Arc::new(MailboxFolderBinding {
                jmap_mailbox_id: MaybeReference::Value(JmapMailboxId::from("MB-INBOX")),
                server_name: "Inbox".to_string(),
                maildir_folder: "INBOX".to_string(),
                remote_path: "Inbox".to_string(),
            });
            let uploads = vec![upload_with_binding(value_binding)];

            let out = resolve_upload_references(uploads, &refs);

            assert_eq!(out.len(), 1);
            match &out[0] {
                SyncAction::UploadMessage { binding, .. } => match &binding.jmap_mailbox_id {
                    MaybeReference::Value(id) => assert_eq!(id.as_ref(), "MB-INBOX"),
                    other => panic!("expected Value, got {:?}", other),
                },
                other => panic!("expected UploadMessage, got {:?}", other),
            }
        }
    }
}
