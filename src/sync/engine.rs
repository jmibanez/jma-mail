use anyhow::Result;
use jmap_client::client::Client;
use rusqlite::Connection;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::ids::{JmapAccountId, JmapEmailId, JmapMailboxId};
use crate::jmap::{
    email as jmap_email, limits, mailbox as jmap_mailbox, session,
    types::{EmailObject, SessionInfo},
};
use crate::maildir_ops::dedupe::{LocalEntry, LocalIndex};
use crate::maildir_ops::{dedupe, scan, store};
use crate::state::queries;
use crate::sync::execute::Executor;
use crate::sync::plan::{SyncAction, SyncDirection};
use crate::sync::reconcile::{self, MessageRecordIndex, ReconcileInput};

/// Outcome of one sync iteration -- enough for the daemon loop to know
/// whether to fire the post-arrival hook and to flag a degraded cycle.
#[derive(Debug, Default, Clone, Copy)]
pub struct SyncOutcome {
    pub downloaded: usize,
    /// Per-id failures from the remote-side Email/set batch
    /// (notUpdated + notDestroyed). Each one is also logged at warn
    /// level with id and folder context; this count is the
    /// at-a-glance summary so the user doesn't have to grep across a
    /// long cycle log to notice anything went wrong. These conditions
    /// are self-healing -- the next cycle re-detects and re-attempts
    /// each rejected action -- so they stay below the `error!` bar.
    pub failed_remote_actions: usize,
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
    client: Client,
    conn: &'a Connection,
    config: &'a Config,
    account_id: JmapAccountId,
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
            client,
            conn,
            config,
            account_id,
        })
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
            .run(dry_run, SyncDirection::Both)
            .await
    }

    /// Connect a fresh engine and pull only (server -> local). Adoption
    /// still runs.
    pub async fn pull_only(conn: &'a Connection, config: &'a Config) -> Result<SyncOutcome> {
        Self::connect(conn, config)
            .await?
            .run(false, SyncDirection::PullOnly)
            .await
    }

    /// Connect a fresh engine and push only (local -> server). Adoption
    /// still runs.
    pub async fn push_only(conn: &'a Connection, config: &'a Config) -> Result<()> {
        Self::connect(conn, config)
            .await?
            .run(false, SyncDirection::PushOnly)
            .await?;
        Ok(())
    }

    /// Single orchestration path. `direction` selects which side(s) of
    /// the plan execute; adoption always runs. Public so the daemon
    /// can drive its long-lived engine across triggers without
    /// reconnecting.
    pub async fn run(&self, dry_run: bool, direction: SyncDirection) -> Result<SyncOutcome> {
        let mailboxes = self.resolve_mailboxes().await?;
        let maildir_root = self.config.maildir_path();

        // Phase 0: dedupe + (conditionally) index. Always before scan so
        // newly-introduced duplicates from a prior aborted run don't get
        // treated as local changes to push.
        //
        // Reconcile only consults `LocalIndex` when its DB-derived
        // lookup misses (initial sync, post-`c7a86e6`-recovery wipe, or
        // any other state where `message_map` is empty). In steady
        // state every remote Message-ID resolves through the DB, the
        // index is allocated and never read. Skip the build entirely
        // when the DB has rows: dedupe still runs (its delete behavior
        // is unconditional), but its `on_kept` callback is a no-op and
        // `LocalIndex` stays at default-empty. Empty-HashMap lookups
        // return None, which is exactly what the existing reconcile
        // stage-2 check already handles.
        let folder_names: Vec<String> = mailboxes.iter().map(|(_, f)| f.clone()).collect();
        let mut local_index = LocalIndex::default();
        if queries::has_message_map_rows(self.conn)? {
            dedupe::dedupe(&maildir_root, &folder_names, |_, _, _| ())?;
        } else {
            dedupe::dedupe(&maildir_root, &folder_names, |folder, msgid, mid| {
                local_index
                    .by_message_id
                    .entry(msgid.clone())
                    .or_default()
                    .push(LocalEntry {
                        folder: folder.to_string(),
                        maildir_id: mid.clone(),
                    });
            })?;
        }

        // Phase 1: scan local changes.
        let mut all_local_changes = Vec::new();
        for (_, folder_name) in &mailboxes {
            let maildir_path = maildir_root.join(folder_name);
            let maildir = store::ensure_maildir(&maildir_path)?;
            let known_state = queries::get_local_state_for_folder(self.conn, folder_name)?;
            let (changes, _seen) = scan::scan_folder(&maildir, folder_name, &known_state)?;
            all_local_changes.extend(changes);
        }

        // Phase 2: collect remote changes.
        let (remote_emails, remote_destroyed, new_state, used_initial_path) =
            self.fetch_remote_state(&mailboxes).await?;

        // Phase 3: build known indices and reconcile.
        let known = build_known_indices(self.conn, &mailboxes)?;

        let plan = reconcile::reconcile(ReconcileInput {
            remote_emails: &remote_emails,
            remote_destroyed: &remote_destroyed,
            local_changes: &all_local_changes,
            known: &known,
            local_index: &local_index,
            mailboxes: &mailboxes,
            strategy: self.config.sync.conflict_strategy,
            new_email_state: Some(new_state),
            max_upload_size: limits::max_size_upload(&self.client),
        });

        if dry_run {
            print!("{}", plan);
            return Ok(SyncOutcome::default());
        }

        if plan.is_empty() {
            info!("Already in sync");
            return Ok(SyncOutcome::default());
        }

        debug!("Plan to execute {}", plan);

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
        let executor = Executor::new(&self.client, self.conn, self.config);
        let outcome = executor.execute(filtered).await?;

        if outcome.failed_remote_actions > 0 {
            warn!(
                "Cycle completed with {} rejected remote action(s); see preceding warnings",
                outcome.failed_remote_actions
            );
        }

        if used_initial_path {
            info!("Initial sync complete ({} downloaded)", outcome.downloaded);
        } else {
            info!("Sync complete ({} downloaded)", outcome.downloaded);
        }
        Ok(outcome)
    }

    /// Resolve the list of mailboxes to sync, returning (jmap_id, folder_name) pairs.
    pub async fn resolve_mailboxes(&self) -> Result<Vec<(JmapMailboxId, String)>> {
        let remote_mailboxes = jmap_mailbox::get_all(&self.client).await?;

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
                &self.config.sync.mailboxes,
                mb,
                self.config.sync.case_insensitive_match,
            ) {
                continue;
            }

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

        info!("Syncing {} mailboxes", synced.len());
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
        for (mailbox_id, folder_name) in mailboxes {
            let ids =
                jmap_email::query_mailbox(&self.client, mailbox_id.as_ref(), folder_name).await?;
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
        let mut out: Vec<EmailObject> = Vec::new();
        let chunk_size = limits::max_objects_in_get(&self.client);
        for chunk in ids.chunks(chunk_size) {
            let batch = jmap_email::get_by_ids(&self.client, chunk).await?;
            out.extend(batch);
        }
        Ok(out)
    }
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
}
