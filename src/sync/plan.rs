use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use crate::ids::{JmapBlobId, JmapEmailId, JmapMailboxId, JmapThreadId, MaildirId, MessageId};
use crate::jmap::types::{MailboxFolderBinding, MaybeReference};

/// A message known by its local maildir handle. The Message-ID rides
/// along so logs can name the message in human-readable form, and so
/// downstream reconcile/execute can use it as the idempotency anchor
/// without re-parsing the file. Required: scan refuses to construct a
/// `LocalId` for a file with no parseable Message-ID, so by the time
/// any code holds one, the id is guaranteed.
#[derive(Debug, Clone)]
pub struct LocalId {
    pub maildir_id: MaildirId,
    pub message_id: MessageId,
}

/// A message known by its opaque JMAP server id. Carries the
/// Message-ID for human-readable logging and as the cross-boundary
/// idempotency anchor: scan and reconcile both refuse to construct a
/// `RemoteId` for a message without one (see `maildir_ops::scan` and
/// `sync::reconcile::process_remote_emails`), so by the time any
/// downstream code holds one, the id is guaranteed.
#[derive(Debug, Clone)]
pub struct RemoteId {
    pub jmap_email_id: JmapEmailId,
    pub message_id: MessageId,
}

/// A message bound on both sides — same RFC 5322 message known
/// locally as `maildir_id` and remotely as `jmap_email_id`. Same
/// Message-ID guarantee as `LocalId` and `RemoteId`.
#[derive(Debug, Clone)]
pub struct BoundId {
    pub maildir_id: MaildirId,
    pub jmap_email_id: JmapEmailId,
    pub message_id: MessageId,
}

impl fmt::Display for LocalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.maildir_id, self.message_id)
    }
}

impl fmt::Display for RemoteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.jmap_email_id, self.message_id)
    }
}

impl fmt::Display for BoundId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{} ({})",
            self.maildir_id, self.jmap_email_id, self.message_id
        )
    }
}

impl BoundId {
    pub fn as_remote(&self) -> RemoteId {
        RemoteId {
            jmap_email_id: self.jmap_email_id.clone(),
            message_id: self.message_id.clone(),
        }
    }
}

/// A single action to perform during sync.
///
/// Variants that touch a specific mailbox carry an
/// `Arc<MailboxFolderBinding>` rather than separate `maildir_folder`
/// and `mailbox_id` fields. The binding is resolved once at the
/// producer boundary (scan for local-driven actions, reconcile for
/// remote-driven ones) and propagated through the plan and executor
/// as a single typed unit; consumers read `binding.maildir_folder`
/// for filesystem ops and
/// `binding.jmap_mailbox_id.expect_resolved("...")` for DB writes
/// without re-resolving against `MailboxBindings`. The
/// `expect_resolved` step unwraps the `MaybeReference<JmapMailboxId>`
/// wrapper -- see `MaybeReference` for why the field carries that
/// discriminator.
#[derive(Debug)]
pub enum SyncAction {
    // Server -> Local
    DownloadMessage {
        id: RemoteId,
        jmap_blob_id: JmapBlobId,
        jmap_thread_id: JmapThreadId,
        binding: Arc<MailboxFolderBinding>,
        keywords: HashMap<String, bool>,
    },
    UpdateLocalFlags {
        id: BoundId,
        binding: Arc<MailboxFolderBinding>,
        new_flags: String,
        keywords: HashMap<String, bool>,
        jmap_blob_id: JmapBlobId,
        jmap_thread_id: JmapThreadId,
    },
    DeleteLocal {
        id: BoundId,
        /// On-disk folder of the file we're deleting. Plain string
        /// because the executor only joins the maildir root with this
        /// path to find the file; the row's `jmap_email_id` for the
        /// DB delete already lives on `id`, and `local_state` is
        /// keyed by `maildir_id` (also on `id`), so the source's
        /// mailbox identity isn't consumed.
        maildir_folder: String,
    },
    MoveLocal {
        id: BoundId,
        /// Source folder string only -- the executor reads this to
        /// construct the on-disk path; the source's `jmap_mailbox_id`
        /// isn't consumed (local_state's row is keyed by `maildir_id`
        /// and just gets overwritten by the destination write, and
        /// message_map's `mailbox_id` is updated to `to_binding.
        /// jmap_mailbox_id` directly). Asymmetric with `to_binding`
        /// deliberately: carrying a `from_binding` we never read
        /// would mislead readers into thinking the source's identity
        /// matters here.
        from_folder: String,
        to_binding: Arc<MailboxFolderBinding>,
    },

    /// Bind an existing local file to a known server email (no download,
    /// no upload — pure DB write). Pre-empts the alreadyExists path on
    /// push and the redundant download path on pull.
    AdoptLocalMessage {
        id: BoundId,
        binding: Arc<MailboxFolderBinding>,
        jmap_blob_id: Option<JmapBlobId>,
        jmap_thread_id: Option<JmapThreadId>,
        keywords: HashMap<String, bool>,
        /// On-disk maildir filename flag suffix at plan time -- the
        /// filesystem-truth view of flags, as opposed to `keywords`
        /// (server's full keyword set including non-standard
        /// entries). `commit_adopt` writes this into
        /// `local_state.flags` so the next scan's filename-vs-DB
        /// comparison sees agreement, while `message_map.flags` is
        /// derived from `keywords` to keep our "what we believe the
        /// server has" view consistent. Populated by each emit site
        /// from whatever filesystem-truth source it has: scan's
        /// `local_flags` map, a `LocalChange::NewMessage.flags`
        /// field, or a `DetectedMove.new_flags`.
        filename_flags: String,
        /// When the adopt rebinds an existing JMAP id from one local
        /// maildir_id to another (cross-folder local move), the old
        /// local_state row needs to be cleaned up so subsequent scans
        /// don't keep emitting DeletedMessage for it. Just a bare
        /// `MaildirId` (not a `LocalId` bundle) — this is purely a
        /// DB-cleanup hint, never logged as identity.
        old_maildir_id: Option<MaildirId>,
        /// When the adopt rebinds an existing maildir_id from one JMAP
        /// email to another -- the remote analog of `old_maildir_id`,
        /// used when the server destroyed Email A and created Email B
        /// with the same wire-format Message-ID, and reconcile has
        /// paired them into a single rebind action so the new row
        /// (jmap_email_id=B, maildir_id=FILE-1) doesn't collide with
        /// the old row (jmap_email_id=A, maildir_id=FILE-1) under the
        /// unique-index-on-maildir-id constraint. commit_adopt deletes
        /// the A row inside the same txn before upserting B. Skipped
        /// here means: the old message_map row stays put. A DB-cleanup
        /// hint, never an identity.
        old_jmap_email_id: Option<JmapEmailId>,
    },

    // Local -> Server
    UploadMessage {
        id: LocalId,
        binding: Arc<MailboxFolderBinding>,
        file_path: PathBuf,
        /// Maildir flags suffix captured at scan time. Plumbed
        /// through from `LocalChange::NewMessage` so the executor
        /// doesn't have to re-parse the on-disk filename, and so
        /// the keywords we upload match the ones the plan was
        /// built against.
        flags: String,
    },
    UpdateRemoteKeywords {
        id: RemoteId,
        keywords: HashMap<String, bool>,
        /// On-disk maildir filename flag suffix at the time the
        /// patch was emitted -- the filesystem-truth view of flags,
        /// mirrored by every emit site from the same source it used
        /// to build the patch (a `LocalChange::FlagsChanged.new_
        /// flags`, a `DetectedMove.new_flags`, or the filename
        /// suffix that drove `flags_to_keyword_patch` in adoption
        /// reconciliation). `apply_remote_set`'s mirror writes this
        /// into `local_state.flags` so the next scan's `known_
        /// flags != entry.flags` comparison stays consistent with
        /// the filesystem, without depending on the implicit
        /// (and fragile) invariant that `keywords_to_flags(patch)
        /// == filename` -- an invariant that holds for additive
        /// patches built from `flags_to_keywords(filename)` but
        /// breaks for any other patch shape.
        filename_flags: String,
    },
    DestroyRemote {
        id: RemoteId,
    },
    MoveRemote {
        id: RemoteId,
        /// Full target set of mailbox ids the email should belong to
        /// after the move. We send this as a full-replacement
        /// `mailboxIds` in Email/set rather than a per-key patch
        /// because jmap-client 0.4.1 cannot serialize a `null` value
        /// for `mailboxIds/{id}` (its patch map is typed `bool`), and
        /// servers like Fastmail correctly reject `false` for a
        /// `Id[Boolean]` set-membership map. Computed at planning
        /// time; for jma's single-mailbox-per-email DB model
        /// this is just `[<destination jmap_mailbox_id>]`.
        target_mailbox_ids: Vec<JmapMailboxId>,
        /// Folder names of the source and destination mailboxes.
        /// Plain strings rather than `Arc<MailboxFolderBinding>`
        /// because the executor never reads the mailbox ids off
        /// these slots -- the on-wire move is driven by
        /// `target_mailbox_ids`, and the strings are purely for log
        /// messages naming the folders in human-readable form.
        from_folder: String,
        to_folder: String,
    },

    /// Folder-level constructive action mirroring a server-side
    /// mailbox creation on disk: `ensure_maildir` (create
    /// `cur/`/`new/`/`tmp/` under `maildir_root/binding.maildir_
    /// folder`) plus `sentinel::write` (`.jma.mapping` TOML
    /// carrying the JMAP id triple). Emitted by reconcile for
    /// every server-known mailbox whose on-disk state is
    /// inconsistent with the binding (folder absent, sentinel
    /// absent, or sentinel content stale).
    ///
    /// `parent_jmap_mailbox_id` is the JMAP parent the server
    /// returned for this mailbox; the executor stamps it into
    /// the sentinel so the parent chain can be reconstructed
    /// from disk after a state-DB nuke. `None` for top-level
    /// mailboxes.
    CreateLocalMailbox {
        binding: Arc<MailboxFolderBinding>,
        parent_jmap_mailbox_id: Option<JmapMailboxId>,
        /// `Some(dead_id)` when this create represents a resurrect
        /// of a mailbox the user previously deleted locally: the
        /// executor must drop stale `message_map` rows pointing at
        /// `dead_id` before downstream `DownloadMessage` actions
        /// write fresh rows for the same id. `None` for ordinary
        /// first-cycle creates (no cached rows exist for the id).
        replaces_orphan_id: Option<JmapMailboxId>,
    },

    /// Folder-level pull-side destructive action: remove a
    /// local maildir whose remote counterpart was deleted
    /// server-side. The executor walks
    /// `fs::remove_dir_all(maildir_root.join(binding.
    /// maildir_folder))` and cascades the cache cleanup:
    /// `delete_messages_by_jmap_mailbox_id` on the dead id,
    /// `delete_local_state_by_folder` on the folder string,
    /// and `delete_folder_checkpoint` so the next cycle's
    /// dedupe dirty-check doesn't trip.
    ///
    /// The executor refuses any binding whose resolved path
    /// escapes `maildir_root` (path traversal). The guard is
    /// defense-in-depth -- `maildir_folder` is producer-
    /// controlled, not user input.
    DeleteLocalFolder {
        binding: Arc<MailboxFolderBinding>,
    },

    /// Folder-level pull-side action mirroring a server-side
    /// mailbox rename on disk: `fs::rename` the maildir from
    /// `from_folder` to `binding.maildir_folder` under
    /// `maildir_root`, rewrite every `local_state.maildir_folder`
    /// row that pointed at the old folder, and refresh the
    /// `.jma.mapping` sentinel at the new path (the server name
    /// may have changed alongside the path). Emitted by reconcile
    /// when the cached `mailbox_map.maildir_folder` for an
    /// already-known mailbox id disagrees with the freshly
    /// resolved name -- either a direct rename or a parent move
    /// that changed the resolved path under the configured layout.
    ///
    /// `parent_jmap_mailbox_id` rides along for the sentinel
    /// refresh, mirroring `CreateLocalMailbox`. `None` for
    /// top-level mailboxes.
    ///
    /// Same-cycle idempotency on the source-missing/target-
    /// present shape lets a parent's `fs::rename` move the
    /// descendant subtree in one call: each descendant's own
    /// queued action then runs its local_state + sentinel
    /// catch-up against the already-moved folder.
    /// `from_folder` is the filesystem source -- where `fs::rename`
    /// reads from. `from_db_folder` is the DB-rewrite source --
    /// where `local_state.maildir_folder` matches the row that
    /// needs its column rewritten. The two are equal for a clean
    /// server-side rename (cache, server, disk all agreed at
    /// cycle start; only the server has moved); they diverge for
    /// `ConflictServerWins` where the user `mv`-ed locally to a
    /// third name -- `from_folder` is the disk path the user
    /// produced, `from_db_folder` is the cached path the row was
    /// last written at. Mirrors the asymmetric shape of
    /// `MoveLocal`, where the fs source is a plain string and the
    /// DB key is a separate identity field.
    RenameLocalMailbox {
        from_folder: String,
        from_db_folder: String,
        binding: Arc<MailboxFolderBinding>,
        parent_jmap_mailbox_id: Option<JmapMailboxId>,
    },

    /// Folder-level push-side primitive: `Mailbox/set { create }`
    /// with the given `name`, optional `parent_jmap_mailbox_id`,
    /// and optional `role`. Symmetric with `CreateLocalMailbox`.
    /// The executor issues the JMAP call; the next sync cycle's
    /// `Mailbox/get` picks up the server-assigned id, which
    /// `resolve_mailboxes` then upserts into `mailbox_map`.
    ///
    /// `parent_jmap_mailbox_id` is `None` for top-level
    /// mailboxes. For a nested create whose parent already
    /// exists server-side, the field carries
    /// `Some(MaybeReference::Value(parent_id))`. For the chained-
    /// create case (a Flat folder `Foo.Bar.Baz` where neither
    /// `Foo` nor `Foo.Bar` exist server-side yet, so both parents
    /// land via earlier `CreateRemoteMailbox` actions in the same
    /// plan), the field carries `Some(MaybeReference::Reference(
    /// parent_folder))` and the executor's `creation_refs` table
    /// resolves it once the parent's own create returns. Top-down
    /// emission order in the plan guarantees the parent has run
    /// before the child fires.
    ///
    /// `folder` is the on-disk folder string this create
    /// corresponds to (the same shape as
    /// `mailbox_map.maildir_folder`). The executor uses it as
    /// the `creation_refs` key so a child's
    /// `MaybeReference::Reference(folder)` resolves to the
    /// server-assigned id once the parent's create returns.
    ///
    /// `role` is the JMAP `role` property (`"inbox"`,
    /// `"archive"`, ...) when the local creation maps to a
    /// well-known mailbox role, or `None` for a plain folder.
    /// Per RFC 8621 the server may reject or silently coerce
    /// role assignment, so callers should not depend on it
    /// landing on the server side.
    CreateRemoteMailbox {
        name: String,
        parent_jmap_mailbox_id: Option<MaybeReference<JmapMailboxId>>,
        role: Option<String>,
        folder: String,
        /// When this create resurrects a local orphan (a
        /// mailbox the server deleted while the disk side
        /// survived), the dead `jmap_mailbox_id` whose
        /// `message_map` rows should be cleaned up before the
        /// new create runs. The executor deletes those rows
        /// inline so the cycle's subsequent upserts (one per
        /// `UploadMessage` re-binding the file to the freshly
        /// created mailbox) can land without collision against
        /// the unique-on-`maildir_id` index. `None` for a
        /// regular (non-resurrect) create.
        replaces_orphan_id: Option<JmapMailboxId>,
    },

    /// Folder-level push-side primitive: `Mailbox/set { update }`
    /// rewriting `name` and `parentId` for an existing mailbox.
    /// Symmetric with `RenameLocalMailbox`. Emitted when the
    /// `.jma.mapping` sentinel walk finds an already-cached
    /// mailbox at a disk path other than the one the cache
    /// (and the server) currently records -- the user renamed
    /// or reparented the maildir locally, and the server should
    /// follow.
    ///
    /// `from_folder` is the pre-rename `mailbox_map.maildir_
    /// folder` value (the executor uses it for the `local_state`
    /// rewrite alongside the cache upsert). `binding` is the
    /// post-rename state: `binding.server_name` is the name to
    /// push, `binding.maildir_folder` is where the sentinel
    /// already is on disk and where the cache should land after
    /// the JMAP call succeeds.
    ///
    /// `parent_jmap_mailbox_id` is `None` for top-level
    /// mailboxes. A reparent requires the new parent to already
    /// exist on the server.
    RenameRemoteMailbox {
        from_folder: String,
        binding: Arc<MailboxFolderBinding>,
        parent_jmap_mailbox_id: Option<JmapMailboxId>,
    },
}

impl SyncAction {
    /// Which side of a sync this action belongs to.
    pub fn direction(&self) -> ActionDirection {
        match self {
            SyncAction::DownloadMessage { .. }
            | SyncAction::UpdateLocalFlags { .. }
            | SyncAction::DeleteLocal { .. }
            | SyncAction::MoveLocal { .. }
            | SyncAction::CreateLocalMailbox { .. }
            | SyncAction::RenameLocalMailbox { .. }
            | SyncAction::DeleteLocalFolder { .. } => ActionDirection::Pull,
            SyncAction::UploadMessage { .. }
            | SyncAction::UpdateRemoteKeywords { .. }
            | SyncAction::DestroyRemote { .. }
            | SyncAction::MoveRemote { .. }
            | SyncAction::CreateRemoteMailbox { .. }
            | SyncAction::RenameRemoteMailbox { .. } => ActionDirection::Push,
            SyncAction::AdoptLocalMessage { .. } => ActionDirection::Both,
        }
    }
}

/// Which direction(s) an action moves data in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionDirection {
    Pull,
    Push,
    Both,
}

/// Top-level sync mode. Selects which side(s) of the plan execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    Both,
    PullOnly,
    PushOnly,
}

/// A computed plan of sync actions to execute.
#[derive(Debug, Default)]
pub struct SyncPlan {
    pub actions: Vec<SyncAction>,
    pub new_email_state: Option<String>,
}

impl SyncPlan {
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn download_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, SyncAction::DownloadMessage { .. }))
            .count()
    }

    pub fn upload_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, SyncAction::UploadMessage { .. }))
            .count()
    }

    pub fn flag_update_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    SyncAction::UpdateLocalFlags { .. } | SyncAction::UpdateRemoteKeywords { .. }
                )
            })
            .count()
    }

    pub fn delete_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    SyncAction::DeleteLocal { .. } | SyncAction::DestroyRemote { .. }
                )
            })
            .count()
    }

    /// Folder-level constructive actions in the plan. Called out
    /// in the dry-run summary so a first-cycle pull (which
    /// creates one per synced mailbox) doesn't read as a wall of
    /// generic downloads.
    pub fn folder_create_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, SyncAction::CreateLocalMailbox { .. }))
            .count()
    }

    /// Server-side mailbox creations queued in the plan.
    /// Symmetric with `folder_create_count` on the push side.
    pub fn remote_folder_create_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, SyncAction::CreateRemoteMailbox { .. }))
            .count()
    }

    /// Local-side mailbox renames queued in the plan, one per
    /// mailbox whose cached folder name disagreed with the
    /// freshly resolved one (direct server rename or a parent
    /// move that re-resolved the path under the configured
    /// layout). Called out in the dry-run summary so a cascade
    /// of N entries reads as "the server renamed N folders we
    /// were tracking" rather than as opaque action noise.
    pub fn folder_rename_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, SyncAction::RenameLocalMailbox { .. }))
            .count()
    }

    /// Server-side mailbox renames queued in the plan, one per
    /// mailbox whose on-disk sentinel folder disagreed with the
    /// cached folder (user `mv`-ed the maildir, or a LocalWins
    /// conflict resolved against the server's rename). Symmetric
    /// with `folder_rename_count` on the push side.
    pub fn remote_folder_rename_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, SyncAction::RenameRemoteMailbox { .. }))
            .count()
    }

    pub fn adopt_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, SyncAction::AdoptLocalMessage { .. }))
            .count()
    }

    /// Split the plan into (kept, dropped) according to `direction`.
    /// AdoptLocalMessage is always kept — it is byte-identical and pure
    /// DB, so adopting in pull-only or push-only mode is still strictly
    /// progress. Pull-only keeps pull-side actions; push-only keeps
    /// push-side; Both keeps everything.
    pub fn into_filtered(self, direction: SyncDirection) -> (SyncPlan, Vec<SyncAction>) {
        let SyncPlan {
            actions,
            new_email_state,
        } = self;

        let mut kept = Vec::with_capacity(actions.len());
        let mut dropped = Vec::new();

        for action in actions {
            if matches!(
                (direction, action.direction()),
                (SyncDirection::Both, _)
                    | (_, ActionDirection::Both)
                    | (SyncDirection::PullOnly, ActionDirection::Pull)
                    | (SyncDirection::PushOnly, ActionDirection::Push)
            ) {
                kept.push(action);
            } else {
                dropped.push(action);
            }
        }

        (
            SyncPlan {
                actions: kept,
                new_email_state,
            },
            dropped,
        )
    }
}

impl fmt::Display for SyncPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return writeln!(f, "Nothing to do.");
        }
        writeln!(f, "Sync plan:")?;
        writeln!(f, "  Downloads:             {}", self.download_count())?;
        writeln!(f, "  Uploads:               {}", self.upload_count())?;
        writeln!(f, "  Adoptions:             {}", self.adopt_count())?;
        writeln!(f, "  Flag updates:          {}", self.flag_update_count())?;
        writeln!(f, "  Deletes:               {}", self.delete_count())?;
        writeln!(f, "  Local folder creates:  {}", self.folder_create_count())?;
        writeln!(
            f,
            "  Remote folder creates: {}",
            self.remote_folder_create_count()
        )?;
        writeln!(f, "  Local folder renames:  {}", self.folder_rename_count())?;
        writeln!(
            f,
            "  Remote folder renames: {}",
            self.remote_folder_rename_count()
        )?;
        writeln!(f)?;

        for action in &self.actions {
            match action {
                SyncAction::DownloadMessage { id, binding, .. } => writeln!(
                    f,
                    "  [PULL]  Download {} -> {}/",
                    id, binding.maildir_folder
                )?,
                SyncAction::UpdateLocalFlags { id, new_flags, .. } => {
                    writeln!(f, "  [PULL]  Update flags on {}: '{}'", id, new_flags)?
                }
                SyncAction::DeleteLocal { id, .. } => writeln!(f, "  [PULL]  Delete local {}", id)?,
                SyncAction::MoveLocal {
                    id,
                    from_folder,
                    to_binding,
                } => writeln!(
                    f,
                    "  [PULL]  Move {} from {}/ to {}/",
                    id, from_folder, to_binding.maildir_folder
                )?,
                SyncAction::AdoptLocalMessage { id, binding, .. } => writeln!(
                    f,
                    "  [BOTH]  Adopt {}/{} as {}",
                    binding.maildir_folder,
                    id.maildir_id,
                    id.as_remote()
                )?,
                SyncAction::UploadMessage { id, binding, .. } => {
                    writeln!(f, "  [PUSH] Upload {} from {}/", id, binding.maildir_folder)?
                }
                SyncAction::UpdateRemoteKeywords { id, .. } => {
                    writeln!(f, "  [PUSH] Update keywords on {}", id)?
                }
                SyncAction::DestroyRemote { id } => writeln!(f, "  [PUSH] Destroy {}", id)?,
                SyncAction::MoveRemote {
                    id,
                    from_folder,
                    to_folder,
                    ..
                } => writeln!(
                    f,
                    "  [PUSH] Move {} from {}/ to {}/",
                    id, from_folder, to_folder
                )?,
                SyncAction::CreateLocalMailbox { binding, .. } => writeln!(
                    f,
                    "  [PULL] Create local mailbox {}/ (server mailbox {})",
                    binding.maildir_folder, binding.jmap_mailbox_id
                )?,
                SyncAction::RenameLocalMailbox {
                    from_folder,
                    binding,
                    ..
                } => writeln!(
                    f,
                    "  [PULL] Rename local mailbox {}/ -> {}/ (server mailbox {})",
                    from_folder, binding.maildir_folder, binding.jmap_mailbox_id
                )?,
                SyncAction::CreateRemoteMailbox {
                    name,
                    parent_jmap_mailbox_id,
                    ..
                } => match parent_jmap_mailbox_id {
                    Some(parent) => writeln!(
                        f,
                        "  [PUSH] Create remote mailbox {:?} under parent {}",
                        name, parent
                    )?,
                    None => writeln!(f, "  [PUSH] Create remote mailbox {:?} (top-level)", name)?,
                },
                SyncAction::RenameRemoteMailbox { binding, .. } => writeln!(
                    f,
                    "  [PUSH] Rename remote mailbox {} -> {:?} (disk: {}/)",
                    binding.jmap_mailbox_id, binding.remote_path, binding.maildir_folder
                )?,
                SyncAction::DeleteLocalFolder { binding } => writeln!(
                    f,
                    "  [PULL] Destroy local folder {}/ (id was {})",
                    binding.maildir_folder, binding.jmap_mailbox_id
                )?,
            }
        }
        Ok(())
    }
}
