//! Per-cycle lookup table over the set of mailboxes the sync engine
//! is operating on. `resolve_mailboxes` builds one of these once per
//! cycle from the freshly-fetched `Mailbox/get` plus the configured
//! layout and filter, and passes it to every downstream consumer
//! (scan, reconcile, execute) that needs to ask "what's the folder
//! for this mailbox id?" or vice versa.
//!
//! O(1) lookup in both directions. The `by_folder` index stores
//! only the `JmapMailboxId`, pointing into `by_id` for the full
//! binding. The two indices stay coherent by construction -- there
//! is no second copy of the binding to fall out of sync.
//!
//! Collisions (two bindings sharing a `maildir_folder`) are not
//! rejected: last-writer-wins on both indices. The struct exposes
//! no way to mutate the maps outside of `insert`, so callers cannot
//! get the indices into a corrupt state, but they can silently lose
//! a binding by inserting two with the same folder. A server-rename
//! that collides with an existing local folder under the configured
//! layout (or via a rename rule) is one path that produces this
//! shape; detecting it sits outside the scope of this type today.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;

use crate::ids::{JmapEmailId, JmapMailboxId, MaildirId, MessageId};
use crate::jmap::types::MailboxFolderBinding;
use crate::state::queries::MailboxRecord;

/// `by_id` is the authoritative store; `by_folder` is a secondary
/// index that `MailboxBindingsBuilder::insert` maintains alongside
/// it. Both fields are private and the only mutator (on the builder)
/// is `insert`, so the secondary index cannot drift from the
/// authoritative one.
///
/// `MailboxBindings` itself is immutable -- once
/// `MailboxBindingsBuilder::build` hands one back, no method on the
/// type mutates state. Construction goes through
/// `MailboxBindings::builder()` (fresh) or
/// `MailboxBindings::into_builder()` (transform an existing one).
///
/// Bindings are stored as `Arc<MailboxFolderBinding>` so downstream
/// in-memory event types (`LocalChange`, `SyncAction`) that need to
/// name a mailbox can hold an `Arc` clone instead of a fresh
/// `(JmapMailboxId, String, String)` triple per emission. The Arc
/// also lets `by_id` and `by_folder` share the same allocation
/// rather than keeping parallel copies.
///
/// The `new_mailboxes` slot rides along as a byproduct of the
/// cache-vs-server diff `resolve_mailboxes` performs to build the
/// live set: any server-known mailbox absent from the pre-upsert
/// `mailbox_map` snapshot is the user's first cycle seeing that
/// mailbox, and reconcile turns each into a `CreateLocalMailbox`
/// action. The `renamed_mailboxes` slot is the analogous output
/// for the rename case: any cached `mailbox_map` row whose folder
/// disagrees with the freshly resolved one becomes a
/// `RenameLocalMailbox` action. The `local_orphans` slot
/// records bindings whose `mailbox_map` row was dropped this
/// cycle because the server no longer advertises the id -- the
/// local maildir is the orphan (parent gone server-side); its
/// `binding.jmap_mailbox_id` is a tombstone, useful only for
/// identifying DB rows that still point at the dead id. The
/// `remote_orphans` slot records the inverse: server-known
/// bindings whose local maildir + sentinel are both absent on
/// disk, where `binding.jmap_mailbox_id` is live and the
/// on-disk side is the one that has vanished.
///
/// `MailboxBindings` is purely a description of what
/// `resolve_mailboxes` decided. It writes nothing to
/// `mailbox_map` directly; instead it surfaces every cache
/// mutation through one of three slots, each drained at a
/// specific point in the engine's run:
///
/// - `mailbox_metadata_writes`: rows upserted *before* the
///   executor runs (`Unchanged`/`CacheStale` bindings whose
///   `maildir_folder` already matches the live binding; only
///   drifted metadata -- `role`, `sort_order`, `parent_id`,
///   `remote_path` -- needs a refresh). Drained by
///   `apply_unconditional_mailbox_writes`. Pre-executor
///   ordering matters: the executor's `destroy_remote_mailboxes`
///   phase also calls `delete_mailbox` for ids it destroys
///   server-side, so running this slot first means the
///   executor's destroy has the last write on any shared id.
/// - `cache_route_orphan_deletes`: ids whose `mailbox_map` row
///   should be dropped this cycle (cache-route orphans -- the
///   id was in the cache at cycle start but the server no
///   longer advertises it). Drained alongside
///   `mailbox_metadata_writes` by
///   `apply_unconditional_mailbox_writes`. Sentinel-route
///   orphans have no cache row to drop and so don't land here.
/// - `pending_mailbox_writes`: rows upserted *after* the
///   executor runs, gated on disk consistency
///   (`FirstCycle`/`ServerRename`/`ConflictServerWins`). The
///   pass at `apply_pending_mailbox_writes` rechecks
///   maildir+sentinel per row and skips on disk-not-ready,
///   honoring the durability invariant jma applies elsewhere
///   (maildir + `F_BARRIERFSYNC`, then state DB).
///
/// Both drain functions sit past the `--dry-run` early return
/// in `run`, so plan-only invocations leave `mailbox_map`
/// untouched regardless of slot.
///
/// `LocalRename`/`ConflictLocalWins` arms emit no cache write
/// at all -- the cache stays at the pre-rename triple until
/// `Mailbox/set { update }` lands and the next cycle re-reads
/// the server view.
///
/// Consumers that only care about the live set ignore all
/// other slots.
#[derive(Debug, Default)]
pub struct MailboxBindings {
    by_id: HashMap<JmapMailboxId, Arc<MailboxFolderBinding>>,
    by_folder: HashMap<String, JmapMailboxId>,
    new_mailboxes: Vec<NewMailboxRecord>,
    renamed_mailboxes: Vec<RenamedMailboxRecord>,
    local_orphans: Vec<LocalOrphanRecord>,
    remote_orphans: Vec<RemoteOrphanRecord>,
    pending_mailbox_writes: Vec<MailboxRecord>,
    mailbox_metadata_writes: Vec<MailboxRecord>,
    cache_route_orphan_deletes: Vec<JmapMailboxId>,
    /// Per-cycle sentinel walk: `jmap_mailbox_id -> relative
    /// folder path` for every `.jma.mapping` `resolve_mailboxes`
    /// found on disk. Populated once during binding construction
    /// and read by scan (to disambiguate "maildir vanished" from
    /// "user renamed maildir", since a sentinel surviving at a
    /// different path means rename territory, not deletion).
    disk_sentinels: HashMap<JmapMailboxId, String>,
}

/// A mailbox whose on-disk state `resolve_mailboxes` found
/// inconsistent with the server-known binding (folder absent,
/// sentinel absent, or sentinel content stale). Reconcile emits
/// a `SyncAction::CreateLocalMailbox` for each, so the
/// executor's pre-everything-else phase creates the maildir +
/// sentinel before any downstream action assumes the folder
/// exists. `parent_jmap_mailbox_id` rides along because the
/// sentinel embeds it so the parent chain can be reconstructed
/// from disk after a state-DB nuke.
#[derive(Debug, Clone)]
pub struct NewMailboxRecord {
    pub binding: MailboxFolderBinding,
    pub parent_jmap_mailbox_id: Option<JmapMailboxId>,
    /// `Some(dead_id)` when the binding represents a resurrect:
    /// the executor's `CreateLocalMailbox` phase must drop stale
    /// `message_map` rows pointing at `dead_id` before any
    /// downstream `DownloadMessage` writes fresh rows for the
    /// same mailbox id. `None` for ordinary first-cycle creates
    /// (no cached rows exist for the id).
    pub replaces_orphan_id: Option<JmapMailboxId>,
}

/// Which side initiated the rename, and therefore which action
/// variant reconcile emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameDirection {
    /// Server renamed; emit `RenameLocalMailbox` to bring disk
    /// into agreement.
    Pull,
    /// User `mv`-ed the maildir locally (or a `LocalWins`
    /// conflict resolved that way); emit `RenameRemoteMailbox`
    /// to push the new name and parent to the server.
    Push,
}

/// A mailbox whose folder name changed since the cache was last
/// updated. `direction` distinguishes the two cases: `Pull` for
/// server renames (cache disagrees with the freshly resolved
/// server path), `Push` for local renames (sentinel found at a
/// disk path the cache and server both disagree with).
/// `binding.maildir_folder` is the post-rename target either way.
///
/// `from_folder` and `from_db_folder` model the asymmetric
/// source paths the executor needs:
/// - `from_folder` is the filesystem source (where `fs::rename`
///   reads from). For `Pull/ServerRename` and `Pull/CacheStale`
///   it equals the cached folder. For `Pull/ConflictServerWins`
///   it's the disk path the user `mv`-ed to (which the executor
///   must `fs::rename` back to the server's resolved path).
///   Unused on `Push` (the user's `mv` already moved the
///   maildir; the executor doesn't `fs::rename` for a push).
/// - `from_db_folder` is the DB-rewrite source (where
///   `local_state.maildir_folder` matches the rows that need
///   their column rewritten). Equal to the cached folder in
///   every case -- jma never updated `local_state` between the
///   user's local `mv` and this cycle.
///
/// The two are equal in the common server-rename case; they
/// diverge in `ConflictServerWins` and for `Push` (where
/// `from_folder` is moot).
#[derive(Debug, Clone)]
pub struct RenamedMailboxRecord {
    pub direction: RenameDirection,
    pub from_folder: String,
    pub from_db_folder: String,
    pub binding: MailboxFolderBinding,
    pub parent_jmap_mailbox_id: Option<JmapMailboxId>,
}

/// A local maildir whose remote counterpart has been deleted
/// -- the fresh `Mailbox/get` did not return its id, so the
/// `mailbox_map` row was dropped this cycle. The
/// `binding.jmap_mailbox_id` is historical: the server no
/// longer serves it, but local artifacts (the maildir on
/// disk, `message_map` rows still pointing at the id) survive
/// until a destructive or resurrect path acts on them.
/// `binding` is the last-known
/// `(jmap_mailbox_id, server_name, maildir_folder, remote_path)`
/// tuple from the dropped cache row (or, under post-DB-nuke
/// recovery, from the `.jma.mapping` sentinel that survived
/// the nuke). `parent_jmap_mailbox_id` records the cached
/// parent so the orphan's position in the hierarchy survives
/// the cache-row drop.
#[derive(Debug, Clone)]
pub struct LocalOrphanRecord {
    pub binding: Arc<MailboxFolderBinding>,
    pub parent_jmap_mailbox_id: Option<JmapMailboxId>,
    /// Messages found in the orphan's maildir at detection
    /// time. Each entry is one file in `cur/`/`new/`;
    /// `jmap_email_id` is `Some` when the file is backed by
    /// a `message_map` row whose `jmap_mailbox_id` matched
    /// the dead id (a previously-synced message), and `None`
    /// when the file has no corresponding row. Files whose
    /// `Message-ID` header is unparseable are dropped at
    /// capture time (jma's idempotency anchor requires one;
    /// without it the file is unactionable). Order is capture
    /// order and is not load-bearing.
    pub messages: Vec<OrphanMessage>,
}

/// One message found in a `LocalOrphanRecord`'s maildir at
/// detection time. `jmap_email_id` discriminates the two
/// cases: `Some` when the file was backed by a `message_map`
/// row pointing at the orphan's now-dead `jmap_mailbox_id`
/// (carried so the row is identifiable without a second DB
/// pass), `None` when no row exists for the file.
#[derive(Debug, Clone)]
pub struct OrphanMessage {
    pub jmap_email_id: Option<JmapEmailId>,
    pub maildir_id: MaildirId,
    pub message_id: MessageId,
    /// Absolute path to the file in `cur/` or `new/` at
    /// capture time. Not refreshed if the file moves between
    /// subdirs mid-cycle.
    pub path: PathBuf,
    /// Maildir filename flag suffix (everything after `:2,`)
    /// at capture time, e.g. `"FS"`. Captured so consumers
    /// don't have to re-parse the filename.
    pub flags: String,
    /// File size in bytes at capture time, from
    /// `std::fs::metadata`. Lets consumers gate uploads
    /// against `max_upload_size` without a second `stat`.
    pub size_bytes: u64,
}

/// A server-known mailbox whose on-disk side has vanished --
/// `try_open_maildir(cached_path)` returns None AND no
/// `.jma.mapping` sentinel for this id exists anywhere on
/// disk. The detector observes absence; the cause (user
/// delete, external script, backup restore that excluded the
/// maildir, etc.) is the consumer's concern. The
/// `binding.jmap_mailbox_id` is live: the server still serves
/// it; only the on-disk side has vanished. `binding` carries
/// the cached
/// `(jmap_mailbox_id, server_name, maildir_folder, remote_path)`
/// tuple as it stood at cycle start. `parent_jmap_mailbox_id`
/// is the server-side parent at cycle start.
#[derive(Debug, Clone)]
pub struct RemoteOrphanRecord {
    pub binding: Arc<MailboxFolderBinding>,
    pub parent_jmap_mailbox_id: Option<JmapMailboxId>,
    /// Total messages the server holds for the orphan mailbox at
    /// cycle start. The engine populates this after detection by
    /// iterating `remote_orphans_mut()` and calling
    /// `Email/query` with `limit: 0`; zero before that pass
    /// runs. The `limit: 0` form returns just the total, so the
    /// count is cheap regardless of mailbox size.
    pub server_email_count: u64,
}

impl MailboxBindings {
    /// Start a fresh, empty builder. Callers populate via
    /// `MailboxBindingsBuilder` mutators, then finish with `.build()`.
    pub(crate) fn builder() -> MailboxBindingsBuilder {
        MailboxBindingsBuilder::default()
    }

    /// Hand a built `MailboxBindings` back to a builder so a later
    /// phase can push additional records (e.g. remote orphans from
    /// `LocalFolderDeleted` events) before re-finalising via
    /// `.build()`. The intermediate phases of the engine consume
    /// the immutable bindings; this escape hatch is for the few
    /// callers that need to append.
    pub(crate) fn into_builder(self) -> MailboxBindingsBuilder {
        MailboxBindingsBuilder(self)
    }

    pub fn by_id(&self, id: &JmapMailboxId) -> Option<&Arc<MailboxFolderBinding>> {
        self.by_id.get(id)
    }

    pub fn by_folder(&self, folder: &str) -> Option<&Arc<MailboxFolderBinding>> {
        let id = self.by_folder.get(folder)?;
        self.by_id.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<MailboxFolderBinding>> {
        self.by_id.values()
    }

    pub fn ids(&self) -> impl Iterator<Item = &JmapMailboxId> {
        self.by_id.keys()
    }

    pub fn folders(&self) -> impl Iterator<Item = &str> {
        self.by_folder.keys().map(String::as_str)
    }

    /// The set of bindings scan should walk this cycle. With
    /// `include_orphans = false` (the steady state under
    /// non-destructive policies) returns the live set only --
    /// orphan folders are off-limits since their JMAP id is
    /// dead and any emitted action targeting that id would be
    /// rejected by the server. With `include_orphans = true`
    /// (destructive policies that may delete the local
    /// folder) extends with orphan bindings so cycle-local
    /// activity inside an orphan (a `mv` into it, a draft
    /// drop, flag edits) surfaces as `LocalChange` events;
    /// reconcile's destructive-handling matrix recognizes
    /// those events via `local_orphans()` and dispatches them
    /// through the conflict-strategy path. Bindings come back
    /// by `Arc` refcount-bump; no allocation per scan.
    pub fn scan_set(&self, include_orphans: bool) -> Vec<Arc<MailboxFolderBinding>> {
        let mut out: Vec<Arc<MailboxFolderBinding>> = self.by_id.values().cloned().collect();
        if include_orphans {
            out.extend(self.local_orphans.iter().map(|o| Arc::clone(&o.binding)));
        }
        out
    }

    /// Server-known bindings that had no cached `mailbox_map`
    /// row at cycle start. Reconcile emits one
    /// `CreateLocalMailbox` per entry. Empty in steady state.
    pub fn new_mailboxes(&self) -> &[NewMailboxRecord] {
        &self.new_mailboxes
    }

    /// Server-known bindings whose cached `mailbox_map.maildir_folder`
    /// disagreed with the freshly resolved path at cycle start.
    /// Reconcile emits one `RenameLocalMailbox` per entry. Empty in
    /// steady state.
    pub fn renamed_mailboxes(&self) -> &[RenamedMailboxRecord] {
        &self.renamed_mailboxes
    }

    /// Bindings whose `mailbox_map` row was dropped this cycle
    /// because the server no longer advertises the id. Empty
    /// in steady state. Order matches detection order in
    /// `resolve_mailboxes`.
    pub fn local_orphans(&self) -> &[LocalOrphanRecord] {
        &self.local_orphans
    }

    /// Server-known bindings whose local maildir was deleted
    /// by the user this cycle. Empty in steady state. Order
    /// matches scan emission order of
    /// `LocalChange::LocalFolderDeleted`.
    pub fn remote_orphans(&self) -> &[RemoteOrphanRecord] {
        &self.remote_orphans
    }

    /// Does a `.jma.mapping` sentinel for this id exist on disk
    /// at any path? Scan uses this to filter `LocalFolderDeleted`
    /// emissions -- if the sentinel survives elsewhere, the
    /// binding's missing maildir is a rename candidate, not a
    /// deletion. Operates on the walk recorded via
    /// `MailboxBindingsBuilder::set_disk_sentinels`; returns
    /// `false` if the walk hasn't run (early-cycle callers see
    /// "no sentinel," which keeps them from misfiring).
    pub fn sentinel_survives_for(&self, id: &JmapMailboxId) -> bool {
        self.disk_sentinels.contains_key(id)
    }

    /// Whether this id is a first-cycle (never-synced) mailbox.
    /// Used by scan to filter `LocalFolderDeleted` emissions:
    /// a first-cycle binding's maildir is absent by design --
    /// `CreateLocalMailbox` will provision it -- so the absence
    /// is not a user deletion.
    pub fn is_new_mailbox(&self, id: &JmapMailboxId) -> bool {
        self.new_mailboxes.iter().any(|nm| {
            matches!(&nm.binding.jmap_mailbox_id,
                crate::jmap::types::MaybeReference::Value(v) if v == id)
        })
    }

    /// `mailbox_map` rows staged this cycle for upsert after
    /// the executor confirms disk state. Empty in steady state
    /// (no new mailboxes or renames). Order matches push order
    /// in `resolve_mailboxes`.
    pub fn pending_mailbox_writes(&self) -> &[MailboxRecord] {
        &self.pending_mailbox_writes
    }

    /// `mailbox_map` rows the engine should upsert without
    /// re-checking disk state -- the `Unchanged`/`CacheStale`
    /// bucket. Applied by `apply_unconditional_mailbox_writes`
    /// before the executor runs, so the executor's
    /// `destroy_remote_mailboxes` phase (which itself calls
    /// `delete_mailbox` after a successful server destroy) has
    /// the last write on any shared id. Gated by the same
    /// dry-run short-circuit as `pending_mailbox_writes`.
    pub fn mailbox_metadata_writes(&self) -> &[MailboxRecord] {
        &self.mailbox_metadata_writes
    }

    /// Ids whose `mailbox_map` row should be dropped this cycle
    /// (cache-route orphans). Sentinel-route orphans have no
    /// cache row to drop and do not appear here. Applied by
    /// `apply_unconditional_mailbox_writes` alongside
    /// `mailbox_metadata_writes`.
    pub fn cache_route_orphan_deletes(&self) -> &[JmapMailboxId] {
        &self.cache_route_orphan_deletes
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// Mutable accumulator for `MailboxBindings`. `resolve_mailboxes`
/// and its peers populate one via the `insert` / `push_*` / `set_*`
/// API, then finish with `.build()` to hand back the immutable
/// bindings. Wrapping `MailboxBindings` keeps the data layout in
/// one place: readers live on `MailboxBindings`, mutators live
/// here, and the builder transparently delegates reads through
/// `Deref` so construction-time code (e.g. the sentinel-route
/// dedup in `resolve_mailboxes` reading `local_orphans()`) works
/// without per-method forwarding.
#[derive(Debug, Default)]
pub(crate) struct MailboxBindingsBuilder(MailboxBindings);

impl MailboxBindingsBuilder {
    /// Finalise the builder. The returned `MailboxBindings` is
    /// immutable; further mutation requires `.into_builder()`.
    pub(crate) fn build(self) -> MailboxBindings {
        self.0
    }

    /// Insert a binding, updating both indices. Last writer wins on
    /// collisions (same `jmap_mailbox_id` or same `maildir_folder`
    /// as an existing entry overwrites it on the relevant index).
    /// The live binding set only ever holds resolved ids -- bindings
    /// in the cache snapshot come from `resolve_mailboxes` which
    /// builds them from server-known `MailboxObject`s, and emitted
    /// bindings (e.g. resurrect-path uploads with `Reference` ids)
    /// flow through `SyncAction` payloads rather than the live set.
    pub(crate) fn insert(&mut self, binding: MailboxFolderBinding) {
        let id = binding
            .jmap_mailbox_id
            .expect_resolved("MailboxBindingsBuilder::insert -- live set holds only resolved ids")
            .clone();
        self.0
            .by_folder
            .insert(binding.maildir_folder.clone(), id.clone());
        self.0.by_id.insert(id, Arc::new(binding));
    }

    /// Record a server-known binding that had no `mailbox_map` row
    /// at cycle start (first cycle the user has seen this mailbox).
    /// Reconcile turns each entry into a `CreateLocalMailbox`
    /// action; the executor then creates the on-disk maildir and
    /// stamps the sentinel before any downstream phase that needs
    /// to write into the folder.
    pub(crate) fn push_new_mailbox(
        &mut self,
        binding: MailboxFolderBinding,
        parent_jmap_mailbox_id: Option<JmapMailboxId>,
        replaces_orphan_id: Option<JmapMailboxId>,
    ) {
        self.0.new_mailboxes.push(NewMailboxRecord {
            binding,
            parent_jmap_mailbox_id,
            replaces_orphan_id,
        });
    }

    /// Record a rename. `direction = Pull` for server-renamed
    /// (cache disagrees with the freshly resolved path), `Push`
    /// for locally-renamed (sentinel disagrees with cache).
    /// `from_folder` is the fs source (where `fs::rename` reads
    /// from); `from_db_folder` is the DB-rewrite source (where
    /// `local_state.maildir_folder` matches). The two are equal
    /// in the common case and diverge only in
    /// `ConflictServerWins` (user `mv`-ed to a third name).
    /// `binding.maildir_folder` is the post-rename target.
    /// Reconcile dispatches on direction to emit the matching
    /// action variant. Push order is significant: under
    /// `LAYOUT=fs` a parent rename moves the descendant subtree
    /// in one `fs::rename` call (for the Pull side) or shifts
    /// the cached subtree paths (for the Push side; descendants
    /// re-resolve via their parent's `mailbox_map` upsert next
    /// cycle), and each descendant's own action recovers via
    /// the source-missing/target-present idempotent branch --
    /// so the caller must enqueue shallowest-first.
    pub(crate) fn push_renamed_mailbox(
        &mut self,
        direction: RenameDirection,
        from_folder: String,
        from_db_folder: String,
        binding: MailboxFolderBinding,
        parent_jmap_mailbox_id: Option<JmapMailboxId>,
    ) {
        self.0.renamed_mailboxes.push(RenamedMailboxRecord {
            direction,
            from_folder,
            from_db_folder,
            binding,
            parent_jmap_mailbox_id,
        });
    }

    /// Record a local orphan: the cached `mailbox_map` row was
    /// dropped this cycle because the fresh `Mailbox/get` no
    /// longer carries the id, or the post-nuke sentinel walk
    /// found a sentinel whose id is not in the live set. The
    /// local maildir is the orphan (its remote counterpart is
    /// gone).
    pub(crate) fn push_local_orphan(
        &mut self,
        binding: MailboxFolderBinding,
        parent_jmap_mailbox_id: Option<JmapMailboxId>,
    ) {
        self.0.local_orphans.push(LocalOrphanRecord {
            binding: Arc::new(binding),
            parent_jmap_mailbox_id,
            messages: Vec::new(),
        });
    }

    /// Record a remote orphan: a server-known binding whose
    /// local maildir is gone and whose `.jma.mapping` sentinel
    /// is absent from disk. Called by the engine's post-scan
    /// conversion loop after consuming a
    /// `LocalChange::LocalFolderDeleted` event. The caller is
    /// responsible for the absence check; scan filters via
    /// `is_new_mailbox` and `sentinel_survives_for` before
    /// emitting the event. `server_email_count` comes from the
    /// caller's `Email/query` round-trip with `limit: 0`.
    pub(crate) fn push_remote_orphan(
        &mut self,
        binding: Arc<MailboxFolderBinding>,
        parent_jmap_mailbox_id: Option<JmapMailboxId>,
        server_email_count: u64,
    ) {
        self.0.remote_orphans.push(RemoteOrphanRecord {
            binding,
            parent_jmap_mailbox_id,
            server_email_count,
        });
    }

    /// Stash the per-cycle sentinel walk so downstream phases
    /// (scan, primarily) can disambiguate "binding's maildir is
    /// missing because the user renamed it" (sentinel survives
    /// at a different path) from "binding's maildir is missing
    /// because the user deleted it" (sentinel gone everywhere).
    /// Called once by `resolve_mailboxes` after its own walk.
    pub(crate) fn set_disk_sentinels(&mut self, walk: HashMap<JmapMailboxId, String>) {
        self.0.disk_sentinels = walk;
    }

    /// Stage a `mailbox_map` row to be upserted after the
    /// executor confirms disk state. Pushed from
    /// `resolve_mailboxes` for decisions where the cache write
    /// follows a disk op (folder create or rename); a separate
    /// post-executor pass applies each row iff the maildir +
    /// sentinel at `record.maildir_folder` are now consistent
    /// with `record`.
    pub(crate) fn push_pending_mailbox_write(&mut self, record: MailboxRecord) {
        self.0.pending_mailbox_writes.push(record);
    }

    /// Bindings whose `mailbox_map` row was dropped this cycle
    /// because the server no longer advertises the id. Empty
    /// in steady state. Order matches detection order in
    /// `resolve_mailboxes`.
    pub(crate) fn local_orphans(&self) -> &[LocalOrphanRecord] {
        &self.0.local_orphans
    }

    /// Populate each `LocalOrphanRecord.messages` via the
    /// provided callback, which receives the orphan record
    /// (with its binding + cached parent) and returns the
    /// message vector to attach. Keeps direct mutation of the
    /// `local_orphans` Vec inside this type.
    pub(crate) fn populate_local_orphan_messages<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&LocalOrphanRecord) -> Result<Vec<OrphanMessage>>,
    {
        for orphan in &mut self.0.local_orphans {
            orphan.messages = f(orphan)?;
        }
        Ok(())
    }

    /// Stage a `mailbox_map` row to be upserted without
    /// re-checking disk state. Pushed for `Unchanged`/`CacheStale`
    /// decisions where the cache row already names the correct
    /// `maildir_folder` and only the metadata (role, sort_order,
    /// parent_id, remote_path) may have drifted; the finalization
    /// pass applies these alongside `pending_mailbox_writes` but
    /// without the disk-state recheck.
    pub(crate) fn push_mailbox_metadata_write(&mut self, record: MailboxRecord) {
        self.0.mailbox_metadata_writes.push(record);
    }

    /// Record a cache-route local orphan: a `mailbox_map` row
    /// dropped this cycle because the fresh `Mailbox/get` no
    /// longer carries the id. Pushes to `local_orphans` (so
    /// reconcile sees the drift) AND queues the `mailbox_map`
    /// row delete on `cache_route_orphan_deletes`. Sentinel-route
    /// orphans go through `push_local_orphan` -- they have no
    /// cache row to drop.
    pub(crate) fn push_cache_route_orphan(
        &mut self,
        binding: MailboxFolderBinding,
        parent_jmap_mailbox_id: Option<JmapMailboxId>,
    ) {
        let dead_id = binding
            .jmap_mailbox_id
            .expect_resolved("cache-route orphan binding id resolved")
            .clone();
        self.0.cache_route_orphan_deletes.push(dead_id);
        self.0.local_orphans.push(LocalOrphanRecord {
            binding: Arc::new(binding),
            parent_jmap_mailbox_id,
            messages: Vec::new(),
        });
    }
}

impl std::ops::Deref for MailboxBindingsBuilder {
    type Target = MailboxBindings;

    fn deref(&self) -> &MailboxBindings {
        &self.0
    }
}
