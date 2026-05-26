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
use std::sync::Arc;

use crate::ids::JmapMailboxId;
use crate::jmap::types::MailboxFolderBinding;

/// `by_id` is the authoritative store; `by_folder` is a secondary
/// index that `MailboxBindingsBuilder::insert` maintains alongside
/// it. Both fields are private and the only mutator (on the builder)
/// is `insert`, so the secondary index cannot drift from the
/// authoritative one.
///
/// `MailboxBindings` itself is immutable -- once
/// `MailboxBindingsBuilder::build` hands one back, no method on the
/// type mutates state. Construction goes through
/// `MailboxBindings::builder()`.
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
/// action. Consumers that only care about the live set ignore
/// the slot.
#[derive(Debug, Default)]
pub struct MailboxBindings {
    by_id: HashMap<JmapMailboxId, Arc<MailboxFolderBinding>>,
    by_folder: HashMap<String, JmapMailboxId>,
    new_mailboxes: Vec<NewMailboxRecord>,
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

impl MailboxBindings {
    /// Start a fresh, empty builder. Callers populate via
    /// `MailboxBindingsBuilder` mutators, then finish with `.build()`.
    pub(crate) fn builder() -> MailboxBindingsBuilder {
        MailboxBindingsBuilder::default()
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

    /// Server-known bindings that had no cached `mailbox_map`
    /// row at cycle start. Reconcile emits one
    /// `CreateLocalMailbox` per entry. Empty in steady state.
    pub fn new_mailboxes(&self) -> &[NewMailboxRecord] {
        &self.new_mailboxes
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
    /// immutable.
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
            .expect_resolved("MailboxBindings::insert -- live set holds only resolved ids")
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
    pub fn push_new_mailbox(
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
}
