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
#[derive(Debug, Default)]
pub struct MailboxBindings {
    by_id: HashMap<JmapMailboxId, Arc<MailboxFolderBinding>>,
    by_folder: HashMap<String, JmapMailboxId>,
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
}
