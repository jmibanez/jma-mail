//! Neutral domain types shared across layers.
//!
//! These types describe mailboxes and mailbox/folder bindings in
//! terms the whole codebase agrees on -- `jmap` produces them at the
//! wire boundary, `maildir_ops` consumes them to lay folders out on
//! disk, and `sync` threads them through reconcile and execute. None
//! of their shape is JMAP-wire-specific (contrast `EmailObject`'s
//! `mailbox_ids: HashMap<JmapMailboxId, bool>`, where the `bool`
//! encodes JMAP's set-membership convention and so stays in
//! `jmap::types`). Keeping them here lets the peer layers depend on a
//! neutral leaf instead of reaching into each other.
//!
//! The module depends only on `crate::ids` and `std`; it must never
//! import `jmap`, `maildir_ops`, `state`, or `sync`, or it would
//! reintroduce the cross-layer edge it exists to remove.

use std::collections::HashMap;
use std::sync::Arc;

use crate::ids::JmapMailboxId;

/// Represents a JMAP Mailbox object.
#[derive(Debug, Clone)]
pub struct MailboxObject {
    pub id: JmapMailboxId,
    pub name: String,
    pub parent_id: Option<JmapMailboxId>,
    pub role: Option<String>,
    pub sort_order: u32,
    pub total_emails: u64,
    pub unread_emails: u64,
}

/// The in-memory bundle that pairs a JMAP mailbox id with the
/// information every consumer of "what mailbox/folder is this"
/// actually needs: the id itself, the JMAP-side leaf name (matches
/// `MailboxObject.name` and the sentinel's `server_name`), and the
/// on-disk folder path under the configured layout.
///
/// Lighter than `MailboxObject` (no parent_id, role, sort_order, or
/// totals) and lighter than `MailboxRecord` -- carries only the
/// fields the sync pipeline needs to talk about a mailbox.
/// Computed by `resolve_mailboxes` from a `MailboxObject` plus the
/// active layout, then threaded through scan, reconcile, and
/// execute so the (mailbox_id, folder) pair never has to be
/// reconstructed from a tuple or rebuilt via lookup.
///
/// `jmap_mailbox_id` is a `MaybeReference<JmapMailboxId>` so a
/// binding can address a mailbox whose server-side id is not yet
/// known -- e.g. a mailbox an earlier action in the same plan
/// creates, whose id an executor-side resolution step would swap
/// in once that create returns. No code constructs the
/// `Reference` variant today, so every binding's id is a resolved
/// `Value`; readers call `expect_resolved("context")` to surface
/// a leaked `Reference` as a panic rather than a silent wrong DB
/// write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailboxFolderBinding {
    pub jmap_mailbox_id: MaybeReference<JmapMailboxId>,
    pub server_name: String,
    pub maildir_folder: String,
    /// Slash-joined server-name path walking up the parent chain
    /// ("Personal/Archive" for a child of "Personal"; "Inbox" for
    /// a top-level). Distinct from `maildir_folder`, which
    /// follows the configured `folder_layout` +
    /// `hierarchy_separator`. Computed by `resolve_mailboxes` in
    /// a single topological pass and cached on
    /// `mailbox_map.remote_path` for between-cycle reads. JMAP
    /// scopes its mailbox uniqueness to `(parent_id, name)` per
    /// RFC 8621 section 2, so the bare `server_name` is ambiguous
    /// across the hierarchy while this path is not.
    pub remote_path: String,
}

/// Either an already-resolved value or a symbolic handle that a
/// later resolution step will swap for a value.
///
/// Used inside `MailboxFolderBinding.jmap_mailbox_id` to express
/// "the target of this action is a mailbox an earlier action in
/// the same plan will create; the executor will swap the resolved
/// id in once the create succeeds." The `Reference` handle's
/// string is the producer action's chosen name; a per-cycle
/// resolution table populated as create actions complete recovers
/// the `Value`.
///
/// `Reference` values are ephemeral -- they only live between
/// the moment reconcile emits an action and the moment the
/// resolution step swaps in the real value. Code outside that
/// window (scan, DB writes, log lines outside the resolution
/// moment) calls `expect_resolved("call-site context")` to
/// assert the value side of the enum; a leaked `Reference`
/// indicates a bug at the resolution step. No code constructs a
/// `Reference` today, so the resolution path is unused; the
/// variant exists so the type boundary is in place ahead of the
/// first producer.
///
/// Eq is structural and `Reference("Archive")` would compare
/// equal to another `Reference("Archive")` from a different
/// cycle even though they resolve to different ids. Today no
/// code compares bindings cross-cycle, so the quirk is latent;
/// callers that need cross-cycle identity comparison should
/// only do so over the resolved `Value` variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaybeReference<T> {
    /// A symbolic handle the executor's reference table resolves
    /// at run time. The string is the producer's chosen name.
    Reference(String),
    /// An already-resolved value -- no lookup needed.
    Value(T),
}

impl<T> MaybeReference<T> {
    /// Borrow the resolved value or panic with `context` in the
    /// message. Use at every read site that does not participate
    /// in the resolution step (which is most of them): scan, DB
    /// writes, log lines outside the per-action resolution
    /// moment. A panic here means a `Reference` leaked past the
    /// resolution boundary -- a bug at the reference's producer
    /// or consumer, not user input.
    pub fn expect_resolved(&self, context: &str) -> &T {
        match self {
            MaybeReference::Value(v) => v,
            MaybeReference::Reference(name) => panic!(
                "unresolved MaybeReference::Reference({:?}) at {}; \
                 a creation reference leaked past the executor's resolution step",
                name, context
            ),
        }
    }

    /// `true` if this is a resolved `Value`. Useful for guards
    /// where the alternative is to call `expect_resolved` and
    /// the panic would be unhelpful.
    pub fn is_resolved(&self) -> bool {
        matches!(self, MaybeReference::Value(_))
    }
}

impl<T: Clone> MaybeReference<T> {
    /// Resolve a `Reference` via the given table; return the
    /// resolved value (cloned) or the original `Value`. Returns
    /// `None` if `Reference` is unresolved in the table -- the
    /// caller decides whether that's a soft skip (warn) or a
    /// hard error (the producer never ran or never succeeded).
    pub fn resolve_with(&self, table: &std::collections::HashMap<String, T>) -> Option<T> {
        match self {
            MaybeReference::Reference(name) => table.get(name).cloned(),
            MaybeReference::Value(v) => Some(v.clone()),
        }
    }
}

impl<T: std::fmt::Display> std::fmt::Display for MaybeReference<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MaybeReference::Reference(name) => write!(f, "#{}", name),
            MaybeReference::Value(v) => write!(f, "{}", v),
        }
    }
}

/// O(1) two-way lookup over a set of `MailboxFolderBinding`s: id ->
/// binding and folder -> binding. `by_id` is the authoritative
/// store; `by_folder` holds only the `JmapMailboxId`, pointing into
/// `by_id` for the full binding, so the two cannot drift -- there is
/// no second copy of the binding to fall out of sync. `insert` is the
/// sole mutator, so callers cannot get the maps into a corrupt state.
///
/// Bindings are stored as `Arc<MailboxFolderBinding>` so downstream
/// in-memory event types (`LocalChange`, `SyncAction`) that name a
/// mailbox can hold an `Arc` clone instead of a fresh
/// `(JmapMailboxId, String, String)` triple per emission, and so the
/// two maps share one allocation rather than keeping parallel copies.
///
/// Collisions (two bindings sharing a `maildir_folder`) are not
/// rejected: last-writer-wins on both indices. The index exposes no
/// way to mutate the maps outside `insert`, so callers cannot corrupt
/// it, but they can silently lose a binding by inserting two with the
/// same folder. Detecting that collision sits outside this type.
#[derive(Debug, Default)]
pub struct MailboxIndex {
    by_id: HashMap<JmapMailboxId, Arc<MailboxFolderBinding>>,
    by_folder: HashMap<String, JmapMailboxId>,
}

impl MailboxIndex {
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

    /// Insert a binding, updating both indices. Last writer wins on
    /// collisions (same `jmap_mailbox_id` or same `maildir_folder`
    /// as an existing entry overwrites it on the relevant index).
    /// The index only ever holds resolved ids -- bindings come from
    /// `resolve_mailboxes`, which builds them from server-known
    /// `MailboxObject`s; emitted bindings carrying a `Reference` id
    /// flow through `SyncAction` payloads, never the live set.
    pub fn insert(&mut self, binding: MailboxFolderBinding) {
        let id = binding
            .jmap_mailbox_id
            .expect_resolved("MailboxIndex::insert -- live set holds only resolved ids")
            .clone();
        self.by_folder
            .insert(binding.maildir_folder.clone(), id.clone());
        self.by_id.insert(id, Arc::new(binding));
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}
