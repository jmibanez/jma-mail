use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ids::{JmapAccountId, JmapBlobId, JmapEmailId, JmapMailboxId, JmapThreadId, MessageId};

/// Represents a JMAP Email object with the properties we care about.
#[derive(Debug, Clone)]
pub struct EmailObject {
    pub id: JmapEmailId,
    pub blob_id: JmapBlobId,
    pub thread_id: JmapThreadId,
    pub mailbox_ids: HashMap<JmapMailboxId, bool>,
    pub keywords: HashMap<String, bool>,
    pub message_id: Option<Vec<MessageId>>,
    pub subject: Option<String>,
    /// RFC 8621 §4.1.1 `size`: total octets of the RFC 5322
    /// message, as known to the server. Used by the remote-dedupe
    /// planner as a cheap pre-check before downloading blobs to
    /// confirm byte-equality; not consulted by the sync engine
    /// (which keys off the maildir file's on-disk size when it
    /// needs one).
    pub size: u64,
}

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
/// binding emitted by reconcile can address a mailbox whose
/// server-side id is not yet known: a later commit's emit path
/// will queue chained actions (e.g. an `UploadMessage` whose
/// target mailbox is being created by an earlier action in the
/// same plan), and an executor-side resolution step will swap
/// the resolved id in once the create returns. Today no producer
/// constructs a `Reference`, so every binding's id is a resolved
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
/// indicates a bug at the resolution step. Today no producer
/// constructs a `Reference`, so the resolution path is unused;
/// it exists so the type boundary is settled before later commits
/// introduce the first producer.
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

/// Result of a JMAP Email/changes call.
#[derive(Debug)]
pub struct ChangesResponse {
    pub old_state: String,
    pub new_state: String,
    pub created: Vec<JmapEmailId>,
    pub updated: Vec<JmapEmailId>,
    pub destroyed: Vec<JmapEmailId>,
    pub has_more_changes: bool,
}

/// JMAP session info extracted after connecting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub api_url: String,
    pub download_url: String,
    pub upload_url: String,
    pub event_source_url: String,
    pub account_id: JmapAccountId,
    pub username: String,
}
