use anyhow::Result;
use maildir::Maildir;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, error};

use crate::config::FolderLayout;
use crate::ids::{JmapMailboxId, MaildirId, MessageId};
use crate::jmap::types::MailboxFolderBinding;
use crate::maildir_ops::headers::parse_message_id_from_file;
use crate::maildir_ops::namespace::is_jma_private;
use crate::maildir_ops::sentinel::MailboxMapping;
use crate::sync::bindings::MailboxBindings;

/// One live `cur/` entry passed into `classify_changes`. Both
/// `scan_folder` (full enumeration via the maildir crate) and
/// `scan_paths` (partial, driven by fsevents) build these from their
/// respective inputs so the classifier sees a uniform shape.
struct CurEntry {
    maildir_id: MaildirId,
    flags: String,
    path: PathBuf,
}

/// One scan pass's output: the classified `LocalChange`s and a
/// MaildirId-keyed map of every on-disk filename's flag suffix the
/// scan visited.
///
/// `local_flags` is the filesystem-truth view of flags. Reconcile
/// reads it at adoption emit sites and `commit_adopt` writes the
/// adopted file's entry into `local_state.flags`, so the next scan's
/// `known_flags != entry.flags` comparison doesn't fire a phantom
/// FlagsChanged. Both scan shapes populate this map from the same
/// `entry.flags()` reads they already do for classification -- no
/// extra I/O. For `scan_folder` the map covers every cur/ + new/
/// file in the folder; for `scan_paths` it covers only the
/// maildir_ids that the event paths pointed at. Out-of-event-set
/// maildir_ids in steady state are covered by the state DB instead
/// (`MessageRecord.flags` post-adoption is the standard-six
/// projection of the server's keywords, which adoption's split also
/// keeps consistent).
#[derive(Debug)]
pub struct ScanResult {
    pub changes: Vec<LocalChange>,
    pub local_flags: HashMap<MaildirId, String>,
}

/// A change detected in the local maildir.
#[derive(Debug)]
pub enum LocalChange {
    /// A new message file appeared that we don't have in the DB.
    ///
    /// `message_id` is required: scan refuses to emit `NewMessage` for a
    /// file whose `Message-ID` header is missing or unparseable. Without
    /// it, the disposable-state-DB invariant breaks (a wipe + re-sync
    /// would re-upload the file as a fresh server email rather than
    /// adopting the existing one), so the file is dropped at the scan
    /// boundary with an `error!` and stays on disk untouched.
    NewMessage {
        maildir_id: MaildirId,
        /// The mailbox this file lives in, as a single typed unit.
        /// scan resolves the binding at the producer boundary
        /// (`classify_changes` has it in hand); downstream consumers
        /// read `binding.maildir_folder` for filesystem ops and
        /// `binding.jmap_mailbox_id.expect_resolved(...)` for DB
        /// writes without re-resolving via `bindings.by_folder(...)`.
        /// `Arc` so emission stays a refcount bump rather than a
        /// three-String clone.
        binding: Arc<MailboxFolderBinding>,
        flags: String,
        path: PathBuf,
        message_id: MessageId,
        /// On-disk byte size at scan time, captured by the same
        /// pass that opens the file for Message-ID parsing. Carried
        /// so reconcile can refuse oversized uploads without doing
        /// its own I/O. `0` for files whose size couldn't be
        /// stat'd; reconcile treats that as "let it through and let
        /// the upload path surface the error."
        size_bytes: u64,
    },
    /// A message file we had recorded is now missing.
    DeletedMessage {
        maildir_id: MaildirId,
        binding: Arc<MailboxFolderBinding>,
    },
    /// The flags on a message file changed.
    FlagsChanged {
        maildir_id: MaildirId,
        binding: Arc<MailboxFolderBinding>,
        old_flags: String,
        new_flags: String,
    },
    /// A maildir-shaped directory appeared under the configured
    /// maildir root that no `MailboxBindings` entry covers. No
    /// `Arc<MailboxFolderBinding>` because the server-side JMAP id
    /// isn't known yet: a later cycle will resolve it (either
    /// because the cache lost a binding that the on-disk sentinel
    /// still pins, or because no prior binding exists and a future
    /// emit path will push the folder to the server). `sentinel`
    /// is the result of reading `path/.jma.mapping`: `Some` means
    /// a past sync wrote a binding here (the consumer can recover
    /// the bound JMAP id from the sentinel); `None` means the
    /// folder is unbound on both sides.
    ///
    /// No producer or consumer today: the variant lives here so
    /// the shape is settled before scan grows the folder-discovery
    /// pass and reconcile grows the arm that turns these into
    /// server-side mailbox actions.
    LocalFolderCreated {
        path: PathBuf,
        sentinel: Option<MailboxMapping>,
    },
    /// A bound folder the cache expects on disk is gone. Emitted
    /// by the Full-scope scan in `engine::run` when a binding's
    /// `try_open_maildir` returns None and neither the
    /// first-cycle nor sentinel-survives-elsewhere guard
    /// applies. The engine reads this event between scan and
    /// reconcile and translates it into a
    /// `RemoteOrphanRecord` on `MailboxBindings`, populating
    /// each orphan's `server_email_count` via `Email/query` for
    /// downstream telemetry.
    LocalFolderDeleted { binding: Arc<MailboxFolderBinding> },
    /// A bound folder's on-disk location moved between cycles
    /// (the same `.jma.mapping` sentinel now sits at a different
    /// path). `from_binding` is the binding as the cache knows it
    /// (old `maildir_folder` matches the original path); `to_path`
    /// is where the sentinel was found this cycle.
    /// `parent_jmap_mailbox_id` is the server-side id of the new
    /// parent folder, recovered from the parent dir's sentinel or
    /// cache lookup at emit time, so the consumer pushing the
    /// rename to the server doesn't have to re-walk the parent
    /// chain. `None` means the new parent is top-level.
    ///
    /// No producer or consumer today: the variant lives here so
    /// the shape is settled before the rename phase lands.
    LocalFolderRenamed {
        from_binding: Arc<MailboxFolderBinding>,
        to_path: PathBuf,
        parent_jmap_mailbox_id: Option<JmapMailboxId>,
    },
}

/// Classify a folder's worth of inputs into LocalChange events.
/// Single source of classification truth: both the full-folder walk
/// (`scan_folder`) and the path-driven walk (`scan_paths`) feed
/// pre-collected inputs through this function so they emit identical
/// shapes for identical states.
///
/// - `cur_entries`: live `cur/` entries the caller observed. Each
///   contributes a NewMessage / FlagsChanged / cross-folder
///   NewMessage as appropriate against `known_state`. Files whose
///   Message-ID is missing are skipped (logged at error!) so they
///   stay on disk untouched.
/// - `explicit_deletes`: maildir ids the caller is sure have
///   disappeared from this folder. Each becomes a DeletedMessage.
///   The caller is responsible for ensuring the DB anchors each id
///   to `folder_name` -- we trust the input here so a single function
///   serves both exhaustive (folder-walk) and event-driven
///   (fsevents) deletion sources without an extra mode flag.
///
/// Live new/ entries aren't passed in: jma (as the MDA) is the only
/// writer to new/, MUAs only ever promote new/ -> cur/, and anything
/// delivered to new/ is tracked in the DB at write time. Any later
/// MUA promotion shows up through the cur/ scan as either a
/// NewMessage (post-DB-wipe rescue) or a FlagsChanged. The caller
/// (scan_folder / scan_paths) still observes new/ entries so it can
/// build its own observed-on-disk set before deciding which DB-
/// anchored ids belong in `explicit_deletes`, but classify_changes
/// itself has no use for them.
fn classify_changes(
    binding: &Arc<MailboxFolderBinding>,
    cur_entries: &[CurEntry],
    explicit_deletes: &[MaildirId],
    known_state: &HashMap<MaildirId, (JmapMailboxId, String)>,
) -> Result<Vec<LocalChange>> {
    let mut changes = Vec::new();

    for entry in cur_entries {
        match known_state.get(&entry.maildir_id) {
            // Maildir-id-preserving cross-folder move: same unique part
            // of the filename, different mailbox than the DB recorded.
            // Comparing on `jmap_mailbox_id` rather than the folder
            // string keeps the check stable when a folder name itself
            // changes underneath us (e.g. a server-side mailbox rename
            // applied between cycles), where the same JMAP identity
            // resolves to a new on-disk path. Treat the destination
            // side as a NewMessage so reconcile's move pre-pass can
            // pair it (by Message-ID) with the DeletedMessage from
            // the source-mailbox walk. Without this, the mismatch is
            // silently swallowed and the move degrades into a destroy
            // + re-upload (or, after partial state drift, a backwards
            // MoveLocal that undoes the user's move).
            Some((known_mailbox_id, _))
                if known_mailbox_id
                    != binding.jmap_mailbox_id.expect_resolved(
                        "scan::classify_changes -- cross-mailbox rename compare",
                    ) =>
            {
                debug!(
                    "Cross-mailbox rename detected: {} now in {} (was bound to mailbox {})",
                    entry.maildir_id, binding.maildir_folder, known_mailbox_id
                );
                let Some(message_id) = require_message_id(&entry.maildir_id, &entry.path)? else {
                    continue;
                };
                let size_bytes = stat_size(&entry.path);
                changes.push(LocalChange::NewMessage {
                    maildir_id: entry.maildir_id.clone(),
                    binding: Arc::clone(binding),
                    flags: entry.flags.clone(),
                    path: entry.path.clone(),
                    message_id,
                    size_bytes,
                });
            }
            Some((_, known_flags)) => {
                if known_flags != &entry.flags {
                    debug!(
                        "Flags changed for {}: '{}' -> '{}'",
                        entry.maildir_id, known_flags, entry.flags
                    );
                    changes.push(LocalChange::FlagsChanged {
                        maildir_id: entry.maildir_id.clone(),
                        binding: Arc::clone(binding),
                        old_flags: known_flags.clone(),
                        new_flags: entry.flags.clone(),
                    });
                }
            }
            None => {
                debug!("New message in cur/: {}", entry.maildir_id);
                let Some(message_id) = require_message_id(&entry.maildir_id, &entry.path)? else {
                    continue;
                };
                let size_bytes = stat_size(&entry.path);
                changes.push(LocalChange::NewMessage {
                    maildir_id: entry.maildir_id.clone(),
                    binding: Arc::clone(binding),
                    flags: entry.flags.clone(),
                    path: entry.path.clone(),
                    message_id,
                    size_bytes,
                });
            }
        }
    }

    for id in explicit_deletes {
        debug!(
            "Deleted message: {} (was in {})",
            id, binding.maildir_folder
        );
        changes.push(LocalChange::DeletedMessage {
            maildir_id: id.clone(),
            binding: Arc::clone(binding),
        });
    }

    Ok(changes)
}

/// Scan a maildir folder and detect changes vs. the known state.
/// Walks the folder exhaustively via the maildir crate, then
/// delegates classification to `classify_changes`. Used by the
/// initial sync, post-disconnect catch-up, and the one-shot CLI
/// commands -- callers that need a full picture without relying on a
/// running fsevents stream.
///
/// `known_state` maps maildir_id -> (mailbox_id, flags). The
/// mailbox_id is the DB-recorded binding for the file: cross-mailbox
/// move detection compares it against `binding.jmap_mailbox_id`
/// (where the scan is currently walking) rather than against the
/// folder string, so a server-side mailbox rename that changes the
/// on-disk path while preserving the JMAP id doesn't read as a
/// user-driven cross-folder move.
pub fn scan_folder(
    maildir: &Maildir,
    binding: &Arc<MailboxFolderBinding>,
    known_state: &HashMap<MaildirId, (JmapMailboxId, String)>,
) -> Result<ScanResult> {
    let mut cur_entries = Vec::new();
    for entry in maildir.list_cur() {
        let entry = entry?;
        cur_entries.push(CurEntry {
            maildir_id: MaildirId::from(entry.id()),
            flags: entry.flags().to_string(),
            path: entry.path().to_path_buf(),
        });
    }

    let mut new_entries = Vec::new();
    for entry in maildir.list_new() {
        let entry = entry?;
        new_entries.push(MaildirId::from(entry.id()));
    }

    // Per-file filename-flag-suffix map for reconcile's adoption
    // emit sites. Built from the same `entry.flags()` reads
    // classify_changes inspects -- no extra I/O. new/ ids carry an
    // empty suffix because the maildir crate doesn't expose flags
    // for new/ entries (and the spec doesn't define them there);
    // they're included for completeness so a downstream lookup
    // never misses due to placement.
    let mut local_flags: HashMap<MaildirId, String> =
        HashMap::with_capacity(cur_entries.len() + new_entries.len());
    for entry in &cur_entries {
        local_flags.insert(entry.maildir_id.clone(), entry.flags.clone());
    }
    for id in &new_entries {
        local_flags.entry(id.clone()).or_default();
    }

    // Exhaustive deletion detection: any DB entry anchored to this
    // folder whose id we didn't observe on disk has gone away. Built
    // here so classify_changes stays input-driven and doesn't need to
    // know about the broader DB shape.
    let observed: HashSet<&MaildirId> = cur_entries
        .iter()
        .map(|e| &e.maildir_id)
        .chain(new_entries.iter())
        .collect();
    let explicit_deletes: Vec<MaildirId> = known_state
        .iter()
        .filter_map(|(id, (mailbox_id, _))| {
            if mailbox_id
                == binding
                    .jmap_mailbox_id
                    .expect_resolved("scan::scan_folder -- explicit deletes compare")
                && !observed.contains(id)
            {
                Some(id.clone())
            } else {
                None
            }
        })
        .collect();

    let changes = classify_changes(binding, &cur_entries, &explicit_deletes, known_state)?;
    Ok(ScanResult {
        changes,
        local_flags,
    })
}

/// Path-driven local scan: classify only the `(folder, maildir_id)`
/// groups touched by the supplied event paths instead of walking
/// every file in every synced folder. Used by the daemon when a
/// `LocalChange` trigger fires -- the watcher hands us the FS event
/// paths, and we trust the event stream to be the authoritative
/// change set for the in-flight cycle.
///
/// `known_states` maps each synced folder to its DB-recorded
/// `(maildir_id -> (mailbox_id, flags))` state. Paths whose folder
/// isn't present are dropped (untracked or stale-watcher noise).
/// `bindings` resolves event-path folder names to the JMAP mailbox
/// id of the folder currently being walked, which is what
/// `classify_changes` compares against the DB-recorded `mailbox_id`
/// to spot cross-mailbox moves.
///
/// Per `(folder, maildir_id)` group: the on-disk file (if any) is
/// found by stat'ing the event paths, with `cur/` preferred over
/// `new/`; the result is fed to `classify_changes` as either a
/// `cur_entries` row, a `new_entries` row, or an `explicit_deletes`
/// row. Deletions are emitted only when the DB anchors the id to
/// this same folder; if the DB has it elsewhere, the
/// destination-folder events handle it via the cross-folder branch
/// of `classify_changes` and reconcile pairs the two halves by
/// Message-ID.
///
/// Cross-folder deletion semantic depends on coalescing: both halves
/// of a move (source-delete and dest-create renames) need to land in
/// the same `scan_paths` call so the source folder's "live path
/// missing" emits `DeletedMessage(src)` and the destination folder's
/// "live path present, DB anchors elsewhere" emits `NewMessage(dst)`,
/// for reconcile to pair. The daemon's trigger loop holds open a
/// coalesce window for exactly this reason; if that window were ever
/// removed or set too tight to cover back-to-back debouncer batches,
/// a split move would degrade into destroy + reupload, losing the
/// JMAP id, thread, and keyword history.
///
/// Limitation: events the watcher never received (macOS
/// `MustScanSubDirs` drops, jma offline) leave drift this function
/// can't see. Bootstrap and post-disconnect cycles run a full
/// `scan_folder` walk to recover; here we trust the event stream.
pub fn scan_paths(
    maildir_root: &Path,
    event_paths: &[PathBuf],
    known_states: &HashMap<String, HashMap<MaildirId, (JmapMailboxId, String)>>,
    bindings: &MailboxBindings,
    layout: FolderLayout,
) -> Result<ScanResult> {
    // (subdir, flags, raw on-disk path) for one event hitting a
    // (folder, maildir_id) group. Multiple entries per group cover
    // both halves of a rename collapsed into the same key.
    type GroupEntry = (String, String, PathBuf);
    // Per-folder classification input: cur/ live entries, new/ ids,
    // and ids that disappeared (live path missing).
    type FolderBucket = (Vec<CurEntry>, Vec<MaildirId>, Vec<MaildirId>);

    // Per-event dispatch, in order:
    //   1. Parse the path; drop if it doesn't fit the
    //      `<root>/<folder>/(cur|new)/<file>` shape.
    //   2. Bound folder: group by (folder, maildir_id) so the source
    //      + destination sides of a single rename collapse into one
    //      classification (forwarded to classify_changes below).
    //   3. Unbound but layout-eligible folder: emit one
    //      `LocalFolderCreated` per folder per call (deduped via
    //      `discovered_folders`), then drop the message-level event
    //      since there's no binding to classify against.
    //   4. Unbound and layout-ineligible (e.g. an event at depth 2
    //      under Flat where mailboxes are depth 1): drop entirely.
    let mut groups: HashMap<(String, MaildirId), Vec<GroupEntry>> = HashMap::new();
    let mut discovered_folders: HashSet<String> = HashSet::new();
    let mut discovery_changes: Vec<LocalChange> = Vec::new();
    for raw in event_paths {
        let Some((folder, subdir, id, flags)) = parse_event_path(maildir_root, raw) else {
            continue;
        };
        if !known_states.contains_key(&folder) {
            if bindings.by_folder(&folder).is_none()
                && layout_eligible_folder(&folder, layout)
                && discovered_folders.insert(folder.clone())
            {
                let folder_path = maildir_root.join(&folder);
                let sentinel = match crate::maildir_ops::sentinel::read(&folder_path) {
                    Ok(opt) => opt,
                    Err(e) => {
                        debug!(
                            "scan_paths: sentinel read failed at {} ({e}); \
                             emitting LocalFolderCreated with sentinel=None",
                            folder_path.display()
                        );
                        None
                    }
                };
                discovery_changes.push(LocalChange::LocalFolderCreated {
                    path: folder_path,
                    sentinel,
                });
            }
            continue;
        }
        groups
            .entry((folder, id))
            .or_default()
            .push((subdir, flags, raw.clone()));
    }

    // Bucket into per-folder inputs for classify_changes.
    let mut by_folder: HashMap<String, FolderBucket> = HashMap::new();
    for ((folder, maildir_id), entries) in groups {
        // Live representative for this group. Prefer cur/ over new/:
        // cur/ is where flag-bearing files land after MUA promotion,
        // new/ is interim and doesn't drive a classification. Express
        // the priority as two ordered finds rather than break-and-
        // overwrite so the rule reads off the page.
        let live = entries
            .iter()
            .find(|e| e.0 == "cur" && e.2.exists())
            .or_else(|| entries.iter().find(|e| e.2.exists()));

        // Defensive: known_states presence was checked when building
        // `groups`, but a future refactor that rearranges that path
        // shouldn't silently panic here. Skip with a debug! if it
        // ever happens; the worst case is one missed classification.
        let Some(known_state) = known_states.get(&folder) else {
            debug!(
                "scan_paths: known_states missing folder {} mid-scan; skipping group",
                folder
            );
            continue;
        };

        let bucket = by_folder.entry(folder.clone()).or_default();
        match live {
            Some((subdir, flags, path)) if subdir == "cur" => {
                bucket.0.push(CurEntry {
                    maildir_id,
                    flags: flags.clone(),
                    path: path.clone(),
                });
            }
            Some(_) => {
                bucket.1.push(maildir_id);
            }
            None => {
                if let Some((known_mailbox_id, _)) = known_state.get(&maildir_id)
                    && let Some(b) = bindings.by_folder(&folder)
                    && known_mailbox_id
                        == b.jmap_mailbox_id
                            .expect_resolved("scan::scan_paths -- known state compare")
                {
                    bucket.2.push(maildir_id);
                }
            }
        }
    }

    let mut all_changes = discovery_changes;
    // Path-driven scans cover only the maildir_ids in the event set,
    // so `local_flags` here is intentionally partial -- callers fall
    // back to the state DB for everything not in this map.
    let mut local_flags: HashMap<MaildirId, String> = HashMap::new();
    for (folder, (cur, new, deletes)) in by_folder {
        // Same defensive guard as above; by_folder is built from
        // `groups`, which was filtered against `known_states`, so this
        // should always succeed.
        let Some(known_state) = known_states.get(&folder) else {
            debug!(
                "scan_paths: known_states missing folder {} at classify; skipping",
                folder
            );
            continue;
        };
        for entry in &cur {
            local_flags.insert(entry.maildir_id.clone(), entry.flags.clone());
        }
        for id in &new {
            local_flags.entry(id.clone()).or_default();
        }
        let Some(binding) = bindings.by_folder(&folder) else {
            debug!(
                "scan_paths: bindings missing folder {} at classify; skipping",
                folder
            );
            continue;
        };
        let changes = classify_changes(binding, &cur, &deletes, known_state)?;
        all_changes.extend(changes);
    }

    Ok(ScanResult {
        changes: all_changes,
        local_flags,
    })
}

/// Walk `maildir_root` for maildir-shaped directories not covered by
/// `bindings`. Each one becomes a `LocalChange::LocalFolderCreated`
/// carrying the on-disk path and the sentinel read result (so a
/// consumer can fork between "cache lost a binding the sentinel still
/// pins" and "genuinely new local folder").
///
/// "Maildir-shaped" means `<dir>/cur/` exists -- the same predicate
/// `store::try_open_maildir` uses elsewhere.
///
/// Walk strategy is `layout`-dependent so we don't pay for traversal
/// that the layout's naming convention rules out:
///
/// - `Flat`: candidates are first-level children of `maildir_root`
///   (every folder, including nested-on-server ones, lives at depth 1
///   under a joined name like `[Airmail].Sent`). No recursion.
/// - `MaildirPP`: candidates are first-level children whose name
///   starts with `.` (the spec's hidden-dot convention). Non-hidden
///   first-level dirs are not mailboxes under this layout.
/// - `Fs`: full recursion through every subdirectory that isn't a
///   `cur`/`new`/`tmp` leaf or a `.jma.*` namespace dir.
///
/// Returned paths are absolute. Consumers wanting the
/// `mailbox_map.maildir_folder` representation should `strip_prefix`
/// `maildir_root` and convert to a `String`. The set is sorted by
/// path so the change stream is deterministic across invocations.
///
/// Sentinel read failures are treated as `None` and logged at debug:
/// the variant's job is to surface the folder for downstream
/// classification, and a torn sentinel falls into the same bucket as
/// a missing one.
pub fn discover_unbound_folders(
    maildir_root: &Path,
    layout: FolderLayout,
    bindings: &MailboxBindings,
) -> Vec<LocalChange> {
    let mut found = Vec::new();
    match layout {
        FolderLayout::Flat => collect_flat(maildir_root, &mut found),
        FolderLayout::MaildirPP => collect_maildir_pp(maildir_root, &mut found),
        FolderLayout::Fs => walk_for_maildir_dirs_recursive(maildir_root, &mut found),
    }
    found.sort();
    found
        .into_iter()
        .filter_map(|abs_path| {
            let rel = abs_path.strip_prefix(maildir_root).ok()?;
            let rel_str = rel.to_string_lossy();
            if bindings.by_folder(rel_str.as_ref()).is_some() {
                return None;
            }
            let sentinel = match crate::maildir_ops::sentinel::read(&abs_path) {
                Ok(opt) => opt,
                Err(e) => {
                    debug!(
                        "discover_unbound_folders: sentinel read failed at {} ({e}); \
                         emitting with sentinel=None",
                        abs_path.display()
                    );
                    None
                }
            };
            Some(LocalChange::LocalFolderCreated {
                path: abs_path,
                sentinel,
            })
        })
        .collect()
}

/// First-level only: every non-`.jma.*` child of `dir` whose own
/// `cur/` subdirectory exists is a candidate. Used under `Flat`,
/// where the layout collapses hierarchy into separator-joined names
/// so every mailbox sits at depth 1.
fn collect_flat(dir: &Path, found: &mut Vec<PathBuf>) {
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
        let path = entry.path();
        if path.join("cur").is_dir() {
            found.push(path);
        }
    }
}

/// First-level hidden-dot children only: every child of `dir`
/// starting with `.` (and not `.jma.*`) whose own `cur/`
/// subdirectory exists is a candidate. Used under `MaildirPP`,
/// where the spec reserves the dot prefix for mailbox folders.
fn collect_maildir_pp(dir: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if !name_str.starts_with('.') || is_jma_private(name_str.as_ref()) {
            continue;
        }
        let path = entry.path();
        if path.join("cur").is_dir() {
            found.push(path);
        }
    }
}

/// `true` iff `rel` (a maildir-root-relative folder string of the
/// shape stored in `mailbox_map.maildir_folder`) is a valid mailbox
/// name under `layout`. Mirrors the shape constraint each layout's
/// folder-discovery walk applies, so `scan_paths`'s
/// `LocalFolderCreated` emission and `discover_unbound_folders`
/// agree on what counts as a folder.
///
/// - `Flat`: single segment (no `/`).
/// - `MaildirPP`: single segment starting with `.` (excluding the
///   `.jma.*` namespace reserved for jma's own state files).
/// - `Fs`: any depth; only `.jma.*` is excluded -- the layout's
///   own validator forbids dot-prefixed segments at write time,
///   but we don't re-check that here, so a stray `.hidden/cur/`
///   would surface (the consumer can drop it on its own).
fn layout_eligible_folder(rel: &str, layout: FolderLayout) -> bool {
    if rel.is_empty() {
        return false;
    }
    match layout {
        FolderLayout::Flat => !rel.contains('/') && !is_jma_private(rel),
        FolderLayout::MaildirPP => {
            !rel.contains('/') && rel.starts_with('.') && !is_jma_private(rel)
        }
        FolderLayout::Fs => !rel.split('/').any(is_jma_private),
    }
}

/// Recursive walk: collect absolute paths of every directory below
/// `dir` that holds a `cur/` subdirectory. Skips the `cur/`/`new/`/
/// `tmp/` triple and any `.jma.*` namespace dir so we don't recurse
/// into sentinel/lock/db neighbours. Used under `Fs`.
fn walk_for_maildir_dirs_recursive(dir: &Path, found: &mut Vec<PathBuf>) {
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
            found.push(path.clone());
        }
        walk_for_maildir_dirs_recursive(&path, found);
    }
}

/// Strip the maildir root prefix off an event path and split out the
/// folder name, the `cur`/`new` subdir, and the canonical maildir id
/// and flags carried by the filename. Returns `None` for paths that
/// don't fit the `<root>/<folder>/(cur|new)/<filename>` shape -- the
/// watcher already filters by `is_maildir_message_path`, so this only
/// rejects malformed input.
///
/// Maildir filenames are `<unique>:2,<flags>` in `cur/` and either
/// bare `<unique>` or `<unique>:2,<flags>` in `new/` (jma writes the
/// suffix-bearing shape to `new/` for non-Seen flag preservation;
/// see `store_message`). Both shapes split on the first `:2,`, with
/// no-`:2,` interpreted as empty flags.
fn parse_event_path(
    maildir_root: &Path,
    path: &Path,
) -> Option<(String, String, MaildirId, String)> {
    let rel = path.strip_prefix(maildir_root).ok()?;
    let rel_str = rel.to_str()?;
    let (folder, subdir, filename) = if let Some(idx) = rel_str.rfind("/cur/") {
        (&rel_str[..idx], "cur", &rel_str[idx + 5..])
    } else if let Some(idx) = rel_str.rfind("/new/") {
        (&rel_str[..idx], "new", &rel_str[idx + 5..])
    } else {
        return None;
    };
    if folder.is_empty() || filename.is_empty() {
        return None;
    }
    let (id, flags) = match filename.split_once(":2,") {
        Some((u, f)) => (u.to_string(), f.to_string()),
        None => (filename.to_string(), String::new()),
    };
    Some((
        folder.to_string(),
        subdir.to_string(),
        MaildirId::from(id),
        flags,
    ))
}

/// Capture the file's on-disk byte size, returning 0 if stat fails.
/// Plumbed onto `LocalChange::NewMessage` so reconcile can refuse
/// oversized uploads without doing its own I/O. Stat failures fall
/// through as 0 — we'd rather let the upload path surface a clear
/// per-file error than swallow the change at scan time on transient
/// metadata failures.
fn stat_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Parse the file's `Message-ID` header, returning `Ok(Some(_))` when
/// a usable id is present and `Ok(None)` (with an `error!` log) when
/// the header is missing — the user produced an RFC-violating file
/// that sync can't anchor idempotently, so the caller skips it.
///
/// I/O errors (file vanished mid-scan, EACCES, malformed UTF-8 in a
/// header) propagate as `Err` and abort the scan: those are systemic
/// problems the user can't fix on a per-file basis, and continuing
/// past them risks misclassifying transient failures as "user data
/// problem."
fn require_message_id(maildir_id: &MaildirId, path: &Path) -> Result<Option<MessageId>> {
    match parse_message_id_from_file(path)? {
        Some(mid) => Ok(Some(mid)),
        None => {
            error!(
                "Skipping {} ({}): no Message-ID header. \
                 jma requires Message-ID to anchor idempotency; \
                 fix the file or remove it.",
                maildir_id,
                path.display()
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jmap::types::MaybeReference;
    use crate::maildir_ops::store::ensure_maildir;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn write_message(dir: &Path, sub: &str, filename: &str, body: &str) {
        let path = dir.join(sub).join(filename);
        fs::write(&path, body).unwrap();
    }

    /// Build a single binding for a scan_folder test call. Tests
    /// conventionally tag a folder "INBOX" / "Spam" / "Archive" with
    /// mailbox id "MB-INBOX" / "MB-SPAM" / "MB-ARCH"; the helper
    /// keeps the assertion sites readable without pulling in the
    /// full MailboxBindings shape every time. Returns an `Arc` so
    /// the test sites match `scan_folder`'s `&Arc<...>` signature.
    fn binding(folder: &str, mailbox_id: &str) -> Arc<MailboxFolderBinding> {
        Arc::new(MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value(mailbox_id.into()),
            server_name: folder.to_string(),
            maildir_folder: folder.to_string(),
            remote_path: folder.to_string(),
        })
    }

    /// `MailboxBindings` with the single folder/id pair the
    /// scan_paths tests need to resolve event-path folders to JMAP
    /// mailbox ids inside classify_changes.
    fn bindings_with(folder: &str, mailbox_id: &str) -> MailboxBindings {
        let mut b = MailboxBindings::builder();
        b.insert(MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value(mailbox_id.into()),
            server_name: folder.to_string(),
            maildir_folder: folder.to_string(),
            remote_path: folder.to_string(),
        });
        b.build()
    }

    /// `ScanResult.local_flags` is the contract reconcile reads at
    /// adoption emit sites. Pin its shape here: one entry per
    /// observed cur/ file with the filename's flag suffix, plus
    /// one entry per new/ id with an empty suffix (since the spec
    /// doesn't define flags for new/ and the maildir crate doesn't
    /// expose them either). A regression that broke this map would
    /// otherwise surface only as a downstream local_state.flags
    /// mis-seeding, which would be tedious to bisect back to scan.
    #[test]
    fn scan_folder_populates_local_flags_for_cur_and_new() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();

        let cur_seen = "1700000000.M1.host";
        let cur_replied = "1700000001.M2.host";
        let new_bare = "1700000002.M3.host";
        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";
        write_message(&inbox_path, "cur", &format!("{cur_seen}:2,S"), body);
        write_message(&inbox_path, "cur", &format!("{cur_replied}:2,RS"), body);
        write_message(&inbox_path, "new", new_bare, body);

        let known = HashMap::new();
        let result = scan_folder(&inbox, &binding("INBOX", "MB-INBOX"), &known).unwrap();

        assert_eq!(
            result.local_flags.get(&MaildirId::from(cur_seen)),
            Some(&"S".to_string()),
            "cur/ file with `:2,S` must map to flags `S`: {:?}",
            result.local_flags
        );
        assert_eq!(
            result.local_flags.get(&MaildirId::from(cur_replied)),
            Some(&"RS".to_string()),
            "cur/ file with `:2,RS` must map to flags `RS`: {:?}",
            result.local_flags
        );
        assert_eq!(
            result.local_flags.get(&MaildirId::from(new_bare)),
            Some(&String::new()),
            "new/ file must map to empty flags (spec-undefined for new/): {:?}",
            result.local_flags
        );
        assert_eq!(
            result.local_flags.len(),
            3,
            "no extra entries beyond the three observed files: {:?}",
            result.local_flags
        );
    }

    /// Maildir-id-preserving cross-folder move: the spec recommends that
    /// movers preserve the unique part of the filename. When that
    /// happens, scan_folder must emit a NewMessage on the destination
    /// side so reconcile's move pre-pass can pair it with the
    /// DeletedMessage emitted on the source side. Without this, the
    /// folder mismatch is silently swallowed at the flags-match branch
    /// and the move degrades into a destroy + re-upload (or, worse, a
    /// backwards MoveLocal once DB and server diverge).
    #[test]
    fn cross_folder_id_preserving_move_emits_new_on_destination() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let spam_path = tmp.path().join("Spam");
        let _inbox = ensure_maildir(&inbox_path).unwrap();
        let spam = ensure_maildir(&spam_path).unwrap();

        // The MUA preserved the unique part across folders.
        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,FS");
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        write_message(&spam_path, "cur", &filename, body);

        // DB still believes the file lives in INBOX with the same flags
        // (the MUA didn't change them, only the folder).
        let mut known = HashMap::new();
        known.insert(unique.into(), ("MB-INBOX".into(), "FS".to_string()));

        let ScanResult { changes, .. } =
            scan_folder(&spam, &binding("Spam", "MB-SPAM"), &known).unwrap();

        assert_eq!(
            changes.len(),
            1,
            "destination scan must emit one change for an id-preserving move, got {:?}",
            changes
        );
        match &changes[0] {
            LocalChange::NewMessage {
                maildir_id,
                binding,
                message_id,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(binding.maildir_folder, "Spam");
                assert_eq!(message_id.as_ref(), "a@x");
            }
            other => panic!("expected NewMessage on destination scan, got {:?}", other),
        }
    }

    /// Server-side mailbox rename: the JMAP mailbox id is stable, the
    /// on-disk folder name changes. The DB-recorded mailbox_id agrees
    /// with the binding's even though the binding's folder is the new
    /// name. Comparing on mailbox_id (not folder string) is what keeps
    /// this case quiet -- the old folder-string check would misfire
    /// here and emit a phantom NewMessage that reconcile would then
    /// pair into a backwards MoveLocal. Pins the comparison axis the
    /// new behavior depends on.
    #[test]
    fn rename_preserving_mailbox_id_emits_nothing() {
        let tmp = TempDir::new().unwrap();
        let folder_path = tmp.path().join("Inbox-renamed");
        let folder_maildir = ensure_maildir(&folder_path).unwrap();

        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,FS");
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        write_message(&folder_path, "cur", &filename, body);

        // DB recorded the file against MB-INBOX. The binding still
        // resolves to MB-INBOX but at a different on-disk path (post-
        // rename). Flags agree, so the only signal a folder-string
        // check would have to fire is "different folder name."
        let mut known = HashMap::new();
        known.insert(unique.into(), ("MB-INBOX".into(), "FS".to_string()));

        let ScanResult { changes, .. } = scan_folder(
            &folder_maildir,
            &binding("Inbox-renamed", "MB-INBOX"),
            &known,
        )
        .unwrap();

        assert!(
            changes.is_empty(),
            "rename that preserves mailbox identity must not emit any \
             LocalChange (a NewMessage here would pair into a backwards \
             MoveLocal in reconcile): {:?}",
            changes
        );
    }

    /// The source-side scan during the same move: file is gone from
    /// INBOX, scan_folder must emit DeletedMessage(INBOX). Reconcile's
    /// move pre-pass will pair this with the destination-side
    /// NewMessage by Message-ID. Regression guard for the existing
    /// deletion detection.
    #[test]
    fn cross_folder_id_preserving_move_emits_deleted_on_source() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();
        // INBOX is empty (file was renamed away).

        let unique = "1700000000.M1.host";
        let mut known = HashMap::new();
        known.insert(unique.into(), ("MB-INBOX".into(), "FS".to_string()));

        let ScanResult { changes, .. } =
            scan_folder(&inbox, &binding("INBOX", "MB-INBOX"), &known).unwrap();

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::DeletedMessage {
                maildir_id,
                binding,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(binding.maildir_folder, "INBOX");
            }
            other => panic!("expected DeletedMessage on source scan, got {:?}", other),
        }
    }

    /// Same folder, same maildir_id, different flags: still emits
    /// FlagsChanged (not NewMessage). Guards the fix from
    /// over-triggering when only the suffix changed.
    #[test]
    fn same_folder_flag_change_emits_flags_changed() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,FS");
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        write_message(&inbox_path, "cur", &filename, body);

        let mut known = HashMap::new();
        known.insert(unique.into(), ("MB-INBOX".into(), "F".to_string()));

        let ScanResult { changes, .. } =
            scan_folder(&inbox, &binding("INBOX", "MB-INBOX"), &known).unwrap();

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::FlagsChanged {
                maildir_id,
                old_flags,
                new_flags,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(old_flags, "F");
                assert_eq!(new_flags, "FS");
            }
            other => panic!("expected FlagsChanged, got {:?}", other),
        }
    }

    /// Maildir-id-regenerating cross-folder move (Gnus's nnmaildir,
    /// mu4e, mbsync's own moves). The destination file has a brand-new
    /// unique part — scan must emit DeletedMessage(src) and
    /// NewMessage(dst) sharing only the Message-ID, so reconcile's
    /// move pre-pass can pair them. This is the path the move pre-pass
    /// was originally written for; this test guards the scan-layer end
    /// of it. Combined with the reconcile-side
    /// `cross_folder_local_move_emits_move_remote_and_adopt` test,
    /// this validates the full Gnus-style move pipeline.
    #[test]
    fn cross_folder_id_changing_move_emits_both_halves() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let spam_path = tmp.path().join("Spam");
        let inbox = ensure_maildir(&inbox_path).unwrap();
        let spam = ensure_maildir(&spam_path).unwrap();
        // INBOX is empty (Gnus removed the source file as part of the move).
        // Spam has the new file with a fresh unique id.
        let old_id = "1700000000.M1.host";
        let new_id = "1700000001.M2.host";
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        let new_filename = format!("{new_id}:2,FS");
        write_message(&spam_path, "cur", &new_filename, body);

        // DB still believes the file is in INBOX with its old id.
        let mut known = HashMap::new();
        known.insert(old_id.into(), ("MB-INBOX".into(), "FS".to_string()));

        let ScanResult {
            changes: inbox_changes,
            ..
        } = scan_folder(&inbox, &binding("INBOX", "MB-INBOX"), &known).unwrap();
        let ScanResult {
            changes: spam_changes,
            ..
        } = scan_folder(&spam, &binding("Spam", "MB-SPAM"), &known).unwrap();

        assert_eq!(
            inbox_changes.len(),
            1,
            "expected one DeletedMessage on INBOX"
        );
        match &inbox_changes[0] {
            LocalChange::DeletedMessage {
                maildir_id,
                binding,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), old_id);
                assert_eq!(binding.maildir_folder, "INBOX");
            }
            other => panic!("expected DeletedMessage, got {:?}", other),
        }

        assert_eq!(spam_changes.len(), 1, "expected one NewMessage on Spam");
        match &spam_changes[0] {
            LocalChange::NewMessage {
                maildir_id,
                binding,
                message_id,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), new_id);
                assert_eq!(binding.maildir_folder, "Spam");
                assert_eq!(message_id.as_ref(), "a@x");
            }
            other => panic!("expected NewMessage, got {:?}", other),
        }
    }

    /// Pins the load-bearing premise: an *unknown* file in new/ must
    /// not produce a `LocalChange::NewMessage`. Pre-simplification scan
    /// would have emitted one (the "rescue" path); the new contract
    /// says new/ is jma's own delivery zone, anything we wrote there
    /// is already in the DB, and anything an external MDA wrote we
    /// pick up post-promotion via the cur/ scan. If this test goes
    /// red, scan has regressed to emitting NewMessage from new/ --
    /// which would loop on every fresh download (deliver to new/ ->
    /// scan -> NewMessage -> reconcile sees a local-only message ->
    /// upload duplicate to server).
    #[test]
    fn unknown_file_in_new_emits_no_change() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";
        write_message(&inbox_path, "new", unique, body);

        let known = HashMap::new();
        let ScanResult { changes, .. } =
            scan_folder(&inbox, &binding("INBOX", "MB-INBOX"), &known).unwrap();

        assert!(
            changes.is_empty(),
            "unknown new/ file must not surface as a LocalChange, got {:?}",
            changes
        );
    }

    /// Files sitting in new/ -- whether bare (`<unique>`) or
    /// suffix-bearing (`<unique>:2,<flags>`, the shape jma writes when
    /// delivering an unseen message with server-set flags) -- must NOT
    /// produce any LocalChange. They're tracked in the DB at delivery
    /// time and only become "real" changes once an MUA promotes them
    /// to cur/. Equally important, both shapes must register as
    /// observed so the deletion-detection loop doesn't fire
    /// DeletedMessage against an undelivered-but-pending file (which
    /// would cascade into a DestroyRemote and silently delete the
    /// message server-side).
    #[test]
    fn new_files_known_to_db_do_not_trigger_deleted_message() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();

        let bare = "1700000000.M1.host";
        let suffixed_unique = "1700000001.M2.host";
        let suffixed = format!("{suffixed_unique}:2,F");
        let body = "Message-ID: <a@x>\r\n\r\nbody\r\n";
        write_message(&inbox_path, "new", bare, body);
        write_message(&inbox_path, "new", &suffixed, body);

        // Both ids are known to the DB (delivery-time tracking). If
        // scan failed to observe either, the deletion-detection loop
        // would emit DeletedMessage for the missing one -- the failure
        // mode this test pins. The suffixed file's `:2,F` portion
        // must be stripped by the walker so the canonical id matches
        // what the DB indexed at delivery time.
        let mut known = HashMap::new();
        known.insert(bare.into(), ("MB-INBOX".into(), "".to_string()));
        known.insert(suffixed_unique.into(), ("MB-INBOX".into(), "F".to_string()));

        let ScanResult { changes, .. } =
            scan_folder(&inbox, &binding("INBOX", "MB-INBOX"), &known).unwrap();

        // No LocalChange of any kind: not a NewMessage (new/ files
        // don't surface as changes), and crucially not a
        // DeletedMessage for either DB-anchored id.
        assert!(
            changes.is_empty(),
            "new/ files known to the DB must not emit any LocalChange (\
             a DeletedMessage here would cascade into DestroyRemote and \
             silently delete server-side): {:?}",
            changes
        );
    }

    /// A file with no Message-ID header (RFC 5322 says it SHOULD be
    /// present, but isn't a hard MUST) must NOT be ingested: jma
    /// anchors idempotency on Message-ID, and emitting NewMessage
    /// without one would either drop the message at reconcile or
    /// produce a server-side duplicate after a state DB wipe. Scan
    /// drops the file (logs error!) so it stays on disk untouched
    /// -- crucially, it does NOT emit DeletedMessage either,
    /// because the file is still observed on disk; only files
    /// missing from disk trigger deletion-detection.
    #[test]
    fn scan_folder_msgid_missing_does_not_trigger_deleted_message() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();

        // No Message-ID header -- only a Subject + body.
        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,");
        let body = "Subject: no msgid\r\n\r\nbody\r\n";
        write_message(&inbox_path, "cur", &filename, body);

        // DB anchors the same id in INBOX. The deletion-detection
        // loop must NOT fire DeletedMessage for it, even though we
        // dropped the file from LocalChange emission for missing
        // its Message-ID -- the file is still on disk and still
        // observed.
        let mut known = HashMap::new();
        known.insert(unique.into(), ("MB-INBOX".into(), "".to_string()));

        let ScanResult { changes, .. } =
            scan_folder(&inbox, &binding("INBOX", "MB-INBOX"), &known).unwrap();

        assert!(
            changes.is_empty(),
            "expected no LocalChanges for a file without Message-ID \
             (especially no DeletedMessage -- the file is on disk): {:?}",
            changes
        );
    }

    /// Same folder, file already known to the DB, but the file's
    /// Message-ID header is missing. This hits the
    /// `Some((_, known_flags))` branch where `require_message_id`
    /// isn't even called (flags match), so no `NewMessage` is
    /// emitted -- but more importantly: the deletion-detection loop
    /// must NOT spuriously emit a DeletedMessage for it. Pins the
    /// "deletion-detection only fires for files missing from disk,
    /// not for files we declined to emit a change for" contract.
    #[test]
    fn scan_folder_known_file_without_message_id_emits_nothing() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,FS");
        let body = "Subject: no msgid\r\n\r\nbody\r\n";
        write_message(&inbox_path, "cur", &filename, body);

        // DB has the file in the same folder with the same flags.
        let mut known = HashMap::new();
        known.insert(unique.into(), ("MB-INBOX".into(), "FS".to_string()));

        let ScanResult { changes, .. } =
            scan_folder(&inbox, &binding("INBOX", "MB-INBOX"), &known).unwrap();

        assert!(
            changes.is_empty(),
            "no changes expected for an unchanged known file, got {:?}",
            changes
        );
    }

    /// Destination-side scan of a Message-ID-less cross-folder move:
    /// the cross-folder branch must run `require_message_id` before
    /// emitting NewMessage. With no Message-ID, the move pre-pass in
    /// reconcile (which keys on Message-ID) couldn't pair this with
    /// any source-side delete anyway, so we refuse at the scan
    /// boundary and the destination produces zero changes.
    #[test]
    fn scan_folder_skips_cross_folder_move_without_message_id() {
        let tmp = TempDir::new().unwrap();
        let spam_path = tmp.path().join("Spam");
        let spam = ensure_maildir(&spam_path).unwrap();

        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,FS");
        // No Message-ID header.
        let body = "Subject: no msgid\r\n\r\nbody\r\n";
        write_message(&spam_path, "cur", &filename, body);

        // DB believes the file lives in INBOX.
        let mut known = HashMap::new();
        known.insert(unique.into(), ("MB-INBOX".into(), "FS".to_string()));

        let ScanResult { changes, .. } =
            scan_folder(&spam, &binding("Spam", "MB-SPAM"), &known).unwrap();

        assert!(
            changes.is_empty(),
            "expected no LocalChanges for a Message-ID-less cross-folder move, got {:?}",
            changes
        );
    }

    /// Build a single-folder `known_states` map for `scan_paths` tests.
    /// Each row's second tuple component is the JMAP mailbox id the DB
    /// recorded the file under -- the value `classify_changes` compares
    /// against the binding's id to spot a cross-mailbox move.
    fn known_for(
        folder: &str,
        rows: &[(&str, &str, &str)],
    ) -> HashMap<String, HashMap<MaildirId, (JmapMailboxId, String)>> {
        let mut inner = HashMap::new();
        for (id, db_mailbox_id, flags) in rows {
            inner.insert(
                MaildirId::from(*id),
                ((*db_mailbox_id).into(), flags.to_string()),
            );
        }
        let mut outer = HashMap::new();
        outer.insert(folder.to_string(), inner);
        outer
    }

    /// `parse_event_path` recognises the canonical
    /// `<root>/<folder>/cur/<unique>:2,<flags>` shape and pulls out
    /// the four pieces. Pins the format we expect from
    /// notify-debouncer-mini paths.
    #[test]
    fn parse_event_path_cur_with_flags() {
        let root = Path::new("/Users/u/Mail");
        let p = Path::new("/Users/u/Mail/INBOX/cur/1700000000.M1.host:2,FS");
        let (folder, subdir, id, flags) = parse_event_path(root, p).unwrap();
        assert_eq!(folder, "INBOX");
        assert_eq!(subdir, "cur");
        assert_eq!(id.as_ref(), "1700000000.M1.host");
        assert_eq!(flags, "FS");
    }

    /// `new/` files can be either bare `<unique>` or
    /// suffix-bearing `<unique>:2,<flags>` (jma writes the latter for
    /// non-Seen flag preservation). Both shapes parse.
    #[test]
    fn parse_event_path_new_bare_and_suffixed() {
        let root = Path::new("/Users/u/Mail");
        let bare = Path::new("/Users/u/Mail/INBOX/new/1700000000.M1.host");
        let (folder, subdir, id, flags) = parse_event_path(root, bare).unwrap();
        assert_eq!(folder, "INBOX");
        assert_eq!(subdir, "new");
        assert_eq!(id.as_ref(), "1700000000.M1.host");
        assert_eq!(flags, "");

        let suffixed = Path::new("/Users/u/Mail/INBOX/new/1700000000.M1.host:2,F");
        let (_, _, id2, flags2) = parse_event_path(root, suffixed).unwrap();
        assert_eq!(id2.as_ref(), "1700000000.M1.host");
        assert_eq!(flags2, "F");
    }

    /// Maildir++ nested folder: the nested name (`[Airmail]/Done`)
    /// becomes the folder name, with `cur`/`new` still split out.
    #[test]
    fn parse_event_path_nested_folder() {
        let root = Path::new("/Users/u/Mail");
        let p = Path::new("/Users/u/Mail/[Airmail]/Done/cur/1700000000.M1.host:2,S");
        let (folder, subdir, id, _) = parse_event_path(root, p).unwrap();
        assert_eq!(folder, "[Airmail]/Done");
        assert_eq!(subdir, "cur");
        assert_eq!(id.as_ref(), "1700000000.M1.host");
    }

    /// Path outside the maildir root or without a `cur`/`new`
    /// segment returns `None` -- the watcher's
    /// `is_maildir_message_path` filter should keep these out, but
    /// the parser itself stays defensive.
    #[test]
    fn parse_event_path_rejects_unfit_paths() {
        let root = Path::new("/Users/u/Mail");
        // Outside root.
        assert!(parse_event_path(root, Path::new("/tmp/foo")).is_none());
        // No cur/new segment.
        assert!(parse_event_path(root, Path::new("/Users/u/Mail/.jma.db")).is_none());
        // Empty filename after cur/.
        assert!(parse_event_path(root, Path::new("/Users/u/Mail/INBOX/cur/")).is_none());
    }

    /// Same-folder flag rename (`<id>:2,F` -> `<id>:2,FS`) drives a
    /// single FlagsChanged event from `scan_paths`. Both source and
    /// destination paths arrive in the events; only the destination
    /// exists on disk, so the live representative is the dest path.
    #[test]
    fn scan_paths_flag_rename_emits_flags_changed() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        // Only the destination shape exists on disk (the rename
        // already happened before the events were delivered).
        write_message(&inbox_path, "cur", &format!("{unique}:2,FS"), body);

        let known = known_for("INBOX", &[(unique, "MB-INBOX", "F")]);
        let event_paths = vec![
            inbox_path.join("cur").join(format!("{unique}:2,F")),
            inbox_path.join("cur").join(format!("{unique}:2,FS")),
        ];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with("INBOX", "MB-INBOX"),
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::FlagsChanged {
                maildir_id,
                old_flags,
                new_flags,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(old_flags, "F");
                assert_eq!(new_flags, "FS");
            }
            other => panic!("expected FlagsChanged, got {:?}", other),
        }
    }

    /// Live cur/ entry whose DB state already matches: classify_changes
    /// emits nothing, so scan_paths returns an empty list. This is the
    /// "ghost" trigger case -- jma's own write completed and updated
    /// the DB before the fsevents debouncer fired, so the eventual
    /// path-driven scan finds disk and DB in agreement.
    #[test]
    fn scan_paths_unchanged_state_emits_nothing() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        write_message(&inbox_path, "cur", &format!("{unique}:2,S"), body);

        let known = known_for("INBOX", &[(unique, "MB-INBOX", "S")]);
        let event_paths = vec![inbox_path.join("cur").join(format!("{unique}:2,S"))];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with("INBOX", "MB-INBOX"),
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        assert!(
            changes.is_empty(),
            "expected no changes for in-sync file, got {:?}",
            changes
        );
    }

    /// File deletion: events fire for the source path, file no longer
    /// exists on disk, DB anchors the id to this folder -> emit
    /// DeletedMessage. This is the case classify_changes would miss
    /// without explicit_deletes plumbed through.
    #[test]
    fn scan_paths_missing_file_emits_deleted() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();
        // INBOX is empty: the file was removed before scan ran.

        let unique = "1700000000.M1.host";
        let known = known_for("INBOX", &[(unique, "MB-INBOX", "S")]);
        let event_paths = vec![inbox_path.join("cur").join(format!("{unique}:2,S"))];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with("INBOX", "MB-INBOX"),
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::DeletedMessage {
                maildir_id,
                binding,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(binding.maildir_folder, "INBOX");
            }
            other => panic!("expected DeletedMessage, got {:?}", other),
        }
    }

    /// Cross-folder id-preserving rename: source folder events show
    /// the file gone (no live path for that group); destination
    /// folder events show the file present. scan_paths emits
    /// DeletedMessage(source) + NewMessage(dest), which reconcile
    /// pairs by Message-ID via the move pre-pass.
    #[test]
    fn scan_paths_cross_folder_move_emits_both_halves() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let spam_path = tmp.path().join("Spam");
        let _inbox = ensure_maildir(&inbox_path).unwrap();
        let _spam = ensure_maildir(&spam_path).unwrap();

        let unique = "1700000000.M1.host";
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        // Only the dest exists; source is gone.
        write_message(&spam_path, "cur", &format!("{unique}:2,FS"), body);

        let mut known_states = HashMap::new();
        let mut inner = HashMap::new();
        inner.insert(
            MaildirId::from(unique),
            ("MB-INBOX".into(), "FS".to_string()),
        );
        known_states.insert("INBOX".into(), inner.clone());
        known_states.insert("Spam".into(), inner);

        let event_paths = vec![
            inbox_path.join("cur").join(format!("{unique}:2,FS")),
            spam_path.join("cur").join(format!("{unique}:2,FS")),
        ];

        let mut bindings = MailboxBindings::builder();
        bindings.insert(MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value("MB-INBOX".into()),
            server_name: "INBOX".to_string(),
            maildir_folder: "INBOX".to_string(),
            remote_path: "INBOX".to_string(),
        });
        bindings.insert(MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value("MB-SPAM".into()),
            server_name: "Spam".to_string(),
            maildir_folder: "Spam".to_string(),
            remote_path: "Spam".to_string(),
        });
        let bindings = bindings.build();
        let mut changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known_states,
            &bindings,
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        // Order isn't guaranteed (HashMap iteration), so sort for the
        // assertion.
        changes.sort_by_key(|c| match c {
            LocalChange::DeletedMessage { binding, .. } => format!("0:{}", binding.maildir_folder),
            LocalChange::NewMessage { binding, .. } => format!("1:{}", binding.maildir_folder),
            LocalChange::FlagsChanged { binding, .. } => format!("2:{}", binding.maildir_folder),
            // Folder-lifecycle variants have no emitter; the test
            // exercises message-level scan_paths only.
            LocalChange::LocalFolderCreated { path, .. } => format!("3:{}", path.display()),
            LocalChange::LocalFolderDeleted { binding, .. } => {
                format!("3:{}", binding.maildir_folder)
            }
            LocalChange::LocalFolderRenamed { from_binding, .. } => {
                format!("3:{}", from_binding.maildir_folder)
            }
        });
        assert_eq!(changes.len(), 2);
        match &changes[0] {
            LocalChange::DeletedMessage { binding, .. } => {
                assert_eq!(binding.maildir_folder, "INBOX")
            }
            other => panic!("expected DeletedMessage on INBOX, got {:?}", other),
        }
        match &changes[1] {
            LocalChange::NewMessage { binding, .. } => assert_eq!(binding.maildir_folder, "Spam"),
            other => panic!("expected NewMessage on Spam, got {:?}", other),
        }
    }

    /// Missing new/ path with a DB anchor: the file was unlinked
    /// from new/ before MUA promotion (user deleted the unread
    /// message directly from new/, or the MDA cleaned it up).
    /// scan_paths must emit `DeletedMessage` so the deletion
    /// propagates upstream. Load-bearing for
    /// `daemon::watcher::batch_might_emit_changes`'s "drop
    /// all-live-new batches" optimization: that filter must keep
    /// batches whose new/ paths have gone missing, otherwise this
    /// DeletedMessage is silently swallowed.
    #[test]
    fn scan_paths_missing_new_path_emits_deleted_when_db_anchored() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();
        // INBOX/new/ is empty -- the file was removed.

        let unique = "1700000000.M1.host";
        let known = known_for("INBOX", &[(unique, "MB-INBOX", "")]);
        let event_paths = vec![inbox_path.join("new").join(unique)];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with("INBOX", "MB-INBOX"),
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::DeletedMessage {
                maildir_id,
                binding,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(binding.maildir_folder, "INBOX");
            }
            other => panic!(
                "expected DeletedMessage for missing new/ path, got {:?}",
                other
            ),
        }
    }

    /// Live new/ event by itself: no change emitted. The MUA's
    /// eventual cur/ promotion is the trigger we care about. Pins
    /// the parity with scan_folder's new/ contract.
    #[test]
    fn scan_paths_new_only_event_emits_nothing() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        write_message(
            &inbox_path,
            "new",
            unique,
            "Message-ID: <a@x>\r\n\r\nbody\r\n",
        );

        // DB already knows about this delivery (jma wrote it itself).
        let known = known_for("INBOX", &[(unique, "MB-INBOX", "")]);
        let event_paths = vec![inbox_path.join("new").join(unique)];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with("INBOX", "MB-INBOX"),
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        assert!(
            changes.is_empty(),
            "new/ event must not produce a LocalChange, got {:?}",
            changes
        );
    }

    /// New/ -> cur/ MUA promotion arriving in the same event batch:
    /// both subdirs have a path for the same `(folder, maildir_id)`,
    /// but only the cur/ shape exists on disk after the rename. The
    /// `live` selector must prefer the cur/ candidate so classify_changes
    /// observes the post-promotion flags rather than dropping the
    /// classification entirely as if it were a new/-only event.
    #[test]
    fn scan_paths_prefers_cur_over_new_when_both_present() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        // Only the cur/ destination exists; the new/ source path is gone.
        write_message(&inbox_path, "cur", &format!("{unique}:2,S"), body);

        let known = known_for("INBOX", &[(unique, "MB-INBOX", "")]);
        let event_paths = vec![
            inbox_path.join("new").join(unique),
            inbox_path.join("cur").join(format!("{unique}:2,S")),
        ];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with("INBOX", "MB-INBOX"),
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::FlagsChanged {
                maildir_id,
                old_flags,
                new_flags,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(old_flags, "");
                assert_eq!(new_flags, "S");
            }
            other => panic!("expected FlagsChanged from cur/ live path, got {:?}", other),
        }
    }

    /// Unparseable event paths interleaved with valid ones: the
    /// invalid ones are silently skipped by `parse_event_path`, the
    /// valid ones still get classified. Pins that the integration in
    /// scan_paths doesn't trip on a path the parser rejects (which
    /// would otherwise abort the whole batch).
    #[test]
    fn scan_paths_skips_unparseable_paths() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let _inbox = ensure_maildir(&inbox_path).unwrap();

        let unique = "1700000000.M1.host";
        let body = "Message-ID: <a@x>\r\nSubject: t\r\n\r\nbody\r\n";
        write_message(&inbox_path, "cur", &format!("{unique}:2,S"), body);

        let known = known_for("INBOX", &[(unique, "MB-INBOX", "")]);
        let event_paths = vec![
            // No /cur/ or /new/ segment.
            tmp.path().join(".jma.db"),
            // Outside maildir root.
            PathBuf::from("/tmp/unrelated"),
            // Real one.
            inbox_path.join("cur").join(format!("{unique}:2,S")),
        ];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with("INBOX", "MB-INBOX"),
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        assert_eq!(
            changes.len(),
            1,
            "unparseable paths must be skipped without aborting, got {:?}",
            changes
        );
    }

    /// Path in a folder that isn't synced but IS a layout-eligible
    /// shape: emit `LocalFolderCreated` for it (once per folder),
    /// then drop the message-level event (no binding to classify
    /// against). The folder-discovery emission gives downstream
    /// reconcile the option to push the unbound folder to the
    /// server; the message under it can't be classified until the
    /// folder is bound, and the next Full-scope cycle picks it up
    /// once binding lands.
    #[test]
    fn scan_paths_emits_local_folder_created_for_eligible_unbound_folder() {
        let tmp = TempDir::new().unwrap();
        let other_path = tmp.path().join("Untracked");
        let _other = ensure_maildir(&other_path).unwrap();

        let unique = "1700000000.M1.host";
        write_message(
            &other_path,
            "cur",
            &format!("{unique}:2,"),
            "Message-ID: <a@x>\r\n\r\nbody\r\n",
        );

        // known_states only covers INBOX.
        let known = known_for("INBOX", &[]);
        let event_paths = vec![other_path.join("cur").join(format!("{unique}:2,"))];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with("INBOX", "MB-INBOX"),
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        assert_eq!(changes.len(), 1, "expected one folder discovery emission");
        match &changes[0] {
            LocalChange::LocalFolderCreated { path, sentinel } => {
                assert_eq!(path, &other_path);
                assert!(sentinel.is_none());
            }
            other => panic!("expected LocalFolderCreated, got {:?}", other),
        }
    }

    /// Multiple events under the same unbound folder dedupe to a
    /// single `LocalFolderCreated` -- the variant is folder-level,
    /// not message-level.
    #[test]
    fn scan_paths_dedupes_local_folder_created_across_events() {
        let tmp = TempDir::new().unwrap();
        let other_path = tmp.path().join("Untracked");
        let _other = ensure_maildir(&other_path).unwrap();

        write_message(
            &other_path,
            "cur",
            "1.x:2,",
            "Message-ID: <a@x>\r\n\r\nbody\r\n",
        );
        write_message(
            &other_path,
            "cur",
            "2.x:2,",
            "Message-ID: <b@x>\r\n\r\nbody\r\n",
        );

        let known = known_for("INBOX", &[]);
        let event_paths = vec![
            other_path.join("cur").join("1.x:2,"),
            other_path.join("cur").join("2.x:2,"),
        ];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with("INBOX", "MB-INBOX"),
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        assert_eq!(
            changes.len(),
            1,
            "two events on the same folder must dedupe, got {:?}",
            changes
        );
    }

    /// Under `Flat`, the layout pins mailbox names at depth 1.
    /// A path two levels deep (`Outer/Inner/cur/file`) is not a
    /// valid mailbox shape, so its folder string `Outer/Inner` is
    /// dropped at the layout-eligibility check rather than
    /// surfacing as a `LocalFolderCreated`.
    #[test]
    fn scan_paths_drops_layout_ineligible_unbound_folder() {
        let tmp = TempDir::new().unwrap();
        let nested = tmp.path().join("Outer").join("Inner");
        let _nested = ensure_maildir(&nested).unwrap();
        write_message(
            &nested,
            "cur",
            "1.x:2,",
            "Message-ID: <a@x>\r\n\r\nbody\r\n",
        );

        let known = known_for("INBOX", &[]);
        let event_paths = vec![nested.join("cur").join("1.x:2,")];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with("INBOX", "MB-INBOX"),
            FolderLayout::Flat,
        )
        .unwrap()
        .changes;
        assert!(
            changes.is_empty(),
            "Flat must not emit LocalFolderCreated for depth-2 paths, got {:?}",
            changes
        );
    }

    /// Under `MaildirPP`, only first-level dot-prefixed folders
    /// are mailboxes. A non-hidden first-level dir (`Notes/cur/`)
    /// is not a valid MaildirPP mailbox shape and must be dropped
    /// rather than emitting `LocalFolderCreated`.
    #[test]
    fn scan_paths_maildir_pp_drops_non_dot_prefixed_unbound_folder() {
        let tmp = TempDir::new().unwrap();
        let stray = tmp.path().join("Notes");
        let _stray = ensure_maildir(&stray).unwrap();
        write_message(&stray, "cur", "1.x:2,", "Message-ID: <a@x>\r\n\r\nbody\r\n");

        let known = known_for(".INBOX", &[]);
        let event_paths = vec![stray.join("cur").join("1.x:2,")];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with(".INBOX", "MB-INBOX"),
            FolderLayout::MaildirPP,
        )
        .unwrap()
        .changes;
        assert!(
            changes.is_empty(),
            "MaildirPP must skip non-dot-prefixed unbound folders, got {:?}",
            changes
        );
    }

    /// Under `MaildirPP`, a dot-prefixed first-level folder is a
    /// valid mailbox shape and surfaces as `LocalFolderCreated`.
    #[test]
    fn scan_paths_maildir_pp_emits_for_dot_prefixed_unbound_folder() {
        let tmp = TempDir::new().unwrap();
        let dotted = tmp.path().join(".Archive.2024");
        let _dotted = ensure_maildir(&dotted).unwrap();
        write_message(
            &dotted,
            "cur",
            "1.x:2,",
            "Message-ID: <a@x>\r\n\r\nbody\r\n",
        );

        let known = known_for(".INBOX", &[]);
        let event_paths = vec![dotted.join("cur").join("1.x:2,")];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings_with(".INBOX", "MB-INBOX"),
            FolderLayout::MaildirPP,
        )
        .unwrap()
        .changes;
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::LocalFolderCreated { path, .. } => assert_eq!(path, &dotted),
            other => panic!("expected LocalFolderCreated, got {:?}", other),
        }
    }

    /// Under `Fs`, a path at any depth is a valid mailbox shape
    /// (subject to the layout's own segment validator, which
    /// `layout_eligible_folder` does not re-check here). A
    /// `Parent/Child/cur/file` event whose folder is unbound
    /// surfaces as `LocalFolderCreated` for the deepest folder.
    #[test]
    fn scan_paths_fs_emits_for_nested_unbound_folder() {
        let tmp = TempDir::new().unwrap();
        let parent = tmp.path().join("Archive");
        let child = parent.join("2024");
        ensure_maildir(&parent).unwrap();
        let _child = ensure_maildir(&child).unwrap();
        write_message(&child, "cur", "1.x:2,", "Message-ID: <a@x>\r\n\r\nbody\r\n");

        let mut bindings = MailboxBindings::builder();
        bindings.insert(MailboxFolderBinding {
            jmap_mailbox_id: MaybeReference::Value("MB-ARCH".into()),
            server_name: "Archive".to_string(),
            maildir_folder: "Archive".to_string(),
            remote_path: "Archive".to_string(),
        });
        let bindings = bindings.build();
        let known = known_for("Archive", &[]);
        let event_paths = vec![child.join("cur").join("1.x:2,")];

        let changes = scan_paths(
            tmp.path(),
            &event_paths,
            &known,
            &bindings,
            FolderLayout::Fs,
        )
        .unwrap()
        .changes;
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::LocalFolderCreated { path, .. } => assert_eq!(path, &child),
            other => panic!("expected LocalFolderCreated, got {:?}", other),
        }
    }

    /// Empty maildir root: nothing to discover, no LocalFolderCreated
    /// emitted. Behavior is independent of layout.
    #[test]
    fn discover_unbound_folders_empty_root() {
        let tmp = TempDir::new().unwrap();
        let bindings = MailboxBindings::builder().build();
        for layout in [
            FolderLayout::Flat,
            FolderLayout::MaildirPP,
            FolderLayout::Fs,
        ] {
            let changes = discover_unbound_folders(tmp.path(), layout, &bindings);
            assert!(
                changes.is_empty(),
                "layout {:?} should emit nothing",
                layout
            );
        }
    }

    /// Flat layout: an unbound first-level folder with no sentinel
    /// surfaces as a single LocalFolderCreated whose `sentinel` is
    /// `None`. The flat-name convention puts every mailbox at depth
    /// 1.
    #[test]
    fn discover_unbound_folders_flat_emits_for_orphan() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().join("[Airmail].Sent");
        ensure_maildir(&folder).unwrap();
        let bindings = MailboxBindings::builder().build();
        let changes = discover_unbound_folders(tmp.path(), FolderLayout::Flat, &bindings);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::LocalFolderCreated { path, sentinel } => {
                assert_eq!(path, &folder);
                assert!(sentinel.is_none());
            }
            other => panic!("expected LocalFolderCreated, got {:?}", other),
        }
    }

    /// Carrying a sentinel: emit LocalFolderCreated whose `sentinel`
    /// is `Some(_)` (so a consumer can fork "cache lost a binding
    /// the disk pins" from "genuinely new local folder").
    #[test]
    fn discover_unbound_folders_flat_carries_sentinel_when_present() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().join("Recovered");
        ensure_maildir(&folder).unwrap();
        crate::maildir_ops::sentinel::write(
            &folder,
            &crate::maildir_ops::sentinel::MailboxMapping {
                jmap_mailbox_id: JmapMailboxId::from("MB-RECOVERED"),
                parent_jmap_mailbox_id: None,
                server_name: "Recovered".to_string(),
            },
        )
        .unwrap();
        let bindings = MailboxBindings::builder().build();
        let changes = discover_unbound_folders(tmp.path(), FolderLayout::Flat, &bindings);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::LocalFolderCreated { sentinel, .. } => {
                let m = sentinel.as_ref().expect("sentinel should be Some");
                assert_eq!(m.jmap_mailbox_id, JmapMailboxId::from("MB-RECOVERED"));
                assert_eq!(m.server_name, "Recovered");
            }
            other => panic!("expected LocalFolderCreated, got {:?}", other),
        }
    }

    /// A folder already covered by `MailboxBindings` is excluded
    /// from discovery -- the per-binding scan loop handles it.
    /// Layout-agnostic behaviour; exercise under Flat where the
    /// `bindings_with` helper is already set up.
    #[test]
    fn discover_unbound_folders_flat_skips_bound_folders() {
        let tmp = TempDir::new().unwrap();
        let bound = tmp.path().join("INBOX");
        let orphan = tmp.path().join("Projects");
        ensure_maildir(&bound).unwrap();
        ensure_maildir(&orphan).unwrap();
        let bindings = bindings_with("INBOX", "MB-INBOX");
        let changes = discover_unbound_folders(tmp.path(), FolderLayout::Flat, &bindings);
        assert_eq!(changes.len(), 1, "only Projects is unbound");
        match &changes[0] {
            LocalChange::LocalFolderCreated { path, .. } => {
                assert_eq!(path, &orphan);
            }
            other => panic!("expected LocalFolderCreated, got {:?}", other),
        }
    }

    /// Flat layout doesn't recurse: a maildir-shaped directory
    /// nested under another mailbox at depth 2 is NOT a candidate
    /// under Flat (the convention is that hierarchy collapses into
    /// the leaf name via separator-joining, so anything at depth 2
    /// is a misconfiguration this task doesn't try to recover).
    #[test]
    fn discover_unbound_folders_flat_does_not_recurse() {
        let tmp = TempDir::new().unwrap();
        let outer = tmp.path().join("Archive");
        let nested = outer.join("Inner");
        ensure_maildir(&outer).unwrap();
        ensure_maildir(&nested).unwrap();
        let bindings = bindings_with("Archive", "MB-ARCH");
        let changes = discover_unbound_folders(tmp.path(), FolderLayout::Flat, &bindings);
        assert!(
            changes.is_empty(),
            "Flat must not recurse into bound folders, got {:?}",
            changes
        );
    }

    /// MaildirPP layout: only first-level children whose name
    /// starts with `.` are candidates. A non-hidden first-level
    /// dir (like a stray `INBOX/cur/` left over from a Flat
    /// migration) is not a mailbox under MaildirPP and must be
    /// ignored.
    #[test]
    fn discover_unbound_folders_maildir_pp_only_dot_prefixed() {
        let tmp = TempDir::new().unwrap();
        let dotted = tmp.path().join(".Archive.2024");
        let stray = tmp.path().join("INBOX");
        ensure_maildir(&dotted).unwrap();
        ensure_maildir(&stray).unwrap();
        let bindings = MailboxBindings::builder().build();
        let changes = discover_unbound_folders(tmp.path(), FolderLayout::MaildirPP, &bindings);
        assert_eq!(
            changes.len(),
            1,
            "MaildirPP picks only the dotted folder, got {:?}",
            changes
        );
        match &changes[0] {
            LocalChange::LocalFolderCreated { path, .. } => assert_eq!(path, &dotted),
            other => panic!("expected LocalFolderCreated, got {:?}", other),
        }
    }

    /// Fs layout: discovery surfaces every depth that holds a
    /// `cur/` subdirectory, so a `Parent/Child` tree where only the
    /// child is unbound emits exactly one LocalFolderCreated for
    /// the child.
    #[test]
    fn discover_unbound_folders_fs_recurses_into_nested_dirs() {
        let tmp = TempDir::new().unwrap();
        let parent = tmp.path().join("Archive");
        let child = parent.join("2024");
        ensure_maildir(&parent).unwrap();
        ensure_maildir(&child).unwrap();
        let bindings = bindings_with("Archive", "MB-ARCH");
        let changes = discover_unbound_folders(tmp.path(), FolderLayout::Fs, &bindings);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::LocalFolderCreated { path, .. } => {
                assert_eq!(path, &child);
            }
            other => panic!("expected LocalFolderCreated, got {:?}", other),
        }
    }

    /// Non-maildir-shaped directories (no `cur/` subdir) are
    /// ignored across all layouts. A plain `mkdir Projects`
    /// without the maildir triplet doesn't produce a candidate.
    #[test]
    fn discover_unbound_folders_ignores_non_maildir_dirs() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir(tmp.path().join("Projects")).unwrap();
        let bindings = MailboxBindings::builder().build();
        for layout in [
            FolderLayout::Flat,
            FolderLayout::MaildirPP,
            FolderLayout::Fs,
        ] {
            let changes = discover_unbound_folders(tmp.path(), layout, &bindings);
            assert!(
                changes.is_empty(),
                "layout {:?} should ignore non-maildir dirs",
                layout
            );
        }
    }

    /// A malformed sentinel (bytes that don't parse as TOML, or
    /// valid TOML missing the required fields) is indistinguishable
    /// from "no sentinel" at the discovery layer: emit
    /// LocalFolderCreated with `sentinel: None` and let a future
    /// consumer choose between treating the folder as new or
    /// recovering its binding by other means.
    #[test]
    fn discover_unbound_folders_treats_malformed_sentinel_as_none() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().join("Trash");
        ensure_maildir(&folder).unwrap();
        std::fs::write(folder.join(".jma.mapping"), b"this is not toml { broken")
            .expect("plant malformed sentinel");
        let bindings = MailboxBindings::builder().build();
        let changes = discover_unbound_folders(tmp.path(), FolderLayout::Flat, &bindings);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::LocalFolderCreated { path, sentinel } => {
                assert_eq!(path, &folder);
                assert!(
                    sentinel.is_none(),
                    "malformed sentinel must surface as None, got {:?}",
                    sentinel
                );
            }
            other => panic!("expected LocalFolderCreated, got {:?}", other),
        }
    }

    /// `.jma.*` namespace dirs (locks, db, sentinel siblings) are
    /// never folder candidates under any layout, even under
    /// MaildirPP where they happen to share the dot-prefix
    /// convention.
    #[test]
    fn discover_unbound_folders_skips_jma_namespace() {
        let tmp = TempDir::new().unwrap();
        ensure_maildir(&tmp.path().join(".jma.something")).unwrap();
        let bindings = MailboxBindings::builder().build();
        for layout in [
            FolderLayout::Flat,
            FolderLayout::MaildirPP,
            FolderLayout::Fs,
        ] {
            let changes = discover_unbound_folders(tmp.path(), layout, &bindings);
            assert!(
                changes.is_empty(),
                "layout {:?} must skip .jma.* dirs",
                layout
            );
        }
    }
}
