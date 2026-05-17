use anyhow::Result;
use maildir::Maildir;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tracing::{debug, error};

use crate::ids::{MaildirId, MessageId};
use crate::maildir_ops::headers::parse_message_id_from_file;

/// One live `cur/` entry passed into `classify_changes`. Both
/// `scan_folder` (full enumeration via the maildir crate) and
/// `scan_paths` (partial, driven by fsevents) build these from their
/// respective inputs so the classifier sees a uniform shape.
struct CurEntry {
    maildir_id: MaildirId,
    flags: String,
    path: PathBuf,
}

/// One scan pass's output: the classified `LocalChange`s, a
/// MaildirId-keyed map of every on-disk filename's flag suffix the
/// scan visited, and (for exhaustive walks) the list of all observed
/// maildir_ids.
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
///
/// `seen_ids` is populated only by exhaustive walks (`scan_folder`):
/// it's the observed-on-disk set used to detect "in DB but missing"
/// deletions. `scan_paths` returns an empty vec here because a
/// path-driven scan has no exhaustive view -- it sees only the
/// event paths and trusts the event stream to deliver any deletions
/// directly.
#[derive(Debug)]
pub struct ScanResult {
    pub changes: Vec<LocalChange>,
    pub local_flags: HashMap<MaildirId, String>,
    pub seen_ids: Vec<MaildirId>,
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
        folder: String,
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
        folder: String,
    },
    /// The flags on a message file changed.
    FlagsChanged {
        maildir_id: MaildirId,
        folder: String,
        old_flags: String,
        new_flags: String,
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
/// - `new_entries`: live `new/` ids the caller observed. They never
///   produce a LocalChange (the MUA's eventual cur/ promotion is the
///   trigger we care about); they're tracked so callers can derive
///   `seen` for their own deletion detection.
/// - `explicit_deletes`: maildir ids the caller is sure have
///   disappeared from this folder. Each becomes a DeletedMessage.
///   The caller is responsible for ensuring the DB anchors each id
///   to `folder_name` -- we trust the input here so a single function
///   serves both exhaustive (folder-walk) and event-driven
///   (fsevents) deletion sources without an extra mode flag.
///
/// Returns `(changes, seen_ids)` where `seen_ids` is the union of
/// `cur_entries` and `new_entries` ids, useful for callers doing
/// their own folder-wide deletion bookkeeping.
fn classify_changes(
    folder_name: &str,
    cur_entries: &[CurEntry],
    new_entries: &[MaildirId],
    explicit_deletes: &[MaildirId],
    known_state: &HashMap<MaildirId, (String, String)>,
) -> Result<(Vec<LocalChange>, Vec<MaildirId>)> {
    let mut changes = Vec::new();
    let mut seen_ids = Vec::with_capacity(cur_entries.len() + new_entries.len());

    for entry in cur_entries {
        seen_ids.push(entry.maildir_id.clone());
        match known_state.get(&entry.maildir_id) {
            // Maildir-id-preserving cross-folder move: same unique part
            // of the filename, different folder than the DB recorded.
            // Treat the destination side as a NewMessage so reconcile's
            // move pre-pass can pair it (by Message-ID) with the
            // DeletedMessage from the source-folder walk. Without this,
            // the folder mismatch is silently swallowed and the move
            // degrades into a destroy + re-upload (or, after partial
            // state drift, a backwards MoveLocal that undoes the
            // user's move).
            Some((known_folder, _)) if known_folder != folder_name => {
                debug!(
                    "Cross-folder rename detected: {} now in {} (was {})",
                    entry.maildir_id, folder_name, known_folder
                );
                let Some(message_id) = require_message_id(&entry.maildir_id, &entry.path)? else {
                    continue;
                };
                let size_bytes = stat_size(&entry.path);
                changes.push(LocalChange::NewMessage {
                    maildir_id: entry.maildir_id.clone(),
                    folder: folder_name.to_string(),
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
                        folder: folder_name.to_string(),
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
                    folder: folder_name.to_string(),
                    flags: entry.flags.clone(),
                    path: entry.path.clone(),
                    message_id,
                    size_bytes,
                });
            }
        }
    }

    // Walk new/ for presence only -- we never emit a LocalChange for
    // a file there. Premise: jma (as the MDA) is the only writer to
    // new/, and MUAs only ever promote new/ -> cur/. Anything we
    // delivered to new/ is already tracked in the DB at write time;
    // any later MUA promotion shows up through the cur/ scan as
    // either a NewMessage (post-DB-wipe rescue) or a FlagsChanged.
    // Files left in new/ by external MDAs likewise surface once the
    // MUA promotes them. The only reason new_entries is plumbed
    // through here is so seen_ids carries those ids back out, letting
    // exhaustive callers (scan_folder) detect deletions correctly
    // without spuriously firing DeletedMessage against
    // undelivered-but-pending messages.
    for id in new_entries {
        seen_ids.push(id.clone());
    }

    for id in explicit_deletes {
        debug!("Deleted message: {} (was in {})", id, folder_name);
        changes.push(LocalChange::DeletedMessage {
            maildir_id: id.clone(),
            folder: folder_name.to_string(),
        });
    }

    Ok((changes, seen_ids))
}

/// Scan a maildir folder and detect changes vs. the known state.
/// Walks the folder exhaustively via the maildir crate, then
/// delegates classification to `classify_changes`. Used by the
/// initial sync, post-disconnect catch-up, and the one-shot CLI
/// commands -- callers that need a full picture without relying on a
/// running fsevents stream.
///
/// `known_state` maps maildir_id -> (folder, flags) from the DB.
pub fn scan_folder(
    maildir: &Maildir,
    folder_name: &str,
    known_state: &HashMap<MaildirId, (String, String)>,
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
        .filter_map(|(id, (folder, _))| {
            if folder == folder_name && !observed.contains(id) {
                Some(id.clone())
            } else {
                None
            }
        })
        .collect();

    let (changes, seen_ids) = classify_changes(
        folder_name,
        &cur_entries,
        &new_entries,
        &explicit_deletes,
        known_state,
    )?;
    Ok(ScanResult {
        changes,
        local_flags,
        seen_ids,
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
/// `(maildir_id -> (folder, flags))` state. Paths whose folder isn't
/// present are dropped (untracked or stale-watcher noise).
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
    known_states: &HashMap<String, HashMap<MaildirId, (String, String)>>,
) -> Result<ScanResult> {
    // Group events by (folder, maildir_id) so the source + destination
    // sides of a single rename collapse into one classification.
    let mut groups: HashMap<(String, MaildirId), Vec<(String, String, PathBuf)>> = HashMap::new();
    for raw in event_paths {
        let Some((folder, subdir, id, flags)) = parse_event_path(maildir_root, raw) else {
            continue;
        };
        if !known_states.contains_key(&folder) {
            continue;
        }
        groups
            .entry((folder, id))
            .or_default()
            .push((subdir, flags, raw.clone()));
    }

    // Bucket into per-folder inputs for classify_changes.
    let mut by_folder: HashMap<String, (Vec<CurEntry>, Vec<MaildirId>, Vec<MaildirId>)> =
        HashMap::new();
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
                if let Some((known_folder, _)) = known_state.get(&maildir_id)
                    && known_folder == &folder
                {
                    bucket.2.push(maildir_id);
                }
            }
        }
    }

    let mut all_changes = Vec::new();
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
        let (changes, _seen) = classify_changes(&folder, &cur, &new, &deletes, known_state)?;
        all_changes.extend(changes);
    }

    Ok(ScanResult {
        changes: all_changes,
        local_flags,
        // Path-driven walks have no exhaustive observed set.
        seen_ids: Vec::new(),
    })
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
    use crate::maildir_ops::store::ensure_maildir;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn write_message(dir: &Path, sub: &str, filename: &str, body: &str) {
        let path = dir.join(sub).join(filename);
        fs::write(&path, body).unwrap();
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
        let result = scan_folder(&inbox, "INBOX", &known).unwrap();

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
        known.insert(unique.into(), ("INBOX".to_string(), "FS".to_string()));

        let ScanResult { changes, .. } = scan_folder(&spam, "Spam", &known).unwrap();

        assert_eq!(
            changes.len(),
            1,
            "destination scan must emit one change for an id-preserving move, got {:?}",
            changes
        );
        match &changes[0] {
            LocalChange::NewMessage {
                maildir_id,
                folder,
                message_id,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(folder, "Spam");
                assert_eq!(message_id.as_ref(), "a@x");
            }
            other => panic!("expected NewMessage on destination scan, got {:?}", other),
        }
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
        known.insert(unique.into(), ("INBOX".to_string(), "FS".to_string()));

        let ScanResult { changes, .. } = scan_folder(&inbox, "INBOX", &known).unwrap();

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::DeletedMessage { maildir_id, folder } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(folder, "INBOX");
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
        known.insert(unique.into(), ("INBOX".to_string(), "F".to_string()));

        let ScanResult { changes, .. } = scan_folder(&inbox, "INBOX", &known).unwrap();

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
        known.insert(old_id.into(), ("INBOX".to_string(), "FS".to_string()));

        let ScanResult {
            changes: inbox_changes,
            ..
        } = scan_folder(&inbox, "INBOX", &known).unwrap();
        let ScanResult {
            changes: spam_changes,
            ..
        } = scan_folder(&spam, "Spam", &known).unwrap();

        assert_eq!(
            inbox_changes.len(),
            1,
            "expected one DeletedMessage on INBOX"
        );
        match &inbox_changes[0] {
            LocalChange::DeletedMessage { maildir_id, folder } => {
                assert_eq!(maildir_id.as_ref(), old_id);
                assert_eq!(folder, "INBOX");
            }
            other => panic!("expected DeletedMessage, got {:?}", other),
        }

        assert_eq!(spam_changes.len(), 1, "expected one NewMessage on Spam");
        match &spam_changes[0] {
            LocalChange::NewMessage {
                maildir_id,
                folder,
                message_id,
                ..
            } => {
                assert_eq!(maildir_id.as_ref(), new_id);
                assert_eq!(folder, "Spam");
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
        let ScanResult {
            changes,
            seen_ids: seen,
            ..
        } = scan_folder(&inbox, "INBOX", &known).unwrap();

        assert!(
            changes.is_empty(),
            "unknown new/ file must not surface as a LocalChange, got {:?}",
            changes
        );
        let seen_strs: Vec<&str> = seen.iter().map(|m| m.as_ref()).collect();
        assert_eq!(seen_strs, vec![unique], "seen_ids contents");
    }

    /// Files sitting in new/ -- whether bare (`<unique>`) or
    /// suffix-bearing (`<unique>:2,<flags>`, the shape jma writes when
    /// delivering an unseen message with server-set flags) -- must NOT
    /// produce any LocalChange. They're tracked in the DB at delivery
    /// time and only become "real" changes once an MUA promotes them
    /// to cur/. Equally important, both shapes must register as seen
    /// so the deletion-detection loop doesn't fire DeletedMessage
    /// against an undelivered-but-pending file (which would cascade
    /// into a DestroyRemote and silently delete the message
    /// server-side).
    #[test]
    fn new_files_emit_no_change_but_count_as_seen() {
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
        // scan failed to count either as seen, the deletion loop would
        // emit DeletedMessage for the missing one.
        let mut known = HashMap::new();
        known.insert(bare.into(), ("INBOX".to_string(), "".to_string()));
        known.insert(
            suffixed_unique.into(),
            ("INBOX".to_string(), "F".to_string()),
        );

        let ScanResult {
            changes,
            seen_ids: seen,
            ..
        } = scan_folder(&inbox, "INBOX", &known).unwrap();

        assert!(
            changes.is_empty(),
            "new/ must not emit LocalChange events, got {:?}",
            changes
        );
        let seen_strs: Vec<&str> = seen.iter().map(|m| m.as_ref()).collect();
        assert!(
            seen_strs.contains(&bare),
            "bare new/ file must be in seen_ids, got {:?}",
            seen_strs
        );
        assert!(
            seen_strs.contains(&suffixed_unique),
            "suffixed new/ file must surface its canonical id (without :2,) in seen_ids, got {:?}",
            seen_strs
        );
    }

    /// A file with no Message-ID header (RFC 5322 says it SHOULD be
    /// present, but isn't a hard MUST) must NOT be ingested: jma
    /// anchors idempotency on Message-ID, and emitting NewMessage
    /// without one would either drop the message at reconcile or
    /// produce a server-side duplicate after a state DB wipe. Scan
    /// drops the file (logs error!) so it stays on disk untouched.
    #[test]
    fn scan_folder_skips_file_without_message_id() {
        let tmp = TempDir::new().unwrap();
        let inbox_path = tmp.path().join("INBOX");
        let inbox = ensure_maildir(&inbox_path).unwrap();

        // No Message-ID header — only a Subject + body.
        let unique = "1700000000.M1.host";
        let filename = format!("{unique}:2,");
        let body = "Subject: no msgid\r\n\r\nbody\r\n";
        write_message(&inbox_path, "cur", &filename, body);

        let known = HashMap::new();
        let ScanResult {
            changes,
            seen_ids: seen,
            ..
        } = scan_folder(&inbox, "INBOX", &known).unwrap();

        assert!(
            changes.is_empty(),
            "expected no LocalChanges for a file without Message-ID, got {:?}",
            changes
        );
        // The file is still seen on disk, so it counts as observed —
        // we just refuse to emit a NewMessage for it.
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].as_ref(), unique);
    }

    /// Same folder, file already known to the DB, but the file's
    /// Message-ID header is missing. This hits the
    /// `Some((_, known_flags))` branch where `require_message_id`
    /// isn't even called (flags match), so no `NewMessage` is emitted
    /// — but more importantly: the maildir_id IS pushed onto
    /// `seen_ids` before any matching, so the trailing
    /// deletion-detection loop must NOT spuriously emit a
    /// DeletedMessage for it. Pins the "seen_ids tracks every walked
    /// file regardless of whether we emit a change for it" invariant.
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
        known.insert(unique.into(), ("INBOX".to_string(), "FS".to_string()));

        let ScanResult { changes, .. } = scan_folder(&inbox, "INBOX", &known).unwrap();

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
        known.insert(unique.into(), ("INBOX".to_string(), "FS".to_string()));

        let ScanResult { changes, .. } = scan_folder(&spam, "Spam", &known).unwrap();

        assert!(
            changes.is_empty(),
            "expected no LocalChanges for a Message-ID-less cross-folder move, got {:?}",
            changes
        );
    }

    /// Build a single-folder `known_states` map for `scan_paths` tests.
    fn known_for(
        folder: &str,
        rows: &[(&str, &str, &str)],
    ) -> HashMap<String, HashMap<MaildirId, (String, String)>> {
        let mut inner = HashMap::new();
        for (id, db_folder, flags) in rows {
            inner.insert(
                MaildirId::from(*id),
                (db_folder.to_string(), flags.to_string()),
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

        let known = known_for("INBOX", &[(unique, "INBOX", "F")]);
        let event_paths = vec![
            inbox_path.join("cur").join(format!("{unique}:2,F")),
            inbox_path.join("cur").join(format!("{unique}:2,FS")),
        ];

        let changes = scan_paths(tmp.path(), &event_paths, &known)
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

        let known = known_for("INBOX", &[(unique, "INBOX", "S")]);
        let event_paths = vec![inbox_path.join("cur").join(format!("{unique}:2,S"))];

        let changes = scan_paths(tmp.path(), &event_paths, &known)
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
        let known = known_for("INBOX", &[(unique, "INBOX", "S")]);
        let event_paths = vec![inbox_path.join("cur").join(format!("{unique}:2,S"))];

        let changes = scan_paths(tmp.path(), &event_paths, &known)
            .unwrap()
            .changes;
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::DeletedMessage { maildir_id, folder } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(folder, "INBOX");
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
            ("INBOX".to_string(), "FS".to_string()),
        );
        known_states.insert("INBOX".to_string(), inner.clone());
        known_states.insert("Spam".to_string(), inner);

        let event_paths = vec![
            inbox_path.join("cur").join(format!("{unique}:2,FS")),
            spam_path.join("cur").join(format!("{unique}:2,FS")),
        ];

        let mut changes = scan_paths(tmp.path(), &event_paths, &known_states)
            .unwrap()
            .changes;
        // Order isn't guaranteed (HashMap iteration), so sort for the
        // assertion.
        changes.sort_by_key(|c| match c {
            LocalChange::DeletedMessage { folder, .. } => format!("0:{}", folder),
            LocalChange::NewMessage { folder, .. } => format!("1:{}", folder),
            LocalChange::FlagsChanged { folder, .. } => format!("2:{}", folder),
        });
        assert_eq!(changes.len(), 2);
        match &changes[0] {
            LocalChange::DeletedMessage { folder, .. } => assert_eq!(folder, "INBOX"),
            other => panic!("expected DeletedMessage on INBOX, got {:?}", other),
        }
        match &changes[1] {
            LocalChange::NewMessage { folder, .. } => assert_eq!(folder, "Spam"),
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
        let known = known_for("INBOX", &[(unique, "INBOX", "")]);
        let event_paths = vec![inbox_path.join("new").join(unique)];

        let changes = scan_paths(tmp.path(), &event_paths, &known)
            .unwrap()
            .changes;
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            LocalChange::DeletedMessage { maildir_id, folder } => {
                assert_eq!(maildir_id.as_ref(), unique);
                assert_eq!(folder, "INBOX");
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
        let known = known_for("INBOX", &[(unique, "INBOX", "")]);
        let event_paths = vec![inbox_path.join("new").join(unique)];

        let changes = scan_paths(tmp.path(), &event_paths, &known)
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

        let known = known_for("INBOX", &[(unique, "INBOX", "")]);
        let event_paths = vec![
            inbox_path.join("new").join(unique),
            inbox_path.join("cur").join(format!("{unique}:2,S")),
        ];

        let changes = scan_paths(tmp.path(), &event_paths, &known)
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

        let known = known_for("INBOX", &[(unique, "INBOX", "")]);
        let event_paths = vec![
            // No /cur/ or /new/ segment.
            tmp.path().join(".jma.db"),
            // Outside maildir root.
            PathBuf::from("/tmp/unrelated"),
            // Real one.
            inbox_path.join("cur").join(format!("{unique}:2,S")),
        ];

        let changes = scan_paths(tmp.path(), &event_paths, &known)
            .unwrap()
            .changes;
        assert_eq!(
            changes.len(),
            1,
            "unparseable paths must be skipped without aborting, got {:?}",
            changes
        );
    }

    /// Path in a folder that isn't synced: dropped silently. The
    /// daemon only feeds known_states for synced folders, and we
    /// shouldn't fabricate changes for paths under untracked folders.
    #[test]
    fn scan_paths_drops_unknown_folder() {
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

        let changes = scan_paths(tmp.path(), &event_paths, &known)
            .unwrap()
            .changes;
        assert!(
            changes.is_empty(),
            "events under untracked folder must be dropped, got {:?}",
            changes
        );
    }
}
