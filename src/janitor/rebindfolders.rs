//! Rebindfolders janitorial task: rebind sentinel-less maildir
//! folders to JMAP mailboxes by Message-ID probing.
//!
//! Every actively-synced folder carries a `.jma.mapping` sentinel
//! naming its bound JMAP mailbox; `resolve_mailboxes` refreshes it
//! every cycle. That natural backfill is enough for steady-state
//! operation, but it cannot rebind a folder whose sentinel was lost
//! OR whose state DB was nuked while the folder is no longer in
//! `mailbox_map` either. Any consumer that classifies such an
//! orphan as a fresh local creation would then push a duplicate
//! server mailbox; this task is the disambiguator that prevents
//! that misclassification by identifying which existing JMAP
//! mailbox an orphan folder corresponds to. It samples Message-ID
//! headers from the orphan's `cur/`/`new/` and asks the server
//! which mailbox those messages live in.
//!
//! Algorithm:
//!
//! 1. Fetch the server's mailbox list once (`Mailbox/get`) and index
//!    it by id. Anything outside this index is unrebindable.
//! 2. Walk the maildir tree for folders carrying a `cur/`
//!    subdirectory. Drop folders whose sentinel already names a
//!    known mailbox, and folders already bound by `mailbox_map` (the
//!    next sync cycle's sentinel-write will backfill those).
//! 3. For each remaining orphan, sample up to `sample_size`
//!    `Message-ID` headers from `cur/` and `new/`.
//! 4. One `Email/query` per folder with `Filter::or` of N
//!    `header("Message-ID", Some(id))` conditions, where `id` is
//!    jma's bracket-stripped internal form. Per RFC 8621 4.4.1
//!    this is a substring match against the server's stored
//!    header value; the stripped form is the JMAP-side convention
//!    (RFC 5322's `Message-ID` is conventionally rendered as the
//!    inner content with no brackets, and the JMAP `messageId`
//!    property exposes that form). Fastmail returns the expected
//!    match for both stripped and bracketed values, verified
//!    against a live account by a one-off probe during
//!    development. The same shape works against Stalwart when
//!    its `Search.indexEmailFields` config has `headers` enabled
//!    -- off by default per Stalwart Discussion #2788, baked on
//!    for the test fixture and exercised end-to-end by
//!    `tests/e2e_rebindfolders.rs`.
//! 5. `Email/get` on the resulting ids for `mailboxIds`. Bin each
//!    returned email by which sample it EXACTLY matched (using the
//!    returned `messageId` array's bracket-stripped form), take the
//!    per-sample union of `mailboxIds`, then the intersection across
//!    samples. The exact-match bin closes the false-positive prefix
//!    risk inherent to substring matching: an email returned because
//!    "abc@x.com" was a substring of its `<xabc@x.com>` header would
//!    be dropped here because no sample equals `xabc@x.com`. The
//!    per-sample union covers the draft-and-sent-copy-share-Message-
//!    ID case; the cross-sample intersection narrows to the mailbox
//!    every sample agrees on.
//! 6. Intersection of size 1: rebind candidate. Empty or larger:
//!    skip with the union in the report.
//!
//! Sampling is "first N parseable" rather than random. A folder
//! whose first N messages are drafts could falsely intersect to the
//! Drafts mailbox; randomization would mitigate, but the
//! deterministic ordering keeps the algorithm reproducible across
//! invocations and the operator review step (default report-only)
//! catches the misclassification before any sentinel lands.

use anyhow::{Context, Result};
use jmap_client::client::Client;
use jmap_client::core::query::Filter as CoreFilter;
use jmap_client::email::query::Filter as EmailFilter;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tracing::{debug, info};

use crate::ids::{JmapEmailId, JmapMailboxId, MessageId};
use crate::jmap::limits;
use crate::jmap::retry::with_retry;
use crate::jmap::types::{EmailObject, MailboxObject};
use crate::jmap::{email as jmap_email, mailbox as jmap_mailbox};
use crate::maildir_ops::headers::parse_message_id_from_file;
use crate::maildir_ops::namespace::is_jma_private;
use crate::maildir_ops::sentinel::{self, MailboxMapping};
use crate::state::queries;

/// Default Message-ID sample size per orphan folder. Ten samples is
/// well above the threshold where the intersection algorithm
/// reliably narrows to a single mailbox in normal traffic, and well
/// below any server's `Email/query` result cap.
pub const DEFAULT_SAMPLE_SIZE: usize = 10;

/// One folder the planner decided to rebind. Carries everything
/// `apply` needs to write the sentinel without re-querying.
#[derive(Debug, Clone)]
pub struct RebindCandidate {
    pub folder_path: PathBuf,
    pub jmap_mailbox_id: JmapMailboxId,
    pub server_name: String,
    pub parent_jmap_mailbox_id: Option<JmapMailboxId>,
    pub sample_count: usize,
}

/// One folder the planner refused to rebind, paired with the reason
/// for the operator-facing report.
#[derive(Debug, Clone)]
pub struct SkippedFolder {
    pub folder_path: PathBuf,
    pub reason: SkipReason,
}

#[derive(Debug, Clone)]
pub enum SkipReason {
    /// No parseable Message-ID headers in `cur/` or `new/`. An
    /// empty folder or one that only carries jma-foreign files
    /// without RFC 5322 headers.
    NoMessageIds,
    /// All sampled Message-IDs were unknown to the server. Could
    /// be a server-deleted folder still living on disk; could be
    /// a folder whose mail was all sent and removed; could be a
    /// folder from a different account that was copied here.
    NoServerMatches,
    /// The cross-sample intersection has zero or more than one
    /// mailbox, so the planner cannot pick a single rebind
    /// target. The vector lists candidate mailboxes in `as_ref()`
    /// order so the operator-facing report is stable across
    /// invocations. When the intersection had more than one
    /// member, the vector IS the intersection -- mailboxes every
    /// sample agreed the folder might be. When the intersection
    /// was empty, the vector is the union of every matched
    /// email's mailboxIds -- the broader pool the operator might
    /// scan for the right answer.
    AmbiguousMailboxes(Vec<JmapMailboxId>),
}

#[derive(Debug, Default)]
pub struct RebindFoldersPlan {
    pub candidates: Vec<RebindCandidate>,
    pub skipped: Vec<SkippedFolder>,
}

/// Build a rebind plan against the live server. Pure plan; nothing
/// is written until `apply`. Caller is responsible for holding the
/// maildir + state DB locks; the function only reads from disk.
pub async fn plan(
    client: &Client,
    conn: &Connection,
    maildir_root: &Path,
    sample_size: usize,
) -> Result<RebindFoldersPlan> {
    let _phase =
        tracing::info_span!(target: crate::profile::TARGET_PHASE, "rebindfolders").entered();

    let server_mailboxes = jmap_mailbox::get_all(client).await?;
    let by_id: HashMap<JmapMailboxId, &MailboxObject> = server_mailboxes
        .iter()
        .map(|mb| (mb.id.clone(), mb))
        .collect();

    let bound_folders: HashSet<String> = queries::list_known_maildir_folders(conn)?
        .into_iter()
        .collect();

    let orphans = walk_for_orphans(maildir_root, &by_id, &bound_folders);
    info!("rebindfolders: found {} candidate folder(s)", orphans.len());

    let mut plan = RebindFoldersPlan::default();
    let get_cap = limits::max_objects_in_get(client);
    for folder_path in orphans {
        match probe_folder(client, &folder_path, sample_size, get_cap, &by_id).await? {
            ProbeOutcome::Bind {
                jmap_mailbox_id,
                sample_count,
            } => {
                let mb = by_id
                    .get(&jmap_mailbox_id)
                    .with_context(|| format!("intersection picked unknown id {jmap_mailbox_id}"))?;
                plan.candidates.push(RebindCandidate {
                    folder_path,
                    jmap_mailbox_id: mb.id.clone(),
                    server_name: mb.name.clone(),
                    parent_jmap_mailbox_id: mb.parent_id.clone(),
                    sample_count,
                });
            }
            ProbeOutcome::Skip(reason) => {
                plan.skipped.push(SkippedFolder {
                    folder_path,
                    reason,
                });
            }
        }
    }
    Ok(plan)
}

/// Write the sentinel for each rebind candidate in `plan`. Returns
/// the count of sentinels written. Skipped folders are not touched
/// (their report is purely informational).
pub fn apply(plan: &RebindFoldersPlan) -> Result<usize> {
    for c in &plan.candidates {
        sentinel::write(
            &c.folder_path,
            &MailboxMapping {
                jmap_mailbox_id: c.jmap_mailbox_id.clone(),
                parent_jmap_mailbox_id: c.parent_jmap_mailbox_id.clone(),
                server_name: c.server_name.clone(),
            },
        )?;
        info!(
            "rebindfolders: bound {} to {} ({})",
            c.folder_path.display(),
            c.jmap_mailbox_id,
            c.server_name
        );
    }
    Ok(plan.candidates.len())
}

/// One-shot helper for the CLI: build the plan, optionally apply,
/// return the plan so the caller can render it.
pub async fn run(
    client: &Client,
    conn: &Connection,
    maildir_root: &Path,
    sample_size: usize,
    dry_run: bool,
) -> Result<RebindFoldersPlan> {
    let p = plan(client, conn, maildir_root, sample_size).await?;
    if !dry_run {
        apply(&p)?;
    }
    Ok(p)
}

/// What `probe_folder` decided about one orphan folder.
enum ProbeOutcome {
    Bind {
        jmap_mailbox_id: JmapMailboxId,
        sample_count: usize,
    },
    Skip(SkipReason),
}

/// Probe a single orphan: sample its Message-IDs, query the server,
/// intersect mailbox memberships, return the verdict.
async fn probe_folder(
    client: &Client,
    folder_path: &Path,
    sample_size: usize,
    get_cap: usize,
    by_id: &HashMap<JmapMailboxId, &MailboxObject>,
) -> Result<ProbeOutcome> {
    let samples = sample_message_ids(folder_path, sample_size)?;
    if samples.is_empty() {
        return Ok(ProbeOutcome::Skip(SkipReason::NoMessageIds));
    }

    let candidate_ids = query_by_message_ids(client, &samples).await?;
    if candidate_ids.is_empty() {
        return Ok(ProbeOutcome::Skip(SkipReason::NoServerMatches));
    }

    // Email/get may return more ids than `maxObjectsInGet`; chunk so
    // any one request stays under the server-advertised cap.
    let mut emails: Vec<EmailObject> = Vec::with_capacity(candidate_ids.len());
    for chunk in candidate_ids.chunks(get_cap.max(1)) {
        let batch = jmap_email::get_by_ids(client, chunk).await?;
        emails.extend(batch);
    }

    let intersection = intersect_mailboxes_by_sample(&emails, &samples);

    // Drop intersection members that aren't on the server (could
    // happen if Mailbox/get and Email/get observed different
    // snapshots; better to fail closed than silently bind to a
    // mailbox we can't render).
    let mut known: Vec<JmapMailboxId> = intersection
        .into_iter()
        .filter(|id| by_id.contains_key(id))
        .collect();
    known.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));

    match known.len() {
        0 => Ok(ProbeOutcome::Skip(SkipReason::AmbiguousMailboxes(
            mailbox_union(&emails),
        ))),
        1 => {
            let id = known.into_iter().next().expect("len == 1 matched above");
            debug!(
                "rebindfolders: {} -> {} (from {} sample(s))",
                folder_path.display(),
                id,
                samples.len()
            );
            Ok(ProbeOutcome::Bind {
                jmap_mailbox_id: id,
                sample_count: samples.len(),
            })
        }
        _ => Ok(ProbeOutcome::Skip(SkipReason::AmbiguousMailboxes(known))),
    }
}

/// Per-sample union (so a Message-ID matching both a draft and a
/// sent copy doesn't poison its sample's vote), then intersect
/// across samples that produced any match. Samples with no match
/// contribute nothing -- a missing sample carries no information
/// about which mailbox the folder binds to.
fn intersect_mailboxes_by_sample(
    emails: &[EmailObject],
    samples: &[MessageId],
) -> HashSet<JmapMailboxId> {
    let sample_set: HashSet<&MessageId> = samples.iter().collect();
    let mut by_sample: HashMap<&MessageId, Vec<&EmailObject>> = HashMap::new();
    for email in emails {
        let Some(message_ids) = &email.message_id else {
            continue;
        };
        for mid in message_ids {
            if let Some(s) = sample_set.get(mid) {
                by_sample.entry(*s).or_default().push(email);
            }
        }
    }
    let mut intersection: Option<HashSet<JmapMailboxId>> = None;
    for matched in by_sample.values() {
        let union: HashSet<JmapMailboxId> = matched
            .iter()
            .flat_map(|e| e.mailbox_ids.keys().cloned())
            .collect();
        intersection = Some(match intersection {
            None => union,
            Some(prev) => prev.intersection(&union).cloned().collect(),
        });
    }
    intersection.unwrap_or_default()
}

/// Union of every mailbox any candidate email lives in. Used to
/// populate the `AmbiguousMailboxes` skip report so the operator
/// can see what the algorithm could not narrow down. Sorted by
/// `as_ref()` for stable rendering across invocations.
fn mailbox_union(emails: &[EmailObject]) -> Vec<JmapMailboxId> {
    let mut set: HashSet<JmapMailboxId> = emails
        .iter()
        .flat_map(|e| e.mailbox_ids.keys().cloned())
        .collect();
    let mut out: Vec<JmapMailboxId> = set.drain().collect();
    out.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    out
}

/// Sample up to `sample_size` parseable Message-IDs from a maildir
/// folder's `cur/` and `new/`. Returns the deduplicated list (one
/// entry per Message-ID even if multiple files share it). First-N
/// is deterministic; see the module doc for the trade-off vs random
/// sampling.
fn sample_message_ids(folder_path: &Path, sample_size: usize) -> Result<Vec<MessageId>> {
    let mut seen: HashSet<MessageId> = HashSet::new();
    let mut out: Vec<MessageId> = Vec::new();
    for sub in ["cur", "new"] {
        let subdir = folder_path.join(sub);
        let Ok(entries) = std::fs::read_dir(&subdir) else {
            continue;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                continue;
            }
            let path = entry.path();
            match parse_message_id_from_file(&path) {
                Ok(Some(mid)) => {
                    if seen.insert(mid.clone()) {
                        out.push(mid);
                        if out.len() >= sample_size {
                            return Ok(out);
                        }
                    }
                }
                Ok(None) => continue,
                Err(e) => {
                    debug!("rebindfolders: skipping {} ({e})", path.display());
                    continue;
                }
            }
        }
    }
    Ok(out)
}

/// One `Email/query` with `Filter::or` over the sampled Message-IDs
/// in their bracket-stripped internal form. Per RFC 8621 4.4.1
/// this is a substring match against the server's stored header
/// value; the stripped form is the JMAP-side convention. The
/// substring false-positive risk (server matches `abc@x.com` as a
/// prefix of `xabc@x.com`) is closed downstream by
/// `intersect_mailboxes_by_sample`, which only keeps emails whose
/// `messageId` array exactly contains a sample.
///
/// Stripped rather than bracketed (`<id>`) is empirically required
/// for cross-server compatibility: Fastmail accepts both, but
/// Stalwart's `header` filter returns zero hits for the bracketed
/// form even with header-FTS enabled (verified live by
/// `tests/e2e_rebindfolders.rs` and an offline Fastmail probe
/// during development). The bracketed form would otherwise be the
/// stronger choice because the `<` / `>` delimiters turn the
/// substring match into an exact match without needing the
/// downstream intersection guard; the downstream guard does the
/// same work and works against both servers.
///
/// Servers must have header indexing enabled for this to return
/// any hits at all -- it is off by default on Stalwart and has
/// to be flipped via the `Search.indexEmailFields` config (cf.
/// the test fixture; exercised end-to-end by
/// `tests/e2e_rebindfolders.rs`). Fastmail honors the filter
/// without any extra configuration.
///
/// No defensive clamp on the response size. The OR carries at most
/// `sample_size` conditions (default 10), and each Message-ID
/// matches at most a small number of server-side emails (drafts +
/// sent copies + maybe a Receipts label), so the result set is
/// bounded by the cardinality of the samples themselves, not by
/// the folder's total size. Every plausible server's
/// `maxQueryResults` cap easily absorbs that.
async fn query_by_message_ids(client: &Client, samples: &[MessageId]) -> Result<Vec<JmapEmailId>> {
    let conditions: Vec<EmailFilter> = samples
        .iter()
        .map(|m| EmailFilter::header("Message-ID", Some(m.as_ref())))
        .collect();
    let filter: CoreFilter<EmailFilter> = CoreFilter::or(conditions);

    let ids = with_retry("Email/query (rebindfolders)", || async {
        let mut request = client.build();
        let q = request
            .query_email()
            .account_id(client.default_account_id());
        q.filter(filter.clone());

        let response = request
            .send()
            .await
            .context("Failed to issue Email/query")?;
        let parsed = response
            .unwrap_method_responses()
            .pop()
            .context("No response for Email/query")?
            .unwrap_query_email()
            .context("Failed to parse Email/query response")?;
        let ids: Vec<JmapEmailId> = parsed
            .ids()
            .iter()
            .map(|s| JmapEmailId::from(s.as_str()))
            .collect();
        Ok(ids)
    })
    .await?;
    Ok(ids)
}

/// Walk `maildir_root` for folders with `cur/` whose binding state
/// looks orphaned: sentinel either missing or naming an id that
/// isn't on the server, AND the on-disk path isn't already known by
/// `mailbox_map` (those will get a fresh sentinel from the next
/// `resolve_mailboxes` cycle, no probe needed).
///
/// `maildir_root` itself is never returned even if it contains a
/// `cur/` subdirectory directly -- jma's layout convention puts
/// the root as a container for per-mailbox folders, not as a
/// maildir itself, and a flat-at-root shape would also confuse
/// the existing drift report.
///
/// Returns the absolute folder paths, sorted, so the output is
/// stable across runs.
fn walk_for_orphans(
    maildir_root: &Path,
    by_id: &HashMap<JmapMailboxId, &MailboxObject>,
    bound_folders: &HashSet<String>,
) -> Vec<PathBuf> {
    let mut found = Vec::new();
    walk_for_maildirs(maildir_root, &mut found);
    found.sort();

    found
        .into_iter()
        .filter(|abs_path| {
            // Skip folders already bound via mailbox_map. The
            // relative path is what `mailbox_map.maildir_folder`
            // stores.
            if let Ok(rel) = abs_path.strip_prefix(maildir_root) {
                let rel_str = rel.to_string_lossy();
                if bound_folders.contains(rel_str.as_ref()) {
                    return false;
                }
            }
            // Skip folders whose sentinel already names a known
            // server mailbox. A sentinel pointing at an id the
            // server doesn't advertise is orphaned (deleted
            // server-side or copied from another account), so
            // include those.
            match sentinel::read(abs_path) {
                Ok(Some(m)) => !by_id.contains_key(&m.jmap_mailbox_id),
                Ok(None) => true,
                Err(e) => {
                    debug!(
                        "rebindfolders: sentinel read failed at {} ({e}); treating as orphan",
                        abs_path.display()
                    );
                    true
                }
            }
        })
        .collect()
}

/// Recursive maildir-tree walk. Mirrors the helper used by the
/// `status` drift report (`main.rs::find_maildir_folders`); kept
/// local because the two callers want subtly different return
/// shapes (drift wants relative strings for set differencing;
/// rebindfolders wants absolute paths for `sentinel::read`).
fn walk_for_maildirs(dir: &Path, found: &mut Vec<PathBuf>) {
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
        walk_for_maildirs(&path, found);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{JmapBlobId, JmapThreadId};
    use std::collections::HashMap as StdHashMap;

    fn mb(id: &str, name: &str, parent: Option<&str>) -> MailboxObject {
        MailboxObject {
            id: JmapMailboxId::from(id),
            name: name.to_string(),
            parent_id: parent.map(JmapMailboxId::from),
            role: None,
            sort_order: 0,
            total_emails: 0,
            unread_emails: 0,
        }
    }

    fn email(id: &str, message_id: &str, mailbox_ids: &[&str]) -> EmailObject {
        let mut mbs = StdHashMap::new();
        for m in mailbox_ids {
            mbs.insert(JmapMailboxId::from(*m), true);
        }
        EmailObject {
            id: JmapEmailId::from(id),
            blob_id: JmapBlobId::from("blob"),
            thread_id: JmapThreadId::from("thread"),
            mailbox_ids: mbs,
            keywords: StdHashMap::new(),
            message_id: Some(vec![MessageId::from(message_id)]),
            subject: None,
            size: 0,
        }
    }

    fn seed_msg(folder: &Path, sub: &str, name: &str, message_id: &str) {
        let dir = folder.join(sub);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(name),
            format!("Message-ID: <{message_id}>\r\nSubject: t\r\n\r\nbody"),
        )
        .unwrap();
    }

    /// Three samples, all matching emails that live in MB-INBOX
    /// alone. Intersection is `{MB-INBOX}`, the algorithm picks
    /// that as the rebind target.
    #[test]
    fn intersect_picks_the_common_mailbox() {
        let samples = vec![
            MessageId::from("a@x"),
            MessageId::from("b@x"),
            MessageId::from("c@x"),
        ];
        let emails = vec![
            email("E1", "a@x", &["MB-INBOX"]),
            email("E2", "b@x", &["MB-INBOX"]),
            email("E3", "c@x", &["MB-INBOX"]),
        ];
        let got = intersect_mailboxes_by_sample(&emails, &samples);
        assert_eq!(got, [JmapMailboxId::from("MB-INBOX")].into());
    }

    /// One Message-ID matches BOTH a draft (in Drafts) and a sent
    /// copy (in Sent); other samples are Sent-only. Per-sample
    /// union keeps the draft case from poisoning the intersection;
    /// the cross-sample intersect narrows to Sent.
    #[test]
    fn intersect_tolerates_draft_and_sent_pair_per_sample() {
        let samples = vec![MessageId::from("a@x"), MessageId::from("b@x")];
        let emails = vec![
            // Sample a@x lives in both Drafts and Sent (two emails
            // share the Message-ID).
            email("E1", "a@x", &["MB-DRAFTS"]),
            email("E2", "a@x", &["MB-SENT"]),
            // Sample b@x lives in Sent only.
            email("E3", "b@x", &["MB-SENT"]),
        ];
        let got = intersect_mailboxes_by_sample(&emails, &samples);
        assert_eq!(got, [JmapMailboxId::from("MB-SENT")].into());
    }

    /// Samples agree on no single mailbox: sample a@x is in Inbox,
    /// sample b@x is in Archive. Intersection is empty; the caller
    /// reports ambiguity.
    #[test]
    fn intersect_returns_empty_for_disagreeing_samples() {
        let samples = vec![MessageId::from("a@x"), MessageId::from("b@x")];
        let emails = vec![
            email("E1", "a@x", &["MB-INBOX"]),
            email("E2", "b@x", &["MB-ARCHIVE"]),
        ];
        let got = intersect_mailboxes_by_sample(&emails, &samples);
        assert!(got.is_empty(), "expected empty intersection, got: {got:?}");
    }

    /// A sample with no server match contributes nothing; the
    /// remaining samples still produce a verdict. Mirrors the
    /// "user wrote a local draft we never sent" case where some
    /// Message-IDs aren't known to the server at all.
    #[test]
    fn intersect_ignores_unmatched_samples() {
        let samples = vec![
            MessageId::from("a@x"),  // matched
            MessageId::from("zz@y"), // unmatched
        ];
        let emails = vec![email("E1", "a@x", &["MB-INBOX"])];
        let got = intersect_mailboxes_by_sample(&emails, &samples);
        assert_eq!(got, [JmapMailboxId::from("MB-INBOX")].into());
    }

    /// An email whose server-side `messageId` is `None` (the
    /// server returned no Message-ID header) contributes nothing
    /// to the intersection -- the binning step matches samples
    /// against the email's message_id list, and an absent list
    /// can't match any sample. Pins the defensive read so a
    /// future "always-Some" refactor that drops the guard gets
    /// caught.
    #[test]
    fn intersect_skips_emails_without_message_id() {
        let samples = vec![MessageId::from("a@x")];
        let mut headerless = email("E1", "a@x", &["MB-INBOX"]);
        headerless.message_id = None;
        let emails = vec![headerless];
        let got = intersect_mailboxes_by_sample(&emails, &samples);
        assert!(got.is_empty(), "expected empty when no samples match");
    }

    /// All emails happen to live in two mailboxes (e.g. Inbox +
    /// All Mail label). Intersection has two members; caller
    /// reports ambiguity rather than guessing.
    #[test]
    fn intersect_returns_multi_element_for_dual_membership() {
        let samples = vec![MessageId::from("a@x")];
        let emails = vec![email("E1", "a@x", &["MB-INBOX", "MB-ALL"])];
        let got = intersect_mailboxes_by_sample(&emails, &samples);
        assert_eq!(got.len(), 2);
        assert!(got.contains(&JmapMailboxId::from("MB-INBOX")));
        assert!(got.contains(&JmapMailboxId::from("MB-ALL")));
    }

    /// Sampling stops at `sample_size` even when more files are
    /// available, and de-duplicates Message-IDs across files
    /// (multiple maildir entries sharing a Message-ID still count
    /// as one sample).
    #[test]
    fn sample_caps_and_dedupes() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("INBOX");
        std::fs::create_dir_all(folder.join("cur")).unwrap();
        std::fs::create_dir_all(folder.join("new")).unwrap();
        seed_msg(&folder, "cur", "1.x:2,", "a@x");
        seed_msg(&folder, "cur", "2.x:2,", "b@x");
        seed_msg(&folder, "cur", "3.x:2,", "a@x"); // duplicate Message-ID
        seed_msg(&folder, "cur", "4.x:2,", "c@x");
        seed_msg(&folder, "new", "5.x:2,", "d@x");

        let three = sample_message_ids(&folder, 3).unwrap();
        assert_eq!(three.len(), 3);
        let unique: HashSet<_> = three.iter().cloned().collect();
        assert_eq!(unique.len(), 3, "sampling must dedupe Message-IDs");

        let big = sample_message_ids(&folder, 100).unwrap();
        // Four unique IDs across the five files.
        assert_eq!(big.len(), 4);
    }

    /// `cur/` and `new/` files without parseable Message-IDs are
    /// skipped; the result is empty rather than synthesized.
    #[test]
    fn sample_returns_empty_for_headerless_files() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("INBOX");
        std::fs::create_dir_all(folder.join("cur")).unwrap();
        std::fs::write(folder.join("cur").join("1.x:2,"), b"no headers here").unwrap();
        let got = sample_message_ids(&folder, 10).unwrap();
        assert!(got.is_empty());
    }

    /// `walk_for_orphans` filters out folders already bound by
    /// `mailbox_map` (sentinel-write will backfill on the next
    /// cycle) and folders whose sentinel names a known mailbox.
    /// Only the unbound, sentinel-less folder is returned.
    #[test]
    fn walk_filters_bound_and_sentinel_present() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        // Three folders on disk:
        //  - INBOX: bound via mailbox_map (no sentinel needed; will
        //    be backfilled).
        //  - Archive: has a sentinel pointing at a known mailbox.
        //  - Lost: no sentinel, no mailbox_map row -- the orphan.
        for f in ["INBOX", "Archive", "Lost"] {
            std::fs::create_dir_all(root.join(f).join("cur")).unwrap();
        }
        sentinel::write(
            &root.join("Archive"),
            &MailboxMapping {
                jmap_mailbox_id: JmapMailboxId::from("MB-ARCH"),
                parent_jmap_mailbox_id: None,
                server_name: "Archive".to_string(),
            },
        )
        .unwrap();

        let server = [
            mb("MB-INBOX", "Inbox", None),
            mb("MB-ARCH", "Archive", None),
        ];
        let by_id: HashMap<JmapMailboxId, &MailboxObject> =
            server.iter().map(|m| (m.id.clone(), m)).collect();
        let bound: HashSet<String> = ["INBOX".to_string()].into();

        let orphans = walk_for_orphans(root, &by_id, &bound);
        assert_eq!(orphans, vec![root.join("Lost")]);
    }

    /// A sentinel naming a mailbox the server no longer advertises
    /// (deleted server-side, or copied from a foreign account) is
    /// still treated as an orphan -- the probe gets a chance to
    /// rebind it to whatever the messages actually map to.
    #[test]
    fn walk_treats_unknown_sentinel_id_as_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("Stranger").join("cur")).unwrap();
        sentinel::write(
            &root.join("Stranger"),
            &MailboxMapping {
                jmap_mailbox_id: JmapMailboxId::from("MB-GONE"),
                parent_jmap_mailbox_id: None,
                server_name: "Stranger".to_string(),
            },
        )
        .unwrap();

        let server = [mb("MB-INBOX", "Inbox", None)];
        let by_id: HashMap<JmapMailboxId, &MailboxObject> =
            server.iter().map(|m| (m.id.clone(), m)).collect();

        let orphans = walk_for_orphans(root, &by_id, &HashSet::new());
        assert_eq!(orphans, vec![root.join("Stranger")]);
    }

    /// `apply` writes the sentinel for every candidate but leaves
    /// `skipped` folders untouched. Returns the count of written
    /// sentinels for the operator-facing summary.
    #[test]
    fn apply_writes_candidates_and_returns_count() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("Lost");
        std::fs::create_dir_all(target.join("cur")).unwrap();
        let plan = RebindFoldersPlan {
            candidates: vec![RebindCandidate {
                folder_path: target.clone(),
                jmap_mailbox_id: JmapMailboxId::from("MB-LOST"),
                server_name: "Lost".to_string(),
                parent_jmap_mailbox_id: None,
                sample_count: 3,
            }],
            skipped: vec![],
        };
        let n = apply(&plan).unwrap();
        assert_eq!(n, 1);
        let got = sentinel::read(&target).unwrap().expect("sentinel written");
        assert_eq!(got.jmap_mailbox_id, JmapMailboxId::from("MB-LOST"));
        assert_eq!(got.server_name, "Lost");
    }
}
