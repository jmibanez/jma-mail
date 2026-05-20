//! Remote dedupe janitorial task: scan per-mailbox on the server,
//! identify groups of Email objects sharing a `messageId` header
//! within the same mailbox, and destroy the duplicates via `Email/
//! set`. Counterpart to `janitor::dedupe`, which removes local-file
//! duplicates by mtime.
//!
//! The duplicate signal lives on the server, not in the state DB:
//! per the partial-unique index on `message_map(maildir_id)`,
//! reconcile refuses to bind a second JMAP id to a maildir_id
//! already in use, so a second remote duplicate never lands as a
//! row. (Reconcile warns and points the user here.) Discovery
//! therefore walks the wire: `Email/query` per mailbox, then
//! `Email/get` for `messageId` and `blobId`.
//!
//! Safety against header forgery: same Message-ID is a header
//! claim, and an attacker (or a buggy migration tool) can stamp any
//! Message-ID onto any RFC822 payload. JMAP's `blobId` is not a
//! portable content signal -- RFC 8620 leaves blob ids server-
//! defined, and at least one popular server (Stalwart) issues a
//! fresh blob id per Email even when bytes match. The planner
//! therefore uses a two-tier content-equality check:
//!
//! 1. **Cheap pre-check on `size`** (`RFC 8621` §4.1.1, mandatory).
//!    Different sizes prove different bytes; the group is refused
//!    with `SkipReason::SizeMismatch` without any download.
//! 2. **Expensive byte equality** via `download_blob` on every
//!    member of any size-passing group. If any pair of payloads
//!    disagrees, the group is refused with `SkipReason::
//!    ContentMismatch`. Only groups whose every member shares
//!    exact bytes survive to the destroy phase.
//!
//! The download is paid once per duplicate group, only after the
//! size check has already pruned the obvious mismatches. Janitor
//! is one-shot so the per-cycle bandwidth is bounded by the
//! account's duplicate count, not its total message count.
//!
//! Survivor rule: prefer the member whose `jmap_email_id` already
//! has a `message_map` row with a non-NULL `maildir_id` (that's the
//! id our local maildir file is currently bound to -- destroying
//! it would orphan the file). If no group member is locally bound,
//! fall back to the lexicographically smallest `jmap_email_id` for
//! a deterministic choice. Since the byte-equality check enforces
//! byte-identical content, survivor selection within a group is
//! cosmetic.

use anyhow::{Context, Result};
use futures_util::StreamExt;
use jmap_client::client::Client;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info};

use crate::ids::{JmapBlobId, JmapEmailId, JmapMailboxId, MessageId};
use crate::jmap::email::{self as jmap_email, EmailSetOp, set_email_batch};
use crate::jmap::limits;

/// One destroy-candidate group: members all share mailbox, Message-
/// ID header, size, and byte-for-byte content. `blob_id` records
/// the survivor's blob for the operator's render; equality between
/// members was already verified via download.
#[derive(Debug, Clone)]
pub struct RemoteDedupeGroup {
    pub mailbox_folder: String,
    pub mailbox_id: JmapMailboxId,
    pub message_id: MessageId,
    pub blob_id: JmapBlobId,
    pub survivor: JmapEmailId,
    pub destroy: Vec<JmapEmailId>,
}

/// A group the planner refused to act on. See `SkipReason` for the
/// individual rules; the operator-facing render names each via
/// `Debug`.
#[derive(Debug, Clone)]
pub struct RemoteDedupeSkippedGroup {
    pub mailbox_folder: String,
    pub message_id: MessageId,
    pub reason: SkipReason,
    pub members: Vec<(JmapEmailId, JmapBlobId)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// Members carry the same `Message-ID` header but the server
    /// reports different `size` values. Different sizes prove
    /// different bytes, so the group fails the cheap pre-check
    /// before any blob download.
    SizeMismatch,
    /// Members share `Message-ID` and `size` but at least one pair
    /// of downloaded blobs differs byte-for-byte. The cheap pre-
    /// check passed; the expensive byte-equality check did not.
    /// Same-size, different-bytes is the deliberate-forgery shape;
    /// refusing the group preserves whichever copy the operator
    /// intended to keep.
    ContentMismatch,
}

#[derive(Debug, Default)]
pub struct RemoteDedupePlan {
    pub groups: Vec<RemoteDedupeGroup>,
    pub skipped: Vec<RemoteDedupeSkippedGroup>,
}

impl RemoteDedupePlan {
    /// Total number of JMAP ids the apply step would destroy.
    pub fn destroy_count(&self) -> usize {
        self.groups.iter().map(|g| g.destroy.len()).sum()
    }
}

/// Server-side outcome of the destroy phase. Mirrors
/// `EmailSetOutcome` but scoped to destroys so the caller doesn't
/// need to know about the keyword/move variants of `EmailSetOp`.
#[derive(Debug, Default)]
pub struct RemoteDedupeOutcome {
    pub attempted: usize,
    pub failed: HashSet<JmapEmailId>,
}

impl RemoteDedupeOutcome {
    pub fn succeeded(&self) -> usize {
        // `failed` is a subset of the ids we asked set_email_batch
        // to destroy; the assert pins that invariant rather than
        // letting saturating_sub paper over a bug if the server
        // ever reports more notDestroyed ids than we requested.
        debug_assert!(
            self.failed.len() <= self.attempted,
            "failed count {} exceeds attempted count {}",
            self.failed.len(),
            self.attempted
        );
        self.attempted - self.failed.len()
    }
}

/// Run the remote dedupe task across `folders`. Each entry must be
/// a known `(maildir_folder, jmap_mailbox_id)` pair (the caller
/// resolves and validates the CLI `--mailbox` arg against
/// `mailbox_map`).
///
/// Always computes the plan; applies the destroys unless `dry_run`
/// suppresses them. The returned `RemoteDedupePlan` carries both
/// the actionable groups and the skipped set (size mismatch /
/// content mismatch) so the caller can render either dry-run
/// preview or post-apply summary. `outcome` is `None` on dry-run,
/// `Some` after a destroy pass.
pub async fn run(
    client: &Client,
    conn: &Connection,
    folders: &[(String, JmapMailboxId)],
    dry_run: bool,
) -> Result<(RemoteDedupePlan, Option<RemoteDedupeOutcome>)> {
    let plan = build_plan(client, conn, folders).await?;
    if dry_run || plan.destroy_count() == 0 {
        return Ok((plan, None));
    }
    let outcome = apply_destroys(client, &plan).await?;
    Ok((plan, Some(outcome)))
}

/// Per-folder `Email/query` + `Email/get` walk; groups members by
/// `(mailbox_folder, message_id)`; classifies each group through
/// `classify_group`. Private to the module so callers can't bypass
/// the dry-run/apply orchestration in `run`.
async fn build_plan(
    client: &Client,
    conn: &Connection,
    folders: &[(String, JmapMailboxId)],
) -> Result<RemoteDedupePlan> {
    let mut plan = RemoteDedupePlan::default();
    if folders.is_empty() {
        return Ok(plan);
    }

    // jmap_email_ids whose local message_map row has a non-NULL
    // maildir_id -- candidates we'd rather keep as survivors so
    // destroying doesn't orphan a local file. Built once over the
    // whole DB so the per-group survivor pick is a HashSet lookup.
    let locally_bound = locally_bound_email_ids(conn)?;

    // Shared blob-download HTTP client for the byte-equality phase.
    // jmap-client's session metadata carries the download URL
    // template; build the reqwest client once and reuse it across
    // all groups so we don't rebuild the connection pool per blob.
    let http = jmap_email::build_blob_http_client(client)?;

    for (folder, mailbox_id) in folders {
        let ids = jmap_email::query_mailbox(client, mailbox_id.as_ref(), folder, 1).await?;
        if ids.is_empty() {
            continue;
        }
        let emails = batched_get_for_dedupe(client, &ids).await?;

        // Group by Message-ID within this mailbox. Emails with no
        // Message-ID header are skipped silently; jma already
        // refuses to ingest those (see process_remote_emails) and
        // a missing header isn't actionable as a duplicate signal.
        let mut by_msgid: HashMap<MessageId, Vec<Member>> = HashMap::new();
        for e in emails {
            let Some(mid) = e.message_id.as_ref().and_then(|m| m.first()).cloned() else {
                continue;
            };
            by_msgid.entry(mid).or_default().push(Member {
                email_id: e.id,
                blob_id: e.blob_id,
                size: e.size,
            });
        }

        for (message_id, members) in by_msgid {
            match classify_group(folder, mailbox_id, &message_id, members, &locally_bound) {
                Classification::Single => {}
                Classification::Skipped(s) => plan.skipped.push(s),
                Classification::SizeMatched {
                    group,
                    members_for_verify,
                } => {
                    // Cheap pre-check passed (all sizes agree).
                    // Now pay the bandwidth: download every member's
                    // blob and demand byte-equality across the set.
                    // Any pair-wise disagreement demotes the whole
                    // group to ContentMismatch.
                    if blobs_byte_equal(&http, client, &members_for_verify).await? {
                        plan.groups.push(group);
                    } else {
                        plan.skipped.push(RemoteDedupeSkippedGroup {
                            mailbox_folder: group.mailbox_folder,
                            message_id: group.message_id,
                            reason: SkipReason::ContentMismatch,
                            members: members_for_verify
                                .into_iter()
                                .map(|m| (m.email_id, m.blob_id))
                                .collect(),
                        });
                    }
                }
            }
        }
    }

    info!(
        "Remote dedupe plan: {} duplicate group(s) ({} ids to destroy), {} skipped",
        plan.groups.len(),
        plan.destroy_count(),
        plan.skipped.len(),
    );
    Ok(plan)
}

/// Group-member projection for the planner: the JMAP id we'd
/// destroy, the blob id we'd download for the byte-equality check,
/// and the size we'd use as the cheap pre-check signal.
#[derive(Debug, Clone)]
struct Member {
    email_id: JmapEmailId,
    blob_id: JmapBlobId,
    size: u64,
}

/// Stream every member's blob through SHA-256 and compare digests.
/// Bandwidth scales with the group size and the per-message
/// payload (one full body per member, paid once), but peak memory
/// is one HTTP chunk + 32 bytes of digest per member -- not the
/// `2 * msg_size` a buffered byte-compare would hold. A SHA-256
/// collision is the only theoretical false-positive shape, and
/// finding one is computationally infeasible for any attacker who
/// would otherwise already have account write access.
async fn blobs_byte_equal(
    http: &reqwest::Client,
    jmap: &Client,
    members: &[Member],
) -> Result<bool> {
    // Identical sizes is the planner's precondition for calling
    // this function (Classification::SizeMatched); an empty or
    // single-member slice is vacuously equal.
    if members.len() < 2 {
        return Ok(true);
    }
    let reference_digest = blob_sha256(http, jmap, &members[0].blob_id).await?;
    for m in &members[1..] {
        let digest = blob_sha256(http, jmap, &m.blob_id).await?;
        if digest != reference_digest {
            debug!(
                "blob content-equality check failed: sha256({}) != sha256({})",
                members[0].blob_id, m.blob_id,
            );
            return Ok(false);
        }
    }
    Ok(true)
}

/// Stream a blob through SHA-256 without ever holding the full
/// payload in memory. The only caller is `blobs_byte_equal`, hence
/// the colocation -- reusable JMAP I/O primitives live in
/// `src/jmap/email.rs`, but per-task content-equality machinery
/// stays with the task. Peak memory is one HTTP chunk (~16KB) plus
/// the hasher state, vs. the full message size that the buffered
/// `download_blob` returns. Retries follow the same `with_retry`
/// envelope as the rest of the JMAP layer; a mid-stream failure
/// restarts the whole hash on retry, which is fine because `Sha256`
/// has no resumption hook and we hold no reference to the partial
/// digest across retries.
async fn blob_sha256(
    http: &reqwest::Client,
    jmap: &Client,
    blob_id: &JmapBlobId,
) -> Result<[u8; 32]> {
    let url = jmap_email::build_download_url(jmap, blob_id.as_ref());
    crate::jmap::retry::with_retry("Email/blob (sha256)", || async {
        let resp = http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("Failed to start blob download for {}", blob_id))?
            .error_for_status()
            .with_context(|| format!("Server error downloading blob {}", blob_id))?;
        let mut hasher = Sha256::new();
        let mut total: u64 = 0;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.with_context(|| format!("Failed to read chunk for blob {}", blob_id))?;
            total += chunk.len() as u64;
            hasher.update(&chunk);
        }
        let digest: [u8; 32] = hasher.finalize().into();
        debug!(
            "Hashed blob {} ({} bytes) -> sha256 prefix {:02x}{:02x}{:02x}{:02x}",
            blob_id, total, digest[0], digest[1], digest[2], digest[3]
        );
        Ok(digest)
    })
    .await
}

/// Run the destroy half of `plan` against the server. Routes
/// through `set_email_batch` so chunking against `maxObjectsInSet`,
/// per-id rejection tracking, and the cursor-chain bookkeeping all
/// reuse sync's existing plumbing.
async fn apply_destroys(client: &Client, plan: &RemoteDedupePlan) -> Result<RemoteDedupeOutcome> {
    let attempted = plan.destroy_count();
    if attempted == 0 {
        return Ok(RemoteDedupeOutcome::default());
    }
    let ops: Vec<EmailSetOp> = plan
        .groups
        .iter()
        .flat_map(|g| g.destroy.iter().cloned())
        .map(|email_id| EmailSetOp::Destroy { email_id })
        .collect();
    let outcome = set_email_batch(client, &ops).await?;
    Ok(RemoteDedupeOutcome {
        attempted,
        failed: outcome.failed_destroys,
    })
}

/// Classify one Message-ID group's worth of members into a
/// candidate destroy plan, a skipped group, or nothing-to-do. Pure
/// function (no I/O); the byte-equality follow-up is done by the
/// caller after this returns `SizeMatched`. Keeping the
/// classification pure lets the size + survivor + multiple-bound
/// rules stay unit-testable without mocking JMAP.
#[derive(Debug)]
enum Classification {
    /// Group has fewer than two members -- not a duplicate.
    Single,
    /// All cheap pre-checks (size equality, single-locally-bound)
    /// passed. The caller must still verify byte-equality via
    /// `blobs_byte_equal` before committing the group to the plan;
    /// `members_for_verify` carries the blob ids to download.
    SizeMatched {
        group: RemoteDedupeGroup,
        members_for_verify: Vec<Member>,
    },
    /// A pre-check refused the group: size mismatch, or two-or-more
    /// locally-bound members.
    Skipped(RemoteDedupeSkippedGroup),
}

fn classify_group(
    mailbox_folder: &str,
    mailbox_id: &JmapMailboxId,
    message_id: &MessageId,
    members: Vec<Member>,
    locally_bound: &HashSet<JmapEmailId>,
) -> Classification {
    if members.len() < 2 {
        return Classification::Single;
    }
    let sizes: HashSet<u64> = members.iter().map(|m| m.size).collect();
    if sizes.len() != 1 {
        return Classification::Skipped(RemoteDedupeSkippedGroup {
            mailbox_folder: mailbox_folder.to_string(),
            message_id: message_id.clone(),
            reason: SkipReason::SizeMismatch,
            members: members
                .into_iter()
                .map(|m| (m.email_id, m.blob_id))
                .collect(),
        });
    }
    // At most one member of a same-Message-ID group can carry a
    // non-NULL `message_map.maildir_id` at the time we run: the
    // partial-unique index forbids two such rows from coexisting,
    // and a steady-state sync (with its Phase 0 dedupe + the
    // downstream DestroyRemote it emits) collapses any
    // multi-local-file dupes before they leave the cycle. If this
    // ever trips, the schema invariant has slipped and we want to
    // know loudly rather than silently picking a survivor that
    // orphans the loser's local file.
    debug_assert!(
        members
            .iter()
            .filter(|m| locally_bound.contains(&m.email_id))
            .count()
            <= 1,
        "More than one Message-ID group member is locally bound -- \
         partial-unique index on message_map(maildir_id) should make this \
         impossible. Group members: {:?}",
        members,
    );
    let mut sorted = members;
    sorted.sort_by(|a, b| a.email_id.as_ref().cmp(b.email_id.as_ref()));
    // Prefer the locally-bound member as survivor so destroying the
    // group doesn't orphan a maildir file; lex-smallest is the
    // deterministic fallback when none is bound.
    let survivor_idx = sorted
        .iter()
        .position(|m| locally_bound.contains(&m.email_id))
        .unwrap_or(0);
    let survivor_member = sorted.remove(survivor_idx);
    let survivor = survivor_member.email_id.clone();
    let blob_id = survivor_member.blob_id.clone();
    let destroy: Vec<JmapEmailId> = sorted.iter().map(|m| m.email_id.clone()).collect();
    // Reassemble the full membership for the byte-equality pass --
    // verification has to see every id, not just the destroys.
    let mut members_for_verify = sorted;
    members_for_verify.insert(0, survivor_member);
    Classification::SizeMatched {
        group: RemoteDedupeGroup {
            mailbox_folder: mailbox_folder.to_string(),
            mailbox_id: mailbox_id.clone(),
            message_id: message_id.clone(),
            blob_id,
            survivor,
            destroy,
        },
        members_for_verify,
    }
}

/// Build the set of jmap_email_ids in `message_map` whose
/// `maildir_id` column is non-NULL -- the candidates we'd rather
/// pick as survivor so destroying doesn't orphan a local file.
fn locally_bound_email_ids(conn: &Connection) -> Result<HashSet<JmapEmailId>> {
    let mut stmt = conn
        .prepare("SELECT jmap_email_id FROM message_map WHERE maildir_id IS NOT NULL")
        .context("Failed to prepare locally-bound lookup")?;
    let rows = stmt
        .query_map([], |row| row.get::<_, JmapEmailId>(0))?
        .collect::<rusqlite::Result<HashSet<_>>>()?;
    debug!(
        "Remote dedupe: {} locally-bound jmap_email_id(s) eligible as survivors",
        rows.len()
    );
    Ok(rows)
}

/// Fetch `EmailObject`s for `ids` by chunking against the server's
/// advertised `maxObjectsInGet` and issuing each chunk sequentially.
/// Janitor isn't latency-sensitive, so the cross-chunk parallelism
/// `engine.rs::batched_get` uses isn't worth the extra surface
/// here.
async fn batched_get_for_dedupe(
    client: &Client,
    ids: &[JmapEmailId],
) -> Result<Vec<crate::jmap::types::EmailObject>> {
    let chunk_size = limits::max_objects_in_get(client);
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(chunk_size) {
        let batch = jmap_email::get_by_ids(client, chunk).await?;
        out.extend(batch);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mb() -> JmapMailboxId {
        "MB-INBOX".into()
    }

    fn mid() -> MessageId {
        "<a@x>".into()
    }

    /// Test-side member constructor. Size 100 by default; pass
    /// `with_size` to vary it for the size-mismatch path.
    fn member(id: &str, blob: &str) -> Member {
        Member {
            email_id: id.into(),
            blob_id: blob.into(),
            size: 100,
        }
    }

    fn member_size(id: &str, blob: &str, size: u64) -> Member {
        Member {
            email_id: id.into(),
            blob_id: blob.into(),
            size,
        }
    }

    #[test]
    fn destroy_count_sums_across_groups() {
        let plan = RemoteDedupePlan {
            groups: vec![
                RemoteDedupeGroup {
                    mailbox_folder: "INBOX".into(),
                    mailbox_id: mb(),
                    message_id: mid(),
                    blob_id: "blob-1".into(),
                    survivor: "A1".into(),
                    destroy: vec!["A2".into(), "A3".into()],
                },
                RemoteDedupeGroup {
                    mailbox_folder: "INBOX".into(),
                    mailbox_id: mb(),
                    message_id: "<b@x>".into(),
                    blob_id: "blob-2".into(),
                    survivor: "B1".into(),
                    destroy: vec!["B2".into()],
                },
            ],
            skipped: Vec::new(),
        };
        assert_eq!(plan.destroy_count(), 3);
    }

    /// Single-member group: never actionable. Same Message-ID
    /// appearing in only one Email is not a duplicate.
    #[test]
    fn single_member_is_not_a_group() {
        let bound = HashSet::new();
        match classify_group("INBOX", &mb(), &mid(), vec![member("E1", "blob")], &bound) {
            Classification::Single => {}
            other => panic!("expected Single, got {other:?}"),
        }
    }

    /// All members share size, none is locally bound: lex-smallest
    /// jmap_email_id wins as survivor for determinism. The byte-
    /// equality follow-up is the caller's job; this test only
    /// pins the pure size-then-survivor classification.
    #[test]
    fn same_size_no_local_binding_picks_lex_smallest_survivor() {
        let bound = HashSet::new();
        let members = vec![
            member("Z1", "blob-z"),
            member("A1", "blob-a"),
            member("M1", "blob-m"),
        ];
        let Classification::SizeMatched { group, .. } =
            classify_group("INBOX", &mb(), &mid(), members, &bound)
        else {
            panic!("expected SizeMatched");
        };
        assert_eq!(group.survivor.as_ref(), "A1");
        let destroyed: HashSet<&str> = group.destroy.iter().map(|i| i.as_ref()).collect();
        assert_eq!(destroyed, ["M1", "Z1"].into_iter().collect());
    }

    /// One member's jmap_email_id is locally bound: that one wins
    /// regardless of lex order, so destroying the group doesn't
    /// orphan the local file.
    #[test]
    fn same_size_with_local_binding_picks_bound_survivor() {
        let mut bound = HashSet::new();
        bound.insert(JmapEmailId::from("Z1"));
        let members = vec![
            member("A1", "blob-a"),
            member("M1", "blob-m"),
            member("Z1", "blob-z"),
        ];
        let Classification::SizeMatched { group, .. } =
            classify_group("INBOX", &mb(), &mid(), members, &bound)
        else {
            panic!("expected SizeMatched");
        };
        assert_eq!(group.survivor.as_ref(), "Z1");
        let destroyed: HashSet<&str> = group.destroy.iter().map(|i| i.as_ref()).collect();
        assert_eq!(destroyed, ["A1", "M1"].into_iter().collect());
    }

    /// Members share Message-ID but the server reports different
    /// sizes. Different sizes prove different bytes -- the cheap
    /// pre-check refuses the group without any blob download. Pins
    /// that the size pass is the *first* filter, not a follow-up.
    #[test]
    fn different_sizes_skip_group_with_size_mismatch() {
        let bound = HashSet::new();
        let members = vec![
            member_size("E1", "blob-a", 100),
            member_size("E2", "blob-b", 200),
        ];
        let Classification::Skipped(s) = classify_group("INBOX", &mb(), &mid(), members, &bound)
        else {
            panic!("expected Skipped");
        };
        assert_eq!(s.reason, SkipReason::SizeMismatch);
        assert_eq!(s.members.len(), 2);
    }

    /// `SizeMatched` carries every member's blob id for the
    /// downstream byte-equality download, not just the destroys.
    /// Pins that the survivor's blob is included in the verify
    /// set -- otherwise a downstream byte check could only compare
    /// destroys against each other and would miss a survivor that
    /// silently mismatched.
    #[test]
    fn size_matched_carries_all_members_for_verify() {
        let bound = HashSet::new();
        let members = vec![member("A1", "blob-a"), member("B1", "blob-b")];
        let Classification::SizeMatched {
            members_for_verify, ..
        } = classify_group("INBOX", &mb(), &mid(), members, &bound)
        else {
            panic!("expected SizeMatched");
        };
        let verified: HashSet<&str> = members_for_verify
            .iter()
            .map(|m| m.email_id.as_ref())
            .collect();
        assert_eq!(verified, ["A1", "B1"].into_iter().collect());
    }
}
