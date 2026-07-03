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
//! 3. For each remaining orphan, sample M groups of N parseable
//!    `Message-ID` headers from `cur/` and `new/`. Sampling is a
//!    PRNG-shuffle of every parseable file in the folder, with the
//!    seed derived from the folder's path. Two consequences fall
//!    out: the same folder produces the same shuffle on every
//!    invocation (so the CLI's report-then-apply workflow stays
//!    convergent across the two `run()` calls), and the M groups
//!    are independent draws from the folder's content (so a
//!    pathological "first-N happens to all live in one non-target
//!    mailbox" run -- thread-clustered timestamps, draft templates,
//!    mailing-list digests with prefix-shared Message-IDs -- can't
//!    silently rebind to the wrong mailbox).
//! 4. One `Email/query` per folder with `Filter::or` of M*N
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
//!    returned `messageId` array's bracket-stripped form). The
//!    exact-match bin closes the false-positive prefix risk inherent
//!    to substring matching: an email returned because "abc@x.com"
//!    was a substring of its `<xabc@x.com>` header is dropped here
//!    because no sample equals `xabc@x.com`. Then, independently
//!    for each of the M groups, take the per-sample union of
//!    `mailboxIds` (covers the draft-and-sent-copy-share-Message-ID
//!    case) and the cross-sample intersection (narrows to the
//!    mailbox every sample in the group agrees on). The result is
//!    M independently-narrowed candidate sets.
//! 6. Consensus check: if every group narrowed to the same
//!    singleton {X}, X is the rebind target. Any other shape --
//!    empty narrowed set in any group, a narrowed set of size > 1,
//!    or two groups disagreeing on which singleton -- refuses the
//!    rebind and surfaces the per-group results in the
//!    `AmbiguousAcrossSamples` report. The math of intersection is
//!    associative, so a single M*N-wide intersection would arrive
//!    at the same {X} when consensus is met; the per-group split
//!    exists so the operator's report shows the disagreement
//!    instead of an opaque single-set "no decision."
//!
//! Sampling is M disjoint groups, not M iid (with-replacement)
//! draws. The disjoint approach gives every parseable Message-ID
//! at most one vote across all groups, so a "groups disagree"
//! signal genuinely comes from M independent witnesses. With-
//! overlap sampling would let a popular Message-ID contribute
//! the same vote redundantly to multiple groups -- collapsing
//! the independence the consensus check relies on -- and on small
//! folders (K < M*N parseable Message-IDs) it would force the
//! groups into high correlation by drawing repeatedly from a
//! small pool, making "all groups agree" look stronger than it
//! is. The disjoint scheme's small-folder fallback (truncate to
//! K and form fewer or partial groups) honestly degrades the
//! consensus signal in that case rather than masking the reduced
//! coverage.
//!
//! After the per-folder pass, a cross-mapping pass runs over any
//! `AmbiguousAcrossSamples` results. The bijection assumption (each
//! maildir corresponds to exactly one server mailbox) plus the
//! cardinality match (server mailbox count equals on-disk maildir
//! count, no NoMessageIds / NoServerMatches in this cycle) turns
//! the remaining ambiguity into a bipartite-matching problem
//! between ambiguous maildirs and still-unclaimed server mailboxes.
//! Strict feasibility -- a maildir is feasible for a mailbox iff
//! that mailbox is in every non-empty per-group narrowed set --
//! prunes the graph; a unique perfect matching then promotes each
//! ambiguous skip to a `RebindCandidate` tagged
//! `ResolveSource::CrossMapping`. The `i_total == N_disk`
//! precondition uses the unfiltered server list on purpose: the
//! disk reflects historical sync state, the `[sync].mailboxes`
//! filter is a runtime decision that may have changed since the
//! maildirs landed, and trusting the filter would risk misbinding
//! across a boundary the user configured for a reason. Non-unique
//! or absent matchings keep the original AmbiguousAcrossSamples
//! skip in place rather than guessing.

use anyhow::{Context, Result};
use jmap_client::client::Client;
use jmap_client::core::query::Filter as CoreFilter;
use jmap_client::email::query::Filter as EmailFilter;
use rand::SeedableRng;
use rand::seq::SliceRandom;
use rusqlite::Connection;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use tracing::{debug, info};

use crate::domain::MailboxObject;
use crate::ids::{JmapEmailId, JmapMailboxId, MessageId};
use crate::jmap::limits;
use crate::jmap::retry::with_retry;
use crate::jmap::types::EmailObject;
use crate::jmap::{email as jmap_email, mailbox as jmap_mailbox};
use crate::maildir_ops::headers::parse_message_id_from_file;
use crate::maildir_ops::namespace::is_jma_private;
use crate::maildir_ops::sentinel::{self, MailboxMapping};
use crate::maildir_ops::store;
use crate::state::queries;

/// Default Message-ID sample size *per consensus group* per orphan
/// folder. Four samples per group at `DEFAULT_GROUP_COUNT = 3`
/// produces 12 total samples per folder -- well above the threshold
/// where the per-group intersection narrows to a single mailbox in
/// normal traffic, and well below any server's `Email/query` result
/// cap. Per-group rather than total is the user-facing semantic so
/// `--sample-size N` always yields exactly N samples in each of M
/// groups: no integer-division truncation when N is not a multiple
/// of M, and the operator can predict the wire cost (`N * M`
/// conditions in the `Email/query` OR) from the flag value alone.
pub const DEFAULT_SAMPLE_SIZE: usize = 4;

/// Number of independent sample groups for the consensus check. Each
/// group narrows to a candidate mailbox set; all groups must agree on
/// the same singleton to rebind. Three groups is the smallest count
/// that gives a meaningful disagreement signal -- two groups is just
/// "do they match," three is "do all of them match" and lets a single
/// outlier surface as the dissenter in the operator's report.
pub(crate) const DEFAULT_GROUP_COUNT: usize = 3;

/// One folder the planner decided to rebind. Carries everything
/// `apply` needs to write the sentinel without re-querying.
#[derive(Debug, Clone)]
pub struct RebindCandidate {
    pub folder_path: PathBuf,
    pub jmap_mailbox_id: JmapMailboxId,
    pub server_name: String,
    /// Slash-joined server-side hierarchy path -- the user-facing
    /// identifier for the bound mailbox (e.g. `INBOX`, `Folders/
    /// Archive`). Carried alongside `jmap_mailbox_id` because the
    /// operator-visible report refers to mailboxes by path, not
    /// by opaque server token.
    pub remote_path: String,
    pub parent_jmap_mailbox_id: Option<JmapMailboxId>,
    pub sample_count: usize,
    pub source: ResolveSource,
}

/// How a `RebindCandidate` arrived at its binding -- surfaced in the
/// operator-facing report so the operator can see whether the choice
/// rests on the per-folder consensus alone or on the cross-folder
/// cardinality argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveSource {
    /// Per-folder M-of-N consensus narrowed to a single mailbox.
    /// Strongest signal: the folder's own samples pointed at the
    /// binding without help.
    Consensus,
    /// The per-folder probe was `AmbiguousAcrossSamples`, but a
    /// unique perfect matching from the ambiguous-maildir set to
    /// the unclaimed-mailbox set forced the assignment under the
    /// strict feasibility test. Implies the bipartite-matching
    /// preconditions held: server mailbox count equals on-disk
    /// maildir count and no folder probed to NoMessageIds /
    /// NoServerMatches in this cycle.
    CrossMapping,
    /// The operator supplied an explicit binding via
    /// `--bind PATH=REMOTE_PATH`. The per-folder probe was
    /// skipped entirely for this path; the cross-mapping post-pass
    /// is globally disabled for this run because providing any
    /// explicit binding is a declaration that the operator is
    /// taking manual control of disambiguation.
    Explicit,
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
    /// All sampled Message-IDs were unknown to the server across
    /// every consensus group. Could be a server-deleted folder
    /// still living on disk; could be a folder whose mail was all
    /// sent and removed; could be a folder from a different
    /// account that was copied here.
    NoServerMatches,
    /// The consensus check failed: at least one group's narrowed
    /// set was empty or larger than one, or the groups arrived at
    /// different singletons. `per_group` records each group's
    /// narrowed set in input-group order, sorted within each group
    /// by `JmapMailboxId::as_ref()` for stable rendering. The
    /// operator-facing report walks this to show the disagreement
    /// (e.g. "group 0 narrowed to {Inbox}, group 1 narrowed to
    /// {Inbox, All Mail}, group 2 narrowed to {}").
    AmbiguousAcrossSamples { per_group: Vec<Vec<JmapMailboxId>> },
}

#[derive(Debug, Default)]
pub struct RebindFoldersPlan {
    pub candidates: Vec<RebindCandidate>,
    pub skipped: Vec<SkippedFolder>,
    /// Lookup the operator-facing render uses to translate the
    /// id-typed entries inside `SkipReason::AmbiguousAcrossSamples`
    /// into user-readable remote_paths at print time. Empty when
    /// the plan is constructed in a unit test fixture; populated
    /// from `Mailbox/get` at the head of `plan()`.
    pub remote_paths: HashMap<JmapMailboxId, String>,
}

/// Build a rebind plan against the live server. Pure plan; nothing
/// is written until `apply`. Caller is responsible for holding the
/// maildir + state DB locks; the function only reads from disk.
pub async fn plan(
    client: &Client,
    conn: &Connection,
    maildir_root: &Path,
    samples_per_group: usize,
    explicit_bindings: HashMap<PathBuf, String>,
) -> Result<RebindFoldersPlan> {
    let _phase =
        tracing::info_span!(target: crate::profile::TARGET_PHASE, "rebindfolders").entered();

    let (server_mailboxes, _mailbox_state) = jmap_mailbox::get_all(client).await?;
    let by_id: HashMap<JmapMailboxId, &MailboxObject> = server_mailboxes
        .iter()
        .map(|mb| (mb.id.clone(), mb))
        .collect();

    // Build the by-id remote-path map via the shared engine helper so
    // it can't drift from the engine's own path computation.
    let remote_paths = crate::jmap::mailbox::build_remote_paths(&by_id);

    let bound_folders: HashSet<String> = queries::list_known_maildir_folders(conn)?
        .into_iter()
        .collect();

    // Reject --bind paths that aren't inside maildir_root, or
    // resolve via symlink to somewhere outside it. `canonicalize`
    // returns the fully-resolved absolute path with all symlinks
    // followed, so a maildir_root-relative symlink pointing at
    // /etc would canonicalize to /etc and fail the prefix check.
    // The orphan walk only enumerates under maildir_root, so an
    // off-root --bind path would otherwise just produce the
    // typo-warn below; rejecting up front gives the operator a
    // clear error instead.
    let canonical_root = maildir_root.canonicalize().with_context(|| {
        format!(
            "maildir_root {} cannot be canonicalized",
            maildir_root.display()
        )
    })?;
    for path in explicit_bindings.keys() {
        let canonical = path.canonicalize().with_context(|| {
            format!(
                "--bind {} rejected: path does not exist or is not accessible",
                path.display()
            )
        })?;
        if !canonical.starts_with(&canonical_root) {
            return Err(anyhow::anyhow!(
                "--bind {} rejected: path resolves to {} which is outside maildir_root {}",
                path.display(),
                canonical.display(),
                canonical_root.display(),
            ));
        }
    }

    // Validate each --bind's remote_path against the live server.
    // Reverse-lookup from path -> id; reject up front when the
    // operator named a mailbox that doesn't exist, so a typo
    // surfaces before any orphan walk or disk write. RFC 8621
    // section 2.5's `nameAlreadyExists` rejects same-name-and-
    // parentId at `Mailbox/set`, so spec-compliant servers
    // shouldn't expose colliding paths in `Mailbox/get`;
    // last-write-wins handles non-conformant servers.
    let path_to_id: HashMap<&str, &JmapMailboxId> = remote_paths
        .iter()
        .map(|(id, path)| (path.as_str(), id))
        .collect();
    let resolved_bindings: HashMap<PathBuf, (JmapMailboxId, String)> = explicit_bindings
        .iter()
        .map(|(path, remote_path)| {
            let id = path_to_id.get(remote_path.as_str()).copied().with_context(|| {
                format!(
                    "explicit --bind {} -> {} rejected: no server mailbox advertises that remote_path",
                    path.display(),
                    remote_path,
                )
            })?;
            Ok::<_, anyhow::Error>((path.clone(), (id.clone(), remote_path.clone())))
        })
        .collect::<Result<_>>()?;

    let (orphans, sentinel_bound_ids) = walk_for_orphans(maildir_root, &by_id, &bound_folders);
    info!("rebindfolders: found {} candidate folder(s)", orphans.len());

    // Refuse any --bind whose target mailbox id is already bound
    // somewhere on disk OR claimed by another --bind in the same
    // invocation. The set is the union of cache-side `mailbox_map`
    // rows, sentinel-survives folders the orphan walk just
    // enumerated, and in-batch duplicates. Without this guard
    // `--bind /foo=Bar` where Bar is already at `/bar`, OR
    // `--bind /foo=Bar --bind /baz=Bar` in the same call, would
    // write two sentinels pointing at the same mailbox id,
    // leaving two on-disk folders claiming the same server
    // mailbox. The next sync cycle's `resolve_mailboxes` would
    // resolve one as canonical and the other as drift --
    // destructive in the silent-data-duplication sense. Fail
    // loudly here instead.
    let cache_claimed_ids: HashSet<JmapMailboxId> =
        queries::list_known_mailbox_ids(conn)?.into_iter().collect();
    let mut seen_target_ids: HashSet<JmapMailboxId> = HashSet::new();
    for (path, (mailbox_id, remote_path)) in &resolved_bindings {
        if cache_claimed_ids.contains(mailbox_id)
            || sentinel_bound_ids.contains(mailbox_id)
            || !seen_target_ids.insert(mailbox_id.clone())
        {
            return Err(anyhow::anyhow!(
                "explicit --bind {} -> {} rejected: that mailbox is already \
                 claimed (by another --bind in this invocation, by `mailbox_map`, \
                 or by a surviving sentinel). Clean up the existing binding \
                 before redirecting.",
                path.display(),
                remote_path,
            ));
        }
    }

    // Surface explicit bindings whose path is not in the orphan
    // walk so the operator notices typos / stale paths instead of
    // a silent no-op. Each unmatched path warns but stays in the
    // map; the per-orphan loop below only iterates real orphans, so
    // no Bind will be emitted for the typo. The cross-mapping gate
    // still treats the run as operator-controlled because the
    // operator has declared intent by passing any --bind at all.
    let orphan_set: HashSet<&PathBuf> = orphans.iter().collect();
    for path in resolved_bindings.keys() {
        if !orphan_set.contains(path) {
            tracing::warn!(
                "rebindfolders: --bind {} ignored: path is not in the orphan walk",
                path.display()
            );
        }
    }

    // Cross-mapping precondition input: maildir count visible to the
    // cache + orphan layer. Counts `bound_folders` (mailbox_map rows
    // with a folder name on disk) plus `orphans` (unclaimed
    // candidates `walk_for_orphans` returned). Does *not* count
    // sentinel-survives-but-cache-lost maildirs -- those are dropped
    // by `walk_for_orphans` because their `.jma.mapping` points at a
    // known server mailbox. In a state-DB-nuke recovery where
    // sentinels survived for some maildirs and were lost for others,
    // `n_disk` undercounts by the survivor population and the
    // `i_total == n_disk` gate fails closed -- cross-mapping skips
    // and the per-folder report still surfaces the ambiguous
    // remnants. Acceptable as conservatism: the alternative is a
    // second disk walk to enumerate sentinel survivors plus the
    // unclaimed-pool adjustment, and the partial-state setup it
    // targets is uncommon.
    let n_disk = bound_folders.len() + orphans.len();

    let mut plan = RebindFoldersPlan::default();
    let get_cap = limits::max_objects_in_get(client);
    for folder_path in orphans {
        // Explicit override: skip the probe and emit a Bind tagged
        // `Explicit` straight from the resolved binding. Validated
        // above against `path_to_id`, so the lookup cannot miss.
        if let Some((mailbox_id, remote_path)) = resolved_bindings.get(&folder_path) {
            let mb = by_id
                .get(mailbox_id)
                .expect("validated id missed by_id lookup");
            plan.candidates.push(RebindCandidate {
                folder_path,
                jmap_mailbox_id: mb.id.clone(),
                server_name: mb.name.clone(),
                remote_path: remote_path.clone(),
                parent_jmap_mailbox_id: mb.parent_id.clone(),
                sample_count: 0,
                source: ResolveSource::Explicit,
            });
            continue;
        }

        match probe_folder(
            client,
            &folder_path,
            samples_per_group,
            DEFAULT_GROUP_COUNT,
            get_cap,
            &by_id,
            &remote_paths,
        )
        .await?
        {
            ProbeOutcome::Bind {
                jmap_mailbox_id,
                sample_count,
            } => {
                let mb = by_id
                    .get(&jmap_mailbox_id)
                    .with_context(|| format!("intersection picked unknown id {jmap_mailbox_id}"))?;
                let remote_path = remote_paths
                    .get(&jmap_mailbox_id)
                    .cloned()
                    .expect("remote_paths populated for every server-known mailbox");
                plan.candidates.push(RebindCandidate {
                    folder_path,
                    jmap_mailbox_id: mb.id.clone(),
                    server_name: mb.name.clone(),
                    remote_path,
                    parent_jmap_mailbox_id: mb.parent_id.clone(),
                    sample_count,
                    source: ResolveSource::Consensus,
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

    // Cross-mapping pass: see module docs. Globally disabled when
    // the operator supplied any explicit binding -- including
    // typo'd `--bind` whose path didn't match an orphan, because
    // the operator's intent ("I am taking manual control") is the
    // signal we honor, not whether each individual binding was
    // load-bearing in this cycle. Mixing operator overrides with
    // algorithmic disambiguation composes surprisingly, so the
    // algorithm steps out entirely.
    if explicit_bindings.is_empty() {
        cross_resolve_ambiguous(&mut plan, conn, &by_id, &remote_paths, n_disk)?;
    }

    plan.remote_paths = remote_paths;
    Ok(plan)
}

/// Promote `AmbiguousAcrossSamples` skips to Binds when the bipartite
/// matching between ambiguous maildirs and unclaimed server mailboxes
/// has a unique perfect solution under strict feasibility.
///
/// Preconditions, any of which short-circuits to a no-op:
/// - Server mailbox count must equal on-disk maildir count
///   (`i_total == N_disk`). The disk-side filter is deliberately
///   ignored -- the disk reflects historical sync state, the
///   `[sync].mailboxes` filter is a runtime decision that may have
///   changed since the maildirs landed, and trusting it would risk
///   misbinding across a filter boundary the user configured for a
///   reason.
/// - Every orphan must have resolved to either Consensus (already
///   on `plan.candidates`) or AmbiguousAcrossSamples. Any other
///   skip reason (NoMessageIds, NoServerMatches) breaks the
///   bijection assumption: a folder with no server-side match
///   doesn't correspond to a server mailbox in this cycle.
/// - The count of ambiguous maildirs must equal the count of
///   still-unclaimed server mailboxes (mailboxes neither in
///   `mailbox_map` nor in this cycle's Consensus binds). With the
///   first two preconditions met this is algebraic; the explicit
///   check is a belt-and-braces guard.
///
/// Strict feasibility: a maildir is feasible for a mailbox iff the
/// mailbox is in every non-empty per-group narrowed set for that
/// maildir and is in the unclaimed pool. Empty per-group sets
/// abstain rather than disqualify -- a group whose samples are all
/// server-side absent (deleted between cycles) carries no
/// constraint, but isn't a vote *against* any mailbox either.
///
/// Uniqueness: standard bipartite matching can find a perfect
/// matching when one exists; uniqueness needs a separate check.
/// The implementation enumerates matchings via backtracking,
/// capping the collection at two so non-uniqueness shows up as
/// `found.len() > 1` without continuing to enumerate. Non-unique
/// matchings keep the original AmbiguousAcrossSamples skip in
/// place -- picking one arbitrarily would push the same "guessing"
/// risk up one level.
fn cross_resolve_ambiguous(
    plan: &mut RebindFoldersPlan,
    conn: &Connection,
    by_id: &HashMap<JmapMailboxId, &MailboxObject>,
    remote_paths: &HashMap<JmapMailboxId, String>,
    n_disk: usize,
) -> Result<()> {
    if by_id.len() != n_disk {
        return Ok(());
    }
    let all_ambiguous = plan
        .skipped
        .iter()
        .all(|s| matches!(s.reason, SkipReason::AmbiguousAcrossSamples { .. }));
    if !all_ambiguous {
        return Ok(());
    }
    let ambiguous: Vec<(PathBuf, Vec<Vec<JmapMailboxId>>)> = plan
        .skipped
        .iter()
        .filter_map(|s| match &s.reason {
            SkipReason::AmbiguousAcrossSamples { per_group } => {
                Some((s.folder_path.clone(), per_group.clone()))
            }
            _ => None,
        })
        .collect();
    if ambiguous.is_empty() {
        return Ok(());
    }

    let already_claimed: HashSet<JmapMailboxId> =
        queries::list_known_mailbox_ids(conn)?.into_iter().collect();
    let cycle_claimed: HashSet<JmapMailboxId> = plan
        .candidates
        .iter()
        .map(|c| c.jmap_mailbox_id.clone())
        .collect();
    let unclaimed: HashSet<JmapMailboxId> = by_id
        .keys()
        .filter(|id| !already_claimed.contains(*id) && !cycle_claimed.contains(*id))
        .cloned()
        .collect();

    if ambiguous.len() != unclaimed.len() {
        return Ok(());
    }

    let feasibility: Vec<Vec<JmapMailboxId>> = ambiguous
        .iter()
        .map(|(_, per_group)| strict_feasibility(per_group, &unclaimed))
        .collect();

    let Some(matching) = unique_perfect_matching(&feasibility) else {
        return Ok(());
    };

    let promoted_paths: HashSet<PathBuf> = ambiguous.iter().map(|(p, _)| p.clone()).collect();
    for (i, (folder_path, _)) in ambiguous.into_iter().enumerate() {
        let mailbox_id = matching[i].clone();
        let mb = by_id
            .get(&mailbox_id)
            .with_context(|| format!("cross-mapping picked unknown id {mailbox_id}"))?;
        let remote_path = remote_paths
            .get(&mailbox_id)
            .cloned()
            .expect("remote_paths populated for every server-known mailbox");
        debug!(
            "rebindfolders: {} -> {} (via cross-mapping)",
            folder_path.display(),
            remote_path
        );
        plan.candidates.push(RebindCandidate {
            folder_path,
            jmap_mailbox_id: mb.id.clone(),
            server_name: mb.name.clone(),
            remote_path,
            parent_jmap_mailbox_id: mb.parent_id.clone(),
            sample_count: 0,
            source: ResolveSource::CrossMapping,
        });
    }
    plan.skipped
        .retain(|s| !promoted_paths.contains(&s.folder_path));
    Ok(())
}

/// Strict feasibility set for one ambiguous maildir: intersection of
/// every non-empty per-group narrowed set, then intersected with the
/// unclaimed pool. Operates on `JmapMailboxId` because that's what
/// the consensus probe binds (and the cross-mapping post-pass
/// consumes); the remote_path conversion lives at the operator-
/// facing render boundary.
fn strict_feasibility(
    per_group: &[Vec<JmapMailboxId>],
    unclaimed: &HashSet<JmapMailboxId>,
) -> Vec<JmapMailboxId> {
    let non_empty: Vec<&Vec<JmapMailboxId>> = per_group.iter().filter(|g| !g.is_empty()).collect();
    if non_empty.is_empty() {
        return Vec::new();
    }
    let mut out: Vec<JmapMailboxId> = non_empty[0]
        .iter()
        .filter(|id| non_empty.iter().all(|g| g.contains(*id)) && unclaimed.contains(*id))
        .cloned()
        .collect();
    out.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    out
}

/// Return Some(matching) iff exactly one perfect matching exists from
/// the feasibility constraints. Backtracking enumeration short-circuits
/// after the second matching is discovered; typical k is small (1-5),
/// so even the worst-case k! enumeration is cheap.
fn unique_perfect_matching(feasibility: &[Vec<JmapMailboxId>]) -> Option<Vec<JmapMailboxId>> {
    if feasibility.is_empty() {
        return None;
    }
    let mut current: Vec<JmapMailboxId> = Vec::with_capacity(feasibility.len());
    let mut used: HashSet<JmapMailboxId> = HashSet::new();
    let mut found: Vec<Vec<JmapMailboxId>> = Vec::new();
    enumerate_matchings(feasibility, 0, &mut current, &mut used, &mut found, 2);
    (found.len() == 1).then(|| found.into_iter().next().expect("len == 1"))
}

fn enumerate_matchings(
    feasibility: &[Vec<JmapMailboxId>],
    i: usize,
    current: &mut Vec<JmapMailboxId>,
    used: &mut HashSet<JmapMailboxId>,
    found: &mut Vec<Vec<JmapMailboxId>>,
    cap: usize,
) {
    if found.len() >= cap {
        return;
    }
    if i == feasibility.len() {
        found.push(current.clone());
        return;
    }
    for candidate in &feasibility[i] {
        if used.contains(candidate) {
            continue;
        }
        used.insert(candidate.clone());
        current.push(candidate.clone());
        enumerate_matchings(feasibility, i + 1, current, used, found, cap);
        current.pop();
        used.remove(candidate);
        if found.len() >= cap {
            return;
        }
    }
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
            "rebindfolders: bound {} to {}",
            c.folder_path.display(),
            c.remote_path,
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
    samples_per_group: usize,
    explicit_bindings: HashMap<PathBuf, String>,
    dry_run: bool,
) -> Result<RebindFoldersPlan> {
    let p = plan(
        client,
        conn,
        maildir_root,
        samples_per_group,
        explicit_bindings,
    )
    .await?;
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

/// Probe a single orphan: sample its Message-IDs in `group_count`
/// independent PRNG-shuffled groups of `samples_per_group` each,
/// query the server with a single flat `Email/query`, then check
/// consensus across groups. `remote_paths` is the by-id remote-path
/// lookup needed to render the `AmbiguousAcrossSamples` skip in
/// user-facing terms; only the skip arm reads it, but threading it
/// through the bind arm too would mean the skip arm couldn't be
/// constructed at the probe boundary without a follow-up pass.
async fn probe_folder(
    client: &Client,
    folder_path: &Path,
    samples_per_group: usize,
    group_count: usize,
    get_cap: usize,
    by_id: &HashMap<JmapMailboxId, &MailboxObject>,
    remote_paths: &HashMap<JmapMailboxId, String>,
) -> Result<ProbeOutcome> {
    let groups = sample_message_id_groups(folder_path, group_count, samples_per_group)?;
    if groups.is_empty() {
        return Ok(ProbeOutcome::Skip(SkipReason::NoMessageIds));
    }

    // Flatten for one Email/query; the per-group split applies at the
    // intersection step, not at the wire.
    let flat: Vec<MessageId> = groups.iter().flatten().cloned().collect();
    let total_samples = flat.len();

    let candidate_ids = query_by_message_ids(client, &flat).await?;
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

    // Per-group narrowed set, filtered to mailboxes the server still
    // advertises (Mailbox/get and Email/get may have observed slightly
    // different snapshots; failing closed beats binding to a mailbox
    // we can't render). Sorted within each group for stable reporting.
    let per_group: Vec<Vec<JmapMailboxId>> = groups
        .iter()
        .map(|samples| {
            let mut narrowed: Vec<JmapMailboxId> = intersect_mailboxes_by_sample(&emails, samples)
                .into_iter()
                .filter(|id| by_id.contains_key(id))
                .collect();
            narrowed.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
            narrowed
        })
        .collect();

    // Consensus: every group must narrow to the SAME singleton.
    let first = per_group
        .first()
        .expect("groups non-empty since flat was non-empty");
    let consensus =
        (first.len() == 1 && per_group.iter().all(|g| g == first)).then(|| first[0].clone());

    match consensus {
        Some(id) => {
            let path_display = remote_paths
                .get(&id)
                .map(String::as_str)
                .unwrap_or_else(|| id.as_ref());
            debug!(
                "rebindfolders: {} -> {} (consensus across {} group(s), {} sample(s) total)",
                folder_path.display(),
                path_display,
                groups.len(),
                total_samples,
            );
            Ok(ProbeOutcome::Bind {
                jmap_mailbox_id: id,
                sample_count: total_samples,
            })
        }
        None => Ok(ProbeOutcome::Skip(SkipReason::AmbiguousAcrossSamples {
            per_group,
        })),
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

/// Collect every parseable Message-ID from a maildir folder's
/// `cur/` and `new/` (deduped across files), PRNG-shuffle the list
/// with a seed derived from the folder path, then partition the
/// first `samples_per_group * group_count` IDs into up to
/// `group_count` groups of `samples_per_group` each.
///
/// `samples_per_group` is the parameter directly: no integer-
/// division surprise. `samples_per_group = 4, group_count = 3`
/// yields 3 groups of 4 (total 12). `samples_per_group = 1` yields
/// 3 groups of 1 (3 samples total) -- a weak but still meaningful
/// 3-of-3 consensus on three independently-drawn votes.
///
/// Returns Vec<Vec<MessageId>>:
/// - Empty when the folder has no parseable Message-IDs.
/// - Length up to `group_count`; the last group may be shorter
///   than `samples_per_group` if the folder ran out of unique
///   Message-IDs.
///
/// Determinism: seeding from the folder path means a second
/// invocation against the same folder produces the same partition.
/// This is load-bearing for the CLI's report-then-apply workflow,
/// which calls `run()` twice and assumes the second call produces
/// the same plan as the first. The guarantee holds within a single
/// binary: both `DefaultHasher` and `StdRng` are reproducible only
/// across same-version builds, so a toolchain or `rand` upgrade
/// between report and apply could shift the partition. The CLI
/// workflow runs both passes from the same binary, so that
/// boundary doesn't bite in practice.
fn sample_message_id_groups(
    folder_path: &Path,
    group_count: usize,
    samples_per_group: usize,
) -> Result<Vec<Vec<MessageId>>> {
    let group_count = group_count.max(1);
    let samples_per_group = samples_per_group.max(1);
    let target_total = samples_per_group * group_count;

    let mut seen: HashSet<MessageId> = HashSet::new();
    let mut all_ids: Vec<MessageId> = Vec::new();
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
                        all_ids.push(mid);
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

    if all_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut hasher = DefaultHasher::new();
    folder_path.hash(&mut hasher);
    let seed = hasher.finish();
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    all_ids.shuffle(&mut rng);

    all_ids.truncate(target_total);

    Ok(all_ids
        .chunks(samples_per_group)
        .map(|c| c.to_vec())
        .collect())
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
/// `samples_per_group * group_count` conditions (default
/// `DEFAULT_SAMPLE_SIZE * DEFAULT_GROUP_COUNT = 12`), and each
/// Message-ID matches at most a small number of server-side emails
/// (drafts, sent copies, plus maybe a Receipts label), so the
/// result set is bounded by the cardinality of the samples
/// themselves rather than the folder's total size. Every plausible
/// server's `maxQueryResults` cap easily absorbs that.
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
) -> (Vec<PathBuf>, HashSet<JmapMailboxId>) {
    let mut found = Vec::new();
    walk_for_maildirs(maildir_root, &mut found);
    found.sort();

    let mut orphans: Vec<PathBuf> = Vec::new();
    let mut sentinel_bound_ids: HashSet<JmapMailboxId> = HashSet::new();
    for abs_path in found {
        // Skip folders already bound via mailbox_map. The relative
        // path is what `mailbox_map.maildir_folder` stores. These
        // are tracked by the cache-side claim set (built from
        // `list_known_mailbox_ids` at the call site); no need to
        // surface their ids here too.
        if let Ok(rel) = abs_path.strip_prefix(maildir_root) {
            let rel_str = rel.to_string_lossy();
            if bound_folders.contains(rel_str.as_ref()) {
                continue;
            }
        }
        // Folders whose sentinel already names a known server
        // mailbox are bound on disk even though the cache may have
        // lost the row. Surface the claimed id so the --bind guard
        // can refuse a binding that would create a duplicate
        // sentinel. A sentinel pointing at an id the server doesn't
        // advertise is orphaned (deleted server-side or copied
        // from another account), so include those in the orphan
        // list.
        match sentinel::read(&abs_path) {
            Ok(Some(m)) if by_id.contains_key(&m.jmap_mailbox_id) => {
                sentinel_bound_ids.insert(m.jmap_mailbox_id);
            }
            Ok(_) => orphans.push(abs_path),
            Err(e) => {
                debug!(
                    "rebindfolders: sentinel read failed at {} ({e}); treating as orphan",
                    abs_path.display()
                );
                orphans.push(abs_path);
            }
        }
    }
    (orphans, sentinel_bound_ids)
}

/// Recursive maildir-tree walk. Mirrors the helper used by the
/// `status` drift report (`maildir_ops::drift::find_maildir_folders`)
/// -- both recognize a folder via the shared `store::is_maildir`
/// predicate (a `cur/` or `new/` subdir), so a maildir whose empty
/// `cur/` was removed but still holds mail in `new/` is discovered here
/// and can be rebound rather than silently skipped. Kept local because
/// the two callers want subtly different return shapes (drift wants
/// relative strings for set differencing; rebindfolders wants absolute
/// paths for `sentinel::read`).
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
        if store::is_maildir(&path) {
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

    /// With enough parseable Message-IDs to fill every group, the
    /// sampler returns exactly `group_count` groups, each of
    /// `samples_per_group` entries, and de-duplicates Message-IDs
    /// across files (multiple maildir entries sharing a Message-ID
    /// still count as one sample).
    #[test]
    fn sample_groups_partitions_when_enough_ids() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("INBOX");
        std::fs::create_dir_all(folder.join("cur")).unwrap();
        std::fs::create_dir_all(folder.join("new")).unwrap();
        for i in 0..20 {
            seed_msg(&folder, "cur", &format!("{i}.x:2,"), &format!("m{i}@x"));
        }
        // Duplicate Message-ID across two files: should count once.
        seed_msg(&folder, "new", "dup.x:2,", "m0@x");

        let groups = sample_message_id_groups(&folder, 3, 4).unwrap();
        assert_eq!(groups.len(), 3, "three groups for group_count=3");
        for g in &groups {
            assert_eq!(g.len(), 4, "samples_per_group=4");
        }
        // No Message-ID appears in two groups.
        let flat: HashSet<_> = groups.iter().flatten().cloned().collect();
        assert_eq!(flat.len(), 12, "12 unique IDs across all groups");
    }

    /// Fewer parseable Message-IDs than `samples_per_group *
    /// group_count`: the sampler returns up to `group_count` chunks
    /// of `samples_per_group` each, and the final chunk may be
    /// shorter when the source runs out.
    #[test]
    fn sample_groups_returns_partial_last_group_when_few_ids() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("INBOX");
        std::fs::create_dir_all(folder.join("cur")).unwrap();
        // Only 5 unique Message-IDs: enough to fill 1 full group of
        // 4 + 1 partial group of 1; the third group never forms.
        for i in 0..5 {
            seed_msg(&folder, "cur", &format!("{i}.x:2,"), &format!("p{i}@x"));
        }
        let groups = sample_message_id_groups(&folder, 3, 4).unwrap();
        assert_eq!(groups.len(), 2, "5 IDs fill 1 full + 1 partial group");
        assert_eq!(groups[0].len(), 4);
        assert_eq!(groups[1].len(), 1);
    }

    /// `samples_per_group = 1`: each group has exactly one sample.
    /// `group_count = 3` still yields three groups for the
    /// consensus check; the per-group "intersection" degenerates
    /// to each sample's own `mailboxIds` set, but the three-of-
    /// three agreement requirement holds.
    #[test]
    fn sample_groups_handles_samples_per_group_of_one() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("INBOX");
        std::fs::create_dir_all(folder.join("cur")).unwrap();
        for i in 0..10 {
            seed_msg(&folder, "cur", &format!("{i}.x:2,"), &format!("s{i}@x"));
        }
        let groups = sample_message_id_groups(&folder, 3, 1).unwrap();
        assert_eq!(groups.len(), 3, "three single-sample groups");
        for g in &groups {
            assert_eq!(g.len(), 1);
        }
        let flat: HashSet<_> = groups.iter().flatten().cloned().collect();
        assert_eq!(flat.len(), 3, "all three samples are distinct");
    }

    /// PRNG seeding is folder-path-deterministic: the same folder
    /// path produces the same shuffle on every invocation. This is
    /// the load-bearing property for the CLI's report-then-apply
    /// workflow.
    #[test]
    fn sample_groups_seed_is_deterministic_for_same_folder() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("INBOX");
        std::fs::create_dir_all(folder.join("cur")).unwrap();
        for i in 0..15 {
            seed_msg(&folder, "cur", &format!("{i}.x:2,"), &format!("d{i}@x"));
        }
        let a = sample_message_id_groups(&folder, 3, 4).unwrap();
        let b = sample_message_id_groups(&folder, 3, 4).unwrap();
        assert_eq!(
            a, b,
            "same folder must produce same group partition across invocations"
        );
    }

    /// `cur/` and `new/` files without parseable Message-IDs are
    /// skipped; the result is an empty Vec rather than empty groups.
    #[test]
    fn sample_groups_returns_empty_for_headerless_files() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("INBOX");
        std::fs::create_dir_all(folder.join("cur")).unwrap();
        std::fs::write(folder.join("cur").join("1.x:2,"), b"no headers here").unwrap();
        let got = sample_message_id_groups(&folder, 3, 4).unwrap();
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

        let (orphans, sentinel_bound_ids) = walk_for_orphans(root, &by_id, &bound);
        assert_eq!(orphans, vec![root.join("Lost")]);
        assert_eq!(
            sentinel_bound_ids,
            [JmapMailboxId::from("MB-ARCH")].into(),
            "Archive's sentinel pins MB-ARCH; that id is what the --bind guard \
             reads to refuse rebinding it under a different folder name"
        );
    }

    /// A folder whose empty `cur/` was removed (e.g. by an empty-dir
    /// cleanup tool) but still holds mail in `new/` is a malformed
    /// maildir, not cruft: `walk_for_orphans` must discover it via the
    /// shared `store::is_maildir` predicate so its Message-IDs can
    /// rebind it, matching what the drift report now reports.
    #[test]
    fn walk_discovers_new_only_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // Lost: new/ with mail, but no cur/ (removed externally).
        std::fs::create_dir_all(root.join("Lost").join("new")).unwrap();

        let by_id: HashMap<JmapMailboxId, &MailboxObject> = HashMap::new();
        let (orphans, _) = walk_for_orphans(root, &by_id, &HashSet::new());
        assert_eq!(
            orphans,
            vec![root.join("Lost")],
            "a new/-only maildir must be discovered as an orphan, not skipped"
        );
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

        let (orphans, sentinel_bound_ids) = walk_for_orphans(root, &by_id, &HashSet::new());
        assert_eq!(orphans, vec![root.join("Stranger")]);
        assert!(
            sentinel_bound_ids.is_empty(),
            "MB-GONE isn't in by_id so the sentinel-survives bucket stays empty"
        );
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
                remote_path: "Lost".to_string(),
                parent_jmap_mailbox_id: None,
                sample_count: 3,
                source: ResolveSource::Consensus,
            }],
            skipped: vec![],
            remote_paths: HashMap::new(),
        };
        let n = apply(&plan).unwrap();
        assert_eq!(n, 1);
        let got = sentinel::read(&target).unwrap().expect("sentinel written");
        assert_eq!(got.jmap_mailbox_id, JmapMailboxId::from("MB-LOST"));
        assert_eq!(got.server_name, "Lost");
    }

    fn mid(s: &str) -> JmapMailboxId {
        JmapMailboxId::from(s)
    }

    fn unclaimed_set(ids: &[&str]) -> HashSet<JmapMailboxId> {
        ids.iter().map(|s| mid(s)).collect()
    }

    /// Every non-empty group narrows to the same single mailbox, the
    /// rest abstain: feasibility is that single mailbox.
    #[test]
    fn feasibility_intersect_of_non_empty_groups() {
        let per_group = vec![vec![mid("MB-A")], vec![mid("MB-A")], vec![]];
        let unclaimed = unclaimed_set(&["MB-A", "MB-B"]);
        assert_eq!(
            strict_feasibility(&per_group, &unclaimed),
            vec![mid("MB-A")]
        );
    }

    /// Every per-group set is empty: feasibility is empty (no
    /// non-empty groups to derive constraints from).
    #[test]
    fn feasibility_all_empty_groups_yields_empty() {
        let per_group: Vec<Vec<JmapMailboxId>> = vec![vec![], vec![]];
        let unclaimed = unclaimed_set(&["MB-A"]);
        assert!(strict_feasibility(&per_group, &unclaimed).is_empty());
    }

    /// Intersection includes a mailbox that's already claimed in
    /// this cycle: the unclaimed filter drops it.
    #[test]
    fn feasibility_filters_out_claimed_mailboxes() {
        let per_group = vec![vec![mid("MB-A"), mid("MB-B")]];
        let unclaimed = unclaimed_set(&["MB-B"]); // MB-A is claimed
        assert_eq!(
            strict_feasibility(&per_group, &unclaimed),
            vec![mid("MB-B")]
        );
    }

    /// Two groups disagree: intersection is empty even though each
    /// group is a singleton. (This is the case the per-folder
    /// consensus check already refuses; cross-mapping would
    /// inherit the empty feasibility and contribute nothing useful.)
    #[test]
    fn feasibility_disjoint_singletons_yield_empty() {
        let per_group = vec![vec![mid("MB-A")], vec![mid("MB-B")]];
        let unclaimed = unclaimed_set(&["MB-A", "MB-B"]);
        assert!(strict_feasibility(&per_group, &unclaimed).is_empty());
    }

    /// One maildir, one feasible mailbox: trivial unique matching.
    #[test]
    fn matching_k1_unique() {
        let f = vec![vec![mid("MB-A")]];
        assert_eq!(unique_perfect_matching(&f), Some(vec![mid("MB-A")]));
    }

    /// Two maildirs with constraints that force a unique assignment:
    /// maildir 0 only feasible for MB-A, maildir 1 feasible for both
    /// but MB-A is taken, so it goes to MB-B.
    #[test]
    fn matching_k2_constraints_force_unique() {
        let f = vec![vec![mid("MB-A")], vec![mid("MB-A"), mid("MB-B")]];
        assert_eq!(
            unique_perfect_matching(&f),
            Some(vec![mid("MB-A"), mid("MB-B")])
        );
    }

    /// Two maildirs, each feasible for both of two mailboxes: two
    /// valid matchings exist, so the call refuses.
    #[test]
    fn matching_k2_two_valid_refuses() {
        let f = vec![
            vec![mid("MB-A"), mid("MB-B")],
            vec![mid("MB-A"), mid("MB-B")],
        ];
        assert_eq!(unique_perfect_matching(&f), None);
    }

    /// Two maildirs both feasible only for the same single mailbox:
    /// no perfect matching exists. Refuses.
    #[test]
    fn matching_k2_no_valid_refuses() {
        let f = vec![vec![mid("MB-A")], vec![mid("MB-A")]];
        assert_eq!(unique_perfect_matching(&f), None);
    }

    /// One maildir whose feasibility list is empty: no perfect
    /// matching exists. Refuses.
    #[test]
    fn matching_empty_feasibility_refuses() {
        let f: Vec<Vec<JmapMailboxId>> = vec![vec![]];
        assert_eq!(unique_perfect_matching(&f), None);
    }

    /// Empty input (no maildirs to match): also None. The caller is
    /// expected to short-circuit on empty `ambiguous` before this is
    /// even reached; the guard exists so the helper never panics on
    /// an unexpected input shape.
    #[test]
    fn matching_zero_maildirs_returns_none() {
        let f: Vec<Vec<JmapMailboxId>> = vec![];
        assert_eq!(unique_perfect_matching(&f), None);
    }

    /// Helper to build a `RebindFoldersPlan` for cross-mapping tests
    /// from a list of (folder_path, per_group) ambiguous skips.
    fn ambig_plan(skips: Vec<(PathBuf, Vec<Vec<JmapMailboxId>>)>) -> RebindFoldersPlan {
        RebindFoldersPlan {
            candidates: vec![],
            remote_paths: HashMap::new(),
            skipped: skips
                .into_iter()
                .map(|(p, g)| SkippedFolder {
                    folder_path: p,
                    reason: SkipReason::AmbiguousAcrossSamples { per_group: g },
                })
                .collect(),
        }
    }

    /// Set up an on-disk state DB with no `mailbox_map` rows so
    /// every server mailbox is "still unclaimed" from the cache's
    /// perspective. Returns the `TempDir` alongside the connection
    /// so the caller can keep both alive for the test's scope; the
    /// directory drops (and the file vanishes) when the binding
    /// goes out of scope.
    fn empty_db() -> (tempfile::TempDir, rusqlite::Connection) {
        let dir = tempfile::tempdir().expect("tempdir for state DB");
        let db_path = dir.path().join("state.db");
        let conn = crate::state::db::open_or_recreate(&db_path).expect("open state DB");
        (dir, conn)
    }

    /// Precondition: server mailbox count must equal on-disk maildir
    /// count. With i != j the cross-mapping pass is a no-op.
    #[test]
    fn cross_resolve_noop_when_count_mismatched() {
        let (_dir, conn) = empty_db();
        let server = [
            mb("MB-A", "A", None),
            mb("MB-B", "B", None),
            mb("MB-C", "C", None),
        ];
        let by_id: HashMap<JmapMailboxId, &MailboxObject> =
            server.iter().map(|m| (m.id.clone(), m)).collect();
        let remote_paths = crate::jmap::mailbox::build_remote_paths(&by_id);
        let mut plan = ambig_plan(vec![(
            PathBuf::from("/disk/folder1"),
            vec![vec![mid("MB-A")], vec![mid("MB-A")]],
        )]);
        // i_total = 3, n_disk = 1 (one ambiguous folder).
        cross_resolve_ambiguous(&mut plan, &conn, &by_id, &remote_paths, 1)
            .expect("no-op should not error");
        assert!(plan.candidates.is_empty());
        assert_eq!(plan.skipped.len(), 1, "ambiguous skip stays put");
    }

    /// Precondition: every skip must be AmbiguousAcrossSamples. A
    /// NoMessageIds or NoServerMatches entry breaks the bijection
    /// assumption (that folder has no server counterpart).
    #[test]
    fn cross_resolve_noop_when_non_ambiguous_skip_present() {
        let (_dir, conn) = empty_db();
        let server = [mb("MB-A", "A", None), mb("MB-B", "B", None)];
        let by_id: HashMap<JmapMailboxId, &MailboxObject> =
            server.iter().map(|m| (m.id.clone(), m)).collect();
        let remote_paths = crate::jmap::mailbox::build_remote_paths(&by_id);
        let mut plan = RebindFoldersPlan {
            candidates: vec![],
            remote_paths: HashMap::new(),
            skipped: vec![
                SkippedFolder {
                    folder_path: PathBuf::from("/disk/folder1"),
                    reason: SkipReason::AmbiguousAcrossSamples {
                        per_group: vec![vec![mid("MB-A")], vec![mid("MB-A")]],
                    },
                },
                SkippedFolder {
                    folder_path: PathBuf::from("/disk/folder2"),
                    reason: SkipReason::NoMessageIds,
                },
            ],
        };
        cross_resolve_ambiguous(&mut plan, &conn, &by_id, &remote_paths, 2)
            .expect("no-op should not error");
        assert!(plan.candidates.is_empty());
        assert_eq!(plan.skipped.len(), 2);
    }

    /// Happy path: two ambiguous maildirs, two unclaimed mailboxes,
    /// constraints force a unique matching. Both promote to
    /// candidates with source CrossMapping and the skips clear.
    #[test]
    fn cross_resolve_promotes_unique_matching_pair() {
        let (_dir, conn) = empty_db();
        let server = [mb("MB-A", "A", None), mb("MB-B", "B", None)];
        let by_id: HashMap<JmapMailboxId, &MailboxObject> =
            server.iter().map(|m| (m.id.clone(), m)).collect();
        let remote_paths = crate::jmap::mailbox::build_remote_paths(&by_id);
        // Maildir 1 narrowed to {MB-A} only; maildir 2 narrowed to
        // {MB-A, MB-B} per group. With MB-A forced to maildir 1, the
        // unique matching sends maildir 2 to MB-B.
        let mut plan = ambig_plan(vec![
            (
                PathBuf::from("/disk/folder1"),
                vec![vec![mid("MB-A")], vec![mid("MB-A")]],
            ),
            (
                PathBuf::from("/disk/folder2"),
                vec![
                    vec![mid("MB-A"), mid("MB-B")],
                    vec![mid("MB-A"), mid("MB-B")],
                ],
            ),
        ]);
        // n_disk = 2 (two ambiguous folders, no consensus binds).
        cross_resolve_ambiguous(&mut plan, &conn, &by_id, &remote_paths, 2)
            .expect("matching succeeds");
        assert_eq!(plan.candidates.len(), 2);
        assert!(plan.skipped.is_empty());
        for c in &plan.candidates {
            assert_eq!(c.source, ResolveSource::CrossMapping);
        }
        let by_path: HashMap<PathBuf, JmapMailboxId> = plan
            .candidates
            .iter()
            .map(|c| (c.folder_path.clone(), c.jmap_mailbox_id.clone()))
            .collect();
        assert_eq!(by_path[&PathBuf::from("/disk/folder1")], mid("MB-A"));
        assert_eq!(by_path[&PathBuf::from("/disk/folder2")], mid("MB-B"));
    }

    /// Non-unique matching: two ambiguous maildirs both feasible for
    /// both unclaimed mailboxes. The cross-mapping pass refuses,
    /// leaves the skips intact, and adds no candidates.
    #[test]
    fn cross_resolve_keeps_skip_when_matching_non_unique() {
        let (_dir, conn) = empty_db();
        let server = [mb("MB-A", "A", None), mb("MB-B", "B", None)];
        let by_id: HashMap<JmapMailboxId, &MailboxObject> =
            server.iter().map(|m| (m.id.clone(), m)).collect();
        let remote_paths = crate::jmap::mailbox::build_remote_paths(&by_id);
        let mut plan = ambig_plan(vec![
            (
                PathBuf::from("/disk/folder1"),
                vec![vec![mid("MB-A"), mid("MB-B")]],
            ),
            (
                PathBuf::from("/disk/folder2"),
                vec![vec![mid("MB-A"), mid("MB-B")]],
            ),
        ]);
        cross_resolve_ambiguous(&mut plan, &conn, &by_id, &remote_paths, 2)
            .expect("no-op should not error");
        assert!(plan.candidates.is_empty());
        assert_eq!(plan.skipped.len(), 2);
    }

    /// Consensus candidates from this cycle reduce the unclaimed
    /// pool. A maildir bound to A via consensus means the remaining
    /// ambiguous maildir's feasibility against {A, B} narrows to
    /// {B} alone, forcing the cross-mapping.
    #[test]
    fn cross_resolve_respects_cycle_consensus_claims() {
        let (_dir, conn) = empty_db();
        let server = [mb("MB-A", "A", None), mb("MB-B", "B", None)];
        let by_id: HashMap<JmapMailboxId, &MailboxObject> =
            server.iter().map(|m| (m.id.clone(), m)).collect();
        let remote_paths = crate::jmap::mailbox::build_remote_paths(&by_id);
        let mut plan = RebindFoldersPlan {
            candidates: vec![RebindCandidate {
                folder_path: PathBuf::from("/disk/folder1"),
                jmap_mailbox_id: mid("MB-A"),
                server_name: "A".to_string(),
                remote_path: "A".to_string(),
                parent_jmap_mailbox_id: None,
                sample_count: 4,
                source: ResolveSource::Consensus,
            }],
            remote_paths: HashMap::new(),
            skipped: vec![SkippedFolder {
                folder_path: PathBuf::from("/disk/folder2"),
                reason: SkipReason::AmbiguousAcrossSamples {
                    per_group: vec![vec![mid("MB-A"), mid("MB-B")]],
                },
            }],
        };
        // n_disk = 2 (consensus bind + ambiguous folder).
        cross_resolve_ambiguous(&mut plan, &conn, &by_id, &remote_paths, 2)
            .expect("matching succeeds");
        assert_eq!(plan.candidates.len(), 2);
        assert!(plan.skipped.is_empty());
        let promoted = plan
            .candidates
            .iter()
            .find(|c| c.source == ResolveSource::CrossMapping)
            .expect("one cross-mapping candidate");
        assert_eq!(promoted.remote_path, "B");
    }
}
