use anyhow::{Context, Result};
use jmap_client::client::Client;
use jmap_client::mailbox;
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path};
use tracing::{debug, info};

use crate::domain::MailboxObject;
use crate::ids::JmapMailboxId;
use crate::jmap::limits;
use crate::jmap::retry::with_retry;

/// Validate a mailbox name received from the JMAP server before it is
/// joined onto the local maildir root. A malicious or compromised
/// server can otherwise return `..`, `/etc/passwd`, or similar and
/// `Path::join` will happily escape the maildir tree (relative `..`
/// segments) or replace the base entirely (absolute paths).
///
/// `cap` is the effective byte-length cap, normally
/// `limits::max_size_mailbox_name(client)` — the server's
/// `maxSizeMailboxName` clamped against `MAX_MAILBOX_NAME_LEN`. The
/// ceiling matches the most common filesystem `NAME_MAX`; anything
/// longer could not be stored as a single maildir directory anyway.
///
/// Rejects: empty strings, NUL bytes, `/` or `\` separators, `.` or
/// `..` components, absolute paths, and names longer than `cap`.
/// JMAP nests mailboxes via `parent_id`, never via separators inside
/// `name`, so a single normal component is always the right shape
/// here.
fn validate_mailbox_name(name: &str, cap: usize) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("mailbox name is empty");
    }
    if name.len() > cap {
        anyhow::bail!(
            "mailbox name longer than {} bytes ({} bytes): {:?}",
            cap,
            name.len(),
            name
        );
    }
    if name.contains('\0') {
        anyhow::bail!("mailbox name contains NUL byte: {:?}", name);
    }
    if name.contains('/') || name.contains('\\') {
        anyhow::bail!("mailbox name contains path separator: {:?}", name);
    }

    // Defense in depth: parse with `Path::components()` and require
    // exactly one Normal component. Catches anything `contains()`
    // missed and explicitly rejects `.` / `..` / absolute paths.
    let mut comps = Path::new(name).components();
    let first = comps.next();
    if comps.next().is_some() {
        anyhow::bail!("mailbox name parses as multi-component path: {:?}", name);
    }
    match first {
        Some(Component::Normal(_)) => Ok(()),
        Some(Component::CurDir) => anyhow::bail!("mailbox name is '.': {:?}", name),
        Some(Component::ParentDir) => anyhow::bail!("mailbox name is '..': {:?}", name),
        Some(Component::RootDir) | Some(Component::Prefix(_)) => {
            anyhow::bail!("mailbox name is an absolute path: {:?}", name)
        }
        None => anyhow::bail!("mailbox name has no path component: {:?}", name),
    }
}

/// Cycle/runaway guard for the parent-chain walk -- not a real nesting
/// limit. No mailbox tree this deep could be stored on disk: the
/// `flat` layout flattens a depth-N mailbox into a single filename of
/// at least 2N-1 bytes (single-char segments + separators), so against
/// a 255-byte NAME_MAX a storable tree caps out near 128, and real
/// names blow that far shallower. 64 keeps ample headroom over any
/// realistic tree (a handful of levels) while terminating a forged
/// parent cycle quickly.
const MAX_MAILBOX_CHAIN_DEPTH: usize = 64;

/// Walk a mailbox's parent chain through `by_id` and return how many
/// ancestors it has (0 for a top-level mailbox). Used to order mailbox
/// processing shallowest-first (e.g. so a parent rename runs before a
/// descendant's in the same cycle). The walk is bounded by
/// `MAX_MAILBOX_CHAIN_DEPTH` so a forged parent cycle can't loop
/// forever.
pub(crate) fn parent_chain_depth(
    mb: &MailboxObject,
    by_id: &HashMap<JmapMailboxId, &MailboxObject>,
) -> usize {
    let mut depth = 0usize;
    let mut current = mb.parent_id.as_ref();
    while let Some(pid) = current {
        depth += 1;
        if depth > MAX_MAILBOX_CHAIN_DEPTH {
            break;
        }
        current = by_id.get(pid).and_then(|p| p.parent_id.as_ref());
    }
    depth
}

/// Join the parent's already-resolved server path with the leaf
/// `name`, falling back to the bare name when the parent is absent or
/// not yet in `remote_paths`.
pub(crate) fn compose_remote_path(
    parent_jmap_mailbox_id: Option<&JmapMailboxId>,
    name: &str,
    remote_paths: &HashMap<JmapMailboxId, String>,
) -> String {
    match parent_jmap_mailbox_id.and_then(|p| remote_paths.get(p)) {
        Some(parent_path) => format!("{}/{}", parent_path, name),
        None => name.to_string(),
    }
}

/// Build every mailbox's slash-joined server-name path (root-first),
/// keyed by id. The single definition of "JMAP mailbox tree -> remote
/// paths", so independent copies of the computation can't drift.
pub fn build_remote_paths(
    by_id: &HashMap<JmapMailboxId, &MailboxObject>,
) -> HashMap<JmapMailboxId, String> {
    // Compose parents before children so each mailbox sees its
    // parent's already-built path; sibling order doesn't affect the
    // result, so the HashMap-iteration source is fine.
    let mut ordered: Vec<&MailboxObject> = by_id.values().copied().collect();
    ordered.sort_by_key(|mb| parent_chain_depth(mb, by_id));
    let mut remote_paths: HashMap<JmapMailboxId, String> = HashMap::new();
    for mb in &ordered {
        let path = compose_remote_path(mb.parent_id.as_ref(), &mb.name, &remote_paths);
        remote_paths.insert(mb.id.clone(), path);
    }
    remote_paths
}

/// One mailbox as the sync-set selection sees it: its slash-joined
/// server-name path (root-first) and its role. Paths are unique per
/// account, so selection is expressed in terms of them.
#[derive(Clone, Copy)]
pub struct MailboxSelectionInput<'a> {
    pub path: &'a str,
    pub role: Option<&'a str>,
}

/// Whether `path`/`role` exactly matches a sync-filter entry: an entry
/// equal to the whole path (case-sensitive unless `case_insensitive`),
/// or the `INBOX` alias when the mailbox carries the inbox role. Exact
/// match only -- subtree inclusion is layered on separately by
/// `get_matching_mailbox_subtrees`.
fn matches_filter_entry(
    config_entries: &[String],
    path: &str,
    role: Option<&str>,
    case_insensitive: bool,
) -> bool {
    config_entries.iter().any(|entry| {
        if entry == "INBOX" && role == Some("inbox") {
            return true;
        }
        if case_insensitive {
            entry.eq_ignore_ascii_case(path)
        } else {
            entry == path
        }
    })
}

/// Whether `path` is a strict descendant of `ancestor` -- `ancestor`
/// followed by `/` and at least one more segment. The separator check
/// is what keeps an entry of `Foo` from sweeping in `Bar/Foo` (not a
/// descendant) or `Foobar` (shares a prefix but no path boundary).
fn is_path_under(path: &str, ancestor: &str) -> bool {
    path.len() > ancestor.len()
        && path.as_bytes()[ancestor.len()] == b'/'
        && path.starts_with(ancestor)
}

/// The paths that exactly match a sync-filter entry (see
/// `matches_filter_entry`). An empty filter selects every mailbox.
pub(crate) fn get_matching_mailboxes(
    config_entries: &[String],
    mailboxes: &[MailboxSelectionInput],
    case_insensitive: bool,
) -> HashSet<String> {
    if config_entries.is_empty() {
        return mailboxes.iter().map(|m| m.path.to_string()).collect();
    }
    mailboxes
        .iter()
        .filter(|m| matches_filter_entry(config_entries, m.path, m.role, case_insensitive))
        .map(|m| m.path.to_string())
        .collect()
}

/// The paths that lie under an already-`matched` path -- the subtrees
/// of the matched mailboxes. Path comparison is exact (both sides are
/// server paths), independent of the filter's case sensitivity.
pub(crate) fn get_matching_mailbox_subtrees(
    matched: &HashSet<String>,
    mailboxes: &[MailboxSelectionInput],
) -> HashSet<String> {
    mailboxes
        .iter()
        .filter(|m| !matched.contains(m.path))
        .filter(|m| {
            matched
                .iter()
                .any(|ancestor| is_path_under(m.path, ancestor))
        })
        .map(|m| m.path.to_string())
        .collect()
}

/// The set of mailbox paths selected by `config_entries`: the exact
/// matches, plus their subtrees when `include_subtrees`. An empty
/// filter selects every mailbox.
///
/// `INBOX` matches the inbox mailbox by role (whatever its localized
/// name); with `include_subtrees` its descendants come along like any
/// other selected subtree. A multi-segment entry (`Foo/Bar`) selects
/// exactly that path and, with subtrees on, everything beneath it --
/// so a bare `Foo` selects `Foo` and its subtree but never an
/// unrelated `Bar/Foo`.
pub fn get_selected_mailboxes(
    config_entries: &[String],
    mailboxes: &[MailboxSelectionInput],
    case_insensitive: bool,
    include_subtrees: bool,
) -> HashSet<String> {
    let mut selected = get_matching_mailboxes(config_entries, mailboxes, case_insensitive);
    if include_subtrees {
        selected.extend(get_matching_mailbox_subtrees(&selected, mailboxes));
    }
    selected
}

/// Fetch all mailboxes from the server using the convenience helper.
///
/// Sends `Mailbox/get` with `ids: null`, which per RFC 8620 §5.1
/// asks the server to return every mailbox in the account — but only
/// if the count fits inside the server's `maxObjectsInGet`. Above
/// that, the server returns a `requestTooLarge` error. We don't
/// chunk this call (which would require a `Mailbox/query` first to
/// enumerate ids and a separate state-handling story across chunks)
/// because real accounts have tens of mailboxes and every plausible
/// server's `maxObjectsInGet` is well into the hundreds. If we ever
/// hit `requestTooLarge` here, the right fix is the query+chunked-
/// get refactor, not raising a hardcoded constant.
pub async fn get_all(client: &Client) -> Result<Vec<MailboxObject>> {
    let name_cap = limits::max_size_mailbox_name(client);
    let (mailboxes, state) = with_retry("Mailbox/get", || async {
        let mut request = client.build();
        let get_request = request
            .get_mailbox()
            .account_id(client.default_account_id());
        get_request.properties([
            mailbox::Property::Id,
            mailbox::Property::Name,
            mailbox::Property::ParentId,
            mailbox::Property::Role,
            mailbox::Property::SortOrder,
            mailbox::Property::TotalEmails,
            mailbox::Property::UnreadEmails,
        ]);

        let response = request.send().await.context("Failed to fetch mailboxes")?;

        let mailbox_response = response
            .unwrap_method_responses()
            .pop()
            .context("No response for mailbox get")?;

        let get_response = mailbox_response
            .unwrap_get_mailbox()
            .context("Failed to parse mailbox response")?;

        let state = get_response.state().to_string();
        let mailboxes: Vec<MailboxObject> = get_response
            .list()
            .iter()
            .map(|mb| -> Result<MailboxObject> {
                let id = JmapMailboxId::from(mb.id().unwrap_or_default());
                let name = mb.name().unwrap_or("(unnamed)").to_string();
                validate_mailbox_name(&name, name_cap)
                    .with_context(|| format!("rejecting mailbox id={}", id))?;
                let parent_id = mb.parent_id().map(JmapMailboxId::from);
                let role = mb.role();
                let role_str = match role {
                    jmap_client::mailbox::Role::None => None,
                    other => Some(format!("{:?}", other).to_lowercase()),
                };
                let sort_order = mb.sort_order();
                let total_emails = mb.total_emails() as u64;
                let unread_emails = mb.unread_emails() as u64;

                // Per-row debug fires eagerly inside the closure so an
                // operator running with --debug to diagnose a validation
                // failure still sees the rows processed before the bad
                // one. The hierarchy-aware second pass below adds tree
                // context but only fires on a successful batch.
                match parent_id {
                    Some(ref actual_parent_id) => {
                        debug!(
                            "Child Mailbox (parent {}): {} (id={}, role={:?}, total={}, unread={})",
                            actual_parent_id, name, id, role_str, total_emails, unread_emails
                        );
                    }
                    None => {
                        debug!(
                            "Mailbox: {} (id={}, role={:?}, total={}, unread={})",
                            name, id, role_str, total_emails, unread_emails
                        );
                    }
                };

                Ok(MailboxObject {
                    id,
                    name,
                    parent_id,
                    role: role_str,
                    sort_order,
                    total_emails,
                    unread_emails,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Hierarchy-aware second pass: now that we have the whole list,
        // resolve each mailbox's `parent_id` references and log the
        // JMAP-side ancestry path. Separate from the per-row debug
        // above because the closure can't see siblings; both fire on a
        // successful batch.
        let by_id: HashMap<JmapMailboxId, &MailboxObject> =
            mailboxes.iter().map(|m| (m.id.clone(), m)).collect();
        let remote_paths = build_remote_paths(&by_id);
        for mb in &mailboxes {
            debug!(
                "Mailbox tree path: {} (id={})",
                remote_paths
                    .get(&mb.id)
                    .map(String::as_str)
                    .unwrap_or_default(),
                mb.id
            );
        }

        Ok((mailboxes, state))
    })
    .await?;

    info!("Fetched {} mailboxes (state: {})", mailboxes.len(), state);

    Ok(mailboxes)
}

/// Issue one `Mailbox/set { create }` against the server.
/// Returns the server-assigned `JmapMailboxId` on success.
///
/// `name` is validated against the same per-byte cap and
/// path-syntax rules `get_all` applies on the read side, so a
/// caller can't push a mailbox name back through the server
/// that would later fail validation when the create echoes it
/// back via `Mailbox/get`. `parent_id` is `None` for top-level
/// mailboxes; servers that nest mailboxes carry parentage in
/// `parent_id`, never in `name`.
///
/// `role` is advisory: per RFC 8621 the server may reject or
/// silently coerce role assignment, so callers should not
/// depend on the role landing on the server side.
///
/// Wrapped in `with_retry` so transient failures (rate limit,
/// network blip) get the same per-call retry treatment every
/// other JMAP call here gets. Server-side rejection
/// (`alreadyExists`, `invalidProperties`, ...) is returned as a
/// non-transient error and surfaces to the executor's per-action
/// warn-and-continue path.
pub async fn create(
    client: &Client,
    name: &str,
    parent_id: Option<&JmapMailboxId>,
    role: Option<&str>,
) -> Result<JmapMailboxId> {
    let name_cap = limits::max_size_mailbox_name(client);
    validate_mailbox_name(name, name_cap)
        .with_context(|| format!("rejecting Mailbox/set create for {:?}", name))?;
    let parsed_role = role.map(parse_role).unwrap_or(mailbox::Role::None);

    with_retry("Mailbox/set create", || async {
        let created = client
            .mailbox_create(
                name.to_string(),
                parent_id.map(|p| p.as_ref().to_string()),
                parsed_role.clone(),
            )
            .await
            .with_context(|| format!("Mailbox/set create for {:?}", name))?;
        let id = created
            .id()
            .ok_or_else(|| anyhow::anyhow!("Mailbox/set create returned no id for {:?}", name))?
            .to_string();
        info!("Created remote mailbox {:?} (id={})", name, id);
        Ok(JmapMailboxId::from(id))
    })
    .await
}

/// Issue one `Mailbox/set { update }` against the server,
/// rewriting `name` and `parentId` in a single request so a
/// pure rename and a reparent-with-rename both land atomically
/// from the client's side.
///
/// `new_name` is validated against the same per-byte cap and
/// path-syntax rules `get_all` applies on the read side.
/// `new_parent_id` is `None` for top-level mailboxes; a nested
/// move requires the parent to already exist on the server.
///
/// Wrapped in `with_retry` so transient failures get the same
/// per-call retry as every other JMAP call. Server-side
/// rejection (`alreadyExists`, `invalidProperties`, ...) lands
/// in `not_updated` and surfaces as an error from `updated()`.
pub async fn update_name_and_parent(
    client: &Client,
    id: &JmapMailboxId,
    new_name: &str,
    new_parent_id: Option<&JmapMailboxId>,
) -> Result<()> {
    let name_cap = limits::max_size_mailbox_name(client);
    validate_mailbox_name(new_name, name_cap)
        .with_context(|| format!("rejecting Mailbox/set update for {} -> {:?}", id, new_name))?;

    with_retry("Mailbox/set update", || async {
        let mut request = client.build();
        request
            .set_mailbox()
            .update(id.as_ref())
            .name(new_name.to_string())
            .parent_id(new_parent_id.map(|p| p.as_ref().to_string()));
        let mut response = request
            .send_single::<jmap_client::core::response::MailboxSetResponse>()
            .await
            .with_context(|| format!("Mailbox/set update for {}", id))?;
        response
            .updated(id.as_ref())
            .with_context(|| format!("Mailbox/set update for {} -> {:?}", id, new_name))?;
        info!(
            "Renamed remote mailbox {} -> {:?} (parent: {:?})",
            id, new_name, new_parent_id
        );
        Ok(())
    })
    .await
}

/// Issue one `Mailbox/set { destroy: [id], onDestroyRemoveEmails:
/// true }` against the server. `onDestroyRemoveEmails: true` lets
/// the server delete any emails still resident in the mailbox at
/// destroy time, which would otherwise surface as `mailboxHasEmail`
/// per RFC 8621 §2.3. Callers are responsible for emitting destroys
/// children-first; the server still rejects with `mailboxHasChild`
/// independent of the flag.
///
/// Wrapped in `with_retry` so transient failures get the same
/// per-call retry as every other JMAP call. Server-side rejection
/// (the `notDestroyed` map per RFC 8621 §5.3) surfaces from
/// `destroyed()` as an error.
pub async fn destroy(client: &Client, id: &JmapMailboxId) -> Result<()> {
    with_retry("Mailbox/set destroy", || async {
        client
            .mailbox_destroy(id.as_ref(), true)
            .await
            .with_context(|| format!("Mailbox/set destroy for {}", id))?;
        info!("Destroyed remote mailbox {}", id);
        Ok(())
    })
    .await
}

/// Map jma's lowercase `role` string to the jmap-client `Role`
/// enum. Pairs only with the well-known role names; the
/// `Role::Other(...)` string that `get_all` emits for unknown
/// server roles does not round-trip through this helper and is
/// not expected to.
fn parse_role(role: &str) -> mailbox::Role {
    match role.to_ascii_lowercase().as_str() {
        "inbox" => mailbox::Role::Inbox,
        "archive" => mailbox::Role::Archive,
        "drafts" => mailbox::Role::Drafts,
        "sent" => mailbox::Role::Sent,
        "trash" => mailbox::Role::Trash,
        "junk" => mailbox::Role::Junk,
        "important" => mailbox::Role::Important,
        other => {
            tracing::warn!(
                "Unknown JMAP role {:?}; submitting Role::None and letting the server \
                 accept the create without a role binding",
                other
            );
            mailbox::Role::None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jmap::limits::MAX_MAILBOX_NAME_LEN;

    fn mb_child(id: &str, name: &str, parent: &str, role: Option<&str>) -> MailboxObject {
        MailboxObject {
            id: JmapMailboxId::from(id),
            name: name.to_string(),
            parent_id: Some(JmapMailboxId::from(parent)),
            role: role.map(str::to_string),
            sort_order: 0,
            total_emails: 0,
            unread_emails: 0,
        }
    }

    fn build_index(mbs: &[MailboxObject]) -> HashMap<JmapMailboxId, &MailboxObject> {
        mbs.iter().map(|m| (m.id.clone(), m)).collect()
    }

    /// A top-level mailbox with a distinct id (the `mb` helper fixes
    /// the id to `mb1`, which collides when a test needs several).
    fn mb_root(id: &str, name: &str) -> MailboxObject {
        MailboxObject {
            id: JmapMailboxId::from(id),
            name: name.to_string(),
            parent_id: None,
            role: None,
            sort_order: 0,
            total_emails: 0,
            unread_emails: 0,
        }
    }

    fn path_of<'a>(paths: &'a HashMap<JmapMailboxId, String>, id: &str) -> Option<&'a str> {
        paths.get(&JmapMailboxId::from(id)).map(String::as_str)
    }

    /// Core contract: each path is its ancestors' names joined
    /// root-first with `/`. `build_remote_paths` sorts by depth
    /// internally, so the `by_id` HashMap's iteration order doesn't
    /// affect the result -- the full `A/B/C` chain only resolves if
    /// every parent is composed before its child.
    #[test]
    fn build_remote_paths_joins_ancestors_root_first() {
        let mbs = vec![
            mb_root("a", "A"),
            mb_child("b", "B", "a", None),
            mb_child("c", "C", "b", None),
        ];
        let paths = build_remote_paths(&build_index(&mbs));
        assert_eq!(path_of(&paths, "a"), Some("A"));
        assert_eq!(path_of(&paths, "b"), Some("A/B"));
        assert_eq!(path_of(&paths, "c"), Some("A/B/C"));
    }

    #[test]
    fn build_remote_paths_top_level_is_bare_name() {
        let mbs = vec![mb_root("a", "Archive")];
        let paths = build_remote_paths(&build_index(&mbs));
        assert_eq!(path_of(&paths, "a"), Some("Archive"));
    }

    /// A `parent_id` pointing at a mailbox not in the set can't be
    /// joined, so the mailbox falls back to its bare leaf name (the
    /// `compose_remote_path` None arm). Mirrors a dangling parent ref.
    #[test]
    fn build_remote_paths_unknown_parent_falls_back_to_name() {
        let mbs = vec![mb_child("c", "C", "missing", None)];
        let paths = build_remote_paths(&build_index(&mbs));
        assert_eq!(path_of(&paths, "c"), Some("C"));
    }

    /// A forged parent cycle must terminate (no infinite loop) and
    /// still yield an entry for every mailbox -- best-effort paths for
    /// input no real server would send.
    #[test]
    fn build_remote_paths_terminates_on_cycle() {
        let mbs = vec![mb_child("a", "A", "b", None), mb_child("b", "B", "a", None)];
        let paths = build_remote_paths(&build_index(&mbs));
        assert!(path_of(&paths, "a").is_some());
        assert!(path_of(&paths, "b").is_some());
    }

    #[test]
    fn compose_remote_path_joins_or_falls_back() {
        let mut paths = HashMap::new();
        paths.insert(JmapMailboxId::from("p"), "Parent".to_string());
        assert_eq!(
            compose_remote_path(Some(&JmapMailboxId::from("p")), "Child", &paths),
            "Parent/Child"
        );
        // Parent missing from the map -> bare name.
        assert_eq!(
            compose_remote_path(Some(&JmapMailboxId::from("missing")), "Child", &paths),
            "Child"
        );
        // No parent -> bare name.
        assert_eq!(compose_remote_path(None, "Top", &paths), "Top");
    }

    #[test]
    fn parent_chain_depth_counts_ancestors() {
        let mbs = vec![
            mb_root("a", "A"),
            mb_child("b", "B", "a", None),
            mb_child("c", "C", "b", None),
        ];
        let idx = build_index(&mbs);
        assert_eq!(parent_chain_depth(&mbs[0], &idx), 0);
        assert_eq!(parent_chain_depth(&mbs[1], &idx), 1);
        assert_eq!(parent_chain_depth(&mbs[2], &idx), 2);
    }

    /// The cap bounds a forged cycle so the walk terminates rather than
    /// looping forever. Also guards against the cap being removed: that
    /// would hang this test instead of returning.
    #[test]
    fn parent_chain_depth_terminates_on_cycle() {
        let mbs = vec![mb_child("a", "A", "b", None), mb_child("b", "B", "a", None)];
        let idx = build_index(&mbs);
        assert!(parent_chain_depth(&mbs[0], &idx) > MAX_MAILBOX_CHAIN_DEPTH);
    }

    /// Run `get_selected_mailboxes` over `(path, role)` pairs and
    /// return the selected paths.
    fn selected(
        entries: &[&str],
        mailboxes: &[(&str, Option<&str>)],
        case_insensitive: bool,
        include_subtrees: bool,
    ) -> HashSet<String> {
        let entries: Vec<String> = entries.iter().map(|s| s.to_string()).collect();
        let inputs: Vec<MailboxSelectionInput> = mailboxes
            .iter()
            .copied()
            .map(|(path, role)| MailboxSelectionInput { path, role })
            .collect();
        get_selected_mailboxes(&entries, &inputs, case_insensitive, include_subtrees)
    }

    fn paths(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn empty_filter_selects_every_path() {
        let got = selected(
            &[],
            &[("INBOX", Some("inbox")), ("Archive", None)],
            false,
            true,
        );
        assert_eq!(got, paths(&["INBOX", "Archive"]));
    }

    /// The inbox is matched by role even when the server localized its
    /// name, so `INBOX` selects it without a literal path match.
    #[test]
    fn inbox_alias_selects_inbox_by_role() {
        let got = selected(&["INBOX"], &[("Indbakke", Some("inbox"))], false, true);
        assert_eq!(got, paths(&["Indbakke"]));
    }

    #[test]
    fn inbox_alias_ignores_non_inbox_role() {
        let got = selected(
            &["INBOX"],
            &[("Inbox", Some("archive")), ("Other", None)],
            false,
            true,
        );
        assert!(got.is_empty());
    }

    #[test]
    fn exact_path_match_is_case_sensitive_by_default() {
        assert_eq!(
            selected(&["Archive"], &[("Archive", None)], false, true),
            paths(&["Archive"])
        );
        assert!(selected(&["Archive"], &[("archive", None)], false, true).is_empty());
    }

    #[test]
    fn case_insensitive_flag_loosens_path_match() {
        assert_eq!(
            selected(&["archive"], &[("Archive", None)], true, true),
            paths(&["Archive"])
        );
        assert!(selected(&["archive"], &[("Archive", None)], false, true).is_empty());
    }

    /// `[Airmail]` selects itself and, with subtrees on, `[Airmail]/Sent`.
    #[test]
    fn subtree_included_when_on() {
        let got = selected(
            &["[Airmail]"],
            &[("[Airmail]", None), ("[Airmail]/Sent", None)],
            false,
            true,
        );
        assert_eq!(got, paths(&["[Airmail]", "[Airmail]/Sent"]));
    }

    /// With subtrees off the user can pick a parent without its
    /// children -- the case the old all-or-none filter couldn't express.
    #[test]
    fn subtree_excluded_when_off() {
        let got = selected(&["Foo"], &[("Foo", None), ("Foo/Bar", None)], false, false);
        assert_eq!(got, paths(&["Foo"]));
    }

    /// Regression: a bare `Foo` selects `Foo` and its subtree but never
    /// an unrelated `Bar/Foo` that merely shares the leaf name -- the
    /// old per-ancestor leaf-name match swept that in.
    #[test]
    fn nested_same_name_is_not_swept_in() {
        let got = selected(
            &["Foo"],
            &[("Foo", None), ("Foo/Bar", None), ("Bar/Foo", None)],
            false,
            true,
        );
        assert_eq!(got, paths(&["Foo", "Foo/Bar"]));
    }

    /// A shared prefix without a path boundary is not a subtree: `Foo`
    /// does not select `Foobar`.
    #[test]
    fn shared_prefix_without_separator_is_not_a_subtree() {
        let got = selected(&["Foo"], &[("Foo", None), ("Foobar", None)], false, true);
        assert_eq!(got, paths(&["Foo"]));
    }

    /// Subtree inclusion attaches to a case-insensitively matched
    /// parent: the parent matches via the ci flag, and its descendant
    /// comes along through the (exact, case-preserving) subtree check.
    #[test]
    fn case_insensitive_match_still_pulls_in_subtree() {
        let got = selected(
            &["archive"],
            &[("Archive", None), ("Archive/Old", None)],
            true,
            true,
        );
        assert_eq!(got, paths(&["Archive", "Archive/Old"]));
    }

    /// A multi-segment entry selects exactly that path and its subtree.
    #[test]
    fn multi_segment_entry_selects_that_subtree() {
        let got = selected(
            &["Foo/Bar"],
            &[("Foo", None), ("Foo/Bar", None), ("Foo/Bar/Baz", None)],
            false,
            true,
        );
        assert_eq!(got, paths(&["Foo/Bar", "Foo/Bar/Baz"]));
    }

    /// The inbox's subtree comes along under `INBOX` even for a
    /// localized inbox: once the inbox is matched by role, its
    /// descendants fall out of the general subtree mechanism.
    #[test]
    fn inbox_subtree_included_for_localized_inbox() {
        let got = selected(
            &["INBOX"],
            &[("Posteingang", Some("inbox")), ("Posteingang/Sub", None)],
            false,
            true,
        );
        assert_eq!(got, paths(&["Posteingang", "Posteingang/Sub"]));
    }

    #[test]
    fn validate_mailbox_name_accepts_normal_names() {
        for name in [
            "Inbox",
            "Sent",
            "Drafts",
            "Spam",
            "All Mail",
            "Archive 2024",
            "Folder.With.Dots",
            "list-personal",
            "Indbakke",
            "受信箱",
            "(unnamed)",
        ] {
            validate_mailbox_name(name, MAX_MAILBOX_NAME_LEN)
                .unwrap_or_else(|e| panic!("expected {:?} to validate, got {}", name, e));
        }
    }

    #[test]
    fn validate_mailbox_name_rejects_empty() {
        assert!(validate_mailbox_name("", MAX_MAILBOX_NAME_LEN).is_err());
    }

    #[test]
    fn validate_mailbox_name_rejects_dot_components() {
        assert!(validate_mailbox_name(".", MAX_MAILBOX_NAME_LEN).is_err());
        assert!(validate_mailbox_name("..", MAX_MAILBOX_NAME_LEN).is_err());
    }

    #[test]
    fn validate_mailbox_name_rejects_separators() {
        for bad in [
            "../etc",
            "../../tmp/x",
            "foo/bar",
            "foo\\bar",
            "/etc/passwd",
            "/tmp/x",
            "\\\\server\\share",
        ] {
            assert!(
                validate_mailbox_name(bad, MAX_MAILBOX_NAME_LEN).is_err(),
                "expected {:?} to be rejected",
                bad
            );
        }
    }

    #[test]
    fn validate_mailbox_name_rejects_nul_byte() {
        assert!(validate_mailbox_name("foo\0bar", MAX_MAILBOX_NAME_LEN).is_err());
        assert!(validate_mailbox_name("\0", MAX_MAILBOX_NAME_LEN).is_err());
    }

    #[test]
    fn validate_mailbox_name_rejects_overlong() {
        let long = "a".repeat(MAX_MAILBOX_NAME_LEN + 1);
        assert!(validate_mailbox_name(&long, MAX_MAILBOX_NAME_LEN).is_err());
        let at_limit = "a".repeat(MAX_MAILBOX_NAME_LEN);
        assert!(validate_mailbox_name(&at_limit, MAX_MAILBOX_NAME_LEN).is_ok());
    }

    /// A server advertising a tighter cap than our 255 ceiling
    /// becomes the binding limit. Pins the new behavior introduced
    /// by routing the cap through `validate_mailbox_name` rather
    /// than hardcoding it inside the function.
    #[test]
    fn validate_mailbox_name_honors_tighter_caller_cap() {
        let name = "a".repeat(50);
        assert!(validate_mailbox_name(&name, 50).is_ok());
        assert!(validate_mailbox_name(&name, 49).is_err());
    }
}
