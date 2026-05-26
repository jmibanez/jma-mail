use anyhow::{Context, Result};
use jmap_client::client::Client;
use jmap_client::mailbox;
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path};
use tracing::{debug, info};

use crate::ids::JmapMailboxId;
use crate::jmap::limits;
use crate::jmap::retry::with_retry;
use crate::jmap::types::MailboxObject;

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

/// JMAP-side hierarchy path for `mb`, root-first segments joined with
/// `/`. Used for log lines and other diagnostic output where we want
/// to show how the *server* sees the mailbox tree, distinct from
/// whatever flattened on-disk shape the maildir layout chooses. A
/// cycle in the parent chain or an unknown `parent_id` truncates the
/// walk; we render whatever ancestors we did manage to resolve. This
/// is intentionally separate from `maildir_ops::layout::resolve_folder_path`
/// even though the algorithms overlap: that helper owns the
/// layout-aware on-disk path, this one owns the protocol-side
/// display. They diverge if a user picks a layout other than `Fs` or
/// configures a non-`/` separator.
fn jmap_hierarchy_path(
    mb: &MailboxObject,
    by_id: &HashMap<&JmapMailboxId, &MailboxObject>,
) -> String {
    let mut chain: Vec<&str> = Vec::new();
    let mut seen: HashSet<&JmapMailboxId> = HashSet::new();
    let mut cur: &MailboxObject = mb;
    loop {
        if !seen.insert(&cur.id) {
            break;
        }
        chain.push(&cur.name);
        match &cur.parent_id {
            None => break,
            Some(pid) => match by_id.get(pid) {
                Some(parent) => cur = *parent,
                None => break,
            },
        }
    }
    chain.reverse();
    chain.join("/")
}

/// Decide whether `mb` matches any entry in the user's configured
/// mailbox list. An empty list means "sync everything".
///
/// `INBOX` is treated as a magic alias for the JMAP inbox role -- this
/// is the IMAP convention and matches what most users expect when they
/// see "INBOX" in a config file. Other entries match by name, exactly
/// or case-insensitively depending on `case_insensitive`.
///
/// The match walks the `parent_id` chain: a config entry that names
/// the mailbox itself or any of its ancestors includes the mailbox.
/// So `mailboxes = ["[Airmail]"]` picks up `[Airmail]` itself plus
/// every descendant under it, matching the "select this folder and
/// its subfolders" intuition users get from mbsync's pattern
/// directives. To exclude a specific descendant the user would need
/// a finer filter (not yet supported); today it's all-or-none per
/// subtree.
///
/// Cycle in the parent chain (forged by a malicious server) breaks
/// the walk and returns `false`. The cycle is also caught with a
/// clearer error inside `maildir_ops::layout::resolve_folder_path`,
/// which runs immediately after this filter.
pub fn is_mailbox_synced(
    config_entries: &[String],
    mb: &MailboxObject,
    by_id: &HashMap<JmapMailboxId, &MailboxObject>,
    case_insensitive: bool,
) -> bool {
    if config_entries.is_empty() {
        return true;
    }
    let mut seen: HashSet<JmapMailboxId> = HashSet::new();
    let mut cur: &MailboxObject = mb;
    loop {
        if !seen.insert(cur.id.clone()) {
            return false;
        }
        for entry in config_entries {
            if entry == "INBOX" && cur.role.as_deref() == Some("inbox") {
                return true;
            }
            let matches_name = if case_insensitive {
                entry.eq_ignore_ascii_case(&cur.name)
            } else {
                entry == &cur.name
            };
            if matches_name {
                return true;
            }
        }
        match &cur.parent_id {
            None => return false,
            Some(pid) => match by_id.get(pid) {
                Some(parent) => cur = *parent,
                None => return false,
            },
        }
    }
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
        let by_id: HashMap<&JmapMailboxId, &MailboxObject> =
            mailboxes.iter().map(|m| (&m.id, m)).collect();
        for mb in &mailboxes {
            debug!(
                "Mailbox tree path: {} (id={})",
                jmap_hierarchy_path(mb, &by_id),
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

    fn mb(name: &str, role: Option<&str>) -> MailboxObject {
        MailboxObject {
            id: JmapMailboxId::from("mb1"),
            name: name.to_string(),
            parent_id: None,
            role: role.map(str::to_string),
            sort_order: 0,
            total_emails: 0,
            unread_emails: 0,
        }
    }

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

    /// For single-mailbox tests where the parent chain doesn't matter
    /// (no `parent_id` set, so the walk terminates immediately).
    fn empty_index() -> HashMap<JmapMailboxId, &'static MailboxObject> {
        HashMap::new()
    }

    fn build_index(mbs: &[MailboxObject]) -> HashMap<JmapMailboxId, &MailboxObject> {
        mbs.iter().map(|m| (m.id.clone(), m)).collect()
    }

    #[test]
    fn empty_config_syncs_everything() {
        let idx = empty_index();
        assert!(is_mailbox_synced(
            &[],
            &mb("Inbox", Some("inbox")),
            &idx,
            false
        ));
        assert!(is_mailbox_synced(&[], &mb("Random", None), &idx, false));
    }

    #[test]
    fn inbox_alias_matches_inbox_role() {
        let entries = vec!["INBOX".to_string()];
        let idx = empty_index();
        assert!(is_mailbox_synced(
            &entries,
            &mb("Inbox", Some("inbox")),
            &idx,
            false
        ));
        // Even if the server localized the name:
        assert!(is_mailbox_synced(
            &entries,
            &mb("Indbakke", Some("inbox")),
            &idx,
            false
        ));
    }

    #[test]
    fn inbox_alias_does_not_match_non_inbox_role() {
        let entries = vec!["INBOX".to_string()];
        let idx = empty_index();
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Inbox", Some("archive")),
            &idx,
            false
        ));
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Inbox", None),
            &idx,
            false
        ));
    }

    #[test]
    fn exact_name_match_is_case_sensitive_by_default() {
        let entries = vec!["Archive".to_string()];
        let idx = empty_index();
        assert!(is_mailbox_synced(
            &entries,
            &mb("Archive", Some("archive")),
            &idx,
            false
        ));
        assert!(!is_mailbox_synced(
            &entries,
            &mb("archive", Some("archive")),
            &idx,
            false
        ));
    }

    #[test]
    fn case_insensitive_flag_loosens_name_match() {
        let entries = vec!["archive".to_string()];
        let idx = empty_index();
        assert!(is_mailbox_synced(
            &entries,
            &mb("Archive", Some("archive")),
            &idx,
            true
        ));
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Archive", Some("archive")),
            &idx,
            false
        ));
    }

    #[test]
    fn unmatched_entry_does_not_sync() {
        let entries = vec!["Sent".to_string(), "Drafts".to_string()];
        let idx = empty_index();
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Spam", Some("junk")),
            &idx,
            false
        ));
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Spam", Some("junk")),
            &idx,
            true
        ));
    }

    /// `mailboxes = ["[Airmail]"]` matches `[Airmail]/Sent` because
    /// the walk finds the parent in the index. This is the core
    /// "select a folder and its subfolders" semantic.
    #[test]
    fn parent_match_includes_descendants() {
        let entries = vec!["[Airmail]".to_string()];
        let mbs = vec![
            MailboxObject {
                id: JmapMailboxId::from("p"),
                name: "[Airmail]".to_string(),
                parent_id: None,
                role: None,
                sort_order: 0,
                total_emails: 0,
                unread_emails: 0,
            },
            mb_child("c", "Sent", "p", None),
        ];
        let idx = build_index(&mbs);
        assert!(is_mailbox_synced(&entries, &mbs[1], &idx, false));
    }

    /// INBOX alias matches at any depth, so a child of the inbox is
    /// included by `mailboxes = ["INBOX"]` even though only its parent
    /// has the inbox role.
    #[test]
    fn inbox_alias_matches_via_ancestor() {
        let entries = vec!["INBOX".to_string()];
        let mbs = vec![
            MailboxObject {
                id: JmapMailboxId::from("i"),
                name: "Indbakke".to_string(),
                parent_id: None,
                role: Some("inbox".to_string()),
                sort_order: 0,
                total_emails: 0,
                unread_emails: 0,
            },
            mb_child("c", "Receipts", "i", None),
        ];
        let idx = build_index(&mbs);
        assert!(is_mailbox_synced(&entries, &mbs[1], &idx, false));
    }

    /// A descendant whose ancestors don't match any config entry
    /// stays excluded -- the parent walk doesn't accidentally sweep
    /// in unrelated folders.
    #[test]
    fn descendant_with_no_matching_ancestor_does_not_sync() {
        let entries = vec!["Archive".to_string()];
        let mbs = vec![
            MailboxObject {
                id: JmapMailboxId::from("p"),
                name: "[Airmail]".to_string(),
                parent_id: None,
                role: None,
                sort_order: 0,
                total_emails: 0,
                unread_emails: 0,
            },
            mb_child("c", "Sent", "p", None),
        ];
        let idx = build_index(&mbs);
        assert!(!is_mailbox_synced(&entries, &mbs[1], &idx, false));
    }

    /// A cycle in the parent chain (forged by a malicious server)
    /// terminates the walk and returns `false`. Defense-in-depth
    /// against an infinite loop here -- `resolve_folder_path` will
    /// reject the same input with a clearer error immediately after.
    #[test]
    fn parent_chain_cycle_does_not_loop() {
        let entries = vec!["Anything".to_string()];
        let mbs = vec![mb_child("a", "A", "b", None), mb_child("b", "B", "a", None)];
        let idx = build_index(&mbs);
        assert!(!is_mailbox_synced(&entries, &mbs[0], &idx, false));
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
