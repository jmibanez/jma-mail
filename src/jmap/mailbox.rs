use anyhow::{Context, Result};
use jmap_client::client::Client;
use jmap_client::mailbox;
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

/// Decide whether `mb` matches any entry in the user's configured mailbox
/// list. An empty list means "sync everything".
///
/// `INBOX` is treated as a magic alias for the JMAP inbox role -- this
/// is the IMAP convention and matches what most users expect when they
/// see "INBOX" in a config file. Other entries match by name, exactly
/// or case-insensitively depending on `case_insensitive`.
pub fn is_mailbox_synced(
    config_entries: &[String],
    mb: &MailboxObject,
    case_insensitive: bool,
) -> bool {
    if config_entries.is_empty() {
        return true;
    }
    for entry in config_entries {
        if entry == "INBOX" && mb.role.as_deref() == Some("inbox") {
            return true;
        }
        let matches_name = if case_insensitive {
            entry.eq_ignore_ascii_case(&mb.name)
        } else {
            entry == &mb.name
        };
        if matches_name {
            return true;
        }
    }
    false
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

                debug!(
                    "Mailbox: {} (id={}, role={:?}, total={}, unread={})",
                    name, id, role_str, total_emails, unread_emails
                );

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

        Ok((mailboxes, state))
    })
    .await?;

    info!("Fetched {} mailboxes (state: {})", mailboxes.len(), state);

    Ok(mailboxes)
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

    #[test]
    fn empty_config_syncs_everything() {
        assert!(is_mailbox_synced(&[], &mb("Inbox", Some("inbox")), false));
        assert!(is_mailbox_synced(&[], &mb("Random", None), false));
    }

    #[test]
    fn inbox_alias_matches_inbox_role() {
        let entries = vec!["INBOX".to_string()];
        assert!(is_mailbox_synced(
            &entries,
            &mb("Inbox", Some("inbox")),
            false
        ));
        // Even if the server localized the name:
        assert!(is_mailbox_synced(
            &entries,
            &mb("Indbakke", Some("inbox")),
            false
        ));
    }

    #[test]
    fn inbox_alias_does_not_match_non_inbox_role() {
        let entries = vec!["INBOX".to_string()];
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Inbox", Some("archive")),
            false
        ));
        assert!(!is_mailbox_synced(&entries, &mb("Inbox", None), false));
    }

    #[test]
    fn exact_name_match_is_case_sensitive_by_default() {
        let entries = vec!["Archive".to_string()];
        assert!(is_mailbox_synced(
            &entries,
            &mb("Archive", Some("archive")),
            false
        ));
        assert!(!is_mailbox_synced(
            &entries,
            &mb("archive", Some("archive")),
            false
        ));
    }

    #[test]
    fn case_insensitive_flag_loosens_name_match() {
        let entries = vec!["archive".to_string()];
        assert!(is_mailbox_synced(
            &entries,
            &mb("Archive", Some("archive")),
            true
        ));
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Archive", Some("archive")),
            false
        ));
    }

    #[test]
    fn unmatched_entry_does_not_sync() {
        let entries = vec!["Sent".to_string(), "Drafts".to_string()];
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Spam", Some("junk")),
            false
        ));
        assert!(!is_mailbox_synced(
            &entries,
            &mb("Spam", Some("junk")),
            true
        ));
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
