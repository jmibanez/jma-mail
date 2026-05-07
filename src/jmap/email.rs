use anyhow::{Context, Result};
use jmap_client::Error as JmapError;
use jmap_client::client::Client;
use jmap_client::core::error::MethodErrorType;
use jmap_client::email;
use std::collections::HashMap;
use tracing::{debug, info};

use crate::ids::{JmapBlobId, JmapEmailId, JmapMailboxId, JmapThreadId, MessageId};
use crate::jmap::limits;
use crate::jmap::retry::with_retry;
use crate::jmap::types::{ChangesResponse, EmailObject};
use crate::sync::plan::LocalId;

/// True iff `err`'s anyhow chain carries a JMAP method-level
/// `cannotCalculateChanges`. Walks the chain and downcasts to
/// `jmap_client::Error::Method(MethodError)`, mirroring
/// `is_transient_error` in `src/jmap/retry.rs`. Pair with
/// `set_jmap_state(.., "")` to wipe the cursor and route the next
/// cycle through the initial-pull path.
pub fn is_cannot_calculate_changes(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(JmapError::Method(m)) = cause.downcast_ref::<JmapError>() {
            return matches!(m.error(), MethodErrorType::CannotCalculateChanges);
        }
    }
    false
}

/// Properties we request for Email/get calls.
fn email_properties() -> Vec<email::Property> {
    vec![
        email::Property::Id,
        email::Property::BlobId,
        email::Property::ThreadId,
        email::Property::MailboxIds,
        email::Property::Keywords,
        email::Property::MessageId,
    ]
}

/// Fetch emails by IDs using the request builder.
pub async fn get_by_ids(client: &Client, ids: &[JmapEmailId]) -> Result<Vec<EmailObject>> {
    with_retry("Email/get", || async {
        let mut request = client.build();
        let get_request = request.get_email().account_id(client.default_account_id());
        get_request.ids(ids.iter().map(String::from));
        get_request.properties(email_properties());

        let response = request.send().await.context("Failed to fetch emails")?;

        let email_response = response
            .unwrap_method_responses()
            .pop()
            .context("No response for email get")?;

        let get_response = email_response
            .unwrap_get_email()
            .context("Failed to parse email response")?;

        Ok(get_response.list().iter().map(parse_email_object).collect())
    })
    .await
}

/// Query all email IDs in a mailbox, paginated. `folder_name` is used
/// only for log readability — the JMAP request identifies the mailbox
/// by `mailbox_id`.
pub async fn query_mailbox(
    client: &Client,
    mailbox_id: &str,
    folder_name: &str,
) -> Result<Vec<JmapEmailId>> {
    let mut all_ids = Vec::new();
    let mut position: usize = 0;
    let page_size: usize = 100;

    loop {
        let ids: Vec<JmapEmailId> = with_retry("Email/query", || async {
            let mut request = client.build();
            let query = request
                .query_email()
                .account_id(client.default_account_id());
            query
                .filter(email::query::Filter::in_mailbox(mailbox_id))
                .position(position as i32)
                .limit(page_size);

            let response = request.send().await.context("Failed to query emails")?;

            let query_response = response
                .unwrap_method_responses()
                .pop()
                .context("No response for email query")?;

            let result = query_response
                .unwrap_query_email()
                .context("Failed to parse email query response")?;

            Ok(result
                .ids()
                .iter()
                .map(|id| JmapEmailId::from(id.as_str()))
                .collect())
        })
        .await?;
        let count = ids.len();
        all_ids.extend(ids);

        debug!(
            "Queried {} page at position {}: got {} emails (total so far: {})",
            folder_name,
            position,
            count,
            all_ids.len()
        );

        if count < page_size {
            break;
        }

        position += page_size;
    }

    info!(
        "Queried {} email IDs from {} ({})",
        all_ids.len(),
        folder_name,
        mailbox_id
    );
    Ok(all_ids)
}

/// Fetch the current Email state by issuing Email/get with an empty id list.
/// Use this to bootstrap the state for delta sync after a full initial pull.
pub async fn get_current_state(client: &Client) -> Result<String> {
    with_retry("Email/get (state bootstrap)", || async {
        let mut request = client.build();
        let get_request = request.get_email().account_id(client.default_account_id());
        get_request.ids(Vec::<String>::new());
        get_request.properties(vec![email::Property::Id]);

        let response = request
            .send()
            .await
            .context("Failed to fetch email state")?;

        let email_response = response
            .unwrap_method_responses()
            .pop()
            .context("No response for email state get")?;

        let get_response = email_response
            .unwrap_get_email()
            .context("Failed to parse email state response")?;

        Ok(get_response.state().to_string())
    })
    .await
}

/// Fetch email changes since a given state using convenience helper.
pub async fn get_changes(client: &Client, since_state: &str) -> Result<ChangesResponse> {
    let changes = with_retry("Email/changes", || async {
        client
            .email_changes(since_state, Some(500))
            .await
            .context("Failed to fetch email changes")
    })
    .await?;

    let result = ChangesResponse {
        old_state: changes.old_state().to_string(),
        new_state: changes.new_state().to_string(),
        created: changes
            .created()
            .iter()
            .map(|id| JmapEmailId::from(id.as_str()))
            .collect(),
        updated: changes
            .updated()
            .iter()
            .map(|id| JmapEmailId::from(id.as_str()))
            .collect(),
        destroyed: changes
            .destroyed()
            .iter()
            .map(|id| JmapEmailId::from(id.as_str()))
            .collect(),
        has_more_changes: changes.has_more_changes(),
    };

    info!(
        "Email changes: {} created, {} updated, {} destroyed (state {} -> {})",
        result.created.len(),
        result.updated.len(),
        result.destroyed.len(),
        result.old_state,
        result.new_state
    );

    Ok(result)
}

/// One unit of work for `set_email_batch`. Collapses keyword changes,
/// mailbox moves, and destroys onto a single Email/set method call so
/// the server sees one transaction per cycle instead of N.
#[derive(Debug, Clone)]
pub enum EmailSetOp {
    /// Patch keywords on an email. Map is `keyword -> set?`.
    Keywords {
        email_id: JmapEmailId,
        keywords: HashMap<String, bool>,
    },
    /// Replace the email's full mailbox-id set. The caller computes
    /// the target set; `set_email_batch` issues a full-replacement
    /// `mailboxIds` update. Used both for cross-mailbox moves and
    /// (in principle) any other membership change.
    SetMailboxes {
        email_id: JmapEmailId,
        target_mailbox_ids: Vec<JmapMailboxId>,
    },
    /// Destroy an email by id.
    Destroy { email_id: JmapEmailId },
}

/// Outcome of `set_email_batch`. The two `failed_*` sets contain ids
/// the server rejected per-row (notUpdated / notDestroyed); callers
/// should suppress DB mirror for those ids.
///
/// `chain_old` / `chain_new` carry the JMAP `oldState` / `newState`
/// fields from the `Email/set` response(s). Across multiple chunks
/// (when the op list exceeds `maxObjectsInSet`), the chain is
/// considered intact iff each subsequent chunk's `oldState` matches
/// the previous chunk's `newState` -- i.e. nothing third-party landed
/// between chunks. When intact, `chain_old` is the very first chunk's
/// `oldState` and `chain_new` is the very last chunk's `newState`.
/// When the chain breaks, `chain_new` is set to `None` so the section 7.1
/// cursor ratchet at the call site won't fire.
#[derive(Debug, Default)]
pub struct EmailSetOutcome {
    pub failed_updates: std::collections::HashSet<JmapEmailId>,
    pub failed_destroys: std::collections::HashSet<JmapEmailId>,
    pub chain_old: Option<String>,
    pub chain_new: Option<String>,
}

impl EmailSetOutcome {
    /// Fold one chunk's outcome into this accumulator. Used by
    /// `set_email_batch` when the op list is split across multiple
    /// `Email/set` calls to honor `maxObjectsInSet`.
    pub(crate) fn merge(&mut self, other: EmailSetOutcome) {
        self.failed_updates.extend(other.failed_updates);
        self.failed_destroys.extend(other.failed_destroys);

        // Chain extension: each subsequent chunk's `chain_old` must
        // match the previous chunk's `chain_new` for the chain to
        // remain intact. Once broken, `chain_new` stays `None` and
        // the section 7.1 cursor ratchet at the call site no-ops.
        match (self.chain_new.take(), other.chain_old, other.chain_new) {
            (None, Some(o_old), Some(o_new)) if self.chain_old.is_none() => {
                self.chain_old = Some(o_old);
                self.chain_new = Some(o_new);
            }
            (Some(prev_new), Some(o_old), Some(o_new)) if prev_new == o_old => {
                self.chain_new = Some(o_new);
            }
            _ => {
                // Chain broke, or one side was missing states.
                // `chain_new` stays at the `None` left by `take()`.
            }
        }
    }
}

/// Apply a batch of Email/set operations, splitting across as many
/// JMAP method calls as needed to honor `maxObjectsInSet`.
///
/// JMAP allows multiple `update` and `destroy` entries in one
/// Email/set, capped server-side by `maxObjectsInSet`. Caller op
/// counts can exceed that cap on a busy cycle; we chunk via
/// `limits::max_objects_in_set` (which itself caps the server's
/// advertised value at our own ceiling) and fold per-chunk outcomes
/// into a combined result. Per-id failures land in
/// `notUpdated`/`notDestroyed` and are surfaced as warnings; they do
/// not fail the chunk. A chunk that fails after retries propagates
/// `Err`, dropping the combined outcome — already-applied chunks
/// remain on the server and are picked up by reconcile on the next
/// cycle.
///
/// Moves are emitted as a *full replacement* of `mailboxIds` (the
/// caller-supplied `target_mailbox_ids`) rather than a per-key patch.
/// Per RFC 8621 §4.1.1, `mailboxIds` is `Id[Boolean]` whose values are
/// always `true`; removing a key requires a `null` patch value (RFC
/// 8620 §5.3). jmap-client 0.4.1 cannot serialize `null` for a
/// mailboxIds patch (its patch map is typed `bool`), and Fastmail
/// correctly rejects `mailboxIds/{id}: false`. Full replacement
/// sidesteps the issue at the cost of stripping any out-of-band
/// mailbox memberships not in the target set — acceptable for
/// jma's single-mailbox-per-email model.
pub async fn set_email_batch(client: &Client, ops: &[EmailSetOp]) -> Result<EmailSetOutcome> {
    if ops.is_empty() {
        return Ok(EmailSetOutcome::default());
    }

    let chunk_size = limits::max_objects_in_set(client);
    let mut combined = EmailSetOutcome::default();
    let mut chunk_count = 0usize;

    for chunk in ops.chunks(chunk_size) {
        let chunk_outcome = with_retry("Email/set (batch)", || async {
            let mut request = client.build();
            {
                let set = request.set_email().account_id(client.default_account_id());
                for op in chunk {
                    match op {
                        EmailSetOp::Keywords { email_id, keywords } => {
                            let upd = set.update(email_id.as_ref());
                            for (kw, val) in keywords {
                                upd.keyword(kw, *val);
                            }
                        }
                        EmailSetOp::SetMailboxes {
                            email_id,
                            target_mailbox_ids,
                        } => {
                            set.update(email_id.as_ref())
                                .mailbox_ids(target_mailbox_ids.iter());
                        }
                        EmailSetOp::Destroy { email_id } => {
                            set.destroy([email_id.as_ref()]);
                        }
                    }
                }
            }

            let response = request
                .send_single::<jmap_client::core::response::EmailSetResponse>()
                .await
                .context("Email/set batch failed")?;

            let chain_old = response.old_state().map(String::from);
            // jmap-client 0.4.1 returns "" when `newState` is missing
            // from the response. See `SetResponse::new_state` at
            // https://github.com/stalwartlabs/jmap-client/blob/v0.4.1/src/core/set.rs#L255-L257
            // -- the body is `self.new_state.as_deref().unwrap_or("")`.
            // Treat the empty string as missing so a non-compliant
            // server doesn't give us a bogus chain to ratchet against.
            let chain_new = match response.new_state() {
                "" => None,
                s => Some(s.to_string()),
            };

            let mut outcome = EmailSetOutcome {
                chain_old,
                chain_new,
                ..Default::default()
            };
            if let Some(not_updated) = response.not_updated_ids() {
                for id in not_updated {
                    tracing::warn!("Email/set batch: notUpdated {}", id);
                    outcome.failed_updates.insert(id.as_str().into());
                }
            }
            if let Some(not_destroyed) = response.not_destroyed_ids() {
                for id in not_destroyed {
                    tracing::warn!("Email/set batch: notDestroyed {}", id);
                    outcome.failed_destroys.insert(id.as_str().into());
                }
            }

            Ok(outcome)
        })
        .await?;

        combined.merge(chunk_outcome);
        chunk_count += 1;
    }

    debug!(
        "Applied {} Email/set operations across {} call(s) (chunk size {})",
        ops.len(),
        chunk_count,
        chunk_size
    );
    Ok(combined)
}

/// Normalize line endings to CRLF for RFC 5322 wire format.
/// Maildir messages are typically stored with bare LF; JMAP servers
/// reject those with `invalidEmail: Message contains bare newlines`.
fn normalize_crlf(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() + input.len() / 32);
    let mut prev = 0u8;
    for &b in input {
        if b == b'\n' && prev != b'\r' {
            out.push(b'\r');
        }
        out.push(b);
        prev = b;
    }
    out
}

/// Import a raw email message (RFC 5322) into a mailbox using convenience helper.
/// `local` and `folder_name` are used only for log readability.
/// One successful Email/import: the assigned JMAP id plus the
/// `oldState`/`newState` pair from the response, used by the
/// `execute`-layer cursor ratchet to chain across all Email/import +
/// Email/set calls in the cycle. `chain_old` / `chain_new` are
/// `None` when the server returned an incomplete response (RFC 8620
/// section 5.6 says the response MUST carry `newState`; the wrapper
/// is defensive).
#[derive(Debug)]
pub struct ImportResult {
    pub email_id: JmapEmailId,
    pub chain_old: Option<String>,
    pub chain_new: Option<String>,
}

pub async fn import_email(
    client: &Client,
    raw_message: &[u8],
    mailbox_id: &str,
    folder_name: &str,
    local: &LocalId,
    keywords: &HashMap<String, bool>,
) -> Result<ImportResult> {
    let keyword_list: Vec<String> = keywords
        .iter()
        .filter(|(_, v)| **v)
        .map(|(k, _)| k.clone())
        .collect();

    let keyword_opt: Option<Vec<String>> = if keyword_list.is_empty() {
        None
    } else {
        Some(keyword_list)
    };

    let normalized = normalize_crlf(raw_message);
    let mailbox_id_owned = mailbox_id.to_string();
    let account_id = client.default_account_id().to_string();

    // Same shape as `jmap_client::email::helpers::email_import_account`,
    // but inlined here so we can read `oldState` and `newState` off
    // the response before extracting the created `Email`. The
    // upload + import pair lives inside `with_retry` to match the
    // existing transient-error budget; an upload re-try is
    // idempotent (server dedupes by blobId), so retrying both is
    // safe if wasteful.
    let result = with_retry("Email/import", || async {
        let blob_id = client
            .upload(account_id.as_str().into(), normalized.clone(), None)
            .await?
            .take_blob_id();

        let mut request = client.build();
        let import_request = request
            .import_email()
            .account_id(&account_id)
            .email(blob_id)
            .mailbox_ids([mailbox_id_owned.clone()]);
        if let Some(kw) = keyword_opt.clone() {
            import_request.keywords(kw);
        }
        let create_id = import_request.create_id();

        let mut response: jmap_client::email::import::EmailImportResponse =
            request.send_single().await?;
        let chain_old = response.old_state().map(String::from);
        let chain_new = match response.new_state() {
            "" => None,
            s => Some(s.to_string()),
        };
        let email = response.created(&create_id)?;
        let email_id = JmapEmailId::from(email.id().unwrap_or_default());

        Ok(ImportResult {
            email_id,
            chain_old,
            chain_new,
        })
    })
    .await
    .context("Failed to import email")?;

    info!(
        "Imported email from {} into {} ({}) -> JMAP {}",
        local, folder_name, mailbox_id, result.email_id
    );
    Ok(result)
}

/// Download the raw blob of an email.
pub async fn download_blob(client: &Client, blob_id: &JmapBlobId) -> Result<Vec<u8>> {
    let data = with_retry("Email/blob", || async {
        client
            .download(blob_id.as_ref())
            .await
            .with_context(|| format!("Failed to download blob {}", blob_id))
    })
    .await?;

    debug!("Downloaded blob {} ({} bytes)", blob_id, data.len());
    Ok(data)
}

fn parse_email_object(email: &jmap_client::email::Email<jmap_client::Get>) -> EmailObject {
    let id = JmapEmailId::from(email.id().unwrap_or_default());
    let blob_id = JmapBlobId::from(email.blob_id().unwrap_or_default());
    let thread_id = JmapThreadId::from(email.thread_id().unwrap_or_default());
    let message_id = email
        .message_id()
        .map(|ids| ids.iter().map(|s| MessageId::from(s.as_str())).collect());

    let mailbox_ids: HashMap<JmapMailboxId, bool> = email
        .mailbox_ids()
        .iter()
        .map(|id| (id.to_string().into(), true))
        .collect();

    let keywords: HashMap<String, bool> = email
        .keywords()
        .iter()
        .map(|kw| (kw.to_string(), true))
        .collect();

    EmailObject {
        id,
        blob_id,
        thread_id,
        mailbox_ids,
        keywords,
        message_id,
        subject: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two chunks' worth of failed-id sets merge into the union, with
    /// updates and destroys staying in their respective buckets.
    /// Pins the contract that `set_email_batch` relies on when it
    /// folds per-chunk outcomes after splitting by maxObjectsInSet.
    #[test]
    fn email_set_outcome_merge_takes_union() {
        let mut acc = EmailSetOutcome::default();
        acc.failed_updates.insert(JmapEmailId::from("a"));
        acc.failed_destroys.insert(JmapEmailId::from("d1"));

        let mut other = EmailSetOutcome::default();
        other.failed_updates.insert(JmapEmailId::from("b"));
        other.failed_updates.insert(JmapEmailId::from("a")); // duplicate
        other.failed_destroys.insert(JmapEmailId::from("d2"));

        acc.merge(other);

        assert_eq!(acc.failed_updates.len(), 2);
        assert!(acc.failed_updates.contains(&JmapEmailId::from("a")));
        assert!(acc.failed_updates.contains(&JmapEmailId::from("b")));
        assert_eq!(acc.failed_destroys.len(), 2);
        assert!(acc.failed_destroys.contains(&JmapEmailId::from("d1")));
        assert!(acc.failed_destroys.contains(&JmapEmailId::from("d2")));
    }

    fn outcome_with_chain(old: &str, new: &str) -> EmailSetOutcome {
        EmailSetOutcome {
            chain_old: Some(old.into()),
            chain_new: Some(new.into()),
            ..Default::default()
        }
    }

    #[test]
    fn merge_first_chunk_seeds_chain() {
        let mut acc = EmailSetOutcome::default();
        acc.merge(outcome_with_chain("S0", "S1"));
        assert_eq!(acc.chain_old.as_deref(), Some("S0"));
        assert_eq!(acc.chain_new.as_deref(), Some("S1"));
    }

    #[test]
    fn merge_extends_chain_when_consecutive_chunks_align() {
        let mut acc = outcome_with_chain("S0", "S1");
        acc.merge(outcome_with_chain("S1", "S2"));
        acc.merge(outcome_with_chain("S2", "S3"));
        assert_eq!(acc.chain_old.as_deref(), Some("S0"));
        assert_eq!(acc.chain_new.as_deref(), Some("S3"));
    }

    #[test]
    fn merge_breaks_chain_on_intervening_state() {
        // Third-party write between our chunks: chunk 2 sees oldState=Sx,
        // not the S1 we just left behind. The section 7.1 ratchet must NOT
        // fire; chain_new becomes None.
        let mut acc = outcome_with_chain("S0", "S1");
        acc.merge(outcome_with_chain("Sx", "Sy"));
        assert_eq!(acc.chain_old.as_deref(), Some("S0"));
        assert!(acc.chain_new.is_none());
    }

    #[test]
    fn merge_breaks_chain_on_missing_states() {
        // A chunk whose response omitted oldState/newState (or
        // jmap-client returned the empty-string default for newState)
        // also breaks the chain. Conservative: don't ratchet on
        // partial information.
        let mut acc = outcome_with_chain("S0", "S1");
        acc.merge(EmailSetOutcome::default());
        assert!(acc.chain_new.is_none());
    }

    #[test]
    fn merge_once_broken_stays_broken() {
        let mut acc = outcome_with_chain("S0", "S1");
        acc.merge(outcome_with_chain("Sx", "Sy")); // breaks
        acc.merge(outcome_with_chain("S1", "S2")); // would chain to original, but already broken
        assert!(acc.chain_new.is_none());
    }
}
