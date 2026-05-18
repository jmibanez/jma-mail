use anyhow::{Context, Result};
use futures_util::stream::{self, StreamExt};
use jmap_client::Error as JmapError;
use jmap_client::blob::URLParameter;
use jmap_client::client::Client;
use jmap_client::core::error::MethodErrorType;
use jmap_client::core::session::URLPart;
use jmap_client::email;
use reqwest::Client as HttpClient;
use reqwest::header::CONTENT_TYPE;
use std::collections::HashMap;
use tracing::field::Empty;
use tracing::{Instrument, debug, info, instrument, warn};

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

/// Page size for `Email/query` pagination. JMAP defines no
/// `maxObjectsInQuery` capability, so this is a pure client-side
/// preference: the server may return any number of IDs up to this
/// limit. `query_mailbox` advances its offset by the actual returned
/// count and stops only on an empty response, so a server that
/// silently clamps below this value still paginates correctly.
const QUERY_PAGE_SIZE: usize = 1000;

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
    // Per-batch phase span so the profile layer can attribute each
    // Email/get round-trip individually. Callers (`batched_get`) issue
    // these in parallel via `buffer_unordered`, so spans overlap in
    // real time; each span's wall_ms is one batch's start-to-finish
    // duration rather than its CPU share. The aggregate JSON makes
    // visible whether batch latency is uniform or whether a tail is
    // dominating the parallel window.
    let span = tracing::info_span!(
        target: crate::profile::TARGET_PHASE,
        "fetch.get_batch",
        count = ids.len() as u64,
    );
    async move {
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

            let partitioned =
                PartitionedRows::partition(get_response.list().iter().map(parse_email_object));
            partitioned.log();
            Ok(partitioned.valid)
        })
        .await
    }
    .instrument(span)
    .await
}

/// One `Email/query` round-trip at a specific offset. `with_total`
/// asks the server to populate `total` in the response so the caller
/// can plan the remaining pages up front; subsequent pages set it
/// `false` to spare the server the count.
async fn query_page(
    client: &Client,
    mailbox_id: &str,
    folder_name: &str,
    position: usize,
    with_total: bool,
) -> Result<(Vec<JmapEmailId>, Option<usize>)> {
    let span = tracing::info_span!(
        target: crate::profile::TARGET_PHASE,
        "fetch.query_page",
        folder = %folder_name,
        position = position as u64,
        count = Empty,
    );
    async move {
        let (ids, total) = with_retry("Email/query", || async {
            let mut request = client.build();
            let query = request
                .query_email()
                .account_id(client.default_account_id());
            query
                .filter(email::query::Filter::in_mailbox(mailbox_id))
                .position(position as i32)
                .limit(QUERY_PAGE_SIZE);
            if with_total {
                query.calculate_total(true);
            }

            let response = request.send().await.context("Failed to query emails")?;

            let query_response = response
                .unwrap_method_responses()
                .pop()
                .context("No response for email query")?;

            let result = query_response
                .unwrap_query_email()
                .context("Failed to parse email query response")?;

            let ids: Vec<JmapEmailId> = result
                .ids()
                .iter()
                .map(|id| JmapEmailId::from(id.as_str()))
                .collect();
            let total = result.total();
            Ok((ids, total))
        })
        .await?;

        tracing::Span::current().record("count", ids.len() as u64);
        Ok((ids, total))
    }
    .instrument(span)
    .await
}

/// Query all email IDs in a mailbox. `folder_name` is used only for
/// log readability -- the JMAP request identifies the mailbox by
/// `mailbox_id`. `concurrency` caps the within-mailbox page fan-out
/// when the server reports `total` on the probe page; the caller is
/// responsible for combining this with its own cross-mailbox budget.
///
/// The first page is issued with `calculateTotal: true`. If the
/// server returns a total, the remaining page positions are computed
/// up front and fanned out through `buffer_unordered`. If the server
/// omits `total` (RFC 8620 section 5.5 makes it optional), the rest of the
/// pages are walked serially using the empty-page end-of-list
/// heuristic.
pub async fn query_mailbox(
    client: &Client,
    mailbox_id: &str,
    folder_name: &str,
    concurrency: usize,
) -> Result<Vec<JmapEmailId>> {
    // Per-folder phase span so the profile layer can attribute total
    // query wall-clock back to a specific folder. With cross-folder
    // fan-out in `initial_remote_state` these spans overlap in real
    // time -- each span's wall_ms is that folder's start-to-finish
    // duration, not its share of CPU. The per-page `fetch.query_page`
    // spans nested below cover individual round-trips.
    let span = tracing::info_span!(
        target: crate::profile::TARGET_PHASE,
        "fetch.query_folder",
        folder = %folder_name,
        count = Empty,
    );
    async move {
        let (first_ids, total) = query_page(client, mailbox_id, folder_name, 0, true).await?;
        let first_count = first_ids.len();
        let mut all_ids = first_ids;

        match total {
            Some(total) if total > all_ids.len() => {
                // Total known: build the remaining page positions and
                // run them through `buffer_unordered`. The server may
                // grow or shrink the result set between pages (new
                // mail, deletions); short or empty pages near the end
                // are harmless because callers dedupe by id and we
                // never assume `total` is the final count.
                let positions: Vec<usize> =
                    (all_ids.len()..total).step_by(QUERY_PAGE_SIZE).collect();
                let futures = positions.into_iter().map(|pos| async move {
                    query_page(client, mailbox_id, folder_name, pos, false).await
                });
                let mut stream = stream::iter(futures).buffer_unordered(concurrency.max(1));
                while let Some(result) = stream.next().await {
                    let (ids, _) = result?;
                    all_ids.extend(ids);
                }
            }
            Some(_) => {
                // First page already covered the full set.
            }
            None => {
                // Server didn't report total. Fall back to serial
                // pagination: advance by the actual returned count
                // and stop on the first empty page (the only
                // end-of-list signal we have without `total`).
                let mut position = first_count;
                loop {
                    let (ids, _) =
                        query_page(client, mailbox_id, folder_name, position, false).await?;
                    let count = ids.len();
                    all_ids.extend(ids);
                    if count == 0 {
                        break;
                    }
                    position += count;
                }
            }
        }

        info!(
            "Queried {} email IDs from {} ({})",
            all_ids.len(),
            folder_name,
            mailbox_id
        );
        debug!(
            "Queried {}: first_page={}, total_reported={:?}, final={}",
            folder_name,
            first_count,
            total,
            all_ids.len()
        );
        tracing::Span::current().record("count", all_ids.len() as u64);
        Ok(all_ids)
    }
    .instrument(span)
    .await
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

#[instrument(
    target = "jma::profile::blob",
    name = "blob.upload",
    skip_all,
    fields(bytes = Empty),
)]
pub async fn import_email(
    client: &Client,
    raw_message: &[u8],
    mailbox_id: &str,
    folder_name: &str,
    local: &LocalId,
    keywords: &HashMap<String, bool>,
) -> Result<ImportResult> {
    tracing::Span::current().record("bytes", raw_message.len() as u64);
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
        // Recovery shape if we hit this: the blob is uploaded
        // regardless, so next cycle's Email/changes lists the
        // message as `created` and the unknown-remote-email path
        // adopts via Message-ID anchor when the local file carries
        // one.
        let email_id = email
            .id()
            .filter(|s| !s.is_empty())
            .map(JmapEmailId::from)
            .context("Email/import response: server omitted or returned empty `id`")?;

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

/// Build a pooled HTTP client for fetching blobs against the JMAP
/// download endpoint. Constructed once per `download_batch` attempt
/// (and dropped at the end of it) so the underlying reqwest
/// connection pool is shared across the N parallel blob fetches but
/// owns no idle connections during quiet periods.
///
/// `jmap_client::Client::download` would otherwise call
/// `HttpClient::builder().build()` on every blob -- giving each
/// fetch its own zero-warm pool, so every blob paid a fresh TCP
/// handshake. That churn was hidden behind the per-file fsync floor
/// before the maildir barrier-sync swap; post-swap, the rate of new
/// TCP setups against the test fixture surfaced as transient
/// `BrokenPipe` / connection-establishment errors that the retry
/// loop was masking.
///
/// Copies the bearer-bearing default headers off the JMAP client
/// (minus Content-Type, which is wrong for a GET) and inherits its
/// request timeout. Redirect / TLS policy uses reqwest defaults --
/// jmap-client's stricter redirect-policy fields aren't exposed via
/// getters, but the session already established trust at connect
/// time and the download URL is server-advertised, so the wider
/// default is acceptable for the blob path.
pub fn build_blob_http_client(jmap: &Client) -> Result<HttpClient> {
    let mut headers = jmap.headers().clone();
    headers.remove(CONTENT_TYPE);
    HttpClient::builder()
        .timeout(jmap.timeout())
        .default_headers(headers)
        .build()
        .context("Failed to build blob-download HTTP client")
}

/// Build the download URL by walking the JMAP-advertised template
/// (parsed by jmap-client at session-connect time) and substituting
/// account id, blob id, and the spec-required `name`/`type`
/// placeholders. `name` and `type` match what
/// `jmap_client::Client::download` uses; the server doesn't care
/// about either value for the retrieval (per RFC 8620 section 6.2).
fn build_download_url(jmap: &Client, blob_id: &str) -> String {
    let account_id = jmap.default_account_id();
    let mut url = String::with_capacity(64 + account_id.len() + blob_id.len());
    for part in jmap.download_url() {
        match part {
            URLPart::Value(v) => url.push_str(v),
            URLPart::Parameter(p) => match p {
                URLParameter::AccountId => url.push_str(account_id),
                URLParameter::BlobId => url.push_str(blob_id),
                URLParameter::Name => url.push_str("none"),
                URLParameter::Type => url.push_str("application/octet-stream"),
            },
        }
    }
    url
}

/// Download the raw blob of an email through a caller-provided
/// `reqwest::Client`. Callers build the client once per batch (see
/// `build_blob_http_client`) so the pool is shared across the
/// parallel fan-out; this fn is intentionally state-free beyond the
/// passed-in handles.
///
/// The download URL is derived from the JMAP session metadata
/// `jmap_client::Client::download` uses internally
/// (`Client::download_url()` / `Client::default_account_id()`).
/// Both are zero-cost field accessors, so reading them here per
/// call beats pre-extracting them at the call site.
#[instrument(
    target = "jma::profile::blob",
    name = "blob.download",
    skip(http, jmap),
    fields(bytes = Empty),
)]
pub async fn download_blob(
    http: &HttpClient,
    jmap: &Client,
    blob_id: &JmapBlobId,
) -> Result<Vec<u8>> {
    let url = build_download_url(jmap, blob_id.as_ref());
    let data = with_retry("Email/blob", || async {
        let resp = http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("Failed to download blob {}", blob_id))?
            .error_for_status()
            .with_context(|| format!("Server error downloading blob {}", blob_id))?;
        let bytes = resp
            .bytes()
            .await
            .with_context(|| format!("Failed to read body for blob {}", blob_id))?;
        Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
    })
    .await?;

    tracing::Span::current().record("bytes", data.len() as u64);
    debug!("Downloaded blob {} ({} bytes)", blob_id, data.len());
    Ok(data)
}

/// Reject rows where the server omitted any of `id`/`blobId`/
/// `threadId`, or where they are empty -- per RFC 8621 each is a
/// required non-empty `Id`, and empty values would collide on the
/// `message_map` 1:1 invariant. Split out of `parse_email_object`
/// so the rejection contract can be unit-tested without standing
/// up a `jmap_client::email::Email<Get>`.
fn validate_required_ids(
    id: Option<&str>,
    blob_id: Option<&str>,
    thread_id: Option<&str>,
) -> Result<(JmapEmailId, JmapBlobId, JmapThreadId)> {
    let id = id
        .filter(|s| !s.is_empty())
        .map(JmapEmailId::from)
        .context("Email/get response: server omitted or returned empty `id`")?;
    let blob_id = blob_id
        .filter(|s| !s.is_empty())
        .map(JmapBlobId::from)
        .with_context(|| format!("Email/get response for {}: missing/empty `blobId`", id))?;
    let thread_id = thread_id
        .filter(|s| !s.is_empty())
        .map(JmapThreadId::from)
        .with_context(|| format!("Email/get response for {}: missing/empty `threadId`", id))?;
    Ok((id, blob_id, thread_id))
}

/// Result of partitioning an `Email/get` response's parsed rows.
/// `valid` flows into reconcile; `dropped` and `first_err` drive
/// the summary warn so the user sees something specific when a
/// server response misbehaves at scale.
struct PartitionedRows {
    valid: Vec<EmailObject>,
    dropped: usize,
    first_err: Option<String>,
}

impl PartitionedRows {
    /// Drop unparseable rows instead of failing the batch. A single
    /// empty/missing-required-id row would otherwise take down
    /// hundreds of valid rows returned in the same response.
    /// Dropped emails are simply not reconciled this cycle; the
    /// next Email/changes will list them again, and a server that
    /// has since fixed itself will deliver a parseable row.
    /// Per-row detail is logged at `debug!` here so `-vv` users see
    /// every drop reason; `log` emits one summary `warn!` per
    /// affected batch.
    fn partition(rows: impl IntoIterator<Item = Result<EmailObject>>) -> Self {
        let mut valid: Vec<EmailObject> = Vec::new();
        let mut dropped = 0usize;
        let mut first_err: Option<String> = None;
        for row in rows {
            match row {
                Ok(eo) => valid.push(eo),
                Err(e) => {
                    dropped += 1;
                    if first_err.is_none() {
                        first_err = Some(format!("{:#}", e));
                    }
                    debug!("Email/get: dropped row: {:#}", e);
                }
            }
        }
        PartitionedRows {
            valid,
            dropped,
            first_err,
        }
    }

    fn log(&self) {
        if self.dropped == 0 {
            return;
        }
        warn!(
            "Email/get: dropped {} of {} row(s) due to invalid server response \
             (first error: {}); reconcile will see the remaining {} this cycle",
            self.dropped,
            self.dropped + self.valid.len(),
            self.first_err.as_deref().unwrap_or("<unknown>"),
            self.valid.len(),
        );
    }
}

/// Parse one `Email/get` row into an `EmailObject`. Required IDs
/// are validated via `validate_required_ids`; `mailboxIds` and the
/// RFC 5322 `messageId` list are filtered of empty members.
fn parse_email_object(email: &jmap_client::email::Email<jmap_client::Get>) -> Result<EmailObject> {
    let (id, blob_id, thread_id) =
        validate_required_ids(email.id(), email.blob_id(), email.thread_id())?;
    let message_id = email.message_id().map(|ids| {
        ids.iter()
            .map(|s| s.as_str().trim())
            .filter(|s| !s.is_empty())
            .map(MessageId::from)
            .collect()
    });

    let mailbox_ids: HashMap<JmapMailboxId, bool> = email
        .mailbox_ids()
        .iter()
        .map(|id| id.to_string())
        .filter(|s| !s.is_empty())
        .map(|s| (JmapMailboxId::from(s), true))
        .collect();

    let keywords: HashMap<String, bool> = email
        .keywords()
        .iter()
        .map(|kw| (kw.to_string(), true))
        .collect();

    Ok(EmailObject {
        id,
        blob_id,
        thread_id,
        mailbox_ids,
        keywords,
        message_id,
        subject: None,
    })
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

    // -- Empty/missing ID rejection --

    /// Happy path: all three required IDs present and non-empty.
    /// Returns typed newtypes; downstream code keys on these.
    #[test]
    fn validate_required_ids_accepts_full_triple() {
        let got = validate_required_ids(Some("E1"), Some("B1"), Some("T1")).unwrap();
        assert_eq!(got.0, JmapEmailId::from("E1"));
        assert_eq!(got.1, JmapBlobId::from("B1"));
        assert_eq!(got.2, JmapThreadId::from("T1"));
    }

    /// Missing `id` is rejected -- the server has to identify the
    /// row it's describing. Without it we have nowhere to bind the
    /// `message_map` row, and `JmapEmailId::from("")` would have
    /// silently collapsed every empty-id row onto one DB key.
    #[test]
    fn validate_required_ids_rejects_missing_id() {
        let e = validate_required_ids(None, Some("B1"), Some("T1")).unwrap_err();
        assert!(
            format!("{:#}", e).contains("`id`"),
            "expected error to name the field `id`, got {:#}",
            e
        );
    }

    #[test]
    fn validate_required_ids_rejects_empty_id() {
        let e = validate_required_ids(Some(""), Some("B1"), Some("T1")).unwrap_err();
        assert!(
            format!("{:#}", e).contains("`id`"),
            "expected error to name the field `id`, got {:#}",
            e
        );
    }

    /// `blobId` is required for `Email/blob` downloads. The error
    /// surface names both the bad field and the row's `id` so the
    /// summary `warn!` can identify which row was bad without
    /// dumping the whole response.
    #[test]
    fn validate_required_ids_rejects_empty_blob_id() {
        let e = validate_required_ids(Some("E1"), Some(""), Some("T1")).unwrap_err();
        let msg = format!("{:#}", e);
        assert!(msg.contains("`blobId`"), "expected `blobId`, got {}", msg);
        assert!(msg.contains("E1"), "expected row id E1, got {}", msg);
    }

    #[test]
    fn validate_required_ids_rejects_missing_thread_id() {
        let e = validate_required_ids(Some("E1"), Some("B1"), None).unwrap_err();
        let msg = format!("{:#}", e);
        assert!(
            msg.contains("`threadId`"),
            "expected `threadId`, got {}",
            msg
        );
        assert!(msg.contains("E1"), "expected row id E1, got {}", msg);
    }

    #[test]
    fn validate_required_ids_rejects_empty_thread_id() {
        let e = validate_required_ids(Some("E1"), Some("B1"), Some("")).unwrap_err();
        let msg = format!("{:#}", e);
        assert!(
            msg.contains("`threadId`"),
            "expected `threadId`, got {}",
            msg
        );
        assert!(msg.contains("E1"), "expected row id E1, got {}", msg);
    }

    // -- Skip-with-warn batch behavior --

    fn email_obj(id: &str) -> EmailObject {
        EmailObject {
            id: JmapEmailId::from(id),
            blob_id: JmapBlobId::from(format!("blob-{id}")),
            thread_id: JmapThreadId::from(format!("thread-{id}")),
            mailbox_ids: HashMap::new(),
            keywords: HashMap::new(),
            message_id: None,
            subject: None,
        }
    }

    /// All-valid batch: every row survives, no drops, no diagnostic
    /// captured. Pins the no-op shape so a future tweak to the
    /// partition logic can't start spuriously dropping rows.
    #[test]
    fn partition_keeps_all_valid_rows() {
        let rows = vec![Ok(email_obj("A")), Ok(email_obj("B"))];
        let p = PartitionedRows::partition(rows);
        assert_eq!(p.valid.len(), 2);
        assert_eq!(p.dropped, 0);
        assert!(p.first_err.is_none());
    }

    /// Mixed batch: one bad row in the middle gets dropped, the
    /// valid rows on either side survive. A single empty/missing-
    /// required-id row must never take down the rest of the batch.
    #[test]
    fn partition_drops_bad_rows_keeps_valid() {
        let rows: Vec<Result<EmailObject>> = vec![
            Ok(email_obj("A")),
            Err(anyhow::anyhow!(
                "Email/get response for X: missing/empty `blobId`"
            )),
            Ok(email_obj("B")),
        ];
        let p = PartitionedRows::partition(rows);
        assert_eq!(p.valid.len(), 2);
        assert_eq!(p.valid[0].id, JmapEmailId::from("A"));
        assert_eq!(p.valid[1].id, JmapEmailId::from("B"));
        assert_eq!(p.dropped, 1);
        assert!(p.first_err.as_deref().unwrap().contains("`blobId`"));
    }

    /// `first_err` captures the first failure, not the last, so the
    /// summary log surfaces the earliest diagnostic. Later drops are
    /// counted but their messages stay at `debug!` level only.
    #[test]
    fn partition_captures_first_error_not_last() {
        let rows: Vec<Result<EmailObject>> = vec![
            Err(anyhow::anyhow!("first failure")),
            Err(anyhow::anyhow!("second failure")),
        ];
        let p = PartitionedRows::partition(rows);
        assert_eq!(p.dropped, 2);
        assert!(p.valid.is_empty());
        assert_eq!(p.first_err.as_deref(), Some("first failure"));
    }

    /// All-bad batch: every row dropped, valid is empty. Reconcile
    /// receives no remote_emails this cycle -- harmless but visible
    /// via the summary warn.
    #[test]
    fn partition_handles_all_bad_batch() {
        let rows: Vec<Result<EmailObject>> = vec![
            Err(anyhow::anyhow!("bad one")),
            Err(anyhow::anyhow!("bad two")),
            Err(anyhow::anyhow!("bad three")),
        ];
        let p = PartitionedRows::partition(rows);
        assert!(p.valid.is_empty());
        assert_eq!(p.dropped, 3);
        assert!(p.first_err.is_some());
    }

    /// Empty batch: nothing to partition. Pins the trivial case so
    /// the partition helper stays safe to call unconditionally.
    #[test]
    fn partition_handles_empty_batch() {
        let p = PartitionedRows::partition(Vec::<Result<EmailObject>>::new());
        assert!(p.valid.is_empty());
        assert_eq!(p.dropped, 0);
        assert!(p.first_err.is_none());
    }
}
