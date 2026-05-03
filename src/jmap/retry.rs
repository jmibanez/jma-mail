use std::future::Future;
use std::time::Duration;
use tracing::warn;

use jmap_client::Error as JmapError;
use jmap_client::core::error::MethodErrorType;
use jmap_client::core::set::SetErrorType;

/// Decide whether an error from a JMAP call is worth retrying.
///
/// Walks the `anyhow::Error` chain looking for a typed
/// `jmap_client::Error`; if one is present we match on the variant.
/// Falls back to a substring scan only when the chain doesn't carry a
/// typed source (e.g. test-synthesized errors or future error sources
/// not yet covered by the typed match).
pub fn is_transient_error(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(e) = cause.downcast_ref::<JmapError>() {
            return is_transient_jmap_error(e);
        }
    }
    is_transient_substring(&err.to_string())
}

fn is_transient_jmap_error(e: &JmapError) -> bool {
    match e {
        // Connection-level failures from reqwest. Stick to the two
        // unambiguously-transient classifiers; `is_request()` is a
        // catch-all that also covers redirect-policy rejections and
        // body-stream errors which are not safe to blanket-retry.
        JmapError::Transport(req) => req.is_timeout() || req.is_connect(),
        // RFC 7807 problem+json body — server gave us a structured status.
        JmapError::Problem(p) => p.status().is_some_and(is_transient_status_u32),
        // Plain HTTP failure where the body wasn't problem+json.
        // jmap-client discards the StatusCode at construction
        // (`src/client.rs:426` builds `Server(format!("{}", status))`),
        // so the only way to recover the numeric code is to parse the
        // leading token of the payload. The format is `http::StatusCode`'s
        // Display: "<u16> <reason>" — locale-stable.
        JmapError::Server(s) => is_transient_server_payload(s),
        // JMAP method-level errors (RFC 8620 §3.6.1). Note: MethodError
        // itself does not impl StdError, so it can only reach us through
        // this variant — never as a free-standing chain entry.
        JmapError::Method(m) => matches!(
            m.error(),
            MethodErrorType::ServerUnavailable
                | MethodErrorType::ServerFail
                | MethodErrorType::ServerPartialFail
        ),
        // Set-level errors surface from helpers that escalate per-row
        // notUpdated/notCreated/notDestroyed entries.
        JmapError::Set(set_err) => matches!(
            set_err.error(),
            SetErrorType::RateLimit | SetErrorType::OverQuota
        ),
        // Parse and Internal are local invariant failures — retrying
        // won't help.
        _ => false,
    }
}

fn is_transient_status_u32(code: u32) -> bool {
    code == 429 || (500..600).contains(&code)
}

fn is_transient_server_payload(s: &str) -> bool {
    s.split_whitespace()
        .next()
        .and_then(|t| t.parse::<u16>().ok())
        .is_some_and(|code| code == 429 || (500..600).contains(&code))
}

/// Last-resort substring scan for errors whose chain doesn't carry a
/// typed `jmap_client::Error`. Covers the historical surface (numeric
/// "status NNN" formats some callers used) and lets test-synthesized
/// errors stay readable.
fn is_transient_substring(s: &str) -> bool {
    s.contains("Request failed: Limit")
        || s.contains("status 429")
        || s.contains("status 502")
        || s.contains("status 503")
        || s.contains("status 504")
        || s.contains("rateLimit")
        || s.contains("timed out")
        || s.contains("connection reset")
        || s.contains("connection closed")
}

/// Retry an async operation with exponential backoff (500ms, 1s, 2s, 4s,
/// 8s) on transient errors. Hard errors propagate immediately.
pub async fn with_retry<F, Fut, T>(label: &str, mut f: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let max_attempts = 5;
    let mut backoff = Duration::from_millis(500);
    let cap = Duration::from_secs(8);

    for attempt in 1..=max_attempts {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < max_attempts && is_transient_error(&e) => {
                // {:#} renders the full anyhow chain joined by ": ", so
                // logs still surface the underlying jmap-client message
                // (e.g. "Failed to download blob B-xyz: Server failed:
                // 503 Service Unavailable") even though .context() only
                // shows the top layer at {}.
                warn!(
                    "{}: transient error ({:#}); retry {}/{} in {:?}",
                    label, e, attempt, max_attempts, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(cap);
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("with_retry loop exited without returning")
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use jmap_client::core::error::MethodError;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn method_err(t: MethodErrorType) -> JmapError {
        JmapError::Method(MethodError { p_type: t })
    }

    // -- Substring fallback path (no typed source in the chain) --

    #[test]
    fn fallback_classifies_503_substring_as_transient() {
        let e = anyhow::anyhow!("Request to https://api/jmap/ failed with status 503");
        assert!(is_transient_error(&e));
    }

    #[test]
    fn fallback_classifies_504_substring_as_transient() {
        let e = anyhow::anyhow!("upstream returned status 504");
        assert!(is_transient_error(&e));
    }

    #[test]
    fn fallback_classifies_jmap_limit_substring_as_transient() {
        let e = anyhow::anyhow!("Request failed: Limit (rateLimit)");
        assert!(is_transient_error(&e));
    }

    #[test]
    fn fallback_classifies_404_substring_as_hard() {
        let e = anyhow::anyhow!("Request to / failed with status 404");
        assert!(!is_transient_error(&e));
    }

    // -- Typed jmap-client error path (real Error variants behind .context) --

    #[test]
    fn typed_classifies_server_503_as_transient() {
        // Mirror what jmap-client builds at client.rs:426 for a non-2xx
        // response without `Content-Type: application/problem+json`.
        let jmap_err = JmapError::Server("503 Service Unavailable".to_string());
        let e: anyhow::Error = Err::<(), _>(jmap_err)
            .context("Failed to download blob B-xyz")
            .unwrap_err();
        assert!(is_transient_error(&e));
    }

    #[test]
    fn typed_classifies_server_429_as_transient() {
        let jmap_err = JmapError::Server("429 Too Many Requests".to_string());
        let e: anyhow::Error = Err::<(), _>(jmap_err).context("Email/get").unwrap_err();
        assert!(is_transient_error(&e));
    }

    #[test]
    fn typed_classifies_server_404_as_hard() {
        let jmap_err = JmapError::Server("404 Not Found".to_string());
        let e: anyhow::Error = Err::<(), _>(jmap_err).context("Email/get").unwrap_err();
        assert!(!is_transient_error(&e));
    }

    #[test]
    fn typed_classifies_method_server_unavailable_as_transient() {
        let e: anyhow::Error = Err::<(), _>(method_err(MethodErrorType::ServerUnavailable))
            .context("Email/get")
            .unwrap_err();
        assert!(is_transient_error(&e));
    }

    #[test]
    fn typed_classifies_method_server_fail_as_transient() {
        let e: anyhow::Error = Err::<(), _>(method_err(MethodErrorType::ServerFail))
            .context("Email/set")
            .unwrap_err();
        assert!(is_transient_error(&e));
    }

    #[test]
    fn typed_classifies_method_unknown_as_hard() {
        let e: anyhow::Error = Err::<(), _>(method_err(MethodErrorType::UnknownMethod))
            .context("Email/get")
            .unwrap_err();
        assert!(!is_transient_error(&e));
    }

    // -- with_retry behavior --

    #[tokio::test(start_paused = true)]
    async fn retries_then_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = calls.clone();
        let result: anyhow::Result<u32> = with_retry("test", move || {
            let calls = calls_c.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(anyhow::anyhow!("status 503"))
                } else {
                    Ok(42)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn propagates_hard_error_immediately() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = calls.clone();
        let result: anyhow::Result<()> = with_retry("test", move || {
            let calls = calls_c.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(anyhow::anyhow!("status 404 not found"))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_max_attempts() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = calls.clone();
        let result: anyhow::Result<()> = with_retry("test", move || {
            let calls = calls_c.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(anyhow::anyhow!("status 503"))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 5);
    }
}
