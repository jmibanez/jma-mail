use std::future::Future;
use std::time::Duration;
use tracing::warn;

/// Substring-match an error's `Display` to detect a transient HTTP / JMAP
/// failure that's worth retrying. Covers JMAP `Limit` (RFC 8620 §3.6.1),
/// HTTP 429 / 502 / 503 / 504, generic timeouts, and connection resets.
///
/// jmap-client (0.4.1) doesn't surface a typed status, so we match the
/// `Display` form. Keep this in sync with the strings the underlying
/// transport produces.
pub fn is_transient_error(err: &anyhow::Error) -> bool {
    let s = err.to_string();
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
                warn!(
                    "{}: transient error ({}); retry {}/{} in {:?}",
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn classifies_503_as_transient() {
        let e = anyhow::anyhow!("Request to https://api/jmap/ failed with status 503");
        assert!(is_transient_error(&e));
    }

    #[test]
    fn classifies_504_as_transient() {
        let e = anyhow::anyhow!("upstream returned status 504");
        assert!(is_transient_error(&e));
    }

    #[test]
    fn classifies_jmap_limit_as_transient() {
        let e = anyhow::anyhow!("Request failed: Limit (rateLimit)");
        assert!(is_transient_error(&e));
    }

    #[test]
    fn classifies_404_as_hard() {
        let e = anyhow::anyhow!("Request to / failed with status 404");
        assert!(!is_transient_error(&e));
    }

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
