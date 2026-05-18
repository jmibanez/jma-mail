use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;
use tracing::warn;

use rand::Rng;

use jmap_client::Error as JmapError;
use jmap_client::core::error::MethodErrorType;
use jmap_client::core::set::SetErrorType;

/// Tunable parameters for `with_retry`. Loaded once at startup from
/// `[sync].retry_*` config fields via `init_retry_config`; `with_retry`
/// reads from the static at call time and falls back to `Default` when
/// the static hasn't been initialized (test harness, library use).
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(8),
        }
    }
}

static RETRY_CONFIG: OnceLock<RetryConfig> = OnceLock::new();

/// Set the retry parameters used by every subsequent `with_retry`
/// call in this process. Idempotent: only the first call wins;
/// subsequent calls are no-ops and the supplied value is dropped.
/// Call once from `load_config`, before any JMAP work; do **not**
/// call from tests -- drive `do_retry` directly with a custom
/// `RetryConfig` so the process-wide `OnceLock` stays at its uninit
/// default and other tests aren't affected.
pub fn init_retry_config(config: RetryConfig) {
    RETRY_CONFIG.get_or_init(|| config);
}

fn current_config() -> RetryConfig {
    RETRY_CONFIG.get().copied().unwrap_or_default()
}

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
        // Blob downloads go through our own `reqwest::Client` (see
        // `jmap::email::download_blob`), so the typed error in the
        // chain is `reqwest::Error` directly -- not wrapped in
        // `JmapError::Transport`. Same precision, same predicates.
        if let Some(e) = cause.downcast_ref::<reqwest::Error>() {
            return is_transient_reqwest_error(e);
        }
    }
    is_transient_substring(&err.to_string())
}

fn is_transient_reqwest_error(e: &reqwest::Error) -> bool {
    e.is_timeout()
        || e.is_connect()
        || transport_source_is_dead_connection(e)
        || e.status()
            .is_some_and(|s| is_transient_status_u32(u32::from(s.as_u16())))
}

fn is_transient_jmap_error(e: &JmapError) -> bool {
    match e {
        // Connection-level failures from reqwest. We retry on:
        //
        // - `is_timeout()` -- the request never completed in time; a
        //   retry is safe because the server didn't ack.
        // - `is_connect()` -- we never even reached the server.
        // - Specific `std::io::Error` kinds in the source chain that
        //   signal the underlying TCP connection died mid-request:
        //   ConnectionReset, ConnectionAborted, BrokenPipe,
        //   UnexpectedEof. These are the resume-from-sleep / NIC-reset
        //   symptoms `is_connect()` misses because the original
        //   connect happened earlier through reqwest's pool; a retry
        //   forces a fresh connection.
        //
        // Deliberately NOT using `is_request()` as a catch-all: it
        // also covers redirect-policy rejections and body-stream
        // protocol errors that aren't safe to blanket-retry.
        JmapError::Transport(req) => {
            req.is_timeout() || req.is_connect() || transport_source_is_dead_connection(req)
        }
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

/// Walk the source chain of an error looking for a `std::io::Error`
/// whose kind says the underlying connection went away. These are
/// the kinds that show up after suspend/resume or a NIC reset, where
/// reqwest's pool handed out a connection the OS hadn't yet noticed
/// was dead and the first I/O on it failed. The next attempt will
/// open a fresh connection and succeed.
///
/// Public-shape (`&dyn Error`) so the unit tests below can drive it
/// with synthesized chains; the call site in
/// `is_transient_jmap_error` passes the `reqwest::Error` directly,
/// relying on its `Error::source` impl.
fn transport_source_is_dead_connection(err: &(dyn std::error::Error + 'static)) -> bool {
    use std::io::ErrorKind;
    let mut source = err.source();
    while let Some(s) = source {
        if let Some(io_err) = s.downcast_ref::<std::io::Error>() {
            return matches!(
                io_err.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::UnexpectedEof
            );
        }
        source = s.source();
    }
    false
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

/// Retry an async operation with exponential backoff on transient
/// errors. Hard errors propagate immediately. Tunables come from the
/// process-wide `RetryConfig` set by `init_retry_config` (defaults:
/// 5 attempts, 500ms initial, 8s cap).
pub async fn with_retry<F, Fut, T>(label: &str, f: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    do_retry(&current_config(), label, f).await
}

/// Inner retry loop, parameterized over `RetryConfig` so tests can
/// drive it with custom max-attempts / backoff values without
/// touching the process-wide `OnceLock`.
async fn do_retry<F, Fut, T>(config: &RetryConfig, label: &str, mut f: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut backoff = config.initial_backoff;

    for attempt in 1..=config.max_attempts {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < config.max_attempts && is_transient_error(&e) => {
                // Full jitter: actual sleep is uniform in [0, backoff].
                // Decorrelates retries from clients that hit the same
                // transient at the same instant; worst case is the
                // unjittered backoff, best case is near-zero. The log
                // shows the jittered value (what we actually wait) so
                // operator-facing math matches reality.
                let sleep = jitter(backoff);
                // {:#} renders the full anyhow chain joined by ": ", so
                // logs still surface the underlying jmap-client message
                // (e.g. "Failed to download blob B-xyz: Server failed:
                // 503 Service Unavailable") even though .context() only
                // shows the top layer at {}.
                warn!(
                    "{}: transient error ({:#}); retry {}/{} in {:?}",
                    label, e, attempt, config.max_attempts, sleep
                );
                tokio::time::sleep(sleep).await;
                backoff = (backoff * 2).min(config.max_backoff);
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("do_retry loop exited without returning")
}

/// Full-jitter helper: returns a random `Duration` uniformly drawn
/// from `[0, upper]` (in millisecond resolution). Pulled out so the
/// jitter math is testable in isolation.
fn jitter(upper: Duration) -> Duration {
    let upper_ms = upper.as_millis() as u64;
    let chosen_ms = rand::rng().random_range(0..=upper_ms);
    Duration::from_millis(chosen_ms)
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

    /// One-link wrapper used to synthesize an error source chain in
    /// the dead-connection tests. `reqwest::Error` is opaque so we
    /// drive `transport_source_is_dead_connection` directly with
    /// these stubs to pin the io-kind classifier without standing up
    /// a real HTTP failure.
    #[derive(Debug)]
    struct WrappedErr(std::io::Error);

    impl std::fmt::Display for WrappedErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "wrapped: {}", self.0)
        }
    }

    impl std::error::Error for WrappedErr {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    // -- Dead-connection io-kind classifier --

    #[test]
    fn dead_connection_classifies_connection_reset_as_transient() {
        let inner = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "peer reset");
        let wrapped = WrappedErr(inner);
        assert!(transport_source_is_dead_connection(&wrapped));
    }

    #[test]
    fn dead_connection_classifies_broken_pipe_as_transient() {
        let inner = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "EPIPE");
        let wrapped = WrappedErr(inner);
        assert!(transport_source_is_dead_connection(&wrapped));
    }

    #[test]
    fn dead_connection_classifies_unexpected_eof_as_transient() {
        let inner = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short read");
        let wrapped = WrappedErr(inner);
        assert!(transport_source_is_dead_connection(&wrapped));
    }

    #[test]
    fn dead_connection_classifies_connection_aborted_as_transient() {
        let inner = std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "ECONNABORTED");
        let wrapped = WrappedErr(inner);
        assert!(transport_source_is_dead_connection(&wrapped));
    }

    /// An io::Error of an unrelated kind (e.g. a permission denied
    /// while reading a config file in some hypothetical chain) must
    /// not be misclassified as a dead connection.
    #[test]
    fn dead_connection_ignores_unrelated_io_kind() {
        let inner = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "EACCES");
        let wrapped = WrappedErr(inner);
        assert!(!transport_source_is_dead_connection(&wrapped));
    }

    /// An error whose source chain holds nothing implementing
    /// `Error` (terminal node) is not a dead connection by default.
    #[test]
    fn dead_connection_returns_false_when_chain_has_no_io_error() {
        #[derive(Debug)]
        struct Plain;
        impl std::fmt::Display for Plain {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "plain")
            }
        }
        impl std::error::Error for Plain {}
        assert!(!transport_source_is_dead_connection(&Plain));
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

    /// Full jitter is bounded above by the supplied upper duration
    /// and below by zero. Hammered with 1000 samples to catch any
    /// off-by-one in the inclusive-range bound.
    #[test]
    fn jitter_stays_within_upper_bound() {
        let upper = Duration::from_millis(100);
        for _ in 0..1000 {
            let j = jitter(upper);
            assert!(j <= upper, "jitter {:?} exceeded upper {:?}", j, upper);
        }
    }

    /// `do_retry` honors a custom `max_attempts` independently of the
    /// process-wide `RETRY_CONFIG`. Pins the contract that
    /// `init_retry_config`'s value flows into the retry loop.
    #[tokio::test(start_paused = true)]
    async fn do_retry_honors_custom_max_attempts() {
        let config = RetryConfig {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = calls.clone();
        let result: anyhow::Result<()> = do_retry(&config, "test", move || {
            let calls = calls_c.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(anyhow::anyhow!("status 503"))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
