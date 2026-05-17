//! Integration test for the SSE listener's ping-interval watchdog
//! and the server-negotiated interval that drives it.
//!
//! Wiremock can't do streaming responses, so we stand up a tiny TCP
//! server that:
//!
//! - Accepts a TCP connection.
//! - Drains the request line/headers (consumes the buffer; doesn't
//!   parse).
//! - Replies with valid SSE response headers and a single ping event
//!   carrying `{"interval": 2}` -- the spec-blessed channel for
//!   telling clients the actual interval the server is using
//!   (RFC 8620 §7.3).
//! - Holds the socket open and goes silent.
//!
//! The listener should parse the ping payload, narrow its watchdog
//! to `interval + PING_WATCHDOG_SLACK` (= 7s), time out on the
//! ensuing silence, and reconnect via the outer backoff loop. We
//! observe the reconnect by counting `accept` calls on the test
//! server.
//!
//! Discriminator behaviour:
//!
//! - **Pre-watchdog code** (before commit 6fef921): listener hangs
//!   forever on `es.next()`; count stays at 1.
//! - **Pre-negotiation code** (6fef921..HEAD~1): watchdog sized from
//!   the *requested* `ping_interval` (60 here), so it doesn't fire
//!   within any reasonable observation window; count stays at 1.
//! - **Current code**: watchdog negotiates down to 7 s on the first
//!   ping event; reconnects at ~8 s after the first ping is
//!   parsed; count >= 2 within the post-first-accept window.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use jma_mail::daemon::eventsource;
use jma_mail::daemon::runner::SyncTrigger;
use jma_mail::ids::JmapAccountId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// Bind a listener on an OS-assigned port, return the URL the
/// listener is reachable on plus a counter of accepted connections.
/// Each accept fires off a per-connection task that ships SSE-shaped
/// headers and then sleeps; the test process exit cleans up the
/// outstanding sleeps so we don't bother joining.
async fn spawn_silent_sse_server() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);
    let connections = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&connections);

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                // Drain the request line + headers; we don't actually
                // parse them, just consume so the client's send
                // completes.
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf).await;

                let resp = b"HTTP/1.1 200 OK\r\n\
                             Content-Type: text/event-stream\r\n\
                             Cache-Control: no-cache\r\n\
                             Connection: keep-alive\r\n\
                             \r\n\
                             event: ping\n\
                             data: {\"@type\":\"Ping\",\"interval\":2}\n\
                             \n";
                let _ = stream.write_all(resp).await;
                let _ = stream.flush().await;

                // Hold the socket open well past the watchdog window
                // so the listener has to detect silence on its own.
                tokio::time::sleep(Duration::from_secs(120)).await;
            });
        }
    });

    (url, connections)
}

#[tokio::test]
async fn ping_watchdog_reconnects_against_silent_server() {
    let (url, connections) = spawn_silent_sse_server().await;
    let (tx, _rx) = mpsc::channel::<SyncTrigger>(32);
    let account_id = JmapAccountId::from("acct");

    // Request a 60 s ping interval (matching the default config).
    // The server's first event hands back `interval: 2`, so the
    // listener narrows its watchdog to 2 + 5 = 7 s, times out on
    // the ensuing silence, sleeps 1 s of initial backoff, and
    // reconnects at ~8 s after the first ping arrives.
    //
    // We anchor the deadline on the *first observed accept* rather
    // than on test start so the test stays insensitive to per-
    // process firewall / TLS-inspection overhead on the very first
    // outbound connection (e.g. Little Snitch evaluating a rule
    // for a freshly-built test binary, which can add several
    // seconds of one-off latency before the connect lands). After
    // the first connect the per-process decision is cached, so
    // the reconnect after the watchdog fires runs at native
    // speed. Pre-negotiation code would size the watchdog from
    // the requested 60 s and never fire in any reasonable window.
    let listen_future =
        eventsource::listen(&url, "test-token", &account_id, 60, HashMap::new(), tx);

    let driver_future = async {
        while connections.load(Ordering::SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // 7 s watchdog (measured from first-ping-parsed, not from
        // the first accept) + 1 s backoff + 2 s margin. The margin
        // covers scheduler jitter, tokio test-runtime timer slop,
        // and the sub-second gap between the accept the loop
        // above observes and the listener parsing the first ping
        // event the server writes immediately after. Tight enough
        // to fail loud if the watchdog doesn't fire; generous
        // enough not to masquerade transient slop as a regression.
        tokio::time::sleep(Duration::from_secs(10)).await;
        connections.load(Ordering::SeqCst)
    };

    let count = tokio::select! {
        result = listen_future => panic!("listen returned unexpectedly: {:?}", result),
        n = driver_future => n,
    };

    assert!(
        count >= 2,
        "expected listener to reconnect after watchdog fired against the \
         server-negotiated 2 s ping interval (got {} connection(s); \
         pre-negotiation code would stick at 1 for the full 60 s window)",
        count
    );
}
