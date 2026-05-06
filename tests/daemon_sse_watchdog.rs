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
//!   within the 12 s test window; count stays at 1.
//! - **Current code**: watchdog negotiates down to 7 s on the first
//!   ping event; reconnects at ~8 s; count >= 2 by t=12 s.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use jmapsync::daemon::eventsource;
use jmapsync::daemon::runner::SyncTrigger;
use jmapsync::ids::JmapAccountId;
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
    // listener should narrow its watchdog to 2 + 5 = 7 s, time out
    // on the ensuing silence, sleep 1 s of initial backoff, and
    // reconnect at ~8 s. We wait 12 s -- 4 s of margin past the
    // second accept -- which keeps the test cheap on CI without
    // flaking. Pre-negotiation code would size the watchdog from
    // the requested 60 and never fire in this window.
    let listen_future =
        eventsource::listen(&url, "test-token", &account_id, 60, HashMap::new(), tx);

    let driver_future = async {
        tokio::time::sleep(Duration::from_secs(12)).await;
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
