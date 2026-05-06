//! Integration test for the SSE listener's ping-interval watchdog.
//!
//! Wiremock can't do streaming responses, so we stand up a tiny TCP
//! server that:
//!
//! - Accepts a TCP connection.
//! - Drains the request line/headers (consumes the buffer; doesn't
//!   parse).
//! - Replies with valid SSE response headers and a single SSE comment
//!   line (`: connected`) so the response is well-formed.
//! - Holds the socket open and goes silent.
//!
//! Without the watchdog, `reqwest_eventsource` would happily await
//! bytes that never arrive and the listener would never surface the
//! dead stream. With the watchdog, `es.next()` times out after
//! `ping_interval + PING_WATCHDOG_SLACK` and the outer reconnect loop
//! opens a fresh connection -- which we observe by counting `accept`
//! calls on the test server. Pre-watchdog code: 1 connection (and
//! the listener hangs forever). Post-watchdog: at least 2 within the
//! test window.

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
                             : connected\n\n";
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

    // ping_interval = 2s, watchdog slack = 5s -> the listener should
    // bail at ~7s after each Open, sleep its 1s initial backoff, and
    // reconnect at ~8s. We wait 12s -- 4s of margin past the second
    // accept -- which keeps the test cheap on CI without flaking.
    let listen_future = eventsource::listen(&url, "test-token", &account_id, 2, HashMap::new(), tx);

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
        "expected listener to reconnect at least once after ping watchdog fired \
         (got {} connection(s); pre-watchdog code would stick at 1)",
        count
    );
}
