//! Wiremock-backed integration tests for `daemon::runner` reconnect
//! behavior. Two scenarios:
//!
//! - `daemon_waits_for_session_recovery_at_startup` -- the JMAP server
//!   is unreachable when the daemon starts. Pre-reconnect-commit code
//!   propagates `Err` once `with_retry` exhausts (~10s of 5xx) and the
//!   daemon process exits. Post-commit `connect_with_backoff` keeps
//!   retrying transient failures forever, so once the gate opens the
//!   initial sync runs and the cursor advances. This is the
//!   pre/post-commit discriminator.
//!
//! - `daemon_processes_event_after_jmap_failure_window` -- in-loop
//!   flow test. Drives a trigger before a `/jmap` failure window,
//!   another inside it (engine.run exhausts retries), and another
//!   after it closes. Asserts the cursor lands on the post-failure
//!   state. `reqwest::Client`'s connection pool tolerates 5xx storms
//!   so this passes pre- and post-commit; it's a regression net for
//!   the trigger pipeline crossing a failure window, not a
//!   discriminator. Marked `#[ignore]` so it stays in the commit
//!   history as a documented flow test without paying its ~15s on
//!   every `cargo test` run; opt in with
//!   `cargo test --test daemon_reconnect -- --ignored`.
//!
//!   Triggers are driven via SSE `StateChange` events against a tiny
//!   TCP server stood up alongside the wiremock JMAP mock -- the
//!   session doc's `eventSourceUrl` points the daemon's SSE listener
//!   at it, and the test driver broadcasts payloads to push the
//!   listener into firing `RemoteChange`. Earlier iterations of this
//!   test dropped sentinel files at the maildir root to fire the FS
//!   watcher, but the watcher filter has since been tightened (8ce1f0d
//!   /cur//new/ allowlist, 932e4d3 digit-prefix shape check) and the
//!   path-scan short-circuit (7fa666e) skips the JMAP cycle when scan
//!   classifies nothing -- both correct production policy, both
//!   incompatible with the old sentinel hack. SSE is the more
//!   production-faithful trigger anyway: on a quiet-FS account it's
//!   how the daemon actually wakes.
//!
//! Both tests are slow (~15-20 s each) because `with_retry`'s
//! exhaustion timing is process-wide via `init_retry_config`'s
//! `OnceLock` and can't be tuned per-test without disturbing the
//! shared default other tests rely on.
//!
//! The daemon is driven via `tokio::select!` against a separate
//! driver future; when the driver completes (cursor advance or
//! timeout), `select!` drops the daemon's outer future. The
//! `tokio::spawn`'d FS watcher and SSE listener tasks the daemon
//! created are NOT transitively dropped -- they outlive the daemon
//! future and only get torn down when `#[tokio::test]`'s runtime
//! itself shuts down. Practically harmless (each test gets its own
//! runtime, temp dir, wiremock server) but worth noting if a future
//! daemon change ever needs explicit teardown (e.g. a JMAP "goodbye"
//! call from a Drop impl).

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jma_mail::config::FolderLayout;
use jma_mail::config::{
    AccountConfig, Config, ConflictStrategy, StateConfig, SyncConfig, WatchConfig,
};
use jma_mail::daemon;
use jma_mail::state::{db, queries};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const ACCOUNT_ID: &str = "u-test-account";

#[derive(Default)]
struct MockState {
    mailboxes: Vec<MockMailbox>,
    mailbox_state: String,
    emails: Vec<MockEmail>,
    email_state: String,
    blobs: HashMap<String, Vec<u8>>,
    /// When true, /jmap returns 503 instead of dispatching.
    jmap_failing: bool,
    /// When true, /.well-known/jmap returns 503 instead of the session doc.
    session_failing: bool,
}

#[derive(Clone)]
struct MockMailbox {
    id: String,
    name: String,
    role: Option<String>,
}

#[derive(Clone)]
struct MockEmail {
    id: String,
    blob_id: String,
    thread_id: String,
    mailbox_ids: Vec<String>,
    keywords: Vec<String>,
    message_id: Option<String>,
}

fn session_doc(server_uri: &str, event_source_url: &str) -> Value {
    json!({
        "capabilities": {
            "urn:ietf:params:jmap:core": {
                "maxSizeUpload": 50_000_000u64,
                "maxConcurrentUpload": 4u64,
                "maxSizeRequest": 10_000_000u64,
                "maxConcurrentRequests": 4u64,
                "maxCallsInRequest": 16u64,
                "maxObjectsInGet": 500u64,
                "maxObjectsInSet": 500u64,
                "collationAlgorithms": ["i;ascii-numeric"],
            },
            "urn:ietf:params:jmap:mail": {
                "maxMailboxesPerEmail": 1000u64,
                "maxMailboxDepth": null,
                "maxSizeMailboxName": 255u64,
                "maxSizeAttachmentsPerEmail": 50_000_000u64,
                "emailQuerySortOptions": [],
                "mayCreateTopLevelMailbox": true,
            }
        },
        "accounts": {
            ACCOUNT_ID: {
                "name": "test@example.com",
                "isPersonal": true,
                "isReadOnly": false,
                "accountCapabilities": { "urn:ietf:params:jmap:mail": {} }
            }
        },
        "primaryAccounts": { "urn:ietf:params:jmap:mail": ACCOUNT_ID },
        "username": "test@example.com",
        "apiUrl": format!("{}/jmap", server_uri),
        "downloadUrl": format!(
            "{}/download/{{accountId}}/{{blobId}}/{{name}}?accept={{type}}",
            server_uri
        ),
        "uploadUrl": format!("{}/upload/{{accountId}}", server_uri),
        "eventSourceUrl": event_source_url.to_string(),
        "state": "abc123",
    })
}

async fn mount_session(server: &MockServer, state: Arc<Mutex<MockState>>, event_source_url: &str) {
    let server_uri = server.uri();
    let event_source_url = event_source_url.to_string();
    Mock::given(method("GET"))
        .and(path("/.well-known/jmap"))
        .respond_with(move |_req: &Request| {
            let st = state.lock().unwrap();
            if st.session_failing {
                ResponseTemplate::new(503).set_body_string("Service Unavailable")
            } else {
                ResponseTemplate::new(200)
                    .set_body_json(session_doc(&server_uri, &event_source_url))
            }
        })
        .mount(server)
        .await;
}

async fn mount_jmap(server: &MockServer, state: Arc<Mutex<MockState>>) {
    Mock::given(method("POST"))
        .and(path("/jmap"))
        .respond_with(move |req: &Request| {
            let st = state.lock().unwrap();
            if st.jmap_failing {
                return ResponseTemplate::new(503).set_body_string("Service Unavailable");
            }
            let body: Value = serde_json::from_slice(&req.body).expect("valid JSON body");
            let calls = body["methodCalls"]
                .as_array()
                .expect("methodCalls array")
                .clone();
            let mut responses = Vec::new();
            for call in calls {
                let arr = call.as_array().expect("methodCall is a triple");
                let method_name = arr[0].as_str().expect("method name").to_string();
                let args = arr[1].clone();
                let call_id = arr[2].as_str().expect("call id").to_string();
                responses.push(handle_method(&method_name, &args, &call_id, &st));
            }
            ResponseTemplate::new(200).set_body_json(json!({
                "methodResponses": responses,
                "sessionState": "session-1",
            }))
        })
        .mount(server)
        .await;
}

async fn mount_blob_downloads(server: &MockServer, state: Arc<Mutex<MockState>>) {
    Mock::given(method("GET"))
        .and(path_regex(format!(r"^/download/{}/", ACCOUNT_ID)))
        .respond_with(move |req: &Request| {
            let url = req.url.path();
            let segments: Vec<&str> = url.trim_start_matches('/').split('/').collect();
            let blob_id = segments.get(2).copied().unwrap_or_default();
            let st = state.lock().unwrap();
            match st.blobs.get(blob_id) {
                Some(bytes) => ResponseTemplate::new(200)
                    .set_body_bytes(bytes.clone())
                    .insert_header("content-type", "application/octet-stream"),
                None => ResponseTemplate::new(404).set_body_string("not found"),
            }
        })
        .mount(server)
        .await;
}

fn handle_method(name: &str, args: &Value, call_id: &str, state: &MockState) -> Value {
    match name {
        "Mailbox/get" => mailbox_get(call_id, state),
        "Email/query" => email_query(args, call_id, state),
        "Email/get" => email_get(args, call_id, state),
        "Email/changes" => email_changes(args, call_id, state),
        other => json!([
            "error",
            { "type": "unknownMethod", "description": format!("unhandled: {}", other) },
            call_id
        ]),
    }
}

fn mailbox_get(call_id: &str, state: &MockState) -> Value {
    let list: Vec<Value> = state
        .mailboxes
        .iter()
        .map(|mb| {
            json!({
                "id": mb.id,
                "name": mb.name,
                "parentId": null,
                "role": mb.role,
                "sortOrder": 0,
                "totalEmails": 0,
                "unreadEmails": 0,
                "totalThreads": 0,
                "unreadThreads": 0,
                "myRights": {
                    "mayReadItems": true, "mayAddItems": true,
                    "mayRemoveItems": true, "maySetSeen": true,
                    "maySetKeywords": true, "mayCreateChild": true,
                    "mayRename": true, "mayDelete": true, "maySubmit": true
                },
                "isSubscribed": true
            })
        })
        .collect();
    json!([
        "Mailbox/get",
        {
            "accountId": ACCOUNT_ID,
            "state": state.mailbox_state,
            "list": list,
            "notFound": []
        },
        call_id
    ])
}

fn email_query(args: &Value, call_id: &str, state: &MockState) -> Value {
    let mailbox_id = args["filter"]["inMailbox"].as_str().unwrap_or_default();
    let ids: Vec<String> = state
        .emails
        .iter()
        .filter(|e| e.mailbox_ids.iter().any(|m| m == mailbox_id))
        .map(|e| e.id.clone())
        .collect();
    json!([
        "Email/query",
        {
            "accountId": ACCOUNT_ID,
            "queryState": "q-1",
            "canCalculateChanges": false,
            "position": 0,
            "ids": ids,
            "total": state.emails.len() as i64,
            "limit": 100
        },
        call_id
    ])
}

fn email_get(args: &Value, call_id: &str, state: &MockState) -> Value {
    let empty = vec![];
    let id_list = args["ids"].as_array().unwrap_or(&empty);
    let want_all_ids = id_list.is_empty();
    let list: Vec<Value> = state
        .emails
        .iter()
        .filter(|e| want_all_ids || id_list.iter().any(|v| v.as_str() == Some(e.id.as_str())))
        .map(|e| {
            let mut mailbox_map = serde_json::Map::new();
            for m in &e.mailbox_ids {
                mailbox_map.insert(m.clone(), Value::Bool(true));
            }
            let mut keyword_map = serde_json::Map::new();
            for k in &e.keywords {
                keyword_map.insert(k.clone(), Value::Bool(true));
            }
            json!({
                "id": e.id,
                "blobId": e.blob_id,
                "threadId": e.thread_id,
                "mailboxIds": Value::Object(mailbox_map),
                "keywords": Value::Object(keyword_map),
                "messageId": e.message_id.as_ref().map(|m| vec![m.clone()]),
            })
        })
        .collect();
    json!([
        "Email/get",
        {
            "accountId": ACCOUNT_ID,
            "state": state.email_state,
            "list": list,
            "notFound": []
        },
        call_id
    ])
}

/// Naive Email/changes that reports every current email as `created`.
/// The engine dedupes via DB-known indices, so already-bound emails
/// produce no actions; only genuinely new ones drive cursor
/// advancement. Sufficient for these tests, which add one fresh
/// email per phase.
fn email_changes(args: &Value, call_id: &str, state: &MockState) -> Value {
    let since = args["sinceState"].as_str().unwrap_or_default();
    json!([
        "Email/changes",
        {
            "accountId": ACCOUNT_ID,
            "oldState": since,
            "newState": state.email_state,
            "hasMoreChanges": false,
            "created": state.emails.iter().map(|e| e.id.clone()).collect::<Vec<_>>(),
            "updated": Vec::<String>::new(),
            "destroyed": Vec::<String>::new(),
        },
        call_id
    ])
}

fn build_test_config(server_uri: &str, maildir_root: &Path, db_path: &Path) -> Config {
    Config {
        account: AccountConfig {
            email: "test@example.com".to_string(),
            token: Some("test-token".to_string()),
            // Force `session::connect` down the explicit-override path
            // so we hit the wiremock server without DNS / well-known
            // discovery.
            session_url: Some(server_uri.to_string()),
        },
        sync: SyncConfig {
            maildir_path: maildir_root.to_string_lossy().into_owned(),
            mailboxes: vec![],
            conflict_strategy: ConflictStrategy::ServerWins,
            case_insensitive_match: false,
            download_concurrency: 2,
            upload_concurrency: 2,
            retry_max_attempts: 5,
            retry_initial_backoff_ms: 500,
            retry_max_backoff_ms: 8_000,
            folder_layout: FolderLayout::Fs,
            hierarchy_separator: '/',
        },
        state: StateConfig {
            db_path: Some(db_path.to_string_lossy().into_owned()),
        },
        watch: WatchConfig {
            debounce_secs: 1,
            ping_interval: 60,
            // Disable coalescing; this test doesn't exercise it and
            // the production default would tax each trigger with a
            // 500ms wait.
            coalesce_window_ms: 0,
            self_write_ttl_secs: None,
            post_arrival_command: None,
            post_arrival_command_retries: 0,
        },
        ..Default::default()
    }
}

/// Pre-create the maildir layout so the FS watcher has a directory to
/// watch at spawn time and the daemon's first `resolve_mailboxes` has
/// the INBOX subdirs in place.
fn provision_maildir(root: &Path) {
    for sub in ["cur", "new", "tmp"] {
        std::fs::create_dir_all(root.join("INBOX").join(sub)).unwrap();
    }
}

/// Poll a predicate until it returns true or the timeout elapses.
async fn wait_until<F>(mut predicate: F, timeout: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

fn cursor_email_state(db_path: &Path) -> Option<String> {
    let conn = db::open(db_path).ok()?;
    queries::get_jmap_state(&conn, ACCOUNT_ID, "Email")
        .ok()
        .flatten()
}

fn email_msg(id: &str, n: u32) -> MockEmail {
    MockEmail {
        id: id.to_string(),
        blob_id: format!("B{}", n),
        thread_id: format!("T{}", n),
        mailbox_ids: vec!["MB-INBOX".to_string()],
        keywords: vec!["$seen".to_string()],
        message_id: Some(format!("<m{}@example.com>", n)),
    }
}

fn inbox() -> MockMailbox {
    MockMailbox {
        id: "MB-INBOX".to_string(),
        name: "INBOX".to_string(),
        role: Some("inbox".to_string()),
    }
}

/// Tiny TCP SSE server. Each accepted connection sends the SSE
/// response headers, subscribes to a `broadcast` channel, and
/// forwards every payload sent on that channel as one `event: state`
/// frame. The connection counter increments on every accept --
/// callers wait on it to barrier on "the daemon's listener has
/// (re)connected" before pushing the next payload (a payload sent to
/// a channel with no subscribers is silently lost).
///
/// wiremock can't do streaming responses, so this is a hand-rolled
/// TCP server -- same shape as `daemon_sse_watchdog.rs`'s helper, but
/// with a control channel instead of a fixed scripted payload.
async fn spawn_sse_server() -> (String, broadcast::Sender<String>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/eventsource", listener.local_addr().unwrap());
    let (event_tx, _) = broadcast::channel::<String>(16);
    let connections = Arc::new(AtomicUsize::new(0));

    let accept_tx = event_tx.clone();
    let accept_count = connections.clone();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let mut event_rx = accept_tx.subscribe();
            accept_count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                // Drain the request line + headers; we don't parse,
                // just consume so the client's send completes.
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf).await;

                let resp = b"HTTP/1.1 200 OK\r\n\
                             Content-Type: text/event-stream\r\n\
                             Cache-Control: no-cache\r\n\
                             Connection: keep-alive\r\n\
                             \r\n";
                if stream.write_all(resp).await.is_err() {
                    return;
                }
                if stream.flush().await.is_err() {
                    return;
                }

                while let Ok(payload) = event_rx.recv().await {
                    let frame = format!("event: state\ndata: {}\n\n", payload);
                    if stream.write_all(frame.as_bytes()).await.is_err() {
                        return;
                    }
                    if stream.flush().await.is_err() {
                        return;
                    }
                }
            });
        }
    });

    (url, event_tx, connections)
}

/// Build the `StateChange` payload an SSE `state` frame would carry.
/// Matches the shape `decide_trigger` parses in `daemon::eventsource`.
fn state_change_payload(email_state: &str) -> String {
    json!({
        "@type": "StateChange",
        "changed": { ACCOUNT_ID: { "Email": email_state } }
    })
    .to_string()
}

/// The JMAP server is initially unreachable (returns 503 on the
/// session URL). Pre-reconnect-commit code's `SyncEngine::connect`
/// propagates `Err` after `with_retry` exhausts (~10s) and the daemon
/// future resolves with the error; cursor never advances and the
/// assertion times out. Post-commit code's `connect_with_backoff`
/// keeps retrying transient failures, so once `session_failing` flips
/// off the initial sync runs and the cursor advances to `s1`.
#[tokio::test]
async fn daemon_waits_for_session_recovery_at_startup() {
    let server = MockServer::start().await;
    let state = Arc::new(Mutex::new(MockState {
        mailboxes: vec![inbox()],
        mailbox_state: "m0".to_string(),
        email_state: "s0".to_string(),
        session_failing: true,
        ..Default::default()
    }));

    // This test only exercises the startup gate; the SSE listener
    // doesn't matter for the cursor-advance assertion. Point at the
    // wiremock unmounted route so any listener that does spin up just
    // 404s and retries in the background.
    let sse_url = format!("{}/eventsource", server.uri());
    mount_session(&server, state.clone(), &sse_url).await;
    mount_jmap(&server, state.clone()).await;
    mount_blob_downloads(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let maildir_root = temp.path().join("maildir");
    provision_maildir(&maildir_root);
    let db_path = temp.path().join("state.db");

    let config = build_test_config(&server.uri(), &maildir_root, &db_path);

    // `rusqlite::Connection` is `!Sync`, so a `&Connection` future
    // can't go through `tokio::spawn`. Run the daemon and the test
    // driver concurrently via `select!`; the driver completes when
    // the cursor advances (or times out) and `select!` drops the
    // daemon future.
    let daemon_future = async {
        let conn = db::open_or_recreate(&db_path).unwrap();
        let _ = daemon::runner::run(&conn, &config, None).await;
    };

    let driver_future = async {
        // Hold the gate closed for longer than `with_retry`'s
        // exhaustion window (5 attempts, ~10s worst-case full-jitter
        // total). 15s is long enough that pre-commit code's daemon
        // definitely exits.
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Open the gate. Stage an email so the initial sync has a
        // non-empty plan -- cursor advancement only persists when
        // the executor runs at least one action (see execute.rs:122).
        {
            let mut st = state.lock().unwrap();
            st.session_failing = false;
            st.email_state = "s1".to_string();
            st.emails.push(email_msg("E1", 1));
            st.blobs
                .insert("B1".to_string(), b"raw email body 1".to_vec());
        }

        wait_until(
            || cursor_email_state(&db_path).as_deref() == Some("s1"),
            Duration::from_secs(30),
        )
        .await
    };

    let advanced = tokio::select! {
        _ = daemon_future => false,
        passed = driver_future => passed,
    };

    assert!(
        advanced,
        "daemon should have retried connect and advanced cursor to s1 after the gate opened"
    );
}

/// Drive triggers across a `/jmap` failure window: one before, one
/// inside (which exhausts `engine.run`'s retries), one after. The
/// cursor should land on the post-failure state. `reqwest::Client`'s
/// pooling tolerates 5xx storms so this passes pre- and post-commit;
/// it pins the trigger flow + cursor advancement under transient
/// failure as a regression net rather than a discriminator.
#[tokio::test]
#[ignore = "slow flow test (~15s); kept as documented regression net, opt in with --ignored"]
async fn daemon_processes_event_after_jmap_failure_window() {
    let server = MockServer::start().await;
    let (sse_url, sse_events, sse_connections) = spawn_sse_server().await;

    let state = Arc::new(Mutex::new(MockState {
        mailboxes: vec![inbox()],
        mailbox_state: "m1".to_string(),
        email_state: "s1".to_string(),
        emails: vec![email_msg("E1", 1)],
        ..Default::default()
    }));
    state
        .lock()
        .unwrap()
        .blobs
        .insert("B1".to_string(), b"raw email body 1".to_vec());

    mount_session(&server, state.clone(), &sse_url).await;
    mount_jmap(&server, state.clone()).await;
    mount_blob_downloads(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let maildir_root = temp.path().join("maildir");
    provision_maildir(&maildir_root);
    let db_path = temp.path().join("state.db");

    let config = build_test_config(&server.uri(), &maildir_root, &db_path);

    let daemon_future = async {
        let conn = db::open_or_recreate(&db_path).unwrap();
        let _ = daemon::runner::run(&conn, &config, None).await;
    };

    let driver_future = async {
        // Initial sync pulls E1; cursor should reach s1.
        if !wait_until(
            || cursor_email_state(&db_path).as_deref() == Some("s1"),
            Duration::from_secs(15),
        )
        .await
        {
            return Err("initial sync should advance cursor to s1");
        }

        // The daemon spawns its SSE listener inside `session()` after
        // initial sync. Wait until the listener has connected to our
        // server before pushing -- a broadcast with no subscriber is
        // silently dropped.
        if !wait_until(
            || sse_connections.load(Ordering::SeqCst) >= 1,
            Duration::from_secs(5),
        )
        .await
        {
            return Err("SSE listener should have connected after initial sync");
        }

        // Pre-failure trigger: stage E2 on the server side, advance
        // its email_state to s2, then broadcast a StateChange. The
        // listener compares s2 against its DB-seeded s1 cache, fires
        // RemoteChange, and the cycle advances the cursor.
        {
            let mut st = state.lock().unwrap();
            st.email_state = "s2".to_string();
            st.emails.push(email_msg("E2", 2));
            st.blobs
                .insert("B2".to_string(), b"raw email body 2".to_vec());
        }
        sse_events
            .send(state_change_payload("s2"))
            .expect("SSE listener subscriber should be live");

        if !wait_until(
            || cursor_email_state(&db_path).as_deref() == Some("s2"),
            Duration::from_secs(10),
        )
        .await
        {
            return Err("pre-failure trigger should advance cursor to s2");
        }

        // Open the failure window. Stage E3/s3, broadcast a fresh
        // StateChange. The current session's engine.run hits /jmap's
        // 503 storm, with_retry exhausts, the session returns
        // TransportError, and the outer run() reconnects -- a new
        // SSE listener subscribes on the way through.
        {
            let mut st = state.lock().unwrap();
            st.jmap_failing = true;
            st.email_state = "s3".to_string();
            st.emails.push(email_msg("E3", 3));
            st.blobs
                .insert("B3".to_string(), b"raw email body 3".to_vec());
        }
        sse_events
            .send(state_change_payload("s3"))
            .expect("SSE listener subscriber should still be live");

        // Wait for engine.run's retry layer to exhaust (~10s) and a
        // bit more for the daemon to settle into its reconnect path.
        tokio::time::sleep(Duration::from_secs(12)).await;

        if cursor_email_state(&db_path).as_deref() != Some("s2") {
            return Err("cursor must stay at s2 while /jmap is failing");
        }

        // Close the failure window and broadcast a fresh trigger.
        // We poll-broadcast rather than fire-and-wait: the session #1
        // SSE listener task has its own internal retry loop (see
        // eventsource::listen), so a transient TCP hiccup can make it
        // reconnect *before* session() actually exits and aborts it.
        // That spurious reconnect inflates `sse_connections` to 2
        // while session #2's real listener hasn't subscribed yet, and
        // a single broadcast at that moment lands only on the doomed
        // session #1 sockets. Looping the broadcast mirrors what a
        // real JMAP server does anyway (it keeps emitting StateChange
        // until clients acknowledge by reconnecting/syncing); the
        // listener's dedup cache absorbs the extras once one has
        // fired.
        state.lock().unwrap().jmap_failing = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let mut advanced = false;
        while std::time::Instant::now() < deadline {
            let _ = sse_events.send(state_change_payload("s3"));
            if wait_until(
                || cursor_email_state(&db_path).as_deref() == Some("s3"),
                Duration::from_millis(500),
            )
            .await
            {
                advanced = true;
                break;
            }
        }
        if !advanced {
            return Err("post-failure trigger should advance cursor to s3");
        }

        Ok(())
    };

    let result: Result<(), &'static str> = tokio::select! {
        _ = daemon_future => Err("daemon future returned before driver finished"),
        r = driver_future => r,
    };

    result.unwrap();
}
