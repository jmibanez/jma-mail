//! Integration test pinning that `Mailbox/get` retries transient
//! failures rather than bubbling them up immediately. The unit tests
//! in `src/jmap/retry.rs` cover the matcher and `with_retry` loop in
//! isolation; this verifies the wired-up path: jmap-client builds a
//! real `Mailbox/get` POST, wiremock returns a transient 503, and
//! `with_retry` actually fires.
//!
//! Run with `--nocapture` to watch the warn lines as retries happen:
//!     CC=/usr/bin/cc cargo test --test mailbox_retry -- --nocapture

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jma_mail::jmap::mailbox::get_all;
use jmap_client::client::{Client, Credentials};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const ACCOUNT_ID: &str = "u-test-account";

fn session_doc(server_uri: &str) -> Value {
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
        "eventSourceUrl": format!("{}/eventsource", server_uri),
        "state": "abc123",
    })
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("jma_mail=debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .try_init();
}

async fn mount_session(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/.well-known/jmap"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_doc(&server.uri())))
        .mount(server)
        .await;
}

async fn build_client(server: &MockServer) -> Client {
    Client::new()
        .credentials(Credentials::bearer("test-token"))
        .connect(&server.uri())
        .await
        .expect("client connect")
}

fn mailbox_get_response(call_id: &str) -> Value {
    json!([
        "Mailbox/get",
        {
            "accountId": ACCOUNT_ID,
            "state": "mb-1",
            "list": [{
                "id": "MB-INBOX",
                "name": "Inbox",
                "parentId": null,
                "role": "inbox",
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
            }],
            "notFound": []
        },
        call_id
    ])
}

/// First two `POST /jmap` calls return 503; the third returns a valid
/// `Mailbox/get` response. Pre-fix code (no `with_retry` around
/// `Mailbox/get`) would bubble the first 503 as "Failed to fetch
/// mailboxes" and never reach the third call. Post-fix code retries
/// transient errors and resolves the mailbox list on attempt 3.
#[tokio::test]
async fn mailbox_get_retries_transient_5xx() {
    init_tracing();

    let server = MockServer::start().await;
    mount_session(&server).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = calls.clone();
    Mock::given(method("POST"))
        .and(path("/jmap"))
        .respond_with(move |req: &Request| {
            let n = calls_c.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                return ResponseTemplate::new(503).set_body_string("Service Unavailable");
            }
            let body: Value = serde_json::from_slice(&req.body).expect("valid JSON body");
            let call_id = body["methodCalls"][0][2]
                .as_str()
                .expect("call id")
                .to_string();
            ResponseTemplate::new(200).set_body_json(json!({
                "methodResponses": [mailbox_get_response(&call_id)],
                "sessionState": "session-1",
            }))
        })
        .mount(&server)
        .await;

    let client = build_client(&server).await;

    let mailboxes = tokio::time::timeout(std::time::Duration::from_secs(15), get_all(&client))
        .await
        .expect("get_all should not exceed timeout")
        .expect("get_all should succeed after retries");

    assert_eq!(mailboxes.len(), 1);
    assert_eq!(mailboxes[0].name, "Inbox");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "expected 2 transient failures + 1 success"
    );
}

/// A 404 from `Mailbox/get` is a hard error; the retry layer must not
/// loop on it. Pins the policy in `is_transient_error` and ensures
/// the new `with_retry` wrapper around `get_all` doesn't accidentally
/// blanket-retry every error class.
#[tokio::test]
async fn mailbox_get_does_not_retry_404() {
    init_tracing();

    let server = MockServer::start().await;
    mount_session(&server).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = calls.clone();
    Mock::given(method("POST"))
        .and(path("/jmap"))
        .respond_with(move |_req: &Request| {
            calls_c.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(404).set_body_string("Not Found")
        })
        .mount(&server)
        .await;

    let client = build_client(&server).await;

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), get_all(&client))
        .await
        .expect("get_all should not exceed timeout");

    assert!(result.is_err(), "404 must propagate as a hard error");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "404 should not trigger any retry attempts"
    );
}
