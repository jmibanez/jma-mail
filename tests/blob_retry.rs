//! Integration tests for `download_blob`'s retry behavior against a
//! real (in-process) HTTP server. The unit tests in `src/jmap/retry.rs`
//! verify the matcher and `with_retry` loop in isolation; this file
//! verifies the wired-up path: jmap-client builds a real HTTP request,
//! wiremock returns a transient 5xx, and `with_retry` actually fires.
//!
//! Run with `--nocapture` to watch the warn lines as retries happen:
//!     CC=/usr/bin/cc cargo test --test blob_retry -- --nocapture

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jma_mail::ids::JmapBlobId;
use jma_mail::jmap::email::download_blob;
use jmap_client::client::{Client, Credentials};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ACCOUNT_ID: &str = "u-test-account";

/// Build a minimal but valid JMAP session document pointing every URL
/// (api/download/upload/eventsource) at the wiremock server. jmap-client
/// will parse this on `Client::connect()` and use it to build the blob
/// download URL via the `{accountId}` and `{blobId}` placeholders.
fn session_doc(server_uri: &str) -> serde_json::Value {
    serde_json::json!({
        "capabilities": {
            "urn:ietf:params:jmap:core": {
                "maxSizeUpload": 50_000_000,
                "maxConcurrentUpload": 4,
                "maxSizeRequest": 10_000_000,
                "maxConcurrentRequests": 4,
                "maxCallsInRequest": 16,
                "maxObjectsInGet": 500,
                "maxObjectsInSet": 500,
                "collationAlgorithms": ["i;ascii-numeric"],
            },
            "urn:ietf:params:jmap:mail": {}
        },
        "accounts": {
            ACCOUNT_ID: {
                "name": "test@example.com",
                "isPersonal": true,
                "isReadOnly": false,
                "accountCapabilities": { "urn:ietf:params:jmap:mail": {} }
            }
        },
        "primaryAccounts": {
            "urn:ietf:params:jmap:mail": ACCOUNT_ID
        },
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

/// Init `tracing` once per process so retry warn! lines hit the test
/// writer (visible under `--nocapture`). Honors `RUST_LOG` if set,
/// otherwise defaults to `jma_mail=debug`. `try_init` swallows the
/// "already-initialized" error if a sibling test got here first.
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

#[tokio::test]
async fn download_blob_retries_503_then_succeeds() {
    init_tracing();

    let server = MockServer::start().await;
    mount_session(&server).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = calls.clone();
    Mock::given(method("GET"))
        .and(path_regex(r"^/download/u-test-account/"))
        .respond_with(move |_: &wiremock::Request| {
            let n = calls_c.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                ResponseTemplate::new(503).set_body_string("Service Unavailable")
            } else {
                ResponseTemplate::new(200)
                    .set_body_bytes(b"raw rfc5322 message".to_vec())
                    .insert_header("content-type", "application/octet-stream")
            }
        })
        .mount(&server)
        .await;

    let client = build_client(&server).await;
    let blob_id = JmapBlobId::from("B-test");

    // Happy path: ~1.5s of backoff (500ms + 1s) then success on attempt 3.
    // The 15s cap covers the worst case where the matcher regresses and
    // all 5 attempts get exercised (500+1000+2000+4000 = 7.5s of sleeps).
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        download_blob(&client, &blob_id),
    )
    .await
    .expect("download_blob should not exceed timeout")
    .expect("download_blob should succeed after retries");

    assert_eq!(bytes.as_slice(), b"raw rfc5322 message");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "expected 2 transient failures + 1 success"
    );
}

#[tokio::test]
async fn download_blob_does_not_retry_404() {
    init_tracing();

    let server = MockServer::start().await;
    mount_session(&server).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = calls.clone();
    Mock::given(method("GET"))
        .and(path_regex(r"^/download/u-test-account/"))
        .respond_with(move |_: &wiremock::Request| {
            calls_c.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(404).set_body_string("Not Found")
        })
        .mount(&server)
        .await;

    let client = build_client(&server).await;
    let blob_id = JmapBlobId::from("B-missing");

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        download_blob(&client, &blob_id),
    )
    .await
    .expect("download_blob should not exceed timeout");

    assert!(result.is_err(), "404 must propagate as a hard error");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "404 should not trigger any retry attempts"
    );
}
