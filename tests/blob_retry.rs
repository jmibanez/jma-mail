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
use jma_mail::jmap::email::{build_blob_http_client, download_blob};
use jma_mail::maildir_ops::store::ensure_maildir;
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
    let http = build_blob_http_client(&client).expect("build blob http client");
    let blob_id = JmapBlobId::from("B-test");
    let tempdir = tempfile::tempdir().expect("tempdir for maildir");
    let maildir = ensure_maildir(tempdir.path()).expect("ensure_maildir");

    // Happy path: ~1.5s of backoff (500ms + 1s) then success on attempt 3.
    // The 15s cap covers the worst case where the matcher regresses and
    // all 5 attempts get exercised (500+1000+2000+4000 = 7.5s of sleeps).
    let tmp = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        download_blob(&http, &client, &blob_id, &maildir),
    )
    .await
    .expect("download_blob should not exceed timeout")
    .expect("download_blob should succeed after retries");

    let bytes = std::fs::read(tmp.path()).expect("read streamed tmp file");
    assert_eq!(bytes.as_slice(), b"raw rfc5322 message");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "expected 2 transient failures + 1 success"
    );
    // Each failed attempt opened its own tmp file before erroring; the
    // `TemporaryMailFile::drop` guard must unlink those, leaving only
    // the successful attempt's still-open handle behind. Without this
    // assertion, a regression where a failed-attempt handle leaks a
    // sibling file would pass undetected.
    let tmp_entries: Vec<_> = std::fs::read_dir(tempdir.path().join("tmp"))
        .expect("tmp/ should exist after ensure_maildir")
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(
        tmp_entries.len(),
        1,
        "tmp/ must contain exactly the final attempt's file; got {:?}",
        tmp_entries
            .iter()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
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
    let http = build_blob_http_client(&client).expect("build blob http client");
    let blob_id = JmapBlobId::from("B-missing");
    let tempdir = tempfile::tempdir().expect("tempdir for maildir");
    let maildir = ensure_maildir(tempdir.path()).expect("ensure_maildir");

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        download_blob(&http, &client, &blob_id, &maildir),
    )
    .await
    .expect("download_blob should not exceed timeout");

    assert!(result.is_err(), "404 must propagate as a hard error");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "404 should not trigger any retry attempts"
    );
    // Hard-error path must not leave the partial tmp file behind:
    // `TemporaryMailFile::drop` unlinks on the failing attempt's
    // handle so subsequent retry attempts can't accumulate orphans
    // in tmp/. Verify by reading the directory directly.
    let tmp_entries: Vec<_> = std::fs::read_dir(tempdir.path().join("tmp"))
        .expect("tmp/ should exist after ensure_maildir")
        .filter_map(|e| e.ok())
        .collect();
    assert!(
        tmp_entries.is_empty(),
        "tmp/ must be empty after a hard error; got {:?}",
        tmp_entries
            .iter()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );
}

/// Parity check: our blob fetcher and jmap-client must construct
/// byte-identical download URLs from the same session metadata. Both
/// substitute `{accountId}`, `{blobId}`, `{name}`, and `{type}` per
/// RFC 8620 section 6.2; this test pins that both paths walk the
/// template the same way (`name=none`, `type=application/octet-stream`,
/// query-string ordering, no extra path segments) so a future
/// upstream rename or accessor change doesn't silently diverge them.
#[tokio::test]
async fn download_blob_url_matches_jmap_client() {
    init_tracing();

    let server = MockServer::start().await;
    mount_session(&server).await;

    Mock::given(method("GET"))
        .and(path_regex(r"^/download/u-test-account/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"x".to_vec())
                .insert_header("content-type", "application/octet-stream"),
        )
        .mount(&server)
        .await;

    let client = build_client(&server).await;
    let blob_id = JmapBlobId::from("B-parity");

    client
        .download(blob_id.as_ref())
        .await
        .expect("jmap-client download");

    let http = build_blob_http_client(&client).expect("build blob http client");
    let tempdir = tempfile::tempdir().expect("tempdir for maildir");
    let maildir = ensure_maildir(tempdir.path()).expect("ensure_maildir");
    download_blob(&http, &client, &blob_id, &maildir)
        .await
        .expect("our download_blob");

    let received = server
        .received_requests()
        .await
        .expect("wiremock retains requests by default");
    let download_urls: Vec<String> = received
        .iter()
        .filter(|r| r.url.path().starts_with("/download/"))
        .map(|r| r.url.to_string())
        .collect();

    assert_eq!(
        download_urls.len(),
        2,
        "expected one GET per path (jmap-client + ours); got {:?}",
        download_urls
    );
    assert_eq!(
        download_urls[0], download_urls[1],
        "jmap-client and our path must hit byte-identical URLs"
    );
}
