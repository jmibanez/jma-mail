//! Wiremock-backed integration tests for `sync::engine`. These pin the
//! engine's interaction with a JMAP server end-to-end:
//!
//! - `resolve_mailboxes` -- INBOX magic alias and the `mailboxes` filter.
//! - `SyncEngine::sync` initial pull -- Mailbox/get + Email/query + Email/get
//!   + blob download + maildir write + state cursor advancement.
//! - `SyncEngine::sync` already-in-sync -- Email/changes returning no
//!   created/updated/destroyed.
//! - `fetch_remote_state` cannotCalculateChanges fallback -- Email/changes
//!   error wipes the cursor and re-runs the initial path, leaving the
//!   downloaded files and a fresh cursor.
//!
//! Together these exercise every JMAP-touching helper in `src/sync/engine.rs`
//! through its public entry points so a structural refactor that regressed
//! plumbing (lost an `account_id`, swapped a `client` for the wrong field)
//! would fail at least one assertion.
//!
//! Run with `--nocapture` to watch the engine's log lines:
//!     CC=/usr/bin/cc cargo test --test engine_jmap -- --nocapture

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use jma_mail::config::FolderLayout;
use jma_mail::config::{AccountConfig, Config, ConflictStrategy, StateConfig, SyncConfig};
use jma_mail::ids::{JmapEmailId, JmapMailboxId};
use jma_mail::maildir_ops::sentinel;
use jma_mail::state::{db, queries};
use jma_mail::sync::engine::SyncEngine;
use rusqlite::Connection;
use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const ACCOUNT_ID: &str = "u-test-account";
const SESSION_STATE: &str = "session-1";

/// Mutable mock-server state shared across the dispatcher closures so a
/// single test can stage multiple outcomes (e.g. pre-load mailboxes and
/// emails, then advance state, or rig the next Email/changes to error
/// with cannotCalculateChanges).
#[derive(Default)]
struct MockState {
    mailboxes: Vec<MockMailbox>,
    mailbox_state: String,
    emails: Vec<MockEmail>,
    email_state: String,
    blobs: HashMap<String, Vec<u8>>,
    /// If set, the next Email/changes call returns this error instead
    /// of a normal response. Consumed on use so a follow-up call gets
    /// the regular code path.
    next_email_changes_error: Option<&'static str>,
    /// If set, Email/changes returns these created/updated/destroyed
    /// id lists. None means "no changes since last cursor".
    pending_email_changes: Option<EmailChangesResp>,
}

#[derive(Default, Clone)]
struct EmailChangesResp {
    created: Vec<String>,
    updated: Vec<String>,
    destroyed: Vec<String>,
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
                "accountCapabilities": {
                    "urn:ietf:params:jmap:mail": {}
                }
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

async fn mount_session(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/.well-known/jmap"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_doc(&server.uri())))
        .mount(server)
        .await;
}

/// Mount the `POST /jmap` dispatcher. Inspects the methodCalls in the
/// request body, dispatches each to the matching handler, and emits a
/// JMAP-shaped methodResponses array.
async fn mount_jmap(server: &MockServer, state: Arc<Mutex<MockState>>) {
    let state_for_closure = state.clone();
    Mock::given(method("POST"))
        .and(path("/jmap"))
        .respond_with(move |req: &Request| {
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
                let resp = handle_method(
                    &method_name,
                    &args,
                    &call_id,
                    &state_for_closure.lock().unwrap(),
                );
                responses.push(resp);
            }
            ResponseTemplate::new(200).set_body_json(json!({
                "methodResponses": responses,
                "sessionState": SESSION_STATE,
            }))
        })
        .mount(server)
        .await;
}

/// Mount `GET /download/...` so `download_blob` finds the bytes that
/// were preloaded into `state.blobs`.
async fn mount_blob_downloads(server: &MockServer, state: Arc<Mutex<MockState>>) {
    let state_for_closure = state.clone();
    Mock::given(method("GET"))
        .and(path_regex(r"^/download/u-test-account/"))
        .respond_with(move |req: &Request| {
            // URL shape: /download/{accountId}/{blobId}/{name}?accept=...
            let url = req.url.path();
            let segments: Vec<&str> = url.trim_start_matches('/').split('/').collect();
            // ["download", accountId, blobId, name]
            let blob_id = segments.get(2).copied().unwrap_or_default();
            let st = state_for_closure.lock().unwrap();
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
        "Mailbox/get" => mailbox_get(args, call_id, state),
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

fn mailbox_get(_args: &Value, call_id: &str, state: &MockState) -> Value {
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
    // Filter is `{ "inMailbox": "<id>" }` per the engine's call shape.
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

fn email_changes(args: &Value, call_id: &str, state: &MockState) -> Value {
    if let Some(err) = state.next_email_changes_error {
        return json!([
            "error",
            { "type": err, "description": format!("rigged {}", err) },
            call_id
        ]);
    }
    let since = args["sinceState"].as_str().unwrap_or_default();
    let changes = state.pending_email_changes.clone().unwrap_or_default();
    json!([
        "Email/changes",
        {
            "accountId": ACCOUNT_ID,
            "oldState": since,
            "newState": state.email_state,
            "hasMoreChanges": false,
            "created": changes.created,
            "updated": changes.updated,
            "destroyed": changes.destroyed,
        },
        call_id
    ])
}

/// Open a fresh on-disk state DB inside the given temp dir. Integration
/// tests can't use `db::open_in_memory` (gated on `cfg(test)` for unit
/// tests), so route through the public `open_or_recreate` against a
/// path the temp dir cleans up automatically.
fn fresh_db(temp: &tempfile::TempDir) -> Connection {
    let db_path = temp.path().join("state.db");
    db::open_or_recreate(&db_path).expect("open state DB")
}

fn test_config(
    server: &MockServer,
    maildir_root: &std::path::Path,
    mailboxes: Vec<String>,
) -> Config {
    Config {
        account: AccountConfig {
            email: "test@example.com".to_string(),
            token: Some("test-token".to_string()),
            // Force `session::connect` down the explicit-override path
            // so it `Client::connect`s the wiremock server instead of
            // doing real DNS/well-known autodiscovery.
            session_url: Some(server.uri()),
        },
        sync: SyncConfig {
            maildir_path: maildir_root.to_string_lossy().into_owned(),
            mailboxes,
            conflict_strategy: ConflictStrategy::ServerWins,
            case_insensitive_match: false,
            download_concurrency: 2,
            upload_concurrency: 2,
            retry_max_attempts: 1,
            retry_initial_backoff_ms: 1,
            retry_max_backoff_ms: 1,
            folder_layout: FolderLayout::Fs,
            hierarchy_separator: '/',
        },
        state: StateConfig {
            db_path: Some(":memory:".to_string()),
        },
        ..Default::default()
    }
}

/// `role: "inbox"` mailbox is exposed to the engine as folder name "INBOX",
/// regardless of the server-supplied display name. The maildir directory
/// is created under that alias and the mailbox_map row records the same
/// folder name. Pins the mbsync-convention magic alias.
#[tokio::test]
async fn sync_applies_inbox_magic_alias() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![MockMailbox {
            id: "MB-INBOX".to_string(),
            name: "Indbakke".to_string(), // localized display name
            role: Some("inbox".to_string()),
        }],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("sync succeeds");

    assert!(temp.path().join("INBOX").join("cur").is_dir());

    let rows = queries::get_all_mailboxes(&conn).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].maildir_folder, "INBOX");
    assert_eq!(rows[0].name, "Indbakke");
    assert_eq!(rows[0].role.as_deref(), Some("inbox"));
}

/// With a non-empty `mailboxes` filter, only listed mailboxes survive
/// the resolution. Server returns three; config asks for two; the third
/// must not appear in the resolved list, in mailbox_map, or as a
/// directory on disk. Pins both the filter and the fact that we don't
/// pre-create directories for unsynced mailboxes.
#[tokio::test]
async fn sync_filter_drops_unlisted() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![
            MockMailbox {
                id: "MB-INBOX".to_string(),
                name: "Inbox".to_string(),
                role: Some("inbox".to_string()),
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
            },
            MockMailbox {
                id: "MB-SPAM".to_string(),
                name: "Spam".to_string(),
                role: Some("junk".to_string()),
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(
        &server,
        temp.path(),
        vec!["INBOX".to_string(), "Archive".to_string()],
    );

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("sync succeeds");

    assert!(temp.path().join("INBOX").join("cur").is_dir());
    assert!(temp.path().join("Archive").join("cur").is_dir());
    assert!(
        !temp.path().join("Spam").exists(),
        "filtered-out mailbox must not be provisioned on disk"
    );

    let rows = queries::get_all_mailboxes(&conn).unwrap();
    let names: std::collections::HashSet<&str> =
        rows.iter().map(|r| r.maildir_folder.as_str()).collect();
    assert!(names.contains("INBOX"));
    assert!(names.contains("Archive"));
    assert!(!names.contains("Spam"));
}

/// Sync writes a `.jma.mapping` sentinel inside every synced
/// maildir folder so the local-folder -> JMAP-mailbox binding
/// survives a state DB nuke. Pins three properties:
///
/// 1. The sentinel is created from scratch on first sync.
/// 2. `server_name` records the JMAP leaf name (e.g. `"Indbakke"`),
///    not the layout-flattened on-disk folder name (`"INBOX"`).
/// 3. A pre-existing stale sentinel (wrong jmap_mailbox_id) is
///    overwritten with the resolved truth on the next cycle.
#[tokio::test]
async fn sync_writes_identity_sentinels() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![
            MockMailbox {
                id: "MB-INBOX".to_string(),
                name: "Indbakke".to_string(),
                role: Some("inbox".to_string()),
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // Plant a stale sentinel under Archive before the first resolve.
    // Under the test config (Fs layout, '/' separator) a top-level
    // mailbox named "Archive" lands at the literal `Archive/` path,
    // and INBOX-role lands at the `INBOX` alias; both folder names
    // are pinned by the existing tests above.
    let archive_dir = temp.path().join("Archive");
    std::fs::create_dir_all(&archive_dir).expect("create archive dir");
    sentinel::write(
        &archive_dir,
        &sentinel::MailboxMapping {
            jmap_mailbox_id: JmapMailboxId::from("MB-WRONG"),
            parent_jmap_mailbox_id: None,
            server_name: "ForeignName".to_string(),
        },
    )
    .expect("write stale sentinel");

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("sync succeeds");

    // INBOX: written from scratch. server_name carries the JMAP
    // leaf name, not the local INBOX alias.
    let inbox_sentinel = sentinel::read(&temp.path().join("INBOX"))
        .expect("inbox sentinel readable")
        .expect("inbox sentinel present");
    assert_eq!(
        inbox_sentinel.jmap_mailbox_id,
        JmapMailboxId::from("MB-INBOX")
    );
    assert_eq!(inbox_sentinel.parent_jmap_mailbox_id, None);
    assert_eq!(inbox_sentinel.server_name, "Indbakke");

    // Archive: the stale `MB-WRONG` binding gets overwritten to
    // match what `Mailbox/get` reported.
    let archive_sentinel = sentinel::read(&archive_dir)
        .expect("archive sentinel readable")
        .expect("archive sentinel present");
    assert_eq!(
        archive_sentinel.jmap_mailbox_id,
        JmapMailboxId::from("MB-ARCH")
    );
    assert_eq!(archive_sentinel.server_name, "Archive");
}

/// On-disk folder exists but its sentinel went missing (partial
/// write the previous cycle, or a hand-edit). `resolve_mailboxes`
/// must detect the gap and re-emit `CreateLocalMailbox` so the
/// executor re-stamps the sentinel. Without this, a one-time
/// sentinel write failure would leave the binding unrecoverable
/// from disk indefinitely (the mailbox_map row landed on the
/// prior cycle, so a cache-snapshot-based newness check would
/// say "steady-state, nothing to do").
#[tokio::test]
async fn sync_re_stamps_sentinel_when_folder_exists_without_one() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![MockMailbox {
            id: "MB-INBOX".to_string(),
            name: "Inbox".to_string(),
            role: Some("inbox".to_string()),
        }],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // First cycle: lands INBOX maildir + sentinel + mailbox_map
    // row.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    let sentinel_path = temp.path().join("INBOX").join(".jma.mapping");
    assert!(sentinel_path.exists(), "first sync writes the sentinel");

    // Simulate the partial-failure case: folder stays, sentinel
    // is gone.
    std::fs::remove_file(&sentinel_path).expect("delete sentinel");
    assert!(!sentinel_path.exists());

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync");

    assert!(
        sentinel_path.exists(),
        "second sync must re-detect the missing sentinel and re-stamp it"
    );
    let restored = sentinel::read(&temp.path().join("INBOX"))
        .expect("readable")
        .expect("present");
    assert_eq!(restored.jmap_mailbox_id, JmapMailboxId::from("MB-INBOX"));
    assert_eq!(restored.server_name, "Inbox");
}

/// `jma push` on a fresh config (no prior sync, no maildirs)
/// has nothing local to upload -- CreateLocalMailbox is Pull-only,
/// so the cycle would burn through scan + fetch + reconcile only
/// to plan zero work. Bail upfront with an actionable error that
/// points the user at sync/pull, and leave the filesystem
/// untouched.
#[tokio::test]
async fn push_only_errors_on_fresh_config_without_local_maildirs() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![
            MockMailbox {
                id: "MB-INBOX".to_string(),
                name: "Inbox".to_string(),
                role: Some("inbox".to_string()),
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    let err = SyncEngine::push_only(&conn, &config, false)
        .await
        .expect_err("push-only on fresh config must error");
    let msg = err.to_string();
    assert!(
        msg.contains("jma sync") || msg.contains("jma pull"),
        "error must point at sync/pull as the prerequisite, got: {msg}"
    );

    assert!(
        !temp.path().join("INBOX").exists(),
        "push-only must not create INBOX"
    );
    assert!(
        !temp.path().join("Archive").exists(),
        "push-only must not create Archive"
    );
}

/// Steady-state `jma push` with one new server-side mailbox the
/// user hasn't pulled yet must NOT error: the user has existing
/// maildirs with messages to push, and the new remote mailbox is
/// irrelevant to that work. The CreateLocalMailbox action for
/// the new folder gets dropped by the direction filter; the rest
/// of the push proceeds normally.
///
/// Asserts the post-conditions (push completes, Projects/ not
/// provisioned) rather than positively pinning the
/// emit-then-drop path. Reconcile's unit tests
/// (`emit_create_local_mailboxes_emits_one_per_new_binding`)
/// cover the emission side; the assertion here is that the
/// direction filter does the right thing with whatever reconcile
/// produces.
#[tokio::test]
async fn push_only_succeeds_when_some_local_maildirs_are_provisioned() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![MockMailbox {
            id: "MB-INBOX".to_string(),
            name: "Inbox".to_string(),
            role: Some("inbox".to_string()),
        }],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;
    mount_blob_downloads(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // Cycle 1: full sync provisions INBOX maildir + sentinel.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first full sync");
    assert!(temp.path().join("INBOX").join("cur").is_dir());

    // Server grows a new mailbox between cycles -- the user
    // hasn't pulled it yet, so its maildir doesn't exist locally.
    {
        let mut st = state.lock().unwrap();
        st.mailboxes.push(MockMailbox {
            id: "MB-NEW".to_string(),
            name: "Projects".to_string(),
            role: None,
        });
        st.mailbox_state = "mb-2".to_string();
    }

    // Push-only must not error here: INBOX is provisioned, and
    // the dropped CreateLocalMailbox for Projects is not the
    // user's concern in a push cycle.
    SyncEngine::push_only(&conn, &config, false)
        .await
        .expect("push-only with at least one provisioned maildir must succeed");

    assert!(
        !temp.path().join("Projects").exists(),
        "push-only must not provision the new server-side mailbox as a side effect"
    );
}

/// First-cycle `--dry-run` must not provision maildirs. Before
/// the `CreateLocalMailbox` extraction, `resolve_mailboxes`
/// `ensure_maildir`'d every synced mailbox unconditionally,
/// which left empty `cur/`/`new/`/`tmp/` trees on disk even when
/// the user asked only for a preview. Now the maildir is created
/// by an executor phase that dry-run skips, so the preview
/// leaves the filesystem untouched.
#[tokio::test]
async fn sync_dry_run_does_not_create_local_maildir_on_first_cycle() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![
            MockMailbox {
                id: "MB-INBOX".to_string(),
                name: "Inbox".to_string(),
                role: Some("inbox".to_string()),
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    SyncEngine::sync(&conn, &config, true)
        .await
        .expect("dry-run first sync");

    assert!(
        !temp.path().join("INBOX").exists(),
        "INBOX maildir must not be created under --dry-run"
    );
    assert!(
        !temp.path().join("Archive").exists(),
        "Archive maildir must not be created under --dry-run"
    );
}

/// End-to-end initial pull. State DB starts empty so engine routes to
/// the initial-pull path: Mailbox/get -> Email/query -> Email/get ->
/// download blob -> write to maildir -> message_map row -> jmap_state
/// cursor. Pins the entire happy-path pipeline.
#[tokio::test]
async fn sync_initial_pull_downloads_email_into_maildir() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![MockMailbox {
            id: "MB-INBOX".to_string(),
            name: "Inbox".to_string(),
            role: Some("inbox".to_string()),
        }],
        emails: vec![MockEmail {
            id: "E1".to_string(),
            blob_id: "B1".to_string(),
            thread_id: "T1".to_string(),
            mailbox_ids: vec!["MB-INBOX".to_string()],
            keywords: vec!["$seen".to_string()],
            message_id: Some("<msg-1@example.com>".to_string()),
        }],
        blobs: {
            let mut m = HashMap::new();
            m.insert(
                "B1".to_string(),
                b"Message-ID: <msg-1@example.com>\r\nSubject: hi\r\n\r\nbody\r\n".to_vec(),
            );
            m
        },
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;
    mount_blob_downloads(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    let outcome = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("sync succeeds");

    assert_eq!(outcome.downloaded, 1, "must report one downloaded email");
    assert_eq!(outcome.failed_remote_actions, 0);

    // File appears in INBOX/cur or new with our blob bytes.
    let inbox_cur = temp.path().join("INBOX").join("cur");
    let inbox_new = temp.path().join("INBOX").join("new");
    let files: Vec<_> = std::fs::read_dir(&inbox_cur)
        .unwrap()
        .chain(std::fs::read_dir(&inbox_new).unwrap())
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(files.len(), 1, "exactly one file should land in INBOX");

    // message_map row pins the JMAP id and Message-ID anchor.
    let rec = queries::get_message_by_jmap_id(&conn, &JmapEmailId::from("E1"))
        .unwrap()
        .expect("message_map must hold the downloaded email");
    assert_eq!(rec.message_id.as_ref(), "<msg-1@example.com>");
    assert_eq!(rec.jmap_mailbox_id.as_ref(), "MB-INBOX");

    // jmap_state cursor advances so the next cycle takes the
    // Email/changes delta path instead of re-pulling.
    let cursor = queries::get_jmap_state(&conn, ACCOUNT_ID, "Email")
        .unwrap()
        .expect("Email cursor must be persisted after a successful initial pull");
    assert_eq!(cursor, "e-1");
}

/// Cursor matches server state and Email/changes returns no
/// created/updated/destroyed -- the cycle is a clean no-op.
/// `SyncOutcome::default()` and no maildir writes. Pins the steady-state
/// path so a refactor that incorrectly forced re-download would fail.
#[tokio::test]
async fn sync_already_in_sync_is_a_noop() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![MockMailbox {
            id: "MB-INBOX".to_string(),
            name: "Inbox".to_string(),
            role: Some("inbox".to_string()),
        }],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // Pre-seed the cursor so the engine takes the Email/changes branch.
    queries::set_jmap_state(&conn, ACCOUNT_ID, "Email", "e-1").unwrap();

    let outcome = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("sync succeeds");

    assert_eq!(outcome.downloaded, 0);
    assert_eq!(outcome.failed_remote_actions, 0);

    // No files should appear in the INBOX maildir.
    let inbox_cur = temp.path().join("INBOX").join("cur");
    let count = std::fs::read_dir(&inbox_cur).unwrap().count();
    assert_eq!(count, 0, "no files should land in INBOX on a no-op cycle");

    // No message_map rows either.
    assert!(!queries::has_message_map_rows(&conn).unwrap());
}

/// `Email/changes` returns `cannotCalculateChanges`. The engine must
/// detect the typed `MethodErrorType::CannotCalculateChanges` (matched
/// across the anyhow chain by `jmap::email::is_cannot_calculate_changes`),
/// wipe the stale cursor, and route the cycle through the initial-pull
/// path so the downloaded file lands and a fresh cursor is persisted.
#[tokio::test]
async fn sync_falls_back_when_email_changes_cannot_calculate() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-2".to_string(),
        mailboxes: vec![MockMailbox {
            id: "MB-INBOX".to_string(),
            name: "Inbox".to_string(),
            role: Some("inbox".to_string()),
        }],
        emails: vec![MockEmail {
            id: "E1".to_string(),
            blob_id: "B1".to_string(),
            thread_id: "T1".to_string(),
            mailbox_ids: vec!["MB-INBOX".to_string()],
            keywords: vec![],
            message_id: Some("<msg-1@example.com>".to_string()),
        }],
        blobs: {
            let mut m = HashMap::new();
            m.insert(
                "B1".to_string(),
                b"Message-ID: <msg-1@example.com>\r\nSubject: hi\r\n\r\nbody\r\n".to_vec(),
            );
            m
        },
        next_email_changes_error: Some("cannotCalculateChanges"),
        pending_email_changes: None,
    }));
    mount_jmap(&server, state.clone()).await;
    mount_blob_downloads(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // Stale cursor; rigged Email/changes will reject it on the first call.
    queries::set_jmap_state(&conn, ACCOUNT_ID, "Email", "stale-cursor").unwrap();

    let outcome = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("sync succeeds via the initial-pull fallback");

    assert_eq!(
        outcome.downloaded, 1,
        "fallback must run the initial pull and download the email"
    );
    assert_eq!(
        outcome.failed_remote_actions, 0,
        "cannotCalculateChanges is metadata signal, not a remote-action failure"
    );

    // Cursor was rewritten to the current state via get_current_state
    // -> set_jmap_state at the tail of execute.
    let cursor = queries::get_jmap_state(&conn, ACCOUNT_ID, "Email").unwrap();
    assert_eq!(cursor.as_deref(), Some("e-2"));

    // The downloaded file is present.
    let rec = queries::get_message_by_jmap_id(&conn, &JmapEmailId::from("E1"))
        .unwrap()
        .expect("E1 must be in message_map after fallback");
    assert_eq!(rec.jmap_mailbox_id.as_ref(), "MB-INBOX");
}

/// Two-cycle test: an initial pull followed by a real Email/changes
/// delta. After the first cycle the cursor sits at the post-pull state;
/// the test then mutates the server (new email + bumped state + a
/// rigged Email/changes response listing the new id as `created`) and
/// runs sync a second time. The engine must take the delta path
/// (Email/changes -> Email/get for the new id only, no Email/query),
/// download just the new email, and advance the cursor to the new
/// server state. Pins the state-handoff between cycles, the most
/// plumbing-rich path through the engine and the most likely place a
/// future struct refactor would leak per-cycle state.
#[tokio::test]
async fn sync_delta_cycle_after_initial_pull_picks_up_new_email() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let blob_e1 = b"Message-ID: <msg-1@example.com>\r\nSubject: one\r\n\r\nbody one\r\n".to_vec();
    let blob_e2 = b"Message-ID: <msg-2@example.com>\r\nSubject: two\r\n\r\nbody two\r\n".to_vec();

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![MockMailbox {
            id: "MB-INBOX".to_string(),
            name: "Inbox".to_string(),
            role: Some("inbox".to_string()),
        }],
        emails: vec![MockEmail {
            id: "E1".to_string(),
            blob_id: "B1".to_string(),
            thread_id: "T1".to_string(),
            mailbox_ids: vec!["MB-INBOX".to_string()],
            keywords: vec![],
            message_id: Some("<msg-1@example.com>".to_string()),
        }],
        blobs: {
            let mut m = HashMap::new();
            m.insert("B1".to_string(), blob_e1);
            m
        },
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;
    mount_blob_downloads(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // Cycle 1: initial pull. Cursor starts empty so the engine runs
    // Email/query + Email/get and bootstraps via get_current_state.
    let first = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("initial pull succeeds");
    assert_eq!(first.downloaded, 1);
    assert_eq!(
        queries::get_jmap_state(&conn, ACCOUNT_ID, "Email")
            .unwrap()
            .as_deref(),
        Some("e-1"),
        "post-initial-pull cursor must be persisted before the delta cycle"
    );

    // Server-side change: a new email arrives, server state advances,
    // and Email/changes is rigged to report E2 as created.
    {
        let mut st = state.lock().unwrap();
        st.email_state = "e-2".to_string();
        st.emails.push(MockEmail {
            id: "E2".to_string(),
            blob_id: "B2".to_string(),
            thread_id: "T2".to_string(),
            mailbox_ids: vec!["MB-INBOX".to_string()],
            keywords: vec!["$seen".to_string()],
            message_id: Some("<msg-2@example.com>".to_string()),
        });
        st.blobs.insert("B2".to_string(), blob_e2);
        st.pending_email_changes = Some(EmailChangesResp {
            created: vec!["E2".to_string()],
            updated: vec![],
            destroyed: vec![],
        });
    }

    // Cycle 2: delta path. Engine consults Email/changes from "e-1",
    // gets [E2] as created, fetches just E2 via Email/get, downloads
    // its blob, and advances the cursor to the new state.
    let second = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("delta cycle succeeds");
    assert_eq!(
        second.downloaded, 1,
        "delta cycle must download exactly the new email"
    );
    assert_eq!(second.failed_remote_actions, 0);

    let cursor = queries::get_jmap_state(&conn, ACCOUNT_ID, "Email").unwrap();
    assert_eq!(
        cursor.as_deref(),
        Some("e-2"),
        "cursor must advance to the post-delta state"
    );

    let e2 = queries::get_message_by_jmap_id(&conn, &JmapEmailId::from("E2"))
        .unwrap()
        .expect("E2 must be in message_map after the delta cycle");
    assert_eq!(e2.jmap_mailbox_id.as_ref(), "MB-INBOX");
    assert_eq!(e2.message_id.as_ref(), "<msg-2@example.com>");

    // E1 from cycle 1 is still bound -- the delta cycle didn't touch
    // it, so its row stays exactly as initial pull left it.
    let e1 = queries::get_message_by_jmap_id(&conn, &JmapEmailId::from("E1"))
        .unwrap()
        .expect("E1 must remain bound across cycles");
    assert_eq!(e1.jmap_mailbox_id.as_ref(), "MB-INBOX");
}
