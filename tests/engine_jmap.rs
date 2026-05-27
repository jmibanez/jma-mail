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
use jma_mail::config::{
    AccountConfig, AllowDestructiveFolderSync, CompiledRenameRule, Config, ConflictStrategy,
    StateConfig, SyncConfig,
};
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
    /// Mailbox/set { create } requests recorded in the order they
    /// arrived. Each entry is the (name, parent_id, role) triple
    /// the client sent; the handler appends the corresponding
    /// server-assigned id into `mailboxes` so a follow-up
    /// `Mailbox/get` would see it.
    mailbox_set_creates: Vec<(String, Option<String>, Option<String>)>,
    /// Names that `Mailbox/set { create }` must reject. Each
    /// matching create goes into `notCreated` with
    /// `invalidProperties` and is not appended to
    /// `mailbox_set_creates`. Pins the warn-and-continue path.
    mailbox_set_reject_names: Vec<String>,
    /// Mailbox/set { update } requests recorded in the order
    /// they arrived. Each entry is the (id, new_name,
    /// new_parent_id) triple the client sent; the handler
    /// rewrites the matching `MockMailbox` so a follow-up
    /// `Mailbox/get` reflects the rename.
    mailbox_set_updates: Vec<(String, String, Option<String>)>,
    /// Ids that `Mailbox/set { update }` must reject. Each
    /// matching update goes into `notUpdated` with
    /// `invalidProperties` and is not appended to
    /// `mailbox_set_updates` / does not rewrite the mailbox.
    mailbox_set_reject_update_ids: Vec<String>,
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
    parent_id: Option<String>,
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
                    &mut state_for_closure.lock().unwrap(),
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

fn handle_method(name: &str, args: &Value, call_id: &str, state: &mut MockState) -> Value {
    match name {
        "Mailbox/get" => mailbox_get(args, call_id, state),
        "Mailbox/set" => mailbox_set(args, call_id, state),
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

fn mailbox_get(_args: &Value, call_id: &str, state: &mut MockState) -> Value {
    let list: Vec<Value> = state
        .mailboxes
        .iter()
        .map(|mb| {
            json!({
                "id": mb.id,
                "name": mb.name,
                "parentId": mb.parent_id,
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

/// Minimal `Mailbox/set { create | update }` dispatcher. Records
/// each create into `state.mailbox_set_creates` and each update
/// into `state.mailbox_set_updates`, rewriting / appending the
/// matching `MockMailbox` so a follow-up `Mailbox/get` reflects
/// the change. `destroy` is unimplemented -- tests that need it
/// have to extend this handler.
fn mailbox_set(args: &Value, call_id: &str, state: &mut MockState) -> Value {
    let mut created_resp = serde_json::Map::new();
    let mut not_created_resp = serde_json::Map::new();
    if let Some(creates) = args["create"].as_object() {
        for (creation_id, props) in creates {
            let name = props["name"].as_str().unwrap_or_default().to_string();
            let parent = props["parentId"].as_str().map(|s| s.to_string());
            let role = match &props["role"] {
                Value::String(s) => Some(s.clone()),
                _ => None,
            };
            if state.mailbox_set_reject_names.iter().any(|n| n == &name) {
                not_created_resp.insert(
                    creation_id.clone(),
                    json!({
                        "type": "invalidProperties",
                        "description": format!("rigged rejection for {:?}", name),
                    }),
                );
                continue;
            }
            let new_id = format!("MB-NEW-{}", state.mailboxes.len() + 1);
            state.mailboxes.push(MockMailbox {
                id: new_id.clone(),
                name: name.clone(),
                role: role.clone(),
                parent_id: parent.clone(),
            });
            state.mailbox_set_creates.push((name, parent, role));
            created_resp.insert(creation_id.clone(), json!({ "id": new_id }));
        }
    }
    let mut updated_resp = serde_json::Map::new();
    let mut not_updated_resp = serde_json::Map::new();
    if let Some(updates) = args["update"].as_object() {
        for (id, props) in updates {
            if state.mailbox_set_reject_update_ids.iter().any(|x| x == id) {
                not_updated_resp.insert(
                    id.clone(),
                    json!({
                        "type": "invalidProperties",
                        "description": format!("rigged rejection for {}", id),
                    }),
                );
                continue;
            }
            let new_name = props["name"].as_str().unwrap_or_default().to_string();
            let new_parent = props["parentId"].as_str().map(|s| s.to_string());
            if let Some(mb) = state.mailboxes.iter_mut().find(|m| m.id == *id) {
                mb.name = new_name.clone();
                mb.parent_id = new_parent.clone();
            }
            state
                .mailbox_set_updates
                .push((id.clone(), new_name, new_parent));
            updated_resp.insert(id.clone(), Value::Null);
        }
    }
    let old_state = state.mailbox_state.clone();
    state.mailbox_state = format!("{}-set", old_state);
    json!([
        "Mailbox/set",
        {
            "accountId": ACCOUNT_ID,
            "oldState": old_state,
            "newState": state.mailbox_state,
            "created": Value::Object(created_resp),
            "updated": Value::Object(updated_resp),
            "destroyed": [],
            "notCreated": Value::Object(not_created_resp),
            "notUpdated": Value::Object(not_updated_resp),
            "notDestroyed": {},
        },
        call_id
    ])
}

fn email_query(args: &Value, call_id: &str, state: &mut MockState) -> Value {
    // Two filter shapes are supported, matching the two callers
    // in the engine + janitor today:
    //   - `{ "inMailbox": "<id>" }`: SyncEngine's per-mailbox query
    //   - `{ "operator": "OR", "conditions": [{ "header": ["Message-ID", "<id>"] }, ...] }`:
    //     rebindfolders' Message-ID probe
    // Anything else returns an empty result; tests using new
    // filter shapes need to extend this dispatcher.
    let filter = &args["filter"];
    let ids: Vec<String> = if let Some(mailbox_id) = filter["inMailbox"].as_str() {
        state
            .emails
            .iter()
            .filter(|e| e.mailbox_ids.iter().any(|m| m == mailbox_id))
            .map(|e| e.id.clone())
            .collect()
    } else if filter["operator"].as_str() == Some("OR") {
        // OR-of-headers: collect every `header[0]: header[1]`
        // pair, match emails whose `messageId` contains the value
        // (with our angle brackets stripped to compare against the
        // server's stored bracket-stripped form).
        let mut wanted: Vec<String> = Vec::new();
        if let Some(conditions) = filter["conditions"].as_array() {
            for cond in conditions {
                let Some(header) = cond["header"].as_array() else {
                    continue;
                };
                if header.len() != 2 {
                    continue;
                }
                let Some(name) = header[0].as_str() else {
                    continue;
                };
                let Some(value) = header[1].as_str() else {
                    continue;
                };
                if !name.eq_ignore_ascii_case("Message-ID") {
                    continue;
                }
                let stripped = value
                    .strip_prefix('<')
                    .and_then(|s| s.strip_suffix('>'))
                    .unwrap_or(value);
                wanted.push(stripped.to_string());
            }
        }
        state
            .emails
            .iter()
            .filter(|e| {
                e.message_id
                    .as_ref()
                    .is_some_and(|m| wanted.iter().any(|w| w == m))
            })
            .map(|e| e.id.clone())
            .collect()
    } else {
        Vec::new()
    };
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

fn email_get(args: &Value, call_id: &str, state: &mut MockState) -> Value {
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

fn email_changes(args: &Value, call_id: &str, state: &mut MockState) -> Value {
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
            allow_destructive_folder_sync: AllowDestructiveFolderSync::None,
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
            parent_id: None,
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
            MockMailbox {
                id: "MB-SPAM".to_string(),
                name: "Spam".to_string(),
                role: Some("junk".to_string()),
                parent_id: None,
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
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
            parent_id: None,
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
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
            parent_id: None,
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
            parent_id: None,
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
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

/// Server-side mailbox rename: the JMAP id is stable, the `name`
/// changes, and the resolved on-disk folder follows. `resolve_
/// mailboxes` must rename the maildir directory, rewrite the
/// `local_state.maildir_folder` rows that pointed at the old
/// path, and refresh the sentinel + mailbox_map row at the new
/// path. The file content inside the folder must survive the
/// move byte-identical.
#[tokio::test]
async fn sync_follows_server_side_rename_on_disk() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // First cycle: both mailboxes synced and cached. Drop a
    // sentinel file and a local_state row under Archive so we can
    // verify both follow the rename.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    let archive_old = temp.path().join("Archive");
    assert!(archive_old.join("cur").is_dir());
    // Drop one stub message file so we can prove the rename moved
    // the folder content byte-identical, not recreated an empty
    // shell. Maildir filename shape so scan accepts it on the
    // post-rename walk.
    std::fs::write(
        archive_old.join("cur").join("1.host:2,S"),
        b"sentinel-payload",
    )
    .expect("write stub");
    // Seed local_state with a row pointing at Archive so we can
    // confirm the rename rewrites the column.
    queries::upsert_local_state(
        &conn,
        &jma_mail::ids::MaildirId::from("M-OLD"),
        "Archive",
        "",
        None,
    )
    .expect("seed local_state");

    // Server-side rename: Archive's display name flips to Archives;
    // the id stays MB-ARCH.
    {
        let mut st = state.lock().unwrap();
        st.mailboxes
            .iter_mut()
            .find(|m| m.id == "MB-ARCH")
            .unwrap()
            .name = "Archives".to_string();
        st.mailbox_state = "mb-2".to_string();
    }

    // Second cycle: resolve_mailboxes must rename Archive to
    // Archives on disk, rewrite the local_state row, and refresh
    // the cached mailbox_map row + sentinel at the new path.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync");

    let archive_new = temp.path().join("Archives");
    assert!(
        archive_new.join("cur").is_dir(),
        "the renamed folder must exist at the new on-disk path"
    );
    assert!(
        !archive_old.exists(),
        "the old on-disk path must be gone after the rename"
    );
    // File content inside the folder must survive the move
    // byte-identical -- the rename was an `fs::rename`, not a
    // recreate.
    let stub_bytes =
        std::fs::read(archive_new.join("cur").join("1.host:2,S")).expect("read stub from new path");
    assert_eq!(stub_bytes, b"sentinel-payload");
    // mailbox_map row's maildir_folder now points at Archives.
    let cached = queries::get_mailbox(&conn, &jma_mail::ids::JmapMailboxId::from("MB-ARCH"))
        .unwrap()
        .expect("MB-ARCH row must still exist");
    assert_eq!(cached.maildir_folder, "Archives");
    assert_eq!(cached.name, "Archives");
    // local_state row was rewritten.
    let state_rows = queries::get_local_state_for_folder(&conn, "Archives").unwrap();
    assert!(
        state_rows.contains_key("M-OLD"),
        "local_state row must follow the rename to the new folder"
    );
    let state_old = queries::get_local_state_for_folder(&conn, "Archive").unwrap();
    assert!(
        !state_old.contains_key("M-OLD"),
        "local_state must not still anchor the row to the old folder"
    );
    // Sentinel at the new path carries the same JMAP id.
    let mapping = sentinel::read(&archive_new)
        .expect("sentinel read")
        .expect("sentinel must exist at the new path");
    assert_eq!(mapping.jmap_mailbox_id.as_ref(), "MB-ARCH");
    assert_eq!(mapping.server_name, "Archives");
}

/// `--dry-run` over a server-side rename emits a
/// `RenameLocalMailbox` action without applying any of its
/// user-state mutations: the maildir is not renamed on disk, the
/// `local_state.maildir_folder` rows that pointed at the old
/// path stay where they are, and the sentinel at the old path
/// keeps its pre-rename `server_name`. The
/// `mailbox_map.maildir_folder` cache advance is a known
/// follow-up shared with the CreateLocalMailbox dry-run case
/// and not pinned here.
#[tokio::test]
async fn sync_dry_run_does_not_rename_local_maildir() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // First cycle: real, so the maildirs land and mailbox_map gets
    // populated.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    let archive_old = temp.path().join("Archive");
    assert!(archive_old.join("cur").is_dir());
    queries::upsert_local_state(
        &conn,
        &jma_mail::ids::MaildirId::from("M-OLD"),
        "Archive",
        "",
        None,
    )
    .expect("seed local_state");

    // Server renames Archive -> Archives.
    {
        let mut st = state.lock().unwrap();
        st.mailboxes
            .iter_mut()
            .find(|m| m.id == "MB-ARCH")
            .unwrap()
            .name = "Archives".to_string();
        st.mailbox_state = "mb-2".to_string();
    }

    // Second cycle: --dry-run. The rename action must be emitted
    // through the plan but its executor handler must not run.
    SyncEngine::sync(&conn, &config, true)
        .await
        .expect("dry-run second sync");

    let archive_new = temp.path().join("Archives");
    assert!(
        archive_old.join("cur").is_dir(),
        "the old maildir path must still exist under --dry-run"
    );
    assert!(
        !archive_new.exists(),
        "the new maildir path must not be created under --dry-run"
    );
    let state_rows = queries::get_local_state_for_folder(&conn, "Archive").unwrap();
    assert!(
        state_rows.contains_key("M-OLD"),
        "local_state row must stay anchored to the old folder under --dry-run"
    );
    let mapping = sentinel::read(&archive_old)
        .expect("sentinel read at old path")
        .expect("sentinel must still be present at the old path");
    assert_eq!(
        mapping.server_name, "Archive",
        "sentinel at the old path must keep the pre-rename server_name under --dry-run"
    );
}

/// Cascading rename: a parent mailbox is renamed in the same
/// cycle that the engine must also reconcile a child mailbox.
/// The default test config uses the Fs layout with `/` as the
/// hierarchy separator, so the child lives at `<parent>/<child>`
/// on disk -- renaming "Personal" to "Private" moves the entire
/// subtree including "Personal/Notes" to "Private/Notes" in a
/// single `fs::rename`, and the child's iteration then sees its
/// old path already absent (the parent rename swept it along)
/// while the new path is present.
///
/// This pins the shallowest-first ordering in `resolve_mailboxes`:
/// under naive insertion order, processing the child first would
/// `fs::rename` against a parent dir that no longer exists; with
/// the depth sort, the parent's rename completes first, the
/// child's queued `RenameLocalMailbox` then recovers via the
/// executor's source-missing/target-present branch in
/// `rename_local_mailboxes`, where DB catch-up lands.
#[tokio::test]
async fn sync_follows_cascading_parent_child_rename() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-PARENT".to_string(),
                name: "Personal".to_string(),
                role: None,
                parent_id: None,
            },
            MockMailbox {
                id: "MB-CHILD".to_string(),
                name: "Notes".to_string(),
                role: None,
                parent_id: Some("MB-PARENT".to_string()),
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // First cycle: parent + child land at their Fs-layout paths.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    let parent_old = temp.path().join("Personal");
    let child_old = parent_old.join("Notes");
    assert!(
        parent_old.join("cur").is_dir(),
        "parent must exist at Personal"
    );
    assert!(
        child_old.join("cur").is_dir(),
        "child must exist at Personal/Notes under Fs layout"
    );
    // Drop stub files in both so we can verify the rename moved
    // content rather than recreating empty shells. Maildir
    // filename shape so scan accepts them on the post-rename
    // walk.
    std::fs::write(parent_old.join("cur").join("1.host:2,S"), b"parent-payload")
        .expect("write parent stub");
    std::fs::write(child_old.join("cur").join("2.host:2,S"), b"child-payload")
        .expect("write child stub");

    // Server-side rename: parent's name changes. Under Fs layout
    // the child's folder path is the parent's path joined with
    // the child's name, so renaming Personal to Private moves
    // Personal/Notes to Private/Notes in the same `fs::rename`.
    {
        let mut st = state.lock().unwrap();
        st.mailboxes
            .iter_mut()
            .find(|m| m.id == "MB-PARENT")
            .unwrap()
            .name = "Private".to_string();
        st.mailbox_state = "mb-2".to_string();
    }

    // Second cycle: both folders must land at their new paths.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync");

    let parent_new = temp.path().join("Private");
    let child_new = parent_new.join("Notes");
    assert!(
        parent_new.join("cur").is_dir(),
        "parent must have moved to Private"
    );
    assert!(
        child_new.join("cur").is_dir(),
        "child must have moved to Private/Notes alongside the parent rename"
    );
    assert!(!parent_old.exists(), "old parent path must be gone");
    assert!(!child_old.exists(), "old child path must be gone");

    // Both stub files must survive byte-identical, proving the
    // rename moved the folder content rather than recreating
    // empty shells.
    let parent_bytes = std::fs::read(parent_new.join("cur").join("1.host:2,S"))
        .expect("read parent stub from new path");
    assert_eq!(parent_bytes, b"parent-payload");
    let child_bytes = std::fs::read(child_new.join("cur").join("2.host:2,S"))
        .expect("read child stub from new path");
    assert_eq!(child_bytes, b"child-payload");

    // mailbox_map rows for both ids carry the new folders.
    let parent_cached =
        queries::get_mailbox(&conn, &jma_mail::ids::JmapMailboxId::from("MB-PARENT"))
            .unwrap()
            .expect("MB-PARENT row must exist");
    assert_eq!(parent_cached.maildir_folder, "Private");
    let child_cached = queries::get_mailbox(&conn, &jma_mail::ids::JmapMailboxId::from("MB-CHILD"))
        .unwrap()
        .expect("MB-CHILD row must exist");
    assert_eq!(child_cached.maildir_folder, "Private/Notes");
}

/// End-to-end rebindfolders: an on-disk folder with no sentinel and
/// no mailbox_map row gets rebound to the JMAP mailbox its sample
/// Message-IDs all live in. Pins the full plumbing (Mailbox/get,
/// Email/query with OR-of-headers, Email/get, intersection,
/// sentinel write).
#[tokio::test]
async fn rebindfolders_rebinds_orphan_folder_from_message_ids() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-SENT".to_string(),
                name: "Sent".to_string(),
                role: Some("sent".to_string()),
                parent_id: None,
            },
        ],
        emails: vec![
            MockEmail {
                id: "E1".to_string(),
                blob_id: "B1".to_string(),
                thread_id: "T1".to_string(),
                mailbox_ids: vec!["MB-SENT".to_string()],
                keywords: vec!["$seen".to_string()],
                message_id: Some("a@x".to_string()),
            },
            MockEmail {
                id: "E2".to_string(),
                blob_id: "B2".to_string(),
                thread_id: "T2".to_string(),
                mailbox_ids: vec!["MB-SENT".to_string()],
                keywords: vec!["$seen".to_string()],
                message_id: Some("b@x".to_string()),
            },
            // Inbox-only email; its Message-ID is NOT in the
            // sampled set so it shouldn't influence the result.
            MockEmail {
                id: "E3".to_string(),
                blob_id: "B3".to_string(),
                thread_id: "T3".to_string(),
                mailbox_ids: vec!["MB-INBOX".to_string()],
                keywords: vec![],
                message_id: Some("c@x".to_string()),
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // Plant an unbound folder on disk holding messages a@x and b@x
    // (both live server-side under MB-SENT). No sentinel, no
    // mailbox_map row.
    let orphan = temp.path().join("MysterySent");
    std::fs::create_dir_all(orphan.join("cur")).unwrap();
    std::fs::create_dir_all(orphan.join("new")).unwrap();
    std::fs::create_dir_all(orphan.join("tmp")).unwrap();
    std::fs::write(
        orphan.join("cur").join("1.x:2,S"),
        b"Message-ID: <a@x>\r\nSubject: t1\r\n\r\nbody",
    )
    .unwrap();
    std::fs::write(
        orphan.join("cur").join("2.x:2,S"),
        b"Message-ID: <b@x>\r\nSubject: t2\r\n\r\nbody",
    )
    .unwrap();

    let client = jma_mail::jmap::session::connect(&config.account, &conn)
        .await
        .expect("connect");

    let plan = jma_mail::janitor::rebindfolders::plan(
        &client,
        &conn,
        temp.path(),
        jma_mail::janitor::rebindfolders::DEFAULT_SAMPLE_SIZE,
    )
    .await
    .expect("plan");

    assert_eq!(plan.candidates.len(), 1, "expected one rebind candidate");
    let c = &plan.candidates[0];
    assert_eq!(c.folder_path, orphan);
    assert_eq!(c.jmap_mailbox_id, JmapMailboxId::from("MB-SENT"));
    assert_eq!(c.server_name, "Sent");
    assert_eq!(c.sample_count, 2);
    assert!(plan.skipped.is_empty(), "no folders should be skipped");

    // No sentinel until apply runs -- plan() is pure.
    assert!(
        sentinel::read(&orphan).expect("read").is_none(),
        "plan() must not write the sentinel"
    );

    let n = jma_mail::janitor::rebindfolders::apply(&plan).expect("apply");
    assert_eq!(n, 1);
    let written = sentinel::read(&orphan).expect("read").expect("present");
    assert_eq!(written.jmap_mailbox_id, JmapMailboxId::from("MB-SENT"));
    assert_eq!(written.server_name, "Sent");
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
            parent_id: None,
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
            parent_id: None,
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
            parent_id: None,
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
        mailbox_set_creates: Vec::new(),
        mailbox_set_reject_names: Vec::new(),
        mailbox_set_updates: Vec::new(),
        mailbox_set_reject_update_ids: Vec::new(),
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
            parent_id: None,
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

/// `CreateRemoteMailbox` actions in a `SyncPlan` issue one
/// `Mailbox/set { create }` per action with the correct name,
/// optional parent id, and optional role. The handler stub
/// records each create; the assertions pin both the wire-side
/// triple and that all three actions land (no early bail on
/// success).
#[tokio::test]
async fn create_remote_mailbox_issues_mailbox_set_per_action() {
    use jma_mail::ids::JmapMailboxId;
    use jma_mail::sync::execute::Executor;
    use jma_mail::sync::plan::{SyncAction, SyncPlan};
    use std::sync::Arc;

    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![MockMailbox {
            id: "MB-INBOX".to_string(),
            name: "Inbox".to_string(),
            role: Some("inbox".to_string()),
            parent_id: None,
        }],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    let client = jma_mail::jmap::session::connect(&config.account, &conn)
        .await
        .expect("session connects");
    let executor = Executor::new(Arc::new(client), &conn, &config, None);

    let plan = SyncPlan {
        actions: vec![
            SyncAction::CreateRemoteMailbox {
                name: "Projects".to_string(),
                parent_jmap_mailbox_id: None,
                role: None,
                folder: "Projects".to_string(),
            },
            SyncAction::CreateRemoteMailbox {
                name: "Foo".to_string(),
                parent_jmap_mailbox_id: Some(jma_mail::jmap::types::MaybeReference::Value(
                    JmapMailboxId::from("MB-INBOX"),
                )),
                role: None,
                folder: "INBOX.Foo".to_string(),
            },
            SyncAction::CreateRemoteMailbox {
                name: "ImportantStuff".to_string(),
                parent_jmap_mailbox_id: None,
                role: Some("important".to_string()),
                folder: "ImportantStuff".to_string(),
            },
        ],
        new_email_state: None,
    };
    executor.execute(plan).await.expect("execute succeeds");

    let creates = state.lock().unwrap().mailbox_set_creates.clone();
    assert_eq!(
        creates,
        vec![
            ("Projects".to_string(), None, None),
            ("Foo".to_string(), Some("MB-INBOX".to_string()), None),
            (
                "ImportantStuff".to_string(),
                None,
                Some("important".to_string())
            ),
        ]
    );
}

/// A `Mailbox/set { create }` rejection on one action must not
/// cascade: the other actions still land and `execute` returns
/// Ok. Pins the per-action warn-and-continue contract documented
/// on `Executor::create_remote_mailboxes`.
#[tokio::test]
async fn create_remote_mailbox_warns_and_continues_on_rejection() {
    use jma_mail::sync::execute::Executor;
    use jma_mail::sync::plan::{SyncAction, SyncPlan};
    use std::sync::Arc;

    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailbox_set_reject_names: vec!["Rejected".to_string()],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    let client = jma_mail::jmap::session::connect(&config.account, &conn)
        .await
        .expect("session connects");
    let executor = Executor::new(Arc::new(client), &conn, &config, None);

    let plan = SyncPlan {
        actions: vec![
            SyncAction::CreateRemoteMailbox {
                name: "Before".to_string(),
                parent_jmap_mailbox_id: None,
                role: None,
                folder: "Before".to_string(),
            },
            SyncAction::CreateRemoteMailbox {
                name: "Rejected".to_string(),
                parent_jmap_mailbox_id: None,
                role: None,
                folder: "Rejected".to_string(),
            },
            SyncAction::CreateRemoteMailbox {
                name: "After".to_string(),
                parent_jmap_mailbox_id: None,
                role: None,
                folder: "After".to_string(),
            },
        ],
        new_email_state: None,
    };
    executor
        .execute(plan)
        .await
        .expect("execute returns Ok even when one create is rejected");

    let creates = state.lock().unwrap().mailbox_set_creates.clone();
    assert_eq!(
        creates,
        vec![
            ("Before".to_string(), None, None),
            ("After".to_string(), None, None),
        ],
        "rejected create must be absent; surrounding creates must still land"
    );
}

/// Chained create: the plan emits a parent + child pair where the
/// child's parent is `MaybeReference::Reference("Personal")`. The
/// executor processes them top-down; after the parent's create
/// returns its server-assigned id, the child's `Reference` resolves
/// against `creation_refs` and the child fires with the parent's
/// real id. Pins the same-cycle chained-create affordance.
#[tokio::test]
async fn create_remote_mailbox_chained_create_resolves_parent_reference() {
    use jma_mail::sync::execute::Executor;
    use jma_mail::sync::plan::{SyncAction, SyncPlan};
    use std::sync::Arc;

    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    let client = jma_mail::jmap::session::connect(&config.account, &conn)
        .await
        .expect("session connects");
    let executor = Executor::new(Arc::new(client), &conn, &config, None);

    let plan = SyncPlan {
        actions: vec![
            SyncAction::CreateRemoteMailbox {
                name: "Personal".to_string(),
                parent_jmap_mailbox_id: None,
                role: None,
                folder: "Personal".to_string(),
            },
            SyncAction::CreateRemoteMailbox {
                name: "Notes".to_string(),
                parent_jmap_mailbox_id: Some(jma_mail::jmap::types::MaybeReference::Reference(
                    "Personal".to_string(),
                )),
                role: None,
                folder: "Personal.Notes".to_string(),
            },
        ],
        new_email_state: None,
    };
    executor.execute(plan).await.expect("execute succeeds");

    // Look up the parent's assigned id from the mock's record
    // instead of hard-coding the mock's counter shape, so the
    // assertion stays meaningful if the mock's id-assignment
    // changes.
    let st = state.lock().unwrap();
    let creates = st.mailbox_set_creates.clone();
    let parent_id = st
        .mailboxes
        .iter()
        .find(|m| m.name == "Personal")
        .map(|m| m.id.clone())
        .expect("Personal must have been created");
    // Drop the guard before the second `state.lock()` below;
    // without this the re-lock for `notes_id` would deadlock
    // against this guard.
    drop(st);
    assert_eq!(creates.len(), 2);
    assert_eq!(creates[0], ("Personal".to_string(), None, None));
    // The child's parentId carries the parent's freshly-assigned
    // id (resolved from the Reference via creation_refs), not
    // the Reference string itself.
    assert_eq!(
        creates[1],
        ("Notes".to_string(), Some(parent_id.clone()), None)
    );

    // Same-cycle sentinel writes: a state-DB nuke between cycles
    // would otherwise leave these folders binding-less on disk,
    // forcing rebindfolders to recover by Message-ID probing.
    // Pin that both sentinels exist and carry the right ids
    // (including the parent reference for the chained child).
    let personal_sentinel = sentinel::read(&temp.path().join("Personal"))
        .expect("read Personal sentinel")
        .expect("Personal sentinel must exist post-create");
    assert_eq!(personal_sentinel.jmap_mailbox_id.as_ref(), parent_id);
    assert_eq!(personal_sentinel.server_name, "Personal");
    assert!(personal_sentinel.parent_jmap_mailbox_id.is_none());

    let notes_path = temp.path().join("Personal.Notes");
    let notes_sentinel = sentinel::read(&notes_path)
        .expect("read Personal.Notes sentinel")
        .expect("Personal.Notes sentinel must exist post-create");
    let notes_id = state
        .lock()
        .unwrap()
        .mailboxes
        .iter()
        .find(|m| m.name == "Notes")
        .map(|m| m.id.clone())
        .expect("Notes must be in mock state");
    assert_eq!(notes_sentinel.jmap_mailbox_id.as_ref(), notes_id);
    assert_eq!(notes_sentinel.server_name, "Notes");
    assert_eq!(
        notes_sentinel
            .parent_jmap_mailbox_id
            .as_ref()
            .map(|p| p.as_ref()),
        Some(parent_id.as_str()),
        "chained child sentinel records the resolved parent id, not the Reference"
    );
    // ensure_maildir runs in the same phase, so cur/new/tmp
    // exist for both folders.
    assert!(temp.path().join("Personal").join("cur").is_dir());
    assert!(notes_path.join("cur").is_dir());
}

/// A child create whose `Reference` parent doesn't appear in the
/// same plan (parent missing entirely, or rejected by the server
/// earlier in the phase) must warn-skip without firing a JMAP
/// call. The next cycle's reconcile re-emits once the parent
/// settles.
#[tokio::test]
async fn create_remote_mailbox_skips_child_when_parent_reference_unresolved() {
    use jma_mail::sync::execute::Executor;
    use jma_mail::sync::plan::{SyncAction, SyncPlan};
    use std::sync::Arc;

    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    let client = jma_mail::jmap::session::connect(&config.account, &conn)
        .await
        .expect("session connects");
    let executor = Executor::new(Arc::new(client), &conn, &config, None);

    let plan = SyncPlan {
        actions: vec![SyncAction::CreateRemoteMailbox {
            name: "Notes".to_string(),
            parent_jmap_mailbox_id: Some(jma_mail::jmap::types::MaybeReference::Reference(
                "Personal".to_string(),
            )),
            role: None,
            folder: "Personal.Notes".to_string(),
        }],
        new_email_state: None,
    };
    executor.execute(plan).await.expect("execute returns Ok");

    let creates = state.lock().unwrap().mailbox_set_creates.clone();
    assert!(
        creates.is_empty(),
        "child must be warn-skipped when parent reference is unresolved, got {:?}",
        creates
    );
}
/// Local-only rename push: the user `mv`-ed the maildir but the
/// server hasn't changed. The sentinel walk finds the tracked id
/// at a disk path the cache (and the server) disagree with, and
/// resolve_mailboxes emits a `RenameRemoteMailbox` that lands a
/// `Mailbox/set { update }` with the new name. After success the
/// cache advances so subsequent cycles see all three views in
/// agreement.
#[tokio::test]
async fn sync_pushes_local_rename_to_server() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // First cycle: maildir lands at Archive/ with a sentinel.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    let archive_old = temp.path().join("Archive");
    assert!(archive_old.join("cur").is_dir());

    // User renames disk Archive/ -> Archives/ (the sentinel
    // travels with the directory; server still says Archive).
    let archives_new = temp.path().join("Archives");
    std::fs::rename(&archive_old, &archives_new).expect("local mv");

    // Second cycle: the sentinel walk finds MB-ARCH at Archives,
    // server still says Archive, cache still says Archive. Emit
    // RenameRemoteMailbox; executor pushes Mailbox/set { update }.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync");

    let updates = state.lock().unwrap().mailbox_set_updates.clone();
    assert_eq!(
        updates,
        vec![("MB-ARCH".to_string(), "Archives".to_string(), None)],
        "exactly one Mailbox/set update must land with the new name"
    );
    let cached = queries::get_mailbox(&conn, &jma_mail::ids::JmapMailboxId::from("MB-ARCH"))
        .unwrap()
        .expect("MB-ARCH row must still exist");
    assert_eq!(
        cached.maildir_folder, "Archives",
        "cache must advance to the disk folder after Mailbox/set succeeds"
    );
    assert_eq!(
        cached.name, "Archives",
        "cache must record the new server-side name"
    );
    // Server-side state now agrees.
    let server_name = state
        .lock()
        .unwrap()
        .mailboxes
        .iter()
        .find(|m| m.id == "MB-ARCH")
        .unwrap()
        .name
        .clone();
    assert_eq!(server_name, "Archives", "server mailbox must be renamed");
}

/// `--dry-run` over a local rename emits the
/// `RenameRemoteMailbox` action but the executor never runs, so
/// no `Mailbox/set { update }` reaches the server and the cache
/// stays anchored to the pre-rename folder. Mirrors the existing
/// dry-run safety guarantee for the server-side rename path.
#[tokio::test]
async fn sync_dry_run_does_not_push_local_rename() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    let archive_old = temp.path().join("Archive");
    let archives_new = temp.path().join("Archives");
    std::fs::rename(&archive_old, &archives_new).expect("local mv");

    SyncEngine::sync(&conn, &config, true)
        .await
        .expect("dry-run second sync");

    assert!(
        state.lock().unwrap().mailbox_set_updates.is_empty(),
        "no Mailbox/set update may land under --dry-run"
    );
    let cached = queries::get_mailbox(&conn, &jma_mail::ids::JmapMailboxId::from("MB-ARCH"))
        .unwrap()
        .expect("MB-ARCH row must still exist");
    assert_eq!(
        cached.maildir_folder, "Archive",
        "cache must stay anchored to the pre-rename folder under --dry-run"
    );
}

/// `apply_unconditional_mailbox_writes` must not fire under
/// `--dry-run`. Pre-populate `mailbox_map` with a row whose
/// metadata disagrees with the freshly fetched server view, run
/// `SyncEngine::sync(.., dry_run = true)`, and assert the row is
/// unchanged -- the metadata-refresh write the drain would
/// otherwise apply must stay un-fired. Then run the same sync
/// with `dry_run = false` as a positive control so a regression
/// that broke the drain entirely doesn't pass the dry-run assertion
/// silently.
///
/// Regression test for a placement bug: the drain was originally
/// called immediately after `resolve_mailboxes` returned, which
/// is BEFORE the dry-run early return in `SyncEngine::run`. The
/// shell/core split that introduced the drain was supposed to
/// move the writes past the dry-run check; the placement
/// inadvertently landed above it instead.
#[tokio::test]
async fn dry_run_does_not_drain_apply_unconditional_mailbox_writes() {
    let server = MockServer::start().await;
    mount_session(&server).await;

    let state = Arc::new(Mutex::new(MockState {
        mailbox_state: "mb-1".to_string(),
        email_state: "e-1".to_string(),
        mailboxes: vec![MockMailbox {
            id: "MB-INBOX".to_string(),
            name: "Inbox".to_string(),
            role: Some("inbox".to_string()),
            parent_id: None,
        }],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // Pre-populate mailbox_map with a row whose `name` and
    // `sort_order` disagree with what the mock server returns.
    // The metadata-refresh write `apply_unconditional_mailbox_writes`
    // stages is the only thing in `run()` that would bring the
    // cache into agreement; if it fires under dry-run, the row
    // updates.
    queries::upsert_mailbox(
        &conn,
        &queries::MailboxRecord {
            jmap_mailbox_id: "MB-INBOX".into(),
            name: "STALE_NAME".to_string(),
            role: Some("inbox".to_string()),
            parent_id: None,
            maildir_folder: "INBOX".to_string(),
            sort_order: 99,
            remote_path: Some("INBOX".to_string()),
        },
    )
    .unwrap();

    // Stamp the on-disk side so the `Unchanged` arm doesn't push
    // a `CreateLocalMailbox` action and we're observing the pure
    // metadata-refresh path. Sentinel content matches the server
    // view so the binding is in steady state.
    let inbox_path = temp.path().join("INBOX");
    std::fs::create_dir_all(inbox_path.join("cur")).unwrap();
    std::fs::create_dir_all(inbox_path.join("new")).unwrap();
    std::fs::create_dir_all(inbox_path.join("tmp")).unwrap();
    sentinel::write(
        &inbox_path,
        &sentinel::MailboxMapping {
            jmap_mailbox_id: "MB-INBOX".into(),
            parent_jmap_mailbox_id: None,
            server_name: "Inbox".to_string(),
        },
    )
    .unwrap();

    SyncEngine::sync(&conn, &config, true)
        .await
        .expect("dry-run sync succeeds");

    let rows = queries::get_all_mailboxes(&conn).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].name, "STALE_NAME",
        "dry-run must not refresh mailbox_map.name"
    );
    assert_eq!(
        rows[0].sort_order, 99,
        "dry-run must not refresh mailbox_map.sort_order"
    );

    // Positive control: a non-dry-run sync refreshes the
    // metadata. Without this, a regression that broke the drain
    // entirely (or staged the wrong record) would also satisfy
    // the dry-run assertion.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("commit sync succeeds");
    let rows = queries::get_all_mailboxes(&conn).unwrap();
    assert_eq!(rows[0].name, "Inbox", "commit sync refreshes name");
    assert_eq!(rows[0].sort_order, 0, "commit sync refreshes sort_order");
}

/// 3-way conflict, `conflict_strategy = ServerWins`: the server
/// renamed and the user also `mv`-ed locally to a third name.
/// ServerWins picks the server's view, emitting a
/// `RenameLocalMailbox` from the user's local name back to the
/// server's name (overwriting the user's intent). No
/// `Mailbox/set { update }` lands.
#[tokio::test]
async fn sync_conflict_rename_serverwins_overwrites_local() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let mut config = test_config(&server, temp.path(), vec![]);
    config.sync.conflict_strategy = ConflictStrategy::ServerWins;

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");

    // Local: Archive -> MyArchive; server: Archive -> Archives.
    std::fs::rename(temp.path().join("Archive"), temp.path().join("MyArchive")).expect("local mv");
    {
        let mut st = state.lock().unwrap();
        st.mailboxes
            .iter_mut()
            .find(|m| m.id == "MB-ARCH")
            .unwrap()
            .name = "Archives".to_string();
        st.mailbox_state = "mb-2".to_string();
    }

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync");

    assert!(
        state.lock().unwrap().mailbox_set_updates.is_empty(),
        "ServerWins must not push the local rename"
    );
    assert!(
        temp.path().join("Archives").join("cur").is_dir(),
        "disk must end at the server's name under ServerWins"
    );
    assert!(
        !temp.path().join("MyArchive").exists(),
        "user's local-rename target must be overwritten under ServerWins"
    );
    let cached = queries::get_mailbox(&conn, &jma_mail::ids::JmapMailboxId::from("MB-ARCH"))
        .unwrap()
        .expect("MB-ARCH row must still exist");
    assert_eq!(cached.maildir_folder, "Archives");
}

/// 3-way conflict, `conflict_strategy = LocalWins`: the user's
/// local rename wins. Emit `RenameRemoteMailbox` pushing the
/// user's name to the server; no `RenameLocalMailbox`
/// overwriting disk. After the cycle, server and disk both carry
/// the user's name and the cache catches up.
#[tokio::test]
async fn sync_conflict_rename_localwins_overwrites_server() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let mut config = test_config(&server, temp.path(), vec![]);
    config.sync.conflict_strategy = ConflictStrategy::LocalWins;

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");

    // Local: Archive -> MyArchive; server: Archive -> Archives.
    std::fs::rename(temp.path().join("Archive"), temp.path().join("MyArchive")).expect("local mv");
    {
        let mut st = state.lock().unwrap();
        st.mailboxes
            .iter_mut()
            .find(|m| m.id == "MB-ARCH")
            .unwrap()
            .name = "Archives".to_string();
        st.mailbox_state = "mb-2".to_string();
    }

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync");

    let updates = state.lock().unwrap().mailbox_set_updates.clone();
    assert_eq!(
        updates,
        vec![("MB-ARCH".to_string(), "MyArchive".to_string(), None)],
        "LocalWins must push the user's name to the server"
    );
    assert!(
        temp.path().join("MyArchive").join("cur").is_dir(),
        "disk must remain at the user's name under LocalWins"
    );
    let cached = queries::get_mailbox(&conn, &jma_mail::ids::JmapMailboxId::from("MB-ARCH"))
        .unwrap()
        .expect("MB-ARCH row must still exist");
    assert_eq!(
        cached.maildir_folder, "MyArchive",
        "cache must advance to the user's name after Mailbox/set succeeds"
    );
}

/// A pure local rename whose Mailbox/set rejection (rigged via
/// the mock's `mailbox_set_reject_update_ids`) must leave the
/// cache anchored at the pre-rename folder so the next cycle's
/// sentinel walk re-emits the same action. Pins the warn-and-
/// continue contract: one server-side rejection doesn't cascade
/// into a corrupted cache.
#[tokio::test]
async fn sync_local_rename_rejection_leaves_cache_for_retry() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        mailbox_set_reject_update_ids: vec!["MB-ARCH".to_string()],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    std::fs::rename(temp.path().join("Archive"), temp.path().join("Archives")).expect("local mv");

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync (rejection is warn-and-continue)");

    let cached = queries::get_mailbox(&conn, &jma_mail::ids::JmapMailboxId::from("MB-ARCH"))
        .unwrap()
        .expect("MB-ARCH row must still exist");
    assert_eq!(
        cached.maildir_folder, "Archive",
        "cache must stay at the pre-rename folder after Mailbox/set rejection"
    );
    let server_name = state
        .lock()
        .unwrap()
        .mailboxes
        .iter()
        .find(|m| m.id == "MB-ARCH")
        .unwrap()
        .name
        .clone();
    assert_eq!(
        server_name, "Archive",
        "server must not have applied the update"
    );
}

/// A mailbox whose disk path was produced by a configured
/// `MapDirectly` rename rule (server `Archive` -> on-disk
/// `MyArchive`) must not produce a `RenameRemoteMailbox` when
/// the user re-renames the disk folder again. The rule's
/// inverse is not generally computable, so pushing a naive
/// decomposition would silently overwrite the server name with
/// whatever the user typed in place of the rule's output. The
/// guard logs a warn and leaves the cache alone so the next
/// cycle re-applies the rule and surfaces the drift instead of
/// fighting it.
#[tokio::test]
async fn sync_skips_local_rename_when_path_was_resolved_by_rule() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let mut config = test_config(&server, temp.path(), vec![]);
    config.compiled_rename_rules = vec![CompiledRenameRule::MapDirectly {
        source_folder_path: "Archive".to_string(),
        renamed_name: "MyArchive".to_string(),
    }];

    // First cycle: server's Archive maps onto on-disk MyArchive
    // via the rule.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    assert!(
        temp.path().join("MyArchive").join("cur").is_dir(),
        "rule-mapped folder must land at MyArchive"
    );

    // User renames the rule-output folder.
    std::fs::rename(temp.path().join("MyArchive"), temp.path().join("StillMine"))
        .expect("local mv");

    // Second cycle: the guard must refuse the local-rename
    // push and leave the cache alone.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync");

    assert!(
        state.lock().unwrap().mailbox_set_updates.is_empty(),
        "rule-mapped mailbox must not produce a Mailbox/set update"
    );
    let cached = queries::get_mailbox(&conn, &jma_mail::ids::JmapMailboxId::from("MB-ARCH"))
        .unwrap()
        .expect("MB-ARCH row must still exist");
    assert_eq!(
        cached.maildir_folder, "MyArchive",
        "cache must stay at the rule's output, ignoring the user's local rename"
    );
}

/// `resolve_mailboxes` drops the `mailbox_map` row for any id the
/// server no longer advertises. Pins three properties of the
/// deletion-detection path:
///
/// 1. After a server-side deletion (id removed from the next
///    `Mailbox/get` response), the cached row goes away.
/// 2. The on-disk maildir for the deleted mailbox is left alone.
/// 3. Mailboxes the server still has are untouched.
#[tokio::test]
async fn resolve_mailboxes_drops_row_for_server_side_deletion() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // First cycle: both mailboxes resolved, cached, and provisioned
    // on disk via the executor's `create_local_mailboxes` phase
    // (a separate phase from `resolve_mailboxes` since 781d8cb).
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    let ids_first: Vec<_> = queries::list_known_mailbox_ids(&conn)
        .unwrap()
        .into_iter()
        .map(|id| id.as_ref().to_string())
        .collect();
    assert_eq!(ids_first, vec!["MB-ARCH", "MB-INBOX"]);
    assert!(temp.path().join("Archive").join("cur").is_dir());

    // Server-side deletion: Archive vanishes from the server's
    // Mailbox/get response. The on-disk maildir is intentionally
    // left in place by the test -- the deletion-detection path's
    // contract is to drop the cache row only; disk state is not
    // touched (destructive resolution is opt-in elsewhere).
    {
        let mut st = state.lock().unwrap();
        st.mailboxes.retain(|m| m.id != "MB-ARCH");
        st.mailbox_state = "mb-2".to_string();
    }

    // Second cycle: only INBOX comes back from the server, and the
    // engine should drop MB-ARCH from mailbox_map.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync");

    let ids_second: Vec<_> = queries::list_known_mailbox_ids(&conn)
        .unwrap()
        .into_iter()
        .map(|id| id.as_ref().to_string())
        .collect();
    assert_eq!(
        ids_second,
        vec!["MB-INBOX"],
        "Archive row must be dropped; Inbox must survive"
    );
    assert!(
        temp.path().join("Archive").join("cur").is_dir(),
        "on-disk maildir for the deleted mailbox must survive the cache cleanup"
    );
    assert!(
        temp.path().join("INBOX").join("cur").is_dir(),
        "Inbox maildir must still be present"
    );
}

/// Server-side mailbox deletion also produces a local-orphan
/// record on the returned `MailboxBindings`. The record carries
/// the last-known binding (id + server_name + maildir_folder +
/// remote_path) and the cached parent, so plan readers observe
/// the dead binding without re-reading the dropped `mailbox_map`
/// row. Sibling to
/// `resolve_mailboxes_drops_row_for_server_side_deletion`, which
/// pins the cache-row-drop side; this one pins the orphan slot.
#[tokio::test]
async fn resolve_mailboxes_records_local_orphan_for_server_side_deletion() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");

    {
        let mut st = state.lock().unwrap();
        st.mailboxes.retain(|m| m.id != "MB-ARCH");
        st.mailbox_state = "mb-2".to_string();
    }

    // Drive resolve_mailboxes directly so the local_orphans slot
    // is observable; the data-shape contract is what we want to
    // pin.
    let engine = SyncEngine::connect(&conn, &config)
        .await
        .expect("connect engine");
    let bindings = engine.resolve_mailboxes().await.expect("resolve mailboxes");

    let orphans = bindings.local_orphans();
    assert_eq!(
        orphans.len(),
        1,
        "exactly one local orphan must be recorded"
    );
    let orphan = &orphans[0];
    assert_eq!(
        orphan
            .binding
            .jmap_mailbox_id
            .expect_resolved("orphan binding is resolved")
            .as_ref(),
        "MB-ARCH"
    );
    assert_eq!(orphan.binding.server_name, "Archive");
    assert_eq!(orphan.binding.maildir_folder, "Archive");
    assert_eq!(orphan.parent_jmap_mailbox_id, None);
}

/// Post-DB-nuke recovery: when `mailbox_map` is empty at cycle
/// start (fresh DB, schema-version nuke, etc.) the cache-vs-
/// server diff finds no orphans. A surviving `.jma.mapping`
/// sentinel whose id the server no longer advertises must still
/// be detected. The sentinel itself supplies `server_name` and
/// `parent_jmap_mailbox_id` for the local-orphan record -- this
/// test exercises the parent-recovery half with a nested orphan
/// (Archive/Old under Archive, both dropped server-side).
#[tokio::test]
async fn resolve_mailboxes_recovers_local_orphan_via_sentinel_post_nuke() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
            MockMailbox {
                id: "MB-OLD".to_string(),
                name: "Old".to_string(),
                role: None,
                parent_id: Some("MB-ARCH".to_string()),
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // First cycle: lands sentinels on disk for all three mailboxes.
    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    assert!(
        temp.path().join("Archive").join("cur").is_dir(),
        "Archive sentinel-bearing folder must exist after first sync"
    );

    // Server deletes Archive and its child Old; we nuke the DB
    // (open a fresh connection on a new path so cache starts
    // empty). Disk still has both sentinels from the first cycle.
    {
        let mut st = state.lock().unwrap();
        st.mailboxes
            .retain(|m| m.id != "MB-ARCH" && m.id != "MB-OLD");
        st.mailbox_state = "mb-2".to_string();
    }
    drop(conn);
    let temp2 = tempfile::tempdir().unwrap();
    let nuked_db = temp2.path().join("state.db");
    let conn = jma_mail::state::db::open_or_recreate(&nuked_db).expect("open fresh DB");
    // `SyncEngine::connect` uses the `&Connection` argument we
    // pass, not `config.state.db_path`, so the new empty `conn`
    // is what makes the cache start fresh.
    let config = test_config(&server, temp.path(), vec![]);

    let engine = SyncEngine::connect(&conn, &config)
        .await
        .expect("connect engine post-nuke");
    let bindings = engine
        .resolve_mailboxes()
        .await
        .expect("resolve mailboxes post-nuke");

    let orphans = bindings.local_orphans();
    assert_eq!(
        orphans.len(),
        2,
        "post-nuke sentinel walk must detect both orphans"
    );

    let arch = orphans
        .iter()
        .find(|o| {
            o.binding
                .jmap_mailbox_id
                .expect_resolved("resolved")
                .as_ref()
                == "MB-ARCH"
        })
        .expect("Archive orphan must be recorded");
    assert_eq!(arch.binding.server_name, "Archive");
    assert_eq!(arch.binding.maildir_folder, "Archive");
    assert_eq!(arch.parent_jmap_mailbox_id, None);

    let old = orphans
        .iter()
        .find(|o| {
            o.binding
                .jmap_mailbox_id
                .expect_resolved("resolved")
                .as_ref()
                == "MB-OLD"
        })
        .expect("Old orphan must be recorded");
    assert_eq!(old.binding.server_name, "Old");
    assert_eq!(
        old.parent_jmap_mailbox_id.as_ref().map(|id| id.as_ref()),
        Some("MB-ARCH"),
        "sentinel-supplied parent must survive the post-nuke walk"
    );
}

/// Local maildir deletion surfaces as a remote-orphan in the
/// post-sync `SyncOutcome.remote_orphans_detected` count. The
/// user `rm -rf`s a previously-synced maildir; the next sync
/// cycle's scan sees `try_open_maildir` return None for the
/// binding, the sentinel walk finds no surviving sentinel, and
/// neither the new-mailbox nor rename guards apply -- so scan
/// emits `LocalChange::LocalFolderDeleted` and the engine
/// converts it into a `RemoteOrphanRecord` before reconcile
/// runs. No re-creation of the deleted maildir, no destructive
/// SyncAction (this commit is detection-only).
#[tokio::test]
async fn sync_detects_remote_orphan_for_locally_deleted_folder() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        // Seed one email on the server so the post-orphan
        // count hydration's `Email/query (limit: 0)` returns
        // a non-zero total; the mock's `email_query` returns
        // `state.emails.len()` regardless of `inMailbox`
        // filter, so any non-empty count exercises the
        // hydration round-trip.
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

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync establishes the maildirs and cache rows");
    assert!(
        temp.path().join("Archive").join("cur").is_dir(),
        "Archive maildir must exist after first sync"
    );

    // User deletes the Archive maildir wholesale. The sentinel
    // lived inside `Archive/`, so the post-deletion sentinel
    // walk will find no record for MB-ARCH anywhere.
    std::fs::remove_dir_all(temp.path().join("Archive")).expect("rm -rf Archive");

    let outcome = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync detects the orphan");
    assert_eq!(
        outcome.remote_orphans_detected, 1,
        "scan + engine must surface exactly one remote orphan; got outcome = {:?}",
        outcome
    );
    assert_eq!(
        outcome.remote_orphan_total_emails, 1,
        "the count-hydration `Email/query` round-trip must populate \
         `server_email_count` (mock state has 1 email); got outcome = {:?}",
        outcome
    );

    // The detection path must NOT have re-created the maildir
    // (that would silently undo the user's delete).
    assert!(
        !temp.path().join("Archive").join("cur").is_dir(),
        "detection must not re-create the deleted maildir"
    );
}

/// A local rename (user `mv`-ed the maildir to a different
/// path) leaves the sentinel intact at the new path. Scan's
/// `LocalFolderDeleted` emission consults
/// `MailboxBindings::sentinel_survives_for`; a survived
/// sentinel suppresses the emission and the rename detection
/// inside `decide_mailbox_action` takes the case instead.
#[tokio::test]
async fn sync_does_not_misfire_remote_orphan_on_local_rename() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");

    // User `mv` Archive -> ArchiveRenamed -- the sentinel
    // lives inside the renamed dir, so the walk will surface
    // MB-ARCH at the new path.
    std::fs::rename(
        temp.path().join("Archive"),
        temp.path().join("ArchiveRenamed"),
    )
    .expect("mv Archive ArchiveRenamed");

    let outcome = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync");
    assert_eq!(
        outcome.remote_orphans_detected, 0,
        "local rename must not misfire as a remote orphan; got {:?}",
        outcome
    );
    // Pin that the rename path actually fired: the renamed
    // maildir survives on disk and the cache row's
    // `maildir_folder` got rewritten to match. A
    // regression where scan emits `LocalFolderDeleted` AND a
    // separate orphan-suppression path happens to mask it
    // would pass the count assertion above but fail here.
    assert!(
        temp.path().join("ArchiveRenamed").join("cur").is_dir(),
        "renamed maildir must survive the rename-detection cycle"
    );
    let cached = jma_mail::state::queries::get_mailbox(&conn, &JmapMailboxId::from("MB-ARCH"))
        .expect("query mailbox_map")
        .expect("MB-ARCH cache row exists post-sync");
    assert_eq!(
        cached.maildir_folder, "ArchiveRenamed",
        "rename-detection must rewrite mailbox_map.maildir_folder to the new path"
    );
}

/// First-cycle: a server-known mailbox with no cached
/// `mailbox_map` row, no on-disk maildir, and no sentinel
/// anywhere. The binding lands on `new_mailboxes` for the
/// `CreateLocalMailbox` flow; scan's `is_new_mailbox` guard
/// suppresses any `LocalFolderDeleted` emission so the
/// not-yet-created mailbox doesn't get misclassified as a
/// deletion.
#[tokio::test]
async fn sync_does_not_misfire_remote_orphan_on_first_cycle() {
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
                parent_id: None,
            },
            MockMailbox {
                id: "MB-ARCH".to_string(),
                name: "Archive".to_string(),
                role: Some("archive".to_string()),
                parent_id: None,
            },
        ],
        ..Default::default()
    }));
    mount_jmap(&server, state.clone()).await;

    let temp = tempfile::tempdir().unwrap();
    let conn = fresh_db(&temp);
    let config = test_config(&server, temp.path(), vec![]);

    // No prior sync: cache is empty, disk is empty, no
    // sentinels anywhere. The first cycle sees both
    // server-known mailboxes for the first time.
    let outcome = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    assert_eq!(
        outcome.remote_orphans_detected, 0,
        "first-cycle sync must not misfire as remote orphans; got {:?}",
        outcome
    );

    // Both mailboxes should have landed on disk via
    // CreateLocalMailbox.
    assert!(temp.path().join("INBOX").join("cur").is_dir());
    assert!(temp.path().join("Archive").join("cur").is_dir());

    // The trailing block that pinned `new_mailboxes()`
    // membership was specific to resolve_mailboxes' direct
    // return value -- after the migration to scan + engine
    // detection, the equivalent assertion is that both
    // maildirs got provisioned (above). Re-resolve to confirm
    // the cache rows landed.
    let engine = SyncEngine::connect(&conn, &config)
        .await
        .expect("connect engine for post-sync resolve");
    let bindings = engine.resolve_mailboxes().await.expect("resolve mailboxes");
    let cached_ids: Vec<&str> = bindings
        .iter()
        .map(|b| {
            b.jmap_mailbox_id
                .expect_resolved("post-sync binding is resolved")
                .as_ref()
        })
        .collect();
    assert!(
        cached_ids.contains(&"MB-INBOX") && cached_ids.contains(&"MB-ARCH"),
        "post-sync bindings must include both mailboxes; got {:?}",
        cached_ids
    );

    // First-cycle `new_mailboxes()` is the cycle's own slot
    // for "server-known but not-yet-cached" bindings; after
    // sync persists the cache rows it's expected to be empty
    // on re-resolve (since the rows now exist).
    let new_ids: Vec<&str> = bindings
        .new_mailboxes()
        .iter()
        .map(|n| {
            n.binding
                .jmap_mailbox_id
                .expect_resolved("new mailbox binding is resolved")
                .as_ref()
        })
        .collect();
    assert!(
        new_ids.is_empty(),
        "post-sync re-resolve must see no first-cycle entries; got {:?}",
        new_ids
    );
}
