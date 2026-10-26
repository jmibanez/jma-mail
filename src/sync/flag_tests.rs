//! Stateful flag-sync tests using real Maildir files and SyncEngine cycles.

//! The server applies keyword patches independently of the client, rejects
//! false keyword values, and exposes its state through fresh Email/get calls.
//! A successful HTTP response alone is not a convergence assertion.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::config::FolderLayout;
use crate::config::{
    AccountConfig, AllowDestructiveFolderSync, Config, ConflictStrategy, StateConfig, SyncConfig,
};
use crate::state::{db, queries};
use crate::sync::engine::SyncEngine;
use rusqlite::Connection;
use serde_json::{Value, json};
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

mod generation;
mod import_integration;
mod maildir_cases;
mod partial;
mod pull_ack;
mod repair;

const ACCOUNT_ID: &str = "u-test-account";
const SESSION_STATE: &str = "session-1";

/// Mutable mock-server state shared across the dispatcher closures so a
/// single test can stage multiple outcomes (e.g. pre-load mailboxes and
/// emails, then advance state, or rig the next Email/changes to error
/// with cannotCalculateChanges).
#[derive(Default)]
struct ServerState {
    mailboxes: Vec<RemoteMailbox>,
    mailbox_state: String,
    emails: Vec<RemoteEmail>,
    email_state: String,
    blobs: HashMap<String, Vec<u8>>,
    set_requests: Vec<Value>,
    set_results: Vec<Value>,
    state_mismatches: usize,
    edit_before_set: Option<RemoteEdit>,
    edit_before_set_at: usize,
    edit_before_set_remaining: usize,
    edit_other_before_set: bool,
    edit_after_metadata_get: Option<RemoteEdit>,
    omit_set_new_state_on: Option<usize>,
    rename_before_set: Option<(PathBuf, PathBuf)>,
    rename_many_before_set: Vec<(PathBuf, PathBuf)>,
    rename_many_after_set: Vec<(PathBuf, PathBuf)>,
    blob_requests: usize,
    email_changes_requests: usize,
    email_changes_failures: usize,
    reject_next_update: usize,
    reject_only_id: Option<String>,
    reject_update_type: Option<String>,
    reject_updates: HashMap<String, (usize, String)>,
    http_failures: usize,
    set_method_failures: usize,
    lost_replies: usize,
    omitted_acknowledgements: usize,
    rename_after_set: Option<(std::path::PathBuf, std::path::PathBuf)>,
    hide_on_get: Option<(std::path::PathBuf, std::path::PathBuf)>,
    /// If set, the next Email/changes call returns this error instead
    /// of a normal response. Consumed on use so a follow-up call gets
    /// the regular code path.
    next_email_changes_error: Option<&'static str>,
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
    /// rewrites the matching `RemoteMailbox` so a follow-up
    /// `Mailbox/get` reflects the rename.
    mailbox_set_updates: Vec<(String, String, Option<String>)>,
    /// Ids that `Mailbox/set { update }` must reject. Each
    /// matching update goes into `notUpdated` with
    /// `invalidProperties` and is not appended to
    /// `mailbox_set_updates` / does not rewrite the mailbox.
    mailbox_set_reject_update_ids: Vec<String>,
    /// Mailbox/set { destroy } ids recorded in the order they
    /// arrived. The handler removes the matching `RemoteMailbox`
    /// so a follow-up `Mailbox/get` no longer reflects it.
    mailbox_set_destroys: Vec<String>,
    /// Ids that `Mailbox/set { destroy }` must reject. Each
    /// matching destroy goes into `notDestroyed` with
    /// `mailboxHasEmail` and is not appended to
    /// `mailbox_set_destroys` / does not remove the mailbox.
    /// Pins the warn-and-continue path.
    mailbox_set_reject_destroy_ids: Vec<String>,
}

#[derive(Clone)]
struct RemoteEdit {
    id: Option<String>,
    flags: String,
    keyword_prefix: &'static str,
}

fn apply_remote_edit(state: &mut ServerState, edit: RemoteEdit, fallback_id: Option<&str>) {
    let id = edit
        .id
        .as_deref()
        .or(fallback_id)
        .expect("an actor edit needs an Email ID");
    let email = state
        .emails
        .iter_mut()
        .find(|email| email.id == id)
        .unwrap();
    email.keywords.retain(|key| {
        ![
            "$draft",
            "$flagged",
            "$forwarded",
            "$answered",
            "$seen",
            "$deleted",
        ]
        .contains(&key.as_str())
    });
    email
        .keywords
        .extend(flags_to_keywords(&edit.flags).into_keys());
    email.keywords.push(format!(
        "{}-{}",
        edit.keyword_prefix,
        state.set_requests.len()
    ));
    advance_email_state(state);
}

#[derive(Default, Clone)]
struct EmailChangesResp {
    created: Vec<String>,
    updated: Vec<String>,
    destroyed: Vec<String>,
}

#[derive(Clone)]
struct RemoteMailbox {
    id: String,
    name: String,
    role: Option<String>,
    parent_id: Option<String>,
}

#[derive(Clone)]
struct RemoteEmail {
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
        "state": SESSION_STATE,
    })
}

async fn mount_session(server: &MockServer, max_objects_in_set: u64) {
    let mut doc = session_doc(&server.uri());
    doc["capabilities"]["urn:ietf:params:jmap:core"]["maxObjectsInSet"] = json!(max_objects_in_set);
    Mock::given(method("GET"))
        .and(path("/.well-known/jmap"))
        .respond_with(ResponseTemplate::new(200).set_body_json(doc))
        .mount(server)
        .await;
}

/// Mount the `POST /jmap` dispatcher. Inspects the methodCalls in the
/// request body, dispatches each to the matching handler, and emits a
/// JMAP-shaped methodResponses array.
async fn mount_jmap(server: &MockServer, state: Arc<Mutex<ServerState>>) {
    let state_for_closure = state.clone();
    Mock::given(method("POST"))
        .and(path("/jmap"))
        .respond_with(move |req: &Request| {
            let body: Value = serde_json::from_slice(&req.body).expect("valid JSON body");
            let calls = body["methodCalls"]
                .as_array()
                .expect("methodCalls array")
                .clone();
            let email_set_request = calls.iter().any(|call| call[0] == "Email/set");
            if email_set_request {
                let mut state = state_for_closure.lock().unwrap();
                state.set_requests.push(body.clone());
                if state.http_failures > 0 {
                    state.http_failures -= 1;
                    return ResponseTemplate::new(503);
                }
            }
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
            if email_set_request {
                let mut state = state_for_closure.lock().unwrap();
                if state.lost_replies > 0 {
                    state.lost_replies -= 1;
                    return ResponseTemplate::new(503);
                }
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
async fn mount_blob_downloads(server: &MockServer, state: Arc<Mutex<ServerState>>) {
    let state_for_closure = state.clone();
    Mock::given(method("GET"))
        .and(path_regex(r"^/download/u-test-account/"))
        .respond_with(move |req: &Request| {
            // URL shape: /download/{accountId}/{blobId}/{name}?accept=...
            let url = req.url.path();
            let segments: Vec<&str> = url.trim_start_matches('/').split('/').collect();
            // ["download", accountId, blobId, name]
            let blob_id = segments.get(2).copied().unwrap_or_default();
            let mut st = state_for_closure.lock().unwrap();
            st.blob_requests += 1;
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

fn handle_method(name: &str, args: &Value, call_id: &str, state: &mut ServerState) -> Value {
    match name {
        "Mailbox/get" => mailbox_get(args, call_id, state),
        "Mailbox/set" => mailbox_set(args, call_id, state),
        "Mailbox/changes" => mailbox_changes(call_id),
        "Email/query" => email_query(args, call_id, state),
        "Email/get" => email_get(args, call_id, state),
        "Email/changes" => email_changes(args, call_id, state),
        "Email/set" => email_set(args, call_id, state),
        other => json!([
            "error",
            { "type": "unknownMethod", "description": format!("unhandled: {}", other) },
            call_id
        ]),
    }
}

fn mailbox_get(_args: &Value, call_id: &str, state: &mut ServerState) -> Value {
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

/// Minimal `Mailbox/set { create | update | destroy }`
/// dispatcher. Records each create into `state.mailbox_set_creates`,
/// each update into `state.mailbox_set_updates`, and each destroy
/// into `state.mailbox_set_destroys`, rewriting / appending /
/// removing the matching `RemoteMailbox` so a follow-up
/// `Mailbox/get` reflects the change.
fn mailbox_set(args: &Value, call_id: &str, state: &mut ServerState) -> Value {
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
            state.mailboxes.push(RemoteMailbox {
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
    let mut destroyed_resp: Vec<Value> = Vec::new();
    let mut not_destroyed_resp = serde_json::Map::new();
    if let Some(destroys) = args["destroy"].as_array() {
        for id_val in destroys {
            let Some(id) = id_val.as_str() else {
                continue;
            };
            if state.mailbox_set_reject_destroy_ids.iter().any(|x| x == id) {
                not_destroyed_resp.insert(
                    id.to_string(),
                    json!({
                        "type": "mailboxHasEmail",
                        "description": format!("rigged rejection for {}", id),
                    }),
                );
                continue;
            }
            state.mailboxes.retain(|m| m.id != id);
            state.mailbox_set_destroys.push(id.to_string());
            destroyed_resp.push(Value::String(id.to_string()));
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
            "destroyed": Value::Array(destroyed_resp),
            "notCreated": Value::Object(not_created_resp),
            "notUpdated": Value::Object(not_updated_resp),
            "notDestroyed": Value::Object(not_destroyed_resp),
        },
        call_id
    ])
}

fn email_query(args: &Value, call_id: &str, state: &mut ServerState) -> Value {
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
    let total = ids.len();
    let position = args["position"].as_u64().unwrap_or(0) as usize;
    let limit = args["limit"].as_u64().unwrap_or(total as u64) as usize;
    let ids: Vec<_> = ids.into_iter().skip(position).take(limit).collect();
    json!([
        "Email/query",
        {
            "accountId": ACCOUNT_ID,
            "queryState": "q-1",
            "canCalculateChanges": false,
            "position": position,
            "ids": ids,
            "total": total,
            "limit": limit
        },
        call_id
    ])
}

fn email_get(args: &Value, call_id: &str, state: &mut ServerState) -> Value {
    if let Some((source, held)) = state.hide_on_get.take() {
        std::fs::rename(source, held).unwrap();
    }
    let empty = vec![];
    let id_list = args["ids"].as_array().unwrap_or(&empty);
    let want_all_ids = args.get("ids").is_none();
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
    let not_found: Vec<_> = id_list
        .iter()
        .filter(|id| {
            !state
                .emails
                .iter()
                .any(|email| Some(email.id.as_str()) == id.as_str())
        })
        .cloned()
        .collect();
    let metadata_id = list
        .first()
        .and_then(|email| email["id"].as_str())
        .map(str::to_owned);
    let response = json!([
        "Email/get",
        {
            "accountId": ACCOUNT_ID,
            "state": state.email_state,
            "list": list,
            "notFound": not_found
        },
        call_id
    ]);
    if metadata_id.is_some()
        && let Some(edit) = state.edit_after_metadata_get.take()
    {
        apply_remote_edit(state, edit, metadata_id.as_deref());
    }
    response
}

fn email_changes(args: &Value, call_id: &str, state: &mut ServerState) -> Value {
    state.email_changes_requests += 1;
    if state.email_changes_failures > 0 {
        state.email_changes_failures -= 1;
        return json!(["error", {"type":"serverFail", "description":"injected changes outage"}, call_id]);
    }
    if let Some(err) = state.next_email_changes_error.take() {
        return json!([
            "error",
            { "type": err, "description": format!("rigged {}", err) },
            call_id
        ]);
    }
    let since = args["sinceState"].as_str().unwrap_or_default();
    let changes = if since == state.email_state {
        EmailChangesResp::default()
    } else {
        EmailChangesResp {
            updated: state.emails.iter().map(|email| email.id.clone()).collect(),
            ..Default::default()
        }
    };
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

/// jma's cache-reuse path calls `Mailbox/changes` once a `Mailbox`
/// cursor exists. Return `cannotCalculateChanges` so the engine falls
/// back to the full `Mailbox/get` this mock serves -- every test keeps
/// exercising the same server-structure path it did before the cursor
/// existed, rather than needing the mock to track mailbox-state deltas.
fn mailbox_changes(call_id: &str) -> Value {
    json!([
        "error",
        { "type": "cannotCalculateChanges", "description": "mock: full refetch" },
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

use crate::maildir_ops::flags::{flags_to_keywords, keywords_to_flags};
use std::collections::BTreeSet;
use std::path::PathBuf;

#[tokio::test]
async fn flag_inventory_move_to_new_preserves_remote_identity() {
    let fixture = FlagFixture::new("F", false, ConflictStrategy::ServerWins).await;
    fixture.local_change("F", true, true);
    let result = fixture.sync().await.unwrap();
    assert_eq!(result.deleted, 0, "a mailbox move is not a remote deletion");
    assert_eq!(result.moved, 1);
    assert_converged(&fixture, "F").await;
    assert_eq!(fixture.snapshot().await["file_folder"], "Deleted Items");
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_inventory_live_new_addition_is_detected() {
    let fixture = FlagFixture::new("", false, ConflictStrategy::ServerWins).await;
    fixture.local_change("F", false, true);
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    assert_converged(&fixture, "F").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_inventory_new_suffix_matches_the_maildir_reader() {
    let fixture = FlagFixture::new("F", false, ConflictStrategy::ServerWins).await;
    let record = queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    let id = record.maildir_id.unwrap();
    let maildir =
        crate::maildir_ops::store::ensure_maildir(&fixture.temp.path().join("INBOX")).unwrap();
    let entry = maildir.list_new().next().unwrap().unwrap();
    assert_eq!(entry.flags(), "F");
    let binding = Arc::new(crate::domain::MailboxFolderBinding {
        jmap_mailbox_id: crate::domain::MaybeReference::Value("MB-INBOX".into()),
        server_name: "INBOX".into(),
        maildir_folder: "INBOX".into(),
        remote_path: "INBOX".into(),
    });
    let known = HashMap::from([(id.clone(), ("MB-INBOX".into(), "F".into()))]);
    let scan = crate::maildir_ops::scan::scan_folder(&maildir, &binding, &known).unwrap();
    assert_eq!(scan.local_flags[&id], "F");
}

#[tokio::test]
async fn flag_inventory_source_only_move_event_is_revalidated() {
    let fixture = FlagFixture::new("F", false, ConflictStrategy::ServerWins).await;
    let source = fixture.file();
    fixture.local_change("F", true, true);
    let engine = SyncEngine::connect(&fixture.conn, &fixture.config)
        .await
        .unwrap();
    let result = engine
        .run(
            false,
            crate::sync::plan::SyncDirection::Both,
            crate::sync::engine::ScanScope::Paths(vec![source]),
        )
        .await
        .unwrap();
    assert_eq!(result.deleted, 0);
    assert_eq!(result.moved, 1);
    assert_converged(&fixture, "F").await;
}

#[tokio::test]
async fn flag_inventory_untracked_new_is_not_implicitly_uploaded() {
    let fixture = FlagFixture::new("S", false, ConflictStrategy::ServerWins).await;
    let path = fixture.temp.path().join("INBOX/new/999999999.local:2,F");
    std::fs::write(
        &path,
        b"Message-ID: <local-only@example.com>\r\n\r\nbody\r\n",
    )
    .unwrap();
    let result = fixture.sync().await.unwrap();
    assert_eq!(result.uploaded, 0);
    assert_eq!(result.deleted, 0);
    assert!(path.exists());
    assert_eq!(fixture.state.lock().unwrap().emails.len(), 1);
}

#[tokio::test]
async fn flag_inventory_changed_id_new_move_defers_remote_destruction() {
    let fixture = FlagFixture::new("F", false, ConflictStrategy::ServerWins).await;
    let source = fixture.file();
    let target = fixture
        .temp
        .path()
        .join("Deleted Items/new/999999999.changed:2,F");
    std::fs::rename(source, &target).unwrap();
    let result = fixture.sync().await.unwrap();
    assert_eq!(
        result.deleted, 0,
        "a live identity candidate forbids destructive inference"
    );
    assert!(target.exists());
    assert_eq!(result.deferred_actions, 1);
    assert!(!result.already_in_sync);
    assert_eq!(fixture.state.lock().unwrap().emails.len(), 1);
}

#[tokio::test]
async fn flag_inventory_stale_destroy_plan_is_vetoed_at_execution() {
    let fixture = FlagFixture::new("S", false, ConflictStrategy::ServerWins).await;
    let client = jmap_client::client::Client::new()
        .credentials("test-token")
        .connect(&fixture._server.uri())
        .await
        .unwrap();
    let executor =
        crate::sync::execute::Executor::new(Arc::new(client), &fixture.conn, &fixture.config, None)
            .unwrap();
    let result = executor
        .execute(
            crate::sync::plan::SyncPlan {
                actions: vec![crate::sync::plan::SyncAction::DestroyRemote {
                    id: crate::sync::plan::RemoteId {
                        jmap_email_id: "E1".into(),
                        message_id: "audit@example.com".into(),
                    },
                }],
                new_email_state: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.deleted, 0);
    assert_eq!(result.deferred_actions, 1);
    assert!(fixture.state.lock().unwrap().set_requests.is_empty());
    assert_converged(&fixture, "S").await;
}

#[tokio::test]
async fn flag_patch_local_removals_cover_every_mapped_flag() {
    let mut failures = Vec::new();
    for flag in ['D', 'F', 'P', 'R', 'S', 'T'] {
        let fixture =
            FlagFixture::new(&flag.to_string(), false, ConflictStrategy::ServerWins).await;
        fixture.local_change("", false, false);
        let first = fixture.sync().await.unwrap();
        let snapshot = fixture.snapshot().await;
        let second = fixture.sync().await.unwrap();
        if first.failed_remote_actions != 0
            || snapshot["server_flags"] != ""
            || !second.already_in_sync
        {
            failures.push((flag, snapshot));
        }
    }
    assert!(
        failures.is_empty(),
        "all mapped removals must reach the server: {failures:?}"
    );
}

#[tokio::test]
async fn flag_patch_removal_with_move_preserves_remote_identity() {
    let fixture = FlagFixture::new("FS", false, ConflictStrategy::ServerWins).await;
    fixture.local_change("S", true, false);
    let result = fixture.sync().await.unwrap();
    assert_eq!(result.failed_remote_actions, 0);
    assert_eq!(result.moved, 1);
    assert_eq!(result.deleted, 0);
    assert_converged(&fixture, "S").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_patch_removal_preserves_unmentioned_server_keywords() {
    let fixture = FlagFixture::new("FS", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("S", false, false);
    assert_eq!(fixture.sync().await.unwrap().failed_remote_actions, 0);
    assert_converged(&fixture, "S").await;
    assert!(
        fixture.snapshot().await["server_keywords"]
            .get("keep-me")
            .is_some()
    );
    let requests = fixture.state.lock().unwrap().set_requests.clone();
    let patch = &requests.last().unwrap()["methodCalls"][0][1]["update"]["E1"];
    assert_eq!(patch.get("keywords/$flagged"), Some(&Value::Null));
    assert!(patch.get("keywords/keep-me").is_none());
}

#[tokio::test]
async fn flag_patch_remove_serializes_null_not_false_or_omission() {
    let fixture = FlagFixture::new("FS", true, ConflictStrategy::ServerWins).await;
    let result = crate::jmap::email::set_email_batch(
        &fixture.verifier,
        &[crate::jmap::email::EmailSetOp::Keywords {
            email_id: "E1".into(),
            keywords: HashMap::from([("$flagged".into(), false)]).into(),
        }],
    )
    .await
    .unwrap();
    assert!(result.failed_updates.is_empty());
    let requests = fixture.state.lock().unwrap().set_requests.clone();
    let patch = &requests.last().unwrap()["methodCalls"][0][1]["update"]["E1"];
    assert_eq!(patch.get("keywords/$flagged"), Some(&Value::Null));
    assert_eq!(fixture.snapshot().await["server_flags"], "S");
}

#[tokio::test]
async fn flag_patch_missing_acknowledgement_is_not_success() {
    let fixture = FlagFixture::new("", false, ConflictStrategy::ServerWins).await;
    fixture.local_change("S", false, false);
    fixture.state.lock().unwrap().omitted_acknowledgements = 1;
    let first = fixture.sync().await.unwrap();
    assert_eq!(first.failed_remote_actions, 1);
    let failed = fixture.snapshot().await;
    assert_eq!(failed["acknowledged_flags"], "");
    assert_eq!(failed["local_observed_flags"], "S");
    assert!(failed["flag_attempt"].is_object());
    assert_eq!(first.pending_flag_actions, 1);
    fixture.sync().await.unwrap();
    assert_converged(&fixture, "S").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_patch_http_retry_and_ambiguous_applied_write_converge() {
    for applied in [false, true] {
        let fixture = FlagFixture::new("FS", true, ConflictStrategy::ServerWins).await;
        fixture.local_change("S", false, false);
        {
            let mut state = fixture.state.lock().unwrap();
            if applied {
                state.lost_replies = 1;
            } else {
                state.http_failures = 1;
            }
        }
        assert_eq!(fixture.sync().await.unwrap().failed_remote_actions, 0);
        assert_converged(&fixture, "S").await;
        assert_eq!(fixture.state.lock().unwrap().set_requests.len(), 2);
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_patch_escapes_keyword_pointer_segments() {
    let fixture = FlagFixture::new("S", false, ConflictStrategy::ServerWins).await;
    let outcome = crate::jmap::email::set_email_batch(
        &fixture.verifier,
        &[crate::jmap::email::EmailSetOp::Keywords {
            email_id: "E1".into(),
            keywords: HashMap::from([("user/a~b".into(), true)]).into(),
        }],
    )
    .await
    .unwrap();
    assert!(outcome.failed_updates.is_empty());
    let requests = fixture.state.lock().unwrap().set_requests.clone();
    let patch = &requests.last().unwrap()["methodCalls"][0][1]["update"]["E1"];
    assert_eq!(patch.get("keywords/user~1a~0b"), Some(&json!(true)));
    assert!(
        fixture.snapshot().await["server_keywords"]
            .get("user/a~b")
            .is_some()
    );
}

#[tokio::test]
async fn flag_recovery_adoption_push_rejection_survives_restart() {
    let mut fixture = FlagFixture::new("FS", false, ConflictStrategy::LocalWins).await;
    fixture.local_change("S", false, false);
    fixture.reset_db();
    fixture.state.lock().unwrap().reject_next_update = 1;
    let failed = fixture.sync().await.unwrap();
    assert_eq!(failed.failed_remote_actions, 1);
    assert_eq!(failed.pending_flag_actions, 1);
    assert!(!failed.already_in_sync);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["server_flags"], "FS");
    assert!(snapshot["acknowledged_flags"].is_null());
    fixture.reopen_db();
    let recovered = fixture.sync().await.unwrap();
    assert_eq!(
        recovered.flag_updates, 1,
        "unfinished adoption must be replanned without a new delta"
    );
    assert_converged(&fixture, "S").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_recovery_adoption_pull_failure_survives_restart() {
    let mut fixture = FlagFixture::new("S", false, ConflictStrategy::ServerWins).await;
    fixture.server_change("FS", false);
    fixture.reset_db();
    let source = fixture.file();
    let held = fixture.temp.path().join("held-message");
    fixture.state.lock().unwrap().hide_on_get = Some((source.clone(), held.clone()));
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 0);
    assert!(held.exists());
    std::fs::rename(held, source).unwrap();
    fixture.reopen_db();
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    assert_converged(&fixture, "FS").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_recovery_direction_filter_does_not_acknowledge_excluded_push() {
    let fixture = FlagFixture::new("FS", false, ConflictStrategy::LocalWins).await;
    fixture.local_change("S", false, false);
    queries::delete_message_by_jmap_id(&fixture.conn, &"E1".into()).unwrap();
    queries::set_jmap_state(&fixture.conn, ACCOUNT_ID, "Email", "").unwrap();
    let result = SyncEngine::pull_only(&fixture.conn, &fixture.config, false)
        .await
        .unwrap();
    assert_eq!(result.flag_updates, 0);
    assert_eq!(fixture.snapshot().await["server_flags"], "FS");
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    assert_converged(&fixture, "S").await;
}

#[tokio::test]
async fn flag_recovery_ambiguous_write_does_not_undo_a_newer_local_edit() {
    let mut fixture = FlagFixture::new("", false, ConflictStrategy::ServerWins).await;
    fixture.local_change("S", false, false);
    let source = fixture.file();
    let name = source
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .split(":2,")
        .next()
        .unwrap();
    let target = source.with_file_name(format!("{name}:2,"));
    fixture.state.lock().unwrap().rename_after_set = Some((source, target));
    fixture.conn.execute_batch("CREATE TRIGGER abort_flag_cache BEFORE UPDATE ON message_map WHEN NEW.flags = 'S' BEGIN SELECT RAISE(FAIL, 'injected post-network DB failure'); END;").unwrap();
    assert!(fixture.sync().await.is_err());
    let failed = fixture.snapshot().await;
    assert_eq!(failed["file_flags"], "");
    assert_eq!(failed["server_flags"], "S");
    assert_eq!(failed["acknowledged_flags"], "");
    assert_eq!(failed["flag_attempt"]["source_flags"], "S");
    assert_eq!(failed["flag_attempt"]["target_flags"], "S");
    fixture
        .conn
        .execute_batch("DROP TRIGGER abort_flag_cache;")
        .unwrap();
    fixture.reopen_db();
    fixture.sync().await.unwrap();
    assert_converged(&fixture, "").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_recovery_excluded_pull_is_not_acknowledged() {
    let mut fixture = FlagFixture::new("S", false, ConflictStrategy::ServerWins).await;
    fixture.local_change("F", false, false);
    queries::delete_message_by_jmap_id(&fixture.conn, &"E1".into()).unwrap();
    queries::set_jmap_state(&fixture.conn, ACCOUNT_ID, "Email", "").unwrap();
    let excluded = SyncEngine::push_only(&fixture.conn, &fixture.config, false)
        .await
        .unwrap();
    assert_eq!(excluded.flag_updates, 0);
    assert_eq!(excluded.pending_flag_actions, 1);
    assert!(fixture.snapshot().await["acknowledged_flags"].is_null());
    fixture.reopen_db();
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    assert_converged(&fixture, "S").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_recovery_dry_run_does_not_prepare_or_acknowledge_a_generation() {
    let fixture = FlagFixture::new("S", false, ConflictStrategy::ServerWins).await;
    fixture.local_change("FS", false, false);
    let before = fixture.snapshot().await;
    SyncEngine::sync(&fixture.conn, &fixture.config, true)
        .await
        .unwrap();
    assert_eq!(
        fixture.snapshot().await,
        before,
        "a preview must not change observations, history, or server state"
    );
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    assert_converged(&fixture, "FS").await;
}

#[tokio::test]
async fn flag_recovery_unowned_suffix_survives_push_and_pull_without_permanent_work() {
    let fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", false, false);
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 0);
    assert!(fixture.sync().await.unwrap().already_in_sync);
    fixture.server_change("FS", true);
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], "FSX");
    assert_eq!(snapshot["local_observed_flags"], "FSX");
    assert_eq!(snapshot["cached_flags"], "FS");
    assert_eq!(snapshot["acknowledged_flags"], "FS");
    fixture.local_change("SX", false, false);
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], "SX");
    assert_eq!(snapshot["server_flags"], "S");
    assert_eq!(snapshot["acknowledged_flags"], "S");
    let idle = fixture.sync().await.unwrap();
    assert_eq!(idle.pending_flag_actions, 0);
    assert!(idle.already_in_sync);
}

#[tokio::test]
async fn flag_recovery_legacy_disagreement_requires_review_not_global_policy_overwrite() {
    for strategy in [ConflictStrategy::LocalWins, ConflictStrategy::ServerWins] {
        let fixture = FlagFixture::new("S", true, strategy).await;
        fixture
            .conn
            .execute("DELETE FROM message_flag_sync", [])
            .unwrap();
        fixture.server_change("FS", true);
        for _ in 0..2 {
            let result = fixture.sync().await.unwrap();
            assert_eq!(result.flag_updates, 0);
            assert_eq!(result.pending_flag_actions, 1);
            assert_eq!(result.blocked_flag_actions, 1);
            assert!(!result.already_in_sync);
            let snapshot = fixture.snapshot().await;
            assert_eq!(snapshot["file_flags"], "S");
            assert_eq!(snapshot["server_flags"], "FS");
            assert_eq!(snapshot["cached_keywords"], snapshot["server_keywords"]);
        }
        assert!(fixture.state.lock().unwrap().set_requests.is_empty());
    }
}

#[tokio::test]
async fn flag_recovery_legacy_confirmed_common_snapshot_preserves_a_local_edit() {
    let fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    fixture
        .conn
        .execute("DELETE FROM message_flag_sync", [])
        .unwrap();
    fixture.local_change("FS", false, false);
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    assert_converged(&fixture, "FS").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_recovery_mixed_rejection_survives_another_ids_acknowledgement_failure() {
    use crate::state::flag_sync;
    let mut fixture = FlagFixture::new("", false, ConflictStrategy::ServerWins).await;
    {
        let mut state = fixture.state.lock().unwrap();
        state.emails.push(RemoteEmail {
            id: "E2".into(),
            blob_id: "B2".into(),
            thread_id: "T2".into(),
            mailbox_ids: vec!["MB-INBOX".into()],
            keywords: vec![],
            message_id: Some("second@example.com".into()),
        });
        state.blobs.insert(
            "B2".into(),
            b"Message-ID: <second@example.com>\r\n\r\nbody\r\n".to_vec(),
        );
        advance_email_state(&mut state);
    }
    assert_eq!(fixture.sync().await.unwrap().downloaded, 1);
    for email_id in ["E1", "E2"] {
        let record = queries::get_message_by_jmap_id(&fixture.conn, &email_id.into())
            .unwrap()
            .unwrap();
        let mid = record.maildir_id.unwrap();
        let folder = fixture.temp.path().join("INBOX");
        std::fs::rename(
            folder.join("new").join(format!("{mid}:2,")),
            folder.join("cur").join(format!("{mid}:2,S")),
        )
        .unwrap();
    }
    {
        let mut state = fixture.state.lock().unwrap();
        state.reject_next_update = 1;
        state.reject_only_id = Some("E2".into());
    }
    fixture
        .conn
        .execute_batch(
            "CREATE TRIGGER abort_one_ack BEFORE UPDATE ON message_map
        WHEN NEW.jmap_email_id='E1' AND NEW.flags='S'
        BEGIN SELECT RAISE(FAIL, 'injected acknowledgement failure'); END;",
        )
        .unwrap();
    assert!(fixture.sync().await.is_err());
    let accepted = flag_sync::get(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    assert!(accepted.attempt.is_some());
    let rejected = flag_sync::get(&fixture.conn, &"E2".into())
        .unwrap()
        .unwrap();
    assert!(
        rejected.attempt.is_none(),
        "a confirmed rejection must survive an unrelated ACK rollback"
    );
    assert!(rejected.blocked);
    assert_eq!(rejected.baseline.as_deref(), Some(""));
    fixture
        .conn
        .execute_batch("DROP TRIGGER abort_one_ack;")
        .unwrap();
    fixture.reopen_db();
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    for email in crate::jmap::email::get_by_ids(&fixture.verifier, &["E1".into(), "E2".into()])
        .await
        .unwrap()
    {
        assert_eq!(keywords_to_flags(&email.keywords), "S");
        let acknowledged = flag_sync::get(&fixture.conn, &email.id).unwrap().unwrap();
        assert_eq!(acknowledged.baseline.as_deref(), Some("S"));
        assert!(acknowledged.attempt.is_none());
    }
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_move_conflicts_use_the_acknowledged_whole_set_policy() {
    for (strategy, expected) in [
        (ConflictStrategy::ServerWins, "FX"),
        (ConflictStrategy::LocalWins, "SX"),
    ] {
        let fixture = FlagFixture::new("", true, strategy).await;
        assert_eq!(fixture.snapshot().await["acknowledged_flags"], "");
        fixture.local_change("SX", true, false);
        fixture.server_change("F", true);
        let result = fixture.sync().await.unwrap();
        assert_eq!(result.moved, 1);
        assert_eq!(result.flag_updates, 1);
        assert_eq!(
            (result.downloaded, result.uploaded, result.deleted),
            (0, 0, 0)
        );
        assert_moved_then_idle(&fixture, expected).await;
    }
}

#[tokio::test]
async fn flag_move_refreshes_full_custom_metadata_even_without_owned_changes() {
    for conflict in [false, true] {
        let fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
        fixture.local_change("SX", true, false);
        {
            let mut state = fixture.state.lock().unwrap();
            state.emails[0].keywords = vec!["$seen".into(), "fresh/a~b".into()];
            if conflict {
                state.emails[0].keywords.push("$flagged".into());
            }
            advance_email_state(&mut state);
        }
        let result = fixture.sync().await.unwrap();
        assert_eq!(result.moved, 1);
        assert_moved_then_idle(&fixture, if conflict { "FSX" } else { "SX" }).await;
        let snapshot = fixture.snapshot().await;
        assert_eq!(snapshot["cached_keywords"]["fresh/a~b"], true);
        assert!(snapshot["cached_keywords"].get("keep-me").is_none());
    }
}

#[tokio::test]
async fn flag_move_membership_rejection_does_not_apply_destination_flags() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", true, false);
    fixture.server_change("F", true);
    fixture.state.lock().unwrap().reject_next_update = 1;
    let rejected = fixture.sync().await.unwrap();
    assert_eq!(rejected.failed_remote_actions, 1);
    assert_eq!(rejected.flag_updates, 0);
    assert!(!rejected.already_in_sync);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], "SX");
    assert_eq!(snapshot["cached_mailbox_id"], "MB-INBOX");
    assert_eq!(snapshot["server_mailboxes"], json!({"MB-INBOX":true}));
    assert_eq!(snapshot["server_flags"], "F");
    assert_eq!(snapshot["acknowledged_flags"], "");
    fixture.reopen_db();
    let recovered = fixture.sync().await.unwrap();
    assert_eq!(recovered.moved, 1);
    assert_eq!(recovered.flag_updates, 1);
    assert_moved_then_idle(&fixture, "FX").await;
}

#[tokio::test]
async fn flag_move_membership_ack_then_rename_failure_recovers_at_destination() {
    use crate::state::flag_sync;
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", true, false);
    fixture.server_change("F", true);
    let source = fixture.file();
    let held = fixture.temp.path().join("held-message");
    fixture.state.lock().unwrap().rename_after_set = Some((source.clone(), held.clone()));
    let failed = fixture.sync().await.unwrap();
    assert_eq!(failed.moved, 1);
    assert_eq!(failed.flag_updates, 0);
    assert_eq!(failed.pending_flag_actions, 1);
    assert!(!failed.already_in_sync);
    assert!(held.exists());
    let record = queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    assert_eq!(record.jmap_mailbox_id.as_ref(), "MB-TRASH");
    assert_eq!(record.flags, "F");
    assert_eq!(
        serde_json::from_str::<Value>(&record.jmap_keywords).unwrap(),
        json!({"$flagged":true,"keep-me":true})
    );
    let observed = queries::get_local_state(&fixture.conn, record.maildir_id.as_ref().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(observed, ("Deleted Items".into(), "SX".into()));
    let history = flag_sync::get(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    assert_eq!(history.baseline.as_deref(), Some(""));
    let attempt = history.attempt.unwrap();
    assert_eq!(attempt.source_folder.as_deref(), Some("Deleted Items"));
    assert_eq!(attempt.source_maildir_id, record.maildir_id);
    assert_eq!(attempt.source_flags, "SX");
    assert_eq!(attempt.target_flags, "F");
    std::fs::rename(held, source).unwrap();
    // No delta is needed: retained flag history must suffice after reopen.
    let state = fixture.state.lock().unwrap().email_state.clone();
    queries::set_jmap_state(&fixture.conn, ACCOUNT_ID, "Email", &state).unwrap();
    fixture.reopen_db();
    let recovered = fixture.sync().await.unwrap();
    assert_eq!(
        recovered.moved, 0,
        "do not rename the missing old source or move backwards"
    );
    assert_eq!(recovered.flag_updates, 1);
    assert_moved_then_idle(&fixture, "FX").await;
}

#[tokio::test]
async fn flag_move_excluded_membership_does_not_adopt_or_apply_paired_pull() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", true, false);
    fixture.server_change("F", true);
    let excluded = SyncEngine::pull_only(&fixture.conn, &fixture.config, false)
        .await
        .unwrap();
    assert_eq!(excluded.flag_updates, 0);
    assert_eq!(excluded.moved, 0);
    assert!(!excluded.already_in_sync);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], "SX");
    assert_eq!(snapshot["cached_mailbox_id"], "MB-INBOX");
    assert_eq!(snapshot["acknowledged_flags"], "");
    assert_eq!(snapshot["set_requests"], 0);
    fixture.reopen_db();
    fixture.sync().await.unwrap();
    assert_moved_then_idle(&fixture, "FX").await;
}

#[tokio::test]
async fn flag_move_excluded_pull_retains_history_after_membership_success() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", true, false);
    fixture.server_change("F", true);
    let excluded = SyncEngine::push_only(&fixture.conn, &fixture.config, false)
        .await
        .unwrap();
    assert_eq!(excluded.flag_updates, 0);
    assert_eq!(excluded.moved, 1);
    assert_eq!(excluded.pending_flag_actions, 1);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], "SX");
    assert_eq!(snapshot["cached_mailbox_id"], "MB-TRASH");
    assert_eq!(snapshot["server_flags"], "F");
    assert_eq!(snapshot["acknowledged_flags"], "");
    fixture.reopen_db();
    let recovered = fixture.sync().await.unwrap();
    assert_eq!(recovered.moved, 0);
    assert_eq!(recovered.flag_updates, 1);
    assert_moved_then_idle(&fixture, "FX").await;
}

#[tokio::test]
async fn flag_move_missing_positive_membership_ack_does_not_complete_placement() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", true, false);
    fixture.server_change("F", true);
    fixture.state.lock().unwrap().omitted_acknowledgements = 1;
    let ambiguous = fixture.sync().await.unwrap();
    assert_eq!(ambiguous.failed_remote_actions, 1);
    assert_eq!(ambiguous.moved, 0);
    assert_eq!(ambiguous.flag_updates, 0);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], "SX");
    assert_eq!(snapshot["cached_mailbox_id"], "MB-INBOX");
    assert_eq!(snapshot["server_mailboxes"], json!({"MB-TRASH":true}));
    assert_eq!(snapshot["acknowledged_flags"], "");
    fixture.reopen_db();
    fixture.sync().await.unwrap();
    assert_moved_then_idle(&fixture, "FX").await;
}

#[tokio::test]
async fn flag_move_push_receipt_captures_destination_and_preserves_a_newer_edit() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", true, false);
    let original = fixture.file();
    let source = original.with_file_name("999999999.moved:2,SX");
    std::fs::rename(original, &source).unwrap();
    let id = source
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .split(":2,")
        .next()
        .unwrap();
    let target = source.with_file_name(format!("{id}:2,X"));
    fixture.state.lock().unwrap().rename_after_set = Some((source, target));
    fixture.conn.execute_batch("CREATE TRIGGER abort_move_cache BEFORE UPDATE ON message_map WHEN NEW.flags = 'S' BEGIN SELECT RAISE(FAIL, 'injected post-network move failure'); END;").unwrap();
    assert!(fixture.sync().await.is_err());
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["server_flags"], "S");
    assert_eq!(snapshot["file_flags"], "X");
    assert_eq!(snapshot["acknowledged_flags"], "");
    assert_eq!(snapshot["flag_attempt"]["source_folder"], "Deleted Items");
    assert_eq!(
        snapshot["flag_attempt"]["source_maildir_id"],
        "999999999.moved"
    );
    assert_eq!(snapshot["flag_attempt"]["source_flags"], "SX");
    fixture
        .conn
        .execute_batch("DROP TRIGGER abort_move_cache;")
        .unwrap();
    fixture.reopen_db();
    fixture.sync().await.unwrap();
    assert_moved_then_idle(&fixture, "X").await;
}

#[tokio::test]
async fn flag_move_matrix_local_and_remote_additions_and_removals() {
    for flag in ['D', 'F', 'P', 'R', 'S', 'T'] {
        for adding in [false, true] {
            let letter = flag.to_string();
            let (initial, desired) = if adding {
                ("", letter.as_str())
            } else {
                (letter.as_str(), "")
            };
            for remote in [false, true] {
                for moved in [false, true] {
                    for keep_new in [false, true] {
                        if flag == 'S' && keep_new {
                            continue;
                        }
                        let fixture =
                            FlagFixture::new(initial, true, ConflictStrategy::ServerWins).await;
                        fixture.local_change(
                            if remote { initial } else { desired },
                            moved,
                            keep_new,
                        );
                        if remote {
                            fixture.server_change(desired, true);
                        }
                        let result = fixture.sync().await.unwrap();
                        assert_eq!(
                            result.flag_updates, 1,
                            "{flag} adding={adding} remote={remote} moved={moved} new={keep_new}"
                        );
                        assert_eq!(result.moved, usize::from(moved));
                        assert_eq!(
                            (result.downloaded, result.uploaded, result.deleted),
                            (0, 0, 0)
                        );
                        if moved {
                            assert_moved_then_idle(&fixture, desired).await;
                        } else {
                            assert_converged(&fixture, desired).await;
                            assert!(fixture.sync().await.unwrap().already_in_sync);
                            assert_converged(&fixture, desired).await;
                        }
                        assert_eq!(
                            fixture.snapshot().await["subdir"],
                            if keep_new { "new" } else { "cur" }
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn flag_move_dry_run_preserves_observations_history_and_server() {
    let fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", true, false);
    fixture.server_change("F", true);
    let before = fixture.snapshot().await;
    SyncEngine::sync(&fixture.conn, &fixture.config, true)
        .await
        .unwrap();
    assert_eq!(fixture.snapshot().await, before);
    fixture.sync().await.unwrap();
    assert_moved_then_idle(&fixture, "FX").await;
}

#[tokio::test]
async fn flag_move_competing_mailbox_destinations_remain_deferred_after_reopen() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", true, false);
    {
        let mut state = fixture.state.lock().unwrap();
        state.mailboxes.push(RemoteMailbox {
            id: "MB-OTHER".into(),
            name: "Other".into(),
            role: None,
            parent_id: None,
        });
        state.mailbox_state = "mb-2".into();
        state.emails[0].mailbox_ids = vec!["MB-OTHER".into()];
        state.emails[0].keywords = vec!["$flagged".into(), "keep-me".into()];
        advance_email_state(&mut state);
    }
    for _ in 0..2 {
        let deferred = fixture.sync().await.unwrap();
        assert_eq!(
            (
                deferred.moved,
                deferred.flag_updates,
                deferred.downloaded,
                deferred.deleted
            ),
            (0, 0, 0, 0)
        );
        assert!(deferred.deferred_actions > 0);
        assert!(!deferred.already_in_sync);
        let snapshot = fixture.snapshot().await;
        assert_eq!(snapshot["file_folder"], "Deleted Items");
        assert_eq!(snapshot["file_flags"], "SX");
        assert_eq!(snapshot["server_mailboxes"], json!({"MB-OTHER":true}));
        assert_eq!(snapshot["cached_mailbox_id"], "MB-INBOX");
        assert_eq!(snapshot["acknowledged_flags"], "");
        assert_eq!(snapshot["set_requests"], 0);
        fixture.reopen_db();
    }
    // Explicitly resolve placement externally, then let the retained local
    // move finish without another MUA edit or a body transfer.
    {
        let mut state = fixture.state.lock().unwrap();
        state.emails[0].mailbox_ids = vec!["MB-INBOX".into()];
        advance_email_state(&mut state);
    }
    fixture.sync().await.unwrap();
    assert_moved_then_idle(&fixture, "FX").await;
}

#[tokio::test]
async fn flag_move_superseded_move_defers_flags_until_after_folder_policy() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.config.sync.allow_destructive_folder_sync = AllowDestructiveFolderSync::DeleteLocal;
    fixture.local_change("SX", true, false);
    fixture.server_change("F", true);
    {
        let mut state = fixture.state.lock().unwrap();
        state.mailboxes.retain(|mailbox| mailbox.id != "MB-TRASH");
        state.mailbox_state = "mb-2".into();
    }
    let counter = fixture.sync().await.unwrap();
    assert_eq!(counter.moved, 1);
    assert_eq!(
        counter.flag_updates, 0,
        "a superseded MoveRemote did not authorize a flag effect"
    );
    assert!(counter.deferred_actions > 0);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_folder"], "INBOX");
    assert_eq!(snapshot["file_flags"], "SX");
    assert_eq!(snapshot["acknowledged_flags"], "");
    assert_eq!(snapshot["server_flags"], "F");
    assert_eq!(snapshot["set_requests"], 0);
    fixture.reopen_db();
    let recovered = fixture.sync().await.unwrap();
    assert_eq!(recovered.flag_updates, 1);
    assert_eq!(recovered.moved, 0);
    for _ in 0..2 {
        let snapshot = fixture.snapshot().await;
        assert_eq!(snapshot["file_folder"], "INBOX");
        assert_eq!(snapshot["file_flags"], "FX");
        assert_eq!(snapshot["local_observed_flags"], "FX");
        assert_eq!(snapshot["server_flags"], "F");
        assert_eq!(snapshot["acknowledged_flags"], "F");
        assert_eq!(snapshot["cached_keywords"], snapshot["server_keywords"]);
        assert_eq!(snapshot["blob_requests"], 1);
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_move_same_folder_rebind_preserves_a_local_only_change() {
    let fixture = FlagFixture::new("FS", true, ConflictStrategy::ServerWins).await;
    let original = fixture.file();
    let renamed = original.with_file_name("999999999.rebound:2,S");
    std::fs::rename(original, &renamed).unwrap();
    let result = fixture.sync().await.unwrap();
    assert_eq!(
        (result.downloaded, result.uploaded, result.deleted),
        (0, 0, 0)
    );
    assert_eq!(result.flag_updates, 1);
    assert_eq!(fixture.file(), renamed);
    assert_converged(&fixture, "S").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
    assert_converged(&fixture, "S").await;
}

#[tokio::test]
async fn flag_move_matrix_no_move_conflicts_and_custom_only_refresh() {
    for (strategy, expected) in [
        (ConflictStrategy::ServerWins, "F"),
        (ConflictStrategy::LocalWins, "S"),
    ] {
        let fixture = FlagFixture::new("", true, strategy).await;
        fixture.local_change("S", false, false);
        fixture.server_change("F", true);
        fixture.sync().await.unwrap();
        assert_converged(&fixture, expected).await;
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
    let fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    for custom in [Some("fresh-custom"), None] {
        {
            let mut state = fixture.state.lock().unwrap();
            state.emails[0].keywords = vec!["$seen".into()];
            if let Some(keyword) = custom {
                state.emails[0].keywords.push(keyword.into());
            }
            advance_email_state(&mut state);
        }
        assert_eq!(fixture.sync().await.unwrap().flag_updates, 0);
        assert_converged(&fixture, "S").await;
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_matrix_cold_adoption_transport_failures_survive_reopen() {
    for fault in ["rejected", "http-exhausted", "lost-replies"] {
        let mut fixture = FlagFixture::new("FS", true, ConflictStrategy::LocalWins).await;
        fixture.local_change("S", false, false);
        fixture.reset_db();
        {
            let mut state = fixture.state.lock().unwrap();
            // Library tests leave the process-wide retry configuration unset.
            let attempts = crate::jmap::retry::RetryConfig::default().max_attempts as usize;
            match fault {
                "rejected" => state.reject_next_update = 1,
                "http-exhausted" => state.http_failures = attempts,
                "lost-replies" => state.lost_replies = attempts,
                _ => unreachable!(),
            }
        }
        let result = fixture.sync().await;
        if fault == "rejected" {
            assert_eq!(result.unwrap().failed_remote_actions, 1);
        } else {
            assert!(result.is_err(), "{fault} must exhaust transport retries");
        }
        assert!(fixture.snapshot().await["acknowledged_flags"].is_null());
        fixture.reopen_db();
        fixture.sync().await.unwrap();
        assert_converged(&fixture, "S").await;
        assert!(fixture.sync().await.unwrap().already_in_sync);
        assert_eq!(fixture.snapshot().await["blob_requests"], 1);
    }
}

#[tokio::test]
async fn flag_matrix_ordinary_pull_rename_failure_survives_reopen() {
    let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    fixture.server_change("FS", true);
    let source = fixture.file();
    let held = fixture.temp.path().join("held-message");
    fixture.state.lock().unwrap().hide_on_get = Some((source.clone(), held.clone()));
    let failed = fixture.sync().await.unwrap();
    assert_eq!(failed.flag_updates, 0);
    assert_eq!(failed.pending_flag_actions, 1);
    std::fs::rename(held, source).unwrap();
    assert_eq!(fixture.snapshot().await["acknowledged_flags"], "S");
    fixture.reopen_db();
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    assert_converged(&fixture, "FS").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

fn seed_excluded_flag_history(fixture: &FlagFixture, legacy: bool) {
    queries::upsert_message(
        &fixture.conn,
        &queries::MessageRecord {
            jmap_email_id: "E2".into(),
            jmap_blob_id: Some("B2".into()),
            jmap_thread_id: Some("T2".into()),
            jmap_mailbox_id: "MB-TRASH".into(),
            maildir_id: Some("excluded".into()),
            message_id: "excluded@example.com".into(),
            flags: "FS".into(),
            jmap_keywords: serde_json::to_string(&flags_to_keywords("FS")).unwrap(),
        },
    )
    .unwrap();
    queries::upsert_local_state(
        &fixture.conn,
        &"excluded".into(),
        "Deleted Items",
        "S",
        None,
    )
    .unwrap();
    if !legacy {
        crate::state::flag_sync::acknowledge(&fixture.conn, &"E2".into(), "FS").unwrap();
    }
}

#[tokio::test]
async fn flag_retry_excluded_histories_do_not_widen_paths_or_pollute_status() {
    use crate::sync::engine::ScanScope;
    use crate::sync::plan::SyncDirection;
    for legacy in [false, true] {
        let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
        seed_excluded_flag_history(&fixture, legacy);
        fixture.config.sync.mailboxes = vec!["INBOX".into()];
        let before = fixture.state.lock().unwrap().email_changes_requests;
        let result = SyncEngine::connect(&fixture.conn, &fixture.config)
            .await
            .unwrap()
            .run(
                false,
                SyncDirection::Both,
                ScanScope::Paths(vec![fixture.file()]),
            )
            .await
            .unwrap();
        assert_eq!(
            fixture.state.lock().unwrap().email_changes_requests,
            before,
            "excluded pending/legacy bindings must not force a full scan"
        );
        assert!(result.already_in_sync, "{result:?}");
        assert_eq!(
            (result.pending_flag_actions, result.blocked_flag_actions),
            (0, 0)
        );
        let full = fixture.sync().await.unwrap();
        assert!(full.already_in_sync, "{full:?}");
        assert_eq!(
            (full.pending_flag_actions, full.blocked_flag_actions),
            (0, 0)
        );
        assert_converged(&fixture, "S").await;
    }
}

#[tokio::test]
async fn flag_retry_excluded_invalid_receipt_does_not_break_selected_sync() {
    let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    seed_excluded_flag_history(&fixture, false);
    fixture
        .conn
        .execute(
            "UPDATE message_flag_sync SET attempt='invalid' WHERE jmap_email_id='E2'",
            [],
        )
        .unwrap();
    fixture.config.sync.mailboxes = vec!["INBOX".into()];
    assert!(fixture.sync().await.unwrap().already_in_sync);
    assert!(
        crate::state::flag_sync::get(&fixture.conn, &"E2".into()).is_err(),
        "excluded corruption is retained, not erased or treated as acknowledgement"
    );
}

#[tokio::test]
async fn flag_retry_blocked_paths_noop_still_reports_unfinished_history() {
    use crate::sync::engine::ScanScope;
    use crate::sync::plan::SyncDirection;
    let fixture = FlagFixture::new("FS", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("S", false, false);
    fixture.state.lock().unwrap().reject_next_update = 1;
    assert_eq!(fixture.sync().await.unwrap().failed_remote_actions, 1);
    let before = fixture.state.lock().unwrap().email_changes_requests;
    let result = SyncEngine::connect(&fixture.conn, &fixture.config)
        .await
        .unwrap()
        .run(
            false,
            SyncDirection::Both,
            ScanScope::Paths(vec![fixture.file()]),
        )
        .await
        .unwrap();
    assert!(
        !result.already_in_sync,
        "a blocked generation is not an idle synchronized state"
    );
    assert_eq!(
        (result.pending_flag_actions, result.blocked_flag_actions),
        (1, 1)
    );
    assert_eq!(
        fixture.state.lock().unwrap().email_changes_requests,
        before,
        "report blocked work without fetching or hammering its rejected mutation"
    );
    assert_eq!(fixture.state.lock().unwrap().set_requests.len(), 1);
}

#[tokio::test]
async fn flag_retry_preview_reports_history_without_mutating_it() {
    let fixture = FlagFixture::new("FS", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("S", false, false);
    fixture.state.lock().unwrap().reject_next_update = 1;
    fixture.sync().await.unwrap();
    let before = fixture.snapshot().await;
    let preview = SyncEngine::sync(&fixture.conn, &fixture.config, true)
        .await
        .unwrap();
    assert_eq!(
        (preview.pending_flag_actions, preview.blocked_flag_actions),
        (1, 1)
    );
    assert!(!preview.already_in_sync);
    assert_eq!(fixture.snapshot().await, before);
}

async fn next_watch_cycle(
    cycles: &mut tokio::sync::mpsc::UnboundedReceiver<
        Result<crate::sync::engine::SyncOutcome, String>,
    >,
) -> crate::sync::engine::SyncOutcome {
    tokio::time::timeout(std::time::Duration::from_secs(15), cycles.recv())
        .await
        .expect("daemon must complete a cycle without another external event")
        .expect("cycle observer must remain open")
        .expect("cycle must reach a protocol outcome")
}

async fn daemon_eventless_flag_retry(startup: bool) {
    use crate::daemon::runner::{SyncTrigger, run_controlled};
    let mut fixture = FlagFixture::new("FS", true, ConflictStrategy::ServerWins).await;
    fixture.config.watch.coalesce_window_ms = if startup { 30_000 } else { 0 };
    if startup {
        fixture.local_change("S", false, false);
        let mut state = fixture.state.lock().unwrap();
        state.reject_next_update = 1;
        state.reject_update_type = Some("rateLimit".into());
    }
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (cycles_tx, mut cycles_rx) = tokio::sync::mpsc::unbounded_channel();
    let stop = Arc::new(tokio::sync::Notify::new());
    let daemon = run_controlled(
        &fixture.conn,
        &fixture.config,
        (tx.clone(), rx),
        cycles_tx,
        Arc::clone(&stop),
    );
    let driver = async {
        if !startup {
            assert!(next_watch_cycle(&mut cycles_rx).await.already_in_sync);
            fixture.local_change("S", false, false);
            {
                let mut state = fixture.state.lock().unwrap();
                state.reject_next_update = 1;
                state.reject_update_type = Some("rateLimit".into());
            }
            tx.send(SyncTrigger::LocalChange(vec![fixture.file()]))
                .await
                .unwrap();
        }
        let rejected = next_watch_cycle(&mut cycles_rx).await;
        assert_eq!(rejected.failed_remote_actions, 1);
        assert_eq!(
            (rejected.pending_flag_actions, rejected.blocked_flag_actions),
            (1, 0)
        );
        // Controlled producers send nothing after the failed cycle. Only the
        // runner's own deadline can produce this next real SyncEngine cycle.
        let repaired = next_watch_cycle(&mut cycles_rx).await;
        assert_eq!(repaired.flag_updates, 1);
        assert_eq!(repaired.pending_flag_actions, 0);
        assert_converged(&fixture, "S").await;
        assert_eq!(fixture.state.lock().unwrap().set_requests.len(), 2);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(2200), cycles_rx.recv())
                .await
                .is_err(),
            "successful repair must disarm its retry deadline"
        );
        stop.notify_one();
    };
    let (finished, ()) = tokio::join!(daemon, driver);
    finished.unwrap();
    assert!(fixture.sync().await.unwrap().already_in_sync);
    assert_converged(&fixture, "S").await;
}

#[tokio::test]
async fn flag_retry_daemon_startup_rejection_retries_without_an_event() {
    daemon_eventless_flag_retry(true).await;
}

#[tokio::test]
async fn flag_retry_daemon_normal_cycle_rejection_retries_without_an_event() {
    daemon_eventless_flag_retry(false).await;
}

#[tokio::test]
async fn flag_retry_cached_status_respects_selection_and_reports_unknown_paths() {
    use crate::sync::status::cached_flag_status;
    let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    seed_excluded_flag_history(&fixture, false);
    crate::state::flag_sync::failure(&fixture.conn, &"E2".into(), true, true, "forbidden").unwrap();
    fixture.config.sync.mailboxes = vec!["INBOX".into()];
    assert!(
        cached_flag_status(&fixture.conn, &fixture.config.sync)
            .unwrap()
            .flags
            .pending
            .is_empty()
    );
    fixture.config.sync.mailboxes.clear();
    let all = cached_flag_status(&fixture.conn, &fixture.config.sync).unwrap();
    assert_eq!((all.flags.pending.len(), all.flags.blocked.len()), (1, 1));
    assert!(all.to_string().contains("1 pending, 1 blocked/review"));
    fixture.config.sync.mailboxes = vec!["Deleted Items".into()];
    fixture
        .conn
        .execute(
            "UPDATE mailbox_map SET remote_path=NULL WHERE jmap_mailbox_id='MB-TRASH'",
            [],
        )
        .unwrap();
    fixture
        .conn
        .pragma_update(None, "query_only", true)
        .unwrap();
    let unknown = cached_flag_status(&fixture.conn, &fixture.config.sync).unwrap();
    assert_eq!(unknown.unclassified_mailboxes, 1);
    assert!(unknown.to_string().contains("could not be classified"));
    assert!(
        crate::state::flag_sync::get(&fixture.conn, &"E2".into())
            .unwrap()
            .unwrap()
            .blocked
    );
}

#[tokio::test]
async fn flag_retry_daemon_does_not_schedule_permission_or_review_only_work() {
    use crate::daemon::runner::{SyncTrigger, run_controlled};
    for review in [false, true] {
        let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
        fixture.config.watch.coalesce_window_ms = 0;
        fixture.local_change("S", false, false);
        if review {
            fixture.server_change("F", true);
            let mut history = crate::state::flag_sync::get(&fixture.conn, &"E1".into())
                .unwrap()
                .unwrap();
            history.review = true;
            crate::state::flag_sync::store(&fixture.conn, &"E1".into(), &history).unwrap();
        } else {
            fixture.state.lock().unwrap().reject_next_update = 1;
        }
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let (cycles_tx, mut cycles_rx) = tokio::sync::mpsc::unbounded_channel();
        let stop = Arc::new(tokio::sync::Notify::new());
        let daemon = run_controlled(
            &fixture.conn,
            &fixture.config,
            (tx.clone(), rx),
            cycles_tx,
            Arc::clone(&stop),
        );
        let driver = async {
            let blocked = next_watch_cycle(&mut cycles_rx).await;
            assert_eq!(
                (blocked.pending_flag_actions, blocked.blocked_flag_actions),
                (1, 1)
            );
            assert!(!blocked.needs_retry());
            assert!(!blocked.already_in_sync);
            assert_eq!(blocked.flag_updates, 0);
            let snapshot = fixture.snapshot().await;
            assert_eq!(snapshot["file_flags"], "S");
            assert_eq!(snapshot["acknowledged_flags"], "");
            assert_eq!(snapshot["server_flags"], if review { "F" } else { "" });
            let requests = fixture.state.lock().unwrap().set_requests.len();
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(2200), cycles_rx.recv())
                    .await
                    .is_err()
            );
            assert_eq!(fixture.state.lock().unwrap().set_requests.len(), requests);
            if !review {
                // Explicit reassessment is allowed after permission repair,
                // without inventing a new flag edit or resetting the DB.
                tx.send(SyncTrigger::Manual).await.unwrap();
                assert_eq!(next_watch_cycle(&mut cycles_rx).await.flag_updates, 1);
                assert_converged(&fixture, "S").await;
            }
            stop.notify_one();
        };
        let (finished, ()) = tokio::join!(daemon, driver);
        finished.unwrap();
    }
}

#[tokio::test]
async fn flag_retry_daemon_transport_outages_recover_without_an_event() {
    use crate::daemon::runner::run_controlled;
    for after_rejection in [false, true] {
        let mut fixture = FlagFixture::new("FS", true, ConflictStrategy::ServerWins).await;
        fixture.config.watch.coalesce_window_ms = 0;
        fixture.local_change("S", false, false);
        let attempts = crate::jmap::retry::RetryConfig::default().max_attempts as usize;
        {
            let mut state = fixture.state.lock().unwrap();
            if after_rejection {
                state.reject_next_update = 1;
                state.reject_update_type = Some("rateLimit".into());
            } else {
                state.email_changes_failures = attempts;
            }
        }
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let (cycles_tx, mut cycles_rx) = tokio::sync::mpsc::unbounded_channel();
        let stop = Arc::new(tokio::sync::Notify::new());
        let daemon = run_controlled(
            &fixture.conn,
            &fixture.config,
            (tx, rx),
            cycles_tx,
            Arc::clone(&stop),
        );
        let driver = async {
            if after_rejection {
                assert_eq!(
                    next_watch_cycle(&mut cycles_rx).await.failed_remote_actions,
                    1
                );
                fixture.state.lock().unwrap().email_changes_failures = attempts;
            }
            let outage = tokio::time::timeout(std::time::Duration::from_secs(20), cycles_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                outage.is_err(),
                "the injected outage must exhaust RPC retries"
            );
            assert_eq!(fixture.state.lock().unwrap().email_changes_failures, 0);
            let repaired = next_watch_cycle(&mut cycles_rx).await;
            assert_eq!(repaired.flag_updates, 1);
            assert_eq!(repaired.pending_flag_actions, 0);
            assert_converged(&fixture, "S").await;
            stop.notify_one();
        };
        let (finished, ()) = tokio::join!(daemon, driver);
        finished.unwrap();
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_retry_daemon_deadline_caps_unrelated_path_coalescing() {
    use crate::daemon::runner::{SyncTrigger, run_controlled};
    let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    fixture.config.watch.coalesce_window_ms = 30_000;
    fixture.local_change("S", true, false);
    {
        let mut state = fixture.state.lock().unwrap();
        state.reject_next_update = 1;
        state.reject_update_type = Some("rateLimit".into());
    }
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (cycles_tx, mut cycles_rx) = tokio::sync::mpsc::unbounded_channel();
    let stop = Arc::new(tokio::sync::Notify::new());
    let daemon = run_controlled(
        &fixture.conn,
        &fixture.config,
        (tx.clone(), rx),
        cycles_tx,
        Arc::clone(&stop),
    );
    let driver = async {
        let rejected = next_watch_cycle(&mut cycles_rx).await;
        assert_eq!(rejected.retryable_remote_actions, 1);
        assert_eq!(
            rejected.pending_flag_actions, 0,
            "this retry cannot rely on flag-DB pending rows"
        );
        tx.send(SyncTrigger::LocalChange(vec![
            fixture.temp.path().join("INBOX/cur/unrelated:2,"),
        ]))
        .await
        .unwrap();
        let repaired = next_watch_cycle(&mut cycles_rx).await;
        assert_eq!(
            repaired.moved, 1,
            "a path no-op cannot consume a due full-placement retry"
        );
        assert!(!repaired.needs_retry());
        assert_eq!(fixture.state.lock().unwrap().set_requests.len(), 2);
        stop.notify_one();
    };
    let (finished, ()) = tokio::join!(daemon, driver);
    finished.unwrap();
    assert_moved_then_idle(&fixture, "S").await;
}

#[tokio::test]
async fn flag_retry_mixed_keyword_retry_does_not_hammer_a_rejected_membership() {
    use crate::daemon::runner::{SyncTrigger, run_controlled};
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.config.watch.coalesce_window_ms = 0;
    {
        let mut state = fixture.state.lock().unwrap();
        state.emails.push(RemoteEmail {
            id: "E2".into(),
            blob_id: "B2".into(),
            thread_id: "T2".into(),
            mailbox_ids: vec!["MB-INBOX".into()],
            keywords: vec!["keep-two".into()],
            message_id: Some("second@example.com".into()),
        });
        state.blobs.insert(
            "B2".into(),
            b"Message-ID: <second@example.com>\r\n\r\nbody\r\n".to_vec(),
        );
        advance_email_state(&mut state);
    }
    assert_eq!(fixture.sync().await.unwrap().downloaded, 1);
    for email_id in ["E1", "E2"] {
        let record = queries::get_message_by_jmap_id(&fixture.conn, &email_id.into())
            .unwrap()
            .unwrap();
        let id = record.maildir_id.unwrap();
        let source = fixture
            .temp
            .path()
            .join("INBOX/new")
            .join(format!("{id}:2,"));
        let target = fixture
            .temp
            .path()
            .join(if email_id == "E1" {
                "INBOX/cur"
            } else {
                "Deleted Items/cur"
            })
            .join(format!(
                "{id}:2,{}",
                if email_id == "E1" { "S" } else { "SX" }
            ));
        std::fs::rename(source, target).unwrap();
    }
    {
        let mut state = fixture.state.lock().unwrap();
        state
            .reject_updates
            .insert("E1".into(), (2, "rateLimit".into()));
        state
            .reject_updates
            .insert("E2".into(), (100, "forbidden".into()));
        state.emails[1].keywords.push("$flagged".into());
        advance_email_state(&mut state);
    }
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (cycles_tx, mut cycles_rx) = tokio::sync::mpsc::unbounded_channel();
    let stop = Arc::new(tokio::sync::Notify::new());
    let daemon = run_controlled(
        &fixture.conn,
        &fixture.config,
        (tx.clone(), rx),
        cycles_tx,
        Arc::clone(&stop),
    );
    let driver = async {
        let first = next_watch_cycle(&mut cycles_rx).await;
        assert_eq!(first.failed_remote_actions, 2);
        assert_eq!(first.retryable_remote_actions, 1);
        assert_eq!(
            first.retryable_local_actions, 0,
            "a gated pull was never attempted"
        );
        assert!(first.blocked_remote_ids.contains("E2"));
        let second = next_watch_cycle(&mut cycles_rx).await;
        assert_eq!(second.failed_remote_actions, 1);
        assert_eq!(second.retryable_remote_actions, 1);
        assert!(second.blocked_remote_ids.contains("E2"));
        let repaired = next_watch_cycle(&mut cycles_rx).await;
        assert_eq!(repaired.flag_updates, 1);
        assert!(
            !repaired.needs_retry(),
            "only the blocked membership remains"
        );
        assert!(repaired.blocked_remote_ids.contains("E2"));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(4200), cycles_rx.recv())
                .await
                .is_err()
        );
        {
            let state = fixture.state.lock().unwrap();
            for (id, expected) in [("E1", 3), ("E2", 1)] {
                let requests = state
                    .set_requests
                    .iter()
                    .filter(|request| {
                        request["methodCalls"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|call| {
                                call[0] == "Email/set" && call[1]["update"].get(id).is_some()
                            })
                    })
                    .count();
                assert_eq!(requests, expected, "{id} must have no hidden extra retries");
            }
            assert_eq!(state.blob_requests, 2);
        }
        let remote = crate::jmap::email::get_by_ids(&fixture.verifier, &["E1".into(), "E2".into()])
            .await
            .unwrap();
        assert_eq!(
            keywords_to_flags(
                &remote
                    .iter()
                    .find(|email| email.id.as_ref() == "E1")
                    .unwrap()
                    .keywords
            ),
            "S"
        );
        assert!(
            remote
                .iter()
                .find(|email| email.id.as_ref() == "E2")
                .unwrap()
                .mailbox_ids
                .contains_key("MB-INBOX")
        );
        let record = queries::get_message_by_jmap_id(&fixture.conn, &"E2".into())
            .unwrap()
            .unwrap();
        assert_eq!(record.jmap_mailbox_id.as_ref(), "MB-INBOX");
        let e2_id = record.maildir_id.unwrap();
        let destination = fixture.temp.path().join("Deleted Items/cur");
        assert!(
            destination.join(format!("{e2_id}:2,SX")).exists(),
            "blocked membership did not authorize its paired pull"
        );
        assert_eq!(
            crate::state::flag_sync::get(&fixture.conn, &"E2".into())
                .unwrap()
                .unwrap()
                .baseline
                .as_deref(),
            Some("")
        );
        fixture.state.lock().unwrap().reject_updates.remove("E2");
        tx.send(SyncTrigger::Manual).await.unwrap();
        let manual = next_watch_cycle(&mut cycles_rx).await;
        assert_eq!(manual.moved, 1);
        assert_eq!(manual.flag_updates, 1);
        assert!(manual.blocked_remote_ids.is_empty());
        assert!(destination.join(format!("{e2_id}:2,FX")).exists());
        let history = crate::state::flag_sync::get(&fixture.conn, &"E2".into())
            .unwrap()
            .unwrap();
        assert_eq!(history.baseline.as_deref(), Some("F"));
        assert!(history.attempt.is_none());
        let remote = crate::jmap::email::get_by_ids(&fixture.verifier, &["E2".into()])
            .await
            .unwrap()
            .remove(0);
        assert!(remote.mailbox_ids.contains_key("MB-TRASH"));
        assert_eq!(remote.keywords.get("keep-two"), Some(&true));
        stop.notify_one();
    };
    let (finished, ()) = tokio::join!(daemon, driver);
    finished.unwrap();
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_retry_daemon_busy_state_db_retries_without_an_event() {
    use crate::daemon::runner::run_controlled;
    let mut fixture = FlagFixture::new("FS", true, ConflictStrategy::ServerWins).await;
    fixture.config.watch.coalesce_window_ms = 0;
    fixture.local_change("S", false, false);
    let writer = db::open_or_recreate(std::path::Path::new(fixture.conn.path().unwrap())).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE;").unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (cycles_tx, mut cycles_rx) = tokio::sync::mpsc::unbounded_channel();
    let stop = Arc::new(tokio::sync::Notify::new());
    let daemon = run_controlled(
        &fixture.conn,
        &fixture.config,
        (tx, rx),
        cycles_tx,
        Arc::clone(&stop),
    );
    let driver = async {
        let busy = tokio::time::timeout(std::time::Duration::from_secs(20), cycles_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            busy.is_err(),
            "the competing writer must prevent the first cycle from committing"
        );
        writer.execute_batch("ROLLBACK;").unwrap();
        let repaired = next_watch_cycle(&mut cycles_rx).await;
        assert_eq!(repaired.flag_updates, 1);
        assert_converged(&fixture, "S").await;
        stop.notify_one();
    };
    let (finished, ()) = tokio::join!(daemon, driver);
    finished.unwrap();
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

async fn assert_moved(fixture: &FlagFixture, raw_flags: &str) {
    let snapshot = fixture.snapshot().await;
    let mapped = crate::state::flag_sync::mapped_flags(raw_flags);
    assert_eq!(snapshot["file_flags"], raw_flags, "{snapshot}");
    assert_eq!(snapshot["local_observed_flags"], raw_flags, "{snapshot}");
    for key in ["server_flags", "cached_flags", "acknowledged_flags"] {
        assert_eq!(snapshot[key], mapped, "{key}: {snapshot}");
    }
    assert_eq!(snapshot["file_folder"], "Deleted Items", "{snapshot}");
    assert_eq!(snapshot["observed_folder"], "Deleted Items", "{snapshot}");
    assert_eq!(snapshot["cached_mailbox_id"], "MB-TRASH", "{snapshot}");
    let file = fixture.file();
    let name = file.file_name().unwrap().to_str().unwrap();
    assert_eq!(
        snapshot["cached_maildir_id"],
        name.split(":2,").next().unwrap()
    );
    assert_eq!(
        snapshot["server_mailboxes"],
        json!({"MB-TRASH":true}),
        "{snapshot}"
    );
    assert_eq!(
        snapshot["cached_keywords"], snapshot["server_keywords"],
        "{snapshot}"
    );
    assert_eq!(snapshot["server_email_id"], "E1");
    assert_eq!(snapshot["server_blob_id"], "B1");
    assert_eq!(snapshot["cached_blob_id"], "B1");
    assert_eq!(snapshot["server_thread_id"], "T1");
    assert_eq!(snapshot["cached_thread_id"], "T1");
    assert_eq!(
        snapshot["blob_requests"], 1,
        "a move must not download the body again"
    );
    assert!(snapshot["flag_attempt"].is_null(), "{snapshot}");
}

async fn assert_moved_then_idle(fixture: &FlagFixture, raw_flags: &str) {
    assert_moved(fixture, raw_flags).await;
    let requests = fixture.state.lock().unwrap().set_requests.len();
    let idle = fixture.sync().await.unwrap();
    assert!(idle.already_in_sync, "{idle:?}");
    assert_eq!(idle.pending_flag_actions, 0);
    assert_eq!(
        (
            idle.downloaded,
            idle.uploaded,
            idle.deleted,
            idle.moved,
            idle.flag_updates
        ),
        (0, 0, 0, 0, 0)
    );
    assert_eq!(fixture.state.lock().unwrap().set_requests.len(), requests);
    assert_moved(fixture, raw_flags).await;
}

async fn assert_converged(fixture: &FlagFixture, flags: &str) {
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], flags, "{snapshot}");
    assert_eq!(snapshot["server_flags"], flags, "{snapshot}");
    assert_eq!(snapshot["cached_flags"], flags, "{snapshot}");
    assert_eq!(snapshot["local_observed_flags"], flags, "{snapshot}");
    assert_eq!(snapshot["acknowledged_flags"], flags, "{snapshot}");
    assert!(snapshot["flag_attempt"].is_null(), "{snapshot}");
    assert_eq!(
        snapshot["cached_keywords"], snapshot["server_keywords"],
        "{snapshot}"
    );
}

#[test]
fn flag_sync_server_enforces_keyword_patch_semantics() {
    let email = RemoteEmail {
        id: "E1".into(),
        blob_id: "B1".into(),
        thread_id: "T1".into(),
        mailbox_ids: vec!["MB-INBOX".into()],
        keywords: vec!["$seen".into(), "$flagged".into(), "keep-me".into()],
        message_id: Some("audit@example.com".into()),
    };
    let mut state = ServerState {
        email_state: "e-1".into(),
        emails: vec![email.clone()],
        ..Default::default()
    };
    let response = email_set(
        &json!({"update":{"E1":{"keywords/$flagged":null}}}),
        "c1",
        &mut state,
    );
    assert!(response[1]["updated"].get("E1").is_some());
    assert!(!state.emails[0].keywords.contains(&"$flagged".into()));
    assert!(state.emails[0].keywords.contains(&"keep-me".into()));
    state.emails = vec![email.clone()];
    let before = state.email_state.clone();
    let response = email_set(
        &json!({"update":{"E1":{"keywords/$flagged":false,"mailboxIds":{"MB-TRASH":true}}}}),
        "c2",
        &mut state,
    );
    assert!(response[1]["notUpdated"].get("E1").is_some());
    assert_eq!(state.emails[0].keywords, email.keywords);
    assert_eq!(state.emails[0].mailbox_ids, email.mailbox_ids);
    assert_eq!(state.email_state, before);
    email_set(
        &json!({"update":{"E1":{"keywords/$seen":true}}}),
        "c3",
        &mut state,
    );
    assert_eq!(state.emails[0].keywords, email.keywords);
}

#[tokio::test]
async fn flag_sync_local_addition_converges_then_stays_idle() {
    let fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("S", false, false);
    let first = fixture.sync().await.unwrap();
    assert_eq!(first.flag_updates, 1);
    assert_converged(&fixture, "S").await;
    let requests = fixture.state.lock().unwrap().set_requests.len();
    let second = fixture.sync().await.unwrap();
    assert!(second.already_in_sync);
    assert_eq!(second.flag_updates, 0);
    assert_eq!(fixture.state.lock().unwrap().set_requests.len(), requests);
    assert_converged(&fixture, "S").await;
}

#[tokio::test]
async fn flag_sync_remote_addition_and_removal_preserve_custom_keywords() {
    let fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    for flags in ["FS", "S"] {
        fixture.server_change(flags, true);
        assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
        assert_converged(&fixture, flags).await;
        assert!(
            fixture.snapshot().await["server_keywords"]
                .get("keep-me")
                .is_some()
        );
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_sync_rejected_ordinary_push_retains_retryable_baseline() {
    let fixture = FlagFixture::new("", false, ConflictStrategy::ServerWins).await;
    fixture.local_change("S", false, false);
    fixture.state.lock().unwrap().reject_next_update = 1;
    assert_eq!(fixture.sync().await.unwrap().failed_remote_actions, 1);
    let failed = fixture.snapshot().await;
    assert_eq!(failed["file_flags"], "S");
    assert_eq!(failed["server_flags"], "");
    assert_eq!(failed["acknowledged_flags"], "");
    assert_eq!(failed["local_observed_flags"], "S");
    assert!(
        failed["flag_attempt"].is_null(),
        "confirmed rejection is not an ambiguous write"
    );
    assert_eq!(failed["flags_blocked"], true);
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    assert_converged(&fixture, "S").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_sync_rebuild_matching_views_reuses_existing_file() {
    let mut fixture = FlagFixture::new("FS", true, ConflictStrategy::ServerWins).await;
    let original = fixture.file();
    fixture.reset_db();
    let result = fixture.sync().await.unwrap();
    assert_eq!(result.downloaded, 0);
    assert_eq!(result.uploaded, 0);
    assert_eq!(fixture.file(), original);
    assert_converged(&fixture, "FS").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

fn advance_email_state(state: &mut ServerState) {
    let n: usize = state
        .email_state
        .strip_prefix("e-")
        .unwrap()
        .parse()
        .unwrap();
    state.email_state = format!("e-{}", n + 1);
}

// Independent RFC-style state transition: validate the entire object, then
// commit it atomically. Omission is not deletion; false is not a keyword value.
fn email_set(args: &Value, call_id: &str, state: &mut ServerState) -> Value {
    if state.edit_before_set_remaining > 0
        && state.set_requests.len() >= state.edit_before_set_at
        && let Some(edit) = state.edit_before_set.clone()
    {
        state.edit_before_set_remaining -= 1;
        let id = args["update"]
            .as_object()
            .and_then(|updates| updates.keys().next())
            .map(String::as_str)
            .or_else(|| {
                args["destroy"]
                    .as_array()
                    .and_then(|ids| ids.first())
                    .and_then(Value::as_str)
            });
        let id = if state.edit_other_before_set {
            state
                .emails
                .iter()
                .find(|email| Some(email.id.as_str()) != id)
                .map(|email| email.id.clone())
        } else {
            id.map(str::to_owned)
        };
        apply_remote_edit(state, edit, id.as_deref());
    }
    for (source, target) in std::mem::take(&mut state.rename_many_before_set) {
        std::fs::rename(source, target).unwrap();
    }
    if let Some((source, target)) = state.rename_before_set.take() {
        std::fs::rename(source, target).unwrap();
    }
    if state.set_method_failures > 0 {
        state.set_method_failures -= 1;
        return json!(["error", {"type":"serverFail"}, call_id]);
    }
    if let Some(expected) = args["ifInState"].as_str()
        && expected != state.email_state
    {
        state.state_mismatches += 1;
        return json!(["error", {"type":"stateMismatch"}, call_id]);
    }
    let old = state.email_state.clone();
    let mut updated = serde_json::Map::new();
    let mut not_updated = serde_json::Map::new();
    if let Some(updates) = args["update"].as_object() {
        for (id, patch) in updates {
            if let Some((remaining, error)) = state.reject_updates.get_mut(id)
                && *remaining > 0
            {
                *remaining -= 1;
                not_updated.insert(id.clone(), json!({"type":error}));
                continue;
            }
            if state.reject_next_update > 0
                && state.reject_only_id.as_ref().is_none_or(|only| only == id)
            {
                state.reject_next_update -= 1;
                not_updated.insert(
                    id.clone(),
                    json!({"type":state.reject_update_type.as_deref().unwrap_or("forbidden")}),
                );
                continue;
            }
            let Some(index) = state.emails.iter().position(|email| &email.id == id) else {
                not_updated.insert(id.clone(), json!({"type":"notFound"}));
                continue;
            };
            let mut candidate = state.emails[index].clone();
            let mut invalid = None;
            for (path, value) in patch.as_object().unwrap() {
                if let Some(keyword) = path.strip_prefix("keywords/") {
                    let keyword = keyword.replace("~1", "/").replace("~0", "~");
                    if value.is_null() {
                        candidate.keywords.retain(|key| key != &keyword);
                    } else if value == &json!(true) {
                        if !candidate.keywords.contains(&keyword) {
                            candidate.keywords.push(keyword);
                        }
                    } else {
                        invalid = Some(format!("{path} must be true or null, got {value}"));
                    }
                } else if path == "mailboxIds" {
                    match value.as_object() {
                        Some(ids) if !ids.is_empty() && ids.values().all(|v| v == &json!(true)) => {
                            candidate.mailbox_ids = ids.keys().cloned().collect();
                        }
                        _ => invalid = Some("invalid mailboxIds replacement".into()),
                    }
                } else {
                    invalid = Some(format!("unexpected patch path {path}"));
                }
            }
            if let Some(description) = invalid {
                not_updated.insert(
                    id.clone(),
                    json!({"type":"invalidProperties", "description":description}),
                );
                continue;
            }
            let before_keywords: BTreeSet<_> = state.emails[index].keywords.iter().collect();
            let after_keywords: BTreeSet<_> = candidate.keywords.iter().collect();
            let changed = before_keywords != after_keywords
                || state.emails[index].mailbox_ids != candidate.mailbox_ids;
            state.emails[index] = candidate;
            if changed {
                advance_email_state(state);
            }
            updated.insert(id.clone(), Value::Null);
        }
    }
    let mut destroyed = Vec::new();
    let mut not_destroyed = serde_json::Map::new();
    if let Some(ids) = args["destroy"].as_array() {
        for id in ids {
            if let Some(index) = state
                .emails
                .iter()
                .position(|email| Some(email.id.as_str()) == id.as_str())
            {
                state.emails.remove(index);
                destroyed.push(id.clone());
                advance_email_state(state);
            } else {
                not_destroyed.insert(id.as_str().unwrap().to_string(), json!({"type":"notFound"}));
            }
        }
    }
    if !updated.is_empty()
        && let Some((source, target)) = state.rename_after_set.take()
    {
        std::fs::rename(source, target).unwrap();
    }
    if !updated.is_empty() {
        for (source, target) in std::mem::take(&mut state.rename_many_after_set) {
            std::fs::rename(source, target).unwrap();
        }
    }
    if state.omitted_acknowledgements > 0 {
        state.omitted_acknowledgements -= 1;
        updated.clear();
        destroyed.clear();
    }
    let mut response = json!(["Email/set", {"accountId":ACCOUNT_ID, "oldState":old, "newState":state.email_state,
        "updated":updated, "notUpdated":not_updated, "destroyed":destroyed, "notDestroyed":not_destroyed}, call_id]);
    if state.omit_set_new_state_on == Some(state.set_requests.len()) {
        response[1].as_object_mut().unwrap().remove("newState");
    }
    state.set_results.push(response.clone());
    response
}

struct FlagFixture {
    _server: MockServer,
    state: Arc<Mutex<ServerState>>,
    temp: tempfile::TempDir,
    conn: Connection,
    config: Config,
    verifier: jmap_client::client::Client,
}

impl FlagFixture {
    async fn new(flags: &str, custom: bool, strategy: ConflictStrategy) -> Self {
        Self::new_with_set_limit(flags, custom, strategy, 500).await
    }

    async fn new_with_set_limit(
        flags: &str,
        custom: bool,
        strategy: ConflictStrategy,
        max_set: u64,
    ) -> Self {
        let server = MockServer::start().await;
        mount_session(&server, max_set).await;
        let mut keywords: Vec<_> = flags_to_keywords(flags).into_keys().collect();
        if custom {
            keywords.push("keep-me".into());
        }
        let state = Arc::new(Mutex::new(ServerState {
            mailbox_state: "mb-1".into(),
            email_state: "e-1".into(),
            mailboxes: vec![
                RemoteMailbox {
                    id: "MB-INBOX".into(),
                    name: "Inbox".into(),
                    role: Some("inbox".into()),
                    parent_id: None,
                },
                RemoteMailbox {
                    id: "MB-TRASH".into(),
                    name: "Deleted Items".into(),
                    role: Some("trash".into()),
                    parent_id: None,
                },
            ],
            emails: vec![RemoteEmail {
                id: "E1".into(),
                blob_id: "B1".into(),
                thread_id: "T1".into(),
                mailbox_ids: vec!["MB-INBOX".into()],
                keywords,
                message_id: Some("audit@example.com".into()),
            }],
            blobs: HashMap::from([(
                "B1".into(),
                b"Message-ID: <audit@example.com>\r\nSubject: flags\r\n\r\nbody\r\n".to_vec(),
            )]),
            ..Default::default()
        }));
        mount_jmap(&server, state.clone()).await;
        mount_blob_downloads(&server, state.clone()).await;
        let verifier = jmap_client::client::Client::new()
            .credentials("test-token")
            .connect(&server.uri())
            .await
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let conn = fresh_db(&temp);
        let mut config = test_config(&server, temp.path(), vec![]);
        config.sync.conflict_strategy = strategy;
        let fixture = Self {
            _server: server,
            state,
            temp,
            conn,
            config,
            verifier,
        };
        let initial = fixture.sync().await.unwrap();
        assert_eq!(
            initial.downloaded, 1,
            "fixture must perform a real initial pull"
        );
        fixture
    }
    async fn sync(&self) -> anyhow::Result<crate::sync::engine::SyncOutcome> {
        SyncEngine::sync(&self.conn, &self.config, false).await
    }
    fn file(&self) -> PathBuf {
        let mut files = vec![];
        for folder in ["INBOX", "Deleted Items"] {
            for sub in ["cur", "new"] {
                let directory = self.temp.path().join(folder).join(sub);
                if let Ok(entries) = std::fs::read_dir(directory) {
                    files.extend(
                        entries
                            .map(|entry| entry.unwrap().path())
                            .filter(|path| path.is_file()),
                    );
                }
            }
        }
        assert_eq!(
            files.len(),
            1,
            "fixture must retain exactly one message: {files:?}"
        );
        files.remove(0)
    }
    fn local_change(&self, flags: &str, moved: bool, keep_new: bool) {
        let old = self.file();
        let id = old
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split(":2,")
            .next()
            .unwrap();
        let folder = if moved { "Deleted Items" } else { "INBOX" };
        let sub = if keep_new { "new" } else { "cur" };
        let target = self
            .temp
            .path()
            .join(folder)
            .join(sub)
            .join(format!("{id}:2,{flags}"));
        std::fs::rename(old, target).unwrap();
    }
    fn server_change(&self, flags: &str, custom: bool) {
        let mut state = self.state.lock().unwrap();
        state.emails[0].keywords = flags_to_keywords(flags).into_keys().collect();
        if custom {
            state.emails[0].keywords.push("keep-me".into());
        }
        advance_email_state(&mut state);
    }
    fn reopen_db(&mut self) {
        let path = self.conn.path().unwrap().to_string();
        self.conn = db::open_or_recreate(std::path::Path::new(&path)).unwrap();
    }

    fn reset_db(&mut self) {
        self.conn = db::open_or_recreate(&self.temp.path().join("fresh.db")).unwrap();
    }
    async fn snapshot(&self) -> Value {
        // Fresh Email/get from a separate client, not the client's cached DB.
        let remote = crate::jmap::email::get_by_ids(&self.verifier, &["E1".into()])
            .await
            .unwrap()
            .remove(0);
        let path = self.file();
        let suffix = path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split_once(":2,")
            .map_or("", |(_, flags)| flags);
        let folder = path
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        let subdir = path
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        let record = queries::get_message_by_jmap_id(&self.conn, &"E1".into())
            .unwrap()
            .unwrap();
        let cached: Value = serde_json::from_str(&record.jmap_keywords).unwrap();
        let local_state =
            queries::get_local_state(&self.conn, record.maildir_id.as_ref().unwrap()).unwrap();
        let observed_folder = local_state.as_ref().map(|(folder, _)| folder);
        let observed_flags = local_state.as_ref().map(|(_, flags)| flags);
        let flag_state = crate::state::flag_sync::get(&self.conn, &"E1".into())
            .unwrap()
            .unwrap();
        let state = self.state.lock().unwrap();
        json!({"file_flags":suffix,"file_folder":folder,"subdir":subdir,
            "acknowledged_flags":flag_state.baseline,"flag_attempt":flag_state.attempt,"flags_blocked":flag_state.blocked,
            "server_flags":keywords_to_flags(&remote.keywords),"server_keywords":remote.keywords,
            "cached_flags":record.flags,"cached_keywords":cached,"local_observed_flags":observed_flags,
            "observed_folder":observed_folder,"cached_mailbox_id":record.jmap_mailbox_id,
            "cached_maildir_id":record.maildir_id,"cached_blob_id":record.jmap_blob_id,"cached_thread_id":record.jmap_thread_id,
            "server_email_id":remote.id,"server_mailboxes":remote.mailbox_ids,"server_blob_id":remote.blob_id,"server_thread_id":remote.thread_id,
            "cursor":queries::get_jmap_state(&self.conn,ACCOUNT_ID,"Email").unwrap(),
            "server_state":state.email_state,"set_requests":state.set_requests.len(),"blob_requests":state.blob_requests})
    }
}
