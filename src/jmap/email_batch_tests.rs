//! Inspect actual Email/set requests, not just the reconciliation plan.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{EmailSetOp, set_email_batch};
use crate::config::{Config, SyncConfig};
use crate::domain::{MailboxFolderBinding, MaybeReference};
use crate::state::{db, queries};
use crate::sync::execute::Executor;
use crate::sync::plan::{BoundId, RemoteId, SyncAction, SyncPlan};
use jmap_client::client::{Client, Credentials};
use serde_json::{Map, Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

async fn mock_client(
    max_objects_in_set: usize,
    rejected_id: Option<&'static str>,
) -> (MockServer, Client) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/.well-known/jmap"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 50_000_000,
                    "maxConcurrentUpload": 4,
                    "maxSizeRequest": 10_000_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 16,
                    "maxObjectsInGet": 500,
                    "maxObjectsInSet": max_objects_in_set,
                    "collationAlgorithms": ["i;ascii-numeric"]
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {
                "u1": {
                    "name": "test", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}
                }
            },
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "u1"},
            "username": "test@example.com",
            "apiUrl": format!("{}/jmap", server.uri()),
            "downloadUrl": format!("{}/dl/{{accountId}}/{{blobId}}/{{name}}", server.uri()),
            "uploadUrl": format!("{}/up/{{accountId}}", server.uri()),
            "eventSourceUrl": format!("{}/events", server.uri()),
            "state": "session-state"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = Client::new()
        .credentials(Credentials::bearer("test-token"))
        .connect(&server.uri())
        .await
        .unwrap();
    assert_eq!(
        crate::jmap::limits::max_objects_in_set(&client),
        max_objects_in_set
    );

    let state = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .and(path("/jmap"))
        .respond_with(move |request: &Request| {
            let body: Value = request.body_json().unwrap();
            let calls = body["methodCalls"].as_array().unwrap();
            assert_eq!(calls.len(), 1);
            let call = &calls[0];
            assert_eq!(call[0], "Email/set");
            let args = &call[1];
            let mut updated = Map::new();
            let mut not_updated = Map::new();
            if let Some(updates) = args["update"].as_object() {
                for id in updates.keys() {
                    if Some(id.as_str()) == rejected_id {
                        not_updated.insert(id.clone(), json!({"type": "forbidden"}));
                    } else {
                        updated.insert(id.clone(), Value::Null);
                    }
                }
            }
            let destroyed = args["destroy"].as_array().cloned().unwrap_or_default();
            let changed = !updated.is_empty() || !destroyed.is_empty();
            let old = state.fetch_add(usize::from(changed), Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({
                "methodResponses": [["Email/set", {
                    "accountId": "u1",
                    "oldState": format!("s{old}"),
                    "newState": format!("s{}", old + usize::from(changed)),
                    "updated": updated,
                    "notUpdated": not_updated,
                    "destroyed": destroyed
                }, call[2]]],
                "sessionState": "session-state"
            }))
        })
        .mount(&server)
        .await;

    (server, client)
}

async fn set_arguments(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.url.path() == "/jmap" && request.method.as_str() == "POST")
        .map(|request| {
            let body: Value = request.body_json().unwrap();
            body["methodCalls"][0][1].clone()
        })
        .collect()
}

fn seen_and_move(id: &str) -> Vec<EmailSetOp> {
    vec![
        EmailSetOp::Keywords {
            email_id: id.into(),
            keywords: HashMap::from([("$seen".into(), true)]),
        },
        EmailSetOp::SetMailboxes {
            email_id: id.into(),
            target_mailbox_ids: vec!["deleted-items".into()],
        },
    ]
}

async fn check_move_and_seen(reverse: bool) {
    let (server, client) = mock_client(500, None).await;
    let mut ops = seen_and_move("E1");
    if reverse {
        ops.reverse();
    }
    let outcome = set_email_batch(&client, &ops).await.unwrap();
    assert!(outcome.failed_updates.is_empty());
    let calls = set_arguments(&server).await;
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0]["update"]["E1"],
        json!({"keywords/$seen": true, "mailboxIds": {"deleted-items": true}}),
        "one Email update must contain both the seen patch and the move"
    );
}

#[tokio::test]
async fn email_set_batch_move_preserves_seen() {
    check_move_and_seen(false).await;
}

#[tokio::test]
async fn email_set_batch_keywords_preserve_move() {
    check_move_and_seen(true).await;
}

#[tokio::test]
async fn email_set_batch_merges_patches_and_uses_last_value() {
    let (server, client) = mock_client(500, None).await;
    let ops = vec![
        EmailSetOp::Keywords {
            email_id: "E1".into(),
            keywords: HashMap::from([("$seen".into(), false)]),
        },
        EmailSetOp::SetMailboxes {
            email_id: "E1".into(),
            target_mailbox_ids: vec!["archive".into()],
        },
        EmailSetOp::Keywords {
            email_id: "E1".into(),
            keywords: HashMap::from([("$flagged".into(), true)]),
        },
        EmailSetOp::SetMailboxes {
            email_id: "E1".into(),
            target_mailbox_ids: vec!["deleted-items".into()],
        },
        EmailSetOp::Keywords {
            email_id: "E1".into(),
            keywords: HashMap::from([("$seen".into(), true)]),
        },
    ];
    set_email_batch(&client, &ops).await.unwrap();
    let calls = set_arguments(&server).await;
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0]["update"]["E1"],
        json!({
            "keywords/$seen": true, "keywords/$flagged": true,
            "mailboxIds": {"deleted-items": true}
        })
    );
}

#[tokio::test]
async fn email_set_batch_groups_updates_before_applying_object_limit() {
    let (server, client) = mock_client(1, None).await;
    let mut ops = seen_and_move("E1");
    let other = seen_and_move("E2");
    // Separate same-ID operations in the input: grouping within a chunk
    // alone is insufficient when the server's limit is one object.
    ops.insert(1, other[0].clone());
    ops.push(other[1].clone());
    ops.push(EmailSetOp::Destroy {
        email_id: "E3".into(),
    });
    let outcome = set_email_batch(&client, &ops).await.unwrap();
    let calls = set_arguments(&server).await;
    assert_eq!(
        calls.len(),
        3,
        "two updated objects plus one destroyed object"
    );
    for id in ["E1", "E2"] {
        let updates: Vec<_> = calls
            .iter()
            .filter_map(|args| args["update"].get(id))
            .collect();
        assert_eq!(updates.len(), 1, "{id} must not be split across requests");
        assert_eq!(
            updates[0],
            &json!({"keywords/$seen": true, "mailboxIds": {"deleted-items": true}})
        );
    }
    for args in &calls {
        let updates = args["update"].as_object().map_or(0, Map::len);
        let destroys = args["destroy"].as_array().map_or(0, Vec::len);
        assert_eq!(updates + destroys, 1, "respect maxObjectsInSet");
    }
    assert!(calls.iter().any(|args| args["destroy"] == json!(["E3"])));
    assert_eq!(outcome.chain_old.as_deref(), Some("s0"));
    assert_eq!(outcome.chain_new.as_deref(), Some("s3"));
}

#[tokio::test]
async fn email_set_batch_reports_rejected_combined_update() {
    let (server, client) = mock_client(500, Some("E1")).await;
    let outcome = set_email_batch(&client, &seen_and_move("E1"))
        .await
        .unwrap();
    assert_eq!(outcome.failed_updates, ["E1".into()].into());
    assert!(outcome.failed_destroys.is_empty());
    let calls = set_arguments(&server).await;
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0]["update"]["E1"],
        json!({"keywords/$seen": true, "mailboxIds": {"deleted-items": true}})
    );
}

async fn check_executor_move(rejected: bool) {
    let (server, client) = mock_client(500, rejected.then_some("E1")).await;
    let conn = db::open_in_memory().unwrap();
    queries::upsert_message(
        &conn,
        &queries::MessageRecord {
            jmap_email_id: "E1".into(),
            jmap_blob_id: Some("B1".into()),
            jmap_thread_id: Some("T1".into()),
            jmap_mailbox_id: "inbox".into(),
            maildir_id: Some("M-OLD".into()),
            message_id: "a@x".into(),
            flags: String::new(),
            jmap_keywords: "{}".into(),
        },
    )
    .unwrap();
    queries::upsert_local_state(&conn, &"M-OLD".into(), "INBOX", "", None).unwrap();
    queries::set_jmap_state(&conn, "u1", "Email", "s0").unwrap();
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        sync: SyncConfig {
            maildir_path: root.path().to_string_lossy().into_owned(),
            ..Default::default()
        },
        ..Default::default()
    };
    let id = RemoteId {
        jmap_email_id: "E1".into(),
        message_id: "a@x".into(),
    };
    let keywords = HashMap::from([("$seen".into(), true)]);
    let plan = SyncPlan {
        actions: vec![
            SyncAction::MoveRemote {
                id: id.clone(),
                target_mailbox_ids: vec!["deleted-items".into()],
                from_folder: "INBOX".into(),
                to_folder: "Deleted Items".into(),
            },
            SyncAction::UpdateRemoteKeywords {
                id,
                keywords: keywords.clone(),
                filename_flags: "S".into(),
            },
            SyncAction::AdoptLocalMessage {
                id: BoundId {
                    maildir_id: "M-NEW".into(),
                    jmap_email_id: "E1".into(),
                    message_id: "a@x".into(),
                },
                binding: Arc::new(MailboxFolderBinding {
                    jmap_mailbox_id: MaybeReference::Value("deleted-items".into()),
                    server_name: "Deleted Items".into(),
                    maildir_folder: "Deleted Items".into(),
                    remote_path: "Deleted Items".into(),
                }),
                jmap_blob_id: Some("B1".into()),
                jmap_thread_id: Some("T1".into()),
                keywords,
                filename_flags: "S".into(),
                old_maildir_id: Some("M-OLD".into()),
                old_jmap_email_id: None,
            },
        ],
        new_email_state: Some("s0".into()),
    };
    let outcome = Executor::new(Arc::new(client), &conn, &config, None)
        .unwrap()
        .execute(plan)
        .await
        .unwrap();
    let record = queries::get_message_by_jmap_id(&conn, &"E1".into())
        .unwrap()
        .unwrap();
    let inbox = queries::get_local_state_for_folder(&conn, "INBOX").unwrap();
    let deleted_items = queries::get_local_state_for_folder(&conn, "Deleted Items").unwrap();
    if rejected {
        assert_eq!(outcome.failed_remote_actions, 1);
        assert_eq!(outcome.flag_updates, 0);
        assert_eq!(outcome.moved, 0);
        assert_eq!(record.jmap_mailbox_id.as_ref(), "inbox");
        assert_eq!(record.maildir_id.as_ref().unwrap().as_ref(), "M-OLD");
        assert_eq!(record.flags, "");
        assert_eq!(record.jmap_keywords, "{}");
        assert_eq!(inbox["M-OLD"].1, "");
        assert!(deleted_items.is_empty());
        assert_eq!(
            queries::get_jmap_state(&conn, "u1", "Email")
                .unwrap()
                .as_deref(),
            Some("s0")
        );
    } else {
        assert_eq!(outcome.failed_remote_actions, 0);
        assert_eq!(outcome.flag_updates, 1);
        assert_eq!(outcome.moved, 1);
        assert_eq!(record.jmap_mailbox_id.as_ref(), "deleted-items");
        assert_eq!(record.maildir_id.as_ref().unwrap().as_ref(), "M-NEW");
        assert_eq!(record.flags, "S");
        assert_eq!(
            serde_json::from_str::<Value>(&record.jmap_keywords).unwrap(),
            json!({"$seen": true})
        );
        assert!(inbox.is_empty());
        assert_eq!(deleted_items["M-NEW"].1, "S");
        assert_eq!(
            queries::get_jmap_state(&conn, "u1", "Email")
                .unwrap()
                .as_deref(),
            Some("s1")
        );
        let calls = set_arguments(&server).await;
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0]["update"]["E1"],
            json!({
                "keywords/$seen": true, "mailboxIds": {"deleted-items": true}
            })
        );
    }
}

#[tokio::test]
async fn email_set_batch_executor_records_seen_and_move_on_success() {
    check_executor_move(false).await;
}

#[tokio::test]
async fn email_set_batch_executor_keeps_cached_flags_and_binding_on_rejection() {
    check_executor_move(true).await;
}

#[tokio::test]
async fn email_set_batch_empty_input_sends_nothing() {
    let (server, client) = mock_client(500, None).await;
    let outcome = set_email_batch(&client, &[]).await.unwrap();
    assert!(set_arguments(&server).await.is_empty());
    assert!(outcome.failed_updates.is_empty());
    assert!(outcome.failed_destroys.is_empty());
    assert!(outcome.chain_old.is_none());
    assert!(outcome.chain_new.is_none());
}
