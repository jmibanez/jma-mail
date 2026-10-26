//! Real CLI process against a strict stateful flags-only JMAP peer.

use jma_mail::maildir_ops::{sentinel, store};
use jma_mail::state::{db, flag_sync, queries};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

#[derive(Default)]
struct Model {
    keywords: HashMap<String, HashMap<String, bool>>,
    state: usize,
    reject: bool,
    sets: Vec<Value>,
    methods: Vec<String>,
}

async fn server() -> (MockServer, Arc<Mutex<Model>>) {
    let server = MockServer::start().await;
    let uri = server.uri();
    Mock::given(method("GET")).and(path("/.well-known/jmap"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": { "maxSizeUpload":50000000, "maxConcurrentUpload":4, "maxSizeRequest":10000000, "maxConcurrentRequests":4, "maxCallsInRequest":16, "maxObjectsInGet":500, "maxObjectsInSet":500, "collationAlgorithms":["i;ascii-numeric"] },
                "urn:ietf:params:jmap:mail": { "maxMailboxesPerEmail":100, "maxMailboxDepth":null, "maxSizeMailboxName":255, "maxSizeAttachmentsPerEmail":50000000, "emailQuerySortOptions":[], "mayCreateTopLevelMailbox":true }
            },
            "accounts": { "acct": { "name":"fixture", "isPersonal":true, "isReadOnly":false, "accountCapabilities":{"urn:ietf:params:jmap:mail":{}} } },
            "primaryAccounts":{"urn:ietf:params:jmap:mail":"acct"}, "username":"fixture@example.invalid",
            "apiUrl":format!("{uri}/jmap"), "downloadUrl":format!("{uri}/download/{{accountId}}/{{blobId}}/{{name}}?accept={{type}}"), "uploadUrl":format!("{uri}/upload/{{accountId}}"), "eventSourceUrl":format!("{uri}/events"), "state":"session"
        }))).mount(&server).await;
    let model = Arc::new(Mutex::new(Model {
        keywords: ["E1", "E2"]
            .into_iter()
            .map(|id| {
                (
                    id.into(),
                    HashMap::from([("$flagged".into(), true), ("keep-me".into(), true)]),
                )
            })
            .collect(),
        state: 1,
        ..Default::default()
    }));
    let state = model.clone();
    Mock::given(method("POST")).and(path("/jmap")).respond_with(move |request: &Request| {
        let request: Value = serde_json::from_slice(&request.body).unwrap();
        let mut state = state.lock().unwrap();
        let responses: Vec<Value> = request["methodCalls"].as_array().unwrap().iter().map(|call| {
            let name = call[0].as_str().unwrap();
            state.methods.push(name.into());
            let args = &call[1]; let tag = &call[2];
            match name {
                "Mailbox/get" => json!([name, {"accountId":"acct", "state":"mb1", "list":[
                    {"id":"MB1", "name":"INBOX", "role":"inbox", "parentId":null, "sortOrder":0, "totalEmails":1, "unreadEmails":1},
                    {"id":"MB2", "name":"Archive", "role":null, "parentId":null, "sortOrder":0, "totalEmails":1, "unreadEmails":1}
                ], "notFound":[]}, tag]),
                "Email/get" => {
                    let mut list = Vec::new(); let mut missing = Vec::new();
                    for id in args["ids"].as_array().unwrap() {
                        let key = id.as_str().unwrap();
                        if let Some(keywords) = state.keywords.get(key) {
                            list.push(json!({"id":key,"blobId":format!("B-{key}"),"threadId":format!("T-{key}"),"messageId":[format!("{key}@example.invalid")],"keywords":keywords,"mailboxIds":{if key=="E1" {"MB1"} else {"MB2"}:true}}));
                        } else { missing.push(id.clone()); }
                    }
                    json!([name,{"accountId":"acct","state":format!("e{}",state.state),"list":list,"notFound":missing},tag])
                },
                "Email/set" => {
                    state.sets.push(args.clone());
                    if args["ifInState"] != format!("e{}",state.state) { return json!(["error",{"type":"stateMismatch"},tag]); }
                    let old = format!("e{}",state.state);
                    let mut updated = serde_json::Map::new(); let mut rejected = serde_json::Map::new();
                    for (id, patch) in args["update"].as_object().unwrap() {
                        if state.reject { rejected.insert(id.clone(),json!({"type":"forbidden"})); continue; }
                        let keywords = state.keywords.get_mut(id).unwrap();
                        for (pointer, value) in patch.as_object().unwrap() {
                            let keyword = pointer.strip_prefix("keywords/").expect("keyword-only repair");
                            if value.is_null() { keywords.remove(keyword); }
                            else { assert_eq!(*value, true); keywords.insert(keyword.into(),true); }
                        }
                        updated.insert(id.clone(),Value::Null);
                        state.state+=1;
                    }
                    json!([name,{"accountId":"acct","oldState":old,"newState":format!("e{}",state.state),"updated":updated,"notUpdated":rejected},tag])
                },
                _ => json!(["error",{"type":"unknownMethod"},tag]),
            }
        }).collect();
        ResponseTemplate::new(200).set_body_json(json!({"methodResponses":responses,"sessionState":"session"}))
    }).mount(&server).await;
    (server, model)
}

fn fixture(
    temp: &Path,
    uri: &str,
) -> (rusqlite::Connection, std::path::PathBuf, std::path::PathBuf) {
    let root = temp.join("Maildir");
    let db_path = temp.join("state.db");
    let conn = db::open_or_recreate(&db_path).unwrap();
    for (id, mailbox, folder) in [("E1", "MB1", "INBOX"), ("E2", "MB2", "Archive")] {
        store::ensure_maildir(&root.join(folder)).unwrap();
        sentinel::write(
            &root.join(folder),
            &sentinel::MailboxMapping {
                jmap_mailbox_id: mailbox.into(),
                parent_jmap_mailbox_id: None,
                server_name: folder.into(),
            },
        )
        .unwrap();
        queries::upsert_mailbox(
            &conn,
            &queries::MailboxRecord {
                jmap_mailbox_id: mailbox.into(),
                name: folder.into(),
                role: (id == "E1").then(|| "inbox".into()),
                parent_id: None,
                maildir_folder: folder.into(),
                sort_order: 0,
                remote_path: Some(folder.into()),
            },
        )
        .unwrap();
        queries::upsert_message(
            &conn,
            &queries::MessageRecord {
                jmap_email_id: id.into(),
                jmap_blob_id: Some(format!("B-{id}").into()),
                jmap_thread_id: Some(format!("T-{id}").into()),
                jmap_mailbox_id: mailbox.into(),
                maildir_id: Some(id.into()),
                message_id: format!("{id}@example.invalid").into(),
                flags: "F".into(),
                jmap_keywords: r#"{"$flagged":true,"keep-me":true}"#.into(),
            },
        )
        .unwrap();
        queries::upsert_local_state(&conn, &id.into(), folder, "SX", None).unwrap();
        flag_sync::store(
            &conn,
            &id.into(),
            &flag_sync::FlagSyncState {
                baseline: None,
                review: true,
                blocked: true,
                ..Default::default()
            },
        )
        .unwrap();
        std::fs::write(
            root.join(folder).join("cur").join(format!("{id}:2,SX")),
            format!("Message-ID: <{id}@example.invalid>\r\n\r\nbody\r\n"),
        )
        .unwrap();
    }
    queries::set_jmap_state(&conn, "acct", "Email", "held-cursor").unwrap();
    let config = temp.join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[account]
email = "repair-fixture@example.invalid"
token = "fake-token"
session_url = "{uri}"
[sync]
maildir_path = {}
mailboxes = ["INBOX"]
folder_layout = "fs"
conflict_strategy = "server-wins"
[state]
db_path = {}
"#,
            serde_json::to_string(&root).unwrap(),
            serde_json::to_string(&db_path).unwrap()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    (conn, config, root)
}

async fn run(config: &Path, args: &[&str]) -> std::process::Output {
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_jma"))
            .arg("repair-flags")
            .arg("--config")
            .arg(config)
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn flag_repair_cli_previews_then_applies_only_selected_flags() {
    let (server, model) = server().await;
    let temp = tempfile::tempdir().unwrap();
    let (conn, config, root) = fixture(temp.path(), &server.uri());
    let history = flag_sync::get(&conn, &"E1".into()).unwrap();
    let excluded = flag_sync::get(&conn, &"E2".into()).unwrap();
    let preview = run(&config, &["--authority", "local", "E1"]).await;
    assert!(
        preview.status.success(),
        "{}",
        String::from_utf8_lossy(&preview.stderr)
    );
    let text = String::from_utf8(preview.stdout).unwrap();
    assert!(
        text.contains("raw suffix=\"SX\"")
            && text.contains("baseline=NULL")
            && text.contains("keep-me")
            && text.contains("--apply"),
        "{text}"
    );
    assert_eq!(flag_sync::get(&conn, &"E1".into()).unwrap(), history);
    assert!(model.lock().unwrap().sets.is_empty());
    let applied = run(&config, &["--authority", "local", "--apply", "E1"]).await;
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let verifier = jmap_client::client::Client::new()
        .credentials("fake-token")
        .connect(&server.uri())
        .await
        .unwrap();
    let remote = jma_mail::jmap::email::get_by_ids(&verifier, &["E1".into(), "E2".into()])
        .await
        .unwrap();
    assert_eq!(
        remote[0].keywords,
        HashMap::from([("$seen".into(), true), ("keep-me".into(), true)])
    );
    assert!(remote[1].keywords.contains_key("$flagged"));
    assert_eq!(
        flag_sync::get(&conn, &"E1".into())
            .unwrap()
            .unwrap()
            .baseline
            .as_deref(),
        Some("S")
    );
    assert_eq!(flag_sync::get(&conn, &"E2".into()).unwrap(), excluded);
    assert!(root.join("INBOX/cur/E1:2,SX").is_file() && root.join("Archive/cur/E2:2,SX").is_file());
    assert_eq!(
        queries::get_jmap_state(&conn, "acct", "Email")
            .unwrap()
            .as_deref(),
        Some("held-cursor")
    );
    {
        let mut state = model.lock().unwrap();
        state.keywords.get_mut("E1").unwrap().remove("$seen");
        state
            .keywords
            .get_mut("E1")
            .unwrap()
            .insert("$flagged".into(), true);
        state.state += 1;
    }
    let pulled = run(&config, &["--authority", "server", "--apply", "E1"]).await;
    assert!(
        pulled.status.success(),
        "{}",
        String::from_utf8_lossy(&pulled.stderr)
    );
    assert!(root.join("INBOX/cur/E1:2,FX").is_file());
    assert_eq!(
        flag_sync::get(&conn, &"E1".into())
            .unwrap()
            .unwrap()
            .baseline
            .as_deref(),
        Some("F")
    );
    assert_eq!(flag_sync::get(&conn, &"E2".into()).unwrap(), excluded);
    assert_eq!(
        queries::get_jmap_state(&conn, "acct", "Email")
            .unwrap()
            .as_deref(),
        Some("held-cursor")
    );
    let state = model.lock().unwrap();
    assert_eq!(state.sets.len(), 1);
    assert_eq!(
        state.sets[0]["update"]["E1"]["keywords/$flagged"],
        Value::Null
    );
    assert!(
        state
            .methods
            .iter()
            .all(|method| ["Email/get", "Mailbox/get", "Email/set"].contains(&method.as_str()))
    );
}

#[tokio::test]
async fn flag_repair_cli_never_recreates_missing_or_incompatible_databases() {
    let (server, model) = server().await;
    let temp = tempfile::tempdir().unwrap();
    let (conn, config, root) = fixture(temp.path(), &server.uri());
    conn.pragma_update(None, "user_version", db::SCHEMA_VERSION + 1)
        .unwrap();
    let failed = run(&config, &["--authority", "local", "--apply", "E1"]).await;
    assert!(!failed.status.success());
    assert_eq!(
        conn.pragma_query_value::<u32, _>(None, "user_version", |row| row.get(0))
            .unwrap(),
        db::SCHEMA_VERSION + 1
    );
    assert_eq!(queries::count_messages(&conn).unwrap(), 2);
    assert!(root.join("INBOX/cur/E1:2,SX").is_file());
    assert!(model.lock().unwrap().methods.is_empty());
    let db_path = temp.path().join("state.db");
    drop(conn);
    std::fs::rename(&db_path, temp.path().join("held.db")).unwrap();
    assert!(
        !run(&config, &["--authority", "local", "--apply", "E1"])
            .await
            .status
            .success()
    );
    assert!(!db_path.exists());
}

#[tokio::test]
async fn flag_repair_cli_refuses_excluded_ids_and_reports_rejected_apply() {
    let (server, model) = server().await;
    let temp = tempfile::tempdir().unwrap();
    let (conn, config, root) = fixture(temp.path(), &server.uri());
    for args in [
        vec!["--authority", "local", "--apply", "E1", "E2"],
        vec!["--authority", "local", "missing"],
        vec!["--authority", "local", "--apply", "E1", "E1"],
        vec!["E1"],
        vec!["--authority", "local"],
    ] {
        assert!(!run(&config, &args).await.status.success());
        assert!(model.lock().unwrap().sets.is_empty());
    }
    model.lock().unwrap().reject = true;
    let failed = run(&config, &["--authority", "local", "--apply", "E1"]).await;
    assert!(!failed.status.success());
    assert!(
        String::from_utf8_lossy(&failed.stderr).contains("incomplete"),
        "{}",
        String::from_utf8_lossy(&failed.stderr)
    );
    assert!(flag_sync::get(&conn, &"E1".into()).unwrap().unwrap().review);
    assert!(root.join("INBOX/cur/E1:2,SX").is_file());
}
