//! Offline CLI status must report scoped history without acknowledging it.

use jma_mail::state::{db, flag_sync, queries};
use std::process::Command;

#[test]
fn status_reports_selected_pending_blocked_and_unverified_history_offline() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("Maildir");
    let db_path = temp.path().join("state.db");
    let conn = db::open_or_recreate(&db_path).unwrap();
    for (mailbox, folder, id) in [("MB-INBOX", "INBOX", "E1"), ("MB-ARCH", "Archive", "E2")] {
        for subdir in ["cur", "new", "tmp"] {
            std::fs::create_dir_all(root.join(folder).join(subdir)).unwrap();
        }
        queries::upsert_mailbox(
            &conn,
            &queries::MailboxRecord {
                jmap_mailbox_id: mailbox.into(),
                name: folder.into(),
                role: (folder == "INBOX").then(|| "inbox".into()),
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
                message_id: format!("{id}@example.com").into(),
                flags: "FS".into(),
                jmap_keywords: r#"{"$seen":true,"$flagged":true,"keep-me":true}"#.into(),
            },
        )
        .unwrap();
        queries::upsert_local_state(&conn, &id.into(), folder, "S", None).unwrap();
        std::fs::write(
            root.join(folder).join("cur").join(format!("{id}:2,S")),
            format!("Message-ID: <{id}@example.com>\r\n\r\nbody\r\n"),
        )
        .unwrap();
    }
    flag_sync::acknowledge(&conn, &"E1".into(), "FS").unwrap();
    flag_sync::failure(&conn, &"E1".into(), true, true, "forbidden").unwrap();
    let before = flag_sync::get(&conn, &"E1".into()).unwrap();
    for (filter, unverified) in [(r#"["INBOX"]"#, 0), ("[]", 1)] {
        let config_path = temp.path().join(format!("config-{unverified}.toml"));
        let config = format!(
            r#"
[account]
email = "test@example.com"
token = "offline-test-token"
session_url = "http://127.0.0.1:1"
[sync]
maildir_path = {}
mailboxes = {filter}
folder_layout = "fs"
[state]
db_path = {}
"#,
            serde_json::to_string(root.to_str().unwrap()).unwrap(),
            serde_json::to_string(db_path.to_str().unwrap()).unwrap()
        );
        std::fs::write(&config_path, config).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let output = Command::new(env!("CARGO_BIN_EXE_jma"))
            .arg("status")
            .arg("--config")
            .arg(&config_path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains(&format!("Flag histories (cached selected bindings): 1 pending, 1 blocked/review, {unverified} unverified")), "{text}");
        assert_eq!(flag_sync::get(&conn, &"E1".into()).unwrap(), before);
        assert!(
            flag_sync::get(&conn, &"E2".into()).unwrap().is_none(),
            "status must not initialize a legacy message's acknowledgement history"
        );
    }
}
