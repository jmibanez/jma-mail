//! Reviewed destructive maintenance must not erase the bindings it checks.

use jma_mail::state::{db, queries};
use std::path::{Path, PathBuf};
use std::process::Command;

fn fixture(root: &Path, mismatch: bool) -> (PathBuf, PathBuf, PathBuf) {
    let mail = root.join("mail");
    std::fs::create_dir_all(mail.join("INBOX/cur")).unwrap();
    std::fs::create_dir_all(mail.join("INBOX/new")).unwrap();
    let db_path = root.join("state.db");
    let conn = db::open_or_recreate(&db_path).unwrap();
    queries::upsert_message(
        &conn,
        &queries::MessageRecord {
            jmap_email_id: "E1".into(),
            jmap_blob_id: Some("B1".into()),
            jmap_thread_id: Some("T1".into()),
            jmap_mailbox_id: "MB1".into(),
            maildir_id: Some("local".into()),
            message_id: "local@example.invalid".into(),
            flags: "S".into(),
            jmap_keywords: r#"{"$seen":true}"#.into(),
        },
    )
    .unwrap();
    queries::upsert_local_state(&conn, &"local".into(), "INBOX", "S", None).unwrap();
    std::fs::write(
        mail.join("INBOX/cur/local:2,S"),
        b"Message-ID: <local@example.invalid>\r\n\r\nbody",
    )
    .unwrap();
    if mismatch {
        conn.pragma_update(None, "user_version", db::SCHEMA_VERSION + 1)
            .unwrap();
    }
    drop(conn); // Inspect the on-disk DB after the child, not an unlinked inode.
    let config = root.join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[account]
email = "destroyids-{}@example.invalid"
token = "fake-test-only"
session_url = "http://127.0.0.1:0"
[sync]
maildir_path = {}
folder_layout = "fs"
retry_max_attempts = 1
retry_initial_backoff_ms = 1
retry_max_backoff_ms = 1
[state]
db_path = {}
"#,
            uuid::Uuid::new_v4(),
            serde_json::to_string(&mail).unwrap(),
            serde_json::to_string(&db_path).unwrap()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let manifest = root.join("reviewed.txt");
    std::fs::write(
        &manifest,
        "# one unbound and one locally bound id\nE2\nE1\n",
    )
    .unwrap();
    (config, manifest, db_path)
}

fn run(config: &Path, manifest: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_jma"))
        .args(["janitor", "destroyids", "--config"])
        .arg(config)
        .arg("--from")
        .arg(manifest)
        .arg("--yes")
        .output()
        .unwrap()
}

#[test]
fn destroyids_schema_mismatch_cannot_erase_the_bindings_before_preflight() {
    let temp = tempfile::tempdir().unwrap();
    let (config, manifest, db_path) = fixture(temp.path(), true);
    let output = run(&config, &manifest);
    assert!(!output.status.success());
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let version: u32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(
        version,
        db::SCHEMA_VERSION + 1,
        "reviewed deletion must refuse an incompatible DB, not recreate it before checking mapped ids"
    );
    assert_eq!(queries::count_messages(&conn).unwrap(), 1);
    assert!(temp.path().join("mail/INBOX/cur/local:2,S").is_file());
}

#[test]
fn maintenance_previews_never_recreate_incompatible_binding_history() {
    for args in [
        vec!["janitor"],
        vec!["janitor", "dedupe", "--apply"],
        vec!["janitor", "remotededupe"],
        vec!["janitor", "rebindfolders"],
        vec!["janitor", "prune"],
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (config, _, db_path) = fixture(temp.path(), true);
        let output = Command::new(env!("CARGO_BIN_EXE_jma"))
            .args(&args)
            .arg("--config")
            .arg(config)
            .output()
            .unwrap();
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(
            version,
            db::SCHEMA_VERSION + 1,
            "maintenance must preserve the history used for identity safety: {args:?}"
        );
        assert!(
            !output.status.success(),
            "incompatible history must be refused: {args:?}"
        );
        assert_eq!(queries::count_messages(&conn).unwrap(), 1);
    }
}

#[test]
fn destroyids_one_mapped_id_refuses_the_entire_manifest_before_connecting() {
    let temp = tempfile::tempdir().unwrap();
    let (config, manifest, db_path) = fixture(temp.path(), false);
    let output = run(&config, &manifest);
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("Refusing exact destroy manifest")
            && error.contains("E1")
            && error.contains("message_map"),
        "{error}"
    );
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    assert_eq!(queries::count_messages(&conn).unwrap(), 1);
}
