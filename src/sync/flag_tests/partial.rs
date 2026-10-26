//! Partial Maildirs are still present: a missing empty peer is not deletion.

use super::*;

#[tokio::test]
async fn flag_partial_maildir_push_preview_and_pull_use_the_live_peer() {
    for (initial, missing, in_new, remote, final_subdir) in [
        ("F", "cur", true, "S", "cur"),
        ("S", "new", false, "F", "cur"),
    ] {
        let fixture = FlagFixture::new(initial, true, ConflictStrategy::ServerWins).await;
        let body = std::fs::read(fixture.file()).unwrap();
        let original = fixture.snapshot().await;
        fixture.local_change("X", false, in_new);
        let missing_path = fixture.temp.path().join("INBOX").join(missing);
        std::fs::remove_dir(&missing_path).unwrap();
        let preview = SyncEngine::sync(&fixture.conn, &fixture.config, true)
            .await
            .unwrap();
        assert!(!preview.retry_scope_checked);
        assert!(
            !missing_path.exists(),
            "preview must not heal a missing peer"
        );
        let snapshot = fixture.snapshot().await;
        assert_eq!(snapshot["server_flags"], initial);
        assert_eq!(snapshot["acknowledged_flags"], initial);
        assert!(fixture.state.lock().unwrap().set_requests.is_empty());
        assert_eq!(
            fixture.sync().await.unwrap().flag_updates,
            1,
            "missing {missing}/ must not hide the live filename edit"
        );
        let snapshot = fixture.snapshot().await;
        assert_eq!(snapshot["file_flags"], "X");
        assert_eq!(snapshot["server_flags"], "");
        assert_eq!(snapshot["acknowledged_flags"], "");
        assert!(
            !missing_path.exists(),
            "a remote keyword push has no local directory write"
        );
        fixture.server_change(remote, true);
        assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
        let snapshot = fixture.snapshot().await;
        assert_eq!(snapshot["file_flags"], format!("{remote}X"));
        assert_eq!(snapshot["subdir"], final_subdir);
        assert_eq!(snapshot["acknowledged_flags"], remote);
        assert_eq!(snapshot["cached_keywords"], snapshot["server_keywords"]);
        assert_eq!(snapshot["cached_maildir_id"], original["cached_maildir_id"]);
        assert_eq!(std::fs::read(fixture.file()).unwrap(), body);
        assert_eq!(snapshot["blob_requests"], 1);
        if missing == "new" {
            assert!(
                !missing_path.exists(),
                "cur-side renames do not provision new/"
            );
        }
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_partial_new_only_noop_is_not_a_folder_deletion_under_destructive_policy() {
    let mut fixture = FlagFixture::new("F", true, ConflictStrategy::LocalWins).await;
    fixture.config.sync.allow_destructive_folder_sync = AllowDestructiveFolderSync::Both;
    let cur = fixture.temp.path().join("INBOX/cur");
    std::fs::remove_dir(&cur).unwrap();
    let outcome = fixture.sync().await.unwrap();
    {
        let state = fixture.state.lock().unwrap();
        assert!(
            state.mailbox_set_destroys.is_empty(),
            "a live new/-only Maildir must not be reported as LocalFolderDeleted"
        );
        assert!(state.mailbox_set_creates.is_empty());
        assert!(state.set_requests.is_empty());
    }
    assert_eq!(
        (
            outcome.downloaded,
            outcome.uploaded,
            outcome.deleted,
            outcome.flag_updates
        ),
        (0, 0, 0, 0)
    );
    assert!(!cur.exists(), "a no-op must not create a missing peer");
    assert_eq!(fixture.snapshot().await["file_flags"], "F");
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[test]
fn flag_partial_scanner_refuses_an_unreadable_present_peer() {
    let temp = tempfile::tempdir().unwrap();
    let md = crate::maildir_ops::store::ensure_maildir(temp.path()).unwrap();
    let id = crate::maildir_ops::store::store_message(&md, b"Message-ID: <x@x>\r\n\r\nbody", "S")
        .unwrap();
    std::fs::remove_dir(temp.path().join("new")).unwrap();
    std::fs::write(temp.path().join("new"), b"not a directory").unwrap();
    let binding = Arc::new(crate::domain::MailboxFolderBinding {
        jmap_mailbox_id: crate::domain::MaybeReference::Value("MB-INBOX".into()),
        server_name: "Inbox".into(),
        maildir_folder: "INBOX".into(),
        remote_path: "Inbox".into(),
    });
    let known = HashMap::from([(id, ("MB-INBOX".into(), "S".into()))]);
    assert!(
        crate::maildir_ops::scan::scan_folder(&md, &binding, &known).is_err(),
        "the pinned iterator silently swallows read_dir errors; scanning must not treat a present unreadable peer as empty"
    );
}

#[test]
fn flag_partial_snapshot_setter_refuses_an_unreadable_present_peer() {
    let temp = tempfile::tempdir().unwrap();
    let md = crate::maildir_ops::store::ensure_maildir(temp.path()).unwrap();
    let id = crate::maildir_ops::store::store_message(&md, b"body", "S").unwrap();
    std::fs::remove_dir(temp.path().join("new")).unwrap();
    std::fs::write(temp.path().join("new"), b"not a directory").unwrap();
    assert!(
        crate::maildir_ops::store::set_flags_if_unchanged(&md, id.as_ref(), "S", "FS").is_err(),
        "snapshot validation must read both present peers successfully before a rename"
    );
    assert!(temp.path().join("cur").join(format!("{id}:2,S")).is_file());
}

#[tokio::test]
async fn flag_partial_new_only_rebuild_adopts_the_existing_file_without_body_transfer() {
    let mut fixture = FlagFixture::new("F", true, ConflictStrategy::ServerWins).await;
    let file = fixture.file();
    let cur = fixture.temp.path().join("INBOX/cur");
    std::fs::remove_dir(&cur).unwrap();
    fixture.reset_db();
    SyncEngine::sync(&fixture.conn, &fixture.config, true)
        .await
        .unwrap();
    assert!(!cur.exists(), "preview must not create the missing cur/");
    let outcome = fixture.sync().await.unwrap();
    assert_eq!(
        outcome.downloaded, 0,
        "a partial existing Maildir must still be indexed for adoption"
    );
    assert_eq!(fixture.file(), file);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["blob_requests"], 1);
    assert_eq!(snapshot["file_flags"], "F");
    assert_eq!(snapshot["acknowledged_flags"], "F");
    assert!(fixture.sync().await.unwrap().already_in_sync);
}
