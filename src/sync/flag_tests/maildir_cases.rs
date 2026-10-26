//! Maildir identity/placement regressions through real engine cycles.

use super::*;

fn delete_trash(fixture: &FlagFixture) {
    let mut state = fixture.state.lock().unwrap();
    state.mailboxes.retain(|mailbox| mailbox.id != "MB-TRASH");
    state.mailbox_state = "mb-2".into();
}

#[tokio::test]
async fn flag_counter_move_changed_basename_preserves_remote_and_live_binding_without_another_edit()
{
    for (flags, raw) in [("S", "SX"), ("F", "FX")] {
        let mut fixture = FlagFixture::new(flags, true, ConflictStrategy::ServerWins).await;
        fixture.config.sync.allow_destructive_folder_sync = AllowDestructiveFolderSync::DeleteLocal;
        fixture.local_change(raw, false, false);
        assert_eq!(fixture.sync().await.unwrap().flag_updates, 0);
        let original = queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
            .unwrap()
            .unwrap();
        let old_id = original.maildir_id.clone().unwrap();
        let body = std::fs::read(fixture.file()).unwrap();
        let destination = fixture
            .temp
            .path()
            .join("Deleted Items")
            .join("cur")
            .join(format!("changed-basename:2,{raw}"));
        std::fs::rename(fixture.file(), &destination).unwrap();
        delete_trash(&fixture);
        let first = fixture.sync().await.unwrap();
        let returned = fixture
            .temp
            .path()
            .join("INBOX/cur")
            .join(format!("changed-basename:2,{raw}"));
        let first_returned = returned.is_file();
        // No new source edit: a later normal cycle must not destroy the Email
        // because an old cached basename is missing after the failed counter.
        let second = fixture.sync().await.unwrap();
        let remote = crate::jmap::email::get_by_ids(&fixture.verifier, &["E1".into()])
            .await
            .unwrap();
        assert!(
            first_returned && returned.is_file(),
            "counter must return the actual changed basename: first moved={}, second deleted={}, remote remaining={}",
            first.moved,
            second.deleted,
            remote.len()
        );
        assert_eq!(remote.len(), 1);
        assert_eq!(first.moved, 1);
        assert_eq!(
            (
                first.uploaded,
                first.downloaded,
                first.deleted,
                first.flag_updates
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(
            (
                second.uploaded,
                second.downloaded,
                second.deleted,
                second.flag_updates,
                second.moved
            ),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(std::fs::read(returned).unwrap(), body);
        assert!(!fixture.temp.path().join("Deleted Items").exists());
        assert!(
            !fixture
                .temp
                .path()
                .join(crate::maildir_ops::namespace::RESCUE_FOLDER_NAME)
                .exists()
        );
        let record = queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
            .unwrap()
            .unwrap();
        assert_eq!(
            record.maildir_id.as_ref().unwrap().as_ref(),
            "changed-basename"
        );
        assert_eq!(record.jmap_mailbox_id, original.jmap_mailbox_id);
        assert_eq!(record.jmap_blob_id, original.jmap_blob_id);
        assert_eq!(record.jmap_thread_id, original.jmap_thread_id);
        assert!(
            queries::get_local_state(&fixture.conn, &old_id)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            queries::get_local_state(&fixture.conn, record.maildir_id.as_ref().unwrap())
                .unwrap()
                .unwrap(),
            ("INBOX".into(), raw.into())
        );
        assert_eq!(keywords_to_flags(&remote[0].keywords), flags);
        assert!(fixture.state.lock().unwrap().set_requests.is_empty());
        fixture.reopen_db();
        assert!(fixture.sync().await.unwrap().already_in_sync);
        assert_eq!(fixture.snapshot().await["local_observed_flags"], raw);
    }
}

#[tokio::test]
async fn flag_counter_move_unknown_new_candidate_holds_orphan_without_inventing_adoption() {
    let mut fixture = FlagFixture::new("F", true, ConflictStrategy::ServerWins).await;
    fixture.config.sync.allow_destructive_folder_sync = AllowDestructiveFolderSync::DeleteLocal;
    let moved = fixture
        .temp
        .path()
        .join("Deleted Items/new/changed-basename:2,FX");
    std::fs::rename(fixture.file(), &moved).unwrap();
    delete_trash(&fixture);
    let original = queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    for _ in 0..3 {
        let outcome = fixture.sync().await.unwrap();
        assert!(
            moved.is_file(),
            "unresolved new/ identity must veto folder destruction as well as Email destruction"
        );
        assert!(outcome.deferred_actions > 0 && !outcome.already_in_sync);
        let remote = crate::jmap::email::get_by_ids(&fixture.verifier, &["E1".into()])
            .await
            .unwrap();
        assert_eq!(remote.len(), 1);
        let record = queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
            .unwrap()
            .unwrap();
        assert_eq!(
            record.maildir_id, original.maildir_id,
            "presence alone does not authorize adopting an unknown new/ copy"
        );
        assert_eq!(
            (
                outcome.uploaded,
                outcome.downloaded,
                outcome.moved,
                outcome.deleted
            ),
            (0, 0, 0, 0)
        );
        fixture.reopen_db();
    }
    // Ordinary MUA promotion makes this a supported move pair. No implicit
    // import/adoption or destructive guess is allowed before that new input.
    let promoted = moved
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("cur/changed-basename:2,FX");
    std::fs::rename(moved, promoted).unwrap();
    assert_eq!(fixture.sync().await.unwrap().moved, 1);
    assert_eq!(
        fixture.snapshot().await["cached_maildir_id"],
        "changed-basename"
    );
    assert!(fixture.sync().await.unwrap().already_in_sync);
    assert!(fixture.state.lock().unwrap().set_requests.is_empty());
}

#[tokio::test]
async fn flag_counter_move_failure_holds_folder_for_same_db_retry_without_new_edits() {
    let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    fixture.config.sync.allow_destructive_folder_sync = AllowDestructiveFolderSync::DeleteLocal;
    let moved = fixture
        .temp
        .path()
        .join("Deleted Items/cur/changed-basename:2,SX");
    std::fs::rename(fixture.file(), &moved).unwrap();
    let held = fixture.temp.path().join("held-message");
    fixture.server_change("S", true);
    fixture.state.lock().unwrap().hide_on_get = Some((moved.clone(), held.clone()));
    delete_trash(&fixture);
    let outcome = fixture.sync().await.unwrap();
    assert_eq!(outcome.retryable_local_actions, 1);
    assert!(
        fixture.temp.path().join("Deleted Items").is_dir(),
        "a failed counter must withhold dependent folder deletion"
    );
    assert!(outcome.deferred_actions > 0);
    std::fs::rename(held, moved).unwrap();
    fixture.reopen_db();
    assert_eq!(fixture.sync().await.unwrap().moved, 1);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_folder"], "INBOX");
    assert_eq!(snapshot["file_flags"], "SX");
    assert_eq!(snapshot["local_observed_flags"], "SX");
    assert_eq!(snapshot["cached_maildir_id"], "changed-basename");
    assert_eq!(snapshot["blob_requests"], 1);
    assert_eq!(snapshot["set_requests"], 0);
    assert!(!fixture.temp.path().join("Deleted Items").exists());
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_counter_move_replays_full_metadata_even_when_owned_flags_do_not_change() {
    let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    fixture.config.sync.allow_destructive_folder_sync = AllowDestructiveFolderSync::DeleteLocal;
    let moved = fixture
        .temp
        .path()
        .join("Deleted Items/cur/changed-basename:2,SX");
    std::fs::rename(fixture.file(), moved).unwrap();
    {
        let mut state = fixture.state.lock().unwrap();
        state.emails[0].keywords.push("fresh-custom".into());
        advance_email_state(&mut state);
    }
    delete_trash(&fixture);
    assert_eq!(fixture.sync().await.unwrap().moved, 1);
    fixture.reopen_db();
    fixture.sync().await.unwrap();
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["server_keywords"]["fresh-custom"], true);
    assert_eq!(
        snapshot["cached_keywords"], snapshot["server_keywords"],
        "a superseded adoption must not discard fresh custom metadata behind an advanced cursor: {snapshot}"
    );
    assert_eq!(snapshot["blob_requests"], 1);
    assert_eq!(snapshot["set_requests"], 0);
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_counter_move_same_basename_mirrors_actual_unowned_suffix() {
    let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    fixture.config.sync.allow_destructive_folder_sync = AllowDestructiveFolderSync::DeleteLocal;
    fixture.local_change("SX", true, false);
    delete_trash(&fixture);
    assert_eq!(fixture.sync().await.unwrap().moved, 1);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], "SX");
    assert_eq!(
        snapshot["local_observed_flags"], "SX",
        "a local move must not replace the raw suffix observation with mapped server flags: {snapshot}"
    );
    assert!(fixture.sync().await.unwrap().already_in_sync);
}
