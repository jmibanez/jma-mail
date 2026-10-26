//! Version-guard regressions: real engine cycles, independent server edits.

use super::*;

fn race(state: &mut ServerState, flags: &str, at: usize, remaining: usize) {
    state.edit_before_set = Some(RemoteEdit {
        id: None,
        flags: flags.into(),
        keyword_prefix: "raced",
    });
    state.edit_before_set_at = at;
    state.edit_before_set_remaining = remaining;
}

async fn add_second(fixture: &FlagFixture) {
    add_second_in(fixture, "MB-INBOX").await;
}

async fn add_second_in(fixture: &FlagFixture, mailbox: &str) {
    {
        let mut state = fixture.state.lock().unwrap();
        state.emails.push(RemoteEmail {
            id: "E2".into(),
            blob_id: "B2".into(),
            thread_id: "T2".into(),
            mailbox_ids: vec![mailbox.into()],
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
}

fn seen_pair(fixture: &FlagFixture) -> Vec<(PathBuf, PathBuf)> {
    ["E1", "E2"]
        .into_iter()
        .map(|email_id| {
            let record = queries::get_message_by_jmap_id(&fixture.conn, &email_id.into())
                .unwrap()
                .unwrap();
            let id = record.maildir_id.unwrap();
            let inbox = fixture.temp.path().join("INBOX");
            let seen = inbox.join("cur").join(format!("{id}:2,S"));
            let unread = inbox.join("cur").join(format!("{id}:2,"));
            std::fs::rename(inbox.join("new").join(format!("{id}:2,")), &seen).unwrap();
            (seen, unread)
        })
        .collect()
}

async fn assert_pair(fixture: &FlagFixture, expected: &[(&str, &str)]) {
    let remote = crate::jmap::email::get_by_ids(&fixture.verifier, &["E1".into(), "E2".into()])
        .await
        .unwrap();
    assert_eq!(remote.len(), 2);
    for email in remote {
        let flags = expected
            .iter()
            .find(|(id, _)| *id == email.id.as_ref())
            .unwrap()
            .1;
        assert_eq!(keywords_to_flags(&email.keywords), flags);
        let record = queries::get_message_by_jmap_id(&fixture.conn, &email.id)
            .unwrap()
            .unwrap();
        assert_eq!(record.flags, flags);
        assert_eq!(
            serde_json::from_str::<HashMap<String, bool>>(&record.jmap_keywords).unwrap(),
            email.keywords
        );
        let history = crate::state::flag_sync::get(&fixture.conn, &email.id)
            .unwrap()
            .unwrap();
        assert_eq!(history.baseline.as_deref(), Some(flags));
        assert!(history.attempt.is_none());
        let id = record.maildir_id.unwrap();
        assert!(
            fixture
                .temp
                .path()
                .join("INBOX/cur")
                .join(format!("{id}:2,{flags}"))
                .exists()
        );
        assert_eq!(
            queries::get_local_state(&fixture.conn, &id)
                .unwrap()
                .unwrap()
                .1,
            flags
        );
    }
    assert_eq!(fixture.state.lock().unwrap().blob_requests, 2);
}

async fn assert_flags(fixture: &FlagFixture, raw: &str) {
    let snapshot = fixture.snapshot().await;
    let mapped = crate::state::flag_sync::mapped_flags(raw);
    assert_eq!(snapshot["file_flags"], raw, "{snapshot}");
    assert_eq!(snapshot["local_observed_flags"], raw, "{snapshot}");
    for key in ["server_flags", "cached_flags", "acknowledged_flags"] {
        assert_eq!(snapshot[key], mapped, "{key}: {snapshot}");
    }
    assert_eq!(
        snapshot["cached_keywords"], snapshot["server_keywords"],
        "{snapshot}"
    );
    assert!(snapshot["flag_attempt"].is_null(), "{snapshot}");
    assert_eq!(snapshot["blob_requests"], 1);
}

fn assert_guarded(fixture: &FlagFixture) {
    let state = fixture.state.lock().unwrap();
    assert!(!state.set_requests.is_empty());
    for request in &state.set_requests {
        for call in request["methodCalls"].as_array().unwrap() {
            if call[0] == "Email/set" {
                assert!(
                    call[1]["ifInState"]
                        .as_str()
                        .is_some_and(|value| !value.is_empty()),
                    "reconciliation must not silently send an unguarded mutation: {request}"
                );
            }
        }
    }
}

#[tokio::test]
async fn flag_generation_concurrent_write_replans_whole_set_policy_at_actual_location() {
    for moved in [false, true] {
        for (strategy, expected) in [
            (ConflictStrategy::ServerWins, "FX"),
            (ConflictStrategy::LocalWins, "SX"),
        ] {
            let fixture = FlagFixture::new("", true, strategy).await;
            fixture.local_change("SX", moved, false);
            race(&mut fixture.state.lock().unwrap(), "F", 1, 1);
            let result = fixture.sync().await.unwrap();
            assert_eq!(
                (result.downloaded, result.uploaded, result.deleted),
                (0, 0, 0)
            );
            assert_eq!(result.moved, usize::from(moved));
            assert_flags(&fixture, expected).await;
            assert_guarded(&fixture);
            assert_eq!(fixture.state.lock().unwrap().state_mismatches, 1);
            assert!(fixture.sync().await.unwrap().already_in_sync);
            assert_flags(&fixture, expected).await;
        }
    }
}

#[tokio::test]
async fn flag_generation_initial_cursor_precedes_metadata_reads() {
    let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", false, false);
    fixture.reset_db();
    fixture.state.lock().unwrap().edit_after_metadata_get = Some(RemoteEdit {
        id: Some("E1".into()),
        flags: "F".into(),
        keyword_prefix: "after-get",
    });
    assert_eq!(fixture.sync().await.unwrap().downloaded, 0);
    assert_eq!(
        fixture.sync().await.unwrap().flag_updates,
        1,
        "an update after metadata was returned must not disappear behind a newer cursor"
    );
    assert_flags(&fixture, "FX").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_generation_initial_push_uses_the_pre_observation_state_and_full_metadata() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::LocalWins).await;
    fixture.local_change("SX", false, false);
    fixture.reset_db();
    fixture.state.lock().unwrap().edit_after_metadata_get = Some(RemoteEdit {
        id: Some("E1".into()),
        flags: "F".into(),
        keyword_prefix: "after-get",
    });
    fixture.sync().await.unwrap();
    assert_guarded(&fixture);
    assert_eq!(
        fixture.state.lock().unwrap().state_mismatches,
        1,
        "a state read after stale metadata cannot authorize that old plan"
    );
    assert_flags(&fixture, "SX").await;
    assert_eq!(
        fixture.snapshot().await["server_keywords"]["after-get-0"],
        true
    );
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_generation_clean_mismatch_is_not_evidence_of_an_older_own_write() {
    for moved in [false, true] {
        let fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
        fixture.local_change("SX", moved, false);
        let source = fixture.file();
        let id = source
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split(":2,")
            .next()
            .unwrap();
        let target = source.with_file_name(format!("{id}:2,X"));
        {
            let mut state = fixture.state.lock().unwrap();
            race(&mut state, "S", 1, 1);
            state.rename_before_set = Some((source, target));
        }
        fixture.sync().await.unwrap();
        assert_eq!(fixture.state.lock().unwrap().state_mismatches, 1);
        assert_flags(&fixture, "SX").await;
        assert_guarded(&fixture);
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_generation_ambiguous_own_write_mismatch_preserves_a_newer_local_edit() {
    for moved in [false, true] {
        let fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
        fixture.local_change("SX", moved, false);
        let source = fixture.file();
        let id = source
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split(":2,")
            .next()
            .unwrap();
        let target = source.with_file_name(format!("{id}:2,X"));
        {
            let mut state = fixture.state.lock().unwrap();
            state.rename_after_set = Some((source, target));
            state.lost_replies = 1;
        }
        fixture.sync().await.unwrap();
        assert_eq!(fixture.state.lock().unwrap().state_mismatches, 1);
        assert_flags(&fixture, "X").await;
        assert_guarded(&fixture);
        {
            let state = fixture.state.lock().unwrap();
            assert_eq!(state.set_requests.len(), 3);
            let first = &state.set_requests[0]["methodCalls"][0][1];
            let retry = &state.set_requests[1]["methodCalls"][0][1];
            let replanned = &state.set_requests[2]["methodCalls"][0][1];
            assert_eq!(first["ifInState"], retry["ifInState"]);
            assert_ne!(first["ifInState"], replanned["ifInState"]);
            assert_eq!(replanned["update"]["E1"]["keywords/$seen"], Value::Null);
        }
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_generation_replanning_is_bounded_and_retains_unfinished_history() {
    let fixture = FlagFixture::new("", true, ConflictStrategy::LocalWins).await;
    fixture.local_change("SX", false, false);
    race(&mut fixture.state.lock().unwrap(), "F", 1, 20);
    let incomplete = fixture.sync().await.unwrap();
    assert!(incomplete.needs_retry());
    assert!(!incomplete.already_in_sync);
    assert_eq!(incomplete.flag_updates, 0);
    assert_eq!(
        fixture.state.lock().unwrap().set_requests.len(),
        3,
        "do not spin indefinitely on stateMismatch"
    );
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["acknowledged_flags"], "");
    assert!(
        snapshot["flag_attempt"].is_null(),
        "confirmed precondition rejection is not an ambiguous write"
    );
    fixture.state.lock().unwrap().edit_before_set_remaining = 0;
    fixture.sync().await.unwrap();
    assert_flags(&fixture, "SX").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_generation_later_chunk_mismatch_preserves_earlier_acknowledgement_and_new_edits() {
    let fixture = FlagFixture::new_with_set_limit("", true, ConflictStrategy::ServerWins, 1).await;
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
    let mut renames = Vec::new();
    for email_id in ["E1", "E2"] {
        let record = queries::get_message_by_jmap_id(&fixture.conn, &email_id.into())
            .unwrap()
            .unwrap();
        let id = record.maildir_id.unwrap();
        let inbox = fixture.temp.path().join("INBOX");
        let source = inbox.join("new").join(format!("{id}:2,"));
        let seen = inbox.join("cur").join(format!("{id}:2,S"));
        let unread = inbox.join("cur").join(format!("{id}:2,"));
        std::fs::rename(source, &seen).unwrap();
        renames.push((seen, unread));
    }
    {
        let mut state = fixture.state.lock().unwrap();
        state.rename_many_after_set = renames;
        race(&mut state, "F", 2, 1);
    }
    let result = fixture.sync().await.unwrap();
    assert_eq!(
        result.flag_updates, 3,
        "retain completed remote work when a later chunk needs replanning"
    );
    assert_eq!(
        (result.downloaded, result.uploaded, result.deleted),
        (0, 0, 0)
    );
    assert_guarded(&fixture);
    let (first_id, guards, first_state) = {
        let state = fixture.state.lock().unwrap();
        assert_eq!(state.state_mismatches, 1);
        let first = state.set_results[0][1]["updated"]
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .clone();
        (
            first,
            state
                .set_requests
                .iter()
                .map(|request| request["methodCalls"][0][1]["ifInState"].clone())
                .collect::<Vec<_>>(),
            state.set_results[0][1]["newState"].clone(),
        )
    };
    assert_eq!(guards.len(), 3);
    assert_eq!(
        guards[1], first_state,
        "each chunk follows only an acknowledged continuous state chain"
    );
    let remote = crate::jmap::email::get_by_ids(&fixture.verifier, &["E1".into(), "E2".into()])
        .await
        .unwrap();
    for email in remote {
        let expected = if email.id.as_ref() == first_id {
            ""
        } else {
            "F"
        };
        assert_eq!(keywords_to_flags(&email.keywords), expected);
        let record = queries::get_message_by_jmap_id(&fixture.conn, &email.id)
            .unwrap()
            .unwrap();
        let history = crate::state::flag_sync::get(&fixture.conn, &email.id)
            .unwrap()
            .unwrap();
        assert_eq!(history.baseline.as_deref(), Some(expected));
        assert!(history.attempt.is_none());
        assert_eq!(
            serde_json::from_str::<HashMap<String, bool>>(&record.jmap_keywords).unwrap(),
            email.keywords
        );
        let id = record.maildir_id.unwrap();
        assert!(
            fixture
                .temp
                .path()
                .join("INBOX/cur")
                .join(format!("{id}:2,{expected}"))
                .exists()
        );
    }
    assert_eq!(fixture.state.lock().unwrap().blob_requests, 2);
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_generation_completed_local_pull_is_not_a_new_user_edit_on_replan() {
    let fixture = FlagFixture::new("", true, ConflictStrategy::LocalWins).await;
    add_second(&fixture).await;
    let record = queries::get_message_by_jmap_id(&fixture.conn, &"E2".into())
        .unwrap()
        .unwrap();
    let id = record.maildir_id.unwrap();
    let inbox = fixture.temp.path().join("INBOX");
    std::fs::rename(
        inbox.join("new").join(format!("{id}:2,")),
        inbox.join("cur").join(format!("{id}:2,S")),
    )
    .unwrap();
    {
        let mut state = fixture.state.lock().unwrap();
        state.emails[0].keywords.push("$seen".into());
        advance_email_state(&mut state);
        race(&mut state, "F", 1, 1);
        state.edit_before_set.as_mut().unwrap().id = Some("E1".into());
    }
    let outcome = fixture.sync().await.unwrap();
    assert_eq!(
        outcome.flag_updates, 3,
        "count both completed pulls and the guarded push exactly once"
    );
    assert_eq!(fixture.state.lock().unwrap().state_mismatches, 1);
    assert_pair(&fixture, &[("E1", "F"), ("E2", "S")]).await;
    assert_guarded(&fixture);
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_generation_unsubmitted_chunk_receipts_are_not_own_write_evidence() {
    let fixture = FlagFixture::new_with_set_limit("", true, ConflictStrategy::ServerWins, 1).await;
    add_second(&fixture).await;
    let renames = seen_pair(&fixture);
    {
        let mut state = fixture.state.lock().unwrap();
        race(&mut state, "S", 1, 1);
        state.edit_other_before_set = true;
        state.rename_many_before_set = renames;
    }
    let outcome = fixture.sync().await.unwrap();
    assert_eq!(outcome.flag_updates, 1);
    let first_id = {
        let state = fixture.state.lock().unwrap();
        assert_eq!(state.state_mismatches, 1);
        assert_eq!(
            state.set_requests.len(),
            1,
            "neither clean rejection nor an unsent chunk owns the remote edit"
        );
        state.set_requests[0]["methodCalls"][0][1]["update"]
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .clone()
    };
    let other = if first_id == "E1" { "E2" } else { "E1" };
    assert_pair(&fixture, &[(first_id.as_str(), ""), (other, "S")]).await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_generation_missing_continuation_state_never_falls_back_to_unguarded_writes() {
    let fixture = FlagFixture::new_with_set_limit("", true, ConflictStrategy::ServerWins, 1).await;
    add_second(&fixture).await;
    seen_pair(&fixture);
    fixture.state.lock().unwrap().omit_set_new_state_on = Some(1);
    let outcome = fixture.sync().await.unwrap();
    assert_eq!(outcome.flag_updates, 2);
    assert_guarded(&fixture);
    {
        let state = fixture.state.lock().unwrap();
        assert_eq!(
            state.set_requests.len(),
            2,
            "stop the old batch instead of sending a stale or unguarded second chunk"
        );
        assert_eq!(state.state_mismatches, 0);
        assert_ne!(
            state.set_requests[0]["methodCalls"][0][1]["ifInState"],
            state.set_requests[1]["methodCalls"][0][1]["ifInState"]
        );
    }
    assert_pair(&fixture, &[("E1", "S"), ("E2", "S")]).await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_generation_one_way_replanning_does_not_override_the_direction_filter() {
    let fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", false, false);
    race(&mut fixture.state.lock().unwrap(), "F", 1, 1);
    let excluded = SyncEngine::push_only(&fixture.conn, &fixture.config, false)
        .await
        .unwrap();
    assert_eq!(excluded.flag_updates, 0);
    assert_eq!(excluded.pending_flag_actions, 1);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], "SX");
    assert_eq!(snapshot["server_flags"], "F");
    assert_eq!(snapshot["acknowledged_flags"], "");
    assert!(snapshot["flag_attempt"].is_null());
    fixture.sync().await.unwrap();
    assert_flags(&fixture, "FX").await;
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_generation_no_snapshot_cannot_authorize_remote_effects_from_a_cursor() {
    use crate::sync::plan::{RemoteId, SyncAction, SyncPlan};
    let fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("S", false, false);
    let before = fixture.snapshot().await;
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
            SyncPlan {
                actions: vec![SyncAction::UpdateRemoteKeywords {
                    id: RemoteId {
                        jmap_email_id: "E1".into(),
                        message_id: "audit@example.com".into(),
                    },
                    keywords: crate::maildir_ops::flags::flags_to_keyword_patch("S"),
                    filename_flags: "S".into(),
                }],
                new_email_state: Some("not-a-planning-snapshot".into()),
            },
            None,
        )
        .await;
    assert!(result.is_err());
    assert_eq!(fixture.snapshot().await, before);
    fixture.sync().await.unwrap();
    assert_flags(&fixture, "S").await;
    assert_guarded(&fixture);
}

#[tokio::test]
async fn flag_generation_clean_method_failures_do_not_leave_ambiguous_own_write_receipts() {
    for failures in [
        1,
        crate::jmap::retry::RetryConfig::default().max_attempts as usize,
    ] {
        let fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
        fixture.local_change("SX", false, false);
        let source = fixture.file();
        let id = source
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split(":2,")
            .next()
            .unwrap();
        let target = source.with_file_name(format!("{id}:2,X"));
        {
            let mut state = fixture.state.lock().unwrap();
            race(&mut state, "S", 1, 1);
            state.rename_before_set = Some((source, target));
            state.set_method_failures = failures;
        }
        let first = fixture.sync().await.unwrap();
        if failures > 1 {
            assert_eq!(first.failed_remote_actions, 1);
            assert!(first.needs_retry());
            let snapshot = fixture.snapshot().await;
            assert_eq!(snapshot["acknowledged_flags"], "");
            assert!(
                snapshot["flag_attempt"].is_null(),
                "serverFail guarantees no visible method effects"
            );
            fixture.sync().await.unwrap();
        }
        assert_flags(&fixture, "SX").await;
        assert_guarded(&fixture);
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_generation_remote_destroy_rechecks_a_concurrent_update_under_both_policies() {
    for strategy in [ConflictStrategy::ServerWins, ConflictStrategy::LocalWins] {
        let fixture = FlagFixture::new("", true, strategy).await;
        std::fs::remove_file(fixture.file()).unwrap();
        race(&mut fixture.state.lock().unwrap(), "F", 1, 1);
        let result = fixture.sync().await.unwrap();
        assert_guarded(&fixture);
        assert_eq!(fixture.state.lock().unwrap().state_mismatches, 1);
        let remote = crate::jmap::email::get_by_ids(&fixture.verifier, &["E1".into()])
            .await
            .unwrap();
        match strategy {
            ConflictStrategy::ServerWins => {
                assert_eq!(result.deleted, 0);
                assert_eq!(
                    result.downloaded, 1,
                    "a new server update wins over the local deletion"
                );
                assert_eq!(remote.len(), 1);
                let snapshot = fixture.snapshot().await;
                assert_eq!(snapshot["file_flags"], "F");
                assert_eq!(snapshot["acknowledged_flags"], "F");
                assert_eq!(snapshot["cached_keywords"], snapshot["server_keywords"]);
            }
            ConflictStrategy::LocalWins => {
                assert_eq!(result.deleted, 1);
                assert_eq!(result.downloaded, 0);
                assert!(remote.is_empty());
                assert!(
                    queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
                        .unwrap()
                        .is_none()
                );
            }
        }
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_generation_mismatch_defers_a_queued_mailbox_destroy_until_fresh_policy() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    add_second_in(&fixture, "MB-TRASH").await;
    fixture.config.sync.allow_destructive_folder_sync = AllowDestructiveFolderSync::DeleteRemote;
    let first = queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap()
        .maildir_id
        .unwrap();
    let inbox = fixture.temp.path().join("INBOX");
    std::fs::rename(
        inbox.join("new").join(format!("{first}:2,")),
        inbox.join("cur").join(format!("{first}:2,S")),
    )
    .unwrap();
    std::fs::remove_dir_all(fixture.temp.path().join("Deleted Items")).unwrap();
    {
        let mut state = fixture.state.lock().unwrap();
        race(&mut state, "F", 1, 1);
        state.edit_before_set.as_mut().unwrap().id = Some("E2".into());
    }
    let result = fixture.sync().await.unwrap();
    assert_eq!(
        result.downloaded, 1,
        "the new server activity must restore the folder instead of destroying it"
    );
    {
        let state = fixture.state.lock().unwrap();
        assert!(
            state.mailbox_set_destroys.is_empty(),
            "a failed precondition cannot authorize downstream destructive work"
        );
        assert!(
            state
                .mailboxes
                .iter()
                .any(|mailbox| mailbox.id == "MB-TRASH")
        );
        assert_eq!(state.state_mismatches, 1);
    }
    let second = queries::get_message_by_jmap_id(&fixture.conn, &"E2".into())
        .unwrap()
        .unwrap();
    assert_eq!(second.jmap_mailbox_id.as_ref(), "MB-TRASH");
    assert_eq!(second.flags, "F");
    assert!(
        fixture
            .temp
            .path()
            .join("Deleted Items/new")
            .join(format!("{}:2,F", second.maildir_id.unwrap()))
            .exists()
    );
    assert_eq!(fixture.state.lock().unwrap().blob_requests, 3);
    assert!(fixture.sync().await.unwrap().already_in_sync);
}
