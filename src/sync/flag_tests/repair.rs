//! Surgical authority repair: stateful server, real files and SAME-DB recovery.

use super::*;
use crate::state::flag_sync::{self, FlagAttempt, FlagSyncState};
use crate::sync::flag_repair::{self, FlagAuthority};

async fn client(fixture: &FlagFixture) -> Arc<jmap_client::client::Client> {
    Arc::new(
        jmap_client::client::Client::new()
            .credentials("test-token")
            .connect(&fixture._server.uri())
            .await
            .unwrap(),
    )
}

async fn preview(fixture: &FlagFixture, authority: FlagAuthority) -> flag_repair::FlagRepair {
    flag_repair::inspect(
        client(fixture).await.as_ref(),
        &fixture.conn,
        &fixture.config,
        &["E1".into()],
        authority,
    )
    .await
    .unwrap()
}

async fn apply(
    fixture: &FlagFixture,
    authority: FlagAuthority,
) -> anyhow::Result<crate::sync::engine::SyncOutcome> {
    let client = client(fixture).await;
    flag_repair::inspect(
        &client,
        &fixture.conn,
        &fixture.config,
        &["E1".into()],
        authority,
    )
    .await?
    .apply(client, &fixture.conn, &fixture.config)
    .await
}

fn review(fixture: &FlagFixture, baseline: Option<&str>) {
    flag_sync::store(
        &fixture.conn,
        &"E1".into(),
        &FlagSyncState {
            baseline: baseline.map(str::to_owned),
            review: true,
            blocked: true,
            error: Some("legacy disagreement".into()),
            ..Default::default()
        },
    )
    .unwrap();
}

fn changes(fixture: &FlagFixture) -> u64 {
    fixture
        .conn
        .query_row("SELECT total_changes()", [], |row| row.get(0))
        .unwrap()
}

fn assert_surgical(fixture: &FlagFixture) {
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.blob_requests, 1);
    assert!(
        state.mailbox_set_creates.is_empty()
            && state.mailbox_set_updates.is_empty()
            && state.mailbox_set_destroys.is_empty()
    );
    for request in &state.set_requests {
        for call in request["methodCalls"].as_array().unwrap() {
            assert_eq!(call[0], "Email/set");
            let args = &call[1];
            assert!(args["ifInState"].is_string(), "{request}");
            assert!(
                args.get("destroy").is_none() && args.get("create").is_none(),
                "{request}"
            );
            for (id, patch) in args["update"].as_object().unwrap() {
                assert_eq!(id, "E1");
                assert_eq!(patch.as_object().unwrap().len(), 6);
                assert!(
                    patch
                        .as_object()
                        .unwrap()
                        .keys()
                        .all(|key| key.starts_with("keywords/")),
                    "{request}"
                );
            }
        }
    }
}

#[tokio::test]
async fn flag_repair_explicit_authority_preserves_identity_full_metadata_and_empty_sets() {
    for (authority, local, remote, expected) in [
        (FlagAuthority::Local, "SX", "F", "SX"),
        (FlagAuthority::Local, "X", "F", "X"),
        (FlagAuthority::Server, "SX", "F", "FX"),
        (FlagAuthority::Server, "SX", "", "X"),
    ] {
        for baseline in [None, Some("S")] {
            let fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
            let original = fixture.snapshot().await;
            let body = std::fs::read(fixture.file()).unwrap();
            #[cfg(unix)]
            let inode = {
                use std::os::unix::fs::MetadataExt;
                fixture.file().metadata().unwrap().ino()
            };
            fixture.local_change(local, false, false);
            fixture.server_change(remote, true);
            fixture.state.lock().unwrap().emails[0]
                .keywords
                .push("fresh-custom".into());
            review(&fixture, baseline);
            let cursor = queries::get_jmap_state(&fixture.conn, ACCOUNT_ID, "Email").unwrap();
            assert_eq!(apply(&fixture, authority).await.unwrap().flag_updates, 1);
            let snap = fixture.snapshot().await;
            for key in ["file_flags", "local_observed_flags"] {
                assert_eq!(snap[key], expected, "{snap}");
            }
            for key in ["server_flags", "cached_flags", "acknowledged_flags"] {
                assert_eq!(snap[key], flag_sync::mapped_flags(expected), "{snap}");
            }
            assert_eq!(snap["cached_keywords"], snap["server_keywords"]);
            assert_eq!(snap["server_keywords"]["keep-me"], true);
            assert_eq!(snap["server_keywords"]["fresh-custom"], true);
            for key in [
                "server_email_id",
                "server_blob_id",
                "server_thread_id",
                "cached_maildir_id",
                "cached_mailbox_id",
                "server_mailboxes",
            ] {
                assert_eq!(snap[key], original[key]);
            }
            assert_eq!(
                queries::get_jmap_state(&fixture.conn, ACCOUNT_ID, "Email").unwrap(),
                cursor
            );
            assert_eq!(std::fs::read(fixture.file()).unwrap(), body);
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                assert_eq!(fixture.file().metadata().unwrap().ino(), inode);
            }
            assert_surgical(&fixture);
            assert!(fixture.sync().await.unwrap().already_in_sync);
            assert_eq!(fixture.snapshot().await["file_flags"], expected);
        }
    }
}

#[tokio::test]
async fn flag_repair_preview_is_select_only_and_shows_legacy_and_retained_receipts() {
    let fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", false, false);
    fixture.server_change("F", true);
    for history in [
        None,
        Some(FlagSyncState {
            baseline: Some("S".into()),
            review: true,
            blocked: true,
            attempt: Some(FlagAttempt {
                source_flags: "SX".into(),
                target_flags: "S".into(),
                source_maildir_id: None,
                source_folder: None,
            }),
            error: Some("needs review".into()),
            ..Default::default()
        }),
    ] {
        if let Some(history) = &history {
            flag_sync::store(&fixture.conn, &"E1".into(), history).unwrap();
        } else {
            fixture
                .conn
                .execute("DELETE FROM message_flag_sync", [])
                .unwrap();
        }
        let before = changes(&fixture);
        fixture
            .conn
            .pragma_update(None, "query_only", true)
            .unwrap();
        let text = preview(&fixture, FlagAuthority::Local).await.to_string();
        assert!(
            text.contains("E1")
                && text.contains("audit@example.com")
                && text.contains("SX")
                && text.contains("keep-me"),
            "{text}"
        );
        assert!(
            text.contains(if history.is_none() {
                "legacy/unverified; baseline=NULL"
            } else {
                "review=true; blocked=true; receipt={"
            }),
            "{text}"
        );
        assert_eq!(changes(&fixture), before);
        assert_eq!(
            flag_sync::get(&fixture.conn, &"E1".into()).unwrap(),
            history
        );
        fixture
            .conn
            .pragma_update(None, "query_only", false)
            .unwrap();
    }
    assert!(fixture.state.lock().unwrap().set_requests.is_empty());
    assert_surgical(&fixture);
}

#[tokio::test]
async fn flag_repair_noop_acknowledges_fresh_agreement_without_io_and_preserves_new() {
    let fixture = FlagFixture::new("F", true, ConflictStrategy::LocalWins).await;
    fixture.local_change("FX", false, true);
    review(&fixture, None);
    assert_eq!(
        apply(&fixture, FlagAuthority::Local)
            .await
            .unwrap()
            .flag_updates,
        0
    );
    let snap = fixture.snapshot().await;
    assert_eq!(snap["file_flags"], "FX");
    assert_eq!(snap["subdir"], "new");
    assert_eq!(snap["acknowledged_flags"], "F");
    assert!(snap["flag_attempt"].is_null());
    assert!(
        !flag_sync::get(&fixture.conn, &"E1".into())
            .unwrap()
            .unwrap()
            .review
    );
    assert_surgical(&fixture);
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_repair_rejected_explicit_push_requires_rerun_not_opposite_global_policy() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", false, false);
    fixture.server_change("F", true);
    review(&fixture, Some(""));
    fixture.state.lock().unwrap().reject_next_update = 1;
    let outcome = apply(&fixture, FlagAuthority::Local).await.unwrap();
    assert_eq!(outcome.failed_remote_actions, 1);
    let history = flag_sync::get(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    assert_eq!(history.baseline.as_deref(), Some(""));
    assert!(history.review && history.attempt.is_none());
    fixture.reopen_db();
    let sets = fixture.state.lock().unwrap().set_requests.len();
    let ordinary = fixture.sync().await.unwrap();
    assert!(!ordinary.already_in_sync && ordinary.blocked_flag_actions > 0);
    assert_eq!(fixture.snapshot().await["file_flags"], "SX");
    assert_eq!(fixture.state.lock().unwrap().set_requests.len(), sets);
    assert_eq!(
        apply(&fixture, FlagAuthority::Local)
            .await
            .unwrap()
            .flag_updates,
        1
    );
    assert_eq!(fixture.snapshot().await["server_flags"], "S");
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_repair_failed_explicit_pull_keeps_null_baseline_review_across_reopen() {
    let mut fixture = FlagFixture::new("S", true, ConflictStrategy::LocalWins).await;
    fixture.local_change("SX", false, false);
    fixture.server_change("F", true);
    review(&fixture, None);
    let preview = preview(&fixture, FlagAuthority::Server).await;
    let source = fixture.file();
    let held = fixture.temp.path().join("held-message");
    // apply validates first, then this empty-id Email/get hides the file just
    // before the actual snapshot rename. It is an FS failure, not setup error.
    fixture.state.lock().unwrap().hide_on_get = Some((source.clone(), held.clone()));
    assert_eq!(
        preview
            .apply(client(&fixture).await, &fixture.conn, &fixture.config)
            .await
            .unwrap()
            .retryable_local_actions,
        1
    );
    let history = flag_sync::get(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    assert!(history.baseline.is_none() && history.review && history.attempt.is_some());
    std::fs::rename(held, source).unwrap();
    fixture.reopen_db();
    let ordinary = fixture.sync().await.unwrap();
    assert!(!ordinary.already_in_sync && ordinary.blocked_flag_actions > 0);
    assert_eq!(fixture.snapshot().await["server_flags"], "F");
    assert_eq!(fixture.snapshot().await["file_flags"], "SX");
    assert_eq!(
        apply(&fixture, FlagAuthority::Server)
            .await
            .unwrap()
            .flag_updates,
        1
    );
    assert!(fixture.sync().await.unwrap().already_in_sync);
    assert_surgical(&fixture);
}

#[tokio::test]
async fn flag_repair_ack_failure_and_later_remote_edit_cannot_turn_our_pull_into_mua_intent() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::LocalWins).await;
    fixture.local_change("SX", false, false);
    fixture.server_change("F", true);
    review(&fixture, Some(""));
    fixture.conn.execute_batch("CREATE TRIGGER fail_repair_ack BEFORE UPDATE OF baseline ON message_flag_sync WHEN NEW.baseline='F' AND NEW.review=0 BEGIN SELECT RAISE(ABORT,'injected ACK failure'); END;").unwrap();
    assert!(apply(&fixture, FlagAuthority::Server).await.is_err());
    assert_eq!(fixture.snapshot().await["file_flags"], "FX");
    let history = flag_sync::get(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    assert_eq!(history.baseline.as_deref(), Some(""));
    assert!(history.review && history.attempt.is_some());
    fixture
        .conn
        .execute_batch("DROP TRIGGER fail_repair_ack;")
        .unwrap();
    fixture.server_change("T", true);
    fixture.reopen_db();
    assert!(!fixture.sync().await.unwrap().already_in_sync);
    assert_eq!(fixture.snapshot().await["server_flags"], "T");
    assert!(fixture.state.lock().unwrap().set_requests.is_empty());
    assert_eq!(
        apply(&fixture, FlagAuthority::Server)
            .await
            .unwrap()
            .flag_updates,
        1
    );
    assert_eq!(fixture.snapshot().await["file_flags"], "TX");
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_repair_state_mismatch_retains_review_and_does_not_replay_a_stale_target() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", false, false);
    review(&fixture, Some(""));
    {
        let mut state = fixture.state.lock().unwrap();
        state.edit_before_set = Some(RemoteEdit {
            id: Some("E1".into()),
            flags: "F".into(),
            keyword_prefix: "raced",
        });
        state.edit_before_set_at = 1;
        state.edit_before_set_remaining = 1;
    }
    let outcome = apply(&fixture, FlagAuthority::Local).await.unwrap();
    assert!(outcome.replan_required);
    assert_eq!(fixture.snapshot().await["server_flags"], "F");
    let history = flag_sync::get(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    assert!(history.review && history.attempt.is_none());
    fixture.reopen_db();
    assert!(!fixture.sync().await.unwrap().already_in_sync);
    assert_eq!(fixture.snapshot().await["file_flags"], "SX");
    assert_eq!(
        apply(&fixture, FlagAuthority::Local)
            .await
            .unwrap()
            .flag_updates,
        1
    );
    assert!(fixture.sync().await.unwrap().already_in_sync);
    assert_surgical(&fixture);
}

#[tokio::test]
async fn flag_repair_stale_previews_fail_before_any_observation_or_effect() {
    for change in [
        "local",
        "replacement",
        "remote",
        "history",
        "selection",
        "folder",
    ] {
        let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
        fixture.server_change("F", true);
        review(&fixture, None);
        let plan = preview(&fixture, FlagAuthority::Server).await;
        match change {
            "local" => fixture.local_change("SX", false, false),
            "replacement" => {
                let source = fixture.file();
                let bytes = std::fs::read(&source).unwrap();
                std::fs::rename(&source, fixture.temp.path().join("old-inode")).unwrap();
                std::fs::write(source, bytes).unwrap();
            }
            "remote" => fixture.server_change("T", true),
            "history" => review(&fixture, Some("F")),
            "selection" => fixture.config.sync.mailboxes = vec!["Deleted Items".into()],
            "folder" => {
                std::fs::rename(
                    fixture.temp.path().join("INBOX"),
                    fixture.temp.path().join("moved-INBOX"),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let before = changes(&fixture);
        assert!(
            plan.apply(client(&fixture).await, &fixture.conn, &fixture.config)
                .await
                .is_err(),
            "{change}"
        );
        assert_eq!(changes(&fixture), before, "{change}");
        if change == "folder" {
            assert!(!fixture.temp.path().join("INBOX").exists());
        }
        assert_surgical(&fixture);
    }
}

#[tokio::test]
async fn flag_repair_refuses_missing_duplicate_moved_or_unsupported_bindings() {
    for change in [
        "missing",
        "duplicate",
        "moved",
        "new-id",
        "message-id",
        "remote-move",
        "multi",
        "remote-id",
        "sentinel",
        "partial",
        "corrupt",
        "excluded",
    ] {
        let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
        fixture.server_change("F", true);
        match change {
            "missing" => std::fs::remove_file(fixture.file()).unwrap(),
            "duplicate" => {
                std::fs::copy(
                    fixture.file(),
                    fixture.temp.path().join("Deleted Items/cur/duplicate:2,S"),
                )
                .unwrap();
            }
            "moved" => fixture.local_change("S", true, false),
            "new-id" => std::fs::rename(
                fixture.file(),
                fixture.temp.path().join("INBOX/cur/new-id:2,S"),
            )
            .unwrap(),
            "message-id" => {
                std::fs::write(fixture.file(), b"Message-ID: <other@x>\r\n\r\nbody").unwrap()
            }
            "remote-move" => {
                fixture.state.lock().unwrap().emails[0].mailbox_ids = vec!["MB-TRASH".into()]
            }
            "multi" => fixture.state.lock().unwrap().emails[0]
                .mailbox_ids
                .push("MB-TRASH".into()),
            "remote-id" => {
                fixture.state.lock().unwrap().emails[0].message_id = Some("other@x".into())
            }
            "sentinel" => {
                std::fs::remove_file(fixture.temp.path().join("INBOX/.jma.mapping")).unwrap()
            }
            "partial" => std::fs::remove_dir(fixture.temp.path().join("INBOX/new")).unwrap(),
            "corrupt" => {
                fixture
                    .conn
                    .execute("UPDATE message_flag_sync SET attempt='invalid'", [])
                    .unwrap();
            }
            "excluded" => fixture.config.sync.mailboxes = vec!["Deleted Items".into()],
            _ => unreachable!(),
        }
        let before = changes(&fixture);
        assert!(
            flag_repair::inspect(
                client(&fixture).await.as_ref(),
                &fixture.conn,
                &fixture.config,
                &["E1".into()],
                FlagAuthority::Local
            )
            .await
            .is_err(),
            "{change}"
        );
        assert_eq!(changes(&fixture), before);
        assert_surgical(&fixture);
        if change == "partial" {
            assert!(!fixture.temp.path().join("INBOX/new").exists());
        }
    }
}

#[tokio::test]
async fn flag_repair_failed_pull_never_recreates_a_vanished_mailbox() {
    let fixture = FlagFixture::new("S", true, ConflictStrategy::LocalWins).await;
    fixture.server_change("F", true);
    let plan = preview(&fixture, FlagAuthority::Server).await;
    let folder = fixture.temp.path().join("INBOX");
    let held = fixture.temp.path().join("held-INBOX");
    fixture.state.lock().unwrap().hide_on_get = Some((folder.clone(), held.clone()));
    let outcome = plan
        .apply(client(&fixture).await, &fixture.conn, &fixture.config)
        .await
        .unwrap();
    assert_eq!(outcome.retryable_local_actions, 1);
    assert!(
        !folder.exists(),
        "A flag-only rename must not create a missing mailbox"
    );
    assert!(held.join("cur").is_dir());
    assert!(
        flag_sync::get(&fixture.conn, &"E1".into())
            .unwrap()
            .unwrap()
            .review
    );
    assert_surgical(&fixture);
}

#[tokio::test]
async fn flag_repair_legacy_missing_history_is_repaired_only_by_explicit_authority() {
    for authority in [FlagAuthority::Local, FlagAuthority::Server] {
        let fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
        fixture.local_change("SX", false, false);
        fixture.server_change("F", true);
        fixture
            .conn
            .execute("DELETE FROM message_flag_sync", [])
            .unwrap();
        assert!(!fixture.sync().await.unwrap().already_in_sync);
        assert_eq!(fixture.snapshot().await["file_flags"], "SX");
        assert_eq!(apply(&fixture, authority).await.unwrap().flag_updates, 1);
        let snap = fixture.snapshot().await;
        let expected = if authority == FlagAuthority::Local {
            "S"
        } else {
            "F"
        };
        assert_eq!(snap["acknowledged_flags"], expected);
        assert_eq!(snap["server_flags"], expected);
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}

#[tokio::test]
async fn flag_repair_push_ack_failure_preserves_newer_mua_edit_across_reopen() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", false, false);
    fixture.server_change("F", true);
    review(&fixture, None);
    let source = fixture.file();
    let newer = source.with_file_name(
        source
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .replace(":2,SX", ":2,TX"),
    );
    fixture.state.lock().unwrap().rename_after_set = Some((source, newer));
    fixture.conn.execute_batch("CREATE TRIGGER fail_repair_ack BEFORE UPDATE OF baseline ON message_flag_sync WHEN NEW.baseline='S' AND NEW.review=0 BEGIN SELECT RAISE(ABORT,'injected ACK failure'); END;").unwrap();
    assert!(apply(&fixture, FlagAuthority::Local).await.is_err());
    assert_eq!(fixture.snapshot().await["server_flags"], "S");
    let history = flag_sync::get(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    assert!(history.baseline.is_none() && history.review && history.attempt.is_some());
    fixture
        .conn
        .execute_batch("DROP TRIGGER fail_repair_ack;")
        .unwrap();
    fixture.reopen_db();
    assert!(!fixture.sync().await.unwrap().already_in_sync);
    assert_eq!(fixture.snapshot().await["file_flags"], "TX");
    assert_eq!(
        apply(&fixture, FlagAuthority::Local)
            .await
            .unwrap()
            .flag_updates,
        1
    );
    assert_eq!(fixture.snapshot().await["server_flags"], "T");
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_repair_does_not_run_unrelated_body_or_placement_work() {
    let fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("SX", false, false);
    fixture.server_change("F", true);
    let unrelated = fixture.temp.path().join("Deleted Items/cur/local-only:2,S");
    std::fs::write(&unrelated, b"Message-ID: <local-only@x>\r\n\r\nbody").unwrap();
    fixture.state.lock().unwrap().emails.push(RemoteEmail {
        id: "E2".into(),
        blob_id: "B2".into(),
        thread_id: "T2".into(),
        mailbox_ids: vec!["MB-INBOX".into()],
        keywords: vec!["$seen".into()],
        message_id: Some("server-only@x".into()),
    });
    assert_eq!(
        apply(&fixture, FlagAuthority::Local)
            .await
            .unwrap()
            .flag_updates,
        1
    );
    assert!(
        queries::get_message_by_jmap_id(&fixture.conn, &"E2".into())
            .unwrap()
            .is_none()
    );
    assert!(unrelated.is_file());
    assert_eq!(queries::count_messages(&fixture.conn).unwrap(), 1);
    assert_surgical(&fixture);
}
