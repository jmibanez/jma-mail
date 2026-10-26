//! Successful own pulls must not become invented MUA edits after ACK failure.

use super::*;
use crate::state::flag_sync;
use crate::sync::flag_repair::{self, FlagAuthority};

#[tokio::test]
async fn flag_pull_ack_failure_retains_ambiguity_when_a_replacement_pull_also_fails() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.local_change("X", false, true);
    fixture.sync().await.unwrap();
    fixture.server_change("F", true);
    fixture.conn.execute_batch("CREATE TRIGGER fail_pull_ack BEFORE UPDATE OF baseline ON message_flag_sync WHEN NEW.baseline='F' BEGIN SELECT RAISE(ABORT,'injected post-rename ACK failure'); END;").unwrap();
    assert!(fixture.sync().await.is_err());
    assert_eq!(fixture.snapshot().await["file_flags"], "FX");
    fixture
        .conn
        .execute_batch("DROP TRIGGER fail_pull_ack")
        .unwrap();
    fixture.server_change("S", true);
    let source = fixture.file();
    let held = fixture.temp.path().join("held-message");
    fixture.state.lock().unwrap().hide_on_get = Some((source.clone(), held.clone()));
    assert_eq!(fixture.sync().await.unwrap().retryable_local_actions, 1);
    std::fs::rename(held, source).unwrap();
    // Replacing a receipt must not erase the older ambiguous own-pull origin
    // of F. A later server return to the original baseline is not authority
    // to repush that F as a one-sided MUA edit, even under ServerWins.
    fixture.server_change("", true);
    fixture.reopen_db();
    let outcome = fixture.sync().await.unwrap();
    let snapshot = fixture.snapshot().await;
    assert_eq!(
        snapshot["server_flags"], "",
        "failed replacement pull erased older own-effect ambiguity: {snapshot}"
    );
    assert_eq!(snapshot["file_flags"], "FX");
    assert_eq!(snapshot["acknowledged_flags"], "");
    assert!(!outcome.already_in_sync && outcome.blocked_flag_actions == 1);
    let history = flag_sync::get(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    assert!(history.review && history.attempt.is_some());
    assert!(fixture.state.lock().unwrap().set_requests.is_empty());
    let client = Arc::new(
        jmap_client::client::Client::new()
            .credentials("test-token")
            .connect(&fixture._server.uri())
            .await
            .unwrap(),
    );
    let preview = flag_repair::inspect(
        &client,
        &fixture.conn,
        &fixture.config,
        &["E1".into()],
        FlagAuthority::Server,
    )
    .await
    .unwrap();
    assert_eq!(
        preview
            .apply(client, &fixture.conn, &fixture.config)
            .await
            .unwrap()
            .flag_updates,
        1
    );
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], "X");
    assert_eq!(snapshot["server_flags"], "");
    assert!(snapshot["flag_attempt"].is_null());
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_pull_ack_failure_can_finish_as_a_safe_fresh_pull() {
    let mut fixture = FlagFixture::new("", true, ConflictStrategy::ServerWins).await;
    fixture.server_change("F", true);
    fixture.conn.execute_batch("CREATE TRIGGER fail_pull_ack BEFORE UPDATE OF baseline ON message_flag_sync WHEN NEW.baseline='F' BEGIN SELECT RAISE(ABORT,'injected post-rename ACK failure'); END;").unwrap();
    assert!(fixture.sync().await.is_err());
    assert_eq!(fixture.snapshot().await["file_flags"], "F");
    fixture
        .conn
        .execute_batch("DROP TRIGGER fail_pull_ack")
        .unwrap();
    fixture.server_change("S", true);
    fixture.reopen_db();
    assert_eq!(fixture.sync().await.unwrap().flag_updates, 1);
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot["file_flags"], "S");
    assert_eq!(snapshot["server_flags"], "S");
    assert_eq!(snapshot["acknowledged_flags"], "S");
    assert!(snapshot["flag_attempt"].is_null());
    assert!(fixture.state.lock().unwrap().set_requests.is_empty());
    assert!(fixture.sync().await.unwrap().already_in_sync);
}

#[tokio::test]
async fn flag_pull_ack_failure_does_not_repush_our_suffix_over_a_later_remote_edit() {
    for (initial, pulled, strategy) in [
        ("", "F", ConflictStrategy::LocalWins),
        ("S", "FS", ConflictStrategy::ServerWins),
    ] {
        let mut fixture = FlagFixture::new(initial, true, strategy).await;
        fixture.local_change(&format!("{initial}X"), false, !initial.contains('S'));
        fixture.sync().await.unwrap();
        let original = fixture.snapshot().await;
        fixture.server_change(pulled, true);
        fixture.conn.execute_batch(&format!("CREATE TRIGGER fail_pull_ack BEFORE UPDATE OF baseline ON message_flag_sync WHEN NEW.baseline='{pulled}' BEGIN SELECT RAISE(ABORT,'injected post-rename ACK failure'); END;")).unwrap();
        assert!(fixture.sync().await.is_err());
        let snapshot = fixture.snapshot().await;
        assert_eq!(snapshot["file_flags"], format!("{pulled}X"));
        assert_eq!(snapshot["server_flags"], pulled);
        assert_eq!(snapshot["acknowledged_flags"], initial);
        assert!(!snapshot["flag_attempt"].is_null());
        fixture
            .conn
            .execute_batch("DROP TRIGGER fail_pull_ack")
            .unwrap();
        fixture.server_change("S", true);
        fixture.reopen_db();
        for _ in 0..2 {
            let outcome = fixture.sync().await.unwrap();
            let snapshot = fixture.snapshot().await;
            assert_eq!(
                snapshot["server_flags"], "S",
                "our unacknowledged pulled suffix is not an independent MUA edit: {snapshot}"
            );
            assert_eq!(snapshot["file_flags"], format!("{pulled}X"));
            assert_eq!(snapshot["acknowledged_flags"], initial);
            assert_eq!(outcome.flag_updates, 0);
            assert!(!outcome.already_in_sync && outcome.blocked_flag_actions == 1);
            let history = flag_sync::get(&fixture.conn, &"E1".into())
                .unwrap()
                .unwrap();
            assert!(history.review && history.attempt.is_some());
            assert!(fixture.state.lock().unwrap().set_requests.is_empty());
        }
        // Current values cannot establish who produced that suffix after the
        // interrupted effect. Preserve both views until explicit authority.
        let client = Arc::new(
            jmap_client::client::Client::new()
                .credentials("test-token")
                .connect(&fixture._server.uri())
                .await
                .unwrap(),
        );
        let preview = flag_repair::inspect(
            &client,
            &fixture.conn,
            &fixture.config,
            &["E1".into()],
            FlagAuthority::Server,
        )
        .await
        .unwrap();
        assert_eq!(
            preview
                .apply(client, &fixture.conn, &fixture.config)
                .await
                .unwrap()
                .flag_updates,
            1
        );
        let snapshot = fixture.snapshot().await;
        assert_eq!(snapshot["file_flags"], "SX");
        assert_eq!(snapshot["server_flags"], "S");
        assert_eq!(snapshot["acknowledged_flags"], "S");
        assert_eq!(snapshot["cached_keywords"], snapshot["server_keywords"]);
        assert_eq!(snapshot["cached_maildir_id"], original["cached_maildir_id"]);
        assert_eq!(snapshot["blob_requests"], 1);
        assert!(fixture.sync().await.unwrap().already_in_sync);
    }
}
