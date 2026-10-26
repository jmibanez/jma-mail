//! Combined flag-history and PR #21 identity-preservation regressions.

use super::*;
use crate::state::flag_sync;
use crate::sync::engine::ScanScope;
use crate::sync::plan::SyncDirection;

#[tokio::test]
async fn flag_import_preserved_copy_cannot_replace_a_live_binding_or_its_flags() {
    for path_cycle in [false, true] {
        for content_variant in [false, true] {
            for strategy in [ConflictStrategy::ServerWins, ConflictStrategy::LocalWins] {
                let mut fixture = FlagFixture::new("FS", true, strategy).await;
                let original = queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
                    .unwrap()
                    .unwrap();
                let original_path = fixture.file();
                let original_body = std::fs::read(&original_path).unwrap();
                let copy = fixture
                    .temp
                    .path()
                    .join("INBOX/cur/000-preserved-copy:2,SX");
                let mut body = original_body.clone();
                if content_variant {
                    body.extend_from_slice(b"another retained revision\r\n");
                }
                std::fs::write(&copy, &body).unwrap();
                std::fs::File::open(&copy)
                    .unwrap()
                    .set_modified(
                        std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1),
                    )
                    .unwrap();
                let engine = SyncEngine::connect(&fixture.conn, &fixture.config)
                    .await
                    .unwrap();
                let scope = if path_cycle {
                    ScanScope::Paths(vec![copy.clone()])
                } else {
                    ScanScope::Full
                };
                let outcome = engine.run(false, SyncDirection::Both, scope).await.unwrap();
                let record = queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    record.maildir_id, original.maildir_id,
                    "preserving a duplicate must not let it steal a live bound identity"
                );
                let remote = crate::jmap::email::get_by_ids(&fixture.verifier, &["E1".into()])
                    .await
                    .unwrap();
                assert_eq!(
                    keywords_to_flags(&remote[0].keywords),
                    "FS",
                    "an untracked copy's suffix is not a bound MUA flag edit"
                );
                assert_eq!(
                    (
                        outcome.flag_updates,
                        outcome.downloaded,
                        outcome.uploaded,
                        outcome.deleted,
                        outcome.moved
                    ),
                    (0, 0, 0, 0, 0)
                );
                assert!(original_path.is_file() && copy.is_file());
                assert_eq!(std::fs::read(&original_path).unwrap(), original_body);
                assert_eq!(std::fs::read(&copy).unwrap(), body);
                assert_eq!(
                    queries::get_local_state(&fixture.conn, original.maildir_id.as_ref().unwrap())
                        .unwrap()
                        .unwrap()
                        .1,
                    "FS"
                );
                let history = flag_sync::get(&fixture.conn, &"E1".into())
                    .unwrap()
                    .unwrap();
                assert_eq!(history.baseline.as_deref(), Some("FS"));
                assert!(history.attempt.is_none());
                if path_cycle {
                    assert!(
                        outcome.retry_scope_checked,
                        "a path-only new identity needs complete planning/indexing"
                    );
                }
                drop(engine);
                fixture.reopen_db();
                assert_eq!(fixture.sync().await.unwrap().flag_updates, 0);
                assert_eq!(
                    queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
                        .unwrap()
                        .unwrap()
                        .maildir_id,
                    original.maildir_id
                );
                assert!(fixture.state.lock().unwrap().set_requests.is_empty());
            }
        }
    }
}

#[tokio::test]
async fn flag_import_missing_anchor_does_not_choose_between_content_variants() {
    let fixture = FlagFixture::new("FS", true, ConflictStrategy::LocalWins).await;
    let original = queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
        .unwrap()
        .unwrap();
    let body = std::fs::read(fixture.file()).unwrap();
    std::fs::remove_file(fixture.file()).unwrap();
    let mut copies = Vec::new();
    for (id, flags, suffix) in [
        ("variant-a", "SX", b"revision A\r\n"),
        ("variant-b", "FY", b"revision B\r\n"),
    ] {
        let path = fixture
            .temp
            .path()
            .join("INBOX/cur")
            .join(format!("{id}:2,{flags}"));
        let mut revision = body.clone();
        revision.extend_from_slice(suffix);
        std::fs::write(&path, revision).unwrap();
        copies.push(path);
    }
    for _ in 0..2 {
        let outcome = fixture.sync().await.unwrap();
        assert!(outcome.deferred_actions > 0 && !outcome.already_in_sync);
        assert_eq!(
            (
                outcome.flag_updates,
                outcome.uploaded,
                outcome.downloaded,
                outcome.deleted,
                outcome.moved
            ),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(
            queries::get_message_by_jmap_id(&fixture.conn, &"E1".into())
                .unwrap()
                .unwrap()
                .maildir_id,
            original.maildir_id
        );
        let remote = crate::jmap::email::get_by_ids(&fixture.verifier, &["E1".into()])
            .await
            .unwrap();
        assert_eq!(keywords_to_flags(&remote[0].keywords), "FS");
        assert!(copies.iter().all(|path| path.is_file()));
    }
    assert!(fixture.state.lock().unwrap().set_requests.is_empty());
}

#[tokio::test]
async fn flag_import_remote_dedupe_preserves_every_locally_bound_copy() {
    let mut fixture = FlagFixture::new("S", true, ConflictStrategy::ServerWins).await;
    std::fs::remove_file(fixture.file()).unwrap();
    fixture.reset_db(); // Deliberate cold-start setup, not restart recovery.
    {
        let mut state = fixture.state.lock().unwrap();
        let mut second = state.emails[0].clone();
        second.id = "E2".into();
        second.blob_id = "B2".into();
        second.thread_id = "T2".into();
        let body = state.blobs["B1"].clone();
        state.blobs.insert("B2".into(), body);
        state.emails.push(second);
        advance_email_state(&mut state);
    }
    assert_eq!(fixture.sync().await.unwrap().downloaded, 2);
    let records =
        queries::get_messages_by_jmap_mailbox_id(&fixture.conn, &"MB-INBOX".into()).unwrap();
    assert_eq!(
        records.len(),
        2,
        "distinct JMAP IDs can legitimately have distinct bound Maildir IDs with one Message-ID"
    );
    assert_ne!(records[0].maildir_id, records[1].maildir_id);
    let (plan, outcome) = crate::janitor::remotededupe::run(
        &fixture.verifier,
        &fixture.conn,
        &[("INBOX".into(), "MB-INBOX".into())],
        false,
    )
    .await
    .unwrap();
    assert!(plan.groups.is_empty() && plan.destroy_count() == 0 && outcome.is_none());
    assert_eq!(plan.skipped.len(), 1);
    assert_eq!(
        plan.skipped[0].reason,
        crate::janitor::remotededupe::SkipReason::MultipleLocalBindings
    );
    assert_eq!(plan.skipped[0].members.len(), 2);
    assert_eq!(queries::count_messages(&fixture.conn).unwrap(), 2);
    assert_eq!(
        crate::jmap::email::get_by_ids(&fixture.verifier, &["E1".into(), "E2".into()])
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(fixture.state.lock().unwrap().set_requests.is_empty());
    for record in records {
        assert!(
            fixture
                .temp
                .path()
                .join("INBOX/cur")
                .join(format!("{}:2,S", record.maildir_id.unwrap()))
                .is_file()
        );
    }
}
