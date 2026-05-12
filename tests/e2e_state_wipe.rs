//! End-to-end check of the state-DB disposability invariant against
//! a real Stalwart container. Pulls a seeded inbox, deletes
//! `state.db` (plus its WAL/SHM siblings), re-pulls, and asserts the
//! second pull adopts the existing maildir files without re-
//! downloading anything -- and that the JMAP-id <-> maildir_id <->
//! Message-ID binding survives the wipe byte-for-byte.
//!
//! This is the test the wiremock suite can't write meaningfully: the
//! adoption path's correctness depends on the server returning a
//! `Message-ID` that matches what was on disk, which a hand-scripted
//! response tautologically asserts. The real Stalwart fixture
//! exercises the round-trip through `Email/import` (during the IMAP
//! APPEND) and `Email/get` (during the post-wipe pull), and pins the
//! invariant from DEVELOPMENT.md section "Idempotency model" that
//! lets users nuke `state.db` without losing local mail.
//!
//! Run locally:
//!     CC=/usr/bin/cc cargo test --test e2e_state_wipe -- --ignored --nocapture

mod common;

use jma_mail::config::{
    AccountConfig, Config, ConflictStrategy, FolderLayout, StateConfig, SyncConfig, WatchConfig,
};
use jma_mail::state::{db, queries};
use jma_mail::sync::engine::SyncEngine;
use std::collections::HashMap;
use std::path::Path;

#[tokio::test]
#[ignore = "requires Docker; run via cargo test -- --ignored"]
async fn state_wipe_re_pull_adopts_existing_files_by_message_id() {
    let fx = common::spawn_stalwart()
        .await
        .expect("spawn Stalwart fixture");
    common::seed_inbox(
        &fx,
        &[
            common::SeedMessage::simple("alice-test@example.com", "First", "Body one."),
            common::SeedMessage::simple("bob-test@example.com", "Second", "Body two."),
        ],
    )
    .await
    .expect("seed INBOX over IMAP");

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("state.db");
    let config = build_config(&fx, temp.path(), &db_path);

    // Phase 1: initial sync, snapshot the JMAP-id <-> maildir_id <->
    // Message-ID binding. Scope the connection so SQLite releases its
    // file handles before we nuke the DB below.
    let initial_snapshot = {
        let conn = db::open_or_recreate(&db_path).expect("open state DB");
        let outcome = SyncEngine::sync(&conn, &config, false)
            .await
            .expect("initial sync");
        assert_eq!(
            outcome.downloaded, 2,
            "both seeded messages must download on the initial sync"
        );
        assert_eq!(outcome.failed_remote_actions, 0);
        snapshot_inbox(&conn)
    };
    assert_eq!(initial_snapshot.len(), 2);

    // Wipe the state DB (and its WAL / SHM siblings -- leaving those
    // behind lets the next open replay the WAL and undo the
    // delete). The maildir files on disk stay untouched, which is
    // the whole point of the disposability invariant.
    for suffix in ["", "-wal", "-shm"] {
        let p = temp.path().join(format!("state.db{suffix}"));
        if p.exists() {
            std::fs::remove_file(&p).expect("remove state DB sibling");
        }
    }

    // Phase 2: re-sync against the wiped DB.
    let conn = db::open_or_recreate(&db_path).expect("open fresh state DB");
    let outcome = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("post-wipe sync");
    assert_eq!(
        outcome.downloaded, 0,
        "post-wipe sync must adopt existing files, not re-download"
    );
    assert_eq!(outcome.failed_remote_actions, 0);

    let rebound = snapshot_inbox(&conn);
    assert_eq!(
        rebound, initial_snapshot,
        "JMAP-id <-> maildir_id <-> Message-ID binding must survive a state wipe byte-for-byte"
    );
}

/// Map from JMAP email id to (maildir_id, message_id) for every
/// row in INBOX. Used to compare the binding before and after a
/// state-DB wipe.
fn snapshot_inbox(conn: &rusqlite::Connection) -> HashMap<String, (String, String)> {
    queries::get_messages_by_folder(conn, "INBOX")
        .expect("query message_map")
        .into_iter()
        .map(|r| {
            let maildir_id = r
                .maildir_id
                .as_ref()
                .map(|m| m.to_string())
                .expect("bound maildir_id");
            (
                r.jmap_email_id.to_string(),
                (maildir_id, r.message_id.to_string()),
            )
        })
        .collect()
}

fn build_config(fx: &common::JmapFixture, maildir_root: &Path, db_path: &Path) -> Config {
    Config {
        account: AccountConfig {
            email: fx.account_email.clone(),
            token: Some(fx.bearer.clone()),
            session_url: Some(fx.session_url.clone()),
        },
        sync: SyncConfig {
            maildir_path: maildir_root.to_string_lossy().into_owned(),
            mailboxes: vec!["INBOX".to_string()],
            conflict_strategy: ConflictStrategy::ServerWins,
            case_insensitive_match: false,
            folder_layout: FolderLayout::Fs,
            hierarchy_separator: '/',
            download_concurrency: 2,
            upload_concurrency: 2,
            retry_max_attempts: 1,
            retry_initial_backoff_ms: 1,
            retry_max_backoff_ms: 1,
        },
        state: StateConfig {
            db_path: Some(db_path.to_string_lossy().into_owned()),
        },
        watch: WatchConfig::default(),
        rename_rules: Vec::new(),
        compiled_rename_rules: Vec::new(),
    }
}
