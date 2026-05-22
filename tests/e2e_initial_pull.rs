//! End-to-end initial-pull against a real Stalwart Mail container.
//! Seeds N messages over IMAP APPEND, points a `jma_mail::Config` at
//! the container's JMAP listener, runs `SyncEngine::sync`, and
//! checks that the maildir, `message_map`, and `jmap_state` cursor
//! all reflect the seeded state. A second sync verifies steady-state
//! idempotence (zero downloads, no spurious DB churn).
//!
//! Marked `#[ignore]` so the default `cargo test` invocation (and
//! the existing `ci.yaml` build job) keeps working on machines
//! without Docker. The dedicated E2E workflow runs `--ignored` to
//! exercise it.
//!
//! Run locally:
//!     CC=/usr/bin/cc cargo test --test e2e_initial_pull -- --ignored --nocapture

mod common;

use jma_mail::config::{
    AccountConfig, AllowDestructiveFolderSync, Config, ConflictStrategy, FolderLayout, StateConfig,
    SyncConfig, WatchConfig,
};
use jma_mail::state::{db, queries};
use jma_mail::sync::engine::SyncEngine;

#[tokio::test]
#[ignore = "requires Docker; run via cargo test -- --ignored"]
async fn initial_pull_downloads_seeded_inbox_then_steady_state_is_noop() {
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
    let conn = db::open_or_recreate(&db_path).expect("open state DB");

    let config = Config {
        account: AccountConfig {
            email: fx.account_email.clone(),
            token: Some(fx.bearer.clone()),
            session_url: Some(fx.session_url.clone()),
        },
        sync: SyncConfig {
            maildir_path: temp.path().to_string_lossy().into_owned(),
            // Constrain to INBOX so the seeded count is what gets
            // pulled. Stalwart provisions default Sent/Drafts/Junk/
            // Trash folders too; whole-account sync belongs in its
            // own test.
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
            allow_destructive_folder_sync: AllowDestructiveFolderSync::None,
        },
        state: StateConfig {
            db_path: Some(db_path.to_string_lossy().into_owned()),
        },
        watch: WatchConfig::default(),
        rename_rules: Vec::new(),
        compiled_rename_rules: Vec::new(),
    };

    let outcome = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("initial sync");
    assert_eq!(outcome.downloaded, 2, "both seeded messages must download");
    assert_eq!(outcome.failed_remote_actions, 0);

    let inbox_cur = temp.path().join("INBOX").join("cur");
    let inbox_new = temp.path().join("INBOX").join("new");
    let on_disk: usize = std::fs::read_dir(&inbox_cur)
        .expect("INBOX/cur must exist after a successful pull")
        .chain(std::fs::read_dir(&inbox_new).expect("INBOX/new must exist after a successful pull"))
        .filter_map(|e| e.ok())
        .count();
    assert_eq!(on_disk, 2, "both seeded messages must land in INBOX");

    assert!(
        queries::has_message_map_rows(&conn).expect("query DB"),
        "message_map must hold rows after a successful pull"
    );

    // Cursor advancement is what routes the next cycle through the
    // Email/changes delta path; without this assertion a regression
    // that downloads correctly but never persists the cursor would
    // still pass the steady-state check below (via re-download
    // suppression) and slip through silently. Stalwart assigns the
    // account id at provisioning time, so the test inspects every
    // jmap_state row rather than hardcoding one.
    let rows = queries::list_jmap_state_rows(&conn).expect("query jmap_state");
    assert!(
        rows.iter().any(|r| r.entity_type == "Email"),
        "Email cursor must be persisted after the initial pull"
    );

    let steady = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("steady-state sync");
    assert_eq!(
        steady.downloaded, 0,
        "second sync must not re-download already-known messages"
    );
    assert_eq!(steady.failed_remote_actions, 0);
}
