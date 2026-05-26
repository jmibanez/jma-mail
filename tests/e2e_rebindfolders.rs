//! End-to-end rebindfolders against a real Stalwart Mail container.
//!
//! The wiremock test in `tests/engine_jmap.rs` proves we send what
//! we think we send. This e2e is intended to prove the *server*
//! interprets what we send the way we expect -- specifically:
//!
//! - That the `header` filter with a Message-ID value matches
//!   against the bracketed form stored in the server's index (the
//!   RFC 8621 4.4.1 substring rule the algorithm builds on).
//! - That `Filter::or` over multiple `header` conditions
//!   round-trips through jmap-client's JSON encoding into something
//!   the server's query engine accepts and dispatches correctly.
//! - That `Email/get` returning `mailboxIds` for each matched id
//!   gives us the membership map the intersection step needs.
//! - That a fresh sentinel written by `rebindfolders::apply`
//!   round-trips through subsequent reads with the bound mailbox
//!   id intact.
//!
//! Setup:
//!  1. Spawn Stalwart, seed N messages into INBOX over IMAP APPEND.
//!  2. Run `SyncEngine::sync` -- INBOX downloads, sentinel written,
//!     mailbox_map populated. Capture the bound INBOX `jmap_
//!     mailbox_id` to compare against.
//!  3. Simulate sentinel loss + state-DB nuke: remove the
//!     `.jma.mapping` file, drop the entire state DB.
//!  4. Run `rebindfolders::run` against the fixture's live JMAP
//!     endpoint.
//!  5. Assert exactly one rebind candidate, pointing at INBOX,
//!     binding to the same mailbox id captured in step 2.
//!
//! Marked `#[ignore]` so the default `cargo test` invocation
//! (and the existing `ci.yaml` build job) keeps working on machines
//! without Docker. The dedicated E2E workflow runs `--ignored` to
//! exercise it.
//!
//! Run locally:
//!     CC=/usr/bin/cc cargo test --test e2e_rebindfolders -- --ignored --nocapture

mod common;

use jma_mail::config::{
    AccountConfig, Config, ConflictStrategy, FolderLayout, StateConfig, SyncConfig, WatchConfig,
};
use jma_mail::janitor::rebindfolders;
use jma_mail::jmap::session;
use jma_mail::maildir_ops::sentinel;
use jma_mail::state::{db, queries};
use jma_mail::sync::engine::SyncEngine;

#[tokio::test]
#[ignore = "requires Docker; run via cargo test -- --ignored"]
async fn rebindfolders_rebinds_inbox_after_db_nuke_and_sentinel_loss() {
    let fx = common::spawn_stalwart()
        .await
        .expect("spawn Stalwart fixture");

    common::seed_inbox(
        &fx,
        &[
            common::SeedMessage::simple("alice-test@example.com", "First", "Body one."),
            common::SeedMessage::simple("bob-test@example.com", "Second", "Body two."),
            common::SeedMessage::simple("carol-test@example.com", "Third", "Body three."),
        ],
    )
    .await
    .expect("seed INBOX over IMAP");

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("state.db");

    let config = Config {
        account: AccountConfig {
            email: fx.account_email.clone(),
            token: Some(fx.bearer.clone()),
            session_url: Some(fx.session_url.clone()),
        },
        sync: SyncConfig {
            maildir_path: temp.path().to_string_lossy().into_owned(),
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
    };

    // Step 1: initial pull seeds the maildir, writes the sentinel,
    // and populates mailbox_map. Capture the INBOX binding so we can
    // assert rebindfolders identifies the same mailbox.
    {
        let conn = db::open_or_recreate(&db_path).expect("open state DB");
        let outcome = SyncEngine::sync(&conn, &config, false)
            .await
            .expect("initial sync");
        assert_eq!(outcome.downloaded, 3, "all seeded messages download");
    }

    let inbox_path = temp.path().join("INBOX");
    assert!(inbox_path.join("cur").is_dir(), "INBOX/cur present");

    let pre_sentinel = sentinel::read(&inbox_path)
        .expect("read INBOX sentinel")
        .expect("sentinel written by sync");
    let expected_inbox_id = pre_sentinel.jmap_mailbox_id.clone();
    assert_eq!(
        pre_sentinel.server_name, "Inbox",
        "Stalwart's leaf name for the inbox role"
    );

    // Step 2: simulate sentinel loss + DB nuke. Removing the
    // sentinel makes the folder look orphaned to walk_for_orphans;
    // dropping the state DB keeps the next resolve_mailboxes from
    // short-circuiting the rebindfolders probe.
    sentinel::remove(&inbox_path).expect("remove INBOX sentinel");
    assert!(
        sentinel::read(&inbox_path)
            .expect("read after remove")
            .is_none(),
        "sentinel must be gone before the probe"
    );

    std::fs::remove_file(&db_path).expect("drop state DB");
    for sib in ["-wal", "-shm"] {
        let mut p = db_path.as_os_str().to_owned();
        p.push(sib);
        let _ = std::fs::remove_file(std::path::PathBuf::from(p));
    }

    // Step 3: open a fresh DB (mailbox_map empty), connect to the
    // live server, run rebindfolders. plan() phase first so we can
    // assert on what the algorithm decided before apply touches
    // disk.
    let conn = db::open_or_recreate(&db_path).expect("reopen fresh state DB");
    let client = session::connect(&config.account, &conn)
        .await
        .expect("connect to fixture JMAP");

    let plan = rebindfolders::plan(
        &client,
        &conn,
        temp.path(),
        rebindfolders::DEFAULT_SAMPLE_SIZE,
    )
    .await
    .expect("rebindfolders::plan against live Stalwart");

    assert!(
        plan.skipped.is_empty(),
        "no folders should be skipped (got: {:?})",
        plan.skipped
    );
    assert_eq!(
        plan.candidates.len(),
        1,
        "exactly one rebind candidate expected (got: {:?})",
        plan.candidates
    );
    let candidate = &plan.candidates[0];
    assert_eq!(candidate.folder_path, inbox_path);
    assert_eq!(
        candidate.jmap_mailbox_id, expected_inbox_id,
        "rebound id must match the originally synced INBOX id"
    );
    assert_eq!(candidate.server_name, "Inbox");
    assert!(
        candidate.sample_count > 0,
        "at least one Message-ID must have been sampled and matched"
    );

    // Step 4: apply writes the sentinel. Plan() alone is read-only;
    // verify that property too -- the sentinel must still be absent
    // after plan() returned.
    assert!(
        sentinel::read(&inbox_path)
            .expect("read post-plan")
            .is_none(),
        "plan() must not write the sentinel"
    );
    let n = rebindfolders::apply(&plan).expect("apply");
    assert_eq!(n, 1, "exactly one sentinel written");

    let post_sentinel = sentinel::read(&inbox_path)
        .expect("read post-apply")
        .expect("sentinel restored by rebindfolders");
    assert_eq!(
        post_sentinel.jmap_mailbox_id, expected_inbox_id,
        "rebound sentinel must point at the original INBOX id"
    );
    assert_eq!(post_sentinel.server_name, "Inbox");

    // Step 5: a subsequent sync should treat this as steady-state.
    // mailbox_map gets repopulated, no re-download (Message-ID
    // anchoring rebinds the local files), and the sentinel is
    // refreshed (idempotently) with the parent_id field as well.
    let outcome = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("post-rebind sync");
    assert_eq!(
        outcome.downloaded, 0,
        "rebind + Message-ID anchoring must not re-download"
    );

    let mailboxes = queries::get_all_mailboxes(&conn).expect("read mailbox_map");
    assert!(
        mailboxes
            .iter()
            .any(|m| m.maildir_folder == "INBOX" && m.jmap_mailbox_id == expected_inbox_id),
        "post-sync mailbox_map must contain the rebound INBOX row"
    );
}
