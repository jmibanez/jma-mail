//! End-to-end round-trip for `CreateRemoteMailbox` against a real
//! Stalwart Mail container.
//!
//! The wiremock tests in `tests/engine_jmap.rs` prove the executor
//! sends the right shape (per-action `Mailbox/set { create }`,
//! warn-and-continue on rejection, chained-create reference
//! resolution). This e2e proves the round-trip closes: after a local
//! folder is pushed to the server and the local state is destroyed,
//! the next sync re-derives the same folder tree from the server
//! plus sentinels alone.
//!
//! Setup:
//!  1. Spawn Stalwart with no seeded messages.
//!  2. Plant a local maildir tree under `Flat` layout (separator
//!     `.`): a top-level `Projects` mailbox plus a nested
//!     `Projects.Notes` whose chain-create path exercises the
//!     `MaybeReference::Reference` parent resolution.
//!  3. Run `SyncEngine::sync`. Reconcile should emit one
//!     `CreateRemoteMailbox` for `Projects` (top-level) and one
//!     for `Notes` (parent = `Reference("Projects")`); the
//!     executor's per-call `creation_refs` resolves the reference
//!     against the freshly-assigned parent id.
//!  4. Verify the server reports both mailboxes via `Mailbox/get`,
//!     with `Notes.parent_id == Projects.id`.
//!  5. Nuke the local state: remove the entire maildir tree and
//!     drop the state DB.
//!  6. Run `SyncEngine::sync` again with a fresh DB. The server
//!     side is unchanged; `resolve_mailboxes` should detect both
//!     `Projects` and `Projects.Notes` as missing on disk and the
//!     executor's `CreateLocalMailbox` phase should re-create the
//!     maildirs + stamp the `.jma.mapping` sentinels.
//!  7. Assert the post-nuke tree matches the pre-nuke shape: both
//!     `Projects/` and `Projects.Notes/` exist with valid maildir
//!     subdirectories and sentinels carrying the original
//!     server-assigned JMAP ids.
//!
//! Marked `#[ignore]` so the default `cargo test` invocation keeps
//! working on machines without Docker. The dedicated E2E workflow
//! runs `--ignored` to exercise it.
//!
//! Run locally:
//!     CC=/usr/bin/cc cargo test --test e2e_create_remote_mailbox -- --ignored --nocapture

mod common;

use jma_mail::config::{
    AccountConfig, Config, ConflictStrategy, FolderLayout, StateConfig, SyncConfig, WatchConfig,
};
use jma_mail::jmap::{mailbox as jmap_mailbox, session};
use jma_mail::maildir_ops::sentinel;
use jma_mail::state::{db, queries};
use jma_mail::sync::engine::SyncEngine;

#[tokio::test]
#[ignore]
async fn create_remote_mailbox_round_trips_through_server() {
    let fx = common::spawn_stalwart()
        .await
        .expect("spawn Stalwart fixture");

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
            // Empty filter so the server's INBOX and our planted
            // folders all sync. The assertion targets the planted
            // ones; INBOX coming along is fine.
            mailboxes: Vec::new(),
            conflict_strategy: ConflictStrategy::ServerWins,
            case_insensitive_match: false,
            folder_layout: FolderLayout::Flat,
            hierarchy_separator: '.',
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

    // Step 1: plant the local maildir tree the test expects to
    // round-trip. Under Flat with separator `.`, the nested-on-
    // server mailbox `Projects/Notes` is the on-disk folder
    // `Projects.Notes`.
    let projects_path = temp.path().join("Projects");
    let projects_notes_path = temp.path().join("Projects.Notes");
    for p in [&projects_path, &projects_notes_path] {
        for sub in ["cur", "new", "tmp"] {
            std::fs::create_dir_all(p.join(sub)).expect("create maildir subdir");
        }
    }

    // Step 2: first sync. CreateLocalMailbox for INBOX lands the
    // alias; CreateRemoteMailbox for Projects + Projects.Notes
    // pushes our planted folders to the server. The chained-create
    // path resolves `Notes.parent = Reference("Projects")` against
    // the per-call creation_refs once `Projects` returns its id.
    // The executor writes the sentinel in the same cycle (no
    // second-cycle race), so the planted folders carry their
    // `.jma.mapping` files as soon as this sync returns.
    {
        let conn = db::open_or_recreate(&db_path).expect("open state DB");
        SyncEngine::sync(&conn, &config, false)
            .await
            .expect("first sync");
    }

    // Capture the server's view of the planted folders so the
    // post-nuke assertion can verify the JMAP ids round-tripped.
    let pre_inbox_id_for_projects;
    let pre_projects_id;
    let pre_notes_id;
    {
        let conn = db::open_or_recreate(&db_path).expect("reopen state DB");
        let client = session::connect(&config.account, &conn)
            .await
            .expect("connect to fixture JMAP");
        let server_mailboxes = jmap_mailbox::get_all(&client)
            .await
            .expect("Mailbox/get against fixture");

        let projects = server_mailboxes
            .iter()
            .find(|mb| mb.name == "Projects")
            .expect("server has Projects after first sync");
        let notes = server_mailboxes
            .iter()
            .find(|mb| mb.name == "Notes")
            .expect("server has Notes after first sync");
        assert_eq!(
            notes.parent_id.as_ref(),
            Some(&projects.id),
            "Notes must be nested under Projects: parent_id={:?}, projects.id={:?}",
            notes.parent_id,
            projects.id
        );
        pre_inbox_id_for_projects = projects.parent_id.clone();
        pre_projects_id = projects.id.clone();
        pre_notes_id = notes.id.clone();
    }
    assert!(
        pre_inbox_id_for_projects.is_none(),
        "Projects must be top-level (no parent), got {pre_inbox_id_for_projects:?}"
    );

    // Sentinels should also be present and match the server ids.
    let pre_projects_sentinel = sentinel::read(&projects_path)
        .expect("read Projects sentinel pre-nuke")
        .expect("Projects sentinel present");
    assert_eq!(pre_projects_sentinel.jmap_mailbox_id, pre_projects_id);
    assert_eq!(pre_projects_sentinel.server_name, "Projects");
    let pre_notes_sentinel = sentinel::read(&projects_notes_path)
        .expect("read Projects.Notes sentinel pre-nuke")
        .expect("Projects.Notes sentinel present");
    assert_eq!(pre_notes_sentinel.jmap_mailbox_id, pre_notes_id);
    assert_eq!(pre_notes_sentinel.server_name, "Notes");
    assert_eq!(
        pre_notes_sentinel.parent_jmap_mailbox_id.as_ref(),
        Some(&pre_projects_id),
        "Notes sentinel must record Projects as parent"
    );

    // Step 3: nuke. Remove the entire maildir tree and drop the
    // state DB, so the next sync has nothing local to consult.
    for p in [&projects_path, &projects_notes_path] {
        std::fs::remove_dir_all(p).expect("remove planted maildir");
    }
    // INBOX gets auto-created by the first sync via
    // CreateLocalMailbox (Stalwart's INBOX role). Remove it too so
    // the post-nuke state is genuinely empty.
    let inbox_path = temp.path().join("INBOX");
    if inbox_path.exists() {
        std::fs::remove_dir_all(&inbox_path).expect("remove INBOX maildir");
    }
    std::fs::remove_file(&db_path).expect("drop state DB");
    for sib in ["-wal", "-shm"] {
        let mut p = db_path.as_os_str().to_owned();
        p.push(sib);
        let _ = std::fs::remove_file(std::path::PathBuf::from(p));
    }

    // Step 4: second sync against a fresh state DB and an empty
    // maildir root. resolve_mailboxes should detect Projects and
    // Projects.Notes as on-disk-absent, emit CreateLocalMailbox
    // for each, and the executor's first phase should re-create
    // the maildir trees + stamp fresh sentinels.
    {
        let conn = db::open_or_recreate(&db_path).expect("reopen fresh state DB");
        SyncEngine::sync(&conn, &config, false)
            .await
            .expect("second sync after nuke");
    }

    // Step 5: assert the post-nuke tree matches the pre-nuke
    // shape. Both planted folders are back, with the full maildir
    // trinity (cur/new/tmp) and sentinels carrying the same JMAP
    // ids the server assigned on the first round.
    for p in [&projects_path, &projects_notes_path] {
        for sub in ["cur", "new", "tmp"] {
            assert!(
                p.join(sub).is_dir(),
                "{}/{} must be re-created from the server",
                p.display(),
                sub
            );
        }
    }

    // Cache caught up too: the state DB's mailbox_map must now
    // hold both folders pointing at the same JMAP ids the server
    // returned. A regression where the executor writes the
    // sentinel but skips upsert_mailbox would slip through the
    // disk-only assertion above.
    {
        let conn = db::open_or_recreate(&db_path).expect("reopen state DB for assertions");
        let rows = queries::get_all_mailboxes(&conn).expect("read mailbox_map");
        let projects_row = rows
            .iter()
            .find(|r| r.maildir_folder == "Projects")
            .expect("Projects row present in mailbox_map");
        assert_eq!(projects_row.jmap_mailbox_id, pre_projects_id);
        let notes_row = rows
            .iter()
            .find(|r| r.maildir_folder == "Projects.Notes")
            .expect("Projects.Notes row present in mailbox_map");
        assert_eq!(notes_row.jmap_mailbox_id, pre_notes_id);
    }

    let post_projects_sentinel = sentinel::read(&projects_path)
        .expect("read Projects sentinel post-nuke")
        .expect("Projects sentinel re-stamped");
    assert_eq!(
        post_projects_sentinel.jmap_mailbox_id, pre_projects_id,
        "Projects must round-trip with the same JMAP id"
    );
    assert_eq!(post_projects_sentinel.server_name, "Projects");

    let post_notes_sentinel = sentinel::read(&projects_notes_path)
        .expect("read Projects.Notes sentinel post-nuke")
        .expect("Projects.Notes sentinel re-stamped");
    assert_eq!(
        post_notes_sentinel.jmap_mailbox_id, pre_notes_id,
        "Notes must round-trip with the same JMAP id"
    );
    assert_eq!(post_notes_sentinel.server_name, "Notes");
    assert_eq!(
        post_notes_sentinel.parent_jmap_mailbox_id.as_ref(),
        Some(&pre_projects_id),
        "Notes sentinel must still record Projects as parent post-nuke"
    );
}
