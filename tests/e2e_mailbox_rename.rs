//! Wire-level smoke tests for the bidirectional mailbox-rename
//! path against a real Stalwart Mail container -- one test per
//! direction, exercising the JMAP shapes our executor builds for
//! `Mailbox/set { update }` (push) and the `Mailbox/get`
//! response shape `resolve_mailboxes` parses for a server-side
//! rename (pull).
//!
//! Coverage rationale: the wiremock suite in
//! `tests/engine_jmap.rs` already pins every `MailboxDecision`
//! variant, both `conflict_strategy` branches, `--dry-run`
//! safety, the rename-rule guard, and the executor's
//! warn-and-continue path under rigged rejection. What it
//! cannot pin is wire-level drift between our model of the
//! JMAP exchange and what a real implementation actually
//! sends/accepts. These two tests are the minimum to catch
//! that class of regression: one per direction, asserting the
//! end-to-end behavior holds against a real server. Other
//! scenarios (dry-run, conflict resolution, rejection retry,
//! rule guard) live in the wiremock suite where they belong.
//!
//! Marked `#[ignore]` so the default `cargo test` invocation
//! stays Docker-free. Run with:
//!     CC=/usr/bin/cc cargo test --test e2e_mailbox_rename -- --ignored --nocapture

mod common;

use anyhow::{Context, Result};
use jma_mail::config::{
    AccountConfig, AllowDestructiveFolderSync, Config, ConflictStrategy, FolderLayout, StateConfig,
    SyncConfig, WatchConfig,
};
use jma_mail::ids::JmapMailboxId;
use jma_mail::maildir_ops::sentinel;
use jma_mail::state::{db, queries};
use jma_mail::sync::engine::SyncEngine;
use jmap_client::client::{Client, Credentials};
use jmap_client::mailbox::Role;

/// Build a `jma_mail::Config` pointed at the fixture's Stalwart
/// instance with a per-test temp dir as the maildir root and DB
/// location. Empty `mailboxes` filter so whatever the test
/// creates server-side gets synced.
fn test_config(fx: &common::JmapFixture, maildir_root: &std::path::Path) -> Config {
    let db_path = maildir_root.join("state.db");
    Config {
        account: AccountConfig {
            email: fx.account_email.clone(),
            token: Some(fx.bearer.clone()),
            session_url: Some(fx.session_url.clone()),
        },
        sync: SyncConfig {
            maildir_path: maildir_root.to_string_lossy().into_owned(),
            // Whole-account sync. `vec!["Archive"]` would match the
            // pre-rename path and drop the mailbox out of the synced
            // set after the rename (the sync filter matches on the
            // server path, which changes on rename, not the stable
            // mailbox id), making the post-rename assertions
            // unreachable.
            mailboxes: vec![],
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
    }
}

/// Connect a fresh `jmap-client` `Client` to the fixture for
/// direct server-side mailbox manipulation. Independent of
/// `jma_mail`'s own JMAP helpers so a regression in those does
/// not silently invalidate the assertions that drive them.
async fn connect_admin_client(fx: &common::JmapFixture) -> Result<Client> {
    Client::new()
        .credentials(Credentials::bearer(&fx.bearer))
        .connect(&fx.session_url)
        .await
        .map_err(|e| anyhow::anyhow!("connect jmap-client to fixture: {e}"))
}

async fn create_mailbox(client: &Client, name: &str) -> Result<JmapMailboxId> {
    let mb = client
        .mailbox_create(name.to_string(), None::<String>, Role::None)
        .await
        .with_context(|| format!("Mailbox/set create {name:?}"))?;
    let id = mb
        .id()
        .ok_or_else(|| anyhow::anyhow!("Mailbox/set create returned no id"))?;
    Ok(JmapMailboxId::from(id))
}

async fn rename_mailbox(client: &Client, id: &JmapMailboxId, new_name: &str) -> Result<()> {
    client
        .mailbox_rename(id.as_ref(), new_name.to_string())
        .await
        .with_context(|| format!("Mailbox/set update {id} -> {new_name:?}"))?;
    Ok(())
}

async fn server_mailbox_name(client: &Client, id: &JmapMailboxId) -> Result<String> {
    let mb = client
        .mailbox_get(id.as_ref(), None::<Vec<jmap_client::mailbox::Property>>)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Mailbox/get {id} returned None"))?;
    Ok(mb.name().unwrap_or_default().to_string())
}

/// Push direction: user `mv`-s the maildir locally; the next
/// sync's sentinel walk detects the disagreement and issues a
/// `Mailbox/set { update }` that the server applies. Pins the
/// wire-side shape `update_name_and_parent` builds.
#[tokio::test]
#[ignore = "requires Docker; run via cargo test -- --ignored"]
async fn e2e_pushes_local_rename_to_server() {
    let fx = common::spawn_stalwart()
        .await
        .expect("spawn Stalwart fixture");
    let client = connect_admin_client(&fx)
        .await
        .expect("connect admin jmap-client");
    let arch_id = create_mailbox(&client, "Archive")
        .await
        .expect("create Archive server-side");

    let temp = tempfile::tempdir().expect("tempdir");
    let config = test_config(&fx, temp.path());
    let conn = db::open_or_recreate(&temp.path().join("state.db")).expect("open state DB");

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    assert!(
        temp.path().join("Archive").join("cur").is_dir(),
        "Archive maildir must land after first sync"
    );

    std::fs::rename(temp.path().join("Archive"), temp.path().join("Archives")).expect("local mv");

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync pushes the rename");

    let on_server = server_mailbox_name(&client, &arch_id)
        .await
        .expect("Mailbox/get for Archive id");
    assert_eq!(
        on_server, "Archives",
        "server must have applied the renamed name"
    );
    let cached = queries::get_mailbox(&conn, &arch_id)
        .unwrap()
        .expect("cache row must persist");
    assert_eq!(cached.maildir_folder, "Archives");
    assert_eq!(cached.name, "Archives");
    // Sentinel at the disk path must reflect the just-pushed
    // server_name. The directory travelled with the user's `mv`
    // but its `server_name` field was the pre-rename name until
    // the executor's post-success refresh.
    let mapping = sentinel::read(&temp.path().join("Archives"))
        .expect("sentinel read")
        .expect("sentinel must exist at the new path");
    assert_eq!(mapping.server_name, "Archives");
}

/// Pull direction: the server renames a mailbox; the next
/// sync's `Mailbox/get` returns the new name, `resolve_
/// mailboxes` detects the cache disagreement, and the executor
/// `fs::rename`s the maildir on disk. Pins the wire-side
/// `Mailbox/get` response shape we parse for a renamed mailbox
/// and the full pull-side rename pipeline (disk move,
/// `local_state` rewrite, sentinel refresh).
#[tokio::test]
#[ignore = "requires Docker; run via cargo test -- --ignored"]
async fn e2e_pulls_server_rename_to_disk() {
    let fx = common::spawn_stalwart()
        .await
        .expect("spawn Stalwart fixture");
    let client = connect_admin_client(&fx)
        .await
        .expect("connect admin jmap-client");
    let arch_id = create_mailbox(&client, "Archive")
        .await
        .expect("create Archive server-side");

    let temp = tempfile::tempdir().expect("tempdir");
    let config = test_config(&fx, temp.path());
    let conn = db::open_or_recreate(&temp.path().join("state.db")).expect("open state DB");

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("first sync");
    let archive_old = temp.path().join("Archive");
    assert!(
        archive_old.join("cur").is_dir(),
        "Archive maildir must land after first sync"
    );

    rename_mailbox(&client, &arch_id, "Archives")
        .await
        .expect("server-side rename");

    SyncEngine::sync(&conn, &config, false)
        .await
        .expect("second sync pulls the rename");

    let archive_new = temp.path().join("Archives");
    assert!(
        archive_new.join("cur").is_dir(),
        "the renamed folder must exist at the new on-disk path"
    );
    assert!(
        !archive_old.exists(),
        "the old on-disk path must be gone after the rename"
    );
    let cached = queries::get_mailbox(&conn, &arch_id)
        .unwrap()
        .expect("cache row must persist");
    assert_eq!(cached.maildir_folder, "Archives");
    assert_eq!(cached.name, "Archives");
    let mapping = sentinel::read(&archive_new)
        .expect("sentinel read")
        .expect("sentinel must exist at the new path");
    assert_eq!(mapping.jmap_mailbox_id, arch_id);
    assert_eq!(mapping.server_name, "Archives");
}
