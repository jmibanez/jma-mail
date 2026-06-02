//! End-to-end rebindfolders against a real Stalwart Mail container.
//!
//! The wiremock test in `tests/engine_jmap.rs` proves we send what
//! we think we send. The two e2e cases in this file prove the
//! *server* interprets what we send the way we expect across two
//! resolution paths the per-folder probe and its cross-mapping
//! post-pass take:
//!
//! - `rebindfolders_rebinds_inbox_after_db_nuke_and_sentinel_loss`
//!   covers the per-folder consensus path. INBOX-only sync, three
//!   seeded messages, nuke INBOX's sentinel + state DB, expect a
//!   single `Consensus`-tagged rebind to the originally-bound
//!   INBOX id. Exercises the `header` filter's bracketed-Message-ID
//!   match (RFC 8621 4.4.1 substring rule), `Filter::or` over
//!   multiple `header` conditions, `Email/get` returning
//!   `mailboxIds`, and the freshly-written sentinel round-tripping.
//!
//! - `rebindfolders_cross_mapping_resolves_ambiguous_archive`
//!   covers the post-pass. Whole-account sync with INBOX seeded to
//!   a heavily-pure majority, four of whose Message-IDs are also
//!   seeded into Archive as distinct emails so the same headers
//!   resolve to `{INBOX, Archive}`, plus one unique email seeded
//!   into each of Stalwart's auto-provisioned role mailboxes (Sent,
//!   Drafts, Junk, Trash) so those probe to clean per-folder
//!   consensus binds rather than `NoMessageIds` skips that would
//!   break the cross-mapping precondition. After the full nuke,
//!   INBOX consensus-binds on its pure majority, the role mailboxes
//!   each consensus-bind on their single seeded sample, and the
//!   cross-mapping pass forces Archive's strict feasibility from
//!   `{INBOX, Archive}` to `{Archive}` via the unclaimed-pool
//!   shrink left by the other consensus claims.
//!
//! Marked `#[ignore]` so the default `cargo test` invocation
//! (and the existing `ci.yaml` build job) keeps working on machines
//! without Docker. The dedicated E2E workflow runs `--ignored` to
//! exercise it.
//!
//! Run locally:
//!     CC=/usr/bin/cc cargo test --test e2e_rebindfolders -- --ignored --nocapture

mod common;

use anyhow::{Result, anyhow};
use jma_mail::config::{
    AccountConfig, AllowDestructiveFolderSync, Config, ConflictStrategy, FolderLayout, StateConfig,
    SyncConfig, WatchConfig,
};
use jma_mail::ids::JmapMailboxId;
use jma_mail::janitor::rebindfolders;
use jma_mail::jmap::session;
use jma_mail::maildir_ops::sentinel;
use jma_mail::state::{db, queries};
use jma_mail::sync::engine::SyncEngine;
use jmap_client::client::{Client, Credentials};
use jmap_client::mailbox::Role;

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
            allow_destructive_folder_sync: AllowDestructiveFolderSync::None,
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
        std::collections::HashMap::new(),
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
    assert_eq!(
        candidate.remote_path, "Inbox",
        "remote_path is the user-facing identifier; for top-level \
         INBOX it equals server_name"
    );
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

/// End-to-end validation of the cross-mapping post-pass. Sets up a
/// fixture where the per-folder M-of-N consensus probe refuses one
/// maildir (Archive) because every sample lives in two server-side
/// mailboxes -- not because the algorithm is wrong, but because the
/// content genuinely is ambiguous from Archive's local perspective.
/// INBOX's content is asymmetric (mostly pure-INBOX, a minority
/// shared with Archive) so its per-folder consensus binds cleanly.
/// The cross-mapping pass then uses the bijection cardinality
/// argument (i_total == n_disk) plus INBOX's claim shrinking the
/// unclaimed pool to drive Archive's strict feasibility to exactly
/// `{Archive}`, forcing the unique matching that promotes Archive
/// with `ResolveSource::CrossMapping`.
#[tokio::test]
#[ignore = "requires Docker; run via cargo test -- --ignored"]
async fn rebindfolders_cross_mapping_resolves_ambiguous_archive() {
    let fx = common::spawn_stalwart()
        .await
        .expect("spawn Stalwart fixture");

    // 30 unique Message-IDs into INBOX. 26 stay pure-INBOX; 4 of
    // those headers are also seeded into Archive as distinct emails
    // below, so Archive's maildir ends up holding only that
    // 4-message shared slice. The 26-pure majority pushes INBOX's
    // per-folder consensus past any plausible flake threshold: under
    // the M=3, N=4 partition the chance an entire group of four
    // shuffled samples lands all-shared (the only path to a non-
    // singleton narrow on INBOX) is negligible at this ratio.
    let mut seeds = Vec::with_capacity(30);
    for i in 0..30 {
        seeds.push(
            common::SeedMessage::simple("sender@example.com", &format!("Inbox-{i}"), "body")
                .with_message_id(&format!("<cross-{i}@test.local>")),
        );
    }
    common::seed_inbox(&fx, &seeds)
        .await
        .expect("seed INBOX with 30 unique Message-IDs");

    // Direct jmap-client for role-folder discovery and the Archive
    // Mailbox/set create so a regression in jma's outbound JMAP
    // shape can't silently invalidate the seed. The load-bearing
    // assertion path (the rebindfolders probe) is what matters for
    // independence.
    let admin = connect_admin_client(&fx)
        .await
        .expect("connect admin jmap-client");

    // Stalwart auto-provisions role mailboxes on account spawn.
    // Whole-account sync pulls them in as empty maildirs which then
    // probe to `NoMessageIds` -- a SkipReason the cross-mapping
    // precondition rejects (it breaks the bijection assumption that
    // every disk maildir corresponds to a server mailbox in this
    // cycle's probe pool). Discover non-INBOX mailboxes via JMAP
    // and seed one unique message into each so the per-folder pass
    // binds them by consensus and removes them from the skip list
    // before cross-mapping looks at the plan. Discovery beats
    // hard-coding "Sent/Drafts/Junk/Trash": Stalwart's IMAP folder
    // names for role mailboxes are not guaranteed to match those
    // labels exactly across image tags, so going through Mailbox/get
    // sources the names the server actually advertises.
    let preexisting = jma_mail::jmap::mailbox::get_all(&admin)
        .await
        .expect("Mailbox/get for role-folder discovery");
    for mb in &preexisting {
        if mb.role.as_deref() == Some("inbox") {
            continue;
        }
        common::seed_folder(
            &fx,
            &mb.name,
            &[common::SeedMessage::simple(
                "sender@example.com",
                &format!("{}-bootstrap", mb.name),
                "body",
            )
            .with_message_id(&format!(
                "<{}-bootstrap@test.local>",
                mb.name.to_lowercase().replace(' ', "-")
            ))],
        )
        .await
        .unwrap_or_else(|e| panic!("seed {} with bootstrap message: {e}", mb.name));
    }

    let archive_id = admin
        .mailbox_create("Archive".to_string(), None::<String>, Role::None)
        .await
        .map(|mb| {
            JmapMailboxId::from(
                mb.id()
                    .expect("Mailbox/set create returned no id for Archive"),
            )
        })
        .expect("create Archive mailbox");

    // Seed the same four Message-IDs that lead INBOX's corpus into
    // Archive as four *distinct* server emails. jma deposits each
    // email into exactly one maildir (one mailbox per `mailbox_ids`
    // membership), so a second email sharing a header is how the
    // same logical message comes to sit in two folders on disk --
    // the shape an account reaches when a message is filed in INBOX
    // and copied to Archive (IMAP COPY mints a second email with the
    // same Message-ID). The probe unions mailbox membership per
    // Message-ID (`intersect_mailboxes_by_sample`), so these four
    // shared headers resolve to {INBOX, Archive}: Archive's per-
    // folder probe lands on `AmbiguousAcrossSamples` while INBOX's
    // pure-majority corpus still consensus-binds. Using two single-
    // membership emails rather than one {INBOX, Archive} email keeps
    // the on-disk deposit deterministic -- `process_remote_emails`
    // routes each email by its sole binding, with no dependence on
    // mailbox iteration order.
    let archive_seeds: Vec<common::SeedMessage> = (0..4)
        .map(|i| {
            common::SeedMessage::simple("sender@example.com", &format!("Inbox-{i}"), "body")
                .with_message_id(&format!("<cross-{i}@test.local>"))
        })
        .collect();
    common::seed_folder(&fx, "Archive", &archive_seeds)
        .await
        .expect("seed Archive with the four shared Message-IDs");

    // Initial sync with no mailbox filter so every server mailbox
    // gets a local maildir -- the cross-mapping precondition
    // demands i_total == n_disk, and with the filter empty Stalwart's
    // role mailboxes plus the explicit Archive form the bijection.
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
    };

    let archive_path = temp.path().join("Archive");
    let inbox_path = temp.path().join("INBOX");
    {
        let conn = db::open_or_recreate(&db_path).expect("open state DB");
        SyncEngine::sync(&conn, &config, false)
            .await
            .expect("initial sync against fixture");
    }
    assert!(inbox_path.join("cur").is_dir(), "INBOX/cur written by sync");
    assert!(
        archive_path.join("cur").is_dir(),
        "Archive/cur written by sync"
    );

    // Capture expected ids from the freshly-written sentinels so the
    // cross-mapping rebind assertion has a ground truth.
    let pre_archive = sentinel::read(&archive_path)
        .expect("read Archive sentinel")
        .expect("sentinel written for Archive by sync");
    assert_eq!(pre_archive.jmap_mailbox_id, archive_id);

    // Nuke EVERY sentinel under the maildir root plus the state DB.
    // walk_for_orphans treats sentinel-survives folders as already
    // bound (drops them from the orphan list) and the cross-mapping
    // precondition needs every disk maildir to enter the orphan +
    // probe path, so leaving any survivor would shrink n_disk.
    for entry in std::fs::read_dir(temp.path()).expect("read maildir root") {
        let path = entry.expect("dir entry").path();
        if path.join("cur").is_dir() {
            sentinel::remove(&path).expect("remove sentinel");
        }
    }
    std::fs::remove_file(&db_path).expect("drop state DB");
    for sib in ["-wal", "-shm"] {
        let mut p = db_path.as_os_str().to_owned();
        p.push(sib);
        let _ = std::fs::remove_file(std::path::PathBuf::from(p));
    }

    // Fresh DB + JMAP client. plan() runs the per-folder probe and
    // the cross-mapping post-pass internally; the returned plan is
    // the composed result.
    let conn = db::open_or_recreate(&db_path).expect("reopen fresh state DB");
    let client = session::connect(&config.account, &conn)
        .await
        .expect("connect to fixture JMAP");
    let plan = rebindfolders::plan(
        &client,
        &conn,
        temp.path(),
        rebindfolders::DEFAULT_SAMPLE_SIZE,
        std::collections::HashMap::new(),
    )
    .await
    .expect("rebindfolders::plan against live Stalwart");

    let archive_candidate = plan
        .candidates
        .iter()
        .find(|c| c.folder_path == archive_path)
        .unwrap_or_else(|| {
            panic!(
                "Archive should be a rebind candidate; plan was: candidates={:?}, skipped={:?}",
                plan.candidates, plan.skipped
            )
        });
    assert_eq!(
        archive_candidate.source,
        rebindfolders::ResolveSource::CrossMapping,
        "Archive should be promoted via the cross-mapping post-pass"
    );
    assert_eq!(
        archive_candidate.jmap_mailbox_id, archive_id,
        "cross-mapping must rebind Archive to its original server id"
    );
    assert_eq!(
        archive_candidate.remote_path, "Archive",
        "cross-mapping candidate carries the user-facing remote_path"
    );

    let inbox_candidate = plan
        .candidates
        .iter()
        .find(|c| c.folder_path == inbox_path)
        .expect("INBOX should rebind via per-folder consensus");
    assert_eq!(
        inbox_candidate.source,
        rebindfolders::ResolveSource::Consensus,
        "INBOX's per-folder probe should consensus-bind given the asymmetric content"
    );
}

/// Connect a fresh `jmap-client` `Client` to the fixture for direct
/// server-side manipulation. Independent of `jma_mail`'s own JMAP
/// helpers so a regression in those does not silently invalidate the
/// fixture-side state the cross-mapping assertion drives off.
async fn connect_admin_client(fx: &common::JmapFixture) -> Result<Client> {
    Client::new()
        .credentials(Credentials::bearer(&fx.bearer))
        .connect(&fx.session_url)
        .await
        .map_err(|e| anyhow!("connect jmap-client to fixture: {e}"))
}
