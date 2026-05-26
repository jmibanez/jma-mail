//! End-to-end check of `janitor::remotededupe::run` against a real
//! Stalwart container. Seeds two byte-identical messages sharing a
//! `Message-ID` header, syncs once to bind the first locally,
//! injects the duplicate via a second IMAP APPEND, and then invokes
//! the remote-dedupe planner + applier directly against the
//! container's JMAP listener.
//!
//! The destructive part is the assertion: after `run` returns, the
//! server must still have exactly ONE Email object with the shared
//! Message-ID, the survivor must be the one our state DB had
//! locally bound, and every other Email in the account must be
//! untouched. The last clause is the load-bearing one -- a
//! regression that "removes duplicates" by `Email/set { destroy:
//! all }` would pass a count-based test but fail this account-
//! contents one. Hence the side check that the unrelated message
//! (different Message-ID) is still on the server.
//!
//! Marked `#[ignore]` so the default `cargo test` invocation keeps
//! working on machines without Docker. The E2E workflow runs
//! `--ignored` to exercise it.
//!
//! Run locally:
//!     CC=/usr/bin/cc cargo test --test e2e_remotededupe -- --ignored --nocapture

mod common;

use jma_mail::config::{
    AccountConfig, Config, ConflictStrategy, FolderLayout, StateConfig, SyncConfig, WatchConfig,
};
use jma_mail::ids::JmapMailboxId;
use jma_mail::jmap::{email as jmap_email, mailbox as jmap_mailbox, session};
use jma_mail::state::{db, queries};
use jma_mail::sync::engine::SyncEngine;
use std::path::Path;

// Two forms of each Message-ID: the bracketed RFC 5322 wire format
// we hand to `seed_inbox` (it goes into the `Message-ID:` header
// verbatim), and the bracketless internal form jma stores in
// `message_map.message_id` and JMAP returns from `Email/get` per
// RFC 8621. They are NOT interchangeable in assertions.
const DUP_MID_HEADER: &str = "<dup@test.local>";
const DUP_MID: &str = "dup@test.local";
const UNRELATED_MID_HEADER: &str = "<unrelated@test.local>";
const UNRELATED_MID: &str = "unrelated@test.local";
const SHARED_BODY: &str = "Identical body content for blob-equality test.";

#[tokio::test]
#[ignore = "requires Docker; run via cargo test -- --ignored"]
async fn remotededupe_destroys_server_duplicate_and_preserves_locally_bound_survivor() {
    let fx = common::spawn_stalwart()
        .await
        .expect("spawn Stalwart fixture");

    // Seed 1: the email that will become the locally-bound survivor,
    // and an unrelated message we use to prove the destroy is scoped
    // to the duplicate group rather than blasting the whole mailbox.
    common::seed_inbox(
        &fx,
        &[
            common::SeedMessage::simple("alice-test@example.com", "Shared", SHARED_BODY)
                .with_message_id(DUP_MID_HEADER),
            common::SeedMessage::simple("bob-test@example.com", "Unrelated", "Different body.")
                .with_message_id(UNRELATED_MID_HEADER),
        ],
    )
    .await
    .expect("seed initial INBOX");

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("state.db");
    let config = build_config(&fx, temp.path(), &db_path);

    let conn = db::open_or_recreate(&db_path).expect("open state DB");
    let outcome = SyncEngine::sync(&conn, &config, false)
        .await
        .expect("initial sync");
    assert_eq!(outcome.downloaded, 2, "two seeded messages must download");
    assert_eq!(outcome.failed_remote_actions, 0);

    // The survivor's jmap_email_id is whatever the initial sync
    // bound to the dup-mid maildir file. Snapshot it now so we can
    // assert later that remotededupe destroyed the *other* id, not
    // this one.
    let inbox_id = queries::get_all_mailboxes(&conn)
        .expect("query mailbox_map")
        .into_iter()
        .find(|m| m.maildir_folder == "INBOX")
        .expect("INBOX must be cached in mailbox_map")
        .jmap_mailbox_id;
    let bound_messages =
        queries::get_messages_by_jmap_mailbox_id(&conn, &inbox_id).expect("DB read");
    let survivor_id = bound_messages
        .iter()
        .find(|m| m.message_id.as_ref() == DUP_MID)
        .map(|m| m.jmap_email_id.clone())
        .expect("expected a message_map row for DUP_MID after initial sync");
    let unrelated_id = bound_messages
        .iter()
        .find(|m| m.message_id.as_ref() == UNRELATED_MID)
        .map(|m| m.jmap_email_id.clone())
        .expect("expected a message_map row for UNRELATED_MID after initial sync");

    // Seed 2: inject the duplicate. Byte-identical envelope (same
    // From / Subject / Date / Message-ID) and body so the server
    // reports identical `Email.size` (cheap pre-check passes) and
    // the streaming sha256 in `blobs_byte_equal` matches across
    // both members (expensive byte-equality check passes). The
    // blob ids themselves disagree on Stalwart -- it issues a fresh
    // blob id per Email object even when bytes match -- so the
    // planner deliberately doesn't compare blob_id directly. We
    // don't sync afterwards: the duplicate stays unknown to
    // message_map, which is the realistic post-bug state
    // remotededupe is meant to clean up.
    common::seed_inbox(
        &fx,
        &[
            common::SeedMessage::simple("alice-test@example.com", "Shared", SHARED_BODY)
                .with_message_id(DUP_MID_HEADER),
        ],
    )
    .await
    .expect("seed duplicate INBOX");

    // Open a fresh JMAP client through the same path the CLI uses,
    // so the test exercises session connect + bearer auth + the
    // limits handshake rather than a hand-rolled client.
    let client = session::connect(&config.account, &conn)
        .await
        .expect("session::connect");

    // Sanity precondition: the server is now holding two Emails with
    // the shared Message-ID and agreeing sizes (the cheap pre-check
    // signal). Byte-equality is verified inside the planner via the
    // blob-download phase; we don't need to assert it here.
    let inbox_id = resolve_inbox_id(&client).await;
    let pre_emails = fetch_inbox_emails(&client, &inbox_id).await;
    let dup_group: Vec<_> = pre_emails
        .iter()
        .filter(|e| {
            e.message_id
                .as_ref()
                .and_then(|m| m.first())
                .map(|m| m.as_ref() == DUP_MID)
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(
        dup_group.len(),
        2,
        "Stalwart must hold both copies of the duplicate Message-ID"
    );
    assert_eq!(
        dup_group[0].size, dup_group[1].size,
        "Identical seed payloads must produce identical Email.size on the server; \
         got {} and {} -- the rest of this test depends on the size pre-check passing.",
        dup_group[0].size, dup_group[1].size,
    );

    // Apply remote dedupe. dry_run=false so the destroy actually
    // fires; --yes-style gating is a CLI concern and doesn't gate
    // the library entry point.
    let folders = vec![("INBOX".to_string(), inbox_id.clone())];
    let (plan, outcome) = jma_mail::janitor::remotededupe::run(&client, &conn, &folders, false)
        .await
        .expect("remotededupe::run");

    assert_eq!(
        plan.groups.len(),
        1,
        "exactly one duplicate group should land in the plan; got {:?}",
        plan.groups
    );
    assert!(
        plan.skipped.is_empty(),
        "homogeneous-blob group must not be skipped; got {:?}",
        plan.skipped
    );
    let group = &plan.groups[0];
    assert_eq!(group.message_id.as_ref(), DUP_MID);
    assert_eq!(
        group.survivor, survivor_id,
        "survivor must be the locally-bound jmap_email_id (preserving the maildir binding)"
    );
    assert_eq!(group.destroy.len(), 1, "exactly one id should be destroyed");

    let outcome = outcome.expect("non-dry-run must return an outcome");
    assert_eq!(
        outcome.succeeded(),
        1,
        "exactly one destroy must succeed; failed={:?}",
        outcome.failed
    );
    assert!(outcome.failed.is_empty(), "destroys must not fail");

    // Post-conditions on the server: only one Email with the shared
    // Message-ID remains, and the unrelated email is untouched. The
    // unrelated check is what catches a regression that destroys
    // too eagerly.
    let post_emails = fetch_inbox_emails(&client, &inbox_id).await;
    let surviving_dup: Vec<_> = post_emails
        .iter()
        .filter(|e| {
            e.message_id
                .as_ref()
                .and_then(|m| m.first())
                .map(|m| m.as_ref() == DUP_MID)
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(
        surviving_dup.len(),
        1,
        "exactly one Email with DUP_MID must remain; got {} (post-destroy state: {:?})",
        surviving_dup.len(),
        post_emails.iter().map(|e| &e.id).collect::<Vec<_>>()
    );
    assert_eq!(
        surviving_dup[0].id, survivor_id,
        "the surviving Email must be the locally-bound one"
    );

    let unrelated_alive = post_emails.iter().any(|e| e.id == unrelated_id);
    assert!(
        unrelated_alive,
        "unrelated Email {} must still exist on the server -- remotededupe must not \
         touch anything outside the duplicate groups",
        unrelated_id
    );

    // Second invocation: now that the server holds only one Email per
    // Message-ID in INBOX, the plan must come back empty. Pins the
    // idempotency of the cleanup: running remotededupe twice doesn't
    // surprise the operator with a second round of destroys.
    let (plan2, outcome2) = jma_mail::janitor::remotededupe::run(&client, &conn, &folders, false)
        .await
        .expect("remotededupe::run (idempotent re-run)");
    assert!(
        plan2.groups.is_empty() && plan2.skipped.is_empty(),
        "second remotededupe run must find no duplicates; got groups={:?} skipped={:?}",
        plan2.groups,
        plan2.skipped,
    );
    assert!(outcome2.is_none(), "empty plan must not call apply");
}

#[tokio::test]
#[ignore = "requires Docker; run via cargo test -- --ignored"]
async fn remotededupe_skips_groups_when_sizes_disagree() {
    // Cheap pre-check exercised here: same Message-ID, different
    // payload lengths -> different `Email.size` -> SkipReason::
    // SizeMismatch. The planner must refuse without ever opening
    // the blob-download path.
    let fx = common::spawn_stalwart()
        .await
        .expect("spawn Stalwart fixture");
    common::seed_inbox(
        &fx,
        &[
            common::SeedMessage::simple("alice-test@example.com", "Shared", "Short body.")
                .with_message_id(DUP_MID_HEADER),
            common::SeedMessage::simple(
                "alice-test@example.com",
                "Shared",
                "A noticeably longer body that does not match the first one in length.",
            )
            .with_message_id(DUP_MID_HEADER),
        ],
    )
    .await
    .expect("seed size-mismatch INBOX");

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("state.db");
    let config = build_config(&fx, temp.path(), &db_path);

    let conn = db::open_or_recreate(&db_path).expect("open state DB");
    let client = session::connect(&config.account, &conn)
        .await
        .expect("session::connect");
    let inbox_id = resolve_inbox_id(&client).await;

    let pre_emails = fetch_inbox_emails(&client, &inbox_id).await;
    assert_eq!(pre_emails.len(), 2);
    assert_ne!(
        pre_emails[0].size, pre_emails[1].size,
        "different-length bodies must produce different Email.size; \
         got {} and {} -- this test can't exercise the size-mismatch path otherwise.",
        pre_emails[0].size, pre_emails[1].size
    );

    let folders = vec![("INBOX".to_string(), inbox_id.clone())];
    let (plan, outcome) = jma_mail::janitor::remotededupe::run(&client, &conn, &folders, false)
        .await
        .expect("remotededupe::run");

    assert!(plan.groups.is_empty(), "got groups={:?}", plan.groups);
    assert_eq!(plan.skipped.len(), 1);
    assert_eq!(
        plan.skipped[0].reason,
        jma_mail::janitor::remotededupe::SkipReason::SizeMismatch,
    );
    assert!(outcome.is_none(), "skip-only plan must not call apply");

    let post_emails = fetch_inbox_emails(&client, &inbox_id).await;
    assert_eq!(
        post_emails.len(),
        2,
        "size-mismatch must leave both Emails untouched"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run via cargo test -- --ignored"]
async fn remotededupe_skips_groups_when_bytes_disagree_despite_same_size() {
    // The forgery scenario: same Message-ID, same byte-length
    // body, but different content. The size pre-check passes
    // (cheap heuristic insufficient on its own); the byte-equality
    // download pass must catch the divergence and route the group
    // to SkipReason::ContentMismatch. This is the load-bearing
    // test for the safety guarantee against a tailored Message-ID
    // collision attack.
    let fx = common::spawn_stalwart()
        .await
        .expect("spawn Stalwart fixture");
    common::seed_inbox(
        &fx,
        &[
            // Both bodies are 15 chars so total RFC822 size matches.
            common::SeedMessage::simple("alice-test@example.com", "Shared", "Body content A.")
                .with_message_id(DUP_MID_HEADER),
            common::SeedMessage::simple("alice-test@example.com", "Shared", "Body content B.")
                .with_message_id(DUP_MID_HEADER),
        ],
    )
    .await
    .expect("seed same-size-different-bytes INBOX");

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("state.db");
    let config = build_config(&fx, temp.path(), &db_path);

    let conn = db::open_or_recreate(&db_path).expect("open state DB");
    let client = session::connect(&config.account, &conn)
        .await
        .expect("session::connect");
    let inbox_id = resolve_inbox_id(&client).await;

    let pre_emails = fetch_inbox_emails(&client, &inbox_id).await;
    assert_eq!(pre_emails.len(), 2);
    assert_eq!(
        pre_emails[0].size, pre_emails[1].size,
        "test precondition: bodies must be byte-length-equal so the size pre-check passes",
    );

    let folders = vec![("INBOX".to_string(), inbox_id.clone())];
    let (plan, outcome) = jma_mail::janitor::remotededupe::run(&client, &conn, &folders, false)
        .await
        .expect("remotededupe::run");

    assert!(
        plan.groups.is_empty(),
        "byte-mismatch must keep the group out of the destroy plan; got groups={:?}",
        plan.groups,
    );
    assert_eq!(plan.skipped.len(), 1);
    assert_eq!(
        plan.skipped[0].reason,
        jma_mail::janitor::remotededupe::SkipReason::ContentMismatch,
        "expected ContentMismatch; got {:?}",
        plan.skipped[0].reason
    );
    assert!(outcome.is_none(), "skip-only plan must not call apply");

    let post_emails = fetch_inbox_emails(&client, &inbox_id).await;
    assert_eq!(
        post_emails.len(),
        2,
        "content-mismatch must leave both Emails untouched"
    );
}

async fn resolve_inbox_id(client: &jmap_client::client::Client) -> JmapMailboxId {
    let mailboxes = jmap_mailbox::get_all(client)
        .await
        .expect("Mailbox/get on test fixture");
    mailboxes
        .into_iter()
        .find(|m| m.name == "INBOX" || m.role.as_deref() == Some("inbox"))
        .map(|m| m.id)
        .expect("INBOX mailbox must exist on the Stalwart fixture")
}

async fn fetch_inbox_emails(
    client: &jmap_client::client::Client,
    inbox_id: &JmapMailboxId,
) -> Vec<jma_mail::jmap::types::EmailObject> {
    let ids = jmap_email::query_mailbox(client, inbox_id.as_ref(), "INBOX", 1)
        .await
        .expect("Email/query against test fixture");
    jmap_email::get_by_ids(client, &ids)
        .await
        .expect("Email/get against test fixture")
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
