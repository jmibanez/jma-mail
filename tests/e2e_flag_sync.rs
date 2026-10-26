//! Real Stalwart flag/placement regressions. Default backend is Docker;
//! tools/test-stalwart-native.sh selects a fresh isolated native RocksDB server.
//! Independent IMAP seeds and fresh JMAP reads verify effects, not just ACKs.

mod common;

use anyhow::{Context, Result, ensure};
use jma_mail::config::{
    AccountConfig, AllowDestructiveFolderSync, Config, ConflictStrategy, FolderLayout, StateConfig,
    SyncConfig, WatchConfig,
};
use jma_mail::ids::{JmapEmailId, JmapMailboxId};
use jma_mail::jmap::types::EmailObject;
use jma_mail::jmap::{email, mailbox};
use jma_mail::maildir_ops::flags::{flags_to_keyword_patch, flags_to_keywords, keywords_to_flags};
use jma_mail::state::{db, flag_sync, queries};
use jma_mail::sync::engine::{SyncEngine, SyncOutcome};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

struct World {
    fx: common::JmapFixture,
    _temp: tempfile::TempDir,
    conn: Connection,
    config: Config,
    verifier: jmap_client::client::Client,
    http: reqwest::Client,
    id: JmapEmailId,
    original: EmailObject,
    body: Vec<u8>,
}

fn one_file(conn: &Connection, root: &Path) -> Result<PathBuf> {
    let mut files = Vec::new();
    for mailbox in queries::get_all_mailboxes(conn)? {
        for side in ["cur", "new"] {
            let path = root.join(&mailbox.maildir_folder).join(side);
            let entries = match std::fs::read_dir(&path) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error).context("inventory actual test Maildir"),
            };
            for entry in entries {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    files.push(entry.path());
                }
            }
        }
    }
    ensure!(
        files.len() == 1,
        "expected one actual local message: {files:?}"
    );
    Ok(files.remove(0))
}

impl World {
    async fn new(imap_flags: &[&'static str]) -> Result<Self> {
        let fx = common::spawn_stalwart().await?;
        let msgid = format!("<jma-flags-{}@example.org>", uuid::Uuid::new_v4());
        common::seed_inbox(
            &fx,
            &[common::SeedMessage::simple(
                "sender@example.org",
                "flag regression",
                "unchanged message body",
            )
            .with_flags(imap_flags)
            .with_message_id(&msgid)],
        )
        .await?;
        let temp = tempfile::tempdir()?;
        let db_path = temp.path().join("state.db");
        let conn = db::open_or_recreate(&db_path)?;
        let config = Config {
            account: AccountConfig {
                // The Bearer/session select the fixture account. A unique
                // lookup identity cannot accidentally read a user's keychain.
                email: format!("jma-flags-{}@example.invalid", uuid::Uuid::new_v4()),
                token: Some(fx.bearer.clone()),
                session_url: Some(fx.session_url.clone()),
            },
            sync: SyncConfig {
                maildir_path: temp.path().to_string_lossy().into_owned(),
                mailboxes: vec![],
                conflict_strategy: ConflictStrategy::ServerWins,
                case_insensitive_match: false,
                folder_layout: FolderLayout::Fs,
                hierarchy_separator: '.',
                download_concurrency: 1,
                upload_concurrency: 1,
                retry_max_attempts: 1,
                retry_initial_backoff_ms: 1,
                retry_max_backoff_ms: 1,
                allow_destructive_folder_sync: AllowDestructiveFolderSync::None,
            },
            state: StateConfig {
                db_path: Some(db_path.to_string_lossy().into_owned()),
            },
            watch: WatchConfig::default(),
            rename_rules: vec![],
            compiled_rename_rules: vec![],
        };
        let initial = SyncEngine::sync(&conn, &config, false).await?;
        ensure!(
            initial.downloaded == 1 && initial.uploaded == 0 && initial.deleted == 0,
            "initial pull did not download exactly the IMAP seed: {initial:?}"
        );
        let verifier = jmap_client::client::Client::new()
            .credentials(fx.bearer.clone())
            .connect(&fx.session_url)
            .await?;
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        let query = Self::call(
            &http,
            &verifier,
            &fx.bearer,
            "Email/query",
            json!({"accountId":verifier.default_account_id()}),
        )
        .await?;
        let ids = query["ids"]
            .as_array()
            .context("independent Email/query ids missing")?;
        ensure!(
            ids.len() == 1,
            "fixture must have exactly one remote message: {query}"
        );
        let id: JmapEmailId = ids[0].as_str().context("invalid fixture Email ID")?.into();
        let mut remote = email::get_by_ids(&verifier, std::slice::from_ref(&id)).await?;
        ensure!(remote.len() == 1, "seeded Email missing after initial pull");
        let original = remote.remove(0);
        let body = std::fs::read(one_file(&conn, temp.path())?)?;
        Ok(Self {
            fx,
            _temp: temp,
            conn,
            config,
            verifier,
            http,
            id,
            original,
            body,
        })
    }

    async fn call(
        http: &reqwest::Client,
        client: &jmap_client::client::Client,
        token: &str,
        name: &str,
        args: Value,
    ) -> Result<Value> {
        let response: Value = http.post(client.session().api_url()).bearer_auth(token)
            .json(&json!({"using":["urn:ietf:params:jmap:core","urn:ietf:params:jmap:mail"],"methodCalls":[[name,args,"oracle"]]}))
            .send().await?.error_for_status()?.json().await?;
        let call = response["methodResponses"]
            .as_array()
            .and_then(|calls| calls.first())
            .context("oracle response missing")?;
        ensure!(call[0] == name, "oracle {name} rejected: {response}");
        Ok(call[1].clone())
    }

    async fn set_remote(&self, flags: &str) -> Result<()> {
        let mut keywords = flags_to_keywords(flags);
        keywords.insert("keep-me".into(), true);
        // Independent full replacement, not SDK keyword(false) and not the
        // production partial-patch adapter. Only this controlled seed is reset.
        let response = Self::call(&self.http, &self.verifier, &self.fx.bearer, "Email/set", json!({
            "accountId":self.verifier.default_account_id(),"update":{self.id.as_ref():{"keywords":keywords}}
        })).await?;
        ensure!(
            response["updated"]
                .as_object()
                .is_some_and(|ids| ids.contains_key(self.id.as_ref())),
            "oracle mutation lacks positive ACK: {response}"
        );
        ensure!(
            keywords_to_flags(&self.remote().await?.keywords) == flags,
            "fresh oracle write did not land"
        );
        Ok(())
    }

    fn root(&self) -> PathBuf {
        self.config.maildir_path()
    }
    fn file(&self) -> Result<PathBuf> {
        one_file(&self.conn, &self.root())
    }

    fn rename_flags(&self, flags: &str) -> Result<()> {
        let old = self.file()?;
        let name = old
            .file_name()
            .and_then(|name| name.to_str())
            .context("non-UTF-8 filename")?;
        let (id, _) = name
            .split_once(":2,")
            .context("missing actual flag suffix")?;
        std::fs::rename(&old, old.with_file_name(format!("{id}:2,{flags}")))?;
        Ok(())
    }

    async fn sync(&self) -> Result<SyncOutcome> {
        SyncEngine::sync(&self.conn, &self.config, false).await
    }

    async fn remote(&self) -> Result<EmailObject> {
        let mut messages =
            email::get_by_ids(&self.verifier, std::slice::from_ref(&self.id)).await?;
        ensure!(messages.len() == 1, "original Email identity disappeared");
        Ok(messages.remove(0))
    }

    async fn assert_view(&self, raw: &str, folder: &str, side: &str) -> Result<()> {
        let remote = self.remote().await?;
        let path = self.file()?;
        let record =
            queries::get_message_by_jmap_id(&self.conn, &self.id)?.context("binding missing")?;
        let mid = record
            .maildir_id
            .as_ref()
            .context("local identity missing")?;
        ensure!(
            path == self
                .root()
                .join(folder)
                .join(side)
                .join(format!("{mid}:2,{raw}")),
            "unexpected actual path: {path:?}"
        );
        ensure!(
            std::fs::read(&path)? == self.body,
            "message bytes changed during flag/placement effects"
        );
        let mapped = flag_sync::mapped_flags(raw);
        ensure!(
            keywords_to_flags(&remote.keywords) == mapped && record.flags == mapped,
            "mapped observations disagree"
        );
        ensure!(
            remote.keywords.get("keep-me") == Some(&true),
            "custom keyword lost"
        );
        let cached: std::collections::HashMap<String, bool> =
            serde_json::from_str(&record.jmap_keywords)?;
        ensure!(
            cached == remote.keywords,
            "cached full keywords differ from fresh remote state"
        );
        ensure!(
            queries::get_local_state(&self.conn, mid)? == Some((folder.into(), raw.into())),
            "raw local observation lost"
        );
        let history = flag_sync::get(&self.conn, &self.id)?.context("flag history missing")?;
        ensure!(
            history.baseline.as_deref() == Some(mapped.as_str())
                && history.attempt.is_none()
                && !history.review
                && !history.blocked
                && history.error.is_none(),
            "flag generation not acknowledged: {history:?}"
        );
        ensure!(
            remote.id == self.original.id
                && remote.blob_id == self.original.blob_id
                && remote.thread_id == self.original.thread_id,
            "server identity/body/thread changed"
        );
        ensure!(
            record.jmap_blob_id.as_ref() == Some(&remote.blob_id)
                && record.jmap_thread_id.as_ref() == Some(&remote.thread_id),
            "cached server identity differs"
        );
        Ok(())
    }

    async fn idle(&self) -> Result<()> {
        let outcome = self.sync().await?;
        ensure!(
            outcome.already_in_sync && !outcome.has_pending_work(),
            "later cycle was not idle: {outcome:?}"
        );
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires real Stalwart: Docker or tools/test-stalwart-native.sh"]
async fn local_removals_and_move_to_new_preserve_identity_and_custom_keywords() -> Result<()> {
    let world = World::new(&["\\Seen", "\\Flagged", "keep-me"]).await?;
    world.assert_view("FS", "INBOX", "cur").await?;
    let mid = queries::get_message_by_jmap_id(&world.conn, &world.id)?
        .unwrap()
        .maildir_id
        .unwrap();
    world.rename_flags("X")?;
    let cleared = world.sync().await?;
    ensure!(
        cleared.flag_updates == 1
            && cleared.uploaded == 0
            && cleared.downloaded == 0
            && cleared.deleted == 0,
        "local removals did not land: {cleared:?}"
    );
    world.assert_view("X", "INBOX", "cur").await?;
    world.idle().await?;
    world.assert_view("X", "INBOX", "cur").await?;
    let destination: JmapMailboxId =
        mailbox::create(&world.verifier, "Flag Archive", None, None).await?;
    world.sync().await?; // Provision the new bound destination, with no body transfer.
    let target = world
        .root()
        .join("Flag Archive/new")
        .join(format!("{mid}:2,FX"));
    std::fs::rename(world.file()?, target)?;
    let moved = world.sync().await?;
    ensure!(
        moved.moved == 1
            && moved.flag_updates == 1
            && moved.downloaded == 0
            && moved.uploaded == 0
            && moved.deleted == 0,
        "tracked new-side move lost effects: {moved:?}"
    );
    ensure!(
        world.remote().await?.mailbox_ids == std::collections::HashMap::from([(destination, true)]),
        "server membership did not follow the local move"
    );
    ensure!(
        queries::get_message_by_jmap_id(&world.conn, &world.id)?
            .unwrap()
            .maildir_id
            == Some(mid),
        "move changed file identity"
    );
    world.assert_view("FX", "Flag Archive", "new").await?;
    world.idle().await?;
    world.assert_view("FX", "Flag Archive", "new").await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires real Stalwart: Docker or tools/test-stalwart-native.sh"]
async fn remote_flags_partial_peers_and_stale_guard_remain_truthful() -> Result<()> {
    let world = World::new(&["\\Flagged", "keep-me"]).await?;
    world.assert_view("F", "INBOX", "new").await?;
    world.rename_flags("FX")?;
    ensure!(
        world.sync().await?.flag_updates == 0,
        "unowned suffix is not a server keyword"
    );
    let cur = world.root().join("INBOX/cur");
    let new = world.root().join("INBOX/new");
    std::fs::remove_dir(&cur)?;
    world.idle().await?;
    ensure!(!cur.exists(), "no-op healed an empty missing peer");
    world.assert_view("FX", "INBOX", "new").await?;
    world.set_remote("FS").await?;
    ensure!(
        world.sync().await?.flag_updates == 1,
        "Seen pull did not promote the actual new-side file"
    );
    world.assert_view("FSX", "INBOX", "cur").await?;
    std::fs::remove_dir(&new)?;
    world.set_remote("S").await?;
    ensure!(
        world.sync().await?.flag_updates == 1,
        "remote unflagging did not pull"
    );
    world.assert_view("SX", "INBOX", "cur").await?;
    ensure!(!new.exists(), "cur-side rename created an unrelated new/");
    let stale = email::get_current_state(&world.verifier).await?;
    world.set_remote("").await?;
    let rejected = email::set_email_batch_if_in_state(
        &world.verifier,
        &[email::EmailSetOp::Keywords {
            email_id: world.id.clone(),
            keywords: flags_to_keyword_patch("F"),
        }],
        &stale,
    )
    .await?;
    ensure!(
        matches!(rejected.replan, Some(email::EmailSetReplan::StateMismatch)),
        "real server did not reject a stale generation: {rejected:?}"
    );
    ensure!(
        keywords_to_flags(&world.remote().await?.keywords).is_empty(),
        "stale patch modified server flags"
    );
    ensure!(
        world.sync().await?.flag_updates == 1,
        "fresh pull after rejection did not finish"
    );
    world.assert_view("X", "INBOX", "cur").await?;
    ensure!(!new.exists(), "clearing Seen must not move cur back to new");
    world.idle().await?;
    world.assert_view("X", "INBOX", "cur").await?;
    Ok(())
}
