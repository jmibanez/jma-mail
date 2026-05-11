use anyhow::{Context, Result};
use clap::Parser;
use jma_mail::maildir_ops::layout::FolderLayoutDefinition;
use std::collections::BTreeSet;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

use jma_mail::cli::{AuthAction, Cli, Command};
use jma_mail::config::{self, Config};
use jma_mail::daemon;
use jma_mail::jmap::retry::{self, RetryConfig};
use jma_mail::jmap::session;
use jma_mail::maildir_ops;
use jma_mail::state;
use jma_mail::sync::engine::SyncEngine;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    jma_mail::ui::init_quiet(cli.quiet);

    // Verbosity dial. At the default level the tracing filter sits
    // at warn -- info-level logs are suppressed, and milestone status
    // ("Sync complete", "Watch mode active") flows through `notify!`
    // as plain stdout text, independent of tracing. -v opens info
    // for our crates, -vv opens debug for our crates, -vvv widens
    // debug to every crate (notably hyper/tokio internals), -vvvv
    // lifts to trace. -q clamps every channel to error.
    let filter = match (cli.quiet, cli.verbose) {
        (true, _) => "error",
        (_, 0) => "jma_mail=warn,jma=warn",
        (_, 1) => "jma_mail=info,jma=info",
        (_, 2) => "jma_mail=debug,jma=debug",
        (_, 3) => "debug",
        (_, _) => "trace",
    };
    // Tracing logs go to stderr so `notify!`'s status text (stdout)
    // stays parseable when the user redirects one and not the other.
    // tracing_subscriber's default writer is stdout, hence the
    // explicit override.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .with_writer(std::io::stderr)
        .init();

    let command = cli.command.clone().unwrap_or(Command::Sync);

    jma_mail::notify!("Running jma version {}", env!("JMA_VERSION"));

    match command {
        Command::Init => cmd_init(&cli).await,
        Command::Mailboxes => cmd_mailboxes(&cli).await,
        Command::Status => cmd_status(&cli).await,
        Command::Sync => cmd_sync(&cli).await,
        Command::Pull => cmd_pull(&cli).await,
        Command::Push => cmd_push(&cli).await,
        Command::Watch => cmd_watch(&cli).await,
        Command::Auth { action, account } => cmd_auth(&cli, action, account).await,
    }
}

async fn cmd_auth(cli: &Cli, action: AuthAction, email: String) -> Result<()> {
    use std::io::{BufRead, IsTerminal};
    match action {
        AuthAction::SetToken => {
            let token = if std::io::stdin().is_terminal() {
                rpassword::prompt_password("Bearer token: ")
                    .context("Failed to read token from prompt")?
            } else {
                let mut s = String::new();
                std::io::stdin()
                    .lock()
                    .read_line(&mut s)
                    .context("Failed to read token from stdin")?;
                s.trim_end_matches(['\n', '\r']).to_string()
            };
            jma_mail::auth::set_bearer_token(&email, &token)?;
            println!("Bearer token saved to keychain for {}.", email);
        }
        AuthAction::ClearToken => {
            jma_mail::auth::clear_bearer_token(&email)?;
            println!("Bearer token cleared from keychain for {}.", email);
        }
        AuthAction::Rediscover => {
            let config = load_config(cli)?;
            if config.account.session_url.is_some() {
                println!(
                    "Note: [account].session_url is set explicitly in the config, \
                     so sync bypasses the discovery cache. Rediscovering anyway -- \
                     this will take effect once you remove the override."
                );
            }
            let domain = config.account.email_domain()?;
            let db_path = config.db_path();
            // No maildir lock here: this only touches the discovery
            // cache, which SQLite serializes internally and which is
            // independent of the maildir. Safe to run alongside an
            // in-progress sync/watch.
            let conn = state::db::open(&db_path)?;

            let prev = jma_mail::state::queries::get_cached_session_url(&conn, domain)?;
            jma_mail::state::queries::clear_cached_session_url(&conn, domain)?;

            let new_url = jma_mail::jmap::discovery::discover(domain).await?;
            jma_mail::state::queries::set_cached_session_url(&conn, domain, &new_url)?;

            match prev.as_deref() {
                Some(p) if p == new_url => {
                    println!("Discovered (unchanged): {new_url}");
                }
                Some(p) => {
                    println!("Discovered: {new_url}");
                    println!("(previously cached: {p})");
                }
                None => println!("Discovered: {new_url}"),
            }
        }
    }
    Ok(())
}

async fn cmd_init(cli: &Cli) -> Result<()> {
    let config_path = config::expand_tilde(&cli.config);

    if config_path.exists() {
        println!("Config file already exists: {}", config_path.display());
    } else {
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Mode 0o600 from creation so a token added later isn't briefly
        // exposed under the user's umask. The load-time perms check (in
        // config::check_token_perms) catches files that already exist
        // with looser perms.
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut f = opts
            .open(&config_path)
            .with_context(|| format!("Failed to create config file: {}", config_path.display()))?;
        std::io::Write::write_all(&mut f, config::default_config_template().as_bytes())
            .with_context(|| format!("Failed to write config file: {}", config_path.display()))?;
        println!("Created config file: {}", config_path.display());
    }

    match Config::load(&config_path) {
        Ok(config) => provision_maildirs(&config)?,
        Err(e) => {
            println!(
                "Skipping maildir provisioning: could not load config ({})",
                e
            );
        }
    }

    println!("Edit the config file and set your Fastmail API token.");
    println!("Generate a token at: https://www.fastmail.com/settings/security/tokens");

    Ok(())
}

/// Pre-create maildir folders for each entry in `[sync].mailboxes` so users
/// can hand them to other tools (notmuch, mu, an MUA) before the first sync.
/// Folder names are taken verbatim from the config -- including the literal
/// `INBOX`, which matches mbsync's on-disk convention and is what the sync
/// engine writes the inbox-role mailbox into.
///
/// Safe to re-run: a folder is only created when its path does not exist or
/// exists but is empty. Already-populated maildirs are left untouched.
fn provision_maildirs(config: &Config) -> Result<()> {
    if config.sync.mailboxes.is_empty() {
        return Ok(());
    }

    let root = config.maildir_path();
    if !root.exists() {
        std::fs::create_dir_all(&root)
            .with_context(|| format!("Failed to create maildir root {}", root.display()))?;
        println!("Created maildir root: {}", root.display());
    }

    for name in &config.sync.mailboxes {
        let path = root.join(name);
        let should_create = match std::fs::read_dir(&path) {
            Ok(mut iter) => iter.next().is_none(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
            Err(e) => {
                println!("Skipping {}: {}", path.display(), e);
                continue;
            }
        };

        if should_create {
            jma_mail::maildir_ops::store::ensure_maildir(&path)?;
            println!("Provisioned maildir: {}", path.display());
        } else {
            println!("Skipping non-empty maildir: {}", path.display());
        }
    }

    Ok(())
}

async fn cmd_mailboxes(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    // Read-only listing, but we still open the state DB so
    // `session::connect` can hit the JMAP discovery cache. No maildir
    // lock here -- this command shouldn't block while a sync/watch
    // invocation holds it.
    let db_path = config.db_path();
    let conn = state::db::open(&db_path)?;
    let client = session::connect(&config.account, &conn).await?;

    let mailboxes = jma_mail::jmap::mailbox::get_all(&client).await?;

    // Index by id so `is_mailbox_synced` can walk the parent chain
    // and `resolve_folder_path` can compute the on-disk name.
    let by_id: std::collections::HashMap<_, _> =
        mailboxes.iter().map(|mb| (mb.id.clone(), mb)).collect();
    let name_cap = jma_mail::jmap::limits::max_size_mailbox_name(&client);

    println!(
        "{:<40} {:>8} {:>8}  Role",
        "On-disk name", "Total", "Unread"
    );
    println!("{}", "-".repeat(70));
    let layout_definition = FolderLayoutDefinition::from_config(&config, name_cap);
    for mb in &mailboxes {
        let synced = if jma_mail::jmap::mailbox::is_mailbox_synced(
            &config.sync.mailboxes,
            mb,
            &by_id,
            config.sync.case_insensitive_match,
        ) {
            "*"
        } else {
            " "
        };
        // Show the on-disk path under the user's configured layout, so
        // the listing matches what jma would actually create.
        // Resolution can fail several ways (separator collision in a
        // segment, joined path over the server's name cap, parent
        // chain cycle, unknown parent_id); whichever it is, the user
        // would hit the same error on a real sync attempt -- mark the
        // row visibly *and* surface the reason via `warn!` so the
        // user has both the at-a-glance signal in the table and the
        // specific cause in the log output.
        let display_name = match jma_mail::maildir_ops::layout::resolve_folder_path(
            mb,
            &by_id,
            &layout_definition,
        ) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("mailbox {} resolution failed: {:#}", mb.id, e);
                format!("{} (unresolved)", mb.name)
            }
        };
        println!(
            "{} {:<38} {:>8} {:>8}  {}",
            synced,
            display_name,
            mb.total_emails,
            mb.unread_emails,
            mb.role.as_deref().unwrap_or("")
        );
    }
    println!("\n* = synced");

    Ok(())
}

async fn cmd_status(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    let db_path = config.db_path();
    // Read-only path: open without acquiring the state DB lock so this
    // command doesn't block while a sync/watch holds it. Mirrors what
    // `cmd_mailboxes` does.
    let conn = state::db::open(&db_path)?;

    println!("Configured account: {}", config.account.email);
    println!("Maildir root: {}", config.maildir_path().display());
    print_db_metadata(&db_path, &conn)?;

    println!();
    print_account_cursors(&conn)?;

    println!();
    print_maildir_drift(&conn, &config.maildir_path())?;

    Ok(())
}

fn print_db_metadata(db_path: &std::path::Path, conn: &rusqlite::Connection) -> Result<()> {
    let user_version: u32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .context("Failed to read PRAGMA user_version")?;
    let size_bytes = std::fs::metadata(db_path).map(|m| m.len()).ok();
    match size_bytes {
        Some(n) => println!(
            "State DB: {} ({}, schema v{})",
            db_path.display(),
            format_bytes(n),
            user_version
        ),
        None => println!(
            "State DB: {} (size unavailable, schema v{})",
            db_path.display(),
            user_version
        ),
    }
    Ok(())
}

fn print_account_cursors(conn: &rusqlite::Connection) -> Result<()> {
    use std::collections::BTreeMap;

    let rows = jma_mail::state::queries::list_jmap_state_rows(conn)?;
    if rows.is_empty() {
        println!("No JMAP state yet -- run `jma sync` to bootstrap.");
        return Ok(());
    }

    let mut by_account: BTreeMap<String, Vec<jma_mail::state::queries::JmapStateRow>> =
        BTreeMap::new();
    for row in rows {
        by_account
            .entry(row.account_id.clone())
            .or_default()
            .push(row);
    }

    let now = chrono::Utc::now();
    for (i, (acct, entries)) in by_account.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("JMAP cursors (account {}):", acct);

        // Last cursor write = MAX(updated_at) across this account's entities.
        // Reported as "last sync" with last-cursor-write semantics: a
        // run that produces no state change is invisible here. Adequate
        // for the staleness question ("should I run sync?") without
        // adding a sync_runs table. Lexicographic max is correct because
        // SQLite's `datetime('now')` always emits fixed-width
        // `YYYY-MM-DD HH:MM:SS` UTC -- if a future migration switches
        // formats, this comparison silently breaks.
        let latest = entries
            .iter()
            .map(|e| e.updated_at.as_str())
            .max()
            .expect("BTreeMap entry built from at least one push");
        let ago = ago_phrase(latest, &now).unwrap_or_else(|_| "?".into());
        println!("  Last cursor write: {} ({})", latest, ago);

        for entry in entries {
            if entry.state.is_empty() {
                println!(
                    "  {} cursor: FORCED RESYNC -- next mutating run will full-resync",
                    entry.entity_type
                );
            } else {
                println!("  {} cursor: healthy", entry.entity_type);
            }
        }
    }

    Ok(())
}

fn ago_phrase(sqlite_ts: &str, now: &chrono::DateTime<chrono::Utc>) -> Result<String> {
    let dt = chrono::NaiveDateTime::parse_from_str(sqlite_ts, "%Y-%m-%d %H:%M:%S")
        .with_context(|| format!("Failed to parse timestamp: {sqlite_ts}"))?;
    let delta = now.signed_duration_since(dt.and_utc());
    // Clamp negative deltas to 0: harmless display fallback if the
    // wall clock has drifted backward since the last cursor write.
    let secs = delta.num_seconds().max(0);
    Ok(if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86400)
    })
}

fn print_maildir_drift(conn: &rusqlite::Connection, maildir_root: &std::path::Path) -> Result<()> {
    println!("Maildir vs DB drift (under {}):", maildir_root.display());

    let known: BTreeSet<String> = jma_mail::state::queries::list_known_maildir_folders(conn)?
        .into_iter()
        .collect();

    if !maildir_root.exists() {
        println!("  Maildir root does not exist -- nothing to compare.");
        return Ok(());
    }

    if known.is_empty() {
        println!("  No mailbox map yet -- DB has no record of synced folders.");
        return Ok(());
    }

    // Walk recursively to find every directory that looks like a
    // maildir (has `cur/` underneath). This is the only shape that
    // works across all three folder layouts:
    //   - Flat:      <root>/foo.bar/cur                (depth 1)
    //   - MaildirPP: <root>/.foo.bar/cur               (depth 1, leading dot)
    //   - Fs:        <root>/foo/bar/cur                (depth N>=1)
    let on_disk = find_maildir_folders(maildir_root);

    let only_disk: Vec<&str> = on_disk.difference(&known).map(String::as_str).collect();
    // For "in DB not on disk", trust the per-folder existence check
    // rather than set-difference: it's layout-independent (a stored
    // `maildir_folder` of `parent/child` joins onto the root with the
    // FS separator, regardless of whether the *layout* uses `/` or
    // `.`) and avoids being fooled by a recursive walk that missed
    // something.
    let only_db: Vec<&str> = known
        .iter()
        .filter(|f| !maildir_root.join(f).join("cur").is_dir())
        .map(String::as_str)
        .collect();

    print_drift_line("  Folders on disk not in DB: ", &only_disk);
    print_drift_line("  Folders in DB not on disk: ", &only_db);

    Ok(())
}

/// Walk `root` recursively and return every relative path whose
/// directory contains a `cur/` subdirectory. That's our "is this a
/// maildir?" marker -- works under any folder layout and matches
/// what `ensure_maildir` actually creates.
///
/// Skips jma's own state markers (`.jma.*`), the maildir
/// internals (`cur`/`new`/`tmp` -- we don't recurse into them, since
/// any directory that *contains* one of those is itself a maildir
/// already). Read errors at any level are silently dropped: this is
/// a diagnostic command, and a partial drift report is more useful
/// than no report.
fn find_maildir_folders(root: &std::path::Path) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    walk_for_maildirs(root, root, &mut found);
    found
}

fn walk_for_maildirs(root: &std::path::Path, dir: &std::path::Path, found: &mut BTreeSet<String>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return,
    };
    for entry in entries.filter_map(|e| e.ok()) {
        // `DirEntry::file_type()` doesn't traverse symlinks, so a
        // symlink pointing at a directory has `is_dir() == false` here
        // and gets filtered out before recursion -- no symlink loops.
        // Side effect: a real maildir tree behind a symlink at the
        // root won't show up in the drift report, which is acceptable
        // for a diagnostic.
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with(".jma.") {
            continue;
        }
        if name_str == "cur" || name_str == "new" || name_str == "tmp" {
            continue;
        }
        let path = entry.path();
        if path.join("cur").is_dir()
            && let Ok(rel) = path.strip_prefix(root)
        {
            found.insert(rel.to_string_lossy().into_owned());
        }
        walk_for_maildirs(root, &path, found);
    }
}

fn print_drift_line(prefix: &str, items: &[&str]) {
    if items.is_empty() {
        println!("{prefix}(none)");
    } else {
        println!("{prefix}{}", items.join(", "));
    }
}

fn format_bytes(n: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if n >= GB {
        format!("{:.1} GB", n as f64 / GB as f64)
    } else if n >= MB {
        format!("{:.1} MB", n as f64 / MB as f64)
    } else if n >= KB {
        format!("{:.1} KB", n as f64 / KB as f64)
    } else {
        format!("{n} B")
    }
}

/// Acquire both process-lifetime locks in the canonical order: maildir
/// first, then state DB. Order is load-bearing -- consistent acquisition
/// order across every mutating call site is what prevents deadlock when
/// two processes contend on different pairs. See the doc comments on
/// `maildir_ops::lock::acquire_lock` and `state::db::acquire_lock` for
/// what each lock protects.
fn acquire_mutator_locks(config: &Config) -> Result<()> {
    maildir_ops::lock::acquire_lock(&config.maildir_path())?;
    state::db::acquire_lock(&config.db_path())?;
    Ok(())
}

async fn cmd_sync(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    SyncEngine::sync(&conn, &config, cli.dry_run).await?;

    Ok(())
}

async fn cmd_pull(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    SyncEngine::pull_only(&conn, &config).await?;

    Ok(())
}

async fn cmd_push(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    SyncEngine::push_only(&conn, &config).await?;

    Ok(())
}

async fn cmd_watch(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    daemon::runner::run(&conn, &config).await?;

    Ok(())
}

fn load_config(cli: &Cli) -> Result<Config> {
    let config = Config::load(&cli.config).context("Failed to load config")?;
    // Apply runtime tunables that live behind a process-wide
    // OnceLock. Idempotent: only the first call per process takes
    // effect, which is fine because every subcommand resolves the
    // same config file.
    retry::init_retry_config(RetryConfig {
        max_attempts: config.sync.retry_max_attempts,
        initial_backoff: Duration::from_millis(config.sync.retry_initial_backoff_ms),
        max_backoff: Duration::from_millis(config.sync.retry_max_backoff_ms),
    });
    Ok(config)
}

#[cfg(test)]
mod drift_tests {
    use super::*;

    /// Build a fake maildir at `<root>/<folder>` with the cur/new/tmp
    /// triplet that `find_maildir_folders` keys on.
    fn touch_maildir(root: &std::path::Path, folder: &str) {
        let path = root.join(folder);
        for sub in ["cur", "new", "tmp"] {
            std::fs::create_dir_all(path.join(sub)).unwrap();
        }
    }

    #[test]
    fn finds_flat_layout_folders() {
        let dir = tempfile::tempdir().unwrap();
        touch_maildir(dir.path(), "INBOX");
        touch_maildir(dir.path(), "Archive");
        touch_maildir(dir.path(), "[Airmail].Sent");

        let found = find_maildir_folders(dir.path());

        let expected: BTreeSet<String> = ["INBOX", "Archive", "[Airmail].Sent"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(found, expected);
    }

    /// Pins the bug fix: the prior `starts_with('.')` filter dropped
    /// every Maildir++ folder. The recursive walk lets dotted folders
    /// through while still skipping our own `.jma.*` markers.
    #[test]
    fn finds_maildir_pp_layout_folders() {
        let dir = tempfile::tempdir().unwrap();
        touch_maildir(dir.path(), ".INBOX");
        touch_maildir(dir.path(), ".Archive");
        touch_maildir(dir.path(), ".[Airmail].Sent");

        let found = find_maildir_folders(dir.path());

        let expected: BTreeSet<String> = [".INBOX", ".Archive", ".[Airmail].Sent"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(found, expected);
    }

    /// Pins the bug fix: the prior top-level-only walk missed every
    /// nested Fs mailbox at depth >= 2. The recursive walk records
    /// each maildir at whatever depth it lives.
    #[test]
    fn finds_fs_layout_nested_folders() {
        let dir = tempfile::tempdir().unwrap();
        touch_maildir(dir.path(), "INBOX");
        touch_maildir(dir.path(), "[Airmail]");
        touch_maildir(dir.path(), "[Airmail]/Sent");
        touch_maildir(dir.path(), "[Airmail]/Drafts");

        let found = find_maildir_folders(dir.path());

        let expected: BTreeSet<String> =
            ["INBOX", "[Airmail]", "[Airmail]/Sent", "[Airmail]/Drafts"]
                .into_iter()
                .map(String::from)
                .collect();
        assert_eq!(found, expected);
    }

    /// `.jma.db`, `.jma.lock`, etc. live at the maildir root
    /// alongside synced folders. They must not be reported as drift.
    #[test]
    fn skips_jma_state_markers() {
        let dir = tempfile::tempdir().unwrap();
        touch_maildir(dir.path(), "INBOX");
        // Mimic the on-disk shape of jma's own state files.
        std::fs::File::create(dir.path().join(".jma.db")).unwrap();
        std::fs::File::create(dir.path().join(".jma.lock")).unwrap();
        // Also a `.jma.foo` directory, just to confirm the filter
        // matches by prefix not by extension.
        std::fs::create_dir_all(dir.path().join(".jma.cache").join("cur")).unwrap();

        let found = find_maildir_folders(dir.path());

        let expected: BTreeSet<String> = ["INBOX"].into_iter().map(String::from).collect();
        assert_eq!(found, expected);
    }

    /// A directory without `cur/` is just a regular directory, not a
    /// maildir. Don't claim it.
    #[test]
    fn ignores_directories_without_cur() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("notes")).unwrap();
        std::fs::create_dir_all(dir.path().join("staging")).unwrap();
        touch_maildir(dir.path(), "INBOX");

        let found = find_maildir_folders(dir.path());

        let expected: BTreeSet<String> = ["INBOX"].into_iter().map(String::from).collect();
        assert_eq!(found, expected);
    }
}
