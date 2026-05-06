use anyhow::{Context, Result};
use clap::Parser;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

use jmapsync::cli::{AuthAction, Cli, Command};
use jmapsync::config::{self, Config};
use jmapsync::daemon;
use jmapsync::jmap::retry::{self, RetryConfig};
use jmapsync::jmap::session;
use jmapsync::maildir_ops;
use jmapsync::state;
use jmapsync::sync::engine::SyncEngine;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Set up logging
    let filter = match (cli.quiet, cli.verbose) {
        (true, _) => "error",
        (_, 0) => "jmapsync=info",
        (_, 1) => "jmapsync=debug",
        (_, 2) => "jmapsync=debug,jmap_client=debug",
        (_, _) => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .init();

    let command = cli.command.clone().unwrap_or(Command::Sync);

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
            jmapsync::auth::set_bearer_token(&email, &token)?;
            println!("Bearer token saved to keychain for {}.", email);
        }
        AuthAction::ClearToken => {
            jmapsync::auth::clear_bearer_token(&email)?;
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

            let prev = jmapsync::state::queries::get_cached_session_url(&conn, domain)?;
            jmapsync::state::queries::clear_cached_session_url(&conn, domain)?;

            let new_url = jmapsync::jmap::discovery::discover(domain).await?;
            jmapsync::state::queries::set_cached_session_url(&conn, domain, &new_url)?;

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
            jmapsync::maildir_ops::store::ensure_maildir(&path)?;
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

    let mailboxes = jmapsync::jmap::mailbox::get_all(&client).await?;

    // Index by id so `is_mailbox_synced` can walk the parent chain
    // and `resolve_folder_path` can compute the on-disk name.
    let by_id: std::collections::HashMap<_, _> =
        mailboxes.iter().map(|mb| (mb.id.clone(), mb)).collect();
    let name_cap = jmapsync::jmap::limits::max_size_mailbox_name(&client);

    println!(
        "{:<40} {:>8} {:>8}  Role",
        "On-disk name", "Total", "Unread"
    );
    println!("{}", "-".repeat(70));
    for mb in &mailboxes {
        let synced = if jmapsync::jmap::mailbox::is_mailbox_synced(
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
        // the listing matches what jmapsync would actually create.
        // Resolution can fail several ways (separator collision in a
        // segment, joined path over the server's name cap, parent
        // chain cycle, unknown parent_id); whichever it is, the user
        // would hit the same error on a real sync attempt -- mark the
        // row visibly *and* surface the reason via `warn!` so the
        // user has both the at-a-glance signal in the table and the
        // specific cause in the log output.
        let display_name = match jmapsync::maildir_ops::layout::resolve_folder_path(
            mb,
            &by_id,
            config.sync.folder_layout,
            config.sync.hierarchy_separator,
            name_cap,
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

    let rows = jmapsync::state::queries::list_jmap_state_rows(conn)?;
    if rows.is_empty() {
        println!("No JMAP state yet -- run `jmapsync sync` to bootstrap.");
        return Ok(());
    }

    let mut by_account: BTreeMap<String, Vec<jmapsync::state::queries::JmapStateRow>> =
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
    use std::collections::BTreeSet;

    println!("Maildir vs DB drift (under {}):", maildir_root.display());

    let known: BTreeSet<String> = jmapsync::state::queries::list_known_maildir_folders(conn)?
        .into_iter()
        .collect();

    if !maildir_root.exists() {
        println!("  Maildir root does not exist -- nothing to compare.");
        return Ok(());
    }

    let on_disk: BTreeSet<String> = match std::fs::read_dir(maildir_root) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .filter_map(|e| {
                let n = e.file_name().to_string_lossy().into_owned();
                // Skip hidden entries: covers `.jmapsync.db*`, lock
                // files, and anything else that isn't a synced folder.
                if n.starts_with('.') { None } else { Some(n) }
            })
            .collect(),
        Err(e) => {
            println!("  Failed to read maildir root: {e}");
            return Ok(());
        }
    };

    if known.is_empty() {
        println!("  No mailbox map yet -- DB has no record of synced folders.");
        return Ok(());
    }

    let only_disk: Vec<&str> = on_disk.difference(&known).map(String::as_str).collect();
    let only_db: Vec<&str> = known.difference(&on_disk).map(String::as_str).collect();

    print_drift_line("  Folders on disk not in DB: ", &only_disk);
    print_drift_line("  Folders in DB not on disk: ", &only_db);

    Ok(())
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
