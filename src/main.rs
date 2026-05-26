use anyhow::{Context, Result};
use clap::Parser;
use jma_mail::maildir_ops::layout::FolderLayoutDefinition;
use std::collections::BTreeSet;
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

use jma_mail::cli::{AuthAction, Cli, Command, JanitorAction};
use jma_mail::config::{self, Config};
use jma_mail::daemon;
use jma_mail::jmap::retry::{self, RetryConfig};
use jma_mail::jmap::session;
use jma_mail::maildir_ops;
use jma_mail::profile::{self, ProfileSink};
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
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(env_filter);

    // Build the optional profile layer + sink up front. The layer
    // installs alongside fmt_layer; the sink rides through the
    // command dispatch so end-of-run / per-cycle flushes share one
    // place to render. `Option<L>` implements `Layer<S>` so the
    // `.with(...)` arm is identical whether profiling is on or off.
    let profile_sink: Option<ProfileSink> = if cli.profile || cli.profile_json.is_some() {
        let (layer, handle) = profile::build_layer();
        // Targets filter pins the profile layer to *only* our
        // instrumentation targets, at any level. Two reasons: the
        // visitor cost stays off unrelated events, and our profile
        // spans/events fire even when the fmt layer's filter would
        // otherwise drop them at the default verbosity.
        let target_filter = tracing_subscriber::filter::Targets::new()
            .with_target(profile::TARGET_PHASE, tracing::Level::TRACE)
            .with_target(profile::TARGET_BLOB, tracing::Level::TRACE)
            .with_target(profile::TARGET_FILE_OP, tracing::Level::TRACE);
        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(layer.with_filter(target_filter))
            .init();
        Some(ProfileSink {
            handle,
            print_table: cli.profile,
            json_path: cli.profile_json.clone(),
        })
    } else {
        tracing_subscriber::registry().with(fmt_layer).init();
        None
    };

    let command = cli.command.clone().unwrap_or(Command::Sync);
    // Capture the discriminant up front: the Auth match arm
    // partial-moves `account` out of `command`, so any later check
    // against `command` would otherwise fail to compile.
    let is_watch = matches!(command, Command::Watch);

    jma_mail::notify!("Running jma version {}", env!("JMA_VERSION"));

    let result = match command {
        Command::Init => cmd_init(&cli).await,
        Command::Mailboxes => cmd_mailboxes(&cli).await,
        Command::Status => cmd_status(&cli).await,
        Command::Sync => cmd_sync(&cli).await,
        Command::Pull => cmd_pull(&cli).await,
        Command::Push => cmd_push(&cli).await,
        Command::Watch => cmd_watch(&cli, profile_sink.clone()).await,
        Command::Auth { action, account } => cmd_auth(&cli, action, account).await,
        Command::Janitor { action } => cmd_janitor(&cli, action).await,
    };

    // One-shot commands (everything except Watch) flush their
    // accumulated profile here. Watch flushes per cycle inside the
    // daemon and has nothing meaningful left over for an end-of-run
    // emit -- the sink it received owns the per-cycle flushing.
    //
    // Flush failure is logged but does not stomp the real command
    // result: profiling is a non-load-bearing side channel, and a
    // failed sync command is the error the user actually needs to
    // see. Mirrors the warn-don't-fail policy in the daemon's
    // per-cycle `flush_profile`.
    if !is_watch
        && let Some(sink) = &profile_sink
        && let Err(e) = sink.flush_snapshot()
    {
        tracing::warn!("Failed to flush profile summary: {:#}", e);
    }

    result
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

    println!("Edit the config file and set your JMAP API token.");
    println!("For Fastmail, generate one at: https://www.fastmail.com/settings/security/tokens");
    println!("Then run `jma sync` to provision your local maildirs and pull existing messages.");

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
/// Skips jma's own private namespace (see
/// `maildir_ops::namespace::is_jma_private`), the maildir
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
        if maildir_ops::namespace::is_jma_private(&name_str) {
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

    SyncEngine::pull_only(&conn, &config, cli.dry_run).await?;

    Ok(())
}

async fn cmd_push(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    SyncEngine::push_only(&conn, &config, cli.dry_run).await?;

    Ok(())
}

async fn cmd_watch(cli: &Cli, profile_sink: Option<ProfileSink>) -> Result<()> {
    let config = load_config(cli)?;
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    daemon::runner::run(&conn, &config, profile_sink).await?;

    Ok(())
}

async fn cmd_janitor(cli: &Cli, action: Option<JanitorAction>) -> Result<()> {
    // None defaults to the safe-default set; today that's just
    // dedupe. As prune/db_gc tasks land, this match grows to fan
    // out across all of them when no specific action was named.
    // `remotededupe` stays off the default set deliberately: it
    // talks to the server and destroys remote Email objects, so
    // it must always be invoked explicitly.
    let action = action.unwrap_or(JanitorAction::Dedupe);
    match action {
        JanitorAction::Dedupe => cmd_janitor_dedupe(cli).await,
        JanitorAction::Remotededupe { mailbox, yes } => {
            cmd_janitor_remotededupe(cli, mailbox, yes).await
        }
        JanitorAction::Rebindfolders { sample_size, apply } => {
            cmd_janitor_rebindfolders(cli, sample_size, apply).await
        }
    }
}

async fn cmd_janitor_dedupe(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    // Offline path: enumerate folders from the DB's mailbox_map
    // instead of going through JMAP. Janitor work shouldn't
    // require connectivity. If no sync has ever run, the DB has
    // no folder list and there is nothing to dedupe.
    let folders = jma_mail::state::queries::list_known_maildir_folders(&conn)?;
    if folders.is_empty() {
        jma_mail::notify!("No known synced folders -- run `jma sync` first.");
        return Ok(());
    }

    let maildir_root = config.maildir_path();
    let plan = jma_mail::janitor::dedupe::run(&maildir_root, &folders, cli.dry_run)?;

    if plan.deletions.is_empty() {
        jma_mail::notify!(
            "Dedupe: no duplicates found across {} folder(s).",
            folders.len()
        );
    } else if cli.dry_run {
        jma_mail::notify!(
            "Dedupe (dry-run): would remove {} duplicate file(s):",
            plan.deletions.len()
        );
        for d in &plan.deletions {
            println!(
                "  [DEDUPE] {} ({}) in {}/  (keeping {})",
                d.maildir_id, d.message_id, d.folder, d.kept_maildir_id
            );
        }
    } else {
        jma_mail::notify!(
            "Dedupe: removed {} duplicate file(s) across {} folder(s).",
            plan.deletions.len(),
            folders.len()
        );
    }
    Ok(())
}

async fn cmd_janitor_remotededupe(cli: &Cli, mailbox: Option<String>, yes: bool) -> Result<()> {
    let config = load_config(cli)?;
    // Acquire the same locks any mutating command does. We don't
    // write the maildir, but a running `jma sync` holds *both* the
    // maildir and DB locks via `acquire_mutator_locks`, so asking
    // only for the DB lock would queue behind the same contention
    // without communicating to the operator that the conflict is
    // total. The honest posture is "this and sync don't co-exist."
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    // Resolve --mailbox (if given) against the synced set so we
    // never query a folder the user has no local mapping for; the
    // server might have many more mailboxes than jma is syncing,
    // and "I meant Archive" vs "I meant [Airmail].Archive" should
    // surface as an error before any JMAP call.
    let mailboxes = jma_mail::state::queries::get_all_mailboxes(&conn)?;
    if mailboxes.is_empty() {
        jma_mail::notify!("No known synced mailboxes -- run `jma sync` first.");
        return Ok(());
    }
    let folders: Vec<(String, jma_mail::ids::JmapMailboxId)> = if let Some(name) = mailbox {
        let matched = mailboxes
            .iter()
            .find(|m| m.maildir_folder == name)
            .with_context(|| {
                format!(
                    "No synced mailbox with maildir folder {:?}. Run `jma mailboxes` \
                     to see the known set.",
                    name
                )
            })?;
        vec![(
            matched.maildir_folder.clone(),
            matched.jmap_mailbox_id.clone(),
        )]
    } else {
        mailboxes
            .iter()
            .map(|m| (m.maildir_folder.clone(), m.jmap_mailbox_id.clone()))
            .collect()
    };

    let client = session::connect(&config.account, &conn).await?;

    // Refuse-by-default outside --dry-run: --yes is the affirmative
    // gate for a destructive remote action. We still build the plan
    // so the user sees what *would* have been destroyed; we just
    // skip the apply step.
    let effective_dry_run = cli.dry_run || !yes;
    let (plan, outcome) =
        jma_mail::janitor::remotededupe::run(&client, &conn, &folders, effective_dry_run).await?;

    render_remotededupe_plan(&plan);

    if plan.destroy_count() == 0 && plan.skipped.is_empty() {
        jma_mail::notify!(
            "Remote dedupe: no duplicates found across {} mailbox(es).",
            folders.len()
        );
        return Ok(());
    }

    if cli.dry_run {
        jma_mail::notify!(
            "Remote dedupe (dry-run): would destroy {} id(s) across {} group(s); \
             {} group(s) skipped. See [REMOTE-DEDUPE-SKIP] lines above for the reason on each.",
            plan.destroy_count(),
            plan.groups.len(),
            plan.skipped.len(),
        );
    } else if !yes {
        jma_mail::notify!(
            "Remote dedupe: {} id(s) eligible for destruction across {} group(s); \
             {} group(s) skipped. Re-run with --yes to apply.",
            plan.destroy_count(),
            plan.groups.len(),
            plan.skipped.len(),
        );
    } else if let Some(outcome) = outcome {
        jma_mail::notify!(
            "Remote dedupe: destroyed {} id(s), {} failed, {} group(s) skipped.",
            outcome.succeeded(),
            outcome.failed.len(),
            plan.skipped.len(),
        );
        if !outcome.failed.is_empty() {
            let mut sorted: Vec<&jma_mail::ids::JmapEmailId> = outcome.failed.iter().collect();
            sorted.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
            for id in sorted {
                println!("  [REMOTE-DEDUPE-FAILED] {}", id);
            }
        }
    }
    Ok(())
}

async fn cmd_janitor_rebindfolders(cli: &Cli, sample_size: Option<u32>, apply: bool) -> Result<()> {
    let config = load_config(cli)?;
    // Same lock posture as remotededupe: rebindfolders hits the
    // network and can write sentinels, so the honest stance is
    // "this does not co-exist with sync." Acquire both.
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    let client = session::connect(&config.account, &conn).await?;
    let maildir_root = config.maildir_path();
    let n = sample_size
        .map(|n| n as usize)
        .unwrap_or(jma_mail::janitor::rebindfolders::DEFAULT_SAMPLE_SIZE);

    // Sentinel writes require explicit --apply; otherwise treat as
    // a dry run. --dry-run also forces dry; --apply is the only
    // affirmative gate so a probe that landed on the wrong mailbox
    // requires explicit acknowledgement before disk changes.
    let effective_dry_run = cli.dry_run || !apply;
    let plan =
        jma_mail::janitor::rebindfolders::run(&client, &conn, &maildir_root, n, effective_dry_run)
            .await?;

    render_rebindfolders_plan(&plan);

    if plan.candidates.is_empty() && plan.skipped.is_empty() {
        jma_mail::notify!("Rebindfolders: no orphan folders found.");
        return Ok(());
    }

    if cli.dry_run {
        jma_mail::notify!(
            "Rebindfolders (dry-run): would rebind {} folder(s); {} skipped.",
            plan.candidates.len(),
            plan.skipped.len(),
        );
    } else if !apply {
        jma_mail::notify!(
            "Rebindfolders: {} folder(s) eligible for rebind; {} skipped. Re-run with --apply to write sentinels.",
            plan.candidates.len(),
            plan.skipped.len(),
        );
    } else {
        jma_mail::notify!(
            "Rebindfolders: rebound {} folder(s); {} skipped.",
            plan.candidates.len(),
            plan.skipped.len(),
        );
    }
    Ok(())
}

fn render_rebindfolders_plan(plan: &jma_mail::janitor::rebindfolders::RebindFoldersPlan) {
    for c in &plan.candidates {
        println!(
            "  [REBIND] {} -> {} ({}, {} sample(s))",
            c.folder_path.display(),
            c.jmap_mailbox_id,
            c.server_name,
            c.sample_count,
        );
    }
    for s in &plan.skipped {
        let detail = match &s.reason {
            jma_mail::janitor::rebindfolders::SkipReason::NoMessageIds => {
                "no parseable Message-IDs".to_string()
            }
            jma_mail::janitor::rebindfolders::SkipReason::NoServerMatches => {
                "no server-side match for any sample".to_string()
            }
            jma_mail::janitor::rebindfolders::SkipReason::AmbiguousMailboxes(union) => {
                let names: Vec<&str> = union.iter().map(|id| id.as_ref()).collect();
                format!("ambiguous (candidates: {})", names.join(","))
            }
        };
        println!("  [REBIND-SKIP] {} -- {}", s.folder_path.display(), detail);
    }
}

fn render_remotededupe_plan(plan: &jma_mail::janitor::remotededupe::RemoteDedupePlan) {
    for g in &plan.groups {
        println!(
            "  [REMOTE-DEDUPE] {}/  Message-ID {}  blob {}  keep {}  destroy {}",
            g.mailbox_folder,
            g.message_id,
            g.blob_id,
            g.survivor,
            g.destroy
                .iter()
                .map(|id| id.as_ref())
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    for s in &plan.skipped {
        let members = s
            .members
            .iter()
            .map(|(id, blob)| format!("{}@{}", id, blob))
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "  [REMOTE-DEDUPE-SKIP] {}/  Message-ID {}  reason {:?}  members {}",
            s.mailbox_folder, s.message_id, s.reason, members,
        );
    }
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
