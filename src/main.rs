use anyhow::{Context, Result};
use clap::Parser;
use jma_mail::maildir_ops::layout::FolderLayoutDefinition;
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

    let command = cli.command.clone().unwrap_or(Command::Sync {
        args: cli.sync.clone(),
    });
    // Capture the discriminant up front: the Auth match arm
    // partial-moves `account` out of `command`, so any later check
    // against `command` would otherwise fail to compile.
    let is_watch = matches!(command, Command::Watch);

    jma_mail::notify!("Running jma version {}", env!("JMA_VERSION"));

    let result = match command {
        Command::Init { no_interactive } => cmd_init(&cli, no_interactive).await,
        Command::Mailboxes => cmd_mailboxes(&cli).await,
        Command::Status => cmd_status(&cli).await,
        Command::Sync { args } => cmd_sync(&cli, args.dry_run).await,
        Command::Pull { dry_run } => cmd_pull(&cli, dry_run).await,
        Command::Push { dry_run } => cmd_push(&cli, dry_run).await,
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
            // TTY: hidden interactive prompt via dialoguer (same
            // library the first-run wizard uses). Non-TTY: read one
            // line from stdin so a scripted `echo $TOKEN | jma auth
            // set-token ...` invocation keeps working in CI.
            let token = if std::io::stdin().is_terminal() {
                dialoguer::Password::with_theme(&dialoguer::theme::ColorfulTheme::default())
                    .with_prompt("Bearer token")
                    .interact()
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

async fn cmd_init(cli: &Cli, no_interactive: bool) -> Result<()> {
    use std::io::IsTerminal;

    let config_path = config::expand_tilde(&cli.config);

    if config_path.exists() {
        println!("Config file already exists: {}", config_path.display());
    } else {
        // The wizard is the friendlier first-run path, but it
        // requires a TTY (dialoguer drives the prompts directly
        // against the terminal). Fall back to the template
        // automatically when stdin isn't a TTY -- this is the
        // case CI / scripted installs hit, and erroring there
        // would force every automated setup to remember a flag.
        // `--no-interactive` forces the template path even when a
        // TTY is available, for users who prefer hand-editing.
        let use_wizard = !no_interactive && std::io::stdin().is_terminal();
        if use_wizard {
            jma_mail::wizard::run(&config_path)?;
        } else {
            write_template_config(&config_path)?;
            println!("Created config file: {}", config_path.display());
            println!("Edit the config file and set your JMAP API token.");
            println!(
                "For Fastmail, generate one at: \
                 https://www.fastmail.com/settings/security/tokens"
            );
            println!(
                "Then run `jma sync` to provision your local maildirs \
                 and pull existing messages."
            );
        }
    }

    // Create the maildir root (not the per-folder tree) so the
    // per-maildir advisory lock has somewhere to live on the first
    // `jma sync`. The sync engine materializes one maildir per
    // server-known mailbox on its first cycle via `CreateLocalMailbox`.
    match Config::load(&config_path) {
        Ok(config) => create_maildir_root(&config)?,
        Err(e) => {
            println!("Skipping maildir root creation: could not load config ({e})");
        }
    }

    Ok(())
}

/// Create the maildir root so the per-maildir advisory lock has
/// somewhere to live when the user runs `jma sync`. Only the root
/// directory is created here; the per-folder maildir tree is
/// materialized on the first sync, one folder per server-known
/// mailbox, through plan-visible `CreateLocalMailbox` actions.
fn create_maildir_root(config: &Config) -> Result<()> {
    let root = config.maildir_path();
    if !root.exists() {
        std::fs::create_dir_all(&root)
            .with_context(|| format!("Failed to create maildir root {}", root.display()))?;
        println!("Created maildir root: {}", root.display());
    }
    Ok(())
}

/// Write the fully-commented default template to `config_path`.
/// Mode 0o600 from creation so a token added later isn't briefly
/// exposed under the user's umask; the load-time perms check (in
/// `config::check_token_perms`) catches files that already exist
/// with looser perms.
fn write_template_config(config_path: &std::path::Path) -> Result<()> {
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts
        .open(config_path)
        .with_context(|| format!("Failed to create config file: {}", config_path.display()))?;
    std::io::Write::write_all(&mut f, config::default_config_template().as_bytes())
        .with_context(|| format!("Failed to write config file: {}", config_path.display()))?;
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

    // Index by id so `resolve_folder_path` can compute the on-disk
    // name and `build_remote_paths` can resolve each server path.
    let by_id: std::collections::HashMap<_, _> =
        mailboxes.iter().map(|mb| (mb.id.clone(), mb)).collect();
    let name_cap = jma_mail::jmap::limits::max_size_mailbox_name(&client);

    // Mark which mailboxes the sync filter selects (exact matches plus
    // their subtrees), computed once over every server path.
    let remote_paths = jma_mail::jmap::mailbox::build_remote_paths(&by_id);
    let selection_inputs: Vec<jma_mail::jmap::mailbox::MailboxSelectionInput> = mailboxes
        .iter()
        .map(|mb| jma_mail::jmap::mailbox::MailboxSelectionInput {
            path: &remote_paths[&mb.id],
            role: mb.role.as_deref(),
        })
        .collect();
    let selected = jma_mail::jmap::mailbox::get_selected_mailboxes(
        &config.sync.mailboxes,
        &selection_inputs,
        config.sync.case_insensitive_match,
        true,
    );

    println!(
        "{:<40} {:>8} {:>8}  Role",
        "On-disk name", "Total", "Unread"
    );
    println!("{}", "-".repeat(70));
    let layout_definition = FolderLayoutDefinition::from_config(&config, name_cap);
    for mb in &mailboxes {
        let synced = if selected.contains(remote_paths[&mb.id].as_str()) {
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
    use jma_mail::maildir_ops::drift::{DriftReport, compute_drift};

    println!("Maildir vs DB drift (under {}):", maildir_root.display());

    match compute_drift(conn, maildir_root)? {
        DriftReport::MaildirRootMissing => {
            println!("  Maildir root does not exist -- nothing to compare.");
        }
        DriftReport::NoMailboxMap => {
            println!("  No mailbox map yet -- DB has no record of synced folders.");
        }
        DriftReport::Drift { only_disk, only_db } => {
            print_drift_line("  Folders on disk not in DB: ", &only_disk);
            print_drift_line("  Folders in DB not on disk: ", &only_db);
        }
    }

    Ok(())
}

fn print_drift_line(prefix: &str, items: &[String]) {
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

async fn cmd_sync(cli: &Cli, dry_run: bool) -> Result<()> {
    let config = load_config(cli)?;
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    SyncEngine::sync(&conn, &config, dry_run).await?;

    Ok(())
}

async fn cmd_pull(cli: &Cli, dry_run: bool) -> Result<()> {
    let config = load_config(cli)?;
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    SyncEngine::pull_only(&conn, &config, dry_run).await?;

    Ok(())
}

async fn cmd_push(cli: &Cli, dry_run: bool) -> Result<()> {
    let config = load_config(cli)?;
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    SyncEngine::push_only(&conn, &config, dry_run).await?;

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
    let action = action.unwrap_or(JanitorAction::Dedupe { apply: false });
    match action {
        JanitorAction::Dedupe { apply } => cmd_janitor_dedupe(cli, apply).await,
        JanitorAction::Remotededupe { mailbox, yes } => {
            cmd_janitor_remotededupe(cli, mailbox, yes).await
        }
        JanitorAction::Rebindfolders {
            sample_size,
            bind,
            apply,
        } => cmd_janitor_rebindfolders(cli, sample_size, bind, apply).await,
    }
}

async fn cmd_janitor_dedupe(cli: &Cli, apply: bool) -> Result<()> {
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
    let plan = jma_mail::janitor::dedupe::run(&maildir_root, &folders, !apply)?;

    if plan.deletions.is_empty() {
        jma_mail::notify!(
            "Dedupe: no duplicates found across {} folder(s).",
            folders.len()
        );
    } else if !apply {
        jma_mail::notify!(
            "Dedupe: {} duplicate file(s) eligible for removal; \
             re-run with --apply to delete:",
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

    // Refuse-by-default: --yes is the affirmative gate for a
    // destructive remote action. We still build the plan so the user
    // sees what *would* have been destroyed; we just skip the apply
    // step.
    let effective_dry_run = !yes;
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

    if !yes {
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

async fn cmd_janitor_rebindfolders(
    cli: &Cli,
    sample_size: Option<u32>,
    bind: Vec<String>,
    apply: bool,
) -> Result<()> {
    let config = load_config(cli)?;
    // Same lock posture as remotededupe: rebindfolders hits the
    // network and can write sentinels, so the honest stance is
    // "this does not co-exist with sync." Acquire both.
    acquire_mutator_locks(&config)?;
    let conn = state::db::open_or_recreate(&config.db_path())?;

    let client = session::connect(&config.account, &conn).await?;
    let maildir_root = config.maildir_path();
    let samples_per_group = sample_size
        .map(|n| n as usize)
        .unwrap_or(jma_mail::janitor::rebindfolders::DEFAULT_SAMPLE_SIZE);

    // Parse each --bind into (PathBuf, remote_path String). Split on
    // the LAST `=` to tolerate `=` in maildir paths. Mailbox names
    // per RFC 8621 section 2 may contain `=` (the spec only
    // forbids `/` and control characters), so a remote_path
    // containing `=` would misparse here -- the up-front validation
    // in `plan()` rejects misparsed paths with a clear error rather
    // than silently misbinding, which is the acceptable failure
    // mode.
    let parsed_bindings: Vec<(std::path::PathBuf, String)> = bind
        .iter()
        .map(|raw| {
            let (path, remote) = raw.rsplit_once('=').with_context(|| {
                format!("invalid --bind value {raw:?}; expected PATH=REMOTE_PATH")
            })?;
            Ok::<_, anyhow::Error>((std::path::PathBuf::from(path), remote.to_string()))
        })
        .collect::<Result<_>>()?;

    // Refuse repeat --bind on the same path. Collecting straight
    // into a HashMap would silently last-write-wins the second
    // entry, dropping the operator's first declaration without a
    // warning -- exactly the class of behavior the rest of --bind
    // is built to refuse. Two distinct --bind on the same path
    // is unambiguously a transcription error.
    let mut seen_paths: std::collections::HashSet<&std::path::PathBuf> =
        std::collections::HashSet::new();
    for (path, _) in &parsed_bindings {
        if !seen_paths.insert(path) {
            anyhow::bail!(
                "--bind {} given twice; specify each PATH at most once",
                path.display()
            );
        }
    }
    let explicit_bindings: std::collections::HashMap<std::path::PathBuf, String> =
        parsed_bindings.into_iter().collect();

    // Sentinel writes require explicit --apply; otherwise treat as
    // a dry run. --apply is the only affirmative gate so a probe that
    // landed on the wrong mailbox requires explicit acknowledgement
    // before disk changes.
    let effective_dry_run = !apply;
    let plan = jma_mail::janitor::rebindfolders::run(
        &client,
        &conn,
        &maildir_root,
        samples_per_group,
        explicit_bindings,
        effective_dry_run,
    )
    .await?;

    render_rebindfolders_plan(&plan);

    if plan.candidates.is_empty() && plan.skipped.is_empty() {
        jma_mail::notify!("Rebindfolders: no orphan folders found.");
        return Ok(());
    }

    if !apply {
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
        let via = match c.source {
            jma_mail::janitor::rebindfolders::ResolveSource::Consensus => {
                format!("consensus, {} sample(s)", c.sample_count)
            }
            jma_mail::janitor::rebindfolders::ResolveSource::CrossMapping => {
                "cross-mapping".to_string()
            }
            jma_mail::janitor::rebindfolders::ResolveSource::Explicit => "explicit".to_string(),
        };
        println!(
            "  [REBIND] {} -> {} ({via})",
            c.folder_path.display(),
            c.remote_path,
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
            jma_mail::janitor::rebindfolders::SkipReason::AmbiguousAcrossSamples { per_group } => {
                let groups: Vec<String> = per_group
                    .iter()
                    .enumerate()
                    .map(|(i, ids)| {
                        let labels: Vec<&str> = ids
                            .iter()
                            .map(|id| {
                                plan.remote_paths
                                    .get(id)
                                    .map(String::as_str)
                                    .unwrap_or_else(|| id.as_ref())
                            })
                            .collect();
                        format!("group {} -> {{{}}}", i, labels.join(","))
                    })
                    .collect();
                format!("ambiguous across samples ({})", groups.join("; "))
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
