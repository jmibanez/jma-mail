use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use jmapsync::cli::{Cli, Command};
use jmapsync::config::{self, Config};
use jmapsync::daemon;
use jmapsync::jmap::session;
use jmapsync::state;
use jmapsync::sync::engine;

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
        Command::Sync => cmd_sync(&cli).await,
        Command::Pull => cmd_pull(&cli).await,
        Command::Push => cmd_push(&cli).await,
        Command::Watch => cmd_watch(&cli).await,
        Command::Status => cmd_status(&cli).await,
    }
}

async fn cmd_init(cli: &Cli) -> Result<()> {
    let config_path = config::expand_tilde(&cli.config);

    if config_path.exists() {
        println!("Config file already exists: {}", config_path.display());
    } else {
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&config_path, config::default_config_template())?;
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
    println!(
        "Generate a token at: https://www.fastmail.com/settings/security/tokens"
    );

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
    let client = session::connect(&config.account).await?;

    let mailboxes = jmapsync::jmap::mailbox::get_all(&client).await?;

    println!("{:<40} {:>8} {:>8}  {}", "Name", "Total", "Unread", "Role");
    println!("{}", "-".repeat(70));
    for mb in &mailboxes {
        let synced = if jmapsync::jmap::mailbox::is_mailbox_synced(
            &config.sync.mailboxes,
            mb,
            config.sync.case_insensitive_match,
        ) {
            "*"
        } else {
            " "
        };
        println!(
            "{} {:<38} {:>8} {:>8}  {}",
            synced,
            mb.name,
            mb.total_emails,
            mb.unread_emails,
            mb.role.as_deref().unwrap_or("")
        );
    }
    println!("\n* = synced");

    Ok(())
}

async fn cmd_sync(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    let client = session::connect(&config.account).await?;
    let db_path = config.db_path();
    let conn = state::db::open(&db_path)?;

    engine::sync(&client, &conn, &config, cli.dry_run).await?;

    Ok(())
}

async fn cmd_pull(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    let client = session::connect(&config.account).await?;
    let db_path = config.db_path();
    let conn = state::db::open(&db_path)?;

    engine::pull_only(&client, &conn, &config).await?;

    Ok(())
}

async fn cmd_push(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    let client = session::connect(&config.account).await?;
    let db_path = config.db_path();
    let conn = state::db::open(&db_path)?;

    engine::push_only(&client, &conn, &config).await?;

    Ok(())
}

async fn cmd_watch(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    let client = session::connect(&config.account).await?;
    let db_path = config.db_path();
    let conn = state::db::open(&db_path)?;

    daemon::runner::run(&client, &conn, &config).await?;

    Ok(())
}

async fn cmd_status(cli: &Cli) -> Result<()> {
    let config = load_config(cli)?;
    let db_path = config.db_path();

    if !db_path.exists() {
        println!("No state database found. Run `jmapsync sync` first.");
        return Ok(());
    }

    let conn = state::db::open(&db_path)?;

    // Show mailbox info
    let mailboxes = jmapsync::state::queries::get_all_mailboxes(&conn)?;
    if mailboxes.is_empty() {
        println!("No mailboxes synced yet.");
        return Ok(());
    }

    println!("Synced mailboxes:");
    for mb in &mailboxes {
        let messages = jmapsync::state::queries::get_messages_by_folder(&conn, &mb.maildir_folder)?;
        println!("  {} ({} messages)", mb.maildir_folder, messages.len());
    }

    Ok(())
}

fn load_config(cli: &Cli) -> Result<Config> {
    Config::load(&cli.config).context("Failed to load config")
}
