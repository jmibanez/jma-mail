use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "jma",
    version = env!("JMA_VERSION"),
    about = "JM's Mail Agent: bidirectional JMAP-to-Maildir email sync",
    // Bare `jma` runs sync, so accept sync's flags at the top level
    // (`jma -n` == `jma sync -n`). args_conflicts_with_subcommands
    // keeps those flags mutually exclusive with an explicit
    // subcommand, so `jma -n pull` is rejected rather than ambiguous.
    args_conflicts_with_subcommands = true
)]
pub struct Cli {
    /// Config file path
    #[arg(
        short,
        long,
        global = true,
        default_value = "~/.config/jma/config.toml"
    )]
    pub config: PathBuf,

    /// Increase verbosity: -v info, -vv debug, -vvv all crates, -vvvv trace.
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Suppress all output except errors
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Print profiling summary to stderr at end of run
    #[arg(long, global = true)]
    pub profile: bool,

    /// Write profiling summary as JSON to PATH
    #[arg(long, global = true, value_name = "PATH")]
    pub profile_json: Option<PathBuf>,

    /// Options for the implicit bidirectional sync that runs when no
    /// subcommand is given (`jma` == `jma sync`).
    #[command(flatten)]
    pub sync: SyncArgs,

    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Flags for bidirectional sync, shared between the implicit `jma`
/// invocation and the explicit `jma sync` subcommand.
#[derive(Args, Clone)]
pub struct SyncArgs {
    /// Show what would be done without making changes
    #[arg(short = 'n', long)]
    pub dry_run: bool,
}

#[derive(Subcommand, Clone)]
pub enum Command {
    /// Run bidirectional sync (default if no command given)
    Sync {
        #[command(flatten)]
        args: SyncArgs,
    },
    /// One-way sync: server -> local only
    Pull {
        /// Show what would be done without making changes
        #[arg(short = 'n', long)]
        dry_run: bool,
    },
    /// One-way sync: local -> server only
    Push {
        /// Show what would be done without making changes
        #[arg(short = 'n', long)]
        dry_run: bool,
    },
    /// Daemon mode: continuous sync on server + local changes
    Watch,
    /// Initialize config file. Runs the interactive setup wizard by
    /// default; pass --no-interactive to write a fully-commented
    /// template config instead. Maildirs are created on the first
    /// sync (preview with `sync --dry-run`).
    Init {
        /// Skip the wizard and write the default fully-commented
        /// template config. Useful for headless deployments and any
        /// flow where you'd rather edit the file by hand.
        #[arg(long)]
        no_interactive: bool,
    },
    /// List remote mailboxes and their local mapping
    Mailboxes,
    /// Show sync staleness, cursor health, and maildir drift.
    Status,
    /// Manage account credentials and the JMAP discovery cache
    Auth {
        #[command(subcommand)]
        action: AuthAction,

        /// Account email to operate on
        #[arg(long, value_name = "EMAIL")]
        account: String,
    },
    /// Run maintenance tasks against the maildir tree and state DB
    Janitor {
        /// Specific task to run; omit for the safe-default set
        #[command(subcommand)]
        action: Option<JanitorAction>,
    },
}

#[derive(Subcommand, Clone)]
pub enum AuthAction {
    /// Read a bearer token from stdin and store in the OS keychain
    SetToken,
    /// Remove the bearer token from the OS keychain
    ClearToken,
    /// Re-run JMAP session URL autodiscovery
    Rediscover,
}

#[derive(Subcommand, Clone)]
pub enum JanitorAction {
    /// Scan and remove per-folder Message-ID duplicates. Default
    /// mode prints the duplicate plan without removing anything.
    /// Pass --apply to delete the redundant files.
    Dedupe {
        /// Apply deletions. Without this flag the duplicate plan is
        /// printed and disk stays untouched.
        #[arg(long)]
        apply: bool,
    },
    /// Scan the server for per-mailbox Message-ID duplicates and
    /// destroy the extras via Email/set. Two-tier safety check:
    /// refuses groups whose members disagree on Email/get `size`
    /// (cheap pre-check) or, for groups passing size, on the byte-
    /// for-byte content of their downloaded blobs (forgery
    /// defense). Requires --yes to apply; without it the plan is
    /// printed and nothing is destroyed.
    Remotededupe {
        /// Limit the scan to one maildir folder name (as it
        /// appears in `mailbox_map.maildir_folder` -- e.g. INBOX,
        /// Archive, [Airmail].Sent). Defaults to every folder
        /// known to the state DB.
        #[arg(long, value_name = "FOLDER")]
        mailbox: Option<String>,
        /// Apply the destroy plan. Without this flag the plan is
        /// printed and the command refuses to destroy anything.
        #[arg(long)]
        yes: bool,
    },
    /// Rebind sentinel-less maildir folders to their JMAP mailboxes
    /// by Message-ID probing. Default mode reports rebind
    /// candidates without writing. Pass --apply to write the
    /// `.jma.mapping` sentinel for each unambiguous candidate.
    Rebindfolders {
        /// Samples per consensus group (default 4; 3 groups, 12 total).
        #[arg(long, value_name = "N", value_parser = clap::value_parser!(u32).range(1..))]
        sample_size: Option<u32>,
        /// Bypass probe, bind `PATH` to `REMOTE_PATH`. Repeatable, disables cross-mapping.
        #[arg(long, value_name = "PATH=REMOTE_PATH")]
        bind: Vec<String>,
        /// Write the resolved sentinels. Without this flag the
        /// plan is printed and disk stays untouched.
        #[arg(long)]
        apply: bool,
    },
}
