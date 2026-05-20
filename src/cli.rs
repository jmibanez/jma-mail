use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "jma",
    version = env!("JMA_VERSION"),
    about = "JM's Mail Agent: bidirectional JMAP-to-Maildir email sync"
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

    /// Show what would be done without making changes
    #[arg(short = 'n', long, global = true)]
    pub dry_run: bool,

    /// Suppress all output except errors
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Print profiling summary to stderr at end of run
    #[arg(long, global = true)]
    pub profile: bool,

    /// Write profiling summary as JSON to PATH
    #[arg(long, global = true, value_name = "PATH")]
    pub profile_json: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Clone)]
pub enum Command {
    /// Run bidirectional sync (default if no command given)
    Sync,
    /// One-way sync: server -> local only
    Pull,
    /// One-way sync: local -> server only
    Push,
    /// Daemon mode: continuous sync on server + local changes
    Watch,
    /// Initialize config file and local maildir structure
    Init,
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
    /// Scan and remove per-folder Message-ID duplicates
    Dedupe,
    /// Scan the server for per-mailbox Message-ID duplicates and
    /// destroy the extras via Email/set. Two-tier safety check:
    /// refuses groups whose members disagree on Email/get `size`
    /// (cheap pre-check) or, for groups passing size, on the byte-
    /// for-byte content of their downloaded blobs (forgery
    /// defense). Requires --yes to apply outside of --dry-run.
    Remotededupe {
        /// Limit the scan to one maildir folder name (as it
        /// appears in `mailbox_map.maildir_folder` -- e.g. INBOX,
        /// Archive, [Airmail].Sent). Defaults to every folder
        /// known to the state DB.
        #[arg(long, value_name = "FOLDER")]
        mailbox: Option<String>,
        /// Apply the destroy plan. Without this flag (and without
        /// --dry-run) the plan is printed and the command refuses
        /// to destroy anything.
        #[arg(long)]
        yes: bool,
    },
}
