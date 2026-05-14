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

    /// Print a profiling summary (phase timings, blob throughput,
    /// file-op counts, RSS deltas) to stderr at end of run.
    #[arg(long, global = true)]
    pub profile: bool,

    /// Write the profiling summary as JSON to PATH at end of run.
    /// In daemon mode, appends one NDJSON line per sync cycle.
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
