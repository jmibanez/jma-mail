use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "jmapsync", version, about = "Bidirectional JMAP-to-Maildir email sync")]
pub struct Cli {
    /// Config file path
    #[arg(short, long, global = true, default_value = "~/.config/jmapsync/config.toml")]
    pub config: PathBuf,

    /// Increase logging verbosity (-v, -vv, -vvv)
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Show what would be done without making changes
    #[arg(short = 'n', long, global = true)]
    pub dry_run: bool,

    /// Suppress all output except errors
    #[arg(short, long, global = true)]
    pub quiet: bool,

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
    /// Daemon mode: watch for push events + local changes, sync continuously
    Watch,
    /// Initialize config file and local maildir structure
    Init,
    /// Show sync state info
    Status,
    /// List remote mailboxes and their local mapping
    Mailboxes,
}
