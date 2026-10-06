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
    Watch {
        /// Disable the watch-mode TUI (auto-disabled when stdout is not a tty)
        #[arg(long)]
        no_tui: bool,
    },
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
        /// Print one audit line per remote Email object, including
        /// JMAP/blob ids, size, received date, subject, and SHA-256.
        /// This downloads size-mismatch blobs too; it never changes
        /// which groups are eligible for deletion.
        #[arg(long)]
        details: bool,
        /// Also print a terminal-safe preview of each raw RFC 5322
        /// message. With no value, shows the first 16 KiB; specify a
        /// byte limit as --content-preview=65536. Implies --details.
        #[arg(
            long,
            value_name = "BYTES",
            num_args = 0..=1,
            default_missing_value = "16384",
            value_parser = parse_content_preview_bytes
        )]
        content_preview: Option<usize>,
        /// Apply the destroy plan. Without this flag the plan is
        /// printed and the command refuses to destroy anything.
        #[arg(long)]
        yes: bool,
    },
    /// Preview or destroy an explicit, externally reviewed list of
    /// JMAP Email ids. The file contains one id per line. Before any
    /// mutation, every id is checked against the state DB and the
    /// command refuses the entire operation if even one has a local
    /// message mapping. Without --yes, only the verified plan is
    /// printed.
    Destroyids {
        /// File containing one opaque JMAP Email id per line. Empty
        /// lines and lines beginning with '#' are ignored.
        #[arg(long, value_name = "PATH")]
        from: std::path::PathBuf,
        /// Apply the exact destroy list. Without this flag Fastmail
        /// is queried for verification but nothing is destroyed.
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
    /// Remediate the maildir/DB drift that `jma status` reports.
    Prune {
        /// Limit to one maildir folder.
        #[arg(long, value_name = "FOLDER")]
        mailbox: Option<String>,
        /// Limit to one drift side: disk or db.
        #[arg(long, value_enum)]
        only: Option<PruneOnly>,
        /// Apply the plan instead of only printing it.
        #[arg(long)]
        apply: bool,
        /// Allow removing a maildir that still has mail.
        #[arg(long)]
        force_non_empty: bool,
        /// Allow clearing rows for a folder still in [sync].mailboxes.
        #[arg(long)]
        force_in_config: bool,
    },
}

/// Which side of the maildir/DB drift `janitor prune` acts on.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum PruneOnly {
    /// Stray and dropped folders on disk.
    Disk,
    /// Stale DB rows whose maildir is gone.
    Db,
}

fn parse_content_preview_bytes(value: &str) -> Result<usize, String> {
    let bytes = value
        .parse::<usize>()
        .map_err(|_| format!("{value:?} is not a positive byte count"))?;
    if (1..=1_048_576).contains(&bytes) {
        Ok(bytes)
    } else {
        Err("content preview must be between 1 and 1048576 bytes".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remotededupe_args(args: &[&str]) -> (bool, Option<usize>, bool) {
        let cli = Cli::try_parse_from(
            ["jma", "janitor", "remotededupe"]
                .into_iter()
                .chain(args.iter().copied()),
        )
        .expect("CLI should parse");
        let Some(Command::Janitor {
            action:
                Some(JanitorAction::Remotededupe {
                    details,
                    content_preview,
                    yes,
                    ..
                }),
        }) = cli.command
        else {
            panic!("expected janitor remotededupe");
        };
        (details, content_preview, yes)
    }

    #[test]
    fn remotededupe_details_is_independent_of_deletion_gate() {
        assert_eq!(remotededupe_args(&["--details"]), (true, None, false));
    }

    #[test]
    fn remotededupe_content_preview_has_default_and_explicit_limits() {
        assert_eq!(
            remotededupe_args(&["--content-preview"]),
            (false, Some(16_384), false)
        );
        assert_eq!(
            remotededupe_args(&["--content-preview=4096"]),
            (false, Some(4096), false)
        );
    }

    #[test]
    fn remotededupe_content_preview_rejects_zero() {
        assert!(
            Cli::try_parse_from(["jma", "janitor", "remotededupe", "--content-preview=0",])
                .is_err()
        );
    }

    #[test]
    fn destroyids_requires_manifest_and_defaults_to_dry_run() {
        let cli =
            Cli::try_parse_from(["jma", "janitor", "destroyids", "--from", "reviewed-ids.txt"])
                .expect("destroyids CLI should parse");
        let Some(Command::Janitor {
            action: Some(JanitorAction::Destroyids { from, yes }),
        }) = cli.command
        else {
            panic!("expected janitor destroyids");
        };
        assert_eq!(from, std::path::PathBuf::from("reviewed-ids.txt"));
        assert!(!yes);
        assert!(
            Cli::try_parse_from(["jma", "janitor", "destroyids", "--yes"]).is_err(),
            "--from is mandatory"
        );
    }
}
