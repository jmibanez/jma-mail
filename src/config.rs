use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub account: AccountConfig,
    pub sync: SyncConfig,
    #[serde(default)]
    pub state: StateConfig,
    #[serde(default)]
    pub watch: WatchConfig,
}

#[derive(Debug, Deserialize)]
pub struct AccountConfig {
    /// API token. Falls back to JMAPSYNC_TOKEN env var if not set.
    pub token: Option<String>,
    /// JMAP session URL. Defaults to Fastmail.
    #[serde(default = "default_session_url")]
    pub session_url: String,
}

#[derive(Debug, Deserialize)]
pub struct SyncConfig {
    /// Root directory for local maildir storage
    pub maildir_path: String,
    /// Which mailboxes to sync. Empty means all.
    #[serde(default)]
    pub mailboxes: Vec<String>,
    /// Conflict resolution strategy
    #[serde(default)]
    pub conflict_strategy: ConflictStrategy,
    /// Max messages to fetch per sync run (0 = unlimited)
    #[serde(default)]
    pub max_messages: u64,
    /// If true, match `mailboxes` entries against server names case-insensitively.
    /// `INBOX` is always treated as an alias for the inbox role regardless.
    #[serde(default)]
    pub case_insensitive_match: bool,
    /// Max concurrent blob downloads during pull. Clamped at runtime to the
    /// server's advertised `maxConcurrentRequests`.
    #[serde(default = "default_download_concurrency")]
    pub download_concurrency: usize,
}

#[derive(Debug, Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum ConflictStrategy {
    #[default]
    ServerWins,
    LocalWins,
}

#[derive(Debug, Deserialize)]
pub struct StateConfig {
    /// Path to SQLite state database
    #[serde(default = "default_db_path")]
    pub db_path: String,
}

#[derive(Debug, Deserialize)]
pub struct WatchConfig {
    /// Debounce interval for local filesystem events (seconds)
    #[serde(default = "default_debounce_secs")]
    pub debounce_secs: u64,
    /// SSE ping interval (seconds)
    #[serde(default = "default_ping_interval")]
    pub ping_interval: u64,
    /// Shell command to run (via `sh -c`) after a sync cycle that
    /// downloaded new messages. Useful for triggering a mail indexer
    /// (mu, notmuch, etc.) once new files land in the maildir. The
    /// command runs asynchronously; if a sync completes while a prior
    /// invocation is still running, a single follow-up run is queued
    /// (multiple events coalesce into one). Only fires in `watch` mode.
    #[serde(default)]
    pub post_arrival_command: Option<String>,
}

fn default_session_url() -> String {
    "https://api.fastmail.com/jmap/session".to_string()
}

fn default_db_path() -> String {
    "~/.local/share/jmapsync/state.db".to_string()
}

fn default_debounce_secs() -> u64 {
    2
}

fn default_ping_interval() -> u64 {
    60
}

fn default_download_concurrency() -> usize {
    8
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            db_path: default_db_path(),
        }
    }
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            debounce_secs: default_debounce_secs(),
            ping_interval: default_ping_interval(),
            post_arrival_command: None,
        }
    }
}

impl AccountConfig {
    /// Get the token from config or JMAPSYNC_TOKEN env var.
    pub fn token(&self) -> Result<String> {
        if let Some(ref token) = self.token {
            Ok(token.clone())
        } else {
            std::env::var("JMAPSYNC_TOKEN")
                .context("No token in config and JMAPSYNC_TOKEN env var not set")
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let path = expand_tilde(path);
        let contents = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let config: Config = toml::from_str(&contents).context("Failed to parse config file")?;
        Ok(config)
    }

    /// Resolved maildir path with ~ expanded.
    pub fn maildir_path(&self) -> PathBuf {
        expand_tilde(Path::new(&self.sync.maildir_path))
    }

    /// Resolved state DB path with ~ expanded.
    pub fn db_path(&self) -> PathBuf {
        expand_tilde(Path::new(&self.state.db_path))
    }
}

/// Expand ~ at the start of a path to the user's home directory.
pub fn expand_tilde(path: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    if s.starts_with("~/") || s == "~" {
        if let Some(home) = dirs::home_dir() {
            return home.join(&s[2..]);
        }
    }
    path.to_path_buf()
}

/// Generate a default config file template.
pub fn default_config_template() -> &'static str {
    r#"[account]
# API token (app-specific password). Can also use JMAPSYNC_TOKEN env var.
# Generate at: https://www.fastmail.com/settings/security/tokens
token = ""
# JMAP session URL (default is Fastmail)
session_url = "https://api.fastmail.com/jmap/session"

[sync]
# Root directory for local maildir storage
maildir_path = "~/Mail/Fastmail"
# Which mailboxes to sync (empty = all). The literal "INBOX" is a magic
# alias for whichever mailbox has the JMAP "inbox" role; other entries
# match the mailbox's name (case-sensitively unless case_insensitive_match
# is true).
mailboxes = ["INBOX", "Archive", "Sent", "Drafts", "Trash"]
# Match `mailboxes` entries case-insensitively against server names.
case_insensitive_match = false
# Conflict resolution: server-wins or local-wins
conflict_strategy = "server-wins"
# Max messages per sync run (0 = unlimited)
max_messages = 0
# Max concurrent blob downloads during pull. Clamped to the server's
# advertised maxConcurrentRequests (Fastmail: 10).
download_concurrency = 8

[state]
# Path to SQLite state database
db_path = "~/.local/share/jmapsync/state.db"

[watch]
# Debounce interval for local filesystem events (seconds)
debounce_secs = 2
# SSE ping interval (seconds)
ping_interval = 60
# Command to run (via `sh -c`) after a watch-mode sync that downloaded
# new messages. Runs asynchronously; overlapping events coalesce into
# a single follow-up run. Leave unset to disable.
# post_arrival_command = "mu index"
"#
}
