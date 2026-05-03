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
    /// Email address for the account. The domain is used to discover
    /// the JMAP session URL via DNS SRV (`_jmap._tcp.<domain>`) and
    /// the `/.well-known/jmap` HTTPS endpoint.
    pub email: String,
    /// API token. Lowest-priority fallback after JMAPSYNC_TOKEN and
    /// the OS keychain (`jmapsync auth set-token`). Kept supported
    /// indefinitely for headless servers and CI where the keychain
    /// isn't available.
    pub token: Option<String>,
    /// Explicit JMAP session URL. When set, it bypasses autodiscovery
    /// and the discovery cache entirely. Leave unset (the default) to
    /// have the URL discovered from the email domain via DNS SRV /
    /// well-known and cached in the state DB.
    #[serde(default)]
    pub session_url: Option<String>,
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
    /// Resolve the bearer token. Order of precedence:
    ///
    /// 1. `JMAPSYNC_TOKEN` env var — explicit override; wins so CI
    ///    and scripted use can inject a token without touching
    ///    config or keychain.
    /// 2. OS keychain (`jmapsync auth set-token`) — preferred
    ///    interactive path; the token never lives on disk in the
    ///    clear.
    /// 3. `token` field in the config file — back-compat fallback
    ///    for headless servers and CI where the keychain isn't
    ///    available. Stays supported indefinitely.
    pub fn token(&self) -> Result<String> {
        if let Ok(t) = std::env::var("JMAPSYNC_TOKEN") {
            return Ok(t);
        }
        if let Some(t) = crate::auth::get_bearer_token() {
            return Ok(t);
        }
        if let Some(t) = self.token.as_deref().filter(|s| !s.is_empty()) {
            return Ok(t.to_string());
        }
        Err(anyhow::anyhow!(
            "no API token: run `jmapsync auth set-token` to store one in your OS keychain, \
             set the JMAPSYNC_TOKEN env var, or set `token` in the config file"
        ))
    }

    /// Domain part of `email`, used as the cache key for discovery
    /// and the input to `_jmap._tcp.<domain>` SRV lookups.
    pub fn email_domain(&self) -> Result<&str> {
        self.email
            .rsplit_once('@')
            .map(|(_, d)| d)
            .filter(|d| !d.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "[account].email is not a valid email address: {:?}",
                    self.email
                )
            })
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let path = expand_tilde(path);
        let contents = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let config: Config = toml::from_str(&contents).context("Failed to parse config file")?;
        if let Some(msg) = check_token_perms(&path, config.account.token.as_deref()) {
            return Err(anyhow::anyhow!(msg));
        }
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

/// If the config file holds a non-empty in-file `token` and is readable
/// by group or world on a Unix-like FS, return an error message naming
/// the file. `Config::load` propagates this as `Err(...)` so every
/// subcommand refuses to start until the user tightens permissions.
///
/// Returns `None` when:
///   - `token` is `None` or empty (nothing on disk to leak),
///   - the platform is non-Unix (mode bits don't apply; ACL story is
///     separate and not in scope here),
///   - the stat fails (don't escalate I/O hiccups into hard failures
///     when the operation isn't security-critical),
///   - permissions are already tight (`mode & 0o077 == 0`).
pub fn check_token_perms(path: &Path, token: Option<&str>) -> Option<String> {
    let has_token = token.is_some_and(|s| !s.is_empty());
    if !has_token {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(path).ok()?;
        if meta.mode() & 0o077 != 0 {
            return Some(format!(
                "config file {} is readable by group or others and contains a non-empty token; \
                 refusing to use it. Run `chmod 600 {}` (or move the token to JMAPSYNC_TOKEN \
                 / `jmapsync auth set-token`).",
                path.display(),
                path.display()
            ));
        }
        None
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// Expand ~ at the start of a path to the user's home directory.
pub fn expand_tilde(path: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    if (s.starts_with("~/") || s == "~")
        && let Some(home) = dirs::home_dir()
    {
        return home.join(&s[2..]);
    }
    path.to_path_buf()
}

/// Generate a default config file template.
pub fn default_config_template() -> &'static str {
    r#"[account]
# Email address for this account. The domain is used to discover the
# JMAP session URL via DNS SRV (_jmap._tcp.<domain>) and the
# /.well-known/jmap HTTPS endpoint, per RFC 8620 §2.2.
email = "you@example.com"
# API token (app-specific password). Generate at:
#   https://www.fastmail.com/settings/security/tokens
#
# Three ways to provide it, in priority order:
#   1. JMAPSYNC_TOKEN env var (best for CI / scripted use).
#   2. OS keychain — run `jmapsync auth set-token` to store the
#      token in macOS Keychain / Linux Secret Service / Windows
#      Credential Manager. Recommended for interactive use.
#   3. The `token` field below — fallback for headless servers
#      and CI where the keychain isn't available.
token = ""
# Explicit JMAP session URL. Leave unset to have it autodiscovered
# from the email domain (DNS SRV + /.well-known/jmap, RFC 8620
# section 2.2). The discovered URL is cached in the state DB. Set
# this only to override autodiscovery -- e.g. for a provider whose
# discovery records aren't published, or to force a specific
# endpoint during testing.
# session_url = "https://api.fastmail.com/jmap/session"

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

#[cfg(test)]
mod email_tests {
    use super::*;

    fn account(email: &str) -> AccountConfig {
        AccountConfig {
            email: email.to_string(),
            token: None,
            session_url: None,
        }
    }

    #[test]
    fn email_domain_extracts_after_at() {
        assert_eq!(
            account("user@example.com").email_domain().unwrap(),
            "example.com"
        );
    }

    #[test]
    fn email_domain_uses_rightmost_at_for_quoted_locals() {
        // RFC 5321 allows @ inside a quoted local-part; rsplit_once
        // takes the rightmost one which is what JMAP discovery needs.
        assert_eq!(
            account("\"weird@local\"@example.com")
                .email_domain()
                .unwrap(),
            "example.com"
        );
    }

    #[test]
    fn email_domain_rejects_missing_at() {
        assert!(account("not-an-email").email_domain().is_err());
    }

    #[test]
    fn email_domain_rejects_empty_domain() {
        assert!(account("user@").email_domain().is_err());
    }

    #[test]
    fn email_domain_accepts_empty_local_part() {
        // Pin current behavior: only the domain matters for
        // discovery, so an empty local-part is not rejected here.
        // Config-time email validity is the user's job.
        assert_eq!(
            account("@example.com").email_domain().unwrap(),
            "example.com"
        );
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn write_with_mode(path: &Path, mode: u32) {
        std::fs::write(path, b"x").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    #[test]
    fn check_token_perms_returns_none_when_mode_is_tight() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("config.toml");
        write_with_mode(&p, 0o600);
        assert!(check_token_perms(&p, Some("secret")).is_none());
    }

    #[test]
    fn check_token_perms_returns_some_when_world_or_group_readable() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("config.toml");
        write_with_mode(&p, 0o644);
        let msg = check_token_perms(&p, Some("secret"))
            .expect("loose perms with non-empty token must error");
        assert!(msg.contains("chmod 600"), "got: {msg}");
    }

    #[test]
    fn check_token_perms_ignores_loose_perms_when_token_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("config.toml");
        write_with_mode(&p, 0o644);
        assert!(check_token_perms(&p, Some("")).is_none());
    }

    #[test]
    fn check_token_perms_ignores_loose_perms_when_token_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("config.toml");
        write_with_mode(&p, 0o644);
        assert!(check_token_perms(&p, None).is_none());
    }
}
