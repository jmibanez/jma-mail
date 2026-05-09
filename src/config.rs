use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Deserialize)]
pub struct Config {
    pub account: AccountConfig,
    pub sync: SyncConfig,
    #[serde(default)]
    pub state: StateConfig,
    #[serde(default)]
    pub watch: WatchConfig,

    #[serde(default)]
    pub rename_rules: Vec<MaildirRenameRule>,

    /// Compiled rename rules; shouldn't be deserialized
    #[serde(skip)]
    pub compiled_rename_rules: Vec<CompiledRenameRule>,
}

#[derive(Debug, Default, Deserialize)]
pub struct AccountConfig {
    /// Email address for the account. The domain is used to discover
    /// the JMAP session URL via DNS SRV (`_jmap._tcp.<domain>`) and
    /// the `/.well-known/jmap` HTTPS endpoint.
    pub email: String,
    /// API token. Lowest-priority fallback after the OS keychain
    /// (`jma auth set-token --account <email>`). Kept supported
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

/// On-disk layout convention for a hierarchical mailbox tree.
///
/// - `Flat` -- mbsync's `Flatten=<sep>` convention. `parent/child`
///   becomes `<root>/parent.child/{cur,new,tmp}` with a user-chosen
///   separator (default `.`).
/// - `MaildirPP` -- the Courier/Dovecot convention defined in Sam
///   Varshavchik's Maildir++ extension. Flat shape: every synced
///   folder is prefixed with a single `.` at the root, so a parent
///   `[Airmail]` with child `Sent` becomes `<root>/.[Airmail].Sent/`.
///   The spec forbids names starting with `.` (would produce `..`)
///   so segments are validated against that.
///
///   INBOX placement under this layout is a deliberate jma
///   convention, not a spec-derived one. Sam Varshavchik's Maildir++
///   spec (README.maildirquota.html in Courier) doesn't address
///   INBOX -- INBOX is an IMAP/JMAP concept, and Maildir++ only
///   defines the dot-prefix folder convention. Dovecot's Maildir
///   docs likewise don't address INBOX placement under the default
///   layout (the documented "Without `DIRNAME`, INBOX will be stored
///   at `~/Maildir/{new,cur,tmp}/`" line is in the section scoped to
///   `LAYOUT=fs`, not Maildir++). With no authority to defer to we
///   pick the simple thing: under our `MaildirPP` layout INBOX maps
///   to `<root>/.INBOX/` just like every other folder. The
///   alternative -- INBOX as the maildir root, the way Courier and
///   Dovecot deployments commonly behave in practice -- would
///   require the rest of the pipeline to accept an empty
///   `maildir_folder` string (the SQL schema's `NOT NULL` on
///   `mailbox_map.maildir_folder`, every `<root>.join(folder)` call,
///   every `get_messages_by_folder` query), which is more than a
///   layout helper should drag along.
/// - `Fs` -- Dovecot's `LAYOUT=fs` convention. Hierarchy is materialised
///   as a recursive directory tree: `<root>/parent/child/{cur,new,tmp}`.
///   The Dovecot docs flag the obvious risk -- a mailbox literally
///   named `cur`/`new`/`tmp` collides with maildir internals -- so
///   segments equal to those names are rejected here. Leading-dot
///   segments are also rejected to avoid colliding with our own
///   `.jma.lock` / `.jma.db` markers at the root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FolderLayout {
    #[default]
    Flat,
    /// `maildir++` is the spelling users see in Dovecot/Courier docs;
    /// keep the config key matching that rather than serde's
    /// kebab-case fallback (`maildir-pp`).
    #[serde(rename = "maildir++")]
    MaildirPP,
    Fs,
}
#[derive(Debug, Default, Deserialize)]
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
    /// On-disk layout for hierarchical mailboxes. `flat` (default)
    /// follows mbsync's `Flatten=<sep>` convention -- nested folders
    /// become a single dotted directory at the maildir root.
    /// `maildir++` matches Courier/Dovecot's flat-with-leading-dot
    /// convention. `fs` matches Dovecot's `LAYOUT=fs` recursive
    /// directory tree. Single-level mailboxes look identical under
    /// `flat` and `fs`; the choice only matters once a server has
    /// nested folders.
    #[serde(default)]
    pub folder_layout: FolderLayout,
    /// Hierarchy separator for `flat` and `maildir++` layouts; ignored
    /// for `fs` (which always uses `/`). Default `.` matches mbsync
    /// and Dovecot/Courier's typical deployments. Pick a different
    /// character if your folder names commonly contain `.`. Must be a
    /// single character; `/`, `\`, and NUL are rejected at runtime.
    #[serde(default = "default_hierarchy_separator")]
    pub hierarchy_separator: char,
    /// Max concurrent blob downloads during pull. Clamped at runtime to the
    /// server's advertised `maxConcurrentRequests`.
    #[serde(default = "default_download_concurrency")]
    pub download_concurrency: usize,
    /// Max concurrent message uploads during push. Clamped at runtime to the
    /// server's advertised `maxConcurrentUpload`. Tune this down if your
    /// upstream bandwidth is constrained.
    #[serde(default = "default_upload_concurrency")]
    pub upload_concurrency: usize,
    /// Max attempts for a transient JMAP call (the original try plus
    /// retries). The default of 5 gives a wall-clock ceiling of about
    /// 15s with the other defaults.
    #[serde(default = "default_retry_max_attempts")]
    pub retry_max_attempts: u32,
    /// Initial exponential-backoff delay between retries, in
    /// milliseconds. Each subsequent retry doubles the delay until
    /// it hits `retry_max_backoff_ms`.
    #[serde(default = "default_retry_initial_backoff_ms")]
    pub retry_initial_backoff_ms: u64,
    /// Cap on the exponential backoff between retries, in milliseconds.
    /// Once reached, all subsequent retries wait this long.
    #[serde(default = "default_retry_max_backoff_ms")]
    pub retry_max_backoff_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum MaildirRenameRule {
    MapDirectly {
        source_folder_path: String,
        renamed_name: String,
    },
    Pattern {
        source_folder_pattern: String,
        rename_pattern: String,
    },
}

#[derive(Clone, Debug)]
pub enum CompiledRenameRule {
    MapDirectly {
        source_folder_path: String,
        renamed_name: String,
    },
    Pattern {
        source_folder_pattern: Regex,
        rename_pattern: String,
    },
}

impl CompiledRenameRule {
    pub fn compile_from_maildir_rename_rule(
        rename_rule: &MaildirRenameRule,
    ) -> Result<Self, anyhow::Error> {
        match rename_rule {
            MaildirRenameRule::MapDirectly {
                source_folder_path,
                renamed_name,
            } => Ok(CompiledRenameRule::MapDirectly {
                source_folder_path: source_folder_path.clone(),
                renamed_name: renamed_name.clone(),
            }),
            MaildirRenameRule::Pattern {
                source_folder_pattern,
                rename_pattern,
            } => Ok(CompiledRenameRule::Pattern {
                source_folder_pattern: Regex::new(source_folder_pattern)?,
                rename_pattern: rename_pattern.clone(),
            }),
        }
    }
}

#[derive(Debug, Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum ConflictStrategy {
    #[default]
    ServerWins,
    LocalWins,
}

#[derive(Debug, Deserialize, Default)]
pub struct StateConfig {
    /// Path to SQLite state database. When unset, the DB lives at
    /// `<maildir_path>/.jma.db` so its lifetime tracks the
    /// maildir it describes -- moving, copying, or deleting the
    /// maildir keeps state and data in sync, and multiple accounts
    /// each get their own DB without coordinating a separate path.
    /// Set this to override the default (e.g. to keep state on a
    /// local-only path when the maildir lives on a synced volume).
    #[serde(default)]
    pub db_path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WatchConfig {
    /// Debounce interval for local filesystem events (seconds)
    #[serde(default = "default_debounce_secs")]
    pub debounce_secs: u64,
    /// SSE ping interval (seconds)
    #[serde(default = "default_ping_interval")]
    pub ping_interval: u64,
    /// Advanced/debug knob; not surfaced in the README. How long the
    /// trigger loop waits after a sync trigger lands before starting
    /// the cycle, absorbing any further triggers that arrive in the
    /// window so back-to-back debouncer batches collapse into one
    /// cycle. Each new trigger resets the timer. 500ms is enough for
    /// the common case; only needs raising in odd situations (very
    /// chatty MUA, high FS event latency, debugging a split-batch
    /// corner case).
    #[serde(default = "default_coalesce_window_ms")]
    pub coalesce_window_ms: u64,
    /// Shell command to run (via `sh -c`) after a sync cycle that
    /// downloaded new messages. Useful for triggering a mail indexer
    /// (mu, notmuch, etc.) once new files land in the maildir. The
    /// command runs asynchronously; if a sync completes while a prior
    /// invocation is still running, a single follow-up run is queued
    /// (multiple events coalesce into one). Only fires in `watch` mode.
    #[serde(default)]
    pub post_arrival_command: Option<String>,
}

fn default_debounce_secs() -> u64 {
    2
}

fn default_ping_interval() -> u64 {
    60
}

fn default_coalesce_window_ms() -> u64 {
    500
}

fn default_download_concurrency() -> usize {
    8
}

fn default_upload_concurrency() -> usize {
    8
}

fn default_retry_max_attempts() -> u32 {
    5
}

fn default_retry_initial_backoff_ms() -> u64 {
    500
}

fn default_retry_max_backoff_ms() -> u64 {
    8_000
}

fn default_hierarchy_separator() -> char {
    '.'
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            debounce_secs: default_debounce_secs(),
            ping_interval: default_ping_interval(),
            coalesce_window_ms: default_coalesce_window_ms(),
            post_arrival_command: None,
        }
    }
}

impl AccountConfig {
    /// Resolve the bearer token. Order of precedence:
    ///
    /// 1. OS keychain entry for this account's email
    ///    (`jma auth set-token --account <email>`) -- preferred
    ///    interactive path; the token never lives on disk in the
    ///    clear, and per-email scoping keeps multi-account configs
    ///    from sharing or overwriting each other's credentials.
    /// 2. `token` field in the config file -- fallback for headless
    ///    servers and CI where the keychain isn't available.
    pub fn token(&self) -> Result<String> {
        if let Some(t) = crate::auth::get_bearer_token(&self.email) {
            return Ok(t);
        }
        if let Some(t) = self.token.as_deref().filter(|s| !s.is_empty()) {
            return Ok(t.to_string());
        }
        Err(anyhow::anyhow!(
            "no API token for {}: run `jma auth set-token --account {}` to store one \
             in your OS keychain, or set `token` under that account in the config file",
            self.email,
            self.email,
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
        let mut config: Config =
            toml::from_str(&contents).context("Failed to parse config file")?;
        if let Some(msg) = check_token_perms(&path, config.account.token.as_deref()) {
            return Err(anyhow::anyhow!(msg));
        }

        check_valid_layout_rule(&config.sync.folder_layout, config.sync.hierarchy_separator)?;
        config.compiled_rename_rules = compile_maildir_rename_rules(&config.rename_rules)?;
        Ok(config)
    }

    /// Resolved maildir path with ~ expanded.
    pub fn maildir_path(&self) -> PathBuf {
        expand_tilde(Path::new(&self.sync.maildir_path))
    }

    /// Resolved state DB path. Returns the explicit `[state].db_path`
    /// override (with `~` expanded) if set, otherwise the default
    /// `<maildir_path>/.jma.db` next to the maildir it describes.
    pub fn db_path(&self) -> PathBuf {
        match self.state.db_path.as_deref() {
            Some(p) => expand_tilde(Path::new(p)),
            None => self.maildir_path().join(".jma.db"),
        }
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
                 refusing to use it. Run `chmod 600 {}` (or move the token to your OS \
                 keychain via `jma auth set-token --account <email>`).",
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

/// Check whether the layout type and hierarchy separator is valid.
/// For instance, if the user specifies layout = "flat" but a
/// hierarchy_separator = "/" on macOS, we error out since that
/// clashes with the system path separator and will create
/// subdirectories
pub fn check_valid_layout_rule(layout: &FolderLayout, separator: char) -> Result<()> {
    // Fs joins with `/` and ignores `separator`, so the collision
    // check only applies to layouts that splice the separator into
    // the on-disk name.
    if matches!(layout, FolderLayout::Flat | FolderLayout::MaildirPP)
        && (separator == '/' || separator == '\\' || separator == '\0')
    {
        anyhow::bail!(
            "hierarchy separator {:?} would collide with filesystem path syntax",
            separator
        );
    }

    Ok(())
}

/// Compile all maildir rename rules. Compilation involves compiling
/// regular expressions in MaildirRenameRule::Pattern rules into
/// regex::Regex; ::MapDirectly rules are just copied over. As a side
/// effect, invalid rules error out and give a diagnostic to the user.
pub fn compile_maildir_rename_rules(
    rename_rules: &[MaildirRenameRule],
) -> Result<Vec<CompiledRenameRule>, anyhow::Error> {
    rename_rules
        .iter()
        .map(CompiledRenameRule::compile_from_maildir_rename_rule)
        .collect()
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
# Two ways to provide it, in priority order:
#   1. OS keychain -- run `jma auth set-token --account <email>`
#      to store the token under this account's email in the macOS
#      Keychain / Linux Secret Service / Windows Credential Manager.
#      Recommended for interactive use; per-account scoping keeps
#      multi-account configs from sharing credentials.
#   2. The `token` field below -- fallback for headless servers and
#      CI where the keychain isn't available.
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
# On-disk layout for hierarchical mailboxes:
#   "flat"      -- mbsync Flatten=<sep>: <root>/parent.child/
#                  (default; matches what most existing setups expect)
#   "maildir++" -- Courier/Dovecot leading-dot: <root>/.parent.child/
#   "fs"        -- Dovecot LAYOUT=fs: <root>/parent/child/
# Single-level mailboxes look identical under "flat" and "fs"; the
# choice only matters once a server has nested folders.
folder_layout = "flat"
# Hierarchy separator for "flat" and "maildir++" layouts. Ignored
# under "fs" (which always uses "/"). Single character; "/", "\", and
# NUL are rejected at runtime. Default "." matches mbsync and
# Dovecot/Courier conventions.
hierarchy_separator = "."
# Conflict resolution: server-wins or local-wins
conflict_strategy = "server-wins"
# Max concurrent blob downloads during pull. Clamped to the server's
# advertised maxConcurrentRequests (Fastmail: 10).
download_concurrency = 8
# Max concurrent message uploads during push. Clamped to the server's
# advertised maxConcurrentUpload. Tune down if your upstream bandwidth
# is constrained.
upload_concurrency = 8
# JMAP retry tuning. The default sequence is roughly 500ms -> 1s -> 2s
# -> 4s -> 8s, giving up after 5 total attempts (~15s wall-clock).
# Drop max_attempts to 1 to disable retries; raise initial_backoff_ms
# on flaky links to give the server more breathing room.
retry_max_attempts = 5
retry_initial_backoff_ms = 500
retry_max_backoff_ms = 8000

# If you want to change how your mailboxes are mapped to your Maildirs,
# add one or more of these [[rename_rules]] tables
#
# For renaming literally, use type = "map-directly"
# [[rename_rules]]
# type = "map-directly"
# source_folder_path = "[Gmail]/Sent"
# renamed_name = "GmailSent"
#
# If you want to match against a regular expression, use type = "pattern"
# [[rename_rules]]
# type = "pattern"
# source_folder_pattern = "\\[Gmail\\]/(.*)"
# rename_pattern = "Gmail.\\1"
#
# To make it easier to type regular expression, use single quotes in the patterns
# [[rename_rules]]
# type = "pattern"
# source_folder_pattern = '\[Gmail\]/(.*)'
# rename_pattern = 'Gmail.\1'
#

[state]
# Path to SQLite state database. By default this is
# `<sync.maildir_path>/.jma.db` -- a hidden file at the maildir
# root, so state and data move together. Uncomment and set this only
# to override that default (e.g. to keep state on a local-only path
# when the maildir lives on a synced or networked volume).
# db_path = "~/.local/share/jma/state.db"

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

    #[test]
    fn compile_maildir_rename_rules_permits_valid_regex() {
        let rules = vec![MaildirRenameRule::Pattern {
            source_folder_pattern: r"[Gmail]\.(.*)".to_string(),
            rename_pattern: r"Gmail.\1".to_string(),
        }];
        assert!(compile_maildir_rename_rules(&rules).is_ok());
    }

    #[test]
    fn compile_maildir_rename_rules_error_on_invalid_regex() {
        let rules = vec![MaildirRenameRule::Pattern {
            source_folder_pattern: r"[Gmail].(".to_string(),
            rename_pattern: r"Gmail.\1".to_string(),
        }];
        assert!(compile_maildir_rename_rules(&rules).is_err());
    }

    /// A separator that would itself inject a path component is
    /// refused at config load time.
    #[test]
    fn check_valid_layout_rule_rejects_separator_that_collides_with_path_syntax() {
        for layout in [FolderLayout::Flat, FolderLayout::MaildirPP] {
            for bad in ['/', '\\', '\0'] {
                let err = check_valid_layout_rule(&layout, bad).unwrap_err();
                let msg = format!("{}", err);
                assert!(msg.contains("collide"), "for {bad:?}: got {msg}");
            }
        }
    }
}
