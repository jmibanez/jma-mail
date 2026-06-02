use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use std::path::{Path, PathBuf};

use crate::maildir_ops::namespace;

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
///   `mailbox_map.maildir_folder`, every `<root>.join(folder)` call),
///   which is more than a layout helper should drag along.
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
    /// Whether destructive folder-level syncs are permitted, and
    /// in which direction(s). See [`AllowDestructiveFolderSync`]
    /// for the full semantics and the composition rules against
    /// `conflict_strategy`. Default `none` means no destructive
    /// folder syncs; cleanup of orphan maildirs goes through
    /// `jma janitor prune`.
    #[serde(default)]
    pub allow_destructive_folder_sync: AllowDestructiveFolderSync,
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

/// Whether jma is permitted to apply destructive folder-level
/// syncs, and in which direction(s).
///
/// "Destructive" here means deleting an on-disk maildir (when the
/// server-side mailbox is gone) or destroying a server-side
/// mailbox (when the local maildir is gone). The default `None`
/// preserves the orphan-and-rely-on-prune behavior: on a
/// server-side deletion the cache row drops from `mailbox_map`
/// but the on-disk maildir is left untouched, and the inverse
/// (locally-removed maildir whose server mailbox still exists)
/// leaves the server mailbox alone; `jma janitor prune` is the
/// explicit cleanup path for the resulting drift. Users who want
/// strict mirror semantics in one or both directions opt in.
///
/// Variants are named for what they permit doing (deleting the
/// local or remote side), not for the direction of propagation:
/// reading `allow_destructive_folder_sync = "delete-local"` in a
/// config makes the user's commitment ("I'm allowing jma to
/// delete local mailboxes") immediately obvious without having
/// to map a directional arrow back to an action.
///
/// When destructive is enabled in a direction and the losing
/// side has unsynced content (e.g. server says the mailbox is
/// gone but local has new local-only messages in it),
/// `ConflictStrategy` is the tiebreaker:
///   - `ServerWins`: proceed; the loser's unsynced content is
///     lost. Consistent with per-message ServerWins semantics.
///   - `LocalWins`: refuse the destructive op; preserve the
///     loser as an orphan; surface via drift.
///
/// `ask`-style interactive confirmation is deliberately not a
/// value here: jma runs unattended under `launchd`, `systemd`,
/// `cron`, and `jma watch`, where stdin isn't a terminal and a
/// prompt-bearing config value would silently change behavior
/// between CLI and daemon contexts. If interactive confirmation
/// is ever wanted, it belongs as a per-invocation CLI flag, not
/// a value of this knob.
#[derive(Debug, Deserialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AllowDestructiveFolderSync {
    /// Never apply destructive folder-level syncs in either
    /// direction. Orphan maildirs whose mailbox vanished
    /// server-side stay on disk; locally-removed maildirs whose
    /// mailbox still exists server-side stay server-side. Drift
    /// is surfaced via `jma status` and cleanup goes through
    /// `jma janitor prune`.
    #[default]
    None,
    /// Permit deleting the local maildir when the server-side
    /// mailbox is gone. Mirror server-side deletions to disk,
    /// subject to the ConflictStrategy tiebreaker if unsynced
    /// local mail is present. Local maildir removals are still
    /// ignored.
    DeleteLocal,
    /// Permit destroying the server-side mailbox when the local
    /// maildir is gone. Mirror local maildir removals to the
    /// server, subject to the ConflictStrategy tiebreaker if
    /// unsynced server-side mail is present. Server-side
    /// deletions are still left as orphans on disk.
    DeleteRemote,
    /// Both directions enabled.
    Both,
}

impl AllowDestructiveFolderSync {
    /// True iff the configured policy permits deleting a local
    /// maildir in response to a server-side mailbox deletion.
    pub fn allows_delete_local(&self) -> bool {
        matches!(self, Self::DeleteLocal | Self::Both)
    }

    /// True iff the configured policy permits destroying a
    /// server-side mailbox in response to a local maildir
    /// removal.
    pub fn allows_delete_remote(&self) -> bool {
        matches!(self, Self::DeleteRemote | Self::Both)
    }
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
    /// Advanced/debug knob; not surfaced in the README. Per-path
    /// debounce interval for filesystem events. The underlying
    /// `notify-debouncer-mini` emits a path's accumulated events
    /// once that path has been quiet this long. See DEVELOPMENT.md
    /// (Daemon internals -> Trigger pipeline) for the full layering
    /// and tuning rationale.
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
    /// Advanced/debug knob; not surfaced in the README. TTL for
    /// entries in the daemon's self-write cache (see DEVELOPMENT.md
    /// "Daemon internals -> Trigger pipeline" for the full
    /// rationale). When unset, the effective TTL is derived from
    /// `debounce_secs` and `coalesce_window_ms` -- see
    /// `effective_self_write_ttl`. Override only when an MUA's
    /// new/->cur/ promotion latency consistently exceeds the
    /// derived default (raise) or when debugging
    /// self-suppression (lower or 0 to effectively disable the
    /// cache).
    #[serde(default)]
    pub self_write_ttl_secs: Option<u64>,
    /// Shell command to run (via `sh -c`) after a sync cycle that
    /// downloaded new messages. Useful for triggering a mail indexer
    /// (mu, notmuch, etc.) once new files land in the maildir. The
    /// command runs asynchronously; if a sync completes while a prior
    /// invocation is still running, a single follow-up run is queued
    /// (multiple events coalesce into one). Only fires in `watch` mode.
    #[serde(default)]
    pub post_arrival_command: Option<String>,
    /// Number of times to retry `post_arrival_command` if it fails
    /// (non-zero exit, spawn failure, or wait error). Default 0 -- run
    /// once and log a warning on failure. Each retry runs synchronously
    /// inside the same logical hook invocation, so an in-flight retry
    /// chain still coalesces follow-up triggers into one queued run.
    /// Values above the cap (currently 10; see
    /// `daemon::hook::MAX_POST_ARRIVAL_RETRIES`) are clamped at startup
    /// so a steady-state failure can't block follow-up triggers
    /// indefinitely.
    #[serde(default)]
    pub post_arrival_command_retries: u32,
}

impl WatchConfig {
    /// Effective TTL for the self-write cache. If
    /// `self_write_ttl_secs` is set, use it verbatim (0 is a valid
    /// "effectively disable the cache" value).
    ///
    /// Otherwise default to `2 * (debounce_secs + coalesce_window_ms)`
    /// rounded to the nearest second, with a 1s floor. The 2x
    /// multiple is wide enough to cover a push-aware MUA noticing
    /// the new/ delivery and renaming it within the next debounce
    /// cycle; polling MUAs (Gnus, others on a manual-fetch
    /// schedule) may need to override upward.
    pub fn effective_self_write_ttl(&self) -> std::time::Duration {
        if let Some(secs) = self.self_write_ttl_secs {
            return std::time::Duration::from_secs(secs);
        }
        let total_ms = self.debounce_secs * 1000 + self.coalesce_window_ms;
        let secs = (total_ms * 2 + 500) / 1000;
        std::time::Duration::from_secs(secs.max(1))
    }
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
            self_write_ttl_secs: None,
            post_arrival_command: None,
            post_arrival_command_retries: 0,
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
        let contents = std::fs::read_to_string(&path).map_err(|e| {
            // A missing file at the default path is the first-run
            // case; point the user at `jma init`. Other IO errors
            // (permission denied, broken symlink) carry their own
            // message through anyhow's source chain.
            if e.kind() == std::io::ErrorKind::NotFound {
                anyhow::anyhow!(
                    "No config at {}. Run `jma init` to set one up \
                     (or pass --config to point at an existing file).",
                    path.display()
                )
            } else {
                anyhow::Error::new(e)
                    .context(format!("Failed to read config file: {}", path.display()))
            }
        })?;
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
            None => self.maildir_path().join(namespace::STATE_DB_FILENAME),
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

/// Inputs to `render_config_toml`. Both the unedited init template
/// and the wizard's filled-in output flow through the same template
/// body; only the substituted slots differ.
#[derive(Debug, Clone)]
pub struct ConfigTomlValues<'a> {
    pub email: &'a str,
    pub token: TomlTokenSlot<'a>,
    pub maildir_path: &'a str,
    /// Already-rendered TOML enum spelling: `"flat"`, `"maildir++"`, `"fs"`.
    pub folder_layout: &'a str,
    pub hierarchy_separator: char,
    /// Already-rendered: `"server-wins"` or `"local-wins"`.
    pub conflict_strategy: &'a str,
    /// Already-rendered: `"none"`, `"delete-local"`, `"delete-remote"`,
    /// or `"both"`.
    pub allow_destructive_folder_sync: &'a str,
}

/// How the `token = ...` slot in the rendered config is filled.
#[derive(Debug, Clone, Copy)]
pub enum TomlTokenSlot<'a> {
    /// `token = ""` placeholder. Used by `jma init` for an
    /// unedited template the user fills in later.
    Placeholder,
    /// No `token` line; substitute a note pointing the reader at
    /// the keychain entry the wizard already created.
    InKeychain,
    /// `token = "<value>"`. Used when the wizard had to fall back
    /// to in-file storage because the keychain rejected the write.
    InFile(&'a str),
}

/// Render the canonical config template with the given values
/// substituted into the answered slots. Single source of truth for
/// the on-disk shape -- the unedited `jma init` template and the
/// wizard's filled-in output share this template body so the two
/// can't drift.
pub fn render_config_toml(values: &ConfigTomlValues<'_>) -> String {
    let token_line = match values.token {
        TomlTokenSlot::Placeholder => "token = \"\"".to_string(),
        TomlTokenSlot::InKeychain => format!(
            "# Token is stored in your OS keychain under this account.\n\
             # Run `jma auth set-token --account {}` to rotate it; uncomment\n\
             # the line below and paste the token only if you need a\n\
             # headless / CI fallback that bypasses the keychain.\n\
             # token = \"\"",
            values.email
        ),
        TomlTokenSlot::InFile(t) => format!("token = {}", toml_basic_string(t)),
    };

    format!(
        r#"[account]
# Email address for this account. The domain is used to discover the
# JMAP session URL via DNS SRV (_jmap._tcp.<domain>) and the
# /.well-known/jmap HTTPS endpoint, per RFC 8620 section 2.2.
email = {email}
# API token (app-specific password) from your JMAP provider.
# For Fastmail, generate one at:
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
{token_line}
# Explicit JMAP session URL. Leave unset to have it autodiscovered
# from the email domain (DNS SRV + /.well-known/jmap, RFC 8620
# section 2.2). The discovered URL is cached in the state DB. Set
# this only to override autodiscovery -- e.g. for a provider whose
# discovery records aren't published, or to force a specific
# endpoint during testing.
# session_url = "https://api.example.org/jmap/session"

[sync]
# Root directory for local maildir storage
maildir_path = {maildir_path}
# Which mailboxes to sync. Omit (or set to []) to sync every server
# mailbox -- the right default for most users. Uncomment and curate
# this list only if you want a subset. The literal "INBOX" is a magic
# alias for whichever mailbox has the JMAP "inbox" role; other entries
# match the mailbox's name (case-sensitively unless
# case_insensitive_match is true).
# mailboxes = ["INBOX", "Archive", "Sent", "Drafts", "Trash"]
# Match `mailboxes` entries case-insensitively against server names.
case_insensitive_match = false
# On-disk layout for hierarchical mailboxes:
#   "flat"      -- mbsync Flatten=<sep>: <root>/parent.child/
#                  (default; matches what most existing setups expect)
#   "maildir++" -- Courier/Dovecot leading-dot: <root>/.parent.child/
#   "fs"        -- Dovecot LAYOUT=fs: <root>/parent/child/
# Single-level mailboxes look identical under "flat" and "fs"; the
# choice only matters once a server has nested folders.
folder_layout = "{folder_layout}"
# Hierarchy separator for "flat" and "maildir++" layouts. Ignored
# under "fs" (which always uses "/"). Single character; "/", "\", and
# NUL are rejected at runtime. Default "." matches mbsync and
# Dovecot/Courier conventions.
hierarchy_separator = "{separator}"
# Conflict resolution: server-wins or local-wins
conflict_strategy = "{conflict_strategy}"
# Whether jma may apply destructive folder-level syncs, and in which
# direction(s). "Destructive" means deleting an on-disk maildir when
# its server mailbox is gone, or destroying a server mailbox when its
# local maildir is gone. When the losing side has unsynced content,
# conflict_strategy is the tiebreaker: server-wins proceeds (and the
# content is lost); local-wins refuses and leaves an orphan.
#   "none"          -- never delete either side; orphans surface via
#                      `jma status` and cleanup goes through
#                      `jma janitor prune` (default)
#   "delete-local"  -- mirror server-side mailbox deletions to disk
#   "delete-remote" -- mirror local maildir removals to the server
#   "both"          -- both directions
allow_destructive_folder_sync = "{allow_destructive_folder_sync}"
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
# SSE ping interval (seconds)
ping_interval = 60
# Command to run (via `sh -c`) after a watch-mode sync that downloaded
# new messages. Runs asynchronously; overlapping events coalesce into
# a single follow-up run. Leave unset to disable.
# post_arrival_command = "mu index"
# Retry the command on failure (non-zero exit, spawn error) this many
# times before giving up on the trigger. Default 0.
# post_arrival_command_retries = 0
"#,
        email = toml_basic_string(values.email),
        token_line = token_line,
        maildir_path = toml_basic_string(values.maildir_path),
        folder_layout = values.folder_layout,
        separator = values.hierarchy_separator,
        conflict_strategy = values.conflict_strategy,
        allow_destructive_folder_sync = values.allow_destructive_folder_sync,
    )
}

/// Unedited config template written by `jma init`. The escape
/// hatch for users who want to fill in the file by hand instead of
/// going through the wizard -- typically headless servers, CI
/// seeds, or anyone curating an advanced config from scratch.
pub fn default_config_template() -> String {
    render_config_toml(&ConfigTomlValues {
        email: "you@example.com",
        token: TomlTokenSlot::Placeholder,
        maildir_path: "~/Mail",
        folder_layout: "flat",
        hierarchy_separator: '.',
        conflict_strategy: "server-wins",
        allow_destructive_folder_sync: "none",
    })
}

/// Escape a string for a TOML basic-string literal. Wraps the input
/// in double quotes and escapes `"` and `\`. We control the inputs
/// (template constants or wizard answers that were already
/// whitespace-trimmed and structurally validated), so a minimal
/// escape set is sufficient.
pub(crate) fn toml_basic_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod template_tests {
    use super::*;

    #[test]
    fn toml_basic_string_escapes_quotes_and_backslashes() {
        assert_eq!(toml_basic_string("simple"), "\"simple\"");
        assert_eq!(
            toml_basic_string("with \"quote\""),
            "\"with \\\"quote\\\"\""
        );
        assert_eq!(toml_basic_string("with\\slash"), "\"with\\\\slash\"");
    }

    /// `jma init`'s template must parse cleanly. A typo in the
    /// rendered TOML or a drift in `ConfigTomlValues` shouldn't
    /// hand users a config jma can't load.
    #[test]
    fn default_template_round_trips() {
        let toml = default_config_template();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, &toml).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let cfg = Config::load(&path).expect("default template must load");
        assert_eq!(cfg.account.email, "you@example.com");
        assert_eq!(cfg.sync.maildir_path, "~/Mail");
        assert_eq!(cfg.sync.folder_layout, FolderLayout::Flat);
        assert_eq!(
            cfg.sync.conflict_strategy as u8,
            ConflictStrategy::ServerWins as u8
        );
        // The template emits an active `allow_destructive_folder_sync`
        // line; pin that it is well-formed and parses to the safe
        // `none` default so a fresh config never deletes mail.
        assert_eq!(
            cfg.sync.allow_destructive_folder_sync,
            AllowDestructiveFolderSync::None
        );
        // Default template ships with `mailboxes` commented out so
        // the "sync everything" branch is the out-of-the-box
        // behaviour. Pin that against an accidental uncomment.
        assert!(
            cfg.sync.mailboxes.is_empty(),
            "default template must leave [sync].mailboxes empty (sync-all); got {:?}",
            cfg.sync.mailboxes
        );
    }
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

    /// Default WatchConfig (debounce=2s, coalesce=500ms) yields a
    /// derived self-write TTL of 5 seconds: 2 * (2000 + 500) ms,
    /// rounded to the nearest second. Pins the formula so a future
    /// tweak to either window doesn't silently shift the cache TTL
    /// off the value the trigger pipeline assumes.
    #[test]
    fn effective_self_write_ttl_default_is_2x_pipeline_rounded() {
        let cfg = WatchConfig::default();
        assert_eq!(cfg.effective_self_write_ttl().as_secs(), 5);
    }

    /// `allow_destructive_folder_sync` defaults to `none`. Pins
    /// the orphan-and-rely-on-prune contract so a future default
    /// change doesn't silently start deleting maildirs on existing
    /// configs.
    #[test]
    fn allow_destructive_folder_sync_defaults_to_none() {
        let cfg = SyncConfig::default();
        assert_eq!(
            cfg.allow_destructive_folder_sync,
            AllowDestructiveFolderSync::None
        );
        assert!(!cfg.allow_destructive_folder_sync.allows_delete_local());
        assert!(!cfg.allow_destructive_folder_sync.allows_delete_remote());
    }

    /// `allow_destructive_folder_sync` accepts the four documented
    /// kebab-case values from TOML. The accessors expose the
    /// per-direction permission booleans that downstream
    /// destructive-sync code branches on.
    #[test]
    fn allow_destructive_folder_sync_parses_each_value() {
        let cases = [
            ("none", AllowDestructiveFolderSync::None, false, false),
            (
                "delete-local",
                AllowDestructiveFolderSync::DeleteLocal,
                true,
                false,
            ),
            (
                "delete-remote",
                AllowDestructiveFolderSync::DeleteRemote,
                false,
                true,
            ),
            ("both", AllowDestructiveFolderSync::Both, true, true),
        ];
        for (literal, expected, delete_local, delete_remote) in cases {
            let toml = format!(
                r#"
maildir_path = "/tmp/mail"
allow_destructive_folder_sync = "{literal}"
"#
            );
            let parsed: SyncConfig = toml::from_str(&toml).expect("parse SyncConfig");
            assert_eq!(parsed.allow_destructive_folder_sync, expected);
            assert_eq!(
                parsed.allow_destructive_folder_sync.allows_delete_local(),
                delete_local,
                "delete_local for {literal}"
            );
            assert_eq!(
                parsed.allow_destructive_folder_sync.allows_delete_remote(),
                delete_remote,
                "delete_remote for {literal}"
            );
        }
    }

    /// An existing user's TOML that doesn't mention
    /// `allow_destructive_folder_sync` at all must keep parsing
    /// and resolve to `None`. Pins the `#[serde(default)]` on
    /// the field itself so a future refactor that drops the
    /// attribute (and would otherwise require every config to
    /// add a line) gets caught.
    #[test]
    fn allow_destructive_folder_sync_uses_default_when_absent_in_toml() {
        let toml = r#"maildir_path = "/tmp/mail""#;
        let parsed: SyncConfig = toml::from_str(toml).expect("parse SyncConfig");
        assert_eq!(
            parsed.allow_destructive_folder_sync,
            AllowDestructiveFolderSync::None
        );
    }

    /// An unknown value for `allow_destructive_folder_sync` fails
    /// parsing rather than silently degrading to a default --
    /// this is a security-relevant knob and a typo should not
    /// open or close the destructive path.
    #[test]
    fn allow_destructive_folder_sync_rejects_unknown_value() {
        let toml = r#"
maildir_path = "/tmp/mail"
allow_destructive_folder_sync = "yes-please"
"#;
        let err = toml::from_str::<SyncConfig>(toml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("allow_destructive_folder_sync") || msg.contains("yes-please"),
            "error should name the offending field or value, got: {msg}"
        );
    }

    /// Explicit override wins over the derived default. 0 is a
    /// valid override (effectively disables the cache).
    #[test]
    fn effective_self_write_ttl_override_wins() {
        let mut cfg = WatchConfig {
            self_write_ttl_secs: Some(60),
            ..Default::default()
        };
        assert_eq!(cfg.effective_self_write_ttl().as_secs(), 60);

        cfg.self_write_ttl_secs = Some(0);
        assert_eq!(cfg.effective_self_write_ttl().as_secs(), 0);
    }

    /// Bumping the trigger pipeline knobs shifts the derived TTL
    /// in lockstep. Pins the rounding behavior at a non-trivial
    /// boundary: debounce=3s, coalesce=700ms -> 2 * 3700 = 7400ms
    /// -> 7s (nearest).
    #[test]
    fn effective_self_write_ttl_tracks_pipeline_widening() {
        let cfg = WatchConfig {
            debounce_secs: 3,
            coalesce_window_ms: 700,
            ..Default::default()
        };
        assert_eq!(cfg.effective_self_write_ttl().as_secs(), 7);
    }

    /// Pathologically small pipeline (sub-second derived TTL)
    /// floors at 1s so a freshly-recorded entry can survive at
    /// least one fsevents tick.
    #[test]
    fn effective_self_write_ttl_floors_at_one_second() {
        let cfg = WatchConfig {
            debounce_secs: 0,
            coalesce_window_ms: 100,
            ..Default::default()
        };
        assert_eq!(cfg.effective_self_write_ttl().as_secs(), 1);
    }
}
