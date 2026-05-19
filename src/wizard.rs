//! First-run config wizard.
//!
//! Triggered when a subcommand needs a config and none exists at the
//! configured path. Walks the user through the small set of values
//! required to reach a working sync (email, API token, maildir path,
//! folder layout, conflict strategy) using `dialoguer` prompts so
//! the choice fields get an arrow-key chooser instead of a typed
//! response. Writes the resulting TOML to disk with 0600 perms, and
//! -- if the keychain accepts the token -- stores the bearer under
//! the account's email instead of leaking it into the file.
//!
//! Every other knob in the config (mailbox filters, rename rules,
//! retry tuning, watch hooks) is emitted as a commented-out
//! template line via `config::render_config_toml`, so the user has
//! the full schema in front of them after the wizard exits and can
//! uncomment whatever they want to change.

use anyhow::{Context, Result};
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Input, Password, Select};
use std::io::Write;
use std::path::Path;

use crate::auth;
use crate::config::{self, ConfigTomlValues, ConflictStrategy, FolderLayout, TomlTokenSlot};

/// Run the wizard against the controlling TTY and write the
/// resulting config to `config_path`.
///
/// **Precondition**: the caller must have already confirmed stdin
/// is a TTY (and that the user didn't pass `--no-interactive`). The
/// only legitimate caller is `cmd_init` in the binary; it routes
/// non-TTY and explicit-`--no-interactive` paths to the template
/// writer instead. Calling this on a non-TTY will fail somewhere
/// inside `dialoguer` with a less actionable message than that
/// gate produces.
pub fn run(config_path: &Path) -> Result<()> {
    print_welcome(config_path);
    let answers = collect_answers()?;
    let in_file_token = persist_token(&answers)?;
    let rendered = render_config(&answers, in_file_token.as_deref());
    write_config_file(config_path, &rendered)?;

    println!();
    println!("Wrote config to {}.", config_path.display());
    if in_file_token.is_some() {
        println!(
            "Token saved in the config file (keychain write failed). \
             Move it to your OS keychain later with `jma auth set-token --account {}`.",
            answers.email
        );
    } else {
        match &answers.token {
            TokenAnswer::Provided(_) => println!("Token saved to your OS keychain."),
            TokenAnswer::AlreadyInKeychain => {
                println!(
                    "Reusing the existing OS keychain token for {}.",
                    answers.email
                )
            }
            TokenAnswer::Skipped => println!(
                "No token stored. Run `jma auth set-token --account {}` before your first sync.",
                answers.email
            ),
        }
    }
    println!();
    println!("Run `jma sync` to provision your local maildirs and pull existing messages.");
    Ok(())
}

/// Answers collected from the user, before any side-effects (token
/// storage, file write) run. Kept as a plain struct so the prompt
/// pass and the persistence pass stay separable for testing.
#[derive(Debug, Clone)]
struct WizardAnswers {
    email: String,
    token: TokenAnswer,
    maildir_path: String,
    folder_layout: FolderLayout,
    conflict_strategy: ConflictStrategy,
}

/// What the wizard found out about the bearer token for this email.
/// The three states are mutually exclusive and decide both whether
/// we need to write the keychain and how the `token = ...` slot in
/// the rendered TOML is filled.
#[derive(Debug, Clone)]
enum TokenAnswer {
    /// The user typed a token at the prompt. We still need to store
    /// it (keychain first, in-file fallback on a keychain failure).
    Provided(String),
    /// A token for this email was already present in the OS keychain
    /// when the wizard started -- typically because the user is
    /// bootstrapping on a new machine whose keychain syncs from
    /// another one they already configured. Skip the token prompt
    /// and reuse the existing entry.
    AlreadyInKeychain,
    /// The user left the prompt blank. No token is stored anywhere;
    /// the rendered config gets a `token = ""` placeholder and the
    /// user is reminded to run `jma auth set-token` later.
    Skipped,
}

fn print_welcome(config_path: &Path) {
    println!();
    println!(
        "Welcome to jma. No config found at {}.",
        config_path.display()
    );
    println!("Let's set one up. Use the arrow keys for the choice prompts,");
    println!("press Enter to accept the highlighted default, or just type");
    println!("your answer for the text prompts.");
    println!();
}

fn collect_answers() -> Result<WizardAnswers> {
    let theme = ColorfulTheme::default();

    let email: String = Input::with_theme(&theme)
        .with_prompt("Email address")
        .validate_with(|s: &String| -> std::result::Result<(), String> { validate_email(s) })
        .interact_text()
        .context("Failed to read email from prompt")?;

    println!();
    // `auth::get_bearer_token` returns `None` for both "no entry"
    // and "backend unreachable" (it logs at debug on the latter).
    // We can't tell them apart here, so a transient keychain
    // outage at wizard time falls through to the prompt. The
    // failure mode is recoverable: if the user later runs jma
    // when the keychain is back, `AccountConfig::token()`
    // consults the keychain first, so a real entry that
    // materialises later wins over any in-file fallback.
    let token = if auth::get_bearer_token(&email).is_some() {
        // Common path for users bootstrapping a second machine
        // whose keychain has already synced this account's entry
        // (iCloud Keychain, Bitwarden / 1Password Secret Service
        // integration, etc.). Reuse the existing entry instead of
        // making them paste the token again.
        println!(
            "Found an existing OS keychain entry for {}; reusing it.",
            email
        );
        println!(
            "(To replace it, exit and run `jma auth set-token --account {}`.)",
            email
        );
        TokenAnswer::AlreadyInKeychain
    } else {
        println!("API token. For Fastmail, generate one at:");
        println!("  https://www.fastmail.com/settings/security/tokens");
        println!("Input is hidden. Leave blank to skip and set it later via");
        println!("`jma auth set-token --account {}`.", email);
        let token_raw = Password::with_theme(&theme)
            .with_prompt("Bearer token")
            .allow_empty_password(true)
            .interact()
            .context("Failed to read token from prompt")?;
        let trimmed = token_raw.trim();
        if trimmed.is_empty() {
            TokenAnswer::Skipped
        } else {
            TokenAnswer::Provided(trimmed.to_string())
        }
    };

    println!();
    let maildir_path: String = Input::with_theme(&theme)
        .with_prompt("Maildir path")
        .default("~/Maildir".to_string())
        .validate_with(|s: &String| -> std::result::Result<(), &'static str> {
            if s.trim().is_empty() {
                Err("Maildir path cannot be empty.")
            } else {
                Ok(())
            }
        })
        .interact_text()
        .context("Failed to read maildir path from prompt")?;

    println!();
    let layout_labels = [
        "flat       -- mbsync-style <root>/parent.child/  (recommended)",
        "maildir++  -- Courier/Dovecot leading-dot <root>/.parent.child/",
        "fs         -- Dovecot LAYOUT=fs recursive tree",
    ];
    let layout_idx = Select::with_theme(&theme)
        .with_prompt("Folder layout")
        .items(&layout_labels)
        .default(0)
        .interact()
        .context("Failed to read folder layout from prompt")?;
    let folder_layout = match layout_idx {
        0 => FolderLayout::Flat,
        1 => FolderLayout::MaildirPP,
        2 => FolderLayout::Fs,
        _ => unreachable!("Select returned an out-of-range index"),
    };

    println!();
    let conflict_labels = [
        "server-wins -- the server's view wins on a flag/delete conflict",
        "local-wins  -- your local change wins on a flag/delete conflict",
    ];
    let conflict_idx = Select::with_theme(&theme)
        .with_prompt("Conflict strategy")
        .items(&conflict_labels)
        .default(0)
        .interact()
        .context("Failed to read conflict strategy from prompt")?;
    let conflict_strategy = match conflict_idx {
        0 => ConflictStrategy::ServerWins,
        1 => ConflictStrategy::LocalWins,
        _ => unreachable!("Select returned an out-of-range index"),
    };

    Ok(WizardAnswers {
        email,
        token,
        maildir_path: maildir_path.trim().to_string(),
        folder_layout,
        conflict_strategy,
    })
}

/// Apply the side-effect implied by `answers.token`:
/// `Provided` writes the keychain (falling back to the config file
/// if the keychain rejects the write); `AlreadyInKeychain` is a
/// no-op because the entry is already there; `Skipped` is a no-op
/// because there's nothing to store. Returns `Some(token)` only on
/// the in-file fallback path, so the caller can render it into the
/// TOML.
fn persist_token(answers: &WizardAnswers) -> Result<Option<String>> {
    match &answers.token {
        TokenAnswer::Provided(token) => match auth::set_bearer_token(&answers.email, token) {
            Ok(()) => Ok(None),
            Err(e) => {
                eprintln!(
                    "Warning: could not store the token in your OS keychain ({:#}). \
                     Falling back to the config file.",
                    e
                );
                Ok(Some(token.clone()))
            }
        },
        TokenAnswer::AlreadyInKeychain | TokenAnswer::Skipped => Ok(None),
    }
}

fn write_config_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create config directory: {}", parent.display()))?;
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts
        .open(path)
        .with_context(|| format!("Failed to create config file: {}", path.display()))?;
    f.write_all(contents.as_bytes())
        .with_context(|| format!("Failed to write config file: {}", path.display()))?;
    Ok(())
}

/// Render the wizard's answers into the canonical config template,
/// substituting the answered slots and choosing how the `token =
/// ...` line is filled based on which `TokenAnswer` branch we
/// took. The `hierarchy_separator` field is hardcoded to the
/// template's default of `.` -- the wizard doesn't prompt for it
/// since the default is the right call for `flat` / `maildir++`
/// users and ignored under `fs`; users who need a different
/// separator can change it in the rendered file.
fn render_config(answers: &WizardAnswers, in_file_token: Option<&str>) -> String {
    let token = match (in_file_token, &answers.token) {
        (Some(t), _) => TomlTokenSlot::InFile(t),
        (None, TokenAnswer::Provided(_) | TokenAnswer::AlreadyInKeychain) => {
            TomlTokenSlot::InKeychain
        }
        (None, TokenAnswer::Skipped) => TomlTokenSlot::Placeholder,
    };
    let folder_layout = match answers.folder_layout {
        FolderLayout::Flat => "flat",
        FolderLayout::MaildirPP => "maildir++",
        FolderLayout::Fs => "fs",
    };
    let conflict_strategy = match answers.conflict_strategy {
        ConflictStrategy::ServerWins => "server-wins",
        ConflictStrategy::LocalWins => "local-wins",
    };
    config::render_config_toml(&ConfigTomlValues {
        email: &answers.email,
        token,
        maildir_path: &answers.maildir_path,
        folder_layout,
        hierarchy_separator: '.',
        conflict_strategy,
    })
}

fn validate_email(s: &str) -> std::result::Result<(), String> {
    let Some((local, domain)) = s.rsplit_once('@') else {
        return Err(format!("{s:?} is not a valid email address (missing '@')"));
    };
    if local.is_empty() {
        return Err(format!("{s:?} has no local part before the '@'"));
    }
    if domain.is_empty() || !domain.contains('.') {
        return Err(format!("{s:?} has no domain part after the '@'"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn validate_email_accepts_normal_form() {
        assert!(validate_email("user@example.com").is_ok());
    }

    #[test]
    fn validate_email_rejects_missing_at() {
        assert!(validate_email("user.example.com").is_err());
    }

    #[test]
    fn validate_email_rejects_empty_local() {
        assert!(validate_email("@example.com").is_err());
    }

    #[test]
    fn validate_email_rejects_dotless_domain() {
        assert!(validate_email("user@localhost").is_err());
    }

    fn sample_answers() -> WizardAnswers {
        WizardAnswers {
            email: "user@example.com".to_string(),
            token: TokenAnswer::Provided("tok".to_string()),
            maildir_path: "~/Maildir".to_string(),
            folder_layout: FolderLayout::Flat,
            conflict_strategy: ConflictStrategy::ServerWins,
        }
    }

    fn write_with_tight_perms(path: &Path, contents: &str) {
        std::fs::write(path, contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    /// In-keychain path: no token line in the rendered TOML, but
    /// the file still loads and the rest of the answered values
    /// survive the round trip.
    #[test]
    fn rendered_config_round_trips_without_in_file_token() {
        let answers = sample_answers();
        let rendered = render_config(&answers, None);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_with_tight_perms(&path, &rendered);
        let cfg = Config::load(&path).expect("rendered TOML must load");
        assert_eq!(cfg.account.email, "user@example.com");
        assert!(cfg.account.token.is_none());
        assert_eq!(cfg.sync.maildir_path, "~/Maildir");
        assert_eq!(cfg.sync.folder_layout, FolderLayout::Flat);
        assert_eq!(cfg.sync.hierarchy_separator, '.');
        // Wizard inherits the shared template's commented-out
        // mailbox list, so the user gets the sync-all default and
        // can curate post-wizard if they actually want a subset.
        assert!(cfg.sync.mailboxes.is_empty());
    }

    /// In-file token path: render and reload, verifying the token
    /// survives. Pins the fallback used when the keychain is
    /// unavailable.
    #[test]
    fn rendered_config_round_trips_with_in_file_token() {
        let answers = WizardAnswers {
            folder_layout: FolderLayout::MaildirPP,
            conflict_strategy: ConflictStrategy::LocalWins,
            ..sample_answers()
        };
        let rendered = render_config(&answers, Some("secret"));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_with_tight_perms(&path, &rendered);
        let cfg = Config::load(&path).expect("rendered TOML must load");
        assert_eq!(cfg.account.token.as_deref(), Some("secret"));
        assert_eq!(cfg.sync.folder_layout, FolderLayout::MaildirPP);
    }

    /// Returns true if the rendered TOML has at least one
    /// uncommented `token = "..."` line. Used by the keychain /
    /// placeholder branch tests to discriminate "the field is set"
    /// from "the field is shown as a commented-out fallback hint".
    fn has_uncommented_token_line(rendered: &str) -> bool {
        rendered
            .lines()
            .any(|l| l.trim_start().starts_with("token ="))
    }

    /// Reuse path: the keychain already had a token for this
    /// email when the wizard started, so no `Provided` ever
    /// happened, but the rendered config should still use the
    /// `InKeychain` slot (a pointer to the keychain entry) rather
    /// than `Placeholder` (a blank `token = ""` line). Pins the
    /// branch that makes second-machine bootstraps frictionless.
    #[test]
    fn rendered_config_uses_in_keychain_slot_when_token_was_already_present() {
        let answers = WizardAnswers {
            token: TokenAnswer::AlreadyInKeychain,
            ..sample_answers()
        };
        let rendered = render_config(&answers, None);
        assert!(
            !has_uncommented_token_line(&rendered),
            "AlreadyInKeychain path must not emit an uncommented token line; \
             rendered:\n{rendered}"
        );
        assert!(
            rendered.contains("Token is stored in your OS keychain"),
            "expected the in-keychain note in the rendered config;\n{rendered}"
        );
        // Loading the file must also see no in-file token.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_with_tight_perms(&path, &rendered);
        let cfg = Config::load(&path).expect("rendered TOML must load");
        assert!(cfg.account.token.is_none());
    }

    /// Skipped path: user pressed Enter at the token prompt
    /// without typing anything. Rendered config keeps the
    /// placeholder `token = ""` so the user can either edit the
    /// file or run `jma auth set-token` later.
    #[test]
    fn rendered_config_uses_placeholder_when_token_skipped() {
        let answers = WizardAnswers {
            token: TokenAnswer::Skipped,
            ..sample_answers()
        };
        let rendered = render_config(&answers, None);
        assert!(
            has_uncommented_token_line(&rendered),
            "Skipped path must emit a placeholder `token = \"\"` line;\n{rendered}"
        );
    }

    /// A stray quote or backslash in a wizard answer must survive
    /// through the TOML escape. We control the inputs so this isn't
    /// adversarial, but a path with a quote shouldn't break the file.
    #[test]
    fn rendered_config_escapes_quotes_in_inputs() {
        let answers = WizardAnswers {
            maildir_path: "/tmp/with \"quote\"".to_string(),
            ..sample_answers()
        };
        let rendered = render_config(&answers, Some("secret\\with\\slashes"));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_with_tight_perms(&path, &rendered);
        let cfg = Config::load(&path).expect("escaped TOML must still load");
        assert_eq!(cfg.sync.maildir_path, "/tmp/with \"quote\"");
        assert_eq!(cfg.account.token.as_deref(), Some("secret\\with\\slashes"));
    }
}
