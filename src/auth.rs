//! Bearer token storage in the OS-native secret store.
//!
//! Uses `keyring-core` (the v1 cross-platform API) plus a
//! platform-specific backend crate selected at compile time:
//!
//!   - macOS: `apple-native-keyring-store` (login keychain)
//!   - Linux: `dbus-secret-service-keyring-store`
//!   - Windows: `windows-native-keyring-store`
//!
//! Service name `jmapsync-bearer`, user `default`. The user key
//! is fixed today; multi-account support will rekey it later.
//!
//! Reads are tolerant: any keyring failure (no backend, locked
//! keychain, denied prompt) is logged at debug and reported as "no
//! token stored", so a config-file or env-var token can still be
//! used. Writes and deletes surface errors loudly — a user who
//! explicitly asked to manage the keychain wants to know if it
//! failed.

use anyhow::{Context, Result, anyhow, bail};
use keyring_core::{Entry, Error};
use std::sync::OnceLock;
use tracing::debug;

const SERVICE: &str = "jmapsync-bearer";
const DEFAULT_USER: &str = "default";

/// Memoised result of registering the platform credential store as
/// keyring-core's default. The error is stringified because
/// `OnceLock` returns a shared reference and `anyhow::Error` isn't
/// `Clone`. A failed init is sticky for the process lifetime: the
/// underlying problem (no backend available, D-Bus session bus
/// missing) is unlikely to resolve mid-run, and silently retrying
/// would just re-pay the same cost.
static STORE_INIT: OnceLock<std::result::Result<(), String>> = OnceLock::new();

fn init_store() -> Result<()> {
    let outcome = STORE_INIT.get_or_init(|| try_init_store().map_err(|e| format!("{e:#}")));
    match outcome {
        Ok(()) => Ok(()),
        Err(msg) => Err(anyhow!("keyring init failed: {msg}")),
    }
}

#[cfg(target_os = "macos")]
fn try_init_store() -> Result<()> {
    let store = apple_native_keyring_store::keychain::Store::new()
        .map_err(|e| anyhow!("{e}"))
        .context("create macOS keychain store")?;
    keyring_core::set_default_store(store);
    Ok(())
}

#[cfg(target_os = "linux")]
fn try_init_store() -> Result<()> {
    let store = dbus_secret_service_keyring_store::Store::new()
        .map_err(|e| anyhow!("{e}"))
        .context("create Linux Secret Service store")?;
    keyring_core::set_default_store(store);
    Ok(())
}

#[cfg(target_os = "windows")]
fn try_init_store() -> Result<()> {
    let store = windows_native_keyring_store::Store::new()
        .map_err(|e| anyhow!("{e}"))
        .context("create Windows Credential Manager store")?;
    keyring_core::set_default_store(store);
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
fn try_init_store() -> Result<()> {
    bail!("no keyring backend available for this platform")
}

fn entry() -> Result<Entry> {
    init_store()?;
    Entry::new(SERVICE, DEFAULT_USER)
        .map_err(|e| anyhow!("{e}"))
        .context("Failed to open keyring entry")
}

/// Store `token` in the OS keychain. Empty tokens are rejected here
/// rather than at read time so a bad input fails fast.
pub fn set_bearer_token(token: &str) -> Result<()> {
    if token.is_empty() {
        bail!("refusing to store an empty bearer token");
    }
    entry()?
        .set_password(token)
        .map_err(|e| anyhow!("{e}"))
        .context("Failed to write bearer token to keyring")
}

/// Read a stored bearer token. Returns `None` for both "no entry"
/// and "backend unreachable" so callers can fall through to other
/// resolution paths (env var, config file).
pub fn get_bearer_token() -> Option<String> {
    let entry = match entry() {
        Ok(e) => e,
        Err(err) => {
            debug!("keyring entry unavailable; falling back: {}", err);
            return None;
        }
    };
    match entry.get_password() {
        Ok(t) => Some(t),
        Err(Error::NoEntry) => None,
        Err(err) => {
            debug!("keyring read failed; falling back: {}", err);
            None
        }
    }
}

/// Remove the stored bearer token. A missing entry is not an error.
pub fn clear_bearer_token() -> Result<()> {
    match entry()?.delete_credential() {
        Ok(()) => Ok(()),
        Err(Error::NoEntry) => Ok(()),
        Err(e) => Err(anyhow!("{e}")).context("Failed to clear bearer token from keyring"),
    }
}
