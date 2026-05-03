use anyhow::{Context, Result};
use jmap_client::client::{Client, Credentials};
use rusqlite::Connection;
use tracing::{info, warn};

use crate::config::AccountConfig;
use crate::ids::JmapAccountId;
use crate::jmap::discovery;
use crate::jmap::retry::with_retry;
use crate::jmap::types::SessionInfo;
use crate::state::queries;

/// Establish a JMAP session with the server.
///
/// URL resolution order:
///
///   1. **Explicit override** — `[account].session_url` in the config.
///      Bypasses the discovery cache and DNS/well-known probes
///      entirely; useful for providers that don't publish discovery
///      records or for forcing a specific endpoint during testing.
///   2. **Discovery cache** — the `jmap_discovery` table keyed by the
///      account's email domain. If the cached URL connect fails, the
///      row is cleared and we fall through to step 3.
///   3. **Autodiscovery** — `discovery::discover` (DNS SRV, then
///      `/.well-known/jmap`). The result is written back to the cache.
///
/// Each connect attempt goes through `with_retry` so a transient 503
/// at startup doesn't fail the whole invocation outright.
pub async fn connect(account: &AccountConfig, conn: &Connection) -> Result<Client> {
    if let Some(url) = account.session_url.as_deref() {
        return connect_with(url, account).await;
    }

    let domain = account.email_domain()?;

    if let Some(url) = queries::get_cached_session_url(conn, domain)? {
        info!("Using cached JMAP session URL for {domain}: {url}");
        match connect_with(&url, account).await {
            Ok(client) => return Ok(client),
            Err(e) => {
                // `with_retry` already absorbed transient 5xx and
                // network errors inside `connect_with`, so reaching
                // here means the cached URL is structurally bad (404,
                // TLS failure, auth refusal, NXDOMAIN on the host).
                // Clearing the row and rediscovering is the correct
                // response; flaky upstreams don't cause cache thrash.
                warn!(
                    "Connect to cached session URL {url} failed: {e:#}; \
                     clearing cache and rediscovering"
                );
                queries::clear_cached_session_url(conn, domain)?;
            }
        }
    }

    let url = discovery::discover(domain).await?;
    queries::set_cached_session_url(conn, domain, &url)?;
    connect_with(&url, account).await
}

async fn connect_with(url: &str, account: &AccountConfig) -> Result<Client> {
    // Resolve the token once outside the retry loop so the keychain /
    // env / config chain isn't re-walked on every attempt.
    let token = account.token()?;

    info!("Connecting to JMAP server at {url}");

    let client = with_retry("session connect", || async {
        Client::new()
            .credentials(Credentials::bearer(token.clone()))
            .connect(url)
            .await
            .context("Failed to connect to JMAP server")
    })
    .await?;

    info!(
        "JMAP session established for {}",
        client.session().username()
    );

    Ok(client)
}

/// Extract session info from an established client.
pub fn session_info(client: &Client) -> Result<SessionInfo> {
    let session = client.session();

    let account_id = session
        .accounts()
        .next()
        .context("No accounts found in JMAP session")?;

    Ok(SessionInfo {
        api_url: session.api_url().to_string(),
        download_url: session.download_url().to_string(),
        upload_url: session.upload_url().to_string(),
        event_source_url: session.event_source_url().to_string(),
        account_id: JmapAccountId::from(account_id.as_str()),
        username: session.username().to_string(),
    })
}
