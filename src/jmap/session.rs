use anyhow::{Context, Result};
use jmap_client::client::{Client, Credentials};
use tracing::info;

use crate::config::AccountConfig;
use crate::jmap::retry::with_retry;
use crate::jmap::types::SessionInfo;

/// Establish a JMAP session with the server. Retries on transient
/// failures with bounded backoff so a 503 at startup doesn't fail the
/// whole `watch` invocation outright.
pub async fn connect(account: &AccountConfig) -> Result<Client> {
    let token = account.token()?;

    info!("Connecting to JMAP server at {}", account.session_url);

    let client = with_retry("session connect", || async {
        Client::new()
            .credentials(Credentials::bearer(token.clone()))
            .connect(&account.session_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to JMAP server: {}", e))
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
        .context("No accounts found in JMAP session")?
        .clone();

    Ok(SessionInfo {
        api_url: session.api_url().to_string(),
        download_url: session.download_url().to_string(),
        upload_url: session.upload_url().to_string(),
        event_source_url: session.event_source_url().to_string(),
        account_id,
        username: session.username().to_string(),
    })
}
