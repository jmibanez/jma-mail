use anyhow::Result;
use futures_util::StreamExt;
use reqwest_eventsource::{Event, EventSource};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::runner::SyncTrigger;

/// Listen to JMAP EventSource (SSE) for state changes and send triggers.
pub async fn listen(
    event_source_url: &str,
    auth_token: &str,
    tx: mpsc::Sender<SyncTrigger>,
) -> Result<()> {
    info!("Connecting to JMAP EventSource: {}", event_source_url);

    // Build the SSE URL with parameters
    let url = format!(
        "{}?types=*&closeafter=no&ping=60",
        event_source_url
    );

    let client = reqwest::Client::new();
    let request = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", auth_token));

    let mut es = EventSource::new(request)?;

    info!("SSE connection established, listening for state changes");

    while let Some(event) = es.next().await {
        match event {
            Ok(Event::Open) => {
                info!("SSE connection opened");
            }
            Ok(Event::Message(msg)) => {
                debug!("SSE event: type={}, data={}", msg.event, msg.data);

                if msg.event == "state" {
                    info!("Server state changed, triggering sync");
                    if tx.send(SyncTrigger::RemoteChange).await.is_err() {
                        info!("Sync channel closed, shutting down SSE listener");
                        break;
                    }
                }
            }
            Err(e) => {
                warn!("SSE error: {}", e);
                // reqwest-eventsource handles reconnection
            }
        }
    }

    info!("SSE listener ended");
    Ok(())
}
