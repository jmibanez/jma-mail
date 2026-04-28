use anyhow::Result;
use futures_util::StreamExt;
use reqwest_eventsource::{Event, EventSource};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::runner::SyncTrigger;

/// Entity types whose state changes should drive a sync. Other types
/// (EmailDelivery, Identity, ...) appear in StateChange payloads but
/// have no effect on what we sync, and treating them as triggers
/// causes a no-op sync loop.
const TRACKED_TYPES: &[&str] = &["Email", "Mailbox"];

/// Listen to JMAP EventSource (SSE) for state changes and send triggers.
///
/// `initial_states` seeds the dedup cache from the state DB so the first
/// event after an initial sync isn't a guaranteed redundant trigger.
pub async fn listen(
    event_source_url: &str,
    auth_token: &str,
    account_id: &str,
    ping_interval: u64,
    initial_states: HashMap<String, String>,
    tx: mpsc::Sender<SyncTrigger>,
) -> Result<()> {
    info!("Connecting to JMAP EventSource: {}", event_source_url);

    // Build the SSE URL with parameters
    let url = format!(
        "{}?types=*&closeafter=no&ping={}",
        event_source_url, ping_interval
    );

    let client = reqwest::Client::new();
    let request = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", auth_token));

    let mut es = EventSource::new(request)?;
    let mut last_states: HashMap<String, String> = initial_states;

    info!("SSE connection established, listening for state changes");

    while let Some(event) = es.next().await {
        match event {
            Ok(Event::Open) => {
                info!("SSE connection opened");
            }
            Ok(Event::Message(msg)) => {
                debug!("SSE event: type={}, data={}", msg.event, msg.data);

                if msg.event != "state" {
                    continue;
                }

                let should_trigger = match decide_trigger(&msg.data, account_id, &mut last_states) {
                    Ok(decision) => decision,
                    Err(e) => {
                        // Don't drop events because of a payload quirk;
                        // fall through and forward the trigger.
                        warn!(
                            "Failed to parse StateChange payload ({}); forwarding trigger anyway",
                            e
                        );
                        true
                    }
                };

                if !should_trigger {
                    continue;
                }

                if tx.send(SyncTrigger::RemoteChange).await.is_err() {
                    info!("Sync channel closed, shutting down SSE listener");
                    break;
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

/// Decide whether a StateChange event represents a real advance for an
/// entity type we care about. Updates `last_states` in place when it does.
fn decide_trigger(
    data: &str,
    account_id: &str,
    last_states: &mut HashMap<String, String>,
) -> Result<bool> {
    let value: serde_json::Value = serde_json::from_str(data)?;

    let changed = value
        .get("changed")
        .and_then(|c| c.get(account_id))
        .and_then(|a| a.as_object())
        .ok_or_else(|| anyhow::anyhow!("missing changed.{} object", account_id))?;

    let mut advanced: Vec<(String, Option<String>, String)> = Vec::new();
    for &type_name in TRACKED_TYPES {
        let Some(new_state) = changed.get(type_name).and_then(|v| v.as_str()) else {
            continue;
        };
        let prev = last_states.get(type_name);
        if prev.map(|s| s.as_str()) == Some(new_state) {
            continue;
        }
        advanced.push((type_name.to_string(), prev.cloned(), new_state.to_string()));
    }

    if advanced.is_empty() {
        debug!(
            "Skipping no-op StateChange (tracked types unchanged): {}",
            data
        );
        return Ok(false);
    }

    for (type_name, prev, new_state) in &advanced {
        info!(
            "{} state {} -> {}",
            type_name,
            prev.as_deref().unwrap_or("<none>"),
            new_state
        );
        last_states.insert(type_name.clone(), new_state.clone());
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(account: &str, types: &[(&str, &str)]) -> String {
        let inner: serde_json::Map<String, serde_json::Value> = types
            .iter()
            .map(|(k, v)| {
                (
                    (*k).to_string(),
                    serde_json::Value::String((*v).to_string()),
                )
            })
            .collect();
        serde_json::json!({
            "@type": "StateChange",
            "changed": { account: inner }
        })
        .to_string()
    }

    #[test]
    fn fires_on_first_email_state() {
        let mut last = HashMap::new();
        let data = make_event("acct", &[("Email", "J1")]);
        assert!(decide_trigger(&data, "acct", &mut last).unwrap());
        assert_eq!(last.get("Email").unwrap(), "J1");
    }

    #[test]
    fn dedups_repeated_email_state() {
        let mut last = HashMap::from([("Email".to_string(), "J1".to_string())]);
        let data = make_event("acct", &[("Email", "J1")]);
        assert!(!decide_trigger(&data, "acct", &mut last).unwrap());
    }

    #[test]
    fn fires_when_email_advances() {
        let mut last = HashMap::from([("Email".to_string(), "J1".to_string())]);
        let data = make_event("acct", &[("Email", "J2")]);
        assert!(decide_trigger(&data, "acct", &mut last).unwrap());
        assert_eq!(last.get("Email").unwrap(), "J2");
    }

    #[test]
    fn ignores_untracked_types() {
        let mut last = HashMap::new();
        let data = make_event("acct", &[("EmailDelivery", "D5"), ("Identity", "I9")]);
        assert!(!decide_trigger(&data, "acct", &mut last).unwrap());
        assert!(last.is_empty());
    }

    #[test]
    fn fires_on_mailbox_change_even_if_email_same() {
        let mut last = HashMap::from([("Email".to_string(), "J1".to_string())]);
        let data = make_event("acct", &[("Email", "J1"), ("Mailbox", "M2")]);
        assert!(decide_trigger(&data, "acct", &mut last).unwrap());
        assert_eq!(last.get("Mailbox").unwrap(), "M2");
    }

    #[test]
    fn missing_account_is_parse_error() {
        let mut last = HashMap::new();
        let data = make_event("other-acct", &[("Email", "J1")]);
        assert!(decide_trigger(&data, "acct", &mut last).is_err());
    }
}
