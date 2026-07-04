use anyhow::Result;
use futures_util::StreamExt;
use reqwest_eventsource::{Event, EventSource};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::runner::SyncTrigger;
use super::{PING_WATCHDOG_SLACK, RECONNECT_INITIAL_BACKOFF, RECONNECT_MAX_BACKOFF};
use crate::ids::JmapAccountId;

/// Outcome of a single SSE connection attempt.
enum ConnectOutcome {
    /// The trigger channel was closed (daemon shutting down). Stop reconnecting.
    ChannelClosed,
    /// The SSE stream ended (server closed, HTTP error, etc.). Reconnect after backoff.
    StreamEnded,
}

/// Entity types whose state changes should drive a sync. Other types
/// (EmailDelivery, Identity, ...) appear in StateChange payloads but
/// have no effect on what we sync, and treating them as triggers
/// causes a no-op sync loop.
///
/// Mailbox is tracked because the structural folder lifecycle
/// (create / rename / destroy on either side, with bidirectional
/// conflict resolution) needs prompt detection: a folder mutation
/// that doesn't co-occur with an Email change (per RFC 8621
/// section 2, Mailbox properties such as `name`, `parentId`,
/// `role`, and `sortOrder` can each move independently of any
/// Email update) would otherwise wait for an unrelated trigger
/// to wake the daemon. Under the destructive-arm policy the
/// resulting drift between cache, server, and disk views can
/// land silently, so the cursor-plumbing cost is worth paying.
pub(super) const TRACKED_TYPES: &[&str] = &["Email", "Mailbox"];

/// Spec-mandated upper bound on a server's allowed maximum ping
/// interval (RFC 8620 §7.3: "servers MUST NOT have ... a maximum
/// allowed value less than 300"). Used as the watchdog's initial
/// budget before the server tells us its actual interval, so a server
/// that clamps our requested value upward (Fastmail can hand us back
/// 300s when we ask for 60s) doesn't trip the watchdog before its
/// first ping arrives.
const SPEC_MAX_PING_INTERVAL_SECS: u64 = 300;

/// Listen to JMAP EventSource (SSE) for state changes and send triggers.
///
/// `initial_states` seeds the dedup cache from the state DB so the first
/// event after an initial sync isn't a guaranteed redundant trigger. The
/// cache is preserved across reconnects so dedup still works after a
/// transient disconnect.
pub async fn listen(
    event_source_url: &str,
    auth_token: &str,
    account_id: &JmapAccountId,
    ping_interval: u64,
    initial_states: HashMap<String, String>,
    tx: mpsc::Sender<SyncTrigger>,
) -> Result<()> {
    let mut last_states = initial_states;
    let mut backoff = RECONNECT_INITIAL_BACKOFF;

    loop {
        match connect_and_listen(
            event_source_url,
            auth_token,
            account_id,
            ping_interval,
            &mut last_states,
            &mut backoff,
            &tx,
        )
        .await
        {
            Ok(ConnectOutcome::ChannelClosed) => {
                info!("SSE listener shutting down (channel closed)");
                return Ok(());
            }
            Ok(ConnectOutcome::StreamEnded) => {
                warn!("SSE stream ended; reconnecting in {:?}", backoff);
            }
            Err(e) => {
                warn!("SSE listener error ({}); reconnecting in {:?}", e, backoff);
            }
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
    }
}

/// One SSE connection attempt: open the stream, dispatch events, and
/// return when the stream ends or the trigger channel closes.
///
/// Two recovery mechanisms layered into this loop:
///
/// 1. **Backoff reset on first message.** `*backoff` resets to the
///    initial value once we receive any `Event::Message` (state or
///    server-side ping), not on `Event::Open`. A server that accepts
///    the connection but never pings would otherwise let us reset to
///    the floor on every reconnect and we'd hammer it at the minimum
///    interval; tying the reset to actual data flow makes silent-
///    server scenarios escalate.
///
/// 2. **Ping watchdog.** `es.next()` is wrapped in a
///    `interval + PING_WATCHDOG_SLACK` timeout. Per RFC 8620 §7.3 the
///    server MAY clamp our requested `ping=N` -- Fastmail e.g. seems
///    to hand back 300s for a 60s request -- and the only spec-blessed
///    way to learn the actual interval is the `interval` field on the
///    server's ping event payload. Until that first ping arrives we
///    budget the spec-mandated maximum (`SPEC_MAX_PING_INTERVAL_SECS`)
///    so we don't false-fire in the request-vs-actual gap, and we
///    re-negotiate on every subsequent ping in case the server moves.
///    If no event of any kind shows up within the current window we
///    treat the stream as silently dead -- common after a laptop
///    wakes from sleep or a NAT entry expires -- and bail to the
///    outer reconnect loop. Independent of the daemon-level reconnect
///    path in `runner::run`, which keys off `engine.run` transient
///    errors instead.
async fn connect_and_listen(
    event_source_url: &str,
    auth_token: &str,
    account_id: &JmapAccountId,
    ping_interval: u64,
    last_states: &mut HashMap<String, String>,
    backoff: &mut Duration,
    tx: &mpsc::Sender<SyncTrigger>,
) -> Result<ConnectOutcome> {
    info!("Connecting to JMAP EventSource: {}", event_source_url);

    let url = format!(
        "{}?types=*&closeafter=no&ping={}",
        event_source_url, ping_interval
    );

    let client = reqwest::Client::new();
    let request = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", auth_token));

    let mut es = EventSource::new(request)?;

    let mut watchdog = Duration::from_secs(SPEC_MAX_PING_INTERVAL_SECS) + PING_WATCHDOG_SLACK;
    let mut watchdog_negotiated = false;
    let mut parse_failure_logged = false;

    loop {
        let next = match tokio::time::timeout(watchdog, es.next()).await {
            Ok(Some(event)) => event,
            Ok(None) => return Ok(ConnectOutcome::StreamEnded),
            Err(_) => {
                warn!("No SSE event in {:?}; reconnecting", watchdog);
                return Ok(ConnectOutcome::StreamEnded);
            }
        };

        match next {
            Ok(Event::Open) => {
                info!("SSE connection opened");
            }
            Ok(Event::Message(msg)) => {
                *backoff = RECONNECT_INITIAL_BACKOFF;

                debug!("SSE event: type={}, data={}", msg.event, msg.data);

                if msg.event == "ping" {
                    match parse_ping_interval(&msg.data) {
                        Some(interval) => {
                            let new_watchdog = Duration::from_secs(interval) + PING_WATCHDOG_SLACK;
                            if !watchdog_negotiated {
                                info!(
                                    "Server-negotiated SSE ping interval: {}s (watchdog {:?})",
                                    interval, new_watchdog
                                );
                                watchdog_negotiated = true;
                            }
                            watchdog = new_watchdog;
                        }
                        None => {
                            // Once per connection. The flag lives on
                            // this stack frame, so reconnecting
                            // resets it: a server that starts
                            // behaving will surface its first valid
                            // interval via the success arm above.
                            if !parse_failure_logged {
                                warn!(
                                    "Ping event missing/invalid `interval`; \
                                     keeping watchdog at {:?}",
                                    watchdog
                                );
                                parse_failure_logged = true;
                            }
                        }
                    }
                    continue;
                }

                if msg.event != "state" {
                    continue;
                }

                let should_trigger = match decide_trigger(
                    &msg.data,
                    account_id.as_ref(),
                    last_states,
                ) {
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
                    return Ok(ConnectOutcome::ChannelClosed);
                }
            }
            Err(e) => {
                // reqwest-eventsource retries transport errors internally,
                // but on HTTP errors (e.g. 503) it terminates the stream;
                // bubble out so the outer loop reconnects with backoff.
                warn!("SSE error: {}", e);
                return Ok(ConnectOutcome::StreamEnded);
            }
        }
    }
}

/// Pull the `interval` (in seconds) out of a server-emitted ping
/// event payload per RFC 8620 §7.3:
///
/// > The data for the ping event MUST be a JSON object containing
/// > an "interval" property, the value (type "UnsignedInt") being
/// > the interval in seconds the server is using to send pings.
///
/// Returns `None` for any payload that isn't valid JSON, has a
/// missing/non-integer `interval`, or reports `interval: 0` (which
/// would shrink the watchdog to bare slack and false-fire on every
/// subsequent event -- spec mandates servers honor a minimum of 30,
/// so 0 is buggy-server territory). The caller keeps the previous
/// watchdog value in any of these cases.
fn parse_ping_interval(data: &str) -> Option<u64> {
    let value: serde_json::Value = serde_json::from_str(data).ok()?;
    let interval = value.get("interval")?.as_u64()?;
    (interval > 0).then_some(interval)
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
    fn mailbox_only_change_fires() {
        // Structural folder lifecycle relies on prompt detection:
        // an event that advances Mailbox state without touching
        // Email (folder rename / parent move / role / sortOrder
        // change, all independent Mailbox properties per RFC 8621
        // section 2) must drive a sync so the next cycle picks
        // up the structural drift.
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

    #[test]
    fn parse_ping_interval_extracts_seconds() {
        let data = r#"{"@type":"Ping","interval":300}"#;
        assert_eq!(parse_ping_interval(data), Some(300));
    }

    #[test]
    fn parse_ping_interval_returns_none_for_missing_field() {
        let data = r#"{"@type":"Ping"}"#;
        assert_eq!(parse_ping_interval(data), None);
    }

    #[test]
    fn parse_ping_interval_returns_none_for_non_integer() {
        let data = r#"{"@type":"Ping","interval":"300"}"#;
        assert_eq!(parse_ping_interval(data), None);
    }

    #[test]
    fn parse_ping_interval_returns_none_for_invalid_json() {
        assert_eq!(parse_ping_interval("not json"), None);
    }

    #[test]
    fn parse_ping_interval_returns_none_for_zero() {
        let data = r#"{"@type":"Ping","interval":0}"#;
        assert_eq!(parse_ping_interval(data), None);
    }
}
