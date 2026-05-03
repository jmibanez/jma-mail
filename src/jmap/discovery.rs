//! JMAP session URL autodiscovery, per RFC 8620 section 2.2.
//!
//! Two probes in sequence:
//!
//! 1. **DNS SRV** — `_jmap._tcp.<domain>`. Records are sorted by
//!    ascending priority, then descending weight (RFC 2782 ordering;
//!    we don't randomise within a priority class because the result
//!    is cached and the determinism aids debugging). For each
//!    target, GET `https://<target>:<port>/.well-known/jmap`.
//!
//! 2. **Plain well-known** — if SRV has no records or every SRV
//!    target is unreachable, fall through to
//!    `https://<domain>/.well-known/jmap`.
//!
//! Per RFC 8620 §2.2 the well-known endpoint may respond with a
//! redirect (301/302/308) to the canonical session URL or serve the
//! JMAP Session resource directly (200). We do **not** follow the
//! redirect: §2.1 says fetching the Session resource requires an
//! authenticated GET, and the well-known probe is unauthenticated.
//! If we let reqwest follow, we'd land on the session resource
//! without a Bearer token and get a 401/403 instead of the URL we
//! actually want.
//!
//! Instead, with `redirect::Policy::none()`:
//!
//!   * 30x → read the `Location` header, resolve it against the
//!     request URL (handles relative `Location: /jmap/session`
//!     values), return that.
//!   * 200 → the well-known IS the session resource; return the
//!     request URL itself.
//!   * Anything else → error.
//!
//! The returned URL is what the caller stores in the discovery
//! cache and hands to `Client::connect` (which then provides the
//! Bearer credentials).

use anyhow::{Context, Result};
use hickory_resolver::TokioResolver;
use hickory_resolver::proto::rr::RData;
use hickory_resolver::proto::rr::rdata::SRV;
use tracing::{debug, info, warn};

/// Discover the JMAP session URL for `domain`. Tries SRV first,
/// then a plain well-known probe at `https://<domain>`.
///
/// On success, the returned URL is either the `Location` header
/// from a 30x response (resolved against the request URL) or the
/// request URL itself when the well-known returned 200.
pub async fn discover(domain: &str) -> Result<String> {
    info!("Discovering JMAP session URL for {domain}");

    if let Some(url) = try_srv(domain).await? {
        return Ok(url);
    }

    debug!("No usable SRV record for {domain}; trying plain well-known");
    try_well_known(&format!("https://{domain}"))
        .await
        .with_context(|| {
            format!(
                "JMAP autodiscovery failed for {domain}: no SRV record \
                 and well-known fallback was unreachable. Set \
                 [account].session_url in your config to bypass \
                 autodiscovery."
            )
        })
}

async fn try_srv(domain: &str) -> Result<Option<String>> {
    let resolver = TokioResolver::builder_tokio()
        .context("Failed to read system DNS configuration")?
        .build()
        .context("Failed to build DNS resolver")?;

    let qname = format!("_jmap._tcp.{domain}");
    let lookup = match resolver.srv_lookup(qname.as_str()).await {
        Ok(l) => l,
        Err(e) if e.is_no_records_found() => {
            // No SRV record (NXDOMAIN or NoRecordsFound) is the
            // common case for most providers and the signal to fall
            // through to plain well-known.
            debug!("No SRV record for {qname}: {e}");
            return Ok(None);
        }
        Err(e) => {
            // Genuine resolver failure (timeout, refused, network
            // down). We still fall through -- the well-known probe
            // may still work via host-file or alternate routing --
            // but surface it to the user since it's not the silent
            // case.
            warn!("SRV lookup for {qname} failed: {e}");
            return Ok(None);
        }
    };

    let srvs: Vec<&SRV> = lookup
        .answers()
        .iter()
        .filter_map(|r| match &r.data {
            RData::SRV(s) => Some(s),
            _ => None,
        })
        .collect();
    let records = sort_srv_records(srvs);

    for rec in records {
        // RFC 2782: a target of "." means "service explicitly not
        // provided" -- skip rather than try to GET https://./...
        let target = rec.target.to_string();
        let target = target.trim_end_matches('.');
        if target.is_empty() {
            debug!("Skipping SRV record with root target");
            continue;
        }
        let port = rec.port;
        let base = format!("https://{target}:{port}");
        debug!("Trying SRV target {base}");
        match try_well_known(&base).await {
            Ok(session_url) => return Ok(Some(session_url)),
            Err(e) => warn!("SRV target {base} unreachable: {e}"),
        }
    }

    Ok(None)
}

/// RFC 2782 ordering: ascending priority, then descending weight.
/// Within a priority class the spec actually mandates weighted
/// random selection, but for one-shot cached discovery the
/// determinism is more useful than the load distribution.
fn sort_srv_records(mut records: Vec<&SRV>) -> Vec<&SRV> {
    records.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| b.weight.cmp(&a.weight))
    });
    records
}

async fn try_well_known(base: &str) -> Result<String> {
    let url = format!("{base}/.well-known/jmap");
    debug!("GET {url}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("Failed to build reqwest client")?;

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("Failed to reach {url}"))?;

    let status = resp.status();

    if status.is_redirection() {
        let location = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .with_context(|| format!("{url} returned {status} without a Location header"))?
            .to_str()
            .with_context(|| format!("Location header at {url} is not valid ASCII"))?;
        let request_url = resp.url().clone();
        let session_url = request_url.join(location).with_context(|| {
            format!("cannot resolve Location {location:?} against {request_url}")
        })?;
        return Ok(session_url.to_string());
    }

    if status.is_success() {
        // RFC 8620 §2.2 allows the well-known endpoint to serve the
        // session resource directly. The probed URL is then the
        // session URL.
        return Ok(url);
    }

    Err(anyhow::anyhow!("{url} returned {status}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_resolver::proto::rr::domain::Name;

    fn srv(priority: u16, weight: u16, target: &str) -> SRV {
        SRV::new(priority, weight, 443, target.parse::<Name>().unwrap())
    }

    #[test]
    fn sort_orders_by_priority_then_weight() {
        let a = srv(20, 100, "high-prio.example.com.");
        let b = srv(10, 50, "low-prio-low-w.example.com.");
        let c = srv(10, 100, "low-prio-high-w.example.com.");
        let sorted = sort_srv_records(vec![&a, &b, &c]);
        let targets: Vec<String> = sorted.iter().map(|r| r.target.to_string()).collect();
        assert_eq!(
            targets,
            vec![
                "low-prio-high-w.example.com.",
                "low-prio-low-w.example.com.",
                "high-prio.example.com.",
            ]
        );
    }

    #[test]
    fn sort_is_stable_for_identical_priority_and_weight() {
        let a = srv(10, 50, "first.example.com.");
        let b = srv(10, 50, "second.example.com.");
        let sorted = sort_srv_records(vec![&a, &b]);
        let targets: Vec<String> = sorted.iter().map(|r| r.target.to_string()).collect();
        assert_eq!(targets, vec!["first.example.com.", "second.example.com."]);
    }
}
