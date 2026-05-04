//! Centralized accessors for JMAP Session-advertised limits.
//!
//! The JMAP server is a trust boundary: it can advertise any value it
//! likes in its session capabilities (RFC 8620 §2). Every accessor in
//! this module combines the server's value with a separate ceiling
//! representing what jmapsync is willing to attempt regardless. The
//! effective limit is always the smaller of the two:
//! `min(server_value, our_ceiling)`. The server can only **lower**
//! what we'd otherwise do; it can never raise it. When the server
//! omits a capability, the accessor falls back to the same ceiling.
//!
//! Commit `313ab36b` ("jmap: Reject mailbox names that would escape
//! the maildir tree") encoded this for `maxSizeMailboxName` — the
//! 255-byte cap stayed hardcoded because trusting the server's
//! advertised value would let a malicious server pick an arbitrary
//! upper bound. The same risk applies to every other server cap, so
//! every accessor here uses
//! `map_or(OUR_CEILING, |s| s.min(OUR_CEILING))`.
//!
//! For the `maxConcurrentRequests` variant, the user's
//! `download_concurrency` config plays the role of the ceiling
//! instead of a hardcoded constant — the user has explicitly opted
//! into a parallelism level, and the server cap can still only lower
//! it.

use jmap_client::client::Client;

/// Effective concurrency for the per-cycle download buffer. Clamps
/// the user-configured `download_concurrency` to the server's
/// advertised `maxConcurrentRequests`; whichever is smaller wins,
/// with a floor of 1 so the stream still makes forward progress when
/// the server advertises 0 (which would technically be legal).
pub fn concurrent_requests(client: &Client, configured: usize) -> usize {
    client
        .session()
        .core_capabilities()
        .map(|c| c.max_concurrent_requests())
        .map_or(configured, |s| configured.min(s))
        .max(1)
}
