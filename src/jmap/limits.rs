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
//! every accessor here uses `map_or(C, |s| s.min(C))` against the
//! appropriate `MAX_*` constant declared up top of this module.
//!
//! For the `maxConcurrentRequests` variant, the user's
//! `download_concurrency` config plays the role of the ceiling
//! instead of a hardcoded constant — the user has explicitly opted
//! into a parallelism level, and the server cap can still only lower
//! it.

use jmap_client::client::Client;

/// Maximum number of operations we'll bundle into a single
/// `Email/set` request body, regardless of what the server
/// advertises for `maxObjectsInSet`. A hostile or buggy server could
/// otherwise advertise an absurd value and force us to bundle
/// arbitrarily many ops into one HTTP body. 500 leaves plenty of
/// headroom for a busy sync cycle while staying well inside any
/// real JMAP server's limit (Fastmail's is in the thousands).
pub const MAX_SET_BATCH_SIZE: usize = 500;

/// Maximum byte size of a single message we'll attempt to upload,
/// regardless of what the server advertises for `maxSizeUpload`. A
/// hostile or buggy server could otherwise advertise a multi-TiB cap
/// and let a pathologically large local file OOM us when
/// `upload_messages` reads the whole thing into RAM. 100 MiB sits
/// above known real-world server caps (Fastmail advertises 70 MiB),
/// so the server's tighter value is what bites in practice on a
/// normal account; the ceiling only matters as a finite worst case
/// if the server omits the capability or advertises something
/// absurd.
pub const MAX_UPLOAD_FILE_SIZE: usize = 100 * 1024 * 1024;

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

/// Effective `Email/set` batch size. Clamps the server's advertised
/// `maxObjectsInSet` against `MAX_SET_BATCH_SIZE`; whichever is
/// smaller wins.
pub fn max_objects_in_set(client: &Client) -> usize {
    client
        .session()
        .core_capabilities()
        .map(|c| c.max_objects_in_set())
        .map_or(MAX_SET_BATCH_SIZE, |s| s.min(MAX_SET_BATCH_SIZE))
        .max(1)
}

/// Effective upload size cap in bytes. Clamps the server's
/// advertised `maxSizeUpload` against `MAX_UPLOAD_FILE_SIZE`;
/// whichever is smaller wins.
pub fn max_size_upload(client: &Client) -> usize {
    client
        .session()
        .core_capabilities()
        .map(|c| c.max_size_upload())
        .map_or(MAX_UPLOAD_FILE_SIZE, |s| s.min(MAX_UPLOAD_FILE_SIZE))
        .max(1)
}
