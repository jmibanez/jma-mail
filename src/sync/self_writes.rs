//! Path-based deduplication of jma's own filesystem writes.
//!
//! After a sync cycle delivers a message to disk, fsevents echoes
//! the write back through the watcher. The path-driven scan
//! correctly classifies these echoes as no-ops, but the cycle
//! itself still costs an mpsc send, the runner's coalesce wait, an
//! `engine.run`, and (worst) an `Email/changes` round-trip that
//! wakes the radio. The watcher's all-live-new filter catches the
//! simplest case (jma writes to `new/`, fsevents fires for the
//! same `new/` path) but cannot help when the batch contains a
//! `cur/` path -- e.g. when an MUA promotes `new/<id>:2,F` to
//! `cur/<id>:2,F` preserving flags, both paths arrive in one
//! batch and the predicate keeps it.
//!
//! `SelfWriteCache` plugs that hole. The executor records the
//! exact path it just wrote, plus the cur/ path the MUA is likely
//! to promote it to (for `new/` deliveries). The watcher consults
//! the cache before sending a trigger and drops batches whose
//! every path matches a recent self-write. A flag-changing
//! promotion (the MUA adds `S`, say) won't match the predicted
//! cur/ path and so falls through to a real cycle.
//!
//! The cache is path-exact, not id-based: two batches with the
//! same maildir id but different flag suffixes are different cache
//! lookups, which is what lets us distinguish a same-flag
//! promotion (drop) from a flag-changing one (keep).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct SelfWriteCache {
    entries: Mutex<HashMap<PathBuf, Instant>>,
    ttl: Duration,
}

impl SelfWriteCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Record one or more paths as recently written by jma. Each
    /// gets a fresh timestamp; lookups within `ttl` will treat them
    /// as self-writes. Opportunistically GCs expired entries on
    /// every record so memory stays bounded under sustained
    /// activity -- the executor records per delivery (not per
    /// batch), and at TTL=30s map size is bounded by recent
    /// throughput, so the O(n) sweep here is acceptable.
    pub fn record(&self, paths: impl IntoIterator<Item = PathBuf>) {
        let now = Instant::now();
        let mut map = self
            .entries
            .lock()
            .expect("self-write cache mutex poisoned; this is a bug");
        for p in paths {
            map.insert(p, now);
        }
        map.retain(|_, t| now.duration_since(*t) < self.ttl);
    }

    /// True iff every path in `batch` has a non-expired cache
    /// entry. On a successful match, all matched entries are
    /// evicted -- each cache entry represents a single expected
    /// fsevents echo of one of jma's writes, and once that echo
    /// has been observed, the entry has done its job. Without
    /// eviction a stale entry could keep suppressing subsequent
    /// real events on the same path within the TTL window
    /// (e.g. an MUA renames `cur/<id>:2,F` to add `S` after
    /// promotion completed; the cache match for the original
    /// promotion would otherwise also swallow the read-state
    /// rename event).
    ///
    /// An empty batch returns false -- callers already filter
    /// empty batches, so this is defensive: an empty "all match"
    /// should never short-circuit a code path.
    pub fn matches_all(&self, batch: &[PathBuf]) -> bool {
        if batch.is_empty() {
            return false;
        }
        let now = Instant::now();
        let mut map = self
            .entries
            .lock()
            .expect("self-write cache mutex poisoned; this is a bug");
        let all_match = batch.iter().all(|p| {
            map.get(p)
                .is_some_and(|t| now.duration_since(*t) < self.ttl)
        });
        if all_match {
            for p in batch {
                map.remove(p);
            }
        }
        all_match
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    /// Recorded paths match within the TTL window.
    #[test]
    fn records_and_matches_within_ttl() {
        let cache = SelfWriteCache::new(Duration::from_secs(60));
        let p = PathBuf::from("/Mail/INBOX/new/1700000000.M1.host:2,F");
        cache.record([p.clone()]);
        assert!(cache.matches_all(&[p]));
    }

    /// A successful match evicts every matched entry. Pins the
    /// consume-on-match contract: once the cache has suppressed an
    /// expected echo, a later real event for the same path must
    /// fall through to a real cycle. The hazard this guards
    /// against: after an MUA promotes new/<id>:2,F to cur/<id>:2,F
    /// (cache match), the MUA later renames cur/<id>:2,F to add S
    /// on read; without eviction the second rename's source-side
    /// fsevent for cur/<id>:2,F would still match the lingering
    /// cache entry and silently drop the real flag change.
    #[test]
    fn matches_all_evicts_matched_entries() {
        let cache = SelfWriteCache::new(Duration::from_secs(60));
        let new_p = PathBuf::from("/Mail/INBOX/new/a:2,F");
        let cur_p = PathBuf::from("/Mail/INBOX/cur/a:2,F");
        cache.record([new_p.clone(), cur_p.clone()]);
        assert!(cache.matches_all(&[new_p.clone(), cur_p.clone()]));
        // Both entries gone; a second batch of the same paths must
        // not match.
        assert!(!cache.matches_all(&[new_p, cur_p]));
    }

    /// A failed match leaves all entries intact -- we only consume
    /// when the predicate as a whole holds, so a single missing
    /// path doesn't silently strip the others.
    #[test]
    fn failed_match_leaves_entries_intact() {
        let cache = SelfWriteCache::new(Duration::from_secs(60));
        let known = PathBuf::from("/Mail/INBOX/new/a:2,F");
        let unknown = PathBuf::from("/Mail/INBOX/cur/a:2,FS");
        cache.record([known.clone()]);
        assert!(!cache.matches_all(&[known.clone(), unknown]));
        // The known entry survived the failed match; a fresh
        // single-path batch can still match it.
        assert!(cache.matches_all(&[known]));
    }

    /// A batch where ANY path is missing from the cache must not
    /// match. Pins the path-exact semantics that keeps a flag-
    /// changing promotion (cur/<id>:2,FS not in cache because we
    /// only predicted cur/<id>:2,F) from being silently dropped --
    /// this scenario falls through to a real cycle so reconcile
    /// can push the new flag to the server.
    #[test]
    fn flag_changing_promotion_falls_through() {
        let cache = SelfWriteCache::new(Duration::from_secs(60));
        let known = PathBuf::from("/Mail/INBOX/new/a:2,F");
        let unknown = PathBuf::from("/Mail/INBOX/cur/a:2,FS");
        cache.record([known.clone()]);
        assert!(!cache.matches_all(&[known, unknown]));
    }

    /// Empty batch returns false. Defensive: callers shouldn't
    /// pass empty batches, and "all match" on no input would be a
    /// vacuous true that could short-circuit a real code path.
    #[test]
    fn empty_batch_does_not_match() {
        let cache = SelfWriteCache::new(Duration::from_secs(60));
        cache.record([PathBuf::from("/Mail/INBOX/new/a:2,F")]);
        assert!(!cache.matches_all(&[]));
    }

    /// Entries past the TTL are not recognised. Pins that a stale
    /// recording can't suppress a fresh user-driven event.
    #[test]
    fn expired_entries_do_not_match() {
        let cache = SelfWriteCache::new(Duration::from_millis(20));
        let p = PathBuf::from("/Mail/INBOX/new/a:2,F");
        cache.record([p.clone()]);
        thread::sleep(Duration::from_millis(40));
        assert!(!cache.matches_all(&[p]));
    }

    /// Recording opportunistically GCs expired entries so memory
    /// stays bounded under sustained delivery activity. Sketch
    /// rather than a tight bound: insert one, expire it, insert
    /// another, expect only the second to survive.
    #[test]
    fn record_gcs_expired_entries() {
        let cache = SelfWriteCache::new(Duration::from_millis(20));
        let stale = PathBuf::from("/Mail/INBOX/new/a:2,F");
        cache.record([stale.clone()]);
        thread::sleep(Duration::from_millis(40));
        let fresh = PathBuf::from("/Mail/INBOX/new/b:2,F");
        cache.record([fresh.clone()]);
        let map = cache.entries.lock().unwrap();
        assert_eq!(map.len(), 1, "stale entry should have been GC'd");
        assert!(map.contains_key(&fresh));
    }
}
