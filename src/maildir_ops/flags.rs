use std::collections::HashMap;

/// JMAP keyword to Maildir flag mapping.
///
/// Maildir flags (in the :2, suffix) are single uppercase chars, sorted:
///   D = Draft, F = Flagged, P = Passed, R = Replied, S = Seen, T = Trashed
///
/// These are the six standard flags Bernstein's original maildir spec
/// defines (cr.yp.to/proto/maildir.html). User-defined keywords from
/// JMAP are not represented here -- those are tracked in the state DB
/// as `message_map.jmap_keywords` and survive jma<->server round-trips,
/// but do not currently appear in the maildir filename suffix. Opt-in
/// Dovecot-compatible lowercase-keyword storage is a separate feature.
///
/// JMAP keywords (RFC 8621): $draft, $flagged, $answered, $forwarded,
/// $seen, $deleted are the spec-defined names mirrored here. Other
/// standard names ($phishing, $junk, $notjunk) and any user-defined
/// keywords pass through the state DB only.
const FLAG_MAPPINGS: &[(&str, &str)] = &[
    ("$draft", "D"),
    ("$flagged", "F"),
    ("$forwarded", "P"),
    ("$answered", "R"),
    ("$seen", "S"),
    ("$deleted", "T"),
];

/// Convert JMAP keywords to a Maildir flags string (sorted).
pub fn keywords_to_flags(keywords: &HashMap<String, bool>) -> String {
    let mut flags: Vec<&str> = FLAG_MAPPINGS
        .iter()
        .filter(|(kw, _)| keywords.get(*kw).copied().unwrap_or(false))
        .map(|(_, flag)| *flag)
        .collect();
    flags.sort();
    flags.concat()
}

/// Convert a Maildir flags string to JMAP keywords.
pub fn flags_to_keywords(flags: &str) -> HashMap<String, bool> {
    let mut keywords = HashMap::new();
    for (kw, flag) in FLAG_MAPPINGS {
        if flags.contains(*flag) {
            keywords.insert(kw.to_string(), true);
        }
    }
    keywords
}

/// Build a full-coverage patch for the six standard JMAP keywords from
/// a Maildir flags string: each of $draft, $flagged, $forwarded,
/// $answered, $seen, $deleted gets an explicit `true` or `false` entry
/// according to whether its flag letter is present in `flags`. Used in
/// patch-style `Email/set` `keywords/<name>` updates where the goal is
/// to make the server's view of the *standard* set agree with the
/// on-disk filename, without touching server-only keywords ($imported,
/// $hasattachment, $x-me-annot-2, user-defined labels).
///
/// `flags_to_keywords` only emits `true` entries, which under JMAP's
/// patch semantics means "set these on, leave everything else alone"
/// -- it cannot remove a standard keyword the server has set. This
/// function pairs with `set_email_batch`'s per-key `upd.keyword(kw,
/// val)` wire shape so a `false` here patches `keywords/<name>: null`
/// and actually clears the keyword.
pub fn flags_to_keyword_patch(flags: &str) -> HashMap<String, bool> {
    let mut keywords = HashMap::new();
    for (kw, flag) in FLAG_MAPPINGS {
        keywords.insert(kw.to_string(), flags.contains(*flag));
    }
    keywords
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keywords_to_flags() {
        let mut keywords = HashMap::new();
        keywords.insert("$seen".to_string(), true);
        keywords.insert("$flagged".to_string(), true);
        assert_eq!(keywords_to_flags(&keywords), "FS");
    }

    #[test]
    fn test_keywords_to_flags_all() {
        let mut keywords = HashMap::new();
        keywords.insert("$seen".to_string(), true);
        keywords.insert("$answered".to_string(), true);
        keywords.insert("$flagged".to_string(), true);
        keywords.insert("$forwarded".to_string(), true);
        keywords.insert("$draft".to_string(), true);
        keywords.insert("$deleted".to_string(), true);
        assert_eq!(keywords_to_flags(&keywords), "DFPRST");
    }

    /// $forwarded <-> P is the sixth flag from Bernstein's original
    /// maildir spec. Pin both directions so a future "trim the
    /// mappings down" refactor can't silently drop it. notmuch,
    /// NeoMutt, and Gnus all recognize P; omitting it produces
    /// silent round-trip loss on forwarded messages.
    #[test]
    fn test_forwarded_round_trips() {
        let mut keywords = HashMap::new();
        keywords.insert("$forwarded".to_string(), true);
        let flags = keywords_to_flags(&keywords);
        assert_eq!(flags, "P");

        let round_tripped = flags_to_keywords(&flags);
        assert_eq!(round_tripped.get("$forwarded"), Some(&true));
        assert_eq!(round_tripped.len(), 1);
    }

    #[test]
    fn test_keywords_to_flags_empty() {
        let keywords = HashMap::new();
        assert_eq!(keywords_to_flags(&keywords), "");
    }

    #[test]
    fn test_flags_to_keywords() {
        let keywords = flags_to_keywords("FS");
        assert_eq!(keywords.get("$seen"), Some(&true));
        assert_eq!(keywords.get("$flagged"), Some(&true));
        assert_eq!(keywords.get("$answered"), None);
    }

    /// Patch shape must include every standard JMAP keyword with
    /// an explicit `true`/`false` value derived from the filename
    /// suffix. The `false` half is the load-bearing part: the JMAP
    /// patch wire-shape only clears a keyword when its value is
    /// patched to `null` (which the jmap-client `keyword(name,
    /// false)` call serializes as), so a caller that wants to
    /// remove e.g. server-side `$forwarded` to match a filename
    /// missing the `P` letter relies on this function's `false`
    /// entries. Pin all six so a refactor narrowing the loop
    /// can't silently break removal semantics.
    #[test]
    fn test_flags_to_keyword_patch_covers_all_six_explicitly() {
        let patch = flags_to_keyword_patch("FS");
        assert_eq!(patch.len(), 6, "patch must cover every standard keyword");
        assert_eq!(patch.get("$flagged"), Some(&true));
        assert_eq!(patch.get("$seen"), Some(&true));
        assert_eq!(patch.get("$forwarded"), Some(&false));
        assert_eq!(patch.get("$draft"), Some(&false));
        assert_eq!(patch.get("$answered"), Some(&false));
        assert_eq!(patch.get("$deleted"), Some(&false));
    }

    /// Empty suffix => every standard keyword explicitly `false`.
    /// Matches the wire intent for "make the server forget every
    /// standard keyword on this email."
    #[test]
    fn test_flags_to_keyword_patch_empty_clears_all_six() {
        let patch = flags_to_keyword_patch("");
        assert_eq!(patch.len(), 6);
        for (kw, _) in FLAG_MAPPINGS {
            assert_eq!(patch.get(*kw), Some(&false), "{kw} must patch to false");
        }
    }
}
