use std::collections::HashMap;

use crate::keywords::{KeywordMutation, KeywordPatch};

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
/// RFC 8621 defines $draft, $flagged, $answered, $forwarded and $seen.
/// jma additionally maps Maildir T to $deleted; that name is a jma
/// convention, not one of those RFC system keywords. Other server
/// keywords pass through the state DB without a filename mapping.
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

/// Replace only owned filename flags, retaining local suffix extensions.
pub fn merge_mapped_flags(current: &str, desired: &str) -> String {
    let owned = |flag: char| {
        FLAG_MAPPINGS
            .iter()
            .any(|(_, mapped)| mapped.starts_with(flag))
    };
    let mut result: Vec<char> = current
        .chars()
        .filter(|flag| !owned(*flag))
        .chain(desired.chars().filter(|flag| owned(*flag)))
        .collect();
    result.sort_unstable();
    result.dedup();
    result.into_iter().collect()
}

/// Set or remove each of the six mapped keywords, leaving every
/// unowned keyword unmentioned. The wire encoder emits Set as true and
/// Remove as null; this is not a replacement keyword property map.
pub fn flags_to_keyword_patch(flags: &str) -> KeywordPatch {
    FLAG_MAPPINGS
        .iter()
        .map(|(keyword, flag)| {
            (
                keyword.to_string(),
                if flags.contains(*flag) {
                    KeywordMutation::Set
                } else {
                    KeywordMutation::Remove
                },
            )
        })
        .collect()
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

    /// Every owned key needs an explicit operation, including removals.
    /// Wire-format assertions live in the request and stateful tests.
    #[test]
    fn test_flags_to_keyword_patch_covers_all_six_explicitly() {
        let patch = flags_to_keyword_patch("FS");
        assert_eq!(patch.len(), 6, "patch must cover every standard keyword");
        assert_eq!(patch.get("$flagged"), Some(&KeywordMutation::Set));
        assert_eq!(patch.get("$seen"), Some(&KeywordMutation::Set));
        assert_eq!(patch.get("$forwarded"), Some(&KeywordMutation::Remove));
        assert_eq!(patch.get("$draft"), Some(&KeywordMutation::Remove));
        assert_eq!(patch.get("$answered"), Some(&KeywordMutation::Remove));
        assert_eq!(patch.get("$deleted"), Some(&KeywordMutation::Remove));
    }

    /// Empty suffix removes all six owned keys, not unowned keywords.
    #[test]
    fn test_flags_to_keyword_patch_empty_clears_all_six() {
        let patch = flags_to_keyword_patch("");
        assert_eq!(patch.len(), 6);
        for (kw, _) in FLAG_MAPPINGS {
            assert_eq!(
                patch.get(*kw),
                Some(&KeywordMutation::Remove),
                "{kw} must be removed"
            );
        }
    }
}
