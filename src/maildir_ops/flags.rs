use std::collections::HashMap;

/// JMAP keyword to Maildir flag mapping.
///
/// Maildir flags (in the :2, suffix) are single uppercase chars, sorted:
///   D = Draft, F = Flagged, R = Replied, S = Seen, T = Trashed
///
/// JMAP keywords:
///   $draft, $flagged, $answered, $seen, $deleted (and others)
const FLAG_MAPPINGS: &[(&str, &str)] = &[
    ("$draft", "D"),
    ("$flagged", "F"),
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
        keywords.insert("$draft".to_string(), true);
        keywords.insert("$deleted".to_string(), true);
        assert_eq!(keywords_to_flags(&keywords), "DFRST");
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
}
