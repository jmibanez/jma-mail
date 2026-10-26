//! Keyword observations and keyword mutations have different meanings.
//!
//! A full JMAP keyword set contains present keys only. A patch must also
//! represent explicit removals; omission means leave the server value alone.

use std::collections::HashMap;
use std::ops::Deref;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeywordMutation {
    Set,
    Remove,
}

/// Changes to individual keywords, never a replacement keyword set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeywordPatch(HashMap<String, KeywordMutation>);

impl KeywordPatch {
    pub fn apply_to(&self, existing: &HashMap<String, bool>) -> HashMap<String, bool> {
        let mut result = existing.clone();
        for (keyword, mutation) in &self.0 {
            match mutation {
                KeywordMutation::Set => {
                    result.insert(keyword.clone(), true);
                }
                KeywordMutation::Remove => {
                    result.remove(keyword);
                }
            }
        }
        result
    }
}

impl Deref for KeywordPatch {
    type Target = HashMap<String, KeywordMutation>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromIterator<(String, KeywordMutation)> for KeywordPatch {
    fn from_iter<T: IntoIterator<Item = (String, KeywordMutation)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// Explicit conversion for callers that already describe set/remove booleans.
/// False is a removal operation here, not a JMAP keyword property value.
impl From<HashMap<String, bool>> for KeywordPatch {
    fn from(values: HashMap<String, bool>) -> Self {
        values
            .into_iter()
            .map(|(key, set)| {
                (
                    key,
                    if set {
                        KeywordMutation::Set
                    } else {
                        KeywordMutation::Remove
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::maildir_ops::flags::{flags_to_keyword_patch, flags_to_keywords, keywords_to_flags};

    #[test]
    fn every_mapped_flag_transition_preserves_unowned_keywords() {
        let flags = ['D', 'F', 'P', 'R', 'S', 'T'];
        let suffix = |mask: usize| {
            flags
                .iter()
                .enumerate()
                .filter_map(|(bit, flag)| ((mask & (1 << bit)) != 0).then_some(*flag))
                .collect::<String>()
        };
        for old in 0..64 {
            let mut observed = flags_to_keywords(&suffix(old));
            observed.insert("user-label".into(), true);
            observed.insert("$junk".into(), true);
            for new in 0..64 {
                let desired = suffix(new);
                let patch = flags_to_keyword_patch(&desired);
                assert_eq!(patch.len(), 6);
                let result = patch.apply_to(&observed);
                assert_eq!(keywords_to_flags(&result), desired, "{old} -> {new}");
                assert_eq!(result.get("user-label"), Some(&true));
                assert_eq!(result.get("$junk"), Some(&true));
                assert!(!patch.contains_key("user-label"));
                assert!(!patch.contains_key("$junk"));
            }
        }
    }
}
