//! On-disk layout for hierarchical mailbox trees.
//!
//! A maildir, classically, is a single flat folder of messages --
//! `cur`/`new`/`tmp` subdirectories and nothing else. There's no
//! native concept of a folder hierarchy. Different tools have invented
//! different conventions for laying a hierarchical mailbox tree onto a
//! maildir root, and this module is where jma picks one.
//!
//! `FolderLayout` enumerates the three conventions worth supporting;
//! `resolve_folder_path` walks an upstream mailbox's parent chain and
//! returns the on-disk folder name under the chosen layout.
//!
//! Filesystem safety on server-supplied segment names (no `..`, no
//! `/`, no NUL, length cap, etc.) is the responsibility of the JMAP-
//! side validator at `Mailbox/get` time, not this module. When a
//! future config knob lets a user remap a server name to a chosen
//! local name, that remap will need its own validator too -- the
//! user-supplied alternate hasn't passed through the JMAP boundary,
//! so neither validator above is on its path. Both validators feed
//! this module already-safe segments; this module focuses on the two
//! concerns it can decide alone: per-layout segment rules (no
//! `cur/new/tmp` under `Fs`, no leading-dot under `MaildirPP`, etc.)
//! and the joined-result length cap on layouts that flatten the path
//! into a single directory entry.

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};

use crate::config::{CompiledRenameRule, Config, FolderLayout};
use crate::ids::JmapMailboxId;
use crate::jmap::types::MailboxObject;

pub struct FolderLayoutDefinition {
    layout: FolderLayout,
    separator: char,
    joined_name_cap: usize,
    rename_rules: Vec<CompiledRenameRule>,
}

impl FolderLayoutDefinition {
    pub fn from_config(config: &Config, name_cap: usize) -> Self {
        FolderLayoutDefinition {
            layout: config.sync.folder_layout,
            separator: config.sync.hierarchy_separator,
            joined_name_cap: name_cap,
            rename_rules: config.compiled_rename_rules.clone(),
        }
    }
}

/// Per-layout segment rules. Each layout has its own forbidden
/// segment shapes that only matter once a segment is spliced into the
/// layout's flattened form. Filesystem safety on the segment itself
/// (path traversal, NUL bytes, `/` or `\`, length cap) is *not*
/// re-checked here: see the module-level note on the upstream
/// validators that own that. Called once per ancestor segment by
/// `resolve_folder_path`.
fn validate_segment_for_layout(name: &str, layout: FolderLayout, separator: char) -> Result<()> {
    match layout {
        FolderLayout::Flat => {
            if name.contains(separator) {
                anyhow::bail!(
                    "mailbox name {:?} contains the configured hierarchy separator {:?}",
                    name,
                    separator
                );
            }
        }
        FolderLayout::MaildirPP => {
            if name.contains(separator) {
                anyhow::bail!(
                    "mailbox name {:?} contains the configured hierarchy separator {:?}",
                    name,
                    separator
                );
            }
            if name.starts_with('.') {
                anyhow::bail!(
                    "Maildir++ forbids segments starting with '.' (would produce '..'): {:?}",
                    name
                );
            }
        }
        FolderLayout::Fs => {
            if matches!(name, "cur" | "new" | "tmp") {
                anyhow::bail!(
                    "FS layout forbids segments named cur/new/tmp \
                     (collide with maildir internals): {:?}",
                    name
                );
            }
            if name.starts_with('.') {
                anyhow::bail!(
                    "FS layout forbids segments starting with '.' \
                     (collide with hidden files / jma state markers): {:?}",
                    name
                );
            }
        }
    }
    Ok(())
}

/// Resolve an upstream mailbox to its on-disk folder name by walking
/// the `parent_id` chain rooted at `mb` and formatting the segments
/// under the chosen `layout`. The inbox-role segment is replaced with
/// the literal `"INBOX"` (mbsync convention; matches the magic alias
/// accepted in `[sync].mailboxes`), at whichever depth it sits in the
/// chain.
///
/// Defends against three malicious or buggy server shapes:
///
/// - cycles in the parent chain (`A.parent_id = B`, `B.parent_id = A`),
///   detected via a per-call seen set;
/// - parent_id pointing at an id absent from `by_id`, surfaced as a
///   clear error rather than a silent truncation;
/// - segments whose names are invalid under the chosen layout (see
///   `validate_segment_for_layout`).
///
/// `joined_name_cap` is the byte ceiling for the *joined* result
/// under `Flat` and `MaildirPP`, where the whole flattened path
/// becomes a single directory entry subject to the filesystem's
/// `NAME_MAX`. Pass the filesystem cap (255 on every filesystem
/// jma will plausibly run on); upstream is responsible for
/// segment-level caps. `Fs` joins with `/` so each segment is its
/// own directory entry; the joined result has no additional cap and
/// `joined_name_cap` is ignored.
pub fn resolve_folder_path(
    mb: &MailboxObject,
    by_id: &HashMap<JmapMailboxId, &MailboxObject>,
    layout: &FolderLayoutDefinition,
) -> Result<String> {
    let mut chain: Vec<&MailboxObject> = Vec::new();
    let mut seen: HashSet<JmapMailboxId> = HashSet::new();
    let mut cur: &MailboxObject = mb;
    loop {
        if !seen.insert(cur.id.clone()) {
            anyhow::bail!("parent chain cycles at mailbox {}", cur.id);
        }
        chain.push(cur);
        match &cur.parent_id {
            None => break,
            Some(pid) => match by_id.get(pid) {
                Some(parent) => cur = *parent,
                None => anyhow::bail!("mailbox {} references unknown parent_id {}", cur.id, pid),
            },
        }
    }
    chain.reverse();

    let segments: Vec<String> = chain
        .iter()
        .map(|m| -> Result<String> {
            let raw = if m.role.as_deref() == Some("inbox") {
                "INBOX".to_string()
            } else {
                m.name.clone()
            };
            validate_segment_for_layout(&raw, layout.layout, layout.separator)
                .with_context(|| format!("rejecting mailbox id={}", m.id))?;
            Ok(raw)
        })
        .collect::<Result<Vec<_>>>()?;

    let sep_str = layout.separator.to_string();

    if let Some(match_result) = maybe_match_rename_rules(&segments, &layout.rename_rules) {
        if match_result.len() > layout.joined_name_cap {
            anyhow::bail!(
                "folder path from rule exceeds {} bytes ({} bytes): {:?}",
                layout.joined_name_cap,
                match_result.len(),
                match_result
            );
        }

        return Ok(match_result);
    }

    let result = match layout.layout {
        FolderLayout::Flat => segments.join(&sep_str),
        FolderLayout::MaildirPP => format!(".{}", segments.join(&sep_str)),
        FolderLayout::Fs => segments.join("/"),
    };

    if matches!(layout.layout, FolderLayout::Flat | FolderLayout::MaildirPP)
        && result.len() > layout.joined_name_cap
    {
        anyhow::bail!(
            "flattened folder path exceeds {} bytes ({} bytes): {:?}",
            layout.joined_name_cap,
            result.len(),
            result
        );
    }

    Ok(result)
}

fn maybe_match_rename_rules(segments: &[String], rules: &[CompiledRenameRule]) -> Option<String> {
    let maildir_as_path = segments.join("/");
    for rule in rules.iter() {
        match rule {
            CompiledRenameRule::MapDirectly {
                source_folder_path,
                renamed_name,
            } => {
                if maildir_as_path == *source_folder_path {
                    return Some(renamed_name.clone());
                }
            }
            CompiledRenameRule::Pattern {
                source_folder_pattern,
                rename_pattern,
            } => {
                if source_folder_pattern.is_match(&maildir_as_path) {
                    return Some(
                        source_folder_pattern
                            .replace_all(&maildir_as_path, rename_pattern)
                            .to_string(),
                    );
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jmap::limits::MAX_MAILBOX_NAME_LEN;
    use regex::Regex;

    fn mb_full(id: &str, name: &str, parent: Option<&str>, role: Option<&str>) -> MailboxObject {
        MailboxObject {
            id: JmapMailboxId::from(id),
            name: name.to_string(),
            parent_id: parent.map(JmapMailboxId::from),
            role: role.map(str::to_string),
            sort_order: 0,
            total_emails: 0,
            unread_emails: 0,
        }
    }

    fn build_index(mailboxes: &[MailboxObject]) -> HashMap<JmapMailboxId, &MailboxObject> {
        mailboxes.iter().map(|m| (m.id.clone(), m)).collect()
    }

    fn resolve(
        target: &MailboxObject,
        all: &[MailboxObject],
        layout: FolderLayout,
        sep: char,
    ) -> Result<String> {
        let idx = build_index(all);
        let layout_definition = FolderLayoutDefinition {
            layout,
            separator: sep,
            joined_name_cap: MAX_MAILBOX_NAME_LEN,
            rename_rules: Vec::default(),
        };
        resolve_folder_path(target, &idx, &layout_definition)
    }

    /// One row of the resolution table: an input mailbox tree, a
    /// target id to resolve, the separator to use, and the expected
    /// on-disk path under each layout. Adding a new test case is
    /// appending one row -- no new `#[test]` needed.
    struct ResolveCase {
        label: &'static str,
        /// `(id, name, parent_id, role)` per mailbox.
        tree: &'static [(
            &'static str,
            &'static str,
            Option<&'static str>,
            Option<&'static str>,
        )],
        target: &'static str,
        separator: char,
        flat: &'static str,
        maildir_pp: &'static str,
        fs: &'static str,
    }

    /// Folder-resolution table. Read top-to-bottom: each row says
    /// "this input tree, resolved at this target, produces this path
    /// under each layout." A new test case is one new row.
    ///
    /// The Fs column ignores the separator (Fs always joins with `/`
    /// regardless), so a row using a non-`.` separator still produces
    /// a `/`-joined Fs result -- that's the point of the
    /// `custom-separator` row, which doubles as proof that the
    /// separator argument is inert under Fs.
    const RESOLVE_CASES: &[ResolveCase] = &[
        ResolveCase {
            label: "top-level mailbox",
            tree: &[("m", "Archive", None, None)],
            target: "m",
            separator: '.',
            flat: "Archive",
            maildir_pp: ".Archive",
            fs: "Archive",
        },
        // Server-localized inbox name; alias replaces it before any
        // layout formatting. Pins the magic-INBOX rule across all
        // three layouts so a future fourth layout can't silently drop
        // it.
        ResolveCase {
            label: "top-level INBOX role uses alias regardless of server name",
            tree: &[("m", "Indbakke", None, Some("inbox"))],
            target: "m",
            separator: '.',
            flat: "INBOX",
            maildir_pp: ".INBOX",
            fs: "INBOX",
        },
        ResolveCase {
            label: "two-level: [Airmail] / Sent",
            tree: &[
                ("p", "[Airmail]", None, None),
                ("c", "Sent", Some("p"), None),
            ],
            target: "c",
            separator: '.',
            flat: "[Airmail].Sent",
            maildir_pp: ".[Airmail].Sent",
            fs: "[Airmail]/Sent",
        },
        ResolveCase {
            label: "three-level chain walks full ancestry",
            tree: &[
                ("g", "A", None, None),
                ("p", "B", Some("g"), None),
                ("c", "C", Some("p"), None),
            ],
            target: "c",
            separator: '.',
            flat: "A.B.C",
            maildir_pp: ".A.B.C",
            fs: "A/B/C",
        },
        // Inbox alias substitutes the inbox-role segment at any depth,
        // not just at the leaf. A child of Inbox lands under INBOX
        // regardless of the server's localized name for the inbox.
        ResolveCase {
            label: "INBOX alias propagates to descendants",
            tree: &[
                ("i", "Indbakke", None, Some("inbox")),
                ("c", "Receipts", Some("i"), None),
            ],
            target: "c",
            separator: '.',
            flat: "INBOX.Receipts",
            maildir_pp: ".INBOX.Receipts",
            fs: "INBOX/Receipts",
        },
        // Custom separator threads through Flat and MaildirPP; Fs
        // ignores it and still uses `/`. One row, both behaviors.
        ResolveCase {
            label: "custom separator ':' (Fs ignores it)",
            tree: &[("p", "Foo", None, None), ("c", "Bar", Some("p"), None)],
            target: "c",
            separator: ':',
            flat: "Foo:Bar",
            maildir_pp: ".Foo:Bar",
            fs: "Foo/Bar",
        },
    ];

    #[test]
    fn resolve_folder_path_table() {
        for case in RESOLVE_CASES {
            let mbs: Vec<MailboxObject> = case
                .tree
                .iter()
                .map(|(id, name, parent, role)| mb_full(id, name, *parent, *role))
                .collect();
            let target = mbs
                .iter()
                .find(|m| m.id.as_ref() == case.target)
                .unwrap_or_else(|| {
                    panic!("[{}] target id {:?} not in tree", case.label, case.target)
                });
            for (layout, expected) in [
                (FolderLayout::Flat, case.flat),
                (FolderLayout::MaildirPP, case.maildir_pp),
                (FolderLayout::Fs, case.fs),
            ] {
                let got = resolve(target, &mbs, layout, case.separator)
                    .unwrap_or_else(|e| panic!("[{}] {:?}: {e:#}", case.label, layout));
                assert_eq!(got, expected, "[{}] under {:?}", case.label, layout);
            }
        }
    }

    #[test]
    fn resolve_folder_using_direct_mapping_skips_normal_resolution() {
        let mbs: Vec<MailboxObject> = [("p", "Foo", None, None), ("c", "Bar", Some("p"), None)]
            .iter()
            .map(|(id, name, parent, role)| mb_full(id, name, *parent, *role))
            .collect();
        let target = mbs.iter().find(|m| m.id.as_ref() == "c").unwrap();
        let idx = build_index(&mbs);
        let match_rule = CompiledRenameRule::MapDirectly {
            source_folder_path: "Foo/Bar".to_string(),
            renamed_name: "Foo::Bar".to_string(),
        };

        let layout_definition = FolderLayoutDefinition {
            layout: FolderLayout::Fs,
            separator: '.',
            joined_name_cap: MAX_MAILBOX_NAME_LEN,
            rename_rules: vec![match_rule],
        };
        let got = resolve_folder_path(target, &idx, &layout_definition);
        assert!(got.is_ok());
        assert_eq!(got.unwrap(), "Foo::Bar");
    }

    #[test]
    fn resolve_folder_using_pattern_mapping_skips_normal_resolution() {
        let mbs: Vec<MailboxObject> = [("p", "Foo", None, None), ("c", "Bar", Some("p"), None)]
            .iter()
            .map(|(id, name, parent, role)| mb_full(id, name, *parent, *role))
            .collect();
        let target = mbs.iter().find(|m| m.id.as_ref() == "c").unwrap();
        let idx = build_index(&mbs);
        let match_rule = CompiledRenameRule::Pattern {
            source_folder_pattern: Regex::new(r"(.*)o/Bar").unwrap(),
            rename_pattern: "Foo::Foo".to_string(),
        };

        let layout_definition = FolderLayoutDefinition {
            layout: FolderLayout::Fs,
            separator: '.',
            joined_name_cap: MAX_MAILBOX_NAME_LEN,
            rename_rules: vec![match_rule],
        };
        let got = resolve_folder_path(target, &idx, &layout_definition);
        assert!(got.is_ok());
        assert_eq!(got.unwrap(), "Foo::Foo");
    }

    #[test]
    fn resolve_folder_using_rules_first_one_wins() {
        let mbs: Vec<MailboxObject> = [("p", "Foo", None, None), ("c", "Bar", Some("p"), None)]
            .iter()
            .map(|(id, name, parent, role)| mb_full(id, name, *parent, *role))
            .collect();
        let target = mbs.iter().find(|m| m.id.as_ref() == "c").unwrap();
        let idx = build_index(&mbs);
        let pattern_match_rule = CompiledRenameRule::Pattern {
            source_folder_pattern: Regex::new(r"(.*)o/Bar").unwrap(),
            rename_pattern: "Foo::Foo".to_string(),
        };
        let direct_match_rule = CompiledRenameRule::MapDirectly {
            source_folder_path: "Foo/Bar".to_string(),
            renamed_name: "Foo::Bar".to_string(),
        };
        let layout_definition_pattern_first = FolderLayoutDefinition {
            layout: FolderLayout::Fs,
            separator: '.',
            joined_name_cap: MAX_MAILBOX_NAME_LEN,
            rename_rules: vec![pattern_match_rule.clone(), direct_match_rule.clone()],
        };
        let layout_definition_pattern_last = FolderLayoutDefinition {
            layout: FolderLayout::Fs,
            separator: '.',
            joined_name_cap: MAX_MAILBOX_NAME_LEN,
            rename_rules: vec![direct_match_rule.clone(), pattern_match_rule.clone()],
        };
        let got = resolve_folder_path(target, &idx, &layout_definition_pattern_first);
        assert!(got.is_ok());
        assert_eq!(got.unwrap(), "Foo::Foo");

        let got = resolve_folder_path(target, &idx, &layout_definition_pattern_last);
        assert!(got.is_ok());
        assert_eq!(got.unwrap(), "Foo::Bar");
    }

    #[test]
    fn resolve_rejects_cycle_in_parent_chain() {
        // a -> b -> a (forged by a malicious server)
        let mbs = vec![
            mb_full("a", "A", Some("b"), None),
            mb_full("b", "B", Some("a"), None),
        ];
        let err = resolve(&mbs[0], &mbs, FolderLayout::Flat, '.').unwrap_err();
        assert!(format!("{}", err).contains("cycle"), "got: {err}");
    }

    #[test]
    fn resolve_rejects_orphan_parent_id() {
        let mbs = vec![mb_full("c", "Child", Some("missing"), None)];
        let err = resolve(&mbs[0], &mbs, FolderLayout::Flat, '.').unwrap_err();
        assert!(
            format!("{}", err).contains("unknown parent_id"),
            "got: {err}"
        );
    }

    /// Each segment is within cap; joined result blows it. Flat and
    /// Maildir++ both become a single directory entry on disk so the
    /// joined length matters; pinning both layouts so a refactor that
    /// accidentally narrows the cap to one of them gets caught.
    #[test]
    fn resolve_rejects_overlong_flattened_path() {
        let mbs = vec![
            mb_full("a", &"a".repeat(200), None, None),
            mb_full("b", &"b".repeat(200), Some("a"), None),
        ];
        for layout in [FolderLayout::Flat, FolderLayout::MaildirPP] {
            let err = resolve(&mbs[1], &mbs, layout, '.').unwrap_err();
            assert!(
                format!("{}", err).contains("exceeds"),
                "for {layout:?}: got {err}"
            );
        }
    }

    /// Fs joins with '/' so each segment is independently capped and
    /// the joined path has no additional cap. Pins the asymmetry so a
    /// future "tidy up validation" pass doesn't accidentally apply the
    /// flat cap to fs paths.
    #[test]
    fn resolve_fs_does_not_apply_join_cap() {
        let mbs = vec![
            mb_full("a", &"a".repeat(200), None, None),
            mb_full("b", &"b".repeat(200), Some("a"), None),
        ];
        let path = resolve(&mbs[1], &mbs, FolderLayout::Fs, '.').unwrap();
        assert_eq!(path.len(), 401);
    }

    #[test]
    fn resolve_flat_rejects_segment_containing_separator() {
        let mbs = vec![mb_full("m", "foo.bar", None, None)];
        let err = resolve(&mbs[0], &mbs, FolderLayout::Flat, '.').unwrap_err();
        // {:#} flattens the anyhow context chain into one string, so we
        // can match the inner per-layout reason rather than the outer
        // "rejecting mailbox id=..." wrapper.
        assert!(format!("{:#}", err).contains("separator"), "got: {err:#}");
    }

    /// Maildir++ spec forbids any subdirectory starting with '..' --
    /// implemented as "no segment starts with '.'", since our prepended
    /// '.' would otherwise produce a forbidden double dot. Uses a
    /// non-default separator so the contains-separator rule (which '.'
    /// would also trigger here) doesn't mask the leading-dot check.
    #[test]
    fn resolve_maildir_pp_rejects_segment_starting_with_dot() {
        let mbs = vec![mb_full("m", ".hidden", None, None)];
        let err = resolve(&mbs[0], &mbs, FolderLayout::MaildirPP, ':').unwrap_err();
        assert!(format!("{:#}", err).contains("Maildir++"), "got: {err:#}");
    }

    #[test]
    fn resolve_fs_rejects_cur_new_tmp_segments() {
        for name in ["cur", "new", "tmp"] {
            let mbs = vec![mb_full("m", name, None, None)];
            let err = resolve(&mbs[0], &mbs, FolderLayout::Fs, '.').unwrap_err();
            assert!(
                format!("{:#}", err).contains("cur/new/tmp"),
                "for {name:?}: got {err:#}"
            );
        }
    }

    #[test]
    fn resolve_fs_rejects_segments_starting_with_dot() {
        let mbs = vec![mb_full("m", ".hidden", None, None)];
        let err = resolve(&mbs[0], &mbs, FolderLayout::Fs, '.').unwrap_err();
        assert!(format!("{:#}", err).contains("FS layout"), "got: {err:#}");
    }
}
