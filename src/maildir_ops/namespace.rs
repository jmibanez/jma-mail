//! jma's private namespace within the user's maildir root.
//!
//! jma persists a handful of sidecar files and directories alongside
//! the user's mail folders -- the state DB (`.jma.db` and its WAL/SHM
//! siblings), the cross-process lock (`.jma.lock`), the rescue
//! maildir (`.jma-rescue`), and so on. They share a `.jma.` or
//! `.jma-` prefix so any walker that scans the maildir can tell
//! jma's own files apart from synced mail without an exhaustive
//! enumeration of every sidecar name.
//!
//! Two prefix shapes coexist: `.jma.<x>` for filenames whose names
//! don't need to survive Maildir++ layout parsing (sentinels,
//! lockfiles, the state DB), and `.jma-<x>` for top-level *folders*
//! that need to be a single Maildir++ segment (Maildir++ uses `.`
//! as its hierarchy separator, so `.jma.rescue` would parse as a
//! `jma/rescue` hierarchy under that layout).
//!
//! New sidecar names that honour either prefix are recognised by
//! every caller of `is_jma_private` without further plumbing.

/// Default filename for the state DB at the maildir root
/// (`<maildir_path>/.jma.db`). Used when `[state].db_path` is unset.
pub const STATE_DB_FILENAME: &str = ".jma.db";

/// Filename for the cross-process advisory lock on a maildir
/// (`<maildir_root>/.jma.lock`).
pub const MAILDIR_LOCK_FILENAME: &str = ".jma.lock";

/// Basename of the sentinel file inside a per-folder maildir
/// directory. Matches the `.jma.*` namespace used by `.jma.lock`
/// and the default `.jma.db` so it never collides with mail files
/// or other MUAs' state.
pub const SENTINEL_FILENAME: &str = ".jma.mapping";

/// Name of the rescue maildir at the root
/// (`<maildir_root>/.jma-rescue`). The dash variant of the
/// namespace keeps the name a single Maildir++ segment (Maildir++
/// uses `.` as a hierarchy separator). The folder itself is a
/// regular maildir with `cur/`, `new/`, `tmp/`; jma stages
/// unmapped files into it when destroying an orphan folder so the
/// user can recover content the destroy would otherwise sweep up.
pub const RESCUE_FOLDER_NAME: &str = ".jma-rescue";

/// Whether a single path-component name belongs to jma's private
/// namespace at the maildir root. Operates on the file or directory
/// name (`OsStr::to_string_lossy()` output is fine), not a full
/// path. Matches both prefix shapes: `.jma.<x>` (sidecar files) and
/// `.jma-<x>` (top-level folders that must be a single Maildir++
/// segment).
pub fn is_jma_private(name: &str) -> bool {
    name.starts_with(".jma.") || name.starts_with(".jma-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_known_sidecars() {
        for name in [
            STATE_DB_FILENAME,
            MAILDIR_LOCK_FILENAME,
            SENTINEL_FILENAME,
            RESCUE_FOLDER_NAME,
            ".jma.db-wal",
            ".jma.db-shm",
            ".jma.db.lock",
        ] {
            assert!(
                is_jma_private(name),
                "{name} should be in the private namespace"
            );
        }
    }

    #[test]
    fn rejects_mail_folders_and_dotfiles() {
        for name in [
            "INBOX",
            ".INBOX",
            "[Airmail].Sent",
            ".DS_Store",
            ".jmaybe",
            "jma.db",
        ] {
            assert!(
                !is_jma_private(name),
                "{name} must not be treated as private"
            );
        }
    }
}
