//! jma's private namespace within the user's maildir root.
//!
//! jma persists a handful of sidecar files and directories alongside
//! the user's mail folders -- the state DB (`.jma.db` and its WAL/SHM
//! siblings), the cross-process lock (`.jma.lock`), and so on. They
//! share the `.jma.` prefix so any walker that scans the maildir can
//! tell jma's own files apart from synced mail without an exhaustive
//! enumeration of every sidecar name.
//!
//! New sidecar names that honour the prefix are recognised by every
//! caller of `is_jma_private` without further plumbing.

/// Default filename for the state DB at the maildir root
/// (`<maildir_path>/.jma.db`). Used when `[state].db_path` is unset.
pub const STATE_DB_FILENAME: &str = ".jma.db";

/// Filename for the cross-process advisory lock on a maildir
/// (`<maildir_root>/.jma.lock`).
pub const MAILDIR_LOCK_FILENAME: &str = ".jma.lock";

/// Whether a single path-component name belongs to jma's private
/// namespace at the maildir root. Operates on the file or directory
/// name (`OsStr::to_string_lossy()` output is fine), not a full
/// path.
pub fn is_jma_private(name: &str) -> bool {
    name.starts_with(".jma.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_known_sidecars() {
        for name in [
            STATE_DB_FILENAME,
            MAILDIR_LOCK_FILENAME,
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
