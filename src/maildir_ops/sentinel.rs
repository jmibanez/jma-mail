//! Per-folder identity sentinel: `<maildir>/.jma.mapping`.
//!
//! Each synced maildir folder carries a small TOML file that pins
//! its bound JMAP mailbox id, the bound parent mailbox id, and the
//! server's leaf-segment name. The sentinel exists so that the
//! local-folder -> JMAP-mailbox binding survives a state DB nuke
//! (the DB is disposable; the maildir + server are the source of
//! truth) and so that a locally-renamed directory still
//! self-identifies as the mailbox it was bound to.
//!
//! **Self-healing metadata, not durable state.** The sentinel is
//! purely derivative -- its information is recoverable from
//! `Mailbox/get` on the server plus the cached `mailbox_map`. A
//! missing, malformed, or partially-written sentinel is therefore
//! never an error; `read` treats every "I can't trust this file"
//! shape as `Ok(None)` and lets the next write rebuild it cleanly.
//! Callers that observe `None` fall back to the rebind path, which
//! is the same path that handles a freshly-created folder.
//!
//! This is why `write` uses a plain `std::fs::write` rather than
//! the tmp+fsync+rename dance the upstream `maildir` crate uses
//! for message delivery: mail bytes can't be reconstituted from
//! anywhere else, but the sentinel can. Defending against a torn
//! write here would only protect against a failure mode whose
//! remediation is already designed in.
//!
//! Unknown TOML keys are tolerated on read so that a newer jma
//! writing additional fields does not break an older jma reading
//! the same file.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::warn;

use crate::ids::JmapMailboxId;

/// Path of the sentinel file paired with a given per-folder maildir
/// directory (`<folder>/.jma.mapping`). Exposed for diagnostics
/// and for callers that need the path without performing I/O.
pub fn sentinel_path_for(folder_path: &Path) -> PathBuf {
    folder_path.join(crate::maildir_ops::namespace::SENTINEL_FILENAME)
}

/// Contents of a `.jma.mapping` sentinel.
///
/// `parent_jmap_mailbox_id` is absent for top-level mailboxes (no
/// JMAP parent). `server_name` is the leaf-segment name as the
/// server knows it (e.g. `"Sent"`), not the layout-flattened
/// on-disk folder name (e.g. `"[Airmail].Sent"`); the on-disk
/// shape is recoverable from the folder path itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxMapping {
    pub jmap_mailbox_id: JmapMailboxId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_jmap_mailbox_id: Option<JmapMailboxId>,
    pub server_name: String,
}

/// Read the sentinel inside `folder_path`. Returns `Ok(None)` for
/// every "I can't trust this file" outcome -- missing file,
/// non-UTF-8 bytes, TOML parse failure, or sanity-check failure on
/// the deserialized fields (empty `jmap_mailbox_id` or empty
/// `server_name`, both of which only happen via partial writes or
/// hand-edits). The corrupted-but-present cases log `warn!` so the
/// occurrence is visible without being user-actionable; the
/// missing case is silent because a freshly-created folder hits it
/// every cycle.
///
/// `Err` is reserved for I/O failures that prevent us from making
/// any determination at all (permission denied, EIO, etc.) -- the
/// caller can surface those rather than papering over them as
/// "rebind needed."
///
/// The sanity-check on `jmap_mailbox_id` and `server_name` defends
/// against a partial-write whose prefix happens to parse as valid
/// TOML. Both fields must be non-empty in any sentinel jma itself
/// produces; a deserialized empty or whitespace-only value is
/// therefore a torn-write signal even when the TOML grammar
/// accepts it. The trim catches the `"\n"`-tail and `"  "` cases
/// without rejecting legitimate names that happen to carry interior
/// whitespace.
pub fn read(folder_path: &Path) -> Result<Option<MailboxMapping>> {
    let path = sentinel_path_for(folder_path);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("Failed to read sentinel {}", path.display()));
        }
    };
    let text = match std::str::from_utf8(&bytes) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                "Sentinel {} is not valid UTF-8 ({}); treating as missing so next sync rebinds",
                path.display(),
                e
            );
            return Ok(None);
        }
    };
    let mapping: MailboxMapping = match toml::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            warn!(
                "Failed to parse sentinel TOML at {} ({}); treating as missing so next sync rebinds",
                path.display(),
                e
            );
            return Ok(None);
        }
    };
    if mapping.jmap_mailbox_id.as_ref().trim().is_empty() || mapping.server_name.trim().is_empty() {
        warn!(
            "Sentinel {} has empty or whitespace-only required field (likely a partial write); treating as missing so next sync rebinds",
            path.display()
        );
        return Ok(None);
    }
    Ok(Some(mapping))
}

/// Write `mapping` to `<folder_path>/.jma.mapping`. Plain
/// `std::fs::write` -- no tmp+rename, no fsync. A crash mid-write
/// can leave a torn payload, but `read` treats every such outcome
/// as `Ok(None)` and the next sync cycle rebuilds the sentinel from
/// server state, so the torn case is self-healing rather than a
/// failure mode that needs structural defense. See the module doc
/// for the threat-model reasoning.
///
/// `folder_path` must already exist as a directory (the caller is
/// expected to have run `ensure_maildir` first).
pub fn write(folder_path: &Path, mapping: &MailboxMapping) -> Result<()> {
    let dest = sentinel_path_for(folder_path);
    let body = toml::to_string(mapping)
        .with_context(|| format!("Failed to serialize sentinel for {}", dest.display()))?;
    std::fs::write(&dest, body.as_bytes())
        .with_context(|| format!("Failed to write sentinel {}", dest.display()))?;
    Ok(())
}

/// Remove the sentinel inside `folder_path`. Missing file is not
/// an error -- this is the desired post-condition.
pub fn remove(folder_path: &Path) -> Result<()> {
    let path = sentinel_path_for(folder_path);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("Failed to remove sentinel {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_mapping() -> MailboxMapping {
        MailboxMapping {
            jmap_mailbox_id: JmapMailboxId::from("M123"),
            parent_jmap_mailbox_id: Some(JmapMailboxId::from("M99")),
            server_name: "Sent".to_string(),
        }
    }

    #[test]
    fn round_trip_with_parent() {
        let dir = tempfile::tempdir().unwrap();
        let mapping = mk_mapping();
        write(dir.path(), &mapping).expect("write should succeed");
        let read_back = read(dir.path()).expect("read should succeed");
        assert_eq!(read_back, Some(mapping));
    }

    #[test]
    fn round_trip_top_level_omits_parent() {
        let dir = tempfile::tempdir().unwrap();
        let mapping = MailboxMapping {
            jmap_mailbox_id: JmapMailboxId::from("Mroot"),
            parent_jmap_mailbox_id: None,
            server_name: "INBOX".to_string(),
        };
        write(dir.path(), &mapping).expect("write should succeed");

        // Pin that the on-disk TOML for a top-level mailbox does
        // not emit a `parent_jmap_mailbox_id` key at all -- a
        // present-with-empty-string value would deserialize back
        // as `Some("")`, which is a different mapping than the
        // intended `None`.
        let body =
            std::fs::read_to_string(sentinel_path_for(dir.path())).expect("file should exist");
        assert!(
            !body.contains("parent_jmap_mailbox_id"),
            "expected no parent key for top-level mapping, got: {body}"
        );

        let read_back = read(dir.path()).expect("read should succeed");
        assert_eq!(read_back, Some(mapping));
    }

    #[test]
    fn read_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let got = read(dir.path()).expect("missing sentinel should not error");
        assert!(
            got.is_none(),
            "expected None for missing file, got: {got:?}"
        );
    }

    /// Self-healing read contract: malformed TOML returns Ok(None)
    /// so the caller falls into the rebind path. The alternative
    /// (returning Err) would require manual intervention to recover
    /// from a torn write, which contradicts the "sentinel is
    /// rebuildable from server state" design.
    #[test]
    fn read_malformed_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            sentinel_path_for(dir.path()),
            b"not = valid = toml = at = all",
        )
        .expect("write should succeed");
        let got = read(dir.path()).expect("malformed TOML should be self-healing");
        assert!(
            got.is_none(),
            "expected None for malformed TOML, got: {got:?}"
        );
    }

    /// Same self-healing contract for non-UTF-8 bytes. A torn
    /// write that lands in the middle of a multi-byte sequence
    /// is the realistic shape here.
    #[test]
    fn read_invalid_utf8_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        // 0xFF is never the first byte of a valid UTF-8 sequence.
        std::fs::write(sentinel_path_for(dir.path()), [0xFFu8, 0xFEu8, 0xFDu8])
            .expect("write should succeed");
        let got = read(dir.path()).expect("non-UTF-8 should be self-healing");
        assert!(
            got.is_none(),
            "expected None for non-UTF-8 bytes, got: {got:?}"
        );
    }

    /// A partial-write whose prefix happens to parse as valid TOML
    /// but leaves `jmap_mailbox_id` empty must be caught by the
    /// field-level sanity check, not silently trusted as a valid
    /// mapping. Empty `jmap_mailbox_id` is impossible in any
    /// sentinel jma itself produces, so it's a torn-write signal.
    #[test]
    fn read_empty_jmap_id_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let body = r#"
jmap_mailbox_id = ""
server_name = "Sent"
"#;
        std::fs::write(sentinel_path_for(dir.path()), body).expect("write");
        let got = read(dir.path()).expect("empty id should be self-healing");
        assert!(
            got.is_none(),
            "expected None for empty jmap id, got: {got:?}"
        );
    }

    /// Whitespace-only required fields are also torn-write
    /// signals: a tail of `"\n"` or `"   "` left after a crash
    /// would parse as TOML but isn't a value any writer would
    /// ever produce. Tightens the empty-string case to "trims to
    /// empty," which closes the class rather than the single
    /// instance.
    #[test]
    fn read_whitespace_only_fields_return_none() {
        let dir = tempfile::tempdir().unwrap();
        let body = r#"
jmap_mailbox_id = "   "
server_name = "Sent"
"#;
        std::fs::write(sentinel_path_for(dir.path()), body).expect("write");
        let got = read(dir.path()).expect("whitespace id should be self-healing");
        assert!(
            got.is_none(),
            "expected None for whitespace id, got: {got:?}"
        );

        let body = r#"
jmap_mailbox_id = "M123"
server_name = "\n"
"#;
        std::fs::write(sentinel_path_for(dir.path()), body).expect("write");
        let got = read(dir.path()).expect("whitespace name should be self-healing");
        assert!(
            got.is_none(),
            "expected None for whitespace server_name, got: {got:?}"
        );
    }

    /// Same sanity-check applies to `server_name`: empty is never
    /// a real server-supplied name, so an empty value indicates a
    /// torn write and must trigger rebind.
    #[test]
    fn read_empty_server_name_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let body = r#"
jmap_mailbox_id = "M123"
server_name = ""
"#;
        std::fs::write(sentinel_path_for(dir.path()), body).expect("write");
        let got = read(dir.path()).expect("empty name should be self-healing");
        assert!(
            got.is_none(),
            "expected None for empty server_name, got: {got:?}"
        );
    }

    /// Forward-compat: a newer jma writing additional fields must
    /// not break an older jma reading the same file. Pin the
    /// unknown-key tolerance so a future schema bump that needs
    /// `deny_unknown_fields` is a conscious choice rather than an
    /// accidental tightening.
    #[test]
    fn unknown_keys_are_tolerated_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let body = r#"
jmap_mailbox_id = "M123"
server_name = "Sent"
future_field_we_dont_understand = "ignored"
another_unknown = 42
"#;
        std::fs::write(sentinel_path_for(dir.path()), body).expect("write should succeed");
        let got = read(dir.path()).expect("unknown keys should be ignored");
        assert_eq!(
            got,
            Some(MailboxMapping {
                jmap_mailbox_id: JmapMailboxId::from("M123"),
                parent_jmap_mailbox_id: None,
                server_name: "Sent".to_string(),
            })
        );
    }

    #[test]
    fn remove_missing_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        remove(dir.path()).expect("removing a missing sentinel should be ok");
    }

    #[test]
    fn remove_existing_deletes_file() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), &mk_mapping()).expect("write");
        assert!(sentinel_path_for(dir.path()).exists());
        remove(dir.path()).expect("remove");
        assert!(!sentinel_path_for(dir.path()).exists());
    }

    #[test]
    fn sentinel_path_uses_dotted_jma_namespace() {
        let p = sentinel_path_for(Path::new("/tmp/maildir/INBOX"));
        assert_eq!(p, Path::new("/tmp/maildir/INBOX/.jma.mapping"));
    }
}
