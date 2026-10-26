//! Non-creating Maildir inventory with explicit directory-open errors.
//!
//! The pinned Maildir iterator silently treats every initial read_dir error
//! as an empty side. Absence of a peer is valid, but PermissionDenied/EIO or
//! a non-directory must never authorize deletion or an unsafe snapshot rename.

use anyhow::{Context, Result, bail, ensure};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::ids::MaildirId;

#[derive(Debug)]
pub(crate) struct Entry {
    pub maildir_id: MaildirId,
    pub flags: String,
    pub path: PathBuf,
    pub in_new: bool,
}

/// Open both present sides before exposing entries. Missing sides are empty;
/// every other open/entry error propagates. No mkdir or body reads. Dot files
/// remain ignored, and new/ may carry either bare IDs or a :2, suffix.
pub(crate) fn read(root: &Path) -> Result<impl Iterator<Item = Result<Entry>>> {
    let mut readers = Vec::with_capacity(2);
    for (subdir, in_new) in [("cur", false), ("new", true)] {
        let directory = root.join(subdir);
        match std::fs::read_dir(&directory) {
            Ok(reader) => readers.push((reader, in_new, directory)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("Cannot inventory {}", directory.display()));
            }
        }
    }
    Ok(readers.into_iter().flat_map(|(reader, in_new, directory)| {
        reader.filter_map(move |entry| {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    return Some(
                        Err(error)
                            .with_context(|| format!("Cannot enumerate {}", directory.display())),
                    );
                }
            };
            let filename = entry.file_name();
            if filename.to_string_lossy().starts_with('.') {
                return None;
            }
            Some(parse(entry.path(), filename, in_new))
        })
    }))
}

fn parse(path: PathBuf, filename: OsString, in_new: bool) -> Result<Entry> {
    let name = filename
        .into_string()
        .map_err(|_| anyhow::anyhow!("Non-UTF-8 Maildir filename at {path:?}"))?;
    let (id, flags) = match name.split_once(":2,") {
        Some(parts) => parts,
        None if in_new => (name.as_str(), ""),
        None => bail!("Non-Maildir filename in cur/: {path:?}"),
    };
    ensure!(!id.is_empty(), "Empty Maildir identity at {path:?}");
    Ok(Entry {
        maildir_id: id.into(),
        flags: flags.into(),
        path,
        in_new,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_inventory_reads_live_peer_without_creating_the_other() {
        for (present, missing) in [("cur", "new"), ("new", "cur")] {
            let temp = tempfile::tempdir().unwrap();
            let side = temp.path().join(present);
            std::fs::create_dir(&side).unwrap();
            std::fs::write(side.join("id:2,FX"), b"body").unwrap();
            std::fs::write(side.join(".ignored"), b"sidecar").unwrap();
            let entries = read(temp.path())
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].maildir_id.as_ref(), "id");
            assert_eq!(entries[0].flags, "FX");
            assert_eq!(entries[0].in_new, present == "new");
            assert!(!temp.path().join(missing).exists());
        }
    }

    #[test]
    fn inventory_preserves_bare_new_and_surfaces_invalid_cur_entries() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp.path().join("new")).unwrap();
        std::fs::write(temp.path().join("new/bare-id"), b"body").unwrap();
        let entry = read(temp.path()).unwrap().next().unwrap().unwrap();
        assert_eq!(entry.maildir_id.as_ref(), "bare-id");
        assert_eq!(entry.flags, "");
        std::fs::create_dir(temp.path().join("cur")).unwrap();
        std::fs::write(temp.path().join("cur/not-maildir"), b"body").unwrap();
        assert!(
            read(temp.path())
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .is_err()
        );
    }
}
