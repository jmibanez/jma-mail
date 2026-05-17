//! Per-folder `(cur/, new/)` snapshot used by the engine's Phase 0
//! dedupe gate. A cycle that finds the recorded checkpoint matches
//! a freshly-taken snapshot can safely skip the dedupe walk for
//! that folder: no file has been added, removed, or renamed across
//! the `cur/`/`new/` boundary since the last successful cycle, so
//! no new per-folder Message-ID duplicate can have formed.
//!
//! `cur/` and `new/` are tracked separately because they're
//! separate directories at the OS level -- `rename(2)` between
//! them bumps both dirs' mtimes and changes both counts, and we
//! want either side's churn to surface as "dirty". `tmp/` is
//! deliberately ignored: maildir delivery writes to `tmp/`,
//! fsyncs, and renames into `new/`; the visible duplicate-creating
//! event is the rename, which already bumps `new/`.
use anyhow::{Context, Result};
use std::path::Path;
use std::time::SystemTime;

use crate::state::queries::FolderCheckpoint;

/// Take a fresh `FolderCheckpoint` for the folder rooted at
/// `folder_path`. Expects `folder_path/cur/` and `folder_path/new/`
/// to exist; callers that might race against a not-yet-created
/// folder should call `store::ensure_maildir` first.
pub fn snapshot_folder(folder_path: &Path) -> Result<FolderCheckpoint> {
    let (cur_mtime_ns, cur_count) = stat_dir(&folder_path.join("cur"))?;
    let (new_mtime_ns, new_count) = stat_dir(&folder_path.join("new"))?;
    Ok(FolderCheckpoint {
        cur_mtime_ns,
        new_mtime_ns,
        cur_count,
        new_count,
    })
}

fn stat_dir(path: &Path) -> Result<(i64, i64)> {
    let meta =
        std::fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    // Nanoseconds since UNIX_EPOCH packed into i64. APFS and modern
    // Linux filesystems carry nanosecond mtime resolution, which is
    // what makes count-stable rename-only churn (in-place flag
    // flips) distinguishable from real adds when we compare two
    // snapshots taken within the same wallclock second. A modified
    // time before 1970 is treated as 0; one past ~2262 saturates to
    // i64::MAX. Neither case is reachable on a real maildir.
    let mtime_ns: i64 = meta
        .modified()
        .with_context(|| format!("filesystem does not expose mtime for {}", path.display()))?
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
        .try_into()
        .unwrap_or(i64::MAX);
    let count = std::fs::read_dir(path)
        .with_context(|| format!("failed to readdir {}", path.display()))?
        .count() as i64;
    Ok((mtime_ns, count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile::tempdir;

    fn make_folder(root: &Path, name: &str) -> std::path::PathBuf {
        let folder = root.join(name);
        fs::create_dir_all(folder.join("cur")).unwrap();
        fs::create_dir_all(folder.join("new")).unwrap();
        fs::create_dir_all(folder.join("tmp")).unwrap();
        folder
    }

    #[test]
    fn snapshot_of_empty_folder_has_zero_counts() {
        let dir = tempdir().unwrap();
        let folder = make_folder(dir.path(), "INBOX");
        let cp = snapshot_folder(&folder).unwrap();
        assert_eq!(cp.cur_count, 0);
        assert_eq!(cp.new_count, 0);
        assert!(cp.cur_mtime_ns > 0);
        assert!(cp.new_mtime_ns > 0);
    }

    #[test]
    fn adding_a_file_to_new_bumps_count_and_mtime() {
        let dir = tempdir().unwrap();
        let folder = make_folder(dir.path(), "INBOX");
        let before = snapshot_folder(&folder).unwrap();

        // Sleep past the OS mtime resolution so the bump is observable.
        sleep(Duration::from_millis(20));
        fs::write(folder.join("new").join("1234.host:2,"), b"body").unwrap();

        let after = snapshot_folder(&folder).unwrap();
        assert_eq!(after.new_count, before.new_count + 1);
        assert!(after.new_mtime_ns > before.new_mtime_ns);
        // cur/ untouched: count and mtime stable.
        assert_eq!(after.cur_count, before.cur_count);
        assert_eq!(after.cur_mtime_ns, before.cur_mtime_ns);
    }

    #[test]
    fn in_place_rename_bumps_mtime_but_not_count() {
        let dir = tempdir().unwrap();
        let folder = make_folder(dir.path(), "INBOX");
        let cur = folder.join("cur");
        fs::write(cur.join("1234.host:2,"), b"body").unwrap();

        let before = snapshot_folder(&folder).unwrap();

        sleep(Duration::from_millis(20));
        // Mimic an MUA flag flip: rename adds the S flag to the
        // info section. Same directory, same inode count.
        fs::rename(cur.join("1234.host:2,"), cur.join("1234.host:2,S")).unwrap();

        let after = snapshot_folder(&folder).unwrap();
        assert_eq!(after.cur_count, before.cur_count);
        assert!(after.cur_mtime_ns > before.cur_mtime_ns);
    }

    #[test]
    fn missing_subdir_errors_cleanly() {
        let dir = tempdir().unwrap();
        let folder = dir.path().join("INBOX");
        // Only create cur/; new/ is missing.
        fs::create_dir_all(folder.join("cur")).unwrap();
        let err = snapshot_folder(&folder).expect_err("missing new/ should fail");
        assert!(format!("{err:#}").contains("new"));
    }
}
