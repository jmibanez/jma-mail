use anyhow::{Context, Result};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::info;

/// Path of the advisory lock file paired with a given maildir root
/// (`<root>/.jmapsync.lock`). Exposed for diagnostics; not normally
/// needed by callers.
pub fn lock_path_for(maildir_root: &Path) -> PathBuf {
    maildir_root.join(".jmapsync.lock")
}

/// Acquire an exclusive advisory lock on `<maildir_root>/.jmapsync.lock`
/// so that at most one mutating jmapsync command (sync/pull/push/watch)
/// touches a given maildir at a time. The lock is keyed on the maildir
/// because the maildir is the cross-process shared mutation surface:
/// two configs pointing at different state DBs but the same maildir
/// would otherwise both write the same Message-ID under different
/// filenames and clobber each other.
///
/// The lock is held for the rest of the process's lifetime; the kernel
/// releases it when the fd closes at process exit, even on panic or
/// SIGKILL -- so a stale lock file is never blocking on its own.
///
/// Stamps our PID into the file purely as a diagnostic, so a second
/// instance can name us in its error message. The PID is never
/// consulted to decide whether to steal the lock -- flock semantics
/// make stealing unnecessary and PID recycling makes it unsafe.
///
/// Read-only commands (`status`, `mailboxes`) and commands that don't
/// touch the maildir (`init`, `auth`) do not call this.
pub fn acquire_lock(maildir_root: &Path) -> Result<()> {
    if !maildir_root.exists() {
        anyhow::bail!(
            "Maildir root does not exist: {}. Run `jmapsync init` first, or fix the \
             [sync].maildir_path in your config.",
            maildir_root.display()
        );
    }

    let lock_path = lock_path_for(maildir_root);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("Failed to open lock file {}", lock_path.display()))?;

    // Box::leak: the lock is held until process exit and the kernel
    // releases it when the fd closes. One small allocation per
    // process; avoids a self-referential guard struct.
    let lock = Box::leak(Box::new(fd_lock::RwLock::new(file)));

    match lock.try_write() {
        Ok(mut guard) => {
            let _ = guard.set_len(0);
            let _ = guard.seek(SeekFrom::Start(0));
            let _ = writeln!(&mut *guard, "{}", std::process::id());
            let _ = guard.flush();
            // Forget the guard so the lock outlives this scope.
            // The leaked RwLock<File> still owns the fd.
            std::mem::forget(guard);
            info!("Acquired maildir lock at {}", lock_path.display());
            Ok(())
        }
        Err(_) => {
            let holder = read_pid(&lock_path)
                .map(|p| format!("pid {}", p))
                .unwrap_or_else(|| "unknown pid".to_string());
            Err(anyhow::anyhow!(
                "another jmapsync is running ({} at {})",
                holder,
                lock_path.display()
            ))
        }
    }
}

fn read_pid(path: &Path) -> Option<u32> {
    let mut f = std::fs::File::open(path).ok()?;
    let mut s = String::new();
    f.read_to_string(&mut s).ok()?;
    s.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_path_is_dotfile_at_maildir_root() {
        let p = lock_path_for(Path::new("/home/user/Mail/foo"));
        assert_eq!(p, Path::new("/home/user/Mail/foo/.jmapsync.lock"));
    }

    #[test]
    fn acquire_lock_writes_pid_and_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        acquire_lock(dir.path()).expect("first acquire should succeed");

        let pid = read_pid(&lock_path_for(dir.path())).expect("pid file readable");
        assert_eq!(pid, std::process::id());
    }

    #[test]
    fn second_acquire_fails_with_holder_pid_in_message() {
        let dir = tempfile::tempdir().unwrap();
        acquire_lock(dir.path()).expect("first acquire should succeed");

        let err = acquire_lock(dir.path()).expect_err("second acquire should fail");
        let msg = format!("{}", err);
        assert!(msg.contains("another jmapsync is running"), "got: {msg}");
        assert!(
            msg.contains(&format!("pid {}", std::process::id())),
            "expected our pid in message, got: {msg}"
        );
    }

    #[test]
    fn acquire_lock_refuses_when_maildir_root_missing() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("does_not_exist");
        let err = acquire_lock(&root).expect_err("acquire should refuse a missing root");
        let msg = format!("{}", err);
        assert!(
            msg.contains("does not exist"),
            "expected actionable error, got: {msg}"
        );
        assert!(
            !lock_path_for(&root).exists(),
            "lock file must not be created"
        );
    }
}
