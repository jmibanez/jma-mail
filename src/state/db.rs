use anyhow::{Context, Result};
use rusqlite::Connection;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::info;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS jmap_state (
    account_id   TEXT NOT NULL,
    entity_type  TEXT NOT NULL,
    state        TEXT NOT NULL,
    updated_at   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (account_id, entity_type)
);

CREATE TABLE IF NOT EXISTS message_map (
    jmap_email_id   TEXT NOT NULL,
    jmap_blob_id    TEXT,
    jmap_thread_id  TEXT,
    mailbox_id      TEXT NOT NULL,
    maildir_id      TEXT,
    maildir_folder  TEXT,
    message_id      TEXT,
    flags           TEXT NOT NULL DEFAULT '',
    jmap_keywords   TEXT NOT NULL DEFAULT '{}',
    last_synced_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (jmap_email_id)
);

CREATE INDEX IF NOT EXISTS idx_message_map_maildir_id ON message_map(maildir_id);
CREATE INDEX IF NOT EXISTS idx_message_map_message_id ON message_map(message_id);
CREATE INDEX IF NOT EXISTS idx_message_map_mailbox_id ON message_map(mailbox_id);

CREATE TABLE IF NOT EXISTS mailbox_map (
    jmap_mailbox_id TEXT NOT NULL PRIMARY KEY,
    name            TEXT NOT NULL,
    role            TEXT,
    parent_id       TEXT,
    maildir_folder  TEXT NOT NULL,
    sort_order      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS local_state (
    maildir_id     TEXT NOT NULL PRIMARY KEY,
    maildir_folder TEXT NOT NULL,
    flags          TEXT NOT NULL DEFAULT '',
    mtime          INTEGER,
    recorded_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
"#;

/// Open (or create) the state database and run schema migrations.
pub fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("Failed to create state DB directory: {}", parent.display())
        })?;
    }

    let conn = Connection::open(path)
        .with_context(|| format!("Failed to open state DB at {}", path.display()))?;

    conn.execute_batch("PRAGMA journal_mode=WAL;")
        .context("Failed to set WAL journal mode")?;
    conn.execute_batch("PRAGMA foreign_keys=ON;")
        .context("Failed to enable foreign keys")?;

    conn.execute_batch(SCHEMA)
        .context("Failed to initialize database schema")?;

    info!("State database opened at {}", path.display());
    Ok(conn)
}

/// Open an in-memory connection with the schema applied. For unit
/// tests that need a real `rusqlite::Connection` without touching disk.
#[cfg(test)]
pub fn open_in_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory().context("Failed to open in-memory state DB")?;
    conn.execute_batch(SCHEMA)
        .context("Failed to initialize in-memory schema")?;
    Ok(conn)
}

/// Path of the advisory lock file paired with a given state DB
/// (`state.db` -> `state.db.lock`). Exposed for diagnostics and
/// documentation; not normally needed by callers.
pub fn lock_path_for(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push(".lock");
    PathBuf::from(p)
}

/// Acquire an exclusive advisory lock on `<db_path>.lock` so that
/// at most one mutating jmapsync command (sync/pull/push/watch)
/// touches a given state DB at a time. The lock is held for the
/// rest of the process's lifetime; the kernel releases it when the
/// fd closes at process exit, even on panic or SIGKILL — so a
/// stale lock file is never blocking on its own.
///
/// Stamps our PID into the file purely as a diagnostic, so a second
/// instance can name us in its error message. The PID is never
/// consulted to decide whether to steal the lock — flock semantics
/// make stealing unnecessary and PID recycling makes it unsafe.
///
/// Read-only commands (`status`, `mailboxes`) and commands that
/// don't touch the state DB (`init`, `auth`) do not call this.
pub fn acquire_lock(db_path: &Path) -> Result<()> {
    let lock_path = lock_path_for(db_path);
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create lock directory: {}", parent.display()))?;
    }

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
            info!("Acquired instance lock at {}", lock_path.display());
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
    fn lock_path_appends_lock_suffix() {
        let p = lock_path_for(Path::new("/var/lib/jmapsync/state.db"));
        assert_eq!(p, Path::new("/var/lib/jmapsync/state.db.lock"));
    }

    #[test]
    fn lock_path_handles_extensionless_db() {
        let p = lock_path_for(Path::new("/tmp/state"));
        assert_eq!(p, Path::new("/tmp/state.lock"));
    }

    #[test]
    fn acquire_lock_writes_pid_and_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        acquire_lock(&db).expect("first acquire should succeed");

        // PID file should now contain our pid.
        let pid = read_pid(&lock_path_for(&db)).expect("pid file readable");
        assert_eq!(pid, std::process::id());
    }

    #[test]
    fn second_acquire_fails_with_holder_pid_in_message() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        acquire_lock(&db).expect("first acquire should succeed");

        let err = acquire_lock(&db).expect_err("second acquire should fail");
        let msg = format!("{}", err);
        assert!(msg.contains("another jmapsync is running"), "got: {msg}");
        assert!(
            msg.contains(&format!("pid {}", std::process::id())),
            "expected our pid in message, got: {msg}"
        );
    }
}
