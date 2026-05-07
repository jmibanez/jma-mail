use anyhow::{Context, Result};
use rusqlite::Connection;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// On-disk schema version. Bump whenever the SQLite schema or the
/// invariants the code expects of existing rows change in a way the
/// previous binary's writes would violate. There is no in-place
/// migration: a version mismatch under a mutating command nukes
/// `state.db` and re-syncs (the maildir + JMAP server are the source
/// of truth, and Message-ID-anchored adoption rebinds existing local
/// files without re-downloading). Read-only commands refuse instead
/// of nuking, since they don't hold the state DB lock.
pub const SCHEMA_VERSION: u32 = 1;

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
    message_id      TEXT NOT NULL,
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

CREATE TABLE IF NOT EXISTS jmap_discovery (
    domain         TEXT NOT NULL PRIMARY KEY,
    session_url    TEXT NOT NULL,
    discovered_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
"#;

/// Open the state database for read-only callers (`status`, `mailboxes`,
/// `auth rediscover`). If the on-disk schema version doesn't match
/// `SCHEMA_VERSION`, refuse with a clear instruction to run a mutating
/// command -- those hold the state DB lock and can safely nuke + recreate.
/// Read-only paths can't, since nuking under a concurrently running
/// `sync`/`watch` would yank the DB out from under it.
pub fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("Failed to create state DB directory: {}", parent.display())
        })?;
    }

    if let Some(stale) = stale_schema_version(path)? {
        anyhow::bail!(
            "State DB at {} is schema version {}, but this jma expects {}. \
             Run `jma sync` (or pull/push/watch) to recreate the DB \
             automatically -- existing local files are rebound by Message-ID, \
             so nothing re-downloads.",
            path.display(),
            stale,
            SCHEMA_VERSION,
        );
    }

    open_and_apply_schema(path)
}

/// Open the state database for mutating callers (`sync`, `pull`, `push`,
/// `watch`). On schema-version mismatch, nuke `state.db` and its
/// WAL/SHM siblings and recreate empty. The disposability invariant
/// (state.db rebuildable from maildir + server) is what makes this
/// safe; the dedupe pass and Message-ID-anchored adoption rebind
/// existing local files without re-downloading.
///
/// Caller must hold the state DB lock from `acquire_lock` before
/// calling this. Otherwise a concurrent jma sharing this state DB
/// (e.g. a misconfigured second config pointing at the same `db_path`,
/// or -- once the multi-account refactor lands -- a sibling per-account
/// driver against the shared DB) could be midway through a cycle when
/// we unlink the file out from under it.
///
/// In practice every mutating caller also holds the maildir lock; see
/// `acquire_mutator_locks` in `src/main.rs` for the canonical order.
pub fn open_or_recreate(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("Failed to create state DB directory: {}", parent.display())
        })?;
    }

    if let Some(stale) = stale_schema_version(path)? {
        warn!(
            "State DB at {} is schema version {}, binary expects {}. \
             Nuking and re-syncing; Message-ID-anchored adoption will rebind \
             existing local files without re-downloading.",
            path.display(),
            stale,
            SCHEMA_VERSION,
        );
        unlink_state_db(path)?;
    }

    open_and_apply_schema(path)
}

/// Inspect the on-disk DB at `path` and decide whether it's compatible
/// with the running binary.
///
/// Returns `Ok(None)` if the file doesn't exist (we never touch it; the
/// caller's `open_and_apply_schema` will create it fresh) or if its
/// `user_version` already matches `SCHEMA_VERSION`. Returns
/// `Ok(Some(version))` when the on-disk DB is stale: either an older
/// versioned schema, or a pre-versioning DB whose `user_version` is
/// still 0 but whose tables are populated. The "fresh empty file with
/// v=0" edge case (rare; user `touch`ed the path) is treated as "needs
/// schema applied" rather than stale, since nuking an empty file is
/// wasted I/O.
fn stale_schema_version(path: &Path) -> Result<Option<u32>> {
    if !path.exists() {
        return Ok(None);
    }
    let conn = Connection::open(path)
        .with_context(|| format!("Failed to probe state DB at {}", path.display()))?;
    let v: u32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .context("Failed to read PRAGMA user_version")?;
    if v == SCHEMA_VERSION {
        return Ok(None);
    }
    if v == 0 && !has_message_map_table(&conn)? {
        // Bare/empty file with no schema yet. Open + schema apply will
        // handle it; no nuke needed.
        return Ok(None);
    }
    Ok(Some(v))
}

fn has_message_map_table(conn: &Connection) -> Result<bool> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'message_map'",
            [],
            |row| row.get(0),
        )
        .context("Failed to inspect sqlite_master for message_map")?;
    Ok(count > 0)
}

fn open_and_apply_schema(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("Failed to open state DB at {}", path.display()))?;

    conn.execute_batch("PRAGMA journal_mode=WAL;")
        .context("Failed to set WAL journal mode")?;
    conn.execute_batch("PRAGMA foreign_keys=ON;")
        .context("Failed to enable foreign keys")?;

    conn.execute_batch(SCHEMA)
        .context("Failed to initialize database schema")?;

    // Stamp the version unconditionally: covers brand-new DBs (where
    // user_version starts at 0) and is a harmless no-op when it's
    // already current. `pragma_update` doesn't accept parameter
    // binding for PRAGMA values, so format the constant in.
    conn.execute_batch(&format!("PRAGMA user_version = {};", SCHEMA_VERSION))
        .context("Failed to stamp schema version")?;

    info!("State database opened at {}", path.display());
    Ok(conn)
}

/// Remove `state.db` and its SQLite auxiliary files (`-wal`, `-shm`).
/// Missing files are not an error — recovery is the goal, not strict
/// cleanup.
///
/// Order matters: delete `-shm` and `-wal` first, the main DB file
/// last. If we error out partway through (e.g. permission flap), a
/// surviving main file alongside missing siblings is fine — SQLite
/// will recreate the WAL/SHM on next open. The opposite order could
/// leave WAL/SHM orphaned without a main file, which is messier.
fn unlink_state_db(path: &Path) -> Result<()> {
    for suffix in ["-shm", "-wal", ""] {
        let mut p = path.as_os_str().to_owned();
        p.push(suffix);
        let target = PathBuf::from(p);
        match std::fs::remove_file(&target) {
            Ok(()) => debug!("Removed {}", target.display()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).with_context(|| format!("Failed to remove {}", target.display()));
            }
        }
    }
    Ok(())
}

/// Open an in-memory connection with the schema applied. For unit
/// tests that need a real `rusqlite::Connection` without touching disk.
#[cfg(test)]
pub fn open_in_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory().context("Failed to open in-memory state DB")?;
    conn.execute_batch(SCHEMA)
        .context("Failed to initialize in-memory schema")?;
    conn.execute_batch(&format!("PRAGMA user_version = {};", SCHEMA_VERSION))
        .context("Failed to stamp in-memory schema version")?;
    Ok(conn)
}

/// Path of the advisory lock file paired with a given state DB
/// (`state.db` -> `state.db.lock`). Exposed for diagnostics; not
/// normally needed by callers.
pub fn lock_path_for(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push(".lock");
    PathBuf::from(p)
}

/// Acquire an exclusive advisory lock on `<db_path>.lock`. This lock
/// specifically gates `open_or_recreate`'s schema-mismatch unlink path:
/// without it, a process that decides the on-disk schema is stale would
/// `unlink` the DB out from under any concurrent process that opened
/// the same DB but doesn't realise it's about to be deleted (writes
/// vanish into the orphaned inode; a later checkpoint can corrupt).
///
/// The lock is keyed on the state DB rather than the maildir because
/// the destruction is a property of the DB file, not the maildir. The
/// maildir lock (`maildir_ops::lock::acquire_lock`) is a separate
/// concern: it serializes maildir mutations across processes that may
/// not even share a DB. Both locks are held for process lifetime.
///
/// Lock-acquisition order is **maildir lock first, then state DB
/// lock**, consistently across every call site, to avoid deadlock.
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
/// Read-only commands (`status`, `mailboxes`) and DB-less commands
/// (`init`, `auth`) do not call this.
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
            info!("Acquired state DB lock at {}", lock_path.display());
            Ok(())
        }
        Err(_) => {
            let holder = read_pid(&lock_path)
                .map(|p| format!("pid {}", p))
                .unwrap_or_else(|| "unknown pid".to_string());
            Err(anyhow::anyhow!(
                "another jma is using this state DB ({} at {})",
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
        let p = lock_path_for(Path::new("/var/lib/jma/state.db"));
        assert_eq!(p, Path::new("/var/lib/jma/state.db.lock"));
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
        assert!(
            msg.contains("another jma is using this state DB"),
            "got: {msg}"
        );
        assert!(
            msg.contains(&format!("pid {}", std::process::id())),
            "expected our pid in message, got: {msg}"
        );
    }

    fn user_version(db_path: &Path) -> u32 {
        let conn = Connection::open(db_path).unwrap();
        conn.pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap()
    }

    fn force_user_version(db_path: &Path, v: u32) {
        let conn = Connection::open(db_path).unwrap();
        conn.execute_batch(&format!("PRAGMA user_version = {};", v))
            .unwrap();
    }

    #[test]
    fn open_or_recreate_stamps_version_on_fresh_db() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");

        let _ = open_or_recreate(&db).expect("fresh open should succeed");

        assert_eq!(user_version(&db), SCHEMA_VERSION);
    }

    #[test]
    fn open_or_recreate_nukes_when_version_is_older() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");

        // Seed a current-schema DB with a row, then forge an older
        // user_version to simulate an upgrade.
        {
            let conn = open_or_recreate(&db).unwrap();
            conn.execute(
                "INSERT INTO jmap_state (account_id, entity_type, state) VALUES ('a', 'Email', 'cookie')",
                [],
            )
            .unwrap();
        }
        force_user_version(&db, 0);

        let conn = open_or_recreate(&db).expect("recreate should succeed");

        assert_eq!(user_version(&db), SCHEMA_VERSION);
        // Row from the prior DB must be gone -- we nuked, not migrated.
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jmap_state", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0, "expected nuked DB to be empty");
    }

    #[test]
    fn open_or_recreate_nukes_when_version_is_newer() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");

        // Seed a current-schema DB with a row, then forge a newer
        // user_version to simulate a downgrade.
        {
            let conn = open_or_recreate(&db).unwrap();
            conn.execute(
                "INSERT INTO jmap_state (account_id, entity_type, state) VALUES ('a', 'Email', 'cookie')",
                [],
            )
            .unwrap();
        }
        force_user_version(&db, SCHEMA_VERSION + 99);

        let conn = open_or_recreate(&db).expect("recreate should succeed");

        assert_eq!(user_version(&db), SCHEMA_VERSION);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jmap_state", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0, "expected nuked DB to be empty");
    }

    #[test]
    fn open_or_recreate_treats_pre_versioning_db_with_data_as_stale() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");

        // Simulate a pre-versioning legacy DB: schema applied, rows
        // present, user_version still 0.
        {
            let conn = open_or_recreate(&db).unwrap();
            conn.execute(
                "INSERT INTO jmap_state (account_id, entity_type, state) VALUES ('a', 'Email', 'cookie')",
                [],
            )
            .unwrap();
        }
        force_user_version(&db, 0);

        let conn = open_or_recreate(&db).expect("recreate should succeed");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jmap_state", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0, "pre-versioning DB with data should be nuked");
        assert_eq!(user_version(&db), SCHEMA_VERSION);
    }

    #[test]
    fn open_refuses_stale_version_with_actionable_message() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");

        {
            let _ = open_or_recreate(&db).unwrap();
        }
        // Pre-versioning DB needs populated tables to count as stale,
        // so seed one row before forging the version back to 0.
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute(
                "INSERT INTO jmap_state (account_id, entity_type, state) VALUES ('a', 'Email', 'cookie')",
                [],
            )
            .unwrap();
        }
        force_user_version(&db, 0);

        let err = open(&db).expect_err("open should refuse a stale DB");
        let msg = format!("{}", err);
        assert!(
            msg.contains("schema version 0"),
            "expected version in message, got: {msg}"
        );
        assert!(
            msg.contains("jma sync"),
            "expected actionable command in message, got: {msg}"
        );
    }

    #[test]
    fn open_accepts_current_version() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let _ = open_or_recreate(&db).unwrap();

        let _ = open(&db).expect("current-version DB should open read-only");
    }

    #[test]
    fn open_creates_fresh_db_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");

        let _ = open(&db).expect("missing DB should be created");

        assert_eq!(user_version(&db), SCHEMA_VERSION);
    }
}
