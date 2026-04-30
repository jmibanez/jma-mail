use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
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
