use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::HashMap;

use crate::ids::{JmapBlobId, JmapEmailId, JmapMailboxId, JmapThreadId, MaildirId, MessageId};

// --- JMAP State ---

/// Get the JMAP state string for an entity type.
pub fn get_jmap_state(
    conn: &Connection,
    account_id: &str,
    entity_type: &str,
) -> Result<Option<String>> {
    let mut stmt =
        conn.prepare("SELECT state FROM jmap_state WHERE account_id = ?1 AND entity_type = ?2")?;
    let result: Option<String> = stmt
        .query_row(params![account_id, entity_type], |row| row.get(0))
        .optional()?;
    // Treat empty-string sentinel (used to force a full re-sync) as "no state".
    Ok(result.filter(|s| !s.is_empty()))
}

/// Set/update the JMAP state string for an entity type.
pub fn set_jmap_state(
    conn: &Connection,
    account_id: &str,
    entity_type: &str,
    state: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO jmap_state (account_id, entity_type, state, updated_at)
         VALUES (?1, ?2, ?3, datetime('now'))
         ON CONFLICT(account_id, entity_type) DO UPDATE SET
            state = excluded.state,
            updated_at = excluded.updated_at",
        params![account_id, entity_type, state],
    )?;
    Ok(())
}

/// One row of the `jmap_state` table, returned verbatim for inspection
/// callers (e.g. `cmd_status`). Unlike `get_jmap_state`, this preserves
/// the empty-string forced-resync sentinel so callers can distinguish
/// "never synced" from "scheduled for full re-pull".
pub struct JmapStateRow {
    pub account_id: String,
    pub entity_type: String,
    pub state: String,
    pub updated_at: String,
}

/// Read every row of `jmap_state`, sorted by `(account_id, entity_type)`
/// for stable display. Returns an empty Vec when no sync has run yet.
pub fn list_jmap_state_rows(conn: &Connection) -> Result<Vec<JmapStateRow>> {
    let mut stmt = conn.prepare(
        "SELECT account_id, entity_type, state, updated_at FROM jmap_state \
         ORDER BY account_id, entity_type",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(JmapStateRow {
                account_id: row.get(0)?,
                entity_type: row.get(1)?,
                state: row.get(2)?,
                updated_at: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// --- Message Map ---

#[derive(Clone)]
pub struct MessageRecord {
    pub jmap_email_id: JmapEmailId,
    pub jmap_blob_id: Option<JmapBlobId>,
    pub jmap_thread_id: Option<JmapThreadId>,
    pub jmap_mailbox_id: JmapMailboxId,
    pub maildir_id: Option<MaildirId>,
    pub message_id: MessageId,
    pub flags: String,
    pub jmap_keywords: String,
}

/// Insert or update a message mapping.
pub fn upsert_message(conn: &Connection, msg: &MessageRecord) -> Result<()> {
    conn.execute(
        "INSERT INTO message_map (
            jmap_email_id, jmap_blob_id, jmap_thread_id, jmap_mailbox_id,
            maildir_id, message_id, flags, jmap_keywords,
            last_synced_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, datetime('now'))
        ON CONFLICT(jmap_email_id) DO UPDATE SET
            jmap_blob_id = excluded.jmap_blob_id,
            jmap_thread_id = excluded.jmap_thread_id,
            jmap_mailbox_id = excluded.jmap_mailbox_id,
            maildir_id = excluded.maildir_id,
            message_id = excluded.message_id,
            flags = excluded.flags,
            jmap_keywords = excluded.jmap_keywords,
            last_synced_at = datetime('now')",
        params![
            msg.jmap_email_id,
            msg.jmap_blob_id,
            msg.jmap_thread_id,
            msg.jmap_mailbox_id,
            msg.maildir_id,
            msg.message_id,
            msg.flags,
            msg.jmap_keywords,
        ],
    )?;
    Ok(())
}

/// Look up a message by JMAP email ID.
pub fn get_message_by_jmap_id(
    conn: &Connection,
    jmap_email_id: &JmapEmailId,
) -> Result<Option<MessageRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_email_id, jmap_blob_id, jmap_thread_id, jmap_mailbox_id,
                maildir_id, message_id, flags, jmap_keywords
         FROM message_map WHERE jmap_email_id = ?1",
    )?;
    let result = stmt
        .query_row(params![jmap_email_id], |row| {
            Ok(MessageRecord {
                jmap_email_id: row.get(0)?,
                jmap_blob_id: row.get(1)?,
                jmap_thread_id: row.get(2)?,
                jmap_mailbox_id: row.get(3)?,
                maildir_id: row.get(4)?,
                message_id: row.get(5)?,
                flags: row.get(6)?,
                jmap_keywords: row.get(7)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Delete a message mapping by JMAP email ID.
pub fn delete_message_by_jmap_id(conn: &Connection, jmap_email_id: &JmapEmailId) -> Result<()> {
    conn.execute(
        "DELETE FROM message_map WHERE jmap_email_id = ?1",
        params![jmap_email_id],
    )?;
    Ok(())
}

/// True iff `message_map` has at least one row. Used by the dedupe
/// pass to decide whether reconcile will need a `LocalIndex` this
/// cycle: an empty table means initial sync or post-recovery wipe,
/// where reconcile's stage-1 (DB-derived) lookup will miss every
/// remote Message-ID and the on-disk index is the only thing that
/// avoids re-downloading already-present files.
pub fn has_message_map_rows(conn: &Connection) -> Result<bool> {
    let n: i64 = conn.query_row("SELECT EXISTS(SELECT 1 FROM message_map)", [], |row| {
        row.get(0)
    })?;
    Ok(n != 0)
}

/// Get all messages in a given mailbox.
pub fn get_messages_by_jmap_mailbox_id(
    conn: &Connection,
    jmap_mailbox_id: &JmapMailboxId,
) -> Result<Vec<MessageRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_email_id, jmap_blob_id, jmap_thread_id, jmap_mailbox_id,
                maildir_id, message_id, flags, jmap_keywords
         FROM message_map WHERE jmap_mailbox_id = ?1",
    )?;
    let rows = stmt.query_map(params![jmap_mailbox_id], |row| {
        Ok(MessageRecord {
            jmap_email_id: row.get(0)?,
            jmap_blob_id: row.get(1)?,
            jmap_thread_id: row.get(2)?,
            jmap_mailbox_id: row.get(3)?,
            maildir_id: row.get(4)?,
            message_id: row.get(5)?,
            flags: row.get(6)?,
            jmap_keywords: row.get(7)?,
        })
    })?;
    let mut messages = Vec::new();
    for row in rows {
        messages.push(row?);
    }
    Ok(messages)
}

// --- Mailbox Map ---

pub struct MailboxRecord {
    pub jmap_mailbox_id: JmapMailboxId,
    pub name: String,
    pub role: Option<String>,
    pub parent_id: Option<JmapMailboxId>,
    pub maildir_folder: String,
    pub sort_order: i32,
}

/// Insert or update a mailbox mapping.
pub fn upsert_mailbox(conn: &Connection, mb: &MailboxRecord) -> Result<()> {
    conn.execute(
        "INSERT INTO mailbox_map (jmap_mailbox_id, name, role, parent_id, maildir_folder, sort_order)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(jmap_mailbox_id) DO UPDATE SET
            name = excluded.name,
            role = excluded.role,
            parent_id = excluded.parent_id,
            maildir_folder = excluded.maildir_folder,
            sort_order = excluded.sort_order",
        params![
            mb.jmap_mailbox_id,
            mb.name,
            mb.role,
            mb.parent_id,
            mb.maildir_folder,
            mb.sort_order,
        ],
    )?;
    Ok(())
}

/// List the distinct `maildir_folder` names recorded in `mailbox_map`,
/// sorted. Used by `cmd_status` to compute the maildir-vs-DB drift set
/// without paying for the full row hydration that `get_all_mailboxes`
/// does.
pub fn list_known_maildir_folders(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT DISTINCT maildir_folder FROM mailbox_map ORDER BY maildir_folder")?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// List every `jmap_mailbox_id` currently recorded in `mailbox_map`,
/// sorted. Lighter than `get_all_mailboxes` for callers that only
/// need the id set, e.g. diffing the cached set against a fresh
/// `Mailbox/get` response to find ids the server no longer has.
pub fn list_known_mailbox_ids(conn: &Connection) -> Result<Vec<JmapMailboxId>> {
    let mut stmt =
        conn.prepare("SELECT jmap_mailbox_id FROM mailbox_map ORDER BY jmap_mailbox_id")?;
    let rows = stmt
        .query_map([], |row| row.get::<_, JmapMailboxId>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Delete the `mailbox_map` row for `id`. Missing rows are a no-op
/// (zero rows affected, still `Ok(())`); the caller does not need
/// to pre-check existence. Counterpart to `upsert_mailbox` for the
/// server-side deletion path, where a mailbox we previously cached
/// no longer appears in the latest `Mailbox/get` response.
pub fn delete_mailbox(conn: &Connection, id: &JmapMailboxId) -> Result<()> {
    conn.execute(
        "DELETE FROM mailbox_map WHERE jmap_mailbox_id = ?1",
        params![id],
    )?;
    Ok(())
}

/// Get all mailbox mappings.
pub fn get_all_mailboxes(conn: &Connection) -> Result<Vec<MailboxRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_mailbox_id, name, role, parent_id, maildir_folder, sort_order
         FROM mailbox_map ORDER BY sort_order, name",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(MailboxRecord {
            jmap_mailbox_id: row.get(0)?,
            name: row.get(1)?,
            role: row.get(2)?,
            parent_id: row.get(3)?,
            maildir_folder: row.get(4)?,
            sort_order: row.get(5)?,
        })
    })?;
    let mut mailboxes = Vec::new();
    for row in rows {
        mailboxes.push(row?);
    }
    Ok(mailboxes)
}

// --- Local State ---

/// Record the local filesystem state of a message.
pub fn upsert_local_state(
    conn: &Connection,
    maildir_id: &MaildirId,
    maildir_folder: &str,
    flags: &str,
    mtime: Option<i64>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO local_state (maildir_id, maildir_folder, flags, mtime, recorded_at)
         VALUES (?1, ?2, ?3, ?4, datetime('now'))
         ON CONFLICT(maildir_id) DO UPDATE SET
            maildir_folder = excluded.maildir_folder,
            flags = excluded.flags,
            mtime = excluded.mtime,
            recorded_at = datetime('now')",
        params![maildir_id, maildir_folder, flags, mtime],
    )?;
    Ok(())
}

/// Get all local state records for a folder, keyed by maildir_id -> (folder, flags).
pub fn get_local_state_for_folder(
    conn: &Connection,
    folder: &str,
) -> Result<HashMap<MaildirId, (String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT maildir_id, maildir_folder, flags FROM local_state WHERE maildir_folder = ?1",
    )?;
    let rows = stmt.query_map(params![folder], |row| {
        Ok((
            row.get::<_, MaildirId>(0)?,
            (row.get::<_, String>(1)?, row.get::<_, String>(2)?),
        ))
    })?;
    let mut map = HashMap::new();
    for row in rows {
        let (id, state) = row?;
        map.insert(id, state);
    }
    Ok(map)
}

/// Update only the `flags` column for a `local_state` row. Used by
/// keyword-update mirrors that already know the row exists for this
/// `maildir_id` and shouldn't have to re-supply the folder. No-op when
/// the row is missing.
pub fn update_local_state_flags(
    conn: &Connection,
    maildir_id: &MaildirId,
    flags: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE local_state
            SET flags = ?2, recorded_at = datetime('now')
          WHERE maildir_id = ?1",
        params![maildir_id, flags],
    )?;
    Ok(())
}

/// Delete a local state record.
pub fn delete_local_state(conn: &Connection, maildir_id: &MaildirId) -> Result<()> {
    conn.execute(
        "DELETE FROM local_state WHERE maildir_id = ?1",
        params![maildir_id],
    )?;
    Ok(())
}

// --- Discovery Cache ---

/// Get the cached JMAP session URL for a domain, or None if no
/// discovery has been persisted for it.
pub fn get_cached_session_url(conn: &Connection, domain: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT session_url FROM jmap_discovery WHERE domain = ?1")?;
    let result = stmt
        .query_row(params![domain], |row| row.get::<_, String>(0))
        .optional()?;
    Ok(result)
}

/// Persist a discovered session URL for a domain, overwriting any
/// prior entry.
pub fn set_cached_session_url(conn: &Connection, domain: &str, session_url: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO jmap_discovery (domain, session_url, discovered_at)
         VALUES (?1, ?2, datetime('now'))
         ON CONFLICT(domain) DO UPDATE SET
            session_url = excluded.session_url,
            discovered_at = excluded.discovered_at",
        params![domain, session_url],
    )?;
    Ok(())
}

/// Drop any cached session URL for a domain. Idempotent: silently
/// does nothing when no entry exists.
pub fn clear_cached_session_url(conn: &Connection, domain: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM jmap_discovery WHERE domain = ?1",
        params![domain],
    )?;
    Ok(())
}

// --- Folder Checkpoint ---

/// One row of the `folder_checkpoint` table -- a snapshot of a
/// maildir folder's cur/ and new/ subdirectories taken at the end
/// of a successful sync cycle. The Phase 0 dedupe gate compares a
/// fresh snapshot against the recorded row to decide whether the
/// folder needs another dedupe walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FolderCheckpoint {
    pub cur_mtime_ns: i64,
    pub new_mtime_ns: i64,
    pub cur_count: i64,
    pub new_count: i64,
}

/// Read the checkpoint row for one folder, or None when no cycle
/// has ever checkpointed it (first sync, recovery, freshly added).
pub fn get_folder_checkpoint(conn: &Connection, folder: &str) -> Result<Option<FolderCheckpoint>> {
    let mut stmt = conn.prepare(
        "SELECT cur_mtime_ns, new_mtime_ns, cur_count, new_count \
         FROM folder_checkpoint WHERE maildir_folder = ?1",
    )?;
    let row = stmt
        .query_row(params![folder], |row| {
            Ok(FolderCheckpoint {
                cur_mtime_ns: row.get(0)?,
                new_mtime_ns: row.get(1)?,
                cur_count: row.get(2)?,
                new_count: row.get(3)?,
            })
        })
        .optional()?;
    Ok(row)
}

/// Upsert the checkpoint row for one folder, stamping `checkpointed_at`
/// to the current time. Called once per synced folder at the tail of
/// every successful cycle.
pub fn upsert_folder_checkpoint(
    conn: &Connection,
    folder: &str,
    cp: &FolderCheckpoint,
) -> Result<()> {
    conn.execute(
        "INSERT INTO folder_checkpoint \
            (maildir_folder, cur_mtime_ns, new_mtime_ns, cur_count, new_count, checkpointed_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, datetime('now')) \
         ON CONFLICT(maildir_folder) DO UPDATE SET \
            cur_mtime_ns = excluded.cur_mtime_ns, \
            new_mtime_ns = excluded.new_mtime_ns, \
            cur_count = excluded.cur_count, \
            new_count = excluded.new_count, \
            checkpointed_at = excluded.checkpointed_at",
        params![
            folder,
            cp.cur_mtime_ns,
            cp.new_mtime_ns,
            cp.cur_count,
            cp.new_count,
        ],
    )?;
    Ok(())
}

// Bring in the Optional extension trait
use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::db::open_or_recreate;
    use tempfile::tempdir;

    fn record_with_maildir_id(jmap_email_id: &str, maildir_id: Option<&str>) -> MessageRecord {
        MessageRecord {
            jmap_email_id: jmap_email_id.into(),
            jmap_blob_id: None,
            jmap_thread_id: None,
            jmap_mailbox_id: "MB-INBOX".into(),
            maildir_id: maildir_id.map(MaildirId::from),
            message_id: "msg@x".into(),
            flags: String::new(),
            jmap_keywords: "{}".into(),
        }
    }

    /// Two distinct `jmap_email_id`s claiming the same non-NULL
    /// `maildir_id` must fail at the SQLite layer. Pins the partial
    /// UNIQUE index that turns the code-level 1:1 invariant
    /// (one local file backs at most one server email) into a
    /// schema-enforced one. Without this index, an adoption-ordering
    /// bug that emits two AdoptLocalMessage actions for the same
    /// maildir_id would silently corrupt message_map; with it, the
    /// second insert errors out and the bug surfaces immediately.
    #[test]
    fn upsert_rejects_duplicate_maildir_id_across_jmap_ids() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let conn = open_or_recreate(&db_path).unwrap();

        let first = record_with_maildir_id("E1", Some("1700.M1.host"));
        upsert_message(&conn, &first).expect("first insert should succeed");

        let second = record_with_maildir_id("E2", Some("1700.M1.host"));
        let err =
            upsert_message(&conn, &second).expect_err("duplicate maildir_id must be rejected");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("UNIQUE constraint failed") || msg.contains("unique constraint"),
            "expected UNIQUE violation, got: {msg}"
        );
    }

    /// Multiple rows with NULL `maildir_id` are allowed -- the index
    /// is partial (`WHERE maildir_id IS NOT NULL`) so the
    /// pre-adoption state, where many `jmap_email_id`s wait without
    /// a bound local file, stays valid. Pins the partial-index shape
    /// against a future refactor that drops the WHERE clause.
    #[test]
    fn upsert_allows_multiple_null_maildir_ids() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let conn = open_or_recreate(&db_path).unwrap();

        upsert_message(&conn, &record_with_maildir_id("E1", None)).unwrap();
        upsert_message(&conn, &record_with_maildir_id("E2", None)).unwrap();
        upsert_message(&conn, &record_with_maildir_id("E3", None)).unwrap();

        // Sanity: all three rows landed.
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM message_map", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3, "all NULL-maildir_id rows must be insertable");
    }

    /// Same `jmap_email_id` re-upserted with the same non-NULL
    /// `maildir_id` must succeed (ON CONFLICT UPDATE on the
    /// primary key). The partial UNIQUE index considers this the
    /// same row, not a new conflicting one. Without this guard the
    /// post-adoption re-sync (which re-emits every row each cycle)
    /// would error on every existing message.
    #[test]
    fn upsert_idempotent_on_same_jmap_id_same_maildir_id() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let conn = open_or_recreate(&db_path).unwrap();

        let row = record_with_maildir_id("E1", Some("1700.M1.host"));
        upsert_message(&conn, &row).unwrap();
        upsert_message(&conn, &row).expect("re-upserting the same row must be idempotent");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM message_map", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    fn mailbox_record(id: &str, name: &str, folder: &str) -> MailboxRecord {
        MailboxRecord {
            jmap_mailbox_id: JmapMailboxId::from(id),
            name: name.to_string(),
            role: None,
            parent_id: None,
            maildir_folder: folder.to_string(),
            sort_order: 0,
        }
    }

    /// `list_known_mailbox_ids` returns every cached id in sorted
    /// order. Cheaper than `get_all_mailboxes` when the caller only
    /// needs the id set; the server-deletion detector will diff
    /// this against a fresh `Mailbox/get`.
    #[test]
    fn list_known_mailbox_ids_returns_sorted_set() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let conn = open_or_recreate(&db_path).unwrap();

        upsert_mailbox(&conn, &mailbox_record("MB-INBOX", "Inbox", "INBOX")).unwrap();
        upsert_mailbox(&conn, &mailbox_record("MB-ARCH", "Archive", "Archive")).unwrap();
        upsert_mailbox(&conn, &mailbox_record("MB-SENT", "Sent", "Sent")).unwrap();

        let ids = list_known_mailbox_ids(&conn).unwrap();
        assert_eq!(
            ids,
            vec![
                JmapMailboxId::from("MB-ARCH"),
                JmapMailboxId::from("MB-INBOX"),
                JmapMailboxId::from("MB-SENT"),
            ]
        );
    }

    /// Empty `mailbox_map` returns an empty vec rather than erroring
    /// or panicking. The drift detector hits this on a freshly
    /// recreated state DB.
    #[test]
    fn list_known_mailbox_ids_empty_table_is_ok() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let conn = open_or_recreate(&db_path).unwrap();
        let ids = list_known_mailbox_ids(&conn).unwrap();
        assert!(ids.is_empty(), "expected empty vec, got: {ids:?}");
    }

    /// `delete_mailbox` removes the row whose `jmap_mailbox_id`
    /// matches and leaves the others untouched.
    #[test]
    fn delete_mailbox_removes_target_row_only() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let conn = open_or_recreate(&db_path).unwrap();

        upsert_mailbox(&conn, &mailbox_record("MB-INBOX", "Inbox", "INBOX")).unwrap();
        upsert_mailbox(&conn, &mailbox_record("MB-ARCH", "Archive", "Archive")).unwrap();

        delete_mailbox(&conn, &JmapMailboxId::from("MB-ARCH")).unwrap();

        let ids = list_known_mailbox_ids(&conn).unwrap();
        assert_eq!(ids, vec![JmapMailboxId::from("MB-INBOX")]);
    }

    /// `delete_mailbox` on a missing id is a silent no-op. The
    /// caller doesn't need to pre-check existence before issuing
    /// the delete.
    #[test]
    fn delete_mailbox_missing_id_is_noop() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let conn = open_or_recreate(&db_path).unwrap();

        upsert_mailbox(&conn, &mailbox_record("MB-INBOX", "Inbox", "INBOX")).unwrap();

        delete_mailbox(&conn, &JmapMailboxId::from("MB-DOES-NOT-EXIST"))
            .expect("missing id should not error");

        let ids = list_known_mailbox_ids(&conn).unwrap();
        assert_eq!(ids, vec![JmapMailboxId::from("MB-INBOX")]);
    }
}
