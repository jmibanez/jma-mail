use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::{HashMap, HashSet};

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

// --- Message Map ---

pub struct MessageRecord {
    pub jmap_email_id: String,
    pub jmap_blob_id: Option<String>,
    pub jmap_thread_id: Option<String>,
    pub mailbox_id: String,
    pub maildir_id: Option<String>,
    pub maildir_folder: Option<String>,
    pub message_id: Option<String>,
    pub flags: String,
    pub jmap_keywords: String,
}

/// Insert or update a message mapping.
pub fn upsert_message(conn: &Connection, msg: &MessageRecord) -> Result<()> {
    conn.execute(
        "INSERT INTO message_map (
            jmap_email_id, jmap_blob_id, jmap_thread_id, mailbox_id,
            maildir_id, maildir_folder, message_id, flags, jmap_keywords,
            last_synced_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, datetime('now'))
        ON CONFLICT(jmap_email_id) DO UPDATE SET
            jmap_blob_id = excluded.jmap_blob_id,
            jmap_thread_id = excluded.jmap_thread_id,
            mailbox_id = excluded.mailbox_id,
            maildir_id = excluded.maildir_id,
            maildir_folder = excluded.maildir_folder,
            message_id = excluded.message_id,
            flags = excluded.flags,
            jmap_keywords = excluded.jmap_keywords,
            last_synced_at = datetime('now')",
        params![
            msg.jmap_email_id,
            msg.jmap_blob_id,
            msg.jmap_thread_id,
            msg.mailbox_id,
            msg.maildir_id,
            msg.maildir_folder,
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
    jmap_email_id: &str,
) -> Result<Option<MessageRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_email_id, jmap_blob_id, jmap_thread_id, mailbox_id,
                maildir_id, maildir_folder, message_id, flags, jmap_keywords
         FROM message_map WHERE jmap_email_id = ?1",
    )?;
    let result = stmt
        .query_row(params![jmap_email_id], |row| {
            Ok(MessageRecord {
                jmap_email_id: row.get(0)?,
                jmap_blob_id: row.get(1)?,
                jmap_thread_id: row.get(2)?,
                mailbox_id: row.get(3)?,
                maildir_id: row.get(4)?,
                maildir_folder: row.get(5)?,
                message_id: row.get(6)?,
                flags: row.get(7)?,
                jmap_keywords: row.get(8)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Look up a message by RFC 5322 Message-ID. If multiple JMAP records share
/// the same Message-ID (e.g. the same email referenced in different mailboxes),
/// returns the first one.
pub fn get_message_by_message_id(
    conn: &Connection,
    message_id: &str,
) -> Result<Option<MessageRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_email_id, jmap_blob_id, jmap_thread_id, mailbox_id,
                maildir_id, maildir_folder, message_id, flags, jmap_keywords
         FROM message_map WHERE message_id = ?1 LIMIT 1",
    )?;
    let result = stmt
        .query_row(params![message_id], |row| {
            Ok(MessageRecord {
                jmap_email_id: row.get(0)?,
                jmap_blob_id: row.get(1)?,
                jmap_thread_id: row.get(2)?,
                mailbox_id: row.get(3)?,
                maildir_id: row.get(4)?,
                maildir_folder: row.get(5)?,
                message_id: row.get(6)?,
                flags: row.get(7)?,
                jmap_keywords: row.get(8)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Look up a message by Message-ID scoped to a specific maildir folder.
/// Used by the pull path so that a JMAP delivery for folder B never rebinds
/// against a local copy that lives in folder A — cross-folder copies are
/// distinct instances by design.
pub fn get_message_by_message_id_in_folder(
    conn: &Connection,
    message_id: &str,
    folder: &str,
) -> Result<Option<MessageRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_email_id, jmap_blob_id, jmap_thread_id, mailbox_id,
                maildir_id, maildir_folder, message_id, flags, jmap_keywords
         FROM message_map WHERE message_id = ?1 AND maildir_folder = ?2 LIMIT 1",
    )?;
    let result = stmt
        .query_row(params![message_id, folder], |row| {
            Ok(MessageRecord {
                jmap_email_id: row.get(0)?,
                jmap_blob_id: row.get(1)?,
                jmap_thread_id: row.get(2)?,
                mailbox_id: row.get(3)?,
                maildir_id: row.get(4)?,
                maildir_folder: row.get(5)?,
                message_id: row.get(6)?,
                flags: row.get(7)?,
                jmap_keywords: row.get(8)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Look up a message by maildir ID.
pub fn get_message_by_maildir_id(
    conn: &Connection,
    maildir_id: &str,
) -> Result<Option<MessageRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_email_id, jmap_blob_id, jmap_thread_id, mailbox_id,
                maildir_id, maildir_folder, message_id, flags, jmap_keywords
         FROM message_map WHERE maildir_id = ?1",
    )?;
    let result = stmt
        .query_row(params![maildir_id], |row| {
            Ok(MessageRecord {
                jmap_email_id: row.get(0)?,
                jmap_blob_id: row.get(1)?,
                jmap_thread_id: row.get(2)?,
                mailbox_id: row.get(3)?,
                maildir_id: row.get(4)?,
                maildir_folder: row.get(5)?,
                message_id: row.get(6)?,
                flags: row.get(7)?,
                jmap_keywords: row.get(8)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// All JMAP email IDs we currently have mapped, as a set for fast contains
/// checks during the adoption-aware initial pull.
pub fn get_all_jmap_email_ids(conn: &Connection) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare("SELECT jmap_email_id FROM message_map")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut out = HashSet::new();
    for row in rows {
        out.insert(row?);
    }
    Ok(out)
}

/// Delete a message mapping by JMAP email ID.
pub fn delete_message_by_jmap_id(conn: &Connection, jmap_email_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM message_map WHERE jmap_email_id = ?1",
        params![jmap_email_id],
    )?;
    Ok(())
}

/// Get all messages in a given mailbox folder.
pub fn get_messages_by_folder(conn: &Connection, folder: &str) -> Result<Vec<MessageRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_email_id, jmap_blob_id, jmap_thread_id, mailbox_id,
                maildir_id, maildir_folder, message_id, flags, jmap_keywords
         FROM message_map WHERE maildir_folder = ?1",
    )?;
    let rows = stmt.query_map(params![folder], |row| {
        Ok(MessageRecord {
            jmap_email_id: row.get(0)?,
            jmap_blob_id: row.get(1)?,
            jmap_thread_id: row.get(2)?,
            mailbox_id: row.get(3)?,
            maildir_id: row.get(4)?,
            maildir_folder: row.get(5)?,
            message_id: row.get(6)?,
            flags: row.get(7)?,
            jmap_keywords: row.get(8)?,
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
    pub jmap_mailbox_id: String,
    pub name: String,
    pub role: Option<String>,
    pub parent_id: Option<String>,
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

/// Look up a mailbox by its JMAP ID.
pub fn get_mailbox_by_jmap_id(
    conn: &Connection,
    jmap_mailbox_id: &str,
) -> Result<Option<MailboxRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_mailbox_id, name, role, parent_id, maildir_folder, sort_order
         FROM mailbox_map WHERE jmap_mailbox_id = ?1",
    )?;
    let result = stmt
        .query_row(params![jmap_mailbox_id], |row| {
            Ok(MailboxRecord {
                jmap_mailbox_id: row.get(0)?,
                name: row.get(1)?,
                role: row.get(2)?,
                parent_id: row.get(3)?,
                maildir_folder: row.get(4)?,
                sort_order: row.get(5)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Look up a mailbox by its local folder name.
pub fn get_mailbox_by_folder(conn: &Connection, folder: &str) -> Result<Option<MailboxRecord>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_mailbox_id, name, role, parent_id, maildir_folder, sort_order
         FROM mailbox_map WHERE maildir_folder = ?1",
    )?;
    let result = stmt
        .query_row(params![folder], |row| {
            Ok(MailboxRecord {
                jmap_mailbox_id: row.get(0)?,
                name: row.get(1)?,
                role: row.get(2)?,
                parent_id: row.get(3)?,
                maildir_folder: row.get(4)?,
                sort_order: row.get(5)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Delete a mailbox mapping.
pub fn delete_mailbox(conn: &Connection, jmap_mailbox_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM mailbox_map WHERE jmap_mailbox_id = ?1",
        params![jmap_mailbox_id],
    )?;
    Ok(())
}

// --- Local State ---

/// Record the local filesystem state of a message.
pub fn upsert_local_state(
    conn: &Connection,
    maildir_id: &str,
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
) -> Result<HashMap<String, (String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT maildir_id, maildir_folder, flags FROM local_state WHERE maildir_folder = ?1",
    )?;
    let rows = stmt.query_map(params![folder], |row| {
        Ok((
            row.get::<_, String>(0)?,
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

/// Delete a local state record.
pub fn delete_local_state(conn: &Connection, maildir_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM local_state WHERE maildir_id = ?1",
        params![maildir_id],
    )?;
    Ok(())
}

// Bring in the Optional extension trait
use rusqlite::OptionalExtension;
