//! Acknowledged flag baselines and receipts for ambiguous I/O outcomes.
//!
//! These are synchronization history, not mail data or a general job queue.
//! A DB rebuild discards them and invokes bootstrap policy; an ordinary
//! restart retains them so observation is not mistaken for acknowledgement.

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::ids::{JmapEmailId, JmapMailboxId, MaildirId};
use crate::maildir_ops::flags::{flags_to_keywords, keywords_to_flags};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlagAttempt {
    pub source_flags: String,
    pub target_flags: String,
    pub source_maildir_id: Option<MaildirId>,
    pub source_folder: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlagSyncState {
    /// None means initial reconciliation has not completed.
    pub baseline: Option<String>,
    pub review: bool,
    pub attempt: Option<FlagAttempt>,
    pub blocked: bool,
    pub error: Option<String>,
    /// In-memory only: an old row has not yet been verified by Email/get.
    pub legacy: bool,
}

pub fn mapped_flags(flags: &str) -> String {
    keywords_to_flags(&flags_to_keywords(flags))
}

type StoredRow = (
    String,
    Option<String>,
    bool,
    Option<String>,
    bool,
    Option<String>,
);
fn decode(row: StoredRow) -> Result<(JmapEmailId, FlagSyncState)> {
    let (id, baseline, review, attempt, blocked, error) = row;
    let attempt = attempt
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .context("Invalid stored flag-attempt receipt; refusing to discard it")?;
    Ok((
        id.into(),
        FlagSyncState {
            baseline,
            review,
            attempt,
            blocked,
            error,
            legacy: false,
        },
    ))
}

pub fn load(conn: &Connection) -> Result<HashMap<JmapEmailId, FlagSyncState>> {
    let mut stmt = conn.prepare(
        "SELECT jmap_email_id, baseline, review, attempt, blocked, error FROM message_flag_sync",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
        ))
    })?;
    rows.map(|row| decode(row?)).collect()
}

/// Load only managed bindings. An excluded account/folder's history (including
/// a corrupt receipt) must not affect reconciliation of the selected scope.
pub fn load_for_mailboxes(
    conn: &Connection,
    mailbox_ids: &[JmapMailboxId],
) -> Result<HashMap<JmapEmailId, FlagSyncState>> {
    let mut stmt = conn.prepare(
        "SELECT f.jmap_email_id,f.baseline,f.review,f.attempt,f.blocked,f.error
        FROM message_map m JOIN message_flag_sync f ON f.jmap_email_id=m.jmap_email_id
        WHERE m.jmap_mailbox_id=?1",
    )?;
    let mut states = HashMap::new();
    let mut seen = HashSet::new();
    for mailbox in mailbox_ids.iter().filter(|id| seen.insert(*id)) {
        let rows = stmt.query_map([mailbox], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })?;
        for row in rows {
            let (id, state) = decode(row?)?;
            states.insert(id, state);
        }
    }
    Ok(states)
}

#[derive(Debug, Default)]
pub struct FlagPending {
    pub pending: Vec<JmapEmailId>,
    pub blocked: Vec<JmapEmailId>,
    pub unverified: Vec<JmapEmailId>,
}

impl FlagPending {
    pub fn retryable(&self) -> usize {
        self.pending.len() - self.blocked.len()
    }
}

struct PendingRow {
    id: JmapEmailId,
    has_history: bool,
    baseline: Option<String>,
    review: bool,
    attempt: Option<String>,
    blocked: bool,
    error: bool,
    remote: String,
    local: Option<String>,
}

/// Read-only, indexed per-mailbox pending status. Missing history remains
/// unverified, not settled; raw suffix extensions alone are not pending work.
pub fn summarize(conn: &Connection, mailbox_ids: &[JmapMailboxId]) -> Result<FlagPending> {
    let mut stmt = conn.prepare(
        "SELECT m.jmap_email_id,f.jmap_email_id IS NOT NULL,f.baseline,
        COALESCE(f.review,0),f.attempt,COALESCE(f.blocked,0),f.error IS NOT NULL,m.flags,l.flags
        FROM message_map m LEFT JOIN message_flag_sync f ON f.jmap_email_id=m.jmap_email_id
        LEFT JOIN local_state l ON l.maildir_id=m.maildir_id
        WHERE m.jmap_mailbox_id=?1 AND m.maildir_id IS NOT NULL AND
        (f.jmap_email_id IS NULL OR f.baseline IS NULL OR f.attempt IS NOT NULL
         OR f.review!=0 OR f.blocked!=0 OR f.error IS NOT NULL OR l.flags IS NULL
         OR f.baseline!=m.flags OR f.baseline!=l.flags)",
    )?;
    let mut status = FlagPending::default();
    let mut seen = HashSet::new();
    for mailbox in mailbox_ids.iter().filter(|id| seen.insert(*id)) {
        let rows = stmt.query_map([mailbox], |row| {
            Ok(PendingRow {
                id: row.get(0)?,
                has_history: row.get(1)?,
                baseline: row.get(2)?,
                review: row.get(3)?,
                attempt: row.get(4)?,
                blocked: row.get(5)?,
                error: row.get(6)?,
                remote: row.get(7)?,
                local: row.get(8)?,
            })
        })?;
        for row in rows {
            let row = row?;
            if !row.has_history {
                status.unverified.push(row.id);
                continue;
            }
            let unresolved = row.baseline.as_ref().is_none_or(|baseline| {
                mapped_flags(&row.remote) != *baseline
                    || row
                        .local
                        .as_ref()
                        .is_none_or(|local| mapped_flags(local) != *baseline)
            });
            if row.review || row.blocked || row.error || row.attempt.is_some() || unresolved {
                let invalid_receipt = row
                    .attempt
                    .as_ref()
                    .is_some_and(|json| serde_json::from_str::<FlagAttempt>(json).is_err());
                if row.review || row.blocked || invalid_receipt {
                    status.blocked.push(row.id.clone());
                }
                status.pending.push(row.id);
            }
        }
    }
    Ok(status)
}

pub fn get(conn: &Connection, id: &JmapEmailId) -> Result<Option<FlagSyncState>> {
    let row: Option<StoredRow> = conn.query_row(
        "SELECT jmap_email_id, baseline, review, attempt, blocked, error FROM message_flag_sync WHERE jmap_email_id=?1",
        [id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
    ).optional()?;
    row.map(decode)
        .transpose()
        .map(|row| row.map(|(_, state)| state))
}

pub fn store(conn: &Connection, id: &JmapEmailId, state: &FlagSyncState) -> Result<()> {
    let attempt = state
        .attempt
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    conn.execute("INSERT INTO message_flag_sync (jmap_email_id,baseline,review,attempt,blocked,error)
        VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(jmap_email_id) DO UPDATE SET
        baseline=excluded.baseline,review=excluded.review,attempt=excluded.attempt,blocked=excluded.blocked,error=excluded.error",
        params![id, state.baseline, state.review, attempt, state.blocked, state.error])?;
    Ok(())
}

pub fn acknowledge(conn: &Connection, id: &JmapEmailId, flags: &str) -> Result<()> {
    store(
        conn,
        id,
        &FlagSyncState {
            baseline: Some(mapped_flags(flags)),
            ..Default::default()
        },
    )
}

pub fn prepare_attempt(conn: &Connection, id: &JmapEmailId, attempt: FlagAttempt) -> Result<()> {
    // A missing row has no acknowledged generation. In particular a cold
    // adoption's observed server flags are not a baseline hint.
    let mut state = get(conn, id)?.unwrap_or_default();
    // A safe fresh pull may start from an older unacknowledged pull's target.
    // Replacing its receipt must not turn that suffix into independent MUA
    // intent if this new effect also fails or the process stops before ACK.
    // Success clears review in acknowledge; otherwise retain explicit review.
    if state.attempt.as_ref().is_some_and(|previous| {
        let source = mapped_flags(&attempt.source_flags);
        mapped_flags(&previous.source_flags) != previous.target_flags
            && source == previous.target_flags
            && source != attempt.target_flags
    }) {
        state.review = true;
    }
    state.attempt = Some(attempt);
    state.blocked = false;
    state.error = None;
    store(conn, id, &state)
}

pub fn failure(
    conn: &Connection,
    id: &JmapEmailId,
    blocked: bool,
    rejected: bool,
    error: &str,
) -> Result<()> {
    let mut state = get(conn, id)?.unwrap_or_default();
    // A confirmed rejection cannot later serve as evidence of our own write.
    // Ambiguous transport/missing-ack outcomes retain their receipt.
    if rejected {
        state.attempt = None;
    }
    state.blocked = blocked;
    state.error = Some(error.into());
    store(conn, id, &state)
}

/// Mailbox IDs with bound messages, including retained orphan bindings which
/// no longer have a mailbox_map row. Selection belongs to the caller.
pub fn bound_mailbox_ids(conn: &Connection) -> Result<Vec<JmapMailboxId>> {
    let mut stmt = conn
        .prepare("SELECT DISTINCT jmap_mailbox_id FROM message_map WHERE maildir_id IS NOT NULL")?;
    Ok(stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?)
}

/// Store-wide compatibility query. Runtime reconciliation uses summarize with
/// its resolved selected mailbox IDs instead of this global view.
pub fn pending_ids(conn: &Connection, retryable_only: bool) -> Result<Vec<JmapEmailId>> {
    let status = summarize(conn, &bound_mailbox_ids(conn)?)?;
    let blocked: HashSet<_> = status.blocked.into_iter().collect();
    Ok(status
        .pending
        .into_iter()
        .filter(|id| !retryable_only || !blocked.contains(id))
        .collect())
}

pub fn has_unverified(conn: &Connection) -> Result<bool> {
    conn.query_row("SELECT EXISTS(SELECT 1 FROM message_map m LEFT JOIN message_flag_sync f
        ON f.jmap_email_id=m.jmap_email_id WHERE m.maildir_id IS NOT NULL AND f.jmap_email_id IS NULL)", [], |row| row.get(0)).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{db, queries};

    fn seed(conn: &Connection) {
        queries::upsert_message(
            conn,
            &queries::MessageRecord {
                jmap_email_id: "E1".into(),
                jmap_blob_id: None,
                jmap_thread_id: None,
                jmap_mailbox_id: "MB1".into(),
                maildir_id: Some("local".into()),
                message_id: "a@x".into(),
                flags: "S".into(),
                jmap_keywords: r#"{"$seen":true}"#.into(),
            },
        )
        .unwrap();
        queries::upsert_local_state(conn, &"local".into(), "INBOX", "SX", None).unwrap();
    }

    fn attempt() -> FlagAttempt {
        FlagAttempt {
            source_flags: "SX".into(),
            target_flags: "S".into(),
            source_maildir_id: Some("local".into()),
            source_folder: Some("INBOX".into()),
        }
    }

    #[test]
    fn observations_and_receipts_are_not_acknowledgements() {
        let conn = db::open_in_memory().unwrap();
        seed(&conn);
        assert!(has_unverified(&conn).unwrap());
        prepare_attempt(&conn, &"E1".into(), attempt()).unwrap();
        let state = get(&conn, &"E1".into()).unwrap().unwrap();
        assert!(state.baseline.is_none());
        assert_eq!(state.attempt, Some(attempt()));
        assert_eq!(
            pending_ids(&conn, true).unwrap(),
            vec![JmapEmailId::from("E1")]
        );
        acknowledge(&conn, &"E1".into(), "S").unwrap();
        assert!(
            pending_ids(&conn, false).unwrap().is_empty(),
            "unowned X is not flag work"
        );
    }

    #[test]
    fn confirmed_rejection_cannot_be_mistaken_for_an_ambiguous_write() {
        let conn = db::open_in_memory().unwrap();
        seed(&conn);
        acknowledge(&conn, &"E1".into(), "F").unwrap();
        prepare_attempt(&conn, &"E1".into(), attempt()).unwrap();
        failure(&conn, &"E1".into(), false, false, "missing ack").unwrap();
        assert!(get(&conn, &"E1".into()).unwrap().unwrap().attempt.is_some());
        failure(&conn, &"E1".into(), true, true, "forbidden").unwrap();
        let state = get(&conn, &"E1".into()).unwrap().unwrap();
        assert_eq!(state.baseline.as_deref(), Some("F"));
        assert!(state.attempt.is_none() && state.blocked);
        assert_eq!(pending_ids(&conn, false).unwrap().len(), 1);
        assert!(pending_ids(&conn, true).unwrap().is_empty());
    }

    #[test]
    fn scoped_summary_separates_history_and_is_read_only() {
        let conn = db::open_in_memory().unwrap();
        seed(&conn);
        let scope = [JmapMailboxId::from("MB1")];
        assert_eq!(
            summarize(&conn, &scope).unwrap().unverified,
            vec![JmapEmailId::from("E1")]
        );
        assert!(summarize(&conn, &[]).unwrap().unverified.is_empty());
        assert!(
            summarize(&conn, &["excluded".into()])
                .unwrap()
                .unverified
                .is_empty()
        );
        acknowledge(&conn, &"E1".into(), "S").unwrap();
        assert!(
            summarize(&conn, &scope).unwrap().pending.is_empty(),
            "unowned X is not pending"
        );
        failure(&conn, &"E1".into(), true, true, "forbidden").unwrap();
        let before: u64 = conn
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();
        conn.pragma_update(None, "query_only", true).unwrap();
        let status = summarize(&conn, &scope).unwrap();
        assert_eq!(status.pending, vec![JmapEmailId::from("E1")]);
        assert_eq!(status.blocked, status.pending);
        assert_eq!(
            status.retryable(),
            0,
            "cached agreement cannot erase an unfinished rejected generation"
        );
        assert_eq!(load_for_mailboxes(&conn, &scope).unwrap().len(), 1);
        let after: u64 = conn
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn scoped_history_does_not_decode_unselected_corrupt_receipts() {
        let conn = db::open_in_memory().unwrap();
        seed(&conn);
        prepare_attempt(&conn, &"E1".into(), attempt()).unwrap();
        conn.execute("UPDATE message_flag_sync SET attempt='invalid'", [])
            .unwrap();
        assert!(
            load_for_mailboxes(&conn, &["excluded".into()])
                .unwrap()
                .is_empty()
        );
        assert!(load_for_mailboxes(&conn, &["MB1".into()]).is_err());
        let status = summarize(&conn, &["MB1".into()]).unwrap();
        assert_eq!(
            status.blocked.len(),
            1,
            "a corrupt receipt needs review, not transport retry"
        );
    }

    #[test]
    fn deleting_a_message_cascades_history_but_invalid_receipts_are_not_ignored() {
        let conn = db::open_in_memory().unwrap();
        seed(&conn);
        prepare_attempt(&conn, &"E1".into(), attempt()).unwrap();
        conn.execute("UPDATE message_flag_sync SET attempt='invalid'", [])
            .unwrap();
        assert!(load(&conn).is_err());
        assert!(get(&conn, &"E1".into()).is_err());
        queries::delete_message_by_jmap_id(&conn, &"E1".into()).unwrap();
        assert!(load(&conn).unwrap().is_empty());
    }
}
