//! Shared TUI state. The tracing layer writes; the render loop reads.
//!
//! Everything lives behind a single `Mutex<Inner>` because the access
//! pattern is "tracing event grabs the lock, mutates a couple of
//! fields, releases" and "render task grabs the lock once per frame,
//! reads a snapshot, releases." Contention is bounded by the render
//! cadence (10 Hz) -- no value to splitting locks per-field.
//!
//! Log lines are stored as already-formatted strings rather than the
//! original `tracing::Event`, so the lock-held window stays short:
//! formatting happens on the producer side, not while the render
//! task holds the lock.

use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use std::sync::Mutex;

/// Cap on the log ring buffer. Anything past the cap drops off the
/// front. Sized to fit a few minutes of normal-verbosity sync output
/// without unbounded growth; the user can still see history by
/// scrolling (TBD: scrollback isn't wired yet, current render shows
/// only the last screenful).
const LOG_CAPACITY: usize = 500;

pub struct TuiState {
    inner: Mutex<Inner>,
}

struct Inner {
    log: VecDeque<LogLine>,
    /// Most recent `notify!` line. Single-slot rather than a ring
    /// because the status bar shows one milestone at a time -- the
    /// log pane already holds history, so older milestones aren't
    /// lost, just demoted from the always-visible bar.
    status: Option<Status>,
}

#[derive(Clone)]
pub struct Status {
    pub at: DateTime<Utc>,
    pub message: String,
}

/// One log entry as the layer prepared it. Render formats this into
/// a `ratatui::text::Line` per frame; we keep the structured fields
/// here so future styling (color by level, dim the target, etc.)
/// doesn't need a re-parse.
#[derive(Clone)]
pub struct LogLine {
    pub ts: DateTime<Utc>,
    pub level: tracing::Level,
    pub target: String,
    pub message: String,
}

impl TuiState {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                log: VecDeque::with_capacity(LOG_CAPACITY),
                status: None,
            }),
        }
    }

    /// Append one log line. Drops the oldest entry if the ring is
    /// already at LOG_CAPACITY. Cheap enough to call from every
    /// tracing event the layer captures.
    pub fn push_log(&self, line: LogLine) {
        let mut i = self.inner.lock().expect("tui state mutex");
        if i.log.len() == LOG_CAPACITY {
            i.log.pop_front();
        }
        i.log.push_back(line);
    }

    /// Snapshot the last `n` log lines into a fresh Vec the render
    /// loop can format without holding the lock. `n` is typically the
    /// log-pane row count.
    pub fn recent_logs(&self, n: usize) -> Vec<LogLine> {
        let i = self.inner.lock().expect("tui state mutex");
        let start = i.log.len().saturating_sub(n);
        i.log.iter().skip(start).cloned().collect()
    }

    /// Replace the status line with a fresh `notify!`-style milestone.
    /// Driven by the `notify!` macro when the TUI is active; older
    /// statuses are dropped (history lives in the log pane).
    pub fn set_status(&self, message: String) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.status = Some(Status {
            at: chrono::Utc::now(),
            message,
        });
    }

    /// Current status snapshot, if any has been set. Render path
    /// clones this and releases the lock before drawing.
    pub fn status(&self) -> Option<Status> {
        let i = self.inner.lock().expect("tui state mutex");
        i.status.clone()
    }
}

impl Default for TuiState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(msg: &str) -> LogLine {
        LogLine {
            ts: chrono::Utc::now(),
            level: tracing::Level::INFO,
            target: "t".into(),
            message: msg.into(),
        }
    }

    /// The ring drops the oldest entry once LOG_CAPACITY is reached
    /// -- the buffer must stay bounded no matter how long the daemon
    /// runs.
    #[test]
    fn push_log_drops_oldest_at_capacity() {
        let state = TuiState::new();
        for i in 0..(LOG_CAPACITY + 3) {
            state.push_log(line(&format!("line {}", i)));
        }
        let logs = state.recent_logs(LOG_CAPACITY + 3);
        assert_eq!(logs.len(), LOG_CAPACITY);
        assert_eq!(logs.first().unwrap().message, "line 3");
        assert_eq!(
            logs.last().unwrap().message,
            format!("line {}", LOG_CAPACITY + 2)
        );
    }

    /// recent_logs returns the newest `n` lines, oldest first -- the
    /// tail the render pane paints top-to-bottom. Asking for more
    /// than exists returns everything without panicking.
    #[test]
    fn recent_logs_returns_tail_window() {
        let state = TuiState::new();
        for i in 0..10 {
            state.push_log(line(&format!("line {}", i)));
        }
        let msgs: Vec<_> = state
            .recent_logs(3)
            .into_iter()
            .map(|l| l.message)
            .collect();
        assert_eq!(msgs, ["line 7", "line 8", "line 9"]);
        assert_eq!(state.recent_logs(50).len(), 10);
    }

    /// The status slot holds exactly one milestone; a newer push
    /// replaces the older one outright. The log pane, not this slot,
    /// carries milestone history.
    #[test]
    fn status_slot_keeps_latest_only() {
        let state = TuiState::new();
        assert!(state.status().is_none());
        state.set_status("first".into());
        state.set_status("second".into());
        assert_eq!(state.status().unwrap().message, "second");
    }
}
