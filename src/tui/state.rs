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
use std::time::{Duration, Instant};

/// Cap on the log ring buffer. Anything past the cap drops off the
/// front. Sized to fit a few minutes of normal-verbosity sync output
/// without unbounded growth; the user can still see history by
/// scrolling (TBD: scrollback isn't wired yet, current render shows
/// only the last screenful).
const LOG_CAPACITY: usize = 500;

/// Window over which the bandwidth panel averages. Short enough
/// that the displayed rate tracks current activity -- a long-
/// horizon average reads as "still downloading" long after a
/// burst ends, draining to zero only as samples age out; long
/// enough to smooth out single-blob jitter so the number doesn't
/// bounce wildly on every closed span.
const BW_WINDOW: Duration = Duration::from_secs(5);

/// If no blob has closed within this interval, the rate snaps to
/// zero rather than showing the tail of the rolling average.
/// Without it, after downloads stop the panel still shows non-zero
/// bytes/s for the full BW_WINDOW duration -- accurate as an
/// average, but reads as phantom activity. The threshold is shorter
/// than the window itself so "downloads stopped" surfaces before
/// the samples have aged out.
const BW_IDLE_THRESHOLD: Duration = Duration::from_secs(1);

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
    /// Cumulative blob bytes since the daemon started. The rolling
    /// `rate_*` calc samples bw_samples for the recent window;
    /// totals are independent and survive past the trim horizon.
    bytes_in_total: u64,
    bytes_out_total: u64,
    /// One entry per closed blob span. Trimmed to `BW_WINDOW`
    /// elapsed on every push, so the window cost is bounded by the
    /// blob rate over that window (in practice: at most a few
    /// hundred entries during a heavy fetch). Stamped with
    /// `Instant` rather than wall-clock so the rate calc is immune
    /// to clock adjustments.
    bw_samples: VecDeque<BwSample>,
}

/// One blob's contribution to the bandwidth window. Stamped at the
/// span-close moment, which is "when the bytes completed transfer"
/// as far as the user can perceive it. The mid-flight `bytes`
/// recording on the span is for the profile layer's accounting; the
/// TUI doesn't try to interpolate.
#[derive(Clone, Copy)]
pub struct BwSample {
    pub at: Instant,
    pub direction: Direction,
    pub bytes: u64,
}

/// Which side of the JMAP exchange a byte count belongs to.
/// `In` is data the server sent us (blob downloads); `Out` is data
/// we sent to the server (Email/import uploads). Maildir-write bytes
/// are not bandwidth -- they're disk throughput -- and don't land
/// in either bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    In,
    Out,
}

/// Snapshot of the bandwidth panel's data. `rate_*` is bytes/sec
/// averaged over `BW_WINDOW`; `total_*` is the run-cumulative count.
#[derive(Clone, Copy, Debug)]
pub struct Bandwidth {
    pub rate_in: f64,
    pub rate_out: f64,
    pub total_in: u64,
    pub total_out: u64,
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
                bytes_in_total: 0,
                bytes_out_total: 0,
                bw_samples: VecDeque::new(),
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

    /// Record one blob's bytes against the appropriate direction.
    /// Called from `TuiLayer::on_close` for `jma::profile::blob`
    /// spans. The window is trimmed inline so the next render's
    /// rate calculation walks a bounded list.
    pub fn add_bandwidth(&self, direction: Direction, bytes: u64) {
        let now = Instant::now();
        let mut i = self.inner.lock().expect("tui state mutex");
        match direction {
            Direction::In => i.bytes_in_total += bytes,
            Direction::Out => i.bytes_out_total += bytes,
        }
        i.bw_samples.push_back(BwSample {
            at: now,
            direction,
            bytes,
        });
        while let Some(front) = i.bw_samples.front() {
            if now.duration_since(front.at) > BW_WINDOW {
                i.bw_samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Snapshot the bandwidth panel. Rates are bytes/sec averaged
    /// over `BW_WINDOW`; totals are cumulative since process start.
    /// When the newest sample is older than `BW_IDLE_THRESHOLD`,
    /// the rate snaps to zero so post-burst idle is visible at a
    /// glance rather than masked by the still-warm window.
    pub fn bandwidth(&self) -> Bandwidth {
        let i = self.inner.lock().expect("tui state mutex");
        let now = Instant::now();
        let window_cutoff = now.checked_sub(BW_WINDOW);
        let secs = BW_WINDOW.as_secs_f64();
        let mut sum_in: u64 = 0;
        let mut sum_out: u64 = 0;
        let mut newest_at: Option<Instant> = None;
        for s in &i.bw_samples {
            let in_window = match window_cutoff {
                Some(c) => s.at >= c,
                None => true,
            };
            if in_window {
                match s.direction {
                    Direction::In => sum_in += s.bytes,
                    Direction::Out => sum_out += s.bytes,
                }
                newest_at = Some(newest_at.map_or(s.at, |old| old.max(s.at)));
            }
        }
        let is_idle = match newest_at {
            None => true,
            Some(a) => now.duration_since(a) > BW_IDLE_THRESHOLD,
        };
        let (rate_in, rate_out) = if is_idle {
            (0.0, 0.0)
        } else {
            (sum_in as f64 / secs, sum_out as f64 / secs)
        };
        Bandwidth {
            rate_in,
            rate_out,
            total_in: i.bytes_in_total,
            total_out: i.bytes_out_total,
        }
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

    /// Cumulative totals track every byte we've seen, regardless of
    /// whether the sample is still inside the window. Pins the
    /// separation between "running total" and "rate over the recent
    /// window" so a future refactor of the trim loop doesn't
    /// accidentally make totals drift down.
    #[test]
    fn totals_count_every_sample() {
        let state = TuiState::new();
        state.add_bandwidth(Direction::In, 1_000);
        state.add_bandwidth(Direction::In, 2_000);
        state.add_bandwidth(Direction::Out, 500);
        let bw = state.bandwidth();
        assert_eq!(bw.total_in, 3_000);
        assert_eq!(bw.total_out, 500);
    }

    /// Rate is bytes-in-window divided by window-seconds. The test
    /// samples land within the same instant so they're definitely
    /// inside the window (and well under the idle threshold); we
    /// just want to confirm the divisor is the window duration,
    /// not the per-sample elapsed time. The latter would produce
    /// "instantaneous bps" which spikes wildly on small samples and
    /// would be useless as a display.
    #[test]
    fn rate_divides_total_by_window_seconds() {
        let state = TuiState::new();
        // 1 KB per window-second worth of bytes -- rate should be
        // exactly 1 KB/s regardless of the chosen window size.
        state.add_bandwidth(Direction::In, BW_WINDOW.as_secs() * 1_000);
        let bw = state.bandwidth();
        assert!(
            (bw.rate_in - 1_000.0).abs() < 1.0,
            "rate_in was {}",
            bw.rate_in
        );
    }

    /// With no samples at all, the rate is zero. Pins the no-data
    /// case so a future refactor that divides by sample count (and
    /// would panic on /0) is caught.
    #[test]
    fn empty_bandwidth_is_zero() {
        let state = TuiState::new();
        let bw = state.bandwidth();
        assert_eq!(bw.rate_in, 0.0);
        assert_eq!(bw.rate_out, 0.0);
    }
}
