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
/// without unbounded growth; scrollback (Up/PgUp/Home) lets the user
/// reach anything still in the ring.
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

/// Cap on the "recent messages" ring. The render shows however many
/// rows fit the pane, so this cap only bounds memory; it is sized
/// comfortably above any realistic pane height (half the terminal,
/// minus borders) so the pane can always be filled from the ring.
const RECENT_CAPACITY: usize = 128;

pub struct TuiState {
    inner: Mutex<Inner>,
}

struct Inner {
    log: VecDeque<LogLine>,
    /// Lines above the live tail. 0 means "follow the bottom" --
    /// new entries auto-appear. >0 means the user has scrolled
    /// back; `push_log` increments the offset in lockstep so the
    /// pinned content doesn't drift as new lines arrive.
    log_scroll: usize,
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
    /// Stack of currently-entered phase spans, most recent on top.
    /// The render picks the top of the stack as "what is sync
    /// doing now"; everything below is an enclosing phase (e.g.
    /// `execute` wraps `download_blobs`). Empty between sync cycles.
    phase_stack: Vec<ActivePhase>,
    /// Last completed phase, if any. Shown after the stack empties
    /// so the status bar's phase slot doesn't go blank between
    /// cycles -- "last: execute (1.2 s)" is more useful than empty
    /// space.
    last_phase: Option<CompletedPhase>,
    /// Live download progress when `download_blobs` is active.
    /// Cleared when the phase ends so a stale 50/50 doesn't sit on
    /// screen forever. Upload progress isn't tracked here -- the
    /// upload stream doesn't expose a per-message counter, only an
    /// at-the-end success count, so there's nothing to plot.
    download_progress: Option<Progress>,
    /// Most recently stored messages (downloads only, today --
    /// uploads are messages the user already saw). Newest at the
    /// back; oldest drops off the front when capacity is reached.
    recent: VecDeque<RecentMessage>,
    /// Health of the JMAP session as last reported by the daemon
    /// runner: None until the first report, then Connected or
    /// Reconnecting. Engine reconnecting means sync itself is down,
    /// not just the push channel.
    engine_conn: Option<ConnState>,
    /// Health of the SSE push channel as reported by the listener's
    /// internal reconnect loop. Degraded push means remote changes
    /// stop arriving as triggers until the stream comes back; local
    /// FS triggers keep working and the listener retries forever.
    sse_conn: Option<ConnState>,
    /// Outcome of the most recent completed sync cycle. Kept in its
    /// own slot because cycle results otherwise flow through the
    /// single-slot `notify!` status, where any later milestone
    /// overwrites them.
    last_cycle: Option<CycleSummary>,
}

/// One completed sync cycle's outcome for the Network pane's "last"
/// row. `in_sync` means the cycle found nothing to do; the counts
/// are only meaningful when it is false.
#[derive(Clone, Copy, Debug)]
pub struct CycleSummary {
    pub at: DateTime<Utc>,
    pub downloaded: u64,
    pub uploaded: u64,
    pub in_sync: bool,
}

/// One connection channel's state. Every degraded state is actively
/// retrying (both the engine and the SSE listener reconnect forever
/// with backoff), so there is no terminal "down" variant -- the
/// backoff carries the "how bad is it" signal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnState {
    Connected,
    Reconnecting { backoff: Duration },
}

/// Snapshot of both connection channels for the status bar.
#[derive(Clone, Copy, Debug)]
pub struct ConnHealth {
    pub engine: Option<ConnState>,
    pub sse: Option<ConnState>,
}

/// One row in the Recent pane. Subject is decoded -- mailparse's
/// `get_first_value` already unwraps RFC 2047 encoded-words -- so
/// the render path is just a styled push.
#[derive(Clone)]
pub struct RecentMessage {
    pub at: DateTime<Utc>,
    pub folder: String,
    pub subject: String,
}

/// One entry in the phase stack. Tracks the span's name and the
/// instant it became active so the render can show elapsed-so-far.
#[derive(Clone)]
pub struct ActivePhase {
    pub name: String,
    pub started: Instant,
    /// Opaque id used to match the span on close. The Layer assigns
    /// these from the `tracing::span::Id` it gets; the TUI never
    /// interprets the value, just compares for equality during pop.
    pub id: u64,
}

#[derive(Clone)]
pub struct CompletedPhase {
    pub name: String,
    pub elapsed: Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct Progress {
    pub done: u64,
    pub total: u64,
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
                log_scroll: 0,
                status: None,
                bytes_in_total: 0,
                bytes_out_total: 0,
                bw_samples: VecDeque::new(),
                phase_stack: Vec::new(),
                last_phase: None,
                download_progress: None,
                recent: VecDeque::with_capacity(RECENT_CAPACITY),
                engine_conn: None,
                sse_conn: None,
                last_cycle: None,
            }),
        }
    }

    /// Append one log line. Drops the oldest entry if the ring is
    /// already at LOG_CAPACITY. Cheap enough to call from every
    /// tracing event the layer captures.
    ///
    /// When the user has scrolled back (log_scroll > 0), bumping the
    /// offset by 1 keeps the pinned content visually stable: the
    /// "distance from newest" framing means a new line at the bottom
    /// would shift the visible window otherwise. The bump is clamped
    /// to `log.len() - 1` so the user can't accidentally scroll past
    /// the oldest available line via incoming-traffic drift.
    pub fn push_log(&self, line: LogLine) {
        let mut i = self.inner.lock().expect("tui state mutex");
        if i.log.len() == LOG_CAPACITY {
            i.log.pop_front();
        }
        i.log.push_back(line);
        if i.log_scroll > 0 {
            let max = i.log.len().saturating_sub(1);
            i.log_scroll = (i.log_scroll + 1).min(max);
        }
    }

    /// Snapshot a height-tall window of the log buffer.
    ///
    /// Returns the lines to render plus the scroll offset (0 means
    /// live-following) -- the render path uses the offset to surface
    /// a "[scrolled +N]" indicator in the title.
    pub fn log_view(&self, height: usize) -> LogView {
        let i = self.inner.lock().expect("tui state mutex");
        let total = i.log.len();
        let end = total.saturating_sub(i.log_scroll);
        let start = end.saturating_sub(height);
        let lines: Vec<LogLine> = i
            .log
            .iter()
            .skip(start)
            .take(end - start)
            .cloned()
            .collect();
        LogView {
            lines,
            scroll: i.log_scroll,
        }
    }

    /// Scroll the log view by `delta` lines toward the past. Clamped
    /// so we never pin past the oldest available line. `height` is
    /// the visible row count, used to compute the clamp.
    pub fn scroll_log_back(&self, delta: usize, height: usize) {
        let mut i = self.inner.lock().expect("tui state mutex");
        let max = i.log.len().saturating_sub(height);
        i.log_scroll = (i.log_scroll + delta).min(max);
    }

    /// Scroll the log view by `delta` lines toward the present. A
    /// move past the live tail collapses to 0 (back on the bottom,
    /// auto-follow re-enabled).
    pub fn scroll_log_forward(&self, delta: usize) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.log_scroll = i.log_scroll.saturating_sub(delta);
    }

    /// Jump to the top of the buffer (or as close to it as the ring
    /// still holds). `height` is the visible row count.
    pub fn scroll_log_to_top(&self, height: usize) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.log_scroll = i.log.len().saturating_sub(height);
    }

    /// Jump back to the live tail.
    pub fn scroll_log_to_bottom(&self) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.log_scroll = 0;
    }
}

/// Snapshot returned by `log_view`. `lines` is the formatted-ready
/// window; `scroll` is the current offset from the tail (0 means
/// live-following) so the render path can decorate the title.
pub struct LogView {
    pub lines: Vec<LogLine>,
    pub scroll: usize,
}

impl TuiState {
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

    /// Push a phase onto the active stack. `id` comes from the
    /// tracing span; the matching pop in `complete_phase` uses it
    /// to leave the stack consistent even if phases nest in an
    /// order we didn't anticipate.
    pub fn enter_phase(&self, id: u64, name: String) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.phase_stack.push(ActivePhase {
            name,
            started: Instant::now(),
            id,
        });
    }

    /// Pop the matching phase. If the id doesn't match anything on
    /// the stack we drop the close on the floor rather than mutating
    /// state we don't understand -- the stack would resync once the
    /// outermost phase ends and the next cycle starts fresh.
    pub fn complete_phase(&self, id: u64) {
        let mut i = self.inner.lock().expect("tui state mutex");
        let Some(idx) = i.phase_stack.iter().rposition(|p| p.id == id) else {
            return;
        };
        let phase = i.phase_stack.remove(idx);
        let elapsed = phase.started.elapsed();
        let name = phase.name;
        // Clearing download progress when its phase ends keeps a
        // stale 50/50 from sitting on screen across cycles. The
        // name-check is so an unrelated phase pop doesn't blank a
        // still-live readout.
        if name == "download_blobs" {
            i.download_progress = None;
        }
        i.last_phase = Some(CompletedPhase { name, elapsed });
    }

    pub fn current_phase(&self) -> Option<ActivePhase> {
        let i = self.inner.lock().expect("tui state mutex");
        i.phase_stack.last().cloned()
    }

    pub fn last_phase(&self) -> Option<CompletedPhase> {
        let i = self.inner.lock().expect("tui state mutex");
        i.last_phase.clone()
    }

    pub fn set_download_progress(&self, done: u64, total: u64) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.download_progress = Some(Progress { done, total });
    }

    pub fn download_progress(&self) -> Option<Progress> {
        let i = self.inner.lock().expect("tui state mutex");
        i.download_progress
    }

    /// Push one freshly-synced message into the recent ring.
    /// Trimmed inline to `RECENT_CAPACITY`. The timestamp is wall-
    /// clock (UTC), matching the log pane's convention so the two
    /// panels read the same way.
    pub fn push_recent(&self, folder: String, subject: String) {
        let mut i = self.inner.lock().expect("tui state mutex");
        if i.recent.len() == RECENT_CAPACITY {
            i.recent.pop_front();
        }
        i.recent.push_back(RecentMessage {
            at: chrono::Utc::now(),
            folder,
            subject,
        });
    }

    /// Snapshot the newest `n` recent messages, oldest first (the
    /// render reverses for newest-on-top). `n` is typically the
    /// pane's inner row count; asking for more than the ring holds
    /// returns everything.
    pub fn recent(&self, n: usize) -> Vec<RecentMessage> {
        let i = self.inner.lock().expect("tui state mutex");
        let start = i.recent.len().saturating_sub(n);
        i.recent.iter().skip(start).cloned().collect()
    }

    /// Record the JMAP session channel's state. Latest report wins;
    /// repeated Reconnecting reports overwrite so the displayed
    /// backoff tracks the runner's actual retry cadence.
    pub fn set_engine_conn(&self, state: ConnState) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.engine_conn = Some(state);
    }

    /// Record the SSE push channel's state. Same latest-wins rule as
    /// the engine channel.
    pub fn set_sse_conn(&self, state: ConnState) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.sse_conn = Some(state);
    }

    /// Snapshot both connection channels for the status bar.
    pub fn conn_health(&self) -> ConnHealth {
        let i = self.inner.lock().expect("tui state mutex");
        ConnHealth {
            engine: i.engine_conn,
            sse: i.sse_conn,
        }
    }

    /// Record a completed sync cycle's outcome, stamped now. Latest
    /// cycle wins; history lives in the log pane.
    pub fn set_last_cycle(&self, downloaded: u64, uploaded: u64, in_sync: bool) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.last_cycle = Some(CycleSummary {
            at: chrono::Utc::now(),
            downloaded,
            uploaded,
            in_sync,
        });
    }

    pub fn last_cycle(&self) -> Option<CycleSummary> {
        let i = self.inner.lock().expect("tui state mutex");
        i.last_cycle
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
        let logs = state.log_view(LOG_CAPACITY + 3).lines;
        assert_eq!(logs.len(), LOG_CAPACITY);
        assert_eq!(logs.first().unwrap().message, "line 3");
        assert_eq!(
            logs.last().unwrap().message,
            format!("line {}", LOG_CAPACITY + 2)
        );
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

    /// The download counter is cleared exactly when its owning phase
    /// pops. An unrelated pop must not blank a still-live readout,
    /// and the `download_blobs` pop must not leave a stale count
    /// behind for the next cycle.
    #[test]
    fn download_progress_clears_on_its_phase_pop_only() {
        let state = TuiState::new();
        state.enter_phase(1, "execute".into());
        state.enter_phase(2, "download_blobs".into());
        state.set_download_progress(5, 10);
        state.complete_phase(1);
        assert!(state.download_progress().is_some());
        state.complete_phase(2);
        assert!(state.download_progress().is_none());
    }

    /// The deepest active phase is what the status bar shows, and
    /// pop-by-id tolerates out-of-order closes without corrupting
    /// the stack.
    #[test]
    fn phase_stack_tracks_deepest_and_pops_by_id() {
        let state = TuiState::new();
        state.enter_phase(1, "execute".into());
        state.enter_phase(2, "download_blobs".into());
        assert_eq!(state.current_phase().unwrap().name, "download_blobs");
        state.complete_phase(1);
        assert_eq!(state.current_phase().unwrap().name, "download_blobs");
        state.complete_phase(2);
        assert!(state.current_phase().is_none());
        assert_eq!(state.last_phase().unwrap().name, "download_blobs");
    }

    /// The recent ring bounds memory at RECENT_CAPACITY (oldest
    /// drops off the front), and `recent(n)` hands the render the
    /// newest `n` rows, oldest first, so the pane can be filled to
    /// exactly its own height.
    #[test]
    fn recent_ring_is_bounded_and_returns_newest_tail() {
        let state = TuiState::new();
        for i in 0..(RECENT_CAPACITY + 2) {
            state.push_recent("INBOX".into(), format!("subject {}", i));
        }
        let all = state.recent(RECENT_CAPACITY + 2);
        assert_eq!(all.len(), RECENT_CAPACITY);
        assert_eq!(all.first().unwrap().subject, "subject 2");
        let tail: Vec<String> = state.recent(3).into_iter().map(|m| m.subject).collect();
        assert_eq!(
            tail,
            vec![
                format!("subject {}", RECENT_CAPACITY - 1),
                format!("subject {}", RECENT_CAPACITY),
                format!("subject {}", RECENT_CAPACITY + 1),
            ]
        );
    }

    /// Latest report wins on both connection channels: a repeated
    /// Reconnecting overwrites so the displayed backoff tracks the
    /// runner's doubling, and Connected clears the degraded state.
    #[test]
    fn conn_channels_track_latest_report() {
        let state = TuiState::new();
        let health = state.conn_health();
        assert!(health.engine.is_none() && health.sse.is_none());

        state.set_engine_conn(ConnState::Reconnecting {
            backoff: Duration::from_secs(2),
        });
        state.set_engine_conn(ConnState::Reconnecting {
            backoff: Duration::from_secs(4),
        });
        state.set_sse_conn(ConnState::Connected);
        let health = state.conn_health();
        assert_eq!(
            health.engine,
            Some(ConnState::Reconnecting {
                backoff: Duration::from_secs(4)
            })
        );
        assert_eq!(health.sse, Some(ConnState::Connected));

        state.set_engine_conn(ConnState::Connected);
        assert_eq!(state.conn_health().engine, Some(ConnState::Connected));
    }

    /// The last-cycle slot is latest-wins: a fresh outcome replaces
    /// the prior one outright, and the recorded counts survive as
    /// given.
    #[test]
    fn last_cycle_keeps_latest_outcome() {
        let state = TuiState::new();
        assert!(state.last_cycle().is_none());
        state.set_last_cycle(12, 3, false);
        state.set_last_cycle(0, 0, true);
        let cycle = state.last_cycle().unwrap();
        assert!(cycle.in_sync);
        assert_eq!((cycle.downloaded, cycle.uploaded), (0, 0));
    }

    /// Push `n` sequentially numbered lines -- helper for the
    /// scroll tests.
    fn push_n(state: &TuiState, n: usize) {
        for i in 0..n {
            state.push_log(line(&format!("line {}", i)));
        }
    }

    /// Default view is the live tail: scroll == 0, last `height`
    /// lines, and a height taller than the buffer returns everything
    /// without panicking. Pins the "no scroll, no surprises" baseline
    /// so a future refactor that defaults to a non-zero offset is
    /// caught.
    #[test]
    fn log_view_at_tail_shows_newest() {
        let state = TuiState::new();
        push_n(&state, 10);
        let view = state.log_view(3);
        assert_eq!(view.scroll, 0);
        let msgs: Vec<_> = view.lines.iter().map(|l| l.message.clone()).collect();
        assert_eq!(msgs, vec!["line 7", "line 8", "line 9"]);
        assert_eq!(state.log_view(50).lines.len(), 10);
    }

    /// When the user is scrolled back, a new push must bump the
    /// offset by 1 so the pinned content stays where the eye left
    /// it. Without this, a steady incoming-log stream would drag
    /// the visible window forward and the user would lose their
    /// reading position. Critical UX invariant -- regress this and
    /// scrollback is useless during active sync.
    #[test]
    fn push_log_keeps_pinned_content_stable() {
        let state = TuiState::new();
        push_n(&state, 10);
        state.scroll_log_back(3, 3); // pin showing lines 4..7
        let before = state.log_view(3);
        assert_eq!(
            before.lines.iter().map(|l| &l.message).collect::<Vec<_>>(),
            vec!["line 4", "line 5", "line 6"]
        );

        push_n(&state, 5); // 15 lines total now, scroll auto-bumped

        let after = state.log_view(3);
        // Same window content as before -- not the new lines.
        assert_eq!(
            after.lines.iter().map(|l| &l.message).collect::<Vec<_>>(),
            vec!["line 4", "line 5", "line 6"]
        );
    }

    /// End / scroll_log_to_bottom snaps back to the live tail and
    /// re-enables auto-follow (scroll == 0). Important enough to
    /// pin because it's the user's escape hatch when they're done
    /// reading scrollback.
    #[test]
    fn scroll_to_bottom_re_enables_follow() {
        let state = TuiState::new();
        push_n(&state, 10);
        state.scroll_log_back(5, 3);
        assert!(state.log_view(3).scroll > 0);
        state.scroll_log_to_bottom();
        assert_eq!(state.log_view(3).scroll, 0);
    }
}
