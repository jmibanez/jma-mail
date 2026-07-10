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
    /// Monotonic count of every line ever pushed -- never decremented,
    /// even when the ring evicts from the front. It gives each entry a
    /// stable identity (its push ordinal): the render pins a
    /// scrolled-back viewport to one entry by ordinal so the same
    /// content stays put across resizes and new arrivals, however the
    /// wrapping shifts. Wraparound is not a concern at any realistic
    /// log rate.
    pushed_total: u64,
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
    /// Health of the filesystem watcher as reported by the daemon
    /// runner. The watcher task is one-shot -- any exit, clean or
    /// not, means local changes stop syncing until the daemon is
    /// restarted -- so this channel is the only place that failure
    /// is visible at all.
    watcher_conn: Option<ConnState>,
    /// Outcome of the most recent completed sync cycle. Kept in its
    /// own slot because cycle results otherwise flow through the
    /// single-slot `notify!` status, where any later milestone
    /// overwrites them.
    last_cycle: Option<CycleSummary>,
}

/// One completed sync cycle's outcome for the Network pane's "last"
/// row. `in_sync` means the cycle found nothing to do; the counts
/// are only meaningful when it is false. `wall` is the whole
/// cycle's wall-clock as timed by the daemon runner.
#[derive(Clone, Copy, Debug)]
pub struct CycleSummary {
    pub at: DateTime<Utc>,
    pub downloaded: u64,
    pub uploaded: u64,
    pub in_sync: bool,
    pub wall: Duration,
}

/// One connection channel's state. The engine and SSE channels only
/// use `Connected` / `Reconnecting` -- both retry forever with
/// backoff, so for them every degraded state is actively healing.
/// `Down` is terminal: the filesystem watcher is a one-shot task
/// that is never restarted, so once it reports down it stays down
/// for the life of the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnState {
    Connected,
    Reconnecting { backoff: Duration },
    Down,
}

/// Snapshot of every health channel for the status bar.
#[derive(Clone, Copy, Debug)]
pub struct ConnHealth {
    pub engine: Option<ConnState>,
    pub sse: Option<ConnState>,
    pub watcher: Option<ConnState>,
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
                pushed_total: 0,
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
                watcher_conn: None,
                last_cycle: None,
            }),
        }
    }

    /// Append one log line. Drops the oldest entry if the ring is
    /// already at LOG_CAPACITY. Cheap enough to call from every
    /// tracing event the layer captures.
    ///
    /// `pushed_total` bumps on every call so each entry gets a stable
    /// push ordinal; the render uses it to keep a scrolled-back
    /// viewport pinned to the same entry as new lines arrive.
    pub fn push_log(&self, line: LogLine) {
        let mut i = self.inner.lock().expect("tui state mutex");
        if i.log.len() == LOG_CAPACITY {
            i.log.pop_front();
        }
        i.log.push_back(line);
        i.pushed_total += 1;
    }

    /// Snapshot the whole log buffer for rendering, along with the
    /// monotonic push counter.
    ///
    /// The render wraps the lines itself -- only it knows the pane
    /// width -- so it needs the raw entries, not a pre-sliced window,
    /// and it owns the scroll position (which entry is pinned). The
    /// counter lets it map that pinned entry's push ordinal to a
    /// current buffer index; the oldest buffered entry's ordinal is
    /// `pushed_total - lines.len()`. The buffer is bounded by
    /// LOG_CAPACITY, so the clone is a fixed ceiling regardless of
    /// uptime.
    pub fn log_snapshot(&self) -> LogSnapshot {
        let i = self.inner.lock().expect("tui state mutex");
        LogSnapshot {
            lines: i.log.iter().cloned().collect(),
            pushed_total: i.pushed_total,
        }
    }
}

/// Snapshot returned by `log_snapshot`: the full log buffer plus the
/// push counter the render needs to place the scroll viewport.
pub struct LogSnapshot {
    /// Every buffered entry, oldest first.
    pub lines: Vec<LogLine>,
    /// Monotonic count of lines ever pushed. The oldest buffered
    /// entry's push ordinal is `pushed_total - lines.len()`.
    pub pushed_total: u64,
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

    /// Estimated time to finish the current download batch:
    /// remaining messages over the recent download rate, where the
    /// rate is In-direction blob closes per second across the
    /// bandwidth window. None when no download is in flight, nothing
    /// remains, the window holds no download samples, or the newest
    /// download sample is older than the idle threshold -- a stalled
    /// transfer shows no ETA rather than a frozen one.
    pub fn download_eta(&self) -> Option<Duration> {
        let i = self.inner.lock().expect("tui state mutex");
        let Progress { done, total } = i.download_progress?;
        let remaining = total.saturating_sub(done);
        if remaining == 0 {
            return None;
        }
        let now = Instant::now();
        let window_cutoff = now.checked_sub(BW_WINDOW);
        let mut count: u64 = 0;
        let mut newest: Option<Instant> = None;
        for s in &i.bw_samples {
            let in_window = match window_cutoff {
                Some(c) => s.at >= c,
                None => true,
            };
            if in_window && s.direction == Direction::In {
                count += 1;
                // Unlike bandwidth()'s newest_at, this deliberately
                // tracks In samples only: the ETA must go stale when
                // downloads idle even while uploads keep the window
                // warm.
                newest = Some(newest.map_or(s.at, |old| old.max(s.at)));
            }
        }
        let stale = match newest {
            None => true,
            Some(a) => now.duration_since(a) > BW_IDLE_THRESHOLD,
        };
        if stale || count == 0 {
            return None;
        }
        let rate = count as f64 / BW_WINDOW.as_secs_f64();
        Some(Duration::from_secs_f64(remaining as f64 / rate))
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

    /// Record the filesystem watcher's state. Same latest-wins rule
    /// as the other channels; in practice the watcher reports
    /// connected once at startup and down at most once, on exit.
    pub fn set_watcher_conn(&self, state: ConnState) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.watcher_conn = Some(state);
    }

    /// Snapshot every health channel for the status bar.
    pub fn conn_health(&self) -> ConnHealth {
        let i = self.inner.lock().expect("tui state mutex");
        ConnHealth {
            engine: i.engine_conn,
            sse: i.sse_conn,
            watcher: i.watcher_conn,
        }
    }

    /// Record a completed sync cycle's outcome, stamped now. Latest
    /// cycle wins; history lives in the log pane.
    pub fn set_last_cycle(&self, downloaded: u64, uploaded: u64, in_sync: bool, wall: Duration) {
        let mut i = self.inner.lock().expect("tui state mutex");
        i.last_cycle = Some(CycleSummary {
            at: chrono::Utc::now(),
            downloaded,
            uploaded,
            in_sync,
            wall,
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
        let logs = state.log_snapshot().lines;
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

    /// The download ETA is remaining messages over the recent
    /// download rate: ten In-direction closes inside the window give
    /// a rate of 10/BW_WINDOW per second, so 60 remaining messages
    /// estimate to 60/rate seconds. With no samples at all there is
    /// no rate and no ETA.
    #[test]
    fn download_eta_uses_in_direction_rate() {
        let state = TuiState::new();
        assert!(state.download_eta().is_none());
        state.set_download_progress(40, 100);
        // Progress alone isn't enough -- no samples means no rate.
        assert!(state.download_eta().is_none());
        for _ in 0..10 {
            state.add_bandwidth(Direction::In, 1_000);
        }
        let eta = state.download_eta().expect("eta with a live rate");
        let expected = 60.0 * BW_WINDOW.as_secs_f64() / 10.0;
        assert!(
            (eta.as_secs_f64() - expected).abs() < 0.5,
            "eta {:?}, expected ~{}s",
            eta,
            expected
        );
    }

    /// Upload closes must not feed the download rate -- the ETA
    /// estimates the dl row's counter, not general wire activity --
    /// and a finished batch has no ETA even with a live rate.
    #[test]
    fn download_eta_ignores_uploads_and_finished_batches() {
        let state = TuiState::new();
        state.set_download_progress(40, 100);
        for _ in 0..10 {
            state.add_bandwidth(Direction::Out, 1_000);
        }
        assert!(state.download_eta().is_none());

        state.add_bandwidth(Direction::In, 1_000);
        state.set_download_progress(100, 100);
        assert!(state.download_eta().is_none());
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

    /// Latest report wins on every health channel: a repeated
    /// Reconnecting overwrites so the displayed backoff tracks the
    /// runner's doubling, Connected clears the degraded state, and
    /// the watcher's one-shot Down lands like any other report.
    #[test]
    fn conn_channels_track_latest_report() {
        let state = TuiState::new();
        let health = state.conn_health();
        assert!(health.engine.is_none() && health.sse.is_none() && health.watcher.is_none());

        state.set_engine_conn(ConnState::Reconnecting {
            backoff: Duration::from_secs(2),
        });
        state.set_engine_conn(ConnState::Reconnecting {
            backoff: Duration::from_secs(4),
        });
        state.set_sse_conn(ConnState::Connected);
        state.set_watcher_conn(ConnState::Connected);
        let health = state.conn_health();
        assert_eq!(
            health.engine,
            Some(ConnState::Reconnecting {
                backoff: Duration::from_secs(4)
            })
        );
        assert_eq!(health.sse, Some(ConnState::Connected));
        assert_eq!(health.watcher, Some(ConnState::Connected));

        state.set_engine_conn(ConnState::Connected);
        assert_eq!(state.conn_health().engine, Some(ConnState::Connected));

        state.set_watcher_conn(ConnState::Down);
        assert_eq!(state.conn_health().watcher, Some(ConnState::Down));
    }

    /// The last-cycle slot is latest-wins: a fresh outcome replaces
    /// the prior one outright, and the recorded counts and duration
    /// survive as given.
    #[test]
    fn last_cycle_keeps_latest_outcome() {
        let state = TuiState::new();
        assert!(state.last_cycle().is_none());
        state.set_last_cycle(12, 3, false, Duration::from_millis(1_200));
        state.set_last_cycle(0, 0, true, Duration::from_millis(300));
        let cycle = state.last_cycle().unwrap();
        assert!(cycle.in_sync);
        assert_eq!((cycle.downloaded, cycle.uploaded), (0, 0));
        assert_eq!(cycle.wall, Duration::from_millis(300));
    }

    /// Push `n` sequentially numbered lines -- helper for the
    /// scroll tests.
    fn push_n(state: &TuiState, n: usize) {
        for i in 0..n {
            state.push_log(line(&format!("line {}", i)));
        }
    }

    /// A fresh snapshot hands back the whole buffer in order and counts
    /// every push -- the render, not the state, decides how many rows
    /// fit and which entry is pinned. `pushed_total` is what lets the
    /// render turn a pinned entry's ordinal into a buffer index, so pin
    /// down that it tracks every push and survives eviction.
    #[test]
    fn log_snapshot_returns_all_lines_and_counts_pushes() {
        let state = TuiState::new();
        push_n(&state, 10);
        let snap = state.log_snapshot();
        assert_eq!(snap.pushed_total, 10);
        assert_eq!(snap.lines.len(), 10);
        assert_eq!(snap.lines.first().unwrap().message, "line 0");
        assert_eq!(snap.lines.last().unwrap().message, "line 9");

        // Past capacity the counter keeps climbing while the buffer
        // stays capped, so `pushed_total - len` names the oldest
        // surviving entry's ordinal.
        push_n(&state, LOG_CAPACITY);
        let snap = state.log_snapshot();
        assert_eq!(snap.pushed_total as usize, 10 + LOG_CAPACITY);
        assert_eq!(snap.lines.len(), LOG_CAPACITY);
    }
}
