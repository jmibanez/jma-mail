//! Render loop + key handling for the `jma watch` TUI.
//!
//! Runs on a dedicated thread (via `tokio::task::spawn_blocking`)
//! because crossterm's event polling is sync and we don't want to
//! block the tokio runtime. The loop alternates between rendering a
//! frame and polling for key events on a single ~100 ms tick; key
//! presses can interrupt the poll early and cause an immediate
//! re-render.
//!
//! Shutdown: 'q' / Esc / Ctrl-C signal the `shutdown` `Notify` and
//! exit the loop. The caller in `cmd_watch` is responsible for
//! propagating that to the daemon (a `tokio::select!` arm watching
//! the same `Notify`).
//!
//! Terminal lifecycle: raw mode + alternate screen on the way in,
//! restored on every way out -- normal return, a setup failure, or
//! a panic unwinding out of the render loop -- via a drop guard
//! armed as soon as raw mode is on. The user can always exit the
//! daemon, but they should be able to read what exploded.

use anyhow::{Context, Result};
use crossterm::cursor;
use crossterm::event::{
    self, Event as CrosstermEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use std::io::{Stdout, stdout};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::tui::state::{
    ActivePhase, Bandwidth, CompletedPhase, ConnHealth, ConnState, CycleSummary, LogLine, Progress,
    Status, TuiState,
};

/// Frame cadence. Crossterm's `poll` returns early on key events, so
/// this is the upper bound on time-to-redraw, not the only redraw
/// signal.
const TICK: Duration = Duration::from_millis(100);

/// Drive the render loop until the user requests shutdown.
///
/// Returns `Ok(())` on clean exit (user pressed q / Esc / Ctrl-C, or
/// the external `shutdown` was triggered). Returns `Err` only if
/// terminal setup or rendering hits an irrecoverable I/O error; the
/// caller in `cmd_watch` logs that and tears down regardless.
///
/// `manual_sync` is notified when the user presses 's'; the caller
/// wires it to the daemon as a sync trigger. The TUI knows nothing
/// about triggers -- it just raises the signal.
pub fn run(state: Arc<TuiState>, shutdown: Arc<Notify>, manual_sync: Arc<Notify>) -> Result<()> {
    enable_raw_mode().context("enable raw mode")?;
    // Armed before any further fallible step: from here, every exit
    // from this function -- normal return, a setup error below, or
    // a panic unwinding out of the render loop -- restores the
    // terminal. Raw mode or the alternate screen outliving the
    // process would leave the user's shell unusable until `reset`.
    let _guard = TerminalGuard;

    let mut terminal = setup_terminal().context("Failed to initialize TUI terminal")?;

    let result = run_loop(&mut terminal, state, &shutdown, &manual_sync);

    crate::tui::mark_active(false);
    if let Err(e) = restore_terminal(&mut terminal) {
        // Explicit restore on the normal path so a failure is
        // reported; the guard's pass behind it is silent.
        eprintln!("TUI: failed to restore terminal: {:#}", e);
    }
    result
}

/// Puts the terminal back together -- raw mode off, alternate screen
/// left, cursor shown -- when dropped, and lowers the active flag so
/// the stderr/stdout gating window ends with the display. Restore is
/// best-effort and silent: the normal exit path reports failures via
/// the explicit `restore_terminal` call before this runs, and
/// re-running the restore steps afterward is harmless.
struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        crate::tui::mark_active(false);
        let _ = disable_raw_mode();
        let _ = execute!(stdout(), LeaveAlternateScreen, cursor::Show);
    }
}

type Tui = Terminal<CrosstermBackend<Stdout>>;

/// Raw mode is the caller's job: `run` enables it and arms the
/// restore guard before any of the steps here can fail.
fn setup_terminal() -> Result<Tui> {
    let mut out = stdout();
    execute!(out, EnterAlternateScreen).context("enter alternate screen")?;
    let backend = CrosstermBackend::new(out);
    let mut terminal = Terminal::new(backend).context("construct ratatui terminal")?;
    // The first frame's diff treats the screen as all-blank and
    // skips cells matching the blank baseline (every blank cell, on
    // the first frame), so correctness depends on the alternate
    // screen actually being blank. Entering it does not
    // guarantee that: clearing on entry is emulator-dependent, and
    // stale alt-screen content shows through the skipped cells
    // (observed as leftover digits inside the pane title). Clear
    // explicitly; this also resets ratatui's diff baseline.
    terminal.clear().context("clear alternate screen")?;
    Ok(terminal)
}

fn restore_terminal(terminal: &mut Tui) -> Result<()> {
    disable_raw_mode().context("disable raw mode")?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen).context("leave alternate screen")?;
    terminal.show_cursor().context("show cursor")?;
    Ok(())
}

fn run_loop(
    terminal: &mut Tui,
    state: Arc<TuiState>,
    shutdown: &Arc<Notify>,
    manual_sync: &Arc<Notify>,
) -> Result<()> {
    // The log pane's scroll position lives here, not in `TuiState`:
    // only the render touches it, and resolving it needs the pane width
    // the shared state never sees. Key presses record their intent on
    // it; the next `draw_log` resolves that intent against the current
    // wrapping.
    let mut log_scroll = LogScroll::default();
    let mut show_help = false;
    loop {
        terminal
            .draw(|frame| {
                draw(frame, &state, show_help, &mut log_scroll);
            })
            .context("draw frame")?;

        if event::poll(TICK).context("poll for terminal event")?
            && let CrosstermEvent::Key(key) = event::read().context("read terminal event")?
        {
            if is_redraw_key(key) {
                // Clear resets ratatui's diff baseline, so the next
                // draw rewrites every cell rather than diffing against
                // a possibly-corrupted screen. Handled ahead of the
                // overlay branch so a redraw never doubles as a close.
                terminal.clear().context("clear on redraw")?;
            } else if show_help {
                match help_overlay_key(key) {
                    HelpKey::Quit => {
                        shutdown.notify_waiters();
                        return Ok(());
                    }
                    HelpKey::Close => show_help = false,
                    HelpKey::Ignore => {}
                }
            } else if is_help_key(key) {
                show_help = true;
            } else if is_sync_key(key) {
                // notify_one stores at most one permit, so holding
                // the key down can't queue a burst of cycles; the
                // daemon's coalescing window absorbs the rest.
                manual_sync.notify_one();
            } else if is_quit_key(key) {
                shutdown.notify_waiters();
                return Ok(());
            } else {
                handle_scroll_key(key, &mut log_scroll);
            }
        }
    }
}

/// Which log entry the scroll viewport is anchored to. `Follow` tracks
/// the live tail (newest at the bottom). `Pinned` fixes a specific
/// entry -- identified by its monotonic push ordinal (`seq`) -- at a
/// given sub-row of the viewport's top, so the same content stays put
/// across resizes and new arrivals however the wrapping shifts. The
/// render re-derives the pixel offset from this every frame; the offset
/// itself is never stored.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Anchor {
    Follow,
    Pinned { seq: u64, row: usize },
}

/// One deferred scroll action. The run loop reads at most one key event
/// between draws, so at most one lands here per frame; `draw_log`
/// applies it once it knows the pane width the motion depends on.
#[derive(Clone, Copy)]
enum ScrollCmd {
    LineUp,
    LineDown,
    PageUp,
    PageDown,
    Home,
    End,
}

/// Render-owned scroll state for the log pane: where the viewport is
/// anchored plus a pending key action awaiting the next frame's
/// width-aware resolution.
struct LogScroll {
    anchor: Anchor,
    pending: Option<ScrollCmd>,
}

impl Default for LogScroll {
    fn default() -> Self {
        Self {
            anchor: Anchor::Follow,
            pending: None,
        }
    }
}

/// What a key press means while the help overlay is open. Ctrl-C
/// stays a hard quit -- it must work no matter what is on screen --
/// and every other press closes the overlay, so there is no way to
/// get stuck in it.
enum HelpKey {
    Quit,
    Close,
    Ignore,
}

fn help_overlay_key(key: KeyEvent) -> HelpKey {
    if key.kind != KeyEventKind::Press {
        return HelpKey::Ignore;
    }
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => HelpKey::Quit,
        _ => HelpKey::Close,
    }
}

fn is_help_key(key: KeyEvent) -> bool {
    key.kind == KeyEventKind::Press && key.code == KeyCode::Char('?')
}

fn is_sync_key(key: KeyEvent) -> bool {
    key.kind == KeyEventKind::Press && key.code == KeyCode::Char('s')
}

/// Record a cursor / paging key as a pending scroll action. No-ops on
/// any other key. The motion is resolved in *visible rows* by the next
/// `draw_log` (arrows move one row, PgUp/PgDn a full pane), which is the
/// only place the pane width -- and thus the wrapping -- is known.
fn handle_scroll_key(key: KeyEvent, scroll: &mut LogScroll) {
    if key.kind != KeyEventKind::Press {
        return;
    }
    let cmd = match key.code {
        KeyCode::Up => ScrollCmd::LineUp,
        KeyCode::Down => ScrollCmd::LineDown,
        KeyCode::PageUp => ScrollCmd::PageUp,
        KeyCode::PageDown => ScrollCmd::PageDown,
        KeyCode::Home => ScrollCmd::Home,
        KeyCode::End => ScrollCmd::End,
        _ => return,
    };
    scroll.pending = Some(cmd);
}

fn is_quit_key(key: KeyEvent) -> bool {
    if key.kind != KeyEventKind::Press {
        return false;
    }
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => true,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => true,
        _ => false,
    }
}

/// Ctrl-L, the conventional "redraw the screen" key. Forces a full
/// repaint on the next frame, recovering a display left corrupted by
/// stray output that reached the alternate screen.
fn is_redraw_key(key: KeyEvent) -> bool {
    key.kind == KeyEventKind::Press
        && key.code == KeyCode::Char('l')
        && key.modifiers.contains(KeyModifiers::CONTROL)
}

fn draw(frame: &mut ratatui::Frame<'_>, state: &TuiState, show_help: bool, scroll: &mut LogScroll) {
    let area = frame.area();
    // Four rows, top to bottom: a one-row version-banner header, the
    // metrics pane (half the remaining height), the log pane (Min(1),
    // so it absorbs the rest and never collapses), and a single-row
    // status bar that carries `notify!` milestones.
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Percentage(50),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(area);

    draw_header(frame, chunks[0]);
    draw_metrics(frame, chunks[1], state);
    draw_log(frame, chunks[2], state, scroll);
    draw_status_bar(frame, chunks[3], state);
    if show_help {
        draw_help_overlay(frame);
    }
}

/// Full-width header strip carrying the app name and the git-appended
/// JMA_VERSION, so the running build is identifiable at a glance when
/// troubleshooting -- most usefully on a local compile, where the
/// version carries the commit it was built from.
fn draw_header(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let style = Style::default()
        .bg(Color::Blue)
        .fg(Color::White)
        .add_modifier(Modifier::BOLD);
    let banner = Line::from(format!(" jma {} ", env!("JMA_VERSION")));
    frame.render_widget(
        Paragraph::new(banner)
            .style(style)
            .alignment(Alignment::Center),
        area,
    );
}

/// Centered overlay listing every key binding. The idle status hint
/// only fits a pointer here, and the first notify! milestone
/// displaces even that -- this overlay is the always-available
/// reference.
fn draw_help_overlay(frame: &mut ratatui::Frame<'_>) {
    let lines = vec![
        Line::from("  q / Esc      quit"),
        Line::from("  Ctrl-C       quit"),
        Line::from("  Ctrl-L       redraw the screen"),
        Line::from("  Up / Down    scroll log by line"),
        Line::from("  PgUp / PgDn  scroll log by screen"),
        Line::from("  Home         oldest buffered line"),
        Line::from("  End          back to live tail"),
        Line::from("  s            sync now"),
        Line::from("  ?            toggle this help"),
    ];
    // Size the box to its content plus borders; center it. Clear
    // erases whatever the frame drew underneath so the overlay
    // doesn't blend into the log pane.
    let area = centered_rect(frame.area(), 40, lines.len() as u16 + 2);
    frame.render_widget(Clear, area);
    let block = Block::default().borders(Borders::ALL).title(" Keys ");
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

/// Center a width x height box inside `area`, clamped to fit --
/// tiny terminals get whatever space exists rather than an
/// off-screen rect.
fn centered_rect(area: Rect, width: u16, height: u16) -> Rect {
    let w = width.min(area.width);
    let h = height.min(area.height);
    Rect {
        x: area.x + (area.width - w) / 2,
        y: area.y + (area.height - h) / 2,
        width: w,
        height: h,
    }
}

/// Width reserved for the Network pane. Sized to comfortably hold
/// the widest realistic line -- "  in     1.0 MB/s  (1.2 GB total)"
/// or the "  dl   12345/67890  (99%)" row -- with two cells of
/// border. Fixed rather than percentage so the Recent pane's width
/// doesn't jitter just because a rate string got a digit wider.
const NETWORK_PANE_WIDTH: u16 = 40;

/// Top half: metrics. Network on the left at a fixed width, Recent
/// on the right with everything else. Recent benefits the most from
/// extra horizontal room (Subject strings are the only variable-
/// width content) and the Network pane has a tight, bounded layout
/// that doesn't grow with the window.
fn draw_metrics(frame: &mut ratatui::Frame<'_>, area: Rect, state: &TuiState) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(NETWORK_PANE_WIDTH), Constraint::Min(0)])
        .split(area);
    draw_bandwidth(
        frame,
        cols[0],
        state.bandwidth(),
        state.download_progress(),
        state.download_eta(),
        state.last_cycle(),
    );
    draw_recent(frame, cols[1], state);
}

/// Network bandwidth: in (blob downloads), out (Email/import
/// uploads), and live download progress while `download_blobs` is
/// active. Each direction gets one line: a rolling-window rate
/// followed by the cumulative total in parens. The window is
/// `BW_WINDOW` -- short enough that the number tracks live
/// activity, long enough that a single multi-megabyte blob doesn't
/// produce a spike that reads as nonsense.
///
/// The download-progress row is conditional: it only renders while
/// the executor is emitting progress events (TuiState clears the
/// slot when the `download_blobs` phase closes). "  dl  12345/67890
/// (89%, ~18m)" reads naturally alongside the in/out rates -- it's
/// the same "what's on the wire" signal set. The status-bar phase
/// widget remains the source of truth for "what is sync doing";
/// this row adds the live counter and finish estimate that matter
/// while the wire is busy.
///
/// The dimmed "last" rows show the most recent completed cycle's
/// outcome with its duration and wall-clock time -- the single-slot
/// notify! status would otherwise overwrite it with the next
/// milestone.
///
/// Maildir-write bytes intentionally don't appear here. They're
/// disk-write throughput, not network bandwidth, and conflating them
/// into a single panel makes the in/out labels lie.
fn draw_bandwidth(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    bw: Bandwidth,
    progress: Option<Progress>,
    eta: Option<Duration>,
    last_cycle: Option<CycleSummary>,
) {
    let block = Block::default().borders(Borders::ALL).title(" Network ");
    let mut lines = vec![
        Line::from(vec![
            Span::raw("  in  "),
            Span::styled(
                format!("{:>10}/s", format_rate(bw.rate_in)),
                Style::default().fg(Color::Cyan),
            ),
            Span::styled(
                format!("  ({} total)", format_bytes(bw.total_in)),
                Style::default().add_modifier(Modifier::DIM),
            ),
        ]),
        Line::from(vec![
            Span::raw("  out "),
            Span::styled(
                format!("{:>10}/s", format_rate(bw.rate_out)),
                Style::default().fg(Color::Magenta),
            ),
            Span::styled(
                format!("  ({} total)", format_bytes(bw.total_out)),
                Style::default().add_modifier(Modifier::DIM),
            ),
        ]),
    ];
    if let Some(line) = dl_line(progress, eta) {
        lines.push(line);
    }
    if let Some(cycle) = last_cycle {
        lines.extend(cycle_lines(cycle));
    }
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

/// The Network pane's download-progress row, present only while a
/// download batch is in flight with a known total. The dim parens
/// carry the completed percentage and, when the download rate is
/// live, the finish estimate; the ETA is omitted while the
/// bandwidth window is empty or stale so a stalled transfer shows
/// no estimate rather than a frozen one.
fn dl_line(progress: Option<Progress>, eta: Option<Duration>) -> Option<Line<'static>> {
    let Progress { done, total } = progress?;
    if total == 0 {
        return None;
    }
    let pct = (done * 100 / total) as u32;
    let paren = match eta {
        Some(eta) => format!("  ({}%, {})", pct, format_eta(eta)),
        None => format!("  ({}%)", pct),
    };
    Some(Line::from(vec![
        Span::raw("  dl  "),
        Span::styled(
            format!("{:>10}", format!("{}/{}", done, total)),
            Style::default().fg(Color::Green),
        ),
        Span::styled(paren, Style::default().add_modifier(Modifier::DIM)),
    ]))
}

/// An ETA as a single coarse unit, always prefixed "~" -- it's an
/// estimate off a 5-second rate window, and false precision ("17m
/// 42s") would suggest a fidelity the sample doesn't have. Seconds
/// up to 89s, then whole minutes (rounded up -- an ETA that
/// undershoots reads as a stall when it passes), then tenths of an
/// hour.
fn format_eta(eta: Duration) -> String {
    let secs = eta.as_secs();
    if secs < 90 {
        format!("~{}s", secs.max(1))
    } else if secs < 90 * 60 {
        format!("~{}m", secs.div_ceil(60))
    } else {
        format!("~{:.1}h", eta.as_secs_f64() / 3600.0)
    }
}

/// The Network pane's "last" item: the most recent completed
/// cycle's outcome, dimmed -- it's context, not live activity. Two
/// rows, because the pane is a fixed 38 inner cells with no wrap
/// and a single row clips the tail on big-count cycles (an initial
/// sync's "150000 dn / 2000 up" plus duration plus timestamp
/// overflows it): the outcome rides the labeled row, the duration
/// and wall-clock time ride a continuation row indented to the
/// value column. In-sync cycles read "in sync" rather than "0 dn /
/// 0 up", which would look like a stall. The duration format
/// matches the status bar's phase widget ("(1.2s)"), so the two
/// readouts of the same cycle agree at a glance.
fn cycle_lines(cycle: CycleSummary) -> [Line<'static>; 2] {
    let text = if cycle.in_sync {
        "in sync".to_string()
    } else {
        format!("{} dn / {} up", cycle.downloaded, cycle.uploaded)
    };
    [
        Line::from(vec![
            Span::raw("  last"),
            Span::styled(
                format!("{:>12}", text),
                Style::default().add_modifier(Modifier::DIM),
            ),
        ]),
        Line::from(vec![
            Span::raw("      "),
            Span::styled(
                format!(
                    "({:.1}s)  {}",
                    cycle.wall.as_secs_f32(),
                    format_ts(cycle.at, &chrono::Local)
                ),
                Style::default().add_modifier(Modifier::DIM),
            ),
        ]),
    ]
}

/// Right column: the most recent messages that landed on disk,
/// as many as fit the pane. Newest at the top -- that's the row
/// the user's eye naturally goes to when a new message arrives,
/// and it matches the "latest first" feel of most mail UIs.
/// Subject is truncated to the inner width so a pathologically
/// long header doesn't push the timestamp off-screen.
fn draw_recent(frame: &mut ratatui::Frame<'_>, area: Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title(" Recent ");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let recent = state.recent(inner.height as usize);
    if recent.is_empty() {
        let hint = Paragraph::new(Line::from(Span::styled(
            "  (no messages synced yet)",
            Style::default().add_modifier(Modifier::DIM),
        )));
        frame.render_widget(hint, inner);
        return;
    }
    // Reserve a few characters for the timestamp and a separator
    // before truncating the subject. `subject_budget` could go
    // negative on absurdly narrow terminals -- clamp at 1 so we
    // always show at least the first char rather than nothing.
    let lines: Vec<Line> = recent
        .into_iter()
        .rev()
        .map(|m| {
            let ts = format_ts(m.at, &chrono::Local);
            let prefix_width = ts.len() + m.folder.len() + 4;
            let subject_budget = (inner.width as usize).saturating_sub(prefix_width).max(1);
            let subject = truncate_with_ellipsis(&m.subject, subject_budget);
            Line::from(vec![
                Span::styled(ts, Style::default().add_modifier(Modifier::DIM)),
                Span::raw(" "),
                Span::styled(m.folder, Style::default().fg(Color::Cyan)),
                Span::raw(" "),
                Span::raw(subject),
            ])
        })
        .collect();
    frame.render_widget(Paragraph::new(lines), inner);
}

/// Cap `s` at `max` display columns, appending an ellipsis if any
/// content had to be dropped. Counts unicode chars rather than
/// bytes -- close enough to a column estimate for ASCII-heavy
/// subjects, and avoids splitting a multi-byte char mid-sequence.
fn truncate_with_ellipsis(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        return s.to_string();
    }
    // Reserve one column for the ellipsis. If max < 2 we just emit
    // the leading char-or-two without a trailing marker.
    let keep = max.saturating_sub(1).max(1);
    let mut out: String = chars.into_iter().take(keep).collect();
    if max >= 2 {
        out.push('\u{2026}');
    }
    out
}

fn format_bytes(n: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if n >= GB {
        format!("{:.1} GB", n as f64 / GB as f64)
    } else if n >= MB {
        format!("{:.1} MB", n as f64 / MB as f64)
    } else if n >= KB {
        format!("{:.1} KB", n as f64 / KB as f64)
    } else {
        format!("{} B", n)
    }
}

fn format_rate(bps: f64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    if bps >= MB {
        format!("{:.1} MB", bps / MB)
    } else if bps >= KB {
        format!("{:.1} KB", bps / KB)
    } else {
        format!("{:.0} B", bps)
    }
}

/// Bottom half: log pane. Renders the lines visible at the current
/// scroll position; level gets a color hint, target is dimmed, message
/// takes the rest of the line. Title shows "[scrolled +N]" (visual rows
/// back from the live tail) when the user has scrolled away from the
/// bottom.
///
/// Scrolling is anchored to a log *entry*, not a pixel offset (see
/// [`Anchor`]). Each frame this resolves the anchor against the current
/// wrapping, applies at most one pending key action in visible-row
/// units, then re-derives the anchor from the resulting position. That
/// single "re-resolve" is what keeps a scrolled-back view pinned to the
/// same content across a terminal resize (which re-wraps every line) or
/// new arrivals at the tail (which shift entry indices) -- the pinned
/// entry's ordinal is stable even as its pixel row moves.
fn draw_log(frame: &mut ratatui::Frame<'_>, area: Rect, state: &TuiState, scroll: &mut LogScroll) {
    let inner_width = area.width.saturating_sub(2);
    let pane_rows = area.height.saturating_sub(2) as usize;
    let snapshot = state.log_snapshot();
    let len = snapshot.lines.len();

    // Build the display lines once. A following view needs only the
    // total wrapped height; only a scrolled-back (pinned) view needs
    // each entry's row span, so the per-entry prefix sums are computed
    // lazily below and a following frame skips them entirely.
    let rendered: Vec<Line> = snapshot
        .lines
        .iter()
        .cloned()
        .map(format_log_line)
        .collect();
    let paragraph = Paragraph::new(rendered).wrap(Wrap { trim: false });

    // Total wrapped height in one whole-buffer pass: same WordWrapper
    // and width as the render path, with no block yet so there are no
    // border rows to subtract.
    let total_rows = paragraph.line_count(inner_width);
    let max_top = total_rows.saturating_sub(pane_rows);
    // Push ordinal of the oldest still-buffered entry: entry i carries
    // ordinal `oldest_seq + i`.
    let oldest_seq = snapshot.pushed_total.saturating_sub(len as u64);

    // `starts[i]` is the first visual row of entry i and `starts[len]`
    // the total. Placing a pinned entry is the only thing that needs
    // it, so build it at most once, and only when the scroll position
    // actually touches an entry.
    let mut starts: Option<Vec<usize>> = None;

    // Resolve the anchor to a top visual-row index.
    let mut top = match scroll.anchor {
        Anchor::Follow => max_top,
        Anchor::Pinned { seq, row } => {
            if len == 0 || seq < oldest_seq {
                // The pinned entry has aged out of the ring; fall back
                // to the oldest surviving row.
                0
            } else {
                let s = starts.get_or_insert_with(|| entry_starts(&snapshot.lines, inner_width));
                let idx = ((seq - oldest_seq) as usize).min(len - 1);
                // A narrower pane may have shrunk this entry; clamp the
                // sub-row into its current height.
                let height = s[idx + 1] - s[idx];
                s[idx] + row.min(height.saturating_sub(1))
            }
        }
    };

    // Apply the one pending key action (the run loop reads a single
    // event between draws). Everything is in visible rows.
    let mut follow = matches!(scroll.anchor, Anchor::Follow);
    match scroll.pending.take() {
        Some(ScrollCmd::LineUp) => {
            top = top.saturating_sub(1);
            follow = false;
        }
        Some(ScrollCmd::PageUp) => {
            top = top.saturating_sub(pane_rows);
            follow = false;
        }
        Some(ScrollCmd::LineDown) => {
            top += 1;
            follow = top >= max_top;
        }
        Some(ScrollCmd::PageDown) => {
            top += pane_rows;
            follow = top >= max_top;
        }
        Some(ScrollCmd::Home) => {
            top = 0;
            follow = false;
        }
        Some(ScrollCmd::End) => follow = true,
        None => {}
    }
    top = top.min(max_top);

    // Persist the anchor for the next frame. Reaching the tail (or End)
    // re-enables follow; otherwise pin to the entry now at the viewport
    // top so resize and new arrivals hold it in place.
    scroll.anchor = if follow || total_rows == 0 {
        Anchor::Follow
    } else {
        let s = starts.get_or_insert_with(|| entry_starts(&snapshot.lines, inner_width));
        let idx = line_at_row(s, top).min(len.saturating_sub(1));
        Anchor::Pinned {
            seq: oldest_seq + idx as u64,
            row: top - s[idx],
        }
    };

    let scrolled = max_top - top;
    let title = if scrolled > 0 {
        format!(" Log [scrolled +{}] ", scrolled)
    } else {
        " Log ".to_string()
    };
    let paragraph = paragraph
        .block(Block::default().borders(Borders::ALL).title(title))
        .scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
    frame.render_widget(paragraph, area);
}

/// Per-entry visual-row prefix sums at `width`: `starts[i]` is the
/// first row of entry i and `starts[len]` the total wrapped height.
/// Only a scrolled-back view needs these, so this stays off the common
/// path. Formats and wraps each entry with the same WordWrapper the
/// render uses; WordWrapper never merges two logical lines, so entry
/// spans are independent and this matches what ratatui draws.
fn entry_starts(lines: &[LogLine], width: u16) -> Vec<usize> {
    let mut starts = Vec::with_capacity(lines.len() + 1);
    let mut acc = 0usize;
    for line in lines {
        starts.push(acc);
        acc += Paragraph::new(format_log_line(line.clone()))
            .wrap(Wrap { trim: false })
            .line_count(width);
    }
    starts.push(acc);
    starts
}

/// Index of the entry whose visual-row span contains `row`. `starts` is
/// ascending with a trailing total-rows sentinel, so the containing
/// entry is one before the first start that exceeds `row`.
fn line_at_row(starts: &[usize], row: usize) -> usize {
    starts.partition_point(|&s| s <= row).saturating_sub(1)
}

/// Single-row status bar at the very bottom. Left half carries the
/// most recent `notify!` milestone (or the quit hint when none has
/// been emitted); right half carries the current sync phase, with
/// progress and elapsed appended when applicable. The two halves
/// share the same blue/white styling so the bar reads as one strip.
///
/// One line is all the phase needs -- name, optional download
/// counter, elapsed-so-far -- so it shares the bar instead of
/// spending a metrics tile. Idle cycles show a dimmed "last: X
/// (1.2s)" so the corner is never empty.
fn draw_status_bar(frame: &mut ratatui::Frame<'_>, area: Rect, state: &TuiState) {
    let style = Style::default()
        .bg(Color::Blue)
        .fg(Color::White)
        .add_modifier(Modifier::BOLD);

    let halves = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(area);

    // A one-cell gap in the bar's own background separates the two
    // badges -- their background colors are what delimit them, so
    // no bracketing glyphs are needed.
    let health = state.conn_health();
    let mut left_spans = vec![jmap_badge(health), Span::raw(" "), fs_badge(health)];
    match state.status() {
        Some(Status { at, message }) => {
            left_spans.push(Span::raw(format!(" [{}] ", format_ts(at, &chrono::Local))));
            left_spans.push(Span::raw(message));
        }
        None => {
            left_spans.push(Span::raw(" jma watch -- ? for keys, q quits "));
        }
    }
    frame.render_widget(
        Paragraph::new(Line::from(left_spans))
            .style(style)
            .alignment(Alignment::Left),
        halves[0],
    );

    let right = phase_status_line(
        state.current_phase(),
        state.last_phase(),
        state.download_progress(),
    );
    frame.render_widget(
        Paragraph::new(right)
            .style(style)
            .alignment(Alignment::Right),
        halves[1],
    );
}

/// Build the right-side phase string for the status bar. Active
/// phase wins: shows name, progress (when download_blobs is the
/// active phase and a total is known), and elapsed-so-far. Idle
/// falls back to a dimmed "last: X (1.2s)" so the corner is never
/// just blue padding. If no phase has ever run, returns an empty
/// line and the bar's blue background carries the space.
fn phase_status_line(
    current: Option<ActivePhase>,
    last: Option<CompletedPhase>,
    progress: Option<Progress>,
) -> Line<'static> {
    if let Some(p) = current {
        let mut spans: Vec<Span<'static>> = vec![Span::raw(p.name.clone())];
        if p.name == "download_blobs"
            && let Some(Progress { done, total }) = progress
            && total > 0
        {
            spans.push(Span::raw(format!(" {}/{}", done, total)));
        }
        spans.push(Span::raw(format!(
            " ({:.1}s) ",
            p.started.elapsed().as_secs_f32()
        )));
        return Line::from(spans);
    }
    if let Some(p) = last {
        return Line::from(Span::styled(
            format!("last: {} ({:.1}s) ", p.name, p.elapsed.as_secs_f32()),
            Style::default().add_modifier(Modifier::DIM),
        ));
    }
    Line::from("")
}

/// Label text of the two health badges, padded to one shared width:
/// uneven badges read as clutter, and every text the jmap badge can
/// show -- label or reconnect timer -- renders at this same width,
/// so the status bar's left edge never reflows on a state
/// transition.
const JMAP_BADGE: &str = " jmap ";
const FS_BADGE: &str = "  fs  ";

/// Health badge for the JMAP side, folding the engine (session) and
/// SSE (push) channels into one color: red when the engine is
/// reconnecting (sync itself is down), yellow when only the push
/// channel is retrying (remote triggers stall but sync still works),
/// green when connected, dim gray before the first report. While a
/// channel is reconnecting, its backoff replaces the label -- a
/// yellow "4s" reads as "push retries in 4s" -- centered into the
/// same width so the badge doesn't resize.
fn jmap_badge(health: ConnHealth) -> Span<'static> {
    let red = Style::default().bg(Color::Red).fg(Color::White);
    let yellow = Style::default().bg(Color::Yellow).fg(Color::Black);
    let (text, style) = match (health.engine, health.sse) {
        (Some(ConnState::Reconnecting { backoff }), _) => (backoff_text(backoff), red),
        (Some(ConnState::Down), _) => (JMAP_BADGE.to_string(), red),
        (_, Some(ConnState::Reconnecting { backoff })) => (backoff_text(backoff), yellow),
        (_, Some(ConnState::Down)) => (JMAP_BADGE.to_string(), yellow),
        (None, None) => (
            JMAP_BADGE.to_string(),
            Style::default().bg(Color::DarkGray).fg(Color::White),
        ),
        _ => (
            JMAP_BADGE.to_string(),
            Style::default().bg(Color::Green).fg(Color::Black),
        ),
    };
    Span::styled(text, style)
}

/// A reconnect backoff centered into the badge width, e.g.
/// "  4s  ". The daemon caps backoff at 60s (RECONNECT_MAX_BACKOFF),
/// so the text always fits and the badge never resizes.
fn backoff_text(backoff: std::time::Duration) -> String {
    format!(
        "{:^width$}",
        format!("{}s", backoff.as_secs()),
        width = JMAP_BADGE.len()
    )
}

/// Health badge for the filesystem watcher: green while it runs,
/// red once it has exited (the task is one-shot -- local changes
/// stop syncing until the daemon restarts, so the red never
/// clears), dim gray before the first report. Always shows its
/// label; there is no retry state to time.
fn fs_badge(health: ConnHealth) -> Span<'static> {
    let style = match health.watcher {
        None => Style::default().bg(Color::DarkGray).fg(Color::White),
        Some(ConnState::Connected) => Style::default().bg(Color::Green).fg(Color::Black),
        Some(ConnState::Reconnecting { .. }) | Some(ConnState::Down) => {
            Style::default().bg(Color::Red).fg(Color::White)
        }
    };
    Span::styled(FS_BADGE, style)
}

/// Formats a captured-UTC timestamp as a time-of-day string in `tz`.
/// The watch TUI stores timestamps in UTC (see `TuiLayer::on_event`
/// for the log pane, `TuiState` for the metric panels) but renders
/// them in the machine's local zone so the times match the wall clock
/// of whoever is watching. Non-interactive stderr output stays UTC.
fn format_ts<Tz: chrono::TimeZone>(ts: chrono::DateTime<chrono::Utc>, tz: &Tz) -> String
where
    // chrono's DelayedFormat requires the offset be Display even though
    // "%H:%M:%S" never prints it -- the bound is chrono's, not ours.
    Tz::Offset: std::fmt::Display,
{
    ts.with_timezone(tz).format("%H:%M:%S").to_string()
}

fn format_log_line(line: LogLine) -> Line<'static> {
    let level_style = match line.level {
        tracing::Level::ERROR => Style::default().fg(Color::Red),
        tracing::Level::WARN => Style::default().fg(Color::Yellow),
        tracing::Level::INFO => Style::default().fg(Color::Green),
        _ => Style::default().fg(Color::DarkGray),
    };
    let ts = format_ts(line.ts, &chrono::Local);
    Line::from(vec![
        Span::styled(ts, Style::default().add_modifier(Modifier::DIM)),
        Span::raw(" "),
        Span::styled(format!("{:5}", line.level), level_style),
        Span::raw(" "),
        Span::styled(line.target, Style::default().add_modifier(Modifier::DIM)),
        Span::raw(" "),
        Span::raw(line.message),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TUI timestamps are captured in UTC but rendered in a wall-clock
    /// zone; the conversion must land on the right time-of-day even when
    /// the offset rolls the date across midnight. Fixed offsets keep the
    /// assertion independent of the test host's own zone -- the render
    /// path itself passes `chrono::Local`.
    #[test]
    fn format_ts_renders_time_of_day_in_the_given_zone() {
        use chrono::{FixedOffset, TimeZone, Utc};
        let ts = Utc.with_ymd_and_hms(2026, 7, 9, 23, 30, 0).unwrap();
        // +08:00 rolls past midnight into the next day; the panes show
        // time-of-day only, so the date roll is invisible.
        assert_eq!(
            format_ts(ts, &FixedOffset::east_opt(8 * 3600).unwrap()),
            "07:30:00"
        );
        // -05:00 stays on the same day.
        assert_eq!(
            format_ts(ts, &FixedOffset::west_opt(5 * 3600).unwrap()),
            "18:30:00"
        );
        // UTC is unchanged -- the non-interactive default.
        assert_eq!(format_ts(ts, &Utc), "23:30:00");
    }

    /// Dropping the guard lowers the active flag no matter how `run`
    /// exits, so the stderr/stdout gating window cannot outlive the
    /// display -- a panic path that left it raised would swallow the
    /// very error output the user needs to read. The terminal-restore
    /// side is not assertable off a tty; those writes are best-effort
    /// no-ops here.
    #[test]
    fn terminal_guard_drop_lowers_active_flag() {
        crate::tui::mark_active(true);
        drop(TerminalGuard);
        assert!(!crate::tui::is_active());
    }

    /// While the overlay is open, Ctrl-C still hard-quits and any
    /// other press closes it -- there is no way to get stuck in the
    /// overlay. Non-press events are ignored so key release can't
    /// close it the instant it opens.
    #[test]
    fn help_overlay_key_routes_quit_close_ignore() {
        assert!(is_help_key(KeyEvent::new(
            KeyCode::Char('?'),
            KeyModifiers::NONE
        )));
        assert!(is_sync_key(KeyEvent::new(
            KeyCode::Char('s'),
            KeyModifiers::NONE
        )));
        let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert!(matches!(help_overlay_key(ctrl_c), HelpKey::Quit));
        let q = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
        assert!(matches!(help_overlay_key(q), HelpKey::Close));
        let mut release = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE);
        release.kind = KeyEventKind::Release;
        assert!(matches!(help_overlay_key(release), HelpKey::Ignore));
    }

    /// Ctrl-L is the redraw key: only the Ctrl-modified press counts,
    /// so a bare `l` or a key release never forces a repaint.
    #[test]
    fn redraw_key_is_ctrl_l_on_press() {
        let ctrl_l = KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL);
        assert!(is_redraw_key(ctrl_l));
        assert!(!is_redraw_key(KeyEvent::new(
            KeyCode::Char('l'),
            KeyModifiers::NONE
        )));
        let mut release = KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL);
        release.kind = KeyEventKind::Release;
        assert!(!is_redraw_key(release));
    }

    /// The overlay box centers inside large areas and clamps to tiny
    /// ones instead of landing off-screen.
    #[test]
    fn centered_rect_centers_and_clamps() {
        let big = centered_rect(Rect::new(0, 0, 100, 40), 40, 9);
        assert_eq!((big.x, big.y, big.width, big.height), (30, 15, 40, 9));
        let tiny = centered_rect(Rect::new(0, 0, 10, 4), 40, 9);
        assert_eq!((tiny.x, tiny.y, tiny.width, tiny.height), (0, 0, 10, 4));
    }

    /// Flatten one rendered line to its text -- helper for the
    /// cycle-lines tests.
    fn flat(line: &Line<'_>) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    /// The dl row carries the ETA inside its dim parens only when
    /// one is available; with no live rate the row falls back to the
    /// bare percentage, and with no progress at all there is no row.
    #[test]
    fn dl_line_includes_eta_only_when_available() {
        let p = Some(Progress {
            done: 40,
            total: 100,
        });
        let with = flat(&dl_line(p, Some(Duration::from_secs(30))).unwrap());
        assert!(with.contains("40/100"));
        assert!(with.contains("(40%, ~30s)"));

        let without = flat(&dl_line(p, None).unwrap());
        assert!(without.contains("(40%)"));

        assert!(dl_line(None, None).is_none());
        assert!(dl_line(Some(Progress { done: 0, total: 0 }), None).is_none());
    }

    /// The ETA renders as one coarse unit: seconds below 90s (never
    /// "~0s" -- a sub-second remainder still reads "~1s"), whole
    /// minutes rounded up below 90 minutes, tenths of an hour past
    /// that.
    #[test]
    fn format_eta_scales_units() {
        assert_eq!(format_eta(Duration::from_millis(200)), "~1s");
        assert_eq!(format_eta(Duration::from_secs(45)), "~45s");
        assert_eq!(format_eta(Duration::from_secs(89)), "~89s");
        assert_eq!(format_eta(Duration::from_secs(90)), "~2m");
        assert_eq!(format_eta(Duration::from_secs(121)), "~3m");
        assert_eq!(format_eta(Duration::from_secs(18 * 60)), "~18m");
        assert_eq!(format_eta(Duration::from_secs(89 * 60 + 59)), "~90m");
        assert_eq!(format_eta(Duration::from_secs(90 * 60)), "~1.5h");
        assert_eq!(format_eta(Duration::from_secs(2 * 3600 + 360)), "~2.1h");
    }

    /// The "last" item reads "in sync" for no-op cycles -- zero
    /// counts would look like a stall -- and the real counts
    /// otherwise; the continuation row carries the duration and
    /// wall-clock time.
    #[test]
    fn cycle_lines_format_in_sync_and_counts() {
        let at = chrono::Utc::now();
        let [top, bottom] = cycle_lines(CycleSummary {
            at,
            downloaded: 0,
            uploaded: 0,
            in_sync: true,
            wall: std::time::Duration::from_millis(300),
        });
        assert!(flat(&top).contains("in sync"));
        assert!(flat(&bottom).contains("(0.3s)"));

        let [top, bottom] = cycle_lines(CycleSummary {
            at,
            downloaded: 12,
            uploaded: 3,
            in_sync: false,
            wall: std::time::Duration::from_millis(1_200),
        });
        assert!(flat(&top).contains("12 dn / 3 up"));
        assert!(flat(&bottom).contains("(1.2s)"));
    }

    /// Both rows of the "last" item fit the Network pane's fixed
    /// inner width for a big-count cycle -- an initial sync's counts
    /// plus duration plus timestamp is exactly the shape that
    /// overflowed a single row, and the pane's Paragraph does not
    /// wrap, so an overflow silently clips the tail.
    #[test]
    fn cycle_lines_fit_the_pane_for_big_count_cycles() {
        let at = chrono::Utc::now();
        let [top, bottom] = cycle_lines(CycleSummary {
            at,
            downloaded: 150_000,
            uploaded: 2_000,
            in_sync: false,
            wall: std::time::Duration::from_secs(3_600),
        });
        let (top, bottom) = (flat(&top), flat(&bottom));
        assert!(top.contains("150000 dn / 2000 up"));
        assert!(bottom.contains("(3600.0s)"));
        // All-ASCII rows, so byte length equals display width.
        let inner = (NETWORK_PANE_WIDTH - 2) as usize;
        assert!(
            top.len() <= inner,
            "top row {} > {inner}: {top:?}",
            top.len()
        );
        assert!(
            bottom.len() <= inner,
            "bottom row {} > {inner}: {bottom:?}",
            bottom.len()
        );
    }

    /// Build a ConnHealth snapshot from per-channel states -- helper
    /// for the badge tests.
    fn health(
        engine: Option<ConnState>,
        sse: Option<ConnState>,
        watcher: Option<ConnState>,
    ) -> ConnHealth {
        ConnHealth {
            engine,
            sse,
            watcher,
        }
    }

    /// The jmap badge folds engine + SSE into one color: engine
    /// degradation (sync down) outranks push degradation (sync still
    /// works), connected is green, and no reports read as startup
    /// gray. While a channel reconnects, the winning channel's
    /// backoff replaces the label; every text renders at the label's
    /// width so the bar never reflows on a transition.
    #[test]
    fn jmap_badge_colors_by_channel_state() {
        use std::time::Duration;
        let retry_4s = ConnState::Reconnecting {
            backoff: Duration::from_secs(4),
        };
        let retry_2s = ConnState::Reconnecting {
            backoff: Duration::from_secs(2),
        };

        let engine_down = jmap_badge(health(Some(retry_4s), Some(ConnState::Connected), None));
        assert_eq!(engine_down.content, "  4s  ");
        assert_eq!(engine_down.style.bg, Some(Color::Red));

        // Engine degradation wins over push degradation when both
        // report at once -- the displayed timer is the engine's.
        let both = jmap_badge(health(Some(retry_4s), Some(retry_2s), None));
        assert_eq!(both.content, "  4s  ");
        assert_eq!(both.style.bg, Some(Color::Red));

        let push_only = jmap_badge(health(Some(ConnState::Connected), Some(retry_2s), None));
        assert_eq!(push_only.content, "  2s  ");
        assert_eq!(push_only.style.bg, Some(Color::Yellow));

        let healthy = jmap_badge(health(
            Some(ConnState::Connected),
            Some(ConnState::Connected),
            None,
        ));
        assert_eq!(healthy.content, JMAP_BADGE);
        assert_eq!(healthy.style.bg, Some(Color::Green));

        let unreported = jmap_badge(health(None, None, None));
        assert_eq!(unreported.content, JMAP_BADGE);
        assert_eq!(unreported.style.bg, Some(Color::DarkGray));

        // Width stability is the badge's layout contract: label and
        // timer render at exactly the same width, including the
        // longest possible timer (the 60s backoff cap).
        let capped = jmap_badge(health(
            Some(ConnState::Reconnecting {
                backoff: Duration::from_secs(60),
            }),
            None,
            None,
        ));
        assert_eq!(capped.content.len(), JMAP_BADGE.len());
        assert_eq!(engine_down.content.len(), JMAP_BADGE.len());
    }

    /// The fs badge tracks the watcher channel alone: green while
    /// running, red once down (terminal -- the watcher task is
    /// one-shot), gray before the first report. The watcher's state
    /// must not bleed into the jmap badge or vice versa: each badge
    /// reads exactly one failure domain.
    #[test]
    fn fs_badge_colors_by_watcher_state() {
        // The two badges share one width -- uneven badges read as
        // clutter, and the labels are consts, so pin it here.
        assert_eq!(JMAP_BADGE.len(), FS_BADGE.len());

        let unreported = fs_badge(health(None, None, None));
        assert_eq!(unreported.content, FS_BADGE);
        assert_eq!(unreported.style.bg, Some(Color::DarkGray));

        let running = fs_badge(health(None, None, Some(ConnState::Connected)));
        assert_eq!(running.style.bg, Some(Color::Green));

        let dead = fs_badge(health(None, None, Some(ConnState::Down)));
        assert_eq!(dead.style.bg, Some(Color::Red));

        // A dead watcher leaves the jmap badge alone, and a degraded
        // engine leaves the fs badge alone.
        let jmap = jmap_badge(health(
            Some(ConnState::Connected),
            Some(ConnState::Connected),
            Some(ConnState::Down),
        ));
        assert_eq!(jmap.style.bg, Some(Color::Green));
        let fs = fs_badge(health(
            Some(ConnState::Down),
            None,
            Some(ConnState::Connected),
        ));
        assert_eq!(fs.style.bg, Some(Color::Green));
    }

    /// Strings within budget pass through untouched -- no gratuitous
    /// ellipsis on content that already fits.
    #[test]
    fn truncate_keeps_fitting_strings_intact() {
        assert_eq!(truncate_with_ellipsis("hello", 5), "hello");
        assert_eq!(truncate_with_ellipsis("hello", 10), "hello");
    }

    /// Over-budget strings keep max-1 chars plus the ellipsis, and
    /// the count is chars, not bytes -- a multi-byte subject must
    /// never be split mid-codepoint.
    #[test]
    fn truncate_counts_chars_not_bytes() {
        assert_eq!(truncate_with_ellipsis("hello world", 6), "hello\u{2026}");
        assert_eq!(
            truncate_with_ellipsis("\u{e9}\u{e9}\u{e9}\u{e9}", 3),
            "\u{e9}\u{e9}\u{2026}"
        );
    }

    /// Degenerate widths: 0 renders nothing, 1 renders the first
    /// char without a marker (an ellipsis alone carries no signal).
    #[test]
    fn truncate_degenerate_widths() {
        assert_eq!(truncate_with_ellipsis("hello", 0), "");
        assert_eq!(truncate_with_ellipsis("hello", 1), "h");
    }

    /// Push `n` lines whose messages are long enough to wrap at any
    /// realistic pane width -- helper for the scroll tests. Entry `i`
    /// carries the marker `L{i:02}`.
    fn push_wrapping(state: &TuiState, n: usize) {
        for i in 0..n {
            state.push_log(LogLine {
                ts: chrono::Utc::now(),
                level: tracing::Level::INFO,
                target: "t".into(),
                message: format!("L{:02}-{}", i, "x".repeat(40)),
            });
        }
    }

    /// The inner rows of a bordered pane (borders stripped), as strings.
    fn inner_rows(
        terminal: &Terminal<ratatui::backend::TestBackend>,
        w: u16,
        h: u16,
    ) -> Vec<String> {
        let buf = terminal.backend().buffer();
        (1..h - 1)
            .map(|y| (1..w - 1).map(|x| buf[(x, y)].symbol()).collect::<String>())
            .collect()
    }

    /// Render the log pane into a fresh `w`x`h` terminal, applying any
    /// pending scroll action, and return its inner rows.
    fn render_scroll(state: &TuiState, scroll: &mut LogScroll, w: u16, h: u16) -> Vec<String> {
        use ratatui::backend::TestBackend;
        let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
        terminal
            .draw(|frame| {
                let area = frame.area();
                draw_log(frame, area, state, scroll);
            })
            .unwrap();
        inner_rows(&terminal, w, h)
    }

    /// A following view keeps the newest line on screen even when lines
    /// wrap past the pane height: the entries wrap top-down and would
    /// otherwise clip the tail, hiding exactly the lines the user most
    /// wants to see. Six lines wider than the pane overflow it, so a
    /// naive top-anchored render would show the oldest and drop the
    /// newest; the anchor pins the tail instead.
    #[test]
    fn following_view_keeps_newest_line_visible_when_lines_wrap() {
        let state = TuiState::new();
        push_wrapping(&state, 6);

        // Inner area is 28 wide x 6 tall; six wrapping lines overflow it.
        let mut scroll = LogScroll::default();
        let rendered = render_scroll(&state, &mut scroll, 30, 8).join("\n");

        assert!(
            rendered.contains("L05"),
            "newest line was clipped off the bottom: {rendered:?}"
        );
        assert!(
            !rendered.contains("L00"),
            "oldest line should have scrolled off the top: {rendered:?}"
        );
    }

    /// Scrolling back one row moves the viewport by a single *visual*
    /// row, not a whole logical entry: the new view is the old one
    /// shifted down by exactly one row, with one older row revealed at
    /// the top. A logical-line scroll would jump a full wrapped entry
    /// (several rows) and this equality would not hold.
    #[test]
    fn scrolling_back_one_row_shifts_a_single_visual_row() {
        let (w, h) = (24, 10); // inner 22 x 8; L-lines wrap to ~3 rows
        let state = TuiState::new();
        push_wrapping(&state, 12);

        let mut scroll = LogScroll::default();
        let at_tail = render_scroll(&state, &mut scroll, w, h);
        scroll.pending = Some(ScrollCmd::LineUp);
        let scrolled = render_scroll(&state, &mut scroll, w, h);

        // One row of older content appeared at the top, and everything
        // else slid down by one row.
        let pane_rows = (h - 2) as usize;
        assert_eq!(
            scrolled[1..pane_rows],
            at_tail[0..pane_rows - 1],
            "a one-row scroll should shift the view by exactly one visual row"
        );
        assert_ne!(
            scrolled[0], at_tail[0],
            "a new older row should be revealed at the top"
        );
    }

    /// A page press moves exactly one screenful of visible rows: one
    /// PageUp lands on the same content as `pane_rows` single-row steps.
    /// This is the property the original logical-line paging broke.
    #[test]
    fn page_scroll_equals_a_screenful_of_line_scrolls() {
        let (w, h) = (24, 12);
        let pane_rows = (h - 2) as usize;
        let state = TuiState::new();
        push_wrapping(&state, 20);

        let mut paged = LogScroll::default();
        let _ = render_scroll(&state, &mut paged, w, h);
        paged.pending = Some(ScrollCmd::PageUp);
        let paged_rows = render_scroll(&state, &mut paged, w, h);

        let mut stepped = LogScroll::default();
        let mut stepped_rows = render_scroll(&state, &mut stepped, w, h);
        for _ in 0..pane_rows {
            stepped.pending = Some(ScrollCmd::LineUp);
            stepped_rows = render_scroll(&state, &mut stepped, w, h);
        }

        assert_eq!(
            paged_rows, stepped_rows,
            "PageUp should move exactly one pane of visible rows"
        );
    }

    /// While scrolled back, lines arriving at the tail must not drag the
    /// pinned content off its spot. The anchor names a specific entry,
    /// so re-resolving it after new pushes leaves the same rows on
    /// screen -- no width-dependent bookkeeping at push time.
    #[test]
    fn scrolled_back_view_is_stable_across_appends() {
        let (w, h) = (24, 10);
        let state = TuiState::new();
        push_wrapping(&state, 12);

        let mut scroll = LogScroll::default();
        let _ = render_scroll(&state, &mut scroll, w, h);
        scroll.pending = Some(ScrollCmd::PageUp);
        let before = render_scroll(&state, &mut scroll, w, h);

        // New lines land at the tail; the pinned view must not move.
        push_wrapping(&state, 3);
        let after = render_scroll(&state, &mut scroll, w, h);

        assert_eq!(
            before, after,
            "appends at the tail dragged a scrolled-back view off its spot"
        );
    }

    /// The heart of the resize fix: a pinned entry stays put when the
    /// terminal is resized, whether the new width wraps its lines more
    /// or less. Home pins the oldest entry; widening (fewer wraps) and
    /// narrowing (more wraps) both keep that same entry -- identified by
    /// its stable push ordinal -- at the top of the pane.
    #[test]
    fn resize_keeps_the_pinned_entry_in_place() {
        let state = TuiState::new();
        push_wrapping(&state, 20);

        let mut scroll = LogScroll::default();
        let _ = render_scroll(&state, &mut scroll, 30, 12);
        scroll.pending = Some(ScrollCmd::Home);

        // Pin the oldest entry (ordinal 0) at the top.
        for (w, note) in [(30u16, "initial"), (60, "wider"), (20, "narrower")] {
            let pane = render_scroll(&state, &mut scroll, w, 12).join("\n");
            assert_eq!(
                scroll.anchor,
                Anchor::Pinned { seq: 0, row: 0 },
                "resize to {note} lost the pinned entry"
            );
            assert!(
                pane.contains("L00"),
                "pinned oldest entry vanished after resize to {note}: {pane:?}"
            );
            assert!(
                !pane.contains("L19"),
                "newest entry should stay off-screen after resize to {note}: {pane:?}"
            );
        }
    }

    /// Widening can shrink the pinned entry to fewer rows than the
    /// sub-row the viewport was resting on. The sub-row must clamp into
    /// the entry's new height so the anchor stays on that entry rather
    /// than sliding onto the next one.
    #[test]
    fn resize_wider_keeps_a_mid_entry_pin_from_sliding() {
        let state = TuiState::new();
        push_wrapping(&state, 20);

        let mut scroll = LogScroll::default();
        let _ = render_scroll(&state, &mut scroll, 24, 12);
        // Pin the oldest entry, then step one row into it: at width 24
        // it wraps to several rows, so the sub-row is non-zero.
        scroll.pending = Some(ScrollCmd::Home);
        let _ = render_scroll(&state, &mut scroll, 24, 12);
        scroll.pending = Some(ScrollCmd::LineDown);
        let _ = render_scroll(&state, &mut scroll, 24, 12);
        let pinned_seq = match scroll.anchor {
            Anchor::Pinned { seq, row } => {
                assert!(row > 0, "expected a non-zero sub-row to exercise the clamp");
                seq
            }
            Anchor::Follow => panic!("expected a pinned anchor"),
        };

        // Widen so the pinned entry collapses to a single row (< the
        // sub-row). The anchor must stay on the same entry.
        let wide = render_scroll(&state, &mut scroll, 70, 12).join("\n");
        assert!(
            matches!(scroll.anchor, Anchor::Pinned { seq, .. } if seq == pinned_seq),
            "widening slid the anchor off its entry: {:?}",
            scroll.anchor
        );
        assert!(wide.contains(&format!("L{pinned_seq:02}")));
    }

    /// A following view stays glued to the tail across a resize: the
    /// newest line is on screen at every width, no matter how the
    /// wrapping changes.
    #[test]
    fn following_view_tracks_tail_across_resize() {
        let state = TuiState::new();
        push_wrapping(&state, 20);

        let mut scroll = LogScroll::default();
        for w in [30u16, 60, 20] {
            let pane = render_scroll(&state, &mut scroll, w, 12).join("\n");
            assert_eq!(scroll.anchor, Anchor::Follow);
            assert!(
                pane.contains("L19"),
                "following view lost the tail at width {w}: {pane:?}"
            );
        }
    }

    /// Each cursor / paging key records the matching pending action;
    /// unrelated keys leave the scroll state alone.
    #[test]
    fn scroll_keys_map_to_pending_commands() {
        let press = |code| KeyEvent::new(code, KeyModifiers::NONE);
        let mut scroll = LogScroll::default();

        handle_scroll_key(press(KeyCode::Up), &mut scroll);
        assert!(matches!(scroll.pending, Some(ScrollCmd::LineUp)));
        handle_scroll_key(press(KeyCode::Down), &mut scroll);
        assert!(matches!(scroll.pending, Some(ScrollCmd::LineDown)));
        handle_scroll_key(press(KeyCode::PageUp), &mut scroll);
        assert!(matches!(scroll.pending, Some(ScrollCmd::PageUp)));
        handle_scroll_key(press(KeyCode::PageDown), &mut scroll);
        assert!(matches!(scroll.pending, Some(ScrollCmd::PageDown)));
        handle_scroll_key(press(KeyCode::Home), &mut scroll);
        assert!(matches!(scroll.pending, Some(ScrollCmd::Home)));
        handle_scroll_key(press(KeyCode::End), &mut scroll);
        assert!(matches!(scroll.pending, Some(ScrollCmd::End)));

        // An unrelated key doesn't disturb the pending action.
        handle_scroll_key(press(KeyCode::Char('x')), &mut scroll);
        assert!(matches!(scroll.pending, Some(ScrollCmd::End)));
    }
}
