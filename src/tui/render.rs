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
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
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
pub fn run(state: Arc<TuiState>, shutdown: Arc<Notify>) -> Result<()> {
    enable_raw_mode().context("enable raw mode")?;
    // Armed before any further fallible step: from here, every exit
    // from this function -- normal return, a setup error below, or
    // a panic unwinding out of the render loop -- restores the
    // terminal. Raw mode or the alternate screen outliving the
    // process would leave the user's shell unusable until `reset`.
    let _guard = TerminalGuard;

    let mut terminal = setup_terminal().context("Failed to initialize TUI terminal")?;
    crate::tui::mark_active(true);

    let result = run_loop(&mut terminal, state, &shutdown);

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

fn run_loop(terminal: &mut Tui, state: Arc<TuiState>, shutdown: &Arc<Notify>) -> Result<()> {
    // `last_log_height` is written by draw_log on each render and
    // read by the scroll-key handler so PgUp/PgDn jumps match the
    // visible-row count. A 1-row default keeps any initial key
    // press before the first draw from dividing by zero -- the
    // first frame overwrites it immediately.
    let mut last_log_height: u16 = 1;
    loop {
        terminal
            .draw(|frame| {
                last_log_height = draw(frame, &state);
            })
            .context("draw frame")?;

        if event::poll(TICK).context("poll for terminal event")?
            && let CrosstermEvent::Key(key) = event::read().context("read terminal event")?
        {
            if is_quit_key(key) {
                shutdown.notify_waiters();
                return Ok(());
            }
            handle_scroll_key(key, &state, last_log_height);
        }
    }
}

/// Map cursor / paging keys to log-pane scroll actions. No-ops on
/// any other key. `pane_height` is the most recently observed log-
/// pane height -- PgUp/PgDn use it for one-screen jumps so the
/// motion matches what the user can see.
fn handle_scroll_key(key: KeyEvent, state: &TuiState, pane_height: u16) {
    if key.kind != KeyEventKind::Press {
        return;
    }
    let h = pane_height.max(1) as usize;
    match key.code {
        KeyCode::Up => state.scroll_log_back(1, h),
        KeyCode::Down => state.scroll_log_forward(1),
        KeyCode::PageUp => state.scroll_log_back(h, h),
        KeyCode::PageDown => state.scroll_log_forward(h),
        KeyCode::Home => state.scroll_log_to_top(h),
        KeyCode::End => state.scroll_log_to_bottom(),
        _ => {}
    }
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

/// Returns the log pane's inner height so the loop can use it for
/// PgUp/PgDn jump sizing on the next key event.
fn draw(frame: &mut ratatui::Frame<'_>, state: &TuiState) -> u16 {
    let area = frame.area();
    // Three rows: metrics (top half), log (most of bottom half), and
    // a single-row status bar at the very bottom that absorbs
    // `notify!` milestones. The status bar uses Min(1) so it stays
    // anchored even when the terminal shrinks; metrics and log split
    // the remaining space 50/50.
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(50),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(area);

    draw_metrics(frame, chunks[0], state);
    let log_height = draw_log(frame, chunks[1], state);
    draw_status_bar(frame, chunks[2], state);
    log_height
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
/// (89%)" reads naturally alongside the in/out rates -- it's the
/// same "what's on the wire" signal set. The status-bar phase
/// widget remains the source of truth for "what is sync doing";
/// this row adds the live counter that matters while the wire is
/// busy.
///
/// The dimmed "last" row shows the most recent completed cycle's
/// outcome with its wall-clock time -- the single-slot notify!
/// status would otherwise overwrite it with the next milestone.
///
/// Maildir-write bytes intentionally don't appear here. They're
/// disk-write throughput, not network bandwidth, and conflating them
/// into a single panel makes the in/out labels lie.
fn draw_bandwidth(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    bw: Bandwidth,
    progress: Option<Progress>,
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
    if let Some(Progress { done, total }) = progress
        && total > 0
    {
        let pct = (done * 100 / total) as u32;
        lines.push(Line::from(vec![
            Span::raw("  dl  "),
            Span::styled(
                format!("{:>10}", format!("{}/{}", done, total)),
                Style::default().fg(Color::Green),
            ),
            Span::styled(
                format!("  ({}%)", pct),
                Style::default().add_modifier(Modifier::DIM),
            ),
        ]));
    }
    if let Some(cycle) = last_cycle {
        lines.push(cycle_line(cycle));
    }
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

/// The Network pane's "last" row: the most recent completed cycle's
/// outcome, dimmed -- it's context, not live activity. In-sync
/// cycles read "in sync" rather than "0 dn / 0 up", which would
/// look like a stall.
fn cycle_line(cycle: CycleSummary) -> Line<'static> {
    let text = if cycle.in_sync {
        "in sync".to_string()
    } else {
        format!("{} dn / {} up", cycle.downloaded, cycle.uploaded)
    };
    Line::from(vec![
        Span::raw("  last"),
        Span::styled(
            format!("{:>12}", text),
            Style::default().add_modifier(Modifier::DIM),
        ),
        Span::styled(
            format!("  {}", cycle.at.format("%H:%M:%S")),
            Style::default().add_modifier(Modifier::DIM),
        ),
    ])
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
            let ts = m.at.format("%H:%M:%S").to_string();
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
/// scroll position; level gets a color hint, target is dimmed,
/// message takes the rest of the line. Title shows "[scrolled +N]"
/// when the user has paged back from the live tail so the deviation
/// is obvious at a glance. Returns the inner pane height so the
/// run loop can size PgUp/PgDn jumps against what's actually on
/// screen.
fn draw_log(frame: &mut ratatui::Frame<'_>, area: Rect, state: &TuiState) -> u16 {
    let inner_height = area.height.saturating_sub(2);
    let view = state.log_view(inner_height as usize);
    let title = if view.scroll > 0 {
        format!(" Log [scrolled +{}] ", view.scroll)
    } else {
        " Log ".to_string()
    };
    let block = Block::default().borders(Borders::ALL).title(title);
    let lines: Vec<Line> = view.lines.into_iter().map(format_log_line).collect();
    let paragraph = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
    inner_height
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

    let mut left_spans = vec![conn_span(state.conn_health())];
    match state.status() {
        Some(Status { at, message }) => {
            left_spans.push(Span::raw(format!(" [{}] ", at.format("%H:%M:%S"))));
            left_spans.push(Span::raw(message));
        }
        None => {
            left_spans.push(Span::raw(
                " jma watch -- arrows/PgUp/PgDn scroll log, q quits ",
            ));
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

/// Compact health segment for the status bar's left edge, colored by
/// severity so degraded states read without consulting the log.
/// Worst state wins: an engine (JMAP session) outage means sync
/// itself is down and shows red; a degraded push channel still syncs
/// on local triggers, so it shows yellow; both healthy is a quiet
/// green "jmap ok". Before the first report from the daemon the
/// segment shows a dim "starting".
fn conn_span(health: ConnHealth) -> Span<'static> {
    if let Some(ConnState::Reconnecting { backoff }) = health.engine {
        return Span::styled(
            format!(" reconnecting {}s ", backoff.as_secs()),
            Style::default().bg(Color::Red).fg(Color::White),
        );
    }
    if let Some(ConnState::Reconnecting { backoff }) = health.sse {
        return Span::styled(
            format!(" push retry {}s ", backoff.as_secs()),
            Style::default().bg(Color::Yellow).fg(Color::Black),
        );
    }
    if health.engine.is_none() && health.sse.is_none() {
        return Span::styled(
            " starting ".to_string(),
            Style::default().bg(Color::DarkGray).fg(Color::White),
        );
    }
    Span::styled(
        " jmap ok ".to_string(),
        Style::default().bg(Color::Green).fg(Color::Black),
    )
}

fn format_log_line(line: LogLine) -> Line<'static> {
    let level_style = match line.level {
        tracing::Level::ERROR => Style::default().fg(Color::Red),
        tracing::Level::WARN => Style::default().fg(Color::Yellow),
        tracing::Level::INFO => Style::default().fg(Color::Green),
        _ => Style::default().fg(Color::DarkGray),
    };
    let ts = line.ts.format("%H:%M:%S").to_string();
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

    /// The "last" row reads "in sync" for no-op cycles -- zero
    /// counts would look like a stall -- and the real counts
    /// otherwise.
    #[test]
    fn cycle_line_formats_in_sync_and_counts() {
        let at = chrono::Utc::now();
        let text: String = cycle_line(CycleSummary {
            at,
            downloaded: 0,
            uploaded: 0,
            in_sync: true,
        })
        .spans
        .iter()
        .map(|s| s.content.as_ref())
        .collect();
        assert!(text.contains("in sync"));

        let text: String = cycle_line(CycleSummary {
            at,
            downloaded: 12,
            uploaded: 3,
            in_sync: false,
        })
        .spans
        .iter()
        .map(|s| s.content.as_ref())
        .collect();
        assert!(text.contains("12 dn / 3 up"));
    }

    /// Worst state wins in the health segment: an engine outage
    /// outranks a degraded push channel, which outranks healthy; no
    /// reports at all reads as startup rather than health.
    #[test]
    fn conn_span_picks_worst_state_first() {
        use std::time::Duration;
        let both_down = ConnHealth {
            engine: Some(ConnState::Reconnecting {
                backoff: Duration::from_secs(4),
            }),
            sse: Some(ConnState::Reconnecting {
                backoff: Duration::from_secs(2),
            }),
        };
        assert_eq!(conn_span(both_down).content, " reconnecting 4s ");

        let push_only = ConnHealth {
            engine: Some(ConnState::Connected),
            sse: Some(ConnState::Reconnecting {
                backoff: Duration::from_secs(2),
            }),
        };
        assert_eq!(conn_span(push_only).content, " push retry 2s ");

        let healthy = ConnHealth {
            engine: Some(ConnState::Connected),
            sse: Some(ConnState::Connected),
        };
        assert_eq!(conn_span(healthy).content, " jmap ok ");

        let unreported = ConnHealth {
            engine: None,
            sse: None,
        };
        assert_eq!(conn_span(unreported).content, " starting ");
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
}
