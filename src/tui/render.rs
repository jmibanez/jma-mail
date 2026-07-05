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
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use std::io::{Stdout, stdout};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::tui::state::{LogLine, Status, TuiState};

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
    loop {
        terminal
            .draw(|frame| draw(frame, &state))
            .context("draw frame")?;

        if event::poll(TICK).context("poll for terminal event")?
            && let CrosstermEvent::Key(key) = event::read().context("read terminal event")?
            && is_quit_key(key)
        {
            shutdown.notify_waiters();
            return Ok(());
        }
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

fn draw(frame: &mut ratatui::Frame<'_>, state: &TuiState) {
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

    draw_metrics_placeholder(frame, chunks[0]);
    draw_log(frame, chunks[1], state);
    draw_status_bar(frame, chunks[2], state);
}

/// Top half: reserved for metrics. Bandwidth, download progress,
/// and recent subjects land in follow-up commits; this is the
/// layout stub so the split exists from the start.
fn draw_metrics_placeholder(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" jma watch ");
    let body = Paragraph::new(vec![
        Line::from("Metrics will land here:"),
        Line::from("  - bandwidth (in / out, rolling rate)"),
        Line::from("  - download progress"),
        Line::from("  - recently synced subjects"),
    ])
    .block(block);
    frame.render_widget(body, area);
}

/// Bottom half: log pane. Renders the most recent N lines that fit
/// the pane's inner height, oldest at top, newest at bottom. Level
/// gets a color hint, target is dimmed, message takes the rest of
/// the line.
fn draw_log(frame: &mut ratatui::Frame<'_>, area: Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title(" Log ");
    let inner = block.inner(area);
    let capacity = inner.height as usize;
    let lines: Vec<Line> = state
        .recent_logs(capacity)
        .into_iter()
        .map(format_log_line)
        .collect();
    let paragraph = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

/// Single-row status bar at the very bottom. Shows the most recent
/// `notify!` milestone, prefixed with its arrival time. When nothing
/// has been emitted yet, shows the quit hint instead so the row
/// always carries useful information.
fn draw_status_bar(frame: &mut ratatui::Frame<'_>, area: Rect, state: &TuiState) {
    let style = Style::default()
        .bg(Color::Blue)
        .fg(Color::White)
        .add_modifier(Modifier::BOLD);
    let line = match state.status() {
        Some(Status { at, message }) => Line::from(vec![
            Span::raw(format!(" [{}] ", at.format("%H:%M:%S"))),
            Span::raw(message),
        ]),
        None => Line::from(" jma watch -- press q, Esc, or Ctrl-C to quit "),
    };
    frame.render_widget(Paragraph::new(line).style(style), area);
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
}
