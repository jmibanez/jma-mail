//! tracing Layer that captures events and spans into `TuiState`.
//!
//! Two data paths:
//!
//!   - Events at INFO and above land in the log pane ring buffer.
//!     The fmt_layer's env-filter already drops everything below
//!     `warn` at default verbosity; the TUI layer raises that floor
//!     back to `info` independently so users running with `-v` don't
//!     drown the log pane in debug spam from `jma::profile::*` and
//!     friends. The render pane is ~half a screen tall; surfacing
//!     debug noise there steals room from real progress signals.
//!
//!   - `jma::profile::blob` spans -- one per blob.download or
//!     blob.upload -- carry a `bytes` field that gets recorded
//!     mid-flight via `Span::current().record("bytes", ...)`. The
//!     layer stashes the in-flight value in span extensions, mirrors
//!     ProfileLayer's pattern, and on close pushes the final byte
//!     count into the bandwidth window.
//!
//! Why piggyback on the profile layer's instrumentation instead of
//! emitting a TUI-specific event: a parallel collector would double-
//! record from every call site. The spans are already shaped for
//! this.

use std::sync::Arc;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::profile::{TARGET_BLOB, TARGET_PHASE};
use crate::tui::state::{ConnState, Direction, LogLine, TuiState};

/// Target the executor emits a per-message download-progress event
/// under. Captured by TuiLayer's on_event into TuiState's live
/// `download_progress`. Kept distinct from `jma::profile::*` so the
/// profile layer's target-filtered Layer doesn't pick it up.
pub const TARGET_TUI_PROGRESS: &str = "jma::tui::progress";

/// Target the executor emits a "message just synced" event under
/// (Subject + folder). Captured by TuiLayer into the Recent pane's
/// ring buffer.
pub const TARGET_TUI_MESSAGE: &str = "jma::tui::message";

/// Target the daemon emits connection-health transitions under.
/// Events carry `channel` ("engine" for the JMAP session, "sse" for
/// the push listener, "watcher" for the filesystem watcher), `state`
/// ("connected" / "reconnecting" / "down"), and `backoff_ms` while
/// reconnecting. Captured by TuiLayer into the status bar's health
/// badges.
pub const TARGET_TUI_CONN: &str = "jma::tui::conn";

/// Target the daemon emits one event per completed sync cycle
/// under, carrying `downloaded`, `uploaded`, `in_sync`, and the
/// cycle's `wall_ms`. Feeds the Network pane's "last" row.
pub const TARGET_TUI_CYCLE: &str = "jma::tui::cycle";

/// One row per event target the layer consumes: the target string
/// and the handler that routes a matching event into `TuiState`.
/// The admission filter is derived from this same table (see
/// [`target_filter`]), so consuming a new event target is one entry
/// here -- there is no separate list that can drift out of sync
/// with the routing.
const EVENT_ROUTES: &[(&str, EventRoute)] = &[
    (TARGET_TUI_PROGRESS, route_progress),
    (TARGET_TUI_MESSAGE, route_message),
    (TARGET_TUI_CONN, route_conn),
    (TARGET_TUI_CYCLE, route_cycle),
];

/// Handler that routes one matched event into `TuiState`.
type EventRoute = fn(&TuiState, &Event<'_>);

/// Targets whose *spans* the layer consumes -- the phase stack and
/// the bandwidth samples ride span open/close, not events, so they
/// dispatch in `on_new_span`/`on_close` rather than through
/// [`EVENT_ROUTES`]. Listed here so [`target_filter`] admits them.
const SPAN_TARGETS: &[&str] = &[TARGET_PHASE, TARGET_BLOB];

/// The admission filter for `TuiLayer`, derived from the layer's
/// own routing tables. Everything the layer consumes fires at
/// TRACE, below any sane default floor, so each target must be
/// admitted explicitly -- one left out is dropped before the layer
/// runs and the panel it feeds silently stays empty. Deriving the
/// filter from [`EVENT_ROUTES`] and [`SPAN_TARGETS`] makes
/// routed-but-unadmitted unrepresentable. The INFO default keeps
/// ordinary log events flowing to the log pane, backstopped by
/// `on_event`'s own INFO floor for defense in depth.
pub fn target_filter() -> tracing_subscriber::filter::Targets {
    tracing_subscriber::filter::Targets::new()
        .with_default(Level::INFO)
        .with_targets(
            EVENT_ROUTES
                .iter()
                .map(|(target, _)| *target)
                .chain(SPAN_TARGETS.iter().copied())
                .map(|target| (target, Level::TRACE)),
        )
}

pub struct TuiLayer {
    state: Arc<TuiState>,
}

impl TuiLayer {
    pub fn new(state: Arc<TuiState>) -> Self {
        Self { state }
    }
}

/// Per-span scratch the layer stashes via `extensions_mut` on the
/// Registry's `SpanRef`. Holds the direction (derived from the span
/// name once at creation) and the most recent `bytes` value recorded
/// on the span, which on_close turns into a bandwidth sample.
struct BlobData {
    direction: Direction,
    bytes: u64,
}

impl<S> Layer<S> for TuiLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let meta = attrs.metadata();
        match meta.target() {
            TARGET_BLOB => {
                let direction = match meta.name() {
                    "blob.download" => Direction::In,
                    "blob.upload" => Direction::Out,
                    _ => return,
                };
                let Some(span) = ctx.span(id) else { return };
                let mut data = BlobData {
                    direction,
                    bytes: 0,
                };
                // The span declares `bytes = Empty` at creation;
                // nothing to capture here today, but visit the
                // attrs anyway so a future call site that supplies
                // bytes up-front gets picked up.
                let mut visitor = BytesVisitor { bytes: data.bytes };
                attrs.record(&mut visitor);
                data.bytes = visitor.bytes;
                span.extensions_mut().insert(data);
            }
            TARGET_PHASE => {
                // Phase name = span name (engine.rs builds them as
                // `info_span!(target: TARGET_PHASE, "scan")` etc.).
                // The id stashed in state is the tracing span id
                // converted to u64 -- ids are unique within a
                // process, so the matching pop in on_close finds
                // exactly the right entry without us needing to
                // mirror tracing's id type into the state layer.
                self.state
                    .enter_phase(id.into_u64(), meta.name().to_string());
            }
            _ => {}
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut ext = span.extensions_mut();
        if let Some(data) = ext.get_mut::<BlobData>() {
            let mut visitor = BytesVisitor { bytes: data.bytes };
            values.record(&mut visitor);
            data.bytes = visitor.bytes;
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let meta = span.metadata();
        match meta.target() {
            TARGET_BLOB => {
                let ext = span.extensions();
                let Some(data) = ext.get::<BlobData>() else {
                    return;
                };
                // Skip the no-byte case so a span that closed
                // without ever recording (download_blob bailing on
                // a transient error before the data hit memory,
                // say) doesn't pollute the window with a zero-byte
                // sample. The error itself surfaces through the
                // log pane.
                if data.bytes == 0 {
                    return;
                }
                self.state.add_bandwidth(data.direction, data.bytes);
            }
            TARGET_PHASE => {
                self.state.complete_phase(id.into_u64());
            }
            _ => {}
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        // Metric events feed the panels, not the log pane. Routed
        // by table lookup before the level floor because they fire
        // at TRACE (e.g. one progress event per stored download).
        if let Some((_, route)) = EVENT_ROUTES
            .iter()
            .find(|(target, _)| *target == meta.target())
        {
            route(&self.state, event);
            return;
        }
        // Floor at INFO. Trace/debug events are too high-volume to
        // share a half-screen pane with the metrics the user wants
        // to read.
        if *meta.level() > Level::INFO {
            return;
        }
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let message = visitor.message.unwrap_or_default();
        if message.is_empty() {
            // No `message` field -- some events only carry structured
            // fields (e.g. `bytes = 4096`). Those are for the metric
            // panels, not the log pane; ignore here.
            return;
        }
        self.state.push_log(LogLine {
            ts: chrono::Utc::now(),
            level: *meta.level(),
            target: meta.target().to_string(),
            message,
        });
    }
}

/// Route one download-progress event into the live progress slot.
fn route_progress(state: &TuiState, event: &Event<'_>) {
    let mut visitor = ProgressVisitor::default();
    event.record(&mut visitor);
    if let (Some(done), Some(total)) = (visitor.done, visitor.total) {
        state.set_download_progress(done, total);
    }
}

/// Route one just-synced-message event into the Recent pane's ring.
fn route_message(state: &TuiState, event: &Event<'_>) {
    let mut visitor = MessageInfoVisitor::default();
    event.record(&mut visitor);
    if let (Some(folder), Some(subject)) = (visitor.folder, visitor.subject) {
        state.push_recent(folder, subject);
    }
}

/// Route one cycle-outcome event into the "last" slot.
fn route_cycle(state: &TuiState, event: &Event<'_>) {
    let mut visitor = CycleVisitor::default();
    event.record(&mut visitor);
    if let (Some(downloaded), Some(uploaded), Some(in_sync), Some(wall_ms)) = (
        visitor.downloaded,
        visitor.uploaded,
        visitor.in_sync,
        visitor.wall_ms,
    ) {
        state.set_last_cycle(
            downloaded,
            uploaded,
            in_sync,
            std::time::Duration::from_millis(wall_ms),
        );
    }
}

/// Route one connection-health transition into the named channel's
/// slot in the status bar's health segment.
fn route_conn(state: &TuiState, event: &Event<'_>) {
    let mut visitor = ConnVisitor::default();
    event.record(&mut visitor);
    if let (Some(channel), Some(conn_state)) =
        (visitor.channel.as_deref(), visitor.state.as_deref())
        && let Some(conn) = conn_state_from_event(conn_state, visitor.backoff_ms)
    {
        match channel {
            "engine" => state.set_engine_conn(conn),
            "sse" => state.set_sse_conn(conn),
            "watcher" => state.set_watcher_conn(conn),
            _ => {}
        }
    }
}

/// Reads the `bytes` field off a span's attrs or a record batch.
/// `record_u64` is the path tracing takes for `record("bytes", N as u64)`;
/// `record_i64` is defensive in case a future call site uses a
/// signed integer.
struct BytesVisitor {
    bytes: u64,
}

impl Visit for BytesVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "bytes" {
            self.bytes = value;
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "bytes" && value >= 0 {
            self.bytes = value as u64;
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

/// Reads `done` and `total` from a `TARGET_TUI_PROGRESS` event.
/// Both must be present for the panel to register an update; a
/// missing field leaves the previous snapshot unchanged.
#[derive(Default)]
struct ProgressVisitor {
    done: Option<u64>,
    total: Option<u64>,
}

impl Visit for ProgressVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "done" => self.done = Some(value),
            "total" => self.total = Some(value),
            _ => {}
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if value >= 0 {
            self.record_u64(field, value as u64);
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

/// Reads `folder` and `subject` from a `TARGET_TUI_MESSAGE` event.
/// Both must be present for the entry to land in the recent pane;
/// missing either drops the push without disturbing prior entries.
#[derive(Default)]
struct MessageInfoVisitor {
    folder: Option<String>,
    subject: Option<String>,
}

impl Visit for MessageInfoVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "folder" => self.folder = Some(value.to_string()),
            "subject" => self.subject = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `event!(subject = some_string)` where the value isn't a
        // `&str` literal can land here. Strip the surrounding quotes
        // so the panel doesn't show "\"Subject text\"" literally.
        match field.name() {
            "folder" => self.folder = Some(format!("{:?}", value).trim_matches('"').to_string()),
            "subject" => self.subject = Some(format!("{:?}", value).trim_matches('"').to_string()),
            _ => {}
        }
    }
}

/// Pulls the `message` field out of a tracing event. Other fields
/// are ignored at this layer -- structured metric fields are
/// captured by the metric-specific event handlers (or, today, by
/// `ProfileLayer` for the existing instrumentation).
#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{:?}", value));
        }
    }
}

/// Reads `channel`, `state`, and `backoff_ms` from a
/// `TARGET_TUI_CONN` event. Channel and state must both be present
/// for the report to register; a missing backoff on a reconnecting
/// report renders as zero rather than dropping the transition.
#[derive(Default)]
struct ConnVisitor {
    channel: Option<String>,
    state: Option<String>,
    backoff_ms: Option<u64>,
}

impl Visit for ConnVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "channel" => self.channel = Some(value.to_string()),
            "state" => self.state = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "backoff_ms" {
            self.backoff_ms = Some(value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Same quote-stripping fallback as MessageInfoVisitor, for
        // call sites that don't pass a `&str` literal.
        match field.name() {
            "channel" => self.channel = Some(format!("{:?}", value).trim_matches('"').to_string()),
            "state" => self.state = Some(format!("{:?}", value).trim_matches('"').to_string()),
            _ => {}
        }
    }
}

/// Reads `downloaded`, `uploaded`, `in_sync`, and `wall_ms` from a
/// `TARGET_TUI_CYCLE` event. All four must be present for the
/// outcome to register; a partial event leaves the previous cycle
/// on display.
#[derive(Default)]
struct CycleVisitor {
    downloaded: Option<u64>,
    uploaded: Option<u64>,
    in_sync: Option<bool>,
    wall_ms: Option<u64>,
}

impl Visit for CycleVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "downloaded" => self.downloaded = Some(value),
            "uploaded" => self.uploaded = Some(value),
            "wall_ms" => self.wall_ms = Some(value),
            _ => {}
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "in_sync" {
            self.in_sync = Some(value);
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

/// Map a `TARGET_TUI_CONN` event's `state` string onto `ConnState`.
/// Unknown states drop the report on the floor rather than guessing;
/// the previous displayed state stays until a recognized transition
/// arrives.
fn conn_state_from_event(state: &str, backoff_ms: Option<u64>) -> Option<ConnState> {
    match state {
        "connected" => Some(ConnState::Connected),
        "reconnecting" => Some(ConnState::Reconnecting {
            backoff: std::time::Duration::from_millis(backoff_ms.unwrap_or(0)),
        }),
        "down" => Some(ConnState::Down),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    /// Every target the layer consumes -- event and span side alike
    /// -- must be admitted at TRACE by the derived filter, the INFO
    /// default must keep ordinary log events flowing to the log
    /// pane, and unrouted TRACE noise must stay out. Because the
    /// filter is built from the routing tables themselves, this
    /// holds for every future table row too.
    #[test]
    fn target_filter_admits_exactly_what_the_layer_routes() {
        let filter = target_filter();
        for (target, _) in EVENT_ROUTES {
            assert!(
                filter.would_enable(target, &Level::TRACE),
                "{target} routed but not admitted"
            );
        }
        for target in SPAN_TARGETS {
            assert!(
                filter.would_enable(target, &Level::TRACE),
                "{target} consumed as spans but not admitted"
            );
        }
        assert!(filter.would_enable("jma::sync::engine", &Level::INFO));
        assert!(!filter.would_enable("jma::sync::engine", &Level::TRACE));
    }

    /// End-to-end through a real subscriber: a metric event emitted
    /// the way the daemon emits it -- TRACE level, through the
    /// derived filter -- must land in the state slot. Layer-only
    /// tests cannot see admission: a target the filter drops never
    /// reaches on_event, and the panel it feeds silently stays
    /// empty.
    #[test]
    fn routed_event_passes_the_derived_filter_into_state() {
        use tracing_subscriber::prelude::*;
        let state = Arc::new(TuiState::new());
        let subscriber = tracing_subscriber::registry()
            .with(TuiLayer::new(state.clone()).with_filter(target_filter()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: TARGET_TUI_CYCLE,
                tracing::Level::TRACE,
                downloaded = 12u64,
                uploaded = 3u64,
                in_sync = false,
                wall_ms = 1_200u64,
            );
        });
        let cycle = state.last_cycle().expect("cycle must reach the state slot");
        assert_eq!((cycle.downloaded, cycle.uploaded), (12, 3));
        assert_eq!(cycle.wall, Duration::from_millis(1_200));
    }

    /// The event-to-state mapping is the layer's contract with the
    /// daemon's emit sites: recognized states convert (reconnecting
    /// carries its backoff, defaulting to zero when absent), unknown
    /// states are dropped so a typo'd emit can't corrupt the display.
    #[test]
    fn conn_state_mapping_covers_known_states_and_drops_unknown() {
        assert_eq!(
            conn_state_from_event("connected", None),
            Some(ConnState::Connected)
        );
        assert_eq!(
            conn_state_from_event("reconnecting", Some(8_000)),
            Some(ConnState::Reconnecting {
                backoff: Duration::from_secs(8)
            })
        );
        assert_eq!(
            conn_state_from_event("reconnecting", None),
            Some(ConnState::Reconnecting {
                backoff: Duration::ZERO
            })
        );
        assert_eq!(conn_state_from_event("down", None), Some(ConnState::Down));
        assert_eq!(conn_state_from_event("degraded", None), None);
    }
}
