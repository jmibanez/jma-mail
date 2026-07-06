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

use crate::profile::TARGET_BLOB;
use crate::tui::state::{Direction, LogLine, TuiState};

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
        if meta.target() != TARGET_BLOB {
            return;
        }
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
        // The span declares `bytes = Empty` at creation; nothing to
        // capture here today, but visit the attrs anyway so a future
        // call site that supplies bytes up-front gets picked up.
        let mut visitor = BytesVisitor { bytes: data.bytes };
        attrs.record(&mut visitor);
        data.bytes = visitor.bytes;
        span.extensions_mut().insert(data);
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
        let ext = span.extensions();
        let Some(data) = ext.get::<BlobData>() else {
            return;
        };
        // Skip the no-byte case so a span that closed without ever
        // recording (download_blob bailing on a transient error before
        // the data hit memory, say) doesn't pollute the window with a
        // zero-byte sample. The error itself surfaces through the log
        // pane.
        if data.bytes == 0 {
            return;
        }
        self.state.add_bandwidth(data.direction, data.bytes);
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
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
