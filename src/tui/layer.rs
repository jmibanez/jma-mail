//! tracing Layer that captures events into `TuiState`.
//!
//! Mirrors the shape of `crate::profile::ProfileLayer`: hold an
//! `Arc<TuiState>`, implement `Layer::on_event`, do a tiny visitor
//! to pull out the message field, and push into the shared state.
//!
//! Filtering: this layer only cares about events at `info!` and
//! above. The fmt_layer's env-filter already drops everything below
//! `warn` at default verbosity; the TUI layer raises that floor back
//! to `info` independently so users running with `-v` don't drown
//! the log pane in debug spam from `jma::profile::*` and friends.
//! The render pane is ~half a screen tall; surfacing debug noise
//! there steals room from real progress signals.

use std::sync::Arc;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

use crate::tui::state::{LogLine, TuiState};

pub struct TuiLayer {
    state: Arc<TuiState>,
}

impl TuiLayer {
    pub fn new(state: Arc<TuiState>) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for TuiLayer
where
    S: Subscriber,
{
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
