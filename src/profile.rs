//! Per-run profiling layer. Aggregates `tracing` spans and events
//! emitted under the `jma::profile::*` targets and renders a summary
//! at end-of-run.
//!
//! The data path is `tracing` spans (for things with a duration:
//! sync phases, blob downloads/uploads) and events (for point-in-
//! time counters: maildir file ops). The aggregate lives in a
//! `Mutex<ProfileState>` because `buffer_unordered` produces span
//! closes from concurrent futures.
//!
//! Output is opt-in: `--profile` prints a table to stderr,
//! `--profile-json <PATH>` writes machine-readable JSON. In daemon
//! mode the same handle is flushed per sync cycle (NDJSON for the
//! JSON case) and the state resets between cycles.
//!
//! This module deliberately does not own its own clock or its own
//! counters at the instrumentation sites -- `tracing` provides both
//! through `info_span!`/`event!` and we layer on top. See
//! `src/sync/engine.rs` and `src/maildir_ops/store.rs` for the call
//! sites; this file only consumes what they emit.

use anyhow::{Context, Result};
use serde::Serialize;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context as LayerContext, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Targets the layer recognizes. Match these in instrumentation call
/// sites; anything else is ignored.
pub const TARGET_PHASE: &str = "jma::profile::phase";
pub const TARGET_BLOB: &str = "jma::profile::blob";
pub const TARGET_FILE_OP: &str = "jma::profile::file_op";

/// `tracing` Layer that aggregates profile spans and events into
/// `ProfileState`. Wrap with `with_filter` to restrict to our
/// targets so the layer doesn't pay the visitor cost on unrelated
/// events from other crates.
pub struct ProfileLayer {
    state: Arc<Mutex<ProfileState>>,
}

impl ProfileLayer {
    pub fn new() -> Self {
        let mut state = ProfileState::default();
        state.run_start = Some(Instant::now());
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Cloneable handle to the underlying aggregate. Held by `main`
    /// (and the daemon) so end-of-run / end-of-cycle code can render
    /// a summary or reset state without taking the layer apart.
    pub fn handle(&self) -> ProfileHandle {
        ProfileHandle {
            state: self.state.clone(),
        }
    }
}

impl Default for ProfileLayer {
    fn default() -> Self {
        Self::new()
    }
}

/// Cloneable handle to the profile aggregate. The layer owns one
/// Arc, the caller owns another -- so the layer keeps recording
/// from concurrent span closes while the caller renders or resets
/// at known epochs.
#[derive(Clone)]
pub struct ProfileHandle {
    state: Arc<Mutex<ProfileState>>,
}

impl ProfileHandle {
    /// Snapshot the current aggregate, reset it, and return the
    /// pre-reset value. Used at end of each daemon cycle so cycles
    /// don't accumulate.
    pub fn take_snapshot(&self) -> ProfileSummary {
        let mut state = self.state.lock().expect("profile state mutex");
        let snapshot = state.summarize();
        *state = ProfileState::default();
        state.run_start = Some(Instant::now());
        snapshot
    }

    /// Snapshot without resetting -- for one-shot CLI commands that
    /// run a single sync cycle and then exit.
    pub fn snapshot(&self) -> ProfileSummary {
        let state = self.state.lock().expect("profile state mutex");
        state.summarize()
    }
}

/// Per-phase wall-clock and RSS deltas. Bytes/count are populated
/// for subphase spans that wrap parallel blob I/O.
#[derive(Debug, Clone, Serialize)]
pub struct PhaseRecord {
    pub name: String,
    pub wall_ms: u64,
    pub rss_delta_bytes: i64,
    pub rss_peak_after_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
}

/// Aggregated blob I/O totals. `total_micros` sums per-op durations
/// and is informational only -- with `buffer_unordered` it
/// overstates wall-clock by the concurrency factor, so the honest
/// "effective rate" comes from the enclosing phase span instead.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BlobAggregate {
    pub count: u64,
    pub total_bytes: u64,
    pub total_micros: u64,
}

/// Per-type counts of maildir file operations.
#[derive(Debug, Clone, Default, Serialize)]
pub struct FileOpCounts {
    pub store: u64,
    pub promote: u64,
    pub set_flags: u64,
    pub mv: u64,
    pub delete: u64,
}

impl FileOpCounts {
    pub fn total(&self) -> u64 {
        self.store + self.promote + self.set_flags + self.mv + self.delete
    }

    fn increment(&mut self, op: &str) {
        match op {
            "store" => self.store += 1,
            "promote" => self.promote += 1,
            "set_flags" => self.set_flags += 1,
            "move" => self.mv += 1,
            "delete" => self.delete += 1,
            _ => {}
        }
    }
}

/// Mutable aggregate. Held behind a Mutex inside the Layer; never
/// exposed directly to consumers -- they go through `ProfileHandle`
/// which clones a `ProfileSummary` out before rendering.
#[derive(Default)]
struct ProfileState {
    run_start: Option<Instant>,
    phases: Vec<PhaseRecord>,
    blob_download: BlobAggregate,
    blob_upload: BlobAggregate,
    file_ops: FileOpCounts,
}

impl ProfileState {
    fn summarize(&self) -> ProfileSummary {
        let total_wall_ms = self
            .run_start
            .map(|s| s.elapsed().as_millis() as u64)
            .unwrap_or(0);
        ProfileSummary {
            total_wall_ms,
            phases: self.phases.clone(),
            blob_download: self.blob_download.clone(),
            blob_upload: self.blob_upload.clone(),
            file_ops: self.file_ops.clone(),
        }
    }
}

/// Frozen aggregate suitable for rendering. Owns its own copies of
/// the records so the layer can keep mutating in the background.
#[derive(Debug, Clone, Serialize)]
pub struct ProfileSummary {
    pub total_wall_ms: u64,
    pub phases: Vec<PhaseRecord>,
    pub blob_download: BlobAggregate,
    pub blob_upload: BlobAggregate,
    pub file_ops: FileOpCounts,
}

impl ProfileSummary {
    /// Effective bytes/sec for one of the parallel blob phases.
    /// Looks up the wrapping phase span by name and uses *its*
    /// wall-clock, not the sum of per-blob durations. Returns 0.0
    /// when the phase didn't run or didn't move any bytes.
    pub fn effective_rate_bps(&self, phase_name: &str, bytes: u64) -> f64 {
        if bytes == 0 {
            return 0.0;
        }
        let phase = self.phases.iter().find(|p| p.name == phase_name);
        match phase {
            Some(p) if p.wall_ms > 0 => (bytes as f64) * 1000.0 / (p.wall_ms as f64),
            _ => 0.0,
        }
    }

    /// Render the summary as a fixed-column table. Lands on stderr
    /// next to the existing `notify!` summary.
    pub fn print_table<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
        writeln!(w, "=== jma profile ===")?;
        writeln!(w, "total wall: {} ms", self.total_wall_ms)?;
        writeln!(w)?;

        writeln!(
            w,
            "{:<18} {:>10} {:>14} {:>14} {:>10} {:>10}",
            "phase", "wall_ms", "rss_delta", "rss_peak", "bytes", "count"
        )?;
        writeln!(w, "{}", "-".repeat(80))?;
        for p in &self.phases {
            writeln!(
                w,
                "{:<18} {:>10} {:>14} {:>14} {:>10} {:>10}",
                p.name,
                p.wall_ms,
                format_signed_bytes(p.rss_delta_bytes),
                format_bytes(p.rss_peak_after_bytes),
                p.bytes
                    .map(|b| format_bytes(b))
                    .unwrap_or_else(|| "-".into()),
                p.count.map(|c| c.to_string()).unwrap_or_else(|| "-".into()),
            )?;
        }
        writeln!(w)?;

        let dl_rate = self.effective_rate_bps("download_blobs", self.blob_download.total_bytes);
        let ul_rate = self.effective_rate_bps("upload_blobs", self.blob_upload.total_bytes);
        writeln!(
            w,
            "blob download: {} count, {} total, {}/s effective (phase wall-clock)",
            self.blob_download.count,
            format_bytes(self.blob_download.total_bytes),
            format_rate(dl_rate),
        )?;
        writeln!(
            w,
            "blob upload:   {} count, {} total, {}/s effective (phase wall-clock)",
            self.blob_upload.count,
            format_bytes(self.blob_upload.total_bytes),
            format_rate(ul_rate),
        )?;
        writeln!(w)?;

        let execute_ms = self
            .phases
            .iter()
            .find(|p| p.name == "execute")
            .map(|p| p.wall_ms)
            .unwrap_or(0);
        let total_ops = self.file_ops.total();
        let ops_per_sec = if execute_ms > 0 {
            (total_ops as f64) * 1000.0 / (execute_ms as f64)
        } else {
            0.0
        };
        writeln!(
            w,
            "file ops: store={} promote={} set_flags={} move={} delete={} (total {}, {:.1} ops/s over execute)",
            self.file_ops.store,
            self.file_ops.promote,
            self.file_ops.set_flags,
            self.file_ops.mv,
            self.file_ops.delete,
            total_ops,
            ops_per_sec,
        )?;
        Ok(())
    }

    pub fn to_json_line(&self) -> Result<String> {
        serde_json::to_string(self).context("Failed to serialize profile summary as JSON")
    }
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

fn format_signed_bytes(n: i64) -> String {
    if n < 0 {
        format!("-{}", format_bytes(n.unsigned_abs()))
    } else {
        format!("+{}", format_bytes(n as u64))
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

/// Snapshot `ru_maxrss` and normalize to bytes. macOS returns
/// bytes; Linux and the BSDs return KiB. Anything that can't be
/// queried returns 0 -- the resulting phase row will show a
/// zero/zero RSS pair, which is honest about the missing data.
fn read_rss_bytes() -> u64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc != 0 {
        return 0;
    }
    let raw = usage.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        raw
    } else {
        raw.saturating_mul(1024)
    }
}

/// Per-span data we stash via `extensions_mut` on the Registry's
/// `SpanRef`. One variant per recognized target so `on_close` can
/// branch cleanly on which aggregate to update.
enum SpanData {
    Phase {
        name: String,
        start: Instant,
        rss_start: u64,
        bytes: Option<u64>,
        count: Option<u64>,
    },
    Blob {
        bucket: BlobBucket,
        start: Instant,
        bytes: Option<u64>,
    },
}

#[derive(Copy, Clone)]
enum BlobBucket {
    Download,
    Upload,
}

impl<S> Layer<S> for ProfileLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        let meta = attrs.metadata();
        let span = match ctx.span(id) {
            Some(s) => s,
            None => return,
        };
        let mut ext = span.extensions_mut();
        match meta.target() {
            TARGET_PHASE => {
                let mut data = SpanData::Phase {
                    name: meta.name().to_string(),
                    start: Instant::now(),
                    rss_start: read_rss_bytes(),
                    bytes: None,
                    count: None,
                };
                // Subphase spans (download_blobs / upload_blobs)
                // declare bytes/count as Empty at creation and
                // record them after the loop drains. Capture
                // anything supplied up-front (none today, but the
                // visitor handles it harmlessly).
                let mut visitor = NumericFieldVisitor {
                    target: NumericTarget::Phase(&mut data),
                };
                attrs.record(&mut visitor);
                ext.insert(data);
            }
            TARGET_BLOB => {
                let bucket = match meta.name() {
                    "blob.download" => BlobBucket::Download,
                    "blob.upload" => BlobBucket::Upload,
                    _ => return,
                };
                let mut data = SpanData::Blob {
                    bucket,
                    start: Instant::now(),
                    bytes: None,
                };
                let mut visitor = NumericFieldVisitor {
                    target: NumericTarget::Blob(&mut data),
                };
                attrs.record(&mut visitor);
                ext.insert(data);
            }
            _ => {}
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: LayerContext<'_, S>) {
        let span = match ctx.span(id) {
            Some(s) => s,
            None => return,
        };
        let mut ext = span.extensions_mut();
        if let Some(data) = ext.get_mut::<SpanData>() {
            let mut visitor = match data {
                SpanData::Phase { .. } => NumericFieldVisitor {
                    target: NumericTarget::Phase(data),
                },
                SpanData::Blob { .. } => NumericFieldVisitor {
                    target: NumericTarget::Blob(data),
                },
            };
            values.record(&mut visitor);
        }
    }

    fn on_close(&self, id: Id, ctx: LayerContext<'_, S>) {
        let span = match ctx.span(&id) {
            Some(s) => s,
            None => return,
        };
        let ext = span.extensions();
        let data = match ext.get::<SpanData>() {
            Some(d) => d,
            None => return,
        };
        let mut state = self.state.lock().expect("profile state mutex");
        match data {
            SpanData::Phase {
                name,
                start,
                rss_start,
                bytes,
                count,
            } => {
                let elapsed = start.elapsed();
                let rss_end = read_rss_bytes();
                state.phases.push(PhaseRecord {
                    name: name.clone(),
                    wall_ms: elapsed.as_millis() as u64,
                    rss_delta_bytes: (rss_end as i64) - (*rss_start as i64),
                    rss_peak_after_bytes: rss_end,
                    bytes: *bytes,
                    count: *count,
                });
            }
            SpanData::Blob {
                bucket,
                start,
                bytes,
            } => {
                let elapsed = start.elapsed();
                let agg = match bucket {
                    BlobBucket::Download => &mut state.blob_download,
                    BlobBucket::Upload => &mut state.blob_upload,
                };
                agg.count += 1;
                agg.total_bytes += bytes.unwrap_or(0);
                agg.total_micros += elapsed.as_micros() as u64;
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        if event.metadata().target() != TARGET_FILE_OP {
            return;
        }
        let mut visitor = FileOpEventVisitor { op: None };
        event.record(&mut visitor);
        if let Some(op) = visitor.op {
            let mut state = self.state.lock().expect("profile state mutex");
            state.file_ops.increment(&op);
        }
    }
}

/// One visitor for both phase and blob spans -- they both want
/// numeric fields (`bytes`, `count`) and ignore everything else.
enum NumericTarget<'a> {
    Phase(&'a mut SpanData),
    Blob(&'a mut SpanData),
}

struct NumericFieldVisitor<'a> {
    target: NumericTarget<'a>,
}

impl<'a> NumericFieldVisitor<'a> {
    fn store_u64(&mut self, name: &str, value: u64) {
        match &mut self.target {
            NumericTarget::Phase(SpanData::Phase { bytes, count, .. }) => match name {
                "bytes" => *bytes = Some(value),
                "count" => *count = Some(value),
                _ => {}
            },
            NumericTarget::Blob(SpanData::Blob { bytes, .. }) => {
                if name == "bytes" {
                    *bytes = Some(value);
                }
            }
            _ => {}
        }
    }
}

impl<'a> Visit for NumericFieldVisitor<'a> {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.store_u64(field.name(), value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if value >= 0 {
            self.store_u64(field.name(), value as u64);
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn fmt::Debug) {}
}

struct FileOpEventVisitor {
    op: Option<String>,
}

impl Visit for FileOpEventVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "op" {
            self.op = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // `event!(op = some_var)` where the var is a non-string
        // type lands here; record the debug form so the counter
        // still increments under unusual call shapes.
        if field.name() == "op" {
            self.op = Some(format!("{:?}", value).trim_matches('"').to_string());
        }
    }
}

/// Helper for callers that want a one-step "build a layer and grab
/// a handle to flush later." Pattern: in main.rs build the layer
/// once, install it via `with(layer)`, keep the handle for the
/// end-of-run flush. Daemon takes a clone of the handle for per-
/// cycle flushes.
pub fn build_layer() -> (ProfileLayer, ProfileHandle) {
    let layer = ProfileLayer::new();
    let handle = layer.handle();
    (layer, handle)
}

/// Bundles the handle with the output preferences derived from the
/// CLI flags. One-shot subcommands call `flush_snapshot` at the end
/// of their dispatch; the daemon calls `flush_and_reset` per sync
/// cycle so the next cycle's report starts clean.
#[derive(Clone)]
pub struct ProfileSink {
    pub handle: ProfileHandle,
    pub print_table: bool,
    pub json_path: Option<PathBuf>,
}

impl ProfileSink {
    pub fn flush_snapshot(&self) -> Result<()> {
        let summary = self.handle.snapshot();
        flush(&summary, self.print_table, self.json_path.as_deref())
    }

    pub fn flush_and_reset(&self) -> Result<()> {
        let summary = self.handle.take_snapshot();
        flush(&summary, self.print_table, self.json_path.as_deref())
    }
}

/// Decide what to do with a snapshot at the end of a run / cycle:
/// stderr table if `print_table`, JSON appended (or written) if a
/// `json_path` is given. The JSON branch always uses NDJSON-style
/// append-open so daemon cycles accumulate cleanly into the same
/// file across a long-lived watch run -- a one-shot CLI just writes
/// the single line and exits.
pub fn flush(summary: &ProfileSummary, print_table: bool, json_path: Option<&Path>) -> Result<()> {
    if print_table {
        let mut stderr = std::io::stderr().lock();
        summary
            .print_table(&mut stderr)
            .context("Failed to print profile table")?;
    }
    if let Some(path) = json_path {
        use std::io::Write;
        let line = summary.to_json_line()?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("Failed to open profile JSON {}", path.display()))?;
        writeln!(file, "{}", line)
            .with_context(|| format!("Failed to write profile JSON to {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tracing::field::Empty;
    use tracing_subscriber::prelude::*;

    /// A phase span that opens, runs briefly, and closes produces
    /// exactly one PhaseRecord with non-zero wall_ms and the right
    /// name.
    #[test]
    fn phase_span_produces_record() {
        let layer = ProfileLayer::new();
        let handle = layer.handle();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(target: TARGET_PHASE, "dedupe");
            let _enter = span.enter();
            std::thread::sleep(Duration::from_millis(5));
        });
        let summary = handle.snapshot();
        assert_eq!(summary.phases.len(), 1);
        assert_eq!(summary.phases[0].name, "dedupe");
        assert!(summary.phases[0].wall_ms >= 4);
    }

    /// A subphase span with bytes/count recorded after the work
    /// captures both fields and the layer reads them on close.
    #[test]
    fn subphase_records_bytes_and_count() {
        let layer = ProfileLayer::new();
        let handle = layer.handle();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                target: TARGET_PHASE,
                "download_blobs",
                bytes = Empty,
                count = Empty,
            );
            let _enter = span.enter();
            span.record("bytes", 12345u64);
            span.record("count", 3u64);
        });
        let summary = handle.snapshot();
        let phase = summary
            .phases
            .iter()
            .find(|p| p.name == "download_blobs")
            .expect("download_blobs phase");
        assert_eq!(phase.bytes, Some(12345));
        assert_eq!(phase.count, Some(3));
    }

    /// Blob spans aggregate by bucket. Two downloads + one upload
    /// produce the expected totals.
    #[test]
    fn blob_spans_aggregate_by_bucket() {
        let layer = ProfileLayer::new();
        let handle = layer.handle();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            for size in [100u64, 200u64] {
                let span = tracing::info_span!(
                    target: TARGET_BLOB,
                    "blob.download",
                    bytes = Empty,
                );
                let _enter = span.enter();
                span.record("bytes", size);
            }
            let up = tracing::info_span!(
                target: TARGET_BLOB,
                "blob.upload",
                bytes = Empty,
            );
            let _enter = up.enter();
            up.record("bytes", 500u64);
        });
        let summary = handle.snapshot();
        assert_eq!(summary.blob_download.count, 2);
        assert_eq!(summary.blob_download.total_bytes, 300);
        assert_eq!(summary.blob_upload.count, 1);
        assert_eq!(summary.blob_upload.total_bytes, 500);
    }

    /// File-op events increment per-type counters.
    #[test]
    fn file_op_events_increment_counters() {
        let layer = ProfileLayer::new();
        let handle = layer.handle();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(target: TARGET_FILE_OP, tracing::Level::TRACE, op = "store");
            tracing::event!(target: TARGET_FILE_OP, tracing::Level::TRACE, op = "store");
            tracing::event!(target: TARGET_FILE_OP, tracing::Level::TRACE, op = "delete");
            tracing::event!(target: TARGET_FILE_OP, tracing::Level::TRACE, op = "promote");
        });
        let summary = handle.snapshot();
        assert_eq!(summary.file_ops.store, 2);
        assert_eq!(summary.file_ops.delete, 1);
        assert_eq!(summary.file_ops.promote, 1);
        assert_eq!(summary.file_ops.total(), 4);
    }

    /// Effective rate uses the wrapping phase wall-clock, not the
    /// sum of per-blob durations. A 1 MB phase that closes in 200ms
    /// is 5 MB/s, not whatever the per-blob times happen to sum to.
    #[test]
    fn effective_rate_uses_phase_wall_clock() {
        let summary = ProfileSummary {
            total_wall_ms: 1000,
            phases: vec![PhaseRecord {
                name: "download_blobs".into(),
                wall_ms: 200,
                rss_delta_bytes: 0,
                rss_peak_after_bytes: 0,
                bytes: Some(1024 * 1024),
                count: Some(10),
            }],
            blob_download: BlobAggregate {
                count: 10,
                total_bytes: 1024 * 1024,
                // Per-blob durations sum to 10s -- if we used this,
                // we'd report 100 KB/s. Using phase wall-clock
                // (200ms) we report ~5 MB/s.
                total_micros: 10_000_000,
            },
            blob_upload: BlobAggregate::default(),
            file_ops: FileOpCounts::default(),
        };
        let rate = summary.effective_rate_bps("download_blobs", 1024 * 1024);
        // 1 MB over 200ms = 5 MB/s = ~5.24 MB/s in decimal.
        assert!(
            rate > 5.0 * 1024.0 * 1024.0 - 1000.0 && rate < 5.0 * 1024.0 * 1024.0 + 1000.0,
            "rate {} not near 5 MB/s",
            rate
        );
    }

    /// JSON round-trip: a populated summary serializes to valid
    /// JSON with the expected top-level keys.
    #[test]
    fn summary_serializes_to_json() {
        let summary = ProfileSummary {
            total_wall_ms: 1234,
            phases: vec![PhaseRecord {
                name: "execute".into(),
                wall_ms: 500,
                rss_delta_bytes: 1024,
                rss_peak_after_bytes: 10240,
                bytes: None,
                count: None,
            }],
            blob_download: BlobAggregate {
                count: 5,
                total_bytes: 12345,
                total_micros: 100_000,
            },
            blob_upload: BlobAggregate::default(),
            file_ops: FileOpCounts {
                store: 3,
                ..Default::default()
            },
        };
        let line = summary.to_json_line().expect("serializes");
        assert!(line.contains("\"total_wall_ms\":1234"));
        assert!(line.contains("\"phases\""));
        assert!(line.contains("\"execute\""));
        assert!(line.contains("\"blob_download\""));
        assert!(line.contains("\"file_ops\""));
    }
}
