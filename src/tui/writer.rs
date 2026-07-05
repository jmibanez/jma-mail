//! Stderr writer that goes silent while the TUI is on the alternate
//! screen.
//!
//! `tracing_subscriber::fmt::layer().with_writer(...)` accepts any
//! `MakeWriter`; `Fn() -> impl Write` implements it out of the box,
//! so `gated_stderr` is a free function exported as the writer
//! factory. Each event constructs a fresh `GatedStderrWriter`, which
//! either forwards to `std::io::Stderr` or drops bytes silently
//! depending on the TUI active flag at write time.
//!
//! Why a writer-side gate and not a layer filter: the gate decision
//! has to happen *per event at write time*, not at layer-install
//! time. The user can also toggle TUI off mid-process (today: by
//! pressing q, which lowers `mark_active`), and from that moment
//! onward fmt_layer should resume writing. A filter installed at
//! init would be stuck with whatever state it was created with.

use std::io::{self, Stderr, Write};

use crate::tui;

/// MakeWriter factory. Pass directly to
/// `tracing_subscriber::fmt::layer().with_writer(...)`.
pub fn gated_stderr() -> GatedStderrWriter {
    GatedStderrWriter {
        inner: io::stderr(),
    }
}

pub struct GatedStderrWriter {
    inner: Stderr,
}

impl Write for GatedStderrWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if tui::is_active() {
            // Drop the bytes on the floor while the alt screen is up.
            // Returning `buf.len()` satisfies fmt_layer's contract
            // without writing anything; the line is lost rather than
            // queued, which is fine because the TUI's log pane is
            // capturing the same events out-of-band.
            Ok(buf.len())
        } else {
            self.inner.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if tui::is_active() {
            Ok(())
        } else {
            self.inner.flush()
        }
    }
}
