pub mod auth;
pub mod cli;
pub mod config;
pub mod daemon;
pub mod ids;
pub mod jmap;
pub mod maildir_ops;
pub mod state;
pub mod sync;
pub mod ui;

/// Cargo-style status text printed to stdout. Use for the small set
/// of "what the tool is doing" messages a user expects at the default
/// verbosity -- "Sync complete", "Watch mode active", a download
/// preamble. Tracing logs go to stderr, so the two streams stay
/// separable: a caller can `jma sync >run.log` and still see warnings
/// and errors live on stderr.
///
/// Independent of the tracing log levels: at normal verbosity tracing
/// is clamped to warn, so `info!` is invisible, but `notify!` still
/// prints. `--quiet` suppresses `notify!` along with every other
/// non-error channel.
#[macro_export]
macro_rules! notify {
    ($($arg:tt)*) => {{
        if !$crate::ui::is_quiet() {
            println!($($arg)*);
        }
    }};
}
