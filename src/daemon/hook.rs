use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Single-slot coalescing runner for the post-arrival shell command.
///
/// When `trigger()` is called and no run is in flight, the command is
/// spawned immediately on the tokio runtime and the call returns. If a
/// run is already in flight, a "pending" flag is raised; when the
/// in-flight run finishes it sees the flag, clears it, and starts one
/// follow-up run -- regardless of how many `trigger()` calls landed
/// while it was busy. This collapses bursts of triggers (e.g. several
/// small SSE-driven syncs in close succession) into one extra
/// invocation, matching the spec: "delay invocation until the previous
/// proc finishes" without unbounded queueing.
///
/// The command runs via `sh -c` so users can pipe / chain freely. The
/// spawned process is detached from this task (the task does
/// `child.wait().await` to learn when it exits, but a kill of the
/// daemon won't wait for the child to clean up).
#[derive(Clone)]
pub struct Hook {
    inner: Arc<Inner>,
}

struct Inner {
    /// Shell command to execute. None means the hook is disabled and
    /// `trigger()` is a no-op.
    command: Option<String>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    running: bool,
    pending: bool,
}

impl Hook {
    pub fn new(command: Option<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                command,
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// Returns true when the hook will actually do something. Useful
    /// for the runner to skip threading the count when no one is
    /// listening.
    pub fn is_enabled(&self) -> bool {
        self.inner.command.is_some()
    }

    /// Request that the post-arrival command run. Returns immediately;
    /// the actual command runs on a tokio task. Coalesces with any
    /// in-flight or already-pending run.
    pub async fn trigger(&self) {
        let cmd = match &self.inner.command {
            Some(c) => c.clone(),
            None => return,
        };

        let mut state = self.inner.state.lock().await;
        if state.running {
            if !state.pending {
                debug!("Post-arrival hook already running; queueing follow-up");
                state.pending = true;
            } else {
                debug!("Post-arrival hook already running with follow-up queued; coalescing");
            }
            return;
        }
        state.running = true;
        drop(state);

        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            run_loop(inner, cmd).await;
        });
    }
}

/// Runs the command, then re-runs once if a follow-up was queued. Loops
/// because consecutive `trigger()` calls during a follow-up should also
/// coalesce into another single follow-up (and so on, until idle).
async fn run_loop(inner: Arc<Inner>, cmd: String) {
    loop {
        run_once(&cmd).await;

        let mut state = inner.state.lock().await;
        if state.pending {
            state.pending = false;
            // running stays true; loop and run again.
            drop(state);
        } else {
            state.running = false;
            return;
        }
    }
}

async fn run_once(cmd: &str) {
    info!("Running post-arrival hook: {}", cmd);
    let mut child = match Command::new("sh").arg("-c").arg(cmd).spawn() {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to spawn post-arrival hook ({}): {}", cmd, e);
            return;
        }
    };
    match child.wait().await {
        Ok(status) if status.success() => {
            info!("Post-arrival hook finished cleanly");
        }
        Ok(status) => {
            warn!("Post-arrival hook exited with {}", status);
        }
        Err(e) => {
            error!("Failed to await post-arrival hook: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Helper that waits up to `timeout` for `predicate` to become true.
    async fn wait_for(predicate: impl Fn() -> bool, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if predicate() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        predicate()
    }

    #[tokio::test]
    async fn disabled_hook_is_a_noop() {
        let hook = Hook::new(None);
        assert!(!hook.is_enabled());
        hook.trigger().await;
    }

    #[tokio::test]
    async fn coalesces_bursts_into_at_most_two_runs() {
        // Use a marker file in a tempdir to count invocations.
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("count");
        let cmd = format!(
            "sleep 0.2; printf x >> {}",
            marker.display()
        );
        let hook = Hook::new(Some(cmd));

        // Fire 5 triggers in quick succession.
        for _ in 0..5 {
            hook.trigger().await;
        }

        // Wait for both runs (initial + 1 coalesced follow-up) to finish.
        wait_for(
            || {
                std::fs::read(&marker)
                    .map(|b| b.len() >= 2)
                    .unwrap_or(false)
            },
            Duration::from_secs(3),
        )
        .await;

        // Give a small grace period in case a third (incorrect) run is in flight.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let bytes = std::fs::read(&marker).unwrap_or_default();
        assert_eq!(
            bytes.len(),
            2,
            "5 triggers should collapse to exactly 2 runs (initial + 1 follow-up), got {}",
            bytes.len()
        );
    }

    #[tokio::test]
    async fn single_trigger_runs_once() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("count");
        let cmd = format!("printf x >> {}", marker.display());
        let hook = Hook::new(Some(cmd));

        hook.trigger().await;

        wait_for(
            || std::fs::read(&marker).map(|b| !b.is_empty()).unwrap_or(false),
            Duration::from_secs(2),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let bytes = std::fs::read(&marker).unwrap_or_default();
        assert_eq!(bytes.len(), 1);
    }
}
