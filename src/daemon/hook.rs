use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// One invocation of the post-arrival command. Returned futures are
/// driven to completion by `run_loop`. Boxed so `Hook` can own it
/// behind `Arc` without a generic parameter leaking through callers.
type CommandRunner = Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

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
    runner: CommandRunner,
}

#[derive(Default)]
struct State {
    running: bool,
    pending: bool,
}

impl Hook {
    pub fn new(command: Option<String>) -> Self {
        Self::with_runner(
            command,
            Arc::new(|cmd| Box::pin(async move { run_shell(&cmd).await })),
        )
    }

    /// Construct with a custom command runner. Production code uses
    /// `new()`, which wires up the shell runner; tests inject a runner
    /// that synchronises on a Notify (no subprocess, no sleeps) so
    /// coalescing assertions are deterministic in millisecond budgets.
    fn with_runner(command: Option<String>, runner: CommandRunner) -> Self {
        Self {
            inner: Arc::new(Inner {
                command,
                state: Mutex::new(State::default()),
                runner,
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
        (inner.runner)(cmd.clone()).await;

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

async fn run_shell(cmd: &str) {
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    /// Test runner that stands in for the shell. `started` fires once
    /// per invocation; the runner then awaits `release` before
    /// completing. Tests drive the state machine by hand instead of
    /// sleeping.
    struct Gate {
        count: Arc<AtomicUsize>,
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Gate {
        fn new() -> Self {
            Self {
                count: Arc::new(AtomicUsize::new(0)),
                started: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
            }
        }

        fn runner(&self) -> CommandRunner {
            let count = self.count.clone();
            let started = self.started.clone();
            let release = self.release.clone();
            Arc::new(move |_cmd| {
                let count = count.clone();
                let started = started.clone();
                let release = release.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    release.notified().await;
                })
            })
        }

        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn disabled_hook_is_a_noop() {
        let hook = Hook::new(None);
        assert!(!hook.is_enabled());
        hook.trigger().await;
    }

    #[tokio::test]
    async fn coalesces_bursts_into_at_most_two_runs() {
        let gate = Gate::new();
        let hook = Hook::with_runner(Some("noop".into()), gate.runner());

        // First trigger: spawns run_loop, runner enters and blocks on release.
        hook.trigger().await;
        gate.started.notified().await;
        assert_eq!(gate.count(), 1);

        // Four more triggers while the first is in flight: the second
        // raises pending, the rest must coalesce into it (no extra runs).
        for _ in 0..4 {
            hook.trigger().await;
        }

        // Release first run; run_loop should pick up pending=true and
        // start a single follow-up.
        gate.release.notify_one();
        gate.started.notified().await;
        assert_eq!(gate.count(), 2);

        // Release the follow-up; run_loop should now exit cleanly. No
        // further runs are expected. We wait one trigger() round-trip
        // through the same Mutex so the run_loop has had a chance to
        // settle, then assert.
        gate.release.notify_one();
        hook.trigger().await; // would observe running=false and respawn
        gate.started.notified().await;
        assert_eq!(gate.count(), 3, "post-burst trigger starts a fresh run");

        gate.release.notify_one();
    }

    #[tokio::test]
    async fn single_trigger_runs_once() {
        let gate = Gate::new();
        let hook = Hook::with_runner(Some("noop".into()), gate.runner());

        hook.trigger().await;
        gate.started.notified().await;
        assert_eq!(gate.count(), 1);

        gate.release.notify_one();
        // No second trigger fired, so no follow-up should ever start.
        // Round-trip through the Mutex via a quick is_enabled check
        // would be racy; instead, fire another trigger and verify the
        // count went from 1 to 2 (would be 3 if a phantom follow-up
        // had also run).
        hook.trigger().await;
        gate.started.notified().await;
        assert_eq!(gate.count(), 2);

        gate.release.notify_one();
    }
}
