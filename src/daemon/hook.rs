use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, error, info, warn};

/// One invocation of the post-arrival command. Returned futures are
/// driven to completion by `run_loop` and resolve to `true` on success
/// (clean exit) or `false` on any failure (spawn error, non-zero exit,
/// wait error). The command is passed as `Arc<str>` so the retry loop
/// can hand it to successive attempts via a refcount bump instead of
/// reallocating the string per attempt.
type CommandRunner =
    Arc<dyn Fn(Arc<str>) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

/// Hard ceiling on `post_arrival_command_retries`. Values larger than
/// this are clamped on `Hook` construction (with a warn log) rather
/// than honored -- a steady-state failure that retries hundreds of
/// times would block every follow-up trigger for the duration of the
/// retry chain, which is worse than dropping the trigger and surfacing
/// the failure to the user.
pub const MAX_POST_ARRIVAL_RETRIES: u32 = 10;

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
    command: Option<Arc<str>>,
    /// Number of retries after the first failed attempt. 0 means
    /// run-once-and-log; N means up to N+1 total attempts before
    /// giving up on this trigger.
    retries: u32,
    state: Mutex<State>,
    /// Fired by `run_loop` whenever it transitions `running` from true
    /// to false. Test helpers park on this to wait for an invocation
    /// (including its retry chain) to fully settle without polling.
    idle: Notify,
    runner: CommandRunner,
}

#[derive(Default)]
struct State {
    running: bool,
    pending: bool,
}

impl Hook {
    pub fn new(command: Option<String>, retries: u32) -> Self {
        Self::with_runner(
            command.map(Arc::<str>::from),
            clamp_retries(retries),
            Arc::new(|cmd| Box::pin(async move { run_shell(&cmd).await })),
        )
    }

    /// Construct with a custom command runner. Production code uses
    /// `new()`, which wires up the shell runner; tests inject a runner
    /// that synchronises on a Notify (no subprocess, no sleeps) so
    /// coalescing assertions are deterministic in millisecond budgets.
    fn with_runner(command: Option<Arc<str>>, retries: u32, runner: CommandRunner) -> Self {
        Self {
            inner: Arc::new(Inner {
                command,
                retries,
                state: Mutex::new(State::default()),
                idle: Notify::new(),
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

    /// Test-only: park until the hook's run_loop has set `running =
    /// false`, i.e. no invocation (including its retry chain) is in
    /// flight. Lets retry tests fire a fresh `trigger()` without
    /// racing the prior run_loop's exit.
    ///
    /// The loop guards against a spurious wake-up from a stale permit
    /// left by a previous idle transition the test didn't consume:
    /// `Notify` only stores one permit, so the worst case is one extra
    /// state check before the real transition.
    #[cfg(test)]
    async fn wait_idle(&self) {
        loop {
            {
                let state = self.inner.state.lock().await;
                if !state.running {
                    return;
                }
            }
            self.inner.idle.notified().await;
        }
    }

    /// Request that the post-arrival command run. Returns immediately;
    /// the actual command runs on a tokio task. Coalesces with any
    /// in-flight or already-pending run.
    pub async fn trigger(&self) {
        let cmd = match &self.inner.command {
            Some(c) => Arc::clone(c),
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
///
/// Each "run" includes up to `inner.retries` retries after a failed
/// attempt, all inside one logical invocation -- triggers that arrive
/// during a retry chain still coalesce into a single follow-up, the
/// same as triggers that arrive during the first attempt.
async fn run_loop(inner: Arc<Inner>, cmd: Arc<str>) {
    loop {
        run_with_retries(&inner, &cmd).await;

        let mut state = inner.state.lock().await;
        if state.pending {
            state.pending = false;
            // running stays true; loop and run again.
            drop(state);
        } else {
            state.running = false;
            drop(state);
            inner.idle.notify_one();
            return;
        }
    }
}

async fn run_with_retries(inner: &Inner, cmd: &Arc<str>) {
    info!("Running post-arrival hook: {}", cmd);
    let total_attempts = inner.retries.saturating_add(1);
    let mut attempt = 0u32;
    loop {
        if (inner.runner)(Arc::clone(cmd)).await {
            return;
        }
        if attempt >= inner.retries {
            if inner.retries > 0 {
                warn!(
                    "Post-arrival hook failed after {} attempts (1 initial + {} retr{}); giving up",
                    total_attempts,
                    inner.retries,
                    if inner.retries == 1 { "y" } else { "ies" },
                );
            }
            return;
        }
        attempt += 1;
        debug!(
            "Post-arrival hook attempt failed; retrying ({}/{})",
            attempt, inner.retries,
        );
    }
}

fn clamp_retries(requested: u32) -> u32 {
    if requested > MAX_POST_ARRIVAL_RETRIES {
        warn!(
            "post_arrival_command_retries={} exceeds the cap of {}; clamping",
            requested, MAX_POST_ARRIVAL_RETRIES,
        );
        MAX_POST_ARRIVAL_RETRIES
    } else {
        requested
    }
}

async fn run_shell(cmd: &str) -> bool {
    let mut child = match Command::new("sh").arg("-c").arg(cmd).spawn() {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to spawn post-arrival hook ({}): {}", cmd, e);
            return false;
        }
    };
    match child.wait().await {
        Ok(status) if status.success() => {
            info!("Post-arrival hook finished cleanly");
            true
        }
        Ok(status) => {
            warn!("Post-arrival hook exited with {}", status);
            false
        }
        Err(e) => {
            error!("Failed to await post-arrival hook: {}", e);
            false
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
            Arc::new(move |_cmd: Arc<str>| {
                let count = count.clone();
                let started = started.clone();
                let release = release.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    release.notified().await;
                    true
                })
            })
        }

        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn disabled_hook_is_a_noop() {
        let hook = Hook::new(None, 0);
        assert!(!hook.is_enabled());
        hook.trigger().await;
    }

    #[tokio::test]
    async fn coalesces_bursts_into_at_most_two_runs() {
        let gate = Gate::new();
        let hook = Hook::with_runner(Some(Arc::from("noop")), 0, gate.runner());

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
        let hook = Hook::with_runner(Some(Arc::from("noop")), 0, gate.runner());

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

    /// Flaky runner: fails the first `fail_first` attempts, succeeds
    /// afterwards. Pushes one tick per attempt onto `tx` so tests can
    /// wait deterministically without sleeping.
    fn flaky_runner(
        count: Arc<AtomicUsize>,
        tx: tokio::sync::mpsc::UnboundedSender<()>,
        fail_first: usize,
    ) -> CommandRunner {
        Arc::new(move |_cmd: Arc<str>| {
            let count = count.clone();
            let tx = tx.clone();
            Box::pin(async move {
                let n = count.fetch_add(1, Ordering::SeqCst);
                let _ = tx.send(());
                n >= fail_first
            })
        })
    }

    #[tokio::test]
    async fn retries_until_success() {
        // Fails the first two attempts, succeeds on the third. With
        // retries=3 there's headroom past the third attempt, so the
        // hook should stop as soon as success lands.
        let count = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let hook = Hook::with_runner(
            Some(Arc::from("noop")),
            3,
            flaky_runner(count.clone(), tx, 2),
        );

        hook.trigger().await;
        rx.recv().await.unwrap();
        rx.recv().await.unwrap();
        rx.recv().await.unwrap();
        hook.wait_idle().await;
        assert_eq!(count.load(Ordering::SeqCst), 3);
        assert!(rx.try_recv().is_err(), "no extra attempts past success");
    }

    #[tokio::test]
    async fn retries_capped_at_configured_count() {
        // Always fails. retries=2 -> at most 3 attempts (1 initial + 2
        // retries), then the hook gives up.
        let count = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let hook = Hook::with_runner(
            Some(Arc::from("noop")),
            2,
            flaky_runner(count.clone(), tx, usize::MAX),
        );

        hook.trigger().await;
        rx.recv().await.unwrap();
        rx.recv().await.unwrap();
        rx.recv().await.unwrap();
        hook.wait_idle().await;
        assert_eq!(count.load(Ordering::SeqCst), 3);
        assert!(rx.try_recv().is_err(), "no extra attempts past the cap");
    }

    #[tokio::test]
    async fn default_retries_zero_runs_once_on_failure() {
        let count = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let hook = Hook::with_runner(
            Some(Arc::from("noop")),
            0,
            flaky_runner(count.clone(), tx, usize::MAX),
        );

        hook.trigger().await;
        rx.recv().await.unwrap();
        hook.wait_idle().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(
            rx.try_recv().is_err(),
            "retries=0 means exactly one attempt"
        );
    }

    #[test]
    fn clamps_retries_above_cap() {
        assert_eq!(
            clamp_retries(MAX_POST_ARRIVAL_RETRIES + 1),
            MAX_POST_ARRIVAL_RETRIES,
        );
        assert_eq!(clamp_retries(u32::MAX), MAX_POST_ARRIVAL_RETRIES);
    }

    #[test]
    fn passes_retries_at_or_below_cap() {
        assert_eq!(clamp_retries(0), 0);
        assert_eq!(clamp_retries(1), 1);
        assert_eq!(
            clamp_retries(MAX_POST_ARRIVAL_RETRIES),
            MAX_POST_ARRIVAL_RETRIES,
        );
    }
}
