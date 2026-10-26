//! One daemon-owned deadline for retryable message work, not a durable queue.

use std::collections::HashSet;
use std::time::Duration;
use tokio::time::Instant;

use crate::ids::JmapEmailId;
use crate::sync::engine::SyncOutcome;

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

pub(super) struct OperationRetry {
    deadline: Option<Instant>,
    backoff: Duration,
    blocked_remote_ids: HashSet<JmapEmailId>,
}

impl Default for OperationRetry {
    fn default() -> Self {
        Self {
            deadline: None,
            backoff: INITIAL_BACKOFF,
            blocked_remote_ids: HashSet::new(),
        }
    }
}

impl OperationRetry {
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    #[cfg(test)]
    fn is_due(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| deadline <= Instant::now())
    }

    pub fn begin_retry(&mut self) -> HashSet<JmapEmailId> {
        self.deadline = None;
        self.blocked_remote_ids.clone()
    }

    pub fn observe(&mut self, outcome: &SyncOutcome) {
        if outcome.retry_scope_checked {
            self.blocked_remote_ids
                .clone_from(&outcome.blocked_remote_ids);
        } else {
            self.blocked_remote_ids
                .extend(outcome.blocked_remote_ids.iter().cloned());
        }
        if outcome.needs_retry() {
            self.arm();
        } else if outcome.retry_scope_checked {
            self.pause();
        }
    }

    /// Reconnects retain this scheduler. Never replace an earlier deadline
    /// because an unrelated cycle or transport failure also noticed work.
    pub fn arm(&mut self) {
        if self.deadline.is_none() {
            self.deadline = Some(Instant::now() + self.backoff);
            self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
        }
    }

    pub fn pause(&mut self) {
        self.deadline = None;
        self.backoff = INITIAL_BACKOFF;
    }
}

pub(super) async fn wait_until_due(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending() -> SyncOutcome {
        SyncOutcome {
            retryable_remote_actions: 1,
            retry_scope_checked: true,
            ..Default::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn paths_noop_and_reconnect_do_not_clear_or_postpone_a_deadline() {
        let mut retry = OperationRetry::default();
        retry.observe(&pending());
        let deadline = retry.deadline().unwrap();
        tokio::time::advance(Duration::from_millis(500)).await;
        retry.observe(&SyncOutcome {
            already_in_sync: true,
            ..Default::default()
        });
        retry.arm(); // A transport failure/reconnect while the deadline is armed.
        assert_eq!(retry.deadline(), Some(deadline));
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(
            retry.is_due(),
            "the deadline survives a longer reconnect without another event"
        );
        retry.begin_retry();
        assert!(retry.deadline().is_none());
        retry.observe(&pending());
        assert_eq!(
            retry.deadline(),
            Some(Instant::now() + Duration::from_secs(2))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retries_back_off_to_a_cap_and_full_success_disarms_and_resets() {
        let mut retry = OperationRetry::default();
        for expected in [1, 2, 4, 8, 16, 32, 60, 60] {
            retry.observe(&pending());
            assert_eq!(
                retry.deadline(),
                Some(Instant::now() + Duration::from_secs(expected))
            );
            wait_until_due(retry.deadline()).await;
            assert!(retry.is_due());
            retry.begin_retry();
        }
        retry.observe(&SyncOutcome {
            retry_scope_checked: true,
            ..Default::default()
        });
        assert!(retry.deadline().is_none());
        retry.observe(&pending());
        assert_eq!(retry.deadline(), Some(Instant::now() + INITIAL_BACKOFF));
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_and_review_work_do_not_arm_a_timer_or_vanish_from_its_mask() {
        let mut retry = OperationRetry::default();
        let blocked = SyncOutcome {
            pending_flag_actions: 2,
            blocked_flag_actions: 2,
            blocked_remote_ids: HashSet::from(["blocked-move".into()]),
            deferred_actions: 1,
            retry_scope_checked: true,
            ..Default::default()
        };
        retry.observe(&blocked);
        assert!(retry.deadline().is_none());
        retry.observe(&SyncOutcome {
            retryable_remote_actions: 1,
            ..blocked.clone()
        });
        wait_until_due(retry.deadline()).await;
        assert_eq!(retry.begin_retry(), blocked.blocked_remote_ids);
        retry.observe(&blocked);
        assert!(
            retry.deadline().is_none(),
            "only permission/review work remains"
        );
    }
}
