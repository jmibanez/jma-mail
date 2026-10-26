//! One flag-policy decision for ordinary updates, adoption and moves.

use crate::config::ConflictStrategy;
use crate::state::flag_sync::{FlagSyncState, mapped_flags};

pub(crate) struct FlagDecision {
    pub target: Option<String>,
    pub state: FlagSyncState,
}

pub(crate) fn decide(
    original: &FlagSyncState,
    local: &str,
    remote: &str,
    strategy: ConflictStrategy,
) -> FlagDecision {
    let local = mapped_flags(local);
    let remote = mapped_flags(remote);
    let mut state = original.clone();
    state.legacy = false;

    if local == remote {
        state = FlagSyncState {
            baseline: Some(local.clone()),
            ..Default::default()
        };
        return FlagDecision {
            target: Some(local),
            state,
        };
    }

    // A successful old generation may be followed by a newer local edit
    // before its DB acknowledgement. Matching the retained receipt's target
    // confirms that generation; the newer filename is a new local change.
    if let Some(attempt) = &state.attempt
        && remote == attempt.target_flags
        && mapped_flags(&attempt.source_flags) == attempt.target_flags
        && local != mapped_flags(&attempt.source_flags)
    {
        state.baseline = Some(remote.clone());
        state.attempt = None;
        state.blocked = false;
        state.error = None;
    }

    // Old software could acknowledge a change it never sent. Do not
    // silently choose a winner for a legacy mismatch with unknown history.
    if state.review
        || (original.legacy
            && state
                .baseline
                .as_ref()
                .is_some_and(|baseline| baseline != &remote))
    {
        state.review = true;
        state.blocked = true;
        state.error.get_or_insert_with(|| {
            "Flag histories require review; choose an explicit repair authority".into()
        });
        return FlagDecision {
            target: None,
            state,
        };
    }

    // A pull may have renamed the file before its ACK transaction failed.
    // If the server has since changed, that old target is not evidence of an
    // independent MUA edit. Values alone cannot distinguish our effect from
    // a coincident local edit, so do not repush it without explicit authority.
    let unacknowledged_pull_target = state
        .attempt
        .as_ref()
        .filter(|attempt| {
            mapped_flags(&attempt.source_flags) != attempt.target_flags
                && local == attempt.target_flags
                && remote != attempt.target_flags
        })
        .map(|attempt| attempt.target_flags.clone());
    let newer_local_edit = state
        .attempt
        .as_ref()
        .is_some_and(|attempt| local != mapped_flags(&attempt.source_flags));
    let target = match state.baseline.as_deref() {
        Some(baseline) if remote == baseline => local,
        Some(baseline) if local == baseline && !newer_local_edit => remote,
        _ => match strategy {
            ConflictStrategy::LocalWins => local,
            ConflictStrategy::ServerWins => remote,
        },
    };
    if unacknowledged_pull_target.as_ref() == Some(&target) {
        state.review = true;
        state.blocked = true;
        state.error = Some("Unacknowledged pull target may be our own rename; choose explicit flag authority before pushing".into());
        return FlagDecision {
            target: None,
            state,
        };
    }
    FlagDecision {
        target: Some(target),
        state,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::flag_sync::FlagAttempt;

    fn settled(flags: &str) -> FlagSyncState {
        FlagSyncState {
            baseline: Some(flags.into()),
            ..Default::default()
        }
    }

    #[test]
    fn one_sided_changes_do_not_become_conflicts() {
        assert_eq!(
            decide(&settled("FS"), "S", "FS", ConflictStrategy::ServerWins)
                .target
                .as_deref(),
            Some("S")
        );
        assert_eq!(
            decide(&settled("S"), "S", "FS", ConflictStrategy::LocalWins)
                .target
                .as_deref(),
            Some("FS")
        );
    }

    #[test]
    fn concurrent_changes_follow_the_existing_whole_flag_set_policy() {
        assert_eq!(
            decide(&settled(""), "S", "F", ConflictStrategy::ServerWins)
                .target
                .as_deref(),
            Some("F")
        );
        assert_eq!(
            decide(&settled(""), "S", "F", ConflictStrategy::LocalWins)
                .target
                .as_deref(),
            Some("S")
        );
    }

    #[test]
    fn an_ambiguous_old_write_does_not_hide_a_newer_local_edit() {
        let mut state = settled("");
        state.attempt = Some(FlagAttempt {
            source_flags: "S".into(),
            target_flags: "S".into(),
            source_maildir_id: None,
            source_folder: None,
        });
        let decision = decide(&state, "", "S", ConflictStrategy::ServerWins);
        assert_eq!(decision.target.as_deref(), Some(""));
        assert_eq!(decision.state.baseline.as_deref(), Some("S"));
        assert!(decision.state.attempt.is_none());
    }

    #[test]
    fn remote_target_alone_does_not_acknowledge_a_failed_local_pull() {
        let mut state = settled("S");
        state.attempt = Some(FlagAttempt {
            source_flags: "S".into(),
            target_flags: "FS".into(),
            source_maildir_id: None,
            source_folder: None,
        });
        let decision = decide(&state, "", "FS", ConflictStrategy::ServerWins);
        assert_eq!(decision.target.as_deref(), Some("FS"));
        assert_eq!(decision.state.baseline.as_deref(), Some("S"));
        assert!(decision.state.attempt.is_some());
    }

    #[test]
    fn unacknowledged_pull_target_cannot_authorize_a_fresh_push() {
        for (baseline, source, target, local, remote, strategy) in [
            ("", "X", "F", "FX", "S", ConflictStrategy::LocalWins),
            ("S", "SX", "FS", "FSX", "S", ConflictStrategy::ServerWins),
        ] {
            let mut state = settled(baseline);
            state.attempt = Some(FlagAttempt {
                source_flags: source.into(),
                target_flags: target.into(),
                source_maildir_id: None,
                source_folder: None,
            });
            let decision = decide(&state, local, remote, strategy);
            assert!(decision.target.is_none());
            assert!(decision.state.review && decision.state.blocked);
            assert_eq!(decision.state.baseline, state.baseline);
            assert_eq!(decision.state.attempt, state.attempt);
        }
    }

    #[test]
    fn unacknowledged_pull_does_not_block_a_safe_pull_or_distinct_mua_edit() {
        let mut state = settled("");
        state.attempt = Some(FlagAttempt {
            source_flags: "X".into(),
            target_flags: "F".into(),
            source_maildir_id: None,
            source_folder: None,
        });
        for (local, remote, strategy, expected) in [
            ("FX", "S", ConflictStrategy::ServerWins, "S"),
            ("TX", "S", ConflictStrategy::LocalWins, "T"),
        ] {
            let decision = decide(&state, local, remote, strategy);
            assert_eq!(decision.target.as_deref(), Some(expected));
            assert!(!decision.state.review);
            assert_eq!(decision.state.baseline, state.baseline);
            assert_eq!(decision.state.attempt, state.attempt);
        }
    }

    #[test]
    fn legacy_disagreement_is_not_silently_called_a_remote_change() {
        let mut state = settled("S");
        state.legacy = true;
        assert!(
            decide(&state, "S", "FS", ConflictStrategy::ServerWins)
                .target
                .is_none()
        );
    }
}
