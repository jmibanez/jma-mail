# Shared helpers for tools/bench-*.sh -- sourced (not executed)
# by each bench script so the boilerplate (arg parsing, git ref
# resolution, binary-build cache, maildir + config setup, state
# reset, powermetrics row summing) lives in exactly one place.
#
# Each helper either:
#   - takes its inputs as positional args and writes to stdout, or
#   - mutates a documented set of globals the caller is expected to
#     have set up beforehand. The globals model keeps the function
#     signatures terse for callers that already have the values in
#     scope, at the cost of an implicit contract -- see the per-
#     function comments below for what each one reads or writes.
#
# Globals consumed across this file:
#   BENCH_DIR        Scratch dir for cached binaries + maildir + state
#   REPO_ROOT        Top of the jma working tree (git rev-parse output)
#   MAILDIR_SOURCE   Real-account-mode maildir to copy in; empty in
#                    testcontainer mode
#   ACCOUNT_EMAIL    Real-account-mode bearer-token owner; empty in
#                    testcontainer mode
#   TESTCONTAINER_MODE  1 = testcontainer fixture, 0 = real account
#   ORIG_REF         User's pre-script git ref (for restore_ref)
#   JMA_BENCH_*      Testcontainer fixture coordinates (set by
#                    _testcontainer.sh::testcontainer_start)

# Parse the 2-or-4 positional arg shape used by every bench script:
#
#   <before-ref> <after-ref>
#       Testcontainer mode -- script seeds its own fixture.
#
#   <before-ref> <after-ref> <maildir-source> <account-email>
#       Real-account mode -- script copies <maildir-source> into
#       $BENCH_DIR/maildir and pulls against the live account.
#
# Returns 0 and sets BEFORE_REF / AFTER_REF / MAILDIR_SOURCE /
# ACCOUNT_EMAIL / TESTCONTAINER_MODE on success. Returns 1 on a
# wrong arg count so the caller can route into its per-script
# usage() text.
parse_bench_args() {
    if (( $# != 2 && $# != 4 )); then
        return 1
    fi
    BEFORE_REF="$1"
    AFTER_REF="$2"
    if (( $# == 4 )); then
        MAILDIR_SOURCE="$3"
        ACCOUNT_EMAIL="$4"
        TESTCONTAINER_MODE=0
    else
        MAILDIR_SOURCE=""
        ACCOUNT_EMAIL=""
        TESTCONTAINER_MODE=1
    fi
    return 0
}

# Resolve a git ref to a full commit SHA. Errors loudly if the ref
# doesn't resolve to a commit so the script doesn't quietly cache
# the wrong binary under an ambiguous name.
resolve_sha() {
    git rev-parse --verify "${1}^{commit}" 2>/dev/null || {
        echo "cannot resolve commit ref: $1" >&2
        exit 1
    }
}

# Subject line of the given commit, used for the resolved-commits
# printout each bench script emits at startup.
subject_of() {
    git --no-pager log -1 --format=%s "$1"
}

# Print the "=== resolved commits ===" block. Takes the four pieces
# explicitly rather than reading globals so the caller doesn't have
# to name them BEFORE_REF / BEFORE_SHA / AFTER_REF / AFTER_SHA --
# any local with the right value works.
print_resolved_commits() {
    local before_ref="$1"
    local before_sha="$2"
    local after_ref="$3"
    local after_sha="$4"
    echo "=== resolved commits ==="
    printf "  BEFORE: %s (%s) -- %s\n" "$before_ref" "${before_sha:0:8}" "$(subject_of "$before_sha")"
    printf "  AFTER:  %s (%s) -- %s\n" "$after_ref"  "${after_sha:0:8}"  "$(subject_of "$after_sha")"
    echo
}

# Refuse to run if tracked files are modified or staged. Untracked
# files are fine -- they don't move under `git checkout`. The bench
# scripts switch refs to build BEFORE/AFTER binaries; a dirty tree
# would either block the checkout or get its changes carried into
# the wrong commit.
require_clean_worktree() {
    if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "working tree has uncommitted tracked changes; commit or stash first" >&2
        exit 1
    fi
}

# Build the jma binary for the given full SHA into the scratch
# cache at $BENCH_DIR/jma-<sha>. No-op if the cached file already
# exists -- delete it to force a rebuild.
#
# The package/bin name has shifted across history (jmapsync ->
# jma-mail, with the bin variously named jma or jmapsync). Nuke
# top-level target/release/ executables first so that after the
# build, whatever single binary exists is unambiguously this
# commit's output -- otherwise a stale `target/release/jma` from
# a prior dev build would get copied into the BEFORE cache slot.
build_for() {
    local sha="$1"
    local target="$BENCH_DIR/jma-$sha"

    if [[ -x "$target" ]]; then
        echo "  cached: jma-${sha:0:8} -> $target"
        return 0
    fi

    echo "  building jma-${sha:0:8} ..."
    git checkout --quiet "$sha"

    find "$REPO_ROOT/target/release" -maxdepth 1 -type f -perm +111 -delete 2>/dev/null || true

    CC=/usr/bin/cc cargo build --release

    local built
    built=$(find "$REPO_ROOT/target/release" -maxdepth 1 -type f -perm +111 2>/dev/null | head -1)
    if [[ -z "$built" ]]; then
        echo "  no executable produced under target/release/" >&2
        return 1
    fi
    cp "$built" "$target"
    chmod +x "$target"
    echo "  built: $target (from $(basename "$built"))"
}

# Pop back to the user's pre-script working ref. Called both
# directly (mid-script, after binaries are built, so the bench
# cells run against the user's original tree) and from the EXIT
# trap (so a crash or interrupt restores the ref too). Idempotent
# via ORIG_REF clearing.
restore_ref() {
    if [[ -n "${ORIG_REF:-}" ]]; then
        ( cd "$REPO_ROOT" && git checkout --quiet "$ORIG_REF" ) || true
        ORIG_REF=""
    fi
}

# Set up the bench maildir under $BENCH_DIR. In testcontainer mode
# every cell starts with an empty maildir (the server is the source
# of truth and the cell measures a true initial download). In real-
# account mode we copy from MAILDIR_SOURCE on first run only;
# subsequent runs reuse the existing copy.
setup_maildir() {
    if (( TESTCONTAINER_MODE )); then
        rm -rf maildir
        mkdir -p maildir
    elif [[ ! -d maildir ]]; then
        echo "=== copying $MAILDIR_SOURCE -> $BENCH_DIR/maildir ==="
        cp -R "$MAILDIR_SOURCE" maildir
        echo
    fi
}

# Write the bench config.toml in $BENCH_DIR with chmod 600. In
# testcontainer mode the bearer + session URL are pinned so jma
# bypasses keychain + autodiscovery; in real-account mode neither
# field is set and jma falls back to the keychain entry.
#
# jma's Config::load refuses to start when [account].token is set
# in a config file with group/other perm bits (mode & 0o077 != 0;
# see 808c151). In testcontainer mode the bearer is in
# [account].token so the chmod is hard-required; in real-account
# mode the token lives in the keychain and the file has nothing
# sensitive, but we chmod the same way regardless for consistency.
write_bench_config() {
    if (( TESTCONTAINER_MODE )); then
        cat > config.toml <<EOF
[account]
email = "$JMA_BENCH_ACCOUNT_EMAIL"
token = "$JMA_BENCH_BEARER"
session_url = "$JMA_BENCH_SESSION_URL"

[sync]
maildir_path = "$BENCH_DIR/maildir"
mailboxes = []
download_concurrency = 8

[state]
db_path = "$BENCH_DIR/state.db"
EOF
    else
        cat > config.toml <<EOF
[account]
email = "$ACCOUNT_EMAIL"

[sync]
maildir_path = "$BENCH_DIR/maildir"
mailboxes = []
download_concurrency = 8

[state]
db_path = "$BENCH_DIR/state.db"
EOF
    fi
    chmod 600 config.toml
}

# Wipe the state DB (and, in testcontainer mode, the maildir) so
# the next cell starts from a fresh-state-DB baseline. The state DB
# location is pinned via [state].db_path in config.toml above, so
# both binaries write to $BENCH_DIR/state.db regardless of their
# built-in default (pre-76eef97 defaulted to
# ~/.local/share/jmapsync/state.db, post-76eef97 to
# <maildir>/.jma.db). Clear the pinned location plus the maildir-
# level advisory lock that maildir_ops::lock writes at the maildir
# root post-rename.
reset_state() {
    rm -f state.db* maildir/.jma.lock
    if (( TESTCONTAINER_MODE )); then
        rm -rf maildir
        mkdir -p maildir
    fi
}

# Sum the Energy Impact column of every powermetrics task row whose
# first field looks like our jma binary. Used by bench-power and
# bench-power-watch; declared in common so a future format change
# (e.g. powermetrics swapping the energy-impact column position)
# gets fixed once.
#
# The cached binaries are named `jma-<full-sha>`, but the kernel's
# comm field truncates at 16 chars (MAXCOMLEN), so powermetrics
# shows `jma-<short-sha>`. Match `jma` exactly or `jma-...` by
# prefix.
#
# With `--samplers tasks --show-process-energy` the Energy Impact
# value is the last numeric column of the row (the Deadlines and
# Wakeups groups contain comma-glued tokens that don't parse as
# pure numbers), so scan from the right for the first pure-numeric
# token and accumulate.
sum_jma_energy() {
    local pmlog="$1"
    awk '
        $1 ~ /^jma(-|$)/ {
            for (i = NF; i >= 1; i--) {
                if ($i ~ /^[0-9]+(\.[0-9]+)?$/) {
                    total += $i
                    samples += 1
                    break
                }
            }
        }
        END {
            printf "%.2f %d", total + 0, samples + 0
        }
    ' "$pmlog"
}
