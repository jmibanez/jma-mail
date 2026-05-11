#!/usr/bin/env bash
# Compare per-cell power impact, CPU time, and wall clock between two
# jma builds across the four cells of an A/B benchmark:
#
#   * BEFORE binary, fresh state DB (initial sync)
#   * BEFORE binary, populated state DB (steady state)
#   * AFTER binary,  fresh state DB (initial sync)
#   * AFTER binary,  populated state DB (steady state)
#
# For each cell the script:
#   1. Starts `powermetrics --samplers tasks --show-process-energy`
#      in the background, sampling every $SAMPLE_MS ms, writing to a
#      per-cell log file.
#   2. Runs the configured subcommand (`pull` by default) under
#      `/usr/bin/time -l` to capture user+sys CPU time and wall clock.
#   3. Stops powermetrics, then sums the Energy Impact column across
#      every sample row whose first field is `jma`.
#
# Energy Impact is Apple's unitless score (not joules); useful for
# comparing two builds running the same workload back-to-back, not
# for absolute power figures. CPU time is the cleaner direct number
# for CPU-bound work; energy impact captures wakeups / GPU / etc.
#
# Usage:
#   tools/bench-power.sh <before-commit> <after-commit> \
#                        <maildir-source> <account-email>
#
# All four positional arguments are required and have no defaults.
# `<before-commit>` and `<after-commit>` are anything `git rev-parse`
# accepts (full SHA, short SHA, branch, tag, `HEAD~3`, ...).
#
# powermetrics requires root. The script primes sudo credentials at
# the start (you will be prompted once) and keeps the ticket alive
# in the background until exit.
#
# The script builds each binary by checking out the target commit
# and running `cargo build --release`, then copies the artifact to
# `$BENCH_DIR/jma-<full-sha>`. Subsequent runs reuse the cached
# binary if it's already present; delete the file to force a rebuild.
# The user's working ref is restored at the end (and on interrupt /
# build failure via an EXIT trap), but the script refuses to start
# if the working tree has tracked uncommitted changes.
#
# Environment overrides:
#   BENCH_DIR   Scratch dir for cached binaries, maildir copy, state
#               DB, logs.
#               Default: /tmp/jma-bench
#   SUBCMD      jma subcommand to benchmark.
#               Default: pull
#   SAMPLE_MS   powermetrics sampling interval in milliseconds.
#               Default: 1000
#
# Setup of the maildir + config is idempotent: the maildir is
# copied and the config written only if missing. Delete $BENCH_DIR
# to force a fresh setup (will also drop cached binaries).
#
# Auth flows through the existing keychain entry under
# `jma-bearer`/`default`; the script does not manage tokens.
# Run `jma auth set-token` first if needed.

set -euo pipefail

usage() {
    cat >&2 <<EOF
usage: $0 <before-commit> <after-commit> <maildir-source> <account-email>

Set BENCH_DIR, SUBCMD, or SAMPLE_MS env vars to override the scratch
dir, subcommand, or sampling interval. See the comment block at the
top of $0 for details.
EOF
    exit 1
}

if [[ $# -ne 4 ]]; then
    usage
fi

BEFORE_REF="$1"
AFTER_REF="$2"
MAILDIR_SOURCE="$3"
ACCOUNT_EMAIL="$4"

BENCH_DIR="${BENCH_DIR:-/tmp/jma-bench}"
# Expand a leading ~ in BENCH_DIR. Bash expands tilde in plain
# `VAR=~/foo` assignments, but a quoted value (`VAR='~/foo'`) or
# one inherited from a different shell context arrives literal,
# and the script would then create a directory called `~` rather
# than landing under $HOME.
BENCH_DIR="${BENCH_DIR/#~/$HOME}"
SUBCMD="${SUBCMD:-pull}"
SAMPLE_MS="${SAMPLE_MS:-1000}"

# Must be inside a git checkout to resolve commits and switch refs.
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null) || {
    echo "not inside a git repository; run from the jma checkout" >&2
    exit 1
}
cd "$REPO_ROOT"

if [[ ! -d "$MAILDIR_SOURCE" ]]; then
    echo "maildir source not a directory: $MAILDIR_SOURCE" >&2
    exit 1
fi

if ! command -v powermetrics >/dev/null 2>&1; then
    echo "powermetrics not on PATH; this script only runs on macOS" >&2
    exit 1
fi

# Resolve refs to full SHAs so cached binaries are unambiguous.
resolve_sha() {
    git rev-parse --verify "${1}^{commit}" 2>/dev/null || {
        echo "cannot resolve commit ref: $1" >&2
        exit 1
    }
}

BEFORE_SHA=$(resolve_sha "$BEFORE_REF")
AFTER_SHA=$(resolve_sha "$AFTER_REF")

# Pretty subject for the resolved-commits printout.
subject_of() {
    git --no-pager log -1 --format=%s "$1"
}

echo "=== resolved commits ==="
printf "  BEFORE: %s (%s) -- %s\n" "$BEFORE_REF" "${BEFORE_SHA:0:8}" "$(subject_of "$BEFORE_SHA")"
printf "  AFTER:  %s (%s) -- %s\n" "$AFTER_REF"  "${AFTER_SHA:0:8}"  "$(subject_of "$AFTER_SHA")"
echo

# Refuse to run if tracked files are modified or staged. Untracked
# files are fine -- they don't move under `git checkout`.
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "working tree has uncommitted tracked changes; commit or stash first" >&2
    exit 1
fi

# Remember the user's current ref so the trap can restore it. If we
# were on a branch, store the branch name; if detached, store the
# SHA. Empty ORIG_REF means no restore needed (or already restored).
ORIG_REF=$(git symbolic-ref --quiet --short HEAD 2>/dev/null || git rev-parse HEAD)

# powermetrics requires root. Prime sudo once up front so the four
# benchmark cells don't each block on a password prompt, and keep
# the ticket alive in the background.
echo "=== sudo (for powermetrics) ==="
if ! sudo -v; then
    echo "sudo authentication failed; powermetrics needs root" >&2
    exit 1
fi
( while true; do
    sudo -n true 2>/dev/null || exit
    sleep 60
    kill -0 "$$" 2>/dev/null || exit
  done ) &
SUDO_KEEPALIVE_PID=$!
echo

restore_ref() {
    if [[ -n "${ORIG_REF:-}" ]]; then
        ( cd "$REPO_ROOT" && git checkout --quiet "$ORIG_REF" ) || true
        ORIG_REF=""
    fi
}

cleanup() {
    if [[ -n "${SUDO_KEEPALIVE_PID:-}" ]]; then
        kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
        SUDO_KEEPALIVE_PID=""
    fi
    restore_ref
}
trap cleanup EXIT INT TERM

mkdir -p "$BENCH_DIR"

# Build a binary for the given full SHA into the scratch cache.
# Returns success if the cached file is present (rebuilds when not).
build_for() {
    local sha="$1"
    local target="$BENCH_DIR/jma-$sha"

    if [[ -x "$target" ]]; then
        echo "  cached: jma-${sha:0:8} -> $target"
        return 0
    fi

    echo "  building jma-${sha:0:8} ..."
    git checkout --quiet "$sha"

    # The package/bin name has shifted across history (jmapsync ->
    # jma-mail, with the bin variously named jma or jmapsync). Nuke
    # top-level target/release/ executables first so that after the
    # build, whatever single binary exists is unambiguously this
    # commit's output -- otherwise a stale `target/release/jma` from
    # a prior dev build would get copied into the BEFORE cache slot.
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

echo "=== build / cache ==="
build_for "$BEFORE_SHA"
build_for "$AFTER_SHA"
echo

# Restore the user's ref now (rather than waiting for the trap) so
# the benchmark cells run with the working tree at the user's
# original commit, not at AFTER's tree. restore_ref clears ORIG_REF
# so the EXIT trap's cleanup is a no-op once we cd into $BENCH_DIR.
restore_ref

BEFORE_BIN="$BENCH_DIR/jma-$BEFORE_SHA"
AFTER_BIN="$BENCH_DIR/jma-$AFTER_SHA"

# --- maildir / config setup ---

cd "$BENCH_DIR"

if [[ ! -d maildir ]]; then
    echo "=== copying $MAILDIR_SOURCE -> $BENCH_DIR/maildir ==="
    cp -R "$MAILDIR_SOURCE" maildir
    echo
fi

# Always (re-)write the config so a config-shape change in the
# binaries doesn't require manual sync.
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

reset_state() {
    # State DB location is pinned via [state].db_path in config.toml
    # above, so both binaries write to $BENCH_DIR/state.db regardless
    # of their built-in default (pre-76eef97 defaulted to
    # ~/.local/share/jmapsync/state.db, post-76eef97 to
    # <maildir>/.jma.db). Clear the pinned location plus the maildir-
    # level advisory lock that maildir_ops::lock writes at the
    # maildir root post-rename.
    rm -f state.db* maildir/.jma.lock
}

# Sum the Energy Impact column of every powermetrics task row whose
# first field looks like our jma binary. The cached binaries are
# named `jma-<full-sha>`, but the kernel's comm field truncates at
# 16 chars (MAXCOMLEN), so powermetrics shows `jma-<short-sha>`.
# Match `jma` exactly or `jma-...` by prefix.
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

run() {
    local label="$1"
    local binary="$2"
    local timelog="time-${label}.txt"
    local pmlog="power-${label}.txt"

    echo "=== $label ==="

    # Start powermetrics in the background. -o truncates the log; we
    # redirect stdout/stderr to discard the "Sampled system activity"
    # banner.
    sudo -n powermetrics \
        --samplers tasks \
        --show-process-energy \
        -i "$SAMPLE_MS" \
        -o "$pmlog" \
        >/dev/null 2>&1 &
    local pm_pid=$!

    # Give powermetrics a moment to produce its first sample so very
    # short jma runs aren't entirely missed. One sample interval plus
    # a little slack.
    sleep "$(awk -v ms="$SAMPLE_MS" 'BEGIN { printf "%.2f", (ms / 1000) + 0.2 }')"

    local jma_status=0
    /usr/bin/time -l "$binary" -c config.toml "$SUBCMD" > "$timelog" 2>&1 || jma_status=$?

    # Stop powermetrics. It's root-owned, so kill via sudo. INT lets
    # it flush its final sample; wait for the pid to reap.
    sudo -n kill -INT "$pm_pid" 2>/dev/null || true
    wait "$pm_pid" 2>/dev/null || true

    if [[ $jma_status -ne 0 ]]; then
        echo "  FAILED (exit $jma_status) -- see ${BENCH_DIR}/${timelog}"
        tail -10 "$timelog"
        echo "  time log:        ${BENCH_DIR}/${timelog}"
        echo "  power log:       ${BENCH_DIR}/${pmlog}"
        echo
        return
    fi

    local real user sys summary energy_pair energy samples
    real=$(awk '/real.*user.*sys/ {print $1; exit}' "$timelog")
    user=$(awk '/real.*user.*sys/ {print $3; exit}' "$timelog")
    sys=$(awk '/real.*user.*sys/ {print $5; exit}' "$timelog")
    summary=$(grep -E "Initial sync complete|Sync complete|Already in sync" "$timelog" | tail -1 || echo "(no summary line)")
    energy_pair=$(sum_jma_energy "$pmlog")
    energy=${energy_pair% *}
    samples=${energy_pair##* }

    echo "  wall clock:      ${real}s"
    echo "  CPU user/sys:    ${user}s / ${sys}s"
    echo "  energy impact:   ${energy} (sum over ${samples} jma samples @ ${SAMPLE_MS}ms)"
    echo "  outcome:         ${summary}"
    echo "  time log:        ${BENCH_DIR}/${timelog}"
    echo "  power log:       ${BENCH_DIR}/${pmlog}"
    echo
}

# Pair 1: BEFORE binary, fresh state DB then steady state.
reset_state
run before-initial "$BEFORE_BIN"
run before-steady  "$BEFORE_BIN"

# Pair 2: AFTER binary, fresh state DB then steady state.
reset_state
run after-initial  "$AFTER_BIN"
run after-steady   "$AFTER_BIN"

echo "=== summary ==="
printf "  %-18s  %10s  %10s  %10s  %14s\n" "scenario" "wall" "user" "sys" "energy impact"
printf "  %-18s  %10s  %10s  %10s  %14s\n" "--------" "----" "----" "---" "-------------"
for label in before-initial before-steady after-initial after-steady; do
    timelog="time-${label}.txt"
    pmlog="power-${label}.txt"
    real=$(awk '/real.*user.*sys/ {print $1; exit}' "$timelog" 2>/dev/null || echo "")
    user=$(awk '/real.*user.*sys/ {print $3; exit}' "$timelog" 2>/dev/null || echo "")
    sys=$(awk '/real.*user.*sys/ {print $5; exit}' "$timelog" 2>/dev/null || echo "")
    if [[ -f "$pmlog" ]]; then
        energy=$(sum_jma_energy "$pmlog")
        energy=${energy% *}
    else
        energy="n/a"
    fi
    printf "  %-18s  %9ss  %9ss  %9ss  %14s\n" \
        "$label" "${real:-n/a}" "${user:-n/a}" "${sys:-n/a}" "${energy:-n/a}"
done
