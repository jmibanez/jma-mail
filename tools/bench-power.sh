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
#   BENCH_DIR         Scratch dir for cached binaries, maildir copy,
#                     state DB, logs.
#                     Default: /tmp/jma-bench
#   SUBCMD            jma subcommand to benchmark.
#                     Default: pull
#   SAMPLE_MS         powermetrics sampling interval in milliseconds.
#                     Default: 1000
#   BENCH_LOOP_COUNT  Number of rounds to run. Each round is one full
#                     quartet (BEFORE-initial, BEFORE-steady,
#                     AFTER-initial, AFTER-steady), so round-to-round
#                     noise affects each cell equally. Per-round logs
#                     land at time-<label>-r<round>.txt and
#                     power-<label>-r<round>.txt when this is >1; the
#                     summary then reports mean/stddev/min/max per
#                     cell for each metric.
#                     Default: 1
#
# Setup of the maildir + config is idempotent: the maildir is
# copied and the config written only if missing. Delete $BENCH_DIR
# to force a fresh setup (will also drop cached binaries).
#
# Auth flows through the existing keychain entry under
# `jma-bearer`/`default`; the script does not manage tokens.
# Run `jma auth set-token` first if needed.

set -euo pipefail

# shellcheck source=tools/_bench-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/_bench-common.sh"

usage() {
    cat >&2 <<EOF
usage: $0 <before-commit> <after-commit>
       $0 <before-commit> <after-commit> <maildir-source> <account-email>

Two-arg form spins up a Stalwart testcontainer fixture seeded from
a synthetic corpus; four-arg form is the original behavior against
a real JMAP account. BEFORE-initial semantics differ between modes
(empty maildir + full download vs. pre-populated maildir + fresh
state-DB re-scan); within a mode BEFORE-vs-AFTER stays
apples-to-apples.

Set BENCH_DIR, SUBCMD, or SAMPLE_MS env vars to override the scratch
dir, subcommand, or sampling interval. BENCH_LOOP_COUNT=N runs N
quartet rounds and the summary reports mean/stddev/min/max per cell.
Testcontainer mode also honours TESTCONTAINER_CORPUS_COUNT,
TESTCONTAINER_CORPUS_FOLDERS, TESTCONTAINER_CORPUS_SEED, and
TESTCONTAINER_NEW_PCT.
EOF
    exit 1
}

parse_bench_args "$@" || usage

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

if (( TESTCONTAINER_MODE == 0 )) && [[ ! -d "$MAILDIR_SOURCE" ]]; then
    echo "maildir source not a directory: $MAILDIR_SOURCE" >&2
    exit 1
fi

if ! command -v powermetrics >/dev/null 2>&1; then
    echo "powermetrics not on PATH; this script only runs on macOS" >&2
    exit 1
fi

BEFORE_SHA=$(resolve_sha "$BEFORE_REF")
AFTER_SHA=$(resolve_sha "$AFTER_REF")
print_resolved_commits "$BEFORE_REF" "$BEFORE_SHA" "$AFTER_REF" "$AFTER_SHA"

require_clean_worktree

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

# Source the testcontainer helper unconditionally (functions and
# defaults only; nothing fires until a function is called). Done
# before any ref-switching cargo operations so the file is loaded
# from the user's original tree.
# shellcheck source=tools/_testcontainer.sh
source "$REPO_ROOT/tools/_testcontainer.sh"

cleanup() {
    testcontainer_stop
    if [[ -n "${SUDO_KEEPALIVE_PID:-}" ]]; then
        kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
        SUDO_KEEPALIVE_PID=""
    fi
    restore_ref
}
trap cleanup EXIT INT TERM

mkdir -p "$BENCH_DIR"

echo "=== build / cache ==="
build_for "$BEFORE_SHA"
build_for "$AFTER_SHA"
echo

# Restore the user's ref now (rather than waiting for the trap) so
# the benchmark cells run with the working tree at the user's
# original commit, not at AFTER's tree. restore_ref clears ORIG_REF
# so the EXIT trap's cleanup is a no-op once we cd into $BENCH_DIR.
restore_ref

# Testcontainer setup happens after restore_ref so the example
# binaries we build live in the user's original tree (the bench-
# server example was introduced post-69c455a and won't exist at
# arbitrary BEFORE refs).
if (( TESTCONTAINER_MODE )); then
    echo "=== testcontainer setup ==="
    testcontainer_build_helpers
    testcontainer_start "$BENCH_DIR"
    echo
fi

BEFORE_BIN="$BENCH_DIR/jma-$BEFORE_SHA"
AFTER_BIN="$BENCH_DIR/jma-$AFTER_SHA"

# --- maildir / config setup ---

cd "$BENCH_DIR"
setup_maildir
write_bench_config

run() {
    local label="$1"
    local binary="$2"
    local timelog="time-${label}.txt"
    local pmlog="power-${label}.txt"
    local profile_json="profile-${label}.json"

    # Pass --profile-json only when the binary advertises it. BEFORE
    # commits predating this flag would error out on an unknown arg,
    # so probe `--help` once and fall through silently when the flag
    # is missing. The JSON file is a side-channel; the bench summary
    # below still reads RSS/wall-clock from `time -l` regardless.
    local profile_args=()
    if "$binary" --help 2>&1 | grep -q -- '--profile-json'; then
        rm -f "$profile_json"
        profile_args=(--profile-json "$profile_json")
    fi

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
    /usr/bin/time -l "$binary" -c config.toml "${profile_args[@]}" "$SUBCMD" > "$timelog" 2>&1 || jma_status=$?

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
    if [[ -s "$profile_json" ]]; then
        echo "  profile:     ${BENCH_DIR}/${profile_json}"
    fi
    echo "  power log:       ${BENCH_DIR}/${pmlog}"
    echo
}

# Quartet-grouped round body. See bench-rss.sh for the rationale on
# interleaving across rounds vs. running cells in pair-blocks.
bench_run_round() {
    local suffix="$1"
    prewarm_server "$BEFORE_BIN" "$suffix"
    reset_state
    run "before-initial${suffix}" "$BEFORE_BIN"
    run "before-steady${suffix}"  "$BEFORE_BIN"
    reset_state
    run "after-initial${suffix}"  "$AFTER_BIN"
    run "after-steady${suffix}"   "$AFTER_BIN"
}

run_loop bench_run_round

# Extractors for aggregate_cell: read one value from one log file.
# Wall / user / sys all come from the same `time -l` summary line.
extract_wall()   { awk '/real.*user.*sys/ {print $1; exit}' "$1"; }
extract_user()   { awk '/real.*user.*sys/ {print $3; exit}' "$1"; }
extract_sys()    { awk '/real.*user.*sys/ {print $5; exit}' "$1"; }
extract_energy() { sum_jma_energy "$1" | awk '{print $1}'; }

CELLS=(before-initial before-steady after-initial after-steady)

if (( BENCH_LOOP_COUNT == 1 )); then
    # Single-round summary: same shape as before the loop landed.
    echo "=== summary ==="
    printf "  %-18s  %10s  %10s  %10s  %14s\n" "scenario" "wall" "user" "sys" "energy impact"
    printf "  %-18s  %10s  %10s  %10s  %14s\n" "--------" "----" "----" "---" "-------------"
    for label in "${CELLS[@]}"; do
        timelog="time-${label}.txt"
        pmlog="power-${label}.txt"
        real=$(extract_wall "$timelog" 2>/dev/null || echo "")
        user=$(extract_user "$timelog" 2>/dev/null || echo "")
        sys=$(extract_sys  "$timelog" 2>/dev/null || echo "")
        if [[ -f "$pmlog" ]]; then
            energy=$(extract_energy "$pmlog" 2>/dev/null || echo "")
        else
            energy=""
        fi
        printf "  %-18s  %9ss  %9ss  %9ss  %14s\n" \
            "$label" "${real:-n/a}" "${user:-n/a}" "${sys:-n/a}" "${energy:-n/a}"
    done
else
    # Multi-round summary: one table per metric to keep each row
    # narrow enough to read without wrapping. Each table reports
    # mean / stddev / min / max for one metric across all four
    # cells. Wall / user / sys live in time-<label>-r<N>.txt;
    # energy lives in power-<label>-r<N>.txt.
    print_aggregate_table() {
        local title="$1"
        local extractor="$2"
        local prefix="$3"
        local scale="$4"
        echo "=== summary: $title over $BENCH_LOOP_COUNT rounds ==="
        printf "  %-18s  %10s  %10s  %10s  %10s\n" "scenario" "mean" "stddev" "min" "max"
        printf "  %-18s  %10s  %10s  %10s  %10s\n" "--------" "----" "------" "---" "---"
        for label in "${CELLS[@]}"; do
            read -r mean sd min max <<< "$(aggregate_cell "$extractor" "${prefix}-${label}" "$scale")"
            printf "  %-18s  %10s  %10s  %10s  %10s\n" "$label" "$mean" "$sd" "$min" "$max"
        done
        echo
    }
    print_aggregate_table "wall clock (s)" extract_wall   time  1
    print_aggregate_table "user CPU (s)"   extract_user   time  1
    print_aggregate_table "sys CPU (s)"    extract_sys    time  1
    print_aggregate_table "energy impact"  extract_energy power 1
fi
