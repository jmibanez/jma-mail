#!/usr/bin/env bash
# Compare peak RSS and wall-clock between two jma builds across
# the four cells of an A/B benchmark:
#
#   * BEFORE binary, fresh state DB (initial sync)
#   * BEFORE binary, populated state DB (steady state)
#   * AFTER binary,  fresh state DB (initial sync)
#   * AFTER binary,  populated state DB (steady state)
#
# Each cell runs the configured subcommand (`pull` by default) under
# `/usr/bin/time -l`, captures peak resident set size and wall-clock
# time, and tabulates the four values at the end.
#
# Usage:
#   tools/bench-rss.sh <before-commit> <after-commit> \
#                      <maildir-source> <account-email>
#
# All four positional arguments are required and have no defaults.
# `<before-commit>` and `<after-commit>` are anything `git rev-parse`
# accepts (full SHA, short SHA, branch, tag, `HEAD~3`, ...).
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
#   BENCH_LOOP_COUNT  Number of rounds to run. Each round is one full
#                     quartet (BEFORE-initial, BEFORE-steady,
#                     AFTER-initial, AFTER-steady), so binary code
#                     paths are interleaved temporally and round-to-
#                     round noise (thermal drift, background load,
#                     page-cache state) affects each cell equally.
#                     Per-round logs land at log-<label>-r<round>.txt
#                     when this is >1; the summary table then reports
#                     mean / stddev / min / max per cell across the
#                     rounds. Stays apples-to-apples for the BEFORE-
#                     vs-AFTER comparison within each round.
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

# Source the shared bench helpers via BASH_SOURCE so this works
# regardless of the caller's cwd. The common file defines the
# verbatim helpers (resolve_sha / subject_of / build_for / etc.);
# the per-script body below stays focused on the rss measurement.
# shellcheck source=tools/_bench-common.sh
source "$(dirname "${BASH_SOURCE[0]}")/_bench-common.sh"

usage() {
    cat >&2 <<EOF
usage: $0 <before-commit> <after-commit>
       $0 <before-commit> <after-commit> <maildir-source> <account-email>

Two-arg form spins up a Stalwart testcontainer fixture seeded from
a synthetic corpus and runs both cells against it -- no live
account or local maildir required. Four-arg form is the original
behavior: copy the user-provided maildir into BENCH_DIR and sync
against the real JMAP account.

Note: BEFORE-initial cells differ semantically between modes. In
four-arg mode the maildir starts pre-populated (from the source
copy), so the cell measures a fresh-state-DB re-scan against an
already-warm tree. In two-arg mode the maildir starts empty and
the cell measures a true initial download from the seeded server.
Within a mode BEFORE-vs-AFTER stays apples-to-apples; across modes
the numbers aren't directly comparable.

Set BENCH_DIR or SUBCMD env vars to override the scratch dir or
subcommand. BENCH_LOOP_COUNT=N runs N quartet rounds and the summary
reports mean/stddev/min/max per cell. Testcontainer mode also honours
TESTCONTAINER_CORPUS_COUNT, TESTCONTAINER_CORPUS_FOLDERS,
TESTCONTAINER_CORPUS_SEED, and TESTCONTAINER_NEW_PCT to size the seed
corpus.
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

BEFORE_SHA=$(resolve_sha "$BEFORE_REF")
AFTER_SHA=$(resolve_sha "$AFTER_REF")
print_resolved_commits "$BEFORE_REF" "$BEFORE_SHA" "$AFTER_REF" "$AFTER_SHA"

require_clean_worktree

# Remember the user's current ref so the trap can restore it. If we
# were on a branch, store the branch name; if detached, store the
# SHA. Empty ORIG_REF means no restore needed (or already restored).
ORIG_REF=$(git symbolic-ref --quiet --short HEAD 2>/dev/null || git rev-parse HEAD)

# Source the testcontainer helper unconditionally (it just defines
# functions and default env vars; nothing fires until a function is
# called). Done before any ref-switching cargo operations so the
# file is loaded from the user's original tree -- BEFORE refs may
# predate when this helper landed.
# shellcheck source=tools/_testcontainer.sh
source "$REPO_ROOT/tools/_testcontainer.sh"

# Cleanup tears down the testcontainer fixture (no-op when one
# wasn't started) and restores the user's ref. restore_ref is
# still called directly mid-script after the build loop so cells
# run on the user's original tree -- this trap is for crash /
# interrupt paths and the final exit.
cleanup() {
    testcontainer_stop
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
# original commit, not at AFTER's tree.
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
    local logfile="log-${label}.txt"
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
    if /usr/bin/time -l "$binary" -c config.toml "${profile_args[@]}" "$SUBCMD" > "$logfile" 2>&1; then
        local rss real summary
        rss=$(awk '/maximum resident set size/ {print $1}' "$logfile")
        real=$(awk '/real/ {print $1; exit}' "$logfile")
        summary=$(grep -E "Initial sync complete|Sync complete|Already in sync" "$logfile" | tail -1 || echo "(no summary line)")
        local rss_mb
        rss_mb=$(awk -v b="$rss" 'BEGIN { printf "%.1f", b / 1024 / 1024 }')

        echo "  peak RSS:    ${rss} bytes (${rss_mb} MB)"
        echo "  wall clock:  ${real}s"
        echo "  outcome:     ${summary}"
        if [[ -s "$profile_json" ]]; then
            echo "  profile:     ${BENCH_DIR}/${profile_json}"
        fi
    else
        echo "  FAILED -- see ${BENCH_DIR}/${logfile}"
        tail -10 "$logfile"
    fi
    echo "  log:         ${BENCH_DIR}/${logfile}"
    echo
}

# Quartet-grouped round body: each round runs all four cells back-
# to-back so round-to-round noise (thermal drift, background load,
# page-cache state) affects each cell equally. The BEFORE-vs-AFTER
# delta within any single round stays apples-to-apples; multi-round
# variance is what's left after that pairing.
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
extract_rss()  { awk '/maximum resident set size/ {print $1; exit}' "$1"; }
extract_wall() { awk '/real/ {print $1; exit}' "$1"; }

CELLS=(before-initial before-steady after-initial after-steady)

if (( BENCH_LOOP_COUNT == 1 )); then
    # Single-round summary: same shape as before the loop landed.
    # Reads the no-suffix log files emitted in single-round mode.
    echo "=== summary ==="
    printf "  %-18s  %10s  %10s\n" "scenario" "peak RSS" "wall clock"
    printf "  %-18s  %10s  %10s\n" "--------" "--------" "----------"
    for label in "${CELLS[@]}"; do
        log="log-${label}.txt"
        rss=$(extract_rss "$log" 2>/dev/null || echo "")
        real=$(extract_wall "$log" 2>/dev/null || echo "")
        if [[ -n "$rss" ]]; then
            rss_mb=$(awk -v b="$rss" 'BEGIN { printf "%.1f MB", b / 1024 / 1024 }')
        else
            rss_mb="(n/a)"
        fi
        printf "  %-18s  %10s  %9ss\n" "$label" "$rss_mb" "${real:-n/a}"
    done
else
    # Multi-round summary: per-cell mean / stddev / min / max. Two
    # tables (RSS in MB, wall-clock in seconds) keep each row
    # narrow enough to read in a terminal without wrapping.
    rss_scale=$((1024 * 1024))
    echo "=== summary: peak RSS over $BENCH_LOOP_COUNT rounds (MB) ==="
    printf "  %-18s  %10s  %10s  %10s  %10s\n" "scenario" "mean" "stddev" "min" "max"
    printf "  %-18s  %10s  %10s  %10s  %10s\n" "--------" "----" "------" "---" "---"
    for label in "${CELLS[@]}"; do
        read -r mean sd min max <<< "$(aggregate_cell extract_rss "log-${label}" "$rss_scale")"
        printf "  %-18s  %10s  %10s  %10s  %10s\n" "$label" "$mean" "$sd" "$min" "$max"
    done
    echo
    echo "=== summary: wall clock over $BENCH_LOOP_COUNT rounds (s) ==="
    printf "  %-18s  %10s  %10s  %10s  %10s\n" "scenario" "mean" "stddev" "min" "max"
    printf "  %-18s  %10s  %10s  %10s  %10s\n" "--------" "----" "------" "---" "---"
    for label in "${CELLS[@]}"; do
        read -r mean sd min max <<< "$(aggregate_cell extract_wall "log-${label}" 1)"
        printf "  %-18s  %10s  %10s  %10s  %10s\n" "$label" "$mean" "$sd" "$min" "$max"
    done
fi
