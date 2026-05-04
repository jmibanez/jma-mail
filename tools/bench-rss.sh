#!/usr/bin/env bash
# Compare peak RSS and wall-clock between two jmapsync builds across
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
# `$BENCH_DIR/jmapsync-<full-sha>`. Subsequent runs reuse the cached
# binary if it's already present; delete the file to force a rebuild.
# The user's working ref is restored at the end (and on interrupt /
# build failure via an EXIT trap), but the script refuses to start
# if the working tree has tracked uncommitted changes.
#
# Environment overrides:
#   BENCH_DIR  Scratch dir for cached binaries, maildir copy, state
#              DB, logs.
#              Default: /tmp/jmapsync-bench
#   SUBCMD     jmapsync subcommand to benchmark.
#              Default: pull
#
# Setup of the maildir + config is idempotent: the maildir is
# copied and the config written only if missing. Delete $BENCH_DIR
# to force a fresh setup (will also drop cached binaries).
#
# Auth flows through the existing keychain entry under
# `jmapsync-bearer`/`default`; the script does not manage tokens.
# Run `jmapsync auth set-token` first if needed.

set -euo pipefail

usage() {
    cat >&2 <<EOF
usage: $0 <before-commit> <after-commit> <maildir-source> <account-email>

Set BENCH_DIR or SUBCMD env vars to override the scratch dir or
subcommand. See the comment block at the top of $0 for details.
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

BENCH_DIR="${BENCH_DIR:-/tmp/jmapsync-bench}"
SUBCMD="${SUBCMD:-pull}"

# Must be inside a git checkout to resolve commits and switch refs.
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null) || {
    echo "not inside a git repository; run from the jmapsync checkout" >&2
    exit 1
}
cd "$REPO_ROOT"

if [[ ! -d "$MAILDIR_SOURCE" ]]; then
    echo "maildir source not a directory: $MAILDIR_SOURCE" >&2
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

restore_ref() {
    if [[ -n "${ORIG_REF:-}" ]]; then
        git checkout --quiet "$ORIG_REF" || true
        ORIG_REF=""
    fi
}
trap restore_ref EXIT INT TERM

mkdir -p "$BENCH_DIR"

# Build a binary for the given full SHA into the scratch cache.
# Returns success if the cached file is present (rebuilds when not).
build_for() {
    local sha="$1"
    local target="$BENCH_DIR/jmapsync-$sha"

    if [[ -x "$target" ]]; then
        echo "  cached: jmapsync-${sha:0:8} -> $target"
        return 0
    fi

    echo "  building jmapsync-${sha:0:8} ..."
    git checkout --quiet "$sha"
    CC=/usr/bin/cc cargo build --release
    cp "$REPO_ROOT/target/release/jmapsync" "$target"
    chmod +x "$target"
    echo "  built: $target"
}

echo "=== build / cache ==="
build_for "$BEFORE_SHA"
build_for "$AFTER_SHA"
echo

# Restore the user's ref now (rather than waiting for the trap) so
# the benchmark cells run with the working tree at the user's
# original commit, not at AFTER's tree.
restore_ref

BEFORE_BIN="$BENCH_DIR/jmapsync-$BEFORE_SHA"
AFTER_BIN="$BENCH_DIR/jmapsync-$AFTER_SHA"

# --- maildir / config setup (unchanged from the run-only script) ---

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
    rm -f state.db state.db-wal state.db-shm
}

run() {
    local label="$1"
    local binary="$2"
    local logfile="log-${label}.txt"

    echo "=== $label ==="
    if /usr/bin/time -l "$binary" -c config.toml "$SUBCMD" > "$logfile" 2>&1; then
        local rss real summary
        rss=$(awk '/maximum resident set size/ {print $1}' "$logfile")
        real=$(awk '/real/ {print $1; exit}' "$logfile")
        summary=$(grep -E "Initial sync complete|Sync complete|Already in sync" "$logfile" | tail -1 || echo "(no summary line)")
        local rss_mb
        rss_mb=$(awk -v b="$rss" 'BEGIN { printf "%.1f", b / 1024 / 1024 }')

        echo "  peak RSS:    ${rss} bytes (${rss_mb} MB)"
        echo "  wall clock:  ${real}s"
        echo "  outcome:     ${summary}"
    else
        echo "  FAILED -- see ${BENCH_DIR}/${logfile}"
        tail -10 "$logfile"
    fi
    echo "  log:         ${BENCH_DIR}/${logfile}"
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
printf "  %-18s  %10s  %10s\n" "scenario" "peak RSS" "wall clock"
printf "  %-18s  %10s  %10s\n" "--------" "--------" "----------"
for label in before-initial before-steady after-initial after-steady; do
    log="log-${label}.txt"
    rss=$(awk '/maximum resident set size/ {print $1}' "$log" 2>/dev/null || echo "")
    real=$(awk '/real/ {print $1; exit}' "$log" 2>/dev/null || echo "")
    if [[ -n "$rss" ]]; then
        rss_mb=$(awk -v b="$rss" 'BEGIN { printf "%.1f MB", b / 1024 / 1024 }')
    else
        rss_mb="(n/a)"
    fi
    printf "  %-18s  %10s  %9ss\n" "$label" "$rss_mb" "${real:-n/a}"
done
