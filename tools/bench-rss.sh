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
#   BENCH_DIR  Scratch dir for cached binaries, maildir copy, state
#              DB, logs.
#              Default: /tmp/jma-bench
#   SUBCMD     jma subcommand to benchmark.
#              Default: pull
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
subcommand. Testcontainer mode also honours TESTCONTAINER_CORPUS_COUNT,
TESTCONTAINER_CORPUS_FOLDERS, TESTCONTAINER_CORPUS_SEED, and
TESTCONTAINER_NEW_PCT to size the seed corpus.
EOF
    exit 1
}

if [[ $# -ne 2 && $# -ne 4 ]]; then
    usage
fi

BEFORE_REF="$1"
AFTER_REF="$2"
if [[ $# -eq 4 ]]; then
    MAILDIR_SOURCE="$3"
    ACCOUNT_EMAIL="$4"
    TESTCONTAINER_MODE=0
else
    MAILDIR_SOURCE=""
    ACCOUNT_EMAIL=""
    TESTCONTAINER_MODE=1
fi

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

if (( TESTCONTAINER_MODE )); then
    # Testcontainer mode: maildir always starts empty; jma pulls
    # everything from the seeded server. Wiping any leftovers from
    # a prior run ensures the BEFORE-initial cell measures a true
    # initial download rather than a partial resync.
    rm -rf maildir
    mkdir -p maildir
elif [[ ! -d maildir ]]; then
    echo "=== copying $MAILDIR_SOURCE -> $BENCH_DIR/maildir ==="
    cp -R "$MAILDIR_SOURCE" maildir
    echo
fi

# Always (re-)write the config so a config-shape change in the
# binaries doesn't require manual sync. In testcontainer mode we
# also pin [account].token (the fixture bearer) and
# [account].session_url (the container's advertised URL) so jma
# bypasses keychain + autodiscovery and hits the fixture directly.
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
# jma's Config::load refuses to start when [account].token is set
# in a config file with group/other perm bits (mode & 0o077 != 0;
# see 808c151). In testcontainer mode the bearer is in
# [account].token so this is hard-required; in real-account mode
# the token lives in the keychain and the file has nothing
# sensitive, but we chmod the same way regardless for
# consistency.
chmod 600 config.toml

reset_state() {
    # State DB location is pinned via [state].db_path in config.toml
    # above, so both binaries write to $BENCH_DIR/state.db regardless
    # of their built-in default (pre-76eef97 defaulted to
    # ~/.local/share/jmapsync/state.db, post-76eef97 to
    # <maildir>/.jma.db). Clear the pinned location plus the maildir-
    # level advisory lock that maildir_ops::lock writes at the
    # maildir root post-rename.
    rm -f state.db* maildir/.jma.lock
    # In testcontainer mode the maildir is never the source of
    # truth (the server is), so reset_state also wipes the maildir
    # between pairs so each "initial" cell measures a true initial
    # download. In real-account mode the maildir copy from
    # MAILDIR_SOURCE is the BEFORE state we want to preserve, so
    # we leave it alone.
    if (( TESTCONTAINER_MODE )); then
        rm -rf maildir
        mkdir -p maildir
    fi
}

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
