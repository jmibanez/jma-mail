#!/usr/bin/env bash
# Compare per-cell Energy Impact between two jma builds while
# `jma watch` is running in a quiescent steady state.
#
# Two cells:
#   * BEFORE binary, watching for $WATCH_SECONDS against a warm state DB
#   * AFTER  binary, watching for $WATCH_SECONDS against a warm state DB
#
# For each cell the script:
#   1. Resets the state DB and runs one untimed `pull` to warm it
#      against the current server state.
#   2. Starts `powermetrics --samplers tasks --show-process-energy`
#      in the background, sampling every $SAMPLE_MS ms.
#   3. Starts `jma watch` in the background.
#   4. Sleeps $WATCH_SECONDS.
#   5. SIGTERMs the watch (then SIGKILL after a short grace period).
#   6. Stops powermetrics and sums jma's Energy Impact rows.
#
# Energy Impact is Apple's unitless score (not joules); useful for
# comparing two builds running the same workload, not for absolute
# power figures. The CPU columns from /usr/bin/time aren't meaningful
# here because the run is a fixed-duration daemon kill, so this
# script reports energy and sample count only.
#
# Usage:
#   tools/bench-power-watch.sh <before-commit> <after-commit> \
#                              <maildir-source> <account-email>
#
# All four positional arguments are required and have no defaults.
# `<before-commit>` and `<after-commit>` are anything `git rev-parse`
# accepts (full SHA, short SHA, branch, tag, `HEAD~3`, ...).
#
# powermetrics requires root. The script primes sudo credentials at
# the start (you will be prompted once) and keeps the ticket alive
# in the background until exit.
#
# Environment overrides:
#   BENCH_DIR       Scratch dir for cached binaries, maildir copy,
#                   state DB, logs.
#                   Default: /tmp/jma-bench
#   WATCH_SECONDS   How long to let `jma watch` run before signalling
#                   it to exit. Bump this to firm up the energy
#                   average; the daemon's idle wake-ups are a few
#                   per second.
#                   Default: 60
#   SAMPLE_MS       powermetrics sampling interval in milliseconds.
#                   At SAMPLE_MS=250 a 60s cell yields ~240 samples;
#                   that's where the signal-to-noise gets usable.
#                   Default: 250
#   STIMULUS_INTERVAL_S
#                   How often to touch an existing message file in
#                   cur/ during the watch window so the daemon sees
#                   a no-op LocalChange. Pre-ba7bdad each trigger
#                   drove a full-maildir scan; post-ba7bdad it's a
#                   path-driven scan of just the changed path. Set
#                   to 0 to disable the stimulus and measure the
#                   truly-idle daemon instead.
#                   Default: 10
#
# Setup of the maildir + config is idempotent: the maildir is copied
# and the config written only if missing. The state DB is wiped
# before each cell so both cells start from the same warm-then-idle
# baseline. Delete $BENCH_DIR to force a fresh maildir copy.
#
# Auth flows through the existing keychain entry under
# `jma-bearer`/`default`; the script does not manage tokens.

set -euo pipefail

usage() {
    cat >&2 <<EOF
usage: $0 <before-commit> <after-commit>
       $0 <before-commit> <after-commit> <maildir-source> <account-email>

Two-arg form spins up a Stalwart testcontainer fixture; four-arg
form runs against a real JMAP account. In testcontainer mode the
watch cells run against a freshly-seeded server with the maildir
warmed by an untimed pull (same warm-then-idle baseline as
real-account mode).

Set BENCH_DIR, WATCH_SECONDS, SAMPLE_MS, or STIMULUS_INTERVAL_S
env vars to override defaults. Testcontainer mode also honours
TESTCONTAINER_CORPUS_COUNT, TESTCONTAINER_CORPUS_FOLDERS,
TESTCONTAINER_CORPUS_SEED, and TESTCONTAINER_NEW_PCT.
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
WATCH_SECONDS="${WATCH_SECONDS:-60}"
SAMPLE_MS="${SAMPLE_MS:-250}"
STIMULUS_INTERVAL_S="${STIMULUS_INTERVAL_S:-10}"

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

resolve_sha() {
    git rev-parse --verify "${1}^{commit}" 2>/dev/null || {
        echo "cannot resolve commit ref: $1" >&2
        exit 1
    }
}

BEFORE_SHA=$(resolve_sha "$BEFORE_REF")
AFTER_SHA=$(resolve_sha "$AFTER_REF")

subject_of() {
    git --no-pager log -1 --format=%s "$1"
}

echo "=== resolved commits ==="
printf "  BEFORE: %s (%s) -- %s\n" "$BEFORE_REF" "${BEFORE_SHA:0:8}" "$(subject_of "$BEFORE_SHA")"
printf "  AFTER:  %s (%s) -- %s\n" "$AFTER_REF"  "${AFTER_SHA:0:8}"  "$(subject_of "$AFTER_SHA")"
echo

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "working tree has uncommitted tracked changes; commit or stash first" >&2
    exit 1
fi

ORIG_REF=$(git symbolic-ref --quiet --short HEAD 2>/dev/null || git rev-parse HEAD)

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

# Source the testcontainer helper unconditionally (functions and
# defaults only; nothing fires until a function is called). Done
# before any ref-switching cargo operations so the file is loaded
# from the user's original tree.
# shellcheck source=tools/_testcontainer.sh
source "$REPO_ROOT/tools/_testcontainer.sh"

cleanup() {
    # Best-effort kill of any stragglers we own; per-cell teardown
    # already handles the happy path, but if we crashed mid-cell
    # these PIDs tell us who to reap so we don't leak a daemon, a
    # touch loop, or a root-owned powermetrics across script exits.
    # TESTCONTAINER_PID goes early so container teardown can overlap
    # with the local-process kills below.
    testcontainer_stop
    if [[ -n "${ACTIVE_STIM_PID:-}" ]]; then
        kill "$ACTIVE_STIM_PID" 2>/dev/null || true
        ACTIVE_STIM_PID=""
    fi
    if [[ -n "${ACTIVE_WATCH_PID:-}" ]]; then
        kill -TERM "$ACTIVE_WATCH_PID" 2>/dev/null || true
        ACTIVE_WATCH_PID=""
    fi
    if [[ -n "${ACTIVE_PM_PID:-}" ]]; then
        sudo -n kill -INT "$ACTIVE_PM_PID" 2>/dev/null || true
        ACTIVE_PM_PID=""
    fi
    if [[ -n "${SUDO_KEEPALIVE_PID:-}" ]]; then
        kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
        SUDO_KEEPALIVE_PID=""
    fi
    restore_ref
}
trap cleanup EXIT INT TERM

mkdir -p "$BENCH_DIR"

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

cd "$BENCH_DIR"

if (( TESTCONTAINER_MODE )); then
    # Testcontainer mode: maildir starts empty; the per-cell warm
    # pull populates it from the seeded server before the watch
    # window opens. Wipe any leftovers from a prior run.
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
# [account].session_url (the container's advertised URL).
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
    # In testcontainer mode the server is the source of truth, so
    # also wipe the maildir between cells. The per-cell warm pull
    # re-downloads from the server, giving each cell the same
    # warm-then-idle baseline as the real-account mode.
    if (( TESTCONTAINER_MODE )); then
        rm -rf maildir
        mkdir -p maildir
    fi
}

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

run_watch() {
    local label="$1"
    local binary="$2"
    local warmlog="warm-${label}.txt"
    local watchlog="watch-${label}.txt"
    local pmlog="power-${label}.txt"

    echo "=== $label ==="

    reset_state

    echo "  warming state DB (untimed pull) ..."
    if ! "$binary" -c config.toml pull > "$warmlog" 2>&1; then
        echo "  FAILED to warm -- see ${BENCH_DIR}/${warmlog}"
        tail -10 "$warmlog"
        echo
        return
    fi

    # powermetrics first, so the daemon's startup is captured in
    # full. The first-sample sleep below ensures we have at least
    # one row written before the daemon connects.
    sudo -n powermetrics \
        --samplers tasks \
        --show-process-energy \
        -i "$SAMPLE_MS" \
        -o "$pmlog" \
        >/dev/null 2>&1 &
    ACTIVE_PM_PID=$!
    sleep "$(awk -v ms="$SAMPLE_MS" 'BEGIN { printf "%.2f", (ms / 1000) + 0.2 }')"

    echo "  watching for ${WATCH_SECONDS}s ..."
    "$binary" -c config.toml watch > "$watchlog" 2>&1 &
    ACTIVE_WATCH_PID=$!

    # Optional no-op LocalChange stimulus. Drops a sidecar file
    # inside some folder's cur/ and removes it, every
    # STIMULUS_INTERVAL_S seconds. The create+remove pair is
    # surfaced by macOS fsevents as structural events
    # (kFSEventStreamEventFlagItemCreated / ItemRemoved) which are
    # reliably reported -- unlike pure mtime changes from `touch`
    # on an existing file, which on APFS via fsevents are
    # frequently coalesced or never delivered to the stream. The
    # sidecar lives under cur/ so the watcher's is_maildir_message_path
    # filter accepts the events; scan_paths then fails to classify
    # the unparseable name and emits no DB change, giving us the
    # no-op LocalChange the comparison wants. The first cycle lands
    # one interval after the loop starts, giving the daemon time to
    # come up and subscribe to fsevents.
    # `-print -quit` makes find exit cleanly after the first match
    # -- piping to `head -1` would SIGPIPE find and trip pipefail.
    local stim_cur=""
    local stim_sidecar=""
    if (( STIMULUS_INTERVAL_S > 0 )); then
        stim_cur=$(find maildir -type d -name cur -print -quit 2>/dev/null)
        if [[ -z "$stim_cur" ]]; then
            echo "  stimulus: no cur/ directory found; disabled for this cell"
        else
            stim_sidecar="$stim_cur/.bench-stim-trigger"
            echo "  stimulus: create+remove $stim_sidecar every ${STIMULUS_INTERVAL_S}s"
            ( while true; do
                sleep "$STIMULUS_INTERVAL_S"
                : > "$stim_sidecar" 2>/dev/null || exit
                rm -f "$stim_sidecar" 2>/dev/null || exit
              done ) &
            ACTIVE_STIM_PID=$!
        fi
    fi

    sleep "$WATCH_SECONDS"

    # Stop the stimulus first so no further touches arrive during
    # the watch's shutdown window.
    if [[ -n "${ACTIVE_STIM_PID:-}" ]]; then
        kill "$ACTIVE_STIM_PID" 2>/dev/null || true
        wait "$ACTIVE_STIM_PID" 2>/dev/null || true
        ACTIVE_STIM_PID=""
    fi

    # SIGTERM, then SIGKILL after a 5s grace period if the daemon
    # didn't drop its SSE connection cleanly.
    kill -TERM "$ACTIVE_WATCH_PID" 2>/dev/null || true
    local waited=0
    while kill -0 "$ACTIVE_WATCH_PID" 2>/dev/null; do
        if (( waited >= 5 )); then
            kill -KILL "$ACTIVE_WATCH_PID" 2>/dev/null || true
            break
        fi
        sleep 1
        waited=$((waited + 1))
    done
    wait "$ACTIVE_WATCH_PID" 2>/dev/null || true
    ACTIVE_WATCH_PID=""

    sudo -n kill -INT "$ACTIVE_PM_PID" 2>/dev/null || true
    wait "$ACTIVE_PM_PID" 2>/dev/null || true
    ACTIVE_PM_PID=""

    local energy_pair energy samples
    energy_pair=$(sum_jma_energy "$pmlog")
    energy=${energy_pair% *}
    samples=${energy_pair##* }

    echo "  energy impact:   ${energy} (sum over ${samples} jma samples @ ${SAMPLE_MS}ms)"
    echo "  watch log:       ${BENCH_DIR}/${watchlog}"
    echo "  power log:       ${BENCH_DIR}/${pmlog}"
    echo
}

run_watch before-watch "$BEFORE_BIN"
run_watch after-watch  "$AFTER_BIN"

echo "=== summary ==="
printf "  %-18s  %12s  %10s\n" "scenario" "watch window" "energy impact"
printf "  %-18s  %12s  %10s\n" "--------" "------------" "-------------"
for label in before-watch after-watch; do
    pmlog="power-${label}.txt"
    if [[ -f "$pmlog" ]]; then
        energy_pair=$(sum_jma_energy "$pmlog")
        energy=${energy_pair% *}
        samples=${energy_pair##* }
    else
        energy="n/a"
        samples="0"
    fi
    printf "  %-18s  %11ss  %13s\n" "$label" "$WATCH_SECONDS" "${energy:-n/a}"
done
echo
echo "  (sample counts: see per-cell output above; energy is sum"
echo "   of jma's Energy Impact column across all samples)"
