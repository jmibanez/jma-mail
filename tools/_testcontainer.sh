#!/usr/bin/env bash
# Shared helpers for bench scripts that spin up a Stalwart fixture
# via examples/bench-server. Sourced from bench-rss.sh,
# bench-power.sh, and bench-power-watch.sh in their testcontainer-
# mode dispatch path; not invoked directly.
#
# Workflow when the bench script omits <maildir-source> and
# <account-email> (testcontainer mode):
#
#   1. cargo build --release of bench-server + gen-bench-maildir.
#   2. Generate a deterministic corpus at $BENCH_DIR/seed-corpus
#      (if not already present from a previous run).
#   3. Spawn bench-server in the background; it spawns the Stalwart
#      container, IMAP-APPENDs the corpus, then writes a shell-
#      sourceable info file at $BENCH_DIR/server-info.env.
#   4. Poll for the info file to know the server is fully ready.
#   5. Source the info file to expose JMA_BENCH_SESSION_URL,
#      JMA_BENCH_BEARER, JMA_BENCH_ACCOUNT_EMAIL to the caller for
#      use in the generated config.toml.
#   6. On exit, the bench script's cleanup trap SIGTERMs the
#      background bench-server and the container tears down.
#
# Callers must set REPO_ROOT before sourcing. All knobs below are
# env-overridable; defaults are sized for a typical small-corpus
# bench (a few hundred messages, single folder, no fat-tail
# stressing) that finishes setup in seconds.

: "${TESTCONTAINER_CORPUS_COUNT:=200}"
: "${TESTCONTAINER_CORPUS_FOLDERS:=1}"
: "${TESTCONTAINER_CORPUS_SEED:=0}"
: "${TESTCONTAINER_NEW_PCT:=10}"
# Number of concurrent IMAP sessions bench-server opens during
# the APPEND phase. Default 8 -- the empirical sweet spot on the
# RocksDB fixture (see table below). Exported so bench-server's
# TESTCONTAINER_SEED_PARALLELISM env-var override picks it up.
: "${TESTCONTAINER_SEED_PARALLELISM:=8}"
export TESTCONTAINER_SEED_PARALLELISM

# IMAP seeding dominates the wall-time for non-trivial corpus
# sizes. The dominant cost is Stalwart's per-insert work; with
# the RocksDB backend the post-cliff regime is CPU-bound and
# scales near-linearly with available cores, so client
# parallelism past 1 buys real throughput (unlike SQLite, where
# the single-writer lock caps it).
#
# Empirical 100k-corpus wall-times against the RocksDB fixture
# (measured directly; finer-grained per-batch numbers in the
# bench-server progress log on each run):
#
#   * 1 worker  / 4 CPUs:  ~1138s
#   * 4 workers / 4 CPUs:   ~700s
#   * 8 workers / 8 CPUs:   ~422s  <- sweet spot, the default above
#   * 16 workers / 8 CPUs:  ~392s  (diminishing returns)
#   * 16 workers / 16 CPUs: ~319s
#
# 8 workers on an 8-CPU host VM is where the curve flattens. More
# workers per CPU saturate the same cores; more CPUs continues to
# scale but with smaller per-step gains. The recommended Colima
# allocation for testcontainer-mode bench runs is therefore
# 8 CPUs / 8 GB.
#
# TESTCONTAINER_READY_TIMEOUT is computed dynamically in
# testcontainer_start from a linear model anchored on the
# 8w/8-CPU RocksDB datapoint: ~5 ms per message average across
# the full 100k corpus, scaled by 1.5x for headroom plus 60s for
# container boot. The 1.5x is generous against the recommended
# config; on smaller hosts (1-4 CPU) seeding takes substantially
# longer than the model assumes and you'll need to bump
# TESTCONTAINER_READY_TIMEOUT or override it directly. The
# estimate is printed at seed-start so you can see what timeout
# was chosen and why.

# Build the two example binaries the testcontainer flow depends on.
# Release builds because bench cells run against the same binaries
# and we don't want debug-build overhead skewing measurements that
# aren't about the example itself.
testcontainer_build_helpers() {
    echo "  building bench-server + gen-bench-maildir (release)..." >&2
    CC=/usr/bin/cc cargo build --release \
        --example bench-server \
        --example gen-bench-maildir >/dev/null
}

# Generate the seed corpus once and cache it under $BENCH_DIR.
# Re-running the bench script reuses the same corpus, which keeps
# server state identical across runs and matches the cached-binary
# pattern build_for() already uses for the jma binaries. Delete
# $BENCH_DIR/seed-corpus to force regeneration.
#
# Status echoes go to stderr so callers can capture the corpus path
# via `corpus=$(testcontainer_generate_corpus ...)` without picking
# up the human-readable lines.
testcontainer_generate_corpus() {
    local bench_dir="$1"
    local corpus="$bench_dir/seed-corpus"
    if [[ -d "$corpus/INBOX" ]]; then
        echo "  corpus cached: $corpus ($TESTCONTAINER_CORPUS_COUNT msgs)" >&2
        printf '%s' "$corpus"
        return 0
    fi
    echo "  generating $TESTCONTAINER_CORPUS_COUNT messages into $corpus..." >&2
    "$REPO_ROOT/target/release/examples/gen-bench-maildir" \
        --count "$TESTCONTAINER_CORPUS_COUNT" \
        --folders "$TESTCONTAINER_CORPUS_FOLDERS" \
        --seed "$TESTCONTAINER_CORPUS_SEED" \
        --new-pct "$TESTCONTAINER_NEW_PCT" \
        --out "$corpus" \
        --allow-non-empty >/dev/null
    printf '%s' "$corpus"
}

# Spawn bench-server in the background, poll for its info file,
# source the file, and export the three JMA_BENCH_* variables plus
# the PID for later teardown. Caller's cleanup trap should invoke
# testcontainer_stop to reap the daemon and trigger Stalwart
# teardown.
testcontainer_start() {
    local bench_dir="$1"
    local corpus
    corpus=$(testcontainer_generate_corpus "$bench_dir")
    local info_file="$bench_dir/server-info.env"
    rm -f "$info_file"

    # Project seed wall-time from a simple linear model anchored on
    # the RocksDB 8w/8-CPU sweet-spot datapoint (100k corpus in
    # ~422s, i.e. ~4.22 ms per message average). Rounded up to
    # 5 ms/msg for ~18% margin even on the anchor itself, then
    # multiplied by 1.5x for general headroom plus 60s for
    # container boot. No quadratic term because we don't have data
    # that supports one against the recommended config -- the
    # post-cliff regime in the bench-server progress log holds a
    # roughly constant aggregate rate, and the bench's bound is
    # CPU-throughput, not anything that grows super-linearly.
    local estimated
    estimated=$(awk -v n="$TESTCONTAINER_CORPUS_COUNT" \
        'BEGIN { printf "%d", int(0.005 * n + 0.5) }')
    # 1.5x headroom on the projection + 60s for container boot/seedup,
    # unless the caller pinned a specific timeout.
    if [[ -z "${TESTCONTAINER_READY_TIMEOUT:-}" ]]; then
        TESTCONTAINER_READY_TIMEOUT=$((estimated * 3 / 2 + 60))
    fi

    echo "  corpus=${TESTCONTAINER_CORPUS_COUNT}, parallelism=${TESTCONTAINER_SEED_PARALLELISM}, estimated seed ~${estimated}s (~$((estimated / 60))m), timeout ${TESTCONTAINER_READY_TIMEOUT}s" >&2
    echo "  starting bench-server with corpus $corpus..."
    "$REPO_ROOT/target/release/examples/bench-server" \
        --seed-from "$corpus" \
        --info-path "$info_file" \
        > "$bench_dir/server.log" 2>&1 &
    TESTCONTAINER_PID=$!

    local elapsed=0
    while [[ ! -f "$info_file" ]]; do
        if ! kill -0 "$TESTCONTAINER_PID" 2>/dev/null; then
            echo "  bench-server died during startup; see $bench_dir/server.log" >&2
            tail -20 "$bench_dir/server.log" >&2
            return 1
        fi
        if (( elapsed >= TESTCONTAINER_READY_TIMEOUT )); then
            echo "  bench-server not ready after ${TESTCONTAINER_READY_TIMEOUT}s; see $bench_dir/server.log" >&2
            kill "$TESTCONTAINER_PID" 2>/dev/null || true
            return 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    # shellcheck source=/dev/null
    source "$info_file"
    export JMA_BENCH_SESSION_URL JMA_BENCH_BEARER JMA_BENCH_ACCOUNT_EMAIL TESTCONTAINER_PID
    echo "  testcontainer ready: $JMA_BENCH_SESSION_URL"
}

# SIGTERM the background bench-server. Idempotent so the cleanup
# trap can fire multiple times (per-cell teardown + EXIT trap)
# without errors.
testcontainer_stop() {
    if [[ -n "${TESTCONTAINER_PID:-}" ]]; then
        kill -TERM "$TESTCONTAINER_PID" 2>/dev/null || true
        wait "$TESTCONTAINER_PID" 2>/dev/null || true
        TESTCONTAINER_PID=""
    fi
}
