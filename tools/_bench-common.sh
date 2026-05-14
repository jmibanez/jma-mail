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
#   BENCH_LOOP_COUNT Number of rounds to run (run_loop / aggregate_cell);
#                    parsed and validated at source-time below

# Number of bench rounds. Validated at source-time so a typo
# doesn't manifest as a silently-skipped loop or a thousand-round
# runaway. Each bench script wraps its cell driver in `run_loop`
# below, which calls a per-script round body $BENCH_LOOP_COUNT
# times.
BENCH_LOOP_COUNT="${BENCH_LOOP_COUNT:-1}"
if ! [[ "$BENCH_LOOP_COUNT" =~ ^[1-9][0-9]*$ ]]; then
    echo "BENCH_LOOP_COUNT must be a positive integer; got: $BENCH_LOOP_COUNT" >&2
    exit 1
fi

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

# Run the caller's per-script round body $BENCH_LOOP_COUNT times.
# The round-body fn (passed by name) receives the round's label
# suffix as $1 -- empty for a single-round run so the per-cell
# log files keep their original `log-<label>.txt` naming and the
# script's existing N==1 summary path doesn't need to know about
# rounds, or "-r<N>" for multi-round runs so each round's files
# are independently preserved for later post-processing.
#
# The round body shape is per-script: bench-rss / bench-power do
# the four-cell quartet (BEFORE-initial, BEFORE-steady, reset,
# AFTER-initial, AFTER-steady) with reset_state between pairs;
# bench-power-watch does two warm-then-watch cells with reset_state
# inside each cell's run_watch. Both shapes slot into this loop
# uniformly -- the loop only knows about rounds and labels, not
# about cell semantics.
run_loop() {
    local round_fn="$1"
    for ((round=1; round<=BENCH_LOOP_COUNT; round++)); do
        local suffix=""
        if (( BENCH_LOOP_COUNT > 1 )); then
            echo "=== round $round / $BENCH_LOOP_COUNT ==="
            suffix="-r${round}"
        fi
        "$round_fn" "$suffix"
    done
}

# Compute mean / stddev / min / max of one metric across the N
# per-round log files for one cell. Outputs four space-separated
# numbers (mean stddev min max), or "n/a n/a n/a n/a" if no
# per-round logs exist or no values extract cleanly.
#
# extractor_fn is a function name; it's invoked as
#   <extractor_fn> <logfile>
# and is expected to output a single number on stdout. The
# function-pointer indirection lets each bench script extract its
# own metrics (RSS / wall / CPU / energy) without this helper
# growing per-script knowledge.
#
# log_prefix is the per-round logfile root: aggregate_cell looks
# for "<log_prefix>-r<round>.txt" for each round in 1..N. scale
# divides the extracted values (1 for already-scaled units like
# seconds or energy impact; 1024*1024 to convert RSS bytes to MB).
#
# stddev uses Bessel's n-1 correction; with n=1 it collapses to 0.
# Callers should branch on BENCH_LOOP_COUNT and use a single-read
# summary path when N==1, since variance over one sample is
# meaningless.
aggregate_cell() {
    local extractor_fn="$1"
    local log_prefix="$2"
    local scale="$3"
    local values=()
    for ((r=1; r<=BENCH_LOOP_COUNT; r++)); do
        local log="${log_prefix}-r${r}.txt"
        [[ -f "$log" ]] || continue
        local v
        v=$("$extractor_fn" "$log" 2>/dev/null)
        [[ -n "$v" ]] && values+=("$v")
    done
    if (( ${#values[@]} == 0 )); then
        echo "n/a n/a n/a n/a"
        return
    fi
    printf '%s\n' "${values[@]}" | awk -v scale="$scale" '
        BEGIN { min = ""; max = "" }
        { v = $1 / scale; n++; s += v; ss += v*v
          if (min == "" || v < min) min = v
          if (max == "" || v > max) max = v }
        END {
            mean = s / n
            if (n > 1) {
                var = (ss - s*s/n) / (n - 1)
                if (var < 0) var = 0
                sd = sqrt(var)
            } else {
                sd = 0
            }
            printf "%.2f %.2f %.2f %.2f", mean, sd, min, max
        }'
}

# Untimed pre-warm pull against the testcontainer server so the
# first measured *-initial cell of the round doesn't bear the full
# cost of warming the server's OS page cache, RocksDB block cache,
# and Stalwart's in-process caches. Without this the BEFORE-initial
# cell pays that warming cost and AFTER-initial rides on its back --
# a constant-sign bias that always favors AFTER and doesn't average
# out across rounds (only round 1's first initial cell is ever
# truly cold; every subsequent initial cell sees a warmer baseline).
#
# Real-account mode is a no-op: the live server's caching is opaque
# and outside the script's control.
#
# Takes the binary to run the throwaway pull with (either BEFORE or
# AFTER works since both see the same server) and the round suffix
# for log naming. State is reset at the start of the function so the
# pull works from a clean DB+maildir baseline; the bench-script round
# body is expected to reset_state again afterwards before its first
# measured cell. Warmup output lands at prewarm<suffix>.txt in
# $BENCH_DIR for debugging.
prewarm_server() {
    (( TESTCONTAINER_MODE )) || return 0
    local binary="$1"
    local suffix="$2"
    local warmlog="prewarm${suffix}.txt"
    echo "=== prewarm${suffix} (untimed, warms server caches) ==="
    reset_state
    if "$binary" -c config.toml pull > "$warmlog" 2>&1; then
        echo "  done -- see ${BENCH_DIR}/${warmlog}"
    else
        echo "  WARN: prewarm pull failed; proceeding anyway -- see ${BENCH_DIR}/${warmlog}" >&2
        tail -5 "$warmlog" >&2 || true
    fi
    echo
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
