#!/usr/bin/env bash
# Regenerate the Stalwart fixture pair under tests/fixtures/stalwart/.
#
# Stalwart's persisted config is a typed-JSON blob produced by the
# install wizard. The existing fixture (committed in 69c455a) was
# walked with the SQLite backend. This script spawns a fresh
# Stalwart container with no config so the wizard fires, prints
# instructions for walking it (this time selecting RocksDB), then
# applies three admin-API tweaks for you given the admin password
# you set during the wizard:
#
#   1. Mint a long-lived API key (Bearer) via x:ApiKey/set.
#   2. Add a plaintext IMAP listener on 0.0.0.0:143 via
#      x:NetworkListener/set.
#   3. Enable allowPlainTextAuth on the IMAP service via
#      x:Imap/set (the singleton id is the literal "singleton").
#
# Listener config doesn't hot-reload -- Stalwart binds all
# listeners at startup. After (2) the container is restarted so
# the new IMAP listener actually binds.
#
# Once the listener is up, the resulting state is docker-cp'd out
# into tests/fixtures/stalwart/ (config.json + the rocksdb/
# directory).
#
# The RocksDB switch matters because Stalwart's SQLite backend
# serializes all writes through a single writer lock, capping
# IMAP-bulk-seed throughput at ~125 msg/s on a 1-CPU host and
# producing a non-deterministic cliff on multi-CPU hosts. RocksDB
# is Stalwart's own published default for single-node installs
# (see crates/main/Cargo.toml in stalwartlabs/stalwart) and uses
# an OptimisticTransactionDB on a rayon worker pool with no
# global writer lock.
#
# Requirements: docker, curl, jq.
#
# After this script finishes:
#   1. Update constants in tests/common/mod.rs:
#        ACCOUNT_IMAP_PASSWORD   = "<password you set in wizard>"
#        FIXTURE_BEARER          = "<bearer printed at end here>"
#        CONTAINER_DB_PATH       = "/var/lib/stalwart/rocksdb"
#                                  (now a directory, not a file)
#   2. Update spawn_stalwart() in tests/common/mod.rs to copy the
#      rocksdb/ directory instead of a single .db file. The
#      easiest pattern: add the `include_dir` crate to
#      [dev-dependencies] and iterate:
#
#        static ROCKSDB_FIXTURE: Dir =
#          include_dir!("$CARGO_MANIFEST_DIR/tests/fixtures/stalwart/rocksdb");
#        for file in ROCKSDB_FIXTURE.files() {
#            image = image.with_copy_to(
#                format!("/var/lib/stalwart/rocksdb/{}", file.path().display()),
#                file.contents().to_vec(),
#            );
#        }
#
#   3. Delete the old SQLite fixture once the RocksDB one is
#      verified: rm tests/fixtures/stalwart/stalwart.db

set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null) || {
    echo "not inside a git checkout" >&2
    exit 1
}
FIXTURE_DIR="$REPO_ROOT/tests/fixtures/stalwart"
mkdir -p "$FIXTURE_DIR"

STALWART_IMAGE="stalwartlabs/stalwart:v0.16"
HTTP_PORT="${WIZARD_HTTP_PORT:-8080}"
IMAP_PORT="${WIZARD_IMAP_PORT:-1143}"

for cmd in docker curl jq; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "$cmd not on PATH; install it first" >&2
        exit 1
    fi
done

if [[ -d "$FIXTURE_DIR/rocksdb" && -z "${FORCE:-}" ]]; then
    echo "$FIXTURE_DIR/rocksdb already exists; set FORCE=1 to overwrite" >&2
    exit 1
fi

echo "=== Stalwart fixture regeneration ==="
echo "  image:      $STALWART_IMAGE"
echo "  http port:  $HTTP_PORT (override via WIZARD_HTTP_PORT)"
echo "  imap port:  $IMAP_PORT (override via WIZARD_IMAP_PORT)"
echo "  fixture:    $FIXTURE_DIR"
echo

echo "Spawning fresh container..."
CONTAINER=$(docker run -d \
    -p "${HTTP_PORT}:8080" \
    -p "${IMAP_PORT}:143" \
    -e "STALWART_PUBLIC_URL=http://127.0.0.1:${HTTP_PORT}" \
    -u 0:0 \
    "$STALWART_IMAGE")
echo "Container: $CONTAINER"

# Set SUCCESS=1 just before clean exit so the trap only tears down
# the container on a happy path. Mid-script failures leave the
# container running so you can docker exec / docker logs / probe
# /jmap to figure out what went wrong, then docker rm by hand.
SUCCESS=0
cleanup() {
    if (( SUCCESS )); then
        echo "Stopping container..."
        docker stop "$CONTAINER" >/dev/null 2>&1 || true
        docker rm   "$CONTAINER" >/dev/null 2>&1 || true
    else
        cat >&2 <<EOM

Script exited before completion. Container preserved for debugging:
  container id: $CONTAINER
  http port:    $HTTP_PORT
  imap port:    $IMAP_PORT

Useful probes (run before cleaning up):
  docker logs $CONTAINER 2>&1 | tail -100
  curl -i -u 'admin@example.org:<password>' http://127.0.0.1:${HTTP_PORT}/jmap/session

Clean up when done:
  docker stop $CONTAINER && docker rm $CONTAINER
EOM
    fi
}
trap cleanup EXIT INT TERM

wait_for_http() {
    local label="$1"
    local timeout="${2:-60}"
    echo "Waiting for HTTP listener ($label)..."
    for _ in $(seq 1 "$timeout"); do
        if curl -sf -o /dev/null "http://127.0.0.1:${HTTP_PORT}/" 2>/dev/null \
            || curl -sf -o /dev/null "http://127.0.0.1:${HTTP_PORT}/install" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "HTTP listener never came up within ${timeout}s" >&2
    return 1
}

wait_for_http "initial boot"

cat <<EOF

=== Walk the install wizard ===
Open in your browser:  http://127.0.0.1:${HTTP_PORT}/

Wizard choices:
  * Admin email:     admin@example.org
  * Admin password:  pick any -- remember it; you'll be prompted
                     for it below so the script can drive the
                     three admin tweaks for you
  * Domain:          example.org
  * Storage backend: RocksDB  (NOT SQLite -- the whole point)

Press Enter once you have walked the wizard to completion.
EOF
read -r -p "Wizard done? "

echo
read -r -p "Admin email [admin@example.org]: " ADMIN_EMAIL
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@example.org}"
read -r -s -p "Admin password (input hidden): " ADMIN_PASS
echo
echo

# Stalwart's wizard writes the new admin Principal to the
# directory store but the running server still holds the bootstrap
# admin credentials in memory. Until the server is restarted, the
# wizard-created admin email/password isn't accepted on /jmap (you
# get 401 with the in-memory bootstrap admin still active). A
# restart picks up the new Principal from the persisted store.
echo "Restarting container so the wizard admin credentials load..."
docker restart "$CONTAINER" >/dev/null
wait_for_http "post-wizard" 60

# Sanity-check before we throw three more admin calls at the
# server. Failing fast here gives a clearer error than a 401 deep
# inside the methodCalls loop.
echo "Verifying admin auth against /jmap/session..."
if ! curl -fsS -o /dev/null -u "${ADMIN_EMAIL}:${ADMIN_PASS}" \
    "http://127.0.0.1:${HTTP_PORT}/jmap/session"; then
    echo >&2
    echo "Admin auth failed (401 or similar). Likely causes:" >&2
    echo "  * The password you typed doesn't match what you set in the wizard." >&2
    echo "  * The wizard's admin email isn't ${ADMIN_EMAIL}." >&2
    echo "  * Stalwart hasn't picked up the wizard-created Principal yet" >&2
    echo "    (try waiting a few more seconds and re-running)." >&2
    echo >&2
    echo "Container left running for inspection -- see cleanup trap output below." >&2
    exit 1
fi
echo "  admin auth ok"

# Helper for the three admin calls. POSTs a methodCalls body to
# /jmap with Basic auth and returns the raw response on stdout.
jmap_post() {
    local body="$1"
    curl -fsS \
        -u "${ADMIN_EMAIL}:${ADMIN_PASS}" \
        -H "Content-Type: application/json" \
        -X POST \
        --data "$body" \
        "http://127.0.0.1:${HTTP_PORT}/jmap"
}

echo "Minting API key via x:ApiKey/set..."
APIKEY_BODY='{
  "using": ["urn:ietf:params:jmap:core", "urn:stalwart:jmap"],
  "methodCalls": [
    ["x:ApiKey/set", {
       "create": {
         "k1": {
           "description": "jma-fixture-seeder",
           "permissions": {"@type": "Inherit"}
         }
       }
    }, "c1"]
  ]
}'
APIKEY_RESP=$(jmap_post "$APIKEY_BODY")
BEARER=$(echo "$APIKEY_RESP" | jq -r '.methodResponses[0][1].created.k1.secret // empty')
if [[ -z "$BEARER" ]]; then
    echo "Failed to mint API key. Response was:" >&2
    echo "$APIKEY_RESP" | jq . >&2 || echo "$APIKEY_RESP" >&2
    exit 1
fi
# Print the full bearer immediately on mint. Earlier iterations
# truncated this to ${BEARER:0:16}...${BEARER: -8} as a
# leak-paranoia measure, but the value has to land in source
# (tests/common/mod.rs:FIXTURE_BEARER) anyway, so withholding the
# middle here just forces a re-run if the script later errors out
# before the final summary block prints it untruncated. The
# fixture's Stalwart instance is throwaway; the bearer authenticates
# only against the test fixture's persisted hash and isn't a
# production credential.
echo "  bearer: $BEARER"

echo "Adding plaintext IMAP listener on [::]:143 via x:NetworkListener/set..."
# bind is a Map<String, bool>: keys are address:port strings,
# values are enabled flags. The wizard's existing listeners all
# use "[::]:port": true (IPv6 all-addresses; dual-stack accepts
# IPv4 too). Set tlsImplicit:false explicitly to mirror the
# wizard's plaintext-protocol listeners (smtp on 25, sieve on
# 4190) where useTls is true but tlsImplicit is false -- in our
# case useTls=false suffices, but tlsImplicit=false is harmless
# and matches the existing convention.
LISTENER_BODY='{
  "using": ["urn:ietf:params:jmap:core", "urn:stalwart:jmap"],
  "methodCalls": [
    ["x:NetworkListener/set", {
       "create": {
         "imap-plain": {
           "name": "imap-plain",
           "protocol": "imap",
           "bind": {"[::]:143": true},
           "useTls": false,
           "tlsImplicit": false
         }
       }
    }, "c1"]
  ]
}'
LISTENER_RESP=$(jmap_post "$LISTENER_BODY")
LISTENER_CREATED=$(echo "$LISTENER_RESP" | jq -r '.methodResponses[0][1].created["imap-plain"] // empty')
if [[ -z "$LISTENER_CREATED" ]]; then
    echo "Failed to create IMAP listener. Response:" >&2
    echo "$LISTENER_RESP" | jq . >&2 || echo "$LISTENER_RESP" >&2
    exit 1
fi

echo "Enabling allowPlainTextAuth via x:Imap/set..."
IMAP_BODY='{
  "using": ["urn:ietf:params:jmap:core", "urn:stalwart:jmap"],
  "methodCalls": [
    ["x:Imap/set", {
       "update": {"singleton": {"allowPlainTextAuth": true}}
    }, "c1"]
  ]
}'
IMAP_RESP=$(jmap_post "$IMAP_BODY")
IMAP_UPDATED=$(echo "$IMAP_RESP" | jq -r '.methodResponses[0][1].updated.singleton // empty')
# Stalwart returns `null` (JSON null) for an updated singleton when
# there are no server-side changes to relay beyond the requested
# update -- which is the success case. Treat the absence of an
# `notUpdated` entry as success.
IMAP_NOT_UPDATED=$(echo "$IMAP_RESP" | jq -r '.methodResponses[0][1].notUpdated.singleton.type // empty')
if [[ -n "$IMAP_NOT_UPDATED" ]]; then
    echo "Failed to set allowPlainTextAuth. Response:" >&2
    echo "$IMAP_RESP" | jq . >&2 || echo "$IMAP_RESP" >&2
    exit 1
fi

echo "Restarting container so the new listener binds..."
# Stalwart binds all listeners once at startup
# (crates/main/src/main.rs spawns them in init.servers.spawn).
# x:Action/set { ReloadSettings } would reload the in-memory
# registry but doesn't rebind sockets, so a real restart is the
# only way to make the new imap-plain listener live.
docker restart "$CONTAINER" >/dev/null
wait_for_http "post-listener-config" 60

echo "Verifying IMAP listener on host port ${IMAP_PORT}..."
IMAP_BANNER=""
for _ in $(seq 1 30); do
    # /dev/tcp gives us a banner read without requiring nc/telnet.
    if banner=$( { exec 3<>"/dev/tcp/127.0.0.1/${IMAP_PORT}" && head -n 1 <&3 && exec 3<&- ; } 2>/dev/null); then
        IMAP_BANNER="$banner"
        break
    fi
    sleep 1
done
if [[ -z "$IMAP_BANNER" ]]; then
    echo "IMAP listener never came up on host port ${IMAP_PORT}" >&2
    echo "Check 'docker logs $CONTAINER' for what went wrong." >&2
    exit 1
fi
echo "  banner: ${IMAP_BANNER}"

echo
echo "Capturing state from the container..."

rm -rf "$FIXTURE_DIR/rocksdb"
docker cp "$CONTAINER:/etc/stalwart/config.json" "$FIXTURE_DIR/config.json"

# Read the RocksDB data path straight from the just-captured
# config.json. Stalwart writes the RocksDB SST/MANIFEST/CURRENT
# files directly into the configured path with no further
# subdirectory, so this is the path we copy out -- typically
# /var/lib/stalwart/. We keep the local fixture layout at
# tests/fixtures/stalwart/rocksdb/ (a more descriptive name than
# whatever the container path happens to be) and translate
# container-side at copy-back time.
ROCKSDB_PATH=$(jq -r '.path // empty' "$FIXTURE_DIR/config.json")
if [[ -z "$ROCKSDB_PATH" ]]; then
    echo "config.json missing .path; cannot locate the RocksDB directory" >&2
    exit 1
fi
# Trim any trailing slash so docker cp's source/dest semantics
# stay predictable.
ROCKSDB_PATH="${ROCKSDB_PATH%/}"
docker cp "$CONTAINER:$ROCKSDB_PATH/." "$FIXTURE_DIR/rocksdb"

# RocksDB's per-process flock marker. Shipping a stale one in the
# fixture risks confusing the next container's RocksDB at open
# time; it'll be recreated on startup anyway.
rm -f "$FIXTURE_DIR/rocksdb/LOCK"
# RocksDB's debug log files. Not load-bearing for correctness;
# strip them so the fixture stays minimal and reproducible.
rm -f "$FIXTURE_DIR/rocksdb/LOG" "$FIXTURE_DIR/rocksdb/LOG.old."*

ROCKSDB_FILE_COUNT=$(find "$FIXTURE_DIR/rocksdb" -type f 2>/dev/null | wc -l | tr -d ' ')
ROCKSDB_SIZE=$(du -sh "$FIXTURE_DIR/rocksdb" 2>/dev/null | awk '{print $1}')

# Mark the script as having reached completion so the cleanup
# trap tears the container down. Mid-script failures leave the
# container running for inspection.
SUCCESS=1

cat <<EOF

=== Fixture regenerated ===
  $FIXTURE_DIR/config.json
  $FIXTURE_DIR/rocksdb/  ($ROCKSDB_FILE_COUNT files, $ROCKSDB_SIZE)

Captured values for tests/common/mod.rs:
  ACCOUNT_IMAP_PASSWORD   = <whatever you typed for "Admin password" above>
  FIXTURE_BEARER          = $BEARER

Next steps:

  1. Edit tests/common/mod.rs:
       * ACCOUNT_IMAP_PASSWORD = <password>
       * FIXTURE_BEARER        = "$BEARER"
       * CONTAINER_DB_PATH     = "/var/lib/stalwart/rocksdb"
       * Replace the single \`include_bytes!\`/\`with_copy_to\` for the
         SQLite .db with an include_dir loop over the rocksdb/
         directory.

  2. Add include_dir to [dev-dependencies] in Cargo.toml:
       include_dir = "0.7"

  3. cargo test --lib --ignored e2e_initial_pull
       (the e2e suite is the canonical "fixture works" smoke test.)

  4. Once verified, delete the old SQLite fixture:
       rm tests/fixtures/stalwart/stalwart.db

  5. Re-run a bench-server seed to measure RocksDB throughput.
EOF
