#!/usr/bin/env bash
# Regenerate the Stalwart fixture pair under tests/fixtures/stalwart/.
#
# The fixture consists of:
#   - config.json: Stalwart's persisted typed-JSON config blob,
#     pointing at a MySQL data store.
#   - mysql.sql: a mysqldump of the wizard-seeded Stalwart schema
#     and rows. At e2e test time this gets mounted into the MySQL
#     sidecar's /docker-entrypoint-initdb.d/ so the schema is
#     re-applied on first boot.
#
# This script:
#   1. Creates a private docker network.
#   2. Spawns a MySQL 8.4 container on it with fixed fixture
#      credentials (see below), aliased as `mysql` on the network.
#   3. Spawns a fresh Stalwart container on the same network so
#      the install wizard fires on its HTTP port.
#   4. Prints instructions for walking the wizard, including the
#      MySQL connection details to type. Stalwart writes its initial
#      schema and rows into MySQL during this step.
#   5. Applies three admin-API tweaks given the wizard-set admin
#      password:
#        a. Mint a long-lived API key (Bearer) via x:ApiKey/set.
#        b. Add a plaintext IMAP listener on 0.0.0.0:143 via
#           x:NetworkListener/set.
#        c. Enable allowPlainTextAuth on the IMAP service via
#           x:Imap/set (the singleton id is the literal "singleton").
#      Listener config doesn't hot-reload, so the container is
#      restarted after step (b) for the new IMAP listener to bind.
#   6. Captures config.json from Stalwart (docker cp) and a
#      mysqldump of the MySQL container into tests/fixtures/stalwart/.
#
# Requirements: docker, curl, jq.
#
# Fixture credentials (must match tests/common/mod.rs constants):
#   MYSQL_ROOT_PASSWORD = stalwart-fixture-root
#   MYSQL_DATABASE      = stalwart
#   MYSQL_USER          = stalwart
#   MYSQL_PASSWORD      = stalwart-fixture-user
#   In-network host     = mysql
#   In-network port     = 3306
#
# After this script finishes:
#   1. Update constants in tests/common/mod.rs:
#        ACCOUNT_IMAP_PASSWORD = <password you set in wizard>
#        FIXTURE_BEARER        = <bearer printed at end here>

set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null) || {
    echo "not inside a git checkout" >&2
    exit 1
}
FIXTURE_DIR="$REPO_ROOT/tests/fixtures/stalwart"
mkdir -p "$FIXTURE_DIR"

STALWART_IMAGE="stalwartlabs/stalwart:v0.16"
MYSQL_IMAGE="mysql:8.4"
HTTP_PORT="${WIZARD_HTTP_PORT:-8080}"
IMAP_PORT="${WIZARD_IMAP_PORT:-1143}"

# Stable resource names. The script tears these down on the happy
# path; mid-script failures leave them up for debugging.
MYSQL_NETWORK="jma-stalwart-fixture-net"
MYSQL_CONTAINER="jma-stalwart-fixture-mysql"
MYSQL_ALIAS="mysql"
STALWART_CONTAINER="jma-stalwart-fixture-stalwart"

# Mirror of tests/common/mod.rs:MYSQL_* constants. Keep in sync;
# the committed config.json's MySQL section captures whatever the
# user types into the wizard, which must match what spawn_mysql()
# advertises at test time.
MYSQL_ROOT_PASSWORD="stalwart-fixture-root"
MYSQL_DATABASE="stalwart"
MYSQL_USER="stalwart"
MYSQL_PASSWORD="stalwart-fixture-user"

for cmd in docker curl jq; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "$cmd not on PATH; install it first" >&2
        exit 1
    fi
done

if [[ -f "$FIXTURE_DIR/mysql.sql" && -z "${FORCE:-}" ]]; then
    echo "$FIXTURE_DIR/mysql.sql already exists; set FORCE=1 to overwrite" >&2
    exit 1
fi

echo "=== Stalwart fixture regeneration ==="
echo "  stalwart image: $STALWART_IMAGE"
echo "  mysql image:    $MYSQL_IMAGE"
echo "  http port:      $HTTP_PORT (override via WIZARD_HTTP_PORT)"
echo "  imap port:      $IMAP_PORT (override via WIZARD_IMAP_PORT)"
echo "  fixture dir:    $FIXTURE_DIR"
echo

# Sweep up any debris from a prior failed run so we don't fight
# with "name already in use" errors on docker create.
echo "Sweeping stale containers/network (if any)..."
docker rm -f "$STALWART_CONTAINER" >/dev/null 2>&1 || true
docker rm -f "$MYSQL_CONTAINER"    >/dev/null 2>&1 || true
docker network rm "$MYSQL_NETWORK" >/dev/null 2>&1 || true

echo "Creating docker network $MYSQL_NETWORK..."
docker network create "$MYSQL_NETWORK" >/dev/null

# Set SUCCESS=1 just before clean exit so the trap only tears
# things down on a happy path. Mid-script failures leave both
# containers and the network up for inspection.
SUCCESS=0
cleanup() {
    if (( SUCCESS )); then
        echo "Stopping containers and removing network..."
        docker stop "$STALWART_CONTAINER" >/dev/null 2>&1 || true
        docker rm   "$STALWART_CONTAINER" >/dev/null 2>&1 || true
        docker stop "$MYSQL_CONTAINER"    >/dev/null 2>&1 || true
        docker rm   "$MYSQL_CONTAINER"    >/dev/null 2>&1 || true
        docker network rm "$MYSQL_NETWORK" >/dev/null 2>&1 || true
    else
        cat >&2 <<EOM

Script exited before completion. Containers and network preserved
for debugging:
  mysql container:    $MYSQL_CONTAINER
  stalwart container: $STALWART_CONTAINER
  docker network:     $MYSQL_NETWORK
  stalwart http port: $HTTP_PORT
  stalwart imap port: $IMAP_PORT

Useful probes (run before cleaning up):
  docker logs $STALWART_CONTAINER 2>&1 | tail -100
  docker logs $MYSQL_CONTAINER    2>&1 | tail -100
  docker exec -it $MYSQL_CONTAINER \\
      mysql -u${MYSQL_USER} -p${MYSQL_PASSWORD} ${MYSQL_DATABASE}

Clean up when done:
  docker stop $STALWART_CONTAINER $MYSQL_CONTAINER
  docker rm   $STALWART_CONTAINER $MYSQL_CONTAINER
  docker network rm $MYSQL_NETWORK
EOM
    fi
}
trap cleanup EXIT INT TERM

echo "Spawning MySQL container..."
docker run -d \
    --name "$MYSQL_CONTAINER" \
    --network "$MYSQL_NETWORK" \
    --network-alias "$MYSQL_ALIAS" \
    -e "MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD" \
    -e "MYSQL_DATABASE=$MYSQL_DATABASE" \
    -e "MYSQL_USER=$MYSQL_USER" \
    -e "MYSQL_PASSWORD=$MYSQL_PASSWORD" \
    "$MYSQL_IMAGE" >/dev/null

# The mysql official image's init phase boots a temporary mysqld
# on a UNIX socket only (logged as `port: 0`), runs the init
# scripts, stops it, then starts the real mysqld bound to TCP
# 3306. We poll the log for the real-server readiness line
# specifically; matching the bare `ready for connections`
# substring is ambiguous (four hits per boot: X-Plugin + main
# mysqld, init phase + real phase) and the init-phase mysqld's
# `ready` fires before TCP 3306 is bound, so peers race the
# init->real-server restart and get connection refused. The
# anchored regex pins us to the `/usr/sbin/mysqld: ready for
# connections ... port: 3306` line on the real server.
echo "Waiting for MySQL to bind TCP 3306..."
mysql_ready=0
for _ in $(seq 1 120); do
    if docker logs "$MYSQL_CONTAINER" 2>&1 | \
        grep -qE '/usr/sbin/mysqld: ready for connections\..*port: 3306'; then
        mysql_ready=1
        break
    fi
    sleep 1
done
if (( ! mysql_ready )); then
    echo "MySQL never bound TCP 3306" >&2
    echo "Check 'docker logs $MYSQL_CONTAINER' for diagnostics." >&2
    exit 1
fi
echo "  mysql ready"

echo "Spawning Stalwart container..."
docker run -d \
    --name "$STALWART_CONTAINER" \
    --network "$MYSQL_NETWORK" \
    -p "${HTTP_PORT}:8080" \
    -p "${IMAP_PORT}:143" \
    -e "STALWART_PUBLIC_URL=http://127.0.0.1:${HTTP_PORT}" \
    -u 0:0 \
    "$STALWART_IMAGE" >/dev/null

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
  * Storage backend: MySQL  (NOT RocksDB / SQLite)
  * MySQL host:      $MYSQL_ALIAS
  * MySQL port:      3306
  * MySQL database:  $MYSQL_DATABASE
  * MySQL username:  $MYSQL_USER
  * MySQL password:  $MYSQL_PASSWORD

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
echo "Restarting Stalwart so the wizard admin credentials load..."
docker restart "$STALWART_CONTAINER" >/dev/null
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
    echo "Containers left running for inspection -- see cleanup trap output below." >&2
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
# Print the full bearer immediately on mint. The fixture's
# Stalwart instance is throwaway; the bearer authenticates only
# against the test fixture's persisted hash and isn't a production
# credential.
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

echo "Creating search-config singleton via x:Search/set..."
# The wizard does not pre-create the x:Search singleton, so this
# is a `create` (not `update`) with the full indexEmailFields
# map. Without this the FTS skips headers during indexing, so
# header-only searches (From:, Subject:, etc.) return empty;
# enabling the rest of the indexable email fields keeps the FTS
# behaviour symmetric with what a fully-tuned Stalwart deployment
# would expose.
SEARCH_BODY='{
  "using": ["urn:ietf:params:jmap:core", "urn:stalwart:jmap"],
  "methodCalls": [
    ["x:Search/set", {
       "create": {
         "singleton": {
           "indexEmailFields": {
             "from": true, "to": true, "cc": true, "bcc": true,
             "subject": true, "body": true, "attachment": true,
             "receivedAt": true, "sentAt": true, "size": true,
             "hasAttachment": true, "headers": true
           }
         }
       }
    }, "c1"]
  ]
}'
SEARCH_RESP=$(jmap_post "$SEARCH_BODY")
SEARCH_NOT_CREATED=$(echo "$SEARCH_RESP" | jq -r '.methodResponses[0][1].notCreated.singleton.type // empty')
if [[ -n "$SEARCH_NOT_CREATED" ]]; then
    echo "Failed to create search-config singleton. Response:" >&2
    echo "$SEARCH_RESP" | jq . >&2 || echo "$SEARCH_RESP" >&2
    exit 1
fi

echo "Restarting Stalwart so the new listener binds..."
# Stalwart binds all listeners once at startup
# (crates/main/src/main.rs spawns them in init.servers.spawn).
# x:Action/set { ReloadSettings } would reload the in-memory
# registry but doesn't rebind sockets, so a real restart is the
# only way to make the new imap-plain listener live.
docker restart "$STALWART_CONTAINER" >/dev/null
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
    echo "Check 'docker logs $STALWART_CONTAINER' for what went wrong." >&2
    exit 1
fi
echo "  banner: ${IMAP_BANNER}"

echo
echo "Capturing state from the containers..."

docker cp "$STALWART_CONTAINER:/etc/stalwart/config.json" \
    "$FIXTURE_DIR/config.json"

# --single-transaction for a consistent snapshot without locking
# the whole database; --routines/--triggers in case Stalwart's
# schema declares them; --no-tablespaces because the InnoDB
# tablespace files aren't part of the dump we'd replay.
#
# Dump to a temp file and rename into place so a mid-stream
# mysqldump failure can't leave a half-written fixture on disk
# that a subsequent `cargo test` would silently consume.
# MYSQL_PWD (over `-p<password>`) keeps the password out of argv
# and suppresses mysqldump's "insecure password on the command
# line" warning on every run.
tmpsql=$(mktemp "${TMPDIR:-/tmp}/jma-mysql.XXXXXX.sql")
docker exec -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" "$MYSQL_CONTAINER" \
    mysqldump \
        -uroot \
        --single-transaction \
        --routines \
        --triggers \
        --no-tablespaces \
        "$MYSQL_DATABASE" \
    > "$tmpsql"
mv "$tmpsql" "$FIXTURE_DIR/mysql.sql"

SQL_SIZE=$(du -sh "$FIXTURE_DIR/mysql.sql" 2>/dev/null | awk '{print $1}')

# Mark the script as having reached completion so the cleanup
# trap tears the containers and network down. Mid-script failures
# leave them running for inspection.
SUCCESS=1

cat <<EOF

=== Fixture regenerated ===
  $FIXTURE_DIR/config.json
  $FIXTURE_DIR/mysql.sql  ($SQL_SIZE)

Captured values for tests/common/mod.rs:
  ACCOUNT_IMAP_PASSWORD = <whatever you typed for "Admin password" above>
  FIXTURE_BEARER        = $BEARER

Next steps:

  1. Edit tests/common/mod.rs:
       * ACCOUNT_IMAP_PASSWORD = <password>
       * FIXTURE_BEARER        = "$BEARER"

  2. cargo test --test e2e_initial_pull -- --ignored --nocapture
     (the e2e suite is the canonical "fixture works" smoke test.)

  3. Commit the regenerated fixture and the mod.rs constants
     together so the trio stays self-consistent:
       * tests/common/mod.rs
       * tests/fixtures/stalwart/config.json
       * tests/fixtures/stalwart/mysql.sql
EOF
