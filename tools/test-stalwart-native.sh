#!/usr/bin/env bash
# Build outside the sandbox, then run real Stalwart suites with no host network
# or /var writes. Optional Cargo test-target names/globs default to e2e_flag_sync.
# Requires Linux user namespaces, Stalwart v0.16, Python 3, util-linux
# (unshare/mount), iproute2, and the normal Rust build environment.
set -euo pipefail

for tool in cargo python3 stalwart unshare mount ip realpath; do
    command -v "$tool" >/dev/null || { printf 'Missing native-test tool: %s\n' "$tool" >&2; exit 1; }
done

export JMA_TEST_NATIVE_STALWART
JMA_TEST_NATIVE_STALWART=$(realpath "$(command -v stalwart)")
targets=("$@")
if ((${#targets[@]} == 0)); then
    targets=(e2e_flag_sync)
fi
build_args=()
for target in "${targets[@]}"; do
    build_args+=(--test "$target")
done
# Cargo's structured output avoids guessing target-directory/hashes and avoids
# needing Cargo, downloads, or a compiler inside the private network namespace.
test_binaries=$(
    cargo test --locked "${build_args[@]}" --no-run --message-format=json |
        python3 -c 'import json,os,sys
paths={}
for line in sys.stdin:
    item=json.loads(line)
    target=item.get("target",{})
    if item.get("reason")=="compiler-artifact" and "test" in target.get("kind",[]) and item.get("executable"):
        paths[target["name"]]=os.path.realpath(item["executable"])
if not paths:
    raise SystemExit("Could not identify any compiled test executables")
for name in sorted(paths):
    print(paths[name])'
)
mapfile -t binaries <<<"$test_binaries"
for binary in "${binaries[@]}"; do
    case "$binary:$JMA_TEST_NATIVE_STALWART" in
        /var/*|*:/var/*) printf 'Native test binaries must live outside the isolated /var\n' >&2; exit 1 ;;
    esac
done

exec unshare --user --map-root-user --net --mount --propagation private -- bash -c '
set -euo pipefail
mount -t tmpfs tmpfs /var
mkdir -p /var/log /var/lib /var/cache
ip link set lo up
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy DISPLAY
export NO_PROXY=127.0.0.1,localhost,::1
export DBUS_SESSION_BUS_ADDRESS=unix:path=/var/jma-no-session-bus
failed=0
for binary in "$@"; do
    printf "\n=== Native Stalwart target: %s ===\n" "${binary##*/}"
    if ! "$binary" --ignored --nocapture --test-threads=1; then
        failed=1
    fi
done
exit "$failed"
' jma-native "${binaries[@]}"
