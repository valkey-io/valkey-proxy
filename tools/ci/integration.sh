#!/usr/bin/env bash
# Bring up a local Valkey cluster plus two valkey-proxy instances and run the
# integration test suites against them. Used by .github/workflows/ci.yml and
# meant to be run the same way locally (see README "Continuous integration").
#
# Usage: tools/ci/integration.sh <command>
#   cluster   start NODES valkey-server processes (cluster mode, modules
#             loaded when MODULE_DIR has them) and create the cluster
#   proxies   start the main proxy (PROXY_PORT) and the "reported identity"
#             proxy (REPORTED_PORT)
#   test      run the default suites (+ --cluster/--reported), then the
#             module suites (bloomfilter / json / search)
#   stop      shut everything down
#   all       cluster + proxies + test, always followed by stop
#
# Needs: valkey-server and valkey-cli on PATH (or VALKEY_SERVER/VALKEY_CLI),
# ./valkey-proxy and ./run-test already built (make && make test).
# Logs and node data go to CI_DIR (default build/ci).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CI_DIR="${CI_DIR:-$ROOT/build/ci}"
VALKEY_SERVER="${VALKEY_SERVER:-valkey-server}"
VALKEY_CLI="${VALKEY_CLI:-valkey-cli}"
MODULE_DIR="${MODULE_DIR:-/usr/lib/valkey}"
HOST="${HOST:-127.0.0.1}"
BASE_PORT="${BASE_PORT:-7001}"
NODES="${NODES:-6}"
REPLICAS="${REPLICAS:-1}"
PROXY_PORT="${PROXY_PORT:-7777}"
REPORTED_PORT="${REPORTED_PORT:-7778}"
# Module suites to run; set to "" to skip them (e.g. a server without modules)
MODULE_SUITES="${MODULE_SUITES-bloomfilter json search}"
# The test client has no socket timeouts, so a proxy that stops answering
# would hang the run forever; cap each run-test invocation instead.
TEST_TIMEOUT="${TEST_TIMEOUT:-600}"

log() { printf '==> %s\n' "$*"; }

wait_for_ping() { # host port what
    for _ in $(seq 1 100); do
        "$VALKEY_CLI" -h "$1" -p "$2" ping >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    echo "error: $3 on $1:$2 did not come up" >&2
    return 1
}

# loadmodule lines for the modules MODULE_DIR has. Search runs with its
# coordinator so FT.SEARCH / FT.INFO ... ALLSHARDS cover every shard. They go
# in a config file: on the command line "--use-coordinator" would be taken
# as a new server option rather than a module argument.
module_conf() {
    local m
    for m in libvalkey_bloom.so libjson.so "libsearch.so --use-coordinator"; do
        [ -f "$MODULE_DIR/${m%% *}" ] && echo "loadmodule $MODULE_DIR/$m"
    done
    return 0
}

start_cluster() {
    mkdir -p "$CI_DIR"
    local mods addrs="" i port dir
    mods="$(module_conf)"
    if [ -n "$mods" ]; then log "modules:" && echo "$mods"; else log "no modules found in $MODULE_DIR"; fi
    for i in $(seq 0 $((NODES - 1))); do
        port=$((BASE_PORT + i))
        dir="$CI_DIR/node-$port"
        rm -rf "$dir" && mkdir -p "$dir"
        cat >"$dir/valkey.conf" <<EOF
port $port
bind $HOST
dir $dir
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
appendonly no
save ""
enable-debug-command yes
enable-module-command yes
daemonize yes
pidfile $dir/valkey.pid
logfile $dir/valkey.log
$mods
EOF
        "$VALKEY_SERVER" "$dir/valkey.conf"
        addrs="$addrs $HOST:$port"
    done
    for i in $(seq 0 $((NODES - 1))); do
        wait_for_ping "$HOST" $((BASE_PORT + i)) valkey-server
    done
    log "creating cluster:$addrs (replicas $REPLICAS)"
    # shellcheck disable=SC2086
    "$VALKEY_CLI" --cluster create $addrs --cluster-replicas "$REPLICAS" --cluster-yes
    for _ in $(seq 1 100); do
        if "$VALKEY_CLI" -h "$HOST" -p "$BASE_PORT" cluster info | grep -q 'cluster_state:ok' &&
            "$VALKEY_CLI" --cluster check "$HOST:$BASE_PORT" >/dev/null 2>&1; then
            log "cluster is ok"
            "$VALKEY_CLI" -h "$HOST" -p "$BASE_PORT" cluster nodes
            return 0
        fi
        sleep 0.2
    done
    echo "error: cluster did not reach cluster_state:ok" >&2
    "$VALKEY_CLI" --cluster check "$HOST:$BASE_PORT" || true
    return 1
}

start_proxy() { # port extra-config
    local port=$1 conf="$CI_DIR/proxy-$1.conf"
    cat >"$conf" <<EOF
server-ip    $HOST
server-port  $BASE_PORT
bind         $HOST
port         $port
cluster_refresh_interval_ms  500
$2
EOF
    "$ROOT/valkey-proxy" "$conf" >"$CI_DIR/proxy-$port.log" 2>&1 &
    echo $! >"$CI_DIR/proxy-$port.pid"
    wait_for_ping "$HOST" "$port" "valkey-proxy (log: $CI_DIR/proxy-$port.log)"
    log "valkey-proxy listening on $HOST:$port"
}

start_proxies() {
    mkdir -p "$CI_DIR"
    start_proxy "$PROXY_PORT" ""
    start_proxy "$REPORTED_PORT" "reported_version  8.0.2
reported_role     replica
reported_modules  search:10002,json"
}

run_tests() {
    local rc=0 s args=""
    # stdbuf: line-buffered output so a hang shows the last test that ran
    log "default suites"
    timeout "$TEST_TIMEOUT" stdbuf -oL "$ROOT/run-test" -h="$HOST" -p="$PROXY_PORT" \
        --cluster="$HOST:$BASE_PORT" --reported="$HOST:$REPORTED_PORT" 2>&1 |
        tee "$CI_DIR/run-test-default.log" || rc=1
    for s in $MODULE_SUITES; do args="$args --$s"; done
    if [ -n "$args" ]; then
        log "module suites:$args"
        # shellcheck disable=SC2086
        timeout "$TEST_TIMEOUT" stdbuf -oL "$ROOT/run-test" -h="$HOST" -p="$PROXY_PORT" $args 2>&1 |
            tee "$CI_DIR/run-test-modules.log" || rc=1
    fi
    # A proxy that died mid-run would otherwise only show up as failed tests.
    for s in "$PROXY_PORT" "$REPORTED_PORT"; do
        if ! kill -0 "$(cat "$CI_DIR/proxy-$s.pid")" 2>/dev/null; then
            echo "error: valkey-proxy on port $s is no longer running (see $CI_DIR/proxy-$s.log)" >&2
            rc=1
        fi
    done
    return $rc
}

stop_all() {
    local f
    for f in "$CI_DIR"/proxy-*.pid "$CI_DIR"/node-*/valkey.pid; do
        if [ -f "$f" ]; then kill "$(cat "$f")" 2>/dev/null || true; fi
    done
    log "stopped"
}

case "${1:-}" in
cluster) start_cluster ;;
proxies) start_proxies ;;
test) run_tests ;;
stop) stop_all ;;
all)
    trap stop_all EXIT
    start_cluster
    start_proxies
    run_tests
    ;;
*)
    sed -n '2,20p' "$0" >&2
    exit 2
    ;;
esac
