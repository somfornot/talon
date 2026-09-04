#!/usr/bin/env bash
# Real C ABI -> coordinator -> worker -> origin capacity and fault benchmark.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/release"
MODE="smoke"
DO_BUILD=1
OUTPUT="${TALON_C_BENCH_OUTPUT:-$ROOT/bench/results/c-client-latest.jsonl}"
PORT_BASE="${TALON_C_BENCH_PORT_BASE:-18700}"
WARMUP_SECONDS="${TALON_C_BENCH_WARMUP_SECONDS:-3}"
MEASURE_SECONDS="${TALON_C_BENCH_MEASURE_SECONDS:-10}"
REPEATS="${TALON_C_BENCH_REPEATS:-3}"
SMALL_CONCURRENCY="${TALON_C_BENCH_SMALL_CONCURRENCY:-1 8 32 64 128 256 512 1024}"
LARGE_CONCURRENCY="${TALON_C_BENCH_LARGE_CONCURRENCY:-1 8 32 64 128 256}"
BACKEND_CONCURRENCY="${TALON_C_BENCH_BACKEND_CONCURRENCY:-1 8 32 64 128 256}"
FAILURE_CONCURRENCY="${TALON_C_BENCH_FAILURE_CONCURRENCY:-64 256 512}"
REQUEST_SIZES="${TALON_C_BENCH_REQUEST_SIZES:-4096 65536 1048576}"
BACKEND_OBJECTS="${TALON_C_BENCH_BACKEND_OBJECTS:-16}"
L2_PAGE_SIZE="${TALON_C_BENCH_L2_PAGE_SIZE:-1048576}"
MEMBERSHIP_CONCURRENCY="${TALON_C_BENCH_MEMBERSHIP_CONCURRENCY:-256}"
MEMBERSHIP_SECONDS="${TALON_C_BENCH_MEMBERSHIP_SECONDS:-12}"
KEEP_RUN_DIR="${TALON_C_BENCH_KEEP_RUN_DIR:-1}"

ORIGIN_PORT=$((PORT_BASE + 0))
COORD_PORT=$((PORT_BASE + 1))
COORD_ADMIN_PORT=$((PORT_BASE + 2))
WORKER1_PORT=$((PORT_BASE + 3))
WORKER1_ADMIN_PORT=$((PORT_BASE + 4))
PROXY_PORT=$((PORT_BASE + 5))
PROXY_ADMIN_PORT=$((PORT_BASE + 6))
WORKER2_PORT=$((PORT_BASE + 7))
WORKER2_ADMIN_PORT=$((PORT_BASE + 8))
BLOCK_SIZE=268435456
HOT_CAPACITY=268435456
PAGED_CAPACITY=268435456
OBJECT_URI="az://container/bench"

RUN_DIR=""
ORIGIN_PID=""
COORD_PID=""
WORKER1_PID=""
WORKER2_PID=""
PROXY_PID=""
STACK_SEQ=0

usage() {
    cat <<'USAGE'
usage: scripts/c_client_loadtest.sh [MODE] [--no-build] [--output PATH]

MODE is smoke, hot, latency, backend, failure, membership, or all.
The full defaults are 3 s warmup, 10 s measurement, and 3 repeats. Override
matrix dimensions with TALON_C_BENCH_* environment variables documented in
BENCHMARKS.md. Results are JSON Lines.
USAGE
}

while (($#)); do
    case "$1" in
        smoke|hot|latency|backend|failure|membership|all)
            MODE="$1"
            shift
            ;;
        --no-build)
            DO_BUILD=0
            shift
            ;;
        --output)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            OUTPUT="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

require_command() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "required command not found: $1" >&2
        exit 2
    }
}

curl_local() {
    curl --noproxy '*' --connect-timeout 1 --max-time 2 "$@"
}

wait_http() {
    local url=$1
    local expected=${2:-200}
    local attempts=${3:-200}
    local code
    for ((i = 0; i < attempts; ++i)); do
        code=$(curl_local -sS -o /dev/null -w '%{http_code}' "$url" 2>/dev/null || true)
        if [[ "$code" == "$expected" ]]; then
            return 0
        fi
        sleep 0.05
    done
    echo "timed out waiting for $url (wanted HTTP $expected)" >&2
    return 1
}

stop_pid() {
    local pid=${1:-}
    [[ -n "$pid" ]] || return 0
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        for _ in {1..20}; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.05
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
    wait "$pid" 2>/dev/null || true
}

stop_stack() {
    stop_pid "$PROXY_PID"
    stop_pid "$WORKER2_PID"
    stop_pid "$WORKER1_PID"
    stop_pid "$COORD_PID"
    PROXY_PID=""
    WORKER2_PID=""
    WORKER1_PID=""
    COORD_PID=""
}

cleanup() {
    stop_stack
    stop_pid "$ORIGIN_PID"
    if [[ "$KEEP_RUN_DIR" == 0 && -n "$RUN_DIR" ]]; then
        rm -rf -- "$RUN_DIR"
    elif [[ -n "$RUN_DIR" ]]; then
        echo "benchmark artifacts: $RUN_DIR" >&2
    fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

build_tools() {
    if [[ "$DO_BUILD" == 1 ]]; then
        cargo build --release --locked -p talon-c -p talon-coordinator
        cargo build --release --locked -p talon-worker \
            --bin talon-worker --bin talon-client-bench-proxy
    fi
    local cc=${CC:-cc}
    "$cc" -O2 -std=c11 -Wall -Wextra -Werror -pedantic -pthread \
        -I"$ROOT/clients/c/include" "$ROOT/clients/c/examples/loadgen.c" \
        -L"$BIN" -Wl,-rpath,"$BIN" -ltalon_c \
        -o "$RUN_DIR/talon-c-loadgen"
}

start_origin() {
    python3 "$ROOT/scripts/loadtest_origin.py" "$ORIGIN_PORT" \
        >"$RUN_DIR/logs/origin.log" 2>&1 &
    ORIGIN_PID=$!
    for _ in {1..200}; do
        if curl_local -fsSI "http://127.0.0.1:$ORIGIN_PORT/account/container/bench" \
            >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.05
    done
    echo "origin did not start; see $RUN_DIR/logs/origin.log" >&2
    return 1
}

start_worker() {
    local number=$1
    local data_port=$2
    local admin_port=$3
    local advertise=$4
    local backend_delay=$5
    local page_size=$6
    local capacity=$7
    local heartbeat=$8
    local cache="$RUN_DIR/cache-$STACK_SEQ-$number"
    local config="$RUN_DIR/worker-$STACK_SEQ-$number.toml"
    mkdir -p "$cache"
    cat >"$config" <<CFG
backend = "azure"
azure_account = "account"
azure_endpoint = "http://127.0.0.1:$ORIGIN_PORT"
cache_dirs = ["$cache"]
block_size = $BLOCK_SIZE
capacity_bytes = $capacity
l1_capacity_bytes = 0
l2_page_size_bytes = $page_size
backend_delay_ms = $backend_delay
backend_jitter_ms = 0
CFG
    local -a worker_env=(
        env
        # Runtime logs a line for every cache hit at INFO. A capacity run must
        # not turn the measured data path into tens of GiB of log-file writes;
        # WARN still preserves injected and unexpected worker failures.
        "RUST_LOG=warn"
        "TALON_WORKER_AZURE_SAS=sv=bench&sig=bench"
        "NO_PROXY=127.0.0.1,localhost"
        "no_proxy=127.0.0.1,localhost"
    )
    if [[ "${TALON_C_BENCH_FORCE_TOKIO:-0}" == 1 ]]; then
        worker_env+=(TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1)
    fi
    "${worker_env[@]}" "$BIN/talon-worker" --config "$config" \
        --listen "127.0.0.1:$data_port" --advertise-addr "$advertise" \
        --admin-listen "127.0.0.1:$admin_port" \
        --coordinator "127.0.0.1:$COORD_PORT" \
        --node-id "bench-worker-$number" --heartbeat-interval-ms "$heartbeat" \
        >"$RUN_DIR/logs/worker-$STACK_SEQ-$number.log" 2>&1 &
    if [[ "$number" == 1 ]]; then
        WORKER1_PID=$!
    else
        WORKER2_PID=$!
    fi
}

start_stack() {
    local route=$1
    local proxy_delay=$2
    local fail_every=$3
    local backend_delay=$4
    local page_size=$5
    local capacity=$6
    local workers=${7:-1}
    local timing=${8:-normal}
    stop_stack
    STACK_SEQ=$((STACK_SEQ + 1))

    local coordinator_heartbeat=250
    local unhealthy=1000
    local lease=3000
    local worker_heartbeat=250
    if [[ "$timing" == membership ]]; then
        coordinator_heartbeat=100
        unhealthy=500
        lease=1500
        worker_heartbeat=100
    fi

    "$BIN/talon-coordinator" --listen "127.0.0.1:$COORD_PORT" \
        --admin-listen "127.0.0.1:$COORD_ADMIN_PORT" \
        --heartbeat-interval-ms "$coordinator_heartbeat" \
        --unhealthy-after-ms "$unhealthy" --lease-ttl-ms "$lease" \
        >"$RUN_DIR/logs/coordinator-$STACK_SEQ.log" 2>&1 &
    COORD_PID=$!
    wait_http "http://127.0.0.1:$COORD_ADMIN_PORT/readyz"

    local advertise="127.0.0.1:$WORKER1_PORT"
    if [[ "$route" == proxy ]]; then
        "$BIN/talon-client-bench-proxy" \
            --listen "127.0.0.1:$PROXY_PORT" \
            --admin-listen "127.0.0.1:$PROXY_ADMIN_PORT" \
            --upstream "127.0.0.1:$WORKER1_PORT" \
            --delay-ms "$proxy_delay" --fail-every "$fail_every" \
            >"$RUN_DIR/logs/proxy-$STACK_SEQ.log" 2>&1 &
        PROXY_PID=$!
        wait_http "http://127.0.0.1:$PROXY_ADMIN_PORT/stats"
        advertise="127.0.0.1:$PROXY_PORT"
    fi

    start_worker 1 "$WORKER1_PORT" "$WORKER1_ADMIN_PORT" "$advertise" \
        "$backend_delay" "$page_size" "$capacity" "$worker_heartbeat"
    if [[ "$workers" == 2 ]]; then
        start_worker 2 "$WORKER2_PORT" "$WORKER2_ADMIN_PORT" \
            "127.0.0.1:$WORKER2_PORT" "$backend_delay" "$page_size" "$capacity" \
            "$worker_heartbeat"
    fi
    wait_http "http://127.0.0.1:$WORKER1_ADMIN_PORT/readyz"
    if [[ "$workers" == 2 ]]; then
        wait_http "http://127.0.0.1:$WORKER2_ADMIN_PORT/readyz"
    fi
    # Readiness precedes the next reconciliation tick; wait until the public
    # node view proves every worker is a placement candidate.
    for _ in {1..200}; do
        local count
        count=$(curl_local -fsS "http://127.0.0.1:$COORD_ADMIN_PORT/api/v1/nodes?role=worker" \
            | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("nodes", [])))' \
            2>/dev/null || echo 0)
        if [[ "$count" == "$workers" ]]; then
            return 0
        fi
        sleep 0.05
    done
    echo "workers did not enter coordinator membership; inspect $RUN_DIR/logs" >&2
    return 1
}

metric_value() {
    local port=$1
    local series=$2
    curl_local -fsS "http://127.0.0.1:$port/metrics" 2>/dev/null \
        | awk -v wanted="$series" '$1 == wanted {sum += $2} END {printf "%.0f", sum + 0}'
}

worker_metric_total() {
    local series=$1
    local total=0
    local value
    if [[ -n "$WORKER1_PID" ]] && kill -0 "$WORKER1_PID" 2>/dev/null; then
        value=$(metric_value "$WORKER1_ADMIN_PORT" "$series")
        total=$((total + value))
    fi
    if [[ -n "$WORKER2_PID" ]] && kill -0 "$WORKER2_PID" 2>/dev/null; then
        value=$(metric_value "$WORKER2_ADMIN_PORT" "$series")
        total=$((total + value))
    fi
    echo "$total"
}

json_field() {
    local json=$1
    local field=$2
    python3 -c 'import json,sys; print(json.loads(sys.argv[1]).get(sys.argv[2], 0))' \
        "$json" "$field"
}

proxy_snapshot() {
    if [[ -n "$PROXY_PID" ]] && kill -0 "$PROXY_PID" 2>/dev/null; then
        curl_local -fsS "http://127.0.0.1:$PROXY_ADMIN_PORT/stats"
    else
        echo '{}'
    fi
}

process_ticks() {
    local pid=$1
    awk '{print $14 + $15}' "/proc/$pid/stat"
}

process_rss_kib() {
    local pid=$1
    awk '/^VmRSS:/ {print $2}' "/proc/$pid/status"
}

run_one() {
    local scenario=$1
    local request_bytes=$2
    local concurrency=$3
    local round=$4
    local objects=${5:-1}
    local warmup=${6:-$WARMUP_SECONDS}
    local seconds=${7:-$MEASURE_SECONDS}
    local bucket_ms=${8:-0}

    local before_requests before_misses before_backend before_membership before_proxy
    before_requests=$(worker_metric_total 'talon_worker_requests_total')
    before_misses=$(worker_metric_total 'talon_worker_cache_tier_misses_total{tier="l2"}')
    before_backend=$(worker_metric_total 'talon_worker_backend_fetch_duration_seconds_count{backend="azure"}')
    before_membership=$(metric_value "$COORD_ADMIN_PORT" \
        'talon_coordinator_control_requests_total{operation="membership_query"}')
    before_proxy=$(proxy_snapshot)
    local before_proxy_attempts before_proxy_forwarded before_proxy_injected
    before_proxy_attempts=$(json_field "$before_proxy" attempts)
    before_proxy_forwarded=$(json_field "$before_proxy" forwarded)
    before_proxy_injected=$(json_field "$before_proxy" injected_unavailable)

    local ticks_before ns_before
    ticks_before=$(process_ticks "$WORKER1_PID")
    ns_before=$(date +%s%N)
    local loadgen_status=0
    LD_LIBRARY_PATH="$BIN${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
        "$RUN_DIR/talon-c-loadgen" --coordinator "127.0.0.1:$COORD_PORT" \
        --uri "$OBJECT_URI" --request-bytes "$request_bytes" \
        --concurrency "$concurrency" --warmup-seconds "$warmup" \
        --seconds "$seconds" --object-count "$objects" --bucket-ms "$bucket_ms" \
        --scenario "$scenario" --round "$round" | tee -a "$OUTPUT" \
        || loadgen_status=${PIPESTATUS[0]}
    local ns_after ticks_after
    ns_after=$(date +%s%N)
    ticks_after=$(process_ticks "$WORKER1_PID")

    local after_requests after_misses after_backend after_membership after_proxy
    after_requests=$(worker_metric_total 'talon_worker_requests_total')
    after_misses=$(worker_metric_total 'talon_worker_cache_tier_misses_total{tier="l2"}')
    after_backend=$(worker_metric_total 'talon_worker_backend_fetch_duration_seconds_count{backend="azure"}')
    after_membership=$(metric_value "$COORD_ADMIN_PORT" \
        'talon_coordinator_control_requests_total{operation="membership_query"}')
    after_proxy=$(proxy_snapshot)
    local after_proxy_attempts after_proxy_forwarded after_proxy_injected
    after_proxy_attempts=$(json_field "$after_proxy" attempts)
    after_proxy_forwarded=$(json_field "$after_proxy" forwarded)
    after_proxy_injected=$(json_field "$after_proxy" injected_unavailable)

    local worker_requests worker_attempts proxy_attempts injected logical_attempts logical_errors
    local retry_attempts retry_succeeded last_summary
    worker_requests=$((after_requests - before_requests))
    # Every loadgen invocation performs exactly one startup stat through a
    # worker. Keep the raw count but exclude that control request from read QPS.
    worker_attempts=$((worker_requests - 1))
    if ((worker_attempts < 0)); then
        worker_attempts=0
    fi
    proxy_attempts=$((after_proxy_attempts - before_proxy_attempts))
    injected=$((after_proxy_injected - before_proxy_injected))
    last_summary=$(tail -n 1 "$OUTPUT")
    logical_attempts=$(json_field "$last_summary" total_attempts)
    logical_errors=$(json_field "$last_summary" total_logical_errors)
    retry_attempts=$((proxy_attempts - logical_attempts))
    if ((retry_attempts < 0)); then
        retry_attempts=0
    fi
    retry_succeeded=$((retry_attempts - logical_errors))
    if ((retry_succeeded < 0)); then
        retry_succeeded=0
    fi
    local hz cpu_cores rss observation_seconds worker_attempt_qps proxy_attempt_qps
    local backend_fetches backend_fetch_qps l2_misses l2_miss_qps
    hz=$(getconf CLK_TCK)
    cpu_cores=$(awk -v ticks="$((ticks_after - ticks_before))" -v hz="$hz" \
        -v ns="$((ns_after - ns_before))" \
        'BEGIN {if (ns == 0) print 0; else printf "%.3f", (ticks / hz) / (ns / 1000000000)}')
    rss=$(process_rss_kib "$WORKER1_PID")
    observation_seconds=$((warmup + seconds))
    l2_misses=$((after_misses - before_misses))
    backend_fetches=$((after_backend - before_backend))
    worker_attempt_qps=$(awk -v attempts="$worker_attempts" -v seconds="$observation_seconds" \
        'BEGIN {printf "%.3f", attempts / seconds}')
    proxy_attempt_qps=$(awk -v attempts="$proxy_attempts" -v seconds="$observation_seconds" \
        'BEGIN {printf "%.3f", attempts / seconds}')
    l2_miss_qps=$(awk -v attempts="$l2_misses" -v seconds="$observation_seconds" \
        'BEGIN {printf "%.3f", attempts / seconds}')
    backend_fetch_qps=$(awk -v attempts="$backend_fetches" -v seconds="$observation_seconds" \
        'BEGIN {printf "%.3f", attempts / seconds}')
    printf '{"type":"stack_metrics","scenario":"%s","round":%s,"request_bytes":%s,"concurrency":%s,"observation_seconds":%s,"logical_attempts":%s,"worker_requests_including_stat":%s,"worker_attempts":%s,"worker_attempt_qps":%s,"l2_cache_misses":%s,"l2_cache_miss_qps":%s,"backend_fetches":%s,"backend_fetch_qps":%s,"membership_queries":%s,"worker_cpu_cores":%s,"worker_rss_kib":%s,"proxy_attempts":%s,"proxy_attempt_qps":%s,"proxy_forwarded":%s,"proxy_injected_unavailable":%s,"retry_attempts":%s,"retry_succeeded":%s}\n' \
        "$scenario" "$round" "$request_bytes" "$concurrency" \
        "$observation_seconds" "$logical_attempts" "$worker_requests" \
        "$worker_attempts" "$worker_attempt_qps" \
        "$l2_misses" "$l2_miss_qps" "$backend_fetches" "$backend_fetch_qps" \
        "$((after_membership - before_membership))" \
        "$cpu_cores" "$rss" "$proxy_attempts" "$proxy_attempt_qps" \
        "$((after_proxy_forwarded - before_proxy_forwarded))" \
        "$injected" "$retry_attempts" "$retry_succeeded" | tee -a "$OUTPUT"
    return "$loadgen_status"
}

run_matrix() {
    local scenario=$1
    local objects=${2:-1}
    local request_bytes concurrency round concurrencies
    for request_bytes in $REQUEST_SIZES; do
        concurrencies="$SMALL_CONCURRENCY"
        if [[ "$request_bytes" -ge 1048576 ]]; then
            concurrencies="$LARGE_CONCURRENCY"
        fi
        for concurrency in $concurrencies; do
            for ((round = 1; round <= REPEATS; ++round)); do
                run_one "$scenario" "$request_bytes" "$concurrency" "$round" "$objects"
            done
        done
    done
}

run_hot() {
    start_stack direct 0 0 0 "$L2_PAGE_SIZE" "$HOT_CAPACITY"
    run_matrix hot-direct 1
    stop_stack
}

run_latency() {
    local delay
    for delay in 0 1 5 10; do
        start_stack proxy "$delay" 0 0 "$L2_PAGE_SIZE" "$HOT_CAPACITY"
        run_matrix "latency-${delay}ms" 1
        stop_stack
    done
}

run_backend() {
    local delay request_bytes concurrency round
    for delay in 0 5 20 50; do
        start_stack direct 0 0 "$delay" "$L2_PAGE_SIZE" "$PAGED_CAPACITY"
        for request_bytes in $REQUEST_SIZES; do
            for concurrency in $BACKEND_CONCURRENCY; do
                for ((round = 1; round <= REPEATS; ++round)); do
                    run_one "backend-${delay}ms" "$request_bytes" "$concurrency" "$round" \
                        "$BACKEND_OBJECTS"
                done
            done
        done
        stop_stack
    done
}

run_failure() {
    local request_bytes concurrency round
    start_stack proxy 0 0 0 "$L2_PAGE_SIZE" "$HOT_CAPACITY"
    for request_bytes in $REQUEST_SIZES; do
        for concurrency in $FAILURE_CONCURRENCY; do
            for ((round = 1; round <= REPEATS; ++round)); do
                run_one failure-baseline "$request_bytes" "$concurrency" "$round" 1
            done
        done
    done
    stop_stack
    start_stack proxy 0 100 0 "$L2_PAGE_SIZE" "$HOT_CAPACITY"
    for request_bytes in $REQUEST_SIZES; do
        for concurrency in $FAILURE_CONCURRENCY; do
            for ((round = 1; round <= REPEATS; ++round)); do
                run_one failure-1pct "$request_bytes" "$concurrency" "$round" 1
            done
        done
    done
    stop_stack
}

node_present() {
    local node=$1
    curl_local -fsS "http://127.0.0.1:$COORD_ADMIN_PORT/api/v1/nodes?role=worker" \
        | python3 -c 'import json,sys; n=sys.argv[1]; print(int(any(x.get("node_id") == n for x in json.load(sys.stdin).get("nodes", []))))' \
            "$node"
}

run_membership() {
    start_stack direct 0 0 0 "$L2_PAGE_SIZE" "$HOT_CAPACITY" 2 membership
    local before1 before2 after1 after2
    before1=$(metric_value "$WORKER1_ADMIN_PORT" 'talon_worker_requests_total')
    before2=$(metric_value "$WORKER2_ADMIN_PORT" 'talon_worker_requests_total')
    LD_LIBRARY_PATH="$BIN${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
        "$RUN_DIR/talon-c-loadgen" --coordinator "127.0.0.1:$COORD_PORT" \
        --uri "$OBJECT_URI" --request-bytes 4096 --concurrency 1 \
        --warmup-seconds 0 --seconds 1 --scenario owner-probe \
        >"$RUN_DIR/owner-probe.jsonl"
    after1=$(metric_value "$WORKER1_ADMIN_PORT" 'talon_worker_requests_total')
    after2=$(metric_value "$WORKER2_ADMIN_PORT" 'talon_worker_requests_total')

    local owner owner_pid survivor_admin
    if ((after1 - before1 > after2 - before2)); then
        owner=bench-worker-1
        owner_pid=$WORKER1_PID
        survivor_admin=$WORKER2_ADMIN_PORT
    else
        owner=bench-worker-2
        owner_pid=$WORKER2_PID
        survivor_admin=$WORKER1_ADMIN_PORT
    fi
    local survivor_before
    survivor_before=$(metric_value "$survivor_admin" 'talon_worker_bytes_served_total')

    LD_LIBRARY_PATH="$BIN${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
        "$RUN_DIR/talon-c-loadgen" --coordinator "127.0.0.1:$COORD_PORT" \
        --uri "$OBJECT_URI" --request-bytes 65536 \
        --concurrency "$MEMBERSHIP_CONCURRENCY" --warmup-seconds "$WARMUP_SECONDS" \
        --seconds "$MEMBERSHIP_SECONDS" --object-count 1 --bucket-ms 1000 \
        --scenario membership --round 1 >"$RUN_DIR/membership-loadgen.jsonl" &
    local loadgen_pid=$!
    local measurement_start_ms=""
    for _ in {1..1000}; do
        if grep -q '"type":"measurement_start"' "$RUN_DIR/membership-loadgen.jsonl"; then
            measurement_start_ms=$(grep '"type":"measurement_start"' \
                "$RUN_DIR/membership-loadgen.jsonl" | tail -n 1 \
                | python3 -c 'import json,sys; print(json.load(sys.stdin)["unix_ms"])')
            break
        fi
        if ! kill -0 "$loadgen_pid" 2>/dev/null; then
            echo "membership loadgen exited before measurement" >&2
            cat "$RUN_DIR/membership-loadgen.jsonl" >&2
            return 1
        fi
        sleep 0.01
    done
    if [[ -z "$measurement_start_ms" ]]; then
        echo "membership loadgen did not publish its measurement boundary" >&2
        return 1
    fi
    sleep 5
    local killed_ms
    killed_ms=$(date +%s%3N)
    kill -KILL "$owner_pid"
    wait "$owner_pid" 2>/dev/null || true
    if [[ "$owner" == bench-worker-1 ]]; then
        WORKER1_PID=""
    else
        WORKER2_PID=""
    fi

    local excluded_ms="" survivor_ms="" deadline now current
    deadline=$((killed_ms + 10000))
    while :; do
        now=$(date +%s%3N)
        if [[ -z "$excluded_ms" ]] && [[ "$(node_present "$owner" 2>/dev/null || echo 1)" == 0 ]]; then
            excluded_ms=$((now - killed_ms))
        fi
        current=$(metric_value "$survivor_admin" 'talon_worker_bytes_served_total')
        if [[ -z "$survivor_ms" ]] && ((current > survivor_before)); then
            survivor_ms=$((now - killed_ms))
        fi
        if [[ -n "$excluded_ms" && -n "$survivor_ms" ]]; then
            break
        fi
        if ((now >= deadline)); then
            break
        fi
        sleep 0.02
    done
    excluded_ms=${excluded_ms:-null}
    survivor_ms=${survivor_ms:-null}
    local kill_after_measure_start_ms=$((killed_ms - measurement_start_ms))
    printf '{"type":"membership_event","owner":"%s","kill_after_measure_start_ms":%s,"coordinator_excluded_after_kill_ms":%s,"survivor_first_request_after_kill_ms":%s,"heartbeat_ms":100,"unhealthy_ms":500,"lease_ms":1500}\n' \
        "$owner" "$kill_after_measure_start_ms" "$excluded_ms" "$survivor_ms" \
        | tee -a "$OUTPUT"
    if ! wait "$loadgen_pid"; then
        echo "membership loadgen failed" >&2
        cat "$RUN_DIR/membership-loadgen.jsonl" >&2
        return 1
    fi
    tee -a "$OUTPUT" <"$RUN_DIR/membership-loadgen.jsonl"
    stop_stack
}

run_smoke() {
    local saved_warmup=$WARMUP_SECONDS
    local saved_seconds=$MEASURE_SECONDS
    WARMUP_SECONDS=1
    MEASURE_SECONDS=2
    start_stack direct 0 0 0 "$L2_PAGE_SIZE" "$HOT_CAPACITY"
    run_one smoke-direct 4096 8 1
    stop_stack
    start_stack proxy 1 100 0 "$L2_PAGE_SIZE" "$HOT_CAPACITY"
    run_one smoke-proxy-failure 65536 32 1
    stop_stack
    WARMUP_SECONDS=$saved_warmup
    MEASURE_SECONDS=$saved_seconds
}

require_command cargo
require_command curl
require_command python3
require_command "${CC:-cc}"
require_command awk
RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/talon-c-bench.XXXXXX")
mkdir -p "$RUN_DIR/logs" "$(dirname "$OUTPUT")"
: >"$OUTPUT"
build_tools
start_origin
git_dirty=false
if [[ -n "$(git -C "$ROOT" status --porcelain --untracked-files=normal)" ]]; then
    git_dirty=true
fi
reported_warmup=$WARMUP_SECONDS
reported_measure=$MEASURE_SECONDS
reported_repeats=$REPEATS
if [[ "$MODE" == smoke ]]; then
    reported_warmup=1
    reported_measure=2
    reported_repeats=1
elif [[ "$MODE" == membership ]]; then
    reported_measure=$MEMBERSHIP_SECONDS
    reported_repeats=1
fi
printf '{"type":"environment","mode":"%s","git_commit":"%s","git_dirty":%s,"block_size":%s,"object_size":67108864,"l1_capacity_bytes":0,"l2_page_size_bytes":%s,"l2_capacity_bytes":%s,"worker_rust_log":"warn","warmup_seconds":%s,"measure_seconds":%s,"repeats":%s,"output":"%s"}\n' \
    "$MODE" "$(git -C "$ROOT" rev-parse HEAD)" "$git_dirty" "$BLOCK_SIZE" \
    "$L2_PAGE_SIZE" "$PAGED_CAPACITY" "$reported_warmup" "$reported_measure" \
    "$reported_repeats" "$OUTPUT" >>"$OUTPUT"

case "$MODE" in
    smoke) run_smoke ;;
    hot) run_hot ;;
    latency) run_latency ;;
    backend) run_backend ;;
    failure) run_failure ;;
    membership) run_membership ;;
    all)
        run_hot
        run_latency
        run_backend
        run_failure
        run_membership
        ;;
esac

python3 "$ROOT/scripts/summarize_c_client_loadtest.py" "$OUTPUT" \
    | tee "$RUN_DIR/summary.jsonl"
echo "raw JSONL: $OUTPUT" >&2
echo "summary JSONL: $RUN_DIR/summary.jsonl" >&2
