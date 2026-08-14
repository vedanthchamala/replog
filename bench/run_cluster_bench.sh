#!/usr/bin/env bash
# Stage 4 headline measurements: (1) what replication costs at each ack level
# on a 3-broker localhost cluster (vs an rf=1 topic on the same cluster), and
# (2) the failover-time distribution over repeated leader kills.
# Fresh cluster per row. Usage: bench/run_cluster_bench.sh [produce.csv] [gaps.csv]
set -euo pipefail
cd "$(dirname "$0")/.."

CARGO="${CARGO:-$HOME/.cargo/bin/cargo}"
OUT="${1:-bench/results/cluster_produce.csv}"
FOUT="${2:-bench/results/failover_gaps.csv}"
DATA_ROOT="target/bench-data-cluster"
FSYNC="batch:1048576:5"

"$CARGO" build --release --bin replog_broker --bin replog_controller \
  --bin broker_bench --bin failover_bench
BROKER=target/release/replog_broker
CONTROLLER=target/release/replog_controller
BENCH=target/release/broker_bench
FBENCH=target/release/failover_bench

mkdir -p "$(dirname "$OUT")"
rm -rf "$DATA_ROOT"
"$BENCH" --header > "$OUT"

PIDS=()
cleanup() { [ "${#PIDS[@]}" -gt 0 ] && kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT

wait_addr() { # logfile -> prints "host:port"
  local addr=""
  for _ in $(seq 1 200); do
    addr=$(sed -n 's/.*listening on //p' "$1" 2>/dev/null | head -1 || true)
    [ -n "$addr" ] && break
    sleep 0.05
  done
  [ -n "$addr" ] || { echo "process did not start ($1)" >&2; exit 1; }
  echo "$addr"
}

start_cluster() { # min_isr; sets BOOTSTRAP
  local min_isr="$1" dir="$DATA_ROOT/run"
  mkdir -p "$dir"
  "$CONTROLLER" --listen 127.0.0.1:0 --state-file "$dir/controller.state" \
    --session-timeout-ms 3000 > "$dir/controller.stdout" &
  PIDS+=($!)
  local caddr; caddr=$(wait_addr "$dir/controller.stdout")
  BOOTSTRAP=""
  for id in 0 1 2; do
    "$BROKER" --listen 127.0.0.1:0 --data-dir "$dir/broker-$id" --fsync "$FSYNC" \
      --broker-id "$id" --controller-addr "$caddr" --min-isr "$min_isr" \
      > "$dir/broker-$id.stdout" 2> "$dir/broker-$id.stderr" &
    PIDS+=($!)
    local baddr; baddr=$(wait_addr "$dir/broker-$id.stdout")
    [ -n "$BOOTSTRAP" ] || BOOTSTRAP="$baddr"
  done
}

stop_cluster() {
  [ "${#PIDS[@]}" -gt 0 ] && kill "${PIDS[@]}" 2>/dev/null || true
  [ "${#PIDS[@]}" -gt 0 ] && wait "${PIDS[@]}" 2>/dev/null || true
  PIDS=()
  rm -rf "$DATA_ROOT/run"
}

run() { # acks batch inflight records rf [min_isr]
  local min_isr="${6:-2}"
  start_cluster "$min_isr"
  echo ">> acks=$1 rf=$5 batch=$2 inflight=$3 records=$4 min_isr=$min_isr" >&2
  "$BENCH" --addr "$BOOTSTRAP" --acks "$1" --batch-records "$2" --inflight "$3" \
    --records "$4" --value-bytes 100 --partitions 1 --rf "$5" >> "$OUT"
  stop_cluster
}

# The replication-overhead curve: acks level x batch size, RF=3, synchronous.
run none    100  1 500000  3
run none    1000 1 1000000 3
run written 1    1 20000   3
run written 100  1 500000  3
run written 1000 1 1000000 3
run durable 1    1 1000    3
run durable 100  1 100000  3
run durable 1000 1 500000  3
run all     1    1 3000    3
run all     100  1 100000  3
run all     1000 1 500000  3

# RF=1 baseline on the SAME cluster binaries (isolates replication itself
# from cluster-mode bookkeeping; min_isr=1 so acks=all is legal with one
# replica — it acks as soon as the leader has written).
run written 100 1 500000 1 1
run durable 100 1 100000 1 1
run all     100 1 500000 1 1

# Pipelining hides replication latency: acks=all, batch=100, deeper inflight.
run all 100 2 200000 3
run all 100 4 300000 3
run all 100 8 500000 3

# Failover-time distribution over repeated real kills (self-orchestrating).
"$FBENCH" --header > "$FOUT"
"$FBENCH" --kills 12 --session-timeout-ms 700 --data-root "$DATA_ROOT/failover" >> "$FOUT"

rm -rf "$DATA_ROOT"
echo "wrote $OUT and $FOUT" >&2
column -s, -t "$OUT"
column -s, -t "$FOUT"
