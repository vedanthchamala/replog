#!/usr/bin/env bash
# Stage 2 headline measurement: what client-side batching buys over TCP, and
# what the durable-ack contract costs vs written-ack.
# Fresh broker process + data dir per row. Usage: bench/run_broker_bench.sh [out.csv]
set -euo pipefail
cd "$(dirname "$0")/.."

CARGO="${CARGO:-$HOME/.cargo/bin/cargo}"
OUT="${1:-bench/results/broker_produce.csv}"
DATA_ROOT="target/bench-data-broker"
FSYNC="batch:1048576:5"

"$CARGO" build --release --bin replog_broker --bin broker_bench
BROKER=target/release/replog_broker
BENCH=target/release/broker_bench

mkdir -p "$(dirname "$OUT")"
rm -rf "$DATA_ROOT"
"$BENCH" --header > "$OUT"

BROKER_PID=""
cleanup() { [ -n "$BROKER_PID" ] && kill "$BROKER_PID" 2>/dev/null || true; }
trap cleanup EXIT

run() { # acks batch_records inflight records [rate] [partitions] [cpp]
  local rate="${5:-0}"
  local parts="${6:-1}"
  local cpp="${7:-}"
  local dir="$DATA_ROOT/$1-b$2-i$3-r$rate-p$parts-c${cpp:-0}"
  local log="$dir.stdout"
  mkdir -p "$dir"
  "$BROKER" --listen 127.0.0.1:0 --data-dir "$dir" --fsync "$FSYNC" > "$log" &
  BROKER_PID=$!
  local addr=""
  for _ in $(seq 1 100); do
    addr=$(sed -n 's/.*listening on //p' "$log" 2>/dev/null || true)
    [ -n "$addr" ] && break
    sleep 0.05
  done
  [ -n "$addr" ] || { echo "broker did not start" >&2; exit 1; }
  echo ">> acks=$1 batch=$2 inflight=$3 records=$4 rate=$rate partitions=$parts cpp=${cpp:-no}" >&2
  "$BENCH" --addr "$addr" --acks "$1" --batch-records "$2" --inflight "$3" \
    --records "$4" --value-bytes 100 --rate "$rate" --partitions "$parts" \
    ${cpp:+--conn-per-partition} >> "$OUT"
  kill "$BROKER_PID" 2>/dev/null || true
  wait "$BROKER_PID" 2>/dev/null || true
  BROKER_PID=""
}

# Batch-size curve, synchronous (inflight=1)
run written 1    1 20000
run written 10   1 100000
run written 100  1 500000
run written 1000 1 1000000
run durable 1    1 1000
run durable 10   1 10000
run durable 100  1 100000
run durable 1000 1 500000

# Pipelining sweep at batch=100, acks=written
run written 100 2  500000
run written 100 4  500000
run written 100 8  500000
run written 100 16 500000

# Open-loop: latency percentiles at fixed arrival rate (below saturation)
run written 100  1 500000 100000
run written 100  1 500000 400000
run durable 1000 1 200000 20000
run durable 1000 1 200000 40000

# Partition scaling (Stage 3): round-robin over N writer threads.
# Shared connection first, then one connection per partition (inflight scaled
# with partition count so pipelining depth per connection stays 8).
run written 100  8 1000000 0 1
run written 100  8 1000000 0 2
run written 100  8 1000000 0 4
run written 100  8 1000000 0 8
run durable 1000 8 500000  0 1
run durable 1000 8 500000  0 2
run durable 1000 8 500000  0 4
run durable 1000 8 500000  0 8
run written 100  16 1000000 0 2 1
run written 100  32 1000000 0 4 1
run written 100  64 2000000 0 8 1
run durable 1000 16 500000  0 2 1
run durable 1000 32 500000  0 4 1
run durable 1000 64 1000000 0 8 1

rm -rf "$DATA_ROOT"
echo "wrote $OUT" >&2
column -s, -t "$OUT"
