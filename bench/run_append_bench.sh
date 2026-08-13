#!/usr/bin/env bash
# Stage 1 headline measurement: what an fsync costs on this SSD, per policy.
# Usage: bench/run_append_bench.sh [out.csv]
set -euo pipefail
cd "$(dirname "$0")/.."

CARGO="${CARGO:-$HOME/.cargo/bin/cargo}"
OUT="${1:-bench/results/append_fsync.csv}"
DATA="target/bench-data"

"$CARGO" build --release --bin append_bench
BIN=target/release/append_bench

mkdir -p "$(dirname "$OUT")"
rm -rf "$DATA"

"$BIN" --header > "$OUT"

run() { # policy records value_bytes
  local dir="$DATA/$1-$3"
  mkdir -p "$dir"
  echo ">> policy=$1 records=$2 value_bytes=$3" >&2
  "$BIN" --dir "$dir" --policy "$1" --records "$2" --value-bytes "$3" >> "$OUT"
}

for size in 100 1024; do
  run always            1000   "$size"
  run batch:1048576:50  200000 "$size"
  run os                200000 "$size"
done

rm -rf "$DATA"
echo "wrote $OUT" >&2
column -s, -t "$OUT"
