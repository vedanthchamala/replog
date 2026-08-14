#!/usr/bin/env bash
# Stage 5 run matrix: seeded random fault schedules (kill -9 + control-plane
# partitions) against a real 3-broker cluster, checker-verified offline.
#
# Usage:            bench/run_torture.sh [seed...]      (default seeds: 1 2 3)
# Env:  DURATION_SECS=120   per-seed duration
#       SOAK_SECS=14400     additionally run one long soak (seed 42, paced
#                           down so hours of history fit in memory)
#
# Exit is nonzero if ANY run reports a contract violation. Histories and
# fault schedules land in target/torture/<name>/ for offline re-checking via
# `replog_torture --verify <dir>`.
set -euo pipefail
cd "$(dirname "$0")/.."

CARGO="${CARGO:-$HOME/.cargo/bin/cargo}"
"$CARGO" build --release --bin replog_broker --bin replog_controller --bin replog_torture
TORTURE=target/release/replog_torture
DURATION="${DURATION_SECS:-120}"

SEEDS=("$@")
[ "${#SEEDS[@]}" -gt 0 ] || SEEDS=(1 2 3)

fail=0
for seed in "${SEEDS[@]}"; do
  echo "== torture seed=$seed duration=${DURATION}s ==" >&2
  "$TORTURE" --seed "$seed" --duration-secs "$DURATION" \
    --out-dir "target/torture/seed-$seed" || fail=1
done

if [ -n "${SOAK_SECS:-}" ]; then
  echo "== soak seed=42 duration=${SOAK_SECS}s ==" >&2
  "$TORTURE" --seed 42 --duration-secs "$SOAK_SECS" --pace-ms 50 \
    --out-dir target/torture/soak || fail=1
fi

exit $fail
