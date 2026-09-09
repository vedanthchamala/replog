#!/usr/bin/env bash
# Stage 6 matrix: the same seeded fault schedules against every target.
#
# Usage:  bench/run_faults.sh <target> [seed...]     target = redpanda | replog
#         (default seeds: 1 2 3)
# Env:    DURATION_SECS=120   per-seed duration
#         DETECT_MS=1000      failure-detection timeout the cluster was brought
#                             up with (deploy/<target>/up.sh DETECT_MS=...)
#         SKIP_PROBE=1        skip the fault-backend probe
#         SKIP_CONTROLS=1     skip the acks=1 / acks=all sensitivity pair
#
# Order matters and is deliberate:
#   1. probe      — every fault provably does what it claims on this target
#   2. baseline   — 30 s, no faults: the checker is clean on a quiet cluster
#   3. controls   — acks=1 vs acks=all under isolate-only schedules: the
#                   checker must be able to fail (acks=1 loses acked ids on
#                   a zombie leader) before its "zero violations" means anything
#   4. seeds      — the contract runs, kill/pause/isolate mixed, acks=all
#
# Results land in bench/results/faults/<target>/<name>/ (history, schedule,
# faults.csv, timeline.csv, leaders.log, summary.txt); exit is nonzero if a
# contract run reports a violation or a control does not behave as expected.
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="${1:?target required: redpanda | replog | kafka}"; shift || true
SEEDS=("$@"); [ "${#SEEDS[@]}" -gt 0 ] || SEEDS=(1 2 3)
DURATION="${DURATION_SECS:-120}"
DETECT="${DETECT_MS:-1000}"
CARGO="${CARGO:-$HOME/.cargo/bin/cargo}"
OUT="bench/results/faults/$TARGET"
mkdir -p "$OUT"

"$CARGO" build --release --features kafka --bin replog_faults 2>&1 | tail -1
BIN=target/release/replog_faults
fail=0

if [ -z "${SKIP_PROBE:-}" ]; then
  echo "== probe target=$TARGET ==" >&2
  "$BIN" probe --target "$TARGET" --detect-ms "$DETECT" | tee "$OUT/probe.txt" || fail=1
fi

echo "== baseline 30 s, no faults ==" >&2
"$BIN" run --target "$TARGET" --faults none --duration-secs 30 --seed 100 \
  --detect-ms "$DETECT" --out-dir "$OUT/baseline" || fail=1

if [ -z "${SKIP_CONTROLS:-}" ]; then
  # Same seed, same isolate-only schedule; only the ack level differs.
  echo "== control: acks=1 under isolate (expected: acked ids LOST) ==" >&2
  if "$BIN" run --target "$TARGET" --faults isolate --acks 1 --duration-secs 60 --seed 7 \
      --detect-ms "$DETECT" --out-dir "$OUT/control-acks1"; then
    echo "control-acks1: checker found NO loss — the harness cannot distinguish acks=1 from acks=all here" >&2
    echo "control-acks1: NO LOSS (unexpected)" > "$OUT/control-acks1/VERDICT"
  else
    echo "control-acks1: LOSS DETECTED (expected)" > "$OUT/control-acks1/VERDICT"
  fi
  echo "== control: acks=all under the same isolate schedule (expected: clean) ==" >&2
  "$BIN" run --target "$TARGET" --faults isolate --acks all --duration-secs 60 --seed 7 \
      --detect-ms "$DETECT" --out-dir "$OUT/control-acksall" || fail=1
fi

for seed in "${SEEDS[@]}"; do
  echo "== contract run seed=$seed duration=${DURATION}s faults=kill,pause,isolate acks=all ==" >&2
  "$BIN" run --target "$TARGET" --seed "$seed" --duration-secs "$DURATION" \
    --detect-ms "$DETECT" --out-dir "$OUT/seed-$seed" || fail=1
done

exit $fail
