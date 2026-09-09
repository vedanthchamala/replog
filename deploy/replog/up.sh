#!/usr/bin/env bash
# Build the replog image and bring up 1 controller + 3 brokers with the same
# network layout as the Redpanda target (shared peers network, one private
# edge network per broker publishing its client port to the host).
# Env: DETECT_MS=1000 (controller session timeout)  LAG_MS=1500 (replica lag)
#      NO_BUILD=1 to skip the image build
set -euo pipefail
cd "$(dirname "$0")/../.."
if [ -z "${NO_BUILD:-}" ]; then
  docker build -q -f deploy/replog/Dockerfile -t replog:latest . >/dev/null
fi
cd deploy/replog
DETECT_MS="${DETECT_MS:-1000}" LAG_MS="${LAG_MS:-1500}" docker compose up -d
echo "replog cluster up: controller localhost:29099, brokers localhost:29000-29002 (detect ${DETECT_MS:-1000} ms)"
