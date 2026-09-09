#!/usr/bin/env bash
# Tear the Redpanda target down. Pass --wipe to also drop the data volumes.
set -euo pipefail
cd "$(dirname "$0")"
if [ "${1:-}" = "--wipe" ]; then docker compose down -v; else docker compose down; fi
