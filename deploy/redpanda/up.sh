#!/usr/bin/env bash
# Bring up the 3-broker Redpanda cluster used as a fault-injection target and
# pin the cluster properties the comparison depends on:
#   write_caching_default=false      acks=all means fsynced on a majority
#   raft_heartbeat_timeout_ms=DETECT_MS   matched failure-detection timeout
# Env: DETECT_MS=1000  WRITE_CACHING=false
set -euo pipefail
cd "$(dirname "$0")"
DETECT_MS="${DETECT_MS:-1000}"
WRITE_CACHING="${WRITE_CACHING:-false}"
docker compose up -d
for i in $(seq 1 60); do
  if curl -fs localhost:29640/v1/cluster/health_overview 2>/dev/null | grep -q '"is_healthy": true' \
     && [ "$(curl -fs localhost:29640/v1/brokers | grep -o '"node_id"' | wc -l | tr -d ' ')" = "3" ]; then
    break
  fi
  sleep 1
done
docker exec rp-0 rpk cluster config set write_caching_default "$WRITE_CACHING" >/dev/null
docker exec rp-0 rpk cluster config set raft_heartbeat_timeout_ms "$DETECT_MS" >/dev/null
# Leadership must change only because of faults: no background rebalancing.
docker exec rp-0 rpk cluster config set enable_leader_balancer false >/dev/null
echo "write_caching_default=$(docker exec rp-0 rpk cluster config get write_caching_default)"
echo "raft_heartbeat_timeout_ms=$(docker exec rp-0 rpk cluster config get raft_heartbeat_timeout_ms)"
echo "enable_leader_balancer=$(docker exec rp-0 rpk cluster config get enable_leader_balancer)"
docker exec rp-0 rpk cluster config status
curl -s localhost:29640/v1/cluster/health_overview; echo
