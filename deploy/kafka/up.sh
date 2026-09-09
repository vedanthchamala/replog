#!/usr/bin/env bash
# 3-node Apache Kafka (KRaft, combined broker+controller) with the same
# network layout as the other targets. Env: DETECT_MS=1000 LAG_MS=1500
set -euo pipefail
cd "$(dirname "$0")"
DETECT_MS="${DETECT_MS:-1000}" LAG_MS="${LAG_MS:-1500}" docker compose up -d
for i in $(seq 1 90); do
  if docker exec kf-0 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 \
     && [ "$(docker exec kf-0 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>/dev/null | grep -c 'id: ')" = "3" ]; then
    break
  fi
  sleep 2
done
docker exec kf-0 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>/dev/null | grep 'id: '
echo "kafka cluster up: brokers localhost:39090-39092 (detect ${DETECT_MS:-1000} ms, lag ${LAG_MS:-1500} ms)"
