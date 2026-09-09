#!/usr/bin/env bash
# Put a target's containers back to a healthy default: running, unpaused,
# attached to the peers network. For cleaning up after an interrupted run.
# Usage: deploy/heal.sh redpanda|replog|kafka
set -euo pipefail
case "${1:?target}" in
  redpanda) net=replog-rp_peers; cs="rp-0 rp-1 rp-2" ;;
  replog)   net=replog-rl_peers; cs="rl-0 rl-1 rl-2" ;;
  kafka)    net=replog-kf_peers; cs="kf-0 kf-1 kf-2" ;;
  *) echo "unknown target $1" >&2; exit 2 ;;
esac
for c in $cs; do
  if [ "$(docker inspect -f '{{.State.Paused}}' "$c")" = "true" ]; then docker unpause "$c" >/dev/null; echo "unpaused $c"; fi
  if [ "$(docker inspect -f '{{.State.Running}}' "$c")" != "true" ]; then
    # Redpanda refuses to start after crash_loop_limit consecutive unclean exits
    # (default 5) until this marker is removed; the compose raises the limit,
    # but clear it anyway so an old container can come back.
    case "$c" in rp-*) docker run --rm --entrypoint /bin/bash --volumes-from "$c" \
      docker.redpanda.com/redpandadata/redpanda:v26.2.2 -c 'rm -f /var/lib/redpanda/data/startup_log' >/dev/null 2>&1 || true ;; esac
    docker start "$c" >/dev/null; echo "started $c"
  fi
  if ! docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$c" | grep -q "$net"; then
    docker network connect "$net" "$c"; echo "reconnected $c to $net"
  fi
done
echo "healed $1"
