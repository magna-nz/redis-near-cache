#!/usr/bin/env bash
# 3-master Redis cluster in ONE container so nodes can announce 127.0.0.1 and the host can follow MOVED.
set -euo pipefail
docker rm -f redis-near-cache-cluster >/dev/null 2>&1 || true
docker run -d --name redis-near-cache-cluster -p 7100-7102:7100-7102 redis:7.4 bash -c '
  for p in 7100 7101 7102; do
    redis-server --port $p --cluster-enabled yes --cluster-config-file nodes-$p.conf \
      --cluster-announce-ip 127.0.0.1 --cluster-announce-port $p --cluster-announce-bus-port $((p+10000)) \
      --save "" --appendonly no --daemonize yes
  done
  sleep 1
  redis-cli --cluster create 127.0.0.1:7100 127.0.0.1:7101 127.0.0.1:7102 --cluster-replicas 0 --cluster-yes
  tail -f /dev/null' >/dev/null
for i in $(seq 1 20); do
  if docker exec redis-near-cache-cluster redis-cli -p 7100 cluster info 2>/dev/null | grep -q cluster_state:ok; then break; fi
  sleep 1
done
docker exec redis-near-cache-cluster redis-cli -p 7100 cluster info | grep -E "cluster_state|cluster_known_nodes"
docker exec redis-near-cache-cluster redis-cli -p 7100 cluster nodes
