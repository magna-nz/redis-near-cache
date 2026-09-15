#!/usr/bin/env bash
# 3 masters + 3 replicas in ONE container so nodes can announce 127.0.0.1 and the host can follow MOVED.
# Masters are 7100-7102, replicas 7103-7105. Image overridable with RNC_REDIS_IMAGE.
set -euo pipefail
IMAGE="${RNC_REDIS_IMAGE:-redis:7.4}"
docker rm -f redis-near-cache-cluster >/dev/null 2>&1 || true
docker run -d --name redis-near-cache-cluster -p 7100-7105:7100-7105 "$IMAGE" bash -c '
  for p in 7100 7101 7102 7103 7104 7105; do
    # One RDB file name per node: all six share /data, and a replica writes the RDB it receives on a full sync to
    # dbfilename. With the default dump.rdb a node restarted by a test loads whichever node last synced, finds
    # keys for slots it does not own, and marks those slots as importing, which then blocks resharding.
    redis-server --port $p --cluster-enabled yes --cluster-config-file nodes-$p.conf --dbfilename dump-$p.rdb \
      --cluster-announce-ip 127.0.0.1 --cluster-announce-port $p --cluster-announce-bus-port $((p+10000)) \
      --cluster-node-timeout 3000 --save "" --appendonly no --daemonize yes
  done
  sleep 1
  redis-cli --cluster create 127.0.0.1:7100 127.0.0.1:7101 127.0.0.1:7102 127.0.0.1:7103 127.0.0.1:7104 127.0.0.1:7105 --cluster-replicas 1 --cluster-yes
  tail -f /dev/null' >/dev/null
for i in $(seq 1 30); do
  if docker exec redis-near-cache-cluster redis-cli -p 7100 cluster info 2>/dev/null | grep -q cluster_state:ok; then break; fi
  sleep 1
done
docker exec redis-near-cache-cluster redis-cli -p 7100 cluster info | grep -E "cluster_state|cluster_known_nodes"
docker exec redis-near-cache-cluster redis-cli -p 7100 cluster nodes
