#!/usr/bin/env bash
# Sentinel-managed master + 2 replicas + 3 sentinels in ONE container, so everything can announce 127.0.0.1 and
# the host (Docker Desktop on macOS included) can reach whatever Sentinel reports.
# Master 6400, replicas 6401-6402, sentinels 26379-26381 monitoring service "mymaster" (quorum 2).
# Image overridable with RNC_REDIS_IMAGE. Uses `redis-server <conf> --sentinel`, not a redis-sentinel binary,
# so it works on redis:6.2, 7.x, 8 and valkey/valkey:8.1 alike.
set -euo pipefail
IMAGE="${RNC_REDIS_IMAGE:-redis:7.4}"
CONTAINER=redis-near-cache-sentinel
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" -p 6400-6402:6400-6402 -p 26379-26381:26379-26381 "$IMAGE" bash -c '
  mkdir -p /sentinel
  # Data servers get a (writable, initially empty) config file too, so the CONFIG REWRITE that Sentinel sends
  # while reconfiguring a node succeeds instead of erroring inside its MULTI.
  for p in 6400 6401 6402; do
    : > /sentinel/redis-$p.conf
    args="--port $p --dir /sentinel --dbfilename dump-$p.rdb --save \"\" --appendonly no --replica-announce-ip 127.0.0.1 --replica-announce-port $p --logfile /sentinel/redis-$p.log --daemonize yes"
    if [ "$p" != 6400 ]; then args="$args --replicaof 127.0.0.1 6400"; fi
    # Recorded so a test that kills a server can restart it with exactly the same arguments.
    echo "$args" > /sentinel/redis-$p.args
    eval redis-server /sentinel/redis-$p.conf $args
  done
  for p in 26379 26380 26381; do
    cat > /sentinel/sentinel-$p.conf <<EOF
port $p
daemonize yes
dir /sentinel
logfile /sentinel/sentinel-$p.log
sentinel announce-ip 127.0.0.1
sentinel announce-port $p
sentinel monitor mymaster 127.0.0.1 6400 2
sentinel down-after-milliseconds mymaster 2000
sentinel failover-timeout mymaster 5000
sentinel parallel-syncs mymaster 2
EOF
    redis-server /sentinel/sentinel-$p.conf --sentinel
  done
  tail -f /dev/null' >/dev/null

ready=0
for i in $(seq 1 60); do
  ok=1
  for p in 26379 26380 26381; do
    docker exec "$CONTAINER" redis-cli -p $p SENTINEL get-master-addr-by-name mymaster 2>/dev/null | grep -q 6400 || ok=0
    # The sentinels must also have discovered each other, or a failover cannot reach quorum/majority.
    docker exec "$CONTAINER" redis-cli -p $p SENTINEL ckquorum mymaster 2>/dev/null | grep -q "OK 3 usable" || ok=0
  done
  for p in 6401 6402; do
    docker exec "$CONTAINER" redis-cli -p $p INFO replication 2>/dev/null | grep -q master_link_status:up || ok=0
  done
  if [ $ok = 1 ]; then ready=1; break; fi
  sleep 1
done
if [ $ready != 1 ]; then
  echo "sentinel topology did not come up" >&2
  docker exec "$CONTAINER" bash -c 'tail -n 20 /sentinel/*.log' >&2 || true
  exit 1
fi

echo "server:   $(docker exec "$CONTAINER" redis-cli -p 6400 INFO server | grep -E '^redis_version|^valkey_version' | tr -d '\r' | tr '\n' ' ')"
for p in 26379 26380 26381; do
  echo "sentinel $p: master=$(docker exec "$CONTAINER" redis-cli -p $p SENTINEL get-master-addr-by-name mymaster | tr '\n' ':' | sed 's/:$//')"
done
for p in 6400 6401 6402; do
  echo "node $p: $(docker exec "$CONTAINER" redis-cli -p $p INFO replication | grep -E '^role|^connected_slaves|^master_link_status' | tr -d '\r' | tr '\n' ' ')"
done
