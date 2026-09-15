#!/usr/bin/env bash
# Managed-style EMULATION of two constraints that could plausibly break RedisNearCache on a managed Redis
# service (AWS ElastiCache, Azure Managed Redis) - neither of which can be run in Docker, so this reproduces
# the two properties of them that matter to this library instead of the services themselves:
#   1. redis-near-cache-restricted (localhost:6410): a standalone server with the admin commands ElastiCache
#      disables renamed away, via a config file written inside the container and passed to redis-server.
#   2. redis-near-cache-cluster-hostname (localhost:7200-7205): a 3-master/3-replica cluster, in one
#      container exactly like cluster-up.sh, but announcing a DNS hostname instead of an IP - the way
#      ElastiCache/Azure OSS-cluster endpoints hand out hostnames in MOVED / CLUSTER SLOTS / CLUSTER SHARDS.
#      Needs Redis 7.0+ or Valkey; skipped on a redis:6.x image.
# This script does NOT stand up, configure, or connect to any real managed service. See
# tests/RedisNearCache.Tests/Managed/ExternalManagedEndpointTests.cs for the opt-in test against one.
# Idempotent (old containers are removed first). Image overridable with RNC_REDIS_IMAGE (default redis:7.4),
# matching the CI matrix: redis:6.2, 7.0, 7.2, 7.4, 8, valkey/valkey:8.1.
set -euo pipefail
IMAGE="${RNC_REDIS_IMAGE:-redis:7.4}"

# --- 1. restricted: ElastiCache-style disabled admin commands ---------------------------------------------
RESTRICTED_CONTAINER=redis-near-cache-restricted
RESTRICTED_PORT=6410

docker rm -f "$RESTRICTED_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$RESTRICTED_CONTAINER" -p "$RESTRICTED_PORT:$RESTRICTED_PORT" "$IMAGE" bash -c '
cat > /tmp/restricted.conf <<EOF
port 6410
save ""
appendonly no
rename-command CONFIG ""
rename-command DEBUG ""
rename-command MONITOR ""
rename-command SAVE ""
rename-command BGSAVE ""
rename-command BGREWRITEAOF ""
rename-command SHUTDOWN ""
rename-command REPLICAOF ""
rename-command SLAVEOF ""
rename-command SYNC ""
rename-command PSYNC ""
rename-command MIGRATE ""
rename-command MODULE ""
EOF
exec redis-server /tmp/restricted.conf
' >/dev/null

for i in $(seq 1 30); do
  if docker exec "$RESTRICTED_CONTAINER" redis-cli -p "$RESTRICTED_PORT" ping 2>/dev/null | grep -q PONG; then break; fi
  sleep 1
done
echo "restricted: $(docker exec "$RESTRICTED_CONTAINER" redis-cli -p "$RESTRICTED_PORT" info server | grep -E '^redis_version|^valkey_version' | tr -d '\r')"

refused="$(docker exec "$RESTRICTED_CONTAINER" redis-cli -p "$RESTRICTED_PORT" CONFIG GET maxmemory 2>&1 || true)"
if echo "$refused" | grep -qi "unknown command"; then
  echo "restricted: CONFIG GET maxmemory refused as expected -> $refused"
else
  echo "restricted: CONFIG GET maxmemory was NOT refused (rename-command did not take): $refused" >&2
  exit 1
fi

# --- 2. cluster-hostname: ElastiCache/Azure OSS-cluster-style DNS endpoints --------------------------------
HOSTNAME_CLUSTER_CONTAINER=redis-near-cache-cluster-hostname

docker rm -f "$HOSTNAME_CLUSTER_CONTAINER" >/dev/null 2>&1 || true

case "$IMAGE" in
  redis:6*)
    echo "cluster-hostname: skipped - $IMAGE is Redis 6.x and --cluster-announce-hostname/--cluster-preferred-endpoint-type need Redis 7.0+ or Valkey"
    ;;
  *)
    docker run -d --name "$HOSTNAME_CLUSTER_CONTAINER" -p 7200-7205:7200-7205 -p 17200-17205:17200-17205 "$IMAGE" bash -c '
      for p in 7200 7201 7202 7203 7204 7205; do
        redis-server --port $p --cluster-enabled yes --cluster-config-file nodes-$p.conf --dbfilename dump-$p.rdb \
          --cluster-announce-ip 127.0.0.1 --cluster-announce-port $p --cluster-announce-bus-port $((p+10000)) \
          --cluster-announce-hostname localhost --cluster-preferred-endpoint-type hostname \
          --cluster-node-timeout 3000 --save "" --appendonly no --daemonize yes
      done
      sleep 1
      redis-cli --cluster create 127.0.0.1:7200 127.0.0.1:7201 127.0.0.1:7202 127.0.0.1:7203 127.0.0.1:7204 127.0.0.1:7205 --cluster-replicas 1 --cluster-yes
      tail -f /dev/null' >/dev/null

    for i in $(seq 1 30); do
      if docker exec "$HOSTNAME_CLUSTER_CONTAINER" redis-cli -p 7200 cluster info 2>/dev/null | grep -q cluster_state:ok; then break; fi
      sleep 1
    done
    docker exec "$HOSTNAME_CLUSTER_CONTAINER" redis-cli -p 7200 cluster info | grep -E "cluster_state|cluster_known_nodes"
    echo "cluster-hostname: CLUSTER SHARDS"
    docker exec "$HOSTNAME_CLUSTER_CONTAINER" redis-cli -p 7200 cluster shards
    echo "cluster-hostname: CLUSTER SLOTS"
    docker exec "$HOSTNAME_CLUSTER_CONTAINER" redis-cli -p 7200 cluster slots
    ;;
esac
