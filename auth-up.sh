#!/usr/bin/env bash
# Four containers for authentication / ACL / mTLS integration testing:
#   redis-near-cache-auth           standalone, password (default user) + the three ACL users   :6450
#   redis-near-cache-mtls           standalone, TLS-only, client certificate required, no password   :6460
#   redis-near-cache-auth-cluster   3 masters + 3 replicas, one container, password + ACL users   :7300-7305
#   redis-near-cache-auth-sentinel  master+2 replicas (password + ACL users) and 3 INDEPENDENT single-sentinel
#                                   groups with different auth, one container   :6470-6472 / :26390-26392
# Idempotent (old containers removed first). Image overridable with RNC_REDIS_IMAGE (default redis:7.4),
# matching the CI matrix: redis:6.2, 7.0, 7.2, 7.4, 8, valkey/valkey:8.1.
# The mTLS container needs certs/client.{crt,key} (run ./certs/gen.sh first; up.sh does this automatically).
set -euo pipefail
cd "$(dirname "$0")"
IMAGE="${RNC_REDIS_IMAGE:-redis:7.4}"
DEFAULT_PW=rnc-default-pw

if [[ ! -f certs/client.crt || ! -f certs/client.key || ! -f certs/ca.crt ]]; then
  echo "certs/client.crt, certs/client.key or certs/ca.crt is missing; run ./certs/gen.sh first" >&2
  exit 1
fi

# --- ACL users (begin) ---
# Single source of truth for the three ACL users shared by every auth-* data node (standalone, cluster,
# sentinel data nodes). Written verbatim as `user <line>` directives into each node's redis.conf. It reaches the
# containers as an environment variable, not by ${script//placeholder/$block}: from bash 5.2 (ubuntu-latest) an
# unquoted & in that replacement means "the matched text", which turns &__redis__:invalidate into the
# placeholder's name, and quoting it instead breaks bash 3.2 (macOS), which keeps the quotes.
#
# The two rnc-minimal-* lines are the least privilege each tracking mode was MEASURED to need: every grant below
# was proven by removing it and watching either the cache break or `ACL LOG` record a denial (see the
# RedisNearCache.Tests.Auth suite, which asserts `ACL LOG` is empty on every node at the end of a run).
#   RedisNearCache needs: client|tracking, client|trackinginfo, client|list (Redirect: find our own subscriber),
#     client|caching + multi/exec (Redirect: reads outside KeyPrefixes), client|id (Broadcast socket), subscribe
#     + &__redis__:invalidate (Redirect), role (Redirect: replica pre-arm), cluster|nodes (reconcile),
#     ping (Broadcast: the 10 s keepalive; a denied PING reads as a dead socket and costs a re-arm + flush),
#     get, pttl, set, setex / psetex (SetAsync with an expiry: StackExchange.Redis sends SETEX for a whole number
#     of seconds and PSETEX otherwise), unlink (RemoveAsync).
#   StackExchange.Redis itself needs: info, echo, config|get, select, client|setname, cluster|slots, readonly
#     (cluster replica connections), ~__Booksleeve_TieBreak (its tie-breaker key), &__Booksleeve_MasterChanged
#     (its configuration channel), and client|setinfo - which is NOT in the lines below: the command exists from
#     Redis 7.2, and a Redis 7.0/7.1 server refuses to START on a rule naming a subcommand it does not know (6.2
#     accepts it). grant_optional_acl adds it once a node is up, ignoring the refusal; where it is refused the
#     client never sends the command either.
# `resetchannels` is explicit because Redis 6.2 still defaults to allchannels. Neither user is granted +auth or
# +hello: those are no-auth commands and run without any permission.
ACL_USERS_CONF=$(cat <<'ACLEOF'
user rnc-full on >rnc-full-pw resetchannels ~* &* +@all
user rnc-minimal-redirect on >rnc-minimal-pw resetchannels ~t:* ~__Booksleeve_TieBreak &__redis__:invalidate &__Booksleeve_MasterChanged +select +client|id +client|list +client|tracking +client|trackinginfo +client|setname +client|caching +role +cluster|nodes +cluster|slots +readonly +subscribe +ping +echo +info +config|get +get +pttl +set +setex +psetex +unlink +multi +exec
user rnc-minimal-bcast on >rnc-minimal-pw resetchannels ~t:* ~bc:* ~__Booksleeve_TieBreak &__Booksleeve_MasterChanged +ping +echo +info +config|get +cluster|nodes +cluster|slots +readonly +subscribe +select +client|id +client|tracking +client|trackinginfo +client|setname +get +pttl +set +setex +psetex +unlink
ACLEOF
)
grant_optional_acl() { # container port...
  local container="$1" p u; shift
  for p in "$@"; do
    for u in rnc-minimal-redirect rnc-minimal-bcast; do
      docker exec "$container" redis-cli -p "$p" -a "$DEFAULT_PW" --no-auth-warning ACL SETUSER "$u" '+client|setinfo' >/dev/null 2>&1 || true
    done
  done
}
# --- ACL users (end) ---

# --- (a) redis-near-cache-auth: standalone, password + ACL users ------------------------------------------
AUTH_CONTAINER=redis-near-cache-auth
AUTH_PORT=6450
docker rm -f "$AUTH_CONTAINER" >/dev/null 2>&1 || true
AUTH_SCRIPT=$(cat <<'EOF'
printf '%s\n' "$ACL_USERS_CONF" > auth.conf
exec redis-server auth.conf --port 6450 --requirepass rnc-default-pw --save "" --appendonly no
EOF
)
docker run -d --name "$AUTH_CONTAINER" -e ACL_USERS_CONF="$ACL_USERS_CONF" -p "$AUTH_PORT:$AUTH_PORT" "$IMAGE" bash -c "$AUTH_SCRIPT" >/dev/null

for i in $(seq 1 30); do
  docker exec "$AUTH_CONTAINER" redis-cli -p "$AUTH_PORT" -a "$DEFAULT_PW" --no-auth-warning ping 2>/dev/null | grep -q PONG && break
  sleep 1
done
if ! docker exec "$AUTH_CONTAINER" redis-cli -p "$AUTH_PORT" -a "$DEFAULT_PW" --no-auth-warning ping 2>/dev/null | grep -q PONG; then
  echo "$AUTH_CONTAINER did not come up" >&2
  docker logs --tail 50 "$AUTH_CONTAINER" >&2 || true
  exit 1
fi
grant_optional_acl "$AUTH_CONTAINER" "$AUTH_PORT"

# --- (b) redis-near-cache-mtls: standalone, TLS-only, client certificate required, no password ------------
MTLS_CONTAINER=redis-near-cache-mtls
MTLS_PORT=6460
docker rm -f "$MTLS_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$MTLS_CONTAINER" -p "$MTLS_PORT:$MTLS_PORT" -v "$(pwd)/certs:/certs:ro" "$IMAGE" \
  redis-server --port 0 --tls-port "$MTLS_PORT" --tls-cert-file /certs/redis.crt --tls-key-file /certs/redis.key \
  --tls-ca-cert-file /certs/ca.crt --tls-auth-clients yes --save "" --appendonly no >/dev/null

for i in $(seq 1 30); do
  docker exec "$MTLS_CONTAINER" redis-cli --tls --cacert /certs/ca.crt --cert /certs/client.crt --key /certs/client.key -p "$MTLS_PORT" ping 2>/dev/null | grep -q PONG && break
  sleep 1
done
if ! docker exec "$MTLS_CONTAINER" redis-cli --tls --cacert /certs/ca.crt --cert /certs/client.crt --key /certs/client.key -p "$MTLS_PORT" ping 2>/dev/null | grep -q PONG; then
  echo "$MTLS_CONTAINER did not come up" >&2
  docker logs --tail 50 "$MTLS_CONTAINER" >&2 || true
  exit 1
fi

# --- (c) redis-near-cache-auth-cluster: 3 masters + 3 replicas, password + ACL users -----------------------
CLUSTER_CONTAINER=redis-near-cache-auth-cluster
docker rm -f "$CLUSTER_CONTAINER" >/dev/null 2>&1 || true
CLUSTER_SCRIPT=$(cat <<'EOF'
printf '%s\n' "$ACL_USERS_CONF" > acl.conf
for p in 7300 7301 7302 7303 7304 7305; do
  redis-server acl.conf --port $p --cluster-enabled yes --cluster-config-file nodes-$p.conf --dbfilename dump-$p.rdb \
    --cluster-announce-ip 127.0.0.1 --cluster-announce-port $p --cluster-announce-bus-port $((p+10000)) \
    --cluster-node-timeout 3000 --save "" --appendonly no \
    --requirepass rnc-default-pw --masterauth rnc-default-pw --daemonize yes
done
sleep 1
redis-cli -a rnc-default-pw --no-auth-warning --cluster create 127.0.0.1:7300 127.0.0.1:7301 127.0.0.1:7302 127.0.0.1:7303 127.0.0.1:7304 127.0.0.1:7305 --cluster-replicas 1 --cluster-yes
tail -f /dev/null
EOF
)
docker run -d --name "$CLUSTER_CONTAINER" -e ACL_USERS_CONF="$ACL_USERS_CONF" -p 7300-7305:7300-7305 "$IMAGE" bash -c "$CLUSTER_SCRIPT" >/dev/null

cluster_ready=0
for i in $(seq 1 30); do
  if docker exec "$CLUSTER_CONTAINER" redis-cli -a "$DEFAULT_PW" --no-auth-warning -p 7300 cluster info 2>/dev/null | grep -q cluster_state:ok; then
    cluster_ready=1
    break
  fi
  sleep 1
done
if [ $cluster_ready != 1 ]; then
  echo "$CLUSTER_CONTAINER cluster did not come up" >&2
  docker logs --tail 80 "$CLUSTER_CONTAINER" >&2 || true
  exit 1
fi
grant_optional_acl "$CLUSTER_CONTAINER" 7300 7301 7302 7303 7304 7305

# --- (d) redis-near-cache-auth-sentinel: master+2 replicas (password + ACL users), 3 independent sentinels -
SENTINEL_CONTAINER=redis-near-cache-auth-sentinel
docker rm -f "$SENTINEL_CONTAINER" >/dev/null 2>&1 || true
SENTINEL_SCRIPT=$(cat <<'EOF'
mkdir -p /sentinel
for p in 6470 6471 6472; do
  printf '%s\n' "$ACL_USERS_CONF" > /sentinel/redis-$p.conf
  args="--port $p --dir /sentinel --dbfilename dump-$p.rdb --save \"\" --appendonly no --replica-announce-ip 127.0.0.1 --replica-announce-port $p --logfile /sentinel/redis-$p.log --requirepass rnc-default-pw --masterauth rnc-default-pw --daemonize yes"
  if [ "$p" != 6470 ]; then args="$args --replicaof 127.0.0.1 6470"; fi
  echo "$args" > /sentinel/redis-$p.args
  eval redis-server /sentinel/redis-$p.conf $args
done

cat > /sentinel/sentinel-26390.conf <<CONF
port 26390
daemonize yes
dir /sentinel
logfile /sentinel/sentinel-26390.log
requirepass rnc-default-pw
sentinel announce-ip 127.0.0.1
sentinel announce-port 26390
sentinel monitor authsame 127.0.0.1 6470 1
sentinel auth-pass authsame rnc-default-pw
sentinel down-after-milliseconds authsame 60000
sentinel failover-timeout authsame 5000
sentinel parallel-syncs authsame 2
CONF
redis-server /sentinel/sentinel-26390.conf --sentinel

cat > /sentinel/sentinel-26391.conf <<CONF
port 26391
daemonize yes
dir /sentinel
logfile /sentinel/sentinel-26391.log
sentinel announce-ip 127.0.0.1
sentinel announce-port 26391
sentinel monitor authopen 127.0.0.1 6470 1
sentinel auth-pass authopen rnc-default-pw
sentinel down-after-milliseconds authopen 60000
sentinel failover-timeout authopen 5000
sentinel parallel-syncs authopen 2
CONF
redis-server /sentinel/sentinel-26391.conf --sentinel

cat > /sentinel/sentinel-26392.conf <<CONF
port 26392
daemonize yes
dir /sentinel
logfile /sentinel/sentinel-26392.log
requirepass rnc-sentinel-pw
sentinel announce-ip 127.0.0.1
sentinel announce-port 26392
sentinel monitor authother 127.0.0.1 6470 1
sentinel auth-pass authother rnc-default-pw
sentinel down-after-milliseconds authother 60000
sentinel failover-timeout authother 5000
sentinel parallel-syncs authother 2
CONF
redis-server /sentinel/sentinel-26392.conf --sentinel

tail -f /dev/null
EOF
)
docker run -d --name "$SENTINEL_CONTAINER" -e ACL_USERS_CONF="$ACL_USERS_CONF" -p 6470-6472:6470-6472 -p 26390-26392:26390-26392 "$IMAGE" bash -c "$SENTINEL_SCRIPT" >/dev/null

ready=0
for i in $(seq 1 60); do
  ok=1
  docker exec "$SENTINEL_CONTAINER" redis-cli -p 26390 -a "$DEFAULT_PW" --no-auth-warning SENTINEL get-master-addr-by-name authsame 2>/dev/null | grep -q 6470 || ok=0
  docker exec "$SENTINEL_CONTAINER" redis-cli -p 26391 SENTINEL get-master-addr-by-name authopen 2>/dev/null | grep -q 6470 || ok=0
  docker exec "$SENTINEL_CONTAINER" redis-cli -p 26392 -a rnc-sentinel-pw --no-auth-warning SENTINEL get-master-addr-by-name authother 2>/dev/null | grep -q 6470 || ok=0
  [ "$(docker exec "$SENTINEL_CONTAINER" redis-cli -p 26390 -a "$DEFAULT_PW" --no-auth-warning SENTINEL master authsame 2>/dev/null | grep -A1 -x num-slaves | tail -1 | tr -d '\r')" = "2" ] || ok=0
  for p in 6471 6472; do
    docker exec "$SENTINEL_CONTAINER" redis-cli -p $p -a "$DEFAULT_PW" --no-auth-warning INFO replication 2>/dev/null | grep -q master_link_status:up || ok=0
  done
  if [ $ok = 1 ]; then ready=1; break; fi
  sleep 1
done
if [ $ready != 1 ]; then
  echo "$SENTINEL_CONTAINER topology did not come up" >&2
  docker exec "$SENTINEL_CONTAINER" bash -c 'tail -n 20 /sentinel/*.log' >&2 || true
  exit 1
fi
grant_optional_acl "$SENTINEL_CONTAINER" 6470 6471 6472

# --- summary -------------------------------------------------------------------------------------------
echo "auth:            $(docker exec "$AUTH_CONTAINER" redis-cli -p "$AUTH_PORT" -a "$DEFAULT_PW" --no-auth-warning info server | grep -E '^redis_version|^valkey_version' | tr -d '\r')"
echo "mtls:             $(docker exec "$MTLS_CONTAINER" redis-cli --tls --cacert /certs/ca.crt --cert /certs/client.crt --key /certs/client.key -p "$MTLS_PORT" ping)"
echo "auth-cluster:     $(docker exec "$CLUSTER_CONTAINER" redis-cli -a "$DEFAULT_PW" --no-auth-warning -p 7300 cluster info | grep -E 'cluster_state|cluster_known_nodes' | tr -d '\r' | tr '\n' ' ')"
for name in authsame authopen authother; do
  case "$name" in
    authsame) sp="-a $DEFAULT_PW --no-auth-warning" ;;
    authopen) sp="" ;;
    authother) sp="-a rnc-sentinel-pw --no-auth-warning" ;;
  esac
  case "$name" in
    authsame) port=26390 ;;
    authopen) port=26391 ;;
    authother) port=26392 ;;
  esac
  echo "auth-sentinel $name ($port): master=$(docker exec "$SENTINEL_CONTAINER" redis-cli -p $port $sp SENTINEL get-master-addr-by-name $name | tr '\n' ':' | sed 's/:$//')"
done
