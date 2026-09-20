#!/usr/bin/env bash
# Single-node Redis Software (Redis Enterprise) in ONE container, headless: the same proxy (DMC) that fronts
# Azure Managed Redis, Redis Cloud and Redis Software sits between the client and the shard, so this is the
# closest local stand-in for those services. It reproduces their documented client-side caching rules
# (RESP3 only, no REDIRECT / two-connection mode) that managed-up.sh cannot, because it has no proxy.
# Redis documents the Docker image as dev/test only; a trial licence is installed when none is given.
#   database  localhost:12000   (no password)                         name nearcache
#   database  localhost:12001   (password rnc-enterprise-pw)          name nearcache-auth
#   admin UI  https://localhost:8443    REST API  https://localhost:9443   (self-signed TLS)
#   admin login RNC_ENTERPRISE_USER / RNC_ENTERPRISE_PASSWORD (defaults below)
# Image overridable with RNC_ENTERPRISE_IMAGE (default redislabs/redis:8.2.0-78.15, multi-arch amd64/arm64).
# Docker Desktop needs ~4 GB allocated. Idempotent (an old container is removed first).
set -euo pipefail
IMAGE="${RNC_ENTERPRISE_IMAGE:-redislabs/redis:8.2.0-78.15}"   # pinned: a floating tag could break CI with no repo change
CONTAINER=redis-near-cache-enterprise
ADMIN_USER="${RNC_ENTERPRISE_USER:-admin@example.com}"
ADMIN_PASSWORD="${RNC_ENTERPRISE_PASSWORD:-NearCache1}"   # rladmin tokenizes on "-": no hyphens
DB_PORT=12000
DB_NAME=nearcache
AUTH_DB_PORT=12001
AUTH_DB_NAME=nearcache-auth
AUTH_DB_PASSWORD=rnc-enterprise-pw

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --cap-add sys_resource --name "$CONTAINER" \
  -p 8443:8443 -p 9443:9443 -p "$DB_PORT:$DB_PORT" -p "$AUTH_DB_PORT:$AUTH_DB_PORT" "$IMAGE" >/dev/null

# --- 1. bootstrap the cluster (rladmin refuses until the node's services are up; retry) -----------------
created=0
for i in $(seq 1 90); do
  if docker exec "$CONTAINER" rladmin cluster create name cluster.local \
       username "$ADMIN_USER" password "$ADMIN_PASSWORD" 2>/dev/null | grep -qi 'ok'; then
    created=1; break
  fi
  sleep 2
done
if [ $created != 1 ]; then
  echo "Redis Software cluster did not bootstrap" >&2
  docker logs --tail 30 "$CONTAINER" >&2 || true
  exit 1
fi

# --- 2. create the databases over the REST API from inside the container --------------------------------
api() { # method path [json]
  docker exec "$CONTAINER" curl -sk -u "$ADMIN_USER:$ADMIN_PASSWORD" -X "$1" "https://localhost:9443$2" \
    -H 'Content-Type: application/json' ${3:+-d "$3"}
}

# Creates a database, waits for it to become active, then waits for the proxy to answer PING (authenticated
# with $password, if given). Prints the new database's uid on stdout on success; returns non-zero and prints
# diagnostics on stderr on failure. The bdb REST field for a database password is authentication_redis_pass.
create_and_wait_db() { # name port [password]
  local name="$1" port="$2" password="${3:-}"
  local payload="{\"name\":\"$name\",\"type\":\"redis\",\"port\":$port,\"memory_size\":268435456"
  if [ -n "$password" ]; then
    payload="$payload,\"authentication_redis_pass\":\"$password\""
  fi
  payload="$payload}"

  local uid="" reply=""
  for i in $(seq 1 30); do
    reply=$(api POST /v1/bdbs "$payload" || true)
    uid=$(printf '%s' "$reply" | python3 -c 'import sys,json
try: print(json.load(sys.stdin).get("uid",""))
except Exception: print("")' 2>/dev/null || true)
    [ -n "$uid" ] && break
    sleep 2
  done
  if [ -z "$uid" ]; then
    echo "database $name create failed: $reply" >&2
    return 1
  fi

  local active=0
  for i in $(seq 1 60); do
    if api GET "/v1/bdbs/$uid" | grep -q '"status": *"active"'; then active=1; break; fi
    sleep 2
  done
  if [ $active != 1 ]; then
    echo "database $name ($uid) did not become active" >&2
    api GET "/v1/bdbs/$uid" >&2 || true
    return 1
  fi

  local pong=0
  for i in $(seq 1 30); do
    if [ -n "$password" ]; then
      docker exec "$CONTAINER" redis-cli -p "$port" -a "$password" --no-auth-warning ping 2>/dev/null | grep -q PONG && { pong=1; break; }
    else
      docker exec "$CONTAINER" redis-cli -p "$port" ping 2>/dev/null | grep -q PONG && { pong=1; break; }
    fi
    sleep 1
  done
  if [ $pong != 1 ]; then
    echo "the proxy on port $port never answered PING although database $name ($uid) is active" >&2
    docker exec "$CONTAINER" rladmin status 2>&1 | tail -n 20 >&2 || true
    return 1
  fi

  echo "$uid"
}

DB_UID=$(create_and_wait_db "$DB_NAME" "$DB_PORT") || exit 1
AUTH_DB_UID=$(create_and_wait_db "$AUTH_DB_NAME" "$AUTH_DB_PORT" "$AUTH_DB_PASSWORD") || exit 1

refused="$(docker exec "$CONTAINER" redis-cli -p "$AUTH_DB_PORT" ping 2>&1 || true)"
if echo "$refused" | grep -qi NOAUTH; then
  echo "$AUTH_DB_NAME: unauthenticated PING refused as expected -> $refused"
else
  echo "$AUTH_DB_NAME: unauthenticated PING was NOT refused: $refused" >&2
  exit 1
fi

echo "enterprise:      $(docker exec "$CONTAINER" redis-cli -p "$DB_PORT" INFO server | grep -E '^redis_version' | tr -d '\r') db uid=$DB_UID proxy port $DB_PORT (no password)"
echo "db version:      $(api GET "/v1/bdbs/$DB_UID" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("redis_version"), "resp3" if d.get("resp3") else "")')"
echo "enterprise-auth: $(docker exec "$CONTAINER" redis-cli -p "$AUTH_DB_PORT" -a "$AUTH_DB_PASSWORD" --no-auth-warning INFO server | grep -E '^redis_version' | tr -d '\r') db uid=$AUTH_DB_UID proxy port $AUTH_DB_PORT (password $AUTH_DB_PASSWORD)"
echo "db version:      $(api GET "/v1/bdbs/$AUTH_DB_UID" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("redis_version"), "resp3" if d.get("resp3") else "")')"
