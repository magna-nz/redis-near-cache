#!/usr/bin/env bash
# Single-node Redis Software (Redis Enterprise) in ONE container, headless: the same proxy (DMC) that fronts
# Azure Managed Redis, Redis Cloud and Redis Software sits between the client and the shard, so this is the
# closest local stand-in for those services. It reproduces their documented client-side caching rules
# (RESP3 only, no REDIRECT / two-connection mode) that managed-up.sh cannot, because it has no proxy.
# Redis documents the Docker image as dev/test only; a trial licence is installed when none is given.
#   database  localhost:12000   (no password)
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

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --cap-add sys_resource --name "$CONTAINER" \
  -p 8443:8443 -p 9443:9443 -p "$DB_PORT:$DB_PORT" "$IMAGE" >/dev/null

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

# --- 2. create the database over the REST API from inside the container --------------------------------
api() { # method path [json]
  docker exec "$CONTAINER" curl -sk -u "$ADMIN_USER:$ADMIN_PASSWORD" -X "$1" "https://localhost:9443$2" \
    -H 'Content-Type: application/json' ${3:+-d "$3"}
}
uid=""
for i in $(seq 1 30); do
  reply=$(api POST /v1/bdbs "{\"name\":\"$DB_NAME\",\"type\":\"redis\",\"port\":$DB_PORT,\"memory_size\":268435456}" || true)
  uid=$(printf '%s' "$reply" | python3 -c 'import sys,json
try: print(json.load(sys.stdin).get("uid",""))
except Exception: print("")' 2>/dev/null || true)
  [ -n "$uid" ] && break
  sleep 2
done
if [ -z "$uid" ]; then
  echo "database create failed: $reply" >&2
  exit 1
fi

active=0
for i in $(seq 1 60); do
  if api GET "/v1/bdbs/$uid" | grep -q '"status": *"active"'; then active=1; break; fi
  sleep 2
done
if [ $active != 1 ]; then
  echo "database $uid did not become active" >&2
  api GET "/v1/bdbs/$uid" >&2 || true
  exit 1
fi
pong=0
for i in $(seq 1 30); do
  if docker exec "$CONTAINER" redis-cli -p "$DB_PORT" ping 2>/dev/null | grep -q PONG; then pong=1; break; fi
  sleep 1
done
if [ $pong != 1 ]; then
  echo "the proxy on port $DB_PORT never answered PING although database $uid is active" >&2
  docker exec "$CONTAINER" rladmin status 2>&1 | tail -n 20 >&2 || true
  exit 1
fi

echo "enterprise: $(docker exec "$CONTAINER" redis-cli -p "$DB_PORT" INFO server | grep -E '^redis_version' | tr -d '\r') db uid=$uid proxy port $DB_PORT"
echo "db version: $(api GET "/v1/bdbs/$uid" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("redis_version"), "resp3" if d.get("resp3") else "")')"
