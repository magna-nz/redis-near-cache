#!/usr/bin/env bash
# Brings up everything the integration tests need: standalone Redis + replica, a TLS Redis, the cluster,
# the Sentinel topology, the managed-style emulation containers (restricted commands, hostname cluster), and
# the authenticated ones (password + ACL users, mTLS, password-protected cluster and Sentinel).
# Same script locally and in CI. Override the image with RNC_REDIS_IMAGE (default redis:7.4).
set -euo pipefail
cd "$(dirname "$0")"
./certs/gen.sh
docker compose up -d --force-recreate
./cluster-up.sh
./sentinel-up.sh
./managed-up.sh
./auth-up.sh
for i in $(seq 1 30); do docker exec redis-near-cache-redis redis-cli ping 2>/dev/null | grep -q PONG && break; sleep 1; done
for i in $(seq 1 30); do docker exec redis-near-cache-replica redis-cli -p 6380 info replication 2>/dev/null | grep -q master_link_status:up && break; sleep 1; done
for i in $(seq 1 30); do docker exec redis-near-cache-tls redis-cli --tls --cacert /certs/ca.crt -p 6390 ping 2>/dev/null | grep -q PONG && break; sleep 1; done
echo "standalone: $(docker exec redis-near-cache-redis redis-cli info server | grep -E '^redis_version|^valkey_version' | tr -d '\r')"
echo "replica:    $(docker exec redis-near-cache-replica redis-cli -p 6380 info replication | grep master_link_status | tr -d '\r')"
echo "tls:        $(docker exec redis-near-cache-tls redis-cli --tls --cacert /certs/ca.crt -p 6390 ping)"
