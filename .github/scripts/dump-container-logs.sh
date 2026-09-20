#!/usr/bin/env bash
# Prints what is needed to diagnose a failed integration run: every test container's state and docker logs,
# plus the per-process server and sentinel log files that sentinel-up.sh and auth-up.sh's auth-sentinel
# container write inside their containers.
set -uo pipefail
docker ps -a --filter name=redis-near-cache --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'
for c in $(docker ps -a --filter name=redis-near-cache --format '{{.Names}}'); do
  echo "::group::docker logs $c"
  docker logs --tail 200 "$c" 2>&1
  echo "::endgroup::"
done
if docker ps --format '{{.Names}}' | grep -qx redis-near-cache-sentinel; then
  echo "::group::sentinel topology logs"
  docker exec redis-near-cache-sentinel bash -c 'for f in /sentinel/*.log; do echo "== $f"; tail -n 200 "$f"; done'
  echo "::endgroup::"
fi
if docker ps --format '{{.Names}}' | grep -qx redis-near-cache-auth-sentinel; then
  echo "::group::auth-sentinel topology logs"
  docker exec redis-near-cache-auth-sentinel bash -c 'for f in /sentinel/*.log; do echo "== $f"; tail -n 200 "$f"; done'
  echo "::endgroup::"
fi
exit 0
