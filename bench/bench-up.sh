#!/usr/bin/env bash
# Stands up (or recreates) the standalone benchmark Redis/Valkey server used by bench/run-matrix.sh and
# bench/RedisNearCache.Bench --load, container redis-near-cache-bench, host port 6420 -> container 6379.
# Idempotent: any existing redis-near-cache-bench container is removed first. This is a separate container
# from the repo's integration-test standalone server (redis-near-cache-redis, port 6379) so benchmarking
# never touches the containers the test suite relies on.
#
# Usage: bench/bench-up.sh [--image IMG] [--cpus N] [--latency DELAY]
#   --image IMG      server image, default ${RNC_REDIS_IMAGE:-redis:7.4}
#   --cpus N         container CPU quota, default 2
#   --latency DELAY  netem delay to apply once the server is up (see bench/bench-latency.sh), default none
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE="${RNC_REDIS_IMAGE:-redis:7.4}"
CPUS=2
LATENCY=""

while [ $# -gt 0 ]; do
  case "$1" in
    --image) IMAGE="$2"; shift 2 ;;
    --cpus) CPUS="$2"; shift 2 ;;
    --latency) LATENCY="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

CONTAINER=redis-near-cache-bench

# Valkey images ship both redis-server and valkey-server (the former is a compatibility symlink), but detect
# by image name too so a future image that only has valkey-server still works.
SERVER_BIN=redis-server
if [[ "$IMAGE" == *valkey* ]]; then
  SERVER_BIN=valkey-server
fi
CLI_BIN=redis-cli
if [[ "$IMAGE" == *valkey* ]]; then
  CLI_BIN=valkey-cli
fi

start_and_wait() {
  local server_bin="$1" cli_bin="$2"
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$CONTAINER" --cpus "$CPUS" -p 6420:6379 "$IMAGE" \
    "$server_bin" --save "" --appendonly no >/dev/null
  for i in $(seq 1 30); do
    # `docker run -d` exits 0 as soon as the container is created, even if the entrypoint binary doesn't
    # exist and the container immediately dies -- so also bail out early if it's no longer running.
    if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" != true ]; then return 1; fi
    if docker exec "$CONTAINER" "$cli_bin" ping 2>/dev/null | grep -q PONG; then CLI_BIN="$cli_bin"; return 0; fi
    sleep 1
  done
  return 1
}

if ! start_and_wait "$SERVER_BIN" "$CLI_BIN"; then
  # Fall back to the other binary name if the image doesn't have the one we guessed.
  if [ "$SERVER_BIN" = "redis-server" ]; then ALT_SERVER=valkey-server; ALT_CLI=valkey-cli; else ALT_SERVER=redis-server; ALT_CLI=redis-cli; fi
  if ! start_and_wait "$ALT_SERVER" "$ALT_CLI"; then
    echo "redis-near-cache-bench did not come up (image=$IMAGE)" >&2
    docker logs "$CONTAINER" >&2 || true
    exit 1
  fi
fi

if [ -n "$LATENCY" ]; then
  "$(dirname "$0")/bench-latency.sh" "$LATENCY"
fi

echo "server:  $(docker exec "$CONTAINER" "$CLI_BIN" info server | grep -E '^redis_version|^valkey_version' | tr -d '\r' | tr '\n' ' ')"
echo "qdisc:   $(docker run --rm --net container:"$CONTAINER" --cap-add NET_ADMIN nicolaka/netshoot tc qdisc show dev eth0 2>/dev/null | tr -d '\r')"
