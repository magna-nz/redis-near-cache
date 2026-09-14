#!/usr/bin/env bash
# Applies (or clears) injected network latency on the benchmark server container, redis-near-cache-bench.
# Uses a short-lived nicolaka/netshoot helper container sharing the bench container's network namespace to
# run `tc qdisc ... netem delay DELAY` against its eth0. netem delays the container's egress, which adds
# DELAY to every round trip the host makes to it (the reply's egress is delayed too) -- so a load test
# against localhost:6420 sees roughly DELAY of extra round-trip time per command.
#
# Usage: bench/bench-latency.sh DELAY
#   DELAY   netem delay syntax, e.g. 0, 500us, 2ms. 0 removes the root qdisc (no-op if none is set).
set -euo pipefail
cd "$(dirname "$0")/.."

DELAY="${1:-}"
if [ -z "$DELAY" ]; then
  echo "usage: bench/bench-latency.sh DELAY   (e.g. 0, 500us, 2ms)" >&2
  exit 1
fi

CONTAINER=redis-near-cache-bench

if [ "$DELAY" = "0" ]; then
  docker run --rm --net container:"$CONTAINER" --cap-add NET_ADMIN nicolaka/netshoot \
    sh -c 'tc qdisc del dev eth0 root 2>&1 | grep -v "Cannot delete qdisc with handle of zero\|No such file or directory" || true'
else
  docker run --rm --net container:"$CONTAINER" --cap-add NET_ADMIN nicolaka/netshoot \
    tc qdisc replace dev eth0 root netem delay "$DELAY"
fi

echo -n "qdisc: "
docker run --rm --net container:"$CONTAINER" --cap-add NET_ADMIN nicolaka/netshoot tc qdisc show dev eth0
