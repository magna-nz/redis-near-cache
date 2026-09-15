#!/usr/bin/env bash
# Removes the benchmark server container, redis-near-cache-bench. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."

docker rm -f redis-near-cache-bench >/dev/null 2>&1 || true
echo "redis-near-cache-bench removed"
