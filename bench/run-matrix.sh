#!/usr/bin/env bash
# Runs the full benchmark matrix (contender x write-mode x latency, topology sweep, and -- once wired up by
# a later agent -- the BenchmarkDotNet and Sailfish micro suites) and renders bench/RedisNearCache.Bench.Report
# over the results. Never run in CI; run by hand locally.
#
# Usage: bench/run-matrix.sh [--quick] [--only load|bdn|sailfish|topologies] [--out DIR] [--seconds N] [--dry-run]
#   --quick     8-second runs, a small profile, and only the 0ms latency point. For a fast sanity check.
#   --only X    restrict to one part of the matrix; repeatable (e.g. --only load --only topologies).
#               Default: run everything (load, bdn, sailfish, topologies).
#   --out DIR   results directory. Default bench/results/<timestamp>.
#   --seconds N override the per-run duration (default 30, or 8 under --quick).
#   --dry-run   print every docker/dotnet command instead of running it; no containers are touched and
#               neither bench project is built. For checking the matrix logic without a working bench build.
set -euo pipefail
cd "$(dirname "$0")/.."

if command -v dotnet >/dev/null 2>&1; then
  DOTNET="${DOTNET:-dotnet}"
else
  DOTNET="${DOTNET:-$HOME/.dotnet/dotnet}"
fi

QUICK=0
ONLY=()
OUT="bench/results/$(date +%Y-%m-%d-%H%M)"
SECS_OVERRIDE=""
DRY_RUN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --quick) QUICK=1; shift ;;
    --only) ONLY+=("$2"); shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --seconds) SECS_OVERRIDE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [ -n "$SECS_OVERRIDE" ]; then
  SECS="$SECS_OVERRIDE"
elif [ "$QUICK" = 1 ]; then
  SECS=8
else
  SECS=30
fi

QUICK_ARGS=()
if [ "$QUICK" = 1 ]; then
  QUICK_ARGS=(--instances 4 --readers 2 --writers 2 --writes-per-sec 400 --keys 2000 --hot-keys 100)
fi

# --- part selection -------------------------------------------------------------------------------------

want() {
  local part="$1"
  if [ ${#ONLY[@]} -eq 0 ]; then return 0; fi
  local o
  for o in "${ONLY[@]}"; do [ "$o" = "$part" ] && return 0; done
  return 1
}

# --- command execution, respecting --dry-run ------------------------------------------------------------

# Runs an external command (a bench/*.sh helper, or dotnet build/run). Under --dry-run, prints the command
# instead of executing it -- nothing is built and no container is touched.
x() {
  if [ "$DRY_RUN" = 1 ]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

FAILURES="$OUT/failures.txt"

# Runs one load-test invocation: dotnet run ... --json <json>, with stdout+stderr teed to <log>. A failure
# is recorded in $OUT/failures.txt and does not abort the matrix.
run_load() {
  local json="$1" log="$2"
  shift 2
  if [ "$DRY_RUN" = 1 ]; then
    printf '+'
    printf ' %q' "$DOTNET" run -c Release --no-build --project bench/RedisNearCache.Bench -- "$@" --json "$json"
    printf '\n'
    echo "  (stdout+stderr tee'd to $log; failures recorded in $FAILURES)"
    return 0
  fi
  mkdir -p "$(dirname "$json")" "$(dirname "$log")" "$(dirname "$FAILURES")"
  if ! "$DOTNET" run -c Release --no-build --project bench/RedisNearCache.Bench -- "$@" --json "$json" 2>&1 | tee "$log"; then
    echo "FAILED: $json" | tee -a "$FAILURES" >&2
  fi
}

container_running() {
  docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$1"
}

label_for() {
  case "$1" in
    0) echo "0ms" ;;
    500us) echo "0.5ms" ;;
    2ms) echo "2ms" ;;
    *) echo "$1" ;;
  esac
}

# --- preflight -------------------------------------------------------------------------------------------

preflight() {
  echo "== preflight =="
  if [ "$DRY_RUN" = 1 ]; then
    echo "+ docker info"
    echo "+ $DOTNET --version"
  else
    docker info >/dev/null || { echo "docker is not reachable" >&2; exit 1; }
    "$DOTNET" --version >/dev/null || { echo "dotnet ($DOTNET) is not resolvable" >&2; exit 1; }
  fi
  x "$DOTNET" build -c Release -nologo bench/RedisNearCache.Bench
  x "$DOTNET" build -c Release -nologo bench/RedisNearCache.Bench.Report
}

write_environment() {
  echo "== environment =="
  if [ "$DRY_RUN" = 1 ]; then
    echo "+ write $OUT/environment.txt (date, git rev-parse HEAD, uname -a, CPU model, docker info CPUs/memory, dotnet --info head, image names)"
    return
  fi
  mkdir -p "$OUT"
  {
    echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git rev-parse HEAD: $(git rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "uname -a: $(uname -a)"
    if [ "$(uname)" = "Darwin" ]; then
      echo "CPU: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
    else
      echo "CPU: $(lscpu 2>/dev/null | grep 'Model name:' | sed 's/Model name:[[:space:]]*//' || echo unknown)"
    fi
    echo "docker info CPUs: $(docker info --format '{{.NCPU}}' 2>/dev/null || echo unknown)"
    echo "docker info memory: $(docker info --format '{{.MemTotal}}' 2>/dev/null || echo unknown)"
    echo "dotnet --info (head):"
    "$DOTNET" --info 2>/dev/null | head -5
    echo "images: redis:7.4 (default bench server), valkey/valkey:8.1 (valkey topology sweep)"
  } > "$OUT/environment.txt"
}

# --- load matrix -------------------------------------------------------------------------------------------

run_load_matrix() {
  if ! want load; then echo "skipping load matrix (--only)"; return; fi
  echo "== load matrix =="

  x bash bench/bench-up.sh

  local latencies=(0 500us 2ms)
  if [ "$QUICK" = 1 ]; then latencies=(0); fi

  local contenders=(Plain MemoryCacheTtl HybridCache FusionCache NearCache NearCacheHybridCache)
  local modes=(foreign api)

  local l label k m
  for l in "${latencies[@]}"; do
    label="$(label_for "$l")"
    x bash bench/bench-latency.sh "$l"

    for k in "${contenders[@]}"; do
      for m in "${modes[@]}"; do
        local name="standalone-$label-$k-$m"
        run_load "$OUT/load/$name.json" "$OUT/logs/$name.log" \
          --load --endpoint localhost:6420 --contender "$k" --write-mode "$m" --topology standalone \
          --latency-label "$label" --seconds "$SECS" ${QUICK_ARGS[@]+"${QUICK_ARGS[@]}"}
      done
    done

    if [ "$label" = "0ms" ]; then
      local chaos_name="standalone-0ms-NearCache-foreign-chaos"
      run_load "$OUT/load/$chaos_name.json" "$OUT/logs/$chaos_name.log" \
        --load --endpoint localhost:6420 --contender NearCache --write-mode foreign --topology standalone \
        --latency-label 0ms --seconds "$SECS" ${QUICK_ARGS[@]+"${QUICK_ARGS[@]}"} --chaos
    fi
  done
}

# --- topologies --------------------------------------------------------------------------------------------

run_topologies() {
  if ! want topologies; then echo "skipping topologies (--only)"; return; fi
  echo "== topologies =="

  local contenders=(NearCache Plain)
  local k

  if container_running redis-near-cache-cluster; then
    for k in "${contenders[@]}"; do
      local name="cluster-0ms-$k-foreign"
      run_load "$OUT/load/$name.json" "$OUT/logs/$name.log" \
        --load --endpoint localhost:7100 --contender "$k" --write-mode foreign --topology cluster \
        --latency-label 0ms --seconds "$SECS" ${QUICK_ARGS[@]+"${QUICK_ARGS[@]}"}
    done
  else
    echo "WARNING: redis-near-cache-cluster is not running, skipping cluster topology" >&2
  fi

  # The TLS container mounts the certs/ of the checkout that ran ./up.sh; from another checkout or worktree, point
  # RNC_BENCH_TLS_CA at that checkout's certs/ca.crt (certs are gitignored and regenerated per checkout).
  local tls_ca="${RNC_BENCH_TLS_CA:-certs/ca.crt}"
  if container_running redis-near-cache-tls && [ -f "$tls_ca" ]; then
    for k in "${contenders[@]}"; do
      local name="tls-0ms-$k-foreign"
      run_load "$OUT/load/$name.json" "$OUT/logs/$name.log" \
        --load --endpoint "localhost:6390,ssl=true,sslHost=localhost" --tls-ca "$tls_ca" \
        --contender "$k" --write-mode foreign --topology tls --latency-label 0ms --seconds "$SECS" ${QUICK_ARGS[@]+"${QUICK_ARGS[@]}"}
    done
  else
    echo "WARNING: redis-near-cache-tls is not running or $tls_ca does not exist (set RNC_BENCH_TLS_CA), skipping tls topology" >&2
  fi

  x bash bench/bench-up.sh --image valkey/valkey:8.1
  for k in "${contenders[@]}"; do
    local name="valkey-0ms-$k-foreign"
    run_load "$OUT/load/$name.json" "$OUT/logs/$name.log" \
      --load --endpoint localhost:6420 --contender "$k" --write-mode foreign --topology valkey \
      --latency-label 0ms --seconds "$SECS" ${QUICK_ARGS[@]+"${QUICK_ARGS[@]}"}
  done
  x bash bench/bench-up.sh
}

# --- micro suites (flags implemented by a later agent; wired up here) ---------------------------------------

run_bdn() {
  if ! want bdn; then echo "skipping bdn (--only)"; return; fi
  echo "== bdn =="

  local latencies=(0 500us 2ms)
  if [ "$QUICK" = 1 ]; then latencies=(0); fi

  local quick_flag=()
  if [ "$QUICK" = 1 ]; then quick_flag=(--quick); fi

  local l label
  for l in "${latencies[@]}"; do
    label="$(label_for "$l")"
    x bash bench/bench-latency.sh "$l"
    local log="$OUT/logs/bdn-$label.log"
    if [ "$DRY_RUN" = 1 ]; then
      printf '+'
      printf ' %q' "$DOTNET" run -c Release --no-build --project bench/RedisNearCache.Bench -- \
        --bdn --endpoint localhost:6420 --latency-label "$label" --artifacts "$OUT/bdn/$label" ${quick_flag[@]+"${quick_flag[@]}"}
      printf '\n'
      echo "  (stdout+stderr tee'd to $log)"
      continue
    fi
    mkdir -p "$OUT/bdn/$label" "$(dirname "$log")"
    if ! "$DOTNET" run -c Release --no-build --project bench/RedisNearCache.Bench -- \
      --bdn --endpoint localhost:6420 --latency-label "$label" --artifacts "$OUT/bdn/$label" ${quick_flag[@]+"${quick_flag[@]}"} \
      2>&1 | tee "$log"; then
      echo "FAILED: bdn $label" | tee -a "$FAILURES" >&2
    fi
  done
}

run_sailfish() {
  if ! want sailfish; then echo "skipping sailfish (--only)"; return; fi
  echo "== sailfish =="

  local latencies=(0 500us 2ms)
  if [ "$QUICK" = 1 ]; then latencies=(0); fi

  local quick_flag=()
  if [ "$QUICK" = 1 ]; then quick_flag=(--quick); fi

  local l label
  for l in "${latencies[@]}"; do
    label="$(label_for "$l")"
    x bash bench/bench-latency.sh "$l"
    local log="$OUT/logs/sailfish-$label.log"
    if [ "$DRY_RUN" = 1 ]; then
      printf '+'
      printf ' %q' "$DOTNET" run -c Release --no-build --project bench/RedisNearCache.Bench -- \
        --sailfish --endpoint localhost:6420 --latency-label "$label" --output "$OUT/sailfish/$label" ${quick_flag[@]+"${quick_flag[@]}"}
      printf '\n'
      echo "  (stdout+stderr tee'd to $log)"
      continue
    fi
    mkdir -p "$OUT/sailfish/$label" "$(dirname "$log")"
    if ! "$DOTNET" run -c Release --no-build --project bench/RedisNearCache.Bench -- \
      --sailfish --endpoint localhost:6420 --latency-label "$label" --output "$OUT/sailfish/$label" ${quick_flag[@]+"${quick_flag[@]}"} \
      2>&1 | tee "$log"; then
      echo "FAILED: sailfish $label" | tee -a "$FAILURES" >&2
    fi
  done
}

# --- main --------------------------------------------------------------------------------------------------

cleanup() {
  if [ "$DRY_RUN" = 1 ]; then
    echo "+ bash bench/bench-latency.sh 0   (reset latency on exit)"
  else
    bash bench/bench-latency.sh 0 >/dev/null 2>&1 || true
  fi
  echo
  echo "redis-near-cache-bench left running. Remove it with: bench/bench-down.sh"
}
trap cleanup EXIT

echo "results directory: $OUT"
mkdir -p "$OUT" 2>/dev/null || true

preflight
write_environment
run_load_matrix
run_topologies
run_bdn
run_sailfish

echo "== report =="
x "$DOTNET" run -c Release --no-build --project bench/RedisNearCache.Bench.Report -- "$OUT"
if [ "$DRY_RUN" != 1 ]; then
  echo "summary: $OUT/summary.md"
fi
