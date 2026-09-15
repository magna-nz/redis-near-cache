# RedisNearCache benchmarks

How RedisNearCache compares with the caches a .NET team would otherwise reach for, measured three ways: a
production-shaped load test with a staleness probe, BenchmarkDotNet for per-call cost, and Sailfish for per-call
latency distributions. None of this runs in CI; everything here runs locally with one script.

<!-- TOC -->
**Contents**

- [Headline](#headline)
- [Running the benchmarks](#running-the-benchmarks)
  - [Prerequisites](#prerequisites)
  - [Everything](#everything)
  - [One piece at a time](#one-piece-at-a-time)
- [What is compared](#what-is-compared)
- [Methodology](#methodology)
  - [Write modes](#write-modes)
  - [The staleness probe](#the-staleness-probe)
  - [Other load-test columns](#other-load-test-columns)
  - [Injected latency](#injected-latency)
  - [BenchmarkDotNet](#benchmarkdotnet)
  - [Sailfish](#sailfish)
  - [Caveats](#caveats)
- [Findings](#findings)
- [Full results](#full-results)
  - [Run environment](#run-environment)
  - [Contender comparison — standalone, 0ms](#contender-comparison--standalone-0ms)
  - [Contender comparison — standalone, 0.5ms](#contender-comparison--standalone-05ms)
  - [Contender comparison — standalone, 2ms](#contender-comparison--standalone-2ms)
  - [Latency sweep](#latency-sweep)
  - [Topologies](#topologies)
  - [Chaos](#chaos)
  - [RedisNearCache internals](#redisnearcache-internals)
  - [Per-call cost — BenchmarkDotNet, 0ms](#per-call-cost--benchmarkdotnet-0ms)
  - [Per-call latency — Sailfish, 0ms](#per-call-latency--sailfish-0ms)
  - [Cross-check: BenchmarkDotNet vs Sailfish — 0ms](#cross-check-benchmarkdotnet-vs-sailfish--0ms)
  - [Per-call cost — BenchmarkDotNet, 0.5ms](#per-call-cost--benchmarkdotnet-05ms)
  - [Per-call latency — Sailfish, 0.5ms](#per-call-latency--sailfish-05ms)
  - [Cross-check: BenchmarkDotNet vs Sailfish — 0.5ms](#cross-check-benchmarkdotnet-vs-sailfish--05ms)
  - [Per-call cost — BenchmarkDotNet, 2ms](#per-call-cost--benchmarkdotnet-2ms)
  - [Per-call latency — Sailfish, 2ms](#per-call-latency--sailfish-2ms)
  - [Cross-check: BenchmarkDotNet vs Sailfish — 2ms](#cross-check-benchmarkdotnet-vs-sailfish--2ms)
- [Zero-traffic demo](#zero-traffic-demo)
- [Layout](#layout)
<!-- /TOC -->

## Headline

20 application instances, 160 concurrent readers, 2,000 writes per second made by **another client** straight to Redis
(a plain `SET`, as a different service or language would), 10,000 keys of 512 bytes with 80 % of reads on 500 hot keys,
30 seconds per run, Redis 7.4 in Docker on the same machine, no injected latency. TTL-based caches use a 10 s TTL.

<!-- HEADLINE -->
| | Plain StackExchange.Redis | `IMemoryCache` + 10 s TTL | `HybridCache` + Redis L2 | FusionCache + backplane | **RedisNearCache** |
|---|---:|---:|---:|---:|---:|
| Reads/s | 164,527 | 14.43 M | 25.60 M | 6.59 M | 1.35 M |
| Server commands/s | 166,527 | 22,420 | 45,409 | 37,663 | 104,596 |
| Reads served stale | 0 | 83.1 % | 84.8 % | 85.0 % | 0.07 % |
| Stalest read | – | 10.0 s | 10.0 s | 10.0 s | 76 ms |
| Stale local entries after writes stop | – | 822 | 67 | 9,625 | 0 |
| Reads served stale, writes through the library's API | 0 | 83.2 % | 83.0 % | 2.4 % | 0.13 % |
<!-- /HEADLINE -->

What that says:

- **Only RedisNearCache stays fresh when someone else writes.** The TTL caches (hand-rolled `IMemoryCache`,
  `HybridCache`, FusionCache) served 83–85 % of reads stale, up to the full 10 s TTL, and still held stale entries
  (67 to 9,625 across 20 instances) after writes stopped. FusionCache's backplane only carries writes made through FusionCache. RedisNearCache
  served 0.07 % of reads stale, the stalest 76 ms old, and held 0 stale entries after writes stopped.
- **It costs server traffic that a TTL cache doesn't spend.** Every write invalidates the key on every instance that
  tracks it (19 instances per write here), and each of those re-reads it on the next access (`GET` plus a pipelined
  `PTTL`). That is 104,596 commands/s against 22,420 for `IMemoryCache` with a 10 s TTL. The rate follows writes, not
  reads: at 0 ms it is 37 % fewer commands than plain StackExchange.Redis while serving 8.2x the reads, but at 2 ms
  injected latency, where plain StackExchange.Redis readers slow to 64,853 commands/s, RedisNearCache sends 98,145
  (51 % more) while serving 13x the reads.
- **In-process TTL caches read faster.** `IMemoryCache` and `HybridCache` hand back a stored object reference (about
  40–50 ns per hit); RedisNearCache decodes the value from its stored bytes and takes a lock-protected coherence check
  on every hit (223 ns for a 1 KB string, see [Findings](#findings)). Under 160 spinning readers that shows up as
  1.35 M reads/s against 6.6 M (FusionCache) to 25.6 M (`HybridCache`).
- **A miss costs about what a plain `GET` costs.** RedisNearCache's cold read (a `GET` with a pipelined `PTTL`) had a
  median 0.95–1.17x a plain StackExchange.Redis `GET` across payloads and latencies (Sailfish, 1,000 samples each);
  `HybridCache` and FusionCache cold reads cost about 2x and 3x a `GET` once there is any latency, because they also
  write their Redis L2.

## Running the benchmarks

### Prerequisites

- Docker (Docker Desktop on macOS works).
- The .NET 10 SDK. The scripts use `dotnet` from `PATH`, then `~/.dotnet/dotnet`; set `DOTNET=/path/to/dotnet` to override.
- For the cluster and TLS topologies: the repo's integration containers, `./up.sh` from the repo root (it also
  generates `certs/`).

### Everything

```bash
bench/run-matrix.sh
```

Builds the three bench projects, starts a dedicated benchmark server (`redis-near-cache-bench`, host port 6420, 2 CPUs),
and runs, in order:

1. the load matrix: 6 contenders × 2 write modes × 3 injected latencies, 30 s each, plus one chaos run;
2. topologies: RedisNearCache and plain StackExchange.Redis on the cluster (`localhost:7100`), TLS (`localhost:6390`)
   and Valkey 8.1;
3. BenchmarkDotNet hit and miss paths at each latency;
4. Sailfish hit and miss paths at each latency;
5. the report: `bench/results/<timestamp>/summary.md`, the tables in [Full results](#full-results).

A full run takes about an hour on an Apple M4 Pro (load matrix and topologies about 30 minutes, BenchmarkDotNet about 20). Results go to `bench/results/` (gitignored). A run that fails is
logged to `failures.txt` in the results directory and the matrix carries on.

| Option | What it does |
|---|---|
| `--quick` | 8 s load runs with 4 instances × 2 readers, 0 ms only, short BenchmarkDotNet/Sailfish jobs (about 10 minutes). |
| `--only load\|topologies\|bdn\|sailfish` | Run only that part; repeatable. |
| `--out DIR` | Results directory (default `bench/results/<timestamp>`). Re-running one part into an existing directory replaces that part and re-renders the report. |
| `--seconds N` | Load run duration. |
| `--dry-run` | Print every command instead of running it. |

| Environment variable | Default | Used for |
|---|---|---|
| `DOTNET` | `dotnet` on `PATH`, else `~/.dotnet/dotnet` | The SDK the scripts and BenchmarkDotNet's child processes use. |
| `RNC_REDIS_IMAGE` | `redis:7.4` | Benchmark server image. |
| `RNC_BENCH_TLS_CA` | `certs/ca.crt` | CA of the TLS container. The container mounts the `certs/` of the checkout that ran `./up.sh`; from another checkout or worktree, point this at that one. |

When you're done: `bench/bench-down.sh` removes the benchmark server.

### One piece at a time

```bash
bench/bench-up.sh [--image valkey/valkey:8.1] [--cpus 2] [--latency 2ms]   # (re)create the benchmark server
bench/bench-latency.sh 500us                                               # change injected latency: 0, 500us, 2ms, ...

# load test: one contender, one write mode
dotnet run -c Release --project bench/RedisNearCache.Bench -- --load --endpoint localhost:6420 \
  --contender NearCache --write-mode foreign --json out.json
# knobs: --instances 20 --readers 8 --writers 4 --writes-per-sec 2000 --keys 10000 --hot-keys 500 --hot-fraction 0.8
#        --value-bytes 512 --seconds 30 --ttl-seconds 10 --chaos --topology <label> --latency-label <label> --tls-ca <path>
# contenders: Plain MemoryCacheTtl HybridCache FusionCache NearCache NearCacheHybridCache;  --baseline = --contender Plain

# BenchmarkDotNet (each benchmark in its own process)
dotnet run -c Release --project bench/RedisNearCache.Bench -- --bdn --endpoint localhost:6420 --latency-label 0ms --artifacts out/bdn [--quick]

# Sailfish
dotnet run -c Release --project bench/RedisNearCache.Bench.Sailfish -- --endpoint localhost:6420 --latency-label 0ms --output out/sailfish [--quick]

# render a report over a results directory (load/*.json, bdn/<label>/, sailfish/<label>/)
dotnet run -c Release --project bench/RedisNearCache.Bench.Report -- bench/results/<run>

# zero-traffic demo (standalone container on 6379)
dotnet run -c Release --project bench/RedisNearCache.Bench -- --demo
```

## What is compared

Every contender reads and writes the same Redis key, holding the raw value (the "source of truth"). Contenders with a
Redis L2 of their own keep it under `bench:l2:` and load from the source key in their factory, the way an application
caches data that lives in Redis. Every contender gets the same key distribution, value, TTL and local entry limit.

| Contender | Setup | How it learns a value changed |
|---|---|---|
| `Plain` | StackExchange.Redis `GET`. No local tier. The floor. | Every read is a round trip. |
| `MemoryCacheTtl` | `IMemoryCache`, 10 s absolute expiry, in front of a `GET`. The hand-rolled near cache. | It doesn't; expiry. |
| `HybridCache` | `Microsoft.Extensions.Caching.Hybrid` 10.10 with its default L1 and `Microsoft.Extensions.Caching.StackExchangeRedis` as L2, 10 s expiration. | It doesn't across instances; expiry. |
| `FusionCache` | ZiggyCreatures.FusionCache 2.8 with L1, Redis L2, System.Text.Json and the official Redis backplane, 10 s duration. | Backplane message, for writes made through FusionCache only. |
| `NearCache` | `IRedisNearCache` directly. | Redis `CLIENT TRACKING` invalidation, for any write by any client. |
| `NearCacheHybridCache` | `HybridCache` over `AddRedisNearCacheHybridCache` (HybridCache's L1 disabled, RedisNearCache's L1 behind it), 10 s expiration. | Tracking of the HybridCache entry's key. A write to the source key doesn't touch that entry, so foreign writes are only seen at expiration. |

FusionCache options changed from its defaults, each so the comparison is fair to it: `WaitForInitialBackplaneSubscribe`
(no backplane messages lost at startup) and a size-limited `MemoryCache` (same local entry limit as everyone else).
`HybridCache` sizes local entries in bytes, so it runs without a local limit; the key count is half the limit, so no
contender evicts for size.

## Methodology

### Write modes

- **`foreign`**: writers are separate plain multiplexers doing `SET key value`, standing in for another service or
  language. No library sees the write.
- **`api`**: writers call the contender's own write API on a separate writer instance of the same kind
  (`HybridCache.SetAsync`, `FusionCache.SetAsync`, `IRedisNearCache.SetAsync`, ...), so whatever cross-instance
  mechanism the library has gets its chance.

### The staleness probe

Every value is `v{version}:` plus padding. Keys are partitioned so each has exactly one writer, which never has two
writes to it in flight, so Redis holds versions in order. When a write is acknowledged (for `api`, when the whole
library call has returned), the writer records the time against that version.

A read is **stale** when it returns version *v* although a newer version had been acknowledged **before the read
started**. A read that races a write is never counted. The **staleness age** of a stale read is its completion time
minus the acknowledgement time of the oldest version newer than the one it returned. Every version is kept for the whole
run, so ages are not truncated for caches that serve values many versions old.

The probe can only undercount: the acknowledgement time is taken when the writer's continuation runs, which is never
before Redis actually replied. An independent verification pass measured the undercount for RedisNearCache at 3–5 % of
its (millisecond-scale) stale reads, and found no case of a fresh read counted stale. Plain StackExchange.Redis reads
report 0 stale reads on a single node, a cluster, TLS and Valkey, which is the probe's sanity check.

After the run the load test waits 2 s and then audits every local entry of every instance against Redis ("stale local
entries after quiescence"); `n/a` means the contender has no local-only read to audit.

### Other load-test columns

- **Local hit %**: reads whose `ValueTask` was already complete when returned, i.e. served without a network await.
  `n/a` for `Plain` (no local tier) and `NearCacheHybridCache` (HybridCache's stampede coordination completes
  concurrent local hits asynchronously, so completion can't classify them).
- **Source loads / read**: reads that went to the source key (a `GET`, a factory call, a RedisNearCache miss).
- **Server cmds/s**, network output, memory: `INFO` deltas summed over every master.

### Injected latency

`bench/bench-latency.sh` adds `tc netem` delay to the benchmark server container's egress, which delays every reply.
The settings are nominal: on Docker Desktop the measured round trip of a plain `GET` (BenchmarkDotNet hit path, one
caller) was about 0.14 ms at `0`, 1.05 ms at `500us` and 3.3 ms at `2ms`. The result tables are labelled with the
setting.

### BenchmarkDotNet

- `HitBenchmarks<T>` / `MissBenchmarks<T>` over `string` (1 KB) and a 5-field record (`Json`), one benchmark per
  contender, `Plain` as the baseline.
- Out-of-process toolchain: every benchmark runs in its own process, and each benchmark's setup builds only the one
  contender it times. Benchmarks return the library's own `ValueTask` with no wrapper.
- **Hit**: the key is already local (asserted: the next read completes synchronously). Throughput strategy, 5 warmup +
  15 measured iterations.
- **Miss**: every invocation reads a key that instance has never read and that has no L2 entry (a pre-seeded pool;
  the run fails rather than reuse a key). 50 invocations × 10 iterations after 3 warmup iterations.
- `Allocated` includes 56 B per call that BenchmarkDotNet itself spends awaiting a `ValueTask` benchmark
  (`MemoryCache.TryGetValue` alone allocates nothing).

### Sailfish

- One class per path and payload, one method per contender, `Plain` as the baseline; Sailfish reports each contender's
  ratio to `Plain` with a 95 % confidence interval and a q-value (Benjamini–Hochberg-adjusted).
- Each method's setup builds only that contender; teardown disposes it. Fixed seed for run order.
- **Hit**: adaptive sampling to a 2 % coefficient of variation, 50–1,000 samples. **Miss**: 1,000 samples of fresh keys.
- Sailfish times one call per sample, so it gives distributions; p95/p99 in the tables are computed from its raw
  samples. Its per-call harness overhead is several hundred nanoseconds, so **for sub-microsecond hit paths use the
  BenchmarkDotNet numbers**; the [cross-check tables](#full-results) show the gap. For network-bound paths the two agree
  within noise.
- Sailfish needs Perfolizer 0.7.1 and BenchmarkDotNet 0.15.8 pins 0.6.1, so Sailfish lives in its own project,
  `bench/RedisNearCache.Bench.Sailfish`, sharing the contender code by file link.

### Caveats

- One machine: the load generator, 20 instances and Redis (in Docker Desktop's VM, capped at 2 CPUs) share an Apple M4
  Pro. 160 readers spin as fast as they can on 14 cores, so thread-pool continuations queue: remote-read latency and
  the millisecond-scale staleness tails (RedisNearCache's invalidation handler, FusionCache's backplane handler) are
  inflated by the load generator, the same way for both. A service doing real work between reads would see less.
- Reads/s is dominated by hit-path CPU cost under this profile; it is not a model of any real service's throughput.
- TTL contenders' staleness scales with the TTL you pick; 10 s is arbitrary.
- Numbers from one run. Short checks were repeated during development and moved by a few percent; the load test's
  reads/s moved more between runs when anything else was running on the machine.

## Findings

Things the comparison surfaced that are worth knowing beyond the tables:

- **RedisNearCache's hit path takes a lock on every read.** `CachingEnabled` evaluates
  `ConcurrentDictionary.IsEmpty` on each L1 hit, and on an empty dictionary (the normal state) that acquires all of its
  locks. A throwaway prototype replacing it with a lock-free counter took the 4 × 2-reader load test from 2.6 M to
  7.3 M reads/s (local hit p50 2.3 µs → 0.3 µs). Everything in these tables was measured before any such fix.
- **Stale reads are not zero for RedisNearCache, by design.** Redis acknowledges the writer and pushes the invalidation
  to the other connections at the same moment; a read on another instance between the acknowledgement and that
  instance handling the push is served the old value. Every stale read observed was of that kind: the invalidation for
  the newer version had not been handled when the read started. The in-flight guard (a read whose key is invalidated
  while its reply is in flight never populates L1) held throughout: 0 stale entries in every audit, including chaos.
- **FusionCache with writes through its API** served 2.2–2.8 % of reads stale, p99 age 95–218 ms, with a rare maximum
  of 5.2–9.9 s (the duration is 10 s), i.e. an occasional instance keeping an old value after the backplane message.
- **The chaos run** (both connections of 5 of 20 instances killed at T/2) raised 39 read errors from the killed
  connections, 44 flushes and 17 re-arms, and the audit still found 0 stale entries.
- **`NearCacheHybridCache` with writes through HybridCache** showed a similar rare long tail at 2 ms latency (p99 4.5 s,
  max 9.7 s over 1,703 stale reads). The suspected cause is HybridCache's background write-back of a value computed
  before a newer `SetAsync` landing after it; not confirmed, and it contradicts the adapter's documentation, so it is
  being investigated separately.
- **Allocations on a hit:** RedisNearCache keeps values as bytes and decodes on every hit (2.08 KB for a 1 KB string,
  408 B for the record), where `IMemoryCache` returns the stored object (56 B, all BenchmarkDotNet's).

## Full results

<!-- BEGIN GENERATED RESULTS -->

### Run environment

```
date: 2026-09-14T23:05:54Z
git rev-parse HEAD: 0251007cb049324d80af6253b11d8927af1e2eb5
uname -a: Darwin Daniels-MacBook-Pro.local 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:17:26 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T6041 arm64
CPU: Apple M4 Pro
docker info CPUs: 14
docker info memory: 8318976000
dotnet --info (head):
.NET SDK:
 Version:           10.0.400
 Commit:            14fbf8d527
 Workload version:  10.0.400-manifests.330ea142
 MSBuild version:   18.9.6+14fbf8d52
images: redis:7.4 (default bench server), valkey/valkey:8.1 (valkey topology sweep)
BenchmarkDotNet suites re-run after two harness fixes (runner overload, miss key pool size), 2026-09-15, bench code otherwise unchanged.
```

Profile (from results):

| Setting | Value |
|---|---:|
| Instances | 20 |
| Readers per instance | 8 |
| Writers | 4 |
| Target writes/s | 2,000 |
| Keys | 10,000 |
| Hot keys | 500 |
| Hot fraction | 80.00 % |
| Value bytes | 512 |
| TTL (s) | 10.0 |
| Duration (s) | 30 |

### Contender comparison — standalone, 0ms

| Contender | Write mode | Reads/s | Local hit % | Local read p50 / p99 (µs) | Remote read p50 / p99 (µs) | Server cmds/s | Source loads / read | Errors | Stale reads (count, %) | Staleness age p50 / p99 / max (ms) | Stale local entries after quiescence |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Plain | Foreign | 164,527 | n/a | n/a | 922 / 2,114 | 166,527 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| Plain | Api | 165,281 | n/a | n/a | 922 / 2,114 | 167,281 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| MemoryCacheTtl | Foreign | 14,433,455 | 99.86 % | 0.12 / 5.5 | 1,121 / 9,134 | 22,420 | 0.14 % | 0 | 360,402,914 (83.11 %) | 5,191.5 / 9,789.4 / 10,000.3 | 822 |
| MemoryCacheTtl | Api | 14,322,608 | 99.86 % | 0.12 / 5.5 | 1,177 / 7,515 | 22,600 | 0.14 % | 0 | 358,527,927 (83.18 %) | 5,191.5 / 9,789.4 / 10,000.5 | 1,549 |
| HybridCache | Foreign | 25,597,268 | 99.92 % | 0.12 / 3.9 | 1,236 / 9,591 | 45,409 | 0.00 % | 0 | 652,132,028 (84.82 %) | 5,723.6 / 9,789.4 / 9,997.5 | 67 |
| HybridCache | Api | 24,610,078 | 99.90 % | 0.12 / 0.71 | 1,298 / 13,496 | 56,729 | 0.01 % | 0 | 614,044,977 (83.02 %) | 5,451.1 / 9,789.4 / 9,996.1 | 17,881 |
| FusionCache | Foreign | 6,587,211 | 99.69 % | 0.34 / 5.0 | 1,431 / 32,479 | 37,663 | 0.02 % | 0 | 168,458,887 (85.00 %) | 6,625.8 / 9,789.4 / 9,999.0 | 9,625 |
| FusionCache | Api | 1,570,624 | 98.76 % | 0.38 / 3.4 | 2,330 / 58,328 | 66,391 | 0.06 % | 0 | 1,123,547 (2.38 %) | 9.6 / 217.8 / 9,914.5 | 1 |
| NearCacheHybridCache | Foreign | 527,667 | n/a | n/a | 63 / 4,844 | 45,970 | 0.21 % | 0 | 13,221,024 (83.40 %) | 5,451.1 / 9,789.4 / 9,994.3 | n/a |
| NearCacheHybridCache | Api | 352,616 | n/a | n/a | 35 / 5,086 | 107,201 | 0.26 % | 0 | 5,087 (0.05 %) | 0.3 / 45.7 / 180.6 | n/a |
| NearCache | Foreign | 1,348,724 | 96.20 % | 0.71 / 30 | 1,739 / 14,171 | 104,596 | 3.80 % | 0 | 30,089 (0.07 %) | 0.3 / 39.5 / 75.9 | 0 |
| NearCache | Api | 1,353,987 | 96.22 % | 0.74 / 25 | 1,826 / 14,171 | 104,365 | 3.78 % | 0 | 52,313 (0.13 %) | 0.3 / 9.1 / 50.6 | 0 |

### Contender comparison — standalone, 0.5ms

| Contender | Write mode | Reads/s | Local hit % | Local read p50 / p99 (µs) | Remote read p50 / p99 (µs) | Server cmds/s | Source loads / read | Errors | Stale reads (count, %) | Staleness age p50 / p99 / max (ms) | Stale local entries after quiescence |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Plain | Foreign | 124,343 | n/a | n/a | 1,236 / 2,330 | 126,344 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| Plain | Api | 124,550 | n/a | n/a | 1,236 / 2,330 | 126,550 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| MemoryCacheTtl | Foreign | 15,868,961 | 99.87 % | 0.12 / 5.5 | 1,502 / 9,134 | 22,556 | 0.13 % | 0 | 397,172,553 (83.19 %) | 5,451.1 / 9,789.4 / 10,000.6 | 9,510 |
| MemoryCacheTtl | Api | 15,984,631 | 99.87 % | 0.12 / 5.5 | 1,502 / 9,134 | 22,421 | 0.13 % | 0 | 400,209,012 (83.30 %) | 5,451.1 / 9,789.4 / 10,000.2 | 9,283 |
| HybridCache | Foreign | 24,961,847 | 99.92 % | 0.12 / 3.5 | 1,577 / 9,591 | 46,137 | 0.00 % | 0 | 640,174,764 (84.91 %) | 5,723.6 / 9,789.4 / 9,996.6 | 4,318 |
| HybridCache | Api | 23,432,031 | 99.89 % | 0.12 / 3.9 | 1,577 / 12,853 | 57,018 | 0.01 % | 0 | 585,318,086 (83.02 %) | 5,451.1 / 9,789.4 / 9,994.6 | 18,653 |
| FusionCache | Foreign | 6,258,546 | 99.68 % | 0.34 / 5.8 | 1,739 / 30,933 | 38,387 | 0.02 % | 0 | 160,122,613 (85.05 %) | 6,625.8 / 9,789.4 / 9,993.2 | 3,205 |
| FusionCache | Api | 1,419,819 | 98.58 % | 0.41 / 3.4 | 2,697 / 55,551 | 68,984 | 0.07 % | 0 | 944,788 (2.21 %) | 7.2 / 127.3 / 8,634.2 | 0 |
| NearCacheHybridCache | Foreign | 525,493 | n/a | n/a | 60 / 4,844 | 45,935 | 0.21 % | 0 | 13,172,393 (83.43 %) | 5,451.1 / 9,789.4 / 9,993.6 | n/a |
| NearCacheHybridCache | Api | 322,090 | n/a | n/a | 37 / 5,086 | 106,093 | 0.28 % | 0 | 3,861 (0.04 %) | 0.2 / 228.7 / 453.4 | n/a |
| NearCache | Foreign | 1,311,492 | 96.07 % | 0.74 / 29 | 2,219 / 10,071 | 105,008 | 3.93 % | 0 | 34,151 (0.09 %) | 0.3 / 35.8 / 58.7 | 0 |
| NearCache | Api | 1,283,808 | 96.01 % | 0.74 / 27 | 2,219 / 10,574 | 104,355 | 3.99 % | 0 | 52,509 (0.14 %) | 0.3 / 5.3 / 62.1 | 0 |

### Contender comparison — standalone, 2ms

| Contender | Write mode | Reads/s | Local hit % | Local read p50 / p99 (µs) | Remote read p50 / p99 (µs) | Server cmds/s | Source loads / read | Errors | Stale reads (count, %) | Staleness age p50 / p99 / max (ms) | Stale local entries after quiescence |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Plain | Foreign | 62,852 | n/a | n/a | 2,447 / 3,443 | 64,853 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| Plain | Api | 63,007 | n/a | n/a | 2,447 / 3,279 | 65,008 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| MemoryCacheTtl | Foreign | 12,726,584 | 99.84 % | 0.12 / 5.5 | 2,697 / 9,591 | 22,615 | 0.16 % | 0 | 320,424,505 (83.37 %) | 6,009.8 / 9,789.4 / 10,000.1 | 24,904 |
| MemoryCacheTtl | Api | 14,600,079 | 99.86 % | 0.12 / 5.2 | 2,697 / 10,574 | 22,632 | 0.14 % | 0 | 366,084,740 (83.44 %) | 6,009.8 / 9,789.4 / 10,000.1 | 25,680 |
| HybridCache | Foreign | 19,533,349 | 99.90 % | 0.12 / 3.7 | 2,697 / 10,071 | 45,446 | 0.01 % | 0 | 501,124,019 (85.25 %) | 6,625.8 / 9,789.4 / 9,998.7 | 537 |
| HybridCache | Api | 19,158,871 | 99.87 % | 0.12 / 0.67 | 2,697 / 13,496 | 56,031 | 0.01 % | 0 | 478,777,332 (83.14 %) | 6,310.3 / 9,789.4 / 9,991.4 | 16,461 |
| FusionCache | Foreign | 4,475,469 | 99.55 % | 0.34 / 5.0 | 3,123 / 45,702 | 39,009 | 0.03 % | 0 | 114,667,594 (85.17 %) | 7,670.2 / 9,789.4 / 9,995.0 | 18,405 |
| FusionCache | Api | 1,183,664 | 98.45 % | 0.41 / 4.7 | 3,796 / 64,307 | 62,862 | 0.08 % | 0 | 1,005,936 (2.82 %) | 9.1 / 95.0 / 5,212.4 | 0 |
| NearCacheHybridCache | Foreign | 444,614 | n/a | n/a | 52 / 5,608 | 45,701 | 0.25 % | 0 | 11,167,403 (83.55 %) | 5,723.6 / 9,789.4 / 9,995.8 | n/a |
| NearCacheHybridCache | Api | 167,088 | n/a | n/a | 33 / 6,183 | 91,782 | 0.53 % | 0 | 1,703 (0.03 %) | 0.1 / 4,484.6 / 9,715.0 | n/a |
| NearCache | Foreign | 830,694 | 94.21 % | 0.71 / 13 | 2,974 / 5,888 | 98,145 | 5.79 % | 0 | 13,245 (0.05 %) | 0.1 / 2.2 / 60.5 | 0 |
| NearCache | Api | 806,339 | 94.06 % | 0.74 / 12 | 2,974 / 5,888 | 97,842 | 5.94 % | 0 | 15,019 (0.06 %) | 0.1 / 2.4 / 35.0 | 0 |

### Latency sweep

| Contender | 0ms Reads/s | 0ms Server cmds/s | 0.5ms Reads/s | 0.5ms Server cmds/s | 2ms Reads/s | 2ms Server cmds/s |
|---|---:|---:|---:|---:|---:|---:|
| Plain | 164,527 | 166,527 | 124,343 | 126,344 | 62,852 | 64,853 |
| MemoryCacheTtl | 14,433,455 | 22,420 | 15,868,961 | 22,556 | 12,726,584 | 22,615 |
| HybridCache | 25,597,268 | 45,409 | 24,961,847 | 46,137 | 19,533,349 | 45,446 |
| FusionCache | 6,587,211 | 37,663 | 6,258,546 | 38,387 | 4,475,469 | 39,009 |
| NearCacheHybridCache | 527,667 | 45,970 | 525,493 | 45,935 | 444,614 | 45,701 |
| NearCache | 1,348,724 | 104,596 | 1,311,492 | 105,008 | 830,694 | 98,145 |

### Topologies

| Topology | Server version | Contender | Write mode | Reads/s | Local hit % | Stale reads | Stale entries |
|---|---|---|---|---:|---:|---:|---:|
| cluster | redis 7.4.11 | Plain | Foreign | 116,995 | n/a | 0 | n/a |
| cluster | redis 7.4.11 | NearCache | Foreign | 1,235,793 | 95.82 % | 37,293 | 0 |
| tls | redis 7.4.11 | Plain | Foreign | 44,767 | n/a | 0 | n/a |
| tls | redis 7.4.11 | NearCache | Foreign | 964,426 | 95.16 % | 28,429 | 0 |
| valkey | valkey 8.1.10 | Plain | Foreign | 172,639 | n/a | 0 | n/a |
| valkey | valkey 8.1.10 | NearCache | Foreign | 1,449,964 | 96.43 % | 37,115 | 0 |

### Chaos

| Contender | Write mode | Topology | Latency | Invalidations | Race discards | Flushes | Re-arms | Tracking keys | Stale entries after quiescence |
|---|---|---|---|---:|---:|---:|---:|---:|---:|
| NearCache | Foreign | standalone | 0ms | 1,132,766 | 4,515 | 44 | 17 | 9,991 | 0 |

### RedisNearCache internals

| Contender | Write mode | Latency | Invalidations | Invalidations/write | Race discards | Flushes | Re-arms | Tracking keys | Server used mem before | Server used mem after |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| NearCacheHybridCache | Foreign | 0ms | 578,077 | 9.62 | 276 | 0 | 0 | 249 | 10.7 MB | 9.9 MB |
| NearCacheHybridCache | Foreign | 0.5ms | 578,046 | 9.62 | 268 | 0 | 0 | 223 | 10.7 MB | 9.9 MB |
| NearCacheHybridCache | Foreign | 2ms | 569,987 | 9.48 | 362 | 0 | 0 | 509 | 10.6 MB | 10.3 MB |
| NearCacheHybridCache | Api | 0ms | 1,391,415 | 23.15 | 1,605 | 0 | 0 | 6,306 | 10.7 MB | 18.5 MB |
| NearCacheHybridCache | Api | 0.5ms | 1,369,330 | 22.79 | 1,475 | 0 | 0 | 6,524 | 10.6 MB | 18.9 MB |
| NearCacheHybridCache | Api | 2ms | 1,149,669 | 19.14 | 998 | 0 | 0 | 8,910 | 10.6 MB | 21.1 MB |
| NearCache | Foreign | 0ms | 1,145,412 | 19.06 | 4,377 | 0 | 0 | 9,987 | 10.0 MB | 17.6 MB |
| NearCache | Foreign | 0.5ms | 1,142,571 | 19.02 | 3,679 | 0 | 0 | 9,994 | 10.0 MB | 17.6 MB |
| NearCache | Foreign | 2ms | 1,094,315 | 18.20 | 1,992 | 0 | 0 | 9,984 | 9.9 MB | 17.6 MB |
| NearCache | Api | 0ms | 1,141,133 | 19.00 | 4,438 | 0 | 0 | 9,987 | 9.9 MB | 17.5 MB |
| NearCache | Api | 0.5ms | 1,140,155 | 18.95 | 3,899 | 0 | 0 | 9,981 | 9.9 MB | 17.6 MB |
| NearCache | Api | 2ms | 1,092,366 | 18.18 | 2,024 | 0 | 0 | 9,985 | 9.8 MB | 17.5 MB |

### Per-call cost — BenchmarkDotNet, 0ms

**Hit path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 135 µs | 1.0 µs | 1.000 | 3.47 KB |
| MemoryCacheTtl | String | 41.3 ns | 0.1 ns | 0.000307 | 56 B |
| HybridCache | String | 50.1 ns | 0.1 ns | 0.000372 | 56 B |
| FusionCache | String | 141.1 ns | 0.3 ns | 0.00105 | 240 B |
| NearCacheHybridCache | String | 573.1 ns | 1.1 ns | 0.00425 | 3.55 KB |
| NearCache | String | 222.5 ns | 0.8 ns | 0.00165 | 2.08 KB |
| Plain | Json | 134 µs | 1.3 µs | 1.000 | 936 B |
| MemoryCacheTtl | Json | 41.3 ns | 0.1 ns | 0.000308 | 56 B |
| HybridCache | Json | 508.6 ns | 0.9 ns | 0.0038 | 408 B |
| FusionCache | Json | 142.7 ns | 0.3 ns | 0.00107 | 240 B |
| NearCacheHybridCache | Json | 820.1 ns | 4.3 ns | 0.00612 | 1.00 KB |
| NearCache | Json | 386.8 ns | 0.7 ns | 0.00289 | 408 B |

**Miss path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 151 µs | 22 µs | 1.000 | 3.41 KB |
| MemoryCacheTtl | String | 139 µs | 6.2 µs | 0.924 | 3.56 KB |
| HybridCache | String | 292 µs | 3.5 µs | 1.934 | 6.49 KB |
| FusionCache | String | 451 µs | 19 µs | 2.990 | 12.66 KB |
| NearCacheHybridCache | String | 295 µs | 9.0 µs | 1.952 | 7.27 KB |
| NearCache | String | 143 µs | 5.5 µs | 0.949 | 4.20 KB |
| Plain | Json | 145 µs | 7.7 µs | 1.000 | 880 B |
| MemoryCacheTtl | Json | 139 µs | 2.7 µs | 0.956 | 1.01 KB |
| HybridCache | Json | 298 µs | 5.5 µs | 2.058 | 4.87 KB |
| FusionCache | Json | 446 µs | 7.9 µs | 3.081 | 9.23 KB |
| NearCacheHybridCache | Json | 298 µs | 12 µs | 2.058 | 4.33 KB |
| NearCache | Json | 142 µs | 5.4 µs | 0.980 | 1.65 KB |

### Per-call latency — Sailfish, 0ms

_p95/p99 are derived here from Sailfish's raw per-sample data (RawExecutionResults); Sailfish itself does not compute percentiles for `[SailfishMethod]` results, only mean/median/confidence intervals._

**Hit path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 233 µs | 235 µs | 333 µs | 450 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 540.0 ns | 559.4 ns | 915.0 ns | 1.5 µs | 0.002 [0.002, 0.002] | 1.0E-300 |
| HybridCache | String | 664.0 ns | 666.3 ns | 1.3 µs | 3.0 µs | 0.003 [0.003, 0.003] | 1.0E-300 |
| FusionCache | String | 1.6 µs | 1.7 µs | 2.9 µs | 5.5 µs | 0.007 [0.007, 0.007] | 1.0E-300 |
| NearCacheHybridCache | String | 3.3 µs | 3.4 µs | 6.6 µs | 14 µs | 0.014 [0.014, 0.015] | 1.0E-300 |
| NearCache | String | 1.5 µs | 1.5 µs | 2.5 µs | 3.7 µs | 0.007 [0.006, 0.007] | 1.0E-300 |
| Plain | Json | 239 µs | 260 µs | 510 µs | 670 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 581.0 ns | 599.2 ns | 915.0 ns | 1.5 µs | 0.002 [0.002, 0.002] | 1.0E-300 |
| HybridCache | Json | 3.1 µs | 3.1 µs | 5.0 µs | 7.3 µs | 0.012 [0.012, 0.012] | 1.0E-300 |
| FusionCache | Json | 1.6 µs | 1.5 µs | 3.1 µs | 6.3 µs | 0.006 [0.006, 0.006] | 1.0E-300 |
| NearCacheHybridCache | Json | 4.7 µs | 4.7 µs | 7.9 µs | 13 µs | 0.018 [0.018, 0.018] | 1.0E-300 |
| NearCache | Json | 2.5 µs | 2.7 µs | 4.7 µs | 8.3 µs | 0.011 [0.01, 0.011] | 1.0E-300 |

**Miss path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 408 µs | 435 µs | 830 µs | 1,247 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 288 µs | 373 µs | 722 µs | 824 µs | 0.857 [0.825, 0.890] | 1.0E-300 |
| HybridCache | String | 451 µs | 465 µs | 806 µs | 1,919 µs | 1.069 [1.041, 1.097] | 1.0E-300 |
| FusionCache | String | 671 µs | 672 µs | 841 µs | 1,005 µs | 1.544 [1.507, 1.582] | 1.0E-300 |
| NearCacheHybridCache | String | 500 µs | 530 µs | 896 µs | 2,156 µs | 1.217 [1.185, 1.251] | 1.0E-300 |
| NearCache | String | 401 µs | 414 µs | 720 µs | 1,019 µs | 0.951 [0.921, 0.982] | 0.093 |
| Plain | Json | 227 µs | 228 µs | 331 µs | 495 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 232 µs | 251 µs | 494 µs | 629 µs | 1.101 [1.088, 1.115] | 1.0E-300 |
| HybridCache | Json | 442 µs | 445 µs | 657 µs | 876 µs | 1.951 [1.941, 1.960] | 1.0E-300 |
| FusionCache | Json | 674 µs | 675 µs | 914 µs | 1,383 µs | 2.957 [2.943, 2.971] | 1.0E-300 |
| NearCacheHybridCache | Json | 438 µs | 438 µs | 588 µs | 676 µs | 1.918 [1.907, 1.930] | 1.0E-300 |
| NearCache | Json | 234 µs | 241 µs | 439 µs | 526 µs | 1.058 [1.051, 1.065] | 1.0E-300 |

### Cross-check: BenchmarkDotNet vs Sailfish — 0ms

| Path | Contender | Payload | BDN median | Sailfish median | Difference |
|---|---|---|---:|---:|---:|
| Hit | Plain | String | 135 µs | 233 µs | 73.2 % |
| Hit | MemoryCacheTtl | String | 41.3 ns | 540.0 ns | 1,206.2 % |
| Hit | HybridCache | String | 50.1 ns | 664.0 ns | 1,224.1 % |
| Hit | FusionCache | String | 141.2 ns | 1.6 µs | 1,049.4 % |
| Hit | NearCacheHybridCache | String | 573.0 ns | 3.3 µs | 481.3 % |
| Hit | NearCache | String | 222.7 ns | 1.5 µs | 591.0 % |
| Hit | Plain | Json | 134 µs | 239 µs | 77.8 % |
| Hit | MemoryCacheTtl | Json | 41.3 ns | 581.0 ns | 1,306.4 % |
| Hit | HybridCache | Json | 508.5 ns | 3.1 µs | 514.1 % |
| Hit | FusionCache | Json | 142.7 ns | 1.6 µs | 1,007.9 % |
| Hit | NearCacheHybridCache | Json | 819.0 ns | 4.7 µs | 474.6 % |
| Hit | NearCache | Json | 386.8 ns | 2.5 µs | 556.7 % |
| Miss | Plain | String | 148 µs | 408 µs | 176.2 % |
| Miss | MemoryCacheTtl | String | 139 µs | 288 µs | 107.8 % |
| Miss | HybridCache | String | 292 µs | 451 µs | 54.7 % |
| Miss | FusionCache | String | 449 µs | 671 µs | 49.7 % |
| Miss | NearCacheHybridCache | String | 294 µs | 500 µs | 69.7 % |
| Miss | NearCache | String | 142 µs | 401 µs | 182.0 % |
| Miss | Plain | Json | 143 µs | 227 µs | 58.6 % |
| Miss | MemoryCacheTtl | Json | 139 µs | 232 µs | 67.2 % |
| Miss | HybridCache | Json | 299 µs | 442 µs | 47.9 % |
| Miss | FusionCache | Json | 445 µs | 674 µs | 51.5 % |
| Miss | NearCacheHybridCache | Json | 299 µs | 438 µs | 46.4 % |
| Miss | NearCache | Json | 143 µs | 234 µs | 64.1 % |

### Per-call cost — BenchmarkDotNet, 0.5ms

**Hit path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 1,049 µs | 36 µs | 1.000 | 3.47 KB |
| MemoryCacheTtl | String | 41.3 ns | 0.1 ns | 3.93E-05 | 56 B |
| HybridCache | String | 50.7 ns | 0.2 ns | 4.84E-05 | 56 B |
| FusionCache | String | 140.0 ns | 0.3 ns | 0.000133 | 240 B |
| NearCacheHybridCache | String | 576.2 ns | 1.3 ns | 0.000549 | 3.55 KB |
| NearCache | String | 219.2 ns | 0.9 ns | 0.000209 | 2.08 KB |
| Plain | Json | 1,047 µs | 28 µs | 1.000 | 937 B |
| MemoryCacheTtl | Json | 40.8 ns | 0.1 ns | 3.9E-05 | 56 B |
| HybridCache | Json | 510.9 ns | 0.6 ns | 0.000488 | 408 B |
| FusionCache | Json | 140.3 ns | 0.3 ns | 0.000134 | 240 B |
| NearCacheHybridCache | Json | 811.1 ns | 1.3 ns | 0.000775 | 1.00 KB |
| NearCache | Json | 385.1 ns | 0.4 ns | 0.000368 | 408 B |

**Miss path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 1,081 µs | 93 µs | 1.000 | 3.41 KB |
| MemoryCacheTtl | String | 1,110 µs | 154 µs | 1.027 | 3.56 KB |
| HybridCache | String | 2,147 µs | 230 µs | 1.986 | 6.40 KB |
| FusionCache | String | 3,163 µs | 138 µs | 2.926 | 12.62 KB |
| NearCacheHybridCache | String | 2,074 µs | 69 µs | 1.919 | 7.17 KB |
| NearCache | String | 1,123 µs | 145 µs | 1.039 | 4.20 KB |
| Plain | Json | 1,071 µs | 96 µs | 1.000 | 880 B |
| MemoryCacheTtl | Json | 1,123 µs | 104 µs | 1.049 | 1.01 KB |
| HybridCache | Json | 2,113 µs | 92 µs | 1.973 | 4.88 KB |
| FusionCache | Json | 3,228 µs | 276 µs | 3.014 | 9.23 KB |
| NearCacheHybridCache | Json | 2,176 µs | 146 µs | 2.032 | 4.34 KB |
| NearCache | Json | 1,061 µs | 120 µs | 0.990 | 1.65 KB |

### Per-call latency — Sailfish, 0.5ms

_p95/p99 are derived here from Sailfish's raw per-sample data (RawExecutionResults); Sailfish itself does not compute percentiles for `[SailfishMethod]` results, only mean/median/confidence intervals._

**Hit path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 1,163 µs | 1,153 µs | 1,432 µs | 1,780 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 665.0 ns | 702.2 ns | 1.4 µs | 2.5 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| HybridCache | String | 706.0 ns | 720.3 ns | 1.7 µs | 3.1 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| FusionCache | String | 1.7 µs | 2.0 µs | 5.5 µs | 11 µs | 0.002 [0.002, 0.002] | 1.0E-300 |
| NearCacheHybridCache | String | 4.0 µs | 4.0 µs | 7.6 µs | 12 µs | 0.003 [0.003, 0.004] | 1.0E-300 |
| NearCache | String | 1.1 µs | 1.2 µs | 2.2 µs | 3.4 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| Plain | Json | 922 µs | 949 µs | 1,340 µs | 2,069 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 664.0 ns | 783.9 ns | 2.5 µs | 5.6 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| HybridCache | Json | 3.1 µs | 3.1 µs | 8.8 µs | 13 µs | 0.003 [0.003, 0.003] | 1.0E-300 |
| FusionCache | Json | 873.0 ns | 1.0 µs | 4.8 µs | 8.8 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| NearCacheHybridCache | Json | 5.0 µs | 5.6 µs | 16 µs | 30 µs | 0.006 [0.006, 0.006] | 1.0E-300 |
| NearCache | Json | 3.0 µs | 3.7 µs | 13 µs | 18 µs | 0.004 [0.004, 0.004] | 1.0E-300 |

**Miss path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 950 µs | 1,000 µs | 1,376 µs | 1,595 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 1,100 µs | 1,107 µs | 1,572 µs | 1,993 µs | 1.107 [1.089, 1.125] | 1.0E-300 |
| HybridCache | String | 1,775 µs | 1,843 µs | 2,419 µs | 2,968 µs | 1.843 [1.819, 1.868] | 1.0E-300 |
| FusionCache | String | 2,948 µs | 3,029 µs | 3,942 µs | 4,850 µs | 3.029 [2.987, 3.072] | 1.0E-300 |
| NearCacheHybridCache | String | 1,844 µs | 1,949 µs | 2,845 µs | 3,534 µs | 1.949 [1.921, 1.978] | 1.0E-300 |
| NearCache | String | 1,154 µs | 1,171 µs | 1,739 µs | 2,734 µs | 1.171 [1.152, 1.191] | 1.0E-300 |
| Plain | Json | 922 µs | 961 µs | 1,251 µs | 1,521 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 993 µs | 1,039 µs | 1,561 µs | 2,119 µs | 1.082 [1.067, 1.096] | 1.0E-300 |
| HybridCache | Json | 1,863 µs | 1,912 µs | 2,569 µs | 3,190 µs | 1.990 [1.969, 2.011] | 1.0E-300 |
| FusionCache | Json | 2,726 µs | 2,829 µs | 3,746 µs | 4,602 µs | 2.944 [2.913, 2.975] | 1.0E-300 |
| NearCacheHybridCache | Json | 1,943 µs | 2,008 µs | 2,714 µs | 3,257 µs | 2.090 [2.065, 2.115] | 1.0E-300 |
| NearCache | Json | 1,011 µs | 1,046 µs | 1,509 µs | 1,993 µs | 1.089 [1.074, 1.104] | 1.0E-300 |

### Cross-check: BenchmarkDotNet vs Sailfish — 0.5ms

| Path | Contender | Payload | BDN median | Sailfish median | Difference |
|---|---|---|---:|---:|---:|
| Hit | Plain | String | 1,043 µs | 1,163 µs | 11.5 % |
| Hit | MemoryCacheTtl | String | 41.3 ns | 665.0 ns | 1,511.7 % |
| Hit | HybridCache | String | 50.8 ns | 706.0 ns | 1,290.9 % |
| Hit | FusionCache | String | 140.0 ns | 1.7 µs | 1,088.5 % |
| Hit | NearCacheHybridCache | String | 576.2 ns | 4.0 µs | 586.7 % |
| Hit | NearCache | String | 219.4 ns | 1.1 µs | 411.9 % |
| Hit | Plain | Json | 1,048 µs | 922 µs | -12.0 % |
| Hit | MemoryCacheTtl | Json | 40.8 ns | 664.0 ns | 1,525.8 % |
| Hit | HybridCache | Json | 510.9 ns | 3.1 µs | 503.1 % |
| Hit | FusionCache | Json | 140.3 ns | 873.0 ns | 522.4 % |
| Hit | NearCacheHybridCache | Json | 811.1 ns | 5.0 µs | 516.2 % |
| Hit | NearCache | Json | 385.1 ns | 3.0 µs | 667.5 % |
| Miss | Plain | String | 1,107 µs | 950 µs | -14.1 % |
| Miss | MemoryCacheTtl | String | 1,089 µs | 1,100 µs | 1.1 % |
| Miss | HybridCache | String | 2,101 µs | 1,775 µs | -15.5 % |
| Miss | FusionCache | String | 3,187 µs | 2,948 µs | -7.5 % |
| Miss | NearCacheHybridCache | String | 2,062 µs | 1,844 µs | -10.6 % |
| Miss | NearCache | String | 1,116 µs | 1,154 µs | 3.4 % |
| Miss | Plain | Json | 1,057 µs | 922 µs | -12.7 % |
| Miss | MemoryCacheTtl | Json | 1,122 µs | 993 µs | -11.5 % |
| Miss | HybridCache | Json | 2,106 µs | 1,863 µs | -11.5 % |
| Miss | FusionCache | Json | 3,167 µs | 2,726 µs | -13.9 % |
| Miss | NearCacheHybridCache | Json | 2,200 µs | 1,943 µs | -11.7 % |
| Miss | NearCache | Json | 1,023 µs | 1,011 µs | -1.2 % |

### Per-call cost — BenchmarkDotNet, 2ms

**Hit path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 3,256 µs | 55 µs | 1.000 | 3.47 KB |
| MemoryCacheTtl | String | 41.2 ns | 0.1 ns | 1.27E-05 | 56 B |
| HybridCache | String | 50.5 ns | 0.1 ns | 1.55E-05 | 56 B |
| FusionCache | String | 140.4 ns | 0.3 ns | 4.31E-05 | 240 B |
| NearCacheHybridCache | String | 570.9 ns | 2.4 ns | 0.000175 | 3.55 KB |
| NearCache | String | 225.7 ns | 1.0 ns | 6.93E-05 | 2.08 KB |
| Plain | Json | 3,255 µs | 53 µs | 1.000 | 937 B |
| MemoryCacheTtl | Json | 40.9 ns | 0.1 ns | 1.26E-05 | 56 B |
| HybridCache | Json | 509.8 ns | 0.4 ns | 0.000157 | 408 B |
| FusionCache | Json | 141.4 ns | 0.5 ns | 4.35E-05 | 240 B |
| NearCacheHybridCache | Json | 816.6 ns | 0.8 ns | 0.000251 | 1.00 KB |
| NearCache | Json | 386.4 ns | 0.5 ns | 0.000119 | 408 B |

**Miss path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 3,134 µs | 92 µs | 1.000 | 3.41 KB |
| MemoryCacheTtl | String | 3,206 µs | 212 µs | 1.023 | 3.56 KB |
| HybridCache | String | 6,394 µs | 291 µs | 2.040 | 6.36 KB |
| FusionCache | String | 9,578 µs | 282 µs | 3.056 | 12.62 KB |
| NearCacheHybridCache | String | 6,355 µs | 227 µs | 2.028 | 7.18 KB |
| NearCache | String | 3,172 µs | 185 µs | 1.012 | 4.20 KB |
| Plain | Json | 3,072 µs | 101 µs | 1.000 | 880 B |
| MemoryCacheTtl | Json | 3,145 µs | 173 µs | 1.024 | 1.01 KB |
| HybridCache | Json | 6,557 µs | 394 µs | 2.134 | 4.73 KB |
| FusionCache | Json | 9,524 µs | 299 µs | 3.100 | 9.19 KB |
| NearCacheHybridCache | Json | 6,308 µs | 233 µs | 2.053 | 4.23 KB |
| NearCache | Json | 3,196 µs | 179 µs | 1.040 | 1.65 KB |

### Per-call latency — Sailfish, 2ms

_p95/p99 are derived here from Sailfish's raw per-sample data (RawExecutionResults); Sailfish itself does not compute percentiles for `[SailfishMethod]` results, only mean/median/confidence intervals._

**Hit path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 2,947 µs | 2,955 µs | 3,403 µs | 4,126 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 706.0 ns | 714.7 ns | 1.9 µs | 3.9 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |
| HybridCache | String | 957.0 ns | 1.1 µs | 3.4 µs | 7.9 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |
| FusionCache | String | 2.6 µs | 3.2 µs | 9.6 µs | 18 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| NearCacheHybridCache | String | 4.0 µs | 4.4 µs | 13 µs | 22 µs | 0.001 [0.001, 0.002] | 1.0E-300 |
| NearCache | String | 1.3 µs | 1.4 µs | 3.1 µs | 5.4 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |
| Plain | Json | 2,899 µs | 2,905 µs | 3,467 µs | 4,077 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 498.0 ns | 497.3 ns | 790.0 ns | 1.3 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |
| HybridCache | Json | 1.7 µs | 1.8 µs | 3.6 µs | 5.1 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| FusionCache | Json | 1.5 µs | 1.5 µs | 2.2 µs | 4.4 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| NearCacheHybridCache | Json | 3.9 µs | 4.3 µs | 11 µs | 31 µs | 0.001 [0.001, 0.002] | 1.0E-300 |
| NearCache | Json | 1.2 µs | 1.3 µs | 2.5 µs | 4.6 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |

**Miss path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 2,885 µs | 2,883 µs | 3,369 µs | 3,929 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 2,959 µs | 2,965 µs | 3,525 µs | 4,109 µs | 1.028 [1.022, 1.035] | 1.0E-300 |
| HybridCache | String | 5,706 µs | 5,710 µs | 6,306 µs | 7,624 µs | 1.980 [1.970, 1.990] | 1.0E-300 |
| FusionCache | String | 8,584 µs | 8,615 µs | 9,588 µs | 12,343 µs | 2.988 [2.972, 3.004] | 1.0E-300 |
| NearCacheHybridCache | String | 5,664 µs | 5,691 µs | 6,316 µs | 8,765 µs | 1.974 [1.963, 1.984] | 1.0E-300 |
| NearCache | String | 2,911 µs | 2,908 µs | 3,313 µs | 3,970 µs | 1.009 [1.003, 1.015] | 0.002 |
| Plain | Json | 2,885 µs | 2,888 µs | 3,328 µs | 4,209 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 2,939 µs | 2,947 µs | 3,525 µs | 4,187 µs | 1.020 [1.014, 1.027] | 7.3E-009 |
| HybridCache | Json | 5,729 µs | 5,751 µs | 6,534 µs | 8,111 µs | 1.991 [1.981, 2.002] | 1.0E-300 |
| FusionCache | Json | 8,580 µs | 8,629 µs | 9,810 µs | 14,889 µs | 2.988 [2.972, 3.004] | 1.0E-300 |
| NearCacheHybridCache | Json | 5,592 µs | 5,608 µs | 6,234 µs | 7,363 µs | 1.942 [1.932, 1.953] | 1.0E-300 |
| NearCache | Json | 2,948 µs | 2,968 µs | 3,553 µs | 4,333 µs | 1.028 [1.021, 1.034] | 1.0E-300 |

### Cross-check: BenchmarkDotNet vs Sailfish — 2ms

| Path | Contender | Payload | BDN median | Sailfish median | Difference |
|---|---|---|---:|---:|---:|
| Hit | Plain | String | 3,250 µs | 2,947 µs | -9.3 % |
| Hit | MemoryCacheTtl | String | 41.2 ns | 706.0 ns | 1,613.7 % |
| Hit | HybridCache | String | 50.5 ns | 957.0 ns | 1,796.0 % |
| Hit | FusionCache | String | 140.4 ns | 2.6 µs | 1,767.9 % |
| Hit | NearCacheHybridCache | String | 570.6 ns | 4.0 µs | 600.7 % |
| Hit | NearCache | String | 225.5 ns | 1.3 µs | 472.1 % |
| Hit | Plain | Json | 3,265 µs | 2,899 µs | -11.2 % |
| Hit | MemoryCacheTtl | Json | 40.9 ns | 498.0 ns | 1,117.6 % |
| Hit | HybridCache | Json | 509.8 ns | 1.7 µs | 235.0 % |
| Hit | FusionCache | Json | 141.4 ns | 1.5 µs | 928.4 % |
| Hit | NearCacheHybridCache | Json | 816.5 ns | 3.9 µs | 379.5 % |
| Hit | NearCache | Json | 386.5 ns | 1.2 µs | 222.9 % |
| Miss | Plain | String | 3,149 µs | 2,885 µs | -8.4 % |
| Miss | MemoryCacheTtl | String | 3,191 µs | 2,959 µs | -7.3 % |
| Miss | HybridCache | String | 6,444 µs | 5,706 µs | -11.5 % |
| Miss | FusionCache | String | 9,595 µs | 8,584 µs | -10.5 % |
| Miss | NearCacheHybridCache | String | 6,356 µs | 5,664 µs | -10.9 % |
| Miss | NearCache | String | 3,177 µs | 2,911 µs | -8.4 % |
| Miss | Plain | Json | 3,067 µs | 2,885 µs | -5.9 % |
| Miss | MemoryCacheTtl | Json | 3,144 µs | 2,939 µs | -6.5 % |
| Miss | HybridCache | Json | 6,558 µs | 5,729 µs | -12.6 % |
| Miss | FusionCache | Json | 9,497 µs | 8,580 µs | -9.7 % |
| Miss | NearCacheHybridCache | Json | 6,321 µs | 5,592 µs | -11.5 % |
| Miss | NearCache | Json | 3,192 µs | 2,948 µs | -7.6 % |

<!-- END GENERATED RESULTS -->

## Zero-traffic demo

`--demo` builds a near cache against the standalone container on `localhost:6379`, sets a key from a second, unrelated
multiplexer, reads it once through the near cache (a miss that arms tracking), then reads it 1,000 more times and checks
the server's `cmdstat_get` counter. It then writes the key from the other multiplexer and times how long the local copy
takes to disappear.

```
1000 reads of 'demo:near-cache:zero-traffic' through the near cache (all should be L1 hits):
  cmdstat_get calls before : 263325
  cmdstat_get calls after  : 263325
  delta (Redis GETs issued): 0  (expected 0)

Write-to-eviction latency  : 1688.0 µs

Statistics: hits=1000 misses=1 invalidations=1 flushes=0 rearms=0 raceDiscards=0
```

The counters are cumulative since the server started; the delta is what matters. The write-to-eviction time is one
sample of the invalidation round trip: write, Redis, `__redis__:invalidate` push, subscriber, L1 evicted.

## Layout

| Path | What |
|---|---|
| `bench/run-matrix.sh` | The whole matrix and the report. |
| `bench/bench-up.sh`, `bench-latency.sh`, `bench-down.sh` | The benchmark server container and its injected latency. |
| `bench/RedisNearCache.Bench/LoadTest.cs` | `--load`: instances, readers, writers, staleness probe, audit, JSON. |
| `bench/RedisNearCache.Bench/Contenders/` | `IContender` and one implementation per contender; `ContenderRedis` builds every connection. |
| `bench/RedisNearCache.Bench/Micro/` | BenchmarkDotNet hit/miss benchmarks, miss key pool, job sizing. |
| `bench/RedisNearCache.Bench.Sailfish/` | Sailfish comparisons (own project, links the contender code). |
| `bench/RedisNearCache.Bench.Report/` | Renders `summary.md` from a results directory. |
| `bench/RedisNearCache.Bench/Results/LoadResult.cs` | The load test's JSON schema. |
