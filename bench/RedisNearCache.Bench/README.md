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
| Reads/s | 164,427 | 14.51 M | 25.99 M | 6.42 M | 2.20 M |
| Server commands/s | 166,427 | 22,322 | 45,448 | 37,315 | 110,426 |
| Reads served stale | 0 | 83.1 % | 84.8 % | 85.0 % | 0.33 % |
| Stalest read | – | 10.0 s | 10.0 s | 10.0 s | 105 ms |
| Stale local entries after writes stop | – | 792 | 605 | 16,701 | 0 |
| Reads served stale, writes through the library's API | 0 | 82.8 % | 83.2 % | 2.3 % | 0.41 % |
<!-- /HEADLINE -->

What that says:

- **Only RedisNearCache stays fresh when someone else writes.** The TTL caches (hand-rolled `IMemoryCache`,
  `HybridCache`, FusionCache) served 83–85 % of reads stale, up to the full 10 s TTL, and still held stale entries
  (605 to 16,701 across 20 instances) after writes stopped. FusionCache's backplane only carries writes made through FusionCache. RedisNearCache
  served 0.33 % of reads stale, the stalest 105 ms old, and held 0 stale entries after writes stopped.
- **It costs server traffic that a TTL cache doesn't spend.** Every write invalidates the key on every instance that
  tracks it (19 instances per write here), and each of those re-reads it on the next access (`GET` plus a pipelined
  `PTTL`). That is 110,426 commands/s against 22,322 for `IMemoryCache` with a 10 s TTL. The rate follows writes, not
  reads: at 0 ms it is 34 % fewer commands than plain StackExchange.Redis while serving 13x the reads, but at 2 ms
  injected latency, where plain StackExchange.Redis readers slow to 64,568 commands/s, RedisNearCache sends 98,813
  (53 % more) while serving 14x the reads.
- **In-process TTL caches read faster.** `IMemoryCache` and `HybridCache` hand back a stored object reference (about
  40–50 ns per hit); RedisNearCache decodes the value from its stored bytes on every hit (169 ns for a 1 KB string,
  see [Findings](#findings)). Under 160 spinning readers that shows up as 2.20 M reads/s against 6.4 M (FusionCache)
  to 26.0 M (`HybridCache`).
- **A miss costs about what a plain `GET` costs.** RedisNearCache's cold read (a `GET` with a pipelined `PTTL`) had a
  ratio of 0.93–1.02x a plain StackExchange.Redis `GET` at 0.5 and 2 ms injected latency (Sailfish, 1,000 samples each;
  0.71–1.34x at 0 ms, where both it and the plain `GET` had heavy tails);
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

- **RedisNearCache's hit path took a lock on every read, until this run.** `CachingEnabled` evaluated
  `ConcurrentDictionary.IsEmpty` on each L1 hit, and on an empty dictionary (the normal state) that acquires all of its
  locks. It now compares a volatile counter to zero (see CHANGELOG). Against the previous full run of this matrix, the
  20-instance load test at 0 ms went from 1.35 M to 2.20 M reads/s (local hit 96.2 % → 97.5 %) and the BenchmarkDotNet
  hit from 223 ns to 169 ns; the 4 × 2-reader load test on `localhost:6379` went from 2.43 M to 7.29 M reads/s (local
  hit p50 2.3 µs → 0.3 µs, p99 3.9 µs → 1.9 µs).
- **Stale reads rose when the lock went, under 160 spinning readers.** At 0 ms, RedisNearCache's stale-read rate went
  from 0.07 % to 0.33 % (writes through its API: 0.13 % → 0.41 %), staleness p50 from 0.3 ms to 5.9 ms and the stalest
  read from 76 ms to 105 ms. Stale local entries after quiescence stayed at 0 everywhere. The likely cause is CPU, not
  coherence: readers that used to park on the lock now keep all 14 cores busy, so the invalidation handler waits
  longer to run, and more reads land in the gap between a write's acknowledgement and that handler. Consistent with
  that, the 4 × 2-reader run, which leaves cores free, saw its stale-read rate fall (0.023 % → 0.014 %). Not proven
  beyond that comparison.
- **Stale reads are not zero for RedisNearCache, by design.** Redis acknowledges the writer and pushes the invalidation
  to the other connections at the same moment; a read on another instance between the acknowledgement and that
  instance handling the push is served the old value. Every stale read observed was of that kind: the invalidation for
  the newer version had not been handled when the read started. The in-flight guard (a read whose key is invalidated
  while its reply is in flight never populates L1) held throughout: 0 stale entries in every audit, including chaos.
- **FusionCache with writes through its API** served 2.3–2.8 % of reads stale, p99 age 140–240 ms, with a rare maximum
  of up to 9.3 s (the duration is 10 s), i.e. an occasional instance keeping an old value after the backplane message.
- **The chaos run** (both connections of 5 of 20 instances killed at T/2) raised 37 read errors from the killed
  connections, 42 flushes and 16 re-arms, and the audit still found 0 stale entries.
- **`NearCacheHybridCache` with writes through HybridCache** showed a similar rare long tail, at 0.5 ms latency in this run
  (p99 3.9 s, max 9.5 s over 4,256 stale reads; the previous run showed it at 2 ms instead, where this one had p99
  2.6 ms). The tail is explained by a cache-aside lost update in HybridCache, not an invalidation gap: a reader's
  factory loads the old source value, the writer then updates the source and awaits `SetAsync`, and the reader's
  background write of its factory result lands after that, overwriting the newer entry in Redis until its 10 s
  expiry. After the initial fill (about a third of the ~900 source loads/s averaged over each run) the factory runs
  almost only on cold keys whose entry has just expired, and a cold key is not written again for ~25 s on average, so
  one such race serves the old value for nearly the whole expiry; on a hot key it lasts only until the next write
  (~300 ms). A run sees a handful of such cold-key races at most, which is why the tail comes and goes between runs
  and latencies. Reproduced deterministically by `tests/RedisNearCache.Tests/HybridCache/HybridCacheWriteBackRaceTests.cs`;
  see the `AddRedisNearCacheHybridCache` documentation for why the adapter does not refuse the write and how to bound it.
- **Allocations on a hit:** RedisNearCache keeps values as bytes and decodes on every hit (2.08 KB for a 1 KB string,
  408 B for the record), where `IMemoryCache` returns the stored object (56 B, all BenchmarkDotNet's).

## Full results

<!-- BEGIN GENERATED RESULTS -->

### Run environment

```
date: 2026-09-15T01:35:46Z
git rev-parse HEAD: d67ea9d1485fb8d6f1cc52bf7cbde6cbb97a178d
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
| Plain | Foreign | 164,427 | n/a | n/a | 922 / 2,114 | 166,427 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| Plain | Api | 166,290 | n/a | n/a | 922 / 2,114 | 168,291 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| MemoryCacheTtl | Foreign | 14,505,927 | 99.86 % | 0.12 / 5.5 | 1,177 / 7,891 | 22,322 | 0.14 % | 0 | 362,157,983 (83.09 %) | 4,944.3 / 9,789.4 / 10,000.0 | 792 |
| MemoryCacheTtl | Api | 13,501,567 | 99.85 % | 0.12 / 5.8 | 1,121 / 9,134 | 22,679 | 0.15 % | 0 | 337,315,985 (82.84 %) | 5,451.1 / 9,789.4 / 10,001.1 | 2,089 |
| HybridCache | Foreign | 25,987,205 | 99.92 % | 0.12 / 3.7 | 1,298 / 10,071 | 45,448 | 0.00 % | 0 | 663,067,550 (84.83 %) | 5,723.6 / 9,789.4 / 9,996.1 | 605 |
| HybridCache | Api | 24,635,238 | 99.90 % | 0.12 / 0.71 | 1,298 / 14,171 | 56,988 | 0.01 % | 0 | 615,483,366 (83.21 %) | 5,451.1 / 9,789.4 / 9,995.1 | 18,162 |
| FusionCache | Foreign | 6,416,919 | 99.69 % | 0.34 / 4.7 | 1,502 / 37,599 | 37,315 | 0.02 % | 0 | 163,922,142 (85.01 %) | 6,625.8 / 9,789.4 / 9,999.5 | 16,701 |
| FusionCache | Api | 1,496,726 | 98.79 % | 0.38 / 3.7 | 2,447 / 74,443 | 63,904 | 0.07 % | 0 | 1,047,070 (2.32 %) | 8.7 / 140.4 / 4,346.3 | 0 |
| NearCacheHybridCache | Foreign | 555,156 | n/a | n/a | 63 / 4,614 | 45,996 | 0.19 % | 0 | 13,901,422 (83.37 %) | 5,451.1 / 9,789.4 / 9,996.4 | n/a |
| NearCacheHybridCache | Api | 372,638 | n/a | n/a | 30 / 5,086 | 108,415 | 0.25 % | 0 | 5,600 (0.05 %) | 0.3 / 217.8 / 393.7 | n/a |
| NearCache | Foreign | 2,198,037 | 97.53 % | 0.41 / 2.0 | 2,219 / 9,591 | 110,426 | 2.47 % | 0 | 216,543 (0.33 %) | 5.9 / 86.2 / 104.7 | 0 |
| NearCache | Api | 2,022,827 | 97.38 % | 0.46 / 2.1 | 2,219 / 10,574 | 108,160 | 2.62 % | 0 | 250,378 (0.41 %) | 2.3 / 67.5 / 112.1 | 0 |

### Contender comparison — standalone, 0.5ms

| Contender | Write mode | Reads/s | Local hit % | Local read p50 / p99 (µs) | Remote read p50 / p99 (µs) | Server cmds/s | Source loads / read | Errors | Stale reads (count, %) | Staleness age p50 / p99 / max (ms) | Stale local entries after quiescence |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Plain | Foreign | 124,616 | n/a | n/a | 1,236 / 2,330 | 126,617 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| Plain | Api | 125,961 | n/a | n/a | 1,236 / 2,330 | 127,961 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| MemoryCacheTtl | Foreign | 12,966,273 | 99.84 % | 0.12 / 5.8 | 1,502 / 7,891 | 22,585 | 0.16 % | 0 | 322,820,953 (82.82 %) | 5,191.5 / 9,789.4 / 10,000.3 | 9,035 |
| MemoryCacheTtl | Api | 12,816,770 | 99.84 % | 0.12 / 5.8 | 1,431 / 9,591 | 22,437 | 0.16 % | 0 | 319,731,388 (83.05 %) | 5,191.5 / 9,789.4 / 10,000.5 | 9,218 |
| HybridCache | Foreign | 20,296,958 | 99.90 % | 0.12 / 5.2 | 1,502 / 9,591 | 45,321 | 0.01 % | 0 | 517,371,055 (84.85 %) | 5,723.6 / 9,789.4 / 9,999.5 | 65 |
| HybridCache | Api | 23,279,172 | 99.89 % | 0.12 / 2.9 | 1,656 / 12,853 | 57,052 | 0.01 % | 0 | 580,747,622 (83.05 %) | 5,451.1 / 9,789.4 / 9,993.2 | 17,574 |
| FusionCache | Foreign | 5,954,832 | 99.66 % | 0.34 / 5.2 | 1,917 / 35,808 | 38,483 | 0.02 % | 0 | 152,003,697 (84.92 %) | 6,957.1 / 9,789.4 / 9,997.3 | 10,743 |
| FusionCache | Api | 1,482,154 | 98.64 % | 0.41 / 4.1 | 2,697 / 52,905 | 68,357 | 0.07 % | 0 | 1,039,397 (2.33 %) | 8.3 / 154.8 / 9,275.9 | 0 |
| NearCacheHybridCache | Foreign | 523,903 | n/a | n/a | 63 / 4,844 | 45,936 | 0.21 % | 0 | 13,134,842 (83.46 %) | 5,451.1 / 9,789.4 / 9,994.0 | n/a |
| NearCacheHybridCache | Api | 347,477 | n/a | n/a | 27 / 4,844 | 107,138 | 0.26 % | 0 | 4,256 (0.04 %) | 0.2 / 3,874.0 / 9,489.1 | n/a |
| NearCache | Foreign | 2,051,488 | 97.38 % | 0.46 / 2.1 | 2,330 / 7,515 | 109,657 | 2.62 % | 0 | 101,272 (0.16 %) | 0.7 / 50.4 / 82.4 | 0 |
| NearCache | Api | 2,014,309 | 97.34 % | 0.41 / 2.1 | 2,219 / 8,285 | 109,049 | 2.66 % | 0 | 180,385 (0.30 %) | 1.4 / 61.2 / 97.1 | 0 |

### Contender comparison — standalone, 2ms

| Contender | Write mode | Reads/s | Local hit % | Local read p50 / p99 (µs) | Remote read p50 / p99 (µs) | Server cmds/s | Source loads / read | Errors | Stale reads (count, %) | Staleness age p50 / p99 / max (ms) | Stale local entries after quiescence |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Plain | Foreign | 62,567 | n/a | n/a | 2,569 / 3,279 | 64,568 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| Plain | Api | 62,537 | n/a | n/a | 2,569 / 3,443 | 64,538 | 100.00 % | 0 | 0 (0.00 %) | n/a | n/a |
| MemoryCacheTtl | Foreign | 14,073,907 | 99.85 % | 0.12 / 5.2 | 2,697 / 9,134 | 22,736 | 0.15 % | 0 | 354,166,141 (83.34 %) | 5,723.6 / 9,789.4 / 10,011.3 | 24,399 |
| MemoryCacheTtl | Api | 12,746,970 | 99.84 % | 0.12 / 5.5 | 2,697 / 9,591 | 22,807 | 0.16 % | 0 | 321,227,002 (83.24 %) | 6,009.8 / 9,789.4 / 10,004.7 | 25,966 |
| HybridCache | Foreign | 19,559,160 | 99.90 % | 0.12 / 3.9 | 2,697 / 10,071 | 45,284 | 0.01 % | 0 | 500,427,927 (85.15 %) | 6,625.8 / 9,789.4 / 9,995.6 | 124 |
| HybridCache | Api | 19,264,634 | 99.87 % | 0.12 / 0.67 | 2,832 / 14,171 | 56,106 | 0.01 % | 0 | 481,646,095 (83.18 %) | 6,310.3 / 9,789.4 / 9,988.9 | 16,144 |
| FusionCache | Foreign | 4,219,258 | 99.52 % | 0.34 / 5.5 | 2,974 / 52,905 | 37,785 | 0.03 % | 0 | 108,169,681 (85.29 %) | 7,670.2 / 9,789.4 / 10,003.3 | 27,190 |
| FusionCache | Api | 1,239,406 | 98.48 % | 0.41 / 5.0 | 3,796 / 55,551 | 64,150 | 0.08 % | 0 | 1,042,010 (2.79 %) | 8.3 / 240.1 / 637.6 | 1 |
| NearCacheHybridCache | Foreign | 476,399 | n/a | n/a | 49 / 5,341 | 45,819 | 0.23 % | 0 | 11,982,072 (83.70 %) | 5,723.6 / 9,789.4 / 9,995.0 | n/a |
| NearCacheHybridCache | Api | 169,113 | n/a | n/a | 32 / 6,183 | 92,445 | 0.53 % | 0 | 1,524 (0.03 %) | 0.1 / 2.6 / 14.3 | n/a |
| NearCache | Foreign | 879,939 | 94.50 % | 0.46 / 2.0 | 2,974 / 5,608 | 98,813 | 5.50 % | 0 | 15,406 (0.06 %) | 0.1 / 7.2 / 69.1 | 0 |
| NearCache | Api | 927,073 | 94.75 % | 0.46 / 2.0 | 2,974 / 5,608 | 99,348 | 5.25 % | 0 | 19,006 (0.07 %) | 0.1 / 2.3 / 30.0 | 0 |

### Latency sweep

| Contender | 0ms Reads/s | 0ms Server cmds/s | 0.5ms Reads/s | 0.5ms Server cmds/s | 2ms Reads/s | 2ms Server cmds/s |
|---|---:|---:|---:|---:|---:|---:|
| Plain | 164,427 | 166,427 | 124,616 | 126,617 | 62,567 | 64,568 |
| MemoryCacheTtl | 14,505,927 | 22,322 | 12,966,273 | 22,585 | 14,073,907 | 22,736 |
| HybridCache | 25,987,205 | 45,448 | 20,296,958 | 45,321 | 19,559,160 | 45,284 |
| FusionCache | 6,416,919 | 37,315 | 5,954,832 | 38,483 | 4,219,258 | 37,785 |
| NearCacheHybridCache | 555,156 | 45,996 | 523,903 | 45,936 | 476,399 | 45,819 |
| NearCache | 2,198,037 | 110,426 | 2,051,488 | 109,657 | 879,939 | 98,813 |

### Topologies

| Topology | Server version | Contender | Write mode | Reads/s | Local hit % | Stale reads | Stale entries |
|---|---|---|---|---:|---:|---:|---:|
| cluster | redis 7.4.11 | Plain | Foreign | 116,968 | n/a | 0 | n/a |
| cluster | redis 7.4.11 | NearCache | Foreign | 1,735,041 | 96.92 % | 74,757 | 0 |
| tls | redis 7.4.11 | Plain | Foreign | 40,406 | n/a | 0 | n/a |
| tls | redis 7.4.11 | NearCache | Foreign | 1,314,845 | 96.36 % | 56,776 | 0 |
| valkey | valkey 8.1.10 | Plain | Foreign | 172,215 | n/a | 0 | n/a |
| valkey | valkey 8.1.10 | NearCache | Foreign | 2,117,385 | 97.47 % | 280,100 | 0 |

### Chaos

| Contender | Write mode | Topology | Latency | Invalidations | Race discards | Flushes | Re-arms | Tracking keys | Stale entries after quiescence |
|---|---|---|---|---:|---:|---:|---:|---:|---:|
| NearCache | Foreign | standalone | 0ms | 1,132,566 | 3,330 | 42 | 16 | 9,994 | 0 |

### RedisNearCache internals

| Contender | Write mode | Latency | Invalidations | Invalidations/write | Race discards | Flushes | Re-arms | Tracking keys | Server used mem before | Server used mem after |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| NearCacheHybridCache | Foreign | 0ms | 578,825 | 9.63 | 273 | 0 | 0 | 185 | 10.7 MB | 9.9 MB |
| NearCacheHybridCache | Foreign | 0.5ms | 577,223 | 9.61 | 291 | 0 | 0 | 273 | 10.7 MB | 10.0 MB |
| NearCacheHybridCache | Foreign | 2ms | 571,445 | 9.51 | 318 | 0 | 0 | 454 | 10.6 MB | 10.3 MB |
| NearCacheHybridCache | Api | 0ms | 1,405,036 | 23.38 | 1,389 | 0 | 0 | 6,389 | 10.7 MB | 18.7 MB |
| NearCacheHybridCache | Api | 0.5ms | 1,388,833 | 23.12 | 1,300 | 0 | 0 | 6,456 | 10.6 MB | 18.7 MB |
| NearCacheHybridCache | Api | 2ms | 1,155,479 | 19.23 | 1,103 | 0 | 0 | 9,030 | 10.6 MB | 21.3 MB |
| NearCache | Foreign | 0ms | 1,155,965 | 19.23 | 3,531 | 0 | 0 | 9,974 | 9.9 MB | 17.6 MB |
| NearCache | Foreign | 0.5ms | 1,149,603 | 19.13 | 2,951 | 0 | 0 | 9,985 | 10.0 MB | 17.7 MB |
| NearCache | Foreign | 2ms | 1,094,383 | 18.21 | 1,878 | 0 | 0 | 9,971 | 9.9 MB | 17.6 MB |
| NearCache | Api | 0ms | 1,149,201 | 19.12 | 3,130 | 0 | 0 | 9,972 | 9.9 MB | 17.5 MB |
| NearCache | Api | 0.5ms | 1,149,247 | 19.10 | 3,122 | 0 | 0 | 9,988 | 9.9 MB | 17.6 MB |
| NearCache | Api | 2ms | 1,097,133 | 18.26 | 2,069 | 0 | 0 | 9,939 | 9.8 MB | 17.4 MB |

### Per-call cost — BenchmarkDotNet, 0ms

**Hit path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 134 µs | 658.2 ns | 1.000 | 3.47 KB |
| MemoryCacheTtl | String | 41.7 ns | 0.1 ns | 0.000312 | 56 B |
| HybridCache | String | 50.4 ns | 0.1 ns | 0.000377 | 56 B |
| FusionCache | String | 140.7 ns | 0.3 ns | 0.00105 | 240 B |
| NearCacheHybridCache | String | 514.1 ns | 0.7 ns | 0.00385 | 3.55 KB |
| NearCache | String | 169.1 ns | 0.5 ns | 0.00127 | 2.08 KB |
| Plain | Json | 135 µs | 686.6 ns | 1.000 | 936 B |
| MemoryCacheTtl | Json | 41.5 ns | 0.1 ns | 0.000308 | 56 B |
| HybridCache | Json | 506.9 ns | 0.6 ns | 0.00376 | 408 B |
| FusionCache | Json | 143.7 ns | 0.9 ns | 0.00107 | 240 B |
| NearCacheHybridCache | Json | 781.0 ns | 3.2 ns | 0.0058 | 1.00 KB |
| NearCache | Json | 345.4 ns | 0.6 ns | 0.00256 | 408 B |

**Miss path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 150 µs | 19 µs | 1.000 | 3.41 KB |
| MemoryCacheTtl | String | 153 µs | 21 µs | 1.019 | 3.56 KB |
| HybridCache | String | 298 µs | 26 µs | 1.991 | 6.49 KB |
| FusionCache | String | 440 µs | 4.5 µs | 2.939 | 12.66 KB |
| NearCacheHybridCache | String | 293 µs | 7.8 µs | 1.955 | 7.27 KB |
| NearCache | String | 144 µs | 3.2 µs | 0.959 | 4.20 KB |
| Plain | Json | 145 µs | 4.1 µs | 1.000 | 880 B |
| MemoryCacheTtl | Json | 147 µs | 18 µs | 1.010 | 1.01 KB |
| HybridCache | Json | 298 µs | 5.7 µs | 2.052 | 4.86 KB |
| FusionCache | Json | 444 µs | 13 µs | 3.055 | 9.23 KB |
| NearCacheHybridCache | Json | 306 µs | 22 µs | 2.108 | 4.33 KB |
| NearCache | Json | 145 µs | 5.0 µs | 0.994 | 1.65 KB |

### Per-call latency — Sailfish, 0ms

_p95/p99 are derived here from Sailfish's raw per-sample data (RawExecutionResults); Sailfish itself does not compute percentiles for `[SailfishMethod]` results, only mean/median/confidence intervals._

**Hit path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 226 µs | 227 µs | 319 µs | 432 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 706.0 ns | 707.5 ns | 956.0 ns | 1.4 µs | 0.003 [0.003, 0.003] | 1.0E-300 |
| HybridCache | String | 873.0 ns | 865.2 ns | 1.4 µs | 2.3 µs | 0.004 [0.004, 0.004] | 1.0E-300 |
| FusionCache | String | 1.7 µs | 1.8 µs | 3.0 µs | 4.5 µs | 0.008 [0.008, 0.008] | 1.0E-300 |
| NearCacheHybridCache | String | 3.8 µs | 3.7 µs | 5.6 µs | 10 µs | 0.017 [0.016, 0.017] | 1.0E-300 |
| NearCache | String | 1.1 µs | 1.1 µs | 1.8 µs | 3.5 µs | 0.005 [0.005, 0.005] | 1.0E-300 |
| Plain | Json | 248 µs | 256 µs | 484 µs | 564 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 623.0 ns | 607.7 ns | 915.0 ns | 1.4 µs | 0.002 [0.002, 0.002] | 1.0E-300 |
| HybridCache | Json | 3.0 µs | 3.0 µs | 4.2 µs | 6.4 µs | 0.012 [0.012, 0.012] | 1.0E-300 |
| FusionCache | Json | 1.5 µs | 1.5 µs | 2.4 µs | 5.0 µs | 0.006 [0.006, 0.006] | 1.0E-300 |
| NearCacheHybridCache | Json | 5.0 µs | 5.0 µs | 8.0 µs | 14 µs | 0.02 [0.019, 0.02] | 1.0E-300 |
| NearCache | Json | 1.2 µs | 1.2 µs | 1.7 µs | 3.2 µs | 0.005 [0.005, 0.005] | 1.0E-300 |

**Miss path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 242 µs | 344 µs | 710 µs | 999 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 280 µs | 342 µs | 600 µs | 730 µs | 0.993 [0.956, 1.031] | 2.3E-004 |
| HybridCache | String | 444 µs | 452 µs | 737 µs | 1,007 µs | 1.312 [1.273, 1.352] | 1.0E-300 |
| FusionCache | String | 666 µs | 668 µs | 858 µs | 1,108 µs | 1.940 [1.883, 1.999] | 1.0E-300 |
| NearCacheHybridCache | String | 505 µs | 514 µs | 748 µs | 1,027 µs | 1.492 [1.446, 1.539] | 1.0E-300 |
| NearCache | String | 234 µs | 245 µs | 507 µs | 670 µs | 0.712 [0.690, 0.735] | 1.0E-300 |
| Plain | Json | 228 µs | 240 µs | 515 µs | 754 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 226 µs | 229 µs | 311 µs | 442 µs | 0.953 [0.945, 0.962] | 1.1E-008 |
| HybridCache | Json | 439 µs | 442 µs | 582 µs | 682 µs | 1.842 [1.826, 1.859] | 1.0E-300 |
| FusionCache | Json | 669 µs | 676 µs | 965 µs | 1,274 µs | 2.818 [2.791, 2.845] | 1.0E-300 |
| NearCacheHybridCache | Json | 447 µs | 442 µs | 555 µs | 709 µs | 1.844 [1.826, 1.862] | 1.0E-300 |
| NearCache | Json | 239 µs | 321 µs | 641 µs | 752 µs | 1.339 [1.300, 1.379] | 1.0E-300 |

### Cross-check: BenchmarkDotNet vs Sailfish — 0ms

| Path | Contender | Payload | BDN median | Sailfish median | Difference |
|---|---|---|---:|---:|---:|
| Hit | Plain | String | 134 µs | 226 µs | 69.4 % |
| Hit | MemoryCacheTtl | String | 41.7 ns | 706.0 ns | 1,592.7 % |
| Hit | HybridCache | String | 50.4 ns | 873.0 ns | 1,633.7 % |
| Hit | FusionCache | String | 140.8 ns | 1.7 µs | 1,111.9 % |
| Hit | NearCacheHybridCache | String | 514.4 ns | 3.8 µs | 636.8 % |
| Hit | NearCache | String | 169.2 ns | 1.1 µs | 563.7 % |
| Hit | Plain | Json | 135 µs | 248 µs | 83.7 % |
| Hit | MemoryCacheTtl | Json | 41.5 ns | 623.0 ns | 1,400.8 % |
| Hit | HybridCache | Json | 506.8 ns | 3.0 µs | 499.7 % |
| Hit | FusionCache | Json | 144.0 ns | 1.5 µs | 969.7 % |
| Hit | NearCacheHybridCache | Json | 779.5 ns | 5.0 µs | 541.1 % |
| Hit | NearCache | Json | 345.2 ns | 1.2 µs | 237.4 % |
| Miss | Plain | String | 145 µs | 242 µs | 67.3 % |
| Miss | MemoryCacheTtl | String | 144 µs | 280 µs | 95.3 % |
| Miss | HybridCache | String | 292 µs | 444 µs | 52.4 % |
| Miss | FusionCache | String | 442 µs | 666 µs | 50.9 % |
| Miss | NearCacheHybridCache | String | 293 µs | 505 µs | 72.5 % |
| Miss | NearCache | String | 144 µs | 234 µs | 63.1 % |
| Miss | Plain | Json | 146 µs | 228 µs | 56.1 % |
| Miss | MemoryCacheTtl | Json | 143 µs | 226 µs | 58.3 % |
| Miss | HybridCache | Json | 299 µs | 439 µs | 47.1 % |
| Miss | FusionCache | Json | 444 µs | 669 µs | 50.7 % |
| Miss | NearCacheHybridCache | Json | 301 µs | 447 µs | 48.6 % |
| Miss | NearCache | Json | 145 µs | 239 µs | 65.2 % |

### Per-call cost — BenchmarkDotNet, 0.5ms

**Hit path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 1,187 µs | 28 µs | 1.000 | 3.47 KB |
| MemoryCacheTtl | String | 42.1 ns | 0.1 ns | 3.55E-05 | 56 B |
| HybridCache | String | 51.9 ns | 0.1 ns | 4.37E-05 | 56 B |
| FusionCache | String | 142.3 ns | 0.2 ns | 0.00012 | 240 B |
| NearCacheHybridCache | String | 535.6 ns | 1.1 ns | 0.000451 | 3.55 KB |
| NearCache | String | 175.4 ns | 0.4 ns | 0.000148 | 2.08 KB |
| Plain | Json | 1,201 µs | 19 µs | 1.000 | 937 B |
| MemoryCacheTtl | Json | 41.3 ns | 0.1 ns | 3.44E-05 | 56 B |
| HybridCache | Json | 522.1 ns | 0.5 ns | 0.000435 | 408 B |
| FusionCache | Json | 143.2 ns | 0.3 ns | 0.000119 | 240 B |
| NearCacheHybridCache | Json | 775.4 ns | 0.6 ns | 0.000646 | 1.00 KB |
| NearCache | Json | 342.8 ns | 0.5 ns | 0.000285 | 408 B |

**Miss path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 1,133 µs | 42 µs | 1.000 | 3.41 KB |
| MemoryCacheTtl | String | 1,093 µs | 77 µs | 0.965 | 3.56 KB |
| HybridCache | String | 2,295 µs | 118 µs | 2.026 | 6.36 KB |
| FusionCache | String | 3,337 µs | 157 µs | 2.946 | 12.62 KB |
| NearCacheHybridCache | String | 2,297 µs | 183 µs | 2.028 | 7.18 KB |
| NearCache | String | 1,144 µs | 130 µs | 1.010 | 4.20 KB |
| Plain | Json | 1,121 µs | 80 µs | 1.000 | 880 B |
| MemoryCacheTtl | Json | 1,131 µs | 96 µs | 1.009 | 1.01 KB |
| HybridCache | Json | 2,307 µs | 193 µs | 2.058 | 4.86 KB |
| FusionCache | Json | 3,486 µs | 196 µs | 3.110 | 9.23 KB |
| NearCacheHybridCache | Json | 2,394 µs | 261 µs | 2.135 | 4.33 KB |
| NearCache | Json | 1,155 µs | 110 µs | 1.030 | 1.65 KB |

### Per-call latency — Sailfish, 0.5ms

_p95/p99 are derived here from Sailfish's raw per-sample data (RawExecutionResults); Sailfish itself does not compute percentiles for `[SailfishMethod]` results, only mean/median/confidence intervals._

**Hit path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 1,205 µs | 1,178 µs | 1,453 µs | 1,676 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 706.0 ns | 699.9 ns | 1.1 µs | 2.1 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| HybridCache | String | 832.0 ns | 873.7 ns | 1.5 µs | 3.7 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| FusionCache | String | 1.4 µs | 1.4 µs | 2.1 µs | 3.7 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| NearCacheHybridCache | String | 3.2 µs | 3.2 µs | 4.9 µs | 8.2 µs | 0.003 [0.003, 0.003] | 1.0E-300 |
| NearCache | String | 748.0 ns | 789.3 ns | 1.2 µs | 2.1 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| Plain | Json | 1,179 µs | 1,152 µs | 1,394 µs | 1,598 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 456.0 ns | 448.4 ns | 789.0 ns | 1.2 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |
| HybridCache | Json | 1.6 µs | 1.6 µs | 2.3 µs | 4.6 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| FusionCache | Json | 1.4 µs | 1.3 µs | 2.0 µs | 4.2 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| NearCacheHybridCache | Json | 4.0 µs | 4.0 µs | 6.1 µs | 9.5 µs | 0.003 [0.003, 0.004] | 1.0E-300 |
| NearCache | Json | 2.3 µs | 2.3 µs | 3.5 µs | 6.0 µs | 0.002 [0.002, 0.002] | 1.0E-300 |

**Miss path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 1,085 µs | 1,106 µs | 1,446 µs | 1,731 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 1,123 µs | 1,139 µs | 1,499 µs | 1,764 µs | 1.030 [1.016, 1.045] | 7.7E-005 |
| HybridCache | String | 2,010 µs | 2,067 µs | 2,585 µs | 2,967 µs | 1.870 [1.845, 1.894] | 1.0E-300 |
| FusionCache | String | 3,368 µs | 3,370 µs | 4,014 µs | 4,559 µs | 3.048 [3.013, 3.083] | 1.0E-300 |
| NearCacheHybridCache | String | 2,136 µs | 2,138 µs | 2,657 µs | 3,071 µs | 1.933 [1.907, 1.959] | 1.0E-300 |
| NearCache | String | 1,130 µs | 1,126 µs | 1,393 µs | 1,586 µs | 1.018 [1.005, 1.032] | 0.002 |
| Plain | Json | 1,089 µs | 1,088 µs | 1,427 µs | 1,643 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 1,043 µs | 1,066 µs | 1,332 µs | 1,556 µs | 0.979 [0.966, 0.993] | 0.029 |
| HybridCache | Json | 2,035 µs | 2,096 µs | 2,645 µs | 3,062 µs | 1.926 [1.900, 1.953] | 1.0E-300 |
| FusionCache | Json | 3,329 µs | 3,281 µs | 3,908 µs | 4,442 µs | 3.015 [2.976, 3.054] | 1.0E-300 |
| NearCacheHybridCache | Json | 1,959 µs | 2,031 µs | 2,579 µs | 2,930 µs | 1.866 [1.841, 1.892] | 1.0E-300 |
| NearCache | Json | 963 µs | 1,006 µs | 1,358 µs | 1,703 µs | 0.925 [0.911, 0.938] | 1.0E-300 |

### Cross-check: BenchmarkDotNet vs Sailfish — 0.5ms

| Path | Contender | Payload | BDN median | Sailfish median | Difference |
|---|---|---|---:|---:|---:|
| Hit | Plain | String | 1,186 µs | 1,205 µs | 1.6 % |
| Hit | MemoryCacheTtl | String | 42.1 ns | 706.0 ns | 1,575.9 % |
| Hit | HybridCache | String | 51.9 ns | 832.0 ns | 1,502.8 % |
| Hit | FusionCache | String | 142.3 ns | 1.4 µs | 864.7 % |
| Hit | NearCacheHybridCache | String | 535.6 ns | 3.2 µs | 498.6 % |
| Hit | NearCache | String | 175.4 ns | 748.0 ns | 326.5 % |
| Hit | Plain | Json | 1,203 µs | 1,179 µs | -2.0 % |
| Hit | MemoryCacheTtl | Json | 41.3 ns | 456.0 ns | 1,002.9 % |
| Hit | HybridCache | Json | 522.1 ns | 1.6 µs | 202.8 % |
| Hit | FusionCache | Json | 143.2 ns | 1.4 µs | 885.7 % |
| Hit | NearCacheHybridCache | Json | 775.3 ns | 4.0 µs | 410.2 % |
| Hit | NearCache | Json | 342.7 ns | 2.3 µs | 568.2 % |
| Miss | Plain | String | 1,136 µs | 1,085 µs | -4.5 % |
| Miss | MemoryCacheTtl | String | 1,093 µs | 1,123 µs | 2.8 % |
| Miss | HybridCache | String | 2,277 µs | 2,010 µs | -11.7 % |
| Miss | FusionCache | String | 3,357 µs | 3,368 µs | 0.3 % |
| Miss | NearCacheHybridCache | String | 2,272 µs | 2,136 µs | -6.0 % |
| Miss | NearCache | String | 1,128 µs | 1,130 µs | 0.2 % |
| Miss | Plain | Json | 1,121 µs | 1,089 µs | -2.8 % |
| Miss | MemoryCacheTtl | Json | 1,124 µs | 1,043 µs | -7.2 % |
| Miss | HybridCache | Json | 2,311 µs | 2,035 µs | -11.9 % |
| Miss | FusionCache | Json | 3,504 µs | 3,329 µs | -5.0 % |
| Miss | NearCacheHybridCache | Json | 2,377 µs | 1,959 µs | -17.6 % |
| Miss | NearCache | Json | 1,172 µs | 963 µs | -17.9 % |

### Per-call cost — BenchmarkDotNet, 2ms

**Hit path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 3,226 µs | 39 µs | 1.000 | 3.47 KB |
| MemoryCacheTtl | String | 41.9 ns | 0.1 ns | 1.3E-05 | 56 B |
| HybridCache | String | 48.8 ns | 0.5 ns | 1.51E-05 | 56 B |
| FusionCache | String | 140.0 ns | 0.3 ns | 4.34E-05 | 240 B |
| NearCacheHybridCache | String | 528.0 ns | 3.4 ns | 0.000164 | 3.55 KB |
| NearCache | String | 167.1 ns | 0.5 ns | 5.18E-05 | 2.08 KB |
| Plain | Json | 3,239 µs | 57 µs | 1.000 | 937 B |
| MemoryCacheTtl | Json | 41.0 ns | 0.1 ns | 1.27E-05 | 56 B |
| HybridCache | Json | 504.1 ns | 0.7 ns | 0.000156 | 408 B |
| FusionCache | Json | 140.3 ns | 0.4 ns | 4.33E-05 | 240 B |
| NearCacheHybridCache | Json | 765.7 ns | 0.7 ns | 0.000236 | 1.00 KB |
| NearCache | Json | 335.5 ns | 0.7 ns | 0.000104 | 408 B |

**Miss path**

| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |
|---|---|---:|---:|---:|---:|
| Plain | String | 3,201 µs | 162 µs | 1.000 | 3.41 KB |
| MemoryCacheTtl | String | 3,215 µs | 139 µs | 1.005 | 3.56 KB |
| HybridCache | String | 6,389 µs | 237 µs | 1.996 | 6.36 KB |
| FusionCache | String | 9,625 µs | 258 µs | 3.007 | 12.62 KB |
| NearCacheHybridCache | String | 6,307 µs | 226 µs | 1.971 | 7.17 KB |
| NearCache | String | 3,218 µs | 233 µs | 1.005 | 4.20 KB |
| Plain | Json | 3,160 µs | 302 µs | 1.000 | 880 B |
| MemoryCacheTtl | Json | 3,130 µs | 140 µs | 0.990 | 1.01 KB |
| HybridCache | Json | 6,340 µs | 244 µs | 2.006 | 4.72 KB |
| FusionCache | Json | 9,451 µs | 355 µs | 2.991 | 9.19 KB |
| NearCacheHybridCache | Json | 6,341 µs | 254 µs | 2.006 | 4.23 KB |
| NearCache | Json | 3,140 µs | 211 µs | 0.994 | 1.65 KB |

### Per-call latency — Sailfish, 2ms

_p95/p99 are derived here from Sailfish's raw per-sample data (RawExecutionResults); Sailfish itself does not compute percentiles for `[SailfishMethod]` results, only mean/median/confidence intervals._

**Hit path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 3,170 µs | 3,157 µs | 3,658 µs | 4,062 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 706.0 ns | 716.0 ns | 998.0 ns | 1.6 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |
| HybridCache | String | 832.0 ns | 886.3 ns | 1.7 µs | 3.4 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |
| FusionCache | String | 1.7 µs | 1.8 µs | 3.4 µs | 5.2 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| NearCacheHybridCache | String | 3.8 µs | 3.8 µs | 5.7 µs | 9.7 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| NearCache | String | 1.1 µs | 1.1 µs | 2.0 µs | 3.5 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |
| Plain | Json | 3,128 µs | 3,118 µs | 3,683 µs | 4,052 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 457.0 ns | 470.6 ns | 748.0 ns | 1.2 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |
| HybridCache | Json | 1.6 µs | 1.6 µs | 3.6 µs | 15 µs | 0.001 [<0.001, 0.001] | 1.0E-300 |
| FusionCache | Json | 1.5 µs | 1.4 µs | 2.2 µs | 4.9 µs | <0.001 [<0.001, <0.001] | 1.0E-300 |
| NearCacheHybridCache | Json | 3.4 µs | 3.3 µs | 5.2 µs | 11 µs | 0.001 [0.001, 0.001] | 1.0E-300 |
| NearCache | Json | 2.5 µs | 2.5 µs | 4.5 µs | 10 µs | 0.001 [0.001, 0.001] | 1.0E-300 |

**Miss path**

| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |
|---|---|---:|---:|---:|---:|---:|---:|
| Plain | String | 3,102 µs | 3,094 µs | 3,579 µs | 3,881 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | String | 3,069 µs | 3,071 µs | 3,584 µs | 4,265 µs | 0.993 [0.984, 1.001] | 0.078 |
| HybridCache | String | 6,111 µs | 6,118 µs | 6,945 µs | 8,452 µs | 1.978 [1.963, 1.992] | 1.0E-300 |
| FusionCache | String | 9,237 µs | 9,257 µs | 10,282 µs | 11,485 µs | 2.993 [2.972, 3.014] | 1.0E-300 |
| NearCacheHybridCache | String | 6,095 µs | 6,104 µs | 6,859 µs | 7,581 µs | 1.973 [1.959, 1.987] | 1.0E-300 |
| NearCache | String | 3,095 µs | 3,098 µs | 3,575 µs | 3,938 µs | 1.002 [0.993, 1.010] | 0.838 |
| Plain | Json | 3,115 µs | 3,124 µs | 3,613 µs | 3,977 µs | 1.000 (baseline) | n/a |
| MemoryCacheTtl | Json | 3,133 µs | 3,122 µs | 3,632 µs | 3,927 µs | 0.999 [0.991, 1.008] | 0.966 |
| HybridCache | Json | 6,118 µs | 6,120 µs | 6,916 µs | 7,713 µs | 1.959 [1.944, 1.974] | 1.0E-300 |
| FusionCache | Json | 9,250 µs | 9,254 µs | 10,411 µs | 16,189 µs | 2.962 [2.941, 2.983] | 1.0E-300 |
| NearCacheHybridCache | Json | 6,085 µs | 6,088 µs | 6,856 µs | 7,831 µs | 1.949 [1.934, 1.963] | 1.0E-300 |
| NearCache | Json | 3,102 µs | 3,118 µs | 3,631 µs | 4,045 µs | 0.998 [0.990, 1.007] | 0.827 |

### Cross-check: BenchmarkDotNet vs Sailfish — 2ms

| Path | Contender | Payload | BDN median | Sailfish median | Difference |
|---|---|---|---:|---:|---:|
| Hit | Plain | String | 3,232 µs | 3,170 µs | -1.9 % |
| Hit | MemoryCacheTtl | String | 41.9 ns | 706.0 ns | 1,584.9 % |
| Hit | HybridCache | String | 48.6 ns | 832.0 ns | 1,612.0 % |
| Hit | FusionCache | String | 140.0 ns | 1.7 µs | 1,148.6 % |
| Hit | NearCacheHybridCache | String | 526.7 ns | 3.8 µs | 619.6 % |
| Hit | NearCache | String | 167.1 ns | 1.1 µs | 547.6 % |
| Hit | Plain | Json | 3,223 µs | 3,128 µs | -3.0 % |
| Hit | MemoryCacheTtl | Json | 41.0 ns | 457.0 ns | 1,014.3 % |
| Hit | HybridCache | Json | 504.1 ns | 1.6 µs | 213.6 % |
| Hit | FusionCache | Json | 140.3 ns | 1.5 µs | 965.2 % |
| Hit | NearCacheHybridCache | Json | 765.6 ns | 3.4 µs | 345.9 % |
| Hit | NearCache | Json | 335.6 ns | 2.5 µs | 643.4 % |
| Miss | Plain | String | 3,197 µs | 3,102 µs | -3.0 % |
| Miss | MemoryCacheTtl | String | 3,206 µs | 3,069 µs | -4.3 % |
| Miss | HybridCache | String | 6,366 µs | 6,111 µs | -4.0 % |
| Miss | FusionCache | String | 9,648 µs | 9,237 µs | -4.3 % |
| Miss | NearCacheHybridCache | String | 6,278 µs | 6,095 µs | -2.9 % |
| Miss | NearCache | String | 3,266 µs | 3,095 µs | -5.2 % |
| Miss | Plain | Json | 3,092 µs | 3,115 µs | 0.7 % |
| Miss | MemoryCacheTtl | Json | 3,135 µs | 3,133 µs | -0.0 % |
| Miss | HybridCache | Json | 6,333 µs | 6,118 µs | -3.4 % |
| Miss | FusionCache | Json | 9,391 µs | 9,250 µs | -1.5 % |
| Miss | NearCacheHybridCache | Json | 6,302 µs | 6,085 µs | -3.4 % |
| Miss | NearCache | Json | 3,118 µs | 3,102 µs | -0.5 % |

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
