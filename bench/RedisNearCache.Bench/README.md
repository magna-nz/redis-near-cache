# RedisNearCache.Bench

Benchmarks and a small zero-traffic demonstration for `RedisNearCache`. Both modes need a Redis server
reachable at `localhost:6379` (the repo's `redis-near-cache-redis` container: `docker compose up -d` from
the repo root).

This machine does not have the dotnet SDK on `PATH` (only at `~/.dotnet/dotnet`), so the benchmark run uses
BenchmarkDotNet's in-process (emit) toolchain instead of the default toolchain, which would otherwise shell
out to a `dotnet` process on `PATH` to build and run each benchmark in isolation. The job also uses
`RunStrategy.Monitoring` (one invocation per iteration, no auto-unrolling) because these benchmarks are
network round trips rather than tight in-memory loops; the default `Throughput` strategy would otherwise try
to pack thousands of Redis calls into each iteration to reach its default target iteration duration.

## Running

From the repo root:

```
# Zero-traffic demo
~/.dotnet/dotnet run -c Release --project bench/RedisNearCache.Bench -- --demo

# BenchmarkDotNet suite
~/.dotnet/dotnet run -c Release --project bench/RedisNearCache.Bench
```

## `--demo`: the zero-traffic demonstration

What it does, in order:

1. Builds a near cache (`AddRedisNearCache("localhost:6379")`) and awaits `Ready`.
2. Opens a second, entirely separate plain `ConnectionMultiplexer` -- standing in for "some other client" --
   and uses it to `SET` a key.
3. Reads that key once through the near cache (`GetAsync<string>`). This is a miss: it populates L1 and,
   because the read went over RedisNearCache's tracked connection, arms server-side invalidation for the key.
4. Records the server's `cmdstat_get` call count (`INFO commandstats`), then reads the same key 1,000 more
   times through the near cache, then records `cmdstat_get` again. **All 1,000 reads should be served
   entirely from L1: the delta should be 0.** This is the "zero traffic" claim -- once a key is cached and
   tracked, repeated reads never touch Redis again until an invalidation arrives.
5. Writes the key again through the *other* (plain) multiplexer, simulating an external write from another
   client/language, and times how long it takes the near cache's local copy to disappear (i.e. how long from
   the write until `TryGetLocal` starts returning `false`). This is the invalidation push's round-trip
   latency: write -> Redis -> `__redis__:invalidate` push -> our subscriber -> L1 evicted.
6. Prints `IRedisNearCache.Statistics` (hits/misses/invalidations/etc.) and a short summary table.

### What the numbers mean

- **Redis GETs for 1000 L1 hits**: should be `0`. Any non-zero value would mean the near cache is not
  actually serving cached reads from memory.
- **Write-to-eviction latency**: wall-clock time from the external `SET` completing to the near cache's L1
  entry being evicted. This is invalidation push latency, not a benchmark of throughput; it depends on
  round trip time to Redis and the Redis server's own dispatch of the invalidation message.
- **Statistics**: `hits=1000` confirms all 1,000 repeated reads were L1 hits; `misses=1` is the initial read
  that populated L1; `invalidations=1` confirms exactly one invalidation was received for the external write.

### Actual output

```
RedisNearCache zero-traffic demo
=================================
Connecting to localhost:6379 ...
Seeded 'demo:near-cache:zero-traffic' = 'initial-value' (first read through the near cache; this one is a miss).

1000 reads of 'demo:near-cache:zero-traffic' through the near cache (all should be L1 hits):
  cmdstat_get calls before : 263325
  cmdstat_get calls after  : 263325
  delta (Redis GETs issued): 0  (expected 0)

Write-to-eviction latency  : 1688.0 µs

Statistics: hits=1000 misses=1 invalidations=1 flushes=0 rearms=0 raceDiscards=0

Summary
-------
Metric                           | Value
------------------------------------------------
Redis GETs for 1000 L1 hits      | 0
Write-to-eviction latency (µs)   | 1688.0
L1 hits                          | 1000
L1 misses                        | 1
Invalidations received           | 1
```

(`cmdstat_get calls before`/`after` are cumulative server-wide counters since the Redis server started, hence
the large absolute values on a container that has been up for a while -- what matters is that the delta
between them, across the 1,000 repeated reads, is `0`.)

## BenchmarkDotNet suite

`ReadBenchmarks` compares, for both a 1 KB string value and a small JSON object (a 5-field record,
serialized with the default `System.Text.Json` serializer):

- `Plain_StringGet` -- baseline: `IDatabase.StringGetAsync` on a plain StackExchange.Redis multiplexer, no
  near cache involved.
- `NearCache_Hit` -- `IRedisNearCache.GetAsync<T>` for a key already present in L1.
- `NearCache_Miss` -- `EvictLocal(key)` followed by `GetAsync<T>`, so the measurement includes the tracked
  GET round trip to Redis.
- `NearCache_TryGetLocal` -- the pure L1 path (`TryGetLocal<T>`); never touches Redis.

`[GlobalSetup]` builds a fresh `ServiceProvider` with `AddRedisNearCache("localhost:6379")`, awaits `Ready`,
and seeds both keys (once per `Kind` parameter value, since BenchmarkDotNet re-runs setup for each value of
the `[Params]`-decorated `Kind` property).

### What the numbers mean

- `NearCache_Hit` and `NearCache_TryGetLocal` should be roughly two to three orders of magnitude faster than
  `Plain_StringGet`, because they never leave the process -- that's the entire point of a near cache.
- `NearCache_Miss` should land close to `Plain_StringGet`, since both do a real Redis round trip; the near
  cache's overhead on the miss path (locking, in-flight bookkeeping, deserialization) is the difference
  between the two.
- `Allocated` (from `[MemoryDiagnoser]`) shows the managed allocation cost per call. The hit/`TryGetLocal`
  paths allocate a little (deserializing the stored value into a fresh instance each call) but skip the
  network buffers and protocol parsing that the Redis round trip paths pay for.

### Actual output

```
BenchmarkDotNet v0.15.8, macOS Tahoe 26.6.2 (25G83) [Darwin 25.6.0]
Apple M4 Pro, 1 CPU, 14 logical and 14 physical cores
  [Host] : .NET 10.0.11 (10.0.11, 10.0.1126.37416), Arm64 RyuJIT armv8.0-a

Toolchain=InProcessEmitToolchain  IterationCount=10  RunStrategy=Monitoring
WarmupCount=3

| Method                | Kind   | Mean       | Error       | StdDev     | Median     | Ratio | RatioSD | Allocated | Alloc Ratio |
|---------------------- |------- |-----------:|------------:|-----------:|-----------:|------:|--------:|----------:|------------:|
| Plain_StringGet       | String | 173.717 us |  22.9249 us | 15.1634 us | 169.833 us | 1.006 |    0.11 |    1360 B |        1.00 |
| NearCache_Hit         | String |   1.604 us |   1.2775 us |  0.8450 us |   1.271 us | 0.009 |    0.00 |    2144 B |        1.58 |
| NearCache_Miss        | String | 198.658 us |  22.6082 us | 14.9540 us | 196.854 us | 1.151 |    0.12 |    9088 B |        6.68 |
| NearCache_TryGetLocal | String |   1.325 us |   1.0384 us |  0.6868 us |   1.062 us | 0.008 |    0.00 |    2072 B |        1.52 |
|                       |        |            |             |            |            |       |         |           |             |
| Plain_StringGet       | Json   | 276.979 us | 109.6931 us | 72.5552 us | 260.916 us |  1.05 |    0.35 |    5456 B |        1.00 |
| NearCache_Hit         | Json   |   3.967 us |   2.9244 us |  1.9343 us |   3.292 us |  0.02 |    0.01 |     424 B |        0.08 |
| NearCache_Miss        | Json   | 280.596 us |  25.2970 us | 16.7324 us | 281.333 us |  1.07 |    0.24 |    6472 B |        1.19 |
| NearCache_TryGetLocal | Json   |   2.875 us |   0.5546 us |  0.3668 us |   2.792 us |  0.01 |    0.00 |     352 B |        0.06 |

Run time: 00:00:00 (0.3 sec), executed benchmarks: 8
Global total time: 00:00:00 (0.56 sec), executed benchmarks: 8
```

`NearCache_Hit` and `NearCache_TryGetLocal` are roughly **100x faster** than the plain Redis round trip in
this run, which is the expected result for a warm, tracked, in-process cache read against a local
single-node Redis container. `NearCache_Miss` lands close to (and, on the string case, a little above)
`Plain_StringGet`, which is also expected: it does the same network round trip plus the near cache's own
bookkeeping and deserialization. Because `RunStrategy.Monitoring` takes one measurement per iteration with
only 10 iterations, run-to-run variance (`Error`/`StdDev`) is higher than a long `Throughput` run would show;
this is a deliberate trade-off to keep the whole suite finishing in well under a minute.
