<div align="center">
  <img src="docs/mark.svg" alt="RedisNearCache mark" width="104" />
  <h1>RedisNearCache</h1>
  <p><strong>Client-side caching on top of StackExchange.Redis. Keeps the values you read in memory and drops them the moment Redis says they changed.</strong></p>
  <p>
    <a href="https://github.com/magna-nz/redis-near-cache/actions/workflows/ci.yml"><img src="https://github.com/magna-nz/redis-near-cache/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI" /></a>
    <a href="https://www.nuget.org/packages/RedisNearCache"><img src="https://img.shields.io/nuget/v/RedisNearCache?label=nuget" alt="NuGet" /></a>
    <a href="https://dotnet.microsoft.com/"><img src="https://img.shields.io/badge/.NET-8%20%7C%2010-512BD4" alt=".NET 8 and 10" /></a>
    <a href="https://redis.io/docs/latest/develop/clients/client-side-caching/"><img src="https://img.shields.io/badge/Redis-6%2B%20%7C%20Valkey-DC382D" alt="Redis 6+ or Valkey" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="MIT License" /></a>
  </p>
  <p><a href="https://magna-nz.github.io/redis-near-cache/">Documentation</a></p>
</div>

<br />

Redis 6 can tell a client when a key it has read changes (`CLIENT TRACKING`). RedisNearCache uses that to
keep a local copy of what you read: a hit is served from memory, and a write from anywhere evicts the copy a
few milliseconds later. It is a package you add next to StackExchange.Redis, not a replacement for it: your existing multiplexer
keeps doing everything it does today, and RedisNearCache opens one extra connection for the tracked reads.

Redis added this in version 6 back in 2020. StackExchange.Redis [never picked it up](https://github.com/StackExchange/StackExchange.Redis/issues/1461),
and the 3.x rewrite still ships without it. Rather than fork the library, RedisNearCache sits on top of it: a
second connection that it owns does the tracking, and your existing one keeps working exactly as before.

RedisNearCache is for code that already runs on StackExchange.Redis.

<div align="center">
  <img src="docs/architecture-diagram.svg" alt="Your code reads through RedisNearCache; misses go over a tracked private connection; Redis pushes invalidations to a private subscriber connection which evicts the local copy" width="820" />
  <br />
  <sub><strong>Only reads that go through the cache are tracked.</strong> Writes can come from anywhere.</sub>
</div>

<br />

More in the [documentation](https://magna-nz.github.io/redis-near-cache/), including the sequence diagrams,
the reconnect model and the API reference.

## Install

```sh
dotnet add package RedisNearCache
```

Add `RedisNearCache.HybridCache` as well if you want it behind `HybridCache` or `IDistributedCache`.
Needs Redis 6 or newer, or Valkey. Garnet does not implement `CLIENT TRACKING`, and neither Redis
Enterprise-based services (Azure Managed Redis, Redis Cloud, Redis Software) nor ElastiCache Serverless are
supported.

## Use

```csharp
services.AddRedisNearCache("localhost:6379");
```

```csharp
var cache = provider.GetRequiredService<IRedisNearCache>();
await cache.Ready;                                     // subscription up, tracking armed on every master

var user = await cache.GetAsync<User>("user:42");      // miss: one GET, tracked, stored locally
var again = await cache.GetAsync<User>("user:42");     // hit: no network call

await cache.SetAsync("user:42", user with { Name = "Ada" });   // writes through, evicts the local copy
Console.WriteLine(cache.Statistics);                   // hits=1 misses=1 invalidations=0 flushes=0 rearms=0 raceDiscards=0
```

Then, from anywhere:

```sh
redis-cli SET user:42 '{"Name":"Grace"}'               # the next GetAsync sees Grace
```

Behind `HybridCache`:

```csharp
services.AddRedisNearCache("localhost:6379");
services.AddRedisNearCacheHybridCache();               // HybridCache's own L1 is disabled; ours is the coherent one
```

## Performance

20 app instances, 160 readers, and 2,000 writes/s from another service straight to Redis. Redis 7.4 on one machine,
TTL caches set to 10 s.

<!-- HEADLINE -->
| | Reads/s | Reads served stale | Stalest read |
|---|---:|---:|---:|
| **RedisNearCache** | **2.20 M** | **0.33 %** | **105 ms** |
| Plain StackExchange.Redis | 164,427 | 0 % | – |
| `IMemoryCache` + 10 s TTL | 14.51 M | 83.1 % | 10.0 s |
| `HybridCache` + Redis L2 | 25.99 M | 84.8 % | 10.0 s |
| FusionCache + backplane | 6.42 M | 85.0 % | 10.0 s |
<!-- /HEADLINE -->

- **13x the reads of plain StackExchange.Redis**, and still fresh: any write from any client evicts the local copy.
- **TTL caches read faster, but served over 80 % of reads stale**, some up to the full 10 s.

**[Full results and methodology →](bench/RedisNearCache.Bench/README.md)** Covers server traffic, latency sweeps,
writes through each library's own API, cluster, TLS, Valkey, chaos, and per-call costs.

## How it stays correct

- The private connection is armed with `CLIENT TRACKING ON REDIRECT <subscriber> NOLOOP` on every master.
- An interactive or subscriber reconnect re-arms that node and flushes the local cache; the cache serves
  straight from Redis until every master is armed again.
- A read whose key was invalidated while the reply was in flight is not cached.
- If tracking cannot be armed at all, every read goes to Redis and nothing is cached, ever, until it can.

## Docs

The quickstart, diagrams, configuration, API reference, HybridCache adapter, operations guide and FAQ live
on the **[documentation site](https://magna-nz.github.io/redis-near-cache/)**.

## Development

```sh
./up.sh                   # every container the tests need (standalone, replica, TLS, cluster, Sentinel, managed-style)
dotnet test tests/RedisNearCache.Tests --filter "Category!=Soak"
bench/run-matrix.sh --quick   # benchmarks, not run in CI; see bench/RedisNearCache.Bench/README.md
```

## License

MIT
