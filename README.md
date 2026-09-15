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

## What you get

Compared with the caches you would otherwise reach for, on one machine against Redis 7.4 in Docker: 20 application
instances, 160 readers, and 2,000 writes per second made by another client straight to Redis. The TTL-based caches use
a 10 s TTL.

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

- **Only RedisNearCache stays fresh when something else writes.** The TTL caches served most reads stale, up to the
  whole TTL; FusionCache's backplane only carries writes made through FusionCache.
- **Freshness costs server traffic.** Every write invalidates the key on every instance tracking it, and each re-reads
  it on next access: more commands than a TTL cache, still fewer than no cache.
- **In-process TTL caches read faster.** They return a stored object; RedisNearCache decodes bytes and checks
  coherence on every hit (223 ns vs 41 ns per hit in BenchmarkDotNet). A cold read costs about what a plain `GET` does.

**[Full results, methodology and how to run them →](bench/RedisNearCache.Bench/README.md)**: latency sweeps (0, 0.5
and 2 ms injected), writes through each library's API, cluster, TLS, Valkey, a chaos run, and per-call BenchmarkDotNet
and Sailfish numbers.

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
