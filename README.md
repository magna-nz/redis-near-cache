<div align="center">
  <img src="docs/mark.svg" alt="RedisNearCache mark" width="104" />
  <h1>RedisNearCache</h1>
  <p><strong>Client-side caching for StackExchange.Redis. Keeps the values you read in memory and drops them the moment Redis says they changed.</strong></p>
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
few milliseconds later. It runs on StackExchange.Redis 3.x over its own connection, so nothing about your
existing setup changes.

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
Needs Redis 6 or newer, or Valkey. Garnet does not implement `CLIENT TRACKING`.

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

Measured on one machine against a local Redis 7.4, 20 application instances, 160 readers and 2,000
foreign writes per second (details and caveats in the [docs](https://magna-nz.github.io/redis-near-cache/#performance)):

| | Near cache | Plain StackExchange.Redis |
|---|---:|---:|
| Reads per second | 1,369,673 | 190,756 |
| Server commands per second | 59,585 | 192,758 |
| Stale local entries after 60,000 foreign writes | 0 | n/a |

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
docker compose up -d      # Redis 7.4 on :6379 with a replica on :6380
./cluster-up.sh           # 3-master cluster on :7100-7102
dotnet test tests/RedisNearCache.Tests
```

`tests/RedisNearCache.UnitTests` needs no Redis (tracker, L1, serializer, configuration, adapter mapping).
`tests/RedisNearCache.Tests` are integration tests against those containers: reconnects, replica, cluster,
races, stress, chaos, degraded mode and recovery. CI runs both on every push and pull request.

## Releasing

Publish a GitHub release whose tag is `vX.Y.Z` (or push that tag). CI runs the unit and integration tests,
packs both packages at that version, pushes them to nuget.org through Trusted Publishing (the workflow's
GitHub identity, no stored secret), and attaches the `.nupkg` files to the release.

## License

MIT
