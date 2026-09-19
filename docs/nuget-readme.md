# RedisNearCache

Client-side caching on top of StackExchange.Redis. Keeps the values you read in memory and drops them the moment Redis says they changed.

**Works with:** Redis 6+ and Valkey (self-hosted, Docker, Kubernetes); Azure Managed Redis, Redis Cloud and Redis
Software, using `TrackingMode.Broadcast`.

Redis 6 can tell a client when a key it has read changes (`CLIENT TRACKING`). RedisNearCache uses that to keep a local copy of what you read: a hit is served from memory, and a write from anywhere evicts the copy a few milliseconds later. It is a package you add next to StackExchange.Redis, not a replacement for it: your existing multiplexer keeps doing everything it does today, and RedisNearCache opens one extra connection for the tracked reads.

StackExchange.Redis [never picked up client tracking](https://github.com/StackExchange/StackExchange.Redis/issues/1461), and the 3.x rewrite still ships without it. RedisNearCache sits on top of it rather than forking it.

Needs Redis 6 or newer, or Valkey, reached directly. Redis Enterprise-based services (Azure Managed Redis, Redis
Cloud, Redis Software) are supported through `TrackingMode.Broadcast` (see below). Garnet does not implement
`CLIENT TRACKING` and ElastiCache Serverless is not supported.

## Install

```sh
dotnet add package RedisNearCache
```

Add `RedisNearCache.HybridCache` as well if you want it behind `HybridCache` or `IDistributedCache`.

## Use

Redis or Valkey reached directly (self-hosted, ElastiCache node-based, Azure Cache for Redis):

```csharp
services.AddRedisNearCache("localhost:6379");
```

Redis Enterprise-based services (Azure Managed Redis, Redis Cloud, Redis Software), whose proxy needs the
broadcast mode described below:

```csharp
services.AddRedisNearCache("my-cache.region.redis.azure.net:10000,ssl=true,password=<access-key>", o =>
{
    o.TrackingMode = TrackingMode.Broadcast;   // default is Redirect, for Redis reached directly
    o.KeyPrefixes.Add("user:");                 // invalidations are broadcast per prefix, so set one
});
```

Everything after registration is the same in both modes.

```csharp
var cache = provider.GetRequiredService<IRedisNearCache>();
await cache.Ready;                                     // subscription up, tracking armed on every master

var user = await cache.GetAsync<User>("user:42");      // miss: one GET, tracked, stored locally
var again = await cache.GetAsync<User>("user:42");     // hit: no network call
var users = await cache.GetManyAsync<User>(["user:1", "user:2", "user:3"]);   // misses share a round trip (256 keys at a time), hits are served locally

await cache.SetAsync("user:42", user with { Name = "Ada" });   // writes through, evicts the local copy
var created = await cache.SetAsync("user:42", user, When.NotExists);   // StackExchange.Redis.When: write only if absent; false if it exists
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

## Redis Enterprise, Azure Managed Redis, Redis Cloud

```csharp
services.AddRedisNearCache("my-cache.region.redis.azure.net:10000,ssl=true,password=<access-key>", o =>
{
    o.TrackingMode = TrackingMode.Broadcast;
    o.KeyPrefixes.Add("product:");
});
```

Their proxy rejects tracking on RESP2 and rejects `REDIRECT` under RESP3, so the default `Redirect` mode cannot
arm there. `Broadcast` opens its own small RESP3 connection per master, arms it with `CLIENT TRACKING ON BCAST
PREFIX` for each `KeyPrefixes` entry, and feeds invalidations from that instead; reads are unchanged.

Entra ID authentication (`Microsoft.Azure.StackExchangeRedis`) works with `Broadcast`, including in-place
re-authentication of a live connection when the token rotates:

```csharp
var cfg = ConfigurationOptions.Parse("my-cache.region.redis.azure.net:10000");
await cfg.ConfigureForAzureWithTokenCredentialAsync(new DefaultAzureCredential());   // Microsoft.Azure.StackExchangeRedis

services.AddRedisNearCache(o =>
{
    o.Configuration = cfg;
    o.TrackingMode = TrackingMode.Broadcast;
    o.KeyPrefixes.Add("product:");
});
```

## Metrics and health checks

`IRedisNearCache.Statistics` and a `System.Diagnostics.Metrics` meter (`RedisNearCacheStatistics.MeterName`) expose the
same counters, plus L1 entry count and coherence as gauges, tagged with each instance's Redis client name. A health
check reports `Degraded` (not `Unhealthy`) while the cache is in pass-through, since reads still succeed straight
from Redis.

```csharp
services.AddOpenTelemetry().WithMetrics(m => m.AddMeter(RedisNearCacheStatistics.MeterName));
services.AddHealthChecks().AddCheck<RedisNearCacheHealthCheck>("redis-near-cache");
```

## How it stays correct

- The private connection is armed with `CLIENT TRACKING ON REDIRECT <subscriber> NOLOOP` on every master.
- An interactive or subscriber reconnect re-arms that node and flushes the local cache; the cache serves straight from Redis until every master is armed again.
- A read whose key was invalidated while the reply was in flight is not cached.
- If tracking cannot be armed at all, every read goes to Redis and nothing is cached, ever, until it can.

## Docs

Quickstart, sequence diagrams, configuration, API reference, the HybridCache adapter, operations guide, benchmarks and FAQ:
**https://magna-nz.github.io/redis-near-cache/**

Source and issues: https://github.com/magna-nz/redis-near-cache

## License

MIT
