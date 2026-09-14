# RedisNearCache

Client-side caching on top of StackExchange.Redis. Keeps the values you read in memory and drops them the moment Redis says they changed.

Redis 6 can tell a client when a key it has read changes (`CLIENT TRACKING`). RedisNearCache uses that to keep a local copy of what you read: a hit is served from memory, and a write from anywhere evicts the copy a few milliseconds later. It is a package you add next to StackExchange.Redis, not a replacement for it: your existing multiplexer keeps doing everything it does today, and RedisNearCache opens one extra connection for the tracked reads.

StackExchange.Redis [never picked up client tracking](https://github.com/StackExchange/StackExchange.Redis/issues/1461), and the 3.x rewrite still ships without it. RedisNearCache sits on top of it rather than forking it.

Needs Redis 6 or newer, or Valkey. Garnet does not implement `CLIENT TRACKING`, and neither Redis
Enterprise-based services (Azure Managed Redis, Redis Cloud, Redis Software) nor ElastiCache Serverless are
supported.

## Install

```sh
dotnet add package RedisNearCache
```

Add `RedisNearCache.HybridCache` as well if you want it behind `HybridCache` or `IDistributedCache`.

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
