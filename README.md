# RedisNearCache

Server-assisted client-side caching for .NET, layered on StackExchange.Redis 3.2.0.

RedisNearCache keeps an in-process copy (an "L1") of the Redis values your application reads, and lets
the Redis server itself tell you when one of them changes, using the `CLIENT TRACKING` feature Redis 6
introduced. There is no cooperation required from writers: any client, in any language, doing a plain
`SET` or `DEL` (or `redis-cli` by hand) causes Redis to push an invalidation message, and RedisNearCache
evicts the stale entry. This is different from an application-level pub/sub backplane (the kind
FusionCache and the proposed HybridCache backplane use), where only writers built with the same library
publish invalidations, so a writer in another language or process silently leaves readers stale. Server-
assisted tracking moves the responsibility to the one place every write already passes through: Redis
itself. RedisNearCache opens its own private connection to do this; your application's existing
`IConnectionMultiplexer` is never touched, reconfigured, or depended on.

See [docs/how-it-works.html](docs/how-it-works.html) for diagrams of the mechanism, the reconnect
handling, and the cluster case.

## Requirements

| Requirement | Detail |
|---|---|
| Redis server | Redis 6 or newer, or Valkey (any version that implements `CLIENT TRACKING`). |
| Garnet | Not supported. Garnet does not implement `CLIENT TRACKING`. |
| .NET | .NET 8 or .NET 10. |
| StackExchange.Redis | 3.2.0 (pinned; see `Directory.Packages.props`). |
| Your own connection | No special requirements. Any protocol (RESP2 or RESP3), no admin mode needed. |
| RedisNearCache's private connection | Opened by RedisNearCache itself, forced to RESP2 with admin mode (`AllowAdmin=true`) and its own client name. This is required for `CLIENT TRACKING` and `CLIENT LIST`, and because StackExchange.Redis 3.x swallows RESP3 invalidation push frames. |

## Quick start

```bash
dotnet add package RedisNearCache
```

Register the cache and resolve it from DI:

```csharp
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache;

var services = new ServiceCollection();
services.AddRedisNearCache("localhost:6379");

await using var provider = services.BuildServiceProvider();
var cache = provider.GetRequiredService<IRedisNearCache>();

// Wait for the invalidation subscription and initial arming to complete.
await cache.Ready;

await cache.SetAsync("user:42", new { Name = "Ada" });

var user = await cache.GetAsync<User>("user:42");     // miss: reads Redis, populates L1, arms tracking
var again = await cache.GetAsync<User>("user:42");    // hit: served from L1, no network call

await cache.RemoveAsync("user:42");

if (cache.TryGetLocal<User>("user:42", out var local))
{
    // local is only set if the key is currently cached in L1; Redis is never touched.
}

Console.WriteLine(cache.Statistics);                  // hits=1 misses=1 invalidations=0 flushes=0 rearms=0 raceDiscards=0

record User(string Name);
```

You can also configure options explicitly:

```csharp
services.AddRedisNearCache(options =>
{
    options.ConnectionString = "localhost:6379";
    options.KeyPrefixes.Add("user:");
    options.L1MaxAge = TimeSpan.FromMinutes(5);
});
```

## How it works

RedisNearCache opens one private StackExchange.Redis multiplexer, cloned from your connection settings
with `Protocol=Resp2`, `AllowAdmin=true` and a unique client name. Under RESP2 that multiplexer has two
connections per Redis node: an **interactive connection**, used for the actual `GET`/`SET` calls, and a
**subscriber connection**, which RedisNearCache tells Redis to redirect invalidations to
(`CLIENT TRACKING ON REDIRECT <subscriber id>`). The subscriber subscribes to `__redis__:invalidate` and
turns each incoming message into an eviction from L1.

Tracking is **one-shot per key**: after Redis sends an invalidation for a key, it forgets that key was
being tracked until the connection reads it again. Every `GetAsync<T>` miss re-arms tracking for that key
by reading it over the tracked interactive connection.

Because an invalidation can arrive between sending a `GET` and storing its reply, RedisNearCache keeps an
in-flight guard: each read records a version token before the request goes out, and the reply is only
stored in L1 if no invalidation for that key was seen in the meantime. A stale reply is still returned to
the caller (it was correct when read); it is simply not cached, and `Statistics.RaceDiscards` counts it.

Writes made through RedisNearCache's own `SetAsync`/`RemoveAsync` are armed with `NOLOOP`, so Redis does
not echo them back as invalidations; RedisNearCache evicts its own L1 entry for the key directly around the
write instead.

## Options

`RedisNearCacheOptions`, configured via `AddRedisNearCache`:

| Property | Default | What it does |
|---|---|---|
| `Configuration` | `null` | `ConfigurationOptions` for the Redis deployment. RedisNearCache clones this and forces `Protocol=Resp2`, `AllowAdmin=true` and its own `ClientName`. Either this or `ConnectionString` must be set. |
| `ConnectionString` | `null` | Alternative to `Configuration`; parsed with `ConfigurationOptions.Parse`. |
| `KeyPrefixes` | empty | Key prefixes that RedisNearCache will cache locally. Reads of keys outside these prefixes still go through RedisNearCache to Redis but are not stored in L1, so they cost nothing to invalidate. Empty (the default) means every key read through RedisNearCache is cached. |
| `L1SizeLimit` | `10_000` | Maximum number of entries held in L1. Least-recently-used entries are evicted beyond this. |
| `L1MaxAge` | 5 minutes | Safety net: an L1 entry is dropped after this age even if no invalidation arrived. Protects against a missed invalidation. Set to `Timeout.InfiniteTimeSpan` to disable. |
| `Serializer` | `JsonRedisNearCacheSerializer.Instance` | Serializer for values. Defaults to `System.Text.Json`; `string` and `byte[]` are passed through untouched. |
| `ClientNamePrefix` | `"rnc"` | Prefix for the Redis client name RedisNearCache sets on its own connections. A unique suffix is appended. |

## Reconnects

Tracking is state Redis holds per connection, so a reconnect of either of RedisNearCache's own connections
(on any node) invalidates that state:

| Event | What Redis does | What RedisNearCache does |
|---|---|---|
| Interactive connection reconnects | Server drops tracking entirely (`CLIENT TRACKINGINFO` reports `flags=off`, `redirect=-1`). | Re-issues `CLIENT TRACKING ON REDIRECT` on that node, then flushes L1. |
| Subscriber connection reconnects | Server keeps redirecting to the now-dead old client id; invalidations are silently lost. | Looks up the new subscriber client id via `CLIENT LIST`, re-issues `CLIENT TRACKING OFF` then `ON REDIRECT <new id>`, then flushes L1. |
| Cluster topology change (a master added/rediscovered) | Slots may have moved to the new master. | Arms the new master, then flushes L1 (`ArmReason.TopologyChanged`); masters already armed are left alone. |
| Any connection failed but not yet restored | Nothing can be trusted. | Flushes L1 and serves every read straight from Redis (no caching) until that node is re-armed. |

Every re-arm after the very first one, and every "tracking lost" event, flushes L1 completely: anything
invalidated during the gap would otherwise be lost. `Statistics.Rearms` counts each re-arm issued after the
initial pass, and `Statistics.Flushes` counts every whole-cache flush (a null invalidation/`FLUSHDB`, a
lost connection, or a re-arm). On a cluster, each master node is armed and re-armed independently: losing
one node's connections flushes L1 (nothing can be trusted while any node is unarmed) but only that node is
re-armed.

## HybridCache and IDistributedCache

```bash
dotnet add package RedisNearCache.HybridCache
```

```csharp
services.AddRedisNearCache("localhost:6379");
services.AddRedisNearCacheHybridCache();
```

This registers `RedisNearCacheDistributedCache` as both `IDistributedCache` and `IBufferDistributedCache`,
backed by the `IRedisNearCache` that `AddRedisNearCache` already registered, and then calls `AddHybridCache`
so that `Microsoft.Extensions.Caching.Hybrid.HybridCache` uses RedisNearCache as its distributed tier.
Use `AddRedisNearCacheDistributedCache()` alone if you only want the `IDistributedCache`/`IBufferDistributedCache`
adapters (for session state, output caching, etc.) without `HybridCache`.

`HybridCache` keeps its own in-process L1 in front of whatever `IDistributedCache` it is given, and that L1
knows nothing about Redis invalidations: only RedisNearCache's own L1 is evicted when the server invalidates
a key. To avoid two L1s where only one is coherent, `AddRedisNearCacheHybridCache` sets
`HybridCacheOptions.DefaultEntryOptions.LocalCacheExpiration` to **10 milliseconds** by default (unless your
`configure` callback overrides it). `TimeSpan.Zero` cannot be used (`HybridCache`'s underlying `MemoryCache`
throws for a non-positive relative expiration), and the true smallest positive `TimeSpan` (one tick) is not
reliable in practice: `HybridCache` persists a newly-computed value to the distributed cache in the
background rather than awaiting it, so a second `GetOrCreateAsync` immediately afterwards can race that
still-in-flight write and re-run the factory even though nothing invalidated the entry. Ten milliseconds was
enough margin for that background write to reliably land first in repeated local testing; it still bounds
how long a value can survive in `HybridCache`'s own untracked L1 to something small relative to
`L1MaxAge`'s five-minute default.

**Sliding expiration limitation.** Redis TTLs, and RedisNearCache's `SetAsync`, have no notion of a sliding
window. `DistributedCacheEntryOptions.SlidingExpiration` is mapped to a plain absolute expiry equal to the
sliding window, applied once at write time, and is never extended by a later read. `Refresh` and
`RefreshAsync` are no-ops for this reason: the Redis TTL set at write time is authoritative until the key
expires, is overwritten, or is deleted. Callers that need a true sliding window must re-`Set` on each access
themselves.

## Limitations

- **RESP3 push tracking is not used.** StackExchange.Redis 3.x collapses RESP3 to a single connection and
  swallows the invalidation push frames on it, so RedisNearCache always forces RESP2 for its own connection.
- **No `OPTIN`/`OPTOUT` tracking mode.** `CLIENT CACHING YES` must be sent immediately adjacent to the next
  command on the wire, which is not guaranteed on a multiplexed connection. Selection of what gets cached is
  done in the library instead, via `KeyPrefixes`.
- **Garnet is not supported.** Garnet does not implement `CLIENT TRACKING`.
- **TTL expiry is only reflected in L1 once Redis actually expires the key.** Redis's active expiry cycle,
  not the TTL deadline itself, is what triggers the invalidation push; there can be a gap between a key's TTL
  elapsing and Redis noticing and sending the invalidation. `L1MaxAge` is the safety net for this and for any
  other missed invalidation: an L1 entry is dropped after that age regardless.
- **Per-write invalidation cost on hot keys.** Every write to a tracked key causes Redis to push an
  invalidation to every reader that had it tracked; a very hot key can mean a lot of pushes. Use
  `KeyPrefixes` to opt only the keys you actually want cached into L1, so writes to everything else cost
  nothing extra.
- **`Ready` only faults if no master could be armed at all.** If some masters armed and others did not,
  `Ready` completes, but the cache stays in pass-through (every read goes to Redis, nothing is stored in L1)
  until every master is armed. Unarmed masters are retried in the background every 5 seconds. Watch
  `Statistics.Hits`: it stays flat while the cache is in pass-through.

## Performance

Numbers below are quoted from `bench/RedisNearCache.Bench/README.md`; see that file for full methodology.
Both modes need a Redis server reachable at `localhost:6379` (the repo's `docker compose` container).

**Zero-traffic demo** (`--demo`): seeds a key, reads it once through the near cache (a miss), then reads it
1,000 more times and checks the server's `cmdstat_get` counter delta:

```text
1000 reads of 'demo:near-cache:zero-traffic' through the near cache (all should be L1 hits):
  cmdstat_get calls before : 263325
  cmdstat_get calls after  : 263325
  delta (Redis GETs issued): 0  (expected 0)

Write-to-eviction latency  : 1688.0 µs

Statistics: hits=1000 misses=1 invalidations=1 flushes=0 rearms=0 raceDiscards=0
```

All 1,000 repeated reads were served from L1 with zero Redis traffic; the one external write (from a
second, separate multiplexer standing in for another client) evicted the local copy in 1688.0 µs, which is
the invalidation push's round trip: write, Redis, `__redis__:invalidate`, subscriber, L1 evicted.

**BenchmarkDotNet suite**, run on an Apple M4 Pro against a local single-node Redis container
(`Toolchain=InProcessEmitToolchain`, `RunStrategy=Monitoring`, 10 iterations):

| Method | Kind | Mean | Allocated |
|---|---|---:|---:|
| Plain_StringGet | String | 173.717 us | 1360 B |
| NearCache_Hit | String | 1.604 us | 2144 B |
| NearCache_Miss | String | 198.658 us | 9088 B |
| NearCache_TryGetLocal | String | 1.325 us | 2072 B |
| Plain_StringGet | Json | 276.979 us | 5456 B |
| NearCache_Hit | Json | 3.967 us | 424 B |
| NearCache_Miss | Json | 280.596 us | 6472 B |
| NearCache_TryGetLocal | Json | 2.875 us | 352 B |

`NearCache_Hit` and `NearCache_TryGetLocal` are roughly **100x faster** than a plain Redis round trip in this
run, because they never leave the process. `NearCache_Miss` lands close to (and, for the string case, a
little above) `Plain_StringGet`, since both do a real Redis round trip; the near cache's overhead on the
miss path is locking, in-flight bookkeeping, and deserialization. `RunStrategy.Monitoring` with only 10
iterations means run-to-run variance is higher than a long `Throughput` run would show; this is a deliberate
trade-off to keep the whole suite finishing in well under a minute.

## Development

Start the containers Redis needs:

```bash
docker compose up -d          # standalone Redis 7.4 on localhost:6379
./cluster-up.sh                # 3-master cluster on 127.0.0.1:7100-7102
```

Build and test:

```bash
dotnet build
dotnet test tests/RedisNearCache.Tests
```

Integration tests expect both containers to be running.

Project layout:

```text
src/RedisNearCache/                 core library: contracts, private connection, tracking armer,
                                     invalidation listener, L1 cache, the facade, and DI registration
src/RedisNearCache.HybridCache/      IDistributedCache / IBufferDistributedCache / HybridCache adapters
tests/RedisNearCache.Tests/          integration tests and Chaos/ (reconnects, cluster, races, stress)
bench/RedisNearCache.Bench/          zero-traffic demo and BenchmarkDotNet suite
samples/MinimalApi, samples/Worker   runnable sample applications
docs/how-it-works.html              mechanism diagrams
```

## License

MIT. See [LICENSE](LICENSE).
