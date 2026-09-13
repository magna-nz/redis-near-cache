# Samples

Two runnable samples that show `IRedisNearCache` (and, for `MinimalApi`, the `HybridCache` adapter) being
invalidated by something *outside* the process that's holding the near cache.

Both connect to `localhost:6379`, so start the standalone Redis container first, from the repo root:

```
docker compose up -d
```

## MinimalApi

An ASP.NET Core minimal API (`samples/MinimalApi`) with:

- `GET /products/{id}` — reads through `IRedisNearCache.GetAsync<Product>("product:{id}")`; on a miss it falls
  back to an in-memory `ProductRepository` (a fake data store with a simulated 50 ms lookup delay) and then
  `SetAsync`s the result back into Redis.
- `PUT /products/{id}` — writes a `Product` via `SetAsync`.
- `GET /stats` — returns `IRedisNearCache.Statistics` (hits, misses, invalidations, flushes, rearms,
  raceDiscards) as JSON.
- `GET /hybrid/{id}` — the same read, but through `Microsoft.Extensions.Caching.Hybrid.HybridCache` (backed by
  RedisNearCache via `AddRedisNearCacheHybridCache()`).

Run it on a free port:

```
~/.dotnet/dotnet run --project samples/MinimalApi --urls http://localhost:5199
```

From a second terminal, curl it a couple of times, then invalidate the key from *outside* the app (a plain
`redis-cli SET`, nothing that goes through this process) and curl again — the third call reflects the change
because the server pushed an invalidation over `CLIENT TRACKING` and evicted the app's L1 entry:

```
curl http://localhost:5199/products/1
curl http://localhost:5199/products/1
curl http://localhost:5199/stats
docker exec redis-near-cache-redis redis-cli SET product:1 '{"id":1,"name":"changed"}'
curl http://localhost:5199/products/1
curl http://localhost:5199/stats
```

### Captured output

Run against the live `redis-near-cache-redis` container, with `product:1` pre-seeded to
`{"id":1,"name":"Widget"}` via `redis-cli` before starting the app (so the first `curl` is a plain read-through
rather than exercising the repository fallback, which is demonstrated by the very first request against a
genuinely empty key in normal use):

```
=== GET /products/1 (1st) ===
{"id":1,"name":"Widget"}
HTTP 200

=== GET /products/1 (2nd) ===
{"id":1,"name":"Widget"}
HTTP 200

=== GET /stats ===
{"hits":1,"misses":1,"invalidations":0,"flushes":0,"rearms":0,"raceDiscards":0}
HTTP 200

=== external write ===
docker exec redis-near-cache-redis redis-cli SET product:1 '{"id":1,"name":"changed"}'
OK

=== GET /products/1 (3rd, after external write) ===
{"id":1,"name":"changed"}
HTTP 200

=== GET /stats (final) ===
{"hits":1,"misses":2,"invalidations":1,"flushes":0,"rearms":0,"raceDiscards":0}
HTTP 200
```

The 2nd call is an L1 hit (`hits` goes from 0 to 1, `misses` stays at 1). After the external `redis-cli SET`,
the 3rd call already returns `"changed"`: `invalidations` goes from 0 to 1 (the server pushed one), and the
GET after it counts as a miss again because the near cache had to go back to Redis to re-populate L1 for that
key. Note: because `SetAsync` (used by `PUT /products/{id}` and by the repository-fallback path on
`GET /products/{id}`) evicts L1 rather than populating it — see `DESIGN.md`, "Not in v1" — the read
immediately after a write is always a miss; the *next* read after that is what gets served from L1.

Kill the process afterwards (`Ctrl+C`, or `pkill -f samples/MinimalApi`).

## Worker

A `BackgroundService` (`samples/Worker`) that polls the key `config:feature-flags` through the near cache
every 500 ms and logs whether that tick was an L1 hit or a Redis miss, plus a full `Statistics` snapshot every
5 seconds.

```
~/.dotnet/dotnet run --project samples/Worker
```

From a second terminal, change the watched key at any point and watch the very next tick pick it up:

```
docker exec redis-near-cache-redis redis-cli SET config:feature-flags '{"beta":true}'
```

### Captured output

Run against the live `redis-near-cache-redis` container. `config:feature-flags` did not exist yet, so every
tick is a miss (RedisNearCache never stores a null value in L1, so a missing key stays a miss on every poll)
until the external `redis-cli SET` partway through, after which the key is a hit on every following tick:

```
RedisNearCache Worker sample starting.
It polls the key 'config:feature-flags' every 500 ms through the near cache.
From another terminal, change the watched key and watch the very next tick pick it up as a miss:
  docker exec redis-near-cache-redis redis-cli SET config:feature-flags '{"beta":true}'

info: RedisNearCache.Internal.RedisNearCacheConnection[0]
      RedisNearCache connecting private multiplexer rnc-a116ac8f9c0548a9b3d8475254b43a3b to Unspecified/localhost:6379 (RESP2, admin)
info: RedisNearCache.Tracking.InvalidationListener[0]
      RedisNearCache subscribed to __redis__:invalidate as client rnc-a116ac8f9c0548a9b3d8475254b43a3b
info: Microsoft.Hosting.Lifetime[0]
      Application started. Press Ctrl+C to shut down.
info: RedisNearCache.Tracking.TrackingArmer[0]
      RedisNearCache armed CLIENT TRACKING on Unspecified/localhost:6379 redirecting to client 3562 (Initial)
info: RedisNearCache.Samples.Worker.ConfigPollingWorker[0]
      MISS config:feature-flags = <null>  (cumulative hits=0 misses=1)
      ... (17 more MISS ticks while the key does not exist yet) ...
info: RedisNearCache.Samples.Worker.ConfigPollingWorker[0]
      Statistics: hits=0 misses=11 invalidations=0 flushes=0 rearms=0 raceDiscards=0
      ... (docker exec redis-near-cache-redis redis-cli SET config:feature-flags '{"beta":true}' run here) ...
info: RedisNearCache.Samples.Worker.ConfigPollingWorker[0]
      MISS config:feature-flags = {"beta":true}  (cumulative hits=0 misses=19)
info: RedisNearCache.Samples.Worker.ConfigPollingWorker[0]
      hit  config:feature-flags = {"beta":true}  (cumulative hits=1 misses=19)
info: RedisNearCache.Samples.Worker.ConfigPollingWorker[0]
      hit  config:feature-flags = {"beta":true}  (cumulative hits=2 misses=19)
info: RedisNearCache.Samples.Worker.ConfigPollingWorker[0]
      hit  config:feature-flags = {"beta":true}  (cumulative hits=3 misses=19)
info: RedisNearCache.Samples.Worker.ConfigPollingWorker[0]
      Statistics: hits=3 misses=19 invalidations=1 flushes=0 rearms=0 raceDiscards=0
info: RedisNearCache.Samples.Worker.ConfigPollingWorker[0]
      hit  config:feature-flags = {"beta":true}  (cumulative hits=4 misses=19)
info: RedisNearCache.Samples.Worker.ConfigPollingWorker[0]
      hit  config:feature-flags = {"beta":true}  (cumulative hits=5 misses=19)
info: Microsoft.Hosting.Lifetime[0]
      Application is shutting down...
```

The first tick after the `redis-cli SET` (`misses=19`) is still a miss — that's the read that goes to Redis
and populates L1 — but it already returns the new value, `{"beta":true}`. Every tick after that is a hit
(`hits` climbs, `misses` stays at 19) until the key is written again from outside.
