# Changelog

## 0.4.0 (2026-09-14)

- Cluster failover: the private multiplexer now checks topology every 5 s, so a promoted master is armed
  within seconds instead of about 60 s after a failover.
- Fix: a faulted startup no longer re-sets the degraded flag on every read, so the background re-arm
  actually restores caching.
- Fix: own writes always run their second invalidation mark, even when the write throws; caching is
  re-enabled only after the re-arm flush; caching is gated for the whole duration of any non-initial arm.
- Fix: replica connection events are ignored and endpoints that leave the cluster are forgotten, so
  neither can pin the cache in pass-through.
- HybridCache adapter: HybridCache's own local cache is disabled via `HybridCacheEntryFlags.DisableLocalCache`
  (merges per call) instead of a short `LocalCacheExpiration` that per-call options overrode.
- Tests: 30 unit tests and 93 Docker-backed integration tests (chaos, edge cases, compat, resilience),
  run in CI on Redis 6.2, 7.0, 7.2, 7.4, 8 and Valkey 8.1.
- Docs site, load test, package description and README rewritten.

## 0.1.0 (2026-09-14)

First release.

- `RedisNearCache`: server-assisted client-side caching (Redis `CLIENT TRACKING`) on top of
  StackExchange.Redis 3.2.0, with a private RESP2 multiplexer, per-node arming, re-arm on both reconnect
  types, background retry, in-flight race guard, `NOLOOP` for own writes, prefix opt-in, and a pass-through
  mode while any node is unarmed.
- `RedisNearCache.HybridCache`: `IDistributedCache` / `IBufferDistributedCache` adapter and
  `AddRedisNearCacheHybridCache`.
- 32 integration, chaos and adapter tests against Redis 7.4 standalone and a 3-master cluster.
- Benchmarks and a zero-traffic demonstration under `bench/`.
