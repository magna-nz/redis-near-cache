# Changelog

## 0.1.0 (unreleased)

First release.

- `RedisNearCache`: server-assisted client-side caching (Redis `CLIENT TRACKING`) on top of
  StackExchange.Redis 3.2.0, with a private RESP2 multiplexer, per-node arming, re-arm on both reconnect
  types, background retry, in-flight race guard, `NOLOOP` for own writes, prefix opt-in, and a pass-through
  mode while any node is unarmed.
- `RedisNearCache.HybridCache`: `IDistributedCache` / `IBufferDistributedCache` adapter and
  `AddRedisNearCacheHybridCache`.
- 32 integration, chaos and adapter tests against Redis 7.4 standalone and a 3-master cluster.
- Benchmarks and a zero-traffic demonstration under `bench/`.
