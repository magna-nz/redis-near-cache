# Changelog

## 0.5.1 (2026-09-14)

No library changes: `RedisNearCache` and `RedisNearCache.HybridCache` are functionally identical to 0.5.0.

- CI: packages are published only from a GitHub Release (a tag push no longer races it), and a release whose
  tag disagrees with `<Version>` fails before publishing.
- Tests: the Sentinel failover tests tolerate client-side thread-pool starvation during the failover
  (StackExchange.Redis's Sentinel reconnect blocks pool threads) and re-issue a failover that Sentinel itself
  aborts. The test assembly raises the minimum worker threads to 32; `RNC_TEST_MIN_WORKER_THREADS=0` keeps the
  runtime default.

## 0.5.0 (2026-09-14)

- Fix: a Sentinel master that is killed no longer keeps the cache in pass-through forever. An unreachable node
  now stops counting as a master once another master is connected, or, in a cluster, once it owns no slots.
  Endpoints are matched by host and port, so hostname-announcing clusters work.
- Fix: losing or removing an endpoint we had armed always flushes L1, even if the multiplexer already flags it
  as a replica. After a graceful Sentinel failover, a value read from the demoted master could previously
  be served stale.
- Fix: a re-arm that finds the node demoted to replica forgets it (flush, leave pass-through) instead of
  returning silently.
- Tests: 20 new unit tests. 8 new Docker-backed integration tests (101 total): Sentinel (arming, invalidation,
  graceful and `kill -9` failover), plus managed-style emulation (ElastiCache-style disabled admin commands, a
  hostname-announcing cluster) and an opt-in `RNC_EXTERNAL_REDIS` test against a real managed endpoint.
- CI: every integration job checks that each suite actually ran, dumps container logs on failure and times out
  after 45 minutes. `ci-local.sh` runs the same checks locally.

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
