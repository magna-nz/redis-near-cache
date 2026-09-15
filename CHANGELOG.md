# Changelog

## Unreleased

- Fix: a replica pre-arm sweep requested while another sweep was still running (e.g. a configuration change arriving
  right after a sweep disarmed a replica whose replication link was down) was silently dropped, so the replica was
  only re-armed by the next 5 s topology check. Such a request now causes one more sweep as soon as the running one
  finishes; sweeps still never run concurrently, and any number of requests during one sweep coalesce into a single
  follow-up. This also removes the intermittent failure of `SweepDisarmsAPreArmedReplicaWhoseLinkIsDown`, whose rig
  has no periodic topology check; `ConfigurationChangeDuringARunningSweepRunsAnotherSweep` reproduces it
  deterministically by holding a sweep on its `CLIENT TRACKING OFF`.
- Docs: `AddRedisNearCacheHybridCache` no longer claims that HybridCache's background write of a factory result can
  only cause "an occasional extra factory call, never a stale read". That write can land after a newer `SetAsync` or
  `RemoveAsync` from another caller and put the old value back in Redis, where every instance serves it until the entry
  expires or the key is written again (a cache-aside lost update, which `CLIENT TRACKING` cannot invalidate). The XML
  remarks and docs now describe it, why the adapter does not refuse the write, and how to bound it; the benchmark's
  multi-second `NearCacheHybridCache` stale tail is attributed to it; `HybridCacheWriteBackRaceTests` reproduces it.
- Performance: the L1 hit path no longer takes a lock. Every read checked whether any master's tracking was lost with
  `ConcurrentDictionary.IsEmpty`, which acquires all of the dictionary's locks when it is empty, i.e. always in the
  steady state. The lost-endpoint set is now mutated under a private lock that publishes its size to a volatile
  counter, and reads compare that counter to zero. The ordering rules are unchanged: a loss stops L1 being read or
  populated before the flush, and an arm or removal re-enables it only after. On an Apple M4 Pro against a local Redis
  7.4 (4 instances × 2 readers, 400 foreign writes/s, 2,000 keys) reads went from 2.43 M/s to 7.29 M/s, local-hit
  latency p50 / p99 from 2.3 / 3.9 µs to 0.3 / 1.9 µs, and a single-threaded BenchmarkDotNet hit from 223 ns to
  172 ns; 0 stale local entries after quiescence in every run. In the 20-instance, 160-reader benchmark matrix, reads
  at 0 ms went from 1.35 M/s to 2.20 M/s, and the rate of reads served stale in the window between a foreign write's
  acknowledgement and its invalidation being handled rose from 0.07 % to 0.33 % (stalest 76 ms → 105 ms), most likely
  because readers that used to park on the lock now keep every core busy, so invalidations are handled later; the
  4-instance run, with cores to spare, saw its stale-read rate fall (0.023 % → 0.014 %). The benchmark tables in
  README.md, bench/RedisNearCache.Bench/README.md and the docs were refreshed from that run.
- Benchmarks: a comparison of RedisNearCache with plain StackExchange.Redis, `IMemoryCache` with a TTL, `HybridCache`
  with a Redis L2, FusionCache with the Redis backplane, and `HybridCache` over the RedisNearCache adapter. The load
  test (`--load`) now takes `--contender` and `--write-mode foreign|api`, measures stale reads and their age against a
  per-key version registry, and writes JSON; new BenchmarkDotNet (`--bdn`, out-of-process) and Sailfish
  (`bench/RedisNearCache.Bench.Sailfish`) per-call suites; `bench/run-matrix.sh` runs everything against a dedicated
  container with injected latency and renders a report. Results and how to run them:
  [bench/RedisNearCache.Bench/README.md](bench/RedisNearCache.Bench/README.md). Not run in CI.

## 0.5.2 (2026-09-14)

- Feature: `RespectServerTtl` option (default `true`). On every miss, `PTTL` is pipelined with the `GET` (one
  extra command, no extra round trip) and the L1 entry's expiration is capped at whichever is shorter, that or
  `L1MaxAge`. Redis's active-expiry cycle only pushes an invalidation once it actually deletes an expired key,
  which on a large keyspace can lag the TTL deadline by minutes; with this on, a value is never served locally
  after its TTL has elapsed. A key found already gone between the `GET` and the `PTTL` is returned but not
  cached, and counts as a `RaceDiscard`.
- Feature: reads of keys outside `KeyPrefixes` (once `KeyPrefixes` is non-empty) are now sent as `CLIENT CACHING
  NO` immediately followed by `GET`, inside a `MULTI`/`EXEC` so the two stay adjacent on the wire, so the server
  no longer tracks them either. The private connection is armed `OPTOUT` instead of plain `ON`.
- Fix: every connected replica is now pre-armed with `CLIENT TRACKING ON ... OPTOUT NOLOOP` ahead of any
  failover. A replica is never read from, so pre-arming costs nothing; from the moment it is promoted, reads
  routed to it are already tracked, closing the window (previously up to 5 s, until the next topology check)
  during which a freshly promoted master served untracked reads. A promotion found already pre-armed is
  reported as `ArmReason.Promoted`: no re-arm and no pass-through gap, one L1 flush for the entries read from the
  demoted master. A pre-armed replica whose connection fails, or whose replication link goes down, is re-armed
  by a later reconcile sweep once it is a connected replica again.
- Feature: `IRedisNearCache.IsCoherent` and `WaitForCoherenceAsync(CancellationToken)` expose directly whether
  every master is armed and L1 is being read and populated, instead of inferring pass-through from statistics.
  Breaking for code that implements `IRedisNearCache` itself (test doubles, decorators): these two and
  `EvictAllLocal()` are new abstract members.
- Feature: `IRedisNearCache.EvictAllLocal()` drops every L1 entry through the same flush path as a whole-cache
  invalidation, for operations Redis does not push an invalidation for, such as `SWAPDB`.
- Fix: arming now wraps a `RedisServerException` from `CLIENT TRACKING ON REDIRECT` with a message naming Redis
  Enterprise-based services and ElastiCache Serverless, instead of surfacing the bare server error.
- Docs: corrected the OPTIN/OPTOUT limitation (OPTOUT plus `CLIENT CACHING NO` inside a `MULTI`/`EXEC` works
  fine on a multiplexed connection; only OPTIN would need to decide before the read), the managed-services
  limitation (ElastiCache Serverless and every Redis Enterprise-based tier of Azure Managed Redis, Redis Cloud
  and Redis Software cannot run RedisNearCache at all, per each service's own documentation), and the
  cluster-failover limitation (replica pre-arming, not just the 5 s topology check, is what closes the
  staleness window).
- Tests: new integration tests for the TTL cap, opt-out untracked reads and replica pre-arm across a cluster
  failover, plus unit tests for the pre-arm bookkeeping.

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
