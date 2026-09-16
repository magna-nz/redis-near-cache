# Changelog

## 1.0.0 (2026-09-16)

- Fix: `EvictLocal` now marks the key for any read already in flight, as `SetAsync`, `RemoveAsync` and an invalidation
  do. Before, it only removed the L1 entry, so a read whose reply was on the wire when `EvictLocal` ran stored that
  reply straight back.
- Feature: `IRedisNearCache.GetBytesAsync` and `SetBytesAsync` read and write the bytes as stored in Redis, without
  the configured `Serializer`. `GetBytesAsync` returns a copy; `SetBytesAsync` copies the caller's buffer.
- Fix: the `IDistributedCache` / `IBufferDistributedCache` adapter now uses those members. It used to go through
  `GetAsync<byte[]>` / `SetAsync<byte[]>`, so with a custom serializer the bytes were re-encoded on the way in and out,
  and other clients of the same keys saw the encoded form.
- Breaking, for anyone implementing `IRedisNearCache` themselves: the two members above are new abstract members. The
  interface is now documented as not meant to be implemented outside the library, and it may gain members in a minor
  release.

## 0.8.0 (2026-09-16)

- Fix: when neither `PTTL` nor the typed TTL can be answered, the cap is now abandoned after three misses in a row
  rather than the cache. A miss with no cap is served but not stored, so a lasting failure used to leave the instance
  reading through to Redis for those keys indefinitely, silently: no counter moved and the cache still reported itself
  coherent. Giving up the cap is what `RespectServerTtl=false` does, invalidations still evict, and it is reported at
  Warning. A single success resets the run, so a blip changes nothing. This is what made `ReshardDuringReadsNoStale`
  fail in about half of CI runs after a slot moved; the reshard test now also captures the library's own log lines so a
  recurrence names the underlying error.
- Docs: the README carries one diagram covering both tracking modes instead of a separate one per mode; the
  platform table drops its `Tested` column; the `Redis Enterprise` and `How it stays correct` sections are gone from
  the README and every link that pointed at them now points at the documentation site, which already covered both.
  The three `works with` badges are one badge, and `supports EntraID` became `auth: ACL · Entra ID · mTLS`, which is
  what the library actually accepts. On the documentation site, six lines that read as machine-written were cut or
  rewritten, including a note to the author that was being published to readers.

## 0.7.0 (2026-09-16)

- Fix: a miss whose `PTTL` failed served the value but cached nothing, and did so silently and for as long as the
  failure lasted, because `RespectServerTtl` will not store an entry without a cap. The raw `PTTL` goes out as an
  `Execute`, which is not redirected the way a keyed command is, so on a resharding cluster it can fail for a key
  whose slot has moved while the `GET` beside it succeeds; that key then stayed uncacheable. The miss now falls
  back to the typed TTL once, which is routed and redirected like any other keyed command, and only when that
  fails too is the value served uncached, reported once at Warning rather than per read. Covered by
  `TypedTtlAnswersWhenTheRawPttlCannotBeRouted`; it is what made `ReshardDuringReadsNoStale` fail intermittently
  in CI, where a migrated key was read 1,471 times without ever being cached.
- Feature: Entra ID (`Microsoft.Azure.StackExchangeRedis`) token rotation is honoured in `TrackingMode.Broadcast`
  while a broadcast connection is live, not just on its next reconnect. RedisNearCache clones the caller's
  `ConfigurationOptions`, and the clone shares the extension's token provider: StackExchange.Redis resolves
  `User`/`Password` through `ConfigurationOptions.Defaults`, which `Clone()` copies by reference. The private
  multiplexer is created from that same provider, so the extension registers it through its `AfterConnectAsync` hook
  (the hook was seen to fire for a clone-built multiplexer on StackExchange.Redis 3.2.0; the re-authentication itself
  is the extension's documented behaviour) and re-authenticates it exactly as it does any multiplexer it manages;
  every broadcast connection reads the current object id and token when it connects; and a live broadcast connection
  now compares its current credentials against the provider on every keepalive tick (10 s, up to about 15 s if a
  `PING` is in flight) and, when they changed, re-authenticates in place with `AUTH <objectId> <token>` — no
  `TrackingLost`, no L1 flush, tracking untouched. If the server rejects the rotated credential the connection keeps
  the one it has (a failed `AUTH` leaves a Redis connection's authentication unchanged) and stays armed, the rejected
  pair is not retried until the provider yields another, and a connection the server closes at token expiry is
  re-armed with an L1 flush like any reconnect. The same mechanism generalises to any rotating credential supplied
  through a custom `DefaultOptionsProvider` subclass whose `User`/`Password` overrides change (ACL password rotation,
  for example); values set directly on `ConfigurationOptions.User`/ `Password` are static and shadow the provider, so
  use the provider for credentials that rotate. Covered by three new integration tests in `CredentialRotationTests`
  and two unit tests in `CredentialProviderCloneTests` (Broadcast suite, CI, OSS Redis with ACL users rotated under a
  live cache): in-place re-authentication keeps the client id, tracking and L1; a reconnect after rotation uses the
  current credentials; and a rejected credential keeps the socket armed until a good one arrives.
- Feature: `RedisNearCacheOptions.TrackingMode`, an enum defaulting to `Redirect` (today's behaviour, unchanged), with
  a new `Broadcast` value for Redis Enterprise-based services (Azure Managed Redis, Redis Cloud, Redis Software),
  whose proxy rejects tracking on RESP2 (`ERR Client tracking is not supported when using RESP2`) and rejects
  `REDIRECT` under RESP3, and whose `CLIENT LIST` does not show our subscriber connection, so the redirect design
  cannot arm there. In `Broadcast`, RedisNearCache opens one small RESP3 connection of its own per master (TLS and
  auth taken from the same connection settings as the private multiplexer, client name `<private client name>-bcast`),
  sends `HELLO 3` and `CLIENT TRACKING ON BCAST PREFIX <p>` for every entry of `KeyPrefixes`, and receives an
  invalidation push for every write or delete under those prefixes, whether or not this instance holds the key;
  `FLUSHDB`/`FLUSHALL` arrive as the null invalidation and flush L1. Reads still go through the private multiplexer,
  unchanged. Replicas are not pre-armed in this mode. If the broadcast connection dies, the cache goes pass-through
  for that endpoint, reconnects with the same fast-then-5-second retry ladder as `Redirect`, re-arms, and flushes L1,
  exactly as the existing "any reconnect = re-arm then flush" rule says; the private multiplexer's `ConnectionFailed`
  for a master is treated as loss of that node's broadcast connection too, the tracking handshake on a new socket has
  a `SyncTimeout` deadline, a killed cluster master is retired by the same `CLUSTER NODES` probe as `Redirect`, and
  `CLIENT TRACKINGINFO` is used to verify the arm where the server has it (6.2+) and assumed where it does not. The
  `CLIENT CACHING NO` transaction for keys outside `KeyPrefixes` is skipped in `Broadcast`, since the reading
  connection is not tracked and a plain `GET` is already untracked. `NOLOOP` does not apply (the broadcast connection
  never writes), so this instance's own `SetAsync`/`RemoveAsync` echo back as pushes: harmless for coherence, but a
  read issued right after a write can be discarded once by the in-flight rule and is cached by the read after it.
  Costs versus `Redirect`: `KeyPrefixes` should be set (empty means every write in the database is broadcast to this
  client, logged once as a warning), invalidation volume scales with the write rate under the prefixes rather than
  with what L1 holds, and the server's per-key tracking table is not used, so the Enterprise `tracking_table_max_keys`
  limit does not apply. Also works on OSS Redis 6+ and Valkey, but `Redirect` stays the default there because it only
  pushes for keys this instance actually read. The broadcast connection reads the current user and password from the
  cloned connection settings at every connect, and a live connection is now re-authenticated in place when they rotate
  — see the Entra ID feature bullet above. In `Redirect` mode, the startup error raised when no subscriber connection
  is found on an endpoint now suggests setting `TrackingMode.Broadcast` in case the endpoint is a Redis
  Enterprise-based service. Covered by two new suites: `RedisNearCache.Tests.Broadcast` (Broadcast mode against OSS
  Redis, runs in every integration job) and `RedisNearCache.Tests.Enterprise` (runs against `enterprise-up.sh`'s
  container in a new CI job, "integration on Redis Enterprise (Redis Software in Docker)").
- Infra: `enterprise-up.sh` brings up a single-node Redis Software (Redis Enterprise) container behind its proxy on
  `localhost:12000`, headless (`rladmin cluster create` + REST API), as the local stand-in for Azure Managed Redis,
  Redis Cloud and Redis Software. Not part of `up.sh` (boots in ~4 minutes, Redis documents the image as dev/test
  only). Probing that proxy established what the docs only imply: RESP2 tracking is rejected outright
  (`ERR Client tracking is not supported when using RESP2`), `REDIRECT` is a syntax error under RESP3, and the
  library today fails even earlier because `CLIENT LIST` through the proxy does not list the multiplexer's subscriber
  connection. RESP3 default per-key tracking and `BCAST PREFIX` tracking both work through the proxy, including
  `NOLOOP`, `CLIENT CACHING NO` inside `MULTI`, and the null invalidation on `FLUSHDB`; `BCAST` pushes are delivered
  to a connection that never reads.
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
