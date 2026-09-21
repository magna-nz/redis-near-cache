# Changelog

## Unreleased

- Feature: `redisnearcache.flushes` and `redisnearcache.rearms` now carry a `reason` tag and emit one series per
  reason (5 for `flushes` - `ServerFlush`, `Rearm`, `TrackingLost`, `EndpointRemoved`, `Manual`; 7 for `rearms` -
  `InteractiveRestored`, `SubscriptionRestored`, `Manual`, `TopologyChanged`, `Recovered`, `VerificationFailed`,
  `PushConnectionRestored`) instead of one. Summed across `reason` the totals are numerically identical to
  before, so a dashboard aggregating with `sum`/`rate` is unaffected; a dashboard that assumed exactly one series
  per `rnc.client_name` for either instrument will now see several and needs a `sum by (rnc.client_name)` (or
  equivalent) added. `ArmReason.Initial` and `ArmReason.Promoted` do not appear on `rearms`: they are arms, not
  re-arms, and were never counted towards `Statistics.Rearms` either.
- Feature: seven new instruments on the `RedisNearCache` meter: `redisnearcache.pass_through.seconds` (gauge,
  `s`, 0 while coherent, otherwise seconds since coherence was lost, counted from construction so an instance
  that never manages its first arm reports a growing duration instead of looking healthy),
  `redisnearcache.endpoints.lost` (gauge, masters whose tracking is currently lost),
  `redisnearcache.ttl_cap.abandoned` and `redisnearcache.untracked_reads.unavailable` (gauges, 0/1, the
  `RespectServerTtl` and `CLIENT CACHING NO`-in-a-transaction latches, previously logged once and then invisible
  for the life of the process), `redisnearcache.serializer_failures` (counter; the exception still reaches the
  caller unchanged), `redisnearcache.l1.store_refusals` (counter; stores `MemoryCache` refused for a reason other
  than the value alone exceeding the size budget - it cannot distinguish that from the `MemoryCache` size-drift
  bug documented on `L1Cache`, so a non-zero reading is a symptom to investigate, not a diagnosis on its own) and
  `redisnearcache.prearm_failures` (counter; failed attempts to pre-arm a replica ahead of a failover). Matching
  new properties on `RedisNearCacheStatistics`; see DESIGN.md's Observability section for why none of this adds
  a write to the read path.
- Feature: the health check's `Data` grew from 8 keys to 15 (one per `Statistics` member above), plus a 16th,
  `lostEndpoints` (the lost endpoints' addresses, comma-joined), present only when the health check is wired to
  this library's own cache and absent for a caller's own `IRedisNearCache` implementation. `Healthy`/`Degraded`
  semantics are unchanged; the check still never returns `Unhealthy`.
- `RedisNearCacheStatistics.ToString()` gained the new fields, appended after the existing ones; the documented
  prefix (`hits=... misses=... invalidations=...`) is unchanged.
- Selected `TrackingArmer` log lines around replica pre-arm changed level; see the source for the current,
  authoritative levels rather than this entry.

## 1.6.0 (2026-09-20)

- Fix: on a deployment with more than one master, the cache could store a value that nothing was tracking for the
  whole of its start. The masters are armed concurrently and each raises its own `Armed(Initial)`; the first of them
  settled startup, so reads stopped waiting for `Ready` and were cached again - including a read routed to a master
  whose `CLIENT TRACKING ON` had not been sent yet. No invalidation can arrive for such an entry and no flush follows
  (`Armed(Initial)` does not flush), so whatever was written to the key afterwards was not served until the entry
  reached its TTL or `L1MaxAge`. The facade now caches nothing until the whole start sequence has returned. Both
  modes, every released version, clusters and Sentinel deployments with several masters; a single-master deployment
  was never affected. Reproduced against a three-master cluster with one node paused.
- Fix: a cache built while Redis was unreachable (`abortConnect=false`) never cached again for the life of the
  process. The invalidation subscription threw, so the tracking armer was never started: it hooked no connection
  event and ran no sweep, and a start runs once, so nothing armed the cache when Redis appeared - `IsCoherent` stayed
  false and every read went to Redis, with no error after the first. The facade now retries the subscription every
  5 s and starts the armer once it succeeds. `Ready` still reports the start the application saw.
- Fix: `TrackingArmer` skipped its 5 s reconcile sweep and its replica pre-arm entirely when the initial arm threw
  (no connected master at startup). Those are what arm a master that connects later without an event naming it, so a
  deployment whose masters all appeared after the cache was built stayed in pass-through. `BroadcastTracker` already
  started its sweep in a `finally`; `TrackingArmer` now does too.
- Fix: a `Reconcile` that found several new masters queued each arm as it went, so the first `Armed` could take the
  facade out of pass-through while the others were still unannounced. All of them are now announced lost first.
- Fix: in `TrackingMode.Redirect`, a subscriber connection that died silently was not noticed, and L1 kept serving
  values the server could no longer invalidate while `IsCoherent` stayed true. The subscriber connection only ever
  receives, so it is what a NAT or load balancer drops as idle, and a half-open socket raises no event: the server
  goes on redirecting every invalidation to a client that is gone. Measured against a proxy that swallowed that one
  flow: about 67 s of silent stale reads, and unbounded (466 of 466 reads stale over 260 s, 29 reconnect attempts,
  not one `ConnectionFailed`/`ConnectionRestored` raised) when the reconnect could not complete either. Two changes:
  the private multiplexer's `KeepAlive` is now 10 s rather than the 60 s default, so StackExchange.Redis notices a
  silent socket in about 20 s and reconnects; and every sixth sweep (about 30 s) now re-checks that each armed master
  is still redirecting to the client id of our subscriber connection as `CLIENT LIST` reports it, re-arming when it is
  not, so the case where no event ever arrives ends in pass-through in about 30 s instead of lasting indefinitely.
  Not on every sweep: the server answers `CLIENT LIST` by walking every client it has. A failed read
  of either command is not treated as a broken arm, so a blip costs nothing. Remaining window, documented in
  DESIGN.md: while the server still believes the subscriber is there, which neither end can see, staleness is bounded
  by the keepalive rather than eliminated. `Broadcast` mode was never affected (it has its own 10 s keepalive).
- Fix: `AddRedisNearCacheDistributedCache` / `AddRedisNearCacheHybridCache` now displace an `IDistributedCache`
  registered before them. Both used `TryAdd`, which skips when any descriptor for the service type exists, so an
  `AddDistributedMemoryCache()` earlier in `Program.cs` (ASP.NET Core session-state boilerplate, itself a `TryAdd`)
  kept the registration while `AddRedisNearCacheHybridCache` still switched `HybridCache`'s own local cache off. The
  result was `HybridCache` with no local tier over a process-local L2 - slower than either tier alone, incoherent
  across processes, and with nothing logged to say so. Ordering between this library's own named and unnamed forms is
  unchanged (the first one called still decides which cache backs the interfaces); a registration made *after* ours
  still wins, which is now documented rather than accidental.
- Fix: the default serializer dispatched `Serialize` on the runtime value and `Deserialize` on the static type, so
  the two disagreed. `SetAsync<string>(key, null)` wrote the four bytes `null` and `GetAsync<string>` handed back the
  four-character string `"null"` - a value the caller never stored; `SetAsync<byte[]>(key, null)` likewise. Both now
  throw `ArgumentNullException`: `string` and `byte[]` pass through untouched, so there is nothing in Redis that means
  null (store nothing, or remove the key). And `Serialize<object>("abc")` wrote raw bytes that `Deserialize<object>`
  could only throw on; `object` now takes the JSON path in both directions. A `null` of a JSON-serialized type is
  still stored as the JSON literal and still reads back as null, unchanged.
- Fix: `CLIENT TRACKINGINFO` reporting tracking off is now treated as a failed arm even when it still reports the
  expected redirect id.
- Options validation: a `KeyNamespace` containing `{` or `}` is refused (on a cluster it is a hash tag, so every key
  the cache touches would land in one slot - the hazard was documented on the property but nothing enforced it), as
  is one with leading or trailing whitespace; a whitespace-only `KeyPrefixes` entry is refused alongside null and
  empty (in `Broadcast` it would go out as a `BCAST PREFIX` argument); and `L1SizeLimit` is no longer validated when
  `L1SizeLimitBytes` is set, since it is ignored then - a zero there used to fail startup over a setting nothing reads.
- Docs: `RedisNearCacheDistributedCache` no longer offers itself for ASP.NET Core session state. `Refresh` is a no-op
  and a sliding expiration becomes a fixed TTL measured from the write, so a session that is only read would expire a
  full `IdleTimeout` after its last write and sign the user out mid-visit. The expiry mapping itself is unchanged and
  was already tested. Real Redis answers `redirect -1` once tracking is off, so this only bites behind something
  that answers differently, but the check cost nothing to add.

## 1.5.0 (2026-09-20)

- Fix: in `TrackingMode.Redirect`, a second arm of one node could turn its tracking off while the cache was serving
  from it. A node restart restores both of the cache's connections and each queues an arm; the first one's `Armed`
  answered every loss announced so far, so the cache was caching again (`IsCoherent` true) when the second arm opened
  with `CLIENT TRACKING OFF`. For two or three round trips, or the whole retry ladder (about 2.4 s plus timeouts) if
  that arm then failed, L1 served keys the server no longer tracked, with nothing marking it. An arm now announces
  the loss again once it is its turn and, under the lifecycle lock, immediately before every `OFF`, where it also
  forgets any pre-arm of that node: a promoted replica whose arm sent `OFF` and then failed used to be reported as
  `Promoted` (armed, no re-arm needed) by the next reconcile, with its tracking off and no time limit. An arm on its
  own still costs one flush. Every released version is affected; `Broadcast` mode is not.
- Fix: the 5 s reconcile queued a new arm, a flush and a log line for any connected master missing from its redirect
  targets, including one whose arm was still running and one that cannot be armed at all (a proxy without `REDIRECT`,
  a restricted ACL user), on top of the retry loop doing the same job. It now skips a master with an arm queued or
  running, and the timer's sweep leaves a lost master to its retry loop. A `ConfigurationChanged` still arms a lost
  master at once, so failover recovery is no slower.
- Tests: an `Auth` integration suite, run on every push against every supported server (Redis 6.2, 7.0, 7.2, 7.4, 8,
  Valkey 8.1) and, for `Broadcast`, the Redis Enterprise proxy. New containers from `auth-up.sh` (part of `up.sh`): a
  password-protected server with ACL users, a server that requires client certificates, a password-protected cluster,
  and a password-protected Sentinel deployment with sentinels that share the data nodes' password, have none, or
  have a different one; `enterprise-up.sh` gains a password-protected database. Every "works" case is proven end to
  end (arm, L1 hit, an external write evicting it); every unsupported one is proven to fail loudly and promptly.
  What it established: `password=` alone works in both modes, including `HELLO 3 AUTH default <password>` through the
  Enterprise proxy; least-privilege ACL users work in both modes, standalone and on a cluster, with the server's
  `ACL LOG` empty afterwards; mutual TLS works in `Redirect` however the certificate is supplied and in `Broadcast`
  through `SslClientAuthenticationOptions`, and with the certificate only on the `CertificateSelection` event
  `Broadcast` faults `Ready` and stays in pass-through; sentinels without a password in front of password-protected
  data nodes work, and a sentinel password different from the data nodes' cannot be expressed (one `Password`, as for
  a plain `ConnectionMultiplexer`).
- Tests: Entra ID. `Auth/EntraIdTests.cs` drives the real `Microsoft.Azure.StackExchangeRedis` extension (a
  test-only dependency) with a stand-in `TokenCredential` against a local server whose ACL user is the token's object
  id and whose password is the token. Both tracking modes work; every connection the cache opens authenticates as
  that user; and on a token rotation the live `Broadcast` connection is re-authenticated in place (same connection,
  no `TrackingLost`, no flush, invalidations still delivered) while the extension re-authenticates the private
  multiplexer, shown by removing the old token from the server and killing the multiplexer's connections. This was
  "verified by hand, not by CI" until now. There is no Azure tenant in CI: the mechanism is proven, not the service.
- Docs: the ACL permissions reference now shows the two least-privilege users the suite runs, in place of the untested
  illustration, which was too small to connect with. About half of what is needed is StackExchange.Redis's own
  (`INFO`, `ECHO`, `CONFIG GET`, `SELECT`, `CLIENT ID`, `CLUSTER SLOTS`, `READONLY`, from Redis 7.2 `CLIENT SETINFO`,
  its tie-breaker key and its configuration channel), and for this library `RemoveAsync` is `UNLINK`, not `DEL`, and
  `SetAsync` with an expiry is `SETEX` or `PSETEX`. The README has an Authentication section; the mutual-TLS
  limitation says what happens and where it is tested; a Sentinel-password limitation is added.

## 1.4.0 (2026-09-20)

- Feature: `RedisNearCacheOptions.KeyNamespace` (`string?`, default `null`), a prefix put in front of every key
  given to the cache, so several applications, tenants or cache instances can share one Redis database: with
  `"app1:"`, `GetAsync("user:42")` reads the Redis key `app1:user:42`. The caller's key becomes `namespace + key`
  once, at the cache's edge (the shared read, the shared write, `RemoveAsync`, `EvictLocal` and `TryGetLocal`); L1,
  the in-flight tracker and Redis all work on the full key from there, which is also what the server's
  invalidations carry, so the invalidation path translates nothing and costs nothing extra. Callers keep using
  their own keys everywhere, including as the keys of the dictionary `GetManyAsync`/`GetManyBytesAsync` return.
  Anything that writes to Redis directly (another service, `redis-cli`) must use the full key for its write to
  invalidate the local copy. When adopting it, stop passing full keys: a key that already begins with the namespace
  gets it a second time (`app1:app1:user:42`), which the cache reports once as a warning and otherwise does as
  asked. Log messages name the full key. On a cluster keep `{`...`}` out of the namespace, where it would be a hash
  tag putting every key in one slot. `KeyPrefixes` are relative to the namespace (`"user:"` under `"app1:"` means Redis
  keys starting `app1:user:`), and in `TrackingMode.Broadcast` a namespace with no `KeyPrefixes` arms the server
  with `BCAST PREFIX <namespace>` instead of the whole keyspace, which removes the main cost of Broadcast mode
  with empty prefixes. With a namespace set, a null key throws `ArgumentNullException`. `null` or empty (the
  default) changes nothing: keys go to Redis exactly as given, byte for byte today's behaviour.
- Feature: named instances. `AddKeyedRedisNearCache(name, configure)` and the connection-string overload
  `AddKeyedRedisNearCache(name, connectionString, configure = null)` register a second (third, ...)
  `IRedisNearCache` as a keyed service, alongside the default one from `AddRedisNearCache` or instead of it: its
  own options, its own private connections to Redis, its own L1 and statistics. Resolve it with
  `[FromKeyedServices(name)] IRedisNearCache` or `GetRequiredKeyedService<IRedisNearCache>(name)`. `name` is both
  the service key and the options name, so `IOptionsMonitor<RedisNearCacheOptions>.Get(name)` returns this
  instance's options; those options are validated like the default ones, at host start under `IHost`, and options
  kept under a name that was not registered this way are left alone. Calling this twice with one name registers
  one instance. Metrics of a named instance carry an extra tag, `rnc.instance = <name>`, stable across restarts
  unlike `rnc.client_name`; the default instance's metric series are unchanged. `AddRedisNearCacheDistributedCacheFor(name)`
  and `AddRedisNearCacheHybridCacheFor(name, configure = null)` (in `RedisNearCache.HybridCache`) back the
  application's single `IDistributedCache`/`HybridCache` with a named instance instead of the default one, on the
  same first-registration-wins terms as the unnamed forms. Every named instance opens its own connections (one
  multiplexer plus its subscriber, or its own Broadcast sockets). One thing to know about keyed services in
  general: code that walks the `IServiceCollection` reading `ImplementationType`/`ImplementationFactory` from
  every descriptor (some older scanning or decorator libraries) throws on a keyed one.
- Packaging: `dotnet pack` now runs the .NET SDK's package validation (`EnablePackageValidation`,
  `PackageValidationBaselineVersion` set to `1.3.0` in both `src/RedisNearCache/RedisNearCache.csproj` and
  `src/RedisNearCache.HybridCache/RedisNearCache.HybridCache.csproj`) against the published baseline, so a change
  that removes or alters public API an earlier 1.x had fails the build. Two things follow for contributors: the
  baseline package is downloaded at restore, so a first restore needs nuget.org; and when the version is bumped for
  a release the baseline stays at the last version actually published (raise it only after the new one is out).
- Compatibility: nothing changes for an application that uses neither feature. `IRedisNearCache` is unchanged;
  the existing `AddRedisNearCache*` methods and their registrations are unchanged (the keyed path above is
  separate code); without a `KeyNamespace` keys are not touched or even inspected; and the default instance's
  metrics keep exactly their existing tags.

## 1.3.0 (2026-09-19)

- Fix: under a sustained storm of writes to the same hot keys, L1 could stop storing anything at all, silently and for
  good, while `IsCoherent` stayed `true` and `Statistics` showed no flush, no re-arm and no race discard to explain it
  (only `Misses` climbing and `L1Entries` falling to zero). All earlier versions are affected, on `GetAsync` as much as
  anywhere. It cost performance only - the cache became a pass-through to Redis and never
  served a stale value - and any whole-cache flush (a reconnect, a re-arm, `FLUSHDB`, `EvictAllLocal`) cleared it.
  The cause is in `MemoryCache`'s size accounting (Microsoft.Extensions.Caching.Memory 9.0 through at least 10.0.12;
  dotnet/runtime#129186, a regression from #103931, fixed for 11.0 by #129215, with the 10.0 backport #129510 still
  open): a `Set`
  that finds an entry already there subtracts its size as a replacement, and if that entry is removed at the same
  moment - by an invalidation, the expiry scan, or a lookup that trips over an expired entry - its size is
  subtracted twice. The total only drifts down; once it is below zero the capacity check, done unsigned, refuses every `Set`. `L1Cache` now takes a lock for the key around its
  store and its remove, and removes the key before storing it, so `MemoryCache.Set` never finds an entry to replace
  and the double-count cannot happen. L1 hits stay lock-free; a miss and an invalidation each take one lock, contended
  only on a key several threads are storing or invalidating at once. Measured with stores, removes and expiries
  racing over ~50 million operations: the total ends at exactly zero, where it ended at -9 before (and at -2 with the
  lock alone, which is why the remove comes first). The workaround is independent of the package version a consuming
  application resolves, and can go once the package floor is a build that contains the upstream fix.

- Feature: conditional writes, `SetAsync<T>(key, value, When when, TimeSpan? expiry = null, bool keepTtl = false, CancellationToken cancellationToken = default)`
  and the raw-bytes `SetBytesAsync` equivalent. `When.NotExists`/`When.Exists` map to `SET NX`/`SET XX`,
  `When.Always` is the existing unconditional write; both return `true` if Redis performed the write, `false` if
  the condition was not met and the key is unchanged. `keepTtl: true` keeps the key's existing TTL (`KEEPTTL`)
  instead of clearing it, and cannot be combined with an `expiry` (`ArgumentException`, paramName `keepTtl`,
  thrown before any Redis call). The L1 copy is evicted before and after the write regardless of the outcome,
  exactly like the existing unconditional `SetAsync`/`SetBytesAsync`: a write that turns out not to have happened
  costs one local eviction, never a stale read. Shipped as default interface methods, so an existing
  `IRedisNearCache` implementation still compiles; its inherited default supports only an unconditional write
  without `keepTtl` (by delegating to the old overload) and throws `NotSupportedException` for anything else.
  `when` comes before `expiry` on purpose: after it, an existing call such as `SetAsync(key, value, expiry,
  default)` would be ambiguous between the new overload and the `CancellationToken` one. **Source-compatibility
  note:** one call shape does become ambiguous (CS0121) and needs a one-word edit when recompiling against this
  version: a bare `default` as the third argument, `SetAsync(key, value, default)` or
  `SetBytesAsync(key, value, default)`; write `expiry: null` or leave the argument out. Already-compiled callers
  are unaffected. `When` is `StackExchange.Redis.When`.
- Feature: multi-key reads, `GetManyAsync<T>(IEnumerable<string> keys, CancellationToken cancellationToken = default)`
  returning `ValueTask<IReadOnlyDictionary<string, T?>>`, and the raw-bytes `GetManyBytesAsync` equivalent returning
  `IReadOnlyDictionary<string, byte[]?>`. One entry per distinct key, compared ordinally; a key that does not exist
  in Redis is present with `null`/`default`, exactly as `GetAsync<T>` returns for it, so a value type should be read
  as its nullable form (`GetManyAsync<int?>`) to tell a missing key from a stored zero. Duplicates in `keys` are
  read once, and `Statistics` counts one hit or miss per distinct key. Deliberately not an `MGET`: every key goes
  through the same single-key read as `GetAsync<T>`/`GetBytesAsync`, all started together so the misses share a
  round trip per Redis node, keeping each key's own in-flight race check, TTL cap, `KeyPrefixes` handling and
  cluster slot routing, which an `MGET` across slots would refuse. Reads are issued at most 256 at a time, so a
  very large key list cannot queue tens of thousands of commands at once and time out its own tail. If any read
  fails, the call throws the first failure (in key order) after every read already started has finished; keys read
  successfully by then stay cached. `keys` null throws `ArgumentNullException`; a null element throws
  `ArgumentException`, before any read starts. Shipped as default interface methods built only on
  `GetAsync<T>`/`GetBytesAsync`, so an existing `IRedisNearCache` implementation or decorator still compiles and
  gets a correct multi-key read for free, through its own `GetAsync<T>`.

## 1.2.0 (2026-09-19)

- Feature: a `System.Diagnostics.Metrics` `Meter` named `RedisNearCache` (`RedisNearCacheStatistics.MeterName`),
  one per cache instance and disposed with it. All instruments are observable, read from `Statistics` (or L1)
  only when something collects, so nothing is added to the read path: counters `redisnearcache.hits`,
  `.misses`, `.invalidations`, `.flushes`, `.rearms`, `.race_discards`, and gauges `redisnearcache.l1.entries`
  and `redisnearcache.coherent`. Every measurement is tagged `rnc.client_name` so several instances in one
  process stay distinct. No new package dependency; wire it up with
  `.WithMetrics(m => m.AddMeter(RedisNearCacheStatistics.MeterName))`, or watch it with `dotnet-counters`.
- Feature: `RedisNearCacheStatistics.L1Entries`, the current L1 entry count, also appended to `ToString()` as
  `l1Entries=N`.
- Feature: `RedisNearCacheOptions.L1SizeLimitBytes` (`long?`, default `null`). When set, L1 is bounded by the
  total bytes of the cached values instead of by entry count and `L1SizeLimit` is ignored; a value larger than
  the limit is never cached, but is still returned to the caller. `null` keeps the existing entry-count bound.
- Feature: `RedisNearCacheHealthCheck` (namespace `RedisNearCache`, in the core package) implements
  `IHealthCheck`: `Healthy` while `IsCoherent`, `Degraded` (not `Unhealthy`, since reads still succeed as
  pass-through) otherwise, with the statistics counters in the result's `Data`. Needs one new dependency,
  `Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions`. Register it with
  `services.AddHealthChecks().AddCheck<RedisNearCacheHealthCheck>("redis-near-cache")`; there is deliberately
  no `IHealthChecksBuilder` extension method, since that would need the full HealthChecks package.
- Feature: `AddRedisNearCache` now registers a validator for `RedisNearCacheOptions` with `ValidateOnStart`,
  checking that `Configuration` or `ConnectionString` is set, `L1SizeLimit > 0`, `L1SizeLimitBytes` is `null` or
  `> 0`, `L1MaxAge` is positive or `Timeout.InfiniteTimeSpan`, `ClientNamePrefix` is non-empty and without
  whitespace, `Serializer` is not null, and `KeyPrefixes` has no null or empty entry. **Behaviour change:** a
  misconfigured registration used to throw `InvalidOperationException` the first time `IRedisNearCache` was
  resolved; it now throws `OptionsValidationException`, at host startup under `IHost` rather than on first
  resolve.
- Fix: `IBufferDistributedCache.TryGetAsync` on `RedisNearCacheDistributedCache` now copies the stored bytes
  straight into the caller's `IBufferWriter<byte>`, one copy instead of two. No API change.
- Fix: `GetAsync`/`GetBytesAsync` now observe an already-cancelled token even when the key is already in L1.
- Fix: `EvictLocal`/`EvictAllLocal` are now no-ops after the cache is disposed; they used to reach the disposed
  `MemoryCache`.
- Packaging: symbol packages (`.snupkg`) are now published alongside the NuGet packages.
- Docs: `Ready`'s doc comment and the Limitations list said a faulted start left the cache in pass-through
  permanently. The armer keeps retrying every 5 s in the background regardless of whether `Ready` faulted; when
  it succeeds the cache leaves pass-through and serves from L1 again. `Ready` itself stays faulted; use
  `IsCoherent`/`WaitForCoherenceAsync` to observe recovery.
- Docs: noted that a null `expiry` on `SetAsync`/`SetBytesAsync` issues a plain `SET`, which clears any TTL the
  key already had.
- Docs: corrected the `L1SizeLimit` description: `MemoryCache` compacts in the background by priority then
  least-recently-used, so while L1 is full a new entry may not be stored until compaction has run (the read
  still returns the value); this is not strict LRU eviction on every write.
- Docs: added an ACL permissions reference (Operations) listing the commands, and the channel and key
  permissions, a restricted user needs in each tracking mode.
- Docs: added Limitations entries for mTLS on the `Broadcast` connection (`ConfigurationOptions.CertificateSelection`/
  `CertificateValidation` are events the broadcast socket cannot read; supply client certificates through
  `SslClientAuthenticationOptions`), for Dragonfly, KeyDB, AWS MemoryDB and Google Cloud Memorystore
  being untested, and for Redis 6.0/6.1 being untested (CI starts at 6.2).

## 1.1.0 (2026-09-17)

- Fix (both tracking modes): a master that stopped being one is now forgotten, which lets the cache serve from L1
  again, only once every master serving now is tracked. Before, the reconcile forgot a master that was demoted while
  connected (`CLUSTER FAILOVER`, Sentinel) or that restarted as a replica straight away. In `TrackingMode.Redirect`,
  the retry loop and a re-arm that found a replica did the same. Meanwhile the promoted master could still be
  untracked, so writes to it produced no invalidations and values read in that window could go stale. In `Redirect`,
  a pre-armed replica counts as tracked, and a demoted master that is still connected keeps its tracking while it waits.
- Fix: a newly found master is marked lost before its arm is queued, so a read routed to it is never stored before
  its tracking is on.
- Fix (cluster): StackExchange.Redis leaves nodes flagged `fail` out of its `CLUSTER NODES` view, so between a master
  failing and its replica's promotion the view looked as if that master had lost its slots. A view whose masters do
  not serve all 16384 slots now keeps the endpoint a master. **Behaviour change:** in a cluster that leaves slots
  unassigned on purpose, a master that stops being one is never forgotten; the wait is logged at Warning.
- Tests: new cluster integration tests for a demotion while connected (Broadcast) and a kill with a fast restart
  (Redirect).

## 1.0.1 (2026-09-16)

- Fix (`TrackingMode.Broadcast`, cluster): after a master failed over, the killed master was forgotten as soon as its
  slots had moved, which let the cache serve from L1 again while the promoted replica had no tracking connection yet.
  Writes to it produced no invalidations, so values read in that window could go stale. The window was about a second
  in testing and unbounded until StackExchange.Redis noticed the promoted node's new role at its periodic check. The
  killed master is now forgotten only once every master serving slots has an armed connection; until then the cache
  stays in pass-through, the private multiplexer is reconfigured so the promotion is seen at once, and a wait that
  lasts is logged at Warning naming the masters still unarmed.
- Tests: the cluster failover test no longer predicts which replica is promoted or reads a replica pairing that other
  nodes have not yet learned, which made it flaky in CI; it now also checks the ordering above.

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
