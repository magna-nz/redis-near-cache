# Design

RedisNearCache gives a .NET application a near cache whose entries are invalidated by the Redis server
itself, using the `CLIENT TRACKING` feature that Redis 6 introduced. It is layered on StackExchange.Redis
without forking it. The design decisions come from a spike (`../redis-client-tracking-spike`), summarised
where they matter below.

## Mechanism

```
user code ──► IRedisNearCache ──► L1 (MemoryCache)   miss ──► private multiplexer (RESP2, admin) ──► Redis
                                          ▲ evict                          │ subscriber connection(s) = REDIRECT targets
                                          └────────── __redis__:invalidate ◄┘
user writes ──► user's own multiplexer / any client ──► Redis ──► invalidation pushed to our subscriber
```

1. `RedisNearCacheConnection` opens a **private** `ConnectionMultiplexer` from a clone of the caller's
   options with `Protocol=Resp2`, `AllowAdmin=true` and a unique `ClientName`.
2. `TrackingArmer`, per master node: `CLIENT LIST` → find our subscriber connection (name + `P` flag) →
   `CLIENT TRACKING OFF` → `CLIENT TRACKING ON REDIRECT <subscriber id> OPTOUT NOLOOP` on that node's interactive
   connection. Every connected replica whose replication link is up is armed ahead of time the same way (no
   preceding `OFF`, followed by `ROLE` on the same connection to confirm it is still a replica), so a failover
   promotion needs no re-arm. A pre-armed replica whose link goes down is disarmed at the next 5 s sweep: a full
   resync empties its keyspace, and the server reports that to tracking clients as the null invalidation a
   `FLUSHDB` sends, which would flush L1 for nothing.
3. `InvalidationListener` subscribes to `__redis__:invalidate`; each message names one key (or is null = flush).
4. `L1Cache` stores values read through the private multiplexer; reads of tracked keys served locally.
5. Only keys read through RedisNearCache are tracked, because only its connection is tracked. When `KeyPrefixes`
   is non-empty, a read of a key outside it is preceded by `CLIENT CACHING NO` inside a `MULTI`/`EXEC` (so the
   two commands stay adjacent on the wire), which stops the server tracking it as well as keeping it out of L1.

## Why these choices (from the spike)

| Decision | Reason |
|---|---|
| Private multiplexer, not the caller's | `CLIENT TRACKING` and `CLIENT LIST` need `allowAdmin`; RESP3 (the 3.x default) swallows invalidation pushes; and every read on a tracked connection is tracked, which is only right for cache reads. |
| RESP2 | Only RESP2 gives StackExchange.Redis a separate subscriber connection to redirect to. |
| Redirect to the multiplexer's own subscriber | Works standalone and on every cluster node; the library routes the message to our handler by channel name even on nodes where the channel is not subscribed. |
| Re-arm on both reconnect types | Interactive reconnect: server drops tracking (flags off). Subscriber reconnect: new client id, server still redirects to the dead one, invalidations lost silently. |
| Flush L1 on every re-arm | Anything invalidated during the gap was lost. |
| Flush L1 when an endpoint is lost or removed | Its tracking may be gone with no invalidation ever sent (see Endpoint lifecycle). |
| In-flight set | An invalidation can arrive between sending GET and storing the reply. Reply is discarded if the key was invalidated meanwhile. |
| `OPTOUT`, not `OPTIN` | `CLIENT CACHING YES`/`NO` only needs to be adjacent to the next command, which a `MULTI`/`EXEC` guarantees even on a multiplexed connection (verified on Redis 7.4 and through StackExchange.Redis 3.2). `OPTOUT` keeps the common, cacheable read a single `GET`; `OPTIN` would wrap every cacheable read in a four-command transaction instead. |
| No Garnet | Garnet does not implement `CLIENT TRACKING`. Valkey and Redis 6+ do. |
| Broadcast on an own RESP3 socket, not RESP3 on the multiplexer | StackExchange.Redis 3.x consumes RESP3 invalidate pushes internally; BCAST decouples tracking from the reading connection so reads can stay on the multiplexer. |

## Components and ownership

| Component | Path | Depends on |
|---|---|---|
| Contracts | `src/RedisNearCache/Abstractions/` | — |
| `TrackingArmer`, `MasterRole`, `InvalidationListener` | `src/RedisNearCache/Tracking/` | `RedisNearCacheConnection` |
| `BroadcastTracker`, `Resp3Connection`, `Resp3Reader` (Broadcast mode) | `src/RedisNearCache/Tracking/Broadcast/` | `RedisNearCacheConnection`, `MasterRole` |
| `L1Cache`, `RedisNearCache` facade | `src/RedisNearCache/Caching/` | armer + listener events |
| `AddRedisNearCache` | `src/RedisNearCache/DependencyInjection/` | all of the above |
| HybridCache / IDistributedCache adapters | `src/RedisNearCache.HybridCache/` | `IRedisNearCache` |
| Integration and chaos tests | `tests/RedisNearCache.Tests/` | docker containers |
| Benchmarks | `bench/` | — |

**Named instances.** `AddKeyedRedisNearCache(name, ...)`, in the same `DependencyInjection/` folder, registers a
second (third, ...) `IRedisNearCache` as a keyed service: its own connection, armer, listener and cache, entirely
separate code from `AddRedisNearCache`, keyed throughout on `name`. Its options come from
`IOptionsMonitor<RedisNearCacheOptions>.Get(name)`; `RedisNearCacheOptionsValidator.ForName(name)` validates only
that name and skips every other named `RedisNearCacheOptions` an application keeps (the default instance's
validator likewise validates only `Options.DefaultName`). Its `Meter` carries an extra `rnc.instance` tag holding
`name`, stable across restarts unlike `rnc.client_name`. None of this touches the default instance's registration,
options or metrics: an application that never calls `AddKeyedRedisNearCache` sees no difference at all.

**Being the application's `IDistributedCache`.** `AddRedisNearCacheDistributedCache` (and the `HybridCache` form)
displaces an `IDistributedCache` registered before it, rather than standing aside. `TryAdd` skips when any descriptor
for the service type exists, so `AddDistributedMemoryCache()` earlier in `Program.cs` used to keep the registration
while `AddRedisNearCacheHybridCache` still switched `HybridCache`'s own local cache off: `HybridCache` then had no
local tier at all and an L2 that was process-local, which is slower than either tier alone and incoherent across
processes, and nothing said so. Ordering between this library's own named and unnamed forms is unchanged - whichever
runs first decides which cache backs the interfaces - because only a foreign registration is displaced. A
`services.Add` afterwards still wins; nothing at registration time can see the future.

**The serializer's two directions must agree.** `JsonRedisNearCacheSerializer` dispatches on the static type in both
directions. Dispatching `Serialize` on the runtime value instead made `string` and `byte[]` asymmetric with their own
`Deserialize`: a declaration pattern never matches `null`, so a null string was written as the JSON literal and read
back as the four-character string `"null"`. `string` and `byte[]` pass through untouched and so cannot represent
null - Redis holds bytes or holds nothing - so a null of those types is refused outright; a null of a JSON-serialized
type is representable and stays legal.

**Package validation.** Both `src/RedisNearCache/RedisNearCache.csproj` and
`src/RedisNearCache.HybridCache/RedisNearCache.HybridCache.csproj` now set `EnablePackageValidation` and
`PackageValidationBaselineVersion` (`1.3.0`; restored from nuget.org, and only ever raised to a version already published), so `dotnet pack` fails if the build removed or altered public API an
earlier 1.x had.

## Read path (facade)

**Key namespaces.** `RedisNearCacheOptions.KeyNamespace` is applied once, at the five points a caller's key enters
the facade: the shared read, the shared write, `RemoveAsync`, `EvictLocal` and `TryGetLocal` (peek). From there on,
L1, the in-flight tracker and Redis all work on the full key, which is also what the server's invalidations carry,
so the invalidation path never translates anything back. `KeyPrefixes` are relative to the namespace;
`RedisNearCacheOptions.EffectiveKeyPrefixes()` computes the namespaced set once, in the one place both the
facade's own filter and the Broadcast tracker's `BCAST PREFIX` list read it from, because the two must agree.

```
GetAsync<T>(key):
  if L1.TryGet(key) → Statistics.Hit; return
  Statistics.Miss
  inflight.Begin(key)                      // records a version/token
  cacheable = key matches KeyPrefixes (or KeyPrefixes is empty)
  if not cacheable and caching enabled:
    bytes = await privateDb via MULTI / CLIENT CACHING NO / GET / EXEC   // adjacent on the wire: not tracked
  else if cacheable and caching enabled and RespectServerTtl:
    bytes, ttlMs = await privateDb pipelined GET(key) + PTTL(key)        // one round trip
  else:
    bytes = await privateDb.StringGetAsync(key)                          // pass-through: plain, tracked GET
  await TestHooks.AfterRedisReadBeforeStore?(key)
  if bytes is null → inflight.End(key); return default
  if cacheable:
    cap = ttlMs switch { null or -1 → MaxAge, < 0 (vanished between GET and PTTL) → discard, ms → min(MaxAge, ms) }
    if cap is discard or inflight.WasInvalidated(key) → Statistics.RaceDiscard; do not store
    else → L1.Set(key, bytes, cap)
  inflight.End(key)
  return Serializer.Deserialize<T>(bytes)
```

Invalidation events call `inflight.MarkInvalidated(key)` and then `L1.Remove(key)`, in that order. `FlushAll`, any `Armed`
event with a non-Initial reason, every `TrackingLost` and every `EndpointRemoved` flush L1 through the one
flush path (`inflight.MarkAllInvalidated()` first, then `L1.Clear()`), so a read in flight across a flush never
stores its reply.

**L1 and `MemoryCache`'s size accounting.** `L1Cache.Set` and `L1Cache.Remove` take a striped lock for the key,
and `Set` removes the key before storing it. `MemoryCache.Set` (Microsoft.Extensions.Caching.Memory 9.0 through at
least 10.0.12: dotnet/runtime#129186, fixed for 11.0 by #129215, 10.0 backport #129510 open; 8.0 is clean) subtracts the size of an entry it finds already there, as a replacement; if that entry is removed at the
same moment (our invalidation, its expiry scan, a lookup tripping over an expired entry) the size goes twice. The
total only drifts down, and below zero the unsigned capacity check refuses every `Set` for good: reads racing
invalidations on hot keys got there in seconds under load, leaving a coherent-looking cache that stored nothing.
The lock alone does not close it (the scan and `TryGetValue` remove outside it; measured -2 where unlocked gave
-9); removing first does, because `MemoryCache.Set` then never sees an entry to replace (measured 0). Neither half
works alone: without the lock a second store of the key sees the first one's entry. `TryGet` stays lock-free. A
reader between the remove and the set misses (and counts a miss) where it used to see the value being replaced. A
package reference is only a lower bound, and an ASP.NET Core 9/10 application resolves its own 9.x/10.x build, so
the workaround stays until the package floor contains the fix; the storm tests fail at once if it is removed early.

**`L1Cache.Clear` takes every stripe too, in index order, around `MemoryCache.Clear()`.** `Set`'s refusal check
(above) asks whether the entry it just stored is still there; `MemoryCache.Clear()` swaps out the whole backing
collection, so a flush landing between that store and that check would empty the cache and read as a silent
refusal that never happened. Holding every stripe for the clear makes the store and its check atomic with respect
to a flush, which is what keeps the refusal counter exact. The cost is 64 uncontended monitors on a path that
already replaces the backing collection: a flush now waits for any store in flight (nothing under a stripe does
I/O, awaits, or runs a callback - `MemoryCache` has none registered), and a store waits for `Clear`'s walk of the
detached old state. That is a new coupling between the invalidation/push thread and L1 size, but a bounded one,
and rare - a flush is a re-arm, an endpoint removal or a server flush, where a store is per miss. Deadlock-free:
`Set` and `Remove` each take exactly one stripe and never call into `Clear`; `Clear` takes them in one fixed
order; there is no cycle to construct.

**The refusal counter's limits.** `L1Cache.StoreRefusals` cannot tell the size-accounting drift above apart from
a legitimate refusal at the point of the `Set` that was refused: a byte budget genuinely full, with compaction
not yet caught up, looks identical. Under a tight `L1SizeLimitBytes` with churn, a non-zero count can therefore
be entirely benign. The only reliable way to tell them apart is the internal size total, reachable only by
reflection, which this deliberately does not use - so treat a non-zero reading as a symptom to investigate, not
a diagnosis on its own.

It under-counts in the other direction as well, deliberately: `Set` does not look for a refusal at all when the
entry's own lifetime is under 50 ms, because an entry that EXPIRED between the store and the presence check one
statement later cannot be told from one that was refused. That costs nothing real - `L1MaxAge` is minutes by
default, and a `RespectServerTtl` cap landing under 50 ms is a key about to vanish anyway - and it is the cheaper
error: a counter that ticks over every short-lived entry is worse than useless. A pause longer than the lifetime
of an entry just ABOVE the threshold can still be miscounted, and nothing short of `MemoryCache` reporting why a
`Set` did not take would close that.

**Multi-key reads.** `GetManyAsync<T>`/`GetManyBytesAsync` (`Abstractions/Internal/ManyReads.cs`) are the read
path above run once per distinct key, all started before any is awaited, so the misses pipeline onto one round
trip per node - not an `MGET`. An `MGET` would need the in-flight check, the TTL cap and its fallbacks,
`KeyPrefixes` handling and slot routing built again outside the one place they already live, and a cluster refuses
it across slots anyway. The cost of going through `GetAsync<T>`/`GetBytesAsync` instead: up to 2N commands (`GET`
+ `PTTL`) for N uncached keys against one `MGET`, for the same round trips. Reads run in windows of 256
(`ManyReads.Window`) so a very large key list cannot queue tens of thousands of commands at once and time out its
own tail. On failure, the first one (in key order) is thrown only once every read already started has finished, so
none is left unobserved or still holding its in-flight token; keys read successfully by then stay cached. Shipped
as default interface methods built only on `GetAsync<T>`/`GetBytesAsync`, so an implementation or decorator that
predates them still compiles and gets a correct multi-key read for free; the library's own cache overrides both
only so a disposed cache throws `ObjectDisposedException` even for an empty key list.

## Endpoint lifecycle and pass-through

The armer raises three lifecycle events per endpoint; the facade mirrors them in its lost set.

| Event | Raised when | Facade |
|---|---|---|
| `TrackingLost` | a connection of a tracked endpoint (armed, waited on, or a connected master) failed, an arm gave up, or any non-initial arm starts | add to lost set, flush |
| `Armed` | `CLIENT TRACKING ON REDIRECT` verified, or a pre-armed replica promoted (`Promoted`: no re-arm) | flush unless Initial, then remove from lost set |
| `EndpointRemoved` | an endpoint we armed or lost is no longer a master of the deployment, and every master serving now is tracked | flush (always), then remove from lost set |

L1 is read and populated only while the lost set is empty (**pass-through rule**). Every `TrackingLost` is
followed by `Armed` or `EndpointRemoved` for that endpoint; the armer mutates its own lost set and raises under
one lock so the facade sees events in the same order as the armer's state, and the per-endpoint background retry
loop (every 5 s) runs until one of the two happens.

**The start is not covered by the lost set.** Both trackers arm the connected masters concurrently and raise one
`Armed(Initial)` per endpoint, and an initial arm announces no loss first (there is nothing in L1 to protect yet), so
an empty lost set during the start does not mean every master is armed. The facade therefore caches nothing until the
whole start sequence has returned, however many `Armed(Initial)` events arrive meanwhile: otherwise the first of them
would let a read routed to a master whose `CLIENT TRACKING ON` is still in flight be stored, tracked by nobody, with
no flush to follow (`Armed(Initial)` does not flush). Every arm after the start announces its loss first, so from then
on the lost set alone decides. A start that fails (no connected master, or every arm failed) leaves the cache in
pass-through, which the first `Armed` from the armer's own retry loop or reconcile ends; those two paths announce the
loss before arming, so a recovery that overlaps the failure being reported cannot be undone by it. A `Reconcile` that
finds several new masters announces all of them lost before queueing any arm, for the same reason.

**A start whose subscription failed is retried.** With `abortConnect=false` - the documented way to let an
application start before its Redis - the invalidation subscription can fail while the multiplexer has no connection.
The armer is then never started, so it hooks no connection event and runs no sweep, and a start runs once: without a
retry the cache would stay in pass-through for the life of the process. The facade retries the subscription every 5 s
and starts the armer once it succeeds. `Ready` keeps the outcome the application saw and stays faulted; `IsCoherent`
tracks what the cache is actually doing. (In `Broadcast` the listener and the armer are one object, whose own 5 s
sweep arms a master that appears later, so only its failure is recorded.)

**Overlapping arms of one endpoint.** A node restart restores both connections and each `ConnectionRestored` queues
an arm; a gate per endpoint keeps them from interleaving on the wire. One `Armed` answers every loss announced
before it, so by the time the second arm gets the gate the facade is caching from that node again, and the arm opens
with `CLIENT TRACKING OFF`. An arm therefore announces the loss again if the endpoint is no longer lost, twice: once
it holds the gate (a queued arm means some event said tracking there is unreliable), and, under the lifecycle lock,
immediately before every `OFF`, where it also forgets any pre-arm of that node. The pre-arm ends with the `OFF`, and an
arm that then fails must not leave an entry a later reconcile would report as `Promoted`, armed with no re-arm, on a
node whose tracking is off. An arm on its own still costs one flush, not two. The reconcile rarely adds to the queue:
it skips a master that has an arm queued or running (before the promotion check too; the window before a queued arm
registers can still let a second one through, which the re-announcement makes harmless), and the timer's sweep also
leaves a lost master to the retry loop that owns it. A `ConfigurationChanged` is news and still arms a lost master at
once. One window is left: `EndpointRemoved` for a node can land while an arm of that node is between its
re-announcement and its `Armed`, which only matters if the node is in fact still serving reads (the multiplexer's role
view and the probe's disagreeing); it lasts until that arm ends, at most the backoff ladder.

An endpoint we armed counts as tracked **whatever its current replica flag**: in a graceful Sentinel failover the
multiplexer can flag the old master as a replica before Sentinel kills our connections there, and those failures
must still flush. `EndpointRemoved` flushes unconditionally because entries read from a node that is no longer a
tracked master are protected by nothing (its connections may be killed without an invalidation ever arriving).
A re-arm that finds the node is now a replica leaves it lost and raises `EndpointRemoved` once the takeover is
tracked (below), rather than returning silently.

**Forgetting waits for the takeover** (`TakeoverGuard`, shared by both modes). `EndpointRemoved` is what lets the
facade cache again, so it is raised only once every connected master and, in a cluster, every slot-owning master
of a fresh `CLUSTER NODES` view is tracked (in `Redirect`, an armed master or a pre-armed replica), in a view whose
masters serve all 16384 slots; outside a cluster some other master must be connected. Every path that forgets a
non-master goes through this check: the reconcile, a re-arm that finds a replica, and the retry loop. Otherwise a
killed master that restarts as a replica, or whose slots `CLUSTER NODES` already shows moved, is forgotten while
the multiplexer (which relearns a promoted node's role only at its periodic check) has not armed the promoted one,
and after a kill that node is usually not pre-armed any more (its replication link went down, so the sweep disarmed
it). While it waits, the private multiplexer is reconfigured (first round, then every sixth) and a reconcile runs; a
lost endpoint keeps the cache in pass-through, and an armed one that was demoted while connected keeps its tracking,
which still covers what was read from it. A master the reconcile newly finds (and, in `Redirect`, did not pre-arm) is
announced lost on the spot, before its arm is queued, so reads routed to it are never stored before it is armed.

The 5 s reconcile sweep and the replica pre-arm are started even when the initial arm threw, because they are what
arm a master that connects later without an event naming it and what notice a promotion the multiplexer relearned
quietly; a start runs once, so a sweep skipped here would never run at all.

**Verifying an arm nothing reported broken.** Every other path here reacts to a `ConnectionFailed`/`ConnectionRestored`
from StackExchange.Redis. The subscriber connection is the one that never sends anything, so it is the one a NAT or
load balancer drops as idle - and a half-open socket raises nothing: the server keeps redirecting invalidations to a
client that is gone, the interactive connection stays healthy, reads keep being served from L1, and `IsCoherent` stays
true. Measured against a proxy that swallowed that one flow: about 67 s of silent stale reads while the multiplexer's
60 s keepalive got round to it, and unbounded (466 of 466 reads stale over 260 s, 29 reconnect attempts, not one
event raised) when the reconnect could not complete either. Two things close it:

- the private multiplexer's `KeepAlive` is forced to 10 s (as `ConfigCheckSeconds` is forced to 5), so
  StackExchange.Redis itself notices a silent socket in about 20 s rather than 67 s and reconnects, which re-arms;
- every sixth sweep (about 30 s) re-checks every armed master: the server must still be redirecting to the client id
  of our subscriber connection *as `CLIENT LIST` reports it now*, and `CLIENT TRACKINGINFO` (where the server has it,
  and it is only ever read as evidence, never as an instruction) must still agree. A mismatch is announced as
  `TrackingLost` and re-armed, so the case where no event ever comes ends in pass-through in about 30 s instead of
  lasting for ever. Not on every sweep, because the server answers `CLIENT LIST` by walking every client it has, and
  an application with thousands of its own connections would pay for that continuously; `CLIENT LIST TYPE pubsub`, or
  `CLIENT LIST ID` on 6.2+, would make it cheap enough to run more often.

A failure to read either command is never treated as a broken arm: only a positive mismatch re-arms, because a blip
would otherwise cost a flush and a pass-through window for nothing. The endpoint's gate is held for the reads, so an
arm in flight is never mistaken for a broken one. What is left is the window where the server still believes the
subscriber is there - nothing can see that from either end, so it is bounded by the keepalive, about 20 s, and L1 is
served from during it. A pre-armed replica's redirect is not verified this way; if it dangles and that replica is then
promoted, the promotion is reported as `Promoted` (no re-arm) on a node whose redirect is dead, until the next
`TrackingLost` there.

**Still a master?** (`MasterRole`) decides between retrying (stay in pass-through) and forgetting an endpoint.
The multiplexer never changes the role it last saw for a node it cannot reach, so a killed master would otherwise
be a master forever:

- Not listed by the multiplexer, or flagged replica → not a master.
- Connected and not a replica → master.
- Disconnected, not in a cluster → master only while no *other* connected, non-replica data server exists (a
  master that is merely down with no replacement keeps the cache in pass-through; once Sentinel's promoted
  replica is connected, the dead one is forgotten). Checked on every configuration change and by the retry loop.
- Disconnected cluster master → the retry loop reads `CLUSTER NODES` from a connected node: a master only while it
  is listed as a master that owns slots (down but not yet failed over stays a master). StackExchange.Redis leaves a
  node flagged `fail` out of that view altogether, so between the cluster failing a master and promoting its
  replica the view simply lacks it and its slots are served by nobody: a view whose masters do not serve all 16384
  slots keeps the endpoint a master, and the takeover check above refuses such a view too. (Cost: in a cluster that
  leaves slots unassigned on purpose, no master that stops being one - dead, or demoted by a clean failover - is
  ever forgotten: a lost one keeps the cache in pass-through for good, and the wait is logged as a warning every few
  seconds.) Nodes are matched on port
  plus IP, announced hostname, or the resolved addresses of a `DnsEndPoint` — never `EndPoint` equality, because
  against `cluster-preferred-endpoint-type hostname` the multiplexer holds `DnsEndPoint(localhost:7201)` while
  `CLUSTER NODES` yields `127.0.0.1:7201`. No readable view → stays a master.

A graceful cluster failover (`CLUSTER FAILOVER`) keeps our connections: the old master is flagged replica at the
next topology check and removed (flush) once the promoted one is tracked. The promoted one, if it was pre-armed while still a replica, is reported
as `Promoted` (no re-arm, so the reads it served since the failover stay tracked, and no pass-through gap; L1 is
flushed once for the entries read from the demoted master); otherwise it is armed with `TopologyChanged` (flush).

## Broadcast mode (Redis Enterprise-based services)

`TrackingMode.Broadcast` (`RedisNearCacheOptions.TrackingMode`) is for Redis Enterprise-based services (Azure
Managed Redis, Redis Cloud, Redis Software, databases 7.4+): their proxy rejects tracking outright on RESP2
(`ERR Client tracking is not supported when using RESP2`) and rejects `REDIRECT` on RESP3, and its `CLIENT LIST`
does not list our subscriber connection, so the redirect design above cannot arm there. What the proxy does accept
is RESP3 `BCAST PREFIX` tracking on the connection that asks for it: a connection that never reads receives a push
for every write under its prefixes (`FLUSHDB`/`FLUSHALL` arriving as the null invalidation).

RedisNearCache opens one small RESP3 connection of its own per master (TLS and auth taken from the same connection
settings as the private multiplexer, client name `<private client name>-bcast`), sends `HELLO 3` followed by
`CLIENT TRACKING ON BCAST PREFIX <p>` for every entry of `KeyPrefixes`, and feeds the pushes it receives into the
existing invalidation path, whether or not this instance holds the key. Reads stay on the private multiplexer,
unchanged. Replicas are not pre-armed in this mode: there is no redirect target to keep warm across a promotion,
so a newly promoted master gets its own broadcast connection like any other master at the next topology change.

Lifecycle mirrors `Redirect`'s reconnect rule exactly: `TrackingLost` → reconnect → `Armed`, in that order, for
this connection just as for the interactive/subscriber pair. A broadcast connection that goes quiet is caught two
ways: a lost socket (closed or reset) fires `TrackingLost` immediately, and a keepalive `PING` sent every 10 s with
a 5 s reply timeout catches a connection the proxy has silently dropped. Either one flushes L1 and puts that
endpoint in pass-through until the connection is re-established and re-armed, per the existing "any reconnect on
any master means re-arm then flush" rule. The 5 s per-endpoint sweep that reconciles `Redirect` arming against
topology changes does the same job here, re-arming a broadcast connection that fell behind a topology change, and a
lost endpoint is retired by the same `MasterProbe` check as in `Redirect` (multiplexer view first,
then `CLUSTER NODES` from a connected node), so a killed cluster master is forgotten once its slots have moved rather
than holding the cache in pass-through. Forgetting it is what lets the facade cache again, so the retry loop does it
only once every master that serves slots now (the promoted replica included) has an armed broadcast connection; until
then the endpoint stays lost, logged as a warning from the third round. This is the same `TakeoverGuard` check as in
`Redirect` (see "Forgetting waits for the takeover"), with an armed broadcast connection as "tracked". An endpoint the multiplexer itself reports as no longer a master (e.g. a killed master that restarted and
rejoined as a replica, or a master demoted by a manual failover while connected) is retired by the
reconcile under the same check, because the multiplexer can learn the demotion in the same reconfigure that reveals the
promoted node; if that endpoint is still armed it keeps its broadcast connection until then, since a replica still
pushes invalidations for the writes it replicates. The private multiplexer's `ConnectionFailed` for a master is treated as loss of
that node's broadcast connection too (one spare flush if the node was fine, no stale window if it was not), and its
`ConnectionRestored` and `ConfigurationChanged` trigger a reconcile. The tracking handshake on a new socket has a
deadline (`SyncTimeout`), so a peer that accepts the TCP connection and then goes quiet cannot hold `Ready` open. `CLIENT TRACKINGINFO` is used to verify the arm where the server has it (6.2+)
and assumed where it does not, as `Redirect` does.

`NOLOOP` cannot help here: it suppresses pushes for writes made by the tracking connection itself, and the broadcast
connection never writes. So this instance's own `SetAsync` and `RemoveAsync` (sent on the private multiplexer) echo
back as pushes. That is harmless for coherence (the facade already evicts the key around its own write) but it means
a `GetAsync` issued right after a `SetAsync` can have its reply discarded once by the in-flight rule when the echo
lands between the `GET` reply and the store; the next read caches the key. Tests that write through the cache and
then expect an L1 hit poll for it (`TestHelpers.ReadUntilCachedAsync`) rather than asserting the first read.

The `CLIENT CACHING NO` transaction that `Redirect` sends for reads outside `KeyPrefixes` is skipped in `Broadcast`:
the reading connection was never tracked, so a plain `GET` is already untracked. Empty `KeyPrefixes` is legal but
means every write in the database is broadcast to this client (logged once as a warning); a prefix that overlaps
another already configured is dropped with a log line, because Redis rejects overlapping prefixes for one client.
Costs versus `Redirect`: prefixes should be set, invalidation volume scales with the write rate under them rather
than with what L1 holds, and the server's per-key tracking table is not used, so the Enterprise
`tracking_table_max_keys` limit does not apply. `Broadcast` also works on OSS Redis 6+ and Valkey — the CI matrix
runs the Broadcast suite on every image — but `Redirect` stays the default there because it only pushes for keys
this instance actually read.

**Credentials.** RedisNearCache clones the caller's `ConfigurationOptions`, and the clone shares the extension's
token provider: StackExchange.Redis resolves `User`/`Password` through `ConfigurationOptions.Defaults`, which
`Clone()` copies by reference, so an Entra ID token provider installed by `Microsoft.Azure.StackExchangeRedis` (or
any custom `DefaultOptionsProvider` subclass) is visible to every connection RedisNearCache opens. The private
multiplexer is created from that same provider, so the extension registers it through its `AfterConnectAsync` hook
and re-authenticates it as it does any multiplexer it manages: an `AUTH` on each server's interactive connection (the
subscriber connection picks the current token up when it next reconnects). `Auth/EntraIdTests.cs` runs the real
extension (3.3.1, on StackExchange.Redis 3.2.0) against a local server with a fake `TokenCredential`: all three
connections authenticate as the token's object id, and after a rotation, with the old token removed from the server
and the private multiplexer's connections killed, it reconnects, which only the new token allows. Every broadcast connection reads the current object id and token when it connects,
and a live broadcast connection compares its current credentials against the provider on every keepalive tick
(10 s, up to about 15 s if a `PING` is in flight); when they changed, it re-authenticates in place with `AUTH <objectId> <token>` without dropping tracking, without a `TrackingLost`, and without an L1 flush (asserted on every CI image by
`Auth/EntraIdTests.cs` and `Broadcast/CredentialRotationTests.cs`: same connection id, no lifecycle event, no flush, and
an external write still evicts afterwards; against the Redis Enterprise proxy it was verified by hand only:
`CLIENT TRACKINGINFO` is unchanged after `AUTH` and pushes keep arriving). That periodic read does a second job with
the Azure extension: its `Password` getter starts a token refresh once the current token has expired, so in `Broadcast`
an expiry is picked up within a keepalive tick or two, while in `Redirect`, where nothing reads the credential
periodically, rotation waits for the extension's own heartbeat (2 minutes, not configurable in 3.3.1). A failed `AUTH` is treated as connection death: the connection
is re-armed with the current credentials, flushing L1 as for any other reconnect. Values set directly on `ConfigurationOptions.User`/`Password` (a `password=` in a connection
string included) are static and shadow the provider, so a rotating credential must come through the provider.
Re-authenticating the private multiplexer is the provider's own job (the Azure extension does it through
`AfterConnectAsync`); this tracker only re-authenticates its broadcast connections. If the server rejects a rotated credential the connection keeps the one it has (a failed
`AUTH` leaves a Redis connection's authentication unchanged) and stays armed, the rejected pair is not retried until the
provider yields another, and a connection the server closes at expiry is an ordinary socket death.

## Observability

Statistics, metrics and the health check all read existing state; none of them add cost to the read or
invalidation path. Tracing is the exception on one path only: a MISS now starts a span. An L1 hit and every
invalidation/flush still add nothing - see below.

- **Metrics are observable instruments, not counters updated on the hot path.** A `System.Diagnostics.Metrics`
  `Meter` named `RedisNearCache` (`RedisNearCacheStatistics.MeterName`) exposes the same counters as
  `Statistics` plus gauges for L1 entry count, coherence, pass-through duration, lost-endpoint count and two
  latched flags (`RespectServerTtl` abandonment, untracked-reads unavailability), but every instrument is read
  from `Statistics`, `L1Cache` or the armer only when something collects (an OpenTelemetry exporter,
  `dotnet-counters`). `GetAsync`, `SetAsync` and the invalidation handlers touch nothing metrics-related. Where
  a new signal needed a count at all (a serializer throwing, an L1 store refusal, a failed replica pre-arm),
  the increment sits on a path that was already a failure path - an `Interlocked` bump inside a `catch` that
  rethrows, or beside a warning that was already being logged - never a new write on the read path.
  "A failed replica pre-arm" means a failed attempt to ESTABLISH one, and nothing else: the periodic re-check of
  an already pre-armed replica is not counted, however it fails. It runs `ROLE` every sweep, so on a server that
  restricts that command - a proxy, or a least-privilege ACL - counting it would report a permanent, growing
  pre-arm failure against a replica that is in fact armed and healthy.
- **One `Meter` per cache instance, tagged `rnc.client_name`.** The cache already owns a unique client name
  per instance (`{ClientNamePrefix}-{guid}`); reusing it as a tag, rather than sharing one process-wide
  `Meter`, is what keeps several `IRedisNearCache` instances in one process (two providers, or tests) distinct
  in an exported series without extra configuration. The `Meter` is disposed with the cache, same lifetime as
  everything else it owns.
- **`redisnearcache.flushes` and `redisnearcache.rearms` carry a `reason` tag, one measurement per reason on
  every collection, zeros included.** The reasons are closed enums (`FlushReason`; the re-arm-causing subset of
  `ArmReason`), so the cardinality this adds is fixed and small - unlike an endpoint address, which is why no
  instrument here is tagged with one (see below). Zeros are emitted rather than letting the series disappear
  when a reason has never fired: an instrument that vanishes reads on a dashboard as a broken exporter, not as
  nothing having gone wrong. `ArmReason.Initial` and `ArmReason.Promoted` are excluded from the `rearms`
  breakdown - they are arms but never re-arms, so a permanent 0 next to them would suggest a re-arm reason that
  simply never fires, which is not the same thing as one that has not fired yet. Summed over the `reason` tag,
  each total is unchanged from before the breakdown existed.
- **Endpoint addresses appear in the health check's `Data` and in logs, never in a metric tag.** An address is
  unbounded cardinality - as many series as the deployment has ever had endpoints - unlike the closed `reason`
  tag above or the per-instance `rnc.client_name`/`rnc.instance` tags. `RedisNearCacheStatistics.LostEndpointCount`
  is the number of endpoints currently lost; only the concrete `RedisNearCache` facade can also name them, which
  is why the health check's `lostEndpoints` key is present only over the library's own cache instance and absent
  for a caller's own `IRedisNearCache` implementation, rather than reported as an empty string that would read
  as "nothing is lost".
- **The health check reports `Degraded`, not `Unhealthy`, when not coherent.** Pass-through is a real state,
  not a failure one: reads still succeed, served straight from Redis, exactly as `IsCoherent` documents.
  `Unhealthy` would tell an orchestrator to stop routing traffic or restart the instance, which would not fix
  anything here and would drop the very traffic pass-through is designed to keep serving; `Degraded` reports
  the condition without recommending an action that makes it worse.

### Tracing

An `ActivitySource` (`RedisNearCacheTracing`), one per cache instance, disposed with the cache exactly as the
`Meter` is. It is named `"RedisNearCache"` - the same string as the meter name, exposed publicly as
`RedisNearCacheStatistics.ActivitySourceName` - deliberately: the metrics and the spans of one cache instance are
one instrumentation scope, sharing a name and a version, so a caller who has wired up `AddMeter(...)` needs only
`AddSource(RedisNearCacheStatistics.ActivitySourceName)` alongside it to get both.

Two spans only, both `ActivityKind.Client`: `redisnearcache.read` around the Redis round trip of a MISS, and
`redisnearcache.arm` around arming one endpoint (the initial arm and every re-arm, distinguished by the
`rnc.arm_reason` attribute rather than a second span name, so a slow arm is attributable without doubling the
span count). Attributes: `rnc.client_name` and `rnc.instance` on every span (the same instance tags the metrics
carry); `rnc.key` and `rnc.stored`/`rnc.not_stored_reason` on the read span; `rnc.endpoint`, `rnc.arm_reason` and
`rnc.redirect_client_id` on the arm span.

The arm span covers BOTH tracking modes under the one name. `TrackingMode.Broadcast` issues no `CLIENT TRACKING
OFF` - a fresh socket never has tracking on - but it does everything else worth timing: `CLIENT TRACKING ON BCAST`
with the configured prefixes, `CLIENT TRACKINGINFO` to verify, and a retry ladder with backoff that `Redirect` has
no equivalent of and that can give up on a node entirely. It is also the mode recommended in front of a proxy, so
its users are the likeliest to be measuring arm latency in the first place. One span per attempt, so that retry
ladder shows as several spans rather than one long one. `rnc.redirect_client_id` is absent in `Broadcast`, which
has no redirect target - omitted rather than emitted as a placeholder, so its presence tells you the mode.

- **A hot L1 hit creates no span, and does not even call `StartActivity`.** `RedisNearCache.GetStoredBytesAsync`
  returns the hit before touching `RedisNearCacheTracing` at all. This is deliberate, not incidental:
  `ActivitySource.StartActivity` is not free even with nobody listening - several field reads and a branch, an
  allocation once something is - so the read span starts only once the read is already going to Redis, which is
  the expensive part it is timing. A test fails if the span moves above the L1 lookup.
- **There is deliberately no flush span.** A flush runs on the invalidation-handler path, which this section's
  opening rule says must touch nothing observability-related, and a flush is already visible through
  `redisnearcache.flushes` and its `reason` tag - a second signal for the same event would just be a slower way
  to see it.
- **A span may carry a cache key and an endpoint address; a metric tag may not.** The instrument-tag cardinality
  rule above (endpoints, closed enums only) is about metrics, which are pre-aggregated into one time series per
  distinct tag value forever. A span is not aggregated - it is stored with the one trace that recorded it - so a
  high-cardinality attribute costs that trace's storage and nothing else. The two rules look inconsistent side by
  side; they are not the same rule, and the read/arm spans are the reason a span is worth having here at all: to
  say which key, or which endpoint. Do not "fix" the spans by copying the metric-tag rule onto them.
- **`rnc.not_stored_reason` is the most operationally useful attribute here.** It is present whenever
  `rnc.stored` is `false`, and says why a reply that reached the facade was not put in L1 - the one thing no
  counter can say per key. The values, in the order `RedisNearCacheTracing` spells them: `key_missing` (nothing
  existed in Redis to store), `race_discarded` (an invalidation for the key arrived while the read was in flight,
  or `PTTL` said it had already gone), `ttl_unknown` (the remaining TTL could not be read at all, so
  `RespectServerTtl` has no cap to store under), `caching_disabled` (the cache is in pass-through),
  `outside_key_prefixes` (the key is outside `KeyPrefixes`, deliberately neither tracked nor stored), and
  `caching_resumed_mid_read` (the read went out during pass-through, so it carries no TTL to cap by, and caching
  came back while it was in flight - rare, and only ever transient).
- **The arm span covers `CLIENT TRACKING OFF` -> `ON REDIRECT` -> `TRACKINGINFO` only.** It starts after the
  point-of-no-return lock (`TrackingArmer`), so no invalidation-handler flush ever runs with a span current, and
  it ends before the `Armed` event is raised, so anything observing that event - the facade's flush, a test -
  already sees a finished span rather than racing one.

### Instrument names stay `redisnearcache.*`

The instruments do not follow OpenTelemetry's semantic conventions for cache or database clients, which would
suggest `cache.*` or `db.*` names instead. That is a deliberate choice, not an oversight the next change should
correct: renaming an instrument changes its identity for every dashboard and alert already built against
`redisnearcache.*`, and publishing both the old and new names side by side would double every series for
everyone who is not in the middle of migrating, forever, to save a rename for those who are. The prefix stays
as it is.

## Not in v1

`OPTIN` tracking, Garnet, write-through population of L1 (`SetAsync` evicts and lets the next read re-track).
