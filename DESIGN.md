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

## Read path (facade)

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

Invalidation events call `L1.Remove(key)` and `inflight.MarkInvalidated(key)`. `FlushAll`, any `Armed`
event with a non-Initial reason, every `TrackingLost` and every `EndpointRemoved` flush L1 through the one
flush path (`inflight.MarkAllInvalidated()` first, then `L1.Clear()`), so a read in flight across a flush never
stores its reply.

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
(the hook was seen to fire for a clone-built multiplexer on StackExchange.Redis 3.2.0; the
re-authentication itself is the extension's documented behaviour) and re-authenticates it exactly as it does any multiplexer it manages. Every broadcast connection reads the current object id and token when it connects,
and a live broadcast connection compares its current credentials against the provider on every keepalive tick
(10 s, up to about 15 s if a `PING` is in flight); when they changed, it re-authenticates in place with `AUTH <objectId> <token>` without dropping tracking (verified by hand, not by CI, on Redis 6.2, 7.4, Valkey 8.1 and the Redis Enterprise proxy: `CLIENT TRACKINGINFO`
is unchanged after `AUTH` and pushes keep arriving),
without a `TrackingLost`, and without an L1 flush. A failed `AUTH` is treated as connection death: the connection
is re-armed with the current credentials, flushing L1 as for any other reconnect. Values set directly on `ConfigurationOptions.User`/`Password` (a `password=` in a connection
string included) are static and shadow the provider, so a rotating credential must come through the provider.
Re-authenticating the private multiplexer is the provider's own job (the Azure extension does it through
`AfterConnectAsync`); this tracker only re-authenticates its broadcast connections. If the server rejects a rotated credential the connection keeps the one it has (a failed
`AUTH` leaves a Redis connection's authentication unchanged) and stays armed, the rejected pair is not retried until the
provider yields another, and a connection the server closes at expiry is an ordinary socket death.

## Observability

Statistics, metrics and the health check all read existing state; none of them add cost to the read or
invalidation path.

- **Metrics are observable instruments, not counters updated on the hot path.** A `System.Diagnostics.Metrics`
  `Meter` named `RedisNearCache` (`RedisNearCacheStatistics.MeterName`) exposes the same counters as
  `Statistics` plus L1 entry count and coherence as gauges, but every instrument is read from `Statistics` or
  `L1Cache` only when something collects (an OpenTelemetry exporter, `dotnet-counters`). `GetAsync`, `SetAsync`
  and the invalidation handlers touch nothing metrics-related.
- **One `Meter` per cache instance, tagged `rnc.client_name`.** The cache already owns a unique client name
  per instance (`{ClientNamePrefix}-{guid}`); reusing it as a tag, rather than sharing one process-wide
  `Meter`, is what keeps several `IRedisNearCache` instances in one process (two providers, or tests) distinct
  in an exported series without extra configuration. The `Meter` is disposed with the cache, same lifetime as
  everything else it owns.
- **The health check reports `Degraded`, not `Unhealthy`, when not coherent.** Pass-through is a real state,
  not a failure one: reads still succeed, served straight from Redis, exactly as `IsCoherent` documents.
  `Unhealthy` would tell an orchestrator to stop routing traffic or restart the instance, which would not fix
  anything here and would drop the very traffic pass-through is designed to keep serving; `Degraded` reports
  the condition without recommending an action that makes it worse.

## Not in v1

`OPTIN` tracking, Garnet, write-through population of L1 (`SetAsync` evicts and lets the next read re-track).
