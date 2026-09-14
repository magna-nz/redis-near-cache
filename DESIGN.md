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
   `CLIENT TRACKING OFF` → `CLIENT TRACKING ON REDIRECT <subscriber id>` on that node's interactive connection.
3. `InvalidationListener` subscribes to `__redis__:invalidate`; each message names one key (or is null = flush).
4. `L1Cache` stores values read through the private multiplexer; reads of tracked keys served locally.
5. Only keys read through RedisNearCache are tracked, because only its connection is tracked. Prefix opt-in
   further limits what is stored in L1.

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
| No `OPTIN`/`OPTOUT` | `CLIENT CACHING YES` must be adjacent on the wire to the next command; impossible on a multiplexed connection. |
| No Garnet | Garnet does not implement `CLIENT TRACKING`. Valkey and Redis 6+ do. |

## Components and ownership

| Component | Path | Depends on |
|---|---|---|
| Contracts | `src/RedisNearCache/Abstractions/` | — |
| `TrackingArmer`, `MasterRole`, `InvalidationListener` | `src/RedisNearCache/Tracking/` | `RedisNearCacheConnection` |
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
  bytes = await privateDb.StringGetAsync(key)
  await TestHooks.AfterRedisReadBeforeStore?(key)
  if bytes is null → inflight.End(key); return default
  if inflight.WasInvalidated(key) → Statistics.RaceDiscard; do not store
  else if key matches prefixes → L1.Set(key, bytes, MaxAge)
  inflight.End(key)
  return Serializer.Deserialize<T>(bytes)
```

Invalidation events call `L1.Remove(key)` and `inflight.MarkInvalidated(key)`. `FlushAll`, any `Armed`
event with a non-Initial reason, every `TrackingLost` and every `EndpointRemoved` flush L1 through the one
flush path (`inflight.MarkAllInvalidated()` first, then `L1.Clear()`), so a read in flight across a flush never
stores its reply.

## Endpoint lifecycle and pass-through

The armer raises three lifecycle events per endpoint; the facade mirrors them in its lost set.

| Event | Raised when | Facade |
|---|---|---|
| `TrackingLost` | a connection of a tracked endpoint (armed, waited on, or a connected master) failed, an arm gave up, or any non-initial arm starts | add to lost set, flush |
| `Armed` | `CLIENT TRACKING ON REDIRECT` verified | flush unless Initial, then remove from lost set |
| `EndpointRemoved` | an endpoint we armed or lost is no longer a master of the deployment | flush (always), then remove from lost set |

L1 is read and populated only while the lost set is empty (**pass-through rule**). Every `TrackingLost` is
followed by `Armed` or `EndpointRemoved` for that endpoint; the armer mutates its own lost set and raises under
one lock so the facade sees events in the same order as the armer's state, and the per-endpoint background retry
loop (every 5 s) runs until one of the two happens.

An endpoint we armed counts as tracked **whatever its current replica flag**: in a graceful Sentinel failover the
multiplexer can flag the old master as a replica before Sentinel kills our connections there, and those failures
must still flush. `EndpointRemoved` flushes unconditionally because entries read from a node that is no longer a
tracked master are protected by nothing (its connections may be killed without an invalidation ever arriving).
A re-arm that finds the node is now a replica raises `EndpointRemoved` rather than returning silently.

**Still a master?** (`MasterRole`) decides between retrying (stay in pass-through) and forgetting an endpoint.
The multiplexer never changes the role it last saw for a node it cannot reach, so a killed master would otherwise
be a master forever:

- Not listed by the multiplexer, or flagged replica → not a master.
- Connected and not a replica → master.
- Disconnected, not in a cluster → master only while no *other* connected, non-replica data server exists (a
  master that is merely down with no replacement keeps the cache in pass-through; once Sentinel's promoted
  replica is connected, the dead one is forgotten). Checked on every configuration change and by the retry loop.
- Disconnected cluster master → the retry loop reads `CLUSTER NODES` from a connected node: a master only while it
  is listed as a master that owns slots (down but not yet failed over stays a master). Nodes are matched on port
  plus IP, announced hostname, or the resolved addresses of a `DnsEndPoint` — never `EndPoint` equality, because
  against `cluster-preferred-endpoint-type hostname` the multiplexer holds `DnsEndPoint(localhost:7201)` while
  `CLUSTER NODES` yields `127.0.0.1:7201`. No readable view → stays a master.

A graceful cluster failover (`CLUSTER FAILOVER`) keeps our connections: the old master is flagged replica at the
next topology check and removed (flush), and the promoted one is armed with `TopologyChanged` (flush).

## Not in v1

RESP3 push tracking, opt-in/opt-out modes, Garnet, write-through population of L1 (`SetAsync` evicts and lets the next read re-track).
