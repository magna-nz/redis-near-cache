# FAQ

## Do my writers need to change?

No. Any client, in any language, doing a plain `SET`, `DEL`, `MSET`, or a `redis-cli` command by hand
causes Redis itself to push an invalidation to every connection tracking that key. RedisNearCache does not
require writers to publish anything or use the same library. This is the point of server-assisted tracking
over an application-level pub/sub backplane: the backplane only invalidates readers whose writers happen to
use the same library and publish to the same channel.

## Can I use my existing `IConnectionMultiplexer`?

Your existing multiplexer is never touched, reconfigured, or depended on. RedisNearCache opens its own
private multiplexer (cloned connection settings, forced to RESP2 with admin mode). Your own connection can
stay on whatever protocol and configuration it already uses.

## Why does RedisNearCache need its own connection instead of using mine?

Two reasons, from the design notes (`DESIGN.md`) and the underlying spike: `CLIENT TRACKING` and
`CLIENT LIST` require `AllowAdmin=true`, which most applications do not (and should not) set on their main
connection; and every read issued on a tracked connection is tracked by Redis, whether or not it is
something you actually want cached. Giving RedisNearCache its own connection means only cache reads are
tracked, and your main connection's admin surface is untouched.

## Why RESP2 instead of RESP3?

StackExchange.Redis 3.x is RESP3 by default, but under RESP3 the library collapses to a single connection
per node and swallows the `invalidate` push frames internally. The spike measured this directly: the
server reports tracking as on, but zero invalidation messages ever reach the application. RESP2 is the only
protocol under which StackExchange.Redis opens a separate subscriber connection, which is what
RedisNearCache uses as the `REDIRECT` target.

## Why does it need admin mode (`AllowAdmin=true`)?

`CLIENT TRACKING ON/OFF`, `CLIENT TRACKINGINFO`, and `CLIENT LIST` are all gated behind admin mode in
StackExchange.Redis; without it they throw `RedisCommandException: not available unless admin mode is
enabled`. This only applies to RedisNearCache's own private connection, not yours.

## What if Redis restarts, or my connection drops?

RedisNearCache re-arms automatically. An interactive-connection reconnect means the server dropped tracking
entirely; a subscriber-connection reconnect means the server is still redirecting to a now-dead client id
and would otherwise silently lose every invalidation. Both cases are handled: RedisNearCache re-issues
`CLIENT TRACKING` (finding the new subscriber id via `CLIENT LIST` when needed) and flushes L1, because
anything invalidated during the gap is unaccounted for. While any node is down and not yet re-armed, every read (not only reads
of that node's keys) goes straight to Redis and nothing is cached (pass-through), so the cache can never serve
something stale from that window. This is exercised directly by the integration tests, including killing both
connections back to back (`Chaos/BothConnectionsKilledTests`) and restarting a whole cluster node
(`Chaos/NodeRestartClusterTests`).

## What if Redis crashes and does not come back?

`IRedisNearCache.Ready` only faults if *no* master could be armed at all after the retry ladder is
exhausted; in that case the cache stays permanently in pass-through (every read goes to Redis, nothing is
cached) rather than throwing on every call. If Redis is unreachable, the reads themselves will fail the same
way they would through a plain StackExchange.Redis connection. RedisNearCache does not add its own
resilience layer on top of that.

## Does it work with Redis Cluster?

Yes. Tracking is per node, so RedisNearCache's armer repeats the arm/re-arm dance independently on every
connected master, using that node's own subscriber connection as the redirect target. A lost or restarted
node re-arms on its own; the other nodes are unaffected. This is covered by
`tests/RedisNearCache.Tests/ClusterPerNodeInvalidationTests.cs`,
`ClusterKillOneNodeInteractiveRearmsTests.cs`, and `Chaos/NodeRestartClusterTests.cs`.

## Does it work with Sentinel, ElastiCache, or Azure Managed Redis?

Not yet tested. The tests in this repository run against a plain standalone Redis 7.4 container and a
3-master Redis Cluster started via `cluster-up.sh`. Nothing in the design specifically depends on the
absence of Sentinel or a managed offering (it works through StackExchange.Redis's own topology handling and
per-master `CLIENT TRACKING`), but no test exercises those configurations, so this is genuinely untested
rather than a documented guarantee.

## How much memory does L1 use?

L1 is a `Microsoft.Extensions.Caching.Memory.MemoryCache` where every entry costs a size of 1 against
`RedisNearCacheOptions.L1SizeLimit` (default `10_000`). There is no accounting for the byte size of values,
so memory use is roughly (number of cached entries) × (average serialized value size), bounded above by
`L1SizeLimit` entries. Use `KeyPrefixes` to limit which keys are eligible for L1 at all if you have large or
very numerous values you do not want held locally.

## What happens to a key's TTL?

RedisNearCache does not track expiry deadlines itself. When Redis's own active expiry cycle actually removes
an expired key, that is a normal deletion from Redis's point of view and it is invalidated like any other
write. Because active expiry can lag the TTL deadline, there can be a small window where L1 still holds a
value whose TTL has technically passed. `L1MaxAge` (default 5 minutes) is the safety net for this and for
any other missed invalidation: an entry is dropped after that age regardless of whether an invalidation
arrived.

## Can I invalidate a key without touching Redis?

Yes, `EvictLocal(key)` removes only the local L1 copy without going to Redis. This is separate from
`RemoveAsync`, which deletes the key in Redis too.

## Does `SetAsync` populate L1 immediately?

No. `SetAsync` writes to Redis and evicts any existing L1 copy of the key; the next `GetAsync<T>` re-reads
it from Redis and re-tracks it. Write-through population of L1 is explicitly out of scope for v1 (see
`DESIGN.md`, "Not in v1").
