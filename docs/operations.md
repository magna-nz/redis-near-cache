# Operations

## Logging categories

RedisNearCache does not register logging itself; if the host never called `AddLogging`, it silently uses a
no-op logger. Wire up `ILoggerFactory` in your host to see any of this. Categories are the fully qualified
type names:

| Category | What it logs |
|---|---|
| `RedisNearCache.Internal.RedisNearCacheConnection` | One `Information` line on connect: the client name and endpoints of the private multiplexer. |
| `RedisNearCache.Tracking.TrackingArmer` | The most operationally important category. `Information` for every successful arm/re-arm (with the endpoint, redirect client id, and `ArmReason`) and for `ConnectionRestored`/topology-change events triggering a re-arm. `Warning` for a retry attempt, for "no connected master to arm", for giving up after the retry ladder is exhausted, and for a lost connection (`ConnectionFailed`). `Debug`/`Trace` for verification detail (`CLIENT TRACKINGINFO` replies, skipped replicas). |
| `RedisNearCache.Tracking.InvalidationListener` | `Information` once, on subscribing. `Debug` for a null (FLUSHDB/FLUSHALL) invalidation. `Trace` for every single-key invalidation (this is high-volume; do not run at `Trace` in production unless you are actively diagnosing). `Error` if handling a message throws (should not happen; the handler is defensive). |
| `RedisNearCache.Caching.RedisNearCache` | `Error` if the initial `StartAsync` (listener + armer) fails entirely, i.e. the near cache will never be coherent for at least one endpoint. |

**What to watch in production:** `Warning`-level messages from `TrackingArmer` are the signal that
something is wrong: a node that could not be armed after retries, or a connection that dropped and has not
come back. A steady stream of `Information`-level "re-arming" messages (rather than occasional ones around
deploys/restarts) suggests something is repeatedly killing the private connections.

## Statistics to graph

`IRedisNearCache.Statistics` exposes six thread-safe counters. Sample them periodically (they are
cumulative, so graph the rate/delta, not the raw value) rather than trying to log every change:

| Counter | What a change means | What to watch for |
|---|---|---|
| `Hits` | A read served entirely from L1, no Redis call. | The primary payoff metric. Compare against `Misses` for a hit ratio. |
| `Misses` | A read that went to Redis (first read of a key, or after eviction/expiry). | A sudden sustained rise (with a proportional `Flushes`/`Rearms` rise) usually means something is invalidating or flushing more than expected. |
| `Invalidations` | A per-key invalidation message received from the server. | Should track your actual external write volume against tracked keys. Unexpectedly high counts on a specific key are the signal to move it out of `KeyPrefixes` (see below). |
| `Flushes` | A whole-cache L1 clear: a null invalidation (`FLUSHDB`/`FLUSHALL`), a lost connection, or any re-arm after the first. | Should be rare in steady state. A rising rate means reconnects are happening repeatedly; check `TrackingArmer` warnings for why. |
| `Rearms` | `CLIENT TRACKING` was (re)issued on an endpoint after its initial arm. | Same signal as `Flushes` (every re-arm causes a flush) but isolates the reconnect-driven case from `FLUSHDB`/`FLUSHALL`. |
| `RaceDiscards` | A Redis reply was thrown away because an invalidation for that key arrived while the read was still in flight. | Expected to be occasionally non-zero under concurrent read/write load on the same key; a value comparable to `Misses` suggests either very hot keys or unusually high latency to Redis. |

## Detecting a lost or unarmed endpoint

There is no dedicated "is everything armed" property, but you can reconstruct the current state from the
pieces the library already exposes:

- **Logs.** `TrackingArmer` logs `Warning` for `ConnectionFailed` (an endpoint just went down) and for
  giving up on the retry ladder (an endpoint that will now only retry every 5 seconds in the background).
  Alert on these.
- **Statistics.** A rising `Flushes`/`Rearms` rate, or `Hits` dropping to near zero while `Misses` stays
  high, both indicate the cache has fallen into pass-through mode (nothing being cached) on at least one
  endpoint, either because it is degraded (`Ready` faulted with no master armed at all) or because one or
  more endpoints are currently in the "lost" state internally.
- **`Ready`.** Awaiting `cache.Ready` only tells you whether *at least one* master armed during startup; it
  does not fault for a single node that failed to arm among several, and it does not report a later loss.
  Treat a completed `Ready` as "the cache is usable", not "every node is armed".
- **Direct check against Redis**, for deeper diagnosis: `CLIENT LIST` on the node in question, filtered to
  RedisNearCache's client name (`ClientNamePrefix`, default `rnc-<guid>`) and the `P` (pub/sub subscriber)
  flag, tells you whether the subscriber connection currently exists; `CLIENT TRACKINGINFO` on the
  interactive connection reports whether tracking is on and what it currently redirects to. This is what the
  integration tests under `tests/RedisNearCache.Tests/Chaos/` use to assert re-arming happened correctly, for
  example `BothConnectionsKilledTests` and `NodeRestartClusterTests`.

## Sizing L1

`L1SizeLimit` (default `10_000`) counts entries, not bytes: every stored value costs a size of 1 regardless
of how large it is. There is no automatic memory-based eviction, so size the limit based on
(expected working-set key count) rather than a memory budget, and separately estimate memory as
(number of entries you expect resident) × (average serialized value size). If your key space includes large
or infrequently-reused values that you would rather not hold in-process at all, use `KeyPrefixes` to opt
only the keys worth caching into L1. Everything outside the configured prefixes is still read through
RedisNearCache but never stored locally, so writes to those keys also never cost an invalidation push.

`L1MaxAge` (default 5 minutes) is a time-based safety net independent of size: it bounds how long a
value can survive after a missed invalidation, not a sizing lever by itself. Setting it very high increases
the blast radius of anything that does get missed; setting it very low pushes more traffic back to Redis
even when tracking is working correctly.
