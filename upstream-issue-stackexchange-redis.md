# Draft issue for StackExchange/StackExchange.Redis

Title: Allow CLIENT TRACKING / CACHING / TRACKINGINFO through Execute without allowAdmin

Body:

I am building server-assisted client-side caching (Redis 6 `CLIENT TRACKING`, issues #1461 and #2583)
as a package layered on StackExchange.Redis rather than a fork, so it works on 2.x and 3.x today.

Two small things in the library force the package to open its own multiplexer instead of attaching to
the application's existing one:

1. `CLIENT` is gated behind `allowAdmin`, so `db.Execute("CLIENT", "TRACKING", "ON", "REDIRECT", id)`,
   `CLIENT CACHING YES` and `CLIENT TRACKINGINFO` all throw
   `RedisCommandException: This operation is not available unless admin mode is enabled: CLIENT`.
   These three subcommands only affect the calling connection; they cannot disrupt other clients the
   way `CLIENT KILL` or `CLIENT PAUSE` can. Would you accept a PR that whitelists
   `CLIENT TRACKING`, `CLIENT CACHING` and `CLIENT TRACKINGINFO` in `CheckMessage`
   (ConnectionMultiplexer.cs) without admin mode? (`CLIENT ID` already passes.)

2. Under RESP3 (the 3.x default) the multiplexer uses one connection per node and the `invalidate` push
   frames are consumed internally; a subscriber of `__redis__:invalidate` never sees them
   (verified on 3.2.0 against Redis 7.4: `TRACKINGINFO` reports tracking on, zero messages delivered).
   Under RESP2 with `REDIRECT` to the multiplexer's own subscriber connection everything works,
   including per-node redirect in cluster mode. A public way to observe push frames (or routing
   `invalidate` pushes to `__redis__:invalidate` subscribers) would let a package work on RESP3.

Happy to open either PR. The spike that established these findings:
<link to the redis-client-tracking-spike repo>
