# RedisNearCache

Server-assisted client-side caching (Redis `CLIENT TRACKING`) layered on StackExchange.Redis 3.x.
Read `DESIGN.md` before changing anything under `src/`.

## Commands

The SDK is not on PATH on this machine.

    ~/.dotnet/dotnet build
    ~/.dotnet/dotnet test tests/RedisNearCache.Tests --filter "Category!=Soak"
    ./up.sh                       # recreates every container below; same script CI runs
    bench/run-matrix.sh [--quick] # benchmark comparison, not in CI; bench/RedisNearCache.Bench/README.md

Containers (image overridable with RNC_REDIS_IMAGE, default redis:7.4; enterprise-up.sh uses RNC_ENTERPRISE_IMAGE instead):

    docker-compose.yml   standalone :6379, replica :6380, TLS :6390
    cluster-up.sh        3-master cluster 127.0.0.1:7100-7105             (redis-near-cache-cluster)
    sentinel-up.sh       master :6400, replicas :6401-6402, sentinels :26379-26381, service mymaster
                                                                          (redis-near-cache-sentinel)
    managed-up.sh        restricted admin commands :6410                  (redis-near-cache-restricted)
                         hostname-announcing cluster localhost:7200-7205  (redis-near-cache-cluster-hostname, 7.0+)
    enterprise-up.sh     single-node Redis Software behind its proxy :12000  (redis-near-cache-enterprise; not in up.sh,
                         ~4 min to boot, image redislabs/redis; the local stand-in for Azure Managed Redis / Redis Cloud)

Integration tests expect all of them except the Enterprise one to be running; the `Enterprise` suite is skipped
unless `RNC_ENTERPRISE_REDIS` (e.g. `localhost:12000`) is set. `ExternalManagedEndpointTests` is skipped unless
`RNC_EXTERNAL_REDIS` holds a connection string to a real (e.g. ElastiCache / Azure Managed Redis) endpoint.

## Rules

- The shared contracts live in `src/RedisNearCache/Abstractions/`. Implement them; never redeclare or
  reshape them. If a contract is wrong, say so in your report instead of working around it.
- Never touch, configure, or depend on the caller's multiplexer. All Redis traffic for caching goes through
  the private `RedisNearCacheConnection` (RESP2, admin mode, own client name).
- Tracking is one-shot per key: after an invalidation the server forgets the key until it is read again.
- Any reconnect of either connection type on any master means: re-arm that node, then flush L1. A pre-armed
  replica's reconnect only drops its pre-arm (nothing in L1 came from it); the next 5 s sweep re-arms it.
- A read whose key was invalidated while the reply was in flight must not populate L1.
- No repo-wide git operations from subagents. Commits are made by the coordinator.
- Warnings are errors in `src/`.
