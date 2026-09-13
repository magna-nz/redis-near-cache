# RedisNearCache

Server-assisted client-side caching (Redis `CLIENT TRACKING`) layered on StackExchange.Redis 3.x.
Read `DESIGN.md` before changing anything under `src/`.

## Commands

The SDK is not on PATH on this machine.

    ~/.dotnet/dotnet build
    ~/.dotnet/dotnet test tests/RedisNearCache.Tests
    docker compose up -d          # standalone Redis 7.4 on localhost:6379 (container redis-near-cache-redis)
    ./cluster-up.sh               # 3-master cluster on 127.0.0.1:7100-7102 (container redis-near-cache-cluster)

Integration tests expect both containers to be running.

## Rules

- The shared contracts live in `src/RedisNearCache/Abstractions/`. Implement them; never redeclare or
  reshape them. If a contract is wrong, say so in your report instead of working around it.
- Never touch, configure, or depend on the caller's multiplexer. All Redis traffic for caching goes through
  the private `RedisNearCacheConnection` (RESP2, admin mode, own client name).
- Tracking is one-shot per key: after an invalidation the server forgets the key until it is read again.
- Any reconnect of either connection type on any node means: re-arm that node, then flush L1.
- A read whose key was invalidated while the reply was in flight must not populate L1.
- No repo-wide git operations from subagents. Commits are made by the coordinator.
- Warnings are errors in `src/`.
