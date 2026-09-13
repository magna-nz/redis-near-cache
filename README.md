# RedisNearCache

Server-assisted client-side caching for .NET, layered on StackExchange.Redis.

Keeps an in-process copy of the Redis values your app reads, and lets the Redis server itself tell you
when any of them change, from any writer in any language, using Redis 6+ `CLIENT TRACKING`.
No forking of StackExchange.Redis, no changes to your existing multiplexer.

Status: pre-release, under construction. See `DESIGN.md`.
