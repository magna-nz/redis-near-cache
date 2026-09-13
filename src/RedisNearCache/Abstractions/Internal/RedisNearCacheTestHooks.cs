namespace RedisNearCache;

/// <summary>Timing hooks for integration tests. Null in production.</summary>
internal sealed class RedisNearCacheTestHooks
{
    /// <summary>
    /// Invoked with the key after the Redis reply for a miss has arrived and BEFORE the value is stored in L1.
    /// Tests use it to perform a concurrent write and prove the stale reply is discarded.
    /// </summary>
    public Func<string, Task>? AfterRedisReadBeforeStore { get; set; }
}
