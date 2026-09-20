namespace RedisNearCache;

/// <summary>Timing hooks for integration tests. Null in production.</summary>
internal sealed class RedisNearCacheTestHooks
{
    /// <summary>
    /// Invoked with the key after the Redis reply for a miss has arrived and BEFORE the value is stored in L1.
    /// Tests use it to perform a concurrent write and prove the stale reply is discarded.
    /// </summary>
    public Func<string, Task>? AfterRedisReadBeforeStore { get; set; }

    /// <summary>
    /// Invoked synchronously inside every whole-cache flush handler (FLUSHDB, re-arm, tracking lost) AFTER the
    /// in-flight tracker has been marked and BEFORE L1 is cleared. Tests use it to land a read's store exactly
    /// in that window and prove the post-store re-check evicts it.
    /// </summary>
    public Action? InsideFlushHandler { get; set; }

    /// <summary>
    /// How long the facade waits between attempts to start again after the invalidation subscription could not be
    /// made at startup. Null in production: <c>TrackingRetry.SlowInterval</c>.
    /// </summary>
    public TimeSpan? StartRetryInterval { get; set; }
}
