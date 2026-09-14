namespace RedisNearCache;

/// <summary>
/// A near cache backed by Redis server-assisted client-side caching (CLIENT TRACKING).
/// Reads are served from an in-process L1 when possible; on a miss the value is fetched from Redis over a
/// connection that the Redis server tracks, so any later write to that key (from any client, any language)
/// causes the server to push an invalidation and the L1 entry is evicted.
/// </summary>
public interface IRedisNearCache : IAsyncDisposable
{
    /// <summary>
    /// Gets the value stored at <paramref name="key"/>, serving from L1 when present and tracked.
    /// Returns <c>default</c> when the key does not exist in Redis.
    /// </summary>
    ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes <paramref name="value"/> to Redis through RedisNearCache's own connection and evicts any L1 copy.
    /// The next <see cref="GetAsync{T}"/> re-reads and re-tracks the key.
    /// </summary>
    ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken cancellationToken = default);

    /// <summary>Deletes the key in Redis and evicts any L1 copy.</summary>
    ValueTask<bool> RemoveAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Evicts the L1 copy only. Redis is not touched.</summary>
    void EvictLocal(string key);

    /// <summary>
    /// Drops every L1 entry. Redis is not touched. A read in flight across the call never stores its reply. Use it
    /// after an operation the server does not invalidate for, such as <c>SWAPDB</c>.
    /// </summary>
    void EvictAllLocal();

    /// <summary>Returns true and the L1 value when the key is currently cached locally. Never touches Redis.</summary>
    bool TryGetLocal<T>(string key, out T? value);

    /// <summary>Counters for hits, misses, invalidations, flushes and re-arms.</summary>
    RedisNearCacheStatistics Statistics { get; }

    /// <summary>
    /// Completes once the invalidation subscription is up and the initial arming pass over every master has
    /// finished. Masters that could not be armed are retried in the background; until every master is armed
    /// the cache serves every read from Redis and stores nothing locally. Faults only if no master could be
    /// armed at all, in which case the cache stays in that pass-through mode permanently.
    /// </summary>
    Task Ready { get; }

    /// <summary>
    /// True while tracking is armed on every master and L1 is being read and populated. False while the cache is
    /// in pass-through: before the initial arm finishes, after a connection or master was lost and until it is
    /// re-armed or forgotten, or permanently if no master could ever be armed.
    /// </summary>
    bool IsCoherent { get; }

    /// <summary>Completes when <see cref="IsCoherent"/> is true, immediately if it already is.</summary>
    Task WaitForCoherenceAsync(CancellationToken cancellationToken = default);
}
