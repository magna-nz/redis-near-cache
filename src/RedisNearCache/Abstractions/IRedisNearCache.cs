namespace RedisNearCache;

/// <summary>
/// A near cache backed by Redis server-assisted client-side caching (CLIENT TRACKING).
/// Reads are served from an in-process L1 when possible; on a miss the value is fetched from Redis over a
/// connection that the Redis server tracks, so any later write to that key (from any client, any language)
/// causes the server to push an invalidation and the L1 entry is evicted.
/// </summary>
/// <remarks>
/// Consume this interface; do not implement it. The library's implementation is the only supported one, and members
/// may be added to it in a minor release. Wrap or decorate it by delegating to the registered instance instead.
/// </remarks>
public interface IRedisNearCache : IAsyncDisposable
{
    /// <summary>
    /// Gets the value stored at <paramref name="key"/>, serving from L1 when present and tracked.
    /// Returns <c>default</c> when the key does not exist in Redis.
    /// </summary>
    ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the bytes stored at <paramref name="key"/> exactly as they are in Redis, without the configured
    /// <see cref="RedisNearCacheOptions.Serializer"/>, serving from L1 when present and tracked. Returns <c>null</c>
    /// when the key does not exist. The array is the caller's own; changing it does not change the cached copy.
    /// </summary>
    ValueTask<byte[]?> GetBytesAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes <paramref name="value"/> to Redis through RedisNearCache's own connection and evicts any L1 copy.
    /// The next <see cref="GetAsync{T}"/> re-reads and re-tracks the key. A null <paramref name="expiry"/> issues a
    /// plain <c>SET</c>, which clears whatever TTL the key already had; pass one to keep or replace it.
    /// </summary>
    ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes <paramref name="value"/> to Redis as is, without the configured <see cref="RedisNearCacheOptions.Serializer"/>,
    /// and evicts any L1 copy, exactly as <see cref="SetAsync{T}"/> does. A null <paramref name="expiry"/> issues a
    /// plain <c>SET</c> here too, clearing any TTL the key already had.
    /// </summary>
    ValueTask SetBytesAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? expiry = null, CancellationToken cancellationToken = default);

    /// <summary>Deletes the key in Redis and evicts any L1 copy.</summary>
    ValueTask<bool> RemoveAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Evicts the L1 copy only. Redis is not touched. A read of the key in flight across the call never stores its
    /// reply. A no-op once the cache has been disposed.
    /// </summary>
    void EvictLocal(string key);

    /// <summary>
    /// Drops every L1 entry. Redis is not touched. A read in flight across the call never stores its reply. Use it
    /// after an operation the server does not invalidate for, such as <c>SWAPDB</c>. A no-op once the cache has
    /// been disposed.
    /// </summary>
    void EvictAllLocal();

    /// <summary>
    /// Returns true and the L1 value when the key is currently cached locally. Never touches Redis, and never
    /// counts towards <see cref="RedisNearCacheStatistics.Hits"/> or <see cref="RedisNearCacheStatistics.Misses"/>:
    /// it inspects the cache rather than reading through it.
    /// </summary>
    bool TryGetLocal<T>(string key, out T? value);

    /// <summary>Counters for hits, misses, invalidations, flushes and re-arms, plus the current L1 entry count.</summary>
    RedisNearCacheStatistics Statistics { get; }

    /// <summary>
    /// Completes once the invalidation subscription is up and the initial arming pass over every master has
    /// finished. Masters that could not be armed are retried in the background; until every master is armed
    /// the cache serves every read from Redis and stores nothing locally. Faults if no master could be armed at
    /// all, which starts the cache in that pass-through mode - but not permanently: the background retry keeps
    /// going every few seconds, and the first master it arms puts the cache back to serving from L1. This task
    /// stays faulted regardless, because it reports how startup went; use <see cref="IsCoherent"/> or
    /// <see cref="WaitForCoherenceAsync"/> to observe the recovery.
    /// </summary>
    Task Ready { get; }

    /// <summary>
    /// True while tracking is armed on every master and L1 is being read and populated. False while the cache is
    /// in pass-through: before the initial arm finishes, after a connection or master was lost and until it is
    /// re-armed or forgotten, or, if no master could be armed at startup, until the background retry arms one.
    /// </summary>
    bool IsCoherent { get; }

    /// <summary>Completes when <see cref="IsCoherent"/> is true, immediately if it already is.</summary>
    Task WaitForCoherenceAsync(CancellationToken cancellationToken = default);
}
