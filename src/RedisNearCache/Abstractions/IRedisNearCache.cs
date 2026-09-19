using StackExchange.Redis;

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
    /// Reads several keys at once: <see cref="GetAsync{T}"/> for every distinct key in <paramref name="keys"/>, all
    /// started together so the misses share a round trip. Keys already in L1 are served locally; the rest are read
    /// from Redis, tracked and stored exactly as a single-key read would.
    /// </summary>
    /// <returns>
    /// One entry per distinct key, compared ordinally. A key that does not exist in Redis is present with
    /// <c>default</c>, as <see cref="GetAsync{T}"/> returns for it; for a value type, read it as its nullable form
    /// (<c>GetManyAsync&lt;int?&gt;</c>) to tell a missing key from a stored zero.
    /// </returns>
    /// <remarks>
    /// This is not an <c>MGET</c>: every key goes through the single-key read path, so each keeps its own race
    /// check, TTL cap, <c>KeyPrefixes</c> handling and cluster routing, and <see cref="Statistics"/> counts one hit
    /// or miss per distinct key. Reads are issued at most 256 at a time. If any read fails the call throws the first
    /// failure, after every read already started has finished; keys read successfully by then stay cached.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="keys"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="keys"/> contains a null key.</exception>
    ValueTask<IReadOnlyDictionary<string, T?>> GetManyAsync<T>(IEnumerable<string> keys, CancellationToken cancellationToken = default) =>
        Internal.ManyReads.ReadAsync<T?>(keys, (key, token) => GetAsync<T>(key, token), cancellationToken);

    /// <summary>
    /// The raw-bytes form of <see cref="GetManyAsync{T}"/>: <see cref="GetBytesAsync"/> for every distinct key, with
    /// the same result shape, batching and failure behaviour. A key that does not exist is present with <c>null</c>;
    /// every array returned is the caller's own copy.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="keys"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="keys"/> contains a null key.</exception>
    ValueTask<IReadOnlyDictionary<string, byte[]?>> GetManyBytesAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default) =>
        Internal.ManyReads.ReadAsync<byte[]?>(keys, (key, token) => GetBytesAsync(key, token), cancellationToken);

    /// <summary>
    /// Writes <paramref name="value"/> to Redis through RedisNearCache's own connection and evicts any L1 copy.
    /// The next <see cref="GetAsync{T}"/> re-reads and re-tracks the key. A null <paramref name="expiry"/> issues a
    /// plain <c>SET</c>, which clears whatever TTL the key already had; pass one to replace it, or use the
    /// overload taking <c>keepTtl</c> to keep it.
    /// </summary>
    ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes <paramref name="value"/> to Redis as is, without the configured <see cref="RedisNearCacheOptions.Serializer"/>,
    /// and evicts any L1 copy, exactly as <see cref="SetAsync{T}(string, T, TimeSpan?, CancellationToken)"/> does. A null <paramref name="expiry"/> issues a
    /// plain <c>SET</c> here too, clearing any TTL the key already had.
    /// </summary>
    ValueTask SetBytesAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? expiry = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// A conditional write: <see cref="When.NotExists"/> writes only if the key is absent (<c>SET NX</c>),
    /// <see cref="When.Exists"/> only if it is present (<c>SET XX</c>), <see cref="When.Always"/> unconditionally.
    /// Returns <c>true</c> if Redis performed the write, <c>false</c> if the condition was not met and the key is
    /// unchanged. With <paramref name="keepTtl"/> the key keeps the TTL it already has (<c>KEEPTTL</c>) instead of
    /// losing it to a plain <c>SET</c>; it cannot be combined with an <paramref name="expiry"/>.
    /// </summary>
    /// <remarks>
    /// The L1 copy is evicted whatever the outcome, exactly as <see cref="SetAsync{T}(string, T, TimeSpan?, CancellationToken)"/>
    /// does: the outcome is only known once the reply is back, and this connection's own writes are not echoed as
    /// invalidations. A write that turned out not to happen therefore costs one local eviction, never a stale read.
    /// <para>
    /// <paramref name="when"/> comes before <paramref name="expiry"/> on purpose. After it, an existing call such as
    /// <c>SetAsync(key, value, expiry, default)</c> would be ambiguous between this overload and the
    /// <see cref="CancellationToken"/> one. What is left ambiguous is a bare <c>default</c> as the third argument
    /// (<c>SetAsync(key, value, default)</c>): write <c>expiry: null</c>, or leave it out.
    /// </para>
    /// <para>
    /// <see cref="When"/> is <c>StackExchange.Redis.When</c>, and it is the third argument, after the value. A call
    /// that leaves the value out, <c>SetAsync(key, When.NotExists)</c>, still compiles: it is the unconditional
    /// overload with the enum itself as the value.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentException"><paramref name="keepTtl"/> is set together with an <paramref name="expiry"/>.</exception>
    /// <exception cref="NotSupportedException">
    /// From this default implementation only, for anything but an unconditional write without <paramref name="keepTtl"/>:
    /// an implementation that predates this member cannot express the condition. The library's own cache supports all of it.
    /// </exception>
    async ValueTask<bool> SetAsync<T>(string key, T value, When when, TimeSpan? expiry = null, bool keepTtl = false, CancellationToken cancellationToken = default)
    {
        Internal.ConditionalWrite.ThrowIfInvalid(expiry, keepTtl);
        Internal.ConditionalWrite.ThrowIfNotExpressible(when, keepTtl);
        await SetAsync(key, value, expiry, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// The raw-bytes form of <see cref="SetAsync{T}(string, T, When, TimeSpan?, bool, CancellationToken)"/>: the same
    /// conditions, <paramref name="keepTtl"/> rule, return value and L1 eviction, without the configured
    /// <see cref="RedisNearCacheOptions.Serializer"/>.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="keepTtl"/> is set together with an <paramref name="expiry"/>.</exception>
    /// <exception cref="NotSupportedException">As for <see cref="SetAsync{T}(string, T, When, TimeSpan?, bool, CancellationToken)"/>.</exception>
    async ValueTask<bool> SetBytesAsync(string key, ReadOnlyMemory<byte> value, When when, TimeSpan? expiry = null, bool keepTtl = false, CancellationToken cancellationToken = default)
    {
        Internal.ConditionalWrite.ThrowIfInvalid(expiry, keepTtl);
        Internal.ConditionalWrite.ThrowIfNotExpressible(when, keepTtl);
        await SetBytesAsync(key, value, expiry, cancellationToken).ConfigureAwait(false);
        return true;
    }

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
