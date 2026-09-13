using System.Buffers;
using Microsoft.Extensions.Caching.Distributed;

namespace RedisNearCache.HybridCache;

/// <summary>
/// Adapts <see cref="IRedisNearCache"/> to <see cref="IDistributedCache"/> and
/// <see cref="IBufferDistributedCache"/>, so that anything built against the standard distributed-cache
/// abstractions (session state, output caching, <c>Microsoft.Extensions.Caching.Hybrid.HybridCache</c>) gets
/// server-assisted client-side caching for its reads.
/// </summary>
/// <remarks>
/// Values are stored as raw <c>byte[]</c>/<see cref="ReadOnlySequence{T}"/> through
/// <see cref="IRedisNearCache.GetAsync{T}"/> and <see cref="IRedisNearCache.SetAsync{T}"/> instantiated at
/// <c>byte[]</c>; the default <see cref="IRedisNearCacheSerializer"/> passes <c>byte[]</c> through untouched,
/// so no double-encoding happens.
///
/// <para>
/// <b>Sliding expiration.</b> Redis TTLs (and RedisNearCache's <see cref="IRedisNearCache.SetAsync{T}"/>) have
/// no notion of a sliding window. <see cref="DistributedCacheEntryOptions.SlidingExpiration"/> is therefore
/// mapped to a plain absolute expiry equal to the sliding window, applied once at write time. It is never
/// extended by a later read: <see cref="Refresh"/> and <see cref="RefreshAsync"/> are no-ops, and the Redis
/// TTL set at write time is authoritative until the key expires, is overwritten, or is deleted. Callers that
/// need a true sliding window must re-<see cref="IDistributedCache.Set(string, byte[], DistributedCacheEntryOptions)"/>
/// on each access themselves.
/// </para>
///
/// <para>
/// <b>Sync members.</b> <see cref="IDistributedCache"/> and <see cref="IBufferDistributedCache"/> expose both
/// sync and async members; every sync member here simply calls its async counterpart and blocks with
/// <c>.GetAwaiter().GetResult()</c>. Prefer the async members.
/// </para>
/// </remarks>
public sealed class RedisNearCacheDistributedCache : IDistributedCache, IBufferDistributedCache
{
    private readonly IRedisNearCache _cache;

    /// <summary>Creates the adapter over an already-registered <see cref="IRedisNearCache"/>.</summary>
    public RedisNearCacheDistributedCache(IRedisNearCache cache)
    {
        ArgumentNullException.ThrowIfNull(cache);
        _cache = cache;
    }

    /// <summary>Blocking equivalent of <see cref="GetAsync"/>. Prefer the async member.</summary>
    public byte[]? Get(string key) => GetAsync(key).GetAwaiter().GetResult();

    /// <summary>
    /// Returns the bytes stored at <paramref name="key"/>, served from RedisNearCache's tracked L1 when
    /// present, or <c>null</c> if the key does not exist.
    /// </summary>
    public async Task<byte[]?> GetAsync(string key, CancellationToken token = default) =>
        await _cache.GetAsync<byte[]>(key, token).ConfigureAwait(false);

    /// <summary>Blocking equivalent of <see cref="SetAsync(string, byte[], DistributedCacheEntryOptions, CancellationToken)"/>. Prefer the async member.</summary>
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options) =>
        SetAsync(key, value, options).GetAwaiter().GetResult();

    /// <summary>
    /// Writes <paramref name="value"/> to Redis through RedisNearCache, evicting any local copy so the next
    /// read re-fetches and re-tracks the key. See the type-level remarks for how <paramref name="options"/>
    /// maps to a Redis expiry.
    /// </summary>
    public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        await _cache.SetAsync(key, value, ResolveExpiry(options), token).ConfigureAwait(false);
    }

    /// <summary>
    /// No-op. RedisNearCache does not implement sliding expiration server-side: the Redis TTL set by
    /// <see cref="Set(string, byte[], DistributedCacheEntryOptions)"/> is authoritative and is never extended by a read.
    /// See the type-level remarks.
    /// </summary>
    public void Refresh(string key)
    {
    }

    /// <summary>No-op; see <see cref="Refresh"/>. Completes synchronously without contacting Redis.</summary>
    public Task RefreshAsync(string key, CancellationToken token = default) => Task.CompletedTask;

    /// <summary>Blocking equivalent of <see cref="RemoveAsync"/>. Prefer the async member.</summary>
    public void Remove(string key) => RemoveAsync(key).GetAwaiter().GetResult();

    /// <summary>Deletes <paramref name="key"/> in Redis and evicts any local copy.</summary>
    public async Task RemoveAsync(string key, CancellationToken token = default) =>
        await _cache.RemoveAsync(key, token).ConfigureAwait(false);

    /// <summary>Blocking equivalent of <see cref="TryGetAsync"/>. Prefer the async member.</summary>
    public bool TryGet(string key, IBufferWriter<byte> destination) =>
        TryGetAsync(key, destination, CancellationToken.None).AsTask().GetAwaiter().GetResult();

    /// <summary>
    /// Writes the bytes stored at <paramref name="key"/> into <paramref name="destination"/> and returns
    /// <c>true</c>, or returns <c>false</c> without writing anything if the key does not exist. This is the
    /// member <c>HybridCache</c> prefers over <see cref="GetAsync"/> because it avoids an intermediate
    /// <c>byte[]</c> allocation for the caller.
    /// </summary>
    public async ValueTask<bool> TryGetAsync(string key, IBufferWriter<byte> destination, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        var value = await _cache.GetAsync<byte[]>(key, token).ConfigureAwait(false);
        if (value is null)
        {
            return false;
        }

        destination.Write(value);
        return true;
    }

    /// <summary>Blocking equivalent of <see cref="SetAsync(string, ReadOnlySequence{byte}, DistributedCacheEntryOptions, CancellationToken)"/>. Prefer the async member.</summary>
    public void Set(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options) =>
        SetAsync(key, value, options).AsTask().GetAwaiter().GetResult();

    /// <summary>
    /// Writes <paramref name="value"/> to Redis through RedisNearCache. This is the member
    /// <c>HybridCache</c> prefers over <see cref="SetAsync(string, byte[], DistributedCacheEntryOptions, CancellationToken)"/>
    /// because the caller need not materialize a contiguous <c>byte[]</c> first.
    /// </summary>
    public async ValueTask SetAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        await _cache.SetAsync(key, value.ToArray(), ResolveExpiry(options), token).ConfigureAwait(false);
    }

    /// <summary>
    /// Maps <see cref="DistributedCacheEntryOptions"/> to the single <see cref="TimeSpan"/> expiry that
    /// <see cref="IRedisNearCache.SetAsync{T}"/> accepts. <see cref="DistributedCacheEntryOptions.AbsoluteExpirationRelativeToNow"/>
    /// wins if set; otherwise <see cref="DistributedCacheEntryOptions.AbsoluteExpiration"/> (converted to a
    /// relative duration); otherwise <see cref="DistributedCacheEntryOptions.SlidingExpiration"/>, treated as
    /// an absolute expiry equal to the sliding window (see the type-level remarks). <c>null</c> if none of the
    /// three is set, meaning the key never expires.
    /// </summary>
    private static TimeSpan? ResolveExpiry(DistributedCacheEntryOptions options)
    {
        if (options.AbsoluteExpirationRelativeToNow is { } relativeToNow)
        {
            return relativeToNow;
        }

        if (options.AbsoluteExpiration is { } absolute)
        {
            var now = DateTimeOffset.UtcNow;
            if (absolute <= now)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    absolute,
                    "The absolute expiration value must be in the future.");
            }

            return absolute - now;
        }

        return options.SlidingExpiration;
    }
}
