using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Caching.Memory;

namespace RedisNearCache.Caching;

/// <summary>
/// The in-process L1 store. A thin wrapper over <see cref="MemoryCache"/>: every entry costs a size of 1
/// against <see cref="RedisNearCacheOptions.L1SizeLimit"/> and, unless
/// <see cref="RedisNearCacheOptions.L1MaxAge"/> is <see cref="Timeout.InfiniteTimeSpan"/>, expires after that
/// age as a safety net against a missed invalidation.
/// </summary>
internal sealed class L1Cache : IDisposable
{
    private readonly TimeSpan _maxAge;
    private readonly MemoryCache _cache;

    public L1Cache(RedisNearCacheOptions options)
    {
        _maxAge = options.L1MaxAge;
        _cache = CreateCache(options.L1SizeLimit);
    }

    private static MemoryCache CreateCache(long sizeLimit) => new(new MemoryCacheOptions { SizeLimit = sizeLimit });

    public bool TryGet(string key, [NotNullWhen(true)] out byte[]? value)
    {
        if (_cache.TryGetValue(key, out var cached) && cached is byte[] bytes)
        {
            value = bytes;
            return true;
        }

        value = null;
        return false;
    }

    /// <summary>
    /// Stores the entry for at most <see cref="RedisNearCacheOptions.L1MaxAge"/>, or for <paramref name="maxAge"/>
    /// when that is shorter (the key's remaining server-side TTL). A non-positive <paramref name="maxAge"/> stores
    /// nothing: the key is already due to expire.
    /// </summary>
    public void Set(string key, byte[] value, TimeSpan? maxAge = null)
    {
        var entryOptions = new MemoryCacheEntryOptions { Size = 1 };
        TimeSpan? lifetime = _maxAge == Timeout.InfiniteTimeSpan ? null : _maxAge;
        if (maxAge is { } cap && (lifetime is null || cap < lifetime.Value)) lifetime = cap;
        if (lifetime is { } age)
        {
            if (age <= TimeSpan.Zero)
            {
                _cache.Remove(key);
                return;
            }

            entryOptions.AbsoluteExpirationRelativeToNow = age;
        }

        _cache.Set(key, value, entryOptions);
    }

    public void Remove(string key) => _cache.Remove(key);

    public void Clear() => _cache.Clear();

    public long Count => _cache.Count;

    public void Dispose() => _cache.Dispose();
}
