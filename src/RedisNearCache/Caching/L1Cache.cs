using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Caching.Memory;

namespace RedisNearCache.Caching;

/// <summary>
/// The in-process L1 store. A thin wrapper over <see cref="MemoryCache"/>: every entry costs a size of 1
/// against <see cref="RedisNearCacheOptions.L1SizeLimit"/> - or the length of its value against
/// <see cref="RedisNearCacheOptions.L1SizeLimitBytes"/> when that is set - and, unless
/// <see cref="RedisNearCacheOptions.L1MaxAge"/> is <see cref="Timeout.InfiniteTimeSpan"/>, expires after that
/// age as a safety net against a missed invalidation.
/// </summary>
internal sealed class L1Cache : IDisposable
{
    private readonly TimeSpan _maxAge;
    private readonly bool _sizeInBytes;
    private readonly MemoryCache _cache;
    private int _disposed;

    public L1Cache(RedisNearCacheOptions options)
    {
        _maxAge = options.L1MaxAge;
        _sizeInBytes = options.L1SizeLimitBytes is not null;
        _cache = CreateCache(options.L1SizeLimitBytes ?? options.L1SizeLimit);
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
    /// nothing: the key is already due to expire. Under a byte budget an entry too big for it is dropped by
    /// MemoryCache itself; the caller still gets the value it read.
    /// </summary>
    public void Set(string key, byte[] value, TimeSpan? maxAge = null)
    {
        // An empty value still occupies an entry, so it must not cost nothing: a zero cost would let an unbounded
        // number of them in.
        var entryOptions = new MemoryCacheEntryOptions { Size = _sizeInBytes ? Math.Max(1, value.Length) : 1 };
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

    /// <summary>Entries held right now. Reported as a gauge, so a dispose racing the reader answers 0 rather than throwing.</summary>
    public long Count => Volatile.Read(ref _disposed) == 1 ? 0 : _cache.Count;

    public void Dispose()
    {
        Volatile.Write(ref _disposed, 1);
        _cache.Dispose();
    }
}
