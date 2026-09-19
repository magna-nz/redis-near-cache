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
/// <remarks>
/// Not quite thin: <see cref="Set"/> and <see cref="Remove"/> take a lock for the key, and <see cref="Set"/> removes
/// the key before it stores it. That is a workaround for <see cref="MemoryCache"/>'s size accounting in
/// Microsoft.Extensions.Caching.Memory 9.0 through at least 10.0.12 (dotnet/runtime#129186; fixed for 11.0 by #129215,
/// 8.0 is clean). It can go once the package floor is a build that has that fix - not before: a consuming
/// application resolves its own version, and the storm tests fail at once without this. Its <c>Set</c>, finding an entry already there, subtracts that
/// entry's size on the assumption that it is replacing it; if anything removes the entry at that moment - our
/// <see cref="Remove"/> for an invalidation, its own expiry scan, a lookup that trips over an expired entry - the
/// size is subtracted twice. The total only drifts down, and once it is below zero the capacity check, done
/// unsigned, reads it as over the limit: every later <c>Set</c> is refused, silently and for good, until a
/// <see cref="Clear"/>. Reads racing invalidations on hot keys are exactly that pattern.
/// <para>
/// The lock alone is not enough, because the expiry scan and <c>TryGetValue</c> remove entries outside it. Removing
/// first is what closes it: with stores of one key serialised, <c>MemoryCache.Set</c> never finds an entry to
/// replace, so the branch that double-counts is never taken, whoever else is removing. Nor is removing first enough
/// without the lock: a second store of the key would find the first one's entry. <see cref="TryGet"/> stays
/// lock-free; the cost is one lock per miss and per invalidation, contended only on a key several threads are
/// storing or invalidating at once, and held for a remove and an insert (no callbacks are registered, so nothing of
/// ours runs inside it). A reader between the remove and the set sees a miss where it used to see the value being
/// replaced, which costs it a read from Redis, nothing more.
/// </para>
/// </remarks>
internal sealed class L1Cache : IDisposable
{
    // Striped rather than per key: a lock object per key would need its own eviction. A power of two, so the stripe
    // is a mask of the hash; collisions only serialise two unrelated keys for the length of a dictionary operation.
    private const int StripeCount = 64;

    private readonly object[] _stripes = CreateStripes();
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
        if (lifetime is { } age && age > TimeSpan.Zero) entryOptions.AbsoluteExpirationRelativeToNow = age;

        lock (Stripe(key))
        {
            // Always remove first, so MemoryCache.Set below never sees an entry to replace; see the remarks on the class.
            _cache.Remove(key);
            if (lifetime is { } due && due <= TimeSpan.Zero) return;
            _cache.Set(key, value, entryOptions);
        }
    }

    public void Remove(string key)
    {
        lock (Stripe(key)) _cache.Remove(key);
    }

    public void Clear() => _cache.Clear();

    /// <summary>Entries held right now. Reported as a gauge, so a dispose racing the reader answers 0 rather than throwing.</summary>
    public long Count => Volatile.Read(ref _disposed) == 1 ? 0 : _cache.Count;

    public void Dispose()
    {
        Volatile.Write(ref _disposed, 1);
        _cache.Dispose();
    }

    private object Stripe(string key) => _stripes[key.GetHashCode() & (StripeCount - 1)];

    private static object[] CreateStripes()
    {
        var stripes = new object[StripeCount];
        for (var i = 0; i < stripes.Length; i++) stripes[i] = new object();
        return stripes;
    }
}
