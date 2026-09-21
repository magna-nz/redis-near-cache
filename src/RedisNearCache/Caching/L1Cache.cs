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
    private readonly long _sizeLimit;
    private readonly MemoryCache _cache;
    private long _storeRefusals;
    private int _disposed;

    public L1Cache(RedisNearCacheOptions options)
    {
        _maxAge = options.L1MaxAge;
        _sizeInBytes = options.L1SizeLimitBytes is not null;
        _sizeLimit = options.L1SizeLimitBytes ?? options.L1SizeLimit;
        _cache = CreateCache(_sizeLimit);
    }

    /// <summary>
    /// The size budget <see cref="MemoryCache"/> was given: entries under
    /// <see cref="RedisNearCacheOptions.L1SizeLimit"/>, bytes under
    /// <see cref="RedisNearCacheOptions.L1SizeLimitBytes"/>. Retained because a refused store is only a symptom of
    /// the size-accounting drift in the class remarks when the value would otherwise have fitted: a value larger than
    /// the whole budget is refused by design, and must not be counted as one.
    /// </summary>
    internal long SizeLimit => _sizeLimit;

    /// <summary>
    /// Stores refused by <see cref="MemoryCache"/> for a reason other than the value exceeding
    /// <see cref="SizeLimit"/> on its own. Reported as a counter, so it is a plain monotonic read.
    /// </summary>
    /// <remarks>
    /// This cannot tell the size-accounting drift bug in the class remarks (dotnet/runtime#129186) apart from a
    /// legitimate refusal: a byte budget genuinely full, with compaction not yet caught up, looks identical at the
    /// point of the <see cref="Set"/> that was refused. Under a tight <see cref="RedisNearCacheOptions.L1SizeLimitBytes"/>
    /// with churn, a non-zero count can therefore be entirely benign. The only reliable way to tell them apart is the
    /// internal size total, reachable only by reflection, which this deliberately does not use.
    /// <para>
    /// It errs towards under-counting in the other direction, too: <see cref="Set"/> does not check for a refusal at
    /// all when the entry's own lifetime is under <c>RefusalCheckMinLifetime</c> (50 ms), because an entry that expired
    /// between the store and the check is indistinguishable from one that was refused. A pause longer than the lifetime
    /// of an entry ABOVE that threshold can still be miscounted as a refusal; nothing short of MemoryCache telling us
    /// why a <c>Set</c> did not take can close that, and one-in-a-blue-moon over-count on an idle counter is the
    /// cheaper error than a counter that ticks over every short-lived entry.
    /// </para>
    /// </remarks>
    public long StoreRefusals => Volatile.Read(ref _storeRefusals);

    /// <summary>Records one such refusal. Called from inside <see cref="Set"/>, which already holds the key's stripe.</summary>
    internal void CountStoreRefusal() => Interlocked.Increment(ref _storeRefusals);

    /// <summary>
    /// The shortest entry lifetime <see cref="Set"/> will check for a refusal at all. Below it, the entry can be gone
    /// from the presence check for the ordinary reason that it EXPIRED between the store and the check one statement
    /// later - a preemption or a GC pause longer than the entry's own lifetime - which is indistinguishable from a
    /// refusal and would be counted as one. 50 ms is far longer than any pause between two adjacent dictionary
    /// operations on one thread (a gen-0/gen-1 collection is sub-millisecond to a few, a scheduler quantum on an
    /// oversubscribed machine ~16 ms), and short enough to leave ordinary entries checked: <see cref="RedisNearCacheOptions.L1MaxAge"/>
    /// is minutes by default, and a <see cref="RedisNearCacheOptions.RespectServerTtl"/> cap that lands under 50 ms is
    /// a key about to vanish anyway.
    /// </summary>
    private static readonly TimeSpan RefusalCheckMinLifetime = TimeSpan.FromMilliseconds(50);

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

        // A value that alone cannot fit the whole budget is refused by MemoryCache by design (see SizeLimit); that
        // is not a symptom of the drift bug in the class remarks and must not be counted as a refusal. Nor is an
        // entry whose own lifetime is short enough to expire inside the check below (see RefusalCheckMinLifetime).
        var checkRefusal = (entryOptions.Size is null || entryOptions.Size.Value <= _sizeLimit)
                           && (lifetime is null || lifetime.Value >= RefusalCheckMinLifetime);

        lock (Stripe(key))
        {
            // Always remove first, so MemoryCache.Set below never sees an entry to replace; see the remarks on the class.
            _cache.Remove(key);
            if (lifetime is { } due && due <= TimeSpan.Zero) return;
            _cache.Set(key, value, entryOptions);
            // One extra dictionary lookup, on the store path only (never on TryGet's hit path): a concurrent
            // L1Cache.Remove takes this same stripe lock, Clear() takes every stripe including this one, and the entry
            // was just given a positive expiry, so a miss here means MemoryCache silently refused the store.
            if (checkRefusal && !_cache.TryGetValue(key, out _))
            {
                CountStoreRefusal();
            }
        }
    }

    public void Remove(string key)
    {
        lock (Stripe(key)) _cache.Remove(key);
    }

    public void Clear()
    {
        // Every stripe, in a fixed order, around the clear itself. Set's refusal check asks whether the entry it just
        // stored is still there, and MemoryCache.Clear detaches its whole coherent state with an Interlocked.Exchange
        // and then walks the ALREADY-DETACHED old state raising the expiry callbacks; so the race is not that the
        // clear removes the new entry (it cannot - it never touches the new state), it is that a store lands in the
        // state that is about to be swapped away: Set stores into the old state, the exchange replaces it, and Set's
        // presence check then looks in the new, empty one and reads a flush as a silent refusal. Holding the stripes
        // makes the store and its check atomic with respect to a flush, which is what makes the counter exact.
        // The cost is 64 uncontended monitors on a path that already replaces the backing collection, and a flush is
        // rare (a re-arm, an endpoint removal, a server flush) where a store is per miss. Deadlock-free: Set and
        // Remove take exactly one stripe and never call in here.
        ClearUnderStripe(0);
    }

    /// <summary>Takes every stripe in index order, innermost frame doing the clear.</summary>
    private void ClearUnderStripe(int stripe)
    {
        if (stripe == StripeCount)
        {
            _cache.Clear();
            return;
        }

        lock (_stripes[stripe]) ClearUnderStripe(stripe + 1);
    }

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
