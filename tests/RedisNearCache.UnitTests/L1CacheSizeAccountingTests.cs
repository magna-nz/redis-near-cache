using System.Reflection;
using Microsoft.Extensions.Caching.Memory;
using RedisNearCache.Caching;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// Regression tests for the L1 store going permanently deaf under load.
/// </summary>
/// <remarks>
/// <c>MemoryCache</c> (Microsoft.Extensions.Caching.Memory 9.0 through at least 10.0.12; dotnet/runtime#129186, fixed
/// for 11.0 by #129215) keeps a running total of entry sizes
/// for its <c>SizeLimit</c>. A <c>Set</c> that finds an existing entry for the key subtracts that entry's size on
/// the assumption that it is replacing it; if something else removes that entry at the same moment - a
/// <c>Remove</c>, the expiry scan, or a lookup that trips over an expired entry - its size is subtracted twice. The
/// total only ever drifts down. Once it is below zero by more than an entry's size, the capacity check (done
/// unsigned) reads it as far over the limit and every later <c>Set</c> is refused, silently and for good: the cache
/// keeps answering, caches nothing, and nothing reports it. Reads racing invalidations on hot keys are exactly that
/// pattern, which is what an invalidation-driven near cache produces all day.
/// <para>
/// <see cref="L1Cache"/> therefore never lets <c>MemoryCache</c> see an existing entry in <c>Set</c>: under a lock
/// for the key it removes the key and then sets it. These tests run the storm against <see cref="L1Cache"/> itself
/// and then demand that the accounting is exact and that new keys are still accepted.
/// </para>
/// </remarks>
public class L1CacheSizeAccountingTests
{
    private static readonly string[] HotKeys = Enumerable.Range(0, 12).Select(i => "hot" + i).ToArray();

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AStormOfStoresInvalidationsAndExpiriesLeavesTheAccountingExactAndTheCacheStillStoring(bool byteBudget)
    {
        var options = new RedisNearCacheOptions { L1SizeLimit = 10_000, L1MaxAge = TimeSpan.FromMinutes(5) };
        if (byteBudget) options.L1SizeLimitBytes = 1_000_000;
        using var l1 = new L1Cache(options);

        var stop = DateTime.UtcNow.AddSeconds(1.5);
        long stores = 0, removes = 0;
        var actors = new List<Task>();

        // Stores with a lifetime of a couple of milliseconds, so MemoryCache's own expired-entry removal (in its scan
        // and inside TryGetValue) races the stores too, not only our Remove.
        for (var i = 0; i < 4; i++) actors.Add(Run(rnd => { l1.Set(HotKeys[rnd.Next(HotKeys.Length)], new byte[8], TimeSpan.FromMilliseconds(2)); Interlocked.Increment(ref stores); }));
        for (var i = 0; i < 2; i++) actors.Add(Run(rnd => { l1.Remove(HotKeys[rnd.Next(HotKeys.Length)]); Interlocked.Increment(ref removes); }));
        for (var i = 0; i < 2; i++) actors.Add(Run(rnd => l1.TryGet(HotKeys[rnd.Next(HotKeys.Length)], out _)));
        await Task.WhenAll(actors);

        // The storm has to have been one for the test to mean anything.
        Assert.True(stores > 10_000 && removes > 10_000, $"too little contention to prove anything: stores={stores}, removes={removes}.");

        foreach (var key in HotKeys) l1.Remove(key);
        Assert.Equal(0, l1.Count);

        // Exact, where MemoryCache lets us look: an empty cache must account for nothing. Any drift at all fails here.
        if (ReadSize(l1) is { } size)
        {
            Assert.True(size == 0, $"MemoryCache's size total is {size} with no entries held (after {stores} stores and {removes} removes).");
        }

        // And behaviourally, whatever the internals are called in a later package: new keys are still accepted.
        for (var i = 0; i < 5; i++)
        {
            var fresh = "fresh" + i;
            l1.Set(fresh, new byte[8]);
            Assert.True(l1.TryGet(fresh, out _), $"L1 refused a new key after the storm (stores={stores}, removes={removes}, size={ReadSize(l1)}).");
        }

        Assert.Equal(5, l1.Count);
        if (ReadSize(l1) is { } finalSize) Assert.Equal(byteBudget ? 40 : 5, finalSize);

        Task Run(Action<Random> body) => Task.Factory.StartNew(
            () =>
            {
                var rnd = new Random();
                while (DateTime.UtcNow < stop) body(rnd);
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    /// <summary>An overwrite must still replace the value: removing first must not turn Set into "set if absent".</summary>
    [Fact]
    public void SetStillOverwritesAnExistingEntry()
    {
        using var l1 = new L1Cache(new RedisNearCacheOptions());
        l1.Set("k", [1]);
        l1.Set("k", [2, 2]);

        Assert.True(l1.TryGet("k", out var value));
        Assert.Equal(new byte[] { 2, 2 }, value);
        Assert.Equal(1, l1.Count);
        if (ReadSize(l1) is { } size) Assert.Equal(1, size);
    }

    // --- Set's own store-refusal detection (StoreRefusals) ---------------------------------------------------

    /// <summary>
    /// A genuine refusal: with an entry-count budget of 1, MemoryCache's overcapacity compaction rounds
    /// <c>1 * CompactionPercentage</c> down to 0 entries to evict, so a second distinct key is refused outright
    /// (the first key survives untouched) rather than evicted to make room. Deterministic on both net8.0 and
    /// net10.0 (checked directly against MemoryCache before writing this test).
    /// </summary>
    [Fact]
    public void ARefusedStoreIsCounted()
    {
        using var l1 = new L1Cache(new RedisNearCacheOptions { L1SizeLimit = 1 });
        l1.Set("a", [1]);
        Assert.Equal(0, l1.StoreRefusals);

        l1.Set("b", [2]);

        Assert.True(l1.TryGet("a", out _), "the first key must still be there for this to be a refusal rather than an eviction.");
        Assert.False(l1.TryGet("b", out _), "the second key must have been refused for the counter assertion to mean anything.");
        Assert.Equal(1, l1.StoreRefusals);
    }

    /// <summary>
    /// The same genuine refusal as <see cref="ARefusedStoreIsCounted"/>, but on an entry whose own lifetime is shorter
    /// than <c>L1Cache.RefusalCheckMinLifetime</c>. <c>Set</c>'s presence check cannot tell "refused" from "expired
    /// between the store and the check one statement later" - a preemption longer than 2 ms is all it takes - so such
    /// an entry is not checked at all and nothing is counted. An entry whose lifetime is above the threshold still is:
    /// the guard is about lifetimes too short to be distinguishable, not about capped entries in general.
    /// </summary>
    [Fact]
    public void ARefusedStoreTooShortLivedToTellFromAnExpiryIsNotCounted()
    {
        using var l1 = new L1Cache(new RedisNearCacheOptions { L1SizeLimit = 1 });
        l1.Set("a", [1]);
        Assert.Equal(0, l1.StoreRefusals);

        l1.Set("short", [2], TimeSpan.FromMilliseconds(2));

        Assert.True(l1.TryGet("a", out _), "the first key must still be there for this to be a refusal rather than an eviction.");
        Assert.False(l1.TryGet("short", out _), "the store must have been refused for the counter assertion to mean anything.");
        Assert.Equal(0, l1.StoreRefusals);

        // Identical refusal, a lifetime that cannot plausibly expire inside the check: counted.
        l1.Set("long", [3], TimeSpan.FromSeconds(30));

        Assert.False(l1.TryGet("long", out _));
        Assert.Equal(1, l1.StoreRefusals);
    }

    /// <summary>
    /// Documented behaviour (see <see cref="L1CacheTests.SizeLimitBytesRefusesAValueBiggerThanTheWholeBudget"/> and
    /// <c>L1ConfigurationTests</c> in the integration suite): a value that alone cannot fit the whole byte budget is
    /// refused by design, not a symptom of the size-accounting drift bug, and must not inflate the counter.
    /// </summary>
    [Fact]
    public void AValueBiggerThanTheWholeByteBudgetIsNotCountedAsARefusal()
    {
        using var l1 = new L1Cache(new RedisNearCacheOptions { L1SizeLimitBytes = 100 });

        l1.Set("big", new byte[101]);

        Assert.False(l1.TryGet("big", out _));
        Assert.Equal(0, l1.StoreRefusals);
    }

    /// <summary>
    /// <see cref="L1Cache.Clear"/> takes every stripe around <c>MemoryCache.Clear</c>, which is what makes
    /// <c>Set</c>'s store and its own presence check atomic with respect to a flush. Without that, a store that landed
    /// in the state the clear is about to swap away would find itself missing and be misread as a refusal. With a
    /// generous budget (no genuine refusal is possible) and continuous concurrent stores and clears, any refusal at
    /// all proves those locks are missing or wrong.
    /// </summary>
    [Fact]
    public async Task AClearRacingAStoreIsNotCountedAsARefusal()
    {
        using var l1 = new L1Cache(new RedisNearCacheOptions { L1SizeLimit = 10_000 });
        var stop = DateTime.UtcNow.AddMilliseconds(500);

        var keys = Enumerable.Range(0, 12).Select(i => "k" + i).ToArray();
        var rnd = new Random();
        var setter = Task.Run(() => { while (DateTime.UtcNow < stop) l1.Set(keys[rnd.Next(keys.Length)], [1, 2, 3]); });
        var clearer = Task.Run(async () => { while (DateTime.UtcNow < stop) { l1.Clear(); await Task.Delay(5); } });
        await Task.WhenAll(setter, clearer);

        Assert.Equal(0, l1.StoreRefusals);
    }

    /// <summary>
    /// <c>MemoryCache.Size</c> is internal. Null when it cannot be read (a renamed member in a later package), in
    /// which case the behavioural assertions still stand on their own.
    /// </summary>
    private static long? ReadSize(L1Cache l1)
    {
        var memoryCache = typeof(L1Cache).GetField("_cache", BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(l1) as MemoryCache;
        var size = typeof(MemoryCache).GetProperty("Size", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)?.GetValue(memoryCache);
        return size is long value ? value : null;
    }
}
