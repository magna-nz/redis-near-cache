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
