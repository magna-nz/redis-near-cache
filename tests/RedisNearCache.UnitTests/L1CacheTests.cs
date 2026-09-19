using RedisNearCache.Caching;
using Xunit;

namespace RedisNearCache.UnitTests;

public class L1CacheTests
{
    private static L1Cache Create(long sizeLimit = 100, TimeSpan? maxAge = null) =>
        new(new RedisNearCacheOptions { L1SizeLimit = sizeLimit, L1MaxAge = maxAge ?? TimeSpan.FromMinutes(5) });

    [Fact]
    public void SetGetRemoveClear()
    {
        using var l1 = Create();
        Assert.False(l1.TryGet("k", out _));
        l1.Set("k", [1, 2, 3]);
        Assert.True(l1.TryGet("k", out var v));
        Assert.Equal(new byte[] { 1, 2, 3 }, v);
        Assert.Equal(1, l1.Count);
        l1.Remove("k");
        Assert.False(l1.TryGet("k", out _));
        l1.Set("a", [1]); l1.Set("b", [2]);
        l1.Clear();
        Assert.Equal(0, l1.Count);
        Assert.False(l1.TryGet("a", out _));
    }

    [Fact]
    public void SizeLimitIsEnforced()
    {
        using var l1 = Create(sizeLimit: 5);
        for (var i = 0; i < 20; i++) l1.Set("k" + i, [1]);
        Assert.True(l1.Count <= 5, $"count was {l1.Count}");
    }

    [Fact]
    public async Task MaxAgeExpiresEntries()
    {
        using var l1 = Create(maxAge: TimeSpan.FromMilliseconds(60));
        l1.Set("k", [1]);
        Assert.True(l1.TryGet("k", out _));
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(3);
        while (DateTime.UtcNow < deadline && l1.TryGet("k", out _)) await Task.Delay(20);
        Assert.False(l1.TryGet("k", out _));
    }

    [Fact]
    public async Task InfiniteMaxAgeKeepsEntries()
    {
        using var l1 = Create(maxAge: Timeout.InfiniteTimeSpan);
        l1.Set("k", [1]);
        await Task.Delay(100);
        Assert.True(l1.TryGet("k", out _));
    }

    [Fact]
    public void RemoveOfMissingKeyIsHarmless()
    {
        using var l1 = Create();
        l1.Remove("nope");
        Assert.Equal(0, l1.Count);
    }

    private static L1Cache CreateBounded(long bytes) =>
        new(new RedisNearCacheOptions { L1SizeLimitBytes = bytes, L1MaxAge = TimeSpan.FromMinutes(5) });

    [Fact]
    public async Task SizeLimitBytesBoundsTheTotalValueLength()
    {
        using var l1 = CreateBounded(bytes: 100);
        var keys = Enumerable.Range(0, 10).Select(i => "k" + i).ToArray();
        foreach (var key in keys) l1.Set(key, new byte[40]);

        // MemoryCache compacts on a thread-pool thread, so the budget is only met eventually.
        long HeldBytes() => keys.Count(k => l1.TryGet(k, out _)) * 40L;
        var withinBudget = await PollUntil(() => HeldBytes() <= 100);

        Assert.True(withinBudget, $"expected at most 100 bytes held; found {HeldBytes()} across {l1.Count} entries.");
        Assert.True(l1.Count <= 2, $"40-byte values under a 100-byte budget means at most 2 entries; found {l1.Count}.");
    }

    [Fact]
    public void SizeLimitBytesRejectedOverwriteDoesNotLeaveTheOlderValueBehind()
    {
        // The one byte-budget case that touches coherence: if MemoryCache refused the new value but kept the old
        // entry, L1 would go on serving a value Redis no longer holds.
        using var l1 = CreateBounded(bytes: 100);
        l1.Set("k", new byte[10]);
        Assert.True(l1.TryGet("k", out _));

        l1.Set("k", new byte[101]);

        Assert.False(l1.TryGet("k", out _), "a refused overwrite must drop the entry, not keep serving the value it replaced.");
    }

    [Fact]
    public void SizeLimitBytesRefusesAValueBiggerThanTheWholeBudget()
    {
        using var l1 = CreateBounded(bytes: 100);
        l1.Set("big", new byte[101]);
        Assert.False(l1.TryGet("big", out _), "a value larger than the whole budget must never be stored.");

        // It also does not poison the budget for everything else.
        l1.Set("small", new byte[10]);
        Assert.True(l1.TryGet("small", out _));
    }

    [Fact]
    public void SizeLimitBytesChargesAnEmptyValueOne()
    {
        using var l1 = CreateBounded(bytes: 1);
        l1.Set("empty", []);
        Assert.True(l1.TryGet("empty", out var v));
        Assert.Empty(v);
    }

    [Fact]
    public void WithoutABytesLimitEveryEntryCostsOne()
    {
        // Three entries far bigger than the limit taken as bytes: under the entry-count default all three fit.
        using var l1 = Create(sizeLimit: 3);
        for (var i = 0; i < 3; i++) l1.Set("k" + i, new byte[1000]);
        for (var i = 0; i < 3; i++) Assert.True(l1.TryGet("k" + i, out _), $"k{i} was evicted although the limit counts entries, not bytes.");
        Assert.Equal(3, l1.Count);
    }

    private static async Task<bool> PollUntil(Func<bool> condition, int timeoutMs = 3000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline) return condition();
            await Task.Delay(20);
        }
        return true;
    }
}

/// <summary>
/// <see cref="RedisNearCacheStatistics.L1Entries"/> is a live gauge over an L1 store the statistics object does not
/// own, so it is wired by <c>AttachL1</c> and must stay harmless once that store is gone.
/// </summary>
public class StatisticsL1EntriesTests
{
    [Fact]
    public void ZeroWithNothingAttached()
    {
        var stats = new RedisNearCacheStatistics();
        Assert.Equal(0, stats.L1Entries);
        Assert.Contains("l1Entries=0", stats.ToString());
    }

    [Fact]
    public void ReflectsSetRemoveAndClear()
    {
        var stats = new RedisNearCacheStatistics();
        using var l1 = new L1Cache(new RedisNearCacheOptions());
        stats.AttachL1(() => l1.Count);

        Assert.Equal(0, stats.L1Entries);
        l1.Set("a", [1]);
        l1.Set("b", [2]);
        Assert.Equal(2, stats.L1Entries);
        Assert.Contains("l1Entries=2", stats.ToString());

        l1.Remove("a");
        Assert.Equal(1, stats.L1Entries);

        l1.Clear();
        Assert.Equal(0, stats.L1Entries);
    }

    [Fact]
    public void ZeroAndNoThrowAfterTheStoreIsDisposed()
    {
        var stats = new RedisNearCacheStatistics();
        var l1 = new L1Cache(new RedisNearCacheOptions());
        stats.AttachL1(() => l1.Count);
        l1.Set("a", [1]);
        Assert.Equal(1, stats.L1Entries);

        l1.Dispose();

        Assert.Equal(0, stats.L1Entries);
        Assert.Contains("l1Entries=0", stats.ToString());

        // Detaching is what the facade does on dispose; it must be just as harmless.
        stats.DetachL1();
        Assert.Equal(0, stats.L1Entries);
    }
}
