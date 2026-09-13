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
}
