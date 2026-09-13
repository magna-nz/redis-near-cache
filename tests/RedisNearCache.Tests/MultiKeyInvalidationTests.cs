using Xunit;

namespace RedisNearCache.Tests;

public class MultiKeyInvalidationTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public MultiKeyInvalidationTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task MultiKeyInvalidation()
    {
        var k1 = TestHelpers.Key("k1");
        var k2 = TestHelpers.Key("k2");
        await _fx.Cache.SetAsync(k1, "a1");
        await _fx.Cache.SetAsync(k2, "a2");

        Assert.Equal("a1", await _fx.Cache.GetAsync<string>(k1));
        Assert.Equal("a2", await _fx.Cache.GetAsync<string>(k2));
        Assert.True(_fx.Cache.TryGetLocal<string>(k1, out _));
        Assert.True(_fx.Cache.TryGetLocal<string>(k2, out _));

        RedisCli.Standalone("MSET", k1, "x", k2, "y");

        var bothEvicted = await Poll.UntilAsync(() =>
            !_fx.Cache.TryGetLocal<string>(k1, out _) && !_fx.Cache.TryGetLocal<string>(k2, out _));
        Assert.True(bothEvicted, "both keys were not evicted after MSET within the deadline.");
    }
}
