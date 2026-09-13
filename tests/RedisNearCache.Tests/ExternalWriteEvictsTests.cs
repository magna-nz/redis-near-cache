using Xunit;

namespace RedisNearCache.Tests;

public class ExternalWriteEvictsTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public ExternalWriteEvictsTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task ExternalWriteEvicts()
    {
        var key = TestHelpers.Key("evict");
        await _fx.Cache.SetAsync(key, "v1");
        var initial = await _fx.Cache.GetAsync<string>(key);
        Assert.Equal("v1", initial);
        Assert.True(_fx.Cache.TryGetLocal<string>(key, out var cached));
        Assert.Equal("v1", cached);

        RedisCli.Standalone("SET", key, "v2");

        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
        Assert.True(evicted, "L1 entry was not evicted after external write within the deadline.");

        var after = await _fx.Cache.GetAsync<string>(key);
        Assert.Equal("v2", after);
        Assert.True(_fx.Cache.Statistics.Invalidations >= 1);
    }
}
