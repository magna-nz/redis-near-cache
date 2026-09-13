using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

public class ConcurrencyTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public ConcurrencyTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task ConcurrentReadsOfSameKeyAllSucceed()
    {
        var key = TestHelpers.Key("concurrent-cold");
        RedisCli.Standalone("SET", key, "v1");

        var before = _fx.Cache.Statistics.Hits + _fx.Cache.Statistics.Misses;

        var tasks = Enumerable.Range(0, 32)
            .Select(_ => _fx.Cache.GetAsync<string>(key).AsTask())
            .ToArray();
        var results = await Task.WhenAll(tasks);

        Assert.All(results, r => Assert.Equal("v1", r));

        var after = _fx.Cache.Statistics.Hits + _fx.Cache.Statistics.Misses;
        Assert.Equal(32, after - before);
    }
}
