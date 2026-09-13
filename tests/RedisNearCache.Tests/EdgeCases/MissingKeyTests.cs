using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>A key that does not exist in Redis at all must never be stored in L1, however many times it is read.</summary>
public class MissingKeyTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public MissingKeyTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task MissingKeyIsNotCached()
    {
        var key = TestHelpers.Key("missing");
        var missesBefore = _fx.Cache.Statistics.Misses;

        var value = await _fx.Cache.GetAsync<string>(key);
        Assert.Null(value);
        Assert.False(_fx.Cache.TryGetLocal<string>(key, out _), "a null reply must never populate L1.");
        Assert.Equal(missesBefore + 1, _fx.Cache.Statistics.Misses);

        RedisCli.Standalone("SET", key, "now-it-exists");

        var afterCreate = await _fx.Cache.GetAsync<string>(key);
        Assert.Equal("now-it-exists", afterCreate);
    }
}
