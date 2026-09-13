using Xunit;

namespace RedisNearCache.Tests;

public class HitServesLocallyTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public HitServesLocallyTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task HitServesLocally()
    {
        var key = TestHelpers.Key("hit");
        await _fx.Cache.SetAsync(key, "v1");

        var server = _fx.Server();

        var first = await _fx.Cache.GetAsync<string>(key);
        var callsAfterFirst = TestHelpers.CommandCalls(server, "get");

        var second = await _fx.Cache.GetAsync<string>(key);
        var callsAfterSecond = TestHelpers.CommandCalls(server, "get");

        Assert.Equal("v1", first);
        Assert.Equal("v1", second);
        Assert.Equal(1, _fx.Cache.Statistics.Hits);
        Assert.Equal(callsAfterFirst, callsAfterSecond);
    }
}
