using Xunit;

namespace RedisNearCache.Tests;

public class SetThroughCacheEvictsAndRetracksTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public SetThroughCacheEvictsAndRetracksTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task SetThroughCacheEvictsAndRetracks()
    {
        var key = TestHelpers.Key("set");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));

        await _fx.Cache.SetAsync(key, "v2");
        Assert.False(_fx.Cache.TryGetLocal<string>(key, out _), "SetAsync must evict the L1 copy.");

        var afterSet = await _fx.Cache.GetAsync<string>(key);
        Assert.Equal("v2", afterSet);

        var hitsBefore = _fx.Cache.Statistics.Hits;
        var secondRead = await _fx.Cache.GetAsync<string>(key);
        Assert.Equal("v2", secondRead);
        Assert.Equal(hitsBefore + 1, _fx.Cache.Statistics.Hits);
    }
}
