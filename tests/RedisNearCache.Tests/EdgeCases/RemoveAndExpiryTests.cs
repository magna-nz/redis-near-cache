using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

public class RemoveAndExpiryTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public RemoveAndExpiryTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task RemoveAsyncEvictsAndReturnsFlag()
    {
        var key = TestHelpers.Key("remove");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
        Assert.True(_fx.Cache.TryGetLocal<string>(key, out _), "key must be cached before RemoveAsync.");

        Assert.True(await _fx.Cache.RemoveAsync(key));
        Assert.False(_fx.Cache.TryGetLocal<string>(key, out _), "RemoveAsync must evict the L1 copy.");

        Assert.False(await _fx.Cache.RemoveAsync(key), "a second RemoveAsync of an already-deleted key must return false.");
        Assert.Null(await _fx.Cache.GetAsync<string>(key));
    }

    [Fact]
    public async Task ServerSideExpiryInvalidates()
    {
        var key = TestHelpers.Key("server-expiry");
        await _fx.Cache.SetAsync(key, "v1", expiry: TimeSpan.FromMilliseconds(500));
        Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
        Assert.True(_fx.Cache.TryGetLocal<string>(key, out _), "key must be cached right after the read.");

        var expired = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
        Assert.True(expired, "the L1 entry was not evicted after the key's server-side TTL expired.");
        Assert.Null(await _fx.Cache.GetAsync<string>(key));
    }
}
