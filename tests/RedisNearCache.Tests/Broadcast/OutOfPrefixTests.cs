using Xunit;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// A key outside <see cref="RedisNearCacheOptions.KeyPrefixes"/> is read through the cache with a plain GET, is
/// never stored in L1, and a foreign write to it never bumps <c>Statistics.Invalidations</c> because the server
/// never broadcasts it to this client's prefix-scoped socket.
/// </summary>
public class OutOfPrefixTests : IClassFixture<BroadcastStandaloneCacheFixture>
{
    private readonly BroadcastStandaloneCacheFixture _fx;

    public OutOfPrefixTests(BroadcastStandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task KeyOutsidePrefixIsNeverCachedAndForeignWriteDoesNotBumpInvalidations()
    {
        // TestHelpers.Key uses the "t:" prefix, which is outside BroadcastKey.Prefix ("bc:").
        var key = TestHelpers.Key("outside-bcast-prefix");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
        Assert.False(_fx.Cache.TryGetLocal<string>(key, out _), "a key outside KeyPrefixes must never be cached in L1 in Broadcast mode.");

        var before = _fx.Cache.Statistics.Invalidations;
        RedisCli.Standalone("SET", key, "v2");

        // Negative assertion: give any (unwanted) invalidation a real chance to arrive before checking it didn't.
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        Assert.Equal(before, _fx.Cache.Statistics.Invalidations);

        Assert.Equal("v2", await _fx.Cache.GetAsync<string>(key));
        Assert.False(_fx.Cache.TryGetLocal<string>(key, out _));
    }
}
