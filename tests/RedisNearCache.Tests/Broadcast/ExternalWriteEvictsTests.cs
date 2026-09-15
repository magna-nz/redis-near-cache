using Xunit;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// Unlike Redirect mode (which only tracks a key once this instance has read it), a Broadcast-mode socket is
/// armed for the whole prefix regardless of who reads what, so <c>SetAsync</c> on a fresh key generates a
/// self-invalidation before the key has ever been read here. The very next <c>GetAsync</c> can legitimately race
/// that push (the documented "reply in flight during invalidation" rule then correctly discards the store), so
/// these tests poll for the value to become cached (<see cref="TestHelpers.ReadUntilCachedAsync"/>) rather than
/// asserting it synchronously right after the first read - exactly the pattern already used for reads right
/// after a re-arm elsewhere in the suite.
/// </summary>
public class ExternalWriteEvictsTests : IClassFixture<BroadcastStandaloneCacheFixture>
{
    private readonly BroadcastStandaloneCacheFixture _fx;

    public ExternalWriteEvictsTests(BroadcastStandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task ExternalSetEvicts()
    {
        var key = BroadcastKey.New("set-evict");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"), $"{key} was not cached after SetAsync+GetAsync.");

        var before = _fx.Cache.Statistics.Invalidations;
        RedisCli.Standalone("SET", key, "v2");

        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
        Assert.True(evicted, "L1 entry was not evicted after external SET within the deadline.");

        Assert.Equal("v2", await _fx.Cache.GetAsync<string>(key));
        Assert.True(_fx.Cache.Statistics.Invalidations > before);
    }

    [Fact]
    public async Task ExternalDelEvicts()
    {
        var key = BroadcastKey.New("del-evict");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"), $"{key} was not cached after SetAsync+GetAsync.");

        var before = _fx.Cache.Statistics.Invalidations;
        RedisCli.Standalone("DEL", key);

        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
        Assert.True(evicted, "L1 entry was not evicted after external DEL within the deadline.");
        Assert.True(_fx.Cache.Statistics.Invalidations > before);
    }

    [Fact]
    public async Task ExternalMSetEvictsBothCachedKeys()
    {
        var key1 = BroadcastKey.New("mset1");
        var key2 = BroadcastKey.New("mset2");
        await _fx.Cache.SetAsync(key1, "a1");
        await _fx.Cache.SetAsync(key2, "b1");
        Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key1, "a1"), $"{key1} was not cached after SetAsync+GetAsync.");
        Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key2, "b1"), $"{key2} was not cached after SetAsync+GetAsync.");

        RedisCli.Standalone("MSET", key1, "a2", key2, "b2");

        var evicted1 = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key1, out _));
        var evicted2 = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key2, out _));
        Assert.True(evicted1, "first MSET key was not evicted.");
        Assert.True(evicted2, "second MSET key was not evicted.");

        Assert.Equal("a2", await _fx.Cache.GetAsync<string>(key1));
        Assert.Equal("b2", await _fx.Cache.GetAsync<string>(key2));
    }
}
