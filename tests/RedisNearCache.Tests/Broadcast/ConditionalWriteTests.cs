using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// The conditional-write overloads in Broadcast mode: a successful <c>NotExists</c> write is our own write, so
/// (as documented on <see cref="ExternalWriteEvictsTests"/>) it echoes back as a self-invalidation and the very
/// next read can legitimately race it, hence <see cref="TestHelpers.ReadUntilCachedAsync"/> rather than a
/// synchronous assertion. A write whose condition is not met performs no Redis write and therefore no echo, but
/// RedisNearCache's own client-side eviction around the write still drops the L1 entry regardless.
/// </summary>
public class ConditionalWriteTests : IClassFixture<BroadcastStandaloneCacheFixture>
{
    private readonly BroadcastStandaloneCacheFixture _fx;

    public ConditionalWriteTests(BroadcastStandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task NotExistsSucceedsThenFailsAndL1StaysCoherent()
    {
        var key = BroadcastKey.New("cond-notexists");
        try
        {
            Assert.True(await _fx.Cache.SetAsync(key, "v1", When.NotExists));
            Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"), $"{key} was not cached after a successful conditional write.");

            Assert.False(await _fx.Cache.SetAsync(key, "v2", When.NotExists));
            var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
            Assert.True(evicted, "a conditional write that did not happen must still evict L1.");

            Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
            Assert.Equal("v1", RedisCli.Standalone("GET", key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }
}
