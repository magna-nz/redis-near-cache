using Xunit;

namespace RedisNearCache.Tests.Broadcast;

// FLUSHDB wipes every key in the standalone database, including whatever other tests are using. The assembly
// already disables cross-collection parallelization (see AssemblyInfo.cs), but this class is still tagged with
// its own named collection, mirroring RedisNearCache.Tests.FlushDbFlushesL1Tests, so that re-enabling
// parallelization in the future does not silently let this test run alongside anything else.
[Collection("broadcast-flush")]
public class FlushDbFlushesL1Tests : IClassFixture<BroadcastStandaloneCacheFixture>
{
    private readonly BroadcastStandaloneCacheFixture _fx;

    public FlushDbFlushesL1Tests(BroadcastStandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task FlushDbFlushesL1()
    {
        var key = BroadcastKey.New("flush");
        await _fx.Cache.SetAsync(key, "v1");
        // A Broadcast socket is armed for the whole prefix regardless of who reads what, so this SetAsync already
        // generated a self-invalidation before the key was ever read: poll rather than asserting the very next
        // GetAsync is cached synchronously (see the remarks on Broadcast.ExternalWriteEvictsTests).
        Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"), $"{key} was not cached after SetAsync+GetAsync.");

        RedisCli.Standalone("FLUSHDB");

        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
        Assert.True(evicted, "L1 entry was not evicted after FLUSHDB within the deadline.");
        Assert.True(_fx.Cache.Statistics.Flushes >= 1);
    }
}
