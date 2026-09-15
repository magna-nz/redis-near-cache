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
        var flushesBefore = _fx.Cache.Statistics.Flushes;

        RedisCli.Standalone("FLUSHDB");

        // Wait for the flush itself, not merely for the key to leave L1: the SetAsync above echoes back as a push in
        // Broadcast mode and can evict the key on its own before the FLUSHDB's null invalidation arrives.
        var flushed = await Poll.UntilAsync(() => _fx.Cache.Statistics.Flushes > flushesBefore, TimeSpan.FromSeconds(10));
        Assert.True(flushed, "FLUSHDB did not flush L1 (no null invalidation handled) within the deadline.");
        Assert.False(_fx.Cache.TryGetLocal<string>(key, out _), "the key was still in L1 after the flush.");
    }
}
