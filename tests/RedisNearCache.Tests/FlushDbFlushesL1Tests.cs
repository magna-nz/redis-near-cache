using Xunit;

namespace RedisNearCache.Tests;

// FLUSHDB wipes every key in the standalone database, including whatever other tests are using. The
// assembly already disables cross-collection parallelization (see AssemblyInfo.cs), but this class is
// still tagged with its own named collection per the test plan, so that re-enabling parallelization in
// the future does not silently let this test run alongside anything else.
[Collection("flush")]
public class FlushDbFlushesL1Tests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public FlushDbFlushesL1Tests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task FlushDbFlushesL1()
    {
        var key = TestHelpers.Key("flush");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
        Assert.True(_fx.Cache.TryGetLocal<string>(key, out _));

        RedisCli.Standalone("FLUSHDB");

        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
        Assert.True(evicted, "L1 entry was not evicted after FLUSHDB within the deadline.");
        Assert.True(_fx.Cache.Statistics.Flushes >= 1);
    }
}
