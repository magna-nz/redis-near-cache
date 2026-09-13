using RedisNearCache.Internal;
using Xunit;

namespace RedisNearCache.Tests;

public class KillInteractiveRearmsTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public KillInteractiveRearmsTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task KillInteractiveRearms()
    {
        var sawInteractiveRestored = false;
        _fx.Armer.Armed += e =>
        {
            if (e.Reason == ArmReason.InteractiveRestored) sawInteractiveRestored = true;
        };

        var db = _fx.Connection.Multiplexer.GetDatabase();
        var interactiveId = (long)await db.ExecuteAsync("CLIENT", "ID");

        RedisCli.Standalone("CLIENT", "KILL", "ID", interactiveId.ToString());

        var rearmed = await Poll.UntilAsync(
            () => sawInteractiveRestored && _fx.Armer.RedirectTargets.Count > 0,
            TimeSpan.FromSeconds(5));
        Assert.True(rearmed, "expected an Armed event with reason InteractiveRestored and RedirectTargets repopulated.");

        // Prove tracking actually works again: read, external write, evicted.
        var key = TestHelpers.Key("interactive-rearm");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
        Assert.True(_fx.Cache.TryGetLocal<string>(key, out _));

        RedisCli.Standalone("SET", key, "v2");
        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
        Assert.True(evicted, "tracking did not evict the key after re-arm.");

        Assert.True(_fx.Cache.Statistics.Rearms >= 1);
    }
}
