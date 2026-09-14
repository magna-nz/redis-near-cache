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
        // The second of the two re-arms may still be gating caching; poll until a read is cached again.
        Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"), "key was not re-cached after the re-arm.");

        RedisCli.Standalone("SET", key, "v2");
        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
        Assert.True(evicted, "tracking did not evict the key after re-arm.");

        Assert.True(_fx.Cache.Statistics.Rearms >= 1);
    }
}
