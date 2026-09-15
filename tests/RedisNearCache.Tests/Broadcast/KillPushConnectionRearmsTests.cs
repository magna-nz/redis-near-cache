using RedisNearCache.Internal;
using Xunit;

namespace RedisNearCache.Tests.Broadcast;

public class KillPushConnectionRearmsTests : IClassFixture<BroadcastStandaloneCacheFixture>
{
    private readonly BroadcastStandaloneCacheFixture _fx;

    public KillPushConnectionRearmsTests(BroadcastStandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task KillingTheBroadcastSocketRearmsAndResumesInvalidations()
    {
        var endpoint = _fx.Connection.Multiplexer.GetEndPoints()[0];
        var oldClientId = _fx.Armer.RedirectTargets[endpoint];

        // Cache a key before the kill, so we can prove it is dropped by the loss (per the "any reconnect means
        // re-arm then flush L1" rule) rather than surviving the outage by coincidence.
        var beforeKey = BroadcastKey.New("before-kill");
        await _fx.Cache.SetAsync(beforeKey, "v1");
        Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, beforeKey, "v1"));

        var sawTrackingLost = false;
        var sawPushConnectionRestored = false;
        _fx.Armer.TrackingLost += ep =>
        {
            if (Equals(ep, endpoint)) sawTrackingLost = true;
        };
        _fx.Armer.Armed += e =>
        {
            if (Equals(e.EndPoint, endpoint) && e.Reason == ArmReason.PushConnectionRestored) sawPushConnectionRestored = true;
        };

        RedisCli.Standalone("CLIENT", "KILL", "ID", oldClientId.ToString());

        var rearmed = await Poll.UntilAsync(
            () => sawTrackingLost
                  && sawPushConnectionRestored
                  && _fx.Armer.RedirectTargets.TryGetValue(endpoint, out var current)
                  && current != oldClientId,
            TimeSpan.FromSeconds(5));
        Assert.True(rearmed, "expected TrackingLost then an Armed event with reason PushConnectionRestored and a changed client id.");
        Assert.True(_fx.Cache.Statistics.Rearms >= 1);

        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(beforeKey, out _));
        Assert.True(evicted, "the key cached before the kill must be gone from L1 after the loss/re-arm.");

        // Prove the re-armed socket is genuinely live: a foreign write after the re-arm still evicts.
        var afterKey = BroadcastKey.New("after-rearm");
        await _fx.Cache.SetAsync(afterKey, "v1");
        Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, afterKey, "v1"), "key was not re-cached after the re-arm.");

        RedisCli.Standalone("SET", afterKey, "v2");
        var evictedAfterRearm = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(afterKey, out _));
        Assert.True(evictedAfterRearm, "invalidation was silently lost after the broadcast socket was re-armed.");
    }
}
