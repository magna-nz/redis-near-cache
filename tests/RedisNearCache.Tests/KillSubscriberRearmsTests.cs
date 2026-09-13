using RedisNearCache.Internal;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests;

public class KillSubscriberRearmsTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public KillSubscriberRearmsTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task KillSubscriberRearms()
    {
        var endpoint = _fx.Connection.Multiplexer.GetEndPoints()[0];
        var server = _fx.Server();

        var subscriber = server.ClientList()
            .FirstOrDefault(c => c.Name == _fx.Connection.ClientName && (c.Flags & ClientFlags.PubSubSubscriber) != 0);
        Assert.NotNull(subscriber);

        var oldRedirectId = _fx.Armer.RedirectTargets[endpoint];

        var sawSubscriptionRestored = false;
        _fx.Armer.Armed += e =>
        {
            if (e.Reason == ArmReason.SubscriptionRestored) sawSubscriptionRestored = true;
        };

        RedisCli.Standalone("CLIENT", "KILL", "ID", subscriber!.Id.ToString());

        var rearmed = await Poll.UntilAsync(
            () => sawSubscriptionRestored
                  && _fx.Armer.RedirectTargets.TryGetValue(endpoint, out var current)
                  && current != oldRedirectId,
            TimeSpan.FromSeconds(5));
        Assert.True(rearmed, "expected an Armed event with reason SubscriptionRestored and a changed redirect id.");

        // This is the silent-loss case the spike found (E5b): the interactive connection kept pointing at
        // the dead subscriber id until re-armed. Prove an external write is still delivered after the re-arm.
        var key = TestHelpers.Key("subscriber-rearm");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
        Assert.True(_fx.Cache.TryGetLocal<string>(key, out _));

        RedisCli.Standalone("SET", key, "v2");
        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
        Assert.True(evicted, "invalidation was silently lost after subscriber reconnect (this is the regression the spike found).");

        Assert.True(_fx.Cache.Statistics.Rearms >= 1);
    }
}
