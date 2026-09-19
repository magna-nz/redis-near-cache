using System.Collections.Concurrent;
using RedisNearCache.Internal;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests;

/// <summary>
/// What a node restart looks like from the client: the interactive and the subscriber connection both drop, both
/// come back, and each <c>ConnectionRestored</c> queues its own arm. The second arm sends <c>CLIENT TRACKING OFF</c>
/// on a node the first has already armed, so it must announce the loss again before it does: no two <c>Armed</c>
/// events of an endpoint without a <c>TrackingLost</c> between them. (How the two arms interleave here is up to the
/// reconnect timing; <c>OverlappingArmTests</c> in the unit tests pins the ordering that used to go wrong.)
/// </summary>
public class KillBothConnectionsRearmsTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public KillBothConnectionsRearmsTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task KillBothConnectionsRearmsWithALossBeforeEveryArm()
    {
        var endpoint = _fx.Connection.Multiplexer.GetEndPoints()[0];
        var server = _fx.Server();

        var events = new ConcurrentQueue<string>();
        _fx.Armer.TrackingLost += ep => { if (ep.Equals(endpoint)) events.Enqueue("lost"); };
        _fx.Armer.Armed += e => { if (e.EndPoint.Equals(endpoint)) events.Enqueue($"armed {e.Reason}"); };

        var ours = server.ClientList().Where(c => c.Name == _fx.Connection.ClientName).ToArray();
        var subscriber = ours.FirstOrDefault(c => (c.Flags & ClientFlags.PubSubSubscriber) != 0);
        var interactive = ours.FirstOrDefault(c => (c.Flags & ClientFlags.PubSubSubscriber) == 0);
        Assert.NotNull(subscriber);
        Assert.NotNull(interactive);
        var oldRedirectId = _fx.Armer.RedirectTargets[endpoint];

        RedisCli.Standalone("CLIENT", "KILL", "ID", subscriber!.Id.ToString());
        RedisCli.Standalone("CLIENT", "KILL", "ID", interactive!.Id.ToString());

        string Describe() => string.Join(" | ", events);
        bool Saw(ArmReason reason) => events.Contains($"armed {reason}");
        var rearmed = await Poll.UntilAsync(
            () => Saw(ArmReason.SubscriptionRestored) && Saw(ArmReason.InteractiveRestored)
                  && _fx.Armer.RedirectTargets.TryGetValue(endpoint, out var current) && current != oldRedirectId,
            TimeSpan.FromSeconds(10));
        Assert.True(rearmed, "expected one arm per restored connection and a new redirect id: " + Describe());
        Assert.True(await Poll.UntilAsync(() => _fx.Cache.IsCoherent, TimeSpan.FromSeconds(5)), "the cache never left pass-through: " + Describe());

        var sequence = events.ToArray();
        var lastArmed = -1;
        for (var i = 0; i < sequence.Length; i++)
        {
            if (!sequence[i].StartsWith("armed", StringComparison.Ordinal)) continue;
            Assert.True(i > 0 && (lastArmed < 0 || sequence.Skip(lastArmed + 1).Take(i - lastArmed - 1).Contains("lost")),
                $"arm #{i} turned tracking off without announcing a loss since the arm before it: " + Describe());
            lastArmed = i;
        }

        // And tracking works once the dust settles: read, external write, evicted.
        var key = TestHelpers.Key("both-rearm");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"), "key was not re-cached after the re-arms.");

        RedisCli.Standalone("SET", key, "v2");
        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
        Assert.True(evicted, "an external write was not delivered after both connections were re-armed: " + Describe());
    }
}
