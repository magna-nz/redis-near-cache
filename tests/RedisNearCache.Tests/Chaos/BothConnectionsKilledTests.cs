using RedisNearCache.Internal;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// The existing suite kills the interactive connection and the subscriber connection in separate tests.
/// This kills both back to back, which is the interleaving where the two re-arm paths can fight each other:
/// the InteractiveRestored arm can run CLIENT LIST while the subscriber is still dead or still coming back,
/// and would then arm REDIRECT at a client id that no longer exists.
/// </summary>
public class BothConnectionsKilledTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;
    private readonly ITestOutputHelper _out;

    public BothConnectionsKilledTests(StandaloneCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    [Fact]
    public async Task BothConnectionsKilled()
    {
        var endpoint = _fx.Connection.Multiplexer.GetEndPoints()[0];
        var oldRedirect = _fx.Armer.RedirectTargets[endpoint];
        var rearmsBefore = _fx.Cache.Statistics.Rearms;

        var reasons = new HashSet<ArmReason>();
        void OnArmed(TrackingArmedEvent e)
        {
            lock (reasons) reasons.Add(e.Reason);
        }

        _fx.Armer.Armed += OnArmed;
        try
        {
            var interactiveId = (long)await _fx.Connection.Multiplexer.GetDatabase().ExecuteAsync("CLIENT", "ID");
            var subscriber = _fx.Server().ClientList()
                .FirstOrDefault(c => c.Name == _fx.Connection.ClientName && (c.Flags & ClientFlags.PubSubSubscriber) != 0);
            Assert.NotNull(subscriber);

            RedisCli.Standalone("CLIENT", "KILL", "ID", interactiveId.ToString());
            RedisCli.Standalone("CLIENT", "KILL", "ID", subscriber!.Id.ToString());

            var rearmed = await Poll.UntilAsync(
                () =>
                {
                    if (!_fx.Armer.RedirectTargets.TryGetValue(endpoint, out var current)) return false;
                    return current != oldRedirect && _fx.Cache.Statistics.Rearms > rearmsBefore;
                },
                TimeSpan.FromSeconds(15));

            lock (reasons)
            {
                _out.WriteLine($"arm reasons seen: {string.Join(",", reasons)}; redirect {oldRedirect} -> " +
                               $"{(_fx.Armer.RedirectTargets.TryGetValue(endpoint, out var now) ? now : -1)}; stats={_fx.Cache.Statistics}");
            }

            Assert.True(rearmed, "after killing both connections the endpoint was never re-armed with a fresh redirect id.");

            // The redirect id must be a client that actually exists right now and is really our subscriber,
            // not a stale id left over from before the kill. This is the silent-loss failure mode (spike E5b).
            var liveSubscriberId = await Poll.UntilAsync(() => LiveSubscriberId() is not null, TimeSpan.FromSeconds(10));
            Assert.True(liveSubscriberId, "our subscriber connection never came back.");
            Assert.Equal(LiveSubscriberId(), _fx.Armer.RedirectTargets[endpoint]);
        }
        finally
        {
            _fx.Armer.Armed -= OnArmed;
        }

        // And prove it end to end: a foreign write must still evict.
        var key = TestHelpers.Key("both-killed");
        await ChaosSupport.WithReconnectRetryAsync(async () => await _fx.Cache.SetAsync(key, "v1"));
        Assert.Equal("v1", await ChaosSupport.WithReconnectRetryAsync(async () => await _fx.Cache.GetAsync<string>(key)));
        Assert.True(_fx.Cache.TryGetLocal<string>(key, out _), "the key was not cached after the re-arm.");

        RedisCli.Standalone("SET", key, "v2");
        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(10));
        Assert.True(evicted, "invalidations were lost after both connections were killed: L1 still holds v1.");
    }

    private long? LiveSubscriberId() =>
        _fx.Server().ClientList()
            .Where(c => c.Name == _fx.Connection.ClientName && (c.Flags & ClientFlags.PubSubSubscriber) != 0)
            .Select(c => (long?)c.Id)
            .Max();
}
