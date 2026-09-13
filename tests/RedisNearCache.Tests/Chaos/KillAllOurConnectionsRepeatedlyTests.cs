using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// One kill is a happy path; five in a row is the test of whether re-arm state accumulates correctly.
/// Each round caches a key, destroys both connections, waits for the re-arm, and proves a foreign write still
/// evicts. A single missed <c>CLIENT TRACKING ON REDIRECT</c> in any round leaves the cache silently
/// incoherent from that round on, which is exactly what the round-by-round eviction proof catches.
/// </summary>
public class KillAllOurConnectionsRepeatedlyTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;
    private readonly ITestOutputHelper _out;

    public KillAllOurConnectionsRepeatedlyTests(StandaloneCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    [Fact]
    public async Task KillAllOurConnectionsRepeatedly()
    {
        var endpoint = _fx.Connection.Multiplexer.GetEndPoints()[0];

        for (var round = 1; round <= 5; round++)
        {
            var key = TestHelpers.Key($"kill-round{round}");
            await ChaosSupport.WithReconnectRetryAsync(async () => await _fx.Cache.SetAsync(key, "v1"));
            Assert.Equal("v1", await ChaosSupport.WithReconnectRetryAsync(async () => await _fx.Cache.GetAsync<string>(key)));
            Assert.True(_fx.Cache.TryGetLocal<string>(key, out _), $"round {round}: key was not cached before the kill.");

            var rearmsBefore = _fx.Cache.Statistics.Rearms;
            var oldRedirect = _fx.Armer.RedirectTargets[endpoint];

            var interactiveId = (long)await ChaosSupport.WithReconnectRetryAsync(async () =>
                (long)await _fx.Connection.Multiplexer.GetDatabase().ExecuteAsync("CLIENT", "ID"));
            var subscriberId = LiveSubscriberId();
            Assert.NotNull(subscriberId);

            RedisCli.Standalone("CLIENT", "KILL", "ID", interactiveId.ToString());
            RedisCli.Standalone("CLIENT", "KILL", "ID", subscriberId!.Value.ToString());

            var rearmed = await Poll.UntilAsync(
                () => _fx.Cache.Statistics.Rearms > rearmsBefore
                      && _fx.Armer.RedirectTargets.TryGetValue(endpoint, out var current)
                      && current != oldRedirect
                      && current == LiveSubscriberIdOrNull(),
                TimeSpan.FromSeconds(15));

            _out.WriteLine($"round {round}: rearms {rearmsBefore} -> {_fx.Cache.Statistics.Rearms}, " +
                           $"redirect {oldRedirect} -> {(_fx.Armer.RedirectTargets.TryGetValue(endpoint, out var r) ? r : -1)}");

            Assert.True(rearmed, $"round {round}: tracking was not re-armed at the live subscriber id after both connections were killed.");
            Assert.True(_fx.Cache.Statistics.Rearms > rearmsBefore, $"round {round}: Statistics.Rearms did not grow.");

            // The kill flushed L1 (every non-Initial arm clears it), so re-read before proving eviction.
            Assert.Equal("v1", await ChaosSupport.WithReconnectRetryAsync(async () => await _fx.Cache.GetAsync<string>(key)));
            Assert.True(_fx.Cache.TryGetLocal<string>(key, out _), $"round {round}: key was not re-cached after the re-arm.");

            RedisCli.Standalone("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, $"round {round}: invalidation was lost after the re-arm; L1 still holds v1.");
        }

        _out.WriteLine($"final stats={_fx.Cache.Statistics}");
    }

    private long? LiveSubscriberId() => LiveSubscriberIdOrNull();

    private long? LiveSubscriberIdOrNull()
    {
        try
        {
            return _fx.Server().ClientList()
                .Where(c => c.Name == _fx.Connection.ClientName && (c.Flags & ClientFlags.PubSubSubscriber) != 0)
                .Select(c => (long?)c.Id)
                .Max();
        }
        catch (Exception ex) when (ex is RedisConnectionException or RedisTimeoutException)
        {
            return null; // still reconnecting; the poll will come back
        }
    }
}
