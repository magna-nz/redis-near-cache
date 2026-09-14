using RedisNearCache.Tests.Chaos;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// The application's own pub/sub and the cache's invalidation channel share a server but nothing else. This
/// test runs both at once - 1,000 application messages on an application channel while keys are being
/// invalidated - and asserts that neither starves the other: every application message is delivered, and
/// every invalidation still evicts. The cache subscribes to <c>__redis__:invalidate</c> on its OWN private
/// multiplexer, so an application that happens to subscribe to thousands of channels cannot displace it.
/// </summary>
public class CoexistsWithApplicationPubSubTests : IClassFixture<StandaloneCacheFixture>
{
    private const int MessageCount = 1_000;

    private readonly StandaloneCacheFixture _fx;
    private readonly ITestOutputHelper _out;

    public CoexistsWithApplicationPubSubTests(StandaloneCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    [Fact]
    public async Task ApplicationPubSubAndInvalidationsBothArrive()
    {
        var channel = RedisChannel.Literal($"app-channel-{Guid.NewGuid():N}");
        var keys = Enumerable.Range(0, 20).Select(i => TestHelpers.Key($"pubsub-{i}")).ToArray();

        // The application's own plain multiplexer: no admin, no forced protocol, no tracking. It both
        // publishes and subscribes, like an app using Redis for messaging next to the cache.
        await using var app = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        var subscriber = app.Multiplexer.GetSubscriber();
        var received = 0;
        await subscriber.SubscribeAsync(channel, (_, _) => Interlocked.Increment(ref received));

        try
        {
            foreach (var key in keys)
            {
                await _fx.Cache.SetAsync(key, "v1");
                Assert.True(
                    await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"),
                    $"{key} was not cached before the pub/sub storm started.");
            }

            // Publish hard while writing the tracked keys from the same foreign client.
            var publishing = Task.Run(async () =>
            {
                for (var i = 0; i < MessageCount; i++)
                {
                    await subscriber.PublishAsync(channel, $"message-{i}");
                }
            });

            var writing = Task.Run(async () =>
            {
                foreach (var key in keys)
                {
                    await app.Db.StringSetAsync(key, "v2");
                }
            });

            await Task.WhenAll(publishing, writing);

            var allDelivered = await Poll.UntilAsync(
                () => Volatile.Read(ref received) >= MessageCount,
                TimeSpan.FromSeconds(10));
            Assert.True(
                allDelivered,
                $"the application's subscriber received {Volatile.Read(ref received)} of {MessageCount} messages.");

            var allEvicted = await Poll.UntilAsync(
                () => keys.All(k => !_fx.Cache.TryGetLocal<string>(k, out _)),
                TimeSpan.FromSeconds(10));
            var survivors = keys.Where(k => _fx.Cache.TryGetLocal<string>(k, out _)).ToArray();
            Assert.True(
                allEvicted,
                $"{survivors.Length} keys were not invalidated while the application's pub/sub traffic was running: {string.Join(", ", survivors)}");

            foreach (var key in keys)
            {
                Assert.Equal("v2", await _fx.Cache.GetAsync<string>(key));
            }

            _out.WriteLine($"delivered {Volatile.Read(ref received)} application messages alongside {keys.Length} invalidations");
        }
        finally
        {
            await subscriber.UnsubscribeAsync(channel);
            await app.Db.KeyDeleteAsync(keys.Select(k => (RedisKey)k).ToArray());
        }
    }
}
