using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using Xunit;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// Empty <see cref="RedisNearCacheOptions.KeyPrefixes"/> is legal in Broadcast mode: the whole keyspace is
/// broadcast to this client (logged once as a warning by <c>BroadcastTracker</c>). Fixture-less because none of
/// the shared fixtures leave <c>KeyPrefixes</c> empty.
/// </summary>
public class EmptyKeyPrefixesTests
{
    [Fact]
    public async Task ArmsWithEmptyKeyPrefixesAndForeignWriteEvictsAnyCachedKey()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(BroadcastStandaloneCacheFixture.ConnectionString, o =>
        {
            o.TrackingMode = TrackingMode.Broadcast;
            // KeyPrefixes deliberately left empty: the whole database is broadcast to this client.
        });
        var provider = services.BuildServiceProvider();
        try
        {
            var cache = provider.GetRequiredService<IRedisNearCache>();
            var armer = provider.GetRequiredService<ITrackingArmer>();
            await cache.Ready;

            Assert.NotEmpty(armer.RedirectTargets);

            var key = TestHelpers.Key("empty-prefixes");
            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"));

            RedisCli.Standalone("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _));
            Assert.True(evicted, "a foreign write to an arbitrary key was not evicted when KeyPrefixes is empty.");
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }
}
