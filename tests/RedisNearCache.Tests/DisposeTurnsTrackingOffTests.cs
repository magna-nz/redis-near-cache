using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using Xunit;

namespace RedisNearCache.Tests;

/// <summary>Disposes its own provider mid-test, so it does not share <see cref="StandaloneCacheFixture"/>.</summary>
public class DisposeTurnsTrackingOffTests
{
    [Fact]
    public async Task DisposeTurnsTrackingOff()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString);
        var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IRedisNearCache>();
        var connection = provider.GetRequiredService<RedisNearCacheConnection>();
        await cache.Ready;

        var clientName = connection.ClientName;
        var listedBeforeDispose = RedisCli.Standalone("CLIENT", "LIST");
        Assert.Contains(clientName, listedBeforeDispose);

        await provider.DisposeAsync();

        var disappeared = await Poll.UntilAsync(() => !RedisCli.Standalone("CLIENT", "LIST").Contains(clientName));
        Assert.True(disappeared, "the private client's connections were not closed on dispose.");
    }
}
