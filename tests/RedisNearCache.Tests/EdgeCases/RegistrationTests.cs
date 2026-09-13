using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.HybridCache;
using RedisNearCache.Internal;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// <c>AddRedisNearCache</c> and the HybridCache adapters register everything through
/// <c>TryAddSingleton</c> (src/RedisNearCache/DependencyInjection/ServiceCollectionExtensions.cs,
/// src/RedisNearCache.HybridCache/ServiceCollectionExtensions.cs), so calling them more than once - directly,
/// or indirectly because <c>AddRedisNearCacheHybridCache</c> itself calls <c>AddRedisNearCacheDistributedCache</c>
/// - must still end up with exactly one of everything.
/// </summary>
public class RegistrationTests
{
    [Fact]
    public async Task AddRedisNearCacheTwiceOpensOneMultiplexer()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString);
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString);
        var provider = services.BuildServiceProvider();
        try
        {
            var cache = provider.GetRequiredService<IRedisNearCache>();
            var connection = provider.GetRequiredService<RedisNearCacheConnection>();
            await cache.Ready;

            var clientList = RedisCli.Standalone("CLIENT", "LIST");
            var ids = EdgeCaseSupport.ClientIdsNamed(clientList, connection.ClientName);
            // One multiplexer opens exactly two connections under its client name: interactive + subscriber.
            // Two multiplexers (had TryAddSingleton not deduplicated) would show four.
            Assert.Equal(2, ids.Count);
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    [Fact]
    public async Task HybridCacheRegistrationsAreIdempotent()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString);
        services.AddRedisNearCacheDistributedCache();
        // Calling this now registers RedisNearCacheDistributedCache a second time internally
        // (AddRedisNearCacheHybridCache calls AddRedisNearCacheDistributedCache itself); TryAddSingleton
        // must make that a no-op.
        services.AddRedisNearCacheHybridCache();
        var provider = services.BuildServiceProvider();
        try
        {
            var cache = provider.GetRequiredService<IRedisNearCache>();
            await cache.Ready;

            var distributedCaches = provider.GetServices<IDistributedCache>().ToList();
            Assert.Single(distributedCaches);
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    [Fact]
    public async Task Resp3ConfigurationIsForcedToResp2()
    {
        var callerConfig = ConfigurationOptions.Parse(StandaloneCacheFixture.ConnectionString);
        callerConfig.Protocol = RedisProtocol.Resp3;
        Assert.False(callerConfig.AllowAdmin);

        var handle = await EdgeCaseSupport.BuildAsync(o => o.Configuration = callerConfig);
        try
        {
            var server = handle.Multiplexer.GetServer(handle.Multiplexer.GetEndPoints()[0]);
            Assert.Equal(RedisProtocol.Resp2, server.Protocol);

            // The caller's own ConfigurationOptions object must be untouched: RedisNearCache clones it.
            Assert.Equal(RedisProtocol.Resp3, callerConfig.Protocol);
            Assert.False(callerConfig.AllowAdmin);
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }
}
