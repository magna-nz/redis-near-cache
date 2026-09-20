using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RedisNearCache.HybridCache;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// Asking for RedisNearCache to be the application's <see cref="IDistributedCache"/> has to mean that, whatever was
/// registered before. It used to be a <c>TryAdd</c>, which skips when any descriptor for the service type exists: an
/// <c>AddDistributedMemoryCache()</c> earlier in <c>Program.cs</c> - ASP.NET Core session-state boilerplate, and
/// itself a <c>TryAdd</c> - kept the registration, and <c>AddRedisNearCacheHybridCache</c> went on to switch
/// <c>HybridCache</c>'s own local cache off anyway. The result was <c>HybridCache</c> with no local tier over a
/// process-local L2: slower than either tier alone, incoherent across processes, and silent.
/// </summary>
public class DistributedCacheRegistrationOrderTests
{
    private static IServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(o => o.ConnectionString = "127.0.0.1:1,abortConnect=false");
        return services;
    }

    [Fact]
    public async Task AForeignDistributedCacheRegisteredFirstIsDisplaced()
    {
        var services = Services();
        services.AddDistributedMemoryCache();
        services.AddRedisNearCacheDistributedCache();

        await using var provider = services.BuildServiceProvider();
        Assert.IsType<RedisNearCacheDistributedCache>(provider.GetRequiredService<IDistributedCache>());
        Assert.IsType<RedisNearCacheDistributedCache>(provider.GetRequiredService<IBufferDistributedCache>());
        // One instance, reached through either interface and by its own type.
        Assert.Same(provider.GetRequiredService<IDistributedCache>(), provider.GetRequiredService<IBufferDistributedCache>());
        Assert.Same(provider.GetRequiredService<IDistributedCache>(), provider.GetRequiredService<RedisNearCacheDistributedCache>());
    }

    [Fact]
    public async Task AForeignDistributedCacheIsDisplacedForHybridCacheToo()
    {
        var services = Services();
        services.AddDistributedMemoryCache();
        services.AddRedisNearCacheHybridCache();

        await using var provider = services.BuildServiceProvider();
        Assert.IsType<RedisNearCacheDistributedCache>(provider.GetRequiredService<IDistributedCache>());
        // And HybridCache's own local cache is still off, so the tracked L1 is the only local tier.
        Assert.Equal(
            HybridCacheEntryFlags.DisableLocalCache,
            provider.GetRequiredService<IOptions<HybridCacheOptions>>().Value.DefaultEntryOptions?.Flags);
    }

    [Fact]
    public async Task SeveralForeignRegistrationsAreAllDisplaced()
    {
        // The last non-keyed registration is what would otherwise be resolved, so removing only the first is not enough.
        var services = Services();
        services.AddSingleton<IDistributedCache>(new ThrowingDistributedCache());
        services.AddSingleton<IDistributedCache>(new ThrowingDistributedCache());
        services.AddRedisNearCacheDistributedCache();

        await using var provider = services.BuildServiceProvider();
        Assert.IsType<RedisNearCacheDistributedCache>(provider.GetRequiredService<IDistributedCache>());
        Assert.Single(provider.GetServices<IDistributedCache>());
    }

    /// <summary>
    /// The deliberate ordering between this library's own forms is unchanged: whichever runs first decides which
    /// cache backs the interfaces, so the keyed form called first keeps the keyed instance.
    /// </summary>
    [Fact]
    public async Task TheFirstOfOurOwnRegistrationsStillWins()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("named", o => o.ConnectionString = "127.0.0.1:1,abortConnect=false");
        services.AddRedisNearCache(o => o.ConnectionString = "127.0.0.1:1,abortConnect=false");
        services.AddRedisNearCacheDistributedCacheFor("named");
        services.AddRedisNearCacheDistributedCache();

        await using var provider = services.BuildServiceProvider();
        var adapter = provider.GetRequiredService<IDistributedCache>();
        Assert.IsType<RedisNearCacheDistributedCache>(adapter);
        Assert.Same(provider.GetRequiredKeyedService<IRedisNearCache>("named"), Backing(adapter));
    }

    [Fact]
    public async Task TheFirstOfOurOwnRegistrationsStillWinsWhenTheDefaultComesFirst()
    {
        var services = new ServiceCollection();
        services.AddKeyedRedisNearCache("named", o => o.ConnectionString = "127.0.0.1:1,abortConnect=false");
        services.AddRedisNearCache(o => o.ConnectionString = "127.0.0.1:1,abortConnect=false");
        services.AddRedisNearCacheDistributedCache();
        services.AddRedisNearCacheDistributedCacheFor("named");

        await using var provider = services.BuildServiceProvider();
        Assert.Same(provider.GetRequiredService<IRedisNearCache>(), Backing(provider.GetRequiredService<IDistributedCache>()));
    }

    /// <summary>A registration made AFTER ours still wins; nothing here can see the future, and it is documented.</summary>
    [Fact]
    public async Task AForeignDistributedCacheRegisteredAfterwardsStillWins()
    {
        var services = Services();
        services.AddRedisNearCacheDistributedCache();
        services.AddSingleton<IDistributedCache>(new ThrowingDistributedCache());

        await using var provider = services.BuildServiceProvider();
        Assert.IsType<ThrowingDistributedCache>(provider.GetRequiredService<IDistributedCache>());
    }

    /// <summary>The cache the adapter reads and writes through, for the ordering assertions above.</summary>
    private static IRedisNearCache Backing(IDistributedCache adapter)
    {
        var field = typeof(RedisNearCacheDistributedCache)
            .GetFields(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            .Single(f => f.FieldType == typeof(IRedisNearCache));
        return (IRedisNearCache)field.GetValue(adapter)!;
    }

    private sealed class ThrowingDistributedCache : IDistributedCache
    {
        public byte[]? Get(string key) => throw new NotSupportedException();
        public Task<byte[]?> GetAsync(string key, CancellationToken token = default) => throw new NotSupportedException();
        public void Refresh(string key) => throw new NotSupportedException();
        public Task RefreshAsync(string key, CancellationToken token = default) => throw new NotSupportedException();
        public void Remove(string key) => throw new NotSupportedException();
        public Task RemoveAsync(string key, CancellationToken token = default) => throw new NotSupportedException();
        public void Set(string key, byte[] value, DistributedCacheEntryOptions options) => throw new NotSupportedException();
        public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default) => throw new NotSupportedException();
    }
}
