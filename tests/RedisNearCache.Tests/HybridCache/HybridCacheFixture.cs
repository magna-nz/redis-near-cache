using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.HybridCache;
using RedisNearCache.Tests;
using Xunit;

namespace RedisNearCache.Tests.HybridCache;

/// <summary>
/// Builds a fresh <see cref="IServiceProvider"/> wired up with <c>AddRedisNearCache</c> followed by
/// <c>AddRedisNearCacheHybridCache</c>, against the standalone Redis container, and exposes the
/// <see cref="IRedisNearCache"/>, <see cref="IDistributedCache"/> and
/// <see cref="Microsoft.Extensions.Caching.Hybrid.HybridCache"/> that resolve from it. Same one-instance-per-
/// test-class pattern as <see cref="StandaloneCacheFixture"/>.
/// </summary>
public sealed class HybridCacheFixture : IAsyncLifetime
{
    public ServiceProvider Provider { get; private set; } = null!;
    public IRedisNearCache Cache { get; private set; } = null!;
    public IDistributedCache DistributedCache { get; private set; } = null!;
    public Microsoft.Extensions.Caching.Hybrid.HybridCache HybridCache { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var built = await CreateAsync();
        Provider = built.Provider;
        Cache = built.Cache;
        DistributedCache = built.DistributedCache;
        HybridCache = built.HybridCache;
    }

    public async Task DisposeAsync() => await Provider.DisposeAsync();

    /// <summary>
    /// Builds an independent provider against the same standalone Redis instance: its own private
    /// <c>RedisNearCacheConnection</c>, its own tracked L1, its own <see cref="Microsoft.Extensions.Caching.Hybrid.HybridCache"/>.
    /// Used by tests that need a second process's view of the same data (e.g. one provider's write being
    /// observed, or invalidated, by another). Callers own the returned <see cref="ServiceProvider"/> and must
    /// dispose it themselves.
    /// </summary>
    public static async Task<HybridCacheProviderHandle> CreateAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString);
        services.AddRedisNearCacheHybridCache();
        var provider = services.BuildServiceProvider();

        var cache = provider.GetRequiredService<IRedisNearCache>();
        var distributedCache = provider.GetRequiredService<IDistributedCache>();
        var hybridCache = provider.GetRequiredService<Microsoft.Extensions.Caching.Hybrid.HybridCache>();
        await cache.Ready;

        return new HybridCacheProviderHandle(provider, cache, distributedCache, hybridCache);
    }
}

/// <summary>A second, independently-built provider (see <see cref="HybridCacheFixture.CreateAsync"/>).</summary>
public sealed class HybridCacheProviderHandle(
    ServiceProvider provider,
    IRedisNearCache cache,
    IDistributedCache distributedCache,
    Microsoft.Extensions.Caching.Hybrid.HybridCache hybridCache) : IAsyncDisposable
{
    public ServiceProvider Provider { get; } = provider;
    public IRedisNearCache Cache { get; } = cache;
    public IDistributedCache DistributedCache { get; } = distributedCache;
    public Microsoft.Extensions.Caching.Hybrid.HybridCache HybridCache { get; } = hybridCache;

    public ValueTask DisposeAsync() => Provider.DisposeAsync();
}
