using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.HybridCache;
using Xunit;

namespace RedisNearCache.Tests.Namespacing;

/// <summary>
/// <see cref="IDistributedCache"/> and <c>HybridCache</c> over a namespaced near cache. The adapters pass the
/// caller's key straight through to <see cref="IRedisNearCache"/>, so the namespace applies to them exactly as it
/// does to a direct caller: the bytes land at <c>namespace + key</c> and a foreign write to that key invalidates
/// what the adapter is serving.
/// </summary>
public class KeyNamespaceHybridCacheTests
{
    private static async Task<(ServiceProvider Provider, IRedisNearCache Cache, IDistributedCache Distributed, Microsoft.Extensions.Caching.Hybrid.HybridCache Hybrid)>
        BuildAsync(string keyNamespace)
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = keyNamespace);
        services.AddRedisNearCacheHybridCache();
        var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IRedisNearCache>();
        await cache.Ready;
        return (provider,
            cache,
            provider.GetRequiredService<IDistributedCache>(),
            provider.GetRequiredService<Microsoft.Extensions.Caching.Hybrid.HybridCache>());
    }

    [Fact]
    public async Task AValueSetThroughIDistributedCacheLandsAtTheNamespacedKeyAndIsInvalidatedThere()
    {
        var ns = $"ns-{Guid.NewGuid():N}:";
        var key = TestHelpers.Key("ns-dc");
        var full = ns + key;
        var built = await BuildAsync(ns);
        try
        {
            await built.Distributed.SetAsync(key, Encoding.UTF8.GetBytes("v1"));

            Assert.Equal("v1", RedisCli.Standalone("GET", full));
            Assert.Equal("0", RedisCli.Standalone("EXISTS", key));

            Assert.Equal("v1", Encoding.UTF8.GetString((await built.Distributed.GetAsync(key))!));
            Assert.True(await Poll.UntilAsync(() => built.Cache.TryGetLocal<byte[]>(key, out _), TimeSpan.FromSeconds(5)),
                "the read through IDistributedCache did not populate the tracked L1, so the invalidation below would prove nothing.");

            RedisCli.Standalone("SET", full, "v2");

            Assert.True(await Poll.UntilAsync(() => !built.Cache.TryGetLocal<byte[]>(key, out _), TimeSpan.FromSeconds(5)),
                "a foreign write to the namespaced key did not invalidate the value IDistributedCache is serving.");
            Assert.Equal("v2", Encoding.UTF8.GetString((await built.Distributed.GetAsync(key))!));
        }
        finally
        {
            await built.Provider.DisposeAsync();
            RedisCli.Standalone("DEL", full, key);
        }
    }

    [Fact]
    public async Task HybridCacheStoresUnderTheNamespaceAndTheFactoryRunsOnce()
    {
        var ns = $"ns-{Guid.NewGuid():N}:";
        var key = TestHelpers.Key("ns-hybrid");
        var full = ns + key;
        var built = await BuildAsync(ns);
        try
        {
            var factoryCalls = 0;
            ValueTask<string> Factory(CancellationToken _)
            {
                Interlocked.Increment(ref factoryCalls);
                return new ValueTask<string>("v1");
            }

            Assert.Equal("v1", await built.Hybrid.GetOrCreateAsync(key, Factory));

            // HybridCache persists a freshly computed value to the distributed tier in the background.
            Assert.True(await Poll.UntilAsync(() => RedisCli.Standalone("EXISTS", full) == "1", TimeSpan.FromSeconds(5)),
                $"nothing was written to the namespaced key {full}.");
            Assert.Equal("0", RedisCli.Standalone("EXISTS", key));

            Assert.Equal("v1", await built.Hybrid.GetOrCreateAsync(key, Factory));
            Assert.Equal(1, Volatile.Read(ref factoryCalls));
        }
        finally
        {
            await built.Provider.DisposeAsync();
            RedisCli.Standalone("DEL", full, key);
        }
    }
}
