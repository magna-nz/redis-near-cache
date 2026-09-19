using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.HybridCache;
using Xunit;

namespace RedisNearCache.Tests.NamedInstances;

/// <summary>
/// <c>AddRedisNearCacheDistributedCacheFor(name)</c> and <c>AddRedisNearCacheHybridCacheFor(name)</c>: the
/// application still has ONE <see cref="IDistributedCache"/> and one <c>HybridCache</c>, but they are backed by a
/// named near cache rather than the default one. Proved by which instance's statistics move.
/// </summary>
public class NamedInstanceAdapterTests
{
    private const string Connection = StandaloneCacheFixture.ConnectionString;

    [Fact]
    public async Task IDistributedCacheRoundTripsThroughTheNamedInstance()
    {
        var ns = $"ns-{Guid.NewGuid():N}:";
        var services = new ServiceCollection();
        services.AddRedisNearCache(Connection);
        services.AddKeyedRedisNearCache("a", Connection, o => o.KeyNamespace = ns);
        services.AddRedisNearCacheDistributedCacheFor("a");
        var provider = services.BuildServiceProvider();
        var key = TestHelpers.Key("named-dc");
        var full = ns + key;
        try
        {
            var named = provider.GetRequiredKeyedService<IRedisNearCache>("a");
            var @default = provider.GetRequiredService<IRedisNearCache>();
            await Task.WhenAll(named.Ready, @default.Ready);
            var distributed = provider.GetRequiredService<IDistributedCache>();

            Assert.Single(provider.GetServices<IDistributedCache>());

            var namedMisses = named.Statistics.Misses;
            var defaultMisses = @default.Statistics.Misses;
            var namedHits = named.Statistics.Hits;

            await distributed.SetAsync(key, Encoding.UTF8.GetBytes("v1"));
            Assert.Equal("v1", Encoding.UTF8.GetString((await distributed.GetAsync(key))!));

            // The named instance did the work - including applying its own namespace.
            Assert.True(named.Statistics.Misses > namedMisses, "the named instance's counters did not move.");
            Assert.Equal(defaultMisses, @default.Statistics.Misses);
            Assert.Equal("v1", RedisCli.Standalone("GET", full));
            Assert.Equal("0", RedisCli.Standalone("EXISTS", key));

            Assert.True(await Poll.UntilAsync(() => named.TryGetLocal<byte[]>(key, out _), TimeSpan.FromSeconds(5)),
                "the read through IDistributedCache did not populate the named instance's L1.");
            Assert.Equal("v1", Encoding.UTF8.GetString((await distributed.GetAsync(key))!));
            Assert.True(named.Statistics.Hits > namedHits, "the second read was not served from the named instance's L1.");

            // And the named instance's own tracking invalidates it.
            RedisCli.Standalone("SET", full, "v2");
            Assert.True(await Poll.UntilAsync(() => !named.TryGetLocal<byte[]>(key, out _), TimeSpan.FromSeconds(5)),
                "a foreign write did not invalidate the named instance behind IDistributedCache.");
            Assert.Equal("v2", Encoding.UTF8.GetString((await distributed.GetAsync(key))!));
        }
        finally
        {
            await provider.DisposeAsync();
            RedisCli.Standalone("DEL", full, key);
        }
    }

    [Fact]
    public async Task HybridCacheRunsOverTheNamedInstance()
    {
        var ns = $"ns-{Guid.NewGuid():N}:";
        var services = new ServiceCollection();
        services.AddRedisNearCache(Connection);
        services.AddKeyedRedisNearCache("a", Connection, o => o.KeyNamespace = ns);
        services.AddRedisNearCacheHybridCacheFor("a");
        var provider = services.BuildServiceProvider();
        var key = TestHelpers.Key("named-hybrid");
        var full = ns + key;
        try
        {
            var named = provider.GetRequiredKeyedService<IRedisNearCache>("a");
            var @default = provider.GetRequiredService<IRedisNearCache>();
            await Task.WhenAll(named.Ready, @default.Ready);
            var hybrid = provider.GetRequiredService<Microsoft.Extensions.Caching.Hybrid.HybridCache>();

            var defaultMisses = @default.Statistics.Misses;
            var namedMisses = named.Statistics.Misses;
            var factoryCalls = 0;

            ValueTask<string> Factory(CancellationToken _)
            {
                Interlocked.Increment(ref factoryCalls);
                return new ValueTask<string>("v1");
            }

            Assert.Equal("v1", await hybrid.GetOrCreateAsync(key, Factory));

            // The value is in the NAMED instance's namespace, and the default instance was not involved at all.
            Assert.True(await Poll.UntilAsync(() => RedisCli.Standalone("EXISTS", full) == "1", TimeSpan.FromSeconds(5)),
                $"HybridCache did not write to the named instance's namespaced key {full}.");
            Assert.Equal("0", RedisCli.Standalone("EXISTS", key));
            Assert.True(named.Statistics.Misses > namedMisses, "the named instance's counters did not move.");
            Assert.Equal(defaultMisses, @default.Statistics.Misses);

            Assert.Equal("v1", await hybrid.GetOrCreateAsync(key, Factory));
            Assert.Equal(1, Volatile.Read(ref factoryCalls));
        }
        finally
        {
            await provider.DisposeAsync();
            RedisCli.Standalone("DEL", full, key);
        }
    }
}
