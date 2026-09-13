using RedisNearCache.Tests;
using Xunit;

namespace RedisNearCache.Tests.HybridCache;

/// <summary>Exercises <c>Microsoft.Extensions.Caching.Hybrid.HybridCache</c> wired up via <c>AddRedisNearCacheHybridCache</c>.</summary>
public class HybridCacheOperationsTests : IClassFixture<HybridCacheFixture>
{
    private readonly HybridCacheFixture _fx;

    public HybridCacheOperationsTests(HybridCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task HybridCache_GetOrCreate_ServesNearAndInvalidates()
    {
        var key = TestHelpers.Key("hc-getorcreate");
        var factoryCalls = 0;

        ValueTask<string> Factory(CancellationToken ct)
        {
            Interlocked.Increment(ref factoryCalls);
            return ValueTask.FromResult("v1");
        }

        var first = await _fx.HybridCache.GetOrCreateAsync<string>(key, Factory);
        var second = await _fx.HybridCache.GetOrCreateAsync<string>(key, Factory);
        Assert.Equal("v1", first);
        Assert.Equal("v1", second);
        Assert.Equal(1, factoryCalls);

        // A second provider is a second process's view of the same Redis instance: its own connection,
        // its own AddRedisNearCacheHybridCache registration. Removing the key there deletes it in Redis,
        // which invalidates it for every tracked reader, including the first provider above.
        var other = await HybridCacheFixture.CreateAsync();
        try
        {
            await other.HybridCache.RemoveAsync(key);

            var invalidated = await Poll.UntilAsync(async () =>
            {
                await _fx.HybridCache.GetOrCreateAsync<string>(key, Factory);
                return Volatile.Read(ref factoryCalls) >= 2;
            });

            Assert.True(
                invalidated,
                "Removing the key from a second provider did not cause the first provider's factory to run again within the deadline.");
            Assert.Equal(2, factoryCalls);
        }
        finally
        {
            await other.DisposeAsync();
        }
    }

    [Fact]
    public async Task HybridCache_TwoProviders_SetIsVisible()
    {
        var key = TestHelpers.Key("hc-twoproviders");

        var b = await HybridCacheFixture.CreateAsync();
        try
        {
            await _fx.HybridCache.SetAsync(key, "v1");

            var readByB = await b.HybridCache.GetOrCreateAsync<string>(key, StillThrows);
            Assert.Equal("v1", readByB);

            await _fx.HybridCache.SetAsync(key, "v2");

            // No RemoveAsync anywhere on b: the key is never absent from Redis, so b's factory must never
            // run; the new value must arrive purely through server-side invalidation of b's tracked L1.
            string? seen = null;
            var updated = await Poll.UntilAsync(async () =>
            {
                seen = await b.HybridCache.GetOrCreateAsync<string>(key, StillThrows);
                return seen == "v2";
            });

            Assert.True(updated, "Provider b did not observe provider a's second SetAsync within the deadline.");
            Assert.Equal("v2", seen);
        }
        finally
        {
            await b.DisposeAsync();
        }

        static ValueTask<string> StillThrows(CancellationToken ct) =>
            throw new InvalidOperationException("the key should already be present; the factory must not run.");
    }
}
