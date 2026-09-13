using Microsoft.Extensions.Caching.Hybrid;
using RedisNearCache.Tests;
using Xunit;

namespace RedisNearCache.Tests.HybridCache;

/// <summary>
/// Regression: per-call HybridCacheEntryOptions used to override the default LocalCacheExpiration and revive
/// HybridCache's own untracked L1 for the whole entry lifetime (measured 30 s of stale reads). With the local
/// cache disabled via Flags, which merge per call, a per-call Expiration must not bring it back.
/// </summary>
public class HybridCachePerCallOptionsTests : IClassFixture<HybridCacheFixture>
{
    private readonly HybridCacheFixture _fx;

    public HybridCachePerCallOptionsTests(HybridCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task PerCallExpiration_DoesNotReviveHybridCacheLocalCache()
    {
        var key = TestHelpers.Key("hc-percall");
        var perCall = new HybridCacheEntryOptions { Expiration = TimeSpan.FromMinutes(10) };

        var v1 = await _fx.HybridCache.GetOrCreateAsync<string>(key, _ => ValueTask.FromResult("v1"), perCall);
        Assert.Equal("v1", v1);
        _ = await _fx.HybridCache.GetOrCreateAsync<string>(key, _ => ValueTask.FromResult("v1"), perCall);

        // Another process's view writes v2 through its own HybridCache.
        var other = await HybridCacheFixture.CreateAsync();
        try
        {
            await other.HybridCache.SetAsync(key, "v2", perCall);

            var sawV2 = await Poll.UntilAsync(async () =>
            {
                var current = await _fx.HybridCache.GetOrCreateAsync<string>(key, _ => ValueTask.FromResult("factory-should-not-run"), perCall);
                return current == "v2";
            }, TimeSpan.FromSeconds(3));

            Assert.True(sawV2, "HybridCache kept serving the old value from its own local cache despite the tracked invalidation");
        }
        finally
        {
            await other.DisposeAsync();
        }
    }
}
