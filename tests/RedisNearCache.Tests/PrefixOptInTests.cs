using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace RedisNearCache.Tests;

/// <summary>
/// Needs its own cache instance configured with <c>KeyPrefixes = ["p:"]</c>, so it does not use the shared
/// <see cref="StandaloneCacheFixture"/> (which caches every key). Builds and tears down its own
/// <see cref="ServiceProvider"/> via IAsyncLifetime.
/// </summary>
public class PrefixOptInTests : IAsyncLifetime
{
    private ServiceProvider _provider = null!;
    private IRedisNearCache _cache = null!;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString, o => o.KeyPrefixes.Add("p:"));
        _provider = services.BuildServiceProvider();
        _cache = _provider.GetRequiredService<IRedisNearCache>();
        await _cache.Ready;
    }

    public async Task DisposeAsync() => await _provider.DisposeAsync();

    [Fact]
    public async Task PrefixOptIn()
    {
        var qKey = "q:" + TestHelpers.Key("q");
        await _cache.SetAsync(qKey, "outside");
        Assert.Equal("outside", await _cache.GetAsync<string>(qKey));
        Assert.False(_cache.TryGetLocal<string>(qKey, out _), "key outside KeyPrefixes must never be cached in L1.");
        var hitsBeforeQ = _cache.Statistics.Hits;
        Assert.Equal("outside", await _cache.GetAsync<string>(qKey));
        Assert.Equal(hitsBeforeQ, _cache.Statistics.Hits); // second read of q: is still a miss, never a hit

        var pKey = "p:" + TestHelpers.Key("p");
        await _cache.SetAsync(pKey, "inside");
        Assert.Equal("inside", await _cache.GetAsync<string>(pKey));
        Assert.True(_cache.TryGetLocal<string>(pKey, out var cached));
        Assert.Equal("inside", cached);
        var hitsBeforeP = _cache.Statistics.Hits;
        Assert.Equal("inside", await _cache.GetAsync<string>(pKey));
        Assert.Equal(hitsBeforeP + 1, _cache.Statistics.Hits); // second read of p: is a hit
    }
}
