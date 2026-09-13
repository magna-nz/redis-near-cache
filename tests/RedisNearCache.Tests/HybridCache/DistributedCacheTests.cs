using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using RedisNearCache.Tests;
using Xunit;

namespace RedisNearCache.Tests.HybridCache;

/// <summary>Exercises <see cref="RedisNearCache.HybridCache.RedisNearCacheDistributedCache"/> through the plain <see cref="IDistributedCache"/> surface.</summary>
public class DistributedCacheTests : IClassFixture<HybridCacheFixture>
{
    private readonly HybridCacheFixture _fx;

    public DistributedCacheTests(HybridCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task DistributedCache_RoundTrip()
    {
        var key = TestHelpers.Key("dc-roundtrip");
        var value = new byte[] { 1, 2, 3, 4, 5 };

        await _fx.DistributedCache.SetAsync(key, value);

        var hitsBefore = _fx.Cache.Statistics.Hits;

        var first = await _fx.DistributedCache.GetAsync(key);
        Assert.Equal(value, first);

        var second = await _fx.DistributedCache.GetAsync(key);
        Assert.Equal(value, second);

        Assert.True(_fx.Cache.Statistics.Hits > hitsBefore, "The second Get was not served from the tracked near cache.");
    }

    [Fact]
    public async Task DistributedCache_AbsoluteExpiry()
    {
        var key = TestHelpers.Key("dc-expiry");
        var value = new byte[] { 9, 9, 9 };
        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMilliseconds(500),
        };

        await _fx.DistributedCache.SetAsync(key, value, options);
        Assert.Equal(value, await _fx.DistributedCache.GetAsync(key));

        // Evict the local copy before every poll attempt so each attempt genuinely re-reads Redis: the near
        // cache only reacts to invalidation pushes (which, for a plain TTL expiry with nobody else touching
        // the key, depend on Redis's own background active-expire cycle and are not guaranteed within any
        // particular window), not to the TTL itself. A real re-read is what makes the deadline deterministic.
        var expired = await Poll.UntilAsync(
            async () =>
            {
                _fx.Cache.EvictLocal(key);
                return await _fx.DistributedCache.GetAsync(key) is null;
            },
            timeout: TimeSpan.FromSeconds(2));

        Assert.True(expired, "The key did not expire in Redis (via its TTL) within the deadline.");
    }

    [Fact]
    public async Task DistributedCache_ExternalWriteVisible()
    {
        var key = TestHelpers.Key("dc-external");
        var v1 = Encoding.UTF8.GetBytes("v1");
        var v2 = Encoding.UTF8.GetBytes("v2");

        await _fx.DistributedCache.SetAsync(key, v1);
        Assert.Equal(v1, await _fx.DistributedCache.GetAsync(key));

        RedisCli.Standalone("SET", key, "v2");

        byte[]? seen = null;
        var updated = await Poll.UntilAsync(async () =>
        {
            seen = await _fx.DistributedCache.GetAsync(key);
            return seen is not null && seen.SequenceEqual(v2);
        });

        Assert.True(updated, "The external redis-cli write was not visible through IDistributedCache within the deadline.");
        Assert.Equal(v2, seen);
    }
}
