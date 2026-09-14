using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The facade's TTL cap against the fake: every branch of the PTTL result (no expiry, a remaining TTL, the key gone
/// between GET and PTTL) and the two failure modes (a transient fault, a server that rejects the command).
/// </summary>
public class TtlCapFacadeTests
{
    private const string Key = "k";

    private static async Task<(FakeMultiplexer mux, Facade cache)> StartAsync(Action<RedisNearCacheOptions>? configure = null)
    {
        var mux = new FakeMultiplexer("rnc-unit");
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var options = new RedisNearCacheOptions();
        configure?.Invoke(options);
        var cache = new Facade(connection, armer, new SilentListener(), Options.Create(options), NullLogger<Facade>.Instance);
        await cache.Ready;
        return (mux, cache);
    }

    [Fact]
    public async Task NoExpiryIsCachedForL1MaxAge()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredTtlMilliseconds = -1;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _));
        Assert.Equal(1, mux.PttlCalls);
        Assert.Equal(0, cache.Statistics.RaceDiscards);
    }

    [Fact]
    public async Task RemainingTtlCapsTheEntry()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredTtlMilliseconds = 80;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _), "the entry was not cached at all.");
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (cache.TryGetLocal<string>(Key, out _) && DateTime.UtcNow < deadline) await Task.Delay(10);
        Assert.False(cache.TryGetLocal<string>(Key, out _), "the entry outlived the 80 ms TTL the server reported.");
    }

    [Fact]
    public async Task KeyGoneBetweenGetAndPttlIsServedButNotCached()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredTtlMilliseconds = -2;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.False(cache.TryGetLocal<string>(Key, out _), "a key that vanished between GET and PTTL must not be cached.");
        Assert.Equal(1, cache.Statistics.RaceDiscards);
    }

    [Fact]
    public async Task TransientPttlFailureServesTheValueWithoutCachingIt()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;
        mux.PttlFailure = new RedisTimeoutException("timeout", CommandStatus.Unknown);
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.False(cache.TryGetLocal<string>(Key, out _));
        Assert.Equal(0, cache.Statistics.RaceDiscards);

        // Transient: the next miss asks again and, once PTTL answers, caches.
        mux.PttlFailure = null;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _));
        Assert.Equal(2, mux.PttlCalls);
    }

    [Fact]
    public async Task ServerRejectingPttlDisablesTheCapInsteadOfTheCache()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;
        mux.PttlFailure = new RedisServerException("NOPERM this user has no permissions to run the 'pttl' command");
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.False(cache.TryGetLocal<string>(Key, out _), "the read that discovered the rejection must not cache without a cap.");

        // From now on: plain GETs, cached for L1MaxAge, and PTTL is never sent again.
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _), "after the server rejected PTTL the cache must keep working without the cap.");
        Assert.Equal(1, mux.PttlCalls);
    }

    [Fact]
    public async Task RespectServerTtlOffNeverSendsPttl()
    {
        var (mux, cache) = await StartAsync(o => o.RespectServerTtl = false);
        await using var lifetime = cache;
        mux.StoredTtlMilliseconds = 10;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _));
        Assert.Equal(0, mux.PttlCalls);
    }
}
