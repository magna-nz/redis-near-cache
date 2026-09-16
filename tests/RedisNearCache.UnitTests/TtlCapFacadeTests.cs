using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

#pragma warning disable CS0618 // the simple exception constructors are the clearest way to fake a server reply here

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
        mux.TypedTtlFailure = new RedisTimeoutException("timeout", CommandStatus.Unknown);
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.False(cache.TryGetLocal<string>(Key, out _));
        Assert.Equal(0, cache.Statistics.RaceDiscards);

        // Transient: the next miss asks again and, once PTTL answers, caches.
        mux.PttlFailure = null;
        mux.TypedTtlFailure = null;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _));
        Assert.Equal(2, mux.PttlCalls);
    }

    [Fact]
    public async Task TypedTtlAnswersWhenTheRawPttlCannotBeRouted()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;

        // A raw Execute is not redirected the way a keyed command is, so on a resharding cluster the PTTL can fail for
        // a key whose slot has moved while the GET beside it succeeds. Without the typed fallback that key stays
        // uncacheable for as long as the condition lasts: served from Redis on every read, and silently so.
        mux.StoredTtlMilliseconds = 60_000;
        mux.PttlFailure = new RedisConnectionException(ConnectionFailureType.UnableToConnect, "No connection is available to service this operation");

        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _), "the typed TTL answered, so the value must be cached under its cap.");
        Assert.Equal(1, mux.PttlCalls);
        Assert.Equal(1, mux.TypedTtlCalls);
        Assert.Equal(0, cache.Statistics.RaceDiscards);
    }

    [Fact]
    public async Task RepeatedTtlFailuresGiveUpTheCapRatherThanTheCache()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;

        // Neither the raw nor the typed TTL can be answered. Serving the value uncached is right for a blip, but a
        // lasting failure would leave this instance reading through to Redis for every key, silently: nothing in the
        // statistics moves, and the cache still reports itself coherent. After a few misses in a row the cap is
        // abandoned instead, which is exactly what RespectServerTtl=false does.
        mux.PttlFailure = new RedisConnectionException(ConnectionFailureType.UnableToConnect, "No connection is available to service this operation");
        mux.TypedTtlFailure = new RedisConnectionException(ConnectionFailureType.UnableToConnect, "No connection is available to service this operation");

        for (var i = 0; i < 3; i++)
        {
            Assert.Equal("v1", await cache.GetAsync<string>(Key));
            Assert.False(cache.TryGetLocal<string>(Key, out _), $"read {i + 1} must be served without being cached while the cap is unknown.");
        }

        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _), "after repeated TTL failures the cap must be abandoned, not the cache.");
        Assert.Equal(0, cache.Statistics.RaceDiscards);
    }

    [Fact]
    public async Task ASuccessfulTtlResetsTheFailureRun()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;

        // Two failures, then an answer: the run restarts, so an occasional blip never abandons the cap.
        mux.PttlFailure = new RedisTimeoutException("timeout", CommandStatus.Unknown);
        mux.TypedTtlFailure = new RedisTimeoutException("timeout", CommandStatus.Unknown);
        for (var i = 0; i < 2; i++) Assert.Equal("v1", await cache.GetAsync<string>(Key));

        mux.PttlFailure = null;
        mux.TypedTtlFailure = null;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _));
        cache.EvictLocal(Key);

        mux.PttlFailure = new RedisTimeoutException("timeout", CommandStatus.Unknown);
        mux.TypedTtlFailure = new RedisTimeoutException("timeout", CommandStatus.Unknown);
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.False(cache.TryGetLocal<string>(Key, out _), "the run restarted, so this single failure must not have abandoned the cap yet.");
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
