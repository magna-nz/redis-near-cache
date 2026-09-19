using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The library's own cache over <see cref="FakeRedis"/>: that the multi-key read really is the single-key read
/// fanned out (an L1 hit sends no GET, a miss sends exactly one and lands in L1, a duplicate key is read once),
/// that <see cref="RedisNearCacheStatistics"/> moves by exactly one hit or miss per DISTINCT key, that the two
/// overrides exist for the one reason they exist - a disposed cache says so even for an empty key list - and that
/// the bytes form hands out copies.
/// </summary>
/// <remarks>
/// <see cref="FakeMultiplexer"/> serves one stored value for every key, so the assertions here are about which
/// commands were sent and which counters moved, not about telling one key's value from another's. That is checked
/// against a real Redis in <c>tests/RedisNearCache.Tests/EdgeCases/GetManyTests.cs</c>.
/// </remarks>
public class GetManyFacadeTests
{
    private static async Task<(FakeMultiplexer mux, Facade cache)> StartAsync()
    {
        var mux = new FakeMultiplexer("rnc-unit-" + Guid.NewGuid().ToString("N"));
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var cache = new Facade(connection, armer, new SilentListener(), Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance);
        await cache.Ready;
        return (mux, cache);
    }

    /// <summary>
    /// Through the INTERFACE, so a class member that stopped matching the interface member exactly (and therefore
    /// left callers in the default implementation) would show up here.
    /// </summary>
    [Fact]
    public async Task KeysAlreadyInL1AreServedWithoutAnyRedisRead()
    {
        var (mux, concrete) = await StartAsync();
        await using var lifetime = concrete;
        IRedisNearCache cache = concrete;

        Assert.Equal("v1", await cache.GetAsync<string>("a"));
        Assert.Equal("v1", await cache.GetAsync<string>("b"));
        Assert.True(concrete.TryGetLocal<string>("a", out _), "the key must be in L1 before this test means anything.");
        Assert.True(concrete.TryGetLocal<string>("b", out _), "the key must be in L1 before this test means anything.");

        var getsBefore = mux.StringGetCalls;
        var hitsBefore = cache.Statistics.Hits;
        var missesBefore = cache.Statistics.Misses;

        var result = await cache.GetManyAsync<string>(["a", "b"]);

        Assert.Equal(2, result.Count);
        Assert.Equal("v1", result["a"]);
        Assert.Equal("v1", result["b"]);
        Assert.Equal(getsBefore, mux.StringGetCalls);
        Assert.Equal(hitsBefore + 2, cache.Statistics.Hits);
        Assert.Equal(missesBefore, cache.Statistics.Misses);
    }

    [Fact]
    public async Task MissesAreReadOnceEachAndLandInL1()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;

        var result = await cache.GetManyAsync<string>(["a", "b", "c"]);

        Assert.Equal(3, result.Count);
        Assert.Equal(3, mux.StringGetCalls);
        Assert.Equal(1, mux.StringGetCallsFor("a"));
        Assert.Equal(1, mux.StringGetCallsFor("b"));
        Assert.Equal(1, mux.StringGetCallsFor("c"));
        Assert.Equal(3, cache.Statistics.Misses);
        Assert.Equal(0, cache.Statistics.Hits);
        foreach (var key in new[] { "a", "b", "c" })
        {
            Assert.True(cache.TryGetLocal<string>(key, out var cached), $"{key} was not stored in L1 by the multi-key read.");
            Assert.Equal("v1", cached);
        }
    }

    [Fact]
    public async Task StatisticsMoveByExactlyOnePerDistinctKeyEvenForAMixOfHitsAndMisses()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;
        Assert.Equal("v1", await cache.GetAsync<string>("hit"));
        Assert.True(cache.TryGetLocal<string>("hit", out _), "the key must be in L1 before this test means anything.");

        var getsBefore = mux.StringGetCalls;
        var hitGetsBefore = mux.StringGetCallsFor("hit");
        var hitsBefore = cache.Statistics.Hits;
        var missesBefore = cache.Statistics.Misses;

        // "hit" twice and "miss" twice: four names, two distinct keys, one hit and one miss.
        var result = await cache.GetManyAsync<string>(["hit", "miss", "hit", "miss"]);

        Assert.Equal(2, result.Count);
        Assert.Equal(hitsBefore + 1, cache.Statistics.Hits);
        Assert.Equal(missesBefore + 1, cache.Statistics.Misses);
        Assert.Equal(getsBefore + 1, mux.StringGetCalls);
        Assert.Equal(1, mux.StringGetCallsFor("miss"));
        Assert.Equal(hitGetsBefore, mux.StringGetCallsFor("hit"));
    }

    [Fact]
    public async Task GetManyBytesReadsThroughToRedisAndCachesToo()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;

        var first = await cache.GetManyBytesAsync(["a", "b"]);
        Assert.Equal(2, mux.StringGetCalls);
        Assert.Equal(2, cache.Statistics.Misses);
        Assert.Equal("v1"u8.ToArray(), first["a"]);

        var second = await cache.GetManyBytesAsync(["a", "b"]);
        Assert.Equal(2, mux.StringGetCalls);
        Assert.Equal(2, cache.Statistics.Hits);
        Assert.Equal("v1"u8.ToArray(), second["b"]);
    }

    [Fact]
    public async Task GetManyBytesReturnsTheCallersOwnArrays()
    {
        var (_, cache) = await StartAsync();
        await using var lifetime = cache;

        var first = await cache.GetManyBytesAsync(["a"]);
        Assert.True(cache.TryGetLocal<string>("a", out _), "the read must have cached the key, or the copy rule is untested.");
        first["a"]![0] = 0;

        var second = await cache.GetManyBytesAsync(["a"]);

        Assert.Equal("v1"u8.ToArray(), second["a"]);
        Assert.NotSame(first["a"], second["a"]);
        // And the array handed out is never the one L1 holds: a third read still sees the original bytes.
        second["a"]![0] = 0;
        var third = await cache.GetManyBytesAsync(["a"]);
        Assert.Equal("v1"u8.ToArray(), third["a"]);
    }

    [Fact]
    public async Task TwoKeysWithTheSameValueGetTheirOwnArrays()
    {
        var (_, cache) = await StartAsync();
        await using var lifetime = cache;

        var result = await cache.GetManyBytesAsync(["a", "b"]);

        Assert.NotSame(result["a"], result["b"]);
    }

    /// <summary>
    /// The only reason the cache overrides these two members at all: an empty key list never reaches a single-key
    /// read, so without the override a disposed cache would quietly answer it with an empty dictionary.
    /// </summary>
    [Fact]
    public async Task BothMembersThrowObjectDisposedExceptionAfterDisposeEvenForAnEmptyKeyList()
    {
        var (_, concrete) = await StartAsync();
        IRedisNearCache cache = concrete;
        await cache.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.GetManyAsync<string>([]).AsTask());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.GetManyBytesAsync([]).AsTask());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.GetManyAsync<string>(["a"]).AsTask());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.GetManyBytesAsync(["a"]).AsTask());
    }

    /// <summary>Pins the observed precedence: disposal is reported ahead of a bad argument, as for every other member.</summary>
    [Fact]
    public async Task DisposalIsReportedAheadOfANullKeyList()
    {
        var (_, cache) = await StartAsync();
        await cache.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.GetManyAsync<string>(null!).AsTask());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.GetManyBytesAsync(null!).AsTask());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.GetManyAsync<string>(["a", null!]).AsTask());
    }

    [Fact]
    public async Task ABadKeyListIsRefusedBeforeAnyReadReachesRedis()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;

        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetManyAsync<string>(null!).AsTask());
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => cache.GetManyAsync<string>(["a", "b", null!]).AsTask());

        Assert.Equal("keys", ex.ParamName);
        Assert.Equal(0, mux.StringGetCalls);
        Assert.Equal(0, cache.Statistics.Misses);
    }

    [Fact]
    public async Task APreCancelledTokenIsObservedBeforeAnyReadEvenWhenEveryKeyIsInL1()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;
        Assert.Equal("v1", await cache.GetAsync<string>("a"));
        Assert.True(cache.TryGetLocal<string>("a", out _), "the key must be in L1 before this test means anything.");

        var getsBefore = mux.StringGetCalls;
        var hitsBefore = cache.Statistics.Hits;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => cache.GetManyAsync<string>(["a", "b"], new CancellationToken(true)).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => cache.GetManyBytesAsync(["a", "b"], new CancellationToken(true)).AsTask());

        Assert.Equal(getsBefore, mux.StringGetCalls);
        Assert.Equal(hitsBefore, cache.Statistics.Hits);
        Assert.True(cache.TryGetLocal<string>("a", out _), "a refused read must leave the entry it did not serve alone.");
    }

    [Fact]
    public async Task AnEmptyKeyListOnALiveCacheReturnsAnEmptyResultWithoutReading()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;

        Assert.Empty(await cache.GetManyAsync<string>([]));
        Assert.Empty(await cache.GetManyBytesAsync([]));
        Assert.Equal(0, mux.StringGetCalls);
        Assert.Equal(0, cache.Statistics.Misses);
    }

    [Fact]
    public async Task AMissingKeyIsPresentWithANullValueAndIsNotCached()
    {
        var (mux, cache) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredValue = StackExchange.Redis.RedisValue.Null;

        var result = await cache.GetManyAsync<string>(["a", "b"]);

        Assert.Equal(2, result.Count);
        Assert.Null(result["a"]);
        Assert.Null(result["b"]);
        Assert.False(cache.TryGetLocal<string>("a", out _), "a null reply must never populate L1.");
        Assert.Equal(2, cache.Statistics.Misses);
        Assert.Equal(0, cache.Statistics.Hits);
    }
}
