using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// Two ways the facade must refuse work rather than reach a store that is gone or a caller that has left:
/// the local-eviction members after <c>DisposeAsync</c>, and a read handed a token that is already cancelled.
/// </summary>
public class DisposedAndCancelledFacadeTests
{
    private const string Key = "k";

    private static async Task<Facade> StartAsync()
    {
        var mux = new FakeMultiplexer("rnc-unit-" + Guid.NewGuid().ToString("N"));
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var cache = new Facade(connection, armer, new SilentListener(), Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance);
        await cache.Ready;
        return cache;
    }

    [Fact]
    public async Task EvictLocalAndEvictAllLocalAreNoOpsAfterDispose()
    {
        var cache = await StartAsync();
        await cache.GetAsync<string>(Key);
        Assert.True(cache.TryGetLocal<string>(Key, out _));

        await cache.DisposeAsync();

        // The L1 store behind these is disposed; reaching it would throw ObjectDisposedException.
        cache.EvictLocal(Key);
        cache.EvictAllLocal();
        cache.EvictLocal("never-read");
        Assert.False(cache.TryGetLocal<string>(Key, out _));
    }

    [Fact]
    public async Task PreCancelledReadThrowsEvenWhenTheKeyIsInL1()
    {
        await using var cache = await StartAsync();
        await cache.GetAsync<string>(Key);
        Assert.True(cache.TryGetLocal<string>(Key, out _), "the key must be in L1 for this test to mean anything.");

        var hitsBefore = cache.Statistics.Hits;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => cache.GetAsync<string>(Key, new CancellationToken(true)).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => cache.GetBytesAsync(Key, new CancellationToken(true)).AsTask());

        Assert.Equal(hitsBefore, cache.Statistics.Hits);
        Assert.True(cache.TryGetLocal<string>(Key, out _), "a cancelled read must not disturb the entry it refused to serve.");
    }
}
