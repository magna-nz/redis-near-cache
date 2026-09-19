using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// Cancellation must never leave the near cache incoherent. <see cref="IRedisNearCache.SetAsync{T}(string, T, TimeSpan?, CancellationToken)"/> evicts
/// L1 unconditionally before and after the write (see the ordering note above
/// <c>RedisNearCache.InvalidateLocal</c>), so a write cancelled before it could even reach Redis still leaves
/// no stale L1 entry behind. <see cref="IRedisNearCache.GetAsync{T}"/> must observe a pre-cancelled token
/// before touching L1 or Redis at all.
/// </summary>
public class CancellationTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public CancellationTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task OwnWriteWithCancelledTokenStillInvalidatesLocal()
    {
        var key = TestHelpers.Key("cancelled-set");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
        Assert.True(_fx.Cache.TryGetLocal<string>(key, out _), "key must be cached before the cancelled write.");

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _fx.Cache.SetAsync(key, "v2", cancellationToken: new CancellationToken(true)).AsTask());

        Assert.False(_fx.Cache.TryGetLocal<string>(key, out _),
            "a cancelled SetAsync must still evict L1 (eviction happens before and after the write, unconditionally).");

        // Whatever Redis actually ended up holding (the write may or may not have landed - the underlying
        // StringSetAsync was already dispatched before the cancellation was observed), the cache's next read
        // must agree with it; it must never serve a value nobody can any longer prove is current.
        var truth = RedisCli.Standalone("GET", key);
        var viaCache = await _fx.Cache.GetAsync<string>(key);
        Assert.Equal(truth, viaCache);
    }

    [Fact]
    public async Task PreCancelledGetThrows()
    {
        var key = TestHelpers.Key("cancelled-get");
        await _fx.Cache.SetAsync(key, "v1");

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _fx.Cache.GetAsync<string>(key, new CancellationToken(true)).AsTask());

        Assert.False(_fx.Cache.TryGetLocal<string>(key, out _), "a pre-cancelled GetAsync must not populate L1.");
    }

    [Fact]
    public async Task PreCancelledGetThrowsEvenWhenTheKeyIsCachedLocally()
    {
        var key = TestHelpers.Key("cancelled-get-hit");
        await _fx.Cache.SetAsync(key, "v1");
        Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"),
            "the key must be in L1 before this test means anything.");

        // The token is checked before L1 is consulted, so an L1 hit is no excuse for ignoring it: otherwise
        // cancellation would be observable only on a miss, which is exactly the non-deterministic case.
        var hitsBefore = _fx.Cache.Statistics.Hits;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _fx.Cache.GetAsync<string>(key, new CancellationToken(true)).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _fx.Cache.GetBytesAsync(key, new CancellationToken(true)).AsTask());

        Assert.Equal(hitsBefore, _fx.Cache.Statistics.Hits);
        Assert.True(_fx.Cache.TryGetLocal<string>(key, out _), "a refused read must leave the entry it did not serve alone.");
    }
}
