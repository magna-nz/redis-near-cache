using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The library's own facade over <see cref="FakeRedis"/>: the argument rules
/// (<see cref="RedisNearCache.Internal.ConditionalWrite"/>), that <c>when</c>/<c>keepTtl</c>/<c>expiry</c> reach
/// Redis unchanged, that the server's true/false comes back untouched, that L1 is evicted around the write
/// regardless of outcome, disposal, and that the raw-bytes overload copies its buffer and bypasses the serializer -
/// exactly as <see cref="BytesAndEvictLocalFacadeTests"/> proves for the unconditional writes.
/// </summary>
public class ConditionalWriteFacadeTests
{
    private const string Key = "k";
    private static readonly RedisValue Absent = RedisValue.Null;

    private static async Task<(FakeMultiplexer mux, Facade cache, RedisNearCacheOptions options)> StartAsync(IRedisNearCacheSerializer? serializer = null)
    {
        var mux = new FakeMultiplexer("rnc-unit-" + Guid.NewGuid().ToString("N"));
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var options = new RedisNearCacheOptions();
        if (serializer is not null) options.Serializer = serializer;
        var cache = new Facade(connection, armer, new SilentListener(), Options.Create(options), NullLogger<Facade>.Instance);
        await cache.Ready;
        return (mux, cache, options);
    }

    [Fact]
    public async Task KeepTtlWithExpiryThrowsAndNeverReachesRedis()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => cache.SetAsync(Key, "v1", When.Always, TimeSpan.FromSeconds(1), keepTtl: true).AsTask());

        Assert.Equal("keepTtl", ex.ParamName);
        Assert.Equal(0, mux.StringSetCalls);
    }

    [Fact]
    public async Task SetBytesKeepTtlWithExpiryThrowsAndNeverReachesRedis()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => cache.SetBytesAsync(Key, new byte[] { 1 }, When.Exists, TimeSpan.FromSeconds(1), keepTtl: true).AsTask());

        Assert.Equal("keepTtl", ex.ParamName);
        Assert.Equal(0, mux.StringSetCalls);
    }

    [Theory]
    [MemberData(nameof(WhenExpiryKeepTtlCombinations))]
    public async Task WhenKeepTtlAndExpiryReachRedisVerbatim(When when, TimeSpan? expiry, bool keepTtl)
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;

        await cache.SetAsync(Key, "v2", when, expiry, keepTtl);

        Assert.Equal(when, mux.LastSetWhen);
        Assert.Equal(expiry, mux.LastSetExpiry);
        Assert.Equal(keepTtl, mux.LastSetKeepTtl);
    }

    public static IEnumerable<object?[]> WhenExpiryKeepTtlCombinations()
    {
        yield return new object?[] { When.NotExists, null, false };
        yield return new object?[] { When.Exists, TimeSpan.FromSeconds(30), false };
        yield return new object?[] { When.Always, null, true };
        // Pointless (an absent key has no TTL to keep) but legal: Redis accepts SET ... NX KEEPTTL, so it goes through.
        yield return new object?[] { When.NotExists, null, true };
    }

    /// <summary>
    /// Through the INTERFACE, over the library's own cache: if the class's methods ever stopped matching the interface
    /// members exactly, calls through <see cref="IRedisNearCache"/> would land in the default implementation, which
    /// throws <see cref="NotSupportedException"/> for a conditional write. Callers only ever hold the interface.
    /// </summary>
    [Fact]
    public async Task ThroughTheInterfaceTheCachesOwnImplementationRunsNotTheDefaultOne()
    {
        var (mux, concrete, _) = await StartAsync();
        await using var lifetime = concrete;
        IRedisNearCache cache = concrete;
        mux.StoredValue = "v1";

        Assert.False(await cache.SetAsync(Key, "v2", When.NotExists));
        Assert.True(await cache.SetBytesAsync(Key, new byte[] { 1 }, When.Exists, keepTtl: true));

        Assert.Equal(When.Exists, mux.LastSetWhen);
        Assert.True(mux.LastSetKeepTtl);
    }

    [Fact]
    public async Task TheArgumentRuleIsCheckedBeforeTheValueIsSerialized()
    {
        var (mux, cache, _) = await StartAsync(new RefusingSerializer());
        await using var lifetime = cache;

        // RefusingSerializer throws InvalidOperationException if reached, so ArgumentException proves the order.
        await Assert.ThrowsAsync<ArgumentException>(
            () => cache.SetAsync(Key, "v1", When.Always, TimeSpan.FromSeconds(1), keepTtl: true).AsTask());
        Assert.Equal(0, mux.StringSetCalls);
    }

    [Fact]
    public async Task NotExistsOnAnAbsentKeySucceeds()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredValue = Absent;

        Assert.True(await cache.SetAsync(Key, "v1", When.NotExists));
        Assert.Equal("v1", (string?)mux.StoredValue);
    }

    [Fact]
    public async Task NotExistsOnAnExistingKeyFails()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredValue = "old";

        Assert.False(await cache.SetAsync(Key, "v1", When.NotExists));
        Assert.Equal("old", (string?)mux.StoredValue);
    }

    [Fact]
    public async Task ExistsOnAnAbsentKeyFails()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredValue = Absent;

        Assert.False(await cache.SetAsync(Key, "v1", When.Exists));
        Assert.True(mux.StoredValue.IsNull);
    }

    [Fact]
    public async Task ExistsOnAnExistingKeySucceeds()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredValue = "old";

        Assert.True(await cache.SetAsync(Key, "v1", When.Exists));
        Assert.Equal("v1", (string?)mux.StoredValue);
    }

    [Fact]
    public async Task AConditionalWriteThatDoesNotHappenStillEvictsL1()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredValue = "v1";
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _), "key must be cached before the conditional write.");

        var result = await cache.SetAsync(Key, "v2", When.NotExists); // key already exists: condition not met

        Assert.False(result);
        Assert.Equal("v1", (string?)mux.StoredValue);
        Assert.False(cache.TryGetLocal<string>(Key, out _), "a write that did not happen must still evict L1.");
    }

    [Fact]
    public async Task AConditionalWriteThatSucceedsEvictsL1()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        mux.StoredValue = "v1";
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _), "key must be cached before the conditional write.");

        var result = await cache.SetAsync(Key, "v2", When.Exists); // key exists: condition met

        Assert.True(result);
        Assert.Equal("v2", (string?)mux.StoredValue);
        Assert.False(cache.TryGetLocal<string>(Key, out _), "a successful conditional write must evict L1.");
    }

    [Fact]
    public async Task BothNewMembersThrowObjectDisposedExceptionAfterDispose()
    {
        var (_, cache, _) = await StartAsync();
        await cache.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.SetAsync(Key, "v1", When.Always).AsTask());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.SetBytesAsync(Key, new byte[] { 1 }, When.Always).AsTask());
        // Disposal is reported ahead of a bad argument, as for every other member.
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => cache.SetAsync(Key, "v1", When.Always, TimeSpan.FromSeconds(1), keepTtl: true).AsTask());
    }

    [Fact]
    public async Task SetBytesCopiesTheCallersBufferForAConditionalWrite()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        var buffer = new byte[] { 7, 8, 9 };

        await cache.SetBytesAsync(Key, buffer.AsMemory(), When.Always);
        buffer[0] = 0;

        Assert.Equal(new byte[] { 7, 8, 9 }, (byte[]?)mux.StoredValue);
    }

    [Fact]
    public async Task SetBytesBypassesTheSerializerForAConditionalWrite()
    {
        var (mux, cache, _) = await StartAsync(new RefusingSerializer());
        await using var lifetime = cache;
        byte[] payload = [0, 1, 2, 250, 255];

        var result = await cache.SetBytesAsync(Key, payload, When.Always);

        Assert.True(result);
        Assert.Equal(payload, (byte[]?)mux.StoredValue);
    }

    /// <summary>Fails the test if a raw-bytes member ever reaches the serializer.</summary>
    private sealed class RefusingSerializer : IRedisNearCacheSerializer
    {
        public byte[] Serialize<T>(T value) => throw new InvalidOperationException("the serializer was used");
        public T? Deserialize<T>(ReadOnlyMemory<byte> data) => throw new InvalidOperationException("the serializer was used");
    }
}
