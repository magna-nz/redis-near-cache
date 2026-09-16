using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The facade's raw-bytes members, which must bypass the configured serializer, and <see cref="IRedisNearCache.EvictLocal"/>
/// racing a read that is already on the wire.
/// </summary>
public class BytesAndEvictLocalFacadeTests
{
    private const string Key = "k";

    private static async Task<(FakeMultiplexer mux, Facade cache, RedisNearCacheOptions options)> StartAsync(IRedisNearCacheSerializer? serializer = null)
    {
        var mux = new FakeMultiplexer("rnc-unit");
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
    public async Task EvictLocalDuringAnInFlightReadDiscardsTheReply()
    {
        var (_, cache, options) = await StartAsync();
        await using var lifetime = cache;
        options.TestHooks.AfterRedisReadBeforeStore = k =>
        {
            cache.EvictLocal(k);
            return Task.CompletedTask;
        };

        Assert.Equal("v1", await cache.GetAsync<string>(Key));

        Assert.False(cache.TryGetLocal<string>(Key, out _), "a reply for a key evicted while its read was in flight must not populate L1.");
        Assert.Equal(1, cache.Statistics.RaceDiscards);

        // Only the read that was in flight is affected: the next one caches as usual.
        options.TestHooks.AfterRedisReadBeforeStore = null;
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out _));
    }

    [Fact]
    public async Task EvictLocalOutsideAReadOnlyDropsTheEntry()
    {
        var (_, cache, _) = await StartAsync();
        await using var lifetime = cache;
        await cache.GetAsync<string>(Key);
        Assert.True(cache.TryGetLocal<string>(Key, out _));

        cache.EvictLocal(Key);

        Assert.False(cache.TryGetLocal<string>(Key, out _));
        Assert.Equal(0, cache.Statistics.RaceDiscards);
        await cache.GetAsync<string>(Key);
        Assert.True(cache.TryGetLocal<string>(Key, out _), "a later read must cache again after EvictLocal.");
    }

    [Fact]
    public async Task BytesMembersBypassTheSerializer()
    {
        var (mux, cache, _) = await StartAsync(new RefusingSerializer());
        await using var lifetime = cache;
        byte[] payload = [0, 1, 2, 250, 255];

        await cache.SetBytesAsync(Key, payload);
        Assert.Equal(payload, (byte[]?)mux.StoredValue);

        Assert.Equal(payload, await cache.GetBytesAsync(Key));
        Assert.Equal(1, cache.Statistics.Misses);
        Assert.Equal(payload, await cache.GetBytesAsync(Key));
        Assert.Equal(1, cache.Statistics.Hits);
    }

    [Fact]
    public async Task GetBytesOfAMissingKeyIsNull()
    {
        var (mux, cache, _) = await StartAsync(new RefusingSerializer());
        await using var lifetime = cache;
        mux.StoredValue = RedisValueNull;
        Assert.Null(await cache.GetBytesAsync(Key));
    }

    [Fact]
    public async Task GetBytesReturnsACopyOfTheCachedValue()
    {
        var (_, cache, _) = await StartAsync();
        await using var lifetime = cache;
        await cache.SetBytesAsync(Key, new byte[] { 1, 2, 3 });

        var miss = (await cache.GetBytesAsync(Key))!;
        miss[0] = 99;
        var hit = (await cache.GetBytesAsync(Key))!;
        Assert.Equal(1, cache.Statistics.Hits);
        Assert.Equal(new byte[] { 1, 2, 3 }, hit);
        hit[1] = 99;
        Assert.Equal(new byte[] { 1, 2, 3 }, await cache.GetBytesAsync(Key));
    }

    [Fact]
    public async Task SetBytesCopiesTheCallersBuffer()
    {
        var (mux, cache, _) = await StartAsync();
        await using var lifetime = cache;
        var buffer = new byte[] { 7, 8, 9 };
        await cache.SetBytesAsync(Key, buffer.AsMemory());
        buffer[0] = 0;
        Assert.Equal(new byte[] { 7, 8, 9 }, (byte[]?)mux.StoredValue);
    }

    [Fact]
    public async Task SetBytesEvictsTheLocalCopy()
    {
        var (_, cache, _) = await StartAsync();
        await using var lifetime = cache;
        await cache.GetAsync<string>(Key);
        Assert.True(cache.TryGetLocal<string>(Key, out _));

        await cache.SetBytesAsync(Key, new byte[] { 1 });

        Assert.False(cache.TryGetLocal<string>(Key, out _));
    }

    [Fact]
    public async Task TypedMembersStillUseTheSerializer()
    {
        var serializer = new CountingSerializer();
        var (_, cache, _) = await StartAsync(serializer);
        await using var lifetime = cache;
        await cache.SetAsync(Key, "x");
        Assert.Equal("x", await cache.GetAsync<string>(Key));
        Assert.Equal(1, serializer.Serialized);
        Assert.Equal(1, serializer.Deserialized);
    }

    private static readonly StackExchange.Redis.RedisValue RedisValueNull = StackExchange.Redis.RedisValue.Null;

    /// <summary>Fails the test if a raw-bytes member ever reaches the serializer.</summary>
    private sealed class RefusingSerializer : IRedisNearCacheSerializer
    {
        public byte[] Serialize<T>(T value) => throw new InvalidOperationException("the serializer was used");
        public T? Deserialize<T>(ReadOnlyMemory<byte> data) => throw new InvalidOperationException("the serializer was used");
    }

    private sealed class CountingSerializer : IRedisNearCacheSerializer
    {
        public int Serialized;
        public int Deserialized;

        public byte[] Serialize<T>(T value)
        {
            Serialized++;
            return JsonRedisNearCacheSerializer.Instance.Serialize(value);
        }

        public T? Deserialize<T>(ReadOnlyMemory<byte> data)
        {
            Deserialized++;
            return JsonRedisNearCacheSerializer.Instance.Deserialize<T>(data);
        }
    }
}
