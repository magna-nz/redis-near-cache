using System.Buffers;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.HybridCache;
using RedisNearCache.Tests.Chaos;
using Xunit;

namespace RedisNearCache.Tests.HybridCache;

/// <summary>
/// The adapters with a <see cref="RedisNearCacheOptions.Serializer"/> that fails if it is ever used: the distributed
/// cache must store and return the caller's bytes as they are, and so must HybridCache on top of it.
/// </summary>
public class CustomSerializerAdapterTests : IAsyncLifetime
{
    private ServiceProvider _provider = null!;
    private IRedisNearCache _cache = null!;
    private IDistributedCache _distributed = null!;
    private IBufferDistributedCache _buffered = null!;
    private Microsoft.Extensions.Caching.Hybrid.HybridCache _hybrid = null!;
    private ForeignClient _foreign = null!;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString, o => o.Serializer = new RefusingSerializer());
        services.AddRedisNearCacheHybridCache();
        _provider = services.BuildServiceProvider();
        _cache = _provider.GetRequiredService<IRedisNearCache>();
        _distributed = _provider.GetRequiredService<IDistributedCache>();
        _buffered = _provider.GetRequiredService<IBufferDistributedCache>();
        _hybrid = _provider.GetRequiredService<Microsoft.Extensions.Caching.Hybrid.HybridCache>();
        await _cache.Ready;
        _foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
    }

    public async Task DisposeAsync()
    {
        await _provider.DisposeAsync();
        await _foreign.DisposeAsync();
    }

    [Fact]
    public async Task ByteArrayPathStoresTheCallersBytesAndServesThemFromL1()
    {
        var key = TestHelpers.Key("dc-custom-serializer");
        var payload = RawBytes.Payload();

        await _distributed.SetAsync(key, payload, new DistributedCacheEntryOptions());

        Assert.Equal(payload, (byte[]?)await _foreign.Db.StringGetAsync(key));
        Assert.True(await ReadUntilHitAsync(async () => await _distributed.GetAsync(key), payload), $"{key} was never served from L1.");
        Assert.Equal(payload, _distributed.Get(key));
    }

    [Fact]
    public async Task BufferPathStoresTheCallersBytesAndServesThemFromL1()
    {
        var key = TestHelpers.Key("dc-buffer-custom-serializer");
        var payload = RawBytes.Payload();
        // Two segments, so the adapter's copy of a non-contiguous sequence is exercised too.
        var first = new Segment(payload.AsMemory(0, 3));
        var last = first.Append(payload.AsMemory(3));

        await _buffered.SetAsync(key, new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length), new DistributedCacheEntryOptions());

        Assert.Equal(payload, (byte[]?)await _foreign.Db.StringGetAsync(key));
        Assert.True(await ReadUntilHitAsync(async () =>
        {
            var writer = new ArrayBufferWriter<byte>();
            return await _buffered.TryGetAsync(key, writer) ? writer.WrittenSpan.ToArray() : null;
        }, payload), $"{key} was never served from L1.");
    }

    [Fact]
    public async Task BytesWrittenByAnotherClientAreReadExactly()
    {
        var key = TestHelpers.Key("dc-foreign-bytes");
        var payload = RawBytes.Payload();
        await _foreign.Db.StringSetAsync(key, payload);

        Assert.Equal(payload, await _distributed.GetAsync(key));
        var writer = new ArrayBufferWriter<byte>();
        Assert.True(await _buffered.TryGetAsync(key, writer));
        Assert.Equal(payload, writer.WrittenSpan.ToArray());
    }

    [Fact]
    public async Task AForeignWriteEvictsTheAdaptersEntry()
    {
        var key = TestHelpers.Key("dc-foreign-write");
        await _distributed.SetAsync(key, [1, 2], new DistributedCacheEntryOptions());
        Assert.True(await ReadUntilHitAsync(async () => await _distributed.GetAsync(key), [1, 2]), $"{key} was never served from L1.");

        await _foreign.Db.StringSetAsync(key, new byte[] { 3 });

        Assert.True(await Poll.UntilAsync(async () => (await _distributed.GetAsync(key))?.SequenceEqual(new byte[] { 3 }) == true, TimeSpan.FromSeconds(10)),
            "the adapter kept serving the old bytes after a foreign write.");
    }

    [Fact]
    public async Task HybridCacheWorksWithACustomSerializer()
    {
        var key = TestHelpers.Key("hc-custom-serializer");
        var factoryCalls = 0;

        ValueTask<string> Factory(CancellationToken ct)
        {
            Interlocked.Increment(ref factoryCalls);
            return ValueTask.FromResult("v1");
        }

        Assert.Equal("v1", await _hybrid.GetOrCreateAsync<string>(key, Factory));
        // HybridCache writes to the distributed tier in the background; once that lands, the factory is not called again.
        var settled = await Poll.UntilAsync(async () =>
        {
            var before = Volatile.Read(ref factoryCalls);
            return await _hybrid.GetOrCreateAsync<string>(key, Factory) == "v1" && Volatile.Read(ref factoryCalls) == before;
        });
        Assert.True(settled, "the entry never settled into being served without the factory.");
        Assert.True(await Poll.UntilAsync(() => _foreign.Db.KeyExistsAsync(key)), "HybridCache did not persist the entry to Redis.");
    }

    /// <summary>Reads until one read returns <paramref name="expected"/> and the hit counter moved.</summary>
    private async Task<bool> ReadUntilHitAsync(Func<Task<byte[]?>> read, byte[] expected)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            var hits = _cache.Statistics.Hits;
            var value = await read();
            if (value is not null && value.AsSpan().SequenceEqual(expected) && _cache.Statistics.Hits > hits)
            {
                return true;
            }

            await Task.Delay(20);
        }

        return false;
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }
}
