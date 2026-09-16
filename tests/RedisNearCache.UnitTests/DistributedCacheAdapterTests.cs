using System.Buffers;
using Microsoft.Extensions.Caching.Distributed;
using RedisNearCache.HybridCache;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>Exercises the IDistributedCache / IBufferDistributedCache mapping against an in-memory fake, no Redis.</summary>
public class DistributedCacheAdapterTests
{
    private sealed class FakeNearCache : IRedisNearCache
    {
        public readonly Dictionary<string, byte[]> Store = new();
        public readonly List<(string key, TimeSpan? expiry)> Writes = new();
        public RedisNearCacheStatistics Statistics { get; } = new();
        public Task Ready => Task.CompletedTask;

        // The adapter must never go through the typed members: they apply the configured serializer.
        public ValueTask<T?> GetAsync<T>(string key, CancellationToken ct = default) =>
            throw new InvalidOperationException("the adapter read through the serializer");

        public ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken ct = default) =>
            throw new InvalidOperationException("the adapter wrote through the serializer");

        public ValueTask<byte[]?> GetBytesAsync(string key, CancellationToken ct = default) =>
            new(Store.TryGetValue(key, out var b) ? b.ToArray() : null);

        public ValueTask SetBytesAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? expiry = null, CancellationToken ct = default)
        {
            Store[key] = value.ToArray();
            Writes.Add((key, expiry));
            return default;
        }

        public ValueTask<bool> RemoveAsync(string key, CancellationToken ct = default) => new(Store.Remove(key));
        public void EvictLocal(string key) { }
        public void EvictAllLocal() { }
        public bool TryGetLocal<T>(string key, out T? value) { value = default; return false; }
        public bool IsCoherent => true;
        public Task WaitForCoherenceAsync(CancellationToken ct = default) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }

    private static (RedisNearCacheDistributedCache cache, FakeNearCache fake) Create()
    {
        var fake = new FakeNearCache();
        return (new RedisNearCacheDistributedCache(fake), fake);
    }

    [Fact]
    public async Task RoundTripAndRemove()
    {
        var (cache, _) = Create();
        await cache.SetAsync("k", [1, 2, 3], new DistributedCacheEntryOptions());
        Assert.Equal(new byte[] { 1, 2, 3 }, await cache.GetAsync("k"));
        Assert.Equal(new byte[] { 1, 2, 3 }, cache.Get("k"));
        await cache.RemoveAsync("k");
        Assert.Null(await cache.GetAsync("k"));
    }

    [Fact]
    public async Task AbsoluteRelativeExpiryIsPassedThrough()
    {
        var (cache, fake) = Create();
        await cache.SetAsync("k", [1], new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(45) });
        Assert.Equal(TimeSpan.FromSeconds(45), fake.Writes.Single().expiry);
    }

    [Fact]
    public async Task AbsoluteExpiryIsConvertedToRelative()
    {
        var (cache, fake) = Create();
        await cache.SetAsync("k", [1], new DistributedCacheEntryOptions { AbsoluteExpiration = DateTimeOffset.UtcNow.AddSeconds(30) });
        var expiry = fake.Writes.Single().expiry;
        Assert.NotNull(expiry);
        Assert.InRange(expiry!.Value.TotalSeconds, 28, 30.5);
    }

    [Fact]
    public async Task SlidingExpiryBecomesAbsoluteWindow()
    {
        var (cache, fake) = Create();
        await cache.SetAsync("k", [1], new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(20) });
        Assert.Equal(TimeSpan.FromSeconds(20), fake.Writes.Single().expiry);
    }

    [Fact]
    public async Task PastAbsoluteExpiryThrows()
    {
        var (cache, _) = Create();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            cache.SetAsync("k", [1], new DistributedCacheEntryOptions { AbsoluteExpiration = DateTimeOffset.UtcNow.AddSeconds(-1) }));
    }

    [Fact]
    public async Task NoExpiryMeansNull()
    {
        var (cache, fake) = Create();
        await cache.SetAsync("k", [1], new DistributedCacheEntryOptions());
        Assert.Null(fake.Writes.Single().expiry);
    }

    [Fact]
    public async Task BufferTryGetWritesOnlyOnHit()
    {
        var (cache, _) = Create();
        var writer = new ArrayBufferWriter<byte>();
        Assert.False(await cache.TryGetAsync("missing", writer));
        Assert.Equal(0, writer.WrittenCount);
        await cache.SetAsync("k", [9, 8, 7], new DistributedCacheEntryOptions());
        Assert.True(await cache.TryGetAsync("k", writer));
        Assert.Equal(new byte[] { 9, 8, 7 }, writer.WrittenSpan.ToArray());
        var sync = new ArrayBufferWriter<byte>();
        Assert.True(cache.TryGet("k", sync));
        Assert.Equal(3, sync.WrittenCount);
    }

    [Fact]
    public async Task BufferSetCopiesMultiSegmentSequences()
    {
        var (cache, fake) = Create();
        var first = new Segment(new byte[] { 1, 2 });
        var last = first.Append(new byte[] { 3 }).Append(new byte[] { 4, 5, 6 });
        var seq = new ReadOnlySequence<byte>(first, 0, last, 3);
        Assert.False(seq.IsSingleSegment);
        await cache.SetAsync("k", seq, new DistributedCacheEntryOptions());
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, fake.Store["k"]);
    }

    [Fact]
    public async Task EveryMemberUsesTheRawBytesPath()
    {
        // The fake's typed members throw, so any path still going through the serializer fails here.
        var (cache, fake) = Create();
        var options = new DistributedCacheEntryOptions();
        await cache.SetAsync("a", [1, 2], options);
        cache.Set("b", [3], options);
        await cache.SetAsync("c", new ReadOnlySequence<byte>(new byte[] { 4, 5 }), options);
        cache.Set("d", new ReadOnlySequence<byte>(new byte[] { 6 }), options);

        Assert.Equal(new byte[] { 1, 2 }, await cache.GetAsync("a"));
        Assert.Equal(new byte[] { 3 }, cache.Get("b"));
        var writer = new ArrayBufferWriter<byte>();
        Assert.True(await cache.TryGetAsync("c", writer));
        Assert.True(cache.TryGet("d", writer));
        Assert.Equal(new byte[] { 4, 5, 6 }, writer.WrittenSpan.ToArray());
        Assert.Equal(4, fake.Writes.Count);
    }

    [Fact]
    public async Task RefreshIsANoOp()
    {
        var (cache, fake) = Create();
        await cache.SetAsync("k", [1], new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromSeconds(5) });
        await cache.RefreshAsync("k");
        cache.Refresh("k");
        Assert.Single(fake.Writes);
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
