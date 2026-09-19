using System.Buffers;
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

    /// <summary>
    /// The member HybridCache actually calls. Over the library's own cache it writes the stored bytes straight into
    /// the writer instead of materialising a <c>byte[]</c> first, so the round trip is worth proving against real
    /// Redis - on the miss that reads from the server and on the hit that reads from L1.
    /// </summary>
    [Fact]
    public async Task BufferDistributedCache_TryGetRoundTrip()
    {
        var buffered = Assert.IsAssignableFrom<IBufferDistributedCache>(_fx.DistributedCache);
        var key = TestHelpers.Key("dc-buffer");
        var value = new byte[] { 0, 1, 2, 250, 255 };

        await buffered.SetAsync(key, new ReadOnlySequence<byte>(value), new DistributedCacheEntryOptions());

        var first = new ArrayBufferWriter<byte>();
        Assert.True(await buffered.TryGetAsync(key, first));
        Assert.Equal(value, first.WrittenSpan.ToArray());

        Assert.True(await Poll.UntilAsync(() => _fx.Cache.TryGetLocal<byte[]>(key, out _)),
            "the first TryGetAsync did not populate L1, so the read below would not be a hit.");

        var hitsBefore = _fx.Cache.Statistics.Hits;
        var second = new ArrayBufferWriter<byte>();
        Assert.True(await buffered.TryGetAsync(key, second));
        Assert.Equal(value, second.WrittenSpan.ToArray());
        Assert.True(_fx.Cache.Statistics.Hits > hitsBefore, "The second TryGetAsync was not served from the tracked near cache.");

        var missing = new ArrayBufferWriter<byte>();
        Assert.False(await buffered.TryGetAsync(TestHelpers.Key("dc-buffer-absent"), missing));
        Assert.Equal(0, missing.WrittenCount);
    }

    [Fact]
    public async Task BufferDistributedCache_ExternalWriteIsVisible()
    {
        var buffered = Assert.IsAssignableFrom<IBufferDistributedCache>(_fx.DistributedCache);
        var key = TestHelpers.Key("dc-buffer-external");

        await buffered.SetAsync(key, new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("v1")), new DistributedCacheEntryOptions());
        var writer = new ArrayBufferWriter<byte>();
        Assert.True(await buffered.TryGetAsync(key, writer));
        Assert.Equal("v1", Encoding.UTF8.GetString(writer.WrittenSpan));

        RedisCli.Standalone("SET", key, "v2");

        string? seen = null;
        var updated = await Poll.UntilAsync(async () =>
        {
            var w = new ArrayBufferWriter<byte>();
            if (!await buffered.TryGetAsync(key, w)) return false;
            seen = Encoding.UTF8.GetString(w.WrittenSpan);
            return seen == "v2";
        });

        Assert.True(updated, $"The external write was not visible through IBufferDistributedCache within the deadline; last saw {seen ?? "<null>"}.");
    }
}
