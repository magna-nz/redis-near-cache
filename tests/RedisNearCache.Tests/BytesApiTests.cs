using RedisNearCache.Tests.Broadcast;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests;

/// <summary>
/// <see cref="IRedisNearCache.GetBytesAsync"/> and <see cref="IRedisNearCache.SetBytesAsync(string, ReadOnlyMemory{byte}, TimeSpan?, CancellationToken)"/> against real Redis, with a
/// serializer that fails if it is ever used, in both tracking modes.
/// </summary>
public abstract class BytesApiTestsBase : IAsyncLifetime
{
    private EdgeCaseProvider _handle = null!;
    private ForeignClient _foreign = null!;

    private protected IRedisNearCache Cache => _handle.Cache;
    private protected IDatabase Foreign => _foreign.Db;

    protected abstract void Configure(RedisNearCacheOptions options);

    protected abstract string NewKey(string suffix);

    public async Task InitializeAsync()
    {
        _handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o =>
        {
            o.Serializer = new RefusingSerializer();
            Configure(o);
        });
        _foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
    }

    public async Task DisposeAsync()
    {
        await _handle.DisposeAsync();
        await _foreign.DisposeAsync();
    }

    [Fact]
    public async Task WrittenBytesAreStoredExactlyAndServedFromL1()
    {
        var key = NewKey("bytes-roundtrip");
        var payload = RawBytes.Payload();

        await Cache.SetBytesAsync(key, payload);

        Assert.Equal(payload, (byte[]?)await Foreign.StringGetAsync(key));
        Assert.True(await RawBytes.ReadUntilCachedAsync(Cache, key, payload), $"{key} was never served from L1.");
    }

    [Fact]
    public async Task BytesWrittenByAnotherClientAreReadExactly()
    {
        var key = NewKey("bytes-foreign-value");
        var payload = RawBytes.Payload();
        await Foreign.StringSetAsync(key, payload);

        Assert.True(await RawBytes.ReadUntilCachedAsync(Cache, key, payload), $"{key} was never served from L1.");
    }

    [Fact]
    public async Task MissingKeyIsNull()
    {
        Assert.Null(await Cache.GetBytesAsync(NewKey("bytes-missing")));
    }

    [Fact]
    public async Task AForeignWriteEvictsACachedBytesEntry()
    {
        var key = NewKey("bytes-foreign-write");
        await Cache.SetBytesAsync(key, new byte[] { 1, 2, 3 });
        Assert.True(await RawBytes.ReadUntilCachedAsync(Cache, key, [1, 2, 3]), $"{key} was never served from L1.");

        var invalidationsBefore = Cache.Statistics.Invalidations;
        await Foreign.StringSetAsync(key, new byte[] { 4, 5 });

        Assert.True(await Poll.UntilAsync(() => Cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromSeconds(10)),
            "the foreign write did not push an invalidation.");
        Assert.True(await RawBytes.ReadUntilCachedAsync(Cache, key, [4, 5]), "the new bytes were never read back and cached.");
    }

    [Fact]
    public async Task ChangingTheReturnedArrayDoesNotChangeTheCachedCopy()
    {
        var key = NewKey("bytes-copy");
        await Cache.SetBytesAsync(key, new byte[] { 1, 2, 3 });
        Assert.True(await RawBytes.ReadUntilCachedAsync(Cache, key, [1, 2, 3]), $"{key} was never served from L1.");

        // Both reads must come from L1 for this to prove anything; a late echo of our own write (Broadcast) can turn
        // one into a miss, so try again until a pair lands.
        for (var attempt = 0; attempt < 20; attempt++)
        {
            var hitsBefore = Cache.Statistics.Hits;
            var first = (await Cache.GetBytesAsync(key))!;
            first[0] = 99;
            var second = (await Cache.GetBytesAsync(key))!;
            Assert.Equal(new byte[] { 1, 2, 3 }, second);
            if (Cache.Statistics.Hits == hitsBefore + 2)
            {
                return;
            }

            await Task.Delay(50);
        }

        Assert.Fail("no two consecutive reads were served from L1.");
    }

    [Fact]
    public async Task RemoveDeletesABytesEntry()
    {
        var key = NewKey("bytes-remove");
        await Cache.SetBytesAsync(key, new byte[] { 1 });
        Assert.True(await RawBytes.ReadUntilCachedAsync(Cache, key, [1]), $"{key} was never served from L1.");

        Assert.True(await Cache.RemoveAsync(key));

        Assert.Null(await Cache.GetBytesAsync(key));
        Assert.False(await Foreign.KeyExistsAsync(key));
    }

    [Fact]
    public async Task ExpiryIsApplied()
    {
        var key = NewKey("bytes-expiry");
        await Cache.SetBytesAsync(key, new byte[] { 1 }, TimeSpan.FromSeconds(30));

        var ttl = await Foreign.KeyTimeToLiveAsync(key);
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalSeconds, 1, 30);
    }
}

public sealed class BytesApiTests : BytesApiTestsBase
{
    protected override void Configure(RedisNearCacheOptions options)
    {
    }

    protected override string NewKey(string suffix) => TestHelpers.Key(suffix);
}

public sealed class BroadcastBytesApiTests : BytesApiTestsBase
{
    protected override void Configure(RedisNearCacheOptions options)
    {
        options.TrackingMode = TrackingMode.Broadcast;
        options.KeyPrefixes.Add(BroadcastKey.Prefix);
    }

    protected override string NewKey(string suffix) => BroadcastKey.New(suffix);
}
