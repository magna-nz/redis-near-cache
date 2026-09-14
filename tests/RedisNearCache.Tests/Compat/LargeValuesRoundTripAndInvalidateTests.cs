using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// Megabyte-sized values: they round-trip unchanged through L1 (no truncation, no re-encoding of the byte[]
/// path), and the invalidation for a 1 MB key is the same tiny key-name message as for a 3-byte one.
///
/// <para>
/// <b>L1SizeLimit counts ENTRIES, not bytes.</b> <see cref="RedisNearCacheOptions.L1SizeLimit"/> is the
/// maximum number of entries the L1 holds; nothing in the cache measures value size. Two 1 MB values under a
/// limit of 2 both stay resident, i.e. ~2 MB of process memory for a configuration that reads as "2". A
/// deployment that caches large values must size the limit from its own expected value size; the library
/// will not do it for them.
/// </para>
/// </summary>
public class LargeValuesRoundTripAndInvalidateTests
{
    private const int OneMegabyte = 1024 * 1024;

    private readonly ITestOutputHelper _out;

    public LargeValuesRoundTripAndInvalidateTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task LargeStringAndByteArrayRoundTripAndInvalidate()
    {
        var stringKey = TestHelpers.Key("large-string");
        var bytesKey = TestHelpers.Key("large-bytes");

        // L1SizeLimit = 2 with two 1 MB entries: the limit is an entry count, so both survive.
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.L1SizeLimit = 2);
        await using var foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;

            var bigString = string.Create(OneMegabyte, 0, (span, _) =>
            {
                for (var i = 0; i < span.Length; i++) span[i] = (char)('a' + (i % 26));
            });
            var bigBytes = new byte[OneMegabyte];
            for (var i = 0; i < bigBytes.Length; i++) bigBytes[i] = (byte)(i % 251);

            await cache.SetAsync(stringKey, bigString);
            await cache.SetAsync(bytesKey, bigBytes);

            Assert.Equal(bigString, await cache.GetAsync<string>(stringKey));
            var readBytes = await cache.GetAsync<byte[]>(bytesKey);
            Assert.NotNull(readBytes);
            Assert.Equal(OneMegabyte, readBytes!.Length);
            Assert.True(bigBytes.AsSpan().SequenceEqual(readBytes), "the 1 MB byte[] did not round-trip unchanged.");

            Assert.True(
                await Poll.UntilAsync(() => cache.TryGetLocal<string>(stringKey, out _), TimeSpan.FromSeconds(3)),
                "the 1 MB string was not cached locally.");
            Assert.True(cache.TryGetLocal<string>(stringKey, out var localString));
            Assert.Equal(bigString, localString);
            Assert.True(
                await Poll.UntilAsync(() => cache.TryGetLocal<byte[]>(bytesKey, out _), TimeSpan.FromSeconds(3)),
                "the 1 MB byte[] was not cached locally.");
            Assert.True(cache.TryGetLocal<byte[]>(bytesKey, out var localBytes));
            Assert.True(bigBytes.AsSpan().SequenceEqual(localBytes), "the L1 copy of the 1 MB byte[] differs from what was stored.");
            _out.WriteLine("both 1 MB entries are resident under L1SizeLimit=2: the limit counts entries, not bytes.");

            // A foreign write of a tiny value invalidates a 1 MB entry exactly like any other key.
            RedisCli.Standalone("SET", stringKey, "small");
            await foreign.Db.StringSetAsync(bytesKey, new byte[] { 1, 2, 3 });

            var evictedString = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(stringKey, out _), TimeSpan.FromSeconds(3));
            Assert.True(evictedString, "the 1 MB string survived an external write.");
            var evictedBytes = await Poll.UntilAsync(() => !cache.TryGetLocal<byte[]>(bytesKey, out _), TimeSpan.FromSeconds(3));
            Assert.True(evictedBytes, "the 1 MB byte[] survived an external write.");

            Assert.Equal("small", await cache.GetAsync<string>(stringKey));
            Assert.Equal(new byte[] { 1, 2, 3 }, await cache.GetAsync<byte[]>(bytesKey));
        }
        finally
        {
            await foreign.Db.KeyDeleteAsync(new RedisKey[] { stringKey, bytesKey });
            await handle.DisposeAsync();
        }
    }
}
