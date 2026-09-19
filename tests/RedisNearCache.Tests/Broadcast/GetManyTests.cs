using RedisNearCache.Tests.EdgeCases;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// <see cref="IRedisNearCache.GetManyAsync{T}"/> in <see cref="TrackingMode.Broadcast"/>: the same mix and
/// foreign-write shape as the Redirect suite, against a cache armed with <c>CLIENT TRACKING ON BCAST PREFIX
/// bc:</c>. Two differences drive the shape of this test: the server pushes for every write under the prefix
/// whoever made it (so the keys are seeded and rewritten with <see cref="RedisCli"/>, and the population is
/// polled rather than asserted on the first read, as <see cref="ExternalWriteEvictsTests"/> documents), and keys
/// outside the prefix are read with a plain, already-untracked GET rather than the <c>CLIENT CACHING NO</c>
/// transaction Redirect sends.
/// </summary>
/// <remarks>
/// Its own cache rather than <see cref="BroadcastStandaloneCacheFixture"/>, because it asserts on exact
/// <see cref="RedisNearCacheStatistics"/> deltas.
/// </remarks>
public class GetManyTests
{
    private readonly ITestOutputHelper _out;

    public GetManyTests(ITestOutputHelper output) => _out = output;

    private static Task<EdgeCaseProvider> BuildAsync() => EdgeCaseSupport.BuildAsync(
        StandaloneCacheFixture.ConnectionString,
        o =>
        {
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add(BroadcastKey.Prefix);
        });

    [Fact]
    public async Task AMixOfCachedUncachedAndMissingKeysUnderTheBroadcastPrefix()
    {
        var cached = BroadcastKey.New("gm-mix-cached");
        var uncached = BroadcastKey.New("gm-mix-uncached");
        var missing = BroadcastKey.New("gm-mix-missing");
        var handle = await BuildAsync();
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone("MSET", cached, "c1", uncached, "u1");

            // The seeding writes are broadcast back to us, so poll for the entry rather than assert on one read.
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, cached, "c1"), $"{cached} was not cached before the multi-key read.");
            Assert.False(cache.TryGetLocal<string>(uncached, out _), $"{uncached} must not be in L1 yet, or the miss path is untested.");

            var result = await cache.GetManyAsync<string>([cached, uncached, missing]);

            Assert.Equal(3, result.Count);
            Assert.Equal("c1", result[cached]);
            Assert.Equal("u1", result[uncached]);
            Assert.Null(result[missing]);
            Assert.False(cache.TryGetLocal<string>(missing, out _), "a null reply must never populate L1.");

            // A push for one of the seeding writes may still have been in flight during the read above, in which
            // case the in-flight rule correctly discarded that store; poll for the settled state before making
            // any claim about hit counts.
            Assert.True(await Poll.UntilAsync(async () =>
            {
                await cache.GetManyAsync<string>([cached, uncached]);
                return cache.TryGetLocal<string>(cached, out _) && cache.TryGetLocal<string>(uncached, out _);
            }, TimeSpan.FromSeconds(10)), "the two existing keys never ended up in L1 together.");

            var hits = cache.Statistics.Hits;
            var misses = cache.Statistics.Misses;

            var second = await cache.GetManyAsync<string>([cached, uncached, missing]);

            Assert.Equal("c1", second[cached]);
            Assert.Equal("u1", second[uncached]);
            Assert.Null(second[missing]);
            Assert.Equal(hits + 2, cache.Statistics.Hits);
            Assert.Equal(misses + 1, cache.Statistics.Misses);
            _out.WriteLine($"broadcast stats: {cache.Statistics}");
        }
        finally
        {
            RedisCli.Standalone("DEL", cached, uncached, missing);
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task AForeignWriteEvictsOnlyItsOwnKeyOfTheSet()
    {
        const int count = 4;
        var keys = Enumerable.Range(0, count).Select(i => BroadcastKey.New($"gm-foreign{i}")).ToArray();
        var handle = await BuildAsync();
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone([.. new[] { "MSET" }, .. keys.SelectMany((k, i) => new[] { k, $"v{i}" })]);

            var populated = await Poll.UntilAsync(async () =>
            {
                await cache.GetManyAsync<string>(keys);
                return keys.All(k => cache.TryGetLocal<string>(k, out _));
            }, TimeSpan.FromSeconds(10));
            Assert.True(populated, "the multi-key read did not leave every key in L1.");

            var written = keys[2];
            var invalidations = cache.Statistics.Invalidations;
            RedisCli.Standalone("SET", written, "rewritten");

            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(written, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "the written key was not evicted from L1 after a foreign SET.");
            Assert.True(cache.Statistics.Invalidations > invalidations, "no broadcast invalidation was counted for the foreign write.");
            foreach (var other in keys.Where(k => k != written))
            {
                Assert.True(cache.TryGetLocal<string>(other, out _), $"{other} was evicted although nothing wrote to it.");
            }

            var hits = cache.Statistics.Hits;
            var misses = cache.Statistics.Misses;

            var result = await cache.GetManyAsync<string>(keys);

            Assert.Equal("rewritten", result[written]);
            for (var i = 0; i < count; i++)
            {
                if (keys[i] == written) continue;
                Assert.Equal($"v{i}", result[keys[i]]);
            }

            Assert.Equal(hits + count - 1, cache.Statistics.Hits);
            Assert.Equal(misses + 1, cache.Statistics.Misses);
        }
        finally
        {
            RedisCli.Standalone([.. new[] { "DEL" }, .. keys]);
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// A key outside <see cref="RedisNearCacheOptions.KeyPrefixes"/> read alongside one inside: both answer
    /// correctly, only the prefixed one is cached, and - unlike Redirect - no transaction is used, because the
    /// reading connection is not tracked in this mode at all.
    /// </summary>
    [Fact]
    public async Task AKeyOutsideThePrefixIsAnsweredButNeverCachedAndNeedsNoTransaction()
    {
        var inside = BroadcastKey.New("gm-inside");
        var outside = TestHelpers.Key("gm-outside"); // t:..., deliberately not under bc:
        var handle = await BuildAsync();
        try
        {
            var cache = handle.Cache;
            var server = handle.Multiplexer.GetServer(handle.Multiplexer.GetEndPoints()[0]);
            RedisCli.Standalone("MSET", inside, "in", outside, "out");

            var multiBefore = TestHelpers.CommandCalls(server, "multi");

            var result = await cache.GetManyAsync<string>([inside, outside]);

            Assert.Equal("in", result[inside]);
            Assert.Equal("out", result[outside]);
            Assert.False(cache.TryGetLocal<string>(outside, out _), "a key outside KeyPrefixes must never be cached in L1.");
            Assert.Equal(multiBefore, TestHelpers.CommandCalls(server, "multi"));

            Assert.True(await Poll.UntilAsync(async () =>
            {
                await cache.GetManyAsync<string>([inside]);
                return cache.TryGetLocal<string>(inside, out _);
            }, TimeSpan.FromSeconds(10)), "the prefixed key was never cached, so the assertion about the unprefixed one proves nothing.");

            // The unprefixed key is still not cached, however often it is read.
            await cache.GetManyAsync<string>([inside, outside]);
            Assert.False(cache.TryGetLocal<string>(outside, out _), "a key outside KeyPrefixes must never be cached in L1.");
        }
        finally
        {
            RedisCli.Standalone("DEL", inside, outside);
            await handle.DisposeAsync();
        }
    }
}
