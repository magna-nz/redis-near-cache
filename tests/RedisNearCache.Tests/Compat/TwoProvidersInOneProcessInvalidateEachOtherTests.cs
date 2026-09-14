using RedisNearCache.Tests.EdgeCases;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// Two independent <c>AddRedisNearCache</c> registrations living in the same process (two DI containers, two
/// private multiplexers, two client names, two tracking redirects). Nothing in the library couples them: each
/// is invalidated by the other's writes purely through the server, exactly as two separate processes would
/// be, and each keeps its own <see cref="RedisNearCacheStatistics"/>.
/// </summary>
public class TwoProvidersInOneProcessInvalidateEachOtherTests
{
    private readonly ITestOutputHelper _out;

    public TwoProvidersInOneProcessInvalidateEachOtherTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task EachProviderIsInvalidatedByTheOther()
    {
        var keyA = TestHelpers.Key("two-providers-a");
        var keyB = TestHelpers.Key("two-providers-b");

        var a = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var b = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            Assert.NotSame(a.Cache, b.Cache);
            Assert.NotSame(a.Cache.Statistics, b.Cache.Statistics);
            Assert.NotEqual(a.Connection.ClientName, b.Connection.ClientName);

            // A caches the key; B writes it. A must lose its copy.
            await a.Cache.SetAsync(keyA, "from-a");
            Assert.True(
                await TestHelpers.ReadUntilCachedAsync(a.Cache, keyA, "from-a"),
                "provider A did not cache its own key before provider B wrote it.");

            var aInvalidationsBefore = a.Cache.Statistics.Invalidations;
            var bInvalidationsBefore = b.Cache.Statistics.Invalidations;

            await b.Cache.SetAsync(keyA, "from-b");

            var aEvicted = await Poll.UntilAsync(() => !a.Cache.TryGetLocal<string>(keyA, out _), TimeSpan.FromSeconds(3));
            Assert.True(aEvicted, "provider A kept its L1 copy after provider B wrote the key.");
            Assert.Equal("from-b", await a.Cache.GetAsync<string>(keyA));
            Assert.True(
                a.Cache.Statistics.Invalidations > aInvalidationsBefore,
                "provider A counted no invalidation for provider B's write.");

            // B never read keyA before writing it, so it was never tracking it: the server had nothing to
            // tell B about, and NOLOOP means B is not told about its own write either.
            Assert.Equal(bInvalidationsBefore, b.Cache.Statistics.Invalidations);

            // Now the other direction.
            await b.Cache.SetAsync(keyB, "from-b");
            Assert.True(
                await TestHelpers.ReadUntilCachedAsync(b.Cache, keyB, "from-b"),
                "provider B did not cache its own key before provider A wrote it.");

            await a.Cache.SetAsync(keyB, "from-a");

            var bEvicted = await Poll.UntilAsync(() => !b.Cache.TryGetLocal<string>(keyB, out _), TimeSpan.FromSeconds(3));
            Assert.True(bEvicted, "provider B kept its L1 copy after provider A wrote the key.");
            Assert.Equal("from-a", await b.Cache.GetAsync<string>(keyB));

            _out.WriteLine($"A: {a.Cache.Statistics}");
            _out.WriteLine($"B: {b.Cache.Statistics}");

            // Separate counters, not a shared static: each provider counted only its own traffic.
            Assert.True(a.Cache.Statistics.Misses > 0 && b.Cache.Statistics.Misses > 0);
        }
        finally
        {
            RedisCli.Standalone("DEL", keyA, keyB);
            await a.DisposeAsync();
            await b.DisposeAsync();
        }
    }
}
