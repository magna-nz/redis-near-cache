using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>Behaviour driven directly by <see cref="RedisNearCacheOptions.L1SizeLimit"/> and <see cref="RedisNearCacheOptions.L1MaxAge"/>.</summary>
public class L1ConfigurationTests
{
    [Fact]
    public async Task L1SizeLimitEvicts()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.L1SizeLimit = 10);
        try
        {
            var cache = handle.Cache;
            var keys = Enumerable.Range(0, 50).Select(i => TestHelpers.Key($"size-limit-{i}")).ToArray();
            foreach (var key in keys)
            {
                await cache.SetAsync(key, key);
            }

            foreach (var key in keys)
            {
                Assert.Equal(key, await cache.GetAsync<string>(key));
            }

            var withinLimit = await Poll.UntilAsync(
                () => keys.Count(k => cache.TryGetLocal<string>(k, out _)) <= 10,
                TimeSpan.FromSeconds(3));
            var cachedCount = keys.Count(k => cache.TryGetLocal<string>(k, out _));
            Assert.True(withinLimit, $"expected at most 10 of 50 keys still cached; found {cachedCount}.");

            // Reads must still be correct regardless of whether a key survived in L1.
            foreach (var key in keys)
            {
                Assert.Equal(key, await cache.GetAsync<string>(key));
            }
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task L1MaxAgeInfiniteKeepsEntries()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.L1MaxAge = Timeout.InfiniteTimeSpan);
        try
        {
            var cache = handle.Cache;
            var key = TestHelpers.Key("infinite-max-age");
            await cache.SetAsync(key, "v1");
            Assert.Equal("v1", await cache.GetAsync<string>(key));
            Assert.True(cache.TryGetLocal<string>(key, out _), "key must be cached immediately after the read.");

            for (var i = 0; i < 3; i++)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500));
                Assert.True(cache.TryGetLocal<string>(key, out var cached),
                    $"check {i + 1}/3: entry disappeared from L1 despite L1MaxAge being infinite.");
                Assert.Equal("v1", cached);
            }
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }
}
