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
    public async Task L1SizeLimitBytesBoundsL1ByValueLength()
    {
        const int budget = 400;
        const int valueLength = 100;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.L1SizeLimitBytes = budget);
        try
        {
            var cache = handle.Cache;
            var value = new string('x', valueLength);
            var keys = Enumerable.Range(0, 20).Select(i => TestHelpers.Key($"size-bytes-{i}")).ToArray();
            foreach (var key in keys) await cache.SetAsync(key, value);
            foreach (var key in keys) Assert.Equal(value, await cache.GetAsync<string>(key));

            // MemoryCache compacts on a thread-pool thread, so the budget is met eventually, not on the last Set.
            long HeldBytes() => keys.Count(k => cache.TryGetLocal<string>(k, out _)) * (long)valueLength;
            var withinBudget = await Poll.UntilAsync(() => HeldBytes() <= budget, TimeSpan.FromSeconds(5));
            Assert.True(withinBudget, $"expected at most {budget} bytes held locally; found {HeldBytes()}.");

            // Whether a key survived eviction or not, the value read must be right.
            foreach (var key in keys) Assert.Equal(value, await cache.GetAsync<string>(key));

            // "Within budget" must not mean "caches nothing": values that fit are still held locally.
            var someResident = await Poll.UntilAsync(
                async () =>
                {
                    foreach (var key in keys) await cache.GetAsync<string>(key);
                    return keys.Any(k => cache.TryGetLocal<string>(k, out _));
                },
                TimeSpan.FromSeconds(5));
            Assert.True(someResident, "a byte budget four values wide held none of them locally.");
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    [Fact]
    public async Task AValueBiggerThanTheByteBudgetIsServedButNeverCached()
    {
        const int budget = 400;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.L1SizeLimitBytes = budget);
        try
        {
            var cache = handle.Cache;
            var key = TestHelpers.Key("size-bytes-oversized");
            var oversized = new string('y', budget * 3);
            await cache.SetAsync(key, oversized);

            // Every read goes to Redis: there is no budget it could ever fit in, so it is never stored.
            for (var i = 0; i < 3; i++)
            {
                Assert.Equal(oversized, await cache.GetAsync<string>(key));
                Assert.False(cache.TryGetLocal<string>(key, out _), $"read {i + 1}: a value larger than the whole byte budget must never enter L1.");
            }

            // A value too big for the whole budget is refused BY DESIGN (L1Cache.SizeLimit's remarks): that is not
            // the MemoryCache size-accounting drift redisnearcache.l1.store_refusals exists to surface, so it must
            // not be counted as one. This is the deterministic, non-flaky half of that counter's coverage; the
            // drift bug itself needs a write storm and is already covered by Chaos/L1SizeAccountingStormTests.
            Assert.Equal(0, cache.Statistics.L1StoreRefusals);

            // A value that does fit still caches, so the oversized one did not poison the budget.
            var small = TestHelpers.Key("size-bytes-small");
            await cache.SetAsync(small, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, small, "v1"), "a value within the budget must still be cached.");
            Assert.Equal(0, cache.Statistics.L1StoreRefusals);
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
