using System.Collections.Concurrent;
using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// Integration-level coverage for <c>redisnearcache.l1.store_refusals</c> / <see cref="RedisNearCacheStatistics.L1StoreRefusals"/>
/// beyond <see cref="L1ConfigurationTests.AValueBiggerThanTheByteBudgetIsServedButNeverCached"/> (the deterministic
/// "too big to ever fit" case, asserted there). This file adds the other deterministic claim worth having at the
/// integration level: a real flush racing real stores must not inflate the counter.
/// </summary>
/// <remarks>
/// Deliberately NOT covered here: asserting a non-zero refusal count on a healthy server. Under a sane budget there
/// is nothing to refuse, and the only way to make one fire for real is the MemoryCache size-accounting drift bug
/// (dotnet/runtime#129186), which needs an unpaced write storm to trigger and is already exercised end to end by
/// <c>Chaos/L1SizeAccountingStormTests</c>. A test that needs that bug to fire to pass would be inherently flaky.
/// </remarks>
public class L1StoreRefusalTests
{
    /// <summary>
    /// <see cref="RedisNearCache.Caching.L1Cache.Clear"/> takes every stripe lock specifically so a store landing in
    /// the instant a flush swaps the backing collection cannot be misread as a refusal (see its remarks). The unit
    /// twin (<c>L1CacheSizeAccountingTests</c>) proves this against a bare <c>L1Cache</c> with no Redis behind it;
    /// this is the same claim end to end - real reads racing a real flush this test triggers itself
    /// (<see cref="IRedisNearCache.EvictAllLocal"/>), not a server-side flush or re-arm.
    /// </summary>
    [Fact]
    public async Task ConcurrentStoresRacingARealFlushDoNotInflateTheStoreRefusalCounter()
    {
        var keys = Enumerable.Range(0, 16).Select(i => TestHelpers.Key($"refusal-race-{i}")).ToArray();
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            foreach (var key in keys) RedisCli.Standalone("SET", key, "v");

            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(1.5));
            var failures = new ConcurrentBag<Exception>();

            // Readers: every hit that a concurrent EvictAllLocal just cleared is a fresh miss, i.e. a fresh store,
            // for that key - so this is a real storm of L1 stores racing a real flush, not merely reads racing reads.
            var readers = Enumerable.Range(0, 16).Select(r => Task.Run(async () =>
            {
                var rng = new Random(4100 + r);
                while (!stop.IsCancellationRequested)
                {
                    try
                    {
                        await cache.GetAsync<string>(keys[rng.Next(keys.Length)]);
                    }
                    catch (Exception ex)
                    {
                        failures.Add(ex);
                    }
                }
            })).ToArray();

            // Flushing as fast as the loop itself allows (no pacing delay): the race this guards against is a Clear()
            // landing in the few nanoseconds between one Set's store and its own presence check, so the flusher needs
            // to run back to back, not on a timer, to have any realistic chance of ever landing there.
            var flusher = Task.Run(() =>
            {
                while (!stop.IsCancellationRequested)
                {
                    cache.EvictAllLocal();
                }
            });

            await Task.WhenAll(readers.Append(flusher));

            Assert.True(failures.IsEmpty, "readers failed: " + string.Join(" | ", failures.Select(e => e.Message).Take(3)));
            // Every flush took every stripe lock around the clear, so no store could ever land in the collection
            // that was about to be swapped away: the counter must be exactly 0, not merely small.
            Assert.Equal(0, cache.Statistics.L1StoreRefusals);
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone(["DEL", .. keys]);
        }
    }
}
