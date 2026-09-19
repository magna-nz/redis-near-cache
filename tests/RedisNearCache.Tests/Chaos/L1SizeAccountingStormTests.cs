using System.Globalization;
using System.Reflection;
using Microsoft.Extensions.Caching.Memory;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// The single-key form of the storm in <see cref="GetManyStressTests"/>: unpaced foreign writers against plain
/// <see cref="IRedisNearCache.GetAsync{T}"/> readers on a handful of hot keys, which is reads racing invalidations
/// on the same key as fast as a real server will push them.
/// </summary>
/// <remarks>
/// Before the workaround in <see cref="RedisNearCache.Caching.L1Cache"/> (its remarks have the mechanism), this left
/// <c>MemoryCache</c>'s size total below zero, after which L1 refused every store for good while the cache still
/// called itself coherent. Two things are asserted once the system is quiet: that the accounting is EXACT (every
/// entry costs 1, so the total must equal the entry count - any drift at all fails, long before it is big enough to
/// bite), and that a key never seen before is cached like any other miss.
/// </remarks>
public sealed class L1SizeAccountingStormTests
{
    private const int KeyCount = 12;
    private const int ReaderCount = 8;
    private const int WriterCount = 4;

    private readonly ITestOutputHelper _out;

    public L1SizeAccountingStormTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task AfterAStormOfSingleKeyReadsRacingInvalidationsTheL1AccountingIsExactAndNewKeysAreCached()
    {
        var keys = StressHarness.KeyPool("l1-size-storm", KeyCount);
        var fresh = TestHelpers.Key("l1-size-storm-fresh");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        await using var foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            var cache = handle.Cache;
            foreach (var key in keys) await foreign.Db.StringSetAsync(key, "seed");

            using var stop = new CancellationTokenSource();
            long reads = 0, writes = 0;
            var failures = new System.Collections.Concurrent.ConcurrentBag<Exception>();
            var readers = Enumerable.Range(0, ReaderCount).Select(r => Task.Run(async () =>
            {
                var rng = new Random(311 + r);
                while (!stop.IsCancellationRequested)
                {
                    try
                    {
                        if (await cache.GetAsync<string>(keys[rng.Next(keys.Count)]) is null) failures.Add(new InvalidOperationException("a key that is never deleted read as null."));
                        Interlocked.Increment(ref reads);
                    }
                    catch (Exception ex)
                    {
                        failures.Add(ex);
                    }

                    // A hit completes synchronously; yield so the readers cannot starve the invalidation callbacks.
                    await Task.Yield();
                }
            })).ToArray();

            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            var writers = Enumerable.Range(0, WriterCount).Select(w => Task.Run(async () =>
            {
                var rng = new Random(7043 + w);
                long counter = 0;
                while (DateTime.UtcNow < deadline)
                {
                    await foreign.Db.StringSetAsync(keys[rng.Next(keys.Count)], $"w{w}-{(++counter).ToString(CultureInfo.InvariantCulture)}");
                    Interlocked.Increment(ref writes);
                }
            })).ToArray();

            await Task.WhenAll(writers);
            await stop.CancelAsync();
            await Task.WhenAll(readers);

            Assert.True(failures.IsEmpty, "readers failed: " + string.Join(" | ", failures.Select(e => e.Message).Take(3)));
            // The storm has to have been one: thousands of invalidations racing reads, and L1 actually in use.
            // Floors a slow runner still clears (a local run does ~55,000 writes), not a throughput requirement.
            Assert.True(writes > 500, $"only {writes} foreign writes in five seconds; too little contention to prove anything.");
            Assert.True(cache.Statistics.Invalidations > writes / 10, $"only {cache.Statistics.Invalidations} invalidations arrived for {writes} writes.");
            Assert.True(cache.Statistics.Hits > 0, "nothing was ever served from L1, so the cache was never working in the first place.");
            Assert.True(await ChaosSupport.QuiesceAsync(cache, TimeSpan.FromSeconds(1)), "the system never quiesced.");

            var (count, size) = ReadL1(cache);
            _out.WriteLine($"reads={reads} writes={writes} stats={cache.Statistics} MemoryCache Count={count} Size={size}");
            if (size is { } total)
            {
                Assert.True(total == count, $"MemoryCache accounts for {total} with {count} entries held, each costing 1: its size total has drifted.");
            }

            // A key this cache has never read, created after the storm: nothing about it is contended.
            await foreign.Db.StringSetAsync(fresh, "fresh");
            Assert.Equal("fresh", await cache.GetAsync<string>(fresh));
            Assert.True(cache.TryGetLocal<string>(fresh, out _), $"L1 refused an uncontended new key after the storm (MemoryCache Count={count} Size={size}).");

            // And every hot key caches again too, holding what Redis holds.
            foreach (var key in keys)
            {
                var truth = (string?)await foreign.Db.StringGetAsync(key);
                Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, truth!), $"{key} did not settle into L1 holding '{truth}'.");
            }
        }
        finally
        {
            await foreign.Db.KeyDeleteAsync([.. keys.Select(k => (RedisKey)k), fresh]);
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// <c>MemoryCache.Size</c> is internal, so it is read reflectively; null if a later package renames it, in which
    /// case the behavioural assertions stand on their own.
    /// </summary>
    private static (long Count, long? Size) ReadL1(IRedisNearCache cache)
    {
        var l1 = cache.GetType().GetField("_l1", BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(cache);
        var memoryCache = l1?.GetType().GetField("_cache", BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(l1) as MemoryCache;
        if (memoryCache is null) return (0, null);
        var size = typeof(MemoryCache).GetProperty("Size", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)?.GetValue(memoryCache);
        return (memoryCache.Count, size is long value ? value : null);
    }
}
